"""
meta_refresh_forecast.py — formerly refresh_forecasts.py (renamed to group with
the other meta_*.py Metaculus scripts).

Triggers:
  1. CLOSING SOON  — question closes within 14 days (configurable)
  2. STALE         — original forecast is older than 30 days (configurable)
  3. SINGLE        — one specific question, triggered manually (e.g. off a
                      "Significant change in Community Prediction" email),
                      identified by URL or post ID rather than by title.

Usage:
  python meta_refresh_forecast.py            # dry run — shows what would be re-forecast
  python meta_refresh_forecast.py --submit   # submits a refresh BATCH to Anthropic (24h turnaround)
  python meta_refresh_forecast.py --check    # retrieves completed refresh batch results
  python meta_refresh_forecast.py --single   # refresh ONE question right now (synchronous, no batch wait)

IMPORTANT — post ID vs question ID:
Metaculus posts and the questions inside them have separate ID sequences.
The URL you see and the email links you get use the POST id. Everywhere else
in this codebase, "question_id" actually means id_of_question — the nested
question's own id, which is the one Metaculus's prediction-submission
endpoint requires. They are NOT always the same number.
--single asks you for what you actually have (the URL / post id), fetches it
through MetaculusClient.get_question_by_post_id (the one library method that
is genuinely post-id-based), then reads the real id_of_question back off the
fetched question object for the actual submission.

FIXED (2026-06-29): fetch_question_by_id(), used by --submit/--check for
re-fetching STALE/CLOSING_SOON questions from local history, previously hit
/api2/questions/{question_id}/ using the stored question_id — but that
endpoint's path parameter is keyed by POST id, not question id. This meant
it was silently fetching whatever unrelated question happened to share that
number as its post id (caught live: Q38099 returning a mortgage-rate
question instead of the real AI-moratorium question at post 38766).
fetch_question_by_id now takes post_id and uses get_question_by_post_id —
the same proven method --single already used. meta_batch_forecast.py was
updated to save post_ids alongside question_ids so this data exists going
forward. Local history saved BEFORE this fix has no post_id on file and is
skipped by fetch_question_by_id with a warning rather than guessed at —
those questions will just get freshly forecast next time they come up
through the normal tournament/batch path.

IMPORTANT — --single always authenticates as your PERSONAL account
(mike_iz_, via METACULUS_TOKEN), never the bot's METAC_TOURNAMENT_TOKEN.
This is deliberate and different from the rest of this file: the
"significant change in Community Prediction" emails this flag is built for
are about YOUR predictions specifically, not the bot's separate, unrelated
forecasting history on the same questions. Using the wrong token here would
silently show/refresh the wrong account's forecast.

IMPORTANT — community prediction & your last forecast, fetched live:
get_question_by_post_id() doesn't request question aggregations, and (a real
quirk in the forecasting_tools library itself) that also blanks out your own
forecast history even when the API actually returned it, because both are
parsed in one shared try/except block. --single works around this with its
own direct call to the same legacy api2 detail endpoint already proven
reliable elsewhere in this codebase (update_community_predictions, the old
fetch_question_by_id) — matched by POST id, authenticated as mike_iz_,
parsing community prediction and your own forecast history independently so
one being missing doesn't blank the other.
"""

import asyncio
import json
import os
import re
import glob
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import anthropic
import aiohttp
import requests

load_dotenv()

from forecasting_tools import MetaculusClient, BinaryQuestion
from live_data import detect_data_needs, format_live_data_for_prompt
from cached_llm import build_forecaster_system_prompt
from meta_prompt_cache import cacheable_system_block
from meta_research import research_question

client_anthropic = anthropic.Anthropic()

# Now using mike_iz_-bot's token for all automated forecasting (cleared by
# Metaculus support for general use, not just tournaments). Falls back to
# METACULUS_TOKEN if METAC_TOURNAMENT_TOKEN isn't set, so this doesn't break
# if .env hasn't been updated yet.
# NOTE: this is used by the BATCH/REFRESH paths (--submit/--check/main), which
# legitimately run as the bot. --single deliberately does NOT use this — see
# personal_client below and the module docstring.
ACTIVE_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if os.getenv("METAC_TOURNAMENT_TOKEN"):
    print("Auth: using METAC_TOURNAMENT_TOKEN (mike_iz_-bot)")
else:
    print("Auth: METAC_TOURNAMENT_TOKEN not set — falling back to METACULUS_TOKEN (mike_iz_)")
client_metaculus = MetaculusClient(token=ACTIVE_TOKEN)

# --single always acts as the PERSONAL account specifically — see module
# docstring for why this is intentionally separate from ACTIVE_TOKEN above.
PERSONAL_TOKEN = os.getenv("METACULUS_TOKEN")
personal_client = MetaculusClient(token=PERSONAL_TOKEN) if PERSONAL_TOKEN else None

# ─── Config ───────────────────────────────────────────────────────────────────
MODEL = "claude-haiku-4-5"
# --single uses Sonnet instead of Haiku: it's a low-volume, manual,
# synchronous path (cost is a non-issue here), and Haiku was confirmed to
# confidently invent specific "verbatim" sourced claims (a fake/coincidental
# Spring 2026 tournament precedent) across three separate runs on the same
# question, despite an explicit anti-fabrication clause already present in
# the system prompt. Sonnet is less prone to this failure mode. The batch
# path (--submit/--check, main()) stays on Haiku/MODEL — unaffected by this
# issue and changing it would have cost implications not yet opted into.
SINGLE_MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 2000
CLOSING_SOON_DAYS = 14
STALE_DAYS = 30
# FIXED 2026-06-30: was "Meta batches" (capital M) — see meta_batch_forecast.py's
# identical fix for the full explanation. This file hadn't been bitten by
# it yet only because it has never run on a case-sensitive (Linux) runner
# either — same latent bug, fixed proactively before it gets the chance.
BATCH_DIR = "meta batches"
REFRESH_BATCH_PREFIX = os.path.join(BATCH_DIR, "batch_jobs_refresh")
REFRESH_RESULTS_PREFIX = os.path.join(BATCH_DIR, "batch_results_refresh")


def ensure_batch_dir():
    os.makedirs(BATCH_DIR, exist_ok=True)


# ─── Pydantic-safe attribute setter ────────────────────────────────────────
def _set_research_text(obj, text) -> None:
    """See meta_batch_forecast.py's identical helper for the full
    explanation — plain attribute assignment crashed BinaryQuestion in a
    live GitHub Actions run; this fixes it here too, since build_refresh_prompt
    below has the same assignment pattern."""
    try:
        obj.research_text_at_access_time = text
    except Exception:
        object.__setattr__(obj, "research_text_at_access_time", text)


# ─── Sliding community weight ─────────────────────────────────────────────────
def community_weight(days_remaining: float, total_days: float) -> float:
    """Return a 0→1 weight for how much to defer to the community prediction.
    Stays low for most of the question's life, then rises sharply in the
    final third via a sigmoid curve."""
    if total_days <= 0:
        return 0.95  # no lifetime info — assume near close, defer heavily
    elapsed = 1 - (days_remaining / total_days)
    import math
    w = 1 / (1 + math.exp(-6 * (elapsed - 0.7)))
    return round(w, 2)


def build_community_context(
    days_remaining: float, total_days: float, cp: float | None,
    has_live_data: bool = True,
) -> str:
    """Return a prompt fragment instructing the model how much to weight the
    community prediction, scaling with how close the question is to closing.

    has_live_data: whether live_data.py actually returned anything for this
    question (it only covers crypto/stock/index/FRED keywords — most
    politics/sports/legal/geopolitics questions get nothing). Note: this
    flag alone no longer means "no real grounding" — meta_research.py's
    research_question() (native Claude web search, wired into this file's
    build_refresh_prompt below) can independently supply real grounding
    for exactly the kinds of questions live_data.py misses. Callers should
    pass has_real_grounding (live_data OR research), not has_live_data
    alone, when deciding how hard to anchor to CP.
    """
    if cp is None:
        return ""
    pct = f"{cp:.0%}"

    if not has_live_data:
        return (
            f"\nCurrent community prediction: {pct}. "
            "IMPORTANT: you have NO live data, news, or search results for "
            "this question — only the static background/resolution text "
            "above, which was frozen at question-creation time. You cannot "
            "see anything that has happened since then. The community "
            "prediction, by contrast, is made by real people reacting to "
            "real, current events as they happen. Stay within 10 "
            "percentage points of the community prediction unless the "
            "background/resolution text above gives you a specific, "
            "concrete reason to diverge — do not diverge based on general "
            "reasoning or instinct alone, since you are working from "
            "stale information and the community is not.\n"
        )

    w = community_weight(days_remaining, total_days)
    if w < 0.10:
        return (
            f"\nCurrent community prediction: {pct}. "
            "Form your own independent view — you are early enough that your "
            "forecast contributes meaningfully to the aggregation. Note if you "
            "diverge by more than 10% and explain why.\n"
        )
    elif w < 0.40:
        return (
            f"\nCurrent community prediction: {pct} (moderate weight). "
            "Give it meaningful weight alongside your own analysis. "
            "It reflects growing aggregated information. Explain any divergence "
            "greater than 10%.\n"
        )
    elif w < 0.75:
        return (
            f"\nCurrent community prediction: {pct} (high weight). "
            "Anchor close to this — the community has aggregated substantial "
            "information at this stage. Only deviate if the background or "
            "resolution text above gives a clear, specific reason the "
            "community hasn't priced in yet.\n"
        )
    else:
        return (
            f"\nCurrent community prediction: {pct} (very high weight — {w:.0%}). "
            "Stay within 5-10 percentage points of this unless the background "
            "or resolution text above gives something genuinely explosive and "
            "specific that is clearly not yet reflected. At this late stage "
            "the community aggregation is far more reliable than your own "
            "static, frozen-at-creation-time information.\n"
        )


# ─── Load full forecast history ───────────────────────────────────────────────
def _build_probability_index() -> dict[str, float]:
    """Scan every results file in BATCH_DIR once and build a single
    custom_id -> probability lookup. This replaces the old per-job-file
    filename guess (job_file.replace('batch_jobs', 'batch_results')),
    which silently failed whenever a results file was saved under a
    different timestamp than its matching jobs file (e.g. --check run
    well after --submit). That failure caused every forecast in the
    affected batch to read probability=None, making them invisible to
    find_questions_to_refresh even when their resolve_time was fine."""
    index: dict[str, float] = {}
    results_files = (
        glob.glob(os.path.join(BATCH_DIR, "batch_results_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_results_refresh_*.json"))
    )
    for rf in results_files:
        try:
            with open(rf) as f:
                results = json.load(f)
            for custom_id, r in results.items():
                if r.get("status") == "success" and r.get("probability") is not None:
                    index.setdefault(custom_id, r["probability"])
        except Exception as e:
            print(f"  Warning: could not load {rf}: {e}")
    return index


def load_all_batches() -> list[dict]:
    """Load all batch_jobs*.json files from Meta batches/ folder."""
    all_forecasts = []

    job_files = (
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )

    if not job_files:
        print(f"No batch job files found in {BATCH_DIR}/")
        return []

    # Build the probability lookup ONCE across all results files, rather
    # than guessing one filename per job file (see _build_probability_index
    # docstring for why the old approach silently dropped questions).
    probability_index = _build_probability_index()

    for job_file in sorted(job_files):
        try:
            with open(job_file) as f:
                batch_info = json.load(f)

            submitted_at    = batch_info.get("submitted_at", "")
            question_ids    = batch_info.get("question_ids", {})
            question_texts  = batch_info.get("question_texts", {})
            resolve_times   = batch_info.get("resolve_times", {})
            categories      = batch_info.get("categories", {})
            community_preds = batch_info.get("community_predictions", {})

            post_ids = batch_info.get("post_ids", {})

            for custom_id, q_id in question_ids.items():
                all_forecasts.append({
                    "custom_id":      custom_id,
                    "question_id":    q_id,
                    "post_id":        post_ids.get(custom_id),  # None for pre-fix history
                    "question_text":  question_texts.get(custom_id, ""),
                    "submitted_at":   submitted_at,
                    "resolve_time":   resolve_times.get(custom_id),
                    "category":       (categories.get(custom_id) or [""])[0],
                    "community_pred": community_preds.get(custom_id),
                    "probability":    probability_index.get(custom_id),
                    "source_file":    job_file,
                })
        except Exception as e:
            print(f"  Warning: could not load {job_file}: {e}")

    print(f"Loaded {len(all_forecasts)} forecasts from {len(job_files)} batch file(s)")
    return all_forecasts


# ─── Identify questions needing refresh ───────────────────────────────────────
def find_questions_to_refresh(all_forecasts: list[dict]) -> tuple[list[dict], list[dict]]:
    now = datetime.now(timezone.utc)
    closing_soon = []
    stale = []
    seen_question_ids = set()

    sorted_forecasts = sorted(
        all_forecasts,
        key=lambda x: x.get("submitted_at") or "",
        reverse=True
    )

    for f in sorted_forecasts:
        q_id = f["question_id"]
        if q_id in seen_question_ids:
            continue

        if f["probability"] is None:
            # Don't mark as seen yet — this is just the newest entry for
            # this question_id, and it happens to lack a usable probability
            # (e.g. a results file that didn't line up with its jobs file).
            # An older entry for the same question_id, later in this sorted
            # list, may still have a valid probability — let it through
            # instead of permanently dropping the question here.
            continue
        seen_question_ids.add(q_id)

        resolve_time_str = f.get("resolve_time")
        submitted_at_str = f.get("submitted_at")

        resolve_time = None
        if resolve_time_str:
            try:
                resolve_time = datetime.fromisoformat(resolve_time_str.replace("Z", "+00:00"))
            except Exception:
                pass

        submitted_at = None
        if submitted_at_str:
            try:
                submitted_at = datetime.fromisoformat(submitted_at_str)
                if submitted_at.tzinfo is None:
                    submitted_at = submitted_at.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        if resolve_time:
            days_to_close = (resolve_time - now).days
            if 0 <= days_to_close <= CLOSING_SOON_DAYS:
                f["days_to_close"] = days_to_close
                f["refresh_reason"] = f"closing in {days_to_close} days"
                closing_soon.append(f)
                continue
            elif days_to_close < 0:
                continue

        if submitted_at:
            age_days = (now - submitted_at).days
            if age_days >= STALE_DAYS:
                f["age_days"] = age_days
                f["refresh_reason"] = f"forecast is {age_days} days old"
                stale.append(f)

    return closing_soon, stale


# ─── Fetch fresh question data from Metaculus ─────────────────────────────────
from meta_question_matching import titles_match


async def fetch_question_by_id(
    post_id: int | None, question_id: int, expected_text: str | None = None
) -> BinaryQuestion | None:
    """Fetch a question by its POST id, using MetaculusClient.get_question_by_post_id
    — the same method --single already uses successfully (see module docstring).

    Previously this hit /api2/questions/{question_id}/ directly with the
    stored question_id (id_of_question). That endpoint's path parameter is
    actually keyed by POST id, not question id — so it was silently fetching
    whatever unrelated question happened to have that number as its post id
    (this is the bug Mike found: Q38099 returning a mortgage-rate question
    instead of the AI moratorium question, whose real post id is 38766).

    post_id is None for any local history saved before this fix — those
    entries have no reliable way to be re-fetched and are skipped rather
    than risking another silent mismatch.

    If expected_text is provided (the title we have on file from when we
    originally forecast it), this also verifies the fetched question's
    title actually matches before returning it — a safety net in case the
    post id on file doesn't correspond to the question we think it does."""
    if post_id is None:
        print(f"    ⚠️  Q{question_id}: no post_id on file (forecast predates the "
              f"post_id fix) — skipping rather than guessing. Will be re-forecast "
              f"fresh next time this question is fetched from the tournament.")
        return None
    try:
        result = client_metaculus.get_question_by_post_id(post_id)
        question = result[0] if isinstance(result, list) else result
        if not isinstance(question, BinaryQuestion):
            print(f"    ⚠️  Post {post_id} is not a binary question "
                  f"({type(question).__name__}). Skipping.")
            return None
        fetched_title = question.question_text or ""
        if expected_text and not titles_match(expected_text, fetched_title):
            print(f"    🛑 MISMATCH on post {post_id} (Q{question_id}): stored title vs API title don't match.")
            print(f"       Stored:  {expected_text[:90]}")
            print(f"       API:     {fetched_title[:90]}")
            print(f"       Skipping — will NOT forecast on the wrong question.")
            return None
        return question
    except Exception as e:
        print(f"  Warning: could not fetch post {post_id} (Q{question_id}): {e}")
        return None


# ─── Build prompt ─────────────────────────────────────────────────────────────
def build_refresh_prompt(
    question: BinaryQuestion,
    original_prob: float | None,
    refresh_reason: str,
    days_to_close: float = 30,
    total_days: float = 365,
) -> str:
    live_data = detect_data_needs(question.question_text)
    live_data_text = format_live_data_for_prompt(live_data)
    has_live_data = bool(live_data)  # live_data.py only covers crypto/stock/
    # index/FRED keywords — empty dict means no real current information at
    # all for this question from that source alone.

    # Added 2026-06-30: was previously missing entirely from this file —
    # tournament_forecast.py and meta_batch_forecast.py both got real-time
    # web search research wired in, but refresh (this file, both --submit
    # batch and --single) had no research call at all, meaning refreshed
    # forecasts on non-live_data questions (most politics/sports/legal/
    # geopolitics questions) were still reasoning from static,
    # frozen-at-creation-time background text only, exactly the failure
    # mode this fix addresses everywhere else.
    research_text = research_question(question.question_text, question.background_info or "")
    has_research = research_text is not None
    research_block = (
        f"\nCURRENT RESEARCH (real-time web search, fetched for this question):\n{research_text}\n"
        if has_research else ""
    )
    # Stashed for submit_refresh_batch to persist below — same pattern as
    # meta_batch_forecast.py and as community_prediction_at_access_time.
    _set_research_text(question, research_text)

    # Either source counts as real grounding for anchoring purposes — see
    # build_community_context's docstring for why has_live_data alone is no
    # longer the right signal now that research_question exists.
    has_real_grounding = has_live_data or has_research

    cp = getattr(question, 'community_prediction_at_access_time', None)
    community = build_community_context(days_to_close, total_days, cp, has_real_grounding)

    if original_prob is not None:
        original_note = f"\nNote: This question was previously forecast at {original_prob:.0%}. Review whether this remains appropriate given current information.\n"
    else:
        original_note = "\nNote: No prior forecast on file for this question — treat this as a fresh, independent forecast.\n"

    no_data_note = ""
    if not has_real_grounding:
        no_data_note = (
            "\nNOTE: No live market data and no research results were found "
            "for this question. You have no current information beyond the "
            "static background/resolution text above.\n"
        )

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

{live_data_text}
{research_block}
{no_data_note}
{community}
{original_note}

Refresh reason: {refresh_reason}
Today is {datetime.now().strftime("%Y-%m-%d")}.

Before answering write:
(a) Time left until resolution
(b) Status quo outcome if nothing changes
(c) Scenario for NO outcome
(d) Scenario for YES outcome
(e) Base rate. HARD RULE: do not reference any specific past tournament
    edition, named forecaster, or quoted/paraphrased tournament outcome
    UNLESS it appears verbatim in the Background, Resolution criteria, or
    Research sections above. Do not invent or recall a "Spring" edition, a
    prior head-to-head result, or any other specific precedent from memory —
    you do not have reliable knowledge of this tournament's history beyond
    what is given above. If no real base-rate data is given above, write
    exactly: "No reliable base rate available — proceeding on priors." and
    move on.
(f) How the live data/research/background above (NOT general news — you
    have none unless explicitly given above) moves you from base rate
(g) How your view has changed (or not) since the original forecast
(h) If community prediction exists and differs >10%, explain why you diverge

The last thing you write is: "Probability: ZZ%"
"""


# ─── Submit refresh batch (BATCH mode — 24h turnaround) ──────────────────────
async def submit_refresh_batch(to_refresh: list[dict]):
    ensure_batch_dir()
    print(f"\nFetching fresh data for {len(to_refresh)} questions...")

    system_prompt = build_forecaster_system_prompt()
    # See meta_prompt_cache.py — same caching treatment as
    # meta_batch_forecast.py's submit_batch, NOT applied to --single
    # below (call_claude_single), which is a one-off call that would
    # never recover the cache-write premium.
    cached_system = cacheable_system_block(system_prompt)
    requests = []
    question_map = {}

    for i, forecast in enumerate(to_refresh):
        q_id = forecast["question_id"]
        print(f"  [{i+1}/{len(to_refresh)}] Fetching Q{q_id}...")

        question = await fetch_question_by_id(
            forecast.get("post_id"), q_id, expected_text=forecast.get("question_text")
        )
        # Always pause between fetch attempts, success or failure — a run of
        # 404s/mismatches previously fired back-to-back with zero delay
        # (the old sleep only sat on the success path below), which is what
        # tripped Metaculus's rate limiting in practice. Bumped to 1.5s after
        # 0.5s still wasn't enough headroom across a run of ~15+ requests.
        await asyncio.sleep(1.5)
        if question is None:
            print(f"    ⚠️  Could not fetch Q{q_id} (or title mismatch — see above), skipping")
            continue

        custom_id = f"refresh_{q_id}_{datetime.now().strftime('%Y%m%d')}"

        # Compute question lifetime for the sliding community weight
        days_to_close = float(forecast.get("days_to_close", 30))
        resolve_time_str = forecast.get("resolve_time")
        resolve_time = None
        if resolve_time_str:
            try:
                resolve_time = datetime.fromisoformat(resolve_time_str.replace("Z", "+00:00"))
            except Exception:
                pass
        open_time = getattr(question, 'open_time', None) or getattr(question, 'created_time', None)
        if resolve_time and open_time:
            total_days = max(float((resolve_time - open_time).days), 1.0)
        else:
            total_days = 365.0  # safe fallback

        question_map[custom_id] = {
            "question":               question,
            "original_prob":          forecast["probability"],
            "refresh_reason":         forecast.get("refresh_reason", ""),
            "question_id":            q_id,
            "original_question_text": forecast.get("question_text", ""),
            # Carried forward from local history. The live scheduled_resolution_time
            # fetched just above is unreliable: when Metaculus hasn't set a real
            # scheduled resolution date yet, forecasting_tools returns a generic
            # placeholder (seen in practice as 2028-01-01T13:00:00) instead of None,
            # which would otherwise silently corrupt a question's resolve_time on
            # every refresh. We already know a good resolve_time from when this
            # question was first forecast, so prefer that unless we don't have one.
            "known_resolve_time":     resolve_time_str,
        }

        prompt = build_refresh_prompt(
            question=question,
            original_prob=forecast["probability"],
            refresh_reason=forecast.get("refresh_reason", ""),
            days_to_close=days_to_close,
            total_days=total_days,
        )

        requests.append({
            "custom_id": custom_id,
            "params": {
                "model": MODEL,
                "max_tokens": MAX_TOKENS,
                "system": cached_system,
                "messages": [{"role": "user", "content": prompt}]
            }
        })

    if not requests:
        print("No questions to submit after fetching.")
        return

    print(f"\nSubmitting refresh batch of {len(requests)} requests...")
    batch = client_anthropic.messages.batches.create(requests=requests)
    batch_id = batch.id

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    batch_file = f"{REFRESH_BATCH_PREFIX}_{timestamp}.json"

    batch_info = {
        "batch_id":     batch_id,
        "submitted_at": datetime.now().isoformat(),
        "batch_type":   "refresh",
        "num_requests": len(requests),
        "question_ids": {
            custom_id: info["question_id"]
            for custom_id, info in question_map.items()
        },
        "question_texts": {
            custom_id: info.get("original_question_text") or info["question"].question_text
            for custom_id, info in question_map.items()
        },
        "original_probabilities": {
            custom_id: info["original_prob"]
            for custom_id, info in question_map.items()
        },
        "refresh_reasons": {
            custom_id: info["refresh_reason"]
            for custom_id, info in question_map.items()
        },
        "community_predictions": {
            custom_id: getattr(info["question"], 'community_prediction_at_access_time', None)
            for custom_id, info in question_map.items()
        },
        # See meta_batch_forecast.py for why — same dashboard/raw-view
        # persistence fix, applied here too.
        "research_texts": {
            custom_id: getattr(info["question"], 'research_text_at_access_time', None)
            for custom_id, info in question_map.items()
        },
        "resolve_times": {
            custom_id: (
                info["known_resolve_time"]
                or (info["question"].scheduled_resolution_time.isoformat()
                    if info["question"].scheduled_resolution_time else None)
            )
            for custom_id, info in question_map.items()
        },
    }

    with open(batch_file, "w") as f:
        json.dump(batch_info, f, indent=2)

    print(f"✅ Refresh batch submitted: {batch_id}")
    print(f"   Saved to {batch_file}")
    print(f"   Run: python meta_refresh_forecast.py --check to retrieve results")


# ─── Check refresh batch ──────────────────────────────────────────────────────
async def check_refresh_batch():
    refresh_files = sorted(
        glob.glob(f"{REFRESH_BATCH_PREFIX}_*.json"),
        reverse=True
    )
    if not refresh_files:
        print(f"No refresh batch files found in {BATCH_DIR}/. Run with --submit first.")
        return

    batch_file = refresh_files[0]
    print(f"Checking: {batch_file}")

    with open(batch_file) as f:
        batch_info = json.load(f)

    batch_id = batch_info["batch_id"]
    batch = client_anthropic.messages.batches.retrieve(batch_id)
    print(f"Status: {batch.processing_status}")
    print(f"Counts: {batch.request_counts}")

    if batch.processing_status != "ended":
        print("Batch not ready yet. Check back later.")
        return

    print("\nBatch complete! Processing results...")

    results = {}
    total_cache_read = 0
    total_cache_write = 0
    for result in client_anthropic.messages.batches.results(batch_id):
        custom_id = result.custom_id

        if result.result.type == "succeeded":
            text = result.result.message.content[0].text
            usage = getattr(result.result.message, "usage", None)
            if usage is not None:
                total_cache_read += getattr(usage, "cache_read_input_tokens", 0) or 0
                total_cache_write += getattr(usage, "cache_creation_input_tokens", 0) or 0
            prob = None
            for line in reversed(text.split('\n')):
                if 'probability:' in line.lower():
                    numbers = re.findall(r'\d+\.?\d*', line)
                    if numbers:
                        prob = float(numbers[-1]) / 100
                        prob = max(0.01, min(0.99, prob))
                        break

            original = batch_info.get("original_probabilities", {}).get(custom_id)
            change = round((prob - original) * 100, 1) if prob and original else None
            change_str = f"{'+' if change >= 0 else ''}{change}pt" if change is not None else "n/a"

            q_id = batch_info["question_ids"][custom_id]
            q_text = batch_info.get("question_texts", {}).get(custom_id) or \
                     next((v for k, v in batch_info.get("question_texts", {}).items()
                           if batch_info.get("question_ids", {}).get(k) == q_id), "Unknown question")

            results[custom_id] = {
                "question_id":    q_id,
                "question_text":  q_text,
                "probability":    prob,
                "original_prob":  original,
                "change_pts":     change,
                "refresh_reason": batch_info.get("refresh_reasons", {}).get(custom_id, ""),
                "reasoning":      text,
                "research_text":  batch_info.get("research_texts", {}).get(custom_id),
                "status":         "success"
            }
            print(f"  Q{q_id}: {original:.0%} → {prob:.0%} ({change_str}) — {q_text[:50]}")
        else:
            q_id = batch_info["question_ids"][custom_id]
            q_text = batch_info.get("question_texts", {}).get(custom_id, "Unknown question")
            results[custom_id] = {
                "question_id":   q_id,
                "question_text": q_text,
                "probability":   None,
                "status":        "failed",
                "research_text": batch_info.get("research_texts", {}).get(custom_id),
            }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    results_file = f"{REFRESH_RESULTS_PREFIX}_{timestamp}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved results to {results_file}")
    if total_cache_read or total_cache_write:
        print(f"  💰 Prompt cache: {total_cache_read} tokens read, "
              f"{total_cache_write} tokens written across this batch")
    else:
        print(f"  ⚠️  Prompt cache: 0 read, 0 written — caching isn't engaging "
              f"(system prompt may be under Haiku 4.5's 4,096-token minimum).")

    await submit_to_metaculus(results)


# ─── Submit to Metaculus (shared by batch + single paths) ────────────────────
async def submit_to_metaculus(results: dict):
    print(f"\nSubmitting updated forecasts to Metaculus...")
    submitted = 0
    failed = 0

    for custom_id, result in results.items():
        if result["status"] != "success" or result["probability"] is None:
            failed += 1
            continue
        try:
            client_metaculus.post_binary_question_prediction(
                question_id=result["question_id"],
                prediction_in_decimal=result["probability"]
            )
            submitted += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            err = str(e)
            if "already closed" in err or "405" in err:
                print(f"  ⏭️  Q{result['question_id']}: already closed, skipping")
            else:
                print(f"  ❌ Q{result['question_id']}: {err[:60]}")
            failed += 1

    print(f"\nSubmitted: {submitted} | Failed: {failed}")


# ─── SINGLE mode: refresh one question right now, by URL or post ID ──────────
def parse_post_id(raw: str) -> int | None:
    """Accept either a bare numeric post ID, or a full Metaculus URL like
    https://www.metaculus.com/questions/12345/some-slug/ — pulls the post ID
    out either way. Returns None if nothing usable was found."""
    raw = raw.strip()
    if raw.isdigit():
        return int(raw)
    match = re.search(r"/questions/(\d+)", raw)
    if match:
        return int(match.group(1))
    return None


def call_claude_single(prompt: str) -> tuple[float | None, str]:
    """Synchronous (non-batch) Claude call for the single-question refresh
    path — no 24h batch wait, since the point of --single is reacting now.
    Uses SINGLE_MODEL (Sonnet), not MODEL (Haiku) — see SINGLE_MODEL comment.

    Deliberately NOT using meta_prompt_cache.cacheable_system_block here —
    see that module's docstring. This is a one-off manual call; the cache
    write premium (1.25x base input price) would never be recovered by a
    subsequent cached read, so caching here would cost slightly MORE."""
    system_prompt = build_forecaster_system_prompt()
    response = client_anthropic.messages.create(
        model=SINGLE_MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text
    prob = None
    for line in reversed(text.split("\n")):
        if "probability:" in line.lower():
            numbers = re.findall(r"\d+\.?\d*", line)
            if numbers:
                prob = float(numbers[-1]) / 100
                prob = max(0.01, min(0.99, prob))
                break
    return prob, text


def _save_single_result(
    post_id: int,
    q_id: int,
    question: BinaryQuestion,
    prob: float,
    original_prob: float | None,
    refresh_reason: str,
    reasoning: str,
):
    """Write a one-entry batch_jobs/batch_results pair in the same schema the
    rest of the codebase uses, so the dashboard and future refresh-eligibility
    scans pick this up automatically. Records post_id AND account alongside
    question_id — existing history has neither, but every new entry from
    here on will, so future --single lookups can tell at a glance whether a
    locally-found forecast was actually yours or the bot's."""
    ensure_batch_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    custom_id = f"single_{q_id}_{timestamp}"

    batch_info = {
        "batch_id": custom_id,
        "submitted_at": datetime.now().isoformat(),
        "batch_type": "single",
        "account": "personal",  # --single always submits as mike_iz_ — see module docstring
        "num_requests": 1,
        "question_ids": {custom_id: q_id},
        "post_ids": {custom_id: post_id},
        "question_texts": {custom_id: question.question_text},
        "original_probabilities": {custom_id: original_prob},
        "refresh_reasons": {custom_id: refresh_reason},
        "community_predictions": {
            custom_id: getattr(question, "community_prediction_at_access_time", None)
        },
        "resolve_times": {
            custom_id: question.scheduled_resolution_time.isoformat()
            if question.scheduled_resolution_time else None
        },
    }
    jobs_file = os.path.join(BATCH_DIR, f"batch_jobs_{timestamp}.json")
    with open(jobs_file, "w") as f:
        json.dump(batch_info, f, indent=2)

    results = {
        custom_id: {
            "question_id": q_id,
            "post_id": post_id,
            "account": "personal",
            "question_text": question.question_text,
            "question_type": "binary",
            "probability": prob,
            "submitted_forecast": prob,
            "original_prob": original_prob,
            "refresh_reason": refresh_reason,
            "reasoning": reasoning,
            # Was previously only saved in the jobs file (batch_info above),
            # making the results file look like CP data was lost/missing
            # whenever someone audited it on its own. Saved here too now so
            # the results file is self-contained.
            "community_prediction": getattr(
                question, "community_prediction_at_access_time", None
            ),
            "research_text": getattr(question, "research_text_at_access_time", None),
            "status": "success",
        }
    }
    results_file = os.path.join(BATCH_DIR, f"batch_results_{timestamp}.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"  Saved to {results_file}")


def _latest_centers(agg_bucket) -> list | None:
    """Given one aggregation bucket (e.g. aggregations['recency_weighted']),
    return its centers list — preferring the 'latest' snapshot, falling back
    to the most recent entry in 'history' when 'latest' is null. Confirmed in
    practice: Metaculus doesn't always keep 'latest' populated even when the
    community prediction is known and shown elsewhere (e.g. email alerts)."""
    if not isinstance(agg_bucket, dict):
        return None
    latest = agg_bucket.get("latest")
    if isinstance(latest, dict):
        centers = latest.get("centers")
        if isinstance(centers, list) and centers:
            return centers
    history = agg_bucket.get("history")
    if isinstance(history, list) and history:
        last_entry = history[-1]
        if isinstance(last_entry, dict):
            centers = last_entry.get("centers")
            if isinstance(centers, list) and centers:
                return centers
    return None


def _safe_dig(d, *keys):
    """Walk a chain of dict keys, returning None the moment anything along
    the way isn't a dict — including when a key is PRESENT but explicitly
    null (Metaculus does this, e.g. "latest": null rather than omitting the
    key), which plain chained .get() calls don't protect against: .get()
    only supplies a default for a MISSING key, not a present-but-None one,
    so the next .get() in the chain throws AttributeError on None."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def fetch_live_personal_context(
    post_id: int, expected_title: str
) -> tuple[float | None, str | None, float | None]:
    """Direct GET against the legacy api2 detail endpoint (matched by POST id
    — see module docstring), authenticated as the PERSONAL account, to pull
    two things get_question_by_post_id doesn't reliably surface: your own
    last forecast on this question, and the current community prediction.
    Parsed independently via _safe_dig (not chained .get()/bracket access)
    so a present-but-null value anywhere in either chain can't throw and
    take the other one down with it."""
    if not PERSONAL_TOKEN:
        return None, None, None
    try:
        url = f"https://www.metaculus.com/api2/questions/{post_id}/"
        resp = requests.get(
            url, headers={"Authorization": f"Token {PERSONAL_TOKEN}"}, timeout=20
        )
        if resp.status_code != 200:
            print(f"    (live personal-context fetch: HTTP {resp.status_code})")
            return None, None, None
        data = resp.json()
        if not isinstance(data, dict):
            print(f"    (live personal-context fetch: unexpected response shape "
                  f"{type(data).__name__}, raw: {resp.text[:200]!r})")
            return None, None, None

        fetched_title = data.get("title") or _safe_dig(data, "question", "title") or ""
        if expected_title and not titles_match(expected_title, fetched_title):
            print(f"    ⚠️  api2 lookup for post {post_id} returned a different title — "
                  f"skipping live personal-context (proceeding without it).")
            print(f"       Expected: {expected_title[:90]}")
            print(f"       Got:      {fetched_title[:90]}")
            return None, None, None

        my_prob = my_ts = None
        history = _safe_dig(data, "question", "my_forecasts", "history")
        if history:
            latest = history[-1] if isinstance(history, list) else None
            if isinstance(latest, dict):
                values = latest.get("forecast_values")
                if isinstance(values, list) and len(values) > 1:
                    my_prob = values[1]
                start_time = latest.get("start_time")
                if start_time is not None:
                    try:
                        my_ts = datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat()
                    except (TypeError, ValueError, OSError):
                        pass

        cp = None
        rw_bucket = _safe_dig(data, "question", "aggregations", "recency_weighted")
        centers = _latest_centers(rw_bucket)
        if not centers:
            uw_bucket = _safe_dig(data, "question", "aggregations", "unweighted")
            centers = _latest_centers(uw_bucket)
        if centers:
            cp = centers[1] if len(centers) > 1 else centers[0]

        # Note: recency_weighted (and unweighted) sometimes comes back fully
        # null across history/latest/score_data/movement for a given
        # question via this endpoint — confirmed in practice, not a parsing
        # bug. When that happens cp is genuinely unavailable; the prompt
        # just proceeds without community-prediction context.

        return my_prob, my_ts, cp
    except Exception as e:
        print(f"    (live personal-context fetch failed: {e})")
        return None, None, None


def run_single():
    """python meta_refresh_forecast.py --single
    Prompts for a Metaculus URL or post ID, fetches it via the post-id-based
    get_question_by_post_id AS YOUR PERSONAL ACCOUNT (mike_iz_ — see module
    docstring for why this is deliberately not the bot token), shows your
    real last forecast on this question next to the current community
    prediction, gets a fresh forecast from Claude synchronously, confirms,
    then submits."""
    if personal_client is None:
        print("  ❌ METACULUS_TOKEN not set in .env — --single needs your PERSONAL "
              "account's token specifically (these refresh emails are about mike_iz_'s "
              "own predictions, not the bot's).")
        return

    print("Auth: --single acts as mike_iz_ (personal) via METACULUS_TOKEN — "
          "not mike_iz_-bot, even if METAC_TOURNAMENT_TOKEN is also set.")

    raw = input("Paste the Metaculus question URL or post ID: ").strip()
    post_id = parse_post_id(raw)
    if post_id is None:
        print(f"Could not find a post ID in '{raw}'. Paste the full URL or just the numeric ID.")
        return

    print(f"Fetching post {post_id} from Metaculus (as mike_iz_)...")
    try:
        result = personal_client.get_question_by_post_id(post_id)
    except Exception as e:
        print(f"  ❌ Could not fetch post {post_id}: {e}")
        return

    question = result[0] if isinstance(result, list) else result
    if not isinstance(question, BinaryQuestion):
        print(f"  ⚠️  Post {post_id} is not a single binary question "
              f"({type(question).__name__}). --single currently only supports binary questions.")
        return

    q_id = question.id_of_question  # the id actually used for submission — not the post id
    confirmed_post_id = question.id_of_post or post_id
    print(f"  Found: {question.question_text}")
    print(f"  (post id {confirmed_post_id} -> question id {q_id})")

    # DEBUG: print the raw background_info exactly as fetched, so we can
    # verify ground truth ourselves instead of trusting the model's claims
    # about what is/isn't "in the background." Remove once resolved.
    print(f"\n  {'─'*60}")
    print("  DEBUG — raw background_info as fetched from Metaculus:")
    print(f"  {'─'*60}")
    print(f"  {question.background_info!r}")
    print(f"  {'─'*60}\n")

    my_prob, my_ts, cp = fetch_live_personal_context(confirmed_post_id, question.question_text)
    question.community_prediction_at_access_time = cp  # used by build_refresh_prompt below

    original_prob = my_prob
    source = "live (mike_iz_'s own forecast history)"
    if original_prob is None:
        # Fall back to local batch history — but that only ever contains
        # BOT-submitted forecasts (your own manual predictions made via the
        # website are never logged locally), and now title-checked, which
        # the original version of this fallback was missing.
        all_forecasts = load_all_batches()
        prior = [
            f for f in all_forecasts
            if f["question_id"] == q_id and f.get("probability") is not None
            and titles_match(f.get("question_text", ""), question.question_text)
        ]
        prior.sort(key=lambda f: f.get("submitted_at") or "", reverse=True)
        original_prob = prior[0]["probability"] if prior else None
        source = "local file — likely the BOT's forecast, not yours" if original_prob is not None else None

    print(f"  Your last forecast on file: "
          f"{f'{original_prob:.0%} ({source})' if original_prob is not None else 'none found'}")
    print(f"  Current community prediction: "
          f"{f'{cp:.0%}' if cp is not None else 'hidden/unavailable'}")

    resolve_time = question.scheduled_resolution_time
    open_time = getattr(question, "open_time", None) or getattr(question, "created_time", None)
    now = datetime.now(timezone.utc)
    days_to_close = (resolve_time - now).days if resolve_time else 30
    total_days = (
        max(float((resolve_time - open_time).days), 1.0)
        if (resolve_time and open_time) else 365.0
    )

    refresh_reason = (
        "manual single refresh (community prediction shift alert)"
        if original_prob is not None else
        "manual single refresh (no prior forecast on file)"
    )

    prompt = build_refresh_prompt(
        question=question,
        original_prob=original_prob,
        refresh_reason=refresh_reason,
        days_to_close=days_to_close,
        total_days=total_days,
    )

    print("  Asking Claude for an updated forecast...")
    prob, reasoning = call_claude_single(prompt)

    if prob is None:
        print("  ❌ Could not parse a probability from the response. Nothing submitted.")
        return

    change_str = ""
    if original_prob is not None:
        change = round((prob - original_prob) * 100, 1)
        change_str = f" ({'+' if change >= 0 else ''}{change}pt vs your last forecast of {original_prob:.0%})"

    print(f"\n{'─'*60}")
    print("  Full reasoning:")
    print(f"{'─'*60}")
    print(f"  {reasoning}")
    print(f"{'─'*60}")

    print(f"\n  New forecast: {prob:.0%}{change_str}")
    confirm = input("  Submit this to Metaculus (as mike_iz_)? [Y/n]: ").strip().lower()
    if confirm == "n":
        print("  Cancelled — nothing submitted.")
        return

    try:
        personal_client.post_binary_question_prediction(
            question_id=q_id, prediction_in_decimal=prob
        )
        print(f"  ✅ Submitted {prob:.0%} on question {q_id} (post {confirmed_post_id}) as mike_iz_")
    except Exception as e:
        print(f"  ❌ Submission failed: {e}")
        return

    _save_single_result(confirmed_post_id, q_id, question, prob, original_prob, refresh_reason, reasoning)


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main(submit: bool = False):
    all_forecasts = load_all_batches()
    if not all_forecasts:
        return

    closing_soon, stale = find_questions_to_refresh(all_forecasts)

    print(f"\n{'='*55}")
    print(f"CLOSING SOON (within {CLOSING_SOON_DAYS} days): {len(closing_soon)} questions")
    for f in closing_soon:
        print(f"  [{f['days_to_close']}d] {f['probability']:.0%} — {f['question_text'][:60]}")

    print(f"\nSTALE (older than {STALE_DAYS} days): {len(stale)} questions")
    for f in stale[:10]:
        print(f"  [{f['age_days']}d old] {f['probability']:.0%} — {f['question_text'][:60]}")
    if len(stale) > 10:
        print(f"  ... and {len(stale) - 10} more")

    total = len(closing_soon) + len(stale)
    print(f"\nTotal to refresh: {total}")

    if total == 0:
        print("Nothing needs refreshing yet.")
        return

    if not submit:
        print(f"\nDry run — run with --submit to submit a refresh batch")
        print(f"Estimated cost: ~${total * 0.05:.2f} (50% batch discount)")
        return

    to_refresh = closing_soon + stale
    await submit_refresh_batch(to_refresh)


if __name__ == "__main__":
    import sys
    if "--check" in sys.argv:
        asyncio.run(check_refresh_batch())
    elif "--submit" in sys.argv:
        asyncio.run(main(submit=True))
    elif "--single" in sys.argv:
        run_single()
    else:
        asyncio.run(main(submit=False))