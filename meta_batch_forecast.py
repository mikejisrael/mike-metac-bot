"""
meta_batch_forecast.py — formerly batch_forecast.py (renamed to group with the
other meta_*.py Metaculus scripts). No functional changes from the renamed
version other than updated usage strings below pointing at the new filename.
"""

import asyncio
import json
import os
import re
import glob
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import anthropic
import aiohttp

load_dotenv()

from forecasting_tools import (
    MetaculusClient, ApiFilter, BinaryQuestion, MultipleChoiceQuestion, NumericQuestion,
)
import math
from live_data import detect_data_needs, format_live_data_for_prompt
from cached_llm import build_batch_forecaster_system_prompt
from meta_prompt_cache import cacheable_system_block
from meta_cp_extract import extract_live_cp
from meta_alerts import send_alert
from meta_research import research_question
from meta_forecast_gate import MIN_FORECASTERS, DAYS_AHEAD, QUESTION_SERIES_IDS, passes_forecast_gate
import tournament_registry

client_anthropic = anthropic.Anthropic()

# Now using mike_iz_-bot's token for all automated forecasting (cleared by
# Metaculus support for general use, not just tournaments). Falls back to
# METACULUS_TOKEN if METAC_TOURNAMENT_TOKEN isn't set, so this doesn't break
# if .env hasn't been updated yet.
ACTIVE_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if os.getenv("METAC_TOURNAMENT_TOKEN"):
    print("Auth: using METAC_TOURNAMENT_TOKEN (mike_iz_-bot)")
else:
    print("Auth: METAC_TOURNAMENT_TOKEN not set — falling back to METACULUS_TOKEN (mike_iz_)")
client_metaculus = MetaculusClient(token=ACTIVE_TOKEN)

# ─── Fail fast on permanently-closed questions ───────────────────────────────
# FIXED 2026-06-30: same fix as tournament_forecast.py and
# meta_refresh_forecast.py — forecasting_tools' _post_question_prediction
# retries ANY HTTPError 3x with exponential backoff (up to 75s/attempt,
# ~100s+ total). Wasteful for a question that's permanently closed: a 405
# "already closed" response can never succeed no matter how many times
# it's retried. This file never had the fix either — applied here too.
import types as _types

_original_post_question_prediction = type(client_metaculus)._post_question_prediction.__wrapped__

def _post_question_prediction_fail_fast_on_closed(self, question_id, forecast_payload):
    max_retries = 3
    delay = 2.5
    for attempt in range(max_retries + 1):
        try:
            return _original_post_question_prediction(self, question_id, forecast_payload)
        except Exception as e:
            if "already closed to forecasting" in str(e):
                print(f"  ⏭️  Q{question_id}: already closed to forecasting — skipping, no retry.")
                raise
            if attempt >= max_retries:
                raise
            import random as _random
            sleep_time = min(delay * _random.uniform(1, 8.0), 75.0)
            print(f"  Retry {attempt + 1}/{max_retries} for submission after {sleep_time:.1f}s. Error: {e}")
            time.sleep(sleep_time)
            delay *= 3

client_metaculus._post_question_prediction = _types.MethodType(
    _post_question_prediction_fail_fast_on_closed, client_metaculus
)

# ─── Config ───────────────────────────────────────────────────────────────────
MODEL = "claude-haiku-4-5"
MAX_TOKENS = 2000
# FIXED 2026-07-02: dropped from 50 to 20 as a deliberate cost control -
# ALLOWED_TOURNAMENTS is expanding from 3 to 8 tournaments below, so
# holding new-questions-per-run steady rather than letting it scale with
# tournament count keeps the Batch API bill from growing 8/3 = ~2.7x
# alongside it.
# RAISED 2026-07-04 (20 -> 30) after the cost optimization review deferred
# 2026-06-28: research now runs primarily through OpenRouter (separate,
# well-funded credit pool from ben@metaculus.com), and prompt caching is
# now live on this batch path (see cached_llm.build_batch_forecaster_
# system_prompt), so the marginal Anthropic-side cost of more questions
# per run is small. Chosen as a moderate +50% step rather than doubling,
# to observe real cost impact via the Console before going further. A
# higher cap also gives the 5 question_series tournaments more chance to
# get covered in a given run, since the 3 prize-money tournaments
# (fetched first) need to supply more fresh questions before hitting a
# now-higher ceiling.
NUM_QUESTIONS = 30
# CHANGED (2026-07-03): DAYS_AHEAD and MIN_FORECASTERS moved to
# meta_forecast_gate.py — shared with meta_coverage_check.py's gap
# classification, so both scripts can never disagree on what "worth
# forecasting" means. Imported near the top of this file now, not
# redefined here.
# FIXED 2026-06-30: was "Meta batches" (capital M) — harmless on Windows
# (case-insensitive filesystem, so it silently aliased to the real,
# long-established "meta batches" folder used by 50+ historical files
# going back to May 28). But this script had never run on a case-SENSITIVE
# filesystem until today's new GitHub Actions cron workflow (Linux) —
# which created a genuinely SEPARATE, empty "Meta batches" folder there,
# silently bypassing fetch_questions()'s entire dedup history (logged as
# "Excluding 0 already-forecasted questions" — not because there were none,
# but because it was looking in the wrong, empty folder). Lowercase now,
# matching every historical file on disk and in git history.
BATCH_DIR = "meta batches"
BATCH_FILE = os.path.join(BATCH_DIR, "batch_jobs.json")
RESULTS_FILE = os.path.join(BATCH_DIR, "batch_results.json")

# Tournament(s) to pull questions from. ApiFilter.allowed_tournaments accepts
# a list of str|int (numeric ID or slug), so adding more is just adding here.
#
# CHANGED 2026-07-17: now derived from tournament_registry.py — every
# entry there with pipeline="batch" and project_type="tournament" (i.e.
# the ones fetched via ApiFilter's allowed_tournaments, as opposed to the
# 5 question_series ones below via the separate project= path). This is
# what actually wires a new registry entry into live forecasting: adding
# a tournament to tournament_registry.py alone does NOT make it get
# forecasted — this list (or QUESTION_SERIES_IDS below) is what a script
# actually fetches against, and both are now sourced from the registry
# for exactly that reason. US Midterms 2026 ("midterms-2026", $10,000
# prize pool, confirmed 2026-07-17 no bot exclusion) is included via this
# migration, alongside the pre-existing 3:
#   "ACX2026"                    = ACX 2026 Prediction Contest
#   "climate"                    = Climate Tipping Points
#   "metaculus-cup-summer-2026"  = Metaculus Cup Summer 2026 (bots can forecast
#                                  here for calibration data, but are NOT prize-
#                                  eligible in this one — humans-only for prizes)
#   "midterms-2026"              = US Midterms 2026 (prize-eligible for bots,
#                                  unlike Metaculus Cup — see tournament_registry.py)
#
# Cost optimization split (2026-06-30, still applies): FutureEval (33022)
# is NOT in this list — it has 90-minute close windows and needs
# tournament_forecast.py's synchronous path instead. This script's
# tournaments stay on the Batch API path (50% cheaper than synchronous)
# on its own ~every-3-days cron schedule, decoupled from tournament_
# forecast.py's tighter cadence.
ALLOWED_TOURNAMENTS = tournament_registry.slugs_for("batch", "tournament")

# FIXED 2026-07-02: the 5 series added alongside ALLOWED_TOURNAMENTS above
# turned out to be type='question_series' on Metaculus's side, not
# type='tournament' like the original 3. Confirmed live via
# check_project_type.py: ApiFilter's allowed_tournaments sends these IDs as
# a `tournaments=` query param, which returned count=7427 (i.e.
# essentially unfiltered, matching almost the whole site) for Nuclear Risk
# Horizons — silently NOT scoping to the series at all. The raw `project=`
# parameter, by contrast, correctly returned count=37, the real Nuclear
# question set. So these 5 need a separate fetch path (see
# fetch_question_series_questions() below) that uses `project=` directly
# and converts matches via client_metaculus.get_question_by_post_id(),
# rather than going through ApiFilter/allowed_tournaments at all.
#
# CHANGED (2026-07-03): the actual ID list moved to meta_forecast_gate.py
# (imported near the top of this file) — meta_coverage_check.py needs the
# same list to know which tournaments' open-question counts are still on
# the unverified ApiFilter fetch path, and a second hardcoded copy here
# risked drifting out of sync with that one.

# How many open questions to pull per series before client-side filtering —
# generous since these series are individually small (Nuclear Risk Horizons
# was 37 total via check_project_type.py), not a per-run forecast cap
# (NUM_QUESTIONS above still governs that, globally, after merging).
QUESTION_SERIES_FETCH_LIMIT = 100


def ensure_batch_dir():
    os.makedirs(BATCH_DIR, exist_ok=True)


# ─── Pydantic-safe attribute setter ────────────────────────────────────────
def _set_research_text(obj, text) -> None:
    """Set research_text_at_access_time regardless of whether the underlying
    pydantic model declares that field. Added 2026-06-30 after a live
    GitHub Actions run crashed every single forecast in a batch with
    `ValueError: "BinaryQuestion" object has no field
    "research_text_at_access_time"` — plain attribute assignment on a
    pydantic model with strict extra-field validation raises instead of
    silently allowing it. This is the same failure mode tournament_forecast.py's
    _set_cp() defends against for NumericQuestion/MultipleChoiceQuestion —
    except here it hit BinaryQuestion too, since (unlike
    community_prediction_at_access_time, which is apparently a genuinely
    declared field in forecasting_tools) research_text_at_access_time has
    no such declaration anywhere. Tournament_forecast.py's docstring
    claiming "BinaryQuestion allows extra attributes" was an incorrect
    assumption — this crash is the proof."""
    try:
        obj.research_text_at_access_time = text
    except Exception:
        object.__setattr__(obj, "research_text_at_access_time", text)


def _set_research_source(obj, source) -> None:
    """Same pydantic-safe-set pattern as _set_research_text, for the
    provider name ("openrouter" / "anthropic" / None) that produced the
    research. Added 2026-07-04 when meta_research.research_question()
    started supporting multiple providers, so the dashboard can show
    which source backed each forecast."""
    try:
        obj.research_source_at_access_time = source
    except Exception:
        object.__setattr__(obj, "research_source_at_access_time", source)


def _set_cp(obj, cp) -> None:
    """Set community_prediction_at_access_time regardless of whether the
    underlying pydantic model declares that field. Ported 2026-07-21 from
    tournament_forecast_v2.py's identical helper: community_prediction_
    at_access_time is a genuinely declared field on BinaryQuestion (plain
    assignment already worked fine there, which is why this file never
    needed this helper while it was binary-only), but NumericQuestion and
    MultipleChoiceQuestion do NOT declare it and raise on plain
    assignment. Needed now that fetch_questions() prefetches CP for all
    three types, not just binary."""
    try:
        obj.community_prediction_at_access_time = cp
    except Exception:
        object.__setattr__(obj, "community_prediction_at_access_time", cp)


def _question_type_str(q) -> str:
    """Single place that maps a question object to the type string used
    throughout this file (gate checks, CP extraction, prompt dispatch,
    result persistence). Added 2026-07-21 alongside multiple_choice/
    numeric support so every call site agrees on the same three strings
    instead of each branch re-deriving isinstance checks independently."""
    if isinstance(q, MultipleChoiceQuestion):
        return "multiple_choice"
    if isinstance(q, NumericQuestion):
        return "numeric"
    return "binary"


# ─── Question identity guard ────────────────────────────────────────────────
from meta_question_matching import titles_match


# ─── question_series fetch path (2026-07-02) ───────────────────────────────
import requests as _requests

async def fetch_question_series_questions(now: datetime) -> list:
    """Fetches open questions from QUESTION_SERIES_IDS via the raw
    `project=` parameter (proven correct — see check_project_type.py),
    since ApiFilter's allowed_tournaments/`tournaments=` parameter silently
    doesn't scope question_series-type projects. Applies the same
    open/close-time/forecaster-count constraints ApiFilter would normally
    enforce, by hand, client-side (question TYPE is now gated via
    meta_forecast_gate.ALLOWED_QUESTION_TYPES — binary/multiple_choice/
    numeric — not binary-only, as of 2026-07-21). Converts each surviving
    match via client_metaculus.get_question_by_post_id() to get a real
    question object compatible with the rest of this pipeline (research,
    CP extraction, submission all branch on isinstance now)."""
    headers = {"Authorization": f"Token {ACTIVE_TOKEN}"}
    results: list = []

    for series_id in QUESTION_SERIES_IDS:
        try:
            r = _requests.get(
                "https://www.metaculus.com/api2/questions/",
                headers=headers,
                params={"project": series_id, "status": "open", "limit": QUESTION_SERIES_FETCH_LIMIT},
                timeout=30,
            )
        except Exception as e:
            print(f"  ⚠️  question_series {series_id}: fetch failed ({e}) — skipping this series this run.")
            continue
        if r.status_code != 200:
            print(f"  ⚠️  question_series {series_id}: HTTP {r.status_code} — skipping this series this run.")
            continue

        raw_matches = (r.json() or {}).get("results") or []
        candidates = []
        for item in raw_matches:
            q_info = item.get("question", item) or {}
            close_str = item.get("scheduled_close_time") or q_info.get("scheduled_close_time")
            close_dt = None
            if close_str:
                try:
                    close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                except Exception:
                    pass
            # CHANGED (2026-07-03): was three separate inline checks
            # (type, close-time window, forecaster threshold) duplicating
            # what ApiFilter enforces server-side for the other 3
            # tournaments — now the same shared gate function both paths
            # go through, so they can't quietly diverge. close_dt=None
            # (no scheduled_close_time at all) still fails the gate, same
            # as the old code's `if not close_str: continue`.
            if close_dt is None:
                continue
            if not passes_forecast_gate(q_info.get("type"), item.get("nr_forecasters"), close_dt, now=now):
                continue
            candidates.append(item.get("id"))  # post_id

        print(f"  question_series {series_id}: {len(raw_matches)} open, "
              f"{len(candidates)} pass binary/close-time/forecaster filters")

        for post_id in candidates:
            if post_id is None:
                continue
            try:
                # FIXED 2026-07-02: confirmed live — this is synchronous,
                # not async. The docs snippet that led me to assume await
                # was needed didn't actually show it being awaited; the
                # runtime error ("BinaryQuestion can't be used in 'await'
                # expression") only happens when the call already returned
                # the real object, not a coroutine.
                q = client_metaculus.get_question_by_post_id(post_id=post_id)
            except Exception as e:
                print(f"    ⚠️  post {post_id}: could not fetch question object ({e}) — skipping.")
                continue
            if isinstance(q, list):  # group_question_mode default may unpack; guard either way
                results.extend(x for x in q if isinstance(x, (BinaryQuestion, MultipleChoiceQuestion, NumericQuestion)))
            elif isinstance(q, (BinaryQuestion, MultipleChoiceQuestion, NumericQuestion)):
                results.append(q)
            await asyncio.sleep(1.2)  # same politeness delay used elsewhere in this codebase

    return results


# ─── Step 1: Fetch questions ───────────────────────────────────────────────────
async def fetch_questions() -> list:
    # Load previously forecasted question IDs (mapped to the title we forecast
    # them under, so a recycled ID — genuinely a different question on
    # Metaculus's side — isn't silently treated as already-done).
    already_done: dict[int, str] = {}
    for rf in glob.glob(os.path.join(BATCH_DIR, "batch_results_2*.json")):
        try:
            with open(rf) as f:
                data = json.load(f)
            for r in data.values():
                if r.get("question_id"):
                    already_done.setdefault(r["question_id"], r.get("question_text", ""))
        except Exception:
            pass
    print(f"Excluding {len(already_done)} already-forecasted questions...")

    now = datetime.now(timezone.utc)
    api_filter = ApiFilter(
        # CHANGED 2026-07-21: was ["binary"] only — silently excluded every
        # multiple_choice and numeric question at the API level, regardless
        # of tournament (this is what made Q44676, the SC Senate nominee
        # MC question, categorically unreachable). See
        # meta_forecast_gate.ALLOWED_QUESTION_TYPES for the single place
        # this list is now mirrored for the client-side gate check.
        allowed_types=["binary", "multiple_choice", "numeric"],
        allowed_statuses=["open"],
        allowed_tournaments=ALLOWED_TOURNAMENTS,
        close_time_gt=now,
        close_time_lt=now + timedelta(days=DAYS_AHEAD),
        num_forecasters_gte=MIN_FORECASTERS,
    )
    # Fetch as many as available, up to our target + buffer
    fetch_count = NUM_QUESTIONS + len(already_done)
    try:
        questions = await client_metaculus.get_questions_matching_filter(
            api_filter=api_filter,
            num_questions=fetch_count,
        )
    except ValueError as e:
        # The library raises if the count found doesn't EXACTLY match what
        # was requested, instead of just returning what's available. The
        # old fallback here hardcoded num_questions=50 — which hits this
        # exact same error a second time, uncaught, the moment fewer than
        # 50 questions are actually available (confirmed live 2026-06-29:
        # 43 available, fallback asked for 50, crashed the whole run).
        # Parse the real count out of the error message and retry with
        # that exact number instead of guessing.
        import re as _re
        match = _re.search(r"number of questions found \((\d+)\)", str(e))
        if not match:
            print(f"  ⚠️  Could not parse available question count from error — "
                  f"returning no questions this run. Raw error: {e}")
            questions = []
        else:
            actual_count = int(match.group(1))
            print(f"  Requested {fetch_count}, but only {actual_count} questions "
                  f"currently match the filter — retrying with the exact count...")
            if actual_count == 0:
                questions = []
            else:
                questions = await client_metaculus.get_questions_matching_filter(
                    api_filter=api_filter,
                    num_questions=actual_count,
                )
    # CHANGED 2026-07-21: was isinstance(q, BinaryQuestion) only — kept the
    # variable name "binary" for now to minimize the diff below, but it's
    # really "candidates" going forward (holds all three accepted types).
    binary = [q for q in questions if isinstance(q, (BinaryQuestion, MultipleChoiceQuestion, NumericQuestion))]

    # 2026-07-02: separate fetch path for question_series-type projects
    # (see fetch_question_series_questions() docstring for why) — merged in
    # here, BEFORE dedup/NUM_QUESTIONS-cap, so those apply globally across
    # both true tournaments and question_series alike.
    series_questions = await fetch_question_series_questions(now)
    print(f"  question_series fetch: {len(series_questions)} candidate(s) across "
          f"{len(QUESTION_SERIES_IDS)} series")
    binary.extend(series_questions)

    # Filter out already-done questions (title-checked — see titles_match)
    fresh = []
    for q in binary:
        if q.id_of_question in already_done:
            stored_title = already_done[q.id_of_question]
            if titles_match(stored_title, q.question_text):
                continue  # genuine duplicate
            print(f"  🛑 Q{q.id_of_question}: ID previously used for a different title — "
                  f"treating as a NEW question (ID likely recycled).")
            print(f"       Previously: {stored_title[:90]}")
            print(f"       Now:        {q.question_text[:90]}")
        fresh.append(q)
        if len(fresh) >= NUM_QUESTIONS:
            break
    print(f"Fetched {len(binary)} questions, {len(fresh)} are new")

    # Fetch CP BEFORE forecasting (not after) so build_user_prompt's
    # CP-anchoring instructions actually have something to anchor to —
    # previously community_prediction_at_access_time was only ever set
    # post-hoc via --update-community, meaning the anchoring logic was
    # always operating on None. Uses q.api_json (set by the
    # forecasting_tools client on fetch) — field path for binary confirmed
    # working elsewhere in this file (update_community_predictions).
    #
    # CHANGED 2026-07-21: was hardcoded extract_live_cp(..., "binary") and
    # plain q.community_prediction_at_access_time = cp — both assumed
    # binary-only. Now branches per-question via _question_type_str() and
    # uses the pydantic-safe _set_cp() setter, since MC/numeric don't
    # declare this field and raise on plain assignment (see _set_cp
    # docstring).
    cp_found = 0
    for q in fresh:
        q_type = _question_type_str(q)
        cp = extract_live_cp(getattr(q, "api_json", None), q_type)
        _set_cp(q, cp)
        if cp is not None:
            cp_found += 1
    print(f"  Live CP found for {cp_found}/{len(fresh)} questions before forecasting "
          f"(rest will forecast without CP-anchoring this run)")

    return fresh


# ─── Step 2: Build prompts ─────────────────────────────────────────────────────
# CHANGED 2026-07-21: renamed from build_user_prompt to build_binary_prompt
# now that this file has three type-specific prompt builders (binary/
# multiple_choice/numeric) — see build_prompt_for_question() below, which
# dispatches to whichever one matches the question's type.
def build_binary_prompt(question: BinaryQuestion) -> str:
    live_data = detect_data_needs(question.question_text)
    live_data_text = format_live_data_for_prompt(live_data)
    has_live_data = bool(live_data)  # live_data.py only covers crypto/stock/
    # index/FRED keywords — most non-financial questions get nothing here.

    # Opted in to OpenRouter-primary/Anthropic-fallback 2026-07-04 (new
    # OpenRouter credit). tournament_forecast.py deliberately does NOT pass
    # provider_order and so stays on Anthropic-only — see meta_research.py
    # docstring.
    research_text, research_source = research_question(
        question.question_text, question.background_info or "",
        provider_order=["openrouter", "anthropic"], return_source=True,
    )
    has_research = research_text is not None
    research_block = (
        f"\nCURRENT RESEARCH (real-time web search, fetched for this question):\n{research_text}\n"
        if has_research else ""
    )
    # Stashed on the question object (same pattern as
    # community_prediction_at_access_time below) so submit_batch can persist
    # it into batch_info/results JSON without re-running research_question
    # or threading a second return value through this function's signature.
    _set_research_text(question, research_text)
    _set_research_source(question, research_source)

    # Either source counts as "real grounding" for anchoring purposes — a
    # question can have research but no live_data (e.g. politics) or vice
    # versa (e.g. a plain BTC-price question with nothing notable to search).
    has_real_grounding = has_live_data or has_research

    community = ""
    cp = getattr(question, 'community_prediction_at_access_time', None)
    if cp is not None:
        if has_real_grounding:
            community = f"\nCurrent community prediction: {cp:.0%}. If your estimate differs by more than 10%, explain why.\n"
        else:
            community = (
                f"\nCurrent community prediction: {cp:.0%}. "
                "IMPORTANT: you have NO live data, news, or search results for "
                "this question — only the static background/resolution text "
                "above, frozen at question-creation time. The community "
                "prediction reflects real people reacting to real, current "
                "events you cannot see. Stay within 10 percentage points of "
                "it unless the background/resolution text above gives a "
                "specific, concrete reason to diverge.\n"
            )

    no_data_note = ""
    if not has_real_grounding:
        no_data_note = (
            "\nNOTE: No live market data and no research results were found "
            "for this question. You have no current information beyond the "
            "static text above.\n"
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

Today is {datetime.now().strftime("%Y-%m-%d")}.

Before answering write:
(a) Time left until resolution
(b) Status quo outcome if nothing changes
(c) Scenario for NO outcome
(d) Scenario for YES outcome
(e) Base rate — how often do similar events occur? Only cite a specific
    historical precedent, named individual, or past event if it appears
    word-for-word in the Background/Resolution criteria/Research above —
    otherwise say "No reliable base rate available" and proceed on priors.
(f) How the live data/research/background above (NOT general knowledge or
    assumed news — only what's literally given above) moves you from base rate
(g) If community prediction exists and differs >10%, explain why you diverge

The last thing you write is: "Probability: ZZ%"
"""


# ─── Step 2b: Multiple-choice prompt/parse (ported 2026-07-21) ────────────────
# Ported near-verbatim from tournament_forecast_v2.py's build_multiple_choice_
# prompt/parse_multiple_choice_response — deliberately NOT rewritten, since
# the parser encodes a real, previously-live bug fix (Q44216, "≤45" vs
# "Less than or equal to 45" failing to match and zeroing out a genuine
# FutureEval submission) via _normalize_option_text's symbol-to-word
# normalization, plus the iterative clamp-to-[0.001,0.999] logic that a
# single clamp-then-renormalize pass doesn't correctly satisfy. Only
# change from the v2 source: uses this file's research_question/
# _set_research_text/_set_research_source (same functions, same imports,
# already present in this file) instead of duplicating them.
def build_multiple_choice_prompt(question: MultipleChoiceQuestion) -> str:
    options_list = "\n".join(f"  - {opt}" for opt in question.options)

    research_text, research_source = research_question(
        question.question_text, question.background_info or "",
        provider_order=["openrouter", "anthropic"], return_source=True,
    )
    _set_research_text(question, research_text)
    _set_research_source(question, research_source)
    has_research = research_text is not None
    research_block = (
        f"\nCURRENT RESEARCH (real-time web search, fetched for this question):\n{research_text}\n"
        if has_research else ""
    )

    cp = getattr(question, "community_prediction_at_access_time", None)
    community = ""
    if isinstance(cp, dict) and cp:
        cp_lines = "\n".join(f"  {opt}: {p:.0%}" for opt, p in cp.items())
        if has_research:
            community = f"\nCurrent community probabilities:\n{cp_lines}\nIf your estimates differ substantially, explain why.\n"
        else:
            community = (
                f"\nCurrent community probabilities:\n{cp_lines}\n"
                "IMPORTANT: you have no research results for this question — "
                "the community estimate reflects real people with access to "
                "current information you don't have. Stay reasonably close "
                "to it unless the background/resolution text above gives a "
                "specific, concrete reason to diverge.\n"
            )

    no_data_note = ""
    if not has_research:
        no_data_note = (
            "\nNOTE: No research results were found for this question. You "
            "have no current information beyond the static text above.\n"
        )

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

Options:
{options_list}
{research_block}
{no_data_note}
{community}
Today is {datetime.now().strftime("%Y-%m-%d")}.

This is a MULTIPLE CHOICE forecasting question. Reason through each option carefully.

Before answering write:
(a) Time left until resolution
(b) Most likely outcome and why
(c) Key uncertainties that could change the outcome — only cite a specific
    precedent or named source if it appears word-for-word in the
    Background/Resolution criteria/Research above.

Then assign a probability to each option. Probabilities must sum to exactly 100%.
End with exactly this format (one line per option):
Option probabilities:
<option>: <number>%
<option>: <number>%
...
"""


def _normalize_option_text(s: str) -> str:
    """Normalize an option label (or Claude's generated option text) so
    symbolic and worded versions of the same comparison are recognized as
    equal — e.g. '≤45' and 'Less than or equal to 45' must match. This is
    exactly the bug that zeroed out a real FutureEval submission on
    2026-06-29 (Q44216) — ported from tournament_forecast_v2.py."""
    s = s.strip().lower()
    replacements = [
        ("≤", " less than or equal to "),
        ("<=", " less than or equal to "),
        ("≥", " greater than or equal to "),
        (">=", " greater than or equal to "),
        ("<", " less than "),
        (">", " greater than "),
    ]
    for sym, word in replacements:
        s = s.replace(sym, word)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_multiple_choice_response(text: str, options: list[str]) -> dict[str, float] | None:
    """Parse option probabilities from Claude's response. Ported from
    tournament_forecast_v2.py's parse_multiple_choice_response, adapted
    to take `options: list[str]` directly rather than a question object
    (this file's batch --check path reconstructs results from persisted
    batch_info, not a live question object — see submit_batch's
    options_by_question)."""
    lines = text.split('\n')
    start = None
    for i, line in enumerate(lines):
        if 'option probabilities' in line.lower():
            start = i + 1
            break
    if start is None:
        return None

    probs: dict[str, float] = {}
    unmatched_mass = 0.0
    unmatched_examples = []

    for line in lines[start:]:
        line = line.strip()
        if not line:
            continue
        match = re.match(r'^(.+?):\s*(\d+\.?\d*)\s*%?$', line)
        if not match:
            continue
        option_text = match.group(1).strip()
        prob = float(match.group(2)) / 100

        matched_opt = next((opt for opt in options if opt.lower() == option_text.lower()), None)
        if matched_opt is None:
            matched_opt = next(
                (opt for opt in options
                 if opt.lower() in option_text.lower() or option_text.lower() in opt.lower()),
                None
            )
        if matched_opt is None:
            norm_option_text = _normalize_option_text(option_text)
            matched_opt = next(
                (opt for opt in options if _normalize_option_text(opt) == norm_option_text), None
            )
            if matched_opt is None:
                matched_opt = next(
                    (opt for opt in options
                     if _normalize_option_text(opt) in norm_option_text
                     or norm_option_text in _normalize_option_text(opt)),
                    None
                )

        if matched_opt is not None:
            probs[matched_opt] = probs.get(matched_opt, 0.0) + prob
        else:
            unmatched_mass += prob
            unmatched_examples.append(option_text)

    if not probs:
        return None

    if unmatched_mass > 0.05:
        print(f"    ⚠️  {unmatched_mass:.0%} of probability mass couldn't be matched to a known "
              f"option (unrecognized: {unmatched_examples}) — rejecting forecast")
        return None

    for opt in options:
        probs.setdefault(opt, 0.0)
    total = sum(probs.values())
    if total <= 0:
        return None
    normalized = {opt: probs[opt] / total for opt in options}

    clamped = dict(normalized)
    for _ in range(10):
        clamped = {opt: min(max(v, 0.001), 0.999) for opt, v in clamped.items()}
        clamped_total = sum(clamped.values())
        if clamped_total <= 0:
            break
        clamped = {opt: v / clamped_total for opt, v in clamped.items()}
        if all(0.001 <= v <= 0.999 for v in clamped.values()):
            break
    return clamped


# ─── Step 2c: Numeric prompt/parse (ported 2026-07-21, NEW to batch mode) ─────
# CHANGED 2026-07-21: numeric support does NOT exist yet anywhere on the
# Batch API path in this codebase — meta_refresh_forecast.py explicitly
# leaves NumericQuestion unsupported (see its module docstring), and only
# tournament_forecast_v2.py's SYNCHRONOUS path has it. The prompt/parse
# logic below is ported near-verbatim from there (including the
# reverse-scan fix for grabbing the wrong "low"/"median"/"high" line, the
# magnitude-shorthand recovery for "89.4B"-style answers, and the
# iterative bounds-clamp), but the BATCH persistence needed to reconstruct
# a CDF from a --check run — potentially a separate process, possibly the
# next day — is new: see submit_batch's numeric_bounds tracking below.
def build_numeric_prompt(question: NumericQuestion) -> str:
    unit = question.unit_of_measure or ""
    lower = question.lower_bound
    upper = question.upper_bound
    open_lower = question.open_lower_bound
    open_upper = question.open_upper_bound

    bounds_desc = f"Range: {lower} to {upper} {unit}".strip()
    if open_lower:
        bounds_desc += " (lower bound is open — values below are possible)"
    if open_upper:
        bounds_desc += " (upper bound is open — values above are possible)"

    live_data = detect_data_needs(question.question_text)
    live_data_text = format_live_data_for_prompt(live_data)
    has_live_data = bool(live_data)

    research_text, research_source = research_question(
        question.question_text, question.background_info or "",
        provider_order=["openrouter", "anthropic"], return_source=True,
    )
    _set_research_text(question, research_text)
    _set_research_source(question, research_source)
    has_research = research_text is not None
    research_block = (
        f"\nCURRENT RESEARCH (real-time web search, fetched for this question):\n{research_text}\n"
        if has_research else ""
    )
    has_real_grounding = has_live_data or has_research

    cp = getattr(question, "community_prediction_at_access_time", None)
    community = ""
    if cp is not None:
        if has_real_grounding:
            community = f"\nCurrent community median estimate: {cp:,.2f} {unit}. If your median differs substantially, explain why.\n"
        else:
            community = (
                f"\nCurrent community median estimate: {cp:,.2f} {unit}. "
                "IMPORTANT: you have no live data or research for this "
                "question — the community estimate reflects real people "
                "with access to current information you don't have. Stay "
                "reasonably close to it unless the background/resolution "
                "text above gives a specific, concrete reason to diverge.\n"
            )

    no_data_note = ""
    if not has_real_grounding:
        no_data_note = (
            "\nNOTE: No live market data and no research results were found "
            "for this question. You have no current information beyond the "
            "static text above.\n"
        )

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

{bounds_desc}
{live_data_text}
{research_block}
{no_data_note}
{community}
Today is {datetime.now().strftime("%Y-%m-%d")}.

This is a NUMERIC forecasting question. Reason through it carefully, then provide your estimate.

Before answering write:
(a) Time left until resolution
(b) Most likely outcome and why
(c) What would push the value lower?
(d) What would push the value higher?
(e) Base rate or historical reference values — only cite a specific
    precedent, named source, or past value if it appears word-for-word in
    the Background/Resolution criteria/Research above, otherwise say "No
    reliable base rate available" and proceed on priors.

Then end with exactly these three lines. Write each number IN FULL, matching
the scale of the range given above — do NOT abbreviate with "B", "M", "K",
"billion", "million", etc. (e.g. if the range above is in the billions,
write 89400000000, not 89.4 or 89.4B):
Low (10th percentile): <number>
Median (50th percentile): <number>
High (90th percentile): <number>
"""


def parse_numeric_response(text: str, bounds: dict) -> list[float] | None:
    """Ported from tournament_forecast_v2.py's parse_numeric_response,
    adapted to take a plain `bounds` dict (lower_bound, upper_bound,
    open_lower_bound, open_upper_bound, cdf_size) instead of a live
    NumericQuestion object — same reason as parse_multiple_choice_response
    above: the --check path reconstructs from persisted batch_info, which
    may run in a separate process after the original question object is
    long gone."""
    low = median = high = None
    for line in reversed(text.split('\n')):
        l = line.lower()
        nums = re.findall(r'-?\d[\d,]*\.?\d*', line)
        nums = [float(n.replace(',', '')) for n in nums]
        if not nums:
            continue
        if 'low' in l and '10' in l and low is None:
            low = nums[-1]
        elif 'median' in l and '50' in l and median is None:
            median = nums[-1]
        elif 'high' in l and '90' in l and high is None:
            high = nums[-1]

    if None in (low, median, high):
        return None

    lower = bounds["lower_bound"]
    upper = bounds["upper_bound"]

    CLAMP_TOLERANCE = 0.20
    if median < lower * (1 - CLAMP_TOLERANCE) or median > upper * (1 + CLAMP_TOLERANCE):
        for _mag_name, _mag in (("thousand", 1e3), ("million", 1e6), ("billion", 1e9), ("trillion", 1e12)):
            _low, _median, _high = low * _mag, median * _mag, high * _mag
            if lower * (1 - CLAMP_TOLERANCE) <= _median <= upper * (1 + CLAMP_TOLERANCE):
                print(f"    ℹ️  Recovered likely '{_mag_name}'-shorthand answer: "
                      f"median {median} -> {_median} (now within bounds [{lower}, {upper}])")
                low, median, high = _low, _median, _high
                break
        else:
            print(f"    ⚠️  Parsed median {median} is >20% outside question bounds "
                  f"[{lower}, {upper}] — likely a unit error, rejecting")
            return None
    if low < lower or high > upper or median < lower or median > upper:
        old_low, old_median, old_high = low, median, high
        low    = max(low, lower)
        median = max(min(median, upper), lower)
        high   = min(high, upper)
        print(f"    ℹ️  Clamped percentiles to question bounds [{lower}, {upper}]: "
              f"({old_low}, {old_median}, {old_high}) -> ({low}, {median}, {high})")

    if not (low <= median <= high):
        print(f"    ⚠️  Parsed percentiles not ordered after clamping "
              f"(low={low}, median={median}, high={high}) — rejecting")
        return None

    std = (high - low) / (2 * 1.2816)
    mean = median
    if std <= 0:
        std = abs(mean) * 0.1 + 0.01

    cdf_size = bounds["cdf_size"]
    start_val = 0.001 if bounds["open_lower_bound"] else 0.0
    end_val   = 0.999 if bounds["open_upper_bound"] else 1.0

    def normal_cdf(x, mu, sigma):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))

    raw = []
    for i in range(cdf_size):
        x = lower + (upper - lower) * i / (cdf_size - 1)
        raw.append(normal_cdf(x, mean, std))

    raw_min, raw_max = raw[0], raw[-1]
    if raw_max <= raw_min:
        raw_max = raw_min + 1e-6
    cdf = [start_val + (v - raw_min) / (raw_max - raw_min) * (end_val - start_val) for v in raw]

    min_step = (end_val - start_val) / (cdf_size * 10)
    for i in range(1, len(cdf)):
        if cdf[i] - cdf[i-1] < min_step:
            cdf[i] = cdf[i-1] + min_step

    if cdf[-1] > end_val:
        scale = end_val / cdf[-1]
        cdf = [v * scale for v in cdf]
        cdf[0] = max(cdf[0], start_val)

    return cdf


# ─── Step 2d: Prompt dispatch ──────────────────────────────────────────────────
def build_prompt_for_question(question) -> str:
    """Single dispatch point added 2026-07-21 so submit_batch doesn't need
    its own isinstance branching — routes to whichever of the three
    type-specific builders matches the question."""
    if isinstance(question, MultipleChoiceQuestion):
        return build_multiple_choice_prompt(question)
    if isinstance(question, NumericQuestion):
        return build_numeric_prompt(question)
    return build_binary_prompt(question)


# ─── Step 3: Submit batch ──────────────────────────────────────────────────────
async def submit_batch(questions: list) -> str:
    ensure_batch_dir()
    # Switched 2026-07-04 to the padded batch variant — the base prompt
    # (still used by tournament_forecast.py, untouched) was only ~990
    # tokens, well under Haiku 4.5's 4,096-token caching floor. Confirmed
    # via a real API call: this variant hits 4,187 tokens and produces
    # genuine cache_read_input_tokens on reuse. See cached_llm.py for the
    # full explanation and content.
    system_prompt = build_batch_forecaster_system_prompt()
    # Wrapped once, reused identically across every request in this batch
    # below — same cached prefix on every request means only the first
    # ever pays the cache-write premium; every subsequent one in the same
    # batch (and any other request anywhere on this API key within the
    # 5-minute TTL) reads the cache instead. See meta_prompt_cache.py for
    # the Haiku 4.5 minimum-prefix-length caveat — not yet confirmed
    # whether this prompt actually clears that threshold.
    cached_system = cacheable_system_block(system_prompt)

    requests = []
    question_map = {}

    for q in questions:
        custom_id = f"q_{q.id_of_question}"
        question_map[custom_id] = q

        requests.append({
            "custom_id": custom_id,
            "params": {
                "model": MODEL,
                "max_tokens": MAX_TOKENS,
                "system": cached_system,
                # CHANGED 2026-07-21: was build_user_prompt(q) (binary-only)
                # — now dispatches per-type via build_prompt_for_question.
                "messages": [{"role": "user", "content": build_prompt_for_question(q)}]
            }
        })

    print(f"Submitting batch of {len(requests)} requests...")

    # ADDED 2026-07-08: one-time sanity check for the close_time attribute
    # used below (close_times field). Prints once per run, not per
    # question, so it's cheap and won't spam.
    #
    # FIXED 2026-07-21: this checked "scheduled_close_time" — confirmed via
    # a live diagnostic (diagnose_close_time.py) against real BinaryQuestion,
    # MultipleChoiceQuestion, AND NumericQuestion objects that this was
    # simply the wrong attribute name on ALL THREE types, not something that
    # drifted or something type-specific to MC/numeric. The real pydantic
    # attribute is "close_time" — "scheduled_close_time" only exists in the
    # raw api_json (both top-level and nested under api_json["question"]),
    # never as an attribute on the object itself. This check has printed
    # "NOT FOUND" (silently, easy to miss in cron logs) since the day it was
    # added, because close_times was being read wrong the entire time — it
    # just never got noticed because this file rarely reached a real
    # submission until the MC/numeric fetch expansion started finding
    # enough fresh questions to actually fill a batch.
    if question_map:
        _sample_q = next(iter(question_map.values()))
        _has_attr = getattr(_sample_q, "close_time", None) is not None
        print(f"  (close_time attr check: close_time = "
              f"{getattr(_sample_q, 'close_time', 'NOT FOUND')}"
              f"{'  ✅' if _has_attr else '  ⚠️ verify attribute name'})")

    batch = client_anthropic.messages.batches.create(requests=requests)
    batch_id = batch.id
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    batch_info = {
        "batch_id": batch_id,
        "submitted_at": datetime.now().isoformat(),
        "num_requests": len(requests),
        "question_ids": {
            custom_id: q.id_of_question
            for custom_id, q in question_map.items()
        },
        # Separate from question_id — needed because /api2/questions/{id}/
        # (used by meta_refresh_forecast.py's fetch_question_by_id) is keyed
        # by POST id, not question id. Without this saved, refresh has no
        # way to re-fetch the right question.
        "post_ids": {
            custom_id: q.id_of_post
            for custom_id, q in question_map.items()
        },
        # Added alongside the dashboard/raw-view persistence fix: previously
        # research_question()'s output only ever existed transiently inside
        # the prompt sent to the forecaster — once submitted, there was no
        # way to see what research a question got (or whether it got any)
        # without manually re-running research_question() by hand. Saved
        # here, then carried into the results JSON by check_batch below,
        # next to reasoning.
        "research_texts": {
            custom_id: getattr(q, 'research_text_at_access_time', None)
            for custom_id, q in question_map.items()
        },
        # Which provider ("openrouter" / "anthropic" / None) produced the
        # research above, for the dashboard's source column. Added 2026-07-04.
        "research_sources": {
            custom_id: getattr(q, 'research_source_at_access_time', None)
            for custom_id, q in question_map.items()
        },
        "question_texts": {
            custom_id: q.question_text
            for custom_id, q in question_map.items()
        },
        "community_predictions": {
            custom_id: getattr(q, 'community_prediction_at_access_time', None)
            for custom_id, q in question_map.items()
        },
        "resolve_times": {
            custom_id: q.scheduled_resolution_time.isoformat() if q.scheduled_resolution_time else None
            for custom_id, q in question_map.items()
        },
        # ADDED 2026-07-08: separate from resolve_times above. Found live
        # (Q43615, the Shakira chart-peak question) that close and resolve
        # dates can diverge by weeks — this one closes 2026-07-15 but
        # doesn't resolve until 2026-07-31 — and meta_refresh_forecast.py's
        # CLOSING_SOON bucket was keyed off resolve_time, so it silently
        # never flagged the question as closing-soon in time to catch it
        # before the actual close date passed. scheduled_close_time is the
        # correct field for that check; resolve_times above is left
        # untouched since other things may still want the resolution date.
        #
        # FIXED 2026-07-21: was q.scheduled_close_time — confirmed via
        # diagnose_close_time.py against real Binary/MultipleChoice/Numeric
        # objects that this attribute name was simply wrong (not
        # type-specific drift — wrong for ALL THREE types, and wrong since
        # this field was added 2026-07-08). The real attribute is
        # q.close_time; "scheduled_close_time" only exists in the raw
        # api_json, never as an object attribute. This means every
        # close_times entry saved between 2026-07-08 and today was silently
        # None — see the one-time debug print above, which was ALSO
        # checking the wrong name and so has been printing "NOT FOUND"
        # this whole time without anyone noticing (it rarely fired at all,
        # since this file rarely completed a real submission until the
        # MC/numeric fetch expansion widened the candidate pool).
        "close_times": {
            custom_id: q.close_time.isoformat() if getattr(q, "close_time", None) else None
            for custom_id, q in question_map.items()
        },
        "categories": {
            custom_id: [c.name for c in q.categories] if q.categories else []
            for custom_id, q in question_map.items()
        },
        # ADDED 2026-07-21 alongside multiple_choice/numeric support.
        # question_types defaults to "binary" on read (see check_batch)
        # for every batch_jobs file submitted before this date, since
        # those were all binary anyway — same backward-compat pattern
        # meta_refresh_forecast.py already uses for its own question_types
        # key.
        "question_types": {
            custom_id: _question_type_str(q)
            for custom_id, q in question_map.items()
        },
        # MC options, VERBATIM as returned by the API — needed by
        # check_batch/parse_multiple_choice_response to match Claude's
        # answer back to real option labels. None for non-MC questions.
        "options_by_question": {
            custom_id: (list(q.options) if isinstance(q, MultipleChoiceQuestion) else None)
            for custom_id, q in question_map.items()
        },
        # Numeric bounds, needed by check_batch/parse_numeric_response to
        # reconstruct the CDF from Claude's low/median/high answer — this
        # is the "new to batch mode" persistence mentioned in
        # build_numeric_prompt's comment above, since --check may run in a
        # separate process, possibly the next day, well after this
        # NumericQuestion object is gone. None for non-numeric questions.
        "numeric_bounds": {
            custom_id: (
                {
                    "lower_bound": q.lower_bound,
                    "upper_bound": q.upper_bound,
                    "open_lower_bound": q.open_lower_bound,
                    "open_upper_bound": q.open_upper_bound,
                    "cdf_size": q.cdf_size,
                    "unit_of_measure": q.unit_of_measure,
                } if isinstance(q, NumericQuestion) else None
            )
            for custom_id, q in question_map.items()
        },
    }

    timestamped_file = os.path.join(BATCH_DIR, f"batch_jobs_{timestamp}.json")
    with open(timestamped_file, 'w', newline='\n') as f:
        json.dump(batch_info, f, indent=2)
    with open(BATCH_FILE, 'w', newline='\n') as f:
        json.dump(batch_info, f, indent=2)

    print(f"✅ Batch submitted: {batch_id}")
    print(f"   Saved to {timestamped_file}")
    print(f"   Results available in up to 24 hours")
    print(f"   Run: python meta_batch_forecast.py --check to retrieve results")

    return batch_id


# ─── Step 4: Check and retrieve results ───────────────────────────────────────
async def check_batch():
    if not os.path.exists(BATCH_FILE):
        print(f"No batch job found at {BATCH_FILE}. Run without --check first.")
        return

    with open(BATCH_FILE) as f:
        batch_info = json.load(f)

    batch_id = batch_info['batch_id']
    submitted_at = batch_info.get('submitted_at', '')[:16].replace('T', ' ')
    print(f"Checking batch: {batch_id}")
    print(f"Submitted at:   {submitted_at}")

    batch = client_anthropic.messages.batches.retrieve(batch_id)
    print(f"Status: {batch.processing_status}")
    print(f"Counts: {batch.request_counts}")

    if batch.processing_status != "ended":
        print(f"\nBatch not ready yet. Check back later.")
        return

    print(f"\nBatch complete! Processing results...")

    results = {}
    total_cache_read = 0
    total_cache_write = 0
    for result in client_anthropic.messages.batches.results(batch_id):
        custom_id = result.custom_id

        # CHANGED 2026-07-21: q_type/options/bounds now read from batch_info
        # (added in submit_batch) instead of assuming binary. .get(...,
        # "binary") default preserves correct behavior for any
        # batch_jobs_*.json submitted before this date, which was all
        # binary anyway and has no "question_types" key at all.
        q_type = batch_info.get("question_types", {}).get(custom_id, "binary")
        mc_options = (batch_info.get("options_by_question", {}) or {}).get(custom_id)
        num_bounds = (batch_info.get("numeric_bounds", {}) or {}).get(custom_id)

        # CHANGED 2026-07-21: base_entry fields shared by every branch,
        # then each type adds its own value-carrying key on top — kept
        # "probability" (binary, float) and "probabilities" (multiple_
        # choice, dict) exactly matching meta_refresh_forecast.py's
        # existing convention so meta_dashboard.py/meta_coverage_check.py/
        # meta_calibration_report.py (which already know how to read
        # refresh-batch results in this shape) don't need any changes to
        # also read main-batch results. "cdf" is new for numeric — no
        # prior convention exists since numeric was never in batch mode
        # before this. "submitted_forecast" is kept too (this file's own
        # pre-existing field, matches tournament_forecast.py) and is set
        # to whichever type-specific value applies.
        base_entry = {
            "question_id":   batch_info['question_ids'][custom_id],
            "post_id":       batch_info.get('post_ids', {}).get(custom_id),
            "question_text": batch_info['question_texts'][custom_id],
            "question_type": q_type,
            "community_prediction": batch_info.get("community_predictions", {}).get(custom_id),
            "research_text": batch_info.get("research_texts", {}).get(custom_id),
            "research_source": batch_info.get("research_sources", {}).get(custom_id),
        }

        if result.result.type == "succeeded":
            text = result.result.message.content[0].text
            usage = getattr(result.result.message, "usage", None)
            if usage is not None:
                total_cache_read += getattr(usage, "cache_read_input_tokens", 0) or 0
                total_cache_write += getattr(usage, "cache_creation_input_tokens", 0) or 0

            if q_type == "multiple_choice" and mc_options:
                probs = parse_multiple_choice_response(text, mc_options)
                results[custom_id] = {
                    **base_entry,
                    "probabilities": probs,
                    "submitted_forecast": probs,
                    "reasoning": text,
                    "status": "success" if probs is not None else "parse_failed",
                }
            elif q_type == "numeric" and num_bounds:
                cdf = parse_numeric_response(text, num_bounds)
                results[custom_id] = {
                    **base_entry,
                    "cdf": cdf,
                    "submitted_forecast": cdf,
                    "reasoning": text,
                    "status": "success" if cdf is not None else "parse_failed",
                }
            else:
                # binary (or MC/numeric missing its persisted options/
                # bounds — falls back here rather than crashing, though
                # that will just fail to find a "Probability:" line and
                # come back None)
                prob = None
                for line in reversed(text.split('\n')):
                    if 'probability:' in line.lower():
                        numbers = re.findall(r'\d+\.?\d*', line)
                        if numbers:
                            prob = max(0.01, min(0.99, float(numbers[-1]) / 100))
                            break
                results[custom_id] = {
                    **base_entry,
                    "question_type": "binary",  # correct mislabeled MC/numeric-without-metadata cases
                    "probability": prob,
                    "submitted_forecast": prob,
                    "reasoning": text,
                    "status": "success" if prob is not None else "parse_failed",
                }
        else:
            entry = {**base_entry, "status": "failed", "error": str(result.result), "submitted_forecast": None}
            if q_type == "multiple_choice":
                entry["probabilities"] = None
            elif q_type == "numeric":
                entry["cdf"] = None
            else:
                entry["probability"] = None
            results[custom_id] = entry

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    timestamped_results = os.path.join(BATCH_DIR, f"batch_results_{timestamp}.json")
    with open(timestamped_results, 'w', newline='\n') as f:
        json.dump(results, f, indent=2)
    with open(RESULTS_FILE, 'w', newline='\n') as f:
        json.dump(results, f, indent=2)

    print(f"Saved {len(results)} results to {timestamped_results}")
    if total_cache_read or total_cache_write:
        print(f"  💰 Prompt cache: {total_cache_read} tokens read, "
              f"{total_cache_write} tokens written across this batch")
    else:
        print(f"  ⚠️  Prompt cache: 0 read, 0 written across this batch — caching "
              f"isn't engaging (system prompt may be under Haiku 4.5's "
              f"4,096-token minimum cacheable length).")
    await submit_to_metaculus(results, current_results_file=timestamped_results)


# ─── Step 5: Submit to Metaculus ───────────────────────────────────────────────
def load_previously_submitted_ids(current_q_ids: set) -> dict[int, str]:
    """Return {question_id: question_text} for everything already submitted in
    previous main batches. A dict (not a bare set) so callers can verify the
    title still matches before treating an ID as a genuine duplicate — guards
    against a recycled question ID silently swallowing a new question."""
    seen: dict[int, str] = {}
    for rf in glob.glob(os.path.join(BATCH_DIR, "batch_results_2*.json")):
        try:
            with open(rf) as f:
                data = json.load(f)
            rf_q_ids = set(r.get("question_id") for r in data.values() if r.get("question_id"))
            if rf_q_ids == current_q_ids:
                continue  # this is the current batch, skip it
            for r in data.values():
                if r.get("status") == "success" and r.get("question_id"):
                    seen.setdefault(r["question_id"], r.get("question_text", ""))
        except Exception:
            pass
    return seen


async def submit_to_metaculus(results: dict, current_results_file: str = None):
    print(f"\nSubmitting forecasts to Metaculus...")

    current_q_ids = set(r.get("question_id") for r in results.values() if r.get("question_id"))
    previously_submitted = load_previously_submitted_ids(current_q_ids)
    if previously_submitted:
        print(f"  (checking {len(previously_submitted)} previously submitted IDs for duplicates)")

    submitted = 0
    skipped = 0
    failed = 0

    for custom_id, result in results.items():
        # CHANGED 2026-07-21: was result["probability"] directly, which
        # KeyErrors on MC (carries "probabilities") or numeric (carries
        # "cdf") entries — same failure mode meta_refresh_forecast.py
        # already found and fixed for its own batch path (see that file's
        # "would KeyError on MC entries" comment). "submitted_forecast" is
        # set to the right value for every type in check_batch above, so
        # checking that instead covers all three uniformly.
        q_type = result.get("question_type", "binary")
        forecast_value = result.get("submitted_forecast")
        if result["status"] != "success" or forecast_value is None:
            print(f"  ⚠️  {custom_id} (Q{result.get('question_id')}): dropped — "
                  f"status={result['status']}, forecast={forecast_value}")
            failed += 1
            continue

        q_id = result["question_id"]

        if q_id in previously_submitted:
            stored_title = previously_submitted[q_id]
            if titles_match(stored_title, result.get("question_text", "")):
                print(f"  ⏭️  Q{q_id}: skipped duplicate — {result['question_text'][:45]}")
                skipped += 1
                continue
            print(f"  🛑 Q{q_id}: previously submitted under a different title — "
                  f"treating as a NEW question (ID likely recycled), submitting.")
            print(f"       Previously: {stored_title[:90]}")
            print(f"       Now:        {result.get('question_text', '')[:90]}")

        try:
            if q_type == "multiple_choice":
                client_metaculus.post_multiple_choice_question_prediction(
                    question_id=q_id,
                    options_with_probabilities=forecast_value,
                )
                top = max(forecast_value, key=forecast_value.get)
                summary = f"top='{top}' ({forecast_value[top]:.0%})"
            elif q_type == "numeric":
                client_metaculus.post_numeric_question_prediction(
                    question_id=q_id,
                    cdf_values=forecast_value,
                )
                median_idx = next((i for i, v in enumerate(forecast_value) if v >= 0.5), len(forecast_value) // 2)
                summary = f"median idx {median_idx}/{len(forecast_value) - 1}"
            else:
                client_metaculus.post_binary_question_prediction(
                    question_id=q_id,
                    prediction_in_decimal=forecast_value,
                )
                summary = f"{forecast_value:.0%}"

            print(f"  ✅ Q{q_id} ({q_type}): {summary} — {result['question_text'][:50]}")
            send_alert(
                f"Q{q_id} ({q_type}): {summary}\n{result['question_text'][:100]}",
                title="New forecast submitted (batch)"
            )
            submitted += 1
            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"  ❌ Q{q_id}: {str(e)[:60]}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Submitted: {submitted} | Skipped (duplicate): {skipped} | Failed: {failed}")
    print(f"Cost: ~${submitted * 0.05:.2f} (50% batch discount applied)")


# ─── Step 6: Update community predictions ─────────────────────────────────────
async def update_community_predictions():
    """Fetch community predictions for the latest batch.

    REWRITTEN 2026-06-29: previously chunked through api2/questions/?ids=,
    which meta_debug_ids_probe.py proved ignores its filter entirely and
    returns unrelated recent questions regardless of what's requested —
    meaning this was silently fetching nothing useful, ever, no matter
    what "still hidden" looked like in the logs. Now loops single calls
    through api2/questions/{id}/ — the SINGULAR detail endpoint, proven
    correct via the same probe — keyed by POST id (confirmed: that
    endpoint is keyed by post_id, not question_id; the two are NOT
    interchangeable here).

    Sequential, not concurrent: this is the manual/--update-community
    path with no time pressure (unlike tournament_forecast.py's
    synchronous fetch-before-forecast path), so simple one-at-a-time
    calls with a politeness delay are fine and easier to reason about.

    CHANGED 2026-07-21: this script used to only ever forecast BINARY
    questions (fetch_questions's allowed_types=["binary"] filter), so
    extract_live_cp was always called with "binary" here. Now branches
    per-question via batch_info["question_types"] (added in submit_batch)
    — calling extract_live_cp with the wrong type string for an MC
    question would have silently returned None or a garbage value instead
    of the real per-option dict.

    post_id is only available for batches submitted after this same date
    (see submit_batch's post_ids field, added alongside this fix) — older
    batches have no reliable ID to re-fetch by and are skipped with a
    count, not guessed at."""
    if not os.path.exists(BATCH_FILE):
        print(f"No {BATCH_FILE} found. Run a batch first.")
        return

    with open(BATCH_FILE) as f:
        batch_info = json.load(f)

    question_ids = batch_info.get("question_ids", {})
    post_ids = batch_info.get("post_ids", {})
    community_preds = batch_info.get("community_predictions", {})

    already_filled = sum(1 for v in community_preds.values() if v is not None)
    print(f"Updating community predictions ({already_filled}/{len(question_ids)} already filled)...")

    headers = {"Authorization": f"Token {ACTIVE_TOKEN}"}
    updated = 0
    still_hidden = 0
    no_post_id = 0
    errors = 0

    to_fetch = [
        custom_id for custom_id in question_ids
        if community_preds.get(custom_id) is None
    ]

    print(f"Fetching {len(to_fetch)} questions one at a time via the singular endpoint...")

    async with aiohttp.ClientSession() as session:
        for i, custom_id in enumerate(to_fetch, 1):
            post_id = post_ids.get(custom_id)
            q_id = question_ids.get(custom_id)

            if post_id is None:
                no_post_id += 1
                continue  # pre-fix batch — no reliable ID to fetch by, skip rather than guess

            url = f"https://www.metaculus.com/api2/questions/{post_id}/"
            retries = 3
            while retries > 0:
                try:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 429:
                            print(f"  Rate limited — waiting 30 seconds...")
                            await asyncio.sleep(30)
                            retries -= 1
                            continue
                        if resp.status != 200:
                            print(f"  ❌ post {post_id} (Q{q_id}): HTTP {resp.status}")
                            errors += 1
                            break

                        data = await resp.json()
                        q_type = batch_info.get("question_types", {}).get(custom_id, "binary")
                        cp = extract_live_cp(data, q_type)

                        if cp is not None:
                            community_preds[custom_id] = cp
                            updated += 1
                            q_text = batch_info.get("question_texts", {}).get(custom_id, "")[:50]
                            # cp is a dict for multiple_choice, a float for binary/numeric
                            cp_str = ", ".join(f"{k}: {v:.0%}" for k, v in cp.items()) if isinstance(cp, dict) else f"{cp:.0%}"
                            print(f"  ✅ post {post_id} (Q{q_id}): {cp_str} — {q_text}")
                        else:
                            still_hidden += 1
                        break

                except Exception as e:
                    print(f"  ❌ post {post_id} (Q{q_id}): {e}")
                    errors += 1
                    break

            if i % 10 == 0:
                print(f"  ...{i}/{len(to_fetch)} done")
            await asyncio.sleep(1.2)

    batch_info["community_predictions"] = community_preds
    with open(BATCH_FILE, "w", newline='\n') as f:
        json.dump(batch_info, f, indent=2)

    # Also update matching timestamped file
    batch_id = batch_info.get("batch_id", "")
    for jf in glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")):
        try:
            with open(jf) as f:
                jdata = json.load(f)
            if jdata.get("batch_id") == batch_id:
                jdata["community_predictions"] = community_preds
                with open(jf, "w", newline='\n') as f:
                    json.dump(jdata, f, indent=2)
                print(f"  Also updated {jf}")
                break
        except Exception:
            pass

    filled = sum(1 for v in community_preds.values() if v is not None)
    print(f"\n✅ Updated {updated} new community predictions")
    print(f"   {filled}/{len(question_ids)} total filled | {still_hidden} still hidden "
          f"(genuinely no CP from this endpoint — check include_bots_in_aggregates if "
          f"this number seems high) | {no_post_id} skipped (no post_id on file — "
          f"predates the post_id fix) | {errors} fetch error(s)")
    if still_hidden > 0:
        print(f"   Run again later to pick up remaining hidden predictions")


async def wait_and_alert_when_ready(batch_id: str, poll_interval: int = 10, max_wait: int = 300):
    """Poll batch status after submission and alert ONLY once it's
    genuinely ready to --check. Real-world turnaround for this batch size
    has been observed at well under 30 seconds in practice, but Anthropic's
    documented SLA is up to 24h — this loop is bounded (max_wait, default
    5 minutes) so a run that doesn't finish quickly doesn't hang the
    GitHub Actions job indefinitely or burn its runtime budget.

    If the batch isn't ready within max_wait, this prints a message and
    returns WITHOUT alerting — deliberately, so Mike is never told "go run
    --check" on a batch that genuinely isn't finished yet. There's no
    second automated --check cron (by design — see meta_batch_forecast.yml);
    an un-alerted batch just waits for the next scheduled --submit run,
    or a manual `python meta_batch_forecast.py --check` whenever convenient.
    """
    elapsed = 0
    while elapsed < max_wait:
        batch = client_anthropic.messages.batches.retrieve(batch_id)
        if batch.processing_status == "ended":
            alert_sent = send_alert(
                f"Batch {batch_id} finished processing after {elapsed}s.\n"
                f"On your machine: git pull, then run "
                f"python meta_batch_forecast.py --check\n"
                f"(git pull first — the batch_jobs file this run just "
                f"committed won't exist locally until you pull it.)",
                title="Batch ready - git pull then --check"
            )
            if alert_sent:
                print(f"  ✅ Batch ready after {elapsed}s — alert sent.")
            else:
                # Caught live 2026-06-30: this used to print "alert sent"
                # unconditionally right after calling send_alert(), even on
                # the run where the send had actually just failed (an
                # emoji in the title crashed the HTTP header — see
                # meta_alerts.py). Batch results are real regardless of
                # whether the notification made it, so this still says so
                # explicitly rather than leaving Mike to find out by
                # checking his phone and seeing nothing.
                print(f"  ⚠️  Batch ready after {elapsed}s, but the alert FAILED "
                      f"to send (see error above) — results are still ready: "
                      f"git pull, then python meta_batch_forecast.py --check")
            return
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    print(f"  ⏳ Batch not ready after {max_wait}s — NOT alerting yet (would be "
          f"premature). It'll be picked up by the next scheduled --submit run, "
          f"or check manually anytime: python meta_batch_forecast.py --check")


# ─── Main ──────────────────────────────────────────────────────────────────────
async def main():
    print("METACULUS BATCH FORECASTER")
    print("=" * 50)
    questions = await fetch_questions()
    if questions:
        batch_id = await submit_batch(questions)
        await wait_and_alert_when_ready(batch_id)
    else:
        print("No questions found matching filter")


if __name__ == "__main__":
    import sys
    if "--check" in sys.argv:
        asyncio.run(check_batch())
    elif "--update-community" in sys.argv:
        asyncio.run(update_community_predictions())
    else:
        asyncio.run(main())