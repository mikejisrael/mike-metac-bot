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

from forecasting_tools import MetaculusClient, ApiFilter, BinaryQuestion
from live_data import detect_data_needs, format_live_data_for_prompt
from cached_llm import build_forecaster_system_prompt
from meta_prompt_cache import cacheable_system_block
from meta_cp_extract import extract_live_cp
from meta_alerts import send_alert
from meta_research import research_question

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
# FIXED 2026-07-02: dropped from 50 to 20 as a deliberate cost control —
# ALLOWED_TOURNAMENTS is expanding from 3 to 8 tournaments below, so
# holding new-questions-per-run steady rather than letting it scale with
# tournament count keeps the Batch API bill from growing 8/3 = ~2.7x
# alongside it. Revisit once the cost optimization review (deferred
# 2026-06-28) actually happens.
NUM_QUESTIONS = 20
DAYS_AHEAD = 365
MIN_FORECASTERS = 5
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
# Cost optimization split (2026-06-30): FutureEval (33022) REMOVED from this
# list — it has 90-minute close windows and needs tournament_forecast.py's
# synchronous path (now its ONLY tournament, see that file's FUTUREEVAL_
# TOURNAMENT_ID-only config). The three remaining tournaments here are none
# of them time-sensitive in the same way, so they stay on this script's
# Batch API path (50% cheaper than synchronous) on its own ~every-3-days
# cron schedule, decoupled from tournament_forecast.py's tighter cadence.
#   "ACX2026"                    = ACX 2026 Prediction Contest
#   "climate"                    = Climate Tipping Points
#   "metaculus-cup-summer-2026"  = Metaculus Cup Summer 2026 (bots can forecast
#                                  here for calibration data, but are NOT prize-
#                                  eligible in this one — humans-only for prizes)
#
# EXPANDED 2026-07-02: added 5 more series for broader category coverage —
# the whole point of the track record is figuring out which question
# categories Claude forecasts well, which needs more categories to compare.
# Each was shortlisted specifically for confirmed peer scoring (checked via
# resolve_series_ids.py against a known question from each) — series
# without peer scoring wouldn't feed meta_calibration_report.py at all, so
# weren't worth adding. IDs resolved via a real question's own `projects`
# field, not guessed — see resolve_series_ids.py in this same session for
# the lookup method (list/search endpoints proved unreliable, silently
# ignoring filters — see check_tournament_ids.py's results).
#   1173   = Nuclear Risk Horizons Project
#   32774  = Current Events⚡
#   3048   = The Taiwan Tinderbox
#   2018   = Economic Indicators
#   2995   = Animal Welfare Series
ALLOWED_TOURNAMENTS = [
    "ACX2026", "climate", "metaculus-cup-summer-2026",
    1173, 32774, 3048, 2018, 2995,
]


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


# ─── Question identity guard ────────────────────────────────────────────────
from meta_question_matching import titles_match


# ─── Step 1: Fetch questions ───────────────────────────────────────────────────
async def fetch_questions() -> list[BinaryQuestion]:
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
        allowed_types=["binary"],
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
    binary = [q for q in questions if isinstance(q, BinaryQuestion)]

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
    cp_found = 0
    for q in fresh:
        cp = extract_live_cp(getattr(q, "api_json", None), "binary")
        q.community_prediction_at_access_time = cp
        if cp is not None:
            cp_found += 1
    print(f"  Live CP found for {cp_found}/{len(fresh)} questions before forecasting "
          f"(rest will forecast without CP-anchoring this run)")

    return fresh


# ─── Step 2: Build prompts ─────────────────────────────────────────────────────
def build_user_prompt(question: BinaryQuestion) -> str:
    live_data = detect_data_needs(question.question_text)
    live_data_text = format_live_data_for_prompt(live_data)
    has_live_data = bool(live_data)  # live_data.py only covers crypto/stock/
    # index/FRED keywords — most non-financial questions get nothing here.

    research_text = research_question(question.question_text, question.background_info or "")
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


# ─── Step 3: Submit batch ──────────────────────────────────────────────────────
async def submit_batch(questions: list[BinaryQuestion]) -> str:
    ensure_batch_dir()
    system_prompt = build_forecaster_system_prompt()
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
                "messages": [{"role": "user", "content": build_user_prompt(q)}]
            }
        })

    print(f"Submitting batch of {len(requests)} requests...")

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
        "categories": {
            custom_id: [c.name for c in q.categories] if q.categories else []
            for custom_id, q in question_map.items()
        }
    }

    timestamped_file = os.path.join(BATCH_DIR, f"batch_jobs_{timestamp}.json")
    with open(timestamped_file, 'w') as f:
        json.dump(batch_info, f, indent=2)
    with open(BATCH_FILE, 'w') as f:
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

            results[custom_id] = {
                "question_id":   batch_info['question_ids'][custom_id],
                "post_id":       batch_info.get('post_ids', {}).get(custom_id),
                "question_text": batch_info['question_texts'][custom_id],
                "question_type": "binary",
                "probability":   prob,
                "submitted_forecast": prob,  # standardized field name, matches tournament_forecast.py
                "reasoning":     text,
                # Was previously only in batch_info (the jobs file), making
                # the results file look CP-blind on its own. Saved here too
                # now — also reflects the pre-forecast value if the live
                # prefetch in fetch_questions() found one.
                "community_prediction": batch_info.get("community_predictions", {}).get(custom_id),
                "research_text": batch_info.get("research_texts", {}).get(custom_id),
                "status":        "success"
            }
        else:
            results[custom_id] = {
                "question_id":   batch_info['question_ids'][custom_id],
                "post_id":       batch_info.get('post_ids', {}).get(custom_id),
                "question_text": batch_info['question_texts'][custom_id],
                "question_type": "binary",
                "probability":   None,
                "submitted_forecast": None,
                "community_prediction": batch_info.get("community_predictions", {}).get(custom_id),
                "research_text": batch_info.get("research_texts", {}).get(custom_id),
                "status":        "failed",
                "error":         str(result.result)
            }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    timestamped_results = os.path.join(BATCH_DIR, f"batch_results_{timestamp}.json")
    with open(timestamped_results, 'w') as f:
        json.dump(results, f, indent=2)
    with open(RESULTS_FILE, 'w') as f:
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
        if result["status"] != "success" or result["probability"] is None:
            print(f"  ⚠️  {custom_id} (Q{result.get('question_id')}): dropped — "
                  f"status={result['status']}, prob={result['probability']}")
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
            client_metaculus.post_binary_question_prediction(
                question_id=q_id,
                prediction_in_decimal=result["probability"]
            )
            print(f"  ✅ Q{q_id}: {result['probability']:.0%} — {result['question_text'][:50]}")
            send_alert(
                f"Q{q_id}: {result['probability']:.0%}\n{result['question_text'][:100]}",
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

    This script only ever forecasts BINARY questions (see fetch_questions's
    allowed_types=["binary"] filter) — extract_live_cp is always called
    with "binary" here for that reason.

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
                        cp = extract_live_cp(data, "binary")

                        if cp is not None:
                            community_preds[custom_id] = cp
                            updated += 1
                            q_text = batch_info.get("question_texts", {}).get(custom_id, "")[:50]
                            print(f"  ✅ post {post_id} (Q{q_id}): {cp:.0%} — {q_text}")
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
    with open(BATCH_FILE, "w") as f:
        json.dump(batch_info, f, indent=2)

    # Also update matching timestamped file
    batch_id = batch_info.get("batch_id", "")
    for jf in glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")):
        try:
            with open(jf) as f:
                jdata = json.load(f)
            if jdata.get("batch_id") == batch_id:
                jdata["community_predictions"] = community_preds
                with open(jf, "w") as f:
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