"""
tournament_forecast.py — Tournament forecaster using synchronous Claude calls.

Handles binary, numeric, discrete, and multiple_choice question types.
Calls Claude synchronously and submits to Metaculus immediately in the same run.
This is essential for tournament questions that may only be open for 90 minutes.

Usage:
  python tournament_forecast.py          # forecast and submit all open questions

Choosing tournaments:
  Defaults to meta_batch_forecast.ALLOWED_TOURNAMENTS (shared with the batch
  script, so both stay in sync). Override without editing either file:
      set METAC_TOURNAMENT_IDS=32977             (bot-testing-area only)
      set METAC_TOURNAMENT_IDS=33022,ACX2026     (comma-separated, mix of
                                                   numeric IDs and slugs)
"""

import asyncio
import os
import re
import glob
import json
import math
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import anthropic
import meta_batch_forecast as bf
from forecasting_tools import (
    MetaculusApi, MetaculusClient,
    BinaryQuestion, NumericQuestion, MultipleChoiceQuestion
)
from cached_llm import build_forecaster_system_prompt
from meta_prompt_cache import cacheable_system_block
from meta_cp_extract import extract_live_cp
from meta_alerts import send_alert
from meta_research import research_question
from live_data import detect_data_needs, format_live_data_for_prompt
from meta_watch import check_new_futureeval_questions, check_resolutions, FUTUREEVAL_TOURNAMENT_ID

# ─── Config ───────────────────────────────────────────────────────────────────
# Cost optimization split (2026-06-30): this script previously fetched and
# synchronously forecast ALL of bf.ALLOWED_TOURNAMENTS — including ACX2026,
# Climate Tipping Points, and Metaculus Cup, none of which are remotely as
# time-sensitive as FutureEval (90-minute close windows). Those three now
# only get forecast via meta_batch_forecast.py's Batch API path (50%
# cheaper, on its own ~every-3-days cron schedule, separate from this
# script's tighter cadence) — see meta_batch_forecast.py's ALLOWED_TOURNAMENTS
# for the other side of this split. This script now defaults to FutureEval
# ONLY, using meta_watch.FUTUREEVAL_TOURNAMENT_ID as the single source of
# truth (same constant meta_watch.py's new-question alert already uses).
# Override without editing either file: set METAC_TOURNAMENT_IDS to a
# comma-separated list, e.g.
#     set METAC_TOURNAMENT_IDS=32977
# (32977 = bot-testing-area, useful for testing in isolation from real tournaments)
_env_override = os.getenv("METAC_TOURNAMENT_IDS")
if _env_override:
    TOURNAMENT_IDS = [t.strip() for t in _env_override.split(",") if t.strip()]
else:
    TOURNAMENT_IDS = [FUTUREEVAL_TOURNAMENT_ID]
TOURNAMENT_BATCH_DIR = "tournament_batches"
MODEL                = "claude-haiku-4-5"
MAX_TOKENS           = 2000

# ─── Clients ──────────────────────────────────────────────────────────────────
client_anthropic = anthropic.Anthropic()
# Tournament submissions should authenticate as the dedicated bot account
# (mike_iz_-bot), not the shared personal-account token meta_batch_forecast.py
# uses. Set METAC_TOURNAMENT_TOKEN in .env once mike_iz_-bot's own API token
# is generated; falls back to METACULUS_TOKEN so this doesn't silently break
# before that's set up.
TOURNAMENT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if os.getenv("METAC_TOURNAMENT_TOKEN"):
    print("Auth: using dedicated METAC_TOURNAMENT_TOKEN (mike_iz_-bot)")
else:
    print("Auth: METAC_TOURNAMENT_TOKEN not set — falling back to shared METACULUS_TOKEN (mike_iz_)")
client_metaculus = MetaculusClient(token=TOURNAMENT_TOKEN)

# ─── Fail fast on permanently-closed questions ───────────────────────────────
# forecasting_tools' _post_question_prediction retries ANY HTTPError 3x with
# exponential backoff (up to 75s per attempt — ~100s+ total per call). That's
# correct for transient errors (network blips, momentary 5xx) but wasteful for
# a question that's permanently closed to forecasting: a 405 "already closed"
# response will never succeed no matter how many times we retry it. In
# practice this was adding 5-10+ minutes of pure dead time to a single run
# when several questions in the batch had closed between fetch and submit.
# We replace it with a version that retries everything else exactly as
# before, but recognizes this one permanent case and skips straight to
# failure instead of retrying it.
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

# ─── Redirect meta_batch_forecast's namespace (keeps dedup isolated) ─────────
bf.BATCH_DIR    = TOURNAMENT_BATCH_DIR
bf.BATCH_FILE   = os.path.join(TOURNAMENT_BATCH_DIR, "batch_jobs.json")
bf.RESULTS_FILE = os.path.join(TOURNAMENT_BATCH_DIR, "batch_results.json")
os.makedirs(TOURNAMENT_BATCH_DIR, exist_ok=True)


# ─── Question identity guard ────────────────────────────────────────────────
from meta_question_matching import titles_match


# ─── Pydantic-safe attribute setter ────────────────────────────────────────
def _set_cp(obj, cp) -> None:
    """Set community_prediction_at_access_time regardless of whether the
    underlying pydantic model declares that field. NumericQuestion and
    MultipleChoiceQuestion do not declare it and raise on a plain attribute
    assignment (confirmed via a live bot-testing-area run — this silently
    dropped 3 of 5 real forecastable questions into 'parse errors' before
    this fix, since the crash happened inside the same try block that
    builds the whole question object).

    CORRECTION 2026-06-30: this docstring previously claimed "BinaryQuestion
    allows extra attributes" — that was never actually verified, just
    assumed because community_prediction_at_access_time happens to be a
    genuinely declared field on BinaryQuestion specifically (so plain
    assignment of THIS field works fine there). It does NOT generalize:
    a live GitHub Actions run crashed when meta_batch_forecast.py tried
    plain-assigning a DIFFERENT, undeclared field
    (research_text_at_access_time) onto a BinaryQuestion object. Use this
    pydantic-safe pattern for any new per-question attribute on ANY
    question type, not just Numeric/MultipleChoice — don't assume Binary
    is exempt."""
    try:
        obj.community_prediction_at_access_time = cp
    except Exception:
        object.__setattr__(obj, "community_prediction_at_access_time", cp)


# ─── Step 1: Fetch open tournament questions ───────────────────────────────────
async def fetch_tournament_questions() -> list:
    # already_done maps question_id -> the title we forecast it under, so a
    # recycled ID (genuinely a different question on Metaculus's side) can be
    # detected via _titles_match instead of being silently skipped as a dup.
    already_done: dict[int, str] = {}
    for rf in glob.glob(os.path.join(TOURNAMENT_BATCH_DIR, "batch_results_2*.json")):
        try:
            with open(rf) as f:
                data = json.load(f)
            for r in data.values():
                if r.get("question_id"):
                    already_done.setdefault(r["question_id"], r.get("question_text", ""))
        except Exception:
            pass

    print(f"Tournaments: {TOURNAMENT_IDS}", flush=True)
    print(f"Excluding {len(already_done)} already-forecast questions (tournament folder)...", flush=True)

    import requests
    headers = {
        "Authorization": f"Token {TOURNAMENT_TOKEN}",
        # Metaculus's bot detection appears to reject the default
        # "python-requests/x.x" user-agent on this endpoint (it returned an
        # HTML page instead of JSON, hence the json-decode failure on the
        # first attempt). Using a normal browser-style UA as a likely fix.
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    raw_posts_by_id: dict[int, dict] = {}
    for tid in TOURNAMENT_IDS:
        tid_posts_by_id: dict[int, dict] = {}  # this tournament only — used
        # below to scope the new-FutureEval-question check; raw_posts_by_id
        # above stays the existing flat cross-tournament dict, unchanged.
        try:
            url = f"https://www.metaculus.com/api/posts/?tournaments={tid}&limit=100"
            tid_count = 0
            page_num = 0
            MAX_PAGES = 10  # safety cap — 166 open questions at limit=100 should
                             # be ~2 pages; if we're still going past this many,
                             # something's wrong (e.g. endpoint returning all
                             # historical posts, not just current open ones)
            while url and page_num < MAX_PAGES:
                page_num += 1
                # Always pause before a fetch (page or tournament) — firing
                # all 4 tournaments' paginated requests back-to-back with no
                # delay is what triggered Metaculus's 429 rate limit here.
                time.sleep(1.5)

                data = None
                fetch_exhausted_on_connection_error = False
                for attempt in range(3):
                    print(f"  ...fetching {tid} (attempt {attempt + 1}/3)...", flush=True)
                    # Added: previously a transient connection failure here
                    # (DNS blip, connection reset, etc.) raised straight out
                    # of this loop with zero retry, was caught by the outer
                    # try/except at the tid level, and silently dropped the
                    # ENTIRE tournament for the run — confirmed live: this is
                    # exactly what happened to FutureEval (id 33022, the main
                    # prize-eligible tournament) on one run, with nothing in
                    # the logs beyond a single one-line warning easy to miss.
                    # The existing 429 handling below already retries
                    # correctly; this just extends the same retry budget and
                    # backoff to connection-level failures instead of only
                    # HTTP-level ones.
                    try:
                        r = requests.get(url, headers=headers, timeout=30)
                    except (requests.exceptions.ConnectionError,
                            requests.exceptions.Timeout) as e:
                        wait = 5 * (attempt + 1)
                        print(f"  ⏳ Tournament {tid}: connection error "
                              f"({type(e).__name__}: {e}), waiting {wait}s "
                              f"before retry {attempt + 1}/3...", flush=True)
                        time.sleep(wait)
                        fetch_exhausted_on_connection_error = (attempt == 2)
                        continue
                    fetch_exhausted_on_connection_error = False
                    if r.status_code == 429:
                        wait = 5 * (attempt + 1)
                        print(f"  ⏳ Tournament {tid}: rate limited (429), "
                              f"waiting {wait}s before retry {attempt + 1}/3...", flush=True)
                        time.sleep(wait)
                        continue
                    if r.status_code != 200:
                        print(f"  ⚠️  Tournament {tid}: HTTP {r.status_code} — "
                              f"body starts with: {r.text[:150]!r}", flush=True)
                        break
                    try:
                        data = r.json()
                    except Exception:
                        print(f"  ⚠️  Tournament {tid}: non-JSON response (status {r.status_code}) — "
                              f"body starts with: {r.text[:150]!r}")
                    break

                if data is None:
                    if fetch_exhausted_on_connection_error:
                        # Loud, not just a console print: this run is almost
                        # always triggered headlessly via cron-job.org ->
                        # GitHub Actions, where console output isn't watched
                        # in real time. A dropped tournament — especially
                        # FutureEval, the main prize-eligible one — needs to
                        # surface somewhere Mike will actually see it.
                        send_alert(
                            f"Tournament {tid}: fetch failed after 3 connection-error "
                            f"retries — this tournament was SKIPPED entirely for this run.",
                            title="⚠️ Tournament fetch dropped"
                        )
                    break  # exhausted retries or hit a non-200/non-JSON response

                tid_posts = data.get("results", [])
                tid_count += len(tid_posts)
                print(f"  ...{tid} page {page_num}: +{len(tid_posts)} posts "
                      f"(running total {tid_count}), next={'yes' if data.get('next') else 'no'}",
                      flush=True)
                for post in tid_posts:
                    post_id = post.get("id")
                    if post_id is not None:
                        raw_posts_by_id.setdefault(post_id, post)
                        tid_posts_by_id.setdefault(post_id, post)
                if not tid_posts:
                    # An empty page means we're past the real end of data —
                    # stop here even if the API still claims a 'next' link.
                    # (Observed in practice: Metaculus keeps returning a next
                    # URL indefinitely past the actual last page.)
                    break
                url = data.get("next")  # paginate — a tournament can have >100 questions
            if page_num >= MAX_PAGES:
                print(f"  ⚠️  Tournament {tid}: hit the {MAX_PAGES}-page safety cap "
                      f"({tid_count} posts so far) — stopping early. This tournament's "
                      f"endpoint may be returning all historical posts, not just open ones.",
                      flush=True)
            print(f"  Tournament {tid}: {tid_count} posts", flush=True)
            if str(tid) == str(FUTUREEVAL_TOURNAMENT_ID):
                check_new_futureeval_questions(tid_posts_by_id)
        except Exception as e:
            print(f"  ⚠️  Could not fetch tournament {tid}: {e}", flush=True)
            # Same reasoning as the connection-error alert above: an
            # unexpected failure here (anything not already handled by the
            # retry logic — e.g. a genuine library/parsing bug) still
            # silently drops the whole tournament if all we do is print.
            send_alert(
                f"Tournament {tid}: fetch failed with an unexpected error — "
                f"this tournament was SKIPPED entirely for this run.\n{str(e)[:150]}",
                title="⚠️ Tournament fetch dropped"
            )

    raw_posts = list(raw_posts_by_id.values())

    now = datetime.now(timezone.utc)
    questions = []
    closed_count = 0
    no_question_field_count = 0
    for post in raw_posts:
        q = post.get("question")
        if not q:
            no_question_field_count += 1
            continue
        # Skip if close time has passed
        close_time = q.get("scheduled_close_time")
        if close_time:
            close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            if close_dt < now:
                closed_count += 1
                continue
        questions.append(post)

    print(f"  Funnel: {len(raw_posts)} raw posts -> {no_question_field_count} missing question field, "
          f"{closed_count} already closed -> {len(questions)} still open", flush=True)

    # Convert to library question objects using from_metaculus_api_json
    from forecasting_tools import NumericQuestion, MultipleChoiceQuestion
    supported = []
    supported_post_ids = []  # parallel list, same index as `supported` —
    # needed for the CP fetch below, which is keyed by POST id (confirmed
    # via meta_debug_ids_probe.py — the singular detail endpoint is keyed
    # by post_id, NOT question_id; they are not interchangeable).
    unsupported_type_counts: dict[str, int] = {}
    dedup_skipped = 0
    parse_errors = 0
    for post in questions:
        q = post.get("question", {})
        q_type = q.get("type")
        try:
            if q_type == "binary":
                obj = BinaryQuestion.from_metaculus_api_json(post)
            elif q_type in ("numeric", "discrete"):
                obj = NumericQuestion.from_metaculus_api_json(post)
            elif q_type == "multiple_choice":
                obj = MultipleChoiceQuestion.from_metaculus_api_json(post)
            else:
                unsupported_type_counts[q_type] = unsupported_type_counts.get(q_type, 0) + 1
                continue
            if obj.id_of_question in already_done:
                stored_title = already_done[obj.id_of_question]
                if titles_match(stored_title, obj.question_text):
                    dedup_skipped += 1
                    continue  # genuine duplicate — already forecast this question
                print(f"  🛑 Q{obj.id_of_question}: ID was previously forecast under a different "
                      f"title — treating as a NEW question (ID likely recycled).")
                print(f"       Previously: {stored_title[:90]}")
                print(f"       Now:        {obj.question_text[:90]}")
            # Live CP, extracted from the post data we already fetched above
            # (no extra API call). The listing endpoint (api/posts/) never
            # carries real aggregation values — confirmed via
            # meta_debug_cp_probe.py, it returns the correct shape with
            # everything nulled out. This is left as a harmless no-op
            # placeholder (will set None) until the real fetch below runs.
            _set_cp(obj, extract_live_cp(post, q_type))
            supported.append(obj)
            supported_post_ids.append(post.get("id"))
        except Exception as e:
            parse_errors += 1
            print(f"  ⚠️  Could not parse Q{q.get('id')}: {e}")

    # Real CP fetch — CONCURRENT, capped, via the SINGULAR /api2/questions/
    # {id}/ endpoint, keyed by post_id.
    #
    # REWRITTEN 2026-06-29: the previous version chunked through
    # api2/questions/?ids=, which meta_debug_ids_probe.py proved ignores
    # its filter entirely and returns unrelated recent questions regardless
    # of what's requested — so this was silently fetching nothing useful,
    # ever. The singular endpoint is proven correct (same probe), but it's
    # keyed by post_id, not question_id, which is why this now needs
    # supported_post_ids built above.
    #
    # CONCURRENT (not sequential like meta_batch_forecast.py's version) on
    # purpose: this function runs synchronously before forecasting even
    # starts, and some tournament questions are only open ~90 minutes —
    # ~200 sequential calls at ~1.2s apart would burn several minutes of
    # that window before a single forecast is made. Capped at
    # MAX_CONCURRENT_CP_FETCHES to stay polite to the API.
    #
    # NOTE: binary CP extraction via this endpoint is proven (see
    # meta_debug_ids_probe.py --single output, 2026-06-29). numeric/
    # multiple_choice extraction is NOT yet separately verified — check the
    # cp_found breakdown by type below on first real run.
    MAX_CONCURRENT_CP_FETCHES = 8
    if supported:
        import aiohttp
        headers_cp = {"Authorization": f"Token {TOURNAMENT_TOKEN}"}
        sem = asyncio.Semaphore(MAX_CONCURRENT_CP_FETCHES)
        cp_no_post_id = 0
        cp_errors = 0

        async def _fetch_one_cp(session, obj, post_id):
            nonlocal cp_no_post_id, cp_errors
            if post_id is None:
                cp_no_post_id += 1
                return
            q_type_for_target = (
                "binary" if isinstance(obj, BinaryQuestion)
                else "multiple_choice" if isinstance(obj, MultipleChoiceQuestion)
                else "numeric"
            )
            url = f"https://www.metaculus.com/api2/questions/{post_id}/"
            async with sem:
                for attempt in range(2):
                    try:
                        async with session.get(
                            url, headers=headers_cp,
                            timeout=aiohttp.ClientTimeout(total=15)
                        ) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(5)
                                continue
                            if resp.status != 200:
                                cp_errors += 1
                                return
                            data = await resp.json()
                            cp = extract_live_cp(data, q_type_for_target)
                            _set_cp(obj, cp)
                            return
                    except Exception:
                        cp_errors += 1
                        return

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                _fetch_one_cp(session, obj, post_id)
                for obj, post_id in zip(supported, supported_post_ids)
            ])

        print(f"  CP fetch (concurrent, capped at {MAX_CONCURRENT_CP_FETCHES}): "
              f"{len(supported) - cp_no_post_id - cp_errors} attempted cleanly, "
              f"{cp_no_post_id} had no post_id, {cp_errors} errored")

    cp_found_by_type: dict[str, list[int, int]] = {}
    for o in supported:
        t = type(o).__name__
        cp_found_by_type.setdefault(t, [0, 0])
        cp_found_by_type[t][1] += 1
        if getattr(o, "community_prediction_at_access_time", None) is not None:
            cp_found_by_type[t][0] += 1
    print(f"  Live CP found by type: "
          f"{ {t: f'{found}/{total}' for t, (found, total) in cp_found_by_type.items()} }")
    print(f"  Funnel: {len(questions)} open -> {dedup_skipped} already forecast, "
          f"{parse_errors} parse errors, {unsupported_type_counts or 'no'} unsupported types "
          f"-> {len(supported)} to forecast", flush=True)
    print(f"Open in tournament: {len(supported)} questions")
    print(f"New to forecast: {len(supported)}")
    return supported


# ─── Step 2a: Binary forecasting ──────────────────────────────────────────────
def build_binary_prompt(question: BinaryQuestion) -> str:
    return bf.build_user_prompt(question)


def parse_binary_response(text: str) -> float | None:
    for line in reversed(text.split('\n')):
        if 'probability:' in line.lower():
            numbers = re.findall(r'\d+\.?\d*', line)
            if numbers:
                prob = float(numbers[-1]) / 100
                return max(0.01, min(0.99, prob))
    return None


# ─── Step 2b: Numeric/discrete forecasting ────────────────────────────────────
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

    research_text = research_question(question.question_text, question.background_info or "")
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

Then end with exactly these three lines:
Low (10th percentile): <number>
Median (50th percentile): <number>
High (90th percentile): <number>
"""


def parse_numeric_response(text: str, question: NumericQuestion) -> list[float] | None:
    low = median = high = None
    for line in text.split('\n'):
        l = line.lower()
        # Match numbers that may contain thousands-separator commas, e.g. "4,500,000"
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

    # Sanity check — catch parsing failures before they become a silent garbage submission
    lower = question.lower_bound
    upper = question.upper_bound
    if not (low <= median <= high):
        print(f"  ⚠️  Parsed percentiles not ordered (low={low}, median={median}, high={high}) — rejecting")
        return None
    if median < lower or median > upper:
        print(f"  ⚠️  Parsed median {median} outside question bounds [{lower}, {upper}] — rejecting")
        return None

    std = (high - low) / (2 * 1.2816)
    mean = median
    if std <= 0:
        std = abs(mean) * 0.1 + 0.01

    cdf_size = question.cdf_size

    # Set boundary values based on open/closed bounds
    start_val = 0.001 if question.open_lower_bound else 0.0
    end_val   = 0.999 if question.open_upper_bound else 1.0

    def normal_cdf(x, mu, sigma):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))

    # Build raw CDF scaled to [start_val, end_val]
    raw = []
    for i in range(cdf_size):
        x = lower + (upper - lower) * i / (cdf_size - 1)
        raw.append(normal_cdf(x, mean, std))

    # Rescale raw CDF to fit within [start_val, end_val]
    raw_min, raw_max = raw[0], raw[-1]
    if raw_max <= raw_min:
        raw_max = raw_min + 1e-6
    cdf = [start_val + (v - raw_min) / (raw_max - raw_min) * (end_val - start_val) for v in raw]

    # Enforce minimum step size (use a safe fraction of the available range)
    min_step = (end_val - start_val) / (cdf_size * 10)
    for i in range(1, len(cdf)):
        if cdf[i] - cdf[i-1] < min_step:
            cdf[i] = cdf[i-1] + min_step

    # Final rescale if min_step enforcement pushed us over end_val
    if cdf[-1] > end_val:
        scale = end_val / cdf[-1]
        cdf = [v * scale for v in cdf]
        cdf[0] = max(cdf[0], start_val)

    return cdf


# ─── Step 2c: Multiple choice forecasting ─────────────────────────────────────
def build_multiple_choice_prompt(question: MultipleChoiceQuestion) -> str:
    options_list = "\n".join(f"  - {opt}" for opt in question.options)

    research_text = research_question(question.question_text, question.background_info or "")
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
    equal — e.g. '≤45' and 'Less than or equal to 45' must match.

    This is exactly the bug that zeroed out a real FutureEval submission
    on 2026-06-29 (Q44216, 'How many mass shootings...'): the site's real
    option was 'Less than or equal to 45', Claude wrote '≤45', and neither
    exact-match nor substring-match recognized them as the same option —
    '≤45' literally never appears as a substring of the worded version,
    since the symbol itself isn't in that text at all."""
    s = s.strip().lower()
    # Order matters: handle two-character symbols before the single ones,
    # so "<=" doesn't get split into "<" + "=" first.
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
    s = re.sub(r"[^\w\s]", " ", s)  # drop remaining punctuation (commas, periods, etc.)
    s = re.sub(r"\s+", " ", s).strip()  # collapse whitespace from the replacements above
    return s


def parse_multiple_choice_response(text: str, question: MultipleChoiceQuestion) -> dict[str, float] | None:
    """Parse option probabilities from Claude's response."""
    # Find the "Option probabilities:" section
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
        # Match "option: number%"
        match = re.match(r'^(.+?):\s*(\d+\.?\d*)\s*%?$', line)
        if not match:
            continue
        option_text = match.group(1).strip()
        prob = float(match.group(2)) / 100

        # Exact match first (case-insensitive) — avoids substring ambiguity
        # entirely whenever Claude reproduces the option text verbatim.
        matched_opt = next(
            (opt for opt in question.options if opt.lower() == option_text.lower()), None
        )
        # Fall back to substring containment on the RAW text.
        if matched_opt is None:
            matched_opt = next(
                (opt for opt in question.options
                 if opt.lower() in option_text.lower() or option_text.lower() in opt.lower()),
                None
            )
        # Final fallback: normalize symbols to words on BOTH sides before
        # comparing — catches "≤45" vs "Less than or equal to 45" and
        # similar symbol/word mismatches that survive the raw checks above.
        if matched_opt is None:
            norm_option_text = _normalize_option_text(option_text)
            matched_opt = next(
                (opt for opt in question.options
                 if _normalize_option_text(opt) == norm_option_text), None
            )
            if matched_opt is None:
                matched_opt = next(
                    (opt for opt in question.options
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

    # Don't silently drop meaningful unmatched probability mass — that's
    # exactly how a real option's share used to vanish after normalization.
    # Reject loudly instead, same philosophy as the numeric sanity check.
    if unmatched_mass > 0.05:
        print(f"  ⚠️  {unmatched_mass:.0%} of probability mass couldn't be matched to a known "
              f"option (unrecognized: {unmatched_examples}) — rejecting forecast")
        return None

    # Fill any options Claude didn't mention with 0, then normalize to 1.0
    for opt in question.options:
        probs.setdefault(opt, 0.0)
    total = sum(probs.values())
    if total <= 0:
        return None
    return {opt: probs[opt] / total for opt in question.options}


# ─── Step 3: Forecast a single question ───────────────────────────────────────
def forecast_question(question) -> tuple[str, any, str | None]:
    """
    Returns (question_type, forecast_value, raw_reasoning_text) where
    forecast_value is:
      - float for binary (probability)
      - list[float] for numeric (201-point CDF)
      - dict[str, float] for multiple_choice
      - None on failure
    raw_reasoning_text is the full model output, for logging/audit — None
    only if the API call itself failed (no response at all).
    """
    system_prompt = build_forecaster_system_prompt()

    if isinstance(question, BinaryQuestion):
        user_prompt = build_binary_prompt(question)
        q_type = "binary"
    elif isinstance(question, NumericQuestion):
        user_prompt = build_numeric_prompt(question)
        q_type = "numeric"
    elif isinstance(question, MultipleChoiceQuestion):
        user_prompt = build_multiple_choice_prompt(question)
        q_type = "multiple_choice"
    else:
        return "unsupported", None, None

    try:
        response = client_anthropic.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=cacheable_system_block(system_prompt),
            messages=[{"role": "user", "content": user_prompt}]
        )
        text = response.content[0].text

        # Visibility for the prompt-caching change above: this is the only
        # way to actually confirm caching is working rather than assuming
        # it (Haiku 4.5's 4,096-token minimum cacheable prefix means a
        # too-short system prompt would silently cache nothing, with no
        # error). cache_read > 0 means a previous call's cache was reused
        # (cheap); cache_creation > 0 with cache_read == 0 means this was
        # the first call writing a fresh cache entry (slightly more
        # expensive than uncached, recovered by the next cached read).
        # Both staying at 0 across an entire run means caching isn't
        # engaging at all — check system prompt length if that's the case.
        usage = getattr(response, "usage", None)
        if usage is not None:
            cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
            cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
            if cache_read or cache_write:
                print(f"    💰 cache: {cache_read} read, {cache_write} written")

        if q_type == "binary":
            result = parse_binary_response(text)
        elif q_type == "numeric":
            result = parse_numeric_response(text, question)
        elif q_type == "multiple_choice":
            result = parse_multiple_choice_response(text, question)
        else:
            result = None

        if result is None:
            print(f"  ⚠️  Could not parse {q_type} response for Q{question.id_of_question}")
        return q_type, result, text

    except Exception as e:
        print(f"  ❌ Claude error for Q{question.id_of_question}: {e}")
        return q_type, None, None


# ─── Step 4: Submit to Metaculus ───────────────────────────────────────────────
def submit_forecast(question, q_type: str, forecast) -> bool:
    try:
        if q_type == "binary":
            client_metaculus.post_binary_question_prediction(
                question_id=question.id_of_question,
                prediction_in_decimal=forecast
            )
            print(f"  ✅ Binary: {forecast:.0%}")

        elif q_type == "numeric":
            client_metaculus.post_numeric_question_prediction(
                question_id=question.id_of_question,
                cdf_values=forecast
            )
            # Find approximate median for display
            median_idx = next((i for i, v in enumerate(forecast) if v >= 0.5), len(forecast)//2)
            median_val = question.lower_bound + (question.upper_bound - question.lower_bound) * median_idx / (len(forecast) - 1)
            print(f"  ✅ Numeric: median≈{median_val:.2f} {question.unit_of_measure or ''}")

        elif q_type == "multiple_choice":
            client_metaculus.post_multiple_choice_question_prediction(
                question_id=question.id_of_question,
                options_with_probabilities=forecast
            )
            top = max(forecast, key=forecast.get)
            print(f"  ✅ Multiple choice: top option='{top}' ({forecast[top]:.0%})")

        return True

    except Exception as e:
        print(f"  ❌ Submission error: {str(e)[:80]}")
        return False


# ─── Step 5: Main run loop ─────────────────────────────────────────────────────
def summarize_forecast_for_alert(q_type: str, forecast) -> str:
    """Short human-readable summary for the push notification."""
    try:
        if q_type == "binary":
            return f"{forecast:.0%}"
        if q_type == "numeric":
            median_idx = next((i for i, v in enumerate(forecast) if v >= 0.5), len(forecast) // 2)
            return f"~{median_idx / (len(forecast) - 1):.0%} through range"
        if q_type == "multiple_choice":
            top = max(forecast, key=forecast.get)
            return f"{top} ({forecast[top]:.0%})"
    except Exception:
        pass
    return str(forecast)[:60]


async def run():
    print("METACULUS TOURNAMENT FORECASTER (synchronous)")
    print("=" * 50)

    questions = await fetch_tournament_questions()
    if not questions:
        print("No questions found — either none are open right now, or all already forecast.")
        return

    results   = {}
    submitted = 0
    failed    = 0
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    for i, q in enumerate(questions, 1):
        print(f"\n[{i}/{len(questions)}] Q{q.id_of_question} ({type(q).__name__}): {q.question_text[:70]}")

        q_type, forecast, reasoning_text = forecast_question(q)
        cp = getattr(q, "community_prediction_at_access_time", None)

        if forecast is None:
            results[f"q_{q.id_of_question}"] = {
                "question_id":   q.id_of_question,
                "post_id":       getattr(q, "id_of_post", None),
                "question_text": q.question_text,
                "question_type": q_type,
                "status":        "failed",
                "submitted_forecast": None,
                "reasoning":     reasoning_text,
                "community_prediction": cp,
            }
            failed += 1
            continue

        submission_ok = submit_forecast(q, q_type, forecast)

        results[f"q_{q.id_of_question}"] = {
            "question_id":   q.id_of_question,
            "post_id":       getattr(q, "id_of_post", None),
            "question_text": q.question_text,
            "question_type": q_type,
            # Reflects whether the forecast actually reached Metaculus, not
            # just whether Claude produced one — a question that's closed or
            # otherwise rejected at submission must not be logged as a
            # success, since this data feeds calibration tracking.
            "status":        "success" if submission_ok else "failed",
            # Audit trail: the actual value submitted to Metaculus, so a
            # mismatch between what got logged here and what the question
            # page displays can be checked without guessing. Shape depends
            # on q_type: float for binary, list[float] (CDF) for numeric,
            # dict[option, float] for multiple_choice.
            "submitted_forecast": forecast,
            # Full reasoning + the live CP this forecast was (or wasn't)
            # anchored against — without these, there was no way to audit
            # *why* a live tournament forecast was made, for any question
            # type. This was the single biggest gap before today's update.
            "reasoning":     reasoning_text,
            "community_prediction": cp,
        }

        if submission_ok:
            submitted += 1
            alert_summary = summarize_forecast_for_alert(q_type, forecast)
            send_alert(
                f"Q{q.id_of_question}: {alert_summary}\n{q.question_text[:100]}",
                title="New forecast submitted (tournament)"
            )
        else:
            failed += 1

        await asyncio.sleep(0.5)

    # Save results for dedup on next run
    results_file = os.path.join(TOURNAMENT_BATCH_DIR, f"batch_results_{timestamp}.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n{'=' * 50}")
    print(f"Submitted: {submitted} | Failed: {failed}")
    print(f"Results saved to {results_file}")

    # Run AFTER forecasting/submission, deliberately — FutureEval questions
    # can close in as little as 90 minutes, so nothing non-time-sensitive
    # should delay getting forecasts submitted. Checking resolutions on
    # already-forecast questions has no such urgency.
    print(f"\n{'=' * 50}")
    print("Checking for resolved questions (bot-submitted forecasts only)...")
    check_resolutions()


if __name__ == "__main__":
    asyncio.run(run())