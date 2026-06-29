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
from meta_cp_extract import extract_live_cp
from meta_alerts import send_alert
from meta_research import research_question
from live_data import detect_data_needs, format_live_data_for_prompt

# ─── Config ───────────────────────────────────────────────────────────────────
# Tournament list comes from meta_batch_forecast.ALLOWED_TOURNAMENTS — single
# source of truth shared between both scripts. Override without editing either
# file: set METAC_TOURNAMENT_IDS to a comma-separated list, e.g.
#     set METAC_TOURNAMENT_IDS=32977
# (32977 = bot-testing-area, useful for testing in isolation from real tournaments)
_env_override = os.getenv("METAC_TOURNAMENT_IDS")
if _env_override:
    TOURNAMENT_IDS = [t.strip() for t in _env_override.split(",") if t.strip()]
else:
    TOURNAMENT_IDS = bf.ALLOWED_TOURNAMENTS
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
                for attempt in range(3):
                    print(f"  ...fetching {tid} (attempt {attempt + 1}/3)...", flush=True)
                    r = requests.get(url, headers=headers, timeout=30)
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
        except Exception as e:
            print(f"  ⚠️  Could not fetch tournament {tid}: {e}", flush=True)

    raw_posts = list(raw_posts_by_id.values())

    now = datetime.now(timezone.utc)
    questions = []
    for post in raw_posts:
        q = post.get("question")
        if not q:
            continue
        # Skip if close time has passed
        close_time = q.get("scheduled_close_time")
        if close_time:
            close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            if close_dt < now:
                continue
        questions.append(post)

    # Convert to library question objects using from_metaculus_api_json
    from forecasting_tools import NumericQuestion, MultipleChoiceQuestion
    supported = []
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
                continue
            if obj.id_of_question in already_done:
                stored_title = already_done[obj.id_of_question]
                if titles_match(stored_title, obj.question_text):
                    continue  # genuine duplicate — already forecast this question
                print(f"  🛑 Q{obj.id_of_question}: ID was previously forecast under a different "
                      f"title — treating as a NEW question (ID likely recycled).")
                print(f"       Previously: {stored_title[:90]}")
                print(f"       Now:        {obj.question_text[:90]}")
            # Live CP, extracted from the post data we already fetched above
            # (no extra API call). Previously never set anywhere in this
            # file, meaning binary's CP-anchoring instructions (in
            # bf.build_user_prompt) were always operating on None despite
            # existing. NOTE: the field path this relies on is confirmed
            # for binary, best-effort/unverified for numeric and
            # multiple_choice in this sandbox — check the printed hit rate
            # below on first real run.
            obj.community_prediction_at_access_time = extract_live_cp(post, q_type)
            supported.append(obj)
        except Exception as e:
            print(f"  ⚠️  Could not parse Q{q.get('id')}: {e}")

    cp_found = sum(1 for o in supported if getattr(o, "community_prediction_at_access_time", None) is not None)
    print(f"  Live CP found for {cp_found}/{len(supported)} questions before forecasting")
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
        # Fall back to substring containment only if no exact match.
        if matched_opt is None:
            matched_opt = next(
                (opt for opt in question.options
                 if opt.lower() in option_text.lower() or option_text.lower() in opt.lower()),
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
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        text = response.content[0].text

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


if __name__ == "__main__":
    asyncio.run(run())