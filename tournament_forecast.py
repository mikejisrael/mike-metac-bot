"""
tournament_forecast.py — Tournament forecaster using synchronous Claude calls.

Handles binary, numeric, discrete, and multiple_choice question types.
Calls Claude synchronously and submits to Metaculus immediately in the same run.
This is essential for tournament questions that may only be open for 90 minutes.

Usage:
  python tournament_forecast.py          # forecast and submit all open questions

Choosing the tournament:
  Defaults to summer-futureeval-2026 (numeric ID 33022).
  Override without editing:
      set METAC_TOURNAMENT_ID=32977      (bot-testing-area)
"""

import asyncio
import os
import re
import glob
import json
import math
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import anthropic
import batch_forecast as bf
from forecasting_tools import (
    MetaculusApi, MetaculusClient,
    BinaryQuestion, NumericQuestion, MultipleChoiceQuestion
)
from cached_llm import build_forecaster_system_prompt

# ─── Config ───────────────────────────────────────────────────────────────────
TOURNAMENT_ID        = os.getenv("METAC_TOURNAMENT_ID", "33022")
TOURNAMENT_BATCH_DIR = "tournament_batches"
MODEL                = "claude-haiku-4-5"
MAX_TOKENS           = 2000

# ─── Clients ──────────────────────────────────────────────────────────────────
client_anthropic = anthropic.Anthropic()
# Tournament submissions should authenticate as the dedicated bot account
# (mike_iz_-bot), not the shared personal-account token batch_forecast.py
# uses. Set METAC_TOURNAMENT_TOKEN in .env once mike_iz_-bot's own API token
# is generated; falls back to METACULUS_TOKEN so this doesn't silently break
# before that's set up.
TOURNAMENT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if os.getenv("METAC_TOURNAMENT_TOKEN"):
    print("Auth: using dedicated METAC_TOURNAMENT_TOKEN (mike_iz_-bot)")
else:
    print("Auth: METAC_TOURNAMENT_TOKEN not set — falling back to shared METACULUS_TOKEN (mike_iz_)")
client_metaculus = MetaculusClient(token=TOURNAMENT_TOKEN)

# ─── Redirect batch_forecast's namespace (keeps dedup isolated) ───────────────
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

    print(f"Tournament: {TOURNAMENT_ID}")
    print(f"Excluding {len(already_done)} already-forecast questions (tournament folder)...")

    import requests
    headers = {"Authorization": f"Token {TOURNAMENT_TOKEN}"}
    r = requests.get(
        f"https://www.metaculus.com/api/posts/?tournaments={TOURNAMENT_ID}&limit=100",
        headers=headers, timeout=30
    )
    raw_posts = r.json().get("results", [])

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
            supported.append(obj)
        except Exception as e:
            print(f"  ⚠️  Could not parse Q{q.get('id')}: {e}")

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

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

{bounds_desc}
Today is {datetime.now().strftime("%Y-%m-%d")}.

This is a NUMERIC forecasting question. Reason through it carefully, then provide your estimate.

Before answering write:
(a) Time left until resolution
(b) Most likely outcome and why
(c) What would push the value lower?
(d) What would push the value higher?
(e) Base rate or historical reference values

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
    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

Options:
{options_list}

Today is {datetime.now().strftime("%Y-%m-%d")}.

This is a MULTIPLE CHOICE forecasting question. Reason through each option carefully.

Before answering write:
(a) Time left until resolution
(b) Most likely outcome and why
(c) Key uncertainties that could change the outcome

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
def forecast_question(question) -> tuple[str, any]:
    """
    Returns (question_type, forecast_value) where forecast_value is:
      - float for binary (probability)
      - list[float] for numeric (201-point CDF)
      - dict[str, float] for multiple_choice
      - None on failure
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
        return "unsupported", None

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
        return q_type, result

    except Exception as e:
        print(f"  ❌ Claude error for Q{question.id_of_question}: {e}")
        return q_type, None


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

        q_type, forecast = forecast_question(q)

        results[f"q_{q.id_of_question}"] = {
            "question_id":   q.id_of_question,
            "question_text": q.question_text,
            "question_type": q_type,
            "status":        "failed" if forecast is None else "success",
            # Audit trail: the actual value submitted to Metaculus, so a
            # mismatch between what got logged here and what the question
            # page displays can be checked without guessing. Shape depends
            # on q_type: float for binary, list[float] (CDF) for numeric,
            # dict[option, float] for multiple_choice.
            "submitted_forecast": forecast,
        }

        if forecast is None:
            failed += 1
            continue

        if submit_forecast(q, q_type, forecast):
            submitted += 1
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