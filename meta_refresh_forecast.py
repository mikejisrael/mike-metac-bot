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
fetched question object for the actual submission — so there's no guessing
or title-matching involved for this path.
(The older fetch_question_by_id() below, used only by --submit/--check for
re-fetching STALE/CLOSING_SOON questions from local history, still keys off
the stored "question_id" value against the /api2/questions/{id}/ endpoint.
Per a prior, separately-confirmed investigation that endpoint matches on
POST id — so for any historical entry where post id and question id differ,
that path can silently miss or mismatch. titles_match() catches genuine
mismatches and skips them, which is most likely the real explanation behind
at least some of the "ID likely recycled" messages seen in the past — not
necessarily actual ID recycling. Flagging this here rather than fixing it
now since it's a separate, larger cleanup from the --single feature.)
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

load_dotenv()

from forecasting_tools import MetaculusClient, BinaryQuestion
from live_data import detect_data_needs, format_live_data_for_prompt
from cached_llm import build_forecaster_system_prompt

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

# ─── Config ───────────────────────────────────────────────────────────────────
MODEL = "claude-haiku-4-5"
MAX_TOKENS = 2000
CLOSING_SOON_DAYS = 14
STALE_DAYS = 30
BATCH_DIR = "Meta batches"
REFRESH_BATCH_PREFIX = os.path.join(BATCH_DIR, "batch_jobs_refresh")
REFRESH_RESULTS_PREFIX = os.path.join(BATCH_DIR, "batch_results_refresh")


def ensure_batch_dir():
    os.makedirs(BATCH_DIR, exist_ok=True)


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


def build_community_context(days_remaining: float, total_days: float, cp: float | None) -> str:
    """Return a prompt fragment instructing the model how much to weight the
    community prediction, scaling with how close the question is to closing."""
    if cp is None:
        return ""
    w = community_weight(days_remaining, total_days)
    pct = f"{cp:.0%}"
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
            "information at this stage. Only deviate if your research reveals "
            "a clear recent development the community hasn't priced in yet.\n"
        )
    else:
        return (
            f"\nCurrent community prediction: {pct} (very high weight — {w:.0%}). "
            "Stay within 5-10 percentage points of this unless you find something "
            "genuinely explosive and recent that is clearly not yet reflected. "
            "At this late stage the community aggregation is more reliable than "
            "independent web search.\n"
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

            for custom_id, q_id in question_ids.items():
                all_forecasts.append({
                    "custom_id":      custom_id,
                    "question_id":    q_id,
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
        seen_question_ids.add(q_id)

        if f["probability"] is None:
            continue

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
    question_id: int, expected_text: str | None = None
) -> BinaryQuestion | None:
    """Fetch a question by ID using the path-based detail endpoint
    (/api2/questions/{id}/). See the post-id-vs-question-id caveat in this
    file's module docstring — this is the path still used by --submit/--check
    for re-fetching STALE/CLOSING_SOON questions from local history, where
    only the stored question_id (not post_id) is on file.

    If expected_text is provided (the title we have on file from when we
    originally forecast it), this also verifies the fetched question's
    title actually matches before returning it — a safety net in case the ID
    being queried doesn't correspond to the question we think it does."""
    try:
        headers = {"Authorization": f"Token {ACTIVE_TOKEN}"}
        url = f"https://www.metaculus.com/api2/questions/{question_id}/"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 404:
                    print(f"    ℹ️  Q{question_id}: 404 — question no longer exists (retired/removed). Skipping.")
                    return None
                if resp.status != 200:
                    print(f"    ⚠️  Q{question_id}: unexpected status {resp.status}")
                    return None
                fetched = await resp.json()
                fetched_title = fetched.get("title") or (fetched.get("question") or {}).get("title") or ""
                if expected_text:
                    if not titles_match(expected_text, fetched_title):
                        print(f"    🛑 MISMATCH on Q{question_id}: stored title vs API title don't match.")
                        print(f"       Stored:  {expected_text[:90]}")
                        print(f"       API:     {fetched_title[:90]}")
                        print(f"       Skipping — will NOT forecast on the wrong question. "
                              f"This may be a post-id/question-id mismatch rather than true ID recycling.")
                        return None
                return BinaryQuestion.from_metaculus_api_json(fetched)
    except Exception as e:
        print(f"  Warning: could not fetch Q{question_id}: {e}")
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

    cp = getattr(question, 'community_prediction_at_access_time', None)
    community = build_community_context(days_to_close, total_days, cp)

    if original_prob is not None:
        original_note = f"\nNote: This question was previously forecast at {original_prob:.0%}. Review whether this remains appropriate given current information.\n"
    else:
        original_note = "\nNote: No prior forecast on file for this question — treat this as a fresh, independent forecast.\n"

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

{live_data_text}

{community}
{original_note}

Refresh reason: {refresh_reason}
Today is {datetime.now().strftime("%Y-%m-%d")}.

Before answering write:
(a) Time left until resolution
(b) Status quo outcome if nothing changes
(c) Scenario for NO outcome
(d) Scenario for YES outcome
(e) Base rate — how often do similar events occur?
(f) How current data/news moves you from base rate
(g) How your view has changed (or not) since the original forecast
(h) If community prediction exists and differs >10%, explain why you diverge

The last thing you write is: "Probability: ZZ%"
"""


# ─── Submit refresh batch (BATCH mode — 24h turnaround) ──────────────────────
async def submit_refresh_batch(to_refresh: list[dict]):
    ensure_batch_dir()
    print(f"\nFetching fresh data for {len(to_refresh)} questions...")

    system_prompt = build_forecaster_system_prompt()
    requests = []
    question_map = {}

    for i, forecast in enumerate(to_refresh):
        q_id = forecast["question_id"]
        print(f"  [{i+1}/{len(to_refresh)}] Fetching Q{q_id}...")

        question = await fetch_question_by_id(q_id, expected_text=forecast.get("question_text"))
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
                "system": system_prompt,
                "messages": [{"role": "user", "content": prompt}]
            }
        })

        await asyncio.sleep(0.2)

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
        "resolve_times": {
            custom_id: info["question"].scheduled_resolution_time.isoformat()
            if info["question"].scheduled_resolution_time else None
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
    for result in client_anthropic.messages.batches.results(batch_id):
        custom_id = result.custom_id

        if result.result.type == "succeeded":
            text = result.result.message.content[0].text
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
            }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    results_file = f"{REFRESH_RESULTS_PREFIX}_{timestamp}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved results to {results_file}")

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
    path — no 24h batch wait, since the point of --single is reacting now."""
    system_prompt = build_forecaster_system_prompt()
    response = client_anthropic.messages.create(
        model=MODEL,
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
    scans pick this up automatically. Also records post_id alongside
    question_id — existing history doesn't have post_id on file, but every
    new entry from here on will, narrowing the gap described in this file's
    module docstring."""
    ensure_batch_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    custom_id = f"single_{q_id}_{timestamp}"

    batch_info = {
        "batch_id": custom_id,
        "submitted_at": datetime.now().isoformat(),
        "batch_type": "single",
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
            "question_text": question.question_text,
            "question_type": "binary",
            "probability": prob,
            "submitted_forecast": prob,
            "original_prob": original_prob,
            "refresh_reason": refresh_reason,
            "reasoning": reasoning,
            "status": "success",
        }
    }
    results_file = os.path.join(BATCH_DIR, f"batch_results_{timestamp}.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"  Saved to {results_file}")


def run_single():
    """python meta_refresh_forecast.py --single
    Prompts for a Metaculus URL or post ID, fetches it via the post-id-based
    get_question_by_post_id, shows your last forecast (if any) next to the
    current community prediction, gets a fresh forecast from Claude
    synchronously, confirms, then submits."""
    raw = input("Paste the Metaculus question URL or post ID: ").strip()
    post_id = parse_post_id(raw)
    if post_id is None:
        print(f"Could not find a post ID in '{raw}'. Paste the full URL or just the numeric ID.")
        return

    print(f"Fetching post {post_id} from Metaculus...")
    try:
        result = client_metaculus.get_question_by_post_id(post_id)
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

    all_forecasts = load_all_batches()
    prior = [
        f for f in all_forecasts
        if f["question_id"] == q_id and f.get("probability") is not None
    ]
    prior.sort(key=lambda f: f.get("submitted_at") or "", reverse=True)
    original_prob = prior[0]["probability"] if prior else None

    cp = getattr(question, "community_prediction_at_access_time", None)
    print(f"  Your last forecast on file: "
          f"{f'{original_prob:.0%}' if original_prob is not None else 'none found'}")
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

    print(f"\n  New forecast: {prob:.0%}{change_str}")
    confirm = input("  Submit this to Metaculus? [Y/n]: ").strip().lower()
    if confirm == "n":
        print("  Cancelled — nothing submitted.")
        return

    try:
        client_metaculus.post_binary_question_prediction(
            question_id=q_id, prediction_in_decimal=prob
        )
        print(f"  ✅ Submitted {prob:.0%} on question {q_id} (post {confirmed_post_id})")
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
