"""
refresh_forecasts.py — Re-forecast questions that need updating.

Triggers:
  1. CLOSING SOON  — question closes within 14 days (configurable)
  2. STALE         — original forecast is older than 30 days (configurable)

Usage:
  python refresh_forecasts.py           # dry run — shows what would be re-forecast
  python refresh_forecasts.py --submit  # submits a new batch to Anthropic
  python refresh_forecasts.py --check   # retrieves completed refresh batch results
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
    (/api2/questions/{id}/), which does a real lookup-by-ID.

    NOTE: the old approach used /api2/questions/?ids={id}&limit=1 — a
    list-filter query. That filter turned out to be broken/ignored on
    Metaculus's side: it was returning an arbitrary question regardless of
    the ids= value (confirmed by querying a nonsense ID and getting back a
    real question instead of an empty result). That's what caused several
    refresh forecasts to silently be built from the WRONG question's
    content. The path-based endpoint below does a genuine per-ID lookup —
    verified to return a clean 404 for both retired and nonsense IDs.

    If expected_text is provided (the title we have on file from when we
    originally forecast it), this also verifies the fetched question's
    title actually matches before returning it — a second, independent
    safety net in case any ID has genuinely drifted to a different
    question on Metaculus's side."""
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
                              f"This ID may have drifted to a different question on Metaculus's side.")
                        return None
                return BinaryQuestion.from_metaculus_api_json(fetched)
    except Exception as e:
        print(f"  Warning: could not fetch Q{question_id}: {e}")
        return None


# ─── Build prompt ─────────────────────────────────────────────────────────────
def build_refresh_prompt(
    question: BinaryQuestion,
    original_prob: float,
    refresh_reason: str,
    days_to_close: float = 30,
    total_days: float = 365,
) -> str:
    live_data = detect_data_needs(question.question_text)
    live_data_text = format_live_data_for_prompt(live_data)

    cp = getattr(question, 'community_prediction_at_access_time', None)
    community = build_community_context(days_to_close, total_days, cp)

    original_note = f"\nNote: This question was previously forecast at {original_prob:.0%}. Review whether this remains appropriate given current information.\n"

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


# ─── Submit refresh batch ─────────────────────────────────────────────────────
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
    print(f"   Run: python refresh_forecasts.py --check to retrieve results")


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


# ─── Submit to Metaculus ──────────────────────────────────────────────────────
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
    else:
        asyncio.run(main(submit=False))