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

# ─── Config ───────────────────────────────────────────────────────────────────
MODEL = "claude-haiku-4-5"
MAX_TOKENS = 2000
NUM_QUESTIONS = 50
DAYS_AHEAD = 365
MIN_FORECASTERS = 5
BATCH_DIR = "Meta batches"
BATCH_FILE = os.path.join(BATCH_DIR, "batch_jobs.json")
RESULTS_FILE = os.path.join(BATCH_DIR, "batch_results.json")

# Tournament(s) to pull questions from. ApiFilter.allowed_tournaments accepts
# a list of str|int (numeric ID or slug), so adding more is just adding here.
# tournament_forecast.py imports this same list as its single source of truth.
#   33022                        = Summer 2026 FutureEval Bot Tournament
#   "ACX2026"                    = ACX 2026 Prediction Contest
#   "climate"                    = Climate Tipping Points
#   "metaculus-cup-summer-2026"  = Metaculus Cup Summer 2026 (bots can forecast
#                                  here for calibration data, but are NOT prize-
#                                  eligible in this one — humans-only for prizes)
ALLOWED_TOURNAMENTS = [33022, "ACX2026", "climate", "metaculus-cup-summer-2026"]


def ensure_batch_dir():
    os.makedirs(BATCH_DIR, exist_ok=True)


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
    except ValueError:
        # Fewer questions available than requested — fetch whatever exists
        questions = await client_metaculus.get_questions_matching_filter(
            api_filter=api_filter,
            num_questions=50,  # fallback to base amount
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
                "system": system_prompt,
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

            results[custom_id] = {
                "question_id":   batch_info['question_ids'][custom_id],
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
                "status":        "success"
            }
        else:
            results[custom_id] = {
                "question_id":   batch_info['question_ids'][custom_id],
                "question_text": batch_info['question_texts'][custom_id],
                "question_type": "binary",
                "probability":   None,
                "submitted_forecast": None,
                "community_prediction": batch_info.get("community_predictions", {}).get(custom_id),
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
    """Fetch community predictions for the latest batch."""
    if not os.path.exists(BATCH_FILE):
        print(f"No {BATCH_FILE} found. Run a batch first.")
        return

    with open(BATCH_FILE) as f:
        batch_info = json.load(f)

    question_ids = batch_info.get("question_ids", {})
    community_preds = batch_info.get("community_predictions", {})

    already_filled = sum(1 for v in community_preds.values() if v is not None)
    print(f"Updating community predictions ({already_filled}/{len(question_ids)} already filled)...")

    headers = {"Authorization": f"Token {ACTIVE_TOKEN}"}
    updated = 0
    still_hidden = 0

    ids_to_fetch = [
        q_id for custom_id, q_id in question_ids.items()
        if community_preds.get(custom_id) is None
    ]

    print(f"Fetching {len(ids_to_fetch)} questions in chunks of 10...")

    async with aiohttp.ClientSession() as session:
        chunk_size = 10
        for i in range(0, len(ids_to_fetch), chunk_size):
            chunk = ids_to_fetch[i:i + chunk_size]
            ids_str = ",".join(str(q_id) for q_id in chunk)
            url = f"https://www.metaculus.com/api2/questions/?ids={ids_str}&limit={chunk_size}"

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
                            print(f"  ❌ HTTP {resp.status}")
                            break

                        data = await resp.json()
                        results = data.get("results", [])

                        for item in results:
                            q = item.get("question", {})
                            q_id = q.get("id") or item.get("id")
                            agg = q.get("aggregations", {}).get("recency_weighted", {}).get("latest")
                            cp = None
                            if agg:
                                centers = agg.get("centers", [])
                                if len(centers) > 1:
                                    cp = centers[1]
                                elif centers:
                                    cp = centers[0]

                            for custom_id, cid in question_ids.items():
                                if cid == q_id:
                                    if cp is not None:
                                        community_preds[custom_id] = cp
                                        updated += 1
                                        q_text = batch_info.get("question_texts", {}).get(custom_id, "")[:50]
                                        print(f"  ✅ Q{q_id}: {cp:.0%} — {q_text}")
                                    else:
                                        still_hidden += 1
                                    break
                        break

                except Exception as e:
                    print(f"  ❌ Chunk error: {e}")
                    break

            print(f"  Chunk {i//chunk_size + 1}/{(len(ids_to_fetch) + chunk_size - 1)//chunk_size} done")
            await asyncio.sleep(3)

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
    print(f"   {filled}/{len(question_ids)} total filled | {still_hidden} still hidden")
    if still_hidden > 0:
        print(f"   Run again later to pick up remaining hidden predictions")


# ─── Main ──────────────────────────────────────────────────────────────────────
async def main():
    print("METACULUS BATCH FORECASTER")
    print("=" * 50)
    questions = await fetch_questions()
    if questions:
        await submit_batch(questions)
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