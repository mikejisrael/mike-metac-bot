"""
test_cache_timing.py — isolated, standalone test of whether Anthropic's
real async Batch API (client.messages.batches.create) actually produces
cache HITS across multiple items sharing an identical system prompt, or
whether — as Anthropic's own docs warn — "a cache entry written during
batch processing would likely expire before the follow-up request runs."

This does NOT touch meta_batch_forecast.py, meta_refresh_forecast.py, or
tournament_forecast.py. It's a scratch experiment to get real numbers
before deciding whether padding the real system prompt in cached_llm.py
is worth doing for the two Batch-API pipelines.

Cost: trivially small. 10 batch items × ~4,200 padded system-prompt
tokens × $1/MTok (Haiku input rate) ≈ $0.04 total, even before any
caching discount. Batch API also gives a 50% discount on top of that.

USAGE (two-step, since batch completion timing is exactly what we're
testing and may not be near-instant):

  Step 1 — submit the test batch:
      python test_cache_timing.py --submit

  This saves the batch ID to test_cache_batch_id.txt and exits
  immediately (does NOT wait for completion — that's the point, we want
  to see how long it actually takes and whether that matters for caching).

  Step 2 — check status / pull results (re-run this every so often):
      python test_cache_timing.py --check

  Once the batch shows "ended", this prints the cache_creation/cache_read
  token counts for EVERY item, in submission order, plus a verdict.
"""

import argparse
import os
import sys
import time
from dotenv import load_dotenv
load_dotenv()

import anthropic

BATCH_ID_FILE = "test_cache_batch_id.txt"
MODEL = "claude-haiku-4-5"
NUM_ITEMS = 10

# Scratch padding to clear the 4,096-token floor for this test — NOT the
# real system prompt content, just filler to test the caching MECHANICS.
# If Step 2 shows this actually works, the padding used in the real fix
# should be genuinely useful content (e.g. expanded calibration examples),
# not junk like this.
PADDING = (
    "ADDITIONAL CALIBRATION REFERENCE MATERIAL (test padding, not final "
    "content):\n\n" + ("This sentence exists only to reach the token "
    "floor required for Anthropic prompt caching to activate on Haiku "
    "4.5, and has no bearing on forecast quality. ") * 130
)

SYSTEM_PROMPT = (
    "You are a professional forecaster. Answer every question with just "
    "a single percentage and nothing else.\n\n" + PADDING
)


def submit():
    client = anthropic.Anthropic()

    requests = []
    for i in range(NUM_ITEMS):
        requests.append({
            "custom_id": f"cache-test-{i}",
            "params": {
                "model": MODEL,
                "max_tokens": 10,
                "system": [
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [
                    {"role": "user", "content": f"What is the probability of test event #{i}? Answer with just a percentage."}
                ],
            },
        })

    batch = client.messages.batches.create(requests=requests)
    with open(BATCH_ID_FILE, "w") as f:
        f.write(batch.id)

    print(f"Submitted batch: {batch.id}")
    print(f"Status: {batch.processing_status}")
    print(f"Saved batch ID to {BATCH_ID_FILE}")
    print(f"\nRun 'python test_cache_timing.py --check' in a bit to see results.")
    print(f"(Batch API items can take anywhere from minutes to hours — that")
    print(f" variability is exactly what we're testing against the 5-minute")
    print(f" cache TTL, so don't assume it's stuck if it's not done yet.)")


def check():
    if not os.path.exists(BATCH_ID_FILE):
        print(f"No {BATCH_ID_FILE} found — run --submit first.")
        sys.exit(1)

    with open(BATCH_ID_FILE) as f:
        batch_id = f.read().strip()

    client = anthropic.Anthropic()
    batch = client.messages.batches.retrieve(batch_id)

    print(f"Batch: {batch_id}")
    print(f"Status: {batch.processing_status}")
    print(f"Created at: {batch.created_at}")
    print(f"Ended at: {batch.ended_at}")

    if batch.processing_status != "ended":
        print("\nStill processing — check back later. Not done yet, this is normal.")
        return

    elapsed = None
    if batch.created_at and batch.ended_at:
        elapsed = (batch.ended_at - batch.created_at).total_seconds()
        print(f"Total processing time: {elapsed:.0f} seconds ({elapsed/60:.1f} minutes)")
        print(f"(Cache TTL is 300 seconds / 5 minutes — if this run took longer than")
        print(f" that spread across items, that alone could explain zero cache hits")
        print(f" even with an otherwise-correct setup.)")

    print("\n--- Per-item cache stats (in submission order) ---")
    results_by_id = {}
    for result in client.messages.batches.results(batch_id):
        results_by_id[result.custom_id] = result

    total_cache_read = 0
    total_cache_write = 0
    total_input = 0
    any_hit = False

    for i in range(NUM_ITEMS):
        custom_id = f"cache-test-{i}"
        result = results_by_id.get(custom_id)
        if result is None:
            print(f"  {custom_id}: NO RESULT FOUND")
            continue

        if result.result.type != "succeeded":
            print(f"  {custom_id}: {result.result.type} — {getattr(result.result, 'error', 'no detail')}")
            continue

        usage = result.result.message.usage
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
        input_tokens = usage.input_tokens

        total_cache_read += cache_read
        total_cache_write += cache_write
        total_input += input_tokens
        if cache_read > 0:
            any_hit = True

        print(f"  {custom_id}: input={input_tokens}, cache_write={cache_write}, cache_read={cache_read}")

    print("\n--- Verdict ---")
    if any_hit:
        print(f"✅ At least one cache HIT occurred. Caching CAN work on this Batch API")
        print(f"   path, at least some of the time. Total cache_read tokens across the")
        print(f"   batch: {total_cache_read}. Worth padding the real system prompt.")
    else:
        print(f"❌ ZERO cache hits across all {NUM_ITEMS} items — every single one paid")
        print(f"   the cache-WRITE premium (1.25x) with no offsetting reads. This confirms")
        print(f"   Anthropic's own warning: Batch API timing doesn't reliably land within")
        print(f"   the 5-minute cache TTL. Padding meta_batch_forecast.py's / meta_refresh_")
        print(f"   forecast.py's system prompt would likely make costs slightly WORSE, not")
        print(f"   better — recommend skipping caching for these two pipelines and logging")
        print(f"   it as a tournament_forecast.py-only fix for whenever that file is unprotected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--submit", action="store_true", help="Submit the test batch")
    group.add_argument("--check", action="store_true", help="Check status / print results")
    args = parser.parse_args()

    if args.submit:
        submit()
    else:
        check()