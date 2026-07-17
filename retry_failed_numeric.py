"""
retry_failed_numeric.py — one-off retry for numeric forecasts that were
computed correctly by Claude but REJECTED by the parse_numeric_response()
bug fixed in tournament_forecast_v2.py on 2026-07-10 (forward line-scan
locking onto an earlier, coincidental "low"/"10" etc. mention instead of
the model's actual final answer — see that file's parse_numeric_response
docstring for the full writeup).

Re-parses the ALREADY-SAVED reasoning text from a previous run's
batch_results file with the FIXED parser and submits directly. Makes
ZERO new Claude/OpenRouter calls — the forecasting work was already done
correctly and already paid for; only the parsing was broken. Only two
kinds of calls happen here, both free: refetching the question object
from Metaculus (to get lower_bound/upper_bound/etc for the sanity-check/
clamping logic in parse_numeric_response) and submitting the prediction.

Usage:
    python retry_failed_numeric.py tournament_batches_v2\\batch_results_20260710_1757.json

Disposable diagnostic script — delete after use, per usual practice.
"""

import sys
import json
from datetime import datetime, timezone

import tournament_forecast_v2 as tf2


def main(results_file: str):
    with open(results_file) as f:
        results = json.load(f)

    candidates = [
        r for r in results.values()
        if r.get("status") == "failed"
        and r.get("question_type") == "numeric"
        and r.get("reasoning")
    ]
    print(f"Found {len(candidates)} failed numeric result(s) with saved reasoning to retry")
    print("(zero new Claude/OpenRouter calls — reusing already-paid-for reasoning text)\n")

    retried = 0
    still_failed = 0

    for r in candidates:
        qid = r["question_id"]
        post_id = r["post_id"]
        print(f"Q{qid}: refetching question object for bounds (free Metaculus call, no LLM)...")

        if post_id is None:
            print(f"  ⚠️  No post_id on file for Q{qid} — can't refetch, skipping")
            still_failed += 1
            continue

        try:
            questions = tf2.client_metaculus.get_question_by_post_id(
                post_id=post_id, group_question_mode="unpack_subquestions"
            )
        except Exception as e:
            print(f"  ⚠️  Could not refetch post {post_id}: {e}")
            still_failed += 1
            continue

        if not isinstance(questions, list):
            questions = [questions]
        match = next((q for q in questions if q.id_of_question == qid), None)
        if match is None:
            print(f"  ⚠️  Q{qid} not found in refetched post {post_id} — question may have "
                  f"closed, resolved, or the group structure changed since the original run")
            still_failed += 1
            continue

        forecast = tf2.parse_numeric_response(r["reasoning"], match)
        if forecast is None:
            print(f"  ⚠️  STILL rejected even with the fixed parser — this one is a genuine "
                  f"model output issue, not the bug we fixed. Leaving as failed.")
            still_failed += 1
            continue

        ok = tf2.submit_forecast(match, "numeric", forecast)
        if ok:
            print(f"  ✅ Re-submitted Q{qid}")
            retried += 1
            r["status"] = "success"
            r["submitted_forecast"] = forecast
            r["submitted_at"] = datetime.now(timezone.utc).isoformat()
            r["retried_2026_07_10_parser_fix"] = True  # audit trail: this record was
            # originally rejected by the parser bug, not re-forecast from scratch
        else:
            print(f"  ❌ Metaculus rejected the submission for Q{qid} (see error above)")
            still_failed += 1

    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*50}")
    print(f"Retried successfully: {retried} | Still failed: {still_failed}")
    print(f"Results file updated in place: {results_file}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python retry_failed_numeric.py <results_file.json>")
    main(sys.argv[1])