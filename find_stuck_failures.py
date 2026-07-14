"""
find_stuck_failures.py -- scans local batch_results history for questions
whose LATEST recorded attempt is "failed" on a non-Market-Pulse tournament.

WHY THIS MATTERS: the dedup bug fixed 2026-07-15 meant any FAILED
submission on FutureEval/ACX2026/Climate/Metaculus Cup got silently and
PERMANENTLY treated as "already forecast" -- no retry ever happened again,
even on the very next cron tick, even though nothing was ever successfully
submitted to Metaculus. This bug existed since 2026-07-11 (the date of the
comment that INCORRECTLY claimed it was already fixed). This script finds
every question that might still be sitting in that stuck state, so they
can be checked and, if still open, manually retried.

Market Pulse is excluded from this scan -- it never had this bug (its own
"status==failed always retries" branch worked correctly the whole time);
this is specifically about the OTHER tournaments where the fix was
missing. Market Pulse questions are identified the same way
check_market_pulse_participation.py already does: "biweekly" appearing in
the question text (every Market Pulse question shares that exact phrasing
in the resolution mechanic).

Usage:
    python find_stuck_failures.py
"""

import glob
import json
import os

BATCH_DIR = "tournament_batches"

files = sorted(glob.glob(os.path.join(BATCH_DIR, "batch_results_*.json")))
if not files:
    raise SystemExit(f"No batch_results_*.json files found in {BATCH_DIR}/ -- "
                      f"check you're running this from the same folder as tournament_forecast_v2.py.")

print(f"Scanning {len(files)} batch_results file(s) in {BATCH_DIR}/...\n")

# Latest-record-wins, same pattern already_done_raw uses -- files are
# processed in chronological (filename) order, so a later file's record
# for the same question_id always overwrites an earlier one.
latest_by_qid = {}
source_file_by_qid = {}

for path in files:
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        print(f"  \u26a0\ufe0f  Could not read {path}: {e}")
        continue
    for record in data.values():
        qid = record.get("question_id")
        if qid is None:
            continue
        latest_by_qid[qid] = record
        source_file_by_qid[qid] = os.path.basename(path)

print(f"Found {len(latest_by_qid)} distinct question_id(s) across all history.\n")

stuck = []
for qid, record in latest_by_qid.items():
    text = record.get("question_text", "")
    is_market_pulse = "biweekly" in text.lower()
    if is_market_pulse:
        continue  # never had this bug -- its own failed-always-retries branch worked
    if record.get("status") == "failed":
        stuck.append((qid, record))

print("=" * 60)
if not stuck:
    print("None found -- no non-Market-Pulse question is currently stuck on a "
          "failed status. Either nothing has failed on those tournaments, or "
          "everything that failed was later successfully retried.")
else:
    print(f"\u26a0\ufe0f  {len(stuck)} question(s) potentially STUCK (latest attempt = failed, "
          f"never successfully retried due to the dedup bug):\n")
    for qid, record in sorted(stuck, key=lambda x: x[1].get("submitted_at") or ""):
        post_id = record.get("post_id")
        submitted_at = record.get("submitted_at", "unknown time")
        text = record.get("question_text", "")[:80]
        src = source_file_by_qid.get(qid, "?")
        print(f"  Post {post_id} (Q{qid}) -- failed at {submitted_at} (from {src})")
        print(f"    {text}")
        if post_id:
            print(f"    https://www.metaculus.com/questions/{post_id}/")
        print()

    print("=" * 60)
    print("Next step: run 'python tournament_forecast_v2.py' normally (no --ids")
    print("needed) -- with today's fix, any of these still open will now be")
    print("correctly retried automatically. Any that already closed in the")
    print("meantime can't be recovered, but are listed above so you know what")
    print("was missed.")