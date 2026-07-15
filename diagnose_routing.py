"""
One-off diagnostic — run from C:\\Users\\mikej\\metac-bot-template.

Replicates exactly what meta_dashboard.py's load_local_results() does
(scan every batch_results_*.json across all three result directories, keep
whichever file is most recent PER question_id), then reports which
directory "won" for each of the 10 questions from today's mixed-routing
selection. That winning directory is exactly what _make_row's is_batch_path
check looks at -- whichever one it is explains the routing you saw.

Usage:
    python diagnose_routing.py
"""
import glob
import json
import os

LOCAL_RESULT_DIRS = ["tournament_batches", "tournament_batches_v2", "meta batches"]

TARGET_IDS = [43984, 43347, 43688, 8050, 7872, 7861, 43992, 43911, 43360, 43865]

# question_id -> (winning_file, winning_dir, submitted_at)
winners = {}

for d in LOCAL_RESULT_DIRS:
    for rf in sorted(glob.glob(os.path.join(d, "batch_results_*.json"))):
        try:
            with open(rf, encoding="utf-8") as f:
                results = json.load(f)
        except Exception as e:
            print(f"  (skipping unreadable {rf}: {e})")
            continue

        mtime = os.path.getmtime(rf)

        for custom_id, r in results.items():
            q_id = r.get("question_id")
            if q_id not in TARGET_IDS:
                continue
            has_forecast = (
                r.get("probability") is not None
                if r.get("question_type", "binary") == "binary"
                else r.get("probabilities") is not None
            )
            if not has_forecast:
                continue
            prev = winners.get(q_id)
            if prev is None or mtime > prev[2]:
                winners[q_id] = (rf, d, mtime)

print(f"{'question_id':<12} {'winning directory':<22} {'winning file'}")
print("-" * 70)
for q_id in TARGET_IDS:
    if q_id in winners:
        rf, d, _ = winners[q_id]
        flag = "" if d == "meta batches" else "  <-- NOT batch-path (this is the one)"
        print(f"{q_id:<12} {d:<22} {rf}{flag}")
    else:
        print(f"{q_id:<12} {'NO LOCAL RESULT FOUND':<22}")