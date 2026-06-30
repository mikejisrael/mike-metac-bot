"""
diagnose_missing.py — one-off diagnostic.

Run this from C:\\Users\\mikej\\metac-bot-template (same folder as meta_refresh_forecast.py)
with venv312 active:

    python diagnose_missing.py

It scans every batch_jobs*.json in "Meta batches/" and, for each of the 11
question IDs that meta_status.py shows as closing soon but meta_refresh_forecast.py
is NOT picking up, prints exactly what's stored: resolve_time, probability
(via the same lookup meta_refresh_forecast.py uses), submitted_at, and which
file it came from. This will show us whether resolve_time or probability is
the missing piece, and whether it's missing in every file or just some.
"""

import json
import glob
import os

BATCH_DIR = "Meta batches"

MISSING_IDS = [43897, 43288, 41544, 43182, 43911, 43910, 43909, 43888, 44016, 43937, 43342]


def build_probability_index():
    index = {}
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
            print(f"  (could not load {rf}: {e})")
    return index


def main():
    probability_index = build_probability_index()

    job_files = sorted(
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )

    # custom_id -> list of (job_file, q_id, resolve_time, submitted_at, probability)
    found = {qid: [] for qid in MISSING_IDS}

    for job_file in job_files:
        try:
            with open(job_file) as f:
                batch_info = json.load(f)
        except Exception as e:
            print(f"  (could not load {job_file}: {e})")
            continue

        submitted_at = batch_info.get("submitted_at", "")
        question_ids = batch_info.get("question_ids", {})
        resolve_times = batch_info.get("resolve_times", {})

        for custom_id, q_id in question_ids.items():
            if q_id in found:
                prob = probability_index.get(custom_id)
                rt = resolve_times.get(custom_id)
                found[q_id].append((job_file, custom_id, rt, submitted_at, prob))

    print("=" * 70)
    for qid in MISSING_IDS:
        entries = found[qid]
        print(f"\nQ{qid}:")
        if not entries:
            print("  NOT FOUND in any batch_jobs file at all.")
            continue
        for job_file, custom_id, rt, submitted_at, prob in entries:
            print(f"  file={job_file}")
            print(f"    custom_id={custom_id}  resolve_time={rt!r}  "
                  f"submitted_at={submitted_at!r}  probability={prob!r}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()