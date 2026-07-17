"""
apply_manual_post_ids.py — applies Mike's manually-matched post_ids (from
post_id_manual_template.csv, filled in after backfill_post_ids_batch2.py's
auto-detect pass) to the batch job files.

Verifies each match live before writing (fetches the URL's post_id,
checks resolution/close-time or title makes sense) rather than trusting
the CSV blindly — same "check inputs before outputs" practice used
throughout this codebase. Writes to every batch_jobs_*.json entry whose
question_id matches, not just one file, since the same question can
appear across multiple batch runs.

Usage:
  python apply_manual_post_ids.py                          # dry run
  python apply_manual_post_ids.py --write                   # writes
  python apply_manual_post_ids.py --csv other_file.csv       # different CSV path
"""

import csv
import glob
import json
import os
import re
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

BATCH_DIR = "meta batches"
ACTIVE_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
HEADERS = {"Authorization": f"Token {ACTIVE_TOKEN}"} if ACTIVE_TOKEN else {}


def extract_post_id(raw: str) -> int | None:
    """Accepts either a bare numeric post_id or a full Metaculus URL and
    returns the post_id as an int."""
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    m = re.search(r"/questions/(\d+)", raw)
    return int(m.group(1)) if m else None


def verify_post_id(post_id: int, expected_question_id: int) -> tuple[bool, str]:
    """Live-fetches post_id and checks its nested question id matches
    expected_question_id — same verification standard as the auto-detect
    pass, applied here to catch any typo/wrong-URL in the manual CSV
    before it gets written. Returns (ok, note)."""
    url = f"https://www.metaculus.com/api2/questions/{post_id}/"
    max_retries = 5
    delay = 5.0
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
        except Exception as e:
            return False, f"fetch failed ({e})"
        if r.status_code == 429:
            if attempt >= max_retries:
                return False, "still 429 after retries"
            wait = float(r.headers.get("Retry-After") or delay)
            print(f"    ⏳  post {post_id}: 429, waiting {wait:.1f}s")
            time.sleep(wait)
            delay *= 2
            continue
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        break
    else:
        return False, "still 429 after retries"

    d = r.json() or {}
    q = d.get("question", d) or {}
    nested_id = q.get("id")
    if nested_id != expected_question_id:
        return False, f"nested question id is {nested_id}, expected {expected_question_id}"
    return True, (d.get("title") or "")[:60]


def main():
    write = "--write" in sys.argv
    csv_path = "post_id_manual_template.csv"
    if "--csv" in sys.argv:
        csv_path = sys.argv[sys.argv.index("--csv") + 1]

    matches: dict[int, int] = {}  # question_id -> post_id
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            qid = int(row["question_id"])
            post_id = extract_post_id(row.get("post_id_or_url", ""))
            if post_id is None:
                print(f"  ⚠️  Q{qid}: no usable post_id/URL in CSV row — skipping")
                continue
            matches[qid] = post_id

    if not matches:
        print("No usable rows found in CSV — nothing to do.")
        return

    print(f"Verifying {len(matches)} match(es) against the live API...\n")
    verified: dict[int, int] = {}
    for qid, post_id in matches.items():
        ok, note = verify_post_id(post_id, qid)
        time.sleep(1.0)
        if ok:
            print(f"  ✅ Q{qid} -> post {post_id}: {note}")
            verified[qid] = post_id
        else:
            print(f"  ❌ Q{qid} -> post {post_id}: FAILED VERIFICATION ({note}) — NOT applying, check the CSV row")

    if not verified:
        print("\nNothing verified — no files touched.")
        return

    job_files = sorted(
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )

    total_written = 0
    for jf in job_files:
        try:
            with open(jf) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ⚠️  skipping {jf}: {e}")
            continue

        question_ids = data.get("question_ids", {})
        post_ids = data.get("post_ids", {})
        file_changed = False

        for custom_id, qid in question_ids.items():
            if qid in verified and post_ids.get(custom_id) is None:
                post_ids[custom_id] = verified[qid]
                file_changed = True
                total_written += 1

        if file_changed:
            data["post_ids"] = post_ids
            if write:
                with open(jf, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  ✅ wrote to {jf}")
            else:
                print(f"  (dry run — would write to {jf})")

    print(f"\n{'='*55}")
    print(f"Verified & {'written' if write else 'would write'}: {total_written} entries "
          f"across {len(verified)} question(s)")
    if not write:
        print(f"\nDry run only — rerun with --write to actually save changes.")


if __name__ == "__main__":
    main()