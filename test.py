"""
diag_shakira_43615.py — one-off diagnostic, delete after use.

Checks two things about the "Dai Dai vs Waka Waka" question:
1. What resolve_time is actually stored locally for post_id 43612/43615,
   vs. the real close_time/scheduled_close_time on the live API. This
   confirms/refutes the "closing_soon bucket keys off the wrong field"
   theory.
2. Whether 43612 and 43615 are the same underlying question posted
   twice (like the Housing Act case, 2026-07-04) or genuinely unrelated.

Usage:
  python diag_shakira_43615.py
"""

import glob
import json
import os

BATCH_DIR = "batches"  # adjust if your batch_jobs files live elsewhere


def scan_local_batches():
    print("=== LOCAL BATCH FILES ===")
    job_files = (
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )
    for jf in sorted(job_files):
        try:
            with open(jf) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  (skip {jf}: {e})")
            continue

        post_ids = data.get("post_ids", {})
        resolve_times = data.get("resolve_times", {})
        q_texts = data.get("question_texts", {})
        q_ids = data.get("question_ids", {})

        for cid, pid in post_ids.items():
            if pid in (43612, 43615):
                print(f"\n  file: {jf}")
                print(f"  custom_id: {cid}")
                print(f"  post_id: {pid}")
                print(f"  question_id: {q_ids.get(cid)}")
                print(f"  resolve_time (stored): {resolve_times.get(cid)}")
                print(f"  text: {q_texts.get(cid, '')[:80]}")


def check_live(post_id: int):
    import requests
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
    headers = {"Authorization": f"Token {token}"} if token else {}
    url = f"https://www.metaculus.com/api2/questions/{post_id}/"
    r = requests.get(url, headers=headers, timeout=15)
    print(f"\n=== LIVE API: post_id {post_id} (status {r.status_code}) ===")
    if r.status_code != 200:
        print(f"  {r.text[:300]}")
        return
    d = r.json()
    print(f"  id: {d.get('id')}")
    print(f"  title: {d.get('title', '')[:80]}")
    # field names vary by API version — print anything close-related
    for key in d:
        if "close" in key.lower() or "resolve" in key.lower():
            print(f"  {key}: {d[key]}")


if __name__ == "__main__":
    scan_local_batches()
    check_live(43612)
    check_live(43615)