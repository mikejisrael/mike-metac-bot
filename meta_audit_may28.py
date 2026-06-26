"""
audit_may28.py — Targeted check of the May 28 batch only.

This batch is where we found 4 confirmed bad question_ids (38265, 38099,
38063, 36295). Rather than auditing all 348 forecasts, this checks just
this one batch's ~10-15 questions to see whether the corruption was
isolated to those 4 or spread wider within the same batch.

Uses the FIXED path-based endpoint (api2/questions/{id}/), not the
broken ?ids= filter.

Run this on YOUR machine (needs METACULUS_TOKEN from .env and network
access to metaculus.com — neither of which this sandbox has).
"""
import os
import json
import re
import time
import requests
from dotenv import load_dotenv

load_dotenv()
HEADERS = {"Authorization": f"Token {os.getenv('METACULUS_TOKEN')}"}
BATCH_FILE = os.path.join("Meta batches", "batch_jobs_20260528_1116.json")


def sig_words(s: str) -> set[str]:
    STOP = {
        "will", "the", "a", "an", "be", "is", "are", "to", "of", "in",
        "on", "by", "before", "after", "and", "or", "for", "at", "than",
        "with", "this", "that", "any", "exceed",
    }
    words = re.findall(r"[a-z0-9]+", s.lower())
    return {w for w in words if w not in STOP and len(w) > 2}


def titles_match(stored: str, fetched: str) -> bool:
    sw, fw = sig_words(stored or ""), sig_words(fetched or "")
    if not sw or not fw:
        return False
    overlap = sw & fw
    smaller = min(len(sw), len(fw))
    return smaller > 0 and (len(overlap) / smaller) >= 0.35


def main():
    if not os.path.exists(BATCH_FILE):
        print(f"Could not find {BATCH_FILE}")
        return

    with open(BATCH_FILE) as f:
        batch = json.load(f)

    question_ids = batch.get("question_ids", {})
    question_texts = batch.get("question_texts", {})

    print(f"Auditing {len(question_ids)} questions from batch_jobs_20260528_1116.json")
    print("=" * 70)

    ok, mismatched, gone = [], [], []

    for custom_id, qid in question_ids.items():
        stored_text = question_texts.get(custom_id, "")

        # Be polite to the API: brief pause between requests, and one
        # automatic retry (with a longer wait) if we get rate-limited.
        time.sleep(1.5)
        try:
            r = requests.get(
                f"https://www.metaculus.com/api2/questions/{qid}/",
                headers=HEADERS,
                timeout=20,
            )
            if r.status_code == 429:
                print(f"  ⏳ Q{qid}: rate limited, waiting 10s and retrying once...")
                time.sleep(10)
                r = requests.get(
                    f"https://www.metaculus.com/api2/questions/{qid}/",
                    headers=HEADERS,
                    timeout=20,
                )
        except Exception as e:
            print(f"  Q{qid}: request error — {e}")
            continue

        if r.status_code == 404:
            print(f"  ❌ Q{qid}: 404 (retired/removed)")
            print(f"     stored: {stored_text[:80]}")
            gone.append((qid, stored_text))
            continue

        if r.status_code != 200:
            print(f"  ⚠️  Q{qid}: unexpected status {r.status_code}")
            continue

        fetched = r.json()
        fetched_title = fetched.get("title") or (fetched.get("question") or {}).get("title") or ""

        if titles_match(stored_text, fetched_title):
            print(f"  ✅ Q{qid}: OK — {stored_text[:70]}")
            ok.append(qid)
        else:
            print(f"  🛑 Q{qid}: MISMATCH")
            print(f"     stored:  {stored_text[:80]}")
            print(f"     fetched: {fetched_title[:80]}")
            mismatched.append((qid, stored_text, fetched_title))

    print("=" * 70)
    print(f"OK: {len(ok)}  |  Mismatched: {len(mismatched)}  |  Gone (404): {len(gone)}")


if __name__ == "__main__":
    main()