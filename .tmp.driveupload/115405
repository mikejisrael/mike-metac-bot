"""
meta_test_qid_as_postid.py — one-off diagnostic (2026-07-06).

Q6462's post_id turned out to be identical to its question_id — confirmed
via the actual Metaculus URL, not a guess. This tests whether that's a
pattern (plausibly: old, simple, non-group questions from before
question_id and post_id numbering diverged) or a one-off coincidence, by
trying question_id AS IF it were a post_id for every currently-known
no_post_id case, and checking whether the result's own nested
question.id actually matches.

Read-only. Does not backfill or modify anything — reports which
question_ids are safe to auto-backfill this way (same qid worked as
post_id) vs which genuinely need a manual URL lookup.

Reuses load_all_batches() and find_questions_to_refresh() directly from
meta_refresh_forecast.py rather than re-deriving/hardcoding the current
no_post_id list, so this always reflects the live, current state.
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

from meta_refresh_forecast import load_all_batches, find_questions_to_refresh

BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
REQUEST_DELAY_SECONDS = 1.0   # politeness delay between requests
MAX_RETRIES = 4               # retries specifically for 429s, with backoff


def _fetch_with_retry(url: str, headers: dict):
    """GETs url, retrying on 429 with exponential backoff (1s, 2s, 4s, 8s)
    rather than treating a rate limit as a real 'not found' — that's
    exactly the mistake the first version of this script made (16 of 22
    questions wrongly read as 'not found' when they were actually just
    rate-limited from firing requests with zero delay between them)."""
    delay = 1.0
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except Exception as e:
            return None, str(e)
        if r.status_code == 429:
            if attempt < MAX_RETRIES:
                print(f"    (429 — waiting {delay:.0f}s and retrying, "
                      f"attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(delay)
                delay *= 2
                continue
            return None, "429 after all retries exhausted"
        return r, None
    return None, "unreachable"


def main():
    if not BOT_TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set.")
        return

    all_forecasts = load_all_batches()
    _, _, _, no_post_id, _ = find_questions_to_refresh(all_forecasts)
    question_ids = sorted({f["question_id"] for f in no_post_id})
    print(f"Testing {len(question_ids)} no-post_id question(s) — trying question_id "
          f"AS post_id for each, {REQUEST_DELAY_SECONDS}s apart with 429 backoff...\n")

    headers = {"Authorization": f"Token {BOT_TOKEN}"}
    confirmed_matches = []
    mismatches = []
    not_found = []
    inconclusive = []

    for i, qid in enumerate(question_ids):
        if i > 0:
            time.sleep(REQUEST_DELAY_SECONDS)

        r, err = _fetch_with_retry(f"https://www.metaculus.com/api2/questions/{qid}/", headers)
        if r is None:
            print(f"  ⚠️  Q{qid}: {err} — genuinely inconclusive, not a confirmed 404")
            inconclusive.append(qid)
            continue

        if r.status_code == 404:
            print(f"  ❌ Q{qid}: 404 — question_id does not work as a post_id here")
            not_found.append(qid)
            continue
        if r.status_code != 200:
            print(f"  ⚠️  Q{qid}: HTTP {r.status_code} — inconclusive")
            inconclusive.append(qid)
            continue

        item = r.json()
        nested_qid = (item.get("question") or {}).get("id")
        if nested_qid == qid:
            print(f"  ✅ Q{qid}: confirmed — question_id also works as post_id")
            confirmed_matches.append(qid)
        else:
            print(f"  🛑 Q{qid}: post {qid} exists but its nested question.id is "
                  f"{nested_qid}, not {qid} — this post_id would silently point to "
                  f"the WRONG question. Needs manual lookup, do NOT auto-backfill.")
            mismatches.append(qid)

    print(f"\n{'=' * 60}")
    print(f"Of {len(question_ids)} no-post_id questions:")
    print(f"  ✅ {len(confirmed_matches)} confirmed safe to auto-backfill (question_id == post_id)")
    print(f"  🛑 {len(mismatches)} would silently mismatch — needs manual lookup, do NOT auto-backfill")
    print(f"  ❌ {len(not_found)} confirmed 404 — question_id doesn't exist as a post_id, needs manual lookup")
    print(f"  ⚠️  {len(inconclusive)} genuinely inconclusive (rate-limited even after retries) — re-run later")

    if confirmed_matches:
        print(f"\nSafe to auto-backfill (question_id == post_id):")
        print(f"  {confirmed_matches}")
    if mismatches:
        print(f"\nNeed manual lookup (question_id ≠ post_id, confirmed mismatch):")
        print(f"  {mismatches}")
    if not_found:
        print(f"\nNeed manual lookup (confirmed 404):")
        print(f"  {not_found}")
    if inconclusive:
        print(f"\nRe-run later (still rate-limited after retries):")
        print(f"  {inconclusive}")


if __name__ == "__main__":
    main()