"""
backfill_close_times.py — one-time (well, re-runnable) backfill for
historical batch_jobs_*.json files that predate meta_batch_forecast.py
saving "close_times" (added 2026-07-08, see Q43615 / Shakira post-mortem).

Without this, every batch file written before today has NO close_times
entry, so meta_refresh_forecast.py falls back to resolve_time for all of
them indefinitely — the exact bug this whole fix was meant to close,
just persisting on old data forever unless backfilled.

WHAT IT DOES:
For every batch_jobs_*.json / batch_jobs_refresh_*.json file in
"meta batches/":
  - if "close_times" key is missing entirely, adds it as {}
  - for every custom_id that has a post_id on file but no close_time yet,
    live-fetches scheduled_close_time from the Metaculus API and fills it
    in
  - entries with NO post_id on file (see meta_refresh_forecast.py's
    no_post_id bucket) are skipped and counted separately — nothing can
    be done for these without a post_id, same limitation as the earlier
    post_id backfill
  - writes the file back in place, same shape as before plus the filled
    close_times

Live fetches are deduped across files by post_id (a question can appear
in multiple batch files) and rate-limited (0.5s between calls) to be
polite to the API.

This is SAFE to re-run — already-backfilled entries are skipped, so
running it again only picks up whatever's still missing.

Usage:
  python backfill_close_times.py            # dry run — reports what WOULD be backfilled
  python backfill_close_times.py --write     # actually fetches and writes the files
"""

import glob
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

BATCH_DIR = "meta batches"
ACTIVE_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
HEADERS = {"Authorization": f"Token {ACTIVE_TOKEN}"} if ACTIVE_TOKEN else {}


def fetch_close_time(post_id: int) -> str | None:
    """Live-fetches scheduled_close_time for a post_id via the api2 detail
    endpoint — same endpoint/auth pattern already proven reliable
    elsewhere in this codebase (fetch_question_by_id, run_single).

    CHANGED (2026-07-08): added retry-with-backoff on 429s. The flat
    0.5s-between-calls delay was fine for the closing_soon-sized backfills
    this was designed around, but a full historical backfill runs through
    hundreds of post_ids in one sitting and started hitting real rate
    limits partway through (Mike, live run). Honors a Retry-After header
    if the API sends one; otherwise falls back to exponential backoff,
    same shape as the existing submission retry logic in
    meta_refresh_forecast.py."""
    url = f"https://www.metaculus.com/api2/questions/{post_id}/"
    max_retries = 5
    delay = 5.0
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
        except Exception as e:
            print(f"    ⚠️  Q(post {post_id}): fetch failed ({e})")
            return None
        if r.status_code == 200:
            return (r.json() or {}).get("scheduled_close_time")
        if r.status_code == 429:
            if attempt >= max_retries:
                print(f"    ⚠️  Q(post {post_id}): still 429 after {max_retries} retries — giving up")
                return None
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else delay
            print(f"    ⏳  Q(post {post_id}): 429, waiting {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
            delay *= 2
            continue
        print(f"    ⚠️  Q(post {post_id}): HTTP {r.status_code}")
        return None
    return None


def main():
    write = "--write" in sys.argv

    job_files = sorted(
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )
    if not job_files:
        print(f"No batch job files found in {BATCH_DIR!r} — check BATCH_DIR path.")
        return

    close_time_cache: dict[int, str | None] = {}
    total_backfilled = 0
    total_already_had = 0
    total_no_post_id = 0
    total_fetch_failed = 0

    for jf in job_files:
        try:
            with open(jf) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ⚠️  skipping {jf}: {e}")
            continue

        post_ids = data.get("post_ids", {})
        close_times = data.get("close_times", {})
        question_texts = data.get("question_texts", {})
        file_changed = False
        file_backfilled = 0

        for custom_id, post_id in post_ids.items():
            if close_times.get(custom_id):
                total_already_had += 1
                continue
            if post_id is None:
                total_no_post_id += 1
                continue

            if post_id not in close_time_cache:
                close_time_cache[post_id] = fetch_close_time(post_id)
                time.sleep(1.5)

            fetched = close_time_cache[post_id]
            if fetched is None:
                total_fetch_failed += 1
                continue

            close_times[custom_id] = fetched
            file_changed = True
            file_backfilled += 1
            total_backfilled += 1
            label = question_texts.get(custom_id, "")[:60]
            print(f"  {'[dry-run] ' if not write else ''}Q(post {post_id}): close_time -> {fetched} — {label}")

        if file_changed:
            data["close_times"] = close_times
            if write:
                with open(jf, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  ✅ wrote {file_backfilled} close_time(s) to {jf}")
            else:
                print(f"  (dry run — would write {file_backfilled} close_time(s) to {jf})")

    print(f"\n{'='*55}")
    print(f"Backfilled:        {total_backfilled}")
    print(f"Already had it:    {total_already_had}")
    print(f"No post_id (skip): {total_no_post_id}")
    print(f"Fetch failed:      {total_fetch_failed}")
    if not write:
        print(f"\nDry run only — rerun with --write to actually save changes.")


if __name__ == "__main__":
    main()