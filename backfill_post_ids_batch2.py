"""
backfill_post_ids_batch2.py — second pass at backfilling missing post_ids,
for the 9 questions currently in meta_refresh_forecast.py's no_post_id
bucket (pre-dates the 2026-06-29 post_id fix in meta_batch_forecast.py).

Same two-step approach as the original post_id backfill (2026-07-0x,
22 total: 8 auto-backfilled, 14 manual via spreadsheet, 1 excluded):

STEP 1 — auto-detect: for some single-question posts, the post_id and the
nested question_id happen to be the same number (Metaculus's two ID
sequences occasionally coincide). Tries fetching
/api2/questions/{question_id}/ AS IF question_id were the post_id; if
that succeeds AND the returned page's own nested question id matches
AND the title matches what's on file, it's confirmed the same question
— auto-backfill post_id = question_id, no human needed.

STEP 2 — whatever's left needs manual matching (title alone isn't
reliable enough to guess a URL). Writes a CSV template
(post_id_manual_template.csv) with question_id + question_text + a blank
post_id_or_url column for Mike to fill in — same shape as last time.

Usage:
  python backfill_post_ids_batch2.py            # dry run
  python backfill_post_ids_batch2.py --write     # writes auto-detected ones to batch files
"""

import csv
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


def _normalize(s: str) -> str:
    return " ".join((s or "").lower().split())


def try_auto_detect(question_id: int, expected_text: str) -> int | None:
    """Returns question_id back (confirming post_id == question_id) if the
    live fetch confirms it's the same question, else None. Retries on 429
    same as backfill_close_times.py."""
    url = f"https://www.metaculus.com/api2/questions/{question_id}/"
    max_retries = 5
    delay = 5.0
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
        except Exception as e:
            print(f"    ⚠️  Q{question_id}: fetch failed ({e})")
            return None
        if r.status_code == 429:
            if attempt >= max_retries:
                print(f"    ⚠️  Q{question_id}: still 429 after {max_retries} retries — giving up")
                return None
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else delay
            print(f"    ⏳  Q{question_id}: 429, waiting {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
            delay *= 2
            continue
        if r.status_code != 200:
            return None  # question_id isn't a valid post_id — needs manual matching
        break
    else:
        return None

    d = r.json() or {}
    q = d.get("question", d) or {}
    nested_id = q.get("id") or d.get("id")
    live_title = d.get("title") or q.get("title") or ""

    if nested_id != question_id:
        return None  # this post_id belongs to a different underlying question

    # Title check as a second confirmation, not just ID coincidence —
    # tolerant of minor whitespace/punctuation differences.
    if expected_text and _normalize(live_title)[:40] != _normalize(expected_text)[:40]:
        print(f"    ⚠️  Q{question_id}: ID matched but titles differ — "
              f"local={expected_text[:50]!r} live={live_title[:50]!r} — treating as NOT confirmed")
        return None

    return question_id


def main():
    write = "--write" in sys.argv

    job_files = sorted(
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )
    if not job_files:
        print(f"No batch job files found in {BATCH_DIR!r} — check BATCH_DIR path.")
        return

    detect_cache: dict[int, int | None] = {}
    auto_fixed = []      # (question_id, text)
    needs_manual = {}    # question_id -> text (dedup across files)
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
        question_texts = data.get("question_texts", {})
        file_changed = False

        for custom_id, qid in question_ids.items():
            if post_ids.get(custom_id) is not None:
                continue  # already has a post_id, not part of this problem
            if qid is None:
                continue

            text = question_texts.get(custom_id, "")

            if qid not in detect_cache:
                detect_cache[qid] = try_auto_detect(qid, text)
                time.sleep(1.5)

            resolved_post_id = detect_cache[qid]
            if resolved_post_id is not None:
                post_ids[custom_id] = resolved_post_id
                file_changed = True
                auto_fixed.append((qid, text))
                print(f"  {'[dry-run] ' if not write else ''}Q{qid}: auto-confirmed post_id == question_id — {text[:60]}")
            else:
                needs_manual[qid] = text

        if file_changed:
            data["post_ids"] = post_ids
            if write:
                with open(jf, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  ✅ wrote post_id(s) to {jf}")
            else:
                print(f"  (dry run — would write to {jf})")

    print(f"\n{'='*55}")
    print(f"Auto-confirmed: {len(set(q for q, _ in auto_fixed))}")
    print(f"Needs manual:   {len(needs_manual)}")

    if needs_manual:
        template_path = "post_id_manual_template.csv"
        with open(template_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["question_id", "question_text", "post_id_or_url"])
            for qid, text in sorted(needs_manual.items()):
                writer.writerow([qid, text, ""])
        print(f"\nWrote {template_path} — fill in post_id_or_url for each row "
              f"(paste the question's URL, e.g. https://www.metaculus.com/questions/12345/..., "
              f"or just the numeric post_id) and send it back.")

    if not write and auto_fixed:
        print(f"\nDry run only — rerun with --write to actually save the auto-confirmed ones.")


if __name__ == "__main__":
    main()