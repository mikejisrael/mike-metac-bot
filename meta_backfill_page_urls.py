"""
meta_backfill_page_urls.py — backfills historical Metaculus batch history
with data that was missing before recent fixes: page_url on results (so
show_reasoning links resolve instead of 404-ing), and — added 2026-07-03 —
post_id on jobs files' post_ids dict.

IMPORTANT FINDING: the api2 `?ids=<question_id>` filter is IGNORED by the
endpoint — it returns the newest feed question regardless of the id passed.
(fetch_question_by_id in meta_refresh_forecast.py uses that same broken query.)

So this script does NOT filter. It pages through the posts feed, where each
post has a top-level `id` (the POST id, used in the URL) and a nested
`question` whose `id` is the QUESTION id you store. From that it builds a
question_id -> post_id map and writes it back into TWO places (a .bak copy
is made of each file before writing):
  - batch_results*.json: item["page_url"] — so show_reasoning links resolve
  - batch_jobs*.json:    batch_info["post_ids"][custom_id] — CHANGED
    2026-07-03: this is the field meta_refresh_forecast.py's --submit/--check
    path actually reads (via load_all_batches -> fetch_question_by_id) to
    decide whether a stale/closing-soon question can be re-fetched at all.
    Before this fix, this script only ever wrote page_url into results
    files — a different file, a different field, keyed differently
    (question_id per-item here vs custom_id-keyed dict in jobs files) — so
    it never actually fed meta_refresh_forecast.py anything. Caught live:
    a --submit run against local history predating the 2026-06-29 post_id
    fix skipped 17 of 19 questions with "no post_id on file", including 7
    of 9 closing-soon questions that will lock with no refreshed forecast
    ever landing on them.

CHANGED (2026-07-03): BATCH_DIR fixed from "Meta batches" (capital M) to
"meta batches", matching the lowercase convention established in
meta_batch_forecast.py and meta_refresh_forecast.py (2026-06-30, for
Linux/GitHub Actions case-sensitivity — NTFS on Windows hid this because
it's case-insensitive, so this script "worked" locally by accident while
silently pointing at a folder name that doesn't match what those two
scripts actually write to).

KNOWN LIMITATION (pre-existing, not introduced by the 2026-07-03 change):
the feed is paged newest-first with a MAX_PAGES safety cap (6000 posts by
default). Very old low-numbered question ids may sit deeper in the feed
than that cap reaches and end up unmapped — left as-is, not guessed at,
same as everywhere else in this codebase that touches post_id/question_id.

Imports only `requests`, so startup is instant.

Usage:
  python meta_backfill_page_urls.py            # process all batch_results*.json and batch_jobs*.json
  python meta_backfill_page_urls.py --dry-run  # build map, show changes, write nothing
"""

import glob
import json
import os
import shutil
import sys
import time

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BATCH_DIR = "meta batches"
LIST_URL = "https://www.metaculus.com/api2/questions/"
PAGE = 100
MAX_PAGES = 60          # safety cap (60 * 100 = 6000 posts)
TIMEOUT = 20


def _headers():
    token = os.getenv("METACULUS_TOKEN")
    return {"Authorization": f"Token {token}"} if token else {}


def _question_ids_in_post(post):
    """Yield every question id contained in a post (simple/group/conditional)."""
    q = post.get("question")
    if isinstance(q, dict) and isinstance(q.get("id"), int):
        yield q["id"]
    grp = post.get("group_of_questions") or {}
    for sub in grp.get("questions", []) if isinstance(grp, dict) else []:
        if isinstance(sub.get("id"), int):
            yield sub["id"]


def collect_target_ids():
    """Returns (results_files, jobs_files, all_target_question_ids).
    results_files: batch_results*.json entries missing page_url.
    jobs_files: batch_jobs*.json entries missing a post_id for their
    custom_id — CHANGED 2026-07-03, this is the half that actually matters
    for meta_refresh_forecast.py (see module docstring).
    all_target_question_ids is the union of both, so a single feed crawl
    (the expensive part) serves both backfills."""
    results_files = sorted(glob.glob(os.path.join(BATCH_DIR, "batch_results*.json")))
    jobs_files = sorted(
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )

    targets = set()
    for path in results_files:
        with open(path) as f:
            data = json.load(f)
        for item in data.values():
            q_id = item.get("question_id")
            if q_id and not item.get("page_url"):
                targets.add(q_id)

    for path in jobs_files:
        with open(path) as f:
            batch_info = json.load(f)
        question_ids = batch_info.get("question_ids", {})
        post_ids = batch_info.get("post_ids", {})
        for custom_id, q_id in question_ids.items():
            if q_id and not post_ids.get(custom_id):
                targets.add(q_id)

    return results_files, jobs_files, targets


def _get_page(offset, attempts=4):
    """Fetch one feed page with retry/backoff. Returns results list, or None on
    persistent failure (so the caller can skip rather than abort)."""
    for a in range(attempts):
        try:
            r = requests.get(LIST_URL, headers=_headers(),
                             params={"limit": PAGE, "offset": offset}, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json().get("results", [])
            print(f"      HTTP {r.status_code} (try {a + 1}/{attempts})", flush=True)
        except Exception as e:
            print(f"      {type(e).__name__} (try {a + 1}/{attempts})", flush=True)
        time.sleep(2 * (a + 1))
    return None


def build_qid_to_post_map(targets):
    """Page the feed (newest first) building {question_id: post_id} until all
    target ids are found or the page cap is hit."""
    found = {}
    shown = 0
    for page in range(MAX_PAGES):
        offset = page * PAGE
        results = _get_page(offset)
        if results is None:
            print(f"  page {page}: failed after retries, skipping", flush=True)
            continue
        if not results:
            break
        for post in results:
            post_id = post.get("id")
            for q_id in _question_ids_in_post(post):
                if q_id not in found:
                    found[q_id] = post_id
                    if shown < 3:   # transparency: confirm the mapping shape
                        print(f"  sample: question {q_id} -> post {post_id}  "
                              f"({(post.get('title') or '')[:45]})", flush=True)
                        shown += 1
        got = len(targets & set(found))
        print(f"  page {page}: scanned, {got}/{len(targets)} targets mapped", flush=True)
        if targets <= set(found):
            break
        time.sleep(0.3)
    return found


def backfill(dry_run=False):
    results_files, jobs_files, targets = collect_target_ids()
    if not results_files and not jobs_files:
        print(f"No batch_results*.json or batch_jobs*.json found in {BATCH_DIR}/")
        return
    print(f"{len(results_files)} result file(s), {len(jobs_files)} job file(s); "
          f"{len(targets)} unique question(s) need backfilling.\n", flush=True)
    if not targets:
        return

    print("Building question_id -> post_id map from the feed...", flush=True)
    qmap = build_qid_to_post_map(targets)
    missing = targets - set(qmap)
    print(f"\nMapped {len(targets) - len(missing)}/{len(targets)} ids."
          + (f" Unmapped (left as-is): {sorted(missing)[:10]}" if missing else ""), flush=True)

    total_page_urls_added = 0
    for path in results_files:
        with open(path) as f:
            data = json.load(f)
        changed = False
        for item in data.values():
            q_id = item.get("question_id")
            if not q_id or item.get("page_url") or q_id not in qmap:
                continue
            item["page_url"] = f"https://www.metaculus.com/questions/{qmap[q_id]}/"
            changed = True
            total_page_urls_added += 1
        if changed and not dry_run:
            shutil.copy2(path, path + ".bak")
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"  updated {path}  (backup: {path}.bak)", flush=True)

    # CHANGED 2026-07-03: the half that actually unblocks
    # meta_refresh_forecast.py --submit/--check re-fetching old history —
    # see module docstring.
    total_post_ids_added = 0
    for path in jobs_files:
        with open(path) as f:
            batch_info = json.load(f)
        question_ids = batch_info.get("question_ids", {})
        post_ids = batch_info.setdefault("post_ids", {})
        changed = False
        for custom_id, q_id in question_ids.items():
            if not q_id or post_ids.get(custom_id) or q_id not in qmap:
                continue
            post_ids[custom_id] = qmap[q_id]
            changed = True
            total_post_ids_added += 1
        if changed and not dry_run:
            shutil.copy2(path, path + ".bak")
            with open(path, "w") as f:
                json.dump(batch_info, f, indent=2)
            print(f"  updated {path}  (backup: {path}.bak)", flush=True)

    verb = "Would add" if dry_run else "Added"
    print(f"\n{verb} page_url to {total_page_urls_added} result(s).", flush=True)
    print(f"{verb} post_id to {total_post_ids_added} job entrie(s) — "
          f"this is the part that unblocks re-fetching old history in "
          f"meta_refresh_forecast.py.", flush=True)


if __name__ == "__main__":
    backfill(dry_run="--dry-run" in sys.argv)