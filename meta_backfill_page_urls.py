"""
meta_backfill_page_urls.py — add the correct Metaculus page_url to historical
batch_results files so show_reasoning links resolve instead of 404-ing.

IMPORTANT FINDING: the api2 `?ids=<question_id>` filter is IGNORED by the
endpoint — it returns the newest feed question regardless of the id passed.
(fetch_question_by_id in refresh_forecasts.py uses that same broken query.)

So this script does NOT filter. It pages through the posts feed, where each
post has a top-level `id` (the POST id, used in the URL) and a nested
`question` whose `id` is the QUESTION id you store. From that it builds a
question_id -> post_id map and writes the correct page_url back into the
result files (a .bak copy is made first).

Imports only `requests`, so startup is instant.

Usage:
  python meta_backfill_page_urls.py            # process all batch_results*.json
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

BATCH_DIR = "Meta batches"
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
    files = sorted(glob.glob(os.path.join(BATCH_DIR, "batch_results*.json")))
    targets = set()
    for path in files:
        with open(path) as f:
            data = json.load(f)
        for item in data.values():
            q_id = item.get("question_id")
            if q_id and not item.get("page_url"):
                targets.add(q_id)
    return files, targets


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
    files, targets = collect_target_ids()
    if not files:
        print(f"No batch_results*.json found in {BATCH_DIR}/")
        return
    print(f"{len(files)} result file(s); {len(targets)} unique question(s) need a page_url.\n", flush=True)
    if not targets:
        return

    print("Building question_id -> post_id map from the feed...", flush=True)
    qmap = build_qid_to_post_map(targets)
    missing = targets - set(qmap)
    print(f"\nMapped {len(targets) - len(missing)}/{len(targets)} ids."
          + (f" Unmapped (left as-is): {sorted(missing)[:10]}" if missing else ""), flush=True)

    total_added = 0
    for path in files:
        with open(path) as f:
            data = json.load(f)
        changed = False
        for item in data.values():
            q_id = item.get("question_id")
            if not q_id or item.get("page_url") or q_id not in qmap:
                continue
            item["page_url"] = f"https://www.metaculus.com/questions/{qmap[q_id]}/"
            changed = True
            total_added += 1
        if changed and not dry_run:
            shutil.copy2(path, path + ".bak")
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"  updated {path}  (backup: {path}.bak)", flush=True)

    verb = "Would add" if dry_run else "Added"
    print(f"\n{verb} page_url to {total_added} result(s).", flush=True)


if __name__ == "__main__":
    backfill(dry_run="--dry-run" in sys.argv)