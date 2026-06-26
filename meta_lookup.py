#!/usr/bin/env python3
"""
meta_lookup.py - resolve ANY Metaculus identifier to its canonical post/question.

THE DISCREPANCY (why this exists):
  - The website URL uses the POST id:      /questions/<post_id>/slug/
  - Your bot code's common language is the QUESTION id, nested inside the post.
  - These are DIFFERENT numbers.
  - /api/posts/<id>/ is keyed by POST id only. There is NO endpoint that takes a
    question id and returns its post. post -> question is free; question -> post
    must come from a stored mapping. This tool keeps that mapping in a local index.

FOOLPROOFNESS RANKING of what you can paste in:
  1. A full website URL          -> unambiguous (URL always carries the post id)
  2. A question id in the index  -> unambiguous (index records the id's type)
  3. A bare integer with no other context -> AMBIGUOUS. A question id can collide
     with some unrelated post id, so a bare number is resolved best-effort only.
     Prefer (1) or (2).

USAGE:
  python meta_lookup.py 38201
  python meta_lookup.py https://www.metaculus.com/questions/38201/some-slug/
  python meta_lookup.py --rebuild-index 33022     # backfill a tournament/project

The permanent fix is to call index_record(...) from your forecast code at forecast
time so the index is always complete (see note at bottom).
"""

import json
import os
import re
import sys
import requests

POST_API = "https://www.metaculus.com/api/posts/{}/"
LIST_API = "https://www.metaculus.com/api/posts/"
INDEX_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forecast_index.json")
HEADERS = {"User-Agent": "meta-lookup/1.0"}


# ---------- index persistence -------------------------------------------------

def _load_index():
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH) as f:
            return json.load(f)
    return {"by_question": {}, "by_post": {}}


def _save_index(idx):
    with open(INDEX_PATH, "w") as f:
        json.dump(idx, f, indent=2)


# ---------- post shape handling (simple / group / conditional) ----------------

def _questions_in_post(post):
    """Return [(question_id, title), ...] for any post shape."""
    out = []
    q = post.get("question")
    if q:
        out.append((q["id"], q.get("title") or post.get("title")))
    grp = post.get("group_of_questions")
    if grp:
        for sub in grp.get("questions", []):
            out.append((sub["id"], sub.get("title") or post.get("title")))
    cond = post.get("conditional")
    if cond:
        for key in ("question_yes", "question_no"):
            sub = cond.get(key)
            if sub:
                out.append((sub["id"], sub.get("title") or post.get("title")))
    return out


def fetch_post(post_id):
    r = requests.get(POST_API.format(post_id), headers=HEADERS, timeout=30)
    return r.json() if r.status_code == 200 else None


def index_post(post, idx):
    pid = post["id"]
    url = "https://www.metaculus.com/questions/{}/".format(pid)
    qids = []
    for qid, title in _questions_in_post(post):
        qids.append(qid)
        idx["by_question"][str(qid)] = {"post_id": pid, "title": title, "url": url}
    idx["by_post"][str(pid)] = {"post_id": pid, "title": post.get("title"),
                                "url": url, "question_ids": qids}
    return idx


# ---------- community prediction (best-effort) --------------------------------
# NOTE: the exact aggregation path has changed across API versions. Your batch
# code already extracts this reliably - reconcile this helper with yours.

def _community_binary(question):
    try:
        latest = question["aggregations"]["recency_weighted"]["latest"]
        for key in ("centers", "forecast_values", "means"):
            v = latest.get(key)
            if v:
                return float(v[-1]) if isinstance(v, list) else float(v)
    except (KeyError, TypeError, ValueError):
        pass
    return None


# ---------- resolution --------------------------------------------------------

def _describe(post, queried_id):
    pid = post["id"]
    rec = {
        "queried_id": queried_id,
        "post_id": pid,
        "title": post.get("title"),
        "status": post.get("status"),
        "scheduled_close_time": post.get("scheduled_close_time"),
        "url": "https://www.metaculus.com/questions/{}/".format(pid),
        "questions": [],
    }
    q = post.get("question")
    for qid, title in _questions_in_post(post):
        entry = {"question_id": qid, "title": title}
        if q and q.get("id") == qid:
            cp = _community_binary(q)
            if cp is not None:
                entry["community"] = round(cp, 4)
        rec["questions"].append(entry)
    return rec


def resolve(identifier):
    idx = _load_index()
    s = str(identifier).strip()

    # (1) URL -> post id
    m = re.search(r"/questions/(\d+)", s)
    if m:
        post = fetch_post(int(m.group(1)))
        if not post:
            return {"error": "URL post id {} returned no post.".format(m.group(1))}
        index_post(post, idx); _save_index(idx)
        return _describe(post, int(m.group(1)))

    # (2)/(3) bare integer
    if s.isdigit():
        n = int(s)
        post = fetch_post(n)          # try as POST id first
        if post:
            index_post(post, idx); _save_index(idx)
            result = _describe(post, n)
            result["_note"] = "Resolved as a POST id. If you meant a question id, verify the title."
            return result
        hit = idx["by_question"].get(str(n))   # fall back: QUESTION id via index
        if hit:
            post = fetch_post(hit["post_id"])
            if post:
                return _describe(post, n)
        return {"error": "{} is not a post id and is not in the local index as a "
                         "question id. Run: python meta_lookup.py --rebuild-index "
                         "<tournament_id> to map it.".format(n)}

    return {"error": "Unrecognized identifier: {}".format(s)}


def rebuild_index(project_id, limit=100):
    """Backfill the index for an entire tournament/project.

    NB: confirm the list filter param against your own batch code - your
    tournament_forecast.py already queries this project, so reuse its exact
    param name here (commonly 'tournaments' on the new API)."""
    idx = _load_index()
    offset = total = 0
    while True:
        r = requests.get(LIST_API, headers=HEADERS, timeout=30,
                         params={"tournaments": project_id, "limit": limit, "offset": offset})
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        for post in results:
            detail = fetch_post(post["id"]) or post
            index_post(detail, idx)
            total += 1
        offset += limit
        if offset >= data.get("count", 0):
            break
    _save_index(idx)
    print("Indexed {} posts -> {}".format(total, INDEX_PATH))


# ---------- forecast-time capture (the permanent fix) -------------------------

def index_record(post_id, question_id, title, url=None):
    """Call this from your forecast code the moment you forecast a question.
    After this, question_id -> post_id is instant and offline, forever."""
    idx = _load_index()
    url = url or "https://www.metaculus.com/questions/{}/".format(post_id)
    idx["by_question"][str(question_id)] = {"post_id": post_id, "title": title, "url": url}
    idx["by_post"].setdefault(str(post_id),
                              {"post_id": post_id, "title": title, "url": url, "question_ids": []})
    if question_id not in idx["by_post"][str(post_id)]["question_ids"]:
        idx["by_post"][str(post_id)]["question_ids"].append(question_id)
    _save_index(idx)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__); sys.exit(0)
    if args[0] == "--rebuild-index":
        rebuild_index(args[1]); sys.exit(0)
    print(json.dumps(resolve(args[0]), indent=2))