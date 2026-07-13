"""
find_nonnumeric_group.py -- one-off scan: find a REAL binary or
multiple_choice group_of_questions post anywhere on Metaculus, to test
whether tournament_forecast_v2.py's group-unpacking (_unpack_group_post,
type-dispatch parsing, prompt building, CP extraction) actually works for
non-numeric sub-questions -- it's only ever been exercised against Market
Pulse, which is 100% numeric. Punch-list item #10.

Confirmed via Metaculus's own FAQ that binary question groups genuinely
exist ("A question group collecting multiple binary questions on a
limited set of outcomes or on mutually exclusive outcomes...") -- this
just needs to find a live one and dump its raw structure so we can see
exactly what shape it has, rather than assuming it matches the numeric
case.

Scans the general open-questions listing (no tournament filter) across
several pages, looking for any post with a group_of_questions field whose
sub-questions are binary or multiple_choice type. Stops at the first
match per type (one binary example, one multiple_choice example) or after
MAX_PAGES if none found.

Usage:
    python find_nonnumeric_group.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if not TOKEN:
    raise SystemExit("No METAC_TOURNAMENT_TOKEN or METACULUS_TOKEN found in .env")

headers = {"Authorization": f"Token {TOKEN}"}

MAX_PAGES = 15
LIMIT = 100

found = {"binary": None, "multiple_choice": None}

url = f"https://www.metaculus.com/api/posts/?limit={LIMIT}&statuses=open"
page = 0

while url and page < MAX_PAGES and not all(found.values()):
    page += 1
    print(f"...page {page}...")
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code} -- stopping")
        break
    data = resp.json()
    posts = data.get("results", [])

    for post in posts:
        group = post.get("group_of_questions")
        if not group:
            continue
        sub_qs = group.get("questions", [])
        types_present = {sq.get("type") for sq in sub_qs}
        for t in ("binary", "multiple_choice"):
            if t in types_present and found[t] is None:
                found[t] = post
                print(f"\n*** Found a {t} group_of_questions post ***")
                print(f"Post ID: {post.get('id')}  Title: {post.get('title')}")
                print(f"URL: https://www.metaculus.com/questions/{post.get('id')}/")
                print(f"Sub-question types in this group: {types_present}")

    url = data.get("next")

print(f"\n{'='*50}")
for t, post in found.items():
    if post:
        print(f"\n--- Full raw JSON for the {t} example (post {post.get('id')}) ---")
        print(json.dumps(post, indent=2, default=str)[:6000])
    else:
        print(f"\nNo {t} group_of_questions post found in {page} page(s) scanned.")