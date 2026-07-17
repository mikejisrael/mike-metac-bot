"""
check_my_prediction.py — definitive, API-level check for whether
mike_iz_-bot has an active prediction on a given question. Screenshots of
the web UI (fan chart tooltips, coverage tables) are suggestive but
ambiguous — this hits the same authenticated endpoint the site itself
uses and prints exactly what Metaculus has on file, no UI interpretation
required.

IMPORTANT: /api2/questions/{id}/ is keyed by POST ID, not the individual
sub-question's own question_id (confirmed live 2026-07-11 — question_id
44679 alone 404'd; post_id 44534 for the same sub-question worked). For a
group_of_questions post, ALL sub-questions share one post_id (e.g. the
whole VIX biweekly group is post_id 44534) — pass BOTH so this script can
fetch the group and then pick out the specific sub-question you care
about.

Usage:
    python check_my_prediction.py <post_id> [question_id]

    If question_id is omitted, prints info for the post's own top-level
    question (works for non-group posts). For a group post, question_id
    is required to pick the right sub-question out of the group.

Example (VIX Jul13-24 sub-question from the 2026-07-11 screenshots):
    python check_my_prediction.py 44534 44679
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 2:
    raise SystemExit("Usage: python check_my_prediction.py <post_id> [question_id]")

post_id = sys.argv[1]
target_question_id = int(sys.argv[2]) if len(sys.argv) > 2 else None

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if not TOKEN:
    raise SystemExit("No METAC_TOURNAMENT_TOKEN or METACULUS_TOKEN found in .env")

headers = {"Authorization": f"Token {TOKEN}"}

resp = requests.get(
    f"https://www.metaculus.com/api2/questions/{post_id}/",
    headers=headers,
    timeout=30,
)
print(f"HTTP {resp.status_code}")
resp.raise_for_status()
data = resp.json()

print(f"\nPost title: {data.get('title')}")

# Figure out which question dict to inspect: top-level "question" for a
# normal post, or the matching entry inside group_of_questions for a group.
group = data.get("group_of_questions")
if group and target_question_id is not None:
    sub_qs = group.get("questions", [])
    q = next((sq for sq in sub_qs if sq.get("id") == target_question_id), None)
    if q is None:
        raise SystemExit(f"question_id {target_question_id} not found among this group's "
                          f"sub-questions: {[sq.get('id') for sq in sub_qs]}")
    print(f"Sub-question ({target_question_id}) matched within group.")
elif group and target_question_id is None:
    raise SystemExit("This post is a group_of_questions — pass the specific question_id "
                      f"as a second argument. Sub-question IDs in this group: "
                      f"{[sq.get('id') for sq in group.get('questions', [])]}")
else:
    q = data.get("question", data)

print(f"Question ID: {q.get('id')}  Type: {q.get('type')}")
print(f"Status: {q.get('status', data.get('status'))}")

# Metaculus's own field names for "your" forecast vary by API version —
# checking every plausible key rather than assuming one, so this doesn't
# silently print "nothing found" just because we guessed the wrong field
# name (same failure mode we hit earlier this week with tournaments= vs
# project=).
candidate_keys = [
    "my_forecasts", "my_forecast", "user_forecast", "user_predictions",
    "active_forecast", "current_user_forecast",
]

found_any = False
for key in candidate_keys:
    if key in q and q[key]:
        found_any = True
        print(f"\n--- Found under question['{key}'] ---")
        print(json.dumps(q[key], indent=2)[:2000])
    if key in data and data[key]:
        found_any = True
        print(f"\n--- Found under top-level['{key}'] ---")
        print(json.dumps(data[key], indent=2)[:2000])

if not found_any:
    print(f"\nNone of the expected keys ({candidate_keys}) were found or were empty.")
    print("Dumping all keys on the matched question object so we can spot the right one:")
    print(list(q.keys()))
    print("\nDumping all top-level keys on the post object:")
    print(list(data.keys()))