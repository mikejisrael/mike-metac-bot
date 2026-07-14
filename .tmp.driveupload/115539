"""
dump_group_subquestions.py -- focused follow-up to find_nonnumeric_group.py:
prints the FULL group_of_questions.questions array for one post, uncut
(the previous script's 6000-char JSON dump cut off before reaching the
actual sub-questions list).

Usage:
    python dump_group_subquestions.py <post_id>
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 2:
    raise SystemExit("Usage: python dump_group_subquestions.py <post_id>")

post_id = sys.argv[1]
TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
headers = {"Authorization": f"Token {TOKEN}"}

resp = requests.get(f"https://www.metaculus.com/api2/questions/{post_id}/", headers=headers, timeout=30)
resp.raise_for_status()
data = resp.json()

group = data.get("group_of_questions", {})
print(f"Post title: {data.get('title')}")
print(f"group_variable: {group.get('group_variable')}")
print(f"graph_type: {group.get('graph_type')}")
print(f"Group-level fine_print present: {bool(group.get('fine_print'))}")
print(f"Group-level resolution_criteria present: {bool(group.get('resolution_criteria'))}")
print(f"\n{len(group.get('questions', []))} sub-questions:\n")

for sq in group.get("questions", []):
    print(json.dumps(sq, indent=2, default=str))
    print("---")