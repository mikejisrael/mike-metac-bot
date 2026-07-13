"""
dump_raw_my_forecasts.py -- raw, unprocessed dump of the my_forecasts
field for one specific sub-question. Built to check a real possibility:
check_market_pulse_participation.py treated "present in history" as
sufficient evidence of an ACTIVE forecast -- but if a forecast was ever
withdrawn, the history entry could still be there while "latest" goes
empty or shows a withdrawn state. That would explain everything: API
history has a record, but the live site correctly shows nothing under
"Me" because it genuinely isn't active. This prints the ENTIRE raw
my_forecasts object with no summarizing/interpretation, so we can see
exactly what's there rather than trust a boolean check.

Usage:
    python dump_raw_my_forecasts.py <post_id> <question_id>

Example (VIX Jul13-24):
    python dump_raw_my_forecasts.py 44534 44679
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 3:
    raise SystemExit("Usage: python dump_raw_my_forecasts.py <post_id> <question_id>")

post_id = sys.argv[1]
question_id = int(sys.argv[2])

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
headers = {"Authorization": f"Token {TOKEN}"}

resp = requests.get(f"https://www.metaculus.com/api2/questions/{post_id}/", headers=headers, timeout=30)
resp.raise_for_status()
data = resp.json()

group = data.get("group_of_questions", {})
sub_qs = {sq["id"]: sq for sq in group.get("questions", [])} if group else {}
q = sub_qs.get(question_id) if sub_qs else data.get("question", data)

if q is None:
    raise SystemExit(f"question_id {question_id} not found in post {post_id}")

print(f"Question: {q.get('title')}")
print(f"Status: {q.get('status')}")
print(f"Open time: {q.get('open_time')}")
print(f"Close time: {q.get('scheduled_close_time')}")
print(f"nr_forecasters on this sub-question (if present): {q.get('nr_forecasters', 'field not present at this level')}")

print(f"\n--- FULL RAW my_forecasts (no interpretation) ---")
print(json.dumps(q.get("my_forecasts"), indent=2, default=str))

# Also check the sub-question object itself for any withdrawal-related
# field we might not know the name of yet -- print every key so nothing
# is missed.
print(f"\n--- All keys on this sub-question object ---")
print(list(q.keys()))