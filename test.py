"""
test3.py — ground truth check. Both project=33066 and
allowed_tournaments=[33066] returned 0 questions, via the raw endpoint AND
the forecasting_tools library — so 33066 likely isn't the right ID (or
isn't populated the way we expect). Rather than keep guessing IDs, fetch
one KNOWN question directly (grab its numeric ID from the URL of an
actual open question on the Market Pulse 26Q3 page in your browser) and
print its full `projects` field — that'll show the real project id/slug/
type for Market Pulse directly from the source of truth.

Usage:
    python test3.py <question_id>

Example: if the question URL is metaculus.com/questions/44601/some-slug/
that means:
    python test3.py 44601
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 2:
    raise SystemExit("Usage: python test3.py <question_id>\n"
                      "Grab the numeric ID from the URL of an open question "
                      "on the Market Pulse 26Q3 tournament page.")

question_id = sys.argv[1]

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if not TOKEN:
    raise SystemExit("No METAC_TOURNAMENT_TOKEN or METACULUS_TOKEN found in .env")

headers = {"Authorization": f"Token {TOKEN}"}

resp = requests.get(
    f"https://www.metaculus.com/api2/questions/{question_id}/",
    headers=headers,
    timeout=30,
)
print(f"HTTP {resp.status_code}")
resp.raise_for_status()
data = resp.json()

print(f"\nTitle: {data.get('title')}")
print(f"Question type: {(data.get('question') or data).get('type')}")
print(f"Is this a group-of-questions container? "
      f"{'group_of_questions' in data or data.get('group_of_questions') is not None}")

print(f"\n--- Full 'projects' field ---")
print(json.dumps(data.get("projects", {}), indent=2))