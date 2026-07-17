"""
check_subq_windows.py — one-off: print each sub-question's own open_time
and scheduled_close_time for a group post, to check whether sub-questions
open/close staggered per-period or all together (all open now, staggered
close dates matching each period's end).

Usage:
    python check_subq_windows.py <post_id>

Example (VIX biweekly group):
    python check_subq_windows.py 44534
"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 2:
    raise SystemExit("Usage: python check_subq_windows.py <post_id>")

post_id = sys.argv[1]
TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
headers = {"Authorization": f"Token {TOKEN}"}

resp = requests.get(f"https://www.metaculus.com/api2/questions/{post_id}/", headers=headers, timeout=30)
resp.raise_for_status()
data = resp.json()

print(f"Post title: {data.get('title')}")
group = data.get("group_of_questions", {})
if not group:
    print("Not a group post — printing top-level question instead:")
    q = data.get("question", data)
    print(f"  id={q.get('id')}  open={q.get('open_time')}  close={q.get('scheduled_close_time')}")
else:
    print(f"\n{len(group.get('questions', []))} sub-questions:")
    for sq in sorted(group.get("questions", []), key=lambda x: x.get("id", 0)):
        label = sq.get("label") or sq.get("title") or ""
        print(f"  id={sq.get('id'):>6}  open={sq.get('open_time')}  "
              f"close={sq.get('scheduled_close_time')}  label={label}")