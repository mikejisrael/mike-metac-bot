"""
check_market_pulse_participation.py -- diagnostic for the "You have not
participated in this tournament yet" widget appearing on Market Pulse
26Q3's page despite our local records showing 48 sub-questions forecast.

Before assuming predictions were actually lost, this checks the GROUND
TRUTH directly: for every Market Pulse (post_id, question_id) pair found
in our local batch_results history, query the live API and report whether
a forecast from mike_iz_-bot (author_id 303026) is actually still present
on that specific sub-question.

This distinguishes two very different situations:
  - If most/all sub-questions genuinely show NO live forecast: something
    real happened server-side (a Metaculus-side reset, a bug, an account
    issue) and we need to resubmit.
  - If most/all sub-questions STILL show a live forecast: the tournament
    page's "My Participation" widget is stale/buggy, and no resubmission
    is needed at all.

Also reports the breakdown if it's PARTIAL (some missing, some present) --
that tells us exactly which specific sub-questions would need resubmission
rather than assuming it's all-or-nothing.

Usage:
    python check_market_pulse_participation.py
"""

import os
import json
import glob
import time
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if not TOKEN:
    raise SystemExit("No METAC_TOURNAMENT_TOKEN or METACULUS_TOKEN found in .env")

headers = {"Authorization": f"Token {TOKEN}"}
BOT_AUTHOR_ID = 303026  # mike_iz_-bot, confirmed earlier this project

# Collect every (post_id, question_id, question_text) triple we believe we
# forecast for Market Pulse, from local history. Scans BOTH directories in
# case anything wasn't merged, though tournament_batches should have
# everything after the Stage 2 merge.
BATCH_DIRS = ["tournament_batches", "tournament_batches_v2"]

records = {}  # question_id -> {post_id, question_text}
for batch_dir in BATCH_DIRS:
    for path in glob.glob(os.path.join(batch_dir, "batch_results_*.json")):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            continue
        for r in data.values():
            qid = r.get("question_id")
            post_id = r.get("post_id")
            text = r.get("question_text", "")
            if qid and post_id and "biweekly" in text.lower():  # Market Pulse's own phrasing
                records[qid] = {"post_id": post_id, "question_text": text}

print(f"Found {len(records)} Market Pulse sub-question(s) in local history\n")

if not records:
    raise SystemExit("No local Market Pulse records found -- check you're running this "
                      "from the same folder as tournament_forecast_v2.py.")

# Group by post_id so we only fetch each group post once, not once per member
by_post_id = {}
for qid, info in records.items():
    by_post_id.setdefault(info["post_id"], []).append(qid)

present = []
missing = []
errors = []

for post_id, qids in sorted(by_post_id.items()):
    try:
        resp = requests.get(
            f"https://www.metaculus.com/api2/questions/{post_id}/",
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            for qid in qids:
                errors.append((qid, post_id, f"HTTP {resp.status_code}"))
            continue
        data = resp.json()
    except Exception as e:
        for qid in qids:
            errors.append((qid, post_id, str(e)))
        continue

    group = data.get("group_of_questions", {})
    sub_qs = {sq["id"]: sq for sq in group.get("questions", [])} if group else {}

    for qid in qids:
        sq = sub_qs.get(qid) if sub_qs else data.get("question", data)
        if sq is None:
            missing.append((qid, post_id, "sub-question not found in current group data"))
            continue

        my_forecasts = sq.get("my_forecasts") or {}
        latest = my_forecasts.get("latest")
        history = my_forecasts.get("history") or []

        has_live_forecast = (
            (latest and latest.get("author_id") == BOT_AUTHOR_ID)
            or any(h.get("author_id") == BOT_AUTHOR_ID for h in history)
        )
        if has_live_forecast:
            present.append((qid, post_id))
        else:
            missing.append((qid, post_id, "no forecast from mike_iz_-bot found"))

    time.sleep(0.3)  # light rate-limit courtesy

print(f"{'='*50}")
print(f"Present (still has a live forecast): {len(present)}")
print(f"Missing (no live forecast found):    {len(missing)}")
print(f"Errors during check:                 {len(errors)}\n")

if missing:
    print("--- MISSING sub-questions ---")
    for qid, post_id, reason in missing:
        text = records.get(qid, {}).get("question_text", "")[:70]
        print(f"  Q{qid} (post {post_id}): {reason}")
        print(f"    {text}")

if errors:
    print("\n--- ERRORS ---")
    for qid, post_id, reason in errors:
        print(f"  Q{qid} (post {post_id}): {reason}")

print(f"\n{'='*50}")
if not missing and not errors:
    print("Every sub-question we believe we forecast still has a live forecast on file.")
    print("This strongly suggests the tournament page's 'My Participation' widget is "
          "stale or buggy -- NOT that predictions were actually lost.")
elif len(missing) == len(records):
    print("ALL sub-questions show no live forecast -- this looks like a real, "
          "tournament-wide loss of predictions, not a UI glitch.")
else:
    print(f"PARTIAL: {len(missing)}/{len(records)} sub-questions are missing a live forecast. "
          f"See the list above for exactly which ones would need resubmission.")