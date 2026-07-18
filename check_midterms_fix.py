"""
check_midterms_fix.py — quick manual check for whether Metaculus has fixed
the project=/tournaments= filter bug reported 2026-07-18 (list endpoints
returning only 3 of ~24 open questions for US Midterms 2026, project 32840).

Run anytime: python check_midterms_fix.py

Logic: fetch the project=32840 list, and separately confirm three known-open
question ids (40598, 44423, 43840 — all individually verified open and
correctly tagged with project 32840 on 2026-07-18) via direct lookup. If
those three now show up in the list fetch too, the bug is fixed. If the
list fetch's count is still stuck at 3 and doesn't include them, it isn't.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = 32840
KNOWN_MISSING_IDS = [40598, 44423, 43840]  # confirmed open + correctly tagged, 2026-07-18


def main():
    token = os.getenv("METAC_TOURNAMENT_TOKEN")
    if not token:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set — cannot run this check.")
        return
    headers = {"Authorization": "Token " + token}

    r = requests.get(
        "https://www.metaculus.com/api2/questions/",
        headers=headers,
        params={"project": PROJECT_ID, "limit": 100},
    )
    if r.status_code != 200:
        print(f"⚠️  Unexpected HTTP {r.status_code} — can't tell, check manually.")
        print(r.text[:300])
        return

    data = r.json()
    count = data.get("count")
    ids_returned = {q.get("id") for q in data.get("results", [])}

    print(f"project={PROJECT_ID} list fetch -> count={count}, {len(ids_returned)} in this page")

    found = [qid for qid in KNOWN_MISSING_IDS if qid in ids_returned]
    still_missing = [qid for qid in KNOWN_MISSING_IDS if qid not in ids_returned]

    print(f"  known-open test ids found in list:  {found}")
    print(f"  known-open test ids still missing:  {still_missing}")
    print()

    if not still_missing and (count is None or count >= len(KNOWN_MISSING_IDS) + 3):
        print("✅ Looks FIXED — previously-missing questions now show up, count is no longer stuck at 3.")
        print("   Worth re-running the full ApiFilter check (test.py from 2026-07-18) before trusting")
        print("   meta_batch_forecast.py's ALLOWED_TOURNAMENTS fetch on this tournament again.")
    else:
        print("❌ Still broken — same symptom as 2026-07-18 (list fetch missing real open questions).")


if __name__ == "__main__":
    main()