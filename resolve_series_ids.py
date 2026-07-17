"""
resolve_series_ids.py — one-off diagnostic, not part of the regular
pipeline. Given a handful of known question IDs (one per candidate
series), fetches each via the single-question detail endpoint (proven
reliable throughout this project, unlike the list/search endpoints — see
check_tournament_ids.py's results) and prints the tournament/question_series
ID it belongs to. Safe to delete after use.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
HEADERS = {"Authorization": f"Token {TOKEN}"}

# label -> a known question_id from that series
CANDIDATES = {
    "Nuclear Risk Horizons":    8636,
    "Current Events":           39711,
    "Taiwan Tinderbox":         21491,
    "Economic Indicators":      14034,
    "Animal Welfare":           15407,
}


def main():
    if not TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set — cannot resolve series IDs.")
        return

    for label, qid in CANDIDATES.items():
        r = requests.get(f"https://www.metaculus.com/api2/questions/{qid}/", headers=HEADERS)
        if r.status_code != 200:
            print(f"{label:22s} (Q{qid}) -> HTTP {r.status_code}")
            continue
        data = r.json()
        projects = data.get("projects", {}) or {}
        entries = (projects.get("tournament") or []) + (projects.get("question_series") or [])
        if not entries:
            print(f"{label:22s} (Q{qid}) -> no tournament/question_series project found")
            continue
        for e in entries:
            print(f"{label:22s} (Q{qid}) -> id={e.get('id')}, type={e.get('type')}, "
                  f"name={e.get('name')!r}, slug={e.get('slug')!r}")


if __name__ == "__main__":
    main()