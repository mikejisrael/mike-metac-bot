"""
check_project_type.py — one-off diagnostic, not part of the regular
pipeline. Nuclear Risk Horizons (id=1173) is type='question_series', not
type='tournament' — unlike the original 3 tournaments already working in
meta_batch_forecast.py. Tests whether the raw API's `tournaments=`
parameter (which is what forecasting_tools' ApiFilter.allowed_tournaments
actually sends — confirmed from the request URL in check_nuclear_gap.py's
output) only matches type='tournament' projects, vs a `project=`
parameter which might be the generic one covering all project types.

Bypasses forecasting_tools entirely — raw requests only — so there's no
ambiguity about which parameter is actually being tested.

Safe to delete after use.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
HEADERS = {"Authorization": f"Token {TOKEN}"}
NUCLEAR_ID = 1173


def main():
    if not TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set.")
        return

    print("=== A. Raw API with tournaments= (what forecasting_tools sends) ===")
    r = requests.get(
        "https://www.metaculus.com/api2/questions/",
        headers=HEADERS,
        params={"tournaments": NUCLEAR_ID, "statuses": "open", "limit": 20},
    )
    print(f"  HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  count={data.get('count')}, results returned={len(data.get('results', []))}")

    print("\n=== B. Raw API with project= instead ===")
    r = requests.get(
        "https://www.metaculus.com/api2/questions/",
        headers=HEADERS,
        params={"project": NUCLEAR_ID, "status": "open", "limit": 20},
    )
    print(f"  HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  count={data.get('count')}, results returned={len(data.get('results', []))}")
        for q in (data.get("results") or [])[:5]:
            print(f"    Q{q.get('id')}: {q.get('title', '')[:70]}")

    print("\n=== C. Newer posts endpoint (api/posts/) with tournaments=, matching the URL seen in check_nuclear_gap.py's error log ===")
    r = requests.get(
        "https://www.metaculus.com/api/posts/",
        headers=HEADERS,
        params={"tournaments": NUCLEAR_ID, "statuses": "open", "limit": 20},
    )
    print(f"  HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  count={data.get('count')}, results returned={len(data.get('results', []))}")


if __name__ == "__main__":
    main()