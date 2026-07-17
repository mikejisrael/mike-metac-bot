"""
check_tournament_ids.py — one-off diagnostic, not part of the regular
pipeline. Resolves a handful of Metaculus "Question Series" slugs (found
via https://www.metaculus.com/tournaments/question-series/) to their
numeric tournament IDs, and prints scoring type + question count so we can
confirm each one is a normally-scored tournament before adding it to
meta_batch_forecast.py's TOURNAMENTS list.

Safe to delete after use.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
HEADERS = {"Authorization": f"Token {TOKEN}"}

# There's no slug->ID lookup endpoint in Metaculus's API, and the
# questions list's `project` filter only accepts a numeric ID (confirmed:
# passing a slug string returns HTTP 400). Workaround: search for a real
# question likely to belong to each series by title keywords, then read
# the tournament/question_series ID straight off THAT question's own
# `projects` field — same structure already relied on elsewhere in this
# codebase (see the H.R.6644 example, and meta_watch.py's
# _extract_tournament_label()). Prints everything found rather than
# guessing which match is correct — eyeball the output and pick.
SEARCHES = {
    "Nuclear Risk Horizons": "nuclear",
    "Iran-Israel Conflict": "Iran Israel",
    "Quantum Computing": "quantum computing",
    "AI 2027": "AI 2027",
    "Taiwan Tinderbox": "Taiwan",
    "Frontiers in Disease Prevention": "disease prevention",
    "Epoch AI Robotics": "Epoch AI robotics",
}


def main():
    if not TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set — cannot check tournament IDs.")
        return

    for label, query in SEARCHES.items():
        print(f"\n=== {label} (search: {query!r}) ===")
        r = requests.get(
            "https://www.metaculus.com/api2/questions/",
            headers=HEADERS,
            params={"search": query, "limit": 5},
        )
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}")
            continue
        results = (r.json() or {}).get("results") or []
        if not results:
            print("  no questions found for this search")
            continue

        seen = set()
        for q in results:
            projects = q.get("projects", {}) or {}
            entries = (projects.get("tournament") or []) + (projects.get("question_series") or [])
            for e in entries:
                key = (e.get("id"), e.get("name"))
                if key in seen:
                    continue
                seen.add(key)
                print(f"  id={e.get('id')}, type={e.get('type')}, "
                      f"name={e.get('name')!r}, slug={e.get('slug')!r}")


if __name__ == "__main__":
    main()