"""
meta_debug_cp_probe.py — ONE-OFF diagnostic script, not part of the regular
pipeline. Fetches a specific known question directly via the same raw
api/posts/?tournaments= endpoint tournament_forecast.py uses, and prints
its full raw "question" dict — specifically checking whether "aggregations"
is present at all in this endpoint's response shape.

We know Q43367 ("Will the Community beat Dylan Matthews...") has a live
98% community prediction visible on the website with 90+ forecasters, so if
this endpoint's data doesn't show it, that's definitive: the endpoint
itself doesn't carry aggregations inline, and CP needs a separate per-
question fetch (like update_community_predictions does via a different
endpoint) rather than being free from the listing call.

Usage:
  python meta_debug_cp_probe.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
TARGET_QUESTION_ID = 43363  # the question_id for Q43367 (post id) — Will the
                             # Community beat Dylan Matthews in Metaculus Cup
                             # Summer 2026

headers = {
    "Authorization": f"Token {TOKEN}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

url = "https://www.metaculus.com/api/posts/?tournaments=metaculus-cup-summer-2026&limit=100"
print(f"Fetching: {url}\n")

found = False
page = 0
while url and page < 5:
    page += 1
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        print(f"HTTP {r.status_code}: {r.text[:200]}")
        break
    data = r.json()
    for post in data.get("results", []):
        q = post.get("question", {})
        if q.get("id") == TARGET_QUESTION_ID:
            found = True
            print(f"FOUND Q{TARGET_QUESTION_ID}: {q.get('title', '(no title)')[:80]}\n")
            print("Top-level question dict keys:")
            print(list(q.keys()))
            print()
            agg = q.get("aggregations")
            print(f"'aggregations' present: {agg is not None}")
            if agg:
                print(f"aggregations keys: {list(agg.keys())}")
                print()
                print("Full aggregations content:")
                print(json.dumps(agg, indent=2)[:2000])
            else:
                print("No 'aggregations' key in this response at all.")

            print()
            print("Timing fields (testing the theory that CP visibility is time-gated,")
            print("not account-gated — same null result happened on BOTH mike_iz_ and")
            print("mike_iz_-bot yesterday/today on this exact question):")
            for field in ["cp_reveal_time", "spot_scoring_time", "open_time",
                          "scheduled_close_time", "status", "default_aggregation_method"]:
                print(f"  {field}: {q.get(field)!r}")
            print()
            print(f"  Current time (UTC): {__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()}")
            break
    if found:
        break
    url = data.get("next")

if not found:
    print(f"Q{TARGET_QUESTION_ID} not found in the Metaculus Cup Summer 2026 listing "
          f"(checked {page} page(s)) — it may have moved tournaments, closed, or the "
          f"ID may be stale. Not itself informative either way about the aggregations question.")

# ─── Part 2: test the ACTUAL fix — api2/questions/?ids= endpoint ──────────────
# This is the endpoint tournament_forecast.py now calls for real CP, the same
# one meta_batch_forecast.py's update_community_predictions() has used
# successfully for months. Testing it directly here sidesteps the "no fresh
# questions available" problem entirely — CP fetching doesn't care whether a
# question is new or already-forecast.
print(f"\n{'='*70}")
print(f"PART 2: testing the api2/questions/?ids= endpoint directly")
print(f"{'='*70}")

cp_url = f"https://www.metaculus.com/api2/questions/?ids={TARGET_QUESTION_ID}&limit=10"
print(f"Fetching: {cp_url}\n")
# Using ONLY the Authorization header here — no spoofed User-Agent — to match
# update_community_predictions()'s proven-working request exactly. Part 1's
# request above used a fake Chrome User-Agent; testing whether THAT was
# actually the cause of getting back unrelated question IDs, rather than
# the ids= filter itself being unreliable.
bare_headers = {"Authorization": f"Token {TOKEN}"}
r2 = requests.get(cp_url, headers=bare_headers, timeout=20)
if r2.status_code != 200:
    print(f"HTTP {r2.status_code}: {r2.text[:200]}")
else:
    data2 = r2.json()
    results2 = data2.get("results", [])
    print(f"Got {len(results2)} result(s)")
    for item in results2:
        q2 = item.get("question", {})
        q2_id = q2.get("id") or item.get("id")
        print(f"\nQuestion id in response: {q2_id}")
        agg2 = q2.get("aggregations", {})
        print(f"aggregations keys: {list(agg2.keys())}")
        latest2 = agg2.get("recency_weighted", {}).get("latest")
        print(f"recency_weighted.latest: {json.dumps(latest2, indent=2)[:500] if latest2 else latest2}")