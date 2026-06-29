"""
meta_debug_ids_probe.py — one-shot probe for Metaculus question-fetch
endpoints. Two modes:

  python meta_debug_ids_probe.py [qid]
      Default mode. Hits /api2/questions/?ids=<qid> (the LIST/filter
      endpoint) and shows exactly what comes back, with zero parsing
      assumptions — used to confirm that endpoint ignores the ids= filter
      entirely (confirmed 2026-06-29: returns recent unrelated questions
      regardless of what ID is requested).

  python meta_debug_ids_probe.py --single
      Hits /api2/questions/{id}/ (the SINGULAR detail endpoint, trailing
      slash, no list filter) for 3 known-good question_ids captured from a
      real tournament_forecast.py submission today, and reports a clean
      match/no-match per ID against the title we know is correct. This is
      the endpoint the OLD fetch_question_by_id() used (with a question_id
      where it should've used a post_id, producing the Q38099->mortgage
      mismatch) — this mode tells us whether the singular endpoint is
      actually keyed correctly by question_id, or just as broken as ids=.

  python meta_debug_ids_probe.py --single <qid> [expected_title]
      Test one specific ID via the singular endpoint instead of the 3
      built-in known-good ones. expected_title is optional — if omitted,
      just prints what comes back without a match/no-match verdict.
"""

import sys
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

from meta_question_matching import titles_match
from meta_cp_extract import extract_live_cp

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
HEADERS = {"Authorization": f"Token {TOKEN}"}

# Known-good (question_id, expected_title) pairs — taken directly from
# batch_results_20260629_1107.json (tournament_batches), real, fresh,
# successful submissions from today's tournament run.
KNOWN_GOOD = [
    (43331, "Which party will hold the most seats in the US House of Representatives after the 2026 elections?"),
    (43325, "What will be Donald Trump's net approval on December 31, 2026?"),
    (43322, "How many of these 15 top US executive branch officials will be out before 2027?"),
]


def probe_list_endpoint(qid: int):
    """Hits the ?ids= LIST/filter endpoint. This is the one already
    confirmed broken — kept here so this remains the one place to check
    it again in future (e.g. if Metaculus ships a fix)."""
    url = f"https://www.metaculus.com/api2/questions/?ids={qid}&limit=10"
    print(f"Fetching: {url}\n")

    r = requests.get(url, headers=HEADERS, timeout=20)
    print(f"HTTP status: {r.status_code}")
    print(f"Response headers content-type: {r.headers.get('content-type')}\n")

    try:
        data = r.json()
    except Exception as e:
        print(f"❌ Could not parse JSON: {e}")
        print(f"Raw body (first 500 chars): {r.text[:500]!r}")
        return

    print(f"Top-level keys: {list(data.keys())}")
    print(f"data.get('count'): {data.get('count')}")

    results = data.get("results", [])
    print(f"len(results): {len(results)}\n")

    print("=" * 60)
    print("FULL FIRST RESULT (raw, unfiltered):")
    print("=" * 60)
    if results:
        print(json.dumps(results[0], indent=2)[:3000])
    else:
        print("(no results returned at all)")

    print("\n" + "=" * 60)
    print("IDs actually present in the response:")
    print("=" * 60)
    for item in results:
        q = item.get("question") or {}
        print(f"  item.get('id')={item.get('id')}  "
              f"item['question'].get('id')={q.get('id')}  "
              f"title={(q.get('title') or item.get('title') or '')[:60]}")

    returned_ids = [item.get("id") for item in results] + \
                   [(item.get("question") or {}).get("id") for item in results]
    print(f"\nRequested ID: {qid}")
    print(f"Was it among the returned IDs? {qid in returned_ids}")


def probe_single_endpoint(qid: int, expected_title: str | None):
    """Hits the SINGULAR /api2/questions/{id}/ detail endpoint."""
    url = f"https://www.metaculus.com/api2/questions/{qid}/"
    print(f"\nFetching: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        print(f"  HTTP status: {r.status_code}")
        if r.status_code != 200:
            print(f"  Body (first 200 chars): {r.text[:200]!r}")
            return
        data = r.json()
        # data may itself be the question, or have a nested "question" key —
        # checking both shapes since we've seen both across this codebase.
        q = data.get("question", data)
        returned_title = q.get("title") or data.get("title") or ""
        returned_id = q.get("id") or data.get("id")

        print(f"  Requested question_id: {qid}")
        print(f"  Returned id:            {returned_id}")
        print(f"  Returned title: {returned_title[:70]}")
        if expected_title:
            match = titles_match(expected_title, returned_title)
            print(f"  Expected title: {expected_title[:70]}")
            print(f"  {'✅ MATCH' if match else '🛑 MISMATCH'}")

        # The real point of all this ID work: is CP actually non-null
        # here? The listing endpoint (api/posts/?tournaments=) is already
        # confirmed to return null aggregations regardless of ID
        # correctness. This is the first time we're checking whether the
        # SINGULAR detail endpoint is any different.
        agg = q.get("aggregations", {}) or {}
        print(f"  aggregations keys: {list(agg.keys())}")
        for agg_key, agg_val in agg.items():
            print(f"\n  --- aggregations['{agg_key}'] (full, raw) ---")
            print("  " + json.dumps(agg_val, indent=2)[:2000].replace("\n", "\n  "))
        node = agg.get("recency_weighted") or agg.get("unweighted") or agg.get("metaculus_prediction") or {}
        q_type_guess = q.get("type", "binary")
        cp = extract_live_cp(data, q_type_guess)
        print(f"\n  extract_live_cp() result: {cp!r}  {'✅ non-null' if cp is not None else '🛑 still None'}")

        # If CP came back null, check whether Metaculus is deliberately
        # gating it from this (bot) account for this specific question —
        # this field showed up in the very first probe today and was
        # never actually checked.
        if cp is None:
            print(f"  include_bots_in_aggregates: {q.get('include_bots_in_aggregates')}")
            print(f"  cp_reveal_time: {q.get('cp_reveal_time')}")
            print(f"  status: {q.get('status')}")
            print(f"  default_aggregation_method: {q.get('default_aggregation_method')}")
    except Exception as e:
        print(f"  ❌ Error: {e}")


def probe_scan(sample_limit: int | None = None):
    """Scan every currently open question across all 4 real tournaments
    and tally include_bots_in_aggregates True/False/missing — a fast,
    decisive answer to 'is CP actually withheld from bots while open,
    across the board, or just on the 2 questions we happened to test?'

    sample_limit: cap per tournament to keep runtime reasonable (None =
    check everything — slower, ~1.3s/question across ~200+ questions)."""
    import meta_batch_forecast as bf
    import time as _time

    tournaments = bf.ALLOWED_TOURNAMENTS
    tally = {True: 0, False: 0, "missing": 0}
    by_tournament = {}

    for tid in tournaments:
        url = f"https://www.metaculus.com/api/posts/?tournaments={tid}&limit=100"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                print(f"  ⚠️  Tournament {tid}: HTTP {r.status_code}")
                continue
            posts = r.json().get("results", [])
        except Exception as e:
            print(f"  ⚠️  Tournament {tid}: {e}")
            continue

        post_ids = [p.get("id") for p in posts if p.get("id") is not None]
        if sample_limit:
            post_ids = post_ids[:sample_limit]

        t_tally = {True: 0, False: 0, "missing": 0}
        print(f"\nTournament {tid}: scanning {len(post_ids)} open question(s)...")
        for i, pid in enumerate(post_ids, 1):
            _time.sleep(1.2)
            try:
                rr = requests.get(f"https://www.metaculus.com/api2/questions/{pid}/",
                                   headers=HEADERS, timeout=20)
                if rr.status_code != 200:
                    continue
                d = rr.json()
                q = d.get("question", d)
                val = q.get("include_bots_in_aggregates")
                key = val if val in (True, False) else "missing"
                t_tally[key] += 1
                tally[key] += 1
            except Exception:
                continue
            if i % 20 == 0:
                print(f"    ...{i}/{len(post_ids)} checked")

        by_tournament[tid] = t_tally
        print(f"  Tournament {tid} result: True={t_tally[True]}  "
              f"False={t_tally[False]}  missing={t_tally['missing']}")

    print("\n" + "=" * 60)
    print("OVERALL TALLY across all tournaments")
    print("=" * 60)
    for tid, t in by_tournament.items():
        print(f"  {tid}: True={t[True]}  False={t[False]}  missing={t['missing']}")
    print("-" * 60)
    print(f"  TOTAL: True={tally[True]}  False={tally[False]}  missing={tally['missing']}")
    if tally[False] > tally[True]:
        print("\n  -> CP is withheld from the bot on the large majority of open "
              "questions. Treat this as expected platform behavior, not a bug — "
              "the existing 'cp is None' fallback prompts are already correct.")
    elif tally[True] > tally[False]:
        print("\n  -> CP IS available on most open questions. The 2 we hand-tested "
              "were the unlucky exceptions — worth revisiting the CP-fetch code "
              "(switch from the broken ?ids= filter to per-post singular calls).")



if __name__ == "__main__":
    # Defensive: a command copy-pasted out of a chat/markdown source can
    # pick up a non-breaking space (U+00A0) instead of a normal space
    # between unquoted tokens, which cmd.exe does NOT treat as a
    # separator — so "--single 36369" can arrive as one mangled argv
    # element instead of two. Fix ONLY the first token here — the title
    # argument legitimately contains real spaces and must NOT be touched,
    # or it'd get shredded into separate words.
    args = list(sys.argv[1:])
    if args:
        first = args[0].replace("\xa0", " ")
        if " " in first:
            args[0:1] = first.split(" ")

    if args and args[0] == "--scan":
        limit = int(args[1]) if len(args) > 1 else None
        probe_scan(sample_limit=limit)
    elif args and args[0] == "--single":
        rest = args[1:]
        if rest:
            qid = int(rest[0])
            expected = rest[1] if len(rest) > 1 else None
            probe_single_endpoint(qid, expected)
        else:
            for qid, expected_title in KNOWN_GOOD:
                probe_single_endpoint(qid, expected_title)
    else:
        qid = int(args[0]) if args else 44150
        probe_list_endpoint(qid)