"""
list_tournaments.py — one-off diagnostic. Run this from your project root
(same place you run meta_dashboard.py) to find the real tournament IDs/slugs
your mike_iz_-bot account's questions actually belong to.

Run:
    python list_tournaments.py

It prints every distinct tournament (id, slug/name) seen across every
question your BOT account has ever predicted on, plus how many questions
fall under each. Use that output to tell me which is which (FutureEval /
ACX2026 / Climate Tipping Points / Metaculus Cup Summer 2026) — FutureEval
should show id 33022, confirming the method works, and the other three IDs
will be new info.
"""

import os
import asyncio
import json
from collections import defaultdict
from dotenv import load_dotenv
from forecasting_tools import MetaculusClient, ApiFilter

load_dotenv()

BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("METAC_TOURNAMENT_TOKEN not found in .env")

client = MetaculusClient(token=BOT_TOKEN)


async def main():
    questions = await client.get_questions_matching_filter(
        ApiFilter(is_previously_forecasted_by_user=True),
        num_questions=1000,
        error_if_question_target_missed=False,
    )

    seen = defaultdict(list)  # (id, name) -> [question_ids]
    no_project_info = []

    for q in questions:
        raw = q.api_json or {}
        projects = raw.get("projects", {})

        found_any = False

        dp = projects.get("default_project")
        if dp and dp.get("type") == "tournament":
            key = (dp.get("id"), dp.get("name") or dp.get("slug"))
            seen[key].append(q.id_of_question)
            found_any = True

        for t in projects.get("tournament", []) or []:
            key = (t.get("id"), t.get("name") or t.get("slug"))
            seen[key].append(q.id_of_question)
            found_any = True

        if not found_any:
            no_project_info.append(q.id_of_question)

    print(f"\nTotal questions checked: {len(questions)}\n")
    print("Distinct tournaments found:")
    print("-" * 60)
    for (tid, name), qids in sorted(seen.items(), key=lambda kv: -len(kv[1])):
        print(f"  id={tid}  name/slug={name!r}  -> {len(qids)} questions")
        print(f"      sample question_ids: {qids[:5]}")

    if no_project_info:
        print(f"\n{len(no_project_info)} questions had no tournament project info "
              f"(likely non-tournament / general questions):")
        print(f"  sample: {no_project_info[:10]}")

    # Also dump the raw projects blob of the first question, just in case
    # the shape above doesn't match what your account's data actually has.
    if questions:
        print("\nRaw 'projects' field of first question, for sanity-check:")
        print(json.dumps(questions[0].api_json.get("projects", {}), indent=2)[:2000])


if __name__ == "__main__":
    asyncio.run(main())