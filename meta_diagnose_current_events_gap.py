"""
meta_diagnose_current_events_gap.py — one-off diagnostic (2026-07-06).

NOT meant to run on a schedule — delete once the discrepancy below is
understood and (if needed) fixed upstream in meta_coverage_check.py /
meta_batch_forecast.py.

Context: meta_coverage_check.py's Current Events report says
open_count=6, forecasted_count=4, with only 1 real gap + 1 gated
(too_far_out) accounting for the other 2 of those 6. But Metaculus's own
"Current Events" series page, viewed AS mike_iz_-bot, says "6 questions
not predicted" — meaning the bot itself agrees 0 of these 6 have been
forecasted, not 4. That's a real discrepancy between what
fetch_forecasted_questions()'s ApiFilter(is_previously_forecasted_by_user=True)
believes and what's actually true.

This script takes the 6 post_ids Mike read directly off that page and,
for each one:
  - resolves post_id -> nested question_id (the id that actually matters
    for matching against the "forecasted" set — see this codebase's
    long-standing post_id/question_id distinction)
  - checks whether that question_id is in the SAME "forecasted" set
    meta_coverage_check.py builds
  - runs it through the same forecast gate meta_coverage_check.py uses,
    so we can see whether it's classified as real/gated/forecasted
  - reads my_forecasts.latest directly off the raw API response, which
    is the actual ground truth for "has the bot forecasted this or not"
    — independent of ApiFilter's is_previously_forecasted_by_user flag,
    so a mismatch between the two here is the smoking gun if there is one

One leading hypothesis, unconfirmed: some of these may be non-binary
(date/numeric) questions. meta_batch_forecast.py only ever forecasts
binary questions (`binary = [q for q in questions if isinstance(q,
BinaryQuestion)]`), so a non-binary question could plausibly still show
up in some other API field as "touched" without ever having a real
submitted forecast. This script surfaces question_type for exactly that
reason, but doesn't assume the answer — read the actual output.
"""

import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

from forecasting_tools import MetaculusClient, ApiFilter
from meta_forecast_gate import forecast_gate_failure_reason

BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")

# The 6 post_ids Mike read directly off Metaculus's Current Events page,
# logged in as mike_iz_-bot, under "My Participation: 6 questions not predicted".
POST_IDS = [44159, 42473, 44163, 43468, 38941, 38895]


async def main():
    if not BOT_TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set — cannot run.")
        return

    client = MetaculusClient(token=BOT_TOKEN)

    # Same "forecasted" set meta_coverage_check.py's fetch_forecasted_questions() builds.
    forecasted = await client.get_questions_matching_filter(
        ApiFilter(is_previously_forecasted_by_user=True),
        num_questions=1000,
        error_if_question_target_missed=False,
    )
    forecasted_ids = {q.id_of_question for q in forecasted}
    print(f"'forecasted' set (is_previously_forecasted_by_user) has {len(forecasted_ids)} question_ids total.\n")

    for post_id in POST_IDS:
        print(f"--- post {post_id} ---")
        try:
            # Synchronous, not async — confirmed elsewhere in this codebase
            # (meta_batch_forecast.py's fetch_question_series_questions()).
            q = client.get_question_by_post_id(post_id=post_id)
        except Exception as e:
            print(f"  ❌ FETCH FAILED: {e}\n")
            continue
        if isinstance(q, list):
            print(f"  ⚠️  returned a list of {len(q)} sub-questions (grouped/conditional post?) — "
                  f"inspecting each:")
            sub_qs = q
        else:
            sub_qs = [q]

        for sq in sub_qs:
            qid = getattr(sq, "id_of_question", None)
            qtype = getattr(sq, "question_type", None)
            nf = getattr(sq, "num_forecasters", None)
            ct = getattr(sq, "close_time", None)
            in_forecasted_set = qid in forecasted_ids
            reason = forecast_gate_failure_reason(qtype, nf, ct)

            my_forecast = None
            try:
                raw = sq.api_json
                inner = raw.get("question", raw)
                my_forecast = (inner.get("my_forecasts") or {}).get("latest")
            except Exception as e:
                print(f"    (couldn't read raw api_json for ground-truth check: {e})")

            print(f"    question_id: {qid}")
            print(f"    type={qtype!r}  num_forecasters={nf!r}  close_time={ct}")
            print(f"    in coverage-check's 'forecasted' set? {in_forecasted_set}")
            print(f"    forecast_gate result: {'PASSES' if reason is None else reason}")
            print(f"    my_forecasts.latest present (ground truth)? {my_forecast is not None}")
            if my_forecast is not None:
                print(f"        -> {my_forecast}")
            if in_forecasted_set != (my_forecast is not None):
                print(f"    🚨 MISMATCH: ApiFilter says 'forecasted={in_forecasted_set}' but "
                      f"my_forecasts.latest says 'has forecast={my_forecast is not None}'")
        print()


asyncio.run(main())
