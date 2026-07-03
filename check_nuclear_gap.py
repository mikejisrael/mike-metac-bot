"""
check_nuclear_gap.py — one-off diagnostic, not part of the regular
pipeline. Isolates why meta_batch_forecast.py found 0 new questions
despite visibly-open, unforecasted Nuclear Risk Horizons questions.

Tests three things separately:
  1. Nuclear Risk Horizons (1173) alone, no forecaster threshold
  2. Nuclear Risk Horizons (1173) alone, WITH MIN_FORECASTERS=5
  3. The full 8-tournament ALLOWED_TOURNAMENTS list, as meta_batch_forecast.py
     actually calls it

Comparing (1) vs (2) isolates whether MIN_FORECASTERS is the culprit.
Comparing (1)+(2) vs (3) isolates whether combining 8 mixed str/int
tournament values in one ApiFilter call is the culprit (this codebase has
already found two other cases of Metaculus filter/list endpoints not
honoring parameters the way forecasting_tools' docs imply — see
meta_coverage_check.py's UNVERIFIED note and the api2/questions/?ids=
issue in project history).

Safe to delete after use.
"""

import os
import asyncio
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

from forecasting_tools import MetaculusClient, ApiFilter

TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
NUCLEAR_ID = 1173
DAYS_AHEAD = 365

# Same 8-tournament list currently in meta_batch_forecast.py's
# ALLOWED_TOURNAMENTS, copied here rather than imported — importing that
# module would trigger its top-level client construction and monkeypatch
# side effects, which this diagnostic doesn't need.
ALLOWED_TOURNAMENTS = [
    "ACX2026", "climate", "metaculus-cup-summer-2026",
    1173, 32774, 3048, 2018, 2995,
]


async def run():
    if not TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set.")
        return

    client = MetaculusClient(token=TOKEN)
    now = datetime.now(timezone.utc)

    print("=== 1. Nuclear Risk Horizons ONLY, no forecaster threshold ===")
    try:
        qs = await client.get_questions_matching_filter(
            ApiFilter(
                allowed_types=["binary"],
                allowed_statuses=["open"],
                allowed_tournaments=[NUCLEAR_ID],
                close_time_gt=now,
                close_time_lt=now + timedelta(days=DAYS_AHEAD),
            ),
            num_questions=200,
            error_if_question_target_missed=False,
        )
        print(f"  -> {len(qs)} open binary questions found")
        for q in qs[:5]:
            print(f"     Q{q.id_of_question}: {q.question_text[:70]}")
    except Exception as e:
        print(f"  ⚠️  error: {e}")

    print("\n=== 2. Nuclear Risk Horizons ONLY, WITH MIN_FORECASTERS=5 ===")
    try:
        qs = await client.get_questions_matching_filter(
            ApiFilter(
                allowed_types=["binary"],
                allowed_statuses=["open"],
                allowed_tournaments=[NUCLEAR_ID],
                close_time_gt=now,
                close_time_lt=now + timedelta(days=DAYS_AHEAD),
                num_forecasters_gte=5,
            ),
            num_questions=200,
            error_if_question_target_missed=False,
        )
        print(f"  -> {len(qs)} open binary questions found (>=5 forecasters)")
    except Exception as e:
        print(f"  ⚠️  error: {e}")

    print("\n=== 3. Full 8-tournament ALLOWED_TOURNAMENTS list (as the real script calls it) ===")
    try:
        qs = await client.get_questions_matching_filter(
            ApiFilter(
                allowed_types=["binary"],
                allowed_statuses=["open"],
                allowed_tournaments=ALLOWED_TOURNAMENTS,
                close_time_gt=now,
                close_time_lt=now + timedelta(days=DAYS_AHEAD),
                num_forecasters_gte=5,
            ),
            num_questions=200,
            error_if_question_target_missed=False,
        )
        print(f"  -> {len(qs)} open binary questions found across all 8")
        # Count how many of THESE actually belong to Nuclear, by checking
        # each question's own projects field — the reliable method.
        nuclear_count = 0
        for q in qs:
            projects = (getattr(q, "api_json", None) or {}).get("projects", {}) or {}
            entries = (projects.get("tournament") or []) + (projects.get("question_series") or [])
            if any(e.get("id") == NUCLEAR_ID for e in entries):
                nuclear_count += 1
        print(f"  -> of those, {nuclear_count} belong to Nuclear Risk Horizons")
    except Exception as e:
        print(f"  ⚠️  error: {e}")


if __name__ == "__main__":
    asyncio.run(run())