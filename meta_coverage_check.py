"""
meta_coverage_check.py — Phase 0 measurement script (read-only).

Compares questions currently OPEN in each tracked tournament against
questions mike_iz_-bot has actually forecasted, and flags any gap.
This turns "we cover everything" from an assumption into a checked fact.

Run standalone, on its own cadence (recommend daily via cron-job.org),
separate from tournament_forecast.py / meta_batch_forecast.py — this
script only reads, it never forecasts or submits anything.

Output:
  reports/coverage_latest.json   — always overwritten, dashboard reads this
  reports/coverage_<ts>.json     — timestamped history, kept forever

Alerts via ntfy (meta_alerts.send_alert) only when a gap is found —
silent on clean runs so it doesn't add alert fatigue.

UNVERIFIED: ApiFilter(allowed_tournaments=...) below is my best guess at
the correct parameter name for filtering by tournament — it has not been
confirmed against forecasting_tools' actual ApiFilter signature or against
tournament_forecast.py's own fetch logic. Run once manually and sanity-check
the open_count numbers against what you see on Metaculus before trusting
this on a schedule.
"""

import os
import json
import asyncio
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from forecasting_tools import MetaculusClient, ApiFilter
from meta_alerts import send_alert

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

TOURNAMENTS = {
    33022: "FutureEval",
    32880: "ACX2026",
     1756: "Climate Tipping Points",
    33021: "Metaculus Cup",
}

BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")


def fetch_open_questions(client, tournament_id: int) -> set[int]:
    """All currently-open question_ids in a tournament."""
    try:
        questions = asyncio.run(
            client.get_questions_matching_filter(
                ApiFilter(
                    allowed_tournaments=[tournament_id],
                    allowed_statuses=["open"],
                ),
                num_questions=1000,
                error_if_question_target_missed=False,
            )
        )
        return {q.id_of_question for q in questions}
    except Exception as e:
        print(f"  ⚠️  could not fetch open questions for tournament {tournament_id}: {e}")
        return set()


def fetch_forecasted_questions(client) -> set[int]:
    """All question_ids mike_iz_-bot has ever forecasted, across tournaments."""
    try:
        questions = asyncio.run(
            client.get_questions_matching_filter(
                ApiFilter(is_previously_forecasted_by_user=True),
                num_questions=1000,
                error_if_question_target_missed=False,
            )
        )
        return {q.id_of_question for q in questions}
    except Exception as e:
        print(f"  ⚠️  could not fetch forecasted questions: {e}")
        return set()


def run_coverage_check():
    if not BOT_TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set — cannot run coverage check.")
        return

    client = MetaculusClient(token=BOT_TOKEN)
    forecasted = fetch_forecasted_questions(client)

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "tournaments": {},
        "total_gaps": 0,
    }

    gap_lines = []
    for tid, label in TOURNAMENTS.items():
        open_qs = fetch_open_questions(client, tid)
        missing = sorted(open_qs - forecasted)
        report["tournaments"][label] = {
            "tournament_id": tid,
            "open_count": len(open_qs),
            "forecasted_count": len(open_qs) - len(missing),
            "missing_question_ids": missing,
        }
        report["total_gaps"] += len(missing)
        if missing:
            gap_lines.append(f"{label}: {len(missing)} open question(s) not forecasted "
                              f"(e.g. {missing[:5]})")
        print(f"  {label}: {len(open_qs)} open, {len(missing)} missing")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with open(os.path.join(REPORTS_DIR, f"coverage_{ts}.json"), "w") as f:
        json.dump(report, f, indent=2)
    with open(os.path.join(REPORTS_DIR, "coverage_latest.json"), "w") as f:
        json.dump(report, f, indent=2)

    if gap_lines:
        send_alert(
            "\n".join(gap_lines),
            title=f"⚠️ Coverage gap: {report['total_gaps']} question(s) not forecasted"
        )
        print(f"  📬 Alert sent — {report['total_gaps']} total gap(s).")
    else:
        print("  ✅ Full coverage — no gaps.")


if __name__ == "__main__":
    run_coverage_check()