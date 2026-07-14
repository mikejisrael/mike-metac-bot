"""
meta_calibration_report.py — Phase 0 measurement script (read-only).

Reproduces the four Metaculus track-record charts you get from a profile
page (calibration curve, score scatter, score histogram, summary stats)
for mike_iz_-bot specifically, so you don't have to eyeball someone else's
profile to know where your own bot stands.

Run standalone, on demand or weekly via cron — read-only against the
Metaculus API, never touches the forecasting pipelines.

Output:
  reports/calibration_latest.json   — always overwritten, dashboard reads this
  reports/calibration_<ts>.json     — timestamped history

CALIBRATION NOTE: only binary questions have a meaningful "predicted
probability vs fraction resolved yes" calibration curve. Numeric and
multiple-choice questions are included in the summary stats and score
scatter, but excluded from the calibration buckets.
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

from forecasting_tools import MetaculusClient, ApiFilter

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")

BUCKET_WIDTH = 0.10  # 10-point buckets, same convention as Metaculus's own chart


def _bucket_for(p: float) -> str:
    lo = int(p // BUCKET_WIDTH) * BUCKET_WIDTH
    lo = min(lo, 0.9)  # clamp so p=1.0 lands in the 90-100% bucket, not a new one
    hi = lo + BUCKET_WIDTH
    return f"{lo:.0%}-{hi:.0%}"


def _extract_binary_prediction(q_json: dict):
    for path in [
        ("my_forecasts", "latest", "forecast_values"),
        ("my_forecasts", "latest", "probability_yes"),
    ]:
        val = q_json
        try:
            for key in path:
                val = val[key]
            if isinstance(val, list) and len(val) == 2:
                return val[1]  # [P(no), P(yes)]
            if isinstance(val, (int, float)):
                return val
        except (KeyError, TypeError):
            continue
    return None


def _extract_resolution(q_json: dict):
    q = q_json.get("question", q_json)
    return q.get("resolution")


def _is_resolved(q_json: dict) -> bool:
    """Same multi-signal check meta_dashboard.py's extract_score_info() uses
    — don't trust a single field alone. Used in place of ApiFilter's
    allowed_statuses=['resolved'] kwarg, which returned an essentially-empty
    result set (see run_calibration_report()'s fetch below for why).

    FIXED (2026-07-03): status was only ever checked at the top level, while
    resolution and actual_resolve_time were checked at BOTH top level and
    nested under "question" — an inconsistency, not a deliberate choice.
    Confirmed via a live diagnostic run that resolved questions genuinely
    exist in the fetched set (5 of them, that run) yet none were detected,
    which pointed straight at this gap: their status lives nested under
    question.status, not top-level, so q_json.get("status") alone could
    never see it. Added the missing nested check for symmetry with the
    other two fields."""
    q = q_json.get("question", q_json)
    return (
        q_json.get("status") == "resolved"
        or q.get("status") == "resolved"
        or q.get("resolution") is not None
        or q_json.get("actual_resolve_time") is not None
        or q.get("actual_resolve_time") is not None
    )


def _extract_peer_score(q_json: dict):
    """FIXED (2026-07-03, round 2): all five path guesses assumed
    my_forecasts/scoring/score_data live at the top level. Live diagnostic
    on a CONFIRMED-resolved question showed my_forecasts=None at the top
    level — the top-level object here is the POST wrapper (has 'title',
    'slug', 'short_title', 'curation_status', etc., confirmed via a full
    top-level key dump), and per-question data lives nested under
    q_json['question'], same root cause as the status bug fixed earlier
    today. Now tries every path against BOTH q_json and its nested
    'question' dict, top-level first."""
    q = q_json.get("question", q_json)
    paths = [
        ("my_forecasts", "score_data", "peer_score"),
        ("my_forecasts", "latest", "score_data", "peer_score"),
        ("my_forecasts", "latest", "peer_score"),
        ("scoring", "peer_score"),
        ("score_data", "peer_score"),
    ]
    for container in (q_json, q):
        for path in paths:
            val = container
            try:
                for key in path:
                    val = val[key]
                if val is not None:
                    return val
            except (KeyError, TypeError):
                continue
    return None


def run_calibration_report():
    if not BOT_TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not set — cannot run calibration report.")
        return

    client = MetaculusClient(token=BOT_TOKEN)

    # FIXED 2026-07-02: previously filtered server-side with
    # ApiFilter(..., allowed_statuses=["resolved"]) and got back an
    # essentially-empty result (0 scored questions) — even though the
    # dashboard's own fetch, using no status filter at all and determining
    # "resolved" from the actual per-question data instead, found 13
    # resolved & scored questions with a real average peer score.
    # meta_coverage_check.py's docstring already flags this exact family of
    # ApiFilter status kwargs as unverified; switching to the dashboard's
    # proven approach here too rather than trusting it a second time.
    try:
        questions = asyncio.run(
            client.get_questions_matching_filter(
                # CHANGED (2026-07-13): added group_question_mode=
                # "unpack_subquestions" — without it (default "exclude"),
                # Market Pulse's group_of_questions sub-questions would be
                # silently excluded from this fetch entirely, same bug
                # already found and fixed this week in meta_dashboard.py
                # and meta_coverage_check.py. Calibration CURVES stay
                # binary-only regardless (per the module docstring — that
                # part of Market Pulse genuinely doesn't apply, it's all
                # numeric), but score_scatter/average_peer_score/
                # questions_scored aggregate every type once resolved, and
                # would have silently missed Market Pulse questions there
                # without this fix.
                ApiFilter(is_previously_forecasted_by_user=True,
                          group_question_mode="unpack_subquestions"),
                num_questions=1000,
                error_if_question_target_missed=False,
            )
        )
    except Exception as e:
        print(f"  ⚠️  could not fetch forecasted questions: {e}")
        return

    buckets = defaultdict(lambda: {"predicted_sum": 0.0, "count": 0, "resolved_yes": 0})
    scatter = []
    scores = []

    for q in questions:
        q_json = q.api_json
        if not _is_resolved(q_json):
            continue

        peer_score = _extract_peer_score(q_json)
        close_time = (q_json.get("scheduled_close_time")
                      or (q_json.get("question") or {}).get("scheduled_close_time"))

        if peer_score is not None:
            scores.append(peer_score)
            scatter.append({
                "question_id": q.id_of_question,
                "close_time": close_time,
                "peer_score": peer_score,
            })

        q_type = (q_json.get("question") or {}).get("type")
        if q_type != "binary":
            continue
        prob = _extract_binary_prediction(q_json)
        resolution = _extract_resolution(q_json)
        if prob is None or resolution not in ("yes", "no"):
            continue
        b = _bucket_for(prob)
        buckets[b]["predicted_sum"] += prob
        buckets[b]["count"] += 1
        if resolution == "yes":
            buckets[b]["resolved_yes"] += 1

    calibration = []
    for b, d in sorted(buckets.items()):
        calibration.append({
            "bucket": b,
            "avg_predicted": d["predicted_sum"] / d["count"],
            "fraction_resolved_yes": d["resolved_yes"] / d["count"],
            "count": d["count"],
        })

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "questions_scored": len(scores),
        # NOTE: now counts ALL forecasted questions ever fetched (open +
        # resolved), not just resolved ones — the fetch no longer filters
        # server-side. questions_resolved_checked is the resolved-only count.
        "questions_predicted_total": len(questions),
        "questions_resolved_checked": sum(1 for q in questions if _is_resolved(q.api_json)),
        "average_peer_score": (sum(scores) / len(scores)) if scores else None,
        "calibration": calibration,
        "score_scatter": scatter,
    }

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with open(os.path.join(REPORTS_DIR, f"calibration_{ts}.json"), "w") as f:
        json.dump(report, f, indent=2)
    with open(os.path.join(REPORTS_DIR, "calibration_latest.json"), "w") as f:
        json.dump(report, f, indent=2)

    if scores:
        print(f"  ✅ {len(scores)} scored questions, avg peer score {report['average_peer_score']:.2f}")
    else:
        print("  No scored questions yet.")


if __name__ == "__main__":
    run_calibration_report()