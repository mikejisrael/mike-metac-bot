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

Alerts via ntfy (meta_alerts.send_alert) only when a REAL gap is found —
silent on clean runs so it doesn't add alert fatigue.

CHANGED (2026-07-03): "gap" used to mean any open question the bot hadn't
forecasted — which conflated genuine misses with questions that
meta_batch_forecast.py would never have forecasted anyway (too few
forecasters, wrong question type, closing too far out). A run showing
"130 gaps" was mostly the latter, making the number useless for deciding
whether anything was actually wrong. Every missing question is now run
through meta_forecast_gate.passes_forecast_gate() — the SAME gate
meta_batch_forecast.py itself uses to decide what's worth forecasting —
and split into:
  - "gated": correctly, deliberately not forecasted. Shown in the report
    for visibility, never alerted on.
  - "real": passes the gate and genuinely should have been forecasted.
    This is the number that matters, and the only one that triggers
    send_alert() now.
total_gaps in the JSON report now means REAL gaps specifically (this is
also what the dashboard's "Coverage gaps" card reads) — total_gated is a
separate, new key for the informational, non-alerting count.

Gate-input extraction failure (couldn't determine question_type,
num_forecasters, OR close_time at all) is treated as a REAL gap rather
than silently bucketed as "gated" — an extraction failure should never be
the reason a genuine problem goes unseen.

RESOLVED (2026-07-03): a live run found 66 of 128 gated questions failing
specifically on "too_far_out" (close_time beyond meta_forecast_gate.
DAYS_AHEAD), with a suspicious pattern — every sampled value landed
exactly on a year boundary (Dec 31/Jan 1) across many different years.
Investigated as a possible placeholder/sentinel bug (there's precedent:
scheduled_resolution_time is documented elsewhere in this codebase as
returning a fake default when unset). Ruled out by comparing the parsed
value against the raw API response directly: the raw scheduled_close_time
field genuinely carries that exact value — not a substitution. Confirmed
via example (a "will there be a war between Russia and a NATO country,
but not the US, by 2035?" question, with real community discussion about
its long resolution window) that these are genuine long-horizon
"by [year]" questions, common in tournaments themed around tail risks
(Nuclear Risk Horizons, Taiwan Tinderbox, etc.) — not a data bug. Mike's
call, 2026-07-03: keep excluding them (DAYS_AHEAD stays as a real
"don't bother forecasting something whose resolution feedback is years
away" policy for these tournaments, accepting reduced coverage there in
exchange for faster calibration-data turnaround elsewhere). No code
change needed — the gate was already doing the right thing; this only
updates messaging that used to describe the pattern as suspicious.

UNVERIFIED FETCH SCOPE: the 5 tournaments in meta_forecast_gate.
QUESTION_SERIES_IDS (Nuclear Risk Horizons, Current Events, Taiwan
Tinderbox, Economic Indicators, Animal Welfare) are type='question_series'
on Metaculus's side, not type='tournament'. meta_batch_forecast.py proved
ApiFilter's allowed_tournaments/`tournaments=` param silently fails to
scope these — it returned count≈7427 (essentially the whole site) for
Nuclear Risk Horizons instead of the real ~37, and had to switch to a raw
`project=` fetch instead. fetch_open_questions() below still uses the
unproven allowed_tournaments mechanism for ALL 9 tournaments, including
these 5 — deliberately NOT fixed here (2026-07-03, Mike's call, given a
standing suspicion the broader question-fetch pipeline may still be
missing whole pools of questions). Every one of these 5 tournaments is
flagged "unverified_fetch_scope": true in the report and marked with ⚠️ in
the console output, whether or not it currently shows a gap — a clean 0
from a possibly-broken fetch isn't proof of real coverage either.
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from collections import Counter

from dotenv import load_dotenv
load_dotenv()

from forecasting_tools import MetaculusClient, ApiFilter
from meta_alerts import send_alert
from meta_forecast_gate import passes_forecast_gate, forecast_gate_failure_reason, QUESTION_SERIES_IDS

REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

TOURNAMENTS = {
    33022: "FutureEval",
    32880: "ACX2026",
     1756: "Climate Tipping Points",
    33021: "Metaculus Cup",
    # EXPANDED 2026-07-02: matches meta_batch_forecast.py's ALLOWED_TOURNAMENTS
    # expansion — same 5 series, same IDs (resolved via resolve_series_ids.py,
    # not guessed). See module docstring: UNVERIFIED FETCH SCOPE for these 5.
     1173: "Nuclear Risk Horizons",
    32774: "Current Events",
     3048: "Taiwan Tinderbox",
     2018: "Economic Indicators",
     2995: "Animal Welfare",
}

BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")


def fetch_open_questions(client, tournament_id: int) -> list:
    """CHANGED (2026-07-03): now returns the full question objects, not
    just a bare set of ids — gate classification needs question_type,
    num_forecasters, and close_time off each one. Callers needing just the
    id set can do {q.id_of_question for q in fetch_open_questions(...)}."""
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
        return list(questions)
    except Exception as e:
        print(f"  ⚠️  could not fetch open questions for tournament {tournament_id}: {e}")
        return []


def _gate_inputs(q) -> tuple:
    """Extract (question_type, num_forecasters, close_time) from a
    forecasting_tools question object. All three are confirmed real,
    declared fields — checked directly against the installed library
    (BinaryQuestion.model_fields), not assumed from API-shape guesswork
    the way earlier bugs in this codebase's sibling scripts were."""
    return (
        getattr(q, "question_type", None),
        getattr(q, "num_forecasters", None),
        getattr(q, "close_time", None),
    )


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
        "total_gaps": 0,     # REAL gaps only (see module docstring) — this drives the alert
        "total_gated": 0,    # informational, never alerts
    }

    total_open = 0
    total_forecasted = 0
    tournaments_with_real_gaps = []
    diagnostic_printed = False
    global_gated_reasons = Counter()
    too_far_out_samples = []

    for tid, label in TOURNAMENTS.items():
        open_questions = fetch_open_questions(client, tid)
        open_qs = {q.id_of_question for q in open_questions}
        by_id = {q.id_of_question: q for q in open_questions}
        missing = sorted(open_qs - forecasted)

        gated_ids = []
        real_ids = []
        gated_reasons_this_tournament = Counter()
        for qid in missing:
            q = by_id.get(qid)
            if q is None:
                # Shouldn't happen (qid came from open_qs, built from the
                # same by_id dict) but fail toward visibility, not silence.
                real_ids.append(qid)
                continue
            q_type, num_forecasters, close_time = _gate_inputs(q)
            extraction_failed = q_type is None and num_forecasters is None and close_time is None
            if extraction_failed:
                # Couldn't tell whether this SHOULD be gated — treat as a
                # real gap so an extraction problem surfaces loudly rather
                # than silently hiding a possible genuine miss.
                real_ids.append(qid)
            else:
                # CHANGED (2026-07-03): was passes_forecast_gate() (plain
                # bool) — now the reason-reporting variant, so the gated
                # bucket can be broken down by WHY, not just counted.
                #
                # RESOLVED (2026-07-03): a live run initially surfaced a
                # suspicious close_time (2049, on year-boundary dates) that
                # looked like it could be a placeholder/sentinel bug. Ruled
                # out by comparing against the raw API response and by
                # example (a genuine "war by 2035" question) — these are
                # real, long-horizon "by [year]" questions, common in
                # tail-risk-themed tournaments, not a data bug. Mike's call:
                # keep excluding them. See module docstring for the full
                # investigation. Sample collection below is kept as an
                # ongoing visibility tool, not a bug hunt — lets Mike
                # glance at which specific long-horizon questions are
                # being excluded under the current policy.
                reason = forecast_gate_failure_reason(q_type, num_forecasters, close_time)
                if reason is None:
                    real_ids.append(qid)
                else:
                    gated_ids.append(qid)
                    gated_reasons_this_tournament[reason] += 1
                    global_gated_reasons[reason] += 1
                    if reason == "too_far_out" and len(too_far_out_samples) < 15:
                        too_far_out_samples.append(
                            {"question_id": qid, "tournament": label, "close_time": close_time.isoformat()}
                        )

            if not diagnostic_printed and missing:
                diagnostic_printed = True
                print(f"  🔍 DIAGNOSTIC: sample missing question_id={qid} in {label} — "
                      f"question_type={q_type!r}, num_forecasters={num_forecasters!r}, "
                      f"close_time={close_time!r}, extraction_failed={extraction_failed}")

        report["tournaments"][label] = {
            "tournament_id": tid,
            "unverified_fetch_scope": tid in QUESTION_SERIES_IDS,
            "open_count": len(open_qs),
            "forecasted_count": len(open_qs) - len(missing),
            "missing_gated_ids": gated_ids,
            "missing_gated_reasons": dict(gated_reasons_this_tournament),
            "missing_real_ids": real_ids,
        }
        report["total_gaps"] += len(real_ids)
        report["total_gated"] += len(gated_ids)
        total_open += len(open_qs)
        total_forecasted += len(open_qs) - len(missing)
        if real_ids:
            scope_flag = " ⚠️unverified-scope" if tid in QUESTION_SERIES_IDS else ""
            tournaments_with_real_gaps.append(f"{label} ({len(real_ids)}){scope_flag}")

    report["total_gated_reasons"] = dict(global_gated_reasons)
    report["too_far_out_samples"] = too_far_out_samples

    print(f"  {total_forecasted}/{total_open} open questions forecasted across "
          f"{len(TOURNAMENTS)} tournaments — {report['total_gaps']} REAL gap(s), "
          f"{report['total_gated']} correctly gated"
          f"{' | real gaps in: ' + ', '.join(tournaments_with_real_gaps) if tournaments_with_real_gaps else ''}")
    print(f"  Gated breakdown: {dict(global_gated_reasons)} "
          f"— too_far_out is confirmed (2026-07-03) to be genuine long-horizon "
          f"\"by [year]\" questions, common in tail-risk tournaments — not a "
          f"data bug. Intentionally excluded per policy; not alert-worthy.")
    if too_far_out_samples:
        print(f"  too_far_out sample ({len(too_far_out_samples)} shown) — "
              f"which specific long-horizon questions are being excluded this run:")
        for s in too_far_out_samples:
            print(f"    Q{s['question_id']} ({s['tournament']}): {s['close_time']}")
    unverified_labels = [TOURNAMENTS[tid] for tid in QUESTION_SERIES_IDS if tid in TOURNAMENTS]
    print(f"  ⚠️  Unverified fetch scope for: {', '.join(unverified_labels)} — see module "
          f"docstring. A clean 0 for these isn't proof of real coverage yet.")
    # Per-tournament breakdown is available in the JSON report (and on
    # metaculus.com directly) for whenever that level of detail is wanted —
    # console/alert output stays at the summary level deliberately.

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with open(os.path.join(REPORTS_DIR, f"coverage_{ts}.json"), "w") as f:
        json.dump(report, f, indent=2)
    with open(os.path.join(REPORTS_DIR, "coverage_latest.json"), "w") as f:
        json.dump(report, f, indent=2)

    if report["total_gaps"] > 0:
        send_alert(
            f"{total_forecasted}/{total_open} open questions forecasted across "
            f"{len(TOURNAMENTS)} tournaments.\n"
            f"{report['total_gaps']} REAL gap(s) in: {', '.join(tournaments_with_real_gaps)}\n"
            f"({report['total_gated']} more correctly gated — below forecaster "
            f"threshold, wrong type, or too far out — not included above.)\n"
            f"Check metaculus.com or the dashboard for per-question detail.",
            title=f"⚠️ Coverage gap: {report['total_gaps']} question(s) not forecasted"
        )
        print(f"  📬 Alert sent — {report['total_gaps']} REAL gap(s).")
    else:
        print(f"  ✅ No real gaps — {report['total_gated']} correctly gated, nothing to alert on.")


if __name__ == "__main__":
    run_coverage_check()