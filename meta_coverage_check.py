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

FIXED (2026-07-06): fetch_open_questions() used the unproven
ApiFilter(allowed_tournaments=...) mechanism for ALL 9 tournaments,
including the 5 QUESTION_SERIES_IDS tournaments (Nuclear Risk Horizons,
Current Events, Taiwan Tinderbox, Economic Indicators, Animal Welfare) —
confirmed broken for type='question_series' projects (returns
count≈7427, essentially the whole site, for Nuclear Risk Horizons
instead of the real ~37). meta_batch_forecast.py already proved the raw
`project=` parameter fetches these correctly (its own
fetch_question_series_questions()) and has been forecasting from it in
production since 2026-07-02. This file now uses the same mechanism via
the new fetch_open_questions_series() below — a lighter variant that
skips meta_batch_forecast.py's per-match BinaryQuestion round-trip (not
needed here; coverage only needs question_id/type/num_forecasters/
close_time, not a full forecastable object) and deliberately does NOT
pre-filter through passes_forecast_gate() the way that function does,
since coverage needs the FULL open set (gated and real alike) to
classify, not just the forecast-worthy subset.

unverified_fetch_scope is now False for these 5 tournaments in the
report. Each series' raw open-question count is still printed on every
run specifically so it can be eyeballed against Metaculus's own
tournament page (or check_project_type.py) a few times before fully
trusting it going forward — a proven mechanism used correctly is not
the same as this specific integration being battle-tested yet.
"""

import os
import json
import asyncio
import time
import requests
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

# Mirrors meta_batch_forecast.py's same-named constant deliberately — how
# many open questions to pull per series before any filtering. Generous
# since these series are individually small (Nuclear Risk Horizons was 37
# total via check_project_type.py).
QUESTION_SERIES_FETCH_LIMIT = 100


def fetch_open_questions_series(series_id: int) -> list[dict]:
    """Fetches open questions for a question_series-type project via the
    raw `project=` parameter — the same mechanism meta_batch_forecast.py's
    fetch_question_series_questions() already proved correct and has used
    in production since 2026-07-02 (see module docstring for the full
    history). Returns plain dicts with question_id/question_type/
    num_forecasters/close_time — deliberately lighter than
    meta_batch_forecast.py's version, which additionally fetches a full
    BinaryQuestion object per match via get_question_by_post_id(); that
    round-trip exists there because it needs a forecastable object, which
    coverage checking doesn't. Also deliberately does NOT pre-filter
    through passes_forecast_gate() — coverage needs the full open set
    (gated and real alike) so run_coverage_check() can classify each one,
    not just the forecast-worthy subset.

    question_id here is the NESTED question id (item.question.id), not
    the top-level post id (item.id) — the same post_id/question_id
    distinction this codebase is careful about everywhere else, since
    forecasted (built from q.id_of_question for the other 4 tournaments)
    is keyed by question_id too."""
    if not BOT_TOKEN:
        return []
    headers = {"Authorization": f"Token {BOT_TOKEN}"}
    try:
        r = requests.get(
            "https://www.metaculus.com/api2/questions/",
            headers=headers,
            params={"project": series_id, "status": "open", "limit": QUESTION_SERIES_FETCH_LIMIT},
            timeout=30,
        )
    except Exception as e:
        print(f"  ⚠️  question_series {series_id}: fetch failed ({e}) — treating as 0 open this run.")
        return []
    if r.status_code != 200:
        print(f"  ⚠️  question_series {series_id}: HTTP {r.status_code} — treating as 0 open this run.")
        return []

    raw_matches = (r.json() or {}).get("results") or []
    out = []
    extraction_failed_post_ids = []
    for item in raw_matches:
        # FIXED 2026-07-06: was `item.get("question", item) or {}` — when
        # the list-endpoint response for a given item has no "question"
        # sub-key at all (confirmed live: multiple_choice, date, and group
        # questions all hit this; both binary questions in the same
        # response extracted fine), this fell back to the top-level
        # `item` dict, whose "id" field is the POST id, not the nested
        # question id. That post_id then got used AS a question_id,
        # occasionally coinciding by pure numeric chance with an actually-
        # different question_id already in the "forecasted" set — a false
        # match that hid a genuine gap. Confirmed live 2026-07-06: exactly
        # this made 4 of 6 genuinely-unforecasted Current Events questions
        # invisible to this report, while Metaculus's own UI correctly
        # showed all 6 as not predicted. No more guessing: if the nested
        # question id can't be found, treat it as an extraction failure —
        # same "never let an extraction problem hide a genuine miss"
        # principle already used below for the other 4 tournaments — via
        # a synthetic "post:<id>" marker that can never collide with a
        # real integer question_id and will always show up as missing.
        q_info = item.get("question") or {}
        qid = q_info.get("id")
        if qid is None:
            post_id = item.get("id")
            extraction_failed_post_ids.append(post_id)
            out.append({
                "question_id": f"post:{post_id}",
                "question_type": None,
                "num_forecasters": None,
                "close_time": None,
            })
            continue
        close_str = item.get("scheduled_close_time") or q_info.get("scheduled_close_time")
        close_dt = None
        if close_str:
            try:
                close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            except Exception:
                pass
        out.append({
            "question_id": qid,
            "question_type": q_info.get("type"),
            "num_forecasters": item.get("nr_forecasters"),
            "close_time": close_dt,
        })
    if extraction_failed_post_ids:
        print(f"  ⚠️  question_series {series_id}: could not resolve a nested question_id for "
              f"{len(extraction_failed_post_ids)} post(s) (post_id(s): {extraction_failed_post_ids}) "
              f"— likely non-binary/group questions with a differently-shaped list response. "
              f"Flagged as extraction failures -> real gaps, not silently dropped.")
    print(f"  question_series {series_id}: {len(raw_matches)} open (raw project= fetch)")
    return out


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
    """Extract (question_type, num_forecasters, close_time). Two shapes
    handled: forecasting_tools question objects (the 4 real tournaments,
    via fetch_open_questions/ApiFilter — confirmed real, declared fields,
    checked directly against the installed library) and plain dicts (the
    5 question_series tournaments, via fetch_open_questions_series — see
    that function's docstring). Added dict support 2026-07-06 rather than
    forcing the question_series path to construct fake question objects
    just to satisfy this function's original object-only assumption."""
    if isinstance(q, dict):
        return (q.get("question_type"), q.get("num_forecasters"), q.get("close_time"))
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
        # CHANGED (2026-07-06): question_series tournaments (Nuclear Risk
        # Horizons, Current Events, Taiwan Tinderbox, Economic Indicators,
        # Animal Welfare) now go through fetch_open_questions_series()
        # (proven project= mechanism) instead of the broken
        # ApiFilter(allowed_tournaments=...) path — see module docstring.
        # Small courtesy delay between series requests, consistent with
        # this codebase's existing politeness-delay convention elsewhere.
        if tid in QUESTION_SERIES_IDS:
            series_items = fetch_open_questions_series(tid)
            open_qs = {d["question_id"] for d in series_items}
            by_id = {d["question_id"]: d for d in series_items}
            time.sleep(0.5)
        else:
            open_questions = fetch_open_questions(client, tid)
            open_qs = {q.id_of_question for q in open_questions}
            by_id = {q.id_of_question: q for q in open_questions}
        # sorted(..., key=str) — open_qs can now mix real int question_ids
        # with synthetic "post:<id>" strings (extraction-failure marker,
        # see fetch_open_questions_series) and plain sorted() crashes on
        # mixed int/str in Python 3.
        missing = sorted(open_qs - forecasted, key=str)

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
            "fetch_method": "project_param" if tid in QUESTION_SERIES_IDS else "allowed_tournaments",
            "unverified_fetch_scope": False,  # FIXED 2026-07-06 — see module docstring
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
            tournaments_with_real_gaps.append(f"{label} ({len(real_ids)})")

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
    series_labels = [TOURNAMENTS[tid] for tid in QUESTION_SERIES_IDS if tid in TOURNAMENTS]
    print(f"  ℹ️  {', '.join(series_labels)} now fetched via the proven project= mechanism "
          f"(same one meta_batch_forecast.py forecasts from) instead of the broken "
          f"ApiFilter(allowed_tournaments=...) path — per-series open counts are printed "
          f"above as they're fetched. Worth eyeballing those against Metaculus's own "
          f"tournament pages a few times before treating this integration as fully proven.")
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