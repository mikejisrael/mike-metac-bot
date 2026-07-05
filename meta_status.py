"""
meta_status.py — Status dashboard for the Metaculus forecasting bot.

Usage:
  python meta_status.py

CHANGED (2026-07-03): file registry and QUICK COMMANDS updated to reflect
the day's work — new shared modules (meta_forecast_gate.py, tracking the
binary/forecaster-count/close-time eligibility gate meta_batch_forecast.py
uses; meta_refresh_gate.py, tracking the minimum-refresh-gap gate
meta_refresh_forecast.py and meta_watch.py both use), the Phase 0
measurement scripts (meta_coverage_check.py, meta_calibration_report.py),
meta_watch.py (push-notification alerting), meta_backfill_page_urls.py
(occasional-use history backfill utility), tournament_forecast.py (the
FutureEval-only synchronous path — was missing from this registry
entirely despite being the most protected, prize-eligible pipeline), and
the shared helper modules several of the above import (meta_alerts.py,
meta_research.py, meta_prompt_cache.py, meta_cp_extract.py,
meta_question_matching.py). Also added a PHASE 0 REPORTS section reading
reports/coverage_latest.json and reports/calibration_latest.json, so this
dashboard actually reflects the real gap/calibration numbers those
scripts now produce, instead of a permanent "Brier score: n/a" placeholder.

FIXED (2026-07-03): BATCH_DIR was "Meta batches" (capital M) — the same
Linux/GitHub-Actions case-sensitivity bug already fixed today in
meta_batch_forecast.py, meta_backfill_page_urls.py, and meta_watch.py.
This was the last file in the codebase still pointing at the capital-M
folder; harmless on Windows (case-insensitive filesystem) but would
silently show an empty dashboard if this script ever ran on Linux.
"""

import json
import glob
import os
from datetime import datetime, timezone, timedelta

BATCH_DIR = "meta batches"

# ─── File registry ─────────────────────────────────────────────────────────────
METACULUS_FILES = [
    ("tournament_forecast.py",      "FutureEval ONLY — synchronous, prize-eligible, most protected"),
    ("meta_batch_forecast.py",      "Main batch forecasting (ACX2026/Climate/Metaculus Cup + 5 question-series)"),
    ("meta_refresh_forecast.py",    "Re-forecast closing-soon / stale questions (binary + MC, --single)"),
    ("meta_forecast_gate.py",       "Shared: is-this-question-worth-forecasting gate (type/forecasters/close-time)"),
    ("meta_refresh_gate.py",        "Shared: minimum-refresh-gap gate (used by refresh + watch alerting)"),
    ("meta_coverage_check.py",      "Phase 0: tournament coverage gaps (real vs. correctly-gated)"),
    ("meta_calibration_report.py",  "Phase 0: calibration curve + peer-score report"),
    ("meta_watch.py",               "Push notifications — new questions, resolutions, refresh candidates"),
    ("meta_backfill_page_urls.py",  "Occasional-use: backfill page_url/post_id into old local history"),
    ("meta_question_matching.py",   "Shared: titles_match() — guards against recycled Metaculus IDs"),
    ("meta_alerts.py",              "Shared: send_alert() (ntfy push notifications)"),
    ("meta_research.py",            "Shared: research_question() (real-time web search grounding)"),
    ("meta_prompt_cache.py",        "Shared: cacheable_system_block() (Anthropic prompt caching)"),
    ("meta_cp_extract.py",          "Shared: extract_live_cp() (community prediction parsing)"),
    ("export_forecasts.py",         "Export forecasts to CSV"),
    ("show_reasoning.py",           "Display bot reasoning for a question ID"),
    ("meta_status.py",              "This status dashboard"),
    ("meta_dashboard.py",           "Streamlit web dashboard (personal + bot account tracking)"),
    ("live_data.py",                "Live data fetcher (VIX, BTC, FRED, etc.)"),
    ("cached_llm.py",               "System prompt builder"),
    ("analyse_reports.py",          "Report analysis script"),
]

METACULUS_BATCH_FILES = [
    (os.path.join(BATCH_DIR, "batch_jobs.json"),             "Latest batch job info (pointer)"),
    (os.path.join(BATCH_DIR, "batch_jobs_2*.json"),          "Timestamped batch job history"),
    (os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"),   "Refresh batch job history"),
    (os.path.join(BATCH_DIR, "batch_results.json"),          "Latest batch results (pointer)"),
    (os.path.join(BATCH_DIR, "batch_results_2*.json"),       "Timestamped batch results history"),
    (os.path.join(BATCH_DIR, "batch_results_refresh_*.json"),"Refresh batch results history"),
]

IBKR_FILES = [
    ("ibkr*.py",    "IBKR bot scripts"),
    ("ib_*.py",     "IBKR connection scripts"),
    ("trade*.py",   "IBKR trade scripts"),
    ("ibkr*.json",  "IBKR data files"),
]

# Added 2026-07-03, alongside the Phase 0 fixes to meta_coverage_check.py
# and meta_calibration_report.py — surfaces the actual report files, not
# just the generic "reports/" folder entry OTHER_FILES already had.
PHASE0_REPORT_FILES = [
    (os.path.join("reports", "coverage_latest.json"),    "Phase 0: tournament coverage (real vs. gated gaps)"),
    (os.path.join("reports", "coverage_2*.json"),        "Timestamped coverage history"),
    (os.path.join("reports", "calibration_latest.json"), "Phase 0: calibration curve + peer scores"),
    (os.path.join("reports", "calibration_2*.json"),     "Timestamped calibration history"),
]

OTHER_FILES = [
    (".env",              "Environment variables (API keys)"),
    ("requirements*.txt", "Python dependencies"),
    ("*.md",              "Documentation"),
    ("reports/",          "Generated reports folder"),
    (BATCH_DIR + "/",     "Batch data folder"),
]


# ─── Helpers ───────────────────────────────────────────────────────────────────
def find_files(pattern):
    return sorted(glob.glob(pattern))

def file_size(path):
    try:
        size = os.path.getsize(path)
        if size < 1024:      return f"{size}B"
        elif size < 1048576: return f"{size//1024}KB"
        else:                return f"{size//1048576}MB"
    except Exception:
        return "?"

def file_mtime(path):
    try:
        return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "?"


# ─── File group printer ────────────────────────────────────────────────────────
def print_file_group(title, file_specs, show_missing=False):
    print(f"\n{'─'*70}")
    print(f"  {title}")
    print(f"{'─'*70}")
    found_any = False
    for pattern, description in file_specs:
        matches = find_files(pattern)
        if matches:
            found_any = True
            if len(matches) == 1:
                f = matches[0]
                print(f"  ✅ {f:<42} {file_size(f):>5}  {file_mtime(f)}  {description}")
            else:
                total = sum(os.path.getsize(f) for f in matches if os.path.exists(f))
                sz = f"{total//1024}KB" if total > 1024 else f"{total}B"
                print(f"  ✅ {pattern:<42} {sz:>5}  ({len(matches)} files)  {description}")
                for f in matches[-3:]:
                    print(f"       └─ {f:<40} {file_size(f):>5}  {file_mtime(f)}")
                if len(matches) > 3:
                    print(f"       └─ ... and {len(matches)-3} more")
        elif show_missing:
            print(f"  ⬜ {pattern:<42}        (not found)  {description}")
    if not found_any and not show_missing:
        print("  (no files found)")


# ─── Match results to job files by question ID overlap ────────────────────────
def build_results_map():
    results_by_ids = {}
    for rf in (glob.glob(os.path.join(BATCH_DIR, "batch_results_2*.json")) +
               glob.glob(os.path.join(BATCH_DIR, "batch_results_refresh_*.json"))):
        try:
            with open(rf) as f:
                data = json.load(f)
            q_ids = frozenset(r.get("question_id") for r in data.values() if r.get("question_id"))
            if q_ids:
                results_by_ids[q_ids] = rf
        except Exception:
            pass

    mapping = {}
    for jf in (glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
               glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))):
        try:
            with open(jf) as f:
                data = json.load(f)
            q_ids = frozenset(data.get("question_ids", {}).values())
            mapping[jf] = results_by_ids.get(q_ids)
        except Exception:
            mapping[jf] = None
    return mapping


# ─── Batch history ─────────────────────────────────────────────────────────────
def load_batch_history():
    results_map = build_results_map()
    batches = []
    for jf in sorted(results_map.keys()):
        try:
            with open(jf) as f:
                data = json.load(f)
            rf = results_map[jf]
            result_count = success_count = 0
            if rf:
                with open(rf) as f:
                    results = json.load(f)
                result_count = len(results)
                success_count = sum(1 for r in results.values() if r.get("status") == "success")
            batches.append({
                "file":          os.path.basename(jf),
                "submitted_at":  data.get("submitted_at", "")[:16].replace("T", " "),
                "batch_type":    data.get("batch_type", "main"),
                "num_requests":  data.get("num_requests", 0),
                "success_count": success_count,
                "result_count":  result_count,
                "has_results":   rf is not None,
            })
        except Exception:
            pass
    return batches


# ─── Total forecast count ──────────────────────────────────────────────────────
def count_total_forecasts() -> int:
    seen = set()
    for rf in glob.glob(os.path.join(BATCH_DIR, "batch_results*.json")):
        try:
            with open(rf) as f:
                results = json.load(f)
            for r in results.values():
                if r.get("status") == "success" and r.get("question_id"):
                    seen.add(r["question_id"])
        except Exception:
            pass
    return len(seen)


# ─── Phase 0 report summaries ──────────────────────────────────────────────────
# Added 2026-07-03 alongside today's meta_coverage_check.py / meta_calibration_
# report.py fixes — reads whatever those scripts most recently wrote, so this
# dashboard reflects real numbers instead of the permanent "Brier score: n/a"
# placeholder that used to sit here regardless of how much real data existed.
def _format_checked_at(iso_str: str) -> str:
    """meta_coverage_check.py and meta_calibration_report.py both write
    checked_at using datetime.now(timezone.utc), but everything else in
    this dashboard (batch submitted_at, the header timestamp) is naive
    local time — side by side, UTC checked_at looked several hours stale
    even when it wasn't. Converts to local time before display."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone()  # UTC -> local system timezone
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso_str[:16].replace("T", " ")  # fallback: old naive behavior


def load_coverage_summary():
    path = os.path.join("reports", "coverage_latest.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        return {
            "checked_at":  _format_checked_at(data.get("checked_at", "")),
            # "total_gaps" here means REAL gaps specifically (not total
            # missing questions) — see meta_coverage_check.py's module
            # docstring for the 2026-07-03 gate-classification change.
            "total_gaps":  data.get("total_gaps", 0),
            "total_gated": data.get("total_gated", 0),
        }
    except Exception:
        return None


def load_calibration_summary():
    path = os.path.join("reports", "calibration_latest.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        return {
            "checked_at":         _format_checked_at(data.get("checked_at", "")),
            "questions_scored":   data.get("questions_scored", 0),
            "average_peer_score": data.get("average_peer_score"),
        }
    except Exception:
        return None


# ─── Closing soon ──────────────────────────────────────────────────────────────
def load_closing_soon(days=14):
    now = datetime.now(timezone.utc)
    closing = []
    seen = set()
    results_map = build_results_map()

    all_probs = {}
    for rf in results_map.values():
        if not rf:
            continue
        try:
            with open(rf) as f:
                results = json.load(f)
            for r in results.values():
                qid = r.get("question_id")
                if qid and r.get("probability") is not None:
                    all_probs[qid] = r["probability"]
        except Exception:
            pass

    for jf in sorted(results_map.keys(), reverse=True):
        try:
            with open(jf) as f:
                data = json.load(f)
            resolve_times  = data.get("resolve_times", {})
            question_texts = data.get("question_texts", {})
            question_ids   = data.get("question_ids", {})

            for custom_id, resolve_str in resolve_times.items():
                if not resolve_str:
                    continue
                q_id = question_ids.get(custom_id)
                if q_id in seen:
                    continue
                try:
                    resolve_time = datetime.fromisoformat(resolve_str.replace("Z", "+00:00"))
                    days_left = (resolve_time - now).days
                    if 0 <= days_left <= days:
                        seen.add(q_id)
                        closing.append({
                            "question_id":   q_id,
                            "question_text": question_texts.get(custom_id, "")[:65],
                            "days_left":     days_left,
                            "probability":   all_probs.get(q_id),
                            "resolve_time":  resolve_str[:10],
                        })
                except Exception:
                    pass
        except Exception:
            pass

    return sorted(closing, key=lambda x: x["days_left"])


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 70)
    print("  METACULUS BOT — STATUS DASHBOARD")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    print_file_group("METACULUS BOT SCRIPTS", METACULUS_FILES)
    print_file_group(f"METACULUS BATCH DATA ({BATCH_DIR}/)", METACULUS_BATCH_FILES)
    print_file_group("PHASE 0 REPORTS", PHASE0_REPORT_FILES)
    print_file_group("IBKR BOT FILES", IBKR_FILES)
    print_file_group("PROJECT FILES", OTHER_FILES)

    # Batch history
    print(f"\n{'─'*70}")
    print(f"  BATCH HISTORY")
    print(f"{'─'*70}")
    batches = load_batch_history()
    if not batches:
        print(f"  No batch history found in {BATCH_DIR}/")
    else:
        print(f"  {'File':<44} {'Submitted':<17} {'Type':<8} {'Req':>4} {'Done':>6}")
        print(f"  {'-'*44} {'-'*17} {'-'*8} {'-'*4} {'-'*6}")
        for b in batches:
            done = f"{b['success_count']}/{b['num_requests']}" if b['has_results'] else "pending"
            print(f"  {b['file']:<44} {b['submitted_at']:<17} {b['batch_type']:<8} {b['num_requests']:>4} {done:>6}")

    # Summary
    print(f"\n{'─'*70}")
    print(f"  SUMMARY")
    print(f"{'─'*70}")
    total           = count_total_forecasts()
    main_batches    = len([b for b in batches if b['batch_type'] == 'main'])
    refresh_batches = len([b for b in batches if b['batch_type'] == 'refresh'])
    pending         = sum(1 for b in batches if not b['has_results'])
    print(f"  Total unique forecasts submitted:  {total}")
    print(f"  Main batches:                      {main_batches}")
    print(f"  Refresh batches:                   {refresh_batches}")
    print(f"  Batches pending results:           {pending}")

    # CHANGED (2026-07-03): was a permanent "Brier score: n/a (waiting for
    # resolutions)" placeholder regardless of how much real data existed.
    # Now reads meta_coverage_check.py / meta_calibration_report.py's
    # actual output, once those scripts have run — both were fixed today
    # (nested-status detection, nested peer_score extraction, and the
    # real-vs-gated gap classification), so this now reflects real numbers.
    cov = load_coverage_summary()
    if cov is not None:
        print(f"  Coverage (as of {cov['checked_at']}):        "
              f"{cov['total_gaps']} real gap(s), {cov['total_gated']} correctly gated")
    else:
        print(f"  Coverage:                          no report yet — run meta_coverage_check.py")

    cal = load_calibration_summary()
    if cal is not None and cal['questions_scored']:
        print(f"  Calibration (as of {cal['checked_at']}):     "
              f"{cal['questions_scored']} scored, avg peer score {cal['average_peer_score']:.2f}")
    else:
        print(f"  Calibration:                       no scored questions yet")

    # Closing soon
    closing = load_closing_soon(days=14)
    print(f"\n{'─'*70}")
    print(f"  CLOSING SOON (next 14 days) — {len(closing)} questions")
    print(f"{'─'*70}")
    if not closing:
        print("  No questions closing in the next 14 days.")
    else:
        for q in closing:
            prob_str = f"{q['probability']:.0%}" if q['probability'] is not None else " n/a"
            print(f"  [{q['days_left']:>2}d] {prob_str:>4}  Q{q['question_id']}  {q['question_text']}")
        print(f"\n  ⚡ Run: python meta_refresh_forecast.py --submit  to refresh these")
        # Added 2026-07-03: this list is purely resolve-time based and
        # doesn't know about meta_refresh_forecast.py's own minimum-
        # refresh-gap gate (meta_refresh_gate.py, 8 days) — a question
        # refreshed a few hours ago will still show up here even though
        # --submit would correctly skip it as "refreshed too recently."
        # This view answers "what's closing soon," not "what --submit
        # will actually attempt" — run meta_refresh_forecast.py itself
        # (no args) for the gate-aware picture.
        print(f"     (this list doesn't account for the refresh-gap gate — "
              f"a just-refreshed question may still appear here but get "
              f"skipped by --submit; run meta_refresh_forecast.py for the "
              f"gate-aware view)")

    # Quick commands
    print(f"\n{'─'*70}")
    print(f"  QUICK COMMANDS")
    print(f"{'─'*70}")
    # FIXED 2026-07-03: was "submit new batch of 50" — NUM_QUESTIONS was
    # dropped from 50 to 20 on 2026-07-02 as a deliberate cost control
    # when meta_batch_forecast.py's tournament count expanded from 3 to 8.
    print(f"  python meta_batch_forecast.py              # submit new batch (up to 20 questions)")
    print(f"  python meta_batch_forecast.py --check      # retrieve completed batch")
    print(f"  python meta_refresh_forecast.py           # dry run — see what needs refresh")
    print(f"  python meta_refresh_forecast.py --submit  # submit refresh batch (closing-soon uncapped, stale capped at 10/run)")
    print(f"  python meta_refresh_forecast.py --check   # retrieve refresh results")
    print(f"  python meta_refresh_forecast.py --single  # refresh one question now, by URL/post ID (binary or MC)")
    print(f"  python meta_coverage_check.py              # Phase 0: tournament coverage gaps (real vs. gated)")
    print(f"  python meta_calibration_report.py          # Phase 0: calibration curve + peer scores")
    print(f"  python meta_watch.py                       # push-notification check (new questions, resolutions, refresh candidates)")
    print(f"  python show_reasoning.py <id>         # show bot reasoning for question")
    print(f"  python meta_status.py                 # this dashboard")
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()