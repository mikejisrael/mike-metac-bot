"""
meta_dashboard.py — Forecasting track-record dashboard (Metaculus side).

v3 — personal account merged back in; detail page; CP note; two-dataset chart.

ACCOUNTS
  Bot   (mike_iz_-bot)  — METAC_TOURNAMENT_TOKEN — primary; appears in table + chart
  Personal (mike_iz_)   — METACULUS_TOKEN        — merged into table with "Personal"
                          tournament tag; separate dataset on chart

  If a question_id exists in BOTH accounts, bot data wins in the table row.
  Personal-only questions appear once with "Personal" tournament tag.

TOURNAMENT SPLIT — derived from live API projects field:
    33022 -> FutureEval
    32880 -> ACX2026
     1756 -> Climate Tipping Points
    33021 -> Metaculus Cup
    "Personal" -> personal account only (no bot prediction on record)
    "Other"    -> has live data but no matching tournament ID above
    "Unknown"  -> local result only, no live match at all

STATUS BUCKETS
  open / closed_unresolved / resolved_scored / resolved_unscored / not_found_live

CP NOTE — most questions return null CP because include_bots_in_aggregates=false
  AND aggregations.recency_weighted.latest=null. This is a Metaculus API limitation
  (bots excluded from community aggregates on most tournaments), not a code bug.

DETAIL PAGE — /detail/<id> shows formatted question details, reasoning, and research
  text from local results JSON alongside live API scores. Raw JSON still accessible
  as a collapsible section at the bottom.

CACHE — background thread refreshes every 5 minutes; page loads read cache instantly.

Run:
  python meta_dashboard.py
Then open http://localhost:5002
"""

import os
import re
import sys
import glob
import json
import asyncio
import threading
import time
import subprocess
import requests
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string, request
from dotenv import load_dotenv
from forecasting_tools import MetaculusClient, ApiFilter
from meta_cp_extract import extract_live_cp
from meta_refresh_exclusions import load_excluded_ids
import meta_refresh_schedule
import tournament_registry

load_dotenv()

app = Flask(__name__)

# ─── Accounts ────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("METAC_TOURNAMENT_TOKEN")
PERSONAL_TOKEN = os.getenv("METACULUS_TOKEN")

bot_client      = MetaculusClient(token=BOT_TOKEN)      if BOT_TOKEN      else None
personal_client = MetaculusClient(token=PERSONAL_TOKEN) if PERSONAL_TOKEN else None

print(f"Bot client:      {'ready' if bot_client      else '⚠️  METAC_TOURNAMENT_TOKEN not set'}")
print(f"Personal client: {'ready' if personal_client else '⚠️  METACULUS_TOKEN not set'}")

LOCAL_RESULT_DIRS = ["tournament_batches", "tournament_batches_v2", "meta batches"]

# CHANGED 2026-07-16: now derived from tournament_registry.py instead of a
# hand-written dict. The old version here only had 5 entries (FutureEval,
# ACX2026, Climate, Metaculus Cup, Market Pulse) and was silently missing
# all 5 question_series tournaments (Nuclear Risk Horizons, Current
# Events, Taiwan Tinderbox, Economic Indicators, Animal Welfare) that
# meta_coverage_check.py's separate TOURNAMENTS dict already had — those
# forecasts were falling into the OTHER_LABEL bucket below. See
# tournament_registry.py's module docstring for the full history.
TOURNAMENT_LABELS = tournament_registry.labels_by_id()
TOURNAMENT_CATEGORIES = tournament_registry.category_by_id()  # id -> "Tournaments"/"Question Series", not yet surfaced in the UI (nested filter UI is a later step)
PERSONAL_LABEL = "Personal"
OTHER_LABEL    = "Other"
UNKNOWN_LABEL  = "Unknown"
TOURNAMENT_ORDER = tournament_registry.display_order() + [
    PERSONAL_LABEL, OTHER_LABEL, UNKNOWN_LABEL,
]
# ADDED 2026-07-16: which display labels belong under the "Question Series"
# filter heading, split out from the main "Tournament" heading (Mike's
# call — the flat pill row was lumping FutureEval/ACX2026/etc. together
# with Nuclear Risk Horizons/Current Events/etc., with no visual
# distinction now that the latter finally show up post-detect_tournaments
# fix). Registry-derived so a new question_series entry gets its own
# heading automatically, no template change needed.
QUESTION_SERIES_LABELS = {
    v["display_name"] for v in tournament_registry.TOURNAMENTS.values()
    if v["category"] == "Question Series"
}

STATUS_LABELS = {
    "open":              "Open",
    "closed_unresolved": "Closed",
    "resolved_scored":   "Resolved & scored",
    "resolved_unscored": "Resolved, no score",
    "not_found_live":    "Withdrawn",
}
STATUS_ORDER = ["open", "closed_unresolved", "resolved_scored", "resolved_unscored", "not_found_live"]

REFRESH_INTERVAL_SECONDS = 300

CACHE: dict = {
    "data": None,
    "live_by_qid": {},       # qid -> live API JSON (bot account)
    "personal_live_by_qid": {},  # qid -> live API JSON (personal account)
    "local_by_qid": {},      # qid -> best local result record
    "last_refresh": None,
    "error": None,
}
CACHE_LOCK = threading.Lock()


# ─── Helpers ─────────────────────────────────────────────────────────────────
def get_confirmed_user_id(client) -> int | None:
    if client is None:
        return None
    try:
        return client.get_current_user_id()
    except Exception as e:
        print(f"  get_confirmed_user_id failed: {e}")
        return None


def _parse_result_filename_timestamp(source_file: str):
    """Parses the YYYYMMDD_HHMM timestamp embedded in
    batch_results_YYYYMMDD_HHMM.json filenames (UTC) — same convention
    meta_watch.py's _forecast_age_days and show_reasoning.py rely on.
    Returns a UTC datetime, or None if the filename doesn't match."""
    import re
    m = re.search(r"batch_results_(?:refresh_)?(\d{8})_(\d{4})", os.path.basename(source_file or ""))
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def load_local_results(dirs: list[str]) -> dict[int, dict]:
    """Merge every batch_results_*.json, keyed by question_id.
    Most-recent-forecast file wins on conflict.

    CHANGED (2026-07-08): "most recent" now means the timestamp embedded
    in the filename (via _parse_result_filename_timestamp), not raw
    filesystem mtime. Found live (Q43615/Shakira, dashboard showing "last
    predicted June 29" when the real most recent refresh was July 3):
    these result files are pulled down via git checkout/pull, and git
    does NOT preserve original commit timestamps — files get stamped with
    local checkout/pull time, not the time they were actually written by
    the pipeline. So two files' mtimes can come out in an order that has
    nothing to do with which forecast is actually newer, especially after
    a fresh clone or pull where many files land within the same second.
    _parse_result_filename_timestamp already existed and was already used
    for the *display* date below — this was the one place still trusting
    mtime for the *selection* itself, which is the more consequential of
    the two (a wrong winner silently drops a real, newer forecast, not
    just mislabels its date). mtime is now only a fallback for files whose
    name doesn't match the batch_results_(refresh_)?YYYYMMDD_HHMM pattern
    (e.g. an old manually-renamed file) — same fallback predicted_at
    already used, now applied consistently in both places.

    Added 2026-07-05: each winning record is stamped with "_source_file"
    and "_source_mtime" so the dashboard can show a "last predicted"
    date/sort column."""
    by_qid: dict[int, dict] = {}
    by_qid_sort_key: dict[int, float] = {}
    for d in dirs:
        for rf in glob.glob(os.path.join(d, "batch_results_*.json")):
            try:
                mtime = os.path.getmtime(rf)
                name_ts = _parse_result_filename_timestamp(rf)
                # Prefer the filename's own timestamp for deciding the
                # winner — falls back to mtime only when the filename
                # doesn't parse, same fallback predicted_at uses below.
                sort_key = name_ts.timestamp() if name_ts is not None else mtime
                with open(rf, encoding="utf-8") as f:
                    data = json.load(f)
                for r in data.values():
                    qid = r.get("question_id")
                    if qid is None:
                        continue
                    if qid not in by_qid or sort_key > by_qid_sort_key[qid]:
                        r["_source_file"] = rf
                        r["_source_mtime"] = mtime
                        by_qid[qid] = r
                        by_qid_sort_key[qid] = sort_key
            except Exception as e:
                print(f"  (skipping unreadable file {rf}: {e})")
    return by_qid


def load_prediction_history(qid: int, dirs: list[str]) -> list[dict]:
    """Scans every batch_results_*.json across dirs (not just the winning
    one load_local_results picked) for every record on this question_id,
    for the detail page's prediction-history section. Returns a list of
    {date_iso, submitted_summary, question_type, source_file}, most
    recent first. A question refreshed 5 times has 5 entries here — one
    per batch_results file that contains a successful result for it."""
    entries = []
    for d in dirs:
        for rf in glob.glob(os.path.join(d, "batch_results_*.json")):
            try:
                with open(rf, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            for r in data.values():
                if r.get("question_id") != qid:
                    continue
                if r.get("status") not in (None, "success"):
                    continue
                ts = _parse_result_filename_timestamp(rf)
                if ts is None:
                    try:
                        ts = datetime.fromtimestamp(os.path.getmtime(rf), tz=timezone.utc)
                    except Exception:
                        ts = None
                q_type = r.get("question_type") or "binary"
                forecast = r.get("submitted_forecast") or r.get("probability") or r.get("probabilities")
                entries.append({
                    "date_iso": ts.isoformat() if ts else None,
                    "_sort_ts": ts or datetime.min.replace(tzinfo=timezone.utc),
                    "submitted_summary": summarize_forecast(q_type, forecast),
                    "question_type": q_type,
                    "source_file": os.path.basename(rf),
                })
    entries.sort(key=lambda e: e["_sort_ts"], reverse=True)
    for e in entries:
        del e["_sort_ts"]
    return entries


def load_phase0_reports() -> dict:
    """Reads the latest coverage/calibration reports written by
    meta_coverage_check.py and meta_calibration_report.py (Phase 0).
    Purely additive — returns None-safe defaults if those scripts haven't
    been run yet, so the dashboard never breaks on a missing reports/
    folder."""
    coverage = None
    calibration = None
    try:
        with open(os.path.join("reports", "coverage_latest.json")) as f:
            coverage = json.load(f)
    except Exception:
        pass
    try:
        with open(os.path.join("reports", "calibration_latest.json")) as f:
            calibration = json.load(f)
    except Exception:
        pass
    return {"coverage": coverage, "calibration": calibration}


def load_refresh_candidate_state() -> dict:
    """Reads watch_state/refresh_candidate_state.json, written by
    meta_watch.py's check_refresh_candidates() (Phase 1). Keyed by
    question_id (string) -> {alerted_at, reasons}. Read-only, same
    None-safe-default pattern as load_phase0_reports — the dashboard
    should never break if meta_watch.py hasn't run yet."""
    try:
        with open(os.path.join("watch_state", "refresh_candidate_state.json")) as f:
            return json.load(f)
    except Exception:
        return {}


def load_openrouter_balance() -> dict | None:
    """Live credit balance for the OpenRouter key, via the same endpoint
    Mike checks manually (GET /api/v1/key). Called once per cache refresh
    cycle (every REFRESH_INTERVAL_SECONDS), not per page load, since it's
    a live network call. Returns None on any failure or missing key so
    the dashboard degrades gracefully — same pattern as load_phase0_reports.
    Anthropic-side balance intentionally NOT shown here: no plain-balance
    endpoint exists for a standard (non-Admin) API key — Mike's call
    2026-07-04 was to skip it rather than provision an Admin key just for
    this. Revisit if that changes."""
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return None
    try:
        r = requests.get(
            "https://openrouter.ai/api/v1/key",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("data", {})
        limit = d.get("limit")
        remaining = d.get("limit_remaining")
        return {
            "limit": limit,
            "remaining": remaining,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"  ⚠️  OpenRouter balance check failed (non-fatal): {e}")
        return None


def fetch_predicted_questions(client, label: str) -> dict[int, dict]:
    """All questions a client's account has predicted on, keyed by question_id."""
    if client is None:
        return {}
    by_qid: dict[int, dict] = {}

    def _run(api_filter):
        try:
            return asyncio.run(
                client.get_questions_matching_filter(
                    api_filter, num_questions=1000, error_if_question_target_missed=False
                )
            )
        except Exception as e:
            print(f"  fetch_predicted_questions [{label}] pass failed: {e}")
            return []

    # FIXED (2026-07-12): both ApiFilter calls previously had no
    # group_question_mode, which defaults to "exclude" — confirmed via
    # forecasting_tools source inspection (2026-07-10 session). This
    # silently dropped every Market Pulse group_of_questions sub-question
    # from bot_live entirely, which would have made every Market Pulse row
    # show live_match_found=False -> status "Withdrawn", despite being
    # genuinely open and forecast. "unpack_subquestions" makes each
    # sub-question come back as its own normal object, same as the fetch
    # fix already applied in tournament_forecast_v2.py.
    for q in _run(ApiFilter(is_previously_forecasted_by_user=True,
                             group_question_mode="unpack_subquestions")):
        by_qid[q.id_of_question] = q.api_json
    for q in _run(ApiFilter(is_previously_forecasted_by_user=True, allowed_statuses=["resolved"],
                             group_question_mode="unpack_subquestions")):
        by_qid.setdefault(q.id_of_question, q.api_json)

    print(f"  fetch_predicted_questions [{label}]: {len(by_qid)} unique questions")
    return by_qid


def extract_score_info(raw: dict) -> dict:
    info = {
        "resolved": False, "resolution": None, "peer_score": None,
        "baseline_score": None, "close_time": None, "title": None,
        "api_status": None, "resolve_time": None, "cp_available": False,
    }
    if not raw or "_error" in raw:
        return info

    q = raw.get("question", raw)
    info["title"]        = raw.get("title") or q.get("title")
    info["close_time"]   = raw.get("scheduled_close_time") or q.get("scheduled_close_time")
    info["api_status"]   = raw.get("status") or q.get("status")
    info["resolve_time"] = (
        raw.get("actual_resolve_time") or q.get("actual_resolve_time")
        or q.get("resolution_set_time") or q.get("scheduled_resolve_time")
        or raw.get("scheduled_resolve_time")
    )

    # CP availability flag — False when include_bots_in_aggregates=False or latest=null
    agg = q.get("aggregations", {}) or {}
    node = agg.get("recency_weighted") or agg.get("metaculus_prediction") or {}
    info["cp_available"] = (
        bool(q.get("include_bots_in_aggregates")) and
        node.get("latest") is not None
    )

    if "resolved" in raw:
        info["resolved"] = bool(raw["resolved"])
    else:
        info["resolved"] = q.get("resolution") is not None
    info["resolution"] = q.get("resolution")

    for path in [
        ("my_forecasts", "score_data", "peer_score"),
        ("my_forecasts", "latest", "score_data", "peer_score"),
        ("my_forecasts", "latest", "peer_score"),
        ("scoring", "peer_score"),
        ("score_data", "peer_score"),
    ]:
        val = q
        try:
            for key in path:
                val = val[key]
            if val is not None:
                info["peer_score"] = val
                break
        except (KeyError, TypeError):
            continue

    for path in [
        ("my_forecasts", "score_data", "baseline_score"),
        ("my_forecasts", "latest", "score_data", "baseline_score"),
        ("scoring", "baseline_score"),
        ("score_data", "baseline_score"),
    ]:
        val = q
        try:
            for key in path:
                val = val[key]
            if val is not None:
                info["baseline_score"] = val
                break
        except (KeyError, TypeError):
            continue

    return info


def extract_submitted_forecast(raw: dict, q_type: str):
    """Fallback source for 'Submitted' when no local batch-result record has
    it (e.g. FutureEval questions run through tournament_forecast.py's
    synchronous path, which may not always write a matching local file).
    Reads straight from the live API's question.my_forecasts.latest.

    Binary: forecast_values is [P(No), P(Yes)] -> return P(Yes) as a scalar,
    matching what summarize_forecast() expects for q_type == "binary".
    Multiple choice: zip forecast_values against options into a dict.
    Numeric: pass the raw forecast_values list through (summarize_forecast
    already knows how to handle a numeric CDF-style list).
    """
    if not raw:
        return None
    q = raw.get("question", raw)
    latest = ((q.get("my_forecasts") or {}).get("latest")) or {}
    values = latest.get("forecast_values")
    if values is None:
        return None
    try:
        if q_type == "binary":
            if isinstance(values, list) and len(values) >= 2:
                return values[1]
            return values
        if q_type == "multiple_choice":
            options = q.get("options") or []
            if isinstance(values, list) and options and len(values) == len(options):
                return dict(zip(options, values))
            return values
        # numeric (and anything else): hand the raw list/value through
        return values
    except Exception:
        return None


def summarize_forecast(q_type: str, forecast) -> str:
    if forecast is None:
        return "—"
    try:
        if q_type == "binary":
            return f"{forecast:.0%}"
        if q_type == "numeric" and isinstance(forecast, list):
            idx = next((i for i, v in enumerate(forecast) if v >= 0.5), len(forecast) // 2)
            frac = idx / (len(forecast) - 1) if len(forecast) > 1 else 0.5
            return f"median≈{frac:.0%} through range"
        if q_type == "numeric" and isinstance(forecast, (int, float)):
            return f"median≈{forecast:,.0f}"
        if q_type == "multiple_choice" and isinstance(forecast, dict):
            top = max(forecast, key=forecast.get)
            return f"{top} ({forecast[top]:.0%})"
    except Exception:
        pass
    return str(forecast)[:60]


def detect_tournaments(raw: dict) -> list[str]:
    """FIXED 2026-07-16: this only ever read projects["tournament"] (plus
    default_project when its type=="tournament") — it never checked
    projects["question_series"] at all. A question that's ONLY on a
    question_series project (Nuclear Risk Horizons, Current Events,
    Taiwan Tinderbox, Economic Indicators, Animal Welfare) therefore
    produced an EMPTY ids set here, falling through to the `or
    [OTHER_LABEL]` fallback at both call sites — landing in "Other" not
    because the label was unmapped, but because this function never even
    looked for it. Fixed alongside the TOURNAMENT_LABELS drift fix (see
    that dict's comment) since patching the label map alone would not
    have changed this function's behavior at all."""
    if not raw:
        return []
    projects = raw.get("projects", {}) or {}
    ids = set()
    for key in ("tournament", "question_series"):
        for t in projects.get(key, []) or []:
            tid = t.get("id")
            if tid is not None:
                ids.add(tid)
    dp = projects.get("default_project")
    if dp and dp.get("type") in ("tournament", "question_series") and dp.get("id") is not None:
        ids.add(dp["id"])
    if not ids:
        return []
    return sorted({TOURNAMENT_LABELS.get(tid, OTHER_LABEL) for tid in ids})


def classify_status(row: dict) -> str:
    if not row["live_match_found"]:
        return "not_found_live"
    if row["resolved"]:
        return "resolved_scored" if row["peer_score"] is not None else "resolved_unscored"
    if (row["api_status"] or "").lower() in ("closed", "pending_resolution"):
        return "closed_unresolved"
    return "open"


def _make_row(qid, local_r, post, is_personal_only=False, refresh_state: dict | None = None,
              excluded_ids: dict | None = None, refresh_overrides: dict | None = None) -> dict:
    score  = extract_score_info(post) if post else extract_score_info(None)
    q_type = (local_r or {}).get("question_type") or (post.get("question") or {}).get("type") if post else None
    cp_val = extract_live_cp(post, q_type) if post else None
    tournaments = detect_tournaments(post) if post else []

    # Last-predicted date: prefer the filename-embedded timestamp (matches
    # meta_watch.py's dating convention exactly), fall back to file mtime.
    # predicted_at_ts is epoch-ms for client-side JS sorting; None sorts
    # last regardless of asc/desc via the JS comparator below.
    predicted_at = None
    predicted_at_ts = None
    if local_r:
        src = local_r.get("_source_file")
        ts = _parse_result_filename_timestamp(src) if src else None
        if ts is None and local_r.get("_source_mtime") is not None:
            try:
                ts = datetime.fromtimestamp(local_r["_source_mtime"], tz=timezone.utc)
            except Exception:
                ts = None
        if ts is not None:
            predicted_at = ts.isoformat()
            predicted_at_ts = int(ts.timestamp() * 1000)

    # Refresh-candidate highlighting (Phase 1 alerts from meta_watch.py):
    # only counts as "still pending" if the alert fired AFTER the last
    # known prediction — otherwise a stale alert from before the most
    # recent refresh would keep highlighting a question that's already
    # been handled. If predicted_at is unknown, err on the side of
    # showing the highlight rather than silently hiding a real signal.
    refresh_state = refresh_state or {}
    alert_info = refresh_state.get(str(qid))
    is_refresh_candidate = False
    refresh_alert_reasons: list[str] = []
    if alert_info:
        alerted_at_str = alert_info.get("alerted_at")
        still_pending = True
        if alerted_at_str and predicted_at_ts is not None:
            try:
                alerted_ts = int(datetime.fromisoformat(alerted_at_str).timestamp() * 1000)
                still_pending = alerted_ts >= predicted_at_ts
            except Exception:
                still_pending = True
        if still_pending:
            is_refresh_candidate = True
            refresh_alert_reasons = alert_info.get("reasons") or []

    # Permanent refresh exclusion (added 2026-07-06, see
    # meta_refresh_exclusions.py) — rare, manually-curated edge cases
    # where the refresh preview would otherwise keep surfacing a question
    # forever with nothing actually actionable (e.g. confirmed closed to
    # forecasting despite a local resolve_time that's still months out).
    # Labeled here rather than silently hidden, since Mike specifically
    # wants these visible, not invisible.
    excluded_ids = excluded_ids or {}
    exclusion_info = excluded_ids.get(qid)
    is_refresh_excluded = exclusion_info is not None
    refresh_exclusion_reason = exclusion_info.get("reason", "") if exclusion_info else ""

    if is_personal_only:
        tournaments = [PERSONAL_LABEL] + [t for t in tournaments if t != OTHER_LABEL]
        if not tournaments:
            tournaments = [PERSONAL_LABEL]
    else:
        tournaments = tournaments or ([OTHER_LABEL] if post else [UNKNOWN_LABEL])

    # post_id is the top-level "id" on the API post dict — different from question_id.
    # Needed to build the correct Metaculus URL: /questions/{post_id}/
    post_id = None
    if post:
        post_id = post.get("id") or (post.get("question") or {}).get("post_id")

    submitted_value = (
        (local_r or {}).get("submitted_forecast")
        or (local_r or {}).get("probability")
        or (extract_submitted_forecast(post, q_type) if post else None)
    )
    # Sortable numeric proxy for the Submitted column: only meaningful for
    # binary (a single float) — MC/numeric forecasts aren't a single
    # comparable number, so they sort as blank (last) rather than fake-sorted.
    submitted_sort = submitted_value if isinstance(submitted_value, (int, float)) else None

    # FIXED 2026-07-06: "Resolved at" was sorted via parseFloat() on the raw
    # ISO string (e.g. "2026-06-01T00:00:00Z") — parseFloat stops at the
    # first non-numeric char, so every date collapses to just its leading
    # year (2026, 2026, 2026...), making the sort a no-op for any two
    # questions resolved in the same year. Same fix pattern as
    # predicted_at_ts: parse to a real epoch-ms int at render time instead
    # of handing a date string to a numeric comparator.
    resolve_time_ts = None
    if score["resolve_time"]:
        try:
            resolve_time_ts = int(
                datetime.fromisoformat(score["resolve_time"].replace("Z", "+00:00")).timestamp() * 1000
            )
        except Exception:
            resolve_time_ts = None

    # ADDED 2026-07-15: same epoch-ms sorting fix as resolve_time_ts above,
    # applied to close_time — needed now that it's a real visible/sortable
    # column (Step 2 of the batch-path refresh scheduling work), not just
    # internal data used for badges/chart tooltips.
    close_time_ts = None
    if score["close_time"]:
        try:
            close_time_ts = int(
                datetime.fromisoformat(score["close_time"].replace("Z", "+00:00")).timestamp() * 1000
            )
        except Exception:
            close_time_ts = None

    # ADDED 2026-07-15, FIXED same day: batch-path refresh scheduling
    # (meta_refresh_schedule — see that module for the checkpoint-ladder
    # design). "Batch-path" is now determined by LIVE tournament
    # membership: tournament_forecast_v2.py only ever knows about exactly
    # two tournaments (FutureEval, Market Pulse) — anything else is, by
    # definition, on the batch path, whether or not meta_batch_forecast.py
    # has explicitly been told about it yet. This still satisfies Mike's
    # "work as standard as we add more tournaments" requirement (a new
    # tournament is batch-path automatically, just by not being one of the
    # two named exceptions) — it does NOT need dashboard code changes
    # either, same as the original design intent.
    #
    # ORIGINALLY this checked local-file provenance instead (where the
    # WINNING local result's _source_file actually lived) — replaced after
    # a real live failure: Q43347 (Metaculus Cup) had no local result file
    # in ANY directory (its forecast history predates/bypassed local
    # tracking), so there was nothing to detect provenance FROM, and it
    # silently fell back to "not batch-path" — routing it to
    # tournament_forecast_v2.py, which correctly found 0 matching
    # questions, since that script has never heard of Metaculus Cup. Live
    # tournament membership doesn't have this gap: it's always known from
    # the live Metaculus fetch, regardless of whether local history exists.
    _V2_SCOPE_TOURNAMENTS = {"FutureEval", "Market Pulse Challenge 26Q3"}
    is_batch_path = not any(t in _V2_SCOPE_TOURNAMENTS for t in tournaments)
    # source_file is kept as an informational/debugging field (added
    # 2026-07-15 for the routing diagnostic — see _classify_refresh_ids)
    # even though it's no longer what decides is_batch_path.
    source_file = (local_r or {}).get("_source_file") or ""

    refresh_after = None
    refresh_after_ts = None
    is_due_for_batch_refresh = False
    batch_refresh_due_reason = ""
    has_refresh_override = str(qid) in (refresh_overrides or {})
    if is_batch_path:
        _forecast_for_schedule = {
            "question_id": qid,
            "submitted_at": predicted_at,
            "close_time": score["close_time"],
        }
        _refresh_after_dt = meta_refresh_schedule.compute_refresh_after(
            _forecast_for_schedule, overrides=refresh_overrides
        )
        if _refresh_after_dt is not None:
            refresh_after = _refresh_after_dt.isoformat()
            refresh_after_ts = int(_refresh_after_dt.timestamp() * 1000)
        is_due_for_batch_refresh, batch_refresh_due_reason = meta_refresh_schedule.is_due_for_refresh(
            _forecast_for_schedule, overrides=refresh_overrides
        )

    return {
        "question_id":       qid,
        "post_id":           post_id,
        "question_text":     (local_r or {}).get("question_text") or score["title"] or "(unknown)",
        "question_type":     q_type,
        "submitted_summary": summarize_forecast(q_type, submitted_value),
        "submitted_sort":    submitted_sort,
        "cp_summary":        summarize_forecast(q_type, cp_val) if cp_val is not None else "—",
        "cp_available":      score["cp_available"],
        "resolved":          score["resolved"],
        "resolution":        score["resolution"],
        "resolve_time":      score["resolve_time"],
        "resolve_time_ts":   resolve_time_ts,
        "peer_score":        score["peer_score"],
        "baseline_score":    score["baseline_score"],
        "close_time":        score["close_time"],
        "close_time_ts":     close_time_ts,
        "api_status":        score["api_status"],
        "live_match_found":  post is not None,
        "tournaments":       tournaments,
        "is_personal_only":  is_personal_only,
        # raw reasoning/research from local file (for detail page)
        "reasoning":         (local_r or {}).get("reasoning", ""),
        "research_text":     (local_r or {}).get("research_text", ""),
        # Which provider produced the research above ("openrouter" /
        # "anthropic" / None). Added 2026-07-04 alongside the OpenRouter
        # switch-back, so calibration can be sliced by research source if
        # peer score trends diverge between the two.
        "research_source":   (local_r or {}).get("research_source"),
        "refresh_reason":    (local_r or {}).get("refresh_reason", ""),
        "original_prob":     (local_r or {}).get("original_prob"),
        "predicted_at":      predicted_at,
        "predicted_at_ts":   predicted_at_ts,
        "is_refresh_candidate": is_refresh_candidate,
        "refresh_alert_reasons": refresh_alert_reasons,
        "is_refresh_excluded": is_refresh_excluded,
        "refresh_exclusion_reason": refresh_exclusion_reason,
        # ADDED 2026-07-15: batch-path refresh scheduling (Step 2) — see
        # meta_refresh_schedule.py for the checkpoint-ladder design. Only
        # meaningful when is_batch_path is True; FutureEval/Market Pulse
        # rows carry these as False/None/"" since they're on a separate,
        # already-automatic refresh path.
        "is_batch_path":     is_batch_path,
        # ADDED 2026-07-15: was computed but never actually exposed —
        # needed for the /refresh routing diagnostic (_classify_refresh_ids)
        # so a "why was this classified this way" question can be answered
        # directly from the row instead of re-deriving it separately.
        "source_file":       source_file or None,
        "refresh_after":     refresh_after,
        "refresh_after_ts":  refresh_after_ts,
        "is_due_for_batch_refresh": is_due_for_batch_refresh,
        "batch_refresh_due_reason": batch_refresh_due_reason,
        "has_refresh_override": has_refresh_override,
    }


def _make_group_row(post_id: int, members: list[dict]) -> dict:
    """Collapses N sub-question rows sharing one post_id (a Market
    Pulse-style group_of_questions post — sharing a post_id is itself a
    reliable "this is a group" signal, since Metaculus never gives two
    independent standalone questions the same post_id) into a single
    summary row for the main table.

    Individual member data is NOT lost — full member rows stay available
    via data['groups_by_post_id'][post_id] for the /group/<post_id>
    detail page. This function only builds the aggregate summary shown
    in the collapsed table row itself.

    Column choices confirmed with Mike 2026-07-12:
      - Status: compact aggregate, e.g. "5 open, 1 closed"
      - Submitted: comma-separated per-member forecast summaries
      - Peer score: average across members that HAVE a score (open/
        unresolved members contribute nothing, not a zero — otherwise the
        average would be dragged down by not-yet-scored questions)
    """
    # Common title: strip each member's trailing "(date range)" suffix —
    # same regex approach used for the ntfy alert grouping in
    # tournament_forecast_v2.py — then take whichever stripped title is
    # most common (should be identical across all members in practice;
    # `most common` is just a defensive tie-breaker, not expected to
    # matter in normal operation).
    titles = [re.sub(r"\s*\([^)]*\)\s*$", "", m["question_text"]).strip() or m["question_text"]
              for m in members]
    common_title = max(set(titles), key=titles.count)

    members_sorted = sorted(members, key=lambda m: m["question_id"])

    status_counts: dict[str, int] = {}
    for m in members:
        status_counts[m["status_bucket"]] = status_counts.get(m["status_bucket"], 0) + 1
    status_agg = ", ".join(
        f"{status_counts[s]} {STATUS_LABELS[s].lower()}"
        for s in STATUS_ORDER if status_counts.get(s)
    )
    # Overall bucket for THIS row, so the Status filter pills behave
    # sensibly against group rows too: "open" if ANY member still needs
    # attention, else whichever status is present, in STATUS_ORDER.
    overall_bucket = "open" if status_counts.get("open") else next(
        (s for s in STATUS_ORDER if status_counts.get(s)), "not_found_live"
    )

    submitted_list = ", ".join(
        m["submitted_summary"] for m in members_sorted if m["submitted_summary"] != "—"
    )

    scored = [m["peer_score"] for m in members if m["peer_score"] is not None]
    avg_peer = (sum(scored) / len(scored)) if scored else None

    predicted_candidates = [m["predicted_at_ts"] for m in members if m["predicted_at_ts"] is not None]
    predicted_at_ts = max(predicted_candidates) if predicted_candidates else None
    predicted_at = (
        datetime.fromtimestamp(predicted_at_ts / 1000, tz=timezone.utc).isoformat()
        if predicted_at_ts is not None else None
    )

    # Search needs to reach into EVERY member's own text/id — a search for
    # e.g. "Jul 13" or a specific question_id lives on a member, not the
    # group's own (de-suffixed) title, so the group row must still match.
    # FIXED (2026-07-12, confirmed live via jsdom test): this previously
    # omitted post_id itself — searching "44536" (the exact number shown
    # in this row's own ID column) returned zero results, since only
    # MEMBER question_ids (44691-44696) were included, never the group's
    # own post_id. post_id is now prepended explicitly.
    search_blob = (f"{post_id} " + " ".join(
        f"{m['question_text']} {m['question_id']}" for m in members
    )).lower()

    exclusion_reasons = sorted({m["refresh_exclusion_reason"] for m in members if m["refresh_exclusion_reason"]})

    return {
        "is_group":          True,
        # Reuses the existing "question_id" slot for sorting/the ID
        # column/detail-link plumbing — post_id IS this row's identity,
        # consistent with the post_id-first-column change applied
        # everywhere else in this file.
        "question_id":       post_id,
        "post_id":           post_id,
        "question_text":     common_title,
        "question_type":     members[0]["question_type"] if members else None,
        "submitted_summary": submitted_list or "—",
        "submitted_sort":    None,  # not a single comparable number — sorts last, same convention as MC/numeric rows
        "cp_summary":        "—",  # per-member CP aggregation isn't meaningful the same way group peer score is — kept simple
        "cp_available":      False,
        "resolved":          bool(members) and all(m["resolved"] for m in members),
        "resolution":        None,
        "resolve_time":      None,
        "resolve_time_ts":   None,
        "peer_score":        avg_peer,
        "baseline_score":    None,
        "close_time":        None,
        "close_time_ts":     None,
        "api_status":        None,
        "live_match_found":  any(m["live_match_found"] for m in members),
        "tournaments":       members[0]["tournaments"] if members else [],
        "is_personal_only":  False,
        "reasoning": "", "research_text": "", "research_source": None, "refresh_reason": "",
        "original_prob":     None,
        "predicted_at":      predicted_at,
        "predicted_at_ts":   predicted_at_ts,
        "is_refresh_candidate": any(m["is_refresh_candidate"] for m in members),
        "refresh_alert_reasons": sorted({reason for m in members for reason in m["refresh_alert_reasons"]}),
        "is_refresh_excluded": bool(members) and all(m["is_refresh_excluded"] for m in members),
        "refresh_exclusion_reason": "; ".join(exclusion_reasons),
        # ADDED 2026-07-15: group rows are Market Pulse only in practice
        # (the only group_of_questions tournament today), which is never
        # batch-path — defaulted off rather than aggregated from members,
        # same convention as close_time/resolve_time above.
        "is_batch_path":     False,
        "source_file":       None,
        "refresh_after":     None,
        "refresh_after_ts":  None,
        "is_due_for_batch_refresh": False,
        "batch_refresh_due_reason": "",
        "has_refresh_override": False,
        "status_bucket":     overall_bucket,
        "status_label":      status_agg or STATUS_LABELS[overall_bucket],
        "member_question_ids": [m["question_id"] for m in members_sorted],
        "member_count":      len(members),
        "search_blob":       search_blob,
    }


def collapse_groups(flat_rows: list[dict]) -> tuple[list[dict], dict[int, list[dict]]]:
    """Splits flat_rows into (table_rows, groups_by_post_id):
      - table_rows: what the main table actually renders — any post_id
        shared by >1 row becomes ONE group summary row; everything else
        (every non-Market-Pulse question, and any group with exactly one
        currently-visible member) passes through unchanged.
      - groups_by_post_id: post_id -> full list of that group's member
        rows, for the /group/<post_id> detail page. NOT limited to
        collapsed groups' members only — kept as a straightforward full
        index so the detail route doesn't need special-casing.

    Called AFTER status_counts/avg_score/chart data/etc. are already
    computed from flat_rows elsewhere in build_dashboard_data() — those
    stats intentionally stay based on the full individual-question set,
    not collapsed groups, so e.g. the "Open (23)" status pill count
    means 23 actual questions, not 23 groups-with-an-open-member."""
    by_post_id: dict[int, list[dict]] = {}
    for r in flat_rows:
        if r["post_id"] is not None:
            by_post_id.setdefault(r["post_id"], []).append(r)

    group_post_ids = {pid for pid, members in by_post_id.items() if len(members) > 1}

    table_rows = []
    for r in flat_rows:
        if r["post_id"] in group_post_ids:
            continue  # replaced by its group row, added once below
        table_rows.append(r)
    for pid in group_post_ids:
        table_rows.append(_make_group_row(pid, by_post_id[pid]))

    # Group rows get appended after the loop above, so re-sort to restore
    # the same default ID-descending order flat_rows already had — matches
    # the sort already applied to `rows` before this function is called.
    table_rows.sort(key=lambda r: r["question_id"], reverse=True)

    return table_rows, by_post_id


# ─── Data assembly ────────────────────────────────────────────────────────────
def build_dashboard_data():
    local          = load_local_results(LOCAL_RESULT_DIRS)
    bot_live       = fetch_predicted_questions(bot_client, "bot")
    personal_live  = fetch_predicted_questions(personal_client, "personal")
    refresh_state  = load_refresh_candidate_state()
    excluded_ids   = load_excluded_ids()
    # ADDED 2026-07-15: loaded once per cache cycle, same pattern as
    # excluded_ids/refresh_state above — see meta_refresh_schedule.py.
    refresh_overrides = meta_refresh_schedule.load_refresh_overrides()

    rows = []
    seen = set()

    # Bot-account rows (local result present)
    for qid, r in sorted(local.items(), key=lambda kv: kv[0], reverse=True):
        post = bot_live.get(qid)
        # Fall back to personal_live so personal-account questions with local
        # results aren't misclassified as not_found_live (and then dropped).
        is_personal_only = False
        if post is None and qid in personal_live:
            post = personal_live[qid]
            is_personal_only = True
        rows.append(_make_row(qid, r, post, is_personal_only=is_personal_only, refresh_state=refresh_state,
                               excluded_ids=excluded_ids, refresh_overrides=refresh_overrides))
        seen.add(qid)

    # Bot-live-only rows (no local result — manual predictions etc.)
    for qid, post in bot_live.items():
        if qid in seen:
            continue
        rows.append(_make_row(qid, None, post, is_personal_only=False, refresh_state=refresh_state,
                               excluded_ids=excluded_ids, refresh_overrides=refresh_overrides))
        seen.add(qid)

    # Personal-only rows (not predicted by bot)
    for qid, post in personal_live.items():
        if qid in seen:
            continue
        local_r = local.get(qid)
        rows.append(_make_row(qid, local_r, post, is_personal_only=True, refresh_state=refresh_state,
                               excluded_ids=excluded_ids, refresh_overrides=refresh_overrides))
        seen.add(qid)

    for row in rows:
        row["status_bucket"] = classify_status(row)
        row["status_label"]  = STATUS_LABELS[row["status_bucket"]]
        # FIXED 2026-07-06: is_refresh_candidate was computed in _make_row
        # purely from meta_watch.py's alert state file, with no check that
        # the question is still actually open. FutureEval questions close
        # within ~3 hours, so by dashboard-render time they're usually
        # already closed_unresolved — but the CP-shift alert signal has no
        # tournament/age gate, so a stale alert from just before close
        # could still be sitting in refresh_candidate_state.json. A closed
        # question can never actually be refreshed, so highlighting it is
        # pure noise (confirmed live: 2 of 3 highlighted rows were
        # already-closed FutureEval questions). Gate here, once
        # status_bucket is known.
        if row["status_bucket"] != "open":
            row["is_refresh_candidate"] = False
            row["refresh_alert_reasons"] = []
            # ADDED 2026-07-15: same reasoning as is_refresh_candidate above,
            # applied to the new batch-path due-for-refresh signal — a
            # closed question can't actually be refreshed either way.
            row["is_due_for_batch_refresh"] = False
            row["batch_refresh_due_reason"] = ""

    # Drop withdrawn rows with nothing useful — no peer score, no resolution, no CP.
    # Keep withdrawn rows that have a peer score (resolved+scored before leaving the feed).
    rows = [r for r in rows if not (r["status_bucket"] == "not_found_live" and r["peer_score"] is None)]

    rows.sort(key=lambda r: r["question_id"], reverse=True)

    status_counts = {k: 0 for k in STATUS_LABELS}
    for row in rows:
        status_counts[row["status_bucket"]] += 1

    resolved_scored = [r for r in rows if r["status_bucket"] == "resolved_scored"]
    avg_score = (
        sum(r["peer_score"] for r in resolved_scored) / len(resolved_scored)
    ) if resolved_scored else None

    # Chart: two datasets — bot (blue) vs personal (orange).
    # Generated directly from each account's live data, independently of the
    # table deduplication (where bot wins on shared questions). This means a
    # question predicted by both accounts contributes one dot per account —
    # correct, since they're independent forecasts with independent peer scores.
    def _chart_points_from_live(live_dict: dict) -> list:
        points = []
        for qid, post in live_dict.items():
            score = extract_score_info(post)
            if score["peer_score"] is None or not score["close_time"]:
                continue
            tournaments = detect_tournaments(post) or [OTHER_LABEL]
            points.append({
                "x": score["close_time"],
                "y": score["peer_score"],
                "tournaments": tournaments,
                "label": score["title"] or str(qid),
                "qid": qid,
            })
        return points

    chart_bot      = _chart_points_from_live(bot_live)
    chart_personal = _chart_points_from_live(personal_live)

    tournaments_present = [t for t in TOURNAMENT_ORDER
                           if any(t in r["tournaments"] for r in rows)]
    # Split for the two-heading filter bar (Tournament / Question Series).
    # tournaments_present itself is kept as-is (unsplit) since other code
    # still reads it for the "Tournament filter applies" chart note etc.
    tournaments_present_main   = [t for t in tournaments_present if t not in QUESTION_SERIES_LABELS]
    tournaments_present_series = [t for t in tournaments_present if t in QUESTION_SERIES_LABELS]

    phase0 = load_phase0_reports()
    openrouter_balance = load_openrouter_balance()
    refresh_candidate_count = sum(1 for r in rows if r["is_refresh_candidate"])
    # ADDED 2026-07-15: mirrors refresh_candidate_count above, for the new
    # batch-path due-for-refresh signal (age/close-time based, distinct
    # from the CP-shift-based one).
    batch_refresh_due_count = sum(1 for r in rows if r["is_due_for_batch_refresh"])

    # Finish-line flag (added 2026-07-06): Mike's plan is to let the
    # existing refresh pipeline naturally re-forecast (under the bot
    # account) any still-open question that was only ever forecasted
    # under Personal historically — no code changes needed there, since
    # already_done/find_questions_to_refresh() were never account-aware
    # to begin with. Once every Personal-tagged row is closed, there's
    # nothing left for the refresh pipeline to pick up, and it's safe to
    # go remove personal-account code (personal_client, PERSONAL_TOKEN,
    # this dashboard's own Personal-tag display, etc.) — this banner is
    # that "you can stop waiting now" signal. False (no banner) if there
    # are zero Personal rows at all — that's a different state (nothing
    # to migrate, not "migration complete") and shouldn't look identical.
    personal_rows = [r for r in rows if r["is_personal_only"]]
    personal_open_count = sum(1 for r in personal_rows if r["status_bucket"] == "open")
    personal_finish_line = len(personal_rows) > 0 and personal_open_count == 0

    # NEW (2026-07-12): collapse Market-Pulse-style group_of_questions
    # sub-questions into one summary row per group for the TABLE only —
    # every stat above (status_counts, avg_score, chart, tournaments_present,
    # refresh_candidate_count, personal_*) intentionally stays computed on
    # the full flat `rows` list, not the collapsed one, so e.g. "Open (23)"
    # means 23 actual questions, not 23 groups-with-an-open-member.
    table_rows, groups_by_post_id = collapse_groups(rows)

    data = {
        "rows":                table_rows,
        "groups_by_post_id":   groups_by_post_id,
        "phase0":              phase0,
        "openrouter_balance":  openrouter_balance,
        "refresh_candidate_count": refresh_candidate_count,
        "batch_refresh_due_count": batch_refresh_due_count,
        "personal_total_count": len(personal_rows),
        "personal_open_count":  personal_open_count,
        "personal_finish_line": personal_finish_line,
        "status_counts":       status_counts,
        "avg_score":           avg_score,
        "chart_bot":           chart_bot,
        "chart_personal":      chart_personal,
        "total_predicted":     len(rows),
        "bot_user_id":         get_confirmed_user_id(bot_client),
        "personal_user_id":    get_confirmed_user_id(personal_client),
        "token_configured":    bot_client is not None,
        "tournaments_present": tournaments_present,
        "tournaments_present_main": tournaments_present_main,
        "tournaments_present_series": tournaments_present_series,
    }
    return data, bot_live, personal_live, local


def refresh_cache_loop():
    while True:
        try:
            data, bot_live, personal_live, local = build_dashboard_data()
            with CACHE_LOCK:
                CACHE["data"]               = data
                CACHE["live_by_qid"]        = bot_live
                CACHE["personal_live_by_qid"] = personal_live
                CACHE["local_by_qid"]       = local
                CACHE["last_refresh"]       = datetime.now(timezone.utc)
                CACHE["error"]              = None
            print(f"  cache refreshed: {data['total_predicted']} questions at {CACHE['last_refresh'].isoformat()}")
        except Exception as e:
            print(f"  cache refresh FAILED: {e}")
            with CACHE_LOCK:
                CACHE["error"] = str(e)
        time.sleep(REFRESH_INTERVAL_SECONDS)


# ─── Templates ───────────────────────────────────────────────────────────────
PAGE_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Metaculus Track Record</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.29.4/moment.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-adapter-moment/1.0.1/chartjs-adapter-moment.min.js"></script>
  <style>
    body { font-family: -apple-system, Segoe UI, Arial, sans-serif; margin: 0; padding: 24px;
           background: #f7f8fa; color: #1a1a1a; }
    h1 { font-size: 20px; margin-bottom: 4px; }
    .sub { color: #666; font-size: 13px; margin-bottom: 20px; }
    .cards { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
    .card { background: white; border-radius: 8px; padding: 16px 20px; box-shadow: 0 1px 3px rgba(0,0,0,.08); min-width: 130px; }
    .card .label { font-size: 12px; color: #888; }
    .card .value { font-size: 24px; font-weight: 600; }
    .filter-bar { background: white; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px;
                  box-shadow: 0 1px 3px rgba(0,0,0,.08); }
    .filter-group-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: .04em;
                          margin: 10px 0 6px; }
    .filter-group-label:first-child { margin-top: 0; }
    .pills { display: flex; gap: 8px; flex-wrap: wrap; }
    .pill { padding: 6px 14px; border-radius: 16px; background: #eef0f3; color: #444; font-size: 13px;
            cursor: pointer; user-select: none; border: 1px solid transparent; transition: all .1s; }
    .pill:hover { background: #e2e5ea; }
    .pill.active { background: #2563eb; color: white; }
    .filter-footer { display: flex; justify-content: space-between; align-items: center; margin-top: 12px;
                     padding-top: 12px; border-top: 1px solid #eee; }
    .clear-btn { font-size: 12px; color: #888; cursor: pointer; text-decoration: underline; }
    .showing-count { font-size: 12px; color: #999; }
    table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px;
            overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.08); font-size: 13px; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #fafafa; color: #666; font-weight: 600; }
    th.sortable { cursor: pointer; user-select: none; white-space: nowrap; }
    th.sortable:hover { color: #2563eb; }
    th.sortable .arrow { display: inline-block; width: 12px; color: #bbb; font-size: 11px; }
    th.sortable.sorted .arrow { color: #2563eb; }
    tr:hover { background: #fafbfc; }
    tr.highlight { animation: rowflash 3s ease-out; }
    @keyframes rowflash { 0% { background: #fef9c3; } 50% { background: #fef9c3; } 100% { background: transparent; } }
    tr.refresh-candidate { box-shadow: inset 3px 0 0 #f59e0b; }
    tr.refresh-candidate td:first-child { background: #fffbeb; }
    .refresh-badge { display: inline-block; font-size: 11px; margin-left: 4px; cursor: help; }
    /* ADDED 2026-07-15: distinct from refresh-candidate (CP-shift based,
       amber) — this is the age/close-time based batch-path signal, given
       its own color so the two are never visually confused. Uses
       border-left instead of refresh-candidate's box-shadow so a row that
       happens to match BOTH signals shows both indicators rather than one
       overwriting the other. */
    tr.batch-refresh-due { border-left: 3px solid #7c3aed; }
    tr.batch-refresh-due td:nth-child(2) { background: #f5f3ff; }
    .refresh-cell { white-space: nowrap; }
    .edit-refresh-btn { cursor: pointer; color: #2563eb; font-size: 11px; margin-left: 5px; }
    .edit-refresh-btn:hover { text-decoration: underline; }
    .pos { color: #16a34a; font-weight: 600; }
    .neg { color: #dc2626; font-weight: 600; }
    .muted { color: #999; }
    .tag { display: inline-block; font-size: 11px; background: #eef0f3; color: #555; border-radius: 4px;
           padding: 1px 6px; margin-right: 4px; }
    .tag.personal { background: #fef3c7; color: #92400e; }
    .chart-wrap { background: white; border-radius: 8px; padding: 16px; margin-bottom: 8px; height: 280px; }
    .chart-note { color: #999; font-size: 12px; margin: 0 0 24px; }
    a.detail-link { font-size: 11px; color: #888; }
    a.id-link { color: #2563eb; text-decoration: none; }
    a.id-link:hover { text-decoration: underline; }
    .refresh-note { color: #999; font-size: 12px; margin: 8px 0 16px; }
    .cp-na { color: #ccc; font-size: 11px; }
    .finish-line-banner { background: #dcfce7; border: 1px solid #86efac; color: #14532d;
                           border-radius: 8px; padding: 14px 18px; margin-bottom: 16px;
                           font-size: 14px; line-height: 1.5; }
    .finish-line-banner strong { color: #166534; }
    .truncate { max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .pagination { display: flex; align-items: center; justify-content: center; gap: 12px;
                  padding: 14px; background: white; border-radius: 0 0 8px 8px; margin-top: -8px;
                  box-shadow: 0 1px 3px rgba(0,0,0,.08); font-size: 13px; }
    .pagination button { border: 1px solid #ddd; background: white; border-radius: 6px; padding: 5px 12px;
                          cursor: pointer; font-size: 13px; color: #444; }
    .pagination button:hover:not(:disabled) { background: #f3f4f6; }
    .pagination button:disabled { color: #ccc; cursor: default; }
    .pagination .page-info { color: #888; }
  </style>
</head>
<body>
  <h1>Metaculus Track Record</h1>
  <div class="sub">
    mike_iz_-bot{% if data.bot_user_id %} ({{ data.bot_user_id }}){% endif %}
    · mike_iz_{% if data.personal_user_id %} ({{ data.personal_user_id }}){% endif %}
    {% if not data.token_configured %} — ⚠️ METAC_TOURNAMENT_TOKEN not set in .env{% endif %}
  </div>
  <div class="refresh-note">
    Last refreshed: {{ last_refresh }} · auto-refreshes every 5 minutes (paused while questions are selected)
    <button id="manualRefreshBtn" onclick="location.reload()"
            style="margin-left:8px;font-size:11px;padding:2px 8px;cursor:pointer;">🔄 Refresh now</button>
    {% if cache_error %} · ⚠️ last refresh failed: {{ cache_error }}{% endif %}
  </div>

  <div class="cards">
    <div class="card"><div class="label">Total predicted</div><div class="value">{{ data.total_predicted }}</div></div>
    <div class="card"><div class="label">Open</div><div class="value">{{ data.status_counts.open }}</div></div>
    <div class="card"><div class="label">Closed</div><div class="value">{{ data.status_counts.closed_unresolved }}</div></div>
    <div class="card"><div class="label">Resolved &amp; scored</div><div class="value">{{ data.status_counts.resolved_scored }}</div></div>
    <div class="card"><div class="label">Resolved, no score</div><div class="value">{{ data.status_counts.resolved_unscored }}</div></div>
    <div class="card"><div class="label">Withdrawn</div><div class="value">{{ data.status_counts.not_found_live }}</div></div>
    <div class="card"><div class="label">🔄 Refresh candidates</div>
      <div class="value {{ 'neg' if data.refresh_candidate_count else '' }}">{{ data.refresh_candidate_count }}</div>
    </div>
    <div class="card"><div class="label">Avg peer score</div>
      <div class="value {{ 'pos' if data.avg_score and data.avg_score > 0 else ('neg' if data.avg_score and data.avg_score < 0 else '') }}">
        {{ '%.2f'|format(data.avg_score) if data.avg_score is not none else '—' }}
      </div>
    </div>
  </div>

  {% if data.personal_finish_line %}
  <div class="finish-line-banner">
    🏁 <strong>Personal-account migration complete</strong> — all {{ data.personal_total_count }}
    question(s) previously tracked under Personal are now closed (0 still open). The refresh
    pipeline has nothing further to pick up here. Safe to go remove personal-account code
    (personal_client, METACULUS_TOKEN usage, this dashboard's Personal-tag display) from the pipeline.
  </div>
  {% endif %}

  {% if data.phase0.coverage or data.phase0.calibration or data.openrouter_balance %}
  <div class="cards" style="margin-top:-4px;">
    {% if data.phase0.coverage %}
    <div class="card">
      <div class="label">Coverage gaps ({{ data.phase0.coverage.checked_at[:10] }})</div>
      <div class="value {{ 'neg' if data.phase0.coverage.total_gaps else 'pos' }}">
        {{ data.phase0.coverage.total_gaps }}
      </div>
    </div>
    {% endif %}
    {% if data.phase0.calibration %}
    <div class="card">
      <div class="label">Calibration sample</div>
      <div class="value">{{ data.phase0.calibration.questions_scored }}</div>
    </div>
    <div class="card">
      <div class="label">Avg peer score (own, resolved)</div>
      <div class="value {{ 'pos' if data.phase0.calibration.average_peer_score and data.phase0.calibration.average_peer_score > 0 else 'neg' }}">
        {{ '%.2f'|format(data.phase0.calibration.average_peer_score) if data.phase0.calibration.average_peer_score is not none else '—' }}
      </div>
    </div>
    {% endif %}
    {% if data.openrouter_balance %}
    <div class="card">
      <div class="label">OpenRouter credit remaining</div>
      <div class="value {{ 'neg' if data.openrouter_balance.remaining is not none and data.openrouter_balance.remaining < 10 else '' }}">
        {% if data.openrouter_balance.remaining is not none %}
          ${{ '%.2f'|format(data.openrouter_balance.remaining) }}{% if data.openrouter_balance.limit %} / ${{ '%.0f'|format(data.openrouter_balance.limit) }}{% endif %}
        {% else %}
          —
        {% endif %}
      </div>
    </div>
    {% endif %}
  </div>
  {% endif %}

  <div class="filter-bar">
    <div class="filter-group-label">Search</div>
    <input type="text" id="searchBox" placeholder="Search question text or ID…"
           style="width:100%;max-width:400px;padding:7px 10px;border:1px solid #ddd;
                  border-radius:6px;font-size:13px;margin-bottom:12px;box-sizing:border-box;">
    <div class="filter-group-label">Tournament</div>
    <div class="pills" id="tournamentPills">
      {% for t in data.tournaments_present_main %}
      <div class="pill" data-value="{{ t }}">{{ t }}</div>
      {% endfor %}
    </div>
    {% if data.tournaments_present_series %}
    <div class="filter-group-label">Question Series</div>
    <div class="pills" id="questionSeriesPills">
      {% for t in data.tournaments_present_series %}
      <div class="pill" data-value="{{ t }}">{{ t }}</div>
      {% endfor %}
    </div>
    {% endif %}
    <div class="filter-group-label">Status</div>
    <div class="pills" id="statusPills">
      {% for s in status_order %}
      <div class="pill" data-value="{{ s }}">{{ status_labels[s] }} ({{ data.status_counts[s] }})</div>
      {% endfor %}
    </div>
    <div class="filter-group-label">Signals</div>
    <div class="pills" id="signalPills">
      <div class="pill" data-value="refresh_candidate">🔄 Refresh candidates only ({{ data.refresh_candidate_count }})</div>
      <div class="pill" data-value="batch_refresh_due">⏰ Due for refresh (batch-path) ({{ data.batch_refresh_due_count }})</div>
    </div>
    <div class="filter-footer">
      <span class="clear-btn" id="clearFilters">Clear all filters</span>
      <span class="showing-count" id="showingCount"></span>
    </div>
  </div>

  <div class="chart-wrap" id="chartWrap" style="display:none;"><canvas id="scoreChart"></canvas></div>
  <p class="chart-note">Peer scores over time (blue = bot account, orange = personal account). X-axis = scheduled close date. Tournament filter applies; status filter does not.</p>
  <div id="noChartMsg" style="background:white;border-radius:8px;padding:24px;margin-bottom:24px;
       text-align:center;color:#999;box-shadow:0 1px 3px rgba(0,0,0,.08);">
    No resolved &amp; scored questions yet — chart appears once at least one has a peer score.
  </div>

  <table id="rowsTable">
    <thead>
      <tr>
        <th style="width:26px;"><input type="checkbox" id="selectAllVisible" title="Select all visible non-group rows"></th>
        <th class="sortable" data-key="id" data-type="num">ID <span class="arrow">▾</span></th>
        <th class="sortable" data-key="question" data-type="str">Question <span class="arrow"></span></th>
        <th class="sortable" data-key="type" data-type="str">Type <span class="arrow"></span></th>
        <th class="sortable" data-key="submitted" data-type="num">Submitted <span class="arrow"></span></th>
        <th class="sortable" data-key="predicted" data-type="num">Last predicted <span class="arrow"></span></th>
        <th class="sortable" data-key="close" data-type="num">Close date <span class="arrow"></span></th>
        <th class="sortable" data-key="status" data-type="str">Status <span class="arrow"></span></th>
        <th class="sortable" data-key="resolution" data-type="str">Resolution <span class="arrow"></span></th>
        <th class="sortable" data-key="resolved" data-type="num">Resolved at <span class="arrow"></span></th>
        <th class="sortable" data-key="peer" data-type="num">Peer score <span class="arrow"></span></th>
        <th class="sortable" data-key="tournament" data-type="str">Tournament(s) <span class="arrow"></span></th>
        <th class="sortable" data-key="refresh" data-type="num" title="Batch-path tournaments only">Refresh <span class="arrow"></span></th>
        <th></th>
      </tr>
    </thead>
    <tbody>
      {% for row in data.rows %}
      <tr id="row-{{ row.question_id }}"
          class="{{ 'refresh-candidate' if row.is_refresh_candidate else '' }} {{ 'batch-refresh-due' if row.is_due_for_batch_refresh else '' }}"
          data-tournaments="{{ row.tournaments|join(',') }}" data-status="{{ row.status_bucket }}"
          data-refresh="{{ 'true' if row.is_refresh_candidate else 'false' }}"
          data-batch-refresh-due="{{ 'true' if row.is_due_for_batch_refresh else 'false' }}"
          data-batch-path="{{ 'true' if row.is_batch_path else 'false' }}"
          data-search="{{ (row.search_blob if row.is_group else (row.question_text ~ ' ' ~ row.question_id ~ ' ' ~ (row.post_id or '')))|lower|e }}"
          data-sort-id="{{ row.question_id }}"
          data-sort-question="{{ row.question_text|lower|e }}"
          data-sort-type="{{ row.question_type or '' }}"
          data-sort-submitted="{{ row.submitted_sort if row.submitted_sort is not none else '' }}"
          data-sort-predicted="{{ row.predicted_at_ts if row.predicted_at_ts is not none else '' }}"
          data-sort-close="{{ row.close_time_ts if row.close_time_ts is not none else '' }}"
          data-sort-status="{{ row.status_label }}"
          data-sort-resolution="{{ row.resolution if row.resolution is not none else '' }}"
          data-sort-resolved="{{ row.resolve_time_ts if row.resolve_time_ts is not none else '' }}"
          data-sort-peer="{{ row.peer_score if row.peer_score is not none else '' }}"
          data-sort-tournament="{{ row.tournaments|join(',') }}"
          data-sort-refresh="{{ row.refresh_after_ts if row.refresh_after_ts is not none else '' }}">
        <td>
          {% if not row.is_group and row.post_id and row.status_bucket == 'open' and not row.is_refresh_excluded %}
            <input type="checkbox" class="row-select" value="{{ row.post_id }}"
                   onclick="toggleRowSelect(this)">
          {% endif %}
        </td>
        <td>
          {% if row.post_id %}
            <a class="id-link" href="https://www.metaculus.com/questions/{{ row.post_id }}/" target="_blank"
               title="Open on Metaculus">{{ row.post_id }}</a>
          {% else %}
            {{ row.post_id or row.question_id }}
          {% endif %}
          {% if row.is_refresh_candidate %}
            <span class="refresh-badge" title="Refresh candidate: {{ row.refresh_alert_reasons|join(', ') }}">🔄</span>
          {% endif %}
          {% if row.is_refresh_excluded %}
            <span class="refresh-badge" title="Permanently excluded from refresh: {{ row.refresh_exclusion_reason }}">🚫</span>
          {% endif %}
        </td>
        <td>
          {% if row.is_group %}<span title="Group of {{ row.member_count }} sub-questions">🗂️</span>{% endif %}
          {{ row.question_text[:70] }}{% if row.is_group %} <span class="muted" style="font-size:11px;">({{ row.member_count }})</span>{% endif %}
        </td>
        <td>{{ row.question_type or '—' }}</td>
        <td class="truncate" title="{{ row.submitted_summary }}">{{ row.submitted_summary }}</td>
        <td>{{ row.predicted_at[:10] if row.predicted_at else '—' }}</td>
        <td>{{ row.close_time[:10] if row.close_time else '—' }}</td>
        <td>{{ row.status_label }}</td>
        <td>{{ row.resolution if row.resolution is not none else '—' }}</td>
        <td>{{ row.resolve_time[:10] if row.resolve_time else '—' }}</td>
        <td class="{{ 'pos' if row.peer_score and row.peer_score > 0 else ('neg' if row.peer_score and row.peer_score < 0 else 'muted') }}">
          {{ '%.2f'|format(row.peer_score) if row.peer_score is not none else (row.resolved and '?' or '—') }}
        </td>
        <td>{% for t in row.tournaments %}<span class="tag {{ 'personal' if t == 'Personal' else '' }}">{{ t }}</span>{% endfor %}</td>
        <td class="refresh-cell">
          {% if row.is_batch_path %}
            <span class="refresh-date-display" id="refresh-display-{{ row.question_id }}">{% if row.is_due_for_batch_refresh %}<span class="refresh-badge" title="{{ row.batch_refresh_due_reason }}">⏰</span> {% endif %}{{ row.refresh_after[:10] if row.refresh_after else '—' }}{% if row.has_refresh_override %} <span class="muted" style="font-size:10px;">(manual)</span>{% endif %}</span>
            <span class="edit-refresh-btn" title="Edit refresh date"
                  onclick="editRefreshAfter({{ row.question_id }}, '{{ row.refresh_after[:10] if row.refresh_after else '' }}')">✎</span>
          {% else %}
            <span class="muted" title="FutureEval/Market Pulse questions refresh automatically — not scheduled here">—</span>
          {% endif %}
        </td>
        <td>
          {% if row.is_group %}
            <a class="detail-link" href="/group/{{ row.post_id }}" target="_blank">detail</a>
          {% else %}
            <a class="detail-link" href="/detail/{{ row.question_id }}" target="_blank">detail</a>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  <div class="pagination" id="pagination">
    <button id="prevPage">← Prev</button>
    <span class="page-info" id="pageInfo"></span>
    <button id="nextPage">Next →</button>
  </div>

  <div id="selectionBar" style="display:none;position:fixed;bottom:0;left:0;right:0;
       background:#1a1a1a;color:white;padding:12px 24px;align-items:center;
       gap:16px;box-shadow:0 -2px 8px rgba(0,0,0,.2);z-index:1000;">
    <span id="selectionCount"></span>
    <button id="refreshSelected" style="background:#2563eb;color:white;border:none;
            border-radius:4px;padding:6px 14px;cursor:pointer;font-size:13px;">Refresh Selected</button>
    <button id="clearSelection" style="background:transparent;color:#ccc;border:1px solid #555;
            border-radius:4px;padding:6px 14px;cursor:pointer;font-size:13px;">Clear</button>
  </div>

  <script>
    const STORAGE_KEY = 'meta_dashboard_filters_v3';
    const botPoints      = {{ chart_bot|tojson }};
    const personalPoints = {{ chart_personal|tojson }};
    let chartInstance = null;

    function getSelected(id) {
      return new Set([...document.querySelectorAll('#' + id + ' .pill.active')].map(el => el.dataset.value));
    }
    // ADDED 2026-07-16: Tournament and Question Series are now two visually
    // separate pill groups (#tournamentPills / #questionSeriesPills) but
    // still one logical filter dimension — a row matches if its tournament
    // is in EITHER selected set. Every place that used to call
    // getSelected('tournamentPills') alone now calls this instead.
    function getSelectedTournaments() {
      return new Set([...getSelected('tournamentPills'), ...getSelected('questionSeriesPills')]);
    }
    function saveFilters() {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({
        t: [...getSelectedTournaments()], s: [...getSelected('statusPills')],
        g: [...getSelected('signalPills')], q: document.getElementById('searchBox').value
      }));
    }
    function restoreFilters() {
      try {
        const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
        (saved.t || []).forEach(v => {
          const el = document.querySelector('#tournamentPills .pill[data-value="' + CSS.escape(v) + '"]') ||
                     document.querySelector('#questionSeriesPills .pill[data-value="' + CSS.escape(v) + '"]');
          if (el) el.classList.add('active');
        });
        (saved.s || []).forEach(v => {
          const el = document.querySelector('#statusPills .pill[data-value="' + CSS.escape(v) + '"]');
          if (el) el.classList.add('active');
        });
        (saved.g || []).forEach(v => {
          const el = document.querySelector('#signalPills .pill[data-value="' + CSS.escape(v) + '"]');
          if (el) el.classList.add('active');
        });
        if (saved.q) document.getElementById('searchBox').value = saved.q;
      } catch(e) {}
    }
    function filterPoints(points, selT) {
      return selT.size === 0 ? points : points.filter(p => p.tournaments.some(t => selT.has(t)));
    }
    function renderChart(selT) {
      const bpts = filterPoints(botPoints, selT);
      const ppts = filterPoints(personalPoints, selT);
      const wrap = document.getElementById('chartWrap');
      const noMsg = document.getElementById('noChartMsg');
      if (bpts.length + ppts.length === 0) {
        wrap.style.display = 'none'; noMsg.style.display = 'block'; return;
      }
      wrap.style.display = 'block'; noMsg.style.display = 'none';
      if (chartInstance) chartInstance.destroy();
      chartInstance = new Chart(document.getElementById('scoreChart'), {
        type: 'scatter',
        data: { datasets: [
          { label: 'Bot (mike_iz_-bot)', data: bpts, backgroundColor: '#2563eb', pointRadius: 5 },
          { label: 'Personal (mike_iz_)', data: ppts, backgroundColor: '#f97316', pointRadius: 5 },
        ]},
        options: {
          responsive: true, maintainAspectRatio: false,
          onClick: (evt, elements) => {
            if (!elements.length) return;
            const pt = chartInstance.data.datasets[elements[0].datasetIndex].data[elements[0].index];
            if (!pt.qid) return;
            const tr = document.getElementById('row-' + pt.qid);
            if (!tr) return;
            // If row is currently hidden by a filter, clear filters first
            if (tr.style.display === 'none') {
              document.querySelectorAll('.pill.active').forEach(p => p.classList.remove('active'));
              saveFilters(); applyFilters();
            }
            tr.scrollIntoView({ behavior: 'smooth', block: 'start' });
            tr.classList.remove('highlight');
            void tr.offsetWidth; // force reflow so animation restarts cleanly
            tr.classList.add('highlight');
          },
          plugins: {
            tooltip: {
              callbacks: {
                label: ctx => {
                  const p = ctx.raw;
                  const score = typeof p.y === 'number' ? p.y.toFixed(2) : p.y;
                  const label = p.label ? p.label.slice(0, 60) : '';
                  return label ? `${score}  — ${label}` : `Score: ${score}`;
                }
              }
            }
          },
          scales: {
            x: { type: 'time', time: { unit: 'day', tooltipFormat: 'MMM D, YYYY', displayFormats: { day: 'MMM D' }},
                 title: { display: true, text: 'Scheduled close date' }},
            y: { title: { display: true, text: 'Peer score' }}
          }
        }
      });
    }
    // ─── Sorting ──────────────────────────────────────────────────────────
    // Default: ID descending (matches the server-side default order, but
    // this makes it explicit and restorable after any other sort).
    const SORT_KEY = 'meta_dashboard_sort_v1';
    let sortState = { key: 'id', dir: 'desc' };
    try {
      const saved = JSON.parse(localStorage.getItem(SORT_KEY) || 'null');
      if (saved && saved.key) sortState = saved;
    } catch(e) {}

    function sortValue(tr, key) {
      const raw = tr.dataset['sort' + key.charAt(0).toUpperCase() + key.slice(1)];
      return raw === undefined ? '' : raw;
    }
    function compareRows(a, b, key, type, dir) {
      let va = sortValue(a, key), vb = sortValue(b, key);
      const aEmpty = va === '', bEmpty = vb === '';
      // Empty values always sort last, regardless of asc/desc — an
      // unknown "last predicted" shouldn't jump to the top just because
      // the sort direction flipped.
      if (aEmpty && bEmpty) return 0;
      if (aEmpty) return 1;
      if (bEmpty) return -1;
      if (type === 'num') { va = parseFloat(va); vb = parseFloat(vb); }
      let cmp = va < vb ? -1 : (va > vb ? 1 : 0);
      return dir === 'desc' ? -cmp : cmp;
    }
    function applySort() {
      const th = document.querySelector('th[data-key="' + sortState.key + '"]');
      const type = th ? th.dataset.type : 'str';
      const tbody = document.querySelector('#rowsTable tbody');
      const rows = [...tbody.querySelectorAll('tr')];
      rows.sort((a, b) => compareRows(a, b, sortState.key, type, sortState.dir));
      rows.forEach(tr => tbody.appendChild(tr));
      document.querySelectorAll('th.sortable').forEach(h => {
        h.classList.remove('sorted');
        const arrow = h.querySelector('.arrow');
        if (h.dataset.key === sortState.key) {
          h.classList.add('sorted');
          arrow.textContent = sortState.dir === 'desc' ? '▾' : '▴';
        } else {
          arrow.textContent = '';
        }
      });
    }
    document.querySelectorAll('th.sortable').forEach(th => {
      th.addEventListener('click', () => {
        if (sortState.key === th.dataset.key) {
          sortState.dir = sortState.dir === 'desc' ? 'asc' : 'desc';
        } else {
          sortState = { key: th.dataset.key, dir: 'desc' };
        }
        localStorage.setItem(SORT_KEY, JSON.stringify(sortState));
        applySort();
        currentPage = 1;
        applyFilters();
      });
    });

    // ─── Pagination ───────────────────────────────────────────────────────
    const PAGE_SIZE = 20;
    let currentPage = 1;

    function applyFilters() {
      const selT = getSelectedTournaments();
      const selS = getSelected('statusPills');
      const selG = getSelected('signalPills');
      const onlyRefreshCandidates = selG.has('refresh_candidate');
      const onlyBatchRefreshDue = selG.has('batch_refresh_due');
      const searchTerm = document.getElementById('searchBox').value.trim().toLowerCase();
      const matched = [];
      document.querySelectorAll('#rowsTable tbody tr').forEach(tr => {
        const tours = tr.dataset.tournaments.split(',');
        const tMatch = selT.size === 0 || tours.some(t => selT.has(t));
        const sMatch = selS.size === 0 || selS.has(tr.dataset.status);
        const gMatch = (!onlyRefreshCandidates || tr.dataset.refresh === 'true') &&
                       (!onlyBatchRefreshDue || tr.dataset.batchRefreshDue === 'true');
        // data-search is pre-lowercased at render time (Jinja |lower filter) —
        // for a group row it's the search_blob (every member's own text/id
        // concatenated), so searching a specific sub-question's date range or
        // question_id still surfaces its parent group row, even though that
        // text isn't in the group's own (de-suffixed) title.
        const qMatch = !searchTerm || tr.dataset.search.includes(searchTerm);
        if (tMatch && sMatch && gMatch && qMatch) matched.push(tr); else tr.style.display = 'none';
      });

      const totalPages = Math.max(1, Math.ceil(matched.length / PAGE_SIZE));
      if (currentPage > totalPages) currentPage = totalPages;
      const start = (currentPage - 1) * PAGE_SIZE;
      const end = start + PAGE_SIZE;

      matched.forEach((tr, i) => { tr.style.display = (i >= start && i < end) ? '' : 'none'; });

      const total = document.querySelectorAll('#rowsTable tbody tr').length;
      document.getElementById('showingCount').textContent =
        'Showing ' + Math.min(matched.length, PAGE_SIZE) + ' of ' + matched.length + ' filtered (' + total + ' total)';
      document.getElementById('pageInfo').textContent = 'Page ' + currentPage + ' of ' + totalPages;
      document.getElementById('prevPage').disabled = currentPage <= 1;
      document.getElementById('nextPage').disabled = currentPage >= totalPages;

      renderChart(selT);
    }
    document.getElementById('prevPage').addEventListener('click', () => {
      if (currentPage > 1) { currentPage--; applyFilters(); window.scrollTo({top: 0, behavior: 'smooth'}); }
    });
    document.getElementById('nextPage').addEventListener('click', () => {
      currentPage++; applyFilters(); window.scrollTo({top: 0, behavior: 'smooth'});
    });
    document.querySelectorAll('.pill').forEach(p =>
      p.addEventListener('click', () => { p.classList.toggle('active'); saveFilters(); currentPage = 1; applyFilters(); })
    );
    document.getElementById('searchBox').addEventListener('input', () => {
      saveFilters(); currentPage = 1; applyFilters();
    });
    document.getElementById('clearFilters').addEventListener('click', () => {
      document.querySelectorAll('.pill.active').forEach(p => p.classList.remove('active'));
      document.getElementById('searchBox').value = '';
      saveFilters(); currentPage = 1; applyFilters();
    });
    restoreFilters();
    applySort();
    applyFilters();
    // CHANGED 2026-07-16: was an unconditional setTimeout(reload, 300000) —
    // this is a plain in-memory JS Set, reset by any full page load, so a
    // reload landing mid-way through checking boxes for a refresh-candidate
    // review silently wiped out the whole in-progress selection with no
    // warning. Now polls every 15s and only actually reloads once 5
    // minutes have passed AND nothing is currently selected — auto-refresh
    // still happens on its own the moment it's safe to, it just waits
    // rather than steamrolling an active selection. The manual "🔄 Refresh
    // now" button (added same day) covers the case where Mike wants to
    // force a reload immediately regardless of either condition.
    let lastAutoRefresh = Date.now();
    setInterval(() => {
      const fiveMinutesPassed = (Date.now() - lastAutoRefresh) >= 300000;
      if (fiveMinutesPassed && selectedIds.size === 0) {
        location.reload();
      }
    }, 15000);

    // ─── Manual-selection refresh (2026-07-14) ────────────────────────────
    // Rough per-question cost estimate, matching the same figure used
    // elsewhere in this codebase's dry-run cost estimates (~1 research
    // call + 1 forecast call, Haiku, system prompt cached where it clears
    // the token floor). Deliberately approximate -- this is a sanity-check
    // number for the confirm dialog, not a billing guarantee.
    const EST_COST_PER_QUESTION_LOW = 0.025;
    const EST_COST_PER_QUESTION_HIGH = 0.03;
    // ADDED 2026-07-15 (Step 3): for questions routed to
    // meta_refresh_forecast.py instead — matches that file's own --submit
    // dry-run estimate exactly (already includes its 50% batch discount).
    const EST_COST_PER_QUESTION_BATCH = 0.05;
    const selectedIds = new Set();

    function updateSelectionBar() {
      const bar = document.getElementById('selectionBar');
      const count = selectedIds.size;
      if (count === 0) {
        bar.style.display = 'none';
        return;
      }
      bar.style.display = 'flex';
      const lowCost = (count * EST_COST_PER_QUESTION_LOW).toFixed(2);
      const highCost = (count * EST_COST_PER_QUESTION_HIGH).toFixed(2);
      document.getElementById('selectionCount').textContent =
        count + ' selected (est. $' + lowCost + '–$' + highCost + ')';
    }

    function toggleRowSelect(checkbox) {
      const id = parseInt(checkbox.value, 10);
      if (checkbox.checked) selectedIds.add(id); else selectedIds.delete(id);
      updateSelectionBar();
    }

    // ADDED 2026-07-15: inline editor for a batch-path question's
    // refresh_after date. Swaps the display span + edit button for a date
    // input with Save/Clear/Cancel, mirroring the pattern this codebase
    // already uses elsewhere (see /refresh route's own docstring):
    // no live in-place recompute of the due-status/badge — on success this
    // just reloads the page, same "eventually consistent, refresh to see
    // it" philosophy as the 5-minute cache cycle. Simpler and more robust
    // than trying to replicate is_due_for_refresh's logic in JS too.
    function editRefreshAfter(questionId, currentDateStr) {
      const cell = document.getElementById('refresh-display-' + questionId).closest('td');
      const editBtn = cell.querySelector('.edit-refresh-btn');
      cell.innerHTML =
        '<input type="date" id="refresh-input-' + questionId + '" value="' + (currentDateStr || '') + '" style="font-size:12px;padding:2px;">' +
        '<button onclick="saveRefreshAfter(' + questionId + ')" style="font-size:11px;margin-left:4px;cursor:pointer;">Save</button>' +
        '<button onclick="clearRefreshOverride(' + questionId + ')" style="font-size:11px;margin-left:2px;cursor:pointer;" title="Reset to default (last forecast + 30 days)">Reset</button>' +
        '<button onclick="location.reload()" style="font-size:11px;margin-left:2px;cursor:pointer;">Cancel</button>';
    }
    function saveRefreshAfter(questionId) {
      const input = document.getElementById('refresh-input-' + questionId);
      if (!input.value) { alert('Pick a date first.'); return; }
      fetch('/set_refresh_after', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({question_id: questionId, refresh_after: input.value})
      }).then(r => r.json()).then(data => {
        if (data.ok) {
          // ADDED 2026-07-15: a date past this question's close_time gets
          // clamped down to close_time server-side (see /set_refresh_after)
          // — worth a heads-up rather than silently landing on a different
          // date than what was actually typed in.
          if (data.clamped_to_close) {
            alert('That date was after this question\\'s close date, so it was set to the close date instead.');
          }
          location.reload();
        }
        else alert('Failed to save: ' + (data.error || 'unknown error'));
      }).catch(e => alert('Failed to save: ' + e));
    }
    function clearRefreshOverride(questionId) {
      fetch('/set_refresh_after', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({question_id: questionId, refresh_after: null})
      }).then(r => r.json()).then(data => {
        if (data.ok) location.reload();
        else alert('Failed to clear: ' + (data.error || 'unknown error'));
      }).catch(e => alert('Failed to clear: ' + e));
    }

    document.getElementById('selectAllVisible').addEventListener('change', (e) => {
      // Only affects currently-visible rows with a checkbox (i.e. non-group
      // rows not hidden by the active filter/search/pagination) -- matches
      // the same "act on what you can see" convention as the filter pills.
      document.querySelectorAll('#rowsTable tbody tr:not([style*="display: none"]) .row-select')
        .forEach(cb => {
          cb.checked = e.target.checked;
          toggleRowSelect(cb);
        });
    });

    document.getElementById('clearSelection').addEventListener('click', () => {
      selectedIds.clear();
      document.querySelectorAll('.row-select').forEach(cb => cb.checked = false);
      document.getElementById('selectAllVisible').checked = false;
      updateSelectionBar();
    });

    document.getElementById('refreshSelected').addEventListener('click', () => {
      const count = selectedIds.size;
      if (count === 0) return;
      // ADDED 2026-07-15 (Step 3): figure out which pipeline each selected
      // id will launch under BEFORE confirming, so the dialog's cost
      // estimate and description are honest instead of always assuming
      // tournament_forecast_v2.py. Keyed off each checkbox's own `value`
      // (same id stored in selectedIds), not the row's `id` HTML attribute
      // -- those aren't always the same number (post_id vs question_id can
      // differ for a given question), so this is the only reliable lookup.
      const batchPathById = {};
      document.querySelectorAll('.row-select').forEach(cb => {
        batchPathById[cb.value] = cb.closest('tr').dataset.batchPath === 'true';
      });
      let v2Count = 0, batchCount = 0;
      selectedIds.forEach(id => { if (batchPathById[id]) batchCount++; else v2Count++; });

      // EST_COST_PER_QUESTION_LOW/HIGH is tuned for tournament_forecast_v2's
      // synchronous per-question cost; EST_COST_PER_QUESTION_BATCH matches
      // meta_refresh_forecast.py's own dry-run estimate (already includes
      // the 50% batch discount — see that file's --submit path).
      const lowCost = (v2Count * EST_COST_PER_QUESTION_LOW + batchCount * EST_COST_PER_QUESTION_BATCH).toFixed(2);
      const highCost = (v2Count * EST_COST_PER_QUESTION_HIGH + batchCount * EST_COST_PER_QUESTION_BATCH).toFixed(2);
      const pipelineNote = batchCount === 0
        ? 'This will run tournament_forecast_v2.py in the background — check the terminal or reload this dashboard in a few minutes to see results.'
        : v2Count === 0
          ? 'This will submit ' + batchCount + ' question(s) to meta_refresh_forecast.py as a batch job — results land after the next --check run, not immediately.'
          : (v2Count + ' question(s) will run via tournament_forecast_v2.py (results in a few minutes); ' +
             batchCount + ' will be submitted to meta_refresh_forecast.py as a batch job (results after the next --check run).');
      const ok = confirm(
        'Refresh ' + count + ' question(s)?\\n\\n' +
        'Estimated cost: $' + lowCost + '–$' + highCost + '\\n\\n' +
        pipelineNote
      );
      if (!ok) return;

      const btn = document.getElementById('refreshSelected');
      btn.disabled = true;
      btn.textContent = 'Launching…';
      fetch('/refresh', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ids: Array.from(selectedIds)})
      })
        .then(r => r.json())
        .then(data => {
          if (data.ok) {
            // ADDED 2026-07-15 (Step 3): the response now reports which
            // pipeline each id actually launched under -- surface that
            // honestly rather than one generic message, since the two
            // pipelines have very different turnaround (v2 finishes
            // within this run; batch-path only SUBMITS now, results land
            // after the next --check).
            const v2 = (data.launched && data.launched.tournament_forecast_v2) || [];
            const batch = (data.launched && data.launched.meta_refresh_forecast) || [];
            const parts = [];
            if (v2.length) parts.push(v2.length + ' via tournament_forecast_v2 (results in a few minutes)');
            if (batch.length) parts.push(batch.length + ' via meta_refresh_forecast batch submission (results after the next --check run)');
            let msg = 'Launched — ' + parts.join('; ') + '.';
            if (data.errors && data.errors.length) msg += '\\n\\nSome launches failed: ' + data.errors.join('; ');
            alert(msg);
            selectedIds.clear();
            document.querySelectorAll('.row-select').forEach(cb => cb.checked = false);
            document.getElementById('selectAllVisible').checked = false;
            updateSelectionBar();
          } else {
            alert('Failed to launch: ' + (data.error || 'unknown error'));
          }
        })
        .catch(err => alert('Failed to launch: ' + err))
        .finally(() => {
          btn.disabled = false;
          btn.textContent = 'Refresh Selected';
        });
    });
  </script>
</body>
</html>
"""

DETAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Question {{ qid }} — Detail</title>
  <style>
    body { font-family: -apple-system, Segoe UI, Arial, sans-serif; margin: 0; padding: 32px;
           background: #f7f8fa; color: #1a1a1a; max-width: 900px; }
    h1 { font-size: 18px; margin: 0 0 6px; line-height: 1.4; }
    .back { font-size: 13px; color: #888; margin-bottom: 20px; display: block; }
    .meta-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; font-size: 13px; }
    .badge { background: #eef0f3; color: #555; border-radius: 4px; padding: 2px 8px; }
    .badge.personal { background: #fef3c7; color: #92400e; }
    .badge.status-open { background: #dcfce7; color: #166534; }
    .badge.status-closed { background: #fef9c3; color: #854d0e; }
    .badge.status-resolved { background: #dbeafe; color: #1e40af; }
    .scores { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
    .score-card { background: white; border-radius: 8px; padding: 14px 18px;
                  box-shadow: 0 1px 3px rgba(0,0,0,.08); min-width: 110px; }
    .score-card .label { font-size: 11px; color: #999; margin-bottom: 4px; }
    .score-card .value { font-size: 20px; font-weight: 600; }
    .pos { color: #16a34a; } .neg { color: #dc2626; }
    section { background: white; border-radius: 8px; padding: 20px 24px; margin-bottom: 16px;
              box-shadow: 0 1px 3px rgba(0,0,0,.08); }
    section h2 { font-size: 13px; color: #999; text-transform: uppercase; letter-spacing: .04em;
                 margin: 0 0 12px; font-weight: 600; }
    .reasoning-text { white-space: pre-wrap; font-size: 13px; line-height: 1.7; color: #333;
                      max-height: 600px; overflow-y: auto; }
    .research-text  { white-space: pre-wrap; font-size: 13px; line-height: 1.7; color: #444;
                      max-height: 400px; overflow-y: auto; }
    details summary { font-size: 13px; color: #888; cursor: pointer; padding: 4px 0; }
    details pre { background: #f4f4f5; border-radius: 6px; padding: 12px; font-size: 11px;
                  overflow-x: auto; max-height: 500px; overflow-y: auto; white-space: pre-wrap; }
    .empty { color: #999; font-style: italic; font-size: 13px; }
    .cp-na { color: #ccc; font-size: 12px; }
    .badge.refresh { background: #fef3c7; color: #92400e; }
    .history-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .history-table th, .history-table td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #eee; }
    .history-table th { color: #999; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: .03em; }
    .history-table tr:first-child td { font-weight: 600; }
  </style>
</head>
<body>
  <a class="back" href="/">← Back to dashboard</a>
  <h1>
    {% if post_id %}
      <a href="https://www.metaculus.com/questions/{{ post_id }}/" target="_blank"
         style="color:inherit;text-decoration:none;border-bottom:1px solid #ccc;">{{ title }}</a>
    {% else %}
      {{ title }}
    {% endif %}
  </h1>

  <div class="meta-row">
    <span class="badge">ID {{ qid }}</span>
    <span class="badge">{{ q_type or 'unknown type' }}</span>
    {% for t in tournaments %}
    <span class="badge {{ 'personal' if t == 'Personal' else '' }}">{{ t }}</span>
    {% endfor %}
    <span class="badge status-{{ status_class }}">{{ status_label }}</span>
    {% if close_time %}<span class="badge">Closes {{ close_time[:10] }}</span>{% endif %}
    {% if resolve_time %}<span class="badge">Resolved {{ resolve_time[:10] }}</span>{% endif %}
    {% if is_refresh_candidate %}
    <span class="badge refresh" title="{{ refresh_alert_reasons|join(', ') }}">🔄 Refresh candidate</span>
    {% endif %}
    {% if is_refresh_excluded %}
    <span class="badge refresh" title="{{ refresh_exclusion_reason }}">🚫 Excluded from refresh</span>
    {% endif %}
  </div>

  <div class="scores">
    <div class="score-card">
      <div class="label">Submitted</div>
      <div class="value">{{ submitted_summary }}</div>
    </div>
    {% if original_prob is not none %}
    <div class="score-card">
      <div class="label">Original</div>
      <div class="value">{{ "%.0f%%"|format(original_prob * 100) }}</div>
    </div>
    {% endif %}
    <div class="score-card">
      <div class="label">CP at access</div>
      <div class="value">
        {% if cp_summary != '—' %}{{ cp_summary }}
        {% else %}<span class="cp-na" title="Bots excluded from community aggregate">n/a</span>
        {% endif %}
      </div>
    </div>
    <div class="score-card">
      <div class="label">Resolution</div>
      <div class="value">{{ resolution or '—' }}</div>
    </div>
    <div class="score-card">
      <div class="label">Peer score</div>
      <div class="value {{ 'pos' if peer_score and peer_score > 0 else ('neg' if peer_score and peer_score < 0 else '') }}">
        {{ '%.2f'|format(peer_score) if peer_score is not none else '—' }}
      </div>
    </div>
    {% if baseline_score is not none %}
    <div class="score-card">
      <div class="label">Baseline score</div>
      <div class="value {{ 'pos' if baseline_score > 0 else 'neg' }}">{{ '%.2f'|format(baseline_score) }}</div>
    </div>
    {% endif %}
    {% if refresh_reason %}
    <div class="score-card" style="min-width:200px;">
      <div class="label">Refresh reason</div>
      <div class="value" style="font-size:13px;font-weight:400;">{{ refresh_reason }}</div>
    </div>
    {% endif %}
  </div>

  <section>
    <h2>Prediction history</h2>
    {% if prediction_history %}
    <table class="history-table">
      <thead><tr><th>Date</th><th>Prediction</th></tr></thead>
      <tbody>
        {% for h in prediction_history %}
        <tr>
          <td>{{ h.date_iso[:16].replace('T', ' ') if h.date_iso else 'unknown date' }}</td>
          <td>{{ h.submitted_summary }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
    <p class="empty">No prediction history found in local batch results for this question.</p>
    {% endif %}
  </section>

  {% if reasoning %}
  <section>
    <h2>Reasoning</h2>
    <div class="reasoning-text">{{ reasoning }}</div>
  </section>
  {% else %}
  <section><h2>Reasoning</h2><p class="empty">No reasoning stored for this question.</p></section>
  {% endif %}

  {% if research_text %}
  <section>
    <h2>Research{% if research_source %} <span style="font-weight:400;font-size:14px;color:#666;">(via {{ research_source }})</span>{% endif %}</h2>
    <div class="research-text">{{ research_text }}</div>
  </section>
  {% endif %}

  <section>
    <details>
      <summary>Raw API JSON</summary>
      <pre>{{ raw_json }}</pre>
    </details>
  </section>
</body>
</html>
"""

GROUP_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{{ group_title }} — Group</title>
  <style>
    body { font-family: -apple-system, Segoe UI, Arial, sans-serif; margin: 0; padding: 32px;
           background: #f7f8fa; color: #1a1a1a; max-width: 900px; }
    h1 { font-size: 18px; margin: 0 0 6px; line-height: 1.4; }
    .back { font-size: 13px; color: #888; margin-bottom: 20px; display: block; }
    .meta-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; font-size: 13px; }
    .badge { background: #eef0f3; color: #555; border-radius: 4px; padding: 2px 8px; }
    .badge.status-open { background: #dcfce7; color: #166534; }
    .badge.status-closed { background: #fef9c3; color: #854d0e; }
    .badge.status-resolved { background: #dbeafe; color: #1e40af; }
    section { background: white; border-radius: 8px; padding: 20px 24px; margin-bottom: 16px;
              box-shadow: 0 1px 3px rgba(0,0,0,.08); }
    section h2 { font-size: 13px; color: #999; text-transform: uppercase; letter-spacing: .04em;
                 margin: 0 0 12px; font-weight: 600; }
    .pos { color: #16a34a; } .neg { color: #dc2626; }
    .members-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .members-table th, .members-table td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #eee; }
    .members-table th { color: #999; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: .03em; }
    .members-table a { color: #2563eb; text-decoration: none; }
    .members-table a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <a class="back" href="/">← Back to dashboard</a>
  <h1>
    🗂️
    {% if post_id %}
      <a href="https://www.metaculus.com/questions/{{ post_id }}/" target="_blank"
         style="color:inherit;text-decoration:none;border-bottom:1px solid #ccc;">{{ group_title }}</a>
    {% else %}
      {{ group_title }}
    {% endif %}
  </h1>

  <div class="meta-row">
    <span class="badge">Group post {{ post_id }}</span>
    <span class="badge">{{ members|length }} sub-questions</span>
    {% for t in tournaments %}
    <span class="badge">{{ t }}</span>
    {% endfor %}
  </div>

  <section>
    <h2>Sub-questions</h2>
    <table class="members-table">
      <thead>
        <tr>
          <th style="width:26px;"></th>
          <th>Period</th>
          <th>Status</th>
          <th>Submitted</th>
          <th>Peer score</th>
          <th>Last predicted</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {% for m in members %}
        <tr>
          <td>
            {% if m.status_bucket == 'open' and not m.is_refresh_excluded %}
              <input type="checkbox" class="member-select" value="{{ m.question_id }}"
                     onclick="toggleMemberSelect(this)">
            {% endif %}
          </td>
          <td>{{ m.period_label }}</td>
          <td><span class="badge status-{{ m.status_class }}">{{ m.status_label }}</span></td>
          <td>{{ m.submitted_summary }}</td>
          <td class="{{ 'pos' if m.peer_score and m.peer_score > 0 else ('neg' if m.peer_score and m.peer_score < 0 else '') }}">
            {{ '%.2f'|format(m.peer_score) if m.peer_score is not none else '—' }}
          </td>
          <td>{{ m.predicted_at[:10] if m.predicted_at else '—' }}</td>
          <td><a href="/detail/{{ m.question_id }}" target="_blank">detail</a></td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </section>

  <div id="selectionBar" style="display:none;position:fixed;bottom:0;left:0;right:0;
       background:#1a1a1a;color:white;padding:12px 24px;align-items:center;
       gap:16px;box-shadow:0 -2px 8px rgba(0,0,0,.2);z-index:1000;">
    <span id="selectionCount"></span>
    <button id="refreshSelected" style="background:#2563eb;color:white;border:none;
            border-radius:4px;padding:6px 14px;cursor:pointer;font-size:13px;">Refresh Selected</button>
    <button id="clearSelection" style="background:transparent;color:#ccc;border:1px solid #555;
            border-radius:4px;padding:6px 14px;cursor:pointer;font-size:13px;">Clear</button>
  </div>

  <script>
    // Same manual-selection-refresh pattern as the main dashboard page
    // (meta_dashboard.py's PAGE_TEMPLATE) — see that copy's comment for
    // the full reasoning. Here every checkbox carries a sub-question's
    // own question_id (not the shared post_id), since that's exactly the
    // precision this page exists for — checking the group's post_id
    // elsewhere would select every sub-question in it, which is why the
    // main table's collapsed group rows deliberately have no checkbox at
    // all and only this page's individual member rows do.
    const EST_COST_PER_QUESTION_LOW = 0.025;
    const EST_COST_PER_QUESTION_HIGH = 0.03;
    const selectedIds = new Set();

    function updateSelectionBar() {
      const bar = document.getElementById('selectionBar');
      const count = selectedIds.size;
      if (count === 0) {
        bar.style.display = 'none';
        return;
      }
      bar.style.display = 'flex';
      const lowCost = (count * EST_COST_PER_QUESTION_LOW).toFixed(2);
      const highCost = (count * EST_COST_PER_QUESTION_HIGH).toFixed(2);
      document.getElementById('selectionCount').textContent =
        count + ' selected (est. $' + lowCost + '–$' + highCost + ')';
    }

    function toggleMemberSelect(checkbox) {
      const id = parseInt(checkbox.value, 10);
      if (checkbox.checked) selectedIds.add(id); else selectedIds.delete(id);
      updateSelectionBar();
    }

    document.getElementById('clearSelection').addEventListener('click', () => {
      selectedIds.clear();
      document.querySelectorAll('.member-select').forEach(cb => cb.checked = false);
      updateSelectionBar();
    });

    document.getElementById('refreshSelected').addEventListener('click', () => {
      const count = selectedIds.size;
      if (count === 0) return;
      const lowCost = (count * EST_COST_PER_QUESTION_LOW).toFixed(2);
      const highCost = (count * EST_COST_PER_QUESTION_HIGH).toFixed(2);
      const ok = confirm(
        'Refresh ' + count + ' sub-question(s)?\\n\\n' +
        'Estimated cost: $' + lowCost + '–$' + highCost + '\\n\\n' +
        'This will run tournament_forecast_v2.py in the background — ' +
        'check the terminal or reload the dashboard in a few minutes to see results.'
      );
      if (!ok) return;

      const btn = document.getElementById('refreshSelected');
      btn.disabled = true;
      btn.textContent = 'Launching…';
      fetch('/refresh', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ids: Array.from(selectedIds)})
      })
        .then(r => r.json())
        .then(data => {
          if (data.ok) {
            alert('Launched — forecasting ' + data.count + ' question(s) in the background.');
            selectedIds.clear();
            document.querySelectorAll('.member-select').forEach(cb => cb.checked = false);
            updateSelectionBar();
          } else {
            alert('Failed to launch: ' + (data.error || 'unknown error'));
          }
        })
        .catch(err => alert('Failed to launch: ' + err))
        .finally(() => {
          btn.disabled = false;
          btn.textContent = 'Refresh Selected';
        });
    });
  </script>
</body>
</html>
"""

LOADING_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="utf-8"><title>Loading…</title>
<meta http-equiv="refresh" content="3"></head>
<body style="font-family:-apple-system,sans-serif;padding:40px;color:#666;">
  <h2>Loading first batch of data…</h2>
  <p>This page refreshes itself every 3 seconds.</p>
  {% if cache_error %}<p style="color:#dc2626;">Last attempt failed: {{ cache_error }}</p>{% endif %}
</body></html>
"""


# ─── Routes ──────────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    with CACHE_LOCK:
        data       = CACHE["data"]
        last_r     = CACHE["last_refresh"]
        cache_err  = CACHE["error"]
    if data is None:
        return render_template_string(LOADING_TEMPLATE, cache_error=cache_err)
    return render_template_string(
        PAGE_TEMPLATE,
        data=data,
        chart_bot=data["chart_bot"],
        chart_personal=data["chart_personal"],
        status_order=STATUS_ORDER,
        status_labels=STATUS_LABELS,
        last_refresh=last_r.strftime("%Y-%m-%d %H:%M:%S UTC") if last_r else "never",
        cache_error=cache_err,
    )


@app.route("/detail/<int:question_id>")
def detail(question_id):
    with CACHE_LOCK:
        data                = CACHE["data"]
        live_by_qid         = CACHE["live_by_qid"]
        personal_live_by_qid = CACHE["personal_live_by_qid"]
        local_by_qid        = CACHE["local_by_qid"]

    if data is None:
        return render_template_string(LOADING_TEMPLATE, cache_error=CACHE["error"])

    # Find the row for this question. Direct lookup covers every
    # standalone question and any group's OWN post_id (shouldn't normally
    # be dereferenced directly, but harmless if it is). Group MEMBER rows
    # (2026-07-12+) are no longer in data["rows"] directly — they were
    # collapsed into their group's summary row — so fall back to
    # searching every group's member list before giving up.
    row = next((r for r in data["rows"] if r["question_id"] == question_id), None)
    if row is None:
        for members in data.get("groups_by_post_id", {}).values():
            row = next((m for m in members if m["question_id"] == question_id), None)
            if row is not None:
                break
    if row is None:
        return f"Question {question_id} not found in dashboard data.", 404

    # Prefer bot live data; fall back to personal for personal-only questions
    raw = live_by_qid.get(question_id) or personal_live_by_qid.get(question_id) or {}

    status_class = {
        "open": "open",
        "closed_unresolved": "closed",
        "resolved_scored": "resolved",
        "resolved_unscored": "resolved",
        "not_found_live": "closed",
    }.get(row["status_bucket"], "closed")

    prediction_history = load_prediction_history(question_id, LOCAL_RESULT_DIRS)

    return render_template_string(
        DETAIL_TEMPLATE,
        qid=question_id,
        post_id=row.get("post_id"),
        title=row["question_text"],
        q_type=row["question_type"],
        tournaments=row["tournaments"],
        status_label=row["status_label"],
        status_class=status_class,
        close_time=row["close_time"],
        resolve_time=row["resolve_time"],
        submitted_summary=row["submitted_summary"],
        original_prob=row["original_prob"],
        cp_summary=row["cp_summary"],
        cp_available=row["cp_available"],
        resolution=row["resolution"],
        peer_score=row["peer_score"],
        baseline_score=row["baseline_score"],
        refresh_reason=row["refresh_reason"],
        reasoning=row["reasoning"],
        research_text=row["research_text"],
        research_source=row["research_source"],
        is_refresh_candidate=row["is_refresh_candidate"],
        refresh_alert_reasons=row["refresh_alert_reasons"],
        is_refresh_excluded=row["is_refresh_excluded"],
        refresh_exclusion_reason=row["refresh_exclusion_reason"],
        prediction_history=prediction_history,
        raw_json=json.dumps(raw, indent=2, default=str),
    )


_STATUS_CLASS_MAP = {
    "open": "open",
    "closed_unresolved": "closed",
    "resolved_scored": "resolved",
    "resolved_unscored": "resolved",
    "not_found_live": "closed",
}


@app.route("/group/<int:post_id>")
def group_detail(post_id):
    with CACHE_LOCK:
        data = CACHE["data"]
    if data is None:
        return render_template_string(LOADING_TEMPLATE, cache_error=CACHE["error"])

    members = data.get("groups_by_post_id", {}).get(post_id)
    if not members:
        return f"Group post {post_id} not found in dashboard data.", 404

    members_sorted = sorted(members, key=lambda m: m["question_id"])
    # Common title (same de-suffixing as _make_group_row) for the page
    # heading — recomputed here rather than stored, since it's cheap and
    # keeps this route self-contained.
    titles = [re.sub(r"\s*\([^)]*\)\s*$", "", m["question_text"]).strip() or m["question_text"]
              for m in members_sorted]
    group_title = max(set(titles), key=titles.count)

    members_view = []
    for m in members_sorted:
        # Period label: whatever's inside the trailing "(...)" that
        # _make_group_row strips off to build the common title — e.g.
        # "Jul 13 - Jul 24". Falls back to the full question text if a
        # member genuinely has no such suffix (shouldn't happen for a
        # real group, but better than showing nothing).
        match = re.search(r"\(([^)]*)\)\s*$", m["question_text"])
        period_label = match.group(1) if match else m["question_text"][:40]
        members_view.append({
            **m,
            "period_label": period_label,
            "status_class": _STATUS_CLASS_MAP.get(m["status_bucket"], "closed"),
        })

    tournaments = members_sorted[0]["tournaments"] if members_sorted else []

    return render_template_string(
        GROUP_TEMPLATE,
        post_id=post_id,
        group_title=group_title,
        tournaments=tournaments,
        members=members_view,
    )


@app.route("/raw/<int:question_id>")
def raw_json(question_id):
    with CACHE_LOCK:
        live_by_qid          = CACHE["live_by_qid"]
        personal_live_by_qid = CACHE["personal_live_by_qid"]
    raw = live_by_qid.get(question_id) or personal_live_by_qid.get(question_id)
    if raw:
        return jsonify(raw)
    return jsonify({"_error": f"Question {question_id} not found in either account's live data."})


def _classify_refresh_ids(ids: list[int]) -> tuple[list[int], list[int]]:
    """Split a list of post_ids/question_ids into (v2_ids, batch_ids) --
    ADDED 2026-07-15 (Step 3). v2_ids go to tournament_forecast_v2.py
    (FutureEval/Market Pulse), batch_ids go to meta_refresh_forecast.py
    (ACX2026/Climate/Metaculus Cup/question_series). Uses the SAME
    is_batch_path signal _make_row computes per row — tournament-membership
    based (see that function's docstring for why this replaced an earlier
    file-provenance-based version same day, after Q43347/Metaculus Cup
    showed the gap live: a question with no local result file anywhere had
    nothing to detect provenance from, and silently defaulted to the wrong
    pipeline).

    Matches on EITHER post_id or question_id per id, mirroring
    tournament_forecast_v2.py's own --ids= dual-matching (group-page member
    checkboxes send question_id; main-table checkboxes send post_id — see
    refresh_selected's docstring).

    Anything not found in the current cache defaults to v2_ids — preserves
    this route's original all-goes-to-v2 behavior as a safe fallback for
    an id the cache happens to be momentarily missing, rather than
    silently dropping it."""
    with CACHE_LOCK:
        data = CACHE["data"]
    rows = data["rows"] if data else []

    batch_path_ids = set()
    for row in rows:
        if row.get("is_batch_path"):
            if row.get("post_id") is not None:
                batch_path_ids.add(row["post_id"])
            if row.get("question_id") is not None:
                batch_path_ids.add(row["question_id"])

    v2_ids, batch_ids = [], []
    for i in ids:
        (batch_ids if i in batch_path_ids else v2_ids).append(i)
    return v2_ids, batch_ids


@app.route("/refresh", methods=["POST"])
def refresh_selected():
    """
    Added 2026-07-14 for the dashboard manual-selection UI. Accepts a JSON
    body {"ids": [44457, 44679, ...]} -- a mix of post_ids (from the main
    table's standalone-question checkboxes) and question_ids (from a
    group's /group/<post_id> detail page sub-question checkboxes) is fine,
    since tournament_forecast_v2.py's --ids flag uses the exact same dual
    post_id-or-question_id matching either way -- see that flag's own
    comment for why checking a whole group's post_id would incorrectly
    select every sub-question in it, which is why group rows on the main
    table deliberately have no checkbox at all; only the group detail
    page's individual member rows do.

    CHANGED 2026-07-15 (Step 3): previously this ALWAYS spawned
    tournament_forecast_v2.py regardless of what was selected -- fine when
    only FutureEval/Market Pulse rows had checkboxes that meant anything,
    but wrong now that the batch-path tournaments' rows are just as
    selectable and that script has no idea those tournaments exist. Now
    splits the incoming ids via _classify_refresh_ids and can launch BOTH
    scripts in the same request if the selection spans both pipelines --
    each spawned independently, so one failing to launch doesn't block the
    other. Note the batch-path launch only SUBMITS a batch job (mirrors
    Mike running `meta_refresh_forecast.py --ids=...` by hand) -- results
    don't land until the next --check run (still on its own automated
    cron, not triggered by this route), unlike tournament_forecast_v2.py's
    synchronous --ids= which finishes and submits within the same run.
    The response's "launched" breakdown lets the frontend say this
    honestly instead of implying both finish equally fast.

    Spawns each script's --ids=... as a background subprocess (Popen, not
    run/check_output -- this route must return immediately, not block for
    however long the actual forecast run takes) and returns right away.
    Uses sys.executable so the subprocess runs under the SAME interpreter
    (and therefore the same venv312 environment) as this dashboard
    process, not whatever "python" happens to resolve to on PATH. There's
    no live streaming of either subprocess's output back to the browser in
    this first pass -- check the dashboard's own 5-minute cache refresh,
    or the terminal/log file, to see the result.

    SAFETY: this endpoint does NOT itself gate on cost or confirm anything
    -- that happens client-side (a confirm() dialog showing the selected
    count and a rough cost estimate) before this is ever called. This
    dashboard is assumed to run on localhost only; if it's ever exposed
    beyond that, this client-side-only gate would need to move server-side
    too -- flagging that assumption now rather than letting it go unstated.
    """
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids", [])
    if not ids or not isinstance(ids, list):
        return jsonify({"ok": False, "error": "No ids provided"}), 400
    try:
        ids = [int(i) for i in ids]
    except (ValueError, TypeError):
        return jsonify({"ok": False, "error": "ids must be a list of integers"}), 400

    v2_ids, batch_ids = _classify_refresh_ids(ids)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    launched = {}
    errors = []

    if v2_ids:
        ids_str = ",".join(str(i) for i in v2_ids)
        script_path = os.path.join(script_dir, "tournament_forecast_v2.py")
        try:
            subprocess.Popen([sys.executable, script_path, f"--ids={ids_str}"], cwd=script_dir)
            launched["tournament_forecast_v2"] = v2_ids
        except Exception as e:
            errors.append(f"Failed to launch tournament_forecast_v2.py: {e}")

    if batch_ids:
        ids_str = ",".join(str(i) for i in batch_ids)
        script_path = os.path.join(script_dir, "meta_refresh_forecast.py")
        try:
            subprocess.Popen([sys.executable, script_path, f"--ids={ids_str}"], cwd=script_dir)
            launched["meta_refresh_forecast"] = batch_ids
        except Exception as e:
            errors.append(f"Failed to launch meta_refresh_forecast.py: {e}")

    if not launched:
        return jsonify({"ok": False, "error": "; ".join(errors) or "Nothing was launched"}), 500

    return jsonify({
        "ok": True,
        "count": len(ids),
        "launched": launched,
        "errors": errors or None,
    })


@app.route("/set_refresh_after", methods=["POST"])
def set_refresh_after():
    """
    Added 2026-07-15 (Step 2 of the batch-path refresh scheduling work).
    Accepts a JSON body {"question_id": 43498, "refresh_after": "2026-08-20"}
    (a plain date string from the <input type="date">, interpreted as
    midnight UTC that day) or {"question_id": ..., "refresh_after": null}
    to clear an existing override and fall back to the default (the
    checkpoint-ladder schedule keyed off close_time — see
    meta_refresh_schedule.py's module docstring). Writes via
    meta_refresh_schedule.save_refresh_override — see that module for the
    override-file format. This route only writes state; it doesn't launch
    anything, so there's no cost/confirmation gate needed the way /refresh
    has one.

    CHANGED 2026-07-15: clamps the requested date to close_time if it
    would otherwise land after it (Mike's call — a refresh scheduled past
    when a question stops accepting forecasts is meaningless).
    compute_refresh_after already enforces this as a safety net regardless
    of how an override was set, but doing it here too means the response
    (and therefore what the dashboard displays right after saving) reflects
    the clamped date immediately, rather than only becoming visible on the
    next page load once the ladder logic applies its own clamp.
    """
    payload = request.get_json(silent=True) or {}
    if "question_id" not in payload:
        return jsonify({"ok": False, "error": "question_id is required"}), 400
    try:
        question_id = int(payload["question_id"])
    except (ValueError, TypeError):
        return jsonify({"ok": False, "error": "question_id must be an integer"}), 400

    raw_date = payload.get("refresh_after")
    refresh_after_dt = None
    clamped = False
    if raw_date:
        try:
            # <input type="date"> sends "YYYY-MM-DD" — treat as midnight UTC.
            refresh_after_dt = datetime.fromisoformat(raw_date).replace(tzinfo=timezone.utc)
        except Exception:
            return jsonify({"ok": False, "error": f"Could not parse date: {raw_date!r}"}), 400

        with CACHE_LOCK:
            data = CACHE["data"]
        rows = data["rows"] if data else []
        match = next((r for r in rows if r.get("post_id") == question_id
                      or r.get("question_id") == question_id), None)
        close_time = None
        if match and match.get("close_time"):
            try:
                close_time = datetime.fromisoformat(match["close_time"].replace("Z", "+00:00"))
            except Exception:
                close_time = None
        if close_time is not None and refresh_after_dt > close_time:
            refresh_after_dt = close_time
            clamped = True

    try:
        meta_refresh_schedule.save_refresh_override(question_id, refresh_after_dt)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to save: {e}"}), 500

    # ADDED 2026-07-18: this route used to only write the override to disk
    # and let the page's location.reload() pick it up via CACHE["data"] —
    # but CACHE["data"] is only rebuilt by the background thread every
    # REFRESH_INTERVAL_SECONDS (5 min), not on every request. That made
    # "did the new date show up after Save?" a race against that timer:
    # sometimes a background rebuild happened to land between the save and
    # the reload (looked fine), sometimes it didn't (showed the stale
    # date) — Mike reported this as intermittent, which matches exactly.
    # Fix: patch the affected row in place here, using the same
    # compute_refresh_after / is_due_for_refresh calls build_dashboard_data
    # itself uses (see there for the canonical version of this block), so
    # the very next page load is correct regardless of the timer.
    with CACHE_LOCK:
        data = CACHE["data"]
        rows = data["rows"] if data else []
        row = next((r for r in rows if r.get("post_id") == question_id
                    or r.get("question_id") == question_id), None)
        if row is not None and row.get("is_batch_path"):
            fresh_overrides = meta_refresh_schedule.load_refresh_overrides()
            _forecast_for_schedule = {
                "question_id": row.get("question_id"),
                "submitted_at": row.get("predicted_at"),
                "close_time": row.get("close_time"),
            }
            _refresh_after_dt = meta_refresh_schedule.compute_refresh_after(
                _forecast_for_schedule, overrides=fresh_overrides
            )
            row["refresh_after"] = _refresh_after_dt.isoformat() if _refresh_after_dt else None
            row["refresh_after_ts"] = int(_refresh_after_dt.timestamp() * 1000) if _refresh_after_dt else None
            row["is_due_for_batch_refresh"], row["batch_refresh_due_reason"] = (
                meta_refresh_schedule.is_due_for_refresh(_forecast_for_schedule, overrides=fresh_overrides)
            )
            row["has_refresh_override"] = str(question_id) in (fresh_overrides or {})

    return jsonify({"ok": True, "question_id": question_id,
                     "refresh_after": refresh_after_dt.isoformat() if refresh_after_dt else None,
                     "clamped_to_close": clamped})


if __name__ == "__main__":
    # ADDED 2026-07-16: use_reloader=True below means Werkzeug now
    # restarts the server automatically whenever this file is saved
    # (Mike's ask — auto-restart on save in Notepad++). That works by
    # spawning a SEPARATE watcher process that re-execs a fresh
    # `python meta_dashboard.py` child on every change; both the watcher
    # and the child independently run this entire script from the top.
    # Without this guard, the initial build_dashboard_data() fetch (hits
    # Metaculus/OpenRouter) and the background refresh_cache_loop thread
    # would BOTH start twice per launch — once wastefully in the watcher,
    # which never actually serves a request. WERKZEUG_RUN_MAIN is only set
    # (by Werkzeug itself) in the real child process, so gating on it
    # means this setup runs exactly once, in the process that matters.
    # Also correctly still runs normally if use_reloader is ever flipped
    # back to False — the "or not USE_RELOADER" branch covers that case
    # (no watcher/child split happens then, so there's nothing to guard
    # against).
    USE_RELOADER = True
    if not USE_RELOADER or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        try:
            data, bot_live, personal_live, local = build_dashboard_data()
            with CACHE_LOCK:
                CACHE["data"]               = data
                CACHE["live_by_qid"]        = bot_live
                CACHE["personal_live_by_qid"] = personal_live
                CACHE["local_by_qid"]       = local
                CACHE["last_refresh"]       = datetime.now(timezone.utc)
            print(f"Initial cache built: {data['total_predicted']} questions")
        except Exception as e:
            print(f"Initial cache build failed (will retry in background): {e}")
            with CACHE_LOCK:
                CACHE["error"] = str(e)

        threading.Thread(target=refresh_cache_loop, daemon=True).start()

    # ADDED 2026-07-16: confirmed live — the default watchdog-based
    # reloader (auto-picked when the watchdog package is installed, per
    # the "Restarting with watchdog (windowsapi)" log line) watches the
    # ENTIRE working directory for .py changes, not just this app's real
    # dependencies. Mike saved bybit_sim.py (a completely unrelated
    # crypto-sim project living in the same folder) and the dashboard
    # restarted anyway — same thing happened for meta_watch.py and
    # meta_coverage_check.py, which this app doesn't even import. Neither
    # restart was actually necessary: this app only ever imports
    # meta_cp_extract, meta_refresh_exclusions, and meta_refresh_schedule
    # (checked directly against this file's own import list) — every
    # forecasting script it LAUNCHES (tournament_forecast_v2.py,
    # meta_refresh_forecast.py) gets re-read fresh from disk on its own
    # each time it's spawned as a new subprocess, restart or not, so this
    # app restarting for THEIR changes was never buying anything either.
    # reloader_type="stat" watches only files backing currently-loaded
    # modules (sys.modules) instead of scanning the whole directory —
    # correctly narrows this to exactly the 3 real imports above with no
    # file list to maintain, rather than restarting for every unrelated
    # script that happens to live in the same folder.
    app.run(port=5002, debug=True, use_reloader=USE_RELOADER, reloader_type="stat")