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
import glob
import json
import asyncio
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string
from dotenv import load_dotenv
from forecasting_tools import MetaculusClient, ApiFilter
from meta_cp_extract import extract_live_cp

load_dotenv()

app = Flask(__name__)

# ─── Accounts ────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("METAC_TOURNAMENT_TOKEN")
PERSONAL_TOKEN = os.getenv("METACULUS_TOKEN")

bot_client      = MetaculusClient(token=BOT_TOKEN)      if BOT_TOKEN      else None
personal_client = MetaculusClient(token=PERSONAL_TOKEN) if PERSONAL_TOKEN else None

print(f"Bot client:      {'ready' if bot_client      else '⚠️  METAC_TOURNAMENT_TOKEN not set'}")
print(f"Personal client: {'ready' if personal_client else '⚠️  METACULUS_TOKEN not set'}")

LOCAL_RESULT_DIRS = ["tournament_batches", "Meta batches"]

TOURNAMENT_LABELS = {
    33022: "FutureEval",
    32880: "ACX2026",
     1756: "Climate Tipping Points",
    33021: "Metaculus Cup",
}
PERSONAL_LABEL = "Personal"
OTHER_LABEL    = "Other"
UNKNOWN_LABEL  = "Unknown"
TOURNAMENT_ORDER = [
    "FutureEval", "ACX2026", "Climate Tipping Points", "Metaculus Cup",
    PERSONAL_LABEL, OTHER_LABEL, UNKNOWN_LABEL,
]

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


def load_local_results(dirs: list[str]) -> dict[int, dict]:
    """Merge every batch_results_*.json, keyed by question_id.
    Most-recently-modified file wins on conflict."""
    by_qid: dict[int, dict] = {}
    by_qid_mtime: dict[int, float] = {}
    for d in dirs:
        for rf in glob.glob(os.path.join(d, "batch_results_*.json")):
            try:
                mtime = os.path.getmtime(rf)
                with open(rf, encoding="utf-8") as f:
                    data = json.load(f)
                for r in data.values():
                    qid = r.get("question_id")
                    if qid is None:
                        continue
                    if qid not in by_qid or mtime > by_qid_mtime[qid]:
                        by_qid[qid] = r
                        by_qid_mtime[qid] = mtime
            except Exception as e:
                print(f"  (skipping unreadable file {rf}: {e})")
    return by_qid


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

    for q in _run(ApiFilter(is_previously_forecasted_by_user=True)):
        by_qid[q.id_of_question] = q.api_json
    for q in _run(ApiFilter(is_previously_forecasted_by_user=True, allowed_statuses=["resolved"])):
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
    if not raw:
        return []
    projects = raw.get("projects", {}) or {}
    ids = set()
    for t in projects.get("tournament", []) or []:
        tid = t.get("id")
        if tid is not None:
            ids.add(tid)
    dp = projects.get("default_project")
    if dp and dp.get("type") == "tournament" and dp.get("id") is not None:
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


def _make_row(qid, local_r, post, is_personal_only=False) -> dict:
    score  = extract_score_info(post) if post else extract_score_info(None)
    q_type = (local_r or {}).get("question_type") or (post.get("question") or {}).get("type") if post else None
    cp_val = extract_live_cp(post, q_type) if post else None
    tournaments = detect_tournaments(post) if post else []
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

    return {
        "question_id":       qid,
        "post_id":           post_id,
        "question_text":     (local_r or {}).get("question_text") or score["title"] or "(unknown)",
        "question_type":     q_type,
        "submitted_summary": summarize_forecast(q_type, (local_r or {}).get("submitted_forecast") or (local_r or {}).get("probability")),
        "cp_summary":        summarize_forecast(q_type, cp_val) if cp_val is not None else "—",
        "cp_available":      score["cp_available"],
        "resolved":          score["resolved"],
        "resolution":        score["resolution"],
        "resolve_time":      score["resolve_time"],
        "peer_score":        score["peer_score"],
        "baseline_score":    score["baseline_score"],
        "close_time":        score["close_time"],
        "api_status":        score["api_status"],
        "live_match_found":  post is not None,
        "tournaments":       tournaments,
        "is_personal_only":  is_personal_only,
        # raw reasoning/research from local file (for detail page)
        "reasoning":         (local_r or {}).get("reasoning", ""),
        "research_text":     (local_r or {}).get("research_text", ""),
        "refresh_reason":    (local_r or {}).get("refresh_reason", ""),
        "original_prob":     (local_r or {}).get("original_prob"),
    }


# ─── Data assembly ────────────────────────────────────────────────────────────
def build_dashboard_data():
    local          = load_local_results(LOCAL_RESULT_DIRS)
    bot_live       = fetch_predicted_questions(bot_client, "bot")
    personal_live  = fetch_predicted_questions(personal_client, "personal")

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
        rows.append(_make_row(qid, r, post, is_personal_only=is_personal_only))
        seen.add(qid)

    # Bot-live-only rows (no local result — manual predictions etc.)
    for qid, post in bot_live.items():
        if qid in seen:
            continue
        rows.append(_make_row(qid, None, post, is_personal_only=False))
        seen.add(qid)

    # Personal-only rows (not predicted by bot)
    for qid, post in personal_live.items():
        if qid in seen:
            continue
        local_r = local.get(qid)
        rows.append(_make_row(qid, local_r, post, is_personal_only=True))
        seen.add(qid)

    for row in rows:
        row["status_bucket"] = classify_status(row)
        row["status_label"]  = STATUS_LABELS[row["status_bucket"]]

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

    phase0 = load_phase0_reports()

    data = {
        "rows":                rows,
        "phase0":              phase0,
        "status_counts":       status_counts,
        "avg_score":           avg_score,
        "chart_bot":           chart_bot,
        "chart_personal":      chart_personal,
        "total_predicted":     len(rows),
        "bot_user_id":         get_confirmed_user_id(bot_client),
        "personal_user_id":    get_confirmed_user_id(personal_client),
        "token_configured":    bot_client is not None,
        "tournaments_present": tournaments_present,
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
    tr:hover { background: #fafbfc; }
    tr.highlight { animation: rowflash 3s ease-out; }
    @keyframes rowflash { 0% { background: #fef9c3; } 50% { background: #fef9c3; } 100% { background: transparent; } }
    .pos { color: #16a34a; font-weight: 600; }
    .neg { color: #dc2626; font-weight: 600; }
    .muted { color: #999; }
    .tag { display: inline-block; font-size: 11px; background: #eef0f3; color: #555; border-radius: 4px;
           padding: 1px 6px; margin-right: 4px; }
    .tag.personal { background: #fef3c7; color: #92400e; }
    .chart-wrap { background: white; border-radius: 8px; padding: 16px; margin-bottom: 8px; height: 280px; }
    .chart-note { color: #999; font-size: 12px; margin: 0 0 24px; }
    a.detail-link { font-size: 11px; color: #888; }
    .refresh-note { color: #999; font-size: 12px; margin: 8px 0 16px; }
    .cp-na { color: #ccc; font-size: 11px; }
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
    Last refreshed: {{ last_refresh }} · auto-refreshes every 5 minutes
    {% if cache_error %} · ⚠️ last refresh failed: {{ cache_error }}{% endif %}
  </div>

  <div class="cards">
    <div class="card"><div class="label">Total predicted</div><div class="value">{{ data.total_predicted }}</div></div>
    <div class="card"><div class="label">Open</div><div class="value">{{ data.status_counts.open }}</div></div>
    <div class="card"><div class="label">Closed</div><div class="value">{{ data.status_counts.closed_unresolved }}</div></div>
    <div class="card"><div class="label">Resolved &amp; scored</div><div class="value">{{ data.status_counts.resolved_scored }}</div></div>
    <div class="card"><div class="label">Resolved, no score</div><div class="value">{{ data.status_counts.resolved_unscored }}</div></div>
    <div class="card"><div class="label">Withdrawn</div><div class="value">{{ data.status_counts.not_found_live }}</div></div>
    <div class="card"><div class="label">Avg peer score</div>
      <div class="value {{ 'pos' if data.avg_score and data.avg_score > 0 else ('neg' if data.avg_score and data.avg_score < 0 else '') }}">
        {{ '%.2f'|format(data.avg_score) if data.avg_score is not none else '—' }}
      </div>
    </div>
  </div>

  {% if data.phase0.coverage or data.phase0.calibration %}
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
  </div>
  {% endif %}

  <div class="filter-bar">
    <div class="filter-group-label">Tournament</div>
    <div class="pills" id="tournamentPills">
      {% for t in data.tournaments_present %}
      <div class="pill" data-value="{{ t }}">{{ t }}</div>
      {% endfor %}
    </div>
    <div class="filter-group-label">Status</div>
    <div class="pills" id="statusPills">
      {% for s in status_order %}
      <div class="pill" data-value="{{ s }}">{{ status_labels[s] }} ({{ data.status_counts[s] }})</div>
      {% endfor %}
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
        <th>ID</th><th>Question</th><th>Type</th><th>Submitted</th><th>CP</th>
        <th>Status</th><th>Resolution</th><th>Resolved at</th><th>Peer score</th><th>Tournament(s)</th><th></th>
      </tr>
    </thead>
    <tbody>
      {% for row in data.rows %}
      <tr id="row-{{ row.question_id }}" data-tournaments="{{ row.tournaments|join(',') }}" data-status="{{ row.status_bucket }}">
        <td>{{ row.question_id }}</td>
        <td>{{ row.question_text[:70] }}</td>
        <td>{{ row.question_type or '—' }}</td>
        <td>{{ row.submitted_summary }}</td>
        <td>
          {% if row.cp_summary != '—' %}
            {{ row.cp_summary }}
          {% elif not row.cp_available %}
            <span class="cp-na" title="CP not available — bots excluded from community aggregate on this question">n/a</span>
          {% else %}
            <span class="muted">—</span>
          {% endif %}
        </td>
        <td>{{ row.status_label }}</td>
        <td>{{ row.resolution if row.resolution is not none else '—' }}</td>
        <td>{{ row.resolve_time[:10] if row.resolve_time else '—' }}</td>
        <td class="{{ 'pos' if row.peer_score and row.peer_score > 0 else ('neg' if row.peer_score and row.peer_score < 0 else 'muted') }}">
          {{ '%.2f'|format(row.peer_score) if row.peer_score is not none else (row.resolved and '?' or '—') }}
        </td>
        <td>{% for t in row.tournaments %}<span class="tag {{ 'personal' if t == 'Personal' else '' }}">{{ t }}</span>{% endfor %}</td>
        <td><a class="detail-link" href="/detail/{{ row.question_id }}" target="_blank">detail</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>

  <script>
    const STORAGE_KEY = 'meta_dashboard_filters_v3';
    const botPoints      = {{ chart_bot|tojson }};
    const personalPoints = {{ chart_personal|tojson }};
    let chartInstance = null;

    function getSelected(id) {
      return new Set([...document.querySelectorAll('#' + id + ' .pill.active')].map(el => el.dataset.value));
    }
    function saveFilters() {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({
        t: [...getSelected('tournamentPills')], s: [...getSelected('statusPills')]
      }));
    }
    function restoreFilters() {
      try {
        const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
        (saved.t || []).forEach(v => {
          const el = document.querySelector('#tournamentPills .pill[data-value="' + CSS.escape(v) + '"]');
          if (el) el.classList.add('active');
        });
        (saved.s || []).forEach(v => {
          const el = document.querySelector('#statusPills .pill[data-value="' + CSS.escape(v) + '"]');
          if (el) el.classList.add('active');
        });
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
    function applyFilters() {
      const selT = getSelected('tournamentPills');
      const selS = getSelected('statusPills');
      let visible = 0;
      document.querySelectorAll('#rowsTable tbody tr').forEach(tr => {
        const tours = tr.dataset.tournaments.split(',');
        const tMatch = selT.size === 0 || tours.some(t => selT.has(t));
        const sMatch = selS.size === 0 || selS.has(tr.dataset.status);
        tr.style.display = tMatch && sMatch ? '' : 'none';
        if (tMatch && sMatch) visible++;
      });
      const total = document.querySelectorAll('#rowsTable tbody tr').length;
      document.getElementById('showingCount').textContent = 'Showing ' + visible + ' of ' + total;
      renderChart(selT);
    }
    document.querySelectorAll('.pill').forEach(p =>
      p.addEventListener('click', () => { p.classList.toggle('active'); saveFilters(); applyFilters(); })
    );
    document.getElementById('clearFilters').addEventListener('click', () => {
      document.querySelectorAll('.pill.active').forEach(p => p.classList.remove('active'));
      saveFilters(); applyFilters();
    });
    restoreFilters();
    applyFilters();
    setTimeout(() => location.reload(), 300000);
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
    <h2>Research</h2>
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

    # Find the row for this question
    row = next((r for r in data["rows"] if r["question_id"] == question_id), None)
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
        resolution=row["resolution"],
        peer_score=row["peer_score"],
        baseline_score=row["baseline_score"],
        refresh_reason=row["refresh_reason"],
        reasoning=row["reasoning"],
        research_text=row["research_text"],
        raw_json=json.dumps(raw, indent=2, default=str),
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


if __name__ == "__main__":
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
    app.run(port=5002, debug=True, use_reloader=False)