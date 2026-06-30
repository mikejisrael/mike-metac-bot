"""
meta_dashboard.py — Forecasting track-record dashboard (Metaculus side).

v2 — single-account, tournament-split, cached, filterable.

ACCOUNT — only mike_iz_-bot (METAC_TOURNAMENT_TOKEN) is tracked. The old
personal/bot split is gone: everything genuinely runs through the bot
account now, so there's nothing left for a "personal" tab to show.

TOURNAMENT SPLIT — local batch_results_*.json files do NOT record which
tournament a question belongs to (confirmed by inspecting real files), so
tournament membership is derived per-question from the live API's
'projects' field (projects.tournament[].id / projects.default_project.id),
matched against the real IDs pulled straight from this account's own data
via list_tournaments.py:
    33022 -> FutureEval
    32880 -> ACX2026
     1756 -> Climate Tipping Points
    33021 -> Metaculus Cup
Anything else with tournament project info -> "Other". Local-only rows
with no live match at all (withdrawn before any live lookup could resolve
project info) -> "Unknown".

SPEED — the live API join (two paginated is_previously_forecasted_by_user
passes) is the slow part, not money (Metaculus's API is free). Instead of
re-running that on every page view, a background thread refreshes an
in-memory cache every REFRESH_INTERVAL_SECONDS (5 min, matching the page's
own autorefresh), and every page load just reads the cache — instant,
no matter how many tabs/clicks/autorefreshes happen in between.

STATUS BUCKETS — five, not four. "Open" and "Closed" used to be lumped
together as one "open" bucket; now split using the live API's own
'status' field, since "closed" (forecasting window over, not yet
resolved) is a real and useful state to filter on separately:
  - Open               — still accepting forecasts
  - Closed              — forecasting closed, not yet resolved
  - Resolved & scored   — resolved AND has a peer score
  - Resolved, no score  — resolved but Annulled/Ambiguous or not yet scored
  - Withdrawn           — no longer in the live "previously forecasted" list

FILTERING — client-side pill bar (tournament x status), multi-select
within each group (OR), AND across groups — same pattern as Metaculus's
own filter UI. No reload needed to filter; selections persist across the
5-minute autorefresh via localStorage.

IMPORTANT CAVEAT — same as before: the live-score field-path guessing in
extract_score_info() is best-effort (built without being able to
authenticate against the real API from dev). Use the "raw" debug link
next to each row to see the actual JSON Metaculus returns if a peer score
looks wrong.

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

# ─── Single account, single client ──────────────────────────────────────────
BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")
bot_client = MetaculusClient(token=BOT_TOKEN) if BOT_TOKEN else None

if bot_client:
    print("Bot client ready (mike_iz_-bot) — METAC_TOURNAMENT_TOKEN set")
else:
    print("⚠️  METAC_TOURNAMENT_TOKEN not set — dashboard will be empty")

LOCAL_RESULT_DIRS = ["tournament_batches", "Meta batches"]

# Real IDs pulled from this account's own live data via list_tournaments.py
TOURNAMENT_LABELS = {
    33022: "FutureEval",
    32880: "ACX2026",
    1756: "Climate Tipping Points",
    33021: "Metaculus Cup",
}
TOURNAMENT_ORDER = ["FutureEval", "ACX2026", "Climate Tipping Points", "Metaculus Cup", "Other", "Unknown"]
OTHER_LABEL = "Other"
UNKNOWN_LABEL = "Unknown"

STATUS_LABELS = {
    "open": "Open",
    "closed_unresolved": "Closed",
    "resolved_scored": "Resolved & scored",
    "resolved_unscored": "Resolved, no score",
    "not_found_live": "Withdrawn",
}
STATUS_ORDER = ["open", "closed_unresolved", "resolved_scored", "resolved_unscored", "not_found_live"]

REFRESH_INTERVAL_SECONDS = 300  # 5 minutes — matches page autorefresh

CACHE: dict = {"data": None, "live_by_qid": {}, "last_refresh": None, "error": None}
CACHE_LOCK = threading.Lock()


def get_confirmed_user_id(client) -> int | None:
    """Ask the API directly which account this token belongs to."""
    if client is None:
        return None
    try:
        return client.get_current_user_id()
    except Exception as e:
        print(f"  get_confirmed_user_id failed: {e}")
        return None


def load_local_results(dirs: list[str]) -> dict[int, dict]:
    """Merge every batch_results_*.json across the given folder(s), keyed by
    question_id. Most-recently-modified file wins on conflict."""
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


def fetch_my_predicted_questions(client) -> dict[int, dict]:
    """All questions this client's account has ever predicted on, keyed by
    question id. Default pass + explicit resolved-status pass merged in."""
    if client is None:
        return {}

    by_qid: dict[int, dict] = {}

    def _run(api_filter: ApiFilter) -> list:
        try:
            return asyncio.run(
                client.get_questions_matching_filter(
                    api_filter, num_questions=1000, error_if_question_target_missed=False
                )
            )
        except Exception as e:
            print(f"  fetch_my_predicted_questions pass failed: {e}")
            return []

    default_pass = _run(ApiFilter(is_previously_forecasted_by_user=True))
    for q in default_pass:
        by_qid[q.id_of_question] = q.api_json

    resolved_pass = _run(
        ApiFilter(is_previously_forecasted_by_user=True, allowed_statuses=["resolved"])
    )
    for q in resolved_pass:
        by_qid.setdefault(q.id_of_question, q.api_json)

    print(f"  fetch_my_predicted_questions: {len(default_pass)} default + "
          f"{len(resolved_pass)} resolved-pass -> {len(by_qid)} unique questions")
    return by_qid


def extract_score_info(raw: dict) -> dict:
    """Top-level 'resolved'/'status', nested question.resolution, and an
    individual forecaster's score under question.my_forecasts."""
    info = {"resolved": False, "resolution": None, "peer_score": None,
            "baseline_score": None, "close_time": None, "title": None,
            "api_status": None, "resolve_time": None}
    if not raw or "_error" in raw:
        return info

    q = raw.get("question", raw)
    info["title"] = raw.get("title") or q.get("title")
    info["close_time"] = raw.get("scheduled_close_time") or q.get("scheduled_close_time")
    info["api_status"] = raw.get("status") or q.get("status")
    info["resolve_time"] = (
        raw.get("actual_resolve_time") or q.get("actual_resolve_time")
        or q.get("resolution_set_time") or q.get("scheduled_resolve_time")
        or raw.get("scheduled_resolve_time")
    )

    if "resolved" in raw:
        info["resolved"] = bool(raw["resolved"])
    else:
        info["resolved"] = q.get("resolution") is not None
    info["resolution"] = q.get("resolution")

    candidate_paths = [
        ("my_forecasts", "score_data", "peer_score"),
        ("my_forecasts", "latest", "score_data", "peer_score"),
        ("my_forecasts", "latest", "peer_score"),
        ("scoring", "peer_score"),
        ("score_data", "peer_score"),
    ]
    for path in candidate_paths:
        val = q
        try:
            for key in path:
                val = val[key]
            if val is not None:
                info["peer_score"] = val
                break
        except (KeyError, TypeError):
            continue

    candidate_baseline_paths = [
        ("my_forecasts", "score_data", "baseline_score"),
        ("my_forecasts", "latest", "score_data", "baseline_score"),
        ("scoring", "baseline_score"),
        ("score_data", "baseline_score"),
    ]
    for path in candidate_baseline_paths:
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
    """Short human-readable summary of a submitted_forecast value."""
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
    """Sorted list of tournament label strings this question belongs to,
    derived from the live API's 'projects' field. Empty list if no live
    data or no tournament project info at all (caller decides fallback
    label — 'Other' vs 'Unknown' depending on whether live data existed)."""
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
    """One of: open / closed_unresolved / resolved_scored / resolved_unscored / not_found_live."""
    if not row["live_match_found"]:
        return "not_found_live"
    if row["resolved"]:
        return "resolved_scored" if row["peer_score"] is not None else "resolved_unscored"
    if (row["api_status"] or "").lower() in ("closed", "pending_resolution"):
        return "closed_unresolved"
    return "open"


# ─── Data assembly (called from background thread, not per-request) ────────
def build_dashboard_data() -> dict:
    local = load_local_results(LOCAL_RESULT_DIRS)
    live_by_qid = fetch_my_predicted_questions(bot_client)
    rows = []
    seen_qids = set()

    for qid, r in sorted(local.items(), key=lambda kv: kv[0], reverse=True):
        post = live_by_qid.get(qid)
        score = extract_score_info(post) if post else extract_score_info(None)
        q_type = r.get("question_type")
        tournaments = detect_tournaments(post) if post else []
        cp_value = extract_live_cp(post, q_type) if post else None
        rows.append({
            "question_id": qid,
            "question_text": r.get("question_text", score["title"] or "(unknown)"),
            "question_type": q_type,
            "submitted_summary": summarize_forecast(
                q_type, r.get("submitted_forecast", r.get("probability"))
            ),
            "cp_summary": summarize_forecast(q_type, cp_value) if cp_value is not None else "—",
            "resolved": score["resolved"],
            "resolution": score["resolution"],
            "resolve_time": score["resolve_time"],
            "peer_score": score["peer_score"],
            "close_time": score["close_time"],
            "api_status": score["api_status"],
            "live_match_found": post is not None,
            "tournaments": tournaments or ([OTHER_LABEL] if post else [UNKNOWN_LABEL]),
        })
        seen_qids.add(qid)

    # Live-only rows (manual predictions, or predates local logging).
    for qid, post in live_by_qid.items():
        if qid in seen_qids:
            continue
        score = extract_score_info(post)
        tournaments = detect_tournaments(post)
        q_type = (post.get("question") or {}).get("type")
        cp_value = extract_live_cp(post, q_type)
        rows.append({
            "question_id": qid,
            "question_text": score["title"] or "(unknown)",
            "question_type": q_type,
            "submitted_summary": "—",
            "cp_summary": summarize_forecast(q_type, cp_value) if cp_value is not None else "—",
            "resolved": score["resolved"],
            "resolution": score["resolution"],
            "resolve_time": score["resolve_time"],
            "peer_score": score["peer_score"],
            "close_time": score["close_time"],
            "api_status": score["api_status"],
            "live_match_found": True,
            "tournaments": tournaments or [OTHER_LABEL],
        })

    for row in rows:
        row["status_bucket"] = classify_status(row)
        row["status_label"] = STATUS_LABELS[row["status_bucket"]]

    rows.sort(key=lambda r: r["question_id"], reverse=True)

    status_counts = {k: 0 for k in STATUS_LABELS}
    for row in rows:
        status_counts[row["status_bucket"]] += 1

    resolved_scored_rows = [r for r in rows if r["status_bucket"] == "resolved_scored"]
    avg_score = (
        sum(r["peer_score"] for r in resolved_scored_rows) / len(resolved_scored_rows)
    ) if resolved_scored_rows else None

    chart_points = [
        {"x": r["close_time"], "y": r["peer_score"], "tournaments": r["tournaments"]}
        for r in resolved_scored_rows if r["close_time"]
    ]

    tournaments_present = [t for t in TOURNAMENT_ORDER
                            if any(t in r["tournaments"] for r in rows)]

    return {
        "rows": rows,
        "status_counts": status_counts,
        "avg_score": avg_score,
        "chart_points": chart_points,
        "total_predicted": len(rows),
        "user_id": get_confirmed_user_id(bot_client),
        "token_configured": bot_client is not None,
        "tournaments_present": tournaments_present,
    }, live_by_qid


def refresh_cache_loop():
    while True:
        try:
            data, live_by_qid = build_dashboard_data()
            with CACHE_LOCK:
                CACHE["data"] = data
                CACHE["live_by_qid"] = live_by_qid
                CACHE["last_refresh"] = datetime.now(timezone.utc)
                CACHE["error"] = None
            print(f"  cache refreshed: {data['total_predicted']} questions "
                  f"at {CACHE['last_refresh'].isoformat()}")
        except Exception as e:
            print(f"  cache refresh FAILED: {e}")
            with CACHE_LOCK:
                CACHE["error"] = str(e)
        time.sleep(REFRESH_INTERVAL_SECONDS)


# ─── Page template ───────────────────────────────────────────────────────────
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
    .card { background: white; border-radius: 8px; padding: 16px 20px; box-shadow: 0 1px 3px rgba(0,0,0,.08);
            min-width: 130px; }
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
    .pos { color: #16a34a; font-weight: 600; }
    .neg { color: #dc2626; font-weight: 600; }
    .muted { color: #999; }
    .tag { display: inline-block; font-size: 11px; background: #eef0f3; color: #555; border-radius: 4px;
           padding: 1px 6px; margin-right: 4px; }
    .chart-wrap { background: white; border-radius: 8px; padding: 16px; margin-bottom: 8px;
                  height: 280px; }
    .chart-note { color: #999; font-size: 12px; margin: 0 0 24px; }
    a.raw { font-size: 11px; color: #888; }
    .refresh-note { color: #999; font-size: 12px; margin: 8px 0 16px; }
  </style>
</head>
<body>
  <h1>Metaculus Track Record</h1>
  <div class="sub">
    mike_iz_-bot{% if data.user_id %} (user id {{ data.user_id }}){% endif %}
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
  <p class="chart-note">X-axis = each question's <b>scheduled close date</b>. Chart respects the tournament filter above (status filter doesn't apply — it's always resolved &amp; scored questions).</p>
  <div id="noChartMsg" style="background:white;border-radius:8px;padding:24px;margin-bottom:24px;
       text-align:center;color:#999;box-shadow:0 1px 3px rgba(0,0,0,.08);">
    No resolved &amp; scored questions yet — the chart will appear once at least one shows a peer score.
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
      <tr data-tournaments="{{ row.tournaments|join(',') }}" data-status="{{ row.status_bucket }}">
        <td>{{ row.question_id }}</td>
        <td>{{ row.question_text[:70] }}</td>
        <td>{{ row.question_type or '—' }}</td>
        <td>{{ row.submitted_summary }}</td>
        <td class="muted">{{ row.cp_summary }}</td>
        <td>{{ row.status_label }}</td>
        <td>{{ row.resolution if row.resolution is not none else '—' }}</td>
        <td>{{ row.resolve_time[:10] if row.resolve_time else '—' }}</td>
        <td class="{{ 'pos' if row.peer_score and row.peer_score > 0 else ('neg' if row.peer_score and row.peer_score < 0 else 'muted') }}">
          {{ '%.2f'|format(row.peer_score) if row.peer_score is not none else (row.resolved and 'unknown field — check raw' or '—') }}
        </td>
        <td>{% for t in row.tournaments %}<span class="tag">{{ t }}</span>{% endfor %}</td>
        <td><a class="raw" href="/raw/{{ row.question_id }}" target="_blank">raw</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>

  <script>
    const STORAGE_KEY = 'meta_dashboard_filters_v2';
    const allPoints = {{ chart_points|tojson }};
    let chartInstance = null;

    function getSelected(containerId) {
      return new Set(
        [...document.querySelectorAll('#' + containerId + ' .pill.active')].map(el => el.dataset.value)
      );
    }

    function saveFilters() {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({
        t: [...getSelected('tournamentPills')],
        s: [...getSelected('statusPills')],
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
      } catch (e) { /* ignore corrupt storage */ }
    }

    function renderChart(selectedTournaments) {
      const points = selectedTournaments.size === 0
        ? allPoints
        : allPoints.filter(p => p.tournaments.some(t => selectedTournaments.has(t)));

      const wrap = document.getElementById('chartWrap');
      const noMsg = document.getElementById('noChartMsg');
      if (points.length === 0) {
        wrap.style.display = 'none';
        noMsg.style.display = 'block';
        return;
      }
      wrap.style.display = 'block';
      noMsg.style.display = 'none';

      if (chartInstance) chartInstance.destroy();
      chartInstance = new Chart(document.getElementById('scoreChart'), {
        type: 'scatter',
        data: { datasets: [{ label: 'Peer score (resolved & scored questions)', data: points,
                              backgroundColor: '#2563eb' }] },
        options: {
          responsive: true, maintainAspectRatio: false,
          scales: {
            x: {
              type: 'time',
              time: { unit: 'day', tooltipFormat: 'MMM D, YYYY', displayFormats: { day: 'MMM D' } },
              title: { display: true, text: 'Scheduled close date' }
            },
            y: { title: { display: true, text: 'Peer score' } }
          }
        }
      });
    }

    function applyFilters() {
      const selT = getSelected('tournamentPills');
      const selS = getSelected('statusPills');
      let visible = 0;
      const rows = document.querySelectorAll('#rowsTable tbody tr');
      rows.forEach(tr => {
        const tours = tr.dataset.tournaments.split(',');
        const status = tr.dataset.status;
        const tMatch = selT.size === 0 || tours.some(t => selT.has(t));
        const sMatch = selS.size === 0 || selS.has(status);
        const show = tMatch && sMatch;
        tr.style.display = show ? '' : 'none';
        if (show) visible++;
      });
      document.getElementById('showingCount').textContent =
        'Showing ' + visible + ' of ' + rows.length;
      renderChart(selT);
    }

    document.querySelectorAll('.pill').forEach(p => {
      p.addEventListener('click', () => {
        p.classList.toggle('active');
        saveFilters();
        applyFilters();
      });
    });

    document.getElementById('clearFilters').addEventListener('click', () => {
      document.querySelectorAll('.pill.active').forEach(p => p.classList.remove('active'));
      saveFilters();
      applyFilters();
    });

    restoreFilters();
    applyFilters();

    // Autorefresh every 5 minutes — matches the server-side cache cadence.
    // Filter selections survive via localStorage + restoreFilters() above.
    setTimeout(() => location.reload(), 300000);
  </script>
</body>
</html>
"""

LOADING_TEMPLATE = """
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Metaculus Track Record</title>
<meta http-equiv="refresh" content="3">
</head>
<body style="font-family: -apple-system, Segoe UI, Arial, sans-serif; padding: 40px; color: #666;">
  <h2>Loading first batch of data…</h2>
  <p>The background cache hasn't finished its first fetch yet — this page refreshes itself every 3s.</p>
  {% if cache_error %}<p style="color:#dc2626;">Last attempt failed: {{ cache_error }}</p>{% endif %}
</body></html>
"""


# ─── Routes ─────────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    with CACHE_LOCK:
        data = CACHE["data"]
        last_refresh = CACHE["last_refresh"]
        cache_error = CACHE["error"]

    if data is None:
        return render_template_string(LOADING_TEMPLATE, cache_error=cache_error)

    return render_template_string(
        PAGE_TEMPLATE,
        data=data,
        chart_points=data["chart_points"],
        status_order=STATUS_ORDER,
        status_labels=STATUS_LABELS,
        last_refresh=last_refresh.strftime("%Y-%m-%d %H:%M:%S UTC") if last_refresh else "never",
        cache_error=cache_error,
    )


@app.route("/raw/<int:question_id>")
def raw(question_id):
    with CACHE_LOCK:
        live_by_qid = CACHE["live_by_qid"]
    if question_id in live_by_qid:
        return jsonify(live_by_qid[question_id])
    return jsonify({"_error": f"Question id {question_id} not found among the bot account's "
                              f"predicted questions. It may not have a forecast registered yet "
                              f"under this token, or was withdrawn."})


if __name__ == "__main__":
    if not BOT_TOKEN:
        print("⚠️  METAC_TOURNAMENT_TOKEN not found in .env — dashboard will be empty.")

    # Build the cache once synchronously before serving, so the first page
    # load isn't the "loading…" placeholder if it can be avoided.
    try:
        data, live_by_qid = build_dashboard_data()
        with CACHE_LOCK:
            CACHE["data"] = data
            CACHE["live_by_qid"] = live_by_qid
            CACHE["last_refresh"] = datetime.now(timezone.utc)
        print(f"Initial cache built: {data['total_predicted']} questions")
    except Exception as e:
        print(f"Initial cache build failed (will retry in background): {e}")
        with CACHE_LOCK:
            CACHE["error"] = str(e)

    threading.Thread(target=refresh_cache_loop, daemon=True).start()

    # use_reloader=False: the default reloader watches the whole working
    # directory recursively for .py changes, which includes venv312/ sitting
    # inside this same project folder — any package writing to its own files
    # was triggering constant restarts. debug=True still gives in-browser
    # tracebacks, just without auto-restart.
    app.run(port=5002, debug=True, use_reloader=False)