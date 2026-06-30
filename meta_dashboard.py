"""
meta_dashboard.py — Forecasting track-record dashboard (Metaculus side),
mirroring the bybit_sim.py / bybit_dashboard.py pattern: a small local Flask
app that collates everything in one page instead of clicking through
Metaculus's own UI question-by-question.

ACCOUNTS — there ARE two real, separate Metaculus identities, confirmed by
Metaculus staff and visible on both profile pages:
  - mike_iz_       (personal account, user id 302314) — auth: METACULUS_TOKEN
  - mike_iz_-bot   (dedicated bot account)             — auth: METAC_TOURNAMENT_TOKEN
A single client/token can only ever authenticate as ONE of these, so the
dashboard now builds one MetaculusClient per token and renders a separate
tab per account instead of merging everything under one label.

Two data sources, combined differently per profile:
  - BOT profile:      local batch_results_*.json (tournament_batches/ and
                       "Meta batches/") for question text/type/status/the
                       actual submitted forecast, ENRICHED with a live
                       per-question lookup for resolution + score.
  - PERSONAL profile: live API only (is_previously_forecasted_by_user) —
                       there's no local log of manual predictions, so this
                       tab is entirely Metaculus-API-driven.

PREDICTED vs SCORED — "questions predicted" and "questions scored" are NOT
the same population, and the gap between them isn't one mystery bucket.
Every predicted question lands in exactly one of:
  - Open            — not yet closed/resolved
  - Resolved+Scored — resolved AND has a peer score (this is what Metaculus's
                       own "questions scored" stat counts)
  - Resolved, no score — resolved but Annulled/Ambiguous (Metaculus does not
                       score these — see metaculus.com/faq), or scored but
                       not yet reflected in the API
  - Not found live  — no longer appears in this account's live
                       "previously forecasted" list at all, almost always
                       because the forecast was withdrawn before close
The dashboard now computes and displays all four buckets per account instead
of lumping everything not currently "open" into one "withdrawn" pile.

IMPORTANT CAVEAT — read this before assuming the live score numbers are
exact: the live-score lookup (fetch_my_predicted_questions / extract_score_info
below) was built without being able to authenticate against the real
Metaculus API from my dev environment, so exact field names for an
individual forecaster's peer/baseline score on a resolved question are a
best-effort guess based on third-party docs, not a verified schema. Use the
"raw" debug link next to each resolved question to see the actual JSON
Metaculus returns — if extract_score_info() picked the wrong field, that raw
view tells us exactly what to fix.

Run:
  python meta_dashboard.py
Then open http://localhost:5002  (?profile=personal or ?profile=bot, default personal)
"""

import os
import glob
import json
import asyncio
from flask import Flask, request, jsonify, render_template_string
from dotenv import load_dotenv
from forecasting_tools import MetaculusClient, ApiFilter

load_dotenv()

app = Flask(__name__)

# ─── Two accounts, two tokens, two clients ──────────────────────────────────
PERSONAL_TOKEN = os.getenv("METACULUS_TOKEN")
BOT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN")

PERSONAL_USER_ID_FALLBACK = int(os.getenv("METAC_USER_ID", "302314"))  # display-only fallback

personal_client = MetaculusClient(token=PERSONAL_TOKEN) if PERSONAL_TOKEN else None
bot_client = MetaculusClient(token=BOT_TOKEN) if BOT_TOKEN else None

if personal_client:
    print("Personal client ready (mike_iz_) — METACULUS_TOKEN set")
else:
    print("⚠️  METACULUS_TOKEN not set — personal (mike_iz_) tab will be empty")
if bot_client:
    print("Bot client ready (mike_iz_-bot) — METAC_TOURNAMENT_TOKEN set")
else:
    print("⚠️  METAC_TOURNAMENT_TOKEN not set — bot (mike_iz_-bot) tab will be empty")

TOURNAMENT_ID = os.getenv("METAC_TOURNAMENT_ID", "33022")
LOCAL_RESULT_DIRS = ["tournament_batches", "Meta batches"]

ACCOUNTS = {
    "personal": {
        "label": "mike_iz_ (personal)",
        "client": personal_client,
        "local_dirs": [],  # no local logs for manual personal forecasts
    },
    "bot": {
        "label": "mike_iz_-bot",
        "client": bot_client,
        "local_dirs": LOCAL_RESULT_DIRS,
    },
}


def get_confirmed_user_id(client) -> int | None:
    """Ask the API directly which account this token belongs to, rather
    than trusting a hardcoded guess."""
    if client is None:
        return None
    try:
        return client.get_current_user_id()
    except Exception as e:
        print(f"  get_confirmed_user_id failed: {e}")
        return None


def load_local_results(dirs: list[str]) -> dict[int, dict]:
    """Merge every batch_results_*.json across the given folder(s), keyed by
    question_id. If the same question shows up in more than one file (e.g. a
    resubmission), the most recently-modified file wins. Each entry is
    tagged with 'category' (tournament/general) based on which folder it
    came from, so the dashboard can split them."""
    by_qid: dict[int, dict] = {}
    by_qid_mtime: dict[int, float] = {}

    for d in dirs:
        category = "tournament" if "tournament" in d.lower() else "general"
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
                        r = {**r, "category": category}
                        by_qid[qid] = r
                        by_qid_mtime[qid] = mtime
            except Exception as e:
                print(f"  (skipping unreadable file {rf}: {e})")
    return by_qid


def fetch_my_predicted_questions(client) -> dict[int, dict]:
    """All questions this client's account has ever predicted on, keyed by
    question id. Makes a default (no status filter) pass first, then a
    second explicit allowed_statuses=['resolved'] pass merged in — belt-and-
    suspenders against any resolved questions getting pushed off the
    default -published_time-ordered page. Returns each question's raw
    api_json — same dict shape extract_score_info() expects."""
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
    """Extraction based on the confirmed real schema (seen via /raw debug
    output): top-level 'resolved' bool and 'status', nested question.resolution,
    and an individual forecaster's score living under question.my_forecasts
    (only populated for whichever account the calling client authenticates as)."""
    info = {"resolved": False, "resolution": None, "peer_score": None,
            "baseline_score": None, "close_time": None, "title": None,
            "status": None}
    if not raw or "_error" in raw:
        return info

    q = raw.get("question", raw)
    info["title"] = raw.get("title") or q.get("title")
    info["close_time"] = raw.get("scheduled_close_time") or q.get("scheduled_close_time")
    info["status"] = raw.get("status") or q.get("status")

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
    """Short human-readable summary of a submitted_forecast value for the
    table — same shapes tournament_forecast.py logs."""
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


# Resolutions Metaculus does not score, per their FAQ (annulled / ambiguous).
UNSCORED_RESOLUTIONS = {"annulled", "ambiguous"}


def detect_category_from_api(raw: dict) -> str:
    """Detect tournament vs general from the live API JSON when no local record exists."""
    try:
        projects = (raw or {}).get("projects", {})
        if projects.get("default_project", {}).get("type") == "tournament":
            return "tournament"
        if any(t.get("type") == "tournament" for t in projects.get("tournament", [])):
            return "tournament"
    except Exception:
        pass
    return "general"


def classify_bucket(row: dict) -> str:
    """One of: open / resolved_scored / resolved_unscored / not_found_live."""
    if not row["live_match_found"]:
        return "not_found_live"
    if not row["resolved"]:
        return "open"
    if row["peer_score"] is not None:
        return "resolved_scored"
    return "resolved_unscored"


# ─── Page assembly ──────────────────────────────────────────────────────────
def build_profile_data(account_key: str) -> dict:
    account = ACCOUNTS[account_key]
    client = account["client"]
    local = load_local_results(account["local_dirs"]) if account["local_dirs"] else {}
    live_by_qid = fetch_my_predicted_questions(client)
    rows = []
    seen_qids = set()

    for qid, r in sorted(local.items(), key=lambda kv: kv[0], reverse=True):
        post = live_by_qid.get(qid)
        score = extract_score_info(post) if post else extract_score_info(None)
        q_type = r.get("question_type") or ("binary" if r.get("category", "general") == "general" else None)
        rows.append({
            "question_id": qid,
            "question_text": r.get("question_text", score["title"] or "(unknown)"),
            "question_type": q_type,
            "status": r.get("status"),
            "submitted_summary": summarize_forecast(
                q_type, r.get("submitted_forecast", r.get("probability"))
            ),
            "resolved": score["resolved"],
            "resolution": score["resolution"],
            "peer_score": score["peer_score"],
            "close_time": score["close_time"],
            "live_match_found": post is not None,
            "category": r.get("category", "general"),
        })
        seen_qids.add(qid)

    # Anything this account predicted on live with no local record at all
    # (manual predictions, or predictions made before logging existed).
    for qid, post in live_by_qid.items():
        if qid in seen_qids:
            continue
        score = extract_score_info(post)
        rows.append({
            "question_id": qid,
            "question_text": score["title"] or "(unknown)",
            "question_type": (post.get("question") or {}).get("type"),
            "status": "n/a (no local record)",
            "submitted_summary": "—",
            "resolved": score["resolved"],
            "resolution": score["resolution"],
            "peer_score": score["peer_score"],
            "close_time": score["close_time"],
            "live_match_found": True,
            "category": detect_category_from_api(post),
        })

    for row in rows:
        row["bucket"] = classify_bucket(row)

    rows.sort(key=lambda r: r["question_id"], reverse=True)

    buckets = {
        "open": [r for r in rows if r["bucket"] == "open"],
        "resolved_scored": [r for r in rows if r["bucket"] == "resolved_scored"],
        "resolved_unscored": [r for r in rows if r["bucket"] == "resolved_unscored"],
        "not_found_live": [r for r in rows if r["bucket"] == "not_found_live"],
    }
    avg_score = (
        sum(r["peer_score"] for r in buckets["resolved_scored"])
        / len(buckets["resolved_scored"])
    ) if buckets["resolved_scored"] else None

    chart_points = [
        {"x": r["close_time"], "y": r["peer_score"]}
        for r in buckets["resolved_scored"] if r["close_time"]
    ]

    return {
        "rows": rows,
        "buckets": buckets,
        "avg_score": avg_score,
        "chart_points": chart_points,
        "total_predicted": len(rows),
        "user_id": get_confirmed_user_id(client),
        "label": account["label"],
        "token_configured": client is not None,
    }


# ─── Routes ─────────────────────────────────────────────────────────────────
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
    .tabs { margin-bottom: 20px; }
    .tabs a { padding: 8px 16px; border-radius: 6px; text-decoration: none; color: #333;
              background: #e8e8ec; margin-right: 8px; font-size: 14px; }
    .tabs a.active { background: #2563eb; color: white; }
    .cards { display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
    .card { background: white; border-radius: 8px; padding: 16px 20px; box-shadow: 0 1px 3px rgba(0,0,0,.08);
            min-width: 140px; }
    .card .label { font-size: 12px; color: #888; }
    .card .value { font-size: 24px; font-weight: 600; }
    table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px;
            overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.08); font-size: 13px; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #fafafa; color: #666; font-weight: 600; }
    tr:hover { background: #fafbfc; }
    .pos { color: #16a34a; font-weight: 600; }
    .neg { color: #dc2626; font-weight: 600; }
    .muted { color: #999; }
    .chart-wrap { background: white; border-radius: 8px; padding: 16px; margin-bottom: 8px;
                  height: 280px; }
    .chart-note { color: #999; font-size: 12px; margin: 0 0 24px; }
    a.raw { font-size: 11px; color: #888; }
    .bucket-explainer { color: #999; font-size: 12px; margin: -8px 0 20px; }
  </style>
</head>
<body>
  <h1>Metaculus Track Record</h1>
  <div class="sub">
    Account: {{ data.label }}{% if data.user_id %} (user id {{ data.user_id }}){% endif %}
    {% if not data.token_configured %} — ⚠️ no token configured for this account in .env{% endif %}
  </div>

  <div class="tabs">
    <a href="/?profile=personal" class="{{ 'active' if profile == 'personal' else '' }}">mike_iz_ (personal)</a>
    <a href="/?profile=bot" class="{{ 'active' if profile == 'bot' else '' }}">mike_iz_-bot</a>
  </div>

  <div class="cards">
    <div class="card"><div class="label">Total predicted</div><div class="value">{{ data.total_predicted }}</div></div>
    <div class="card"><div class="label">Open</div><div class="value">{{ data.buckets.open|length }}</div></div>
    <div class="card"><div class="label">Resolved &amp; scored</div><div class="value">{{ data.buckets.resolved_scored|length }}</div></div>
    <div class="card"><div class="label">Resolved, no score</div><div class="value">{{ data.buckets.resolved_unscored|length }}</div></div>
    <div class="card"><div class="label">Withdrawn / not found live</div><div class="value">{{ data.buckets.not_found_live|length }}</div></div>
    <div class="card"><div class="label">Avg peer score</div>
      <div class="value {{ 'pos' if data.avg_score and data.avg_score > 0 else ('neg' if data.avg_score and data.avg_score < 0 else '') }}">
        {{ '%.2f'|format(data.avg_score) if data.avg_score is not none else '—' }}
      </div>
    </div>
  </div>
  <p class="bucket-explainer">
    Total predicted = Open + Resolved&amp;scored + Resolved,no score + Withdrawn/not found live.
    "Resolved, no score" = Annulled/Ambiguous (Metaculus doesn't score these) or scoring not yet posted.
    "Withdrawn / not found live" = no longer in this account's live forecasted-questions list at all.
  </p>

  <div class="chart-wrap" id="chartWrap" style="display:none;"><canvas id="scoreChart"></canvas></div>
  <p class="chart-note">X-axis = each question's <b>scheduled close date</b>, not the date you submitted a forecast.</p>
  <div id="noChartMsg" style="background:white;border-radius:8px;padding:24px;margin-bottom:24px;
       text-align:center;color:#999;box-shadow:0 1px 3px rgba(0,0,0,.08);">
    No resolved &amp; scored questions yet — the chart will appear once at least one shows a peer score.
  </div>

  {% macro render_rows(rows) %}
    {% for row in rows %}
    <tr>
      <td>{{ row.question_id }}</td>
      <td>{{ row.question_text[:70] }}</td>
      <td>{{ row.question_type or '—' }}</td>
      <td>{{ row.submitted_summary }}</td>
      <td>{{ '✅' if row.resolved else '⏳' }}</td>
      <td>{{ row.resolution if row.resolution is not none else '—' }}</td>
      <td class="{{ 'pos' if row.peer_score and row.peer_score > 0 else ('neg' if row.peer_score and row.peer_score < 0 else 'muted') }}">
        {{ '%.2f'|format(row.peer_score) if row.peer_score is not none else (row.resolved and 'unknown field — check raw' or '—') }}
      </td>
      <td>{{ row.category }}</td>
      <td><a class="raw" href="/raw/{{ profile }}/{{ row.question_id }}" target="_blank">raw</a></td>
    </tr>
    {% endfor %}
  {% endmacro %}

  {% macro section(title, rows) %}
  <h2 style="font-size:16px;margin:24px 0 10px;">{{ title }} ({{ rows|length }})</h2>
  <table>
    <tr>
      <th>ID</th><th>Question</th><th>Type</th><th>Submitted</th>
      <th>Resolved?</th><th>Resolution</th><th>Peer score</th><th>Category</th><th></th>
    </tr>
    {{ render_rows(rows) }}
  </table>
  {% endmacro %}

  {{ section('⏳ Open', data.buckets.open) }}
  {{ section('✅ Resolved &amp; scored', data.buckets.resolved_scored) }}
  {{ section('🚫 Resolved, no score (annulled / ambiguous / pending)', data.buckets.resolved_unscored) }}

  <details style="margin-top:24px;">
    <summary style="font-size:16px;cursor:pointer;padding:8px 0;color:#666;">
      🗄️ Withdrawn / not found live ({{ data.buckets.not_found_live|length }}) — click to expand
    </summary>
    <p style="color:#999;font-size:13px;margin:8px 0;">
      Submitted at the time, but no longer present in this account's current live
      "previously forecasted" list — most often because the forecast was withdrawn
      before close.
    </p>
    <table>
      <tr>
        <th>ID</th><th>Question</th><th>Type</th><th>Submitted</th>
        <th>Resolved?</th><th>Resolution</th><th>Peer score</th><th>Category</th><th></th>
      </tr>
      {{ render_rows(data.buckets.not_found_live) }}
    </table>
  </details>

  <script>
    const points = {{ chart_points|tojson }};
    if (points.length > 0) {
      document.getElementById('chartWrap').style.display = 'block';
      document.getElementById('noChartMsg').style.display = 'none';
      new Chart(document.getElementById('scoreChart'), {
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
  </script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    profile = request.args.get("profile", "personal")
    if profile not in ACCOUNTS:
        profile = "personal"
    data = build_profile_data(profile)
    return render_template_string(
        PAGE_TEMPLATE,
        data=data,
        profile=profile,
        chart_points=data["chart_points"],
    )


@app.route("/raw/<profile>/<int:question_id>")
def raw(profile, question_id):
    if profile not in ACCOUNTS:
        return jsonify({"_error": f"unknown profile '{profile}'"})
    client = ACCOUNTS[profile]["client"]
    posts = fetch_my_predicted_questions(client)
    if question_id in posts:
        return jsonify(posts[question_id])
    return jsonify({"_error": f"Question id {question_id} not found among the {profile} "
                              f"account's predicted questions. It may not have a forecast "
                              f"registered yet under this token, or was withdrawn."})


if __name__ == "__main__":
    if not PERSONAL_TOKEN and not BOT_TOKEN:
        print("⚠️  Neither METACULUS_TOKEN nor METAC_TOURNAMENT_TOKEN found in .env — "
              "both tabs will be empty.")
    # use_reloader=False: the default reloader watches the whole working
    # directory recursively for .py changes, which includes venv312/ sitting
    # inside this same project folder — any package writing to its own files
    # was triggering constant restarts. debug=True still gives in-browser
    # tracebacks, just without auto-restart.
    app.run(port=5002, debug=True, use_reloader=False)