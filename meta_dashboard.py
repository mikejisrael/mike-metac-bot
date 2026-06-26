"""
meta_dashboard.py — Forecasting track-record dashboard (Metaculus side),
mirroring the bybit_sim.py / bybit_dashboard.py pattern: a small local Flask
app that collates everything in one page instead of clicking through
Metaculus's own UI question-by-question.

IMPORTANT CAVEAT — read this before assuming the live score numbers are
exact: I built the live-score lookup (fetch_question_live / extract_score_info
below) without being able to authenticate against the real Metaculus API from
my environment, so the exact field names Metaculus uses for an individual
forecaster's peer/baseline score on a resolved question are a best-effort
guess based on third-party docs, not a verified schema. Use the "raw" debug
link next to each resolved question to see the actual JSON Metaculus returns
— if extract_score_info() picked the wrong field, that raw view tells us
exactly what to fix.

Two data sources, combined differently per profile:
  - BOT profile:      local batch_results_*.json (tournament_batches/ and
                       "Meta batches/") for question text/type/status/the
                       actual submitted forecast, ENRICHED with a live
                       per-question lookup for resolution + score once
                       available.
  - PERSONAL profile:  live API only (?guessed_by=<your user id>) — there's
                       no local log of your own manual predictions, so this
                       tab is entirely Metaculus-API-driven.

Run:
  python meta_dashboard.py
Then open http://localhost:5002
"""

import os
import glob
import json
import asyncio
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from forecasting_tools import MetaculusClient, ApiFilter

load_dotenv()

app = Flask(__name__)

METACULUS_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if os.getenv("METAC_TOURNAMENT_TOKEN"):
    print("Auth: using METAC_TOURNAMENT_TOKEN (mike_iz_-bot)")
else:
    print("Auth: METAC_TOURNAMENT_TOKEN not set — falling back to METACULUS_TOKEN (mike_iz_)")

TOURNAMENT_ID = os.getenv("METAC_TOURNAMENT_ID", "33022")

# Confirmed via a Metaculus compliance email (549 API forecasts flagged on
# this account): there is only ONE real account behind all the automation —
# mike_iz_, 302314. The earlier "bot account 303026" was not a separate
# posting identity; both local result folders and all live API activity
# belong to this single account. No profile split needed unless/until a
# genuinely separate account is registered for manual-only forecasting.
ACCOUNT_USER_ID = int(os.getenv("METAC_USER_ID", "302314"))  # fallback display value only


def get_confirmed_user_id() -> int:
    """Ask the API directly which account this token actually belongs to,
    rather than trusting a hardcoded guess. Falls back to ACCOUNT_USER_ID
    only if the call itself fails (e.g. no network)."""
    try:
        return ft_client.get_current_user_id()
    except Exception as e:
        print(f"  get_confirmed_user_id failed, falling back to configured default: {e}")
        return ACCOUNT_USER_ID
LOCAL_RESULT_DIRS = ["tournament_batches", "Meta batches"]


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


# ─── Live Metaculus API ─────────────────────────────────────────────────────
# ─── Live Metaculus API ─────────────────────────────────────────────────────
# Using forecasting_tools' own MetaculusClient + ApiFilter rather than the
# guessed_by list-filter trick from earlier — that trick came from browser
# bookmarklets relying on cookie/session auth, and returned a genuine 403
# when used with Token auth from this account. is_previously_forecasted_by_user
# is a real, documented ApiFilter field, and it's the same client class
# batch_forecast.py already uses successfully for its own question fetching.
ft_client = MetaculusClient(token=METACULUS_TOKEN)


def fetch_my_predicted_questions() -> dict[int, dict]:
    """All questions the authenticated account (whoever METACULUS_TOKEN
    belongs to) has predicted on, keyed by question id. Returns each
    question's raw api_json — same dict shape extract_score_info() already
    expects, so no parsing changes needed downstream."""
    try:
        api_filter = ApiFilter(is_previously_forecasted_by_user=True)
        questions = asyncio.run(
            ft_client.get_questions_matching_filter(
                api_filter, num_questions=1000, error_if_question_target_missed=False
            )
        )
    except Exception as e:
        print(f"  fetch_my_predicted_questions failed: {e}")
        return {}
    by_qid = {q.id_of_question: q.api_json for q in questions}
    print(f"  fetch_my_predicted_questions: collected {len(by_qid)} questions")
    return by_qid





def extract_score_info(raw: dict) -> dict:
    """Extraction based on the confirmed real schema (seen via /raw debug
    output): top-level 'resolved' bool and 'status', nested question.resolution,
    and an individual forecaster's score living under question.my_forecasts
    (which is only populated for whichever account METACULUS_TOKEN
    authenticates as — this will be empty for the personal-account tab
    unless a second token for that account is configured)."""
    info = {"resolved": False, "resolution": None, "peer_score": None,
            "baseline_score": None, "close_time": None, "title": None}
    if not raw or "_error" in raw:
        return info

    q = raw.get("question", raw)
    info["title"] = raw.get("title") or q.get("title")
    info["close_time"] = raw.get("scheduled_close_time") or q.get("scheduled_close_time")
    # Top-level 'resolved' is the authoritative flag where present; fall back
    # to "does the nested question have a non-null resolution" otherwise.
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
    table — same shapes tournament_forecast.py now logs."""
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
            # Older entries (e.g. Q44150, corrected via a one-off resubmit
            # script before the full-CDF audit trail existed) only ever
            # logged the median value itself, not the full distribution.
            return f"median≈{forecast:,.0f}"
        if q_type == "multiple_choice" and isinstance(forecast, dict):
            top = max(forecast, key=forecast.get)
            return f"{top} ({forecast[top]:.0%})"
    except Exception:
        pass
    return str(forecast)[:60]


# ─── Page assembly ──────────────────────────────────────────────────────────
def build_profile_data(local_dirs: list[str], profile_name: str) -> dict:
    local = load_local_results(local_dirs)
    live_by_qid = fetch_my_predicted_questions()
    rows = []

    # Local-first: every locally-logged question, enriched with live data if found
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

    # Anything this account predicted on live but with no local record at all
    # (e.g. predictions made before logging existed, or made manually).
    # Can't reliably tell tournament vs general for these — default general.
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
            "category": "general",
        })

    rows.sort(key=lambda r: r["question_id"], reverse=True)
    return {"rows": rows, "profile": profile_name}


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
    .cards { display: flex; gap: 16px; margin-bottom: 24px; }
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
    .chart-wrap { background: white; border-radius: 8px; padding: 16px; margin-bottom: 24px;
                  box-shadow: 0 1px 3px rgba(0,0,0,.08); height: 280px; }
    a.raw { font-size: 11px; color: #888; }
  </style>
</head>
<body>
  <h1>Metaculus Track Record</h1>
  <div class="sub">Account: mike_iz_ ({{ user_id }}). All local result folders + live Metaculus scores, one account — confirmed there's no separate bot identity behind the automation.</div>

  <div class="cards">
    <div class="card"><div class="label">Total questions</div><div class="value">{{ tournament_rows|length + general_rows|length + archived_rows|length }}</div></div>
    <div class="card"><div class="label">Resolved</div><div class="value">{{ resolved_count }}</div></div>
    <div class="card"><div class="label">Avg peer score</div>
      <div class="value {{ 'pos' if avg_score and avg_score > 0 else ('neg' if avg_score and avg_score < 0 else '') }}">
        {{ '%.2f'|format(avg_score) if avg_score is not none else '—' }}
      </div>
    </div>
  </div>

  <div class="chart-wrap" id="chartWrap" style="display:none;"><canvas id="scoreChart"></canvas></div>
  <div id="noChartMsg" style="background:white;border-radius:8px;padding:24px;margin-bottom:24px;
       text-align:center;color:#999;box-shadow:0 1px 3px rgba(0,0,0,.08);">
    No resolved &amp; scored questions yet — the chart will appear once at least one shows a peer score.
  </div>

  {% macro render_rows(rows, show_category=false) %}
    {% for row in rows %}
    <tr>
      <td>{{ row.question_id }}</td>
      <td>{{ row.question_text[:70] }}</td>
      <td>{{ row.question_type or '—' }}</td>
      <td>{{ row.submitted_summary }}</td>
      <td>{{ row.status }}</td>
      <td>{{ '✅' if row.resolved else '⏳' }}</td>
      <td>{{ row.resolution if row.resolution is not none else '—' }}</td>
      <td class="{{ 'pos' if row.peer_score and row.peer_score > 0 else ('neg' if row.peer_score and row.peer_score < 0 else 'muted') }}">
        {{ '%.2f'|format(row.peer_score) if row.peer_score is not none else (row.resolved and 'unknown field — check raw' or '—') }}
      </td>
      {% if show_category %}<td>{{ row.category }}</td>{% endif %}
      <td><a class="raw" href="/raw/{{ row.question_id }}" target="_blank">raw</a></td>
    </tr>
    {% endfor %}
  {% endmacro %}

  <h2 style="font-size:16px;margin:24px 0 10px;">🏆 Tournament questions ({{ tournament_rows|length }})</h2>
  <table>
    <tr>
      <th>ID</th><th>Question</th><th>Type</th><th>Submitted</th><th>Status</th>
      <th>Resolved?</th><th>Resolution</th><th>Peer score</th><th></th>
    </tr>
    {{ render_rows(tournament_rows) }}
  </table>

  <h2 style="font-size:16px;margin:24px 0 10px;">📋 General questions ({{ general_rows|length }})</h2>
  <table>
    <tr>
      <th>ID</th><th>Question</th><th>Type</th><th>Submitted</th><th>Status</th>
      <th>Resolved?</th><th>Resolution</th><th>Peer score</th><th></th>
    </tr>
    {{ render_rows(general_rows) }}
  </table>

  <details style="margin-top:24px;">
    <summary style="font-size:16px;cursor:pointer;padding:8px 0;color:#666;">
      🗄️ Withdrawn / no longer active ({{ archived_rows|length }}) — click to expand
    </summary>
    <p style="color:#999;font-size:13px;margin:8px 0;">
      These were successfully submitted at the time but no longer show up in this account's
      currently-forecasted questions — almost always because they were withdrawn (e.g. during
      the period auto-withdrawal was enabled on this account), not because anything failed.
    </p>
    <table>
      <tr>
        <th>ID</th><th>Question</th><th>Type</th><th>Submitted</th><th>Status</th>
        <th>Resolved?</th><th>Resolution</th><th>Peer score</th><th>Category</th><th></th>
      </tr>
      {{ render_rows(archived_rows, true) }}
    </table>
  </details>

  <script>
    const points = {{ chart_points|tojson }};
    if (points.length > 0) {
      document.getElementById('chartWrap').style.display = 'block';
      document.getElementById('noChartMsg').style.display = 'none';
      new Chart(document.getElementById('scoreChart'), {
        type: 'scatter',
        data: { datasets: [{ label: 'Peer score (resolved questions)', data: points,
                              backgroundColor: '#2563eb' }] },
        options: {
          responsive: true, maintainAspectRatio: false,
          scales: {
            x: { type: 'time', title: { display: true, text: 'Scheduled close time' } },
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
    data = build_profile_data(LOCAL_RESULT_DIRS, "mike_iz_")
    rows = data["rows"]

    tournament_rows = [r for r in rows if r["category"] == "tournament" and r["live_match_found"]]
    general_rows = [r for r in rows if r["category"] == "general" and r["live_match_found"]]
    archived_rows = [r for r in rows if not r["live_match_found"]]

    resolved_rows = [r for r in rows if r["resolved"]]
    scored_rows = [r for r in resolved_rows if r["peer_score"] is not None]
    avg_score = (sum(r["peer_score"] for r in scored_rows) / len(scored_rows)) if scored_rows else None

    chart_points = [
        {"x": r["close_time"], "y": r["peer_score"]}
        for r in scored_rows if r["close_time"]
    ]

    # Diagnostic for the local-vs-live count gap: of the rows NOT found live,
    # how many were locally marked "failed" to begin with? If that accounts
    # for most of the gap, the explanation is mundane (failed submissions
    # never existed on Metaculus's side). If most not-found rows are locally
    # "success", the gap is more likely withdrawals.
    not_found_rows = [r for r in rows if not r["live_match_found"]]
    not_found_failed = sum(1 for r in not_found_rows if r["status"] == "failed")
    not_found_success = sum(1 for r in not_found_rows if r["status"] == "success")
    print(f"  Local-vs-live gap diagnostic: {len(not_found_rows)} not found live — "
          f"{not_found_failed} were locally 'failed', {not_found_success} were locally "
          f"'success' (these are the ones likely withdrawn, not failed)")

    from flask import render_template_string
    return render_template_string(
        PAGE_TEMPLATE,
        tournament_rows=tournament_rows,
        general_rows=general_rows,
        archived_rows=archived_rows,
        resolved_count=len(resolved_rows),
        avg_score=avg_score,
        chart_points=chart_points,
        user_id=get_confirmed_user_id(),
    )


@app.route("/raw/<int:question_id>")
def raw(question_id):
    posts = fetch_my_predicted_questions()
    if question_id in posts:
        return jsonify(posts[question_id])
    return jsonify({"_error": f"Question id {question_id} not found among this account's "
                              f"predicted questions. It may not have a forecast registered "
                              f"yet under this token."})


if __name__ == "__main__":
    if not METACULUS_TOKEN:
        print("⚠️  Neither METAC_TOURNAMENT_TOKEN nor METACULUS_TOKEN found in .env — live score lookups will fail.")
    # use_reloader=False: the default reloader watches the whole working
    # directory recursively for .py changes, which includes venv312/ sitting
    # inside this same project folder — any package writing to its own files
    # was triggering constant restarts. debug=True still gives in-browser
    # tracebacks, just without auto-restart.
    app.run(port=5002, debug=True, use_reloader=False)