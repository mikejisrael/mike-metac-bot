"""
meta_watch.py — two alerting checks, run alongside tournament_forecast.py
(same cadence — both are called from its run()):

1. check_new_futureeval_questions(): alerts the moment a FutureEval question
   you haven't seen before appears as open. Built specifically to make the
   CP-fetch fix verification (api2/questions/?ids= chunked endpoint, fixed
   2026-06-29 but never proven against a live FutureEval question with real
   CP values, since FutureEval had no open questions at the time) actionable
   the moment a testable question exists, instead of relying on remembering
   to check back manually.

2. check_resolutions(): alerts the moment a question mike_iz_-bot has
   forecast resolves. Scoped to bot-submitted forecasts ONLY — entries
   explicitly tagged account="personal" are skipped. meta_refresh_forecast.py's
   --single path is currently the only writer that tags personal-account
   results (see that file's module docstring for why --single always
   authenticates as mike_iz_, not the bot); everything else in this
   codebase (meta_batch_forecast.py, tournament_forecast.py itself) submits
   via the bot token and is included.

Resolution field names below (status/resolution/actual_resolve_time) were
checked against Metaculus's own public API examples before writing this —
not guessed — but this is still the first time this codebase has read
these specific fields, so the first real resolution alert is worth
eyeballing against the actual question page once, the same way the CP-fetch
fix needs a live check.

Both checks persist their own small state files under watch_state/ so they
only ever alert once per event, not every run.
"""

import os
import json
import time
import glob
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from meta_alerts import send_alert

WATCH_DIR = "watch_state"
os.makedirs(WATCH_DIR, exist_ok=True)

FUTUREEVAL_SEEN_FILE   = os.path.join(WATCH_DIR, "futureeval_seen_posts.json")
RESOLUTION_STATE_FILE  = os.path.join(WATCH_DIR, "resolution_state.json")

FUTUREEVAL_TOURNAMENT_ID = 33022

# Bounds a single run's worth of resolution-checking API calls (at ~1.2s
# each, 150 = ~3 minutes). First run against existing history (~238
# tracked forecasts) will take 2 runs to fully catch up; every run after
# that only re-checks whatever's still unresolved, which shrinks over time
# as questions actually resolve — so this cap matters most on the very
# first run and barely matters afterward.
MAX_RESOLUTION_CHECKS_PER_RUN = 150

# Same token-selection logic used everywhere else in this codebase.
WATCH_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
_HEADERS = {
    "Authorization": f"Token {WATCH_TOKEN}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}


# ─── Shared helpers ─────────────────────────────────────────────────────────
def _get_with_retry(url: str, max_attempts: int = 3, timeout: int = 30):
    """Same connection-error + 429 retry pattern added to
    tournament_forecast.py's fetch loop, kept as its own small copy here
    rather than imported — tournament_forecast.py has import-time side
    effects (client construction, monkeypatching) that shouldn't fire just
    to borrow one helper function."""
    for attempt in range(max_attempts):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait = 5 * (attempt + 1)
            print(f"    ⏳ connection error ({type(e).__name__}), waiting {wait}s "
                  f"before retry {attempt + 1}/{max_attempts}...")
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"    ⏳ rate limited (429), waiting {wait}s before retry "
                  f"{attempt + 1}/{max_attempts}...")
            time.sleep(wait)
            continue
        return r
    return None


def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  Warning: could not load {path}: {e}")
        return default


def _save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ─── 1. New FutureEval question alert ──────────────────────────────────────
def check_new_futureeval_questions(raw_posts_by_id: dict) -> None:
    """raw_posts_by_id: {post_id: raw_post_dict}, scoped to FutureEval ONLY.
    Caller (tournament_forecast.py's fetch loop) is responsible for this
    scoping — that loop already separates posts per-tournament before
    merging everything into its own flat all-tournaments dict, so this
    function never sees ACX2026/Climate/Metaculus Cup posts.

    FIXED 2026-06-30: previously sent ONE individual ntfy POST per newly-
    detected question with zero delay between calls. On the very first run
    against a tournament with many already-open questions, every single
    one registers as "new" against an empty seen-state file simultaneously
    — confirmed live: 158 FutureEval questions on a fresh watch_state file
    fired 158 rapid-fire individual alerts, which hit ntfy.sh's rate limit
    partway through (77 of 158 actually landed, the rest 429'd) and took
    real wall-clock time away from FutureEval's tight close windows for no
    benefit. Now batches all new questions from a single check into ONE
    alert. The very-first-ever-run case (a large existing backlog, not
    genuinely "new" events) gets special handling: silently seed the watch
    list with one short summary notification instead of listing all of
    them — only questions that appear between two CONSECUTIVE runs from
    here on are genuinely new events worth detailing individually in the
    alert body."""
    seen = set(_load_json(FUTUREEVAL_SEEN_FILE, []))
    new_ids = [pid for pid in raw_posts_by_id if pid not in seen]
    is_first_run = len(seen) == 0 and len(new_ids) > 0

    if new_ids:
        if is_first_run:
            print(f"  📬 First-ever run: seeding watch list with {len(new_ids)} "
                  f"currently-open FutureEval question(s) — sending ONE summary "
                  f"alert, not {len(new_ids)} individual ones.")
            send_alert(
                f"Watch list initialized with {len(new_ids)} currently-open "
                f"FutureEval question(s). From now on you'll get an alert "
                f"only for genuinely NEW questions between runs.",
                title="FutureEval watch list initialized"
            )
        else:
            print(f"  📬 {len(new_ids)} new FutureEval question(s) detected — "
                  f"alerting (single batched notification)...")
            MAX_LISTED = 15  # ntfy has a practical body-size limit, and a
            # notification listing 50+ questions stops being readable anyway
            lines = []
            for pid in new_ids[:MAX_LISTED]:
                post = raw_posts_by_id[pid]
                title = (post.get("question") or {}).get("title") or post.get("title") or "Unknown title"
                lines.append(f"- {title[:100]}\n  https://www.metaculus.com/questions/{pid}/")
            body = "\n".join(lines)
            if len(new_ids) > MAX_LISTED:
                body += f"\n...and {len(new_ids) - MAX_LISTED} more."
            send_alert(body, title=f"{len(new_ids)} new FutureEval question(s)")
    else:
        print(f"  No new FutureEval questions since last check ({len(seen)} known).")

    seen.update(raw_posts_by_id.keys())
    _save_json(FUTUREEVAL_SEEN_FILE, sorted(seen))


# ─── 2. Resolution alert (bot-submitted forecasts only) ────────────────────
def _load_bot_forecasts() -> dict:
    """Scan both batch dirs for bot-submitted forecasts (anything NOT
    explicitly tagged account="personal"). Returns
    {question_id: {post_id, question_text, probability, source_file}},
    deduped to the most recently loaded entry per question_id (files are
    sorted, so later == newer, matching show_reasoning.py's convention)."""
    forecasts: dict = {}
    result_files = sorted(
        f for d in ("Meta batches", "tournament_batches")
        for f in glob.glob(os.path.join(d, "batch_results*.json"))
    )
    for rf in result_files:
        data = _load_json(rf, {})
        for item in data.values():
            if item.get("account") == "personal":
                continue  # meta_refresh_forecast.py --single — not bot, skip
            q_id = item.get("question_id")
            post_id = item.get("post_id")
            if not q_id or not post_id:
                continue  # no post_id on file (pre-fix history) — can't check, skip
            forecasts[q_id] = {
                "post_id":        post_id,
                "question_text":  item.get("question_text", ""),
                "probability":    item.get("probability") or item.get("submitted_forecast"),
                "source_file":    rf,
            }
    return forecasts


def check_resolutions() -> None:
    """For every bot-submitted forecast not already known-resolved, check
    its current status via the singular /api2/questions/{post_id}/ detail
    endpoint. Alerts once per question the first time it's found resolved,
    never again after that (tracked in RESOLUTION_STATE_FILE)."""
    bot_forecasts = _load_bot_forecasts()
    state = _load_json(RESOLUTION_STATE_FILE, {})

    to_check = [
        (q_id, info) for q_id, info in bot_forecasts.items()
        if not state.get(str(q_id), {}).get("alerted")
    ]
    capped = to_check[:MAX_RESOLUTION_CHECKS_PER_RUN]
    print(f"  Checking resolution status for {len(capped)}/{len(to_check)} not-yet-resolved "
          f"bot forecast(s) this run (of {len(bot_forecasts)} total tracked)...")

    newly_resolved = 0
    for i, (q_id, info) in enumerate(capped, 1):
        post_id = info["post_id"]
        url = f"https://www.metaculus.com/api2/questions/{post_id}/"
        r = _get_with_retry(url)
        if r is None or r.status_code != 200:
            print(f"    ❌ Q{q_id} (post {post_id}): could not fetch — will retry next run.")
            continue

        try:
            data = r.json()
        except Exception:
            print(f"    ❌ Q{q_id} (post {post_id}): non-JSON response — will retry next run.")
            continue

        # Multiple signals checked defensively rather than trusting one
        # field alone — consistent with this codebase's existing approach
        # to Metaculus API fields (see extract_live_cp, _latest_centers).
        is_resolved = (
            data.get("status") == "resolved"
            or data.get("resolution") is not None
            or data.get("actual_resolve_time") is not None
        )

        if is_resolved:
            newly_resolved += 1
            resolution_value = data.get("resolution")
            prob = info.get("probability")
            prob_str = f"{prob:.0%}" if isinstance(prob, (int, float)) else "n/a"
            send_alert(
                f"Q{q_id}: {info['question_text'][:100]}\n"
                f"Resolved: {resolution_value}\n"
                f"You forecast: {prob_str}",
                title="✅ Forecast resolved"
            )
            state[str(q_id)] = {
                "alerted":     True,
                "resolution":  resolution_value,
                "checked_at":  datetime.now(timezone.utc).isoformat(),
            }
        else:
            state[str(q_id)] = {
                "alerted":     False,
                "checked_at":  datetime.now(timezone.utc).isoformat(),
            }

        if i % 10 == 0:
            print(f"    ...{i}/{len(capped)} checked")
        time.sleep(1.2)  # same politeness delay used elsewhere in this codebase

    _save_json(RESOLUTION_STATE_FILE, state)
    print(f"  Resolution check complete: {newly_resolved} newly resolved (alerted), "
          f"{len(capped) - newly_resolved} still open/pending"
          f"{f', {len(to_check) - len(capped)} deferred to next run (cap reached)' if len(to_check) > len(capped) else ''}.")
