"""
meta_watch.py — three alerting checks, run alongside tournament_forecast.py
(same cadence — all three are called from its run()):

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

3. check_refresh_candidates() [added Phase 1, 2026-07-01]: alerts on
   still-open, bot-forecasted questions that look worth a manual refresh —
   either closing within CLOSING_SOON_HOURS, or where the live community
   prediction has moved by more than CP_SHIFT_THRESHOLD since your last
   submitted probability. ALERT-ONLY: this never triggers a refresh itself,
   you run meta_refresh_forecast.py by hand off the alert, same as the
   existing --check workflow. Deliberately does not touch FutureEval
   forecasting logic or auto-execute anything — this is purely a signal.

Resolution field names below (status/resolution/actual_resolve_time) were
checked against Metaculus's own public API examples before writing this —
not guessed — but this is still the first time this codebase has read
these specific fields, so the first real resolution alert is worth
eyeballing against the actual question page once, the same way the CP-fetch
fix needs a live check.

All three checks persist their own small state files under watch_state/ so
they only ever alert once per event, not every run.
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
REFRESH_STATE_FILE     = os.path.join(WATCH_DIR, "refresh_candidate_state.json")

FUTUREEVAL_TOURNAMENT_ID = 33022

# Bounds a single run's worth of resolution-checking API calls (at ~1.2s
# each, 150 = ~3 minutes). First run against existing history (~238
# tracked forecasts) will take 2 runs to fully catch up; every run after
# that only re-checks whatever's still unresolved, which shrinks over time
# as questions actually resolve — so this cap matters most on the very
# first run and barely matters afterward.
MAX_RESOLUTION_CHECKS_PER_RUN = 150

# Same cap pattern applied to refresh-candidate checking.
MAX_REFRESH_CHECKS_PER_RUN = 150

# Refresh-candidate thresholds — starting values, tune once you see real
# alert volume. Don't change these based on the first few alerts alone.
CLOSING_SOON_HOURS = 48
CP_SHIFT_THRESHOLD = 0.15
MIN_HOURS_BETWEEN_REFRESH_ALERTS = 24

# FIXED 2026-07-02: "closing soon" alone isn't a useful refresh signal for
# FutureEval — those questions open and close within ~3 hours by design
# (the "reveal and close in period" structure), so EVERY FutureEval
# question is always "closing soon" the moment it exists, immediately
# after being forecasted. What actually matters is whether the EXISTING
# forecast is stale relative to now — a forecast submitted minutes before
# close was never stale to begin with, no matter how tight the window.
# Requiring the forecast to be at least this many days old before
# "closing soon" counts as a real signal naturally excludes FutureEval
# (forecasts there are always minutes old) without hardcoding a tournament
# exclusion, and still catches genuinely stale forecasts on long-horizon
# tournaments (ACX2026, Climate, Metaculus Cup) that happen to be closing
# soon. Starting value — tune once you see real alert volume, same as the
# other thresholds above.
MIN_FORECAST_AGE_DAYS = 1.0

# Maps a tournament's numeric ID to the short label used in refresh-
# candidate notifications, so an alert can say which tournament a question
# belongs to. Kept in sync with meta_coverage_check.py's TOURNAMENTS dict.
TOURNAMENT_LABELS = {
    33022: "FutureEval",
    32880: "ACX2026",
     1756: "Climate Tipping Points",
    33021: "Metaculus Cup",
}

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


def _forecast_age_days(source_file: str, now: datetime):
    """Parses the YYYYMMDD_HHMM timestamp embedded in
    batch_results_YYYYMMDD_HHMM.json filenames (UTC — same convention
    show_reasoning.py relies on for 'later filename == newer' sorting) to
    estimate how long ago this forecast was submitted. Returns None if the
    filename doesn't match the expected pattern, rather than guessing."""
    import re
    m = re.search(r"batch_results_(\d{8})_(\d{4})", os.path.basename(source_file or ""))
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        return (now - dt).total_seconds() / 86400
    except Exception:
        return None


def _extract_tournament_label(data: dict) -> str:
    """Reads the tournament off the live per-question API response (same
    'projects.tournament' field seen in Metaculus's own post detail JSON),
    falling back to the API's own tournament name if it's not one of the
    four this codebase tracks by ID."""
    tournaments = (data.get("projects") or {}).get("tournament") or []
    if not tournaments:
        return "Other"
    tid = tournaments[0].get("id")
    if tid in TOURNAMENT_LABELS:
        return TOURNAMENT_LABELS[tid]
    return (tournaments[0].get("name") or "Other")[:20]


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
            print(f"  📋 First-ever run: seeding watch list with {len(new_ids)} "
                  f"currently-open FutureEval question(s). No push notification "
                  f"sent for this — it's not actionable, just a baseline. "
                  f"From now on you'll only get a push alert for genuinely "
                  f"NEW questions between runs.")
            # Deliberately no send_alert() call here. This event isn't
            # something to act on, and firing a push for it was confusing
            # next to the "new question" alert below, which IS actionable.
            # If this line is repeating on every run rather than printing
            # once ever, that's a sign watch_state/ isn't being committed
            # back to the repo — see tournament_forecast.yaml's commit step.
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
            send_alert(body, title=f"🆕 {len(new_ids)} new FutureEval question(s)")
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


# ─── 3. Refresh candidate alert (Phase 1, added 2026-07-01) ────────────────
def check_refresh_candidates() -> None:
    """Flags bot-forecasted, still-open questions that look worth a manual
    refresh: either closing soon, or where the live community prediction
    has moved meaningfully since your last submitted probability.

    ALERT-ONLY — this never triggers meta_refresh_forecast.py itself. You
    run --check manually off the alert, same as the existing workflow.

    Skips anything RESOLUTION_STATE_FILE already knows is resolved, to
    avoid a redundant API call for questions check_resolutions() has
    already confirmed are done — cuts this function's call volume roughly
    in half once your resolved history builds up.

    CP-shift check is binary-only for now: multiple_choice/numeric CP
    comparison isn't apples-to-apples against a single stored probability
    and would need separate logic — deferred until binary refresh triggers
    are proven useful. CP availability is also not guaranteed (see
    meta_dashboard.py's CP NOTE — bots are excluded from community
    aggregates on most tournaments), so this check is best-effort and will
    silently do nothing on questions where CP is null.
    """
    bot_forecasts = _load_bot_forecasts()
    resolution_state = _load_json(RESOLUTION_STATE_FILE, {})
    state = _load_json(REFRESH_STATE_FILE, {})
    now = datetime.now(timezone.utc)

    candidates_pool = [
        (q_id, info) for q_id, info in bot_forecasts.items()
        if not resolution_state.get(str(q_id), {}).get("alerted")
    ]
    capped = candidates_pool[:MAX_REFRESH_CHECKS_PER_RUN]
    print(f"  Checking {len(capped)}/{len(candidates_pool)} not-yet-resolved bot "
          f"forecast(s) for refresh signals...")

    candidates = []
    for i, (q_id, info) in enumerate(capped, 1):
        post_id = info["post_id"]
        url = f"https://www.metaculus.com/api2/questions/{post_id}/"
        r = _get_with_retry(url)
        if r is None or r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue

        q = data.get("question", data)
        if q.get("resolution") is not None or data.get("status") == "resolved":
            continue  # resolved since last check_resolutions run — not our concern here

        tournament_label = _extract_tournament_label(data)
        reasons = []

        close_time_str = data.get("scheduled_close_time") or q.get("scheduled_close_time")
        if close_time_str:
            try:
                close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                hours_left = (close_time - now).total_seconds() / 3600
                if 0 < hours_left <= CLOSING_SOON_HOURS:
                    age_days = _forecast_age_days(info.get("source_file"), now)
                    # Only a real signal if the EXISTING forecast is itself
                    # old — see MIN_FORECAST_AGE_DAYS above for why this
                    # naturally excludes FutureEval without hardcoding it.
                    if age_days is None or age_days >= MIN_FORECAST_AGE_DAYS:
                        age_str = f"forecast {age_days:.1f}d old" if age_days is not None else "forecast age unknown"
                        reasons.append(f"closing in {hours_left:.0f}h, {age_str}")
            except Exception:
                pass

        agg = q.get("aggregations", {}) or {}
        node = agg.get("recency_weighted") or agg.get("metaculus_prediction") or {}
        cp_latest = node.get("latest")
        submitted = info.get("probability")
        if (isinstance(cp_latest, (int, float)) and isinstance(submitted, (int, float))
                and abs(cp_latest - submitted) >= CP_SHIFT_THRESHOLD):
            reasons.append(f"CP moved to {cp_latest:.0%} vs your {submitted:.0%}")

        if reasons:
            last_alerted = state.get(str(q_id), {}).get("alerted_at")
            skip = False
            if last_alerted:
                try:
                    last_dt = datetime.fromisoformat(last_alerted)
                    if (now - last_dt).total_seconds() / 3600 < MIN_HOURS_BETWEEN_REFRESH_ALERTS:
                        skip = True
                except Exception:
                    pass
            if not skip:
                candidates.append((q_id, info, reasons, tournament_label))
                state[str(q_id)] = {"alerted_at": now.isoformat(), "reasons": reasons}

        if i % 10 == 0:
            print(f"    ...{i}/{len(capped)} checked")
        time.sleep(1.2)

    if candidates:
        MAX_LISTED = 15
        lines = []
        for q_id, info, reasons, tournament_label in candidates[:MAX_LISTED]:
            lines.append(f"- Q{q_id} [{tournament_label}]: {info['question_text'][:70]} — {', '.join(reasons)}")
        body = "\n".join(lines)
        if len(candidates) > MAX_LISTED:
            body += f"\n...and {len(candidates) - MAX_LISTED} more."
        send_alert(body, title=f"🔄 {len(candidates)} refresh candidate(s)")
        print(f"  📬 {len(candidates)} refresh candidate(s) — alerted.")
    else:
        print("  No refresh candidates this run.")

    _save_json(REFRESH_STATE_FILE, state)