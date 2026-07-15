"""
meta_refresh_schedule.py - Refresh-scheduling logic for the batch-path tournaments (ACX2026,
Climate, Metaculus Cup, the 5 question_series) — the "when is a question
due for a refresh" question, and nothing else.

EXTRACTED 2026-07-15 from meta_refresh_forecast.py, into its own module,
specifically so meta_dashboard.py can import it. meta_refresh_forecast.py
instantiates the Anthropic and Metaculus API clients (and monkey-patches
MetaculusClient) at MODULE level, not inside a function — importing
anything from that file directly would silently run all of that setup
every time the dashboard starts or reloads its cache: extra client init,
extra log noise, and a real risk of the dashboard failing to start if
something about that setup ever hiccups. This module has NO such
side effects — no client instantiation, no load_dotenv, nothing beyond
stdlib (json/os/datetime) — so it's safe for the dashboard to import
freely. Both meta_refresh_forecast.py and meta_dashboard.py import from
here, so they can never disagree about what "due for refresh" means —
agreement by construction, not by convention.

Design (Mike's decisions, 2026-07-15): two independent triggers, either
one is sufficient:
  A) routine  — now >= refresh_after, where refresh_after defaults to
     (last forecast time + REFRESH_AFTER_DEFAULT_DAYS) and can be
     overridden per-question via the dashboard (writes to
     watch_state/refresh_overrides.json).
  B) final safety net — within FINAL_REFRESH_WINDOW_HOURS hours of close,
     mirroring tournament_forecast_v2.py's Market Pulse "final hour before
     close" trigger — just with a much longer window, since these
     tournaments' questions run for weeks/months rather than Market
     Pulse's 59-155-hour sub-question lifespans, so a flat 60-minute
     window would rarely fire in time to matter here. 48h chosen as a
     reasonable default — trigger B exists so a question whose close date
     arrives BEFORE its 30-day timer matures (e.g. forecast 5 days ago,
     closes in 10 days) still gets one last refresh rather than locking in
     a month-old forecast.
"""
import json
import os
from datetime import datetime, timezone, timedelta

REFRESH_AFTER_DEFAULT_DAYS = 30
FINAL_REFRESH_WINDOW_HOURS = 48

REFRESH_OVERRIDES_PATH = os.path.join("watch_state", "refresh_overrides.json")


def load_refresh_overrides() -> dict:
    """watch_state/refresh_overrides.json — optional per-question manual
    overrides for refresh_after, keyed by question_id (string):
        {"44667": {"refresh_after": "2026-08-20T00:00:00Z", "set_at": "...", "note": "..."}}
    Written by the dashboard when Mike edits a question's refresh date via
    the UI; read here to compute each question's EFFECTIVE refresh_after.
    Deliberately a thin standalone file rather than a field on the
    batch_jobs/batch_results history — those are an append-only forecast
    record, and a mutable "when should this run again" scheduling field
    doesn't belong mixed into that. Same None-safe-default pattern as
    load_excluded_ids/load_refresh_candidate_state elsewhere — the
    scheduling layer should never break other things if this file doesn't
    exist yet."""
    try:
        with open(REFRESH_OVERRIDES_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_refresh_override(question_id: int, refresh_after: datetime | None, note: str = "") -> dict:
    """Write (or clear, if refresh_after is None) one question's manual
    override. Returns the full updated overrides dict. Used by the
    dashboard's /set_refresh_after route."""
    overrides = load_refresh_overrides()
    q_id = str(question_id)
    if refresh_after is None:
        overrides.pop(q_id, None)
    else:
        overrides[q_id] = {
            "refresh_after": refresh_after.isoformat(),
            "set_at": datetime.now(timezone.utc).isoformat(),
            "note": note,
        }
    os.makedirs("watch_state", exist_ok=True)
    with open(REFRESH_OVERRIDES_PATH, "w") as f:
        json.dump(overrides, f, indent=2)
    return overrides


def compute_refresh_after(forecast: dict, overrides: dict | None = None) -> datetime | None:
    """Effective refresh_after for one forecast record (see module
    docstring for the two-trigger design): a manual override from
    refresh_overrides.json if one is set for this question_id, else (last
    forecast time + REFRESH_AFTER_DEFAULT_DAYS). Returns None only if
    there's no override AND submitted_at is missing/unparseable — callers
    should treat that as "unknown, don't auto-flag via trigger A" rather
    than crashing (trigger B, the close-time safety net, doesn't depend on
    this and can still fire independently).

    `forecast` needs at minimum: question_id, submitted_at (ISO string),
    optionally close_time (ISO string) for trigger B."""
    overrides = overrides if overrides is not None else load_refresh_overrides()
    q_id = str(forecast.get("question_id"))
    override = overrides.get(q_id, {}).get("refresh_after")
    if override:
        try:
            return datetime.fromisoformat(override.replace("Z", "+00:00"))
        except Exception:
            pass  # bad/unparseable override value — fall through to the default

    submitted_at_str = forecast.get("submitted_at")
    if not submitted_at_str:
        return None
    try:
        submitted_at = datetime.fromisoformat(submitted_at_str)
        if submitted_at.tzinfo is None:
            submitted_at = submitted_at.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    return submitted_at + timedelta(days=REFRESH_AFTER_DEFAULT_DAYS)


def is_due_for_refresh(forecast: dict, now: datetime | None = None,
                        overrides: dict | None = None) -> tuple[bool, str]:
    """Whether one forecast record is due for a refresh right now, and why.
    Either trigger is sufficient — see module docstring for the full design
    rationale.

    Returns (is_due, reason) — reason is a short human-readable string for
    display/logging, "" if not due."""
    now = now or datetime.now(timezone.utc)

    refresh_after = compute_refresh_after(forecast, overrides=overrides)
    if refresh_after is not None and now >= refresh_after:
        return True, f"routine refresh due (refresh_after {refresh_after.date().isoformat()} has passed)"

    close_time_str = forecast.get("close_time")
    if close_time_str:
        try:
            close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        except Exception:
            close_time = None
        if close_time is not None:
            hours_to_close = (close_time - now).total_seconds() / 3600
            if 0 <= hours_to_close <= FINAL_REFRESH_WINDOW_HOURS:
                submitted_at = None
                submitted_at_str = forecast.get("submitted_at")
                if submitted_at_str:
                    try:
                        submitted_at = datetime.fromisoformat(submitted_at_str)
                        if submitted_at.tzinfo is None:
                            submitted_at = submitted_at.replace(tzinfo=timezone.utc)
                    except Exception:
                        submitted_at = None
                already_did_final_refresh = (
                    submitted_at is not None
                    and submitted_at >= (close_time - timedelta(hours=FINAL_REFRESH_WINDOW_HOURS))
                )
                if not already_did_final_refresh:
                    return True, (f"closing within {FINAL_REFRESH_WINDOW_HOURS}h "
                                   f"({close_time.date().isoformat()}) — final refresh")

    return False, ""