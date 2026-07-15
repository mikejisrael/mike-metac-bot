"""Refresh-scheduling logic for the batch-path tournaments (ACX2026,
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

Design (Mike's decision, 2026-07-15) — a checkpoint ladder based on how
far out a question's close date is, replacing the earlier flat-30-day-
after-last-forecast default (which was refreshing multi-year questions,
e.g. one closing in 2034, after just a month — much too eager):

    days to close >= 365          -> no refresh scheduled yet
    180 <= days to close < 365    -> checkpoint at close_time - 180 days
    90  <= days to close < 180    -> checkpoint at close_time - 90 days
    30  <= days to close < 90     -> checkpoint at close_time - 30 days
    days to close < 30            -> checkpoint at close_time - FINAL_REFRESH_WINDOW_HOURS
                                      (this rung subsumes what used to be a
                                      separate "trigger B" safety net — it's
                                      now just the last step of the same
                                      ladder, not a bolt-on second trigger)

IMPLEMENTATION NOTE — why this is a ladder of fixed absolute checkpoints,
not "recompute a single target from whichever tier `now` currently sits
in": the latter has a real bug. Picture a question at exactly 180 days to
close: today the target is close_time-180d = today, so it's flagged due.
If nobody acts on it, TOMORROW `now` has drifted to 179 days-to-close,
which is a DIFFERENT tier (90-179) whose target is close_time-90d — 89
days in the future. Recomputing from the current tier would make the "due"
flag silently vanish for the next ~89 days, even though nothing was ever
refreshed. That defeats the point of a reminder (agreed with Mike,
2026-07-15).

The fix: close_time alone implies a FIXED set of checkpoint dates
(close_time - 180d, - 90d, - 30d, - FINAL_REFRESH_WINDOW_HOURS) that never
move. A question is "due" if ANY checkpoint falls after its last forecast
and at/before now — so once a checkpoint is missed, the question stays
flagged continuously (not just for the single day it was first crossed)
until an actual refresh updates submitted_at past it. This is "sticky" by
construction, not because of a separate patch on top.
"""
import json
import os
from datetime import datetime, timezone, timedelta

# (days_to_close_upper_bound_exclusive, lead_days_before_close)
# Read as: for a question whose days-to-close currently falls under this
# upper bound (and at/above the next tier's bound), its relevant checkpoint
# is `lead_days_before_close` days before close_time. Ordered descending —
# see _ladder_checkpoints, which turns this into fixed absolute datetimes.
REFRESH_LADDER = [
    (365, 180),
    (180, 90),
    (90, 30),
]
# Below 30 days to close: falls through to this hour-based final checkpoint
# instead of a day-based one — same value as the old "trigger B", now just
# the last rung of one unified ladder rather than a separate mechanism.
FINAL_REFRESH_WINDOW_HOURS = 48
# At or beyond this many days to close, no refresh is scheduled at all
# (Mike's explicit call, 2026-07-15) — nothing meaningful to show yet.
NO_SCHEDULE_THRESHOLD_DAYS = 365

REFRESH_OVERRIDES_PATH = os.path.join("watch_state", "refresh_overrides.json")


def load_refresh_overrides() -> dict:
    """watch_state/refresh_overrides.json — optional per-question manual
    overrides for refresh_after, keyed by question_id (string):
        {"44667": {"refresh_after": "2026-08-20T00:00:00Z", "set_at": "...", "note": "..."}}
    Written by the dashboard when Mike edits a question's refresh date via
    the UI; read here to compute each question's EFFECTIVE refresh_after.
    An override always takes precedence over the ladder below, including
    overriding the "no schedule yet" >=365-day default — that's the whole
    point of letting Mike push a date in manually. Deliberately a thin
    standalone file rather than a field on the batch_jobs/batch_results
    history — those are an append-only forecast record, and a mutable
    "when should this run again" scheduling field doesn't belong mixed
    into that. Same None-safe-default pattern as load_excluded_ids/
    load_refresh_candidate_state elsewhere — the scheduling layer should
    never break other things if this file doesn't exist yet."""
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


def _parse_dt(s: str | None) -> datetime | None:
    """Small shared helper — parse an ISO datetime string (tolerating a
    trailing 'Z' and a missing tzinfo, both of which show up across this
    codebase's various JSON files), returning None on anything unparseable
    rather than raising. Used throughout this module so every parse site
    fails the same safe way."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _ladder_checkpoints(close_time: datetime) -> list[tuple[datetime, str]]:
    """All fixed checkpoint (datetime, label) pairs implied by a question's
    close_time — see module docstring for why these are fixed absolute
    points in time rather than something recomputed from the current tier.
    Ordered ascending (furthest from close first, i.e. earliest in time)."""
    checkpoints = [
        (close_time - timedelta(days=lead), f"{lead}-day")
        for _, lead in REFRESH_LADDER
    ]
    checkpoints.append(
        (close_time - timedelta(hours=FINAL_REFRESH_WINDOW_HOURS), f"final ({FINAL_REFRESH_WINDOW_HOURS}h)")
    )
    return checkpoints


def _next_checkpoint(forecast: dict, now: datetime) -> tuple[datetime | None, str]:
    """The earliest ladder checkpoint that hasn't yet been satisfied by
    this forecast's last submission — i.e. the checkpoint that determines
    both the displayed "next" refresh date AND whether a refresh is due
    right now (due iff that checkpoint is at/before `now`). Returns
    (None, "") if close_time is missing/unparseable, or if days-to-close is
    still >= NO_SCHEDULE_THRESHOLD_DAYS (Mike's explicit "don't set a date
    yet" call), or if every checkpoint has already been satisfied."""
    close_time = _parse_dt(forecast.get("close_time"))
    if close_time is None:
        return None, ""

    days_to_close = (close_time - now).total_seconds() / 86400
    if days_to_close >= NO_SCHEDULE_THRESHOLD_DAYS:
        return None, ""

    submitted_at = _parse_dt(forecast.get("submitted_at"))
    unsatisfied = [
        (c, label) for c, label in _ladder_checkpoints(close_time)
        if submitted_at is None or c > submitted_at
    ]
    if not unsatisfied:
        return None, ""
    return min(unsatisfied, key=lambda pair: pair[0])


def compute_refresh_after(forecast: dict, overrides: dict | None = None,
                           now: datetime | None = None) -> datetime | None:
    """Effective refresh_after for one forecast record — a manual override
    from refresh_overrides.json if one is set for this question_id, else
    the earliest not-yet-satisfied checkpoint from the ladder (see module
    docstring). Returns None if there's no override and either close_time
    is missing, days-to-close is still >= NO_SCHEDULE_THRESHOLD_DAYS, or
    every checkpoint has already been satisfied by the last forecast.

    A manual override is clamped to close_time if it would otherwise land
    AFTER it (added 2026-07-15, Mike's call) — the ladder's own checkpoints
    can never do this by construction (each is close_time minus some
    positive duration), but nothing previously stopped a manual override
    from being set past the point the question actually stops accepting
    forecasts, which would just be a silently-pointless date to show.

    `forecast` needs at minimum: question_id, close_time (ISO string);
    submitted_at (ISO string) is used too if present, to know which
    checkpoints are already satisfied."""
    now = now or datetime.now(timezone.utc)
    overrides = overrides if overrides is not None else load_refresh_overrides()
    q_id = str(forecast.get("question_id"))
    override = overrides.get(q_id, {}).get("refresh_after")
    if override:
        overridden = _parse_dt(override)
        if overridden is not None:
            close_time = _parse_dt(forecast.get("close_time"))
            if close_time is not None and overridden > close_time:
                return close_time
            return overridden
        # bad/unparseable override value — fall through to the ladder

    checkpoint, _label = _next_checkpoint(forecast, now)
    return checkpoint


def is_due_for_refresh(forecast: dict, now: datetime | None = None,
                        overrides: dict | None = None) -> tuple[bool, str]:
    """Whether one forecast record is due for a refresh right now, and why.
    "Due" means the effective refresh_after (see compute_refresh_after) is
    at or before `now`. Because refresh_after is always the EARLIEST
    unsatisfied checkpoint (not a value that gets silently recomputed to a
    later tier's target once time passes it), this stays true continuously
    once crossed — sticky by construction — until an actual refresh moves
    submitted_at past it.

    CHANGED 2026-07-15: now calls compute_refresh_after directly instead of
    re-parsing the override separately — that duplication was exactly how
    the close_time clamp above almost got fixed in only one of the two
    places. Single source of truth for "what is refresh_after" now feeds
    both the displayed date and the due-determination, guaranteed
    consistent by construction.

    Returns (is_due, reason) — reason is a short human-readable string for
    display/logging, "" if not due."""
    now = now or datetime.now(timezone.utc)
    overrides = overrides if overrides is not None else load_refresh_overrides()

    refresh_after = compute_refresh_after(forecast, overrides=overrides, now=now)
    if refresh_after is None or now < refresh_after:
        return False, ""

    q_id = str(forecast.get("question_id"))
    if overrides.get(q_id, {}).get("refresh_after"):
        return True, f"manual refresh date {refresh_after.date().isoformat()} has passed"

    _checkpoint, label = _next_checkpoint(forecast, now)
    close_time = _parse_dt(forecast.get("close_time"))
    days_to_close_str = f", {(close_time - now).days}d to close" if close_time is not None else ""
    return True, f"{label} threshold passed {refresh_after.date().isoformat()}{days_to_close_str}"