"""
meta_refresh_gate.py — shared minimum-refresh-gap logic for anything that
decides whether a question is due for a fresh forecast, based on how
recently it was last forecast.

Used by (at least):
  - meta_refresh_forecast.py: gates the CLOSING_SOON eligibility bucket in
    find_questions_to_refresh(). Added 2026-07-03 after Mike noticed the
    same closing-soon questions (budget reconciliation bill, Monsanto,
    etc.) being flagged — and actually re-forecast — on every single dry
    run / --submit, no matter how recently they'd already been refreshed.
    The CLOSING_SOON bucket had no recency check at all before this;
    STALE_DAYS already provided the analogous protection for the STALE
    bucket, but closing_soon bypasses that check entirely (it `continue`s
    before ever reaching it).
  - meta_watch.py: gates push-notification "refresh candidate: closing
    soon" alerts, so it doesn't keep pinging about a question that was
    just refreshed. (Mike's recollection: this already used a 24h gap.)

MIN_REFRESH_GAP_HOURS = 192 (8 days), chosen 2026-07-03 specifically so
that within meta_refresh_forecast.py's 14-day CLOSING_SOON window, a
question gets refreshed at most twice — once on entering the window
around day 14, once more around day 6 — rather than on every run in
between. If meta_watch.py's own closing-soon threshold differs from 14
days, the "at most twice" framing won't translate exactly 1:1 there, but
the same gate function and constant are still the right shared building
block — same intent (don't re-flag something just handled), same number,
single source of truth instead of two constants that can drift apart.
"""

from datetime import datetime, timezone

MIN_REFRESH_GAP_HOURS = 192  # 8 days


def is_refresh_due(
    submitted_at: datetime | None,
    min_gap_hours: float = MIN_REFRESH_GAP_HOURS,
    now: datetime | None = None,
) -> bool:
    """Returns True if enough time has passed since submitted_at to allow
    another refresh, False if it's too soon.

    submitted_at=None (no prior forecast on file at all) always returns
    True — there's nothing to gate against; a question with zero prior
    forecasts is obviously due.

    submitted_at may be naive or timezone-aware; naive datetimes are
    treated as UTC (matching how meta_refresh_forecast.py already parses
    its own submitted_at strings elsewhere).

    now defaults to datetime.now(timezone.utc); callers processing many
    questions in a loop should compute `now` once and pass it explicitly
    rather than letting each call take its own timestamp, so results stay
    consistent across a single run."""
    if submitted_at is None:
        return True
    if now is None:
        now = datetime.now(timezone.utc)
    if submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=timezone.utc)
    elapsed_hours = (now - submitted_at).total_seconds() / 3600
    return elapsed_hours >= min_gap_hours