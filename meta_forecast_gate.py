"""
meta_forecast_gate.py — shared "is this question worth forecasting" gate
logic and the tournament ID list that needs special handling, extracted
2026-07-03 so meta_batch_forecast.py (decides what to ACTUALLY forecast)
and meta_coverage_check.py (decides whether a "missing" question is a
genuine gap or an expected, deliberate skip) can't silently drift apart on
what the gate conditions are, or on which tournament IDs need the
alternate `project=` fetch path.

Before this, meta_coverage_check.py's "130 gaps" number conflated two very
different things: questions that fail the SAME quality/relevance
thresholds meta_batch_forecast.py already filters on (low forecaster
engagement, wrong question type, too far out to forecast yet) — expected,
not a problem — with questions that genuinely should have been forecasted
and weren't. Only the second category is worth alerting on.

MIN_FORECASTERS / DAYS_AHEAD / QUESTION_SERIES_IDS live here now, not
duplicated in meta_batch_forecast.py — that file imports them from here.
Field names below (question_type, num_forecasters, close_time) are
confirmed real, declared fields on forecasting_tools' question classes —
checked directly against the installed library, not assumed.

NOTE (2026-07-03): meta_coverage_check.py's own open-question fetch for
the 5 QUESTION_SERIES_IDS tournaments still uses the generic
ApiFilter(allowed_tournaments=...) mechanism, NOT the project= fix
meta_batch_forecast.py already applies for its own forecasting fetch.
That mechanism is independently confirmed broken for question_series-type
projects (see meta_batch_forecast.py's ALLOWED_TOURNAMENTS comment —
check_project_type.py found it returns count≈7427, essentially
unfiltered, instead of the real ~37-question Nuclear Risk Horizons set).
Deliberately NOT fixed here (Mike's call, 2026-07-03, given his standing
suspicion that the wider question-fetch pipeline is still missing pools
of questions somewhere) — coverage numbers for those 5 tournaments are
flagged as unverified-scope in meta_coverage_check.py's output rather
than trusted or silently patched over.
"""

from datetime import datetime, timezone, timedelta

MIN_FORECASTERS = 5
DAYS_AHEAD = 365

# Tournament/project IDs that are type='question_series' on Metaculus's
# side, not type='tournament' — confirmed via check_project_type.py.
# meta_batch_forecast.py fetches these via the `project=` parameter
# directly (see fetch_question_series_questions there); ApiFilter's
# allowed_tournaments/`tournaments=` param silently fails to scope them.
#   1173   = Nuclear Risk Horizons Project
#   32774  = Current Events
#   3048   = The Taiwan Tinderbox
#   2018   = Economic Indicators
#   2995   = Animal Welfare Series
QUESTION_SERIES_IDS = [1173, 32774, 3048, 2018, 2995]


def passes_forecast_gate(
    question_type: str | None,
    num_forecasters: int | None,
    close_time: datetime | None,
    now: datetime | None = None,
) -> bool:
    """Mirrors the exact filter meta_batch_forecast.py applies: binary
    type only, at least MIN_FORECASTERS forecasters, and closing within
    DAYS_AHEAD days from now. Same three conditions whether enforced via
    ApiFilter server-side (allowed_types/num_forecasters_gte/close_time
    bounds) or the hand-rolled question_series path — one function
    instead of two copies that could quietly disagree.

    Does NOT check status=="open" — callers are expected to have already
    scoped to open questions before calling this; "open" is a
    precondition for being in the candidate set at all, not itself a
    forecasting-worthiness signal this function decides on."""
    return forecast_gate_failure_reason(question_type, num_forecasters, close_time, now=now) is None


def forecast_gate_failure_reason(
    question_type: str | None,
    num_forecasters: int | None,
    close_time: datetime | None,
    now: datetime | None = None,
) -> str | None:
    """Added 2026-07-03 as a diagnostic companion to passes_forecast_gate
    — same three conditions, same order, but reports WHICH one failed
    (or None if it passed) instead of a plain bool. Built specifically so
    meta_coverage_check.py can break its "gated" bucket down by reason,
    after a live run surfaced a suspicious close_time value (2049, a
    likely placeholder/sentinel rather than a real close date — same
    failure mode already documented for scheduled_resolution_time
    elsewhere in this codebase) that could be silently inflating the
    "too_far_out" bucket with questions that are actually fine on type
    and forecaster count. passes_forecast_gate() itself is defined in
    terms of this function now, so there's still only one place the
    actual gate logic lives — this doesn't introduce a second copy."""
    if question_type != "binary":
        return "wrong_type"
    if (num_forecasters or 0) < MIN_FORECASTERS:
        return "too_few_forecasters"
    if close_time is not None:
        if now is None:
            now = datetime.now(timezone.utc)
        if close_time.tzinfo is None:
            close_time = close_time.replace(tzinfo=timezone.utc)
        if not (now < close_time < now + timedelta(days=DAYS_AHEAD)):
            return "too_far_out"
    return None