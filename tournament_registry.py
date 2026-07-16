"""
tournament_registry.py — single source of truth for every Metaculus
tournament/question_series the bot forecasts on, plus display metadata.

Created 2026-07-16 to stop the tournament-label drift that had already
happened once in practice: meta_dashboard.py's old hand-written
TOURNAMENT_LABELS dict was missing all 5 question_series tournaments
that meta_coverage_check.py's separate TOURNAMENTS dict already had
(added there 2026-07-02) — silently bucketing every question_series
forecast into "Other" on the dashboard. Two files holding the same
information independently drifted apart; this file exists so there's
only one place left to update.

Deliberately dependency-free (stdlib only) so every consumer — including
lightweight, read-only reporting scripts like meta_coverage_check.py,
which already avoids importing tournament_forecast_v2.py's heavier
dependency chain just to get one constant — can import this freely.

Each entry:
  id            Metaculus numeric project ID.
  slug          Metaculus slug string, only set where a script needs it
                for ApiFilter(allowed_tournaments=[...]) — currently just
                the 3 non-time-sensitive type='tournament' entries fetched
                via meta_batch_forecast.py's ALLOWED_TOURNAMENTS. None
                elsewhere (question_series types don't use slugs at all;
                Market Pulse/FutureEval are fetched by numeric ID only).
  display_name  Human label shown in the dashboard / coverage check.
  category      Top-level display grouping: "Tournaments" | "Question Series".
                "Other" and "Personal" are NOT registry entries — they're
                fallback buckets consumers assign when a question doesn't
                match anything below, same as before this file existed.
  project_type  Metaculus's own type field: "tournament" | "question_series"
                — determines which fetch mechanism actually works (see
                fetch_method).
  fetch_method  "allowed_tournaments" (ApiFilter's tournaments= param,
                confirmed correct for type='tournament') or "project_param"
                (raw project= query param — REQUIRED for
                type='question_series', since tournaments= silently
                returns ~unfiltered results for those — confirmed via
                check_project_type.py; see meta_forecast_gate.py's
                original docstring for the full history).
  pipeline      Which script actually submits forecasts here:
                "sync"  -> tournament_forecast_v2.py (time-sensitive)
                "batch" -> meta_batch_forecast.py (Claude Batch API path)
  notes         Free-text, optional — anything a consumer might want to
                surface (e.g. Market Pulse's group_of_questions handling).

NOT YET INCLUDED: US Midterms 2026 (metaculus.com/tournament/midterms-2026/).
Numeric project ID not yet confirmed live via the API — add it here,
following the same shape as the other type='tournament' entries, once
confirmed rather than guessed.
"""

TOURNAMENTS = {
    "futureeval": {
        "id": 33022,
        "slug": None,
        "display_name": "FutureEval",
        "category": "Tournaments",
        "project_type": "tournament",
        "fetch_method": "allowed_tournaments",
        "pipeline": "sync",
        "notes": None,
    },
    "market_pulse_26q3": {
        "id": 33066,
        "slug": None,
        "display_name": "Market Pulse Challenge 26Q3",
        "category": "Tournaments",
        "project_type": "tournament",
        "fetch_method": "allowed_tournaments",
        "pipeline": "sync",
        "notes": (
            "group_of_questions container posts with numeric sub-questions "
            "(59-155h lifespans); uses a final-hour-before-close refresh "
            "trigger instead of the generic 192h staleness gate."
        ),
    },
    "acx2026": {
        "id": 32880,
        "slug": "ACX2026",
        "display_name": "ACX2026",
        "category": "Tournaments",
        "project_type": "tournament",
        "fetch_method": "allowed_tournaments",
        "pipeline": "batch",
        "notes": None,
    },
    "climate": {
        "id": 1756,
        "slug": "climate",
        "display_name": "Climate Tipping Points",
        "category": "Tournaments",
        "project_type": "tournament",
        "fetch_method": "allowed_tournaments",
        "pipeline": "batch",
        "notes": None,
    },
    "metaculus_cup": {
        "id": 33021,
        "slug": "metaculus-cup-summer-2026",
        "display_name": "Metaculus Cup",
        "category": "Tournaments",
        "project_type": "tournament",
        "fetch_method": "allowed_tournaments",
        "pipeline": "batch",
        "notes": (
            "Humans-only for prize money — mike_iz_-bot forecasts here for "
            "calibration data only, not prize-eligible."
        ),
    },
    "nuclear_risk_horizons": {
        "id": 1173,
        "slug": None,
        "display_name": "Nuclear Risk Horizons",
        "category": "Question Series",
        "project_type": "question_series",
        "fetch_method": "project_param",
        "pipeline": "batch",
        "notes": None,
    },
    "current_events": {
        "id": 32774,
        "slug": None,
        "display_name": "Current Events",
        "category": "Question Series",
        "project_type": "question_series",
        "fetch_method": "project_param",
        "pipeline": "batch",
        "notes": None,
    },
    "taiwan_tinderbox": {
        "id": 3048,
        "slug": None,
        "display_name": "Taiwan Tinderbox",
        "category": "Question Series",
        "project_type": "question_series",
        "fetch_method": "project_param",
        "pipeline": "batch",
        "notes": None,
    },
    "economic_indicators": {
        "id": 2018,
        "slug": None,
        "display_name": "Economic Indicators",
        "category": "Question Series",
        "project_type": "question_series",
        "fetch_method": "project_param",
        "pipeline": "batch",
        "notes": None,
    },
    "animal_welfare": {
        "id": 2995,
        "slug": None,
        "display_name": "Animal Welfare",
        "category": "Question Series",
        "project_type": "question_series",
        "fetch_method": "project_param",
        "pipeline": "batch",
        "notes": None,
    },
}

# Fallback categories that are NOT registry entries — a question that
# doesn't match any id in TOURNAMENTS falls into "Other"; a question with
# only personal-account (non-bot) prediction history is "Personal".
# Consumers assign these labels directly; listed here just so the full
# category list lives in one place too.
OTHER_CATEGORY = "Other"
PERSONAL_CATEGORY = "Personal"
CATEGORY_ORDER = ["Tournaments", "Question Series", OTHER_CATEGORY, PERSONAL_CATEGORY]


# ─── Derived lookup helpers ─────────────────────────────────────────────────
# Consumers should use these instead of re-deriving their own dicts/lists
# from TOURNAMENTS, so there's exactly one place that knows how to turn
# the registry into whatever shape a given script needs.

def labels_by_id() -> dict[int, str]:
    """id -> display_name. Drop-in replacement for the old hand-written
    TOURNAMENT_LABELS dicts in meta_dashboard.py / meta_coverage_check.py."""
    return {v["id"]: v["display_name"] for v in TOURNAMENTS.values()}


def category_by_id() -> dict[int, str]:
    """id -> category ("Tournaments" | "Question Series"). For consumers
    that want to tag rows by category without yet building a full nested
    UI (that's a separate, later step)."""
    return {v["id"]: v["category"] for v in TOURNAMENTS.values()}


def ids_for(project_type: str) -> list[int]:
    """All ids with the given project_type — e.g. ids_for("question_series")
    replaces the old hardcoded QUESTION_SERIES_IDS list."""
    return [v["id"] for v in TOURNAMENTS.values() if v["project_type"] == project_type]


def ids_for_pipeline(pipeline: str) -> list[int]:
    """All ids forecasted by a given pipeline script — e.g.
    ids_for_pipeline("sync") replaces tournament_forecast[_v2].py's
    hand-written TOURNAMENT_IDS default."""
    return [v["id"] for v in TOURNAMENTS.values() if v["pipeline"] == pipeline]


def slugs_for(pipeline: str, project_type: str | None = None) -> list[str]:
    """Slugs for a given pipeline (and optionally project_type), skipping
    entries with no slug set. Replaces meta_batch_forecast.py's hardcoded
    ALLOWED_TOURNAMENTS list."""
    out = []
    for v in TOURNAMENTS.values():
        if v["pipeline"] != pipeline:
            continue
        if project_type is not None and v["project_type"] != project_type:
            continue
        if v["slug"]:
            out.append(v["slug"])
    return out


def display_order() -> list[str]:
    """Display names in a stable, category-grouped order — Tournaments
    first (in registry insertion order), then Question Series. Matches
    the old hand-written TOURNAMENT_ORDER list. Does NOT include "Other"/
    "Personal"/"Unknown" — those aren't registry entries, so consumers
    append them themselves, same as before this file existed."""
    order = []
    for cat in ("Tournaments", "Question Series"):
        for v in TOURNAMENTS.values():
            if v["category"] == cat and v["display_name"] not in order:
                order.append(v["display_name"])
    return order