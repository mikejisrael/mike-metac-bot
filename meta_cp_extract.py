"""
meta_cp_extract.py — best-effort extraction of a LIVE community-aggregate
value from a raw Metaculus post/question JSON dict, for binary, numeric,
and multiple_choice questions.

IMPORTANT — UNVERIFIED FIELD PATHS: the binary path (aggregations.
recency_weighted.latest.centers) was confirmed working in
meta_batch_forecast.py's existing update_community_predictions(). The
numeric and multiple_choice paths below are best-effort guesses based on
the same general shape, NOT empirically confirmed against a live response
in this environment. On first real run, print extract_live_cp's return
value next to the raw aggregations dict for a few numeric/MC questions
and adjust the paths below if they're consistently returning None.
"""


def extract_live_cp(raw: dict | None, q_type: str):
    """
    raw: a post dict (has a "question" key) OR a question dict directly OR a
         BinaryQuestion-style .api_json. Tries both shapes.
    q_type: "binary" | "numeric" | "multiple_choice"

    Returns:
      binary          -> float | None
      numeric         -> float | None  (rough community median estimate)
      multiple_choice -> dict[str, float] | None
    Never raises — any miss just returns None.
    """
    if not raw:
        return None
    try:
        q = raw.get("question", raw)
        agg = q.get("aggregations", {}) or {}
        node = agg.get("recency_weighted") or agg.get("metaculus_prediction") or {}
        latest = node.get("latest") or {}
        if not latest:
            return None

        if q_type == "binary":
            centers = latest.get("centers", [])
            if len(centers) > 1:
                return centers[1]
            elif centers:
                return centers[0]
            return None

        if q_type == "numeric":
            centers = latest.get("centers", [])
            if not centers:
                return None
            return centers[len(centers) // 2]

        if q_type == "multiple_choice":
            options = q.get("options")
            pmf = latest.get("forecast_values") or latest.get("means")
            if pmf and options and len(pmf) == len(options):
                return dict(zip(options, pmf))
            return None

    except Exception:
        return None

    return None