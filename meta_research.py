"""
meta_research.py — real-time web research for forecasting questions, via
Perplexity's Sonar models through OpenRouter. One API call returns a
search-grounded, current answer — no separate search-then-synthesize step
needed (unlike raw Exa results, which would need a second LLM call).

Requires OPENROUTER_API_KEY in .env (and as a GitHub secret + workflow env
var for live/scheduled runs — confirmed wired into tournament_forecast.yaml
as of 2026-06-29). If unset or still the placeholder value, research_question()
returns None and callers fall back to the existing CP-anchoring safety net.
This function never raises and never blocks a forecast on a missing or
failed research call — a failed search should degrade to "no grounding",
not crash the run.

NOT YET LIVE-TESTED in this environment (openrouter.ai isn't reachable from
this sandbox) — first real run should be watched closely. If the model slug
below 404s or errors consistently, check https://openrouter.ai/models for
the current exact name (OpenRouter occasionally renames/versions these).
"""

import os
import requests

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
SONAR_MODEL = "perplexity/sonar"


def research_question(question_text: str, background_info: str = "", timeout: int = 25) -> str | None:
    """
    Returns a short, current, search-grounded research summary for the
    given question, or None if research isn't configured, unavailable, or
    failed. Never raises.
    """
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key or api_key == "REPLACE_ME":
        return None

    context_line = f"Context: {background_info[:500]}\n\n" if background_info else ""
    prompt = (
        f"Research question for a forecasting platform: {question_text}\n\n"
        f"{context_line}"
        "Give a concise, factual summary (under 200 words) of the most "
        "current, relevant information available right now that would help "
        "forecast this question's outcome. Include specific dates, numbers, "
        "and named sources where possible. If you genuinely find nothing "
        "relevant or recent, say so explicitly rather than padding with "
        "generic background."
    )

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": SONAR_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 400,
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            print(f"  ⚠️  Research call failed: HTTP {resp.status_code} — {resp.text[:150]!r}")
            return None
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()
        return text if text else None
    except Exception as e:
        print(f"  ⚠️  Research call failed (non-fatal, forecasting will proceed without it): {e}")
        return None
