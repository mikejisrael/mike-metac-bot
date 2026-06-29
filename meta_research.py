"""
meta_research.py — real-time web research for forecasting questions.

REWRITTEN 2026-06-29: previously routed through Perplexity's Sonar models
via OpenRouter, which required a second vendor account/credit pool on top
of the Anthropic billing already in place for forecasting itself. That
account ran out of credits (HTTP 402 on every single call for the entire
2026-06-29 session — every forecast that day ran with zero real research
grounding, silently degrading to base-rate/CP-only reasoning).

Now uses Claude's own native web_search tool directly — one API call,
billed straight through the same ANTHROPIC_API_KEY already paying for
forecasting itself. No second vendor, no separate credit balance to run
dry without anyone noticing.

Cost note: web search is billed at $10 per 1,000 searches, separate from
and in addition to normal token costs for the request. At MIN_FORECASTERS-
filtered tournament volume this is a small add-on, but worth knowing if
volume increases significantly.

Requires ANTHROPIC_API_KEY in .env (same one used everywhere else in this
codebase — no new secret to add anywhere, including GitHub Actions, since
it's already wired in). If unset, research_question() returns None and
callers fall back to the existing CP-anchoring safety net, same graceful-
degradation contract as before.

NOT YET LIVE-TESTED in this environment (api.anthropic.com web_search
specifically — the messages.create() calls themselves work fine, but the
search tool's actual behavior hasn't been observed end-to-end here). Watch
the first real run closely, same caution as the original Perplexity
integration got.
"""

import os
import anthropic
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-haiku-4-5"


def research_question(question_text: str, background_info: str = "", timeout: int = 25) -> str | None:
    """
    Returns a short, current, search-grounded research summary for the
    given question, or None if research isn't configured, unavailable, or
    failed. Never raises — a failed search should degrade to "no
    grounding", not crash the run, same contract as the previous
    OpenRouter/Perplexity version.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    context_line = f"Context: {background_info[:500]}\n\n" if background_info else ""
    prompt = (
        f"Research question for a forecasting platform: {question_text}\n\n"
        f"{context_line}"
        "Search the web and give a concise, factual summary (under 200 words) "
        "of the most current, relevant information available right now that "
        "would help forecast this question's outcome. Include specific dates, "
        "numbers, and named sources where possible. If you genuinely find "
        "nothing relevant or recent, say so explicitly rather than padding "
        "with generic background."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        response = client.messages.create(
            model=MODEL,
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 3,
            }],
        )

        # response.content can include text blocks, server_tool_use blocks
        # (the search calls themselves), and web_search_tool_result blocks
        # (raw results) interleaved with the final answer. Only the text
        # blocks are the actual synthesized summary we want — join them in
        # order, same as how a normal multi-block response is read.
        text_parts = [
            block.text for block in response.content
            if getattr(block, "type", None) == "text"
        ]
        text = "\n".join(text_parts).strip()
        return text if text else None

    except Exception as e:
        print(f"  ⚠️  Research call failed (non-fatal, forecasting will proceed without it): {e}")
        return None