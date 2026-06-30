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

VERIFICATION PASS added 2026-06-30: manual spot-checking across 7 sample
questions found research_question() reliably fires and returns real,
search-grounded content, but ~2/7 samples contained internal numeric/date
contradictions — e.g. the same month's index value attributed to two
different months, or a YTD count that decreases then jumps inconsistently
across sequential "as of" dates. This is Haiku blending multiple web
sources/timeframes without reconciling them, not a missing-search problem.
A second, search-free Haiku call (_verify_research) now checks the first
call's output for exactly this failure mode and resolves it automatically
(preferring the most-recently-dated figure, discarding the rest, and
saying so in one line) before the text ever reaches the forecaster. If
verification itself fails or returns nothing usable, we fall back to the
original unverified text rather than losing the research entirely — same
graceful-degradation philosophy as the rest of this module.
"""

import os
import anthropic
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-haiku-4-5"


def _verify_research(raw_text: str, question_text: str, timeout: int = 20) -> str:
    """
    Second-pass, search-free check: ask Haiku to find and resolve internal
    numeric/date contradictions in the research text it already produced
    (e.g. the same index/count attributed to two different months, or a
    YTD figure that decreases then jumps across sequential 'as of' dates).
    Never raises and never returns empty — on any failure or empty result,
    callers should keep using the original raw_text, same graceful-
    degradation contract as research_question() itself.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or not raw_text:
        return raw_text

    prompt = (
        f"Forecasting question: {question_text}\n\n"
        f"Research summary to check:\n{raw_text}\n\n"
        "Check the above summary ONLY for internal contradictions — the same "
        "metric, index, or count attributed to two different values, dates, "
        "or time periods (e.g. one figure said to be from 'May' and the same "
        "number later said to be from 'April'; a year-to-date total that "
        "decreases and then jumps inconsistently across sequential dates).\n\n"
        "If you find no contradictions, return the summary UNCHANGED, "
        "word-for-word.\n\n"
        "If you find contradictions, rewrite the summary: keep all "
        "non-contradictory content as-is, resolve each contradiction by "
        "preferring the figure with the most recent or most specific date, "
        "and add one short line at the end stating exactly what you "
        "discarded and why (e.g. 'Note: discarded an earlier 40.8 figure "
        "misattributed to May; June's confirmed value is 43.6.'). If the "
        "contradiction can't be resolved from the text given, say so "
        "explicitly in that same line rather than guessing."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        response = client.messages.create(
            model=MODEL,
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        text_parts = [
            block.text for block in response.content
            if getattr(block, "type", None) == "text"
        ]
        verified = "\n".join(text_parts).strip()
        return verified if verified else raw_text

    except Exception as e:
        print(f"  ⚠️  Research verification failed (non-fatal, using unverified research): {e}")
        return raw_text


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
        if not text:
            return None
        return _verify_research(text, question_text)

    except Exception as e:
        print(f"  ⚠️  Research call failed (non-fatal, forecasting will proceed without it): {e}")
        return None