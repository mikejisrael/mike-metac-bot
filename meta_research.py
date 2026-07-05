"""
meta_research.py — real-time web research for forecasting questions.

REWRITTEN 2026-06-29: previously routed through Perplexity's Sonar models
via OpenRouter, which required a second vendor account/credit pool on top
of the Anthropic billing already in place for forecasting itself. That
account ran out of credits (HTTP 402 on every single call for the entire
2026-06-29 session — every forecast that day ran with zero real research
grounding, silently degrading to base-rate/CP-only reasoning). Switched to
Claude's own native web_search tool, billed straight through
ANTHROPIC_API_KEY.

UPDATED 2026-07-04: a new OpenRouter key with fresh credit (initially
Mike's own, now also one from ben@metaculus.com) is back in the mix. This
module supports BOTH providers behind a configurable, ordered
provider_order list, so each caller decides its own risk posture:

  - meta_batch_forecast.py / meta_refresh_forecast.py opt in to
    provider_order=["openrouter", "anthropic"] — OpenRouter PRIMARY,
    Anthropic FALLBACK. This is deliberate, confirmed 2026-07-04: easy to
    flip back to Anthropic-primary later (just swap the list order) if
    calibration diverges between the two sources — that's exactly why
    research_source is now tracked per-question, see below.
  - tournament_forecast.py (the protected FutureEval pipeline, short
    close windows, no changes until proven elsewhere) is NOT touched by
    this change: it calls research_question() with no provider_order
    argument, which defaults to PROVIDER_ORDER_DEFAULT = ["anthropic"],
    i.e. byte-for-byte the same behavior it had before this rewrite.

Because research source now varies per-call, research_question() can
optionally return (text, source) instead of just text — pass
return_source=True. Existing callers that don't pass it keep getting a
bare string back, unchanged.

Model-on-key note (2026-07-04): OpenRouter API keys are NOT bound to a
specific model — the model is chosen per-request in the request body, not
by whichever key is used. So there's no way to ask "what model does this
key use" in the abstract, including for the ben@metaculus.com key. What
IS checkable: every OpenRouter response includes a "model" field showing
what actually served that specific request (OpenRouter can occasionally
reroute if a model isn't available on a given account). _research_via_
openrouter logs a warning if the served model differs from OPENROUTER_MODEL,
so a silent reroute doesn't go unnoticed.

Cost notes:
  - Anthropic web_search: $10 per 1,000 searches, on top of normal token
    costs for the request.
  - OpenRouter (google/gemini-2.5-flash:online): billed against whichever
    OpenRouter key is configured, plus OpenRouter's web-search surcharge
    for the ":online" suffix (~$4 per 1,000 results as of 2026-07-04,
    on top of normal token costs). Switched here from perplexity/sonar
    2026-07-04 after finding the ben@metaculus.com key's provider
    allow-list excludes Perplexity — see OPENROUTER_MODEL comment below
    for the full story.

Requires at least one of ANTHROPIC_API_KEY or OPENROUTER_API_KEY in
.env. If neither configured provider in a given provider_order succeeds,
research_question() returns None (or (None, None) with return_source=True)
and callers fall back to the existing CP-anchoring safety net — same
graceful-degradation contract as before.

VERIFICATION PASS added 2026-06-30: manual spot-checking across 7 sample
questions found research reliably fires and returns real, search-grounded
content, but ~2/7 samples contained internal numeric/date contradictions
— e.g. the same month's index value attributed to two different months,
or a YTD count that decreases then jumps inconsistently across sequential
"as of" dates. A second, search-free Haiku call (_verify_research) checks
whichever provider's output for exactly this failure mode and resolves it
automatically (preferring the most-recently-dated figure, discarding the
rest, and saying so in one line) before the text ever reaches the
forecaster. If verification itself fails or returns nothing usable, we
fall back to the original unverified text rather than losing the
research entirely — same graceful-degradation philosophy as the rest of
this module. This runs regardless of which provider produced the raw text.
"""

import os
import requests
import anthropic
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_MODEL = "claude-haiku-4-5"

# UPDATED 2026-07-04: switched from "perplexity/sonar" after discovering
# the ben@metaculus.com OpenRouter key has a provider allow-list limited
# to ["openai", "anthropic", "google-ai-studio"] — Perplexity isn't on
# it, so every perplexity/sonar call 404'd with "No allowed providers are
# available for the selected model," regardless of credit balance (which
# was fine — $100/$100 unused). Rather than chase down account settings
# on a key we don't administer, switched to a model from an already-
# allowed provider with OpenRouter's ":online" web-search suffix, which
# uses that provider's own native search — Google here, all three allowed
# providers support it. If this ever needs to change again, any of
# "openai/..." or "anthropic/..." + ":online" would also work on this key.
OPENROUTER_MODEL = "google/gemini-2.5-flash:online"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Callers that don't pass provider_order get exactly the pre-2026-07-04
# behavior: Anthropic web_search only. This is deliberate — it means
# tournament_forecast.py's existing call site needs zero edits and zero
# behavior change from this rewrite.
PROVIDER_ORDER_DEFAULT = ["anthropic"]


def _verify_research(raw_text: str, question_text: str, timeout: int = 20) -> str:
    """
    Second-pass, search-free check: ask Haiku to find and resolve internal
    numeric/date contradictions in the research text it already produced
    (e.g. the same index/count attributed to two different months, or a
    YTD figure that decreases then jumps across sequential 'as of' dates).
    Never raises and never returns empty — on any failure or empty result,
    callers should keep using the original raw_text, same graceful-
    degradation contract as research_question() itself. Provider-agnostic:
    runs the same way regardless of whether raw_text came from Anthropic
    or OpenRouter.
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
            model=ANTHROPIC_MODEL,
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


def _research_via_anthropic(question_text: str, background_info: str, timeout: int) -> str | None:
    """Native Claude web_search_20250305 tool. Returns raw (unverified) text or None."""
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
            model=ANTHROPIC_MODEL,
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
        # blocks are the actual synthesized summary we want.
        text_parts = [
            block.text for block in response.content
            if getattr(block, "type", None) == "text"
        ]
        text = "\n".join(text_parts).strip()
        return text if text else None

    except Exception as e:
        print(f"  ⚠️  Anthropic research call failed (non-fatal): {e}")
        return None


def _research_via_openrouter(question_text: str, background_info: str, timeout: int) -> str | None:
    """OpenRouter with a ":online" web-search-enabled model (currently
    google/gemini-2.5-flash:online — see OPENROUTER_MODEL comment for why).
    The ":online" suffix triggers OpenRouter's native web-search plugin for
    supported providers (OpenAI, Anthropic, Google, Perplexity, xAI), so no
    separate tool declaration is needed here, same as the Perplexity
    approach this replaced. Returns raw (unverified) text or None.

    Logs a warning (non-fatal) if the "model" field on the response differs
    from OPENROUTER_MODEL — OpenRouter keys aren't bound to a specific
    model, so this is the only way to notice a silent reroute rather than
    assuming the requested model is always what actually served the call.
    """
    api_key = os.getenv("OPENROUTER_API_KEY")
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
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        served_model = data.get("model")
        # OPENROUTER_MODEL may carry a ":online" (or other) routing suffix
        # that's a hint to OpenRouter, not necessarily part of the model
        # identity it echoes back — compare on the base model id so a
        # normal, expected response doesn't get flagged as a reroute.
        requested_base = OPENROUTER_MODEL.split(":")[0]
        served_base = served_model.split(":")[0] if served_model else None
        if served_base and served_base != requested_base:
            print(f"  ⚠️  OpenRouter served '{served_model}', not requested "
                  f"'{OPENROUTER_MODEL}' — key may be rerouting; check OpenRouter dashboard")

        text = (
            data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
        ).strip()
        return text if text else None

    except Exception as e:
        print(f"  ⚠️  OpenRouter research call failed (non-fatal): {e}")
        return None


_PROVIDER_FUNCS = {
    "anthropic": _research_via_anthropic,
    "openrouter": _research_via_openrouter,
}


def research_question(
    question_text: str,
    background_info: str = "",
    timeout: int = 25,
    provider_order: list[str] | None = None,
    return_source: bool = False,
):
    """
    Returns a short, current, search-grounded research summary for the
    given question, or None if research isn't configured, unavailable, or
    failed on every provider tried. Never raises — a failed search should
    degrade to "no grounding", not crash the run.

    provider_order: list of provider names to try in order, e.g.
    ["openrouter", "anthropic"]. Defaults to PROVIDER_ORDER_DEFAULT
    (["anthropic"] only) if not given — this preserves exact prior
    behavior for any caller that doesn't explicitly opt in to OpenRouter.

    return_source: if True, returns (text, source_name) instead of just
    text. source_name is the provider string that actually produced the
    result, or None if every provider in provider_order failed/was
    unconfigured.
    """
    order = provider_order or PROVIDER_ORDER_DEFAULT

    for provider in order:
        func = _PROVIDER_FUNCS.get(provider)
        if func is None:
            print(f"  ⚠️  Unknown research provider '{provider}' in provider_order, skipping")
            continue

        raw_text = func(question_text, background_info, timeout)
        if raw_text:
            verified = _verify_research(raw_text, question_text)
            return (verified, provider) if return_source else verified

    return (None, None) if return_source else None