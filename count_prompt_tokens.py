"""
count_prompt_tokens.py — get the REAL token count of the batch-variant
system prompt, straight from the API's usage.input_tokens field, instead
of guessing with chars/4 or words*1.3 heuristics (which disagreed with
each other by ~900 tokens on this exact prompt — not precise enough to
trust for a hard 4,096-token threshold).

Cost: one tiny call, a few thousand input tokens at Haiku 4.5's $1/MTok
rate — a fraction of a cent.

Run from the same folder as cached_llm.py:
    python count_prompt_tokens.py
"""

import os
from dotenv import load_dotenv
load_dotenv()

import anthropic
from cached_llm import build_forecaster_system_prompt, build_batch_forecaster_system_prompt

client = anthropic.Anthropic()

for label, builder in [
    ("Base (tournament_forecast.py's, unchanged)", build_forecaster_system_prompt),
    ("Batch variant (with new padding)", build_batch_forecaster_system_prompt),
]:
    prompt = builder()
    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=5,
        system=[{"type": "text", "text": prompt, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": "ok"}],
    )
    input_tokens = response.usage.input_tokens
    cache_write = getattr(response.usage, "cache_creation_input_tokens", 0) or 0
    cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
    # BUG FIXED 2026-07-04: once caching activates, the prompt's tokens move
    # OUT of input_tokens and INTO cache_creation_input_tokens (on a write)
    # or cache_read_input_tokens (on a read) — comparing bare input_tokens
    # against 4096 gives a false "still short" the moment it actually works,
    # since a successful write correctly drops input_tokens near zero.
    total = input_tokens + cache_write + cache_read

    print(f"{label}:")
    print(f"  input_tokens: {input_tokens}")
    print(f"  cache_creation_input_tokens: {cache_write}")
    print(f"  cache_read_input_tokens: {cache_read}")
    print(f"  TOTAL (this is the real prompt size): {total}")
    if "Batch" in label:
        if cache_write > 0:
            print(f"  ✅ Cache WRITE occurred — prompt cleared the 4,096 floor.")
        elif cache_read > 0:
            print(f"  ✅ Cache READ occurred — reusing a cache written by a prior run.")
        else:
            gap = 4096 - total
            print(f"  ⚠️  Neither write nor read occurred — still {gap} tokens short." if gap > 0
                  else "  ⚠️  Cleared 4096 by size but neither write nor read fired — worth re-running.")
    print()