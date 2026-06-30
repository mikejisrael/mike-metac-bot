"""
meta_prompt_cache.py — tiny shared helper for prompt-caching the system
prompt across tournament_forecast.py, meta_batch_forecast.py, and
meta_refresh_forecast.py's batch path.

Why a separate module: cached_llm.py's build_forecaster_system_prompt()
returns a plain string, used directly by several call sites across this
codebase. Wrapping that string in Anthropic's cache_control block format
is small but easy to get subtly wrong if copy-pasted three times —
keeping it in one place means one bug to fix instead of three.

IMPORTANT — whether this actually saves anything is NOT yet confirmed:
Claude Haiku 4.5 requires a minimum 4,096-token prefix before caching
activates at all (confirmed via Anthropic's official prompt-caching docs,
checked June 2026) — notably higher than the more commonly-cited
1,024-token minimum that applies to some other models. This codebase has
never measured build_forecaster_system_prompt()'s actual token count. If
it's under ~4,096 tokens, this wrapper is harmless (cache_creation_
input_tokens and cache_read_input_tokens will both just read 0 in the API
response — same cost as before, no error) but provides zero savings.
Check the first live response's `usage` object to confirm one way or the
other — don't assume this is working just because nothing broke.

Deliberately NOT used by meta_refresh_forecast.py's --single path
(call_claude_single / run_single): that's a single one-off synchronous
call, and the cache write premium (1.25x base input price) is never
recovered without at least one subsequent cache READ within the 5-minute
TTL — which a one-off manual call, by definition, never gets. Caching
there would make that specific path slightly MORE expensive, not less.
"""


def cacheable_system_block(system_text: str) -> list:
    """Wrap a system prompt string in Anthropic's explicit cache_control
    block format. Pass the RETURN VALUE of this function as the `system=`
    argument to messages.create() (or in a batch request's params) —
    not the raw string."""
    return [
        {
            "type": "text",
            "text": system_text,
            "cache_control": {"type": "ephemeral"},
        }
    ]
