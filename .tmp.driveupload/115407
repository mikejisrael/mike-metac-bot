"""
test_research_dry_run.py — one-off local sanity check for meta_research.py's
OpenRouter-primary / Anthropic-fallback path, BEFORE pushing to GitHub Actions.

Run this from the same folder as meta_research.py (project root), with
venv312 active, so it picks up the same .env and imports cleanly.

What it checks:
  1. OpenRouter path fires and returns real, current, search-grounded text
     (provider_order=["openrouter", "anthropic"] — same as batch/refresh).
  2. Confirms which provider actually served it (should say "openrouter").
  3. Watches stdout for the "served '<model>', not requested" warning —
     if that fires, OpenRouter rerouted away from perplexity/sonar on
     Ben's key and OPENROUTER_MODEL may need adjusting.
  4. Anthropic-only path (provider_order=["anthropic"]) still works
     unchanged — this is what tournament_forecast.py uses, confirming
     we haven't broken it.
  5. OpenRouter balance check (same endpoint the dashboard now polls).

Nothing here submits to Metaculus or costs more than a couple of small
API calls (2 research calls + 1 verification call + 1 balance check).
"""

import os
from dotenv import load_dotenv
load_dotenv()

from meta_research import research_question

TEST_QUESTION = "Will the US Federal Reserve cut interest rates at its next FOMC meeting?"
TEST_BACKGROUND = "Resolves YES if the Fed announces any rate cut at its next scheduled meeting."


def check_env():
    print("=== Environment check ===")
    has_or = bool(os.getenv("OPENROUTER_API_KEY"))
    has_an = bool(os.getenv("ANTHROPIC_API_KEY"))
    print(f"  OPENROUTER_API_KEY set: {has_or}")
    print(f"  ANTHROPIC_API_KEY set:  {has_an}")
    if not has_or:
        print("  ⚠️  OPENROUTER_API_KEY not found — the primary path will silently "
              "skip straight to Anthropic. Check your .env is in this folder and "
              "the key isn't still commented out.")
    print()


def test_openrouter_primary():
    print("=== Test 1: OpenRouter primary, Anthropic fallback (batch/refresh path) ===")
    text, source = research_question(
        TEST_QUESTION, TEST_BACKGROUND,
        provider_order=["openrouter", "anthropic"],
        return_source=True,
    )
    print(f"  Source used: {source}")
    if text:
        print(f"  Research text ({len(text)} chars):\n  {text[:400]}{'...' if len(text) > 400 else ''}")
    else:
        print("  ⚠️  No research returned — both providers failed or are unconfigured. "
              "Check the warning lines printed above this for the specific error.")
    print()
    return source


def test_anthropic_only():
    print("=== Test 2: Anthropic-only, default provider_order (tournament_forecast.py path) ===")
    text, source = research_question(
        TEST_QUESTION, TEST_BACKGROUND,
        return_source=True,  # note: no provider_order passed — this is the default
    )
    print(f"  Source used: {source} (should be 'anthropic' or None — never 'openrouter')")
    if text:
        print(f"  Research text ({len(text)} chars):\n  {text[:400]}{'...' if len(text) > 400 else ''}")
    print()
    if source == "openrouter":
        print("  ❌ UNEXPECTED: default call used OpenRouter. This would mean "
              "tournament_forecast.py's behavior changed too — stop and investigate "
              "before deploying anything.")
    print()


def test_openrouter_balance():
    print("=== Test 3: OpenRouter balance check (same call the dashboard makes) ===")
    import requests
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("  Skipped — OPENROUTER_API_KEY not set.")
        return
    try:
        r = requests.get(
            "https://openrouter.ai/api/v1/key",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("data", {})
        print(f"  limit: {d.get('limit')}")
        print(f"  limit_remaining: {d.get('limit_remaining')}")
    except Exception as e:
        print(f"  ⚠️  Balance check failed: {e}")
    print()


if __name__ == "__main__":
    check_env()
    source1 = test_openrouter_primary()
    test_anthropic_only()
    test_openrouter_balance()

    print("=== Summary ===")
    if source1 == "openrouter":
        print("✅ OpenRouter primary path is working — safe to deploy meta_batch_forecast.py "
              "and meta_refresh_forecast.py changes.")
    elif source1 == "anthropic":
        print("⚠️  OpenRouter did not serve the first test call (fell back to Anthropic). "
              "Check the warning lines above for why — likely OPENROUTER_API_KEY, "
              "OPENROUTER_MODEL, or the key's credit balance.")
    else:
        print("❌ Neither provider returned research — check both API keys before deploying.")