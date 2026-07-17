"""
test_openrouter_raw.py — bare-metal diagnostic for the 402 error, no
meta_research.py wrapper involved. Prints the FULL response body (not just
the generic "402 Client Error" message) from both endpoints, so we can see
OpenRouter's actual stated reason (insufficient balance vs. model not
enabled for this key vs. something else entirely).

Also prints which key is actually being read from .env (masked) so we can
confirm it's the key you think it is.

Run from the same folder as .env, with venv312 active:
    python test_openrouter_raw.py
"""

import os
import requests
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("OPENROUTER_API_KEY", "")

print("=== Key sanity check ===")
if not api_key:
    print("  ❌ OPENROUTER_API_KEY is empty or not found in .env")
else:
    # Masked print — never show the full key, but enough to confirm which
    # one is actually loaded (first 12 + last 4 chars).
    masked = api_key[:12] + "..." + api_key[-4:] if len(api_key) > 20 else "(short/unusual key)"
    print(f"  Key loaded: {masked}")
    print(f"  Length: {len(api_key)} chars")
    print(f"  Has leading/trailing whitespace: {api_key != api_key.strip()}")
print()

print("=== Raw balance check: GET /api/v1/key ===")
try:
    r = requests.get(
        "https://openrouter.ai/api/v1/key",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=10,
    )
    print(f"  Status code: {r.status_code}")
    print(f"  Raw body: {r.text}")
except Exception as e:
    print(f"  Request itself failed: {e}")
print()

print("=== Raw chat completion attempt: POST /api/v1/chat/completions ===")
try:
    r = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": "google/gemini-2.5-flash:online",
            "messages": [{"role": "user", "content": "Say 'test ok' and nothing else."}],
        },
        timeout=20,
    )
    print(f"  Status code: {r.status_code}")
    print(f"  Raw body: {r.text}")
except Exception as e:
    print(f"  Request itself failed: {e}")
print()

print("=== Summary ===")
print("Compare the masked key above against what you see on openrouter.ai/settings/keys")
print("to confirm this is the key you expect (personal vs. ben@metaculus.com one).")
print("The raw body from the failed request above should state the actual reason")
print("for the 402 (e.g. 'insufficient balance', 'model requires payment method', etc.)")