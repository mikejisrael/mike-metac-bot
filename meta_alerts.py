"""
meta_alerts.py — push a notification when the bot submits a new prediction.

Uses ntfy.sh: a free, no-signup-required push notification service. You
pick a private topic name (treat it like a password — anyone who knows it
can read your notifications), then either:
  - install the ntfy app (iOS/Android) and subscribe to that topic, or
  - just visit https://ntfy.sh/<your-topic> in a browser to see alerts live.

Setup:
  1. Pick a random-ish topic name, e.g. "mikejisrael-metac-bot-7f3a"
  2. Add to .env:  ALERT_NTFY_TOPIC=mikejisrael-metac-bot-7f3a
  3. (Optional) install the ntfy app and subscribe to that exact topic name.

If ALERT_NTFY_TOPIC isn't set, send_alert() is a silent no-op — nothing
breaks, you just don't get notifications until you configure it.

FIXED 2026-06-30: HTTP header VALUES must be ASCII-only — confirmed via
real-world bug reports against this exact ntfy Title-header pattern
(binwiederhier/ntfy#1410) and an identical root cause in an unrelated
tool's ntfy integration (Sonarr/Sonarr#6679: a non-ASCII character in a
notification title crashed the request entirely). This was caught live:
an alert with an emoji in its title ("📦 Batch ready...") crashed with
`'latin-1' codec can't encode character '\U0001f4e6'` — requests/urllib3
try to encode header values as Latin-1 by default, which can't represent
emoji or most non-ASCII characters at all. The restriction is HEADER-ONLY
— the message BODY (sent via `data=`, UTF-8-encoded) was never affected
and supports emoji/unicode fine; only the Title header needed sanitizing.

ALSO FIXED: send_alert() previously returned nothing and printed nothing
on success, while silently swallowing failures into a single non-fatal
print — meaning a caller had no reliable way to know whether a
notification actually went out. A real bug caused by exactly this: a
caller printed "alert sent" unconditionally right after calling
send_alert(), even on the run where the send had just failed (the emoji
crash above). send_alert() now returns True/False so callers can check
before claiming success.
"""

import os
import re
import requests


def _ascii_safe_title(title: str) -> str:
    """Make a title safe for an HTTP header: substitute common
    typographic characters with plain ASCII equivalents first (so
    em-dashes and smart quotes degrade gracefully instead of just
    vanishing), then drop anything still non-ASCII (emoji, etc.) rather
    than letting the request crash. Never touches the message body."""
    title = (title
             .replace("—", "-").replace("–", "-")
             .replace("’", "'").replace("‘", "'")
             .replace("“", '"').replace("”", '"'))
    title = title.encode("ascii", errors="ignore").decode("ascii")
    title = re.sub(r"\s+", " ", title).strip()
    return title or "Metaculus Bot"


def send_alert(message: str, title: str = "Metaculus Bot") -> bool:
    """Returns True only if the alert was actually sent successfully.
    Returns False if ALERT_NTFY_TOPIC isn't configured, or if the send
    failed for any reason (network error, ntfy.sh down, etc.) — callers
    that want to confirm a notification truly went out should check this
    return value rather than assuming success because nothing raised."""
    topic = os.getenv("ALERT_NTFY_TOPIC")
    if not topic:
        return False  # not configured — no-op, never blocks the main flow
    try:
        response = requests.post(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={"Title": _ascii_safe_title(title)},
            timeout=5,
        )
        response.raise_for_status()
        return True
    except Exception as e:
        # Alerts are a nice-to-have — never let a notification failure
        # break or slow down an actual forecast submission.
        print(f"  (alert failed, non-fatal: {e})")
        return False