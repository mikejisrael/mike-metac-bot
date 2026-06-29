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
"""

import os
import requests


def send_alert(message: str, title: str = "Metaculus Bot") -> None:
    topic = os.getenv("ALERT_NTFY_TOPIC")
    if not topic:
        return  # not configured — no-op, never blocks the main flow
    try:
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={"Title": title},
            timeout=5,
        )
    except Exception as e:
        # Alerts are a nice-to-have — never let a notification failure
        # break or slow down an actual forecast submission.
        print(f"  (alert failed, non-fatal: {e})")
