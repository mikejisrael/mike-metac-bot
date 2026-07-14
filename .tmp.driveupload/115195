"""
poll_tournament.py — Scheduled wrapper around tournament_forecast.run().

Run this on a tight Task Scheduler interval (5-10 min) instead of waiting
for an email notification and manually triggering tournament_forecast.py.
tournament_forecast.run() already fetches open tournament posts and skips
anything already forecast (via the title-aware dedup guard), so running it
on a loop costs nothing extra when there's nothing new — it just logs a
no-op line and exits.

Every run is logged to poll_tournament.log next to this script, including
no-op runs, so you can confirm overnight that polling actually happened on
schedule rather than silently failing.

Usage:
  python poll_tournament.py

Intended to be triggered by Windows Task Scheduler every 5-10 minutes —
see run_poll.bat.
"""

import asyncio
import contextlib
import io
import traceback
from datetime import datetime
from pathlib import Path

import tournament_forecast as tf

LOG_FILE = Path(__file__).parent / "poll_tournament.log"


def _log(line: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


async def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    buf = io.StringIO()

    try:
        with contextlib.redirect_stdout(buf):
            await tf.run()
        output = buf.getvalue().strip()

        if "No questions found" in output or not output:
            _log(f"[{timestamp}] no-op — nothing open / nothing new")
        else:
            summary_line = next(
                (l for l in output.splitlines() if l.startswith("Submitted:")), ""
            )
            _log(f"[{timestamp}] ACTIVITY — {summary_line or 'see details below'}")
            for line in output.splitlines():
                _log(f"    {line}")

    except Exception as e:
        _log(f"[{timestamp}] ERROR — {type(e).__name__}: {e}")
        for line in traceback.format_exc().splitlines():
            _log(f"    {line}")


if __name__ == "__main__":
    asyncio.run(main())
