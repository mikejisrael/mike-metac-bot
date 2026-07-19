"""
meta_refresh_exclusions.py — shared "permanently excluded from refresh"
list, so meta_refresh_forecast.py (skips excluded questions during
eligibility) and meta_dashboard.py (labels them for visibility) can't
silently drift apart on what's excluded or why.

Added 2026-07-06 after Q39825 ("brain-computer interfaces" question) was
found permanently stuck at the top of the STALE preview: it has a
perfectly valid post_id (ruling out the separate no_post_id issue this
same session already found and flagged automatically), but the actual
Metaculus API confirms it's closed to new forecasting even though its
local resolve_time is still months away — something only discoverable
via a live fetch, which the preview (local-data-only, by design, so it
stays cheap to run) never does. Properly fixing this would mean making a
live status check for every stale candidate on every dry run, defeating
the point of a cheap local preview. Mike's call (2026-07-06): for these
rare edge cases, a manual exclusion list is the right trade-off — not
worth automating detection for something this infrequent.

File: watch_state/refresh_excluded.json
Schema: {"<question_id>": {"reason": str, "excluded_at": iso8601 str}}

CLI usage:
  python meta_refresh_exclusions.py                          # list current exclusions
  python meta_refresh_exclusions.py add 39825 "closed despite future resolve_time"
  python meta_refresh_exclusions.py remove 39825
"""

import os
import json
from datetime import datetime, timezone

EXCLUSIONS_FILE = os.path.join("watch_state", "refresh_excluded.json")


def load_excluded_ids() -> dict[int, dict]:
    """Returns {question_id: {"reason": ..., "excluded_at": ...}}. Never
    raises if the file doesn't exist yet — same None-safe-default pattern
    used elsewhere in this codebase (load_phase0_reports,
    load_refresh_candidate_state)."""
    try:
        with open(EXCLUSIONS_FILE) as f:
            raw = json.load(f)
        return {int(k): v for k, v in raw.items()}
    except Exception:
        return {}


def _save(excluded: dict[int, dict]) -> None:
    os.makedirs("watch_state", exist_ok=True)
    with open(EXCLUSIONS_FILE, "w", newline='\n') as f:
        json.dump({str(k): v for k, v in excluded.items()}, f, indent=2)


def add_exclusion(question_id: int, reason: str) -> None:
    excluded = load_excluded_ids()
    excluded[question_id] = {
        "reason": reason,
        "excluded_at": datetime.now(timezone.utc).isoformat(),
    }
    _save(excluded)
    print(f"✅ Q{question_id} added to permanent refresh exclusions: {reason}")


def remove_exclusion(question_id: int) -> None:
    excluded = load_excluded_ids()
    if question_id not in excluded:
        print(f"Q{question_id} is not currently excluded — nothing to remove.")
        return
    del excluded[question_id]
    _save(excluded)
    print(f"✅ Q{question_id} removed from permanent refresh exclusions.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3 and sys.argv[1] == "add":
        add_exclusion(int(sys.argv[2]), " ".join(sys.argv[3:]) or "manually excluded")
    elif len(sys.argv) >= 3 and sys.argv[1] == "remove":
        remove_exclusion(int(sys.argv[2]))
    else:
        excluded = load_excluded_ids()
        if not excluded:
            print("No questions currently excluded from refresh.")
        else:
            print(f"{len(excluded)} question(s) permanently excluded from refresh:")
            for qid, info in sorted(excluded.items()):
                print(f"  Q{qid}: {info.get('reason')} (excluded {info.get('excluded_at', '')[:10]})")
