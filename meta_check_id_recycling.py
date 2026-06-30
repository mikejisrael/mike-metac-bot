"""
check_id_recycling.py — one-off diagnostic, NOT part of the regular pipeline.

Scans every batch_jobs*.json / batch_results*.json file in "Meta batches/"
(and "tournament_batches/" if present) and checks whether the same
question_id, or the same post_id, ever appears against two genuinely
different question titles.

This exists to settle a real open question: the codebase currently assumes
Metaculus recycles IDs (see the "ID likely recycled" warnings in
meta_batch_forecast.py), but that assumption was never actually confirmed
against live evidence — it may simply be defensive code written without
proof either way. This script checks your own historical data instead of
guessing.

Usage:
  python check_id_recycling.py

Output: prints any question_id or post_id found attached to 2+ meaningfully
different titles, with the source files and titles involved, so you can
eyeball whether it's genuine recycling or just a title that got reworded
slightly (e.g. a typo fix) — titles_match() is used to do the same fuzzy
comparison the rest of the codebase relies on, so an "edited slightly" title
won't falsely show up here as recycling.
"""

import json
import glob
import os
from collections import defaultdict

from meta_question_matching import titles_match

BATCH_DIRS = ["Meta batches", "tournament_batches"]


def collect_id_title_pairs(id_field: str) -> dict[int, list[tuple[str, str]]]:
    """Return {id_value: [(title, source_file), ...]} for every occurrence
    of id_field found across all batch_jobs*.json files (which have the
    fullest, earliest-recorded titles — results files are derived from
    these and would just duplicate the same pairs)."""
    pairs: dict[int, list[tuple[str, str]]] = defaultdict(list)

    job_files = sorted(
        f for d in BATCH_DIRS
        for f in glob.glob(os.path.join(d, "batch_jobs*.json"))
    )

    for jf in job_files:
        try:
            with open(jf) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  Warning: could not load {jf}: {e}")
            continue

        id_map = data.get(f"{id_field}s", {})       # e.g. "question_ids" / "post_ids"
        text_map = data.get("question_texts", {})

        for custom_id, id_value in id_map.items():
            if id_value is None:
                continue
            title = text_map.get(custom_id, "")
            if not title:
                continue
            pairs[id_value].append((title, jf))

    return pairs


def report(id_field: str):
    print(f"\n{'='*70}")
    print(f"Checking for recycling of: {id_field}")
    print(f"{'='*70}")

    pairs = collect_id_title_pairs(id_field)
    if not pairs:
        print(f"  No {id_field} data found across {BATCH_DIRS} — nothing to check.")
        return

    suspects = 0
    for id_value, occurrences in pairs.items():
        titles_seen = []
        for title, source in occurrences:
            # Only add if it doesn't fuzzy-match something already seen
            if not any(titles_match(title, seen_title) for seen_title, _ in titles_seen):
                titles_seen.append((title, source))

        if len(titles_seen) > 1:
            suspects += 1
            print(f"\n  🛑 {id_field} {id_value} appears with {len(titles_seen)} DIFFERENT titles:")
            for title, source in titles_seen:
                print(f"     [{source}] {title[:90]}")

    print(f"\n  Total {id_field} values checked: {len(pairs)}")
    print(f"  Suspected recycling cases found: {suspects}")
    if suspects == 0:
        print(f"  ✅ No evidence of {id_field} recycling found in your local history.")


def main():
    report("question_id")
    report("post_id")
    print(f"\n{'='*70}")
    print("Note: this only covers IDs that have appeared in your own batch_jobs")
    print("files. A clean result here means 'no recycling observed in your")
    print("history so far' — not 'Metaculus guarantees IDs are never reused'.")
    print("The dedup guard is worth keeping either way as cheap insurance.")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()