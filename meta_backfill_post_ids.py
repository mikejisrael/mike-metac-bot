"""
meta_backfill_post_ids.py — one-off backfill (2026-07-06).

Confirmed 2026-07-06: fetch_question_by_id() in meta_refresh_forecast.py
requires post_id to look up a question, and silently skips (rather than
guessing) whenever it's missing. Two locally-known legacy records —
question_id 39825 and 6462 — predate the post_id fix and have
post_id: None in their batch_jobs_*.json files, making them permanently
unrefreshable. This is exactly what the new find_questions_to_refresh()
"no_post_id" flag (added 2026-07-06) now catches automatically going
forward — this script is the one-time fix for the two it already found.

Patches post_ids[custom_id] in whichever batch_jobs_*.json file(s)
reference each given question_id. Dry-run by default — prints exactly
what would change without writing anything; pass --apply to actually
save.

⚠️ Note: only patches post_id — Q39825 turned out to have a valid post_id
already (this backfill isn't relevant to it after all; see the separate
"closed to forecasting despite future resolve_time" issue, handled via
meta_refresh_exclusions.py instead). This script now only has one real
patch to make: Q6462.

Usage:
  python meta_backfill_post_ids.py            # preview only, writes nothing
  python meta_backfill_post_ids.py --apply     # actually patch the files
"""

import os
import sys
import json
import glob

BATCH_DIR = "meta batches"

# question_id -> real post_id. Confirm each one against the actual
# Metaculus URL before trusting it — do not add entries here from
# inference alone.
POST_ID_BACKFILL = {
    # Confirmed 2026-07-06 via manual Metaculus title lookup (see
    # URL_Manual_Lookup.xlsx) — these are the 14 remaining after
    # meta_test_qid_as_postid.py ruled out question_id==post_id for them
    # (11 pointed at a different real question, 3 were outright 404s).
    # The 8 confirmed via that automated shortcut, and Q6462 confirmed
    # earlier via its own manual lookup, have already been applied in
    # prior runs of this script — removed from this list since there's
    # nothing left to do for them.
    38067: 38770,
    38463: 39127,
    38464: 39128,
    40104: 40543,
    40107: 40546,
    40303: 40969,
    41075: 41364,
    41076: 41365,
    41219: 41490,
    41231: 41502,
    41418: 41678,
    43732: 43703,
    43735: 43706,
    43738: 43709,
}


def main():
    apply = "--apply" in sys.argv

    job_files = (
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_2*.json")) +
        glob.glob(os.path.join(BATCH_DIR, "batch_jobs_refresh_*.json"))
    )
    if not job_files:
        print(f"No batch_jobs files found in {BATCH_DIR}/")
        return

    total_patches = 0
    for jf in job_files:
        try:
            with open(jf) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ⚠️  skipping unreadable {jf}: {e}")
            continue

        question_ids = data.get("question_ids", {})
        post_ids = data.setdefault("post_ids", {})
        changed = False

        for custom_id, q_id in question_ids.items():
            if q_id in POST_ID_BACKFILL:
                current = post_ids.get(custom_id)
                new_post_id = POST_ID_BACKFILL[q_id]
                if current == new_post_id:
                    continue  # already correct, nothing to do
                print(f"  {jf}: custom_id={custom_id} question_id={q_id} "
                      f"post_id {current!r} -> {new_post_id}")
                post_ids[custom_id] = new_post_id
                changed = True
                total_patches += 1

        if changed and apply:
            with open(jf, "w") as f:
                json.dump(data, f, indent=2)
            print(f"    ✅ saved {jf}")

    print(f"\n{total_patches} patch(es) {'applied' if apply else 'would be applied (dry run)'}.")
    if not apply and total_patches:
        print("Run with --apply to actually write these changes.")
    elif apply and total_patches == 0:
        print("Nothing to patch — either already correct, or these question_ids "
              "weren't found in any batch_jobs file.")


if __name__ == "__main__":
    main()