"""
merge_batch_history.py -- one-off migration: copies every batch_results_*.json
from tournament_batches_v2 (the isolated dev sandbox used while proving out
Market Pulse support) into tournament_batches (the real production
directory Stage 2 now points TOURNAMENT_BATCH_DIR at).

WHY THIS IS NEEDED: Stage 2 (2026-07-13) switched tournament_forecast_v2.py's
TOURNAMENT_BATCH_DIR from "tournament_batches_v2" to "tournament_batches" so
already_done dedup could see v1's real FutureEval history. But every Market
Pulse forecast submitted over the past several days was written to the OLD
directory -- switching orphaned that history entirely. Confirmed live: a run
right after the switch queued all 40 open Market Pulse sub-questions as
"to forecast", because from the script's new vantage point none of them had
ever been touched. This merges the real history back into view WITHOUT
re-running anything or spending anything -- it's just copying files.

Filenames are timestamped (batch_results_<YYYYMMDD>_<HHMM>.json), so
collisions between the two directories are only possible if a v1-relevant
run and a v2 run happened in the exact same minute -- vanishingly unlikely
given v1 only ever wrote FutureEval/ACX2026/etc, never Market Pulse. Skips
(never overwrites) any filename that already exists in the destination,
and reports exactly what it did.

Usage:
    python merge_batch_history.py            # do it
    python merge_batch_history.py --dry-run  # show what WOULD be copied, copy nothing
"""

import os
import sys
import shutil
import glob

SRC_DIR = "tournament_batches_v2"
DST_DIR = "tournament_batches"

dry_run = "--dry-run" in sys.argv

if not os.path.isdir(SRC_DIR):
    raise SystemExit(f"Source directory {SRC_DIR!r} not found -- nothing to merge.")
if not os.path.isdir(DST_DIR):
    raise SystemExit(f"Destination directory {DST_DIR!r} not found -- check you're running "
                      f"this from the same folder as tournament_forecast_v2.py.")

src_files = sorted(glob.glob(os.path.join(SRC_DIR, "batch_results_*.json")))
print(f"Found {len(src_files)} batch_results_*.json file(s) in {SRC_DIR}/\n")

copied = 0
skipped = 0

for src_path in src_files:
    filename = os.path.basename(src_path)
    dst_path = os.path.join(DST_DIR, filename)

    if os.path.exists(dst_path):
        print(f"  SKIP (already exists in {DST_DIR}/): {filename}")
        skipped += 1
        continue

    if dry_run:
        print(f"  WOULD COPY: {filename}")
    else:
        shutil.copy2(src_path, dst_path)
        print(f"  copied: {filename}")
    copied += 1

print(f"\n{'Would copy' if dry_run else 'Copied'}: {copied} | Skipped (already present): {skipped}")
if dry_run:
    print("\nRun without --dry-run to actually copy.")
else:
    print(f"\n{DST_DIR}/ now has the full Market Pulse forecast history. "
          f"Next tournament_forecast_v2.py run should correctly see all "
          f"previously-forecast sub-questions as already done again.")