"""
meta_triage_remaining_no_post_id.py — one-off diagnostic (2026-07-06).

Of the 22 no-post_id questions found, 8 were confirmed safe to
auto-backfill (question_id == post_id, via meta_test_qid_as_postid.py)
and 14 were not: 11 where question_id-as-post_id pointed at a
DIFFERENT real question (a coincidental collision, not the right
answer), and 3 where it 404'd outright. Both cases genuinely need a
manual Metaculus lookup by title — there's no shortcut left to try.

Before spending that effort on all 14, this shows each one's local
resolve_time and days-to-resolve, sorted soonest-first — if a question's
resolve_time has already passed, fixing its post_id accomplishes
nothing (it'd just get skipped as closed instead of skipped as
unfetchable — same practical outcome). Lets Mike prioritize which are
actually worth a manual lookup.

Read-only. Does not look anything up or modify anything.
"""

from datetime import datetime, timezone

from meta_refresh_forecast import load_all_batches, find_questions_to_refresh

REMAINING_QUESTION_IDS = {
    # mismatches — question_id-as-post_id pointed at a different question
    38067, 40107, 40303, 41075, 41076, 41219, 41231, 41418, 43732, 43735, 43738,
    # confirmed 404s — question_id doesn't exist as any post at all
    38463, 38464, 40104,
}


def main():
    all_forecasts = load_all_batches()
    _, _, _, no_post_id, _ = find_questions_to_refresh(all_forecasts)

    now = datetime.now(timezone.utc)
    entries = []
    for f in no_post_id:
        if f["question_id"] not in REMAINING_QUESTION_IDS:
            continue
        resolve_time_str = f.get("resolve_time")
        resolve_time = None
        if resolve_time_str:
            try:
                resolve_time = datetime.fromisoformat(resolve_time_str.replace("Z", "+00:00"))
            except Exception:
                pass
        days_to_resolve = (resolve_time - now).days if resolve_time else None
        entries.append({
            "question_id": f["question_id"],
            "question_text": f["question_text"],
            "resolve_time": resolve_time_str,
            "days_to_resolve": days_to_resolve,
        })

    # Sort: still-resolving-in-the-future first (soonest first), already-past next.
    entries.sort(key=lambda e: (e["days_to_resolve"] is None,
                                 e["days_to_resolve"] if e["days_to_resolve"] is not None else 0,))

    still_relevant = [e for e in entries if e["days_to_resolve"] is not None and e["days_to_resolve"] >= 0]
    likely_moot = [e for e in entries if e["days_to_resolve"] is not None and e["days_to_resolve"] < 0]
    unknown = [e for e in entries if e["days_to_resolve"] is None]

    print(f"Of {len(entries)} remaining no-post_id questions needing a manual lookup:\n")

    if still_relevant:
        print(f"🟢 {len(still_relevant)} still resolving in the future — worth a manual lookup:")
        for e in still_relevant:
            print(f"    Q{e['question_id']} ({e['days_to_resolve']}d to resolve): {e['question_text'][:65]}")
    if likely_moot:
        print(f"\n🔴 {len(likely_moot)} already past their resolve_time — fixing post_id "
              f"wouldn't change anything (would just be skipped as closed instead):")
        for e in likely_moot:
            print(f"    Q{e['question_id']} ({-e['days_to_resolve']}d ago): {e['question_text'][:65]}")
    if unknown:
        print(f"\n⚠️  {len(unknown)} have no resolve_time on file at all — can't assess, "
              f"worth a lookup just to find out:")
        for e in unknown:
            print(f"    Q{e['question_id']}: {e['question_text'][:65]}")


if __name__ == "__main__":
    main()