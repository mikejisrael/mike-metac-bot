"""
show_reasoning.py — Display the bot's reasoning for a given question.

Usage:
  python show_reasoning.py                              # prompts for input
  python show_reasoning.py 38063                         # by question_id
  python show_reasoning.py https://www.metaculus.com/questions/43182/some-slug/
                                                          # by URL (post_id)
  python show_reasoning.py "Will the S&P 500 close above"
                                                          # by question text (fuzzy match)

Accepts a question_id, a post_id, a full Metaculus URL, or a fragment of
the question text — see resolve_question_id() below for how each is
disambiguated. Metaculus URLs contain the POST id, not the question_id —
these are frequently different numbers in this codebase's data (see
tournament_forecast.py's _set_cp docstring for the live-confirmed example)
— so a pasted URL is resolved via a post_id -> question_id index built
from your saved results, not treated as a question_id directly.
"""

import json
import glob
import re
import sys
import os

from meta_question_matching import titles_match

BATCH_DIRS = ["meta batches", "tournament_batches"]


def load_all_results() -> dict:
    """Load all batch results into a single dict keyed by question_id."""
    all_results = {}

    # Find all results files across both batch folders
    result_files = sorted(
        f for d in BATCH_DIRS
        for f in glob.glob(os.path.join(d, "batch_results*.json"))
    )

    if not result_files:
        print(f"No batch_results*.json files found in {BATCH_DIRS}")
        return {}

    for rf in result_files:
        try:
            with open(rf) as f:
                data = json.load(f)
            for custom_id, item in data.items():
                q_id = item.get("question_id")
                new_title = item.get("question_text", "")
                if q_id and item.get("reasoning"):
                    # Guard added 2026-06-30: previously this just overwrote
                    # all_results[q_id] unconditionally, on the (never
                    # actually confirmed) assumption that two entries with
                    # the same question_id are always the same question. A
                    # local-data check found that assumption did fail in
                    # practice — but from a since-fixed post_id/question_id
                    # endpoint bug, not from genuine Metaculus ID recycling.
                    # Keeping this guard anyway: it's free when titles match
                    # (the normal case) and loudly flags it instead of
                    # silently showing the wrong question's reasoning if a
                    # mismatch ever happens again, for any reason.
                    existing = all_results.get(q_id)
                    if existing and not titles_match(existing["question_text"], new_title):
                        print(f"  🛑 Q{q_id}: title mismatch between source files — "
                              f"NOT silently merging.")
                        print(f"       {existing['source_file']}: {existing['question_text'][:80]}")
                        print(f"       {rf}: {new_title[:80]}")
                        print(f"       Keeping the most recently loaded entry — if this is "
                              f"unexpected, check both source files.")
                    # Keep most recent if duplicate (later files overwrite earlier)
                    all_results[q_id] = {
                        "question_text":  new_title,
                        "probability":    item.get("probability") or item.get("submitted_forecast"),
                        "original_prob":  item.get("original_prob"),
                        "reasoning":      item.get("reasoning", ""),
                        "research_text":  item.get("research_text"),
                        "refresh_reason": item.get("refresh_reason", ""),
                        "community_pred": item.get("community_pred"),
                        "gap_pts":        item.get("gap_pts"),
                        "challenge":      item.get("challenge"),
                        "page_url":       item.get("page_url"),
                        "post_id":        item.get("post_id"),
                        "source_file":    rf,
                    }
        except Exception as e:
            print(f"Warning: could not load {rf}: {e}")

    return all_results


def resolve_question_id(raw: str, all_results: dict) -> int | None:
    """Resolve a question_id, post_id, Metaculus URL, or question-text
    fragment into a single question_id. Returns None if it can't be
    resolved (caller is responsible for printing why — kept separate so
    main() and any future caller can phrase failures differently)."""
    raw = raw.strip()

    # 1. Full URL — Metaculus puts the POST id in the path, not the
    # question_id (e.g. metaculus.com/questions/43182/some-slug/ — 43182
    # there is a post_id, which is commonly a DIFFERENT number from the
    # question_id in this codebase's data). Resolve via the post_id index
    # built from your saved results, never treat the URL number as a
    # question_id directly.
    url_match = re.search(r"metaculus\.com/questions/(\d+)", raw)
    if url_match:
        post_id = int(url_match.group(1))
        matches = [qid for qid, info in all_results.items() if info.get("post_id") == post_id]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            print(f"  ⚠️  post_id {post_id} matched {len(matches)} question_ids "
                  f"({matches}) — using the first. This shouldn't normally happen; "
                  f"investigate if it does.")
            return matches[0]
        print(f"  ❌ No saved result has post_id {post_id} (from that URL). "
              f"It may not have been forecast yet, or predates post_id being saved.")
        return None

    # 2. Bare integer — try as question_id first (the common case, and
    # what this script has always accepted), then fall back to treating it
    # as a post_id in case Mike copied the number straight from a URL bar
    # without pasting the full link.
    if raw.lstrip("-").isdigit():
        candidate = int(raw)
        if candidate in all_results:
            return candidate
        matches = [qid for qid, info in all_results.items() if info.get("post_id") == candidate]
        if len(matches) == 1:
            print(f"  (interpreted {candidate} as a post_id, not question_id — "
                  f"resolved to Q{matches[0]})")
            return matches[0]
        print(f"  ❌ {candidate} isn't a known question_id or post_id.")
        return None

    # 3. Free text — fuzzy match against question_text using the same
    # titles_match() helper the rest of this codebase already trusts.
    text_matches = [
        (qid, info["question_text"]) for qid, info in all_results.items()
        if titles_match(raw, info["question_text"]) or raw.lower() in info["question_text"].lower()
    ]
    if len(text_matches) == 1:
        return text_matches[0][0]
    if len(text_matches) > 1:
        print(f"  ⚠️  '{raw}' matched {len(text_matches)} questions — be more specific:")
        for qid, title in text_matches[:10]:
            print(f"       Q{qid}: {title[:90]}")
        return None
    print(f"  ❌ No question text matched '{raw}'.")
    return None


def display_reasoning(q_id: int, all_results: dict):
    if q_id not in all_results:
        print(f"\n❌ No reasoning found for Q{q_id}")
        print(f"   Available question IDs: {sorted(all_results.keys())[:10]}{'...' if len(all_results) > 10 else ''}")
        return

    item = all_results[q_id]
    prob = item["probability"]
    original = item.get("original_prob")
    source = item["source_file"]
    community = item.get("community_pred")
    gap_pts = item.get("gap_pts")
    challenge = item.get("challenge")
    research_text = item.get("research_text")
    # Prefer the library-verified URL; fall back to the question-id URL only if absent.
    url = item.get("page_url") or f"https://www.metaculus.com/questions/{q_id}/"

    print("\n" + "=" * 70)
    print(f"Q{q_id}: {item['question_text']}")
    print("=" * 70)
    print(f"Source file:  {source}")

    if original is not None and original != prob:
        print(f"Probability:  {original:.0%} → {prob:.0%}  (refreshed)")
        if item.get("refresh_reason"):
            print(f"Refresh reason: {item['refresh_reason']}")
    else:
        print(f"Probability:  {prob:.0%}" if prob is not None else "Probability:  n/a")

    if community is not None:
        gap_str = f"  (gap {gap_pts:+.0f}pt)" if gap_pts is not None else ""
        print(f"Community:    {community:.0%}{gap_str}")

    print(f"Metaculus:    {url}")
    print("-" * 70)

    if research_text:
        print("📡 RESEARCH (real-time web search, fetched for this question):")
        print(research_text)
        print("-" * 70)
    else:
        print("📡 RESEARCH: none (no live_data match and no research result for this question)")
        print("-" * 70)

    print(item["reasoning"])

    if challenge:
        print("\n" + "▼ DIVERGENCE CHALLENGE " + "▼" * 47)
        print(challenge)
        print("▲" * 70)

    print("=" * 70)


def main():
    all_results = load_all_results()

    if not all_results:
        print("No results found. Run a batch first.")
        return

    total_files = sum(len(glob.glob(os.path.join(d, "batch_results*.json"))) for d in BATCH_DIRS)
    print(f"Loaded reasoning for {len(all_results)} questions across {total_files} result file(s)")

    # Get question identifier from command line or prompt — accepts
    # question_id, post_id, a Metaculus URL, or question-text fragment;
    # see resolve_question_id() for how each is disambiguated.
    if len(sys.argv) > 1:
        raw = " ".join(sys.argv[1:])  # join in case an unquoted text query has spaces
    else:
        try:
            raw = input("\nEnter question ID, post ID, URL, or question text: ").strip()
        except EOFError:
            print("Invalid input.")
            return

    if not raw:
        print("No input given.")
        return

    q_id = resolve_question_id(raw, all_results)
    if q_id is None:
        return

    display_reasoning(q_id, all_results)


if __name__ == "__main__":
    main()