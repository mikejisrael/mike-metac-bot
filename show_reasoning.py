"""
show_reasoning.py — Display the bot's reasoning for a given question ID.

Usage:
  python show_reasoning.py              # prompts for question ID
  python show_reasoning.py 38063        # directly show reasoning for Q38063
"""

import json
import glob
import sys
import os

BATCH_DIRS = ["Meta batches", "tournament_batches"]


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
                if q_id and item.get("reasoning"):
                    # Keep most recent if duplicate (later files overwrite earlier)
                    all_results[q_id] = {
                        "question_text":  item.get("question_text", ""),
                        "probability":    item.get("probability") or item.get("submitted_forecast"),
                        "original_prob":  item.get("original_prob"),
                        "reasoning":      item.get("reasoning", ""),
                        "refresh_reason": item.get("refresh_reason", ""),
                        "community_pred": item.get("community_pred"),
                        "gap_pts":        item.get("gap_pts"),
                        "challenge":      item.get("challenge"),
                        "page_url":       item.get("page_url"),
                        "source_file":    rf,
                    }
        except Exception as e:
            print(f"Warning: could not load {rf}: {e}")

    return all_results


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

    # Get question ID from command line or prompt
    if len(sys.argv) > 1:
        try:
            q_id = int(sys.argv[1])
        except ValueError:
            print(f"Invalid question ID: {sys.argv[1]}")
            return
    else:
        try:
            q_id = int(input("\nEnter question ID: ").strip())
        except (ValueError, EOFError):
            print("Invalid input.")
            return

    display_reasoning(q_id, all_results)


if __name__ == "__main__":
    main()