"""
meta_question_matching.py — Shared question-identity guard.

Metaculus question IDs are not guaranteed stable indefinitely: an ID can end
up referring to a different question than the one you originally stored it
against (confirmed by querying the old /api2/questions/?ids= list-filter
with a nonsense ID and getting back an unrelated real question instead of an
empty result — see refresh_forecasts.py's fetch_question_by_id for the full
story). Any code that matches a stored question_id against a freshly fetched
question should run that match through titles_match() before treating it as
the same question.

Used by: batch_forecast.py, tournament_forecast.py, refresh_forecasts.py.
Keep this file as the single source of truth — don't re-copy the function
body into individual scripts again.
"""

import re

# Common words that carry no identifying signal for a question title —
# includes generic English filler AND interrogative question-template words
# ("how", "many", "what", etc.) which recur across completely unrelated
# Metaculus questions because so many titles share the same "How many X
# will Y by <date>?" scaffolding.
_STOP_WORDS = {
    "will", "the", "a", "an", "be", "is", "are", "to", "of", "in",
    "on", "by", "before", "after", "and", "or", "for", "at", "than",
    "with", "this", "that", "any", "exceed",
    "how", "many", "what", "which", "much", "when", "where", "why",
    "does", "do", "did", "who", "whom",
}


def _significant_words(s: str) -> set[str]:
    words = re.findall(r"[a-z0-9]+", (s or "").lower())
    # Bare numeric tokens (years, counts) are excluded entirely — they
    # recur across unrelated questions (almost everything currently open
    # mentions "2026", say) and provide no real distinguishing signal on
    # their own.
    return {w for w in words if w not in _STOP_WORDS and len(w) > 2 and not w.isdigit()}


def titles_match(stored_text: str, fetched_title: str, threshold: float = 0.35,
                  min_overlap: int = 2) -> bool:
    """Cheap but effective sanity check: do these two titles refer to the same
    question? Uses word-overlap rather than exact match, because minor
    wording differences (capitalization, a trailing '?') are normal even for
    a correct match. A genuine ID mismatch (different topic entirely) will
    share almost no significant words and will fail this easily.

    Two safeguards beyond a plain overlap ratio:
      - Generic interrogative/template words and bare numbers are stripped
        before comparing (see _significant_words) — otherwise two unrelated
        "How many X will happen in 2026?" questions look deceptively similar.
      - An absolute minimum overlap (min_overlap) is required in addition to
        the ratio, so two short titles can't pass on a single lucky word
        match alone.

    Args:
        stored_text: the title/text we have on file for this question ID.
        fetched_title: the title just returned by the API for that same ID.
        threshold: minimum overlap ratio (relative to the shorter title's
            significant-word count) required to call it a match.
        min_overlap: minimum absolute number of overlapping significant
            words required, unless the shorter title has fewer words than
            this, in which case full overlap is required instead.

    Returns:
        True if the two titles plausibly refer to the same question.
    """
    stored_words = _significant_words(stored_text)
    fetched_words = _significant_words(fetched_title)
    if not stored_words or not fetched_words:
        return False
    overlap = stored_words & fetched_words
    smaller = min(len(stored_words), len(fetched_words))
    required_overlap = min(min_overlap, smaller)
    if len(overlap) < required_overlap:
        return False
    return smaller > 0 and (len(overlap) / smaller) >= threshold