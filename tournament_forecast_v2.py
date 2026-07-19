"""
tournament_forecast_v2.py — DEV BRANCH of tournament_forecast.py, built to
add Market Pulse Challenge 26Q3 support without touching the production
file (which stays on its proven 30-min cron, untouched, per Mike's
2026-07-10 request — "leave tournament_forecast.py untouched while we
develop this new branch. Once we've proven the new branch, we can
replace the old one.").

Two additions on top of the base file, both scoped as tightly as possible
to Market Pulse specifically so FutureEval's existing behavior is
unchanged:

1. GROUP-OF-QUESTIONS SUPPORT. Confirmed live (test3.py, 2026-07-10, Q44534
   "VIX biweekly max Q3 2026") that Market Pulse questions are structured
   as group_of_questions containers — a single post wraps several numeric
   sub-questions (e.g. one per biweekly period), each with its own real
   question id. The base file's fetch_tournament_questions() reads
   post.get("question") directly and has no handling for group posts at
   all (a group post has no top-level "question" field — it has
   "group_of_questions" instead) — so every Market Pulse post was
   silently falling into no_question_field_count and vanishing with no
   distinct log line. _unpack_group_post() below mirrors what
   forecasting_tools' MetaculusClient._unpack_group_question does
   internally (confirmed via source inspection of the installed library,
   2026-07-10), but works at the raw dict level, since this file parses
   /api/posts/ JSON directly rather than going through ApiFilter. Each
   unpacked sub-question becomes an ordinary post-shaped dict that flows
   through the EXISTING binary/numeric/multiple_choice parsing, CP fetch,
   prompt-building, and submission code completely unchanged.

2. PER-TOURNAMENT REFRESH GATING. Every other tournament in this file
   forecasts a question ONCE, ever — already_done exclusion is permanent.
   That's correct for FutureEval (Metaculus's own docs: "our normal
   FutureEval tournaments do not require updating"), but Market Pulse
   explicitly requires bots to "continuously update forecasts during the
   question lifetime" (Metaculus Summer 2026 FutureEval Bot Tournament
   announcement, EA Forum). So already_done now also carries a
   submitted_at timestamp per question (added to each result record at
   write time in run(), below), and Market Pulse questions specifically
   are gated through a purpose-built "final hour before close" check
   (see the dedup block below) instead of being
   permanently excluded once forecast. Every other tournament's dedup
   behavior is untouched — this only branches differently when a
   question's source post is tagged with MARKET_PULSE_TOURNAMENT_ID (see
   the _source_tournament_id tagging in the per-tournament fetch loop).

MERGE STATUS (v1 -> v2), updated as it progresses:
  Stage 1 (2026-07-13): FutureEval re-added to TOURNAMENT_IDS, tested
  against real FutureEval + Market Pulse data with TOURNAMENT_BATCH_DIR
  still isolated ("tournament_batches_v2") — validated cleanly.
  Stage 2 (2026-07-13): TOURNAMENT_BATCH_DIR below now points at the REAL
  production "tournament_batches" — v2's already_done dedup can see v1's
  actual forecast history now, so it won't re-forecast something v1
  already answered. This also resolves punch-list item #9's "merge
  strategy" decision: full merge, shared history from here on.
  Stage 3 (not yet): retire v1's cron, decommission tournament_forecast.py.
  UNTIL STAGE 3: v1's cron remains the production system for FutureEval's
  90-minute windows — run this file manually only, not on a schedule.

Handles binary, numeric, discrete, multiple_choice, AND (new) numeric
sub-questions unpacked from group_of_questions containers.
Calls Claude synchronously and submits to Metaculus immediately in the same run.

Usage:
  python tournament_forecast_v2.py             # forecast and submit all open questions
  python tournament_forecast_v2.py --dry-run   # fetch + show what WOULD be forecast, no
                                                 # Claude/OpenRouter calls, no Metaculus
                                                 # submissions, no state written to disk
  python tournament_forecast_v2.py --simulate-now=2026-07-13T02:30:00Z
                                                # Override "now" for the final-hour Market
                                                # Pulse refresh trigger and all close-time
                                                # math — lets you test that trigger on
                                                # demand instead of waiting on real time.
                                                # Everything else (Metaculus fetch,
                                                # Claude calls, submissions) still happens
                                                # for real unless combined with --dry-run.

Choosing tournaments:
  Defaults to [FUTUREEVAL_TOURNAMENT_ID, MARKET_PULSE_TOURNAMENT_ID]. Override
  without editing either file:
      set METAC_TOURNAMENT_IDS=32977             (bot-testing-area only)
      set METAC_TOURNAMENT_IDS=33022,ACX2026     (comma-separated, mix of
                                                   numeric IDs and slugs)
"""

import asyncio
import os
import re
import glob
import json
import math
import time
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

import anthropic
import meta_batch_forecast as bf
from forecasting_tools import (
    MetaculusApi, MetaculusClient,
    BinaryQuestion, NumericQuestion, MultipleChoiceQuestion
)
from cached_llm import build_forecaster_system_prompt
from meta_prompt_cache import cacheable_system_block
from meta_cp_extract import extract_live_cp
from meta_alerts import send_alert
from meta_research import research_question
from live_data import detect_data_needs, format_live_data_for_prompt
from meta_watch import check_new_futureeval_questions, FUTUREEVAL_TOURNAMENT_ID
# NOTE (2026-07-11): meta_refresh_gate.is_refresh_due()/MIN_REFRESH_GAP_HOURS
# are no longer used here — replaced by the final-hour-before-close check
# in the dedup block below (see that block's comments for why the generic
# 192h gate was structurally too long for Market Pulse's 59-155h sub-
# question lifespans). Not importing them keeps it obvious this file no
# longer depends on that shared constant.
# CHANGED (2026-07-06): check_resolutions and check_refresh_candidates
# moved to meta_phase_reports.yaml's daily cron (via meta_watch.py's new
# run_watch_checks() entry point) — they're fully self-contained (each
# hits the Metaculus API directly) and only ever ran here because this
# was the script with the most frequent existing cadence, not because
# FutureEval's cadence actually matters to them; FutureEval's ~3-hour
# windows are too tight for a refresh to ever be actionable anyway.
# check_new_futureeval_questions stays — it needs open_tid_posts_by_id
# from this script's own fetch loop below and is FutureEval-specific.

# ─── Config ───────────────────────────────────────────────────────────────────
# NOTE: the paragraph below describes tournament_forecast.py's (v1's)
# original design — inherited into this file when v2 was created and never
# updated since. Kept for historical context on WHY meta_batch_forecast.py
# handles ACX2026/Climate/Metaculus Cup via its cheaper Batch API path
# rather than synchronously here (that reasoning still holds), but the
# "defaults to FutureEval ONLY" claim is no longer accurate for THIS file
# as of the Stage 1 v1->v2 merge (2026-07-13) — see the TOURNAMENT_IDS
# block below for what v2 actually defaults to now.
#
# Cost optimization split (2026-06-30): this script previously fetched and
# synchronously forecast ALL of bf.ALLOWED_TOURNAMENTS — including ACX2026,
# Climate Tipping Points, and Metaculus Cup, none of which are remotely as
# time-sensitive as FutureEval (90-minute close windows). Those three now
# only get forecast via meta_batch_forecast.py's Batch API path (50%
# cheaper, on its own ~every-3-days cron schedule, separate from this
# script's tighter cadence) — see meta_batch_forecast.py's ALLOWED_TOURNAMENTS
# for the other side of this split.
# Override without editing either file: set METAC_TOURNAMENT_IDS to a
# comma-separated list, e.g.
#     set METAC_TOURNAMENT_IDS=32977
# (32977 = bot-testing-area, useful for testing in isolation from real tournaments)
# Market Pulse Challenge 26Q3 (slug: market-pulse-26q3). Numeric ID
# confirmed live 2026-07-10 (Mike checked the site directly) — the raw
# /api2/questions/?tournaments=<slug> and ?project=<id> lookups we tried
# first both came back empty/wrong (see test.py/test2.py/test3.py from
# that session), so this is NOT re-derived from any of those failed
# lookups; it's the number Mike confirmed against the actual tournament
# page. type="tournament" (confirmed via test3.py against Q44534's
# "projects" field) — NOT question_series, so this does NOT need the
# project= fetch path meta_batch_forecast.py uses for the 5 question_series
# tournaments.
MARKET_PULSE_TOURNAMENT_ID = 33066

# TEST_QUESTION_ID_LIMIT RESET (2026-07-14): the 5 Metaculus Cup ad-hoc
# questions (Farage/Clacton, Brent crude, copper futures, Albania PM,
# Australia CWG golds) all submitted successfully and confirmed on the
# site — "5 questions not predicted" dropped to 0. Back to None (default,
# unrestricted) state. Set to a real set again if a similar targeted
# ad-hoc run is needed later.
TEST_QUESTION_ID_LIMIT = None

_env_override = os.getenv("METAC_TOURNAMENT_IDS")
if _env_override:
    TOURNAMENT_IDS = [t.strip() for t in _env_override.split(",") if t.strip()]
else:
    # STAGE 1 of the v1->v2 merge (2026-07-13, Mike's call after reviewing
    # where the project's heading): FutureEval re-added here. Deliberately
    # staged, not a single cutover — TOURNAMENT_BATCH_DIR below STAYS
    # pointed at tournament_batches_v2, NOT the real tournament_batches,
    # for this stage specifically. That means:
    #   - This exercises the full funnel (check_new_futureeval_questions,
    #     binary/numeric/MC parsing, submission) against REAL FutureEval
    #     questions, for real validation — not synthetic test data.
    #   - v2's already_done dedup can't see v1's forecast history yet
    #     (different directory), so v2 will treat every FutureEval
    #     question as new and forecast+submit it independently of
    #     whatever v1's own 30-min cron already did or will do.
    #   - This MUST run alongside (not instead of) v1's existing
    #     production cron for now — v1 is still the one Metaculus actually
    #     relies on for FutureEval's 90-minute windows during this stage.
    #     Both scripts submitting real forecasts to real FutureEval
    #     questions independently is the intended comparison, not a bug —
    #     but it does mean don't run this unattended/on a cron yet.
    # Stage 2 (once this output looks right): point TOURNAMENT_BATCH_DIR
    # at the real tournament_batches, so already_done dedup sees v1's
    # actual history. Stage 3: retire v1's cron, decommission v1.
    TOURNAMENT_IDS = [
        FUTUREEVAL_TOURNAMENT_ID,
        MARKET_PULSE_TOURNAMENT_ID,
    ]
# STAGE 2 of the v1->v2 merge (2026-07-13, Mike's call after Stage 1
# validated cleanly against real FutureEval + Market Pulse data): now
# pointed at the REAL production "tournament_batches", not the isolated
# tournament_batches_v2 sandbox. This is the whole point of Stage 2 —
# v2's already_done dedup can now see v1's actual forecast history, so it
# won't try to re-forecast a FutureEval question v1 already answered.
#
# CONSEQUENCE WORTH BEING DELIBERATE ABOUT: this file's writes
# (batch_results_<timestamp>.json) now land in the SAME directory v1
# writes to and meta_dashboard.py reads from — v2-submitted forecasts
# will appear in the dashboard mixed with v1's, indistinguishable by
# directory alone. That's the intended end state (this effectively
# resolves punch-list item #9's "merge strategy" decision: full merge,
# shared history from here on, not a separate reconciliation later).
#
# STILL TRUE, same caution as Stage 1: v1's cron is still the production
# system for FutureEval's 90-minute windows. Run this manually only,
# not on a schedule, until Stage 3 (retire v1's cron, decommission v1).
TOURNAMENT_BATCH_DIR = "tournament_batches"
MODEL                = "claude-haiku-4-5"
MAX_TOKENS           = 2000

# ─── Clients ──────────────────────────────────────────────────────────────────
client_anthropic = anthropic.Anthropic()
# Tournament submissions should authenticate as the dedicated bot account
# (mike_iz_-bot), not the shared personal-account token meta_batch_forecast.py
# uses. Set METAC_TOURNAMENT_TOKEN in .env once mike_iz_-bot's own API token
# is generated; falls back to METACULUS_TOKEN so this doesn't silently break
# before that's set up.
TOURNAMENT_TOKEN = os.getenv("METAC_TOURNAMENT_TOKEN") or os.getenv("METACULUS_TOKEN")
if os.getenv("METAC_TOURNAMENT_TOKEN"):
    print("Auth: using dedicated METAC_TOURNAMENT_TOKEN (mike_iz_-bot)")
else:
    print("Auth: METAC_TOURNAMENT_TOKEN not set — falling back to shared METACULUS_TOKEN (mike_iz_)")
client_metaculus = MetaculusClient(token=TOURNAMENT_TOKEN)

# ─── Fail fast on permanently-closed questions ───────────────────────────────
# forecasting_tools' _post_question_prediction retries ANY HTTPError 3x with
# exponential backoff (up to 75s per attempt — ~100s+ total per call). That's
# correct for transient errors (network blips, momentary 5xx) but wasteful for
# a question that's permanently closed to forecasting: a 405 "already closed"
# response will never succeed no matter how many times we retry it. In
# practice this was adding 5-10+ minutes of pure dead time to a single run
# when several questions in the batch had closed between fetch and submit.
# We replace it with a version that retries everything else exactly as
# before, but recognizes this one permanent case and skips straight to
# failure instead of retrying it.
import types as _types

_original_post_question_prediction = type(client_metaculus)._post_question_prediction.__wrapped__

def _post_question_prediction_fail_fast_on_closed(self, question_id, forecast_payload):
    max_retries = 3
    delay = 2.5
    for attempt in range(max_retries + 1):
        try:
            return _original_post_question_prediction(self, question_id, forecast_payload)
        except Exception as e:
            if "already closed to forecasting" in str(e):
                print(f"  ⏭️  Q{question_id}: already closed to forecasting — skipping, no retry.")
                raise
            if attempt >= max_retries:
                raise
            import random as _random
            sleep_time = min(delay * _random.uniform(1, 8.0), 75.0)
            print(f"  Retry {attempt + 1}/{max_retries} for submission after {sleep_time:.1f}s. Error: {e}")
            time.sleep(sleep_time)
            delay *= 3

client_metaculus._post_question_prediction = _types.MethodType(
    _post_question_prediction_fail_fast_on_closed, client_metaculus
)

# ─── Redirect meta_batch_forecast's namespace (keeps dedup isolated) ─────────
bf.BATCH_DIR    = TOURNAMENT_BATCH_DIR
bf.BATCH_FILE   = os.path.join(TOURNAMENT_BATCH_DIR, "batch_jobs.json")
bf.RESULTS_FILE = os.path.join(TOURNAMENT_BATCH_DIR, "batch_results.json")
os.makedirs(TOURNAMENT_BATCH_DIR, exist_ok=True)


# ─── Question identity guard ────────────────────────────────────────────────
from meta_question_matching import titles_match


# ─── Pydantic-safe attribute setter ────────────────────────────────────────
def _set_cp(obj, cp) -> None:
    """Set community_prediction_at_access_time regardless of whether the
    underlying pydantic model declares that field. NumericQuestion and
    MultipleChoiceQuestion do not declare it and raise on a plain attribute
    assignment (confirmed via a live bot-testing-area run — this silently
    dropped 3 of 5 real forecastable questions into 'parse errors' before
    this fix, since the crash happened inside the same try block that
    builds the whole question object).

    CORRECTION 2026-06-30: this docstring previously claimed "BinaryQuestion
    allows extra attributes" — that was never actually verified, just
    assumed because community_prediction_at_access_time happens to be a
    genuinely declared field on BinaryQuestion specifically (so plain
    assignment of THIS field works fine there). It does NOT generalize:
    a live GitHub Actions run crashed when meta_batch_forecast.py tried
    plain-assigning a DIFFERENT, undeclared field
    (research_text_at_access_time) onto a BinaryQuestion object. Use this
    pydantic-safe pattern for any new per-question attribute on ANY
    question type, not just Numeric/MultipleChoice — don't assume Binary
    is exempt."""
    try:
        obj.community_prediction_at_access_time = cp
    except Exception:
        object.__setattr__(obj, "community_prediction_at_access_time", cp)


def _set_research_text(obj, text) -> None:
    """Set research_text_at_access_time regardless of whether the
    underlying pydantic model declares that field — same pydantic-safe
    pattern as _set_cp/_set_research_source, matching meta_batch_forecast.py's
    equivalent helper exactly (confirmed via reading that file directly,
    2026-07-13). Added alongside the research_source fix: run() never
    stored research_text at all (only reasoning), so the dashboard's
    detail page — which reads (local_r or {}).get("research_text", "")
    per record — has been silently showing nothing for every question
    this file has ever forecast. bf.build_user_prompt() already sets this
    for binary questions on the same object build_binary_prompt() passes
    through; this covers numeric/multiple_choice the same way."""
    try:
        obj.research_text_at_access_time = text
    except Exception:
        object.__setattr__(obj, "research_text_at_access_time", text)


def _set_research_source(obj, source) -> None:
    """Same pydantic-safe side-channel pattern as _set_cp above, for
    tracking which provider (openrouter/anthropic/None) actually served
    the research call for this question. Added 2026-07-13 (punch list
    item #6).

    FIXED 2026-07-13: originally used a different attribute name
    (research_source_used) than meta_batch_forecast.py's own equivalent
    helper — confirmed via reading that file directly — which uses
    research_source_at_access_time and already sets it inside
    bf.build_user_prompt() (called by build_binary_prompt() below on the
    SAME question object). Renamed to match exactly, so binary/numeric/
    multiple_choice all converge on one attribute regardless of which
    code path set it — run() only needs to read one name now, and binary
    questions are covered "for free" via bf.build_user_prompt() without
    needing any changes to that shared production file.

    Set on the question object itself (not returned from the builder
    functions) so run() can read it back the same way it already reads
    cp — build_numeric_prompt(question)/build_multiple_choice_prompt(question)
    mutate the SAME object passed into forecast_question(q), not a copy."""
    try:
        obj.research_source_at_access_time = source
    except Exception:
        object.__setattr__(obj, "research_source_at_access_time", source)


# ─── Step 1: Fetch open tournament questions ───────────────────────────────────
async def fetch_tournament_questions(simulate_now: datetime | None = None, dry_run: bool = False) -> list:
    # already_done maps question_id -> the title we forecast it under, so a
    # recycled ID (genuinely a different question on Metaculus's side) can be
    # detected via _titles_match instead of being silently skipped as a dup.
    #
    # FIXED 2026-06-30: previously loaded and printed here, UNSCOPED — every
    # question_id ever recorded in ANY tournament_batches file, regardless
    # of which tournament it came from. This script only got split to
    # FutureEval-ONLY today; every historical file predates that split and
    # mixes in ACX2026/Climate/Metaculus Cup forecasts too. Confirmed live:
    # printed "Excluding 92" when the real FutureEval-specific count was
    # ~17 — the other 75 were forecasts from other tournaments that happen
    # to be sitting in the same folder, not genuinely already-forecast
    # FutureEval questions. Result records don't store which tournament
    # they came from (nothing to filter by directly), so this is now
    # loaded raw here, then INTERSECTED against this run's actual fetched
    # FutureEval question_ids further below before being used or printed.
    # Trade-off: if a previously-forecast FutureEval question somehow
    # doesn't appear in this run's fetch (e.g. genuinely removed from the
    # tournament), it would no longer be excluded by this scoping — low
    # risk given the listing has been complete (3 full pages, no
    # MAX_PAGES truncation) every time so far.
    # CHANGED (v2): value is now a dict {"title":..., "submitted_at":...,
    # "status":...} instead of a bare title string — Market Pulse's
    # refresh/final-hour logic (below) needs the timestamp AND whether
    # that attempt succeeded or failed.
    #
    # FIXED (2026-07-11): this was using glob.glob() in whatever order the
    # OS filesystem happens to return (NOT guaranteed chronological) combined
    # with setdefault() (keeps the FIRST record seen per question_id) — so
    # already_done_raw could silently reflect an OLD attempt instead of the
    # most recent one, depending on directory listing order. Since
    # batch_results_<timestamp>.json filenames sort chronologically as
    # strings, sorting the glob results fixes ordering; switching from
    # setdefault() to plain assignment means the LAST (most recent) file
    # processed always wins per question_id, so this always reflects the
    # actual latest attempt — success or failure.
    already_done_raw: dict[int, dict] = {}
    for rf in sorted(glob.glob(os.path.join(TOURNAMENT_BATCH_DIR, "batch_results_2*.json"))):
        try:
            with open(rf) as f:
                data = json.load(f)
            for r in data.values():
                if r.get("question_id"):
                    already_done_raw[r["question_id"]] = {
                        "title": r.get("question_text", ""),
                        "submitted_at": r.get("submitted_at"),  # None for pre-v2 records — treated as "due" (no prior attempt on file)
                        "status": r.get("status"),
                    }
        except Exception:
            pass

    print(f"Tournaments: {TOURNAMENT_IDS}", flush=True)

    import requests
    headers = {
        "Authorization": f"Token {TOURNAMENT_TOKEN}",
        # Metaculus's bot detection appears to reject the default
        # "python-requests/x.x" user-agent on this endpoint (it returned an
        # HTML page instead of JSON, hence the json-decode failure on the
        # first attempt). Using a normal browser-style UA as a likely fix.
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    raw_posts_by_id: dict[int, dict] = {}
    now = simulate_now or datetime.now(timezone.utc)
    if simulate_now:
        print(f"  🧪 SIMULATED NOW: {simulate_now.isoformat()} (real time NOT used for "
              f"final-hour/close-time calculations this run)", flush=True)

    def _is_open_question(q: dict) -> bool:
        """True if a Metaculus question dict (the NESTED 'question' object
        within a post, not the post itself) is still open for forecasting
        — i.e. has no scheduled_close_time already in the past. FIXED
        2026-06-30: previously this check only existed inline, post-loop,
        in the funnel below — check_new_futureeval_questions was being
        called on RAW unfiltered posts (open AND closed) earlier in the
        loop, with no open/closed filtering applied at all. Confirmed
        live: this produced a misleading "158 new FutureEval questions"
        alert where 157 of those were already closed — genuinely "never
        logged before" (since this was the watch-list's first-ever run)
        but not genuinely openable, alert-worthy new questions. Extracting
        this as one shared helper, used by both the alert gating below
        and the funnel further down, means they can no longer silently
        disagree about what counts as "open" the way they just did."""
        close_time = q.get("scheduled_close_time")
        if close_time:
            close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            if close_dt < now:
                return False
        return True

    for tid in TOURNAMENT_IDS:
        tid_posts_by_id: dict[int, dict] = {}  # this tournament only — used
        # below to scope the new-FutureEval-question check; raw_posts_by_id
        # above stays the existing flat cross-tournament dict, unchanged.
        try:
            url = f"https://www.metaculus.com/api/posts/?tournaments={tid}&limit=100"
            tid_count = 0
            page_num = 0
            MAX_PAGES = 10  # safety cap — 166 open questions at limit=100 should
                             # be ~2 pages; if we're still going past this many,
                             # something's wrong (e.g. endpoint returning all
                             # historical posts, not just current open ones)
            while url and page_num < MAX_PAGES:
                page_num += 1
                # Always pause before a fetch (page or tournament) — firing
                # all 4 tournaments' paginated requests back-to-back with no
                # delay is what triggered Metaculus's 429 rate limit here.
                time.sleep(1.5)

                data = None
                fetch_exhausted_on_connection_error = False
                for attempt in range(3):
                    print(f"  ...fetching {tid} (attempt {attempt + 1}/3)...", flush=True)
                    # Added: previously a transient connection failure here
                    # (DNS blip, connection reset, etc.) raised straight out
                    # of this loop with zero retry, was caught by the outer
                    # try/except at the tid level, and silently dropped the
                    # ENTIRE tournament for the run — confirmed live: this is
                    # exactly what happened to FutureEval (id 33022, the main
                    # prize-eligible tournament) on one run, with nothing in
                    # the logs beyond a single one-line warning easy to miss.
                    # The existing 429 handling below already retries
                    # correctly; this just extends the same retry budget and
                    # backoff to connection-level failures instead of only
                    # HTTP-level ones.
                    try:
                        r = requests.get(url, headers=headers, timeout=30)
                    except (requests.exceptions.ConnectionError,
                            requests.exceptions.Timeout) as e:
                        wait = 5 * (attempt + 1)
                        print(f"  ⏳ Tournament {tid}: connection error "
                              f"({type(e).__name__}: {e}), waiting {wait}s "
                              f"before retry {attempt + 1}/3...", flush=True)
                        time.sleep(wait)
                        fetch_exhausted_on_connection_error = (attempt == 2)
                        continue
                    fetch_exhausted_on_connection_error = False
                    if r.status_code == 429:
                        wait = 5 * (attempt + 1)
                        print(f"  ⏳ Tournament {tid}: rate limited (429), "
                              f"waiting {wait}s before retry {attempt + 1}/3...", flush=True)
                        time.sleep(wait)
                        continue
                    if r.status_code != 200:
                        print(f"  ⚠️  Tournament {tid}: HTTP {r.status_code} — "
                              f"body starts with: {r.text[:150]!r}", flush=True)
                        break
                    try:
                        data = r.json()
                    except Exception:
                        print(f"  ⚠️  Tournament {tid}: non-JSON response (status {r.status_code}) — "
                              f"body starts with: {r.text[:150]!r}")
                    break

                if data is None:
                    if fetch_exhausted_on_connection_error:
                        # Loud, not just a console print: this run is almost
                        # always triggered headlessly via cron-job.org ->
                        # GitHub Actions, where console output isn't watched
                        # in real time. A dropped tournament — especially
                        # FutureEval, the main prize-eligible one — needs to
                        # surface somewhere Mike will actually see it.
                        send_alert(
                            f"Tournament {tid}: fetch failed after 3 connection-error "
                            f"retries — this tournament was SKIPPED entirely for this run.",
                            title="⚠️ Tournament fetch dropped"
                        )
                    break  # exhausted retries or hit a non-200/non-JSON response

                tid_posts = data.get("results", [])
                tid_count += len(tid_posts)
                print(f"  ...{tid} page {page_num}: +{len(tid_posts)} posts "
                      f"(running total {tid_count}), next={'yes' if data.get('next') else 'no'}",
                      flush=True)
                for post in tid_posts:
                    post_id = post.get("id")
                    if post_id is not None:
                        # NEW (v2): tag with source tournament so the
                        # dedup/refresh logic below can tell Market Pulse
                        # questions apart from everything else without
                        # re-deriving tournament membership from the
                        # post's own "projects" field (which the listing
                        # endpoint may or may not populate consistently —
                        # not verified either way, so tagging at the
                        # point we already KNOW which tid this came from
                        # is the safer bet).
                        post["_source_tournament_id"] = tid
                        raw_posts_by_id.setdefault(post_id, post)
                        tid_posts_by_id.setdefault(post_id, post)
                if not tid_posts:
                    # An empty page means we're past the real end of data —
                    # stop here even if the API still claims a 'next' link.
                    # (Observed in practice: Metaculus keeps returning a next
                    # URL indefinitely past the actual last page.)
                    break
                url = data.get("next")  # paginate — a tournament can have >100 questions
            if page_num >= MAX_PAGES:
                print(f"  ⚠️  Tournament {tid}: hit the {MAX_PAGES}-page safety cap "
                      f"({tid_count} posts so far) — stopping early. This tournament's "
                      f"endpoint may be returning all historical posts, not just open ones.",
                      flush=True)
            print(f"  Tournament {tid}: {tid_count} posts", flush=True)
            if str(tid) == str(FUTUREEVAL_TOURNAMENT_ID):
                open_tid_posts_by_id = {
                    pid: post for pid, post in tid_posts_by_id.items()
                    if post.get("question") and _is_open_question(post["question"])
                }
                if not dry_run:
                    check_new_futureeval_questions(open_tid_posts_by_id)
        except Exception as e:
            print(f"  ⚠️  Could not fetch tournament {tid}: {e}", flush=True)
            # Same reasoning as the connection-error alert above: an
            # unexpected failure here (anything not already handled by the
            # retry logic — e.g. a genuine library/parsing bug) still
            # silently drops the whole tournament if all we do is print.
            send_alert(
                f"Tournament {tid}: fetch failed with an unexpected error — "
                f"this tournament was SKIPPED entirely for this run.\n{str(e)[:150]}",
                title="⚠️ Tournament fetch dropped"
            )

    raw_posts = list(raw_posts_by_id.values())

    # NEW (v2): unpack group_of_questions posts (Market Pulse) into one
    # synthetic post per sub-question, BEFORE the open/closed filter below
    # (which reads post["question"] and would just skip a group post
    # entirely, same silent no_question_field_count fate as before this
    # change). Mirrors forecasting_tools.MetaculusClient._unpack_group_question
    # (confirmed via source inspection, 2026-07-10) at the raw dict level,
    # since this file never goes through ApiFilter for its own fetch.
    def _unpack_group_post(post: dict) -> list[dict]:
        group = post.get("group_of_questions")
        if not group:
            return [post]
        sub_posts = []
        for sub_q in group.get("questions", []):
            sub_q = dict(sub_q)
            # Group-level fields live on the group, not each sub-question —
            # borrow them down so build_*_prompt() (which reads
            # question.background_info / resolution_criteria / fine_print)
            # sees the same text a human forecaster would on the question
            # page, same as the library's own unpack does.
            sub_q.setdefault("fine_print", group.get("fine_print"))
            sub_q.setdefault("description", group.get("description"))
            sub_q.setdefault("resolution_criteria", group.get("resolution_criteria"))
            sub_post = dict(post)  # preserves _source_tournament_id via shallow copy
            sub_post["question"] = sub_q
            sub_posts.append(sub_post)
        return sub_posts

    _pre_unpack_count = len(raw_posts)
    _group_posts_found = sum(1 for p in raw_posts if p.get("group_of_questions"))
    if _group_posts_found:
        expanded = []
        for post in raw_posts:
            expanded.extend(_unpack_group_post(post))
        raw_posts = expanded
        print(f"  Unpacked {_group_posts_found} group_of_questions post(s) "
              f"({_pre_unpack_count} raw posts -> {len(raw_posts)} after "
              f"unpacking sub-questions)", flush=True)

    # Now that we know what's actually in THIS tournament, scope the dedup
    # set down to only question_ids that genuinely belong here — see the
    # FIXED comment above already_done_raw for why this matters.
    current_tournament_question_ids = {
        post["question"]["id"]
        for post in raw_posts
        if post.get("question") and post["question"].get("id") is not None
    }
    already_done = {
        qid: info for qid, info in already_done_raw.items()
        if qid in current_tournament_question_ids
    }
    print(f"Excluding {len(already_done)} already-forecast questions "
          f"(scoped to tournament {TOURNAMENT_IDS}; "
          f"{len(already_done_raw) - len(already_done)} other-tournament "
          f"records ignored)...", flush=True)

    questions = []
    closed_count = 0
    no_question_field_count = 0
    for post in raw_posts:
        q = post.get("question")
        if not q:
            no_question_field_count += 1
            continue
        if not _is_open_question(q):
            closed_count += 1
            continue
        questions.append(post)

    print(f"  Funnel: {len(raw_posts)} raw posts -> {no_question_field_count} missing question field, "
          f"{closed_count} already closed -> {len(questions)} still open", flush=True)

    # Convert to library question objects using from_metaculus_api_json
    from forecasting_tools import NumericQuestion, MultipleChoiceQuestion
    supported = []
    supported_post_ids = []  # parallel list, same index as `supported` —
    # needed for the CP fetch below, which is keyed by POST id (confirmed
    # via meta_debug_ids_probe.py — the singular detail endpoint is keyed
    # by post_id, NOT question_id; they are not interchangeable).
    unsupported_type_counts: dict[str, int] = {}
    dedup_skipped = 0
    parse_errors = 0
    for post in questions:
        q = post.get("question", {})
        q_type = q.get("type")
        try:
            if q_type == "binary":
                obj = BinaryQuestion.from_metaculus_api_json(post)
            elif q_type in ("numeric", "discrete"):
                obj = NumericQuestion.from_metaculus_api_json(post)
            elif q_type == "multiple_choice":
                obj = MultipleChoiceQuestion.from_metaculus_api_json(post)
            else:
                unsupported_type_counts[q_type] = unsupported_type_counts.get(q_type, 0) + 1
                continue
            if obj.id_of_question in already_done:
                prior = already_done[obj.id_of_question]
                stored_title = prior["title"]
                if titles_match(stored_title, obj.question_text):
                    # CHANGED (v2, 2026-07-11): replaced the generic
                    # 192h/8-day refresh gate for Market Pulse with a
                    # purpose-built "final hour" trigger, per Mike's
                    # request. Reasoning: check_subq_windows.py confirmed
                    # live (2026-07-11) that every Market Pulse
                    # sub-question's ENTIRE open lifespan is only
                    # 59-155 hours (2.5-6.5 days) — shorter than the old
                    # 192h gate, meaning that gate could structurally
                    # never fire a second forecast before a sub-question
                    # closed. Sub-questions close at the START of their
                    # own labeled period (locking in a forward-looking
                    # forecast before the observed window begins), so the
                    # most valuable moment for a second, fresher forecast
                    # is right before that lock — hence: refresh once
                    # more if we're within 60 minutes of close AND our
                    # last attempt happened before that final-hour window
                    # started. With the 30-min cron cadence, this
                    # reliably catches exactly one "final" refresh per
                    # sub-question (fires on whichever of the ~2 cron
                    # ticks inside that hour gets there first; the second
                    # tick sees submitted_at already inside the final
                    # hour and skips).
                    #
                    # FAILURE-RETRY FIX (2026-07-11, Mike's request): a
                    # FAILED attempt no longer blocks a retry at all —
                    # previously it got the exact same treatment as a
                    # success (stamped submitted_at, gated the same way),
                    # so a parse/submission failure could go unretried
                    # for the rest of the question's short open window.
                    # Now: status=="failed" always means "due", full
                    # stop, regardless of timing — the very next cron
                    # tick (≤30 min later) will retry it. This relies on
                    # already_done_raw now correctly reflecting the LATEST
                    # attempt per question_id (see the sort+overwrite fix
                    # above), not an arbitrary earlier one.
                    #
                    # FIXED (2026-07-15): this check previously lived
                    # INSIDE the is_market_pulse branch below, so it only
                    # ever actually applied to Market Pulse — despite the
                    # comment above claiming it applied universally. Every
                    # OTHER tournament (FutureEval included) fell straight
                    # into the plain else-branch permanent-duplicate skip,
                    # regardless of whether the prior attempt had actually
                    # succeeded. Confirmed via a real, live incident: post
                    # 44460 (Q44558, FutureEval) failed a real submission
                    # (Metaculus rejected an out-of-bounds MC probability —
                    # see parse_multiple_choice_response's own fix, same
                    # day), and the FAILED record then silently and
                    # permanently blocked every subsequent cron tick from
                    # ever retrying it — exactly the bug this comment
                    # already claimed was fixed, just never actually wired
                    # up outside Market Pulse. Moved out of the
                    # is_market_pulse branch so "failed always retries,
                    # no cooldown" now genuinely applies to every
                    # tournament, matching the stated intent above.
                    if prior.get("status") == "failed":
                        print(f"  🔁 Post {post.get('id')} (Q{obj.id_of_question}): last attempt failed — retrying "
                              f"(no cooldown applied to failures)")
                    else:
                        is_market_pulse = post.get("_source_tournament_id") == MARKET_PULSE_TOURNAMENT_ID
                        if is_market_pulse:
                            submitted_at_str = prior.get("submitted_at")
                            submitted_at = None
                            if submitted_at_str:
                                try:
                                    submitted_at = datetime.fromisoformat(submitted_at_str)
                                    if submitted_at.tzinfo is None:
                                        submitted_at = submitted_at.replace(tzinfo=timezone.utc)
                                except Exception:
                                    submitted_at = None  # unparseable — treat as due, same as no prior record

                            close_time = None
                            close_time_str = q.get("scheduled_close_time")
                            if close_time_str:
                                try:
                                    close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                                except Exception:
                                    close_time = None

                            within_final_hour = (
                                close_time is not None
                                and (close_time - now) <= timedelta(minutes=60)
                                and (close_time - now) > timedelta(0)  # not already past close
                            )
                            already_did_final_refresh = (
                                within_final_hour and submitted_at is not None
                                and submitted_at >= (close_time - timedelta(minutes=60))
                            )

                            # DEBUG print removed (2026-07-12) — mechanism fully
                            # validated across 3 simulated-time test runs
                            # (2026-07-11/12) against Q44679: not due before the
                            # window, fires correctly inside it, doesn't
                            # double-fire on a second check. The verbose
                            # per-question time-math line isn't needed for
                            # routine runs; --simulate-now is still available
                            # if this needs re-debugging later, just add the
                            # print back if so.

                            if not (within_final_hour and not already_did_final_refresh):
                                dedup_skipped += 1
                                continue  # not in the final hour yet, or already refreshed within it
                            print(f"  🔄 Post {post.get('id')} (Q{obj.id_of_question}): within final hour before close "
                                  f"({close_time_str}) — final refresh")
                        else:
                            dedup_skipped += 1
                            continue  # genuine duplicate — already forecast this question successfully
                else:
                    print(f"  🛑 Post {post.get('id')} (Q{obj.id_of_question}): ID was previously forecast under a different "
                          f"title — treating as a NEW question (ID likely recycled).")
                    print(f"       Previously: {stored_title[:90]}")
                    print(f"       Now:        {obj.question_text[:90]}")
            # Live CP, extracted from the post data we already fetched above
            # (no extra API call). The listing endpoint (api/posts/) never
            # carries real aggregation values — confirmed via
            # meta_debug_cp_probe.py, it returns the correct shape with
            # everything nulled out. This is left as a harmless no-op
            # placeholder (will set None) until the real fetch below runs.
            _set_cp(obj, extract_live_cp(post, q_type))
            supported.append(obj)
            supported_post_ids.append(post.get("id"))
        except Exception as e:
            parse_errors += 1
            print(f"  ⚠️  Could not parse post {post.get('id')} (Q{q.get('id')}): {e}")

    if TEST_QUESTION_ID_LIMIT is not None:
        # FIXED (2026-07-13): previously matched q.id_of_question ONLY.
        # WRONG ASSUMPTION, corrected by Mike directly: post_id and
        # question_id are NOT reliably equal even for standalone
        # (non-grouped) questions — almost every standalone question
        # we've actually seen has DIFFERENT post_id and question_id, this
        # isn't a group-only quirk. Mike's own words: "You should always
        # default to the post ID as that is what I can see on the
        # website." Matching on EITHER post_id or question_id now, so
        # this works with whatever's actually visible on the page
        # (standalone questions: the URL id, which is post_id) while
        # still allowing sub-question-level precision within a group
        # (Market Pulse: post_id alone would match ALL sub-questions in
        # that group, which is usually NOT what's wanted — question_id
        # narrows to one specific period).
        _before_limit = len(supported)
        _limited_pairs = [
            (q, pid) for q, pid in zip(supported, supported_post_ids)
            if pid in TEST_QUESTION_ID_LIMIT or q.id_of_question in TEST_QUESTION_ID_LIMIT
        ]
        supported = [q for q, _ in _limited_pairs]
        supported_post_ids = [pid for _, pid in _limited_pairs]
        print(f"  🧪 TEST_QUESTION_ID_LIMIT active: {_before_limit} -> {len(supported)} "
              f"question(s) (restricted to {sorted(TEST_QUESTION_ID_LIMIT)}, matched by "
              f"post_id or question_id)")

    # Real CP fetch — CONCURRENT, capped, via the SINGULAR /api2/questions/
    # {id}/ endpoint, keyed by post_id.
    #
    # REWRITTEN 2026-06-29: the previous version chunked through
    # api2/questions/?ids=, which meta_debug_ids_probe.py proved ignores
    # its filter entirely and returns unrelated recent questions regardless
    # of what's requested — so this was silently fetching nothing useful,
    # ever. The singular endpoint is proven correct (same probe), but it's
    # keyed by post_id, not question_id, which is why this now needs
    # supported_post_ids built above.
    #
    # CONCURRENT (not sequential like meta_batch_forecast.py's version) on
    # purpose: this function runs synchronously before forecasting even
    # starts, and some tournament questions are only open ~90 minutes —
    # ~200 sequential calls at ~1.2s apart would burn several minutes of
    # that window before a single forecast is made. Capped at
    # MAX_CONCURRENT_CP_FETCHES to stay polite to the API.
    #
    # NOTE: binary CP extraction via this endpoint is proven (see
    # meta_debug_ids_probe.py --single output, 2026-06-29). numeric/
    # multiple_choice extraction pipeline is confirmed to run end-to-end
    # without error (see live FutureEval Q44219, 2026-07-01), but has only
    # been tested against a question with NO community prediction present
    # (structurally null on FutureEval — see meta_dashboard.py's CP NOTE).
    # Whether extract_live_cp() correctly PARSES a populated multiple_choice
    # CP payload remains untested in practice, and given bots are excluded
    # from community aggregates on most tournaments, may rarely be
    # testable at all. Not treating this as a blocking open item.
    MAX_CONCURRENT_CP_FETCHES = 8
    if supported:
        import aiohttp
        headers_cp = {"Authorization": f"Token {TOURNAMENT_TOKEN}"}
        sem = asyncio.Semaphore(MAX_CONCURRENT_CP_FETCHES)
        cp_no_post_id = 0
        cp_errors = 0

        async def _fetch_one_cp(session, obj, post_id):
            nonlocal cp_no_post_id, cp_errors
            if post_id is None:
                cp_no_post_id += 1
                return
            q_type_for_target = (
                "binary" if isinstance(obj, BinaryQuestion)
                else "multiple_choice" if isinstance(obj, MultipleChoiceQuestion)
                else "numeric"
            )
            url = f"https://www.metaculus.com/api2/questions/{post_id}/"
            async with sem:
                for attempt in range(2):
                    try:
                        async with session.get(
                            url, headers=headers_cp,
                            timeout=aiohttp.ClientTimeout(total=15)
                        ) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(5)
                                continue
                            if resp.status != 200:
                                cp_errors += 1
                                return
                            data = await resp.json()
                            cp = extract_live_cp(data, q_type_for_target)
                            _set_cp(obj, cp)
                            return
                    except Exception:
                        cp_errors += 1
                        return

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                _fetch_one_cp(session, obj, post_id)
                for obj, post_id in zip(supported, supported_post_ids)
            ])

        print(f"  CP fetch (concurrent, capped at {MAX_CONCURRENT_CP_FETCHES}): "
              f"{len(supported) - cp_no_post_id - cp_errors} attempted cleanly, "
              f"{cp_no_post_id} had no post_id, {cp_errors} errored")

    cp_found_by_type: dict[str, list[int, int]] = {}
    for o in supported:
        t = type(o).__name__
        cp_found_by_type.setdefault(t, [0, 0])
        cp_found_by_type[t][1] += 1
        if getattr(o, "community_prediction_at_access_time", None) is not None:
            cp_found_by_type[t][0] += 1
    print(f"  Live CP found by type: "
          f"{ {t: f'{found}/{total}' for t, (found, total) in cp_found_by_type.items()} }")
    print(f"  Funnel: {len(questions)} open -> {dedup_skipped} already forecast, "
          f"{parse_errors} parse errors, {unsupported_type_counts or 'no'} unsupported types "
          f"-> {len(supported)} to forecast", flush=True)
    print(f"Open in tournament: {len(supported)} questions")
    print(f"New to forecast: {len(supported)}")
    return supported


# ─── Step 2a: Binary forecasting ──────────────────────────────────────────────
def build_binary_prompt(question: BinaryQuestion) -> str:
    return bf.build_user_prompt(question)


def parse_binary_response(text: str) -> float | None:
    for line in reversed(text.split('\n')):
        if 'probability:' in line.lower():
            numbers = re.findall(r'\d+\.?\d*', line)
            if numbers:
                prob = float(numbers[-1]) / 100
                return max(0.01, min(0.99, prob))
    return None


# ─── Step 2b: Numeric/discrete forecasting ────────────────────────────────────
def build_numeric_prompt(question: NumericQuestion) -> str:
    unit = question.unit_of_measure or ""
    lower = question.lower_bound
    upper = question.upper_bound
    open_lower = question.open_lower_bound
    open_upper = question.open_upper_bound

    bounds_desc = f"Range: {lower} to {upper} {unit}".strip()
    if open_lower:
        bounds_desc += " (lower bound is open — values below are possible)"
    if open_upper:
        bounds_desc += " (upper bound is open — values above are possible)"

    live_data = detect_data_needs(question.question_text)
    live_data_text = format_live_data_for_prompt(live_data)
    has_live_data = bool(live_data)

    # CHANGED (v2, 2026-07-10): OpenRouter-primary / Anthropic-fallback —
    # same opt-in every numeric question needs, since Market Pulse
    # sub-questions are ALL numeric and this call site previously had no
    # provider_order at all (silently defaulting to PROVIDER_ORDER_DEFAULT
    # = ["anthropic"] only, per meta_research.py). Mike's request
    # 2026-07-10: preserve Anthropic balance, OpenRouter has the bigger
    # funded pool. NOTE: _verify_research() inside research_question()
    # still always uses ANTHROPIC_API_KEY regardless of provider_order —
    # this reduces but does not zero out Anthropic spend on research.
    # CHANGED (2026-07-13, punch list #6): now captures return_source=True
    # and stores it via _set_research_source, so run() can log which
    # provider actually served this call.
    research_text, research_source = research_question(
        question.question_text, question.background_info or "",
        provider_order=["openrouter", "anthropic"],
        return_source=True,
    )
    _set_research_text(question, research_text)
    _set_research_source(question, research_source)
    has_research = research_text is not None
    research_block = (
        f"\nCURRENT RESEARCH (real-time web search, fetched for this question):\n{research_text}\n"
        if has_research else ""
    )
    has_real_grounding = has_live_data or has_research

    cp = getattr(question, "community_prediction_at_access_time", None)
    community = ""
    if cp is not None:
        if has_real_grounding:
            community = f"\nCurrent community median estimate: {cp:,.2f} {unit}. If your median differs substantially, explain why.\n"
        else:
            community = (
                f"\nCurrent community median estimate: {cp:,.2f} {unit}. "
                "IMPORTANT: you have no live data or research for this "
                "question — the community estimate reflects real people "
                "with access to current information you don't have. Stay "
                "reasonably close to it unless the background/resolution "
                "text above gives a specific, concrete reason to diverge.\n"
            )

    no_data_note = ""
    if not has_real_grounding:
        no_data_note = (
            "\nNOTE: No live market data and no research results were found "
            "for this question. You have no current information beyond the "
            "static text above.\n"
        )

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

{bounds_desc}
{live_data_text}
{research_block}
{no_data_note}
{community}
Today is {datetime.now().strftime("%Y-%m-%d")}.

This is a NUMERIC forecasting question. Reason through it carefully, then provide your estimate.

Before answering write:
(a) Time left until resolution
(b) Most likely outcome and why
(c) What would push the value lower?
(d) What would push the value higher?
(e) Base rate or historical reference values — only cite a specific
    precedent, named source, or past value if it appears word-for-word in
    the Background/Resolution criteria/Research above, otherwise say "No
    reliable base rate available" and proceed on priors.

Then end with exactly these three lines. Write each number IN FULL, matching
the scale of the range given above — do NOT abbreviate with "B", "M", "K",
"billion", "million", etc. (e.g. if the range above is in the billions,
write 89400000000, not 89.4 or 89.4B):
Low (10th percentile): <number>
Median (50th percentile): <number>
High (90th percentile): <number>
"""


def parse_numeric_response(text: str, question: NumericQuestion) -> list[float] | None:
    # FIXED (v2, 2026-07-10): was scanning forward and locking onto the
    # FIRST line containing 'low'+'10' / 'median'+'50' / 'high'+'90',
    # first-match-wins. Confirmed live across 8 Market Pulse rejections in
    # the same run (Q44689, Q44684, Q44676, Q44667, Q44669, Q44664,
    # Q44649, Q44651): in every single case the model's actual final
    # answer (the "Low (10th percentile): X" block build_numeric_prompt()
    # explicitly asks it to end with) was well-formed and correctly
    # ordered — but the forward scan grabbed an earlier, unrelated line
    # from the free-form reasoning that happened to contain the same
    # trigger substrings, e.g. "10th percentile (low outperformance): -6.0
    # pp (Apple outperforms by ~6%)" matched 'low'+'10' and returned the
    # LAST number on THAT line (6.0, from the trailing restatement), not
    # the real value (-6.0). Or worse: "The current environment shows low
    # volatility with zero days above 30 in the past month" matched
    # 'low'+'10' purely coincidentally (no percentile content at all) and
    # returned 30.0. Scanning in REVERSE — end of text first — means the
    # canonical final block (which the prompt guarantees comes last) is
    # found before any of these coincidental earlier mentions, with zero
    # other logic changes. Verified against all 8 saved reasoning texts
    # from the 2026-07-10 17:57 run: every one now parses to the model's
    # actual, correctly-ordered intended answer.
    low = median = high = None
    for line in reversed(text.split('\n')):
        l = line.lower()
        # Match numbers that may contain thousands-separator commas, e.g. "4,500,000"
        nums = re.findall(r'-?\d[\d,]*\.?\d*', line)
        nums = [float(n.replace(',', '')) for n in nums]
        if not nums:
            continue
        if 'low' in l and '10' in l and low is None:
            low = nums[-1]
        elif 'median' in l and '50' in l and median is None:
            median = nums[-1]
        elif 'high' in l and '90' in l and high is None:
            high = nums[-1]

    if None in (low, median, high):
        return None

    # Sanity check — catch parsing failures before they become a silent garbage submission.
    lower = question.lower_bound
    upper = question.upper_bound

    # FIXED 2026-07-01: previously rejected the entire forecast if the parsed
    # median fell outside the question's bounds, even by a tiny margin. Confirmed
    # live: the model forecast 298,000 for a question with lower_bound 300,000
    # (0.67% below) — a completely credible SPR value — and the hard reject threw
    # away the entire research+forecast call and submitted nothing to FutureEval.
    # Now clamps each percentile to [lower, upper] instead, with a warning so the
    # clamping is always visible in the log. Still hard-rejects if the median is
    # more than 20% outside bounds, since that's most likely a genuine unit error
    # (e.g. model answered in barrels when the question is in thousand-barrels)
    # rather than a rounding edge-case worth clamping.
    CLAMP_TOLERANCE = 0.20
    if median < lower * (1 - CLAMP_TOLERANCE) or median > upper * (1 + CLAMP_TOLERANCE):
        # RECOVERY (2026-07-15): before hard-rejecting, check whether this is
        # the "89.4B" shorthand bug — confirmed live on Q44826 (Market Pulse
        # revenue, bounds ~$87-92B): the model's raw answer ended "Median
        # (50th percentile): 89.4B", but the number-extraction regex above
        # only captures digits, so it silently dropped the "B" and passed
        # along 89.4 — 1000x too small. build_numeric_prompt() above now
        # tells the model not to abbreviate, but this is a defense-in-depth
        # net for whenever it still happens. Only ever activates on a value
        # that was ALREADY about to be rejected, and only ACCEPTS the
        # recovered value if applying a single common magnitude multiplier
        # (thousand/million/billion/trillion) to low, median, AND high
        # together lands the median back inside bounds — so this can't
        # silently corrupt an already-correct answer, only rescue one that
        # would otherwise have been thrown away entirely.
        for _mag_name, _mag in (("thousand", 1e3), ("million", 1e6), ("billion", 1e9), ("trillion", 1e12)):
            _low, _median, _high = low * _mag, median * _mag, high * _mag
            if lower * (1 - CLAMP_TOLERANCE) <= _median <= upper * (1 + CLAMP_TOLERANCE):
                print(f"  ℹ️  Recovered likely '{_mag_name}'-shorthand answer: "
                      f"median {median} -> {_median} (now within bounds "
                      f"[{lower}, {upper}])")
                low, median, high = _low, _median, _high
                break
        else:
            print(f"  ⚠️  Parsed median {median} is >20% outside question bounds "
                  f"[{lower}, {upper}] — likely a unit error, rejecting")
            return None
    if low < lower or high > upper or median < lower or median > upper:
        old_low, old_median, old_high = low, median, high
        low    = max(low, lower)
        median = max(min(median, upper), lower)
        high   = min(high, upper)
        print(f"  ℹ️  Clamped percentiles to question bounds [{lower}, {upper}]: "
              f"({old_low}, {old_median}, {old_high}) -> ({low}, {median}, {high})")

    if not (low <= median <= high):
        print(f"  ⚠️  Parsed percentiles not ordered after clamping "
              f"(low={low}, median={median}, high={high}) — rejecting")
        return None

    std = (high - low) / (2 * 1.2816)
    mean = median
    if std <= 0:
        std = abs(mean) * 0.1 + 0.01

    cdf_size = question.cdf_size

    # Set boundary values based on open/closed bounds
    start_val = 0.001 if question.open_lower_bound else 0.0
    end_val   = 0.999 if question.open_upper_bound else 1.0

    def normal_cdf(x, mu, sigma):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))

    # Build raw CDF scaled to [start_val, end_val]
    raw = []
    for i in range(cdf_size):
        x = lower + (upper - lower) * i / (cdf_size - 1)
        raw.append(normal_cdf(x, mean, std))

    # Rescale raw CDF to fit within [start_val, end_val]
    raw_min, raw_max = raw[0], raw[-1]
    if raw_max <= raw_min:
        raw_max = raw_min + 1e-6
    cdf = [start_val + (v - raw_min) / (raw_max - raw_min) * (end_val - start_val) for v in raw]

    # Enforce minimum step size (use a safe fraction of the available range)
    min_step = (end_val - start_val) / (cdf_size * 10)
    for i in range(1, len(cdf)):
        if cdf[i] - cdf[i-1] < min_step:
            cdf[i] = cdf[i-1] + min_step

    # Final rescale if min_step enforcement pushed us over end_val
    if cdf[-1] > end_val:
        scale = end_val / cdf[-1]
        cdf = [v * scale for v in cdf]
        cdf[0] = max(cdf[0], start_val)

    return cdf


# ─── Step 2c: Multiple choice forecasting ─────────────────────────────────────
def build_multiple_choice_prompt(question: MultipleChoiceQuestion) -> str:
    options_list = "\n".join(f"  - {opt}" for opt in question.options)

    # CHANGED (v2, 2026-07-10): same OpenRouter-primary opt-in as
    # build_numeric_prompt above, for consistency — Market Pulse doesn't
    # currently have multiple_choice sub-questions (all confirmed numeric),
    # but no reason to leave this call site on the old Anthropic-only
    # default while the numeric one right above it isn't.
    # CHANGED (2026-07-13, punch list #6): captures return_source=True too,
    # same as build_numeric_prompt above.
    research_text, research_source = research_question(
        question.question_text, question.background_info or "",
        provider_order=["openrouter", "anthropic"],
        return_source=True,
    )
    _set_research_text(question, research_text)
    _set_research_source(question, research_source)
    has_research = research_text is not None
    research_block = (
        f"\nCURRENT RESEARCH (real-time web search, fetched for this question):\n{research_text}\n"
        if has_research else ""
    )

    cp = getattr(question, "community_prediction_at_access_time", None)
    community = ""
    if isinstance(cp, dict) and cp:
        cp_lines = "\n".join(f"  {opt}: {p:.0%}" for opt, p in cp.items())
        if has_research:
            community = f"\nCurrent community probabilities:\n{cp_lines}\nIf your estimates differ substantially, explain why.\n"
        else:
            community = (
                f"\nCurrent community probabilities:\n{cp_lines}\n"
                "IMPORTANT: you have no research results for this question — "
                "the community estimate reflects real people with access to "
                "current information you don't have. Stay reasonably close "
                "to it unless the background/resolution text above gives a "
                "specific, concrete reason to diverge.\n"
            )

    no_data_note = ""
    if not has_research:
        no_data_note = (
            "\nNOTE: No research results were found for this question. You "
            "have no current information beyond the static text above.\n"
        )

    return f"""Question: {question.question_text}

Background:
{question.background_info or 'No background provided'}

Resolution criteria:
{question.resolution_criteria or 'No resolution criteria provided'}

{question.fine_print or ''}

Options:
{options_list}
{research_block}
{no_data_note}
{community}
Today is {datetime.now().strftime("%Y-%m-%d")}.

This is a MULTIPLE CHOICE forecasting question. Reason through each option carefully.

Before answering write:
(a) Time left until resolution
(b) Most likely outcome and why
(c) Key uncertainties that could change the outcome — only cite a specific
    precedent or named source if it appears word-for-word in the
    Background/Resolution criteria/Research above.

Then assign a probability to each option. Probabilities must sum to exactly 100%.
End with exactly this format (one line per option):
Option probabilities:
<option>: <number>%
<option>: <number>%
...
"""


def _normalize_option_text(s: str) -> str:
    """Normalize an option label (or Claude's generated option text) so
    symbolic and worded versions of the same comparison are recognized as
    equal — e.g. '≤45' and 'Less than or equal to 45' must match.

    This is exactly the bug that zeroed out a real FutureEval submission
    on 2026-06-29 (Q44216, 'How many mass shootings...'): the site's real
    option was 'Less than or equal to 45', Claude wrote '≤45', and neither
    exact-match nor substring-match recognized them as the same option —
    '≤45' literally never appears as a substring of the worded version,
    since the symbol itself isn't in that text at all."""
    s = s.strip().lower()
    # Order matters: handle two-character symbols before the single ones,
    # so "<=" doesn't get split into "<" + "=" first.
    replacements = [
        ("≤", " less than or equal to "),
        ("<=", " less than or equal to "),
        ("≥", " greater than or equal to "),
        (">=", " greater than or equal to "),
        ("<", " less than "),
        (">", " greater than "),
    ]
    for sym, word in replacements:
        s = s.replace(sym, word)
    s = re.sub(r"[^\w\s]", " ", s)  # drop remaining punctuation (commas, periods, etc.)
    s = re.sub(r"\s+", " ", s).strip()  # collapse whitespace from the replacements above
    return s


def parse_multiple_choice_response(text: str, question: MultipleChoiceQuestion) -> dict[str, float] | None:
    """Parse option probabilities from Claude's response."""
    # Find the "Option probabilities:" section
    lines = text.split('\n')
    start = None
    for i, line in enumerate(lines):
        if 'option probabilities' in line.lower():
            start = i + 1
            break

    if start is None:
        return None

    probs: dict[str, float] = {}
    unmatched_mass = 0.0
    unmatched_examples = []

    for line in lines[start:]:
        line = line.strip()
        if not line:
            continue
        # Match "option: number%"
        match = re.match(r'^(.+?):\s*(\d+\.?\d*)\s*%?$', line)
        if not match:
            continue
        option_text = match.group(1).strip()
        prob = float(match.group(2)) / 100

        # Exact match first (case-insensitive) — avoids substring ambiguity
        # entirely whenever Claude reproduces the option text verbatim.
        matched_opt = next(
            (opt for opt in question.options if opt.lower() == option_text.lower()), None
        )
        # Fall back to substring containment on the RAW text.
        if matched_opt is None:
            matched_opt = next(
                (opt for opt in question.options
                 if opt.lower() in option_text.lower() or option_text.lower() in opt.lower()),
                None
            )
        # Final fallback: normalize symbols to words on BOTH sides before
        # comparing — catches "≤45" vs "Less than or equal to 45" and
        # similar symbol/word mismatches that survive the raw checks above.
        if matched_opt is None:
            norm_option_text = _normalize_option_text(option_text)
            matched_opt = next(
                (opt for opt in question.options
                 if _normalize_option_text(opt) == norm_option_text), None
            )
            if matched_opt is None:
                matched_opt = next(
                    (opt for opt in question.options
                     if _normalize_option_text(opt) in norm_option_text
                     or norm_option_text in _normalize_option_text(opt)),
                    None
                )

        if matched_opt is not None:
            probs[matched_opt] = probs.get(matched_opt, 0.0) + prob
        else:
            unmatched_mass += prob
            unmatched_examples.append(option_text)

    if not probs:
        return None

    # Don't silently drop meaningful unmatched probability mass — that's
    # exactly how a real option's share used to vanish after normalization.
    # Reject loudly instead, same philosophy as the numeric sanity check.
    if unmatched_mass > 0.05:
        print(f"  ⚠️  {unmatched_mass:.0%} of probability mass couldn't be matched to a known "
              f"option (unrecognized: {unmatched_examples}) — rejecting forecast")
        return None

    # Fill any options Claude didn't mention with 0, then normalize to 1.0
    for opt in question.options:
        probs.setdefault(opt, 0.0)
    total = sum(probs.values())
    if total <= 0:
        return None
    normalized = {opt: probs[opt] / total for opt in question.options}

    # FIXED (2026-07-15): clamp each option into Metaculus's required
    # [0.001, 0.999] range before returning. Confirmed via a real
    # submission failure (post 44460, Q44558, "How many Level 4 travel
    # advisories...") — Metaculus's API rejects ANY option at exactly 0.0
    # or 1.0 with HTTP 400 "Probabilities for current options must be
    # between 0.001 and 0.999". Because this is a deterministic
    # validation error (the computed probabilities never change), the
    # existing 3-retry submission logic couldn't help at all — it just
    # retried the exact same rejected values 3 times and gave up. Most
    # common trigger: an option Claude never mentions gets defaulted to
    # 0.0 just above (setdefault), or one very-dominant option normalizes
    # to ~1.0 while the rest round down near 0.
    #
    # A SINGLE clamp-then-renormalize pass is NOT sufficient — confirmed
    # by testing against the actual failure case: renormalizing after
    # clamping can push a value that was exactly at the 0.001 floor back
    # slightly BELOW it again (e.g. 0.001 / 1.002 = 0.000998...), which
    # would still fail the same validation. This iterates clamp+renormalize
    # until every value is genuinely within bounds — converges in a
    # handful of iterations for any realistic option count, with a hard
    # cap as a safety valve against a pathological case looping forever.
    clamped = dict(normalized)
    for _ in range(10):
        clamped = {opt: min(max(v, 0.001), 0.999) for opt, v in clamped.items()}
        clamped_total = sum(clamped.values())
        if clamped_total <= 0:
            break
        clamped = {opt: v / clamped_total for opt, v in clamped.items()}
        if all(0.001 <= v <= 0.999 for v in clamped.values()):
            break
    return clamped


# ─── Step 3: Forecast a single question ───────────────────────────────────────
def forecast_question(question) -> tuple[str, any, str | None]:
    """
    Returns (question_type, forecast_value, raw_reasoning_text) where
    forecast_value is:
      - float for binary (probability)
      - list[float] for numeric (201-point CDF)
      - dict[str, float] for multiple_choice
      - None on failure
    raw_reasoning_text is the full model output, for logging/audit — None
    only if the API call itself failed (no response at all).
    """
    system_prompt = build_forecaster_system_prompt()

    if isinstance(question, BinaryQuestion):
        user_prompt = build_binary_prompt(question)
        q_type = "binary"
    elif isinstance(question, NumericQuestion):
        user_prompt = build_numeric_prompt(question)
        q_type = "numeric"
    elif isinstance(question, MultipleChoiceQuestion):
        user_prompt = build_multiple_choice_prompt(question)
        q_type = "multiple_choice"
    else:
        return "unsupported", None, None

    try:
        response = client_anthropic.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=cacheable_system_block(system_prompt),
            messages=[{"role": "user", "content": user_prompt}]
        )
        text = response.content[0].text

        # Visibility for the prompt-caching change above: this is the only
        # way to actually confirm caching is working rather than assuming
        # it (Haiku 4.5's 4,096-token minimum cacheable prefix means a
        # too-short system prompt would silently cache nothing, with no
        # error). cache_read > 0 means a previous call's cache was reused
        # (cheap); cache_creation > 0 with cache_read == 0 means this was
        # the first call writing a fresh cache entry (slightly more
        # expensive than uncached, recovered by the next cached read).
        # Both staying at 0 across an entire run means caching isn't
        # engaging at all — check system prompt length if that's the case.
        usage = getattr(response, "usage", None)
        if usage is not None:
            cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
            cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
            if cache_read or cache_write:
                print(f"    💰 cache: {cache_read} read, {cache_write} written")

        if q_type == "binary":
            result = parse_binary_response(text)
        elif q_type == "numeric":
            result = parse_numeric_response(text, question)
        elif q_type == "multiple_choice":
            result = parse_multiple_choice_response(text, question)
        else:
            result = None

        if result is None:
            print(f"  ⚠️  Could not parse {q_type} response for post "
                  f"{getattr(question, 'id_of_post', None)} (Q{question.id_of_question})")
        return q_type, result, text

    except Exception as e:
        print(f"  ❌ Claude error for post {getattr(question, 'id_of_post', None)} "
              f"(Q{question.id_of_question}): {e}")
        return q_type, None, None


# ─── Step 4: Submit to Metaculus ───────────────────────────────────────────────
def submit_forecast(question, q_type: str, forecast) -> bool:
    try:
        if q_type == "binary":
            client_metaculus.post_binary_question_prediction(
                question_id=question.id_of_question,
                prediction_in_decimal=forecast
            )
            print(f"  ✅ Binary: {forecast:.0%}")

        elif q_type == "numeric":
            client_metaculus.post_numeric_question_prediction(
                question_id=question.id_of_question,
                cdf_values=forecast
            )
            # Find approximate median for display
            median_idx = next((i for i, v in enumerate(forecast) if v >= 0.5), len(forecast)//2)
            median_val = question.lower_bound + (question.upper_bound - question.lower_bound) * median_idx / (len(forecast) - 1)
            print(f"  ✅ Numeric: median≈{median_val:.2f} {question.unit_of_measure or ''}")

        elif q_type == "multiple_choice":
            client_metaculus.post_multiple_choice_question_prediction(
                question_id=question.id_of_question,
                options_with_probabilities=forecast
            )
            top = max(forecast, key=forecast.get)
            print(f"  ✅ Multiple choice: top option='{top}' ({forecast[top]:.0%})")

        return True

    except Exception as e:
        print(f"  ❌ Submission error for post {getattr(question, 'id_of_post', None)} "
              f"(Q{question.id_of_question}): {str(e)[:80]}")
        return False


# ─── Step 5: Main run loop ─────────────────────────────────────────────────────
def summarize_forecast_for_alert(q_type: str, forecast) -> str:
    """Short human-readable summary for the push notification."""
    try:
        if q_type == "binary":
            return f"{forecast:.0%}"
        if q_type == "numeric":
            median_idx = next((i for i, v in enumerate(forecast) if v >= 0.5), len(forecast) // 2)
            return f"~{median_idx / (len(forecast) - 1):.0%} through range"
        if q_type == "multiple_choice":
            top = max(forecast, key=forecast.get)
            return f"{top} ({forecast[top]:.0%})"
    except Exception:
        pass
    return str(forecast)[:60]


async def run(dry_run: bool = False, simulate_now: datetime | None = None):
    mode_label = "DRY RUN — no Claude calls, no Metaculus submissions" if dry_run else "LIVE"
    print(f"METACULUS TOURNAMENT FORECASTER (synchronous) — v2 DEV BRANCH [{mode_label}]")
    print("(Market Pulse refresh: final-hour-before-close trigger, no generic time gate)")
    print("=" * 50)

    questions = await fetch_tournament_questions(simulate_now=simulate_now, dry_run=dry_run)
    if not questions:
        print("No questions found — either none are open right now, or all already forecast.")
        return

    if dry_run:
        # Matches this codebase's existing dry-run convention (see
        # meta_refresh_forecast.py's main()): show what WOULD be forecast
        # and an estimated cost, without spending anything. fetch_tournament_
        # questions() above already made real calls, but only to Metaculus
        # (free) — no Claude/OpenRouter calls happen until forecast_question()
        # below, which this branch deliberately never reaches. Nothing is
        # written to TOURNAMENT_BATCH_DIR in dry-run mode either — there's
        # no real forecast to record, so no dedup/refresh-gate state should
        # be created from a dry run.
        print(f"\n{'='*50}")
        print(f"Would forecast {len(questions)} question(s):")
        for q in questions:
            print(f"  Post {getattr(q, 'id_of_post', None)} (Q{q.id_of_question}, {type(q).__name__}): {q.question_text[:80]}")
        # Rough per-instance cost from the 2026-07-10 costing exercise:
        # ~$0.025-0.03 covering 1 research call (web search + tokens) + 1
        # forecast call (Haiku, system prompt cached where it clears the
        # 4,096-token floor). This is an ESTIMATE, not a live measurement —
        # actual cost varies with search-result length and whether OpenRouter
        # vs Anthropic serves the research call.
        low_est  = len(questions) * 0.025
        high_est = len(questions) * 0.03
        print(f"\nEstimated cost: ~${low_est:.2f}-${high_est:.2f} "
              f"({len(questions)} questions x ~$0.025-0.03/question — "
              f"1 research call + 1 forecast call each)")
        print(f"\nRun without --dry-run to actually forecast and submit.")
        return

    results   = {}
    submitted = 0
    failed    = 0
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    # NEW (v2, 2026-07-11): buffer successful-submission alerts by post_id
    # instead of firing send_alert() immediately per question. All
    # sub-questions in a group_of_questions post share one post_id (see
    # 2026-07-10 notes — confirmed live, e.g. the whole VIX biweekly group
    # is post_id 44534 regardless of which of its 6 sub-questions), so
    # this collapses "one alert per sub-question" into "one alert per
    # group" — with ZERO special-casing for Market Pulse specifically: a
    # standalone, non-grouped question just ends up as a "group" of one,
    # so every other tournament's alert behavior is unchanged.
    #
    # CHANGED (2026-07-11, Mike's request): flush each group's alert as
    # soon as that group's LAST member for this run finishes processing,
    # rather than waiting for the whole run to end — a slow run (42+
    # questions) shouldn't hold every earlier group's notification
    # hostage until the very last question completes. Membership counts
    # are computed up front from the actual `questions` list this run is
    # about to process (i.e. AFTER dedup/refresh-gate/test-limit
    # filtering already ran) — so a group where some sub-questions were
    # already excluded (already forecast, refresh not due yet, etc.)
    # still flushes correctly once its REMAINING members are done,
    # without needing every original group member to be present.
    from collections import Counter
    post_id_remaining = Counter(
        getattr(q, "id_of_post", None) or q.id_of_question for q in questions
    )
    group_alert_buffer: dict[int, dict] = {}

    def _flush_group_alert(post_id):
        group = group_alert_buffer.pop(post_id, None)
        if not group or not group["lines"]:
            return  # nothing succeeded in this group — nothing to alert on
        n = len(group["lines"])
        if n == 1:
            send_alert(group["lines"][0], title="New forecast submitted (tournament)")
        else:
            body = "\n\n".join(group["lines"])
            send_alert(
                f"{n} forecasts submitted:\n\n{body}",
                title=f"{n} forecasts: {group['title'][:60]}"
            )

    # FIXED 2026-06-30: previously only written once, at the very end —
    # confirmed live: a run was interrupted (terminal closed, exact cause
    # unknown) partway through 66 questions; some had already been
    # genuinely submitted to Metaculus (post_binary_question_prediction
    # happens inside this loop, well before any save), but since nothing
    # was written to disk until the end, the LOCAL dedup record lost all
    # of that — the next run would have no way to know those questions
    # were already done and would harmlessly-but-wastefully redo them.
    # Now written after EVERY question, so an interruption only loses
    # progress on whichever single question was in flight at the time.
    results_file = os.path.join(TOURNAMENT_BATCH_DIR, f"batch_results_{timestamp}.json")

    def _save_results_so_far():
        with open(results_file, 'w', newline='\n') as f:
            json.dump(results, f, indent=2)

    for i, q in enumerate(questions, 1):
        print(f"\n[{i}/{len(questions)}] Post {getattr(q, 'id_of_post', None)} "
              f"(Q{q.id_of_question}, {type(q).__name__}): {q.question_text[:70]}")

        q_type, forecast, reasoning_text = forecast_question(q)
        cp = getattr(q, "community_prediction_at_access_time", None)
        # Punch list #6 (2026-07-13): which provider (openrouter/anthropic/
        # None) actually served the research call for this question — set
        # via _set_research_source() inside build_numeric_prompt()/
        # build_multiple_choice_prompt(), OR (2026-07-13, gap closed)
        # already set by bf.build_user_prompt() for binary questions —
        # confirmed by reading meta_batch_forecast.py directly: it uses
        # the exact same attribute name (research_source_at_access_time),
        # so this one getattr covers all three question types with no
        # further changes needed to the shared production file.
        research_source = getattr(q, "research_source_at_access_time", None)
        # Companion fix, same pass: research_text itself (not just which
        # provider served it) was never stored either — see
        # _set_research_text's docstring above.
        research_text_value = getattr(q, "research_text_at_access_time", None)

        if forecast is None:
            results[f"q_{q.id_of_question}"] = {
                "question_id":   q.id_of_question,
                "post_id":       getattr(q, "id_of_post", None),
                "question_text": q.question_text,
                "question_type": q_type,
                "status":        "failed",
                "submitted_forecast": None,
                "reasoning":     reasoning_text,
                "community_prediction": cp,
                "research_source": research_source,
                "research_text": research_text_value,
                # NEW (v2): per-question timestamp (not a shared batch-level
                # one — this file is synchronous, one question at a time, so
                # this is a genuine per-question "when did we last touch
                # this" record). Stamped even on failure, for audit ("when
                # did we last attempt this") — but as of 2026-07-11, status
                # now DOES matter for Market Pulse gating: a "failed" status
                # here means the dedup block above will treat this question
                # as immediately due again on the very next run, regardless
                # of this timestamp. See the failure-retry fix in the dedup
                # block for the full reasoning.
                # FIXED (2026-07-12): uses simulate_now when provided, not
                # always real wall-clock time — otherwise a --simulate-now
                # test run would stamp with REAL time while the final-hour
                # comparison (in fetch_tournament_questions, above) runs on
                # the SIMULATED clock, making already_did_final_refresh
                # checks on a subsequent test run unreliable (confirmed
                # live 2026-07-12: real time was still Jul 12 while the
                # simulated close-time math was set to Jul 13, so a
                # freshly-stamped real submitted_at would look "before"
                # the simulated final-hour window on the next check).
                "submitted_at": (simulate_now or datetime.now(timezone.utc)).isoformat(),
            }
            failed += 1
            _save_results_so_far()
            # NEW (v2, 2026-07-11): this question's group membership is
            # "done" (with a failure) too — decrement and flush if it was
            # the last outstanding member, same as the success/failure
            # path below.
            _pid = getattr(q, "id_of_post", None) or q.id_of_question
            post_id_remaining[_pid] -= 1
            if post_id_remaining[_pid] <= 0:
                _flush_group_alert(_pid)
            continue

        submission_ok = submit_forecast(q, q_type, forecast)

        results[f"q_{q.id_of_question}"] = {
            "question_id":   q.id_of_question,
            "post_id":       getattr(q, "id_of_post", None),
            "question_text": q.question_text,
            "question_type": q_type,
            # Reflects whether the forecast actually reached Metaculus, not
            # just whether Claude produced one — a question that's closed or
            # otherwise rejected at submission must not be logged as a
            # success, since this data feeds calibration tracking.
            "status":        "success" if submission_ok else "failed",
            # Audit trail: the actual value submitted to Metaculus, so a
            # mismatch between what got logged here and what the question
            # page displays can be checked without guessing. Shape depends
            # on q_type: float for binary, list[float] (CDF) for numeric,
            # dict[option, float] for multiple_choice.
            "submitted_forecast": forecast,
            # Full reasoning + the live CP this forecast was (or wasn't)
            # anchored against — without these, there was no way to audit
            # *why* a live tournament forecast was made, for any question
            # type. This was the single biggest gap before today's update.
            "reasoning":     reasoning_text,
            "community_prediction": cp,
            "research_source": research_source,
            "research_text": research_text_value,
            # NEW (v2): see docstring on the failed-branch record above —
            # same reasoning, same field.
            "submitted_at": (simulate_now or datetime.now(timezone.utc)).isoformat(),
        }

        if submission_ok:
            submitted += 1
            alert_summary = summarize_forecast_for_alert(q_type, forecast)
            post_id = getattr(q, "id_of_post", None) or q.id_of_question  # fall back to
            # question's own id if id_of_post is somehow missing, so this never crashes —
            # worst case it just doesn't group with siblings, same as today's per-question behavior.
            # Strip a trailing "(...)" date-range suffix (e.g. "(Jul 13 - Jul 24)")
            # to get the shared group name common to every sub-question in
            # this post — falls back to the full question text if there's no
            # such suffix (i.e. a normal, non-grouped question).
            common_title = re.sub(r"\s*\([^)]*\)\s*$", "", q.question_text).strip() or q.question_text
            group = group_alert_buffer.setdefault(post_id, {"title": common_title, "lines": []})
            group["lines"].append(f"Post {post_id} (Q{q.id_of_question}): {alert_summary}\n{q.question_text[:100]}")
        else:
            failed += 1
            post_id = getattr(q, "id_of_post", None) or q.id_of_question

        # NEW (v2, 2026-07-11): this group member is done (success or
        # failure either way) — decrement its group's countdown and flush
        # immediately once the LAST outstanding member of that group (for
        # THIS run's queue, post-filtering) finishes, rather than waiting
        # for the whole run to end.
        post_id_remaining[post_id] -= 1
        if post_id_remaining[post_id] <= 0:
            _flush_group_alert(post_id)

        _save_results_so_far()
        await asyncio.sleep(0.5)

    # Safety net only — every group should already have been flushed above
    # the moment its last member finished. Anything left here means the
    # post_id_remaining count didn't match reality for some reason (e.g. an
    # exception skipped a decrement) — flush it now rather than silently
    # dropping the notification.
    for post_id in list(group_alert_buffer.keys()):
        _flush_group_alert(post_id)

    print(f"\n{'=' * 50}")
    print(f"Submitted: {submitted} | Failed: {failed}")
    print(f"Results saved to {results_file}")


if __name__ == "__main__":
    import sys
    _simulate_now = None
    for _arg in sys.argv:
        if _arg.startswith("--simulate-now="):
            _iso = _arg.split("=", 1)[1]
            try:
                _simulate_now = datetime.fromisoformat(_iso.replace("Z", "+00:00"))
                if _simulate_now.tzinfo is None:
                    _simulate_now = _simulate_now.replace(tzinfo=timezone.utc)
            except Exception as _e:
                raise SystemExit(f"Could not parse --simulate-now value {_iso!r}: {_e}\n"
                                  f"Expected ISO format, e.g. --simulate-now=2026-07-13T02:30:00Z")
        elif _arg.startswith("--ids="):
            # ADDED 2026-07-14 (dashboard manual-selection UI): promotes
            # TEST_QUESTION_ID_LIMIT from an edit-the-file-and-redeploy
            # constant to a real runtime flag. Same dual post_id-or-
            # question_id matching as the constant always used (see that
            # matching logic's own comment for why — post_id for
            # standalone questions since that's what's visible on the
            # site and what the dashboard's checkboxes naturally carry,
            # question_id for precision within a group's sub-questions).
            # This is what the dashboard's "Refresh Selected" action will
            # actually invoke as a background subprocess — see
            # meta_dashboard.py's /refresh route.
            _ids_str = _arg.split("=", 1)[1]
            try:
                TEST_QUESTION_ID_LIMIT = {int(_id.strip()) for _id in _ids_str.split(",") if _id.strip()}
            except ValueError as _e:
                raise SystemExit(f"Could not parse --ids value {_ids_str!r} — expected comma-separated "
                                  f"integers, e.g. --ids=44457,44679: {_e}")
            if not TEST_QUESTION_ID_LIMIT:
                raise SystemExit("--ids was given but parsed to an empty set — nothing to do.")
    asyncio.run(run(dry_run="--dry-run" in sys.argv, simulate_now=_simulate_now))