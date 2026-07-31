"""
bybit_coin_bench.py

Auto-benching module for the Bybit sim. Tracks per-coin recent performance
and automatically benches (excludes from new entries) coins that are
underperforming in the *current* market environment, then automatically
retests them after a cooldown period.

Design notes:
- Benching only affects NEW entries (the scan/signal stage). It does NOT
  touch currently open positions - those are tracked via state.json and
  continue to be monitored/closed normally regardless of bench status.
  (Confirmed empirically: ENA/NEAR/HYPE/ALGO continued to close out fine
  after being manually removed from WATCHLIST while a position was open.)
- All 4 tunable parameters live in bench_config.json, NOT hardcoded here,
  so the dashboard can expose them as editable input fields. This module
  always reads the config fresh (no caching) so dashboard edits take
  effect on the next scan cycle without restarting the bot.
- benched_coins.json is the persistent state of what's currently benched
  and when each bench expires (auto-unbench/retest).
- bench_history.json tracks the last time each coin was unbenched. This
  is the fix for the "coins never come back off the bench" bug: a coin
  can't be judged on a re-bench window made up of the exact same trades
  that put it on the bench in the first place. Once a coin is unbenched
  (cooldown expiry or manual override), it needs `window_trades` worth
  of *fresh* closed trades - trades closed strictly after that unbench
  timestamp - before it's eligible to be re-benched again. Until then,
  run_bench_check() will leave it active even if its lifetime window
  still looks bad, because that window is stale (pre-unbench) data.

Integrate into bybit_sim.py:

    from bybit_coin_bench import get_active_watchlist, run_bench_check

    # Once per scan cycle, before building this cycle's scan list:
    run_bench_check(CLOSED_TRADES_CSV_PATH)
    active_watchlist = get_active_watchlist(WATCHLIST)

    # Use active_watchlist (not WATCHLIST) when looping to generate new
    # entry signals. Existing position management loops should keep
    # iterating over state.json positions as before - untouched.
"""

import json
import csv
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Paths - relative path matches bybit_sim.py's own DATA_DIR convention, so
# this resolves correctly as long as it's run from the same working
# directory (C:\Users\Mike\metac-bot-template\), same as bybit_sim.py.
# ---------------------------------------------------------------------------
DATA_DIR = "bybit_sim_data"
CONFIG_PATH = os.path.join(DATA_DIR, "bench_config.json")
BENCHED_PATH = os.path.join(DATA_DIR, "benched_coins.json")
BENCH_HISTORY_PATH = os.path.join(DATA_DIR, "bench_history.json")
BENCH_LOG_PATH = os.path.join(DATA_DIR, "bench_log.csv")

DEFAULT_CONFIG = {
    "window_trades": 4,           # how many of the coin's most recent closed trades to evaluate
    "min_trades_before_active": 7,  # coin needs at least this many total closed trades before bench logic applies
    "win_rate_threshold_pct": 20.0,  # bench if win rate over the window is below this
    "cooldown_days": 7            # days a coin stays benched before being auto-retested
}

DATE_FORMATS = ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S")


def _parse_dt(date_str, time_str):
    combined = f"{date_str} {time_str}"
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(combined, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date/time format: {combined!r}")


# ---------------------------------------------------------------------------
# Config (4 tunable variables - dashboard-editable)
# ---------------------------------------------------------------------------

def load_config():
    """Always reads fresh from disk so dashboard edits apply on next cycle."""
    if not os.path.exists(CONFIG_PATH):
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)
    # backfill any missing keys with defaults (forward-compatible if we add params later)
    merged = dict(DEFAULT_CONFIG)
    merged.update(cfg)
    return merged


def save_config(cfg):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_PATH, "w", newline='\n') as f:
        json.dump(cfg, f, indent=2)


# ---------------------------------------------------------------------------
# Benched coins state
# ---------------------------------------------------------------------------

def load_benched():
    if not os.path.exists(BENCHED_PATH):
        return {}
    with open(BENCHED_PATH, "r") as f:
        return json.load(f)


def save_benched(benched):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(BENCHED_PATH, "w", newline='\n') as f:
        json.dump(benched, f, indent=2)


# ---------------------------------------------------------------------------
# Bench history - last unbench timestamp per symbol. Persists across
# bench/unbench cycles (unlike benched_coins.json, which only holds
# CURRENTLY benched coins). Used to gate re-bench eligibility on fresh
# post-unbench trade data.
# ---------------------------------------------------------------------------

def load_bench_history():
    if not os.path.exists(BENCH_HISTORY_PATH):
        return {}
    with open(BENCH_HISTORY_PATH, "r") as f:
        return json.load(f)


def save_bench_history(history):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(BENCH_HISTORY_PATH, "w", newline='\n') as f:
        json.dump(history, f, indent=2)


def _record_unbench_history(symbol, when):
    history = load_bench_history()
    history[symbol] = when.isoformat(timespec="seconds")
    save_bench_history(history)


def is_benched(symbol):
    """Check bench status, auto-expiring if cooldown has passed."""
    benched = load_benched()
    entry = benched.get(symbol)
    if entry is None:
        return False
    unbench_at = datetime.fromisoformat(entry["unbench_at"])
    if datetime.now() >= unbench_at:
        # Cooldown expired - auto-unbench right here so callers always
        # see a consistent view without needing run_bench_check() first.
        _unbench(symbol, reason="cooldown expired (auto-retest)")
        return False
    return True


def get_active_watchlist(full_watchlist):
    """Returns the watchlist with currently-benched symbols filtered out.
    Use this for NEW entry scanning only - existing open positions are
    unaffected and keep being monitored independently of this list."""
    return [s for s in full_watchlist if not is_benched(s)]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _log_event(event_type, symbol, detail):
    os.makedirs(DATA_DIR, exist_ok=True)
    file_exists = os.path.exists(BENCH_LOG_PATH)
    with open(BENCH_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "event", "symbol", "detail"])
        writer.writerow([datetime.now().isoformat(timespec="seconds"), event_type, symbol, detail])


def _bench(symbol, win_rate_pct, window_trades, cooldown_days):
    benched = load_benched()
    now = datetime.now()
    benched[symbol] = {
        "benched_at": now.isoformat(timespec="seconds"),
        "unbench_at": (now + timedelta(days=cooldown_days)).isoformat(timespec="seconds"),
        "reason": f"{win_rate_pct:.1f}% win rate over last {window_trades} trades"
    }
    save_benched(benched)
    _log_event("BENCH", symbol, f"win_rate={win_rate_pct:.1f}% window={window_trades} cooldown_days={cooldown_days}")


def _unbench(symbol, reason="manual"):
    benched = load_benched()
    if symbol in benched:
        del benched[symbol]
        save_benched(benched)
        now = datetime.now()
        _record_unbench_history(symbol, now)
        _log_event("UNBENCH", symbol, reason)


def manually_unbench(symbol):
    """Escape hatch if you want to override and re-enable a coin early.
    Also resets the fresh-trade clock, same as an automatic unbench - the
    coin gets a genuine new window before it can be re-benched."""
    _unbench(symbol, reason="manual override")


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def _read_closed_trades(closed_trades_csv_path):
    with open(closed_trades_csv_path, "r", newline="") as f:
        return list(csv.DictReader(f))


def _recent_trades(trades, window_trades):
    """Returns the most recent `window_trades` trades from the given list,
    sorted chronologically, using whichever date format each row has."""
    ordered = sorted(trades, key=lambda c: _parse_dt(c["close_date"], c["close_time"]))
    return ordered[-window_trades:]


def evaluate_coin(symbol, all_closed, config=None):
    """Evaluate a single coin against current config. Returns a dict with
    the stats used, regardless of whether action was taken - useful for
    a dashboard preview/debug view.

    Re-bench eligibility is gated on FRESH trades only: if this coin has
    been unbenched before, only trades closed after that unbench count
    toward the window. This stops a coin from being instantly re-benched
    off the back of the same losing trades that benched it last time,
    before it's had any chance to trade again.
    """
    cfg = config or load_config()
    all_for_symbol = [c for c in all_closed if c["symbol"] == symbol]
    total_trades = len(all_for_symbol)

    history = load_bench_history()
    last_unbenched_at_str = history.get(symbol)
    last_unbenched_at = datetime.fromisoformat(last_unbenched_at_str) if last_unbenched_at_str else None

    if last_unbenched_at is not None:
        eligible_pool = [
            c for c in all_for_symbol
            if _parse_dt(c["close_date"], c["close_time"]) > last_unbenched_at
        ]
    else:
        eligible_pool = all_for_symbol

    has_min_history = total_trades >= cfg["min_trades_before_active"]
    has_fresh_window = len(eligible_pool) >= cfg["window_trades"]

    result = {
        "symbol": symbol,
        "total_trades": total_trades,
        "eligible": has_min_history and has_fresh_window,
        "window_trades": cfg["window_trades"],
        "window_wins": None,
        "window_win_rate_pct": None,
        "currently_benched": is_benched(symbol),
        "last_unbenched_at": last_unbenched_at_str,
        "fresh_trades_since_unbench": len(eligible_pool) if last_unbenched_at is not None else None,
        "awaiting_fresh_data": last_unbenched_at is not None and not has_fresh_window,
    }

    if not result["eligible"]:
        return result

    recent = _recent_trades(eligible_pool, cfg["window_trades"])
    wins = sum(1 for c in recent if c["outcome"] == "WIN")
    win_rate = (wins / len(recent) * 100) if recent else None
    result["window_wins"] = wins
    result["window_win_rate_pct"] = win_rate
    return result


def run_bench_check(closed_trades_csv_path):
    """Call once per scan cycle. Evaluates every symbol that appears in
    closed_trades.csv, benching/unbenching as needed per current config.
    Returns a list of evaluation dicts (one per symbol seen) for logging
    or dashboard display."""
    cfg = load_config()
    all_closed = _read_closed_trades(closed_trades_csv_path)
    symbols = sorted(set(c["symbol"] for c in all_closed))

    results = []
    for symbol in symbols:
        res = evaluate_coin(symbol, all_closed, cfg)
        results.append(res)

        if not res["eligible"]:
            continue

        already_benched = is_benched(symbol)  # also auto-expires cooldowns
        win_rate = res["window_win_rate_pct"]

        if win_rate is not None and win_rate < cfg["win_rate_threshold_pct"] and not already_benched:
            _bench(symbol, win_rate, cfg["window_trades"], cfg["cooldown_days"])
        # Note: we don't re-bench an already-benched coin even if it's
        # still underperforming - it stays benched until its cooldown
        # naturally expires, at which point it needs a fresh window of
        # post-unbench trades (not the same stale ones) to prove itself
        # before it can be benched again.

    return results


if __name__ == "__main__":
    # Quick manual check from the command line:
    #   python bybit_coin_bench.py <path_to_closed_trades.csv>
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(DATA_DIR, "closed_trades.csv")
    results = run_bench_check(path)
    print(f"{'SYMBOL':<12}{'TRADES':<8}{'ELIGIBLE':<10}{'WINS':<6}{'WR%':<8}{'BENCHED':<9}{'AWAITING':<10}")
    for r in results:
        wr_str = f"{r['window_win_rate_pct']:.1f}" if r['window_win_rate_pct'] is not None else "-"
        print(f"{r['symbol']:<12}{r['total_trades']:<8}{str(r['eligible']):<10}"
              f"{str(r['window_wins']):<6}{wr_str:<8}{str(r['currently_benched']):<9}"
              f"{str(r['awaiting_fresh_data']):<10}")
    print(f"\nCurrently benched: {load_benched()}")