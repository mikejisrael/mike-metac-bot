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
# directory (C:\Users\mikej\metac-bot-template\), same as bybit_sim.py.
# ---------------------------------------------------------------------------
DATA_DIR = "bybit_sim_data"
CONFIG_PATH = os.path.join(DATA_DIR, "bench_config.json")
BENCHED_PATH = os.path.join(DATA_DIR, "benched_coins.json")
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
        _log_event("UNBENCH", symbol, reason)


def manually_unbench(symbol):
    """Escape hatch if you want to override and re-enable a coin early."""
    _unbench(symbol, reason="manual override")


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def _read_closed_trades(closed_trades_csv_path):
    with open(closed_trades_csv_path, "r", newline="") as f:
        return list(csv.DictReader(f))


def _recent_trades_for_symbol(all_closed, symbol, window_trades):
    """Returns the most recent `window_trades` closed trades for a symbol,
    sorted chronologically, using whichever date format each row has."""
    matching = [c for c in all_closed if c["symbol"] == symbol]
    matching.sort(key=lambda c: _parse_dt(c["close_date"], c["close_time"]))
    return matching[-window_trades:]


def evaluate_coin(symbol, all_closed, config=None):
    """Evaluate a single coin against current config. Returns a dict with
    the stats used, regardless of whether action was taken - useful for
    a dashboard preview/debug view."""
    cfg = config or load_config()
    all_for_symbol = [c for c in all_closed if c["symbol"] == symbol]
    total_trades = len(all_for_symbol)

    result = {
        "symbol": symbol,
        "total_trades": total_trades,
        "eligible": total_trades >= cfg["min_trades_before_active"],
        "window_trades": cfg["window_trades"],
        "window_wins": None,
        "window_win_rate_pct": None,
        "currently_benched": is_benched(symbol),
    }

    if not result["eligible"]:
        return result

    recent = _recent_trades_for_symbol(all_closed, symbol, cfg["window_trades"])
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
        # naturally expires, at which point it gets a fresh window to
        # prove itself again.

    return results


if __name__ == "__main__":
    # Quick manual check from the command line:
    #   python bybit_coin_bench.py <path_to_closed_trades.csv>
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(DATA_DIR, "closed_trades.csv")
    results = run_bench_check(path)
    print(f"{'SYMBOL':<12}{'TRADES':<8}{'ELIGIBLE':<10}{'WINS':<6}{'WR%':<8}{'BENCHED':<8}")
    for r in results:
        wr_str = f"{r['window_win_rate_pct']:.1f}" if r['window_win_rate_pct'] is not None else "-"
        print(f"{r['symbol']:<12}{r['total_trades']:<8}{str(r['eligible']):<10}"
              f"{str(r['window_wins']):<6}{wr_str:<8}{str(r['currently_benched']):<8}")
    print(f"\nCurrently benched: {load_benched()}")