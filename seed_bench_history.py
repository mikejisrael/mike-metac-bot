"""
seed_bench_history.py

One-off script. Run this ONCE, after updating bybit_coin_bench.py with the
fresh-trade-window fix, to give every coin currently stuck on the bench an
immediate clean slate instead of waiting out one more stale cooldown cycle.

What it does, per currently-benched coin:
  1. Removes it from benched_coins.json (unbenches it right now)
  2. Records "now" as its unbench timestamp in bench_history.json

After this runs, those coins are immediately eligible for new entries again.
They won't be re-bench-evaluated until they accumulate a fresh window of
trades closed from this point forward - exactly the same protection newly
auto-unbenched coins get going forward.

This only touches coins that are CURRENTLY benched. It does not affect
config, open positions, or anything else.

Usage (run from the same directory as bybit_sim_data/, i.e. alongside
bybit_sim.py / bybit_coin_bench.py):

    python seed_bench_history.py
"""

from bybit_coin_bench import load_benched, manually_unbench

def main():
    benched = load_benched()

    if not benched:
        print("No coins are currently benched. Nothing to do.")
        return

    print(f"Currently benched: {sorted(benched.keys())}\n")
    print("Unbenching all of them now and seeding bench_history.json "
          "so they get a genuine fresh window going forward...\n")

    for symbol in sorted(benched.keys()):
        manually_unbench(symbol)
        print(f"  Unbenched {symbol}")

    print(f"\nDone. {len(benched)} coin(s) unbenched and reset. "
          f"Check bench_history.json / bench_log.csv to confirm.")

if __name__ == "__main__":
    main()
