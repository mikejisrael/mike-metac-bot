"""
check_positions.py
------------------
Run this script anytime to see your open positions, current P&L,
and confirm stop/take-profit orders are still active on IBKR.
Closed trades are automatically logged to ibkr_logs/closed_trades.csv.

Usage:
    python check_positions.py
"""

from ib_insync import IB
import csv
import os
from datetime import datetime

# ─── Connect ──────────────────────────────────────────────────────────────────
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=2)

# ─── Logging setup ────────────────────────────────────────────────────────────
LOG_DIR          = 'ibkr_logs'
CLOSED_TRADE_LOG = os.path.join(LOG_DIR, 'closed_trades.csv')
os.makedirs(LOG_DIR, exist_ok=True)

CLOSED_HEADERS = [
    'close_date', 'close_time', 'symbol', 'currency',
    'shares', 'entry_price', 'exit_price',
    'pnl', 'pnl_pct', 'outcome'
]

def init_csv(path, headers):
    if not os.path.exists(path):
        with open(path, 'w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()

def append_csv(path, headers, row):
    with open(path, 'a', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=headers).writerow(row)

init_csv(CLOSED_TRADE_LOG, CLOSED_HEADERS)

now = datetime.now()

print("IBKR POSITION MONITOR")
print("=" * 55)
print(f"Checked: {now.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 55)

# ─── Load last known prices from scan_log as fallback ─────────────────────────
def load_last_known_prices():
    """Read scan_log.csv and return the most recent price per symbol."""
    prices = {}
    scan_log = os.path.join(LOG_DIR, 'scan_log.csv')
    if os.path.exists(scan_log):
        with open(scan_log, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                prices[row['symbol']] = {
                    'price': float(row['price']),
                    'date':  row['scan_date'],
                    'time':  row['scan_time'],
                }
    return prices  # last row per symbol wins (file is append-only)

last_known = load_last_known_prices()

# ─── Open positions ───────────────────────────────────────────────────────────
positions = ib.positions()
open_positions = [p for p in positions if p.position > 0]

if not open_positions:
    print("\nNo open positions.")
else:
    print(f"\nOpen positions: {len(open_positions)}\n")

    # Try to get live prices — silently fall back to scan_log if unavailable
    contracts = [p.contract for p in open_positions]
    try:
        tickers   = ib.reqTickers(*contracts)
        price_map = {}
        for t in tickers:
            mp = t.marketPrice()
            if mp and mp == mp:  # check not nan
                price_map[t.contract.symbol] = ('live', mp)
    except Exception:
        price_map = {}

    total_pnl = 0.0

    for p in open_positions:
        symbol   = p.contract.symbol
        currency = p.contract.currency
        shares   = p.position
        avg_cost = p.avgCost

        # Use live price if available, else fall back to last scan price
        # Note: positions use bare symbol (e.g. 'BHP') but scan_log uses 'BHP.AX'
        scan_symbol = symbol if symbol in last_known else symbol + '.AX'

        if symbol in price_map:
            price_source, mkt_price = price_map[symbol]
        elif scan_symbol in last_known:
            price_source = f"last scan {last_known[scan_symbol]['date']}"
            mkt_price    = last_known[scan_symbol]['price']
        else:
            price_source = 'entry price only - not in recent scan'
            mkt_price    = avg_cost  # neutral — shows 0 P&L

        pnl     = (mkt_price - avg_cost) * shares
        pnl_pct = ((mkt_price - avg_cost) / avg_cost) * 100 if avg_cost else 0
        total_pnl += pnl

        pnl_str     = f"+{pnl:,.2f}"     if pnl     >= 0 else f"{pnl:,.2f}"
        pnl_pct_str = f"+{pnl_pct:.2f}%" if pnl_pct >= 0 else f"{pnl_pct:.2f}%"

        print(f"  {symbol:<10} {shares} shares")
        print(f"    Entry:   {currency} {avg_cost:.2f}")
        print(f"    Current: {currency} {mkt_price:.2f}  ({pnl_pct_str})  [{price_source}]")
        print(f"    P&L:     {currency} {pnl_str}")
        print()

    total_str = f"+{total_pnl:,.2f}" if total_pnl >= 0 else f"{total_pnl:,.2f}"
    print(f"  Total unrealised P&L: {total_str}")
    print(f"  Note: prices marked [last scan] are from most recent bot run, not live.")

# ─── Open orders (stop/take-profit check) ─────────────────────────────────────
print(f"\n{'='*55}")
print("Open orders (stop-loss / take-profit):\n")

# reqAllOpenOrders() pulls orders from ALL clients (and TWS), not just this
# session. The bot places brackets as clientId=1; this script runs as
# clientId=2, so plain openOrders() would return nothing. We read the
# resulting Trade objects (not bare Order objects) because a Trade carries
# the contract — so we can group by symbol and show order status.
ib.reqAllOpenOrders()
open_trades = ib.openTrades()

if not open_trades:
    print("  No open orders found.")
    print("  WARNING: If you have open positions, check your bracket orders in TWS!")
else:
    by_symbol = {}
    for t in open_trades:
        sym = t.contract.symbol if t.contract else 'UNKNOWN'
        by_symbol.setdefault(sym, []).append(t)

    for sym, trades in by_symbol.items():
        print(f"  {sym}:")
        for t in trades:
            o          = t.order
            action     = o.action
            order_type = o.orderType
            qty        = o.totalQuantity
            price      = o.lmtPrice if order_type == 'LMT' else o.auxPrice
            status     = t.orderStatus.status if t.orderStatus else ''
            label      = 'Take profit' if action == 'SELL' and order_type == 'LMT' else \
                         'Stop loss'   if action == 'SELL' and order_type == 'STP' else \
                         action
            print(f"    {label:<14} {qty} @ {price:.2f}  [{order_type}] {status}")
        print()

# ─── Closed trades (from fill/execution history) ──────────────────────────────
print(f"{'='*55}")
print("Recent executions (today):\n")

try:
    # reqExecutions returns Fill objects with .contract and .execution attributes
    fills = ib.reqExecutions()
    today_str = now.strftime('%Y%m%d')
    today_fills = [
        f for f in fills
        if hasattr(f, 'execution') and str(f.execution.time)[:8] == today_str
    ]

    if not today_fills:
        print("  No executions found for today.")
    else:
        logged_closes = []
        for fill in today_fills:
            sym      = fill.contract.symbol
            currency = fill.contract.currency
            side     = fill.execution.side   # 'BOT' or 'SLD'
            qty      = fill.execution.shares
            price    = fill.execution.price
            t        = fill.execution.time
            print(f"  {sym:<10} {side} {qty} @ {price:.2f}  at {t}")

            if side == 'SLD':
                entry_price = None
                trade_log_path = os.path.join(LOG_DIR, 'trade_log.csv')
                if os.path.exists(trade_log_path):
                    with open(trade_log_path, 'r', encoding='utf-8') as f:
                        for row in csv.DictReader(f):
                            if row['symbol'] == sym:
                                entry_price = float(row['entry_price'])

                if entry_price:
                    pnl     = (price - entry_price) * qty
                    pnl_pct = ((price - entry_price) / entry_price) * 100
                    outcome = 'WIN' if pnl > 0 else 'LOSS'

                    close_row = {
                        'close_date':  now.strftime('%Y-%m-%d'),
                        'close_time':  now.strftime('%H:%M:%S'),
                        'symbol':      sym,
                        'currency':    currency,
                        'shares':      qty,
                        'entry_price': entry_price,
                        'exit_price':  price,
                        'pnl':         round(pnl, 2),
                        'pnl_pct':     round(pnl_pct, 2),
                        'outcome':     outcome,
                    }
                    logged_closes.append(close_row)
                    pnl_str = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
                    print(f"    -> {outcome}: entry {entry_price:.2f}, exit {price:.2f}, P&L {pnl_str}")

        # Append new closed trades (deduplicated)
        existing = set()
        if os.path.exists(CLOSED_TRADE_LOG):
            with open(CLOSED_TRADE_LOG, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    existing.add((row['close_date'], row['symbol'], row['exit_price']))

        new_closes = 0
        for row in logged_closes:
            key = (row['close_date'], row['symbol'], str(row['exit_price']))
            if key not in existing:
                append_csv(CLOSED_TRADE_LOG, CLOSED_HEADERS, row)
                new_closes += 1

        if new_closes:
            print(f"\n  Logged {new_closes} closed trade(s) to {CLOSED_TRADE_LOG}")

except Exception as e:
    print(f"  Could not retrieve executions: {e}")

# ─── Summary of log files ─────────────────────────────────────────────────────
print(f"\n{'='*55}")
print("Log files:\n")
for fname in ['scan_log.csv', 'trade_log.csv', 'closed_trades.csv']:
    fpath = os.path.join(LOG_DIR, fname)
    if os.path.exists(fpath):
        with open(fpath, 'r', encoding='utf-8') as f:
            rows = sum(1 for _ in f) - 1
        print(f"  {fname:<25} {rows} rows")
    else:
        print(f"  {fname:<25} (not created yet)")

ib.disconnect()
print("\nDone.")