"""
bybit_sim.py
------------
Paper trading simulation engine using real Bybit market data.
Runs every 4 hours via Windows Task Scheduler.

- Fetches real prices, funding rates, OI from Bybit public API
- Uses Claude Haiku for signal generation (identical to live bot)
- Simulates market-fill order execution locally
- Checks open positions for TP/SL hits each scan
- Writes all state to bybit_sim_data/state.json (read by dashboard)
- Logs to bybit_sim_data/scan_log.csv, trade_log.csv, closed_trades.csv

No exchange account needed. No regulatory restrictions. 100% real market data.

Usage:
    python bybit_sim.py
"""

import os, csv, json, time, hmac, hashlib, math, requests
from datetime import datetime
from urllib.parse import urlencode
from dotenv import load_dotenv
import anthropic
from bybit_coin_bench import get_active_watchlist, run_bench_check

load_dotenv()

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')

# ─── Configuration ─────────────────────────────────────────────────────────────
STARTING_BALANCE     = 10_000.0   # USDT
POSITION_SIZE_PCT    = 0.01        # margin per trade = 1% of current balance
BASE_LEVERAGE        = 5
MAX_LEVERAGE         = 10
MAX_POSITIONS        = 20
CONFIDENCE_THRESHOLD = 0.65
STOP_LOSS_PCT        = 0.02       # 2% adverse move
TAKE_PROFIT_PCT      = 0.04       # 4% favourable move
CANDLE_INTERVAL      = '240'      # 4h candles
CANDLES_TO_FETCH     = 50
MARKET_URL           = 'https://api.bybit.com'
# Cloudflare (fronting Bybit's API) commonly 403s the default
# requests library User-Agent ("python-requests/x.x.x"), especially from
# datacenter/cloud IP ranges like GitHub Actions runners. Worked fine
# locally from a residential IP even without this. Added 2026-07-17 after
# migrating bybit_sim.py to run on GitHub Actions.
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}

WATCHLIST = [
    'AAVEUSDT',
    'ADAUSDT',
    'ALGOUSDT',
    'APTUSDT',
    'ATOMUSDT',
    'AVAXUSDT',
    'BCHUSDT',
    'BTCUSDT',
    'BNBUSDT',    
    'DOGEUSDT',
    'DOTUSDT',
    'ENAUSDT',
    'ETHUSDT',
    'HBARUSDT',
    'HYPEUSDT',
    'ICPUSDT',
    'KASUSDT',
    'LINKUSDT',
    'LTCUSDT',
    'MNTUSDT',
    'ONDOUSDT',
    'PAXGUSDT',
    'NEARUSDT',
    'SEIUSDT',
    'SKYUSDT',
    'SOLUSDT',
    'SUIUSDT',
    'TAOUSDT',
    'TONUSDT',
    'TRXUSDT',
    'UNIUSDT',
    'XLMUSDT',
    'XPLUSDT',
    'XRPUSDT',
    'ZECUSDT',
] 
# Manual exclusions above are now handled by bybit_coin_bench.py instead -
# coins get auto-benched/retested based on rolling recent performance
# rather than permanently commented out. See bench_config.json to tune.
# ─── Data directory ────────────────────────────────────────────────────────────
DATA_DIR         = 'bybit_sim_data'
STATE_FILE       = os.path.join(DATA_DIR, 'state.json')
SCAN_LOG         = os.path.join(DATA_DIR, 'scan_log.csv')
TRADE_LOG        = os.path.join(DATA_DIR, 'trade_log.csv')
CLOSED_TRADE_LOG = os.path.join(DATA_DIR, 'closed_trades.csv')
os.makedirs(DATA_DIR, exist_ok=True)

SCAN_HEADERS = [
    'scan_date', 'scan_time', 'symbol', 'price', 'decision', 'direction',
    'confidence', 'leverage', 'risk', 'momentum_24h', 'momentum_7d',
    'volume_trend_pct', 'rsi', 'funding_rate', 'oi_change_pct', 'reasoning', 'sfp_signal',
]
TRADE_HEADERS = [
    'trade_date', 'trade_time', 'symbol', 'direction', 'entry_price',
    'qty', 'notional_usdt', 'margin_usdt', 'leverage',
    'take_profit', 'stop_loss', 'confidence', 'funding_rate', 'oi_change_pct',
    'fear_greed_at_open', 'regime_at_open', 'sfp_at_open',
]
CLOSED_HEADERS = [
    'close_date', 'close_time', 'symbol', 'direction', 'qty',
    'entry_price', 'exit_price', 'pnl', 'pnl_pct', 'outcome', 'close_reason',
    'fear_greed_at_close', 'regime_at_close',
]

def init_csv(path, headers):
    if not os.path.exists(path):
        with open(path, 'w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()

def append_csv(path, headers, row):
    with open(path, 'a', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=headers).writerow(row)

for _p, _h in [(SCAN_LOG, SCAN_HEADERS), (TRADE_LOG, TRADE_HEADERS),
               (CLOSED_TRADE_LOG, CLOSED_HEADERS)]:
    init_csv(_p, _h)

# ─── State management ──────────────────────────────────────────────────────────

def load_state() -> dict:
    """Load portfolio state from JSON. Creates fresh state if none exists."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'balance':          STARTING_BALANCE,   # available cash
        'starting_balance': STARTING_BALANCE,
        'positions':        {},                  # symbol -> position dict
        'equity_history':   [],                  # [{date, equity}]
        'last_scan':        None,
        'scan_count':       0,
        'total_trades':     0,
        'total_wins':       0,
        'total_losses':     0,
        'realised_pnl':     0.0,
    }

def save_state(state: dict):
    with open(STATE_FILE, 'w', newline='\n', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

# ─── Bybit public API (market data only, no auth) ──────────────────────────────

def bybit_get(endpoint: str, params: dict = None) -> dict:
    params = params or {}
    query  = urlencode(params)
    url    = f'{MARKET_URL}{endpoint}?{query}' if query else f'{MARKET_URL}{endpoint}'
    r = requests.get(url, timeout=15, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    if data.get('retCode') != 0:
        raise ValueError(f"Bybit {data.get('retCode')}: {data.get('retMsg')}")
    return data['result']

def get_klines(symbol: str) -> list:
    result  = bybit_get('/v5/market/kline', {
        'category': 'linear', 'symbol': symbol,
        'interval': CANDLE_INTERVAL, 'limit': str(CANDLES_TO_FETCH),
    })
    candles = result.get('list', [])
    candles.reverse()
    return candles

def get_ticker(symbol: str) -> dict:
    result = bybit_get('/v5/market/tickers', {'category': 'linear', 'symbol': symbol})
    items  = result.get('list', [])
    return items[0] if items else {}

def get_open_interest(symbol: str) -> list:
    result = bybit_get('/v5/market/open-interest', {
        'category': 'linear', 'symbol': symbol, 'intervalTime': '4h', 'limit': '2',
    })
    return result.get('list', [])

def get_current_price(symbol: str) -> float | None:
    """Fast price fetch for TP/SL checking."""
    try:
        ticker = get_ticker(symbol)
        return float(ticker.get('lastPrice', 0)) or None
    except Exception:
        return None

def get_fear_greed() -> tuple:
    try:
        r    = requests.get('https://api.alternative.me/fng/?limit=1', timeout=5)
        data = r.json()['data'][0]
        return int(data['value']), data['value_classification']
    except Exception:
        return 50, 'Neutral'

def get_btc_dominance() -> float | None:
    try:
        r   = requests.get('https://api.coingecko.com/api/v3/global', timeout=5)
        pct = r.json()['data']['market_cap_percentage']['btc']
        return round(float(pct), 1)
    except Exception:
        return None

# ─── Technical indicators ──────────────────────────────────────────────────────

def compute_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 1)

def detect_sfp(candles: list, lookback: int = 10) -> dict:
    """
    Detect a Swing Failure Pattern on the most recently closed candle.

    Bearish SFP: current candle wicks ABOVE the prior `lookback` candles'
    high, but closes back BELOW that prior high (and below its own open)
    - a failed breakout / liquidity sweep to the upside, often associated
    with a short-side reversal.

    Bullish SFP: current candle wicks BELOW the prior `lookback` candles'
    low, but closes back ABOVE that prior low (and above its own open)
    - a failed breakdown / liquidity sweep to the downside, often
    associated with a long-side reversal.

    This is logged as additional context only - it is NOT wired into the
    BUY/SELL/HOLD decision rules. The intent is to accumulate enough
    SFP-tagged trade outcomes to test whether it's actually predictive
    before giving it any weight in the rules themselves.

    NOTE: candles[-1] from the exchange's kline endpoint is typically the
    CURRENTLY-FORMING candle, not a finished one - a still-forming bar
    can't yet show the "wick out then close back in" structure the
    pattern depends on. So we evaluate candles[-2] (the last bar that's
    actually finished) and compute the prior swing window from before
    that, rather than using the live/incomplete bar.
    """
    if len(candles) < lookback + 3:
        return {'bullish_sfp': False, 'bearish_sfp': False, 'sfp_level': None}

    opens  = [float(c[1]) for c in candles]
    highs  = [float(c[2]) for c in candles]
    lows   = [float(c[3]) for c in candles]
    closes = [float(c[4]) for c in candles]

    # Prior swing high/low excludes the evaluated candle AND the still-
    # forming live candle at index -1.
    prior_high = max(highs[-(lookback + 2):-2])
    prior_low  = min(lows[-(lookback + 2):-2])

    cur_open, cur_high, cur_low, cur_close = opens[-2], highs[-2], lows[-2], closes[-2]

    bearish_sfp = cur_high > prior_high and cur_close < prior_high and cur_close < cur_open
    bullish_sfp = cur_low < prior_low and cur_close > prior_low and cur_close > cur_open

    sfp_level = None
    if bearish_sfp:
        sfp_level = round(prior_high, 6)
    elif bullish_sfp:
        sfp_level = round(prior_low, 6)

    return {'bullish_sfp': bullish_sfp, 'bearish_sfp': bearish_sfp, 'sfp_level': sfp_level}

def build_market_data(symbol: str) -> dict | None:
    try:
        candles = get_klines(symbol)
        if len(candles) < 10:
            return None

        closes  = [float(c[4]) for c in candles]
        volumes = [float(c[5]) for c in candles]
        highs   = [float(c[2]) for c in candles]
        lows    = [float(c[3]) for c in candles]
        price   = closes[-1]

        c24          = closes[-7]  if len(closes) >= 7  else closes[0]
        c7d          = closes[-43] if len(closes) >= 43 else closes[0]
        momentum_24h = (closes[-1] - c24) / c24 * 100
        momentum_7d  = (closes[-1] - c7d) / c7d * 100

        lb            = min(42, len(candles))
        high_7d       = max(highs[-lb:])
        low_7d        = min(lows[-lb:])
        high_24h      = max(highs[-7:]) if len(highs) >= 7 else max(highs)
        low_24h       = min(lows[-7:])  if len(lows)  >= 7 else min(lows)
        pct_from_high = (price - high_7d) / high_7d * 100
        pct_from_low  = (price - low_7d)  / low_7d  * 100

        avg_vol      = sum(volumes[-lb:]) / lb
        recent_vol   = sum(volumes[-2:]) / 2
        volume_trend = ((recent_vol - avg_vol) / avg_vol * 100) if avg_vol else 0.0

        rsi = compute_rsi(closes)

        ticker       = get_ticker(symbol)
        funding_rate = float(ticker.get('fundingRate', 0)) * 100

        oi_list   = get_open_interest(symbol)
        oi_change = 0.0
        if len(oi_list) >= 2:
            oi_now  = float(oi_list[0].get('openInterest', 0))
            oi_prev = float(oi_list[1].get('openInterest', 0))
            oi_change = ((oi_now - oi_prev) / oi_prev * 100) if oi_prev else 0.0

        sfp = detect_sfp(candles)

        return {
            'price':         price,
            'momentum_24h':  round(momentum_24h, 2),
            'momentum_7d':   round(momentum_7d, 2),
            'pct_from_high': round(pct_from_high, 2),
            'pct_from_low':  round(pct_from_low, 2),
            'volume_trend':  round(volume_trend, 1),
            'rsi':           rsi,
            'funding_rate':  round(funding_rate, 4),
            'oi_change':     round(oi_change, 2),
            'high_24h':      high_24h,
            'low_24h':       low_24h,
            'bullish_sfp':   sfp['bullish_sfp'],
            'bearish_sfp':   sfp['bearish_sfp'],
            'sfp_level':     sfp['sfp_level'],
        }
    except Exception as e:
        print(f"  Data error for {symbol}: {e}")
        return None

# ─── Leverage calculation ──────────────────────────────────────────────────────

def load_coin_win_rates() -> dict:
    stats = {}
    if not os.path.exists(CLOSED_TRADE_LOG):
        return stats
    with open(CLOSED_TRADE_LOG, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            sym = row['symbol']
            stats.setdefault(sym, {'wins': 0, 'total': 0})
            stats[sym]['total'] += 1
            if row['outcome'] == 'WIN':
                stats[sym]['wins'] += 1
    return {s: v['wins']/v['total'] for s, v in stats.items() if v['total'] >= 3}

def calculate_leverage(confidence: float, coin_win_rate: float | None) -> int:
    wr    = coin_win_rate if coin_win_rate is not None else 0.5
    score = confidence * 0.65 + wr * 0.35
    if score <= 0.60:
        return BASE_LEVERAGE
    scale    = (score - 0.60) / 0.40
    leverage = BASE_LEVERAGE + (MAX_LEVERAGE - BASE_LEVERAGE) * scale
    return int(min(MAX_LEVERAGE, max(BASE_LEVERAGE, round(leverage))))

# ─── Claude signal generation ──────────────────────────────────────────────────
_claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def get_signal(symbol: str, mdata: dict, macro: dict) -> dict:
    coin = symbol.replace('USDT', '')

    if mdata.get('bearish_sfp'):
        sfp_text = (f"BEARISH — wicked above prior swing high {mdata['sfp_level']} "
                    f"but closed back below it (failed breakout, possible reversal down)")
    elif mdata.get('bullish_sfp'):
        sfp_text = (f"BULLISH — wicked below prior swing low {mdata['sfp_level']} "
                    f"but closed back above it (failed breakdown, possible reversal up)")
    else:
        sfp_text = "none detected"

    prompt = f"""You are a professional crypto trading analyst specialising in perpetual futures.
Analyse the data below for {coin}/USDT and return a JSON trading signal.

MACRO ENVIRONMENT:
- Fear & Greed Index: {macro['fear_greed']} — {macro['fear_greed_label']}
- BTC Dominance: {macro['btc_dominance']}%
- Market regime: {macro['regime']}

TECHNICAL DATA ({coin}):
- Price:              {mdata['price']}
- 24h momentum:       {mdata['momentum_24h']:+.2f}%
- 7d momentum:        {mdata['momentum_7d']:+.2f}%
- Distance from 7d high: {mdata['pct_from_high']:.2f}%
- Distance from 7d low:  {mdata['pct_from_low']:.2f}%
- Volume trend vs 7d avg: {mdata['volume_trend']:+.1f}%
- RSI (14):           {mdata['rsi']}

MARKET STRUCTURE:
- Funding rate:       {mdata['funding_rate']:+.4f}% per 8h
- Open interest 4h change: {mdata['oi_change']:+.2f}%
- 24h range:          {mdata['low_24h']} – {mdata['high_24h']}
- Swing Failure Pattern: {sfp_text}

DECISION RULES:
- BUY (open long):  24h momentum positive AND 7d momentum > -10%, volume
                    confirming, RSI not overbought (< 70),
                    funding not excessively positive (< +0.15%)
- SELL (open short): 7d momentum < -10% AND price more than 8% below 7d high,
                     RSI not oversold (> 30), volume confirming,
                     funding not excessively negative (> -0.15%)
- HOLD: mixed signals, 7d downtrend without confirmation, or low conviction

RISK RULES:
- HIGH risk: RSI > 75 or < 25, extreme funding (> ±0.15%), large OI spike (> ±20%)
- Extreme funding warns against crowded side (squeeze risk)
- Rising OI + rising price = strong trend; falling OI + rising price = weak trend

Return ONLY valid JSON — no markdown, no preamble:
{{
  "decision":   "BUY" | "SELL" | "HOLD",
  "direction":  "long" | "short" | "none",
  "confidence": 0.00–1.00,
  "risk":       "LOW" | "MEDIUM" | "HIGH",
  "reasoning":  "2-3 sentence summary",
  "key_risk":   "single biggest risk to this trade"
}}"""

    response = _claude.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=350,
        messages=[{'role': 'user', 'content': prompt}],
    )
    text = response.content[0].text.strip()
    text = text.replace('```json', '').replace('```', '').strip()
    return json.loads(text)

# ─── TP/SL checker ────────────────────────────────────────────────────────────

def check_positions(state: dict, now: datetime, macro: dict) -> list:
    """
    Check each open position against current price.
    Returns list of close events (dicts) for any that hit TP or SL.
    """
    closes = []
    for symbol, pos in list(state['positions'].items()):
        price = get_current_price(symbol)
        if price is None:
            continue

        direction  = pos['direction']
        entry_px   = pos['entry_price']
        tp_price   = pos['take_profit']
        sl_price   = pos['stop_loss']
        qty        = pos['qty']
        leverage   = pos['leverage']
        close_reason = None

        if direction == 'long':
            if price >= tp_price:
                close_reason = 'TAKE_PROFIT'
                exit_px = tp_price
            elif price <= sl_price:
                close_reason = 'STOP_LOSS'
                exit_px = sl_price
        else:  # short
            if price <= tp_price:
                close_reason = 'TAKE_PROFIT'
                exit_px = tp_price
            elif price >= sl_price:
                close_reason = 'STOP_LOSS'
                exit_px = sl_price

        if close_reason:
            if direction == 'long':
                pnl = (exit_px - entry_px) * qty
            else:
                pnl = (entry_px - exit_px) * qty

            pnl_pct = (pnl / (entry_px * qty / leverage)) * 100  # % of margin
            outcome = 'WIN' if pnl > 0 else 'LOSS'

            # Return margin + P&L to balance
            margin_returned = pos['margin_usdt'] + pnl
            state['balance'] += margin_returned

            closes.append({
                'symbol':       symbol,
                'direction':    direction,
                'qty':          qty,
                'entry_price':  entry_px,
                'exit_price':   exit_px,
                'pnl':          round(pnl, 4),
                'pnl_pct':      round(pnl_pct, 2),
                'outcome':      outcome,
                'close_reason': close_reason,
                'close_date':   now.strftime('%Y-%m-%d'),
                'close_time':   now.strftime('%H:%M:%S'),
                'fear_greed_at_close': macro['fear_greed'],
                'regime_at_close':     macro['regime'],
            })

            # Update state stats
            state['total_trades'] += 1
            state['realised_pnl'] = round(state['realised_pnl'] + pnl, 4)
            if outcome == 'WIN':
                state['total_wins'] += 1
            else:
                state['total_losses'] += 1

            # Remove from positions
            del state['positions'][symbol]

            print(f"  >> {symbol} {close_reason}: {outcome}  PnL {pnl:+.4f} USDT  "
                  f"({pnl_pct:+.1f}% of margin)")

        time.sleep(0.2)

    return closes

# ─── Simulated order fill ──────────────────────────────────────────────────────

def open_position(state: dict, symbol: str, direction: str, price: float,
                  leverage: int, confidence: float,
                  funding_rate: float, oi_change: float, now: datetime,
                  macro: dict, sfp_signal: str = 'NONE'):
    """Simulate a market fill and add position to state."""
    if direction == 'long':
        tp_price = round(price * (1 + TAKE_PROFIT_PCT), 6)
        sl_price = round(price * (1 - STOP_LOSS_PCT), 6)
    else:
        tp_price = round(price * (1 - TAKE_PROFIT_PCT), 6)
        sl_price = round(price * (1 + STOP_LOSS_PCT), 6)

    margin     = round(state['balance'] * POSITION_SIZE_PCT, 2)
    notional   = margin * leverage
    qty        = round(notional / price, 6)

    # Deduct margin from balance
    state['balance'] = round(state['balance'] - margin, 4)

    state['positions'][symbol] = {
        'symbol':       symbol,
        'direction':    direction,
        'entry_price':  price,
        'current_price': price,
        'qty':          qty,
        'notional_usdt': round(notional, 2),
        'margin_usdt':  margin,
        'leverage':     leverage,
        'take_profit':  tp_price,
        'stop_loss':    sl_price,
        'confidence':   round(confidence, 4),
        'funding_rate': funding_rate,
        'oi_change':    oi_change,
        'open_date':    now.strftime('%Y-%m-%d'),
        'open_time':    now.strftime('%H:%M:%S'),
        'unrealised_pnl': 0.0,
        'fear_greed_at_open': macro['fear_greed'],
        'regime_at_open':     macro['regime'],
        'sfp_at_open':        sfp_signal,
    }

    print(f"  >> FILLED {direction.upper()} {symbol} @ {price}  "
          f"TP: {tp_price}  SL: {sl_price}  "
          f"Lev: {leverage}x  Margin: ${margin}")

    append_csv(TRADE_LOG, TRADE_HEADERS, {
        'trade_date':    now.strftime('%Y-%m-%d'),
        'trade_time':    now.strftime('%H:%M:%S'),
        'symbol':        symbol,
        'direction':     direction,
        'entry_price':   price,
        'qty':           qty,
        'notional_usdt': round(notional, 2),
        'margin_usdt':   margin,
        'leverage':      leverage,
        'take_profit':   tp_price,
        'stop_loss':     sl_price,
        'confidence':    round(confidence, 4),
        'funding_rate':  funding_rate,
        'oi_change_pct': oi_change,
        'fear_greed_at_open': macro['fear_greed'],
        'regime_at_open':     macro['regime'],
        'sfp_at_open':        sfp_signal,
    })

# ─── Update unrealised P&L on open positions ───────────────────────────────────

def update_unrealised(state: dict):
    """Refresh current_price and unrealised_pnl for all open positions."""
    for symbol, pos in state['positions'].items():
        price = get_current_price(symbol)
        if price is None:
            continue
        direction = pos['direction']
        entry_px  = pos['entry_price']
        qty       = pos['qty']
        if direction == 'long':
            pnl = (price - entry_px) * qty
        else:
            pnl = (entry_px - price) * qty
        pos['current_price']   = price
        pos['unrealised_pnl']  = round(pnl, 4)
        time.sleep(0.15)

# ─── Equity snapshot ───────────────────────────────────────────────────────────

def record_equity(state: dict, now: datetime):
    """Add current equity to history (used for equity curve in dashboard)."""
    unrealised = sum(p['unrealised_pnl'] for p in state['positions'].values())
    equity     = round(state['balance'] + unrealised +
                       sum(p['margin_usdt'] for p in state['positions'].values()), 2)
    state['equity_history'].append({
        'ts':     now.strftime('%Y-%m-%d %H:%M'),
        'equity': equity,
    })
    # Keep last 500 data points (~83 days at 4h cadence)
    if len(state['equity_history']) > 500:
        state['equity_history'] = state['equity_history'][-500:]
    return equity

# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    now   = datetime.now()
    state = load_state()

    print("BYBIT SIMULATION ENGINE")
    print("=" * 57)
    print(f"Scan:          {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Balance:       {state['balance']:.2f} USDT")
    print(f"Open positions:{len(state['positions'])}")
    print(f"Scan #:        {state['scan_count'] + 1}")
    print("=" * 57)

    # ── Step 0: Macro context (fetched first so opens/closes can be stamped) ──
    print("\nFetching macro context...")
    fear_greed, fear_greed_label = get_fear_greed()
    btc_dominance = get_btc_dominance()

    if fear_greed >= 75:
        regime = 'GREED — caution on longs, favour shorts'
    elif fear_greed <= 25:
        regime = 'FEAR — market oversold, slight long bias but follow technicals'
    else:
        regime = 'NEUTRAL — standard sizing'

    macro = {
        'fear_greed':       fear_greed,
        'fear_greed_label': fear_greed_label,
        'btc_dominance':    btc_dominance if btc_dominance else 'N/A',
        'regime':           regime,
    }

    print(f"  Fear & Greed:  {fear_greed} — {fear_greed_label}")
    print(f"  BTC Dominance: {macro['btc_dominance']}%")
    print(f"  Regime:        {regime}\n")

    # ── Step 1: Check existing positions for TP/SL hits ───────────────────────
    if state['positions']:
        print("Checking open positions for TP/SL...")
        closes = check_positions(state, now, macro)
        for c in closes:
            append_csv(CLOSED_TRADE_LOG, CLOSED_HEADERS, {
                'close_date':  c['close_date'],
                'close_time':  c['close_time'],
                'symbol':      c['symbol'],
                'direction':   c['direction'],
                'qty':         c['qty'],
                'entry_price': c['entry_price'],
                'exit_price':  c['exit_price'],
                'pnl':         c['pnl'],
                'pnl_pct':     c['pnl_pct'],
                'outcome':     c['outcome'],
                'close_reason': c['close_reason'],
                'fear_greed_at_close': c['fear_greed_at_close'],
                'regime_at_close':     c['regime_at_close'],
            })
        if closes:
            save_state(state)
    else:
        print("\nNo open positions to check.")

    # ── Step 2: Update unrealised P&L on remaining positions ──────────────────
    update_unrealised(state)

    # ── Step 3: Coin benching — exclude underperformers from new entries ──────
    print("\nChecking coin bench status...")
    run_bench_check(CLOSED_TRADE_LOG)
    active_watchlist = get_active_watchlist(WATCHLIST)
    benched_now = sorted(set(WATCHLIST) - set(active_watchlist))
    if benched_now:
        print(f"  Benched (skipped this cycle): {', '.join(benched_now)}")
    else:
        print("  No coins currently benched.")

    # ── Step 4: Scan watchlist ────────────────────────────────────────────────
    committed       = set(state['positions'].keys())
    available_slots = MAX_POSITIONS - len(committed)
    win_rates       = load_coin_win_rates()
    candidates      = []
    scan_results    = []

    print(f"Available slots: {available_slots}\n")

    for symbol in active_watchlist:
        if symbol in committed:
            print(f"Skipping {symbol} — position open")
            continue

        print(f"Analysing {symbol}...")
        mdata = build_market_data(symbol)
        if mdata is None:
            print(f"  Insufficient data — skipping")
            continue

        try:
            result = get_signal(symbol, mdata, macro)
        except Exception as e:
            print(f"  Signal error: {e}")
            continue

        decision   = result.get('decision', 'HOLD')
        direction  = result.get('direction', 'none')
        confidence = float(result.get('confidence', 0.0))
        risk       = result.get('risk', 'MEDIUM')
        reasoning  = result.get('reasoning', '')
        key_risk   = result.get('key_risk', '')

        if mdata.get('bearish_sfp'):
            sfp_signal = 'BEARISH'
        elif mdata.get('bullish_sfp'):
            sfp_signal = 'BULLISH'
        else:
            sfp_signal = 'NONE'

        coin_wr  = win_rates.get(symbol)
        leverage = calculate_leverage(confidence, coin_wr) \
                   if decision in ('BUY', 'SELL') else BASE_LEVERAGE

        dir_label = {'long': 'LONG ', 'short': 'SHORT', 'none': '     '}.get(direction, '     ')
        print(f"  {decision:<4} {dir_label} | Conf: {confidence:.0%} | "
              f"Lev: {leverage}x | Risk: {risk} | "
              f"Funding: {mdata['funding_rate']:+.4f}% | SFP: {sfp_signal}")
        print(f"  {reasoning[:110]}")

        scan_results.append({
            'symbol':    symbol,
            'price':     mdata['price'],
            'decision':  decision,
            'direction': direction,
            'confidence': confidence,
            'leverage':  leverage,
            'risk':      risk,
            'momentum_24h': mdata['momentum_24h'],
            'momentum_7d':  mdata['momentum_7d'],
            'rsi':       mdata['rsi'],
            'funding_rate': mdata['funding_rate'],
            'oi_change': mdata['oi_change'],
            'sfp_signal': sfp_signal,
            'reasoning': reasoning[:200],
            'key_risk':  key_risk[:200],
            'executed':  False,
        })

        append_csv(SCAN_LOG, SCAN_HEADERS, {
            'scan_date':        now.strftime('%Y-%m-%d'),
            'scan_time':        now.strftime('%H:%M:%S'),
            'symbol':           symbol,
            'price':            mdata['price'],
            'decision':         decision,
            'direction':        direction,
            'confidence':       round(confidence, 4),
            'leverage':         leverage,
            'risk':             risk,
            'momentum_24h':     mdata['momentum_24h'],
            'momentum_7d':      mdata['momentum_7d'],
            'volume_trend_pct': mdata['volume_trend'],
            'rsi':              mdata['rsi'],
            'funding_rate':     mdata['funding_rate'],
            'oi_change_pct':    mdata['oi_change'],
            'sfp_signal':       sfp_signal,
            'reasoning':        reasoning[:200],
        })

        if decision in ('BUY', 'SELL') and confidence >= CONFIDENCE_THRESHOLD:
            if risk == 'HIGH' and confidence < 0.82:
                print(f"  Skipping HIGH risk (conf {confidence:.0%} < 82%)")
                continue
            candidates.append({
                'symbol':       symbol,
                'direction':    direction,
                'price':        mdata['price'],
                'leverage':     leverage,
                'confidence':   confidence,
                'risk':         risk,
                'funding_rate': mdata['funding_rate'],
                'oi_change':    mdata['oi_change'],
                'key_risk':     key_risk,
                'sfp_signal':   sfp_signal,
            })

        time.sleep(0.4)

    # ── Step 5: Open new positions ────────────────────────────────────────────
    print(f"\n{'='*57}")
    print(f"Candidates: {len(candidates)}")

    candidates.sort(key=lambda x: x['confidence'], reverse=True)
    trades_placed = 0

    for c in candidates:
        if trades_placed >= available_slots:
            break
        if state['balance'] * POSITION_SIZE_PCT < 1.0:
            print(f"Insufficient balance ({state['balance']:.2f} USDT). Stopping.")
            break

        # Directional concentration guard — max 50% of slots in one direction
        # unless confidence >= 0.82 (high conviction overrides)
        direction_cap = MAX_POSITIONS * 0.5
        open_positions = state['positions'].values()
        longs  = sum(1 for p in open_positions if p['direction'] == 'long')
        shorts = sum(1 for p in open_positions if p['direction'] == 'short')
        dir_count = longs if c['direction'] == 'long' else shorts
        if dir_count >= direction_cap and c['confidence'] < 0.82:
            print(f"  Skipping {c['symbol']} — {c['direction'].upper()} concentration at "
                  f"{dir_count}/{MAX_POSITIONS} slots (conf {c['confidence']:.0%} < 82%)")
            continue

        print(f"\nOpening {c['direction'].upper()} — {c['symbol']}:")
        open_position(
            state, c['symbol'], c['direction'], c['price'],
            c['leverage'], c['confidence'],
            c['funding_rate'], c['oi_change'], now,
            macro, c['sfp_signal'],
        )
        # Mark the matching scan_result row as executed
        for r in scan_results:
            if r['symbol'] == c['symbol']:
                r['executed'] = True
                break
        trades_placed += 1

    # ── Step 6: Save state ────────────────────────────────────────────────────
    state['scan_count'] += 1
    state['last_scan']   = now.strftime('%Y-%m-%d %H:%M:%S')
    state['last_scan_results'] = scan_results
    state['last_macro']  = macro

    equity = record_equity(state, now)
    save_state(state)

    pnl_total = round(equity - state['starting_balance'], 2)
    win_rate  = (state['total_wins'] / state['total_trades'] * 100) \
                if state['total_trades'] else 0

    print(f"\n{'='*57}")
    print(f"Scan #{state['scan_count']} complete")
    print(f"Equity:        {equity:.2f} USDT  ({pnl_total:+.2f} USDT overall)")
    print(f"Trades:        {state['total_trades']} closed  "
          f"({state['total_wins']}W / {state['total_losses']}L  "
          f"{win_rate:.0f}% win rate)")
    print(f"Open positions:{len(state['positions'])}")
    print(f"New trades:    {trades_placed}")
    print("Done.")

if __name__ == '__main__':
    main()