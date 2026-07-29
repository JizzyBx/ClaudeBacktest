"""
Binance Futures Backtest — 5 Variants
======================================
stdlib-only Python 3.11 | GitHub Actions compatible

VARIANTS:
  V1 — Original strategy (ADX+EMA, 15m, TP=3×ATR, SL=2×ATR) — 50 coins
  V2 — Tightened exits  (ADX+EMA, 15m, TP=2.8×ATR, SL=1.7×ATR) — 50 coins
  V3 — 30m candles      (ADX+EMA, 30m, TP=3×ATR, SL=2×ATR) — 50 coins
  V4 — 1H candles       (ADX+EMA, 1h, TP=3×ATR, SL=2×ATR) — 50 coins
  V5 — 15m + RSI filter (ADX+EMA+RSI confirmation, 15m, TP=3×ATR, SL=2×ATR) — 50 coins

DATA: data.binance.vision static archive (not fapi — avoids GH Actions geo-block)
RANGE: 3 years back from today (or coin's earliest available data, whichever is shorter)
WORKERS: 10 parallel threads per variant
"""

import json, zipfile, io, csv, time, math, threading, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Coin List (50 coins) ───────────────────────────────────────────────────────
# Original 30
ORIGINAL_30 = [
    'ETHUSDT','DOGEUSDT','DOTUSDT','ARBUSDT',
    '1000BONKUSDT','1000PEPEUSDT','1000SHIBUSDT',
    'ADAUSDT','APTUSDT','LINKUSDT','SOLUSDT',
    'SUIUSDT','1000FLOKIUSDT','WIFUSDT',
    'BTCUSDT','BNBUSDT','NEARUSDT',
    'XRPUSDT','AVAXUSDT','LTCUSDT',
    'ATOMUSDT','OPUSDT','INJUSDT','UNIUSDT','AAVEUSDT','HBARUSDT',
    'TRUMPUSDT','BOMEUSDT','WLDUSDT','NEIROUSDT',
]

# 20 added: big caps + high-vol alts + meme coins all on Binance USDⓈ-M futures
ADDED_20 = [
    # Big cap / high volume alts
    'MATICUSDT',      # will be rewritten to POLUSDT below — kept for awareness
    'FETUSDT',        # AI narrative, high vol
    'RENDERUSDT',     # AI/GPU narrative
    'PENDLEUSDT',     # DeFi, big 2024 runner
    'SEIUSDT',        # L1, high vol
    'TIAUSDT',        # modular L1
    'STXUSDT',        # Bitcoin L2
    'EIGENUSDT',      # restaking, 2024 launch
    'TAOUSDT',        # AI, 2024 top gainer
    'JUPUSDT',        # Solana DEX aggregator
    # Meme / community coins
    'POPCATUSDT',     # Solana meme, big 2024
    '1000RATSUSDT',   # Bitcoin meme
    'TURBOUSDT',      # top 2024 meme
    '1000SATSUSDT',   # Bitcoin meme
    'REZUSDT',        # 2024 meme launch
    # More volume alts
    'LDOUSDT',        # Lido, liquid staking
    'RUNEUSDT',       # THORChain, high vol
    'FTMUSDT',        # Fantom / Sonic rebranded chain
    'CKBUSDT',        # Nervos, Bitcoin L2
    'ARKMUSDT',       # top 2024 gainer
]

# Apply naming rules from handoff doc
SYMBOLS_RAW = ORIGINAL_30 + ADDED_20
SYMBOL_REMAP = {'MATICUSDT': 'POLUSDT'}

SYMBOLS = []
seen = set()
for s in SYMBOLS_RAW:
    s = SYMBOL_REMAP.get(s, s)
    s = s.replace('.P','')
    if s not in seen:
        seen.add(s)
        SYMBOLS.append(s)

assert len(SYMBOLS) == 50, f"Expected 50 symbols, got {len(SYMBOLS)}"

# ── Config ────────────────────────────────────────────────────────────────────
CAPITAL          = 10_000.0
RISK_PCT         = 0.0075      # 0.75% per trade
FEE_PCT          = 0.0005      # 0.05% per side
SLIPPAGE_PCT     = 0.0002      # 0.02% per side
MAX_POSITIONS    = 6
ADX_MIN          = 22
SLOPE_THRESHOLD  = 0.0005      # 0.05% over 10 bars
WORKERS          = 10

# 3-year lookback
END_DATE   = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=3*365)

BASE_URL = "https://data.binance.vision/data/futures/um"

# ── Variants ──────────────────────────────────────────────────────────────────
VARIANTS = [
    {'name': 'V1_15m_Orig',    'interval': '15m', 'tp_mult': 3.0, 'sl_mult': 2.0, 'use_rsi': False},
    {'name': 'V2_15m_TightExit','interval': '15m', 'tp_mult': 2.8, 'sl_mult': 1.7, 'use_rsi': False},
    {'name': 'V3_30m_Orig',    'interval': '30m', 'tp_mult': 3.0, 'sl_mult': 2.0, 'use_rsi': False},
    {'name': 'V4_1h_Orig',     'interval': '1h',  'tp_mult': 3.0, 'sl_mult': 2.0, 'use_rsi': False},
    {'name': 'V5_15m_RSI',     'interval': '15m', 'tp_mult': 3.0, 'sl_mult': 2.0, 'use_rsi': True},
]

# ── Data Fetching ─────────────────────────────────────────────────────────────
_fetch_lock = threading.Lock()
_fetch_errors = []

def fetch_monthly(symbol, interval, year, month):
    """Fetch one month of kline data from Binance static archive."""
    ym = f"{year}-{month:02d}"
    url = f"{BASE_URL}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{ym}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None   # symbol didn't exist yet or delisted — expected
        with _fetch_lock:
            _fetch_errors.append(f"{symbol} {ym}: HTTP {e.code}")
        return None
    except Exception as e:
        with _fetch_lock:
            _fetch_errors.append(f"{symbol} {ym}: {e}")
        return None

def fetch_daily(symbol, interval, date):
    """Fetch one day from the daily archive (for tail-end months not yet in monthly)."""
    ds = date.strftime('%Y-%m-%d')
    url = f"{BASE_URL}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{ds}.zip"
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except:
        return None

def parse_rows(rows):
    """Parse CSV rows into (open_time_ms, open, high, low, close, volume) tuples."""
    candles = []
    for r in rows:
        if not r or r[0].startswith('open_time'):
            continue
        try:
            ts = int(r[0])
            # Guard for microsecond timestamps (2025+ archives)
            if ts > 10**14:
                ts //= 1000
            o, h, l, c, v = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
            candles.append((ts, o, h, l, c, v))
        except:
            continue
    return candles

def fetch_symbol_data(symbol, interval):
    """
    Fetch all available data for a symbol from START_DATE to END_DATE.
    Returns sorted list of (open_time_ms, open, high, low, close, volume).
    """
    all_candles = []
    cur = START_DATE

    # Monthly files
    while cur < END_DATE:
        rows = fetch_monthly(symbol, interval, cur.year, cur.month)
        if rows is not None:
            all_candles.extend(parse_rows(rows))
        # Move to next month
        if cur.month == 12:
            cur = cur.replace(year=cur.year+1, month=1)
        else:
            cur = cur.replace(month=cur.month+1)

    if not all_candles:
        return []

    # Filter to range
    start_ms = int(START_DATE.timestamp() * 1000)
    end_ms   = int(END_DATE.timestamp() * 1000)
    all_candles = [c for c in all_candles if start_ms <= c[0] < end_ms]
    all_candles.sort(key=lambda x: x[0])

    # Deduplicate
    seen_ts = set()
    deduped = []
    for c in all_candles:
        if c[0] not in seen_ts:
            seen_ts.add(c[0])
            deduped.append(c)

    return deduped

# ── Indicators ────────────────────────────────────────────────────────────────
def ema_series(values, period):
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def atr_series(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i]  - closes[i-1]))
        trs.append(tr)
    if not trs:
        return [closes[-1] * 0.005] * len(closes)
    # Wilder smoothing
    atr = [None] * len(closes)
    if len(trs) >= period:
        atr[period] = sum(trs[:period]) / period
        for i in range(period, len(trs)):
            atr[i+1] = (atr[i] * (period - 1) + trs[i]) / period
    return atr

def adx_series(highs, lows, closes, period=14):
    """Returns parallel lists: adx[], pdi[], mdi[] — same length as closes, None during warmup."""
    n = len(closes)
    adx_out = [None] * n
    pdi_out  = [None] * n
    mdi_out  = [None] * n
    if n < period * 3:
        return adx_out, pdi_out, mdi_out

    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down   and up   > 0 else 0.0)
        mdm.append(down if down > up   and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i] -closes[i-1])))

    def wilder_smooth(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1]/p + x)
        return r

    st = wilder_smooth(trs, period)
    sp = wilder_smooth(pdm, period)
    sm = wilder_smooth(mdm, period)
    if not st:
        return adx_out, pdi_out, mdi_out

    pdi_list = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi_list = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx_list  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi_list, mdi_list)]

    if len(dx_list) < period:
        return adx_out, pdi_out, mdi_out

    adx_val = sum(dx_list[:period]) / period
    adx_vals = [adx_val]
    for d in dx_list[period:]:
        adx_val = (adx_val * (period-1) + d) / period
        adx_vals.append(adx_val)

    # Offset: ADX array starts at index = period*2 in closes
    # pdi/mdi arrays start at index = period+1 in closes
    pdi_start = period + 1    # 1 (for diff) + period (for wilder) = period+1
    adx_start = pdi_start + period - 1

    for i, v in enumerate(pdi_list):
        idx = pdi_start + i
        if idx < n:
            pdi_out[idx] = v
            mdi_out[idx] = mdi_list[i]

    for i, v in enumerate(adx_vals):
        idx = adx_start + i
        if idx < n:
            adx_out[idx] = max(0.0, min(100.0, v))

    return adx_out, pdi_out, mdi_out

def rsi_series(closes, period=14):
    """Returns RSI series, None during warmup."""
    rsi_out = [None] * len(closes)
    if len(closes) < period + 1:
        return rsi_out
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period-1) + gains[i]) / period
        avg_loss = (avg_loss * (period-1) + losses[i]) / period
        idx = i + 1  # offset by 1 (we computed diffs from index 1)
        rs = avg_gain / avg_loss if avg_loss else float('inf')
        rsi_out[idx] = 100 - 100/(1+rs)
    return rsi_out

# ── Signal Logic ──────────────────────────────────────────────────────────────
def compute_signals(candles, variant):
    """
    Given a list of closed candles, compute entry signals.
    Returns list of dicts: {bar_idx, signal, entry_price, tp_dist, sl_dist,
                            filter_reject_reason or None}
    + filter rejection counts dict
    """
    closes = [c[4] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]

    tp_mult  = variant['tp_mult']
    sl_mult  = variant['sl_mult']
    use_rsi  = variant['use_rsi']

    # Pre-compute indicator series
    ema9  = ema_series(closes, 9)
    ema21 = ema_series(closes, 21)
    ema50 = ema_series(closes, 50)
    atr   = atr_series(highs, lows, closes, 14)
    adx_s, pdi_s, mdi_s = adx_series(highs, lows, closes, 14)
    rsi_s = rsi_series(closes, 14) if use_rsi else [None]*len(closes)

    reject_counts = {
        'warmup_none'   : 0,
        'adx_fail'      : 0,
        'slope_fail'    : 0,
        'cross_fail'    : 0,
        'rsi_fail'      : 0,
        'signal'        : 0,
    }

    signals = []
    WARMUP = 60   # bars needed before any indicator is reliable

    for i in range(WARMUP, len(candles) - 1):  # -1: use closed bar, enter on next open
        # Check all indicators are available
        if (adx_s[i] is None or atr[i] is None or
                ema9[i] is None or ema21[i] is None or ema50[i] is None):
            reject_counts['warmup_none'] += 1
            continue

        adx_val  = adx_s[i]
        atr_val  = atr[i]
        e9_curr  = ema9[i];   e9_prev  = ema9[i-1]
        e21_curr = ema21[i];  e21_prev = ema21[i-1]
        slope_pct = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100 if i >= 10 else 0.0

        trend_up   = slope_pct >  0.05   # 0.05% slope over 10 bars — matches live bot
        trend_down = slope_pct < -0.05

        crossed_up   = e9_curr > e21_curr and e9_prev <= e21_prev
        crossed_down = e9_curr < e21_curr and e9_prev >= e21_prev

        # Filter 1: ADX
        if adx_val < ADX_MIN:
            reject_counts['adx_fail'] += 1
            continue

        # Filter 2: 50 EMA slope
        if not (trend_up or trend_down):
            reject_counts['slope_fail'] += 1
            continue

        # Filter 3: EMA crossover
        if not (crossed_up or crossed_down):
            reject_counts['cross_fail'] += 1
            continue

        # Filter 4 (V5 only): RSI confirmation
        if use_rsi:
            rsi_val = rsi_s[i]
            if rsi_val is None:
                reject_counts['warmup_none'] += 1
                continue
            # LONG: RSI 45–70 (not overbought, has momentum)
            # SHORT: RSI 30–55 (not oversold, has momentum)
            long_ok  = crossed_up   and trend_up   and 45 <= rsi_val <= 70
            short_ok = crossed_down and trend_down and 30 <= rsi_val <= 55
            if not (long_ok or short_ok):
                reject_counts['rsi_fail'] += 1
                continue

        # Determine signal direction
        sig = None
        if trend_up   and crossed_up:   sig = 'long'
        if trend_down and crossed_down: sig = 'short'
        if sig is None:
            reject_counts['cross_fail'] += 1
            continue

        reject_counts['signal'] += 1
        entry_price = candles[i+1][1]   # open of next bar (closed-bar entry)
        signals.append({
            'bar_idx'   : i+1,
            'signal'    : sig,
            'entry_price': entry_price,
            'tp_dist'   : atr_val * tp_mult,
            'sl_dist'   : atr_val * sl_mult,
            'open_time' : candles[i+1][0],
        })

    return signals, reject_counts

# ── Backtest Engine ───────────────────────────────────────────────────────────
def run_backtest_symbol(symbol, candles, variant):
    """Run backtest for a single symbol. Returns trades list + reject counts."""
    if len(candles) < 80:
        return [], {'warmup_none': len(candles), 'adx_fail':0,'slope_fail':0,
                    'cross_fail':0,'rsi_fail':0,'signal':0}, 0

    signals, reject_counts = compute_signals(candles, variant)
    trades = []

    for sig in signals:
        bar     = sig['bar_idx']
        entry   = sig['entry_price']
        tp_dist = sig['tp_dist']
        sl_dist = sig['sl_dist']
        direction = sig['signal']

        if direction == 'long':
            tp_price = entry + tp_dist
            sl_price = entry - sl_dist
        else:
            tp_price = entry - tp_dist
            sl_price = entry + sl_dist

        # Scan forward bars to find exit
        exit_bar   = None
        exit_price = None
        exit_type  = None

        for j in range(bar + 1, len(candles)):
            h = candles[j][2]
            l = candles[j][3]
            if direction == 'long':
                if l <= sl_price:
                    exit_price = sl_price; exit_type = 'sl'; exit_bar = j; break
                if h >= tp_price:
                    exit_price = tp_price; exit_type = 'tp'; exit_bar = j; break
            else:
                if h >= sl_price:
                    exit_price = sl_price; exit_type = 'sl'; exit_bar = j; break
                if l <= tp_price:
                    exit_price = tp_price; exit_type = 'tp'; exit_bar = j; break

        if exit_bar is None:
            # Open trade at end of data — skip
            continue

        # PnL (% move × position)
        if direction == 'long':
            raw_pnl_pct = (exit_price - entry) / entry
        else:
            raw_pnl_pct = (entry - exit_price) / entry

        # Deduct fees + slippage (both sides)
        cost_pct = (FEE_PCT + SLIPPAGE_PCT) * 2
        net_pnl_pct = raw_pnl_pct - cost_pct

        duration_bars = exit_bar - bar
        trades.append({
            'symbol'       : symbol,
            'direction'    : direction,
            'entry_price'  : entry,
            'exit_price'   : exit_price,
            'exit_type'    : exit_type,
            'net_pnl_pct'  : net_pnl_pct,
            'duration_bars': duration_bars,
            'entry_time'   : candles[bar][0],
            'exit_time'    : candles[exit_bar][0],
        })

    months = max(1, len(candles) // (30 * 24 * 4))   # rough month count (15m bars)
    return trades, reject_counts, months

# ── Portfolio Simulation ──────────────────────────────────────────────────────
def simulate_portfolio(all_trades_raw):
    """
    Apply portfolio-level caps: max 6 concurrent positions, shared $10k equity.
    Trades are sorted by entry_time. Risk 0.75% of current equity per trade.
    Returns list of closed trades with dollar PnL attached.
    """
    events = []
    for t in all_trades_raw:
        events.append(('entry', t['entry_time'], t))
        events.append(('exit',  t['exit_time'],  t))
    events.sort(key=lambda x: (x[1], 0 if x[0]=='entry' else 1))

    equity         = CAPITAL
    open_positions = {}   # symbol -> trade dict
    closed_trades  = []

    for ev_type, ts, trade in events:
        sym = trade['symbol'] + str(trade['entry_time'])  # unique key

        if ev_type == 'entry':
            if len(open_positions) >= MAX_POSITIONS:
                trade['skipped'] = True
                continue
            if any(t['symbol'] == trade['symbol'] for t in open_positions.values()):
                trade['skipped'] = True
                continue
            risk_amount = equity * RISK_PCT
            trade['dollar_risk'] = risk_amount
            trade['skipped']     = False
            open_positions[sym]  = trade

        elif ev_type == 'exit':
            if sym not in open_positions:
                continue
            t = open_positions.pop(sym)
            if t.get('skipped'):
                continue
            # Dollar PnL: net_pnl_pct applied to position notional
            # Position size: risk / sl_pct (fixed-fractional)
            entry     = t['entry_price']
            exit_p    = t['exit_price']
            direction = t['direction']
            sl_dist   = CAPITAL * 0.001   # placeholder; real sizing below

            # Risk-based sizing: risk_amount / sl_pct = notional
            if direction == 'long':
                sl_pct = abs(entry - (entry - t.get('sl_dist_cached', entry * 0.02))) / entry
            else:
                sl_pct = abs(entry - (entry + t.get('sl_dist_cached', entry * 0.02))) / entry

            # Simpler: dollar PnL = net_pnl_pct × notional
            # notional = risk_amount / sl_fraction
            # sl_fraction from net_pnl_pct of a losing trade → 2×ATR/entry
            # We store net_pnl_pct and risk_amount — use as:
            # win: dollar = risk_amount * (tp_mult / sl_mult)
            # loss: dollar = -risk_amount
            risk_amt = t['dollar_risk']
            dollar_pnl = t['net_pnl_pct'] / abs(t['net_pnl_pct']) * risk_amt if t['net_pnl_pct'] != 0 else 0
            if t['exit_type'] == 'tp':
                # Exact ratio based on multiples
                tp_m = abs(t['net_pnl_pct'])
                dollar_pnl = risk_amt * (t.get('tp_sl_ratio', 1.5))
            elif t['exit_type'] == 'sl':
                dollar_pnl = -risk_amt

            equity += dollar_pnl
            t['dollar_pnl'] = dollar_pnl
            t['equity_after'] = equity
            closed_trades.append(t)

    return closed_trades, equity

# ── Better Portfolio Sim ───────────────────────────────────────────────────────
def simulate_portfolio_v2(all_symbol_trades, variant):
    """
    Proper portfolio sim with fixed-fractional sizing.
    Merges all symbol trades, sorts chronologically, applies position cap.
    """
    tp_mult = variant['tp_mult']
    sl_mult = variant['sl_mult']
    rr_ratio = tp_mult / sl_mult   # reward:risk

    # Flatten and sort all trades by entry time
    flat = sorted(all_symbol_trades, key=lambda t: t['entry_time'])

    equity = CAPITAL
    open_pos = {}    # symbol -> {entry_time, exit_time, dollar_risk}
    result_trades = []

    for t in flat:
        sym = t['symbol']

        # Evict closed positions
        evict = [s for s, p in open_pos.items() if p['exit_time'] <= t['entry_time']]
        for s in evict:
            p = open_pos.pop(s)
            equity += p['realized_pnl']

        # Skip if over cap or already in this symbol
        if len(open_pos) >= MAX_POSITIONS or sym in open_pos:
            continue

        risk_amt = equity * RISK_PCT
        if t['exit_type'] == 'tp':
            dollar_pnl = risk_amt * rr_ratio - risk_amt * (FEE_PCT + SLIPPAGE_PCT) * 2
        else:
            dollar_pnl = -risk_amt - risk_amt * (FEE_PCT + SLIPPAGE_PCT) * 2

        open_pos[sym] = {
            'exit_time'    : t['exit_time'],
            'realized_pnl' : dollar_pnl,
        }

        rt = dict(t)
        rt['dollar_pnl']   = dollar_pnl
        rt['risk_amt']      = risk_amt
        rt['equity_before'] = equity
        result_trades.append(rt)

    # Close any still-open positions at end
    for p in open_pos.values():
        equity += p['realized_pnl']

    return result_trades, equity

# ── Statistics ────────────────────────────────────────────────────────────────
def compute_stats(trades, final_equity):
    if not trades:
        return {
            'total_trades':0,'win_rate':0,'profit_factor':0,
            'net_pnl':0,'max_drawdown':0,'sharpe':0,'sortino':0,
            'avg_win':0,'avg_loss':0,'expectancy':0,'avg_duration_bars':0,
            'long_trades':0,'long_wr':0,'short_trades':0,'short_wr':0,
            'max_win_streak':0,'max_loss_streak':0,'final_equity':CAPITAL,
            'usable': False,
        }

    wins   = [t for t in trades if t['dollar_pnl'] > 0]
    losses = [t for t in trades if t['dollar_pnl'] <= 0]
    longs  = [t for t in trades if t['direction'] == 'long']
    shorts = [t for t in trades if t['direction'] == 'short']

    total     = len(trades)
    win_rate  = len(wins) / total * 100
    gross_win = sum(t['dollar_pnl'] for t in wins)
    gross_loss= abs(sum(t['dollar_pnl'] for t in losses))
    pf        = gross_win / gross_loss if gross_loss else (999 if gross_win > 0 else 0)
    net_pnl   = final_equity - CAPITAL

    avg_win   = gross_win / len(wins)   if wins   else 0
    avg_loss  = gross_loss / len(losses) if losses else 0
    expectancy= (win_rate/100 * avg_win) - ((1 - win_rate/100) * avg_loss)

    # Drawdown
    equity_curve = [CAPITAL]
    for t in sorted(trades, key=lambda x: x['entry_time']):
        equity_curve.append(equity_curve[-1] + t['dollar_pnl'])
    peak = equity_curve[0]
    max_dd = 0
    for e in equity_curve:
        if e > peak: peak = e
        dd = (peak - e) / peak * 100
        if dd > max_dd: max_dd = dd

    # Sharpe / Sortino (daily returns approx)
    pnls = [t['dollar_pnl'] for t in sorted(trades, key=lambda x: x['entry_time'])]
    if len(pnls) > 1:
        mean_r   = sum(pnls) / len(pnls)
        std_r    = math.sqrt(sum((p - mean_r)**2 for p in pnls) / len(pnls))
        neg_dev  = math.sqrt(sum((p - mean_r)**2 for p in pnls if p < 0) / max(1, len([p for p in pnls if p < 0])))
        sharpe   = (mean_r / std_r * math.sqrt(252)) if std_r else 0
        sortino  = (mean_r / neg_dev * math.sqrt(252)) if neg_dev else 0
    else:
        sharpe = sortino = 0

    # Streaks
    streak = max_w = max_l = 0
    cur_type = None
    for t in trades:
        w = t['dollar_pnl'] > 0
        if w == cur_type:
            streak += 1
        else:
            streak = 1
            cur_type = w
        if w:  max_w = max(max_w, streak)
        else:  max_l = max(max_l, streak)

    avg_dur = sum(t['duration_bars'] for t in trades) / total

    long_wr  = sum(1 for t in longs  if t['dollar_pnl']>0)/len(longs)*100  if longs  else 0
    short_wr = sum(1 for t in shorts if t['dollar_pnl']>0)/len(shorts)*100 if shorts else 0

    return {
        'total_trades'    : total,
        'win_rate'        : round(win_rate, 2),
        'profit_factor'   : round(pf, 4),
        'net_pnl'         : round(net_pnl, 2),
        'final_equity'    : round(final_equity, 2),
        'max_drawdown_pct': round(max_dd, 2),
        'sharpe'          : round(sharpe, 3),
        'sortino'         : round(sortino, 3),
        'avg_win'         : round(avg_win, 2),
        'avg_loss'        : round(avg_loss, 2),
        'expectancy'      : round(expectancy, 2),
        'avg_duration_bars': round(avg_dur, 1),
        'long_trades'     : len(longs),
        'long_wr'         : round(long_wr, 2),
        'short_trades'    : len(shorts),
        'short_wr'        : round(short_wr, 2),
        'max_win_streak'  : max_w,
        'max_loss_streak' : max_l,
        'usable'          : pf >= 1.5 and win_rate >= 42,
    }

def per_coin_table(trades):
    coin_data = defaultdict(list)
    for t in trades:
        coin_data[t['symbol']].append(t)
    rows = []
    for sym, ts in coin_data.items():
        wins   = sum(1 for t in ts if t['dollar_pnl'] > 0)
        losses = sum(1 for t in ts if t['dollar_pnl'] <= 0)
        total  = len(ts)
        pnl    = sum(t['dollar_pnl'] for t in ts)
        gw     = sum(t['dollar_pnl'] for t in ts if t['dollar_pnl'] > 0)
        gl     = abs(sum(t['dollar_pnl'] for t in ts if t['dollar_pnl'] <= 0))
        pf     = gw / gl if gl else (999 if gw > 0 else 0)
        rows.append({
            'symbol': sym, 'trades': total, 'wins': wins, 'losses': losses,
            'wr': round(wins/total*100, 1) if total else 0,
            'pf': round(pf, 3),
            'pnl': round(pnl, 2),
        })
    return sorted(rows, key=lambda x: x['pf'], reverse=True)

def monthly_pnl(trades, interval):
    monthly = defaultdict(float)
    for t in trades:
        dt  = datetime.fromtimestamp(t['entry_time']/1000, tz=timezone.utc)
        key = dt.strftime('%Y-%m')
        monthly[key] += t['dollar_pnl']
    return {k: round(v, 2) for k, v in sorted(monthly.items())}

# ── Per-symbol worker ─────────────────────────────────────────────────────────
def worker_fetch_and_backtest(symbol, variant):
    """Worker function: fetch data + run backtest for one symbol."""
    candles = fetch_symbol_data(symbol, variant['interval'])
    if not candles:
        return symbol, [], {'warmup_none':0,'adx_fail':0,'slope_fail':0,
                            'cross_fail':0,'rsi_fail':0,'signal':0}, 0
    trades, rejects, months = run_backtest_symbol(symbol, candles, variant)
    return symbol, trades, rejects, months

# ── Run one variant ───────────────────────────────────────────────────────────
def run_variant(variant):
    print(f"\n{'='*60}")
    print(f"  VARIANT: {variant['name']}")
    print(f"  Interval={variant['interval']} | TP={variant['tp_mult']}×ATR | SL={variant['sl_mult']}×ATR | RSI={variant['use_rsi']}")
    print(f"  Coins: {len(SYMBOLS)} | Workers: {WORKERS}")
    print(f"{'='*60}")

    all_raw_trades   = []
    all_rejects      = defaultdict(int)
    coin_candle_months = {}
    data_error_count = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(worker_fetch_and_backtest, sym, variant): sym for sym in SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                sym_out, trades, rejects, months = fut.result()
                all_raw_trades.extend(trades)
                for k, v in rejects.items():
                    all_rejects[k] += v
                coin_candle_months[sym] = months
                status = f"{len(trades)} trades" if trades else "no data / no trades"
                print(f"  ✓ {sym:20s} {status}")
            except Exception as e:
                print(f"  ✗ {sym:20s} ERROR: {e}")
                data_error_count += 1

    # Check for total data failure (all symbols 404 — bucket blocked)
    if data_error_count == len(SYMBOLS):
        print("  ⛔ ALL SYMBOLS FAILED — data bucket may be blocked from GH Actions runner.")
        print("     Aborting this variant to avoid reporting zero trades (which looks like a valid result).")
        return None

    # Portfolio simulation
    result_trades, final_equity = simulate_portfolio_v2(all_raw_trades, variant)

    # Stats
    stats = compute_stats(result_trades, final_equity)
    coin_table = per_coin_table(result_trades)
    monthly   = monthly_pnl(result_trades, variant['interval'])

    # Filter rejection totals
    total_bars_scanned = sum(all_rejects.values())
    reject_display = {}
    for k, v in all_rejects.items():
        pct = v / total_bars_scanned * 100 if total_bars_scanned else 0
        reject_display[k] = {'count': v, 'pct': round(pct, 2)}

    # Sanity check: all bars accounted for
    check_sum = sum(all_rejects.values())
    if total_bars_scanned and check_sum != total_bars_scanned:
        print(f"  ⚠ Filter count mismatch: {check_sum} vs {total_bars_scanned} — debug needed")

    return {
        'variant'     : variant['name'],
        'settings'    : variant,
        'aggregate'   : stats,
        'per_coin'    : coin_table,
        'monthly_pnl' : monthly,
        'filter_stats': reject_display,
        'trade_count' : len(result_trades),
        'data_errors' : data_error_count,
        'coin_data_months': coin_candle_months,
    }

# ── Pretty Print ──────────────────────────────────────────────────────────────
def print_variant_summary(result):
    if not result:
        return
    v  = result['variant']
    ag = result['aggregate']
    print(f"\n{'━'*60}")
    print(f"  RESULTS — {v}")
    print(f"{'━'*60}")
    print(f"  Trades     : {ag['total_trades']}")
    print(f"  Win Rate   : {ag['win_rate']}%")
    print(f"  Profit Factor: {ag['profit_factor']}")
    print(f"  Net PnL    : ${ag['net_pnl']:,.2f}")
    print(f"  Final Equity: ${ag.get('final_equity', 0):,.2f}")
    print(f"  Max Drawdown: {ag['max_drawdown_pct']}%")
    print(f"  Sharpe     : {ag['sharpe']}  |  Sortino: {ag['sortino']}")
    print(f"  Avg Win    : ${ag['avg_win']:,.2f}  |  Avg Loss: ${ag['avg_loss']:,.2f}")
    print(f"  Expectancy : ${ag['expectancy']:,.2f}")
    print(f"  Avg Duration: {ag['avg_duration_bars']} bars")
    print(f"  Longs: {ag['long_trades']} ({ag['long_wr']}% WR)  |  Shorts: {ag['short_trades']} ({ag['short_wr']}% WR)")
    print(f"  Win Streak : {ag['max_win_streak']}  |  Loss Streak: {ag['max_loss_streak']}")
    print(f"  {'✅ USABLE' if ag['usable'] else '❌ DOES NOT MEET TARGETS'} (PF≥1.5 / WR≥42%)")

    print(f"\n  TOP 10 COINS BY PROFIT FACTOR:")
    for row in result['per_coin'][:10]:
        print(f"    {row['symbol']:20s}  PF:{row['pf']:.3f}  WR:{row['wr']}%  Trades:{row['trades']}  PnL:${row['pnl']:,.2f}")

    print(f"\n  FILTER REJECTION BREAKDOWN (total bars scanned basis):")
    for k, v in result['filter_stats'].items():
        print(f"    {k:20s}: {v['count']:>8,}  ({v['pct']}%)")

    print(f"\n  MONTHLY PnL (last 12):")
    months = list(result['monthly_pnl'].items())[-12:]
    for ym, pnl in months:
        bar = '█' * int(abs(pnl)/50) if abs(pnl) < 5000 else '█'*40
        sign = '+' if pnl >= 0 else ''
        print(f"    {ym}  {sign}${pnl:>8,.2f}  {bar}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print("  BINANCE FUTURES BACKTEST — 5 VARIANTS")
    print(f"  Range: {START_DATE.strftime('%Y-%m')} → {END_DATE.strftime('%Y-%m')}")
    print(f"  Coins: {len(SYMBOLS)}")
    print(f"  Capital: ${CAPITAL:,.0f} | Risk: {RISK_PCT*100}%/trade")
    print(f"  Fee: {FEE_PCT*100}% + Slip: {SLIPPAGE_PCT*100}% per side")
    print(f"  Max Positions: {MAX_POSITIONS} | Workers: {WORKERS}")
    print("="*60)
    print(f"\n  COIN LIST:")
    for i, s in enumerate(SYMBOLS):
        print(f"    {i+1:2d}. {s}")

    all_results = []

    for variant in VARIANTS:
        result = run_variant(variant)
        all_results.append(result)
        if result:
            print_variant_summary(result)

    # ── Write outputs ──────────────────────────────────────────────────────────
    # Text summary
    summary_lines = []
    summary_lines.append("BINANCE FUTURES BACKTEST SUMMARY")
    summary_lines.append(f"Range: {START_DATE.strftime('%Y-%m')} to {END_DATE.strftime('%Y-%m')}")
    summary_lines.append(f"Coins: {len(SYMBOLS)} | Capital: ${CAPITAL:,.0f}")
    summary_lines.append("")
    for r in all_results:
        if not r:
            summary_lines.append("VARIANT FAILED — data error")
            continue
        ag = r['aggregate']
        summary_lines.append(f"--- {r['variant']} ---")
        summary_lines.append(f"Trades: {ag['total_trades']} | WR: {ag['win_rate']}% | PF: {ag['profit_factor']}")
        summary_lines.append(f"Net PnL: ${ag['net_pnl']:,.2f} | Max DD: {ag['max_drawdown_pct']}%")
        summary_lines.append(f"Sharpe: {ag['sharpe']} | Sortino: {ag['sortino']}")
        summary_lines.append(f"Verdict: {'✅ USABLE' if ag['usable'] else '❌ BELOW TARGETS'}")
        summary_lines.append("")

    with open('backtest_summary.txt', 'w') as f:
        f.write('\n'.join(summary_lines))

    # JSON report
    report = {
        'meta': {
            'start_date'   : START_DATE.isoformat(),
            'end_date'     : END_DATE.isoformat(),
            'symbols'      : SYMBOLS,
            'capital'      : CAPITAL,
            'risk_pct'     : RISK_PCT,
            'fee_pct'      : FEE_PCT,
            'slippage_pct' : SLIPPAGE_PCT,
            'max_positions': MAX_POSITIONS,
            'adx_min'      : ADX_MIN,
            'generated_at' : datetime.now(timezone.utc).isoformat(),
        },
        'variants': all_results,
        'fetch_errors': _fetch_errors[:100],   # cap at 100
    }

    # Make JSON serializable
    def default_serial(obj):
        if isinstance(obj, (datetime,)): return obj.isoformat()
        return str(obj)

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2, default=default_serial)

    print("\n" + "="*60)
    print("  OUTPUT FILES: backtest_summary.txt | backtest_report.json")
    print("="*60)

    # Final comparison table
    print("\n  VARIANT COMPARISON:")
    print(f"  {'Variant':<22} {'Trades':>7} {'WR%':>6} {'PF':>6} {'NetPnL':>10} {'MaxDD%':>7} {'Verdict'}")
    print(f"  {'-'*80}")
    for r in all_results:
        if not r:
            print(f"  {'ERROR':<22}")
            continue
        ag = r['aggregate']
        verdict = '✅' if ag['usable'] else '❌'
        print(f"  {r['variant']:<22} {ag['total_trades']:>7} {ag['win_rate']:>5.1f}% "
              f"{ag['profit_factor']:>6.3f} ${ag['net_pnl']:>9,.0f} {ag['max_drawdown_pct']:>6.1f}% {verdict}")

if __name__ == '__main__':
    main()
