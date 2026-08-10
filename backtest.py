"""
Micro-Profit Backtest — Options A & B
======================================
Option A: GMaxV1 signal (EMA9/21 crossover + EMA50 slope + ADX>=22)
          on 15m AND 5m, testing 5 TP/SL combos each = 10 variants
Option B: Bollinger Band(20,2) + RSI(14) mean reversion on 5m
          Long: close < lower_BB and RSI < 35
          Short: close > upper_BB and RSI > 65
          TP=0.8% / SL=2.5%

Coins    : 117 USDT-M futures
Period   : up to 2 years back from Aug 2026 — each coin uses its own
           available history (6-month coin → 6 months tested)
Capital  : $10,000 | Risk 0.75% | Leverage 5x | Fee 0.05% | Slip 0.02%
Metrics  : PF, WR, Sharpe, Max DD, Expectancy, Avg Win/Loss, Trade count
"""

import sys, json, csv, io, zipfile, math, time, os
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ── Coin List (117) ────────────────────────────────────────────────────────────
ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT','AIOTUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ANKRUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT','COMBOUSDT',
    'COMMONUSDT','CRCLUSDT','CUSDT','DAMUSDT','DEFIUSDT','DEXEUSDT','DIAUSDT',
    'DMCUSDT','EIGENUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT',
    'EVAAUSDT','FLNCUSDT','FLUXUSDT','FUNUSDT','FXSUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','IPUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','MTLUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT',
    'PEOPLEUSDT','PIPPINUSDT','PIXELUSDT','PLUMEUSDT','POLUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','PUNDIXUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT',
    'SEIUSDT','SIGNUSDT','SKRUSDT','SNDKUSDT','SOMIUSDT','SPELLUSDT',
    'SPKUSDT','STABLEUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VANRYUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT','WLDUSDT',
    'XEMUSDT','XLMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

NUM_SHARDS = 8
WORKERS    = 16

# ── Capital / Risk ─────────────────────────────────────────────────────────────
CAPITAL   = 10_000.0
RISK_PCT  = 0.0075   # 0.75%
FEE       = 0.0005   # 0.05% per side
SLIP      = 0.0002   # 0.02% per side
LEVERAGE  = 5

# ── Date range: Aug 2024 → Jul 2026 (2 years) ─────────────────────────────────
START_YM = (2024, 8)
END_YM   = (2026, 7)

# ── Variants ───────────────────────────────────────────────────────────────────
# Option A: GMaxV1 signal on 15m and 5m, 5 TP/SL combos each
OPTION_A_CONFIGS = [
    # (label, timeframe, tp_pct, sl_pct, max_bars)
    ('A_15m_TP0.5_SL2',   '15m', 0.005, 0.020,  48),
    ('A_15m_TP1.0_SL3',   '15m', 0.010, 0.030,  48),
    ('A_15m_TP1.5_SL5',   '15m', 0.015, 0.050,  96),
    ('A_15m_TP2.0_SL6',   '15m', 0.020, 0.060,  96),
    ('A_15m_TP3.0_SL15',  '15m', 0.030, 0.150, 960),  # baseline
    ('A_5m_TP0.5_SL2',    '5m',  0.005, 0.020,  48),
    ('A_5m_TP1.0_SL3',    '5m',  0.010, 0.030,  96),
    ('A_5m_TP1.5_SL5',    '5m',  0.015, 0.050, 144),
    ('A_5m_TP2.0_SL6',    '5m',  0.020, 0.060, 192),
    ('A_5m_TP3.0_SL15',   '5m',  0.030, 0.150, 960),
]

# Option B: BB+RSI mean reversion on 5m
OPTION_B_CONFIG = {
    'label':    'B_5m_BB_RSI',
    'tf':       '5m',
    'tp':       0.008,   # 0.8%
    'sl':       0.025,   # 2.5%
    'max_bars': 72,      # 6 hours max hold on 5m
    'bb_period':  20,
    'bb_std':     2.0,
    'rsi_period': 14,
    'rsi_long':   35,    # RSI < 35 for long
    'rsi_short':  65,    # RSI > 65 for short
}

# ── Data Fetch ─────────────────────────────────────────────────────────────────
BASE_URLS = [
    "https://data.binance.vision/data/futures/um/monthly/klines",
    "https://data.binance.com/data/futures/um/monthly/klines",
]

def fetch_month(symbol, tf, year, month):
    fname = f"{symbol}-{tf}-{year}-{month:02d}.zip"
    for base in BASE_URLS:
        url = f"{base}/{symbol}/{tf}/{fname}"
        for attempt in range(3):
            try:
                raw = urlopen(url, timeout=20).read()
                with zipfile.ZipFile(io.BytesIO(raw)) as z:
                    csvname = z.namelist()[0]
                    lines = z.read(csvname).decode().splitlines()
                rows = []
                for line in lines:
                    parts = line.split(',')
                    if len(parts) < 5: continue
                    try:
                        ts = int(parts[0])
                        if ts > 10**14: ts //= 1000
                        o, h, l, c = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4])
                        v = float(parts[5]) if len(parts) > 5 else 0.0
                        rows.append((ts, o, h, l, c, v))
                    except (ValueError, IndexError):
                        continue
                return rows
            except HTTPError as e:
                if e.code == 404: return []
                time.sleep(1.5 ** attempt)
            except Exception:
                time.sleep(1.5 ** attempt)
    return []

def month_range(start_ym, end_ym):
    y, m = start_ym
    while (y, m) <= end_ym:
        yield y, m
        m += 1
        if m > 12: m = 1; y += 1

def fetch_symbol(symbol, tf):
    all_rows = []
    for y, m in month_range(START_YM, END_YM):
        all_rows.extend(fetch_month(symbol, tf, y, m))
    if not all_rows: return []
    seen = {}
    for r in all_rows:
        seen[r[0]] = r
    return sorted(seen.values(), key=lambda x: x[0])

# ── Indicators (pure Python) ───────────────────────────────────────────────────
def ema_series(values, period):
    # Seeds from values[0] — matches GMaxV1 live bot exactly
    if not values: return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    result = [None] * n
    if n < period * 3: return result
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_vals  = [0.0] * n
    for i in range(1, n):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr_vals[i]  = max(highs[i] - lows[i],
                          abs(highs[i] - closes[i-1]),
                          abs(lows[i]  - closes[i-1]))
    def smooth(arr, p):
        s = [0.0] * n
        s[p] = sum(arr[1:p+1])
        for i in range(p+1, n):
            s[i] = s[i-1] - s[i-1]/p + arr[i]
        return s
    str14   = smooth(tr_vals,  period)
    spdm14  = smooth(plus_dm,  period)
    smdm14  = smooth(minus_dm, period)
    dx_vals = [None] * n
    for i in range(period, n):
        if str14[i] == 0: continue
        pdi = 100 * spdm14[i] / str14[i]
        mdi = 100 * smdm14[i] / str14[i]
        denom = pdi + mdi
        if denom == 0: continue
        dx_vals[i] = 100 * abs(pdi - mdi) / denom
    # smooth DX into ADX
    start = period * 2
    if n <= start: return result
    valid_dx = [dx_vals[i] for i in range(period, start) if dx_vals[i] is not None]
    if not valid_dx: return result
    adx_prev = sum(valid_dx) / len(valid_dx) if valid_dx else 0
    result[start - 1] = adx_prev
    for i in range(start, n):
        if dx_vals[i] is None: continue
        adx_prev = (adx_prev * (period - 1) + dx_vals[i]) / period
        result[i] = adx_prev
    return result

def rsi_series(closes, period=14):
    n = len(closes)
    result = [None] * n
    if n <= period: return result
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period, n):
        if i > period:
            d = closes[i] - closes[i-1]
            avg_gain = (avg_gain * (period - 1) + max(d, 0))  / period
            avg_loss = (avg_loss * (period - 1) + max(-d, 0)) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100 - 100 / (1 + rs)
    return result

def bb_series(closes, period=20, num_std=2.0):
    n = len(closes)
    upper = [None] * n
    lower = [None] * n
    mid   = [None] * n
    for i in range(period - 1, n):
        window = closes[i - period + 1: i + 1]
        m = sum(window) / period
        var = sum((x - m) ** 2 for x in window) / period
        s = math.sqrt(var)
        mid[i]   = m
        upper[i] = m + num_std * s
        lower[i] = m - num_std * s
    return upper, mid, lower

# ── Signal Functions ───────────────────────────────────────────────────────────
def signal_gmaxv1(i, opens, highs, lows, closes,
                  ema9, ema21, ema50, adx):
    """GMaxV1: EMA9/21 crossover + EMA50 slope + ADX>=22
    Matches live bot check_signal_G() exactly:
    - EMA50 slope uses 10-bar lookback with 0.05% threshold
    - EMA seeded from first value (not SMA)
    - ADX checked after crossover confirmed
    """
    if i < 11: return None  # need i-10 for slope
    # EMA50 slope — 10-bar lookback, 0.05% threshold (live bot exact)
    if ema50[i-10] == 0: return None
    slope_pct  = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down: return None  # flat — skip
    # EMA9/21 crossover on last closed bar
    cross_up   = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
    cross_down = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]
    if not cross_up and not cross_down: return None
    if trend_up   and not cross_up:   return None  # trend/cross direction mismatch
    if trend_down and not cross_down: return None
    # ADX >= 22
    if adx[i] is None or adx[i] < 22: return None
    return 'buy' if cross_up else 'sell'

def signal_bb_rsi(i, closes, upper_bb, lower_bb, rsi, cfg):
    """BB+RSI mean reversion"""
    if upper_bb[i] is None or lower_bb[i] is None: return None
    if rsi[i] is None: return None
    if closes[i] < lower_bb[i] and rsi[i] < cfg['rsi_long']:  return 'buy'
    if closes[i] > upper_bb[i] and rsi[i] > cfg['rsi_short']: return 'sell'
    return None

# ── Position Sizing ────────────────────────────────────────────────────────────
def position_size(sl_pct):
    risk_usd  = CAPITAL * RISK_PCT
    notional  = min(risk_usd / sl_pct, CAPITAL * LEVERAGE)
    return notional

# ── Backtest Single Symbol — Option A ─────────────────────────────────────────
def backtest_a(symbol, candles, tp, sl, max_bars):
    if len(candles) < 100: return []
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    ts_arr = [c[0] for c in candles]

    ema9  = ema_series(closes, 9)   # list, same length, no Nones
    ema21 = ema_series(closes, 21)
    ema50 = ema_series(closes, 50)
    adx   = adx_series(highs, lows, closes, 14)  # list with Nones at start

    trades = []
    n = len(closes)
    notional = position_size(sl)
    i = 0
    while i < n - 1:
        sig = signal_gmaxv1(i, opens, highs, lows, closes,
                             ema9, ema21, ema50, adx)
        if sig is None:
            i += 1; continue
        entry_open = opens[i + 1]
        if sig == 'buy':
            ep = entry_open * (1 + FEE + SLIP)
            tp_price = ep * (1 + tp)
            sl_price = ep * (1 - sl)
        else:
            ep = entry_open * (1 - FEE - SLIP)
            tp_price = ep * (1 - tp)
            sl_price = ep * (1 + sl)

        entry_ts = ts_arr[i + 1]
        exit_i, exit_price, reason = i + 1, ep, 'max_hold'
        for j in range(i + 2, min(i + 2 + max_bars, n)):
            h, l = highs[j], lows[j]
            if sig == 'buy':
                if l <= sl_price:
                    exit_price, reason, exit_i = sl_price, 'sl', j; break
                if h >= tp_price:
                    exit_price, reason, exit_i = tp_price, 'tp', j; break
            else:
                if h >= sl_price:
                    exit_price, reason, exit_i = sl_price, 'sl', j; break
                if l <= tp_price:
                    exit_price, reason, exit_i = tp_price, 'tp', j; break
        else:
            exit_i = min(i + 1 + max_bars, n - 1)
            exit_price = closes[exit_i]
            reason = 'max_hold' if exit_i < n - 1 else 'end_of_data'

        if sig == 'buy':
            net = (exit_price - ep) / ep - (FEE + SLIP)
        else:
            net = (ep - exit_price) / ep - (FEE + SLIP)
        pnl = notional * net

        trades.append({
            'symbol':      symbol,
            'side':        sig,
            'entry_ts':    entry_ts,
            'exit_ts':     ts_arr[exit_i],
            'entry_price': ep,
            'exit_price':  exit_price,
            'pnl':         pnl,
            'reason':      reason,
            'bars':        exit_i - (i + 1),
        })
        i = exit_i + 1
    return trades

# ── Backtest Single Symbol — Option B ─────────────────────────────────────────
def backtest_b(symbol, candles, cfg):
    if len(candles) < 50: return []
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    ts_arr = [c[0] for c in candles]

    rsi_vals               = rsi_series(closes, cfg['rsi_period'])
    upper_bb, _, lower_bb  = bb_series(closes, cfg['bb_period'], cfg['bb_std'])

    tp, sl, max_bars = cfg['tp'], cfg['sl'], cfg['max_bars']
    notional = position_size(sl)
    trades = []
    n = len(closes)
    i = 0
    while i < n - 1:
        sig = signal_bb_rsi(i, closes, upper_bb, lower_bb, rsi_vals, cfg)
        if sig is None:
            i += 1; continue
        entry_open = opens[i + 1]
        if sig == 'buy':
            ep = entry_open * (1 + FEE + SLIP)
            tp_price = ep * (1 + tp)
            sl_price = ep * (1 - sl)
        else:
            ep = entry_open * (1 - FEE - SLIP)
            tp_price = ep * (1 - tp)
            sl_price = ep * (1 + sl)

        entry_ts = ts_arr[i + 1]
        exit_i, exit_price, reason = i + 1, ep, 'max_hold'
        for j in range(i + 2, min(i + 2 + max_bars, n)):
            h, l = highs[j], lows[j]
            if sig == 'buy':
                if l <= sl_price:
                    exit_price, reason, exit_i = sl_price, 'sl', j; break
                if h >= tp_price:
                    exit_price, reason, exit_i = tp_price, 'tp', j; break
            else:
                if h >= sl_price:
                    exit_price, reason, exit_i = sl_price, 'sl', j; break
                if l <= tp_price:
                    exit_price, reason, exit_i = tp_price, 'tp', j; break
        else:
            exit_i = min(i + 1 + max_bars, n - 1)
            exit_price = closes[exit_i]
            reason = 'max_hold' if exit_i < n - 1 else 'end_of_data'

        if sig == 'buy':
            net = (exit_price - ep) / ep - (FEE + SLIP)
        else:
            net = (ep - exit_price) / ep - (FEE + SLIP)
        pnl = notional * net

        trades.append({
            'symbol':      symbol,
            'side':        sig,
            'entry_ts':    entry_ts,
            'exit_ts':     ts_arr[exit_i],
            'entry_price': ep,
            'exit_price':  exit_price,
            'pnl':         pnl,
            'reason':      reason,
            'bars':        exit_i - (i + 1),
        })
        i = exit_i + 1
    return trades

# ── Stats ──────────────────────────────────────────────────────────────────────
def stats(trades, tf_minutes=5):
    if not trades:
        return {'total': 0, 'win_rate': 0, 'profit_factor': 0,
                'net_pnl': 0, 'max_drawdown': 0, 'avg_win': 0,
                'avg_loss': 0, 'expectancy': 0, 'sharpe': 0,
                'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {}}
    wins   = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] <= 0]
    gross_win  = sum(wins)
    gross_loss = abs(sum(losses))
    pf  = gross_win / gross_loss if gross_loss > 0 else float('inf')
    wr  = len(wins) / len(trades) * 100
    aw  = sum(wins)   / len(wins)   if wins   else 0
    al  = sum(losses) / len(losses) if losses else 0
    exp = (wr/100 * aw) + ((1 - wr/100) * al)

    # Max drawdown
    equity = 0.0
    peak   = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x['entry_ts']):
        equity += t['pnl']
        if equity > peak: peak = equity
        dd = peak - equity
        if dd > max_dd: max_dd = dd

    # Sharpe — daily PnL buckets
    daily = defaultdict(float)
    for t in trades:
        day = t['entry_ts'] // (86400 * 1000)
        daily[day] += t['pnl']
    daily_vals = list(daily.values())
    if len(daily_vals) > 1:
        mean_d = sum(daily_vals) / len(daily_vals)
        var_d  = sum((x - mean_d) ** 2 for x in daily_vals) / len(daily_vals)
        std_d  = math.sqrt(var_d)
        sharpe = (mean_d / std_d * math.sqrt(365)) if std_d > 0 else 0
    else:
        sharpe = 0

    # Monthly
    monthly = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    for t in trades:
        from datetime import datetime
        dt  = datetime.utcfromtimestamp(t['entry_ts'] / 1000)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        if t['pnl'] > 0: monthly[key]['w'] += 1

    # Per-coin
    per_coin = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0})
    for t in trades:
        s = t['symbol']
        per_coin[s]['pnl'] += t['pnl']
        per_coin[s]['n']   += 1
        if t['pnl'] > 0: per_coin[s]['w'] += 1
    for s in per_coin:
        n_ = per_coin[s]['n']
        per_coin[s]['wr'] = per_coin[s]['w'] / n_ * 100 if n_ > 0 else 0

    return {
        'total':         len(trades),
        'win_rate':      round(wr, 2),
        'profit_factor': round(pf, 4),
        'net_pnl':       round(sum(t['pnl'] for t in trades), 4),
        'max_drawdown':  round(max_dd, 4),
        'avg_win':       round(aw, 4),
        'avg_loss':      round(al, 4),
        'expectancy':    round(exp, 4),
        'sharpe':        round(sharpe, 4),
        'longs':         sum(1 for t in trades if t['side'] == 'buy'),
        'shorts':        sum(1 for t in trades if t['side'] == 'sell'),
        'monthly':       {k: dict(v) for k, v in sorted(monthly.items())},
        'per_coin':      {k: dict(v) for k, v in per_coin.items()},
    }

# ── Shard Runner ───────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    t0 = time.time()
    my_symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(my_symbols)} coins")

    # Fetch all needed timeframes
    needed_tfs = set()
    for label, tf, tp, sl, mb in OPTION_A_CONFIGS:
        needed_tfs.add(tf)
    needed_tfs.add(OPTION_B_CONFIG['tf'])

    # Fetch candles per (symbol, tf)
    candle_cache = {}   # (symbol, tf) -> list of candles

    def fetch_task(symbol, tf):
        return (symbol, tf, fetch_symbol(symbol, tf))

    tasks = [(sym, tf) for sym in my_symbols for tf in needed_tfs]
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_task, sym, tf): (sym, tf) for sym, tf in tasks}
        for fut in as_completed(futs):
            sym, tf, candles = fut.result()
            candle_cache[(sym, tf)] = candles
            if candles:
                print(f"  [{shard_idx}] {sym} {tf}: {len(candles)} bars")
            else:
                print(f"  [{shard_idx}] {sym} {tf}: NO DATA")

    # Run all variants
    # Structure: { variant_label: { symbol: [trades] } }
    variant_trades = defaultdict(list)

    for sym in my_symbols:
        # Option A variants
        for (label, tf, tp, sl, mb) in OPTION_A_CONFIGS:
            candles = candle_cache.get((sym, tf), [])
            if not candles: continue
            t = backtest_a(sym, candles, tp, sl, mb)
            variant_trades[label].extend(t)

        # Option B
        b_tf = OPTION_B_CONFIG['tf']
        candles = candle_cache.get((sym, b_tf), [])
        if candles:
            t = backtest_b(sym, candles, OPTION_B_CONFIG)
            variant_trades[OPTION_B_CONFIG['label']].extend(t)

    # Compute per-variant stats for this shard
    variant_stats = {}
    for label, trades in variant_trades.items():
        tf_min = 5 if '5m' in label else 15
        variant_stats[label] = stats(trades, tf_min)

    shard_data = {
        'shard':          shard_idx,
        'symbols':        my_symbols,
        'variant_trades': {k: v for k, v in variant_trades.items()},
        'variant_stats':  variant_stats,
        'elapsed':        round(time.time() - t0, 2),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(shard_data, f)
    print(f"[Shard {shard_idx}] Done in {shard_data['elapsed']}s")

# ── Merge ──────────────────────────────────────────────────────────────────────
def merge_shards():
    all_variant_trades = defaultdict(list)

    for idx in range(NUM_SHARDS):
        fname = f'shard_{idx}.json'
        if not os.path.exists(fname):
            print(f"WARNING: {fname} missing"); continue
        with open(fname) as f:
            d = json.load(f)
        for label, trades in d['variant_trades'].items():
            all_variant_trades[label].extend(trades)

    # All variant labels in order
    all_labels = [cfg[0] for cfg in OPTION_A_CONFIGS] + [OPTION_B_CONFIG['label']]

    # Compute final stats per variant
    final = {}
    for label in all_labels:
        trades = all_variant_trades.get(label, [])
        tf_min = 5 if '5m' in label else 15
        final[label] = {
            'stats':  stats(trades, tf_min),
            'trades': len(trades),
        }

    # Write JSON report
    report = {
        'period':    f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'coins':     len(ALL_SYMBOLS),
        'capital':   CAPITAL,
        'leverage':  LEVERAGE,
        'risk_pct':  RISK_PCT,
        'fee':       FEE,
        'slip':      SLIP,
        'variants':  final,
    }
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # Write summary txt
    PASS = lambda s: '✅ PASS' if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42 else '❌ FAIL'

    lines = []
    lines.append("=" * 72)
    lines.append("  MICRO-PROFIT BACKTEST — Options A & B")
    lines.append(f"  Period  : {report['period']}  |  Coins: {report['coins']}")
    lines.append(f"  Capital : ${CAPITAL:,.0f}  |  Leverage: {LEVERAGE}x  |  Risk/trade: {RISK_PCT*100:.2f}%")
    lines.append(f"  Fees    : {FEE*100:.2f}% + {SLIP*100:.2f}% slip (both sides)")
    lines.append("=" * 72)
    lines.append("")
    lines.append(f"{'Variant':<22} {'Trades':>7} {'WR%':>7} {'PF':>7} {'Sharpe':>8} {'MaxDD$':>9} {'NetPnL$':>10} {'Expect$':>9}  Result")
    lines.append("-" * 100)

    for label in all_labels:
        if label not in final: continue
        s = final[label]['stats']
        n = final[label]['trades']
        lines.append(
            f"{label:<22} {n:>7} {s['win_rate']:>7.1f} {s['profit_factor']:>7.3f} "
            f"{s['sharpe']:>8.3f} {s['max_drawdown']:>9.2f} {s['net_pnl']:>10.2f} "
            f"{s['expectancy']:>9.4f}  {PASS(s)}"
        )

    lines.append("")
    lines.append("─" * 72)
    lines.append("OPTION A — GMaxV1 Signal (EMA9/21 + EMA50 slope + ADX>=22)")
    lines.append("  15m timeframe: 5 TP/SL combos  |  5m timeframe: 5 TP/SL combos")
    lines.append("  Baseline: A_15m_TP3.0_SL15 = original GMaxV1 parameters")
    lines.append("")
    lines.append("OPTION B — Bollinger Band(20,2) + RSI(14) Mean Reversion on 5m")
    lines.append("  Long: close < lower_BB AND RSI < 35")
    lines.append("  Short: close > upper_BB AND RSI > 65")
    lines.append("  TP=0.8% / SL=2.5% / Max hold=72 bars (6h)")
    lines.append("─" * 72)
    lines.append("")

    # Per-coin top 20 for each passing variant
    for label in all_labels:
        if label not in final: continue
        s = final[label]['stats']
        if not s['per_coin']: continue
        ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)[:20]
        lines.append(f"  Top coins — {label}:")
        for sym, d in ranked:
            lines.append(f"    {sym:<22} trades={d['n']:>4}  wr={d['wr']:>5.1f}%  pnl=${d['pnl']:>9.2f}")
        lines.append("")

    lines.append("=" * 72)
    txt = "\n".join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(txt)
    print(txt)
    print("\n✅ Merge complete — backtest_report.json + backtest_summary.txt written")

# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>"); sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

