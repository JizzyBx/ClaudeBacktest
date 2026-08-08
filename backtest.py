"""
8-Strategy Exploration Backtest — GitHub Actions Pipeline
117-coin universe (from GMax bot), 5x leverage, 20 parallel shards.
All 8 strategies are tested against the same fetched candle data per coin.
stdlib only. No pip installs.
"""

import sys, os, json, csv, io, zipfile, urllib.request, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Coin Universe (117 coins, from GMax COINS_UNIVERSE) ──────────────
ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT','1000SATSUSDT',
    'A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT','AIOTUSDT','ALGOUSDT','ALICEUSDT',
    'ALPINEUSDT','ANKRUSDT','ARKMUSDT','ASRUSDT','ASTERUSDT','AUSDT','AWEUSDT',
    'BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT','BMTUSDT','BTRUSDT','CFXUSDT',
    'CHIPUSDT','COAIUSDT','COMBOUSDT','COMMONUSDT','CRCLUSDT','CUSDT','DAMUSDT',
    'DEFIUSDT','DEXEUSDT','DIAUSDT','DMCUSDT','EIGENUSDT','ELSAUSDT','ENAUSDT',
    'EPICUSDT','EPTUSDT','ETHUSDT','EVAAUSDT','FLNCUSDT','FLUXUSDT','FUNUSDT',
    'FXSUSDT','GLMUSDT','GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT',
    'INITUSDT','IOUSDT','IPUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT',
    'LYNUSDT','MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','MTLUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT',
    'PEOPLEUSDT','PIPPINUSDT','PIXELUSDT','PLUMEUSDT','POLUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','PUNDIXUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT','SEIUSDT',
    'SIGNUSDT','SKRUSDT','SNDKUSDT','SOMIUSDT','SPELLUSDT','SPKUSDT','STABLEUSDT',
    'STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT','USUALUSDT','VANRYUSDT','VINEUSDT',
    'VIRTUALUSDT','VVVUSDT','WLDUSDT','XEMUSDT','XLMUSDT','XRPUSDT','YBUSDT',
    'ZECUSDT','ZEREBROUSDT',
]

NUM_SHARDS = 20
WORKERS = 16

# ── Config ─────────────────────────────────────────────────────────
START_YM = (2024, 8)
END_YM   = (2026, 7)
CAPITAL   = 10000.0
RISK_PCT  = 0.0075       # 0.75% risk per trade
FEE       = 0.0005       # 0.05%
SLIP      = 0.0002       # 0.02%
LEVERAGE  = 5             # per user instruction: minimum 5x
MIN_BARS  = 100           # warmup bars before any signal can fire

TIMEFRAMES_NEEDED = ['15m', '1h']  # union of all TFs used by the 8 strategies

# ── Strategy Definitions ──────────────────────────────────────────
# Each strategy declares: id, name, primary tf, secondary tf (or None), tp/sl pct
STRATEGIES = {
    'S1_EMA_RIBBON':   {'name': 'EMA Ribbon Trend + ADX',        'tf': '15m', 'tf2': None,  'tp': 0.035, 'sl': 0.14},
    'S2_FVG_SWEEP':    {'name': 'FVG + Liquidity Sweep',         'tf': '15m', 'tf2': None,  'tp': 0.03,  'sl': 0.12},
    'S3_SR_PINBAR':    {'name': 'S/R Zone + Pin Bar Rejection',  'tf': '1h',  'tf2': None,  'tp': 0.04,  'sl': 0.15},
    'S4_BB_MEANREV':   {'name': 'BB Mean-Reversion (regime)',    'tf': '15m', 'tf2': None,  'tp': 0.025, 'sl': 0.10},
    'S5_DONCHIAN_VOL': {'name': 'Donchian Breakout + Volume',    'tf': '1h',  'tf2': None,  'tp': 0.05,  'sl': 0.16},
    'S6_VWAP_REV':     {'name': 'VWAP Session Reversion',        'tf': '15m', 'tf2': None,  'tp': 0.02,  'sl': 0.09},
    'S7_VOL_SQUEEZE':  {'name': 'Volatility Squeeze Breakout',   'tf': '1h',  'tf2': None,  'tp': 0.045, 'sl': 0.15},
    'S8_MTF_CONFLUENCE':{'name': 'MTF Confluence (1h bias+15m)', 'tf': '15m', 'tf2': '1h',  'tp': 0.03,  'sl': 0.13},
}

MAX_BARS_BY_TF = {'15m': 60, '1h': 40}   # max hold bars before force-close

# ── Data fetch (Binance Vision monthly archives) ──────────────────
def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out

def fetch_month(symbol, tf, year, month):
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines/"
           f"{symbol}/{tf}/{symbol}-{tf}-{year:04d}-{month:02d}.zip")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read()
        zf = zipfile.ZipFile(io.BytesIO(data))
        name = zf.namelist()[0]
        rows = []
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding='utf-8')
            reader = csv.reader(text)
            for r in reader:
                if not r or r[0] in ('open_time', ''):
                    continue
                try:
                    ts = int(r[0])
                    if ts > 10**14:
                        ts //= 1000
                    o, h, l, c = float(r[1]), float(r[2]), float(r[3]), float(r[4])
                    vol = float(r[5])
                    rows.append((ts, o, h, l, c, vol))
                except (ValueError, IndexError):
                    continue
        return rows
    except Exception:
        return []

def fetch_symbol_tf(symbol, tf):
    months = month_range(START_YM, END_YM)
    all_rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_month, symbol, tf, y, m): (y, m) for (y, m) in months}
        for fut in as_completed(futs):
            rows = fut.result()
            if rows:
                all_rows.extend(rows)
    if not all_rows:
        return None
    dedup = {}
    for row in all_rows:
        dedup[row[0]] = row
    sorted_rows = sorted(dedup.values(), key=lambda r: r[0])
    return sorted_rows

def fetch_symbol_all_tfs(symbol):
    out = {}
    for tf in TIMEFRAMES_NEEDED:
        rows = fetch_symbol_tf(symbol, tf)
        if rows:
            out[tf] = rows
    return out

# ── Indicators (pure python) ───────────────────────────────────────
def ema_series(closes, period):
    if len(closes) < period:
        return [None] * len(closes)
    out = [None] * (period - 1)
    k = 2.0 / (period + 1)
    sma = sum(closes[:period]) / period
    out.append(sma)
    prev = sma
    for c in closes[period:]:
        val = c * k + prev * (1 - k)
        out.append(val)
        prev = val
    return out

def sma_series(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        out[i] = sum(values[i - period + 1:i + 1]) / period
    return out

def stdev_series(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        m = sum(window) / period
        var = sum((x - m) ** 2 for x in window) / period
        out[i] = var ** 0.5
    return out

def rsi_series(closes, period=14):
    n = len(closes)
    out = [None] * n
    if n <= period:
        return out
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    out[period] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0); loss = max(-d, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
    return out

def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    if n <= period * 2:
        return out
    tr, pdm, mdm = [0.0], [0.0], [0.0]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        dn = lows[i - 1] - lows[i]
        pdm.append(up if (up > dn and up > 0) else 0.0)
        mdm.append(dn if (dn > up and dn > 0) else 0.0)
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr = sum(tr[1:period + 1]) / period
    pdi_s = sum(pdm[1:period + 1])
    mdi_s = sum(mdm[1:period + 1])
    dx_list = []
    for i in range(period + 1, n):
        atr = (atr * (period - 1) + tr[i]) / period
        pdi_s = (pdi_s * (period - 1) + pdm[i])
        mdi_s = (mdi_s * (period - 1) + mdm[i])
        pdi = 100 * (pdi_s / period) / atr if atr > 0 else 0
        mdi = 100 * (mdi_s / period) / atr if atr > 0 else 0
        dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 0
        dx_list.append(dx)
        if len(dx_list) == period:
            out[i] = sum(dx_list) / period
        elif len(dx_list) > period:
            out[i] = (out[i - 1] * (period - 1) + dx) / period
    return out

def bollinger(closes, period=20, mult=2.0):
    mid = sma_series(closes, period)
    sd = stdev_series(closes, period)
    upper = [None if mid[i] is None else mid[i] + mult * sd[i] for i in range(len(closes))]
    lower = [None if mid[i] is None else mid[i] - mult * sd[i] for i in range(len(closes))]
    width = [None if mid[i] is None else (upper[i] - lower[i]) / mid[i] for i in range(len(closes))]
    return mid, upper, lower, width

def donchian(highs, lows, period=20):
    n = len(highs)
    upper = [None] * n
    lower = [None] * n
    for i in range(period, n):
        upper[i] = max(highs[i - period:i])
        lower[i] = min(lows[i - period:i])
    return upper, lower

def rolling_vwap(closes, highs, lows, vols, period=96):
    # session-anchored approximation: rolling VWAP over `period` bars (96 x 15m = 24h)
    n = len(closes)
    out = [None] * n
    for i in range(period, n):
        tp_vol = 0.0
        vol_sum = 0.0
        for j in range(i - period, i):
            tp = (highs[j] + lows[j] + closes[j]) / 3.0
            tp_vol += tp * vols[j]
            vol_sum += vols[j]
        out[i] = tp_vol / vol_sum if vol_sum > 0 else None
    return out

def percentile_rank(values, idx, lookback=200):
    if idx < lookback:
        return None
    window = [v for v in values[idx - lookback:idx] if v is not None]
    if not window or values[idx] is None:
        return None
    below = sum(1 for v in window if v <= values[idx])
    return below / len(window) * 100

# ── Signal functions — each returns 'buy' / 'sell' / None on bar i (closed bar) ──

def sig_ema_ribbon(ctx, i):
    e9, e21, e50, adx = ctx['e9'], ctx['e21'], ctx['e50'], ctx['adx']
    if None in (e9[i], e21[i], e50[i], adx[i], e9[i-1], e21[i-1]):
        return None
    if adx[i] < 22:
        return None
    slope = (e50[i] - e50[i-5]) / e50[i-5] * 100 if i >= 5 and e50[i-5] else 0
    crossed_up = e9[i-1] <= e21[i-1] and e9[i] > e21[i]
    crossed_dn = e9[i-1] >= e21[i-1] and e9[i] < e21[i]
    if crossed_up and slope > 0:
        return 'buy'
    if crossed_dn and slope < 0:
        return 'sell'
    return None

def sig_fvg_sweep(ctx, i):
    highs, lows, closes = ctx['highs'], ctx['lows'], ctx['closes']
    e21 = ctx['e21']
    if i < 25 or e21[i] is None:
        return None
    lookback_high = max(highs[i-20:i-2])
    lookback_low = min(lows[i-20:i-2])
    swept_high = highs[i-1] > lookback_high and closes[i-1] < lookback_high
    swept_low = lows[i-1] < lookback_low and closes[i-1] > lookback_low
    # simple 3-candle FVG: gap between candle i-2 high and candle i low (bullish) etc.
    bull_fvg = lows[i] > highs[i-2]
    bear_fvg = highs[i] < lows[i-2]
    if swept_low and bull_fvg and closes[i] > e21[i]:
        return 'buy'
    if swept_high and bear_fvg and closes[i] < e21[i]:
        return 'sell'
    return None

def sig_sr_pinbar(ctx, i):
    highs, lows, closes, opens = ctx['highs'], ctx['lows'], ctx['closes'], ctx['opens']
    e50 = ctx['e50']
    if i < 30 or e50[i] is None:
        return None
    zone_high = max(highs[i-20:i])
    zone_low = min(lows[i-20:i])
    body = abs(closes[i] - opens[i])
    rng = highs[i] - lows[i]
    if rng == 0:
        return None
    lower_wick = min(opens[i], closes[i]) - lows[i]
    upper_wick = highs[i] - max(opens[i], closes[i])
    is_pin_bull = lower_wick > body * 2 and lower_wick > rng * 0.55
    is_pin_bear = upper_wick > body * 2 and upper_wick > rng * 0.55
    near_support = lows[i] <= zone_low * 1.01
    near_resistance = highs[i] >= zone_high * 0.99
    if is_pin_bull and near_support and closes[i] > e50[i]:
        return 'buy'
    if is_pin_bear and near_resistance and closes[i] < e50[i]:
        return 'sell'
    return None

def sig_bb_meanrev(ctx, i):
    closes = ctx['closes']
    bb_low, bb_up, rsi, adx = ctx['bb_low'], ctx['bb_up'], ctx['rsi'], ctx['adx']
    if None in (bb_low[i], bb_up[i], rsi[i], adx[i]):
        return None
    if adx[i] >= 20:   # only trade ranges
        return None
    if closes[i] <= bb_low[i] and rsi[i] < 30:
        return 'buy'
    if closes[i] >= bb_up[i] and rsi[i] > 70:
        return 'sell'
    return None

def sig_donchian_vol(ctx, i):
    closes, vols = ctx['closes'], ctx['vols']
    dc_up, dc_low = ctx['dc_up'], ctx['dc_low']
    if i < 25 or dc_up[i-1] is None or dc_low[i-1] is None:
        return None
    avg_vol = sum(vols[i-20:i]) / 20
    vol_spike = vols[i] > avg_vol * 1.5
    if closes[i] > dc_up[i-1] and vol_spike:
        return 'buy'
    if closes[i] < dc_low[i-1] and vol_spike:
        return 'sell'
    return None

def sig_vwap_rev(ctx, i):
    closes = ctx['closes']
    vwap = ctx['vwap']
    if vwap[i] is None:
        return None
    dev = (closes[i] - vwap[i]) / vwap[i] * 100
    if dev < -2.0:
        return 'buy'
    if dev > 2.0:
        return 'sell'
    return None

def sig_vol_squeeze(ctx, i):
    closes = ctx['closes']
    bb_up, bb_low, bb_width = ctx['bb_up'], ctx['bb_low'], ctx['bb_width']
    if i < 210 or bb_width[i-1] is None:
        return None
    pct = percentile_rank(bb_width, i - 1, lookback=200)
    if pct is None or pct > 10:
        return None
    if closes[i] > bb_up[i]:
        return 'buy'
    if closes[i] < bb_low[i]:
        return 'sell'
    return None

def sig_mtf_confluence(ctx, i, htf_ctx, htf_idx):
    e9, e21 = ctx['e9'], ctx['e21']
    if htf_idx is None or htf_idx < 1:
        return None
    htf_e9, htf_e21 = htf_ctx['e9'], htf_ctx['e21']
    if None in (e9[i], e21[i], e9[i-1], e21[i-1], htf_e9[htf_idx], htf_e21[htf_idx]):
        return None
    htf_bull = htf_e9[htf_idx] > htf_e21[htf_idx]
    htf_bear = htf_e9[htf_idx] < htf_e21[htf_idx]
    crossed_up = e9[i-1] <= e21[i-1] and e9[i] > e21[i]
    crossed_dn = e9[i-1] >= e21[i-1] and e9[i] < e21[i]
    if crossed_up and htf_bull:
        return 'buy'
    if crossed_dn and htf_bear:
        return 'sell'
    return None

# ── Build indicator context for a candle series ────────────────────
def build_ctx(rows):
    closes = [r[4] for r in rows]
    highs  = [r[2] for r in rows]
    lows   = [r[3] for r in rows]
    opens  = [r[1] for r in rows]
    vols   = [r[5] for r in rows]
    ts     = [r[0] for r in rows]
    e9  = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)
    adx = adx_series(highs, lows, closes, 14)
    rsi = rsi_series(closes, 14)
    bb_mid, bb_up, bb_low, bb_width = bollinger(closes, 20, 2.0)
    dc_up, dc_low = donchian(highs, lows, 20)
    vwap = rolling_vwap(closes, highs, lows, vols, period=96)
    return {
        'ts': ts, 'closes': closes, 'highs': highs, 'lows': lows, 'opens': opens, 'vols': vols,
        'e9': e9, 'e21': e21, 'e50': e50, 'adx': adx, 'rsi': rsi,
        'bb_up': bb_up, 'bb_low': bb_low, 'bb_width': bb_width,
        'dc_up': dc_up, 'dc_low': dc_low, 'vwap': vwap,
    }

def find_htf_index(htf_ts, target_ts):
    # binary search for the last htf bar with ts <= target_ts
    lo, hi = 0, len(htf_ts) - 1
    res = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if htf_ts[mid] <= target_ts:
            res = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return res

# ── Backtest a single strategy against a single symbol's candles ──
def backtest_strategy(strat_id, symbol, ctx, htf_ctx=None):
    cfg = STRATEGIES[strat_id]
    tp_pct, sl_pct = cfg['tp'], cfg['sl']
    tf = cfg['tf']
    max_bars = MAX_BARS_BY_TF[tf]
    closes, highs, lows, opens = ctx['closes'], ctx['highs'], ctx['lows'], ctx['opens']
    ts = ctx['ts']
    n = len(closes)
    trades = []
    if n < MIN_BARS + 5:
        return trades

    i = MIN_BARS
    while i < n - 1:
        if strat_id == 'S1_EMA_RIBBON':
            sig = sig_ema_ribbon(ctx, i)
        elif strat_id == 'S2_FVG_SWEEP':
            sig = sig_fvg_sweep(ctx, i)
        elif strat_id == 'S3_SR_PINBAR':
            sig = sig_sr_pinbar(ctx, i)
        elif strat_id == 'S4_BB_MEANREV':
            sig = sig_bb_meanrev(ctx, i)
        elif strat_id == 'S5_DONCHIAN_VOL':
            sig = sig_donchian_vol(ctx, i)
        elif strat_id == 'S6_VWAP_REV':
            sig = sig_vwap_rev(ctx, i)
        elif strat_id == 'S7_VOL_SQUEEZE':
            sig = sig_vol_squeeze(ctx, i)
        elif strat_id == 'S8_MTF_CONFLUENCE':
            htf_idx = find_htf_index(htf_ctx['ts'], ts[i]) if htf_ctx else None
            sig = sig_mtf_confluence(ctx, i, htf_ctx, htf_idx)
        else:
            sig = None

        if sig is None:
            i += 1
            continue

        entry_i = i + 1
        if entry_i >= n:
            break
        if sig == 'buy':
            entry_p = opens[entry_i] * (1 + FEE + SLIP)
            tp_p = entry_p * (1 + tp_pct)
            sl_p = entry_p * (1 - sl_pct)
        else:
            entry_p = opens[entry_i] * (1 - FEE - SLIP)
            tp_p = entry_p * (1 - tp_pct)
            sl_p = entry_p * (1 + sl_pct)

        exit_p, reason, bars_held, exit_ts = None, None, 0, ts[entry_i]
        j = entry_i
        while j < n:
            bars_held = j - entry_i
            if bars_held >= max_bars:
                exit_p, reason, exit_ts = closes[j], 'max_hold', ts[j]
                break
            if sig == 'buy':
                if lows[j] <= sl_p:
                    exit_p, reason, exit_ts = sl_p, 'sl', ts[j]
                    break
                if highs[j] >= tp_p:
                    exit_p, reason, exit_ts = tp_p, 'tp', ts[j]
                    break
            else:
                if highs[j] >= sl_p:
                    exit_p, reason, exit_ts = sl_p, 'sl', ts[j]
                    break
                if lows[j] <= tp_p:
                    exit_p, reason, exit_ts = tp_p, 'tp', ts[j]
                    break
            j += 1
        else:
            exit_p, reason, exit_ts = closes[n-1], 'end_of_data', ts[n-1]
            bars_held = n - 1 - entry_i

        notional = min(CAPITAL * RISK_PCT / sl_pct, CAPITAL * LEVERAGE)
        if sig == 'buy':
            gross = (exit_p - entry_p) / entry_p
        else:
            gross = (entry_p - exit_p) / entry_p
        net = gross - (FEE + SLIP) * 2
        pnl = notional * net

        trades.append({
            'symbol': symbol, 'side': sig, 'entry_ts': ts[entry_i], 'exit_ts': exit_ts,
            'entry_price': entry_p, 'exit_price': exit_p, 'pnl': pnl,
            'reason': reason, 'bars': bars_held,
        })
        i = entry_i + bars_held + 1

    return trades

# ── Stats ────────────────────────────────────────────────────────
def compute_stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
            'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }
    total = len(trades)
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    win_rate = len(wins) / total * 100
    gross_win = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else (float('inf') if gross_win > 0 else 0.0)
    net_pnl = sum(t['pnl'] for t in trades)
    avg_win = gross_win / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    expectancy = net_pnl / total

    sorted_trades = sorted(trades, key=lambda t: t['exit_ts'])
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for t in sorted_trades:
        equity += t['pnl']
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    monthly = {}
    for t in trades:
        dt = datetime.fromtimestamp(t['exit_ts'], tz=timezone.utc)
        key = f"{dt.year:04d}-{dt.month:02d}"
        m = monthly.setdefault(key, {'pnl': 0.0, 'n': 0, 'w': 0})
        m['pnl'] += t['pnl']; m['n'] += 1
        if t['pnl'] > 0: m['w'] += 1

    per_coin = {}
    for t in trades:
        c = per_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0})
        c['pnl'] += t['pnl']; c['n'] += 1
        if t['pnl'] > 0: c['w'] += 1
    for c in per_coin.values():
        c['wr'] = c['w'] / c['n'] * 100 if c['n'] else 0.0

    return {
        'total': total, 'win_rate': round(win_rate, 2), 'profit_factor': round(pf, 3) if pf != float('inf') else 999.0,
        'net_pnl': round(net_pnl, 2), 'max_drawdown': round(max_dd, 2),
        'avg_win': round(avg_win, 2), 'avg_loss': round(avg_loss, 2), 'expectancy': round(expectancy, 3),
        'longs': sum(1 for t in trades if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades if t['side'] == 'sell'),
        'monthly': monthly, 'per_coin': per_coin,
    }

# ── Shard runner ────────────────────────────────────────────────
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    t0 = time.time()
    all_trades = {sid: [] for sid in STRATEGIES}
    with_data = []

    for symbol in symbols:
        tfs = fetch_symbol_all_tfs(symbol)
        if '15m' not in tfs and '1h' not in tfs:
            continue
        with_data.append(symbol)

        ctx_15m = build_ctx(tfs['15m']) if '15m' in tfs else None
        ctx_1h  = build_ctx(tfs['1h']) if '1h' in tfs else None

        for strat_id, cfg in STRATEGIES.items():
            tf = cfg['tf']
            ctx = ctx_15m if tf == '15m' else ctx_1h
            if ctx is None:
                continue
            htf_ctx = None
            if strat_id == 'S8_MTF_CONFLUENCE':
                htf_ctx = ctx_1h
                if htf_ctx is None:
                    continue
            trades = backtest_strategy(strat_id, symbol, ctx, htf_ctx)
            all_trades[strat_id].extend(trades)

    out = {
        'shard': shard_idx,
        'symbols': symbols,
        'with_data': with_data,
        'trades_by_strategy': all_trades,
        'elapsed': round(time.time() - t0, 1),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(out, f)
    print(f"Shard {shard_idx} done: {len(with_data)}/{len(symbols)} coins with data, "
          f"{sum(len(v) for v in all_trades.values())} total trades, {out['elapsed']}s")

# ── Merge ───────────────────────────────────────────────────────
def merge_shards():
    combined_trades = {sid: [] for sid in STRATEGIES}
    all_symbols_attempted = []
    all_with_data = []

    for idx in range(NUM_SHARDS):
        fname = f'shard_{idx}.json'
        if not os.path.exists(fname):
            print(f"WARNING: {fname} missing, skipping")
            continue
        with open(fname) as f:
            data = json.load(f)
        all_symbols_attempted.extend(data['symbols'])
        all_with_data.extend(data['with_data'])
        for sid, trades in data['trades_by_strategy'].items():
            combined_trades[sid].extend(trades)

    if not all_with_data:
        with open('backtest_summary.txt', 'w') as f:
            f.write("ERROR: 0 symbols returned data across all shards.\n"
                    "Likely geo-block on Binance Vision or network issue in Actions runner.\n")
        with open('backtest_report.json', 'w') as f:
            json.dump({'error': 'no_data'}, f)
        return

    report = {
        'period': f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'symbols_attempted': len(set(all_symbols_attempted)),
        'symbols_with_data': len(set(all_with_data)),
        'leverage': LEVERAGE,
        'capital': CAPITAL,
        'strategies': {}
    }

    summary_lines = []
    summary_lines.append("=" * 70)
    summary_lines.append("8-STRATEGY EXPLORATION BACKTEST — SUMMARY REPORT")
    summary_lines.append("=" * 70)
    summary_lines.append(f"Period: {report['period']}")
    summary_lines.append(f"Symbols attempted: {report['symbols_attempted']}  |  With data: {report['symbols_with_data']}")
    summary_lines.append(f"Capital: ${CAPITAL:,.0f}  |  Leverage: {LEVERAGE}x  |  Risk/trade: {RISK_PCT*100:.2f}%")
    summary_lines.append(f"Fee: {FEE*100:.3f}%  |  Slippage: {SLIP*100:.3f}%")
    summary_lines.append("")

    ranked = []
    for sid, cfg in STRATEGIES.items():
        trades = combined_trades[sid]
        stats = compute_stats(trades)
        report['strategies'][sid] = {'name': cfg['name'], 'tf': cfg['tf'], 'tp': cfg['tp'], 'sl': cfg['sl'], 'stats': stats}
        ranked.append((sid, cfg, stats))

    # sort by profit factor then win rate for the leaderboard
    ranked_sorted = sorted(ranked, key=lambda x: (x[2]['profit_factor'], x[2]['win_rate']), reverse=True)

    summary_lines.append("-" * 70)
    summary_lines.append("LEADERBOARD (ranked by profit factor)")
    summary_lines.append("-" * 70)
    summary_lines.append(f"{'Strategy':<28}{'TF':<6}{'Trades':<8}{'WR%':<8}{'PF':<8}{'NetPnL':<12}{'MaxDD':<10}{'Verdict'}")
    for sid, cfg, stats in ranked_sorted:
        verdict = "USABLE" if (stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42 and stats['total'] >= 30) else "NOT USABLE"
        summary_lines.append(
            f"{cfg['name']:<28}{cfg['tf']:<6}{stats['total']:<8}{stats['win_rate']:<8}{stats['profit_factor']:<8}"
            f"{stats['net_pnl']:<12}{stats['max_drawdown']:<10}{verdict}"
        )
    summary_lines.append("")

    for sid, cfg, stats in ranked_sorted:
        summary_lines.append("=" * 70)
        summary_lines.append(f"{sid} — {cfg['name']}  (TF: {cfg['tf']}, TP: {cfg['tp']*100:.1f}%, SL: {cfg['sl']*100:.1f}%)")
        summary_lines.append("=" * 70)
        if stats['total'] == 0:
            summary_lines.append("No trades generated.")
            summary_lines.append("")
            continue
        summary_lines.append(f"Total trades: {stats['total']}  |  Win rate: {stats['win_rate']}%  |  Profit Factor: {stats['profit_factor']}")
        summary_lines.append(f"Net PnL: ${stats['net_pnl']:,.2f}  |  Max Drawdown: ${stats['max_drawdown']:,.2f}")
        summary_lines.append(f"Avg Win: ${stats['avg_win']:,.2f}  |  Avg Loss: ${stats['avg_loss']:,.2f}  |  Expectancy: ${stats['expectancy']:,.3f}")
        summary_lines.append(f"Longs: {stats['longs']}  |  Shorts: {stats['shorts']}")
        verdict = "✅ USABLE" if (stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42 and stats['total'] >= 30) else "❌ NOT USABLE"
        summary_lines.append(f"RECOMMENDATION: {verdict} (threshold: PF>=1.5, WR>=42%, trades>=30)")
        summary_lines.append("")
        top_coins = sorted(stats['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)[:15]
        summary_lines.append("Top 15 coins by net PnL:")
        for sym, c in top_coins:
            summary_lines.append(f"  {sym:<18}trades={c['n']:<5}wr={c['wr']:.1f}%   pnl=${c['pnl']:,.2f}")
        summary_lines.append("")
        summary_lines.append("Monthly PnL:")
        for month in sorted(stats['monthly'].keys()):
            md = stats['monthly'][month]
            summary_lines.append(f"  {month}:  pnl=${md['pnl']:,.2f}   trades={md['n']}   wins={md['w']}")
        summary_lines.append("")

    with open('backtest_summary.txt', 'w') as f:
        f.write("\n".join(summary_lines))
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f)

    print("Merge complete. backtest_summary.txt and backtest_report.json written.")

# ── Entry point ────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))
