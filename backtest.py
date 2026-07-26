"""
BACKTEST v6 — Kimi AI Strategy Enhancement
Tests: TEST0 (Baseline) through TEST6 (Full Stack)
Coins: BTCUSDT, ETHUSDT, SOLUSDT, DOGEUSDT, 1000PEPEUSDT, WIFUSDT
Period: 6 months of 15m candles + 1H candles for HTF filter
Data: Binance public API (data-api.binance.vision)
Stdlib only: json, math, statistics, urllib, datetime, time, collections
"""

import json
import math
import time
import datetime
import urllib.request
import urllib.error
import statistics
import collections

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
COINS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "DOGEUSDT",
    "1000PEPEUSDT",
    "WIFUSDT",
]

FEE_RATE    = 0.0005   # 0.05% per side taker fee
INITIAL_CAP = 100.0

# 6 months back from now
NOW_MS  = int(time.time() * 1000)
SIX_MO  = 6 * 30 * 24 * 60 * 60 * 1000   # approx 6 months in ms
START_MS = NOW_MS - SIX_MO

BASE_URL = "https://data-api.binance.vision/api/v3/klines"

# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def fetch_klines(symbol, interval, start_ms, end_ms):
    """Fetch all klines for a symbol/interval between start_ms and end_ms."""
    all_raw = []
    cur = start_ms
    retries_total = 0
    while cur < end_ms:
        url = (f"{BASE_URL}?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={end_ms}&limit=1000")
        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
                break
            except urllib.error.HTTPError as e:
                if e.code in (400, 451):
                    print(f"  [SKIP] {symbol} HTTP {e.code} — skipping")
                    return None
                wait = (2 ** attempt) * 0.5
                time.sleep(wait)
            except Exception:
                wait = (2 ** attempt) * 0.5
                time.sleep(wait)
        else:
            print(f"  [WARN] {symbol} {interval} fetch failed after retries")
            return None

        if not data:
            break
        all_raw.extend(data)
        last_open = int(data[-1][0])
        if last_open >= end_ms or len(data) < 1000:
            break
        cur = last_open + 1
        time.sleep(0.13)

    return all_raw if all_raw else None


def parse_klines(raw):
    """Parse raw klines into (opens, highs, lows, closes, volumes, times)."""
    opens   = [float(c[1]) for c in raw]
    highs   = [float(c[2]) for c in raw]
    lows    = [float(c[3]) for c in raw]
    closes  = [float(c[4]) for c in raw]
    volumes = [float(c[5]) for c in raw]
    times   = [int(c[0])   for c in raw]
    return opens, highs, lows, closes, volumes, times

# ─────────────────────────────────────────────
# INDICATORS (pure Python, no numpy)
# ─────────────────────────────────────────────
def ema(values, period):
    result = [None] * len(values)
    k = 2.0 / (period + 1)
    e = None
    for i, v in enumerate(values):
        if v is None:
            result[i] = None
            continue
        if e is None:
            e = v
        else:
            e = v * k + e * (1 - k)
        result[i] = e
    return result


def sma(values, period):
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1: i + 1]
        if any(v is None for v in window):
            continue
        result[i] = sum(window) / period
    return result


def atr(highs, lows, closes, period=14):
    trs = [None]
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        if None in (h, l, pc):
            trs.append(None)
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    result = [None] * len(closes)
    # RMA (Wilder's smoothing)
    init_vals = [v for v in trs[1:period+1] if v is not None]
    if len(init_vals) < period:
        return result
    rma = sum(init_vals) / period
    result[period] = rma
    for i in range(period + 1, len(trs)):
        if trs[i] is None:
            result[i] = result[i-1]
            continue
        rma = (rma * (period - 1) + trs[i]) / period
        result[i] = rma
    return result


def rsi(closes, period=14):
    result = [None] * len(closes)
    if len(closes) < period + 1:
        return result
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    if avg_l == 0:
        result[period] = 100.0
    else:
        rs = avg_g / avg_l
        result[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        g = max(d, 0)
        l = max(-d, 0)
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
        if avg_l == 0:
            result[i] = 100.0
        else:
            rs = avg_g / avg_l
            result[i] = 100 - 100 / (1 + rs)
    return result


def adx_full(highs, lows, closes, period=14):
    n = len(closes)
    adx_list = [None] * n
    pdi_list = [None] * n
    mdi_list = [None] * n
    if n < period * 2 + 1:
        return adx_list, pdi_list, mdi_list

    dm_plus  = [0.0] * n
    dm_minus = [0.0] * n
    tr_list  = [0.0] * n

    for i in range(1, n):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        dm_plus[i]  = up   if up > down and up > 0   else 0.0
        dm_minus[i] = down if down > up and down > 0 else 0.0
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr_list[i] = max(h - l, abs(h - pc), abs(l - pc))

    # Wilder smoothing
    atr_w  = sum(tr_list[1:period+1])
    sdmp   = sum(dm_plus[1:period+1])
    sdmm   = sum(dm_minus[1:period+1])

    dx_list = []
    for i in range(period, n):
        if i > period:
            atr_w = atr_w - atr_w / period + tr_list[i]
            sdmp  = sdmp  - sdmp  / period + dm_plus[i]
            sdmm  = sdmm  - sdmm  / period + dm_minus[i]
        if atr_w == 0:
            dx_list.append(0.0)
            pdi_list[i] = 0.0
            mdi_list[i] = 0.0
            continue
        pdi = 100 * sdmp / atr_w
        mdi = 100 * sdmm / atr_w
        pdi_list[i] = pdi
        mdi_list[i] = mdi
        denom = pdi + mdi
        dx = 100 * abs(pdi - mdi) / denom if denom != 0 else 0.0
        dx_list.append(dx)

    # ADX = smoothed DX
    if len(dx_list) < period:
        return adx_list, pdi_list, mdi_list
    adx_val = sum(dx_list[:period]) / period
    adx_list[period * 2 - 1] = adx_val
    for j in range(period, len(dx_list)):
        adx_val = (adx_val * (period - 1) + dx_list[j]) / period
        adx_list[period + j] = adx_val

    return adx_list, pdi_list, mdi_list


def vsma(volumes, period=10):
    return sma(volumes, period)


# ─────────────────────────────────────────────
# HTF (1H) TREND MAP
# ─────────────────────────────────────────────
def build_1h_trend(h1_raw):
    """
    Returns (trend_map, sorted_timestamps).
    trend_map[ts] = 'BULL' | 'BEAR' | None
    """
    if not h1_raw:
        return {}, []
    opens, highs, lows, closes, volumes, times = parse_klines(h1_raw)
    ema50 = ema(closes, 50)
    trend_map = {}
    for i in range(len(closes)):
        e = ema50[i]
        if e is None:
            trend_map[times[i]] = None
            continue
        slope = (closes[i] - e) / e * 100
        if closes[i] > e and slope > 0:
            trend_map[times[i]] = 'BULL'
        elif closes[i] < e and slope < 0:
            trend_map[times[i]] = 'BEAR'
        else:
            trend_map[times[i]] = None
    return trend_map, sorted(times)


def get_1h_trend(ts_15m, sorted_1h_ts, trend_map):
    """Binary search for the 1H candle that contains a 15m timestamp."""
    lo, hi = 0, len(sorted_1h_ts) - 1
    idx = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if sorted_1h_ts[mid] <= ts_15m:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1
    if idx == -1:
        return None
    return trend_map.get(sorted_1h_ts[idx])


# ─────────────────────────────────────────────
# SWING HIGH / LOW PIVOTS
# ─────────────────────────────────────────────
def find_swing_low(lows, idx, lookback=20):
    """Find the lowest swing low (5-candle pivot) in last `lookback` candles before idx."""
    start = max(2, idx - lookback)
    best = None
    for j in range(start, idx - 2):
        # middle of 5: j is lowest of j-2..j+2 (clamp at edges)
        lo_j = lows[j]
        window = lows[max(0,j-2):min(len(lows),j+3)]
        if lo_j == min(window) and len(window) == 5:
            if best is None or lo_j < best:
                best = lo_j
    return best


def find_swing_high(highs, idx, lookback=40):
    """Find the nearest swing high (5-candle pivot) in last `lookback` candles before idx."""
    start = max(2, idx - lookback)
    best = None
    for j in range(start, idx - 2):
        hi_j = highs[j]
        window = highs[max(0,j-2):min(len(highs),j+3)]
        if hi_j == max(window) and len(window) == 5:
            if best is None or hi_j > best:
                best = hi_j
    return best


def find_swing_high_sl(highs, idx, lookback=20):
    """Swing high for SHORT SL."""
    start = max(2, idx - lookback)
    best = None
    for j in range(start, idx - 2):
        hi_j = highs[j]
        window = highs[max(0,j-2):min(len(highs),j+3)]
        if hi_j == max(window) and len(window) == 5:
            if best is None or hi_j > best:
                best = hi_j
    return best


def find_swing_low_tp(lows, idx, lookback=40):
    """Swing low for SHORT TP."""
    start = max(2, idx - lookback)
    best = None
    for j in range(start, idx - 2):
        lo_j = lows[j]
        window = lows[max(0,j-2):min(len(lows),j+3)]
        if lo_j == min(window) and len(window) == 5:
            if best is None or lo_j < best:
                best = lo_j
    return best


# ─────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────
def get_leverage_and_pct(balance):
    if balance < 10:
        return 10, 0.30
    elif balance <= 50:
        return 10, 0.10
    else:
        return 5, 0.10


# ─────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────
def calc_metrics(trades):
    if not trades:
        return {}
    n = len(trades)
    wins  = [t for t in trades if t['win']]
    losses= [t for t in trades if not t['win']]
    wr = len(wins) / n * 100
    pnls = [t['pnl'] for t in trades]
    gp = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))
    pf = gp / gl if gl > 0 else float('inf') if gp > 0 else 0.0
    net = sum(pnls)
    aw  = gp / len(wins)  if wins   else 0.0
    al  = gl / len(losses) if losses else 0.0
    exp = (wr/100 * aw) - ((1 - wr/100) * al)

    # Drawdown
    equity = 0.0
    peak   = 0.0
    mdd    = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > mdd:
            mdd = dd

    # Sharpe (daily returns proxy)
    if len(pnls) > 1:
        try:
            std = statistics.stdev(pnls)
            sharpe = (statistics.mean(pnls) / std * math.sqrt(252)) if std > 0 else 0.0
        except:
            sharpe = 0.0
    else:
        sharpe = 0.0

    # Avg R:R winners
    win_rr = []
    for t in wins:
        if 'sl_dist' in t and t['sl_dist'] and t['sl_dist'] > 0:
            win_rr.append(t.get('tp_dist',0) / t['sl_dist'])
    avg_win_rr = sum(win_rr)/len(win_rr) if win_rr else 0.0

    return {
        'n': n,
        'wins': len(wins),
        'losses': len(losses),
        'wr': round(wr, 2),
        'pf': round(pf, 4),
        'net': round(net, 4),
        'gp': round(gp, 4),
        'gl': round(gl, 4),
        'aw': round(aw, 4),
        'al': round(al, 4),
        'exp': round(exp, 4),
        'mdd': round(mdd, 4),
        'sharpe': round(sharpe, 4),
        'avg_win_rr': round(avg_win_rr, 4),
    }


# ─────────────────────────────────────────────
# BACKTEST CORE — runs one TEST config on one symbol
# ─────────────────────────────────────────────
def run_test(test_id, symbol,
             opens, highs, lows, closes, volumes, times,
             sorted_1h_ts, trend_map):

    n = len(closes)
    WARMUP = 150  # bars needed for indicator warmup

    # Pre-compute all indicators once
    ema9   = ema(closes, 9)
    ema21  = ema(closes, 21)
    ema50  = ema(closes, 50)
    atr14  = atr(highs, lows, closes, 14)
    rsi14  = rsi(closes, 14)
    vol_sma10 = vsma(volumes, 10)
    adx14, pdi14, mdi14 = adx_full(highs, lows, closes, 14)

    trades = []
    filter_counts = collections.defaultdict(int)
    in_trade = False
    entry_price = sl = tp = 0.0
    direction = None
    balance = INITIAL_CAP
    last_close_bar = -99
    tp_dist_entry = sl_dist_entry = 0.0

    for i in range(WARMUP, n - 1):
        # ── MANAGE OPEN TRADE ──────────────────────────────
        if in_trade:
            h, l = highs[i], lows[i]
            hit_sl = (direction == 'LONG'  and l <= sl) or (direction == 'SHORT' and h >= sl)
            hit_tp = (direction == 'LONG'  and h >= tp) or (direction == 'SHORT' and l <= tp)

            close_trade = False
            win = False
            exit_price = None

            if hit_tp and hit_sl:
                # Both hit — assume TP first if TP is closer to open
                if direction == 'LONG':
                    close_trade = True
                    win = True
                    exit_price = tp
                else:
                    close_trade = True
                    win = True
                    exit_price = tp
            elif hit_tp:
                close_trade = True
                win = True
                exit_price = tp
            elif hit_sl:
                close_trade = True
                win = False
                exit_price = sl

            if close_trade:
                lev, pos_pct = get_leverage_and_pct(balance)
                risk_dollar = balance * pos_pct
                sl_dist = abs(entry_price - sl)
                if sl_dist > 0:
                    qty = (risk_dollar / sl_dist) * lev if test_id >= 4 else \
                          (balance * pos_pct * lev / entry_price)
                else:
                    qty = balance * pos_pct * lev / entry_price

                if direction == 'LONG':
                    gross = (exit_price - entry_price) * qty
                else:
                    gross = (entry_price - exit_price) * qty
                fee = (entry_price + exit_price) * qty * FEE_RATE
                pnl = gross - fee
                balance += pnl

                # TP accuracy tracking (for tests 4-6)
                max_fav = 0.0
                if test_id >= 4:
                    if direction == 'LONG':
                        max_fav = (highs[i] - entry_price)
                    else:
                        max_fav = (entry_price - lows[i])

                trades.append({
                    'dir': direction,
                    'entry': entry_price,
                    'exit': exit_price,
                    'pnl': pnl,
                    'win': win,
                    'sl_dist': abs(entry_price - sl),
                    'tp_dist': abs(tp - entry_price),
                    'max_fav': max_fav,
                    'tp': tp,
                    'sl': sl,
                })
                in_trade = False
                last_close_bar = i
            continue

        # ── COOLDOWN ───────────────────────────────────────
        if i - last_close_bar < 1:   # 1 bar cooldown ≈ 15 min
            continue

        # ── ENTRY SIGNAL (BASE) ───────────────────────────
        adx_val = adx14[i]
        e9      = ema9[i];   e9p = ema9[i-1]
        e21     = ema21[i];  e21p= ema21[i-1]
        e50     = ema50[i]
        atr_val = atr14[i]

        if None in (adx_val, e9, e9p, e21, e21p, e50, atr_val):
            continue
        if adx_val < 22:
            filter_counts['adx'] += 1
            continue

        slope50 = (e50 - ema50[i-5]) / ema50[i-5] * 100 if ema50[i-5] else 0
        if abs(slope50) < 0.05:
            filter_counts['slope'] += 1
            continue

        bull_cross = (e9p < e21p) and (e9 >= e21)
        bear_cross = (e9p > e21p) and (e9 <= e21)
        if not bull_cross and not bear_cross:
            continue

        direction_sig = 'LONG' if bull_cross else 'SHORT'

        # Slope direction check
        if direction_sig == 'LONG'  and slope50 < 0.05:
            filter_counts['slope'] += 1
            continue
        if direction_sig == 'SHORT' and slope50 > -0.05:
            filter_counts['slope'] += 1
            continue

        # ── LAYER A: HTF TREND FILTER ─────────────────────
        if test_id >= 1:
            if sorted_1h_ts:
                h1_trend = get_1h_trend(times[i], sorted_1h_ts, trend_map)
                if direction_sig == 'LONG'  and h1_trend != 'BULL':
                    filter_counts['htf'] += 1
                    continue
                if direction_sig == 'SHORT' and h1_trend != 'BEAR':
                    filter_counts['htf'] += 1
                    continue

        # ── LAYER B: VOLUME CONFIRMATION ──────────────────
        if test_id >= 2:
            vol_s = vol_sma10[i]
            if vol_s is None or volumes[i] < 1.3 * vol_s:
                filter_counts['volume'] += 1
                continue

        # ── LAYER C: RSI MOMENTUM FILTER ──────────────────
        if test_id >= 3:
            rsi_val = rsi14[i]
            if rsi_val is None:
                filter_counts['rsi'] += 1
                continue
            if direction_sig == 'LONG'  and not (40 <= rsi_val <= 68):
                filter_counts['rsi'] += 1
                continue
            if direction_sig == 'SHORT' and not (32 <= rsi_val <= 60):
                filter_counts['rsi'] += 1
                continue

        # ── LAYER E: ATR VOLATILITY GATE ──────────────────
        if test_id >= 5:
            recent_atrs = [atr14[j] for j in range(max(0,i-100), i+1) if atr14[j] is not None]
            if len(recent_atrs) >= 10:
                threshold = sorted(recent_atrs)[int(len(recent_atrs) * 0.60)]
                if atr_val < threshold:
                    filter_counts['atr_gate'] += 1
                    continue

        # ── LAYER F: CANDLESTICK CONFIRMATION ─────────────
        if test_id >= 6:
            if direction_sig == 'LONG'  and closes[i] <= opens[i]:
                filter_counts['candle'] += 1
                continue
            if direction_sig == 'SHORT' and closes[i] >= opens[i]:
                filter_counts['candle'] += 1
                continue

        # ── COMPUTE TP / SL ───────────────────────────────
        entry = closes[i]

        if test_id <= 3:
            # Blind ATR TP/SL
            if direction_sig == 'LONG':
                sl_price = entry - 2 * atr_val
                tp_price = entry + 3 * atr_val
            else:
                sl_price = entry + 2 * atr_val
                tp_price = entry - 3 * atr_val
        else:
            # LAYER D: Structure-based TP/SL
            if direction_sig == 'LONG':
                swing_sl = find_swing_low(lows, i, lookback=20)
                swing_tp = find_swing_high(highs, i, lookback=40)
                if swing_sl is None:
                    swing_sl = entry - 2 * atr_val
                sl_price = swing_sl - 0.5 * atr_val
                sl_dist  = entry - sl_price
                if sl_dist <= 0:
                    filter_counts['structure'] += 1
                    continue
                if swing_tp is not None and swing_tp > entry:
                    tp_price = swing_tp
                else:
                    tp_price = entry + 2.5 * sl_dist
                # Min R:R check
                rr = (tp_price - entry) / sl_dist
                if rr < 1.8:
                    filter_counts['rr'] += 1
                    continue
            else:
                swing_sl = find_swing_high_sl(highs, i, lookback=20)
                swing_tp = find_swing_low_tp(lows, i, lookback=40)
                if swing_sl is None:
                    swing_sl = entry + 2 * atr_val
                sl_price = swing_sl + 0.5 * atr_val
                sl_dist  = sl_price - entry
                if sl_dist <= 0:
                    filter_counts['structure'] += 1
                    continue
                if swing_tp is not None and swing_tp < entry:
                    tp_price = swing_tp
                else:
                    tp_price = entry - 2.5 * sl_dist
                rr = (entry - tp_price) / sl_dist
                if rr < 1.8:
                    filter_counts['rr'] += 1
                    continue

        # Sanity check TP/SL direction
        if direction_sig == 'LONG'  and (tp_price <= entry or sl_price >= entry):
            continue
        if direction_sig == 'SHORT' and (tp_price >= entry or sl_price <= entry):
            continue

        # ── ENTER TRADE ───────────────────────────────────
        in_trade    = True
        entry_price = entry
        sl          = sl_price
        tp          = tp_price
        direction   = direction_sig

    return trades, dict(filter_counts)


# ─────────────────────────────────────────────
# TP ACCURACY ANALYSIS (Tests 4-6)
# ─────────────────────────────────────────────
def tp_accuracy(trades):
    """For losing trades, what % of the way to TP did price get?"""
    losing = [t for t in trades if not t['win']]
    if not losing:
        return None
    pcts = []
    for t in losing:
        tp_d = t.get('tp_dist', 0)
        mf   = t.get('max_fav', 0)
        if tp_d > 0:
            pcts.append(min(mf / tp_d * 100, 100))
    if not pcts:
        return None
    return round(sum(pcts) / len(pcts), 2)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 70)
    print("BACKTEST v6 — Kimi Strategy Enhancement")
    print(f"Period: 6 months | Coins: {len(COINS)} | Tests: 7 (TEST0-TEST6)")
    print("=" * 70)

    # ── FETCH ALL DATA ────────────────────────────────────
    print("\n[1/2] Fetching data from Binance...")
    data_15m = {}
    data_1h  = {}

    for sym in COINS:
        print(f"  Fetching {sym} 15m...", end=" ", flush=True)
        raw15 = fetch_klines(sym, "15m", START_MS, NOW_MS)
        if raw15 is None:
            print("FAILED — skipping")
            continue
        data_15m[sym] = raw15
        print(f"{len(raw15)} candles OK")
        time.sleep(0.15)

        print(f"  Fetching {sym} 1h...",  end=" ", flush=True)
        raw1h = fetch_klines(sym, "1h",  START_MS, NOW_MS)
        if raw1h is None:
            print("FAILED — 1H missing, HTF filter disabled for this coin")
            data_1h[sym] = None
        else:
            data_1h[sym] = raw1h
            print(f"{len(raw1h)} candles OK")
        time.sleep(0.15)

    available = [s for s in COINS if s in data_15m]
    print(f"\n  Available coins: {available}")

    if not available:
        print("ERROR: No data fetched. Exiting.")
        return

    # ── BUILD 1H TREND MAPS ───────────────────────────────
    trend_maps = {}
    for sym in available:
        raw1h = data_1h.get(sym)
        if raw1h:
            tm, sts = build_1h_trend(raw1h)
            trend_maps[sym] = (tm, sts)
        else:
            trend_maps[sym] = ({}, [])

    # ── RUN ALL TESTS ─────────────────────────────────────
    print("\n[2/2] Running backtests...")
    TEST_NAMES = [
        "TEST0 — Baseline",
        "TEST1 — +HTF Filter",
        "TEST2 — +Volume",
        "TEST3 — +RSI",
        "TEST4 — +Structure TP/SL",
        "TEST5 — +ATR Gate",
        "TEST6 — Full Stack",
    ]

    all_results = {}

    for test_id in range(7):
        print(f"\n  {TEST_NAMES[test_id]}")
        test_trades_all  = []
        test_filters_all = collections.defaultdict(int)
        per_coin         = {}

        for sym in available:
            raw15 = data_15m[sym]
            opens, highs, lows, closes, volumes, times = parse_klines(raw15)
            tm, sts = trend_maps[sym]

            trades, fc = run_test(
                test_id, sym,
                opens, highs, lows, closes, volumes, times,
                sts, tm
            )
            m = calc_metrics(trades)
            per_coin[sym] = {'metrics': m, 'trades': trades, 'filters': fc}
            test_trades_all.extend(trades)
            for k, v in fc.items():
                test_filters_all[k] += v

            if m:
                print(f"    {sym:16s} | T={m['n']:4d} | WR={m['wr']:5.1f}% | PF={m['pf']:.4f} | Net=${m['net']:.2f}")
            else:
                print(f"    {sym:16s} | No trades")

        agg = calc_metrics(test_trades_all)
        tp_acc = tp_accuracy(test_trades_all) if test_id >= 4 else None

        all_results[test_id] = {
            'name': TEST_NAMES[test_id],
            'agg': agg,
            'per_coin': {s: {'metrics': v['metrics'], 'filters': v['filters']} for s, v in per_coin.items()},
            'filters_total': dict(test_filters_all),
            'tp_accuracy_pct': tp_acc,
        }

        if agg:
            print(f"    {'AGGREGATE':16s} | T={agg['n']:4d} | WR={agg['wr']:5.1f}% | PF={agg['pf']:.4f} | Net=${agg['net']:.2f} | MDD=${agg['mdd']:.2f}")

    # ── BUILD REPORT ──────────────────────────────────────
    print("\n" + "=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)

    summary_lines = []
    summary_lines.append("=" * 70)
    summary_lines.append("BACKTEST v6 — Kimi Strategy Enhancement")
    summary_lines.append(f"Generated: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    summary_lines.append(f"Period: 6 months | Start: {datetime.datetime.utcfromtimestamp(START_MS/1000).strftime('%Y-%m-%d')}")
    summary_lines.append(f"Coins: {available}")
    summary_lines.append("=" * 70)
    summary_lines.append("")

    # Comparison table
    summary_lines.append("SIDE-BY-SIDE COMPARISON")
    summary_lines.append("-" * 70)
    header = f"{'Test':<30} {'Trades':>7} {'WR%':>6} {'PF':>7} {'Net$':>8} {'MDD$':>8} {'Sharpe':>7}"
    summary_lines.append(header)
    summary_lines.append("-" * 70)

    ranked_pf   = []
    ranked_adj  = []

    for tid in range(7):
        r = all_results[tid]
        a = r['agg']
        if not a:
            summary_lines.append(f"{r['name']:<30} {'N/A':>7}")
            continue
        adj = a['pf'] / max(a['mdd'], 0.01) if a['mdd'] else 0
        line = (f"{r['name']:<30} {a['n']:>7} {a['wr']:>6.1f} {a['pf']:>7.4f}"
                f" {a['net']:>8.2f} {a['mdd']:>8.2f} {a['sharpe']:>7.4f}")
        summary_lines.append(line)
        ranked_pf.append((tid, a['pf']))
        ranked_adj.append((tid, adj))

    summary_lines.append("-" * 70)
    summary_lines.append("")

    # Rankings
    ranked_pf.sort(key=lambda x: -x[1])
    ranked_adj.sort(key=lambda x: -x[1])

    summary_lines.append("RANKING BY PROFIT FACTOR:")
    for rank, (tid, pf_val) in enumerate(ranked_pf, 1):
        name = all_results[tid]['name']
        summary_lines.append(f"  #{rank}: {name} — PF={pf_val:.4f}")

    summary_lines.append("")
    summary_lines.append("RANKING BY RISK-ADJUSTED RETURN (PF / MDD):")
    for rank, (tid, adj_val) in enumerate(ranked_adj, 1):
        name = all_results[tid]['name']
        summary_lines.append(f"  #{rank}: {name} — Score={adj_val:.4f}")

    summary_lines.append("")

    # Validation check
    summary_lines.append("VALIDATION CHECK (Target: PF ≥ 1.5, WR ≥ 42%)")
    summary_lines.append("-" * 50)
    for tid in range(7):
        r = all_results[tid]
        a = r['agg']
        if not a:
            continue
        pf_ok = "✅" if a['pf'] >= 1.5 else "❌"
        wr_ok = "✅" if a['wr'] >= 42  else "❌"
        summary_lines.append(f"  {r['name']:<30} PF {pf_ok} ({a['pf']:.4f})  WR {wr_ok} ({a['wr']:.1f}%)")

    summary_lines.append("")

    # Per-test detailed breakdown
    summary_lines.append("=" * 70)
    summary_lines.append("DETAILED PER-TEST BREAKDOWN")
    summary_lines.append("=" * 70)

    for tid in range(7):
        r = all_results[tid]
        a = r['agg']
        summary_lines.append(f"\n{r['name']}")
        summary_lines.append("-" * 50)
        if not a:
            summary_lines.append("  No trades generated.")
            continue
        summary_lines.append(f"  Total Trades    : {a['n']}")
        summary_lines.append(f"  Win Rate        : {a['wr']:.2f}%")
        summary_lines.append(f"  Loss Rate       : {100-a['wr']:.2f}%")
        summary_lines.append(f"  Profit Factor   : {a['pf']:.4f}")
        summary_lines.append(f"  Net PnL ($)     : ${a['net']:.4f}")
        summary_lines.append(f"  Avg Win ($)     : ${a['aw']:.4f}")
        summary_lines.append(f"  Avg Loss ($)    : ${a['al']:.4f}")
        summary_lines.append(f"  Expectancy/Trade: ${a['exp']:.4f}")
        summary_lines.append(f"  Max Drawdown ($): ${a['mdd']:.4f}")
        summary_lines.append(f"  Sharpe Ratio    : {a['sharpe']:.4f}")
        summary_lines.append(f"  Avg Win R:R     : {a['avg_win_rr']:.4f}")
        if r['tp_accuracy_pct'] is not None:
            summary_lines.append(f"  TP Accuracy (SL trades reached % of TP): {r['tp_accuracy_pct']:.2f}%")

        # Per-coin table
        summary_lines.append(f"\n  PER-COIN (sorted by PF desc):")
        coin_rows = []
        for sym, cv in r['per_coin'].items():
            m = cv['metrics']
            if m:
                coin_rows.append((sym, m))
        coin_rows.sort(key=lambda x: -x[1].get('pf', 0))
        for sym, m in coin_rows:
            summary_lines.append(
                f"    {sym:<16} T={m.get('n',0):4d} WR={m.get('wr',0):5.1f}% PF={m.get('pf',0):.4f} Net=${m.get('net',0):.2f}")

        # Filter stats
        fc = r['filters_total']
        if fc:
            summary_lines.append(f"\n  FILTER REJECTION COUNTS:")
            for k, v in sorted(fc.items(), key=lambda x: -x[1]):
                summary_lines.append(f"    {k:<12}: {v:6d} signals rejected")

    # Recommendation
    summary_lines.append("")
    summary_lines.append("=" * 70)
    summary_lines.append("RECOMMENDATION")
    summary_lines.append("=" * 70)

    best_tid = ranked_pf[0][0] if ranked_pf else None
    if best_tid is not None:
        br = all_results[best_tid]
        ba = br['agg']
        summary_lines.append(f"Best by PF: {br['name']}")
        summary_lines.append(f"  PF={ba['pf']:.4f} | WR={ba['wr']:.1f}% | Net=${ba['net']:.2f} | MDD=${ba['mdd']:.2f}")
        summary_lines.append("")
        if ba['pf'] >= 1.5:
            summary_lines.append("✅ TARGET REACHED: PF ≥ 1.5 achieved.")
        else:
            summary_lines.append("❌ Target not yet reached. Best PF below 1.5.")
            summary_lines.append("   Recommendation: Focus on coins where PF ≥ 1.5 individually.")

    summary_lines.append("")
    summary_lines.append("OVERFITTING WARNING:")
    summary_lines.append("  - 6 coins over 6 months is a narrow sample.")
    summary_lines.append("  - Layer D (structure TP/SL) uses lookback on same data — mild lookahead risk.")
    summary_lines.append("  - RSI/ATR gate thresholds should be validated on out-of-sample data.")
    summary_lines.append("  - Walk-forward test recommended before live deployment.")
    summary_lines.append("=" * 70)

    summary_text = "\n".join(summary_lines)

    # Print summary
    print(summary_text)

    # ── WRITE OUTPUT FILES ────────────────────────────────
    with open("backtest_summary.txt", "w", encoding="utf-8") as f:
        f.write(summary_text)

    # JSON report
    json_out = {}
    for tid in range(7):
        r = all_results[tid]
        json_out[f"TEST{tid}"] = {
            'name': r['name'],
            'aggregate': r['agg'],
            'per_coin': r['per_coin'],
            'filters_total': r['filters_total'],
            'tp_accuracy_pct': r['tp_accuracy_pct'],
        }

    with open("backtest_report.json", "w", encoding="utf-8") as f:
        json.dump(json_out, f, indent=2)

    print("\n✅ Done. Output files:")
    print("   backtest_summary.txt")
    print("   backtest_report.json")


if __name__ == "__main__":
    main()
