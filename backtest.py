"""
CRYPTO FUTURES BACKTEST — v7 (Grok Spec)
Strategies:
  S1 — SuperTrend + ADX (flip entry, ATR exit Variant A + SuperTrend flip Variant B)
  S2 — ADX + 50EMA Trend + 21EMA Pullback (ATR exit + EMA exit)
  S3 — Volatility Breakout + ADX + Volume (NO trailing stop)
Data: data-api.binance.vision, 30m candles, 2024-07-01 to 2026-07-01
Stdlib only: json, math, statistics, urllib, datetime, collections, time
"""

import json
import math
import time
import datetime
import statistics
import urllib.request
import urllib.error
import collections

# ============================================================
# CONFIG
# ============================================================
START_DATE   = "2024-07-01"
END_DATE     = "2026-07-01"
INTERVAL     = "30m"
INITIAL_CAP  = 10000.0
RISK_PER_TRADE = 0.0075      # 0.75% equity risk per trade
FEE_RATE     = 0.0005        # 0.05% per side (maker)
SLIPPAGE     = 0.0002        # 0.02% per side
MAX_POSITIONS = 6            # max concurrent positions across all symbols

COINS = [
    "ETHUSDT", "DOGEUSDT", "DOTUSDT", "ARBUSDT",
    "ADAUSDT", "APTUSDT", "LINKUSDT", "SOLUSDT",
    "SUIUSDT", "WIFUSDT", "BTCUSDT", "BNBUSDT",
    "NEARUSDT", "XRPUSDT", "AVAXUSDT", "LTCUSDT",
    "ATOMUSDT", "OPUSDT", "INJUSDT", "UNIUSDT",
    "AAVEUSDT", "HBARUSDT", "TRUMPUSDT", "BOMEUSDT",
    "WLDUSDT", "NEIROUSDT",
    # Skipped: 1000FLOKIUSDT (HTTP 400), 1000BONKUSDT, 1000PEPEUSDT, 1000SHIBUSDT
    # (not available on data-api.binance.vision spot endpoint)
]

# ============================================================
# DATE HELPERS
# ============================================================
def date_to_ms(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)

START_MS = date_to_ms(START_DATE)
END_MS   = date_to_ms(END_DATE)

# ============================================================
# DATA FETCHING
# ============================================================
def fetch_klines(symbol, interval, start_ms, end_ms):
    base_url = "https://data-api.binance.vision/api/v3/klines"
    all_klines = []
    current_ms = start_ms
    max_retries = 5

    while current_ms < end_ms:
        url = (f"{base_url}?symbol={symbol}&interval={interval}"
               f"&startTime={current_ms}&endTime={end_ms}&limit=1000")
        for attempt in range(max_retries):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
                    if not data:
                        return all_klines
                    all_klines.extend(data)
                    last_time = data[-1][0]
                    if last_time >= end_ms - 1:
                        return all_klines
                    current_ms = last_time + 1
                    time.sleep(0.13)
                    break
            except urllib.error.HTTPError as e:
                if e.code in (400, 451):
                    print(f"  [SKIP] {symbol} HTTP {e.code} — skipping coin")
                    return None
                wait = 2 ** attempt
                print(f"  [RETRY] {symbol} HTTP {e.code}, wait {wait}s")
                time.sleep(wait)
            except Exception as e:
                wait = 2 ** attempt
                print(f"  [RETRY] {symbol} error: {e}, wait {wait}s")
                time.sleep(wait)
        else:
            print(f"  [FAIL] {symbol} max retries exceeded")
            return all_klines

    return all_klines

def parse_klines(raw):
    opens   = [float(k[1]) for k in raw]
    highs   = [float(k[2]) for k in raw]
    lows    = [float(k[3]) for k in raw]
    closes  = [float(k[4]) for k in raw]
    volumes = [float(k[5]) for k in raw]
    times   = [int(k[0])   for k in raw]
    return opens, highs, lows, closes, volumes, times

# ============================================================
# INDICATORS (pure Python, no numpy/pandas)
# ============================================================
def ema(values, period):
    result = [None] * len(values)
    k = 2.0 / (period + 1)
    # seed with SMA
    valid_start = period - 1
    if valid_start >= len(values):
        return result
    sma_val = sum(values[:period]) / period
    result[valid_start] = sma_val
    for i in range(valid_start + 1, len(values)):
        result[i] = values[i] * k + result[i-1] * (1 - k)
    return result

def sma(values, period):
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1: i + 1]) / period
    return result

def wilders_smooth(values, period):
    """Wilder's smoothing (for ADX/ATR)"""
    result = [None] * len(values)
    if len(values) < period:
        return result
    # first value = sum of first 'period' values
    first_sum = sum(v for v in values[:period] if v is not None)
    result[period - 1] = first_sum
    for i in range(period, len(values)):
        if values[i] is not None and result[i-1] is not None:
            result[i] = result[i-1] - (result[i-1] / period) + values[i]
    return result

def atr(highs, lows, closes, period=14):
    n = len(closes)
    tr_list = [None] * n
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        tr_list[i] = max(hl, hc, lc)
    # Wilder smoothing
    smoothed = wilders_smooth([t if t is not None else 0 for t in tr_list], period)
    result = [None] * n
    for i in range(period, n):
        if smoothed[i] is not None:
            result[i] = smoothed[i] / period
    return result

def adx_full(highs, lows, closes, period=14):
    n = len(closes)
    dm_plus  = [0.0] * n
    dm_minus = [0.0] * n
    tr_list  = [0.0] * n

    for i in range(1, n):
        up_move   = highs[i] - highs[i-1]
        down_move = lows[i-1] - lows[i]
        dm_plus[i]  = up_move   if (up_move > down_move and up_move > 0) else 0.0
        dm_minus[i] = down_move if (down_move > up_move and down_move > 0) else 0.0
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        tr_list[i] = max(hl, hc, lc)

    sm_tr  = wilders_smooth(tr_list,  period)
    sm_dmp = wilders_smooth(dm_plus,  period)
    sm_dmm = wilders_smooth(dm_minus, period)

    pdi = [None] * n
    mdi = [None] * n
    dx  = [None] * n

    for i in range(period, n):
        if sm_tr[i] and sm_tr[i] != 0:
            pdi[i] = 100 * (sm_dmp[i] / period) / (sm_tr[i] / period)
            mdi[i] = 100 * (sm_dmm[i] / period) / (sm_tr[i] / period)
            dif = abs(pdi[i] - mdi[i])
            tot = pdi[i] + mdi[i]
            dx[i] = 100 * dif / tot if tot != 0 else 0.0

    # ADX = Wilder smooth of DX
    dx_clean = [d if d is not None else 0.0 for d in dx]
    sm_dx = wilders_smooth(dx_clean, period)
    adx_vals = [None] * n
    for i in range(period * 2, n):
        if sm_dx[i] is not None:
            adx_vals[i] = sm_dx[i] / period

    return adx_vals, pdi, mdi

def rsi(closes, period=14):
    n = len(closes)
    result = [None] * n
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        diff = closes[i] - closes[i-1]
        gains[i]  = diff if diff > 0 else 0.0
        losses[i] = -diff if diff < 0 else 0.0
    # Wilder avg
    avg_gain = sum(gains[1:period+1]) / period
    avg_loss = sum(losses[1:period+1]) / period
    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100 - 100 / (1 + rs)
    return result

def vsma(volumes, period=20):
    return sma(volumes, period)

def supertrend(highs, lows, closes, atr_period=10, multiplier=3.0):
    """
    Classic ratcheting SuperTrend.
    Returns: (direction, st_line) — direction +1=bull, -1=bear
    """
    n = len(closes)
    atr_vals = atr(highs, lows, closes, atr_period)

    direction = [None] * n
    st_line   = [None] * n
    upper_band = [None] * n
    lower_band = [None] * n

    for i in range(atr_period + 1, n):
        if atr_vals[i] is None:
            continue
        hl2 = (highs[i] + lows[i]) / 2
        ub = hl2 + multiplier * atr_vals[i]
        lb = hl2 - multiplier * atr_vals[i]

        # Ratchet upper band
        if upper_band[i-1] is not None and closes[i-1] is not None:
            upper_band[i] = ub if ub < upper_band[i-1] or closes[i-1] > upper_band[i-1] else upper_band[i-1]
        else:
            upper_band[i] = ub

        # Ratchet lower band
        if lower_band[i-1] is not None and closes[i-1] is not None:
            lower_band[i] = lb if lb > lower_band[i-1] or closes[i-1] < lower_band[i-1] else lower_band[i-1]
        else:
            lower_band[i] = lb

        # Determine direction
        if direction[i-1] is None:
            direction[i] = 1 if closes[i] > upper_band[i] else -1
        else:
            prev_dir = direction[i-1]
            if prev_dir == -1 and closes[i] > upper_band[i]:
                direction[i] = 1
            elif prev_dir == 1 and closes[i] < lower_band[i]:
                direction[i] = -1
            else:
                direction[i] = prev_dir

        st_line[i] = lower_band[i] if direction[i] == 1 else upper_band[i]

    return direction, st_line

def atr_sma(highs, lows, closes, atr_period=14, sma_period=50):
    atr_vals = atr(highs, lows, closes, atr_period)
    atr_clean = [v if v is not None else 0.0 for v in atr_vals]
    return sma(atr_clean, sma_period)

# ============================================================
# METRICS
# ============================================================
def metrics(trades, symbol=""):
    if not trades:
        return {
            "symbol": symbol, "n": 0, "wr": 0, "pf": 0, "net": 0,
            "mdd": 0, "sharpe": 0, "sortino": 0, "aw": 0, "al": 0,
            "exp": 0, "dur": 0, "nlongs": 0, "nshorts": 0,
            "lwr": 0, "swr": 0, "monthly": {}, "maxcw": 0, "maxcl": 0,
            "gp": 0, "gl": 0
        }

    pnls     = [t["pnl"] for t in trades]
    wins     = [p for p in pnls if p > 0]
    losses   = [p for p in pnls if p <= 0]
    n        = len(pnls)
    wr       = len(wins) / n * 100
    gp       = sum(wins)
    gl       = abs(sum(losses))
    pf       = gp / gl if gl > 0 else float('inf')
    net      = sum(pnls)
    aw       = statistics.mean(wins) if wins else 0
    al       = statistics.mean(losses) if losses else 0
    exp      = statistics.mean(pnls)

    # Duration
    durs = [t.get("dur", 0) for t in trades]
    dur  = statistics.mean(durs) if durs else 0

    # Max drawdown
    equity = INITIAL_CAP
    peak   = INITIAL_CAP
    mdd    = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > mdd:
            mdd = dd

    # Sharpe (daily-ish: group by day)
    daily = collections.defaultdict(float)
    for t in trades:
        day_key = str(t["exit_t"])[:8]
        daily[day_key] += t["pnl"]
    daily_vals = list(daily.values())
    if len(daily_vals) > 1:
        mean_d = statistics.mean(daily_vals)
        std_d  = statistics.stdev(daily_vals)
        sharpe = (mean_d / std_d * math.sqrt(365)) if std_d > 0 else 0
        neg    = [v for v in daily_vals if v < 0]
        dstd   = statistics.stdev(neg) if len(neg) > 1 else 0
        sortino = (mean_d / dstd * math.sqrt(365)) if dstd > 0 else 0
    else:
        sharpe  = 0
        sortino = 0

    # Long/short breakdown
    longs  = [t for t in trades if t.get("dir") == "long"]
    shorts = [t for t in trades if t.get("dir") == "short"]
    nlongs  = len(longs)
    nshorts = len(shorts)
    lwr = len([t for t in longs  if t["pnl"] > 0]) / nlongs  * 100 if nlongs  else 0
    swr = len([t for t in shorts if t["pnl"] > 0]) / nshorts * 100 if nshorts else 0

    # Monthly PnL
    monthly = collections.defaultdict(float)
    for t in trades:
        ts_s = t["exit_t"] // 1000
        dt   = datetime.datetime.utcfromtimestamp(ts_s)
        key  = f"{dt.year}-{dt.month:02d}"
        monthly[key] += t["pnl"]

    # Consecutive wins/losses
    maxcw = maxcl = cw = cl = 0
    for p in pnls:
        if p > 0:
            cw += 1; cl = 0
            maxcw = max(maxcw, cw)
        else:
            cl += 1; cw = 0
            maxcl = max(maxcl, cl)

    return {
        "symbol": symbol, "n": n, "wr": round(wr, 2), "pf": round(pf, 4),
        "net": round(net, 2), "mdd": round(mdd, 2), "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3), "aw": round(aw, 2), "al": round(al, 2),
        "exp": round(exp, 2), "dur": round(dur / 60000, 2),
        "nlongs": nlongs, "nshorts": nshorts,
        "lwr": round(lwr, 2), "swr": round(swr, 2),
        "monthly": dict(monthly), "maxcw": maxcw, "maxcl": maxcl,
        "gp": round(gp, 2), "gl": round(gl, 2)
    }

# ============================================================
# TRADE HELPERS
# ============================================================
def entry_price(close, direction):
    slip = SLIPPAGE + FEE_RATE
    return close * (1 + slip) if direction == "long" else close * (1 - slip)

def exit_price_calc(price, direction):
    slip = SLIPPAGE + FEE_RATE
    return price * (1 - slip) if direction == "long" else price * (1 + slip)

def calc_pnl(ep, xp, direction, equity, sl_dist):
    """Returns dollar PnL based on fixed risk sizing."""
    if sl_dist <= 0:
        return 0.0
    risk_dollars = equity * RISK_PER_TRADE
    size = risk_dollars / sl_dist
    if direction == "long":
        return (xp - ep) * size
    else:
        return (ep - xp) * size

# ============================================================
# STRATEGY 1 — SuperTrend + ADX
# Variant A: Fixed ATR exits (2.5 TP / 1.8 SL)
# Variant B: SuperTrend flip exit + 2.0 ATR hard stop
# ============================================================
def run_s1(symbol, opens, highs, lows, closes, volumes, times):
    n = len(closes)
    if n < 100:
        return [], [], {}

    st_dir, st_line = supertrend(highs, lows, closes, atr_period=10, multiplier=3.0)
    atr14 = atr(highs, lows, closes, 14)
    adx14, pdi, mdi = adx_full(highs, lows, closes, 14)

    trades_a = []
    trades_b = []
    filter_counts = {"no_st": 0, "no_adx": 0, "adx_low": 0, "di_fail": 0, "cooldown": 0, "signals": 0}

    cooldown_a = -1
    cooldown_b = -1
    pos_a = None
    pos_b = None
    equity_a = INITIAL_CAP
    equity_b = INITIAL_CAP
    concurrent_a = 0
    concurrent_b = 0

    for i in range(30, n):
        if st_dir[i] is None or st_dir[i-1] is None:
            filter_counts["no_st"] += 1
            continue
        if adx14[i] is None or pdi[i] is None or mdi[i] is None:
            filter_counts["no_adx"] += 1
            continue
        if atr14[i] is None:
            continue

        # --- Check exits first ---
        # Variant A exit
        if pos_a is not None:
            ep, sl, tp, dr, sl_d = pos_a["ep"], pos_a["sl"], pos_a["tp"], pos_a["dir"], pos_a["sl_dist"]
            hit_tp = hit_sl = False
            if dr == "long":
                if lows[i] <= sl:
                    xp = exit_price_calc(sl, dr); hit_sl = True
                elif highs[i] >= tp:
                    xp = exit_price_calc(tp, dr); hit_tp = True
            else:
                if highs[i] >= sl:
                    xp = exit_price_calc(sl, dr); hit_sl = True
                elif lows[i] <= tp:
                    xp = exit_price_calc(tp, dr); hit_tp = True
            if hit_tp or hit_sl:
                pnl = calc_pnl(ep, xp, dr, equity_a, sl_d)
                equity_a += pnl
                trades_a.append({
                    "dir": dr, "entry": ep, "exit": xp,
                    "entry_t": pos_a["entry_t"], "exit_t": times[i],
                    "pnl": round(pnl, 4), "win": pnl > 0,
                    "hit_tp": hit_tp, "dur": times[i] - pos_a["entry_t"],
                    "risk": round(sl_d, 6)
                })
                cooldown_a = i + 2
                pos_a = None
                concurrent_a = max(0, concurrent_a - 1)

        # Variant B exit
        if pos_b is not None:
            ep, sl, dr, sl_d = pos_b["ep"], pos_b["sl"], pos_b["dir"], pos_b["sl_dist"]
            hit_stop = False
            flip_exit = False
            xp = None
            if dr == "long":
                if lows[i] <= sl:
                    xp = exit_price_calc(sl, dr); hit_stop = True
                elif st_dir[i] == -1:
                    xp = exit_price_calc(closes[i], dr); flip_exit = True
            else:
                if highs[i] >= sl:
                    xp = exit_price_calc(sl, dr); hit_stop = True
                elif st_dir[i] == 1:
                    xp = exit_price_calc(closes[i], dr); flip_exit = True
            if hit_stop or flip_exit:
                pnl = calc_pnl(ep, xp, dr, equity_b, sl_d)
                equity_b += pnl
                trades_b.append({
                    "dir": dr, "entry": ep, "exit": xp,
                    "entry_t": pos_b["entry_t"], "exit_t": times[i],
                    "pnl": round(pnl, 4), "win": pnl > 0,
                    "hit_tp": flip_exit, "dur": times[i] - pos_b["entry_t"],
                    "risk": round(sl_d, 6)
                })
                cooldown_b = i + 2
                pos_b = None
                concurrent_b = max(0, concurrent_b - 1)

        # --- Check entries ---
        # SuperTrend flip detection
        flip_bull = (st_dir[i] == 1 and st_dir[i-1] == -1)
        flip_bear = (st_dir[i] == -1 and st_dir[i-1] == 1)

        if not (flip_bull or flip_bear):
            continue
        if adx14[i] < 25:
            filter_counts["adx_low"] += 1
            continue

        direction = "long" if flip_bull else "short"

        # DI confirmation
        if direction == "long" and pdi[i] <= mdi[i]:
            filter_counts["di_fail"] += 1
            continue
        if direction == "short" and mdi[i] <= pdi[i]:
            filter_counts["di_fail"] += 1
            continue

        filter_counts["signals"] += 1
        ep_raw = closes[i]

        # Variant A
        if pos_a is None and i >= cooldown_a and concurrent_a < MAX_POSITIONS:
            ep = entry_price(ep_raw, direction)
            sl_dist = 1.8 * atr14[i]
            sl = ep - sl_dist if direction == "long" else ep + sl_dist
            tp_dist = 2.5 * atr14[i]
            tp = ep + tp_dist if direction == "long" else ep - tp_dist
            pos_a = {"ep": ep, "sl": sl, "tp": tp, "dir": direction,
                     "sl_dist": sl_dist, "entry_t": times[i]}
            concurrent_a += 1

        # Variant B
        if pos_b is None and i >= cooldown_b and concurrent_b < MAX_POSITIONS:
            ep = entry_price(ep_raw, direction)
            sl_dist = 2.0 * atr14[i]
            sl = ep - sl_dist if direction == "long" else ep + sl_dist
            pos_b = {"ep": ep, "sl": sl, "dir": direction,
                     "sl_dist": sl_dist, "entry_t": times[i]}
            concurrent_b += 1

    return trades_a, trades_b, filter_counts

# ============================================================
# STRATEGY 2 — ADX + 50EMA Trend + 21EMA Pullback
# Exit A: ATR (3.0 TP / 1.8 SL)
# Exit B: EMA21 cross exit + ATR hard stop
# ============================================================
def run_s2(symbol, opens, highs, lows, closes, volumes, times):
    n = len(closes)
    if n < 120:
        return [], [], {}

    ema50 = ema(closes, 50)
    ema21 = ema(closes, 21)
    ema9  = ema(closes, 9)
    atr14 = atr(highs, lows, closes, 14)
    adx14, pdi, mdi = adx_full(highs, lows, closes, 14)

    trades_a = []
    trades_b = []
    filter_counts = {
        "no_ema": 0, "no_trend": 0, "slope_fail": 0,
        "adx_low": 0, "no_pullback": 0, "cooldown": 0, "signals": 0
    }

    cooldown_a = -1
    cooldown_b = -1
    pos_a = None
    pos_b = None
    equity_a = INITIAL_CAP
    equity_b = INITIAL_CAP
    concurrent_a = 0
    concurrent_b = 0

    for i in range(60, n):
        if ema50[i] is None or ema21[i] is None or ema9[i] is None:
            filter_counts["no_ema"] += 1
            continue
        if adx14[i] is None:
            filter_counts["no_ema"] += 1
            continue
        if atr14[i] is None:
            continue
        if ema50[i-6] is None:
            continue

        # Slope check
        slope = (ema50[i] - ema50[i-6]) / ema50[i-6] * 100
        uptrend   = slope > 0.08 and closes[i] > ema50[i]
        downtrend = slope < -0.08 and closes[i] < ema50[i]

        # --- Exits ---
        if pos_a is not None:
            ep, sl, tp, dr, sl_d = pos_a["ep"], pos_a["sl"], pos_a["tp"], pos_a["dir"], pos_a["sl_dist"]
            hit_tp = hit_sl = False
            if dr == "long":
                if lows[i] <= sl:
                    xp = exit_price_calc(sl, dr); hit_sl = True
                elif highs[i] >= tp:
                    xp = exit_price_calc(tp, dr); hit_tp = True
            else:
                if highs[i] >= sl:
                    xp = exit_price_calc(sl, dr); hit_sl = True
                elif lows[i] <= tp:
                    xp = exit_price_calc(tp, dr); hit_tp = True
            if hit_tp or hit_sl:
                pnl = calc_pnl(ep, xp, dr, equity_a, sl_d)
                equity_a += pnl
                trades_a.append({
                    "dir": dr, "entry": ep, "exit": xp,
                    "entry_t": pos_a["entry_t"], "exit_t": times[i],
                    "pnl": round(pnl, 4), "win": pnl > 0,
                    "hit_tp": hit_tp, "dur": times[i] - pos_a["entry_t"],
                    "risk": round(sl_d, 6)
                })
                cooldown_a = i + 3
                pos_a = None
                concurrent_a = max(0, concurrent_a - 1)

        if pos_b is not None:
            ep, sl, dr, sl_d = pos_b["ep"], pos_b["sl"], pos_b["dir"], pos_b["sl_dist"]
            hit_stop = ema_exit = False
            xp = None
            if dr == "long":
                if lows[i] <= sl:
                    xp = exit_price_calc(sl, dr); hit_stop = True
                elif closes[i] < ema21[i]:
                    xp = exit_price_calc(closes[i], dr); ema_exit = True
            else:
                if highs[i] >= sl:
                    xp = exit_price_calc(sl, dr); hit_stop = True
                elif closes[i] > ema21[i]:
                    xp = exit_price_calc(closes[i], dr); ema_exit = True
            if hit_stop or ema_exit:
                pnl = calc_pnl(ep, xp, dr, equity_b, sl_d)
                equity_b += pnl
                trades_b.append({
                    "dir": dr, "entry": ep, "exit": xp,
                    "entry_t": pos_b["entry_t"], "exit_t": times[i],
                    "pnl": round(pnl, 4), "win": pnl > 0,
                    "hit_tp": ema_exit, "dur": times[i] - pos_b["entry_t"],
                    "risk": round(sl_d, 6)
                })
                cooldown_b = i + 3
                pos_b = None
                concurrent_b = max(0, concurrent_b - 1)

        # --- Entries ---
        if not (uptrend or downtrend):
            filter_counts["no_trend"] += 1
            continue
        if adx14[i] < 25:
            filter_counts["adx_low"] += 1
            continue

        direction = "long" if uptrend else "short"

        # Pullback check: price touched/within 0.15% of EMA21 in last 3 candles
        # and current candle closes back above/below EMA21
        touched = False
        for j in range(max(0, i-2), i+1):
            if ema21[j] is None:
                continue
            if direction == "long":
                band = ema21[j] * 1.0015
                if lows[j] <= band:
                    touched = True; break
            else:
                band = ema21[j] * 0.9985
                if highs[j] >= band:
                    touched = True; break

        if not touched:
            filter_counts["no_pullback"] += 1
            continue

        # Closed back beyond EMA21
        if direction == "long" and closes[i] <= ema21[i]:
            filter_counts["no_pullback"] += 1
            continue
        if direction == "short" and closes[i] >= ema21[i]:
            filter_counts["no_pullback"] += 1
            continue

        # Optional: EMA9 > EMA21 for longs
        if direction == "long" and ema9[i] <= ema21[i]:
            continue
        if direction == "short" and ema9[i] >= ema21[i]:
            continue

        filter_counts["signals"] += 1
        ep_raw = closes[i]

        # Variant A
        if pos_a is None and i >= cooldown_a and concurrent_a < MAX_POSITIONS:
            ep = entry_price(ep_raw, direction)
            sl_dist = 1.8 * atr14[i]
            sl = ep - sl_dist if direction == "long" else ep + sl_dist
            tp = ep + 3.0 * atr14[i] if direction == "long" else ep - 3.0 * atr14[i]
            pos_a = {"ep": ep, "sl": sl, "tp": tp, "dir": direction,
                     "sl_dist": sl_dist, "entry_t": times[i]}
            concurrent_a += 1

        # Variant B
        if pos_b is None and i >= cooldown_b and concurrent_b < MAX_POSITIONS:
            ep = entry_price(ep_raw, direction)
            sl_dist = 1.8 * atr14[i]
            sl = ep - sl_dist if direction == "long" else ep + sl_dist
            pos_b = {"ep": ep, "sl": sl, "dir": direction,
                     "sl_dist": sl_dist, "entry_t": times[i]}
            concurrent_b += 1

    return trades_a, trades_b, filter_counts

# ============================================================
# STRATEGY 3 — Volatility Breakout + ADX + Volume
# NO trailing stop (proven harmful in live bot)
# ============================================================
def run_s3(symbol, opens, highs, lows, closes, volumes, times):
    n = len(closes)
    if n < 120:
        return [], {}

    atr14 = atr(highs, lows, closes, 14)
    adx14, pdi, mdi = adx_full(highs, lows, closes, 14)
    vol_sma20 = vsma(volumes, 20)
    atr_sma50 = atr_sma(highs, lows, closes, 14, 50)

    trades = []
    filter_counts = {
        "no_indicators": 0, "adx_low": 0, "vol_low": 0,
        "atr_low": 0, "no_breakout": 0, "cooldown": 0, "signals": 0
    }

    cooldown = -1
    pos = None
    equity = INITIAL_CAP
    concurrent = 0

    for i in range(60, n):
        if atr14[i] is None or adx14[i] is None:
            filter_counts["no_indicators"] += 1
            continue
        if vol_sma20[i] is None or atr_sma50[i] is None:
            filter_counts["no_indicators"] += 1
            continue

        # --- Exit ---
        if pos is not None:
            ep, sl, tp, dr, sl_d = pos["ep"], pos["sl"], pos["tp"], pos["dir"], pos["sl_dist"]
            hit_tp = hit_sl = False
            if dr == "long":
                if lows[i] <= sl:
                    xp = exit_price_calc(sl, dr); hit_sl = True
                elif highs[i] >= tp:
                    xp = exit_price_calc(tp, dr); hit_tp = True
            else:
                if highs[i] >= sl:
                    xp = exit_price_calc(sl, dr); hit_sl = True
                elif lows[i] <= tp:
                    xp = exit_price_calc(tp, dr); hit_tp = True
            if hit_tp or hit_sl:
                pnl = calc_pnl(ep, xp, dr, equity, sl_d)
                equity += pnl
                trades.append({
                    "dir": dr, "entry": ep, "exit": xp,
                    "entry_t": pos["entry_t"], "exit_t": times[i],
                    "pnl": round(pnl, 4), "win": pnl > 0,
                    "hit_tp": hit_tp, "dur": times[i] - pos["entry_t"],
                    "risk": round(sl_d, 6)
                })
                cooldown = i + 4
                pos = None
                concurrent = max(0, concurrent - 1)

        # --- Entry ---
        if pos is not None or i < cooldown or concurrent >= MAX_POSITIONS:
            if i < cooldown:
                filter_counts["cooldown"] += 1
            continue

        if adx14[i] < 22:
            filter_counts["adx_low"] += 1
            continue
        if volumes[i] <= 1.3 * vol_sma20[i]:
            filter_counts["vol_low"] += 1
            continue
        if atr14[i] <= atr_sma50[i]:
            filter_counts["atr_low"] += 1
            continue

        # Donchian-style breakout (previous 20 candles excluding current)
        lookback = 20
        if i < lookback:
            continue
        hh = max(highs[i - lookback: i])
        ll = min(lows[i  - lookback: i])

        direction = None
        if closes[i] > hh:
            direction = "long"
        elif closes[i] < ll:
            direction = "short"

        if direction is None:
            filter_counts["no_breakout"] += 1
            continue

        filter_counts["signals"] += 1
        ep_raw = closes[i]
        ep = entry_price(ep_raw, direction)
        sl_dist = 1.6 * atr14[i]
        sl = ep - sl_dist if direction == "long" else ep + sl_dist
        tp = ep + 2.8 * atr14[i] if direction == "long" else ep - 2.8 * atr14[i]

        pos = {"ep": ep, "sl": sl, "tp": tp, "dir": direction,
               "sl_dist": sl_dist, "entry_t": times[i]}
        concurrent += 1

    return trades, filter_counts

# ============================================================
# OUTPUT WRITERS
# ============================================================
def write_summary(all_results, filename="backtest_summary.txt"):
    lines = []
    lines.append("=" * 80)
    lines.append("BACKTEST SUMMARY — v7 (Grok Spec)")
    lines.append(f"Period : {START_DATE} → {END_DATE}")
    lines.append(f"Candles: 30m | Coins tested: {len(all_results.get('coins_tested', []))}")
    lines.append(f"Fees   : {FEE_RATE*100:.3f}% per side | Slippage: {SLIPPAGE*100:.3f}% per side")
    lines.append(f"Risk   : {RISK_PER_TRADE*100:.2f}% equity per trade | Start cap: ${INITIAL_CAP:,.0f}")
    lines.append("=" * 80)

    strategy_labels = {
        "s1a": "S1 — SuperTrend+ADX [Variant A: ATR exits 2.5TP/1.8SL]",
        "s1b": "S1 — SuperTrend+ADX [Variant B: ST flip exit + 2.0 ATR stop]",
        "s2a": "S2 — EMA Pullback     [Variant A: ATR exits 3.0TP/1.8SL]",
        "s2b": "S2 — EMA Pullback     [Variant B: EMA21 exit + 1.8 ATR stop]",
        "s3":  "S3 — Breakout         [Fixed exits 2.8TP/1.6SL, NO trailing]",
    }

    targets = {"pf": 1.5, "wr": 42.0}

    for strat_key, label in strategy_labels.items():
        if strat_key not in all_results:
            continue
        data = all_results[strat_key]
        agg  = data.get("aggregate", {})
        coin_metrics = data.get("per_coin", [])

        lines.append("")
        lines.append("─" * 80)
        lines.append(label)
        lines.append("─" * 80)

        if not agg or agg.get("n", 0) == 0:
            lines.append("  No trades generated.")
            continue

        pf_ok = len([c for c in coin_metrics if c.get("pf", 0) >= targets["pf"]])
        wr_ok = len([c for c in coin_metrics if c.get("wr", 0) >= targets["wr"]])
        nc    = len(coin_metrics)

        lines.append(f"  Total Trades  : {agg['n']}")
        lines.append(f"  Win Rate      : {agg['wr']:.2f}%")
        lines.append(f"  Profit Factor : {agg['pf']:.4f}")
        lines.append(f"  Net PnL       : ${agg['net']:,.2f}  ({agg['net']/INITIAL_CAP*100:.2f}%)")
        lines.append(f"  Max Drawdown  : {agg['mdd']:.2f}%")
        lines.append(f"  Sharpe Ratio  : {agg['sharpe']:.3f}")
        lines.append(f"  Sortino Ratio : {agg['sortino']:.3f}")
        lines.append(f"  Avg Win       : ${agg['aw']:,.2f}")
        lines.append(f"  Avg Loss      : ${agg['al']:,.2f}")
        lines.append(f"  Avg R/trade   : ${agg['exp']:,.2f}")
        lines.append(f"  Avg Duration  : {agg['dur']:.1f} hrs")
        lines.append(f"  Longs/Shorts  : {agg['nlongs']} / {agg['nshorts']}")
        lines.append(f"  Long WR       : {agg['lwr']:.2f}%  | Short WR: {agg['swr']:.2f}%")
        lines.append(f"  Max Con. Wins : {agg['maxcw']}  | Max Con. Losses: {agg['maxcl']}")
        lines.append(f"  Gross Profit  : ${agg['gp']:,.2f} | Gross Loss: ${agg['gl']:,.2f}")
        lines.append("")
        lines.append(f"  ✅ VALIDATION:")
        lines.append(f"     PF >= 1.5 on {pf_ok}/{nc} coins")
        lines.append(f"     WR >= 42% on {wr_ok}/{nc} coins")

        # Filter stats
        fc = data.get("filter_counts", {})
        if fc:
            lines.append("")
            lines.append("  FILTER REJECTION STATS:")
            for k, v in fc.items():
                lines.append(f"    {k:<20}: {v:,}")

        # Per-coin table sorted by PF desc
        if coin_metrics:
            sorted_coins = sorted(coin_metrics, key=lambda x: x.get("pf", 0), reverse=True)
            lines.append("")
            lines.append(f"  PER-COIN TABLE (sorted by PF desc):")
            lines.append(f"  {'Symbol':<16} {'N':>5} {'WR%':>7} {'PF':>7} {'Net$':>9} {'MDD%':>7} {'Sharpe':>7} {'AvgHr':>7}")
            lines.append(f"  {'-'*16} {'-'*5} {'-'*7} {'-'*7} {'-'*9} {'-'*7} {'-'*7} {'-'*7}")
            for c in sorted_coins:
                pf_str = f"{c['pf']:.3f}" if c['pf'] != float('inf') else "∞"
                flag = " ✅" if c['pf'] >= 1.5 and c['wr'] >= 42 else ""
                lines.append(
                    f"  {c['symbol']:<16} {c['n']:>5} {c['wr']:>7.2f} {pf_str:>7} "
                    f"{c['net']:>9.2f} {c['mdd']:>7.2f} {c['sharpe']:>7.3f} {c['dur']:>7.1f}{flag}"
                )

        # Monthly PnL
        monthly = agg.get("monthly", {})
        if monthly:
            lines.append("")
            lines.append("  MONTHLY PnL:")
            for month in sorted(monthly.keys()):
                val = monthly[month]
                bar = "█" * min(40, max(0, int(abs(val) / max(1, agg["net"]) * 40)))
                sign = "+" if val >= 0 else "-"
                lines.append(f"    {month}  {sign}${abs(val):>8.2f}  {bar}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("END OF REPORT")
    lines.append("=" * 80)

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[OUT] {filename} written")

# ============================================================
# AGGREGATE METRICS ACROSS ALL COINS
# ============================================================
def aggregate_metrics(all_trades_list, symbol_label="AGGREGATE"):
    combined = []
    for trades in all_trades_list:
        combined.extend(trades)
    return metrics(combined, symbol_label)

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("BACKTEST v7 — Grok Spec (3 Strategies)")
    print(f"Period: {START_DATE} → {END_DATE} | 30m candles")
    print(f"Coins to test: {len(COINS)}")
    print("=" * 60)

    results = {
        "s1a": {"per_coin": [], "all_trades": [], "filter_counts": {}},
        "s1b": {"per_coin": [], "all_trades": [], "filter_counts": {}},
        "s2a": {"per_coin": [], "all_trades": [], "filter_counts": {}},
        "s2b": {"per_coin": [], "all_trades": [], "filter_counts": {}},
        "s3":  {"per_coin": [], "all_trades": [], "filter_counts": {}},
    }

    fc_s1 = collections.defaultdict(int)
    fc_s2 = collections.defaultdict(int)
    fc_s3 = collections.defaultdict(int)

    coins_tested = []

    for coin in COINS:
        print(f"\n[COIN] {coin}")
        raw = fetch_klines(coin, INTERVAL, START_MS, END_MS)
        if raw is None or len(raw) < 100:
            print(f"  [SKIP] {coin} — insufficient data ({len(raw) if raw else 0} candles)")
            continue

        opens, highs, lows, closes, volumes, times = parse_klines(raw)
        print(f"  Candles: {len(closes)}")
        coins_tested.append(coin)

        # S1
        t_s1a, t_s1b, fc1 = run_s1(coin, opens, highs, lows, closes, volumes, times)
        m1a = metrics(t_s1a, coin)
        m1b = metrics(t_s1b, coin)
        results["s1a"]["per_coin"].append(m1a)
        results["s1b"]["per_coin"].append(m1b)
        results["s1a"]["all_trades"].append(t_s1a)
        results["s1b"]["all_trades"].append(t_s1b)
        for k, v in fc1.items(): fc_s1[k] += v
        print(f"  S1A: {len(t_s1a)} trades | PF={m1a['pf']:.3f} | WR={m1a['wr']:.1f}%")
        print(f"  S1B: {len(t_s1b)} trades | PF={m1b['pf']:.3f} | WR={m1b['wr']:.1f}%")

        # S2
        t_s2a, t_s2b, fc2 = run_s2(coin, opens, highs, lows, closes, volumes, times)
        m2a = metrics(t_s2a, coin)
        m2b = metrics(t_s2b, coin)
        results["s2a"]["per_coin"].append(m2a)
        results["s2b"]["per_coin"].append(m2b)
        results["s2a"]["all_trades"].append(t_s2a)
        results["s2b"]["all_trades"].append(t_s2b)
        for k, v in fc2.items(): fc_s2[k] += v
        print(f"  S2A: {len(t_s2a)} trades | PF={m2a['pf']:.3f} | WR={m2a['wr']:.1f}%")
        print(f"  S2B: {len(t_s2b)} trades | PF={m2b['pf']:.3f} | WR={m2b['wr']:.1f}%")

        # S3
        t_s3, fc3 = run_s3(coin, opens, highs, lows, closes, volumes, times)
        m3 = metrics(t_s3, coin)
        results["s3"]["per_coin"].append(m3)
        results["s3"]["all_trades"].append(t_s3)
        for k, v in fc3.items(): fc_s3[k] += v
        print(f"  S3 : {len(t_s3)} trades | PF={m3['pf']:.3f} | WR={m3['wr']:.1f}%")

    # Aggregate
    print("\n[AGG] Computing aggregates...")
    for key, fc_map in [("s1a", fc_s1), ("s1b", fc_s1), ("s2a", fc_s2), ("s2b", fc_s2), ("s3", fc_s3)]:
        results[key]["aggregate"]     = aggregate_metrics(results[key]["all_trades"])
        results[key]["filter_counts"] = dict(fc_map)

    results["coins_tested"] = coins_tested

    # Write JSON
    print("[OUT] Writing backtest_report.json...")
    report = {}
    for key in ["s1a", "s1b", "s2a", "s2b", "s3"]:
        report[key] = {
            "aggregate":     results[key]["aggregate"],
            "per_coin":      results[key]["per_coin"],
            "filter_counts": results[key]["filter_counts"],
        }
    report["coins_tested"] = coins_tested
    report["config"] = {
        "start": START_DATE, "end": END_DATE,
        "interval": INTERVAL, "initial_cap": INITIAL_CAP,
        "risk_per_trade": RISK_PER_TRADE, "fee_rate": FEE_RATE,
        "slippage": SLIPPAGE, "max_positions": MAX_POSITIONS,
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("[OUT] backtest_report.json written")

    # Write summary
    write_summary(results)

    # Final console summary
    print("\n" + "=" * 60)
    print("FINAL RESULTS SUMMARY")
    print("=" * 60)
    for key, label in [
        ("s1a", "S1 Variant A"), ("s1b", "S1 Variant B"),
        ("s2a", "S2 Variant A"), ("s2b", "S2 Variant B"),
        ("s3",  "S3 Breakout"),
    ]:
        agg = results[key].get("aggregate", {})
        if agg.get("n", 0) == 0:
            print(f"  {label:<20}: No trades")
        else:
            pf = agg['pf']
            pf_str = f"{pf:.4f}" if pf != float('inf') else "∞"
            flag = " ✅" if pf >= 1.5 and agg['wr'] >= 42 else " ❌"
            print(f"  {label:<20}: {agg['n']:>5} trades | PF={pf_str} | WR={agg['wr']:.1f}% | Net=${agg['net']:,.0f}{flag}")
    print("=" * 60)
    print("Done. Check backtest_report.json + backtest_summary.txt")

if __name__ == "__main__":
    main()

