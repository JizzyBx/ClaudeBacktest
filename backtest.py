"""
Backtest Pipeline — 10 Strategies × Top 100 Binance USDM Futures
3-Year backtest | 50 parallel workers | stdlib only (Python 3.11)

Strategies:
  OLD (from handoff history):
    S1 — EMA Pullback + RSI + ADX (30m)
    S2 — SuperTrend Pullback + 4H Filter + RSI (30m)
    S3 — EMA Stack + Volume Surge (15m)
    S4 — Chandelier Exit + EMA200 + RSI/SMA Cross (1H)
    S5 — SuperTrend + MACD + BB + Volume (15m)
  NEW (untested):
    N1 — Liquidity Sweep + Reclaim (30m)
    N2 — CVD Divergence + EMA + ATR (15m)
    N4 — Liquidity Sweep + FVG Fill (30m)
    N5 — MTF RSI Divergence (15m + 1H)
    N3 — Funding Rate Extreme placeholder (1H) — skipped, data not in archive

Data: data.binance.vision monthly OHLCV archives
"""

import urllib.request
import urllib.error
import zipfile
import csv
import io
import json
import math
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed

# ─────────────────────────── CONFIG ───────────────────────────

START_YEAR, START_MONTH = 2022, 7
END_YEAR,   END_MONTH   = 2025, 6

STARTING_CAPITAL   = 10_000.0
RISK_PER_TRADE     = 0.0075   # 0.75%
FEE_RATE           = 0.0005   # 0.05% per side
SLIPPAGE_RATE      = 0.0002   # 0.02% per side
MAX_CONCURRENT     = 6        # max open positions across portfolio
MAX_WORKERS        = 50

# Strategies to run
STRATEGIES = ["S1", "S2", "S3", "S4", "S5", "N1", "N2", "N4", "N5"]

# Timeframes needed per strategy
STRATEGY_TF = {
    "S1": "30m", "S2": "30m", "S3": "15m",
    "S4": "1h",  "S5": "15m", "N1": "30m",
    "N2": "15m", "N4": "30m", "N5": "15m",
}
# HTF needed (for S2 needs 4h, N5 needs 1h)
STRATEGY_HTF = {
    "S2": "4h", "N5": "1h",
}

# Top 100 USDM Futures — curated list covering majors, mid-caps, memes
# Script also attempts to auto-fetch from Binance exchange info at runtime
FALLBACK_COINS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","XRPUSDT","SOLUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT",
    "MATICUSDT","LTCUSDT","UNIUSDT","ATOMUSDT","ETCUSDT",
    "XLMUSDT","ALGOUSDT","VETUSDT","FILUSDT","TRXUSDT",
    "NEARUSDT","ICPUSDT","APTUSDT","ARBUSDT","OPUSDT",
    "INJUSDT","SUIUSDT","AAVEUSDT","WLDUSDT","SEIUSDT",
    "TIAUSDT","JUPUSDT","PYTHUSDT","STXUSDT","HBARUSDT",
    "RUNEUSDT","ORDIUSDT","KASUSDT","THETAUSDT","EGLDUSDT",
    "FLOWUSDT","APEUSDT","GALAUSDT","SANDUSDT","MANAUSDT",
    "AXSUSDT","RNDRUSDT","GRTUSDT","ENJUSDT","LRCUSDT",
    "ZECUSDT","XMRUSDT","DASHUSDT","NEOUSDT","WAVESUSDT",
    "COMPUSDT","MKRUSDT","SNXUSDT","YFIUSDT","CRVUSDT",
    "SUSHIUSDT","BALUSDT","UMAUSDT","BANDUSDT","COTIUSDT",
    "IOTAUSDT","ZILUSDT","KSMUSDT","CVCUSDT","STORJUSDT",
    "SKLUSDT","CELRUSDT","BTTUSDT","HOTUSDT","WINUSDT",
    "CTKUSDT","JOEUSDT","MAGICUSDT","GMXUSDT","PENDLEUSDT",
    "WIFUSDT","BOMEUSDT","NEIROUSDT","TRUMPUSDT",
    "1000BONKUSDT","1000PEPEUSDT","1000SHIBUSDT","1000FLOKIUSDT",
    "1000RATSUSDT","1000XECUSDT","1000LUNCUSDT",
    "POLUSDT","RENDERUSDT","FETUSDT","AGIXUSDT","OCEANUSDT",
    "ARKMUSDT","ACEUSDT","ALTUSDT","PORTALUSDT","DYMUSDT",
    "PIXELUSDT","RONINUSDT","AEVOUSDT","WUSDT","ENAUSDT",
]

SYMBOL_REMAP = {
    "MATICUSDT": "POLUSDT",
}

# ─────────────────────────── DATA FETCH ───────────────────────────

BASE_URL = "https://data.binance.vision/data/futures/um"

def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def fetch_monthly_klines(symbol, interval, year, month):
    fname = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/monthly/klines/{symbol}/{interval}/{fname}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csvname = zf.namelist()[0]
            with zf.open(csvname) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None  # symbol didn't exist yet or was delisted
        raise
    except Exception:
        return None

def parse_klines(rows):
    """Parse raw CSV rows into list of dicts. Handles ms/us timestamps."""
    candles = []
    for row in rows:
        if not row or row[0].startswith("open_time"):
            continue
        try:
            ot = int(row[0])
            if ot > 10**14:
                ot //= 1000  # microseconds → milliseconds
            candles.append({
                "t": ot,
                "o": float(row[1]),
                "h": float(row[2]),
                "l": float(row[3]),
                "c": float(row[4]),
                "v": float(row[5]),
            })
        except (ValueError, IndexError):
            continue
    return candles

def load_symbol_candles(symbol, interval):
    """Load all candles for symbol/interval across the full date range."""
    all_candles = []
    failed_months = 0
    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        rows = fetch_monthly_klines(symbol, interval, y, m)
        if rows is None:
            failed_months += 1
            continue
        all_candles.extend(parse_klines(rows))
    if failed_months == len(list(month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH))):
        return None  # 100% failure = likely blocked or symbol doesn't exist
    # Sort and deduplicate
    seen = set()
    unique = []
    for c in sorted(all_candles, key=lambda x: x["t"]):
        if c["t"] not in seen:
            seen.add(c["t"])
            unique.append(c)
    return unique

# ─────────────────────────── INDICATORS ───────────────────────────

def ema(values, period):
    result = [None] * len(values)
    if len(values) < period:
        return result
    k = 2 / (period + 1)
    # seed with SMA
    sma = sum(values[:period]) / period
    result[period - 1] = sma
    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i-1] * (1 - k)
    return result

def sma(values, period):
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1:i + 1]) / period
    return result

def atr(candles, period=14):
    result = [None] * len(candles)
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["h"] - c["l"])
        else:
            prev_c = candles[i-1]["c"]
            trs.append(max(c["h"] - c["l"], abs(c["h"] - prev_c), abs(c["l"] - prev_c)))
    # Wilder smoothing
    if len(trs) < period:
        return result
    init = sum(trs[:period]) / period
    result[period - 1] = init
    for i in range(period, len(candles)):
        result[i] = (result[i-1] * (period - 1) + trs[i]) / period
    return result

def rsi(closes, period=14):
    result = [None] * len(closes)
    if len(closes) < period + 1:
        return result
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i-1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    # Wilder smoothing
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(closes)):
        idx = i  # result index
        ag = (avg_gain * (period - 1) + gains[i - 1]) / period
        al = (avg_loss * (period - 1) + losses[i - 1]) / period
        avg_gain, avg_loss = ag, al
        rs = ag / al if al != 0 else float("inf")
        result[idx] = 100 - (100 / (1 + rs))
    return result

def adx_dmi(candles, period=14):
    """Returns (adx, plus_di, minus_di) lists."""
    n = len(candles)
    adx_r = [None] * n
    pdi_r = [None] * n
    mdi_r = [None] * n
    if n < period * 2:
        return adx_r, pdi_r, mdi_r
    trs, pdms, mdms = [], [], []
    for i in range(1, n):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        up = h - candles[i-1]["h"]
        dn = candles[i-1]["l"] - l
        pdm = up if up > dn and up > 0 else 0
        mdm = dn if dn > up and dn > 0 else 0
        trs.append(tr); pdms.append(pdm); mdms.append(mdm)
    # Wilder smooth
    atr14 = sum(trs[:period]) / period
    pdi14 = sum(pdms[:period]) / period
    mdi14 = sum(mdms[:period]) / period
    dx_list = []
    for i in range(period, len(trs)):
        atr14 = (atr14 * (period - 1) + trs[i]) / period
        pdi14 = (pdi14 * (period - 1) + pdms[i]) / period
        mdi14 = (mdi14 * (period - 1) + mdms[i]) / period
        pdi_val = (pdi14 / atr14 * 100) if atr14 else 0
        mdi_val = (mdi14 / atr14 * 100) if atr14 else 0
        pdi_r[i + 1] = pdi_val
        mdi_r[i + 1] = mdi_val
        dx = abs(pdi_val - mdi_val) / (pdi_val + mdi_val) * 100 if (pdi_val + mdi_val) else 0
        dx_list.append(dx)
    # ADX = smoothed DX
    if len(dx_list) < period:
        return adx_r, pdi_r, mdi_r
    adx_val = sum(dx_list[:period]) / period
    base_idx = period * 2
    adx_r[base_idx] = adx_val
    for j in range(period, len(dx_list)):
        adx_val = (adx_val * (period - 1) + dx_list[j]) / period
        adx_r[base_idx + j - period + 1] = adx_val
    return adx_r, pdi_r, mdi_r

def supertrend(candles, period=10, multiplier=3.0):
    """Returns list of (direction, line) per candle. direction: 1=bull, -1=bear."""
    n = len(candles)
    atr_vals = atr(candles, period)
    result = [None] * n
    upper_band = [None] * n
    lower_band = [None] * n
    for i in range(period, n):
        if atr_vals[i] is None:
            continue
        hl2 = (candles[i]["h"] + candles[i]["l"]) / 2
        upper_band[i] = hl2 + multiplier * atr_vals[i]
        lower_band[i] = hl2 - multiplier * atr_vals[i]
    direction = 1
    final_upper = [None] * n
    final_lower = [None] * n
    for i in range(period, n):
        if upper_band[i] is None:
            continue
        # Adjust bands
        fu = upper_band[i]
        fl = lower_band[i]
        if i > period and final_upper[i-1] is not None:
            fu = min(fu, final_upper[i-1]) if candles[i-1]["c"] < final_upper[i-1] else fu
            fl = max(fl, final_lower[i-1]) if candles[i-1]["c"] > final_lower[i-1] else fl
        final_upper[i] = fu
        final_lower[i] = fl
        if i > period and result[i-1] is not None:
            prev_dir = result[i-1][0]
            if prev_dir == 1:
                direction = 1 if candles[i]["c"] >= final_lower[i] else -1
            else:
                direction = -1 if candles[i]["c"] <= final_upper[i] else 1
        line = final_lower[i] if direction == 1 else final_upper[i]
        result[i] = (direction, line)
    return result

def bollinger(closes, period=20, std_dev=2.0):
    n = len(closes)
    upper = [None] * n
    lower = [None] * n
    mid = sma(closes, period)
    for i in range(period - 1, n):
        if mid[i] is None:
            continue
        variance = sum((closes[i - j] - mid[i]) ** 2 for j in range(period)) / period
        sd = math.sqrt(variance)
        upper[i] = mid[i] + std_dev * sd
        lower[i] = mid[i] - std_dev * sd
    return upper, mid, lower

def macd(closes, fast=12, slow=26, signal=9):
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line[i] = ema_fast[i] - ema_slow[i]
    # Signal line = EMA of macd_line
    macd_vals = [v if v is not None else 0 for v in macd_line]
    sig_line = ema(macd_vals, signal)
    hist = [None] * len(closes)
    for i in range(len(closes)):
        if macd_line[i] is not None and sig_line[i] is not None:
            hist[i] = macd_line[i] - sig_line[i]
    return macd_line, sig_line, hist

def vol_sma(candles, period=20):
    vols = [c["v"] for c in candles]
    return sma(vols, period)

def cvd_approx(candles):
    """Approximate CVD: cumulative (buy_vol - sell_vol) using candle direction."""
    result = []
    cumulative = 0.0
    for c in candles:
        if c["c"] >= c["o"]:
            cumulative += c["v"]
        else:
            cumulative -= c["v"]
        result.append(cumulative)
    return result

def swing_highs_lows(candles, lookback=10):
    """Returns list of (is_high, index, price) for swing points."""
    n = len(candles)
    swings = []
    for i in range(lookback, n - lookback):
        h = candles[i]["h"]
        l = candles[i]["l"]
        is_high = all(candles[j]["h"] <= h for j in range(i - lookback, i + lookback + 1) if j != i)
        is_low  = all(candles[j]["l"] >= l for j in range(i - lookback, i + lookback + 1) if j != i)
        if is_high:
            swings.append((True, i, h))
        if is_low:
            swings.append((False, i, l))
    return swings

# ─────────────────────────── TRADE ENGINE ───────────────────────────

def apply_cost(price, direction):
    """Apply fee + slippage. direction: 1=long entry or short exit, -1=short entry or long exit."""
    cost = FEE_RATE + SLIPPAGE_RATE
    return price * (1 + cost) if direction == 1 else price * (1 - cost)

def calc_position_size(equity, entry, sl):
    """Risk RISK_PER_TRADE of equity on the trade."""
    risk_amount = equity * RISK_PER_TRADE
    dist = abs(entry - sl)
    if dist == 0:
        return 0
    return risk_amount / dist

def run_trades(signals, candles, equity_ref):
    """
    signals: list of {i, direction, sl, tp1, tp2, tp1_pct} per bar index
    Returns: list of closed trade dicts
    """
    trades = []
    open_positions = []  # {direction, entry, sl, tp1, tp2, tp1_hit, size, entry_i}
    equity = equity_ref[0]
    active_count = equity_ref[1]

    sig_map = {s["i"]: s for s in signals}

    for i in range(len(candles)):
        bar = candles[i]
        # Check open positions for exit
        still_open = []
        for pos in open_positions:
            closed = False
            pnl = 0.0
            exit_price = None
            exit_reason = None
            if pos["direction"] == 1:  # long
                if bar["l"] <= pos["sl"]:
                    exit_price = apply_cost(pos["sl"], -1)
                    pnl = (exit_price - pos["entry"]) * pos["size"]
                    exit_reason = "sl"
                    closed = True
                elif not pos["tp1_hit"] and bar["h"] >= pos["tp1"]:
                    # TP1: close 50%
                    ep = apply_cost(pos["tp1"], -1)
                    pnl += (ep - pos["entry"]) * pos["size"] * 0.5
                    pos["size"] *= 0.5
                    pos["tp1_hit"] = True
                    pos["sl"] = pos["entry"]  # move SL to BE
                if pos["tp1_hit"] and not closed and bar["h"] >= pos["tp2"]:
                    exit_price = apply_cost(pos["tp2"], -1)
                    pnl += (exit_price - pos["entry"]) * pos["size"]
                    exit_reason = "tp2"
                    closed = True
            else:  # short
                if bar["h"] >= pos["sl"]:
                    exit_price = apply_cost(pos["sl"], 1)
                    pnl = (pos["entry"] - exit_price) * pos["size"]
                    exit_reason = "sl"
                    closed = True
                elif not pos["tp1_hit"] and bar["l"] <= pos["tp1"]:
                    ep = apply_cost(pos["tp1"], 1)
                    pnl += (pos["entry"] - ep) * pos["size"] * 0.5
                    pos["size"] *= 0.5
                    pos["tp1_hit"] = True
                    pos["sl"] = pos["entry"]
                if pos["tp1_hit"] and not closed and bar["l"] <= pos["tp2"]:
                    exit_price = apply_cost(pos["tp2"], 1)
                    pnl += (pos["entry"] - exit_price) * pos["size"]
                    exit_reason = "tp2"
                    closed = True

            if closed:
                equity += pnl
                active_count -= 1
                dur = i - pos["entry_i"]
                trades.append({
                    "direction": pos["direction"],
                    "entry": pos["entry"],
                    "exit": exit_price,
                    "pnl": pnl,
                    "win": pnl > 0,
                    "exit_reason": exit_reason,
                    "duration": dur,
                    "entry_i": pos["entry_i"],
                    "exit_i": i,
                })
            else:
                still_open.append(pos)
        open_positions = still_open

        # Check for new signal
        if i in sig_map and active_count < MAX_CONCURRENT:
            sig = sig_map[i]
            raw_entry = bar["c"]  # enter on close of signal bar
            direction = sig["direction"]
            entry = apply_cost(raw_entry, direction)
            sl = sig["sl"]
            tp1 = sig["tp1"]
            tp2 = sig["tp2"]
            size = calc_position_size(equity, entry, sl)
            if size > 0:
                open_positions.append({
                    "direction": direction,
                    "entry": entry,
                    "sl": sl,
                    "tp1": tp1,
                    "tp2": tp2,
                    "tp1_hit": False,
                    "size": size,
                    "entry_i": i,
                })
                active_count += 1

    equity_ref[0] = equity
    equity_ref[1] = active_count
    return trades

# ─────────────────────────── STRATEGIES ───────────────────────────

def strategy_S1(candles):
    """EMA Pullback + RSI + ADX (30m)"""
    closes = [c["c"] for c in candles]
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    adx_vals, pdi, mdi = adx_dmi(candles, 14)
    atr14 = atr(candles, 14)

    signals = []
    stats = {"warmup_none":0,"no_trend":0,"no_adx":0,"no_pullback":0,"no_rsi":0,"signal":0}

    for i in range(2, len(candles)):
        if any(x is None for x in [ema21[i], ema50[i], rsi14[i], adx_vals[i], atr14[i]]):
            stats["warmup_none"] += 1; continue
        # Trend
        trend = 1 if ema21[i] > ema50[i] else -1
        # ADX filter
        if adx_vals[i] < 22:
            stats["no_adx"] += 1; continue
        # Pullback: prev bar touched/crossed EMA21, this bar closed back above/below
        if trend == 1:
            touched = candles[i-1]["l"] <= ema21[i-1] * 1.002
            reclaimed = candles[i]["c"] > ema21[i]
        else:
            touched = candles[i-1]["h"] >= ema21[i-1] * 0.998
            reclaimed = candles[i]["c"] < ema21[i]
        if not (touched and reclaimed):
            stats["no_pullback"] += 1; continue
        # RSI
        rsi_ok = (rsi14[i] > 45 and trend == 1) or (rsi14[i] < 55 and trend == -1)
        if not rsi_ok:
            stats["no_rsi"] += 1; continue

        entry = candles[i]["c"]
        sl = entry - trend * 1.0 * atr14[i]
        tp1 = entry + trend * 1.5 * atr14[i]
        tp2 = entry + trend * 2.5 * atr14[i]
        signals.append({"i": i, "direction": trend, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_S2(candles, htf_candles):
    """SuperTrend Pullback + 4H Filter + RSI (30m). Enters N bars after flip."""
    if not htf_candles:
        return [], {"error": "no_htf_data"}
    closes = [c["c"] for c in candles]
    rsi14 = rsi(closes, 14)
    atr14 = atr(candles, 14)
    st30 = supertrend(candles, 10, 3.0)
    st4h = supertrend(htf_candles, 10, 3.0)

    # Build 4H direction lookup by timestamp (use closed candle = previous bar)
    htf_dir_by_ts = {}
    for j, c in enumerate(htf_candles):
        if st4h[j] is not None:
            htf_dir_by_ts[c["t"]] = st4h[j][0]
    htf_times = sorted(htf_dir_by_ts.keys())

    def get_htf_dir(bar_ts):
        # Find latest HTF candle that CLOSED before this bar (offset by 4H period)
        HTF_MS = 4 * 3600 * 1000
        query = bar_ts - HTF_MS
        idx = None
        for t in htf_times:
            if t <= query:
                idx = t
            else:
                break
        return htf_dir_by_ts.get(idx)

    signals = []
    stats = {"warmup_none":0,"no_htf":0,"no_st_flip":0,"not_n_bars_after":0,"no_pullback":0,"no_rsi":0,"signal":0}

    flip_bar = {}  # direction -> last flip index
    for i in range(1, len(candles)):
        if st30[i] is None or st30[i-1] is None:
            stats["warmup_none"] += 1; continue
        cur_dir, cur_line = st30[i]
        prev_dir, _ = st30[i-1]
        if cur_dir != prev_dir:
            flip_bar[cur_dir] = i

    N_BARS_AFTER = 3  # enter 3+ bars after flip

    for i in range(3, len(candles)):
        if any(x is None for x in [rsi14[i], atr14[i]]) or st30[i] is None:
            stats["warmup_none"] += 1; continue

        cur_dir, st_line = st30[i]
        htf_dir = get_htf_dir(candles[i]["t"])
        if htf_dir is None:
            stats["no_htf"] += 1; continue
        if htf_dir != cur_dir:
            stats["no_htf"] += 1; continue

        last_flip = flip_bar.get(cur_dir)
        if last_flip is None or (i - last_flip) < N_BARS_AFTER:
            stats["not_n_bars_after"] += 1; continue

        # Pullback: price touches ST line and reclaims
        if cur_dir == 1:
            touched = candles[i]["l"] <= st_line * 1.005
            if not touched:
                stats["no_pullback"] += 1; continue
            rsi_ok = 40 <= rsi14[i] <= 65
        else:
            touched = candles[i]["h"] >= st_line * 0.995
            if not touched:
                stats["no_pullback"] += 1; continue
            rsi_ok = 35 <= rsi14[i] <= 60

        if not rsi_ok:
            stats["no_rsi"] += 1; continue

        entry = candles[i]["c"]
        sl = entry - cur_dir * 1.0 * atr14[i]
        tp1 = entry + cur_dir * 1.5 * atr14[i]
        tp2 = entry + cur_dir * 2.5 * atr14[i]
        signals.append({"i": i, "direction": cur_dir, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_S3(candles):
    """EMA Stack + Volume Surge (15m)"""
    closes = [c["c"] for c in candles]
    ema9  = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    atr14 = atr(candles, 14)
    vsma  = vol_sma(candles, 20)

    signals = []
    stats = {"warmup_none":0,"no_stack":0,"no_volume":0,"no_rsi":0,"no_pullback":0,"no_atr":0,"signal":0}

    for i in range(2, len(candles)):
        if any(x is None for x in [ema9[i], ema21[i], ema50[i], rsi14[i], atr14[i], vsma[i]]):
            stats["warmup_none"] += 1; continue
        # Stack
        bull_stack = ema9[i] > ema21[i] > ema50[i]
        bear_stack = ema9[i] < ema21[i] < ema50[i]
        if not (bull_stack or bear_stack):
            stats["no_stack"] += 1; continue
        direction = 1 if bull_stack else -1
        # Volatility gate
        if atr14[i] < atr14[i-1] * 0.7:
            stats["no_atr"] += 1; continue
        # Pullback to EMA21
        if direction == 1:
            touched = candles[i-1]["l"] <= ema21[i-1] * 1.003
            reclaimed = candles[i]["c"] > ema21[i]
        else:
            touched = candles[i-1]["h"] >= ema21[i-1] * 0.997
            reclaimed = candles[i]["c"] < ema21[i]
        if not (touched and reclaimed):
            stats["no_pullback"] += 1; continue
        # Volume spike
        if candles[i]["v"] < vsma[i] * 1.5:
            stats["no_volume"] += 1; continue
        # RSI
        if direction == 1 and rsi14[i] <= 50:
            stats["no_rsi"] += 1; continue
        if direction == -1 and rsi14[i] >= 50:
            stats["no_rsi"] += 1; continue

        entry = candles[i]["c"]
        sl = entry - direction * 1.0 * atr14[i]
        tp1 = entry + direction * 1.5 * atr14[i]
        tp2 = entry + direction * 2.5 * atr14[i]
        signals.append({"i": i, "direction": direction, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_S4(candles):
    """Chandelier Exit + EMA200 + RSI/SMA Cross (1H)"""
    closes = [c["c"] for c in candles]
    ema200 = ema(closes, 200)
    rsi25  = rsi(closes, 25)
    atr14  = atr(candles, 14)

    # RSI SMA 150
    rsi_filled = [v if v is not None else 0 for v in rsi25]
    rsi_sma150 = sma(rsi_filled, 150)

    # Chandelier Exit (ATR 1, mult 2.3) — using period=1 means TR-based
    n = len(candles)
    ce_long  = [None] * n  # highest_high(1) - 2.3*ATR(1)
    ce_short = [None] * n
    for i in range(1, n):
        if atr14[i] is None: continue
        ce_long[i]  = candles[i]["h"] - 2.3 * atr14[i]
        ce_short[i] = candles[i]["l"] + 2.3 * atr14[i]

    signals = []
    stats = {"warmup_none":0,"no_ema200":0,"no_ce_flip":0,"no_rsi_cross":0,"signal":0}

    # Pre-compute CE direction per bar
    ce_directions = [0] * n
    for i in range(1, n):
        if ce_long[i] is None or ce_short[i] is None:
            ce_directions[i] = ce_directions[i-1]
            continue
        if closes[i] > ce_long[i]:
            ce_directions[i] = 1
        elif closes[i] < ce_short[i]:
            ce_directions[i] = -1
        else:
            ce_directions[i] = ce_directions[i-1]

    for i in range(2, n):
        if any(x is None for x in [ema200[i], rsi25[i], rsi_sma150[i], atr14[i]]):
            stats["warmup_none"] += 1; continue

        prev_ce_dir = ce_directions[i-1]
        cur_ce_dir  = ce_directions[i]
        flip = (cur_ce_dir != 0 and prev_ce_dir != 0 and cur_ce_dir != prev_ce_dir)
        direction = cur_ce_dir
        if not flip:
            stats["no_ce_flip"] += 1; continue

        direction = ce_dir
        # EMA200 filter
        if direction == 1 and closes[i] < ema200[i]:
            stats["no_ema200"] += 1; continue
        if direction == -1 and closes[i] > ema200[i]:
            stats["no_ema200"] += 1; continue

        # RSI aligned with direction relative to its SMA (crossed in last 5 bars)
        rsi_aligned = False
        for look in range(min(5, i)):
            j = i - look
            if rsi25[j] is None or rsi_sma150[j] is None: continue
            if direction == 1 and rsi25[j] > rsi_sma150[j]:
                rsi_aligned = True; break
            if direction == -1 and rsi25[j] < rsi_sma150[j]:
                rsi_aligned = True; break
        if not rsi_aligned:
            stats["no_rsi_cross"] += 1; continue

        entry = closes[i]
        sl = entry - direction * 1.5 * atr14[i]
        tp1 = entry + direction * 2.0 * atr14[i]
        tp2 = entry + direction * 3.0 * atr14[i]
        signals.append({"i": i, "direction": direction, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_S5(candles):
    """SuperTrend + MACD + BB Breakout + Volume (15m)"""
    closes = [c["c"] for c in candles]
    st = supertrend(candles, 10, 3.0)
    macd_line, sig_line, hist = macd(closes, 12, 26, 9)
    bb_upper, bb_mid, bb_lower = bollinger(closes, 20, 2.0)
    atr14 = atr(candles, 14)
    vsma  = vol_sma(candles, 20)
    rsi14 = rsi(closes, 14)

    signals = []
    stats = {"warmup_none":0,"no_st":0,"no_macd":0,"no_bb":0,"no_volume":0,"no_rsi":0,"no_atr":0,"signal":0}

    for i in range(2, len(candles)):
        if any(x is None for x in [atr14[i], vsma[i], rsi14[i], bb_mid[i], bb_upper[i], bb_lower[i]]):
            stats["warmup_none"] += 1; continue
        if st[i] is None or st[i-1] is None:
            stats["warmup_none"] += 1; continue
        if macd_line[i] is None or sig_line[i] is None:
            stats["warmup_none"] += 1; continue

        cur_dir, _ = st[i]
        prev_dir, _ = st[i-1]
        flip = cur_dir != prev_dir
        if not flip:
            stats["no_st"] += 1; continue

        # Volatility gate
        if atr14[i] / closes[i] < 0.005:
            stats["no_atr"] += 1; continue

        # MACD crossover same direction
        if cur_dir == 1:
            macd_ok = macd_line[i] > sig_line[i] and macd_line[i-1] <= sig_line[i-1]
        else:
            macd_ok = macd_line[i] < sig_line[i] and macd_line[i-1] >= sig_line[i-1]
        if not macd_ok:
            stats["no_macd"] += 1; continue

        # RSI
        if cur_dir == 1 and rsi14[i] <= 50:
            stats["no_rsi"] += 1; continue
        if cur_dir == -1 and rsi14[i] >= 50:
            stats["no_rsi"] += 1; continue

        # BB breakout
        if cur_dir == 1 and closes[i] <= bb_mid[i]:
            stats["no_bb"] += 1; continue
        if cur_dir == -1 and closes[i] >= bb_mid[i]:
            stats["no_bb"] += 1; continue

        # Volume spike
        if candles[i]["v"] < vsma[i] * 1.3:
            stats["no_volume"] += 1; continue

        entry = closes[i]
        sl = entry - cur_dir * 1.0 * atr14[i]
        tp1 = entry + cur_dir * 1.0 * atr14[i]
        tp2 = entry + cur_dir * 2.0 * atr14[i]
        signals.append({"i": i, "direction": cur_dir, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_N1(candles):
    """Liquidity Sweep + Reclaim (30m)"""
    closes = [c["c"] for c in candles]
    atr14 = atr(candles, 14)
    vsma  = vol_sma(candles, 20)
    rsi14 = rsi(closes, 14)
    ema50 = ema(closes, 50)
    LOOKBACK = 15

    signals = []
    stats = {"warmup_none":0,"no_swing":0,"no_sweep":0,"no_reclaim":0,"no_volume":0,"no_rsi":0,"signal":0}

    for i in range(LOOKBACK + 2, len(candles)):
        if any(x is None for x in [atr14[i], vsma[i], rsi14[i], ema50[i]]):
            stats["warmup_none"] += 1; continue

        window = candles[i - LOOKBACK:i]
        swing_high = max(c["h"] for c in window)
        swing_low  = min(c["l"] for c in window)

        bar = candles[i]
        swept_low  = bar["l"] < swing_low and bar["c"] > swing_low   # sweep low, reclaim
        swept_high = bar["h"] > swing_high and bar["c"] < swing_high  # sweep high, reclaim

        if not (swept_low or swept_high):
            stats["no_sweep"] += 1; continue

        direction = 1 if swept_low else -1

        # HTF filter: EMA50 slope
        if i >= 3:
            slope = ema50[i] - ema50[i-3] if ema50[i-3] else 0
            if direction == 1 and slope < 0:
                stats["no_rsi"] += 1; continue
            if direction == -1 and slope > 0:
                stats["no_rsi"] += 1; continue

        # Volume spike on sweep bar
        if bar["v"] < vsma[i] * 1.5:
            stats["no_volume"] += 1; continue

        # RSI not extreme
        if not (30 < rsi14[i] < 70):
            stats["no_rsi"] += 1; continue

        entry = bar["c"]
        if direction == 1:
            sl = swing_low - 0.5 * atr14[i]
            tp1 = entry + 1.5 * atr14[i]
            tp2 = entry + 2.5 * atr14[i]
        else:
            sl = swing_high + 0.5 * atr14[i]
            tp1 = entry - 1.5 * atr14[i]
            tp2 = entry - 2.5 * atr14[i]

        signals.append({"i": i, "direction": direction, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_N2(candles):
    """CVD Divergence + EMA + ATR (15m)"""
    closes = [c["c"] for c in candles]
    ema21 = ema(closes, 21)
    atr14 = atr(candles, 14)
    rsi14 = rsi(closes, 14)
    vsma  = vol_sma(candles, 20)
    cvd   = cvd_approx(candles)
    LOOKBACK = 20

    signals = []
    stats = {"warmup_none":0,"no_divergence":0,"no_rsi":0,"no_atr":0,"no_ema":0,"signal":0}

    for i in range(LOOKBACK + 2, len(candles)):
        if any(x is None for x in [ema21[i], atr14[i], rsi14[i], vsma[i]]):
            stats["warmup_none"] += 1; continue

        window_prices = [candles[j]["c"] for j in range(i - LOOKBACK, i + 1)]
        window_cvd    = cvd[i - LOOKBACK:i + 1]

        price_hh = closes[i] > max(window_prices[:-1])
        price_ll = closes[i] < min(window_prices[:-1])
        cvd_hh   = cvd[i] > max(window_cvd[:-1])
        cvd_ll   = cvd[i] < min(window_cvd[:-1])

        # Bearish divergence: price new high, CVD not
        bear_div = price_hh and not cvd_hh
        # Bullish divergence: price new low, CVD not
        bull_div = price_ll and not cvd_ll

        if not (bear_div or bull_div):
            stats["no_divergence"] += 1; continue

        direction = -1 if bear_div else 1

        # Price near EMA21
        ema_dist = abs(closes[i] - ema21[i]) / ema21[i]
        if ema_dist > 0.02:
            stats["no_ema"] += 1; continue

        # ATR active market
        if atr14[i] < atr14[i-1] * 0.7:
            stats["no_atr"] += 1; continue

        # RSI neutral zone
        if not (35 <= rsi14[i] <= 65):
            stats["no_rsi"] += 1; continue

        entry = closes[i]
        sl  = entry - direction * 1.0 * atr14[i]
        tp1 = entry + direction * 1.5 * atr14[i]
        tp2 = entry + direction * 2.5 * atr14[i]
        signals.append({"i": i, "direction": direction, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_N4(candles):
    """Liquidity Sweep + FVG Fill (30m)"""
    closes = [c["c"] for c in candles]
    atr14 = atr(candles, 14)
    vsma  = vol_sma(candles, 20)
    ema50 = ema(closes, 50)
    LOOKBACK = 15

    signals = []
    stats = {"warmup_none":0,"no_sweep":0,"no_fvg":0,"no_fill":0,"no_volume":0,"signal":0}

    # Pre-compute FVGs
    fvgs = []  # (direction, gap_high, gap_low, bar_idx)
    for i in range(2, len(candles)):
        # Bullish FVG: candle[i].low > candle[i-2].high
        if candles[i]["l"] > candles[i-2]["h"]:
            fvgs.append((1, candles[i]["l"], candles[i-2]["h"], i))
        # Bearish FVG: candle[i].high < candle[i-2].low
        if candles[i]["h"] < candles[i-2]["l"]:
            fvgs.append((-1, candles[i-2]["l"], candles[i]["h"], i))

    fvg_idx = 0
    for i in range(LOOKBACK + 3, len(candles)):
        if any(x is None for x in [atr14[i], vsma[i], ema50[i]]):
            stats["warmup_none"] += 1; continue

        window = candles[i - LOOKBACK:i-1]
        swing_high = max(c["h"] for c in window)
        swing_low  = min(c["l"] for c in window)
        bar = candles[i]

        swept_low  = bar["l"] < swing_low and bar["c"] > swing_low
        swept_high = bar["h"] > swing_high and bar["c"] < swing_high

        if not (swept_low or swept_high):
            stats["no_sweep"] += 1; continue

        direction = 1 if swept_low else -1
        if candles[i]["v"] < vsma[i] * 1.3:
            stats["no_volume"] += 1; continue

        # Look for a FVG in the displacement direction within last 5 bars
        found_fvg = None
        for fg in fvgs:
            fd, fhigh, flow, fidx = fg
            if fd == direction and (i - 5) <= fidx <= i:
                found_fvg = fg
                break

        if found_fvg is None:
            stats["no_fvg"] += 1; continue

        # Enter at 50% of FVG on next pullback — approximate as market entry
        fd, fhigh, flow, fidx = found_fvg
        fvg_mid = (fhigh + flow) / 2

        entry = bar["c"]  # market entry on close
        if direction == 1:
            sl = swing_low - 0.5 * atr14[i]
            tp1 = entry + 1.5 * atr14[i]
            tp2 = entry + 3.0 * atr14[i]
        else:
            sl = swing_high + 0.5 * atr14[i]
            tp1 = entry - 1.5 * atr14[i]
            tp2 = entry - 3.0 * atr14[i]

        signals.append({"i": i, "direction": direction, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

def strategy_N5(candles, htf_candles):
    """MTF RSI Divergence — 15m entry, 1H divergence filter"""
    if not htf_candles:
        return [], {"error": "no_htf_data"}

    closes = [c["c"] for c in candles]
    ema21 = ema(closes, 21)
    atr14 = atr(candles, 14)
    rsi14_ltf = rsi(closes, 14)

    htf_closes = [c["c"] for c in htf_candles]
    rsi14_htf  = rsi(htf_closes, 14)
    LOOKBACK_HTF = 10

    # Build HTF divergence signal map
    htf_div = {}
    for j in range(LOOKBACK_HTF + 1, len(htf_candles)):
        if rsi14_htf[j] is None: continue
        window_p = htf_closes[j - LOOKBACK_HTF:j + 1]
        window_r = rsi14_htf[j - LOOKBACK_HTF:j + 1]
        if None in window_r: continue
        p_hh = htf_closes[j] > max(window_p[:-1])
        p_ll = htf_closes[j] < min(window_p[:-1])
        r_hh = rsi14_htf[j] > max(window_r[:-1])
        r_ll = rsi14_htf[j] < min(window_r[:-1])
        if p_hh and not r_hh:
            htf_div[htf_candles[j]["t"]] = -1
        elif p_ll and not r_ll:
            htf_div[htf_candles[j]["t"]] = 1

    htf_times = sorted(htf_div.keys())

    def get_htf_div(bar_ts):
        HTF_MS = 3600 * 1000
        query = bar_ts - HTF_MS
        best = None
        for t in htf_times:
            if t <= query: best = t
            else: break
        return htf_div.get(best)

    signals = []
    stats = {"warmup_none":0,"no_htf_div":0,"no_rsi_cross":0,"no_ema_slope":0,"no_atr":0,"signal":0}

    for i in range(2, len(candles)):
        if any(x is None for x in [ema21[i], atr14[i], rsi14_ltf[i]]):
            stats["warmup_none"] += 1; continue

        htf_direction = get_htf_div(candles[i]["t"])
        if htf_direction is None:
            stats["no_htf_div"] += 1; continue

        direction = htf_direction
        # RSI crosses 50 in trade direction on LTF
        if direction == 1:
            rsi_cross = rsi14_ltf[i] > 50 and rsi14_ltf[i-1] <= 50
        else:
            rsi_cross = rsi14_ltf[i] < 50 and rsi14_ltf[i-1] >= 50
        if not rsi_cross:
            stats["no_rsi_cross"] += 1; continue

        # EMA slope agrees
        if ema21[i-1] is None:
            stats["no_ema_slope"] += 1; continue
        slope = ema21[i] - ema21[i-1]
        if direction == 1 and slope < 0:
            stats["no_ema_slope"] += 1; continue
        if direction == -1 and slope > 0:
            stats["no_ema_slope"] += 1; continue

        # ATR gate
        if atr14[i] < atr14[i-1] * 0.7:
            stats["no_atr"] += 1; continue

        entry = candles[i]["c"]
        sl  = entry - direction * 1.0 * atr14[i]
        tp1 = entry + direction * 1.5 * atr14[i]
        tp2 = entry + direction * 2.5 * atr14[i]
        signals.append({"i": i, "direction": direction, "sl": sl, "tp1": tp1, "tp2": tp2})
        stats["signal"] += 1

    return signals, stats

# ─────────────────────────── METRICS ───────────────────────────

def compute_metrics(trades, starting_equity):
    if not trades:
        return {"total_trades": 0, "win_rate": 0, "profit_factor": 0,
                "net_pnl": 0, "max_drawdown": 0, "sharpe": 0, "sortino": 0,
                "avg_win": 0, "avg_loss": 0, "expectancy": 0, "avg_duration": 0,
                "long_count": 0, "long_wr": 0, "short_count": 0, "short_wr": 0,
                "max_win_streak": 0, "max_loss_streak": 0}

    wins = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    longs = [t for t in trades if t["direction"] == 1]
    shorts = [t for t in trades if t["direction"] == -1]

    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss   = abs(sum(t["pnl"] for t in losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    avg_win  = gross_profit / len(wins) if wins else 0
    avg_loss = gross_loss / len(losses) if losses else 0
    wr = len(wins) / len(trades)
    expectancy = wr * avg_win - (1 - wr) * avg_loss

    # Equity curve & drawdown
    equity = starting_equity
    peak = equity
    max_dd = 0
    equity_curve = [equity]
    for t in trades:
        equity += t["pnl"]
        if equity > peak: peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd: max_dd = dd
        equity_curve.append(equity)

    # Returns per trade
    returns = [t["pnl"] / starting_equity for t in trades]
    avg_ret = sum(returns) / len(returns) if returns else 0
    std_ret = math.sqrt(sum((r - avg_ret)**2 for r in returns) / len(returns)) if len(returns) > 1 else 0
    sharpe = (avg_ret / std_ret * math.sqrt(252)) if std_ret > 0 else 0
    neg_returns = [r for r in returns if r < 0]
    std_neg = math.sqrt(sum(r**2 for r in neg_returns) / len(neg_returns)) if neg_returns else 0
    sortino = (avg_ret / std_neg * math.sqrt(252)) if std_neg > 0 else 0

    # Streaks
    max_win_streak = max_loss_streak = cur_win = cur_loss = 0
    for t in trades:
        if t["win"]:
            cur_win += 1; cur_loss = 0
        else:
            cur_loss += 1; cur_win = 0
        max_win_streak  = max(max_win_streak, cur_win)
        max_loss_streak = max(max_loss_streak, cur_loss)

    return {
        "total_trades": len(trades),
        "win_rate": round(wr * 100, 2),
        "profit_factor": round(pf, 4),
        "net_pnl": round(sum(t["pnl"] for t in trades), 2),
        "max_drawdown": round(max_dd * 100, 2),
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "expectancy": round(expectancy, 4),
        "avg_duration": round(sum(t["duration"] for t in trades) / len(trades), 1),
        "long_count": len(longs),
        "long_wr": round(len([t for t in longs if t["win"]]) / len(longs) * 100, 2) if longs else 0,
        "short_count": len(shorts),
        "short_wr": round(len([t for t in shorts if t["win"]]) / len(shorts) * 100, 2) if shorts else 0,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
    }

def monthly_pnl(trades, candles):
    """Group trade PnL by month of exit."""
    monthly = {}
    for t in trades:
        if t["exit_i"] >= len(candles): continue
        ts = candles[t["exit_i"]]["t"] / 1000
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] = monthly.get(key, 0) + t["pnl"]
    return {k: round(v, 2) for k, v in sorted(monthly.items())}

# ─────────────────────────── WORKER ───────────────────────────

def worker(args):
    """Single unit of work: (symbol, strategy_id) → result dict"""
    symbol, strat_id = args
    symbol = SYMBOL_REMAP.get(symbol, symbol)

    try:
        tf = STRATEGY_TF[strat_id]
        htf = STRATEGY_HTF.get(strat_id)

        candles = load_symbol_candles(symbol, tf)
        if not candles or len(candles) < 200:
            return {"symbol": symbol, "strategy": strat_id, "status": "insufficient_data",
                    "trades": 0, "win_rate": 0, "profit_factor": 0, "net_pnl": 0}

        htf_candles = None
        if htf:
            htf_candles = load_symbol_candles(symbol, htf)

        # Run strategy
        if strat_id == "S1":
            signals, fstats = strategy_S1(candles)
        elif strat_id == "S2":
            signals, fstats = strategy_S2(candles, htf_candles)
        elif strat_id == "S3":
            signals, fstats = strategy_S3(candles)
        elif strat_id == "S4":
            signals, fstats = strategy_S4(candles)
        elif strat_id == "S5":
            signals, fstats = strategy_S5(candles)
        elif strat_id == "N1":
            signals, fstats = strategy_N1(candles)
        elif strat_id == "N2":
            signals, fstats = strategy_N2(candles)
        elif strat_id == "N4":
            signals, fstats = strategy_N4(candles)
        elif strat_id == "N5":
            signals, fstats = strategy_N5(candles, htf_candles)
        else:
            return {"symbol": symbol, "strategy": strat_id, "status": "unknown_strategy"}

        equity_ref = [STARTING_CAPITAL, 0]
        trades = run_trades(signals, candles, equity_ref)
        metrics = compute_metrics(trades, STARTING_CAPITAL)
        mpnl = monthly_pnl(trades, candles)

        return {
            "symbol": symbol,
            "strategy": strat_id,
            "status": "ok",
            "filter_stats": fstats,
            "monthly_pnl": mpnl,
            "trades_list": [{"dir": t["direction"], "pnl": round(t["pnl"],4),
                             "win": t["win"], "reason": t["exit_reason"],
                             "dur": t["duration"]} for t in trades],
            **metrics,
        }

    except Exception as e:
        return {"symbol": symbol, "strategy": strat_id, "status": "error",
                "error": str(e), "traceback": traceback.format_exc(),
                "trades": 0, "win_rate": 0, "profit_factor": 0, "net_pnl": 0}

# ─────────────────────────── COIN LIST ───────────────────────────

def fetch_top_coins():
    """Fetch all USDM futures symbols from Binance exchange info, return top by known liquidity."""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read())
        symbols = [
            s["symbol"] for s in data["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and s["contractType"] == "PERPETUAL"
        ]
        # Prioritize our known list, then fill from fetched
        ordered = []
        seen = set()
        for s in FALLBACK_COINS:
            s2 = SYMBOL_REMAP.get(s, s)
            if s2 in symbols and s2 not in seen:
                ordered.append(s2); seen.add(s2)
        for s in symbols:
            if s not in seen and len(ordered) < 100:
                ordered.append(s); seen.add(s)
        print(f"[INFO] Fetched {len(symbols)} symbols from Binance, using top {len(ordered)}")
        return ordered[:100]
    except Exception as e:
        print(f"[WARN] Could not fetch live coin list ({e}), using fallback list")
        cleaned = []
        seen = set()
        for s in FALLBACK_COINS:
            s2 = SYMBOL_REMAP.get(s, s)
            if s2 not in seen:
                cleaned.append(s2); seen.add(s2)
        return cleaned[:100]

# ─────────────────────────── AGGREGATION ───────────────────────────

def aggregate_strategy(results):
    """Combine per-coin results into strategy-level aggregate."""
    all_trades = []
    per_coin = []
    for r in results:
        if r.get("status") != "ok": continue
        per_coin.append({
            "symbol": r["symbol"],
            "trades": r["total_trades"],
            "win_rate": r["win_rate"],
            "profit_factor": r["profit_factor"],
            "net_pnl": r["net_pnl"],
            "max_drawdown": r["max_drawdown"],
        })
        for t in r.get("trades_list", []):
            all_trades.append(t)

    per_coin.sort(key=lambda x: x["profit_factor"], reverse=True)
    wins   = [t for t in all_trades if t["win"]]
    losses = [t for t in all_trades if not t["win"]]
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = round(gp / gl, 4) if gl else float("inf")
    wr = round(len(wins) / len(all_trades) * 100, 2) if all_trades else 0
    net = round(sum(t["pnl"] for t in all_trades), 2)

    return {
        "total_trades": len(all_trades),
        "win_rate": wr,
        "profit_factor": pf,
        "net_pnl": net,
        "per_coin": per_coin,
        "usable": pf >= 1.5 and wr >= 42,
    }

# ─────────────────────────── REPORT ───────────────────────────

def write_summary(all_results, coins):
    lines = []
    lines.append("=" * 70)
    lines.append("BACKTEST SUMMARY — 10 Strategies × Top 100 Coins × 3 Years")
    lines.append(f"Period: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    lines.append(f"Capital: ${STARTING_CAPITAL:,.0f} | Risk: {RISK_PER_TRADE*100}% | Fee: {FEE_RATE*100}% | Slip: {SLIPPAGE_RATE*100}%")
    lines.append(f"Max concurrent: {MAX_CONCURRENT} | Workers used: {MAX_WORKERS}")
    lines.append("=" * 70)

    report = {"meta": {
        "period": f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
        "coins": coins, "strategies": STRATEGIES,
        "capital": STARTING_CAPITAL, "risk_pct": RISK_PER_TRADE,
        "fee": FEE_RATE, "slippage": SLIPPAGE_RATE,
        "max_concurrent": MAX_CONCURRENT,
    }, "strategies": {}}

    for strat_id in STRATEGIES:
        strat_results = [r for r in all_results if r.get("strategy") == strat_id]
        agg = aggregate_strategy(strat_results)
        report["strategies"][strat_id] = {
            "aggregate": agg,
            "per_coin_results": strat_results,
        }

        verdict = "✅ USABLE (PF≥1.5, WR≥42%)" if agg["usable"] else "❌ FAILED"
        lines.append(f"\n{'─'*60}")
        lines.append(f"Strategy: {strat_id} ({STRATEGY_TF[strat_id]})")
        lines.append(f"  Total Trades : {agg['total_trades']}")
        lines.append(f"  Win Rate     : {agg['win_rate']}%")
        lines.append(f"  Profit Factor: {agg['profit_factor']}")
        lines.append(f"  Net PnL      : ${agg['net_pnl']:,.2f}")
        lines.append(f"  Verdict      : {verdict}")
        lines.append(f"\n  Top 10 Coins by PF:")
        for pc in agg["per_coin"][:10]:
            flag = "✅" if pc["profit_factor"] >= 1.5 else "❌"
            lines.append(f"    {flag} {pc['symbol']:20s} PF={pc['profit_factor']:.3f} WR={pc['win_rate']:.1f}% Trades={pc['trades']} PnL=${pc['net_pnl']:.2f}")

        # Error/skip summary
        errors = [r for r in strat_results if r.get("status") != "ok"]
        if errors:
            lines.append(f"\n  ⚠️  {len(errors)} coins skipped/errored")

    lines.append("\n" + "=" * 70)
    lines.append("END OF REPORT")

    summary_text = "\n".join(lines)
    with open("backtest_summary.txt", "w") as f:
        f.write(summary_text)
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(summary_text)

# ─────────────────────────── MAIN ───────────────────────────

def main():
    print(f"[INFO] Backtest starting — {len(STRATEGIES)} strategies")
    coins = fetch_top_coins()
    print(f"[INFO] Coins: {len(coins)} | Strategies: {len(STRATEGIES)}")

    # Build all (symbol, strategy) pairs
    tasks = [(coin, strat) for coin in coins for strat in STRATEGIES]
    print(f"[INFO] Total tasks: {len(tasks)} | Workers: {MAX_WORKERS}")

    all_results = []
    completed = 0
    t0 = time.time()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, task): task for task in tasks}
        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)
            completed += 1
            if completed % 50 == 0:
                elapsed = time.time() - t0
                rate = completed / elapsed
                remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                pf = result.get("profit_factor", 0)
                sym = result.get("symbol", "?")
                strat = result.get("strategy", "?")
                print(f"[{completed}/{len(tasks)}] {sym}/{strat} PF={pf} | {elapsed:.0f}s elapsed | ~{remaining:.0f}s remaining")

    elapsed = time.time() - t0
    print(f"\n[INFO] All tasks done in {elapsed:.1f}s")
    write_summary(all_results, coins)

if __name__ == "__main__":
    main()
