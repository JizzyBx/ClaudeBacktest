"""
BACKTEST V6 — LIQUIDITY SWEEP REVERSAL + RSI DIVERGENCE + EMA21 BOUNCE
400 Coins | 1 Year | 30M | Binance USDT-M Perpetuals
6 Configurations: S1-A, S1-B, S1-C, S2-A, S2-B, S2-C
10-coin parallel execution via ThreadPoolExecutor
"""

import json
import math
import time
import datetime
import urllib.request
import urllib.error
import collections
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

START_DATE = "2025-07-01"
END_DATE   = "2026-07-01"
INTERVAL   = "30m"

FEE_RATE    = 0.0005   # 0.05% per side
INITIAL_CAP = 100.0    # per coin
RISK_PCT    = 0.02     # 2% risk cap per trade
LEVERAGE    = 10.0
FIXED_MARGIN         = 1.0   # $1 margin per trade (normal)
FIXED_MARGIN_SHORT_HEAVY = 3.0  # $3 for short-heavy configs
MAX_CONCURRENT = 3
COOLDOWN_BARS  = 1     # 5 min cooldown = ~1 bar on 30m

MIN_RR       = 1.5
MIN_TRADES_TIER = 5    # min trades to appear in per-coin table

# Swing lookbacks
SWING_LOOKBACK_S1  = 20   # for liquidity sweep
SWING_LOOKBACK_S2  = 40   # for RSI divergence
SWING_TARGET_S1    = 15   # for TP structure target
SWING_TARGET_S2    = 20   # for TP structure target

# Auto-disable thresholds
AUTO_DISABLE_MIN_TRADES = 10
AUTO_DISABLE_WR         = 0.35
AUTO_DISABLE_PF         = 0.80

# Volume SMAs
VOL_SMA_S1 = 10
VOL_SMA_S2 = 10
VOL_MULT_S1 = 1.5
VOL_MULT_S2 = 1.2

ATR_PERIOD  = 14
RSI_PERIOD  = 14
ADX_PERIOD  = 14
EMA21_PERIOD = 21

PRINT_LOCK = Lock()

def log(msg):
    with PRINT_LOCK:
        print(msg, flush=True)

# ═══════════════════════════════════════════════════════════
# DATE HELPERS
# ═══════════════════════════════════════════════════════════

def date_to_ms(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)

START_MS = date_to_ms(START_DATE)
END_MS   = date_to_ms(END_DATE)

# ═══════════════════════════════════════════════════════════
# FETCH BINANCE DATA
# ═══════════════════════════════════════════════════════════

BASE_URL = "https://data-api.binance.vision/api/v3/klines"

def fetch_klines(symbol, interval, start_ms, end_ms):
    all_klines = []
    cur = start_ms
    retries = 0
    max_retries = 5

    while cur < end_ms:
        url = (f"{BASE_URL}?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={end_ms}&limit=1000")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            if not data:
                break
            all_klines.extend(data)
            last_ts = data[-1][0]
            if last_ts <= cur:
                break
            cur = last_ts + 1
            retries = 0
            time.sleep(0.13)
        except urllib.error.HTTPError as e:
            if e.code in (400, 451):
                return None  # skip this coin
            retries += 1
            if retries > max_retries:
                return None
            time.sleep(2 ** retries)
        except Exception:
            retries += 1
            if retries > max_retries:
                return None
            time.sleep(2 ** retries)

    return all_klines if len(all_klines) >= 100 else None

def parse_klines(raw):
    opens   = [float(k[1]) for k in raw]
    highs   = [float(k[2]) for k in raw]
    lows    = [float(k[3]) for k in raw]
    closes  = [float(k[4]) for k in raw]
    volumes = [float(k[5]) for k in raw]
    times   = [int(k[0])   for k in raw]
    return opens, highs, lows, closes, volumes, times

def get_all_symbols():
    url = "https://data-api.binance.vision/api/v3/exchangeInfo"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        symbols = []
        for s in data.get("symbols", []):
            sym = s.get("symbol", "")
            status = s.get("status", "")
            qt = s.get("quoteAsset", "")
            if qt == "USDT" and status == "TRADING" and sym.endswith("USDT"):
                symbols.append(sym)
        return sorted(set(symbols))
    except Exception as e:
        log(f"[ERROR] Could not fetch symbol list: {e}")
        return []

# ═══════════════════════════════════════════════════════════
# INDICATORS (pure Python, stdlib only)
# ═══════════════════════════════════════════════════════════

def ema(values, period):
    result = [None] * len(values)
    k = 2.0 / (period + 1)
    started = False
    prev = 0.0
    count = 0
    for i, v in enumerate(values):
        if v is None:
            continue
        if not started:
            count += 1
            prev += v
            if count == period:
                prev /= period
                result[i] = prev
                started = True
        else:
            prev = v * k + prev * (1 - k)
            result[i] = prev
    return result

def sma(values, period):
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        if any(v is None for v in window):
            continue
        result[i] = sum(window) / period
    return result

def atr(highs, lows, closes, period=14):
    n = len(closes)
    tr_list = [None] * n
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr_list[i] = max(h - l, abs(h - pc), abs(l - pc))
    result = [None] * n
    # First ATR = simple average
    if period < n:
        first_vals = [tr_list[j] for j in range(1, period + 1) if tr_list[j] is not None]
        if len(first_vals) == period:
            prev = sum(first_vals) / period
            result[period] = prev
            for i in range(period + 1, n):
                if tr_list[i] is not None:
                    prev = (prev * (period - 1) + tr_list[i]) / period
                    result[i] = prev
    return result

def rsi(closes, period=14):
    n = len(closes)
    result = [None] * n
    if n < period + 1:
        return result
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    if avg_l == 0:
        result[period] = 100.0
    else:
        rs = avg_g / avg_l
        result[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, n):
        diff = closes[i] - closes[i-1]
        g = max(diff, 0)
        l = max(-diff, 0)
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
    adx_out = [None] * n
    pdi_out = [None] * n
    mdi_out = [None] * n
    if n < period * 2 + 1:
        return adx_out, pdi_out, mdi_out

    tr_list, dm_plus, dm_minus = [], [], []
    for i in range(1, n):
        h, l, ph, pl, pc = highs[i], lows[i], highs[i-1], lows[i-1], closes[i-1]
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
        up = h - ph
        dn = pl - l
        dm_plus.append(up if up > dn and up > 0 else 0)
        dm_minus.append(dn if dn > up and dn > 0 else 0)

    def wilder_smooth(lst, p):
        out = [None] * len(lst)
        if len(lst) < p:
            return out
        first = sum(lst[:p])
        out[p-1] = first
        for i in range(p, len(lst)):
            out[i] = out[i-1] - out[i-1]/p + lst[i]
        return out

    s_tr  = wilder_smooth(tr_list, period)
    s_dmp = wilder_smooth(dm_plus, period)
    s_dmm = wilder_smooth(dm_minus, period)

    di_plus  = [None] * (n - 1)
    di_minus = [None] * (n - 1)
    dx_list  = []

    for i in range(len(s_tr)):
        if s_tr[i] and s_tr[i] != 0:
            dip = 100 * s_dmp[i] / s_tr[i] if s_dmp[i] is not None else None
            dim = 100 * s_dmm[i] / s_tr[i] if s_dmm[i] is not None else None
            di_plus[i]  = dip
            di_minus[i] = dim
            if dip is not None and dim is not None:
                denom = dip + dim
                if denom != 0:
                    dx_list.append((i, abs(dip - dim) / denom * 100))

    if len(dx_list) < period:
        return adx_out, pdi_out, mdi_out

    # First ADX
    first_dx_vals = [v for _, v in dx_list[:period]]
    adx_val = sum(first_dx_vals) / period
    base_i = dx_list[period-1][0]
    # offset into original arrays
    off = 1  # because tr_list starts at index 1 in original
    adx_out[base_i + off] = adx_val
    pdi_out[base_i + off] = di_plus[base_i]
    mdi_out[base_i + off] = di_minus[base_i]

    for j in range(period, len(dx_list)):
        idx, dx_val = dx_list[j]
        adx_val = (adx_val * (period - 1) + dx_val) / period
        adx_out[idx + off] = adx_val
        pdi_out[idx + off] = di_plus[idx]
        mdi_out[idx + off] = di_minus[idx]

    return adx_out, pdi_out, mdi_out

def vsma(volumes, period=20):
    return sma(volumes, period)

# ═══════════════════════════════════════════════════════════
# SWING HIGH / LOW DETECTION
# ═══════════════════════════════════════════════════════════

def get_swing_high(highs, end_idx, lookback):
    start = max(0, end_idx - lookback)
    window = highs[start:end_idx]
    return max(window) if window else None

def get_swing_low(lows, end_idx, lookback):
    start = max(0, end_idx - lookback)
    window = lows[start:end_idx]
    return min(window) if window else None

def get_prev_swing_high(highs, end_idx, lookback):
    """Get the highest high in [end_idx-lookback, end_idx-lookback//2]"""
    start = max(0, end_idx - lookback)
    mid   = max(0, end_idx - lookback // 2)
    window = highs[start:mid]
    return max(window) if window else None

def get_prev_swing_low(lows, end_idx, lookback):
    start = max(0, end_idx - lookback)
    mid   = max(0, end_idx - lookback // 2)
    window = lows[start:mid]
    return min(window) if window else None

def get_next_swing_low(lows, start_idx, lookback):
    end = min(len(lows), start_idx + lookback)
    window = lows[start_idx:end]
    return min(window) if window else None

def get_next_swing_high(highs, start_idx, lookback):
    end = min(len(highs), start_idx + lookback)
    window = highs[start_idx:end]
    return max(window) if window else None

# ═══════════════════════════════════════════════════════════
# TRADE EXECUTION HELPERS
# ═══════════════════════════════════════════════════════════

def calc_pnl(direction, entry, exit_price, qty):
    if direction == "LONG":
        raw = (exit_price - entry) * qty
    else:
        raw = (entry - exit_price) * qty
    fee = (entry + exit_price) * qty * FEE_RATE
    return raw - fee

def simulate_exit(direction, entry, sl, tp, highs, lows, start_bar, n):
    for i in range(start_bar, n):
        h, l = highs[i], lows[i]
        if direction == "LONG":
            if l <= sl:
                return sl, i, False
            if h >= tp:
                return tp, i, True
        else:
            if h >= sl:
                return sl, i, False
            if l <= tp:
                return tp, i, True
    # Exit at last bar close
    return None, n - 1, False

# ═══════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════

def calc_metrics(trades, symbol=""):
    if not trades:
        return None
    n = len(trades)
    wins  = [t for t in trades if t["win"]]
    losses= [t for t in trades if not t["win"]]
    wr    = len(wins) / n
    gp    = sum(t["pnl"] for t in wins)
    gl    = abs(sum(t["pnl"] for t in losses)) if losses else 0
    pf    = gp / gl if gl > 0 else (float("inf") if gp > 0 else 0)
    net   = sum(t["pnl"] for t in trades)
    aw    = gp / len(wins)  if wins   else 0
    al    = gl / len(losses) if losses else 0
    exp   = wr * aw - (1 - wr) * al

    # MDD
    balance = INITIAL_CAP
    peak = balance
    mdd  = 0.0
    for t in trades:
        balance += t["pnl"]
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak if peak > 0 else 0
        if dd > mdd:
            mdd = dd

    # Sharpe / Sortino
    pnls = [t["pnl"] for t in trades]
    try:
        avg_pnl = statistics.mean(pnls)
        std_pnl = statistics.stdev(pnls) if len(pnls) > 1 else 0
        sharpe  = avg_pnl / std_pnl * math.sqrt(252 * 48) if std_pnl > 0 else 0
        neg = [p for p in pnls if p < 0]
        down_std = statistics.stdev(neg) if len(neg) > 1 else 0
        sortino = avg_pnl / down_std * math.sqrt(252 * 48) if down_std > 0 else 0
    except Exception:
        sharpe, sortino = 0, 0

    # Direction stats
    longs  = [t for t in trades if t["dir"] == "LONG"]
    shorts = [t for t in trades if t["dir"] == "SHORT"]
    lwr    = len([t for t in longs  if t["win"]]) / len(longs)  if longs  else 0
    swr    = len([t for t in shorts if t["win"]]) / len(shorts) if shorts else 0

    # Monthly PnL
    monthly = collections.defaultdict(float)
    for t in trades:
        mo = datetime.datetime.utcfromtimestamp(t["entry_t"]/1000).strftime("%Y-%m")
        monthly[mo] += t["pnl"]

    # Max consec win/loss
    max_cw, max_cl, cw, cl = 0, 0, 0, 0
    for t in trades:
        if t["win"]:
            cw += 1; cl = 0
        else:
            cl += 1; cw = 0
        max_cw = max(max_cw, cw)
        max_cl = max(max_cl, cl)

    # Avg R:R
    rr_vals = [t.get("rr", 0) for t in trades if t.get("rr", 0) > 0]
    avg_rr  = sum(rr_vals) / len(rr_vals) if rr_vals else 0

    return {
        "symbol": symbol,
        "n": n, "wins": len(wins), "losses": len(losses),
        "wr": wr, "lwr": lwr, "swr": swr,
        "pf": pf, "net": net, "mdd": mdd,
        "sharpe": sharpe, "sortino": sortino,
        "aw": aw, "al": al, "exp": exp,
        "gp": gp, "gl": gl,
        "nlongs": len(longs), "nshorts": len(shorts),
        "monthly": dict(monthly),
        "max_cw": max_cw, "max_cl": max_cl,
        "avg_rr": avg_rr,
    }

# ═══════════════════════════════════════════════════════════
# STRATEGY 1: LIQUIDITY SWEEP REVERSAL
# ═══════════════════════════════════════════════════════════

def run_s1(symbol, opens, highs, lows, closes, volumes, times, config):
    """
    config: "A" = Short only, "B" = Long+Short equal, "C" = Short heavy
    """
    n = len(closes)
    atr_vals  = atr(highs, lows, closes, ATR_PERIOD)
    vol_sma   = vsma(volumes, VOL_SMA_S1)
    rsi_vals  = rsi(closes, RSI_PERIOD)

    trades = []
    filter_counts = collections.defaultdict(int)
    disabled = False
    cooldown_until = -1

    open_positions = 0  # tracked globally would need lock; approximate per-coin
    # Per-symbol we track max 1 position at a time

    in_trade = False
    trade_exit_bar = -1

    for i in range(SWING_LOOKBACK_S1 + 5, n - 1):
        if disabled:
            break

        # Auto-disable check
        if len(trades) >= AUTO_DISABLE_MIN_TRADES:
            recent = trades[-AUTO_DISABLE_MIN_TRADES:]
            r_wr = len([t for t in recent if t["win"]]) / AUTO_DISABLE_MIN_TRADES
            r_gp = sum(t["pnl"] for t in recent if t["win"])
            r_gl = abs(sum(t["pnl"] for t in recent if not t["win"]))
            r_pf = r_gp / r_gl if r_gl > 0 else (float("inf") if r_gp > 0 else 0)
            if r_wr < AUTO_DISABLE_WR and r_pf < AUTO_DISABLE_PF:
                disabled = True
                break

        if i <= trade_exit_bar + COOLDOWN_BARS:
            filter_counts["cooldown"] += 1
            continue
        if in_trade:
            continue

        atr_v   = atr_vals[i]
        vol_v   = volumes[i]
        vol_s   = vol_sma[i]
        rsi_v   = rsi_vals[i]

        if atr_v is None or vol_s is None or rsi_v is None:
            filter_counts["warmup"] += 1
            continue

        swing_high = get_swing_high(highs, i, SWING_LOOKBACK_S1)
        swing_low  = get_swing_low(lows,  i, SWING_LOOKBACK_S1)

        # ── SHORT SETUP ──
        if config in ("A", "B", "C"):
            sh = swing_high
            if sh is not None:
                swept  = highs[i] > sh
                closed_below = closes[i] < sh
                wick_size    = highs[i] - closes[i]
                wick_ok      = wick_size > 0.3 * atr_v
                vol_ok       = vol_v > VOL_MULT_S1 * vol_s

                filter_counts["s1_short_scanned"] += 1
                if not swept:
                    filter_counts["s1_short_no_sweep"] += 1
                elif not closed_below:
                    filter_counts["s1_short_no_close_below"] += 1
                elif not wick_ok:
                    filter_counts["s1_short_wick_too_small"] += 1
                elif not vol_ok:
                    filter_counts["s1_short_vol_fail"] += 1
                else:
                    # RSI divergence check (optional)
                    prev_sh  = get_prev_swing_high(highs, i, SWING_LOOKBACK_S1)
                    prev_rsi = None
                    if prev_sh is not None:
                        # Find RSI at previous swing high approximate bar
                        for rb in range(max(0, i - SWING_LOOKBACK_S1), i):
                            if highs[rb] >= prev_sh * 0.999 and rsi_vals[rb] is not None:
                                prev_rsi = rsi_vals[rb]
                                break
                    rsi_div = True
                    if prev_rsi is not None and rsi_v is not None:
                        # Bearish divergence: price higher high, RSI lower high
                        rsi_div = (highs[i] > (prev_sh or 0)) and (rsi_v < prev_rsi)

                    # TP = next swing low
                    tp_level = get_next_swing_low(lows, i + 1, SWING_TARGET_S1)
                    sl_level = highs[i] + 0.5 * atr_v

                    if tp_level is None:
                        filter_counts["s1_short_no_tp"] += 1
                    else:
                        entry = closes[i] * (1 - FEE_RATE)
                        dist_sl = abs(sl_level - entry)
                        dist_tp = abs(entry - tp_level)
                        if dist_tp <= 0 or (dist_sl > 0 and dist_tp / dist_sl < MIN_RR):
                            filter_counts["s1_short_rr_fail"] += 1
                        else:
                            # Position sizing
                            notional = FIXED_MARGIN * LEVERAGE
                            if config == "C":
                                notional = FIXED_MARGIN_SHORT_HEAVY * LEVERAGE
                            qty = notional / entry
                            risk_cap_qty = (INITIAL_CAP * RISK_PCT) / (dist_sl * LEVERAGE) if dist_sl > 0 else qty
                            qty = min(qty, risk_cap_qty)

                            exit_p, exit_bar, hit_tp = simulate_exit(
                                "SHORT", entry, sl_level, tp_level,
                                highs, lows, i + 1, n)
                            if exit_p is None:
                                exit_p = closes[-1]
                                exit_bar = n - 1
                                hit_tp = False

                            pnl = calc_pnl("SHORT", entry, exit_p, qty)
                            rr  = dist_tp / dist_sl if dist_sl > 0 else 0

                            trades.append({
                                "dir": "SHORT", "entry": entry, "exit": exit_p,
                                "entry_t": times[i], "exit_t": times[min(exit_bar, n-1)],
                                "pnl": pnl, "win": hit_tp,
                                "sl": sl_level, "tp": tp_level,
                                "rr": rr, "risk": dist_sl * qty,
                                "rsi_div": rsi_div,
                            })
                            trade_exit_bar = exit_bar
                            filter_counts["s1_short_trades"] += 1
                            continue

        # ── LONG SETUP ──
        if config in ("B", "C"):
            sl2 = swing_low
            if sl2 is not None:
                swept  = lows[i] < sl2
                closed_above = closes[i] > sl2
                wick_size    = closes[i] - lows[i]
                wick_ok      = wick_size > 0.3 * atr_v
                vol_ok       = vol_v > VOL_MULT_S1 * vol_s

                filter_counts["s1_long_scanned"] += 1
                if swept and closed_above and wick_ok and vol_ok:
                    tp_level = get_next_swing_high(highs, i + 1, SWING_TARGET_S1)
                    sl_level = lows[i] - 0.5 * atr_v

                    if tp_level is not None:
                        entry = closes[i] * (1 + FEE_RATE)
                        dist_sl = abs(entry - sl_level)
                        dist_tp = abs(tp_level - entry)
                        if dist_tp > 0 and dist_sl > 0 and dist_tp / dist_sl >= MIN_RR:
                            notional = FIXED_MARGIN * LEVERAGE
                            qty = notional / entry
                            risk_cap_qty = (INITIAL_CAP * RISK_PCT) / (dist_sl * LEVERAGE) if dist_sl > 0 else qty
                            qty = min(qty, risk_cap_qty)

                            exit_p, exit_bar, hit_tp = simulate_exit(
                                "LONG", entry, sl_level, tp_level,
                                highs, lows, i + 1, n)
                            if exit_p is None:
                                exit_p = closes[-1]
                                exit_bar = n - 1
                                hit_tp = False

                            pnl = calc_pnl("LONG", entry, exit_p, qty)
                            rr  = dist_tp / dist_sl

                            trades.append({
                                "dir": "LONG", "entry": entry, "exit": exit_p,
                                "entry_t": times[i], "exit_t": times[min(exit_bar, n-1)],
                                "pnl": pnl, "win": hit_tp,
                                "sl": sl_level, "tp": tp_level,
                                "rr": rr, "risk": dist_sl * qty,
                            })
                            trade_exit_bar = exit_bar
                            filter_counts["s1_long_trades"] += 1

    return trades, dict(filter_counts), disabled

# ═══════════════════════════════════════════════════════════
# STRATEGY 2: RSI DIVERGENCE + EMA21 BOUNCE
# ═══════════════════════════════════════════════════════════

def run_s2(symbol, opens, highs, lows, closes, volumes, times, config):
    """
    config: "A" = Short only, "B" = Long+Short equal, "C" = Short heavy
    """
    n = len(closes)
    atr_vals  = atr(highs, lows, closes, ATR_PERIOD)
    rsi_vals  = rsi(closes, RSI_PERIOD)
    ema21_vals = ema(closes, EMA21_PERIOD)
    vol_sma   = vsma(volumes, VOL_SMA_S2)
    adx_vals, pdi_vals, mdi_vals = adx_full(highs, lows, closes, ADX_PERIOD)

    trades = []
    filter_counts = collections.defaultdict(int)
    disabled = False
    trade_exit_bar = -1

    for i in range(SWING_LOOKBACK_S2 + 5, n - 1):
        if disabled:
            break

        # Auto-disable check
        if len(trades) >= AUTO_DISABLE_MIN_TRADES:
            recent = trades[-AUTO_DISABLE_MIN_TRADES:]
            r_wr = len([t for t in recent if t["win"]]) / AUTO_DISABLE_MIN_TRADES
            r_gp = sum(t["pnl"] for t in recent if t["win"])
            r_gl = abs(sum(t["pnl"] for t in recent if not t["win"]))
            r_pf = r_gp / r_gl if r_gl > 0 else (float("inf") if r_gp > 0 else 0)
            if r_wr < AUTO_DISABLE_WR and r_pf < AUTO_DISABLE_PF:
                disabled = True
                break

        if i <= trade_exit_bar + COOLDOWN_BARS:
            filter_counts["cooldown"] += 1
            continue

        atr_v   = atr_vals[i]
        rsi_v   = rsi_vals[i]
        ema21_v = ema21_vals[i]
        vol_v   = volumes[i]
        vol_s   = vol_sma[i]
        adx_v   = adx_vals[i]

        if any(v is None for v in [atr_v, rsi_v, ema21_v, vol_s, adx_v]):
            filter_counts["warmup"] += 1
            continue

        # ── ADX filter: avoid extreme trends ──
        if adx_v >= 30:
            filter_counts["s2_adx_too_high"] += 1
            continue

        # Get previous RSI extremes for divergence
        # Look back SWING_LOOKBACK_S2 bars for RSI pivots
        lookback_rsi = [rsi_vals[j] for j in range(max(0, i - SWING_LOOKBACK_S2), i) if rsi_vals[j] is not None]
        if len(lookback_rsi) < 10:
            continue

        # ── SHORT SETUP ──
        if config in ("A", "B", "C"):
            filter_counts["s2_short_scanned"] += 1
            # Price makes higher high vs prev 40 candles
            price_hh = highs[i]
            prev_hh  = get_prev_swing_high(highs, i, SWING_LOOKBACK_S2)
            rsi_prev_hh = max(lookback_rsi)
            # Bearish div: price HH but RSI lower high
            price_made_hh = prev_hh is not None and price_hh > prev_hh
            rsi_div_bear  = rsi_v < rsi_prev_hh

            # EMA21: touched or went above, then closed below
            touched_above = highs[i] >= ema21_v
            closed_below  = closes[i] < ema21_v
            bearish_candle = closes[i] < opens[i]
            vol_ok = vol_v > VOL_MULT_S2 * vol_s

            if not price_made_hh:
                filter_counts["s2_short_no_hh"] += 1
            elif not rsi_div_bear:
                filter_counts["s2_short_no_rsi_div"] += 1
            elif not (touched_above and closed_below):
                filter_counts["s2_short_no_ema_reject"] += 1
            elif not bearish_candle:
                filter_counts["s2_short_not_bearish"] += 1
            elif not vol_ok:
                filter_counts["s2_short_vol_fail"] += 1
            else:
                # Exit logic
                tp_level = get_next_swing_low(lows, i + 1, SWING_TARGET_S2)
                sl_level = price_hh + 0.5 * atr_v  # above the divergence high

                if tp_level is None:
                    filter_counts["s2_short_no_tp"] += 1
                else:
                    entry = closes[i] * (1 - FEE_RATE)
                    dist_sl = abs(sl_level - entry)
                    dist_tp = abs(entry - tp_level)
                    if dist_tp <= 0 or dist_sl <= 0 or dist_tp / dist_sl < MIN_RR:
                        filter_counts["s2_short_rr_fail"] += 1
                    else:
                        notional = FIXED_MARGIN * LEVERAGE
                        if config == "C":
                            notional = FIXED_MARGIN_SHORT_HEAVY * LEVERAGE
                        qty = notional / entry
                        risk_cap_qty = (INITIAL_CAP * RISK_PCT) / (dist_sl * LEVERAGE) if dist_sl > 0 else qty
                        qty = min(qty, risk_cap_qty)

                        exit_p, exit_bar, hit_tp = simulate_exit(
                            "SHORT", entry, sl_level, tp_level,
                            highs, lows, i + 1, n)
                        if exit_p is None:
                            exit_p = closes[-1]
                            exit_bar = n - 1
                            hit_tp = False

                        pnl = calc_pnl("SHORT", entry, exit_p, qty)
                        rr  = dist_tp / dist_sl

                        trades.append({
                            "dir": "SHORT", "entry": entry, "exit": exit_p,
                            "entry_t": times[i], "exit_t": times[min(exit_bar, n-1)],
                            "pnl": pnl, "win": hit_tp,
                            "sl": sl_level, "tp": tp_level,
                            "rr": rr, "risk": dist_sl * qty,
                        })
                        trade_exit_bar = exit_bar
                        filter_counts["s2_short_trades"] += 1
                        continue

        # ── LONG SETUP ──
        if config in ("B", "C"):
            filter_counts["s2_long_scanned"] += 1
            price_ll = lows[i]
            prev_ll  = get_prev_swing_low(lows, i, SWING_LOOKBACK_S2)
            rsi_prev_ll = min(lookback_rsi)
            price_made_ll = prev_ll is not None and price_ll < prev_ll
            rsi_div_bull  = rsi_v > rsi_prev_ll

            touched_below  = lows[i] <= ema21_v
            closed_above   = closes[i] > ema21_v
            bullish_candle = closes[i] > opens[i]
            vol_ok = vol_v > VOL_MULT_S2 * vol_s

            if price_made_ll and rsi_div_bull and touched_below and closed_above and bullish_candle and vol_ok:
                tp_level = get_next_swing_high(highs, i + 1, SWING_TARGET_S2)
                sl_level = price_ll - 0.5 * atr_v

                if tp_level is not None:
                    entry = closes[i] * (1 + FEE_RATE)
                    dist_sl = abs(entry - sl_level)
                    dist_tp = abs(tp_level - entry)
                    if dist_tp > 0 and dist_sl > 0 and dist_tp / dist_sl >= MIN_RR:
                        notional = FIXED_MARGIN * LEVERAGE
                        qty = notional / entry
                        risk_cap_qty = (INITIAL_CAP * RISK_PCT) / (dist_sl * LEVERAGE) if dist_sl > 0 else qty
                        qty = min(qty, risk_cap_qty)

                        exit_p, exit_bar, hit_tp = simulate_exit(
                            "LONG", entry, sl_level, tp_level,
                            highs, lows, i + 1, n)
                        if exit_p is None:
                            exit_p = closes[-1]
                            exit_bar = n - 1
                            hit_tp = False

                        pnl = calc_pnl("LONG", entry, exit_p, qty)
                        rr  = dist_tp / dist_sl

                        trades.append({
                            "dir": "LONG", "entry": entry, "exit": exit_p,
                            "entry_t": times[i], "exit_t": times[min(exit_bar, n-1)],
                            "pnl": pnl, "win": hit_tp,
                            "sl": sl_level, "tp": tp_level,
                            "rr": rr, "risk": dist_sl * qty,
                        })
                        trade_exit_bar = exit_bar
                        filter_counts["s2_long_trades"] += 1

    return trades, dict(filter_counts), disabled

# ═══════════════════════════════════════════════════════════
# PROCESS ONE COIN
# ═══════════════════════════════════════════════════════════

def process_coin(symbol):
    raw = fetch_klines(symbol, INTERVAL, START_MS, END_MS)
    if raw is None or len(raw) < 200:
        return symbol, None, "SKIPPED"

    opens, highs, lows, closes, volumes, times = parse_klines(raw)

    results = {}
    for strat in ["S1", "S2"]:
        for cfg in ["A", "B", "C"]:
            key = f"{strat}-{cfg}"
            try:
                if strat == "S1":
                    trades, filters, disabled = run_s1(
                        symbol, opens, highs, lows, closes, volumes, times, cfg)
                else:
                    trades, filters, disabled = run_s2(
                        symbol, opens, highs, lows, closes, volumes, times, cfg)
                m = calc_metrics(trades, symbol)
                results[key] = {
                    "metrics": m,
                    "trades": trades,
                    "filters": filters,
                    "disabled": disabled,
                    "n_candles": len(closes),
                }
            except Exception as e:
                results[key] = {"metrics": None, "error": str(e), "disabled": False}

    # Print live progress for S1-B as representative
    rep = results.get("S1-B", {})
    rm  = rep.get("metrics")
    if rm:
        log(f"  ✓ {symbol:20s} | S1-B: {rm['n']:4d} trades | PF {rm['pf']:.3f} | WR {rm['wr']*100:.1f}%")
    else:
        log(f"  ✗ {symbol:20s} | S1-B: no trades")

    return symbol, results, "OK"

# ═══════════════════════════════════════════════════════════
# AGGREGATE METRICS ACROSS COINS
# ═══════════════════════════════════════════════════════════

def aggregate_config(all_coin_results, config_key):
    all_trades = []
    coin_summaries = []
    total_filters  = collections.defaultdict(int)
    disabled_coins = []

    for symbol, res in all_coin_results.items():
        if res is None:
            continue
        cfg_res = res.get(config_key)
        if not cfg_res:
            continue
        m = cfg_res.get("metrics")
        if m and m["n"] >= MIN_TRADES_TIER:
            coin_summaries.append(m)
        all_trades.extend(cfg_res.get("trades", []))
        for k, v in cfg_res.get("filters", {}).items():
            total_filters[k] += v
        if cfg_res.get("disabled"):
            disabled_coins.append(symbol)

    agg = calc_metrics(all_trades, "AGGREGATE")
    return agg, coin_summaries, dict(total_filters), disabled_coins

def tier_coins(coin_summaries):
    tier1, tier2, tier3 = [], [], []
    for m in coin_summaries:
        if m["n"] >= 15 and m["wr"] >= 0.45 and m["pf"] >= 1.5 and m["net"] > 0:
            tier1.append(m)
        elif m["n"] >= 10 and m["wr"] >= 0.40 and m["pf"] >= 1.2 and m["net"] > 0:
            tier2.append(m)
        else:
            tier3.append(m)
    tier1.sort(key=lambda x: x["pf"], reverse=True)
    tier2.sort(key=lambda x: x["pf"], reverse=True)
    tier3.sort(key=lambda x: x["pf"], reverse=True)
    return tier1, tier2, tier3

# ═══════════════════════════════════════════════════════════
# REPORT WRITING
# ═══════════════════════════════════════════════════════════

def fmt_pct(v): return f"{v*100:.2f}%"
def fmt_f(v, d=4): return f"{v:.{d}f}"

def write_summary(all_coin_results, skipped, config_aggregates, outfile):
    lines = []
    lines.append("═"*80)
    lines.append("BACKTEST V6 — LIQUIDITY SWEEP & RSI DIVERGENCE")
    lines.append(f"Period: {START_DATE} → {END_DATE} | Interval: {INTERVAL}")
    lines.append(f"Coins tested: {len(all_coin_results)} | Skipped: {len(skipped)}")
    lines.append("═"*80)

    config_keys = ["S1-A","S1-B","S1-C","S2-A","S2-B","S2-C"]
    config_labels = {
        "S1-A": "Liquidity Sweep — Short Only",
        "S1-B": "Liquidity Sweep — Long + Short",
        "S1-C": "Liquidity Sweep — Short Heavy",
        "S2-A": "RSI Divergence — Short Only",
        "S2-B": "RSI Divergence — Long + Short",
        "S2-C": "RSI Divergence — Short Heavy",
    }

    # ── CONFIG COMPARISON TABLE ──
    lines.append("")
    lines.append("CONFIG COMPARISON TABLE")
    lines.append("─"*80)
    hdr = f"{'Config':<8} {'Strategy':<35} {'Trades':>7} {'WR':>7} {'PF':>7} {'Net$':>9} {'MDD':>7} {'Sharpe':>7}"
    lines.append(hdr)
    lines.append("─"*80)

    best_pf = 0
    best_cfg = None
    for ck in config_keys:
        agg = config_aggregates[ck]["agg"]
        if agg:
            pf_v = agg["pf"]
            if pf_v > best_pf:
                best_pf = pf_v
                best_cfg = ck
            lines.append(
                f"{ck:<8} {config_labels[ck]:<35} {agg['n']:>7} "
                f"{fmt_pct(agg['wr']):>7} {fmt_f(agg['pf'],3):>7} "
                f"{agg['net']:>9.2f} {fmt_pct(agg['mdd']):>7} "
                f"{fmt_f(agg['sharpe'],2):>7}"
            )
        else:
            lines.append(f"{ck:<8} {config_labels[ck]:<35} {'NO DATA':>7}")
    lines.append("─"*80)
    lines.append(f"★ BEST CONFIG: {best_cfg} — {config_labels.get(best_cfg,'')}")

    # ── PER CONFIG DETAILS ──
    for ck in config_keys:
        agg   = config_aggregates[ck]["agg"]
        coins = config_aggregates[ck]["coins"]
        filt  = config_aggregates[ck]["filters"]
        dis   = config_aggregates[ck]["disabled"]
        t1, t2, t3 = tier_coins(coins)

        lines.append("")
        lines.append("═"*80)
        lines.append(f"CONFIG {ck}: {config_labels[ck]}")
        lines.append("═"*80)

        if not agg:
            lines.append("  [NO DATA]")
            continue

        lines.append(f"  Total Trades     : {agg['n']}")
        lines.append(f"  Win Rate         : {fmt_pct(agg['wr'])} (L:{fmt_pct(agg['lwr'])} / S:{fmt_pct(agg['swr'])})")
        lines.append(f"  Profit Factor    : {fmt_f(agg['pf'],4)}")
        lines.append(f"  Net PnL          : ${agg['net']:.2f} ({agg['net']/INITIAL_CAP/len([s for s in all_coin_results])*100:.2f}% of total capital)")
        lines.append(f"  Max Drawdown     : {fmt_pct(agg['mdd'])}")
        lines.append(f"  Avg Win          : ${agg['aw']:.4f}  |  Avg Loss: ${agg['al']:.4f}")
        lines.append(f"  Expectancy       : ${agg['exp']:.4f} / trade")
        lines.append(f"  Sharpe           : {fmt_f(agg['sharpe'],3)}")
        lines.append(f"  Avg R:R          : {fmt_f(agg['avg_rr'],3)}")
        lines.append(f"  Longs / Shorts   : {agg['nlongs']} / {agg['nshorts']}")
        lines.append(f"  Auto-Disabled    : {len(dis)} coins")

        # Validation
        pf_ok = sum(1 for c in coins if c["pf"] >= 1.5)
        wr_ok = sum(1 for c in coins if c["wr"] >= 0.42)
        lines.append(f"  Validation       : PF≥1.5 on {pf_ok}/{len(coins)} coins | WR≥42% on {wr_ok}/{len(coins)} coins")

        # Filter stats
        lines.append("")
        lines.append("  FILTER STATS:")
        for fk, fv in sorted(filt.items(), key=lambda x: -x[1]):
            lines.append(f"    {fk:<40}: {fv:>8,}")

        # Tiers
        lines.append("")
        lines.append(f"  TIER 1 — ELITE ({len(t1)} coins): WR≥45% | PF≥1.5 | Net>$0 | 15+ trades")
        if t1:
            lines.append(f"  {'Symbol':<16} {'Trades':>6} {'WR':>7} {'PF':>7} {'Net$':>8} {'MDD':>7}")
            for m in t1:
                lines.append(f"  {m['symbol']:<16} {m['n']:>6} {fmt_pct(m['wr']):>7} {fmt_f(m['pf'],3):>7} {m['net']:>8.3f} {fmt_pct(m['mdd']):>7}")
        else:
            lines.append("  [NONE]")

        lines.append("")
        lines.append(f"  TIER 2 — MONITOR ({len(t2)} coins): WR≥40% | PF≥1.2 | Net>$0 | 10+ trades")
        if t2:
            lines.append(f"  {'Symbol':<16} {'Trades':>6} {'WR':>7} {'PF':>7} {'Net$':>8}")
            for m in t2[:30]:
                lines.append(f"  {m['symbol']:<16} {m['n']:>6} {fmt_pct(m['wr']):>7} {fmt_f(m['pf'],3):>7} {m['net']:>8.3f}")
        else:
            lines.append("  [NONE]")

        lines.append("")
        lines.append(f"  PER-COIN TABLE (all coins with ≥{MIN_TRADES_TIER} trades, sorted by PF desc):")
        all_coins_sorted = sorted(coins, key=lambda x: x["pf"], reverse=True)
        lines.append(f"  {'Symbol':<16} {'Trades':>6} {'WR':>7} {'PF':>7} {'Net$':>8} {'MDD':>7} {'Shr':>6}")
        for m in all_coins_sorted:
            flag = " ★T1" if m in t1 else (" ◆T2" if m in t2 else "")
            outlier = " [OUTLIER]" if m["pf"] > 5.0 and m["n"] < 15 else ""
            lines.append(
                f"  {m['symbol']:<16} {m['n']:>6} {fmt_pct(m['wr']):>7} "
                f"{fmt_f(m['pf'],3):>7} {m['net']:>8.3f} {fmt_pct(m['mdd']):>7} "
                f"{fmt_f(m['sharpe'],2):>6}{flag}{outlier}"
            )

        # Monthly PnL
        lines.append("")
        lines.append("  MONTHLY PnL (aggregate):")
        monthly_agg = collections.defaultdict(float)
        for sym, res in all_coin_results.items():
            if res is None: continue
            cr = res.get(ck)
            if not cr: continue
            for t in cr.get("trades", []):
                mo = datetime.datetime.utcfromtimestamp(t["entry_t"]/1000).strftime("%Y-%m")
                monthly_agg[mo] += t["pnl"]
        for mo in sorted(monthly_agg):
            lines.append(f"    {mo}: ${monthly_agg[mo]:.3f}")

    # ── CROSS-CONFIG ANALYSIS ──
    lines.append("")
    lines.append("═"*80)
    lines.append("CROSS-CONFIG ANALYSIS")
    lines.append("═"*80)

    # Direction bias
    lines.append("")
    lines.append("DIRECTION BIAS (Short-Only vs Equal vs Short-Heavy):")
    for strat in ["S1","S2"]:
        lines.append(f"  {strat}:")
        for variant, label in [("A","Short-Only"),("B","Equal"),("C","Short-Heavy")]:
            ck = f"{strat}-{variant}"
            agg = config_aggregates[ck]["agg"]
            if agg:
                lines.append(f"    {label:<15}: PF={agg['pf']:.4f} | WR={fmt_pct(agg['wr'])} | Net=${agg['net']:.2f}")

    # S1 vs S2
    lines.append("")
    lines.append("STRATEGY COMPARISON (S1 Liquidity Sweep vs S2 RSI Divergence):")
    for variant, label in [("A","Short-Only"),("B","Equal"),("C","Short-Heavy")]:
        s1 = config_aggregates[f"S1-{variant}"]["agg"]
        s2 = config_aggregates[f"S2-{variant}"]["agg"]
        if s1 and s2:
            winner = "S1" if s1["pf"] > s2["pf"] else "S2"
            lines.append(f"  {label}: S1 PF={s1['pf']:.4f} vs S2 PF={s2['pf']:.4f} → {winner} wins")

    # Top 30 best config
    if best_cfg:
        lines.append("")
        lines.append(f"TOP 30 COINS — BEST CONFIG ({best_cfg}):")
        best_coins = sorted(config_aggregates[best_cfg]["coins"], key=lambda x: x["pf"], reverse=True)[:30]
        lines.append(f"{'#':<3} {'Symbol':<16} {'Trades':>6} {'WR':>7} {'PF':>7} {'Net$':>8}")
        for idx, m in enumerate(best_coins, 1):
            lines.append(f"{idx:<3} {m['symbol']:<16} {m['n']:>6} {fmt_pct(m['wr']):>7} {fmt_f(m['pf'],3):>7} {m['net']:>8.3f}")

        lines.append("")
        lines.append(f"BOTTOM 20 COINS — BEST CONFIG ({best_cfg}):")
        worst_coins = sorted(config_aggregates[best_cfg]["coins"], key=lambda x: x["pf"])[:20]
        lines.append(f"{'#':<3} {'Symbol':<16} {'Trades':>6} {'WR':>7} {'PF':>7} {'Net$':>8}")
        for idx, m in enumerate(worst_coins, 1):
            lines.append(f"{idx:<3} {m['symbol']:<16} {m['n']:>6} {fmt_pct(m['wr']):>7} {fmt_f(m['pf'],3):>7} {m['net']:>8.3f}")

    # Skipped coins
    lines.append("")
    lines.append(f"SKIPPED COINS ({len(skipped)}):")
    lines.append("  " + ", ".join(sorted(skipped)))

    lines.append("")
    lines.append("═"*80)
    lines.append("END OF REPORT")
    lines.append("═"*80)

    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log(f"\n[DONE] Summary written to {outfile}")

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    log("═"*60)
    log("BACKTEST V6 — Starting")
    log(f"Period : {START_DATE} → {END_DATE}")
    log(f"Interval: {INTERVAL}")
    log(f"Workers : 10 parallel")
    log("═"*60)

    # Fetch all symbols
    log("\n[1/3] Fetching symbol list from Binance...")
    all_syms = get_all_symbols()
    log(f"  Found {len(all_syms)} USDT symbols")

    # Known fixes
    fixed_syms = []
    skip_hard = {"1000FLOKIUSDT", "1000BONKUSDT", "1000PEPEUSDT", "1000SHIBUSDT"}
    for s in all_syms:
        if s in skip_hard:
            continue
        if s == "MATICUSDT":
            fixed_syms.append("POLUSDT")
            continue
        fixed_syms.append(s)
    fixed_syms = sorted(set(fixed_syms))
    log(f"  After fixes: {len(fixed_syms)} symbols to test")

    # Run all coins in parallel
    log(f"\n[2/3] Running backtest on {len(fixed_syms)} coins (10 workers)...")
    all_results = {}
    skipped     = []
    done = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_coin, sym): sym for sym in fixed_syms}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                symbol, res, status = future.result()
                if status == "SKIPPED" or res is None:
                    skipped.append(symbol)
                else:
                    all_results[symbol] = res
            except Exception as e:
                log(f"  [ERR] {sym}: {e}")
                skipped.append(sym)
            done += 1
            if done % 50 == 0:
                log(f"  Progress: {done}/{len(fixed_syms)} coins done")

    log(f"\n  Complete: {len(all_results)} coins processed, {len(skipped)} skipped")

    # Aggregate per config
    log("\n[3/3] Aggregating results across 6 configs...")
    config_keys = ["S1-A","S1-B","S1-C","S2-A","S2-B","S2-C"]
    config_aggregates = {}
    for ck in config_keys:
        agg, coins, filters, disabled = aggregate_config(all_results, ck)
        config_aggregates[ck] = {
            "agg": agg, "coins": coins, "filters": filters, "disabled": disabled
        }
        if agg:
            log(f"  {ck}: {agg['n']} trades | PF {agg['pf']:.4f} | WR {agg['wr']*100:.1f}%")

    # Write outputs
    log("\nWriting outputs...")

    write_summary(all_results, skipped, config_aggregates, "backtest_summary.txt")

    # JSON report
    json_out = {}
    for ck in config_keys:
        agg = config_aggregates[ck]["agg"]
        coins = config_aggregates[ck]["coins"]
        t1, t2, t3 = tier_coins(coins)
        json_out[ck] = {
            "aggregate": agg,
            "tier1": t1,
            "tier2": t2,
            "tier3_count": len(t3),
            "disabled_coins": config_aggregates[ck]["disabled"],
            "filters": config_aggregates[ck]["filters"],
            "per_coin": coins,
        }

    with open("backtest_report.json", "w", encoding="utf-8") as f:
        json.dump(json_out, f, indent=2, default=str)
    log("backtest_report.json written.")
    log("\n✅ V6 Backtest complete.")

if __name__ == "__main__":
    main()
