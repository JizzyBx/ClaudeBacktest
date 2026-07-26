"""
================================================================================
BACKTEST.PY - v6.1 ADX-DI Confluence Scalper (15m), 30-coin universe
================================================================================
Pure Python 3.11 stdlib only. No numpy/pandas/requests.
Data source: https://data-api.binance.vision/api/v3/klines (public, spot proxy)

All 30 core coins share ONE portfolio balance (as in the live bot - no hard cap
on concurrent positions). Symbols are scanned in a fixed order at every 15m
tick, exactly mirroring the pseudocode's sequential for-loop, so position
sizing on each new entry reflects whatever the running balance is at that
moment (including PnL from other symbols closed earlier that same tick).

Outputs: backtest_report.json (full data), backtest_summary.txt (human readable)
================================================================================
"""

import json
import math
import statistics
import urllib.request
import urllib.error
import time
import datetime
from collections import defaultdict

# ================================================================================
# 1. ASSET UNIVERSE  (spec name -> actual spot symbol to fetch)
# ================================================================================
# 1000-prefixed futures notation has no spot equivalent; spot lists the base
# token directly. Established project rule: strip the 1000 prefix for spot data.
SYMBOL_MAP = {
    "1000BONKUSDT": "BONKUSDT",
    "1000PEPEUSDT": "PEPEUSDT",
    "1000SHIBUSDT": "SHIBUSDT",
    "1000FLOKIUSDT": "FLOKIUSDT",
}

CORE_COINS_SPEC = [
    "ETHUSDT", "DOGEUSDT", "DOTUSDT", "ARBUSDT", "1000BONKUSDT", "1000PEPEUSDT",
    "1000SHIBUSDT", "ADAUSDT", "APTUSDT", "LINKUSDT", "SOLUSDT", "SUIUSDT",
    "1000FLOKIUSDT", "WIFUSDT", "BTCUSDT", "BNBUSDT", "NEARUSDT", "XRPUSDT",
    "AVAXUSDT", "LTCUSDT", "ATOMUSDT", "OPUSDT", "INJUSDT", "UNIUSDT", "AAVEUSDT",
    "HBARUSDT", "TRUMPUSDT", "BOMEUSDT", "WLDUSDT", "NEIROUSDT",
]

# ordered list of (spec_name, fetch_symbol) - scan order matches spec order
SYMBOLS = [(s, SYMBOL_MAP.get(s, s)) for s in CORE_COINS_SPEC]

# ================================================================================
# 2. CONFIG
# ================================================================================
FEE_TAKER = 0.0005      # 0.05% - ALL orders are MARKET type per spec (entry/TP/SL)
SLIPPAGE = 0.0002       # 0.02% per side, per spec notes section 12
INITIAL_CAP = 10000.0   # project-standard default (keeps results comparable to prior rounds)
COOLDOWN_MS = 5 * 60 * 1000   # 5 minutes, per-symbol

INTERVAL = "15m"
INTERVAL_MS = 15 * 60 * 1000
BASE_URL = "https://data-api.binance.vision/api/v3/klines"
MAX_RETRIES = 5

YEARS_BACK = 2
WARMUP_BARS = 400  # extra candles before official window for EMA50/ADX/RSI convergence

ADX_MIN = 22
SLOPE_LOOKBACK = 20
SLOPE_THRESHOLD = 0.15
RSI_LONG_MIN = 52
RSI_SHORT_MAX = 48
ATR_PCT_MIN = 0.08

def get_atr_multipliers(adx_val):
    if adx_val > 30:
        return 4.0, 2.0
    elif adx_val > 25:
        return 3.0, 1.8
    else:  # 22 <= adx <= 25 (also covers the 25<adx<=30... handled above)
        return 2.5, 1.6

def get_leverage_and_pct(balance):
    if balance < 10:
        return 10, 0.30
    elif balance <= 50:
        return 10, 0.10
    else:
        return 5, 0.10

# ================================================================================
# 3. FETCH + PARSE
# ================================================================================
def fetch(symbol, start_ms, end_ms):
    all_klines = []
    cursor = start_ms
    while cursor < end_ms:
        url = f"{BASE_URL}?symbol={symbol}&interval={INTERVAL}&startTime={cursor}&endTime={end_ms}&limit=1000"
        data = None
        for attempt in range(MAX_RETRIES):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    raw = resp.read()
                data = json.loads(raw)
                break
            except urllib.error.HTTPError as e:
                if e.code in (400, 451):
                    print(f"  [SKIP] {symbol}: HTTP {e.code} - {e.reason}")
                    return all_klines
                wait = 0.5 * (2 ** attempt)
                print(f"  [RETRY {attempt+1}/{MAX_RETRIES}] {symbol} HTTP {e.code}, waiting {wait:.1f}s")
                time.sleep(wait)
            except Exception as e:
                wait = 0.5 * (2 ** attempt)
                print(f"  [RETRY {attempt+1}/{MAX_RETRIES}] {symbol} error: {e}, waiting {wait:.1f}s")
                time.sleep(wait)
        if data is None:
            print(f"  [FAIL] {symbol}: giving up after {MAX_RETRIES} retries")
            break
        if not data:
            break
        all_klines.extend(data)
        last_open_time = data[-1][0]
        if len(data) < 1000:
            break
        cursor = last_open_time + 1
        time.sleep(0.13)
    return all_klines


def parse(raw):
    opens, highs, lows, closes, volumes, times = [], [], [], [], [], []
    for k in raw:
        times.append(int(k[0]))
        opens.append(float(k[1]))
        highs.append(float(k[2]))
        lows.append(float(k[3]))
        closes.append(float(k[4]))
        volumes.append(float(k[5]))
    return opens, highs, lows, closes, volumes, times


# ================================================================================
# 4. INDICATORS
# ================================================================================
def ema(values, period):
    out = [None] * len(values)
    if len(values) < period:
        return out
    k = 2.0 / (period + 1)
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    prev = seed
    for i in range(period, len(values)):
        cur = values[i] * k + prev * (1 - k)
        out[i] = cur
        prev = cur
    return out


def sma(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        out[i] = sum(values[i - period + 1:i + 1]) / period
    return out


def rsi(closes, period=14):
    out = [None] * len(closes)
    if len(closes) < period + 1:
        return out
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out[period] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out_idx = i + 1
        out[out_idx] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    return out


def atr(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    if n < period + 1:
        return out
    trs = [None] * n
    for i in range(1, n):
        trs[i] = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
    seed = sum(trs[1:period + 1]) / period
    out[period] = seed
    prev = seed
    for i in range(period + 1, n):
        prev = (prev * (period - 1) + trs[i]) / period
        out[i] = prev
    return out


def first_valid_index(lst):
    for i, v in enumerate(lst):
        if v is not None:
            return i
    return None


def wilder_sum_smooth(raw, period):
    """Wilder's running-SUM smoothing (used for TR, +DM, -DM)."""
    n = len(raw)
    out = [None] * n
    fi = first_valid_index(raw)
    if fi is None or n - fi < period:
        return out
    seed = sum(raw[fi:fi + period])
    out[fi + period - 1] = seed
    prev = seed
    for i in range(fi + period, n):
        prev = prev - prev / period + raw[i]
        out[i] = prev
    return out


def wilder_avg_smooth(raw, period):
    """Wilder's running-AVERAGE smoothing (used for ADX itself)."""
    n = len(raw)
    out = [None] * n
    fi = first_valid_index(raw)
    if fi is None or n - fi < period:
        return out
    seed = sum(raw[fi:fi + period]) / period
    out[fi + period - 1] = seed
    prev = seed
    for i in range(fi + period, n):
        prev = (prev * (period - 1) + raw[i]) / period
        out[i] = prev
    return out


def adx_dmi(highs, lows, closes, period=14):
    n = len(closes)
    plus_dm = [None] * n
    minus_dm = [None] * n
    tr = [None] * n
    for i in range(1, n):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))

    sm_tr = wilder_sum_smooth(tr, period)
    sm_pdm = wilder_sum_smooth(plus_dm, period)
    sm_mdm = wilder_sum_smooth(minus_dm, period)

    pdi = [None] * n
    mdi = [None] * n
    dx = [None] * n
    for i in range(n):
        if sm_tr[i] is not None and sm_tr[i] > 0:
            pdi[i] = 100.0 * sm_pdm[i] / sm_tr[i]
            mdi[i] = 100.0 * sm_mdm[i] / sm_tr[i]
            s = pdi[i] + mdi[i]
            dx[i] = 100.0 * abs(pdi[i] - mdi[i]) / s if s > 0 else 0.0

    adx = wilder_avg_smooth(dx, period)
    return adx, pdi, mdi


# ================================================================================
# 5. TIME-GRID ALIGNMENT (for the shared multi-symbol simulation)
# ================================================================================
def align_to_grid(times, series_dict, grid_index):
    """Map each series (keyed by name -> list aligned to native `times`) onto
    the global grid_index (dict: timestamp -> global slot). Returns dict of
    name -> list[None]*grid_len with values placed at matching slots."""
    grid_len = len(grid_index)
    out = {name: [None] * grid_len for name in series_dict}
    for local_i, t in enumerate(times):
        slot = grid_index.get(t)
        if slot is None:
            continue
        for name, series in series_dict.items():
            out[name][slot] = series[local_i]
    return out


# ================================================================================
# 6. MAIN SIMULATION
# ================================================================================
def main():
    now = datetime.datetime.utcnow()
    end_dt = now
    start_dt = end_dt - datetime.timedelta(days=365 * YEARS_BACK)
    official_start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    fetch_start_ms = official_start_ms - WARMUP_BARS * INTERVAL_MS

    print("=" * 80)
    print("BACKTEST START - ADX-DI Confluence Scalper v6.1 (15m, 30-coin shared portfolio)")
    print(f"Window: {start_dt.isoformat()} -> {end_dt.isoformat()} UTC")
    print(f"Symbols ({len(SYMBOLS)}): {[s[1] for s in SYMBOLS]}")
    print("=" * 80)

    # ---- 6.1 fetch + compute indicators per symbol ----
    per_symbol = {}  # spec_name -> dict of aligned arrays + raw meta
    global_time_set = set()

    for spec_name, fetch_sym in SYMBOLS:
        print(f"\nFetching {fetch_sym} ({spec_name}) ...")
        raw = fetch(fetch_sym, fetch_start_ms, end_ms)
        print(f"  -> {len(raw)} candles")
        if len(raw) < 250:
            print(f"  [SKIP] {spec_name}: insufficient data")
            per_symbol[spec_name] = None
            continue
        opens, highs, lows, closes, volumes, times = parse(raw)

        ema9 = ema(closes, 9)
        ema21 = ema(closes, 21)
        ema50 = ema(closes, 50)
        rsi14 = rsi(closes, 14)
        atr14 = atr(highs, lows, closes, 14)
        vol_sma20 = sma(volumes, 20)
        adx14, pdi14, mdi14 = adx_dmi(highs, lows, closes, 14)

        per_symbol[spec_name] = {
            "fetch_sym": fetch_sym,
            "times": times,
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
            "ema9": ema9,
            "ema21": ema21,
            "ema50": ema50,
            "rsi": rsi14,
            "atr": atr14,
            "vol_sma": vol_sma20,
            "adx": adx14,
            "pdi": pdi14,
            "mdi": mdi14,
        }
        global_time_set.update(t for t in times if t >= official_start_ms)

    valid_symbols = [s for s in CORE_COINS_SPEC if per_symbol.get(s) is not None]
    if not valid_symbols:
        print("No symbols had usable data. Aborting.")
        return

    global_times = sorted(global_time_set)
    grid_index = {t: i for i, t in enumerate(global_times)}
    grid_len = len(global_times)
    print(f"\nGlobal 15m grid: {grid_len} ticks from official window across {len(valid_symbols)} usable symbols")

    # ---- 6.2 align each symbol's arrays onto the global grid ----
    for spec_name in valid_symbols:
        d = per_symbol[spec_name]
        aligned = align_to_grid(d["times"], {
            "close": d["closes"], "high": d["highs"], "low": d["lows"], "volume": d["volumes"],
            "ema9": d["ema9"], "ema21": d["ema21"], "ema50": d["ema50"], "rsi": d["rsi"],
            "atr": d["atr"], "vol_sma": d["vol_sma"], "adx": d["adx"], "pdi": d["pdi"], "mdi": d["mdi"],
        }, grid_index)
        d["grid"] = aligned

    # ---- 6.3 event-driven simulation ----
    balance = INITIAL_CAP
    positions = {s: None for s in valid_symbols}
    cooldown_until = {s: -1 for s in valid_symbols}
    trades = []
    filter_counts = defaultdict(int)
    scans = defaultdict(int)

    for slot in range(grid_len):
        t = global_times[slot]

        # ---- Step A: manage exits first (all symbols) ----
        for spec_name in valid_symbols:
            pos = positions[spec_name]
            if pos is None:
                continue
            g = per_symbol[spec_name]["grid"]
            h, l = g["high"][slot], g["low"][slot]
            if h is None or l is None:
                continue

            exit_price = None
            hit_tp = False
            if pos["dir"] == "long":
                if l <= pos["sl"]:
                    exit_price = pos["sl"] * (1 - SLIPPAGE)
                elif h >= pos["tp"]:
                    exit_price = pos["tp"] * (1 - SLIPPAGE)
                    hit_tp = True
            else:
                if h >= pos["sl"]:
                    exit_price = pos["sl"] * (1 + SLIPPAGE)
                elif l <= pos["tp"]:
                    exit_price = pos["tp"] * (1 + SLIPPAGE)
                    hit_tp = True

            if exit_price is not None:
                size = pos["size"]
                if pos["dir"] == "long":
                    raw_pnl = size * (exit_price - pos["entry"])
                else:
                    raw_pnl = size * (pos["entry"] - exit_price)
                exit_fee = size * exit_price * FEE_TAKER
                net_pnl = raw_pnl - exit_fee - pos["entry_fee"]
                balance += net_pnl
                trades.append({
                    "symbol": spec_name, "dir": pos["dir"], "entry": pos["entry"],
                    "exit": exit_price, "entry_t": pos["entry_t"], "exit_t": t,
                    "pnl": net_pnl, "win": net_pnl > 0, "hit_tp": hit_tp,
                    "dur": (t - pos["entry_t"]) / 60000.0,
                })
                positions[spec_name] = None
                cooldown_until[spec_name] = t + COOLDOWN_MS

        # ---- Step B: look for new entries (fixed scan order) ----
        for spec_name in valid_symbols:
            if positions[spec_name] is not None:
                continue
            if t < cooldown_until[spec_name]:
                continue
            g = per_symbol[spec_name]["grid"]
            close = g["close"][slot]
            if close is None:
                continue

            adx_v = g["adx"][slot]
            pdi_v = g["pdi"][slot]
            mdi_v = g["mdi"][slot]
            rsi_v = g["rsi"][slot]
            atr_v = g["atr"][slot]
            vol_v = g["volume"][slot]
            vsma_v = g["vol_sma"][slot]
            ema9_v = g["ema9"][slot]
            ema21_v = g["ema21"][slot]
            ema50_v = g["ema50"][slot]

            if slot < SLOPE_LOOKBACK:
                continue
            ema9_p = g["ema9"][slot - 1]
            ema21_p = g["ema21"][slot - 1]
            ema50_lb = g["ema50"][slot - SLOPE_LOOKBACK]

            required = [adx_v, pdi_v, mdi_v, rsi_v, atr_v, vol_v, vsma_v, ema9_v, ema21_v,
                        ema50_v, ema9_p, ema21_p, ema50_lb]
            if any(v is None for v in required):
                continue

            scans[spec_name] += 1

            if adx_v < ADX_MIN:
                filter_counts["ADX_LOW"] += 1
                continue

            atr_pct = atr_v / close * 100.0
            if atr_pct < ATR_PCT_MIN:
                filter_counts["LOW_ATR"] += 1
                continue

            if vol_v <= vsma_v:
                filter_counts["LOW_VOL"] += 1
                continue

            slope_pct = (ema50_v - ema50_lb) / ema50_lb * 100.0 if ema50_lb != 0 else 0.0
            bullish_cross = ema9_p <= ema21_p and ema9_v > ema21_v
            bearish_cross = ema9_p >= ema21_p and ema9_v < ema21_v

            direction = None
            if slope_pct > SLOPE_THRESHOLD:
                if not bullish_cross:
                    filter_counts["WAIT_UP"] += 1
                    continue
                if not (pdi_v > mdi_v):
                    filter_counts["DI_MISALIGN_LONG"] += 1
                    continue
                if not (rsi_v > RSI_LONG_MIN):
                    filter_counts["RSI_LOW"] += 1
                    continue
                direction = "long"
            elif slope_pct < -SLOPE_THRESHOLD:
                if not bearish_cross:
                    filter_counts["WAIT_DOWN"] += 1
                    continue
                if not (mdi_v > pdi_v):
                    filter_counts["DI_MISALIGN_SHORT"] += 1
                    continue
                if not (rsi_v < RSI_SHORT_MAX):
                    filter_counts["RSI_HIGH"] += 1
                    continue
                direction = "short"
            else:
                filter_counts["NO_TREND"] += 1
                continue

            filter_counts["SIGNALS_GENERATED"] += 1

            if balance < 0.50:
                filter_counts["BALANCE_FLOOR_SKIP"] += 1
                continue
            leverage, pct = get_leverage_and_pct(balance)

            entry_raw = close
            entry_price = entry_raw * (1 + SLIPPAGE) if direction == "long" else entry_raw * (1 - SLIPPAGE)
            raw_qty = (balance * pct * leverage) / entry_price
            if raw_qty <= 0:
                filter_counts["ZERO_QTY_SKIP"] += 1
                continue

            tp_mult, sl_mult = get_atr_multipliers(adx_v)
            if direction == "long":
                sl = entry_price - atr_v * sl_mult
                tp = entry_price + atr_v * tp_mult
            else:
                sl = entry_price + atr_v * sl_mult
                tp = entry_price - atr_v * tp_mult

            entry_fee = raw_qty * entry_price * FEE_TAKER

            positions[spec_name] = {
                "dir": direction, "entry": entry_price, "sl": sl, "tp": tp,
                "entry_t": t, "size": raw_qty, "entry_fee": entry_fee,
                "leverage": leverage, "pct": pct,
            }

    # ---- 6.4 metrics ----
    def compute_metrics(trade_list, label):
        n = len(trade_list)
        if n == 0:
            return {"label": label, "n": 0, "wr": 0.0, "pf": 0.0, "net": 0.0, "net_pct": 0.0,
                    "mdd": 0.0, "sharpe": 0.0, "aw": 0.0, "al": 0.0, "exp": 0.0, "dur": 0.0,
                    "nlongs": 0, "nshorts": 0, "lwr": 0.0, "swr": 0.0, "monthly": {},
                    "maxcw": 0, "maxcl": 0, "gp": 0.0, "gl": 0.0, "trades_per_day": 0.0}
        wins = [t for t in trade_list if t["win"]]
        losses = [t for t in trade_list if not t["win"]]
        gp = sum(t["pnl"] for t in wins)
        gl = abs(sum(t["pnl"] for t in losses))
        net = sum(t["pnl"] for t in trade_list)
        pf = (gp / gl) if gl > 0 else (999.0 if gp > 0 else 0.0)
        wr = 100.0 * len(wins) / n

        eq = INITIAL_CAP
        peak = eq
        mdd = 0.0
        for tr in sorted(trade_list, key=lambda x: x["exit_t"]):
            eq += tr["pnl"]
            peak = max(peak, eq)
            dd = (peak - eq) / peak * 100.0 if peak > 0 else 0.0
            mdd = max(mdd, dd)

        aw = (gp / len(wins)) if wins else 0.0
        al = (gl / len(losses)) if losses else 0.0
        exp = net / n
        rets = [t["pnl"] / INITIAL_CAP for t in trade_list]
        sharpe = (statistics.mean(rets) / statistics.pstdev(rets) * math.sqrt(n)) if len(rets) > 1 and statistics.pstdev(rets) > 0 else 0.0
        durs = [t["dur"] for t in trade_list]
        avg_dur = statistics.mean(durs) if durs else 0.0
        longs = [t for t in trade_list if t["dir"] == "long"]
        shorts = [t for t in trade_list if t["dir"] == "short"]
        lwr = 100.0 * sum(1 for t in longs if t["win"]) / len(longs) if longs else 0.0
        swr = 100.0 * sum(1 for t in shorts if t["win"]) / len(shorts) if shorts else 0.0

        monthly = defaultdict(float)
        for tr in trade_list:
            dt = datetime.datetime.utcfromtimestamp(tr["exit_t"] / 1000.0)
            monthly[f"{dt.year}-{dt.month:02d}"] += tr["pnl"]

        maxcw = cw = 0
        maxcl = cl = 0
        for tr in sorted(trade_list, key=lambda x: x["exit_t"]):
            if tr["win"]:
                cw += 1; cl = 0
            else:
                cl += 1; cw = 0
            maxcw = max(maxcw, cw)
            maxcl = max(maxcl, cl)

        span_days = max(1.0, (end_ms - official_start_ms) / 86400000.0)
        return {
            "label": label, "n": n, "wr": round(wr, 2),
            "pf": round(pf, 4) if pf != 999.0 else 999.0,
            "net": round(net, 2), "net_pct": round(100.0 * net / INITIAL_CAP, 2),
            "mdd": round(mdd, 2), "sharpe": round(sharpe, 3),
            "aw": round(aw, 2), "al": round(al, 2), "exp": round(exp, 2),
            "dur": round(avg_dur, 1), "nlongs": len(longs), "nshorts": len(shorts),
            "lwr": round(lwr, 2), "swr": round(swr, 2), "monthly": dict(sorted(monthly.items())),
            "maxcw": maxcw, "maxcl": maxcl, "gp": round(gp, 2), "gl": round(gl, 2),
            "trades_per_day": round(n / span_days, 2),
        }

    per_coin_metrics = []
    for spec_name in valid_symbols:
        coin_trades = [t for t in trades if t["symbol"] == spec_name]
        per_coin_metrics.append(compute_metrics(coin_trades, spec_name))
    per_coin_metrics.sort(key=lambda x: x["pf"], reverse=True)
    aggregate = compute_metrics(trades, "AGGREGATE")

    pf_pass = sum(1 for c in per_coin_metrics if c["pf"] >= 1.60)
    wr_pass = sum(1 for c in per_coin_metrics if c["wr"] >= 50)

    # ---- 6.5 write report ----
    report = {
        "generated": now.isoformat(),
        "config": {
            "initial_cap": INITIAL_CAP, "fee_taker": FEE_TAKER, "slippage": SLIPPAGE,
            "years_back": YEARS_BACK, "window_start": start_dt.isoformat(), "window_end": end_dt.isoformat(),
            "symbols_used": valid_symbols, "symbols_skipped": [s for s in CORE_COINS_SPEC if s not in valid_symbols],
        },
        "aggregate": aggregate,
        "per_coin": per_coin_metrics,
        "filters": dict(filter_counts),
        "scans_per_symbol": dict(scans),
        "final_balance": round(balance, 2),
        "validation": {"pf_target": 1.60, "wr_target": 50, "pf_pass_count": pf_pass, "wr_pass_count": wr_pass,
                       "total_coins": len(per_coin_metrics), "baseline_v60_pf": 1.01, "baseline_v60_wr": 42},
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    lines = []
    lines.append("=" * 80)
    lines.append("BACKTEST SUMMARY - ADX-DI Confluence Scalper v6.1 (shared portfolio, 30 coins)")
    lines.append(f"Generated: {now.isoformat()} UTC")
    lines.append(f"Window: {start_dt.date()} -> {end_dt.date()} ({YEARS_BACK} years)")
    lines.append(f"Initial Capital: ${INITIAL_CAP:,.2f} (single shared balance across all symbols)")
    lines.append(f"Fees: Taker {FEE_TAKER*100:.2f}% (all orders are MARKET) | Slippage: {SLIPPAGE*100:.2f}%")
    lines.append(f"Leverage/position tiers: <$10=10x/30% | $10-50=10x/10% | >$50=5x/10%")
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Symbols used ({len(valid_symbols)}/30): {', '.join(valid_symbols)}")
    skipped = [s for s in CORE_COINS_SPEC if s not in valid_symbols]
    if skipped:
        lines.append(f"Symbols skipped (insufficient data): {', '.join(skipped)}")
    lines.append("")
    lines.append(f"FINAL BALANCE: ${balance:,.2f} (started at ${INITIAL_CAP:,.2f})")
    lines.append("")
    lines.append(f"AGGREGATE: trades={aggregate['n']} | WR={aggregate['wr']}% | PF={aggregate['pf']} | "
                  f"Net={aggregate['net_pct']}% (${aggregate['net']:,.2f}) | MaxDD={aggregate['mdd']}% | "
                  f"Trades/day={aggregate['trades_per_day']}")
    lines.append(f"VALIDATION: PF>=1.60 on {pf_pass}/{len(per_coin_metrics)} coins | WR>=50% on {wr_pass}/{len(per_coin_metrics)} coins")
    lines.append(f"COMPARISON BASELINE (v6.0 live): WR~42% | PF~1.01")
    lines.append("")
    lines.append("FILTER / REJECTION STATS (aggregate across all symbols):")
    for k in ["ADX_LOW", "LOW_ATR", "LOW_VOL", "NO_TREND", "WAIT_UP", "WAIT_DOWN",
              "DI_MISALIGN_LONG", "DI_MISALIGN_SHORT", "RSI_LOW", "RSI_HIGH",
              "BALANCE_FLOOR_SKIP", "ZERO_QTY_SKIP", "SIGNALS_GENERATED"]:
        lines.append(f"  {k}: {filter_counts.get(k, 0)}")
    lines.append("")
    lines.append("PER-COIN TABLE (sorted by PF descending):")
    lines.append(f"  {'SYMBOL':<14}{'TRADES':>8}{'WR%':>8}{'PF':>8}{'NET%':>10}{'MDD%':>8}{'T/DAY':>8}{'AVGWIN':>10}{'AVGLOSS':>10}")
    for c in per_coin_metrics:
        lines.append(f"  {c['label']:<14}{c['n']:>8}{c['wr']:>8}{c['pf']:>8}{c['net_pct']:>10}{c['mdd']:>8}{c['trades_per_day']:>8}{c['aw']:>10}{c['al']:>10}")
    lines.append("")
    lines.append("MONTHLY PNL (aggregate):")
    for month, pnl in aggregate["monthly"].items():
        lines.append(f"  {month}: ${pnl:,.2f}")
    lines.append("")
    lines.append("=" * 80)
    lines.append("DESIRED OUTPUTS RECAP")
    lines.append("=" * 80)
    lines.append(f"AGGREGATE: NetPnL={aggregate['net_pct']}% | WR={aggregate['wr']}% | PF={aggregate['pf']} | "
                  f"AvgWin=${aggregate['aw']} | AvgLoss=${aggregate['al']} | Expectancy=${aggregate['exp']} | "
                  f"MaxDD={aggregate['mdd']}% | Sharpe={aggregate['sharpe']} | Trades={aggregate['n']} | "
                  f"Trades/day={aggregate['trades_per_day']}")
    lines.append("=" * 80)
    lines.append("END OF SUMMARY")
    lines.append("=" * 80)

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n\nDONE. Wrote backtest_report.json and backtest_summary.txt")


if __name__ == "__main__":
    main()

