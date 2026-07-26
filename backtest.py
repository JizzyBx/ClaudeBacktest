"""
================================================================================
BACKTEST.PY - Live Bot Strategy (v6 base entry), 3 TP/SL ATR-multiplier variants
================================================================================
Pure Python 3.11 stdlib only. No numpy/pandas/requests.
Data source: https://data-api.binance.vision/api/v3/klines (public, spot proxy)

Entry logic replicated EXACTLY from trading_bot.py (get_signal()):
  - ADX(14) >= 22
  - 50EMA slope over 10 bars (2.5h) > +0.05% (long) or < -0.05% (short)
  - EMA9/21 crossover on the current candle
  - NO RSI / DI / Volume filters (matches the live bot - none exist there)
  - Entry executes at the SAME candle's close (matches the live bot: it places
    the market order immediately on signal, using closes[-1] as the reference
    price for TP/SL calc) - no next-candle-open deferral this time.

Three independent strategies (same entry, different TP/SL ATR multipliers),
each run as its own full 30-coin shared-portfolio simulation:
  S1: TP = 2.0x ATR | SL = 4.0x ATR
  S2: TP = 1.5x ATR | SL = 2.5x ATR
  S3: TP = 4.0x ATR | SL = 3.0x ATR

Fees/slippage are NOT modeled in the live bot's own code, but real fills always
incur them - this backtest adds a standard 0.05% taker fee/side + 0.02%
slippage for realism, flagged clearly in the report.

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
# 1. ASSET UNIVERSE (spec name -> actual spot symbol to fetch)
# ================================================================================
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

SYMBOLS = [(s, SYMBOL_MAP.get(s, s)) for s in CORE_COINS_SPEC]

# ================================================================================
# 2. CONFIG
# ================================================================================
FEE_TAKER = 0.0005      # 0.05% per side - realistic addition, not modeled in live bot
SLIPPAGE = 0.0002       # 0.02% - realistic addition, not modeled in live bot
INITIAL_CAP = 10000.0
COOLDOWN_MS = 5 * 60 * 1000   # matches live bot's default cooldown_min=5

INTERVAL = "15m"
INTERVAL_MS = 15 * 60 * 1000
BASE_URL = "https://data-api.binance.vision/api/v3/klines"
MAX_RETRIES = 5

YEARS_BACK = 2
WARMUP_BARS = 400

ADX_MIN = 22
SLOPE_LOOKBACK = 10     # matches live bot: e50[-1] vs e50[-10]
SLOPE_THRESHOLD = 0.05  # matches live bot: slope_pct > 0.05 / < -0.05

STRATEGIES = {
    "S1_TP2x_SL4x": {"tp_mult": 2.0, "sl_mult": 4.0},
    "S2_TP1.5x_SL2.5x": {"tp_mult": 1.5, "sl_mult": 2.5},
    "S3_TP4x_SL3x": {"tp_mult": 4.0, "sl_mult": 3.0},
}

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
# 4. INDICATORS (mirrors trading_bot.py's ema/atr_calc/adx_calc exactly in spirit)
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

    dx = [None] * n
    for i in range(n):
        if sm_tr[i] is not None and sm_tr[i] > 0:
            pdi = 100.0 * sm_pdm[i] / sm_tr[i]
            mdi = 100.0 * sm_mdm[i] / sm_tr[i]
            s = pdi + mdi
            dx[i] = 100.0 * abs(pdi - mdi) / s if s > 0 else 0.0

    adx = wilder_avg_smooth(dx, period)
    return adx


# ================================================================================
# 5. TIME-GRID ALIGNMENT
# ================================================================================
def align_to_grid(times, series_dict, grid_index):
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
# 6. ONE STRATEGY SIMULATION (shared 30-coin portfolio)
# ================================================================================
def run_strategy_sim(per_symbol, valid_symbols, global_times, official_start_ms, end_ms, tp_mult, sl_mult):
    grid_len = len(global_times)
    balance = INITIAL_CAP
    positions = {s: None for s in valid_symbols}
    cooldown_until = {s: -1 for s in valid_symbols}
    trades = []
    filter_counts = defaultdict(int)

    for slot in range(grid_len):
        t = global_times[slot]

        # ---- Step A: exits ----
        for spec_name in valid_symbols:
            pos = positions[spec_name]
            if pos is None:
                continue
            g = per_symbol[spec_name]["grid"]
            h, l = g["high"][slot], g["low"][slot]
            if h is None or l is None:
                continue

            exit_price = None
            if pos["dir"] == "long":
                if l <= pos["sl"]:
                    exit_price = pos["sl"] * (1 - SLIPPAGE)
                elif h >= pos["tp"]:
                    exit_price = pos["tp"] * (1 - SLIPPAGE)
            else:
                if h >= pos["sl"]:
                    exit_price = pos["sl"] * (1 + SLIPPAGE)
                elif l <= pos["tp"]:
                    exit_price = pos["tp"] * (1 + SLIPPAGE)

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
                    "pnl": net_pnl, "win": net_pnl > 0,
                    "dur": (t - pos["entry_t"]) / 60000.0,
                })
                positions[spec_name] = None
                cooldown_until[spec_name] = t + COOLDOWN_MS

        # ---- Step B: entries (same-candle execution, matches live bot) ----
        for spec_name in valid_symbols:
            if positions[spec_name] is not None:
                continue
            if t < cooldown_until[spec_name]:
                continue
            g = per_symbol[spec_name]["grid"]
            close = g["close"][slot]
            if close is None:
                continue
            if slot < SLOPE_LOOKBACK:
                continue

            adx_v = g["adx"][slot]
            atr_v = g["atr"][slot]
            ema9_v = g["ema9"][slot]
            ema21_v = g["ema21"][slot]
            ema50_v = g["ema50"][slot]
            ema9_p = g["ema9"][slot - 1]
            ema21_p = g["ema21"][slot - 1]
            ema50_lb = g["ema50"][slot - SLOPE_LOOKBACK]

            required = [adx_v, atr_v, ema9_v, ema21_v, ema50_v, ema9_p, ema21_p, ema50_lb]
            if any(v is None for v in required):
                continue

            if adx_v < ADX_MIN:
                filter_counts["ADX_LOW"] += 1
                continue

            slope_pct = (ema50_v - ema50_lb) / ema50_lb * 100.0 if ema50_lb != 0 else 0.0
            bullish_cross = ema9_p <= ema21_p and ema9_v > ema21_v
            bearish_cross = ema9_p >= ema21_p and ema9_v < ema21_v
            trend_up = slope_pct > SLOPE_THRESHOLD
            trend_down = slope_pct < -SLOPE_THRESHOLD

            direction = None
            if not (trend_up or trend_down):
                filter_counts["NO_TREND"] += 1
                continue
            elif trend_up and not bullish_cross:
                filter_counts["WAIT_UP"] += 1
                continue
            elif trend_down and not bearish_cross:
                filter_counts["WAIT_DOWN"] += 1
                continue
            elif trend_up and bullish_cross:
                direction = "long"
            elif trend_down and bearish_cross:
                direction = "short"
            else:
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

            tp_dist = tp_mult * atr_v
            sl_dist = sl_mult * atr_v
            if direction == "long":
                tp = entry_price + tp_dist
                sl = entry_price - sl_dist
            else:
                tp = entry_price - tp_dist
                sl = entry_price + sl_dist

            entry_fee = raw_qty * entry_price * FEE_TAKER

            positions[spec_name] = {
                "dir": direction, "entry": entry_price, "sl": sl, "tp": tp,
                "entry_t": t, "size": raw_qty, "entry_fee": entry_fee,
                "leverage": leverage, "pct": pct,
            }

    return trades, filter_counts, balance


# ================================================================================
# 7. METRICS
# ================================================================================
def compute_metrics(trade_list, label, official_start_ms, end_ms):
    n = len(trade_list)
    if n == 0:
        return {"label": label, "n": 0, "wr": 0.0, "pf": 0.0, "net": 0.0, "net_pct": 0.0,
                "mdd": 0.0, "sharpe": 0.0, "aw": 0.0, "al": 0.0, "exp": 0.0, "dur": 0.0,
                "nlongs": 0, "nshorts": 0, "lwr": 0.0, "swr": 0.0, "monthly": {},
                "maxcw": 0, "maxcl": 0, "gp": 0.0, "gl": 0.0, "trades_per_day": 0.0}
    wins = [tr for tr in trade_list if tr["win"]]
    losses = [tr for tr in trade_list if not tr["win"]]
    gp = sum(tr["pnl"] for tr in wins)
    gl = abs(sum(tr["pnl"] for tr in losses))
    net = sum(tr["pnl"] for tr in trade_list)
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
    expct = net / n
    rets = [tr["pnl"] / INITIAL_CAP for tr in trade_list]
    sharpe = (statistics.mean(rets) / statistics.pstdev(rets) * math.sqrt(n)) if len(rets) > 1 and statistics.pstdev(rets) > 0 else 0.0
    durs = [tr["dur"] for tr in trade_list]
    avg_dur = statistics.mean(durs) if durs else 0.0
    longs = [tr for tr in trade_list if tr["dir"] == "long"]
    shorts = [tr for tr in trade_list if tr["dir"] == "short"]
    lwr = 100.0 * sum(1 for tr in longs if tr["win"]) / len(longs) if longs else 0.0
    swr = 100.0 * sum(1 for tr in shorts if tr["win"]) / len(shorts) if shorts else 0.0

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
    breakeven_wr = round(100.0 * al / (aw + al), 2) if (aw + al) > 0 else 0.0
    return {
        "label": label, "n": n, "wr": round(wr, 2),
        "pf": round(pf, 4) if pf != 999.0 else 999.0,
        "net": round(net, 2), "net_pct": round(100.0 * net / INITIAL_CAP, 2),
        "mdd": round(mdd, 2), "sharpe": round(sharpe, 3),
        "aw": round(aw, 2), "al": round(al, 2), "breakeven_wr": breakeven_wr,
        "exp": round(expct, 2), "dur": round(avg_dur, 1),
        "nlongs": len(longs), "nshorts": len(shorts), "lwr": round(lwr, 2), "swr": round(swr, 2),
        "monthly": dict(sorted(monthly.items())), "maxcw": maxcw, "maxcl": maxcl,
        "gp": round(gp, 2), "gl": round(gl, 2), "trades_per_day": round(n / span_days, 2),
    }


# ================================================================================
# 8. MAIN
# ================================================================================
def main():
    now = datetime.datetime.utcnow()
    end_dt = now
    start_dt = end_dt - datetime.timedelta(days=365 * YEARS_BACK)
    official_start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    fetch_start_ms = official_start_ms - WARMUP_BARS * INTERVAL_MS

    print("=" * 80)
    print("BACKTEST START - Live Bot Strategy, 3 TP/SL ATR-multiplier variants")
    print(f"Window: {start_dt.isoformat()} -> {end_dt.isoformat()} UTC")
    print("=" * 80)

    # ---- fetch + indicators ONCE, reused across all 3 strategies ----
    per_symbol = {}
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
        atr14 = atr(highs, lows, closes, 14)
        adx14 = adx_dmi(highs, lows, closes, 14)

        per_symbol[spec_name] = {
            "times": times, "closes": closes, "highs": highs, "lows": lows,
            "ema9": ema9, "ema21": ema21, "ema50": ema50, "atr": atr14, "adx": adx14,
        }
        global_time_set.update(t for t in times if t >= official_start_ms)

    valid_symbols = [s for s in CORE_COINS_SPEC if per_symbol.get(s) is not None]
    if not valid_symbols:
        print("No symbols had usable data. Aborting.")
        return

    global_times = sorted(global_time_set)
    grid_index = {t: i for i, t in enumerate(global_times)}
    print(f"\nGlobal 15m grid: {len(global_times)} ticks across {len(valid_symbols)} usable symbols")

    for spec_name in valid_symbols:
        d = per_symbol[spec_name]
        aligned = align_to_grid(d["times"], {
            "close": d["closes"], "high": d["highs"], "low": d["lows"],
            "ema9": d["ema9"], "ema21": d["ema21"], "ema50": d["ema50"],
            "atr": d["atr"], "adx": d["adx"],
        }, grid_index)
        d["grid"] = aligned

    all_results = {}
    for strat_name, cfg in STRATEGIES.items():
        print(f"\n{'='*80}\nRunning {strat_name} (TP={cfg['tp_mult']}x ATR, SL={cfg['sl_mult']}x ATR)\n{'='*80}")
        trades, filter_counts, final_balance = run_strategy_sim(
            per_symbol, valid_symbols, global_times, official_start_ms, end_ms,
            cfg["tp_mult"], cfg["sl_mult"]
        )
        per_coin_metrics = []
        for spec_name in valid_symbols:
            coin_trades = [tr for tr in trades if tr["symbol"] == spec_name]
            per_coin_metrics.append(compute_metrics(coin_trades, spec_name, official_start_ms, end_ms))
        per_coin_metrics.sort(key=lambda x: x["pf"], reverse=True)
        aggregate = compute_metrics(trades, "AGGREGATE", official_start_ms, end_ms)
        pf_pass = sum(1 for c in per_coin_metrics if c["pf"] >= 1.5)
        wr_pass = sum(1 for c in per_coin_metrics if c["wr"] >= 42)
        print(f"  trades={aggregate['n']} WR={aggregate['wr']}% PF={aggregate['pf']} "
              f"Net={aggregate['net_pct']}% MDD={aggregate['mdd']}% FinalBal=${final_balance:,.2f}")

        all_results[strat_name] = {
            "tp_mult": cfg["tp_mult"], "sl_mult": cfg["sl_mult"],
            "aggregate": aggregate, "per_coin": per_coin_metrics,
            "filters": dict(filter_counts), "final_balance": round(final_balance, 2),
            "validation": {"pf_target": 1.5, "wr_target": 42, "pf_pass_count": pf_pass,
                           "wr_pass_count": wr_pass, "total_coins": len(per_coin_metrics)},
        }

    # ---- write report ----
    report = {
        "generated": now.isoformat(),
        "config": {
            "initial_cap": INITIAL_CAP, "fee_taker": FEE_TAKER, "slippage": SLIPPAGE,
            "years_back": YEARS_BACK, "window_start": start_dt.isoformat(), "window_end": end_dt.isoformat(),
            "symbols_used": valid_symbols, "symbols_skipped": [s for s in CORE_COINS_SPEC if s not in valid_symbols],
            "entry_logic": "ADX>=22 + 50EMA slope(10bar,0.05%) + EMA9/21 cross (matches live bot exactly)",
        },
        "strategies": all_results,
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    lines = []
    lines.append("=" * 80)
    lines.append("BACKTEST SUMMARY - Live Bot Strategy, 3 TP/SL ATR-multiplier variants")
    lines.append(f"Generated: {now.isoformat()} UTC")
    lines.append(f"Window: {start_dt.date()} -> {end_dt.date()} ({YEARS_BACK} years)")
    lines.append(f"Initial Capital: ${INITIAL_CAP:,.2f} (single shared balance per strategy, across all symbols)")
    lines.append(f"Entry logic (identical across all 3): ADX>=22 + 50EMA slope(10bar,0.05%) + EMA9/21 cross")
    lines.append(f"Fees/slippage added for realism (not in live bot code): Taker {FEE_TAKER*100:.2f}%/side, Slippage {SLIPPAGE*100:.2f}%")
    lines.append(f"Leverage/position tiers (from live bot): <$10=10x/30% | $10-50=10x/10% | >$50=5x/10%")
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Symbols used ({len(valid_symbols)}/30): {', '.join(valid_symbols)}")
    skipped = [s for s in CORE_COINS_SPEC if s not in valid_symbols]
    if skipped:
        lines.append(f"Symbols skipped (insufficient data): {', '.join(skipped)}")
    lines.append("")

    lines.append("QUICK COMPARISON TABLE")
    lines.append(f"  {'STRATEGY':<20}{'TP/SL':<12}{'TRADES':>8}{'WR%':>8}{'PF':>8}{'NET%':>10}{'MDD%':>8}{'FINAL BAL':>12}")
    for strat_name, res in all_results.items():
        agg = res["aggregate"]
        tpsl = f"{res['tp_mult']}x/{res['sl_mult']}x"
        lines.append(f"  {strat_name:<20}{tpsl:<12}{agg['n']:>8}{agg['wr']:>8}{agg['pf']:>8}{agg['net_pct']:>10}{agg['mdd']:>8}{'$'+format(res['final_balance'],',.2f'):>12}")
    lines.append("")

    for strat_name, res in all_results.items():
        lines.append("-" * 80)
        lines.append(f"STRATEGY: {strat_name}  (TP={res['tp_mult']}x ATR / SL={res['sl_mult']}x ATR)")
        lines.append("-" * 80)
        agg = res["aggregate"]
        lines.append(f"AGGREGATE: trades={agg['n']} | WR={agg['wr']}% | PF={agg['pf']} | "
                      f"Net={agg['net_pct']}% (${agg['net']:,.2f}) | MaxDD={agg['mdd']}% | Trades/day={agg['trades_per_day']}")
        lines.append(f"Avg Win: ${agg['aw']} | Avg Loss: ${agg['al']} | Breakeven WR needed: {agg['breakeven_wr']}%")
        lines.append(f"Final Balance: ${res['final_balance']:,.2f} (started at ${INITIAL_CAP:,.2f})")
        v = res["validation"]
        lines.append(f"VALIDATION: PF>=1.5 on {v['pf_pass_count']}/{v['total_coins']} coins | WR>=42% on {v['wr_pass_count']}/{v['total_coins']} coins")
        lines.append("")
        lines.append("Filter stats:")
        f = res["filters"]
        for k in ["ADX_LOW", "NO_TREND", "WAIT_UP", "WAIT_DOWN", "BALANCE_FLOOR_SKIP", "ZERO_QTY_SKIP", "SIGNALS_GENERATED"]:
            lines.append(f"  {k}: {f.get(k, 0)}")
        lines.append("")
        lines.append("Per-coin table (sorted by PF descending):")
        lines.append(f"  {'SYMBOL':<14}{'TRADES':>8}{'WR%':>8}{'PF':>8}{'NET%':>10}{'MDD%':>8}{'AVGWIN':>10}{'AVGLOSS':>10}")
        for c in res["per_coin"]:
            lines.append(f"  {c['label']:<14}{c['n']:>8}{c['wr']:>8}{c['pf']:>8}{c['net_pct']:>10}{c['mdd']:>8}{c['aw']:>10}{c['al']:>10}")
        lines.append("")
        lines.append("Monthly PnL:")
        for month, pnl in agg["monthly"].items():
            lines.append(f"  {month}: ${pnl:,.2f}")
        lines.append("")

    lines.append("=" * 80)
    lines.append("END OF SUMMARY")
    lines.append("=" * 80)

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n\nDONE. Wrote backtest_report.json and backtest_summary.txt")


if __name__ == "__main__":
    main()
