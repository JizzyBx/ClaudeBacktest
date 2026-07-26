"""
================================================================================
BACKTEST.PY - Gemini Strategy Spec: EMA Crossover (15m + 30m), BTCUSDT/ETHUSDT
================================================================================
Pure Python 3.11 stdlib only. No numpy/pandas/requests.
Data source: https://data-api.binance.vision/api/v3/klines (public, no auth)

STRATEGY 1 (15m): 9/21 EMA crossover, 200 EMA macro filter, RSI<70 (long) / RSI>30 (short)
                  SL = 2.0*ATR, TP = 3.5*ATR, opposite-cross emergency flip enabled
STRATEGY 2 (30m): 9/21 EMA crossover, 200 EMA macro filter,
                  RSI 45-68 (long) / RSI 32-55 (short)
                  SL = 2.5*ATR, TP = 4.0*ATR, no emergency flip

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
# 1. COIN LIST
# ================================================================================
COINS = ["BTCUSDT", "ETHUSDT"]

# ================================================================================
# 2. CONFIG
# ================================================================================
FEE_MAKER = 0.0002      # 0.02% - assumed on TP fills (resting/limit-style exit)
FEE_TAKER = 0.0005      # 0.05% - assumed on entries and SL fills (market-style)
SLIPPAGE = 0.0002       # 0.02% extra slippage applied to entries and stop exits
INITIAL_CAP = 10000.0
RISK_PER_TRADE = 0.02   # 2% of current equity risked per trade (ATR-based stop)
LEVERAGE = 10           # isolated margin context (informational / margin check)
COOLDOWN_BARS = 3       # bars to skip after a trade closes, per symbol/strategy

BASE_URL = "https://data-api.binance.vision/api/v3/klines"
MAX_RETRIES = 5

# 2 years of real trading history, plus warmup buffer for EMA200/ATR/RSI convergence
YEARS_BACK = 2
WARMUP_BARS = 400  # extra candles fetched before the official window, for indicator warmup

STRATEGIES = {
    "S1_15m_Momentum_Crossover": {
        "interval": "15m",
        "interval_ms": 15 * 60 * 1000,
        "sl_mult": 2.0,
        "tp_mult": 3.5,
        "rsi_long_max": 70,
        "rsi_short_min": 30,
        "rsi_mode": "threshold",   # long: rsi < 70 | short: rsi > 30
        "emergency_flip": True,
    },
    "S2_30m_Trend_Confirmed_Crossover": {
        "interval": "30m",
        "interval_ms": 30 * 60 * 1000,
        "sl_mult": 2.5,
        "tp_mult": 4.0,
        "rsi_long_band": (45, 68),
        "rsi_short_band": (32, 55),
        "rsi_mode": "band",        # long: 45<=rsi<=68 | short: 32<=rsi<=55
        "emergency_flip": False,
    },
}

# ================================================================================
# 3. FETCH + PARSE
# ================================================================================
def fetch(symbol, interval, start_ms, end_ms):
    """Fetch full kline history for [start_ms, end_ms), paginating 1000 at a time."""
    all_klines = []
    cursor = start_ms
    while cursor < end_ms:
        url = f"{BASE_URL}?symbol={symbol}&interval={interval}&startTime={cursor}&endTime={end_ms}&limit=1000"
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
                    print(f"  [SKIP] {symbol} {interval}: HTTP {e.code} - {e.reason}")
                    return all_klines
                wait = 0.5 * (2 ** attempt)
                print(f"  [RETRY {attempt+1}/{MAX_RETRIES}] {symbol} HTTP {e.code}, waiting {wait:.1f}s")
                time.sleep(wait)
            except Exception as e:
                wait = 0.5 * (2 ** attempt)
                print(f"  [RETRY {attempt+1}/{MAX_RETRIES}] {symbol} error: {e}, waiting {wait:.1f}s")
                time.sleep(wait)
        if data is None:
            print(f"  [FAIL] {symbol} {interval}: giving up after {MAX_RETRIES} retries")
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
# 4. INDICATORS (pure python, None during warmup)
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
    idx = period  # corresponds to closes[period]
    out[idx] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out_idx = i + 1
        if avg_loss == 0:
            out[out_idx] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[out_idx] = 100.0 - (100.0 / (1.0 + rs))
    return out


def atr(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    if n < period + 1:
        return out
    trs = [None] * n
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs[i] = tr
    seed = sum(trs[1:period + 1]) / period
    out[period] = seed
    prev = seed
    for i in range(period + 1, n):
        cur = (prev * (period - 1) + trs[i]) / period
        out[i] = cur
        prev = cur
    return out


# ================================================================================
# 5. STRATEGY SIGNAL FUNCTION
# ================================================================================
def run_strategy(symbol, raw, cfg, official_start_ms):
    opens, highs, lows, closes, volumes, times = parse(raw)
    n = len(closes)
    filter_counts = {
        "candles_scanned": 0,
        "cross_up_events": 0,
        "cross_down_events": 0,
        "macro_reject": 0,
        "rsi_reject": 0,
        "cooldown_or_position_skip": 0,
        "signals_generated": 0,
    }

    if n < 250:
        return [], filter_counts

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema200 = ema(closes, 200)
    rsi14 = rsi(closes, 14)
    atr14 = atr(highs, lows, closes, 14)

    trades = []
    position = None  # dict with dir, entry, sl, tp, entry_i, entry_t, size, risk
    cooldown_until = -1
    equity = INITIAL_CAP  # running equity for position sizing (compounding)

    def rsi_ok(direction, r):
        if cfg["rsi_mode"] == "threshold":
            if direction == "long":
                return r < cfg["rsi_long_max"]
            else:
                return r > cfg["rsi_short_min"]
        else:
            if direction == "long":
                lo, hi = cfg["rsi_long_band"]
            else:
                lo, hi = cfg["rsi_short_band"]
            return lo <= r <= hi

    start_i = 201  # need ema200[i-1] and ema200[i] valid, plus buffer
    for i in range(start_i, n):
        if ema9[i] is None or ema21[i] is None or ema200[i] is None or rsi14[i] is None or atr14[i] is None:
            continue
        if ema9[i - 1] is None or ema21[i - 1] is None:
            continue

        in_official_window = times[i] >= official_start_ms
        if in_official_window:
            filter_counts["candles_scanned"] += 1

        bullish_cross = ema9[i - 1] <= ema21[i - 1] and ema9[i] > ema21[i]
        bearish_cross = ema9[i - 1] >= ema21[i - 1] and ema9[i] < ema21[i]

        # ---- manage open position first ----
        if position is not None:
            exit_price = None
            exit_reason = None
            if position["dir"] == "long":
                # conservative: check SL before TP if both could trigger this candle
                if lows[i] <= position["sl"]:
                    exit_price = position["sl"] * (1 - SLIPPAGE)
                    exit_reason = "SL"
                elif highs[i] >= position["tp"]:
                    exit_price = position["tp"]
                    exit_reason = "TP"
                elif cfg["emergency_flip"] and bearish_cross:
                    exit_price = closes[i]
                    exit_reason = "FLIP"
            else:  # short
                if highs[i] >= position["sl"]:
                    exit_price = position["sl"] * (1 + SLIPPAGE)
                    exit_reason = "SL"
                elif lows[i] <= position["tp"]:
                    exit_price = position["tp"]
                    exit_reason = "TP"
                elif cfg["emergency_flip"] and bullish_cross:
                    exit_price = closes[i]
                    exit_reason = "FLIP"

            if exit_price is not None:
                size = position["size"]
                if position["dir"] == "long":
                    raw_pnl = size * (exit_price - position["entry"])
                else:
                    raw_pnl = size * (position["entry"] - exit_price)
                exit_fee_rate = FEE_MAKER if exit_reason == "TP" else FEE_TAKER
                exit_fee = size * exit_price * exit_fee_rate
                net_pnl = raw_pnl - exit_fee - position["entry_fee"]
                equity += net_pnl
                trades.append({
                    "symbol": symbol,
                    "dir": position["dir"],
                    "entry": position["entry"],
                    "exit": exit_price,
                    "entry_t": position["entry_t"],
                    "exit_t": times[i],
                    "pnl": net_pnl,
                    "win": net_pnl > 0,
                    "hit_tp": exit_reason == "TP",
                    "exit_reason": exit_reason,
                    "dur": (times[i] - position["entry_t"]) / 60000.0,  # minutes
                    "risk": position["risk_amt"],
                })
                position = None
                cooldown_until = i + COOLDOWN_BARS
                continue  # don't evaluate new entries on the same bar we just exited

        # ---- look for new entries ----
        if position is not None:
            continue

        if bullish_cross:
            filter_counts["cross_up_events"] += 1 if in_official_window else 0
        if bearish_cross:
            filter_counts["cross_down_events"] += 1 if in_official_window else 0

        if not (bullish_cross or bearish_cross):
            continue

        if i <= cooldown_until:
            if in_official_window:
                filter_counts["cooldown_or_position_skip"] += 1
            continue

        direction = "long" if bullish_cross else "short"
        macro_ok = closes[i] > ema200[i] if direction == "long" else closes[i] < ema200[i]
        if not macro_ok:
            if in_official_window:
                filter_counts["macro_reject"] += 1
            continue

        if not rsi_ok(direction, rsi14[i]):
            if in_official_window:
                filter_counts["rsi_reject"] += 1
            continue

        # signal confirmed -> open position (only trade within the official 2yr window)
        if not in_official_window:
            continue

        filter_counts["signals_generated"] += 1

        a = atr14[i]
        if a is None or a <= 0:
            continue

        entry_raw = closes[i]
        entry_price = entry_raw * (1 + SLIPPAGE) if direction == "long" else entry_raw * (1 - SLIPPAGE)
        sl_dist = cfg["sl_mult"] * a
        tp_dist = cfg["tp_mult"] * a
        if direction == "long":
            sl = entry_price - sl_dist
            tp = entry_price + tp_dist
        else:
            sl = entry_price + sl_dist
            tp = entry_price - tp_dist

        risk_amt = equity * RISK_PER_TRADE
        size = risk_amt / sl_dist  # base-asset size such that hitting SL loses ~risk_amt
        entry_fee = size * entry_price * FEE_TAKER

        # informational margin check under 10x isolated leverage (not enforced as a hard cap)
        notional = size * entry_price
        margin_required = notional / LEVERAGE

        position = {
            "dir": direction,
            "entry": entry_price,
            "sl": sl,
            "tp": tp,
            "entry_i": i,
            "entry_t": times[i],
            "size": size,
            "entry_fee": entry_fee,
            "risk_amt": risk_amt,
            "margin_required": margin_required,
        }

    return trades, filter_counts


# ================================================================================
# 6. METRICS
# ================================================================================
def metrics(trades, symbol):
    n = len(trades)
    if n == 0:
        return {
            "symbol": symbol, "n": 0, "wr": 0.0, "pf": 0.0, "net": 0.0, "net_pct": 0.0,
            "mdd": 0.0, "sharpe": 0.0, "sortino": 0.0, "aw": 0.0, "al": 0.0, "exp": 0.0,
            "dur": 0.0, "nlongs": 0, "nshorts": 0, "lwr": 0.0, "swr": 0.0, "monthly": {},
            "maxcw": 0, "maxcl": 0, "gp": 0.0, "gl": 0.0,
        }

    wins = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    net = sum(t["pnl"] for t in trades)
    pf = (gp / gl) if gl > 0 else (float("inf") if gp > 0 else 0.0)
    wr = 100.0 * len(wins) / n

    # equity curve & max drawdown
    equity = INITIAL_CAP
    peak = equity
    mdd = 0.0
    curve = []
    for t in trades:
        equity += t["pnl"]
        curve.append(equity)
        peak = max(peak, equity)
        dd = (peak - equity) / peak * 100.0 if peak > 0 else 0.0
        mdd = max(mdd, dd)

    aw = (gp / len(wins)) if wins else 0.0
    al = (gl / len(losses)) if losses else 0.0
    exp = net / n

    rets = [t["pnl"] / INITIAL_CAP for t in trades]
    if len(rets) > 1 and statistics.pstdev(rets) > 0:
        sharpe = (statistics.mean(rets) / statistics.pstdev(rets)) * math.sqrt(n)
    else:
        sharpe = 0.0
    downside = [r for r in rets if r < 0]
    if downside and statistics.pstdev(downside) > 0:
        sortino = (statistics.mean(rets) / statistics.pstdev(downside)) * math.sqrt(n)
    else:
        sortino = 0.0

    durs = [t["dur"] for t in trades]
    avg_dur = statistics.mean(durs) if durs else 0.0

    longs = [t for t in trades if t["dir"] == "long"]
    shorts = [t for t in trades if t["dir"] == "short"]
    lwr = 100.0 * sum(1 for t in longs if t["win"]) / len(longs) if longs else 0.0
    swr = 100.0 * sum(1 for t in shorts if t["win"]) / len(shorts) if shorts else 0.0

    # monthly pnl
    monthly = defaultdict(float)
    for t in trades:
        dt = datetime.datetime.utcfromtimestamp(t["exit_t"] / 1000.0)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] += t["pnl"]

    # max consecutive wins/losses
    maxcw = cw = 0
    maxcl = cl = 0
    for t in trades:
        if t["win"]:
            cw += 1
            cl = 0
        else:
            cl += 1
            cw = 0
        maxcw = max(maxcw, cw)
        maxcl = max(maxcl, cl)

    return {
        "symbol": symbol, "n": n, "wr": round(wr, 2),
        "pf": round(pf, 4) if pf != float("inf") else 999.0,
        "net": round(net, 2), "net_pct": round(100.0 * net / INITIAL_CAP, 2),
        "mdd": round(mdd, 2), "sharpe": round(sharpe, 3), "sortino": round(sortino, 3),
        "aw": round(aw, 2), "al": round(al, 2), "exp": round(exp, 2),
        "dur": round(avg_dur, 1), "nlongs": len(longs), "nshorts": len(shorts),
        "lwr": round(lwr, 2), "swr": round(swr, 2),
        "monthly": dict(sorted(monthly.items())),
        "maxcw": maxcw, "maxcl": maxcl,
        "gp": round(gp, 2), "gl": round(gl, 2),
    }


# ================================================================================
# 7. MAIN
# ================================================================================
def main():
    now = datetime.datetime.utcnow()
    end_dt = now
    start_dt = end_dt - datetime.timedelta(days=365 * YEARS_BACK)
    official_start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    print("=" * 80)
    print("BACKTEST START")
    print(f"Window: {start_dt.isoformat()} -> {end_dt.isoformat()} UTC")
    print(f"Coins: {COINS}")
    print(f"Strategies: {list(STRATEGIES.keys())}")
    print("=" * 80)

    # fetch data once per (symbol, interval) - shared across strategies if intervals match
    raw_cache = {}
    for strat_name, cfg in STRATEGIES.items():
        interval = cfg["interval"]
        interval_ms = cfg["interval_ms"]
        fetch_start_ms = official_start_ms - WARMUP_BARS * interval_ms
        for symbol in COINS:
            key = (symbol, interval)
            if key in raw_cache:
                continue
            print(f"\nFetching {symbol} {interval} ...")
            raw = fetch(symbol, interval, fetch_start_ms, end_ms)
            print(f"  -> {len(raw)} candles fetched")
            raw_cache[key] = raw

    all_results = {}
    report = {"generated": now.isoformat(), "config": {
        "initial_cap": INITIAL_CAP, "risk_per_trade": RISK_PER_TRADE,
        "leverage": LEVERAGE, "fee_maker": FEE_MAKER, "fee_taker": FEE_TAKER,
        "slippage": SLIPPAGE, "years_back": YEARS_BACK,
        "window_start": start_dt.isoformat(), "window_end": end_dt.isoformat(),
    }, "strategies": {}}

    for strat_name, cfg in STRATEGIES.items():
        print(f"\n{'='*80}\nSTRATEGY: {strat_name} ({cfg['interval']})\n{'='*80}")
        per_coin = []
        all_trades = []
        agg_filters = defaultdict(int)

        for symbol in COINS:
            raw = raw_cache[(symbol, cfg["interval"])]
            if not raw or len(raw) < 250:
                print(f"  [SKIP] {symbol}: insufficient data ({len(raw)} candles)")
                continue
            trades, filt = run_strategy(symbol, raw, cfg, official_start_ms)
            m = metrics(trades, symbol)
            per_coin.append(m)
            all_trades.extend(trades)
            for k, v in filt.items():
                agg_filters[k] += v
            print(f"  {symbol}: trades={m['n']} PF={m['pf']} WR={m['wr']}% net={m['net_pct']}% mdd={m['mdd']}%")

        per_coin.sort(key=lambda x: x["pf"], reverse=True)
        agg_metrics = metrics(all_trades, "AGGREGATE")

        pf_pass = sum(1 for c in per_coin if c["pf"] >= 1.5)
        wr_pass = sum(1 for c in per_coin if c["wr"] >= 42)

        all_results[strat_name] = {
            "per_coin": per_coin,
            "aggregate": agg_metrics,
            "filters": dict(agg_filters),
            "validation": {
                "pf_target": 1.5, "wr_target": 42,
                "pf_pass_count": pf_pass, "wr_pass_count": wr_pass,
                "total_coins": len(per_coin),
            },
        }
        report["strategies"][strat_name] = all_results[strat_name]

    # ---- write JSON report ----
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    # ---- write human readable summary ----
    lines = []
    lines.append("=" * 80)
    lines.append("BACKTEST SUMMARY - Gemini EMA Crossover Strategy Spec")
    lines.append(f"Generated: {now.isoformat()} UTC")
    lines.append(f"Window: {start_dt.date()} -> {end_dt.date()} ({YEARS_BACK} years)")
    lines.append(f"Initial Capital: ${INITIAL_CAP:,.2f} | Risk/Trade: {RISK_PER_TRADE*100:.1f}% | Leverage: {LEVERAGE}x")
    lines.append(f"Fees: Maker {FEE_MAKER*100:.2f}% / Taker {FEE_TAKER*100:.2f}% | Slippage: {SLIPPAGE*100:.2f}%")
    lines.append("=" * 80)

    for strat_name, res in all_results.items():
        lines.append("")
        lines.append("-" * 80)
        lines.append(f"STRATEGY: {strat_name}")
        lines.append("-" * 80)
        agg = res["aggregate"]
        lines.append(f"AGGREGATE (all coins combined): trades={agg['n']} | WR={agg['wr']}% | PF={agg['pf']} | "
                      f"Net={agg['net_pct']}% (${agg['net']:,.2f}) | MaxDD={agg['mdd']}%")
        v = res["validation"]
        lines.append(f"VALIDATION: PF>=1.5 on {v['pf_pass_count']}/{v['total_coins']} coins | "
                      f"WR>=42% on {v['wr_pass_count']}/{v['total_coins']} coins")

        lines.append("")
        lines.append("FILTER STATS:")
        f = res["filters"]
        lines.append(f"  Candles scanned (official window): {f.get('candles_scanned', 0)}")
        lines.append(f"  Bullish cross events: {f.get('cross_up_events', 0)}")
        lines.append(f"  Bearish cross events: {f.get('cross_down_events', 0)}")
        lines.append(f"  Rejected by 200EMA macro filter: {f.get('macro_reject', 0)}")
        lines.append(f"  Rejected by RSI filter: {f.get('rsi_reject', 0)}")
        lines.append(f"  Skipped (cooldown/position open): {f.get('cooldown_or_position_skip', 0)}")
        lines.append(f"  Signals generated (trades opened): {f.get('signals_generated', 0)}")

        lines.append("")
        lines.append("PER-COIN TABLE (sorted by PF descending):")
        lines.append(f"  {'SYMBOL':<10}{'TRADES':>8}{'WR%':>8}{'PF':>8}{'NET%':>10}{'MDD%':>8}{'SHARPE':>8}{'AVGWIN':>10}{'AVGLOSS':>10}")
        for c in res["per_coin"]:
            lines.append(f"  {c['symbol']:<10}{c['n']:>8}{c['wr']:>8}{c['pf']:>8}{c['net_pct']:>10}{c['mdd']:>8}{c['sharpe']:>8}{c['aw']:>10}{c['al']:>10}")

        lines.append("")
        lines.append("MONTHLY PNL (aggregate):")
        for month, pnl in agg["monthly"].items():
            lines.append(f"  {month}: ${pnl:,.2f}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("DESIRED OUTPUTS RECAP (per coin, per strategy)")
    lines.append("=" * 80)
    for strat_name, res in all_results.items():
        for c in res["per_coin"]:
            lines.append(f"{strat_name} | {c['symbol']}: NetPnL={c['net_pct']}% | WR={c['wr']}% | "
                          f"PF={c['pf']} | MaxDD={c['mdd']}% | Trades={c['n']}")

    lines.append("=" * 80)
    lines.append("END OF SUMMARY")
    lines.append("=" * 80)

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n\nDONE. Wrote backtest_report.json and backtest_summary.txt")


if __name__ == "__main__":
    main()
