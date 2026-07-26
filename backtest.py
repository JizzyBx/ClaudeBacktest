"""
TRIPLE LOCK CONFLUENCE STRATEGY — BACKTEST V5
==============================================
Strategy : EMA200 + Daily VWAP + MACD(12,26,9) + ATR(14)
Timeframe : 30M | Period: 2 Years | ~28 Coins
R:R       : 1:2.5 | SL = 1.5x ATR | TP = 3.75x ATR
Fee       : 0.05% per side | Slippage: 0.02% per side
stdlib ONLY — zero pip installs
Data      : data-api.binance.vision (public, no auth)
"""

import json
import time
import statistics
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ─────────────────────────────────────────────
# COINS
# ─────────────────────────────────────────────
COINS = [
    # Majors
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    # Layer 1s & 2s  (MATIC renamed to POL in 2024)
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "POLUSDT",
    "ARBUSDT", "OPUSDT", "SUIUSDT", "APTUSDT", "NEARUSDT",
    # High-Volume Utility
    "FETUSDT", "RNDRUSDT", "INJUSDT", "TIAUSDT", "SEIUSDT",
    # Memes  (1000 prefix required on Binance for these)
    "DOGEUSDT", "1000SHIBUSDT", "1000PEPEUSDT", "WIFUSDT", "1000BONKUSDT",
    # Volume-Heavy Memes
    "BOMEUSDT", "MEMEUSDT", "NEIROUSDT",
    # SKIP: 1000FLOKIUSDT (HTTP 400), DOGSUSDT (unlisted)
]

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
INTERVAL     = "30m"
YEARS        = 2
FEE_RATE     = 0.0005   # 0.05% per side
SLIPPAGE     = 0.0002   # 0.02% per side
ROUND_TRIP   = (FEE_RATE + SLIPPAGE) * 2   # 0.14% total

EMA_PERIOD   = 200
ATR_PERIOD   = 14
MACD_FAST    = 12
MACD_SLOW    = 26
MACD_SIG     = 9
ATR_SL       = 1.5
ATR_TP       = 3.75     # gives 2.5:1 R:R

BINANCE_URL  = "https://data-api.binance.vision/api/v3/klines"
BATCH        = 1000
SLEEP        = 0.13     # seconds between requests

# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────

def fetch_klines(symbol, interval, start_ms, end_ms):
    all_data  = []
    cur_start = start_ms

    while cur_start < end_ms:
        url = (f"{BINANCE_URL}?symbol={symbol}&interval={interval}"
               f"&startTime={cur_start}&endTime={end_ms}&limit={BATCH}")

        for attempt in range(5):
            try:
                with urllib.request.urlopen(url, timeout=20) as r:
                    batch = json.loads(r.read().decode())
                break
            except Exception as e:
                wait = 2 ** attempt
                print(f"    retry {attempt+1}/5 ({e}) — wait {wait}s")
                time.sleep(wait)
        else:
            print(f"    SKIP {symbol} — all retries failed")
            return None

        if not batch:
            break

        all_data.extend(batch)
        cur_start = batch[-1][0] + 1

        if len(batch) < BATCH:
            break
        time.sleep(SLEEP)

    return all_data


def parse(raw):
    times, opens, highs, lows, closes, vols = [], [], [], [], [], []
    for c in raw:
        times.append(int(c[0]))
        opens.append(float(c[1]))
        highs.append(float(c[2]))
        lows.append(float(c[3]))
        closes.append(float(c[4]))
        vols.append(float(c[5]))
    return opens, highs, lows, closes, vols, times

# ─────────────────────────────────────────────
# INDICATORS — pure Python, no numpy
# ─────────────────────────────────────────────

def ema(values, period):
    result = [None] * len(values)
    k = 2.0 / (period + 1)
    last = None
    for i, v in enumerate(values):
        if v is None:
            continue
        if last is None:
            last = v
        else:
            last = v * k + last * (1 - k)
        result[i] = last
    return result


def calc_atr(highs, lows, closes, period=14):
    trs = [None]
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i]  - closes[i-1]))
        trs.append(tr)
    return ema(trs, period)


def calc_macd_hist(closes):
    ef  = ema(closes, MACD_FAST)
    es  = ema(closes, MACD_SLOW)
    ml  = [f - s if f and s else None for f, s in zip(ef, es)]
    sig = ema(ml, MACD_SIG)
    return [m - s if m is not None and s is not None else None
            for m, s in zip(ml, sig)]


def calc_vwap(highs, lows, closes, vols, times):
    """Daily VWAP anchored to 00:00 UTC — resets each day."""
    vwap    = [None] * len(closes)
    cum_pv  = 0.0
    cum_vol = 0.0
    cur_day = None

    for i in range(len(closes)):
        dt  = datetime.fromtimestamp(times[i] / 1000, tz=timezone.utc)
        day = dt.date()
        if day != cur_day:
            cur_day = day
            cum_pv  = 0.0
            cum_vol = 0.0
        tp       = (highs[i] + lows[i] + closes[i]) / 3.0
        cum_pv  += tp * vols[i]
        cum_vol += vols[i]
        if cum_vol > 0:
            vwap[i] = cum_pv / cum_vol
    return vwap

# ─────────────────────────────────────────────
# STRATEGY
# ─────────────────────────────────────────────

def run_triple_lock(symbol, raw):
    opens, highs, lows, closes, vols, times = parse(raw)
    n = len(closes)

    e200  = ema(closes, EMA_PERIOD)
    atr_v = calc_atr(highs, lows, closes, ATR_PERIOD)
    hist  = calc_macd_hist(closes)
    vwap  = calc_vwap(highs, lows, closes, vols, times)

    trades  = []
    filters = defaultdict(int)
    filters["total_candles"] = n

    in_trade  = False
    direction = None
    entry_px  = 0.0
    sl_px     = 0.0
    tp_px     = 0.0
    entry_idx = 0
    entry_t   = 0

    warmup = max(EMA_PERIOD, MACD_SLOW + MACD_SIG) + 5

    for i in range(warmup, n - 1):
        # ── EXIT ────────────────────────────────────
        if in_trade:
            h, l = highs[i], lows[i]
            ep, et = None, None

            if direction == "LONG":
                if l <= sl_px:
                    ep, et = sl_px, "SL"
                elif h >= tp_px:
                    ep, et = tp_px, "TP"
            else:
                if h >= sl_px:
                    ep, et = sl_px, "SL"
                elif l <= tp_px:
                    ep, et = tp_px, "TP"

            if ep is not None:
                raw_pnl = ((ep - entry_px) / entry_px if direction == "LONG"
                           else (entry_px - ep) / entry_px)
                net_pnl = raw_pnl - ROUND_TRIP
                trades.append({
                    "dir":     direction,
                    "entry":   entry_px,
                    "exit":    ep,
                    "entry_t": entry_t,
                    "exit_t":  times[i],
                    "pnl":     net_pnl * 100,
                    "win":     net_pnl > 0,
                    "hit_tp":  et == "TP",
                    "dur":     i - entry_idx,
                })
                in_trade = False
            continue

        # ── SIGNAL ──────────────────────────────────
        e   = e200[i]
        a   = atr_v[i]
        vw  = vwap[i]
        h_c = hist[i]
        h_p = hist[i-1]
        cl  = closes[i]
        lo  = lows[i]
        hi  = highs[i]
        lp  = lows[i-1]
        hp  = highs[i-1]

        if any(v is None for v in [e, a, vw, h_c, h_p]):
            filters["warmup"] += 1
            continue

        # LONG: all 4 locks
        long_ok = (
            cl > e                          # 1. Trend Lock
            and (lo <= vw or lp <= vw)      # 2. Value Lock
            and h_p < 0 and h_c > 0        # 3. Momentum Lock (cross up)
            and cl > vw                    # 4. Confirmation
        )

        # SHORT: all 4 locks
        short_ok = (
            cl < e                          # 1. Trend Lock
            and (hi >= vw or hp >= vw)      # 2. Value Lock
            and h_p > 0 and h_c < 0        # 3. Momentum Lock (cross down)
            and cl < vw                    # 4. Confirmation
        )

        # Filter counting (for rejected candles)
        if not long_ok and not short_ok:
            if cl <= e:   filters["long_trend_fail"] += 1
            if hi < vw:   filters["short_value_fail"] += 1
            filters["no_signal"] += 1
            continue

        if long_ok:
            ep        = opens[i+1] * (1 + SLIPPAGE)
            sl_px     = ep - ATR_SL * a
            tp_px     = ep + ATR_TP * a
            in_trade  = True
            direction = "LONG"
            entry_px  = ep
            entry_idx = i + 1
            entry_t   = times[i+1]
            filters["long_signals"] += 1

        elif short_ok:
            ep        = opens[i+1] * (1 - SLIPPAGE)
            sl_px     = ep + ATR_SL * a
            tp_px     = ep - ATR_TP * a
            in_trade  = True
            direction = "SHORT"
            entry_px  = ep
            entry_idx = i + 1
            entry_t   = times[i+1]
            filters["short_signals"] += 1

    return trades, dict(filters)

# ─────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────

def metrics(trades, symbol):
    if not trades:
        return {"symbol": symbol, "n": 0, "wr": 0, "pf": 0, "net": 0,
                "mdd": 0, "aw": 0, "al": 0, "exp": 0, "dur": 0,
                "nlongs": 0, "nshorts": 0, "lwr": 0, "swr": 0,
                "tp_cnt": 0, "sl_cnt": 0, "gp": 0, "gl": 0,
                "maxcw": 0, "maxcl": 0, "monthly": {}}

    pnls  = [t["pnl"] for t in trades]
    wins  = [p for p in pnls if p > 0]
    loses = [p for p in pnls if p <= 0]

    gp = sum(wins)
    gl = abs(sum(loses)) if loses else 0
    pf = round(gp / gl, 4) if gl > 0 else float("inf")
    wr = round(len(wins) / len(pnls) * 100, 2)

    equity = 100.0
    peak   = 100.0
    mdd    = 0.0
    for p in pnls:
        equity *= (1 + p / 100)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > mdd:
            mdd = dd

    aw  = round(statistics.mean(wins),  4) if wins  else 0
    al  = round(statistics.mean(loses), 4) if loses else 0
    exp = round((wr/100 * aw) + ((1 - wr/100) * al), 4)

    longs  = [t for t in trades if t["dir"] == "LONG"]
    shorts = [t for t in trades if t["dir"] == "SHORT"]
    lwr    = round(sum(1 for t in longs  if t["win"]) / max(len(longs),1)  * 100, 2)
    swr    = round(sum(1 for t in shorts if t["win"]) / max(len(shorts),1) * 100, 2)
    tp_cnt = sum(1 for t in trades if t["hit_tp"])

    monthly = defaultdict(float)
    for t in trades:
        key = datetime.fromtimestamp(t["entry_t"]/1000, tz=timezone.utc).strftime("%Y-%m")
        monthly[key] += t["pnl"]

    maxcw = maxcl = cw = cl_cnt = 0
    for t in trades:
        if t["win"]: cw += 1; cl_cnt = 0
        else:        cl_cnt += 1; cw = 0
        maxcw = max(maxcw, cw)
        maxcl = max(maxcl, cl_cnt)

    return {
        "symbol":  symbol,
        "n":       len(trades),
        "wr":      wr,
        "pf":      pf,
        "net":     round(sum(pnls), 2),
        "mdd":     round(mdd, 2),
        "aw":      aw,
        "al":      al,
        "exp":     exp,
        "dur":     round(statistics.mean([t["dur"] for t in trades]), 1),
        "nlongs":  len(longs),
        "nshorts": len(shorts),
        "lwr":     lwr,
        "swr":     swr,
        "tp_cnt":  tp_cnt,
        "sl_cnt":  len(trades) - tp_cnt,
        "gp":      round(gp, 2),
        "gl":      round(gl, 2),
        "maxcw":   maxcw,
        "maxcl":   maxcl,
        "monthly": dict(sorted(monthly.items())),
    }


def aggregate(results):
    valid = [r for r in results if r.get("n", 0) >= 10]
    if not valid:
        return {}
    nt  = sum(r["n"] for r in valid)
    nw  = sum(int(r["wr"] / 100 * r["n"]) for r in valid)
    tgp = sum(r["gp"] for r in valid)
    tgl = sum(r["gl"] for r in valid)
    return {
        "coins_tested":     len(valid),
        "total_trades":     nt,
        "overall_wr":       round(nw / nt * 100, 2) if nt else 0,
        "overall_pf":       round(tgp / tgl, 4) if tgl else float("inf"),
        "avg_max_dd":       round(statistics.mean([r["mdd"] for r in valid]), 2),
        "coins_pf_ge_1_5":  sum(1 for r in valid if r["pf"] >= 1.5),
        "coins_pf_ge_1_0":  sum(1 for r in valid if r["pf"] >= 1.0),
        "coins_wr_ge_42":   sum(1 for r in valid if r["wr"] >= 42),
    }

# ─────────────────────────────────────────────
# SUMMARY WRITER
# ─────────────────────────────────────────────

def write_summary(results, agg, all_filters, skipped):
    lines = []
    sep   = "=" * 70

    lines += [sep,
              "TRIPLE LOCK CONFLUENCE — BACKTEST RESULTS",
              f"Generated : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
              "Strategy  : EMA200 + Daily VWAP + MACD(12,26,9) + ATR(14)",
              "Period    : 2 Years | 30M | SL=1.5xATR TP=3.75xATR Fee=0.14%",
              sep, ""]

    lines += ["AGGREGATE", "-" * 40]
    for k, v in agg.items():
        lines.append(f"  {k:<28}: {v}")

    lines += ["", sep, "VALIDATION", "-" * 40]
    checks = [
        ("PF >= 1.5 on 8+ coins",  agg.get("coins_pf_ge_1_5", 0) >= 8),
        ("WR >= 42% on 8+ coins",  agg.get("coins_wr_ge_42",  0) >= 8),
        ("Avg max DD < 20%",        agg.get("avg_max_dd", 99)    <  20),
        ("Overall PF >= 1.5",       agg.get("overall_pf", 0)     >= 1.5),
    ]
    for label, passed in checks:
        lines.append(f"  {'PASS' if passed else 'FAIL'}  {label}")
    verdict = all(p for _, p in checks)
    lines += ["", f"  VERDICT: {'STRATEGY VIABLE' if verdict else 'NEEDS REVISION'}"]

    lines += ["", sep, "PER-COIN (sorted by PF)", "-" * 70,
              f"{'Coin':<16}{'Trades':>7}{'WR%':>7}{'PF':>8}{'Net%':>9}{'DD%':>7}{'TP%':>6}{'Dur':>6}",
              "-" * 70]

    valid = sorted([r for r in results if r.get("n", 0) >= 1],
                   key=lambda x: x["pf"], reverse=True)
    for r in valid:
        tp_r = round(r["tp_cnt"] / r["n"] * 100, 1) if r["n"] else 0
        flag = "+" if r["pf"] >= 1.5 else " "
        lines.append(
            f"{flag}{r['symbol']:<15}{r['n']:>7}{r['wr']:>7.1f}"
            f"{r['pf']:>8.4f}{r['net']:>9.2f}{r['mdd']:>7.2f}"
            f"{tp_r:>5.1f}%{r['dur']:>6.1f}"
        )

    lines += ["", sep, "LONG vs SHORT", "-" * 55,
              f"{'Coin':<16}{'L.Trades':>9}{'L.WR%':>8}{'S.Trades':>10}{'S.WR%':>8}",
              "-" * 55]
    for r in valid:
        lines.append(f"{r['symbol']:<16}{r['nlongs']:>9}{r['lwr']:>8.1f}"
                     f"{r['nshorts']:>10}{r['swr']:>8.1f}")

    lines += ["", sep, "FILTER STATS (all coins combined)", "-" * 50]
    combined = defaultdict(int)
    for f in all_filters.values():
        for k, v in f.items():
            combined[k] += v
    for k, v in sorted(combined.items(), key=lambda x: -x[1]):
        lines.append(f"  {k:<35}: {v:>8,}")

    if skipped:
        lines += ["", f"SKIPPED: {', '.join(skipped)}"]

    lines += ["", sep, "MONTHLY PnL — BTCUSDT (reference)", "-" * 50]
    btc = next((r for r in results if r["symbol"] == "BTCUSDT"), None)
    if btc and btc.get("monthly"):
        for mo, pnl in btc["monthly"].items():
            bar = ("+" if pnl > 0 else "-") * min(int(abs(pnl)), 40)
            lines.append(f"  {mo}  {pnl:>8.2f}%  {bar}")

    lines += ["", sep, "END OF REPORT", sep]

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))
    print("Written → backtest_summary.txt")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("TRIPLE LOCK BACKTEST | 2yr | 30M | stdlib only")
    print("=" * 65)

    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = int((datetime.now(timezone.utc) - timedelta(days=365 * YEARS)).timestamp() * 1000)

    print(f"Range: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).date()} "
          f"to {datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()}")
    print(f"Coins: {len(COINS)}")

    all_results = []
    all_filters = {}
    skipped     = []

    for idx, symbol in enumerate(COINS, 1):
        print(f"\n[{idx:02d}/{len(COINS)}] {symbol} fetching...", flush=True)
        raw = fetch_klines(symbol, INTERVAL, start_ms, end_ms)

        if not raw or len(raw) < EMA_PERIOD + 100:
            cnt = len(raw) if raw else 0
            print(f"         SKIP — only {cnt} candles")
            skipped.append(symbol)
            continue

        print(f"         {len(raw):,} candles — running strategy...", flush=True)
        trades, filters = run_triple_lock(symbol, raw)
        all_filters[symbol] = filters

        r = metrics(trades, symbol)
        all_results.append(r)

        if r["n"] == 0:
            print(f"         No trades (L={filters.get('long_signals',0)} S={filters.get('short_signals',0)} signals)")
        else:
            flag = "PASS" if r["pf"] >= 1.5 else "FAIL"
            print(f"         [{flag}] PF={r['pf']:.4f} WR={r['wr']:.1f}% "
                  f"Trades={r['n']} DD={r['mdd']:.1f}% "
                  f"TP={r['tp_cnt']} SL={r['sl_cnt']}")

    agg = aggregate(all_results)

    print("\n" + "=" * 65)
    print("AGGREGATE")
    for k, v in agg.items():
        print(f"  {k}: {v}")

    write_summary(all_results, agg, all_filters, skipped)

    report = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "strategy":  "Triple Lock Confluence",
        "period":    f"{YEARS} years | 30M",
        "params": {
            "ema": EMA_PERIOD, "atr": ATR_PERIOD,
            "macd": f"{MACD_FAST}/{MACD_SLOW}/{MACD_SIG}",
            "sl_mult": ATR_SL, "tp_mult": ATR_TP,
            "round_trip_cost": ROUND_TRIP,
        },
        "aggregate":  agg,
        "per_coin":   all_results,
        "filters":    all_filters,
        "skipped":    skipped,
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("Written → backtest_report.json")
    print("=" * 65)


if __name__ == "__main__":
    main()
