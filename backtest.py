"""
Sweep & Engulf — Dual Variant Backtest
=======================================
Variants tested in one run:
  V1 : 1H timeframe
  V2 : 4H timeframe

Strategy:
  Bullish : candle wicks BELOW prev low AND closes ABOVE prev high
  Bearish : candle wicks ABOVE prev high AND closes BELOW prev low

Filters:
  - EMA 200 trend filter (bullish only above EMA, bearish only below)
  - ATR guard (no signal until ATR fully warmed)
  - Previous candle direction: "any" or "same"

Config (shared):
  Capital    : $10,000
  Risk/trade : 0.75%
  TP         : 3.0x ATR
  SL         : 1.5x ATR  (RR = 2.0)
  Leverage   : 5x
  Fee        : 0.05%  Slip: 0.02%
  EMA period : 200
  ATR period : 14

Output (per variant):
  backtest_1h_report.json + backtest_1h_summary.txt
  backtest_4h_report.json + backtest_4h_summary.txt

Author: Paqu / Engineered By Paqu
"""

import csv
import io
import json
import os
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.error import HTTPError

# ── Coin list ─────────────────────────────────────────────────────────────────
ALL_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","XRPUSDT","SOLUSDT","ADAUSDT","DOGEUSDT",
    "AVAXUSDT","DOTUSDT","TRXUSDT","LINKUSDT","MATICUSDT","LTCUSDT","BCHUSDT",
    "XLMUSDT","ATOMUSDT","ETCUSDT","XMRUSDT","FILUSDT","APTUSDT","ARBUSDT",
    "OPUSDT","NEARUSDT","INJUSDT","RUNEUSDT","AAVEUSDT","LDOUSDT","MKRUSDT",
    "SNXUSDT","COMPUSDT","UNIUSDT","SUSHIUSDT","1INCHUSDT","CRVUSDT","BALUSDT",
    "YFIUSDT","MANAUSDT","SANDUSDT","AXSUSDT","GALAUSDT","ENJUSDT","CHZUSDT",
    "FLOWUSDT","IMXUSDT","GMTUSDT","STXUSDT","HBARUSDT","EGLDUSDT","THETAUSDT",
    "ALGOUSDT","VETUSDT","ICPUSDT","FTMUSDT","ZILUSDT","KAVAUSDT","BANDUSDT",
    "RSRUSDT","CTKUSDT","OCEANUSDT","ANKRUSDT","CELRUSDT","IOTAUSDT","ONEUSDT",
    "ONTUSDT","ZECUSDT","DASHUSDT","WAVESUSDT","KSMUSDT","SKLUSDT","COTIUSDT",
    "BAKEUSDT","PONDUSDT","LITUSDT","SFPUSDT","FTTUSDT","RAYUSDT","SRMUSDT",
    "C98USDT","WOOUSDT","TLMUSDT","ALICEUSDT","LRCUSDT","PEOPLEUSDT","ROSEUSDT",
    "DYDXUSDT","GRTUSDT","ARUSDT","ENSUSDT","STORJUSDT","POWRUSDT","IDEXUSDT",
    "REQUSDT","CTSIUSDT","PERPUSDT","SPELLUSDT","JASMYUSDT","MAGICUSDT",
    "HOOKUSDT","HIGHUSDT","PHBUSDT","CFXUSDT","SSVUSDT","ACHUSDT","IDUSDT",
    "SUIUSDT","PEPEUSDT","FLOKIUSDT","WLDUSDT","CYBERUSDT","SEIUSDT",
    "TIAUSDT","ORDIUSDT","BEAMXUSDT","PIXELUSDT","PORTALUSDT","PDAUSDT",
    "AXLUSDT","STRKUSDT","ALTUSDT","JUPUSDT","DYMUSDT","PYTHUSDT","JSTOUSDT",
]

_seen, _deduped = set(), []
for _s in ALL_SYMBOLS:
    if _s not in _seen:
        _seen.add(_s)
        _deduped.append(_s)
ALL_SYMBOLS = _deduped[:117]

# ── Shared config ─────────────────────────────────────────────────────────────
NUM_SHARDS  = 8
WORKERS     = 16
START_YM    = (2023, 1)
END_YM      = (2025, 7)
CAPITAL     = 10_000.0
RISK_PCT    = 0.0075
FEE         = 0.0005
SLIP        = 0.0002
LEVERAGE    = 5
EMA_PERIOD  = 200
ATR_PERIOD  = 14
ATR_TP_MULT = 3.0
ATR_SL_MULT = 1.5
MIN_BARS    = EMA_PERIOD + 20   # 220

USE_EMA_FILTER     = True
PREV_CANDLE_FILTER = "any"  # "any" | "same"

# ── Variants ──────────────────────────────────────────────────────────────────
VARIANTS = {
    "1h": {"timeframe": "1h", "max_bars": 100},
    "4h": {"timeframe": "4h", "max_bars": 50},
}

# ── Data fetch ────────────────────────────────────────────────────────────────
BASE_URL = (
    "https://data.binance.vision/data/futures/um/monthly/klines"
    "/{sym}/{tf}/{sym}-{tf}-{yyyy}-{mm:02d}.zip"
)

def fetch_month(symbol, timeframe, year, month):
    url = BASE_URL.format(sym=symbol, tf=timeframe, yyyy=year, mm=month)
    try:
        with urlopen(url, timeout=30) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as fh:
                rows = list(csv.reader(io.TextIOWrapper(fh)))
        candles = []
        for r in rows:
            if not r or not r[0].isdigit():
                continue
            ts = int(r[0])
            if ts > 10**14:
                ts //= 1000
            candles.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4])))
        return candles
    except HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        return []

def fetch_symbol(symbol, timeframe):
    sy, sm = START_YM
    ey, em = END_YM
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    all_candles = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_month, symbol, timeframe, yr, mo): (yr, mo) for yr, mo in months}
        for f in as_completed(futs):
            all_candles.extend(f.result())
    seen_ts = {}
    for c in all_candles:
        seen_ts[c[0]] = c
    return sorted(seen_ts.values(), key=lambda x: x[0])

# ── Indicators ────────────────────────────────────────────────────────────────
def calc_ema(closes, period):
    ema = [None] * len(closes)
    if len(closes) < period:
        return ema
    k = 2.0 / (period + 1)
    ema[period - 1] = sum(closes[:period]) / period
    for i in range(period, len(closes)):
        ema[i] = closes[i] * k + ema[i - 1] * (1 - k)
    return ema

def calc_atr(highs, lows, closes, period):
    n = len(closes)
    atr = [None] * n
    if n < period + 1:
        return atr
    trs = []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return atr
    atr[period] = sum(trs[:period]) / period
    for i in range(period + 1, n):
        atr[i] = (atr[i - 1] * (period - 1) + trs[i - 1]) / period
    return atr

# ── Signal ────────────────────────────────────────────────────────────────────
def get_signal(i, opens, highs, lows, closes, ema_vals, atr_vals):
    if i < 1:
        return None
    if atr_vals[i] is None:
        return None
    if USE_EMA_FILTER and ema_vals[i] is None:
        return None

    cur_h, cur_l, cur_c = highs[i], lows[i], closes[i]
    prv_o, prv_h, prv_l, prv_c = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
    prv_bull = prv_c > prv_o
    prv_bear = prv_c < prv_o

    if USE_EMA_FILTER:
        trend_bull = cur_c > ema_vals[i]
        trend_bear = cur_c < ema_vals[i]
    else:
        trend_bull = trend_bear = True

    bullish = (cur_l < prv_l) and (cur_c > prv_h)
    if bullish:
        if PREV_CANDLE_FILTER == "same" and not prv_bull:
            bullish = False
        if not trend_bull:
            bullish = False

    bearish = (cur_h > prv_h) and (cur_c < prv_l)
    if bearish:
        if PREV_CANDLE_FILTER == "same" and not prv_bear:
            bearish = False
        if not trend_bear:
            bearish = False

    if bullish:
        return "buy"
    if bearish:
        return "sell"
    return None

# ── Backtest single symbol ────────────────────────────────────────────────────
def backtest(symbol, candles, max_bars):
    if len(candles) < MIN_BARS + 2:
        return []

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    ema_vals = calc_ema(closes, EMA_PERIOD)
    atr_vals = calc_atr(highs, lows, closes, ATR_PERIOD)

    trades    = []
    n         = len(candles)
    in_trade  = False
    side      = None
    entry_p   = sl_p = tp_p = notional = 0.0
    entry_ts  = entry_bar = 0

    for i in range(MIN_BARS, n - 1):
        if in_trade:
            bar_h, bar_l = highs[i], lows[i]
            bars_held = i - entry_bar
            exit_p = reason = None

            if side == "buy":
                if bar_l <= sl_p:
                    exit_p, reason = sl_p, "sl"
                elif bar_h >= tp_p:
                    exit_p, reason = tp_p, "tp"
            else:
                if bar_h >= sl_p:
                    exit_p, reason = sl_p, "sl"
                elif bar_l <= tp_p:
                    exit_p, reason = tp_p, "tp"

            if reason is None and bars_held >= max_bars:
                exit_p, reason = closes[i], "max_hold"

            if exit_p is not None:
                gross = (exit_p - entry_p) / entry_p if side == "buy" \
                        else (entry_p - exit_p) / entry_p
                pnl = notional * (gross - (FEE + SLIP) * 2) * LEVERAGE
                trades.append({
                    "symbol":      symbol,
                    "side":        side,
                    "entry_ts":    entry_ts,
                    "exit_ts":     ts_arr[i],
                    "entry_price": entry_p,
                    "exit_price":  exit_p,
                    "pnl":         pnl,
                    "reason":      reason,
                    "bars":        bars_held,
                })
                in_trade = False

        else:
            sig = get_signal(i, opens, highs, lows, closes, ema_vals, atr_vals)
            if sig is None:
                continue
            atr = atr_vals[i]
            if atr is None:
                continue

            next_open = opens[i + 1]
            if sig == "buy":
                ep = next_open * (1 + FEE + SLIP)
                sl = ep - ATR_SL_MULT * atr
                tp = ep + ATR_TP_MULT * atr
            else:
                ep = next_open * (1 - FEE - SLIP)
                sl = ep + ATR_SL_MULT * atr
                tp = ep - ATR_TP_MULT * atr

            sl_dist_pct = abs(ep - sl) / ep
            if sl_dist_pct <= 0:
                continue

            size = min(CAPITAL * RISK_PCT / sl_dist_pct, CAPITAL * LEVERAGE)
            in_trade  = True
            side      = sig
            entry_p   = ep
            sl_p      = sl
            tp_p      = tp
            entry_ts  = ts_arr[i + 1]
            entry_bar = i + 1
            notional  = size

    if in_trade:
        exit_p = closes[-1]
        bars_held = (n - 1) - entry_bar
        gross = (exit_p - entry_p) / entry_p if side == "buy" \
                else (entry_p - exit_p) / entry_p
        pnl = notional * (gross - (FEE + SLIP) * 2) * LEVERAGE
        trades.append({
            "symbol":      symbol,
            "side":        side,
            "entry_ts":    entry_ts,
            "exit_ts":     ts_arr[-1],
            "entry_price": entry_p,
            "exit_price":  exit_p,
            "pnl":         pnl,
            "reason":      "end_of_data",
            "bars":        (n - 1) - entry_bar,
        })

    return trades

# ── Stats ─────────────────────────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {
            "total": 0, "win_rate": 0.0, "profit_factor": 0.0,
            "net_pnl": 0.0, "max_drawdown": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "expectancy": 0.0,
            "longs": 0, "shorts": 0, "monthly": {}, "per_coin": {},
        }

    wins   = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    gw     = sum(wins)
    gl     = abs(sum(losses))
    net    = sum(t["pnl"] for t in trades)
    wr     = len(wins) / len(trades) * 100
    pf     = gw / gl if gl > 0 else float("inf")
    aw     = gw / len(wins)   if wins   else 0.0
    al     = gl / len(losses) if losses else 0.0
    exp    = (wr / 100 * aw) - ((1 - wr / 100) * al)

    equity = peak = max_dd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        equity += t["pnl"]
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    monthly  = {}
    per_coin = {}
    for t in trades:
        dt  = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        key = dt.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"pnl": 0.0, "n": 0, "w": 0}
        monthly[key]["pnl"] += t["pnl"]
        monthly[key]["n"]   += 1
        if t["pnl"] > 0:
            monthly[key]["w"] += 1

        sym = t["symbol"]
        if sym not in per_coin:
            per_coin[sym] = {"pnl": 0.0, "n": 0, "w": 0, "wr": 0.0}
        per_coin[sym]["pnl"] += t["pnl"]
        per_coin[sym]["n"]   += 1
        if t["pnl"] > 0:
            per_coin[sym]["w"] += 1

    for v in per_coin.values():
        v["wr"] = v["w"] / v["n"] * 100 if v["n"] > 0 else 0.0

    return {
        "total":         len(trades),
        "win_rate":      round(wr, 2),
        "profit_factor": round(pf, 4),
        "net_pnl":       round(net, 2),
        "max_drawdown":  round(max_dd / CAPITAL * 100, 2),
        "avg_win":       round(aw, 2),
        "avg_loss":      round(al, 2),
        "expectancy":    round(exp, 2),
        "longs":         sum(1 for t in trades if t["side"] == "buy"),
        "shorts":        sum(1 for t in trades if t["side"] == "sell"),
        "monthly":       monthly,
        "per_coin":      per_coin,
    }

# ── Shard runner ──────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    import time
    start   = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} symbols — variants: {list(VARIANTS.keys())}")

    results   = {vk: [] for vk in VARIANTS}
    with_data = {vk: [] for vk in VARIANTS}

    def process(sym, vkey, vconf):
        tf      = vconf["timeframe"]
        mb      = vconf["max_bars"]
        candles = fetch_symbol(sym, tf)
        if len(candles) < MIN_BARS + 2:
            print(f"  [{vkey}] {sym}: {len(candles)} bars (skip)")
            return vkey, sym, []
        trades = backtest(sym, candles, mb)
        print(f"  [{vkey}] {sym}: {len(candles)} bars -> {len(trades)} trades")
        return vkey, sym, trades

    tasks = [(sym, vk, vc) for sym in symbols for vk, vc in VARIANTS.items()]
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process, sym, vk, vc): (sym, vk) for sym, vk, vc in tasks}
        for f in as_completed(futs):
            vkey, sym, trades = f.result()
            results[vkey].extend(trades)
            if trades and sym not in with_data[vkey]:
                with_data[vkey].append(sym)

    elapsed = time.time() - start
    shard_out = {
        "shard":   shard_idx,
        "symbols": symbols,
        "elapsed": round(elapsed, 1),
        "variants": {
            vk: {
                "trades":    results[vk],
                "with_data": with_data[vk],
                "stats":     stats(results[vk]),
            }
            for vk in VARIANTS
        },
    }
    out_path = f"shard_{shard_idx}.json"
    with open(out_path, "w") as f:
        json.dump(shard_out, f)
    for vk in VARIANTS:
        print(f"  [{vk}] shard {shard_idx}: {len(results[vk])} trades")
    print(f"[Shard {shard_idx}] Done {elapsed:.1f}s -> {out_path}")

# ── Summary builder ───────────────────────────────────────────────────────────
def build_summary(s, label, tf, max_bars, n_sym, n_wd):
    usable  = s["profit_factor"] >= 1.5 and s["win_rate"] >= 42.0
    verdict = "USABLE" if usable else "NOT USABLE"
    tick    = "v" if usable else "x"

    lines = [
        "=" * 60,
        f"  SWEEP & ENGULF ({label}) - BACKTEST SUMMARY",
        "=" * 60,
        f"  Period     : {START_YM[0]}-{START_YM[1]:02d} -> {END_YM[0]}-{END_YM[1]:02d}",
        f"  Timeframe  : {tf}  (max hold: {max_bars} bars)",
        f"  Symbols    : {n_sym} attempted / {n_wd} with data",
        f"  EMA Filter : {'ON (200 EMA)' if USE_EMA_FILTER else 'OFF'}",
        f"  Prev Candle: {PREV_CANDLE_FILTER.upper()}",
        f"  Capital    : ${CAPITAL:,.0f}  Leverage: {LEVERAGE}x",
        f"  Risk/Trade : {RISK_PCT*100:.2f}%  Fee: {FEE*100:.3f}%  Slip: {SLIP*100:.3f}%",
        f"  ATR SL     : {ATR_SL_MULT}xATR   ATR TP: {ATR_TP_MULT}xATR   RR: {ATR_TP_MULT/ATR_SL_MULT:.1f}",
        "-" * 60,
        f"  Total Trades   : {s['total']}",
        f"  Win Rate       : {s['win_rate']:.2f}%",
        f"  Profit Factor  : {s['profit_factor']:.4f}",
        f"  Net PnL        : ${s['net_pnl']:,.2f}",
        f"  Max Drawdown   : {s['max_drawdown']:.2f}%",
        f"  Avg Win        : ${s['avg_win']:.2f}",
        f"  Avg Loss       : ${s['avg_loss']:.2f}",
        f"  Expectancy     : ${s['expectancy']:.2f}",
        f"  Longs / Shorts : {s['longs']} / {s['shorts']}",
        "-" * 60,
        f"  RECOMMENDATION : [{tick}] {verdict}",
        f"  (Pass: PF >= 1.5 and WR >= 42%)",
        "=" * 60,
        "",
        "TOP 50 COINS BY NET PNL",
        "-" * 60,
    ]
    top50 = sorted(s["per_coin"].items(), key=lambda x: x[1]["pnl"], reverse=True)[:50]
    lines.append(f"  {'Symbol':<20} {'Trades':>7} {'WR%':>7} {'PnL':>12}")
    lines.append(f"  {'-'*20} {'-'*7} {'-'*7} {'-'*12}")
    for sym, v in top50:
        lines.append(f"  {sym:<20} {v['n']:>7} {v['wr']:>6.1f}% ${v['pnl']:>10,.2f}")

    lines += [
        "",
        "MONTHLY PNL",
        "-" * 60,
        f"  {'Month':<10} {'Trades':>7} {'WR%':>7} {'PnL':>12}",
        f"  {'-'*10} {'-'*7} {'-'*7} {'-'*12}",
    ]
    for month in sorted(s["monthly"].keys()):
        mv  = s["monthly"][month]
        mwr = mv["w"] / mv["n"] * 100 if mv["n"] > 0 else 0
        lines.append(f"  {month:<10} {mv['n']:>7} {mwr:>6.1f}% ${mv['pnl']:>10,.2f}")

    lines.append("=" * 60)
    return "\n".join(lines)

# ── Merge ─────────────────────────────────────────────────────────────────────
def merge_shards():
    trades    = {vk: [] for vk in VARIANTS}
    with_data = {vk: set() for vk in VARIANTS}
    all_syms  = []

    for i in range(NUM_SHARDS):
        path = f"shard_{i}.json"
        if not os.path.exists(path):
            print(f"WARNING: {path} missing - skipping")
            continue
        with open(path) as f:
            d = json.load(f)
        all_syms.extend(d["symbols"])
        for vk in VARIANTS:
            vd = d["variants"].get(vk, {})
            trades[vk].extend(vd.get("trades", []))
            for sym in vd.get("with_data", []):
                with_data[vk].add(sym)

    output_files = []
    stats_cache  = {}

    for vk, vconf in VARIANTS.items():
        tf    = vconf["timeframe"]
        mb    = vconf["max_bars"]
        s     = stats(trades[vk])
        stats_cache[vk] = s
        label = vk.upper()
        wd    = list(with_data[vk])

        report = {
            "strategy":          f"Sweep & Engulf ({label})",
            "timeframe":         tf,
            "period":            f"{START_YM[0]}-{START_YM[1]:02d} -> {END_YM[0]}-{END_YM[1]:02d}",
            "symbols_total":     len(set(all_syms)),
            "symbols_with_data": len(wd),
            "config": {
                "capital": CAPITAL, "risk_pct": RISK_PCT,
                "leverage": LEVERAGE, "fee": FEE, "slip": SLIP,
                "ema_period": EMA_PERIOD, "atr_period": ATR_PERIOD,
                "atr_tp_mult": ATR_TP_MULT, "atr_sl_mult": ATR_SL_MULT,
                "rr_ratio": ATR_TP_MULT / ATR_SL_MULT, "max_bars": mb,
                "use_ema_filter": USE_EMA_FILTER,
                "prev_candle_filter": PREV_CANDLE_FILTER,
            },
            "stats": s,
        }

        rf = f"backtest_{vk}_report.json"
        sf = f"backtest_{vk}_summary.txt"

        with open(rf, "w") as f:
            json.dump(report, f, indent=2)

        summary = build_summary(s, label, tf, mb, len(set(all_syms)), len(wd))
        with open(sf, "w") as f:
            f.write(summary)

        print(summary)
        output_files += [rf, sf]

    # Side-by-side comparison
    print("\n" + "=" * 60)
    print("  VARIANT COMPARISON: 1H vs 4H")
    print("=" * 60)
    print(f"  {'Metric':<22} {'1H':>14} {'4H':>14}")
    print(f"  {'-'*22} {'-'*14} {'-'*14}")
    rows = [
        ("Total Trades",  "total",         "d",    ""),
        ("Win Rate",      "win_rate",       ".2f",  "%"),
        ("Profit Factor", "profit_factor",  ".4f",  ""),
        ("Net PnL",       "net_pnl",        ",.2f", "$"),
        ("Max Drawdown",  "max_drawdown",   ".2f",  "%"),
        ("Avg Win",       "avg_win",        ".2f",  "$"),
        ("Avg Loss",      "avg_loss",       ".2f",  "$"),
        ("Expectancy",    "expectancy",     ".2f",  "$"),
    ]
    s1, s4 = stats_cache["1h"], stats_cache["4h"]
    for lbl, key, fmt, pfx in rows:
        v1 = s1[key]; v4 = s4[key]
        f1 = f"{pfx}{v1:{fmt}}" if pfx == "$" else f"{v1:{fmt}}{pfx}"
        f4 = f"{pfx}{v4:{fmt}}" if pfx == "$" else f"{v4:{fmt}}{pfx}"
        print(f"  {lbl:<22} {f1:>14} {f4:>14}")
    print("=" * 60)
    print("\nFiles written:")
    for fn in output_files:
        print(f"  {fn}")

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))

