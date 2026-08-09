"""
Sweep & Engulf Backtest
=======================
Strategy:
  Bullish: current candle wicks BELOW prev low AND closes ABOVE prev high
  Bearish: current candle wicks ABOVE prev high AND closes BELOW prev low

Filters:
  - EMA trend filter (optional): bullish only above EMA, bearish only below EMA
  - Previous candle direction: Any / Same direction
  - ATR must be valid before signal fires (no stuck trades from bar 1)

Config:
  Timeframe  : 15m
  Capital    : $10,000
  Risk/trade : 0.75%
  TP         : 3.0× ATR from entry
  SL         : 1.5× ATR from entry  (RR = 2.0)
  Leverage   : 5×
  Fee        : 0.05%
  Slippage   : 0.02%
  EMA period : 200
  ATR period : 14
  Max hold   : 200 bars
  Min warmup : 220 bars (covers EMA200 + ATR14)

Author: Paqu / Engineered By Paqu
"""

import csv
import io
import json
import math
import os
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen
from urllib.error import HTTPError

# ── Coin list ────────────────────────────────────────────────────────────────
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
    "ARBUSDT","SUIUSDT","PEPEUSDT","FLOKIUSDT","WLDUSDT","CYBERUSDT","SEIUSDT",
    "TIAUSDT","ORDIUSDT","BEAMXUSDT","PIXELUSDT","PORTALUSDT","PDAUSDT",
    "AXLUSDT","STRKUSDT","ALTUSDT","JUPUSDT","DYMUSDT","PYTHUSDT","JSTOUSDT",
]

# De-duplicate preserving order
seen = set()
DEDUPED = []
for s in ALL_SYMBOLS:
    if s not in seen:
        seen.add(s)
        DEDUPED.append(s)
ALL_SYMBOLS = DEDUPED[:117]  # cap at 117

# ── Config ───────────────────────────────────────────────────────────────────
NUM_SHARDS  = 8
WORKERS     = 16

START_YM    = (2023, 1)
END_YM      = (2025, 7)
TIMEFRAME   = "15m"

CAPITAL     = 10_000.0
RISK_PCT    = 0.0075     # 0.75%
FEE         = 0.0005     # 0.05%
SLIP        = 0.0002     # 0.02%
LEVERAGE    = 5

EMA_PERIOD  = 200
ATR_PERIOD  = 14
ATR_TP_MULT = 3.0        # TP = 3× ATR
ATR_SL_MULT = 1.5        # SL = 1.5× ATR  → RR ≈ 2.0

MAX_BARS    = 200
MIN_BARS    = EMA_PERIOD + 20   # 220 — ensures EMA + ATR fully warmed

# Strategy filters (edit here to toggle):
USE_EMA_FILTER     = True        # True  = trend filter on
PREV_CANDLE_FILTER = "any"       # "any" | "same"

# ── Data fetch ───────────────────────────────────────────────────────────────
BASE_URL = (
    "https://data.binance.vision/data/futures/um/monthly/klines"
    "/{sym}/{tf}/{sym}-{tf}-{yyyy}-{mm:02d}.zip"
)

def fetch_month(symbol, year, month):
    url = BASE_URL.format(sym=symbol, tf=TIMEFRAME, yyyy=year, mm=month)
    try:
        with urlopen(url, timeout=30) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
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

def fetch_symbol(symbol):
    sy, sm = START_YM
    ey, em = END_YM
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    all_candles = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_month, symbol, yr, mo): (yr, mo) for yr, mo in months}
        for f in as_completed(futs):
            all_candles.extend(f.result())

    # Deduplicate + sort
    seen_ts = {}
    for c in all_candles:
        seen_ts[c[0]] = c
    return sorted(seen_ts.values(), key=lambda x: x[0])

# ── Indicators ───────────────────────────────────────────────────────────────
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
    # First ATR = simple average
    if len(trs) < period:
        return atr
    first_idx = period  # index in original array
    atr[first_idx] = sum(trs[:period]) / period
    for i in range(first_idx + 1, n):
        atr[i] = (atr[i - 1] * (period - 1) + trs[i - 1]) / period
    return atr

# ── Signal ───────────────────────────────────────────────────────────────────
def signal(i, opens, highs, lows, closes, ema_vals, atr_vals):
    """
    Returns 'buy', 'sell', or None.
    Evaluated on fully closed bar i.
    Entry on bar i+1 open.
    """
    if i < 1:
        return None

    # ATR guard — must be valid
    if atr_vals[i] is None:
        return None

    # EMA guard — must be valid when filter is on
    if USE_EMA_FILTER and ema_vals[i] is None:
        return None

    cur_o, cur_h, cur_l, cur_c = opens[i], highs[i], lows[i], closes[i]
    prv_o, prv_h, prv_l, prv_c = opens[i-1], highs[i-1], lows[i-1], closes[i-1]

    # Previous candle direction
    prv_bull = prv_c > prv_o
    prv_bear = prv_c < prv_o

    # Trend
    if USE_EMA_FILTER:
        trend_bull = cur_c > ema_vals[i]
        trend_bear = cur_c < ema_vals[i]
    else:
        trend_bull = True
        trend_bear = True

    # ── Bullish sweep-engulf ──
    # Wicks below prev low AND closes above prev high
    bullish = (cur_l < prv_l) and (cur_c > prv_h)
    if bullish:
        if PREV_CANDLE_FILTER == "same" and not prv_bull:
            bullish = False
        if not trend_bull:
            bullish = False

    # ── Bearish sweep-engulf ──
    # Wicks above prev high AND closes below prev low
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
def backtest(symbol, candles):
    if len(candles) < MIN_BARS + 2:
        return []

    ts_arr    = [c[0] for c in candles]
    opens     = [c[1] for c in candles]
    highs     = [c[2] for c in candles]
    lows      = [c[3] for c in candles]
    closes    = [c[4] for c in candles]

    ema_vals  = calc_ema(closes, EMA_PERIOD)
    atr_vals  = calc_atr(highs, lows, closes, ATR_PERIOD)

    trades    = []
    n         = len(candles)
    in_trade  = False
    side      = None
    entry_p   = 0.0
    sl_p      = 0.0
    tp_p      = 0.0
    entry_ts  = 0
    entry_bar = 0
    notional  = 0.0

    for i in range(MIN_BARS, n - 1):
        if in_trade:
            bar_h = highs[i]
            bar_l = lows[i]
            bars_held = i - entry_bar
            exit_p = None
            reason = None

            if side == "buy":
                # SL first (conservative)
                if bar_l <= sl_p:
                    exit_p = sl_p
                    reason = "sl"
                elif bar_h >= tp_p:
                    exit_p = tp_p
                    reason = "tp"
            else:  # sell
                if bar_h >= sl_p:
                    exit_p = sl_p
                    reason = "sl"
                elif bar_l <= tp_p:
                    exit_p = tp_p
                    reason = "tp"

            if reason is None and bars_held >= MAX_BARS:
                exit_p = closes[i]
                reason = "max_hold"

            if exit_p is not None:
                if side == "buy":
                    gross = (exit_p - entry_p) / entry_p
                else:
                    gross = (entry_p - exit_p) / entry_p
                net_move = gross - (FEE + SLIP) * 2
                pnl = notional * net_move * LEVERAGE

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
            sig = signal(i, opens, highs, lows, closes, ema_vals, atr_vals)
            if sig is None:
                continue

            atr = atr_vals[i]
            if atr is None:
                continue

            # Entry on next bar open
            next_open = opens[i + 1]
            if sig == "buy":
                ep = next_open * (1 + FEE + SLIP)
                sl = ep - ATR_SL_MULT * atr
                tp = ep + ATR_TP_MULT * atr
            else:
                ep = next_open * (1 - FEE - SLIP)
                sl = ep + ATR_SL_MULT * atr
                tp = ep - ATR_TP_MULT * atr

            # Position sizing: risk-based, capped by leverage
            sl_dist_pct = abs(ep - sl) / ep
            if sl_dist_pct <= 0:
                continue
            risk_dollars = CAPITAL * RISK_PCT
            size = risk_dollars / sl_dist_pct          # notional exposure
            size = min(size, CAPITAL * LEVERAGE)

            in_trade  = True
            side      = sig
            entry_p   = ep
            sl_p      = sl
            tp_p      = tp
            entry_ts  = ts_arr[i + 1]
            entry_bar = i + 1
            notional  = size

    # Close open trade at end of data
    if in_trade:
        exit_p = closes[-1]
        bars_held = (n - 1) - entry_bar
        if side == "buy":
            gross = (exit_p - entry_p) / entry_p
        else:
            gross = (entry_p - exit_p) / entry_p
        net_move = gross - (FEE + SLIP) * 2
        pnl = notional * net_move * LEVERAGE
        trades.append({
            "symbol":      symbol,
            "side":        side,
            "entry_ts":    entry_ts,
            "exit_ts":     ts_arr[-1],
            "entry_price": entry_p,
            "exit_price":  exit_p,
            "pnl":         pnl,
            "reason":      "end_of_data",
            "bars":        bars_held,
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

    wins  = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    gross_win  = sum(wins)
    gross_loss = abs(sum(losses))

    net_pnl = sum(t["pnl"] for t in trades)
    wr = len(wins) / len(trades) * 100
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    avg_win  = gross_win  / len(wins)   if wins   else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    expectancy = (wr / 100 * avg_win) - ((1 - wr / 100) * avg_loss)

    longs  = sum(1 for t in trades if t["side"] == "buy")
    shorts = sum(1 for t in trades if t["side"] == "sell")

    # Max drawdown (equity curve)
    equity = 0.0
    peak   = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        equity += t["pnl"]
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    max_dd_pct = max_dd / CAPITAL * 100 if CAPITAL > 0 else 0.0

    # Monthly
    monthly = {}
    for t in trades:
        from datetime import datetime, timezone
        dt  = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        key = dt.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"pnl": 0.0, "n": 0, "w": 0}
        monthly[key]["pnl"] += t["pnl"]
        monthly[key]["n"]   += 1
        if t["pnl"] > 0:
            monthly[key]["w"] += 1

    # Per coin
    per_coin = {}
    for t in trades:
        sym = t["symbol"]
        if sym not in per_coin:
            per_coin[sym] = {"pnl": 0.0, "n": 0, "w": 0, "wr": 0.0}
        per_coin[sym]["pnl"] += t["pnl"]
        per_coin[sym]["n"]   += 1
        if t["pnl"] > 0:
            per_coin[sym]["w"] += 1
    for sym, v in per_coin.items():
        v["wr"] = v["w"] / v["n"] * 100 if v["n"] > 0 else 0.0

    return {
        "total":         len(trades),
        "win_rate":      round(wr, 2),
        "profit_factor": round(pf, 4),
        "net_pnl":       round(net_pnl, 2),
        "max_drawdown":  round(max_dd_pct, 2),
        "avg_win":       round(avg_win, 2),
        "avg_loss":      round(avg_loss, 2),
        "expectancy":    round(expectancy, 2),
        "longs":         longs,
        "shorts":        shorts,
        "monthly":       monthly,
        "per_coin":      per_coin,
    }

# ── Shard runner ─────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    import time
    start = time.time()

    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] Assigned {len(symbols)} symbols: {symbols[:5]}...")

    def process(sym):
        candles = fetch_symbol(sym)
        if len(candles) < MIN_BARS + 2:
            print(f"  {sym}: insufficient data ({len(candles)} bars)")
            return sym, []
        trades = backtest(sym, candles)
        print(f"  {sym}: {len(candles)} bars → {len(trades)} trades")
        return sym, trades

    with_data = []
    all_trades = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process, sym): sym for sym in symbols}
        for f in as_completed(futs):
            sym, trades = f.result()
            if trades or len(fetch_symbol(sym)) >= MIN_BARS + 2:
                with_data.append(sym)
            all_trades.extend(trades)

    shard_stats = stats(all_trades)
    elapsed = time.time() - start

    result = {
        "shard":     shard_idx,
        "symbols":   symbols,
        "with_data": with_data,
        "trades":    all_trades,
        "stats":     shard_stats,
        "elapsed":   round(elapsed, 1),
    }

    out_path = f"shard_{shard_idx}.json"
    with open(out_path, "w") as f:
        json.dump(result, f)
    print(f"[Shard {shard_idx}] Done in {elapsed:.1f}s — {len(all_trades)} trades → {out_path}")

# ── Merge ─────────────────────────────────────────────────────────────────────
def merge_shards():
    all_trades  = []
    all_symbols = []
    with_data   = []

    for i in range(NUM_SHARDS):
        path = f"shard_{i}.json"
        if not os.path.exists(path):
            print(f"WARNING: {path} missing — skipping")
            continue
        with open(path) as f:
            d = json.load(f)
        all_trades.extend(d["trades"])
        all_symbols.extend(d["symbols"])
        with_data.extend(d.get("with_data", []))

    combined = stats(all_trades)

    with open("backtest_report.json", "w") as f:
        json.dump({
            "strategy":      "Sweep & Engulf",
            "timeframe":     TIMEFRAME,
            "period":        f"{START_YM[0]}-{START_YM[1]:02d} → {END_YM[0]}-{END_YM[1]:02d}",
            "symbols_total": len(all_symbols),
            "symbols_with_data": len(with_data),
            "config": {
                "capital": CAPITAL, "risk_pct": RISK_PCT,
                "leverage": LEVERAGE, "fee": FEE, "slip": SLIP,
                "ema_period": EMA_PERIOD, "atr_period": ATR_PERIOD,
                "atr_tp_mult": ATR_TP_MULT, "atr_sl_mult": ATR_SL_MULT,
                "rr_ratio": ATR_TP_MULT / ATR_SL_MULT,
                "use_ema_filter": USE_EMA_FILTER,
                "prev_candle_filter": PREV_CANDLE_FILTER,
            },
            "stats": combined,
        }, f, indent=2)

    # ── Summary text ──
    s = combined
    usable = s["profit_factor"] >= 1.5 and s["win_rate"] >= 42.0
    verdict = "✅ USABLE" if usable else "❌ NOT USABLE"

    lines = [
        "=" * 60,
        "  SWEEP & ENGULF — BACKTEST SUMMARY",
        "=" * 60,
        f"  Period     : {START_YM[0]}-{START_YM[1]:02d} → {END_YM[0]}-{END_YM[1]:02d}",
        f"  Timeframe  : {TIMEFRAME}",
        f"  Symbols    : {len(all_symbols)} attempted / {len(with_data)} with data",
        f"  EMA Filter : {'ON (200 EMA)' if USE_EMA_FILTER else 'OFF'}",
        f"  Prev Candle: {PREV_CANDLE_FILTER.upper()}",
        f"  Capital    : ${CAPITAL:,.0f}  Leverage: {LEVERAGE}×",
        f"  Risk/Trade : {RISK_PCT*100:.2f}%  Fee: {FEE*100:.3f}%  Slip: {SLIP*100:.3f}%",
        f"  ATR SL     : {ATR_SL_MULT}×ATR   ATR TP: {ATR_TP_MULT}×ATR   RR: {ATR_TP_MULT/ATR_SL_MULT:.1f}",
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
        f"  RECOMMENDATION : {verdict}",
        f"  (Pass: PF ≥ 1.5 and WR ≥ 42%)",
        "=" * 60,
        "",
        "TOP 50 COINS BY NET PNL",
        "-" * 60,
    ]

    sorted_coins = sorted(
        s["per_coin"].items(),
        key=lambda x: x[1]["pnl"],
        reverse=True
    )[:50]

    lines.append(f"  {'Symbol':<20} {'Trades':>7} {'WR%':>7} {'PnL':>12}")
    lines.append(f"  {'-'*20} {'-'*7} {'-'*7} {'-'*12}")
    for sym, v in sorted_coins:
        lines.append(f"  {sym:<20} {v['n']:>7} {v['wr']:>6.1f}% ${v['pnl']:>10,.2f}")

    lines += [
        "",
        "MONTHLY PNL",
        "-" * 60,
        f"  {'Month':<10} {'Trades':>7} {'WR%':>7} {'PnL':>12}",
        f"  {'-'*10} {'-'*7} {'-'*7} {'-'*12}",
    ]
    for month in sorted(s["monthly"].keys()):
        mv = s["monthly"][month]
        mwr = mv["w"] / mv["n"] * 100 if mv["n"] > 0 else 0
        lines.append(f"  {month:<10} {mv['n']:>7} {mwr:>6.1f}% ${mv['pnl']:>10,.2f}")

    lines.append("=" * 60)
    summary_text = "\n".join(lines)

    with open("backtest_summary.txt", "w") as f:
        f.write(summary_text)

    print(summary_text)
    print("\n✅ backtest_report.json + backtest_summary.txt written.")

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

