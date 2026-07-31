"""
Binance USDT-M Futures Strategy Backtest — v8.4
Stdlib only (Python 3.11). Runs on GitHub Actions.

WHAT'S NEW IN v8.4 vs v8.3:
1. Symbol list is now pulled LIVE from the Binance Vision static bucket
   (data.binance.vision) at run start, instead of a hardcoded SYMBOLS list.
   This covers every USDT-M perpetual with kline history, not just the ~130
   that were manually listed before. Falls back to a hardcoded backup list
   if the bucket listing call fails for any reason (rate limit, schema
   change, etc) so a bad run never means zero coins.
   NOTE: the fapi.binance.com REST API (incl. /fapi/v1/exchangeInfo) is
   451-blocked on GH Actions runners — that's why this uses the Vision
   *static* bucket listing (different domain, just serves zip files) rather
   than exchangeInfo. This is the first live run of that discovery code —
   watch the "[symbol discovery]" log line in Actions output; if it falls
   back, that's a signal to debug the bucket-listing endpoint, not silently
   trust the backup list.

2. Variants: G and H are unchanged (still the SL 15% base). Added:
     J1  = TP 5% / SL 15%   (ratio 1:3   -> breakeven WR ~75.0%)
     J2  = TP 3% / SL 10%   (ratio 1:3.3 -> breakeven WR ~76.9%)
     J3  = TP 5% / SL 10%   (ratio 1:2   -> breakeven WR ~66.7%)
   plus the same three with a confirmation filter stack (like variant I
   had) to see if better-quality entries clear the now-lower breakeven bar
   by a wider margin instead of sitting right on it like G/H did in v8.3:
     J1F, J2F, J3F = same TP/SL as J1/J2/J3 + filters:
       - ADX(14) >= 27 (raised from base 22)
       - MACD(12,26,9) histogram agrees with trade direction
       - Volume > 1.5x its 20-bar average
       - 50EMA slope over 40 bars (~10h) agrees with trade direction

WHY THIS MATTERS (v8.3 finding): position size = risk / SL%, so TP:SL ratio
directly sets the breakeven win rate needed just to cover fees/slippage.
G (1:5 ratio) needs 83.3% WR to break even and v8.3 actually landed at
83.35% WR — dead on the line, so fees alone made it PF 0.946. J1/J2/J3 cut
that required WR to 66-77%, which should leave real room above breakeven
instead of balancing on it.
"""

import csv
import io
import json
import math
import re
import statistics
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CAPITAL = 10_000.0
RISK_PCT = 0.0075
FEE = 0.0005
SLIP = 0.0002
MAX_POSITIONS = 5
ADX_MIN = 22
RSI_LONG_MIN = 45
RSI_SHORT_MAX = 55
COOLDOWN_BARS = 4
WARMUP_BARS = 100
INTERVAL = "15m"

START_YM = (2024, 7)
END_YM = (2026, 6)

VARIANTS = {
    "G":   {"tp": 0.03, "sl": 0.15, "filters": False},
    "H":   {"tp": 0.04, "sl": 0.15, "filters": False},
    "J1":  {"tp": 0.05, "sl": 0.15, "filters": False},
    "J2":  {"tp": 0.03, "sl": 0.10, "filters": False},
    "J3":  {"tp": 0.05, "sl": 0.10, "filters": False},
    "J1F": {"tp": 0.05, "sl": 0.15, "filters": True},
    "J2F": {"tp": 0.03, "sl": 0.10, "filters": True},
    "J3F": {"tp": 0.05, "sl": 0.10, "filters": True},
}

FILTER_ADX_MIN = 27
FILTER_VOL_MULT = 1.5
FILTER_HTF_BARS = 40  # ~10h at 15m candles

VISION_LIST_URL = "https://data.binance.vision/?delimiter=/&prefix=data/futures/um/monthly/klines/"
VISION_ZIP_URL = "https://data.binance.vision/data/futures/um/monthly/klines/{sym}/{interval}/{sym}-{interval}-{yyyy}-{mm}.zip"

# Backup list used only if the live bucket listing fails outright.
FALLBACK_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", "ADAUSDT",
    "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT", "INJUSDT",
    "TRXUSDT", "BNBUSDT", "1000PEPEUSDT", "1000BONKUSDT", "1000SHIBUSDT",
    "WIFUSDT", "1000FLOKIUSDT", "SEIUSDT", "TIAUSDT", "STRKUSDT", "ALGOUSDT",
    "SNXUSDT", "FETUSDT", "MANAUSDT", "IMXUSDT", "XLMUSDT", "WLDUSDT",
]


# ---------------------------------------------------------------------------
# SYMBOL DISCOVERY
# ---------------------------------------------------------------------------

def fetch_all_futures_symbols():
    """Pull the live USDT-M perpetual symbol list from the Binance Vision
    static bucket listing (NOT the fapi REST API — that's 451-blocked on
    GH Actions runners). Falls back to FALLBACK_SYMBOLS on any failure.
    Excludes dated/quarterly contracts (they contain an underscore, e.g.
    BTCUSDT_240329) and non-USDT-margined pairs.
    """
    try:
        req = urllib.request.Request(
            VISION_LIST_URL, headers={"User-Agent": "backtest-v8.4"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml = resp.read().decode("utf-8", errors="ignore")

        prefixes = re.findall(r"<Prefix>(.*?)</Prefix>", xml)
        symbols = set()
        for p in prefixes:
            sym = p.rsplit("/", 2)[-2] if p.endswith("/") else p.split("/")[-1]
            sym = sym.strip("/")
            if sym.endswith("USDT") and "_" not in sym and sym.isupper():
                symbols.add(sym)

        if not symbols:
            print("[symbol discovery] bucket listing returned 0 symbols, falling back")
            return sorted(FALLBACK_SYMBOLS)

        print(f"[symbol discovery] found {len(symbols)} live USDT-M symbols from Vision bucket")
        return sorted(symbols)

    except Exception as e:
        print(f"[symbol discovery] failed ({e}), falling back to hardcoded list")
        return sorted(FALLBACK_SYMBOLS)


# ---------------------------------------------------------------------------
# DATA FETCH
# ---------------------------------------------------------------------------

def month_range(start_ym, end_ym):
    y, m = start_ym
    ey, em = end_ym
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def fetch_symbol_data(symbol):
    """Download + parse all monthly klines for a symbol. Returns list of
    bar dicts sorted by open_time, or None if no data at all."""
    bars = []
    for (y, m) in month_range(START_YM, END_YM):
        url = VISION_ZIP_URL.format(sym=symbol, interval=INTERVAL, yyyy=y, mm=f"{m:02d}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "backtest-v8.4"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue  # symbol didn't exist / no data that month
            print(f"[{symbol}] HTTP {e.code} for {y}-{m:02d}")
            continue
        except Exception as e:
            print(f"[{symbol}] fetch failed {y}-{m:02d}: {e}")
            continue

        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as f:
                    text = io.TextIOWrapper(f, encoding="utf-8")
                    reader = csv.reader(text)
                    for row in reader:
                        if not row or row[0] in ("open_time",):
                            continue
                        try:
                            bars.append({
                                "t": int(row[0]),
                                "o": float(row[1]),
                                "h": float(row[2]),
                                "l": float(row[3]),
                                "c": float(row[4]),
                                "v": float(row[5]),
                            })
                        except (ValueError, IndexError):
                            continue
        except zipfile.BadZipFile:
            print(f"[{symbol}] bad zip for {y}-{m:02d}")
            continue

    if not bars:
        return None
    bars.sort(key=lambda b: b["t"])
    return bars


# ---------------------------------------------------------------------------
# INDICATORS (pure python, stdlib only)
# ---------------------------------------------------------------------------

def ema_series(values, period):
    k = 2.0 / (period + 1)
    out = [None] * len(values)
    ema = None
    for i, v in enumerate(values):
        if ema is None:
            if i + 1 == period:
                ema = sum(values[:period]) / period
                out[i] = ema
            continue
        ema = v * k + ema * (1 - k)
        out[i] = ema
    return out


def rsi_series(closes, period=14):
    out = [None] * len(closes)
    if len(closes) <= period:
        return out
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss else float("inf")
    out[period] = 100 - 100 / (1 + rs) if avg_loss else 100.0
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0)
        loss = max(-d, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss else float("inf")
        out[i] = 100 - 100 / (1 + rs) if avg_loss else 100.0
    return out


def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    if n <= period * 2:
        return out
    tr, plus_dm, minus_dm = [0.0] * n, [0.0] * n, [0.0] * n
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        tr[i] = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )

    atr = sum(tr[1:period + 1]) / period
    p_dm = sum(plus_dm[1:period + 1]) / period
    m_dm = sum(minus_dm[1:period + 1]) / period
    dx_list = [None] * n

    def _dx(p_di, m_di):
        s = p_di + m_di
        return 100 * abs(p_di - m_di) / s if s else 0.0

    p_di = 100 * p_dm / atr if atr else 0.0
    m_di = 100 * m_dm / atr if atr else 0.0
    dx_list[period] = _dx(p_di, m_di)

    for i in range(period + 1, n):
        atr = (atr * (period - 1) + tr[i]) / period
        p_dm = (p_dm * (period - 1) + plus_dm[i]) / period
        m_dm = (m_dm * (period - 1) + minus_dm[i]) / period
        p_di = 100 * p_dm / atr if atr else 0.0
        m_di = 100 * m_dm / atr if atr else 0.0
        dx_list[i] = _dx(p_di, m_di)

    idx0 = period * 2
    if idx0 < n and all(dx_list[period:idx0 + 1]):
        adx = sum(x for x in dx_list[period:idx0 + 1] if x is not None) / (idx0 - period + 1)
        out[idx0] = adx
        for i in range(idx0 + 1, n):
            if dx_list[i] is None:
                continue
            adx = (adx * (period - 1) + dx_list[i]) / period
            out[i] = adx
    return out


def macd_hist_series(closes, fast=12, slow=26, signal=9):
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    macd_line = [
        (a - b) if (a is not None and b is not None) else None
        for a, b in zip(ema_fast, ema_slow)
    ]
    valid = [x for x in macd_line if x is not None]
    if len(valid) < signal:
        return [None] * len(closes)
    sig_full = [None] * len(macd_line)
    start = next(i for i, x in enumerate(macd_line) if x is not None)
    sig_vals = ema_series(macd_line[start:], signal)
    for i, v in enumerate(sig_vals):
        sig_full[start + i] = v
    hist = [
        (m - s) if (m is not None and s is not None) else None
        for m, s in zip(macd_line, sig_full)
    ]
    return hist


# ---------------------------------------------------------------------------
# SIGNAL LOGIC
# ---------------------------------------------------------------------------

def compute_signals(bars, use_filters):
    """Returns list of 'long' / 'short' / None per bar index."""
    closes = [b["c"] for b in bars]
    highs = [b["h"] for b in bars]
    lows = [b["l"] for b in bars]
    vols = [b["v"] for b in bars]
    n = len(bars)

    ema9 = ema_series(closes, 9)
    ema21 = ema_series(closes, 21)
    ema50 = ema_series(closes, 50)
    rsi = rsi_series(closes, 14)
    adx = adx_series(highs, lows, closes, 14)
    macd_hist = macd_hist_series(closes) if use_filters else [None] * n

    vol_avg20 = [None] * n
    if use_filters:
        for i in range(19, n):
            vol_avg20[i] = sum(vols[i - 19:i + 1]) / 20

    signals = [None] * n
    min_adx = FILTER_ADX_MIN if use_filters else ADX_MIN

    for i in range(WARMUP_BARS, n):
        if None in (ema9[i], ema21[i], ema50[i], ema9[i - 1], ema21[i - 1], rsi[i], adx[i]):
            continue
        if ema50[i - 10] is None:
            continue

        slope50 = (ema50[i] - ema50[i - 10]) / ema50[i - 10] * 100 if ema50[i - 10] else 0
        cross_up = ema9[i - 1] <= ema21[i - 1] and ema9[i] > ema21[i]
        cross_down = ema9[i - 1] >= ema21[i - 1] and ema9[i] < ema21[i]

        if adx[i] < min_adx:
            continue

        side = None
        if slope50 > 0.05 and cross_up and rsi[i] >= RSI_LONG_MIN:
            side = "long"
        elif slope50 < -0.05 and cross_down and rsi[i] <= RSI_SHORT_MAX:
            side = "short"

        if side is None:
            continue

        if use_filters:
            if macd_hist[i] is None:
                continue
            if side == "long" and macd_hist[i] <= 0:
                continue
            if side == "short" and macd_hist[i] >= 0:
                continue

            if vol_avg20[i] is None or vols[i] <= vol_avg20[i] * FILTER_VOL_MULT:
                continue

            j = i - FILTER_HTF_BARS
            if j < 0 or ema50[j] is None or ema50[j] == 0:
                continue
            htf_slope = (ema50[i] - ema50[j]) / ema50[j] * 100
            if side == "long" and htf_slope <= 0:
                continue
            if side == "short" and htf_slope >= 0:
                continue

        signals[i] = side

    return signals


# ---------------------------------------------------------------------------
# SINGLE-COIN, SINGLE-VARIANT BACKTEST
# ---------------------------------------------------------------------------

def backtest_coin_variant(symbol, bars, variant_name, variant_cfg):
    signals = compute_signals(bars, variant_cfg["filters"])
    tp_pct, sl_pct = variant_cfg["tp"], variant_cfg["sl"]

    trades = []
    cooldown_until = -1
    i = 0
    n = len(bars)

    while i < n:
        if i <= cooldown_until or signals[i] is None:
            i += 1
            continue

        side = signals[i]
        entry_bar = bars[i]
        entry_price = entry_bar["c"]
        entry_time = entry_bar["t"]

        if side == "long":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        exit_price = None
        exit_time = None
        exit_reason = None
        j = i + 1
        while j < n:
            hi, lo = bars[j]["h"], bars[j]["l"]
            if side == "long":
                hit_sl = lo <= sl_price
                hit_tp = hi >= tp_price
            else:
                hit_sl = hi >= sl_price
                hit_tp = lo <= tp_price

            if hit_sl and hit_tp:
                # ambiguous same-bar hit; assume SL first (conservative)
                exit_price, exit_reason = sl_price, "SL"
            elif hit_sl:
                exit_price, exit_reason = sl_price, "SL"
            elif hit_tp:
                exit_price, exit_reason = tp_price, "TP"

            if exit_price is not None:
                exit_time = bars[j]["t"]
                break
            j += 1

        if exit_price is None:
            break  # ran off the end of data with position open; drop it

        risk_amt = CAPITAL * RISK_PCT
        pos_size = risk_amt / sl_pct  # notional
        fee_cost = pos_size * FEE * 2
        slip_cost = pos_size * SLIP * 2

        if side == "long":
            raw_pnl = pos_size * (exit_price - entry_price) / entry_price
        else:
            raw_pnl = pos_size * (entry_price - exit_price) / entry_price

        net_pnl = raw_pnl - fee_cost - slip_cost

        trades.append({
            "symbol": symbol, "variant": variant_name, "side": side,
            "entry_time": entry_time, "exit_time": exit_time,
            "entry_price": entry_price, "exit_price": exit_price,
            "reason": exit_reason, "pnl": net_pnl,
            "bars_held": j - i,
        })

        cooldown_until = j + COOLDOWN_BARS
        i = j + 1

    return trades


# ---------------------------------------------------------------------------
# WORKER (one process per symbol, runs ALL variants on that symbol's data)
# ---------------------------------------------------------------------------

def worker_task(symbol):
    bars = fetch_symbol_data(symbol)
    if not bars or len(bars) < WARMUP_BARS + 60:
        return symbol, None, "insufficient ({} bars)".format(len(bars) if bars else 0)

    result = {}
    for vname, vcfg in VARIANTS.items():
        result[vname] = backtest_coin_variant(symbol, bars, vname, vcfg)
    return symbol, result, None


# ---------------------------------------------------------------------------
# PORTFOLIO CAP
# ---------------------------------------------------------------------------

def apply_portfolio_cap(all_trades):
    """all_trades: list of trade dicts (single variant, all symbols).
    Sorts by entry time, drops trades that would exceed MAX_POSITIONS
    concurrent slots. Returns the accepted trade list."""
    events = sorted(all_trades, key=lambda t: t["entry_time"])
    open_positions = []  # list of exit_time for currently open slots
    accepted = []
    for t in events:
        open_positions = [e for e in open_positions if e > t["entry_time"]]
        if len(open_positions) < MAX_POSITIONS:
            open_positions.append(t["exit_time"])
            accepted.append(t)
    return accepted


# ---------------------------------------------------------------------------
# STATS
# ---------------------------------------------------------------------------

def calc_stats(trades):
    if not trades:
        return None
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss = -sum(t["pnl"] for t in losses)
    net_pnl = gross_profit - gross_loss
    pf = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
    wr = len(wins) / len(trades) * 100

    equity = CAPITAL
    peak = CAPITAL
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_time"]):
        equity += t["pnl"]
        peak = max(peak, equity)
        dd = (peak - equity) / peak * 100 if peak else 0
        max_dd = max(max_dd, dd)

    pnls = [t["pnl"] for t in trades]
    sharpe = (statistics.mean(pnls) / statistics.pstdev(pnls) * math.sqrt(252)
              if len(pnls) > 1 and statistics.pstdev(pnls) > 0 else 0.0)

    return {
        "trades": len(trades),
        "wr": wr,
        "pf": pf,
        "net_pnl": net_pnl,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "expectancy": net_pnl / len(trades),
        "avg_bars": statistics.mean(t["bars_held"] for t in trades),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "long_trades": [t for t in trades if t["side"] == "long"],
        "short_trades": [t for t in trades if t["side"] == "short"],
    }


def per_coin_stats(trades):
    by_symbol = {}
    for t in trades:
        by_symbol.setdefault(t["symbol"], []).append(t)
    out = {}
    for sym, tl in by_symbol.items():
        s = calc_stats(tl)
        if s:
            out[sym] = s
    return out


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()
    symbols = fetch_all_futures_symbols()
    print(f"Testing {len(symbols)} symbols across {len(VARIANTS)} variants...")

    per_symbol_results = {}
    issues = []

    with ProcessPoolExecutor(max_workers=60) as ex:
        futures = {ex.submit(worker_task, s): s for s in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                symbol, result, issue = fut.result()
            except Exception as e:
                issues.append(f"{sym}: worker crashed ({e})")
                continue
            if issue:
                issues.append(f"{symbol}: {issue}")
                continue
            per_symbol_results[symbol] = result

    variant_reports = {}
    for vname in VARIANTS:
        all_trades = []
        for sym, res in per_symbol_results.items():
            all_trades.extend(res[vname])
        capped_trades = apply_portfolio_cap(all_trades)
        stats = calc_stats(capped_trades)
        coin_stats = per_coin_stats(capped_trades)
        variant_reports[vname] = {"stats": stats, "coins": coin_stats}

    write_outputs(variant_reports, issues, symbols)
    print(f"Done in {time.time() - t0:.1f}s")


def days_in_range():
    from datetime import date as _d
    d0 = _d(START_YM[0], START_YM[1], 1)
    y, m = END_YM
    d1 = _d(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1)
    return (d1 - d0).days


def write_outputs(variant_reports, issues, symbols):
    days = days_in_range()
    lines = []

    def fmt_pf(pf):
        return "inf" if pf == float("inf") else f"{pf:.3f}"

    for vname, cfg in VARIANTS.items():
        rep = variant_reports[vname]
        stats = rep["stats"]
        lines.append("=" * 65)
        tag = " + confirmation filters" if cfg["filters"] else ""
        lines.append(f"  VARIANT {vname} — TP {cfg['tp']*100:.0f}% / SL {cfg['sl']*100:.0f}%{tag}")
        lines.append("=" * 65)
        if not stats:
            lines.append("  NO TRADES")
            lines.append("")
            continue

        lines.append(f"  Trades       : {stats['trades']}")
        lines.append(f"  Win Rate     : {stats['wr']:.2f}%")
        lines.append(f"  Profit Factor: {fmt_pf(stats['pf'])}")
        lines.append(f"  Net PnL      : ${stats['net_pnl']:.1f}")
        lines.append(f"  Max Drawdown : {stats['max_dd']:.2f}%")
        lines.append(f"  Sharpe       : {stats['sharpe']:.3f}")
        lines.append(f"  Expectancy   : ${stats['expectancy']:.2f}/trade")
        lines.append(f"  Avg Bars Held: {stats['avg_bars']:.1f}")
        lt, st = stats["long_trades"], stats["short_trades"]
        lwr = (sum(1 for t in lt if t["pnl"] > 0) / len(lt) * 100) if lt else 0
        swr = (sum(1 for t in st if t["pnl"] > 0) / len(st) * 100) if st else 0
        lines.append(f"  Long  : {len(lt)} trades | WR {lwr:.2f}%")
        lines.append(f"  Short : {len(st)} trades | WR {swr:.2f}%")
        lines.append(f"  Gross Profit : ${stats['gross_profit']:.1f} | Loss: ${stats['gross_loss']:.1f}")
        lines.append("")

        coins = rep["coins"]
        ranked = sorted(coins.items(), key=lambda kv: (kv[1]["pf"] if kv[1]["pf"] != float("inf") else 9e9), reverse=True)
        lines.append("  -- Top 25 by Profit Factor --")
        for sym, s in ranked[:25]:
            lines.append(f"  {sym:<22} {s['trades']:>5}  {s['wr']:>6.1f}%  {fmt_pf(s['pf']):>7}  ${s['net_pnl']:>9.2f}")
        lines.append("")

        passing = {
            sym: s for sym, s in coins.items()
            if (s["pf"] == float("inf") or s["pf"] >= 1.5) and s["wr"] >= 42 and s["trades"] >= 8
        }
        lines.append(f"  -- Passing Coins (PF>=1.5 WR>=42% >=8 trades): {len(passing)} --")
        for sym, s in sorted(passing.items(), key=lambda kv: (kv[1]["pf"] if kv[1]["pf"] != float("inf") else 9e9), reverse=True):
            lines.append(f"  OK {sym:<22} PF={fmt_pf(s['pf'])} WR={s['wr']:.1f}% T={s['trades']}")
        tpd = stats["trades"] / days if days else 0
        lines.append(f"\n  Trades/day (portfolio): ~{tpd:.1f}")
        lines.append("")

    lines.append("=" * 65)
    lines.append("  CROSS-VARIANT SUMMARY")
    lines.append("=" * 65)
    lines.append(f"  {'Var':<5}{'TP':>5}{'SL':>6}{'Trades':>9}{'T/day':>8}{'WR%':>8}{'PF':>9}{'NetPnL':>13}{'MaxDD':>8}  Verdict")
    for vname, cfg in VARIANTS.items():
        stats = variant_reports[vname]["stats"]
        if not stats:
            continue
        tpd = stats["trades"] / days if days else 0
        verdict = "LIVE-CANDIDATE" if (stats["pf"] >= 1.5 and stats["wr"] >= 80 and stats["max_dd"] < 20) else (
            "PROFITABLE" if stats["pf"] > 1.0 else "NOT YET")
        lines.append(
            f"  {vname:<5}{cfg['tp']*100:>4.0f}%{cfg['sl']*100:>5.0f}%{stats['trades']:>9}{tpd:>8.1f}"
            f"{stats['wr']:>7.1f}%{fmt_pf(stats['pf']):>9}  ${stats['net_pnl']:>10.2f}{stats['max_dd']:>7.1f}%  {verdict}"
        )
    lines.append("")

    lines.append("-- Symbol Issues ({}) --".format(len(issues)))
    for iss in issues:
        lines.append(f"  {iss}")

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    report = {
        "meta": {
            "version": "v8.4",
            "period": f"{START_YM[0]}-{START_YM[1]:02d} -> {END_YM[0]}-{END_YM[1]:02d}",
            "symbols_tested": len(symbols),
            "variants": {k: {"tp": v["tp"], "sl": v["sl"], "filters": v["filters"]} for k, v in VARIANTS.items()},
            "settings": {
                "capital": CAPITAL, "risk_pct": RISK_PCT, "fee": FEE, "slip": SLIP,
                "adx_min": ADX_MIN, "filter_adx_min": FILTER_ADX_MIN,
                "max_positions": MAX_POSITIONS,
                "rsi_long_min": RSI_LONG_MIN, "rsi_short_max": RSI_SHORT_MAX,
            },
        },
        "variants": {
            vname: {
                "stats": {k: v for k, v in rep["stats"].items() if k not in ("long_trades", "short_trades")} if rep["stats"] else None,
                "coins": rep["coins"],
            }
            for vname, rep in variant_reports.items()
        },
        "symbol_issues": issues,
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, default=str)


if __name__ == "__main__":
    main()
