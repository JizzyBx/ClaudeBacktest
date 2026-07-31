"""
Backtest v8.5 — Full Binance USDT-M Futures universe
- Symbols: auto-discovered from data.binance.vision S3 bucket (no fapi needed)
- Timeframe: 30m candles, 2-year history (2024-07 to 2026-06)
- Variants: G, H, J1, J2, J3, J1F, J2F, J3F (same as v8.4)
- Infrastructure: GitHub Actions, ProcessPoolExecutor, stdlib only
"""

import os, sys, io, csv, json, math, time, zipfile, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# ── CONFIG ──────────────────────────────────────────────────────────────────
INTERVAL        = "30m"
CAPITAL         = 10_000.0
RISK_PCT        = 0.0075
FEE             = 0.0005
SLIP            = 0.0002
MAX_POSITIONS   = 5
ADX_MIN         = 22
FILTER_ADX_MIN  = 27        # stricter ADX for F-variants
RSI_LONG_MIN    = 45
RSI_SHORT_MAX   = 55
COOLDOWN_BARS   = 4
WARMUP_BARS     = 100
MAX_WORKERS     = 60

# Date range: July 2024 → June 2026 (24 months)
MONTHS = []
for y in range(2024, 2027):
    for m in range(1, 13):
        if (y == 2024 and m < 7): continue
        if (y == 2026 and m > 6): break
        MONTHS.append(f"{y}-{m:02d}")

BASE_URL = "https://data.binance.vision"
S3_URL   = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

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

# ── SYMBOL DISCOVERY ────────────────────────────────────────────────────────
def discover_symbols():
    """Fetch all USDT perpetual futures symbols from data.binance.vision S3 listing."""
    prefix    = f"data/futures/um/monthly/klines/"
    delimiter = "/"
    url = f"{S3_URL}?prefix={prefix}&delimiter={delimiter}"
    symbols = []
    marker = ""
    while True:
        req_url = url + (f"&marker={marker}" if marker else "")
        try:
            with urllib.request.urlopen(req_url, timeout=30) as r:
                xml_data = r.read()
        except Exception as e:
            print(f"[WARN] S3 listing failed: {e}", flush=True)
            break

        root = ET.fromstring(xml_data)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        for cp in root.findall("s3:CommonPrefixes", ns):
            p = cp.find("s3:Prefix", ns).text  # e.g. data/futures/um/monthly/klines/BTCUSDT/
            sym = p.rstrip("/").split("/")[-1]
            if sym.endswith("USDT") and "_" not in sym:  # skip quarterly contracts like BTCUSDT_250627
                symbols.append(sym)

        is_truncated = root.find("s3:IsTruncated", ns)
        if is_truncated is not None and is_truncated.text.lower() == "true":
            # get next marker
            last = root.find("s3:NextMarker", ns)
            if last is not None:
                marker = last.text
            else:
                # fallback: use last CommonPrefixes
                all_cp = root.findall("s3:CommonPrefixes", ns)
                if all_cp:
                    marker = all_cp[-1].find("s3:Prefix", ns).text
                else:
                    break
        else:
            break

    # Known problem coins — skip upfront (HTTP 400 / delisted / bad data)
    BAD = {"1000FLOKIUSDT", "COCOUSDT", "MUBARAKUSDT", "TSTUSDT", "OMUSDT", "ACTUSDT"}
    symbols = [s for s in symbols if s not in BAD]

    # Fix known renames
    renamed = []
    for s in symbols:
        if s == "MATICUSDT":
            renamed.append("POLUSDT")
        else:
            renamed.append(s)
    return sorted(set(renamed))

# ── DATA FETCHING ────────────────────────────────────────────────────────────
def fetch_month(symbol, month, interval):
    """Download one monthly zip from data.binance.vision, return list of OHLCV rows."""
    sym_dl = symbol
    # POLUSDT was MATICUSDT on Binance before rename — try both
    candidates = [sym_dl]
    if sym_dl == "POLUSDT":
        candidates = ["POLUSDT", "MATICUSDT"]

    for sym in candidates:
        url = f"{BASE_URL}/data/futures/um/monthly/klines/{sym}/{interval}/{sym}-{interval}-{month}.zip"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = r.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as f:
                    rows = list(csv.reader(io.TextIOWrapper(f)))
            # strip header if present
            if rows and not rows[0][0].lstrip('-').isdigit():
                rows = rows[1:]
            return [[float(c) for c in r[:6]] for r in rows if len(r) >= 6]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            raise
        except Exception:
            continue
    return []

def fetch_symbol_data(symbol, interval, months):
    """Fetch all months for a symbol, return combined OHLCV list."""
    bars = []
    for month in months:
        bars.extend(fetch_month(symbol, month, interval))
    return bars  # [[open_time, open, high, low, close, volume], ...]

# ── INDICATORS ───────────────────────────────────────────────────────────────
def ema(vals, period):
    k = 2.0 / (period + 1)
    result = [None] * len(vals)
    for i, v in enumerate(vals):
        if i == 0:
            result[i] = v
        else:
            result[i] = v * k + result[i-1] * (1 - k)
    return result

def compute_adx(highs, lows, closes, period=14):
    n = len(closes)
    adx = [None] * n
    if n < period * 2 + 1:
        return adx
    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        pdm = max(highs[i] - highs[i-1], 0) if (highs[i] - highs[i-1]) > (lows[i-1] - lows[i]) else 0
        ndm = max(lows[i-1] - lows[i], 0) if (lows[i-1] - lows[i]) > (highs[i] - highs[i-1]) else 0
        tr_list.append(tr); pdm_list.append(pdm); ndm_list.append(ndm)

    def smooth(lst, p):
        s = [None] * len(lst)
        s[p-1] = sum(lst[:p])
        for i in range(p, len(lst)):
            s[i] = s[i-1] - s[i-1]/p + lst[i]
        return s

    atr = smooth(tr_list, period)
    pdi_s = smooth(pdm_list, period)
    ndi_s = smooth(ndm_list, period)
    dx_list = []
    for i in range(period-1, len(tr_list)):
        a = atr[i]; p = pdi_s[i]; nd = ndi_s[i]
        if a == 0: dx_list.append(0); continue
        pdi = 100 * p / a; ndi = 100 * nd / a
        dx = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) > 0 else 0
        dx_list.append(dx)

    # smooth DX into ADX
    adx_vals = smooth(dx_list, period)
    offset = period  # adx_vals[0] corresponds to index period in original
    for i in range(len(adx_vals)):
        orig_i = i + offset
        if orig_i < n and adx_vals[i] is not None:
            adx[orig_i] = adx_vals[i]
    return adx

def compute_rsi(closes, period=14):
    n = len(closes)
    rsi = [None] * n
    if n < period + 1:
        return rsi
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, n):
        if i > period:
            avg_g = (avg_g * (period-1) + gains[i-1]) / period
            avg_l = (avg_l * (period-1) + losses[i-1]) / period
        rsi[i] = 100 - 100/(1 + avg_g/avg_l) if avg_l != 0 else 100
    return rsi

def compute_macd(closes, fast=12, slow=26, signal=9):
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [f - s if f and s else None for f, s in zip(ema_fast, ema_slow)]
    valid = [v for v in macd_line if v is not None]
    sig_raw = ema(valid, signal)
    sig_full = [None] * len(macd_line)
    vi = 0
    for i, v in enumerate(macd_line):
        if v is not None:
            sig_full[i] = sig_raw[vi]; vi += 1
    hist = [m - s if m is not None and s is not None else None
            for m, s in zip(macd_line, sig_full)]
    return hist

# ── SIGNAL GENERATION ────────────────────────────────────────────────────────
def compute_signals(bars, variant='G'):
    """Return list of signal dicts for each bar."""
    use_filters = VARIANTS[variant]["filters"]
    adx_thresh = FILTER_ADX_MIN if use_filters else ADX_MIN

    opens  = [b[1] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    closes = [b[4] for b in bars]
    vols   = [b[5] for b in bars]
    n = len(bars)

    ema9  = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    adx   = compute_adx(highs, lows, closes, 14)
    rsi   = compute_rsi(closes, 14)

    macd_hist = None
    vol_ma    = None
    if use_filters:
        macd_hist = compute_macd(closes, 12, 26, 9)
        vol_ma = [None] * n
        for i in range(20, n):
            vol_ma[i] = sum(vols[i-20:i]) / 20

    signals = [None] * n
    for i in range(WARMUP_BARS, n):
        if (ema9[i] is None or ema21[i] is None or ema50[i] is None
                or adx[i] is None or rsi[i] is None):
            continue
        if adx[i] < adx_thresh:
            continue

        # 50EMA slope (10 bars)
        slope_bars = 10
        if i < slope_bars or ema50[i-slope_bars] is None:
            continue
        slope = (ema50[i] - ema50[i-slope_bars]) / ema50[i-slope_bars]
        slope_thresh = 0.0005

        # 9/21 EMA cross: current bar crossed (prev below/above, now above/below)
        if i < 1 or ema9[i-1] is None or ema21[i-1] is None:
            continue
        cross_long  = ema9[i-1] <= ema21[i-1] and ema9[i] > ema21[i]
        cross_short = ema9[i-1] >= ema21[i-1] and ema9[i] < ema21[i]

        long_base  = slope > slope_thresh  and cross_long  and rsi[i] >= RSI_LONG_MIN
        short_base = slope < -slope_thresh and cross_short and rsi[i] <= RSI_SHORT_MAX

        if use_filters:
            # MACD histogram confirmation
            mh = macd_hist[i] if macd_hist and macd_hist[i] is not None else None
            if mh is None:
                continue
            macd_ok_long  = mh > 0
            macd_ok_short = mh < 0

            # Volume spike: > 1.5x 20-bar average
            vm = vol_ma[i] if vol_ma and vol_ma[i] is not None else None
            if vm is None or vm == 0:
                continue
            vol_ok = vols[i] > 1.5 * vm

            # HTF slope: 50EMA over 40 bars
            htf_bars = 40
            if i < htf_bars or ema50[i-htf_bars] is None:
                continue
            htf_slope = (ema50[i] - ema50[i-htf_bars]) / ema50[i-htf_bars]
            htf_long  = htf_slope > 0
            htf_short = htf_slope < 0

            long_base  = long_base  and macd_ok_long  and vol_ok and htf_long
            short_base = short_base and macd_ok_short and vol_ok and htf_short

        if long_base:
            signals[i] = "long"
        elif short_base:
            signals[i] = "short"

    return signals

# ── BACKTESTING ───────────────────────────────────────────────────────────────
def backtest_coin_variant(bars, variant):
    """Run backtest for one coin+variant. Returns list of trade dicts."""
    tp_pct = VARIANTS[variant]["tp"]
    sl_pct = VARIANTS[variant]["sl"]
    signals = compute_signals(bars, variant)
    n = len(bars)

    trades = []
    in_trade = False
    cooldown = 0

    for i in range(n):
        sig = signals[i]
        bar = bars[i]
        ts, o, h, l, c = int(bar[0]), bar[1], bar[2], bar[3], bar[4]

        if in_trade:
            entry, direction, tp_price, sl_price, entry_ts = (
                trade_entry, trade_dir, trade_tp, trade_sl, trade_ts)
            # check exit on this bar
            exit_price = None
            result = None
            if direction == "long":
                if l <= sl_price:
                    exit_price = sl_price; result = "sl"
                elif h >= tp_price:
                    exit_price = tp_price; result = "tp"
            else:  # short
                if h >= sl_price:
                    exit_price = sl_price; result = "sl"
                elif l <= tp_price:
                    exit_price = tp_price; result = "tp"

            if result:
                pnl_pct = (exit_price - entry) / entry if direction == "long" else (entry - exit_price) / entry
                pos_size = (CAPITAL * RISK_PCT) / sl_pct
                pnl = pnl_pct * pos_size - (FEE + SLIP) * 2 * pos_size
                trades.append({
                    "entry_ts": entry_ts, "exit_ts": ts,
                    "direction": direction, "result": result,
                    "pnl": pnl, "bars_held": i - entry_bar_idx,
                })
                in_trade = False
                cooldown = COOLDOWN_BARS
            continue

        if cooldown > 0:
            cooldown -= 1
            continue

        if sig in ("long", "short"):
            entry_price = bar[4]  # close of signal bar
            if sig == "long":
                tp_price = entry_price * (1 + tp_pct)
                sl_price = entry_price * (1 - sl_pct)
            else:
                tp_price = entry_price * (1 - tp_pct)
                sl_price = entry_price * (1 + sl_pct)

            in_trade = True
            trade_entry = entry_price
            trade_dir   = sig
            trade_tp    = tp_price
            trade_sl    = sl_price
            trade_ts    = ts
            entry_bar_idx = i

    return trades

# ── STATS ─────────────────────────────────────────────────────────────────────
def calc_stats(trades):
    if not trades:
        return {"trades": 0, "wr": 0, "pf": 0, "net_pnl": 0, "max_dd": 0,
                "sharpe": 0, "expectancy": 0, "avg_bars": 0,
                "long_trades": 0, "long_wr": 0, "short_trades": 0, "short_wr": 0,
                "gross_profit": 0, "gross_loss": 0}

    wins  = [t for t in trades if t["pnl"] > 0]
    losses= [t for t in trades if t["pnl"] <= 0]
    longs = [t for t in trades if t["direction"] == "long"]
    shorts= [t for t in trades if t["direction"] == "short"]

    wr = len(wins) / len(trades) * 100
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = gp / gl if gl > 0 else float("inf")
    net = sum(t["pnl"] for t in trades)
    exp = net / len(trades)
    avg_bars = sum(t["bars_held"] for t in trades) / len(trades)

    # max drawdown on running equity
    equity = 0; peak = 0; max_dd = 0
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        equity += t["pnl"]
        if equity > peak: peak = equity
        dd = (peak - equity) / (CAPITAL + peak) * 100
        if dd > max_dd: max_dd = dd

    # sharpe (per trade)
    pnls = [t["pnl"] for t in trades]
    mean = sum(pnls) / len(pnls)
    std  = math.sqrt(sum((p - mean)**2 for p in pnls) / len(pnls)) if len(pnls) > 1 else 1
    sharpe = mean / std * math.sqrt(len(pnls)) if std > 0 else 0

    l_wr = len([t for t in longs if t["pnl"] > 0]) / len(longs) * 100 if longs else 0
    s_wr = len([t for t in shorts if t["pnl"] > 0]) / len(shorts) * 100 if shorts else 0

    return {
        "trades": len(trades), "wr": wr, "pf": pf, "net_pnl": net,
        "max_dd": max_dd, "sharpe": sharpe, "expectancy": exp, "avg_bars": avg_bars,
        "long_trades": len(longs), "long_wr": l_wr,
        "short_trades": len(shorts), "short_wr": s_wr,
        "gross_profit": gp, "gross_loss": gl,
    }

def apply_portfolio_cap(all_trades_flat):
    """Apply MAX_POSITIONS cap across all coins. Returns filtered trade list."""
    all_trades_flat.sort(key=lambda t: t["entry_ts"])
    active = []  # list of exit_ts
    kept = []
    for t in all_trades_flat:
        # remove expired
        active = [e for e in active if e > t["entry_ts"]]
        if len(active) < MAX_POSITIONS:
            active.append(t["exit_ts"])
            kept.append(t)
    return kept

# ── WORKER ───────────────────────────────────────────────────────────────────
def worker_task(symbol):
    """Fetch data + run all variants for one symbol. Returns (symbol, variant->trades)."""
    try:
        bars = fetch_symbol_data(symbol, INTERVAL, MONTHS)
        if len(bars) < WARMUP_BARS + 50:
            return symbol, None, f"not enough data ({len(bars)} bars)"

        result = {}
        for vname in VARIANTS:
            trades = backtest_coin_variant(bars, vname)
            result[vname] = trades
        return symbol, result, None
    except Exception as e:
        return symbol, None, str(e)

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=== Backtest v8.5 — Full Binance USDT-M Universe ===", flush=True)

    # 1. Discover symbols
    print("Discovering symbols from data.binance.vision S3...", flush=True)
    symbols = discover_symbols()
    print(f"Found {len(symbols)} USDT-M perpetual symbols", flush=True)

    # 2. Run parallel workers
    all_coin_trades = defaultdict(dict)  # variant -> coin -> [trades]
    symbol_issues = []
    done = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(worker_task, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                symbol, result, err = fut.result()
            except Exception as e:
                symbol_issues.append({"symbol": sym, "error": str(e)})
                print(f"[{done}/{len(symbols)}] {sym} EXCEPTION: {e}", flush=True)
                continue

            if err:
                symbol_issues.append({"symbol": symbol, "error": err})
                print(f"[{done}/{len(symbols)}] {symbol} SKIP: {err}", flush=True)
                continue

            for vname, trades in result.items():
                all_coin_trades[vname][symbol] = trades
            print(f"[{done}/{len(symbols)}] {symbol} OK", flush=True)

    # 3. Apply portfolio cap per variant, compute stats
    report = {"meta": {}, "variants": {}, "symbol_issues": symbol_issues}
    summary_lines = []

    # count trading days
    all_ts = []
    for vname in VARIANTS:
        for sym, trades in all_coin_trades[vname].items():
            for t in trades:
                all_ts.append(t["exit_ts"])
    if all_ts:
        days = (max(all_ts) - min(all_ts)) / (1000 * 86400)
    else:
        days = 365 * 2

    for vname, vcfg in VARIANTS.items():
        # flatten all trades for this variant
        flat = []
        for sym, trades in all_coin_trades[vname].items():
            for t in trades:
                t["symbol"] = sym
                flat.append(t)

        capped = apply_portfolio_cap(flat)

        # per-coin stats
        coin_trades_map = defaultdict(list)
        for t in capped:
            coin_trades_map[t["symbol"]].append(t)

        coin_stats = {}
        for sym, trades in coin_trades_map.items():
            coin_stats[sym] = calc_stats(trades)

        portfolio_stats = calc_stats(capped)
        trades_per_day = len(capped) / days if days > 0 else 0

        # passing coins: PF>=1.5, WR>=42%, trades>=8
        passing = {s: st for s, st in coin_stats.items()
                   if st["pf"] >= 1.5 and st["wr"] >= 42 and st["trades"] >= 8}

        report["variants"][vname] = {
            "stats": portfolio_stats,
            "coins": coin_stats,
        }

        # ── Format summary block ──
        tp_pct = int(vcfg["tp"]*100)
        sl_pct = int(vcfg["sl"]*100)
        tag = f"TP {tp_pct}% / SL {sl_pct}%"
        if vcfg["filters"]: tag += " + confirmation filters"
        lines = [
            f"\n{'='*65}",
            f"  VARIANT {vname} — {tag}",
            f"{'='*65}",
            f"  Trades       : {portfolio_stats['trades']}",
            f"  Win Rate     : {portfolio_stats['wr']:.2f}%",
            f"  Profit Factor: {portfolio_stats['pf']:.3f}",
            f"  Net PnL      : ${portfolio_stats['net_pnl']:.1f}",
            f"  Max Drawdown : {portfolio_stats['max_dd']:.2f}%",
            f"  Sharpe       : {portfolio_stats['sharpe']:.3f}",
            f"  Expectancy   : ${portfolio_stats['expectancy']:.2f}/trade",
            f"  Avg Bars Held: {portfolio_stats['avg_bars']:.1f}",
            f"  Long  : {portfolio_stats['long_trades']} trades | WR {portfolio_stats['long_wr']:.2f}%",
            f"  Short : {portfolio_stats['short_trades']} trades | WR {portfolio_stats['short_wr']:.2f}%",
            f"  Gross Profit : ${portfolio_stats['gross_profit']:.1f} | Loss: ${portfolio_stats['gross_loss']:.1f}",
        ]

        # top 25 by PF
        lines.append(f"\n  -- Top 25 by Profit Factor --")
        sorted_coins = sorted(coin_stats.items(), key=lambda x: (x[1]["pf"] if x[1]["pf"] != float("inf") else 9999, x[1]["wr"]), reverse=True)
        for sym, st in sorted_coins[:25]:
            pf_str = "   inf" if st["pf"] == float("inf") else f"{st['pf']:6.3f}"
            lines.append(f"  {sym:<26}{st['trades']:>5}  {st['wr']:>6.1f}%  {pf_str}  ${st['net_pnl']:>9.2f}")

        # passing coins
        lines.append(f"\n  -- Passing Coins (PF>=1.5 WR>=42% >=8 trades): {len(passing)} --")
        for sym, st in sorted(passing.items(), key=lambda x: x[1]["pf"], reverse=True):
            pf_str = "inf" if st["pf"] == float("inf") else f"{st['pf']:.3f}"
            lines.append(f"  OK {sym:<22} PF={pf_str} WR={st['wr']:.1f}% T={st['trades']}")

        lines.append(f"\n  Trades/day (portfolio): ~{trades_per_day:.1f}")
        summary_lines.extend(lines)

    # ── Cross-variant table ──
    summary_lines.append(f"\n{'='*65}")
    summary_lines.append("  CROSS-VARIANT SUMMARY")
    summary_lines.append(f"{'='*65}")
    header = f"  {'Var':<7} {'TP':>4} {'SL':>4} {'Trades':>7} {'T/day':>6} {'WR%':>7} {'PF':>8} {'NetPnL':>12} {'MaxDD':>6}  Verdict"
    summary_lines.append(header)
    for vname, vcfg in VARIANTS.items():
        st = report["variants"][vname]["stats"]
        tpd = st["trades"] / days if days > 0 else 0
        verdict = "PROFITABLE" if st["net_pnl"] > 0 and st["pf"] > 1.0 else "NOT YET"
        summary_lines.append(
            f"  {vname:<7} {int(vcfg['tp']*100):>3}% {int(vcfg['sl']*100):>3}%"
            f" {st['trades']:>7} {tpd:>6.1f} {st['wr']:>6.1f}%"
            f" {st['pf']:>8.3f} ${st['net_pnl']:>10.2f} {st['max_dd']:>5.1f}%  {verdict}"
        )

    # ── Symbol issues ──
    summary_lines.append(f"\n-- Symbol Issues ({len(symbol_issues)}) --")
    for si in symbol_issues:
        summary_lines.append(f"  {si['symbol']}: {si['error']}")

    # ── Write outputs ──
    summary_text = "\n".join(summary_lines)
    with open("backtest_summary.txt", "w") as f:
        f.write(summary_text)

    report["meta"] = {
        "version": "v8.5",
        "period": f"{MONTHS[0]} -> {MONTHS[-1]}",
        "symbols_tested": len(symbols),
        "variants": {k: {**v, "tp": v["tp"], "sl": v["sl"]} for k, v in VARIANTS.items()},
        "settings": {
            "capital": CAPITAL, "risk_pct": RISK_PCT,
            "fee": FEE, "slip": SLIP,
            "adx_min": ADX_MIN, "filter_adx_min": FILTER_ADX_MIN,
            "max_positions": MAX_POSITIONS,
            "rsi_long_min": RSI_LONG_MIN, "rsi_short_max": RSI_SHORT_MAX,
        }
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, default=str)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s", flush=True)
    print(summary_text, flush=True)

if __name__ == "__main__":
    main()
