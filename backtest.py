"""
Backtest v8 — Fixed % TP/SL Multi-Variant
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy : ADX≥22 + 50EMA slope + 9/21 EMA crossover (15m)
TP/SL    : Fixed % (6 variants A-F)
Coins    : Top 200 USDT-M perpetuals by volume (auto-fetched)
Period   : 2 years (coins with less history use whatever they have)
Capital  : $10,000 shared | Risk 0.75%/trade | Max 6 positions
Fees     : 0.05% taker per side | Slippage 0.02% per side
Workers  : 60 ProcessPoolExecutor inside single GH Actions job
stdlib   : only — no pip installs
"""

import csv, io, json, math, os, sys, time, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Variants ──────────────────────────────────────────────────────────────
VARIANTS = {
    'A': {'tp': 0.02, 'sl': 0.01},   # 2% TP / 1% SL   R:R 1:2
    'B': {'tp': 0.02, 'sl': 0.015},  # 2% TP / 1.5% SL R:R 1:1.3
    'C': {'tp': 0.03, 'sl': 0.01},   # 3% TP / 1% SL   R:R 1:3
    'D': {'tp': 0.03, 'sl': 0.015},  # 3% TP / 1.5% SL R:R 1:2
    'E': {'tp': 0.03, 'sl': 0.02},   # 3% TP / 2% SL   R:R 1:1.5
    'F': {'tp': 0.03, 'sl': 0.09},   # 3% TP / 9% SL   wide SL
}

# ── Settings ──────────────────────────────────────────────────────────────
CAPITAL        = 10_000.0
RISK_PCT       = 0.0075
MAX_POSITIONS  = 6
FEE_RATE       = 0.0005   # 0.05% taker
SLIP_RATE      = 0.0002   # 0.02% slippage
ADX_MIN        = 22
SLOPE_THRESH   = 0.0005   # 0.05%
COOLDOWN_BARS  = 4        # skip 4 bars after close on same coin
WORKERS        = 60
TOP_N_COINS    = 200
INTERVAL       = '15m'

# 2-year window ending last completed month
_now    = datetime.now(timezone.utc)
_end_y  = _now.year if _now.month > 1 else _now.year - 1
_end_m  = _now.month - 1 if _now.month > 1 else 12
END_YEAR, END_MONTH = _end_y, _end_m
START_YEAR = END_YEAR - 2
START_MONTH = END_MONTH + 1 if END_MONTH < 12 else 1
if END_MONTH == 12:
    START_YEAR += 1

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ── Coin fetching ─────────────────────────────────────────────────────────
def fetch_top_coins(n=TOP_N_COINS):
    """Fetch top N USDT-M perps by 24h quote volume from Binance ticker."""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    known_renames = {'MATICUSDT': 'POLUSDT'}
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"[WARN] Could not fetch live tickers: {e}")
        print("[WARN] Falling back to hardcoded 30-coin list")
        return FALLBACK_SYMBOLS

    usdt = [
        d for d in data
        if d['symbol'].endswith('USDT')
        and not d['symbol'].endswith('DOWNUSDT')
        and not d['symbol'].endswith('UPUSDT')
        and not d['symbol'].endswith('BULLUSDT')
        and not d['symbol'].endswith('BEARUSDT')
    ]
    usdt.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
    syms = []
    for d in usdt:
        s = d['symbol']
        s = known_renames.get(s, s)
        if s not in syms:
            syms.append(s)
        if len(syms) >= n:
            break
    print(f"[INFO] Fetched {len(syms)} coins from Binance futures ticker")
    return syms

FALLBACK_SYMBOLS = [
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT',
    'ADAUSDT','AVAXUSDT','LTCUSDT','LINKUSDT','DOTUSDT',
    'ARBUSDT','OPUSDT','BNBUSDT','NEARUSDT','APTUSDT',
    'SUIUSDT','INJUSDT','UNIUSDT','AAVEUSDT','HBARUSDT',
    'ATOMUSDT','WLDUSDT','TRUMPUSDT','BOMEUSDT','NEIROUSDT',
    '1000BONKUSDT','1000PEPEUSDT','1000SHIBUSDT','1000FLOKIUSDT','WIFUSDT',
]

# ── Month iterator ─────────────────────────────────────────────────────────
def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

# ── Data fetching ─────────────────────────────────────────────────────────
def fetch_month(symbol, interval, year, month):
    fn  = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{symbol}/{interval}/{fn}"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            csvname = z.namelist()[0]
            with z.open(csvname) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None   # symbol didn't exist yet or delisted
        raise
    except Exception:
        return None

def fetch_symbol_data(symbol):
    rows_all = []
    months_fetched = 0
    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        rows = fetch_month(symbol, INTERVAL, y, m)
        if rows is None:
            continue
        months_fetched += 1
        for row in rows:
            if not row or row[0] == 'open_time':
                continue
            try:
                ts = int(row[0])
                if ts > 10**14:
                    ts //= 1000
                rows_all.append((
                    ts,
                    float(row[1]),  # open
                    float(row[2]),  # high
                    float(row[3]),  # low
                    float(row[4]),  # close
                    float(row[5]),  # volume
                ))
            except (ValueError, IndexError):
                continue
    rows_all.sort(key=lambda x: x[0])
    return rows_all, months_fetched

# ── Indicators ────────────────────────────────────────────────────────────
def ema_series(closes, period):
    k = 2.0 / (period + 1)
    r = [closes[0]]
    for v in closes[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def atr_series(highs, lows, closes, period=14):
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))
    atr = [sum(trs[:period]) / period]
    for t in trs[period:]:
        atr.append((atr[-1] * (period - 1) + t) / period)
    return [None] * (period - 1) + atr

def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 2:
        return [None] * n, [None] * n, [None] * n

    pdm_list, mdm_list, tr_list = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_list.append(up   if up > down and up > 0   else 0.0)
        mdm_list.append(down if down > up and down > 0 else 0.0)
        tr_list.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))

    def wilder(arr, p):
        if len(arr) < p:
            return []
        s = [sum(arr[:p])]
        for x in arr[p:]:
            s.append(s[-1] - s[-1] / p + x)
        return s

    str_ = wilder(tr_list, period)
    spdm = wilder(pdm_list, period)
    smdm = wilder(mdm_list, period)
    if not str_:
        return [None]*n, [None]*n, [None]*n

    pdi_list = [100*p/t if t else 0 for p, t in zip(spdm, str_)]
    mdi_list = [100*m/t if t else 0 for m, t in zip(smdm, str_)]
    dx_list  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi_list, mdi_list)]

    if len(dx_list) < period:
        return [None]*n, [None]*n, [None]*n

    adx_val = [sum(dx_list[:period]) / period]
    for d in dx_list[period:]:
        adx_val.append((adx_val[-1] * (period - 1) + d) / period)

    # align to original length (first bar has no prev, so offset by 1)
    pad_adx = [None] * (n - len(adx_val))
    pad_di  = [None] * (n - len(pdi_list) - 1)

    adx_out = pad_adx + adx_val
    pdi_out = [None] + pad_di + pdi_list
    mdi_out = [None] + pad_di + mdi_list
    return adx_out, pdi_out, mdi_out

# ── Signal generation ──────────────────────────────────────────────────────
def compute_signals(bars):
    """Returns list of signal dicts per bar index."""
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    n      = len(closes)

    warmup = 60  # need at least 60 bars for indicators
    if n < warmup:
        return []

    e9  = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)
    adx_arr, pdi_arr, mdi_arr = adx_series(highs, lows, closes, 14)

    signals = []
    for i in range(warmup, n):
        adx = adx_arr[i]
        if adx is None:
            signals.append({'i': i, 'signal': None, 'reason': 'warmup_adx'})
            continue

        # ADX filter
        if adx < ADX_MIN:
            signals.append({'i': i, 'signal': None, 'reason': 'adx_low'})
            continue

        # 50 EMA slope over last 10 bars
        if i < 10:
            signals.append({'i': i, 'signal': None, 'reason': 'warmup_slope'})
            continue
        slope = (e50[i] - e50[i-10]) / e50[i-10]
        trend_up   = slope >  SLOPE_THRESH
        trend_down = slope < -SLOPE_THRESH

        if not trend_up and not trend_down:
            signals.append({'i': i, 'signal': None, 'reason': 'no_trend'})
            continue

        # EMA crossover (confirmed on closed bar)
        cross_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
        cross_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

        sig = None
        reason = 'no_cross'
        if trend_up   and cross_up:   sig = 'long';  reason = 'long'
        if trend_down and cross_down: sig = 'short'; reason = 'short'

        signals.append({'i': i, 'signal': sig, 'reason': reason})

    return signals

# ── Backtest single coin × single variant ─────────────────────────────────
def backtest_coin_variant(symbol, bars, variant_name, tp_pct, sl_pct,
                           shared_equity_ref=None):
    """
    Returns list of trade dicts.
    shared_equity_ref is not used here (portfolio-level managed outside),
    but position sizing uses 0.75% risk on $10k base.
    """
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    n      = len(bars)

    signals = compute_signals(bars)
    sig_map = {s['i']: s for s in signals}

    trades      = []
    in_position = False
    entry_price = 0.0
    entry_i     = 0
    position_side = None
    last_close_i  = -999

    for i in range(len(signals)):
        bar_i = signals[i]['i']
        if bar_i >= n:
            break

        # Update open position — check high/low of current bar for TP/SL
        if in_position:
            h = highs[bar_i]
            l = lows[bar_i]
            c = closes[bar_i]
            ts = bars[bar_i][0]

            hit_tp = hit_sl = False
            exit_price = 0.0

            if position_side == 'long':
                tp_price = entry_price * (1 + tp_pct)
                sl_price = entry_price * (1 - sl_pct)
                if l <= sl_price:
                    hit_sl = True; exit_price = sl_price
                if h >= tp_price:
                    # if SL also triggered same bar, assume SL hit first (conservative)
                    if not hit_sl:
                        hit_tp = True; exit_price = tp_price
            else:  # short
                tp_price = entry_price * (1 - tp_pct)
                sl_price = entry_price * (1 + sl_pct)
                if h >= sl_price:
                    hit_sl = True; exit_price = sl_price
                if l <= tp_price:
                    if not hit_sl:
                        hit_tp = True; exit_price = tp_price

            if hit_tp or hit_sl:
                gross_pnl_pct = (
                    (exit_price - entry_price) / entry_price
                    if position_side == 'long'
                    else (entry_price - exit_price) / entry_price
                )
                # fees + slippage on notional (entry + exit)
                total_cost_pct = (FEE_RATE + SLIP_RATE) * 2
                net_pnl_pct    = gross_pnl_pct - total_cost_pct

                # risk-based position size: risk 0.75% of capital
                # SL distance = sl_pct, so position_size = (capital * 0.75%) / sl_pct
                position_size_usd = (CAPITAL * RISK_PCT) / sl_pct
                net_pnl_usd = position_size_usd * net_pnl_pct

                trades.append({
                    'symbol'    : symbol,
                    'variant'   : variant_name,
                    'side'      : position_side,
                    'entry_i'   : entry_i,
                    'exit_i'    : bar_i,
                    'entry_ts'  : bars[entry_i][0],
                    'exit_ts'   : ts,
                    'entry_px'  : round(entry_price, 8),
                    'exit_px'   : round(exit_price, 8),
                    'result'    : 'tp' if hit_tp else 'sl',
                    'pnl_pct'   : round(net_pnl_pct * 100, 4),
                    'pnl_usd'   : round(net_pnl_usd, 4),
                    'bars_held' : bar_i - entry_i,
                })
                in_position = False
                last_close_i = bar_i

        # Entry signal on this bar (only if not in position)
        if not in_position:
            sig_info = sig_map.get(bar_i)
            if sig_info and sig_info['signal'] in ('long', 'short'):
                # cooldown check
                if bar_i - last_close_i >= COOLDOWN_BARS:
                    in_position    = True
                    entry_price    = closes[bar_i]
                    entry_i        = bar_i
                    position_side  = sig_info['signal']

    return trades

# ── Worker function (runs in subprocess) ──────────────────────────────────
def worker_task(symbol):
    """Fetch data + run all 6 variants for one symbol. Returns (symbol, results)."""
    try:
        bars, months = fetch_symbol_data(symbol)
        if len(bars) < 200:
            return symbol, None, f"insufficient data ({len(bars)} bars)"

        all_trades = {}
        for vname, vcfg in VARIANTS.items():
            trades = backtest_coin_variant(
                symbol, bars, vname,
                vcfg['tp'], vcfg['sl']
            )
            all_trades[vname] = trades

        return symbol, all_trades, f"ok ({months} months, {len(bars)} bars)"
    except Exception as e:
        return symbol, None, f"error: {e}"

# ── Stats calculator ──────────────────────────────────────────────────────
def calc_stats(trades):
    if not trades:
        return None
    wins   = [t for t in trades if t['pnl_usd'] > 0]
    losses = [t for t in trades if t['pnl_usd'] <= 0]
    total  = len(trades)
    wr     = len(wins) / total * 100 if total else 0

    gross_profit = sum(t['pnl_usd'] for t in wins)
    gross_loss   = abs(sum(t['pnl_usd'] for t in losses))
    pf           = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    net_pnl      = sum(t['pnl_usd'] for t in trades)

    longs  = [t for t in trades if t['side'] == 'long']
    shorts = [t for t in trades if t['side'] == 'short']
    lw     = len([t for t in longs  if t['pnl_usd'] > 0])
    sw     = len([t for t in shorts if t['pnl_usd'] > 0])

    # max drawdown (sequential equity curve)
    equity = CAPITAL
    peak   = CAPITAL
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        equity += t['pnl_usd']
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > max_dd:
            max_dd = dd

    avg_win  = gross_profit / len(wins)   if wins   else 0
    avg_loss = gross_loss   / len(losses) if losses else 0
    exp      = (wr/100 * avg_win) - ((1-wr/100) * avg_loss)

    # Sharpe (simplified, per-trade returns)
    returns = [t['pnl_usd'] for t in trades]
    mean_r  = sum(returns) / len(returns)
    std_r   = math.sqrt(sum((r - mean_r)**2 for r in returns) / len(returns)) if len(returns) > 1 else 0
    sharpe  = (mean_r / std_r) * math.sqrt(252 * 6.5 * 4) if std_r > 0 else 0  # annualised 15m

    avg_bars = sum(t['bars_held'] for t in trades) / total

    return {
        'total_trades' : total,
        'win_rate'     : round(wr, 2),
        'profit_factor': round(pf, 3),
        'net_pnl'      : round(net_pnl, 2),
        'max_drawdown' : round(max_dd, 2),
        'sharpe'       : round(sharpe, 3),
        'avg_win'      : round(avg_win, 2),
        'avg_loss'     : round(avg_loss, 2),
        'expectancy'   : round(exp, 2),
        'avg_bars_held': round(avg_bars, 1),
        'long_trades'  : len(longs),
        'short_trades' : len(shorts),
        'long_wr'      : round(lw/len(longs)*100, 2)  if longs  else 0,
        'short_wr'     : round(sw/len(shorts)*100, 2) if shorts else 0,
        'gross_profit' : round(gross_profit, 2),
        'gross_loss'   : round(gross_loss, 2),
    }

def monthly_pnl(trades):
    buckets = {}
    for t in trades:
        dt  = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        buckets[key] = buckets.get(key, 0) + t['pnl_usd']
    return {k: round(v, 2) for k, v in sorted(buckets.items())}

# ── Portfolio-level position cap simulation ───────────────────────────────
def apply_portfolio_cap(all_symbol_trades, variant_name):
    """
    Merge all coin trades for a variant, apply max 6 concurrent positions,
    re-calculate PnL stream respecting the cap.
    Returns filtered trade list.
    """
    trades = []
    for sym_trades in all_symbol_trades.values():
        trades.extend([t for t in sym_trades if t['variant'] == variant_name])

    # Sort by entry time
    trades.sort(key=lambda x: x['entry_ts'])

    active   = []  # list of exit_ts for open positions
    accepted = []

    for t in trades:
        # Remove positions that have closed before this entry
        active = [ex for ex in active if ex > t['entry_ts']]
        if len(active) < MAX_POSITIONS:
            active.append(t['exit_ts'])
            accepted.append(t)

    return accepted

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 60)
    print("  Backtest v8 — Fixed % TP/SL | 6 Variants | 200 Coins")
    print("=" * 60)
    print(f"Period  : {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}")
    print(f"Variants: {list(VARIANTS.keys())}")
    print(f"Workers : {WORKERS}")
    print()

    # Fetch coin list
    symbols = fetch_top_coins(TOP_N_COINS)
    print(f"[INFO] Testing {len(symbols)} symbols\n")

    # Run in parallel
    results_raw  = {}   # symbol -> {variant -> [trades]}
    symbol_notes = {}

    done = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker_task, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                symbol, all_trades, note = fut.result()
                symbol_notes[symbol] = note
                if all_trades:
                    results_raw[symbol] = all_trades
            except Exception as e:
                symbol_notes[sym] = f"exception: {e}"
            done += 1
            if done % 20 == 0 or done == len(symbols):
                print(f"  [{done}/{len(symbols)}] symbols processed...")

    print(f"\n[INFO] Data fetched in {time.time()-t0:.1f}s")
    print(f"[INFO] Symbols with data: {len(results_raw)} / {len(symbols)}")

    # ── Per-variant aggregate results ──────────────────────────────────────
    summary_lines = []
    report        = {'meta': {}, 'variants': {}, 'symbol_notes': symbol_notes}

    report['meta'] = {
        'period_start'  : f"{START_YEAR}-{START_MONTH:02d}",
        'period_end'    : f"{END_YEAR}-{END_MONTH:02d}",
        'capital'       : CAPITAL,
        'risk_pct'      : RISK_PCT,
        'fee_rate'      : FEE_RATE,
        'slip_rate'     : SLIP_RATE,
        'adx_min'       : ADX_MIN,
        'slope_thresh'  : SLOPE_THRESH,
        'max_positions' : MAX_POSITIONS,
        'cooldown_bars' : COOLDOWN_BARS,
        'symbols_tested': len(results_raw),
        'variants'      : VARIANTS,
    }

    for vname, vcfg in VARIANTS.items():
        tp_pct = vcfg['tp'] * 100
        sl_pct = vcfg['sl'] * 100

        # Apply portfolio cap
        capped_trades = apply_portfolio_cap(results_raw, vname)

        stats = calc_stats(capped_trades)
        if not stats:
            summary_lines.append(f"\n[Variant {vname}] No trades generated.")
            continue

        monthly = monthly_pnl(capped_trades)

        # Per-coin breakdown
        coin_stats_map = {}
        for sym, sym_trades_all in results_raw.items():
            sym_v_trades = [t for t in sym_trades_all.get(vname, [])
                            if any(ct['entry_ts'] == t['entry_ts'] and
                                   ct['symbol'] == t['symbol']
                                   for ct in capped_trades)]
            if sym_v_trades:
                cs = calc_stats(sym_v_trades)
                if cs:
                    coin_stats_map[sym] = cs

        coin_sorted = sorted(coin_stats_map.items(),
                             key=lambda x: x[1]['profit_factor'], reverse=True)

        usable = stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42
        verdict = "✅ USABLE" if usable else "❌ NOT YET"

        lines = []
        lines.append(f"\n{'='*60}")
        lines.append(f"  VARIANT {vname} — TP {tp_pct:.0f}% / SL {sl_pct:.1f}%  |  {verdict}")
        lines.append(f"{'='*60}")
        lines.append(f"  Trades      : {stats['total_trades']}")
        lines.append(f"  Win Rate    : {stats['win_rate']}%")
        lines.append(f"  Profit Factor: {stats['profit_factor']}")
        lines.append(f"  Net PnL     : ${stats['net_pnl']}")
        lines.append(f"  Max Drawdown: {stats['max_drawdown']}%")
        lines.append(f"  Sharpe      : {stats['sharpe']}")
        lines.append(f"  Expectancy  : ${stats['expectancy']}/trade")
        lines.append(f"  Avg Bars Held: {stats['avg_bars_held']}")
        lines.append(f"  Long  : {stats['long_trades']} trades | WR {stats['long_wr']}%")
        lines.append(f"  Short : {stats['short_trades']} trades | WR {stats['short_wr']}%")
        lines.append(f"  Gross Profit: ${stats['gross_profit']} | Gross Loss: ${stats['gross_loss']}")

        lines.append(f"\n  ── Top 20 Coins by Profit Factor ──")
        lines.append(f"  {'Coin':<18} {'Trades':>6} {'WR%':>7} {'PF':>7} {'NetPnL':>10}")
        lines.append(f"  {'-'*52}")
        for sym, cs in coin_sorted[:20]:
            lines.append(
                f"  {sym:<18} {cs['total_trades']:>6} {cs['win_rate']:>6.1f}%"
                f" {cs['profit_factor']:>7.3f} ${cs['net_pnl']:>9.2f}"
            )

        lines.append(f"\n  ── Bottom 10 Coins ──")
        for sym, cs in coin_sorted[-10:]:
            lines.append(
                f"  {sym:<18} {cs['total_trades']:>6} {cs['win_rate']:>6.1f}%"
                f" {cs['profit_factor']:>7.3f} ${cs['net_pnl']:>9.2f}"
            )

        lines.append(f"\n  ── Monthly PnL ──")
        for month, pnl in monthly.items():
            bar = '█' * int(abs(pnl) / 50) if abs(pnl) > 0 else ''
            sign = '+' if pnl >= 0 else ''
            lines.append(f"  {month}  {sign}${pnl:>8.2f}  {bar}")

        # Coins that beat targets
        good_coins = [(s, c) for s, c in coin_sorted
                      if c['profit_factor'] >= 1.5 and c['win_rate'] >= 42
                      and c['total_trades'] >= 10]
        lines.append(f"\n  ── Coins Passing Targets (PF≥1.5, WR≥42%, ≥10 trades): {len(good_coins)} ──")
        for sym, cs in good_coins:
            lines.append(
                f"  ✅ {sym:<18} PF={cs['profit_factor']:.3f} WR={cs['win_rate']:.1f}%"
                f" Trades={cs['total_trades']}"
            )

        block = '\n'.join(lines)
        summary_lines.append(block)
        print(block)

        report['variants'][vname] = {
            'config'      : vcfg,
            'aggregate'   : stats,
            'monthly_pnl' : monthly,
            'per_coin'    : {s: c for s, c in coin_sorted},
            'good_coins'  : [s for s, _ in good_coins],
            'verdict'     : verdict,
            'trades'      : capped_trades,
        }

    # ── Symbol fetch notes ──────────────────────────────────────────────────
    failed = [(s, n) for s, n in symbol_notes.items() if 'error' in n or 'insufficient' in n]
    summary_lines.append(f"\n── Fetch Notes ({len(failed)} issues) ──")
    for sym, note in failed[:30]:
        summary_lines.append(f"  {sym}: {note}")

    elapsed = time.time() - t0
    summary_lines.append(f"\nCompleted in {elapsed:.1f}s")
    print(f"\n✅ Done in {elapsed:.1f}s")

    # ── Write outputs ───────────────────────────────────────────────────────
    out_dir = Path(os.environ.get('GITHUB_WORKSPACE', '.'))

    summary_text = '\n'.join(summary_lines)
    (out_dir / 'backtest_summary.txt').write_text(summary_text)
    print(f"[OUT] backtest_summary.txt written")

    # Trim trades in report to save space (keep last 500 per variant)
    for vname in report['variants']:
        tr = report['variants'][vname].get('trades', [])
        report['variants'][vname]['trades'] = tr[-500:]

    (out_dir / 'backtest_report.json').write_text(
        json.dumps(report, indent=2, default=str)
    )
    print(f"[OUT] backtest_report.json written")

if __name__ == '__main__':
    main()

