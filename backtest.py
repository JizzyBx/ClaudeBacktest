"""
G Max — Multi-Variant Backtest (TP 0.5% / SL 1.5%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Signal: EMA9/21 crossover + EMA50 slope filter + ADX(14) >= 22
Variants (8, one per shard):
  0: 15m  no S/R      4: 15m  +4h S/R fractal filter
  1: 30m  no S/R      5: 30m  +4h S/R fractal filter
  2: 1h   no S/R      6: 1h   +4h S/R fractal filter
  3: 4h   no S/R      7: 4h   +4h S/R fractal filter

S/R filter: 4h fractal pivots (5 bars either side). Block a long if price
is within 0.5% of a 4h resistance pivot above it; block a short if price
is within 0.5% of a 4h support pivot below it.

stdlib only. Data: data.binance.vision futures monthly klines.
"""

import sys, json, time, zipfile, io, csv, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from math import sqrt

# ── Config ──────────────────────────────────────────────────
ALL_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'SOLUSDT']

START_YM = (2024, 8)
END_YM   = (2026, 7)

CAPITAL   = 1000.0
RISK_PCT  = 0.02
FEE       = 0.0004
SLIP      = 0.0002
LEVERAGE  = 10

TP_PCT = 0.005
SL_PCT = 0.015
MIN_BARS = 100

WORKERS = 16
NUM_SHARDS = 8

TIMEFRAMES = {
    0: '15m', 1: '30m', 2: '1h', 3: '4h',
    4: '15m', 5: '30m', 6: '1h', 7: '4h',
}
USE_SR = {0: False, 1: False, 2: False, 3: False,
          4: True,  5: True,  6: True,  7: True}

MAX_BARS_BY_TF = {  # ~10 days max hold, converted to bar count per TF
    '15m': 960, '30m': 480, '1h': 240, '4h': 60,
}

SR_FRACTAL_N = 5      # bars either side for fractal pivot
SR_BUFFER    = 0.005  # 0.5%

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines/{sym}/{tf}/{sym}-{tf}-{y}-{m:02d}.zip"

# ── Data fetch ──────────────────────────────────────────────
def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out

def fetch_month(symbol, tf, year, month):
    url = BASE_URL.format(sym=symbol, tf=tf, y=year, m=month)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        rows = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.reader(text)
                for row in reader:
                    if not row:
                        continue
                    try:
                        ts = int(row[0])
                    except ValueError:
                        continue  # header row
                    # Binance kline open_time is milliseconds; normalize to seconds.
                    # (>10**14 case = microseconds, seen on some archive variants)
                    if ts > 10**14:
                        ts = ts // 1_000_000
                    else:
                        ts = ts // 1000
                    o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
                    rows.append((ts, o, h, l, c))
        return rows
    except urllib.error.HTTPError:
        return []
    except Exception:
        return []

def fetch_symbol(symbol, tf):
    months = month_range(START_YM, END_YM)
    all_rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_month, symbol, tf, y, m): (y, m) for y, m in months}
        for fut in as_completed(futs):
            rows = fut.result()
            if rows:
                all_rows.extend(rows)
    dedup = {}
    for ts, o, h, l, c in all_rows:
        dedup[ts] = (o, h, l, c)
    out = [(ts, *dedup[ts]) for ts in sorted(dedup)]
    return out

def fetch_symbol_multi_tf(symbol, tfs):
    """Fetch a symbol at multiple timeframes concurrently."""
    result = {}
    with ThreadPoolExecutor(max_workers=max(1, WORKERS // max(1, len(tfs)))) as ex:
        pass
    for tf in tfs:
        result[tf] = fetch_symbol(symbol, tf)
    return result

# ── Indicators ──────────────────────────────────────────────
def ema_series(values, period):
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out

def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 3:
        return [0.0] * n
    pdm = [0.0] * n
    mdm = [0.0] * n
    trs = [0.0] * n
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        pdm[i] = up if (up > down and up > 0) else 0.0
        mdm[i] = down if (down > up and down > 0) else 0.0
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))

    def wilder_smooth(v, p):
        out = [0.0] * n
        if n <= p:
            return out
        s = sum(v[1:p + 1])
        out[p] = s
        for i in range(p + 1, n):
            out[i] = out[i - 1] - out[i - 1] / p + v[i]
        return out

    st = wilder_smooth(trs, period)
    sp = wilder_smooth(pdm, period)
    sm = wilder_smooth(mdm, period)

    pdi = [0.0] * n
    mdi = [0.0] * n
    for i in range(period, n):
        if st[i]:
            pdi[i] = 100 * sp[i] / st[i]
            mdi[i] = 100 * sm[i] / st[i]

    dx = [0.0] * n
    for i in range(period, n):
        s = pdi[i] + mdi[i]
        dx[i] = 100 * abs(pdi[i] - mdi[i]) / s if s else 0.0

    adx = [0.0] * n
    start = period * 2
    if start < n:
        adx[start] = sum(dx[period:start]) / period if start > period else 0.0
        for i in range(start + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx

def fractal_pivots(highs, lows, n=SR_FRACTAL_N):
    """Return (res_levels, sup_levels) as parallel arrays: for each index i,
    the most recent confirmed resistance/support pivot price at or before i
    (0.0 if none yet). A pivot at index k is confirmed only from index k+n
    onward (needs n bars after it to confirm)."""
    length = len(highs)
    res_at = [0.0] * length
    sup_at = [0.0] * length
    last_res = 0.0
    last_sup = 0.0
    for i in range(length):
        k = i - n
        if k - n >= 0 and k + n < length:
            is_res = all(highs[k] >= highs[k - j] for j in range(1, n + 1)) and \
                     all(highs[k] >= highs[k + j] for j in range(1, n + 1))
            is_sup = all(lows[k] <= lows[k - j] for j in range(1, n + 1)) and \
                     all(lows[k] <= lows[k + j] for j in range(1, n + 1))
            if is_res:
                last_res = highs[k]
            if is_sup:
                last_sup = lows[k]
        res_at[i] = last_res
        sup_at[i] = last_sup
    return res_at, sup_at

def build_sr_lookup(candles_4h):
    """Build a time-sorted lookup of (ts, res_level, sup_level) from 4h candles,
    for use by lower-timeframe variants doing an as-of lookup."""
    if not candles_4h:
        return [], [], []
    highs = [c[2] for c in candles_4h]
    lows  = [c[3] for c in candles_4h]
    ts    = [c[0] for c in candles_4h]
    res_at, sup_at = fractal_pivots(highs, lows, SR_FRACTAL_N)
    return ts, res_at, sup_at

def sr_asof(ts_list, res_at, sup_at, target_ts):
    """Binary search for the most recent 4h S/R levels at or before target_ts."""
    if not ts_list:
        return 0.0, 0.0
    lo, hi = 0, len(ts_list) - 1
    if target_ts < ts_list[0]:
        return 0.0, 0.0
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if ts_list[mid] <= target_ts:
            lo = mid
        else:
            hi = mid - 1
    return res_at[lo], sup_at[lo]

# ── Signal ──────────────────────────────────────────────────
def precompute_signal_arrays(candles):
    closes = [c[4] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    ema9  = ema_series(closes, 9)
    ema21 = ema_series(closes, 21)
    ema50 = ema_series(closes, 50)
    adx   = adx_series(highs, lows, closes, 14)
    return {'closes': closes, 'highs': highs, 'lows': lows,
            'ema9': ema9, 'ema21': ema21, 'ema50': ema50, 'adx': adx}

def signal_at(arrs, i):
    """Evaluate signal on bar i (closed bar). Returns 'buy', 'sell', or None."""
    if i < 55:
        return None
    ema9, ema21, ema50, adx = arrs['ema9'], arrs['ema21'], arrs['ema50'], arrs['adx']
    if adx[i] < 22:
        return None
    cross_up   = ema9[i - 1] <= ema21[i - 1] and ema9[i] > ema21[i]
    cross_down = ema9[i - 1] >= ema21[i - 1] and ema9[i] < ema21[i]
    slope_up   = ema50[i] > ema50[i - 3]
    slope_down = ema50[i] < ema50[i - 3]
    if cross_up and slope_up:
        return 'buy'
    if cross_down and slope_down:
        return 'sell'
    return None

def sr_blocks(signal, price, res_level, sup_level, buffer=SR_BUFFER):
    """Return True if the S/R filter should block this signal."""
    if signal == 'buy' and res_level > 0:
        if price >= res_level * (1 - buffer):
            return True
    if signal == 'sell' and sup_level > 0:
        if price <= sup_level * (1 + buffer):
            return True
    return False

# ── Backtest single symbol ─────────────────────────────────
def backtest(symbol, candles, tf, use_sr, sr_lookup=None):
    if len(candles) < MIN_BARS:
        return []
    arrs = precompute_signal_arrays(candles)
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = arrs['highs']
    lows   = arrs['lows']
    closes = arrs['closes']
    max_bars = MAX_BARS_BY_TF.get(tf, 240)

    trades = []
    n = len(candles)
    i = 0
    while i < n - 1:
        sig = signal_at(arrs, i)
        if sig is None:
            i += 1
            continue

        if use_sr and sr_lookup:
            ts_list, res_at, sup_at = sr_lookup
            res_lvl, sup_lvl = sr_asof(ts_list, res_at, sup_at, ts_arr[i])
            if sr_blocks(sig, closes[i], res_lvl, sup_lvl):
                i += 1
                continue

        entry_idx = i + 1
        if entry_idx >= n:
            break
        raw_entry = opens[entry_idx]
        side = 'buy' if sig == 'buy' else 'sell'
        if side == 'buy':
            entry_p = raw_entry * (1 + FEE + SLIP)
            tp_p = entry_p * (1 + TP_PCT)
            sl_p = entry_p * (1 - SL_PCT)
        else:
            entry_p = raw_entry * (1 - FEE - SLIP)
            tp_p = entry_p * (1 - TP_PCT)
            sl_p = entry_p * (1 + SL_PCT)

        exit_p = None
        exit_ts = None
        reason = None
        bars_held = 0
        j = entry_idx
        while j < n:
            bars_held = j - entry_idx + 1
            hi, lo = highs[j], lows[j]
            hit_sl = (lo <= sl_p) if side == 'buy' else (hi >= sl_p)
            hit_tp = (hi >= tp_p) if side == 'buy' else (lo <= tp_p)
            if hit_sl:
                exit_p = sl_p; exit_ts = ts_arr[j]; reason = 'sl'; break
            if hit_tp:
                exit_p = tp_p; exit_ts = ts_arr[j]; reason = 'tp'; break
            if bars_held >= max_bars:
                exit_p = closes[j]; exit_ts = ts_arr[j]; reason = 'max_hold'; break
            j += 1
        if exit_p is None:
            exit_p = closes[-1]; exit_ts = ts_arr[-1]; reason = 'end_of_data'
            bars_held = (n - 1) - entry_idx + 1

        if side == 'buy':
            gross = (exit_p - entry_p) / entry_p
        else:
            gross = (entry_p - exit_p) / entry_p
        net = gross - (FEE + SLIP) * 2

        notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
        pnl = notional * net

        trades.append({
            'symbol': symbol, 'side': side,
            'entry_ts': ts_arr[entry_idx], 'exit_ts': exit_ts,
            'entry_price': entry_p, 'exit_price': exit_p,
            'pnl': pnl, 'reason': reason, 'bars': bars_held,
        })

        i = j + 1 if reason != 'end_of_data' else n
    return trades

# ── Stats ───────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
                'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
                'sharpe_ratio': 0.0, 'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {}}

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gross_win = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    net_pnl = sum(t['pnl'] for t in trades)

    trades_sorted = sorted(trades, key=lambda t: t['exit_ts'])
    equity = CAPITAL
    peak = CAPITAL
    max_dd = 0.0
    for t in trades_sorted:
        equity += t['pnl']
        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    monthly = {}
    for t in trades_sorted:
        ym = datetime.utcfromtimestamp(t['exit_ts']).strftime('%Y-%m')
        m = monthly.setdefault(ym, {'pnl': 0.0, 'n': 0, 'w': 0})
        m['pnl'] += t['pnl']; m['n'] += 1
        if t['pnl'] > 0:
            m['w'] += 1

    monthly_returns = [v['pnl'] / CAPITAL for v in monthly.values()]
    if len(monthly_returns) >= 3:
        mean_r = sum(monthly_returns) / len(monthly_returns)
        var = sum((r - mean_r) ** 2 for r in monthly_returns) / (len(monthly_returns) - 1)
        std_r = sqrt(var)
        # guard against near-zero std (e.g. very few months / all-win streak)
        # producing an unstable, meaningless Sharpe spike
        sharpe = (mean_r / std_r) * sqrt(12) if std_r > 1e-9 else 0.0
    else:
        sharpe = 0.0

    per_coin = {}
    for t in trades:
        c = per_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0})
        c['pnl'] += t['pnl']; c['n'] += 1
        if t['pnl'] > 0:
            c['w'] += 1
    for c in per_coin.values():
        c['wr'] = round(100 * c['w'] / c['n'], 2) if c['n'] else 0.0

    return {
        'total': len(trades),
        'win_rate': round(100 * len(wins) / len(trades), 2),
        'profit_factor': round(gross_win / gross_loss, 3) if gross_loss > 0 else (float('inf') if gross_win > 0 else 0.0),
        'net_pnl': round(net_pnl, 2),
        'max_drawdown': round(100 * max_dd, 2),
        'avg_win': round(gross_win / len(wins), 3) if wins else 0.0,
        'avg_loss': round(gross_loss / len(losses), 3) if losses else 0.0,
        'expectancy': round(net_pnl / len(trades), 3),
        'sharpe_ratio': round(sharpe, 3),
        'longs': sum(1 for t in trades if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades if t['side'] == 'sell'),
        'monthly': monthly,
        'per_coin': per_coin,
    }

# ── Shard runner (1 shard = 1 variant) ─────────────────────
def run_shard(shard_idx):
    t0 = time.time()
    tf = TIMEFRAMES[shard_idx]
    use_sr = USE_SR[shard_idx]
    variant_name = f"{tf}{'_SR' if use_sr else ''}"

    all_trades = []
    with_data = []

    for symbol in ALL_SYMBOLS:
        candles = fetch_symbol(symbol, tf)
        sr_lookup = None
        if use_sr:
            if tf == '4h':
                sr_lookup = build_sr_lookup(candles)
            else:
                candles_4h = fetch_symbol(symbol, '4h')
                sr_lookup = build_sr_lookup(candles_4h)
        if len(candles) >= MIN_BARS:
            with_data.append(symbol)
            trades = backtest(symbol, candles, tf, use_sr, sr_lookup)
            all_trades.extend(trades)

    result = {
        'shard': shard_idx,
        'variant': variant_name,
        'timeframe': tf,
        'use_sr': use_sr,
        'symbols': ALL_SYMBOLS,
        'with_data': with_data,
        'trades': all_trades,
        'stats': stats(all_trades),
        'elapsed': round(time.time() - t0, 1),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(result, f)
    print(f"Shard {shard_idx} ({variant_name}): {len(all_trades)} trades, "
          f"{len(with_data)}/{len(ALL_SYMBOLS)} symbols, {result['elapsed']}s")

# ── Merge ───────────────────────────────────────────────────
def merge_shards():
    variants = {}
    for idx in range(NUM_SHARDS):
        path = f'shard_{idx}.json'
        try:
            with open(path) as f:
                d = json.load(f)
        except FileNotFoundError:
            print(f"WARNING: {path} missing, skipping")
            continue
        variants[d['variant']] = d

    report = {'generated': datetime.utcnow().isoformat(), 'variants': variants}
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f)

    lines = []
    lines.append("=" * 70)
    lines.append("G MAX — MULTI-VARIANT BACKTEST SUMMARY")
    lines.append(f"Period: {START_YM} to {END_YM}  |  Coins: {', '.join(ALL_SYMBOLS)}")
    lines.append(f"TP: {TP_PCT*100:.2f}%  SL: {SL_PCT*100:.2f}%  Leverage: {LEVERAGE}x  "
                 f"Capital: ${CAPITAL:.0f}  Risk/trade: {RISK_PCT*100:.1f}%")
    lines.append("=" * 70)
    lines.append("")

    header = f"{'Variant':<10}{'Trades':>8}{'WinRate':>10}{'PF':>8}{'NetPnL':>12}{'MaxDD%':>9}{'Sharpe':>9}{'Verdict':>12}"
    lines.append(header)
    lines.append("-" * len(header))

    ordered = sorted(variants.items(), key=lambda kv: (kv[1]['timeframe'], kv[1]['use_sr']))
    for name, d in ordered:
        s = d['stats']
        pf_disp = f"{s['profit_factor']:.2f}" if s['profit_factor'] != float('inf') else "inf"
        verdict = "USABLE" if (s['profit_factor'] >= 1.5 and s['win_rate'] >= 42) else "NOT USABLE"
        mark = "\u2705" if verdict == "USABLE" else "\u274c"
        lines.append(f"{name:<10}{s['total']:>8}{s['win_rate']:>9.2f}%{pf_disp:>8}"
                     f"{s['net_pnl']:>12.2f}{s['max_drawdown']:>9.2f}{s['sharpe_ratio']:>9.3f}"
                     f"  {mark} {verdict}")

    lines.append("")
    for name, d in ordered:
        s = d['stats']
        lines.append("-" * 70)
        lines.append(f"VARIANT: {name}  (timeframe={d['timeframe']}, S/R filter={d['use_sr']})")
        lines.append(f"  Symbols with data: {len(d['with_data'])}/{len(d['symbols'])}")
        lines.append(f"  Total trades: {s['total']}  (Longs: {s['longs']}, Shorts: {s['shorts']})")
        lines.append(f"  Win Rate: {s['win_rate']:.2f}%")
        pf_disp = f"{s['profit_factor']:.3f}" if s['profit_factor'] != float('inf') else "inf"
        lines.append(f"  Profit Factor: {pf_disp}")
        lines.append(f"  Net PnL: ${s['net_pnl']:.2f}")
        lines.append(f"  Max Drawdown: {s['max_drawdown']:.2f}%")
        lines.append(f"  Sharpe Ratio (annualized, monthly returns): {s['sharpe_ratio']:.3f}")
        lines.append(f"  Avg Win: ${s['avg_win']:.3f}  Avg Loss: ${s['avg_loss']:.3f}  Expectancy: ${s['expectancy']:.3f}")
        lines.append(f"  Per-coin breakdown:")
        for sym, c in sorted(s['per_coin'].items(), key=lambda kv: -kv[1]['pnl']):
            lines.append(f"    {sym:<10} trades={c['n']:<5} wr={c['wr']:>6.2f}%  pnl=${c['pnl']:.2f}")
        lines.append("")

    with open('backtest_summary.txt', 'w') as f:
        f.write('\n'.join(lines))
    print('\n'.join(lines))

# ── Entry point ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))
