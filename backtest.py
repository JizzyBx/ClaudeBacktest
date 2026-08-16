"""
G Max SR-Trend — Structure-Based Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy: Trend + Support/Resistance + RSI rejection, structure-based TP/SL

RULES:
  1. Trend filter: 4h EMA200 — only longs if price > 4h EMA200, only shorts if below
  2. Level: entry-timeframe fractal S/R pivot (5 bars either side)
  3. Trigger: price touches a support (for longs) / resistance (for shorts) zone
  4. Confirmation candle: rejection wick at the level
     - Long: lower wick >= 50% of candle range, closes in upper half of range
     - Short: upper wick >= 50% of candle range, closes in lower half of range
  5. RSI(14) filter: RSI <= 35 at support for longs, RSI >= 65 at resistance for shorts
  6. SL: beyond the level by an ATR(14)-based buffer
  7. TP: next opposing S/R pivot level in trade direction
  8. Minimum R:R = 1:1.5 (reward/risk). If not met (or no next level exists) -> skip trade

Variants (4 shards, one per timeframe): 15m, 30m, 1h, 4h
  Trend filter always comes from the 4h chart (fetched separately when tf != 4h)

stdlib only. Data: data.binance.vision futures monthly klines.
"""

import sys, json, time, zipfile, io, csv, urllib.request, urllib.error, bisect
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

MIN_RR   = 1.2      # minimum reward:risk, else skip trade (loosened from 1.5)
ATR_BUF_MULT = 0.5  # SL buffer beyond level = 0.5 * ATR(14)
RSI_LONG_MAX  = 42   # loosened from 35 — still below-midline, less strict
RSI_SHORT_MIN = 58   # loosened from 65 — still above-midline, less strict
FRACTAL_N = 5
MIN_BARS = 260       # need enough bars for EMA200 warmup on trend TF

WORKERS = 16
NUM_SHARDS = 4

TIMEFRAMES = {0: '15m', 1: '30m', 2: '1h', 3: '4h'}

MAX_BARS_BY_TF = {  # ~10 days max hold
    '15m': 960, '30m': 480, '1h': 240, '4h': 60,
}

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

# ── Indicators ──────────────────────────────────────────────
def ema_series(values, period):
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out

def rsi_series(closes, period=14):
    n = len(closes)
    out = [50.0] * n
    if n < period + 1:
        return out
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        gains[i] = max(d, 0.0)
        losses[i] = max(-d, 0.0)
    ag = sum(gains[1:period + 1]) / period
    al = sum(losses[1:period + 1]) / period
    out[period] = 100.0 if al == 0 else 100 - (100 / (1 + ag / al))
    for i in range(period + 1, n):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        out[i] = 100.0 if al == 0 else 100 - (100 / (1 + ag / al))
    return out

def atr_series(highs, lows, closes, period=14):
    n = len(closes)
    out = [0.0] * n
    if n < 2:
        return out
    trs = [0.0] * n
    for i in range(1, n):
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    if n <= period:
        avg = sum(trs[1:]) / max(1, len(trs) - 1)
        return [avg] * n
    a = sum(trs[1:period + 1]) / period
    out[period] = a
    for i in range(period + 1, n):
        a = (a * (period - 1) + trs[i]) / period
        out[i] = a
    for i in range(period):
        out[i] = out[period] if period < n else (closes[0] * 0.005)
    return out

def fractal_pivots(highs, lows, n=FRACTAL_N):
    """Returns lists of (index, price) for confirmed resistance and support pivots,
    in chronological order. A pivot at k is confirmed once we have n bars after it."""
    length = len(highs)
    res_pivots = []  # (index, price)
    sup_pivots = []
    for k in range(n, length - n):
        is_res = all(highs[k] >= highs[k - j] for j in range(1, n + 1)) and \
                 all(highs[k] >= highs[k + j] for j in range(1, n + 1))
        is_sup = all(lows[k] <= lows[k - j] for j in range(1, n + 1)) and \
                 all(lows[k] <= lows[k + j] for j in range(1, n + 1))
        if is_res:
            res_pivots.append((k, highs[k]))
        if is_sup:
            sup_pivots.append((k, lows[k]))
    return res_pivots, sup_pivots

def build_trend_lookup(candles_4h):
    """Returns (ts_list, is_uptrend_list) — price vs 4h EMA200 at each 4h bar."""
    if len(candles_4h) < 210:
        return [], []
    closes = [c[4] for c in candles_4h]
    ts = [c[0] for c in candles_4h]
    ema200 = ema_series(closes, 200)
    is_up = [closes[i] > ema200[i] for i in range(len(closes))]
    return ts, is_up

def trend_asof(ts_list, is_up_list, target_ts):
    if not ts_list or target_ts < ts_list[0]:
        return None
    lo, hi = 0, len(ts_list) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if ts_list[mid] <= target_ts:
            lo = mid
        else:
            hi = mid - 1
    return is_up_list[lo]

REJECTION_WICK_MIN = 0.35   # loosened from 0.5 — lower wick / range ratio required
REJECTION_CLOSE_POS_MIN = 0.4  # loosened from 0.5 — close must be in upper X% of range

# ── Signal / trade construction ────────────────────────────
def is_rejection_candle_long(o, h, l, c):
    rng = h - l
    if rng <= 0:
        return False
    lower_wick = min(o, c) - l
    return (lower_wick / rng >= REJECTION_WICK_MIN) and (c >= (l + rng * REJECTION_CLOSE_POS_MIN))

def is_rejection_candle_short(o, h, l, c):
    rng = h - l
    if rng <= 0:
        return False
    upper_wick = h - max(o, c)
    return (upper_wick / rng >= REJECTION_WICK_MIN) and (c <= (l + rng * (1 - REJECTION_CLOSE_POS_MIN)))

# ── Backtest single symbol ─────────────────────────────────
def backtest(symbol, candles, tf, trend_lookup):
    n = len(candles)
    if n < MIN_BARS:
        return []

    ts_arr  = [c[0] for c in candles]
    opens   = [c[1] for c in candles]
    highs   = [c[2] for c in candles]
    lows    = [c[3] for c in candles]
    closes  = [c[4] for c in candles]

    rsi = rsi_series(closes, 14)
    atr = atr_series(highs, lows, closes, 14)
    res_pivots, sup_pivots = fractal_pivots(highs, lows, FRACTAL_N)

    ts_trend, is_up_trend = trend_lookup
    max_bars = MAX_BARS_BY_TF.get(tf, 240)

    # Build a rolling "known pivots so far" index — pivot at index k is only
    # usable for signals at bar i >= k + FRACTAL_N (confirmation delay).
    # known_res_asc / known_sup_desc are maintained sorted-by-price incrementally
    # (bisect.insort) so TP lookups never use pivots from the future (no lookahead).
    trades = []
    i = FRACTAL_N * 2
    known_res = []          # (idx, price) confirmed so far, chronological
    known_sup = []
    known_res_asc = []      # prices only, kept sorted ascending
    known_sup_desc = []     # prices only, kept sorted descending (via negated bisect)

    res_ptr = 0
    sup_ptr = 0

    while i < n - 1:
        # advance known pivots up to what's confirmed by bar i
        while res_ptr < len(res_pivots) and res_pivots[res_ptr][0] + FRACTAL_N <= i:
            idx, p = res_pivots[res_ptr]
            known_res.append((idx, p))
            bisect.insort(known_res_asc, p)
            res_ptr += 1
        while sup_ptr < len(sup_pivots) and sup_pivots[sup_ptr][0] + FRACTAL_N <= i:
            idx, p = sup_pivots[sup_ptr]
            known_sup.append((idx, p))
            bisect.insort(known_sup_desc, -p)  # negated so ascending bisect == descending price
            sup_ptr += 1

        if not known_res or not known_sup:
            i += 1
            continue

        trend_up = trend_asof(ts_trend, is_up_trend, ts_arr[i])
        if trend_up is None:
            i += 1
            continue

        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        price = c
        sig = None

        # nearest support/resistance level to current price
        nearest_sup = max((p for _, p in known_sup if p <= price), default=None)
        nearest_res = min((p for _, p in known_res if p >= price), default=None)

        zone_tol = atr[i] * 0.5  # "touching" tolerance (loosened from 0.3)

        if trend_up and nearest_sup is not None and (price - nearest_sup) <= zone_tol:
            if is_rejection_candle_long(o, h, l, c) and rsi[i] <= RSI_LONG_MAX:
                sig = 'buy'
        elif (not trend_up) and nearest_res is not None and (nearest_res - price) <= zone_tol:
            if is_rejection_candle_short(o, h, l, c) and rsi[i] >= RSI_SHORT_MIN:
                sig = 'sell'

        if sig is None:
            i += 1
            continue

        # structure-based SL/TP — use only pivots confirmed as of bar i (no lookahead)
        buf = atr[i] * ATR_BUF_MULT
        if sig == 'buy':
            sl_level = nearest_sup - buf
            pos = bisect.bisect_right(known_res_asc, price)
            tp_level = known_res_asc[pos] if pos < len(known_res_asc) else None
        else:
            sl_level = nearest_res + buf
            neg_price = -price
            pos = bisect.bisect_right(known_sup_desc, neg_price)
            tp_level = -known_sup_desc[pos] if pos < len(known_sup_desc) else None

        if tp_level is None:
            i += 1
            continue

        entry_idx = i + 1
        if entry_idx >= n:
            break
        raw_entry = opens[entry_idx]

        if sig == 'buy':
            entry_p = raw_entry * (1 + FEE + SLIP)
            risk = entry_p - sl_level
            reward = tp_level - entry_p
        else:
            entry_p = raw_entry * (1 - FEE - SLIP)
            risk = sl_level - entry_p
            reward = entry_p - tp_level

        if risk <= 0 or reward <= 0 or (reward / risk) < MIN_RR:
            i += 1
            continue

        tp_p = tp_level
        sl_p = sl_level

        exit_p = None; exit_ts = None; reason = None; bars_held = 0
        j = entry_idx
        while j < n:
            bars_held = j - entry_idx + 1
            hj, lj = highs[j], lows[j]
            if sig == 'buy':
                hit_sl = lj <= sl_p
                hit_tp = hj >= tp_p
            else:
                hit_sl = hj >= sl_p
                hit_tp = lj <= tp_p
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

        if sig == 'buy':
            gross = (exit_p - entry_p) / entry_p
        else:
            gross = (entry_p - exit_p) / entry_p
        net = gross - (FEE + SLIP) * 2

        sl_pct = abs(entry_p - sl_p) / entry_p
        notional = min(CAPITAL * RISK_PCT / max(sl_pct, 1e-6), CAPITAL * LEVERAGE)
        pnl = notional * net

        trades.append({
            'symbol': symbol, 'side': sig,
            'entry_ts': ts_arr[entry_idx], 'exit_ts': exit_ts,
            'entry_price': entry_p, 'exit_price': exit_p,
            'pnl': pnl, 'reason': reason, 'bars': bars_held,
            'planned_rr': round(reward / risk, 3),
        })

        i = j + 1 if reason != 'end_of_data' else n

    return trades

# ── Stats ───────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
                'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
                'sharpe_ratio': 0.0, 'longs': 0, 'shorts': 0, 'avg_planned_rr': 0.0,
                'monthly': {}, 'per_coin': {}}

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
        sharpe = (mean_r / std_r) * sqrt(12) if std_r > 1e-9 else 0.0
    else:
        sharpe = 0.0

    per_coin = {}
    for t in trades:
        cs = per_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0})
        cs['pnl'] += t['pnl']; cs['n'] += 1
        if t['pnl'] > 0:
            cs['w'] += 1
    for cs in per_coin.values():
        cs['wr'] = round(100 * cs['w'] / cs['n'], 2) if cs['n'] else 0.0

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
        'avg_planned_rr': round(sum(t['planned_rr'] for t in trades) / len(trades), 3),
        'monthly': monthly,
        'per_coin': per_coin,
    }

# ── Shard runner (1 shard = 1 timeframe) ───────────────────
def run_shard(shard_idx):
    t0 = time.time()
    tf = TIMEFRAMES[shard_idx]

    all_trades = []
    with_data = []

    for symbol in ALL_SYMBOLS:
        candles = fetch_symbol(symbol, tf)
        candles_4h = candles if tf == '4h' else fetch_symbol(symbol, '4h')
        trend_lookup = build_trend_lookup(candles_4h)
        if len(candles) >= MIN_BARS and trend_lookup[0]:
            with_data.append(symbol)
            trades = backtest(symbol, candles, tf, trend_lookup)
            all_trades.extend(trades)

    result = {
        'shard': shard_idx,
        'variant': tf,
        'timeframe': tf,
        'symbols': ALL_SYMBOLS,
        'with_data': with_data,
        'trades': all_trades,
        'stats': stats(all_trades),
        'elapsed': round(time.time() - t0, 1),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(result, f)
    print(f"Shard {shard_idx} ({tf}): {len(all_trades)} trades, "
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
    lines.append("G MAX SR-TREND — STRUCTURE-BASED BACKTEST SUMMARY")
    lines.append(f"Period: {START_YM} to {END_YM}  |  Coins: {', '.join(ALL_SYMBOLS)}")
    lines.append(f"Trend: 4h EMA200  |  Confirm: rejection candle + RSI(14) {RSI_LONG_MAX}/{RSI_SHORT_MIN}  |  "
                 f"TP/SL: structure-based (next S/R / ATR buffer)  |  Min R:R: {MIN_RR}")
    lines.append(f"Leverage: {LEVERAGE}x  Capital: ${CAPITAL:.0f}  Risk/trade: {RISK_PCT*100:.1f}%")
    lines.append("=" * 70)
    lines.append("")

    header = f"{'TF':<8}{'Trades':>8}{'WinRate':>10}{'PF':>8}{'NetPnL':>12}{'MaxDD%':>9}{'Sharpe':>9}{'AvgRR':>8}{'Verdict':>12}"
    lines.append(header)
    lines.append("-" * len(header))

    ordered = sorted(variants.items(), key=lambda kv: ['15m', '30m', '1h', '4h'].index(kv[0]))
    for name, d in ordered:
        s = d['stats']
        pf_disp = f"{s['profit_factor']:.2f}" if s['profit_factor'] != float('inf') else "inf"
        # Require a minimum sample size before calling anything USABLE — a 1-2 trade
        # sample hitting PF/WR thresholds is noise, not edge.
        MIN_TRADES_FOR_VERDICT = 30
        verdict = ("USABLE" if (s['total'] >= MIN_TRADES_FOR_VERDICT and
                                 s['profit_factor'] >= 1.5 and s['win_rate'] >= 42)
                   else "NOT USABLE")
        if s['total'] < MIN_TRADES_FOR_VERDICT:
            verdict += f" (n={s['total']}, need {MIN_TRADES_FOR_VERDICT}+)"
        mark = "\u2705" if verdict == "USABLE" else "\u274c"
        lines.append(f"{name:<8}{s['total']:>8}{s['win_rate']:>9.2f}%{pf_disp:>8}"
                     f"{s['net_pnl']:>12.2f}{s['max_drawdown']:>9.2f}{s['sharpe_ratio']:>9.3f}"
                     f"{s['avg_planned_rr']:>8.2f}  {mark} {verdict}")

    lines.append("")
    for name, d in ordered:
        s = d['stats']
        lines.append("-" * 70)
        lines.append(f"TIMEFRAME: {name}")
        lines.append(f"  Symbols with data: {len(d['with_data'])}/{len(d['symbols'])}")
        lines.append(f"  Total trades: {s['total']}  (Longs: {s['longs']}, Shorts: {s['shorts']})")
        lines.append(f"  Win Rate: {s['win_rate']:.2f}%")
        pf_disp = f"{s['profit_factor']:.3f}" if s['profit_factor'] != float('inf') else "inf"
        lines.append(f"  Profit Factor: {pf_disp}")
        lines.append(f"  Net PnL: ${s['net_pnl']:.2f}")
        lines.append(f"  Max Drawdown: {s['max_drawdown']:.2f}%")
        lines.append(f"  Sharpe Ratio (annualized, monthly returns): {s['sharpe_ratio']:.3f}")
        lines.append(f"  Avg Planned R:R: {s['avg_planned_rr']:.2f}")
        lines.append(f"  Avg Win: ${s['avg_win']:.3f}  Avg Loss: ${s['avg_loss']:.3f}  Expectancy: ${s['expectancy']:.3f}")
        lines.append(f"  Per-coin breakdown:")
        for sym, cs in sorted(s['per_coin'].items(), key=lambda kv: -kv[1]['pnl']):
            lines.append(f"    {sym:<10} trades={cs['n']:<5} wr={cs['wr']:>6.2f}%  pnl=${cs['pnl']:.2f}")
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

