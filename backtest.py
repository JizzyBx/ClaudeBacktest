"""
G Max — Multi-Timeframe Backtest (15m / 1H / 4H)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Runs the SAME strategy logic (EMA50 slope + EMA9/21 cross + ADX>=22 filter,
TP 3% / SL 15%) across three timeframes to compare which one the bot should
actually trade on. Full 117-coin universe, 2-year lookback (auto-shrinks per
coin to whatever history exists — no padding), 5x leverage.

Usage on GitHub Actions:
    python backtest.py <shard_idx>   # 0-7, runs one shard for ALL 3 timeframes
    python backtest.py merge         # merges all shards, writes final report

stdlib only.
"""
import json, time, sys, math, urllib.request, zipfile, io, csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────────
START_YM   = (2024, 8)     # 2 years back from ~Aug 2026
END_YM     = (2026, 7)
TIMEFRAMES = ['15m', '1h', '4h']

CAPITAL    = 1000.0
RISK_PCT   = 0.02          # 2% risk per trade
FEE        = 0.0004        # 0.04% taker
SLIP       = 0.0005        # 0.05% slippage
LEVERAGE   = 5

TP_PCT     = 0.030
SL_PCT     = 0.150
MIN_BARS   = 100           # warmup needed before signals are valid

# Effectively "no cap" — safety ceiling only, ~90 days per timeframe
MAX_BARS = {
    '15m': 90 * 24 * 4,    # 8640 bars
    '1h':  90 * 24,        # 2160 bars
    '4h':  90 * 6,         # 540 bars
}

NUM_SHARDS = 8
WORKERS    = 16

ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT','COMBOUSDT',
    'CRCLUSDT','DAMUSDT','DEFIUSDT','DIAUSDT',
    'DMCUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT',
    'FLNCUSDT','FLUXUSDT','FXSUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT',
    'PIXELUSDT','PLUMEUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT',
    'SKRUSDT','SOMIUSDT','SPELLUSDT',
    'SPKUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT',
    'XEMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

# ── Data fetch ───────────────────────────────────────────────
def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out

def fetch_month(symbol, year, month, timeframe):
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines/"
           f"{symbol}/{timeframe}/{symbol}-{timeframe}-{year:04d}-{month:02d}.zip")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        candles = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding='utf-8')
                for row in csv.reader(text):
                    if not row or row[0] in ('open_time',):
                        continue
                    ts = int(float(row[0]))
                    # Normalize to milliseconds. Binance archives are ms, but
                    # some symbol/date ranges use microseconds (16 digits).
                    if ts > 10**14:
                        ts = ts // 1000
                    o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
                    candles.append((ts, o, h, l, c))
        return candles
    except Exception:
        return []

def fetch_symbol(symbol, timeframe):
    months = month_range(START_YM, END_YM)
    all_candles = []
    for (y, m) in months:
        all_candles.extend(fetch_month(symbol, y, m, timeframe))
    if not all_candles:
        return []
    dedup = {}
    for c in all_candles:
        dedup[c[0]] = c
    rows = sorted(dedup.values(), key=lambda x: x[0])
    return rows

# ── Indicators (mirrors production GMaxV1.py exactly) ─────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i-1]; down = lows[i-1] - lows[i]
        pdm.append(up if up > down and up > 0 else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    def ws(v, p):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st: return 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]:
        adx = (adx*(period-1) + d) / period
    return max(0.0, min(100.0, adx))

# ── Signal (identical logic to check_signal_G, timeframe-agnostic) ─
def signal(i, closes, highs, lows, e9, e21, e50):
    if i < 10 or i >= len(closes):
        return None
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up = slope_pct > 0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None
    crossed_up = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if trend_up and not crossed_up:
        return None
    if trend_down and not crossed_down:
        return None
    adx_val = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_val < 22:
        return None
    return 'buy' if crossed_up else 'sell'

# ── Backtest single symbol ──────────────────────────────────
def backtest(symbol, candles, timeframe):
    if len(candles) < MIN_BARS:
        return []
    closes = [c[4] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    opens  = [c[1] for c in candles]
    ts_arr = [c[0] for c in candles]

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    trades = []
    in_pos = False
    side = None; entry_p = 0.0; entry_i = 0; entry_ts = 0
    max_bars = MAX_BARS[timeframe]

    i = MIN_BARS
    n = len(candles)
    while i < n - 1:
        if not in_pos:
            sig = signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                side = sig
                entry_i = i + 1
                if entry_i >= n:
                    break
                entry_p = opens[entry_i] * (1 + FEE + SLIP) if side == 'buy' \
                          else opens[entry_i] * (1 - FEE - SLIP)
                entry_ts = ts_arr[entry_i]
                in_pos = True
                i = entry_i
                continue
        else:
            bars_held = i - entry_i
            hi, lo, cl = highs[i], lows[i], closes[i]
            exit_p = None; reason = None
            if side == 'buy':
                sl_price = entry_p * (1 - SL_PCT)
                tp_price = entry_p * (1 + TP_PCT)
                if lo <= sl_price:
                    exit_p, reason = sl_price, 'sl'
                elif hi >= tp_price:
                    exit_p, reason = tp_price, 'tp'
            else:
                sl_price = entry_p * (1 + SL_PCT)
                tp_price = entry_p * (1 - TP_PCT)
                if hi >= sl_price:
                    exit_p, reason = sl_price, 'sl'
                elif lo <= tp_price:
                    exit_p, reason = tp_price, 'tp'

            if exit_p is None and bars_held >= max_bars:
                exit_p, reason = cl, 'max_hold'
            if exit_p is None and i == n - 2:
                exit_p, reason = cl, 'end_of_data'

            if exit_p is not None:
                if side == 'buy':
                    gross = (exit_p - entry_p) / entry_p
                else:
                    gross = (entry_p - exit_p) / entry_p
                net = gross - (FEE + SLIP) * 2
                notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
                pnl = notional * net
                trades.append({
                    'symbol': symbol, 'side': side,
                    'entry_ts': entry_ts, 'exit_ts': ts_arr[i],
                    'entry_price': entry_p, 'exit_price': exit_p,
                    'pnl': round(pnl, 4), 'reason': reason,
                    'bars': bars_held,
                })
                in_pos = False
        i += 1
    return trades

# ── Stats ────────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
                'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
                'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {}}
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gp = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))
    net_pnl = sum(t['pnl'] for t in trades)

    trades_sorted = sorted(trades, key=lambda t: t['exit_ts'])
    equity = 0.0; peak = 0.0; max_dd = 0.0
    monthly = {}
    for t in trades_sorted:
        equity += t['pnl']
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
        ym = datetime.fromtimestamp(t['exit_ts'] / 1000, tz=timezone.utc).strftime('%Y-%m')
        m = monthly.setdefault(ym, {'pnl': 0.0, 'n': 0, 'w': 0})
        m['pnl'] += t['pnl']; m['n'] += 1
        if t['pnl'] > 0: m['w'] += 1

    per_coin = {}
    for t in trades:
        d = per_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0})
        d['pnl'] += t['pnl']; d['n'] += 1
        if t['pnl'] > 0: d['w'] += 1
    for sym, d in per_coin.items():
        d['wr'] = round(d['w'] / d['n'] * 100, 2) if d['n'] else 0.0
        d['pnl'] = round(d['pnl'], 2)

    return {
        'total': len(trades),
        'win_rate': round(len(wins) / len(trades) * 100, 2),
        'profit_factor': round(gp / gl, 3) if gl > 0 else (0.0 if gp == 0 else float('inf')),
        'net_pnl': round(net_pnl, 2),
        'max_drawdown': round(max_dd, 2),
        'avg_win': round(gp / len(wins), 3) if wins else 0.0,
        'avg_loss': round(gl / len(losses), 3) if losses else 0.0,
        'expectancy': round(net_pnl / len(trades), 4),
        'longs': sum(1 for t in trades if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades if t['side'] == 'sell'),
        'monthly': monthly,
        'per_coin': per_coin,
    }

# ── Shard runner ─────────────────────────────────────────────
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    result = {'shard': shard_idx, 'symbols': symbols, 'timeframes': {}}

    for tf in TIMEFRAMES:
        t0 = time.time()
        with_data = []
        all_trades = []

        def work(sym):
            candles = fetch_symbol(sym, tf)
            if len(candles) >= MIN_BARS:
                trades = backtest(sym, candles, tf)
                return sym, len(candles), trades
            return sym, len(candles), []

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, s) for s in symbols]
            for fut in as_completed(futs):
                sym, n_candles, trades = fut.result()
                if n_candles >= MIN_BARS:
                    with_data.append(sym)
                all_trades.extend(trades)

        result['timeframes'][tf] = {
            'with_data': with_data,
            'trades': all_trades,
            'stats': stats(all_trades),
            'elapsed': round(time.time() - t0, 1),
        }
        print(f"shard {shard_idx} [{tf}]: {len(with_data)}/{len(symbols)} coins, "
              f"{len(all_trades)} trades, {result['timeframes'][tf]['elapsed']}s")

    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(result, f)

# ── Merge ────────────────────────────────────────────────────
def merge_shards():
    merged = {tf: {'with_data': [], 'trades': []} for tf in TIMEFRAMES}
    total_symbols_attempted = 0

    for i in range(NUM_SHARDS):
        try:
            with open(f'shard_{i}.json') as f:
                d = json.load(f)
        except FileNotFoundError:
            print(f"WARNING: shard_{i}.json missing")
            continue
        total_symbols_attempted += len(d['symbols'])
        for tf in TIMEFRAMES:
            td = d['timeframes'][tf]
            merged[tf]['with_data'].extend(td['with_data'])
            merged[tf]['trades'].extend(td['trades'])

    report = {}
    for tf in TIMEFRAMES:
        s = stats(merged[tf]['trades'])
        report[tf] = {
            'with_data_count': len(merged[tf]['with_data']),
            'symbols_attempted': total_symbols_attempted,
            'stats': s,
        }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    lines = []
    lines.append("=" * 70)
    lines.append("G MAX — MULTI-TIMEFRAME BACKTEST SUMMARY")
    lines.append(f"Period: {START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}")
    lines.append(f"Leverage: {LEVERAGE}x | TP: {TP_PCT*100:.1f}% | SL: {SL_PCT*100:.1f}%")
    lines.append(f"Coins attempted: {total_symbols_attempted}")
    lines.append("=" * 70)

    if all(report[tf]['with_data_count'] == 0 for tf in TIMEFRAMES):
        lines.append("\n❌ ERROR: 0 symbols returned data across all timeframes.")
        lines.append("Likely a geo-block on data.binance.vision from the runner region.")
        lines.append("Backtest aborted — no results to report.")
    else:
        for tf in TIMEFRAMES:
            r = report[tf]; s = r['stats']
            lines.append(f"\n── {tf.upper()} ──────────────────────────────")
            lines.append(f"Coins with data: {r['with_data_count']}/{r['symbols_attempted']}")
            lines.append(f"Total trades:    {s['total']}")
            lines.append(f"Win rate:        {s['win_rate']}%")
            lines.append(f"Profit factor:   {s['profit_factor']}")
            lines.append(f"Net PnL:         ${s['net_pnl']}")
            lines.append(f"Max drawdown:    ${s['max_drawdown']}")
            lines.append(f"Avg win / loss:  ${s['avg_win']} / ${s['avg_loss']}")
            lines.append(f"Expectancy:      ${s['expectancy']} per trade")
            lines.append(f"Longs / Shorts:  {s['longs']} / {s['shorts']}")
            usable = s['profit_factor'] >= 1.5 and s['win_rate'] >= 42
            lines.append(f"RECOMMENDATION:  {'✅ USABLE' if usable else '❌ NOT USABLE'} "
                         f"(needs PF>=1.5 and WR>=42%)")

            lines.append(f"\nTop 50 coins by net PnL ({tf}):")
            ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)[:50]
            for sym, d in ranked:
                lines.append(f"  {sym:<20} trades={d['n']:<4} wr={d['wr']:>6}%  pnl=${d['pnl']}")

            lines.append(f"\nMonthly PnL ({tf}):")
            for ym in sorted(s['monthly'].keys()):
                m = s['monthly'][ym]
                lines.append(f"  {ym}: pnl=${round(m['pnl'],2):<10} n={m['n']:<4} w={m['w']}")

        lines.append("\n" + "=" * 70)
        lines.append("VERDICT — which timeframe to trade:")
        best = max(TIMEFRAMES, key=lambda tf: report[tf]['stats']['profit_factor']
                   if report[tf]['stats']['total'] >= 30 else -1)
        lines.append(f"Highest profit factor with sufficient sample (30+ trades): {best.upper()}")
        lines.append("Review win rate, drawdown, and trade count together before deciding —")
        lines.append("a timeframe with very few trades can show an inflated PF by chance.")

    with open('backtest_summary.txt', 'w') as f:
        f.write('\n'.join(lines))
    print('\n'.join(lines))

# ── Entry point ──────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx 0-7 | merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

