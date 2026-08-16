"""
G Max — Tight TP/SL Backtest (ADX>=26, baseline vs TP1/SL5 vs TP0.5/SL2)
Volume filter ($3M/24h), 5x leverage, full 96-coin universe, 15m
Data: Binance USDT-M futures monthly kline archives (stdlib only)
Usage:
    python backtest.py <shard_idx 0-7>   # run one shard, all variants
    python backtest.py merge             # merge all 8 shards into final report
"""

import csv, io, json, sys, time, zipfile
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ── Coin universe (COINS_UNIVERSE from GMaxV1.py, 96 coins) ──
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

NUM_SHARDS = 8
WORKERS = 16

# ── Config ──
START_YM, END_YM = (2024, 7), (2026, 6)
TIMEFRAME = "15m"
CAPITAL = 100.0
MARGIN_USD = 1.0
LEVERAGE = 5
FEE = 0.0005
SLIP = 0.0002
MAX_BARS = 960
MIN_BARS = 100
ADX_MIN = 26                       # <- stricter, per this run
VOLUME_FILTER_USD = 3_000_000
BARS_PER_DAY = 96

# ── Variants: (name, tp_pct, sl_pct) ──
VARIANTS = [
    ('TP3_SL15',   0.030, 0.150),
    ('TP1_SL15',   0.010, 0.150),
    ('TP1.5_SL15', 0.015, 0.150),
    ('TP0.8_SL10', 0.008, 0.100),
]

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines/{sym}/{tf}/{sym}-{tf}-{y:04d}-{m:02d}.zip"


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


def fetch_month(symbol, year, month):
    url = BASE_URL.format(sym=symbol, tf=TIMEFRAME, y=year, m=month)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        return []

    out = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                reader = csv.reader(text)
                for row in reader:
                    if not row or row[0] in ("open_time", ""):
                        continue
                    try:
                        ts = int(row[0])
                        if ts > 10**14:      # microseconds -> ms
                            ts = ts // 1000
                        if ts > 10**11:      # ms -> seconds
                            ts = ts // 1000
                        o = float(row[1]); h = float(row[2])
                        l = float(row[3]); c = float(row[4])
                        v = float(row[5])
                        qv = float(row[7]) if len(row) > 7 else v * c
                        out.append((ts, o, h, l, c, v, qv))
                    except (ValueError, IndexError):
                        continue
    except zipfile.BadZipFile:
        return []
    return out


def fetch_symbol(symbol):
    months = month_range(START_YM, END_YM)
    all_rows = []
    for (y, m) in months:
        rows = fetch_month(symbol, y, m)
        all_rows.extend(rows)
    if not all_rows:
        return []
    dedup = {}
    for r in all_rows:
        dedup[r[0]] = r
    return [dedup[k] for k in sorted(dedup.keys())]


# ── Indicators ──
def ema_series(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r


def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 3:
        return [0.0] * n
    pdm = [0.0]; mdm = [0.0]; trs = [0.0]
    for i in range(1, n):
        up = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up if (up > down and up > 0) else 0.0)
        mdm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))

    def wilder_smooth(v, p):
        out = [0.0] * len(v)
        if len(v) <= p:
            return out
        s = sum(v[1:p+1])
        out[p] = s
        for i in range(p+1, len(v)):
            s = s - (s / p) + v[i]
            out[i] = s
        return out

    st = wilder_smooth(trs, period)
    sp = wilder_smooth(pdm, period)
    sm = wilder_smooth(mdm, period)

    dx = [0.0] * n
    warmup_end = period * 2
    for i in range(period, n):
        t = st[i]
        if t == 0:
            continue
        pdi = 100 * sp[i] / t
        mdi = 100 * sm[i] / t
        denom = pdi + mdi
        dx[i] = 100 * abs(pdi - mdi) / denom if denom else 0.0

    adx_out = [0.0] * n
    if n > warmup_end:
        first_adx = sum(dx[period:warmup_end]) / period if warmup_end > period else 0.0
        adx_out[warmup_end] = first_adx
        for i in range(warmup_end + 1, n):
            adx_out[i] = (adx_out[i-1] * (period - 1) + dx[i]) / period
    return adx_out


def compute_series(closes, highs, lows):
    e9 = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)
    adx = adx_series(highs, lows, closes, 14)
    return e9, e21, e50, adx


def signal_at(i, closes, e9, e21, e50, adx):
    if i < 60 or i - 10 < 0:
        return None
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100 if e50[i-10] != 0 else 0.0
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
    if i >= len(adx) or adx[i] < ADX_MIN:
        return None
    return 'buy' if crossed_up else 'sell'


def has_volume(qvols, i, bars_per_day=BARS_PER_DAY, min_usd=VOLUME_FILTER_USD):
    start = max(0, i - bars_per_day + 1)
    window = qvols[start:i+1]
    if not window:
        return False
    return sum(window) >= min_usd


def find_signals(symbol, candles):
    """Compute indicators + volume-gated signals ONCE per symbol (shared across all variants)."""
    if len(candles) < MIN_BARS:
        return []
    ts = [c[0] for c in candles]
    opens = [c[1] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    qvols = [c[6] for c in candles]

    e9, e21, e50, adx = compute_series(closes, highs, lows)
    n = len(candles)

    signals = []  # list of (bar_idx, side)
    i = MIN_BARS
    while i < n - 1:
        if has_volume(qvols, i):
            sig = signal_at(i, closes, e9, e21, e50, adx)
            if sig:
                signals.append((i, sig))
        i += 1
    return signals, (ts, opens, highs, lows, closes)


def backtest_variant(signals, arrays, tp_pct, sl_pct, symbol):
    """Simulate ONE TP/SL variant against a precomputed signal list, respecting
    'no new entry while in position' by skipping signals inside an open trade window."""
    ts, opens, highs, lows, closes = arrays
    n = len(closes)
    trades = []
    next_free_bar = 0

    for (sig_bar, sig) in signals:
        entry_idx = sig_bar + 1
        if entry_idx <= 0 or entry_idx >= n:
            continue
        if entry_idx < next_free_bar:
            continue  # still in a position from a prior signal

        raw_open = opens[entry_idx]
        if sig == 'buy':
            entry_p = raw_open * (1 + FEE + SLIP)
            tp_p = entry_p * (1 + tp_pct)
            sl_p = entry_p * (1 - sl_pct)
        else:
            entry_p = raw_open * (1 - FEE - SLIP)
            tp_p = entry_p * (1 - tp_pct)
            sl_p = entry_p * (1 + sl_pct)

        notional = min(MARGIN_USD * LEVERAGE, CAPITAL * LEVERAGE)

        exit_p = None; exit_reason = None; exit_idx = None; bars_held = 0
        j = entry_idx
        while j < n:
            bars_held = j - entry_idx
            if j > entry_idx:
                hi = highs[j]; lo = lows[j]
                if sig == 'buy':
                    hit_sl = lo <= sl_p
                    hit_tp = hi >= tp_p
                else:
                    hit_sl = hi >= sl_p
                    hit_tp = lo <= tp_p
                if hit_sl:
                    exit_p = sl_p; exit_reason = 'sl'; exit_idx = j; break
                if hit_tp:
                    exit_p = tp_p; exit_reason = 'tp'; exit_idx = j; break
            if bars_held >= MAX_BARS:
                exit_p = closes[j]; exit_reason = 'max_hold'; exit_idx = j; break
            j += 1

        if exit_p is None:
            exit_p = closes[n-1]; exit_reason = 'end_of_data'; exit_idx = n - 1

        if sig == 'buy':
            exit_p_adj = exit_p * (1 - FEE - SLIP)
            gross = (exit_p_adj - entry_p) / entry_p
        else:
            exit_p_adj = exit_p * (1 + FEE + SLIP)
            gross = (entry_p - exit_p_adj) / entry_p

        pnl = notional * gross
        trades.append({
            'symbol': symbol, 'side': sig,
            'entry_ts': ts[entry_idx], 'exit_ts': ts[exit_idx],
            'entry_price': entry_p, 'exit_price': exit_p_adj,
            'pnl': round(pnl, 6), 'reason': exit_reason, 'bars': bars_held,
        })
        next_free_bar = exit_idx + 1

    return trades


def backtest_all_variants(symbol, candles):
    """Returns {variant_name: [trades]} for this symbol."""
    result = find_signals(symbol, candles)
    if not result:
        return {name: [] for name, _, _ in VARIANTS}
    signals, arrays = result
    out = {}
    for name, tp_pct, sl_pct in VARIANTS:
        out[name] = backtest_variant(signals, arrays, tp_pct, sl_pct, symbol)
    return out


# ── Stats ──
def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
            'max_drawdown': 0.0, 'max_drawdown_pct': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0,
            'expectancy': 0.0, 'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }
    trades_sorted = sorted(trades, key=lambda t: t['entry_ts'])
    gp = gl = 0.0
    wins = losses = 0
    longs = shorts = 0
    equity = 0.0; peak = 0.0; max_dd = 0.0
    monthly = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    per_coin = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})

    for t in trades_sorted:
        pnl = t['pnl']
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        if pnl > 0: gp += pnl; wins += 1
        elif pnl < 0: gl += abs(pnl); losses += 1
        if t['side'] == 'buy': longs += 1
        else: shorts += 1
        ym = time.strftime('%Y-%m', time.gmtime(t['entry_ts']))
        monthly[ym]['pnl'] += pnl; monthly[ym]['n'] += 1
        if pnl > 0: monthly[ym]['w'] += 1
        sym = t['symbol']
        per_coin[sym]['pnl'] += pnl; per_coin[sym]['n'] += 1
        if pnl > 0: per_coin[sym]['w'] += 1

    total = wins + losses
    net_pnl = gp - gl
    pf = (gp / gl) if gl > 0 else (float('inf') if gp > 0 else 0.0)
    avg_win = gp / wins if wins else 0.0
    avg_loss = gl / losses if losses else 0.0
    win_rate = (wins / total * 100) if total else 0.0
    expectancy = (net_pnl / total) if total else 0.0
    dd_pct = (max_dd / CAPITAL * 100) if CAPITAL else 0.0

    per_coin_out = {sym: {'pnl': round(d['pnl'],4), 'n': d['n'], 'w': d['w'],
                           'wr': round((d['w']/d['n']*100) if d['n'] else 0.0, 2)}
                     for sym, d in per_coin.items()}
    monthly_out = {ym: {'pnl': round(d['pnl'],4), 'n': d['n'], 'w': d['w']}
                    for ym, d in sorted(monthly.items())}

    return {
        'total': total, 'win_rate': round(win_rate, 2),
        'profit_factor': round(pf, 4) if pf != float('inf') else pf,
        'net_pnl': round(net_pnl, 4), 'max_drawdown': round(max_dd, 4),
        'max_drawdown_pct': round(dd_pct, 2),
        'avg_win': round(avg_win, 4), 'avg_loss': round(avg_loss, 4),
        'expectancy': round(expectancy, 4), 'longs': longs, 'shorts': shorts,
        'monthly': monthly_out, 'per_coin': per_coin_out,
    }


# ── Shard runner ──
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    t0 = time.time()
    variant_trades = {name: [] for name, _, _ in VARIANTS}
    with_data = []

    def work(sym):
        candles = fetch_symbol(sym)
        if len(candles) < MIN_BARS:
            return sym, None
        vt = backtest_all_variants(sym, candles)
        return sym, vt

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(work, s): s for s in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                sym2, vt = fut.result()
                if vt is not None:
                    with_data.append(sym2)
                    for name in variant_trades:
                        variant_trades[name].extend(vt[name])
                    total_trades = sum(len(v) for v in vt.values())
                    print(f"[shard {shard_idx}] {sym2}: {total_trades} trades across {len(VARIANTS)} variants")
                else:
                    print(f"[shard {shard_idx}] {sym2}: no data")
            except Exception as e:
                print(f"[shard {shard_idx}] {sym} ERROR: {e}")

    out = {
        'shard': shard_idx, 'symbols': symbols, 'with_data': with_data,
        'variant_trades': variant_trades, 'elapsed': time.time() - t0,
    }
    with open(f"shard_{shard_idx}.json", "w") as f:
        json.dump(out, f)
    print(f"[shard {shard_idx}] done in {out['elapsed']:.1f}s — {len(with_data)}/{len(symbols)} symbols with data")


# ── Merge ──
def merge_shards():
    variant_trades = {name: [] for name, _, _ in VARIANTS}
    all_symbols = []
    all_with_data = []
    for i in range(NUM_SHARDS):
        try:
            with open(f"shard_{i}.json") as f:
                d = json.load(f)
            all_symbols.extend(d['symbols'])
            all_with_data.extend(d['with_data'])
            for name in variant_trades:
                variant_trades[name].extend(d['variant_trades'][name])
        except FileNotFoundError:
            print(f"WARNING: shard_{i}.json missing")

    if not all_with_data:
        print("ERROR: 0 symbols returned data across all shards. Aborting.")
        with open("backtest_report.json", "w") as f:
            json.dump({'error': 'no data', 'symbols_attempted': all_symbols}, f)
        with open("backtest_summary.txt", "w") as f:
            f.write("ERROR: 0 symbols returned data. Check geo-blocking / data source.\n")
        return

    report = {
        'config': {
            'timeframe': TIMEFRAME, 'start': START_YM, 'end': END_YM,
            'capital': CAPITAL, 'margin_usd': MARGIN_USD, 'leverage': LEVERAGE,
            'adx_min': ADX_MIN, 'volume_filter_usd': VOLUME_FILTER_USD,
            'fee': FEE, 'slip': SLIP, 'max_bars': MAX_BARS,
            'variants': [{'name': n, 'tp_pct': tp, 'sl_pct': sl} for n, tp, sl in VARIANTS],
        },
        'symbols_attempted': len(all_symbols),
        'symbols_with_data': len(all_with_data),
        'variant_stats': {},
    }

    lines = []
    lines.append("=" * 70)
    lines.append("G MAX TP/SL BACKTEST — BASELINE vs TP1/SL15 vs TP1.5/SL15 vs TP0.8/SL10")
    lines.append("=" * 70)
    lines.append(f"Period: {START_YM} to {END_YM}  |  Timeframe: {TIMEFRAME}")
    lines.append(f"Symbols attempted: {len(all_symbols)}  |  With data: {len(all_with_data)}")
    lines.append(f"Capital: ${CAPITAL}  |  Margin/trade: ${MARGIN_USD}  |  Leverage: {LEVERAGE}x")
    lines.append(f"ADX filter: >= {ADX_MIN}")
    lines.append(f"Volume filter: >= ${VOLUME_FILTER_USD:,.0f} trailing 24h quote volume")
    lines.append("")
    lines.append("-" * 70)
    lines.append("VARIANT COMPARISON (sorted by Profit Factor)")
    lines.append("-" * 70)
    lines.append(f"{'Variant':12s} {'TP%':>5s} {'SL%':>5s} {'Trades':>7s} {'WR%':>7s} {'PF':>7s} {'NetPnL':>10s} {'DD':>8s} {'DD%':>6s} {'Loss:Win':>9s}")

    variant_summaries = []
    for name, tp_pct, sl_pct in VARIANTS:
        s = stats(variant_trades[name])
        report['variant_stats'][name] = s
        pf_disp = s['profit_factor'] if s['profit_factor'] != float('inf') else 999.0
        ratio = (s['avg_loss'] / s['avg_win']) if s['avg_win'] else 0.0
        variant_summaries.append((name, tp_pct, sl_pct, s, pf_disp, ratio))

    variant_summaries.sort(key=lambda x: x[4], reverse=True)
    for name, tp_pct, sl_pct, s, pf_disp, ratio in variant_summaries:
        pf_str = f"{s['profit_factor']:.3f}" if s['profit_factor'] != float('inf') else "inf"
        lines.append(f"{name:12s} {tp_pct*100:5.1f} {sl_pct*100:5.1f} {s['total']:7d} {s['win_rate']:7.2f} {pf_str:>7s} "
                      f"${s['net_pnl']:9.2f} ${s['max_drawdown']:7.2f} {s['max_drawdown_pct']:5.1f}% {ratio:8.2f}x")

    lines.append("")
    lines.append("-" * 70)
    lines.append("DETAIL PER VARIANT")
    lines.append("-" * 70)
    for name, tp_pct, sl_pct, s, pf_disp, ratio in variant_summaries:
        lines.append("")
        lines.append(f"### {name} (TP {tp_pct*100:.1f}% / SL {sl_pct*100:.1f}%) ###")
        lines.append(f"Trades: {s['total']}  |  WR: {s['win_rate']}%  |  PF: {s['profit_factor']}")
        lines.append(f"Net PnL: ${s['net_pnl']}  |  Max DD: ${s['max_drawdown']} ({s['max_drawdown_pct']}%)")
        lines.append(f"Avg win: ${s['avg_win']}  |  Avg loss: ${s['avg_loss']}  |  Loss:Win ratio: {ratio:.2f}x")
        lines.append(f"Expectancy/trade: ${s['expectancy']}  |  Longs/Shorts: {s['longs']}/{s['shorts']}")
        ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
        top5 = ranked[:5]
        lines.append("Top 5 coins: " + ", ".join(f"{sym}(${d['pnl']:.2f})" for sym, d in top5))

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n".join(lines))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx 0-7 | merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))

