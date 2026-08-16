"""
G Max VAR_D Backtest — with $3M/day volume filter, 5x leverage, full 96-coin universe
Strategy: EMA50 slope filter + EMA9/21 crossover + ADX(14)>=22, TP 3.0% / SL 15.0%, max hold 10d (960 bars @15m)
Data: Binance USDT-M futures monthly kline archives (stdlib only, no pip installs)
Usage:
    python backtest.py <shard_idx 0-7>   # run one shard
    python backtest.py merge             # merge all 8 shards into final report
"""

import csv, io, json, math, sys, time, zipfile
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
MARGIN_USD = 1.0           # fixed margin per trade
LEVERAGE = 5
FEE = 0.0005                # 0.05% per side
SLIP = 0.0002               # 0.02% per side
TP_PCT = 0.030
SL_PCT = 0.150
MAX_BARS = 960               # 10 days @ 15m
MIN_BARS = 100                # warmup needed before signals valid
ADX_MIN = 22
VOLUME_FILTER_USD = 3_000_000   # min 24h quote volume required to trade that day
BARS_PER_DAY = 96                # 15m bars in 24h

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
                        if ts > 10**14:
                            ts = ts // 1000
                        o = float(row[1]); h = float(row[2])
                        l = float(row[3]); c = float(row[4])
                        v = float(row[5])          # base volume
                        qv = float(row[7]) if len(row) > 7 else v * c   # quote volume
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
    sorted_rows = [dedup[k] for k in sorted(dedup.keys())]
    return sorted_rows


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

    adx_out = [0.0] * n
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

    # ADX = Wilder-smoothed DX, valid only after warmup_end (3x period from series start)
    if n > warmup_end:
        first_adx = sum(dx[period:warmup_end]) / period if warmup_end > period else 0.0
        adx_out[warmup_end] = first_adx
        for i in range(warmup_end + 1, n):
            adx_out[i] = (adx_out[i-1] * (period - 1) + dx[i]) / period
    return adx_out


# ── Signal (evaluated on closed bar i, entry at bar i+1 open) ──
def compute_series(closes, highs, lows):
    e9 = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)
    adx = adx_series(highs, lows, closes, 14)
    return e9, e21, e50, adx


def signal_at(i, closes, e9, e21, e50, adx):
    """i = last closed bar index. Requires i >= MIN_BARS-1 and i-10 >= 0."""
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
    """Trailing 24h (96 bars) quote volume ending at bar i must be >= threshold."""
    start = max(0, i - bars_per_day + 1)
    window = qvols[start:i+1]
    if not window:
        return False
    return sum(window) >= min_usd


# ── Backtest single symbol ──
def backtest(symbol, candles):
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

    trades = []
    i = MIN_BARS
    in_pos = False
    while i < n - 1:
        if not in_pos:
            if not has_volume(qvols, i):
                i += 1
                continue
            sig = signal_at(i, closes, e9, e21, e50, adx)
            if sig is None:
                i += 1
                continue
            entry_idx = i + 1
            if entry_idx >= n:
                break
            raw_open = opens[entry_idx]
            if sig == 'buy':
                entry_p = raw_open * (1 + FEE + SLIP)
                tp_p = entry_p * (1 + TP_PCT)
                sl_p = entry_p * (1 - SL_PCT)
            else:
                entry_p = raw_open * (1 - FEE - SLIP)
                tp_p = entry_p * (1 - TP_PCT)
                sl_p = entry_p * (1 + SL_PCT)

            notional = min(MARGIN_USD * LEVERAGE, CAPITAL * LEVERAGE)
            qty = notional / entry_p if entry_p > 0 else 0.0

            exit_p = None
            exit_reason = None
            exit_idx = None
            bars_held = 0
            j = entry_idx
            while j < n:
                bars_held = j - entry_idx
                if j > entry_idx:  # can't exit on entry bar itself
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
            else:
                pass

            if exit_p is None:
                exit_p = closes[n-1]
                exit_reason = 'end_of_data'
                exit_idx = n - 1

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
                'pnl': round(pnl, 6), 'reason': exit_reason,
                'bars': bars_held,
            })
            i = exit_idx + 1
        else:
            i += 1
    return trades


# ── Stats ──
def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
            'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }
    trades_sorted = sorted(trades, key=lambda t: t['entry_ts'])
    gp = gl = 0.0
    wins = losses = 0
    longs = shorts = 0
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    monthly = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    per_coin = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})

    for t in trades_sorted:
        pnl = t['pnl']
        equity += pnl
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

        if pnl > 0:
            gp += pnl; wins += 1
        elif pnl < 0:
            gl += abs(pnl); losses += 1

        if t['side'] == 'buy':
            longs += 1
        else:
            shorts += 1

        ym = time.strftime('%Y-%m', time.gmtime(t['entry_ts']))
        monthly[ym]['pnl'] += pnl
        monthly[ym]['n'] += 1
        if pnl > 0:
            monthly[ym]['w'] += 1

        sym = t['symbol']
        per_coin[sym]['pnl'] += pnl
        per_coin[sym]['n'] += 1
        if pnl > 0:
            per_coin[sym]['w'] += 1

    total = wins + losses
    net_pnl = gp - gl
    pf = (gp / gl) if gl > 0 else (float('inf') if gp > 0 else 0.0)
    avg_win = gp / wins if wins else 0.0
    avg_loss = gl / losses if losses else 0.0
    win_rate = (wins / total * 100) if total else 0.0
    expectancy = (net_pnl / total) if total else 0.0

    per_coin_out = {}
    for sym, d in per_coin.items():
        wr = (d['w'] / d['n'] * 100) if d['n'] else 0.0
        per_coin_out[sym] = {'pnl': round(d['pnl'], 4), 'n': d['n'], 'w': d['w'], 'wr': round(wr, 2)}

    monthly_out = {ym: {'pnl': round(d['pnl'], 4), 'n': d['n'], 'w': d['w']} for ym, d in sorted(monthly.items())}

    dd_pct = (max_dd / CAPITAL * 100) if CAPITAL else 0.0

    return {
        'total': total, 'win_rate': round(win_rate, 2),
        'profit_factor': round(pf, 4) if pf != float('inf') else pf,
        'net_pnl': round(net_pnl, 4),
        'max_drawdown': round(max_dd, 4), 'max_drawdown_pct': round(dd_pct, 2),
        'avg_win': round(avg_win, 4), 'avg_loss': round(avg_loss, 4),
        'expectancy': round(expectancy, 4),
        'longs': longs, 'shorts': shorts,
        'monthly': monthly_out, 'per_coin': per_coin_out,
    }


# ── Shard runner ──
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    t0 = time.time()
    all_trades = []
    with_data = []

    def work(sym):
        candles = fetch_symbol(sym)
        if len(candles) < MIN_BARS:
            return sym, []
        trades = backtest(sym, candles)
        return sym, trades

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(work, s): s for s in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                sym2, trades = fut.result()
                if trades or sym2 in symbols:
                    pass
                if trades:
                    with_data.append(sym2)
                all_trades.extend(trades)
                print(f"[shard {shard_idx}] {sym2}: {len(trades)} trades")
            except Exception as e:
                print(f"[shard {shard_idx}] {sym} ERROR: {e}")

    shard_stats = stats(all_trades)
    out = {
        'shard': shard_idx, 'symbols': symbols, 'with_data': with_data,
        'trades': all_trades, 'stats': shard_stats, 'elapsed': time.time() - t0,
    }
    with open(f"shard_{shard_idx}.json", "w") as f:
        json.dump(out, f)
    print(f"[shard {shard_idx}] done in {out['elapsed']:.1f}s — {len(all_trades)} trades from {len(with_data)}/{len(symbols)} symbols")


# ── Merge ──
def merge_shards():
    all_trades = []
    all_symbols = []
    all_with_data = []
    for i in range(NUM_SHARDS):
        try:
            with open(f"shard_{i}.json") as f:
                d = json.load(f)
            all_trades.extend(d['trades'])
            all_symbols.extend(d['symbols'])
            all_with_data.extend(d['with_data'])
        except FileNotFoundError:
            print(f"WARNING: shard_{i}.json missing")

    if not all_with_data:
        print("ERROR: 0 symbols returned data across all shards — likely geo-blocked or data source issue. Aborting.")
        with open("backtest_report.json", "w") as f:
            json.dump({'error': 'no data', 'symbols_attempted': all_symbols}, f)
        with open("backtest_summary.txt", "w") as f:
            f.write("ERROR: 0 symbols returned data. Check geo-blocking / data source.\n")
        return

    agg = stats(all_trades)
    report = {
        'config': {
            'strategy': 'G Max VAR_D + volume filter',
            'timeframe': TIMEFRAME, 'start': START_YM, 'end': END_YM,
            'capital': CAPITAL, 'margin_usd': MARGIN_USD, 'leverage': LEVERAGE,
            'tp_pct': TP_PCT, 'sl_pct': SL_PCT, 'max_bars': MAX_BARS,
            'adx_min': ADX_MIN, 'volume_filter_usd': VOLUME_FILTER_USD,
            'fee': FEE, 'slip': SLIP,
        },
        'symbols_attempted': len(all_symbols),
        'symbols_with_data': len(all_with_data),
        'stats': agg,
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Summary text
    lines = []
    lines.append("=" * 60)
    lines.append("G MAX VAR_D BACKTEST — VOLUME FILTER + 5x LEVERAGE")
    lines.append("=" * 60)
    lines.append(f"Period: {START_YM} to {END_YM}  |  Timeframe: {TIMEFRAME}")
    lines.append(f"Symbols attempted: {len(all_symbols)}  |  With data: {len(all_with_data)}")
    lines.append(f"Capital: ${CAPITAL}  |  Margin/trade: ${MARGIN_USD}  |  Leverage: {LEVERAGE}x")
    lines.append(f"TP: {TP_PCT*100:.1f}%  SL: {SL_PCT*100:.1f}%  Max hold: {MAX_BARS} bars")
    lines.append(f"ADX filter: >= {ADX_MIN} (bug-fixed, properly warmed up)")
    lines.append(f"Volume filter: coin must have >= ${VOLUME_FILTER_USD:,.0f} trailing 24h quote volume to trade")
    lines.append("")
    lines.append("-" * 60)
    lines.append("AGGREGATE RESULTS")
    lines.append("-" * 60)
    s = agg
    lines.append(f"Total trades:     {s['total']}")
    lines.append(f"Win rate:         {s['win_rate']}%")
    pf_disp = s['profit_factor'] if s['profit_factor'] != float('inf') else 'inf'
    lines.append(f"Profit factor:    {pf_disp}")
    lines.append(f"Net PnL:          ${s['net_pnl']}")
    lines.append(f"Max drawdown:     ${s['max_drawdown']} ({s.get('max_drawdown_pct', 0)}%)")
    lines.append(f"Avg win:          ${s['avg_win']}")
    lines.append(f"Avg loss:         ${s['avg_loss']}")
    lines.append(f"Expectancy/trade: ${s['expectancy']}")
    lines.append(f"Longs / Shorts:   {s['longs']} / {s['shorts']}")
    lines.append("")

    pf_val = s['profit_factor'] if s['profit_factor'] != float('inf') else 999
    usable = pf_val >= 1.5 and s['win_rate'] >= 42
    lines.append("RECOMMENDATION: " + ("✅ USABLE" if usable else "❌ NOT USABLE") + " (target: PF>=1.5, WR>=42%)")
    lines.append("")

    lines.append("-" * 60)
    lines.append("TOP 50 COINS BY NET PNL")
    lines.append("-" * 60)
    ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
    for sym, d in ranked[:50]:
        lines.append(f"{sym:20s} trades={d['n']:4d}  wr={d['wr']:6.2f}%  pnl=${d['pnl']:.4f}")
    lines.append("")

    lines.append("-" * 60)
    lines.append("MONTHLY PNL")
    lines.append("-" * 60)
    for ym, d in s['monthly'].items():
        lines.append(f"{ym}: pnl=${d['pnl']:.4f}  trades={d['n']}  wins={d['w']}")

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

