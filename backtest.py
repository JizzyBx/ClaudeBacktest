"""
G Max V1 — ADX Variant Sweep Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tests 36 ADX filter variants (6 thresholds x 3 periods x rising-ADX on/off)
against the SAME fixed pipeline as live Strategy G VAR_D:
  EMA50 slope filter -> EMA9/21 crossover -> ADX gate (varied)
  TP 3.0% | SL 15.0% | Max hold 960 bars (10d) | 15m | Universe coins

Everything except the ADX gate is byte-identical to GMaxV1.py's check_signal_G().
Candles are fetched ONCE per coin and reused across all 36 variants (fast).

stdlib only. Run:
  python backtest.py <shard_idx>   # 0..7
  python backtest.py merge
"""

import csv, io, json, sys, time, zipfile, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
NUM_SHARDS = 8
WORKERS    = 16

START_YM = (2025, 8)
END_YM   = (2026, 7)
TIMEFRAME = "15m"

CAPITAL   = 10000.0
RISK_PCT  = 0.0075     # 0.75% of capital risked per trade
FEE       = 0.0005     # 0.05% taker
SLIP      = 0.0002     # 0.02% slippage
LEVERAGE  = 5

TP_PCT   = 0.030
SL_PCT   = 0.150
MAX_BARS = 960
MIN_BARS = 70

# ── ADX variant matrix ──────────────────────────────────────
ADX_THRESHOLDS   = [18, 20, 22, 25, 28, 30]
ADX_PERIODS      = [10, 14, 21]
ADX_RISING_MODES = [False, True]   # True = require adx[i] > adx[i-3]
RISING_LOOKBACK  = 3

def build_variants():
    variants = []
    for period in ADX_PERIODS:
        for thresh in ADX_THRESHOLDS:
            for rising in ADX_RISING_MODES:
                vid = f"P{period}_T{thresh}_{'RISE' if rising else 'FLAT'}"
                variants.append({'id': vid, 'period': period, 'thresh': thresh, 'rising': rising})
    return variants

VARIANTS = build_variants()   # 36 entries

# ══════════════════════════════════════════════════════════════
# COIN UNIVERSE (from GMaxV1.py COINS_UNIVERSE)
# ══════════════════════════════════════════════════════════════
ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT','AIOTUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ANKRUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT','COMBOUSDT',
    'COMMONUSDT','CRCLUSDT','CUSDT','DAMUSDT','DEFIUSDT','DEXEUSDT','DIAUSDT',
    'DMCUSDT','EIGENUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT',
    'EVAAUSDT','FLNCUSDT','FLUXUSDT','FUNUSDT','FXSUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','IPUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','MTLUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT',
    'PEOPLEUSDT','PIPPINUSDT','PIXELUSDT','PLUMEUSDT','POLUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','PUNDIXUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT',
    'SEIUSDT','SIGNUSDT','SKRUSDT','SNDKUSDT','SOMIUSDT','SPELLUSDT',
    'SPKUSDT','STABLEUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VANRYUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT','WLDUSDT',
    'XEMUSDT','XLMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

# ══════════════════════════════════════════════════════════════
# DATA FETCH — Binance public monthly klines archive
# ══════════════════════════════════════════════════════════════
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines/{sym}/{tf}/{sym}-{tf}-{y:04d}-{m:02d}.zip"

def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out

def fetch_month(symbol, year, month):
    url = BASE_URL.format(sym=symbol, tf=TIMEFRAME, y=year, m=month)
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
                    if not row or row[0] in ('open_time', 'open time'):
                        continue
                    try:
                        ts = int(float(row[0]))
                        o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
                    except (ValueError, IndexError):
                        continue
                    if ts > 10**14:
                        ts = int(ts / 1000)
                    rows.append((ts, o, h, l, c))
        return rows
    except urllib.error.HTTPError:
        return []
    except Exception:
        return []

def fetch_symbol(symbol):
    months = month_range(START_YM, END_YM)
    all_rows = []
    for (y, m) in months:
        all_rows.extend(fetch_month(symbol, y, m))
    if not all_rows:
        return []
    dedup = {}
    for r in all_rows:
        dedup[r[0]] = r
    return [dedup[k] for k in sorted(dedup.keys())]

# ══════════════════════════════════════════════════════════════
# INDICATORS (identical math to GMaxV1.py)
# ══════════════════════════════════════════════════════════════
def ema(values, period):
    k = 2.0 / (period + 1); r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_series_calc(highs, lows, closes, period):
    """Returns the full ADX series (not just last value) so we can check 'rising'."""
    n = len(closes)
    if n < period * 3:
        return []
    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        up = highs[i] - highs[i-1]; down = lows[i-1] - lows[i]
        pdm.append(up if up > down and up > 0 else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))

    def ws(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1]/p + x)
        return r

    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st:
        return []
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period:
        return []
    adx_vals = [sum(dx[:period]) / period]
    for d in dx[period:]:
        adx_vals.append((adx_vals[-1]*(period-1) + d) / period)
    # adx_vals[k] corresponds to closes index (1 + period + period - 1 + k) roughly;
    # we only need relative "rising" comparisons and the latest value, so we align
    # by padding to closes length for safe indexing from the end.
    pad = n - len(adx_vals)
    return [None]*pad + adx_vals

# ══════════════════════════════════════════════════════════════
# SIGNAL — fixed pipeline (EMA50 slope + EMA9/21 cross), ADX gate varied
# ══════════════════════════════════════════════════════════════
def precompute_emas(closes):
    return ema(closes, 9), ema(closes, 21), ema(closes, 50)

def base_trigger(closes, e9, e21, e50, i):
    """Returns ('buy'|'sell'|None). Identical logic to live check_signal_G filters 1+2."""
    if i < 10:
        return None
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None
    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if trend_up and crossed_up:
        return 'buy'
    if trend_down and crossed_down:
        return 'sell'
    return None

def adx_gate_pass(adx_series, i, period, thresh, rising):
    if i >= len(adx_series) or adx_series[i] is None:
        return False, 0.0
    val = adx_series[i]
    if val < thresh:
        return False, val
    if rising:
        j = i - RISING_LOOKBACK
        if j < 0 or adx_series[j] is None:
            return False, val
        if not (val > adx_series[j]):
            return False, val
    return True, val

# ══════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# ══════════════════════════════════════════════════════════════
def position_size(entry_price):
    notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
    return notional

def simulate_trade(side, entry_i, opens, highs, lows, closes, ts_list):
    entry_open = opens[entry_i]
    if side == 'buy':
        entry_p = entry_open * (1 + FEE + SLIP)
        tp_p = entry_p * (1 + TP_PCT)
        sl_p = entry_p * (1 - SL_PCT)
    else:
        entry_p = entry_open * (1 - FEE - SLIP)
        tp_p = entry_p * (1 - TP_PCT)
        sl_p = entry_p * (1 + SL_PCT)

    notional = position_size(entry_p)
    n = len(closes)
    bars_held = 0
    for j in range(entry_i, min(entry_i + MAX_BARS, n)):
        bars_held = j - entry_i + 1
        h, l = highs[j], lows[j]
        if side == 'buy':
            hit_sl = l <= sl_p
            hit_tp = h >= tp_p
        else:
            hit_sl = h >= sl_p
            hit_tp = l <= tp_p
        if hit_sl:
            exit_p = sl_p; reason = 'sl'
        elif hit_tp:
            exit_p = tp_p; reason = 'tp'
        else:
            continue
        if side == 'buy':
            exit_p_adj = exit_p * (1 - FEE - SLIP)
            gross = (exit_p_adj - entry_p) / entry_p
        else:
            exit_p_adj = exit_p * (1 + FEE + SLIP)
            gross = (entry_p - exit_p_adj) / entry_p
        pnl = notional * gross
        return {
            'side': side, 'entry_ts': ts_list[entry_i], 'exit_ts': ts_list[j],
            'entry_price': entry_p, 'exit_price': exit_p_adj, 'pnl': pnl,
            'reason': reason, 'bars': bars_held,
        }
    # max hold or end of data
    last_j = min(entry_i + MAX_BARS, n) - 1
    if last_j < entry_i:
        last_j = entry_i
    close_p = closes[last_j]
    if side == 'buy':
        exit_p_adj = close_p * (1 - FEE - SLIP)
        gross = (exit_p_adj - entry_p) / entry_p
    else:
        exit_p_adj = close_p * (1 + FEE + SLIP)
        gross = (entry_p - exit_p_adj) / entry_p
    pnl = notional * gross
    reason = 'max_hold' if (last_j - entry_i + 1) >= MAX_BARS else 'end_of_data'
    return {
        'side': side, 'entry_ts': ts_list[entry_i], 'exit_ts': ts_list[last_j],
        'entry_price': entry_p, 'exit_price': exit_p_adj, 'pnl': pnl,
        'reason': reason, 'bars': last_j - entry_i + 1,
    }

def backtest_symbol_all_variants(symbol, candles):
    """Run all 36 ADX variants against one candle set. Returns {variant_id: [trades]}."""
    if len(candles) < MIN_BARS:
        return {v['id']: [] for v in VARIANTS}

    ts_list = [c[0] for c in candles]
    opens   = [c[1] for c in candles]
    highs   = [c[2] for c in candles]
    lows    = [c[3] for c in candles]
    closes  = [c[4] for c in candles]
    n = len(closes)

    e9, e21, e50 = precompute_emas(closes)

    # precompute base trigger signal per bar (independent of ADX)
    base_sig = [None] * n
    for i in range(n):
        base_sig[i] = base_trigger(closes, e9, e21, e50, i)

    # precompute ADX series per period (only 3 periods needed, reused across thresholds)
    adx_by_period = {p: adx_series_calc(highs, lows, closes, p) for p in ADX_PERIODS}

    results = {}
    for variant in VARIANTS:
        vid = variant['id']
        adx_series = adx_by_period[variant['period']]
        trades = []
        i = MIN_BARS
        in_position_until = -1
        while i < n - 1:
            if i <= in_position_until:
                i += 1
                continue
            sig = base_sig[i]
            if sig is not None:
                ok, _ = adx_gate_pass(adx_series, i, variant['period'], variant['thresh'], variant['rising'])
                if ok:
                    entry_i = i + 1
                    if entry_i < n:
                        trade = simulate_trade(sig, entry_i, opens, highs, lows, closes, ts_list)
                        trade['symbol'] = symbol
                        trades.append(trade)
                        exit_ts = trade['exit_ts']
                        # find bar index for exit_ts to resume scanning after it
                        try:
                            exit_idx = ts_list.index(exit_ts, entry_i)
                        except ValueError:
                            exit_idx = n
                        in_position_until = exit_idx
            i += 1
        results[vid] = trades
    return results

# ══════════════════════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════════════════════
def compute_stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
            'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0,
        }
    wins = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    net_pnl = sum(t['pnl'] for t in trades)
    pf = (gross_win / gross_loss) if gross_loss > 0 else (float('inf') if gross_win > 0 else 0.0)
    win_rate = len(wins) / len(trades) * 100

    # max drawdown from cumulative equity curve, sorted by exit time
    sorted_trades = sorted(trades, key=lambda t: t['exit_ts'])
    equity = 0.0; peak = 0.0; max_dd = 0.0
    for t in sorted_trades:
        equity += t['pnl']
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    return {
        'total': len(trades),
        'win_rate': round(win_rate, 2),
        'profit_factor': round(pf, 3) if pf != float('inf') else 999.0,
        'net_pnl': round(net_pnl, 2),
        'max_drawdown': round(max_dd, 2),
        'avg_win': round(sum(wins)/len(wins), 2) if wins else 0.0,
        'avg_loss': round(sum(losses)/len(losses), 2) if losses else 0.0,
        'expectancy': round(net_pnl/len(trades), 4),
        'longs': sum(1 for t in trades if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades if t['side'] == 'sell'),
    }

# ══════════════════════════════════════════════════════════════
# SHARD RUNNER
# ══════════════════════════════════════════════════════════════
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    t0 = time.time()
    print(f"[shard {shard_idx}] {len(symbols)} symbols: {symbols}")

    fetched = {}
    with_data = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, s): s for s in symbols}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                candles = fut.result()
            except Exception as e:
                print(f"[shard {shard_idx}] fetch error {s}: {e}")
                candles = []
            if candles and len(candles) >= MIN_BARS:
                fetched[s] = candles
                with_data.append(s)
            else:
                print(f"[shard {shard_idx}] {s}: insufficient data ({len(candles)} bars)")

    if not with_data:
        print(f"[shard {shard_idx}] ERROR: 0 symbols returned data. Possible geo-block or archive issue.")

    variant_trades = defaultdict(list)
    for s in with_data:
        try:
            per_variant = backtest_symbol_all_variants(s, fetched[s])
        except Exception as e:
            print(f"[shard {shard_idx}] backtest error {s}: {e}")
            continue
        for vid, trades in per_variant.items():
            variant_trades[vid].extend(trades)

    out = {
        'shard': shard_idx,
        'symbols': symbols,
        'with_data': with_data,
        'variant_trades': {vid: trades for vid, trades in variant_trades.items()},
        'elapsed': time.time() - t0,
    }
    fname = f"shard_{shard_idx}.json"
    with open(fname, 'w') as f:
        json.dump(out, f)
    print(f"[shard {shard_idx}] done in {out['elapsed']:.1f}s, {len(with_data)}/{len(symbols)} symbols had data")

# ══════════════════════════════════════════════════════════════
# MERGE
# ══════════════════════════════════════════════════════════════
def merge_shards():
    all_variant_trades = defaultdict(list)
    all_with_data = set()
    all_symbols = set()
    total_elapsed = 0.0

    for idx in range(NUM_SHARDS):
        fname = f"shard_{idx}.json"
        try:
            with open(fname) as f:
                shard = json.load(f)
        except FileNotFoundError:
            print(f"WARNING: {fname} missing, skipping")
            continue
        all_symbols.update(shard['symbols'])
        all_with_data.update(shard['with_data'])
        total_elapsed += shard.get('elapsed', 0.0)
        for vid, trades in shard['variant_trades'].items():
            all_variant_trades[vid].extend(trades)

    if not all_with_data:
        print("ERROR: no shard returned any data. Aborting merge — check geo-block / archive access.")
        with open('backtest_summary.txt', 'w') as f:
            f.write("ERROR: 0 symbols returned data across all shards.\n")
        with open('backtest_report.json', 'w') as f:
            json.dump({'error': 'no_data'}, f)
        return

    # compute stats per variant
    variant_results = {}
    for variant in VARIANTS:
        vid = variant['id']
        trades = all_variant_trades.get(vid, [])
        stats = compute_stats(trades)
        variant_results[vid] = {
            'period': variant['period'],
            'thresh': variant['thresh'],
            'rising': variant['rising'],
            'stats': stats,
        }

    report = {
        'period_range': f"{START_YM} to {END_YM}",
        'timeframe': TIMEFRAME,
        'symbols_attempted': len(all_symbols),
        'symbols_with_data': len(all_with_data),
        'config': {
            'CAPITAL': CAPITAL, 'RISK_PCT': RISK_PCT, 'FEE': FEE, 'SLIP': SLIP,
            'LEVERAGE': LEVERAGE, 'TP_PCT': TP_PCT, 'SL_PCT': SL_PCT,
            'MAX_BARS': MAX_BARS, 'MIN_BARS': MIN_BARS,
            'rising_lookback': RISING_LOOKBACK,
        },
        'variants': variant_results,
        'elapsed_total_shard_seconds': round(total_elapsed, 1),
    }
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f)

    # ── comparison table, ranked by profit_factor desc ──
    ranked = sorted(
        variant_results.items(),
        key=lambda kv: (kv[1]['stats']['profit_factor'], kv[1]['stats']['win_rate']),
        reverse=True,
    )

    lines = []
    lines.append("=" * 100)
    lines.append("G MAX V1 — ADX VARIANT SWEEP BACKTEST")
    lines.append("=" * 100)
    lines.append(f"Period: {START_YM} to {END_YM}  |  Timeframe: {TIMEFRAME}  |  Universe coins")
    lines.append(f"Symbols attempted: {len(all_symbols)}  |  Symbols with data: {len(all_with_data)}")
    lines.append(f"Capital: ${CAPITAL:,.0f}  |  Risk/trade: {RISK_PCT*100:.2f}%  |  Leverage: {LEVERAGE}x")
    lines.append(f"Fee: {FEE*100:.3f}%  |  Slippage: {SLIP*100:.3f}%  |  TP: {TP_PCT*100:.1f}%  SL: {SL_PCT*100:.1f}%")
    lines.append(f"Fixed pipeline: EMA50 slope filter + EMA9/21 crossover (identical to live)")
    lines.append(f"Rising-ADX lookback: {RISING_LOOKBACK} bars")
    lines.append("")
    lines.append(f"{'RANK':<5}{'VARIANT':<20}{'PERIOD':<8}{'THRESH':<8}{'RISING':<8}"
                  f"{'TRADES':<9}{'WR%':<8}{'PF':<8}{'NET_PNL':<12}{'MAX_DD':<10}{'EXP':<9}")
    lines.append("-" * 100)
    for rank, (vid, v) in enumerate(ranked, 1):
        s = v['stats']
        lines.append(
            f"{rank:<5}{vid:<20}{v['period']:<8}{v['thresh']:<8}{str(v['rising']):<8}"
            f"{s['total']:<9}{s['win_rate']:<8}{s['profit_factor']:<8}"
            f"{s['net_pnl']:<12}{s['max_drawdown']:<10}{s['expectancy']:<9}"
        )
    lines.append("")

    baseline = variant_results.get('P14_T22_FLAT')
    if baseline:
        bs = baseline['stats']
        lines.append("-" * 100)
        lines.append(f"BASELINE (current live config P14_T22_FLAT): trades={bs['total']} "
                      f"WR={bs['win_rate']}% PF={bs['profit_factor']} net_pnl={bs['net_pnl']} "
                      f"max_dd={bs['max_drawdown']}")
        lines.append("-" * 100)

    lines.append("")
    lines.append(f"Total shard compute time: {total_elapsed:.1f}s")
    lines.append("=" * 100)

    with open('backtest_summary.txt', 'w') as f:
        f.write("\n".join(lines))

    print("\n".join(lines[:20]))
    print(f"... full comparison table for all {len(VARIANTS)} variants written to backtest_summary.txt")

# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx>|merge")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))

