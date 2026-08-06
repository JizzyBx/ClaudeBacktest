"""
G Max V1 — Backtest Pipeline
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy : Variant G VAR_D · 15m
           EMA50 slope filter + EMA9/21 crossover + ADX(14) >= 22
           TP: 3.0%  |  SL: 15.0%  |  Max hold: 960 bars (10 days)

Coins     : 117 (full Universe list from GMaxV1.py)
Period    : Aug 2023 – Jul 2025  (2 years)
Leverage  : 5x
Capital   : $10,000  |  Risk: 0.75% per trade
Shards    : 8 parallel GitHub Actions jobs × 16 I/O threads each

Usage:
  python backtest.py 0        # run shard 0
  python backtest.py merge    # merge all shard JSONs → final report
"""

import sys, json, csv, io, math, time, zipfile, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ── Coin list (117 coins — full Universe) ─────────────────────────────────────
ALL_SYMBOLS = [
    # Confirmed USDT-M futures perpetual coins from Binance archive
    # Removed spot-only / no futures archive: KITEUSDT, OPNUSDT, RLSUSDT, SOMIUSDT, AUSDT
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT','AIOTUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ANKRUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT','COMBOUSDT',
    'COMMONUSDT','CRCLUSDT','CUSDT','DAMUSDT','DEFIUSDT','DEXEUSDT','DIAUSDT',
    'DMCUSDT','EIGENUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT',
    'EVAAUSDT','FLNCUSDT','FLUXUSDT','FUNUSDT','FXSUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','IPUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','MTLUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','ORBSUSDT',
    'PEOPLEUSDT','PIPPINUSDT','PIXELUSDT','PLUMEUSDT','POLUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','PUNDIXUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT',
    'SEIUSDT','SIGNUSDT','SKRUSDT','SNDKUSDT','SPELLUSDT',
    'SPKUSDT','STABLEUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VANRYUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT','WLDUSDT',
    'XEMUSDT','XLMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

NUM_SHARDS = 8

# ── Config ────────────────────────────────────────────────────────────────────
START_YM   = (2023, 8)
END_YM     = (2025, 7)
TIMEFRAME  = '15m'

CAPITAL    = 10_000.0
RISK_PCT   = 0.0075     # 0.75% risk per trade
LEVERAGE   = 5
FEE        = 0.0005     # 0.05% maker fee each side
SLIP       = 0.0002     # 0.02% slippage each side

TP_PCT     = 0.030      # 3.0%
SL_PCT     = 0.150      # 15.0%
MAX_BARS   = 960        # 10 days × 96 bars/day
MIN_BARS   = 100        # EMA50 + 10 slope bars + ADX warmup

WORKERS    = 16         # I/O threads per shard

# ── Position sizing ───────────────────────────────────────────────────────────
# Risk-based notional capped by leverage limit
NOTIONAL = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)

# ── Data fetch ────────────────────────────────────────────────────────────────
BASE_URL = (
    'https://data.binance.vision/data/futures/um/monthly/klines'
    '/{symbol}/{tf}/{symbol}-{tf}-{yyyy}-{mm:02d}.zip'
)

def _month_range(start_ym, end_ym):
    y, m = start_ym
    while (y, m) <= end_ym:
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def fetch_month(symbol, year, month):
    url = BASE_URL.format(symbol=symbol, tf=TIMEFRAME, yyyy=year, mm=month)
    for attempt in range(2):   # 1 retry on network error, not on 404
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = resp.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                name = zf.namelist()[0]
                content = zf.read(name).decode('utf-8')
            rows = []
            for line in csv.reader(io.StringIO(content)):
                if not line or not line[0].isdigit():
                    continue
                ts = int(line[0])
                if ts > 10**14:   # microseconds guard
                    ts //= 1000
                o = float(line[1])
                h = float(line[2])
                l = float(line[3])
                c = float(line[4])
                rows.append((ts, o, h, l, c))
            return rows
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []   # coin not listed that month — fast skip, no retry
            print(f'[WARN] {symbol} {year}-{month:02d} HTTP {e.code}', flush=True)
            return []
        except Exception as e:
            if attempt == 0:
                time.sleep(1)   # brief pause then retry once
                continue
            print(f'[WARN] {symbol} {year}-{month:02d} {e}', flush=True)
            return []
    return []

def fetch_symbol(symbol):
    all_rows = []
    for y, m in _month_range(START_YM, END_YM):
        all_rows.extend(fetch_month(symbol, y, m))
    # deduplicate by timestamp, sort
    seen = {}
    for row in all_rows:
        seen[row[0]] = row
    return sorted(seen.values())

# ── Indicators (pure Python, exact copies from GMaxV1.py) ────────────────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i]   - highs[i-1]
        down = lows[i-1]  - lows[i]
        pdm.append(up   if up   > down and up   > 0 else 0.0)
        mdm.append(down if down > up   and down > 0 else 0.0)
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i]   - closes[i-1]),
            abs(lows[i]    - closes[i-1])
        ))
    def ws(v, p):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1] / p + x)
        return r
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st: return 0.0, 0.0, 0.0
    pdi = [100 * p / t if t else 0 for p, t in zip(sp, st)]
    mdi = [100 * m / t if t else 0 for m, t in zip(sm, st)]
    dx  = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0, pdi[-1], mdi[-1]
    adx_v = sum(dx[:period]) / period
    for d in dx[period:]: adx_v = (adx_v * (period - 1) + d) / period
    return max(0.0, min(100.0, adx_v)), pdi[-1], mdi[-1]

# ── Signal — Variant G (exact port from GMaxV1.py::check_signal_G) ────────────
def signal(i, closes, highs, lows):
    """
    Evaluate signal on bar i (last CLOSED bar).
    Returns 'buy', 'sell', or None.
    """
    if i < MIN_BARS:
        return None

    sub_c = closes[:i+1]
    sub_h = highs[:i+1]
    sub_l = lows[:i+1]

    e9  = ema(sub_c, 9)
    e21 = ema(sub_c, 21)
    e50 = ema(sub_c, 50)

    idx = len(sub_c) - 1   # = i relative to sub arrays

    # Guard: need at least 10 bars for slope
    if idx < 10:
        return None

    # Filter 1: EMA50 slope (10-bar lookback)
    slope_pct  = (e50[idx] - e50[idx - 10]) / e50[idx - 10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None

    # Filter 2: EMA9/21 crossover on the closed bar
    crossed_up   = e9[idx] > e21[idx] and e9[idx-1] <= e21[idx-1]
    crossed_down = e9[idx] < e21[idx] and e9[idx-1] >= e21[idx-1]
    if not crossed_up and not crossed_down:
        return None

    # Direction alignment
    if trend_up   and not crossed_up:   return None
    if trend_down and not crossed_down: return None

    # Filter 3: ADX(14) >= 22
    adx_v, _, _ = adx_calc(sub_h, sub_l, sub_c, 14)
    if adx_v < 22:
        return None

    return 'buy' if crossed_up else 'sell'

# ── Backtest single symbol ────────────────────────────────────────────────────
def backtest(symbol, candles):
    """
    Returns list of trade dicts.
    Standard entry/exit per handoff conventions.
    """
    if len(candles) < MIN_BARS + 2:
        return []

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    trades = []
    position = None   # no concurrent positions per symbol

    for i in range(MIN_BARS, len(candles) - 1):
        # ── Check open position ──────────────────────────────────────
        if position is not None:
            pos = position
            side = pos['side']
            bars_held = i - pos['entry_bar']

            # Check SL first (conservative), then TP
            if side == 'buy':
                sl_hit = lows[i] <= pos['sl_price']
                tp_hit = highs[i] >= pos['tp_price']
            else:
                sl_hit = highs[i] >= pos['sl_price']
                tp_hit = lows[i]  <= pos['tp_price']

            exit_reason = None
            if sl_hit:
                exit_reason = 'sl'
                exit_price  = pos['sl_price']
            elif tp_hit:
                exit_reason = 'tp'
                exit_price  = pos['tp_price']
            elif bars_held >= MAX_BARS:
                exit_reason = 'max_hold'
                exit_price  = closes[i]

            if exit_reason:
                # Net PnL (fee+slip on both legs)
                if side == 'buy':
                    gross = (exit_price - pos['entry_price']) / pos['entry_price']
                else:
                    gross = (pos['entry_price'] - exit_price) / pos['entry_price']
                net = gross - (FEE + SLIP) * 2
                pnl = NOTIONAL * net * LEVERAGE

                trades.append({
                    'symbol':      symbol,
                    'side':        side,
                    'entry_ts':    pos['entry_ts'],
                    'exit_ts':     ts_arr[i],
                    'entry_price': pos['entry_price'],
                    'exit_price':  exit_price,
                    'pnl':         round(pnl, 6),
                    'reason':      exit_reason,
                    'bars':        bars_held,
                })
                position = None
                continue

        # ── Check for new signal (only if flat) ──────────────────────
        if position is None:
            sig = signal(i, closes, highs, lows)
            if sig:
                # Entry on bar i+1 open
                raw_entry = opens[i + 1]
                if sig == 'buy':
                    entry_p  = raw_entry * (1 + FEE + SLIP)
                    tp_price = entry_p   * (1 + TP_PCT)
                    sl_price = entry_p   * (1 - SL_PCT)
                else:
                    entry_p  = raw_entry * (1 - FEE - SLIP)
                    tp_price = entry_p   * (1 - TP_PCT)
                    sl_price = entry_p   * (1 + SL_PCT)

                position = {
                    'side':        sig,
                    'entry_bar':   i + 1,
                    'entry_ts':    ts_arr[i + 1],
                    'entry_price': entry_p,
                    'tp_price':    tp_price,
                    'sl_price':    sl_price,
                }

    # Close any open position at end of data
    if position is not None:
        ep = closes[-1]
        pos = position
        side = pos['side']
        if side == 'buy':
            gross = (ep - pos['entry_price']) / pos['entry_price']
        else:
            gross = (pos['entry_price'] - ep) / pos['entry_price']
        net = gross - (FEE + SLIP) * 2
        pnl = NOTIONAL * net * LEVERAGE
        trades.append({
            'symbol':      symbol,
            'side':        side,
            'entry_ts':    pos['entry_ts'],
            'exit_ts':     ts_arr[-1],
            'entry_price': pos['entry_price'],
            'exit_price':  ep,
            'pnl':         round(pnl, 6),
            'reason':      'end_of_data',
            'bars':        len(candles) - 1 - pos['entry_bar'],
        })

    return trades

# ── Stats ─────────────────────────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
            'net_pnl': 0.0, 'max_drawdown': 0.0,
            'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
            'long_wr': 0.0, 'short_wr': 0.0,
            'tp_count': 0, 'sl_count': 0, 'max_hold_count': 0, 'eod_count': 0,
        }

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gross_profit = sum(t['pnl'] for t in wins)
    gross_loss   = abs(sum(t['pnl'] for t in losses))
    net_pnl      = sum(t['pnl'] for t in trades)

    pf = (gross_profit / gross_loss) if gross_loss > 0 else (float('inf') if gross_profit > 0 else 0.0)

    avg_win  = gross_profit / len(wins)   if wins   else 0.0
    avg_loss = gross_loss   / len(losses) if losses else 0.0
    win_rate = len(wins) / len(trades) * 100

    # Expectancy per trade
    expectancy = (win_rate / 100 * avg_win) - ((1 - win_rate / 100) * avg_loss)

    # Max drawdown (equity curve)
    equity = 0.0; peak = 0.0; max_dd = 0.0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        equity += t['pnl']
        if equity > peak: peak = equity
        dd = peak - equity
        if dd > max_dd: max_dd = dd

    # Long / short split
    longs  = [t for t in trades if t['side'] == 'buy']
    shorts = [t for t in trades if t['side'] == 'sell']
    long_wins  = [t for t in longs  if t['pnl'] > 0]
    short_wins = [t for t in shorts if t['pnl'] > 0]
    long_wr  = len(long_wins)  / len(longs)  * 100 if longs  else 0.0
    short_wr = len(short_wins) / len(shorts) * 100 if shorts else 0.0

    # Exit reason counts
    tp_count       = sum(1 for t in trades if t['reason'] == 'tp')
    sl_count       = sum(1 for t in trades if t['reason'] == 'sl')
    max_hold_count = sum(1 for t in trades if t['reason'] == 'max_hold')
    eod_count      = sum(1 for t in trades if t['reason'] == 'end_of_data')

    # Monthly breakdown
    monthly = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    for t in trades:
        # ms timestamp → YYYY-MM
        dt_s = t['exit_ts'] // 1000 if t['exit_ts'] > 10**11 else t['exit_ts']
        import time as _time
        lt = _time.gmtime(dt_s)
        key = f'{lt.tm_year}-{lt.tm_mon:02d}'
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        if t['pnl'] > 0:
            monthly[key]['w'] += 1

    # Per-coin breakdown
    per_coin = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0})
    for t in trades:
        sym = t['symbol']
        per_coin[sym]['pnl'] += t['pnl']
        per_coin[sym]['n']   += 1
        if t['pnl'] > 0:
            per_coin[sym]['w'] += 1
    for sym in per_coin:
        n = per_coin[sym]['n']
        per_coin[sym]['wr'] = round(per_coin[sym]['w'] / n * 100, 1) if n else 0.0

    return {
        'total':          len(trades),
        'win_rate':       round(win_rate, 2),
        'profit_factor':  round(pf, 4),
        'net_pnl':        round(net_pnl, 2),
        'max_drawdown':   round(max_dd, 2),
        'avg_win':        round(avg_win, 2),
        'avg_loss':       round(avg_loss, 2),
        'expectancy':     round(expectancy, 2),
        'longs':          len(longs),
        'shorts':         len(shorts),
        'long_wr':        round(long_wr, 2),
        'short_wr':       round(short_wr, 2),
        'tp_count':       tp_count,
        'sl_count':       sl_count,
        'max_hold_count': max_hold_count,
        'eod_count':      eod_count,
        'monthly':        dict(monthly),
        'per_coin':       dict(per_coin),
    }

# ── Shard runner ──────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    t0 = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f'[Shard {shard_idx}] {len(symbols)} symbols: {symbols}', flush=True)

    # Fetch all symbols in parallel
    candle_map = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, s): s for s in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                rows = fut.result()
                candle_map[sym] = rows
                print(f'[Shard {shard_idx}] {sym}: {len(rows)} candles', flush=True)
            except Exception as e:
                print(f'[Shard {shard_idx}] {sym} fetch error: {e}', flush=True)
                candle_map[sym] = []

    # Geo-block check
    all_empty = all(len(v) == 0 for v in candle_map.values())
    if all_empty:
        print(f'[Shard {shard_idx}] ERROR: All symbols returned 0 candles. '
              f'Possible geo-block or network issue. Aborting.', flush=True)
        sys.exit(1)

    # Backtest each symbol
    all_trades = []
    with_data  = []
    rejected   = []
    for sym in symbols:
        rows = candle_map.get(sym, [])
        if len(rows) < MIN_BARS + 2:
            rejected.append({'symbol': sym, 'reason': f'only {len(rows)} candles < {MIN_BARS+2}'})
            print(f'[Shard {shard_idx}] {sym}: SKIPPED ({len(rows)} candles)', flush=True)
            continue
        with_data.append(sym)
        trades = backtest(sym, rows)
        all_trades.extend(trades)
        print(f'[Shard {shard_idx}] {sym}: {len(trades)} trades', flush=True)

    shard_stats = stats(all_trades)
    elapsed = round(time.time() - t0, 1)

    result = {
        'shard':     shard_idx,
        'symbols':   symbols,
        'with_data': with_data,
        'rejected':  rejected,
        'trades':    all_trades,
        'stats':     shard_stats,
        'elapsed':   elapsed,
    }

    out_path = f'shard_{shard_idx}.json'
    with open(out_path, 'w') as f:
        json.dump(result, f)
    print(f'[Shard {shard_idx}] Done in {elapsed}s — {len(all_trades)} trades, '
          f'PF={shard_stats["profit_factor"]}, WR={shard_stats["win_rate"]}%', flush=True)

# ── Merge ─────────────────────────────────────────────────────────────────────
def merge_shards():
    all_trades   = []
    all_with_data = []
    all_rejected  = []
    total_symbols = []

    for idx in range(NUM_SHARDS):
        path = f'shard_{idx}.json'
        try:
            with open(path) as f:
                shard = json.load(f)
            all_trades.extend(shard['trades'])
            all_with_data.extend(shard.get('with_data', []))
            all_rejected.extend(shard.get('rejected', []))
            total_symbols.extend(shard['symbols'])
            print(f'Loaded shard {idx}: {len(shard["trades"])} trades', flush=True)
        except FileNotFoundError:
            print(f'[ERROR] shard_{idx}.json not found!', flush=True)
            sys.exit(1)

    print(f'Total trades across all shards: {len(all_trades)}', flush=True)
    agg = stats(all_trades)

    # ── Write backtest_report.json ────────────────────────────────────────────
    report = {
        'strategy':       'G Max V1 — Variant G VAR_D',
        'timeframe':      TIMEFRAME,
        'period':         f'{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}',
        'leverage':       LEVERAGE,
        'tp_pct':         TP_PCT * 100,
        'sl_pct':         SL_PCT * 100,
        'max_bars':       MAX_BARS,
        'capital':        CAPITAL,
        'risk_pct':       RISK_PCT * 100,
        'notional':       round(NOTIONAL, 2),
        'symbols_attempted': len(total_symbols),
        'symbols_with_data': len(all_with_data),
        'symbols_rejected':  len(all_rejected),
        'rejected_detail':   all_rejected,
        'stats':          agg,
        'trades':         all_trades,
    }
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # ── Write backtest_summary.txt ────────────────────────────────────────────
    usable = agg['profit_factor'] >= 1.5 and agg['win_rate'] >= 42.0
    verdict = '✅ USABLE' if usable else '❌ NOT USABLE'

    lines = []
    lines.append('=' * 70)
    lines.append('  G MAX V1 — VARIANT G VAR_D  BACKTEST SUMMARY')
    lines.append('=' * 70)
    lines.append(f'  Strategy   : EMA50 slope + EMA9/21 cross + ADX(14)>=22 | 15m')
    lines.append(f'  Period     : {START_YM[0]}-{START_YM[1]:02d} → {END_YM[0]}-{END_YM[1]:02d}  (2 years)')
    lines.append(f'  TP / SL    : {TP_PCT*100:.1f}% / {SL_PCT*100:.1f}%  |  Max hold: {MAX_BARS} bars (10d)')
    lines.append(f'  Leverage   : {LEVERAGE}x  |  Capital: ${CAPITAL:,.0f}  |  Risk/trade: {RISK_PCT*100:.2f}%')
    lines.append(f'  Notional   : ${NOTIONAL:.2f} per trade')
    lines.append(f'  Symbols    : {len(total_symbols)} attempted | {len(all_with_data)} with data | {len(all_rejected)} rejected')
    lines.append('')
    lines.append('── AGGREGATE STATS ─────────────────────────────────────────────')
    lines.append(f'  Total Trades   : {agg["total"]}')
    lines.append(f'  Win Rate       : {agg["win_rate"]:.2f}%')
    lines.append(f'  Profit Factor  : {agg["profit_factor"]:.4f}')
    lines.append(f'  Net PnL        : ${agg["net_pnl"]:,.2f}')
    lines.append(f'  Max Drawdown   : ${agg["max_drawdown"]:,.2f}')
    lines.append(f'  Avg Win        : ${agg["avg_win"]:.2f}')
    lines.append(f'  Avg Loss       : ${agg["avg_loss"]:.2f}')
    lines.append(f'  Expectancy     : ${agg["expectancy"]:.2f} per trade')
    lines.append('')
    lines.append('── DIRECTIONAL SPLIT ───────────────────────────────────────────')
    lines.append(f'  Longs  : {agg["longs"]}  WR={agg["long_wr"]:.2f}%')
    lines.append(f'  Shorts : {agg["shorts"]}  WR={agg["short_wr"]:.2f}%')
    lines.append('')
    lines.append('── EXIT REASONS ────────────────────────────────────────────────')
    lines.append(f'  TP hits        : {agg["tp_count"]}')
    lines.append(f'  SL hits        : {agg["sl_count"]}')
    lines.append(f'  Max hold close : {agg["max_hold_count"]}')
    lines.append(f'  End-of-data    : {agg["eod_count"]}')
    lines.append('')
    lines.append(f'  RECOMMENDATION : {verdict}  (threshold: PF≥1.5 and WR≥42%)')
    lines.append('')

    # Rejected symbols
    if all_rejected:
        lines.append('── REJECTED (insufficient data) ────────────────────────────────')
        for r in all_rejected:
            lines.append(f'  {r["symbol"]:<30} {r["reason"]}')
        lines.append('')

    # Top 50 coins by net PnL
    per_coin = agg['per_coin']
    sorted_coins = sorted(per_coin.items(), key=lambda x: x[1]['pnl'], reverse=True)
    lines.append('── TOP 50 COINS BY NET PNL ─────────────────────────────────────')
    lines.append(f'  {"Symbol":<22} {"Trades":>6} {"WR%":>7} {"Net PnL":>12}')
    lines.append('  ' + '-' * 50)
    for sym, d in sorted_coins[:50]:
        lines.append(f'  {sym:<22} {d["n"]:>6} {d["wr"]:>6.1f}%  ${d["pnl"]:>10,.2f}')
    lines.append('')

    # Bottom 20 coins
    lines.append('── BOTTOM 20 COINS BY NET PNL ──────────────────────────────────')
    lines.append(f'  {"Symbol":<22} {"Trades":>6} {"WR%":>7} {"Net PnL":>12}')
    lines.append('  ' + '-' * 50)
    for sym, d in sorted_coins[-20:]:
        lines.append(f'  {sym:<22} {d["n"]:>6} {d["wr"]:>6.1f}%  ${d["pnl"]:>10,.2f}')
    lines.append('')

    # Monthly PnL table
    monthly = agg['monthly']
    lines.append('── MONTHLY PNL ─────────────────────────────────────────────────')
    lines.append(f'  {"Month":<10} {"Trades":>7} {"Wins":>6} {"WR%":>7} {"PnL":>12}')
    lines.append('  ' + '-' * 46)
    running = 0.0
    for mon in sorted(monthly.keys()):
        d = monthly[mon]
        running += d['pnl']
        wr = d['w'] / d['n'] * 100 if d['n'] else 0.0
        lines.append(
            f'  {mon:<10} {d["n"]:>7} {d["w"]:>6} {wr:>6.1f}%  ${d["pnl"]:>10,.2f}'
        )
    lines.append(f'  {"TOTAL":<10} {agg["total"]:>7}             ${agg["net_pnl"]:>10,.2f}')
    lines.append('')
    lines.append('=' * 70)

    summary = '\n'.join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary)

    print(summary, flush=True)
    print('\n✅ backtest_report.json and backtest_summary.txt written.', flush=True)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python backtest.py <shard_idx|merge>')
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

