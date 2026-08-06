"""
GMax V1 — Variant G · 15m — Backtest
======================================
Strategy : EMA50 slope (±0.05%) + EMA9/21 crossover + ADX(14) >= 22
           Signal on last closed bar, entry on next bar open
           TP: 3.0% | SL: 15.0% | Max hold: 960 bars (10 days)

Coin lists:
  WHITELIST  : 58 coins (PF>=1.5 from live bot)
  UNIVERSE   : 117 coins (full scan list)
  Both lists backtested separately, results clearly labeled.

Data     : data.binance.vision futures monthly archives (auto-skip 404)
Period   : 2023-08 → 2025-07 (24 months)
           Coins with less history get only available months — no padding.
Capital  : $10,000 | Risk: 0.75%/trade | Fee: 0.05%/side | Slip: 0.02%/side
Leverage : 5x (notional cap)
Workers  : 16 per shard (I/O-bound)
Shards   : 8 parallel GitHub Actions jobs

Usage:
  python backtest.py <0-7>   — run one shard
  python backtest.py merge   — merge all shards into final report
"""

import csv, io, json, os, sys, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# ── Coin Lists (extracted from GMaxV1.py) ──────────────────────────────────

COINS_WHITELIST = [
    '1000000BOBUSDT','1000CATUSDT','1000RATSUSDT','ACHUSDT',
    'AINUSDT','AIOTUSDT','ALGOUSDT','ALPINEUSDT','ASRUSDT','AUSDT','AWEUSDT',
    'BASEDUSDT','BELUSDT','BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT',
    'DAMUSDT','DIAUSDT','ENAUSDT','ETHUSDT',
    'GLMUSDT','GUAUSDT','HANAUSDT','LIGHTUSDT','LYNUSDT',
    'MAGICUSDT','NMRUSDT','NOTUSDT',
    'PEOPLEUSDT','PIXELUSDT','POWERUSDT','POWRUSDT','PUNDIXUSDT','RAVEUSDT',
    'SEIUSDT','SPELLUSDT','TRUTHUSDT',
    'TURBOUSDT','UBUSDT','VANRYUSDT','ZECUSDT',
    'ZEREBROUSDT',
]

COINS_UNIVERSE = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','ACHUSDT','AINUSDT','AIOTUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ANKRUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT',
    'CUSDT','DAMUSDT','DEXEUSDT','DIAUSDT',
    'EIGENUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','ETHUSDT',
    'EVAAUSDT','FLUXUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MOODENGUSDT','MTLUSDT',
    'NMRUSDT','NOTUSDT','OPENUSDT','OPNUSDT',
    'PEOPLEUSDT','PIPPINUSDT','PIXELUSDT','PLUMEUSDT','POLUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','PUNDIXUSDT','RAVEUSDT',
    'RESOLVUSDT','SAGAUSDT','SANTOSUSDT',
    'SEIUSDT','SIGNUSDT','SKRUSDT','SOMIUSDT','SPELLUSDT',
    'SPKUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VANRYUSDT','VIRTUALUSDT','VVVUSDT','WLDUSDT',
    'XLMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

# Combined deduplicated list for shard splitting
ALL_SYMBOLS = list(dict.fromkeys(COINS_UNIVERSE + COINS_WHITELIST))

NUM_SHARDS = 8

# ── Config ──────────────────────────────────────────────────────────────────

START_YM  = (2023, 8)
END_YM    = (2025, 7)
TIMEFRAME = '15m'

CAPITAL   = 10_000.0
RISK_PCT  = 0.0075
FEE       = 0.0005
SLIP      = 0.0002
LEVERAGE  = 5
TP_PCT    = 0.030
SL_PCT    = 0.150
MAX_BARS  = 960
MIN_BARS  = 72
WORKERS   = 16

BASE_URL  = 'https://data.binance.vision/data/futures/um/monthly/klines'

# ── Months ──────────────────────────────────────────────────────────────────

def months_in_range(start, end):
    y, m = start
    ey, em = end
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m > 12: m = 1; y += 1
    return out

ALL_MONTHS = months_in_range(START_YM, END_YM)

# ── Fetch ───────────────────────────────────────────────────────────────────

def fetch_month(symbol, year, month):
    fname = f'{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip'
    url   = f'{BASE_URL}/{symbol}/{TIMEFRAME}/{fname}'
    try:
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urlopen(req, timeout=25) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                rows = list(csv.reader(io.TextIOWrapper(f, 'utf-8')))
        out = []
        for row in rows:
            if not row or not row[0].isdigit(): continue
            ts = int(row[0])
            if ts > 10**14: ts //= 1000
            out.append((ts, float(row[1]), float(row[2]),
                        float(row[3]), float(row[4])))
        return out
    except HTTPError as e:
        if e.code == 404: return []
        raise
    except Exception:
        return []

def fetch_symbol(symbol):
    """Fetch all available months. 404 months skipped — coin gets only its real history."""
    all_c = []
    for (y, m) in ALL_MONTHS:
        all_c.extend(fetch_month(symbol, y, m))
    all_c.sort(key=lambda x: x[0])
    seen = set(); out = []
    for c in all_c:
        if c[0] not in seen:
            seen.add(c[0]); out.append(c)
    return out

# ── Indicators ──────────────────────────────────────────────────────────────

def ema(closes, period):
    k = 2.0 / (period + 1)
    v = [closes[0]]
    for c in closes[1:]: v.append(c * k + v[-1] * (1 - k))
    return v

def adx(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 3: return 0.0
    pdm, mdm, tr = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up  and down > 0 else 0.0)
        tr.append(max(highs[i]-lows[i],
                      abs(highs[i]-closes[i-1]),
                      abs(lows[i]-closes[i-1])))
    def ws(v):
        if len(v) < period: return []
        r = [sum(v[:period])]
        for x in v[period:]: r.append(r[-1] - r[-1]/period + x)
        return r
    st = ws(tr); sp = ws(pdm); sm = ws(mdm)
    if not st: return 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if p+m else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0
    adx_v = sum(dx[:period]) / period
    for d in dx[period:]: adx_v = (adx_v*(period-1)+d) / period
    return max(0.0, min(100.0, adx_v))

# ── Signal — Variant G (exact match to GMaxV1.py check_signal_G) ────────────

def signal_g(i, e9, e21, e50, highs, lows, closes):
    """
    i = index of last closed bar.
    Returns 'buy', 'sell', or None.
    """
    if i < 10: return None

    # Filter 1: EMA50 slope
    slope_pct  = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down: return None

    # Filter 2: EMA9/21 crossover on closed bar
    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if not crossed_up and not crossed_down: return None
    if trend_up   and not crossed_up:   return None
    if trend_down and not crossed_down: return None

    # Filter 3: ADX >= 22
    if adx(highs[:i+1], lows[:i+1], closes[:i+1]) < 22: return None

    return 'buy' if crossed_up else 'sell'

# ── Backtest Single Symbol ───────────────────────────────────────────────────

def backtest(symbol, candles):
    if len(candles) < MIN_BARS + 2: return []

    ts  = [c[0] for c in candles]
    op  = [c[1] for c in candles]
    hi  = [c[2] for c in candles]
    lo  = [c[3] for c in candles]
    cl  = [c[4] for c in candles]

    e9  = ema(cl, 9)
    e21 = ema(cl, 21)
    e50 = ema(cl, 50)

    trades = []
    pos    = None

    for i in range(MIN_BARS, len(candles) - 1):
        # ── Manage open position ──────────────────────────────────────────
        if pos:
            held = i - pos['eb']
            ep   = pos['ep']
            if pos['side'] == 'buy':
                if lo[i] <= pos['sl']:
                    exit_p, reason, gross = pos['sl'], 'sl',       (pos['sl'] - ep) / ep
                elif hi[i] >= pos['tp']:
                    exit_p, reason, gross = pos['tp'], 'tp',       (pos['tp'] - ep) / ep
                elif held >= MAX_BARS:
                    exit_p, reason, gross = cl[i],   'max_hold',   (cl[i]    - ep) / ep
                else:
                    continue
            else:
                if hi[i] >= pos['sl']:
                    exit_p, reason, gross = pos['sl'], 'sl',       (ep - pos['sl']) / ep
                elif lo[i] <= pos['tp']:
                    exit_p, reason, gross = pos['tp'], 'tp',       (ep - pos['tp']) / ep
                elif held >= MAX_BARS:
                    exit_p, reason, gross = cl[i],   'max_hold',   (ep - cl[i])    / ep
                else:
                    continue
            pnl = pos['notional'] * (gross - (FEE + SLIP) * 2)
            trades.append({
                'symbol': symbol, 'side': pos['side'],
                'entry_ts': pos['ts'], 'exit_ts': ts[i],
                'entry_price': round(ep, 8), 'exit_price': round(exit_p, 8),
                'pnl': round(pnl, 6), 'reason': reason, 'bars': held,
            })
            pos = None

        # ── Check for new signal ──────────────────────────────────────────
        sig = signal_g(i, e9, e21, e50, hi, lo, cl)
        if sig is None or pos is not None: continue

        eb  = i + 1
        ep  = op[eb] * (1 + FEE + SLIP if sig == 'buy' else 1 - FEE - SLIP)
        notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
        tp = ep * (1 + TP_PCT) if sig == 'buy' else ep * (1 - TP_PCT)
        sl = ep * (1 - SL_PCT) if sig == 'buy' else ep * (1 + SL_PCT)
        pos = {'side': sig, 'eb': eb, 'ts': ts[eb], 'ep': ep,
               'tp': tp, 'sl': sl, 'notional': notional}

    # ── Close any open position at end of data ────────────────────────────
    if pos:
        i   = len(candles) - 1
        ep  = pos['ep']
        gross = (cl[i] - ep) / ep if pos['side'] == 'buy' else (ep - cl[i]) / ep
        pnl = pos['notional'] * (gross - (FEE + SLIP) * 2)
        trades.append({
            'symbol': symbol, 'side': pos['side'],
            'entry_ts': pos['ts'], 'exit_ts': ts[i],
            'entry_price': round(ep, 8), 'exit_price': round(cl[i], 8),
            'pnl': round(pnl, 6), 'reason': 'end_of_data', 'bars': i - pos['eb'],
        })
    return trades

# ── Stats ────────────────────────────────────────────────────────────────────

def stats(trades):
    if not trades: return {}
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    total  = len(trades)
    gp     = sum(t['pnl'] for t in wins)
    gl     = sum(abs(t['pnl']) for t in losses)
    pf     = round(gp / gl, 4) if gl else float('inf')
    net    = sum(t['pnl'] for t in trades)
    avg_w  = gp / len(wins)   if wins   else 0.0
    avg_l  = sum(t['pnl'] for t in losses) / len(losses) if losses else 0.0
    wr     = len(wins) / total * 100

    eq = 0; peak = 0; dd = 0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        eq += t['pnl']
        if eq > peak: peak = eq
        if peak > 0: dd = max(dd, peak - eq)

    monthly = {}
    for t in trades:
        k = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc).strftime('%Y-%m')
        monthly.setdefault(k, {'pnl': 0.0, 'n': 0, 'w': 0})
        monthly[k]['pnl'] += t['pnl']
        monthly[k]['n']   += 1
        if t['pnl'] > 0: monthly[k]['w'] += 1

    per_coin = {}
    for t in trades:
        s = t['symbol']
        per_coin.setdefault(s, {'pnl': 0.0, 'n': 0, 'w': 0})
        per_coin[s]['pnl'] += t['pnl']
        per_coin[s]['n']   += 1
        if t['pnl'] > 0: per_coin[s]['w'] += 1
    for s in per_coin:
        v = per_coin[s]
        v['wr'] = round(v['w'] / v['n'] * 100, 1)

    return {
        'total':         total,
        'win_rate':      round(wr, 2),
        'profit_factor': pf,
        'net_pnl':       round(net, 4),
        'max_drawdown':  round(dd, 4),
        'avg_win':       round(avg_w, 4),
        'avg_loss':      round(avg_l, 4),
        'expectancy':    round((wr/100)*avg_w + (1-wr/100)*avg_l, 4),
        'longs':         len([t for t in trades if t['side'] == 'buy']),
        'shorts':        len([t for t in trades if t['side'] == 'sell']),
        'monthly':       dict(sorted(monthly.items())),
        'per_coin':      dict(sorted(per_coin.items(),
                              key=lambda x: x[1]['pnl'], reverse=True)),
    }

# ── Shard Runner ─────────────────────────────────────────────────────────────

def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    t0 = time.time()
    print(f'[Shard {shard_idx}] {len(symbols)} coins | {WORKERS} workers')

    raw = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, s): s for s in symbols}
        done = 0
        for f in as_completed(futs):
            sym = futs[f]
            done += 1
            try:   raw[sym] = f.result()
            except Exception as e:
                print(f'  FETCH ERR {sym}: {e}'); raw[sym] = []
            if done % 15 == 0 or done == len(symbols):
                print(f'  Fetched {done}/{len(symbols)}  {time.time()-t0:.0f}s')

    with_data = [s for s in symbols if len(raw.get(s, [])) >= MIN_BARS + 2]
    print(f'  {len(with_data)}/{len(symbols)} have enough data')

    all_trades = []
    for sym in with_data:
        try:   all_trades.extend(backtest(sym, raw[sym]))
        except Exception as e: print(f'  BT ERR {sym}: {e}')

    out = {
        'shard':     shard_idx,
        'symbols':   symbols,
        'with_data': with_data,
        'trades':    all_trades,
        'stats':     stats(all_trades),
        'elapsed':   round(time.time() - t0, 1),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(out, f)

    s = out['stats']
    print(f'[Shard {shard_idx}] Done {out["elapsed"]}s | '
          f'Trades={len(all_trades)} | WR={s.get("win_rate","?")}% | '
          f'PF={s.get("profit_factor","?")} | PnL=${s.get("net_pnl","?")}')

# ── Merge ────────────────────────────────────────────────────────────────────

def merge_shards():
    print('Merging shards...')
    all_trades = []
    meta = {'shards': [], 'symbols_attempted': 0,
            'symbols_with_data': 0, 'total_elapsed': 0}

    for i in range(NUM_SHARDS):
        fname = f'shard_{i}.json'
        if not os.path.exists(fname):
            print(f'  WARNING: {fname} missing'); continue
        with open(fname) as f: d = json.load(f)
        all_trades.extend(d['trades'])
        meta['symbols_attempted'] += len(d['symbols'])
        meta['symbols_with_data'] += len(d['with_data'])
        meta['total_elapsed']     += d['elapsed']
        meta['shards'].append({'shard': i, 'elapsed': d['elapsed'],
                               'trades': len(d['trades'])})
        print(f'  Shard {i}: {len(d["trades"])} trades, {d["elapsed"]}s')

    # ── Separate stats for whitelist vs universe coins ────────────────────
    wl_set = set(COINS_WHITELIST)
    wl_trades  = [t for t in all_trades if t['symbol'] in wl_set]
    uni_trades = all_trades   # universe includes whitelist coins too

    agg_all = stats(all_trades)
    agg_wl  = stats(wl_trades)

    def pf_str(s):
        pf = s.get('profit_factor', 0)
        return 'inf' if pf == float('inf') else str(pf)

    def rec(s):
        pf = s.get('profit_factor', 0)
        wr = s.get('win_rate', 0)
        ok = (pf >= 1.5 or pf == float('inf')) and wr >= 42.0
        return ('✅ USABLE — PF>=1.5 and WR>=42% met' if ok
                else f'❌ NOT USABLE — PF={pf_str(s)}, WR={wr}%')

    report = {
        'meta': {
            **meta,
            'strategy': 'GMax V1 · Variant G · 15m',
            'period': f'{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}',
            'capital': CAPITAL, 'risk_pct': RISK_PCT, 'leverage': LEVERAGE,
            'tp_pct': TP_PCT, 'sl_pct': SL_PCT, 'fee': FEE, 'slip': SLIP,
            'whitelist_coins': len(COINS_WHITELIST),
            'universe_coins':  len(COINS_UNIVERSE),
        },
        'universe_aggregate':  agg_all,
        'whitelist_aggregate': agg_wl,
        'universe_recommendation':  rec(agg_all),
        'whitelist_recommendation': rec(agg_wl),
        'trades': sorted(all_trades, key=lambda x: x['exit_ts']),
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # ── Summary text ─────────────────────────────────────────────────────
    def section(label, s, recommendation):
        lines = [
            f'── {label} ──',
            f'  Trades        : {s.get("total",0)}',
            f'  Win rate      : {s.get("win_rate",0)}%',
            f'  Profit factor : {pf_str(s)}',
            f'  Net PnL       : ${s.get("net_pnl",0):,.2f}',
            f'  Max drawdown  : ${s.get("max_drawdown",0):,.2f}',
            f'  Avg win       : ${s.get("avg_win",0):,.2f}',
            f'  Avg loss      : ${s.get("avg_loss",0):,.2f}',
            f'  Expectancy    : ${s.get("expectancy",0):,.2f}',
            f'  Longs         : {s.get("longs",0)}',
            f'  Shorts        : {s.get("shorts",0)}',
            f'',
            f'  RECOMMENDATION: {recommendation}',
        ]
        return lines

    lines = [
        '=' * 62,
        'GMax V1 — Variant G · 15m — FULL BACKTEST RESULTS',
        '=' * 62,
        f'Period     : {report["meta"]["period"]}',
        f'Symbols    : {meta["symbols_with_data"]} with data / {meta["symbols_attempted"]} attempted',
        f'Total time : {meta["total_elapsed"]:.0f}s across shards',
        '',
    ]
    lines += section('UNIVERSE (all coins)',
                     agg_all, rec(agg_all))
    lines += ['']
    lines += section(f'WHITELIST ({len(COINS_WHITELIST)} coins only)',
                     agg_wl, rec(agg_wl))

    lines += ['', '── PER-COIN TOP 60 by Net PnL (Universe) ──',
              f'  {"Symbol":<22} {"Trades":>6} {"WR%":>7} {"PnL":>12}',
              '  ' + '-' * 52]
    for sym, v in list(agg_all.get('per_coin', {}).items())[:60]:
        lines.append(f'  {sym:<22} {v["n"]:>6} {v["wr"]:>6.1f}%  ${v["pnl"]:>10.2f}')

    lines += ['', '── MONTHLY PnL (Universe) ──']
    for mo, v in agg_all.get('monthly', {}).items():
        sign  = '+' if v['pnl'] >= 0 else ''
        wr_mo = v['w']/v['n']*100 if v['n'] else 0
        lines.append(f'  {mo}  {sign}{v["pnl"]:>9.2f} USDT  n={v["n"]}  wr={wr_mo:.0f}%')

    summary = '\n'.join(lines)
    print('\n' + summary)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary)

    print('\n✅ backtest_report.json + backtest_summary.txt written')

# ── Entry ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python backtest.py <0-7|merge>')
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        try:
            idx = int(arg)
            assert 0 <= idx < NUM_SHARDS
        except (ValueError, AssertionError):
            print(f'shard index must be 0-{NUM_SHARDS-1}')
            sys.exit(1)
        run_shard(idx)

