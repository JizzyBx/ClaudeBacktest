"""
GMax V1 Backtest — 4 Variants (VAR_D / VAR_A / VAR_B / VAR_C)
Strategy : EMA50 slope + EMA9/21 crossover + ADX(14) >= 22 — 15m timeframe
Leverage : 5x | Capital: $10,000 | Risk: 0.75% per trade | No position cap
Coins    : 117-coin Universe | Period: 2 years (or full available history)
Speed    : 200 threads for I/O fetch + 50 processes for CPU backtest
Outputs  : backtest_summary.txt + backtest_report.json ONLY
"""

import csv, io, json, sys, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Variants ──────────────────────────────────────────────────────────────────
VARIANTS = {
    'VAR_D': {'tp': 0.030, 'sl': 0.150, 'label': 'VAR_D (TP 3.0% / SL 15.0%)'},
    'VAR_A': {'tp': 0.010, 'sl': 0.100, 'label': 'VAR_A (TP 1.0% / SL 10.0%)'},
    'VAR_B': {'tp': 0.005, 'sl': 0.080, 'label': 'VAR_B (TP 0.5% / SL 8.0%)'},
    'VAR_C': {'tp': 0.003, 'sl': 0.080, 'label': 'VAR_C (TP 0.3% / SL 8.0%)'},
}

# ── 117-coin Universe ─────────────────────────────────────────────────────────
COINS = [
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

# ── Settings ──────────────────────────────────────────────────────────────────
CAPITAL        = 10_000.0
RISK_PCT       = 0.0075
LEVERAGE       = 5
FEE_RATE       = 0.0005
SLIP_RATE      = 0.0002
MAX_HOLD_BARS  = 960
INTERVAL       = '15m'
FETCH_THREADS  = 200   # threads — perfect for I/O bound downloads
BACKTEST_PROCS = 50    # processes — CPU bound backtest
ADX_MIN        = 22
SLOPE_MIN      = 0.05

# ── Date range ────────────────────────────────────────────────────────────────
_NOW      = datetime.now(timezone.utc)
END_YEAR  = _NOW.year if _NOW.month > 1 else _NOW.year - 1
END_MONTH = _NOW.month - 1 if _NOW.month > 1 else 12
START_DT  = datetime(_NOW.year - 2, _NOW.month, 1, tzinfo=timezone.utc)


def _months_in_range():
    y, m = START_DT.year, START_DT.month
    while (y, m) <= (END_YEAR, END_MONTH):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1


# ── Data fetch — ThreadPoolExecutor (I/O bound, no GIL penalty) ───────────────
def fetch_symbol_month(args):
    symbol, year, month = args
    ym  = f"{year}-{month:02d}"
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines"
           f"/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip")
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = r.read()
    except Exception:
        return symbol, []

    bars = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                for row in csv.reader(io.TextIOWrapper(f)):
                    if not row or not row[0].isdigit():
                        continue
                    try:
                        ts = int(row[0])
                        if ts > 10**14: ts //= 1000
                        bars.append((ts, float(row[1]), float(row[2]),
                                     float(row[3]), float(row[4])))
                    except Exception:
                        continue
    except Exception:
        pass
    return symbol, bars


def fetch_all(coins):
    tasks   = [(s, y, m) for s in coins for y, m in _months_in_range()]
    buckets = {s: {} for s in coins}
    done    = 0
    total   = len(tasks)
    # 200 threads — all sitting in network wait simultaneously
    with ThreadPoolExecutor(max_workers=FETCH_THREADS) as ex:
        futs = {ex.submit(fetch_symbol_month, t): t for t in tasks}
        for fut in as_completed(futs):
            sym, bars = fut.result()
            for b in bars:
                buckets[sym][b[0]] = b
            done += 1
            if done % 500 == 0 or done == total:
                print(f"  [fetch] {done}/{total} chunks done", flush=True)
    return {s: sorted(buckets[s].values()) for s in coins}


# ── Indicators — O(n) pre-computed series ─────────────────────────────────────
def _ema_series(values, period):
    k   = 2.0 / (period + 1)
    out = [values[0]] * len(values)
    for i in range(1, len(values)):
        out[i] = values[i] * k + out[i-1] * (1 - k)
    return out


def _adx_series(highs, lows, closes, period=14):
    n       = len(closes)
    adx_out = [0.0] * n
    if n < period * 3:
        return adx_out

    pdm, mdm, tr = [], [], []
    for i in range(1, n):
        up = highs[i]  - highs[i-1]
        dn = lows[i-1] - lows[i]
        pdm.append(up if up > dn and up > 0 else 0.0)
        mdm.append(dn if dn > up and dn > 0 else 0.0)
        tr.append(max(highs[i] - lows[i],
                      abs(highs[i] - closes[i-1]),
                      abs(lows[i]  - closes[i-1])))

    def wilder(lst):
        if len(lst) < period: return []
        r = [sum(lst[:period])]
        for x in lst[period:]: r.append(r[-1] - r[-1]/period + x)
        return r

    atr_w = wilder(tr)
    pdm_w = wilder(pdm)
    mdm_w = wilder(mdm)
    if not atr_w:
        return adx_out

    pdi = [100*p/t if t else 0 for p, t in zip(pdm_w, atr_w)]
    mdi = [100*m/t if t else 0 for m, t in zip(mdm_w, atr_w)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]

    if len(dx) < period:
        return adx_out

    adx_val = sum(dx[:period]) / period
    base    = period * 2
    if base < n:
        adx_out[base] = max(0.0, min(100.0, adx_val))
    for j, d in enumerate(dx[period:], 1):
        adx_val = (adx_val * (period - 1) + d) / period
        idx = base + j
        if idx < n:
            adx_out[idx] = max(0.0, min(100.0, adx_val))

    return adx_out


# ── Per-symbol backtest — ProcessPoolExecutor (CPU bound) ─────────────────────
def backtest_symbol(args):
    symbol, bars = args
    if len(bars) < 100:
        return symbol, {vk: _empty(symbol, vk) for vk in VARIANTS}

    ts_arr = [b[0] for b in bars]
    opens  = [b[1] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    closes = [b[4] for b in bars]
    n      = len(closes)

    # Pre-compute all indicators once per symbol
    e9  = _ema_series(closes, 9)
    e21 = _ema_series(closes, 21)
    e50 = _ema_series(closes, 50)
    adx = _adx_series(highs, lows, closes, 14)

    slope = [0.0] * n
    for i in range(10, n):
        if e50[i-10] != 0:
            slope[i] = (e50[i] - e50[i-10]) / e50[i-10] * 100

    results = {}

    for vk, vcfg in VARIANTS.items():
        TP_PCT = vcfg['tp']
        SL_PCT = vcfg['sl']

        equity = CAPITAL
        pos    = None
        trades = []
        gp = gl = 0.0
        rej = {
            'warmup': 0, 'no_trend': 0, 'no_cross': 0,
            'dir_mismatch': 0, 'adx_low': 0, 'in_trade': 0, 'signals': 0,
        }
        scanned = 0

        for i in range(1, n - 1):
            scanned += 1

            # ── Manage open position ──────────────────────────────────────
            if pos is not None:
                ch   = highs[i]
                cl   = lows[i]
                held = i - pos['bar']
                ex   = None

                if pos['side'] == 'buy':
                    if   ch >= pos['tp']:       ex = pos['tp']
                    elif cl <= pos['sl']:        ex = pos['sl']
                    elif held >= MAX_HOLD_BARS:  ex = closes[i]
                else:
                    if   cl <= pos['tp']:        ex = pos['tp']
                    elif ch >= pos['sl']:         ex = pos['sl']
                    elif held >= MAX_HOLD_BARS:  ex = closes[i]

                if ex is not None:
                    epx  = pos['epx']; qty = pos['qty']
                    raw  = (ex - epx)*qty if pos['side']=='buy' else (epx - ex)*qty
                    cost = (epx + ex) * qty * (FEE_RATE + SLIP_RATE)
                    pnl  = raw - cost
                    equity += pnl
                    if pnl > 0: gp += pnl
                    else:       gl += abs(pnl)
                    trades.append({
                        'side':     pos['side'],
                        'pnl':      round(pnl, 6),
                        'entry_ts': ts_arr[pos['bar']],
                        'exit_ts':  ts_arr[i],
                    })
                    pos = None

            # ── Signal check ──────────────────────────────────────────────
            if pos is not None:
                rej['in_trade'] += 1
                continue

            if i < 70:
                rej['warmup'] += 1
                continue

            s          = slope[i]
            trend_up   = s >  SLOPE_MIN
            trend_down = s < -SLOPE_MIN
            if not trend_up and not trend_down:
                rej['no_trend'] += 1
                continue

            cross_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
            cross_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
            if not cross_up and not cross_down:
                rej['no_cross'] += 1
                continue

            if (trend_up and not cross_up) or (trend_down and not cross_down):
                rej['dir_mismatch'] += 1
                continue

            if adx[i] < ADX_MIN:
                rej['adx_low'] += 1
                continue

            if i + 1 >= n:
                break

            epx      = opens[i + 1]
            risk     = equity * RISK_PCT
            notional = risk / (SL_PCT + (FEE_RATE + SLIP_RATE) * 2)
            qty      = notional / epx
            side     = 'buy' if cross_up else 'sell'
            tp       = epx * (1 + TP_PCT) if side=='buy' else epx * (1 - TP_PCT)
            sl       = epx * (1 - SL_PCT) if side=='buy' else epx * (1 + SL_PCT)

            pos = {'side': side, 'epx': epx, 'tp': tp, 'sl': sl,
                   'qty': qty, 'bar': i + 1}
            rej['signals'] += 1

        if pos is not None:
            ex   = closes[-1]; epx = pos['epx']; qty = pos['qty']
            raw  = (ex - epx)*qty if pos['side']=='buy' else (epx - ex)*qty
            cost = (epx + ex) * qty * (FEE_RATE + SLIP_RATE)
            pnl  = raw - cost
            equity += pnl
            if pnl > 0: gp += pnl
            else:       gl += abs(pnl)
            trades.append({
                'side':     pos['side'],
                'pnl':      round(pnl, 6),
                'entry_ts': ts_arr[pos['bar']],
                'exit_ts':  ts_arr[-1],
            })

        results[vk] = _build(symbol, vk, trades, gp, gl,
                             equity - CAPITAL, scanned, rej, n)

    return symbol, results


def _empty(symbol, vk):
    return {
        'symbol': symbol, 'variant': vk, 'trades': [], 'total': 0,
        'wins': 0, 'losses': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
        'net_pnl': 0.0, 'gross_profit': 0.0, 'gross_loss': 0.0,
        'avg_win': 0.0, 'avg_loss': 0.0, 'candles_scanned': 0,
        'bars_available': 0, 'rejections': {},
    }


def _build(symbol, vk, trades, gp, gl, net_pnl, scanned, rej, bars):
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    nw, nl = len(wins), len(losses)
    total  = len(trades)
    wr     = nw / total * 100 if total else 0.0
    pf     = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)
    return {
        'symbol':          symbol,
        'variant':         vk,
        'trades':          trades,
        'total':           total,
        'wins':            nw,
        'losses':          nl,
        'win_rate':        round(wr, 2),
        'profit_factor':   round(pf, 4) if pf != float('inf') else 'inf',
        'net_pnl':         round(net_pnl, 4),
        'gross_profit':    round(gp, 4),
        'gross_loss':      round(gl, 4),
        'avg_win':         round(sum(t['pnl'] for t in wins)   / nw, 4) if nw else 0.0,
        'avg_loss':        round(sum(t['pnl'] for t in losses) / nl, 4) if nl else 0.0,
        'candles_scanned': scanned,
        'bars_available':  bars,
        'rejections':      rej,
    }


# ── Aggregate portfolio ───────────────────────────────────────────────────────
def aggregate(all_results):
    out = {}
    for vk in VARIANTS:
        all_trades = []
        for sd in all_results.values():
            if vk in sd:
                all_trades.extend(sd[vk]['trades'])
        all_trades.sort(key=lambda t: t['entry_ts'])

        equity = CAPITAL
        for t in all_trades: equity += t['pnl']

        wins   = [t for t in all_trades if t['pnl'] > 0]
        losses = [t for t in all_trades if t['pnl'] <= 0]
        gp     = sum(t['pnl'] for t in wins)
        gl     = sum(abs(t['pnl']) for t in losses)
        nw, nl = len(wins), len(losses)
        total  = len(all_trades)
        wr     = nw / total * 100 if total else 0.0
        pf     = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)

        run = peak = mdd = mdd_pct = 0.0
        for t in all_trades:
            run += t['pnl']
            if run > peak: peak = run
            dd = peak - run
            if dd > mdd:
                mdd     = dd
                mdd_pct = dd / (CAPITAL + peak) * 100 if (CAPITAL + peak) else 0

        monthly = {}
        for t in all_trades:
            dt  = datetime.fromtimestamp(t['exit_ts'] / 1000, tz=timezone.utc)
            key = f"{dt.year}-{dt.month:02d}"
            monthly[key] = monthly.get(key, 0.0) + t['pnl']

        longs  = [t for t in all_trades if t['side'] == 'buy']
        shorts = [t for t in all_trades if t['side'] == 'sell']
        avg_w  = sum(t['pnl'] for t in wins)   / nw if nw else 0.0
        avg_l  = sum(t['pnl'] for t in losses) / nl if nl else 0.0

        out[vk] = {
            'total':         total,
            'wins':          nw,
            'losses':        nl,
            'win_rate':      round(wr, 2),
            'profit_factor': round(pf, 4) if pf != float('inf') else 'inf',
            'net_pnl':       round(equity - CAPITAL, 4),
            'gross_profit':  round(gp, 4),
            'gross_loss':    round(gl, 4),
            'max_dd':        round(mdd, 4),
            'max_dd_pct':    round(mdd_pct, 2),
            'avg_win':       round(avg_w, 6),
            'avg_loss':      round(avg_l, 6),
            'longs':         len(longs),
            'shorts':        len(shorts),
            'long_wr':       round(len([t for t in longs  if t['pnl']>0])/len(longs)*100,  2) if longs  else 0.0,
            'short_wr':      round(len([t for t in shorts if t['pnl']>0])/len(shorts)*100, 2) if shorts else 0.0,
            'monthly':       {k: round(v, 4) for k, v in sorted(monthly.items())},
            'trades':        all_trades,
        }
    return out


# ── Report ────────────────────────────────────────────────────────────────────
def agg_rej(all_results, vk):
    tot = {'warmup':0,'no_trend':0,'no_cross':0,'dir_mismatch':0,
           'adx_low':0,'in_trade':0,'signals':0}
    sc  = 0
    for sd in all_results.values():
        if vk not in sd: continue
        for k in tot: tot[k] += sd[vk]['rejections'].get(k, 0)
        sc += sd[vk]['candles_scanned']
    return tot, sc


def report(all_results, portfolio):
    L = []
    L.append("=" * 72)
    L.append("  GMax V1 Backtest Report")
    L.append(f"  Period : {START_DT.strftime('%Y-%m')} → {END_YEAR}-{END_MONTH:02d}")
    L.append(f"  Coins  : {len(COINS)} | Capital: ${CAPITAL:,.0f} | Lev: {LEVERAGE}x | Risk: {RISK_PCT*100:.2f}%")
    L.append(f"  Fee: {FEE_RATE*100:.3f}%/side | Slip: {SLIP_RATE*100:.3f}%/side | No position cap")
    L.append("=" * 72)

    for vk, vcfg in VARIANTS.items():
        p      = portfolio[vk]
        rej, sc = agg_rej(all_results, vk)
        usable  = (p['profit_factor'] not in ('inf', 0.0) and
                   p['profit_factor'] >= 1.5 and p['win_rate'] >= 42.0)
        pf_s    = f"{p['profit_factor']:.4f}" if p['profit_factor'] != 'inf' else '∞'

        L.append(f"\n{'─'*72}")
        L.append(f"  {vcfg['label']}")
        L.append(f"{'─'*72}")
        L.append(f"  Trades    : {p['total']}  |  Wins: {p['wins']}  |  Losses: {p['losses']}")
        L.append(f"  Win Rate  : {p['win_rate']:.2f}%   (target ≥ 42%)")
        L.append(f"  Prof.Fact : {pf_s}   (target ≥ 1.5)")
        L.append(f"  Net PnL   : ${p['net_pnl']:+,.2f}")
        L.append(f"  Gross P/L : +${p['gross_profit']:,.2f} / -${p['gross_loss']:,.2f}")
        L.append(f"  Max DD    : ${p['max_dd']:,.2f}  ({p['max_dd_pct']:.2f}%)")
        L.append(f"  Avg W/L   : ${p['avg_win']:+.4f} / ${p['avg_loss']:+.4f}")
        L.append(f"  Longs     : {p['longs']}  WR {p['long_wr']:.2f}%")
        L.append(f"  Shorts    : {p['shorts']}  WR {p['short_wr']:.2f}%")
        L.append(f"  Verdict   : {'✅ USABLE' if usable else '❌ NOT USABLE'}")

        L.append(f"\n  ── Filter Rejections ──")
        L.append(f"  Scanned        : {sc:,}")
        L.append(f"  Warmup         : {rej['warmup']:,}")
        L.append(f"  No trend       : {rej['no_trend']:,}")
        L.append(f"  No cross       : {rej['no_cross']:,}")
        L.append(f"  Dir mismatch   : {rej['dir_mismatch']:,}")
        L.append(f"  ADX < 22       : {rej['adx_low']:,}")
        L.append(f"  In trade       : {rej['in_trade']:,}")
        L.append(f"  Signals fired  : {rej['signals']:,}")

        L.append(f"\n  ── Per-Coin (sorted by PF) ──")
        L.append(f"  {'Symbol':<22} {'Trades':>6} {'WR%':>7} {'PF':>7} {'NetPnL':>10} {'Bars':>7}")
        L.append("  " + "-" * 58)

        rows = []
        for sym, sd in all_results.items():
            if vk not in sd: continue
            r    = sd[vk]
            pf_v = r['profit_factor'] if r['profit_factor'] != 'inf' else 9999.0
            rows.append((sym, r['total'], r['win_rate'], pf_v,
                         r['net_pnl'], r['bars_available']))
        rows.sort(key=lambda x: x[3], reverse=True)
        for sym, tot, wr_c, pf_c, npnl, bars_av in rows:
            pf_str = f"{pf_c:.4f}" if pf_c < 9999 else "∞"
            L.append(f"  {sym:<22} {tot:>6} {wr_c:>7.2f} {pf_str:>7} {npnl:>+10.2f} {bars_av:>7}")

        L.append(f"\n  ── Monthly PnL ──")
        vals = list(p['monthly'].values())
        mx   = max(abs(v) for v in vals) if vals else 1
        for ym, v in p['monthly'].items():
            bar  = "█" * max(1, int(abs(v) / mx * 30)) if v else ""
            sign = "+" if v >= 0 else "-"
            L.append(f"  {ym}  {sign}${abs(v):>9.2f}  {bar}")

    L.append("\n" + "=" * 72)
    L.append("  END OF REPORT")
    L.append("=" * 72)
    return "\n".join(L)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"[GMax] {len(COINS)} coins | 4 variants | {LEVERAGE}x lev", flush=True)
    print(f"[GMax] Fetch: {FETCH_THREADS} threads | Backtest: {BACKTEST_PROCS} procs", flush=True)
    print(f"[GMax] Period: {START_DT.strftime('%Y-%m')} → {END_YEAR}-{END_MONTH:02d}", flush=True)

    # Phase 1: Download — threads crush this (pure network I/O)
    print("\n[Phase 1] Downloading data...", flush=True)
    t0          = datetime.now()
    symbol_bars = fetch_all(COINS)
    print(f"[Phase 1] Done in {(datetime.now()-t0).seconds}s", flush=True)

    live = {s: b for s, b in symbol_bars.items() if len(b) >= 100}
    dead = [s for s, b in symbol_bars.items() if len(b) < 100]
    print(f"  {len(live)} tradeable | {len(dead)} skipped (no data)", flush=True)
    if dead:
        print(f"  Skipped: {', '.join(dead)}", flush=True)
    for s, b in sorted(live.items()):
        print(f"  {s:<26} {len(b)} bars", flush=True)

    if not live:
        print("ERROR: no data"); sys.exit(1)

    # Phase 2: Backtest — processes for true CPU parallelism
    print(f"\n[Phase 2] Backtesting {len(live)} symbols × 4 variants...", flush=True)
    t1          = datetime.now()
    all_results = {}
    with ProcessPoolExecutor(max_workers=BACKTEST_PROCS) as ex:
        futs = {ex.submit(backtest_symbol, (s, b)): s for s, b in live.items()}
        done = 0
        for fut in as_completed(futs):
            sym, res = fut.result()
            all_results[sym] = res
            done += 1
            vd   = res.get('VAR_D', {})
            pf_s = f"PF={vd['profit_factor']}" if vd.get('total', 0) > 0 else "0 trades"
            print(f"  [{done:>3}/{len(live)}] {sym:<26} VAR_D {pf_s}", flush=True)
    print(f"[Phase 2] Done in {(datetime.now()-t1).seconds}s", flush=True)

    # Phase 3: Aggregate + output
    print("\n[Phase 3] Aggregating...", flush=True)
    portfolio = aggregate(all_results)

    txt = report(all_results, portfolio)
    print(txt)

    # ── 2 output files only ───────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write(txt)

    jout = {
        'meta': {
            'capital':       CAPITAL,
            'risk_pct':      RISK_PCT,
            'leverage':      LEVERAGE,
            'fee_rate':      FEE_RATE,
            'slip_rate':     SLIP_RATE,
            'max_hold_bars': MAX_HOLD_BARS,
            'interval':      INTERVAL,
            'coins':         len(COINS),
            'start':         START_DT.strftime('%Y-%m'),
            'end':           f"{END_YEAR}-{END_MONTH:02d}",
        },
        'variants': {},
    }
    for vk in VARIANTS:
        p        = portfolio[vk]
        per_coin = []
        for sym, sd in all_results.items():
            if vk in sd:
                r = sd[vk]
                per_coin.append({
                    'symbol':        sym,
                    'total':         r['total'],
                    'wins':          r['wins'],
                    'losses':        r['losses'],
                    'win_rate':      r['win_rate'],
                    'profit_factor': r['profit_factor'],
                    'net_pnl':       r['net_pnl'],
                    'bars':          r['bars_available'],
                })
        per_coin.sort(
            key=lambda x: (x['profit_factor'] if x['profit_factor'] != 'inf' else 9999),
            reverse=True,
        )
        rej, sc = agg_rej(all_results, vk)
        jout['variants'][vk] = {
            'aggregate':    {k: v for k, v in p.items() if k != 'trades'},
            'per_coin':     per_coin,
            'filter_stats': {'total_scanned': sc, **rej},
            'monthly_pnl':  p['monthly'],
            'trades':       p['trades'],
        }

    with open("backtest_report.json", "w") as f:
        json.dump(jout, f, indent=2)

    total_secs = (datetime.now() - t0).seconds
    print(f"\n[Done] Total: {total_secs}s | backtest_summary.txt + backtest_report.json", flush=True)


if __name__ == '__main__':
    main()
