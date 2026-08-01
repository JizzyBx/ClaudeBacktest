"""
InfinityX V8 / V9 Backtest
============================
Strategies : V1, V2, V3, V4, V5, V7  (S3/S4 excluded)
TP/SL      : v8_style (TP×1.7 SL×1.7)  |  v9_style (TP×1.2 SL×2.0)
ADX        : 22 (fixed)
Leverage   : 10x  (position size × 10, no liquidation tracking)
Data       : data.binance.vision futures monthly archive
Runtime    : single job, threaded data fetch (8 threads)
"""

import io, json, csv, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Constants ──────────────────────────────────────────────────────────────────
START_YEAR, START_MONTH = 2024, 1
END_YEAR,   END_MONTH   = 2025, 6

CAPITAL_START  = 10_000.0
RISK_PCT       = 0.0075   # 0.75% risk per trade (on leveraged notional)
FEE_RATE       = 0.0005   # 0.05% per side
SLIPPAGE_RATE  = 0.0002   # 0.02% per side
LEVERAGE       = 10
MAX_POSITIONS  = 6
WARMUP_BARS    = 400
ADX_MIN        = 22

TPSL_VARIANTS = {
    'v8_style': (1.7, 1.7),
    'v9_style': (1.2, 2.0),
}

STRATEGIES = {
    'V1': {
        'name': 'V1 · 15m Original', 'logic': 'core',
        'tf': '15m', 'tp': 3.0, 'sl': 2.0,
        'coins': ['XRPUSDT','TIAUSDT','TURBOUSDT','SEIUSDT','1000RATSUSDT',
                  '1000BONKUSDT','EIGENUSDT','APTUSDT','REZUSDT','POPCATUSDT',
                  'DOGEUSDT','AVAXUSDT','BTCUSDT','LDOUSDT','BNBUSDT',
                  'BOMEUSDT','FETUSDT','RUNEUSDT','ATOMUSDT','STXUSDT',
                  'AXSUSDT','ALGOUSDT','TRXUSDT'],
    },
    'V2': {
        'name': 'V2 · 15m Tight Exits', 'logic': 'core',
        'tf': '15m', 'tp': 2.8, 'sl': 1.7,
        'coins': ['1000RATSUSDT','XRPUSDT','TIAUSDT','TURBOUSDT','SEIUSDT',
                  '1000BONKUSDT','BTCUSDT','EIGENUSDT','APTUSDT','REZUSDT',
                  'POPCATUSDT','AVAXUSDT','DOGEUSDT','LDOUSDT','BNBUSDT',
                  'RUNEUSDT','BOMEUSDT','FETUSDT','AXSUSDT','ATOMUSDT',
                  'STXUSDT','TRXUSDT','ALGOUSDT'],
    },
    'V3': {
        'name': 'V3 · 30m Candles', 'logic': 'core',
        'tf': '30m', 'tp': 3.0, 'sl': 2.0,
        'coins': ['BTCUSDT','XRPUSDT','TIAUSDT','BNBUSDT','DOGEUSDT','SEIUSDT',
                  'APTUSDT','AVAXUSDT','FETUSDT','TRXUSDT','ALGOUSDT','STXUSDT',
                  'DOTUSDT'],
    },
    'V4': {
        'name': 'V4 · 1H Candles', 'logic': 'core',
        'tf': '1h', 'tp': 3.0, 'sl': 2.0,
        'coins': ['BTCUSDT','TRUMPUSDT','AVAXUSDT','XRPUSDT','TIAUSDT','BNBUSDT',
                  'DOGEUSDT','SEIUSDT','APTUSDT','SOLUSDT','FETUSDT','ATOMUSDT',
                  'STXUSDT','DOTUSDT','ALGOUSDT','TRXUSDT','RUNEUSDT','LDOUSDT',
                  'AXSUSDT','REZUSDT'],
    },
    'V5': {
        'name': 'V5 · 15m + RSI Confirm', 'logic': 'core_rsi',
        'tf': '15m', 'tp': 3.0, 'sl': 2.0,
        'rsi_long': (45, 70), 'rsi_short': (30, 55),
        'coins': ['TIAUSDT','XRPUSDT','TURBOUSDT','SEIUSDT','DOGEUSDT',
                  '1000RATSUSDT','APTUSDT','BTCUSDT','REZUSDT','POPCATUSDT',
                  'AVAXUSDT','BNBUSDT','FETUSDT','LDOUSDT','RUNEUSDT',
                  'BOMEUSDT','AXSUSDT','ATOMUSDT','STXUSDT','ALGOUSDT','TRXUSDT'],
    },
    'V7': {
        'name': 'V7 · 15m Clean Whitelist', 'logic': 'core',
        'tf': '15m', 'tp': 2.8, 'sl': 1.7,
        'coins': ['DOGEUSDT','TIAUSDT','XRPUSDT','SEIUSDT','APTUSDT','REZUSDT',
                  'AXSUSDT','1000XECUSDT','STXUSDT','ALGOUSDT','TRXUSDT',
                  'FETUSDT','DOTUSDT'],
    },
}

# ── Data fetching ──────────────────────────────────────────────────────────────
def fetch_monthly(symbol, interval, year, month):
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines/"
           f"{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404: return []
        raise
    rows = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        with z.open(z.namelist()[0]) as f:
            for row in csv.reader(io.TextIOWrapper(f, 'utf-8')):
                if not row or row[0].startswith('open_time'): continue
                try:
                    ts = int(row[0])
                    if ts > 10**14: ts //= 1000
                    rows.append({'ts': ts, 'open': float(row[1]),
                                 'high': float(row[2]), 'low': float(row[3]),
                                 'close': float(row[4]), 'vol': float(row[5])})
                except (ValueError, IndexError):
                    continue
    return rows

def fetch_all(symbol, interval):
    rows = []
    y, m = START_YEAR, START_MONTH
    while (y, m) <= (END_YEAR, END_MONTH):
        rows.extend(fetch_monthly(symbol, interval, y, m))
        m += 1
        if m > 12: m = 1; y += 1
    rows.sort(key=lambda r: r['ts'])
    return rows

def fetch_parallel(pairs, workers=8):
    cache = {}
    def job(pair):
        sym, tf = pair
        return pair, fetch_all(sym, tf)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(job, p): p for p in pairs}
        total = len(futs); done = 0
        for fut in as_completed(futs):
            pair, bars = fut.result()
            cache[pair] = bars
            done += 1
            print(f"  [{done}/{total}] {pair[0]} {pair[1]} → {len(bars)} bars")
    return cache

# ── Indicators ─────────────────────────────────────────────────────────────────
def ema(vals, p):
    if not vals: return []
    k = 2.0 / (p + 1); r = [vals[0]]
    for v in vals[1:]: r.append(v * k + r[-1] * (1 - k))
    return r

def rsi(closes, p=14):
    if len(closes) < p + 1: return 50.0
    g, l = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        g.append(max(d, 0.0)); l.append(max(-d, 0.0))
    ag = sum(g[:p]) / p; al = sum(l[:p]) / p
    for i in range(p, len(g)):
        ag = (ag*(p-1) + g[i]) / p; al = (al*(p-1) + l[i]) / p
    return 100.0 if al == 0 else 100 - 100 / (1 + ag/al)

def atr(highs, lows, closes, p=14):
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]),
               abs(lows[i]-closes[i-1])) for i in range(1, len(closes))]
    if not trs: return closes[-1] * 0.005
    if len(trs) < p: return sum(trs) / len(trs)
    a = sum(trs[:p]) / p
    for t in trs[p:]: a = (a*(p-1) + t) / p
    return a

def adx(highs, lows, closes, p=14):
    if len(closes) < p * 3: return 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up = highs[i]-highs[i-1]; dn = lows[i-1]-lows[i]
        pdm.append(up if up > dn and up > 0 else 0.0)
        mdm.append(dn if dn > up and dn > 0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    def ws(v):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r
    st = ws(trs); sp = ws(pdm); sm = ws(mdm)
    if not st: return 0.0
    pdi = [100*a/b if b else 0 for a,b in zip(sp,st)]
    mdi = [100*a/b if b else 0 for a,b in zip(sm,st)]
    dx  = [100*abs(a-b)/(a+b) if (a+b) else 0 for a,b in zip(pdi,mdi)]
    if len(dx) < p: return 0.0
    v = sum(dx[:p]) / p
    for d in dx[p:]: v = (v*(p-1) + d) / p
    return max(0.0, min(100.0, v))

def slope_pct(e50):
    if len(e50) < 11 or e50[-11] == 0: return 0.0
    return (e50[-1] - e50[-11]) / e50[-11] * 100

# ── Signal logic ───────────────────────────────────────────────────────────────
def signal_core(bars, tp_mult, sl_mult):
    closes = [b['close'] for b in bars]
    highs  = [b['high']  for b in bars]
    lows   = [b['low']   for b in bars]
    e9 = ema(closes, 9); e21 = ema(closes, 21); e50 = ema(closes, 50)
    adx_val = adx(highs, lows, closes)
    sl_val  = slope_pct(e50)
    if adx_val < ADX_MIN: return None, None, None
    crossed_up   = e9[-1] > e21[-1] and e9[-2] <= e21[-2]
    crossed_down = e9[-1] < e21[-1] and e9[-2] >= e21[-2]
    sig = None
    if sl_val > 0.05  and crossed_up:   sig = 'buy'
    if sl_val < -0.05 and crossed_down:  sig = 'sell'
    if not sig: return None, None, None
    a = atr(highs, lows, closes)
    return sig, tp_mult * a, sl_mult * a

def signal_core_rsi(bars, tp_mult, sl_mult, rsi_long=(45,70), rsi_short=(30,55)):
    sig, tp, sl = signal_core(bars, tp_mult, sl_mult)
    if not sig: return None, None, None
    closes = [b['close'] for b in bars]
    r = rsi(closes)
    lo, hi = rsi_long if sig == 'buy' else rsi_short
    if not (lo <= r <= hi): return None, None, None
    return sig, tp, sl

# ── Portfolio ──────────────────────────────────────────────────────────────────
class Portfolio:
    def __init__(self):
        self.capital = CAPITAL_START
        self.positions = {}
        self.trades = []
        self.equity = [CAPITAL_START]

    def can_open(self, sym): return sym not in self.positions and len(self.positions) < MAX_POSITIONS

    def open(self, sym, side, entry, tp_dist, sl_dist, ts):
        notional    = self.capital * RISK_PCT * LEVERAGE
        sl_pct      = sl_dist / entry
        qty         = (notional / entry) / sl_pct if sl_pct > 0 else 0
        cost        = entry * qty * (FEE_RATE + SLIPPAGE_RATE)
        self.capital -= cost
        self.positions[sym] = {
            'side': side, 'entry': entry, 'qty': qty,
            'tp': entry + tp_dist if side == 'buy' else entry - tp_dist,
            'sl': entry - sl_dist if side == 'buy' else entry + sl_dist,
            'open_ts': ts,
        }

    def check_close(self, sym, bar, ts):
        pos = self.positions.get(sym)
        if not pos: return
        h, l = bar['high'], bar['low']
        side, tp, sl, qty = pos['side'], pos['tp'], pos['sl'], pos['qty']
        closed = None
        if side == 'buy':
            if l <= sl: closed = ('sl', sl)
            elif h >= tp: closed = ('tp', tp)
        else:
            if h >= sl: closed = ('sl', sl)
            elif l <= tp: closed = ('tp', tp)
        if not closed: return
        reason, exit_price = closed
        gross = (exit_price - pos['entry']) * qty if side == 'buy' else (pos['entry'] - exit_price) * qty
        fee   = exit_price * qty * (FEE_RATE + SLIPPAGE_RATE)
        net   = gross - fee
        self.capital += net
        self.trades.append({'symbol': sym, 'side': side, 'entry': pos['entry'],
                            'exit': exit_price, 'pnl': net, 'reason': reason,
                            'open_ts': pos['open_ts'], 'close_ts': ts})
        self.equity.append(self.capital)
        del self.positions[sym]

    def force_close_all(self, last_bars, ts):
        for sym in list(self.positions.keys()):
            bar = last_bars.get(sym)
            if not bar: continue
            pos = self.positions[sym]
            ep  = bar['close']; qty = pos['qty']
            gross = (ep - pos['entry']) * qty if pos['side'] == 'buy' else (pos['entry'] - ep) * qty
            fee   = ep * qty * (FEE_RATE + SLIPPAGE_RATE)
            self.capital += gross - fee
            self.trades.append({'symbol': sym, 'side': pos['side'], 'entry': pos['entry'],
                                'exit': ep, 'pnl': gross - fee, 'reason': 'end_of_data',
                                'open_ts': pos['open_ts'], 'close_ts': ts})
        self.positions.clear()

# ── Stats ──────────────────────────────────────────────────────────────────────
def stats(trades, equity):
    if not trades:
        return {'total_trades': 0, 'win_rate': 0, 'profit_factor': 0,
                'net_pnl': 0, 'max_drawdown': 0, 'expectancy': 0,
                'final_capital': CAPITAL_START}
    wins   = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] < 0]
    gp = sum(wins); gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0)
    wr = len(wins) / len(trades) * 100
    aw = sum(wins)/len(wins)   if wins   else 0
    al = sum(losses)/len(losses) if losses else 0
    peak = equity[0]; mdd = 0
    for v in equity:
        peak = max(peak, v); mdd = max(mdd, peak - v)
    longs  = [t for t in trades if t['side'] == 'buy']
    shorts = [t for t in trades if t['side'] == 'sell']
    lwr = len([t for t in longs  if t['pnl']>0])/len(longs)*100  if longs  else 0
    swr = len([t for t in shorts if t['pnl']>0])/len(shorts)*100 if shorts else 0
    return {
        'total_trades': len(trades), 'win_rate': round(wr, 2),
        'profit_factor': round(pf, 3), 'net_pnl': round(equity[-1]-equity[0], 2),
        'max_drawdown': round(mdd, 2), 'expectancy': round((wr/100*aw)+((1-wr/100)*al), 4),
        'avg_win': round(aw, 4), 'avg_loss': round(al, 4),
        'longs': len(longs), 'shorts': len(shorts),
        'long_wr': round(lwr, 2), 'short_wr': round(swr, 2),
        'final_capital': round(equity[-1], 2),
        'meets_targets': pf >= 1.5 and wr >= 42,
    }

def per_coin(trades):
    cd = defaultdict(list)
    for t in trades: cd[t['symbol']].append(t)
    rows = []
    for sym, ts in cd.items():
        wins = [t['pnl'] for t in ts if t['pnl'] > 0]
        losses = [t['pnl'] for t in ts if t['pnl'] < 0]
        gp = sum(wins); gl = abs(sum(losses))
        pf = gp/gl if gl > 0 else (999.0 if gp > 0 else 0)
        rows.append({'symbol': sym, 'trades': len(ts),
                     'win_rate': round(len(wins)/len(ts)*100, 1),
                     'profit_factor': round(pf, 3),
                     'net_pnl': round(sum(t['pnl'] for t in ts), 2)})
    return sorted(rows, key=lambda r: r['profit_factor'], reverse=True)

def monthly_pnl(trades):
    m = defaultdict(float)
    for t in trades:
        dt = datetime.fromtimestamp(t['close_ts']/1000, tz=timezone.utc)
        m[f"{dt.year}-{dt.month:02d}"] += t['pnl']
    return {k: round(v, 2) for k, v in sorted(m.items())}

# ── Run one variant ────────────────────────────────────────────────────────────
def run_variant(strat_id, cfg, tp_mult, sl_mult, tpsl_name, klines_cache):
    logic = cfg['logic']; coins = cfg['coins']; tf = cfg['tf']

    coin_bars = {c: klines_cache.get((c, tf), []) for c in coins}
    all_ts = sorted({b['ts'] for bars in coin_bars.values() for b in bars})
    coin_idx = {c: {b['ts']: i for i, b in enumerate(bars)}
                for c, bars in coin_bars.items()}

    port = Portfolio()
    for ts in all_ts:
        cur = {c: coin_bars[c][coin_idx[c][ts]]
               for c in coins if ts in coin_idx.get(c, {})}

        for sym in list(port.positions):
            if sym in cur: port.check_close(sym, cur[sym], ts)

        for sym in coins:
            if not port.can_open(sym): continue
            if sym not in cur: continue
            bi = coin_idx.get(sym, {}).get(ts)
            if bi is None or bi < WARMUP_BARS: continue
            window = coin_bars[sym][max(0, bi-499): bi+1]
            if len(window) < 60: continue
            try:
                if logic == 'core':
                    sig, tp_d, sl_d = signal_core(window, tp_mult, sl_mult)
                elif logic == 'core_rsi':
                    sig, tp_d, sl_d = signal_core_rsi(
                        window, tp_mult, sl_mult,
                        cfg.get('rsi_long', (45,70)), cfg.get('rsi_short', (30,55)))
                else: continue
            except Exception: continue
            if not sig: continue
            nbi = bi + 1
            if nbi >= len(coin_bars[sym]): continue
            nb = coin_bars[sym][nbi]
            if nb['open'] <= 0: continue
            port.open(sym, sig, nb['open'], tp_d, sl_d, nb['ts'])

    last_bars = {c: coin_bars[c][-1] for c in coins if coin_bars.get(c)}
    port.force_close_all(last_bars, all_ts[-1] if all_ts else 0)

    s = stats(port.trades, port.equity)
    s['variant_id']  = f"{strat_id}|ADX{ADX_MIN}|{tpsl_name}"
    s['strategy']    = strat_id
    s['tpsl']        = tpsl_name
    s['tp_mult']     = tp_mult
    s['sl_mult']     = sl_mult
    s['leverage']    = LEVERAGE
    s['per_coin']    = per_coin(port.trades)
    s['monthly_pnl'] = monthly_pnl(port.trades)
    return s

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("InfinityX V8/V9 Backtest | ADX22 | 10x Leverage")
    print(f"Period: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print(f"Capital: ${CAPITAL_START:,.0f} | Risk: {RISK_PCT*100}% × {LEVERAGE}x = "
          f"{RISK_PCT*LEVERAGE*100}% notional per trade")
    print("="*60)

    # Collect all (coin, tf) pairs needed
    needed = set()
    for cfg in STRATEGIES.values():
        for coin in cfg['coins']:
            needed.add((coin, cfg['tf']))

    print(f"\nFetching {len(needed)} (coin, tf) pairs with 8 threads...")
    klines_cache = fetch_parallel(needed, workers=8)

    # Build all 12 variants (6 strategies × 2 tpsl)
    variants = []
    for strat_id, cfg in STRATEGIES.items():
        for tpsl_name, (tp_mult, sl_mult) in TPSL_VARIANTS.items():
            variants.append((strat_id, cfg, tp_mult, sl_mult, tpsl_name))

    print(f"\nRunning {len(variants)} variants...")
    results = []
    for strat_id, cfg, tp_mult, sl_mult, tpsl_name in variants:
        vid = f"{strat_id}|ADX{ADX_MIN}|{tpsl_name}"
        print(f"  {vid}...", end='', flush=True)
        r = run_variant(strat_id, cfg, tp_mult, sl_mult, tpsl_name, klines_cache)
        results.append(r)
        tag = "✅ PASS" if r['meets_targets'] else "❌"
        print(f" PF={r['profit_factor']} WR={r['win_rate']}% "
              f"Trades={r['total_trades']} Net=${r['net_pnl']:+.2f} {tag}")

    results.sort(key=lambda r: r['profit_factor'], reverse=True)
    passing = [r for r in results if r['meets_targets']]

    # ── Summary output ─────────────────────────────────────────────────────────
    lines = []
    lines.append("="*70)
    lines.append("INFINITYX V8/V9 BACKTEST RESULTS")
    lines.append("="*70)
    lines.append(f"Period   : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    lines.append(f"Capital  : ${CAPITAL_START:,.0f}  |  Leverage: {LEVERAGE}x")
    lines.append(f"Risk/trade: {RISK_PCT*100}% × {LEVERAGE}x = {RISK_PCT*LEVERAGE*100:.1f}% notional")
    lines.append(f"Fee      : {FEE_RATE*100}%/side  |  Slippage: {SLIPPAGE_RATE*100}%/side")
    lines.append(f"ADX      : {ADX_MIN} (fixed)")
    lines.append(f"\nTotal variants : {len(results)}")
    lines.append(f"Passing (PF≥1.5 & WR≥42%) : {len(passing)}")
    lines.append("")
    lines.append(f"{'VARIANT':<30} {'PF':>6} {'WR%':>6} {'Trades':>7} {'Net$':>10} {'MaxDD$':>9} {'Status'}")
    lines.append("-"*75)
    for r in results:
        tag = "✅ PASS" if r['meets_targets'] else "❌ fail"
        lines.append(f"{r['variant_id']:<30} {r['profit_factor']:>6.3f} "
                     f"{r['win_rate']:>6.1f} {r['total_trades']:>7d} "
                     f"{r['net_pnl']:>+10.2f} {r['max_drawdown']:>9.2f} {tag}")

    lines.append("")
    lines.append("="*70)
    lines.append("PASSING VARIANTS DETAIL")
    lines.append("="*70)
    if not passing:
        lines.append("No variants passed PF≥1.5 and WR≥42%.")
    for r in passing:
        lines.append(f"\n✅ {r['variant_id']} | TP×{r['tp_mult']} SL×{r['sl_mult']} | {LEVERAGE}x leverage")
        lines.append(f"   PF={r['profit_factor']} | WR={r['win_rate']}% | "
                     f"Trades={r['total_trades']} | Net=${r['net_pnl']:+.2f} | MaxDD=${r['max_drawdown']:.2f}")
        lines.append(f"   Longs={r['longs']}({r['long_wr']}%WR) Shorts={r['shorts']}({r['short_wr']}%WR)")
        lines.append("   Per-coin top 10:")
        for pc in r['per_coin'][:10]:
            flag = "✅" if pc['profit_factor'] >= 1.5 else "  "
            lines.append(f"    {flag} {pc['symbol']:20s} PF={pc['profit_factor']:.3f} "
                         f"WR={pc['win_rate']}% T={pc['trades']} Net=${pc['net_pnl']:+.2f}")
        lines.append("   Monthly PnL:")
        lines.append("   " + " | ".join(f"{m}: ${v:+.2f}" for m, v in r['monthly_pnl'].items()))

    lines.append("")
    lines.append("="*70)
    lines.append("V8 vs V9 COMPARISON")
    lines.append("="*70)
    for tname in ['v8_style', 'v9_style']:
        group = [r for r in results if r['tpsl'] == tname]
        avg_pf = sum(r['profit_factor'] for r in group) / len(group) if group else 0
        best   = max(group, key=lambda r: r['profit_factor']) if group else None
        n_pass = sum(1 for r in group if r['meets_targets'])
        lines.append(f"  {tname}: avg PF={avg_pf:.3f} | best={best['variant_id']} "
                     f"PF={best['profit_factor']} | passing={n_pass}")

    summary = "\n".join(lines)
    print("\n" + summary)

    with open("v8v9_summary.txt", "w") as f:
        f.write(summary)

    report = {
        'meta': {
            'period': f"{START_YEAR}-{START_MONTH:02d} / {END_YEAR}-{END_MONTH:02d}",
            'capital': CAPITAL_START, 'leverage': LEVERAGE,
            'risk_pct': RISK_PCT, 'fee_rate': FEE_RATE,
            'slippage': SLIPPAGE_RATE, 'adx_min': ADX_MIN,
        },
        'results': results,
        'passing_count': len(passing),
        'total_count': len(results),
    }
    with open("v8v9_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nDone. Summary → v8v9_summary.txt | JSON → v8v9_report.json")

if __name__ == '__main__':
    main()
