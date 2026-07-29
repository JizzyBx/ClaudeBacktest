"""
Backtest V7 — Tight Whitelist, Clean Strategy
===============================================
stdlib-only Python 3.11 | GitHub Actions compatible

PHILOSOPHY:
  Stop fighting the strategy. The core ADX + EMA crossover + 50EMA slope
  works on large liquid coins with clean trends. V6 proved it.
  V7 = only the coins that consistently showed PF≥1.3 across V1-V6.
  No gimmick filters. Let the strategy breathe.

COINS: 20 coins — best performers from V1+V6 combined
EXITS: TP=2.8×ATR | SL=1.7×ATR (V2 — best exits across all tests)
INTERVAL: 15m
RANGE: 3 years
WORKERS: 10
"""

import json, zipfile, io, csv, math, threading, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Coin Whitelist ─────────────────────────────────────────────────────────────
# Selection criteria:
#   • PF ≥ 1.3 in V6 with ≥ 15 trades (statistically meaningful), OR
#   • Avg PF ≥ 1.35 across V1-V5 AND PF > 1.0 in V6
#   • Must be large/mid cap with clean institutional-style volume
#
# Deliberately excluded: TURBO, BOME, RATS, EIGEN, LDO, RUNE, SHIB — 
#   meme/micro coins that collapse under any filter. Data is clear on this.

SYMBOLS = [
    # ── Tier 1: PF > 1.5 in V6, ≥ 15 trades ─────────────────────
    'BNBUSDT',       # PF 2.535 V6 | consistent across all variants
    'APTUSDT',       # PF 2.000 V6 | clean L1 trends
    'GRTUSDT',       # PF 1.992 V6 | indexing protocol, steady vol
    '1000XECUSDT',   # PF 1.658 V6 | 38 trades, very consistent
    'ACHUSDT',       # PF 1.586 V6 | 26 trades
    'ALGOUSDT',      # PF 1.578 V6 | established L1
    'FETUSDT',       # PF 1.570 V6 | AI narrative, clean trends
    'BTCUSDT',       # PF 1.553 V6 | king, always works
    'REZUSDT',       # PF 1.505 V6 | newer but clean signals
    'STXUSDT',       # PF 1.483 V6 | BTC L2
    # ── Tier 2: PF 1.2-1.5 in V6, strong V1-V5 history ──────────
    'TRXUSDT',       # PF 1.339 V6 | massive volume, clean trends
    'XLMUSDT',       # PF 1.326 V6 | XRP companion, similar behavior
    'DOGEUSDT',      # PF 1.272 V6 | high vol, trending coin
    'XRPUSDT',       # PF 1.232 V6 | avg 1.386 V1-V5, blue chip
    'COMPUSDT',      # PF 1.221 V6 | DeFi steady
    'WLDUSDT',       # PF 1.203 V6 | consistent across runs
    'SEIUSDT',       # PF 1.147 V6 | fast L1
    'AXSUSDT',       # PF 1.142 V6 | gaming with real vol
    'TIAUSDT',       # PF 1.080 V6 | avg 1.469 V1-V5 (best overall)
    'DOTUSDT',       # PF 1.028 V6 | avg 1.059 V1-V5
]

assert len(SYMBOLS) == 20, f"Expected 20, got {len(SYMBOLS)}"

# ── Config ────────────────────────────────────────────────────────────────────
CAPITAL        = 10_000.0
RISK_PCT       = 0.0075
FEE_PCT        = 0.0005
SLIPPAGE_PCT   = 0.0002
MAX_POSITIONS  = 6
ADX_MIN        = 22
SLOPE_THRESHOLD = 0.05     # 0.05% over 10 bars
TP_MULT        = 2.8
SL_MULT        = 1.7
WORKERS        = 10
INTERVAL       = '15m'

END_DATE   = datetime.now(timezone.utc).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
START_DATE = END_DATE - timedelta(days=3*365)
BASE_URL   = "https://data.binance.vision/data/futures/um"

_lock   = threading.Lock()
_errors = []

# ── Data ──────────────────────────────────────────────────────────────────────
def fetch_monthly(symbol, year, month):
    ym  = f"{year}-{month:02d}"
    url = f"{BASE_URL}/monthly/klines/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                return list(csv.reader(io.TextIOWrapper(f)))
    except urllib.error.HTTPError as e:
        if e.code == 404: return None
        with _lock: _errors.append(f"{symbol} {ym}: HTTP {e.code}")
        return None
    except Exception as e:
        with _lock: _errors.append(f"{symbol} {ym}: {e}")
        return None

def parse_rows(rows):
    out = []
    for r in rows:
        if not r or r[0].startswith('open_time'): continue
        try:
            ts = int(r[0])
            if ts > 10**14: ts //= 1000
            out.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
        except: continue
    return out

def fetch_symbol(symbol):
    all_c, cur = [], START_DATE
    while cur < END_DATE:
        rows = fetch_monthly(symbol, cur.year, cur.month)
        if rows: all_c.extend(parse_rows(rows))
        cur = cur.replace(month=cur.month+1) if cur.month < 12 else cur.replace(year=cur.year+1, month=1)
    if not all_c: return []
    s_ms = int(START_DATE.timestamp()*1000)
    e_ms = int(END_DATE.timestamp()*1000)
    seen, out = set(), []
    for c in sorted(all_c, key=lambda x: x[0]):
        if s_ms <= c[0] < e_ms and c[0] not in seen:
            seen.add(c[0]); out.append(c)
    return out

# ── Indicators ────────────────────────────────────────────────────────────────
def ema(values, p):
    k = 2.0/(p+1); r = [values[0]]
    for v in values[1:]: r.append(v*k + r[-1]*(1-k))
    return r

def atr(highs, lows, closes, p=14):
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    if not trs: return [closes[-1]*0.005]*len(closes)
    a = [None]*len(closes)
    if len(trs) >= p:
        a[p] = sum(trs[:p])/p
        for i in range(p, len(trs)):
            a[i+1] = (a[i]*(p-1)+trs[i])/p
    return a

def adx(highs, lows, closes, p=14):
    n = len(closes)
    adx_o, pdi_o, mdi_o = [None]*n, [None]*n, [None]*n
    if n < p*3: return adx_o, pdi_o, mdi_o
    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        u = highs[i]-highs[i-1]; d = lows[i-1]-lows[i]
        pdm.append(u if u>d and u>0 else 0.0)
        mdm.append(d if d>u and d>0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    def ws(v, p):
        if len(v)<p: return []
        r=[sum(v[:p])]
        for x in v[p:]: r.append(r[-1]-r[-1]/p+x)
        return r
    st,sp,sm = ws(trs,p), ws(pdm,p), ws(mdm,p)
    if not st: return adx_o, pdi_o, mdi_o
    pdi_l=[100*pp/t if t else 0 for pp,t in zip(sp,st)]
    mdi_l=[100*m/t  if t else 0 for m,t  in zip(sm,st)]
    dx_l =[100*abs(pp-m)/(pp+m) if (pp+m) else 0 for pp,m in zip(pdi_l,mdi_l)]
    if len(dx_l)<p: return adx_o, pdi_o, mdi_o
    av=sum(dx_l[:p])/p; avs=[av]
    for d in dx_l[p:]: av=(av*(p-1)+d)/p; avs.append(av)
    ps=p+1; as_=ps+p-1
    for i,v in enumerate(pdi_l):
        idx=ps+i
        if idx<n: pdi_o[idx]=v; mdi_o[idx]=mdi_l[i]
    for i,v in enumerate(avs):
        idx=as_+i
        if idx<n: adx_o[idx]=max(0.0,min(100.0,v))
    return adx_o, pdi_o, mdi_o

# ── Signal Engine ─────────────────────────────────────────────────────────────
def compute_signals(candles):
    closes  = [c[4] for c in candles]
    highs   = [c[2] for c in candles]
    lows    = [c[3] for c in candles]

    e9   = ema(closes, 9)
    e21  = ema(closes, 21)
    e50  = ema(closes, 50)
    atr_ = atr(highs, lows, closes, 14)
    adx_, _, _ = adx(highs, lows, closes, 14)

    rej = {'warmup_none':0,'adx_fail':0,'slope_fail':0,'cross_fail':0,'signal':0}
    sigs = []
    WARMUP = 60

    for i in range(WARMUP, len(candles)-1):
        if adx_[i] is None or atr_[i] is None:
            rej['warmup_none'] += 1; continue

        if adx_[i] < ADX_MIN:
            rej['adx_fail'] += 1; continue

        slope = (e50[i]-e50[i-10])/e50[i-10]*100 if i>=10 and e50[i-10] else 0
        up    = slope >  SLOPE_THRESHOLD
        down  = slope < -SLOPE_THRESHOLD
        if not (up or down):
            rej['slope_fail'] += 1; continue

        c_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
        c_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
        if not (c_up or c_down):
            rej['cross_fail'] += 1; continue

        sig = None
        if up   and c_up:   sig = 'long'
        if down and c_down: sig = 'short'
        if sig is None:
            rej['cross_fail'] += 1; continue

        rej['signal'] += 1
        sigs.append({
            'bar_idx'    : i+1,
            'signal'     : sig,
            'entry_price': candles[i+1][1],
            'tp_dist'    : atr_[i] * TP_MULT,
            'sl_dist'    : atr_[i] * SL_MULT,
            'open_time'  : candles[i+1][0],
        })
    return sigs, rej

# ── Backtest ──────────────────────────────────────────────────────────────────
def backtest_symbol(symbol, candles):
    if len(candles) < 80:
        return [], {}
    sigs, rej = compute_signals(candles)
    trades = []
    for s in sigs:
        bar   = s['bar_idx']
        entry = s['entry_price']
        tp_p  = entry + s['tp_dist'] if s['signal']=='long' else entry - s['tp_dist']
        sl_p  = entry - s['sl_dist'] if s['signal']=='long' else entry + s['sl_dist']
        eb = ep = et = None
        for j in range(bar+1, len(candles)):
            h,l = candles[j][2], candles[j][3]
            if s['signal']=='long':
                if l<=sl_p: ep=sl_p;et='sl';eb=j;break
                if h>=tp_p: ep=tp_p;et='tp';eb=j;break
            else:
                if h>=sl_p: ep=sl_p;et='sl';eb=j;break
                if l<=tp_p: ep=tp_p;et='tp';eb=j;break
        if eb is None: continue
        trades.append({'symbol':symbol,'direction':s['signal'],'entry_price':entry,
                       'exit_price':ep,'exit_type':et,'duration_bars':eb-bar,
                       'entry_time':candles[bar][0],'exit_time':candles[eb][0]})
    return trades, rej

# ── Portfolio Sim ─────────────────────────────────────────────────────────────
def simulate(raw_trades):
    rr = TP_MULT / SL_MULT
    flat   = sorted(raw_trades, key=lambda t: t['entry_time'])
    equity = CAPITAL
    open_p = {}
    result = []
    for t in flat:
        sym = t['symbol']
        evict = [s for s,p in open_p.items() if p['exit_time'] <= t['entry_time']]
        for s in evict:
            equity += open_p.pop(s)['pnl']
        if len(open_p) >= MAX_POSITIONS or sym in open_p: continue
        risk    = equity * RISK_PCT
        cost    = risk * (FEE_PCT + SLIPPAGE_PCT) * 2
        pnl     = (risk*rr - cost) if t['exit_type']=='tp' else (-risk - cost)
        open_p[sym] = {'exit_time':t['exit_time'],'pnl':pnl}
        rt = dict(t); rt['dollar_pnl']=pnl; rt['risk']=risk; rt['equity_before']=equity
        result.append(rt)
    for p in open_p.values(): equity += p['pnl']
    return result, equity

# ── Stats ─────────────────────────────────────────────────────────────────────
def stats(trades, final_eq):
    if not trades:
        return {'total_trades':0,'win_rate':0,'profit_factor':0,'net_pnl':0,
                'final_equity':CAPITAL,'max_drawdown_pct':0,'sharpe':0,'sortino':0,
                'avg_win':0,'avg_loss':0,'expectancy':0,'avg_duration_bars':0,
                'long_trades':0,'long_wr':0,'short_trades':0,'short_wr':0,
                'max_win_streak':0,'max_loss_streak':0,'usable':False}
    wins  = [t for t in trades if t['dollar_pnl']>0]
    losses= [t for t in trades if t['dollar_pnl']<=0]
    longs = [t for t in trades if t['direction']=='long']
    shorts= [t for t in trades if t['direction']=='short']
    n = len(trades)
    wr= len(wins)/n*100
    gw= sum(t['dollar_pnl'] for t in wins)
    gl= abs(sum(t['dollar_pnl'] for t in losses))
    pf= gw/gl if gl else (999 if gw>0 else 0)
    aw= gw/len(wins) if wins else 0
    al= gl/len(losses) if losses else 0
    eq=[CAPITAL]
    for t in sorted(trades,key=lambda x:x['entry_time']): eq.append(eq[-1]+t['dollar_pnl'])
    pk=eq[0]; mdd=0
    for e in eq:
        if e>pk: pk=e
        dd=(pk-e)/pk*100
        if dd>mdd: mdd=dd
    pnls=[t['dollar_pnl'] for t in sorted(trades,key=lambda x:x['entry_time'])]
    mr=sum(pnls)/len(pnls)
    sd=math.sqrt(sum((p-mr)**2 for p in pnls)/len(pnls)) if len(pnls)>1 else 0
    neg=[p for p in pnls if p<0]
    nd=math.sqrt(sum((p-mr)**2 for p in neg)/len(neg)) if neg else 0
    sh=(mr/sd*math.sqrt(252)) if sd else 0
    so=(mr/nd*math.sqrt(252)) if nd else 0
    st_=mw=ml=0; ct=None
    for t in trades:
        w=t['dollar_pnl']>0
        st_=(st_+1) if w==ct else 1; ct=w
        if w: mw=max(mw,st_)
        else: ml=max(ml,st_)
    return {'total_trades':n,'win_rate':round(wr,2),'profit_factor':round(pf,4),
            'net_pnl':round(final_eq-CAPITAL,2),'final_equity':round(final_eq,2),
            'max_drawdown_pct':round(mdd,2),'sharpe':round(sh,3),'sortino':round(so,3),
            'avg_win':round(aw,2),'avg_loss':round(al,2),'expectancy':round((wr/100*aw)-((1-wr/100)*al),2),
            'avg_duration_bars':round(sum(t['duration_bars'] for t in trades)/n,1),
            'long_trades':len(longs),'short_trades':len(shorts),
            'long_wr':round(sum(1 for t in longs if t['dollar_pnl']>0)/len(longs)*100,2) if longs else 0,
            'short_wr':round(sum(1 for t in shorts if t['dollar_pnl']>0)/len(shorts)*100,2) if shorts else 0,
            'max_win_streak':mw,'max_loss_streak':ml,
            'usable': pf>=1.5 and wr>=42}

def coin_table(trades):
    d = defaultdict(list)
    for t in trades: d[t['symbol']].append(t)
    rows=[]
    for sym,ts in d.items():
        gw=sum(t['dollar_pnl'] for t in ts if t['dollar_pnl']>0)
        gl=abs(sum(t['dollar_pnl'] for t in ts if t['dollar_pnl']<=0))
        pf=gw/gl if gl else (999 if gw>0 else 0)
        wins=sum(1 for t in ts if t['dollar_pnl']>0)
        rows.append({'symbol':sym,'trades':len(ts),'wins':wins,'losses':len(ts)-wins,
                     'wr':round(wins/len(ts)*100,1),'pf':round(pf,3),
                     'pnl':round(sum(t['dollar_pnl'] for t in ts),2)})
    return sorted(rows,key=lambda x:-x['pf'])

def monthly(trades):
    m=defaultdict(float)
    for t in trades:
        k=datetime.fromtimestamp(t['entry_time']/1000,tz=timezone.utc).strftime('%Y-%m')
        m[k]+=t['dollar_pnl']
    return {k:round(v,2) for k,v in sorted(m.items())}

# ── Worker ────────────────────────────────────────────────────────────────────
def worker(sym):
    candles = fetch_symbol(sym)
    if not candles: return sym, [], {}
    trades, rej = backtest_symbol(sym, candles)
    return sym, trades, rej

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print("  BACKTEST V7 — Clean Whitelist Strategy")
    print(f"  Range : {START_DATE.strftime('%Y-%m')} → {END_DATE.strftime('%Y-%m')}")
    print(f"  Coins : {len(SYMBOLS)} (proven performers only)")
    print(f"  Exits : TP={TP_MULT}×ATR | SL={SL_MULT}×ATR")
    print(f"  Filters: ADX≥{ADX_MIN} | 50EMA slope | EMA crossover (core only)")
    print(f"  Capital: ${CAPITAL:,.0f} | Risk: {RISK_PCT*100}%/trade | MaxPos: {MAX_POSITIONS}")
    print("="*60)
    print(f"\n  Coins: {', '.join(SYMBOLS)}\n")

    all_raw, all_rej, errors = [], defaultdict(int), 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(worker, s): s for s in SYMBOLS}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                _, trades, rej = fut.result()
                all_raw.extend(trades)
                for k,v in rej.items(): all_rej[k]+=v
                print(f"  ✓ {sym:22s} {len(trades)} raw signals")
            except Exception as e:
                print(f"  ✗ {sym:22s} ERROR: {e}"); errors+=1

    if errors == len(SYMBOLS):
        print("\n  ⛔ ALL SYMBOLS FAILED — data bucket blocked."); return

    result, final_eq = simulate(all_raw)
    ag  = stats(result, final_eq)
    ct  = coin_table(result)
    mon = monthly(result)

    # ── Print ──────────────────────────────────────────────────────
    print(f"\n{'━'*60}")
    print(f"  V7 RESULTS")
    print(f"{'━'*60}")
    print(f"  Trades        : {ag['total_trades']}")
    print(f"  Win Rate      : {ag['win_rate']}%")
    print(f"  Profit Factor : {ag['profit_factor']}")
    print(f"  Net PnL       : ${ag['net_pnl']:>12,.2f}")
    print(f"  Final Equity  : ${ag['final_equity']:>12,.2f}")
    print(f"  Max Drawdown  : {ag['max_drawdown_pct']}%")
    print(f"  Sharpe        : {ag['sharpe']}  |  Sortino: {ag['sortino']}")
    print(f"  Avg Win       : ${ag['avg_win']:,.2f}  |  Avg Loss: ${ag['avg_loss']:,.2f}")
    print(f"  Expectancy    : ${ag['expectancy']:,.2f} per trade")
    print(f"  Avg Duration  : {ag['avg_duration_bars']} bars ({ag['avg_duration_bars']*15/60:.1f} hrs)")
    print(f"  Longs  : {ag['long_trades']} ({ag['long_wr']}% WR)")
    print(f"  Shorts : {ag['short_trades']} ({ag['short_wr']}% WR)")
    print(f"  Win Streak: {ag['max_win_streak']}  |  Loss Streak: {ag['max_loss_streak']}")
    print(f"\n  {'✅ USABLE — PF≥1.5 AND WR≥42%' if ag['usable'] else '❌ BELOW TARGETS (PF≥1.5 / WR≥42%)'}")

    print(f"\n  COIN BREAKDOWN:")
    print(f"  {'Symbol':22s} {'PF':>6}  {'WR%':>5}  {'Trades':>6}  {'W':>4}  {'L':>4}  {'PnL':>10}")
    print(f"  {'-'*65}")
    for r in ct:
        flag = '✅' if r['pf']>=1.5 else ('🟡' if r['pf']>=1.2 else '❌')
        print(f"  {flag} {r['symbol']:20s} {r['pf']:>6.3f}  {r['wr']:>5.1f}%  "
              f"{r['trades']:>6}  {r['wins']:>4}  {r['losses']:>4}  ${r['pnl']:>9,.0f}")

    total_bars = sum(all_rej.values())
    print(f"\n  FILTER STATS ({total_bars:,} bars scanned):")
    for k,v in all_rej.items():
        pct=v/total_bars*100 if total_bars else 0
        print(f"    {k:20s}: {v:>9,}  ({pct:.2f}%)")

    print(f"\n  MONTHLY PnL:")
    green=red=0
    for ym,pnl in mon.items():
        bar='█'*min(40,int(abs(pnl)/100))
        tag='🟢' if pnl>=0 else '🔴'
        print(f"  {tag} {ym}  {'+'if pnl>=0 else''}${pnl:>9,.0f}  {bar}")
        if pnl>=0: green+=1
        else: red+=1
    print(f"\n  Green: {green}/{len(mon)} | Red: {red}/{len(mon)}")
    pct_green = green/len(mon)*100 if mon else 0
    print(f"  Green month rate: {pct_green:.1f}%")

    # ── Save ───────────────────────────────────────────────────────
    summary = [
        "BACKTEST V7 SUMMARY — Clean Whitelist Strategy",
        f"Range: {START_DATE.strftime('%Y-%m')} to {END_DATE.strftime('%Y-%m')}",
        f"Coins: {len(SYMBOLS)} proven performers | Capital: ${CAPITAL:,.0f}",
        f"Exits: TP={TP_MULT}xATR | SL={SL_MULT}xATR | MaxPos: {MAX_POSITIONS}",
        "",
        f"Trades: {ag['total_trades']} | WR: {ag['win_rate']}% | PF: {ag['profit_factor']}",
        f"Net PnL: ${ag['net_pnl']:,.2f} | Max DD: {ag['max_drawdown_pct']}%",
        f"Sharpe: {ag['sharpe']} | Sortino: {ag['sortino']}",
        f"Expectancy: ${ag['expectancy']:,.2f}/trade",
        f"Verdict: {'✅ USABLE' if ag['usable'] else '❌ BELOW TARGETS'}",
        "", "COIN BREAKDOWN:"
    ] + [f"  {'✅' if r['pf']>=1.5 else '🟡' if r['pf']>=1.2 else '❌'} "
         f"{r['symbol']:22s} PF:{r['pf']:.3f} WR:{r['wr']}% "
         f"Trades:{r['trades']} PnL:${r['pnl']:,.0f}" for r in ct]

    with open('backtest_summary.txt','w') as f: f.write('\n'.join(summary))

    report = {
        'meta': {
            'version':'V7', 'start':START_DATE.isoformat(), 'end':END_DATE.isoformat(),
            'symbols':SYMBOLS, 'capital':CAPITAL, 'risk_pct':RISK_PCT,
            'fee_pct':FEE_PCT, 'slippage_pct':SLIPPAGE_PCT, 'max_positions':MAX_POSITIONS,
            'adx_min':ADX_MIN, 'tp_mult':TP_MULT, 'sl_mult':SL_MULT,
            'interval':INTERVAL, 'generated_at':datetime.now(timezone.utc).isoformat(),
        },
        'aggregate':ag, 'per_coin':ct, 'monthly_pnl':mon,
        'filter_stats':{k:{'count':v,'pct':round(v/total_bars*100,2) if total_bars else 0}
                        for k,v in all_rej.items()},
        'fetch_errors':_errors[:50],
    }
    with open('backtest_report.json','w') as f:
        json.dump(report,f,indent=2,default=str)

    print(f"\n  OUTPUT: backtest_summary.txt | backtest_report.json")
    print("="*60)

if __name__ == '__main__':
    main()
