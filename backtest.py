"""
Backtest Engine V3 — 3 Strategies
S1: 4H Trend Confluence Scalper (all 13 coins)
S2: Bollinger Squeeze Breakout (majors only)
S3: VWAP Deviation Reversion (BTC, ETH, SOL)
Data: data-api.binance.vision (no geo-block)
Output: backtest_report.json + backtest_summary.txt
"""

import json, time, math, statistics
from datetime import datetime, timezone
from collections import defaultdict
import urllib.request

# ── Coins ─────────────────────────────────────────────────────────────────────
TIER1 = ['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT']
TIER2 = ['ADAUSDT','DOGEUSDT','LINKUSDT','LTCUSDT','POLUSDT']  # MATIC→POL
MEMES = ['1000BONKUSDT','1000PEPEUSDT','1000SHIBUSDT']
ALL_COINS = TIER1 + TIER2 + MEMES

S1_COINS = ALL_COINS
S2_COINS = TIER1
S3_COINS = ['BTCUSDT','ETHUSDT','SOLUSDT']

def is_meme(sym):
    return sym in MEMES

# ── Config ────────────────────────────────────────────────────────────────────
FEE_RATE    = 0.0005   # 0.05% per side
SLIPPAGE    = 0.0002
INITIAL_CAP = 10000.0

# S1 risk
def s1_risk(sym): return 0.0025 if is_meme(sym) else 0.005
def s1_sl(sym):   return 2.0   if is_meme(sym) else 1.5
def s1_tp(sym):   return 4.0   if is_meme(sym) else 3.5
def s1_vol(sym):  return 3.0   if is_meme(sym) else 1.5

# ── Binance fetch ─────────────────────────────────────────────────────────────
BASE = "https://data-api.binance.vision"

def fetch(symbol, interval, start_ms, end_ms):
    out, cur, retries = [], start_ms, 0
    while cur < end_ms:
        url = (f"{BASE}/api/v3/klines?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={end_ms}&limit=1000")
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                data = json.loads(r.read())
            if not data: break
            out.extend(data)
            last = data[-1][0]
            if last == cur: break
            cur = last + 1
            retries = 0
            time.sleep(0.12)
        except Exception as e:
            retries += 1
            if retries > 5:
                print(f"[!] {symbol} {interval}: {e}")
                break
            time.sleep(2 * retries)
    return out

def parse(raw):
    o=[float(k[1]) for k in raw]; h=[float(k[2]) for k in raw]
    l=[float(k[3]) for k in raw]; c=[float(k[4]) for k in raw]
    v=[float(k[5]) for k in raw]; t=[int(k[0])   for k in raw]
    return o,h,l,c,v,t

# ── Indicators ────────────────────────────────────────────────────────────────
def ema(vals, p):
    if len(vals) < p: return [None]*len(vals)
    k = 2/(p+1)
    r = [None]*(p-1)
    r.append(sum(vals[:p])/p)
    for v in vals[p:]: r.append(v*k + r[-1]*(1-k))
    return r

def sma(vals, p):
    r = [None]*(p-1)
    for i in range(p-1, len(vals)):
        r.append(sum(vals[i-p+1:i+1])/p)
    return r

def atr(h,l,c,p=14):
    tr=[None]
    for i in range(1,len(c)):
        tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    if len(tr)<p+1: return [None]*len(c)
    r=[None]*p
    r.append(sum(tr[1:p+1])/p)
    for i in range(p+1,len(tr)):
        r.append((r[-1]*(p-1)+tr[i])/p)
    return r

def rsi(c, p=14):
    if len(c)<p+1: return [None]*len(c)
    d=[c[i]-c[i-1] for i in range(1,len(c))]
    g=[max(x,0) for x in d]; ls=[abs(min(x,0)) for x in d]
    ag=sum(g[:p])/p; al=sum(ls[:p])/p
    r=[None]*p
    r.append(100 if al==0 else 100-100/(1+ag/al))
    for i in range(p,len(d)):
        ag=(ag*(p-1)+g[i])/p; al=(al*(p-1)+ls[i])/p
        r.append(100 if al==0 else 100-100/(1+ag/al))
    return r

def adx_full(h,l,c,p=14):
    n=len(c)
    if n<p*3: return [None]*n,[None]*n,[None]*n
    pdm,mdm,tr=[],[],[]
    for i in range(1,n):
        up=h[i]-h[i-1]; dn=l[i-1]-l[i]
        pdm.append(up if up>dn and up>0 else 0)
        mdm.append(dn if dn>up and dn>0 else 0)
        tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    def ws(lst,p):
        if len(lst)<p: return []
        r=[sum(lst[:p])]
        for x in lst[p:]: r.append(r[-1]-r[-1]/p+x)
        return r
    st=ws(tr,p); sp=ws(pdm,p); sm=ws(mdm,p)
    if not st: return [None]*n,[None]*n,[None]*n
    pdi=[100*a/b if b else 0 for a,b in zip(sp,st)]
    mdi=[100*a/b if b else 0 for a,b in zip(sm,st)]
    dx=[100*abs(a-b)/(a+b) if (a+b) else 0 for a,b in zip(pdi,mdi)]
    if len(dx)<p: return [None]*n,[None]*n,[None]*n
    adxv=[sum(dx[:p])/p]
    for x in dx[p:]: adxv.append((adxv[-1]*(p-1)+x)/p)
    pad=n-len(adxv)
    return ([None]*pad+adxv,
            [None]*(n-len(pdi))+pdi,
            [None]*(n-len(mdi))+mdi)

def vsma(v,p=20):
    r=[None]*(p-1)
    for i in range(p-1,len(v)): r.append(sum(v[i-p+1:i+1])/p)
    return r

def bbands(c, p=20, mult=2.0):
    mid=sma(c,p)
    upper=[None]*(p-1); lower=[None]*(p-1)
    for i in range(p-1,len(c)):
        sl=c[i-p+1:i+1]
        m=sum(sl)/p
        sd=math.sqrt(sum((x-m)**2 for x in sl)/p)
        upper.append(m+mult*sd)
        lower.append(m-mult*sd)
    return upper, mid, lower

def atr_percentile(atr_vals, idx, lookback=20, pct=0.30):
    """Return 30th percentile of last `lookback` ATR values before idx."""
    window=[x for x in atr_vals[max(0,idx-lookback):idx] if x is not None]
    if not window: return None
    window.sort()
    pos=int(len(window)*pct)
    return window[min(pos,len(window)-1)]

# ── 4H trend builder ──────────────────────────────────────────────────────────
def build_4h(h4_raw):
    if len(h4_raw)<210: return {}, []
    o,h,l,c,v,t=parse(h4_raw)
    e50=ema(c,50); e200=ema(c,200)
    adxv,_,_=adx_full(h,l,c,14)
    tmap={}
    for i in range(len(c)):
        if None in (e50[i],e200[i],adxv[i]):
            tmap[t[i]]=None; continue
        if e50[i]>e200[i] and adxv[i]>20 and c[i]>e50[i]:
            tmap[t[i]]='BULL'
        elif e50[i]<e200[i] and adxv[i]>20 and c[i]<e50[i]:
            tmap[t[i]]='BEAR'
        else:
            tmap[t[i]]=None
    return tmap, sorted(tmap.keys())

def get_trend(ts, sorted_ts, tmap):
    lo,hi,res=0,len(sorted_ts)-1,None
    while lo<=hi:
        mid=(lo+hi)//2
        if sorted_ts[mid]<=ts: res=sorted_ts[mid]; lo=mid+1
        else: hi=mid-1
    return tmap.get(res) if res is not None else None

def get_4h_ema50(h4_raw):
    """Returns dict of ts->ema50 value for S2 4H filter."""
    if len(h4_raw)<55: return {}, []
    o,h,l,c,v,t=parse(h4_raw)
    e50=ema(c,50)
    m={}
    for i in range(len(c)):
        m[t[i]]=e50[i]
    return m, sorted(m.keys())

def get_4h_val(ts, sorted_ts, vmap):
    lo,hi,res=0,len(sorted_ts)-1,None
    while lo<=hi:
        mid=(lo+hi)//2
        if sorted_ts[mid]<=ts: res=sorted_ts[mid]; lo=mid+1
        else: hi=mid-1
    return vmap.get(res) if res is not None else None

# ── VWAP (daily reset at 00:00 UTC) ──────────────────────────────────────────
def calc_vwap(times, highs, lows, closes, volumes):
    vwap=[None]*len(closes)
    cum_pv=0.0; cum_v=0.0; cur_day=None
    for i in range(len(closes)):
        dt=datetime.fromtimestamp(times[i]/1000, tz=timezone.utc)
        day=dt.date()
        if day!=cur_day:
            cum_pv=0.0; cum_v=0.0; cur_day=day
        tp=(highs[i]+lows[i]+closes[i])/3
        cum_pv+=tp*volumes[i]; cum_v+=volumes[i]
        vwap[i]=cum_pv/cum_v if cum_v>0 else None
    return vwap

# ── Generic trade simulator ───────────────────────────────────────────────────
def simulate_trades(signals):
    """
    signals: list of dicts with keys:
      direction, entry, sl, tp, entry_bar, entry_time, exit_time_if_timeout
    Returns list of trade result dicts.
    """
    return signals  # signals already resolved in each strategy function

# ── Metrics ───────────────────────────────────────────────────────────────────
def metrics(trades, sym):
    if not trades: return None
    wins=[t for t in trades if t['win']]
    loss=[t for t in trades if not t['win']]
    n=len(trades); nw=len(wins)
    gp=sum(t['pnl'] for t in wins)   if wins else 0
    gl=abs(sum(t['pnl'] for t in loss)) if loss else 0
    pf=gp/gl if gl>0 else float('inf')
    wr=nw/n
    net=sum(t['pnl'] for t in trades)*100
    aw=statistics.mean(t['pnl'] for t in wins)*100   if wins else 0
    al=statistics.mean(t['pnl'] for t in loss)*100   if loss else 0
    exp=wr*aw+(1-wr)*al
    dur=statistics.mean(t['dur'] for t in trades)
    # drawdown
    eq=INITIAL_CAP; pk=eq; mdd=0
    for t in trades:
        r=t.get('risk',0.005)
        eq+=t['pnl']/r*eq*r
        if eq>pk: pk=eq
        dd=(pk-eq)/pk
        if dd>mdd: mdd=dd
    pnls=[t['pnl'] for t in trades]
    if len(pnls)>1:
        mr=statistics.mean(pnls); sr=statistics.stdev(pnls)
        sharpe=mr/sr*math.sqrt(252*48) if sr>0 else 0
        neg=[p for p in pnls if p<0]
        ds=statistics.stdev(neg) if len(neg)>1 else sr
        sortino=mr/ds*math.sqrt(252*48) if ds>0 else 0
    else: sharpe=sortino=0
    longs=[t for t in trades if t['dir']=='LONG']
    shorts=[t for t in trades if t['dir']=='SHORT']
    lwr=sum(1 for t in longs if t['win'])/len(longs)*100 if longs else 0
    swr=sum(1 for t in shorts if t['win'])/len(shorts)*100 if shorts else 0
    monthly=defaultdict(float)
    for t in trades:
        dt=datetime.fromtimestamp(t['exit_t']/1000,tz=timezone.utc)
        monthly[dt.strftime('%Y-%m')]+=t['pnl']*100
    # consec
    maxcw=maxcl=cw=cl=0
    for t in trades:
        if t['win']: cw+=1; cl=0; maxcw=max(maxcw,cw)
        else: cl+=1; cw=0; maxcl=max(maxcl,cl)
    return dict(
        symbol=sym, n=n, wr=round(wr*100,2), pf=round(pf,4),
        net=round(net,4), mdd=round(mdd*100,2),
        sharpe=round(sharpe,4), sortino=round(sortino,4),
        aw=round(aw,4), al=round(al,4), exp=round(exp,4),
        dur=round(dur,1), nlongs=len(longs), nshorts=len(shorts),
        lwr=round(lwr,2), swr=round(swr,2),
        monthly=dict(sorted(monthly.items())),
        maxcw=maxcw, maxcl=maxcl,
        gp=round(gp*100,4), gl=round(gl*100,4)
    )

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1 — 4H Trend Confluence Scalper
# ══════════════════════════════════════════════════════════════════════════════
def run_s1(sym, h30_raw, h4_raw):
    if len(h30_raw)<300 or len(h4_raw)<210:
        return None, {}
    o,h,l,c,v,t=parse(h30_raw)
    tmap,sorted_4h=build_4h(h4_raw)

    e21=ema(c,21); atr14=atr(h,l,c,14)
    rsi14=rsi(c,14); vs20=vsma(v,20)
    sl_m=s1_sl(sym); tp_m=s1_tp(sym); vol_m=s1_vol(sym)
    risk=s1_risk(sym)
    total_fee=FEE_RATE+SLIPPAGE

    flt=defaultdict(int)
    trades=[]; in_pos=False
    ep=sl=tp=d=eb=et=ea=None
    cooldown=0

    for i in range(1,len(c)):
        flt['total']+=1
        # exit
        if in_pos:
            hit_sl=hit_tp=False
            if d=='LONG':
                if l[i]<=sl: hit_sl=True
                if h[i]>=tp: hit_tp=True
            else:
                if h[i]>=sl: hit_sl=True
                if l[i]<=tp: hit_tp=True
            if hit_sl or hit_tp:
                xp=tp if hit_tp else sl
                raw=(xp-ep)/ep if d=='LONG' else (ep-xp)/ep
                net=raw-2*total_fee
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],
                    pnl=net,win=net>0,hit_tp=hit_tp,
                    dur=i-eb,risk=risk))
                in_pos=False; cooldown=3
            else:
                flt['in_pos']+=1
                continue

        if cooldown>0: cooldown-=1; flt['cooldown']+=1; continue

        # indicators
        e=e21[i]; a=atr14[i]; r_cur=rsi14[i]; r_prv=rsi14[i-1]
        vs=vs20[i]
        if None in (e,a,r_cur,r_prv,vs): continue

        # ATR percentile
        atp=atr_percentile(atr14,i,20,0.30)
        if atp is None: flt['no_atp']+=1; continue

        # 4H trend
        trend=get_trend(t[i],sorted_4h,tmap)
        if trend is None: flt['no_4h']+=1; continue

        # volatility filter
        if a<=atp: flt['atr_weak']+=1; continue

        # volume
        if v[i]<=vol_m*vs: flt['vol']+=1; continue

        sig=None
        if trend=='BULL':
            if l[i]>e: flt['no_pullback']+=1; continue      # no touch EMA21
            if r_prv>=45: flt['rsi_prv']+=1; continue        # prev RSI not < 45
            if r_cur<45:  flt['rsi_cur']+=1; continue        # cur RSI not >= 45
            sig='LONG'
        elif trend=='BEAR':
            if h[i]<e: flt['no_pullback']+=1; continue
            if r_prv<=55: flt['rsi_prv']+=1; continue
            if r_cur>55:  flt['rsi_cur']+=1; continue
            sig='SHORT'

        if sig:
            flt['signals']+=1
            ep=c[i]*(1+SLIPPAGE if sig=='LONG' else 1-SLIPPAGE)
            ea=a
            if sig=='LONG': sl=ep-sl_m*a; tp=ep+tp_m*a
            else:           sl=ep+sl_m*a; tp=ep-tp_m*a
            d=sig; eb=i; et=t[i]; in_pos=True

    return trades, dict(flt)

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2 — Bollinger Squeeze Breakout
# ══════════════════════════════════════════════════════════════════════════════
def run_s2(sym, h30_raw, h4_raw):
    if len(h30_raw)<300 or len(h4_raw)<60: return None, {}
    o,h,l,c,v,t=parse(h30_raw)
    e4map,sorted_e4=get_4h_ema50(h4_raw)

    upper,mid,lower=bbands(c,20,2.0)
    atr14=atr(h,l,c,14); rsi14=rsi(c,14); vs20=vsma(v,20)
    total_fee=FEE_RATE+SLIPPAGE; risk=0.005

    flt=defaultdict(int)
    trades=[]; in_pos=False
    ep=sl=tp=d=eb=ea=None
    cooldown=0; trades_today=defaultdict(int)

    for i in range(20,len(c)):
        flt['total']+=1
        # exit
        if in_pos:
            hit_sl=hit_tp=False
            # gap check
            if d=='LONG' and o[i]<ep-0.5*ea:
                xp=o[i]; raw=(xp-ep)/ep; net=raw-2*total_fee
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=False,dur=i-eb,risk=risk))
                in_pos=False; cooldown=6; continue
            if d=='SHORT' and o[i]>ep+0.5*ea:
                xp=o[i]; raw=(ep-xp)/ep; net=raw-2*total_fee
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=False,dur=i-eb,risk=risk))
                in_pos=False; cooldown=6; continue
            if d=='LONG':
                if l[i]<=sl: hit_sl=True
                if h[i]>=tp: hit_tp=True
            else:
                if h[i]>=sl: hit_sl=True
                if l[i]<=tp: hit_tp=True
            if hit_sl or hit_tp:
                xp=tp if hit_tp else sl
                raw=(xp-ep)/ep if d=='LONG' else (ep-xp)/ep
                net=raw-2*total_fee
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=hit_tp,dur=i-eb,risk=risk))
                in_pos=False; cooldown=6
            else: flt['in_pos']+=1; continue

        if cooldown>0: cooldown-=1; flt['cooldown']+=1; continue

        u=upper[i]; m=mid[i]; lo_b=lower[i]
        a=atr14[i]; r=rsi14[i]; vs=vs20[i]
        if None in (u,m,lo_b,a,r,vs): continue

        # daily trade limit
        dt=datetime.fromtimestamp(t[i]/1000,tz=timezone.utc).strftime('%Y-%m-%d')
        if trades_today[dt]>=2: flt['daily_limit']+=1; continue

        # squeeze detection: last 10 bars all squeezed
        squeeze=True
        for j in range(i-9,i+1):
            if j<0 or upper[j] is None or mid[j] is None or lower[j] is None:
                squeeze=False; break
            bw=(upper[j]-lower[j])/mid[j] if mid[j] else 1
            if bw>=0.05: squeeze=False; break
        if not squeeze: flt['no_squeeze']+=1; continue

        # 4H EMA50
        e50_4h=get_4h_val(t[i],sorted_e4,e4map)
        if e50_4h is None: flt['no_4h']+=1; continue

        # candle body
        body=abs(c[i]-o[i]); rng=h[i]-l[i]
        if rng==0: flt['no_body']+=1; continue
        if body/rng<0.6: flt['body_weak']+=1; continue

        sig=None
        if c[i]>u and c[i]>e50_4h and r>55 and v[i]>2.0*vs:
            sig='LONG'
        elif c[i]<lo_b and c[i]<e50_4h and r<45 and v[i]>2.0*vs:
            sig='SHORT'
        else: flt['no_sig']+=1; continue

        if sig:
            flt['signals']+=1
            ep=c[i]*(1+SLIPPAGE if sig=='LONG' else 1-SLIPPAGE)
            ea=a
            # SL = closer of middle band or 1.5xATR
            if sig=='LONG':
                sl_atr=ep-1.5*a; sl=max(sl_atr,m)  # closer = higher
                tp=ep+2.5*a
            else:
                sl_atr=ep+1.5*a; sl=min(sl_atr,m)
                tp=ep-2.5*a
            d=sig; eb=i; in_pos=True
            trades_today[dt]+=1

    return trades, dict(flt)

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3 — VWAP Deviation Reversion
# ══════════════════════════════════════════════════════════════════════════════
VWAP_DEV = {'BTCUSDT':0.010,'ETHUSDT':0.010,'SOLUSDT':0.015}

def run_s3(sym, h30_raw):
    if len(h30_raw)<300: return None, {}
    o,h,l,c,v,t=parse(h30_raw)
    vwap=calc_vwap(t,h,l,c,v)
    rsi7=rsi(c,7); vs20=vsma(v,20)
    total_fee=FEE_RATE+SLIPPAGE; risk=0.005
    dev_thresh=VWAP_DEV.get(sym,0.015)
    timeout=8  # bars

    flt=defaultdict(int)
    trades=[]; in_pos=False
    ep=sl=tp=d=eb=None
    cooldown=0

    for i in range(1,len(c)):
        flt['total']+=1
        if in_pos:
            hit_sl=hit_tp=timeout_hit=False
            if d=='LONG':
                if l[i]<=sl: hit_sl=True
                if h[i]>=tp: hit_tp=True
            else:
                if h[i]>=sl: hit_sl=True
                if l[i]<=tp: hit_tp=True
            if i-eb>=timeout: timeout_hit=True
            if hit_sl or hit_tp or timeout_hit:
                if hit_tp: xp=tp
                elif hit_sl: xp=sl
                else: xp=c[i]  # timeout exit at close
                raw=(xp-ep)/ep if d=='LONG' else (ep-xp)/ep
                net=raw-2*total_fee
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=hit_tp,dur=i-eb,risk=risk))
                in_pos=False; cooldown=4
            else: flt['in_pos']+=1; continue

        if cooldown>0: cooldown-=1; flt['cooldown']+=1; continue

        vw=vwap[i]; r=rsi7[i]; vs=vs20[i]
        if None in (vw,r,vs): continue

        dev=abs(c[i]-vw)/vw

        sig=None
        if c[i]<vw and dev>dev_thresh and r<30:
            # wick rejection: lower wick > 40% range
            rng=h[i]-l[i]
            if rng==0: flt['no_wick']+=1; continue
            low_wick=(min(o[i],c[i])-l[i])
            if low_wick/rng<0.40: flt['no_wick']+=1; continue
            if v[i]<=vs20[i]: flt['vol']+=1; continue
            # momentum slowing
            if i>0 and l[i]<l[i-1]: flt['momentum']+=1; continue
            sig='LONG'
        elif c[i]>vw and dev>dev_thresh and r>70:
            rng=h[i]-l[i]
            if rng==0: flt['no_wick']+=1; continue
            up_wick=h[i]-max(o[i],c[i])
            if up_wick/rng<0.40: flt['no_wick']+=1; continue
            if v[i]<=vs20[i]: flt['vol']+=1; continue
            if i>0 and h[i]>h[i-1]: flt['momentum']+=1; continue
            sig='SHORT'
        else: flt['no_sig']+=1; continue

        if sig:
            flt['signals']+=1
            ep=c[i]*(1+SLIPPAGE if sig=='LONG' else 1-SLIPPAGE)
            if sig=='LONG': sl=l[i]; tp=vw
            else:           sl=h[i]; tp=vw
            d=sig; eb=i; in_pos=True

    return trades, dict(flt)

# ── Output helpers ────────────────────────────────────────────────────────────
def agg_metrics(all_trades):
    if not all_trades: return None
    return metrics(all_trades, 'ALL')

def print_agg(label, m, target_pf=1.5, target_wr=42):
    if not m: print(f"  {label}: no trades"); return
    pf_ok=m['pf']>=target_pf; wr_ok=m['wr']>=target_wr
    print(f"\n  ── {label} AGGREGATE ──")
    print(f"  Trades: {m['n']} | PF: {m['pf']} {'✅' if pf_ok else '❌'} | WR: {m['wr']}% {'✅' if wr_ok else '❌'}")
    print(f"  Net: {m['net']}% | MaxDD: {m['mdd']}% | Sharpe: {m['sharpe']}")
    print(f"  AvgWin: {m['aw']}% | AvgLoss: {m['al']}% | Expectancy: {m['exp']}%")
    print(f"  Longs WR: {m['lwr']}% | Shorts WR: {m['swr']}%")
    print(f"  MaxConsecWins: {m['maxcw']} | MaxConsecLoss: {m['maxcl']}")

def write_summary(results, path='backtest_summary.txt'):
    L=[]
    L.append("="*72)
    L.append("BACKTEST REPORT V3 — 3 Strategies | 13 Coins | 2 Years")
    L.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    L.append(f"Fee: {FEE_RATE*100}% + {SLIPPAGE*100}% slippage per side")
    L.append("="*72)

    for skey, sdata in results.items():
        L.append(f"\n{'─'*72}")
        L.append(f"  {sdata['name']}")
        L.append(f"{'─'*72}")
        agg=sdata.get('aggregate')
        if agg:
            pf_ok=agg['pf']>=1.5; wr_ok=agg['wr']>=42
            L.append(f"  AGGREGATE")
            L.append(f"  Total Trades      : {agg['n']}")
            L.append(f"  Profit Factor     : {agg['pf']}  {'✅ TARGET MET' if pf_ok else '❌ BELOW 1.5'}")
            L.append(f"  Win Rate          : {agg['wr']}%  {'✅' if wr_ok else '❌ BELOW 42%'}")
            L.append(f"  Net PnL           : {agg['net']}%")
            L.append(f"  Max Drawdown      : {agg['mdd']}%")
            L.append(f"  Sharpe            : {agg['sharpe']}")
            L.append(f"  Sortino           : {agg['sortino']}")
            L.append(f"  Avg Winner        : {agg['aw']}%")
            L.append(f"  Avg Loser         : {agg['al']}%")
            L.append(f"  Expectancy/Trade  : {agg['exp']}%")
            L.append(f"  Avg Duration(bars): {agg['dur']}")
            L.append(f"  Long WR           : {agg['lwr']}%  ({agg['nlongs']} trades)")
            L.append(f"  Short WR          : {agg['swr']}%  ({agg['nshorts']} trades)")
            L.append(f"  Max Consec Wins   : {agg['maxcw']}")
            L.append(f"  Max Consec Losses : {agg['maxcl']}")
            # validation
            coins_pf=[d for d in sdata['by_symbol'].values() if isinstance(d,dict) and 'pf' in d]
            pass_pf=sum(1 for d in coins_pf if d['pf']>=1.5)
            pass_wr=sum(1 for d in coins_pf if d['wr']>=42)
            total_c=len(coins_pf)
            L.append(f"\n  VALIDATION")
            L.append(f"  Coins PF>=1.5 : {pass_pf}/{total_c}  (need 9+)")
            L.append(f"  Coins WR>=42% : {pass_wr}/{total_c}  (need 9+)")
            btc=sdata['by_symbol'].get('BTCUSDT',{}); eth=sdata['by_symbol'].get('ETHUSDT',{})
            btc_ok=isinstance(btc,dict) and btc.get('pf',0)>=1.5
            eth_ok=isinstance(eth,dict) and eth.get('pf',0)>=1.5
            L.append(f"  BTC PF>=1.5   : {'✅' if btc_ok else '❌'}  ETH PF>=1.5: {'✅' if eth_ok else '❌'}")
            valid=pass_pf>=9 and pass_wr>=9 and btc_ok and eth_ok
            L.append(f"  OVERALL VALID : {'✅ YES — consider live testing' if valid else '❌ NO — do not trade live'}")

        # filter stats
        fs=sdata.get('filter_stats',{})
        if fs:
            L.append(f"\n  FILTER STATS")
            for k,v2 in fs.items():
                L.append(f"    {k:<20}: {v2}")

        # per coin
        L.append(f"\n  {'Symbol':<16}{'Trades':>7}{'PF':>8}{'WR%':>7}{'Net%':>9}{'DD%':>7}{'Longs':>7}{'Shorts':>8}")
        L.append(f"  {'─'*66}")
        by=sdata.get('by_symbol',{})
        rows=[(s,d) for s,d in by.items() if isinstance(d,dict) and 'pf' in d]
        rows.sort(key=lambda x: x[1]['pf'],reverse=True)
        for s,d in rows:
            L.append(f"  {s:<16}{d['n']:>7}{d['pf']:>8.3f}{d['wr']:>7.1f}{d['net']:>9.2f}{d['mdd']:>7.2f}{d['nlongs']:>7}{d['nshorts']:>8}")
        for s,d in by.items():
            if isinstance(d,dict) and 'error' in d:
                L.append(f"  {s:<16}  SKIP — {d['error']}")

    L.append(f"\n{'='*72}\nEND OF REPORT\n{'='*72}")
    with open(path,'w') as f: f.write('\n'.join(L))
    print(f"✅ {path} written")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now_ms   = int(time.time()*1000)
    start_ms = now_ms - 2*365*24*3600*1000

    results={}

    # ── Fetch all needed data first ──
    print("Fetching data...")
    data_30m={}; data_4h={}
    all_needed=list(set(S1_COINS+S2_COINS+S3_COINS))
    for sym in all_needed:
        print(f"  {sym} 30m...", end=' ', flush=True)
        data_30m[sym]=fetch(sym,'30m',start_ms,now_ms)
        print(f"{len(data_30m[sym])} candles | 4h...", end=' ', flush=True)
        data_4h[sym]=fetch(sym,'4h',start_ms,now_ms)
        print(f"{len(data_4h[sym])} candles")

    # ══ STRATEGY 1 ══
    print(f"\n{'='*60}\n  STRATEGY 1 — 4H Trend Confluence Scalper\n{'='*60}")
    s1_by={}; s1_all=[]; s1_flt=defaultdict(int)
    for sym in S1_COINS:
        print(f"  {sym}...", end=' ', flush=True)
        trades,flt=run_s1(sym,data_30m.get(sym,[]),data_4h.get(sym,[]))
        if trades is None:
            print("SKIP"); s1_by[sym]={'error':'insufficient_data'}; continue
        m=metrics(trades,sym)
        if m:
            print(f"Trades:{m['n']} PF:{m['pf']} WR:{m['wr']}%")
            m['filter_stats']=flt; s1_by[sym]=m; s1_all.extend(trades)
        else:
            print("no trades"); s1_by[sym]={'error':'no_trades','filter_stats':flt}
        for k,v2 in flt.items(): s1_flt[k]+=v2
    agg1=agg_metrics(s1_all)
    print_agg("S1",agg1)
    results['S1']={'name':'Strategy 1 — 4H Trend Confluence Scalper',
                   'aggregate':agg1,'by_symbol':s1_by,
                   'filter_stats':dict(s1_flt)}

    # ══ STRATEGY 2 ══
    print(f"\n{'='*60}\n  STRATEGY 2 — Bollinger Squeeze Breakout\n{'='*60}")
    s2_by={}; s2_all=[]; s2_flt=defaultdict(int)
    for sym in S2_COINS:
        print(f"  {sym}...", end=' ', flush=True)
        trades,flt=run_s2(sym,data_30m.get(sym,[]),data_4h.get(sym,[]))
        if trades is None:
            print("SKIP"); s2_by[sym]={'error':'insufficient_data'}; continue
        m=metrics(trades,sym)
        if m:
            print(f"Trades:{m['n']} PF:{m['pf']} WR:{m['wr']}%")
            m['filter_stats']=flt; s2_by[sym]=m; s2_all.extend(trades)
        else:
            print("no trades"); s2_by[sym]={'error':'no_trades','filter_stats':flt}
        for k,v2 in flt.items(): s2_flt[k]+=v2
    agg2=agg_metrics(s2_all)
    print_agg("S2",agg2)
    results['S2']={'name':'Strategy 2 — Bollinger Squeeze Breakout',
                   'aggregate':agg2,'by_symbol':s2_by,
                   'filter_stats':dict(s2_flt)}

    # ══ STRATEGY 3 ══
    print(f"\n{'='*60}\n  STRATEGY 3 — VWAP Deviation Reversion\n{'='*60}")
    s3_by={}; s3_all=[]; s3_flt=defaultdict(int)
    for sym in S3_COINS:
        print(f"  {sym}...", end=' ', flush=True)
        trades,flt=run_s3(sym,data_30m.get(sym,[]))
        if trades is None:
            print("SKIP"); s3_by[sym]={'error':'insufficient_data'}; continue
        m=metrics(trades,sym)
        if m:
            print(f"Trades:{m['n']} PF:{m['pf']} WR:{m['wr']}%")
            m['filter_stats']=flt; s3_by[sym]=m; s3_all.extend(trades)
        else:
            print("no trades"); s3_by[sym]={'error':'no_trades','filter_stats':flt}
        for k,v2 in flt.items(): s3_flt[k]+=v2
    agg3=agg_metrics(s3_all)
    print_agg("S3",agg3)
    results['S3']={'name':'Strategy 3 — VWAP Deviation Reversion',
                   'aggregate':agg3,'by_symbol':s3_by,
                   'filter_stats':dict(s3_flt)}

    # ── Write outputs ──
    with open('backtest_report.json','w') as f: json.dump(results,f,indent=2)
    print("\n✅ backtest_report.json written")
    write_summary(results)
    print("\n🏁 Done. Share backtest_report.json + backtest_summary.txt")

if __name__=='__main__':
    print("🔍 Backtest V3 | 3 Strategies | 13 Coins | 2 Years")
    main()

