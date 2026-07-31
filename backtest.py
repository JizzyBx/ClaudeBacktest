"""
Backtest v8.6 — Full Binance USDT-M universe, 15m candles
Variants:
  G  — ADX>=22, TP 3%, SL 15%
  H  — ADX>=22, TP 4%, SL 15%
  L  — ADX>=14, TP 3%, SL 15%  (low ADX, more signals)
"""

import os, io, csv, json, math, time, zipfile, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── CONFIG ───────────────────────────────────────────────────────────────────
INTERVAL      = "15m"
CAPITAL       = 10_000.0
RISK_PCT      = 0.0075
FEE           = 0.0005
SLIP          = 0.0002
MAX_POSITIONS = 5
RSI_LONG_MIN  = 45
RSI_SHORT_MAX = 55
COOLDOWN_BARS = 4
WARMUP_BARS   = 100
MAX_WORKERS   = 60

# July 2024 → June 2026
MONTHS = []
for y in range(2024, 2027):
    for m in range(1, 13):
        if y == 2024 and m < 7: continue
        if y == 2026 and m > 6: break
        MONTHS.append(f"{y}-{m:02d}")

BASE_URL = "https://data.binance.vision"
S3_URL   = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

VARIANTS = {
    "G": {"tp": 0.03, "sl": 0.15, "adx_min": 22},
    "H": {"tp": 0.04, "sl": 0.15, "adx_min": 22},
    "L": {"tp": 0.03, "sl": 0.15, "adx_min": 14},
}

BAD_COINS = {"1000FLOKIUSDT","COCOUSDT","MUBARAKUSDT","TSTUSDT","OMUSDT","ACTUSDT"}

# ── SYMBOL DISCOVERY ─────────────────────────────────────────────────────────
def discover_symbols():
    prefix = "data/futures/um/monthly/klines/"
    url    = f"{S3_URL}?prefix={prefix}&delimiter=/"
    symbols, marker = [], ""
    while True:
        req = url + (f"&marker={marker}" if marker else "")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                xml_data = r.read()
        except Exception as e:
            print(f"[WARN] S3 listing error: {e}", flush=True)
            break
        root = ET.fromstring(xml_data)
        ns   = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        for cp in root.findall("s3:CommonPrefixes", ns):
            sym = cp.find("s3:Prefix", ns).text.rstrip("/").split("/")[-1]
            if sym.endswith("USDT") and "_" not in sym:
                symbols.append(sym)
        trunc = root.find("s3:IsTruncated", ns)
        if trunc is not None and trunc.text.lower() == "true":
            nxt = root.find("s3:NextMarker", ns)
            if nxt is not None:
                marker = nxt.text
            else:
                cps = root.findall("s3:CommonPrefixes", ns)
                marker = cps[-1].find("s3:Prefix", ns).text if cps else ""
                if not marker: break
        else:
            break
    symbols = [s for s in symbols if s not in BAD_COINS]
    return sorted({("POLUSDT" if s == "MATICUSDT" else s) for s in symbols})

# ── DATA FETCH ────────────────────────────────────────────────────────────────
def fetch_month(symbol, month, interval):
    candidates = ["POLUSDT","MATICUSDT"] if symbol == "POLUSDT" else [symbol]
    for sym in candidates:
        url = f"{BASE_URL}/data/futures/um/monthly/klines/{sym}/{interval}/{sym}-{interval}-{month}.zip"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = r.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                with zf.open(zf.namelist()[0]) as f:
                    rows = list(csv.reader(io.TextIOWrapper(f)))
            if rows and not rows[0][0].lstrip('-').isdigit():
                rows = rows[1:]
            return [[float(c) for c in r[:6]] for r in rows if len(r) >= 6]
        except urllib.error.HTTPError as e:
            if e.code == 404: continue
            raise
        except Exception:
            continue
    return []

def fetch_symbol(symbol):
    bars = []
    for m in MONTHS:
        bars.extend(fetch_month(symbol, m, INTERVAL))
    return bars

# ── INDICATORS ────────────────────────────────────────────────────────────────
def ema(vals, p):
    k, res = 2.0/(p+1), [None]*len(vals)
    for i,v in enumerate(vals):
        res[i] = v if i==0 else v*k + res[i-1]*(1-k)
    return res

def compute_adx(highs, lows, closes, p=14):
    n = len(closes)
    adx = [None]*n
    if n < p*2+1: return adx
    tr_l, pdm_l, ndm_l = [], [], []
    for i in range(1, n):
        h,l,pc = highs[i],lows[i],closes[i-1]
        tr_l.append(max(h-l, abs(h-pc), abs(l-pc)))
        up, dn = highs[i]-highs[i-1], lows[i-1]-lows[i]
        pdm_l.append(up if up>dn and up>0 else 0)
        ndm_l.append(dn if dn>up and dn>0 else 0)

    def smooth(lst):
        s=[None]*len(lst); s[p-1]=sum(lst[:p])
        for i in range(p,len(lst)): s[i]=s[i-1]-s[i-1]/p+lst[i]
        return s

    atr=smooth(tr_l); pdi_s=smooth(pdm_l); ndi_s=smooth(ndm_l)
    dx_l=[]
    for i in range(p-1,len(tr_l)):
        a=atr[i]; pd=pdi_s[i]; nd=ndi_s[i]
        if not a: dx_l.append(0); continue
        pdi=100*pd/a; ndi=100*nd/a
        dx_l.append(100*abs(pdi-ndi)/(pdi+ndi) if (pdi+ndi)>0 else 0)
    adx_v=smooth(dx_l)
    for i,v in enumerate(adx_v):
        idx=i+p
        if idx<n and v is not None: adx[idx]=v
    return adx

def compute_rsi(closes, p=14):
    n=len(closes); rsi=[None]*n
    if n<p+1: return rsi
    g=[max(closes[i]-closes[i-1],0) for i in range(1,n)]
    l=[max(closes[i-1]-closes[i],0) for i in range(1,n)]
    ag=sum(g[:p])/p; al=sum(l[:p])/p
    for i in range(p,n):
        if i>p:
            ag=(ag*(p-1)+g[i-1])/p
            al=(al*(p-1)+l[i-1])/p
        rsi[i]=100-100/(1+ag/al) if al else 100
    return rsi

# ── SIGNALS ───────────────────────────────────────────────────────────────────
def compute_signals(bars, adx_min):
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    closes = [b[4] for b in bars]
    n      = len(bars)

    ema9  = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    adx   = compute_adx(highs, lows, closes, 14)
    rsi   = compute_rsi(closes, 14)

    sigs = [None]*n
    for i in range(WARMUP_BARS, n):
        if None in (ema9[i], ema21[i], ema50[i], adx[i], rsi[i]): continue
        if adx[i] < adx_min: continue
        if i < 10 or ema50[i-10] is None: continue
        slope = (ema50[i]-ema50[i-10])/ema50[i-10]
        if i < 1 or None in (ema9[i-1], ema21[i-1]): continue
        cross_l = ema9[i-1]<=ema21[i-1] and ema9[i]>ema21[i]
        cross_s = ema9[i-1]>=ema21[i-1] and ema9[i]<ema21[i]
        if slope>0.0005  and cross_l and rsi[i]>=RSI_LONG_MIN:  sigs[i]="long"
        elif slope<-0.0005 and cross_s and rsi[i]<=RSI_SHORT_MAX: sigs[i]="short"
    return sigs

# ── BACKTEST ──────────────────────────────────────────────────────────────────
def backtest_coin(bars, tp_pct, sl_pct, adx_min):
    sigs = compute_signals(bars, adx_min)
    n    = len(bars)
    trades, in_trade, cooldown = [], False, 0
    for i in range(n):
        b = bars[i]
        ts,o,h,l,c = int(b[0]),b[1],b[2],b[3],b[4]
        if in_trade:
            result = exit_p = None
            if t_dir=="long":
                if l<=t_sl: exit_p,result=t_sl,"sl"
                elif h>=t_tp: exit_p,result=t_tp,"tp"
            else:
                if h>=t_sl: exit_p,result=t_sl,"sl"
                elif l<=t_tp: exit_p,result=t_tp,"tp"
            if result:
                pct=(exit_p-t_entry)/t_entry if t_dir=="long" else (t_entry-exit_p)/t_entry
                pos=(CAPITAL*RISK_PCT)/sl_pct
                trades.append({"entry_ts":t_ts,"exit_ts":ts,"direction":t_dir,
                                "result":result,"pnl":pct*pos-(FEE+SLIP)*2*pos,
                                "bars_held":i-t_bar})
                in_trade=False; cooldown=COOLDOWN_BARS
            continue
        if cooldown>0: cooldown-=1; continue
        if sigs[i] in ("long","short"):
            t_entry=c; t_dir=sigs[i]; t_ts=ts; t_bar=i; in_trade=True
            t_tp=t_entry*(1+tp_pct) if t_dir=="long" else t_entry*(1-tp_pct)
            t_sl=t_entry*(1-sl_pct) if t_dir=="long" else t_entry*(1+sl_pct)
    return trades

# ── STATS ─────────────────────────────────────────────────────────────────────
def calc_stats(trades, days=730):
    if not trades:
        return dict(trades=0,wr=0,pf=0,net_pnl=0,max_dd=0,
                    long_trades=0,long_wr=0,short_trades=0,short_wr=0,
                    gross_profit=0,gross_loss=0,trades_per_day=0)
    wins  = [t for t in trades if t["pnl"]>0]
    losses= [t for t in trades if t["pnl"]<=0]
    longs = [t for t in trades if t["direction"]=="long"]
    shorts= [t for t in trades if t["direction"]=="short"]
    gp=sum(t["pnl"] for t in wins)
    gl=abs(sum(t["pnl"] for t in losses))
    net=sum(t["pnl"] for t in trades)
    eq=0; peak=0; mdd=0
    for t in sorted(trades,key=lambda x:x["exit_ts"]):
        eq+=t["pnl"]; peak=max(peak,eq)
        mdd=max(mdd,(peak-eq)/(CAPITAL+max(peak,0))*100 if peak>0 else 0)
    return dict(
        trades=len(trades), wr=len(wins)/len(trades)*100,
        pf=gp/gl if gl>0 else float("inf"),
        net_pnl=net, max_dd=mdd,
        long_trades=len(longs),
        long_wr=len([t for t in longs if t["pnl"]>0])/len(longs)*100 if longs else 0,
        short_trades=len(shorts),
        short_wr=len([t for t in shorts if t["pnl"]>0])/len(shorts)*100 if shorts else 0,
        gross_profit=gp, gross_loss=gl,
        trades_per_day=len(trades)/days
    )

def apply_cap(flat):
    flat.sort(key=lambda t:t["entry_ts"])
    active=[]; kept=[]
    for t in flat:
        active=[e for e in active if e>t["entry_ts"]]
        if len(active)<MAX_POSITIONS:
            active.append(t["exit_ts"]); kept.append(t)
    return kept

# ── WORKER ────────────────────────────────────────────────────────────────────
def worker(symbol):
    try:
        bars=fetch_symbol(symbol)
        if len(bars)<WARMUP_BARS+50:
            return symbol,None,f"only {len(bars)} bars"
        res={}
        for vname,vc in VARIANTS.items():
            res[vname]=backtest_coin(bars,vc["tp"],vc["sl"],vc["adx_min"])
        return symbol,res,None
    except Exception as e:
        return symbol,None,str(e)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    t0=time.time()
    print("=== Backtest v8.6 | 15m | G / H / L ===",flush=True)

    print("Discovering symbols...",flush=True)
    symbols=discover_symbols()
    print(f"Found {len(symbols)} symbols",flush=True)

    coin_trades=defaultdict(dict)  # vname -> sym -> trades
    issues=[]; done=0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs={ex.submit(worker,s):s for s in symbols}
        for fut in as_completed(futs):
            sym=futs[fut]; done+=1
            try:
                s,res,err=fut.result()
            except Exception as e:
                issues.append({"symbol":sym,"error":str(e)})
                print(f"[{done}/{len(symbols)}] {sym} EXC: {e}",flush=True)
                continue
            if err:
                issues.append({"symbol":s,"error":err})
                print(f"[{done}/{len(symbols)}] {s} SKIP: {err}",flush=True)
                continue
            for vname,trades in res.items():
                coin_trades[vname][sym]=trades
            print(f"[{done}/{len(symbols)}] {s} OK",flush=True)

    # calc days
    all_ts=[]
    for vn in VARIANTS:
        for tr in coin_trades[vn].values():
            all_ts.extend(t["exit_ts"] for t in tr)
    days=(max(all_ts)-min(all_ts))/(1000*86400) if all_ts else 730

    report={"meta":{},"variants":{},"symbol_issues":issues}
    summary=[]

    for vname,vc in VARIANTS.items():
        flat=[]
        for sym,trades in coin_trades[vname].items():
            for t in trades: t["symbol"]=sym; flat.append(t)
        capped=apply_cap(flat)
        coin_map=defaultdict(list)
        for t in capped: coin_map[t["symbol"]].append(t)
        coin_stats={s:calc_stats(tr,days) for s,tr in coin_map.items()}
        port=calc_stats(capped,days)

        passing={s:st for s,st in coin_stats.items()
                 if st["pf"]>=1.5 and st["wr"]>=42 and st["trades"]>=10}

        report["variants"][vname]={"stats":port,"coins":coin_stats}

        tp_p=int(vc["tp"]*100); sl_p=int(vc["sl"]*100)
        summary+=[
            f"\n{'='*65}",
            f"  VARIANT {vname} — TP {tp_p}% / SL {sl_p}% / ADX>={vc['adx_min']} / 15m",
            f"{'='*65}",
            f"  Trades       : {port['trades']}",
            f"  Trades/day   : {port['trades_per_day']:.2f}",
            f"  Win Rate     : {port['wr']:.2f}%",
            f"  Profit Factor: {port['pf']:.3f}",
            f"  Net PnL      : ${port['net_pnl']:.2f}",
            f"  Max Drawdown : {port['max_dd']:.2f}%",
            f"  Long  : {port['long_trades']} trades | WR {port['long_wr']:.1f}%",
            f"  Short : {port['short_trades']} trades | WR {port['short_wr']:.1f}%",
            f"  Gross Profit : ${port['gross_profit']:.2f} | Loss: ${port['gross_loss']:.2f}",
        ]

        # top 30 by PF (min 5 trades)
        top=sorted([(s,st) for s,st in coin_stats.items() if st["trades"]>=5],
                   key=lambda x: x[1]["pf"] if x[1]["pf"]!=float("inf") else 9999,
                   reverse=True)[:30]
        summary.append(f"\n  -- Top 30 (min 5 trades) --")
        summary.append(f"  {'Symbol':<26}{'Trades':>6}{'WR%':>8}{'PF':>9}{'NetPnL':>12}")
        for s,st in top:
            pfs="   inf" if st["pf"]==float("inf") else f"{st['pf']:.3f}"
            summary.append(f"  {s:<26}{st['trades']:>6}{st['wr']:>7.1f}%{pfs:>9}  ${st['net_pnl']:>9.2f}")

        summary.append(f"\n  -- Passing (PF>=1.5, WR>=42%, T>=10): {len(passing)} --")
        for s,st in sorted(passing.items(),key=lambda x:x[1]["pf"] if x[1]["pf"]!=float("inf") else 9999,reverse=True):
            pfs="inf" if st["pf"]==float("inf") else f"{st['pf']:.3f}"
            summary.append(f"  OK {s:<24} PF={pfs} WR={st['wr']:.1f}% T={st['trades']} PnL=${st['net_pnl']:.2f}")

    # cross-variant table
    summary+=[f"\n{'='*65}","  CROSS-VARIANT SUMMARY",f"{'='*65}",
               f"  {'Var':<5}{'ADX':>5}{'TP':>5}{'SL':>5}{'Trades':>8}{'T/day':>7}{'WR%':>7}{'PF':>9}{'NetPnL':>13}{'MaxDD':>7}"]
    for vname,vc in VARIANTS.items():
        st=report["variants"][vname]["stats"]
        summary.append(
            f"  {vname:<5}{vc['adx_min']:>5}{int(vc['tp']*100):>4}%{int(vc['sl']*100):>4}%"
            f"{st['trades']:>8}{st['trades_per_day']:>7.1f}{st['wr']:>6.1f}%"
            f"{st['pf']:>9.3f}  ${st['net_pnl']:>10.2f}{st['max_dd']:>6.1f}%"
        )

    summary+=["",f"-- Issues ({len(issues)}) --"]
    for i in issues: summary.append(f"  {i['symbol']}: {i['error']}")

    txt="\n".join(summary)
    with open("backtest_summary.txt","w") as f: f.write(txt)

    report["meta"]={"version":"v8.6","interval":"15m","period":f"{MONTHS[0]} -> {MONTHS[-1]}",
                    "symbols_tested":len(symbols),"variants":VARIANTS,
                    "settings":dict(capital=CAPITAL,risk_pct=RISK_PCT,fee=FEE,slip=SLIP,
                                    max_positions=MAX_POSITIONS,cooldown=COOLDOWN_BARS)}
    with open("backtest_report.json","w") as f: json.dump(report,f,default=str)

    print(f"\nDone in {time.time()-t0:.0f}s",flush=True)
    print(txt,flush=True)

if __name__=="__main__":
    main()

