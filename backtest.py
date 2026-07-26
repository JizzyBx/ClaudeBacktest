"""
Backtest Engine V4 — Kimi AI Strategies
Strategy A: Liquidity Sweep + Volume (all 13 coins)
Strategy B: Session Open Momentum (majors only)
Strategy C: Hybrid Sweep + Session (BTC/ETH/SOL only)
Data: data-api.binance.vision | 2 Years | stdlib only
"""

import json, time, math, statistics
from datetime import datetime, timezone
from collections import defaultdict
import urllib.request

# ── Coins ─────────────────────────────────────────────────────────────────────
TIER1 = ['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT']
TIER2 = ['ADAUSDT','DOGEUSDT','LINKUSDT','LTCUSDT','POLUSDT']
MEMES = ['1000BONKUSDT','1000PEPEUSDT','1000SHIBUSDT']
ALL_COINS = TIER1 + TIER2 + MEMES

SA_COINS = ALL_COINS
SB_COINS = TIER1
SC_COINS = ['BTCUSDT','ETHUSDT','SOLUSDT']

def is_meme(s): return s in MEMES

# ── Risk config ───────────────────────────────────────────────────────────────
FEE_RATE    = 0.0005
SLIPPAGE    = 0.0002
TOTAL_FEE   = FEE_RATE + SLIPPAGE  # per side
INITIAL_CAP = 10000.0

def risk(s):   return 0.0025 if is_meme(s) else (0.004 if s in TIER2 else 0.005)
def wick_t(s): return 0.005  if is_meme(s) else 0.003   # wick threshold
def vol_m(s):  return 2.0    if is_meme(s) else 1.5     # volume multiplier
def sl_pct(s): return 0.010  if is_meme(s) else 0.005   # SL % below sweep
def rr():      return 2.0                                 # R:R for A and C

# ── Fetch ─────────────────────────────────────────────────────────────────────
BASE = "https://data-api.binance.vision"

def fetch(sym, interval, start_ms, end_ms):
    out, cur, retries = [], start_ms, 0
    while cur < end_ms:
        url = (f"{BASE}/api/v3/klines?symbol={sym}&interval={interval}"
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
            if retries > 5: print(f"  [!] {sym} {interval}: {e}"); break
            time.sleep(2 * retries)
    return out

def parse(raw):
    o=[float(k[1]) for k in raw]; h=[float(k[2]) for k in raw]
    l=[float(k[3]) for k in raw]; c=[float(k[4]) for k in raw]
    v=[float(k[5]) for k in raw]; t=[int(k[0])   for k in raw]
    return o,h,l,c,v,t

# ── Indicators ────────────────────────────────────────────────────────────────
def vsma(v, p=20):
    r = [None]*(p-1)
    for i in range(p-1, len(v)):
        r.append(sum(v[i-p+1:i+1])/p)
    return r

# ── Session helpers ───────────────────────────────────────────────────────────
def session_of(ts_ms):
    """Returns session name or None. London=07-12, NY=13:30-17."""
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    h = dt.hour; m = dt.minute
    mins = h*60 + m
    if 7*60 <= mins < 12*60:   return 'LONDON'
    if 13*60+30 <= mins < 17*60: return 'NY'
    return None

def in_kill_zone(ts_ms):
    """London 07-10, NY 13:30-16:30 — tighter for Strategy C."""
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    mins = dt.hour*60 + dt.minute
    if 7*60 <= mins < 10*60:    return True
    if 13*60+30 <= mins < 16*60+30: return True
    return False

def is_session_open_candle(ts_ms):
    """Returns (is_first_or_second, session) for Strategy B."""
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    mins = dt.hour*60 + dt.minute
    # London: 08:00-10:00 (first 2 candles of 08:00 session)
    if mins in (8*60, 8*60+30, 9*60, 9*60+30):
        return True, 'LONDON'
    # NY: 13:30-15:30
    if mins in (13*60+30, 14*60, 14*60+30, 15*60):
        return True, 'NY'
    return False, None

# ── PDH/PDL calculator ────────────────────────────────────────────────────────
def calc_pdh_pdl(times, highs, lows):
    """For each candle index, return the PDH and PDL (previous calendar day)."""
    pdh = [None]*len(times)
    pdl = [None]*len(times)
    # Group by UTC date
    day_h = {}; day_l = {}
    for i, ts in enumerate(times):
        dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
        d = dt.date()
        day_h[d] = max(day_h.get(d, highs[i]), highs[i])
        day_l[d] = min(day_l.get(d, lows[i]),  lows[i])
    sorted_days = sorted(day_h.keys())
    day_to_prev = {}
    for i, d in enumerate(sorted_days):
        if i > 0:
            day_to_prev[d] = sorted_days[i-1]
    for i, ts in enumerate(times):
        dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
        d = dt.date()
        prev = day_to_prev.get(d)
        if prev:
            pdh[i] = day_h[prev]
            pdl[i] = day_l[prev]
    return pdh, pdl

# ── Swing high/low ────────────────────────────────────────────────────────────
def get_recent_swing(highs, lows, idx, lookback=48, n=3):
    """
    Find most recent unbroken swing high and swing low
    within last `lookback` candles before idx.
    Swing high: candle high > n candles before AND n candles after.
    We scan backwards so we need confirmed swings (n bars after exist).
    """
    sh = None; sl_val = None
    start = max(n, idx - lookback)
    # scan from most recent backwards
    for j in range(idx - n - 1, start - 1, -1):
        if j < n: continue
        # swing high
        if sh is None:
            is_sh = all(highs[j] > highs[j-k] for k in range(1, n+1)) and \
                    all(highs[j] > highs[j+k] for k in range(1, min(n+1, idx-j+1)))
            if is_sh:
                # check not broken (no candle after j up to idx exceeded it)
                if all(highs[k] <= highs[j] for k in range(j+1, idx+1)):
                    sh = highs[j]
        # swing low
        if sl_val is None:
            is_sl = all(lows[j] < lows[j-k] for k in range(1, n+1)) and \
                    all(lows[j] < lows[j+k] for k in range(1, min(n+1, idx-j+1)))
            if is_sl:
                if all(lows[k] >= lows[j] for k in range(j+1, idx+1)):
                    sl_val = lows[j]
        if sh is not None and sl_val is not None:
            break
    return sh, sl_val

# ── Metrics ───────────────────────────────────────────────────────────────────
def calc_metrics(trades, sym):
    if not trades: return None
    wins  = [t for t in trades if t['win']]
    loss  = [t for t in trades if not t['win']]
    n = len(trades); nw = len(wins)
    gp = sum(t['pnl'] for t in wins)        if wins else 0
    gl = abs(sum(t['pnl'] for t in loss))   if loss else 0
    pf = gp/gl if gl > 0 else float('inf')
    wr = nw/n
    net = sum(t['pnl'] for t in trades)*100
    aw  = statistics.mean(t['pnl'] for t in wins)*100  if wins else 0
    al  = statistics.mean(t['pnl'] for t in loss)*100  if loss else 0
    exp = wr*aw + (1-wr)*al
    dur = statistics.mean(t['dur'] for t in trades)
    # drawdown
    eq=INITIAL_CAP; pk=eq; mdd=0
    for t in trades:
        r=t.get('risk',0.005)
        eq += t['pnl']/r * eq * r
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
    longs  = [t for t in trades if t['dir']=='LONG']
    shorts = [t for t in trades if t['dir']=='SHORT']
    lwr = sum(1 for t in longs  if t['win'])/len(longs)*100  if longs  else 0
    swr = sum(1 for t in shorts if t['win'])/len(shorts)*100 if shorts else 0
    monthly=defaultdict(float)
    for t in trades:
        dt=datetime.fromtimestamp(t['exit_t']/1000,tz=timezone.utc)
        monthly[dt.strftime('%Y-%m')]+=t['pnl']*100
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

def agg(all_trades):
    return calc_metrics(all_trades,'ALL') if all_trades else None

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY A — Liquidity Sweep + Volume
# ══════════════════════════════════════════════════════════════════════════════
def run_A(sym, raw):
    if len(raw) < 300: return None, {}
    o,h,l,c,v,t = parse(raw)
    vs = vsma(v, 20)
    pdh_arr, pdl_arr = calc_pdh_pdl(t, h, l)
    wt   = wick_t(sym)
    vm   = vol_m(sym)
    slp  = sl_pct(sym)
    rsk  = risk(sym)
    timeout = 8
    cooldown_bars = 4

    flt = defaultdict(int)
    trades = []; in_pos = False
    ep=sl=tp=d=eb=None; cooldown=0
    # State: waiting for reclaim after a confirmed sweep
    sweep_pending = None  # dict with sweep info

    for i in range(50, len(c)):
        flt['total'] += 1

        # ── Exit ──
        if in_pos:
            hit_sl=hit_tp=False
            if d=='LONG':
                if l[i]<=sl: hit_sl=True
                if h[i]>=tp: hit_tp=True
            else:
                if h[i]>=sl: hit_sl=True
                if l[i]<=tp: hit_tp=True
            timeout_hit = (i - eb) >= timeout
            if hit_sl or hit_tp or timeout_hit:
                xp = tp if hit_tp else (sl if hit_sl else c[i])
                raw_pnl = (xp-ep)/ep if d=='LONG' else (ep-xp)/ep
                net = raw_pnl - 2*TOTAL_FEE
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=hit_tp,dur=i-eb,risk=rsk))
                in_pos=False; cooldown=cooldown_bars; sweep_pending=None
            else:
                flt['in_pos']+=1; continue

        if cooldown>0: cooldown-=1; flt['cooldown']+=1; continue

        vs_i = vs[i]
        if vs_i is None: continue

        # ── Check reclaim from previous sweep ──
        if sweep_pending is not None:
            sp = sweep_pending
            # Must be the NEXT candle after sweep
            if i == sp['sweep_bar'] + 1:
                reclaimed = False
                if sp['dir']=='LONG'  and c[i] > sp['level']:  reclaimed=True
                if sp['dir']=='SHORT' and c[i] < sp['level']:  reclaimed=True
                if reclaimed:
                    ep = c[i]*(1+SLIPPAGE if sp['dir']=='LONG' else 1-SLIPPAGE)
                    if sp['dir']=='LONG':
                        sl_price = sp['sweep_low']  * (1 - slp)
                        tp_price = ep + rr() * (ep - sl_price)
                    else:
                        sl_price = sp['sweep_high'] * (1 + slp)
                        tp_price = ep - rr() * (sl_price - ep)
                    d=sp['dir']; eb=i; ep=ep; sl=sl_price; tp=tp_price
                    in_pos=True; flt['signals']+=1
                else:
                    flt['no_reclaim']+=1
                sweep_pending=None
                if in_pos: continue

            else:
                # stale — void it
                sweep_pending=None

        # ── Session filter ──
        sess = session_of(t[i])
        if sess is None: flt['no_session']+=1; continue

        # ── Get levels ──
        pdh_i = pdh_arr[i]; pdl_i = pdl_arr[i]
        sh_i, sl_i_level = get_recent_swing(h, l, i, lookback=48, n=3)

        # closest resistance (lower of PDH vs SH)
        res_levels = [x for x in [pdh_i, sh_i] if x is not None]
        sup_levels = [x for x in [pdl_i, sl_i_level] if x is not None]
        if not res_levels and not sup_levels: flt['no_levels']+=1; continue

        res = min(res_levels) if res_levels else None
        sup = max(sup_levels) if sup_levels else None

        # ── LONG sweep: low breaks below support ──
        if sup is not None:
            if l[i] < sup:
                # wick extension
                wick_ext = (sup - l[i]) / sup
                if wick_ext >= wt:
                    # volume
                    if v[i] >= vm * vs_i:
                        # wick: lower wick must exist (low below body)
                        body_low = min(o[i], c[i])
                        if l[i] < body_low:
                            sweep_pending = dict(
                                dir='LONG', level=sup,
                                sweep_bar=i, sweep_low=l[i],
                                sweep_high=h[i]
                            )
                            flt['sweep_long']+=1
                            continue
                        else: flt['no_wick_body']+=1
                    else: flt['vol_long']+=1
                else: flt['wick_ext_long']+=1

        # ── SHORT sweep: high breaks above resistance ──
        if res is not None:
            if h[i] > res:
                wick_ext = (h[i] - res) / res
                if wick_ext >= wt:
                    if v[i] >= vm * vs_i:
                        body_high = max(o[i], c[i])
                        if h[i] > body_high:
                            sweep_pending = dict(
                                dir='SHORT', level=res,
                                sweep_bar=i, sweep_low=l[i],
                                sweep_high=h[i]
                            )
                            flt['sweep_short']+=1
                            continue
                        else: flt['no_wick_body']+=1
                    else: flt['vol_short']+=1
                else: flt['wick_ext_short']+=1

        flt['no_setup']+=1

    return trades, dict(flt)

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY B — Session Open Momentum
# ══════════════════════════════════════════════════════════════════════════════
def run_B(sym, raw):
    if len(raw) < 100: return None, {}
    o,h,l,c,v,t = parse(raw)
    vs = vsma(v, 20)
    rsk = risk(sym)
    timeout_bars = 4  # ~2 hours = end of session window

    flt = defaultdict(int)
    trades = []; in_pos = False
    ep=sl=tp=d=eb=None; cooldown=0
    # Track session first candle
    session_first = {}   # session_key -> candle index

    for i in range(20, len(c)):
        flt['total']+=1

        # ── Exit ──
        if in_pos:
            hit_sl=hit_tp=False
            if d=='LONG':
                if l[i]<=sl: hit_sl=True
                if h[i]>=tp: hit_tp=True
            else:
                if h[i]>=sl: hit_sl=True
                if l[i]<=tp: hit_tp=True
            # Time stop: end of session window
            timeout_hit = (i - eb) >= timeout_bars
            if hit_sl or hit_tp or timeout_hit:
                xp = tp if hit_tp else (sl if hit_sl else c[i])
                raw_pnl = (xp-ep)/ep if d=='LONG' else (ep-xp)/ep
                net = raw_pnl - 2*TOTAL_FEE
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=hit_tp,dur=i-eb,risk=rsk))
                in_pos=False; cooldown=6
            else: flt['in_pos']+=1; continue

        if cooldown>0: cooldown-=1; flt['cooldown']+=1; continue

        vs_i = vs[i]
        if vs_i is None: continue

        is_open, sess = is_session_open_candle(t[i])
        if not is_open: flt['no_session']+=1; continue

        dt = datetime.fromtimestamp(t[i]/1000, tz=timezone.utc)
        sess_key = f"{dt.date()}_{sess}"

        # First candle of session
        if sess_key not in session_first:
            session_first[sess_key] = i
            flt['first_candle']+=1
            continue  # just record it, don't trade yet

        first_i = session_first[sess_key]
        # Make sure this is second or third candle of session
        candle_num = i - first_i
        if candle_num > 3: flt['too_late']+=1; continue

        # First candle must be directional + volume
        fc_body = abs(c[first_i] - o[first_i])
        fc_range = h[first_i] - l[first_i]
        if fc_range == 0: flt['no_range']+=1; continue
        if fc_body / fc_range < 0.50: flt['weak_body']+=1; continue
        if v[first_i] < 2.0 * vs_i: flt['vol_weak']+=1; continue

        bullish_first = c[first_i] > o[first_i]
        fc_low = l[first_i]; fc_high = h[first_i]
        fc_range_val = fc_high - fc_low
        midpoint = (fc_high + fc_low) / 2

        sig = None
        if bullish_first:
            # Pullback candle: retraces to 50% but doesn't close below low
            if l[i] <= midpoint and c[i] >= fc_low:
                sig = 'LONG'
            else: flt['no_pullback']+=1; continue
        else:
            # Bearish first candle
            if h[i] >= midpoint and c[i] <= fc_high:
                sig = 'SHORT'
            else: flt['no_pullback']+=1; continue

        if sig:
            flt['signals']+=1
            ep = c[i]*(1+SLIPPAGE if sig=='LONG' else 1-SLIPPAGE)
            if sig=='LONG':
                sl = fc_low * (1 - 0.001)
                tp = ep + 1.5 * fc_range_val
            else:
                sl = fc_high * (1 + 0.001)
                tp = ep - 1.5 * fc_range_val
            d=sig; eb=i; in_pos=True

    return trades, dict(flt)

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY C — Hybrid Sweep + Session (Kill Zone only)
# ══════════════════════════════════════════════════════════════════════════════
def run_C(sym, raw):
    """Same as Strategy A but ONLY takes sweeps inside kill zones."""
    if len(raw) < 300: return None, {}
    o,h,l,c,v,t = parse(raw)
    vs = vsma(v, 20)
    pdh_arr, pdl_arr = calc_pdh_pdl(t, h, l)
    wt   = wick_t(sym)
    vm   = vol_m(sym)
    slp  = sl_pct(sym)
    rsk  = risk(sym)
    timeout = 6
    cooldown_bars = 8

    flt = defaultdict(int)
    trades = []; in_pos = False
    ep=sl=tp=d=eb=None; cooldown=0
    sweep_pending = None

    for i in range(50, len(c)):
        flt['total']+=1
        if in_pos:
            hit_sl=hit_tp=False
            if d=='LONG':
                if l[i]<=sl: hit_sl=True
                if h[i]>=tp: hit_tp=True
            else:
                if h[i]>=sl: hit_sl=True
                if l[i]<=tp: hit_tp=True
            timeout_hit = (i - eb) >= timeout
            if hit_sl or hit_tp or timeout_hit:
                xp = tp if hit_tp else (sl if hit_sl else c[i])
                raw_pnl = (xp-ep)/ep if d=='LONG' else (ep-xp)/ep
                net = raw_pnl - 2*TOTAL_FEE
                trades.append(dict(dir=d,entry=ep,exit=xp,
                    entry_t=t[eb],exit_t=t[i],pnl=net,win=net>0,
                    hit_tp=hit_tp,dur=i-eb,risk=rsk))
                in_pos=False; cooldown=cooldown_bars; sweep_pending=None
            else: flt['in_pos']+=1; continue

        if cooldown>0: cooldown-=1; flt['cooldown']+=1; continue

        vs_i = vs[i]
        if vs_i is None: continue

        # reclaim check
        if sweep_pending is not None:
            sp = sweep_pending
            if i == sp['sweep_bar'] + 1:
                reclaimed = False
                if sp['dir']=='LONG'  and c[i] > sp['level']: reclaimed=True
                if sp['dir']=='SHORT' and c[i] < sp['level']: reclaimed=True
                if reclaimed:
                    ep2 = c[i]*(1+SLIPPAGE if sp['dir']=='LONG' else 1-SLIPPAGE)
                    if sp['dir']=='LONG':
                        sl_p = sp['sweep_low']  * (1 - slp)
                        tp_p = ep2 + rr() * (ep2 - sl_p)
                    else:
                        sl_p = sp['sweep_high'] * (1 + slp)
                        tp_p = ep2 - rr() * (sl_p - ep2)
                    d=sp['dir']; eb=i; ep=ep2; sl=sl_p; tp=tp_p
                    in_pos=True; flt['signals']+=1
                else: flt['no_reclaim']+=1
                sweep_pending=None
                if in_pos: continue
            else:
                sweep_pending=None

        # KILL ZONE filter — stricter than Strategy A
        if not in_kill_zone(t[i]): flt['no_killzone']+=1; continue

        pdh_i = pdh_arr[i]; pdl_i = pdl_arr[i]
        sh_i, sl_i_level = get_recent_swing(h, l, i, lookback=48, n=3)

        res_levels = [x for x in [pdh_i, sh_i] if x is not None]
        sup_levels = [x for x in [pdl_i, sl_i_level] if x is not None]
        if not res_levels and not sup_levels: flt['no_levels']+=1; continue

        res = min(res_levels) if res_levels else None
        sup = max(sup_levels) if sup_levels else None

        if sup is not None and l[i] < sup:
            wick_ext = (sup - l[i]) / sup
            if wick_ext >= wt and v[i] >= vm * vs_i:
                body_low = min(o[i], c[i])
                if l[i] < body_low:
                    sweep_pending=dict(dir='LONG',level=sup,
                        sweep_bar=i,sweep_low=l[i],sweep_high=h[i])
                    flt['sweep_long']+=1; continue

        if res is not None and h[i] > res:
            wick_ext = (h[i] - res) / res
            if wick_ext >= wt and v[i] >= vm * vs_i:
                body_high = max(o[i], c[i])
                if h[i] > body_high:
                    sweep_pending=dict(dir='SHORT',level=res,
                        sweep_bar=i,sweep_low=l[i],sweep_high=h[i])
                    flt['sweep_short']+=1; continue

        flt['no_setup']+=1

    return trades, dict(flt)

# ── Summary writer ────────────────────────────────────────────────────────────
def write_summary(results, path='backtest_summary.txt'):
    L=[]
    L.append("="*72)
    L.append("BACKTEST REPORT V4 — Kimi AI Strategies | 2 Years")
    L.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    L.append(f"Fee: {FEE_RATE*100}% + {SLIPPAGE*100}% slippage per side")
    L.append("="*72)

    for sk, sd in results.items():
        L.append(f"\n{'─'*72}")
        L.append(f"  {sd['name']}")
        L.append(f"{'─'*72}")
        ag = sd.get('aggregate')
        if ag:
            pf_ok=ag['pf']>=1.5; wr_ok=ag['wr']>=42
            L.append(f"  AGGREGATE")
            L.append(f"  Total Trades      : {ag['n']}")
            L.append(f"  Profit Factor     : {ag['pf']}  {'✅ TARGET MET' if pf_ok else '❌ BELOW 1.5'}")
            L.append(f"  Win Rate          : {ag['wr']}%  {'✅' if wr_ok else '❌ BELOW 42%'}")
            L.append(f"  Net PnL           : {ag['net']}%")
            L.append(f"  Max Drawdown      : {ag['mdd']}%")
            L.append(f"  Sharpe            : {ag['sharpe']}")
            L.append(f"  Sortino           : {ag['sortino']}")
            L.append(f"  Avg Winner        : {ag['aw']}%")
            L.append(f"  Avg Loser         : {ag['al']}%")
            L.append(f"  Expectancy/Trade  : {ag['exp']}%")
            L.append(f"  Avg Duration(bars): {ag['dur']}")
            L.append(f"  Long WR           : {ag['lwr']}% ({ag['nlongs']} trades)")
            L.append(f"  Short WR          : {ag['swr']}% ({ag['nshorts']} trades)")
            L.append(f"  Max Consec Wins   : {ag['maxcw']}")
            L.append(f"  Max Consec Losses : {ag['maxcl']}")

            # validation
            coins=[d for d in sd['by_symbol'].values() if isinstance(d,dict) and 'pf' in d]
            pp=sum(1 for d in coins if d['pf']>=1.5)
            pw=sum(1 for d in coins if d['wr']>=42)
            tc=len(coins)
            btc=sd['by_symbol'].get('BTCUSDT',{})
            btc_ok=isinstance(btc,dict) and btc.get('pf',0)>=1.3
            L.append(f"\n  VALIDATION (need 8+ coins)")
            L.append(f"  Coins PF>=1.5 : {pp}/{tc}")
            L.append(f"  Coins WR>=42% : {pw}/{tc}")
            L.append(f"  BTC PF>=1.3   : {'✅' if btc_ok else '❌'}")
            valid=pp>=8 and pw>=8 and btc_ok
            L.append(f"  OVERALL VALID : {'✅ CONSIDER LIVE' if valid else '❌ NOT READY'}")

        fs=sd.get('filter_stats',{})
        if fs:
            L.append(f"\n  FILTER STATS")
            for k,v2 in sorted(fs.items(),key=lambda x:-x[1]):
                L.append(f"    {k:<22}: {v2}")

        L.append(f"\n  {'Symbol':<16}{'Trades':>7}{'PF':>8}{'WR%':>7}"
                 f"{'Net%':>9}{'DD%':>7}{'Longs':>7}{'Shorts':>8}")
        L.append(f"  {'─'*68}")
        by=sd.get('by_symbol',{})
        rows=[(s,d) for s,d in by.items() if isinstance(d,dict) and 'pf' in d]
        rows.sort(key=lambda x:x[1]['pf'],reverse=True)
        for s,d in rows:
            L.append(f"  {s:<16}{d['n']:>7}{d['pf']:>8.3f}{d['wr']:>7.1f}"
                     f"{d['net']:>9.2f}{d['mdd']:>7.2f}{d['nlongs']:>7}{d['nshorts']:>8}")
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

    # Fetch all data first
    print("📥 Fetching data...")
    d30={}
    needed=list(dict.fromkeys(SA_COINS+SB_COINS+SC_COINS))
    for sym in needed:
        print(f"  {sym} 30m...", end=' ', flush=True)
        d30[sym]=fetch(sym,'30m',start_ms,now_ms)
        print(f"{len(d30[sym])} candles")

    results={}

    # ══ STRATEGY A ══
    print(f"\n{'='*60}\n  STRATEGY A — Liquidity Sweep + Volume\n{'='*60}")
    sa_by={}; sa_all=[]; sa_flt=defaultdict(int)
    for sym in SA_COINS:
        print(f"  {sym}...", end=' ', flush=True)
        tr,flt=run_A(sym, d30.get(sym,[]))
        if tr is None: print("SKIP"); sa_by[sym]={'error':'insufficient_data'}; continue
        m=calc_metrics(tr,sym)
        if m:
            print(f"Trades:{m['n']} PF:{m['pf']} WR:{m['wr']}%")
            m['filter_stats']=flt; sa_by[sym]=m; sa_all.extend(tr)
        else: print("no trades"); sa_by[sym]={'error':'no_trades','fs':flt}
        for k,v2 in flt.items(): sa_flt[k]+=v2
    ag_a=agg(sa_all)
    if ag_a:
        print(f"\n  ── AGGREGATE A ──")
        print(f"  Trades:{ag_a['n']} PF:{ag_a['pf']} WR:{ag_a['wr']}% "
              f"Net:{ag_a['net']}% DD:{ag_a['mdd']}%")
    results['A']=dict(name='Strategy A — Liquidity Sweep + Volume',
                      aggregate=ag_a,by_symbol=sa_by,filter_stats=dict(sa_flt))

    # ══ STRATEGY B ══
    print(f"\n{'='*60}\n  STRATEGY B — Session Open Momentum\n{'='*60}")
    sb_by={}; sb_all=[]; sb_flt=defaultdict(int)
    for sym in SB_COINS:
        print(f"  {sym}...", end=' ', flush=True)
        tr,flt=run_B(sym, d30.get(sym,[]))
        if tr is None: print("SKIP"); sb_by[sym]={'error':'insufficient_data'}; continue
        m=calc_metrics(tr,sym)
        if m:
            print(f"Trades:{m['n']} PF:{m['pf']} WR:{m['wr']}%")
            m['filter_stats']=flt; sb_by[sym]=m; sb_all.extend(tr)
        else: print("no trades"); sb_by[sym]={'error':'no_trades','fs':flt}
        for k,v2 in flt.items(): sb_flt[k]+=v2
    ag_b=agg(sb_all)
    if ag_b:
        print(f"\n  ── AGGREGATE B ──")
        print(f"  Trades:{ag_b['n']} PF:{ag_b['pf']} WR:{ag_b['wr']}% "
              f"Net:{ag_b['net']}% DD:{ag_b['mdd']}%")
    results['B']=dict(name='Strategy B — Session Open Momentum',
                      aggregate=ag_b,by_symbol=sb_by,filter_stats=dict(sb_flt))

    # ══ STRATEGY C ══
    print(f"\n{'='*60}\n  STRATEGY C — Hybrid Sweep + Kill Zone\n{'='*60}")
    sc_by={}; sc_all=[]; sc_flt=defaultdict(int)
    for sym in SC_COINS:
        print(f"  {sym}...", end=' ', flush=True)
        tr,flt=run_C(sym, d30.get(sym,[]))
        if tr is None: print("SKIP"); sc_by[sym]={'error':'insufficient_data'}; continue
        m=calc_metrics(tr,sym)
        if m:
            print(f"Trades:{m['n']} PF:{m['pf']} WR:{m['wr']}%")
            m['filter_stats']=flt; sc_by[sym]=m; sc_all.extend(tr)
        else: print("no trades"); sc_by[sym]={'error':'no_trades','fs':flt}
        for k,v2 in flt.items(): sc_flt[k]+=v2
    ag_c=agg(sc_all)
    if ag_c:
        print(f"\n  ── AGGREGATE C ──")
        print(f"  Trades:{ag_c['n']} PF:{ag_c['pf']} WR:{ag_c['wr']}% "
              f"Net:{ag_c['net']}% DD:{ag_c['mdd']}%")
    results['C']=dict(name='Strategy C — Hybrid Sweep + Kill Zone',
                      aggregate=ag_c,by_symbol=sc_by,filter_stats=dict(sc_flt))

    # ── Write outputs ──
    with open('backtest_report.json','w') as f: json.dump(results,f,indent=2)
    print("\n✅ backtest_report.json written")
    write_summary(results)
    print("\n🏁 Done. Share backtest_report.json + backtest_summary.txt")

if __name__=='__main__':
    print("🔍 Backtest V4 | Kimi Strategies | 13 Coins | 2 Years")
    main()
