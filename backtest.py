"""
G Max — 6-Variant Backtest
Variants: A (ADX+DI), B (ATR-TP), C (HTF 1h in-memory), D (S/R), E (RSI gate), F (ADX28+TP5%)
Coins: 117 COINS_UNIVERSE | Timeframe: 15m | Period: 2 years
Leverage: 5x | Margin: 2% equity/trade | Shards: 30 | Fetch workers: 20 | Backtest workers: 8
1h bars derived from 15m — no second download round
"""

import sys, json, csv, io, zipfile, urllib.request, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ── Coin Universe (117) ────────────────────────────────────
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

NUM_SHARDS      = 30
FETCH_WORKERS   = 20   # parallel downloads per shard
BACKTEST_WORKERS= 8    # parallel coin backtests per shard

_NOW      = datetime.now(timezone.utc)
END_YM    = (_NOW.year, _NOW.month)
_START    = _NOW - timedelta(days=730)
START_YM  = (_START.year, _START.month)

TIMEFRAME = '15m'
CAPITAL   = 10_000.0
MARGIN_PCT= 0.02
LEVERAGE  = 5
FEE       = 0.0005
SLIP      = 0.0002
MAX_BARS  = 960    # 10 days at 15m
MIN_BARS  = 100

VARIANTS = {
    'A': {'name': 'ADX+DI Direction',  'tp': 0.030, 'sl': 0.120},
    'B': {'name': 'ATR Dynamic TP',    'tp': None,  'sl': 0.120, 'atr_mult': 2.5},
    'C': {'name': 'HTF 1h Confirm',    'tp': 0.040, 'sl': 0.120},
    'D': {'name': 'S/R Zone Filter',   'tp': 0.035, 'sl': 0.120},
    'E': {'name': 'RSI Gate',          'tp': 0.030, 'sl': 0.120},
    'F': {'name': 'ADX28 + TP5%',      'tp': 0.050, 'sl': 0.120},
}

# ── Fetch ──────────────────────────────────────────────────
BASE_URL = 'https://data.binance.vision/data/futures/um/monthly/klines'

def _months(start_ym, end_ym):
    y, m = start_ym
    ey, em = end_ym
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out

def fetch_month(symbol, year, month):
    url = f'{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year:04d}-{month:02d}.zip'
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=45) as r:
                data = r.read()
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                with z.open(z.namelist()[0]) as f:
                    rows = []
                    for row in csv.reader(io.TextIOWrapper(f)):
                        try:
                            ts = int(row[0])
                            if ts > 10**14: ts //= 1000
                            rows.append((ts, float(row[1]), float(row[2]),
                                         float(row[3]), float(row[4])))
                        except (ValueError, IndexError):
                            continue
                    return rows
        except Exception:
            if attempt < 2:
                time.sleep(1 + attempt)
    return []

def fetch_symbol_parallel(symbol):
    """Fetch all months for a symbol using a thread pool."""
    months = _months(START_YM, END_YM)
    results = [None] * len(months)
    with ThreadPoolExecutor(max_workers=min(len(months), 8)) as ex:
        futs = {ex.submit(fetch_month, symbol, y, m): idx
                for idx, (y, m) in enumerate(months)}
        for fut in as_completed(futs):
            results[futs[fut]] = fut.result() or []
    all_rows = [r for batch in results for r in batch]
    seen = {}
    for row in all_rows:
        seen[row[0]] = row
    return [seen[k] for k in sorted(seen)]

# ── HTF builder: 15m → 1h in memory ───────────────────────
def build_1h_from_15m(candles_15m):
    """Group 15m candles into 1h candles. No download needed."""
    if not candles_15m:
        return []
    buckets = {}
    for ts, o, h, l, c in candles_15m:
        # floor to hour boundary (ms)
        hour_ts = (ts // 3_600_000) * 3_600_000
        if hour_ts not in buckets:
            buckets[hour_ts] = [o, h, l, c]
        else:
            buckets[hour_ts][1] = max(buckets[hour_ts][1], h)
            buckets[hour_ts][2] = min(buckets[hour_ts][2], l)
            buckets[hour_ts][3] = c
    return [(k, *v) for k in sorted(buckets)]

# ── Indicators ─────────────────────────────────────────────
def ema(values, period):
    if not values: return []
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def atr_series(highs, lows, closes, period=14):
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    if len(trs) < period: return trs or [0.0]
    a = sum(trs[:period]) / period
    out = [a]
    for t in trs[period:]:
        a = (a*(period-1)+t)/period
        out.append(a)
    return out  # aligns to closes[1:]

def rsi_val(closes, period=14):
    if len(closes) < period+1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d,0.)); losses.append(max(-d,0.))
    ag = sum(gains[:period])/period
    al = sum(losses[:period])/period
    for i in range(period, len(gains)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
    return 100.0 if al==0 else 100-(100/(1+ag/al))

def adx_full(highs, lows, closes, period=14):
    n = len(closes)
    if n < period*3: return 0., 0., 0.
    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        up   = highs[i]-highs[i-1]; down = lows[i-1]-lows[i]
        pdm.append(up   if up>down  and up>0   else 0.)
        mdm.append(down if down>up  and down>0 else 0.)
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
    def wilder(v, p):
        if len(v)<p: return []
        r=[sum(v[:p])]
        for x in v[p:]: r.append(r[-1]-r[-1]/p+x)
        return r
    st=wilder(trs,period); sp=wilder(pdm,period); sm=wilder(mdm,period)
    if not st: return 0.,0.,0.
    pdi=[100*p/t if t else 0 for p,t in zip(sp,st)]
    mdi=[100*m/t if t else 0 for m,t in zip(sm,st)]
    dx =[100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi,mdi)]
    if len(dx)<period: return 0., pdi[-1] if pdi else 0., mdi[-1] if mdi else 0.
    adx_v=sum(dx[:period])/period
    for d in dx[period:]: adx_v=(adx_v*(period-1)+d)/period
    return max(0.,min(100.,adx_v)), pdi[-1], mdi[-1]

# ── Base filter (shared) ───────────────────────────────────
def base_signal(closes, highs, lows):
    """EMA50 slope + EMA9/21 cross. Returns (sig, i) or None."""
    if len(closes) < MIN_BARS: return None
    i = len(closes)-2
    if i < 10: return None
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    sl  = (e50[i]-e50[i-10])/e50[i-10]*100
    up  = sl >  0.05
    dn  = sl < -0.05
    if not up and not dn: return None
    cu = e9[i]>e21[i] and e9[i-1]<=e21[i-1]
    cd = e9[i]<e21[i] and e9[i-1]>=e21[i-1]
    if not cu and not cd: return None
    if up and not cu: return None
    if dn and not cd: return None
    return ('buy' if cu else 'sell'), i

# ── Variant signals ────────────────────────────────────────
def sig_A(closes, highs, lows):
    b = base_signal(closes, highs, lows)
    if not b: return None
    sig, i = b
    adx_v, pdi, mdi = adx_full(highs, lows, closes)
    if adx_v < 22: return None
    if sig=='buy'  and pdi<=mdi: return None
    if sig=='sell' and mdi<=pdi: return None
    return sig

def sig_B(closes, highs, lows):
    b = base_signal(closes, highs, lows)
    if not b: return None
    sig, i = b
    adx_v, _, _ = adx_full(highs, lows, closes)
    return sig if adx_v >= 22 else None

def sig_C(closes, highs, lows, htf_closes, htf_highs, htf_lows):
    b = base_signal(closes, highs, lows)
    if not b: return None
    sig, i = b
    adx_v, _, _ = adx_full(highs, lows, closes)
    if adx_v < 22: return None
    if len(htf_closes) < 60: return None
    # Use bar i-1 on HTF to avoid lookahead (closed bar)
    hi = len(htf_closes)-2
    if hi < 10: return None
    he50 = ema(htf_closes, 50)
    hsl  = (he50[hi]-he50[hi-10])/he50[hi-10]*100
    if sig=='buy'  and hsl <= 0.05: return None
    if sig=='sell' and hsl >= -0.05: return None
    return sig

def sig_D(closes, highs, lows):
    b = base_signal(closes, highs, lows)
    if not b: return None
    sig, i = b
    adx_v, _, _ = adx_full(highs, lows, closes)
    if adx_v < 22: return None
    if i < 20: return None
    sw_lo = min(lows[i-20:i])
    sw_hi = max(highs[i-20:i])
    price = closes[i]
    if sig=='buy'  and price <= sw_lo*1.005: return None
    if sig=='sell' and price >= sw_hi*0.995: return None
    return sig

def sig_E(closes, highs, lows):
    b = base_signal(closes, highs, lows)
    if not b: return None
    sig, i = b
    adx_v, _, _ = adx_full(highs, lows, closes)
    if adx_v < 22: return None
    rv = rsi_val(closes[:i+1])
    if sig=='buy'  and rv <= 52: return None
    if sig=='sell' and rv >= 48: return None
    return sig

def sig_F(closes, highs, lows):
    b = base_signal(closes, highs, lows)
    if not b: return None
    sig, i = b
    adx_v, _, _ = adx_full(highs, lows, closes)
    return sig if adx_v >= 28 else None

SIG_FNS = {'A': sig_A, 'B': sig_B, 'D': sig_D, 'E': sig_E, 'F': sig_F}

# ── Single-symbol backtest ─────────────────────────────────
def backtest_symbol(symbol, candles_15m):
    if len(candles_15m) < MIN_BARS+2:
        return {v: [] for v in VARIANTS}

    ts    = [c[0] for c in candles_15m]
    opens = [c[1] for c in candles_15m]
    highs = [c[2] for c in candles_15m]
    lows  = [c[3] for c in candles_15m]
    closes= [c[4] for c in candles_15m]

    # Build 1h in memory for Variant C
    c1h = build_1h_from_15m(candles_15m)
    htf_ts     = [c[0] for c in c1h]
    htf_closes = [c[4] for c in c1h]
    htf_highs  = [c[2] for c in c1h]
    htf_lows   = [c[3] for c in c1h]

    atr = atr_series(highs, lows, closes)  # aligns to closes[1:]

    result    = {v: [] for v in VARIANTS}
    positions = {v: None for v in VARIANTS}
    equity    = {v: CAPITAL for v in VARIANTS}

    n = len(closes)
    for i in range(MIN_BARS, n-1):
        entry_bar = i+1
        cl_i = closes[:i+1]
        hi_i = highs[:i+1]
        lo_i = lows[:i+1]

        # HTF slice: all 1h bars whose open_ts < ts[i] (no lookahead)
        bar_ms = ts[i]
        htf_end = 0
        for j in range(len(htf_ts)):
            if htf_ts[j] < bar_ms:
                htf_end = j+1
        htf_cl = htf_closes[:htf_end]
        htf_hi = htf_highs[:htf_end]
        htf_lo = htf_lows[:htf_end]

        for vk in VARIANTS:
            pos = positions[vk]

            # ── Exit check ──
            if pos is not None:
                side     = pos['side']
                ep       = pos['entry_price']
                tp_price = pos['tp_price']
                sl_price = pos['sl_price']
                bars_held= i - pos['entry_i']
                exited   = False

                if side=='buy':
                    if lows[i] <= sl_price:
                        exit_p, reason = sl_price, 'sl'; exited=True
                    elif highs[i] >= tp_price:
                        exit_p, reason = tp_price, 'tp'; exited=True
                else:
                    if highs[i] >= sl_price:
                        exit_p, reason = sl_price, 'sl'; exited=True
                    elif lows[i] <= tp_price:
                        exit_p, reason = tp_price, 'tp'; exited=True

                if not exited and bars_held >= MAX_BARS:
                    exit_p, reason = closes[i], 'max_hold'; exited=True

                if exited:
                    gross   = (exit_p-ep)/ep if side=='buy' else (ep-exit_p)/ep
                    net_pct = gross - (FEE+SLIP)*2
                    pnl     = pos['notional'] * net_pct
                    equity[vk] += pnl
                    result[vk].append({
                        'symbol': symbol, 'variant': vk, 'side': side,
                        'entry_ts': ts[pos['entry_i']], 'exit_ts': ts[i],
                        'entry_price': ep, 'exit_price': exit_p,
                        'pnl': pnl, 'reason': reason, 'bars': bars_held,
                    })
                    positions[vk] = None
                    pos = None

            if pos is not None:
                continue  # still in trade

            # ── Entry signal ──
            if vk == 'C':
                sig = sig_C(cl_i, hi_i, lo_i, htf_cl, htf_hi, htf_lo)
            else:
                sig = SIG_FNS[vk](cl_i, hi_i, lo_i)
            if sig is None:
                continue

            ep_raw = opens[entry_bar]
            ep     = ep_raw*(1+FEE+SLIP) if sig=='buy' else ep_raw*(1-FEE-SLIP)
            margin   = equity[vk] * MARGIN_PCT
            notional = margin * LEVERAGE

            cfg = VARIANTS[vk]
            if vk == 'B':
                atr_idx = max(0, i-1)
                atr_val = atr[atr_idx] if atr_idx < len(atr) else closes[i]*0.005
                tp_dist = atr_val * cfg['atr_mult']
                tp_price = ep+tp_dist if sig=='buy' else ep-tp_dist
                sl_price = ep*(1-cfg['sl']) if sig=='buy' else ep*(1+cfg['sl'])
            else:
                tp_p = cfg['tp']; sl_p = cfg['sl']
                tp_price = ep*(1+tp_p) if sig=='buy' else ep*(1-tp_p)
                sl_price = ep*(1-sl_p) if sig=='buy' else ep*(1+sl_p)

            positions[vk] = {
                'side': sig, 'entry_i': entry_bar,
                'entry_price': ep, 'tp_price': tp_price,
                'sl_price': sl_price, 'notional': notional,
            }

    # Close open positions at end
    for vk, pos in positions.items():
        if pos is None: continue
        ep    = pos['entry_price']
        exit_p= closes[-1]
        side  = pos['side']
        gross = (exit_p-ep)/ep if side=='buy' else (ep-exit_p)/ep
        pnl   = pos['notional'] * (gross-(FEE+SLIP)*2)
        equity[vk] += pnl
        result[vk].append({
            'symbol': symbol, 'variant': vk, 'side': side,
            'entry_ts': ts[pos['entry_i']], 'exit_ts': ts[-1],
            'entry_price': ep, 'exit_price': exit_p,
            'pnl': pnl, 'reason': 'end_of_data',
            'bars': len(closes)-1-pos['entry_i'],
        })
    return result

# ── Stats ──────────────────────────────────────────────────
def calc_stats(trades):
    if not trades:
        return {'total':0,'win_rate':0.,'profit_factor':0.,'net_pnl':0.,
                'max_drawdown':0.,'avg_win':0.,'avg_loss':0.,'expectancy':0.,
                'longs':0,'shorts':0,'monthly':{},'per_coin':{}}
    wins   = [t['pnl'] for t in trades if t['pnl']>0]
    losses = [t['pnl'] for t in trades if t['pnl']<=0]
    gp, gl = sum(wins), abs(sum(losses))
    pf     = gp/gl if gl else float('inf')
    eq=0.; peak=0.; mdd=0.
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        eq+=t['pnl']
        if eq>peak: peak=eq
        if peak-eq>mdd: mdd=peak-eq
    monthly={}
    for t in trades:
        dt  = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        if key not in monthly: monthly[key]={'pnl':0.,'n':0,'w':0}
        monthly[key]['pnl']+=t['pnl']; monthly[key]['n']+=1
        if t['pnl']>0: monthly[key]['w']+=1
    per_coin={}
    for t in trades:
        s=t['symbol']
        if s not in per_coin: per_coin[s]={'pnl':0.,'n':0,'w':0,'wr':0.}
        per_coin[s]['pnl']+=t['pnl']; per_coin[s]['n']+=1
        if t['pnl']>0: per_coin[s]['w']+=1
    for v in per_coin.values():
        v['wr']=round(v['w']/v['n']*100,1) if v['n'] else 0.
    return {
        'total': len(trades),
        'win_rate': round(len(wins)/len(trades)*100,2),
        'profit_factor': round(pf,4),
        'net_pnl': round(sum(t['pnl'] for t in trades),2),
        'max_drawdown': round(mdd,2),
        'avg_win': round(sum(wins)/len(wins) if wins else 0.,2),
        'avg_loss': round(sum(losses)/len(losses) if losses else 0.,2),
        'expectancy': round((gp-gl)/len(trades),2),
        'longs': sum(1 for t in trades if t['side']=='buy'),
        'shorts': sum(1 for t in trades if t['side']=='sell'),
        'monthly': monthly, 'per_coin': per_coin,
    }

# ── Shard runner ───────────────────────────────────────────
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} coins: {symbols}", flush=True)
    t0 = time.time()

    # Parallel fetch (each symbol itself fetches months in parallel)
    data_15m = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {ex.submit(fetch_symbol_parallel, sym): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                data_15m[sym] = fut.result()
                print(f"  [Shard {shard_idx}] fetched {sym}: {len(data_15m[sym])} bars", flush=True)
            except Exception as e:
                print(f"  [Shard {shard_idx}] fetch error {sym}: {e}", flush=True)
                data_15m[sym] = []

    with_data=[s for s in symbols if len(data_15m.get(s,[]))>=MIN_BARS+2]
    no_data  =[s for s in symbols if s not in with_data]
    if not with_data:
        print(f"[Shard {shard_idx}] No data — likely geo-block. Abort.", flush=True)
        sys.exit(1)

    # Parallel backtest
    all_trades = {v: [] for v in VARIANTS}
    with ThreadPoolExecutor(max_workers=BACKTEST_WORKERS) as ex:
        futs = {ex.submit(backtest_symbol, sym, data_15m[sym]): sym for sym in with_data}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                res = fut.result()
                for vk, trades in res.items():
                    all_trades[vk].extend(trades)
            except Exception as e:
                print(f"  [Shard {shard_idx}] backtest error {sym}: {e}", flush=True)

    var_stats = {vk: calc_stats(all_trades[vk]) for vk in VARIANTS}
    all_flat  = [t for trades in all_trades.values() for t in trades]

    out = {
        'shard': shard_idx, 'symbols': symbols,
        'with_data': with_data, 'no_data': no_data,
        'trades': all_flat,
        'variant_stats': var_stats,
        'elapsed': round(time.time()-t0, 1),
    }
    with open(f'shard_{shard_idx}.json','w') as f:
        json.dump(out, f)

    for vk in VARIANTS:
        st=var_stats[vk]
        print(f"  [Shard {shard_idx}] {vk}: {st['total']} trades | "
              f"PF {st['profit_factor']} | WR {st['win_rate']}% | PnL ${st['net_pnl']}", flush=True)
    print(f"[Shard {shard_idx}] done {out['elapsed']}s | {len(with_data)}/{len(symbols)} coins", flush=True)

# ── Merge ──────────────────────────────────────────────────
def merge_shards():
    all_trades={v:[] for v in VARIANTS}
    with_data=[]; no_data=[]; all_syms=[]; elapsed=0.
    for idx in range(NUM_SHARDS):
        try:
            with open(f'shard_{idx}.json') as f:
                sh=json.load(f)
        except FileNotFoundError:
            print(f"WARNING: shard_{idx}.json missing — skipped", flush=True)
            continue
        all_syms.extend(sh.get('symbols',[]))
        with_data.extend(sh.get('with_data',[]))
        no_data.extend(sh.get('no_data',[]))
        elapsed+=sh.get('elapsed',0.)
        for t in sh.get('trades',[]):
            all_trades[t['variant']].append(t)

    var_stats={vk: calc_stats(all_trades[vk]) for vk in VARIANTS}

    report={
        'period':    f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe': TIMEFRAME, 'capital': CAPITAL,
        'leverage':  LEVERAGE, 'margin_pct': MARGIN_PCT*100,
        'symbols_attempted': len(set(all_syms)),
        'symbols_with_data': len(set(with_data)),
        'total_elapsed_s':   round(elapsed,1),
        'variants': {vk:{'config':VARIANTS[vk],'stats':var_stats[vk]} for vk in VARIANTS},
    }
    with open('backtest_report.json','w') as f:
        json.dump(report, f, indent=2)

    lines=[]
    lines.append("="*70)
    lines.append("G MAX — 6-VARIANT BACKTEST SUMMARY")
    lines.append("="*70)
    lines.append(f"Period   : {report['period']}")
    lines.append(f"TF: {TIMEFRAME}  Leverage: {LEVERAGE}x  Margin: {MARGIN_PCT*100:.0f}% equity/trade")
    lines.append(f"Capital  : ${CAPITAL:,.0f}  Fee: {FEE*100:.3f}%/side  Slip: {SLIP*100:.3f}%/side")
    lines.append(f"Coins    : {len(set(with_data))} with data / {len(set(all_syms))} attempted")
    lines.append("")
    lines.append(f"{'VAR':<4} {'NAME':<22} {'TP':>7} {'SL':>6} {'TRADES':>7} {'WR%':>7} {'PF':>7} {'NET PNL':>10} {'MAX DD':>10} {'OK?':>5}")
    lines.append("-"*80)
    for vk in VARIANTS:
        cfg=VARIANTS[vk]; st=var_stats[vk]
        tp_s = f"{cfg['tp']*100:.1f}%" if cfg.get('tp') else 'ATR×2.5'
        sl_s = f"{cfg['sl']*100:.1f}%"
        ok   = '✅' if st['profit_factor']>=1.5 and st['win_rate']>=42 else '❌'
        lines.append(f"{vk:<4} {cfg['name']:<22} {tp_s:>7} {sl_s:>6} "
                     f"{st['total']:>7} {st['win_rate']:>7.1f} {st['profit_factor']:>7.4f} "
                     f"${st['net_pnl']:>9,.2f} ${st['max_drawdown']:>9,.2f} {ok:>5}")
    lines.append("-"*80)
    lines.append("✅ = PF>=1.5 AND WR>=42%")
    lines.append("")

    for vk in VARIANTS:
        cfg=VARIANTS[vk]; st=var_stats[vk]
        lines.append("="*70)
        lines.append(f"VARIANT {vk} — {cfg['name'].upper()}")
        lines.append(f"  Trades:{st['total']} WR:{st['win_rate']}% PF:{st['profit_factor']} PnL:${st['net_pnl']:,.2f}")
        lines.append(f"  AvgWin:${st['avg_win']} AvgLoss:${st['avg_loss']} Expectancy:${st['expectancy']}")
        lines.append(f"  MaxDD:${st['max_drawdown']:,.2f} Longs:{st['longs']} Shorts:{st['shorts']}")
        lines.append("")
        coin_rows=sorted(st['per_coin'].items(),key=lambda x:x[1]['pnl'],reverse=True)
        lines.append(f"  TOP COINS  {'COIN':<22} {'N':>6} {'WR%':>7} {'PNL':>10}")
        lines.append(f"  {'-'*48}")
        for sym,d in coin_rows[:25]:
            lines.append(f"  {sym:<26} {d['n']:>6} {d['wr']:>7.1f} ${d['pnl']:>9,.2f}")
        lines.append("")
        lines.append("  MONTHLY:")
        for mo in sorted(st['monthly']):
            m=st['monthly'][mo]
            wr_m=round(m['w']/m['n']*100,1) if m['n'] else 0.
            lines.append(f"    {mo}: ${m['pnl']:>8,.2f}  ({m['n']} trades WR {wr_m}%)")
        lines.append("")

    lines.append("="*70)
    lines.append("CROSS-VARIANT: coins positive in 3+ variants")
    coin_scores={}
    for vk in VARIANTS:
        for sym,d in var_stats[vk]['per_coin'].items():
            if sym not in coin_scores: coin_scores[sym]=0
            if d['pnl']>0: coin_scores[sym]+=1
    for sym,cnt in sorted(coin_scores.items(),key=lambda x:x[1],reverse=True):
        if cnt>=3:
            lines.append(f"  {sym:<26} positive in {cnt}/6 variants")
    lines.append(f"\nTotal shard time: ~{elapsed/60:.1f} min across {NUM_SHARDS} shards")
    lines.append("="*70)

    txt='\n'.join(lines)
    with open('backtest_summary.txt','w') as f: f.write(txt)
    print(txt, flush=True)
    print("\nDone: backtest_report.json  backtest_summary.txt", flush=True)

# ── Entry ──────────────────────────────────────────────────
if __name__=='__main__':
    if len(sys.argv)<2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg=sys.argv[1]
    if arg=='merge': merge_shards()
    else: run_shard(int(arg))

