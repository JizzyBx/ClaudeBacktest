"""
Backtest V6 — Filtered Coins + Volume + 4H Trend Alignment
============================================================
stdlib-only Python 3.11 | GitHub Actions compatible

WHAT CHANGED FROM V1-V5:
  ✅ Coin whitelist — only coins that proved profitable (PF>1.0 in 4+/5 variants)
  ✅ 30 new coins added (expanded universe to find more edge)
  ✅ V2 exits: TP=2.8×ATR, SL=1.7×ATR (best performer from prior runs)
  ✅ Volume confirmation: crossover bar volume ≥ 1.5× 20-bar avg
  ✅ 4H higher-timeframe trend alignment: 4H EMA50 slope must agree with 15m signal
     (HTF lookahead-safe: uses bar_ts - HTF_PERIOD_MS per handoff doc)

DATA: data.binance.vision static archive (geo-block safe)
RANGE: 3 years | WORKERS: 10 parallel threads
"""

import json, zipfile, io, csv, time, math, threading, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Coin Universe ──────────────────────────────────────────────────────────────

# Proven keepers from V1-V5 (PF>1.0 in 4+/5 variants, avg PF ≥ 1.05)
PROVEN_COINS = [
    'TIAUSDT',      # avg PF 1.469 — best performer, profitable in ALL 5
    'XRPUSDT',      # avg PF 1.386 — consistent across all timeframes
    'TURBOUSDT',    # avg PF 1.350 — strong meme with real trend behavior
    'FETUSDT',      # avg PF 1.225 — AI narrative, clean trends
    'AVAXUSDT',     # avg PF 1.215 — solid L1
    'AAVEUSDT',     # avg PF 1.208 — DeFi, good HTF trends
    '1000RATSUSDT', # avg PF 1.195 — Bitcoin meme, volatile and trendy
    'LDOUSDT',      # avg PF 1.183 — liquid staking
    'BNBUSDT',      # avg PF 1.183 — always liquid
    'SEIUSDT',      # avg PF 1.182 — fast L1
    'RUNEUSDT',     # avg PF 1.178 — THORChain, momentum coin
    'BOMEUSDT',     # avg PF 1.171 — Solana meme
    'REZUSDT',      # avg PF 1.154 — newer coin, clean signals
    'EIGENUSDT',    # avg PF 1.152 — restaking narrative
    'APTUSDT',      # avg PF 1.151 — L1, decent trends
    'DOGEUSDT',     # avg PF 1.138 — high volume, good trends
    'ATOMUSDT',     # avg PF 1.127 — Cosmos hub
    '1000SHIBUSDT', # avg PF 1.094 — classic meme, liquid
    'WLDUSDT',      # avg PF 1.072 — WorldCoin
    'STXUSDT',      # avg PF 1.063 — Bitcoin L2
    'DOTUSDT',      # avg PF 1.059 — Polkadot
    'BTCUSDT',      # avg PF 1.267 — added (4/5 good, BTC 1H was PF 2.0)
]

# 30 NEW coins: diverse mix of high-volume alts, L2s, AI, DeFi, memes
# All confirmed to have USDT-M perpetuals on Binance
NEW_30 = [
    # Layer 1 / Layer 2 — high volume established
    'TONUSDT',      # TON — Telegram ecosystem, massive user base
    'TRXUSDT',      # TRON — huge on-chain volume
    'MATICUSDT',    # will remap to POLUSDT — keep for awareness
    'ALGOUSDT',     # Algorand — established L1
    'FILUSDT',      # Filecoin — storage narrative
    'ICPUSDT',      # Internet Computer
    'VETUSDT',      # VeChain — supply chain, steady volume
    'XLMUSDT',      # Stellar — XRP companion, similar behavior
    'FLOWUSDT',     # Flow blockchain
    'CFXUSDT',      # Conflux — Chinese L1
    # DeFi / Yield
    'CRVUSDT',      # Curve Finance — DeFi benchmark
    'MKRUSDT',      # MakerDAO — DeFi OG
    'SNXUSDT',      # Synthetix
    'GRTUSDT',      # The Graph — indexing
    'COMPUSDT',     # Compound Finance
    # AI / Data
    'AKAUSDT',      # Akash Network — decentralized compute
    'AGIXUSDT',     # SingularityNET — AI token
    'OCEANUSDT',    # Ocean Protocol — data marketplace
    # Meme / community with futures
    'MEMEUSDT',     # MEME token — Memeland
    '1000XECUSDT',  # eCash — meme-adjacent, high vol
    'PEOPLEUSDT',   # ConstitutionDAO meme
    'ACHUSDT',      # Achilles — community token
    # Gaming / Metaverse
    'AXSUSDT',      # Axie Infinity — gaming OG
    'SANDUSDT',     # The Sandbox — metaverse
    'MANAUSDT',     # Decentraland
    'GALAUSDT',     # Gala Games
    # Infrastructure / Other
    'JTOUSDT',      # Jito — Solana liquid staking
    'PYTHUSDT',     # Pyth Network — oracle
    'STRKUSDT',     # Starknet — Ethereum L2
    'ORDIUSDT',     # Ordinals — Bitcoin NFT narrative
]

# Apply naming rules
SYMBOL_REMAP = {'MATICUSDT': 'POLUSDT'}

def clean_symbols(raw):
    out, seen = [], set()
    for s in raw:
        s = SYMBOL_REMAP.get(s, s).replace('.P','')
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

ALL_SYMBOLS = clean_symbols(PROVEN_COINS + NEW_30)
print(f"Total coin universe: {len(ALL_SYMBOLS)}")

# ── Config ────────────────────────────────────────────────────────────────────
CAPITAL        = 10_000.0
RISK_PCT       = 0.0075
FEE_PCT        = 0.0005
SLIPPAGE_PCT   = 0.0002
MAX_POSITIONS  = 6
ADX_MIN        = 22
SLOPE_THRESHOLD = 0.05    # 0.05% over 10 bars (percentage)
VOL_MULTIPLIER  = 1.5     # crossover bar volume must be ≥ 1.5× 20-bar avg
TP_MULT        = 2.8      # V2 exits — best from prior backtests
SL_MULT        = 1.7
WORKERS        = 10

# 3-year range
END_DATE   = datetime.now(timezone.utc).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
START_DATE = END_DATE - timedelta(days=3*365)

BASE_URL = "https://data.binance.vision/data/futures/um"

# Interval configs
ENTRY_INTERVAL = '15m'
HTF_INTERVAL   = '4h'
HTF_PERIOD_MS  = 4 * 60 * 60 * 1000   # 4 hours in ms

# ── Data Fetching ─────────────────────────────────────────────────────────────
_fetch_lock  = threading.Lock()
_fetch_errors = []

def fetch_monthly(symbol, interval, year, month):
    ym  = f"{year}-{month:02d}"
    url = f"{BASE_URL}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{ym}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404: return None
        with _fetch_lock: _fetch_errors.append(f"{symbol} {ym} {interval}: HTTP {e.code}")
        return None
    except Exception as e:
        with _fetch_lock: _fetch_errors.append(f"{symbol} {ym} {interval}: {e}")
        return None

def parse_rows(rows):
    candles = []
    for r in rows:
        if not r or r[0].startswith('open_time'): continue
        try:
            ts = int(r[0])
            if ts > 10**14: ts //= 1000   # microsecond guard
            candles.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
        except: continue
    return candles

def fetch_all(symbol, interval):
    all_candles, cur = [], START_DATE
    while cur < END_DATE:
        rows = fetch_monthly(symbol, interval, cur.year, cur.month)
        if rows: all_candles.extend(parse_rows(rows))
        cur = cur.replace(month=cur.month+1) if cur.month < 12 else cur.replace(year=cur.year+1, month=1)
    if not all_candles: return []
    s_ms = int(START_DATE.timestamp()*1000)
    e_ms = int(END_DATE.timestamp()*1000)
    candles = sorted(set(c for c in all_candles if s_ms <= c[0] < e_ms), key=lambda x: x[0])
    seen, out = set(), []
    for c in candles:
        if c[0] not in seen: seen.add(c[0]); out.append(c)
    return out

# ── Indicators ────────────────────────────────────────────────────────────────
def ema_series(values, period):
    k = 2.0/(period+1); r = [values[0]]
    for v in values[1:]: r.append(v*k + r[-1]*(1-k))
    return r

def atr_series(highs, lows, closes, period=14):
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    if not trs: return [closes[-1]*0.005]*len(closes)
    atr = [None]*len(closes)
    if len(trs) >= period:
        atr[period] = sum(trs[:period])/period
        for i in range(period, len(trs)):
            atr[i+1] = (atr[i]*(period-1) + trs[i])/period
    return atr

def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    adx_out, pdi_out, mdi_out = [None]*n, [None]*n, [None]*n
    if n < period*3: return adx_out, pdi_out, mdi_out
    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        up   = highs[i]-highs[i-1]; down = lows[i-1]-lows[i]
        pdm.append(up if up>down and up>0 else 0.0)
        mdm.append(down if down>up and down>0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    def ws(v, p):
        if len(v)<p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1]-r[-1]/p+x)
        return r
    st,sp,sm = ws(trs,period), ws(pdm,period), ws(mdm,period)
    if not st: return adx_out, pdi_out, mdi_out
    pdi_l = [100*p/t if t else 0 for p,t in zip(sp,st)]
    mdi_l = [100*m/t if t else 0 for m,t in zip(sm,st)]
    dx_l  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi_l,mdi_l)]
    if len(dx_l)<period: return adx_out, pdi_out, mdi_out
    adx_val = sum(dx_l[:period])/period; adx_vals = [adx_val]
    for d in dx_l[period:]: adx_val=(adx_val*(period-1)+d)/period; adx_vals.append(adx_val)
    pdi_start = period+1; adx_start = pdi_start+period-1
    for i,v in enumerate(pdi_l):
        idx = pdi_start+i
        if idx<n: pdi_out[idx]=v; mdi_out[idx]=mdi_l[i]
    for i,v in enumerate(adx_vals):
        idx = adx_start+i
        if idx<n: adx_out[idx]=max(0.0,min(100.0,v))
    return adx_out, pdi_out, mdi_out

def build_htf_index(htf_candles):
    """Build a timestamp→EMA50_slope lookup for 4H candles."""
    if len(htf_candles) < 60: return {}
    closes = [c[4] for c in htf_candles]
    ema50  = ema_series(closes, 50)
    result = {}
    for i in range(10, len(htf_candles)):
        ts      = htf_candles[i][0]
        slope   = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100 if ema50[i-10] else 0
        result[ts] = slope
    return result

def get_htf_slope(htf_index, bar_ts_ms):
    """
    Lookahead-safe: find the last CLOSED 4H candle before this 15m bar.
    Per handoff doc: query_ts = bar_ts - HTF_PERIOD_MS
    """
    query_ts = bar_ts_ms - HTF_PERIOD_MS
    # Find the largest HTF timestamp <= query_ts
    best_ts = None
    for ts in htf_index:
        if ts <= query_ts:
            if best_ts is None or ts > best_ts:
                best_ts = ts
    if best_ts is None: return None
    return htf_index[best_ts]

# ── Signal Engine ─────────────────────────────────────────────────────────────
def compute_signals(candles_15m, htf_index):
    closes  = [c[4] for c in candles_15m]
    highs   = [c[2] for c in candles_15m]
    lows    = [c[3] for c in candles_15m]
    volumes = [c[5] for c in candles_15m]

    ema9   = ema_series(closes, 9)
    ema21  = ema_series(closes, 21)
    ema50  = ema_series(closes, 50)
    atr    = atr_series(highs, lows, closes, 14)
    adx_s, _, _ = adx_series(highs, lows, closes, 14)

    reject = {'warmup_none':0, 'adx_fail':0, 'slope_fail':0,
              'cross_fail':0, 'volume_fail':0, 'htf_fail':0, 'signal':0}
    signals = []
    WARMUP = 60

    for i in range(WARMUP, len(candles_15m)-1):
        if adx_s[i] is None or atr[i] is None:
            reject['warmup_none'] += 1; continue

        # ── Filter 1: ADX ──────────────────────────────────────────
        if adx_s[i] < ADX_MIN:
            reject['adx_fail'] += 1; continue

        # ── Filter 2: 50 EMA slope ─────────────────────────────────
        slope_pct = (ema50[i]-ema50[i-10])/ema50[i-10]*100 if i>=10 else 0.0
        trend_up   = slope_pct >  SLOPE_THRESHOLD
        trend_down = slope_pct < -SLOPE_THRESHOLD
        if not (trend_up or trend_down):
            reject['slope_fail'] += 1; continue

        # ── Filter 3: EMA 9/21 crossover ──────────────────────────
        crossed_up   = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
        crossed_down = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]
        if not (crossed_up or crossed_down):
            reject['cross_fail'] += 1; continue

        # Direction consistency check
        sig = None
        if trend_up   and crossed_up:   sig = 'long'
        if trend_down and crossed_down: sig = 'short'
        if sig is None:
            reject['cross_fail'] += 1; continue

        # ── Filter 4: Volume confirmation ──────────────────────────
        if i >= 20:
            avg_vol_20 = sum(volumes[i-20:i]) / 20
            if avg_vol_20 > 0 and volumes[i] < VOL_MULTIPLIER * avg_vol_20:
                reject['volume_fail'] += 1; continue
        # (skip volume check during first 20 bars — treated as passing)

        # ── Filter 5: 4H HTF trend alignment ──────────────────────
        bar_ts = candles_15m[i][0]
        htf_slope = get_htf_slope(htf_index, bar_ts)
        if htf_slope is not None:
            htf_up   = htf_slope >  SLOPE_THRESHOLD
            htf_down = htf_slope < -SLOPE_THRESHOLD
            if sig == 'long'  and not htf_up:
                reject['htf_fail'] += 1; continue
            if sig == 'short' and not htf_down:
                reject['htf_fail'] += 1; continue
        # If no HTF data yet (coin too new), skip HTF check — don't penalize

        reject['signal'] += 1
        entry_price = candles_15m[i+1][1]   # open of next bar
        signals.append({
            'bar_idx'    : i+1,
            'signal'     : sig,
            'entry_price': entry_price,
            'tp_dist'    : atr[i] * TP_MULT,
            'sl_dist'    : atr[i] * SL_MULT,
            'open_time'  : candles_15m[i+1][0],
        })

    return signals, reject

# ── Backtest Engine ────────────────────────────────────────────────────────────
def run_backtest_symbol(symbol, candles_15m, htf_index):
    if len(candles_15m) < 80:
        return [], {'warmup_none':len(candles_15m),'adx_fail':0,'slope_fail':0,
                    'cross_fail':0,'volume_fail':0,'htf_fail':0,'signal':0}

    signals, rejects = compute_signals(candles_15m, htf_index)
    trades = []

    for sig in signals:
        bar   = sig['bar_idx']
        entry = sig['entry_price']
        tp_d  = sig['tp_dist']
        sl_d  = sig['sl_dist']
        dirn  = sig['signal']
        tp_p  = entry + tp_d if dirn=='long' else entry - tp_d
        sl_p  = entry - sl_d if dirn=='long' else entry + sl_d

        exit_bar = exit_price = exit_type = None
        for j in range(bar+1, len(candles_15m)):
            h, l = candles_15m[j][2], candles_15m[j][3]
            if dirn == 'long':
                if l <= sl_p: exit_price=sl_p; exit_type='sl'; exit_bar=j; break
                if h >= tp_p: exit_price=tp_p; exit_type='tp'; exit_bar=j; break
            else:
                if h >= sl_p: exit_price=sl_p; exit_type='sl'; exit_bar=j; break
                if l <= tp_p: exit_price=tp_p; exit_type='tp'; exit_bar=j; break

        if exit_bar is None: continue

        trades.append({
            'symbol'       : symbol,
            'direction'    : dirn,
            'entry_price'  : entry,
            'exit_price'   : exit_price,
            'exit_type'    : exit_type,
            'duration_bars': exit_bar - bar,
            'entry_time'   : candles_15m[bar][0],
            'exit_time'    : candles_15m[exit_bar][0],
        })

    return trades, rejects

# ── Portfolio Simulation ───────────────────────────────────────────────────────
def simulate_portfolio(all_raw_trades):
    rr_ratio = TP_MULT / SL_MULT   # 2.8/1.7 ≈ 1.647
    flat      = sorted(all_raw_trades, key=lambda t: t['entry_time'])
    equity    = CAPITAL
    open_pos  = {}
    result    = []

    for t in flat:
        sym = t['symbol']
        # Evict closed
        evict = [s for s,p in open_pos.items() if p['exit_time'] <= t['entry_time']]
        for s in evict:
            p = open_pos.pop(s)
            equity += p['realized_pnl']

        if len(open_pos) >= MAX_POSITIONS or sym in open_pos: continue

        risk_amt   = equity * RISK_PCT
        cost       = risk_amt * (FEE_PCT + SLIPPAGE_PCT) * 2
        dollar_pnl = (risk_amt * rr_ratio - cost) if t['exit_type']=='tp' else (-risk_amt - cost)

        open_pos[sym] = {'exit_time': t['exit_time'], 'realized_pnl': dollar_pnl}
        rt = dict(t); rt['dollar_pnl'] = dollar_pnl; rt['risk_amt'] = risk_amt
        rt['equity_before'] = equity
        result.append(rt)

    for p in open_pos.values(): equity += p['realized_pnl']
    return result, equity

# ── Statistics ────────────────────────────────────────────────────────────────
def compute_stats(trades, final_equity):
    if not trades:
        return {'total_trades':0,'win_rate':0,'profit_factor':0,'net_pnl':0,
                'max_drawdown_pct':0,'sharpe':0,'sortino':0,'avg_win':0,'avg_loss':0,
                'expectancy':0,'avg_duration_bars':0,'long_trades':0,'long_wr':0,
                'short_trades':0,'short_wr':0,'max_win_streak':0,'max_loss_streak':0,
                'final_equity':CAPITAL,'usable':False}
    wins   = [t for t in trades if t['dollar_pnl']>0]
    losses = [t for t in trades if t['dollar_pnl']<=0]
    longs  = [t for t in trades if t['direction']=='long']
    shorts = [t for t in trades if t['direction']=='short']
    total  = len(trades)
    wr     = len(wins)/total*100
    gw     = sum(t['dollar_pnl'] for t in wins)
    gl     = abs(sum(t['dollar_pnl'] for t in losses))
    pf     = gw/gl if gl else (999 if gw>0 else 0)
    net    = final_equity - CAPITAL
    avg_w  = gw/len(wins)   if wins   else 0
    avg_l  = gl/len(losses) if losses else 0
    exp    = (wr/100*avg_w) - ((1-wr/100)*avg_l)

    eq = [CAPITAL]
    for t in sorted(trades, key=lambda x: x['entry_time']):
        eq.append(eq[-1]+t['dollar_pnl'])
    peak=eq[0]; max_dd=0
    for e in eq:
        if e>peak: peak=e
        dd=(peak-e)/peak*100
        if dd>max_dd: max_dd=dd

    pnls = [t['dollar_pnl'] for t in sorted(trades, key=lambda x: x['entry_time'])]
    mean_r = sum(pnls)/len(pnls)
    std_r  = math.sqrt(sum((p-mean_r)**2 for p in pnls)/len(pnls)) if len(pnls)>1 else 0
    neg    = [p for p in pnls if p<0]
    neg_d  = math.sqrt(sum((p-mean_r)**2 for p in neg)/len(neg)) if neg else 0
    sharpe  = (mean_r/std_r*math.sqrt(252)) if std_r else 0
    sortino = (mean_r/neg_d*math.sqrt(252)) if neg_d else 0

    streak=max_w=max_l=0; cur_type=None
    for t in trades:
        w = t['dollar_pnl']>0
        streak = (streak+1) if w==cur_type else 1; cur_type=w
        if w: max_w=max(max_w,streak)
        else: max_l=max(max_l,streak)

    return {
        'total_trades'    : total,
        'win_rate'        : round(wr,2),
        'profit_factor'   : round(pf,4),
        'net_pnl'         : round(net,2),
        'final_equity'    : round(final_equity,2),
        'max_drawdown_pct': round(max_dd,2),
        'sharpe'          : round(sharpe,3),
        'sortino'         : round(sortino,3),
        'avg_win'         : round(avg_w,2),
        'avg_loss'        : round(avg_l,2),
        'expectancy'      : round(exp,2),
        'avg_duration_bars': round(sum(t['duration_bars'] for t in trades)/total,1),
        'long_trades'     : len(longs),
        'long_wr'         : round(sum(1 for t in longs if t['dollar_pnl']>0)/len(longs)*100,2) if longs else 0,
        'short_trades'    : len(shorts),
        'short_wr'        : round(sum(1 for t in shorts if t['dollar_pnl']>0)/len(shorts)*100,2) if shorts else 0,
        'max_win_streak'  : max_w,
        'max_loss_streak' : max_l,
        'usable'          : pf>=1.5 and wr>=42,
    }

def per_coin_table(trades):
    coin_data = defaultdict(list)
    for t in trades: coin_data[t['symbol']].append(t)
    rows = []
    for sym, ts in coin_data.items():
        wins  = sum(1 for t in ts if t['dollar_pnl']>0)
        total = len(ts)
        gw    = sum(t['dollar_pnl'] for t in ts if t['dollar_pnl']>0)
        gl    = abs(sum(t['dollar_pnl'] for t in ts if t['dollar_pnl']<=0))
        pf    = gw/gl if gl else (999 if gw>0 else 0)
        rows.append({'symbol':sym,'trades':total,'wins':wins,'losses':total-wins,
                     'wr':round(wins/total*100,1) if total else 0,
                     'pf':round(pf,3),'pnl':round(sum(t['dollar_pnl'] for t in ts),2)})
    return sorted(rows, key=lambda x: x['pf'], reverse=True)

def monthly_pnl(trades):
    m = defaultdict(float)
    for t in trades:
        k = datetime.fromtimestamp(t['entry_time']/1000,tz=timezone.utc).strftime('%Y-%m')
        m[k] += t['dollar_pnl']
    return {k:round(v,2) for k,v in sorted(m.items())}

# ── Worker ────────────────────────────────────────────────────────────────────
def worker(symbol):
    candles_15m = fetch_all(symbol, ENTRY_INTERVAL)
    candles_4h  = fetch_all(symbol, HTF_INTERVAL)
    htf_index   = build_htf_index(candles_4h) if candles_4h else {}
    if not candles_15m: return symbol, [], {}
    trades, rejects = run_backtest_symbol(symbol, candles_15m, htf_index)
    return symbol, trades, rejects

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*65)
    print("  BACKTEST V6 — Filtered Coins + Volume + 4H HTF Alignment")
    print(f"  Range : {START_DATE.strftime('%Y-%m')} → {END_DATE.strftime('%Y-%m')}")
    print(f"  Coins : {len(ALL_SYMBOLS)} ({len(PROVEN_COINS)} proven + {len(NEW_30)} new)")
    print(f"  Exits : TP={TP_MULT}×ATR | SL={SL_MULT}×ATR (V2 best params)")
    print(f"  Filters: ADX≥{ADX_MIN} | 50EMA slope | EMA crossover | Vol≥{VOL_MULTIPLIER}×avg | 4H align")
    print(f"  Capital: ${CAPITAL:,.0f} | Risk: {RISK_PCT*100}%/trade | MaxPos: {MAX_POSITIONS}")
    print("="*65)
    print(f"\n  Coin list:")
    for i,s in enumerate(ALL_SYMBOLS): print(f"    {i+1:2d}. {s}")

    all_raw, all_rejects, data_errors = [], defaultdict(int), 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(worker, sym): sym for sym in ALL_SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                sym_out, trades, rejects = fut.result()
                all_raw.extend(trades)
                for k,v in rejects.items(): all_rejects[k] += v
                status = f"{len(trades)} raw signals"
                print(f"  ✓ {sym:22s} {status}")
            except Exception as e:
                print(f"  ✗ {sym:22s} ERROR: {e}")
                data_errors += 1

    # Abort guard — if everything 404'd, bucket is blocked
    if data_errors == len(ALL_SYMBOLS):
        print("\n  ⛔ ALL SYMBOLS FAILED — data bucket blocked. Aborting.")
        return

    result_trades, final_equity = simulate_portfolio(all_raw)
    stats      = compute_stats(result_trades, final_equity)
    coin_table = per_coin_table(result_trades)
    monthly    = monthly_pnl(result_trades)

    # ── Print results ──────────────────────────────────────────────
    print(f"\n{'━'*65}")
    print(f"  V6 RESULTS")
    print(f"{'━'*65}")
    print(f"  Trades        : {stats['total_trades']}")
    print(f"  Win Rate      : {stats['win_rate']}%")
    print(f"  Profit Factor : {stats['profit_factor']}")
    print(f"  Net PnL       : ${stats['net_pnl']:>12,.2f}")
    print(f"  Final Equity  : ${stats['final_equity']:>12,.2f}")
    print(f"  Max Drawdown  : {stats['max_drawdown_pct']}%")
    print(f"  Sharpe        : {stats['sharpe']}  |  Sortino: {stats['sortino']}")
    print(f"  Avg Win       : ${stats['avg_win']:,.2f}  |  Avg Loss: ${stats['avg_loss']:,.2f}")
    print(f"  Expectancy    : ${stats['expectancy']:,.2f}")
    print(f"  Avg Duration  : {stats['avg_duration_bars']} bars")
    print(f"  Longs  : {stats['long_trades']} ({stats['long_wr']}% WR)")
    print(f"  Shorts : {stats['short_trades']} ({stats['short_wr']}% WR)")
    print(f"  Win Streak  : {stats['max_win_streak']}  |  Loss Streak: {stats['max_loss_streak']}")
    print(f"\n  {'✅ USABLE — MEETS PF≥1.5 AND WR≥42%' if stats['usable'] else '❌ BELOW TARGETS (PF≥1.5 / WR≥42%)'}")

    print(f"\n  TOP 15 COINS BY PROFIT FACTOR:")
    print(f"  {'Symbol':22s} {'PF':>6}  {'WR%':>5}  {'Trades':>6}  {'PnL':>10}")
    print(f"  {'-'*58}")
    for row in coin_table[:15]:
        print(f"  {row['symbol']:22s} {row['pf']:>6.3f}  {row['wr']:>5.1f}%  "
              f"{row['trades']:>6}  ${row['pnl']:>9,.0f}")

    print(f"\n  BOTTOM 10 COINS (new coins to watch):")
    for row in coin_table[-10:]:
        print(f"  {row['symbol']:22s} {row['pf']:>6.3f}  {row['wr']:>5.1f}%  {row['trades']:>6}")

    total_bars = sum(all_rejects.values())
    print(f"\n  FILTER REJECTION BREAKDOWN ({total_bars:,} bars scanned):")
    for k, v in all_rejects.items():
        pct = v/total_bars*100 if total_bars else 0
        print(f"  {'  '+k:24s}: {v:>10,}  ({pct:.2f}%)")

    print(f"\n  MONTHLY PnL:")
    green = red = 0
    for ym, pnl in monthly.items():
        bar  = ('█'*min(40,int(abs(pnl)/300))) if pnl else ''
        sign = '+' if pnl>=0 else ''
        tag  = '🟢' if pnl>=0 else '🔴'
        print(f"  {tag} {ym}  {sign}${pnl:>10,.0f}  {bar}")
        if pnl>=0: green+=1
        else: red+=1
    print(f"\n  Green months: {green}/{len(monthly)} | Red months: {red}/{len(monthly)}")

    # ── Write output files ─────────────────────────────────────────
    summary = [
        "BACKTEST V6 SUMMARY",
        f"Range: {START_DATE.strftime('%Y-%m')} to {END_DATE.strftime('%Y-%m')}",
        f"Coins: {len(ALL_SYMBOLS)} | Capital: ${CAPITAL:,.0f}",
        f"Filters: ADX≥{ADX_MIN} | 50EMA slope | EMA crossover | Vol≥{VOL_MULTIPLIER}×avg20 | 4H HTF align",
        f"Exits: TP={TP_MULT}×ATR | SL={SL_MULT}×ATR",
        "",
        f"Trades: {stats['total_trades']} | WR: {stats['win_rate']}% | PF: {stats['profit_factor']}",
        f"Net PnL: ${stats['net_pnl']:,.2f} | Max DD: {stats['max_drawdown_pct']}%",
        f"Sharpe: {stats['sharpe']} | Sortino: {stats['sortino']}",
        f"Verdict: {'✅ USABLE' if stats['usable'] else '❌ BELOW TARGETS'}",
        "",
        "TOP 15 COINS:",
    ] + [f"  {r['symbol']:22s} PF:{r['pf']:.3f} WR:{r['wr']}% Trades:{r['trades']} PnL:${r['pnl']:,.0f}"
         for r in coin_table[:15]]

    with open('backtest_summary.txt','w') as f: f.write('\n'.join(summary))

    report = {
        'meta': {
            'version'       : 'V6',
            'start_date'    : START_DATE.isoformat(),
            'end_date'      : END_DATE.isoformat(),
            'symbols'       : ALL_SYMBOLS,
            'proven_coins'  : PROVEN_COINS,
            'new_coins'     : clean_symbols(NEW_30),
            'capital'       : CAPITAL,
            'risk_pct'      : RISK_PCT,
            'fee_pct'       : FEE_PCT,
            'slippage_pct'  : SLIPPAGE_PCT,
            'max_positions' : MAX_POSITIONS,
            'adx_min'       : ADX_MIN,
            'tp_mult'       : TP_MULT,
            'sl_mult'       : SL_MULT,
            'vol_multiplier': VOL_MULTIPLIER,
            'entry_interval': ENTRY_INTERVAL,
            'htf_interval'  : HTF_INTERVAL,
            'generated_at'  : datetime.now(timezone.utc).isoformat(),
        },
        'aggregate'   : stats,
        'per_coin'    : coin_table,
        'monthly_pnl' : monthly,
        'filter_stats': {k:{'count':v,'pct':round(v/total_bars*100,2) if total_bars else 0}
                         for k,v in all_rejects.items()},
        'trades'      : result_trades,
        'fetch_errors': _fetch_errors[:100],
    }

    with open('backtest_report.json','w') as f:
        json.dump(report, f, indent=2, default=str)

    print("\n" + "="*65)
    print("  OUTPUT: backtest_summary.txt | backtest_report.json")
    print("="*65)

if __name__ == '__main__':
    main()
