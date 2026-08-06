"""
GMax V1 — Strategy G Backtest (Matrix / Shard Mode)
=====================================================
Run via GitHub Actions matrix: 8 parallel jobs, each handles ~66 coins.
A merge job combines all shard JSONs into the final report.

Strategy : Variant G · 15m
  EMA50 slope filter (10-bar, ±0.05%)
  EMA9/21 crossover (direction must match slope)
  ADX(14) >= 22
  TP: 3.0% | SL: 15.0% | Max hold: 960 bars (10 days)

Usage:
  python backtest.py <shard_index>   # 0..7  — run one shard
  python backtest.py merge           # merge all shard JSONs
"""

import csv, io, json, os, sys, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# ─── CONFIG ────────────────────────────────────────────────────────────────

ALL_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BCHUSDT","XRPUSDT","LTCUSDT","TRXUSDT","ETCUSDT",
    "LINKUSDT","XLMUSDT","ADAUSDT","XMRUSDT","DASHUSDT","ZECUSDT","XTZUSDT",
    "BNBUSDT","ATOMUSDT","ONTUSDT","IOTAUSDT","BATUSDT","VETUSDT","NEOUSDT",
    "QTUMUSDT","IOSTUSDT","THETAUSDT","ALGOUSDT","ZILUSDT","KNCUSDT","ZRXUSDT",
    "COMPUSDT","DOGEUSDT","KAVAUSDT","BANDUSDT","RLCUSDT","SNXUSDT","DOTUSDT",
    "YFIUSDT","CRVUSDT","TRBUSDT","RUNEUSDT","SUSHIUSDT","EGLDUSDT","SOLUSDT",
    "ICXUSDT","STORJUSDT","UNIUSDT","AVAXUSDT","ENJUSDT","KSMUSDT","NEARUSDT",
    "AAVEUSDT","FILUSDT","RSRUSDT","BELUSDT","AXSUSDT","ZENUSDT","SKLUSDT",
    "GRTUSDT","1INCHUSDT","CHZUSDT","SANDUSDT","ANKRUSDT","RVNUSDT","SFPUSDT",
    "COTIUSDT","CHRUSDT","MANAUSDT","ALICEUSDT","HBARUSDT","ONEUSDT","CELRUSDT",
    "HOTUSDT","MTLUSDT","OGNUSDT","1000SHIBUSDT","GTCUSDT","BTCDOMUSDT",
    "IOTXUSDT","C98USDT","MASKUSDT","DYDXUSDT","1000XECUSDT","GALAUSDT",
    "CELOUSDT","ARUSDT","ARPAUSDT","CTSIUSDT","LPTUSDT","ENSUSDT","PEOPLEUSDT",
    "ROSEUSDT","DUSKUSDT","FLOWUSDT","IMXUSDT","API3USDT","GMTUSDT","APEUSDT",
    "WOOUSDT","JASMYUSDT","OPUSDT","INJUSDT","STGUSDT","SPELLUSDT",
    "1000LUNCUSDT","LUNA2USDT","LDOUSDT","ICPUSDT","APTUSDT","QNTUSDT",
    "FETUSDT","MAGICUSDT","TUSDT","MINAUSDT","ASTRUSDT","GMXUSDT","CFXUSDT",
    "STXUSDT","ACHUSDT","SSVUSDT","CKBUSDT","LQTYUSDT","USDCUSDT","IDUSDT",
    "ARBUSDT","JOEUSDT","TLMUSDT","HFTUSDT","XVSUSDT","BLURUSDT","EDUUSDT",
    "SUIUSDT","1000PEPEUSDT","1000FLOKIUSDT","UMAUSDT","NMRUSDT","MAVUSDT",
    "XVGUSDT","WLDUSDT","PENDLEUSDT","ARKMUSDT","AGLDUSDT","YGGUSDT",
    "DODOXUSDT","BNTUSDT","SEIUSDT","CYBERUSDT","ARKUSDT","BICOUSDT",
    "BIGTIMEUSDT","WAXPUSDT","BSVUSDT","RIFUSDT","POLYXUSDT","GASUSDT",
    "POWRUSDT","TIAUSDT","CAKEUSDT","MEMEUSDT","TWTUSDT","ORDIUSDT",
    "STEEMUSDT","ILVUSDT","KASUSDT","BEAMXUSDT","1000BONKUSDT","PYTHUSDT",
    "SUPERUSDT","USTCUSDT","ONGUSDT","ETHWUSDT","JTOUSDT","1000SATSUSDT",
    "AUCTIONUSDT","1000RATSUSDT","ACEUSDT","MOVRUSDT","XAIUSDT","WIFUSDT",
    "MANTAUSDT","ONDOUSDT","LSKUSDT","ALTUSDT","JUPUSDT","ZETAUSDT",
    "RONINUSDT","DYMUSDT","PIXELUSDT","STRKUSDT","GLMUSDT","PORTALUSDT",
    "AXLUSDT","METISUSDT","AEVOUSDT","VANRYUSDT","BOMEUSDT","ETHFIUSDT",
    "ENAUSDT","WUSDT","TNSRUSDT","SAGAUSDT","TAOUSDT","REZUSDT","BBUSDT",
    "NOTUSDT","TURBOUSDT","IOUSDT","ZKUSDT","MEWUSDT","LISTAUSDT","ZROUSDT",
    "RENDERUSDT","BANANAUSDT","RAREUSDT","GUSDT","SYNUSDT","BRETTUSDT",
    "POPCATUSDT","SUNUSDT","DOGSUSDT","FLUXUSDT","RPLUSDT","POLUSDT",
    "1MBABYDOGEUSDT","NEIROUSDT","FIDAUSDT","CATIUSDT","HMSTRUSDT","EIGENUSDT",
    "DIAUSDT","1000CATUSDT","SCRUSDT","GOATUSDT","MOODENGUSDT","SAFEUSDT",
    "SANTOSUSDT","COWUSDT","CETUSUSDT","1000000MOGUSDT","GRASSUSDT","DRIFTUSDT",
    "ACTUSDT","PNUTUSDT","BANUSDT","AKTUSDT","SCRTUSDT","1000CHEEMSUSDT",
    "THEUSDT","MORPHOUSDT","CHILLGUYUSDT","KAIAUSDT","AEROUSDT","ACXUSDT",
    "ORCAUSDT","MOVEUSDT","RAYSOLUSDT","KOMAUSDT","VIRTUALUSDT","SPXUSDT",
    "MEUSDT","AVAUSDT","VELODROMEUSDT","MOCAUSDT","VANAUSDT","PENGUUSDT",
    "LUMIAUSDT","USUALUSDT","AIXBTUSDT","FARTCOINUSDT","KMNOUSDT","CGPTUSDT",
    "HIVEUSDT","DEXEUSDT","PHAUSDT","GRIFFAINUSDT","ZEREBROUSDT","BIOUSDT",
    "COOKIEUSDT","ALCHUSDT","SWARMSUSDT","SONICUSDT","PROMUSDT","SUSDT",
    "SOLVUSDT","ARCUSDT","AVAAIUSDT","TRUMPUSDT","MELANIAUSDT","VTHOUSDT",
    "ANIMEUSDT","PIPPINUSDT","VVVUSDT","BERASUSDT","TSTUSDT","LAYERUSDT",
    "HEIUSDT","GPSUSDT","SHELLUSDT","KAITOUSDT","REDUSDT","VICUSDT","EPICUSDT",
    "BMTUSDT","MUBARAKUSDT","FORMUSDT","TUTUSDT","BROCCOLI714USDT",
    "BROCCOLIF3BUSDT","SIRENUSDT","BANANAS31USDT","BRUSDT","PLUMEUSDT",
    "NILUSDT","PARTIUSDT","JELLYJELLYUSDT","MAVIAUSDT","PAXGUSDT","WALUSDT",
    "GUNUSDT","ATHUSDT","BABYUSDT","PROMPTUSDT","STOUSDT","FHEUSDT",
    "KERNELUSDT","WCTUSDT","INITUSDT","BANKUSDT","DEEPUSDT","HYPERUSDT",
    "JSTUSDT","SIGNUSDT","PUNDIXUSDT","CTKUSDT","AIOTUSDT","DOLOUSDT",
    "HAEDALUSDT","SXTUSDT","ASRUSDT","ALPINEUSDT","B2USDT","SYRUPUSDT",
    "DOODUSDT","OGUSDT","SKYAIUSDT","NXPCUSDT","CVCUSDT","AGTUSDT","AWEUSDT",
    "BUSDT","SOONUSDT","HUMAUSDT","AUSDT","SOPHUSDT","MERLUSDT","HYPEUSDT",
    "1000000BOBUSDT","LAUSDT","HOMEUSDT","RESOLVUSDT","TAIKOUSDT","SQDUSDT",
    "PUMPBTCUSDT","SPKUSDT","MYXUSDT","FUSDT","NEWTUSDT","HUSDT","SAHARAUSDT",
    "ICNTUSDT","BULLAUSDT","IDOLUSDT","MUSDT","PUMPUSDT","CROSSUSDT","AINUSDT",
    "CUSDT","VELVETUSDT","TACUSDT","ERAUSDT","TAUSDT","CVXUSDT","SLPUSDT",
    "ZORAUSDT","TAGUSDT","ESPORTSUSDT","TREEUSDT","PLAYUSDT","NAORISUSDT",
    "TOWNSUSDT","PROVEUSDT","ALLUSDT","INUSDT","CARVUSDT","AIOUSDT","XNYUSDT",
    "USELESSUSDT","SAPIENUSDT","XPLUSDT","WLFIUSDT","SOMIUSDT","BASUSDT",
    "BTRUSDT","MITOUSDT","HEMIUSDT","LINEAUSDT","QUSDT","ARIAUSDT","TAKEUSDT",
    "PTBUSDT","OPENUSDT","FLOCKUSDT","SKYUSDT","AVNTUSDT","HOLOUSDT","XPINUSDT",
    "UBUSDT","ZKCUSDT","TOSHIUSDT","STBLUSDT","0GUSDT","BARDUSDT","ASTERUSDT",
    "TRADOORUSDT","BLESSUSDT","FLUIDUSDT","COAIUSDT","HANAUSDT","MIRAUSDT",
    "AKEUSDT","ORDERUSDT","LIGHTUSDT","XANUSDT","FFUSDT","EDENUSDT","NOMUSDT",
    "TRUTHUSDT","2ZUSDT","EVAAUSDT","LYNUSDT","KGENUSDT","4USDT","GIGGLEUSDT",
    "MONUSDT","YBUSDT","METUSDT","EULUSDT","ENSOUSDT","CLOUSDT","RECALLUSDT",
    "ZBTUSDT","LABUSDT","RIVERUSDT","BLUAIUSDT","TURTLEUSDT","APRUSDT","ONUSDT",
    "KITEUSDT","ATUSDT","CCUSDT","MMTUSDT","TRUSTUSDT","UAIUSDT","FOLKSUSDT",
    "STABLEUSDT","JCTUSDT","ALLUSDT","CLANKERUSDT","BEATUSDT","PIEVERSUSDT",
    "SENTUSDT","IRYSUSDT","POWERUSDT","WETUSDT","NIGHTUSDT","USUSDT","CYSUSDT",
    "RAVEUSDT","ZKPUSDT","GUAUSDT","LITUSDT","BREVUSDT","COLLECTUSDT",
    "MAGMAUSDT","ZAMAUSDT","FOGOUSDT","FRAXUSDT","SPORTFUNUSDT","AIAUSDT",
    "ACUUSDT","ELSAUSDT","SKRUSDT","SPACEUSDT","FIGHTUSDT","BIRBUSDT",
    "GWEIUSDT","MEGAUSDT","INXUSDT","TRIAUSDT","ESPUSDT","AZTECUSDT","OPNUSDT",
    "ROBOUSDT","KATUSDT","MANTRAUSDT","CFGUSDT","EDGEUSDT","BSBUSDT","XAUTUSDT",
    "BASEDUSDT","PRLUSDT","GENIUSUSDT","CHIPUSDT","OPGUSDT","AIGENSYNUSDT",
    "BILLUSDT","PHAROSUSDT","STARUSDT","CTRUSDT","SLXUSDT","ZESTUSDT","BTWUSDT",
    "REUSDT","ARXUSDT","OUSDT","CAPUSDT","GRAMUSDT","DATAIPUSDT","GRVTUSDT",
]

NUM_SHARDS = 8

START_YM  = (2023, 8)
END_YM    = (2025, 7)
TIMEFRAME = "15m"

CAPITAL   = 10_000.0
RISK_PCT  = 0.0075
FEE       = 0.0005
SLIP      = 0.0002
LEVERAGE  = 5
TP_PCT    = 0.030
SL_PCT    = 0.150
MAX_BARS  = 960
MIN_BARS  = 72
WORKERS   = 16   # per shard: each job only has ~66 coins, go aggressive

BASE_URL  = "https://data.binance.vision/data/futures/um/monthly/klines"

# ─── UTILS ─────────────────────────────────────────────────────────────────

def get_shard(shard_idx):
    return ALL_SYMBOLS[shard_idx::NUM_SHARDS]

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

# ─── DATA FETCH ─────────────────────────────────────────────────────────────

def fetch_month(symbol, year, month):
    fname = f"{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    url   = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{fname}"
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=25) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                rows = list(csv.reader(io.TextIOWrapper(f, "utf-8")))
        candles = []
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts > 10**14: ts //= 1000
            candles.append((ts, float(row[1]), float(row[2]),
                            float(row[3]), float(row[4])))
        return candles
    except HTTPError as e:
        if e.code == 404: return []
        raise
    except Exception:
        return []

def fetch_symbol(symbol):
    """Fetch all available months for this symbol (skip 404s)."""
    all_c = []
    for (y, m) in ALL_MONTHS:
        all_c.extend(fetch_month(symbol, y, m))
    all_c.sort(key=lambda x: x[0])
    seen = set(); out = []
    for c in all_c:
        if c[0] not in seen:
            seen.add(c[0]); out.append(c)
    return out

# ─── INDICATORS ─────────────────────────────────────────────────────────────

def ema(closes, period):
    k = 2.0 / (period + 1)
    v = [closes[0]]
    for c in closes[1:]:
        v.append(c * k + v[-1] * (1 - k))
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
    pdi = [100*p/t if t else 0 for p,t in zip(sp,st)]
    mdi = [100*m/t if t else 0 for m,t in zip(sm,st)]
    dx  = [100*abs(p-m)/(p+m) if p+m else 0 for p,m in zip(pdi,mdi)]
    if len(dx) < period: return 0.0
    adx_v = sum(dx[:period]) / period
    for d in dx[period:]: adx_v = (adx_v*(period-1)+d)/period
    return max(0.0, min(100.0, adx_v))

# ─── SIGNAL ─────────────────────────────────────────────────────────────────

def signal(i, e9, e21, e50, highs, lows, closes):
    if i < 10: return None
    slope = (e50[i] - e50[i-10]) / e50[i-10] * 100
    up    = slope >  0.05
    down  = slope < -0.05
    if not up and not down: return None
    xu = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    xd = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if not xu and not xd: return None
    if up and not xu: return None
    if down and not xd: return None
    if adx(highs[:i+1], lows[:i+1], closes[:i+1]) < 22: return None
    return 'buy' if xu else 'sell'

# ─── BACKTEST ONE SYMBOL ─────────────────────────────────────────────────────

def backtest(symbol, candles):
    if len(candles) < MIN_BARS + 2:
        return []
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
        if pos:
            h, l, ep = hi[i], lo[i], pos['ep']
            held = i - pos['eb']
            if pos['side'] == 'buy':
                if l <= pos['sl']:
                    exit_p, reason = pos['sl'], 'sl'
                    gross = (exit_p - ep) / ep
                elif h >= pos['tp']:
                    exit_p, reason = pos['tp'], 'tp'
                    gross = (exit_p - ep) / ep
                elif held >= MAX_BARS:
                    exit_p, reason = cl[i], 'max_hold'
                    gross = (exit_p - ep) / ep
                else:
                    continue
            else:
                if h >= pos['sl']:
                    exit_p, reason = pos['sl'], 'sl'
                    gross = (ep - exit_p) / ep
                elif l <= pos['tp']:
                    exit_p, reason = pos['tp'], 'tp'
                    gross = (ep - exit_p) / ep
                elif held >= MAX_BARS:
                    exit_p, reason = cl[i], 'max_hold'
                    gross = (ep - exit_p) / ep
                else:
                    continue
            net = gross - (FEE + SLIP) * 2
            pnl = pos['notional'] * net
            trades.append({
                'symbol': symbol,
                'side': pos['side'],
                'entry_ts': pos['ts'],
                'exit_ts': ts[i],
                'entry_price': round(ep, 8),
                'exit_price': round(exit_p, 8),
                'pnl': round(pnl, 6),
                'reason': reason,
                'bars': held,
            })
            pos = None

        sig = signal(i, e9, e21, e50, hi, lo, cl)
        if sig is None or pos is not None:
            continue

        eb  = i + 1
        ep  = op[eb] * (1 + FEE + SLIP if sig == 'buy' else 1 - FEE - SLIP)
        notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
        tp = ep * (1 + TP_PCT) if sig == 'buy' else ep * (1 - TP_PCT)
        sl = ep * (1 - SL_PCT) if sig == 'buy' else ep * (1 + SL_PCT)
        pos = {'side': sig, 'eb': eb, 'ts': ts[eb],
               'ep': ep, 'tp': tp, 'sl': sl, 'notional': notional}

    if pos:
        i = len(candles) - 1
        exit_p = cl[i]
        gross = (exit_p - pos['ep']) / pos['ep'] if pos['side'] == 'buy' \
                else (pos['ep'] - exit_p) / pos['ep']
        net = gross - (FEE + SLIP) * 2
        trades.append({
            'symbol': symbol, 'side': pos['side'],
            'entry_ts': pos['ts'], 'exit_ts': ts[i],
            'entry_price': round(pos['ep'], 8),
            'exit_price': round(exit_p, 8),
            'pnl': round(pos['notional'] * net, 6),
            'reason': 'end_of_data',
            'bars': i - pos['eb'],
        })
    return trades

# ─── STATS ─────────────────────────────────────────────────────────────────

def stats(trades):
    if not trades: return {}
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    total  = len(trades)
    wr     = len(wins) / total * 100
    gp     = sum(t['pnl'] for t in wins)
    gl     = sum(abs(t['pnl']) for t in losses)
    pf     = round(gp / gl, 4) if gl else float('inf')
    net    = sum(t['pnl'] for t in trades)
    avg_w  = gp / len(wins) if wins else 0
    avg_l  = sum(t['pnl'] for t in losses) / len(losses) if losses else 0

    # max drawdown on PnL stream
    eq = 0; peak = 0; dd = 0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        eq += t['pnl']
        if eq > peak: peak = eq
        if peak > 0: dd = max(dd, peak - eq)

    # monthly
    monthly = {}
    for t in trades:
        k = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc).strftime('%Y-%m')
        monthly.setdefault(k, {'pnl': 0.0, 'n': 0, 'w': 0})
        monthly[k]['pnl'] += t['pnl']
        monthly[k]['n']   += 1
        if t['pnl'] > 0: monthly[k]['w'] += 1

    # per-coin
    by_coin = {}
    for t in trades:
        by_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0})
        by_coin[t['symbol']]['pnl'] += t['pnl']
        by_coin[t['symbol']]['n']   += 1
        if t['pnl'] > 0: by_coin[t['symbol']]['w'] += 1

    return {
        'total': total,
        'win_rate': round(wr, 2),
        'profit_factor': pf,
        'net_pnl': round(net, 4),
        'max_drawdown': round(dd, 4),
        'avg_win': round(avg_w, 4),
        'avg_loss': round(avg_l, 4),
        'expectancy': round((wr/100)*avg_w + (1-wr/100)*avg_l, 4),
        'longs':  len([t for t in trades if t['side']=='buy']),
        'shorts': len([t for t in trades if t['side']=='sell']),
        'monthly': dict(sorted(monthly.items())),
        'per_coin': {k: {**v, 'wr': round(v['w']/v['n']*100,1)}
                     for k, v in sorted(by_coin.items(),
                     key=lambda x: x[1]['pnl'], reverse=True)},
    }

# ─── SHARD RUN ─────────────────────────────────────────────────────────────

def run_shard(shard_idx):
    symbols = get_shard(shard_idx)
    t0 = time.time()
    print(f"[Shard {shard_idx}] {len(symbols)} coins | {WORKERS} workers")

    raw = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, s): s for s in symbols}
        done = 0
        for f in as_completed(futs):
            sym = futs[f]
            done += 1
            try:
                raw[sym] = f.result()
            except Exception as e:
                print(f"  FETCH ERR {sym}: {e}")
                raw[sym] = []
            if done % 20 == 0 or done == len(symbols):
                print(f"  Fetched {done}/{len(symbols)}  elapsed={time.time()-t0:.0f}s")

    with_data = [s for s in symbols if len(raw.get(s,[])) >= MIN_BARS+2]
    print(f"  {len(with_data)}/{len(symbols)} have enough data")

    all_trades = []
    for sym in with_data:
        try:
            all_trades.extend(backtest(sym, raw[sym]))
        except Exception as e:
            print(f"  BACKTEST ERR {sym}: {e}")

    s = stats(all_trades)
    out = {
        'shard': shard_idx,
        'symbols': symbols,
        'with_data': with_data,
        'trades': all_trades,
        'stats': s,
        'elapsed': round(time.time()-t0, 1),
    }

    fname = f"shard_{shard_idx}.json"
    with open(fname, 'w') as f:
        json.dump(out, f)

    print(f"[Shard {shard_idx}] Done in {out['elapsed']}s | "
          f"Trades={len(all_trades)} | "
          f"WR={s.get('win_rate','?')}% | "
          f"PF={s.get('profit_factor','?')} | "
          f"PnL={s.get('net_pnl','?')}")
    print(f"  Written: {fname}")

# ─── MERGE ─────────────────────────────────────────────────────────────────

def merge_shards():
    print("Merging shards...")
    all_trades = []
    meta = {'shards': [], 'symbols_attempted': 0,
            'symbols_with_data': 0, 'total_elapsed': 0}

    for i in range(NUM_SHARDS):
        fname = f"shard_{i}.json"
        if not os.path.exists(fname):
            print(f"  WARNING: {fname} missing — shard {i} may have failed")
            continue
        with open(fname) as f:
            d = json.load(f)
        all_trades.extend(d['trades'])
        meta['symbols_attempted'] += len(d['symbols'])
        meta['symbols_with_data'] += len(d['with_data'])
        meta['total_elapsed']     += d['elapsed']
        meta['shards'].append({'shard': i, 'elapsed': d['elapsed'],
                               'trades': len(d['trades'])})
        print(f"  Shard {i}: {len(d['trades'])} trades, {d['elapsed']}s")

    s = stats(all_trades)
    pf = s.get('profit_factor', 0)
    wr = s.get('win_rate', 0)
    if pf == float('inf'):
        pf_display = 'inf'
        meets = wr >= 42.0
    else:
        pf_display = str(pf)
        meets = pf >= 1.5 and wr >= 42.0
    rec = ("✅ USABLE — PF≥1.5 and WR≥42% met" if meets
           else f"❌ NOT USABLE — PF={pf_display}, WR={wr}%")

    report = {
        'meta': {
            **meta,
            'strategy': 'GMax V1 · Variant G · 15m',
            'period': f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
            'capital': CAPITAL,
            'risk_pct': RISK_PCT,
            'leverage': LEVERAGE,
            'tp_pct': TP_PCT,
            'sl_pct': SL_PCT,
            'fee': FEE,
            'slip': SLIP,
        },
        'aggregate': s,
        'recommendation': rec,
        'trades': sorted(all_trades, key=lambda x: x['exit_ts']),
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # Human-readable summary
    lines = [
        "=" * 60,
        "GMax V1 — Variant G · 15m — FULL BACKTEST RESULTS",
        "=" * 60,
        f"Period     : {report['meta']['period']}",
        f"Symbols    : {meta['symbols_with_data']} with data / {meta['symbols_attempted']} attempted",
        f"Total time : {meta['total_elapsed']:.0f}s across shards",
        "",
        "── AGGREGATE ──",
        f"  Total trades   : {s.get('total',0)}",
        f"  Win rate       : {s.get('win_rate',0)}%",
        f"  Profit factor  : {pf_display}",
        f"  Net PnL        : ${s.get('net_pnl',0):,.2f}",
        f"  Max drawdown   : ${s.get('max_drawdown',0):,.2f}",
        f"  Avg win        : ${s.get('avg_win',0):,.2f}",
        f"  Avg loss       : ${s.get('avg_loss',0):,.2f}",
        f"  Expectancy     : ${s.get('expectancy',0):,.2f}",
        f"  Longs          : {s.get('longs',0)}",
        f"  Shorts         : {s.get('shorts',0)}",
        "",
        f"  RECOMMENDATION : {rec}",
        "",
        "── PER-COIN (top 50 by Net PnL) ──",
        f"  {'Symbol':<22} {'Trades':>6} {'WR%':>7} {'PnL':>12}",
        "  " + "-"*50,
    ]
    per = s.get('per_coin', {})
    for sym, v in list(per.items())[:50]:
        lines.append(f"  {sym:<22} {v['n']:>6} {v['wr']:>6.1f}%  ${v['pnl']:>10.2f}")

    lines += ["", "── MONTHLY PnL ──"]
    for mo, v in s.get('monthly', {}).items():
        sign = '+' if v['pnl'] >= 0 else ''
        wr_mo = v['w']/v['n']*100 if v['n'] else 0
        lines.append(f"  {mo}  {sign}{v['pnl']:>9.2f} USDT  n={v['n']}  wr={wr_mo:.0f}%")

    summary = "\n".join(lines)
    print("\n" + summary)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary)

    print("\n✅ backtest_report.json and backtest_summary.txt written")

# ─── ENTRY ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_index|merge>")
        sys.exit(1)

    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        try:
            idx = int(arg)
            if not 0 <= idx < NUM_SHARDS:
                raise ValueError
        except ValueError:
            print(f"shard_index must be 0..{NUM_SHARDS-1}")
            sys.exit(1)
        run_shard(idx)

