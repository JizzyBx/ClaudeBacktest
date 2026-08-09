"""
Multi-Confirmation Strategy Exploration Backtest — GitHub Actions Pipeline
117-coin universe (from GMax bot), 5x leverage, 20 parallel shards.
5 strategies, each requiring 2-4 independent confirmations across timeframes
(30m/1h/2h/4h) to fire. Quality over quantity — expect low trade counts.
stdlib only. No pip installs.
"""

import sys, os, json, csv, io, zipfile, urllib.request, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Coin Universe (527 coins, full Binance USDT-perpetual list) ──────
ALL_SYMBOLS = [
    '0GUSDT','1000000BOBUSDT','1000000MOGUSDT','1000BONKUSDT','1000CATUSDT',
    '1000CHEEMSUSDT','1000FLOKIUSDT','1000LUNCUSDT','1000PEPEUSDT','1000RATSUSDT',
    '1000SATSUSDT','1000SHIBUSDT','1000XECUSDT','1INCHUSDT','1MBABYDOGEUSDT',
    '2ZUSDT','4USDT','AAVEUSDT','ACEUSDT','ACHUSDT',
    'ACTUSDT','ACUUSDT','ACXUSDT','ADAUSDT','AEROUSDT',
    'AEVOUSDT','AGLDUSDT','AGTUSDT','AIAUSDT','AIGENSYNUSDT',
    'AINUSDT','AIOTUSDT','AIOUSDT','AIXBTUSDT','AKEUSDT',
    'AKTUSDT','ALCHUSDT','ALGOUSDT','ALICEUSDT','ALLOUSDT',
    'ALLUSDT','ALPINEUSDT','ALTUSDT','ANIMEUSDT','ANKRUSDT',
    'APEUSDT','API3USDT','APRUSDT','APTUSDT','ARBUSDT',
    'ARCUSDT','ARIAUSDT','ARKMUSDT','ARKUSDT','ARPAUSDT',
    'ARUSDT','ARXUSDT','ASRUSDT','ASTERUSDT','ASTRUSDT',
    'ATHUSDT','ATOMUSDT','ATUSDT','AUCTIONUSDT','AUSDT',
    'AVAAIUSDT','AVAUSDT','AVAXUSDT','AVNTUSDT','AWEUSDT',
    'AXLUSDT','AXSUSDT','AZTECUSDT','B2USDT','BABYUSDT',
    'BANANAS31USDT','BANANAUSDT','BANDUSDT','BANKUSDT','BANUSDT',
    'BARDUSDT','BASEDUSDT','BASUSDT','BATUSDT','BBUSDT',
    'BCHUSDT','BEAMXUSDT','BEATUSDT','BELUSDT','BERAUSDT',
    'BICOUSDT','BIGTIMEUSDT','BILLUSDT','BIOUSDT','BIRBUSDT',
    'BLESSUSDT','BLUAIUSDT','BLURUSDT','BMTUSDT','BNBUSDT',
    'BNTUSDT','BOMEUSDT','BRETTUSDT','BREVUSDT','BROCCOLI714USDT',
    'BROCCOLIF3BUSDT','BRUSDT','BSBUSDT','BSVUSDT','BTCDOMUSDT',
    'BTCUSDT','BTRUSDT','BTWUSDT','BULLAUSDT','BUSDT',
    'C98USDT','CAKEUSDT','CAPUSDT','CARVUSDT','CATIUSDT',
    'CCUSDT','CELOUSDT','CELRUSDT','CETUSUSDT','CFGUSDT',
    'CFXUSDT','CGPTUSDT','CHILLGUYUSDT','CHIPUSDT','CHRUSDT',
    'CHZUSDT','CKBUSDT','CLANKERUSDT','CLOUSDT','COAIUSDT',
    'COLLECTUSDT','COMPUSDT','COOKIEUSDT','COTIUSDT','COWUSDT',
    'CROSSUSDT','CRVUSDT','CTKUSDT','CTRUSDT','CTSIUSDT',
    'CUSDT','CVCUSDT','CVXUSDT','CYBERUSDT','CYSUSDT',
    'DASHUSDT','DATAIPUSDT','DEEPUSDT','DEXEUSDT','DIAUSDT',
    'DODOXUSDT','DOGEUSDT','DOGSUSDT','DOLOUSDT','DOODUSDT',
    'DOTUSDT','DRIFTUSDT','DUSKUSDT','DYDXUSDT','DYMUSDT',
    'EDENUSDT','EDGEUSDT','EDUUSDT','EGLDUSDT','EIGENUSDT',
    'ELSAUSDT','ENAUSDT','ENJUSDT','ENSOUSDT','ENSUSDT',
    'EPICUSDT','ERAUSDT','ESPORTSUSDT','ESPUSDT','ETCUSDT',
    'ETHFIUSDT','ETHUSDT','ETHWUSDT','EULUSDT','EVAAUSDT',
    'FARTCOINUSDT','FETUSDT','FFUSDT','FHEUSDT','FIDAUSDT',
    'FIGHTUSDT','FILUSDT','FLOCKUSDT','FLOWUSDT','FLUIDUSDT',
    'FLUXUSDT','FOGOUSDT','FOLKSUSDT','FORMUSDT','FRAXUSDT',
    'FUSDT','GALAUSDT','GASUSDT','GENIUSUSDT','GIGGLEUSDT',
    'GLMUSDT','GMTUSDT','GMXUSDT','GOATUSDT','GPSUSDT',
    'GRAMUSDT','GRASSUSDT','GRIFFAINUSDT','GRTUSDT','GRVTUSDT',
    'GTCUSDT','GUAUSDT','GUNUSDT','GUSDT','GWEIUSDT',
    'HAEDALUSDT','HANAUSDT','HBARUSDT','HEIUSDT','HEMIUSDT',
    'HFTUSDT','HIVEUSDT','HMSTRUSDT','HOLOUSDT','HOMEUSDT',
    'HOTUSDT','HUMAUSDT','HUSDT','HYPERUSDT','HYPEUSDT',
    'ICNTUSDT','ICPUSDT','ICXUSDT','IDOLUSDT','IDUSDT',
    'ILVUSDT','IMXUSDT','INITUSDT','INJUSDT','INUSDT',
    'INXUSDT','IOSTUSDT','IOTAUSDT','IOTXUSDT','IOUSDT',
    'IRYSUSDT','JASMYUSDT','JCTUSDT','JELLYJELLYUSDT','JOEUSDT',
    'JSTUSDT','JTOUSDT','JUPUSDT','KAIAUSDT','KAITOUSDT',
    'KASUSDT','KATUSDT','KAVAUSDT','KERNELUSDT','KGENUSDT',
    'KITEUSDT','KMNOUSDT','KNCUSDT','KOMAUSDT','KSMUSDT',
    'LABUSDT','LAUSDT','LAYERUSDT','LDOUSDT','LIGHTUSDT',
    'LINEAUSDT','LINKUSDT','LISTAUSDT','LITUSDT','LPTUSDT',
    'LQTYUSDT','LSKUSDT','LTCUSDT','LUMIAUSDT','LUNA2USDT',
    'LYNUSDT','MAGICUSDT','MAGMAUSDT','MANAUSDT','MANTAUSDT',
    'MANTRAUSDT','MASKUSDT','MAVIAUSDT','MAVUSDT','MEGAUSDT',
    'MELANIAUSDT','MEMEUSDT','MERLUSDT','METISUSDT','METUSDT',
    'MEUSDT','MEWUSDT','MINAUSDT','MIRAUSDT','MITOUSDT',
    'MMTUSDT','MOCAUSDT','MONUSDT','MOODENGUSDT','MORPHOUSDT',
    'MOVEUSDT','MOVRUSDT','MTLUSDT','MUBARAKUSDT','MUSDT',
    'MYXUSDT','NAORISUSDT','NEARUSDT','NEIROUSDT','NEOUSDT',
    'NEWTUSDT','NIGHTUSDT','NILUSDT','NMRUSDT','NOMUSDT',
    'NOTUSDT','NXPCUSDT','OGNUSDT','OGUSDT','ONDOUSDT',
    'ONEUSDT','ONGUSDT','ONTUSDT','ONUSDT','OPENUSDT',
    'OPGUSDT','OPNUSDT','OPUSDT','ORCAUSDT','ORDERUSDT',
    'ORDIUSDT','OUSDT','PARTIUSDT','PAXGUSDT','PENDLEUSDT',
    'PENGUUSDT','PEOPLEUSDT','PHAROSUSDT','PHAUSDT','PIEVERSEUSDT',
    'PIPPINUSDT','PIXELUSDT','PLAYUSDT','PLUMEUSDT','PNUTUSDT',
    'POLUSDT','POLYXUSDT','POPCATUSDT','PORTALUSDT','POWERUSDT',
    'POWRUSDT','PRLUSDT','PROMPTUSDT','PROMUSDT','PROVEUSDT',
    'PTBUSDT','PUMPBTCUSDT','PUMPUSDT','PUNDIXUSDT','PYTHUSDT',
    'QNTUSDT','QTUMUSDT','QUSDT','RAREUSDT','RAVEUSDT',
    'RAYSOLUSDT','RECALLUSDT','REDUSDT','RENDERUSDT','RESOLVUSDT',
    'REUSDT','REZUSDT','RIFUSDT','RIVERUSDT','RLCUSDT',
    'ROBOUSDT','RONINUSDT','ROSEUSDT','RPLUSDT','RSRUSDT',
    'RUNEUSDT','RVNUSDT','SAFEUSDT','SAGAUSDT','SAHARAUSDT',
    'SANDUSDT','SANTOSUSDT','SAPIENUSDT','SCRTUSDT','SCRUSDT',
    'SEIUSDT','SENTUSDT','SFPUSDT','SHELLUSDT','SIGNUSDT',
    'SIRENUSDT','SKLUSDT','SKRUSDT','SKYAIUSDT','SKYUSDT',
    'SLPUSDT','SLXUSDT','SNXUSDT','SOLUSDT','SOLVUSDT',
    'SOMIUSDT','SONICUSDT','SOONUSDT','SOPHUSDT','SPACEUSDT',
    'SPELLUSDT','SPKUSDT','SPORTFUNUSDT','SPXUSDT','SQDUSDT',
    'SSVUSDT','STABLEUSDT','STARUSDT','STBLUSDT','STEEMUSDT',
    'STGUSDT','STORJUSDT','STOUSDT','STRKUSDT','STXUSDT',
    'SUIUSDT','SUNUSDT','SUPERUSDT','SUSDT','SUSHIUSDT',
    'SWARMSUSDT','SXTUSDT','SYNUSDT','SYRUPUSDT','TACUSDT',
    'TAGUSDT','TAIKOUSDT','TAKEUSDT','TAOUSDT','TAUSDT',
    'THETAUSDT','THEUSDT','TIAUSDT','TLMUSDT','TNSRUSDT',
    'TOSHIUSDT','TOWNSUSDT','TRADOORUSDT','TRBUSDT','TREEUSDT',
    'TRIAUSDT','TRUMPUSDT','TRUSTUSDT','TRUTHUSDT','TRXUSDT',
    'TSTUSDT','TURBOUSDT','TURTLEUSDT','TUSDT','TUTUSDT',
    'TWTUSDT','UAIUSDT','UBUSDT','UMAUSDT','UNIUSDT',
    'USDCUSDT','USELESSUSDT','USTCUSDT','USUALUSDT','USUSDT',
    'VANAUSDT','VANRYUSDT','VELODROMEUSDT','VELVETUSDT','VETUSDT',
    'VICUSDT','VIRTUALUSDT','VTHOUSDT','VVVUSDT','WALUSDT',
    'WAXPUSDT','WCTUSDT','WETUSDT','WIFUSDT','WLDUSDT',
    'WLFIUSDT','WOOUSDT','WUSDT','XAIUSDT','XANUSDT',
    'XAUTUSDT','XLMUSDT','XMRUSDT','XNYUSDT','XPINUSDT',
    'XPLUSDT','XRPUSDT','XTZUSDT','XVGUSDT','XVSUSDT',
    'YBUSDT','YFIUSDT','YGGUSDT','ZAMAUSDT','ZBTUSDT',
    'ZECUSDT','ZENUSDT','ZEREBROUSDT','ZESTUSDT','ZETAUSDT',
    'ZILUSDT','ZKCUSDT','ZKPUSDT','ZKUSDT','ZORAUSDT',
    'ZROUSDT','ZRXUSDT',
]

NUM_SHARDS = 20
WORKERS = 16

# ── Config ─────────────────────────────────────────────────────────
START_YM = (2024, 8)
END_YM   = (2026, 7)
CAPITAL   = 10000.0
RISK_PCT  = 0.0075       # 0.75% risk per trade
FEE       = 0.0005       # 0.05%
SLIP      = 0.0002       # 0.02%
LEVERAGE  = 5             # per user instruction: minimum 5x
MIN_BARS  = 60            # warmup bars before any signal can fire (lower TF bar count is coarser now)

TIMEFRAMES_NEEDED = ['15m', '1h']  # only what S8 + S8_REFINED actually need

# ── Strategy Definitions ──────────────────────────────────────────
# Each strategy declares entry tf + the bias tf(s) it needs for confirmation.
# Trimmed to S8 (original) + S8_REFINED only for this run — testing both against
# the full 527-coin universe. Other exploratory strategies (M1-M7) all showed
# flat/negative edge in prior batches; dropping them keeps this run's fetch load
# manageable on the free-tier 20-shard limit with 4.5x more coins than before.
STRATEGIES = {
    'S8_MTF_CONFLUENCE': {
        # ORIGINAL, UNCHANGED — the only strategy across all prior batches with PF > 1.0
        # (PF 1.345, WR 51.2%, Sharpe 1.695 on the 117-coin universe). Permanent control.
        'name': 'MTF Confluence ORIGINAL (1h bias + 15m entry)',
        'tf': '15m', 'bias_tfs': ['1h'], 'tp': 0.05, 'sl': 0.035,
    },
    'S8R_MTF_CONFLUENCE_REFINED': {
        # REFINED VARIANT — same core mechanism, two changes being tested:
        # 1. TP widened 5.0% -> 5.5% (tests if edge survives a better R:R before assuming
        #    win rate holds up)
        # 2. Long-side confirmation tightened (stronger bullish separation required) to
        #    address the 56-long/188-short imbalance seen in the original's results —
        #    testing whether that skew was a real regime effect or an artifact of a
        #    slightly looser long-side trigger.
        'name': 'MTF Confluence REFINED (wider TP, balanced long/short filter)',
        'tf': '15m', 'bias_tfs': ['1h'], 'tp': 0.055, 'sl': 0.035,
    },
}

MAX_BARS_BY_TF = {'15m': 60, '1h': 45}

# ── Data fetch (Binance Vision monthly archives) ──────────────────
def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out

def fetch_month(symbol, tf, year, month):
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines/"
           f"{symbol}/{tf}/{symbol}-{tf}-{year:04d}-{month:02d}.zip")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read()
        zf = zipfile.ZipFile(io.BytesIO(data))
        name = zf.namelist()[0]
        rows = []
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding='utf-8')
            reader = csv.reader(text)
            for r in reader:
                if not r or r[0] in ('open_time', ''):
                    continue
                try:
                    ts = int(r[0])
                    # Normalize to seconds: >=10^16 us, >=10^12 ms, else already seconds
                    if ts >= 10**16:
                        ts //= 1_000_000
                    elif ts >= 10**12:
                        ts //= 1000
                    o, h, l, c = float(r[1]), float(r[2]), float(r[3]), float(r[4])
                    vol = float(r[5])
                    rows.append((ts, o, h, l, c, vol))
                except (ValueError, IndexError):
                    continue
        return rows
    except Exception:
        return []

def fetch_symbol_tf(symbol, tf):
    months = month_range(START_YM, END_YM)
    all_rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_month, symbol, tf, y, m): (y, m) for (y, m) in months}
        for fut in as_completed(futs):
            rows = fut.result()
            if rows:
                all_rows.extend(rows)
    if not all_rows:
        return None
    dedup = {}
    for row in all_rows:
        dedup[row[0]] = row
    return sorted(dedup.values(), key=lambda r: r[0])

def fetch_symbol_all_tfs(symbol):
    out = {}
    for tf in TIMEFRAMES_NEEDED:
        rows = fetch_symbol_tf(symbol, tf)
        if rows:
            out[tf] = rows
    return out

# ── Indicators (pure python) ───────────────────────────────────────
def ema_series(closes, period):
    if len(closes) < period:
        return [None] * len(closes)
    out = [None] * (period - 1)
    k = 2.0 / (period + 1)
    sma = sum(closes[:period]) / period
    out.append(sma)
    prev = sma
    for c in closes[period:]:
        val = c * k + prev * (1 - k)
        out.append(val)
        prev = val
    return out

def sma_series(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        out[i] = sum(values[i - period + 1:i + 1]) / period
    return out

def stdev_series(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        m = sum(window) / period
        var = sum((x - m) ** 2 for x in window) / period
        out[i] = var ** 0.5
    return out

def rsi_series(closes, period=14):
    n = len(closes)
    out = [None] * n
    if n <= period:
        return out
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    out[period] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0); loss = max(-d, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
    return out

def adx_series(highs, lows, closes, period=14):
    """
    Wilder's ADX. NOTE: a prior version of this function had a critical bug —
    pdi_s/mdi_s were updated as `val*(period-1) + new` without dividing back by
    period, causing unbounded exponential growth (13^n) that overflowed to inf
    after ~290 bars on every single symbol/timeframe in every prior backtest run.
    Any strategy gated on ADX (all of them) was silently getting ADX=inf/nan
    almost immediately, meaning ADX filters were effectively broken pipeline-wide.
    Fixed here with correct Wilder smoothing: new = old*(period-1)/period + current.
    """
    n = len(closes)
    out = [None] * n
    if n <= period * 2:
        return out
    tr, pdm, mdm = [0.0], [0.0], [0.0]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        dn = lows[i - 1] - lows[i]
        pdm.append(up if (up > dn and up > 0) else 0.0)
        mdm.append(dn if (dn > up and dn > 0) else 0.0)
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr = sum(tr[1:period + 1]) / period
    pdi_s = sum(pdm[1:period + 1]) / period
    mdi_s = sum(mdm[1:period + 1]) / period
    dx_list = []
    for i in range(period + 1, n):
        atr = (atr * (period - 1) + tr[i]) / period
        pdi_s = (pdi_s * (period - 1) + pdm[i]) / period
        mdi_s = (mdi_s * (period - 1) + mdm[i]) / period
        pdi = 100 * pdi_s / atr if atr > 1e-12 else 0.0
        mdi = 100 * mdi_s / atr if atr > 1e-12 else 0.0
        dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 1e-12 else 0.0
        dx_list.append(dx)
        if len(dx_list) == period:
            out[i] = sum(dx_list) / period
        elif len(dx_list) > period:
            prev = out[i - 1] if out[i - 1] is not None else 0.0
            out[i] = (prev * (period - 1) + dx) / period
    return out

def bollinger(closes, period=20, mult=2.0):
    mid = sma_series(closes, period)
    sd = stdev_series(closes, period)
    upper = [None if mid[i] is None else mid[i] + mult * sd[i] for i in range(len(closes))]
    lower = [None if mid[i] is None else mid[i] - mult * sd[i] for i in range(len(closes))]
    return mid, upper, lower

def donchian(highs, lows, period=20):
    n = len(highs)
    upper = [None] * n
    lower = [None] * n
    for i in range(period, n):
        upper[i] = max(highs[i - period:i])
        lower[i] = min(lows[i - period:i])
    return upper, lower

def macd_series(closes, fast=12, slow=26, signal=9):
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    macd_line = [None if (ema_fast[i] is None or ema_slow[i] is None) else ema_fast[i] - ema_slow[i]
                 for i in range(len(closes))]
    valid_start = next((i for i, v in enumerate(macd_line) if v is not None), None)
    signal_line = [None] * len(closes)
    if valid_start is not None:
        seg = macd_line[valid_start:]
        k = 2.0 / (signal + 1)
        seg_signal = [None] * len(seg)
        if len(seg) >= signal:
            sma0 = sum(v for v in seg[:signal]) / signal
            seg_signal[signal - 1] = sma0
            prev = sma0
            for j in range(signal, len(seg)):
                val = seg[j] * k + prev * (1 - k)
                seg_signal[j] = val
                prev = val
            for j, v in enumerate(seg_signal):
                signal_line[valid_start + j] = v
    hist = [None if (macd_line[i] is None or signal_line[i] is None) else macd_line[i] - signal_line[i]
            for i in range(len(closes))]
    return macd_line, signal_line, hist

def find_htf_index(htf_ts, target_ts):
    lo, hi = 0, len(htf_ts) - 1
    res = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if htf_ts[mid] <= target_ts:
            res = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return res

# ── Build indicator context for a candle series ────────────────────
def build_ctx(rows):
    closes = [r[4] for r in rows]
    highs  = [r[2] for r in rows]
    lows   = [r[3] for r in rows]
    opens  = [r[1] for r in rows]
    vols   = [r[5] for r in rows]
    ts     = [r[0] for r in rows]
    e9  = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)
    adx = adx_series(highs, lows, closes, 14)
    rsi = rsi_series(closes, 14)
    bb_mid, bb_up, bb_low = bollinger(closes, 20, 2.0)
    dc_up, dc_low = donchian(highs, lows, 20)
    macd_line, macd_signal, macd_hist = macd_series(closes)
    return {
        'ts': ts, 'closes': closes, 'highs': highs, 'lows': lows, 'opens': opens, 'vols': vols,
        'e9': e9, 'e21': e21, 'e50': e50, 'adx': adx, 'rsi': rsi,
        'bb_mid': bb_mid, 'bb_up': bb_up, 'bb_low': bb_low,
        'dc_up': dc_up, 'dc_low': dc_low,
        'macd_line': macd_line, 'macd_signal': macd_signal, 'macd_hist': macd_hist,
    }

# ── Confirmation helper functions (return True/False/None) ────────
def htf_trend_bull(ctx, idx):
    if idx is None:
        return None
    e21, e50 = ctx['e21'], ctx['e50']
    if idx >= len(e21) or e21[idx] is None or e50[idx] is None:
        return None
    return e21[idx] > e50[idx]

def htf_trend_bear(ctx, idx):
    if idx is None:
        return None
    e21, e50 = ctx['e21'], ctx['e50']
    if idx >= len(e21) or e21[idx] is None or e50[idx] is None:
        return None
    return e21[idx] < e50[idx]

# ── Signal functions — each returns 'buy' / 'sell' / None on bar i ──

def sig_s8_mtf_confluence(ctx, i, bias_ctxs):
    """
    RESTORED, UNCHANGED from the batch where it scored PF 1.309 / WR ~57% / 223 trades.
    Confirmation 1: 15m EMA9/21 fresh cross (entry trigger)
    Confirmation 2: 1h EMA9/21 separation meaningful (>0.2%), not a marginal cross
    Confirmation 3: 15m ADX > 20 (momentum confirmed, not chop)
    """
    e9, e21, adx = ctx['e9'], ctx['e21'], ctx['adx']
    if i < 1 or None in (e9[i], e21[i], e9[i-1], e21[i-1], adx[i]):
        return None
    htf_ctx, htf_idx = bias_ctxs['1h']
    if htf_ctx is None or htf_idx is None:
        return None
    htf_e9, htf_e21 = htf_ctx['e9'], htf_ctx['e21']
    if htf_idx >= len(htf_e9) or htf_e9[htf_idx] is None or htf_e21[htf_idx] is None or not htf_e21[htf_idx]:
        return None
    htf_sep = (htf_e9[htf_idx] - htf_e21[htf_idx]) / htf_e21[htf_idx] * 100
    htf_bull = htf_sep > 0.2
    htf_bear = htf_sep < -0.2
    crossed_up = e9[i-1] <= e21[i-1] and e9[i] > e21[i]
    crossed_dn = e9[i-1] >= e21[i-1] and e9[i] < e21[i]
    if crossed_up and htf_bull and adx[i] > 20:
        return 'buy'
    if crossed_dn and htf_bear and adx[i] > 20:
        return 'sell'
    return None

def sig_s8_refined_mtf_confluence(ctx, i, bias_ctxs):
    """
    REFINED variant of S8. Same core mechanism (15m EMA9/21 cross + 1h bias +
    ADX filter), with two deliberate changes to test against the original:
    1. Long-side bullish separation threshold raised from 0.2% to 0.35% — the
       original showed a 56-long/188-short imbalance; this tests whether that
       skew was a genuine regime effect (refinement won't fix it) or partly an
       artifact of a looser long trigger (refinement should partially rebalance it).
       Short-side threshold left unchanged at -0.2% for direct comparison.
    2. ADX floor raised from 20 to 23 on both sides for modestly higher selectivity.
    TP widened to 5.5% (from 5.0%) via STRATEGIES config; SL unchanged at 3.5%.
    """
    e9, e21, adx = ctx['e9'], ctx['e21'], ctx['adx']
    if i < 1 or None in (e9[i], e21[i], e9[i-1], e21[i-1], adx[i]):
        return None
    htf_ctx, htf_idx = bias_ctxs['1h']
    if htf_ctx is None or htf_idx is None:
        return None
    htf_e9, htf_e21 = htf_ctx['e9'], htf_ctx['e21']
    if htf_idx >= len(htf_e9) or htf_e9[htf_idx] is None or htf_e21[htf_idx] is None or not htf_e21[htf_idx]:
        return None
    htf_sep = (htf_e9[htf_idx] - htf_e21[htf_idx]) / htf_e21[htf_idx] * 100
    htf_bull = htf_sep > 0.35
    htf_bear = htf_sep < -0.2
    crossed_up = e9[i-1] <= e21[i-1] and e9[i] > e21[i]
    crossed_dn = e9[i-1] >= e21[i-1] and e9[i] < e21[i]
    if crossed_up and htf_bull and adx[i] > 23:
        return 'buy'
    if crossed_dn and htf_bear and adx[i] > 23:
        return 'sell'
    return None

SIGNAL_FUNCS = {
    'S8_MTF_CONFLUENCE': sig_s8_mtf_confluence,
    'S8R_MTF_CONFLUENCE_REFINED': sig_s8_refined_mtf_confluence,
}

# ── Backtest a single strategy against a single symbol's candles ──
def backtest_strategy(strat_id, symbol, ctx, all_ctxs):
    cfg = STRATEGIES[strat_id]
    tp_pct, sl_pct = cfg['tp'], cfg['sl']
    tf = cfg['tf']
    max_bars = MAX_BARS_BY_TF[tf]
    closes, highs, lows, opens = ctx['closes'], ctx['highs'], ctx['lows'], ctx['opens']
    ts = ctx['ts']
    n = len(closes)
    trades = []
    if n < MIN_BARS + 5:
        return trades

    sig_func = SIGNAL_FUNCS[strat_id]
    bias_tfs = cfg['bias_tfs']

    i = MIN_BARS
    while i < n - 1:
        bias_ctxs = {}
        for btf in bias_tfs:
            bctx = all_ctxs.get(btf)
            if bctx is None:
                bias_ctxs[btf] = (None, None)
            else:
                bidx = find_htf_index(bctx['ts'], ts[i])
                bias_ctxs[btf] = (bctx, bidx)

        sig = sig_func(ctx, i, bias_ctxs)

        if sig is None:
            i += 1
            continue

        entry_i = i + 1
        if entry_i >= n:
            break
        if sig == 'buy':
            entry_p = opens[entry_i] * (1 + FEE + SLIP)
            tp_p = entry_p * (1 + tp_pct)
            sl_p = entry_p * (1 - sl_pct)
        else:
            entry_p = opens[entry_i] * (1 - FEE - SLIP)
            tp_p = entry_p * (1 - tp_pct)
            sl_p = entry_p * (1 + sl_pct)

        exit_p, reason, bars_held, exit_ts = None, None, 0, ts[entry_i]
        j = entry_i
        while j < n:
            bars_held = j - entry_i
            if bars_held >= max_bars:
                exit_p, reason, exit_ts = closes[j], 'max_hold', ts[j]
                break
            if sig == 'buy':
                if lows[j] <= sl_p:
                    exit_p, reason, exit_ts = sl_p, 'sl', ts[j]
                    break
                if highs[j] >= tp_p:
                    exit_p, reason, exit_ts = tp_p, 'tp', ts[j]
                    break
            else:
                if highs[j] >= sl_p:
                    exit_p, reason, exit_ts = sl_p, 'sl', ts[j]
                    break
                if lows[j] <= tp_p:
                    exit_p, reason, exit_ts = tp_p, 'tp', ts[j]
                    break
            j += 1
        else:
            exit_p, reason, exit_ts = closes[n-1], 'end_of_data', ts[n-1]
            bars_held = n - 1 - entry_i

        notional = min(CAPITAL * RISK_PCT / sl_pct, CAPITAL * LEVERAGE)
        if sig == 'buy':
            gross = (exit_p - entry_p) / entry_p
        else:
            gross = (entry_p - exit_p) / entry_p
        net = gross - (FEE + SLIP) * 2
        pnl = notional * net

        trades.append({
            'symbol': symbol, 'side': sig, 'entry_ts': ts[entry_i], 'exit_ts': exit_ts,
            'entry_price': entry_p, 'exit_price': exit_p, 'pnl': pnl,
            'reason': reason, 'bars': bars_held,
        })
        i = entry_i + bars_held + 1

    return trades

# ── Stats ────────────────────────────────────────────────────────
def compute_stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
            'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'sharpe': 0.0, 'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }
    total = len(trades)
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    win_rate = len(wins) / total * 100
    gross_win = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
    net_pnl = sum(t['pnl'] for t in trades)
    avg_win = gross_win / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    expectancy = net_pnl / total

    sorted_trades = sorted(trades, key=lambda t: t['exit_ts'])
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for t in sorted_trades:
        equity += t['pnl']
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    # Sharpe ratio: based on per-trade PnL-as-%-of-capital returns, annualized by
    # trade frequency (trades/year). This is a trade-level Sharpe, not a daily-equity
    # Sharpe — appropriate here since trades are irregularly spaced, not daily bars.
    returns = [t['pnl'] / CAPITAL for t in trades]
    n_ret = len(returns)
    mean_ret = sum(returns) / n_ret
    var_ret = sum((r - mean_ret) ** 2 for r in returns) / n_ret if n_ret > 1 else 0.0
    std_ret = var_ret ** 0.5
    span_days = 1
    if len(sorted_trades) >= 2:
        span_seconds = sorted_trades[-1]['exit_ts'] - sorted_trades[0]['entry_ts']
        span_days = max(span_seconds / 86400, 1)
    trades_per_year = total / span_days * 365
    if std_ret > 0:
        sharpe = (mean_ret / std_ret) * (trades_per_year ** 0.5)
    else:
        sharpe = 0.0

    monthly = {}
    for t in trades:
        raw_ts = t['exit_ts']
        ts_val = raw_ts
        if ts_val >= 10**16:
            ts_val //= 1_000_000
        elif ts_val >= 10**12:
            ts_val //= 1000
        try:
            dt = datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            continue
        key = f"{dt.year:04d}-{dt.month:02d}"
        m = monthly.setdefault(key, {'pnl': 0.0, 'n': 0, 'w': 0})
        m['pnl'] += t['pnl']; m['n'] += 1
        if t['pnl'] > 0: m['w'] += 1

    per_coin = {}
    for t in trades:
        c = per_coin.setdefault(t['symbol'], {
            'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0,
            'gross_win': 0.0, 'gross_loss': 0.0, 'pf': 0.0,
        })
        c['pnl'] += t['pnl']; c['n'] += 1
        if t['pnl'] > 0:
            c['w'] += 1
            c['gross_win'] += t['pnl']
        else:
            c['gross_loss'] += abs(t['pnl'])
    for c in per_coin.values():
        c['wr'] = c['w'] / c['n'] * 100 if c['n'] else 0.0
        c['pf'] = round(c['gross_win'] / c['gross_loss'], 3) if c['gross_loss'] > 0 else (999.0 if c['gross_win'] > 0 else 0.0)
        del c['gross_win']; del c['gross_loss']

    return {
        'total': total, 'win_rate': round(win_rate, 2), 'profit_factor': round(pf, 3),
        'net_pnl': round(net_pnl, 2), 'max_drawdown': round(max_dd, 2),
        'avg_win': round(avg_win, 2), 'avg_loss': round(avg_loss, 2), 'expectancy': round(expectancy, 3),
        'sharpe': round(sharpe, 3),
        'longs': sum(1 for t in trades if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades if t['side'] == 'sell'),
        'monthly': monthly, 'per_coin': per_coin,
    }

# ── Shard runner ────────────────────────────────────────────────
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    t0 = time.time()
    all_trades = {sid: [] for sid in STRATEGIES}
    with_data = []

    for symbol in symbols:
        tfs = fetch_symbol_all_tfs(symbol)
        if not tfs:
            continue
        with_data.append(symbol)

        all_ctxs = {tf: build_ctx(rows) for tf, rows in tfs.items()}

        for strat_id, cfg in STRATEGIES.items():
            entry_tf = cfg['tf']
            ctx = all_ctxs.get(entry_tf)
            if ctx is None:
                continue
            missing_bias = any(btf not in all_ctxs for btf in cfg['bias_tfs'])
            if missing_bias:
                continue
            trades = backtest_strategy(strat_id, symbol, ctx, all_ctxs)
            all_trades[strat_id].extend(trades)

    out = {
        'shard': shard_idx,
        'symbols': symbols,
        'with_data': with_data,
        'trades_by_strategy': all_trades,
        'elapsed': round(time.time() - t0, 1),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(out, f)
    print(f"Shard {shard_idx} done: {len(with_data)}/{len(symbols)} coins with data, "
          f"{sum(len(v) for v in all_trades.values())} total trades, {out['elapsed']}s")

# ── Merge ───────────────────────────────────────────────────────
def merge_shards():
    combined_trades = {sid: [] for sid in STRATEGIES}
    all_symbols_attempted = []
    all_with_data = []

    for idx in range(NUM_SHARDS):
        fname = f'shard_{idx}.json'
        if not os.path.exists(fname):
            print(f"WARNING: {fname} missing, skipping")
            continue
        with open(fname) as f:
            data = json.load(f)
        all_symbols_attempted.extend(data['symbols'])
        all_with_data.extend(data['with_data'])
        for sid, trades in data['trades_by_strategy'].items():
            combined_trades[sid].extend(trades)

    if not all_with_data:
        with open('backtest_summary.txt', 'w') as f:
            f.write("ERROR: 0 symbols returned data across all shards.\n"
                    "Likely geo-block on Binance Vision or network issue in Actions runner.\n")
        with open('backtest_report.json', 'w') as f:
            json.dump({'error': 'no_data'}, f)
        return

    report = {
        'period': f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'symbols_attempted': len(set(all_symbols_attempted)),
        'symbols_with_data': len(set(all_with_data)),
        'leverage': LEVERAGE,
        'capital': CAPITAL,
        'strategies': {}
    }

    summary_lines = []
    summary_lines.append("=" * 70)
    summary_lines.append("MULTI-CONFIRMATION STRATEGY BACKTEST — SUMMARY REPORT")
    summary_lines.append("=" * 70)
    summary_lines.append(f"Period: {report['period']}")
    summary_lines.append(f"Symbols attempted: {report['symbols_attempted']}  |  With data: {report['symbols_with_data']}")
    summary_lines.append(f"Capital: ${CAPITAL:,.0f}  |  Leverage: {LEVERAGE}x  |  Risk/trade: {RISK_PCT*100:.2f}%")
    summary_lines.append(f"Fee: {FEE*100:.3f}%  |  Slippage: {SLIP*100:.3f}%")
    summary_lines.append("")

    ranked = []
    for sid, cfg in STRATEGIES.items():
        trades = combined_trades[sid]
        stats = compute_stats(trades)
        report['strategies'][sid] = {'name': cfg['name'], 'tf': cfg['tf'], 'bias_tfs': cfg['bias_tfs'],
                                       'tp': cfg['tp'], 'sl': cfg['sl'], 'stats': stats}
        ranked.append((sid, cfg, stats))

    ranked_sorted = sorted(ranked, key=lambda x: (x[2]['profit_factor'], x[2]['win_rate']), reverse=True)

    summary_lines.append("-" * 70)
    summary_lines.append("LEADERBOARD (ranked by profit factor)")
    summary_lines.append("-" * 70)
    summary_lines.append(f"{'Strategy':<48}{'TF':<6}{'Trades':<8}{'WR%':<8}{'PF':<8}{'Sharpe':<9}{'NetPnL':<12}{'MaxDD':<10}{'Verdict'}")
    for sid, cfg, stats in ranked_sorted:
        verdict = "USABLE" if (stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42 and stats['total'] >= 30) else "NOT USABLE"
        summary_lines.append(
            f"{cfg['name']:<48}{cfg['tf']:<6}{stats['total']:<8}{stats['win_rate']:<8}{stats['profit_factor']:<8}"
            f"{stats['sharpe']:<9}{stats['net_pnl']:<12}{stats['max_drawdown']:<10}{verdict}"
        )
    summary_lines.append("")

    for sid, cfg, stats in ranked_sorted:
        summary_lines.append("=" * 70)
        bias_str = "+".join(cfg['bias_tfs']) if cfg['bias_tfs'] else "none"
        summary_lines.append(f"{sid} — {cfg['name']}")
        summary_lines.append(f"(Entry TF: {cfg['tf']}, Bias TF(s): {bias_str}, TP: {cfg['tp']*100:.1f}%, SL: {cfg['sl']*100:.1f}%)")
        summary_lines.append("=" * 70)
        if stats['total'] == 0:
            summary_lines.append("No trades generated.")
            summary_lines.append("")
            continue
        summary_lines.append(f"Total trades: {stats['total']}  |  Win rate: {stats['win_rate']}%  |  Profit Factor: {stats['profit_factor']}  |  Sharpe: {stats['sharpe']}")
        summary_lines.append(f"Net PnL: ${stats['net_pnl']:,.2f}  |  Max Drawdown: ${stats['max_drawdown']:,.2f}")
        summary_lines.append(f"Avg Win: ${stats['avg_win']:,.2f}  |  Avg Loss: ${stats['avg_loss']:,.2f}  |  Expectancy: ${stats['expectancy']:,.3f}")
        summary_lines.append(f"Longs: {stats['longs']}  |  Shorts: {stats['shorts']}")
        verdict = "✅ USABLE" if (stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42 and stats['total'] >= 30) else "❌ NOT USABLE"
        summary_lines.append(f"RECOMMENDATION: {verdict} (threshold: PF>=1.5, WR>=42%, trades>=30)")
        summary_lines.append("")
        top_coins = sorted(stats['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)[:15]
        summary_lines.append("Top 15 coins by net PnL (with per-coin PF):")
        for sym, c in top_coins:
            summary_lines.append(f"  {sym:<18}trades={c['n']:<5}wr={c['wr']:5.1f}%   pf={c['pf']:<7}pnl=${c['pnl']:,.2f}")
        summary_lines.append("")
        summary_lines.append("Monthly PnL:")
        for month in sorted(stats['monthly'].keys()):
            md = stats['monthly'][month]
            summary_lines.append(f"  {month}:  pnl=${md['pnl']:,.2f}   trades={md['n']}   wins={md['w']}")
        summary_lines.append("")

    with open('backtest_summary.txt', 'w') as f:
        f.write("\n".join(summary_lines))
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f)

    print("Merge complete. backtest_summary.txt and backtest_report.json written.")

# ── Entry point ────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))
