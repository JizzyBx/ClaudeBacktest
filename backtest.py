"""
GMax V1 — Strategy G Backtest
==============================
Strategy : Variant G · 15m
  EMA50 slope filter (10-bar, ±0.05%)
  EMA9/21 crossover (direction must match slope)
  ADX(14) >= 22
  TP: 3.0% | SL: 15.0% | Max hold: 960 bars (10 days)

Data     : data.binance.vision futures monthly archives
Coins    : 527 USDT-M perpetuals (auto-skip 404s)
Period   : 2023-08 → 2025-07  (24 months)
Capital  : $10,000 shared equity
Risk     : 0.75% per trade
Fees     : 0.05% per side | Slippage: 0.02% per side
Leverage : 5x  (for position sizing only)
Max pos  : unlimited (matches live bot)
Workers  : 8 parallel (I/O-bound, GH Actions 2-core)
"""

import csv, io, json, os, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ─── CONFIG ────────────────────────────────────────────────────────────────

SYMBOLS = [
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
    "SOLVUSDT","ARCUSDT","AVAAIUSDT","TRUMPUSDT","MELANIAUSDT","VTHOUSD",
    "ANIMEUSDT","PIPPINUSDT","VVVUSDT","BERAUSUSDT","TSTUSDT","LAYERUSDT",
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
    "STABLEUSDT","JCTUSDT","ALLOUSDLT","CLANKERUSDT","BEATUSDT","PIEVERSEUSD",
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

START_YM   = (2023, 8)    # inclusive
END_YM     = (2025, 7)    # inclusive
TIMEFRAME  = "15m"

CAPITAL    = 10_000.0
RISK_PCT   = 0.0075       # 0.75% per trade
FEE        = 0.0005       # 0.05% per side
SLIP       = 0.0002       # 0.02% per side
LEVERAGE   = 5            # for notional sizing
TP_PCT     = 0.030        # 3.0%
SL_PCT     = 0.150        # 15.0%
MAX_BARS   = 960          # 10 days at 15m

WORKERS    = 8

BASE_URL   = "https://data.binance.vision/data/futures/um/monthly/klines"

# ─── HELPERS ───────────────────────────────────────────────────────────────

def months_in_range(start, end):
    y, m = start
    ey, em = end
    result = []
    while (y, m) <= (ey, em):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return result

ALL_MONTHS = months_in_range(START_YM, END_YM)

def fetch_month(symbol, year, month):
    fname = f"{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    url   = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{fname}"
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f, "utf-8")))
        candles = []
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts > 10**14:   # microseconds guard
                ts //= 1000
            candles.append((
                ts,
                float(row[1]),  # open
                float(row[2]),  # high
                float(row[3]),  # low
                float(row[4]),  # close
                float(row[5]),  # volume
            ))
        return candles
    except HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        return []

def fetch_symbol(symbol):
    all_candles = []
    for (y, m) in ALL_MONTHS:
        chunk = fetch_month(symbol, y, m)
        all_candles.extend(chunk)
    all_candles.sort(key=lambda x: x[0])
    # deduplicate by timestamp
    seen = set(); deduped = []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0]); deduped.append(c)
    return deduped

# ─── INDICATORS ─────────────────────────────────────────────────────────────

def ema(closes, period):
    k = 2.0 / (period + 1)
    out = [closes[0]]
    for v in closes[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up  and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))

    def ws(v, p):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r

    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st: return 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0
    adx_v = sum(dx[:period]) / period
    for d in dx[period:]:
        adx_v = (adx_v * (period - 1) + d) / period
    return max(0.0, min(100.0, adx_v))

# ─── SIGNAL ─────────────────────────────────────────────────────────────────

MIN_BARS = 70

def check_signal(i, e9, e21, e50, highs, lows, closes):
    """
    Returns 'buy', 'sell', or None.
    i = index of the LAST CLOSED bar (signal bar).
    Entry happens on i+1 open (next bar).
    """
    if i < 10: return None

    # Filter 1: EMA50 slope
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None

    # Filter 2: EMA9/21 crossover
    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if not crossed_up and not crossed_down:
        return None
    if trend_up and not crossed_up:
        return None
    if trend_down and not crossed_down:
        return None

    # Filter 3: ADX >= 22
    adx_v = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_v < 22:
        return None

    return 'buy' if crossed_up else 'sell'

# ─── BACKTEST SINGLE SYMBOL ─────────────────────────────────────────────────

COST_FACTOR = (1 + FEE + SLIP)  # applied both open and close

def backtest_symbol(symbol, candles, equity_ref):
    """
    equity_ref: list of [float] — mutable reference so we can
    read/write shared equity for position sizing.
    NOTE: for speed we keep this per-symbol and merge at the end.
    We use STARTING equity for all sizing (no live equity sharing
    across parallel workers — that would require locking).
    """
    if len(candles) < MIN_BARS + 2:
        return symbol, []

    timestamps = [c[0] for c in candles]
    opens      = [c[1] for c in candles]
    highs      = [c[2] for c in candles]
    lows       = [c[3] for c in candles]
    closes     = [c[4] for c in candles]

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    trades = []
    pos    = None   # active position dict

    for i in range(MIN_BARS, len(candles) - 1):
        # ── Manage open position ──────────────────────────────
        if pos is not None:
            bars_held = i - pos['entry_bar']
            # Check SL/TP on current bar OHLC (conservative: SL first)
            hi = highs[i]; lo = lows[i]; op = opens[i]
            entry = pos['entry_price']
            if pos['side'] == 'buy':
                sl_hit = lo <= pos['sl']
                tp_hit = hi >= pos['tp']
                # SL first
                if sl_hit:
                    exit_p = pos['sl']
                    gross  = (exit_p - entry) / entry
                    net    = gross - (FEE + SLIP) * 2
                    pnl    = pos['risk_usd'] / (SL_PCT * COST_FACTOR * COST_FACTOR) * net * entry / entry
                    # simpler: pnl = notional * net
                    pnl    = pos['notional'] * net
                    trades.append({**pos, 'exit_bar': i, 'exit_price': exit_p,
                                   'exit_ts': timestamps[i], 'pnl': pnl, 'reason': 'sl'})
                    pos = None; continue
                if tp_hit:
                    exit_p = pos['tp']
                    gross  = (exit_p - entry) / entry
                    net    = gross - (FEE + SLIP) * 2
                    pnl    = pos['notional'] * net
                    trades.append({**pos, 'exit_bar': i, 'exit_price': exit_p,
                                   'exit_ts': timestamps[i], 'pnl': pnl, 'reason': 'tp'})
                    pos = None; continue
            else:  # sell
                sl_hit = hi >= pos['sl']
                tp_hit = lo <= pos['tp']
                if sl_hit:
                    exit_p = pos['sl']
                    gross  = (entry - exit_p) / entry
                    net    = gross - (FEE + SLIP) * 2
                    pnl    = pos['notional'] * net
                    trades.append({**pos, 'exit_bar': i, 'exit_price': exit_p,
                                   'exit_ts': timestamps[i], 'pnl': pnl, 'reason': 'sl'})
                    pos = None; continue
                if tp_hit:
                    exit_p = pos['tp']
                    gross  = (entry - exit_p) / entry
                    net    = gross - (FEE + SLIP) * 2
                    pnl    = pos['notional'] * net
                    trades.append({**pos, 'exit_bar': i, 'exit_price': exit_p,
                                   'exit_ts': timestamps[i], 'pnl': pnl, 'reason': 'tp'})
                    pos = None; continue
            # Max hold
            if bars_held >= MAX_BARS:
                exit_p = closes[i]
                if pos['side'] == 'buy':
                    gross = (exit_p - entry) / entry
                else:
                    gross = (entry - exit_p) / entry
                net = gross - (FEE + SLIP) * 2
                pnl = pos['notional'] * net
                trades.append({**pos, 'exit_bar': i, 'exit_price': exit_p,
                               'exit_ts': timestamps[i], 'pnl': pnl, 'reason': 'max_hold'})
                pos = None

        # ── Check for new signal ──────────────────────────────
        if pos is not None:
            continue   # already in trade

        sig = check_signal(i, e9, e21, e50, highs, lows, closes)
        if sig is None:
            continue

        # Entry on NEXT bar open (i+1)
        entry_bar = i + 1
        entry_p   = opens[entry_bar] * (1 + FEE + SLIP if sig == 'buy' else 1 - FEE - SLIP)
        # Position sizing: risk 0.75% of starting capital per trade
        risk_usd  = CAPITAL * RISK_PCT
        # Notional = risk_usd / (SL_PCT as fraction) * leverage (capped by capital)
        notional  = min(risk_usd / SL_PCT, CAPITAL * LEVERAGE)

        if sig == 'buy':
            tp = entry_p * (1 + TP_PCT)
            sl = entry_p * (1 - SL_PCT)
        else:
            tp = entry_p * (1 - TP_PCT)
            sl = entry_p * (1 + SL_PCT)

        pos = {
            'symbol':      symbol,
            'side':        sig,
            'entry_bar':   entry_bar,
            'entry_ts':    timestamps[entry_bar],
            'entry_price': entry_p,
            'tp':          tp,
            'sl':          sl,
            'notional':    notional,
            'risk_usd':    risk_usd,
            'signal_bar':  i,
        }

    # Close any still-open position at last candle
    if pos is not None:
        i = len(candles) - 1
        exit_p = closes[i]
        if pos['side'] == 'buy':
            gross = (exit_p - pos['entry_price']) / pos['entry_price']
        else:
            gross = (pos['entry_price'] - exit_p) / pos['entry_price']
        net = gross - (FEE + SLIP) * 2
        pnl = pos['notional'] * net
        trades.append({**pos, 'exit_bar': i, 'exit_price': exit_p,
                       'exit_ts': timestamps[i], 'pnl': pnl, 'reason': 'end_of_data'})

    return symbol, trades

# ─── AGGREGATION ─────────────────────────────────────────────────────────────

def profit_factor(wins, losses):
    gp = sum(t['pnl'] for t in wins)
    gl = sum(abs(t['pnl']) for t in losses)
    if gl == 0:
        return float('inf') if gp > 0 else 0.0
    return round(gp / gl, 4)

def sharpe(returns, rf=0.0):
    if len(returns) < 2: return 0.0
    avg = sum(returns) / len(returns)
    var = sum((r - avg)**2 for r in returns) / (len(returns) - 1)
    std = var**0.5
    return round((avg - rf) / std * (252**0.5), 4) if std > 0 else 0.0

def sortino(returns, rf=0.0):
    if len(returns) < 2: return 0.0
    avg = sum(returns) / len(returns)
    neg = [r for r in returns if r < 0]
    if not neg: return float('inf')
    semi = (sum(r**2 for r in neg) / len(neg))**0.5
    return round((avg - rf) / semi * (252**0.5), 4) if semi > 0 else 0.0

def max_drawdown(trades_sorted):
    equity = 0.0; peak = 0.0; dd = 0.0
    for t in trades_sorted:
        equity += t['pnl']
        if equity > peak: peak = equity
        if peak > 0 and (peak - equity) > dd:
            dd = peak - equity
    return round(dd, 4)

def monthly_breakdown(trades):
    monthly = {}
    for t in trades:
        dt = datetime.fromtimestamp(t['exit_ts'] / 1000, tz=timezone.utc)
        key = dt.strftime('%Y-%m')
        monthly.setdefault(key, {'pnl': 0.0, 'trades': 0, 'wins': 0})
        monthly[key]['pnl']    += t['pnl']
        monthly[key]['trades'] += 1
        if t['pnl'] > 0:
            monthly[key]['wins'] += 1
    return dict(sorted(monthly.items()))

def aggregate_stats(all_trades):
    if not all_trades:
        return {}
    sorted_trades = sorted(all_trades, key=lambda t: t['exit_ts'])
    wins   = [t for t in sorted_trades if t['pnl'] > 0]
    losses = [t for t in sorted_trades if t['pnl'] < 0]
    total  = len(sorted_trades)
    wr     = len(wins) / total * 100 if total else 0

    net_pnl   = sum(t['pnl'] for t in sorted_trades)
    avg_win   = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
    avg_loss  = sum(t['pnl'] for t in losses) / len(losses) if losses else 0
    avg_dur_bars = sum(t['exit_bar'] - t['entry_bar'] for t in sorted_trades) / total
    returns = [t['pnl'] / CAPITAL for t in sorted_trades]

    # Long / short split
    longs  = [t for t in sorted_trades if t['side'] == 'buy']
    shorts = [t for t in sorted_trades if t['side'] == 'sell']

    # Streaks
    cur_w = cur_l = 0; max_w = max_l = 0
    for t in sorted_trades:
        if t['pnl'] > 0: cur_w += 1; cur_l = 0; max_w = max(max_w, cur_w)
        elif t['pnl'] < 0: cur_l += 1; cur_w = 0; max_l = max(max_l, cur_l)
        else: cur_w = cur_l = 0

    expectancy = (wr/100 * avg_win) + ((1 - wr/100) * avg_loss)

    return {
        'total_trades':  total,
        'win_rate':      round(wr, 2),
        'profit_factor': profit_factor(wins, losses),
        'net_pnl':       round(net_pnl, 4),
        'max_drawdown':  max_drawdown(sorted_trades),
        'sharpe':        sharpe(returns),
        'sortino':       sortino(returns),
        'avg_win':       round(avg_win, 4),
        'avg_loss':      round(avg_loss, 4),
        'expectancy':    round(expectancy, 4),
        'avg_duration_bars': round(avg_dur_bars, 1),
        'longs':  len(longs),
        'shorts': len(shorts),
        'long_wr':  round(len([t for t in longs if t['pnl']>0])/len(longs)*100, 2) if longs else 0,
        'short_wr': round(len([t for t in shorts if t['pnl']>0])/len(shorts)*100, 2) if shorts else 0,
        'max_win_streak':  max_w,
        'max_loss_streak': max_l,
    }

def per_coin_stats(all_trades):
    by_sym = {}
    for t in all_trades:
        by_sym.setdefault(t['symbol'], []).append(t)
    rows = []
    for sym, trades in by_sym.items():
        wins   = [t for t in trades if t['pnl'] > 0]
        losses = [t for t in trades if t['pnl'] < 0]
        total  = len(trades)
        pf     = profit_factor(wins, losses)
        wr     = round(len(wins)/total*100, 2) if total else 0
        net    = round(sum(t['pnl'] for t in trades), 4)
        rows.append({'symbol': sym, 'trades': total, 'win_rate': wr,
                     'profit_factor': pf, 'net_pnl': net})
    rows.sort(key=lambda x: x['profit_factor'], reverse=True)
    return rows

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print(f"[GMax V1 Backtest] {len(SYMBOLS)} symbols | {START_YM} → {END_YM} | {len(ALL_MONTHS)} months | {WORKERS} workers")
    print("Phase 1: Fetching candle data...")

    # Fetch all symbols in parallel
    raw_data = {}
    failed   = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_symbol, sym): sym for sym in SYMBOLS}
        done = 0
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                candles = fut.result()
                raw_data[sym] = candles
                if done % 50 == 0 or done == len(SYMBOLS):
                    print(f"  Fetched {done}/{len(SYMBOLS)} symbols...")
            except Exception as e:
                failed.append(sym)
                raw_data[sym] = []
                print(f"  FETCH ERROR {sym}: {e}")

    # Count how many had data
    with_data = [s for s in SYMBOLS if len(raw_data.get(s, [])) >= MIN_BARS + 2]
    no_data   = [s for s in SYMBOLS if len(raw_data.get(s, [])) < MIN_BARS + 2]
    print(f"  Symbols with usable data: {len(with_data)} | Skipped (404/too-short): {len(no_data)}")

    # Check for total failure (geo-block detection)
    if len(with_data) == 0:
        print("CRITICAL: 0 symbols returned data. Possible geo-block on data.binance.vision. Aborting.")
        return

    print("\nPhase 2: Running strategy backtests...")
    all_trades = []
    equity_ref = [CAPITAL]

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(backtest_symbol, sym, raw_data[sym], equity_ref): sym
                   for sym in with_data}
        done = 0
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                _, trades = fut.result()
                all_trades.extend(trades)
            except Exception as e:
                print(f"  BACKTEST ERROR {sym}: {e}")
            if done % 50 == 0 or done == len(with_data):
                print(f"  Backtested {done}/{len(with_data)} symbols...")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s | Total trades: {len(all_trades)}")

    # ── Aggregate ──────────────────────────────────────────────────────────
    agg   = aggregate_stats(all_trades)
    coins = per_coin_stats(all_trades)
    mthly = monthly_breakdown(all_trades)

    # Recommendation
    pf_ok = agg.get('profit_factor', 0)
    wr_ok = agg.get('win_rate', 0)
    meets = pf_ok >= 1.5 and wr_ok >= 42.0
    recommendation = "✅ USABLE — meets PF≥1.5 and WR≥42% targets" if meets else \
                     f"❌ NOT USABLE — PF={pf_ok}, WR={wr_ok}% (targets: PF≥1.5, WR≥42%)"

    # ── Print summary ──────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("AGGREGATE RESULTS — GMax V1 (Variant G · 15m)")
    print("="*60)
    for k, v in agg.items():
        print(f"  {k:<25} {v}")
    print(f"\n  {'RECOMMENDATION':<25} {recommendation}")

    print("\n" + "="*60)
    print("PER-COIN TABLE (sorted by Profit Factor, top 40)")
    print("="*60)
    print(f"  {'Symbol':<22} {'Trades':>6} {'WR%':>7} {'PF':>7} {'Net PnL':>10}")
    print("  " + "-"*57)
    for row in coins[:40]:
        pf_str = f"{row['profit_factor']:.3f}" if row['profit_factor'] != float('inf') else "∞"
        print(f"  {row['symbol']:<22} {row['trades']:>6} {row['win_rate']:>6.1f}% "
              f"{pf_str:>7} {row['net_pnl']:>10.2f}")
    if len(coins) > 40:
        print(f"  ... and {len(coins)-40} more coins")

    print("\n" + "="*60)
    print("MONTHLY PnL BREAKDOWN")
    print("="*60)
    for mo, v in mthly.items():
        bar = "█" * min(40, int(abs(v['pnl']) / max(1, max(abs(x['pnl']) for x in mthly.values())) * 40))
        sign = "+" if v['pnl'] >= 0 else ""
        print(f"  {mo}  {sign}{v['pnl']:>9.2f} USDT  trades={v['trades']}  wr={v['wins']/v['trades']*100:.0f}%  {bar}")

    print("\n" + "="*60)
    print("DATA COVERAGE")
    print("="*60)
    print(f"  Symbols attempted : {len(SYMBOLS)}")
    print(f"  Symbols with data : {len(with_data)}")
    print(f"  Symbols skipped   : {len(no_data)}")
    print(f"  Symbols traded    : {len(set(t['symbol'] for t in all_trades))}")
    print(f"  Fetch errors      : {len(failed)}")

    # ── Write outputs ──────────────────────────────────────────────────────
    summary_lines = [
        "GMax V1 Backtest — Summary",
        f"Period : {START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        f"Symbols: {len(with_data)} with data / {len(SYMBOLS)} attempted",
        "",
        "AGGREGATE",
    ]
    for k, v in agg.items():
        summary_lines.append(f"  {k}: {v}")
    summary_lines.append(f"\n  RECOMMENDATION: {recommendation}")
    summary_lines.append("\nPER-COIN (all, by PF)")
    for row in coins:
        pf_str = f"{row['profit_factor']:.3f}" if row['profit_factor'] != float('inf') else "inf"
        summary_lines.append(f"  {row['symbol']}: trades={row['trades']} wr={row['win_rate']}% pf={pf_str} pnl={row['net_pnl']}")
    summary_lines.append("\nMONTHLY")
    for mo, v in mthly.items():
        summary_lines.append(f"  {mo}: pnl={v['pnl']:.2f} trades={v['trades']} wr={v['wins']/v['trades']*100:.1f}%")

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(summary_lines))

    report = {
        "meta": {
            "strategy": "GMax V1 · Variant G · 15m",
            "period": f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
            "symbols_attempted": len(SYMBOLS),
            "symbols_with_data": len(with_data),
            "capital": CAPITAL,
            "risk_pct": RISK_PCT,
            "fee": FEE,
            "slip": SLIP,
            "leverage": LEVERAGE,
            "tp_pct": TP_PCT,
            "sl_pct": SL_PCT,
            "max_hold_bars": MAX_BARS,
        },
        "aggregate":   agg,
        "per_coin":    coins,
        "monthly":     mthly,
        "recommendation": recommendation,
        "trades": [
            {k: v for k, v in t.items() if k not in ('notional', 'risk_usd', 'signal_bar', 'entry_bar', 'exit_bar')}
            for t in sorted(all_trades, key=lambda x: x['exit_ts'])
        ],
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n✅ backtest_summary.txt and backtest_report.json written")
    print(f"   Runtime: {elapsed:.1f}s")

if __name__ == "__main__":
    main()
