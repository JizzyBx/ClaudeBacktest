"""
Strategy G — 4-Variant Backtest
Variants:
  A: TP 3.0%, SL 15.0%  (original VAR_D)
  B: TP 1.5%, SL 12.0%
  C: TP 1.0%, SL 10.0%
  D: TP 0.3% trailing-stop trigger, SL 15.0%

Period : Aug 2024 – Jul 2026 (24 months)
Capital: $10,000  |  Leverage: 5x  |  Timeframe: 15m
Signals: EMA50 slope (±0.05%, 10-bar) + EMA9/21 cross + ADX(14) >= 22
"""

import sys, os, json, csv, io, zipfile, time, math
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Coin universe (527 symbols, Chinese-char entries stripped) ──────────────
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
    "ANIMEUSDT","PIPPINUSDT","VVVUSDT","BERAUSDT","TSTUSDT","LAYERUSDT",
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
    "STABLEUSDT","JCTUSDT","ALLOUSDT","CLANKERUSDT","BEATUSDT","PIEVERSEUSDT",
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

# ── Global config ────────────────────────────────────────────────────────────
NUM_SHARDS  = 20
WORKERS     = 16
TIMEFRAME   = "15m"
START_YM    = (2024, 8)
END_YM      = (2026, 7)
CAPITAL     = 10_000.0
RISK_PCT    = 0.0075        # 0.75% risk per trade
FEE         = 0.0005        # 0.05%/side
SLIP        = 0.0002        # 0.02%/side
LEVERAGE    = 5
MAX_BARS    = 960           # 10 days at 15m
MIN_BARS    = 100           # warmup

# 4 variants
VARIANTS = {
    "A": {"tp": 0.030, "sl": 0.150, "trail": False, "trail_trigger": 0.0},
    "B": {"tp": 0.015, "sl": 0.120, "trail": False, "trail_trigger": 0.0},
    "C": {"tp": 0.010, "sl": 0.100, "trail": False, "trail_trigger": 0.0},
    "D": {"tp": 0.003, "sl": 0.150, "trail": True,  "trail_trigger": 0.003},
}

BASE_URL = (
    "https://data.binance.vision/data/futures/um/monthly/klines"
    "/{sym}/{tf}/{sym}-{tf}-{y:04d}-{m:02d}.zip"
)

# ── Data fetch ───────────────────────────────────────────────────────────────
def fetch_month(sym, y, m):
    url = BASE_URL.format(sym=sym, tf=TIMEFRAME, y=y, m=m)
    try:
        with urlopen(url, timeout=30) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = f.read().decode("utf-8")
        rows = []
        for line in text.splitlines():
            if not line or line.startswith("open"):
                continue
            p = line.split(",")
            if len(p) < 5:
                continue
            ts = int(p[0])
            if ts > 10**14:
                ts //= 1000
            rows.append((ts, float(p[1]), float(p[2]), float(p[3]), float(p[4])))
        return rows
    except HTTPError as e:
        if e.code == 404:
            return []
        return []
    except Exception:
        return []


def ym_range(start, end):
    y, m = start
    ey, em = end
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def fetch_symbol(sym):
    seen = set()
    rows = []
    for y, m in ym_range(START_YM, END_YM):
        for r in fetch_month(sym, y, m):
            if r[0] not in seen:
                seen.add(r[0])
                rows.append(r)
    rows.sort(key=lambda x: x[0])
    return rows

# ── Indicators (pure Python) ─────────────────────────────────────────────────
def ema(values, period):
    if len(values) < period:
        return [None] * len(values)
    k = 2.0 / (period + 1)
    result = [None] * (period - 1)
    cur = sum(values[:period]) / period
    result.append(cur)
    for v in values[period:]:
        cur = v * k + cur * (1 - k)
        result.append(cur)
    return result


def adx_calc(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 3:
        return [None] * n
    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        pdm = max(h - highs[i - 1], 0) if (h - highs[i - 1]) > (lows[i - 1] - l) else 0
        ndm = max(lows[i - 1] - l, 0) if (lows[i - 1] - l) > (h - highs[i - 1]) else 0
        tr_list.append(tr)
        pdm_list.append(pdm)
        ndm_list.append(ndm)

    def wilder_smooth(data, p):
        out = [None] * p
        s = sum(data[:p])
        out.append(s)
        for v in data[p:]:
            s = s - s / p + v
            out.append(s)
        return out

    atr  = wilder_smooth(tr_list,  period)
    pdi_ = wilder_smooth(pdm_list, period)
    ndi_ = wilder_smooth(ndm_list, period)

    dx_list = []
    for i in range(len(atr)):
        a = atr[i]
        if a is None or a == 0:
            dx_list.append(None)
            continue
        pdi = 100 * pdi_[i] / a
        ndi = 100 * ndi_[i] / a
        s = pdi + ndi
        dx_list.append(100 * abs(pdi - ndi) / s if s else 0)

    # ADX = Wilder smoothing of DX
    valid_dx = [v for v in dx_list if v is not None]
    if len(valid_dx) < period:
        return [None] * (n)

    adx_out = [None] * (period * 2)
    first_none = next(i for i, v in enumerate(dx_list) if v is not None)
    start = first_none + period - 1
    if start >= len(dx_list):
        return [None] * n

    adx_val = sum(v for v in dx_list[first_none:first_none + period] if v is not None) / period
    result_map = {}
    result_map[start + 1] = adx_val  # +1 because dx_list is 1-indexed vs closes
    idx = first_none + period
    bar = start + 2
    while idx < len(dx_list):
        if dx_list[idx] is not None:
            adx_val = (adx_val * (period - 1) + dx_list[idx]) / period
        result_map[bar] = adx_val
        idx += 1
        bar += 1

    out = []
    for i in range(n):
        out.append(result_map.get(i))
    return out

# ── Signal ───────────────────────────────────────────────────────────────────
def compute_signal(i, opens, highs, lows, closes, e9, e21, e50, adx_arr):
    if e9[i] is None or e21[i] is None or e50[i] is None:
        return None
    if e50[i - 10] is None or e50[i - 10] == 0:
        return None
    slope_pct = (e50[i] - e50[i - 10]) / e50[i - 10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05

    crossed_up   = e9[i] > e21[i] and e9[i - 1] <= e21[i - 1]
    crossed_down = e9[i] < e21[i] and e9[i - 1] >= e21[i - 1]

    if not crossed_up and not crossed_down:
        return None
    if trend_up and not crossed_up:
        return None
    if trend_down and not crossed_down:
        return None

    adx_val = adx_arr[i]
    if adx_val is None or adx_val < 22:
        return None

    return "buy" if crossed_up else "sell"

# ── Backtest single symbol × single variant ──────────────────────────────────
def backtest_symbol(sym, candles, var_cfg):
    if len(candles) < MIN_BARS + 10:
        return []

    tss    = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    e9   = ema(closes, 9)
    e21  = ema(closes, 21)
    e50  = ema(closes, 50)
    adx_arr = adx_calc(highs, lows, closes, 14)

    TP       = var_cfg["tp"]
    SL       = var_cfg["sl"]
    TRAIL    = var_cfg["trail"]
    T_TRIG   = var_cfg["trail_trigger"]

    # position sizing: risk-based capped by leverage
    notional = min(CAPITAL * RISK_PCT / SL, CAPITAL * LEVERAGE)

    trades  = []
    in_pos  = False
    pos     = {}

    n = len(candles)
    for i in range(MIN_BARS, n - 1):
        if not in_pos:
            sig = compute_signal(i, opens, highs, lows, closes, e9, e21, e50, adx_arr)
            if sig is None:
                continue
            # entry on bar i+1 open
            ep = opens[i + 1]
            if ep <= 0:
                continue
            if sig == "buy":
                entry_p = ep * (1 + FEE + SLIP)
                tp_p    = entry_p * (1 + TP)
                sl_p    = entry_p * (1 - SL)
            else:
                entry_p = ep * (1 - FEE - SLIP)
                tp_p    = entry_p * (1 - TP)
                sl_p    = entry_p * (1 + SL)
            in_pos = True
            pos = {
                "sym":    sym,
                "side":   sig,
                "entry_ts": tss[i + 1],
                "entry_p":  entry_p,
                "tp_p":     tp_p,
                "sl_p":     sl_p,
                "bar_in":   i + 1,
                "trail_active": False,
                "best_p":   entry_p,
            }
        else:
            j = i + 1  # current bar (we check this bar's h/l)
            if j >= n:
                break
            hi, lo = highs[j], lows[j]
            side   = pos["side"]
            ep_pos = pos["entry_p"]
            exit_p = None
            reason = None

            if TRAIL and not pos["trail_active"]:
                # check if trail trigger reached
                if side == "buy" and hi >= ep_pos * (1 + T_TRIG):
                    pos["trail_active"] = True
                    pos["best_p"] = hi
                elif side == "sell" and lo <= ep_pos * (1 - T_TRIG):
                    pos["trail_active"] = True
                    pos["best_p"] = lo

            if TRAIL and pos["trail_active"]:
                # update best price & trail SL
                if side == "buy":
                    pos["best_p"] = max(pos["best_p"], hi)
                    trail_sl = pos["best_p"] * (1 - SL)
                    pos["sl_p"] = max(pos["sl_p"], trail_sl)
                else:
                    pos["best_p"] = min(pos["best_p"], lo)
                    trail_sl = pos["best_p"] * (1 + SL)
                    pos["sl_p"] = min(pos["sl_p"], trail_sl)

            # SL check first (conservative)
            if side == "buy":
                if lo <= pos["sl_p"]:
                    exit_p, reason = pos["sl_p"], "sl"
                elif hi >= pos["tp_p"] and not TRAIL:
                    exit_p, reason = pos["tp_p"], "tp"
                elif TRAIL and pos["trail_active"] and lo <= pos["sl_p"]:
                    exit_p, reason = pos["sl_p"], "tp"  # trail exit counts as tp
            else:
                if hi >= pos["sl_p"]:
                    exit_p, reason = pos["sl_p"], "sl"
                elif lo <= pos["tp_p"] and not TRAIL:
                    exit_p, reason = pos["tp_p"], "tp"
                elif TRAIL and pos["trail_active"] and hi >= pos["sl_p"]:
                    exit_p, reason = pos["sl_p"], "tp"

            # For non-trail variants: re-check TP after SL didn't trigger
            if exit_p is None and not TRAIL:
                if side == "buy" and hi >= pos["tp_p"]:
                    exit_p, reason = pos["tp_p"], "tp"
                elif side == "sell" and lo <= pos["tp_p"]:
                    exit_p, reason = pos["tp_p"], "tp"

            # Max hold
            bars_held = j - pos["bar_in"]
            if exit_p is None and bars_held >= MAX_BARS:
                exit_p = closes[j]
                reason = "max_hold"

            if exit_p is None:
                continue

            # Compute PnL
            if side == "buy":
                gross = (exit_p - ep_pos) / ep_pos
            else:
                gross = (ep_pos - exit_p) / ep_pos
            net_pct = gross - (FEE + SLIP) * 2
            pnl = notional * net_pct

            trades.append({
                "symbol":      sym,
                "side":        side,
                "entry_ts":    pos["entry_ts"],
                "exit_ts":     tss[j],
                "entry_price": round(ep_pos, 8),
                "exit_price":  round(exit_p, 8),
                "pnl":         round(pnl, 4),
                "reason":      reason,
                "bars":        bars_held,
            })
            in_pos = False
            pos = {}

    # End of data: close any open position
    if in_pos and len(candles) > 0:
        j   = n - 1
        ep_pos = pos["entry_p"]
        exit_p = closes[j]
        side   = pos["side"]
        if side == "buy":
            gross = (exit_p - ep_pos) / ep_pos
        else:
            gross = (ep_pos - exit_p) / ep_pos
        net_pct = gross - (FEE + SLIP) * 2
        pnl = notional * net_pct
        bars_held = j - pos["bar_in"]
        trades.append({
            "symbol":      sym,
            "side":        side,
            "entry_ts":    pos["entry_ts"],
            "exit_ts":     tss[j],
            "entry_price": round(ep_pos, 8),
            "exit_price":  round(exit_p, 8),
            "pnl":         round(pnl, 4),
            "reason":      "end_of_data",
            "bars":        bars_held,
        })

    return trades

# ── Stats ────────────────────────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {
            "total": 0, "win_rate": 0.0, "profit_factor": 0.0,
            "net_pnl": 0.0, "max_drawdown": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "expectancy": 0.0,
            "longs": 0, "shorts": 0, "monthly": {}, "per_coin": {},
        }

    wins  = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses= [t["pnl"] for t in trades if t["pnl"] <= 0]
    longs = sum(1 for t in trades if t["side"] == "buy")
    shorts= sum(1 for t in trades if t["side"] == "sell")

    gross_win  = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    wr = 100 * len(wins) / len(trades)
    avg_win  = gross_win  / len(wins)   if wins   else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    expectancy = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)

    # Max drawdown on cumulative PnL curve
    sorted_t = sorted(trades, key=lambda x: x["exit_ts"])
    peak = cum = 0.0
    max_dd = 0.0
    for t in sorted_t:
        cum += t["pnl"]
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    # Monthly
    monthly = {}
    for t in sorted_t:
        ts_s = t["exit_ts"] / 1000 if t["exit_ts"] > 10**12 else t["exit_ts"]
        import datetime
        dt = datetime.datetime.utcfromtimestamp(ts_s)
        key = f"{dt.year:04d}-{dt.month:02d}"
        if key not in monthly:
            monthly[key] = {"pnl": 0.0, "n": 0, "w": 0}
        monthly[key]["pnl"] += t["pnl"]
        monthly[key]["n"]   += 1
        if t["pnl"] > 0:
            monthly[key]["w"] += 1

    # Per coin
    per_coin = {}
    for t in sorted_t:
        s = t["symbol"]
        if s not in per_coin:
            per_coin[s] = {"pnl": 0.0, "n": 0, "w": 0}
        per_coin[s]["pnl"] += t["pnl"]
        per_coin[s]["n"]   += 1
        if t["pnl"] > 0:
            per_coin[s]["w"] += 1
    for s in per_coin:
        d = per_coin[s]
        d["wr"] = round(100 * d["w"] / d["n"], 2) if d["n"] else 0.0

    return {
        "total":         len(trades),
        "win_rate":      round(wr, 4),
        "profit_factor": round(pf, 4),
        "net_pnl":       round(sum(t["pnl"] for t in trades), 2),
        "max_drawdown":  round(max_dd, 2),
        "avg_win":       round(avg_win, 4),
        "avg_loss":      round(avg_loss, 4),
        "expectancy":    round(expectancy, 4),
        "longs":         longs,
        "shorts":        shorts,
        "monthly":       monthly,
        "per_coin":      per_coin,
    }

# ── Shard runner ─────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    t0 = time.time()
    my_syms = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[shard {shard_idx}] {len(my_syms)} symbols", flush=True)

    # Fetch all candles in parallel
    candle_map = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut_map = {ex.submit(fetch_symbol, s): s for s in my_syms}
        for fut in as_completed(fut_map):
            s = fut_map[fut]
            try:
                candle_map[s] = fut.result()
            except Exception as e:
                print(f"  [shard {shard_idx}] fetch error {s}: {e}", flush=True)
                candle_map[s] = []

    with_data = [s for s in my_syms if len(candle_map[s]) >= MIN_BARS + 10]
    print(f"[shard {shard_idx}] {len(with_data)}/{len(my_syms)} have data", flush=True)

    # Run all 4 variants
    variant_results = {}
    for vname, vcfg in VARIANTS.items():
        all_trades = []
        for s in with_data:
            trades = backtest_symbol(s, candle_map[s], vcfg)
            all_trades.extend(trades)
        variant_results[vname] = {
            "trades": all_trades,
            "stats":  stats(all_trades),
        }
        print(
            f"  [shard {shard_idx}] VAR_{vname}: {len(all_trades)} trades "
            f"PF={variant_results[vname]['stats']['profit_factor']:.4f}",
            flush=True,
        )

    elapsed = time.time() - t0
    out = {
        "shard":     shard_idx,
        "symbols":   my_syms,
        "with_data": with_data,
        "variants":  variant_results,
        "elapsed":   round(elapsed, 2),
    }
    fname = f"shard_{shard_idx}.json"
    with open(fname, "w") as f:
        json.dump(out, f)
    print(f"[shard {shard_idx}] done in {elapsed:.1f}s → {fname}", flush=True)

# ── Merge ─────────────────────────────────────────────────────────────────────
def merge_shards():
    print("Merging shards...", flush=True)
    all_shards = []
    for i in range(NUM_SHARDS):
        fname = f"shard_{i}.json"
        if not os.path.exists(fname):
            print(f"  WARNING: {fname} missing", flush=True)
            continue
        with open(fname) as f:
            all_shards.append(json.load(f))

    total_syms  = sum(len(s["symbols"])   for s in all_shards)
    total_wdata = sum(len(s["with_data"]) for s in all_shards)

    combined = {}
    for vname in VARIANTS:
        all_trades = []
        for shard in all_shards:
            v = shard.get("variants", {}).get(vname, {})
            all_trades.extend(v.get("trades", []))
        combined[vname] = {
            "trades": all_trades,
            "stats":  stats(all_trades),
        }

    report = {
        "period":         f"{START_YM[0]:04d}-{START_YM[1]:02d} to {END_YM[0]:04d}-{END_YM[1]:02d}",
        "timeframe":      TIMEFRAME,
        "leverage":       LEVERAGE,
        "capital":        CAPITAL,
        "risk_pct":       RISK_PCT,
        "fee":            FEE,
        "slip":           SLIP,
        "symbols_tested": total_syms,
        "symbols_w_data": total_wdata,
        "variants":       {},
    }
    for vname, vcfg in VARIANTS.items():
        st = combined[vname]["stats"]
        report["variants"][vname] = {
            "config": vcfg,
            "stats":  st,
        }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Summary text
    lines = []
    lines.append("=" * 70)
    lines.append("STRATEGY G — 4-VARIANT BACKTEST SUMMARY")
    lines.append("=" * 70)
    lines.append(f"Period     : {report['period']}")
    lines.append(f"Timeframe  : {TIMEFRAME} | Leverage: {LEVERAGE}x | Capital: ${CAPITAL:,.0f}")
    lines.append(f"Risk/trade : {RISK_PCT*100:.2f}% | Fee: {FEE*100:.3f}%/side | Slip: {SLIP*100:.3f}%/side")
    lines.append(f"Symbols    : {total_wdata} with data / {total_syms} attempted")
    lines.append(f"Entry logic: EMA50 slope ±0.05% (10-bar) + EMA9/21 cross + ADX(14)>=22")
    lines.append("")

    variant_labels = {
        "A": "VAR_A — TP 3.0% / SL 15.0%  (original VAR_D)",
        "B": "VAR_B — TP 1.5% / SL 12.0%",
        "C": "VAR_C — TP 1.0% / SL 10.0%",
        "D": "VAR_D — TP 0.3% trailing (trail SL 15.0%)",
    }

    for vname in ["A", "B", "C", "D"]:
        st  = combined[vname]["stats"]
        pf  = st["profit_factor"]
        usable = "✅ USABLE" if pf >= 1.5 and st["win_rate"] >= 42 else "❌ NOT USABLE"
        lines.append("-" * 70)
        lines.append(variant_labels[vname])
        lines.append("-" * 70)
        lines.append(f"  RECOMMENDATION : {usable}")
        lines.append(f"  Total trades   : {st['total']:,}")
        lines.append(f"  Win rate       : {st['win_rate']:.2f}%")
        lines.append(f"  Profit factor  : {pf:.4f}")
        lines.append(f"  Net PnL        : ${st['net_pnl']:,.2f}")
        lines.append(f"  Max drawdown   : ${st['max_drawdown']:,.2f}")
        lines.append(f"  Avg win        : ${st['avg_win']:.4f}")
        lines.append(f"  Avg loss       : ${st['avg_loss']:.4f}")
        lines.append(f"  Expectancy     : ${st['expectancy']:.4f}")
        lines.append(f"  Longs/Shorts   : {st['longs']:,} / {st['shorts']:,}")
        lines.append("")

        # Top 50 coins by PnL
        pc = st["per_coin"]
        top50 = sorted(pc.items(), key=lambda x: x[1]["pnl"], reverse=True)[:50]
        lines.append(f"  Top 50 coins by Net PnL:")
        lines.append(f"  {'Symbol':<22} {'Trades':>7} {'WR%':>7} {'PnL':>12}")
        lines.append(f"  {'-'*22} {'-'*7} {'-'*7} {'-'*12}")
        for sym, d in top50:
            lines.append(f"  {sym:<22} {d['n']:>7} {d['wr']:>6.1f}% {d['pnl']:>12.2f}")
        lines.append("")

        # Monthly PnL
        monthly = st["monthly"]
        if monthly:
            lines.append(f"  Monthly PnL:")
            lines.append(f"  {'Month':<10} {'Trades':>7} {'Wins':>6} {'PnL':>12}")
            lines.append(f"  {'-'*10} {'-'*7} {'-'*6} {'-'*12}")
            for mo in sorted(monthly.keys()):
                d = monthly[mo]
                lines.append(
                    f"  {mo:<10} {d['n']:>7} {d['w']:>6} {d['pnl']:>12.2f}"
                )
        lines.append("")

    lines.append("=" * 70)
    lines.append("END OF REPORT")
    lines.append("=" * 70)

    summary = "\n".join(lines)
    with open("backtest_summary.txt", "w") as f:
        f.write(summary)

    print(summary)
    print("\nWrote backtest_report.json + backtest_summary.txt", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))

