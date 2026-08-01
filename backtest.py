"""
Backtest — Three TP/SL Variants (G, H, New)
Strategy: ADX≥22 + EMA50 slope + EMA9/21 crossover (15m)
Exit:      Fixed percentage TP/SL (no ATR, no trailing, no breakeven)
Variants:
  G   — TP 3%  / SL 15%
  H   — TP 4%  / SL 15%
  New — TP 4%  / SL 12%
Universe:  Full 592-coin list from data.binance.vision
Period:    Jul 2024 – Jun 2026 (2 years, monthly archives)
Capital:   $10,000 shared equity | Risk 0.75%/trade | Fee 0.05%+Slip 0.02% per side
Max pos:   6 concurrent (portfolio-wide)
"""

import csv, io, json, math, os, time, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
VARIANTS = {
    "G":   {"tp_pct": 3.0,  "sl_pct": 15.0},
    "H":   {"tp_pct": 4.0,  "sl_pct": 15.0},
    "New": {"tp_pct": 4.0,  "sl_pct": 12.0},
}

CAPITAL       = 10_000.0
RISK_PER_TRADE = 0.0075          # 0.75%
FEE_RATE      = 0.0005           # 0.05% per side
SLIP_RATE     = 0.0002           # 0.02% per side
MAX_POS       = 6
ADX_MIN       = 22
SLOPE_THRESH  = 0.05             # % over 10 bars for EMA50 slope
INTERVAL      = "15m"
MONTHS        = [
    (2024,7),(2024,8),(2024,9),(2024,10),(2024,11),(2024,12),
    (2025,1),(2025,2),(2025,3),(2025,4),(2025,5),(2025,6),
    (2025,7),(2025,8),(2025,9),(2025,10),(2025,11),(2025,12),
    (2026,1),(2026,2),(2026,3),(2026,4),(2026,5),(2026,6),
]
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
WORKERS  = 50

SYMBOLS = [
    "0GUSDT","1000000BOBUSDT","1000000MOGUSDT","1000BONKUSDT","1000CATUSDT",
    "1000CHEEMSUSDT","1000LUNCUSDT","1000PEPEUSDT","1000RATSUSDT","1000SATSUSDT",
    "1000SHIBUSDT","1000WHYUSDT","1000XECUSDT","1000XUSDT","1INCHUSDT",
    "1MBABYDOGEUSDT","2ZUSDT","42USDT","4USDT","A2ZUSDT",
    "AAVEUSDT","ACEUSDT","ACHUSDT","ACUUSDT","ACXUSDT",
    "ADAUSDT","AEROUSDT","AEVOUSDT","AGLDUSDT","AGTUSDT",
    "AI16ZUSDT","AIAUSDT","AINUSDT","AIOTUSDT","AIOUSDT",
    "AIUSDT","AIXBTUSDT","AKEUSDT","AKTUSDT","ALABUSDT",
    "ALCHUSDT","ALGOUSDT","ALICEUSDT","ALLUSDT","ALPACAUSDT",
    "ALPHAUSDT","ALPINEUSDT","ALTUSDT","AMBUSDT","AMDUSDT",
    "AMZNUSDT","ANIMEUSDT","ANKRUSDT","APEUSDT","API3USDT",
    "APRUSDT","APTUSDT","ARBUSDT","ARCUSDT","ARIAUSDT",
    "ARKMUSDT","ARKUSDT","ARPAUSDT","ARUSDT","ARXUSDT",
    "ASRUSDT","ASTERUSDT","ASTRUSDT","ATAUSDT","ATHUSDT",
    "ATOMUSDT","ATUSDT","AUCTIONUSDT","AUSDT","AVAAIUSDT",
    "AVAUSDT","AVAXUSDT","AVNTUSDT","AWEUSDT","AXLUSDT",
    "AXSUSDT","AZTECUSDT","B2USDT","B3USDT","BABYUSDT",
    "BADGERUSDT","BAKEUSDT","BALUSDT","BANANAS31USDT","BANANAUSDT",
    "BANDUSDT","BANKUSDT","BANUSDT","BASEDUSDT","BASUSDT",
    "BATUSDT","BBUSDT","BBXUSDT","BCHUSDT","BDXNUSDT",
    "BEAMXUSDT","BEATUSDT","BELUSDT","BERAUSDT","BICOUSDT",
    "BIDUSDT","BIGTIMEUSDT","BILLUSDT","BIOUSDT","BIRBUSDT",
    "BLESSUSDT","BLUAIUSDT","BLURUSDT","BLZUSDT","BMTUSDT",
    "BNBUSDT","BNTUSDT","BNXUSDT","BOBUSDT","BOMEUSDT",
    "BONDUSDT","BRETTUSDT","BREVUSDT","BROCCOLI714USDT","BROCCOLIF3BUSDT",
    "BRUSDT","BSBUSDT","BSVUSDT","BSWUSDT","BTCDOMUSDT",
    "BTCUSDT","BTRUSDT","BULLAUSDT","BUSDT","BZUSDT",
    "C98USDT","CAKEUSDT","CATIUSDT","CCUSDT","CELOUSDT",
    "CELRUSDT","CETUSUSDT","CFGUSDT","CFXUSDT","CGPTUSDT",
    "CHESSUSDT","CHILLGUYUSDT","CHIPUSDT","CHRUSDT","CKBUSDT",
    "CLOUSDT","CLUSDT","COAIUSDT","COLLECTUSDT","COMBOUSDT",
    "COMMONUSDT","COMPUSDT","COOKIEUSDT","COSUSDT","COTIUSDT",
    "COWUSDT","CRCLUSDT","CROSSUSDT","CRVUSDT","CTKUSDT",
    "CTRUSDT","CTSIUSDT","CUDISUSDT","CUSDT","CVCUSDT",
    "CVXUSDT","CYBERUSDT","DAMUSDT","DARUSDT","DASHUSDT",
    "DEEPUSDT","DEFIUSDT","DEGENUSDT","DEGOUSDT","DENTUSDT",
    "DEXEUSDT","DFUSDT","DIAUSDT","DISUSDT","DMCUSDT",
    "DODOXUSDT","DOGEUSDT","DOGSUSDT","DOLOUSDT","DOODUSDT",
    "DOTUSDT","DRIFTUSDT","DUSDT","DUSKUSDT","DYDXUSDT",
    "DYMUSDT","EDENUSDT","EDUUSDT","EGLDUSDT","EIGENUSDT",
    "ELSAUSDT","ENAUSDT","ENJUSDT","ENSOUSDT","ENSUSDT",
    "EPICUSDT","EPTUSDT","ESPORTSUSDT","ESPUSDT","ETCUSDT",
    "ETHFIUSDT","ETHUSDT","ETHWUSDT","EULUSDT","EVAAUSDT",
    "FARTCOINUSDT","FETUSDT","FFUSDT","FHEUSDT","FIDAUSDT",
    "FIGHTUSDT","FILUSDT","FIOUSDT","FISUSDT","FLMSUSDT",
    "FLNCUSDT","FLOCKUSDT","FLOWUSDT","FLUXUSDT","FOGOUSDT",
    "FOLKSUSDT","FORMUSDT","FRAXUSDT","FRONTUSDT","FTMUSDT",
    "FUNUSDT","FUSDT","FXSUSDT","GALAUSDT","GASUSDT",
    "GHSTUSDT","GIGGGEUSDT","GLMUSDT","GMTUSDT","GMXUSDT",
    "GOATUSDT","GPSUSDT","GRASSUSDT","GRIFFAINUSDT","GRTUSDT",
    "GTCUSDT","GUAUSDT","GUNUSDT","GUSDT","GWEIUSDT",
    "HAEDALUSDT","HANAUSDT","HBARUSDT","HEIUSDT","HEMIUSDT",
    "HFTUSDT","HIFIUSDT","HIGHUSDT","HIPPOUSDT","HIVEUSDT",
    "HMSTRUSDT","HOLOUSDT","HOMEUSDT","HOOKUSDT","HOTUSDT",
    "HUMAUSDT","HUSDT","HYPERUSDT","HYPEUSDT","ICNTUSDT",
    "ICPUSDT","ICXUSDT","IDOLUSDT","IDUSDT","ILVUSDT",
    "IMXUSDT","INITUSDT","INJUSDT","INUSDT","INXUSDT",
    "IOSTUSDT","IOTAUSDT","IOTXUSDT","IOUSDT","IPUSDT",
    "IRUSDT","IRYSUSDT","JASMYUSDT","JCTUSDT","JELLYJELLYUSDT",
    "JOEUSDT","JSTUSDT","JTOUSDT","JUPUSDT","KAIAUSDT",
    "KAITOUSDT","KASUSDT","KATUSDT","KAVAUSDT","KDAUSDT",
    "KERNELUSDT","KEYUSDT","KITEUSDT","KLAYUSDT","KMNOUSDT",
    "KNCUSDT","KOMAUSDT","KSMUSDT","LABUSDT","LAUSDT",
    "LAYERUSDT","LDOUSDT","LEVERUSDT","LIGHTUSDT","LINAUSDT",
    "LINEAUSDT","LINKUSDT","LISTAUSDT","LITUSDT","LOKAUSDT",
    "LOOMUSDT","LPTUSDT","LQTYUSDT","LRCUSDT","LSKUSDT",
    "LTCUSDT","LUMIAUSDT","LUNA2USDT","LYNUSDT","MAGICUSDT",
    "MANTAUSDT","MASKUSDT","MAVIAUSDT","MAVUSDT","MBOXUSDT",
    "MEGAUSDT","MELANIAUSDT","MEMEFIUSDT","MEMEUSDT","MERLUSDT",
    "METISUSDT","METUSDT","MEUSDT","MEWUSDT","MILKUSDT",
    "MINAUSDT","MIRAUSDT","MITOUSDT","MKRUSDT","MLNUSDT",
    "MMTUSDT","MOCAUSDT","MONUSDT","MOODENGUSDT","MORPHOUSDT",
    "MOVEUSDT","MOVRUSDT","MTLUSDT","MUSDT","MYROUSDT",
    "MYXUSDT","NAORISUSDT","NEARUSDT","NEIROETHUSDT","NEIROUSDT",
    "NEOUSDT","NEWTUSDT","NFPUSDT","NIGHTUSDT","NILUSDT",
    "NKNUSDT","NMRUSDT","NOKUSDT","NOMUSDT","NOTUSDT",
    "NOWUSDT","NTRNUSDT","NULSUSDT","NXPCUSDT","OBOLUSDT",
    "OGNUSDT","OGUSDT","OLUSDT","OMGUSDT","OMNIUSDT",
    "ONDOUSDT","ONEUSDT","ONGUSDT","ONTUSDT","ONUSDT",
    "OPENUSDT","OPNUSDT","OPUSDT","ORBSUSDT","ORCAUSDT",
    "OXTUSDT","PARTIUSDT","PAXGUSDT","PENDLEUSDT","PENGUUSDT",
    "PEOPLEUSDT","PERPUSDT","PHAUSDT","PHBUSDT","PIEVERSUSDT",
    "PIPPINUSDT","PIXELUSDT","PLAYUSDT","PLUMEUSDT","PNUTUSDT",
    "POLUSDT","POLYXUSDT","PONKEUSDT","POPCATUSDT","PORT3USDT",
    "PORTALUSDT","POWERUSDT","POWRUSDT","PROMPTUSDT","PROMUSDT",
    "PTBUSDT","PUFFERUSDT","PUMPBTCUSDT","PUMPUSDT","PUNDIXUSDT",
    "PYTHUSDT","QNTUSDT","QTUMUSDT","QUICKUSDT","QUSDT",
    "RAREUSDT","RAVEUSDT","RAYSOLUSDT","RECALLUSDT","REDUSDT",
    "REEFUSDT","REIUSDT","RENDERUSDT","RENUSDT","RESOLVUSDT",
    "REZUSDT","RIFUSDT","RIVERUSDT","RLSUSDT","RNDRUSDT",
    "RONINUSDT","ROSEUSDT","RPLUSDT","RSRUSDT","RUNEUSDT",
    "RVNUSDT","RVVUSDT","SAFEUSDT","SAGAUSDT","SAHARAUSDT",
    "SANDUSDT","SANTOSUSDT","SCRTUSDT","SCRUSDT","SEIUSDT",
    "SHELLUSDT","SIGNUSDT","SIRENUSDT","SKATEUSDT","SKLUSDT",
    "SKRUSDT","SKYAIUSDT","SKYUSDT","SLXUSDT","SNDKUSDT",
    "SOLUSDT","SOLVUSDT","SOMIUSDT","SONICUSDT","SOONUSDT",
    "SOPHUSDT","SOXLUSDT","SPACEUSDT","SPCXUSDT","SPELLUSDT",
    "SPKUSDT","SPORTFUNUSDT","SQDUSDT","SSVUSDT","STABLEUSDT",
    "STBLUSDT","STEEMUSDT","STGUSDT","STMXUSDT","STORJUSDT",
    "STOUSDT","STRKUSDT","STXUSDT","SUIUSDT","SUNUSDT",
    "SUSDT","SWARMSUSDT","SWELLUSDT","SYNUSDT","SYRUPUSDT",
    "SYSUSDT","TACUSDT","TAGUSDT","TAIKOUSDT","TAKEUSDT",
    "TANSSIUSDT","TAOUSDT","THEUSDT","TIAUSDT","TLMUSDT",
    "TNSRUSDT","TOKENUSDT","TONUSDT","TRADOORUSDT","TRBUSDT",
    "TREEUSDT","TRIAUSDT","TROYUSDT","TRUMPUSDT","TRUSTUSDT",
    "TRUTHUSDT","TRUUSDT","TRXUSDT","TURBOUSDT","TURTLEUSDT",
    "TUTUSDT","TWTUSDT","UAIUSDT","UBUSDT","UMAUSDT",
    "USELESSUSDT","USTCUSDT","USUALUSDT","USUSDT","UXLINKUSDT",
    "VANAUSDT","VANRYUSDT","VELVETUSDT","VETUSDT","VICUSDT",
    "VIDTUSDT","VINEUSDT","VIRTUALUSDT","VOXELUSDT","VTHOUSDT",
    "VVVUSDT","WAXPUSDT","WCTUSDT","WETUSDT","WIFUSDT",
    "WLDUSDT","WLFIUSDT","WOOUSDT","WUSDT","XAGUSDT",
    "XAIUSDT","XANUSDT","XCNUSDT","XEMUSDT","XLMUSDT",
    "XMRUSDT","XNYUSDT","XPINUSDT","XPLUSDT","XRPUSDT",
    "XTZUSDT","XVGUSDT","XVSUSDT","YALAUSDT","YBUSDT",
    "YFIUSDT","YGGUSDT","ZECUSDT","ZENUSDT","ZEREBROUSDT",
    "ZETAUSDT","ZKJUSDT","ZKPUSDT","ZKUSDT","ZORAUSDT",
    "ZROUSDT","ZRXUSDT",
]

# ── Indicators (stdlib only) ───────────────────────────────────────────────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up  and down > 0 else 0.0)
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1]),
        ))
    def ws(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st:
        return 0.0
    pdi = [100 * p / t if t else 0 for p, t in zip(sp, st)]
    mdi = [100 * m / t if t else 0 for m, t in zip(sm, st)]
    dx  = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period:
        return 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]:
        adx = (adx * (period - 1) + d) / period
    return max(0.0, min(100.0, adx))

# ── Data fetch ─────────────────────────────────────────────────────────────────
def fetch_symbol_candles(symbol):
    """Download all monthly klines for symbol, return list of (ts_ms, o, h, l, c)."""
    candles = []
    for year, month in MONTHS:
        url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{year}-{month:02d}.zip"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = resp.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    for row in reader:
                        if not row or not row[0].isdigit():
                            continue
                        ts = int(row[0])
                        # Guard microsecond timestamps (Binance changed spot in 2025)
                        if ts > 10**14:
                            ts //= 1000
                        candles.append((
                            ts,
                            float(row[1]),  # open
                            float(row[2]),  # high
                            float(row[3]),  # low
                            float(row[4]),  # close
                        ))
        except Exception:
            pass  # 404 = coin didn't exist that month; any other error = skip
    candles.sort(key=lambda x: x[0])
    return candles

# ── Per-symbol backtest ────────────────────────────────────────────────────────
def backtest_symbol(args):
    symbol, variants_cfg = args
    candles = fetch_symbol_candles(symbol)
    if len(candles) < 100:
        return symbol, None  # not enough data

    ts_list = [c[0] for c in candles]
    opens   = [c[1] for c in candles]
    highs   = [c[2] for c in candles]
    lows    = [c[3] for c in candles]
    closes  = [c[4] for c in candles]
    n       = len(candles)

    # Pre-compute indicators (need at least 60 bars warmup)
    WARMUP = 60
    e9_all  = ema(closes, 9)
    e21_all = ema(closes, 21)
    e50_all = ema(closes, 50)

    # Results per variant: list of trade dicts
    results = {vname: [] for vname in variants_cfg}

    # Walk bars — signals on closed candles (index i means candle i just closed,
    # we'd enter at open of candle i+1, but we simulate entry at close of i)
    for i in range(WARMUP, n - 1):
        # ADX over last 60 bars up to i (inclusive)
        seg_h = highs[max(0, i-59):i+1]
        seg_l = lows[max(0, i-59):i+1]
        seg_c = closes[max(0, i-59):i+1]
        adx = adx_calc(seg_h, seg_l, seg_c, 14)

        # EMA50 slope: % change over last 10 bars
        if i < 10:
            continue
        slope_pct = (e50_all[i] - e50_all[i-10]) / e50_all[i-10] * 100

        trend_up   = slope_pct >  SLOPE_THRESH
        trend_down = slope_pct < -SLOPE_THRESH

        # EMA9/21 crossover (this bar vs previous bar)
        crossed_up   = e9_all[i] > e21_all[i]   and e9_all[i-1] <= e21_all[i-1]
        crossed_down = e9_all[i] < e21_all[i]   and e9_all[i-1] >= e21_all[i-1]

        adx_ok = adx >= ADX_MIN

        signal = None
        if adx_ok and trend_up   and crossed_up:   signal = "buy"
        if adx_ok and trend_down and crossed_down:  signal = "sell"

        if not signal:
            continue

        entry = closes[i]  # enter at close of signal bar

        for vname, vcfg in variants_cfg.items():
            tp_pct = vcfg["tp_pct"] / 100.0
            sl_pct = vcfg["sl_pct"] / 100.0

            if signal == "buy":
                tp_price = entry * (1 + tp_pct)
                sl_price = entry * (1 - sl_pct)
            else:
                tp_price = entry * (1 - tp_pct)
                sl_price = entry * (1 + sl_pct)

            # Walk forward to find exit
            outcome = None
            exit_price = None
            exit_bar   = None
            for j in range(i + 1, n):
                h = highs[j]; l = lows[j]
                if signal == "buy":
                    if l <= sl_price:
                        outcome    = "sl"
                        exit_price = sl_price
                        exit_bar   = j
                        break
                    if h >= tp_price:
                        outcome    = "tp"
                        exit_price = tp_price
                        exit_bar   = j
                        break
                else:
                    if h >= sl_price:
                        outcome    = "sl"
                        exit_price = sl_price
                        exit_bar   = j
                        break
                    if l <= tp_price:
                        outcome    = "tp"
                        exit_price = tp_price
                        exit_bar   = j
                        break

            if outcome is None:
                # Never hit TP or SL — close at last candle
                outcome    = "timeout"
                exit_price = closes[-1]
                exit_bar   = n - 1

            results[vname].append({
                "signal":     signal,
                "entry_bar":  i,
                "exit_bar":   exit_bar,
                "entry":      entry,
                "exit":       exit_price,
                "outcome":    outcome,
                "entry_ts":   ts_list[i],
                "exit_ts":    ts_list[exit_bar],
            })

    return symbol, results

# ── Portfolio simulation (apply position cap + equity sizing) ─────────────────
def simulate_portfolio(all_symbol_trades, variant_name, tp_pct, sl_pct):
    """
    Merge all raw trade signals for one variant across all symbols,
    apply MAX_POS cap and shared equity sizing, return aggregate stats.
    """
    # Flatten all signals into one list sorted by entry_ts
    raw = []
    for sym, trades in all_symbol_trades.items():
        for t in trades:
            raw.append({**t, "symbol": sym})
    raw.sort(key=lambda x: x["entry_ts"])

    equity    = CAPITAL
    open_pos  = {}   # symbol -> {entry, tp, sl, signal, risk_dollar, entry_ts}
    closed    = []
    rejected_cap = 0

    for sig in raw:
        sym = sig["symbol"]

        # Close any positions that have exited before this signal's entry
        to_close = []
        for s, pos in open_pos.items():
            if pos["exit_ts"] <= sig["entry_ts"]:
                to_close.append(s)
        for s in to_close:
            pos = open_pos.pop(s)
            raw_ret  = (pos["exit_price"] - pos["entry"]) / pos["entry"]
            if pos["signal"] == "sell":
                raw_ret = -raw_ret
            cost = (FEE_RATE + SLIP_RATE) * 2
            net_ret  = raw_ret - cost
            pnl      = pos["risk_dollar"] * (net_ret / (pos["sl_pct"]))
            equity  += pnl
            win      = pnl > 0
            closed.append({
                "symbol":   s,
                "signal":   pos["signal"],
                "outcome":  pos["outcome"],
                "pnl":      pnl,
                "win":      win,
                "entry_ts": pos["entry_ts"],
                "exit_ts":  pos["exit_ts"],
                "bars":     pos.get("bars", 0),
            })

        if sym in open_pos:
            continue  # already in this coin

        if len(open_pos) >= MAX_POS:
            rejected_cap += 1
            continue

        # Size position: risk 0.75% of current equity
        risk_dollar = equity * RISK_PER_TRADE
        entry       = sig["entry"]

        if sig["signal"] == "buy":
            tp_p = entry * (1 + tp_pct / 100)
            sl_p = entry * (1 - sl_pct / 100)
        else:
            tp_p = entry * (1 - tp_pct / 100)
            sl_p = entry * (1 + sl_pct / 100)

        open_pos[sym] = {
            "signal":      sig["signal"],
            "entry":       entry,
            "exit_price":  sig["exit"],
            "outcome":     sig["outcome"],
            "tp":          tp_p,
            "sl":          sl_p,
            "sl_pct":      sl_pct / 100,
            "risk_dollar": risk_dollar,
            "entry_ts":    sig["entry_ts"],
            "exit_ts":     sig["exit_ts"],
            "bars":        sig["exit_bar"] - sig["entry_bar"],
        }

    # Close remaining open positions at their recorded exit
    for s, pos in open_pos.items():
        raw_ret = (pos["exit_price"] - pos["entry"]) / pos["entry"]
        if pos["signal"] == "sell":
            raw_ret = -raw_ret
        cost    = (FEE_RATE + SLIP_RATE) * 2
        net_ret = raw_ret - cost
        pnl     = pos["risk_dollar"] * (net_ret / pos["sl_pct"])
        equity += pnl
        closed.append({
            "symbol":   s,
            "signal":   pos["signal"],
            "outcome":  pos["outcome"],
            "pnl":      pnl,
            "win":      pnl > 0,
            "entry_ts": pos["entry_ts"],
            "exit_ts":  pos["exit_ts"],
            "bars":     pos.get("bars", 0),
        })

    if not closed:
        return None, rejected_cap

    # ── Aggregate stats ────────────────────────────────────────────────────────
    wins   = [t for t in closed if t["win"]]
    losses = [t for t in closed if not t["win"]]
    total  = len(closed)
    wr     = len(wins) / total * 100 if total else 0

    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss   = abs(sum(t["pnl"] for t in losses))
    pf           = gross_profit / gross_loss if gross_loss else float("inf")
    net_pnl      = sum(t["pnl"] for t in closed)

    avg_win  = gross_profit / len(wins)   if wins   else 0
    avg_loss = gross_loss   / len(losses) if losses else 0
    expectancy = (wr / 100 * avg_win) - ((1 - wr / 100) * avg_loss)

    # Max drawdown
    peak = CAPITAL; max_dd = 0.0; running = CAPITAL
    for t in sorted(closed, key=lambda x: x["exit_ts"]):
        running += t["pnl"]
        if running > peak:
            peak = running
        dd = (peak - running) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Sharpe / Sortino (daily PnL buckets)
    daily = {}
    for t in closed:
        day = t["exit_ts"] // 86400000
        daily[day] = daily.get(day, 0) + t["pnl"]
    vals = list(daily.values())
    if len(vals) > 1:
        mean = sum(vals) / len(vals)
        var  = sum((v - mean) ** 2 for v in vals) / len(vals)
        std  = var ** 0.5
        down = [v for v in vals if v < 0]
        dvar = sum(v ** 2 for v in down) / len(down) if down else 0
        dstd = dvar ** 0.5
        sharpe  = (mean / std  * (252 ** 0.5)) if std  else 0
        sortino = (mean / dstd * (252 ** 0.5)) if dstd else 0
    else:
        sharpe = sortino = 0

    # Streaks
    streak = best_w = best_l = cur = 0
    prev_win = None
    for t in sorted(closed, key=lambda x: x["exit_ts"]):
        w = t["win"]
        if w == prev_win:
            cur += 1
        else:
            cur = 1
        if w:
            best_w = max(best_w, cur)
        else:
            best_l = max(best_l, cur)
        prev_win = w

    # Long / short split
    longs  = [t for t in closed if t["signal"] == "buy"]
    shorts = [t for t in closed if t["signal"] == "sell"]
    lwr = sum(1 for t in longs  if t["win"]) / len(longs)  * 100 if longs  else 0
    swr = sum(1 for t in shorts if t["win"]) / len(shorts) * 100 if shorts else 0

    # Avg duration in bars
    avg_bars = sum(t["bars"] for t in closed) / total if total else 0

    # Monthly PnL
    monthly = {}
    for t in closed:
        dt  = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] = monthly.get(key, 0) + t["pnl"]

    # Per-coin table
    coin_stats = {}
    for t in closed:
        sym = t["symbol"]
        if sym not in coin_stats:
            coin_stats[sym] = {"trades": 0, "wins": 0, "pnl": 0.0}
        coin_stats[sym]["trades"] += 1
        coin_stats[sym]["pnl"]   += t["pnl"]
        if t["win"]:
            coin_stats[sym]["wins"] += 1
    for sym, cs in coin_stats.items():
        cs["wr"] = cs["wins"] / cs["trades"] * 100 if cs["trades"] else 0
        cs_wins   = [t["pnl"] for t in closed if t["symbol"] == sym and t["win"]]
        cs_losses = [abs(t["pnl"]) for t in closed if t["symbol"] == sym and not t["win"]]
        gp = sum(cs_wins); gl = sum(cs_losses)
        cs["pf"] = gp / gl if gl else float("inf")

    coin_table = sorted(coin_stats.items(), key=lambda x: x[1]["pf"], reverse=True)

    agg = {
        "variant":        variant_name,
        "tp_pct":         tp_pct,
        "sl_pct":         sl_pct,
        "total_trades":   total,
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(wr, 2),
        "profit_factor":  round(pf, 4),
        "net_pnl":        round(net_pnl, 2),
        "final_equity":   round(equity, 2),
        "max_drawdown":   round(max_dd, 2),
        "sharpe":         round(sharpe, 3),
        "sortino":        round(sortino, 3),
        "avg_win":        round(avg_win, 4),
        "avg_loss":       round(avg_loss, 4),
        "expectancy":     round(expectancy, 4),
        "avg_bars":       round(avg_bars, 1),
        "long_trades":    len(longs),
        "long_wr":        round(lwr, 2),
        "short_trades":   len(shorts),
        "short_wr":       round(swr, 2),
        "best_win_streak":  best_w,
        "best_loss_streak": best_l,
        "rejected_by_cap":  rejected_cap,
        "usable":         pf >= 1.5 and wr >= 42,
    }
    return {
        "aggregate":  agg,
        "per_coin":   coin_table,
        "monthly":    sorted(monthly.items()),
        "trades":     closed,
    }, rejected_cap

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"{'='*60}")
    print(f"  Backtest — Variants G / H / New")
    print(f"  Universe: {len(SYMBOLS)} coins | Period: Jul 2024–Jun 2026")
    print(f"  Strategy: ADX≥{ADX_MIN} + EMA50 slope + EMA9/21 cross (15m)")
    print(f"{'='*60}")

    # Phase 1 — Parallel data fetch + per-symbol raw signal generation
    print(f"\n[Phase 1] Downloading data & generating signals ({WORKERS} workers)…")
    args = [(sym, VARIANTS) for sym in SYMBOLS]

    symbol_results = {}  # sym -> {variant -> [trades]}
    done = 0
    failed = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(backtest_symbol, a): a[0] for a in args}
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                sym_out, res = fut.result()
                if res is None:
                    failed += 1
                    if done % 50 == 0 or done == len(SYMBOLS):
                        print(f"  [{done}/{len(SYMBOLS)}] {failed} skipped (no data)")
                else:
                    symbol_results[sym_out] = res
                    if done % 50 == 0 or done == len(SYMBOLS):
                        print(f"  [{done}/{len(SYMBOLS)}] {len(symbol_results)} symbols with data")
            except Exception as e:
                failed += 1
                print(f"  ERROR {sym}: {e}")

    # Abort-if-all-failed guard
    if len(symbol_results) == 0:
        print("\n⛔ ABORT: 0 symbols returned data — data source is likely blocked.")
        print("   Check if data.binance.vision is accessible from this runner.")
        return

    print(f"\n  ✅ {len(symbol_results)} symbols loaded | {failed} skipped")

    # Phase 2 — Portfolio simulation per variant
    print(f"\n[Phase 2] Running portfolio simulation for each variant…\n")

    report = {}
    summary_lines = []

    for vname, vcfg in VARIANTS.items():
        tp = vcfg["tp_pct"]; sl = vcfg["sl_pct"]
        print(f"  Variant {vname}: TP {tp}% / SL {sl}%")

        # Build per-variant symbol trade map
        sym_trades = {}
        for sym, vdata in symbol_results.items():
            if vname in vdata and vdata[vname]:
                sym_trades[sym] = vdata[vname]

        result, rej = simulate_portfolio(sym_trades, vname, tp, sl)
        if result is None:
            print(f"    ⚠ No trades generated for variant {vname}\n")
            continue

        agg = result["aggregate"]
        verdict = "✅ USABLE" if agg["usable"] else "❌ NOT USABLE"
        print(f"    Trades: {agg['total_trades']} | WR: {agg['win_rate']}% | PF: {agg['profit_factor']} | Net PnL: ${agg['net_pnl']} | DD: {agg['max_drawdown']}% → {verdict}\n")

        report[vname] = result
        summary_lines.append(
            f"Variant {vname} (TP {tp}% SL {sl}%): "
            f"{agg['total_trades']} trades | WR {agg['win_rate']}% | PF {agg['profit_factor']} | "
            f"Net ${agg['net_pnl']} | DD {agg['max_drawdown']}% | {verdict}"
        )

    elapsed = time.time() - t0

    # ── Write backtest_summary.txt ─────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write("BACKTEST SUMMARY\n")
        f.write("=" * 60 + "\n")
        f.write(f"Strategy : ADX≥{ADX_MIN} + EMA50 slope + EMA9/21 crossover\n")
        f.write(f"Timeframe: 15m | Period: Jul 2024 – Jun 2026\n")
        f.write(f"Universe : {len(SYMBOLS)} coins ({len(symbol_results)} with data)\n")
        f.write(f"Capital  : ${CAPITAL:,.0f} | Risk/trade: {RISK_PER_TRADE*100}%\n")
        f.write(f"Fees     : {FEE_RATE*100}% + {SLIP_RATE*100}% slip per side\n")
        f.write(f"Max pos  : {MAX_POS} | Run time: {elapsed:.0f}s\n")
        f.write("=" * 60 + "\n\n")

        for vname, result in report.items():
            agg = result["aggregate"]
            f.write(f"── VARIANT {vname} (TP {agg['tp_pct']}% / SL {agg['sl_pct']}%) ──\n")
            f.write(f"Total Trades   : {agg['total_trades']}\n")
            f.write(f"Wins / Losses  : {agg['wins']} / {agg['losses']}\n")
            f.write(f"Win Rate       : {agg['win_rate']}%\n")
            f.write(f"Profit Factor  : {agg['profit_factor']}\n")
            f.write(f"Net PnL        : ${agg['net_pnl']}\n")
            f.write(f"Final Equity   : ${agg['final_equity']}\n")
            f.write(f"Max Drawdown   : {agg['max_drawdown']}%\n")
            f.write(f"Sharpe         : {agg['sharpe']}\n")
            f.write(f"Sortino        : {agg['sortino']}\n")
            f.write(f"Avg Win        : ${agg['avg_win']}\n")
            f.write(f"Avg Loss       : ${agg['avg_loss']}\n")
            f.write(f"Expectancy     : ${agg['expectancy']}\n")
            f.write(f"Avg Duration   : {agg['avg_bars']} bars\n")
            f.write(f"Longs          : {agg['long_trades']} trades | WR {agg['long_wr']}%\n")
            f.write(f"Shorts         : {agg['short_trades']} trades | WR {agg['short_wr']}%\n")
            f.write(f"Best Win Streak: {agg['best_win_streak']}\n")
            f.write(f"Best Loss Streak:{agg['best_loss_streak']}\n")
            f.write(f"Rejected (cap) : {agg['rejected_by_cap']}\n")
            f.write(f"VERDICT        : {'✅ USABLE' if agg['usable'] else '❌ NOT USABLE'}\n")
            f.write(f"               (PF≥1.5 and WR≥42% required)\n\n")

            f.write("Monthly PnL:\n")
            for mo, pnl in result["monthly"]:
                f.write(f"  {mo}: ${pnl:+.2f}\n")
            f.write("\n")

            f.write("Top 20 Coins by Profit Factor:\n")
            for sym, cs in result["per_coin"][:20]:
                f.write(f"  {sym:25s} PF:{cs['pf']:.3f}  WR:{cs['wr']:.1f}%  Trades:{cs['trades']}  PnL:${cs['pnl']:.2f}\n")
            f.write("\n")

            f.write("Bottom 10 Coins by Profit Factor:\n")
            for sym, cs in result["per_coin"][-10:]:
                f.write(f"  {sym:25s} PF:{cs['pf']:.3f}  WR:{cs['wr']:.1f}%  Trades:{cs['trades']}  PnL:${cs['pnl']:.2f}\n")
            f.write("\n" + "=" * 60 + "\n\n")

        f.write("QUICK COMPARISON\n")
        f.write("-" * 60 + "\n")
        for line in summary_lines:
            f.write(line + "\n")
        f.write(f"\nCompleted in {elapsed:.0f}s\n")

    # ── Write backtest_report.json ─────────────────────────────────────────────
    json_out = {
        "meta": {
            "strategy":    "ADX+EMA50slope+EMA9/21cross",
            "timeframe":   INTERVAL,
            "period":      "2024-07 to 2026-06",
            "symbols":     len(SYMBOLS),
            "symbols_with_data": len(symbol_results),
            "capital":     CAPITAL,
            "risk_pct":    RISK_PER_TRADE * 100,
            "fee_pct":     FEE_RATE * 100,
            "slip_pct":    SLIP_RATE * 100,
            "max_pos":     MAX_POS,
            "adx_min":     ADX_MIN,
            "slope_thresh": SLOPE_THRESH,
            "run_seconds": round(elapsed, 1),
        },
        "variants": {},
    }
    for vname, result in report.items():
        json_out["variants"][vname] = {
            "aggregate": result["aggregate"],
            "per_coin":  [
                {"symbol": sym, **cs}
                for sym, cs in result["per_coin"]
            ],
            "monthly":   [{"month": mo, "pnl": pnl} for mo, pnl in result["monthly"]],
            "trades":    result["trades"][:2000],  # cap to keep file size sane
        }

    with open("backtest_report.json", "w") as f:
        json.dump(json_out, f, indent=2)

    print(f"\n{'='*60}")
    print("  RESULTS SUMMARY")
    print(f"{'='*60}")
    for line in summary_lines:
        print(f"  {line}")
    print(f"\n  ✅ backtest_summary.txt + backtest_report.json written")
    print(f"  ⏱ Total time: {elapsed:.0f}s")

if __name__ == "__main__":
    main()
