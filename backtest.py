"""
Backtest — Screening Run (No Position Cap)
Strategy: ADX≥22 + EMA50 slope + EMA9/21 crossover (15m)
Exit:      Fixed percentage TP/SL
Variants:
  G   — TP 3%  / SL 15%
  H   — TP 4%  / SL 15%
  New — TP 4%  / SL 12%

PURPOSE: Coin screening — every signal on every coin executes independently.
No portfolio cap. Find which coins perform well, then whitelist them.
Each trade risks 0.75% of $10,000 fixed base (not compound) so per-coin
PF/WR stats are directly comparable across coins.

Universe: Full 592-coin list | Period: Jul 2024 – Jun 2026 | 15m candles
Data:     data.binance.vision monthly archives (no API key needed)
"""

import csv, io, json, time, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
VARIANTS = {
    "G":   {"tp_pct": 3.0,  "sl_pct": 15.0},
    "H":   {"tp_pct": 4.0,  "sl_pct": 15.0},
    "New": {"tp_pct": 4.0,  "sl_pct": 12.0},
}

CAPITAL        = 10_000.0
RISK_PER_TRADE = 0.0075   # 0.75% of fixed base per trade
FEE_RATE       = 0.0005   # 0.05% per side
SLIP_RATE      = 0.0002   # 0.02% per side
COST_PER_SIDE  = FEE_RATE + SLIP_RATE  # 0.07% per side, 0.14% round trip

ADX_MIN       = 22
SLOPE_THRESH  = 0.05      # EMA50 must move 0.05% over 10 bars
INTERVAL      = "15m"
WORKERS       = 50

MONTHS = [
    (2024,7),(2024,8),(2024,9),(2024,10),(2024,11),(2024,12),
    (2025,1),(2025,2),(2025,3),(2025,4),(2025,5),(2025,6),
    (2025,7),(2025,8),(2025,9),(2025,10),(2025,11),(2025,12),
    (2026,1),(2026,2),(2026,3),(2026,4),(2026,5),(2026,6),
]
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

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

# ── Indicators ────────────────────────────────────────────────────────────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 2 + 1:
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
    def wilder(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r
    st = wilder(trs, period)
    sp = wilder(pdm, period)
    sm = wilder(mdm, period)
    if not st:
        return 0.0
    pdi = [100 * p / t if t else 0 for p, t in zip(sp, st)]
    mdi = [100 * m / t if t else 0 for m, t in zip(sm, st)]
    dx  = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period:
        return 0.0
    adx_val = sum(dx[:period]) / period
    for d in dx[period:]:
        adx_val = (adx_val * (period - 1) + d) / period
    return max(0.0, min(100.0, adx_val))

# ── Data fetch ────────────────────────────────────────────────────────────────
def fetch_candles(symbol):
    candles = []
    for year, month in MONTHS:
        url = (f"{BASE_URL}/{symbol}/{INTERVAL}/"
               f"{symbol}-{INTERVAL}-{year}-{month:02d}.zip")
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                raw = resp.read()
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                with zf.open(zf.namelist()[0]) as f:
                    for row in csv.reader(io.TextIOWrapper(f)):
                        if not row or not row[0].isdigit():
                            continue
                        ts = int(row[0])
                        if ts > 10**14:
                            ts //= 1000
                        candles.append((
                            ts,
                            float(row[2]),  # high
                            float(row[3]),  # low
                            float(row[4]),  # close
                        ))
        except Exception:
            pass
    candles.sort(key=lambda x: x[0])
    return candles

# ── Per-symbol backtest (NO position cap — screening mode) ────────────────────
def backtest_symbol(symbol):
    candles = fetch_candles(symbol)
    if len(candles) < 150:
        return symbol, None

    ts_arr  = [c[0] for c in candles]
    highs   = [c[1] for c in candles]
    lows    = [c[2] for c in candles]
    closes  = [c[3] for c in candles]
    n       = len(candles)

    # Pre-compute EMAs on full series
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    WARMUP = 60  # need enough bars for ADX + EMA50 slope

    # Per-variant trade lists
    variant_trades = {v: [] for v in VARIANTS}

    for i in range(WARMUP, n - 1):
        # EMA50 slope over last 10 bars
        slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
        trend_up   = slope_pct >  SLOPE_THRESH
        trend_down = slope_pct < -SLOPE_THRESH
        if not trend_up and not trend_down:
            continue

        # EMA9/21 crossover on this bar
        crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
        crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
        if not crossed_up and not crossed_down:
            continue

        # Direction must match trend
        if trend_up and not crossed_up:
            continue
        if trend_down and not crossed_down:
            continue

        # ADX on last 60 bars (expensive — only compute when crossover+trend pass)
        seg_h = highs[max(0, i-59): i+1]
        seg_l = lows [max(0, i-59): i+1]
        seg_c = closes[max(0, i-59): i+1]
        adx = adx_calc(seg_h, seg_l, seg_c, 14)
        if adx < ADX_MIN:
            continue

        signal = "buy" if crossed_up else "sell"
        entry  = closes[i]

        # Run forward for each variant independently
        for vname, vcfg in VARIANTS.items():
            tp_pct = vcfg["tp_pct"] / 100.0
            sl_pct = vcfg["sl_pct"] / 100.0

            if signal == "buy":
                tp_price = entry * (1 + tp_pct)
                sl_price = entry * (1 - sl_pct)
            else:
                tp_price = entry * (1 - tp_pct)
                sl_price = entry * (1 + sl_pct)

            outcome    = "timeout"
            exit_price = closes[-1]
            exit_bar   = n - 1

            for j in range(i + 1, n):
                h = highs[j]; l = lows[j]
                if signal == "buy":
                    if l <= sl_price:
                        outcome = "sl"; exit_price = sl_price; exit_bar = j; break
                    if h >= tp_price:
                        outcome = "tp"; exit_price = tp_price; exit_bar = j; break
                else:
                    if h >= sl_price:
                        outcome = "sl"; exit_price = sl_price; exit_bar = j; break
                    if l <= tp_price:
                        outcome = "tp"; exit_price = tp_price; exit_bar = j; break

            # PnL: fixed risk per trade (screening — not compound)
            risk_dollar = CAPITAL * RISK_PER_TRADE  # $75 per trade
            if signal == "buy":
                raw_ret = (exit_price - entry) / entry
            else:
                raw_ret = (entry - exit_price) / entry
            net_ret = raw_ret - COST_PER_SIDE * 2
            # Scale PnL: risk_dollar is what you lose on full SL hit
            pnl = risk_dollar * (net_ret / sl_pct)

            variant_trades[vname].append({
                "signal":    signal,
                "entry":     entry,
                "exit":      exit_price,
                "outcome":   outcome,
                "pnl":       pnl,
                "win":       pnl > 0,
                "entry_ts":  ts_arr[i],
                "exit_ts":   ts_arr[exit_bar],
                "bars":      exit_bar - i,
            })

    return symbol, variant_trades

# ── Aggregate stats for a list of trades ─────────────────────────────────────
def calc_stats(trades, tp_pct, sl_pct):
    if not trades:
        return None
    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    total  = len(trades)
    wr     = len(wins) / total * 100

    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss   = abs(sum(t["pnl"] for t in losses))
    pf           = gross_profit / gross_loss if gross_loss else float("inf")
    net_pnl      = gross_profit - gross_loss

    avg_win      = gross_profit / len(wins)   if wins   else 0
    avg_loss     = gross_loss   / len(losses) if losses else 0
    expectancy   = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)

    longs  = [t for t in trades if t["signal"] == "buy"]
    shorts = [t for t in trades if t["signal"] == "sell"]
    lwr = sum(1 for t in longs  if t["win"]) / len(longs)  * 100 if longs  else 0
    swr = sum(1 for t in shorts if t["win"]) / len(shorts) * 100 if shorts else 0

    avg_bars = sum(t["bars"] for t in trades) / total

    # Monthly PnL
    monthly = defaultdict(float)
    for t in trades:
        dt  = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        monthly[f"{dt.year}-{dt.month:02d}"] += t["pnl"]

    # Max drawdown (running equity)
    running = 0.0; peak = 0.0; max_dd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        running += t["pnl"]
        if running > peak:
            peak = running
        dd = (peak - running) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # Streaks
    best_w = best_l = cur = 0
    prev_win = None
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        w = t["win"]
        cur = cur + 1 if w == prev_win else 1
        if w:   best_w = max(best_w, cur)
        else:   best_l = max(best_l, cur)
        prev_win = w

    return {
        "total_trades":   total,
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(wr, 2),
        "profit_factor":  round(pf, 4),
        "net_pnl":        round(net_pnl, 2),
        "max_drawdown":   round(max_dd, 2),
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
        "monthly":        dict(sorted(monthly.items())),
        "usable":         pf >= 1.5 and wr >= 42,
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 65)
    print("  SCREENING BACKTEST — Variants G / H / New")
    print(f"  {len(SYMBOLS)} coins | Jul 2024–Jun 2026 | 15m | NO position cap")
    print("=" * 65)

    # Phase 1 — parallel fetch + signal generation
    print(f"\n[Phase 1] Downloading & scanning ({WORKERS} workers)…")
    all_results = {}  # symbol -> {variant -> [trades]}
    done = failed = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(backtest_symbol, sym): sym for sym in SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                sym_out, res = fut.result()
                if res is None:
                    failed += 1
                else:
                    all_results[sym_out] = res
            except Exception as e:
                failed += 1
                print(f"  ERROR {sym}: {e}")
            if done % 100 == 0 or done == len(SYMBOLS):
                print(f"  [{done}/{len(SYMBOLS)}] done | {len(all_results)} with data | {failed} skipped")

    if not all_results:
        print("\n⛔ ABORT: 0 symbols returned data.")
        print("   data.binance.vision may be blocked on this runner.")
        return

    # Sanity check — abort if >90% failed (data source blocked)
    if len(all_results) < len(SYMBOLS) * 0.1:
        print(f"\n⛔ ABORT: Only {len(all_results)}/{len(SYMBOLS)} symbols loaded.")
        print("   Looks like data source is blocked. Check runner network.")
        return

    print(f"\n  ✅ {len(all_results)} symbols loaded | {failed} skipped (no data / too new)")

    # Phase 2 — aggregate stats per variant
    print("\n[Phase 2] Computing stats…")

    report      = {}
    summary_lines = []

    for vname, vcfg in VARIANTS.items():
        tp = vcfg["tp_pct"]; sl = vcfg["sl_pct"]

        # Collect ALL trades across all coins for this variant
        all_trades = []
        coin_stats = {}

        for sym, vdata in all_results.items():
            trades = vdata.get(vname, [])
            if not trades:
                continue
            all_trades.extend(trades)
            cs = calc_stats(trades, tp, sl)
            if cs:
                coin_stats[sym] = cs

        agg = calc_stats(all_trades, tp, sl)
        if not agg:
            print(f"  {vname}: no trades")
            continue

        # Per-coin table sorted by PF
        coin_table = sorted(
            coin_stats.items(),
            key=lambda x: (x[1]["profit_factor"] if x[1]["profit_factor"] != float("inf") else 9999, x[1]["net_pnl"]),
            reverse=True
        )

        # Whitelist: coins with PF>=1.5 AND WR>=42% AND >=10 trades (statistically meaningful)
        whitelist = [
            sym for sym, cs in coin_stats.items()
            if cs["profit_factor"] >= 1.5
            and cs["win_rate"] >= 42
            and cs["total_trades"] >= 10
        ]

        verdict = "✅ USABLE" if agg["usable"] else "❌ NOT USABLE"
        print(f"  {vname} (TP {tp}% SL {sl}%): {agg['total_trades']} trades | "
              f"WR {agg['win_rate']}% | PF {agg['profit_factor']} | "
              f"Net ${agg['net_pnl']} | {len(whitelist)} whitelisted coins | {verdict}")

        report[vname] = {
            "aggregate":  agg,
            "coin_table": coin_table,
            "whitelist":  sorted(whitelist),
            "tp_pct":     tp,
            "sl_pct":     sl,
        }
        summary_lines.append(
            f"Variant {vname} (TP {tp}% SL {sl}%): "
            f"{agg['total_trades']} trades | WR {agg['win_rate']}% | "
            f"PF {agg['profit_factor']} | Net ${agg['net_pnl']} | "
            f"DD {agg['max_drawdown']}% | Whitelist: {len(whitelist)} coins | {verdict}"
        )

    elapsed = time.time() - t0

    # ── backtest_summary.txt ──────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write("SCREENING BACKTEST SUMMARY\n")
        f.write("=" * 65 + "\n")
        f.write(f"Strategy : ADX≥{ADX_MIN} + EMA50 slope({SLOPE_THRESH}%) + EMA9/21 cross\n")
        f.write(f"Timeframe: 15m | Period: Jul 2024 – Jun 2026\n")
        f.write(f"Universe : {len(SYMBOLS)} coins ({len(all_results)} with data)\n")
        f.write(f"Mode     : SCREENING (no position cap — every signal executes)\n")
        f.write(f"Risk/trade: {RISK_PER_TRADE*100}% of ${CAPITAL:,.0f} fixed = ${CAPITAL*RISK_PER_TRADE:.0f}/trade\n")
        f.write(f"Fees     : {FEE_RATE*100}% + {SLIP_RATE*100}% slip per side\n")
        f.write(f"Run time : {elapsed:.0f}s\n")
        f.write("=" * 65 + "\n\n")

        for vname, res in report.items():
            agg = res["aggregate"]
            f.write(f"{'='*65}\n")
            f.write(f"VARIANT {vname}  —  TP {res['tp_pct']}% / SL {res['sl_pct']}%\n")
            f.write(f"{'='*65}\n")
            f.write(f"Total Trades    : {agg['total_trades']}\n")
            f.write(f"Wins / Losses   : {agg['wins']} / {agg['losses']}\n")
            f.write(f"Win Rate        : {agg['win_rate']}%\n")
            f.write(f"Profit Factor   : {agg['profit_factor']}\n")
            f.write(f"Net PnL         : ${agg['net_pnl']}\n")
            f.write(f"Max Drawdown    : {agg['max_drawdown']}%\n")
            f.write(f"Avg Win         : ${agg['avg_win']}\n")
            f.write(f"Avg Loss        : ${agg['avg_loss']}\n")
            f.write(f"Expectancy      : ${agg['expectancy']}\n")
            f.write(f"Avg Duration    : {agg['avg_bars']} bars ({agg['avg_bars']*0.25:.1f}h)\n")
            f.write(f"Longs           : {agg['long_trades']} trades | WR {agg['long_wr']}%\n")
            f.write(f"Shorts          : {agg['short_trades']} trades | WR {agg['short_wr']}%\n")
            f.write(f"Best Win Streak : {agg['best_win_streak']}\n")
            f.write(f"Best Loss Streak: {agg['best_loss_streak']}\n")
            f.write(f"VERDICT         : {'✅ USABLE' if agg['usable'] else '❌ NOT USABLE'}\n\n")

            f.write("Monthly PnL:\n")
            for mo, pnl in agg["monthly"].items():
                bar = "█" * int(abs(pnl) / 50) if abs(pnl) > 0 else ""
                sign = "+" if pnl >= 0 else ""
                f.write(f"  {mo}: ${sign}{pnl:,.2f}  {bar}\n")
            f.write("\n")

            f.write(f"WHITELIST (PF≥1.5, WR≥42%, ≥10 trades): {len(res['whitelist'])} coins\n")
            f.write("  " + ", ".join(res["whitelist"]) + "\n\n")

            f.write(f"TOP 30 COINS by Profit Factor:\n")
            f.write(f"  {'Symbol':<22} {'PF':>7}  {'WR':>6}  {'Trades':>7}  {'Net PnL':>10}  {'DD':>6}\n")
            f.write(f"  {'-'*62}\n")
            for sym, cs in res["coin_table"][:30]:
                pf_str = f"{cs['profit_factor']:.3f}" if cs["profit_factor"] != float("inf") else "  inf"
                f.write(f"  {sym:<22} {pf_str:>7}  {cs['win_rate']:>5.1f}%  {cs['total_trades']:>7}  "
                        f"${cs['net_pnl']:>9.2f}  {cs['max_drawdown']:>5.1f}%\n")
            f.write("\n")

            f.write(f"BOTTOM 20 COINS by Profit Factor:\n")
            f.write(f"  {'Symbol':<22} {'PF':>7}  {'WR':>6}  {'Trades':>7}  {'Net PnL':>10}\n")
            f.write(f"  {'-'*55}\n")
            for sym, cs in res["coin_table"][-20:]:
                pf_str = f"{cs['profit_factor']:.3f}" if cs["profit_factor"] != float("inf") else "  inf"
                f.write(f"  {sym:<22} {pf_str:>7}  {cs['win_rate']:>5.1f}%  {cs['total_trades']:>7}  "
                        f"${cs['net_pnl']:>9.2f}\n")
            f.write("\n")

        f.write("=" * 65 + "\n")
        f.write("QUICK COMPARISON\n")
        f.write("=" * 65 + "\n")
        for line in summary_lines:
            f.write(line + "\n")
        f.write(f"\nCompleted in {elapsed:.0f}s\n")

    # ── backtest_report.json ──────────────────────────────────────────────────
    json_out = {
        "meta": {
            "strategy":          "ADX+EMA50slope+EMA9/21cross",
            "mode":              "screening_no_cap",
            "timeframe":         INTERVAL,
            "period":            "2024-07 to 2026-06",
            "symbols_total":     len(SYMBOLS),
            "symbols_with_data": len(all_results),
            "capital_base":      CAPITAL,
            "risk_pct":          RISK_PER_TRADE * 100,
            "fee_pct":           FEE_RATE * 100,
            "slip_pct":          SLIP_RATE * 100,
            "adx_min":           ADX_MIN,
            "slope_thresh":      SLOPE_THRESH,
            "run_seconds":       round(elapsed, 1),
        },
        "variants": {},
    }

    for vname, res in report.items():
        json_out["variants"][vname] = {
            "aggregate": res["aggregate"],
            "whitelist": res["whitelist"],
            "per_coin": [
                {
                    "symbol":        sym,
                    "trades":        cs["total_trades"],
                    "wins":          cs["wins"],
                    "losses":        cs["losses"],
                    "win_rate":      cs["win_rate"],
                    "profit_factor": cs["profit_factor"] if cs["profit_factor"] != float("inf") else 9999,
                    "net_pnl":       cs["net_pnl"],
                    "max_drawdown":  cs["max_drawdown"],
                    "long_trades":   cs["long_trades"],
                    "long_wr":       cs["long_wr"],
                    "short_trades":  cs["short_trades"],
                    "short_wr":      cs["short_wr"],
                    "avg_bars":      cs["avg_bars"],
                    "usable":        cs["usable"],
                }
                for sym, cs in res["coin_table"]
            ],
        }

    with open("backtest_report.json", "w") as f:
        json.dump(json_out, f, indent=2)

    # Print final summary
    print(f"\n{'='*65}")
    print("  FINAL RESULTS")
    print(f"{'='*65}")
    for line in summary_lines:
        print(f"  {line}")
    print(f"\n  Files: backtest_summary.txt + backtest_report.json")
    print(f"  Time : {elapsed:.0f}s")

if __name__ == "__main__":
    main()
