"""
Backtest — Wider ROI TP @ 10x Leverage, Compounding, NO Cap (v8.9)
Strategy: ADX filter + EMA50 slope + EMA9/21 crossover (15m)
Exit:      Fixed percentage TP/SL, defined in ROI terms (post-leverage)

WHAT CHANGED FROM v8.8:
v8.8 used the original TP numbers (3%/4%/4% ROI) reinterpreted at 10x
leverage and it wiped every variant's equity to $0 — the TP was so small in
price terms (0.3-0.4%) that round-trip fees (~0.14% price) ate nearly half
of it, so even at ~75-80% win rate the strategy lost money overall. This
version widens TP (SL unchanged per variant) to give real room over the fee
drag:

  G    — ADX>=22, TP 5% ROI (0.5% price) / SL 15% ROI (1.5% price)
  H    — ADX>=22, TP 6% ROI (0.6% price) / SL 15% ROI (1.5% price)
  New  — ADX>=22, TP 6% ROI (0.6% price) / SL 12% ROI (1.2% price)

Rough math before running: net of the ~0.14% round-trip cost, G's
loss-to-win ratio drops from ~8.5:1 (v8.8) to ~3.8:1, needing ~79% win rate
to break even — plausible given v8.8 showed G running ~80% WR, but not
guaranteed, since win rate itself can shift when the TP distance changes
(a farther TP is generally harder to hit before SL). Watch WR alongside PF
in the results, not just PF alone.

Sizing stays compounding (0.75% of current equity per trade), still NO
concurrency cap. Coin universe unchanged from v8.6/v8.7/v8.8 (G=144,
H=134, New=133) — same caveat as before: this whitelist was built from the
ORIGINAL wide price TP/SL, not these ROI-derived thresholds, so it may not
reflect which coins actually perform best here.
"""

import csv, io, json, time, heapq, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
LEVERAGE = 10   # ROI% / LEVERAGE = price% — this is what actually drives TP/SL below

# tp_pct/sl_pct below are PRICE percentages, derived from the ROI targets
# you actually trade with (5%/15%, 6%/15%, 6%/12% ROI at 10x leverage).
VARIANTS = {
    "G":   {"tp_pct": 5.0 / LEVERAGE, "sl_pct": 15.0 / LEVERAGE, "adx_min": 22},   # 0.5% / 1.5% price
    "H":   {"tp_pct": 6.0 / LEVERAGE, "sl_pct": 15.0 / LEVERAGE, "adx_min": 22},   # 0.6% / 1.5% price
    "New": {"tp_pct": 6.0 / LEVERAGE, "sl_pct": 12.0 / LEVERAGE, "adx_min": 22},   # 0.6% / 1.2% price
}

STARTING_CAPITAL = 10_000.0
RISK_PER_TRADE    = 0.0075   # 0.75% of CURRENT equity per trade (compounding)

FEE_RATE       = 0.0005   # 0.05% per side
SLIP_RATE      = 0.0002   # 0.02% per side
COST_PER_SIDE  = FEE_RATE + SLIP_RATE

SLOPE_THRESH   = 0.05     # EMA50 must move 0.05% over 10 bars
INTERVAL       = "15m"
WORKERS        = 50

MONTHS = [
    (2024,7),(2024,8),(2024,9),(2024,10),(2024,11),(2024,12),
    (2025,1),(2025,2),(2025,3),(2025,4),(2025,5),(2025,6),
    (2025,7),(2025,8),(2025,9),(2025,10),(2025,11),(2025,12),
    (2026,1),(2026,2),(2026,3),(2026,4),(2026,5),(2026,6),
]
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# Per-variant whitelist (unchanged from v8.6): drop a symbol from a variant
# only if it was PF<1.15 AND trades>=30 for that variant in the prior
# 195-coin no-cap run.
VARIANT_SYMBOLS = {
    "G": [
        "0GUSDT", "1000000BOBUSDT", "1000BONKUSDT", "1000CATUSDT", "1000RATSUSDT",
        "1000SATSUSDT", "A2ZUSDT", "ACHUSDT", "AI16ZUSDT", "AINUSDT", "AIOTUSDT",
        "ALGOUSDT", "ALICEUSDT", "ALPINEUSDT", "ANKRUSDT", "ARKMUSDT", "ASRUSDT",
        "ASTERUSDT", "ATAUSDT", "AUSDT", "AWEUSDT", "AXLUSDT", "BANDUSDT", "BANKUSDT",
        "BASEDUSDT", "BASUSDT", "BATUSDT", "BDXNUSDT", "BELUSDT", "BIDUSDT", "BMTUSDT",
        "BTRUSDT", "CFXUSDT", "CHIPUSDT", "COAIUSDT", "COMBOUSDT", "COMMONUSDT",
        "COTIUSDT", "CRCLUSDT", "CUSDT", "DAMUSDT", "DEFIUSDT", "DEXEUSDT", "DIAUSDT",
        "DMCUSDT", "EIGENUSDT", "ELSAUSDT", "ENAUSDT", "EPICUSDT", "EPTUSDT", "ESPUSDT",
        "ETCUSDT", "ETHUSDT", "EVAAUSDT", "FIOUSDT", "FLNCUSDT", "FLUXUSDT", "FOLKSUSDT",
        "FUNUSDT", "FXSUSDT", "GLMUSDT", "GRIFFAINUSDT", "GUAUSDT", "HANAUSDT",
        "HEMIUSDT", "ICPUSDT", "ICXUSDT", "INITUSDT", "IOSTUSDT", "IOUSDT", "IPUSDT",
        "KITEUSDT", "LABUSDT", "LIGHTUSDT", "LRCUSDT", "LYNUSDT", "MAGICUSDT", "MAVUSDT",
        "MEGAUSDT", "MILKUSDT", "MOODENGUSDT", "MTLUSDT", "NFPUSDT", "NMRUSDT",
        "NOMUSDT", "NOTUSDT", "OBOLUSDT", "OMGUSDT", "OPENUSDT", "OPNUSDT", "ORBSUSDT",
        "PEOPLEUSDT", "PIPPINUSDT", "PIXELUSDT", "PLUMEUSDT", "POLUSDT", "POWERUSDT",
        "POWRUSDT", "PROMPTUSDT", "PTBUSDT", "PUMPBTCUSDT", "PUNDIXUSDT", "QUICKUSDT",
        "RAVEUSDT", "REEFUSDT", "RESOLVUSDT", "REZUSDT", "RLSUSDT", "RVVUSDT",
        "SAGAUSDT", "SAHARAUSDT", "SANTOSUSDT", "SEIUSDT", "SIGNUSDT", "SKRUSDT",
        "SNDKUSDT", "SOMIUSDT", "SPELLUSDT", "SPKUSDT", "STABLEUSDT", "STBLUSDT",
        "STXUSDT", "TNSRUSDT", "TRBUSDT", "TRUTHUSDT", "TURBOUSDT", "UBUSDT",
        "USUALUSDT", "UXLINKUSDT", "VANRYUSDT", "VINEUSDT", "VIRTUALUSDT", "VVVUSDT",
        "WAXPUSDT", "WLDUSDT", "XCNUSDT", "XEMUSDT", "XLMUSDT", "XRPUSDT", "YBUSDT",
        "ZECUSDT", "ZENUSDT", "ZEREBROUSDT", "ZKJUSDT",
    ],
    "H": [
        "0GUSDT", "1000BONKUSDT", "1000CATUSDT", "1000RATSUSDT", "A2ZUSDT", "ACEUSDT",
        "ACXUSDT", "AI16ZUSDT", "AINUSDT", "AIOTUSDT", "AKTUSDT", "ALGOUSDT",
        "ALPINEUSDT", "ASRUSDT", "ASTERUSDT", "ATAUSDT", "AUSDT", "AXLUSDT",
        "BANANAUSDT", "BANDUSDT", "BANKUSDT", "BASEDUSDT", "BASUSDT", "BATUSDT",
        "BELUSDT", "BIDUSDT", "BLZUSDT", "BOMEUSDT", "BTRUSDT", "CFXUSDT", "CHIPUSDT",
        "COAIUSDT", "COMBOUSDT", "COMMONUSDT", "COTIUSDT", "CRCLUSDT", "CUSDT",
        "DAMUSDT", "DEFIUSDT", "ELSAUSDT", "ENAUSDT", "EPICUSDT", "EPTUSDT", "ESPUSDT",
        "ETCUSDT", "ETHUSDT", "ETHWUSDT", "EVAAUSDT", "FIDAUSDT", "FIOUSDT", "FISUSDT",
        "FLNCUSDT", "FLUXUSDT", "FOGOUSDT", "FRAXUSDT", "FUNUSDT", "FXSUSDT", "GLMUSDT",
        "GRIFFAINUSDT", "GUAUSDT", "GUNUSDT", "HAEDALUSDT", "HANAUSDT", "ICPUSDT",
        "ICXUSDT", "ILVUSDT", "INITUSDT", "IOTXUSDT", "IOUSDT", "IPUSDT", "KEYUSDT",
        "KITEUSDT", "LABUSDT", "LAYERUSDT", "LIGHTUSDT", "LOKAUSDT", "LRCUSDT",
        "LYNUSDT", "MILKUSDT", "MOODENGUSDT", "MORPHOUSDT", "MYROUSDT", "NOMUSDT",
        "NOTUSDT", "NTRNUSDT", "ONEUSDT", "ORBSUSDT", "PEOPLEUSDT", "PIPPINUSDT",
        "PIXELUSDT", "PLAYUSDT", "PLUMEUSDT", "POLUSDT", "POWERUSDT", "POWRUSDT",
        "PTBUSDT", "PUFFERUSDT", "PUMPBTCUSDT", "QUICKUSDT", "RAVEUSDT", "REEFUSDT",
        "RESOLVUSDT", "RVVUSDT", "SAHARAUSDT", "SEIUSDT", "SIGNUSDT", "SKATEUSDT",
        "SKRUSDT", "SNDKUSDT", "SPELLUSDT", "SPKUSDT", "STXUSDT", "SYRUPUSDT",
        "TAIKOUSDT", "TRUTHUSDT", "TURBOUSDT", "UBUSDT", "USELESSUSDT", "VANRYUSDT",
        "VINEUSDT", "VIRTUALUSDT", "VOXELUSDT", "WAXPUSDT", "WLDUSDT", "XCNUSDT",
        "XLMUSDT", "XNYUSDT", "XPLUSDT", "XRPUSDT", "YALAUSDT", "YBUSDT", "ZECUSDT",
        "ZENUSDT", "ZKJUSDT",
    ],
    "New": [
        "0GUSDT", "1000BONKUSDT", "1000CATUSDT", "1000RATSUSDT", "1000SHIBUSDT",
        "A2ZUSDT", "ACEUSDT", "ADAUSDT", "AI16ZUSDT", "AINUSDT", "AIOTUSDT", "ALGOUSDT",
        "ALPINEUSDT", "ANIMEUSDT", "API3USDT", "ASRUSDT", "ASTERUSDT", "ATAUSDT",
        "AUSDT", "BANANAUSDT", "BANDUSDT", "BASEDUSDT", "BASUSDT", "BATUSDT", "BCHUSDT",
        "BELUSDT", "BIDUSDT", "BLZUSDT", "BSWUSDT", "BTRUSDT", "CFXUSDT", "CHIPUSDT",
        "COAIUSDT", "COMBOUSDT", "COMMONUSDT", "COTIUSDT", "DAMUSDT", "DEFIUSDT",
        "DIAUSDT", "DOODUSDT", "DUSDT", "ELSAUSDT", "ENAUSDT", "EPICUSDT", "EPTUSDT",
        "ESPUSDT", "ETHWUSDT", "EVAAUSDT", "FIDAUSDT", "FISUSDT", "FLNCUSDT", "FLUXUSDT",
        "FOGOUSDT", "FORMUSDT", "FRAXUSDT", "FUNUSDT", "FXSUSDT", "GLMUSDT",
        "GRIFFAINUSDT", "GUNUSDT", "HAEDALUSDT", "HANAUSDT", "ICPUSDT", "ICXUSDT",
        "ILVUSDT", "IOTXUSDT", "IPUSDT", "KEYUSDT", "KITEUSDT", "LABUSDT", "LIGHTUSDT",
        "LOKAUSDT", "LRCUSDT", "LYNUSDT", "MAGICUSDT", "MAVUSDT", "MITOUSDT",
        "MOODENGUSDT", "MORPHOUSDT", "MYROUSDT", "NOMUSDT", "NOTUSDT", "NTRNUSDT",
        "ONEUSDT", "OPENUSDT", "ORBSUSDT", "PEOPLEUSDT", "PHBUSDT", "PIPPINUSDT",
        "PIXELUSDT", "PLAYUSDT", "PLUMEUSDT", "POWERUSDT", "POWRUSDT", "PTBUSDT",
        "PUMPBTCUSDT", "QUICKUSDT", "RAVEUSDT", "REEFUSDT", "RENDERUSDT", "RESOLVUSDT",
        "RPLUSDT", "RVVUSDT", "SANTOSUSDT", "SKATEUSDT", "SKLUSDT", "SKRUSDT",
        "SNDKUSDT", "SOPHUSDT", "SPKUSDT", "STXUSDT", "SYNUSDT", "SYRUPUSDT", "THEUSDT",
        "TNSRUSDT", "TRUTHUSDT", "TRUUSDT", "TURBOUSDT", "TWTUSDT", "UBUSDT",
        "USELESSUSDT", "UXLINKUSDT", "VANRYUSDT", "VINEUSDT", "VIRTUALUSDT", "VOXELUSDT",
        "WAXPUSDT", "XLMUSDT", "XNYUSDT", "YBUSDT", "ZECUSDT", "ZENUSDT", "ZKJUSDT",
    ],
}

# Fetch data once for the union of every symbol needed by any variant.
SYMBOLS = sorted(set().union(*VARIANT_SYMBOLS.values()))

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

# ── Per-symbol signal detection ────────────────────────────────────────────────
# Only resolves the price-path outcome (net_ret) per candidate signal — NOT
# its dollar size. Sizing is compounding (depends on equity at entry time),
# so it's computed afterward in Phase 2 across all symbols in time order.
def backtest_symbol(symbol):
    candles = fetch_candles(symbol)
    if len(candles) < 150:
        return symbol, None

    ts_arr = [c[0] for c in candles]
    highs  = [c[1] for c in candles]
    lows   = [c[2] for c in candles]
    closes = [c[3] for c in candles]
    n      = len(candles)

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    WARMUP = 60
    variant_candidates = {v: [] for v in VARIANTS}

    for i in range(WARMUP, n - 1):
        slope_pct  = (e50[i] - e50[i-10]) / e50[i-10] * 100
        trend_up   = slope_pct >  SLOPE_THRESH
        trend_down = slope_pct < -SLOPE_THRESH
        if not trend_up and not trend_down:
            continue

        crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
        crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
        if not crossed_up and not crossed_down:
            continue
        if trend_up and not crossed_up:
            continue
        if trend_down and not crossed_down:
            continue

        seg_h = highs[max(0, i-59): i+1]
        seg_l = lows [max(0, i-59): i+1]
        seg_c = closes[max(0, i-59): i+1]
        adx = adx_calc(seg_h, seg_l, seg_c, 14)

        signal = "buy" if crossed_up else "sell"
        entry  = closes[i]

        for vname, vcfg in VARIANTS.items():
            if symbol not in VARIANT_SYMBOLS[vname]:
                continue
            if adx < vcfg["adx_min"]:
                continue

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

            if signal == "buy":
                raw_ret = (exit_price - entry) / entry
            else:
                raw_ret = (entry - exit_price) / entry
            net_ret = raw_ret - COST_PER_SIDE * 2

            variant_candidates[vname].append({
                "symbol":   symbol,
                "signal":   signal,
                "entry":    entry,
                "exit":     exit_price,
                "outcome":  outcome,
                "net_ret":  net_ret,
                "sl_pct":   sl_pct,
                "entry_ts": ts_arr[i],
                "exit_ts":  ts_arr[exit_bar],
                "bars":     exit_bar - i,
            })

    return symbol, variant_candidates

# ── Phase 2: compounding sizing simulation (NO cap) ────────────────────────────
def run_compounding_sim(candidates, risk_pct, starting_capital):
    """
    Every candidate signal is taken — nothing is skipped. Trades are
    processed in entry_ts order so each trade's dollar size reflects
    equity AT ITS OWN ENTRY TIME (compounding). A min-heap tracks open
    positions by exit_ts so equity gets updated as trades close, in the
    correct chronological order, without capping how many can be open
    at once.
    """
    candidates_sorted = sorted(candidates, key=lambda t: (t["entry_ts"], t["symbol"]))

    equity = starting_capital
    heap = []  # (exit_ts, pnl) for currently open (already-sized) trades
    executed = []
    equity_curve = []  # (ts, equity) snapshots for drawdown calc

    for c in candidates_sorted:
        # settle every position that has already closed by this entry
        while heap and heap[0][0] <= c["entry_ts"]:
            exit_ts, pnl = heapq.heappop(heap)
            equity += pnl
            equity_curve.append((exit_ts, equity))

        risk_dollar = equity * risk_pct
        pnl = risk_dollar * (c["net_ret"] / c["sl_pct"])

        trade = dict(c)
        trade["pnl"] = pnl
        trade["win"] = pnl > 0
        executed.append(trade)
        heapq.heappush(heap, (c["exit_ts"], pnl))

    # settle whatever's left open at the end
    while heap:
        exit_ts, pnl = heapq.heappop(heap)
        equity += pnl
        equity_curve.append((exit_ts, equity))

    return executed, equity, equity_curve

# ── Aggregate stats ───────────────────────────────────────────────────────────
def calc_stats(trades, equity_curve=None, starting_capital=None):
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

    avg_win  = gross_profit / len(wins)   if wins   else 0
    avg_loss = gross_loss   / len(losses) if losses else 0
    expectancy = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)

    longs  = [t for t in trades if t["signal"] == "buy"]
    shorts = [t for t in trades if t["signal"] == "sell"]
    lwr = sum(1 for t in longs  if t["win"]) / len(longs)  * 100 if longs  else 0
    swr = sum(1 for t in shorts if t["win"]) / len(shorts) * 100 if shorts else 0

    avg_bars = sum(t["bars"] for t in trades) / total

    monthly = defaultdict(float)
    for t in trades:
        dt = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        monthly[f"{dt.year}-{dt.month:02d}"] += t["pnl"]

    # Equity-curve based drawdown when available (true compounding DD)
    max_dd = 0.0
    if equity_curve and starting_capital:
        peak = starting_capital
        for _, eq in sorted(equity_curve, key=lambda x: x[0]):
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
    else:
        running = 0.0; peak = 0.0
        for t in sorted(trades, key=lambda x: x["exit_ts"]):
            running += t["pnl"]
            if running > peak:
                peak = running
            dd = (peak - running) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

    best_w = best_l = cur = 0
    prev_win = None
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        w = t["win"]
        cur = cur + 1 if w == prev_win else 1
        if w:   best_w = max(best_w, cur)
        else:   best_l = max(best_l, cur)
        prev_win = w

    return {
        "total_trades":    total,
        "wins":            len(wins),
        "losses":          len(losses),
        "win_rate":        round(wr, 2),
        "profit_factor":   round(pf, 4),
        "net_pnl":         round(net_pnl, 2),
        "max_drawdown":    round(max_dd, 2),
        "avg_win":         round(avg_win, 4),
        "avg_loss":        round(avg_loss, 4),
        "expectancy":      round(expectancy, 4),
        "avg_bars":        round(avg_bars, 1),
        "long_trades":     len(longs),
        "long_wr":         round(lwr, 2),
        "short_trades":    len(shorts),
        "short_wr":        round(swr, 2),
        "best_win_streak": best_w,
        "best_loss_streak":best_l,
        "monthly":         dict(sorted(monthly.items())),
        "usable":          pf >= 1.5 and wr >= 42,
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 65)
    print(f"  WIDER ROI TP @ {LEVERAGE}x LEVERAGE — Variants G / H / New (no cap)")
    print(f"  Per-variant filtered universe | Jul 2024-Jun 2026 | 15m")
    print(f"  Risk: {RISK_PER_TRADE*100}% of CURRENT equity per trade (compounding)")
    print("=" * 65)

    print(f"\n[Phase 1] Downloading & scanning ({WORKERS} workers)…")
    all_results = {}
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
            if done % 50 == 0 or done == len(SYMBOLS):
                print(f"  [{done}/{len(SYMBOLS)}] done | {len(all_results)} with data | {failed} skipped")

    if not all_results:
        print("\n⛔ ABORT: 0 symbols returned data.")
        print("   data.binance.vision may be blocked on this runner.")
        return

    if len(all_results) < len(SYMBOLS) * 0.1:
        print(f"\n⛔ ABORT: Only {len(all_results)}/{len(SYMBOLS)} symbols loaded.")
        print("   Looks like data source is blocked. Check runner network.")
        return

    print(f"\n  ✅ {len(all_results)} symbols loaded | {failed} skipped")

    print(f"\n[Phase 2] Compounding sizing simulation (no cap)…")

    report = {}
    summary_lines = []

    for vname, vcfg in VARIANTS.items():
        tp = vcfg["tp_pct"]; sl = vcfg["sl_pct"]; adx_min = vcfg["adx_min"]
        roi_tp = tp * LEVERAGE; roi_sl = sl * LEVERAGE

        all_candidates = []
        for sym, vdata in all_results.items():
            if sym not in VARIANT_SYMBOLS[vname]:
                continue
            all_candidates.extend(vdata.get(vname, []))

        executed, final_equity, equity_curve = run_compounding_sim(
            all_candidates, RISK_PER_TRADE, STARTING_CAPITAL
        )

        agg = calc_stats(executed, equity_curve, STARTING_CAPITAL)
        if not agg:
            print(f"  {vname}: no trades")
            continue

        by_symbol = defaultdict(list)
        for t in executed:
            by_symbol[t["symbol"]].append(t)
        coin_stats = {}
        for sym, trades in by_symbol.items():
            cs = calc_stats(trades)
            if cs:
                coin_stats[sym] = cs

        coin_table = sorted(
            coin_stats.items(),
            key=lambda x: (x[1]["profit_factor"] if x[1]["profit_factor"] != float("inf") else 9999, x[1]["net_pnl"]),
            reverse=True
        )

        whitelist = [
            sym for sym, cs in coin_stats.items()
            if cs["profit_factor"] >= 1.5
            and cs["win_rate"] >= 42
            and cs["total_trades"] >= 10
        ]

        verdict = "✅ USABLE" if agg["usable"] else "❌ NOT USABLE"
        print(f"  {vname} (ADX>={adx_min}, TP {roi_tp:.0f}%ROI/{tp}%px SL {roi_sl:.0f}%ROI/{sl}%px): {len(VARIANT_SYMBOLS[vname])} coins | "
              f"{agg['total_trades']} trades | WR {agg['win_rate']}% | PF {agg['profit_factor']} | "
              f"Net ${agg['net_pnl']} | DD {agg['max_drawdown']}% | Final equity ${final_equity:,.2f} | {verdict}")

        report[vname] = {
            "aggregate":     agg,
            "coin_table":    coin_table,
            "whitelist":     sorted(whitelist),
            "tp_pct":        tp,
            "sl_pct":        sl,
            "adx_min":       adx_min,
            "universe_size": len(VARIANT_SYMBOLS[vname]),
            "final_equity":  round(final_equity, 2),
        }
        summary_lines.append(
            f"Variant {vname} (ADX>={adx_min}, TP {roi_tp:.0f}%ROI/{tp}%px SL {roi_sl:.0f}%ROI/{sl}%px): "
            f"{len(VARIANT_SYMBOLS[vname])} coins | {agg['total_trades']} trades | "
            f"WR {agg['win_rate']}% | PF {agg['profit_factor']} | Net ${agg['net_pnl']} | "
            f"DD {agg['max_drawdown']}% | Final equity ${final_equity:,.2f} | "
            f"Whitelist: {len(whitelist)} coins | {verdict}"
        )

    elapsed = time.time() - t0

    # ── backtest_summary.txt ──────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write(f"WIDER ROI TP @ {LEVERAGE}x LEVERAGE BACKTEST SUMMARY (no cap)\n")
        f.write("=" * 65 + "\n")
        f.write(f"Strategy : EMA50 slope({SLOPE_THRESH}%) + EMA9/21 cross (ADX per variant)\n")
        f.write(f"Timeframe: 15m | Period: Jul 2024 – Jun 2026\n")
        f.write(f"Leverage : {LEVERAGE}x — TP/SL below are entered as ROI%, converted to price% by /LEVERAGE\n")
        f.write(f"Universe : per-variant filtered whitelist (v8.6 filter, PF<1.15 & trades>=30 removed)\n")
        f.write(f"Mode     : NO position cap — every signal executes independently\n")
        f.write(f"Sizing   : {RISK_PER_TRADE*100}% of CURRENT equity per trade (compounding, not fixed $)\n")
        f.write(f"Starting capital: ${STARTING_CAPITAL:,.0f}\n")
        f.write(f"Fees     : {FEE_RATE*100}% + {SLIP_RATE*100}% slip per side\n")
        f.write(f"Run time : {elapsed:.0f}s\n")
        f.write("=" * 65 + "\n\n")

        for vname, res in report.items():
            agg = res["aggregate"]
            f.write(f"{'='*65}\n")
            f.write(f"VARIANT {vname}  —  ADX>={res['adx_min']} | TP {res['tp_pct']*LEVERAGE:.0f}% ROI ({res['tp_pct']}% price) / SL {res['sl_pct']*LEVERAGE:.0f}% ROI ({res['sl_pct']}% price) | Leverage: {LEVERAGE}x | Universe: {res['universe_size']} coins\n")
            f.write(f"{'='*65}\n")
            f.write(f"Total Trades    : {agg['total_trades']}\n")
            f.write(f"Wins / Losses   : {agg['wins']} / {agg['losses']}\n")
            f.write(f"Win Rate        : {agg['win_rate']}%\n")
            f.write(f"Profit Factor   : {agg['profit_factor']}\n")
            f.write(f"Net PnL         : ${agg['net_pnl']}\n")
            f.write(f"Final Equity    : ${res['final_equity']:,.2f}\n")
            f.write(f"Max Drawdown    : {agg['max_drawdown']}%  (equity-curve based)\n")
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
                bar = "█" * int(abs(pnl) / 100) if abs(pnl) > 0 else ""
                sign = "+" if pnl >= 0 else ""
                f.write(f"  {mo}: ${sign}{pnl:,.2f}  {bar}\n")
            f.write("\n")

            f.write(f"WHITELIST (PF>=1.5, WR>=42%, >=10 trades): {len(res['whitelist'])} coins\n")
            f.write("  " + ", ".join(res["whitelist"]) + "\n\n")

            f.write(f"TOP 30 COINS by Profit Factor:\n")
            f.write(f"  {'Symbol':<22} {'PF':>7}  {'WR':>6}  {'Trades':>7}  {'Net PnL':>10}\n")
            f.write(f"  {'-'*57}\n")
            for sym, cs in res["coin_table"][:30]:
                pf_str = f"{cs['profit_factor']:.3f}" if cs["profit_factor"] != float("inf") else "  inf"
                f.write(f"  {sym:<22} {pf_str:>7}  {cs['win_rate']:>5.1f}%  {cs['total_trades']:>7}  "
                        f"${cs['net_pnl']:>9.2f}\n")
            f.write("\n")

            f.write(f"BOTTOM 20 COINS by Profit Factor:\n")
            f.write(f"  {'Symbol':<22} {'PF':>7}  {'WR':>6}  {'Trades':>7}  {'Net PnL':>10}\n")
            f.write(f"  {'-'*57}\n")
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
            "strategy":          "EMA50slope+EMA9/21cross+ADX_per_variant",
            "mode":              "roi_based_tpsl_compounding_no_cap",
            "leverage":          LEVERAGE,
            "timeframe":         INTERVAL,
            "period":            "2024-07 to 2026-06",
            "symbols_fetched":   len(SYMBOLS),
            "symbols_with_data": len(all_results),
            "starting_capital":  STARTING_CAPITAL,
            "risk_pct":          RISK_PER_TRADE * 100,
            "fee_pct":           FEE_RATE * 100,
            "slip_pct":          SLIP_RATE * 100,
            "slope_thresh":      SLOPE_THRESH,
            "run_seconds":       round(elapsed, 1),
        },
        "variants": {},
    }

    for vname, res in report.items():
        json_out["variants"][vname] = {
            "config": {
                "adx_min": res["adx_min"],
                "tp_pct":  res["tp_pct"],
                "sl_pct":  res["sl_pct"],
            },
            "universe_size": res["universe_size"],
            "aggregate":     res["aggregate"],
            "final_equity":  res["final_equity"],
            "whitelist":     res["whitelist"],
            "per_coin": [
                {
                    "symbol":        sym,
                    "trades":        cs["total_trades"],
                    "wins":          cs["wins"],
                    "losses":        cs["losses"],
                    "win_rate":      cs["win_rate"],
                    "profit_factor": cs["profit_factor"] if cs["profit_factor"] != float("inf") else 9999,
                    "net_pnl":       cs["net_pnl"],
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

    print(f"\n{'='*65}")
    print("  FINAL RESULTS")
    print(f"{'='*65}")
    for line in summary_lines:
        print(f"  {line}")
    print(f"\n  Files: backtest_summary.txt + backtest_report.json")
    print(f"  Time : {elapsed:.0f}s")

if __name__ == "__main__":
    main()

