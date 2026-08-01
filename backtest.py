"""
Backtest — Compounding % Risk Sizing + Portfolio Guardrails (v9.0)
Strategy: ADX filter + EMA50 slope + EMA9/21 crossover (15m)
Exit:      Fixed percentage TP/SL

Variants (G / H / New unchanged, Tight is new this round):
  G     — ADX>=22, TP 3%  / SL 15%
  H     — ADX>=22, TP 4%  / SL 15%
  New   — ADX>=22, TP 4%  / SL 12%
  Tight — ADX>=22, TP 4%  / SL 10%   <-- NEW this round

WHAT CHANGED FROM v8.7 (compounding, no cap):
v8.7 let every qualifying signal fire independently with no limit on how
many correlated positions could stack up at once — that's what was driving
the 29-46% drawdowns despite high win rates (alts move together, so the
signal fires across dozens of coins on the same market move and you end up
with one giant leveraged directional bet instead of 130 independent ones).

This version adds four portfolio-level guardrails, applied identically to
ALL FOUR variants (G/H/New keep their original TP/SL/ADX config — only the
execution/risk layer changed):

  1. MAX_CONCURRENT = 17   — hard cap on simultaneously open positions
                              (portfolio-wide, within a variant's own sim).
                              A signal that arrives when 17 are already open
                              is skipped, not queued.
  2. One trade per symbol at a time — a coin can't open a second position
                              while it already has one open (this was NOT
                              enforced in v8.6/v8.7).
  3. Post-close cooldown = 2h — once a symbol's position closes (TP, SL, or
                              timeout), that symbol can't open a new trade
                              for 2 hours.
  4. Loss-streak guardrail — if a symbol loses 3 trades in a row, that
                              symbol is benched for 24h before it can trade
                              again. The streak counter resets on either a
                              win or a cooldown trigger.

NOTE ON THE "Tight" VARIANT'S COIN UNIVERSE: no fresh per-coin PF/WR filter
run has been done for this variant yet (that requires downloading and
scoring all 195 coins again, which needs network access this sandbox
doesn't have). Tight reuses the "New" variant's already-filtered 133-coin
whitelist as a starting point since it's the closest existing config (same
TP, next SL step down). Re-run the PF<1.15/trades>=30 filter against Tight's
own results once this comes back, per the standard workflow.

Sizing is unchanged from v8.7: 0.75% of CURRENT equity per trade
(compounding — position size shrinks in a drawdown, grows as equity grows).
"""

import csv, io, json, math, time, heapq, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
VARIANTS = {
    "G":     {"tp_pct": 3.0, "sl_pct": 15.0, "adx_min": 22},
    "H":     {"tp_pct": 4.0, "sl_pct": 15.0, "adx_min": 22},
    "New":   {"tp_pct": 4.0, "sl_pct": 12.0, "adx_min": 22},
    "Tight": {"tp_pct": 4.0, "sl_pct": 10.0, "adx_min": 22},
}

STARTING_CAPITAL = 10_000.0
RISK_PER_TRADE    = 0.0075   # 0.75% of CURRENT equity per trade (compounding)

# ── Portfolio guardrails (new this round, applies to ALL variants) ────────────
MAX_CONCURRENT           = 17                          # hard cap, open positions
SAME_SYMBOL_COOLDOWN_MS  = 2  * 60 * 60 * 1000          # 2h after a position closes
LOSS_STREAK_LIMIT        = 3                            # consecutive losses
LOSS_STREAK_COOLDOWN_MS  = 24 * 60 * 60 * 1000          # 1 day bench

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

# Per-variant whitelist (G/H/New unchanged from v8.6/v8.7 filter run).
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
# Tight reuses New's whitelist for now — see docstring note above.
VARIANT_SYMBOLS["Tight"] = list(VARIANT_SYMBOLS["New"])

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
    """Pulls from the futures static-archive bucket (not the live REST API —
    that's geo-blocked on GH Actions runners, see HANDOFF section 1)."""
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
                        if ts > 10**14:      # archives moved to microseconds in 2025+
                            ts //= 1000
                        candles.append((
                            ts,
                            float(row[2]),  # high
                            float(row[3]),  # low
                            float(row[4]),  # close
                        ))
        except Exception:
            pass   # 404 = symbol didn't exist that month / was delisted — expected
    candles.sort(key=lambda x: x[0])
    return candles

# ── Per-symbol signal detection (unchanged mechanics from v8.7) ───────────────
# Resolves the price-path outcome (net_ret) per candidate signal only — NOT
# its dollar size or whether it actually gets taken. Sizing AND the new
# concurrency/cooldown/loss-streak gates are compounding + path-dependent
# across the whole portfolio, so they're resolved afterward in Phase 2, in
# strict time order, across all symbols in a variant at once.
def backtest_symbol(symbol):
    candles = fetch_candles(symbol)
    if len(candles) < 150:
        return symbol, None, None

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

    # Filter-rejection counters (HANDOFF 2c / 5): every bar scanned must land
    # in exactly one bucket below, so counts always reconcile.
    counters = {
        "total_bars_scanned": 0,
        "no_trend": 0,
        "no_cross": 0,
        "direction_mismatch": 0,
        "setups_found": 0,
        "variants": {v: {"not_in_universe": 0, "adx_rejected": 0, "candidates": 0} for v in VARIANTS},
    }

    for i in range(WARMUP, n - 1):
        counters["total_bars_scanned"] += 1

        slope_pct  = (e50[i] - e50[i-10]) / e50[i-10] * 100
        trend_up   = slope_pct >  SLOPE_THRESH
        trend_down = slope_pct < -SLOPE_THRESH
        if not trend_up and not trend_down:
            counters["no_trend"] += 1
            continue

        crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
        crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
        if not crossed_up and not crossed_down:
            counters["no_cross"] += 1
            continue
        if (trend_up and not crossed_up) or (trend_down and not crossed_down):
            counters["direction_mismatch"] += 1
            continue

        counters["setups_found"] += 1

        seg_h = highs[max(0, i-59): i+1]
        seg_l = lows [max(0, i-59): i+1]
        seg_c = closes[max(0, i-59): i+1]
        adx = adx_calc(seg_h, seg_l, seg_c, 14)

        signal = "buy" if crossed_up else "sell"
        entry  = closes[i]

        for vname, vcfg in VARIANTS.items():
            if symbol not in VARIANT_SYMBOLS[vname]:
                counters["variants"][vname]["not_in_universe"] += 1
                continue
            if adx < vcfg["adx_min"]:
                counters["variants"][vname]["adx_rejected"] += 1
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

            counters["variants"][vname]["candidates"] += 1
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

    return symbol, variant_candidates, counters

# ── Phase 2: compounding sizing + portfolio guardrails ─────────────────────────
def run_execution_sim(candidates, risk_pct, starting_capital):
    """
    Every candidate signal is a *candidate* — whether it actually executes now
    depends on four gates, checked in this order: concurrency cap, one-open-
    trade-per-symbol, post-close cooldown, loss-streak bench. Trades are
    processed in entry_ts order so equity/positions/cooldowns all reflect
    state as of that exact moment (no lookahead). A min-heap tracks open
    positions by exit_ts so state gets settled chronologically.
    """
    candidates_sorted = sorted(candidates, key=lambda t: (t["entry_ts"], t["symbol"]))

    equity = starting_capital
    heap = []  # (exit_ts, symbol, pnl, win)
    executed = []
    equity_curve = []

    open_count = 0
    in_position = set()
    cooldown_until = defaultdict(int)
    penalty_until = defaultdict(int)
    consec_losses = defaultdict(int)

    rej = {"concurrency_cap": 0, "same_symbol_open": 0, "cooldown": 0,
           "loss_streak_penalty": 0, "executed": 0}

    def settle_up_to(ts):
        nonlocal equity, open_count
        while heap and heap[0][0] <= ts:
            exit_ts, sym, pnl, win = heapq.heappop(heap)
            equity += pnl
            open_count -= 1
            in_position.discard(sym)
            cooldown_until[sym] = exit_ts + SAME_SYMBOL_COOLDOWN_MS
            if win:
                consec_losses[sym] = 0
            else:
                consec_losses[sym] += 1
                if consec_losses[sym] >= LOSS_STREAK_LIMIT:
                    penalty_until[sym] = exit_ts + LOSS_STREAK_COOLDOWN_MS
                    consec_losses[sym] = 0
            equity_curve.append((exit_ts, equity))

    for c in candidates_sorted:
        settle_up_to(c["entry_ts"])

        sym, ts = c["symbol"], c["entry_ts"]

        if open_count >= MAX_CONCURRENT:
            rej["concurrency_cap"] += 1
            continue
        if sym in in_position:
            rej["same_symbol_open"] += 1
            continue
        if ts < cooldown_until[sym]:
            rej["cooldown"] += 1
            continue
        if ts < penalty_until[sym]:
            rej["loss_streak_penalty"] += 1
            continue

        risk_dollar = equity * risk_pct
        pnl = risk_dollar * (c["net_ret"] / c["sl_pct"])
        win = pnl > 0

        trade = dict(c)
        trade["pnl"] = pnl
        trade["win"] = win
        executed.append(trade)
        rej["executed"] += 1

        in_position.add(sym)
        open_count += 1
        heapq.heappush(heap, (c["exit_ts"], sym, pnl, win))

    while heap:
        exit_ts, sym, pnl, win = heapq.heappop(heap)
        equity += pnl
        equity_curve.append((exit_ts, equity))

    return executed, equity, equity_curve, rej

# ── Sharpe / Sortino (from daily-resampled equity curve) ──────────────────────
def calc_sharpe_sortino(equity_curve, starting_capital):
    if not equity_curve:
        return 0.0, 0.0
    curve = sorted(equity_curve, key=lambda x: x[0])
    daily = {}
    for ts, eq in curve:
        daily[ts // 86_400_000] = eq   # last snapshot of each day wins
    days = sorted(daily.keys())
    values = [starting_capital] + [daily[d] for d in days]
    rets = [(values[i] - values[i-1]) / values[i-1]
            for i in range(1, len(values)) if values[i-1] > 0]
    if len(rets) < 2:
        return 0.0, 0.0
    mean_r = sum(rets) / len(rets)
    var = sum((r - mean_r) ** 2 for r in rets) / (len(rets) - 1)
    std = math.sqrt(var)
    sharpe = (mean_r / std * math.sqrt(365)) if std > 0 else 0.0
    downside = [r for r in rets if r < 0]
    if downside:
        dstd = math.sqrt(sum(r ** 2 for r in downside) / len(downside))
        sortino = (mean_r / dstd * math.sqrt(365)) if dstd > 0 else 0.0
    else:
        sortino = float("inf") if mean_r > 0 else 0.0
    return round(sharpe, 3), round(sortino, 3)

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

    sharpe, sortino = calc_sharpe_sortino(equity_curve, starting_capital) if equity_curve else (0.0, 0.0)

    return {
        "total_trades":    total,
        "wins":            len(wins),
        "losses":          len(losses),
        "win_rate":        round(wr, 2),
        "profit_factor":   round(pf, 4),
        "net_pnl":         round(net_pnl, 2),
        "max_drawdown":    round(max_dd, 2),
        "sharpe":          sharpe,
        "sortino":         sortino,
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
    print("  COMPOUNDING SIZING + PORTFOLIO GUARDRAILS — v9.0")
    print("  Variants: G / H / New / Tight")
    print(f"  Max concurrent: {MAX_CONCURRENT} | Same-symbol cooldown: 2h | "
          f"Loss-streak bench: 3L -> 24h")
    print("=" * 65)

    print(f"\n[Phase 1] Downloading & scanning ({WORKERS} workers)…")
    all_results  = {}
    all_counters = {}
    done = failed = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(backtest_symbol, sym): sym for sym in SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                sym_out, res, counters = fut.result()
                if res is None:
                    failed += 1
                else:
                    all_results[sym_out] = res
                    all_counters[sym_out] = counters
            except Exception as e:
                failed += 1
                print(f"  ERROR {sym}: {e}")
            if done % 50 == 0 or done == len(SYMBOLS):
                print(f"  [{done}/{len(SYMBOLS)}] done | {len(all_results)} with data | {failed} skipped")

    if not all_results:
        print("\n\u26d4 ABORT: 0 symbols returned data.")
        print("   data.binance.vision may be blocked on this runner.")
        return

    if len(all_results) < len(SYMBOLS) * 0.1:
        print(f"\n\u26d4 ABORT: Only {len(all_results)}/{len(SYMBOLS)} symbols loaded.")
        print("   Looks like data source is blocked. Check runner network.")
        return

    print(f"\n  \u2705 {len(all_results)} symbols loaded | {failed} skipped")
    print(f"\n[Phase 2] Execution simulation (concurrency cap + cooldowns)…")

    report = {}
    summary_lines = []

    for vname, vcfg in VARIANTS.items():
        tp = vcfg["tp_pct"]; sl = vcfg["sl_pct"]; adx_min = vcfg["adx_min"]

        all_candidates = []
        for sym, vdata in all_results.items():
            if sym not in VARIANT_SYMBOLS[vname]:
                continue
            all_candidates.extend(vdata.get(vname, []))

        executed, final_equity, equity_curve, rej = run_execution_sim(
            all_candidates, RISK_PER_TRADE, STARTING_CAPITAL
        )

        agg = calc_stats(executed, equity_curve, STARTING_CAPITAL)
        if not agg:
            print(f"  {vname}: no trades")
            continue

        # Reconcile filter-rejection stats across all symbols for this variant
        vfilter = {"not_in_universe": 0, "adx_rejected": 0, "candidates": 0}
        for c in all_counters.values():
            for k in vfilter:
                vfilter[k] += c["variants"][vname][k]
        vfilter.update(rej)   # add Phase 2 execution-gate rejections

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

        verdict = "\u2705 USABLE" if agg["usable"] else "\u274c NOT USABLE"
        print(f"  {vname} (ADX>={adx_min}, TP {tp}% SL {sl}%): {len(VARIANT_SYMBOLS[vname])} coins | "
              f"{agg['total_trades']} trades | WR {agg['win_rate']}% | PF {agg['profit_factor']} | "
              f"Net ${agg['net_pnl']} | DD {agg['max_drawdown']}% | Sharpe {agg['sharpe']} | "
              f"Final equity ${final_equity:,.2f} | {verdict}")

        report[vname] = {
            "aggregate":     agg,
            "coin_table":    coin_table,
            "whitelist":     sorted(whitelist),
            "tp_pct":        tp,
            "sl_pct":        sl,
            "adx_min":       adx_min,
            "universe_size": len(VARIANT_SYMBOLS[vname]),
            "final_equity":  round(final_equity, 2),
            "filter_stats":  vfilter,
        }
        summary_lines.append(
            f"Variant {vname} (ADX>={adx_min}, TP {tp}% SL {sl}%): "
            f"{len(VARIANT_SYMBOLS[vname])} coins | {agg['total_trades']} trades | "
            f"WR {agg['win_rate']}% | PF {agg['profit_factor']} | Net ${agg['net_pnl']} | "
            f"DD {agg['max_drawdown']}% | Sharpe {agg['sharpe']} | Sortino {agg['sortino']} | "
            f"Final equity ${final_equity:,.2f} | Whitelist: {len(whitelist)} coins | {verdict}"
        )

    elapsed = time.time() - t0

    # ── backtest_summary.txt ──────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write("COMPOUNDING SIZING + PORTFOLIO GUARDRAILS — v9.0\n")
        f.write("=" * 65 + "\n")
        f.write(f"Strategy : EMA50 slope({SLOPE_THRESH}%) + EMA9/21 cross (ADX per variant)\n")
        f.write(f"Timeframe: 15m | Period: Jul 2024 - Jun 2026\n")
        f.write(f"Universe : per-variant filtered whitelist (Tight reuses New's, pending its own filter run)\n")
        f.write(f"Guardrails: max {MAX_CONCURRENT} concurrent | one open trade/symbol | "
                f"2h post-close cooldown | 3-loss-streak -> 24h bench\n")
        f.write(f"Sizing   : {RISK_PER_TRADE*100}% of CURRENT equity per trade (compounding)\n")
        f.write(f"Starting capital: ${STARTING_CAPITAL:,.0f}\n")
        f.write(f"Fees     : {FEE_RATE*100}% + {SLIP_RATE*100}% slip per side\n")
        f.write(f"Run time : {elapsed:.0f}s\n")
        f.write("=" * 65 + "\n\n")

        for vname, res in report.items():
            agg = res["aggregate"]; fs = res["filter_stats"]
            f.write(f"{'='*65}\n")
            f.write(f"VARIANT {vname}  -  ADX>={res['adx_min']} | TP {res['tp_pct']}% / SL {res['sl_pct']}% | Universe: {res['universe_size']} coins\n")
            f.write(f"{'='*65}\n")
            f.write(f"Total Trades    : {agg['total_trades']}\n")
            f.write(f"Wins / Losses   : {agg['wins']} / {agg['losses']}\n")
            f.write(f"Win Rate        : {agg['win_rate']}%\n")
            f.write(f"Profit Factor   : {agg['profit_factor']}\n")
            f.write(f"Net PnL         : ${agg['net_pnl']}\n")
            f.write(f"Final Equity    : ${res['final_equity']:,.2f}\n")
            f.write(f"Max Drawdown    : {agg['max_drawdown']}%  (equity-curve based)\n")
            f.write(f"Sharpe          : {agg['sharpe']}\n")
            f.write(f"Sortino         : {agg['sortino']}\n")
            f.write(f"Avg Win         : ${agg['avg_win']}\n")
            f.write(f"Avg Loss        : ${agg['avg_loss']}\n")
            f.write(f"Expectancy      : ${agg['expectancy']}\n")
            f.write(f"Avg Duration    : {agg['avg_bars']} bars ({agg['avg_bars']*0.25:.1f}h)\n")
            f.write(f"Longs           : {agg['long_trades']} trades | WR {agg['long_wr']}%\n")
            f.write(f"Shorts          : {agg['short_trades']} trades | WR {agg['short_wr']}%\n")
            f.write(f"Best Win Streak : {agg['best_win_streak']}\n")
            f.write(f"Best Loss Streak: {agg['best_loss_streak']}\n")
            f.write(f"VERDICT         : {'USABLE' if agg['usable'] else 'NOT USABLE'}\n\n")

            f.write("Filter rejection stats (candidates -> executed):\n")
            f.write(f"  Not in this variant's universe : {fs['not_in_universe']}\n")
            f.write(f"  Rejected by ADX filter          : {fs['adx_rejected']}\n")
            f.write(f"  Candidate signals (passed ADX)  : {fs['candidates']}\n")
            f.write(f"    -> skipped, concurrency cap ({MAX_CONCURRENT}) : {fs['concurrency_cap']}\n")
            f.write(f"    -> skipped, symbol already open      : {fs['same_symbol_open']}\n")
            f.write(f"    -> skipped, in 2h post-close cooldown: {fs['cooldown']}\n")
            f.write(f"    -> skipped, 3-loss-streak bench       : {fs['loss_streak_penalty']}\n")
            f.write(f"    -> EXECUTED                            : {fs['executed']}\n\n")

            f.write("Monthly PnL:\n")
            for mo, pnl in agg["monthly"].items():
                bar = "#" * int(abs(pnl) / 100) if abs(pnl) > 0 else ""
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
            "strategy":            "EMA50slope+EMA9/21cross+ADX_per_variant",
            "mode":                "compounding_pct_sizing_with_guardrails",
            "timeframe":           INTERVAL,
            "period":              "2024-07 to 2026-06",
            "symbols_fetched":     len(SYMBOLS),
            "symbols_with_data":   len(all_results),
            "starting_capital":    STARTING_CAPITAL,
            "risk_pct":            RISK_PER_TRADE * 100,
            "fee_pct":             FEE_RATE * 100,
            "slip_pct":            SLIP_RATE * 100,
            "slope_thresh":        SLOPE_THRESH,
            "max_concurrent":      MAX_CONCURRENT,
            "same_symbol_cooldown_hours": SAME_SYMBOL_COOLDOWN_MS / 3_600_000,
            "loss_streak_limit":   LOSS_STREAK_LIMIT,
            "loss_streak_cooldown_hours": LOSS_STREAK_COOLDOWN_MS / 3_600_000,
            "run_seconds":         round(elapsed, 1),
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
            "filter_stats":  res["filter_stats"],
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

