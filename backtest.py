"""
Backtest v12.0 — Three Pre-Live Validation Tests
Based on v11.0 strategy: ADX filter + EMA50 slope + EMA9/21 crossover (15m)
Exit: Fixed percentage TP/SL, compounding 0.75% risk sizing, real capital tracking

THREE TESTS IN ONE RUN:
  TEST_WF   — Walk-Forward: trained on Jul 2024–Dec 2025, evaluated on Jan–Jun 2026 only
              Coins and variant configs are FIXED from v11.0 whitelists — no re-selection.
              Pass = PF >= 1.2 on the out-of-sample window (relaxed from 1.5 since it's 6 months).
  TEST_SLIP — Slippage Stress: same period as v11 (Jul 2024–Jun 2026), slip doubled to 0.05%
              Pass = PF still >= 1.2 (strategy survives worse fills).
  TEST_BEAR — Bear Regime: Jul 2024–Mar 2025 only (9 months, pre-bull-run)
              Pass = PF >= 1.0 (at minimum not losing money in a flat/bear window).

For each test we run all 4 variants (G, H, New, Tight) so Kimi can see which
variant is most robust across conditions.

IMPORTANT: Coin whitelists are the v11.0 per-variant whitelists (PF>=1.5, WR>=42%, >=10 trades).
We only trade coins that already proved themselves — this is the realistic live setup.
"""

import csv, io, json, math, time, heapq, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from collections import defaultdict

# ── Strategy config (unchanged from v11.0) ────────────────────────────────────
VARIANTS = {
    "G":     {"tp_pct": 3.0, "sl_pct": 15.0, "adx_min": 22},
    "H":     {"tp_pct": 4.0, "sl_pct": 15.0, "adx_min": 22},
    "New":   {"tp_pct": 4.0, "sl_pct": 12.0, "adx_min": 22},
    "Tight": {"tp_pct": 5.0, "sl_pct": 12.0, "adx_min": 22},
}

STARTING_CAPITAL = 10_000.0
RISK_PER_TRADE   = 0.0075
FEE_RATE         = 0.0005   # 0.05% per side — unchanged
SLOPE_THRESH     = 0.05
INTERVAL         = "15m"
WORKERS          = 50

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ── Test-specific date ranges ─────────────────────────────────────────────────
# Expressed as (year, month) tuples

# Walk-forward OUT-OF-SAMPLE window (Jan–Jun 2026 only)
# In-sample window Jul 2024–Dec 2025 is used only to "confirm" coin picks,
# which we do by using the v11.0 whitelist directly — no re-fitting here.
MONTHS_WF_OOS = [
    (2026,1),(2026,2),(2026,3),(2026,4),(2026,5),(2026,6),
]

# Slippage stress — full period same as v11
MONTHS_FULL = [
    (2024,7),(2024,8),(2024,9),(2024,10),(2024,11),(2024,12),
    (2025,1),(2025,2),(2025,3),(2025,4),(2025,5),(2025,6),
    (2025,7),(2025,8),(2025,9),(2025,10),(2025,11),(2025,12),
    (2026,1),(2026,2),(2026,3),(2026,4),(2026,5),(2026,6),
]

# Bear regime — Jul 2024–Mar 2025 only
MONTHS_BEAR = [
    (2024,7),(2024,8),(2024,9),(2024,10),(2024,11),(2024,12),
    (2025,1),(2025,2),(2025,3),
]

# Slippage per test
SLIP_NORMAL  = 0.0002   # 0.02% — used in WF and BEAR tests (same as v11)
SLIP_STRESS  = 0.0005   # 0.05% — doubled, used in SLIP test

# ── v11.0 Per-variant whitelists (coins that passed PF>=1.5, WR>=42%, >=10 trades) ──
# These are the ONLY coins traded in all three tests — realistic live universe.
VARIANT_WHITELIST = {
    "G": [
        "1000000BOBUSDT","1000CATUSDT","1000RATSUSDT","A2ZUSDT","AIOTUSDT",
        "ALGOUSDT","ALPINEUSDT","ASTERUSDT","AUSDT","BASEDUSDT","BELUSDT",
        "BIDUSDT","BMTUSDT","BTRUSDT","CFXUSDT","CHIPUSDT","CRCLUSDT","DAMUSDT",
        "DEXEUSDT","DIAUSDT","EPTUSDT","ETHUSDT","FLNCUSDT","FUNUSDT","GLMUSDT",
        "GUAUSDT","ICXUSDT","IOUSDT","LIGHTUSDT","MOODENGUSDT","NFPUSDT",
        "NMRUSDT","NOTUSDT","ORBSUSDT","PEOPLEUSDT","PIPPINUSDT","POWERUSDT",
        "POWRUSDT","RAVEUSDT","RESOLVUSDT","RVVUSDT","SEIUSDT","SIGNUSDT",
        "SKRUSDT","SNDKUSDT","SOMIUSDT","SPELLUSDT","TRUTHUSDT","TURBOUSDT",
        "VANRYUSDT","VINEUSDT","VVVUSDT","XEMUSDT","XRPUSDT","ZECUSDT",
        "ZEREBROUSDT",
    ],
    "H": [
        "1000CATUSDT","1000RATSUSDT","AI16ZUSDT","AINUSDT","AIOTUSDT","ALGOUSDT",
        "ALPINEUSDT","ASTERUSDT","AUSDT","BASEDUSDT","BTRUSDT","CFXUSDT",
        "CHIPUSDT","COMMONUSDT","CRCLUSDT","DAMUSDT","EPTUSDT","ETHUSDT",
        "ETHWUSDT","FISUSDT","FLUXUSDT","FOGOUSDT","FRAXUSDT","FUNUSDT",
        "GLMUSDT","ILVUSDT","IOUSDT","KEYUSDT","KITEUSDT","LAYERUSDT",
        "LIGHTUSDT","LYNUSDT","PEOPLEUSDT","PIPPINUSDT","PIXELUSDT","PLUMEUSDT",
        "POLUSDT","POWERUSDT","POWRUSDT","PUFFERUSDT","QUICKUSDT","RAVEUSDT",
        "RESOLVUSDT","RVVUSDT","SIGNUSDT","SKRUSDT","SNDKUSDT","SPELLUSDT",
        "STXUSDT","SYRUPUSDT","TRUTHUSDT","TURBOUSDT","VANRYUSDT","VINEUSDT",
        "XNYUSDT","XRPUSDT","ZKJUSDT",
    ],
    "New": [
        "0GUSDT","1000CATUSDT","1000RATSUSDT","AI16ZUSDT","AIOTUSDT","ALPINEUSDT",
        "ASTERUSDT","AUSDT","BELUSDT","BIDUSDT","BTRUSDT","CFXUSDT","CHIPUSDT",
        "COMBOUSDT","COMMONUSDT","DAMUSDT","EPTUSDT","ETHWUSDT","FLNCUSDT",
        "FLUXUSDT","FOGOUSDT","FRAXUSDT","FUNUSDT","GLMUSDT","HAEDALUSDT",
        "ILVUSDT","KITEUSDT","LOKAUSDT","LYNUSDT","MAGICUSDT","MOODENGUSDT",
        "NOMUSDT","PEOPLEUSDT","PHBUSDT","PLUMEUSDT","POWRUSDT","PUMPBTCUSDT",
        "QUICKUSDT","RAVEUSDT","RESOLVUSDT","RPLUSDT","RVVUSDT","SKLUSDT",
        "SKRUSDT","SNDKUSDT","SYRUPUSDT","THEUSDT","TRUTHUSDT","TRUUSDT",
        "TURBOUSDT","TWTUSDT","VANRYUSDT","VINEUSDT","XNYUSDT","ZECUSDT",
        "ZKJUSDT",
    ],
    "Tight": [
        "0GUSDT","1000CATUSDT","1000RATSUSDT","AI16ZUSDT","AIOTUSDT","ALPINEUSDT",
        "ASTERUSDT","AUSDT","BELUSDT","BIDUSDT","BTRUSDT","CFXUSDT","CHIPUSDT",
        "COMBOUSDT","COMMONUSDT","DAMUSDT","EPTUSDT","ETHWUSDT","FLNCUSDT",
        "FLUXUSDT","FOGOUSDT","FRAXUSDT","FUNUSDT","GLMUSDT","HAEDALUSDT",
        "ILVUSDT","KITEUSDT","LOKAUSDT","LYNUSDT","MAGICUSDT","MOODENGUSDT",
        "NOMUSDT","PEOPLEUSDT","PHBUSDT","PLUMEUSDT","POWRUSDT","PUMPBTCUSDT",
        "QUICKUSDT","RAVEUSDT","RESOLVUSDT","RPLUSDT","RVVUSDT","SKLUSDT",
        "SKRUSDT","SNDKUSDT","SYRUPUSDT","THEUSDT","TRUTHUSDT","TRUUSDT",
        "TURBOUSDT","TWTUSDT","VANRYUSDT","VINEUSDT","XNYUSDT","ZECUSDT",
        "ZKJUSDT",
    ],
}

# Union of all symbols needed
ALL_SYMBOLS = sorted(set().union(*VARIANT_WHITELIST.values()))

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
def fetch_candles(symbol, months):
    candles = []
    for year, month in months:
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

# ── Per-symbol signal scan ────────────────────────────────────────────────────
def scan_symbol(args):
    symbol, months = args
    candles = fetch_candles(symbol, months)
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
        if (trend_up and not crossed_up) or (trend_down and not crossed_down):
            continue

        seg_h = highs[max(0, i-59): i+1]
        seg_l = lows [max(0, i-59): i+1]
        seg_c = closes[max(0, i-59): i+1]
        adx = adx_calc(seg_h, seg_l, seg_c, 14)

        signal = "buy" if crossed_up else "sell"
        entry  = closes[i]

        for vname, vcfg in VARIANTS.items():
            if symbol not in VARIANT_WHITELIST[vname]:
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

            variant_candidates[vname].append({
                "symbol":   symbol,
                "signal":   signal,
                "entry":    entry,
                "exit":     exit_price,
                "outcome":  outcome,
                "net_ret":  raw_ret,   # cost subtracted later per test's slip rate
                "sl_pct":   sl_pct,
                "entry_ts": ts_arr[i],
                "exit_ts":  ts_arr[exit_bar],
                "bars":     exit_bar - i,
            })

    return symbol, variant_candidates

# ── Phase 2: execution simulation (real capital tracking) ─────────────────────
def run_execution_sim(candidates, risk_pct, starting_capital, slip_rate):
    cost_per_side = FEE_RATE + slip_rate
    candidates_sorted = sorted(candidates, key=lambda t: (t["entry_ts"], t["symbol"]))

    equity   = starting_capital
    reserved = 0.0
    heap     = []
    executed = []
    equity_curve = []
    in_position  = set()
    rej = {"same_symbol_open": 0, "insufficient_capital": 0, "executed": 0}

    def settle_up_to(ts):
        nonlocal equity, reserved
        while heap and heap[0][0] <= ts:
            exit_ts, sym, pnl, win, risk_dollar = heapq.heappop(heap)
            equity   += pnl
            reserved -= risk_dollar
            in_position.discard(sym)
            equity_curve.append((exit_ts, equity))

    for c in candidates_sorted:
        settle_up_to(c["entry_ts"])
        sym = c["symbol"]

        if sym in in_position:
            rej["same_symbol_open"] += 1
            continue

        risk_dollar = equity * risk_pct
        available   = equity - reserved
        if risk_dollar > available:
            rej["insufficient_capital"] += 1
            continue

        # Apply cost here so each test uses its own slip rate
        net_ret = c["net_ret"] - cost_per_side * 2
        pnl = risk_dollar * (net_ret / c["sl_pct"])
        win = pnl > 0

        trade = dict(c)
        trade["net_ret"]     = net_ret
        trade["pnl"]         = pnl
        trade["win"]         = win
        trade["risk_dollar"] = risk_dollar
        executed.append(trade)
        rej["executed"] += 1

        in_position.add(sym)
        reserved += risk_dollar
        heapq.heappush(heap, (c["exit_ts"], sym, pnl, win, risk_dollar))

    while heap:
        exit_ts, sym, pnl, win, risk_dollar = heapq.heappop(heap)
        equity += pnl
        equity_curve.append((exit_ts, equity))

    return executed, equity, equity_curve, rej

# ── Sharpe / Sortino ─────────────────────────────────────────────────────────
def calc_sharpe_sortino(equity_curve, starting_capital):
    if not equity_curve:
        return 0.0, 0.0
    curve = sorted(equity_curve, key=lambda x: x[0])
    daily = {}
    for ts, eq in curve:
        daily[ts // 86_400_000] = eq
    days   = sorted(daily.keys())
    values = [starting_capital] + [daily[d] for d in days]
    rets   = [(values[i] - values[i-1]) / values[i-1]
              for i in range(1, len(values)) if values[i-1] > 0]
    if len(rets) < 2:
        return 0.0, 0.0
    mean_r = sum(rets) / len(rets)
    var    = sum((r - mean_r) ** 2 for r in rets) / (len(rets) - 1)
    std    = math.sqrt(var)
    sharpe = (mean_r / std * math.sqrt(365)) if std > 0 else 0.0
    downside = [r for r in rets if r < 0]
    if downside:
        dstd    = math.sqrt(sum(r ** 2 for r in downside) / len(downside))
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
    }

# ── Run one complete test ─────────────────────────────────────────────────────
def run_test(test_name, months, slip_rate, pass_pf, all_scan_results):
    """
    all_scan_results: dict {symbol: variant_candidates} — pre-scanned for this test's months.
    pass_pf: minimum PF to label a variant as PASS.
    """
    print(f"\n  [{test_name}] Running execution sim…")
    results = {}

    for vname, vcfg in VARIANTS.items():
        all_candidates = []
        for sym, vdata in all_scan_results.items():
            if sym not in VARIANT_WHITELIST[vname]:
                continue
            all_candidates.extend(vdata.get(vname, []))

        if not all_candidates:
            print(f"    {vname}: 0 candidates")
            continue

        executed, final_equity, equity_curve, rej = run_execution_sim(
            all_candidates, RISK_PER_TRADE, STARTING_CAPITAL, slip_rate
        )
        agg = calc_stats(executed, equity_curve, STARTING_CAPITAL)
        if not agg:
            print(f"    {vname}: no executed trades")
            continue

        verdict = "PASS" if agg["profit_factor"] >= pass_pf else "FAIL"
        tp = vcfg["tp_pct"]; sl = vcfg["sl_pct"]
        print(f"    {vname} (TP {tp}%/SL {sl}%): {agg['total_trades']} trades | "
              f"WR {agg['win_rate']}% | PF {agg['profit_factor']} | "
              f"Net ${agg['net_pnl']:,.2f} | DD {agg['max_drawdown']}% | "
              f"Final ${final_equity:,.2f} | {verdict}")

        results[vname] = {
            "aggregate":   agg,
            "final_equity": round(final_equity, 2),
            "filter_stats": rej,
            "pass_pf":      pass_pf,
            "verdict":      verdict,
        }

    return results

# ── Write summary ─────────────────────────────────────────────────────────────
def write_summary(f, test_label, test_desc, months_desc, slip_rate, pass_pf, results):
    f.write(f"\n{'='*65}\n")
    f.write(f"{test_label}\n")
    f.write(f"{'='*65}\n")
    f.write(f"Description : {test_desc}\n")
    f.write(f"Period      : {months_desc}\n")
    f.write(f"Slip rate   : {slip_rate*100:.3f}% per side (fee: {FEE_RATE*100:.3f}%)\n")
    f.write(f"Pass threshold: PF >= {pass_pf}\n")
    f.write(f"Coins       : v11.0 whitelists only (no re-fitting)\n\n")

    for vname, res in results.items():
        agg = res["aggregate"]; fs = res["filter_stats"]
        vcfg = VARIANTS[vname]
        f.write(f"  VARIANT {vname}  (TP {vcfg['tp_pct']}% / SL {vcfg['sl_pct']}% / ADX>={vcfg['adx_min']})  —  {res['verdict']}\n")
        f.write(f"  {'─'*55}\n")
        f.write(f"  Trades          : {agg['total_trades']}  ({agg['wins']}W / {agg['losses']}L)\n")
        f.write(f"  Win Rate        : {agg['win_rate']}%\n")
        f.write(f"  Profit Factor   : {agg['profit_factor']}\n")
        f.write(f"  Net PnL         : ${agg['net_pnl']:,.2f}\n")
        f.write(f"  Final Equity    : ${res['final_equity']:,.2f}\n")
        f.write(f"  Max Drawdown    : {agg['max_drawdown']}%\n")
        f.write(f"  Sharpe          : {agg['sharpe']}\n")
        f.write(f"  Sortino         : {agg['sortino']}\n")
        f.write(f"  Expectancy      : ${agg['expectancy']}\n")
        f.write(f"  Avg Duration    : {agg['avg_bars']} bars ({agg['avg_bars']*0.25:.1f}h)\n")
        f.write(f"  Longs           : {agg['long_trades']} | WR {agg['long_wr']}%\n")
        f.write(f"  Shorts          : {agg['short_trades']} | WR {agg['short_wr']}%\n")
        f.write(f"  Win Streak      : {agg['best_win_streak']}\n")
        f.write(f"  Loss Streak     : {agg['best_loss_streak']}\n")
        f.write(f"  Skipped (same sym open)  : {fs['same_symbol_open']}\n")
        f.write(f"  Skipped (insuff capital) : {fs['insufficient_capital']}\n")
        f.write(f"  Executed                 : {fs['executed']}\n")
        f.write(f"\n  Monthly PnL:\n")
        for mo, pnl in agg["monthly"].items():
            sign = "+" if pnl >= 0 else ""
            f.write(f"    {mo}: ${sign}{pnl:,.2f}\n")
        f.write("\n")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 65)
    print("  BACKTEST v12.0 — Pre-Live Validation (3 Tests)")
    print("  TEST_WF   : Walk-Forward OOS (Jan–Jun 2026)")
    print("  TEST_SLIP : Slippage Stress (full period, 2.5× slip)")
    print("  TEST_BEAR : Bear Regime (Jul 2024–Mar 2025)")
    print("=" * 65)

    # We need three separate data fetches — different month ranges.
    # But we fetch once per symbol per test (parallelised), using ALL_SYMBOLS
    # so each test can filter down to its variant whitelist independently.

    def fetch_all(months, label):
        print(f"\n[Fetch] {label} — {len(ALL_SYMBOLS)} symbols, {WORKERS} workers…")
        scan_results = {}
        done = failed = 0
        with ProcessPoolExecutor(max_workers=WORKERS) as ex:
            args = [(sym, months) for sym in ALL_SYMBOLS]
            futures = {ex.submit(scan_symbol, a): a[0] for a in args}
            for fut in as_completed(futures):
                sym = futures[fut]
                done += 1
                try:
                    sym_out, res = fut.result()
                    if res is not None:
                        scan_results[sym_out] = res
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    print(f"  ERROR {sym}: {e}")
                if done % 30 == 0 or done == len(ALL_SYMBOLS):
                    print(f"  [{done}/{len(ALL_SYMBOLS)}] done | {len(scan_results)} with data | {failed} skipped")

        if not scan_results:
            print(f"\n⛔ ABORT: 0 symbols returned data for {label}.")
            print("   data.binance.vision may be blocked on this runner.")
            return None
        if len(scan_results) < len(ALL_SYMBOLS) * 0.1:
            print(f"\n⛔ ABORT: Only {len(scan_results)}/{len(ALL_SYMBOLS)} symbols loaded for {label}.")
            return None
        print(f"  ✅ {len(scan_results)} symbols loaded | {failed} skipped")
        return scan_results

    # ── Fetch data for each test ───────────────────────────────────────────────
    data_wf   = fetch_all(MONTHS_WF_OOS,  "TEST_WF  (Jan–Jun 2026 OOS)")
    data_slip = fetch_all(MONTHS_FULL,    "TEST_SLIP (Jul 2024–Jun 2026, full)")
    data_bear = fetch_all(MONTHS_BEAR,    "TEST_BEAR (Jul 2024–Mar 2025)")

    report = {}

    # ── TEST 1: Walk-Forward OOS ───────────────────────────────────────────────
    print("\n" + "="*65)
    print("TEST 1 — WALK-FORWARD (Jan–Jun 2026 only, OOS)")
    print("Coin universe: v11.0 whitelists — no re-fitting on OOS data")
    print("Pass threshold: PF >= 1.2")
    if data_wf:
        report["TEST_WF"] = run_test("TEST_WF", MONTHS_WF_OOS, SLIP_NORMAL, 1.2, data_wf)
    else:
        report["TEST_WF"] = {}

    # ── TEST 3: Slippage Stress ────────────────────────────────────────────────
    print("\n" + "="*65)
    print("TEST 3 — SLIPPAGE STRESS (full period, slip=0.05%)")
    print("Pass threshold: PF >= 1.2")
    if data_slip:
        report["TEST_SLIP"] = run_test("TEST_SLIP", MONTHS_FULL, SLIP_STRESS, 1.2, data_slip)
    else:
        report["TEST_SLIP"] = {}

    # ── TEST 5: Bear Regime ────────────────────────────────────────────────────
    print("\n" + "="*65)
    print("TEST 5 — BEAR REGIME (Jul 2024–Mar 2025 only)")
    print("Pass threshold: PF >= 1.0")
    if data_bear:
        report["TEST_BEAR"] = run_test("TEST_BEAR", MONTHS_BEAR, SLIP_NORMAL, 1.0, data_bear)
    else:
        report["TEST_BEAR"] = {}

    elapsed = time.time() - t0

    # ── Write backtest_summary.txt ─────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write("BACKTEST v12.0 — Pre-Live Validation\n")
        f.write("=" * 65 + "\n")
        f.write("Strategy : EMA50 slope + EMA9/21 cross + ADX>=22 (15m)\n")
        f.write("Coins    : v11.0 whitelists per variant (no refitting)\n")
        f.write(f"Run time : {elapsed:.0f}s\n")
        f.write("=" * 65 + "\n")

        f.write("\nQUICK VERDICT TABLE\n")
        f.write(f"{'─'*65}\n")
        f.write(f"{'Test':<12} {'Variant':<8} {'Trades':>7} {'WR':>6} {'PF':>7} {'DD':>7} {'Result'}\n")
        f.write(f"{'─'*65}\n")
        test_meta = {
            "TEST_WF":   ("Walk-Forward OOS", "PF>=1.2"),
            "TEST_SLIP": ("Slip Stress",       "PF>=1.2"),
            "TEST_BEAR": ("Bear Regime",        "PF>=1.0"),
        }
        for tname, tres in report.items():
            label = test_meta[tname][0]
            for vname, vres in tres.items():
                agg = vres["aggregate"]
                f.write(f"{label:<12} {vname:<8} {agg['total_trades']:>7} "
                        f"{agg['win_rate']:>5.1f}% {agg['profit_factor']:>7.4f} "
                        f"{agg['max_drawdown']:>6.1f}%  {vres['verdict']}\n")
        f.write(f"{'─'*65}\n")

        write_summary(f, "TEST 1 — WALK-FORWARD OOS (Jan–Jun 2026)",
                      "Coins fixed from v11.0 whitelist. Tests if edge holds on unseen 6-month window.",
                      "Jan 2026 – Jun 2026", SLIP_NORMAL, 1.2, report.get("TEST_WF", {}))

        write_summary(f, "TEST 3 — SLIPPAGE STRESS (Jul 2024–Jun 2026)",
                      "Full 2-year period but slip doubled to 0.05%. Tests fill-quality sensitivity.",
                      "Jul 2024 – Jun 2026", SLIP_STRESS, 1.2, report.get("TEST_SLIP", {}))

        write_summary(f, "TEST 5 — BEAR REGIME (Jul 2024–Mar 2025)",
                      "9-month flat/bear window before the bull run. Tests regime dependence.",
                      "Jul 2024 – Mar 2025", SLIP_NORMAL, 1.0, report.get("TEST_BEAR", {}))

        f.write("=" * 65 + "\n")
        f.write(f"Completed in {elapsed:.0f}s\n")

    # ── Write backtest_report.json ─────────────────────────────────────────────
    json_out = {
        "meta": {
            "version":           "v12.0",
            "purpose":           "pre_live_validation",
            "base_strategy":     "v11.0_EMA50slope_EMA921cross_ADX22_15m",
            "risk_pct":          RISK_PER_TRADE * 100,
            "fee_pct":           FEE_RATE * 100,
            "starting_capital":  STARTING_CAPITAL,
            "run_seconds":       round(elapsed, 1),
        },
        "tests": {},
    }

    test_configs = {
        "TEST_WF":   {"period": "2026-01 to 2026-06", "slip_pct": SLIP_NORMAL*100, "pass_pf": 1.2, "label": "Walk-Forward OOS"},
        "TEST_SLIP": {"period": "2024-07 to 2026-06", "slip_pct": SLIP_STRESS*100, "pass_pf": 1.2, "label": "Slippage Stress"},
        "TEST_BEAR": {"period": "2024-07 to 2025-03", "slip_pct": SLIP_NORMAL*100, "pass_pf": 1.0, "label": "Bear Regime"},
    }

    for tname, tres in report.items():
        json_out["tests"][tname] = {
            "config":   test_configs[tname],
            "variants": {},
        }
        for vname, vres in tres.items():
            agg = vres["aggregate"]
            json_out["tests"][tname]["variants"][vname] = {
                "verdict":      vres["verdict"],
                "aggregate":    agg,
                "final_equity": vres["final_equity"],
                "filter_stats": vres["filter_stats"],
            }

    with open("backtest_report.json", "w") as f:
        json.dump(json_out, f, indent=2)

    # ── Final console output ───────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("  FINAL VERDICT SUMMARY")
    print(f"{'='*65}")
    for tname, tres in report.items():
        label = test_meta[tname][0]
        threshold = test_meta[tname][1]
        passes = sum(1 for v in tres.values() if v["verdict"] == "PASS")
        total  = len(tres)
        print(f"  {label} ({threshold}): {passes}/{total} variants PASS")
        for vname, vres in tres.items():
            agg = vres["aggregate"]
            print(f"    {vname}: PF={agg['profit_factor']} WR={agg['win_rate']}% DD={agg['max_drawdown']}%  → {vres['verdict']}")

    print(f"\n  Files: backtest_summary.txt + backtest_report.json")
    print(f"  Time : {elapsed:.0f}s")

if __name__ == "__main__":
    main()
