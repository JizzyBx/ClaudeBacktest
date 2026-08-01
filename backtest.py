"""
Backtest — Whitelist Run (195 coins, PF≥1.15, ≥20 trades from screening)
Strategy: ADX≥22 + EMA50 slope + EMA9/21 crossover (15m)
Exit:      Fixed percentage TP/SL
Variants:
  G   — TP 3%  / SL 15%
  H   — TP 4%  / SL 15%
  New — TP 4%  / SL 12%

Universe: 195 pre-screened coins (PF≥1.15 & ≥20 trades in screening run)
Period:   Jul 2024 – Jun 2026 | 15m candles
Capital:  $10,000 shared portfolio | Max 6 concurrent positions
Risk:     0.75% of current equity per trade
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

CAPITAL         = 10_000.0
RISK_PER_TRADE  = 0.0075    # 0.75% of current equity
FEE_RATE        = 0.0005    # 0.05% per side
SLIP_RATE       = 0.0002    # 0.02% per side
COST_PER_SIDE   = FEE_RATE + SLIP_RATE
MAX_POSITIONS   = 6         # portfolio-wide cap

ADX_MIN         = 22
SLOPE_THRESH    = 0.05      # EMA50 must move 0.05% over 10 bars
INTERVAL        = "15m"
WORKERS         = 50

MONTHS = [
    (2024,7),(2024,8),(2024,9),(2024,10),(2024,11),(2024,12),
    (2025,1),(2025,2),(2025,3),(2025,4),(2025,5),(2025,6),
    (2025,7),(2025,8),(2025,9),(2025,10),(2025,11),(2025,12),
    (2026,1),(2026,2),(2026,3),(2026,4),(2026,5),(2026,6),
]
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

SYMBOLS = [
    "0GUSDT","1000000BOBUSDT","1000BONKUSDT","1000CATUSDT","1000RATSUSDT",
    "1000SATSUSDT","1000SHIBUSDT","A2ZUSDT","ACEUSDT","ACHUSDT",
    "ACXUSDT","ADAUSDT","AI16ZUSDT","AINUSDT","AIOTUSDT",
    "AKTUSDT","ALGOUSDT","ALICEUSDT","ALPINEUSDT","ANIMEUSDT",
    "ANKRUSDT","API3USDT","ARKMUSDT","ASRUSDT","ASTERUSDT",
    "ATAUSDT","AUSDT","AWEUSDT","AXLUSDT","BANANAUSDT",
    "BANDUSDT","BANKUSDT","BASEDUSDT","BASUSDT","BATUSDT",
    "BCHUSDT","BDXNUSDT","BELUSDT","BIDUSDT","BLZUSDT",
    "BMTUSDT","BOMEUSDT","BSWUSDT","BTRUSDT","CFXUSDT",
    "CHIPUSDT","COAIUSDT","COMBOUSDT","COMMONUSDT","COTIUSDT",
    "CRCLUSDT","CUSDT","DAMUSDT","DEFIUSDT","DEXEUSDT",
    "DIAUSDT","DMCUSDT","DOODUSDT","DUSDT","EIGENUSDT",
    "ELSAUSDT","ENAUSDT","EPICUSDT","EPTUSDT","ESPUSDT",
    "ETCUSDT","ETHUSDT","ETHWUSDT","EVAAUSDT","FIDAUSDT",
    "FIOUSDT","FISUSDT","FLNCUSDT","FLUXUSDT","FOGOUSDT",
    "FOLKSUSDT","FORMUSDT","FRAXUSDT","FUNUSDT","FXSUSDT",
    "GLMUSDT","GRIFFAINUSDT","GUAUSDT","GUNUSDT","HAEDALUSDT",
    "HANAUSDT","HEMIUSDT","ICPUSDT","ICXUSDT","ILVUSDT",
    "INITUSDT","IOSTUSDT","IOTXUSDT","IOUSDT","IPUSDT",
    "KEYUSDT","KITEUSDT","LABUSDT","LAYERUSDT","LIGHTUSDT",
    "LOKAUSDT","LRCUSDT","LYNUSDT","MAGICUSDT","MAVUSDT",
    "MEGAUSDT","MILKUSDT","MITOUSDT","MOODENGUSDT","MORPHOUSDT",
    "MTLUSDT","MYROUSDT","NFPUSDT","NMRUSDT","NOMUSDT",
    "NOTUSDT","NTRNUSDT","OBOLUSDT","OMGUSDT","ONEUSDT",
    "OPENUSDT","OPNUSDT","ORBSUSDT","PEOPLEUSDT","PHBUSDT",
    "PIPPINUSDT","PIXELUSDT","PLAYUSDT","PLUMEUSDT","POLUSDT",
    "POWERUSDT","POWRUSDT","PROMPTUSDT","PTBUSDT","PUFFERUSDT",
    "PUMPBTCUSDT","PUNDIXUSDT","QUICKUSDT","RAVEUSDT","REEFUSDT",
    "RENDERUSDT","RESOLVUSDT","REZUSDT","RLSUSDT","RPLUSDT",
    "RVVUSDT","SAGAUSDT","SAHARAUSDT","SANTOSUSDT","SEIUSDT",
    "SIGNUSDT","SKATEUSDT","SKLUSDT","SKRUSDT","SNDKUSDT",
    "SOMIUSDT","SOPHUSDT","SPELLUSDT","SPKUSDT","STABLEUSDT",
    "STBLUSDT","STXUSDT","SYNUSDT","SYRUPUSDT","TAIKOUSDT",
    "THEUSDT","TNSRUSDT","TRBUSDT","TRUTHUSDT","TRUUSDT",
    "TURBOUSDT","TWTUSDT","UBUSDT","USELESSUSDT","USUALUSDT",
    "UXLINKUSDT","VANRYUSDT","VINEUSDT","VIRTUALUSDT","VOXELUSDT",
    "VVVUSDT","WAXPUSDT","WLDUSDT","XCNUSDT","XEMUSDT",
    "XLMUSDT","XNYUSDT","XPLUSDT","XRPUSDT","YALAUSDT",
    "YBUSDT","ZECUSDT","ZENUSDT","ZEREBROUSDT","ZKJUSDT",
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

# ── Per-symbol signal generation (returns signals, NOT resolved trades) ───────
def generate_signals(symbol):
    candles = fetch_candles(symbol)
    if len(candles) < 150:
        return symbol, None

    ts_arr  = [c[0] for c in candles]
    highs   = [c[1] for c in candles]
    lows    = [c[2] for c in candles]
    closes  = [c[3] for c in candles]
    n       = len(candles)

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    WARMUP = 60
    signals = []  # list of (bar_idx, signal, entry_price, highs, lows, ts_arr)

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
        if adx < ADX_MIN:
            continue

        signal = "buy" if crossed_up else "sell"
        signals.append((i, signal, closes[i]))

    return symbol, signals, highs, lows, ts_arr, n, closes

# ── Portfolio-level simulation with position cap ──────────────────────────────
def run_portfolio(all_signals_data, vname, vcfg):
    """
    Merge all coin signals by timestamp, enforce MAX_POSITIONS cap,
    simulate trades with shared equity, compound risk.
    Returns list of trade dicts.
    """
    tp_pct = vcfg["tp_pct"] / 100.0
    sl_pct = vcfg["sl_pct"] / 100.0

    # Build a global event list: (entry_ts, symbol, signal, entry_price, highs, lows, ts_arr, entry_bar, n, closes)
    all_entries = []
    for sym, (signals, highs, lows, ts_arr, n, closes) in all_signals_data.items():
        for (bar_idx, signal, entry_price) in signals:
            # Entry on NEXT bar after signal bar
            entry_bar = bar_idx + 1
            if entry_bar >= n:
                continue
            actual_entry = closes[entry_bar]
            all_entries.append((
                ts_arr[entry_bar], sym, signal, actual_entry,
                highs, lows, ts_arr, entry_bar, n, closes
            ))

    # Sort by entry timestamp
    all_entries.sort(key=lambda x: x[0])

    equity = CAPITAL
    open_positions = []   # list of position dicts
    trades = []

    for event in all_entries:
        entry_ts, sym, signal, entry_price, highs, lows, ts_arr, entry_bar, n, closes = event

        # First: close any open positions that have resolved by this entry_ts
        still_open = []
        for pos in open_positions:
            if pos["exit_ts"] <= entry_ts:
                # Position closed before this new entry
                equity += pos["pnl"]
                trades.append(pos["trade"])
            else:
                still_open.append(pos)
        open_positions = still_open

        # Enforce position cap
        if len(open_positions) >= MAX_POSITIONS:
            continue

        # Compute TP/SL prices
        if signal == "buy":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        # Forward-scan to resolve this trade
        outcome    = "timeout"
        exit_price = closes[-1]
        exit_bar   = n - 1
        exit_ts    = ts_arr[exit_bar]

        for j in range(entry_bar + 1, n):
            h = highs[j]; l = lows[j]
            if signal == "buy":
                if l <= sl_price:
                    outcome = "sl"; exit_price = sl_price; exit_bar = j; exit_ts = ts_arr[j]; break
                if h >= tp_price:
                    outcome = "tp"; exit_price = tp_price; exit_bar = j; exit_ts = ts_arr[j]; break
            else:
                if h >= sl_price:
                    outcome = "sl"; exit_price = sl_price; exit_bar = j; exit_ts = ts_arr[j]; break
                if l <= tp_price:
                    outcome = "tp"; exit_price = tp_price; exit_bar = j; exit_ts = ts_arr[j]; break

        # PnL with compound equity
        risk_dollar = equity * RISK_PER_TRADE
        if signal == "buy":
            raw_ret = (exit_price - entry_price) / entry_price
        else:
            raw_ret = (entry_price - exit_price) / entry_price
        net_ret = raw_ret - COST_PER_SIDE * 2
        pnl = risk_dollar * (net_ret / sl_pct)

        trade = {
            "symbol":    sym,
            "signal":    signal,
            "entry":     entry_price,
            "exit":      exit_price,
            "outcome":   outcome,
            "pnl":       round(pnl, 4),
            "win":       pnl > 0,
            "entry_ts":  entry_ts,
            "exit_ts":   exit_ts,
            "bars":      exit_bar - entry_bar,
        }

        open_positions.append({
            "exit_ts": exit_ts,
            "pnl":     pnl,
            "trade":   trade,
        })

    # Flush remaining open positions
    for pos in open_positions:
        equity += pos["pnl"]
        trades.append(pos["trade"])

    return trades

# ── Aggregate stats ───────────────────────────────────────────────────────────
def calc_stats(trades):
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

    # Max drawdown on running equity
    running = CAPITAL; peak = CAPITAL; max_dd = 0.0
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

    # Per-coin stats
    coin_trades = defaultdict(list)
    for t in trades:
        coin_trades[t["symbol"]].append(t)

    per_coin = {}
    for sym, ctrades in coin_trades.items():
        cw = [t for t in ctrades if t["win"]]
        cl = [t for t in ctrades if not t["win"]]
        ct = len(ctrades)
        cwr = len(cw) / ct * 100
        cgp = sum(t["pnl"] for t in cw)
        cgl = abs(sum(t["pnl"] for t in cl))
        cpf = cgp / cgl if cgl else float("inf")
        per_coin[sym] = {
            "total_trades": ct,
            "wins":         len(cw),
            "losses":       len(cl),
            "win_rate":     round(cwr, 2),
            "profit_factor": round(cpf, 4) if cpf != float("inf") else 9999,
            "net_pnl":      round(cgp - cgl, 2),
        }

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
        "per_coin":        per_coin,
        "usable":          pf >= 1.5 and wr >= 42,
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 65)
    print("  WHITELIST BACKTEST — Variants G / H / New")
    print(f"  {len(SYMBOLS)} coins | Jul 2024–Jun 2026 | 15m | Max {MAX_POSITIONS} positions")
    print("=" * 65)

    # Phase 1 — parallel fetch + signal generation
    print(f"\n[Phase 1] Downloading & generating signals ({WORKERS} workers)…")
    all_signals_data = {}  # symbol -> (signals, highs, lows, ts_arr, n, closes)
    done = failed = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(generate_signals, sym): sym for sym in SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                result = fut.result()
                if result[1] is None:
                    failed += 1
                else:
                    sym_out, signals, highs, lows, ts_arr, n, closes = result
                    all_signals_data[sym_out] = (signals, highs, lows, ts_arr, n, closes)
            except Exception as e:
                failed += 1
                print(f"  ERROR {sym}: {e}")
            if done % 50 == 0 or done == len(SYMBOLS):
                print(f"  [{done}/{len(SYMBOLS)}] done | {len(all_signals_data)} with data | {failed} skipped")

    if not all_signals_data:
        print("\n⛔ ABORT: 0 symbols returned data.")
        print("   data.binance.vision may be blocked on this runner.")
        return

    if len(all_signals_data) < len(SYMBOLS) * 0.1:
        print(f"\n⛔ ABORT: Only {len(all_signals_data)}/{len(SYMBOLS)} symbols loaded.")
        print("   Looks like data source is blocked. Check runner network.")
        return

    print(f"\n  ✅ {len(all_signals_data)} symbols loaded | {failed} skipped")

    # Phase 2 — portfolio simulation per variant
    print("\n[Phase 2] Running portfolio simulation…")
    report = {}
    summary_lines = []

    for vname, vcfg in VARIANTS.items():
        tp = vcfg["tp_pct"]; sl = vcfg["sl_pct"]
        trades = run_portfolio(all_signals_data, vname, vcfg)
        agg = calc_stats(trades)
        if not agg:
            print(f"  {vname}: no trades")
            continue

        verdict = "✅ USABLE" if agg["usable"] else "❌ NOT USABLE"
        print(f"  {vname} (TP {tp}% SL {sl}%): {agg['total_trades']} trades | "
              f"WR {agg['win_rate']}% | PF {agg['profit_factor']} | "
              f"Net ${agg['net_pnl']} | {verdict}")

        # Sort per-coin by PF descending
        coin_table = sorted(
            agg["per_coin"].items(),
            key=lambda x: (x[1]["profit_factor"] if x[1]["profit_factor"] != 9999 else 99999, x[1]["net_pnl"]),
            reverse=True
        )

        report[vname] = {
            "aggregate":  agg,
            "coin_table": coin_table,
            "tp_pct":     tp,
            "sl_pct":     sl,
        }
        summary_lines.append(
            f"Variant {vname} (TP {tp}% SL {sl}%): "
            f"{agg['total_trades']} trades | WR {agg['win_rate']}% | "
            f"PF {agg['profit_factor']} | Net ${agg['net_pnl']} | "
            f"DD {agg['max_drawdown']}% | {verdict}"
        )

    elapsed = time.time() - t0

    # ── backtest_summary.txt ──────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write("WHITELIST BACKTEST SUMMARY\n")
        f.write("=" * 65 + "\n")
        f.write(f"Strategy : ADX≥{ADX_MIN} + EMA50 slope({SLOPE_THRESH}%) + EMA9/21 cross\n")
        f.write(f"Timeframe: 15m | Period: Jul 2024 – Jun 2026\n")
        f.write(f"Universe : {len(SYMBOLS)} coins (pre-screened PF≥1.15 & ≥20 trades)\n")
        f.write(f"Mode     : PORTFOLIO (max {MAX_POSITIONS} concurrent, shared $10k equity)\n")
        f.write(f"Risk/trade: {RISK_PER_TRADE*100}% of current equity (compound)\n")
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

            f.write(f"ALL COINS by Profit Factor:\n")
            f.write(f"  {'Symbol':<22} {'PF':>7}  {'WR':>6}  {'Trades':>7}  {'Net PnL':>10}\n")
            f.write(f"  {'-'*58}\n")
            for sym, cs in res["coin_table"]:
                pf_str = f"{cs['profit_factor']:.3f}" if cs["profit_factor"] != 9999 else "  inf"
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
            "mode":              "portfolio_whitelist",
            "timeframe":         INTERVAL,
            "period":            "2024-07 to 2026-06",
            "symbols_total":     len(SYMBOLS),
            "capital_base":      CAPITAL,
            "risk_pct":          RISK_PER_TRADE * 100,
            "fee_pct":           FEE_RATE * 100,
            "slip_pct":          SLIP_RATE * 100,
            "max_positions":     MAX_POSITIONS,
            "adx_min":           ADX_MIN,
            "slope_thresh":      SLOPE_THRESH,
            "entry_rule":        "next_bar_close_after_signal",
            "run_seconds":       round(elapsed, 1),
        },
        "variants": {},
    }

    for vname, res in report.items():
        agg = res["aggregate"]
        json_out["variants"][vname] = {
            "aggregate": {k: v for k, v in agg.items() if k not in ("per_coin",)},
            "per_coin": [
                {"symbol": sym, **cs}
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

