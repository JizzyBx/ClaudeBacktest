"""
SuperTrend + Multi-Filter Backtest (Grok v8 — Fixed)
  S1 = SuperTrend 30m flip + 4H SuperTrend filter + ADX/DI/ATR filters
  S2 = Same but NO 4H SuperTrend filter (more aggressive)

Fixes applied vs previous version:
  1. Data source → Binance FUTURES (fapi.binance.com) not Spot
  2. Global equity — one account shared across all coins
  3. Max 6 concurrent positions enforced across all coins

Period : 2024-07-01 → 2026-07-01
Output : backtest_report.json + backtest_summary.txt
"""

import json, math, time, datetime, statistics, urllib.request, urllib.error
from collections import defaultdict

# ── COIN LIST ──────────────────────────────────────────────────────────────────
ALL_COINS = [
    "ETHUSDT","DOGEUSDT","DOTUSDT","ARBUSDT",
    "1000BONKUSDT","1000PEPEUSDT","1000SHIBUSDT",
    "ADAUSDT","APTUSDT","LINKUSDT","SOLUSDT",
    "SUIUSDT","1000FLOKIUSDT","WIFUSDT",
    "BTCUSDT","BNBUSDT","NEARUSDT",
    "XRPUSDT","AVAXUSDT","LTCUSDT",
    "ATOMUSDT","OPUSDT","INJUSDT","UNIUSDT","AAVEUSDT","HBARUSDT",
    "TRUMPUSDT","BOMEUSDT","WLDUSDT","NEIROUSDT",
]

# ── CONFIG ─────────────────────────────────────────────────────────────────────
FEE_RATE       = 0.0005
SLIPPAGE       = 0.0002
INITIAL_CAP    = 10_000.0
RISK_PER_TRADE = 0.0075
ATR_PERIOD     = 14
ST_ATR_PERIOD  = 10
ST_MULT        = 3.0
ADX_PERIOD     = 14
ADX_MIN        = 25
DI_SEP_MIN     = 8
DIST_ATR_MULT  = 1.5
COOLDOWN_BARS  = 3
MAX_POSITIONS  = 6
STOP_ATR_MULT  = 2.0

START_MS = int(datetime.datetime(2024, 7, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
END_MS   = int(datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)

# ── FUTURES ENDPOINT ──────────────────────────────────────────────────────────
# Using Binance Futures public endpoint — no API key required
FUTURES_URL = "https://fapi.binance.com/fapi/v1/klines"

# ── DATA FETCH ─────────────────────────────────────────────────────────────────
def fetch_klines(symbol, interval, start_ms, end_ms):
    all_rows = []
    cur = start_ms
    while cur < end_ms:
        url = (f"{FUTURES_URL}?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={end_ms}&limit=1500")
        for attempt in range(5):
            try:
                with urllib.request.urlopen(url, timeout=25) as r:
                    data = json.loads(r.read())
                break
            except Exception as e:
                if attempt == 4:
                    raise
                time.sleep(0.5 * (2 ** attempt))
        if not data:
            break
        all_rows.extend(data)
        last_open = data[-1][0]
        if last_open >= end_ms or len(data) < 1500:
            break
        cur = last_open + 1
        time.sleep(0.12)
    return all_rows

def parse_klines(raw):
    times, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for row in raw:
        times.append(int(row[0]))
        opens.append(float(row[1]))
        highs.append(float(row[2]))
        lows.append(float(row[3]))
        closes.append(float(row[4]))
        volumes.append(float(row[5]))
    return times, opens, highs, lows, closes, volumes

# ── INDICATORS ─────────────────────────────────────────────────────────────────
def calc_atr(highs, lows, closes, period=14):
    n = len(closes)
    result = [None] * n
    if n < period + 1:
        return result
    tr = [0.0] * n
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i-1]),
                    abs(lows[i]  - closes[i-1]))
    # Wilder seed
    result[period] = sum(tr[1:period+1]) / period
    for i in range(period + 1, n):
        result[i] = (result[i-1] * (period - 1) + tr[i]) / period
    return result

def calc_adx(highs, lows, closes, period=14):
    """
    Returns (adx, pdi, mdi) — all None before warmup (2*period bars).
    Uses true Wilder smoothing: seed = sum of first period, then rolling subtract+add.
    """
    n = len(closes)
    tr_raw  = [0.0] * n
    pdm_raw = [0.0] * n
    mdm_raw = [0.0] * n
    for i in range(1, n):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_raw[i] = up   if (up > down   and up   > 0) else 0.0
        mdm_raw[i] = down if (down > up   and down > 0) else 0.0
        tr_raw[i]  = max(highs[i] - lows[i],
                         abs(highs[i]  - closes[i-1]),
                         abs(lows[i]   - closes[i-1]))

    def wilder(raw, p):
        res = [None] * n
        if n < p + 1:
            return res
        res[p] = sum(raw[1:p+1])
        for i in range(p + 1, n):
            res[i] = res[i-1] - res[i-1] / p + raw[i]
        return res

    s_tr  = wilder(tr_raw,  period)
    s_pdm = wilder(pdm_raw, period)
    s_mdm = wilder(mdm_raw, period)

    pdi = [None] * n
    mdi = [None] * n
    dx  = [None] * n
    for i in range(period, n):
        if s_tr[i] and s_tr[i] != 0:
            pdi[i] = 100.0 * s_pdm[i] / s_tr[i]
            mdi[i] = 100.0 * s_mdm[i] / s_tr[i]
            dsum = pdi[i] + mdi[i]
            if dsum != 0:
                dx[i] = 100.0 * abs(pdi[i] - mdi[i]) / dsum

    adx = [None] * n
    seed_end = period * 2
    if seed_end >= n:
        return adx, pdi, mdi
    valid_dx = [dx[i] for i in range(period, seed_end + 1) if dx[i] is not None]
    if len(valid_dx) < period:
        return adx, pdi, mdi
    adx[seed_end] = sum(valid_dx[-period:]) / period
    for i in range(seed_end + 1, n):
        if dx[i] is not None and adx[i-1] is not None:
            adx[i] = (adx[i-1] * (period - 1) + dx[i]) / period
    return adx, pdi, mdi

def calc_supertrend(highs, lows, closes, atr_period=10, mult=3.0):
    """Returns (st_line, direction) — direction: +1=bull, -1=bear, None before warmup."""
    n = len(closes)
    atr_vals = calc_atr(highs, lows, closes, atr_period)
    st   = [None] * n
    dire = [None] * n
    ub   = [None] * n
    lb   = [None] * n
    for i in range(n):
        if atr_vals[i] is None:
            continue
        mid     = (highs[i] + lows[i]) / 2.0
        raw_ub  = mid + mult * atr_vals[i]
        raw_lb  = mid - mult * atr_vals[i]
        if ub[i-1] is None:
            ub[i] = raw_ub
            lb[i] = raw_lb
        else:
            ub[i] = raw_ub if (raw_ub < ub[i-1] or closes[i-1] > ub[i-1]) else ub[i-1]
            lb[i] = raw_lb if (raw_lb > lb[i-1] or closes[i-1] < lb[i-1]) else lb[i-1]
        if dire[i-1] is None:
            dire[i] = 1 if closes[i] > lb[i] else -1
        elif dire[i-1] == 1:
            dire[i] = -1 if closes[i] < lb[i] else 1
        else:
            dire[i] =  1 if closes[i] > ub[i] else -1
        st[i] = lb[i] if dire[i] == 1 else ub[i]
    return st, dire

# ── 4H TREND LOOKUP ───────────────────────────────────────────────────────────
def build_htf_map(h4_times, h4_highs, h4_lows, h4_closes):
    _, dire = calc_supertrend(h4_highs, h4_lows, h4_closes, ST_ATR_PERIOD, ST_MULT)
    trend_map = {}
    for i, t in enumerate(h4_times):
        if dire[i] is not None:
            trend_map[t] = dire[i]
    sorted_ts = sorted(trend_map.keys())
    return trend_map, sorted_ts

def get_htf_trend(bar_ts, sorted_ts, trend_map):
    lo, hi, idx = 0, len(sorted_ts) - 1, -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if sorted_ts[mid] <= bar_ts:
            idx = mid; lo = mid + 1
        else:
            hi = mid - 1
    return trend_map[sorted_ts[idx]] if idx >= 0 else None

# ── PRE-COMPUTE INDICATORS PER COIN ──────────────────────────────────────────
def precompute(symbol, h30_raw, h4_raw, use_htf):
    """Returns dict of all indicator arrays + 4H map, keyed by bar index."""
    if not h30_raw or len(h30_raw) < 100:
        return None
    times, _, highs, lows, closes, _ = parse_klines(h30_raw)
    atr14        = calc_atr(highs, lows, closes, ATR_PERIOD)
    adx, pdi, mdi = calc_adx(highs, lows, closes, ADX_PERIOD)
    st_line, st_dir = calc_supertrend(highs, lows, closes, ST_ATR_PERIOD, ST_MULT)

    htf_map = {}
    htf_ts  = []
    if use_htf and h4_raw and len(h4_raw) >= 50:
        t4, _, h4h, h4l, h4c, _ = parse_klines(h4_raw)
        htf_map, htf_ts = build_htf_map(t4, h4h, h4l, h4c)

    return dict(
        symbol=symbol, times=times, highs=highs, lows=lows, closes=closes,
        atr14=atr14, adx=adx, pdi=pdi, mdi=mdi,
        st_line=st_line, st_dir=st_dir,
        htf_map=htf_map, htf_ts=htf_ts,
        n=len(closes),
    )

# ── PORTFOLIO SIMULATION ──────────────────────────────────────────────────────
def simulate_portfolio(all_coin_data, use_htf):
    """
    Time-aligned portfolio simulation.
    One global equity, max MAX_POSITIONS open at once.
    Returns (all_trades, filter_counts_per_coin)
    """
    # Build a unified sorted timeline of all 30m bar timestamps
    all_times = set()
    for cd in all_coin_data.values():
        all_times.update(cd["times"])
    timeline = sorted(all_times)

    # Index: symbol → {timestamp: bar_index}
    time_index = {}
    for sym, cd in all_coin_data.items():
        time_index[sym] = {t: i for i, t in enumerate(cd["times"])}

    # Per-symbol state
    coin_state = {}
    for sym in all_coin_data:
        coin_state[sym] = dict(
            in_trade=None,
            cooldown=0,
            fc=defaultdict(int),
        )

    equity    = INITIAL_CAP
    open_pos  = {}   # sym → trade dict
    all_trades = []

    for ts in timeline:
        # ── EXITS first (process before entries on same bar) ──
        to_close = []
        for sym, pos in open_pos.items():
            cd  = all_coin_data[sym]
            idx = time_index[sym].get(ts)
            if idx is None:
                continue
            lows_  = cd["lows"]
            highs_ = cd["highs"]
            closes_= cd["closes"]
            st_dir = cd["st_dir"]

            exit_p      = None
            exit_reason = None

            if pos["dir"] == "LONG":
                if lows_[idx] <= pos["sl"]:
                    exit_p = pos["sl"]; exit_reason = "SL"
                elif st_dir[idx] == -1 and st_dir[idx-1] == 1 if idx > 0 else False:
                    exit_p = closes_[idx]; exit_reason = "ST_FLIP"
            else:
                if highs_[idx] >= pos["sl"]:
                    exit_p = pos["sl"]; exit_reason = "SL"
                elif st_dir[idx] == 1 and st_dir[idx-1] == -1 if idx > 0 else False:
                    exit_p = closes_[idx]; exit_reason = "ST_FLIP"

            if exit_p is not None:
                if pos["dir"] == "LONG":
                    exit_adj = exit_p * (1 - SLIPPAGE) * (1 - FEE_RATE)
                    pnl = (exit_adj - pos["entry_adj"]) * pos["size"]
                else:
                    exit_adj = exit_p * (1 + SLIPPAGE) * (1 + FEE_RATE)
                    pnl = (pos["entry_adj"] - exit_adj) * pos["size"]

                equity += pnl
                dur = (ts - pos["entry_t"]) / (1000 * 60 * 30)
                all_trades.append(dict(
                    symbol=sym, dir=pos["dir"],
                    entry=pos["entry"], exit=exit_p,
                    entry_t=pos["entry_t"], exit_t=ts,
                    pnl=round(pnl, 4), win=(pnl > 0),
                    dur=dur, exit_reason=exit_reason,
                ))
                coin_state[sym]["cooldown"] = COOLDOWN_BARS
                to_close.append(sym)

        for sym in to_close:
            del open_pos[sym]

        # ── ENTRIES ──
        if len(open_pos) >= MAX_POSITIONS:
            continue

        for sym, cd in all_coin_data.items():
            if sym in open_pos:
                continue
            if coin_state[sym]["cooldown"] > 0:
                coin_state[sym]["cooldown"] -= 1
                continue
            if len(open_pos) >= MAX_POSITIONS:
                break

            idx = time_index[sym].get(ts)
            if idx is None or idx < 2:
                continue

            fc = coin_state[sym]["fc"]
            fc["total_candles"] += 1

            atr14  = cd["atr14"]
            adx    = cd["adx"]
            pdi    = cd["pdi"]
            mdi    = cd["mdi"]
            st_dir = cd["st_dir"]
            st_line= cd["st_line"]
            closes_= cd["closes"]
            times_ = cd["times"]

            # Need valid indicators
            if (atr14[idx] is None or adx[idx] is None or
                pdi[idx] is None or mdi[idx] is None or
                st_dir[idx] is None or st_dir[idx-1] is None or st_dir[idx-2] is None):
                continue

            # ST flip detection
            flipped_long  = (st_dir[idx] == 1  and st_dir[idx-1] == -1)
            flipped_short = (st_dir[idx] == -1 and st_dir[idx-1] == 1)
            if not flipped_long and not flipped_short:
                fc["no_flip"] += 1
                continue

            direction = "LONG" if flipped_long else "SHORT"

            # F1: 4H HTF alignment
            if use_htf and cd["htf_ts"]:
                h4t = get_htf_trend(ts, cd["htf_ts"], cd["htf_map"])
                if h4t is None or (direction == "LONG" and h4t != 1) or (direction == "SHORT" and h4t != -1):
                    fc["htf_filter"] += 1
                    continue

            # F2: ADX >= 25
            if adx[idx] < ADX_MIN:
                fc["adx_min"] += 1
                continue

            # F3: ADX rising vs 2 bars ago
            if adx[idx-2] is None or adx[idx] <= adx[idx-2]:
                fc["adx_rising"] += 1
                continue

            # F4: Distance from ST <= 1.5 * ATR14
            if st_line[idx] is None:
                continue
            dist = abs(closes_[idx] - st_line[idx])
            if dist > DIST_ATR_MULT * atr14[idx]:
                fc["dist_filter"] += 1
                continue

            # F5: DI dominance + separation >= 8
            if pdi[idx] is None or mdi[idx] is None:
                fc["di_sep"] += 1
                continue
            if direction == "LONG":
                if pdi[idx] <= mdi[idx] or (pdi[idx] - mdi[idx]) < DI_SEP_MIN:
                    fc["di_sep"] += 1
                    continue
            else:
                if mdi[idx] <= pdi[idx] or (mdi[idx] - pdi[idx]) < DI_SEP_MIN:
                    fc["di_sep"] += 1
                    continue

            fc["signals_generated"] += 1

            # Position sizing on global equity
            stop_dist = STOP_ATR_MULT * atr14[idx]
            if stop_dist <= 0:
                continue
            risk_amt = equity * RISK_PER_TRADE
            size     = risk_amt / stop_dist

            entry_p = closes_[idx]
            if direction == "LONG":
                entry_adj = entry_p * (1 + SLIPPAGE) * (1 + FEE_RATE)
                sl_price  = entry_p - stop_dist
            else:
                entry_adj = entry_p * (1 - SLIPPAGE) * (1 - FEE_RATE)
                sl_price  = entry_p + stop_dist

            open_pos[sym] = dict(
                dir=direction, entry=entry_p, entry_adj=entry_adj,
                sl=sl_price, size=size, entry_t=ts,
            )

    return all_trades, {sym: dict(coin_state[sym]["fc"]) for sym in coin_state}

# ── METRICS ───────────────────────────────────────────────────────────────────
def compute_metrics(trades, symbol="ALL"):
    if not trades:
        return None
    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    n      = len(trades)
    wr     = len(wins) / n
    gp     = sum(t["pnl"] for t in wins)
    gl     = abs(sum(t["pnl"] for t in losses)) if losses else 0
    pf     = gp / gl if gl > 0 else (float("inf") if gp > 0 else 0)
    net    = sum(t["pnl"] for t in trades)
    aw     = gp / len(wins)   if wins   else 0
    al     = gl / len(losses) if losses else 0
    exp    = wr * aw - (1 - wr) * al

    # Global equity curve for MDD
    equity = INITIAL_CAP; peak = INITIAL_CAP; mdd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_t"]):
        equity += t["pnl"]
        if equity > peak: peak = equity
        dd = (peak - equity) / peak
        if dd > mdd: mdd = dd

    # Monthly PnL
    monthly = defaultdict(float)
    for t in trades:
        mo = datetime.datetime.utcfromtimestamp(t["exit_t"]/1000).strftime("%Y-%m")
        monthly[mo] += t["pnl"]
    mo_vals = list(monthly.values())
    sharpe  = sortino = 0.0
    if len(mo_vals) > 1:
        avg_r = statistics.mean(mo_vals)
        std_r = statistics.stdev(mo_vals)
        if std_r > 0:
            sharpe = avg_r / std_r * math.sqrt(12)
        neg = [r for r in mo_vals if r < 0]
        if len(neg) > 1:
            dstd = statistics.stdev(neg)
            if dstd > 0:
                sortino = avg_r / dstd * math.sqrt(12)

    nlongs  = sum(1 for t in trades if t.get("dir") == "LONG")
    nshorts = sum(1 for t in trades if t.get("dir") == "SHORT")
    lwr = sum(1 for t in trades if t.get("dir")=="LONG"  and t["win"]) / nlongs  if nlongs  else 0
    swr = sum(1 for t in trades if t.get("dir")=="SHORT" and t["win"]) / nshorts if nshorts else 0

    maxcw = maxcl = cw = cl = 0
    for t in trades:
        if t["win"]: cw += 1; cl = 0; maxcw = max(maxcw, cw)
        else:        cl += 1; cw = 0; maxcl = max(maxcl, cl)

    avg_dur = statistics.mean([t["dur"] for t in trades])

    return dict(
        symbol=symbol, n=n, wr=round(wr,4), pf=round(pf,4),
        net=round(net,2), mdd=round(mdd,4), sharpe=round(sharpe,3),
        sortino=round(sortino,3), aw=round(aw,2), al=round(al,2),
        exp=round(exp,2), dur=round(avg_dur,1),
        nlongs=nlongs, nshorts=nshorts, lwr=round(lwr,4), swr=round(swr,4),
        monthly=dict(sorted(monthly.items())),
        maxcw=maxcw, maxcl=maxcl, gp=round(gp,2), gl=round(gl,2),
    )

# ── SUMMARY WRITER ────────────────────────────────────────────────────────────
def strategy_summary(strat_name, all_trades, per_coin_metrics, filter_agg):
    lines = [f"\n{'='*72}", f"  {strat_name}", f"{'='*72}"]
    if not all_trades:
        lines.append("  NO TRADES GENERATED")
        lines.append(f"\n  FILTER REJECTION STATS")
        lines.append(f"  {'─'*40}")
        lines.append(f"  Total candles scanned : {filter_agg.get('total_candles',0):,}")
        lines.append(f"  No ST flip            : {filter_agg.get('no_flip',0):,}")
        lines.append(f"  4H HTF filter         : {filter_agg.get('htf_filter',0):,}")
        lines.append(f"  ADX < {ADX_MIN}             : {filter_agg.get('adx_min',0):,}")
        lines.append(f"  ADX not rising        : {filter_agg.get('adx_rising',0):,}")
        lines.append(f"  Dist > 1.5×ATR        : {filter_agg.get('dist_filter',0):,}")
        lines.append(f"  DI separation < {DI_SEP_MIN}   : {filter_agg.get('di_sep',0):,}")
        lines.append(f"  Signals generated     : {filter_agg.get('signals_generated',0):,}")
        return "\n".join(lines)

    agg = compute_metrics(all_trades, "AGGREGATE")
    if agg:
        lines += [
            f"\n  AGGREGATE RESULTS", f"  {'─'*50}",
            f"  Total Trades : {agg['n']}",
            f"  Win Rate     : {agg['wr']*100:.1f}%",
            f"  Profit Factor: {agg['pf']:.4f}  {'✅ PASS' if agg['pf'] >= 1.5 else '❌ FAIL'}",
            f"  Net PnL      : ${agg['net']:,.2f}",
            f"  Max Drawdown : {agg['mdd']*100:.1f}%",
            f"  Sharpe       : {agg['sharpe']:.3f}",
            f"  Sortino      : {agg['sortino']:.3f}",
            f"  Avg Win      : ${agg['aw']:.2f}",
            f"  Avg Loss     : ${agg['al']:.2f}",
            f"  Expectancy   : ${agg['exp']:.2f}",
            f"  Avg Duration : {agg['dur']:.1f} bars",
            f"  Longs/Shorts : {agg['nlongs']}/{agg['nshorts']}",
            f"  Long WR      : {agg['lwr']*100:.1f}%",
            f"  Short WR     : {agg['swr']*100:.1f}%",
            f"  Max Win Streak: {agg['maxcw']}  Max Loss Streak: {agg['maxcl']}",
            f"\n  VALIDATION: PF≥1.5 {'✅' if agg['pf']>=1.5 else '❌'}  WR≥42% {'✅' if agg['wr']>=0.42 else '❌'}",
        ]

    # Per-coin
    valid = [m for m in per_coin_metrics if m]
    valid.sort(key=lambda x: x["pf"], reverse=True)
    pf_pass = sum(1 for m in valid if m["pf"] >= 1.5)
    lines.append(f"  PF≥1.5 on {pf_pass}/{len(valid)} coins")
    lines.append(f"\n  PER-COIN BREAKDOWN (sorted by PF desc)")
    lines.append(f"  {'Symbol':<18} {'Trades':>6} {'WR':>7} {'PF':>8} {'Net PnL':>10} {'MDD':>6} {'L/S':>5}")
    lines.append(f"  {'─'*65}")
    for m in valid:
        mk = "✅" if m["pf"] >= 1.5 else ("⚠️ " if m["pf"] >= 1.0 else "❌")
        lines.append(
            f"  {m['symbol']:<18} {m['n']:>6} {m['wr']*100:>6.1f}%"
            f" {m['pf']:>8.4f}{mk} ${m['net']:>9,.2f} {m['mdd']*100:>5.1f}%"
            f" {m['nlongs']}/{m['nshorts']}"
        )

    # Monthly
    monthly_all = defaultdict(float)
    for t in all_trades:
        mo = datetime.datetime.utcfromtimestamp(t["exit_t"]/1000).strftime("%Y-%m")
        monthly_all[mo] += t["pnl"]
    lines.append(f"\n  MONTHLY PnL")
    lines.append(f"  {'─'*30}")
    for mo in sorted(monthly_all):
        bar = "▲" if monthly_all[mo] >= 0 else "▼"
        lines.append(f"  {mo}  {bar}  ${monthly_all[mo]:>9,.2f}")

    # Filters
    lines += [
        f"\n  FILTER REJECTION STATS (aggregated across all coins)",
        f"  {'─'*40}",
        f"  Total candles scanned : {filter_agg.get('total_candles',0):,}",
        f"  No ST flip            : {filter_agg.get('no_flip',0):,}",
        f"  4H HTF filter         : {filter_agg.get('htf_filter',0):,}",
        f"  ADX < {ADX_MIN}             : {filter_agg.get('adx_min',0):,}",
        f"  ADX not rising        : {filter_agg.get('adx_rising',0):,}",
        f"  Dist > 1.5×ATR        : {filter_agg.get('dist_filter',0):,}",
        f"  DI separation < {DI_SEP_MIN}   : {filter_agg.get('di_sep',0):,}",
        f"  Signals generated     : {filter_agg.get('signals_generated',0):,}",
        f"  Max-positions blocked : {filter_agg.get('max_pos_blocked',0):,}",
    ]
    return "\n".join(lines)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"SuperTrend + Multi-Filter Backtest — Grok v8 (Fixed)")
    print(f"Data source : Binance FUTURES (fapi.binance.com)")
    print(f"Coins       : {len(ALL_COINS)}")
    print(f"Period      : 2024-07-01 → 2026-07-01")
    print(f"Equity      : Global ${INITIAL_CAP:,.0f}, max {MAX_POSITIONS} positions")
    print("=" * 65)

    # ── PHASE 1: FETCH ────────────────────────────────────────────────
    print("\n[PHASE 1] Fetching 30m futures data...")
    raw_30 = {}
    for sym in ALL_COINS:
        print(f"  {sym:<18} 30m...", end=" ", flush=True)
        try:
            raw = fetch_klines(sym, "30m", START_MS, END_MS)
            if raw:
                raw_30[sym] = raw
                print(f"OK ({len(raw):,} candles)")
            else:
                print("EMPTY — skip")
        except Exception as e:
            print(f"FAIL: {e}")

    print(f"\n[PHASE 1b] Fetching 4h futures data...")
    raw_4h = {}
    for sym in ALL_COINS:
        if sym not in raw_30:
            continue
        print(f"  {sym:<18} 4h...", end=" ", flush=True)
        try:
            raw = fetch_klines(sym, "4h", START_MS, END_MS)
            if raw:
                raw_4h[sym] = raw
                print(f"OK ({len(raw):,} candles)")
            else:
                print("EMPTY")
        except Exception as e:
            print(f"FAIL: {e}")

    fetched = [s for s in ALL_COINS if s in raw_30]
    skipped = [s for s in ALL_COINS if s not in raw_30]
    print(f"\n  Fetched: {len(fetched)} coins  |  Skipped: {skipped or 'none'}")

    # ── PHASE 2: PRE-COMPUTE INDICATORS ──────────────────────────────
    print("\n[PHASE 2] Pre-computing indicators...")

    def build_coin_data(use_htf):
        cd = {}
        for sym in fetched:
            result = precompute(sym, raw_30[sym], raw_4h.get(sym,[]), use_htf)
            if result:
                cd[sym] = result
            else:
                print(f"  WARNING: {sym} had insufficient data — skipped")
        return cd

    # ── PHASE 3: SIMULATE ─────────────────────────────────────────────
    print("\n[PHASE 3] Simulating S1 (with 4H filter)...")
    cd_s1 = build_coin_data(use_htf=True)
    s1_trades, s1_fc_per_coin = simulate_portfolio(cd_s1, use_htf=True)

    # Aggregate filter counts
    s1_fc = defaultdict(int)
    for fc in s1_fc_per_coin.values():
        for k, v in fc.items(): s1_fc[k] += v

    # Per-coin metrics
    s1_coin_metrics = []
    for sym in fetched:
        ct = [t for t in s1_trades if t.get("symbol") == sym]
        s1_coin_metrics.append(compute_metrics(ct, sym) if ct else None)

    print(f"  S1 total trades: {len(s1_trades)}")

    print("\n[PHASE 4] Simulating S2 (no 4H filter)...")
    cd_s2 = build_coin_data(use_htf=False)
    s2_trades, s2_fc_per_coin = simulate_portfolio(cd_s2, use_htf=False)

    s2_fc = defaultdict(int)
    for fc in s2_fc_per_coin.values():
        for k, v in fc.items(): s2_fc[k] += v

    s2_coin_metrics = []
    for sym in fetched:
        ct = [t for t in s2_trades if t.get("symbol") == sym]
        s2_coin_metrics.append(compute_metrics(ct, sym) if ct else None)

    print(f"  S2 total trades: {len(s2_trades)}")

    # ── PHASE 5: WRITE OUTPUTS ────────────────────────────────────────
    print("\n[PHASE 5] Writing outputs...")

    header = [
        "SUPERTREND + MULTI-FILTER BACKTEST — Grok v8 (Fixed)",
        f"Period     : 2024-07-01 → 2026-07-01",
        f"Data source: Binance FUTURES (fapi.binance.com)",
        f"Coins      : {len(fetched)} fetched | Skipped: {skipped or 'none'}",
        f"Targets    : PF≥1.5  |  WR≥42%",
        f"Portfolio  : ${INITIAL_CAP:,.0f} global equity, max {MAX_POSITIONS} concurrent positions",
        f"Generated  : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
    ]

    s1_text = strategy_summary(
        "S1 — SuperTrend + 4H HTF Filter (Conservative)",
        s1_trades, s1_coin_metrics, dict(s1_fc)
    )
    s2_text = strategy_summary(
        "S2 — SuperTrend, NO 4H Filter (Aggressive)",
        s2_trades, s2_coin_metrics, dict(s2_fc)
    )

    s1_agg = compute_metrics(s1_trades)
    s2_agg = compute_metrics(s2_trades)

    rec = ["\n" + "="*72, "  RECOMMENDATION", "="*72]
    for name, agg in [("S1 (4H filter)", s1_agg), ("S2 (no 4H)", s2_agg)]:
        if agg:
            status = "✅ USABLE" if agg["pf"] >= 1.5 and agg["wr"] >= 0.42 else "❌ NOT READY"
            rec.append(f"  {name}: PF={agg['pf']:.4f}  WR={agg['wr']*100:.1f}%  Net=${agg['net']:,.2f}  MDD={agg['mdd']*100:.1f}%  → {status}")
        else:
            rec.append(f"  {name}: NO TRADES → ❌ NOT READY")

    full_txt = "\n".join(header) + s1_text + s2_text + "\n".join(rec)
    with open("backtest_summary.txt", "w") as f:
        f.write(full_txt)
    print("  ✓ backtest_summary.txt")

    def safe(m): return m if m else {}
    report = {
        "meta": {
            "strategy": "SuperTrend + Multi-Filter (Grok v8 Fixed)",
            "period": "2024-07-01 to 2026-07-01",
            "data_source": "Binance Futures fapi.binance.com",
            "coins_fetched": fetched,
            "coins_skipped": skipped,
            "settings": {
                "fee_rate": FEE_RATE, "slippage": SLIPPAGE,
                "initial_cap": INITIAL_CAP, "risk_per_trade": RISK_PER_TRADE,
                "st_atr_period": ST_ATR_PERIOD, "st_mult": ST_MULT,
                "adx_period": ADX_PERIOD, "adx_min": ADX_MIN,
                "di_sep_min": DI_SEP_MIN, "dist_atr_mult": DIST_ATR_MULT,
                "stop_atr_mult": STOP_ATR_MULT, "cooldown_bars": COOLDOWN_BARS,
                "max_positions": MAX_POSITIONS,
            }
        },
        "S1_with_4h_filter": {
            "aggregate": safe(s1_agg),
            "per_coin": [safe(m) for m in s1_coin_metrics],
            "filter_stats": dict(s1_fc),
            "trades": s1_trades,
        },
        "S2_no_4h_filter": {
            "aggregate": safe(s2_agg),
            "per_coin": [safe(m) for m in s2_coin_metrics],
            "filter_stats": dict(s2_fc),
            "trades": s2_trades,
        }
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("  ✓ backtest_report.json")

    print("\n" + "="*65)
    print(full_txt)

if __name__ == "__main__":
    main()

