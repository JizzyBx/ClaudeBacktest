"""
SuperTrend + Multi-Filter Backtest (Grok v8)
Two strategies:
  S1 = SuperTrend 30m flip + 4H SuperTrend filter + ADX/DI/ATR filters
  S2 = Same but NO 4H SuperTrend filter (more aggressive)
Period : 2024-07-01 → 2026-07-01
Data   : Binance public klines (data-api.binance.vision)
Output : backtest_report.json + backtest_summary.txt
"""

import json, math, time, datetime, statistics, urllib.request, urllib.error
from collections import defaultdict

# ── COIN LIST ──────────────────────────────────────────────────────────────────
ALL_COINS_RAW = [
    "ETHUSDT","DOGEUSDT","DOTUSDT","ARBUSDT",
    "1000BONKUSDT","1000PEPEUSDT","1000SHIBUSDT",
    "ADAUSDT","APTUSDT","LINKUSDT","SOLUSDT",
    "SUIUSDT","1000FLOKIUSDT","WIFUSDT",
    "BTCUSDT","BNBUSDT","NEARUSDT",
    "XRPUSDT","AVAXUSDT","LTCUSDT",
    "ATOMUSDT","OPUSDT","INJUSDT","UNIUSDT","AAVEUSDT","HBARUSDT",
    "TRUMPUSDT","BOMEUSDT","WLDUSDT","NEIROUSDT",
]
# Coins that use 1000x prefix on futures but trade as plain name on spot endpoint
# We auto-try the plain name as fallback — no skipping
PREFIX_1000_COINS = {
    "1000BONKUSDT": "BONKUSDT",
    "1000PEPEUSDT": "PEPEUSDT",
    "1000SHIBUSDT": "SHIBUSDT",
    "1000FLOKIUSDT": "FLOKIUSDT",
}
SKIP_COINS = set()  # nothing skipped by default

# ── CONFIG ─────────────────────────────────────────────────────────────────────
FEE_RATE        = 0.0005
SLIPPAGE        = 0.0002
INITIAL_CAP     = 10_000.0
RISK_PER_TRADE  = 0.0075
ATR_PERIOD      = 14
ST_ATR_PERIOD   = 10
ST_MULT         = 3.0
ADX_PERIOD      = 14
ADX_MIN         = 25
DI_SEP_MIN      = 8
DIST_ATR_MULT   = 1.5
COOLDOWN_BARS   = 3
MAX_POSITIONS   = 6
STOP_ATR_MULT   = 2.0

START_MS = int(datetime.datetime(2024, 7,  1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
END_MS   = int(datetime.datetime(2026, 7,  1, tzinfo=datetime.timezone.utc).timestamp() * 1000)

BASE_URL = "https://data-api.binance.vision/api/v3/klines"

# ── DATA FETCH ─────────────────────────────────────────────────────────────────
def fetch_klines(symbol, interval, start_ms, end_ms):
    all_rows = []
    cur = start_ms
    retries_total = 0
    while cur < end_ms:
        url = f"{BASE_URL}?symbol={symbol}&interval={interval}&startTime={cur}&endTime={end_ms}&limit=1000"
        for attempt in range(5):
            try:
                with urllib.request.urlopen(url, timeout=20) as r:
                    data = json.loads(r.read())
                break
            except Exception as e:
                if attempt == 4:
                    raise
                wait = 0.5 * (2 ** attempt)
                time.sleep(wait)
                retries_total += 1
        if not data:
            break
        all_rows.extend(data)
        last_open = data[-1][0]
        if last_open >= end_ms or len(data) < 1000:
            break
        cur = last_open + 1
        time.sleep(0.13)
    return all_rows

def parse_klines(raw):
    opens, highs, lows, closes, volumes, times = [], [], [], [], [], []
    for row in raw:
        times.append(int(row[0]))
        opens.append(float(row[1]))
        highs.append(float(row[2]))
        lows.append(float(row[3]))
        closes.append(float(row[4]))
        volumes.append(float(row[5]))
    return opens, highs, lows, closes, volumes, times

# ── INDICATORS ─────────────────────────────────────────────────────────────────
def ema(values, period):
    result = [None] * len(values)
    k = 2.0 / (period + 1)
    seed = None
    for i, v in enumerate(values):
        if v is None:
            continue
        if seed is None:
            seed = v
            result[i] = v
        else:
            seed = v * k + seed * (1 - k)
            result[i] = seed
    return result

def atr(highs, lows, closes, period=14):
    n = len(closes)
    tr = [None] * n
    for i in range(1, n):
        if closes[i-1] is None:
            continue
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i-1]),
                    abs(lows[i]  - closes[i-1]))
    result = [None] * n
    # Wilder smoothing
    # find first full window
    start = None
    for i in range(1, n):
        if tr[i] is not None:
            if start is None:
                start = i
            if i - start + 1 == period:
                result[i] = sum(tr[start:i+1]) / period
                prev_idx = i
                break
    if start is None or result[prev_idx] is None:
        return result
    for i in range(prev_idx + 1, n):
        if tr[i] is None:
            continue
        result[i] = (result[prev_idx] * (period - 1) + tr[i]) / period
        prev_idx = i
    return result

def adx_full(highs, lows, closes, period=14):
    """
    Clean Wilder ADX implementation.
    Uses straight arrays (no None mid-series gaps) — all raw values are numeric from bar 1.
    Returns (adx, pdi, mdi) as lists, None before warmup period.
    """
    n = len(closes)

    # Raw DM / TR — all numeric from index 1
    tr_raw  = [0.0] * n
    pdm_raw = [0.0] * n
    mdm_raw = [0.0] * n

    for i in range(1, n):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_raw[i] = up   if (up > down and up > 0)   else 0.0
        mdm_raw[i] = down if (down > up and down > 0) else 0.0
        tr_raw[i]  = max(highs[i] - lows[i],
                         abs(highs[i]  - closes[i-1]),
                         abs(lows[i]   - closes[i-1]))

    # Wilder smooth — first value is SMA of first `period` bars, then rolling
    def wilder(raw, p):
        res = [None] * n
        if n < p + 1:
            return res
        # seed at index p (covers bars 1..p)
        res[p] = sum(raw[1:p+1])          # Wilder uses sum for seed, not average
        for i in range(p + 1, n):
            res[i] = res[i-1] - (res[i-1] / p) + raw[i]
        return res

    s_tr  = wilder(tr_raw,  period)
    s_pdm = wilder(pdm_raw, period)
    s_mdm = wilder(mdm_raw, period)

    pdi = [None] * n
    mdi = [None] * n
    dx  = [None] * n

    for i in range(period, n):
        if s_tr[i] is None or s_tr[i] == 0:
            continue
        pdi[i] = 100.0 * s_pdm[i] / s_tr[i]
        mdi[i] = 100.0 * s_mdm[i] / s_tr[i]
        dsum = pdi[i] + mdi[i]
        if dsum != 0:
            dx[i] = 100.0 * abs(pdi[i] - mdi[i]) / dsum

    # Second Wilder pass on DX to get ADX
    adx = [None] * n
    # seed at index period*2
    seed_start = period
    seed_end   = period * 2
    if seed_end >= n:
        return adx, pdi, mdi
    valid_dx = [dx[i] for i in range(seed_start, seed_end + 1) if dx[i] is not None]
    if len(valid_dx) < period:
        return adx, pdi, mdi
    adx[seed_end] = sum(valid_dx[-period:]) / period
    for i in range(seed_end + 1, n):
        if dx[i] is None or adx[i-1] is None:
            continue
        adx[i] = (adx[i-1] * (period - 1) + dx[i]) / period

    return adx, pdi, mdi

# ── SUPERTREND ─────────────────────────────────────────────────────────────────
def supertrend(highs, lows, closes, atr_period=10, mult=3.0):
    """Returns (st_values, direction) where direction: +1=bull, -1=bear"""
    n = len(closes)
    atr_vals = atr(highs, lows, closes, atr_period)
    st   = [None]*n
    dire = [None]*n  # +1 bull, -1 bear
    ub   = [None]*n  # upper band
    lb   = [None]*n  # lower band

    for i in range(n):
        if atr_vals[i] is None:
            continue
        mid = (highs[i] + lows[i]) / 2
        raw_ub = mid + mult * atr_vals[i]
        raw_lb = mid - mult * atr_vals[i]

        # Band persistence
        if i == 0 or ub[i-1] is None:
            ub[i] = raw_ub
            lb[i] = raw_lb
        else:
            ub[i] = raw_ub if raw_ub < ub[i-1] or closes[i-1] > ub[i-1] else ub[i-1]
            lb[i] = raw_lb if raw_lb > lb[i-1] or closes[i-1] < lb[i-1] else lb[i-1]

        # Direction
        if dire[i-1] is None:
            dire[i] = 1 if closes[i] > lb[i] else -1
        else:
            if dire[i-1] == 1:
                dire[i] = -1 if closes[i] < lb[i] else 1
            else:
                dire[i] =  1 if closes[i] > ub[i] else -1

        st[i] = lb[i] if dire[i] == 1 else ub[i]

    return st, dire

# ── 4H TREND MAP ──────────────────────────────────────────────────────────────
def build_4h_supertrend(h4_opens, h4_highs, h4_lows, h4_closes, h4_times):
    st_vals, st_dir = supertrend(h4_highs, h4_lows, h4_closes, ST_ATR_PERIOD, ST_MULT)
    trend_map = {}
    for i, t in enumerate(h4_times):
        if st_dir[i] is not None:
            trend_map[t] = st_dir[i]
    sorted_ts = sorted(trend_map.keys())
    return trend_map, sorted_ts

def get_4h_trend(ts_ms, sorted_ts, trend_map):
    # binary search for latest 4h candle open <= ts_ms
    lo, hi = 0, len(sorted_ts) - 1
    idx = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if sorted_ts[mid] <= ts_ms:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1
    if idx == -1:
        return None
    return trend_map[sorted_ts[idx]]

# ── METRICS ───────────────────────────────────────────────────────────────────
def compute_metrics(trades, symbol="ALL"):
    if not trades:
        return None
    pnls   = [t["pnl"] for t in trades]
    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    n      = len(trades)
    wr     = len(wins) / n if n else 0
    gp     = sum(p["pnl"] for p in wins)
    gl     = abs(sum(p["pnl"] for p in losses)) if losses else 0
    pf     = gp / gl if gl > 0 else (float("inf") if gp > 0 else 0)
    net    = sum(pnls)
    aw     = gp / len(wins)   if wins   else 0
    al     = gl / len(losses) if losses else 0
    exp    = wr * aw - (1 - wr) * al

    # Max drawdown on equity curve
    equity = INITIAL_CAP
    peak   = INITIAL_CAP
    mdd    = 0.0
    eq_curve = []
    for t in sorted(trades, key=lambda x: x["exit_t"]):
        equity += t["pnl"]
        eq_curve.append(equity)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > mdd:
            mdd = dd

    # Sharpe / Sortino (monthly returns)
    monthly = defaultdict(float)
    for t in trades:
        mo = datetime.datetime.utcfromtimestamp(t["exit_t"]/1000).strftime("%Y-%m")
        monthly[mo] += t["pnl"]
    mo_rets = list(monthly.values())
    sharpe  = 0.0
    sortino = 0.0
    if len(mo_rets) > 1:
        avg_r = statistics.mean(mo_rets)
        std_r = statistics.stdev(mo_rets)
        if std_r > 0:
            sharpe = avg_r / std_r * math.sqrt(12)
        neg = [r for r in mo_rets if r < 0]
        if neg:
            dstd = statistics.stdev(neg) if len(neg) > 1 else abs(neg[0])
            if dstd > 0:
                sortino = avg_r / dstd * math.sqrt(12)

    nlongs  = sum(1 for t in trades if t["dir"] == "LONG")
    nshorts = sum(1 for t in trades if t["dir"] == "SHORT")
    lwr     = sum(1 for t in trades if t["dir"]=="LONG"  and t["win"]) / nlongs  if nlongs  else 0
    swr     = sum(1 for t in trades if t["dir"]=="SHORT" and t["win"]) / nshorts if nshorts else 0

    # streak
    maxcw = maxcl = cw = cl = 0
    for t in trades:
        if t["win"]:
            cw += 1; cl = 0
            if cw > maxcw: maxcw = cw
        else:
            cl += 1; cw = 0
            if cl > maxcl: maxcl = cl

    avg_dur = statistics.mean([t["dur"] for t in trades]) if trades else 0

    return dict(
        symbol=symbol, n=n, wr=round(wr,4), pf=round(pf,4),
        net=round(net,2), mdd=round(mdd,4), sharpe=round(sharpe,3),
        sortino=round(sortino,3), aw=round(aw,2), al=round(al,2),
        exp=round(exp,2), dur=round(avg_dur,1),
        nlongs=nlongs, nshorts=nshorts, lwr=round(lwr,4), swr=round(swr,4),
        monthly=dict(sorted(monthly.items())),
        maxcw=maxcw, maxcl=maxcl, gp=round(gp,2), gl=round(gl,2),
    )

# ── STRATEGY ENGINE ───────────────────────────────────────────────────────────
def run_strategy(symbol, h30_raw, h4_raw, use_htf_filter=True):
    """
    Returns (trades, filter_counts)
    """
    if not h30_raw or len(h30_raw) < 100:
        return [], {}

    o30, h30, l30, c30, v30, t30 = parse_klines(h30_raw)
    n = len(c30)

    # Indicators on 30m
    atr14   = atr(h30, l30, c30, ATR_PERIOD)
    adx_arr, pdi_arr, mdi_arr = adx_full(h30, l30, c30, ADX_PERIOD)
    st30_v, st30_d = supertrend(h30, l30, c30, ST_ATR_PERIOD, ST_MULT)

    # 4H supertrend
    trend_map = {}
    sorted_ts = []
    if use_htf_filter and h4_raw and len(h4_raw) >= 50:
        o4, h4, l4, c4, v4, t4 = parse_klines(h4_raw)
        trend_map, sorted_ts = build_4h_supertrend(o4, h4, l4, c4, t4)

    trades = []
    cooldown = 0
    in_trade = None

    fc = defaultdict(int)
    fc["total_candles"] = 0
    fc["no_flip"] = 0
    fc["htf_filter"] = 0
    fc["adx_min"] = 0
    fc["adx_rising"] = 0
    fc["dist_filter"] = 0
    fc["di_sep"] = 0
    fc["signals_generated"] = 0

    equity = INITIAL_CAP  # simplified per-symbol equity tracking

    for i in range(2, n):
        fc["total_candles"] += 1

        # ── EXIT LOGIC ──
        if in_trade is not None:
            cur_dir  = in_trade["dir"]
            entry_p  = in_trade["entry"]
            sl_price = in_trade["sl"]
            entry_atr = in_trade["entry_atr"]

            exit_p   = None
            exit_reason = None

            if cur_dir == "LONG":
                # Check SL hit (low)
                if l30[i] <= sl_price:
                    exit_p = sl_price
                    exit_reason = "SL"
                # Check ST flip
                elif st30_d[i] == -1 and st30_d[i-1] == 1:
                    exit_p = c30[i]
                    exit_reason = "ST_FLIP"
            else:  # SHORT
                if h30[i] >= sl_price:
                    exit_p = sl_price
                    exit_reason = "SL"
                elif st30_d[i] == 1 and st30_d[i-1] == -1:
                    exit_p = c30[i]
                    exit_reason = "ST_FLIP"

            if exit_p is not None:
                slip_mult = (1 - SLIPPAGE) if cur_dir == "LONG" else (1 + SLIPPAGE)
                fee_mult  = (1 - FEE_RATE)
                if cur_dir == "LONG":
                    exit_adj = exit_p * (1 - SLIPPAGE) * (1 - FEE_RATE)
                    pnl = (exit_adj - in_trade["entry_adj"]) * in_trade["size"]
                else:
                    exit_adj = exit_p * (1 + SLIPPAGE) * (1 + FEE_RATE)
                    pnl = (in_trade["entry_adj"] - exit_adj) * in_trade["size"]

                win = pnl > 0
                dur = (t30[i] - in_trade["entry_t"]) / (1000 * 60 * 30)  # bars

                trades.append(dict(
                    dir=cur_dir, entry=in_trade["entry"], exit=exit_p,
                    entry_t=in_trade["entry_t"], exit_t=t30[i],
                    pnl=round(pnl, 4), win=win,
                    hit_tp=(exit_reason == "ST_FLIP"),
                    dur=dur, risk=in_trade["risk"],
                    exit_reason=exit_reason,
                ))
                cooldown = COOLDOWN_BARS
                in_trade = None
                continue

        if cooldown > 0:
            cooldown -= 1
            continue

        if in_trade is not None:
            continue

        # ── ENTRY LOGIC ──
        # Need valid indicators
        if atr14[i] is None or adx_arr[i] is None or pdi_arr[i] is None or mdi_arr[i] is None:
            continue
        if st30_d[i] is None or st30_d[i-1] is None or st30_d[i-2] is None:
            continue

        # Detect flip on close of candle i
        flipped_long  = (st30_d[i] == 1  and st30_d[i-1] == -1)
        flipped_short = (st30_d[i] == -1 and st30_d[i-1] == 1)

        if not flipped_long and not flipped_short:
            fc["no_flip"] += 1
            continue

        direction = "LONG" if flipped_long else "SHORT"

        # Filter 1: 4H SuperTrend alignment
        if use_htf_filter and sorted_ts:
            h4_trend = get_4h_trend(t30[i], sorted_ts, trend_map)
            if h4_trend is None:
                fc["htf_filter"] += 1
                continue
            if direction == "LONG"  and h4_trend != 1:
                fc["htf_filter"] += 1
                continue
            if direction == "SHORT" and h4_trend != -1:
                fc["htf_filter"] += 1
                continue

        # Filter 2: ADX >= 25
        if adx_arr[i] < ADX_MIN:
            fc["adx_min"] += 1
            continue

        # Filter 3: ADX rising (ADX > ADX[i-2])
        if adx_arr[i-2] is None or adx_arr[i] <= adx_arr[i-2]:
            fc["adx_rising"] += 1
            continue

        # Filter 4: Distance from SuperTrend <= 1.5 * ATR14
        if st30_v[i] is None:
            continue
        dist = abs(c30[i] - st30_v[i])
        if dist > DIST_ATR_MULT * atr14[i]:
            fc["dist_filter"] += 1
            continue

        # Filter 5: DI separation >= 8 and correct DI dominance
        if pdi_arr[i] is None or mdi_arr[i] is None:
            fc["di_sep"] += 1
            continue
        if direction == "LONG":
            if pdi_arr[i] <= mdi_arr[i]:
                fc["di_sep"] += 1
                continue
            if (pdi_arr[i] - mdi_arr[i]) < DI_SEP_MIN:
                fc["di_sep"] += 1
                continue
        else:
            if mdi_arr[i] <= pdi_arr[i]:
                fc["di_sep"] += 1
                continue
            if (mdi_arr[i] - pdi_arr[i]) < DI_SEP_MIN:
                fc["di_sep"] += 1
                continue

        fc["signals_generated"] += 1

        # ── POSITION SIZING ──
        stop_dist = STOP_ATR_MULT * atr14[i]
        if stop_dist <= 0:
            continue
        risk_amt  = equity * RISK_PER_TRADE
        size      = risk_amt / stop_dist

        entry_p = c30[i]
        if direction == "LONG":
            entry_adj = entry_p * (1 + SLIPPAGE) * (1 + FEE_RATE)
            sl_price  = entry_p - stop_dist
        else:
            entry_adj = entry_p * (1 - SLIPPAGE) * (1 - FEE_RATE)
            sl_price  = entry_p + stop_dist

        in_trade = dict(
            dir=direction, entry=entry_p, entry_adj=entry_adj,
            sl=sl_price, size=size, entry_t=t30[i],
            entry_atr=atr14[i], risk=risk_amt,
        )

    return trades, dict(fc)


# ── SUMMARY WRITER ────────────────────────────────────────────────────────────
def strategy_summary(strat_name, all_trades, per_coin_metrics, filter_agg):
    lines = []
    lines.append(f"\n{'='*72}")
    lines.append(f"  {strat_name}")
    lines.append(f"{'='*72}")

    if not all_trades:
        lines.append("  NO TRADES GENERATED")
        return "\n".join(lines)

    agg = compute_metrics(all_trades, "AGGREGATE")
    if agg:
        lines.append(f"\n  AGGREGATE RESULTS")
        lines.append(f"  {'─'*50}")
        lines.append(f"  Total Trades : {agg['n']}")
        lines.append(f"  Win Rate     : {agg['wr']*100:.1f}%")
        lines.append(f"  Profit Factor: {agg['pf']:.4f}  {'✅ PASS' if agg['pf'] >= 1.5 else '❌ FAIL'}")
        lines.append(f"  Net PnL      : ${agg['net']:,.2f}")
        lines.append(f"  Max Drawdown : {agg['mdd']*100:.1f}%")
        lines.append(f"  Sharpe       : {agg['sharpe']:.3f}")
        lines.append(f"  Sortino      : {agg['sortino']:.3f}")
        lines.append(f"  Avg Win      : ${agg['aw']:.2f}")
        lines.append(f"  Avg Loss     : ${agg['al']:.2f}")
        lines.append(f"  Expectancy   : ${agg['exp']:.2f}")
        lines.append(f"  Avg Duration : {agg['dur']:.1f} bars")
        lines.append(f"  Longs/Shorts : {agg['nlongs']}/{agg['nshorts']}")
        lines.append(f"  Long WR      : {agg['lwr']*100:.1f}%")
        lines.append(f"  Short WR     : {agg['swr']*100:.1f}%")
        lines.append(f"  Max Streak W : {agg['maxcw']}  Max Streak L: {agg['maxcl']}")
        lines.append(f"\n  VALIDATION   : PF≥1.5 {'✅' if agg['pf']>=1.5 else '❌'}  WR≥42% {'✅' if agg['wr']>=0.42 else '❌'}")

    # Validation per coin
    pf_pass  = sum(1 for m in per_coin_metrics if m and m["pf"] >= 1.5)
    wr_pass  = sum(1 for m in per_coin_metrics if m and m["wr"] >= 0.42)
    total_c  = len([m for m in per_coin_metrics if m])
    lines.append(f"  PF≥1.5 on {pf_pass}/{total_c} coins  |  WR≥42% on {wr_pass}/{total_c} coins")

    # Per-coin table
    valid = [m for m in per_coin_metrics if m]
    valid.sort(key=lambda x: x["pf"], reverse=True)
    lines.append(f"\n  PER-COIN BREAKDOWN (sorted by PF desc)")
    lines.append(f"  {'Symbol':<16} {'Trades':>6} {'WR':>7} {'PF':>7} {'Net PnL':>10} {'MDD':>6} {'L/S':>5}")
    lines.append(f"  {'─'*62}")
    for m in valid:
        pf_mark = "✅" if m["pf"] >= 1.5 else ("⚠️ " if m["pf"] >= 1.0 else "❌")
        lines.append(
            f"  {m['symbol']:<16} {m['n']:>6} {m['wr']*100:>6.1f}%"
            f" {m['pf']:>7.4f}{pf_mark} ${m['net']:>9,.2f} {m['mdd']*100:>5.1f}%"
            f" {m['nlongs']}/{m['nshorts']}"
        )

    # Monthly PnL
    monthly_all = defaultdict(float)
    for t in all_trades:
        mo = datetime.datetime.utcfromtimestamp(t["exit_t"]/1000).strftime("%Y-%m")
        monthly_all[mo] += t["pnl"]
    lines.append(f"\n  MONTHLY PnL")
    lines.append(f"  {'─'*30}")
    for mo in sorted(monthly_all):
        bar = "▲" if monthly_all[mo] >= 0 else "▼"
        lines.append(f"  {mo}  {bar}  ${monthly_all[mo]:>9,.2f}")

    # Filter stats
    lines.append(f"\n  FILTER REJECTION STATS (aggregated across all coins)")
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


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    coins = list(ALL_COINS_RAW)
    print(f"Coins to test: {len(coins)}")
    print(f"Period: 2024-07-01 → 2026-07-01")
    print(f"Strategies: S1 (with 4H filter) + S2 (without 4H filter)")
    print("="*60)

    all_data_30 = {}   # keyed by original symbol name (e.g. 1000BONKUSDT)
    all_data_4h = {}
    symbol_map  = {}   # original → actual symbol used for fetch

    # ── FETCH ALL DATA FIRST ──
    print("\n[PHASE 1] Fetching market data...")
    for symbol in coins:
        # Determine fetch symbol — try plain fallback for 1000x coins
        fetch_sym = symbol
        print(f"  Fetching {symbol} 30m...", end=" ", flush=True)
        raw = None
        try:
            raw = fetch_klines(fetch_sym, "30m", START_MS, END_MS)
        except Exception as e:
            print(f"FAIL ({e})", end=" ", flush=True)
            raw = None

        # If failed and this is a 1000x coin, try plain name
        if (not raw) and symbol in PREFIX_1000_COINS:
            plain = PREFIX_1000_COINS[symbol]
            print(f"→ trying {plain}...", end=" ", flush=True)
            try:
                raw = fetch_klines(plain, "30m", START_MS, END_MS)
                if raw:
                    fetch_sym = plain
            except Exception as e2:
                print(f"FAIL ({e2})", end=" ", flush=True)
                raw = None

        if raw:
            all_data_30[symbol] = raw
            symbol_map[symbol] = fetch_sym
            print(f"OK ({len(raw)} candles) [as {fetch_sym}]")
        else:
            print("SKIP — no data on spot endpoint")

    print()
    for symbol in coins:
        if symbol not in all_data_30:
            continue
        fetch_sym = symbol_map.get(symbol, symbol)
        print(f"  Fetching {symbol} 4h...", end=" ", flush=True)
        try:
            raw = fetch_klines(fetch_sym, "4h", START_MS, END_MS)
            if raw:
                all_data_4h[symbol] = raw
                print(f"OK ({len(raw)} candles)")
            else:
                print("EMPTY")
        except Exception as e:
            print(f"ERROR: {e}")

    # ── STRATEGY 1: WITH 4H FILTER ──
    print("\n[PHASE 2] Running S1 (SuperTrend + 4H filter)...")
    s1_all_trades = []
    s1_coin_metrics = []
    s1_filter_agg = defaultdict(int)

    for symbol in coins:
        if symbol not in all_data_30:
            continue
        h30_raw = all_data_30[symbol]
        h4_raw  = all_data_4h.get(symbol, [])
        trades, fc = run_strategy(symbol, h30_raw, h4_raw, use_htf_filter=True)
        s1_all_trades.extend(trades)
        m = compute_metrics(trades, symbol) if trades else None
        s1_coin_metrics.append(m)
        for k, v in fc.items():
            s1_filter_agg[k] += v
        if m:
            print(f"  {symbol:<18} trades={m['n']:>4}  PF={m['pf']:.3f}  WR={m['wr']*100:.1f}%")
        else:
            print(f"  {symbol:<18} NO TRADES")

    # ── STRATEGY 2: NO 4H FILTER ──
    print("\n[PHASE 3] Running S2 (SuperTrend, no 4H filter)...")
    s2_all_trades = []
    s2_coin_metrics = []
    s2_filter_agg = defaultdict(int)

    for symbol in coins:
        if symbol not in all_data_30:
            continue
        h30_raw = all_data_30[symbol]
        trades, fc = run_strategy(symbol, h30_raw, [], use_htf_filter=False)
        s2_all_trades.extend(trades)
        m = compute_metrics(trades, symbol) if trades else None
        s2_coin_metrics.append(m)
        for k, v in fc.items():
            s2_filter_agg[k] += v
        if m:
            print(f"  {symbol:<18} trades={m['n']:>4}  PF={m['pf']:.3f}  WR={m['wr']*100:.1f}%")
        else:
            print(f"  {symbol:<18} NO TRADES")

    # ── WRITE OUTPUTS ──
    print("\n[PHASE 4] Writing outputs...")

    # Summary TXT
    summary_lines = []
    summary_lines.append("SUPERTREND + MULTI-FILTER BACKTEST — Grok v8")
    summary_lines.append(f"Period  : 2024-07-01 → 2026-07-01")
    summary_lines.append(f"Coins   : {len(coins)} (after skip)")
    actually_skipped = [c for c in coins if c not in all_data_30]
    summary_lines.append(f"Skipped : {', '.join(actually_skipped) if actually_skipped else 'None'}")
    summary_lines.append(f"Targets : PF≥1.5  |  WR≥42%")
    summary_lines.append(f"Generated: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    s1_text = strategy_summary(
        "S1 — SuperTrend + 4H HTF Filter (Conservative)",
        s1_all_trades, s1_coin_metrics, dict(s1_filter_agg)
    )
    s2_text = strategy_summary(
        "S2 — SuperTrend, NO 4H Filter (Aggressive)",
        s2_all_trades, s2_coin_metrics, dict(s2_filter_agg)
    )

    summary_lines.append(s1_text)
    summary_lines.append(s2_text)

    # Recommendation
    s1_agg = compute_metrics(s1_all_trades)
    s2_agg = compute_metrics(s2_all_trades)
    summary_lines.append("\n" + "="*72)
    summary_lines.append("  RECOMMENDATION")
    summary_lines.append("="*72)
    for name, agg in [("S1 (4H filter)", s1_agg), ("S2 (no 4H)", s2_agg)]:
        if agg:
            status = "✅ USABLE" if agg["pf"] >= 1.5 and agg["wr"] >= 0.42 else "❌ NOT READY"
            summary_lines.append(f"  {name}: PF={agg['pf']:.4f}  WR={agg['wr']*100:.1f}%  → {status}")
        else:
            summary_lines.append(f"  {name}: NO TRADES → ❌ NOT READY")

    txt_out = "\n".join(summary_lines)
    with open("backtest_summary.txt", "w") as f:
        f.write(txt_out)
    print("  ✓ backtest_summary.txt written")

    # JSON report
    def safe_metrics(m):
        return m if m else {}

    report = {
        "meta": {
            "strategy": "SuperTrend + Multi-Filter (Grok v8)",
            "period": "2024-07-01 to 2026-07-01",
            "coins_tested": coins,
            "coins_skipped": [c for c in coins if c not in all_data_30],
            "symbol_map": symbol_map,
            "settings": {
                "fee_rate": FEE_RATE, "slippage": SLIPPAGE,
                "initial_cap": INITIAL_CAP, "risk_per_trade": RISK_PER_TRADE,
                "st_atr_period": ST_ATR_PERIOD, "st_mult": ST_MULT,
                "adx_period": ADX_PERIOD, "adx_min": ADX_MIN,
                "di_sep_min": DI_SEP_MIN, "dist_atr_mult": DIST_ATR_MULT,
                "stop_atr_mult": STOP_ATR_MULT, "cooldown_bars": COOLDOWN_BARS,
            }
        },
        "S1_with_4h_filter": {
            "aggregate": safe_metrics(s1_agg),
            "per_coin": [safe_metrics(m) for m in s1_coin_metrics],
            "filter_stats": dict(s1_filter_agg),
            "trades": s1_all_trades,
        },
        "S2_no_4h_filter": {
            "aggregate": safe_metrics(s2_agg),
            "per_coin": [safe_metrics(m) for m in s2_coin_metrics],
            "filter_stats": dict(s2_filter_agg),
            "trades": s2_all_trades,
        }
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("  ✓ backtest_report.json written")

    print("\n" + "="*60)
    print("BACKTEST COMPLETE")
    print(txt_out)

if __name__ == "__main__":
    main()
