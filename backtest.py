"""
SuperTrend + Multi-Filter Backtest (Grok v8 spec)
  S1 = SuperTrend 30m flip + 4H SuperTrend filter + ADX/DI/ATR filters
  S2 = Same but NO 4H SuperTrend filter (more aggressive)

Data source: data.binance.vision monthly USDS-M futures kline archives.
This is Binance's static historical-data bucket, NOT the fapi.binance.com
live REST API — GitHub Actions runner IPs are blocked from the live futures
API (451), but this bucket is a plain file host and has worked from Actions
in this pipeline before. If it also gets blocked, that will show up
immediately as 403/404s on every symbol in Phase 1 and the run will report
zero fetched coins.

Fix vs the version Grok/Claude reviewed before running:
  - 4H HTF filter previously could read the *current, still-forming* 4H
    candle's SuperTrend direction (lookahead bias). Fixed by offsetting the
    lookup by one 4H period so only a fully CLOSED 4H candle is used.
  - Indicators are computed once per coin and reused for both S1 and S2
    (previously recomputed from scratch for each), just to save time.

Period : 2024-07-01 -> 2026-07-01
Output : backtest_report.json + backtest_summary.txt
"""

import json, math, io, csv, zipfile, datetime, statistics, urllib.request, urllib.error
from collections import defaultdict

# ── COIN LIST (exactly as specified) ────────────────────────────────────────
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

# ── CONFIG (matches Grok v8 spec exactly) ───────────────────────────────────
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

FOUR_HOURS_MS = 4 * 60 * 60 * 1000

START_YM = (2024, 7)
END_YM   = (2026, 6)     # last full month before the 2026-07-01 cutoff

# ── DATA SOURCE: static monthly USDS-M futures kline archives ──────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def month_range(start_ym, end_ym):
    y, m = start_ym
    ey, em = end_ym
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out

MONTHS = month_range(START_YM, END_YM)

def fetch_month_csv(symbol, interval, year, month):
    """Download+unzip one monthly kline archive. Returns list of raw rows or None if missing."""
    fname = f"{symbol}-{interval}-{year:04d}-{month:02d}"
    url = f"{BASE_URL}/{symbol}/{interval}/{fname}.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            blob = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except urllib.error.URLError:
        raise

    rows = []
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        inner = zf.namelist()[0]
        with zf.open(inner) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.reader(text)
            for row in reader:
                if not row:
                    continue
                # header row (if present) starts with a non-numeric field
                try:
                    open_time = int(row[0])
                except ValueError:
                    continue
                rows.append(row)
    return rows

def fetch_symbol_klines(symbol, interval, months):
    """Fetch+merge all monthly archives for a symbol. Returns raw row list sorted by open_time."""
    all_rows = []
    got_any = False
    for (y, m) in months:
        try:
            rows = fetch_month_csv(symbol, interval, y, m)
        except Exception:
            rows = None
        if rows:
            got_any = True
            all_rows.extend(rows)
    if not got_any:
        return None
    all_rows.sort(key=lambda r: int(r[0]))
    # de-dupe on open_time in case of overlap
    seen = set()
    dedup = []
    for r in all_rows:
        ot = int(r[0])
        if ot in seen:
            continue
        seen.add(ot)
        dedup.append(r)
    return dedup

def parse_klines(raw):
    """raw: list of csv rows [open_time, open, high, low, close, volume, ...] -> arrays."""
    # Binance futures kline timestamps: pre-2025 archives are ms; some 2025+
    # archives switched to microseconds for SPOT (not confirmed for futures,
    # but guard anyway by detecting absurdly large values).
    times, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for row in raw:
        ot = int(row[0])
        if ot > 10**14:   # looks like microseconds, not ms
            ot //= 1000
        times.append(ot)
        opens.append(float(row[1]))
        highs.append(float(row[2]))
        lows.append(float(row[3]))
        closes.append(float(row[4]))
        volumes.append(float(row[5]))
    return times, opens, highs, lows, closes, volumes

# ── INDICATORS ─────────────────────────────────────────────────────────────
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
    result[period] = sum(tr[1:period+1]) / period
    for i in range(period + 1, n):
        result[i] = (result[i-1] * (period - 1) + tr[i]) / period
    return result

def calc_adx(highs, lows, closes, period=14):
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
    n = len(closes)
    atr_vals = calc_atr(highs, lows, closes, atr_period)
    st   = [None] * n
    dire = [None] * n
    ub   = [None] * n
    lb   = [None] * n
    for i in range(n):
        if atr_vals[i] is None:
            continue
        mid    = (highs[i] + lows[i]) / 2.0
        raw_ub = mid + mult * atr_vals[i]
        raw_lb = mid - mult * atr_vals[i]
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
            dire[i] = 1 if closes[i] > ub[i] else -1
        st[i] = lb[i] if dire[i] == 1 else ub[i]
    return st, dire

# ── 4H TREND LOOKUP (lookahead-safe) ────────────────────────────────────────
def build_htf_map(h4_times, h4_highs, h4_lows, h4_closes):
    _, dire = calc_supertrend(h4_highs, h4_lows, h4_closes, ST_ATR_PERIOD, ST_MULT)
    trend_map = {}
    for i, t in enumerate(h4_times):
        if dire[i] is not None:
            trend_map[t] = dire[i]
    sorted_ts = sorted(trend_map.keys())
    return trend_map, sorted_ts

def get_htf_trend(bar_ts, sorted_ts, trend_map):
    """
    Returns the direction of the most recent FULLY CLOSED 4H candle as of bar_ts.
    A 4H candle with open time T covers [T, T+4H) and is not closed until T+4H.
    We offset the query by one 4H period so a candle whose open time equals
    (or is later than) bar_ts - 4H is never treated as closed prematurely.
    """
    query_ts = bar_ts - FOUR_HOURS_MS
    lo, hi, idx = 0, len(sorted_ts) - 1, -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if sorted_ts[mid] <= query_ts:
            idx = mid; lo = mid + 1
        else:
            hi = mid - 1
    return trend_map[sorted_ts[idx]] if idx >= 0 else None

# ── PRE-COMPUTE INDICATORS PER COIN (once, reused for S1 and S2) ───────────
def precompute(symbol, raw_30, raw_4h):
    if not raw_30 or len(raw_30) < 100:
        return None
    times, _, highs, lows, closes, _ = parse_klines(raw_30)
    atr14         = calc_atr(highs, lows, closes, ATR_PERIOD)
    adx, pdi, mdi = calc_adx(highs, lows, closes, ADX_PERIOD)
    st_line, st_dir = calc_supertrend(highs, lows, closes, ST_ATR_PERIOD, ST_MULT)

    htf_map, htf_ts = {}, []
    if raw_4h and len(raw_4h) >= 50:
        t4, _, h4h, h4l, h4c, _ = parse_klines(raw_4h)
        htf_map, htf_ts = build_htf_map(t4, h4h, h4l, h4c)

    return dict(
        symbol=symbol, times=times, highs=highs, lows=lows, closes=closes,
        atr14=atr14, adx=adx, pdi=pdi, mdi=mdi,
        st_line=st_line, st_dir=st_dir,
        htf_map=htf_map, htf_ts=htf_ts,
        n=len(closes),
    )

DEBUG_SAMPLES = []   # module-level, filled during the first debug-enabled run

# ── PORTFOLIO SIMULATION ─────────────────────────────────────────────────────
def simulate_portfolio(all_coin_data, use_htf, enable_dist=True, enable_di=True, debug=False):
    all_times = set()
    for cd in all_coin_data.values():
        all_times.update(cd["times"])
    timeline = sorted(all_times)

    time_index = {sym: {t: i for i, t in enumerate(cd["times"])}
                  for sym, cd in all_coin_data.items()}

    coin_state = {sym: dict(cooldown=0, fc=defaultdict(int)) for sym in all_coin_data}

    equity     = INITIAL_CAP
    open_pos   = {}
    all_trades = []
    max_pos_blocked = 0

    for ts in timeline:
        # ── EXITS ──
        to_close = []
        for sym, pos in open_pos.items():
            cd  = all_coin_data[sym]
            idx = time_index[sym].get(ts)
            if idx is None:
                continue
            lows_, highs_, closes_, st_dir = cd["lows"], cd["highs"], cd["closes"], cd["st_dir"]

            exit_p = None
            exit_reason = None
            flip_against = idx > 0 and st_dir[idx] is not None and st_dir[idx-1] is not None

            if pos["dir"] == "LONG":
                if lows_[idx] <= pos["sl"]:
                    exit_p, exit_reason = pos["sl"], "SL"
                elif flip_against and st_dir[idx] == -1 and st_dir[idx-1] == 1:
                    exit_p, exit_reason = closes_[idx], "ST_FLIP"
            else:
                if highs_[idx] >= pos["sl"]:
                    exit_p, exit_reason = pos["sl"], "SL"
                elif flip_against and st_dir[idx] == 1 and st_dir[idx-1] == -1:
                    exit_p, exit_reason = closes_[idx], "ST_FLIP"

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
                    symbol=sym, dir=pos["dir"], entry=pos["entry"], exit=exit_p,
                    entry_t=pos["entry_t"], exit_t=ts, pnl=round(pnl, 4),
                    win=(pnl > 0), dur=dur, exit_reason=exit_reason,
                ))
                coin_state[sym]["cooldown"] = COOLDOWN_BARS
                to_close.append(sym)

        for sym in to_close:
            del open_pos[sym]

        # ── ENTRIES ──
        if len(open_pos) >= MAX_POSITIONS:
            max_pos_blocked += 1
            continue

        for sym, cd in all_coin_data.items():
            if sym in open_pos:
                continue
            if coin_state[sym]["cooldown"] > 0:
                coin_state[sym]["cooldown"] -= 1
                continue
            if len(open_pos) >= MAX_POSITIONS:
                max_pos_blocked += 1
                break

            idx = time_index[sym].get(ts)
            if idx is None or idx < 2:
                continue

            fc = coin_state[sym]["fc"]
            fc["total_candles"] += 1

            atr14, adx, pdi, mdi = cd["atr14"], cd["adx"], cd["pdi"], cd["mdi"]
            st_dir, st_line, closes_ = cd["st_dir"], cd["st_line"], cd["closes"]

            if (atr14[idx] is None or adx[idx] is None or pdi[idx] is None or
                mdi[idx] is None or st_dir[idx] is None or st_dir[idx-1] is None or
                st_dir[idx-2] is None):
                fc["warmup_none"] += 1
                continue

            flipped_long  = (st_dir[idx] == 1  and st_dir[idx-1] == -1)
            flipped_short = (st_dir[idx] == -1 and st_dir[idx-1] == 1)
            if not flipped_long and not flipped_short:
                fc["no_flip"] += 1
                continue

            direction = "LONG" if flipped_long else "SHORT"

            if use_htf and cd["htf_ts"]:
                h4t = get_htf_trend(ts, cd["htf_ts"], cd["htf_map"])
                if h4t is None or (direction == "LONG" and h4t != 1) or (direction == "SHORT" and h4t != -1):
                    fc["htf_filter"] += 1
                    continue

            if adx[idx] < ADX_MIN:
                fc["adx_min"] += 1
                continue

            if adx[idx-2] is None or adx[idx] <= adx[idx-2]:
                fc["adx_rising"] += 1
                continue

            if st_line[idx] is None:
                fc["warmup_none"] += 1
                continue
            dist = abs(closes_[idx] - st_line[idx])

            if debug and len(DEBUG_SAMPLES) < 40:
                DEBUG_SAMPLES.append(dict(
                    symbol=sym, ts=ts, direction=direction,
                    dist=round(dist, 6), dist_threshold=round(DIST_ATR_MULT * atr14[idx], 6),
                    dist_ratio=round(dist / (atr14[idx] or 1), 3),
                    pdi=round(pdi[idx], 3), mdi=round(mdi[idx], 3),
                    di_sep=round(abs(pdi[idx] - mdi[idx]), 3),
                    adx=round(adx[idx], 3),
                ))

            if enable_dist:
                if dist > DIST_ATR_MULT * atr14[idx]:
                    fc["dist_filter"] += 1
                    continue

            if enable_di:
                if direction == "LONG":
                    if pdi[idx] <= mdi[idx] or (pdi[idx] - mdi[idx]) < DI_SEP_MIN:
                        fc["di_sep"] += 1
                        continue
                else:
                    if mdi[idx] <= pdi[idx] or (mdi[idx] - pdi[idx]) < DI_SEP_MIN:
                        fc["di_sep"] += 1
                        continue

            fc["signals_generated"] += 1

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

            open_pos[sym] = dict(dir=direction, entry=entry_p, entry_adj=entry_adj,
                                  sl=sl_price, size=size, entry_t=ts)

    fc_out = {sym: dict(coin_state[sym]["fc"]) for sym in coin_state}
    fc_out["_max_pos_blocked"] = max_pos_blocked
    return all_trades, fc_out

# ── METRICS ───────────────────────────────────────────────────────────────
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

    equity = INITIAL_CAP; peak = INITIAL_CAP; mdd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_t"]):
        equity += t["pnl"]
        if equity > peak: peak = equity
        dd = (peak - equity) / peak
        if dd > mdd: mdd = dd

    monthly = defaultdict(float)
    for t in trades:
        mo = datetime.datetime.utcfromtimestamp(t["exit_t"]/1000).strftime("%Y-%m")
        monthly[mo] += t["pnl"]
    mo_vals = list(monthly.values())
    sharpe = sortino = 0.0
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
    avg_r = (aw / (al if al else 1)) if al else 0  # crude avg-R proxy (win$/loss$)

    return dict(
        symbol=symbol, n=n, wr=round(wr,4), pf=round(pf,4),
        net=round(net,2), mdd=round(mdd,4), sharpe=round(sharpe,3),
        sortino=round(sortino,3), aw=round(aw,2), al=round(al,2),
        exp=round(exp,2), dur=round(avg_dur,1), avg_r=round(avg_r,3),
        nlongs=nlongs, nshorts=nshorts, lwr=round(lwr,4), swr=round(swr,4),
        monthly=dict(sorted(monthly.items())),
        maxcw=maxcw, maxcl=maxcl, gp=round(gp,2), gl=round(gl,2),
    )

# ── SUMMARY WRITER ───────────────────────────────────────────────────────
def strategy_summary(strat_name, all_trades, per_coin_metrics, filter_agg):
    lines = [f"\n{'='*72}", f"  {strat_name}", f"{'='*72}"]
    max_pos_blocked = filter_agg.get("_max_pos_blocked", 0)
    if not all_trades:
        lines.append("  NO TRADES GENERATED")
        lines.append(f"\n  FILTER REJECTION STATS")
        lines.append(f"  {'─'*40}")
        lines.append(f"  Total candles scanned : {filter_agg.get('total_candles',0):,}")
        lines.append(f"  No ST flip            : {filter_agg.get('no_flip',0):,}")
        lines.append(f"  4H HTF filter         : {filter_agg.get('htf_filter',0):,}")
        lines.append(f"  ADX < {ADX_MIN}             : {filter_agg.get('adx_min',0):,}")
        lines.append(f"  ADX not rising        : {filter_agg.get('adx_rising',0):,}")
        lines.append(f"  Dist > 1.5xATR        : {filter_agg.get('dist_filter',0):,}")
        lines.append(f"  DI separation < {DI_SEP_MIN}   : {filter_agg.get('di_sep',0):,}")
        lines.append(f"  Signals generated     : {filter_agg.get('signals_generated',0):,}")
        lines.append(f"  Warmup / None indicators: {filter_agg.get('warmup_none',0):,}")
        lines.append(f"  Max-positions blocked : {max_pos_blocked:,}")
        return "\n".join(lines)

    agg = compute_metrics(all_trades, "AGGREGATE")
    if agg:
        lines += [
            f"\n  AGGREGATE RESULTS", f"  {'─'*50}",
            f"  Total Trades : {agg['n']}",
            f"  Win Rate     : {agg['wr']*100:.1f}%",
            f"  Profit Factor: {agg['pf']:.4f}  {'PASS' if agg['pf'] >= 1.5 else 'FAIL'}",
            f"  Net PnL      : ${agg['net']:,.2f}",
            f"  Max Drawdown : {agg['mdd']*100:.1f}%",
            f"  Sharpe       : {agg['sharpe']:.3f}",
            f"  Sortino      : {agg['sortino']:.3f}",
            f"  Avg Win      : ${agg['aw']:.2f}",
            f"  Avg Loss     : ${agg['al']:.2f}",
            f"  Avg R (proxy): {agg['avg_r']:.3f}",
            f"  Expectancy   : ${agg['exp']:.2f}",
            f"  Avg Duration : {agg['dur']:.1f} bars",
            f"  Longs/Shorts : {agg['nlongs']}/{agg['nshorts']}",
            f"  Long WR      : {agg['lwr']*100:.1f}%",
            f"  Short WR     : {agg['swr']*100:.1f}%",
            f"  Max Win Streak: {agg['maxcw']}  Max Loss Streak: {agg['maxcl']}",
            f"\n  VALIDATION: PF>=1.5 {'YES' if agg['pf']>=1.5 else 'NO'}  WR>=42% {'YES' if agg['wr']>=0.42 else 'NO'}",
        ]

    valid = [m for m in per_coin_metrics if m]
    valid.sort(key=lambda x: x["pf"], reverse=True)
    pf_pass = sum(1 for m in valid if m["pf"] >= 1.5)
    lines.append(f"  PF>=1.5 on {pf_pass}/{len(valid)} coins")
    lines.append(f"\n  PER-COIN BREAKDOWN (sorted by PF desc)")
    lines.append(f"  {'Symbol':<18} {'Trades':>6} {'WR':>7} {'PF':>8} {'Net PnL':>10} {'MDD':>6} {'L/S':>5}")
    lines.append(f"  {'─'*65}")
    for m in valid:
        mk = "PASS" if m["pf"] >= 1.5 else ("~   " if m["pf"] >= 1.0 else "FAIL")
        lines.append(
            f"  {m['symbol']:<18} {m['n']:>6} {m['wr']*100:>6.1f}%"
            f" {m['pf']:>8.4f}{mk} ${m['net']:>9,.2f} {m['mdd']*100:>5.1f}%"
            f" {m['nlongs']}/{m['nshorts']}"
        )

    monthly_all = defaultdict(float)
    for t in all_trades:
        mo = datetime.datetime.utcfromtimestamp(t["exit_t"]/1000).strftime("%Y-%m")
        monthly_all[mo] += t["pnl"]
    lines.append(f"\n  MONTHLY PnL")
    lines.append(f"  {'─'*30}")
    for mo in sorted(monthly_all):
        bar = "+" if monthly_all[mo] >= 0 else "-"
        lines.append(f"  {mo}  {bar}  ${monthly_all[mo]:>9,.2f}")

    lines += [
        f"\n  FILTER REJECTION STATS (aggregated across all coins)",
        f"  {'─'*40}",
        f"  Total candles scanned : {filter_agg.get('total_candles',0):,}",
        f"  No ST flip            : {filter_agg.get('no_flip',0):,}",
        f"  4H HTF filter         : {filter_agg.get('htf_filter',0):,}",
        f"  ADX < {ADX_MIN}             : {filter_agg.get('adx_min',0):,}",
        f"  ADX not rising        : {filter_agg.get('adx_rising',0):,}",
        f"  Dist > 1.5xATR        : {filter_agg.get('dist_filter',0):,}",
        f"  DI separation < {DI_SEP_MIN}   : {filter_agg.get('di_sep',0):,}",
        f"  Signals generated     : {filter_agg.get('signals_generated',0):,}",
        f"  Warmup / None indicators: {filter_agg.get('warmup_none',0):,}",
        f"  Max-positions blocked : {max_pos_blocked:,}",
    ]
    return "\n".join(lines)

# ── MAIN ─────────────────────────────────────────────────────────────────
def main():
    print("SuperTrend + Multi-Filter Backtest — Grok v8 spec")
    print("Data source : data.binance.vision (futures/um monthly klines)")
    print(f"Coins       : {len(ALL_COINS)}")
    print(f"Period      : 2024-07-01 -> 2026-07-01 ({len(MONTHS)} months)")
    print(f"Equity      : Global ${INITIAL_CAP:,.0f}, max {MAX_POSITIONS} positions")
    print("=" * 65)

    print("\n[PHASE 1] Fetching 30m futures data...")
    raw_30 = {}
    for sym in ALL_COINS:
        print(f"  {sym:<18} 30m...", end=" ", flush=True)
        try:
            raw = fetch_symbol_klines(sym, "30m", MONTHS)
            if raw:
                raw_30[sym] = raw
                print(f"OK ({len(raw):,} candles)")
            else:
                print("EMPTY — skip")
        except Exception as e:
            print(f"FAIL: {e}")

    print("\n[PHASE 1b] Fetching 4h futures data...")
    raw_4h = {}
    for sym in ALL_COINS:
        if sym not in raw_30:
            continue
        print(f"  {sym:<18} 4h...", end=" ", flush=True)
        try:
            raw = fetch_symbol_klines(sym, "4h", MONTHS)
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

    if not fetched:
        with open("backtest_summary.txt", "w") as f:
            f.write("NO DATA FETCHED FOR ANY COIN.\n"
                    "data.binance.vision likely blocked from this runner too, "
                    "or the futures/um monthly klines path/symbol list is wrong.\n"
                    f"Skipped: {skipped}\n")
        print("ABORT: no data fetched for any coin — see backtest_summary.txt")
        return

    print("\n[PHASE 2] Pre-computing indicators (once per coin, shared by S1 & S2)...")
    coin_data = {}
    for sym in fetched:
        result = precompute(sym, raw_30[sym], raw_4h.get(sym, []))
        if result:
            coin_data[sym] = result
        else:
            print(f"  WARNING: {sym} had insufficient data — skipped")

    def run_variant(name, use_htf, enable_dist, enable_di, debug=False):
        trades, fc_per_coin = simulate_portfolio(
            coin_data, use_htf=use_htf, enable_dist=enable_dist,
            enable_di=enable_di, debug=debug)
        fc = defaultdict(int)
        for sym, f in fc_per_coin.items():
            if sym == "_max_pos_blocked":
                fc["_max_pos_blocked"] += f
                continue
            for k, v in f.items():
                fc[k] += v
        coin_metrics = [compute_metrics([t for t in trades if t["symbol"] == sym], sym) for sym in fetched]
        print(f"  {name} total trades: {len(trades)}")
        return trades, dict(fc), coin_metrics

    print("\n[PHASE 3] Simulating S1 (full spec, with 4H filter)...")
    s1_trades, s1_fc, s1_coin_metrics = run_variant("S1", use_htf=True, enable_dist=True, enable_di=True, debug=True)

    print("\n[PHASE 4] Simulating S2 (full spec, no 4H filter)...")
    s2_trades, s2_fc, s2_coin_metrics = run_variant("S2", use_htf=False, enable_dist=True, enable_di=True)

    print("\n[PHASE 4b] Simulating S3 (relaxed: flip + ADX + rising ADX only, WITH 4H filter)...")
    s3_trades, s3_fc, s3_coin_metrics = run_variant("S3", use_htf=True, enable_dist=False, enable_di=False)

    print("\n[PHASE 4c] Simulating S4 (relaxed: flip + ADX + rising ADX only, NO 4H filter)...")
    s4_trades, s4_fc, s4_coin_metrics = run_variant("S4", use_htf=False, enable_dist=False, enable_di=False)

    print("\n[PHASE 5] Writing outputs...")
    header = [
        "SUPERTREND + MULTI-FILTER BACKTEST — Grok v8 spec",
        "Period     : 2024-07-01 -> 2026-07-01",
        "Data source: data.binance.vision (futures/um monthly klines)",
        f"Coins      : {len(fetched)} fetched | Skipped: {skipped or 'none'}",
        "Targets    : PF>=1.5  |  WR>=42%",
        f"Portfolio  : ${INITIAL_CAP:,.0f} global equity, max {MAX_POSITIONS} concurrent positions",
        f"Generated  : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
    ]

    s1_text = strategy_summary("S1 — SuperTrend + 4H HTF Filter (Conservative, full spec)", s1_trades, s1_coin_metrics, s1_fc)
    s2_text = strategy_summary("S2 — SuperTrend, NO 4H Filter (Aggressive, full spec)", s2_trades, s2_coin_metrics, s2_fc)
    s3_text = strategy_summary("S3 — RELAXED: flip + ADX>=25 + rising ADX only, WITH 4H filter (dist+DI disabled)", s3_trades, s3_coin_metrics, s3_fc)
    s4_text = strategy_summary("S4 — RELAXED: flip + ADX>=25 + rising ADX only, NO 4H filter (dist+DI disabled)", s4_trades, s4_coin_metrics, s4_fc)

    s1_agg = compute_metrics(s1_trades)
    s2_agg = compute_metrics(s2_trades)
    s3_agg = compute_metrics(s3_trades)
    s4_agg = compute_metrics(s4_trades)

    rec = ["\n" + "="*72, "  RECOMMENDATION", "="*72]
    for name, agg in [("S1 (full spec, 4H filter)", s1_agg), ("S2 (full spec, no 4H)", s2_agg),
                       ("S3 (relaxed, 4H filter)", s3_agg), ("S4 (relaxed, no 4H)", s4_agg)]:
        if agg:
            status = "USABLE" if agg["pf"] >= 1.5 and agg["wr"] >= 0.42 else "NOT READY"
            rec.append(f"  {name}: PF={agg['pf']:.4f}  WR={agg['wr']*100:.1f}%  Net=${agg['net']:,.2f}  MDD={agg['mdd']*100:.1f}%  -> {status}")
        else:
            rec.append(f"  {name}: NO TRADES -> NOT READY")

    debug_lines = ["\n" + "="*72, "  DEBUG SAMPLES — first flip candles that reached the dist/DI check", "="*72]
    if DEBUG_SAMPLES:
        debug_lines.append(f"  {'Symbol':<16}{'Dir':<7}{'dist':>10}{'dist_thr':>10}{'dist/ATR14':>12}{'pdi':>8}{'mdi':>8}{'DIsep':>8}{'ADX':>7}")
        for s in DEBUG_SAMPLES:
            debug_lines.append(
                f"  {s['symbol']:<16}{s['direction']:<7}{s['dist']:>10.4f}{s['dist_threshold']:>10.4f}"
                f"{s['dist_ratio']:>12.3f}{s['pdi']:>8.2f}{s['mdi']:>8.2f}{s['di_sep']:>8.2f}{s['adx']:>7.2f}"
            )
        avg_ratio = sum(s['dist_ratio'] for s in DEBUG_SAMPLES) / len(DEBUG_SAMPLES)
        debug_lines.append(f"\n  Avg dist/threshold ratio across samples: {avg_ratio:.3f}"
                            f"  ({'>1 means dist filter would reject this candle' if avg_ratio>1 else '<=1 means it would pass'})")
        none_pdi = sum(1 for s in DEBUG_SAMPLES if s['pdi'] is None or s['mdi'] is None)
        debug_lines.append(f"  Samples with None pdi/mdi: {none_pdi}/{len(DEBUG_SAMPLES)} (confirms whether DI values are valid at this point)")
    else:
        debug_lines.append("  No candles ever reached the dist/DI check in S1 — every flip was rejected earlier"
                            " (by no-flip, HTF, ADX-min, or ADX-rising).")

    full_txt = "\n".join(header) + s1_text + s2_text + s3_text + s4_text + "\n".join(rec) + "\n".join(debug_lines)
    with open("backtest_summary.txt", "w") as f:
        f.write(full_txt)
    print("  wrote backtest_summary.txt")

    def safe(m): return m if m else {}
    report = {
        "meta": {
            "strategy": "SuperTrend + Multi-Filter (Grok v8 spec)",
            "period": "2024-07-01 to 2026-07-01",
            "data_source": "data.binance.vision futures/um monthly klines",
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
            "aggregate": safe(s1_agg), "per_coin": [safe(m) for m in s1_coin_metrics],
            "filter_stats": s1_fc, "trades": s1_trades,
        },
        "S2_no_4h_filter": {
            "aggregate": safe(s2_agg), "per_coin": [safe(m) for m in s2_coin_metrics],
            "filter_stats": s2_fc, "trades": s2_trades,
        },
        "S3_relaxed_with_4h": {
            "aggregate": safe(s3_agg), "per_coin": [safe(m) for m in s3_coin_metrics],
            "filter_stats": s3_fc, "trades": s3_trades,
        },
        "S4_relaxed_no_4h": {
            "aggregate": safe(s4_agg), "per_coin": [safe(m) for m in s4_coin_metrics],
            "filter_stats": s4_fc, "trades": s4_trades,
        },
        "debug_samples_S1": DEBUG_SAMPLES,
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("  wrote backtest_report.json")

    print("\n" + "="*65)
    print(full_txt)

if __name__ == "__main__":
    main()
