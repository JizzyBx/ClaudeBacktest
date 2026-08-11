"""
GMax V1 — Final Stage Backtest
Strategies: BASELINE (P14_T22), CANDIDATE A (P21_T25), CANDIDATE B (P21_T28)
Coins: PF >= 1.2 filtered per-strategy (86 / 82 / 90 coins)
Leverage: 5x | Capital: $10,000 | Risk/trade: 0.75%
Period: Aug 2024 – Jul 2026 (2 years)

Market regime sub-periods tested:
  GOOD_1: Aug 2024 – Oct 2024  (crypto bull pre-ATH)
  GOOD_2: Nov 2024 – Jan 2025  (post-election pump / ATH zone)
  BAD_1:  Feb 2025 – Apr 2025  (correction / crash)
  BAD_2:  May 2025 – Jul 2025  (choppy bear / recovery attempt)
  GOOD_3: Aug 2025 – Oct 2025  (mid-cycle recovery)
  MID:    Nov 2025 – Jan 2026
  BAD_3:  Feb 2026 – Apr 2026  (late bear)
  RECENT: May 2026 – Jul 2026  (recent)

Usage:
  python backtest.py <shard_idx>        # run shard 0-7
  python backtest.py merge              # merge all shards
"""

import sys, json, csv, io, zipfile, urllib.request, time, os
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# ─────────────────────────────────────────────
# STRATEGIES
# ─────────────────────────────────────────────
STRATEGIES = {
    "BASELINE": {
        "adx_period": 14,
        "adx_thresh": 22,
        "coins": [
            "TURBOUSDT","1000RATSUSDT","VINEUSDT","FXSUSDT","ACHUSDT","PIXELUSDT",
            "BIDUSDT","MOODENGUSDT","POWRUSDT","ALGOUSDT","NMRUSDT","AINUSDT",
            "NOTUSDT","ENAUSDT","ZECUSDT","MAGICUSDT","ETHUSDT","1000CATUSDT",
            "NFPUSDT","SAGAUSDT","ALPINEUSDT","BELUSDT","UBUSDT","EPICUSDT",
            "ASRUSDT","EPTUSDT","SANTOSUSDT","1000000BOBUSDT","PUMPBTCUSDT",
            "CFXUSDT","PLUMEUSDT","KITEUSDT","USUALUSDT","DAMUSDT","YBUSDT",
            "ALICEUSDT","POWERUSDT","DIAUSDT","ARKMUSDT","SPELLUSDT","LIGHTUSDT",
            "CRCLUSDT","ICXUSDT","FLUXUSDT","A2ZUSDT","BTRUSDT","ZEREBROUSDT",
            "RESOLVUSDT","ORBSUSDT","XRPUSDT","AI16ZUSDT","RAVEUSDT","LRCUSDT",
            "GUAUSDT","BMTUSDT","PTBUSDT","AUSDT","LYNUSDT","GRIFFAINUSDT",
            "COMBOUSDT","ELSAUSDT","OBOLUSDT","HEMIUSDT","HANAUSDT","DMCUSDT",
            "QUICKUSDT","AWEUSDT","COAIUSDT","SPKUSDT","XEMUSDT","STBLUSDT",
            "RVVUSDT","MILKUSDT","TRUTHUSDT","ASTERUSDT","MEGAUSDT","OPENUSDT",
            "LABUSDT","SOMIUSDT","RLSUSDT","FLNCUSDT","CHIPUSDT","REEFUSDT",
            "OPNUSDT","BASEDUSDT","SKRUSDT",
        ],
    },
    "CAND_A": {
        "adx_period": 21,
        "adx_thresh": 25,
        "coins": [
            "1000RATSUSDT","EIGENUSDT","ZECUSDT","VANRYUSDT","NOTUSDT","FLUXUSDT",
            "TURBOUSDT","ETHUSDT","ACHUSDT","PIXELUSDT","SEIUSDT","BMTUSDT",
            "DEXEUSDT","CFXUSDT","MOODENGUSDT","BELUSDT","ENAUSDT","WLDUSDT",
            "BIDUSDT","ALPINEUSDT","SPKUSDT","ALGOUSDT","MTLUSDT","IPUSDT",
            "PIPPINUSDT","LIGHTUSDT","BANKUSDT","HEMIUSDT","PLUMEUSDT","SAGAUSDT",
            "FXSUSDT","DAMUSDT","ASRUSDT","SPELLUSDT","QUICKUSDT","AINUSDT",
            "LRCUSDT","VVVUSDT","KITEUSDT","VINEUSDT","AI16ZUSDT","BTRUSDT",
            "POWERUSDT","ZEREBROUSDT","ASTERUSDT","UBUSDT","STBLUSDT","EVAAUSDT",
            "PUMPBTCUSDT","1000000BOBUSDT","POLUSDT","SANTOSUSDT","TRUTHUSDT",
            "OPNUSDT","AIOTUSDT","COAIUSDT","CRCLUSDT","BASEDUSDT","XLMUSDT",
            "NOMUSDT","HANAUSDT","RAVEUSDT","ORBSUSDT","XEMUSDT","COMBOUSDT",
            "OPENUSDT","AUSDT","SOMIUSDT","A2ZUSDT","AWEUSDT","GRIFFAINUSDT",
            "CHIPUSDT","YBUSDT","CUSDT","GUAUSDT","RVVUSDT","RLSUSDT",
            "SNDKUSDT","DMCUSDT","ELSAUSDT","STABLEUSDT","SKRUSDT",
        ],
    },
    "CAND_B": {
        "adx_period": 21,
        "adx_thresh": 28,
        "coins": [
            "TURBOUSDT","NOTUSDT","NFPUSDT","PIXELUSDT","FLUXUSDT","XLMUSDT",
            "ZEREBROUSDT","BMTUSDT","EIGENUSDT","ACHUSDT","MTLUSDT","DEXEUSDT",
            "1000RATSUSDT","SEIUSDT","BELUSDT","VANRYUSDT","1000000BOBUSDT",
            "SAGAUSDT","ZECUSDT","MOODENGUSDT","HEMIUSDT","XRPUSDT","PIPPINUSDT",
            "KITEUSDT","ALPINEUSDT","QUICKUSDT","NOMUSDT","SPKUSDT","ETHUSDT",
            "FXSUSDT","VINEUSDT","USUALUSDT","POWRUSDT","ARKMUSDT","NMRUSDT",
            "POWERUSDT","XEMUSDT","HANAUSDT","INITUSDT","PUMPBTCUSDT","SANTOSUSDT",
            "IPUSDT","SIGNUSDT","CFXUSDT","ALICEUSDT","BANKUSDT","BIDUSDT",
            "DMCUSDT","EVAAUSDT","CUSDT","AIOTUSDT","UBUSDT","AI16ZUSDT",
            "YBUSDT","ASRUSDT","ICXUSDT","WLDUSDT","STBLUSDT","CHIPUSDT",
            "TRUTHUSDT","ELSAUSDT","RAVEUSDT","COAIUSDT","DAMUSDT","ANKRUSDT",
            "AWEUSDT","BTRUSDT","LIGHTUSDT","AINUSDT","GLMUSDT","OBOLUSDT",
            "ASTERUSDT","GUAUSDT","BASEDUSDT","OPNUSDT","SNDKUSDT","COMBOUSDT",
            "EPICUSDT","CRCLUSDT","A2ZUSDT","GRIFFAINUSDT","PLUMEUSDT","MEGAUSDT",
            "RLSUSDT","FLNCUSDT","STABLEUSDT","REEFUSDT","SOMIUSDT","RVVUSDT",
            "ORBSUSDT",
        ],
    },
}

# ─────────────────────────────────────────────
# FIXED TRADE PARAMETERS
# ─────────────────────────────────────────────
CAPITAL   = 10_000.0
RISK_PCT  = 0.0075   # 0.75%
FEE       = 0.0005   # 0.05%
SLIP      = 0.0002   # 0.02%
LEVERAGE  = 5
TP_PCT    = 0.030    # 3%
SL_PCT    = 0.150    # 15%
MAX_BARS  = 960      # 10 days on 15m
TIMEFRAME = "15m"
MIN_BARS  = 150      # warmup (max adx_period*3 = 63 + ema50 = 50, use 150 safe)

# ─────────────────────────────────────────────
# FULL PERIOD + REGIME SUB-PERIODS
# ─────────────────────────────────────────────
FULL_START = (2024, 8)
FULL_END   = (2026, 7)

REGIMES = {
    "FULL_2YR":  {"start": (2024,  8), "end": (2026,  7), "label": "Full 2-Year"},
    "GOOD_1":    {"start": (2024,  8), "end": (2024, 10), "label": "Good Market 1 — Bull Run (Aug-Oct 2024)"},
    "GOOD_2":    {"start": (2024, 11), "end": (2025,  1), "label": "Good Market 2 — Post-Election ATH (Nov 2024-Jan 2025)"},
    "BAD_1":     {"start": (2025,  2), "end": (2025,  4), "label": "Bad Market 1 — Crash/Correction (Feb-Apr 2025)"},
    "BAD_2":     {"start": (2025,  5), "end": (2025,  7), "label": "Bad Market 2 — Choppy Bear (May-Jul 2025)"},
    "MID_1":     {"start": (2025,  8), "end": (2025, 10), "label": "Mid Market 1 — Recovery (Aug-Oct 2025)"},
    "MID_2":     {"start": (2025, 11), "end": (2026,  1), "label": "Mid Market 2 — Late Cycle (Nov 2025-Jan 2026)"},
    "BAD_3":     {"start": (2026,  2), "end": (2026,  4), "label": "Bad Market 3 — Late Bear (Feb-Apr 2026)"},
    "RECENT":    {"start": (2026,  5), "end": (2026,  7), "label": "Recent Period (May-Jul 2026)"},
}

# ─────────────────────────────────────────────
# SHARDING
# ─────────────────────────────────────────────
NUM_SHARDS = 8
WORKERS    = 16

def all_symbols():
    seen = set()
    out  = []
    for s in STRATEGIES.values():
        for c in s["coins"]:
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out

# ─────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month):
    url = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        out = []
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts > 10**14:
                ts //= 1000
            out.append((ts, float(row[1]), float(row[2]), float(row[3]), float(row[4])))
        return out
    except Exception:
        return []

def fetch_symbol(symbol, start_ym=FULL_START, end_ym=FULL_END):
    all_candles = []
    y, m = start_ym
    ey, em = end_ym
    while (y, m) <= (ey, em):
        all_candles.extend(fetch_month(symbol, y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    # deduplicate + sort
    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    return sorted(seen.values())

# ─────────────────────────────────────────────
# INDICATORS (pure Python)
# ─────────────────────────────────────────────
def ema_series(prices, period):
    k = 2.0 / (period + 1)
    out = [None] * len(prices)
    for i, p in enumerate(prices):
        if i == 0:
            out[i] = p
        else:
            out[i] = p * k + out[i-1] * (1 - k)
    return out

def adx_series(highs, lows, closes, period):
    n = len(closes)
    if n < period + 1:
        return [0.0] * n

    tr_list   = [0.0] * n
    pdm_list  = [0.0] * n
    mdm_list  = [0.0] * n

    for i in range(1, n):
        hi, lo, pc = highs[i], lows[i], closes[i-1]
        tr = max(hi - lo, abs(hi - pc), abs(lo - pc))
        pdm = max(highs[i] - highs[i-1], 0)
        mdm = max(lows[i-1] - lows[i], 0)
        if pdm > mdm:
            mdm = 0.0
        elif mdm > pdm:
            pdm = 0.0
        else:
            pdm = mdm = 0.0
        tr_list[i]  = tr
        pdm_list[i] = pdm
        mdm_list[i] = mdm

    # Wilder smoothing
    k = 1.0 / period
    atr_s = pdi_s = mdi_s = 0.0
    adx_out = [0.0] * n

    for i in range(1, n):
        if i < period:
            atr_s += tr_list[i]
            pdi_s += pdm_list[i]
            mdi_s += mdm_list[i]
        elif i == period:
            atr_s += tr_list[i]
            pdi_s += pdm_list[i]
            mdi_s += mdm_list[i]
        else:
            atr_s = atr_s * (1 - k) + tr_list[i]
            pdi_s = pdi_s * (1 - k) + pdm_list[i]
            mdi_s = mdi_s * (1 - k) + mdm_list[i]

    # Second pass for ADX using Wilder on DX
    atr_s = pdi_s = mdi_s = dx_smooth = 0.0
    dx_list = [0.0] * n
    for i in range(1, n):
        if i <= period:
            atr_s += tr_list[i]
            pdi_s += pdm_list[i]
            mdi_s += mdm_list[i]
            if i == period:
                pdi = 100 * pdi_s / atr_s if atr_s else 0
                mdi = 100 * mdi_s / atr_s if atr_s else 0
                dx  = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) else 0
                dx_list[i] = dx
        else:
            atr_s = atr_s - atr_s / period + tr_list[i]
            pdi_s = pdi_s - pdi_s / period + pdm_list[i]
            mdi_s = mdi_s - mdi_s / period + mdm_list[i]
            pdi = 100 * pdi_s / atr_s if atr_s else 0
            mdi = 100 * mdi_s / atr_s if atr_s else 0
            dx  = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) else 0
            dx_list[i] = dx

    # ADX = Wilder EMA of DX over period bars (start after first valid DX)
    adx_val = 0.0
    count   = 0
    for i in range(1, n):
        if i < period:
            continue
        if count < period:
            adx_val += dx_list[i]
            count   += 1
            if count == period:
                adx_val /= period
                adx_out[i] = adx_val
        else:
            adx_val = (adx_val * (period - 1) + dx_list[i]) / period
            adx_out[i] = adx_val

    return adx_out

# ─────────────────────────────────────────────
# SIGNAL
# ─────────────────────────────────────────────
def signal(i, ema9, ema21, ema50, adx_vals, adx_thresh):
    if i < 1 or adx_vals[i] == 0:
        return None

    # Stage 1: EMA50 slope
    if i < 10:
        return None
    slope = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100 if ema50[i-10] else 0
    trend_up   = slope >  0.05
    trend_down = slope < -0.05

    # Stage 2: EMA9/21 crossover
    crossed_up   = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
    crossed_down = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]

    # Stage 3: ADX gate
    if adx_vals[i] < adx_thresh:
        return None

    if trend_up and crossed_up:
        return "buy"
    if trend_down and crossed_down:
        return "sell"
    return None

# ─────────────────────────────────────────────
# BACKTEST SINGLE SYMBOL + STRATEGY
# ─────────────────────────────────────────────
def backtest_symbol(symbol, candles, adx_period, adx_thresh):
    if len(candles) < MIN_BARS:
        return []

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    ema9_arr  = ema_series(closes, 9)
    ema21_arr = ema_series(closes, 21)
    ema50_arr = ema_series(closes, 50)
    adx_arr   = adx_series(highs, lows, closes, adx_period)

    notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
    trades   = []
    in_trade = False
    n        = len(candles)

    for i in range(MIN_BARS - 1, n - 1):
        if in_trade:
            # Check SL then TP on each bar (bar i+1 while in trade from entry)
            bar_hi = highs[i]
            bar_lo = lows[i]
            bar_cl = closes[i]
            bar_ts = ts_arr[i]
            bars_held += 1

            if trade_side == "buy":
                sl_hit = bar_lo <= sl_price
                tp_hit = bar_hi >= tp_price
            else:
                sl_hit = bar_hi >= sl_price
                tp_hit = bar_lo <= tp_price

            exit_price = exit_reason = None

            if sl_hit:
                exit_price  = sl_price
                exit_reason = "sl"
            elif tp_hit:
                exit_price  = tp_price
                exit_reason = "tp"
            elif bars_held >= MAX_BARS:
                exit_price  = bar_cl
                exit_reason = "max_hold"

            if exit_price is not None:
                if trade_side == "buy":
                    gross = (exit_price - entry_price) / entry_price
                else:
                    gross = (entry_price - exit_price) / entry_price
                net = gross - (FEE + SLIP) * 2
                pnl = notional * net
                trades.append({
                    "symbol":      symbol,
                    "side":        trade_side,
                    "entry_ts":    entry_ts,
                    "exit_ts":     bar_ts,
                    "entry_price": entry_price,
                    "exit_price":  exit_price,
                    "pnl":         round(pnl, 4),
                    "reason":      exit_reason,
                    "bars":        bars_held,
                })
                in_trade = False
            continue

        # No open trade — check signal
        sig = signal(i, ema9_arr, ema21_arr, ema50_arr, adx_arr, adx_thresh)
        if sig is None:
            continue

        # Entry on bar i+1 open
        raw_entry = opens[i + 1]
        if sig == "buy":
            entry_price = raw_entry * (1 + FEE + SLIP)
            tp_price    = entry_price * (1 + TP_PCT)
            sl_price    = entry_price * (1 - SL_PCT)
        else:
            entry_price = raw_entry * (1 - FEE - SLIP)
            tp_price    = entry_price * (1 - TP_PCT)
            sl_price    = entry_price * (1 + SL_PCT)

        entry_ts  = ts_arr[i + 1]
        trade_side = sig
        bars_held  = 0
        in_trade   = True

    # Close open trade at end of data
    if in_trade:
        exit_price = closes[-1]
        if trade_side == "buy":
            gross = (exit_price - entry_price) / entry_price
        else:
            gross = (entry_price - exit_price) / entry_price
        net = gross - (FEE + SLIP) * 2
        pnl = notional * net
        trades.append({
            "symbol":      symbol,
            "side":        trade_side,
            "entry_ts":    entry_ts,
            "exit_ts":     ts_arr[-1],
            "entry_price": entry_price,
            "exit_price":  exit_price,
            "pnl":         round(pnl, 4),
            "reason":      "end_of_data",
            "bars":        bars_held,
        })

    return trades

# ─────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────
def compute_stats(trades, label=""):
    if not trades:
        return {
            "label": label, "total": 0, "win_rate": 0, "profit_factor": 0,
            "net_pnl": 0, "max_drawdown": 0, "avg_win": 0, "avg_loss": 0,
            "expectancy": 0, "longs": 0, "shorts": 0,
            "sharpe": 0, "monthly": {}, "per_coin": {},
        }

    wins  = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses= [t["pnl"] for t in trades if t["pnl"] <= 0]
    total = len(trades)
    wr    = len(wins) / total * 100

    gross_win  = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 0 else 999.0
    net_pnl = sum(t["pnl"] for t in trades)

    avg_win  = gross_win  / len(wins)   if wins   else 0
    avg_loss = gross_loss / len(losses) if losses else 0
    exp      = (wr / 100 * avg_win) - ((1 - wr / 100) * avg_loss)

    # Max drawdown (cumulative equity curve)
    equity   = 0.0
    peak     = 0.0
    max_dd   = 0.0
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        equity += t["pnl"]
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # Sharpe (trade-level, annualized via sqrt(trades_per_year))
    import math
    pnls = [t["pnl"] for t in trades]
    mean_pnl = sum(pnls) / len(pnls)
    var = sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)
    std = math.sqrt(var) if var > 0 else 1e-9
    # Estimate trades per year: assume 2yr period
    tpy = total / 2.0
    sharpe = (mean_pnl / std) * math.sqrt(tpy)

    # Monthly PnL
    monthly = defaultdict(lambda: {"pnl": 0.0, "n": 0, "w": 0})
    for t in trades:
        import datetime
        dt  = datetime.datetime.utcfromtimestamp(t["exit_ts"] / 1000)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key]["pnl"] += t["pnl"]
        monthly[key]["n"]   += 1
        if t["pnl"] > 0:
            monthly[key]["w"] += 1

    # Per-coin
    per_coin = defaultdict(lambda: {"pnl": 0.0, "n": 0, "w": 0, "wr": 0.0})
    for t in trades:
        s = t["symbol"]
        per_coin[s]["pnl"] += t["pnl"]
        per_coin[s]["n"]   += 1
        if t["pnl"] > 0:
            per_coin[s]["w"] += 1
    for s in per_coin:
        n = per_coin[s]["n"]
        per_coin[s]["wr"] = round(per_coin[s]["w"] / n * 100, 2) if n else 0

    longs  = sum(1 for t in trades if t["side"] == "buy")
    shorts = sum(1 for t in trades if t["side"] == "sell")

    return {
        "label":          label,
        "total":          total,
        "win_rate":       round(wr, 2),
        "profit_factor":  round(pf, 3),
        "net_pnl":        round(net_pnl, 2),
        "max_drawdown":   round(max_dd, 2),
        "max_drawdown_pct": round(max_dd / CAPITAL * 100, 2),
        "avg_win":        round(avg_win, 2),
        "avg_loss":       round(avg_loss, 2),
        "expectancy":     round(exp, 2),
        "sharpe":         round(sharpe, 3),
        "longs":          longs,
        "shorts":         shorts,
        "monthly":        {k: dict(v) for k, v in sorted(monthly.items())},
        "per_coin":       {k: dict(v) for k, v in per_coin.items()},
    }

# ─────────────────────────────────────────────
# REGIME FILTER  (filter trades by timestamp range)
# ─────────────────────────────────────────────
def ts_from_ym(year, month, end=False):
    import calendar, datetime
    if end:
        last_day = calendar.monthrange(year, month)[1]
        dt = datetime.datetime(year, month, last_day, 23, 59, 59)
    else:
        dt = datetime.datetime(year, month, 1, 0, 0, 0)
    return int(dt.timestamp() * 1000)

def filter_trades_by_regime(trades, start_ym, end_ym):
    ts_start = ts_from_ym(*start_ym, end=False)
    ts_end   = ts_from_ym(*end_ym,   end=True)
    return [t for t in trades if ts_start <= t["exit_ts"] <= ts_end]

# ─────────────────────────────────────────────
# SHARD RUNNER
# ─────────────────────────────────────────────
def run_shard(shard_idx):
    symbols = all_symbols()
    my_symbols = symbols[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(my_symbols)} unique symbols to fetch", flush=True)

    # Fetch all candles
    candle_cache = {}
    def _fetch(sym):
        data = fetch_symbol(sym, FULL_START, FULL_END)
        return sym, data

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(_fetch, my_symbols))

    for sym, data in results:
        candle_cache[sym] = data
        status = f"✓ {len(data)} candles" if data else "✗ no data"
        print(f"  {sym}: {status}", flush=True)

    # Run backtest for each strategy × each coin in this shard
    all_trades_by_strategy = {name: [] for name in STRATEGIES}

    for strat_name, strat_cfg in STRATEGIES.items():
        adx_p = strat_cfg["adx_period"]
        adx_t = strat_cfg["adx_thresh"]
        for sym in strat_cfg["coins"]:
            if sym not in candle_cache:
                continue
            candles = candle_cache[sym]
            if not candles:
                continue
            trades = backtest_symbol(sym, candles, adx_p, adx_t)
            all_trades_by_strategy[strat_name].extend(trades)

    elapsed = time.time() - t0

    shard_out = {
        "shard":    shard_idx,
        "symbols":  my_symbols,
        "with_data": [s for s, d in results if d],
        "strategy_trades": {k: v for k, v in all_trades_by_strategy.items()},
        "elapsed":  round(elapsed, 1),
    }

    with open(f"shard_{shard_idx}.json", "w") as f:
        json.dump(shard_out, f)
    print(f"[Shard {shard_idx}] Done in {elapsed:.1f}s", flush=True)

# ─────────────────────────────────────────────
# MERGE + REPORT
# ─────────────────────────────────────────────
def merge_shards():
    all_trades_by_strategy = {name: [] for name in STRATEGIES}
    all_symbols_seen = []
    all_with_data    = []

    for i in range(NUM_SHARDS):
        fname = f"shard_{i}.json"
        if not os.path.exists(fname):
            print(f"WARNING: {fname} not found, skipping")
            continue
        with open(fname) as f:
            shard = json.load(f)
        all_symbols_seen.extend(shard.get("symbols", []))
        all_with_data.extend(shard.get("with_data", []))
        for strat_name in STRATEGIES:
            t = shard.get("strategy_trades", {}).get(strat_name, [])
            all_trades_by_strategy[strat_name].extend(t)

    # Build report
    report = {
        "meta": {
            "period_full":  "Aug 2024 – Jul 2026",
            "capital":      CAPITAL,
            "leverage":     LEVERAGE,
            "risk_pct":     RISK_PCT,
            "tp_pct":       TP_PCT,
            "sl_pct":       SL_PCT,
            "fee":          FEE,
            "slip":         SLIP,
            "symbols_attempted": len(set(all_symbols_seen)),
            "symbols_with_data": len(set(all_with_data)),
        },
        "strategies": {},
    }

    for strat_name, trades in all_trades_by_strategy.items():
        strat_report = {
            "config": {
                "adx_period": STRATEGIES[strat_name]["adx_period"],
                "adx_thresh": STRATEGIES[strat_name]["adx_thresh"],
                "coin_count": len(STRATEGIES[strat_name]["coins"]),
            },
            "regimes": {},
        }

        for regime_key, regime_info in REGIMES.items():
            if regime_key == "FULL_2YR":
                rt = trades
            else:
                rt = filter_trades_by_regime(trades, regime_info["start"], regime_info["end"])
            stats = compute_stats(rt, label=regime_info["label"])
            strat_report["regimes"][regime_key] = stats

        report["strategies"][strat_name] = strat_report

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Write human-readable summary
    write_summary(report)
    print("✅ Merge complete. backtest_report.json + backtest_summary.txt written.")

# ─────────────────────────────────────────────
# SUMMARY TEXT
# ─────────────────────────────────────────────
def write_summary(report):
    lines = []
    def w(s=""):
        lines.append(s)

    w("=" * 72)
    w("  GMAX V1 — FINAL STAGE BACKTEST REPORT")
    w("  PF >= 1.2 Filtered Coins | 5x Leverage | 0.75% Risk/Trade")
    w("=" * 72)
    m = report["meta"]
    w(f"  Period:    {m['period_full']}")
    w(f"  Capital:   ${m['capital']:,.0f}  |  Leverage: {m['leverage']}x")
    w(f"  TP: {m['tp_pct']*100:.1f}%  |  SL: {m['sl_pct']*100:.1f}%  |  Fee: {m['fee']*100:.3f}%  |  Slip: {m['slip']*100:.3f}%")
    w(f"  Symbols attempted: {m['symbols_attempted']}  |  With data: {m['symbols_with_data']}")
    w()

    REGIME_ORDER = ["FULL_2YR","GOOD_1","GOOD_2","BAD_1","BAD_2","MID_1","MID_2","BAD_3","RECENT"]
    STRAT_ORDER  = ["BASELINE","CAND_A","CAND_B"]

    for strat_name in STRAT_ORDER:
        strat_data = report["strategies"].get(strat_name, {})
        cfg        = strat_data.get("config", {})
        w("─" * 72)
        w(f"  STRATEGY: {strat_name}  "
          f"(ADX period={cfg.get('adx_period')} thresh={cfg.get('adx_thresh')}  "
          f"coins={cfg.get('coin_count')})")
        w("─" * 72)

        # Full period
        full = strat_data.get("regimes", {}).get("FULL_2YR", {})
        usable = "✅ USABLE" if full.get("profit_factor", 0) >= 1.2 and full.get("win_rate", 0) >= 55 else "❌ REVIEW"
        w(f"  FULL 2-YEAR SUMMARY  {usable}")
        w(f"    Trades: {full.get('total',0):,}  |  WR: {full.get('win_rate',0):.2f}%  |  PF: {full.get('profit_factor',0):.3f}")
        w(f"    Net PnL: ${full.get('net_pnl',0):,.2f}  |  Max DD: ${full.get('max_drawdown',0):,.2f} ({full.get('max_drawdown_pct',0):.2f}%)")
        w(f"    Sharpe: {full.get('sharpe',0):.3f}  |  Expectancy: ${full.get('expectancy',0):.2f}/trade")
        w(f"    Avg Win: ${full.get('avg_win',0):.2f}  |  Avg Loss: ${full.get('avg_loss',0):.2f}")
        w()

        # Monthly profitable months
        monthly = full.get("monthly", {})
        if monthly:
            green = sum(1 for v in monthly.values() if v["pnl"] > 0)
            total_m = len(monthly)
            w(f"    Green months: {green}/{total_m}")
            w()

        # Regime breakdown table
        w("  REGIME BREAKDOWN:")
        w(f"  {'Regime':<30} {'Trades':>7} {'WR%':>7} {'PF':>7} {'PnL $':>10} {'DD%':>7} {'Sharpe':>8}")
        w("  " + "-" * 70)

        for rkey in REGIME_ORDER:
            if rkey == "FULL_2YR":
                continue
            r = strat_data.get("regimes", {}).get(rkey, {})
            label = REGIMES[rkey]["label"].split("—")[0].strip()[:29]
            w(f"  {label:<30} {r.get('total',0):>7,} {r.get('win_rate',0):>7.2f} "
              f"{r.get('profit_factor',0):>7.3f} {r.get('net_pnl',0):>10.2f} "
              f"{r.get('max_drawdown_pct',0):>7.2f} {r.get('sharpe',0):>8.3f}")
        w()

        # Monthly PnL table
        w("  MONTHLY PnL (Full Period):")
        w(f"  {'Month':<10} {'PnL $':>12} {'Trades':>8} {'WR%':>8}")
        w("  " + "-" * 44)
        for month_key in sorted(monthly.keys()):
            mv = monthly[month_key]
            mwr = mv["w"] / mv["n"] * 100 if mv["n"] else 0
            marker = "  ✅" if mv["pnl"] > 0 else "  ❌"
            w(f"  {month_key:<10} {mv['pnl']:>12.2f} {mv['n']:>8} {mwr:>8.1f}{marker}")
        w()

        # Top 20 coins by PnL
        per_coin = full.get("per_coin", {})
        if per_coin:
            top20 = sorted(per_coin.items(), key=lambda x: x[1]["pnl"], reverse=True)[:20]
            w("  TOP 20 COINS (Full Period, by Net PnL):")
            w(f"  {'Symbol':<22} {'Trades':>7} {'WR%':>7} {'PnL $':>10}")
            w("  " + "-" * 52)
            for sym, sv in top20:
                w(f"  {sym:<22} {sv['n']:>7} {sv['wr']:>7.1f} {sv['pnl']:>10.2f}")
        w()

    # Cross-strategy comparison
    w("=" * 72)
    w("  CROSS-STRATEGY COMPARISON MATRIX")
    w("=" * 72)
    w(f"  {'Regime':<28} {'':>4} {'BASELINE':>10} {'CAND_A':>10} {'CAND_B':>10}")
    w("  " + "-" * 68)

    metrics_to_show = [
        ("PF",      "profit_factor", "{:.3f}"),
        ("WR%",     "win_rate",      "{:.2f}"),
        ("PnL $",   "net_pnl",       "{:.0f}"),
        ("DD%",     "max_drawdown_pct", "{:.2f}"),
        ("Sharpe",  "sharpe",        "{:.3f}"),
    ]

    for rkey in REGIME_ORDER:
        rlabel = REGIMES[rkey]["label"]
        # short label
        if "FULL" in rkey:
            short = "Full 2-Year"
        else:
            short = rlabel.split("—")[0].strip()[:27]

        w(f"\n  {short}")
        for mname, mkey, mfmt in metrics_to_show:
            row = f"    {mname:<10}"
            for sname in STRAT_ORDER:
                val = report["strategies"].get(sname, {}).get("regimes", {}).get(rkey, {}).get(mkey, 0)
                row += f"  {mfmt.format(val):>10}"
            w(row)

    w()
    w("=" * 72)
    w("  END OF REPORT")
    w("=" * 72)

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))
