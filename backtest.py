#!/usr/bin/env python3
"""
Binance USDT-M Futures Multi-Strategy Backtest
Repo: JizzyBx/Backtestyml
Spec: Grok multi-strategy brief, 2026-07-28 (V6)

stdlib-only, Python 3.11+. No pip installs.

Follows HANDOFF_Backtest_Pipeline.md:
  - Data source: data.binance.vision futures/um monthly (+ daily tail) archives
    (fapi.binance.com is 451-blocked on GH Actions runners; data-api.binance.vision
    is spot-only and lacks 1000-prefixed coins)
  - Closed-candle-only evaluation, no lookahead
  - Filter-rejection counters sum back to total candles scanned (per strategy)
  - Zero-trades / 100%-fetch-failure gets a loud, explicit abort message,
    not a silent "0 trades" report

SCOPE NOTE (this run): only the PRIMARY timeframe per strategy is implemented
(30m for Strategy 1 & 3, 1h for Strategy 2 & 4). The spec's "also test 1H as
secondary" variants for S1/S3 are not included in this pass -- add as a
follow-up run once we see which strategies show any life.

Coin list: static (see COIN LIST section) -- live top-100-by-volume ranking
would require fapi.binance.com, which is geo-blocked on GH Actions runners.
Per-symbol 6-month-minimum-history is enforced at runtime, not by pre-filtering
the list.
"""

import json
import csv
import io
import zipfile
import urllib.request
import urllib.error
import math
import statistics
from datetime import datetime, timezone, timedelta

# ============================================================================
# 1. CONFIG
# ============================================================================

START_DATE = "2024-07-01"
END_DATE   = "2026-07-01"

STARTING_CAPITAL = 10_000.0
RISK_PER_TRADE   = 0.0075
MAX_CONCURRENT   = 8
FEE_PER_SIDE     = 0.0005
SLIPPAGE_PER_SIDE = 0.0002
MIN_TRADES_TO_REPORT = 30
PF_TARGET = 1.50
MIN_TRADES_TARGET = 40
MIN_MONTHS_HISTORY = 6

COIN_UNIVERSE = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT",
    "AVAXUSDT","LINKUSDT","DOTUSDT","LTCUSDT","TRXUSDT","ATOMUSDT","UNIUSDT",
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","SUIUSDT","INJUSDT","AAVEUSDT",
    "HBARUSDT","FILUSDT","ETCUSDT","ICPUSDT","RENDERUSDT","SEIUSDT","TIAUSDT",
    "STXUSDT","RUNEUSDT","ALGOUSDT","GRTUSDT","MKRUSDT","LDOUSDT","IMXUSDT",
    "EGLDUSDT","FTMUSDT","SANDUSDT","MANAUSDT","AXSUSDT","THETAUSDT","XTZUSDT",
    "FLOWUSDT","CHZUSDT","GALAUSDT","KAVAUSDT","MINAUSDT","ROSEUSDT","ZILUSDT",
    "ENJUSDT","1000BONKUSDT","1000PEPEUSDT","1000SHIBUSDT","1000FLOKIUSDT",
    "WIFUSDT","ORDIUSDT","JUPUSDT","PYTHUSDT","WLDUSDT","ARKMUSDT","BOMEUSDT",
    "NEIROUSDT","TRUMPUSDT","POLUSDT","GMTUSDT","APEUSDT","DYDXUSDT","CRVUSDT",
    "COMPUSDT","SNXUSDT","1INCHUSDT","YFIUSDT","SUSHIUSDT","BALUSDT","ZRXUSDT",
    "KNCUSDT","ENSUSDT","MASKUSDT","CFXUSDT","ARUSDT","LPTUSDT","BLURUSDT",
    "NOTUSDT","TONUSDT","RAYUSDT","JTOUSDT","PENDLEUSDT","ONDOUSDT","ENAUSDT",
    "ETHFIUSDT","TAOUSDT",
]

STRATEGIES = ["S1_BB_RSI_MeanRev", "S2_SuperTrend_ADX", "S3_Donchian_Vol", "S4_EMA_Pullback"]

TIMEFRAME_FOR_STRATEGY = {
    "S1_BB_RSI_MeanRev": "30m",
    "S2_SuperTrend_ADX": "1h",
    "S3_Donchian_Vol":   "30m",
    "S4_EMA_Pullback":   "1h",
}

# ============================================================================
# 2. DATA FETCH  (data.binance.vision futures/um archives)
# ============================================================================

BASE_MONTHLY = "https://data.binance.vision/data/futures/um/monthly/klines/{sym}/{iv}/{sym}-{iv}-{y:04d}-{m:02d}.zip"
BASE_DAILY   = "https://data.binance.vision/data/futures/um/daily/klines/{sym}/{iv}/{sym}-{iv}-{y:04d}-{m:02d}-{d:02d}.zip"

def _iter_months(start_date, end_date):
    y, m = int(start_date[:4]), int(start_date[5:7])
    ey, em = int(end_date[:4]), int(end_date[5:7])
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1

def _fetch_zip_csv(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except urllib.error.URLError:
        raise
    rows = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.reader(text)
            for row in reader:
                if not row or row[0] in ("open_time", ""):
                    continue
                rows.append(row)
    return rows

def fetch_symbol_interval(symbol, interval, start_date, end_date, log):
    candles = []
    months_with_data = set()
    now = datetime.now(timezone.utc)
    last_complete_month = (now.year, now.month - 1) if now.day >= 8 else (now.year, now.month - 2)

    for y, m in _iter_months(start_date, end_date):
        if (y, m) > last_complete_month:
            days_in_month = 31
            got_any = False
            d = 1
            while d <= days_in_month:
                try:
                    url = BASE_DAILY.format(sym=symbol, iv=interval, y=y, m=m, d=d)
                    rows = _fetch_zip_csv(url)
                except Exception as e:
                    log.setdefault("errors", []).append(f"{symbol} {interval} {y}-{m:02d}-{d:02d}: {e}")
                    d += 1
                    continue
                if rows:
                    got_any = True
                    candles.extend(rows)
                d += 1
            if got_any:
                months_with_data.add((y, m))
            continue

        try:
            url = BASE_MONTHLY.format(sym=symbol, iv=interval, y=y, m=m)
            rows = _fetch_zip_csv(url)
        except Exception as e:
            log.setdefault("errors", []).append(f"{symbol} {interval} {y}-{m:02d}: {e}")
            continue
        if rows is None:
            continue
        candles.extend(rows)
        months_with_data.add((y, m))

    if not candles:
        return [], 0

    out = []
    for row in candles:
        ot = int(row[0])
        if ot > 10**14:
            ot //= 1000
        out.append({
            "t": ot, "o": float(row[1]), "h": float(row[2]),
            "l": float(row[3]), "c": float(row[4]), "v": float(row[5]),
        })
    out.sort(key=lambda x: x["t"])
    dedup = []
    seen = set()
    for c in out:
        if c["t"] in seen:
            continue
        seen.add(c["t"])
        dedup.append(c)
    return dedup, len(months_with_data)

def fetch_all(symbols, intervals, start_date, end_date):
    data = {}
    months_ok = {}
    log = {}
    total = len(symbols)
    fail_count = 0
    for sym in symbols:
        data[sym] = {}
        best_months = 0
        for iv in intervals:
            candles, n_months = fetch_symbol_interval(sym, iv, start_date, end_date, log)
            data[sym][iv] = candles
            best_months = max(best_months, n_months)
        months_ok[sym] = best_months
        if best_months == 0:
            fail_count += 1

    if total > 0 and fail_count == total:
        raise RuntimeError(
            "ABORT: 100% of symbols returned zero data from data.binance.vision. "
            "This means the data source itself is unreachable/blocked from this "
            "runner -- NOT that strategies produced zero trades. Check network "
            "access before treating results as valid (handoff sec 1)."
        )
    return data, months_ok, log

# ============================================================================
# 3. INDICATORS (stdlib only)
# ============================================================================

def sma(vals, period):
    out = [None]*len(vals)
    s = 0.0
    for i, v in enumerate(vals):
        s += v
        if i >= period:
            s -= vals[i-period]
        if i >= period-1:
            out[i] = s/period
    return out

def ema(vals, period):
    out = [None]*len(vals)
    k = 2/(period+1)
    for i, v in enumerate(vals):
        if i == period-1:
            out[i] = sum(vals[:period])/period
        elif i >= period:
            out[i] = v*k + out[i-1]*(1-k)
    return out

def rsi(closes, period=14):
    out = [None]*len(closes)
    gains, losses = [0.0]*len(closes), [0.0]*len(closes)
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains[i] = max(d, 0.0)
        losses[i] = max(-d, 0.0)
    avg_g = avg_l = None
    for i in range(1, len(closes)):
        if i == period:
            avg_g = sum(gains[1:period+1])/period
            avg_l = sum(losses[1:period+1])/period
        elif i > period:
            avg_g = (avg_g*(period-1)+gains[i])/period
            avg_l = (avg_l*(period-1)+losses[i])/period
        if i >= period and avg_l is not None:
            rs = avg_g/avg_l if avg_l != 0 else float("inf")
            out[i] = 100 - (100/(1+rs)) if avg_l != 0 else 100.0
    return out

def true_range(highs, lows, closes):
    tr = [None]*len(closes)
    for i in range(len(closes)):
        if i == 0:
            tr[i] = highs[i]-lows[i]
        else:
            tr[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
    return tr

def atr(highs, lows, closes, period=14):
    tr = true_range(highs, lows, closes)
    out = [None]*len(closes)
    avg = None
    for i in range(len(closes)):
        if i == period:
            avg = sum(tr[1:period+1])/period
            out[i] = avg
        elif i > period:
            avg = (avg*(period-1)+tr[i])/period
            out[i] = avg
    return out

def adx(highs, lows, closes, period=14):
    n = len(closes)
    plus_dm = [0.0]*n
    minus_dm = [0.0]*n
    for i in range(1, n):
        up = highs[i]-highs[i-1]
        dn = lows[i-1]-lows[i]
        plus_dm[i] = up if (up > dn and up > 0) else 0.0
        minus_dm[i] = dn if (dn > up and dn > 0) else 0.0
    tr = true_range(highs, lows, closes)

    def wilder_smooth(vals):
        out = [None]*n
        avg = None
        for i in range(n):
            if i == period:
                avg = sum(vals[1:period+1])
                out[i] = avg
            elif i > period:
                avg = avg - (avg/period) + vals[i]
                out[i] = avg
        return out

    str_ = wilder_smooth(tr)
    spdm = wilder_smooth(plus_dm)
    smdm = wilder_smooth(minus_dm)

    plus_di = [None]*n
    minus_di = [None]*n
    dx = [None]*n
    for i in range(n):
        if str_[i] and str_[i] != 0:
            plus_di[i] = 100*spdm[i]/str_[i]
            minus_di[i] = 100*smdm[i]/str_[i]
            denom = plus_di[i]+minus_di[i]
            dx[i] = 100*abs(plus_di[i]-minus_di[i])/denom if denom != 0 else 0.0

    adx_out = [None]*n
    avg_dx = None
    start = period*2
    for i in range(n):
        if i == start:
            vals = [x for x in dx[period+1:start+1] if x is not None]
            if vals:
                avg_dx = sum(vals)/len(vals)
                adx_out[i] = avg_dx
        elif i > start and avg_dx is not None and dx[i] is not None:
            avg_dx = (avg_dx*(period-1)+dx[i])/period
            adx_out[i] = avg_dx
    return adx_out, plus_di, minus_di

def bollinger(closes, period=20, mult=2.0):
    mid = sma(closes, period)
    upper = [None]*len(closes)
    lower = [None]*len(closes)
    for i in range(len(closes)):
        if mid[i] is not None:
            window = closes[i-period+1:i+1]
            sd = statistics.pstdev(window)
            upper[i] = mid[i] + mult*sd
            lower[i] = mid[i] - mult*sd
    return upper, mid, lower

def supertrend(highs, lows, closes, atr_period=10, mult=3.0):
    atr_v = atr(highs, lows, closes, atr_period)
    n = len(closes)
    st = [None]*n
    trend = [None]*n
    up_band = [None]*n
    dn_band = [None]*n
    for i in range(n):
        if atr_v[i] is None:
            continue
        hl2 = (highs[i]+lows[i])/2
        basic_up = hl2 + mult*atr_v[i]
        basic_dn = hl2 - mult*atr_v[i]
        prev_i = i-1
        if (prev_i < 0) or (up_band[prev_i] is None):
            fu, fd = basic_up, basic_dn
        else:
            fu = basic_up if (basic_up < up_band[prev_i] or closes[prev_i] > up_band[prev_i]) else up_band[prev_i]
            fd = basic_dn if (basic_dn > dn_band[prev_i] or closes[prev_i] < dn_band[prev_i]) else dn_band[prev_i]
        up_band[i], dn_band[i] = fu, fd

        if (prev_i < 0) or (trend[prev_i] is None):
            trend[i] = 1 if closes[i] > fu else -1
        else:
            if trend[prev_i] == 1:
                trend[i] = -1 if closes[i] < fd else 1
            else:
                trend[i] = 1 if closes[i] > fu else -1
        st[i] = dn_band[i] if trend[i] == 1 else up_band[i]
    return st, trend

def donchian(highs, lows, period=20):
    n = len(highs)
    hh = [None]*n
    ll = [None]*n
    for i in range(n):
        if i >= period:
            window_h = highs[i-period:i]
            window_l = lows[i-period:i]
            hh[i] = max(window_h)
            ll[i] = min(window_l)
    return hh, ll

# ============================================================================
# 4. STRATEGY SIGNAL GENERATORS
# ============================================================================

def gen_signals_S1(c):
    closes = [x["c"] for x in c]; highs=[x["h"] for x in c]; lows=[x["l"] for x in c]
    n = len(c)
    up, mid, lo = bollinger(closes, 20, 2.0)
    r = rsi(closes, 14)
    a = atr(highs, lows, closes, 14)
    rej = {"warmup_none":0, "no_trigger":0, "rsi_filter":0}
    cands = []
    for i in range(n):
        if up[i] is None or r[i] is None or a[i] is None:
            rej["warmup_none"] += 1
            continue
        long_trig = closes[i] <= lo[i]
        short_trig = closes[i] >= up[i]
        if not long_trig and not short_trig:
            rej["no_trigger"] += 1
            continue
        if long_trig:
            if r[i] > 30:
                rej["rsi_filter"] += 1
                continue
            stop = closes[i] - 1.8*a[i]
            tp = mid[i]
            cands.append({"idx": i, "direction":"long", "stop":stop, "tp":tp, "exit_rule":"tp_sl_or_opposite"})
        elif short_trig:
            if r[i] < 70:
                rej["rsi_filter"] += 1
                continue
            stop = closes[i] + 1.8*a[i]
            tp = mid[i]
            cands.append({"idx": i, "direction":"short", "stop":stop, "tp":tp, "exit_rule":"tp_sl_or_opposite"})
    return cands, rej, n

def gen_signals_S2(c):
    closes=[x["c"] for x in c]; highs=[x["h"] for x in c]; lows=[x["l"] for x in c]
    n = len(c)
    st, trend = supertrend(highs, lows, closes, 10, 3.0)
    a_val, pdi, mdi = adx(highs, lows, closes, 14)
    atr_v = atr(highs, lows, closes, 14)
    rej = {"warmup_none":0, "no_trigger":0, "adx_level":0, "adx_rising":0}
    cands = []
    rej["warmup_none"] += min(2, n)  # bars 0,1 can't be evaluated (need i-2 lookback)
    for i in range(2, n):
        if trend[i] is None or trend[i-1] is None or a_val[i] is None or a_val[i-2] is None or atr_v[i] is None:
            rej["warmup_none"] += 1
            continue
        flip_long = trend[i] == 1 and trend[i-1] == -1
        flip_short = trend[i] == -1 and trend[i-1] == 1
        if not flip_long and not flip_short:
            rej["no_trigger"] += 1
            continue
        if a_val[i] < 23:
            rej["adx_level"] += 1
            continue
        if not (a_val[i] > a_val[i-2]):
            rej["adx_rising"] += 1
            continue
        direction = "long" if flip_long else "short"
        stop = closes[i] - 2.0*atr_v[i] if direction=="long" else closes[i] + 2.0*atr_v[i]
        cands.append({"idx": i, "direction":direction, "stop":stop, "tp":None, "exit_rule":"st_flip_or_stop"})
    return cands, rej, n

def gen_signals_S3(c):
    closes=[x["c"] for x in c]; highs=[x["h"] for x in c]; lows=[x["l"] for x in c]; vols=[x["v"] for x in c]
    n = len(c)
    hh, ll = donchian(highs, lows, 20)
    vol_sma = sma(vols, 20)
    a = atr(highs, lows, closes, 14)
    a_full_sma = [None]*n
    valid_idx = [i for i,v in enumerate(a) if v is not None]
    tmp = sma([a[i] for i in valid_idx], 50)
    for j, i in enumerate(valid_idx):
        a_full_sma[i] = tmp[j]

    rej = {"warmup_none":0, "no_trigger":0, "volume_filter":0, "volatility_filter":0}
    cands = []
    for i in range(n):
        if hh[i] is None or vol_sma[i] is None or a[i] is None or a_full_sma[i] is None:
            rej["warmup_none"] += 1
            continue
        long_trig = closes[i] > hh[i]
        short_trig = closes[i] < ll[i]
        if not long_trig and not short_trig:
            rej["no_trigger"] += 1
            continue
        if vols[i] <= 1.4*vol_sma[i]:
            rej["volume_filter"] += 1
            continue
        if not (a[i] > a_full_sma[i]):
            rej["volatility_filter"] += 1
            continue
        direction = "long" if long_trig else "short"
        tp = closes[i] + 2.5*a[i] if direction=="long" else closes[i] - 2.5*a[i]
        stop = closes[i] - 1.6*a[i] if direction=="long" else closes[i] + 1.6*a[i]
        cands.append({"idx": i, "direction":direction, "stop":stop, "tp":tp, "exit_rule":"tp_sl"})
    return cands, rej, n

def gen_signals_S4(c):
    closes=[x["c"] for x in c]; highs=[x["h"] for x in c]; lows=[x["l"] for x in c]
    n = len(c)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    a_val, pdi, mdi = adx(highs, lows, closes, 14)
    atr_v = atr(highs, lows, closes, 14)
    rej = {"warmup_none":0, "no_trend":0, "adx_filter":0, "no_pullback_trigger":0}
    cands = []
    rej["warmup_none"] += min(1, n)  # bar 0 can't be evaluated (need i-1 lookback for EMA50 slope)
    for i in range(1, n):
        if e21[i] is None or e50[i] is None or e50[i-1] is None or a_val[i] is None or atr_v[i] is None:
            rej["warmup_none"] += 1
            continue
        uptrend = e21[i] > e50[i] and (e50[i]-e50[i-1]) > 0
        downtrend = e21[i] < e50[i] and (e50[i]-e50[i-1]) < 0
        if not uptrend and not downtrend:
            rej["no_trend"] += 1
            continue
        if a_val[i] < 22:
            rej["adx_filter"] += 1
            continue
        near_ema = abs(closes[i]-e21[i])/e21[i] <= 0.002 or (lows[i] <= e21[i] <= highs[i])
        if uptrend:
            trig = near_ema and closes[i] > e21[i]
        else:
            trig = near_ema and closes[i] < e21[i]
        if not trig:
            rej["no_pullback_trigger"] += 1
            continue
        direction = "long" if uptrend else "short"
        tp = closes[i] + 2.8*atr_v[i] if direction=="long" else closes[i] - 2.8*atr_v[i]
        stop = closes[i] - 1.7*atr_v[i] if direction=="long" else closes[i] + 1.7*atr_v[i]
        cands.append({"idx": i, "direction":direction, "stop":stop, "tp":tp, "exit_rule":"tp_sl_or_ema50_break"})
    return cands, rej, n

SIGNAL_GENERATORS = {
    "S1_BB_RSI_MeanRev": gen_signals_S1,
    "S2_SuperTrend_ADX": gen_signals_S2,
    "S3_Donchian_Vol":   gen_signals_S3,
    "S4_EMA_Pullback":   gen_signals_S4,
}
COOLDOWN_BARS = {
    "S1_BB_RSI_MeanRev": 2,
    "S2_SuperTrend_ADX": 2,
    "S3_Donchian_Vol":   3,
    "S4_EMA_Pullback":   2,
}

# ============================================================================
# 5. GLOBAL PORTFOLIO ENGINE
# ============================================================================

def run_strategy_portfolio(strategy_name, symbol_candles):
    gen = SIGNAL_GENERATORS[strategy_name]
    cooldown_len = COOLDOWN_BARS[strategy_name]

    per_symbol = {}
    for sym, candles in symbol_candles.items():
        if len(candles) < 100:
            continue
        cands, rej, total = gen(candles)
        cand_by_idx = {c["idx"]: c for c in cands}
        entry = {"candles": candles, "cand_by_idx": cand_by_idx,
                 "rejections": rej, "total_bars": total}
        # Precompute exit-check series ONCE per symbol (was previously recomputed
        # from scratch on every single bar inside the event loop -- O(n^2) per
        # symbol, very slow across 90 coins x 2yr x hourly data).
        if strategy_name == "S2_SuperTrend_ADX":
            highs = [x["h"] for x in candles]; lows = [x["l"] for x in candles]; closes = [x["c"] for x in candles]
            _, trend_series = supertrend(highs, lows, closes, 10, 3.0)
            entry["trend_series"] = trend_series
        if strategy_name == "S4_EMA_Pullback":
            closes = [x["c"] for x in candles]
            entry["ema50_series"] = ema(closes, 50)
        per_symbol[sym] = entry

    events = []
    for sym, d in per_symbol.items():
        for i, c in enumerate(d["candles"]):
            events.append((c["t"], sym, i))
    events.sort(key=lambda x: x[0])

    equity = STARTING_CAPITAL
    open_positions = {}
    cooldown_until = {}
    trades = []
    equity_curve = []

    for ts, sym, i in events:
        d = per_symbol[sym]
        candle = d["candles"][i]
        pos = open_positions.get(sym)

        if pos is not None:
            exited = False
            exit_price = None
            reason = None
            if pos["direction"] == "long":
                if pos.get("stop") is not None and candle["l"] <= pos["stop"]:
                    exit_price = pos["stop"]; reason = "stop"; exited = True
                elif pos.get("tp") is not None and candle["h"] >= pos["tp"]:
                    exit_price = pos["tp"]; reason = "tp"; exited = True
            else:
                if pos.get("stop") is not None and candle["h"] >= pos["stop"]:
                    exit_price = pos["stop"]; reason = "stop"; exited = True
                elif pos.get("tp") is not None and candle["l"] <= pos["tp"]:
                    exit_price = pos["tp"]; reason = "tp"; exited = True

            if not exited and pos["exit_rule"] == "st_flip_or_stop":
                cur_trend = d["trend_series"][i]
                if cur_trend is not None:
                    if (pos["direction"]=="long" and cur_trend==-1) or (pos["direction"]=="short" and cur_trend==1):
                        exit_price = candle["c"]; reason = "st_flip"; exited = True

            if not exited and pos["exit_rule"] == "tp_sl_or_ema50_break":
                e50_val = d["ema50_series"][i]
                if e50_val is not None:
                    if pos["direction"]=="long" and candle["c"] < e50_val:
                        exit_price = candle["c"]; reason = "ema50_break"; exited = True
                    elif pos["direction"]=="short" and candle["c"] > e50_val:
                        exit_price = candle["c"]; reason = "ema50_break"; exited = True

            if not exited and pos["exit_rule"] == "tp_sl_or_opposite":
                cand = d["cand_by_idx"].get(i)
                if cand and cand["direction"] != pos["direction"]:
                    exit_price = candle["c"]; reason = "opposite_signal"; exited = True

            if exited:
                slip = exit_price * SLIPPAGE_PER_SIDE
                exit_price_adj = exit_price - slip if pos["direction"]=="long" else exit_price + slip
                gross = (exit_price_adj - pos["entry_price"]) * pos["qty"] if pos["direction"]=="long" \
                        else (pos["entry_price"] - exit_price_adj) * pos["qty"]
                exit_fee = abs(exit_price_adj * pos["qty"]) * FEE_PER_SIDE
                pnl = gross - exit_fee - pos["entry_fee"]
                equity += pnl
                trades.append({
                    "strategy": strategy_name, "symbol": sym, "direction": pos["direction"],
                    "entry_ts": pos["entry_ts"], "exit_ts": ts,
                    "entry_price": pos["entry_price"], "exit_price": exit_price_adj,
                    "qty": pos["qty"], "pnl": pnl, "reason": reason,
                    "r_multiple": (pnl / pos["risk_amount"]) if pos["risk_amount"] else None,
                    "duration_bars": i - pos["entry_idx"],
                })
                del open_positions[sym]
                cooldown_until[sym] = i + cooldown_len
                equity_curve.append((ts, equity))

        if sym not in open_positions:
            if i <= cooldown_until.get(sym, -1):
                pass
            elif len(open_positions) >= MAX_CONCURRENT:
                pass
            else:
                cand = d["cand_by_idx"].get(i)
                if cand:
                    entry_price_raw = candle["c"]
                    slip = entry_price_raw * SLIPPAGE_PER_SIDE
                    entry_price = entry_price_raw + slip if cand["direction"]=="long" else entry_price_raw - slip
                    stop = cand["stop"]
                    stop_distance = abs(entry_price - stop)
                    if stop_distance <= 0:
                        pass
                    else:
                        risk_amount = equity * RISK_PER_TRADE
                        qty = risk_amount / stop_distance
                        entry_fee = abs(entry_price * qty) * FEE_PER_SIDE
                        open_positions[sym] = {
                            "direction": cand["direction"], "entry_price": entry_price,
                            "stop": stop, "tp": cand.get("tp"), "qty": qty,
                            "entry_ts": ts, "entry_idx": i, "risk_amount": risk_amount,
                            "entry_fee": entry_fee, "exit_rule": cand["exit_rule"],
                        }

    per_symbol_rejections = {sym: d["rejections"] for sym, d in per_symbol.items()}
    total_bars = {sym: d["total_bars"] for sym, d in per_symbol.items()}
    return trades, equity_curve, per_symbol_rejections, total_bars

# ============================================================================
# 6. STATS
# ============================================================================

def compute_stats(trades, starting_capital=STARTING_CAPITAL):
    if not trades:
        return None
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss = -sum(t["pnl"] for t in losses)
    pf = (gross_profit/gross_loss) if gross_loss > 0 else float("inf")
    win_rate = len(wins)/len(trades)*100
    net_pnl = sum(t["pnl"] for t in trades)
    avg_win = gross_profit/len(wins) if wins else 0
    avg_loss = -gross_loss/len(losses) if losses else 0
    avg_r = statistics.mean([t["r_multiple"] for t in trades if t["r_multiple"] is not None]) \
            if any(t["r_multiple"] is not None for t in trades) else None
    avg_dur = statistics.mean([t["duration_bars"] for t in trades])
    expectancy = net_pnl/len(trades)

    eq = starting_capital
    curve = [eq]
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        eq += t["pnl"]
        curve.append(eq)
    peak = curve[0]
    max_dd = 0.0
    for v in curve:
        peak = max(peak, v)
        dd = (peak - v)/peak*100 if peak > 0 else 0
        max_dd = max(max_dd, dd)

    rets = [t["pnl"]/starting_capital for t in sorted(trades, key=lambda x: x["exit_ts"])]
    mean_r = statistics.mean(rets)
    sd_r = statistics.pstdev(rets) if len(rets) > 1 else 0
    sharpe = (mean_r/sd_r*math.sqrt(len(rets))) if sd_r > 0 else None
    downside = [r for r in rets if r < 0]
    dsd = statistics.pstdev(downside) if len(downside) > 1 else 0
    sortino = (mean_r/dsd*math.sqrt(len(rets))) if dsd > 0 else None

    longs = [t for t in trades if t["direction"]=="long"]
    shorts = [t for t in trades if t["direction"]=="short"]
    def wr(lst): return (len([t for t in lst if t["pnl"]>0])/len(lst)*100) if lst else None

    best_win_streak, best_loss_streak, cur, cur_type = 0,0,0,None
    for t in sorted(trades, key=lambda x: x["exit_ts"]):
        is_win = t["pnl"] > 0
        if cur_type == is_win:
            cur += 1
        else:
            cur = 1
            cur_type = is_win
        if is_win:
            best_win_streak = max(best_win_streak, cur)
        else:
            best_loss_streak = max(best_loss_streak, cur)

    return {
        "total_trades": len(trades), "win_rate": round(win_rate,2),
        "profit_factor": round(pf,3) if pf != float("inf") else None,
        "net_pnl": round(net_pnl,2), "max_drawdown_pct": round(max_dd,2),
        "sharpe": round(sharpe,3) if sharpe is not None else None,
        "sortino": round(sortino,3) if sortino is not None else None,
        "avg_win": round(avg_win,2), "avg_loss": round(avg_loss,2),
        "expectancy": round(expectancy,2), "avg_r": round(avg_r,3) if avg_r is not None else None,
        "avg_duration_bars": round(avg_dur,1),
        "long_count": len(longs), "long_win_rate": round(wr(longs),2) if wr(longs) is not None else None,
        "short_count": len(shorts), "short_win_rate": round(wr(shorts),2) if wr(shorts) is not None else None,
        "best_win_streak": best_win_streak, "best_loss_streak": best_loss_streak,
    }

def monthly_pnl(trades):
    buckets = {}
    for t in trades:
        dt = datetime.fromtimestamp(t["exit_ts"]/1000, tz=timezone.utc)
        key = f"{dt.year:04d}-{dt.month:02d}"
        buckets[key] = buckets.get(key,0)+t["pnl"]
    return dict(sorted(buckets.items()))

# ============================================================================
# 7. MAIN
# ============================================================================

def main():
    log = {}
    all_report = {"meta": {}, "variants": []}
    summary_lines = []

    summary_lines.append("="*80)
    summary_lines.append("BINANCE FUTURES MULTI-STRATEGY BACKTEST -- V6 (Grok spec, 2026-07-28)")
    summary_lines.append(f"Run timestamp: {datetime.now(timezone.utc).isoformat()}")
    summary_lines.append(f"Period requested: {START_DATE} -> {END_DATE}")
    summary_lines.append(f"Coins in universe: {len(COIN_UNIVERSE)}")
    summary_lines.append("="*80)

    all_report["meta"] = {
        "starting_capital": STARTING_CAPITAL, "risk_per_trade": RISK_PER_TRADE,
        "max_concurrent": MAX_CONCURRENT, "fee_per_side": FEE_PER_SIDE,
        "slippage_per_side": SLIPPAGE_PER_SIDE, "period": [START_DATE, END_DATE],
        "coin_universe": COIN_UNIVERSE, "pf_target": PF_TARGET,
        "min_trades_target": MIN_TRADES_TARGET,
        "note": "Primary timeframe only per strategy this run (30m for S1/S3, 1h for S2/S4).",
    }

    intervals_needed = sorted(set(TIMEFRAME_FOR_STRATEGY.values()))
    data_by_interval = {}
    months_by_interval = {}
    for iv in intervals_needed:
        summary_lines.append(f"\nFetching {iv} data for {len(COIN_UNIVERSE)} symbols...")
        d, months_ok, fetch_log = fetch_all(COIN_UNIVERSE, [iv], START_DATE, END_DATE)
        data_by_interval[iv] = {sym: d[sym][iv] for sym in d}
        months_by_interval[iv] = months_ok
        log[iv] = fetch_log

    all_pass_table = []
    best_overall = []

    for strat in STRATEGIES:
        iv = TIMEFRAME_FOR_STRATEGY[strat]
        symbol_candles = {}
        excluded_short_history = []
        for sym in COIN_UNIVERSE:
            months = months_by_interval[iv].get(sym, 0)
            if months < MIN_MONTHS_HISTORY:
                excluded_short_history.append(sym)
                continue
            candles = data_by_interval[iv].get(sym, [])
            if candles:
                symbol_candles[sym] = candles

        summary_lines.append(f"\n{'-'*80}\nSTRATEGY: {strat}  (timeframe {iv})")
        summary_lines.append(f"Symbols with >= {MIN_MONTHS_HISTORY} months history: {len(symbol_candles)} "
                              f"(excluded {len(excluded_short_history)}: {', '.join(excluded_short_history[:10])}"
                              f"{'...' if len(excluded_short_history)>10 else ''})")

        trades, equity_curve, rejections, total_bars = run_strategy_portfolio(strat, symbol_candles)

        agg = compute_stats(trades)
        by_symbol = {}
        for sym in symbol_candles:
            sym_trades = [t for t in trades if t["symbol"]==sym]
            if len(sym_trades) >= MIN_TRADES_TO_REPORT:
                by_symbol[sym] = compute_stats(sym_trades)

        filter_totals = {}
        grand_total_bars = 0
        for sym, rej in rejections.items():
            grand_total_bars += total_bars.get(sym,0)
            for k,v in rej.items():
                filter_totals[k] = filter_totals.get(k,0)+v
        n_candidates_total = sum(total_bars.get(sym,0) - sum(rej.values()) for sym, rej in rejections.items())
        summary_lines.append(f"Total bars scanned: {grand_total_bars} | "
                              f"Rejected (by filter): {sum(filter_totals.values())} | "
                              f"Passed all filters (candidate signals): {n_candidates_total} | "
                              f"Sum check: {'OK' if sum(filter_totals.values())+n_candidates_total==grand_total_bars else 'MISMATCH'}")
        summary_lines.append(f"Filter rejection breakdown: {json.dumps(filter_totals)}")

        if agg:
            summary_lines.append(f"AGGREGATE: trades={agg['total_trades']} WR={agg['win_rate']}% "
                                  f"PF={agg['profit_factor']} netPnL=${agg['net_pnl']} maxDD={agg['max_drawdown_pct']}% "
                                  f"Sharpe={agg['sharpe']} Sortino={agg['sortino']}")
        else:
            summary_lines.append("AGGREGATE: 0 trades produced by this strategy across the full universe.")

        summary_lines.append("PER-COIN (>= %d trades), sorted by PF:" % MIN_TRADES_TO_REPORT)
        sorted_syms = sorted(by_symbol.items(), key=lambda kv: (kv[1]["profit_factor"] or 0), reverse=True)
        for sym, s in sorted_syms:
            passed = (s["profit_factor"] or 0) >= PF_TARGET and s["total_trades"] >= MIN_TRADES_TARGET
            summary_lines.append(f"  {sym:16s} trades={s['total_trades']:4d} WR={s['win_rate']:6.2f}% "
                                  f"PF={s['profit_factor']:.3f} netPnL=${s['net_pnl']:9.2f} "
                                  f"maxDD={s['max_drawdown_pct']:6.2f}% avgR={s['avg_r']} "
                                  f"{'  <-- PASSES' if passed else ''}")
            if passed:
                all_pass_table.append({"strategy":strat,"symbol":sym, **s})
            best_overall.append({"strategy":strat,"symbol":sym, **s})

        mpnl = monthly_pnl(trades) if trades else {}
        all_report["variants"].append({
            "strategy": strat, "timeframe": iv,
            "aggregate": agg, "per_coin": by_symbol,
            "filter_stats": filter_totals, "total_bars_scanned": grand_total_bars,
            "monthly_pnl": mpnl,
            "excluded_short_history": excluded_short_history,
            "recommendation": "usable" if (agg and (agg["profit_factor"] or 0) >= PF_TARGET and agg["total_trades"] >= MIN_TRADES_TARGET) else "not usable in aggregate -- check per-coin table",
            "trades": trades,
        })

    summary_lines.append(f"\n{'='*80}\nTABLE A -- All Strategy+Coin combos with PF >= {PF_TARGET} and >= {MIN_TRADES_TARGET} trades")
    summary_lines.append("="*80)
    table_a = sorted(all_pass_table, key=lambda x: x["profit_factor"] or 0, reverse=True)
    for row in table_a:
        summary_lines.append(f"  {row['strategy']:22s} {row['symbol']:16s} PF={row['profit_factor']:.3f} "
                              f"trades={row['total_trades']} WR={row['win_rate']}% netPnL=${row['net_pnl']}")
    if not table_a:
        summary_lines.append("  (none passed)")

    summary_lines.append(f"\n{'='*80}\nTABLE B -- Best 15-20 combos overall (regardless of PF target)")
    summary_lines.append("="*80)
    table_b = sorted(best_overall, key=lambda x: x["profit_factor"] or 0, reverse=True)[:20]
    for row in table_b:
        summary_lines.append(f"  {row['strategy']:22s} {row['symbol']:16s} PF={row['profit_factor']:.3f} "
                              f"trades={row['total_trades']} WR={row['win_rate']}% netPnL=${row['net_pnl']}")

    summary_lines.append(f"\nCoins tested: {len(COIN_UNIVERSE)}")
    summary_lines.append(f"Combinations passing PF >= {PF_TARGET} (>= {MIN_TRADES_TARGET} trades): {len(table_a)}")
    strat_pass_counts = {}
    for row in table_a:
        strat_pass_counts[row["strategy"]] = strat_pass_counts.get(row["strategy"],0)+1
    if strat_pass_counts:
        most_robust = max(strat_pass_counts.items(), key=lambda kv: kv[1])
        summary_lines.append(f"Most robust strategy (most passing combos): {most_robust[0]} ({most_robust[1]} combos)")
    else:
        summary_lines.append("No strategy produced a passing combination this round.")

    all_report["table_a_passing"] = table_a
    all_report["table_b_best_overall"] = table_b

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(summary_lines))
    with open("backtest_report.json", "w") as f:
        json.dump(all_report, f, indent=2, default=str)

    print("\n".join(summary_lines))

if __name__ == "__main__":
    main()
