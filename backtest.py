#!/usr/bin/env python3
"""
BACKTEST V5 — Pullback Entry, Multi-Coin Universe, 4 Config Comparison
Stdlib only. Runs on GitHub Actions (ubuntu-latest, Python 3.11).
Data source: data-api.binance.vision (spot klines mirror — fapi.binance.com
is geo-blocked / 451 on GitHub-hosted runners, this is the known workaround).
"""

import json
import math
import statistics
import time
import datetime
import urllib.request
import urllib.error
import concurrent.futures
from collections import defaultdict

# ============================================================================
# 1. COIN UNIVERSE
# ============================================================================
# The universe is fetched LIVE from Binance's exchangeInfo at runtime (every
# active USDT spot pair), not hand-typed — this is what actually gets us to
# 400+ coins instead of the ~140 unique symbols in Kimi's (heavily duplicated)
# category lists. CATEGORIES below is kept only to LABEL known coins in the
# report; anything the live fetch returns that isn't in CATEGORIES is tagged
# "UNCATEGORIZED" rather than dropped.
# Symbols use plain spot naming (BONKUSDT / PEPEUSDT / SHIBUSDT / FLOKIUSDT,
# not 1000-prefixed — the 1000-prefixed futures names aren't on this data
# source). MATIC is POLUSDT (2024 rebrand). RNDR was renamed RENDER.
# Leveraged tokens (UP/DOWN/BULL/BEAR suffixes) are excluded — they're not
# representative of a real futures scalping strategy.
# Any symbol that 400s or has <6 months of history is skipped automatically.

EXCHANGE_INFO_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"
LEVERAGED_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
MAX_UNIVERSE = 450  # safety cap so a single run stays within Actions timeout

CATEGORIES = {
    "TOP": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
            "DOGEUSDT", "DOTUSDT", "LINKUSDT", "TRXUSDT", "AVAXUSDT", "SHIBUSDT",
            "LTCUSDT", "BCHUSDT", "NEARUSDT", "UNIUSDT", "ATOMUSDT", "ETCUSDT",
            "XLMUSDT", "FILUSDT", "VETUSDT", "ICPUSDT", "ARBUSDT", "OPUSDT",
            "APTUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "STRKUSDT", "INJUSDT",
            "HBARUSDT", "TONUSDT", "WLDUSDT", "RENDERUSDT", "FETUSDT"],
    "LAYER1": ["ALGOUSDT", "POLUSDT", "FTMUSDT", "KASUSDT"],
    "LAYER2": ["MANTAUSDT", "IMXUSDT", "METISUSDT", "ZKUSDT", "SCROLLUSDT"],
    "MEME": ["PEPEUSDT", "BONKUSDT", "FLOKIUSDT", "WIFUSDT", "BOMEUSDT",
             "NEIROUSDT", "TRUMPUSDT", "TURBOUSDT", "POPCATUSDT", "BRETTUSDT",
             "MEWUSDT", "MOGUSDT"],
    "DEFI": ["AAVEUSDT", "MKRUSDT", "LDOUSDT", "SNXUSDT", "COMPUSDT", "YFIUSDT",
             "CRVUSDT", "1INCHUSDT", "SUSHIUSDT", "DYDXUSDT", "PENDLEUSDT",
             "JUPUSDT", "RAYUSDT", "CAKEUSDT", "GMXUSDT", "GRTUSDT", "RDNTUSDT",
             "RUNEUSDT"],
    "AI_DATA": ["AGIXUSDT", "OCEANUSDT", "ARKMUSDT", "NFPUSDT", "AIUSDT", "PHBUSDT"],
    "GAMING": ["SANDUSDT", "MANAUSDT", "AXSUSDT", "GALAUSDT", "ENJUSDT",
               "ILVUSDT", "MAGICUSDT", "YGGUSDT", "BEAMUSDT", "PYRUSDT"],
    "INFRA": ["THETAUSDT", "HNTUSDT", "HOTUSDT", "LPTUSDT", "STORJUSDT"],
    "OTHER": ["FLOWUSDT", "EGLDUSDT", "ZENUSDT", "CELOUSDT", "MINAUSDT",
              "ASTRUSDT", "BICOUSDT", "BLURUSDT", "ALTUSDT", "MTLUSDT",
              "SKLUSDT", "ACHUSDT", "WOOUSDT", "COTIUSDT", "DATAUSDT", "WUSDT",
              "ORDIUSDT", "BANDUSDT", "HOOKUSDT", "STGUSDT", "KSMUSDT",
              "GLMUSDT", "OGNUSDT", "RSRUSDT", "MDTUSDT", "AGLDUSDT",
              "SPELLUSDT", "RAREUSDT", "BATUSDT", "CTSIUSDT", "LRCUSDT",
              "IDEXUSDT", "ONTUSDT", "TFUELUSDT", "PERPUSDT", "CKBUSDT",
              "MAVUSDT", "CYBERUSDT", "ZRXUSDT", "DYMUSDT", "PYTHUSDT",
              "APEUSDT", "QNTUSDT", "CFXUSDT", "IDUSDT", "JTOUSDT", "ARUSDT"],
}

SYMBOL_CATEGORY = {}
FALLBACK_COINS = []
for cat, syms in CATEGORIES.items():
    for s in syms:
        if s not in SYMBOL_CATEGORY:
            SYMBOL_CATEGORY[s] = cat
            FALLBACK_COINS.append(s)


def fetch_live_universe():
    """Pull every active USDT spot symbol from Binance exchangeInfo.
    Falls back to the curated FALLBACK_COINS list if the call fails."""
    try:
        req = urllib.request.Request(EXCHANGE_INFO_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"WARNING: exchangeInfo fetch failed ({e}); falling back to curated list "
              f"({len(FALLBACK_COINS)} coins)")
        return list(FALLBACK_COINS)

    live_symbols = []
    for s in data.get("symbols", []):
        sym = s.get("symbol", "")
        if (s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
                and s.get("isSpotTradingAllowed", True)
                and not sym.endswith(LEVERAGED_SUFFIXES)):
            live_symbols.append(sym)

    if not live_symbols:
        print("WARNING: exchangeInfo returned no usable symbols; falling back to curated list")
        return list(FALLBACK_COINS)

    live_set = set(live_symbols)
    # curated/known coins first (keeps category labels meaningful), then
    # everything else the exchange reports, up to MAX_UNIVERSE
    ordered = [s for s in FALLBACK_COINS if s in live_set]
    ordered += [s for s in live_symbols if s not in SYMBOL_CATEGORY]
    print(f"Live exchangeInfo: {len(live_symbols)} active USDT pairs found "
          f"(using first {min(len(ordered), MAX_UNIVERSE)} of them)")
    return ordered[:MAX_UNIVERSE]

# ============================================================================
# 2. CONFIG
# ============================================================================
BASE_URL = "https://data-api.binance.vision/api/v3/klines"
INTERVAL = "30m"
DAYS_BACK = 365
MIN_CANDLES = 180 * 48          # ~6 months minimum or the coin is skipped
FEE_RATE = 0.0005                # 0.05% taker per side
INITIAL_CAP = 100.0              # starting virtual balance per coin
MARGIN = 1.0
LEVERAGE = 10
NOTIONAL = MARGIN * LEVERAGE     # $10 fixed notional per trade
MAX_CONCURRENT = 3               # portfolio-wide, across all coins
COOLDOWN_MS = 5 * 60 * 1000      # 5 minutes per symbol after close
RISK_CAP_PCT = 0.02
MAX_HOLD_BARS = 500              # safety cap (~10 days on 30m) before force-close
FETCH_WORKERS = 10               # parallel fetch threads (network-bound, so GIL isn't a bottleneck)
STRUCT_SL_LOOKBACK = 20
STRUCT_TP_LOOKBACK = 40
STRUCT_MIN_RR = 1.5

CONFIGS = ["A", "B", "C", "D"]
CONFIG_NAMES = {
    "A": "V4 Baseline (immediate entry, ATR exit)",
    "B": "V5 Pullback + ATR Exit",
    "C": "V5 Pullback + Structure Exit",
    "D": "V5 Pullback + Structure Exit + Short Bias (50% long size)",
}

# ============================================================================
# 3. FETCH / PARSE
# ============================================================================
def fetch_klines(symbol, interval, start_ms, end_ms):
    all_rows = []
    cur = start_ms
    while cur < end_ms:
        url = (f"{BASE_URL}?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={end_ms}&limit=1000")
        rows = None
        last_err = None
        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    rows = json.loads(resp.read().decode())
                break
            except urllib.error.HTTPError as e:
                if e.code == 400:
                    raise
                last_err = e
                time.sleep(0.3 * (attempt + 1))
            except Exception as e:
                last_err = e
                time.sleep(0.3 * (attempt + 1))
        if rows is None:
            raise RuntimeError(f"fetch failed for {symbol}: {last_err}")
        if not rows:
            break
        all_rows.extend(rows)
        last_open = rows[-1][0]
        if last_open <= cur:
            break
        cur = last_open + 1
        time.sleep(0.13)
    return all_rows


def parse(raw):
    opens, highs, lows, closes, volumes, times = [], [], [], [], [], []
    for r in raw:
        times.append(int(r[0]))
        opens.append(float(r[1]))
        highs.append(float(r[2]))
        lows.append(float(r[3]))
        closes.append(float(r[4]))
        volumes.append(float(r[5]))
    return opens, highs, lows, closes, volumes, times


# ============================================================================
# 4. INDICATORS (pure python)
# ============================================================================
def ema(values, period):
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    seed = sum(values[0:period]) / period
    out[period - 1] = seed
    k = 2.0 / (period + 1)
    prev = seed
    for i in range(period, n):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def sma(values, period):
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    window_sum = sum(values[0:period])
    out[period - 1] = window_sum / period
    for i in range(period, n):
        window_sum += values[i] - values[i - period]
        out[i] = window_sum / period
    return out


def rsi(closes, period=14):
    n = len(closes)
    out = [None] * n
    if n < period + 1:
        return out
    gains = losses = 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += -diff
    avg_gain = gains / period
    avg_loss = losses / period
    out[period] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    for i in range(period + 1, n):
        diff = closes[i] - closes[i - 1]
        gain = diff if diff > 0 else 0.0
        loss = -diff if diff < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return out


def atr(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    trs = [None] * n
    for i in range(1, n):
        trs[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
    if n < period + 1:
        return out
    seed = sum(trs[1:period + 1]) / period
    out[period] = seed
    prev = seed
    for i in range(period + 1, n):
        prev = (prev * (period - 1) + trs[i]) / period
        out[i] = prev
    return out


def adx_full(highs, lows, closes, period=14):
    n = len(closes)
    adx_out = [None] * n
    pdi_out = [None] * n
    mdi_out = [None] * n
    if n < period * 2 + 1:
        return adx_out, pdi_out, mdi_out
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    tr = [0.0] * n
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]))
    atr_s = sum(tr[1:period + 1])
    plus_s = sum(plus_dm[1:period + 1])
    minus_s = sum(minus_dm[1:period + 1])
    dx_list = [None] * n
    idx = period
    pdi = 100 * plus_s / atr_s if atr_s > 0 else 0.0
    mdi = 100 * minus_s / atr_s if atr_s > 0 else 0.0
    pdi_out[idx] = pdi
    mdi_out[idx] = mdi
    dx_list[idx] = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 0.0
    for i in range(period + 1, n):
        atr_s = atr_s - atr_s / period + tr[i]
        plus_s = plus_s - plus_s / period + plus_dm[i]
        minus_s = minus_s - minus_s / period + minus_dm[i]
        pdi = 100 * plus_s / atr_s if atr_s > 0 else 0.0
        mdi = 100 * minus_s / atr_s if atr_s > 0 else 0.0
        pdi_out[i] = pdi
        mdi_out[i] = mdi
        dx_list[i] = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 0.0
    start = period * 2
    if start >= n:
        return adx_out, pdi_out, mdi_out
    seed_vals = [x for x in dx_list[period:start] if x is not None]
    if not seed_vals:
        return adx_out, pdi_out, mdi_out
    seed_adx = sum(seed_vals) / len(seed_vals)
    adx_out[start - 1] = seed_adx
    prev = seed_adx
    for i in range(start, n):
        if dx_list[i] is None:
            continue
        prev = (prev * (period - 1) + dx_list[i]) / period
        adx_out[i] = prev
    return adx_out, pdi_out, mdi_out


# ============================================================================
# 5. SIGNAL DETECTION (Phase 1 — shared by all configs)
# ============================================================================
def detect_signals(opens, highs, lows, closes, volumes, rsi_lo_long=48, rsi_hi_long=62,
                    rsi_lo_short=38, rsi_hi_short=52):
    n = len(closes)
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    atr14 = atr(highs, lows, closes, 14)
    adx14, _, _ = adx_full(highs, lows, closes, 14)
    vsma10 = sma(volumes, 10)

    signals = []
    filt = defaultdict(int)
    start = 60
    for i in range(start, n):
        filt["scanned"] += 1
        if (adx14[i] is None or ema50[i] is None or ema50[i - 10] is None
                or ema9[i] is None or ema21[i] is None or ema9[i - 1] is None
                or ema21[i - 1] is None or rsi14[i] is None or vsma10[i] is None
                or atr14[i] is None):
            filt["warmup"] += 1
            continue
        if adx14[i] < 22:
            filt["adx"] += 1
            continue
        slope = (ema50[i] - ema50[i - 10]) / ema50[i - 10] * 100 if ema50[i - 10] != 0 else 0.0
        vol_ok = volumes[i] > 1.3 * vsma10[i]
        long_cross = ema9[i] > ema21[i] and ema9[i - 1] <= ema21[i - 1]
        short_cross = ema9[i] < ema21[i] and ema9[i - 1] >= ema21[i - 1]

        if not (long_cross or short_cross):
            filt["no_cross"] += 1
            continue
        if not vol_ok:
            filt["volume"] += 1
            continue
        if long_cross and slope > 0.05 and rsi_lo_long <= rsi14[i] <= rsi_hi_long:
            signals.append({"idx": i, "dir": "LONG"})
            filt["signals"] += 1
        elif short_cross and slope < -0.05 and rsi_lo_short <= rsi14[i] <= rsi_hi_short:
            signals.append({"idx": i, "dir": "SHORT"})
            filt["signals"] += 1
        else:
            if long_cross and not (slope > 0.05):
                filt["slope"] += 1
            elif short_cross and not (slope < -0.05):
                filt["slope"] += 1
            else:
                filt["rsi"] += 1
    return signals, filt, ema21, atr14


# ============================================================================
# 6. ENTRY MECHANICS
# ============================================================================
def find_pullback_entry(idx, direction, opens, highs, lows, closes, ema21, atr14):
    n = len(closes)
    for t in range(idx + 1, min(idx + 3, n - 1) + 1):
        if t >= n:
            break
        e21 = ema21[t]
        a = atr14[t]
        if e21 is None or a is None:
            continue
        if direction == "LONG":
            touched = lows[t] <= e21
            bullish_close = closes[t] > opens[t]
            not_crash = lows[t] >= e21 - 0.5 * a
            if touched and bullish_close and not_crash:
                return t, closes[t]
        else:
            touched = highs[t] >= e21
            bearish_close = closes[t] < opens[t]
            not_rip = highs[t] <= e21 + 0.5 * a
            if touched and bearish_close and not_rip:
                return t, closes[t]
    return None, None


def get_tp_sl_atr(entry_price, direction, atr_val):
    if direction == "LONG":
        return entry_price + 3 * atr_val, entry_price - 2 * atr_val
    return entry_price - 3 * atr_val, entry_price + 2 * atr_val


def get_tp_sl_structure(entry_idx, entry_price, direction, atr_val, highs, lows):
    if entry_idx < STRUCT_TP_LOOKBACK:
        return None
    if direction == "LONG":
        swing_low = min(lows[entry_idx - STRUCT_SL_LOOKBACK:entry_idx])
        sl = swing_low - 0.5 * atr_val
        swing_high = max(highs[entry_idx - STRUCT_TP_LOOKBACK:entry_idx])
        tp = swing_high
    else:
        swing_high = max(highs[entry_idx - STRUCT_SL_LOOKBACK:entry_idx])
        sl = swing_high + 0.5 * atr_val
        swing_low = min(lows[entry_idx - STRUCT_TP_LOOKBACK:entry_idx])
        tp = swing_low
    risk = abs(entry_price - sl)
    reward = abs(tp - entry_price)
    if risk <= 0 or reward / risk < STRUCT_MIN_RR:
        return None
    return tp, sl


# ============================================================================
# 7. EXIT RESOLUTION (shared wick-order logic)
# ============================================================================
def resolve_exit(entry_idx, entry_price, direction, tp, sl, opens, highs, lows, closes, times):
    n = len(closes)
    limit = min(n, entry_idx + 1 + MAX_HOLD_BARS)
    for j in range(entry_idx + 1, limit):
        o, h, l, c = opens[j], highs[j], lows[j], closes[j]
        bullish = c > o
        if direction == "LONG":
            hit_tp = h >= tp
            hit_sl = l <= sl
        else:
            hit_tp = l <= tp
            hit_sl = h >= sl
        if hit_tp and hit_sl:
            if bullish:
                return (j, sl, False, times[j]) if direction == "LONG" else (j, tp, True, times[j])
            else:
                return (j, tp, True, times[j]) if direction == "LONG" else (j, sl, False, times[j])
        elif hit_tp:
            return j, tp, True, times[j]
        elif hit_sl:
            return j, sl, False, times[j]
    # timeout / ran out of data -> force close at last available candle
    j = limit - 1
    if j <= entry_idx:
        return None
    win = closes[j] > entry_price if direction == "LONG" else closes[j] < entry_price
    return j, closes[j], win, times[j]


# ============================================================================
# 8. CANDIDATE GENERATION PER CONFIG
# ============================================================================
def generate_candidates(config_name, symbol, signals, opens, highs, lows, closes,
                         times, ema21, atr14):
    candidates = []
    for sig in signals:
        idx = sig["idx"]
        direction = sig["dir"]
        if atr14[idx] is None:
            continue
        if config_name == "A":
            entry_idx, entry_price = idx, closes[idx]
        else:
            entry_idx, entry_price = find_pullback_entry(
                idx, direction, opens, highs, lows, closes, ema21, atr14)
            if entry_idx is None:
                continue
        a_val = atr14[entry_idx] if atr14[entry_idx] is not None else atr14[idx]
        if a_val is None or a_val <= 0:
            continue

        if config_name in ("A", "B"):
            tp, sl = get_tp_sl_atr(entry_price, direction, a_val)
            exit_method = "atr"
        else:
            res = get_tp_sl_structure(entry_idx, entry_price, direction, a_val, highs, lows)
            if res is None:
                tp, sl = get_tp_sl_atr(entry_price, direction, a_val)
                exit_method = "atr_fallback"
            else:
                tp, sl = res
                exit_method = "structure"

        candidates.append({
            "symbol": symbol, "dir": direction, "entry_idx": entry_idx,
            "entry_price": entry_price, "entry_time": times[entry_idx],
            "tp": tp, "sl": sl, "exit_method": exit_method, "signal_idx": idx,
        })
    return candidates


# ============================================================================
# 9. PORTFOLIO-LEVEL SIMULATION
# ============================================================================
def run_config(config_name, all_data):
    all_candidates = []
    for symbol, d in all_data.items():
        cands = generate_candidates(config_name, symbol, d["signals"], d["opens"],
                                     d["highs"], d["lows"], d["closes"], d["times"],
                                     d["ema21"], d["atr14"])
        for c in cands:
            res = resolve_exit(c["entry_idx"], c["entry_price"], c["dir"], c["tp"], c["sl"],
                                d["opens"], d["highs"], d["lows"], d["closes"], d["times"])
            if res is None:
                continue
            j, exit_price, win, exit_time = res
            c["exit_idx"] = j
            c["exit_price"] = exit_price
            c["win"] = win
            c["exit_time"] = exit_time
            all_candidates.append(c)

    all_candidates.sort(key=lambda c: c["entry_time"])

    open_trades = []          # [{'symbol':..., 'exit_time':...}]
    balances = defaultdict(lambda: INITIAL_CAP)
    last_close_time = defaultdict(lambda: -10**18)
    paused = {}                # symbol -> threshold at which paused
    coin_trades = defaultdict(list)
    trades = []
    rejected_concurrency = 0

    for cand in all_candidates:
        sym = cand["symbol"]
        open_trades = [o for o in open_trades if o["exit_time"] > cand["entry_time"]]
        if sym in paused:
            continue
        if any(o["symbol"] == sym for o in open_trades):
            continue
        if len(open_trades) >= MAX_CONCURRENT:
            rejected_concurrency += 1
            continue
        if cand["entry_time"] - last_close_time[sym] < COOLDOWN_MS:
            continue

        bal = balances[sym]
        qty = NOTIONAL / cand["entry_price"]
        if config_name == "D" and cand["dir"] == "LONG":
            qty *= 0.5
        risk_dist = abs(cand["entry_price"] - cand["sl"])
        if risk_dist > 0:
            risk_qty = (bal * RISK_CAP_PCT) / (risk_dist * LEVERAGE)
            qty = min(qty, risk_qty)
        if qty <= 0:
            continue

        if cand["dir"] == "LONG":
            gross_pnl = (cand["exit_price"] - cand["entry_price"]) * qty
        else:
            gross_pnl = (cand["entry_price"] - cand["exit_price"]) * qty
        fees = (cand["entry_price"] + cand["exit_price"]) * qty * FEE_RATE
        net_pnl = gross_pnl - fees

        balances[sym] = bal + net_pnl
        trade = dict(cand)
        trade["qty"] = qty
        trade["net_pnl"] = net_pnl
        trades.append(trade)
        coin_trades[sym].append(trade)

        # auto-disable checks
        ct = coin_trades[sym]
        n_ct = len(ct)
        m = compute_metrics(ct)
        if sym not in paused:
            if n_ct == 10 and (m["wr"] < 35 or m["pf"] < 0.80):
                paused[sym] = 10
            elif n_ct == 20 and (m["wr"] < 40 or m["pf"] < 1.00):
                paused[sym] = 20
            elif n_ct == 30 and (m["wr"] < 42 or m["pf"] < 1.20):
                paused[sym] = 30

        last_close_time[sym] = cand["exit_time"]
        open_trades.append({"symbol": sym, "exit_time": cand["exit_time"]})

    total_signals = sum(len(d["signals"]) for d in all_data.values())
    total_candidates_pre_resolve = sum(
        1 for _ in all_candidates)  # already resolved candidates (entries that filled)
    return {
        "trades": trades,
        "coin_trades": coin_trades,
        "paused": paused,
        "total_signals": total_signals,
        "entries_filled": len(all_candidates),
        "entries_taken": len(trades),
        "rejected_concurrency": rejected_concurrency,
    }


# ============================================================================
# 10. METRICS
# ============================================================================
def compute_metrics(trades):
    n = len(trades)
    if n == 0:
        return {"n": 0, "wr": 0.0, "pf": 0.0, "net": 0.0, "mdd": 0.0, "sharpe": 0.0,
                "aw": 0.0, "al": 0.0, "exp": 0.0, "gp": 0.0, "gl": 0.0,
                "nlongs": 0, "nshorts": 0, "lwr": 0.0, "swr": 0.0}
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    gp = sum(t["net_pnl"] for t in wins)
    gl = abs(sum(t["net_pnl"] for t in losses))
    net = sum(t["net_pnl"] for t in trades)
    wr = len(wins) / n * 100
    pf = (gp / gl) if gl > 0 else (999.0 if gp > 0 else 0.0)
    aw = gp / len(wins) if wins else 0.0
    al = gl / len(losses) if losses else 0.0
    exp = net / n
    equity = peak = mdd = 0.0
    for t in trades:
        equity += t["net_pnl"]
        peak = max(peak, equity)
        mdd = max(mdd, peak - equity)
    pnl_list = [t["net_pnl"] for t in trades]
    if n > 1 and statistics.pstdev(pnl_list) > 0:
        sharpe = statistics.mean(pnl_list) / statistics.pstdev(pnl_list) * math.sqrt(n)
    else:
        sharpe = 0.0
    longs = [t for t in trades if t["dir"] == "LONG"]
    shorts = [t for t in trades if t["dir"] == "SHORT"]
    lwr = (sum(1 for t in longs if t["net_pnl"] > 0) / len(longs) * 100) if longs else 0.0
    swr = (sum(1 for t in shorts if t["net_pnl"] > 0) / len(shorts) * 100) if shorts else 0.0
    return {"n": n, "wr": round(wr, 2), "pf": round(pf, 3), "net": round(net, 4),
            "mdd": round(mdd, 4), "sharpe": round(sharpe, 3), "aw": round(aw, 4),
            "al": round(al, 4), "exp": round(exp, 4), "gp": round(gp, 4), "gl": round(gl, 4),
            "nlongs": len(longs), "nshorts": len(shorts), "lwr": round(lwr, 2), "swr": round(swr, 2)}


def tier_of(m):
    if m["n"] >= 15 and m["wr"] >= 45 and m["pf"] >= 1.5 and m["net"] > 0:
        return "TIER1"
    if m["n"] >= 10 and m["wr"] >= 40 and m["pf"] >= 1.2 and m["net"] > 0:
        return "TIER2"
    return "TIER3"


# ============================================================================
# 11. MAIN
# ============================================================================
def process_symbol(symbol, start_ms, end_ms):
    """Fetch + parse + compute indicators for one symbol. Runs in a worker thread."""
    try:
        raw = fetch_klines(symbol, INTERVAL, start_ms, end_ms)
        if len(raw) < MIN_CANDLES:
            return symbol, None, f"insufficient_data({len(raw)})"
        opens, highs, lows, closes, volumes, times = parse(raw)
        signals, filt, ema21, atr14 = detect_signals(opens, highs, lows, closes, volumes)
        # secondary pass with OLD (V4) RSI bounds, counting-only, for the RSI impact report
        signals_old, _, _, _ = detect_signals(
            opens, highs, lows, closes, volumes,
            rsi_lo_long=40, rsi_hi_long=68, rsi_lo_short=32, rsi_hi_short=60)
        data = {
            "opens": opens, "highs": highs, "lows": lows, "closes": closes,
            "volumes": volumes, "times": times, "signals": signals,
            "filt": filt, "ema21": ema21, "atr14": atr14,
            "signals_old_rsi_count": len(signals_old),
        }
        return symbol, data, None
    except urllib.error.HTTPError as e:
        return symbol, None, f"http_{e.code}"
    except Exception as e:
        return symbol, None, f"error:{e}"


def main():
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_BACK * 24 * 60 * 60 * 1000

    COINS = fetch_live_universe()
    all_data = {}
    tested, skipped = [], []

    print(f"=== V5 BACKTEST START — {len(COINS)} symbols in universe "
          f"(fetching with {FETCH_WORKERS} parallel workers) ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        futures = {executor.submit(process_symbol, sym, start_ms, end_ms): sym for sym in COINS}
        done_count = 0
        for fut in concurrent.futures.as_completed(futures):
            done_count += 1
            symbol, data, err = fut.result()
            if err:
                skipped.append((symbol, err))
                print(f"[{done_count}/{len(COINS)}] {symbol}: SKIP {err}")
            else:
                all_data[symbol] = data
                tested.append(symbol)
                print(f"[{done_count}/{len(COINS)}] {symbol}: OK "
                      f"{len(data['closes'])} candles, {len(data['signals'])} signals")

    print(f"\n=== DATA FETCH DONE: {len(tested)} tested, {len(skipped)} skipped ===\n")

    # ---- Run all 4 configs ----
    config_results = {}
    for cfg in CONFIGS:
        print(f"--- Running Config {cfg}: {CONFIG_NAMES[cfg]} ---")
        res = run_config(cfg, all_data)
        global_metrics = compute_metrics(res["trades"])
        per_coin = {}
        for sym, ct in res["coin_trades"].items():
            m = compute_metrics(ct)
            per_coin[sym] = {
                "metrics": m,
                "category": SYMBOL_CATEGORY.get(sym, "UNCATEGORIZED"),
                "paused_at": res["paused"].get(sym),
                "tier": tier_of(m),
            }
        config_results[cfg] = {
            "global": global_metrics,
            "per_coin": per_coin,
            "total_signals": res["total_signals"],
            "entries_filled": res["entries_filled"],
            "entries_taken": res["entries_taken"],
            "rejected_concurrency": res["rejected_concurrency"],
            "coins_paused": len(res["paused"]),
        }
        print(f"    Global: n={global_metrics['n']} WR={global_metrics['wr']}% "
              f"PF={global_metrics['pf']} Net={global_metrics['net']}")

    # ---- Aggregate filter stats across all coins ----
    agg_filt = defaultdict(int)
    total_old_rsi_signals = 0
    for d in all_data.values():
        for k, v in d["filt"].items():
            agg_filt[k] += v
        total_old_rsi_signals += d["signals_old_rsi_count"]

    # ---- Build report ----
    report = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "universe_requested": len(COINS),
        "universe_tested": len(tested),
        "universe_skipped": len(skipped),
        "skipped_detail": skipped,
        "filter_stats_aggregate": dict(agg_filt),
        "rsi_tightening_impact": {
            "v5_tightened_signal_count": agg_filt.get("signals", 0),
            "v4_loose_rsi_signal_count_estimate": total_old_rsi_signals,
        },
        "configs": config_results,
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    # ---- Summary text ----
    lines = []
    lines.append("=" * 80)
    lines.append("V5 BACKTEST SUMMARY — PULLBACK ENTRY, 4-CONFIG COMPARISON")
    lines.append("=" * 80)
    lines.append(f"Universe requested: {len(COINS)} | tested: {len(tested)} | skipped: {len(skipped)}")
    lines.append("")
    lines.append("--- CONFIG COMPARISON ---")
    lines.append(f"{'Config':<8}{'Name':<45}{'Trades':>8}{'WR%':>8}{'PF':>8}{'Net':>10}{'MDD':>10}")
    best_cfg = None
    best_pf = -1
    for cfg in CONFIGS:
        g = config_results[cfg]["global"]
        lines.append(f"{cfg:<8}{CONFIG_NAMES[cfg]:<45}{g['n']:>8}{g['wr']:>8}{g['pf']:>8}{g['net']:>10}{g['mdd']:>10}")
        if g["n"] >= 30 and g["pf"] > best_pf:
            best_pf = g["pf"]
            best_cfg = cfg
    lines.append("")
    lines.append(f"WINNING CONFIG (by PF, min 30 trades): {best_cfg or 'N/A - insufficient trades'}")
    lines.append("")

    lines.append("--- PULLBACK EFFECTIVENESS (Config B vs A) ---")
    a = config_results["A"]
    b = config_results["B"]
    lines.append(f"Config A (immediate) signals={a['total_signals']} entries_taken={a['entries_taken']}")
    lines.append(f"Config B (pullback)  signals={b['total_signals']} filled={b['entries_filled']} "
                 f"entries_taken={b['entries_taken']} "
                 f"(fill rate {round(100*b['entries_filled']/b['total_signals'],2) if b['total_signals'] else 0}%)")
    lines.append(f"Config A WR={a['global']['wr']}% PF={a['global']['pf']}  |  "
                 f"Config B WR={b['global']['wr']}% PF={b['global']['pf']}")
    lines.append("")

    lines.append("--- RSI TIGHTENING IMPACT ---")
    lines.append(f"V5 tightened RSI (48-62/38-52) signal count: {agg_filt.get('signals',0)}")
    lines.append(f"V4 loose RSI (40-68/32-60) signal count estimate: {total_old_rsi_signals}")
    lines.append("")

    lines.append("--- STRUCTURE VS ATR EXIT (Config C vs B) ---")
    c = config_results["C"]
    lines.append(f"Config B (ATR exit) WR={b['global']['wr']}% PF={b['global']['pf']} MDD={b['global']['mdd']}")
    lines.append(f"Config C (Structure exit) WR={c['global']['wr']}% PF={c['global']['pf']} MDD={c['global']['mdd']}")
    lines.append("")

    lines.append("--- SHORT BIAS TEST (Config D vs C) ---")
    d = config_results["D"]
    lines.append(f"Config C WR={c['global']['wr']}% PF={c['global']['pf']} Net={c['global']['net']}")
    lines.append(f"Config D WR={d['global']['wr']}% PF={d['global']['pf']} Net={d['global']['net']}")
    lines.append("")

    lines.append("--- AUTO-DISABLE / PAUSE STATS ---")
    for cfg in CONFIGS:
        lines.append(f"Config {cfg}: {config_results[cfg]['coins_paused']} coins paused "
                      f"(rejected-by-concurrency events: {config_results[cfg]['rejected_concurrency']})")
    lines.append("")

    for cfg in CONFIGS:
        lines.append("=" * 80)
        lines.append(f"CONFIG {cfg} — {CONFIG_NAMES[cfg]}")
        lines.append("=" * 80)
        pc = config_results[cfg]["per_coin"]
        tier1 = sorted([s for s, v in pc.items() if v["tier"] == "TIER1"],
                        key=lambda s: -pc[s]["metrics"]["pf"])
        tier2 = sorted([s for s, v in pc.items() if v["tier"] == "TIER2"],
                        key=lambda s: -pc[s]["metrics"]["pf"])
        lines.append(f"TIER 1 (Elite, {len(tier1)} coins): {', '.join(tier1) if tier1 else 'none'}")
        lines.append(f"TIER 2 (Monitor, {len(tier2)} coins): {', '.join(tier2) if tier2 else 'none'}")
        lines.append("")
        cat_count = defaultdict(int)
        for s in tier1:
            cat_count[pc[s]["category"]] += 1
        lines.append(f"Tier 1 category breakdown: {dict(cat_count)}")
        lines.append("")
        ranked = sorted(
            [(s, v["metrics"]) for s, v in pc.items() if v["metrics"]["n"] >= 5],
            key=lambda x: -x[1]["pf"])
        lines.append(f"{'Symbol':<14}{'Cat':<10}{'N':>6}{'WR%':>8}{'PF':>8}{'Net':>10}{'MDD':>10}{'Paused':>8}")
        lines.append("-- TOP 30 BY PF --")
        for s, m in ranked[:30]:
            paused_at = pc[s]["paused_at"] or "-"
            lines.append(f"{s:<14}{pc[s]['category']:<10}{m['n']:>6}{m['wr']:>8}{m['pf']:>8}{m['net']:>10}{m['mdd']:>10}{str(paused_at):>8}")
        lines.append("-- BOTTOM 30 BY PF --")
        for s, m in ranked[-30:]:
            paused_at = pc[s]["paused_at"] or "-"
            lines.append(f"{s:<14}{pc[s]['category']:<10}{m['n']:>6}{m['wr']:>8}{m['pf']:>8}{m['net']:>10}{m['mdd']:>10}{str(paused_at):>8}")
        lines.append("")

    lines.append("=" * 80)
    lines.append("NOTES / OVERFITTING WARNINGS")
    lines.append("=" * 80)
    lines.append("- 1 year is a single market regime; results may not generalize forward.")
    lines.append("- Concurrency (max 3 open) is enforced portfolio-wide with admission control;")
    lines.append("  each candidate trade's own outcome is resolved independently of concurrency,")
    lines.append("  then accepted/rejected retrospectively by entry-time ordering.")
    lines.append("- Coins with PF > 3.0 and < 20 trades should be treated as potential outliers.")
    lines.append("- V4-vs-V5 RSI comparison above is a signal-count estimate on the same price")
    lines.append("  data, not a full re-run of the old V4 backtest.")
    lines.append("=" * 80)

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n".join(lines[-40:]))
    print("\n=== DONE. backtest_report.json and backtest_summary.txt written. ===")


if __name__ == "__main__":
    main()
