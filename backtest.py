"""
BACKTEST v4 — 30M Strategy, 150 Coins, 1 Year
Bot Version: Trading Bot v6 — 30M Enhancement
Strategy: ADX(14)>=22 + 50EMA slope + 9/21 EMA cross + Volume + RSI
Exit: TP=3xATR, SL=2xATR (fixed, no trailing, no breakeven)
Auto-disable: pause underperforming coins at 10/20/30 trade milestones
"""

import json
import math
import time
import datetime
import urllib.request
import urllib.error
import statistics
from collections import defaultdict

# ═══════════════════════════════════════════════════════
# 1. COIN UNIVERSE — 150 coins (known issues pre-fixed)
# ═══════════════════════════════════════════════════════

COIN_LIST = [
    # Layer 1
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "AVAXUSDT",
    "NEARUSDT", "ATOMUSDT", "APTUSDT", "SUIUSDT", "INJUSDT",
    "HBARUSDT", "BNBUSDT", "TONUSDT", "TRXUSDT", "XRPUSDT",
    "DOTUSDT", "ALGOUSDT", "FTMUSDT", "SEIUSDT", "KASUSDT",

    # Layer 2
    "ARBUSDT", "OPUSDT", "POLUSDT", "STRKUSDT", "IMXUSDT",
    "METISUSDT", "ZKUSDT", "MANTAUSDT", "SCROLLUSDT",

    # Meme
    "DOGEUSDT", "WIFUSDT", "BOMEUSDT", "NEIROUSDT", "TRUMPUSDT",
    "POPCATUSDT", "BRETTUSDT", "MEWUSDT", "TURBOUSDT", "MOGUSDT",

    # DeFi
    "LINKUSDT", "UNIUSDT", "AAVEUSDT", "MKRUSDT", "LDOUSDT",
    "RNDERUSDT", "ARUSDT", "GRTUSDT", "SNXUSDT", "COMPUSDT",
    "YFIUSDT", "CRVUSDT", "1INCHUSDT", "SUSHIUSDT", "DYDXUSDT",
    "PENDLEUSDT", "JUPUSDT", "RAYUSDT",

    # AI / Data
    "FETUSDT", "AGIXUSDT", "OCEANUSDT", "WLDUSDT", "RENDERUSDT",
    "ARKMUSDT", "NFPUSDT", "AIUSDT", "PHBUSDT",

    # Gaming / Metaverse
    "SANDUSDT", "MANAUSDT", "AXSUSDT", "GALAUSDT", "ENJUSDT",
    "ILVUSDT", "MAGICUSDT", "YGGUSDT", "BEAMUSDT", "PYRUSDT",

    # Infrastructure
    "LTCUSDT", "BCHUSDT", "ETCUSDT", "XLMUSDT", "VETUSDT",
    "FILUSDT", "ICPUSDT", "THETAUSDT", "HNTUSDT", "HOTUSDT",

    # Other high-volume
    "APEUSDT", "GALXEUSDT", "BLURUSDT", "CYBERUSDT", "ORDIUSDT",
    "SATSUSDT", "TIAUSDT", "DYMUSDT", "ALTUSDT", "JUPUSDT",
    "PYTHUSDT", "WUSDT", "JTOUSDT", "MNTUSDT", "CAKEUSDT",
    "GMXUSDT", "GLMUSDT", "CELOUSDT", "FLOWUSDT", "EGLDUSDT",
    "KSMUSDT", "RUNEUSDT", "ONEUSDT", "ZENUSDT", "ZRXUSDT",
    "BATUSDT", "STORJUSDT", "CTSIUSDT", "LRCUSDT", "BANDUSDT",
    "RSRUSDT", "SKLUSDT", "CVCUSDT", "MTLUSDT", "OGNUSDT",
    "DATAUSDT", "WOOUSDT", "MAVUSDT", "IDUSDT", "CFXUSDT",
    "HOOKUSDT", "STGUSDT", "ACHUSDT", "MDTUSDT", "QNTUSDT",
    "GALUSDT", "AMBUSDT", "ASTRUSDT", "LEVERUSDT", "LPTUSDT",
    "PERPUSDT", "RAREUSDT", "MINAUSDT", "SPELLUSDT", "ALPACAUSDT",
    "AGLDUSDT", "IDEXUSDT", "RDNTUSDT", "BICOUSDT", "COTIUSDT",
    "STXUSDT", "IOTAUSDT", "ONTUSDT", "CKBUSDT", "TFUELUSDT",
]

# Remove duplicates while preserving order
seen = set()
COIN_LIST_DEDUPED = []
for c in COIN_LIST:
    if c not in seen:
        seen.add(c)
        COIN_LIST_DEDUPED.append(c)
COIN_LIST = COIN_LIST_DEDUPED

# Known broken coins to skip immediately
SKIP_COINS = {"1000FLOKIUSDT", "MATICUSDT"}

# Coin category mapping
CATEGORY_MAP = {}
for s in ["BTCUSDT","ETHUSDT","SOLUSDT","ADAUSDT","AVAXUSDT","NEARUSDT","ATOMUSDT",
          "APTUSDT","SUIUSDT","INJUSDT","HBARUSDT","BNBUSDT","TONUSDT","TRXUSDT",
          "XRPUSDT","DOTUSDT","ALGOUSDT","FTMUSDT","SEIUSDT","KASUSDT"]:
    CATEGORY_MAP[s] = "Layer1"
for s in ["ARBUSDT","OPUSDT","POLUSDT","STRKUSDT","IMXUSDT","METISUSDT",
          "ZKUSDT","MANTAUSDT","SCROLLUSDT"]:
    CATEGORY_MAP[s] = "Layer2"
for s in ["DOGEUSDT","WIFUSDT","BOMEUSDT","NEIROUSDT","TRUMPUSDT","POPCATUSDT",
          "BRETTUSDT","MEWUSDT","TURBOUSDT","MOGUSDT"]:
    CATEGORY_MAP[s] = "Meme"
for s in ["LINKUSDT","UNIUSDT","AAVEUSDT","MKRUSDT","LDOUSDT","RNDERUSDT","ARUSDT",
          "GRTUSDT","SNXUSDT","COMPUSDT","YFIUSDT","CRVUSDT","1INCHUSDT","SUSHIUSDT",
          "DYDXUSDT","PENDLEUSDT","JUPUSDT","RAYUSDT"]:
    CATEGORY_MAP[s] = "DeFi"
for s in ["FETUSDT","AGIXUSDT","OCEANUSDT","WLDUSDT","RENDERUSDT","ARKMUSDT",
          "NFPUSDT","AIUSDT","PHBUSDT"]:
    CATEGORY_MAP[s] = "AI"
for s in ["SANDUSDT","MANAUSDT","AXSUSDT","GALAUSDT","ENJUSDT","ILVUSDT",
          "MAGICUSDT","YGGUSDT","BEAMUSDT","PYRUSDT"]:
    CATEGORY_MAP[s] = "Gaming"
for s in ["LTCUSDT","BCHUSDT","ETCUSDT","XLMUSDT","VETUSDT","FILUSDT","ICPUSDT",
          "THETAUSDT","HNTUSDT","HOTUSDT"]:
    CATEGORY_MAP[s] = "Infra"

# ═══════════════════════════════════════════════════════
# 2. CONFIG
# ═══════════════════════════════════════════════════════

FEE_RATE    = 0.0005   # 0.05% taker per side
SLIPPAGE    = 0.0      # per brief: ignore slippage
LEVERAGE    = 10
FIXED_MARGIN = 1.0     # $1 margin per trade
NOTIONAL    = FIXED_MARGIN * LEVERAGE  # $10 notional
INITIAL_BAL = 100.0    # per coin
MAX_CONCURRENT = 3     # max open positions portfolio-wide
COOLDOWN_BARS  = 1     # 1 bar (~30 min) cooldown after trade close
MAX_RISK_PCT   = 0.02  # 2% max risk per trade

# Data range: 1 year
END_DT   = datetime.datetime(2026, 7, 26, 0, 0, 0)
START_DT = datetime.datetime(2025, 7, 26, 0, 0, 0)
START_MS = int(START_DT.timestamp() * 1000)
END_MS   = int(END_DT.timestamp() * 1000)
INTERVAL = "30m"

# ═══════════════════════════════════════════════════════
# 3. FETCH FUNCTIONS
# ═══════════════════════════════════════════════════════

BASE_URL = "https://data-api.binance.vision/api/v3/klines"

def fetch_klines(symbol, interval, start_ms, end_ms):
    """Fetch all klines with pagination, retry logic, returns raw list."""
    all_klines = []
    current_start = start_ms
    max_retries = 5

    while current_start < end_ms:
        url = (f"{BASE_URL}?symbol={symbol}&interval={interval}"
               f"&startTime={current_start}&endTime={end_ms}&limit=1000")
        retries = 0
        while retries < max_retries:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
                if not data:
                    return all_klines
                all_klines.extend(data)
                last_open_time = data[-1][0]
                current_start = last_open_time + 1
                if len(data) < 1000:
                    return all_klines
                time.sleep(0.13)
                break
            except urllib.error.HTTPError as e:
                if e.code in (400, 451):
                    print(f"    [{symbol}] HTTP {e.code} — skipping coin")
                    return None
                retries += 1
                time.sleep(2 ** retries)
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    print(f"    [{symbol}] Fetch error after {max_retries} retries: {e}")
                    return None
                time.sleep(2 ** retries)
    return all_klines

def parse_klines(raw):
    """Parse raw klines into OHLCV lists."""
    opens   = [float(k[1]) for k in raw]
    highs   = [float(k[2]) for k in raw]
    lows    = [float(k[3]) for k in raw]
    closes  = [float(k[4]) for k in raw]
    volumes = [float(k[5]) for k in raw]
    times   = [int(k[0])   for k in raw]
    return opens, highs, lows, closes, volumes, times

# ═══════════════════════════════════════════════════════
# 4. INDICATORS (pure Python, no numpy/pandas)
# ═══════════════════════════════════════════════════════

def ema(values, period):
    result = [None] * len(values)
    k = 2.0 / (period + 1)
    for i in range(len(values)):
        if i < period - 1:
            result[i] = None
        elif i == period - 1:
            result[i] = sum(values[:period]) / period
        else:
            if result[i-1] is None:
                result[i] = None
            else:
                result[i] = values[i] * k + result[i-1] * (1 - k)
    return result

def sma(values, period):
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1:i + 1]) / period
    return result

def atr(highs, lows, closes, period=14):
    n = len(closes)
    tr_list = [None] * n
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        tr_list[i] = max(hl, hc, lc)
    result = [None] * n
    if n > period:
        initial = [t for t in tr_list[1:period+1] if t is not None]
        if len(initial) == period:
            result[period] = sum(initial) / period
            for i in range(period+1, n):
                if tr_list[i] is not None and result[i-1] is not None:
                    result[i] = (result[i-1] * (period-1) + tr_list[i]) / period
    return result

def rsi(closes, period=14):
    n = len(closes)
    result = [None] * n
    if n < period + 1:
        return result
    gains = []
    losses = []
    for i in range(1, period+1):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - (100.0 / (1.0 + rs))
    for i in range(period+1, n):
        diff = closes[i] - closes[i-1]
        g = max(diff, 0)
        l = max(-diff, 0)
        avg_gain = (avg_gain * (period-1) + g) / period
        avg_loss = (avg_loss * (period-1) + l) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100.0 - (100.0 / (1.0 + rs))
    return result

def adx_full(highs, lows, closes, period=14):
    n = len(closes)
    pdi_list = [None] * n
    mdi_list = [None] * n
    adx_list = [None] * n
    if n < period * 2 + 2:
        return adx_list, pdi_list, mdi_list
    tr_list  = [0.0] * n
    pdm_list = [0.0] * n
    mdm_list = [0.0] * n
    for i in range(1, n):
        hl  = highs[i] - lows[i]
        hpc = abs(highs[i] - closes[i-1])
        lpc = abs(lows[i] - closes[i-1])
        tr_list[i] = max(hl, hpc, lpc)
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_list[i] = up   if (up > down and up > 0)   else 0.0
        mdm_list[i] = down if (down > up and down > 0) else 0.0
    # Wilder smoothing
    atr_s  = [0.0] * n
    pdm_s  = [0.0] * n
    mdm_s  = [0.0] * n
    atr_s[period]  = sum(tr_list[1:period+1])
    pdm_s[period]  = sum(pdm_list[1:period+1])
    mdm_s[period]  = sum(mdm_list[1:period+1])
    for i in range(period+1, n):
        atr_s[i]  = atr_s[i-1]  - atr_s[i-1]/period  + tr_list[i]
        pdm_s[i]  = pdm_s[i-1]  - pdm_s[i-1]/period  + pdm_list[i]
        mdm_s[i]  = mdm_s[i-1]  - mdm_s[i-1]/period  + mdm_list[i]
    for i in range(period, n):
        if atr_s[i] == 0:
            continue
        pdi = 100.0 * pdm_s[i] / atr_s[i]
        mdi = 100.0 * mdm_s[i] / atr_s[i]
        pdi_list[i] = pdi
        mdi_list[i] = mdi
    dx_list = [None] * n
    for i in range(period, n):
        p = pdi_list[i]
        m = mdi_list[i]
        if p is None or m is None:
            continue
        denom = p + m
        if denom == 0:
            dx_list[i] = 0.0
        else:
            dx_list[i] = 100.0 * abs(p - m) / denom
    adx_init = [dx_list[i] for i in range(period, period*2) if dx_list[i] is not None]
    if len(adx_init) == period:
        adx_list[period*2 - 1] = sum(adx_init) / period
        for i in range(period*2, n):
            if dx_list[i] is not None and adx_list[i-1] is not None:
                adx_list[i] = (adx_list[i-1] * (period-1) + dx_list[i]) / period
    return adx_list, pdi_list, mdi_list

def vsma(volumes, period=10):
    return sma(volumes, period)

# ═══════════════════════════════════════════════════════
# 5. STRATEGY — 30M EMA Cross + ADX + Volume + RSI
# ═══════════════════════════════════════════════════════

def run_strategy(symbol, opens, highs, lows, closes, volumes, times, balance_start):
    """
    Run strategy on one coin. Returns (trades_list, filter_counts, paused_at).
    """
    n = len(closes)

    # Compute indicators
    ema9   = ema(closes, 9)
    ema21  = ema(closes, 21)
    ema50  = ema(closes, 50)
    atr14  = atr(highs, lows, closes, 14)
    rsi14  = rsi(closes, 14)
    vol_sma = vsma(volumes, 10)
    adx14, _, _ = adx_full(highs, lows, closes, 14)

    trades = []
    balance = balance_start
    in_trade = False
    cooldown_until = -1
    paused_at = None

    # Filter rejection counters
    fc = {
        "total_candles": 0,
        "skip_warmup": 0,
        "skip_cooldown": 0,
        "skip_none_indicator": 0,
        "reject_adx": 0,
        "reject_ema50_slope": 0,
        "reject_no_cross": 0,
        "reject_volume": 0,
        "reject_rsi": 0,
        "signals_generated": 0,
    }

    # Start from bar index 50 (enough warmup for all indicators)
    for i in range(50, n):
        fc["total_candles"] += 1

        # Auto-disable check (only fully closed trades — pnl not None)
        if paused_at is None:
            closed = [t for t in trades if t["pnl"] is not None]
            ntrades = len(closed)
            if ntrades >= 10:
                wr = sum(1 for t in closed if t["win"]) / ntrades
                wins   = [t["pnl"] for t in closed if t["win"]]
                losses = [abs(t["pnl"]) for t in closed if not t["win"]]
                gp = sum(wins)
                gl = sum(losses)
                pf = gp / gl if gl > 0 else (999 if gp > 0 else 0)
                if wr < 0.35 or pf < 0.8:
                    paused_at = 10
                    break
            if ntrades >= 20:
                wr = sum(1 for t in closed if t["win"]) / ntrades
                wins   = [t["pnl"] for t in closed if t["win"]]
                losses = [abs(t["pnl"]) for t in closed if not t["win"]]
                gp = sum(wins)
                gl = sum(losses)
                pf = gp / gl if gl > 0 else (999 if gp > 0 else 0)
                if wr < 0.40 or pf < 1.0:
                    paused_at = 20
                    break
            if ntrades >= 30:
                wr = sum(1 for t in closed if t["win"]) / ntrades
                wins   = [t["pnl"] for t in closed if t["win"]]
                losses = [abs(t["pnl"]) for t in closed if not t["win"]]
                gp = sum(wins)
                gl = sum(losses)
                pf = gp / gl if gl > 0 else (999 if gp > 0 else 0)
                if wr < 0.42 or pf < 1.2:
                    paused_at = 30
                    break

        if in_trade:
            # Check exit on each subsequent candle using high/low
            t = trades[-1]
            tp_price = t["tp"]
            sl_price = t["sl"]
            direction = t["dir"]
            h = highs[i]
            l = lows[i]
            o = opens[i]
            c = closes[i]

            hit_tp = False
            hit_sl = False

            if direction == "LONG":
                tp_hit = h >= tp_price
                sl_hit = l <= sl_price
            else:
                tp_hit = l <= tp_price
                sl_hit = h >= sl_price

            if tp_hit and sl_hit:
                # Wick analysis: bullish candle → low hit first → SL first for long
                # Bearish candle → high hit first → SL first for short
                if direction == "LONG":
                    if c > o:  # bullish: assume low(SL) hit first
                        hit_sl = True
                    else:      # bearish: assume high hit first → TP for long? No.
                        # bearish: high hit first → for long, high=TP hits first
                        hit_tp = True
                else:  # SHORT
                    if c < o:  # bearish: assume high(SL for short) hit first
                        hit_sl = True
                    else:
                        hit_tp = True
            elif tp_hit:
                hit_tp = True
            elif sl_hit:
                hit_sl = True

            if hit_tp or hit_sl:
                exit_price = tp_price if hit_tp else sl_price
                qty = t["qty"]
                if direction == "LONG":
                    gross = (exit_price - t["entry"]) * qty
                else:
                    gross = (t["entry"] - exit_price) * qty
                fee = (t["entry"] + exit_price) * qty * FEE_RATE
                net_pnl = gross - fee
                balance += net_pnl
                t["exit"]   = exit_price
                t["exit_t"] = times[i]
                t["pnl"]    = net_pnl
                t["win"]    = net_pnl > 0
                t["hit_tp"] = hit_tp
                t["dur"]    = (times[i] - t["entry_t"]) / (1000 * 60 * 30)  # bars
                in_trade = False
                cooldown_until = i + COOLDOWN_BARS
            continue

        # Cooldown check
        if i <= cooldown_until:
            fc["skip_cooldown"] += 1
            continue

        # Indicator None check
        if (ema9[i] is None or ema9[i-1] is None or
            ema21[i] is None or ema21[i-1] is None or
            ema50[i] is None or ema50[i-10] is None or
            adx14[i] is None or atr14[i] is None or
            rsi14[i] is None or vol_sma[i] is None):
            fc["skip_none_indicator"] += 1
            continue

        # Filter 1: ADX
        if adx14[i] < 22:
            fc["reject_adx"] += 1
            continue

        # Filter 2: EMA50 slope
        slope = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100
        if not (slope > 0.05 or slope < -0.05):
            fc["reject_ema50_slope"] += 1
            continue

        # Filter 3: 9/21 EMA cross
        bull_cross = (ema9[i] > ema21[i]) and (ema9[i-1] <= ema21[i-1])
        bear_cross = (ema9[i] < ema21[i]) and (ema9[i-1] >= ema21[i-1])
        if not (bull_cross or bear_cross):
            fc["reject_no_cross"] += 1
            continue

        # Direction from cross + slope alignment
        if bull_cross and slope > 0.05:
            direction = "LONG"
        elif bear_cross and slope < -0.05:
            direction = "SHORT"
        else:
            fc["reject_ema50_slope"] += 1
            continue

        # Filter 4: Volume
        if volumes[i] <= 1.3 * vol_sma[i]:
            fc["reject_volume"] += 1
            continue

        # Filter 5: RSI
        rv = rsi14[i]
        if direction == "LONG" and not (40 <= rv <= 68):
            fc["reject_rsi"] += 1
            continue
        if direction == "SHORT" and not (32 <= rv <= 60):
            fc["reject_rsi"] += 1
            continue

        fc["signals_generated"] += 1

        # Entry
        entry_price = closes[i]
        atr_val = atr14[i]
        tp_price = entry_price + 3 * atr_val if direction == "LONG" else entry_price - 3 * atr_val
        sl_price = entry_price - 2 * atr_val if direction == "LONG" else entry_price + 2 * atr_val

        # Position sizing
        fixed_qty = NOTIONAL / entry_price
        sl_dist = abs(entry_price - sl_price)
        max_qty_by_risk = (balance * MAX_RISK_PCT) / (sl_dist * LEVERAGE) if sl_dist > 0 else fixed_qty
        qty = min(fixed_qty, max_qty_by_risk)

        fee_entry = entry_price * qty * FEE_RATE

        trades.append({
            "dir":     direction,
            "entry":   entry_price,
            "tp":      tp_price,
            "sl":      sl_price,
            "qty":     qty,
            "entry_t": times[i],
            "exit":    None,
            "exit_t":  None,
            "pnl":     None,
            "win":     None,
            "hit_tp":  None,
            "dur":     None,
            "risk":    sl_dist * qty,
            "fee_entry": fee_entry,
        })
        balance -= fee_entry
        in_trade = True

    # Close any open trade at last price
    if in_trade and len(trades) > 0:
        t = trades[-1]
        if t["exit"] is None:
            exit_price = closes[-1]
            qty = t["qty"]
            if t["dir"] == "LONG":
                gross = (exit_price - t["entry"]) * qty
            else:
                gross = (t["entry"] - exit_price) * qty
            fee = (t["entry"] + exit_price) * qty * FEE_RATE
            net_pnl = gross - fee
            balance += net_pnl
            t["exit"]   = exit_price
            t["exit_t"] = times[-1]
            t["pnl"]    = net_pnl
            t["win"]    = net_pnl > 0
            t["hit_tp"] = False
            t["dur"]    = (times[-1] - t["entry_t"]) / (1000 * 60 * 30)

    # Only keep closed trades
    closed_trades = [t for t in trades if t["pnl"] is not None]
    return closed_trades, fc, paused_at

# ═══════════════════════════════════════════════════════
# 6. METRICS
# ═══════════════════════════════════════════════════════

def compute_metrics(trades, symbol=""):
    n = len(trades)
    if n == 0:
        return {
            "symbol": symbol, "n": 0, "wr": 0, "pf": 0, "net": 0,
            "mdd": 0, "sharpe": 0, "aw": 0, "al": 0, "exp": 0,
            "nlongs": 0, "nshorts": 0, "lwr": 0, "swr": 0,
            "gp": 0, "gl": 0
        }

    wins   = [t["pnl"] for t in trades if t["win"]]
    losses = [t["pnl"] for t in trades if not t["win"]]
    gp = sum(wins)   if wins   else 0
    gl = sum(abs(p) for p in losses) if losses else 0
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)
    wr = len(wins) / n
    aw = gp / len(wins)   if wins   else 0
    al = gl / len(losses) if losses else 0
    net = sum(t["pnl"] for t in trades)
    exp = net / n

    longs  = [t for t in trades if t["dir"] == "LONG"]
    shorts = [t for t in trades if t["dir"] == "SHORT"]
    lwr = sum(1 for t in longs  if t["win"]) / len(longs)  if longs  else 0
    swr = sum(1 for t in shorts if t["win"]) / len(shorts) if shorts else 0

    # Drawdown on cumulative pnl curve
    cum = 0.0
    peak = 0.0
    mdd = 0.0
    for t in trades:
        cum += t["pnl"]
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > mdd:
            mdd = dd

    # Sharpe (simplified, daily grouping not available so use per-trade)
    if n > 1:
        pnl_list = [t["pnl"] for t in trades]
        mean_pnl = statistics.mean(pnl_list)
        std_pnl  = statistics.stdev(pnl_list)
        sharpe = (mean_pnl / std_pnl * math.sqrt(n)) if std_pnl > 0 else 0
    else:
        sharpe = 0

    return {
        "symbol": symbol,
        "n": n,
        "wr": round(wr * 100, 2),
        "pf": round(pf, 4),
        "net": round(net, 4),
        "mdd": round(mdd, 4),
        "sharpe": round(sharpe, 3),
        "aw": round(aw, 4),
        "al": round(al, 4),
        "exp": round(exp, 4),
        "nlongs": len(longs),
        "nshorts": len(shorts),
        "lwr": round(lwr * 100, 2),
        "swr": round(swr * 100, 2),
        "gp": round(gp, 4),
        "gl": round(gl, 4),
    }

# ═══════════════════════════════════════════════════════
# 7. MAIN
# ═══════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("BACKTEST v4 — 30M Strategy, ~150 Coins, 1 Year")
    print(f"Period: {START_DT.date()} → {END_DT.date()}")
    print(f"Interval: {INTERVAL}")
    print(f"Coins to test: {len(COIN_LIST)}")
    print("=" * 60)

    all_results    = []
    all_trades     = []
    skipped_coins  = []
    paused_summary = {10: [], 20: [], 30: []}
    total_open_positions = 0  # portfolio concurrent cap (simplified per-coin sim)

    for idx, symbol in enumerate(COIN_LIST):
        if symbol in SKIP_COINS:
            print(f"  [{idx+1}/{len(COIN_LIST)}] {symbol} — SKIPPED (known issue)")
            skipped_coins.append(symbol)
            continue

        print(f"  [{idx+1}/{len(COIN_LIST)}] {symbol} fetching...", end=" ", flush=True)
        raw = fetch_klines(symbol, INTERVAL, START_MS, END_MS)

        if raw is None or len(raw) < 200:
            print(f"  → SKIPPED (insufficient data: {len(raw) if raw else 0} candles)")
            skipped_coins.append(symbol)
            continue

        print(f"{len(raw)} candles → running strategy...", end=" ", flush=True)
        opens, highs, lows, closes, volumes, times = parse_klines(raw)

        closed_trades, fc, paused_at = run_strategy(
            symbol, opens, highs, lows, closes, volumes, times, INITIAL_BAL
        )

        m = compute_metrics(closed_trades, symbol)
        m["category"]  = CATEGORY_MAP.get(symbol, "Other")
        m["paused_at"] = paused_at
        m["filter_counts"] = fc

        all_results.append(m)
        all_trades.extend(closed_trades)

        if paused_at:
            paused_summary[paused_at].append(symbol)

        status = f"PF={m['pf']:.3f} WR={m['wr']:.1f}% n={m['n']}"
        if paused_at:
            status += f" PAUSED@{paused_at}"
        print(status)

        time.sleep(0.05)

    print("\n" + "=" * 60)
    print("ALL COINS DONE — Computing final report...")
    print("=" * 60)

    # ── Global portfolio metrics ──
    total_trades = len(all_trades)
    if total_trades > 0:
        port_wins   = [t for t in all_trades if t["win"]]
        port_losses = [t for t in all_trades if not t["win"]]
        port_wr     = len(port_wins) / total_trades * 100
        port_gp     = sum(t["pnl"] for t in port_wins)
        port_gl     = sum(abs(t["pnl"]) for t in port_losses)
        port_pf     = port_gp / port_gl if port_gl > 0 else 999
        port_net    = sum(t["pnl"] for t in all_trades)
        port_aw     = port_gp / len(port_wins)   if port_wins   else 0
        port_al     = port_gl / len(port_losses) if port_losses else 0
        port_exp    = port_net / total_trades

        # Portfolio drawdown
        cum = 0.0; peak = 0.0; port_mdd = 0.0
        for t in sorted(all_trades, key=lambda x: x["entry_t"]):
            cum += t["pnl"]
            if cum > peak: peak = cum
            dd = peak - cum
            if dd > port_mdd: port_mdd = dd
    else:
        port_wr = port_pf = port_net = port_aw = port_al = port_exp = port_mdd = 0

    coins_tested  = len(all_results)
    coins_paused  = sum(1 for r in all_results if r["paused_at"] is not None)
    coins_30plus  = sum(1 for r in all_results if r["n"] >= 30)

    # ── Tiering ──
    tier1 = [r for r in all_results if r["wr"] >= 45 and r["pf"] >= 1.5 and r["net"] > 0 and r["n"] >= 15]
    tier2 = [r for r in all_results if r not in tier1 and r["wr"] >= 40 and r["pf"] >= 1.2 and r["net"] > 0 and r["n"] >= 10]
    tier3 = [r for r in all_results if r not in tier1 and r not in tier2]

    tier1.sort(key=lambda x: x["pf"], reverse=True)
    tier2.sort(key=lambda x: x["pf"], reverse=True)
    tier3.sort(key=lambda x: x["pf"], reverse=True)

    # ── Category breakdown ──
    cat_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "gp": 0.0, "gl": 0.0})
    for r in all_results:
        cat = r["category"]
        cat_stats[cat]["trades"] += r["n"]
        cat_stats[cat]["wins"]   += round(r["n"] * r["wr"] / 100)
        cat_stats[cat]["gp"]     += r["gp"]
        cat_stats[cat]["gl"]     += r["gl"]

    # ── Sorted coin lists ──
    sorted_by_pf = sorted([r for r in all_results if r["n"] >= 5], key=lambda x: x["pf"], reverse=True)
    top20    = sorted_by_pf[:20]
    bottom20 = sorted_by_pf[-20:]

    # ── Validation ──
    pf_pass  = sum(1 for r in all_results if r["pf"] >= 1.5)
    wr_pass  = sum(1 for r in all_results if r["wr"] >= 42)
    eligible = len(all_results)

    # ══════════════════════════════════════════
    # WRITE backtest_summary.txt
    # ══════════════════════════════════════════
    lines = []
    def w(s=""): lines.append(s)

    w("=" * 70)
    w("BACKTEST v4 — 30M STRATEGY | ~150 COINS | 1 YEAR")
    w(f"Period : {START_DT.date()} → {END_DT.date()}")
    w(f"Run at : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    w("=" * 70)

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("1. GLOBAL PORTFOLIO METRICS")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  Coins tested           : {coins_tested}")
    w(f"  Coins skipped          : {len(skipped_coins)}")
    w(f"  Coins auto-paused      : {coins_paused}")
    w(f"  Coins with 30+ trades  : {coins_30plus}")
    w(f"  Total Trades           : {total_trades}")
    w(f"  Win Rate               : {port_wr:.2f}%")
    w(f"  Profit Factor          : {port_pf:.4f}")
    w(f"  Total Net PnL          : ${port_net:.2f}")
    w(f"  Max Drawdown           : ${port_mdd:.2f}")
    w(f"  Avg Win                : ${port_aw:.4f}")
    w(f"  Avg Loss               : ${port_al:.4f}")
    w(f"  Expectancy/Trade       : ${port_exp:.4f}")
    w()
    w(f"  TARGET CHECK:")
    w(f"    PF >= 1.5  → {pf_pass}/{eligible} coins pass")
    w(f"    WR >= 42%  → {wr_pass}/{eligible} coins pass")
    w(f"    Portfolio PF {'✅ PASS' if port_pf >= 1.5 else '❌ FAIL'} ({port_pf:.3f})")
    w(f"    Portfolio WR {'✅ PASS' if port_wr >= 42  else '❌ FAIL'} ({port_wr:.1f}%)")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("2. AUTO-DISABLE ANALYSIS")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  Paused at 10-trade mark : {len(paused_summary[10])} coins")
    if paused_summary[10]:
        w(f"    → {', '.join(paused_summary[10])}")
    w(f"  Paused at 20-trade mark : {len(paused_summary[20])} coins")
    if paused_summary[20]:
        w(f"    → {', '.join(paused_summary[20])}")
    w(f"  Paused at 30-trade mark : {len(paused_summary[30])} coins")
    if paused_summary[30]:
        w(f"    → {', '.join(paused_summary[30])}")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("3. CATEGORY PERFORMANCE")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  {'Category':<12} {'Trades':>7} {'WR%':>7} {'PF':>7} {'Net$':>9}")
    w("  " + "-" * 46)
    for cat, cs in sorted(cat_stats.items()):
        if cs["trades"] == 0:
            continue
        cat_wr = cs["wins"] / cs["trades"] * 100 if cs["trades"] else 0
        cat_pf = cs["gp"] / cs["gl"] if cs["gl"] > 0 else 999
        cat_net = cs["gp"] - cs["gl"]
        w(f"  {cat:<12} {cs['trades']:>7} {cat_wr:>7.1f} {cat_pf:>7.3f} {cat_net:>9.2f}")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("4. TOP 20 COINS BY PROFIT FACTOR (≥5 trades)")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  {'Symbol':<16} {'N':>5} {'WR%':>7} {'PF':>7} {'Net$':>9} {'MDD$':>8} {'Cat':<8} {'Paused':>7}")
    w("  " + "-" * 72)
    for r in top20:
        ps = str(r["paused_at"]) if r["paused_at"] else "-"
        w(f"  {r['symbol']:<16} {r['n']:>5} {r['wr']:>7.1f} {r['pf']:>7.3f} {r['net']:>9.4f} {r['mdd']:>8.4f} {r['category']:<8} {ps:>7}")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("5. BOTTOM 20 COINS BY PROFIT FACTOR (≥5 trades)")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  {'Symbol':<16} {'N':>5} {'WR%':>7} {'PF':>7} {'Net$':>9} {'Cat':<8} {'Paused':>7}")
    w("  " + "-" * 60)
    for r in bottom20:
        ps = str(r["paused_at"]) if r["paused_at"] else "-"
        w(f"  {r['symbol']:<16} {r['n']:>5} {r['wr']:>7.1f} {r['pf']:>7.3f} {r['net']:>9.4f} {r['category']:<8} {ps:>7}")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("6. TIER 1 — ELITE COINS (WR≥45%, PF≥1.5, Net>0, 15+trades)")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  Count: {len(tier1)}")
    if tier1:
        w(f"  {'Symbol':<16} {'N':>5} {'WR%':>7} {'PF':>7} {'Net$':>9} {'Cat':<8}")
        w("  " + "-" * 55)
        for r in tier1:
            w(f"  {r['symbol']:<16} {r['n']:>5} {r['wr']:>7.1f} {r['pf']:>7.3f} {r['net']:>9.4f} {r['category']:<8}")
    w()
    w("  TIER 1 WHITELIST (copy-paste):")
    w("  " + ", ".join(r["symbol"] for r in tier1))

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("7. TIER 2 — MONITOR COINS (WR≥40%, PF≥1.2, Net>0, 10+trades)")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  Count: {len(tier2)}")
    if tier2:
        w(f"  {'Symbol':<16} {'N':>5} {'WR%':>7} {'PF':>7} {'Net$':>9} {'Cat':<8}")
        w("  " + "-" * 55)
        for r in tier2:
            w(f"  {r['symbol']:<16} {r['n']:>5} {r['wr']:>7.1f} {r['pf']:>7.3f} {r['net']:>9.4f} {r['category']:<8}")
    w()
    w("  TIER 2 WHITELIST (copy-paste):")
    w("  " + ", ".join(r["symbol"] for r in tier2))

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("8. TIER 3 — ELIMINATED")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  Count: {len(tier3)}")
    w("  (Full list in backtest_report.json)")
    elim_line = ", ".join(r["symbol"] for r in tier3[:40])
    if len(tier3) > 40:
        elim_line += f" ... (+{len(tier3)-40} more)"
    w(f"  {elim_line}")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("9. FILTER REJECTION STATS (AGGREGATE)")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    agg_fc = defaultdict(int)
    for r in all_results:
        for k, v in r["filter_counts"].items():
            agg_fc[k] += v
    for k, v in agg_fc.items():
        w(f"  {k:<30} : {v:>10,}")

    w()
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w("10. FULL PER-COIN TABLE (sorted by PF, ≥5 trades)")
    w("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    w(f"  {'Symbol':<16} {'N':>5} {'WR%':>7} {'PF':>7} {'Net$':>9} {'MDD$':>8} {'Longs':>6} {'Shorts':>7} {'LWR%':>6} {'SWR%':>6} {'Cat':<8} {'Pause':>6}")
    w("  " + "-" * 92)
    for r in sorted_by_pf:
        ps = str(r["paused_at"]) if r["paused_at"] else "-"
        w(f"  {r['symbol']:<16} {r['n']:>5} {r['wr']:>7.1f} {r['pf']:>7.3f} {r['net']:>9.4f} {r['mdd']:>8.4f} {r['nlongs']:>6} {r['nshorts']:>7} {r['lwr']:>6.1f} {r['swr']:>6.1f} {r['category']:<8} {ps:>6}")

    w()
    w("=" * 70)
    w("END OF REPORT")
    w("=" * 70)

    summary_text = "\n".join(lines)
    print(summary_text)

    with open("backtest_summary.txt", "w", encoding="utf-8") as f:
        f.write(summary_text)
    print("\n✅ backtest_summary.txt written")

    # ── Write JSON report ──
    report = {
        "meta": {
            "version": "v4",
            "interval": INTERVAL,
            "start": str(START_DT.date()),
            "end": str(END_DT.date()),
            "coins_requested": len(COIN_LIST),
            "coins_tested": coins_tested,
            "coins_skipped": skipped_coins,
        },
        "portfolio": {
            "total_trades": total_trades,
            "win_rate_pct": round(port_wr, 2),
            "profit_factor": round(port_pf, 4),
            "net_pnl": round(port_net, 4),
            "max_drawdown": round(port_mdd, 4),
            "avg_win": round(port_aw, 4),
            "avg_loss": round(port_al, 4),
            "expectancy": round(port_exp, 4),
            "coins_paused": coins_paused,
            "coins_30plus": coins_30plus,
        },
        "auto_disable": {
            "paused_at_10": paused_summary[10],
            "paused_at_20": paused_summary[20],
            "paused_at_30": paused_summary[30],
        },
        "tiers": {
            "tier1": [r["symbol"] for r in tier1],
            "tier2": [r["symbol"] for r in tier2],
            "tier3": [r["symbol"] for r in tier3],
        },
        "category_stats": {
            cat: {
                "trades": cs["trades"],
                "wr_pct": round(cs["wins"]/cs["trades"]*100, 2) if cs["trades"] else 0,
                "pf": round(cs["gp"]/cs["gl"], 4) if cs["gl"] > 0 else 999,
                "net": round(cs["gp"]-cs["gl"], 4),
            }
            for cat, cs in cat_stats.items() if cs["trades"] > 0
        },
        "per_coin": all_results,
        "aggregate_filters": dict(agg_fc),
    }

    with open("backtest_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print("✅ backtest_report.json written")
    print("\n🏁 DONE.")

if __name__ == "__main__":
    main()
