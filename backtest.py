"""
GMaxV1 Multi-Strategy Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
6 Strategies tested against 117 Universe coins
Period: Aug 2024 – Jul 2026 (or full coin history if shorter)
Timeframe: 15m | Leverage: 5x | SL: 15% | TP max: 3%
Fees: 0.05% + 0.02% slippage per side
Capital: $10,000 | Risk: 0.75% per trade
Shards: 20 | Workers: 16 per shard

STRATEGIES:
  S0 — Baseline:            Fixed TP 3% / SL 15% (original, no changes)
  S1 — EMA Reversal Exit:   Hold until EMA9/21 reverses, TP cap 3%
  S2 — RSI Overbought Exit: Exit when RSI>=75 (long) / <=25 (short), TP cap 3%
  S3 — Signal Score TP:     ADX+slope score → TP tier (max 3%)
  S4 — Tiered Time Exit:    Close stalled trades early, TP cap 3%
  S5 — Breakeven + EMA:     Lock SL to entry at 1.5% profit, exit on EMA reversal, TP cap 3%
"""

import sys, os, json, csv, io, zipfile, time
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Constants ──────────────────────────────────────────────
NUM_SHARDS   = 20
WORKERS      = 16
START_YM     = (2024, 8)
END_YM       = (2026, 7)
TIMEFRAME    = '15m'
CAPITAL      = 10_000.0
RISK_PCT     = 0.0075      # 0.75%
FEE          = 0.0005      # 0.05%
SLIP         = 0.0002      # 0.02%
LEVERAGE     = 5
SL_PCT       = 0.150       # 15%
TP_PCT       = 0.030       # 3% hard cap for all strategies
MAX_BARS     = 960         # 10 days at 15m
MIN_BARS     = 100         # warmup

STRATEGIES   = ['S0','S1','S2','S3','S4','S5']

ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT','AIOTUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ANKRUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT','COMBOUSDT',
    'COMMONUSDT','CRCLUSDT','CUSDT','DAMUSDT','DEFIUSDT','DEXEUSDT','DIAUSDT',
    'DMCUSDT','EIGENUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT',
    'EVAAUSDT','FLNCUSDT','FLUXUSDT','FUNUSDT','FXSUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','IPUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','MTLUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT',
    'PEOPLEUSDT','PIPPINUSDT','PIXELUSDT','PLUMEUSDT','POLUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','PUNDIXUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT',
    'SEIUSDT','SIGNUSDT','SKRUSDT','SNDKUSDT','SOMIUSDT','SPELLUSDT',
    'SPKUSDT','STABLEUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VANRYUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT','WLDUSDT',
    'XEMUSDT','XLMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

# ── Data Fetch ─────────────────────────────────────────────
BASE_URLS = [
    "https://data.binance.vision/data/futures/um/monthly/klines",
    "https://data.binance.com/data/futures/um/monthly/klines",
]

def _parse_zip(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            rows = list(csv.reader(io.TextIOWrapper(f)))
    out = []
    for row in rows:
        if len(row) < 5:
            continue
        try:
            ts = int(row[0])
            if ts > 10**14:
                ts //= 1000
            o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
            out.append((ts, o, h, l, c))
        except (ValueError, IndexError):
            continue
    return out

def fetch_month(symbol, year, month):
    ym = f"{year}-{month:02d}"
    fname = f"{symbol}-{TIMEFRAME}-{ym}.zip"
    for base in BASE_URLS:
        url = f"{base}/{symbol}/{TIMEFRAME}/{fname}"
        for attempt in range(3):
            try:
                with urlopen(url, timeout=40) as r:
                    raw = r.read()
                return _parse_zip(raw)
            except HTTPError as e:
                if e.code == 404:
                    break   # month doesn't exist, try next base
                if attempt < 2:
                    time.sleep(2 ** attempt)  # 1s, 2s backoff
                continue
            except URLError:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                continue
            except Exception:
                break
    return []

def fetch_symbol(symbol):
    sy, sm = START_YM
    ey, em = END_YM
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1

    all_candles = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_month, symbol, y, m): (y, m) for y, m in months}
        for f in as_completed(futs):
            all_candles.extend(f.result())

    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    candles = sorted(seen.values(), key=lambda x: x[0])
    return candles

# ── Indicators ─────────────────────────────────────────────
def ema(values, period):
    if not values:
        return []
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def atr_series(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    if not trs:
        return []
    if len(trs) < period:
        return [sum(trs)/len(trs)] * len(trs)
    out = []
    a = sum(trs[:period]) / period
    out.append(a)
    for t in trs[period:]:
        a = (a*(period-1) + t) / period
        out.append(a)
    # pad front to align with closes index (atr[i] corresponds to closes[i+1])
    return out

def rsi_series(closes, period=14):
    """Returns RSI series aligned to closes index."""
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi_vals = [50.0] * (period + 1)
    for i in range(period, len(gains)):
        ag = (ag*(period-1) + gains[i]) / period
        al = (al*(period-1) + losses[i]) / period
        rs = ag / al if al else 100.0
        rsi_vals.append(100 - (100 / (1 + rs)))
    # pad to same length as closes
    while len(rsi_vals) < len(closes):
        rsi_vals.append(rsi_vals[-1])
    return rsi_vals

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up if up > down and up > 0 else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    def ws(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1]/p + x)
        return r
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st:
        return 0.0, 0.0, 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period:
        return 0.0, pdi[-1] if pdi else 0.0, mdi[-1] if mdi else 0.0
    adx_val = sum(dx[:period]) / period
    for d in dx[period:]:
        adx_val = (adx_val*(period-1) + d) / period
    return max(0.0, min(100.0, adx_val)), pdi[-1], mdi[-1]

# ── Base Signal (shared by all strategies) ─────────────────
def base_signal(i, closes, highs, lows, e9, e21, e50):
    """Returns ('buy'|'sell'|None, adx_val, slope_pct)"""
    if i < 10:
        return None, 0.0, 0.0
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None, 0.0, slope_pct

    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

    if not crossed_up and not crossed_down:
        return None, 0.0, slope_pct
    if trend_up and not crossed_up:
        return None, 0.0, slope_pct
    if trend_down and not crossed_down:
        return None, 0.0, slope_pct

    adx_val, _, _ = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_val < 22:
        return None, adx_val, slope_pct

    sig = 'buy' if crossed_up else 'sell'
    return sig, adx_val, slope_pct

# ── Position Sizing ────────────────────────────────────────
def calc_notional():
    return min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)

# ── Trade Builder ──────────────────────────────────────────
def make_trade(symbol, side, entry_ts, exit_ts, entry_p, exit_p, reason, bars):
    if side == 'buy':
        gross = (exit_p - entry_p) / entry_p
    else:
        gross = (entry_p - exit_p) / entry_p
    net = gross - (FEE + SLIP) * 2
    notional = calc_notional()
    pnl = notional * net * LEVERAGE
    return {
        'symbol':      symbol,
        'side':        side,
        'entry_ts':    entry_ts,
        'exit_ts':     exit_ts,
        'entry_price': entry_p,
        'exit_price':  exit_p,
        'pnl':         round(pnl, 4),
        'reason':      reason,
        'bars':        bars,
    }

# ══════════════════════════════════════════════════════════
# STRATEGY IMPLEMENTATIONS
# ══════════════════════════════════════════════════════════

def backtest_S0(symbol, candles):
    """S0 — Baseline: Fixed TP 3% / SL 15%"""
    if len(candles) < MIN_BARS:
        return []
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    trades = []
    pos = None
    for i in range(MIN_BARS, len(closes) - 1):
        if pos is None:
            sig, adx_val, slope = base_signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                raw_entry = opens[i+1]
                if sig == 'buy':
                    entry_p = raw_entry * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + TP_PCT)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = raw_entry * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - TP_PCT)
                    sl_p = entry_p * (1 + SL_PCT)
                pos = {'side': sig, 'entry_p': entry_p, 'tp_p': tp_p,
                       'sl_p': sl_p, 'entry_ts': ts_arr[i+1], 'entry_bar': i+1}
        else:
            j = i
            side = pos['side']
            hi, lo = highs[j], lows[j]
            bars_held = j - pos['entry_bar']
            hit_sl = lo <= pos['sl_p'] if side == 'buy' else hi >= pos['sl_p']
            hit_tp = hi >= pos['tp_p'] if side == 'buy' else lo <= pos['tp_p']
            reason = exit_p = None
            if hit_sl:
                reason = 'sl'; exit_p = pos['sl_p']
            elif hit_tp:
                reason = 'tp'; exit_p = pos['tp_p']
            elif bars_held >= MAX_BARS:
                reason = 'max_hold'; exit_p = closes[j]
            if reason:
                trades.append(make_trade(symbol, side, pos['entry_ts'],
                    ts_arr[j], pos['entry_p'], exit_p, reason, bars_held))
                pos = None
    if pos:
        j = len(closes) - 1
        trades.append(make_trade(symbol, pos['side'], pos['entry_ts'],
            ts_arr[j], pos['entry_p'], closes[j], 'end_of_data',
            j - pos['entry_bar']))
    return trades


def backtest_S1(symbol, candles):
    """S1 — EMA9/21 Reversal Exit: hold until cross reverses, TP cap 3%"""
    if len(candles) < MIN_BARS:
        return []
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    trades = []
    pos = None
    for i in range(MIN_BARS, len(closes) - 1):
        if pos is None:
            sig, adx_val, slope = base_signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                raw_entry = opens[i+1]
                if sig == 'buy':
                    entry_p = raw_entry * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + TP_PCT)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = raw_entry * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - TP_PCT)
                    sl_p = entry_p * (1 + SL_PCT)
                pos = {'side': sig, 'entry_p': entry_p, 'tp_p': tp_p,
                       'sl_p': sl_p, 'entry_ts': ts_arr[i+1], 'entry_bar': i+1}
        else:
            j = i
            side = pos['side']
            hi, lo = highs[j], lows[j]
            bars_held = j - pos['entry_bar']
            hit_sl = lo <= pos['sl_p'] if side == 'buy' else hi >= pos['sl_p']
            hit_tp = hi >= pos['tp_p'] if side == 'buy' else lo <= pos['tp_p']
            # EMA reversal check on closed bar
            ema_reversed = False
            if side == 'buy':
                ema_reversed = e9[j] < e21[j] and e9[j-1] >= e21[j-1]
            else:
                ema_reversed = e9[j] > e21[j] and e9[j-1] <= e21[j-1]
            reason = exit_p = None
            if hit_sl:
                reason = 'sl'; exit_p = pos['sl_p']
            elif hit_tp:
                reason = 'tp'; exit_p = pos['tp_p']
            elif ema_reversed:
                reason = 'ema_reversal'; exit_p = closes[j]
            elif bars_held >= MAX_BARS:
                reason = 'max_hold'; exit_p = closes[j]
            if reason:
                trades.append(make_trade(symbol, side, pos['entry_ts'],
                    ts_arr[j], pos['entry_p'], exit_p, reason, bars_held))
                pos = None
    if pos:
        j = len(closes) - 1
        trades.append(make_trade(symbol, pos['side'], pos['entry_ts'],
            ts_arr[j], pos['entry_p'], closes[j], 'end_of_data',
            j - pos['entry_bar']))
    return trades


def backtest_S2(symbol, candles):
    """S2 — RSI Overbought/Oversold Exit: exit RSI>=75 long / <=25 short, TP cap 3%"""
    if len(candles) < MIN_BARS:
        return []
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    rsi = rsi_series(closes, 14)
    trades = []
    pos = None
    for i in range(MIN_BARS, len(closes) - 1):
        if pos is None:
            sig, adx_val, slope = base_signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                raw_entry = opens[i+1]
                if sig == 'buy':
                    entry_p = raw_entry * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + TP_PCT)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = raw_entry * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - TP_PCT)
                    sl_p = entry_p * (1 + SL_PCT)
                pos = {'side': sig, 'entry_p': entry_p, 'tp_p': tp_p,
                       'sl_p': sl_p, 'entry_ts': ts_arr[i+1], 'entry_bar': i+1}
        else:
            j = i
            side = pos['side']
            hi, lo = highs[j], lows[j]
            bars_held = j - pos['entry_bar']
            hit_sl = lo <= pos['sl_p'] if side == 'buy' else hi >= pos['sl_p']
            hit_tp = hi >= pos['tp_p'] if side == 'buy' else lo <= pos['tp_p']
            rsi_exit = False
            if side == 'buy' and rsi[j] >= 75:
                rsi_exit = True
            elif side == 'sell' and rsi[j] <= 25:
                rsi_exit = True
            reason = exit_p = None
            if hit_sl:
                reason = 'sl'; exit_p = pos['sl_p']
            elif hit_tp:
                reason = 'tp'; exit_p = pos['tp_p']
            elif rsi_exit:
                reason = 'rsi_exit'; exit_p = closes[j]
            elif bars_held >= MAX_BARS:
                reason = 'max_hold'; exit_p = closes[j]
            if reason:
                trades.append(make_trade(symbol, side, pos['entry_ts'],
                    ts_arr[j], pos['entry_p'], exit_p, reason, bars_held))
                pos = None
    if pos:
        j = len(closes) - 1
        trades.append(make_trade(symbol, pos['side'], pos['entry_ts'],
            ts_arr[j], pos['entry_p'], closes[j], 'end_of_data',
            j - pos['entry_bar']))
    return trades


def backtest_S3(symbol, candles):
    """S3 — Signal Score TP Tier: ADX+slope → TP tier (max 3%)"""
    if len(candles) < MIN_BARS:
        return []
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    trades = []
    pos = None
    for i in range(MIN_BARS, len(closes) - 1):
        if pos is None:
            sig, adx_val, slope = base_signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                # Score: ADX tier
                if adx_val >= 40:
                    adx_score = 3
                elif adx_val >= 30:
                    adx_score = 2
                else:
                    adx_score = 1
                # Score: slope tier
                abs_slope = abs(slope)
                if abs_slope >= 0.40:
                    slope_score = 2
                elif abs_slope >= 0.15:
                    slope_score = 1
                else:
                    slope_score = 0
                total_score = adx_score + slope_score  # range 1–5
                # TP tiers — capped at 3%
                if total_score >= 4:
                    tp_use = 0.030   # 3.0%
                elif total_score == 3:
                    tp_use = 0.025   # 2.5%
                elif total_score == 2:
                    tp_use = 0.020   # 2.0%
                else:
                    tp_use = 0.015   # 1.5% (weakest signal)
                raw_entry = opens[i+1]
                if sig == 'buy':
                    entry_p = raw_entry * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + tp_use)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = raw_entry * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - tp_use)
                    sl_p = entry_p * (1 + SL_PCT)
                pos = {'side': sig, 'entry_p': entry_p, 'tp_p': tp_p,
                       'sl_p': sl_p, 'entry_ts': ts_arr[i+1], 'entry_bar': i+1,
                       'tp_use': tp_use}
        else:
            j = i
            side = pos['side']
            hi, lo = highs[j], lows[j]
            bars_held = j - pos['entry_bar']
            hit_sl = lo <= pos['sl_p'] if side == 'buy' else hi >= pos['sl_p']
            hit_tp = hi >= pos['tp_p'] if side == 'buy' else lo <= pos['tp_p']
            reason = exit_p = None
            if hit_sl:
                reason = 'sl'; exit_p = pos['sl_p']
            elif hit_tp:
                reason = 'tp'; exit_p = pos['tp_p']
            elif bars_held >= MAX_BARS:
                reason = 'max_hold'; exit_p = closes[j]
            if reason:
                trades.append(make_trade(symbol, side, pos['entry_ts'],
                    ts_arr[j], pos['entry_p'], exit_p, reason, bars_held))
                pos = None
    if pos:
        j = len(closes) - 1
        trades.append(make_trade(symbol, pos['side'], pos['entry_ts'],
            ts_arr[j], pos['entry_p'], closes[j], 'end_of_data',
            j - pos['entry_bar']))
    return trades


def backtest_S4(symbol, candles):
    """S4 — Tiered Time Exit: close stalled trades early, TP cap 3%
    Rules:
      - If bars_held >= 96  (24h)  and current_pnl_pct < 0.5%  -> close
      - If bars_held >= 192 (48h)  and current_pnl_pct < 1.0%  -> close
      - If bars_held >= 384 (96h)  and current_pnl_pct < 1.5%  -> close
    """
    if len(candles) < MIN_BARS:
        return []
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    trades = []
    pos = None
    for i in range(MIN_BARS, len(closes) - 1):
        if pos is None:
            sig, adx_val, slope = base_signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                raw_entry = opens[i+1]
                if sig == 'buy':
                    entry_p = raw_entry * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + TP_PCT)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = raw_entry * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - TP_PCT)
                    sl_p = entry_p * (1 + SL_PCT)
                pos = {'side': sig, 'entry_p': entry_p, 'tp_p': tp_p,
                       'sl_p': sl_p, 'entry_ts': ts_arr[i+1], 'entry_bar': i+1}
        else:
            j = i
            side = pos['side']
            hi, lo = highs[j], lows[j]
            bars_held = j - pos['entry_bar']
            hit_sl = lo <= pos['sl_p'] if side == 'buy' else hi >= pos['sl_p']
            hit_tp = hi >= pos['tp_p'] if side == 'buy' else lo <= pos['tp_p']
            # Tiered stall check using current close
            cur = closes[j]
            ep  = pos['entry_p']
            if side == 'buy':
                cur_pct = (cur - ep) / ep
            else:
                cur_pct = (ep - cur) / ep
            stall_exit = False
            if   bars_held >= 384 and cur_pct < 0.015:
                stall_exit = True
            elif bars_held >= 192 and cur_pct < 0.010:
                stall_exit = True
            elif bars_held >= 96  and cur_pct < 0.005:
                stall_exit = True
            reason = exit_p = None
            if hit_sl:
                reason = 'sl'; exit_p = pos['sl_p']
            elif hit_tp:
                reason = 'tp'; exit_p = pos['tp_p']
            elif stall_exit:
                reason = 'stall_exit'; exit_p = closes[j]
            elif bars_held >= MAX_BARS:
                reason = 'max_hold'; exit_p = closes[j]
            if reason:
                trades.append(make_trade(symbol, side, pos['entry_ts'],
                    ts_arr[j], pos['entry_p'], exit_p, reason, bars_held))
                pos = None
    if pos:
        j = len(closes) - 1
        trades.append(make_trade(symbol, pos['side'], pos['entry_ts'],
            ts_arr[j], pos['entry_p'], closes[j], 'end_of_data',
            j - pos['entry_bar']))
    return trades


def backtest_S5(symbol, candles):
    """S5 — Breakeven Lock + EMA Reversal:
    Once trade reaches 1.5% profit -> move SL to entry (one-time, static).
    Exit when EMA9/21 reverses OR TP 3% OR original SL.
    """
    if len(candles) < MIN_BARS:
        return []
    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    trades = []
    pos = None
    for i in range(MIN_BARS, len(closes) - 1):
        if pos is None:
            sig, adx_val, slope = base_signal(i, closes, highs, lows, e9, e21, e50)
            if sig:
                raw_entry = opens[i+1]
                if sig == 'buy':
                    entry_p = raw_entry * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + TP_PCT)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = raw_entry * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - TP_PCT)
                    sl_p = entry_p * (1 + SL_PCT)
                pos = {'side': sig, 'entry_p': entry_p, 'tp_p': tp_p,
                       'sl_p': sl_p, 'entry_ts': ts_arr[i+1], 'entry_bar': i+1,
                       'be_locked': False}
        else:
            j = i
            side = pos['side']
            hi, lo = highs[j], lows[j]
            bars_held = j - pos['entry_bar']
            ep = pos['entry_p']

            # Breakeven lock: if 1.5% profit reached and not yet locked
            if not pos['be_locked']:
                if side == 'buy' and hi >= ep * 1.015:
                    pos['sl_p'] = ep   # move SL to breakeven
                    pos['be_locked'] = True
                elif side == 'sell' and lo <= ep * 0.985:
                    pos['sl_p'] = ep
                    pos['be_locked'] = True

            hit_sl = lo <= pos['sl_p'] if side == 'buy' else hi >= pos['sl_p']
            hit_tp = hi >= pos['tp_p'] if side == 'buy' else lo <= pos['tp_p']
            # EMA reversal exit
            ema_reversed = False
            if side == 'buy':
                ema_reversed = e9[j] < e21[j] and e9[j-1] >= e21[j-1]
            else:
                ema_reversed = e9[j] > e21[j] and e9[j-1] <= e21[j-1]
            reason = exit_p = None
            if hit_sl:
                reason = 'sl' if not pos['be_locked'] else 'breakeven'
                exit_p = pos['sl_p']
            elif hit_tp:
                reason = 'tp'; exit_p = pos['tp_p']
            elif ema_reversed:
                reason = 'ema_reversal'; exit_p = closes[j]
            elif bars_held >= MAX_BARS:
                reason = 'max_hold'; exit_p = closes[j]
            if reason:
                trades.append(make_trade(symbol, side, pos['entry_ts'],
                    ts_arr[j], pos['entry_p'], exit_p, reason, bars_held))
                pos = None
    if pos:
        j = len(closes) - 1
        trades.append(make_trade(symbol, pos['side'], pos['entry_ts'],
            ts_arr[j], pos['entry_p'], closes[j], 'end_of_data',
            j - pos['entry_bar']))
    return trades


STRATEGY_FNS = {
    'S0': backtest_S0,
    'S1': backtest_S1,
    'S2': backtest_S2,
    'S3': backtest_S3,
    'S4': backtest_S4,
    'S5': backtest_S5,
}

STRATEGY_NAMES = {
    'S0': 'Baseline — Fixed TP 3% / SL 15%',
    'S1': 'EMA9/21 Reversal Exit (TP cap 3%)',
    'S2': 'RSI Overbought/Oversold Exit (TP cap 3%)',
    'S3': 'Signal Score TP Tier (max 3%)',
    'S4': 'Tiered Time Stall Exit (TP cap 3%)',
    'S5': 'Breakeven Lock + EMA Reversal Exit (TP cap 3%)',
}

# ── Stats ──────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
            'net_pnl': 0.0, 'max_drawdown': 0.0,
            'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }
    wins = losses = longs = shorts = 0
    gp = gl = net_pnl = 0.0
    win_pnls = []; loss_pnls = []
    monthly = {}; per_coin = {}

    sorted_trades = sorted(trades, key=lambda t: t['entry_ts'])
    equity = 0.0; peak = 0.0; max_dd = 0.0

    for t in sorted_trades:
        pnl = t['pnl']
        net_pnl += pnl
        equity += pnl
        if equity > peak:
            peak = equity
        dd = (peak - equity) / max(abs(peak), 1) * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

        if pnl > 0:
            wins += 1; gp += pnl; win_pnls.append(pnl)
        elif pnl < 0:
            losses += 1; gl += abs(pnl); loss_pnls.append(pnl)

        if t['side'] == 'buy':
            longs += 1
        else:
            shorts += 1

        # Monthly
        dt = datetime.utcfromtimestamp(t['entry_ts'] / 1000)
        mk = dt.strftime('%Y-%m')
        if mk not in monthly:
            monthly[mk] = {'pnl': 0.0, 'n': 0, 'w': 0}
        monthly[mk]['pnl'] += pnl
        monthly[mk]['n'] += 1
        if pnl > 0:
            monthly[mk]['w'] += 1

        # Per coin
        sym = t['symbol']
        if sym not in per_coin:
            per_coin[sym] = {'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0}
        per_coin[sym]['pnl'] += pnl
        per_coin[sym]['n'] += 1
        if pnl > 0:
            per_coin[sym]['w'] += 1

    total = wins + losses
    for sym in per_coin:
        d = per_coin[sym]
        d['wr'] = round(d['w'] / d['n'] * 100, 1) if d['n'] else 0.0
        d['pnl'] = round(d['pnl'], 4)

    pf = (gp / gl) if gl > 0 else (float('inf') if gp > 0 else 0.0)
    avg_win  = sum(win_pnls)  / len(win_pnls)  if win_pnls  else 0.0
    avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
    expectancy = (wins/total * avg_win + losses/total * avg_loss) if total else 0.0

    return {
        'total':         total,
        'win_rate':      round(wins / total * 100, 2) if total else 0.0,
        'profit_factor': round(pf, 4),
        'net_pnl':       round(net_pnl, 4),
        'max_drawdown':  round(max_dd, 2),
        'avg_win':       round(avg_win, 4),
        'avg_loss':      round(avg_loss, 4),
        'expectancy':    round(expectancy, 4),
        'longs':         longs,
        'shorts':        shorts,
        'monthly':       {k: {kk: round(vv,4) if isinstance(vv,float) else vv
                              for kk,vv in v.items()}
                          for k,v in sorted(monthly.items())},
        'per_coin':      per_coin,
    }

# ── Shard Runner ───────────────────────────────────────────
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} coins: {symbols}", flush=True)

    # Fetch all candles in parallel
    coin_candles = {}
    def fetch_one(sym):
        c = fetch_symbol(sym)
        return sym, c

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_one, sym): sym for sym in symbols}
        for f in as_completed(futs):
            sym, candles = f.result()
            coin_candles[sym] = candles
            print(f"[Shard {shard_idx}] {sym}: {len(candles)} candles", flush=True)

    # Check for geo-block
    total_candles = sum(len(v) for v in coin_candles.values())
    if total_candles == 0:
        print(f"[Shard {shard_idx}] ERROR: 0 candles fetched — possible geo-block!", flush=True)
        # Write empty shard
        result = {'shard': shard_idx, 'symbols': symbols, 'with_data': [],
                  'strategies': {}, 'elapsed': 0.0, 'error': 'geo_block'}
        with open(f'shard_{shard_idx}.json', 'w') as f:
            json.dump(result, f)
        return

    t0 = time.time()
    with_data = [sym for sym, c in coin_candles.items() if len(c) >= MIN_BARS]

    strategy_results = {}
    for strat in STRATEGIES:
        fn = STRATEGY_FNS[strat]
        all_trades = []
        for sym in with_data:
            trades = fn(sym, coin_candles[sym])
            all_trades.extend(trades)
        strategy_results[strat] = {
            'trades': all_trades,
            'stats':  stats(all_trades),
        }
        print(f"[Shard {shard_idx}] {strat}: {len(all_trades)} trades", flush=True)

    elapsed = round(time.time() - t0, 2)
    result = {
        'shard':      shard_idx,
        'symbols':    symbols,
        'with_data':  with_data,
        'strategies': strategy_results,
        'elapsed':    elapsed,
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(result, f)
    print(f"[Shard {shard_idx}] done in {elapsed}s", flush=True)

# ── Merge ──────────────────────────────────────────────────
def merge_shards():
    all_strategy_trades = {s: [] for s in STRATEGIES}
    all_with_data = set()
    all_symbols = set()
    shard_errors = []

    for idx in range(NUM_SHARDS):
        fname = f'shard_{idx}.json'
        if not os.path.exists(fname):
            print(f"WARNING: {fname} missing", flush=True)
            continue
        with open(fname) as f:
            shard = json.load(f)
        if shard.get('error'):
            shard_errors.append(idx)
            continue
        all_symbols.update(shard.get('symbols', []))
        all_with_data.update(shard.get('with_data', []))
        for strat in STRATEGIES:
            if strat in shard.get('strategies', {}):
                all_strategy_trades[strat].extend(
                    shard['strategies'][strat].get('trades', []))

    # Build combined stats per strategy
    combined = {}
    for strat in STRATEGIES:
        combined[strat] = {
            'name':   STRATEGY_NAMES[strat],
            'trades': all_strategy_trades[strat],
            'stats':  stats(all_strategy_trades[strat]),
        }

    report = {
        'period':       f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe':    TIMEFRAME,
        'symbols_attempted': len(all_symbols),
        'symbols_with_data': len(all_with_data),
        'capital':      CAPITAL,
        'risk_pct':     RISK_PCT,
        'leverage':     LEVERAGE,
        'fee':          FEE,
        'slip':         SLIP,
        'tp_cap':       TP_PCT,
        'sl_pct':       SL_PCT,
        'shard_errors': shard_errors,
        'strategies':   combined,
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # ── Summary Text ───────────────────────────────────────
    lines = []
    lines.append("=" * 70)
    lines.append("  GMaxV1 MULTI-STRATEGY BACKTEST REPORT")
    lines.append("=" * 70)
    lines.append(f"  Period    : {report['period']}")
    lines.append(f"  Timeframe : {TIMEFRAME} | Leverage: {LEVERAGE}x")
    lines.append(f"  Capital   : ${CAPITAL:,.0f} | Risk: {RISK_PCT*100:.2f}% per trade")
    lines.append(f"  Fees      : {FEE*100:.2f}% + {SLIP*100:.2f}% slip per side")
    lines.append(f"  TP Cap    : {TP_PCT*100:.1f}% | SL: {SL_PCT*100:.1f}%")
    lines.append(f"  Coins     : {len(all_with_data)}/{len(all_symbols)} had sufficient data")
    if shard_errors:
        lines.append(f"  ⚠ Shard errors: {shard_errors}")
    lines.append("")

    for strat in STRATEGIES:
        d  = combined[strat]
        st = d['stats']
        lines.append("-" * 70)
        lines.append(f"  [{strat}] {d['name']}")
        lines.append("-" * 70)
        lines.append(f"  Trades       : {st['total']}  |  Longs: {st['longs']}  Shorts: {st['shorts']}")
        lines.append(f"  Win Rate     : {st['win_rate']:.2f}%")
        lines.append(f"  Profit Factor: {st['profit_factor']:.4f}")
        lines.append(f"  Net PnL      : ${st['net_pnl']:,.2f}")
        lines.append(f"  Max Drawdown : {st['max_drawdown']:.2f}%")
        lines.append(f"  Avg Win      : ${st['avg_win']:.2f}  |  Avg Loss: ${st['avg_loss']:.2f}")
        lines.append(f"  Expectancy   : ${st['expectancy']:.4f}")

        # Exit reason breakdown
        reason_counts = {}
        for t in d['trades']:
            r = t['reason']
            reason_counts[r] = reason_counts.get(r, 0) + 1
        if reason_counts:
            lines.append(f"  Exit Reasons : " +
                "  ".join(f"{k}={v}" for k,v in sorted(reason_counts.items())))

        # Recommendation
        usable = st['profit_factor'] >= 1.5 and st['win_rate'] >= 42
        lines.append(f"  RECOMMENDATION: {'✅ USABLE' if usable else '❌ NOT USABLE'}"
                     f"  (PF≥1.5 and WR≥42% required)")

        # Top 20 coins by net PnL
        per_coin = st['per_coin']
        top_coins = sorted(per_coin.items(), key=lambda x: x[1]['pnl'], reverse=True)[:20]
        if top_coins:
            lines.append("")
            lines.append(f"  Top 20 Coins by Net PnL:")
            lines.append(f"  {'Symbol':<22} {'Trades':>6} {'WR%':>7} {'PnL':>10}")
            lines.append(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*10}")
            for sym, cd in top_coins:
                lines.append(f"  {sym:<22} {cd['n']:>6} {cd['wr']:>6.1f}% ${cd['pnl']:>9.2f}")

        # Monthly PnL
        monthly = st['monthly']
        if monthly:
            lines.append("")
            lines.append(f"  Monthly PnL:")
            lines.append(f"  {'Month':<10} {'Trades':>6} {'Wins':>5} {'PnL':>10}")
            lines.append(f"  {'-'*10} {'-'*6} {'-'*5} {'-'*10}")
            for mk in sorted(monthly.keys()):
                md = monthly[mk]
                lines.append(f"  {mk:<10} {md['n']:>6} {md['w']:>5} ${md['pnl']:>9.2f}")
        lines.append("")

    # Strategy comparison table
    lines.append("=" * 70)
    lines.append("  STRATEGY COMPARISON")
    lines.append("=" * 70)
    lines.append(f"  {'ID':<4} {'Trades':>7} {'WR%':>7} {'PF':>7} {'Net PnL':>12} {'MaxDD%':>8}")
    lines.append(f"  {'-'*4} {'-'*7} {'-'*7} {'-'*7} {'-'*12} {'-'*8}")
    for strat in STRATEGIES:
        st = combined[strat]['stats']
        lines.append(
            f"  {strat:<4} {st['total']:>7} {st['win_rate']:>6.1f}% "
            f"{st['profit_factor']:>7.4f} ${st['net_pnl']:>11.2f} {st['max_drawdown']:>7.2f}%"
        )
    lines.append("=" * 70)

    summary_text = "\n".join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary_text)

    print(summary_text, flush=True)

    # Zip both outputs
    with zipfile.ZipFile('backtest_results.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write('backtest_report.json')
        zf.write('backtest_summary.txt')
    print("\n✅ backtest_results.zip created (backtest_report.json + backtest_summary.txt)", flush=True)

# ── Entry Point ────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_index|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

