"""
4-Strategy Backtest — GMax Universe (117 coins)
Period  : 2025-01 → 2026-07  (1.5 years)
Capital : $10,000 | Leverage: 5x | Risk/trade: 0.75%
Fees    : 0.05% entry + 0.02% slip (both sides)
Strategies:
  S1 — EMA Ribbon Squeeze + Volume (15m)
  S2 — Opening Range Breakout (5m)
  S3 — RSI(2) Extreme Reversion (15m)
  S4 — Candle Body Momentum (15m)
stdlib only — no pip installs
"""

import sys, os, json, csv, io, zipfile, time
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ── Constants ──────────────────────────────────────────────────────────────────
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

NUM_SHARDS = 8
WORKERS    = 16

# Date range: 1.5 years ending July 2026
START_YM = (2025, 1)
END_YM   = (2026, 7)

# Capital & risk
CAPITAL  = 10_000.0
RISK_PCT = 0.0075    # 0.75% risk per trade
LEVERAGE = 5
FEE      = 0.0005    # 0.05% per side
SLIP     = 0.0002    # 0.02% per side

# Strategy parameters
STRATEGIES = {
    'S1_Ribbon': {
        'tf'      : '15m',
        'tp'      : 0.010,   # 1.0%
        'sl'      : 0.005,   # 0.5%
        'max_bars': 48,
        'min_bars': 100,     # warmup: EMA34 needs ~34, ADX needs ~42 — use 100 to be safe
    },
    'S2_ORB': {
        'tf'      : '5m',
        'tp'      : 0.015,   # 1.5%
        'sl'      : 0.005,   # 0.5%
        'max_bars': 36,
        'min_bars': 80,      # warmup: ADX14 × 3 = 42, padding to 80
    },
    'S3_RSI2': {
        'tf'      : '15m',
        'tp'      : 0.012,   # 1.2%
        'sl'      : 0.006,   # 0.6%
        'max_bars': 24,
        'min_bars': 250,     # warmup: EMA200 needs 200 bars minimum
    },
    'S4_BodyMom': {
        'tf'      : '15m',
        'tp'      : 0.010,   # 1.0%
        'sl'      : 0.005,   # 0.5%
        'max_bars': 32,
        'min_bars': 100,     # warmup: EMA50 + ADX14
    },
}


# ── Data fetch ─────────────────────────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month, tf):
    """Fetch one month of OHLCV for symbol/tf. Returns list of (ts,o,h,l,c,v) or []."""
    url = f"{BASE_URL}/{symbol}/{tf}/{symbol}-{tf}-{year}-{month:02d}.zip"
    try:
        req  = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = urlopen(req, timeout=30).read()
        zf   = zipfile.ZipFile(io.BytesIO(data))
        name = zf.namelist()[0]
        raw  = zf.read(name).decode("utf-8")
        rows = list(csv.reader(io.StringIO(raw)))
        out  = []
        for r in rows:
            if len(r) < 6: continue
            try:
                ts = int(r[0])
                # convert microseconds to ms if needed
                if ts > 10**14: ts //= 1000
                out.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
            except (ValueError, IndexError):
                continue
        return out
    except HTTPError:
        return []
    except Exception:
        return []

def fetch_symbol(symbol, tf):
    """Fetch all months in range. Dedup by ts, sort ascending."""
    y, m = START_YM
    ey, em = END_YM
    months = []
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12: m = 1; y += 1

    all_candles = []
    for yr, mo in months:
        all_candles.extend(fetch_month(symbol, yr, mo, tf))

    # dedup by timestamp, sort
    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    return sorted(seen.values(), key=lambda x: x[0])


# ── Indicators ─────────────────────────────────────────────────────────────────
# All match GMaxV1.py exactly

def ema(values, period):
    """EMA seeded from first value — matches GMaxV1 live bot."""
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    """Wilder-smoothed ADX — exact copy from GMaxV1."""
    if len(closes) < period * 3:
        return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    def ws(v, p):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st: return 0.0, 0.0, 0.0
    pdi = [100*p/t if t else 0 for p,t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m,t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0, pdi[-1], mdi[-1]
    adx_v = sum(dx[:period]) / period
    for d in dx[period:]: adx_v = (adx_v*(period-1) + d) / period
    return max(0.0, min(100.0, adx_v)), pdi[-1], mdi[-1]

def rsi_period(closes, period):
    """RSI with arbitrary period (used for RSI-2)."""
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0)); losses.append(max(-d, 0.0))
    if len(gains) < period: return 50.0
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag*(period-1) + gains[i]) / period
        al = (al*(period-1) + losses[i]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))


# ── Signal functions ───────────────────────────────────────────────────────────

def signal_S1_ribbon(i, opens, highs, lows, closes, vols):
    """
    EMA Ribbon Squeeze + Volume on 15m.
    Squeeze = all 5 EMAs within 0.3% of each other.
    Entry when squeeze just resolved + ribbon aligned + vol>1.5x + ADX>=25.
    Signal on bar i, entry on bar i+1 open.
    """
    # need at least 50 bars for EMA34 to be meaningful
    if i < 50: return None

    c_slice = closes[:i+1]
    e5  = ema(c_slice, 5)
    e8  = ema(c_slice, 8)
    e13 = ema(c_slice, 13)
    e21 = ema(c_slice, 21)
    e34 = ema(c_slice, 34)

    # Check squeeze on previous bar (i-1)
    prev = [e5[-2], e8[-2], e13[-2], e21[-2], e34[-2]]
    prev_mid = sum(prev) / 5
    if prev_mid == 0: return None
    prev_squeeze = (max(prev) - min(prev)) / prev_mid < 0.003

    # Check squeeze on current bar (i)
    cur = [e5[-1], e8[-1], e13[-1], e21[-1], e34[-1]]
    cur_mid = sum(cur) / 5
    if cur_mid == 0: return None
    cur_squeeze = (max(cur) - min(cur)) / cur_mid < 0.003

    # Must have been in squeeze and just broken out
    if not (prev_squeeze and not cur_squeeze): return None

    # Ribbon fully aligned
    long_align  = e5[-1] > e8[-1] > e13[-1] > e21[-1] > e34[-1]
    short_align = e5[-1] < e8[-1] < e13[-1] < e21[-1] < e34[-1]
    if not long_align and not short_align: return None

    # Volume: current bar vs 20-bar avg of the bars BEFORE current bar
    start_v = max(0, i - 20)
    avg_vol  = sum(vols[start_v:i]) / max(1, i - start_v)
    if avg_vol == 0: return None
    if vols[i] < 1.5 * avg_vol: return None

    # ADX >= 25
    adx_v, _, _ = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_v < 25: return None

    return 'buy' if long_align else 'sell'


def signal_S2_orb(i, opens, highs, lows, closes, vols, day_ranges, day_traded):
    """
    Opening Range Breakout on 5m.
    day_ranges: dict { day_key: (or_high, or_low, avg_vol_OR) }
    day_traded: set of day_keys that already had a trade for this symbol
    Signal on bar i (must be bar 4+ of the day), entry on bar i+1 open.
    """
    ts      = int(opens[i])   # we pass timestamps as opens array trick — see backtest_S2
    day_key = ts // 86400000

    if day_key not in day_ranges: return None
    if day_key in day_traded:     return None

    or_high, or_low, or_avg_vol = day_ranges[day_key]
    if or_high == or_low: return None

    # breakout: close breaks above OR high + 0.1% buffer
    buf_high = or_high * 1.001
    buf_low  = or_low  * 0.999

    sig = None
    if closes[i] > buf_high:
        sig = 'buy'
    elif closes[i] < buf_low:
        sig = 'sell'
    if sig is None: return None

    # Volume check
    start_v = max(0, i - 20)
    avg_vol  = sum(vols[start_v:i]) / max(1, i - start_v)
    if avg_vol == 0: return None
    if vols[i] < 2.0 * avg_vol: return None

    # ADX >= 22
    adx_v, _, _ = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_v < 22: return None

    return sig


def signal_S3_rsi2(i, opens, highs, lows, closes, vols):
    """
    RSI(2) Extreme Reversion on 15m.
    Long: RSI(2) < 5 for 2 consecutive bars AND above EMA200.
    Short: RSI(2) > 95 for 2 consecutive bars AND below EMA200.
    ADX between 15 and 35.
    """
    if i < 220: return None   # need EMA200 warm up

    rsi_cur  = rsi_period(closes[:i+1], 2)
    rsi_prev = rsi_period(closes[:i],   2)

    # EMA200
    e200 = ema(closes[:i+1], 200)
    price = closes[i]

    # ADX between 15 and 35 (avoid flat AND violent trend)
    adx_v, _, _ = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if not (15 <= adx_v <= 35): return None

    if rsi_prev < 5 and rsi_cur < 5 and price > e200[-1]:
        return 'buy'
    if rsi_prev > 95 and rsi_cur > 95 and price < e200[-1]:
        return 'sell'
    return None


def signal_S4_body(i, opens, highs, lows, closes, vols):
    """
    Candle Body Momentum on 15m.
    Body ratio >= 0.75 + EMA21/50 aligned + ADX>=22 + vol>=1.2x.
    Next candle confirmation: bar[i+1] open within 0.2% of bar[i] close.
    NOTE: this signal is evaluated on bar i-1, confirmed on bar i open.
    We pass i as the CONFIRMATION bar. Signal bar = i-1.
    """
    if i < 60: return None
    sig_bar = i - 1   # the strong body candle
    if sig_bar < 1: return None

    o  = opens[sig_bar]
    h  = highs[sig_bar]
    l  = lows[sig_bar]
    c  = closes[sig_bar]
    rng = h - l
    if rng == 0: return None
    body = abs(c - o)
    ratio = body / rng
    if ratio < 0.75: return None

    # EMA21, EMA50 on signal bar
    e21 = ema(closes[:sig_bar+1], 21)
    e50 = ema(closes[:sig_bar+1], 50)

    # ADX on signal bar
    adx_v, _, _ = adx_calc(highs[:sig_bar+1], lows[:sig_bar+1], closes[:sig_bar+1], 14)
    if adx_v < 22: return None

    # Volume on signal bar vs 20-bar avg before signal bar
    start_v = max(0, sig_bar - 20)
    avg_vol  = sum(vols[start_v:sig_bar]) / max(1, sig_bar - start_v)
    if avg_vol == 0: return None
    if vols[sig_bar] < 1.2 * avg_vol: return None

    # Direction
    bull = c > o and c > e21[-1] and e21[-1] > e50[-1]
    bear = c < o and c < e21[-1] and e21[-1] < e50[-1]
    if not bull and not bear: return None

    # Confirmation: bar i open must be within 0.2% of signal bar close
    conf_open = opens[i]
    if c == 0: return None
    if abs(conf_open - c) / c > 0.002: return None

    return 'buy' if bull else 'sell'


# ── Single-symbol backtest ─────────────────────────────────────────────────────

def _position_size(sl_pct):
    """Risk-based notional capped by leverage."""
    if sl_pct <= 0: return 0.0
    return min(CAPITAL * RISK_PCT / sl_pct, CAPITAL * LEVERAGE)

def backtest_15m(symbol, candles, strat_name, cfg):
    """Generic 15m backtest runner for S1, S3, S4."""
    TP      = cfg['tp']
    SL      = cfg['sl']
    MAX_B   = cfg['max_bars']
    MIN_B   = cfg['min_bars']
    trades  = []

    n = len(candles)
    if n < MIN_B + 2: return trades

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    vols   = [c[5] for c in candles]

    notional = _position_size(SL)
    if notional <= 0: return trades

    in_trade   = False
    side       = None
    entry_price = 0.0
    entry_ts   = 0
    entry_bar  = 0

    for i in range(MIN_B, n - 1):
        # ── Manage open position ──
        if in_trade:
            bars_held = i - entry_bar
            bar_h = highs[i]
            bar_l = lows[i]
            bar_c = closes[i]

            if side == 'buy':
                tp_price = entry_price * (1 + TP)
                sl_price = entry_price * (1 - SL)
                hit_sl   = bar_l <= sl_price
                hit_tp   = bar_h >= tp_price
            else:
                tp_price = entry_price * (1 - TP)
                sl_price = entry_price * (1 + SL)
                hit_sl   = bar_h >= sl_price
                hit_tp   = bar_l <= tp_price

            exit_price = None
            reason     = None

            # SL checked first (conservative)
            if hit_sl:
                exit_price = sl_price
                reason     = 'sl'
            elif hit_tp:
                exit_price = tp_price
                reason     = 'tp'
            elif bars_held >= MAX_B:
                exit_price = bar_c
                reason     = 'max_hold'

            if exit_price is not None:
                if side == 'buy':
                    gross = (exit_price - entry_price) / entry_price
                else:
                    gross = (entry_price - exit_price) / entry_price
                net  = gross - (FEE + SLIP)   # exit side fee+slip
                pnl  = notional * net
                trades.append({
                    'symbol'     : symbol,
                    'strategy'   : strat_name,
                    'side'       : side,
                    'entry_ts'   : entry_ts,
                    'exit_ts'    : ts_arr[i],
                    'entry_price': round(entry_price, 8),
                    'exit_price' : round(exit_price,  8),
                    'pnl'        : round(pnl, 4),
                    'reason'     : reason,
                    'bars'       : bars_held,
                })
                in_trade = False
                continue   # don't look for new entry same bar

        # ── Look for new signal (only if flat) ──
        if not in_trade:
            sig = None
            if strat_name == 'S1_Ribbon':
                sig = signal_S1_ribbon(i, opens, highs, lows, closes, vols)
            elif strat_name == 'S3_RSI2':
                sig = signal_S3_rsi2(i, opens, highs, lows, closes, vols)
            elif strat_name == 'S4_BodyMom':
                # Body momentum needs i+1 to be the confirmation bar
                # so we check signal on i, confirm on i+1 open
                # but here i is the signal bar, i+1 is entry
                sig = signal_S4_body(i + 1, opens, highs, lows, closes, vols)

            if sig is not None:
                # Entry on bar i+1 open, adjusted for fee+slip
                raw_entry = opens[i + 1]
                if sig == 'buy':
                    entry_price = raw_entry * (1 + FEE + SLIP)
                else:
                    entry_price = raw_entry * (1 - FEE - SLIP)
                side       = sig
                entry_ts   = ts_arr[i + 1]
                entry_bar  = i + 1
                in_trade   = True

    # Close any open trade at end of data
    if in_trade:
        exit_price = closes[-1]
        if side == 'buy':
            gross = (exit_price - entry_price) / entry_price
        else:
            gross = (entry_price - exit_price) / entry_price
        net  = gross - (FEE + SLIP)
        pnl  = notional * net
        trades.append({
            'symbol'     : symbol,
            'strategy'   : strat_name,
            'side'       : side,
            'entry_ts'   : entry_ts,
            'exit_ts'    : ts_arr[-1],
            'entry_price': round(entry_price, 8),
            'exit_price' : round(exit_price,  8),
            'pnl'        : round(pnl, 4),
            'reason'     : 'end_of_data',
            'bars'       : len(candles) - 1 - entry_bar,
        })

    return trades


def backtest_S2_orb(symbol, candles):
    """ORB 5m backtest — requires day grouping pre-pass."""
    cfg    = STRATEGIES['S2_ORB']
    TP     = cfg['tp']
    SL     = cfg['sl']
    MAX_B  = cfg['max_bars']
    MIN_B  = cfg['min_bars']
    trades = []

    n = len(candles)
    if n < MIN_B + 2: return trades

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    vols   = [c[5] for c in candles]

    notional = _position_size(SL)
    if notional <= 0: return trades

    # ── Pre-pass: build opening range per UTC day ──
    # day_candles: { day_key: [ (bar_idx, candle), ... ] }
    day_candles = defaultdict(list)
    for idx, c in enumerate(candles):
        dk = c[0] // 86400000
        day_candles[dk].append((idx, c))

    # day_ranges: { day_key: (or_high, or_low, avg_vol_first3) }
    day_ranges = {}
    for dk, bars in day_candles.items():
        if len(bars) < 4: continue   # need at least 4 bars (3 OR + 1 signal)
        or_high = max(bars[0][1][2], bars[1][1][2], bars[2][1][2])
        or_low  = min(bars[0][1][3], bars[1][1][3], bars[2][1][3])
        or_avg_vol = (bars[0][1][5] + bars[1][1][5] + bars[2][1][5]) / 3.0
        day_ranges[dk] = (or_high, or_low, or_avg_vol)

    in_trade    = False
    side        = None
    entry_price = 0.0
    entry_ts    = 0
    entry_bar   = 0
    day_traded  = set()   # day_keys where a trade was already taken

    for i in range(MIN_B, n - 1):
        if in_trade:
            bars_held = i - entry_bar
            bar_h = highs[i]
            bar_l = lows[i]
            bar_c = closes[i]

            if side == 'buy':
                tp_price = entry_price * (1 + TP)
                sl_price = entry_price * (1 - SL)
                hit_sl   = bar_l <= sl_price
                hit_tp   = bar_h >= tp_price
            else:
                tp_price = entry_price * (1 - TP)
                sl_price = entry_price * (1 + SL)
                hit_sl   = bar_h >= sl_price
                hit_tp   = bar_l <= tp_price

            exit_price = None
            reason     = None

            if hit_sl:
                exit_price = sl_price; reason = 'sl'
            elif hit_tp:
                exit_price = tp_price; reason = 'tp'
            elif bars_held >= MAX_B:
                exit_price = bar_c;   reason = 'max_hold'

            if exit_price is not None:
                if side == 'buy':
                    gross = (exit_price - entry_price) / entry_price
                else:
                    gross = (entry_price - exit_price) / entry_price
                net = gross - (FEE + SLIP)
                pnl = notional * net
                trades.append({
                    'symbol'     : symbol,
                    'strategy'   : 'S2_ORB',
                    'side'       : side,
                    'entry_ts'   : entry_ts,
                    'exit_ts'    : ts_arr[i],
                    'entry_price': round(entry_price, 8),
                    'exit_price' : round(exit_price,  8),
                    'pnl'        : round(pnl, 4),
                    'reason'     : reason,
                    'bars'       : bars_held,
                })
                in_trade = False
                continue

        if not in_trade:
            dk = ts_arr[i] // 86400000
            if dk in day_ranges and dk not in day_traded:
                or_high, or_low, or_avg_vol = day_ranges[dk]
                if or_high != or_low:
                    buf_high = or_high * 1.001
                    buf_low  = or_low  * 0.999
                    sig = None
                    if closes[i] > buf_high:
                        sig = 'buy'
                    elif closes[i] < buf_low:
                        sig = 'sell'

                    if sig is not None:
                        # Volume: bar i vs 20-bar avg before it
                        start_v = max(0, i - 20)
                        avg_vol  = sum(vols[start_v:i]) / max(1, i - start_v)
                        if avg_vol > 0 and vols[i] >= 2.0 * avg_vol:
                            adx_v, _, _ = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
                            if adx_v >= 22:
                                raw_entry = opens[i + 1]
                                if sig == 'buy':
                                    entry_price = raw_entry * (1 + FEE + SLIP)
                                else:
                                    entry_price = raw_entry * (1 - FEE - SLIP)
                                side       = sig
                                entry_ts   = ts_arr[i + 1]
                                entry_bar  = i + 1
                                in_trade   = True
                                day_traded.add(dk)

    # Close open trade at end of data
    if in_trade:
        exit_price = closes[-1]
        if side == 'buy':
            gross = (exit_price - entry_price) / entry_price
        else:
            gross = (entry_price - exit_price) / entry_price
        net = gross - (FEE + SLIP)
        pnl = notional * net
        trades.append({
            'symbol'     : symbol,
            'strategy'   : 'S2_ORB',
            'side'       : side,
            'entry_ts'   : entry_ts,
            'exit_ts'    : ts_arr[-1],
            'entry_price': round(entry_price, 8),
            'exit_price' : round(exit_price,  8),
            'pnl'        : round(pnl, 4),
            'reason'     : 'end_of_data',
            'bars'       : len(candles) - 1 - entry_bar,
        })

    return trades


# ── Stats ──────────────────────────────────────────────────────────────────────

def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
            'net_pnl': 0.0, 'max_drawdown': 0.0,
            'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }

    wins   = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] <= 0]
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))
    net_pnl      = sum(t['pnl'] for t in trades)

    pf   = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    wr   = 100.0 * len(wins) / len(trades)
    avg_w = gross_profit / len(wins)   if wins   else 0.0
    avg_l = sum(losses) / len(losses)  if losses else 0.0
    exp   = (wr/100 * avg_w) + ((1 - wr/100) * avg_l)

    # Max drawdown
    equity = 0.0; peak = 0.0; max_dd = 0.0
    for t in sorted(trades, key=lambda x: x['entry_ts']):
        equity += t['pnl']
        if equity > peak: peak = equity
        dd = peak - equity
        if dd > max_dd: max_dd = dd

    # Monthly
    monthly = {}
    for t in trades:
        ts_s = t['entry_ts'] / 1000
        import time as _time
        lt   = _time.gmtime(ts_s)
        key  = f"{lt.tm_year}-{lt.tm_mon:02d}"
        if key not in monthly: monthly[key] = {'pnl': 0.0, 'n': 0, 'w': 0}
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        if t['pnl'] > 0: monthly[key]['w'] += 1

    # Per coin
    per_coin = {}
    for t in trades:
        sym = t['symbol']
        if sym not in per_coin: per_coin[sym] = {'pnl': 0.0, 'n': 0, 'w': 0}
        per_coin[sym]['pnl'] += t['pnl']
        per_coin[sym]['n']   += 1
        if t['pnl'] > 0: per_coin[sym]['w'] += 1
    for sym, d in per_coin.items():
        d['wr'] = round(100.0 * d['w'] / d['n'], 1) if d['n'] > 0 else 0.0

    return {
        'total'        : len(trades),
        'win_rate'     : round(wr, 2),
        'profit_factor': round(pf, 4),
        'net_pnl'      : round(net_pnl, 2),
        'max_drawdown' : round(max_dd, 2),
        'avg_win'      : round(avg_w, 4),
        'avg_loss'     : round(avg_l, 4),
        'expectancy'   : round(exp, 4),
        'longs'        : sum(1 for t in trades if t['side'] == 'buy'),
        'shorts'       : sum(1 for t in trades if t['side'] == 'sell'),
        'monthly'      : monthly,
        'per_coin'     : per_coin,
    }


# ── Shard runner ───────────────────────────────────────────────────────────────

def run_symbol(symbol):
    """Fetch all timeframes needed and run all 4 strategies. Returns list of trades."""
    all_trades = []

    # Timeframes needed: 15m for S1/S3/S4, 5m for S2
    candles_15m = fetch_symbol(symbol, '15m')
    candles_5m  = fetch_symbol(symbol, '5m')

    # S1 — Ribbon Squeeze (15m)
    if len(candles_15m) >= STRATEGIES['S1_Ribbon']['min_bars'] + 2:
        t = backtest_15m(symbol, candles_15m, 'S1_Ribbon', STRATEGIES['S1_Ribbon'])
        all_trades.extend(t)

    # S2 — ORB (5m)
    if len(candles_5m) >= STRATEGIES['S2_ORB']['min_bars'] + 2:
        t = backtest_S2_orb(symbol, candles_5m)
        all_trades.extend(t)

    # S3 — RSI2 Reversion (15m)
    if len(candles_15m) >= STRATEGIES['S3_RSI2']['min_bars'] + 2:
        t = backtest_15m(symbol, candles_15m, 'S3_RSI2', STRATEGIES['S3_RSI2'])
        all_trades.extend(t)

    # S4 — Body Momentum (15m)
    if len(candles_15m) >= STRATEGIES['S4_BodyMom']['min_bars'] + 2:
        t = backtest_15m(symbol, candles_15m, 'S4_BodyMom', STRATEGIES['S4_BodyMom'])
        all_trades.extend(t)

    return all_trades


def run_shard(shard_idx):
    t0      = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] Starting {len(symbols)} symbols", flush=True)

    all_trades   = []
    with_data    = []
    no_data      = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(run_symbol, sym): sym for sym in symbols}
        done    = 0
        for fut in as_completed(futures):
            sym = futures[fut]
            done += 1
            try:
                trades = fut.result()
                if trades:
                    all_trades.extend(trades)
                    with_data.append(sym)
                else:
                    no_data.append(sym)
            except Exception as e:
                print(f"[Shard {shard_idx}] ERROR {sym}: {e}", flush=True)
                no_data.append(sym)
            if done % 5 == 0:
                print(f"[Shard {shard_idx}] {done}/{len(symbols)} done", flush=True)

    elapsed = time.time() - t0
    result  = {
        'shard'    : shard_idx,
        'symbols'  : symbols,
        'with_data': with_data,
        'no_data'  : no_data,
        'trades'   : all_trades,
        'stats'    : stats(all_trades),
        'elapsed'  : round(elapsed, 1),
    }

    fname = f"shard_{shard_idx}.json"
    with open(fname, 'w') as f:
        json.dump(result, f)
    print(f"[Shard {shard_idx}] Done in {elapsed:.0f}s — {len(all_trades)} trades, wrote {fname}", flush=True)


# ── Merge ──────────────────────────────────────────────────────────────────────

def merge_shards():
    all_trades   = []
    all_symbols  = []
    with_data    = []

    for idx in range(NUM_SHARDS):
        fname = f"shard_{idx}.json"
        if not os.path.exists(fname):
            print(f"WARNING: {fname} not found — skipping", flush=True)
            continue
        with open(fname) as f:
            shard = json.load(f)
        all_trades.extend(shard['trades'])
        all_symbols.extend(shard['symbols'])
        with_data.extend(shard.get('with_data', []))

    agg = stats(all_trades)

    # Per-strategy breakdown
    strat_names = ['S1_Ribbon', 'S2_ORB', 'S3_RSI2', 'S4_BodyMom']
    strat_stats = {}
    for s in strat_names:
        sub = [t for t in all_trades if t.get('strategy') == s]
        strat_stats[s] = stats(sub)

    report = {
        'period'        : f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'symbols_total' : len(all_symbols),
        'symbols_with_data': len(with_data),
        'aggregate'     : agg,
        'by_strategy'   : strat_stats,
        'trades'        : all_trades,
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # ── Summary text ──
    lines = []
    lines.append("=" * 72)
    lines.append("  4-STRATEGY BACKTEST REPORT")
    lines.append(f"  Period   : {report['period']}")
    lines.append(f"  Symbols  : {report['symbols_total']} attempted, {report['symbols_with_data']} with data")
    lines.append(f"  Capital  : ${CAPITAL:,.0f}  Leverage: {LEVERAGE}x  Risk/trade: {RISK_PCT*100}%")
    lines.append(f"  Fees     : {FEE*100}% + {SLIP*100}% slip (both sides)")
    lines.append("=" * 72)
    lines.append("")

    lines.append(f"{'Strategy':<16} {'Trades':>7} {'WR%':>6} {'PF':>6} {'MaxDD$':>10} {'NetPnL$':>10}  Result")
    lines.append("-" * 72)
    for s in strat_names:
        ss   = strat_stats[s]
        pf   = ss['profit_factor']
        wr   = ss['win_rate']
        ok   = "✅ PASS" if pf >= 1.5 and wr >= 42 else "❌ FAIL"
        lines.append(f"{s:<16} {ss['total']:>7} {wr:>6.1f} {pf:>6.3f} {ss['max_drawdown']:>10.2f} {ss['net_pnl']:>10.2f}  {ok}")

    lines.append("")
    lines.append("── AGGREGATE (all strategies combined) ──")
    a = agg
    lines.append(f"  Trades        : {a['total']}")
    lines.append(f"  Win Rate      : {a['win_rate']:.2f}%")
    lines.append(f"  Profit Factor : {a['profit_factor']:.4f}")
    lines.append(f"  Net PnL       : ${a['net_pnl']:,.2f}")
    lines.append(f"  Max Drawdown  : ${a['max_drawdown']:,.2f}")
    lines.append(f"  Avg Win       : ${a['avg_win']:.4f}")
    lines.append(f"  Avg Loss      : ${a['avg_loss']:.4f}")
    lines.append(f"  Expectancy    : ${a['expectancy']:.4f}")
    lines.append(f"  Longs         : {a['longs']}  Shorts: {a['shorts']}")
    lines.append("")

    # Top 20 coins per strategy by net PnL
    for s in strat_names:
        ss = strat_stats[s]
        lines.append(f"── Top 20 coins — {s} ──")
        top = sorted(ss['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)[:20]
        for sym, d in top:
            lines.append(f"  {sym:<24} trades={d['n']:>4}  wr={d['wr']:>5.1f}%  pnl=${d['pnl']:>9.2f}")
        lines.append("")

    # Monthly PnL (aggregate)
    lines.append("── Monthly PnL (aggregate all strategies) ──")
    for mo in sorted(a['monthly'].keys()):
        md = a['monthly'][mo]
        lines.append(f"  {mo}  n={md['n']:>5}  pnl=${md['pnl']:>9.2f}")
    lines.append("")

    # RECOMMENDATION
    any_pass = any(
        strat_stats[s]['profit_factor'] >= 1.5 and strat_stats[s]['win_rate'] >= 42
        for s in strat_names
    )
    lines.append("── RECOMMENDATION ──")
    if any_pass:
        lines.append("  ✅ AT LEAST ONE STRATEGY USABLE (PF >= 1.5, WR >= 42%)")
    else:
        lines.append("  ❌ NO STRATEGY MEETS THRESHOLD — do not trade live")
    lines.append("=" * 72)

    summary = "\n".join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary)

    print(summary, flush=True)
    print("\nWrote: backtest_report.json, backtest_summary.txt", flush=True)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest_4strats.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

