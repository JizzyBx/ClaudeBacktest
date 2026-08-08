"""
Strategy G VAR_D — Multi-TP Backtest
6 TP strategies compared side by side
20 shards | 5x leverage | unlimited positions | 1 year
stdlib only — no pip installs
"""

import sys, json, csv, io, zipfile, math, time
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, date

# ══════════════════════════════════════════════════════════════
# COIN LIST — 117-coin universe
# ══════════════════════════════════════════════════════════════
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

NUM_SHARDS = 20
WORKERS    = 16

# ── Date range: 1 year back from Aug 2025 ──
# Aug 2024 → Jul 2025
START_YM = (2024, 8)
END_YM   = (2025, 7)

TIMEFRAME = '15m'

# ── Risk / cost ──
CAPITAL   = 1000.0
LEVERAGE  = 5
FEE       = 0.0005   # 0.05% taker
SLIP      = 0.0003   # 0.03% slippage

# ── Strategy G constants ──
SL_PCT   = 0.150     # 15% SL — fixed, unchanged
MAX_BARS = 960       # 10 days max hold
MIN_BARS = 100       # indicator warmup

# ── TP Strategy definitions ──
# tp_id  : short label used in results
# tp_type: 'fixed' | 'atr' | 'adx_scaled' | 'partial_adx'
TP_STRATEGIES = [
    # 0: Original baseline — fixed 3%
    {'id': 'TP0_FIXED_3PCT',     'tp_type': 'fixed',        'tp_fixed': 0.030},
    # 1: ATR-based (2.0x ATR)
    {'id': 'TP1_ATR_2x',         'tp_type': 'atr',          'atr_mult': 2.0},
    # 2: ATR-based (3.0x ATR)
    {'id': 'TP2_ATR_3x',         'tp_type': 'atr',          'atr_mult': 3.0},
    # 3: ADX-scaled tiered TP
    {'id': 'TP3_ADX_SCALED',     'tp_type': 'adx_scaled'},
    # 4: Hybrid ATR floor + ADX ceiling
    {'id': 'TP4_HYBRID',         'tp_type': 'hybrid'},
    # 5: Partial TP (50% at 2%, rest at ADX-scaled) merged with ADX scaling
    {'id': 'TP5_PARTIAL_ADX',    'tp_type': 'partial_adx'},
]

# ══════════════════════════════════════════════════════════════
# DATA FETCH
# ══════════════════════════════════════════════════════════════
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month):
    url = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    try:
        with urlopen(url, timeout=30) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        out = []
        for r in rows:
            if not r or not r[0].isdigit():
                continue
            ts = int(r[0])
            if ts > 10**14:
                ts //= 1000
            out.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4])))
        return out
    except (HTTPError, URLError):
        return []
    except Exception:
        return []

def fetch_symbol(symbol):
    all_candles = []
    y, m = START_YM
    ey, em = END_YM
    while (y, m) <= (ey, em):
        all_candles.extend(fetch_month(symbol, y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    return [seen[k] for k in sorted(seen)]

# ══════════════════════════════════════════════════════════════
# INDICATORS (pure Python)
# ══════════════════════════════════════════════════════════════
def ema_series(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def atr_value(highs, lows, closes, period=14, idx=None):
    """ATR at a specific index (or last bar if idx=None)."""
    if idx is None:
        idx = len(closes) - 1
    start = max(1, idx - period * 3)
    trs = []
    for i in range(start, idx + 1):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))
    if not trs:
        return closes[idx] * 0.005
    if len(trs) < period:
        return sum(trs) / len(trs)
    a = sum(trs[:period]) / period
    for t in trs[period:]:
        a = (a * (period - 1) + t) / period
    return a

def adx_at(highs, lows, closes, period=14, idx=None):
    """ADX, +DI, -DI at a specific bar index."""
    if idx is None:
        idx = len(closes) - 1
    start = max(1, idx - period * 4)
    pdm, mdm, trs = [], [], []
    for i in range(start, idx + 1):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))
    def ws(v, p):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st: return 0.0, 0.0, 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0, pdi[-1] if pdi else 0.0, mdi[-1] if mdi else 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]: adx = (adx*(period-1) + d) / period
    return max(0.0, min(100.0, adx)), pdi[-1], mdi[-1]

# ══════════════════════════════════════════════════════════════
# SIGNAL — Strategy G VAR_D
# ══════════════════════════════════════════════════════════════
def signal_G(i, closes, highs, lows, e9, e21, e50):
    """
    Returns (signal, adx_val, slope_pct, atr14) or (None, ...) if no signal.
    Evaluated on bar i (last CLOSED bar). Entry on bar i+1.
    """
    if i < 70:
        return None, 0.0, 0.0, 0.0

    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None, 0.0, slope_pct, 0.0

    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

    if trend_up and not crossed_up:
        return None, 0.0, slope_pct, 0.0
    if trend_down and not crossed_down:
        return None, 0.0, slope_pct, 0.0
    if not crossed_up and not crossed_down:
        return None, 0.0, slope_pct, 0.0

    adx_val, _, _ = adx_at(highs, lows, closes, 14, i)
    if adx_val < 22:
        return None, adx_val, slope_pct, 0.0

    atr14 = atr_value(highs, lows, closes, 14, i)
    sig = 'buy' if crossed_up else 'sell'
    return sig, adx_val, slope_pct, atr14

# ══════════════════════════════════════════════════════════════
# TP CALCULATOR
# ══════════════════════════════════════════════════════════════
def calc_tp(tp_cfg, side, entry_price, adx_val, atr14):
    """
    Returns tp_price for full close strategies.
    For partial_adx returns (tp1_price, tp2_price, tp1_frac).
    """
    tp_type = tp_cfg['tp_type']

    if tp_type == 'fixed':
        pct = tp_cfg['tp_fixed']
        if side == 'buy': return entry_price * (1 + pct)
        else:             return entry_price * (1 - pct)

    elif tp_type == 'atr':
        mult = tp_cfg['atr_mult']
        dist = atr14 * mult
        if side == 'buy': return entry_price + dist
        else:             return entry_price - dist

    elif tp_type == 'adx_scaled':
        pct = _adx_tier(adx_val)
        if side == 'buy': return entry_price * (1 + pct)
        else:             return entry_price * (1 - pct)

    elif tp_type == 'hybrid':
        # ATR floor (1.5x), scaled by ADX tier, no hard cap
        atr_floor = atr14 * 1.5
        adx_pct   = _adx_tier(adx_val)
        adx_dist  = entry_price * adx_pct
        dist = max(atr_floor, adx_dist)
        if side == 'buy': return entry_price + dist
        else:             return entry_price - dist

    elif tp_type == 'partial_adx':
        # TP1: 2% (50% of position)
        # TP2: ADX-scaled (remaining 50%)
        pct2 = _adx_tier(adx_val)
        if side == 'buy':
            tp1 = entry_price * 1.02
            tp2 = entry_price * (1 + pct2)
        else:
            tp1 = entry_price * 0.98
            tp2 = entry_price * (1 - pct2)
        return tp1, tp2, 0.5   # (tp1, tp2, fraction_at_tp1)

    return entry_price * (1.03 if side == 'buy' else 0.97)

def _adx_tier(adx_val):
    if adx_val >= 50: return 0.060
    if adx_val >= 35: return 0.040
    return 0.025   # ADX 22–35

# ══════════════════════════════════════════════════════════════
# BACKTEST — single symbol, single TP strategy
# ══════════════════════════════════════════════════════════════
def backtest_symbol(symbol, candles, tp_cfg):
    if len(candles) < MIN_BARS:
        return []

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    n = len(closes)

    # Pre-compute EMA series
    e9  = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)

    trades = []
    tp_type = tp_cfg['tp_type']
    is_partial = (tp_type == 'partial_adx')

    # Position sizing — unlimited concurrent positions
    # Each trade risks CAPITAL * 0.75% / SL_PCT, capped by CAPITAL * LEVERAGE
    risk_usd = CAPITAL * 0.0075
    notional = min(risk_usd / SL_PCT, CAPITAL * LEVERAGE)

    i = MIN_BARS
    while i < n - 1:
        sig, adx_val, slope_pct, atr14 = signal_G(i, closes, highs, lows, e9, e21, e50)

        if sig is None:
            i += 1
            continue

        # Entry on bar i+1 open
        if i + 1 >= n:
            break

        raw_entry = opens[i+1]
        if sig == 'buy':
            entry_p = raw_entry * (1 + FEE + SLIP)
            sl_price = entry_p * (1 - SL_PCT)
        else:
            entry_p = raw_entry * (1 - FEE - SLIP)
            sl_price = entry_p * (1 + SL_PCT)

        # Compute TP(s)
        if is_partial:
            tp1, tp2, frac1 = calc_tp(tp_cfg, sig, entry_p, adx_val, atr14)
            tp1_hit = False
            tp1_pnl = 0.0
        else:
            tp_price = calc_tp(tp_cfg, sig, entry_p, adx_val, atr14)

        # Sanity check TP direction
        if not is_partial:
            if sig == 'buy' and tp_price <= entry_p:
                i += 1; continue
            if sig == 'sell' and tp_price >= entry_p:
                i += 1; continue

        entry_ts = ts_arr[i+1]
        exit_ts = entry_ts
        exit_price = entry_p
        reason = 'end_of_data'
        bars_held = 0

        # Simulate bar by bar
        j = i + 1
        while j < n:
            bars_held = j - (i + 1)
            h = highs[j]
            l = lows[j]
            c = closes[j]

            if is_partial:
                if not tp1_hit:
                    # Phase 1: both TP1 and SL active
                    if sig == 'buy':
                        sl_hit = l <= sl_price
                        t1_hit = h >= tp1
                    else:
                        sl_hit = h >= sl_price
                        t1_hit = l <= tp1

                    if sl_hit and t1_hit:
                        # SL wins (conservative)
                        sl_hit = True; t1_hit = False

                    if sl_hit:
                        exit_price = sl_price
                        exit_ts    = ts_arr[j]
                        reason     = 'sl'
                        # Full position hit SL
                        gross = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                        net   = gross - (FEE + SLIP) * 2
                        trade_pnl = notional * net
                        trades.append({
                            'symbol': symbol, 'side': sig,
                            'entry_ts': entry_ts, 'exit_ts': exit_ts,
                            'entry_price': entry_p, 'exit_price': exit_price,
                            'pnl': round(trade_pnl, 6),
                            'reason': reason, 'bars': bars_held,
                            'tp_id': tp_cfg['id'],
                            'adx': round(adx_val, 1),
                        })
                        break

                    if t1_hit:
                        # 50% closed at TP1
                        tp1_hit = True
                        tp1_exit = tp1
                        gross1 = (tp1_exit - entry_p) / entry_p if sig == 'buy' else (entry_p - tp1_exit) / entry_p
                        net1   = gross1 - (FEE + SLIP) * 2
                        tp1_pnl = notional * frac1 * net1
                        # Move SL to breakeven for remainder
                        sl_price = entry_p

                else:
                    # Phase 2: TP2 with breakeven SL
                    if sig == 'buy':
                        sl_hit = l <= sl_price
                        t2_hit = h >= tp2
                    else:
                        sl_hit = h >= sl_price
                        t2_hit = l <= tp2

                    if sl_hit and t2_hit:
                        sl_hit = True; t2_hit = False

                    if sl_hit:
                        # Breakeven stop — remainder exits at entry_p
                        exit_price = sl_price
                        exit_ts    = ts_arr[j]
                        reason     = 'sl_breakeven'
                        gross2 = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                        net2   = gross2 - (FEE + SLIP) * 2
                        tp2_pnl = notional * (1 - frac1) * net2
                        trade_pnl = tp1_pnl + tp2_pnl
                        trades.append({
                            'symbol': symbol, 'side': sig,
                            'entry_ts': entry_ts, 'exit_ts': exit_ts,
                            'entry_price': entry_p, 'exit_price': exit_price,
                            'pnl': round(trade_pnl, 6),
                            'reason': reason, 'bars': bars_held,
                            'tp_id': tp_cfg['id'],
                            'adx': round(adx_val, 1),
                        })
                        break

                    if t2_hit:
                        exit_price = tp2
                        exit_ts    = ts_arr[j]
                        reason     = 'tp'
                        gross2 = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                        net2   = gross2 - (FEE + SLIP) * 2
                        tp2_pnl = notional * (1 - frac1) * net2
                        trade_pnl = tp1_pnl + tp2_pnl
                        trades.append({
                            'symbol': symbol, 'side': sig,
                            'entry_ts': entry_ts, 'exit_ts': exit_ts,
                            'entry_price': entry_p, 'exit_price': exit_price,
                            'pnl': round(trade_pnl, 6),
                            'reason': reason, 'bars': bars_held,
                            'tp_id': tp_cfg['id'],
                            'adx': round(adx_val, 1),
                        })
                        break

                # Max hold
                if bars_held >= MAX_BARS:
                    exit_price = c
                    exit_ts    = ts_arr[j]
                    reason     = 'max_hold'
                    gross_r = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                    net_r   = gross_r - (FEE + SLIP) * 2
                    remainder_pnl = notional * (1 - (frac1 if tp1_hit else 0)) * net_r
                    trade_pnl = (tp1_pnl if tp1_hit else 0) + remainder_pnl
                    trades.append({
                        'symbol': symbol, 'side': sig,
                        'entry_ts': entry_ts, 'exit_ts': exit_ts,
                        'entry_price': entry_p, 'exit_price': exit_price,
                        'pnl': round(trade_pnl, 6),
                        'reason': reason, 'bars': bars_held,
                        'tp_id': tp_cfg['id'],
                        'adx': round(adx_val, 1),
                    })
                    break

                j += 1
                if j >= n:
                    # End of data — close remainder
                    exit_price = closes[-1]
                    exit_ts    = ts_arr[-1]
                    reason     = 'end_of_data'
                    gross_r = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                    net_r   = gross_r - (FEE + SLIP) * 2
                    remainder_pnl = notional * (1 - (frac1 if tp1_hit else 0)) * net_r
                    trade_pnl = (tp1_pnl if tp1_hit else 0) + remainder_pnl
                    trades.append({
                        'symbol': symbol, 'side': sig,
                        'entry_ts': entry_ts, 'exit_ts': exit_ts,
                        'entry_price': entry_p, 'exit_price': exit_price,
                        'pnl': round(trade_pnl, 6),
                        'reason': reason, 'bars': bars_held,
                        'tp_id': tp_cfg['id'],
                        'adx': round(adx_val, 1),
                    })

            else:
                # Non-partial strategies
                if sig == 'buy':
                    sl_hit = l <= sl_price
                    tp_hit = h >= tp_price
                else:
                    sl_hit = h >= sl_price
                    tp_hit = l <= tp_price

                # SL wins if both on same bar
                if sl_hit and tp_hit:
                    sl_hit = True; tp_hit = False

                if sl_hit:
                    exit_price = sl_price
                    exit_ts    = ts_arr[j]
                    reason     = 'sl'
                    gross = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                    net   = gross - (FEE + SLIP) * 2
                    trade_pnl = notional * net
                    trades.append({
                        'symbol': symbol, 'side': sig,
                        'entry_ts': entry_ts, 'exit_ts': exit_ts,
                        'entry_price': entry_p, 'exit_price': exit_price,
                        'pnl': round(trade_pnl, 6),
                        'reason': reason, 'bars': bars_held,
                        'tp_id': tp_cfg['id'],
                        'adx': round(adx_val, 1),
                    })
                    break

                if tp_hit:
                    exit_price = tp_price
                    exit_ts    = ts_arr[j]
                    reason     = 'tp'
                    gross = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                    net   = gross - (FEE + SLIP) * 2
                    trade_pnl = notional * net
                    trades.append({
                        'symbol': symbol, 'side': sig,
                        'entry_ts': entry_ts, 'exit_ts': exit_ts,
                        'entry_price': entry_p, 'exit_price': exit_price,
                        'pnl': round(trade_pnl, 6),
                        'reason': reason, 'bars': bars_held,
                        'tp_id': tp_cfg['id'],
                        'adx': round(adx_val, 1),
                    })
                    break

                if bars_held >= MAX_BARS:
                    exit_price = c
                    exit_ts    = ts_arr[j]
                    reason     = 'max_hold'
                    gross = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                    net   = gross - (FEE + SLIP) * 2
                    trade_pnl = notional * net
                    trades.append({
                        'symbol': symbol, 'side': sig,
                        'entry_ts': entry_ts, 'exit_ts': exit_ts,
                        'entry_price': entry_p, 'exit_price': exit_price,
                        'pnl': round(trade_pnl, 6),
                        'reason': reason, 'bars': bars_held,
                        'tp_id': tp_cfg['id'],
                        'adx': round(adx_val, 1),
                    })
                    break

                j += 1
                if j >= n:
                    exit_price = closes[-1]
                    exit_ts    = ts_arr[-1]
                    reason     = 'end_of_data'
                    gross = (exit_price - entry_p) / entry_p if sig == 'buy' else (entry_p - exit_price) / entry_p
                    net   = gross - (FEE + SLIP) * 2
                    trade_pnl = notional * net
                    trades.append({
                        'symbol': symbol, 'side': sig,
                        'entry_ts': entry_ts, 'exit_ts': exit_ts,
                        'entry_price': entry_p, 'exit_price': exit_price,
                        'pnl': round(trade_pnl, 6),
                        'reason': reason, 'bars': bars_held,
                        'tp_id': tp_cfg['id'],
                        'adx': round(adx_val, 1),
                    })

        # Next scan from bar i+2 (bar after entry) — unlimited concurrent positions
        # so we don't skip i+1; we just advance past the signal bar
        i += 1

    return trades

# ══════════════════════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════════════════════
def compute_stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
            'net_pnl': 0.0, 'max_drawdown': 0.0, 'max_drawdown_pct': 0.0,
            'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'sharpe': 0.0, 'longs': 0, 'shorts': 0,
            'tp_hits': 0, 'sl_hits': 0, 'max_hold_hits': 0,
            'avg_bars': 0.0,
            'monthly': {}, 'per_coin': {},
        }

    wins   = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] < 0]
    total  = len(trades)
    win_rate = len(wins) / total * 100 if total else 0.0
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)
    net_pnl   = sum(t['pnl'] for t in trades)
    avg_win   = sum(wins)  / len(wins)   if wins   else 0.0
    avg_loss  = sum(losses)/ len(losses) if losses else 0.0
    expectancy = (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)
    avg_bars   = sum(t['bars'] for t in trades) / total

    # Max drawdown
    equity = 0.0; peak = 0.0; max_dd = 0.0; max_dd_pct = 0.0
    daily_pnl = defaultdict(float)
    for t in trades:
        equity += t['pnl']
        peak = max(peak, equity)
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = (dd / peak * 100) if peak > 0 else 0.0
        # Daily for Sharpe
        d_str = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m-%d')
        daily_pnl[d_str] += t['pnl']

    # Sharpe (daily returns, annualised)
    if len(daily_pnl) > 1:
        vals = list(daily_pnl.values())
        mean = sum(vals) / len(vals)
        variance = sum((v - mean)**2 for v in vals) / len(vals)
        std = math.sqrt(variance) if variance > 0 else 0.0
        sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Monthly
    monthly = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    for t in trades:
        ym = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m')
        monthly[ym]['pnl'] += t['pnl']
        monthly[ym]['n']   += 1
        if t['pnl'] > 0: monthly[ym]['w'] += 1

    # Per-coin
    per_coin = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    for t in trades:
        sym = t['symbol']
        per_coin[sym]['pnl'] += t['pnl']
        per_coin[sym]['n']   += 1
        if t['pnl'] > 0: per_coin[sym]['w'] += 1
    for sym in per_coin:
        n = per_coin[sym]['n']
        per_coin[sym]['wr'] = per_coin[sym]['w'] / n * 100 if n else 0.0

    return {
        'total':           total,
        'win_rate':        round(win_rate, 2),
        'profit_factor':   round(pf, 4) if pf != float('inf') else 9999.0,
        'net_pnl':         round(net_pnl, 4),
        'max_drawdown':    round(max_dd, 4),
        'max_drawdown_pct':round(max_dd_pct, 2),
        'avg_win':         round(avg_win, 4),
        'avg_loss':        round(avg_loss, 4),
        'expectancy':      round(expectancy, 4),
        'sharpe':          round(sharpe, 3),
        'longs':           sum(1 for t in trades if t['side'] == 'buy'),
        'shorts':          sum(1 for t in trades if t['side'] == 'sell'),
        'tp_hits':         sum(1 for t in trades if t['reason'] == 'tp'),
        'sl_hits':         sum(1 for t in trades if t['reason'] in ('sl','sl_breakeven')),
        'max_hold_hits':   sum(1 for t in trades if t['reason'] == 'max_hold'),
        'avg_bars':        round(avg_bars, 1),
        'monthly':         {k: dict(v) for k, v in sorted(monthly.items())},
        'per_coin':        {k: dict(v) for k, v in per_coin.items()},
    }

# ══════════════════════════════════════════════════════════════
# SHARD RUNNER
# ══════════════════════════════════════════════════════════════
def run_shard(shard_idx):
    t0 = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} symbols: {symbols}")

    # Fetch all candles in parallel
    candle_map = {}
    def _fetch(sym):
        data = fetch_symbol(sym)
        return sym, data

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_fetch, sym): sym for sym in symbols}
        for fut in as_completed(futs):
            sym, data = fut.result()
            candle_map[sym] = data
            print(f"[Shard {shard_idx}] {sym}: {len(data)} candles")

    # Check geo-block
    total_candles = sum(len(v) for v in candle_map.values())
    if total_candles == 0:
        print(f"[Shard {shard_idx}] GEO-BLOCK or no data — 0 candles fetched. Aborting.")
        result = {
            'shard': shard_idx, 'symbols': symbols,
            'with_data': [], 'trades': [],
            'stats_by_tp': {}, 'elapsed': time.time() - t0,
            'error': 'GEO_BLOCK_OR_NO_DATA'
        }
        with open(f'shard_{shard_idx}.json', 'w') as f:
            json.dump(result, f)
        return

    # Backtest each symbol × each TP strategy
    all_trades_by_tp = {tp['id']: [] for tp in TP_STRATEGIES}
    with_data = []

    for sym in symbols:
        candles = candle_map[sym]
        if len(candles) < MIN_BARS:
            print(f"[Shard {shard_idx}] {sym}: skipped (only {len(candles)} candles)")
            continue
        with_data.append(sym)
        for tp_cfg in TP_STRATEGIES:
            trades = backtest_symbol(sym, candles, tp_cfg)
            all_trades_by_tp[tp_cfg['id']].extend(trades)
            print(f"[Shard {shard_idx}] {sym} {tp_cfg['id']}: {len(trades)} trades")

    # Stats per TP strategy
    stats_by_tp = {}
    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        trades = all_trades_by_tp[tp_id]
        stats_by_tp[tp_id] = compute_stats(trades)

    # Flatten all trades (include tp_id tag)
    all_trades_flat = []
    for tp_id, trades in all_trades_by_tp.items():
        all_trades_flat.extend(trades)

    result = {
        'shard':        shard_idx,
        'symbols':      symbols,
        'with_data':    with_data,
        'trades':       all_trades_flat,
        'stats_by_tp':  stats_by_tp,
        'elapsed':      round(time.time() - t0, 1),
    }

    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(result, f)

    print(f"[Shard {shard_idx}] Done in {result['elapsed']}s — {len(with_data)} coins with data")

# ══════════════════════════════════════════════════════════════
# MERGE
# ══════════════════════════════════════════════════════════════
def merge_shards():
    import glob
    shard_files = sorted(glob.glob('shard_*.json'))
    print(f"Merging {len(shard_files)} shard files...")

    all_trades_by_tp = {tp['id']: [] for tp in TP_STRATEGIES}
    all_symbols = []
    with_data   = []

    for sf in shard_files:
        with open(sf) as f:
            shard = json.load(f)
        all_symbols.extend(shard.get('symbols', []))
        with_data.extend(shard.get('with_data', []))
        # Trades are tagged with tp_id
        for trade in shard.get('trades', []):
            tp_id = trade.get('tp_id')
            if tp_id in all_trades_by_tp:
                all_trades_by_tp[tp_id].append(trade)

    # Compute stats per TP strategy
    results_by_tp = {}
    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        trades = all_trades_by_tp[tp_id]
        results_by_tp[tp_id] = {
            'config': tp_cfg,
            'stats':  compute_stats(trades),
            'trade_count': len(trades),
        }

    report = {
        'period':         f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe':      TIMEFRAME,
        'leverage':       LEVERAGE,
        'sl_pct':         SL_PCT,
        'symbols_attempted': len(set(all_symbols)),
        'symbols_with_data': len(set(with_data)),
        'tp_strategies':  results_by_tp,
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # ── Summary text ──
    lines = []
    lines.append("=" * 72)
    lines.append("  STRATEGY G VAR_D — MULTI-TP BACKTEST RESULTS")
    lines.append("=" * 72)
    lines.append(f"  Period   : {report['period']}")
    lines.append(f"  Timeframe: {TIMEFRAME}   Leverage: {LEVERAGE}x")
    lines.append(f"  SL       : {SL_PCT*100:.0f}% (fixed)")
    lines.append(f"  Capital  : ${CAPITAL}   Fee: {FEE*100:.2f}%   Slip: {SLIP*100:.2f}%")
    lines.append(f"  Coins    : {report['symbols_attempted']} attempted, {report['symbols_with_data']} with data")
    lines.append("=" * 72)
    lines.append("")

    # Comparison table
    headers = ["TP Strategy", "Trades", "WR%", "PF", "Net PnL", "MaxDD%", "Sharpe", "AvgBars", "TPs", "SLs"]
    col_w   = [20, 7, 7, 7, 10, 8, 8, 9, 7, 7]

    def _pad(s, w): return str(s)[:w].ljust(w)
    def _row(vals): return "  " + "  ".join(_pad(v, w) for v, w in zip(vals, col_w))

    lines.append(_row(headers))
    lines.append("  " + "-" * 70)

    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        s = results_by_tp[tp_id]['stats']
        pf_str = f"{s['profit_factor']:.3f}" if s['profit_factor'] < 9999 else "∞"
        row = [
            tp_id, s['total'], f"{s['win_rate']:.1f}%",
            pf_str, f"${s['net_pnl']:+.2f}",
            f"{s['max_drawdown_pct']:.1f}%",
            f"{s['sharpe']:.3f}", f"{s['avg_bars']:.0f}",
            s['tp_hits'], s['sl_hits'],
        ]
        lines.append(_row(row))

    lines.append("")
    lines.append("=" * 72)
    lines.append("  DETAILED BREAKDOWN PER TP STRATEGY")
    lines.append("=" * 72)

    best_tp = None; best_pf = 0.0
    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        s = results_by_tp[tp_id]['stats']
        pf_val = s['profit_factor'] if s['profit_factor'] < 9999 else 0.0
        if pf_val > best_pf:
            best_pf = pf_val; best_tp = tp_id

        lines.append("")
        lines.append(f"  ── {tp_id} ──")
        lines.append(f"     Trades     : {s['total']}")
        lines.append(f"     Win Rate   : {s['win_rate']:.2f}%")
        pf_str = f"{s['profit_factor']:.4f}" if s['profit_factor'] < 9999 else "∞"
        lines.append(f"     Prof Factor: {pf_str}")
        lines.append(f"     Net PnL    : ${s['net_pnl']:+.4f}")
        lines.append(f"     Max DD     : ${s['max_drawdown']:+.4f}  ({s['max_drawdown_pct']:.2f}%)")
        lines.append(f"     Sharpe     : {s['sharpe']:.3f}")
        lines.append(f"     Avg Win    : ${s['avg_win']:+.4f}")
        lines.append(f"     Avg Loss   : ${s['avg_loss']:+.4f}")
        lines.append(f"     Expectancy : ${s['expectancy']:+.4f}")
        lines.append(f"     Longs      : {s['longs']}  Shorts: {s['shorts']}")
        lines.append(f"     TP hits    : {s['tp_hits']}  SL hits: {s['sl_hits']}  MaxHold: {s['max_hold_hits']}")
        lines.append(f"     Avg Bars   : {s['avg_bars']:.1f}")
        rec = "✅ USABLE" if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42 else "❌ NOT USABLE"
        lines.append(f"     VERDICT    : {rec}")

        # Monthly PnL
        lines.append(f"     Monthly PnL:")
        for ym, mv in sorted(s['monthly'].items()):
            bar = "+" * min(30, int(abs(mv['pnl']))) if mv['pnl'] > 0 else "-" * min(30, int(abs(mv['pnl'])))
            lines.append(f"       {ym}  ${mv['pnl']:+8.2f}  {bar}  ({mv['w']}/{mv['n']})")

        # Top 20 coins
        ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
        lines.append(f"     Top 20 Coins (by PnL):")
        for sym, cd in ranked[:20]:
            pf_c = "∞" if cd['pnl'] > 0 and cd.get('n', 1) > 0 else "--"
            lines.append(f"       {sym:<28}  {cd['n']:>4} trades  {cd['wr']:5.1f}% WR  ${cd['pnl']:+.4f}")

    lines.append("")
    lines.append("=" * 72)
    lines.append(f"  WINNER: {best_tp}  (PF={best_pf:.4f})")
    lines.append("=" * 72)

    summary = "\n".join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary)

    print(summary)
    print("\n✅ backtest_report.json + backtest_summary.txt written")

# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

