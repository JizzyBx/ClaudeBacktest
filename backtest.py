"""
Strategy G VAR_D — Indicator-Based TP Exit Comparison
VAR_RSI   : Exit when RSI14 > 75 (long) or < 25 (short)
VAR_EMA   : Exit when EMA9/21 crosses back against position
VAR_COMBO : Whichever of RSI or EMA fires first
20 shards | 5x leverage | 3 years | stdlib only
"""

import sys, json, csv, io, zipfile, math, time
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime

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

# ── 3-year date range ──
START_YM = (2022, 8)
END_YM   = (2025, 7)

TIMEFRAME = '15m'

# ── Risk / cost ──
CAPITAL   = 1000.0
LEVERAGE  = 5
FEE       = 0.0005
SLIP      = 0.0003

# ── Strategy G constants ──
SL_PCT   = 0.150
MAX_BARS = 960       # 10-day hard cap
MIN_BARS = 100

# RSI thresholds
RSI_OB = 75.0   # overbought — exit long
RSI_OS = 25.0   # oversold   — exit short
RSI_PERIOD = 14

# ── TP Strategy definitions ──
TP_STRATEGIES = [
    {'id': 'VAR_RSI',   'tp_type': 'rsi'},
    {'id': 'VAR_EMA',   'tp_type': 'ema_cross'},
    {'id': 'VAR_COMBO', 'tp_type': 'combo'},
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
# INDICATORS
# ══════════════════════════════════════════════════════════════
def ema_series(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def rsi_series(closes, period=14):
    """Full RSI series, same length as closes."""
    rsi = [50.0] * len(closes)
    if len(closes) < period + 1:
        return rsi
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period, len(closes)):
        if i > period:
            diff = closes[i] - closes[i-1]
            gain = max(diff, 0.0)
            loss = max(-diff, 0.0)
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def atr_value(highs, lows, closes, period=14, idx=None):
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
# SIGNAL — Strategy G VAR_D (unchanged)
# ══════════════════════════════════════════════════════════════
def signal_G(i, closes, highs, lows, e9, e21, e50):
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
# BACKTEST — single symbol, single TP strategy
# ══════════════════════════════════════════════════════════════
def backtest_symbol(symbol, candles, tp_cfg, rsi_arr, e9_arr, e21_arr):
    if len(candles) < MIN_BARS:
        return []

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    n = len(closes)

    tp_type = tp_cfg['tp_type']
    trades  = []

    risk_usd = CAPITAL * 0.0075
    notional = min(risk_usd / SL_PCT, CAPITAL * LEVERAGE)

    i = MIN_BARS
    while i < n - 1:
        sig, adx_val, slope_pct, atr14 = signal_G(i, closes, highs, lows, e9_arr, e21_arr, e9_arr)
        # Note: signal_G uses e9, e21, e50 — we pass e9_arr as e50 placeholder but
        # e50 is only used for slope internally so we need to pass correctly below
        # (fixed in the actual call — see run loop)
        if sig is None:
            i += 1
            continue
        if i + 1 >= n:
            break

        raw_entry = opens[i+1]
        if sig == 'buy':
            entry_p  = raw_entry * (1 + FEE + SLIP)
            sl_price = entry_p * (1 - SL_PCT)
        else:
            entry_p  = raw_entry * (1 - FEE - SLIP)
            sl_price = entry_p * (1 + SL_PCT)

        entry_ts  = ts_arr[i+1]
        bars_held = 0
        exit_price = entry_p
        exit_ts    = entry_ts
        reason     = 'end_of_data'

        j = i + 1
        while j < n:
            bars_held = j - (i + 1)
            h = highs[j]
            l = lows[j]
            c = closes[j]

            # ── SL check (always first) ──
            if sig == 'buy':
                sl_hit = l <= sl_price
            else:
                sl_hit = h >= sl_price

            if sl_hit:
                exit_price = sl_price
                exit_ts    = ts_arr[j]
                reason     = 'sl'
                gross = (exit_price - entry_p)/entry_p if sig == 'buy' else (entry_p - exit_price)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, exit_ts, entry_p,
                                     exit_price, notional*net, reason, bars_held,
                                     tp_cfg['id'], adx_val))
                break

            # ── TP indicator checks ──
            rsi_val = rsi_arr[j]

            rsi_exit = False
            if tp_type in ('rsi', 'combo'):
                if sig == 'buy'  and rsi_val >= RSI_OB:
                    rsi_exit = True
                if sig == 'sell' and rsi_val <= RSI_OS:
                    rsi_exit = True

            ema_exit = False
            if tp_type in ('ema_cross', 'combo'):
                if j > 0:
                    # EMA9 crosses back against position on this bar
                    if sig == 'buy'  and e9_arr[j] < e21_arr[j] and e9_arr[j-1] >= e21_arr[j-1]:
                        ema_exit = True
                    if sig == 'sell' and e9_arr[j] > e21_arr[j] and e9_arr[j-1] <= e21_arr[j-1]:
                        ema_exit = True

            tp_hit = rsi_exit or ema_exit

            if tp_hit:
                exit_price = c   # close of the bar where indicator fired
                exit_ts    = ts_arr[j]
                reason     = 'tp'
                gross = (exit_price - entry_p)/entry_p if sig == 'buy' else (entry_p - exit_price)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, exit_ts, entry_p,
                                     exit_price, notional*net, reason, bars_held,
                                     tp_cfg['id'], adx_val))
                break

            # ── Max hold ──
            if bars_held >= MAX_BARS:
                exit_price = c
                exit_ts    = ts_arr[j]
                reason     = 'max_hold'
                gross = (exit_price - entry_p)/entry_p if sig == 'buy' else (entry_p - exit_price)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, exit_ts, entry_p,
                                     exit_price, notional*net, reason, bars_held,
                                     tp_cfg['id'], adx_val))
                break

            j += 1
            if j >= n:
                exit_price = closes[-1]
                exit_ts    = ts_arr[-1]
                reason     = 'end_of_data'
                gross = (exit_price - entry_p)/entry_p if sig == 'buy' else (entry_p - exit_price)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, exit_ts, entry_p,
                                     exit_price, notional*net, reason, bars_held,
                                     tp_cfg['id'], adx_val))

        i += 1

    return trades

def _trade(symbol, side, entry_ts, exit_ts, entry_p, exit_p, pnl, reason, bars, tp_id, adx):
    return {
        'symbol':      symbol,
        'side':        side,
        'entry_ts':    entry_ts,
        'exit_ts':     exit_ts,
        'entry_price': entry_p,
        'exit_price':  exit_p,
        'pnl':         round(pnl, 6),
        'reason':      reason,
        'bars':        bars,
        'tp_id':       tp_id,
        'adx':         round(adx, 1),
    }

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
            'avg_bars': 0.0, 'monthly': {}, 'per_coin': {},
        }

    wins   = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] < 0]
    total  = len(trades)
    win_rate   = len(wins) / total * 100
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)
    net_pnl    = sum(t['pnl'] for t in trades)
    avg_win    = sum(wins)   / len(wins)   if wins   else 0.0
    avg_loss   = sum(losses) / len(losses) if losses else 0.0
    expectancy = (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)
    avg_bars   = sum(t['bars'] for t in trades) / total

    equity = 0.0; peak = 0.0; max_dd = 0.0; max_dd_pct = 0.0
    daily_pnl = defaultdict(float)
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        equity += t['pnl']
        peak = max(peak, equity)
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = (dd / peak * 100) if peak > 0 else 0.0
        d_str = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m-%d')
        daily_pnl[d_str] += t['pnl']

    if len(daily_pnl) > 1:
        vals = list(daily_pnl.values())
        mean = sum(vals) / len(vals)
        var  = sum((v - mean)**2 for v in vals) / len(vals)
        std  = math.sqrt(var) if var > 0 else 0.0
        sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0

    monthly  = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    per_coin = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0})
    for t in trades:
        ym  = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m')
        sym = t['symbol']
        monthly[ym]['pnl']  += t['pnl']; monthly[ym]['n'] += 1
        if t['pnl'] > 0: monthly[ym]['w'] += 1
        per_coin[sym]['pnl'] += t['pnl']; per_coin[sym]['n'] += 1
        if t['pnl'] > 0: per_coin[sym]['w'] += 1
    for sym in per_coin:
        n = per_coin[sym]['n']
        per_coin[sym]['wr'] = per_coin[sym]['w'] / n * 100 if n else 0.0

    return {
        'total':            total,
        'win_rate':         round(win_rate, 2),
        'profit_factor':    round(pf, 4) if pf != float('inf') else 9999.0,
        'net_pnl':          round(net_pnl, 4),
        'max_drawdown':     round(max_dd, 4),
        'max_drawdown_pct': round(max_dd_pct, 2),
        'avg_win':          round(avg_win, 4),
        'avg_loss':         round(avg_loss, 4),
        'expectancy':       round(expectancy, 4),
        'sharpe':           round(sharpe, 3),
        'longs':            sum(1 for t in trades if t['side'] == 'buy'),
        'shorts':           sum(1 for t in trades if t['side'] == 'sell'),
        'tp_hits':          sum(1 for t in trades if t['reason'] == 'tp'),
        'sl_hits':          sum(1 for t in trades if t['reason'] == 'sl'),
        'max_hold_hits':    sum(1 for t in trades if t['reason'] == 'max_hold'),
        'avg_bars':         round(avg_bars, 1),
        'monthly':          {k: dict(v) for k, v in sorted(monthly.items())},
        'per_coin':         {k: dict(v) for k, v in per_coin.items()},
    }

# ══════════════════════════════════════════════════════════════
# SHARD RUNNER
# ══════════════════════════════════════════════════════════════
def run_shard(shard_idx):
    t0 = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} symbols: {symbols}")

    candle_map = {}
    def _fetch(sym):
        return sym, fetch_symbol(sym)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_fetch, sym): sym for sym in symbols}
        for fut in as_completed(futs):
            sym, data = fut.result()
            candle_map[sym] = data
            print(f"[Shard {shard_idx}] {sym}: {len(data)} candles")

    if sum(len(v) for v in candle_map.values()) == 0:
        print(f"[Shard {shard_idx}] GEO-BLOCK or no data. Aborting.")
        with open(f'shard_{shard_idx}.json', 'w') as f:
            json.dump({'shard': shard_idx, 'symbols': symbols, 'with_data': [],
                       'trades': [], 'stats_by_tp': {}, 'elapsed': time.time()-t0,
                       'error': 'GEO_BLOCK_OR_NO_DATA'}, f)
        return

    all_trades_by_tp = {tp['id']: [] for tp in TP_STRATEGIES}
    with_data = []

    for sym in symbols:
        candles = candle_map[sym]
        if len(candles) < MIN_BARS:
            print(f"[Shard {shard_idx}] {sym}: skipped ({len(candles)} candles)")
            continue
        with_data.append(sym)

        closes = [c[4] for c in candles]
        highs  = [c[2] for c in candles]
        lows   = [c[3] for c in candles]

        # Pre-compute all indicator series once per symbol
        e9_arr  = ema_series(closes, 9)
        e21_arr = ema_series(closes, 21)
        e50_arr = ema_series(closes, 50)
        rsi_arr = rsi_series(closes, RSI_PERIOD)

        for tp_cfg in TP_STRATEGIES:
            trades = _run_symbol(sym, candles, tp_cfg, rsi_arr, e9_arr, e21_arr, e50_arr)
            all_trades_by_tp[tp_cfg['id']].extend(trades)
            print(f"[Shard {shard_idx}] {sym} {tp_cfg['id']}: {len(trades)} trades")

    stats_by_tp = {tp['id']: compute_stats(all_trades_by_tp[tp['id']]) for tp in TP_STRATEGIES}
    all_trades_flat = [t for trades in all_trades_by_tp.values() for t in trades]

    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump({
            'shard': shard_idx, 'symbols': symbols, 'with_data': with_data,
            'trades': all_trades_flat, 'stats_by_tp': stats_by_tp,
            'elapsed': round(time.time()-t0, 1),
        }, f)
    print(f"[Shard {shard_idx}] Done in {round(time.time()-t0,1)}s — {len(with_data)} coins")


def _run_symbol(symbol, candles, tp_cfg, rsi_arr, e9_arr, e21_arr, e50_arr):
    """Backtest one symbol with pre-computed indicator arrays."""
    if len(candles) < MIN_BARS:
        return []

    ts_arr = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    n = len(closes)

    tp_type  = tp_cfg['tp_type']
    trades   = []
    risk_usd = CAPITAL * 0.0075
    notional = min(risk_usd / SL_PCT, CAPITAL * LEVERAGE)

    i = MIN_BARS
    while i < n - 1:
        # Signal uses pre-computed EMA arrays
        if i < 70:
            i += 1; continue

        slope_pct  = (e50_arr[i] - e50_arr[i-10]) / e50_arr[i-10] * 100
        trend_up   = slope_pct >  0.05
        trend_down = slope_pct < -0.05
        if not trend_up and not trend_down:
            i += 1; continue

        crossed_up   = e9_arr[i] > e21_arr[i] and e9_arr[i-1] <= e21_arr[i-1]
        crossed_down = e9_arr[i] < e21_arr[i] and e9_arr[i-1] >= e21_arr[i-1]

        if trend_up   and not crossed_up:   i += 1; continue
        if trend_down and not crossed_down: i += 1; continue
        if not crossed_up and not crossed_down: i += 1; continue

        adx_val, _, _ = adx_at(highs, lows, closes, 14, i)
        if adx_val < 22:
            i += 1; continue

        sig = 'buy' if crossed_up else 'sell'

        if i + 1 >= n:
            break

        raw_entry = opens[i+1]
        if sig == 'buy':
            entry_p  = raw_entry * (1 + FEE + SLIP)
            sl_price = entry_p * (1 - SL_PCT)
        else:
            entry_p  = raw_entry * (1 - FEE - SLIP)
            sl_price = entry_p * (1 + SL_PCT)

        entry_ts  = ts_arr[i+1]
        bars_held = 0

        j = i + 1
        appended = False
        while j < n:
            bars_held = j - (i + 1)
            h = highs[j]; l = lows[j]; c = closes[j]

            # SL first
            sl_hit = (l <= sl_price) if sig == 'buy' else (h >= sl_price)
            if sl_hit:
                gross = (sl_price - entry_p)/entry_p if sig == 'buy' else (entry_p - sl_price)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, ts_arr[j], entry_p,
                                     sl_price, notional*net, 'sl', bars_held, tp_cfg['id'], adx_val))
                appended = True; break

            # Indicator TP
            rsi_val  = rsi_arr[j]
            rsi_exit = False
            if tp_type in ('rsi', 'combo'):
                rsi_exit = (sig == 'buy' and rsi_val >= RSI_OB) or \
                           (sig == 'sell' and rsi_val <= RSI_OS)

            ema_exit = False
            if tp_type in ('ema_cross', 'combo') and j > 0:
                ema_exit = (sig == 'buy'  and e9_arr[j] < e21_arr[j] and e9_arr[j-1] >= e21_arr[j-1]) or \
                           (sig == 'sell' and e9_arr[j] > e21_arr[j] and e9_arr[j-1] <= e21_arr[j-1])

            if rsi_exit or ema_exit:
                gross = (c - entry_p)/entry_p if sig == 'buy' else (entry_p - c)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, ts_arr[j], entry_p,
                                     c, notional*net, 'tp', bars_held, tp_cfg['id'], adx_val))
                appended = True; break

            # Max hold
            if bars_held >= MAX_BARS:
                gross = (c - entry_p)/entry_p if sig == 'buy' else (entry_p - c)/entry_p
                net   = gross - (FEE + SLIP) * 2
                trades.append(_trade(symbol, sig, entry_ts, ts_arr[j], entry_p,
                                     c, notional*net, 'max_hold', bars_held, tp_cfg['id'], adx_val))
                appended = True; break

            j += 1

        if not appended and j >= n:
            c = closes[-1]
            gross = (c - entry_p)/entry_p if sig == 'buy' else (entry_p - c)/entry_p
            net   = gross - (FEE + SLIP) * 2
            trades.append(_trade(symbol, sig, entry_ts, ts_arr[-1], entry_p,
                                 c, notional*net, 'end_of_data', bars_held, tp_cfg['id'], adx_val))
        i += 1

    return trades

# ══════════════════════════════════════════════════════════════
# MERGE
# ══════════════════════════════════════════════════════════════
def merge_shards():
    import glob
    shard_files = sorted(glob.glob('shard_*.json'))
    print(f"Merging {len(shard_files)} shard files...")

    all_trades_by_tp = {tp['id']: [] for tp in TP_STRATEGIES}
    all_symbols = []; with_data = []

    for sf in shard_files:
        with open(sf) as f:
            shard = json.load(f)
        all_symbols.extend(shard.get('symbols', []))
        with_data.extend(shard.get('with_data', []))
        for trade in shard.get('trades', []):
            tp_id = trade.get('tp_id')
            if tp_id in all_trades_by_tp:
                all_trades_by_tp[tp_id].append(trade)

    results_by_tp = {}
    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        results_by_tp[tp_id] = {
            'config': tp_cfg,
            'stats':  compute_stats(all_trades_by_tp[tp_id]),
            'trade_count': len(all_trades_by_tp[tp_id]),
        }

    report = {
        'period':              f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe':           TIMEFRAME,
        'leverage':            LEVERAGE,
        'sl_pct':              SL_PCT,
        'symbols_attempted':   len(set(all_symbols)),
        'symbols_with_data':   len(set(with_data)),
        'tp_strategies':       results_by_tp,
    }
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    lines = []
    lines.append("=" * 72)
    lines.append("  STRATEGY G VAR_D — INDICATOR TP EXIT COMPARISON")
    lines.append("  VAR_RSI: RSI14>75/<25  |  VAR_EMA: EMA9/21 cross  |  VAR_COMBO: either")
    lines.append("=" * 72)
    lines.append(f"  Period   : {report['period']}  (3 years)")
    lines.append(f"  Timeframe: {TIMEFRAME}   Leverage: {LEVERAGE}x")
    lines.append(f"  SL       : {SL_PCT*100:.0f}% fixed   Max Hold: {MAX_BARS} bars (10 days)")
    lines.append(f"  Capital  : ${CAPITAL}   Fee: {FEE*100:.2f}%   Slip: {SLIP*100:.2f}%")
    lines.append(f"  Coins    : {report['symbols_attempted']} attempted, {report['symbols_with_data']} with data")
    lines.append("=" * 72)
    lines.append("")

    headers = ["TP Strategy", "Trades", "WR%", "PF", "Net PnL", "MaxDD%", "Sharpe", "AvgBars", "TPs", "SLs", "MaxHold"]
    col_w   = [14, 7, 7, 7, 10, 8, 8, 9, 7, 7, 9]
    def _pad(s, w): return str(s)[:w].ljust(w)
    def _row(vals): return "  " + "  ".join(_pad(v, w) for v, w in zip(vals, col_w))

    lines.append(_row(headers))
    lines.append("  " + "-" * 72)

    best_tp = None; best_pf = 0.0
    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        s = results_by_tp[tp_id]['stats']
        pf_str = f"{s['profit_factor']:.3f}" if s['profit_factor'] < 9999 else "∞"
        row = [tp_id, s['total'], f"{s['win_rate']:.1f}%", pf_str,
               f"${s['net_pnl']:+.2f}", f"{s['max_drawdown_pct']:.1f}%",
               f"{s['sharpe']:.3f}", f"{s['avg_bars']:.0f}",
               s['tp_hits'], s['sl_hits'], s['max_hold_hits']]
        lines.append(_row(row))
        pf_val = s['profit_factor'] if s['profit_factor'] < 9999 else 0.0
        if pf_val > best_pf:
            best_pf = pf_val; best_tp = tp_id

    lines.append("")
    lines.append("=" * 72)
    lines.append("  DETAILED BREAKDOWN")
    lines.append("=" * 72)

    for tp_cfg in TP_STRATEGIES:
        tp_id = tp_cfg['id']
        s = results_by_tp[tp_id]['stats']
        pf_str = f"{s['profit_factor']:.4f}" if s['profit_factor'] < 9999 else "∞"
        rec = "✅ USABLE" if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42 else "❌ NOT USABLE"

        lines.append(f"\n  ── {tp_id} ──")
        lines.append(f"     Trades      : {s['total']}")
        lines.append(f"     Win Rate    : {s['win_rate']:.2f}%")
        lines.append(f"     Prof Factor : {pf_str}")
        lines.append(f"     Net PnL     : ${s['net_pnl']:+.4f}")
        lines.append(f"     Max DD      : ${s['max_drawdown']:+.4f}  ({s['max_drawdown_pct']:.2f}%)")
        lines.append(f"     Sharpe      : {s['sharpe']:.3f}")
        lines.append(f"     Avg Win     : ${s['avg_win']:+.4f}")
        lines.append(f"     Avg Loss    : ${s['avg_loss']:+.4f}")
        lines.append(f"     Expectancy  : ${s['expectancy']:+.4f}")
        lines.append(f"     Longs       : {s['longs']}  Shorts: {s['shorts']}")
        lines.append(f"     TP hits     : {s['tp_hits']}  SL hits: {s['sl_hits']}  MaxHold: {s['max_hold_hits']}")
        lines.append(f"     Avg Bars    : {s['avg_bars']:.1f}")
        lines.append(f"     VERDICT     : {rec}")

        lines.append(f"     Monthly PnL:")
        for ym, mv in sorted(s['monthly'].items()):
            bar_len = min(30, int(abs(mv['pnl']) / 10))
            bar = ("+" if mv['pnl'] >= 0 else "-") * bar_len
            lines.append(f"       {ym}  ${mv['pnl']:+8.2f}  {bar:<30}  ({mv['w']}/{mv['n']})")

        ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
        lines.append(f"     Top 50 Coins (by PnL):")
        for sym, cd in ranked[:50]:
            lines.append(f"       {sym:<28}  {cd['n']:>4} trades  {cd['wr']:5.1f}% WR  ${cd['pnl']:+.4f}")

        lines.append(f"     Bottom 10 Coins (by PnL):")
        for sym, cd in ranked[-10:][::-1]:
            lines.append(f"       {sym:<28}  {cd['n']:>4} trades  {cd['wr']:5.1f}% WR  ${cd['pnl']:+.4f}")

    lines.append("")
    lines.append("=" * 72)
    if best_tp:
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

