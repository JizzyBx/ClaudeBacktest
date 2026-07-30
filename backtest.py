"""
InfinityX Multi-Strategy Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
8 strategies, each tested on their own coin list.
Data: data.binance.vision futures archive (no API key, no geo-block).
Workers: 10 parallel threads for data download.
Window: up to 5 years (2020-07 → 2025-06). If a coin has less history,
        uses whatever is available (min 30 candles required).
Output: backtest_summary.txt + backtest_report.json
"""

import os, io, json, csv, zipfile, urllib.request, urllib.error
import time, math, calendar
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ──────────────────────────────────────────────
# 1. STRATEGY DEFINITIONS  (exact from InfinityX.py)
# ──────────────────────────────────────────────
STRATEGIES = {
    'V1': {'name':'V1 · 15m Original',        'logic':'core',
           'tf':'15m','tp':3.0,'sl':2.0,
           'coins':['XRPUSDT','TIAUSDT','TURBOUSDT','SEIUSDT','1000RATSUSDT',
                    '1000BONKUSDT','EIGENUSDT','APTUSDT','REZUSDT','POPCATUSDT',
                    'DOGEUSDT','AVAXUSDT','BTCUSDT','LDOUSDT','BNBUSDT',
                    'BOMEUSDT','FETUSDT','RUNEUSDT','ATOMUSDT','STXUSDT','AXSUSDT']},
    'V2': {'name':'V2 · 15m Tight Exits',     'logic':'core',
           'tf':'15m','tp':2.8,'sl':1.7,
           'coins':['1000RATSUSDT','XRPUSDT','TIAUSDT','TURBOUSDT','SEIUSDT',
                    '1000BONKUSDT','BTCUSDT','EIGENUSDT','APTUSDT','REZUSDT',
                    'POPCATUSDT','AVAXUSDT','DOGEUSDT','LDOUSDT','BNBUSDT',
                    'RUNEUSDT','BOMEUSDT','FETUSDT','AXSUSDT','ATOMUSDT',
                    'STXUSDT','TRXUSDT','ALGOUSDT']},
    'V3': {'name':'V3 · 30m Candles',         'logic':'core',
           'tf':'30m','tp':3.0,'sl':2.0,
           'coins':['BTCUSDT','XRPUSDT','TIAUSDT','BNBUSDT','DOGEUSDT','SEIUSDT',
                    'APTUSDT','AVAXUSDT','FETUSDT','TRXUSDT','ALGOUSDT','STXUSDT',
                    'DOTUSDT']},
    'V4': {'name':'V4 · 1H Candles',          'logic':'core',
           'tf':'1h','tp':3.0,'sl':2.0,
           'coins':['BTCUSDT','TRUMPUSDT','AVAXUSDT','XRPUSDT','TIAUSDT','BNBUSDT',
                    'DOGEUSDT','SEIUSDT','APTUSDT','SOLUSDT','FETUSDT','ATOMUSDT',
                    'STXUSDT','DOTUSDT','ALGOUSDT','TRXUSDT','RUNEUSDT','LDOUSDT',
                    'AXSUSDT','REZUSDT']},
    'V5': {'name':'V5 · 15m + RSI Confirm',   'logic':'core_rsi',
           'tf':'15m','tp':3.0,'sl':2.0,
           'rsi_long':(45,70),'rsi_short':(30,55),
           'coins':['TIAUSDT','XRPUSDT','TURBOUSDT','SEIUSDT','DOGEUSDT',
                    '1000RATSUSDT','APTUSDT','BTCUSDT','REZUSDT','POPCATUSDT',
                    'AVAXUSDT','BNBUSDT','FETUSDT','LDOUSDT','RUNEUSDT',
                    'BOMEUSDT','AXSUSDT','ATOMUSDT','STXUSDT','ALGOUSDT','TRXUSDT']},
    'V7': {'name':'V7 · 15m Clean Whitelist', 'logic':'core',
           'tf':'15m','tp':2.8,'sl':1.7,
           'coins':['DOGEUSDT','TIAUSDT','XRPUSDT','SEIUSDT','APTUSDT','REZUSDT',
                    'AXSUSDT','1000XECUSDT','STXUSDT','ALGOUSDT','TRXUSDT',
                    'FETUSDT','DOTUSDT']},
    'S3': {'name':'S3 · Donchian + Volume',   'logic':'donchian',
           'tf':'30m','tp':2.5,'sl':1.6,'don_period':20,'vol_mult':1.4,
           'coins':['FILUSDT','KNCUSDT','DYDXUSDT','CFXUSDT','WIFUSDT','MANAUSDT',
                    'MKRUSDT','1000FLOKIUSDT','ICPUSDT']},
    'S4': {'name':'S4 · EMA Pullback',        'logic':'ema_pullback',
           'tf':'1h','tp':2.8,'sl':1.7,
           'coins':['GRTUSDT','ZILUSDT','MANAUSDT','WLDUSDT']},
}

# ──────────────────────────────────────────────
# 2. BACKTEST SETTINGS
# ──────────────────────────────────────────────
CAPITAL       = 10_000.0
RISK_PCT      = 0.0075        # 0.75% per trade
FEE_PCT       = 0.0005        # 0.05% per side
SLIP_PCT      = 0.0002        # 0.02% per side
MAX_POSITIONS = 6
ADX_MIN       = 22

# Date range: 5 years back from 2025-06
END_YEAR, END_MONTH   = 2025, 6
START_YEAR, START_MONTH = 2020, 7

TF_MINUTES = {'15m': 15, '30m': 30, '1h': 60}
WORKERS = 10

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
DAILY_URL = "https://data.binance.vision/data/futures/um/daily/klines"

# ──────────────────────────────────────────────
# 3. DATA DOWNLOAD
# ──────────────────────────────────────────────
def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12: m = 1; y += 1

def fetch_monthly_csv(symbol, tf, year, month):
    tag = f"{year}-{month:02d}"
    url = f"{BASE_URL}/{symbol}/{tf}/{symbol}-{tf}-{tag}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None   # symbol didn't exist yet or delisted
        raise
    except Exception:
        return None

def parse_rows(rows):
    """Convert raw CSV rows to (open_time_ms, open, high, low, close, volume)."""
    out = []
    for r in rows:
        if not r or r[0].startswith('open'): continue
        try:
            ts = int(r[0])
            if ts > 10**14: ts //= 1000   # microsecond guard
            out.append((ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
        except (ValueError, IndexError):
            continue
    return out

def download_symbol_tf(symbol, tf):
    """Download all available monthly data for symbol/tf, return sorted candle list."""
    all_candles = []
    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        rows = fetch_monthly_csv(symbol, tf, y, m)
        if rows is not None:
            all_candles.extend(parse_rows(rows))
    # deduplicate and sort
    seen = set()
    deduped = []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0]); deduped.append(c)
    deduped.sort(key=lambda x: x[0])
    return deduped

# ──────────────────────────────────────────────
# 4. INDICATORS  (exact logic from InfinityX.py)
# ──────────────────────────────────────────────
def ema(values, period):
    if not values: return []
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def rsi_calc(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0)); losses.append(max(-d, 0.0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3: return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i-1]; down = lows[i-1] - lows[i]
        pdm.append(up if up > down and up > 0 else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
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
    if len(dx) < period: return 0.0, pdi[-1], mdi[-1]
    adxv = sum(dx[:period]) / period
    for d in dx[period:]: adxv = (adxv * (period-1) + d) / period
    return max(0.0, min(100.0, adxv)), pdi[-1], mdi[-1]

def atr_calc(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if not trs: return closes[-1] * 0.005
    if len(trs) < period: return sum(trs) / len(trs)
    a = sum(trs[:period]) / period
    for t in trs[period:]: a = (a * (period-1) + t) / period
    return a

def atr_series(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < period: return trs
    out = []; a = sum(trs[:period]) / period; out.append(a)
    for t in trs[period:]: a = (a*(period-1)+t)/period; out.append(a)
    return out

def sma(values, period):
    if not values: return 0.0
    if len(values) < period: return sum(values) / len(values)
    return sum(values[-period:]) / period

def _slope(e50):
    if len(e50) < 11 or e50[-11] == 0: return 0.0
    return (e50[-1] - e50[-11]) / e50[-11] * 100

# ──────────────────────────────────────────────
# 5. SIGNAL LOGIC  (mirrors InfinityX.py exactly)
# ──────────────────────────────────────────────
def _core_cross(closes, adx, slope_pct):
    e9 = ema(closes, 9); e21 = ema(closes, 21)
    crossed_up   = e9[-1] > e21[-1] and e9[-2] <= e21[-2]
    crossed_down = e9[-1] < e21[-1] and e9[-2] >= e21[-2]
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    adx_ok = adx >= ADX_MIN
    sig = None
    if adx_ok and trend_up   and crossed_up:   sig = 'buy'
    if adx_ok and trend_down and crossed_down: sig = 'sell'
    return sig

def signal_core(closes, highs, lows):
    if len(closes) < 60: return None, None
    adx, _, _ = adx_calc(highs, lows, closes)
    e50 = ema(closes, 50); slope = _slope(e50)
    sig = _core_cross(closes, adx, slope)
    if not sig: return None, None
    atr = atr_calc(highs, lows, closes)
    return sig, atr

def signal_core_rsi(closes, highs, lows, rsi_long, rsi_short):
    if len(closes) < 60: return None, None
    adx, _, _ = adx_calc(highs, lows, closes)
    e50 = ema(closes, 50); slope = _slope(e50)
    sig = _core_cross(closes, adx, slope)
    if not sig: return None, None
    rsi = rsi_calc(closes, 14)
    lo, hi = rsi_long if sig == 'buy' else rsi_short
    if not (lo <= rsi <= hi): return None, None
    atr = atr_calc(highs, lows, closes)
    return sig, atr

def signal_donchian(closes, highs, lows, don_period, vol_mult):
    if len(closes) < don_period + 55: return None, None
    dc_high  = max(highs[-don_period-1:-1])
    dc_low   = min(lows[-don_period-1:-1])
    vols_sl  = [0.0] * len(closes)   # placeholder — volume passed separately
    return None, None   # handled below in full version with vols

def signal_donchian_full(closes, highs, lows, vols, don_period, vol_mult):
    if len(closes) < don_period + 55: return None, None
    dc_high = max(highs[-don_period-1:-1])
    dc_low  = min(lows[-don_period-1:-1])
    vol_sma = sma(vols[-don_period-1:-1], don_period)
    atrs = atr_series(highs, lows, closes, 14)
    if len(atrs) < 51: return None, None
    atr_now   = atrs[-1]
    atr_sma50 = sma(atrs[-51:-1], 50)
    close = closes[-1]
    vol_ok = vols[-1] > vol_mult * vol_sma
    exp_ok = atr_now > atr_sma50
    sig = None
    if close > dc_high and vol_ok and exp_ok: sig = 'buy'
    elif close < dc_low and vol_ok and exp_ok: sig = 'sell'
    if not sig: return None, None
    return sig, atr_now

def signal_ema_pullback(closes, highs, lows):
    if len(closes) < 60: return None, None
    e21 = ema(closes, 21); e50 = ema(closes, 50)
    adx, _, _ = adx_calc(highs, lows, closes)
    uptrend   = e21[-1] > e50[-1] and e50[-1] > e50[-5]
    downtrend = e21[-1] < e50[-1] and e50[-1] < e50[-5]
    sig = None
    if uptrend   and adx >= 22 and lows[-1]  <= e21[-1] * 1.001 and closes[-1] > e21[-1]: sig = 'buy'
    elif downtrend and adx >= 22 and highs[-1] >= e21[-1] * 0.999 and closes[-1] < e21[-1]: sig = 'sell'
    if not sig: return None, None
    atr = atr_calc(highs, lows, closes)
    return sig, atr

# ──────────────────────────────────────────────
# 6. BACKTESTER  (bar-by-bar, closed-candles only)
# ──────────────────────────────────────────────
COST_PCT = FEE_PCT + SLIP_PCT  # per side (entry + exit)

def backtest_strategy_coin(sid, cfg, candles):
    """
    Returns list of trade dicts for one strategy on one coin.
    Uses percentage-of-equity risk sizing.
    TP/SL = tp_mult * ATR and sl_mult * ATR from entry price.
    """
    if len(candles) < 65:
        return []

    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    vols   = [c[5] for c in candles]
    times  = [c[0] for c in candles]

    tp_mult  = cfg['tp']
    sl_mult  = cfg['sl']
    logic    = cfg['logic']

    # strategy-specific params
    rsi_long   = cfg.get('rsi_long',  (45, 70))
    rsi_short  = cfg.get('rsi_short', (30, 55))
    don_period = cfg.get('don_period', 20)
    vol_mult   = cfg.get('vol_mult',   1.4)

    trades  = []
    position = None          # {'side', 'entry', 'tp', 'sl', 'open_bar', 'size_usd', 'equity_at_open'}
    warmup   = 60            # bars needed for indicators

    equity = CAPITAL         # local equity for this coin-strategy pair (approximation for sizing)

    for i in range(warmup, len(closes) - 1):
        c  = closes[:i+1]
        h  = highs[:i+1]
        lo = lows[:i+1]
        v  = vols[:i+1]

        # ── check exit on NEXT bar open (closed candle signal → next bar execution)
        if position:
            next_open  = opens[i+1]
            next_high  = highs[i+1]
            next_low   = lows[i+1]
            next_close = closes[i+1]
            side = position['side']
            tp = position['tp']; sl_price = position['sl']

            hit_tp = hit_sl = False
            if side == 'buy':
                if next_high >= tp:  hit_tp = True
                if next_low  <= sl_price: hit_sl = True
            else:  # sell
                if next_low  <= tp:  hit_tp = True
                if next_high >= sl_price: hit_sl = True

            if hit_tp or hit_sl:
                if hit_tp and hit_sl:
                    # both wicked — check which comes first by position within candle
                    # conservative: assume SL hit first
                    hit_tp = False; hit_sl = True

                exit_price = tp if hit_tp else sl_price
                side_mult  = 1 if side == 'buy' else -1
                gross_pct  = side_mult * (exit_price - position['entry']) / position['entry']
                net_pct    = gross_pct - 2 * COST_PCT   # entry + exit fees/slip
                pnl        = net_pct * position['size_usd']

                trades.append({
                    'sid'     : sid,
                    'symbol'  : None,   # filled by caller
                    'side'    : side,
                    'entry'   : position['entry'],
                    'exit'    : exit_price,
                    'tp'      : tp,
                    'sl'      : sl_price,
                    'open_ts' : times[position['open_bar']],
                    'close_ts': times[i+1],
                    'pnl'     : round(pnl, 4),
                    'result'  : 'win' if hit_tp else 'loss',
                    'bars'    : i+1 - position['open_bar'],
                })
                equity += pnl
                position = None
                continue   # don't also enter on this bar

        if position:
            continue   # already in a trade, skip signal check

        # ── signal on current closed candle (bar i) → entry at open of bar i+1
        sig = atr = None
        if logic == 'core':
            sig, atr = signal_core(c, h, lo)
        elif logic == 'core_rsi':
            sig, atr = signal_core_rsi(c, h, lo, rsi_long, rsi_short)
        elif logic == 'donchian':
            sig, atr = signal_donchian_full(c, h, lo, v, don_period, vol_mult)
        elif logic == 'ema_pullback':
            sig, atr = signal_ema_pullback(c, h, lo)

        if sig and atr and i + 1 < len(closes):
            entry = opens[i+1]
            if entry <= 0 or atr <= 0:
                continue
            tp_dist = tp_mult * atr
            sl_dist = sl_mult * atr
            risk_usd = equity * RISK_PCT
            # position size based on SL distance in %
            sl_pct   = sl_dist / entry
            size_usd = risk_usd / sl_pct if sl_pct > 0 else risk_usd * 10

            if sig == 'buy':
                tp_price = entry + tp_dist
                sl_price = entry - sl_dist
            else:
                tp_price = entry - tp_dist
                sl_price = entry + sl_dist

            position = {
                'side'    : sig,
                'entry'   : entry,
                'tp'      : tp_price,
                'sl'      : sl_price,
                'open_bar': i + 1,
                'size_usd': size_usd,
            }

    return trades

# ──────────────────────────────────────────────
# 7. ANALYTICS
# ──────────────────────────────────────────────
def calc_metrics(trades):
    if not trades:
        return {
            'total_trades':0,'wins':0,'losses':0,'win_rate':0.0,
            'profit_factor':0.0,'net_pnl':0.0,'max_drawdown':0.0,
            'avg_win':0.0,'avg_loss':0.0,'expectancy':0.0,
            'avg_bars':0.0,'long_trades':0,'short_trades':0,
            'long_win_rate':0.0,'short_win_rate':0.0,
            'max_win_streak':0,'max_loss_streak':0,
            'sharpe':0.0,'sortino':0.0,
        }
    wins   = [t for t in trades if t['result'] == 'win']
    losses = [t for t in trades if t['result'] == 'loss']
    gp = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))
    pf = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)

    longs  = [t for t in trades if t['side'] == 'buy']
    shorts = [t for t in trades if t['side'] == 'sell']
    lw = sum(1 for t in longs  if t['result']=='win')
    sw = sum(1 for t in shorts if t['result']=='win')

    # drawdown
    equity = CAPITAL
    peak = CAPITAL; dd = 0.0
    for t in trades:
        equity += t['pnl']
        if equity > peak: peak = equity
        dd = max(dd, (peak - equity) / peak * 100)

    # streaks
    max_win_s = max_loss_s = cur_w = cur_l = 0
    for t in trades:
        if t['result'] == 'win':
            cur_w += 1; cur_l = 0; max_win_s = max(max_win_s, cur_w)
        else:
            cur_l += 1; cur_w = 0; max_loss_s = max(max_loss_s, cur_l)

    # Sharpe / Sortino (daily approximation using PnL per trade)
    pnls = [t['pnl'] for t in trades]
    avg_pnl = sum(pnls)/len(pnls) if pnls else 0
    var     = sum((p - avg_pnl)**2 for p in pnls)/len(pnls) if pnls else 0
    std     = math.sqrt(var)
    sharpe  = (avg_pnl / std * math.sqrt(252)) if std > 0 else 0.0
    neg     = [p for p in pnls if p < 0]
    dstd    = math.sqrt(sum(p**2 for p in neg)/len(neg)) if neg else 0
    sortino = (avg_pnl / dstd * math.sqrt(252)) if dstd > 0 else 0.0

    return {
        'total_trades' : len(trades),
        'wins'         : len(wins),
        'losses'       : len(losses),
        'win_rate'     : len(wins)/len(trades)*100,
        'profit_factor': round(pf, 4),
        'net_pnl'      : round(sum(t['pnl'] for t in trades), 2),
        'max_drawdown' : round(dd, 2),
        'avg_win'      : round(gp/len(wins), 2)   if wins   else 0.0,
        'avg_loss'     : round(-gl/len(losses), 2) if losses else 0.0,
        'expectancy'   : round(sum(t['pnl'] for t in trades)/len(trades), 4),
        'avg_bars'     : round(sum(t['bars'] for t in trades)/len(trades), 1),
        'long_trades'  : len(longs),
        'short_trades' : len(shorts),
        'long_win_rate' : round(lw/len(longs)*100,1)  if longs  else 0.0,
        'short_win_rate': round(sw/len(shorts)*100,1) if shorts else 0.0,
        'max_win_streak' : max_win_s,
        'max_loss_streak': max_loss_s,
        'sharpe'  : round(sharpe, 3),
        'sortino' : round(sortino, 3),
    }

def monthly_pnl(trades):
    by_month = defaultdict(float)
    for t in trades:
        if t.get('close_ts'):
            dt = datetime.utcfromtimestamp(t['close_ts'] / 1000)
            key = dt.strftime('%Y-%m')
            by_month[key] += t['pnl']
    return {k: round(v, 2) for k, v in sorted(by_month.items())}

# ──────────────────────────────────────────────
# 8. PARALLEL DATA DOWNLOAD
# ──────────────────────────────────────────────
def collect_all_data():
    """Collect unique (symbol, tf) pairs across all strategies and download in parallel."""
    jobs = set()
    for cfg in STRATEGIES.values():
        for coin in cfg['coins']:
            jobs.add((coin, cfg['tf']))

    results = {}
    print(f"[DATA] Downloading {len(jobs)} symbol/tf combinations with {WORKERS} workers...")

    def worker(symbol, tf):
        candles = download_symbol_tf(symbol, tf)
        return (symbol, tf), candles

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker, sym, tf): (sym, tf) for sym, tf in jobs}
        done = 0
        for fut in as_completed(futures):
            key, candles = fut.result()
            results[key] = candles
            done += 1
            sym, tf = key
            bars = len(candles)
            if bars > 0:
                first = datetime.utcfromtimestamp(candles[0][0]/1000).strftime('%Y-%m')
                last  = datetime.utcfromtimestamp(candles[-1][0]/1000).strftime('%Y-%m')
                print(f"  [{done:3d}/{len(jobs)}] {sym:20s} {tf} — {bars:6d} bars  ({first} → {last})")
            else:
                print(f"  [{done:3d}/{len(jobs)}] {sym:20s} {tf} — NO DATA")

    return results

# ──────────────────────────────────────────────
# 9. MAIN RUNNER
# ──────────────────────────────────────────────
def run():
    t0 = time.time()
    print("=" * 65)
    print("  InfinityX Multi-Strategy Backtest")
    print(f"  Window : {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}  (up to 5 yrs)")
    print(f"  Capital: ${CAPITAL:,.0f}  Risk: {RISK_PCT*100:.2f}%/trade")
    print(f"  Fees   : {FEE_PCT*100:.3f}% each side  Slip: {SLIP_PCT*100:.3f}% each side")
    print("=" * 65)

    # Download all data
    data_cache = collect_all_data()

    # Run backtests per strategy per coin
    all_strategy_results = {}   # sid -> {coin -> [trades]}
    all_trades_flat = []

    for sid, cfg in STRATEGIES.items():
        print(f"\n[{sid}] {cfg['name']}  ({cfg['tf']})")
        strategy_coin_trades = {}

        for coin in cfg['coins']:
            candles = data_cache.get((coin, cfg['tf']), [])
            if len(candles) < 65:
                strategy_coin_trades[coin] = []
                print(f"  {coin:22s} — skipped (only {len(candles)} bars)")
                continue

            trades = backtest_strategy_coin(sid, cfg, candles)
            for t in trades: t['symbol'] = coin
            strategy_coin_trades[coin] = trades

            m = calc_metrics(trades)
            status = "✅ PASS" if m['profit_factor'] >= 1.5 and m['win_rate'] >= 42 else "❌ FAIL"
            first = datetime.utcfromtimestamp(candles[0][0]/1000).strftime('%Y-%m')
            last  = datetime.utcfromtimestamp(candles[-1][0]/1000).strftime('%Y-%m')
            print(f"  {coin:22s} {len(candles):6d} bars ({first}→{last})  "
                  f"T:{m['total_trades']:4d}  WR:{m['win_rate']:5.1f}%  PF:{m['profit_factor']:.3f}  "
                  f"PnL:${m['net_pnl']:8.2f}  {status}")

            all_trades_flat.extend(trades)

        all_strategy_results[sid] = strategy_coin_trades

    elapsed = time.time() - t0

    # ── Build report
    report = {'meta': {
        'run_date'   : datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'start'      : f"{START_YEAR}-{START_MONTH:02d}",
        'end'        : f"{END_YEAR}-{END_MONTH:02d}",
        'capital'    : CAPITAL,
        'risk_pct'   : RISK_PCT,
        'fee_pct'    : FEE_PCT,
        'slip_pct'   : SLIP_PCT,
        'adx_min'    : ADX_MIN,
        'workers'    : WORKERS,
        'elapsed_s'  : round(elapsed, 1),
    }, 'strategies': {}}

    lines = []
    lines.append("=" * 65)
    lines.append("  InfinityX Multi-Strategy Backtest Results")
    lines.append(f"  Run: {report['meta']['run_date']}  |  Elapsed: {elapsed:.0f}s")
    lines.append("=" * 65)

    for sid, cfg in STRATEGIES.items():
        coin_trades = all_strategy_results[sid]
        all_s_trades = [t for ts in coin_trades.values() for t in ts]
        agg = calc_metrics(all_s_trades)
        verdict = "✅ USABLE" if agg['profit_factor'] >= 1.5 and agg['win_rate'] >= 42 else "❌ NOT YET"

        lines.append(f"\n{'─'*65}")
        lines.append(f"  {sid} — {cfg['name']}  [{cfg['tf']}]  TP:{cfg['tp']}×ATR  SL:{cfg['sl']}×ATR")
        lines.append(f"  AGGREGATE: {verdict}")
        lines.append(f"{'─'*65}")
        lines.append(f"  Trades  : {agg['total_trades']}  "
                     f"(Longs:{agg['long_trades']} / Shorts:{agg['short_trades']})")
        lines.append(f"  Win Rate: {agg['win_rate']:.1f}%  "
                     f"(Long WR:{agg['long_win_rate']}%  Short WR:{agg['short_win_rate']}%)")
        lines.append(f"  PF      : {agg['profit_factor']:.3f}")
        lines.append(f"  Net PnL : ${agg['net_pnl']:,.2f}")
        lines.append(f"  Max DD  : {agg['max_drawdown']:.2f}%")
        lines.append(f"  Avg Win : ${agg['avg_win']:.2f}  Avg Loss: ${agg['avg_loss']:.2f}")
        lines.append(f"  Expect  : ${agg['expectancy']:.4f}/trade  Avg Duration: {agg['avg_bars']:.1f} bars")
        lines.append(f"  Streaks : W{agg['max_win_streak']} / L{agg['max_loss_streak']}")
        lines.append(f"  Sharpe  : {agg['sharpe']}  Sortino: {agg['sortino']}")

        # Per-coin table
        per_coin_data = []
        for coin in cfg['coins']:
            ts = coin_trades.get(coin, [])
            m  = calc_metrics(ts)
            per_coin_data.append((coin, m))
        per_coin_data.sort(key=lambda x: x[1]['profit_factor'], reverse=True)

        lines.append(f"\n  Per-Coin (sorted by PF):")
        lines.append(f"  {'Coin':<22} {'T':>5} {'WR':>6} {'PF':>7} {'PnL':>10} {'DD':>7} {'Bars':>6} {'Status'}")
        lines.append(f"  {'─'*22} {'─'*5} {'─'*6} {'─'*7} {'─'*10} {'─'*7} {'─'*6} {'─'*7}")
        for coin, m in per_coin_data:
            st = "✅" if m['profit_factor'] >= 1.5 and m['win_rate'] >= 42 else "❌"
            lines.append(f"  {coin:<22} {m['total_trades']:>5} {m['win_rate']:>5.1f}% "
                         f"{m['profit_factor']:>7.3f} ${m['net_pnl']:>9.2f} "
                         f"{m['max_drawdown']:>6.1f}% {m['avg_bars']:>6.1f} {st}")

        # Monthly PnL
        monthly = monthly_pnl(all_s_trades)
        if monthly:
            lines.append(f"\n  Monthly PnL:")
            row = ""
            for k, v in monthly.items():
                cell = f"  {k}: ${v:+.0f}"
                row += cell
                if len(row) > 80: lines.append(row); row = ""
            if row: lines.append(row)

        # Build report entry
        report['strategies'][sid] = {
            'name'       : cfg['name'],
            'tf'         : cfg['tf'],
            'aggregate'  : agg,
            'verdict'    : verdict,
            'per_coin'   : {coin: calc_metrics(coin_trades.get(coin,[])) for coin in cfg['coins']},
            'monthly_pnl': monthly,
            'trades'     : all_s_trades,
        }

    # Overall cross-strategy summary
    lines.append(f"\n{'='*65}")
    lines.append("  CROSS-STRATEGY SUMMARY")
    lines.append(f"{'='*65}")
    lines.append(f"  {'Strategy':<8} {'Name':<30} {'Trades':>7} {'WR':>6} {'PF':>7} {'Net PnL':>11} {'Status'}")
    lines.append(f"  {'─'*8} {'─'*30} {'─'*7} {'─'*6} {'─'*7} {'─'*11} {'─'*8}")
    for sid, cfg in STRATEGIES.items():
        all_s = [t for ts in all_strategy_results[sid].values() for t in ts]
        m = calc_metrics(all_s)
        st = "✅ PASS" if m['profit_factor'] >= 1.5 and m['win_rate'] >= 42 else "❌ FAIL"
        lines.append(f"  {sid:<8} {cfg['name']:<30} {m['total_trades']:>7} "
                     f"{m['win_rate']:>5.1f}% {m['profit_factor']:>7.3f} "
                     f"${m['net_pnl']:>10.2f} {st}")

    lines.append(f"\n  Targets: PF ≥ 1.5  |  WR ≥ 42%")
    lines.append(f"  Total elapsed: {elapsed:.0f}s")
    lines.append("=" * 65)

    summary = "\n".join(lines)
    print("\n" + summary)

    with open("backtest_summary.txt", "w") as f:
        f.write(summary)

    # Trim trades from JSON to avoid huge file — keep last 500 per strategy
    for sid in report['strategies']:
        t = report['strategies'][sid]['trades']
        report['strategies'][sid]['trades'] = t[-500:]

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n[DONE] backtest_summary.txt + backtest_report.json written.")

if __name__ == "__main__":
    run()
