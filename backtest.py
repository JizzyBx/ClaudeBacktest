"""
Backtest: RSI Divergence + EMA21 Bounce (S2-B)
Strategy extracted from Infinity Trading Bot v1 by Paqu
Timeframe : 30m candles
Period    : July 2023 – July 2025 (25 months)
Universe  : 98 coins (bot whitelist, symbol-fixed)
Capital   : $10,000 shared equity
Risk/trade: 0.75% of current equity
Fees      : 0.05% per side | Slippage: 0.02% per side
Max pos   : 6 concurrent (pipeline default) + 3 (bot setting)
TP        : 3 × ATR14  |  SL : 2 × ATR14

OPTIMIZATIONS (no quality loss):
  - 50 parallel workers for fetch
  - All indicators pre-baked once per symbol before timeline loop
  - O(1) indicator lookup per bar instead of O(N) recompute

DIAGNOSTICS:
  1. Long vs Short separate verdicts
  2. Per-coin thin-data flag (<20 trades)
  3. Quarterly PnL breakdown
  4. ADX band split: <20 vs 20-30
"""

import urllib.request, zipfile, io, csv, json, math, time, sys, threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
START_YEAR, START_MONTH = 2023, 7
END_YEAR,   END_MONTH   = 2025, 7
INTERVAL    = "30m"
CAPITAL     = 10_000.0
RISK_PCT    = 0.0075
FEE         = 0.0005
SLIP        = 0.0002
MAX_POS     = 6
MAX_POS_BOT = 3

TP_MULT    = 3.0
SL_MULT    = 2.0
ADX_MAX    = 30
VOL_MULT   = 1.2
VOL_PERIOD = 10
LOOKBACK   = 40
MIN_BARS   = 120
MIN_TRADES = 20

FETCH_WORKERS = 50

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ─────────────────────────────────────────────
# SYMBOLS
# ─────────────────────────────────────────────
SYMBOLS = [
    'WLFIUSDT','TRBUSDT','GNSUSDT','CAKEUSDT','PNUTUSDT',
    'TIAUSDT','PHAUSDT','OSMOUSDT','STGUSDT','QUICKUSDT',
    'ACHUSDT','GRTUSDT','RAREUSDT','C98USDT','IOSTUSDT',
    'IOUSDT','UMAUSDT','SUIUSDT','ZRXUSDT','PIXELUSDT',
    'APEUSDT','ZROUSDT','PLUMEUSDT','YGGUSDT','WOOUSDT',
    'ADXUSDT','DOTUSDT','COTIUSDT','BBUSDT','NXPCUSDT',
    'XAIUSDT','ALPINEUSDT','SENTUSDT','ATUSDT','TUTUSDT',
    'ZAMAUSDT','ATOMUSDT','WUSDT','TNSRUSDT','CUSDT',
    'POLUSDT','ACTUSDT','PARTIUSDT','ZKPUSDT','BROCCOLI714USDT',
    'ZENUSDT','USTCUSDT','RSRUSDT','ADAUSDT','OPUSDT',
    'ARPAUSDT','CFXUSDT','INITUSDT','WLDUSDT','HOLOUSDT',
    'IDUSDT','MANAUSDT','BIGTIMEUSDT','SYNUSDT','NEOUSDT',
    'ARUSDT','GUSDT','AMPUSDT','GMTUSDT','QTUMUSDT',
    'QIUSDT','LQTYUSDT','1000SHIBUSDT','KITEUSDT','INJUSDT',
    'DODOUSDT','MAVUSDT','MANTAUSDT','HOTUSDT','ACXUSDT',
    'ROBOUSDT','SIGNUSDT','AVAXUSDT','ICPUSDT','DGBUSDT',
    'TLMUSDT','DUSKUSDT','CETUSUSDT','PROVEUSDT','ACEUSDT',
    'IOTXUSDT','EIGENUSDT','BEAMXUSDT','STXUSDT','FIDAUSDT',
    'ZILUSDT','ONDOUSDT','LDOUSDT','BTCUSDT','FLUXUSDT',
    'RPLUSDT','PSGUSDT','ANIMEUSDT',
]

# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def fetch_month(sym, year, month):
    ym  = f"{year}-{month:02d}"
    url = f"{BASE_URL}/{sym}/{INTERVAL}/{sym}-{INTERVAL}-{ym}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            with z.open(z.namelist()[0]) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        out = []
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts > 10**14:
                ts //= 1000
            out.append({
                'ts':    ts,
                'open':  float(row[1]),
                'high':  float(row[2]),
                'low':   float(row[3]),
                'close': float(row[4]),
                'vol':   float(row[5]),
            })
        return out
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception:
        return None

def fetch_symbol(sym):
    all_c = []
    for year in range(START_YEAR, END_YEAR + 1):
        ms = START_MONTH if year == START_YEAR else 1
        me = END_MONTH   if year == END_YEAR   else 12
        for month in range(ms, me + 1):
            c = fetch_month(sym, year, month)
            if c:
                all_c.extend(c)
    seen, out = set(), []
    for c in all_c:
        if c['ts'] not in seen:
            seen.add(c['ts'])
            out.append(c)
    out.sort(key=lambda x: x['ts'])
    return out

# ─────────────────────────────────────────────
# INDICATOR BUILDERS  (full-array, called once per symbol)
# ─────────────────────────────────────────────
def build_ema(closes, period):
    k = 2.0 / (period + 1)
    r = [closes[0]]
    for v in closes[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def build_rsi(closes, period=14):
    n = len(closes)
    if n < period + 2:
        return [50.0] * n
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi = [50.0] * (period + 1)
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rs = ag / al if al else 999.0
        rsi.append(100.0 - 100.0 / (1.0 + rs))
    return rsi

def build_atr(highs, lows, closes, period=14):
    n = len(closes)
    atr = [0.0] * n
    if n < 2:
        return atr
    trs = []
    for i in range(1, n):
        trs.append(max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1])
        ))
    if not trs:
        return atr
    # first valid ATR at index period
    if len(trs) >= period:
        val = sum(trs[:period]) / period
        atr[period] = val
        for i in range(period, len(trs)):
            val = (val * (period - 1) + trs[i]) / period
            atr[i + 1] = val
    else:
        val = sum(trs) / len(trs)
        for i in range(1, n):
            atr[i] = val
    return atr

def build_adx(highs, lows, closes, period=14):
    n = len(closes)
    adx_arr = [0.0] * n
    if n < period * 3:
        return adx_arr
    pdm_r, mdm_r, tr_r = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_r.append(up   if up   > down and up   > 0 else 0.0)
        mdm_r.append(down if down > up   and down > 0 else 0.0)
        tr_r.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1])
        ))

    def wilder(arr, p):
        if len(arr) < p:
            return []
        s = [sum(arr[:p])]
        for x in arr[p:]:
            s.append(s[-1] - s[-1] / p + x)
        return s

    st = wilder(tr_r,  period)
    sp = wilder(pdm_r, period)
    sm = wilder(mdm_r, period)
    if not st:
        return adx_arr

    pdi = [100 * p / t if t else 0 for p, t in zip(sp, st)]
    mdi = [100 * m / t if t else 0 for m, t in zip(sm, st)]
    dx  = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]

    if len(dx) < period:
        return adx_arr

    adx_val = sum(dx[:period]) / period
    # offset: wilder series starts at index `period` in the original array
    # so adx[period*2] is first valid adx
    base = period * 2
    if base < n:
        adx_arr[base] = adx_val
    for i in range(period, len(dx)):
        adx_val = (adx_val * (period - 1) + dx[i]) / period
        idx = i + period + 1
        if idx < n:
            adx_arr[idx] = max(0.0, min(100.0, adx_val))
    return adx_arr

def build_vol_sma(volumes, period=10):
    n = len(volumes)
    vsma = [0.0] * n
    for i in range(period - 1, n):
        vsma[i] = sum(volumes[i - period + 1: i + 1]) / period
    return vsma

def prebake(sym, candles):
    """
    Pre-compute all indicator arrays for a symbol.
    Returns dict of arrays indexed by candle position.
    """
    closes  = [c['close'] for c in candles]
    highs   = [c['high']  for c in candles]
    lows    = [c['low']   for c in candles]
    opens   = [c['open']  for c in candles]
    volumes = [c['vol']   for c in candles]

    return {
        'closes':  closes,
        'highs':   highs,
        'lows':    lows,
        'opens':   opens,
        'volumes': volumes,
        'ema21':   build_ema(closes, 21),
        'rsi14':   build_rsi(closes, 14),
        'atr14':   build_atr(highs, lows, closes, 14),
        'adx14':   build_adx(highs, lows, closes, 14),
        'vsma':    build_vol_sma(volumes, VOL_PERIOD),
    }

# ─────────────────────────────────────────────
# SIGNAL LOGIC — O(1) per bar using pre-baked arrays
# Same conditions as original get_signal, just reads arrays instead of recomputing
# ─────────────────────────────────────────────
def get_signal_fast(ind, ci):
    """
    ind : pre-baked indicator dict for this symbol
    ci  : current candle index (strictly closed bar)
    Returns (signal, atr_val, adx_val, reason)
    """
    if ci < MIN_BARS:
        return None, 0.0, 0.0, 'warmup'

    atr_val = ind['atr14'][ci]
    adx_val = ind['adx14'][ci]
    ema21   = ind['ema21'][ci]
    rsi_now = ind['rsi14'][ci]
    vol_now = ind['volumes'][ci]
    vsma    = ind['vsma'][ci]
    close   = ind['closes'][ci]
    open_   = ind['opens'][ci]
    high    = ind['highs'][ci]
    low     = ind['lows'][ci]

    if adx_val >= ADX_MAX:
        return None, atr_val, adx_val, 'adx'

    if vsma == 0 or vol_now <= VOL_MULT * vsma:
        return None, atr_val, adx_val, 'vol'

    # Pivot windows: bars [ci-LOOKBACK-1 .. ci-2]  (exclude current bar)
    w_start = max(0, ci - LOOKBACK - 1)
    w_end   = ci - 1  # exclusive

    if w_end <= w_start:
        return None, atr_val, adx_val, 'no_signal'

    seg_lows  = ind['lows'][w_start:w_end]
    seg_highs = ind['highs'][w_start:w_end]
    seg_rsi   = ind['rsi14'][w_start:w_end]

    if not seg_lows:
        return None, atr_val, adx_val, 'no_signal'

    prev_low      = min(seg_lows)
    prev_high     = max(seg_highs)
    prev_rsi_low  = min(seg_rsi)
    prev_rsi_high = max(seg_rsi)

    # ── LONG ──
    bounced_up = (low <= ema21 and close > ema21 and close > open_)
    long_div   = (low < prev_low and rsi_now > prev_rsi_low)
    if long_div and bounced_up:
        return 'buy', atr_val, adx_val, None

    # ── SHORT ──
    bounced_dn  = (high >= ema21 and close < ema21 and close < open_)
    short_div   = (high > prev_high and rsi_now < prev_rsi_high)
    if short_div and bounced_dn:
        return 'sell', atr_val, adx_val, None

    return None, atr_val, adx_val, 'no_signal'

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def ts_to_quarter(ts):
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return f"{dt.year}-Q{(dt.month - 1) // 3 + 1}"

def verdict(pf, wr):
    ok = pf >= 1.10 and wr >= 40.0
    return ok, ("✅ USABLE (PF≥1.10 & WR≥40%)" if ok else "❌ NOT USABLE")

# ─────────────────────────────────────────────
# PORTFOLIO ENGINE
# ─────────────────────────────────────────────
def backtest_all(max_pos_limit, shared=None):
    """
    shared: dict with pre-loaded data (sym_data, indicators, sym_idx, timeline)
            Pass None for first run; returns it so second variant can reuse.
    """
    equity    = CAPITAL
    positions = {}
    trades    = []

    # ── Phase 1: Fetch (only on first call) ──
    if shared is None:
        shared = {}
        print(f"\n[Phase 1] Fetching {len(SYMBOLS)} symbols ({FETCH_WORKERS} workers)...")
        sym_data    = {}
        fetch_fails = 0
        completed   = 0
        lock        = threading.Lock()

        def fetch_one(sym):
            return sym, fetch_symbol(sym)

        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            futs = {ex.submit(fetch_one, sym): sym for sym in SYMBOLS}
            for fut in as_completed(futs):
                sym, candles = fut.result()
                with lock:
                    completed += 1
                    if len(candles) >= MIN_BARS:
                        sym_data[sym] = candles
                        print(f"  {completed:3d}/{len(SYMBOLS)}  {sym}  ({len(candles)} bars)", flush=True)
                    else:
                        fetch_fails += 1
                        print(f"  {completed:3d}/{len(SYMBOLS)}  {sym}  → skipped ({len(candles)} candles)", flush=True)

        if not sym_data:
            print("FATAL: No data fetched.")
            sys.exit(1)
        print(f"\n[Phase 1 done] {len(sym_data)} loaded, {fetch_fails} skipped")

        # ── Phase 2: Pre-bake all indicators ──
        print("\n[Phase 2] Pre-baking indicators for all symbols...")
        indicators = {}
        for sym, candles in sym_data.items():
            indicators[sym] = prebake(sym, candles)
        print(f"  Done — {len(indicators)} symbols ready")

        # ── Build timeline + index ──
        print("\n[Phase 3] Building unified timeline...")
        all_ts = set()
        for candles in sym_data.values():
            for c in candles:
                all_ts.add(c['ts'])
        timeline = sorted(all_ts)
        t0 = datetime.fromtimestamp(timeline[0]/1000,  tz=timezone.utc).strftime('%Y-%m-%d')
        t1 = datetime.fromtimestamp(timeline[-1]/1000, tz=timezone.utc).strftime('%Y-%m-%d')
        print(f"  {len(timeline):,} bars  ({t0} → {t1})")

        sym_idx = {}
        for sym, candles in sym_data.items():
            sym_idx[sym] = {c['ts']: i for i, c in enumerate(candles)}

        shared['sym_data']   = sym_data
        shared['indicators'] = indicators
        shared['sym_idx']    = sym_idx
        shared['timeline']   = timeline

    sym_data   = shared['sym_data']
    indicators = shared['indicators']
    sym_idx    = shared['sym_idx']
    timeline   = shared['timeline']

    # ── Phase 4: Backtest engine ──
    reject = {
        'warmup': 0, 'adx': 0, 'vol': 0,
        'no_signal': 0, 'max_pos': 0, 'in_pos': 0, 'signal': 0,
    }
    adx_bands = {
        'low_adx_signals': 0, 'low_adx_wins': 0,
        'mid_adx_signals': 0, 'mid_adx_wins': 0,
    }
    total_bars    = 0
    monthly_pnl   = {}
    quarterly_pnl = {}

    print(f"\n[Phase 4] Running engine (max_pos={max_pos_limit})...", flush=True)
    prog_step = len(timeline) // 20
    for ti, ts in enumerate(timeline):
        if prog_step and ti % prog_step == 0:
            pct = ti / len(timeline) * 100
            print(f"  {pct:5.1f}%  open_pos={len(positions)}", flush=True)

        dt        = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        month_key = dt.strftime('%Y-%m')
        qtr_key   = ts_to_quarter(ts)

        # ── Check exits ──
        for sym in list(positions.keys()):
            pos = positions[sym]
            idx = sym_idx[sym].get(ts)
            if idx is None:
                continue
            c   = sym_data[sym][idx]
            hit = None
            if pos['side'] == 'buy':
                if   c['low']  <= pos['sl']: hit = 'sl'
                elif c['high'] >= pos['tp']: hit = 'tp'
            else:
                if   c['high'] >= pos['sl']: hit = 'sl'
                elif c['low']  <= pos['tp']: hit = 'tp'

            if hit:
                ep      = pos['tp'] if hit == 'tp' else pos['sl']
                raw_pnl = (
                    (ep - pos['entry']) / pos['entry'] * pos['size_usd']
                    if pos['side'] == 'buy'
                    else (pos['entry'] - ep) / pos['entry'] * pos['size_usd']
                )
                cost    = pos['size_usd'] * (FEE + SLIP) * 2
                net_pnl = raw_pnl - cost
                equity += net_pnl
                dur     = (ts - pos['entry_ts']) // (30 * 60 * 1000)

                trades.append({
                    'sym':      sym,
                    'side':     pos['side'],
                    'entry':    pos['entry'],
                    'exit':     ep,
                    'hit':      hit,
                    'pnl':      net_pnl,
                    'size':     pos['size_usd'],
                    'ts_in':    pos['entry_ts'],
                    'ts_out':   ts,
                    'dur':      dur,
                    'month':    month_key,
                    'quarter':  qtr_key,
                    'adx_band': pos['adx_band'],
                })
                monthly_pnl[month_key]  = monthly_pnl.get(month_key,  0.0) + net_pnl
                quarterly_pnl[qtr_key]  = quarterly_pnl.get(qtr_key,  0.0) + net_pnl

                band = pos['adx_band']
                if band == 'low':
                    adx_bands['low_adx_signals'] += 1
                    if net_pnl > 0: adx_bands['low_adx_wins'] += 1
                else:
                    adx_bands['mid_adx_signals'] += 1
                    if net_pnl > 0: adx_bands['mid_adx_wins'] += 1

                del positions[sym]

        # ── Scan entries ──
        open_count = len(positions)
        for sym, ind in indicators.items():
            if sym in positions:
                reject['in_pos'] += 1
                total_bars += 1
                continue
            if open_count >= max_pos_limit:
                reject['max_pos'] += 1
                total_bars += 1
                continue
            idx = sym_idx[sym].get(ts)
            if idx is None:
                continue

            total_bars += 1
            sig, atr_val, adx_val, reason = get_signal_fast(ind, idx)

            if reason in reject:
                reject[reason] += 1
            elif sig is None:
                reject['no_signal'] += 1

            if sig is None:
                continue

            entry    = ind['closes'][idx]
            sl_dist  = atr_val * SL_MULT
            tp_dist  = atr_val * TP_MULT
            if sl_dist <= 0:
                continue

            risk_usd   = equity * RISK_PCT
            size_usd   = risk_usd / (sl_dist / entry)
            equity    -= size_usd * (FEE + SLIP)

            tp       = entry + tp_dist if sig == 'buy' else entry - tp_dist
            sl       = entry - sl_dist if sig == 'buy' else entry + sl_dist
            adx_band = 'low' if adx_val < 20 else 'mid'

            positions[sym] = {
                'side':     sig,
                'entry':    entry,
                'tp':       tp,
                'sl':       sl,
                'size_usd': size_usd,
                'entry_ts': ts,
                'adx_band': adx_band,
            }
            open_count += 1
            reject['signal'] += 1

    # ── Force-close ──
    for sym, pos in positions.items():
        candles    = sym_data[sym]
        ep         = candles[-1]['close']
        last_ts    = candles[-1]['ts']
        dt         = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        month_key  = dt.strftime('%Y-%m')
        qtr_key    = ts_to_quarter(last_ts)
        raw_pnl    = (
            (ep - pos['entry']) / pos['entry'] * pos['size_usd']
            if pos['side'] == 'buy'
            else (pos['entry'] - ep) / pos['entry'] * pos['size_usd']
        )
        cost    = pos['size_usd'] * (FEE + SLIP) * 2
        net_pnl = raw_pnl - cost
        equity += net_pnl
        dur     = (last_ts - pos['entry_ts']) // (30 * 60 * 1000)
        trades.append({
            'sym':      sym,
            'side':     pos['side'],
            'entry':    pos['entry'],
            'exit':     ep,
            'hit':      'force_close',
            'pnl':      net_pnl,
            'size':     pos['size_usd'],
            'ts_in':    pos['entry_ts'],
            'ts_out':   last_ts,
            'dur':      dur,
            'month':    month_key,
            'quarter':  qtr_key,
            'adx_band': pos['adx_band'],
        })
        monthly_pnl[month_key]  = monthly_pnl.get(month_key,  0.0) + net_pnl
        quarterly_pnl[qtr_key]  = quarterly_pnl.get(qtr_key,  0.0) + net_pnl

    return (trades, equity, monthly_pnl, quarterly_pnl,
            reject, adx_bands, total_bars, shared)

# ─────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────
def compute_stats(trades, equity_final):
    if not trades:
        return None
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    longs  = [t for t in trades if t['side'] == 'buy']
    shorts = [t for t in trades if t['side'] == 'sell']
    l_wins = [t for t in longs  if t['pnl'] > 0]
    s_wins = [t for t in shorts if t['pnl'] > 0]

    gw  = sum(t['pnl'] for t in wins)
    gl  = abs(sum(t['pnl'] for t in losses))
    pf  = gw / gl if gl else float('inf')
    wr  = len(wins) / len(trades) * 100
    aw  = gw / len(wins)   if wins   else 0
    al  = gl / len(losses) if losses else 0
    exp = (wr/100 * aw) - ((1 - wr/100) * al)

    rets    = [t['pnl'] for t in trades]
    mean_r  = sum(rets) / len(rets)
    std_r   = math.sqrt(sum((r - mean_r)**2 for r in rets) / len(rets)) if len(rets) > 1 else 0
    neg_r   = [r for r in rets if r < 0]
    down_s  = math.sqrt(sum(r**2 for r in neg_r) / len(neg_r)) if neg_r else 0

    eq = CAPITAL; peak = eq; mdd = 0.0
    for t in trades:
        eq  += t['pnl']
        peak = max(peak, eq)
        mdd  = max(mdd, (peak - eq) / peak * 100)

    bs = ws = cs = 0; cw = None
    for t in trades:
        w = t['pnl'] > 0
        if w == cw: cs += 1
        else: cw = w; cs = 1
        if w:  bs = max(bs, cs)
        else:  ws = max(ws, cs)

    l_gw = sum(t['pnl'] for t in l_wins)
    l_gl = abs(sum(t['pnl'] for t in longs if t['pnl'] <= 0))
    s_gw = sum(t['pnl'] for t in s_wins)
    s_gl = abs(sum(t['pnl'] for t in shorts if t['pnl'] <= 0))

    return {
        'total_trades':      len(trades),
        'wins':              len(wins),
        'losses':            len(losses),
        'win_rate':          round(wr, 2),
        'profit_factor':     round(pf, 4),
        'net_pnl':           round(equity_final - CAPITAL, 4),
        'net_pnl_pct':       round((equity_final - CAPITAL) / CAPITAL * 100, 2),
        'gross_win':         round(gw, 4),
        'gross_loss':        round(gl, 4),
        'avg_win':           round(aw, 4),
        'avg_loss':          round(al, 4),
        'expectancy':        round(exp, 4),
        'max_drawdown_pct':  round(mdd, 2),
        'sharpe':            round(mean_r / std_r  if std_r  else 0, 4),
        'sortino':           round(mean_r / down_s if down_s else 0, 4),
        'avg_duration_bars': round(sum(t['dur'] for t in trades) / len(trades), 1),
        'avg_duration_hours':round(sum(t['dur'] for t in trades) / len(trades) * 0.5, 1),
        'best_win_streak':   bs,
        'worst_loss_streak': ws,
        'longs':             len(longs),
        'shorts':            len(shorts),
        'long_wr':           round(len(l_wins)/len(longs)*100,  2) if longs  else 0,
        'short_wr':          round(len(s_wins)/len(shorts)*100, 2) if shorts else 0,
        'long_pf':           round(l_gw / l_gl if l_gl else float('inf'), 4),
        'short_pf':          round(s_gw / s_gl if s_gl else float('inf'), 4),
        'long_net':          round(sum(t['pnl'] for t in longs),  4),
        'short_net':         round(sum(t['pnl'] for t in shorts), 4),
        'final_equity':      round(equity_final, 4),
    }

def per_coin_stats(trades):
    cm = {}
    for t in trades:
        cm.setdefault(t['sym'], []).append(t)
    rows = []
    for sym, ts in cm.items():
        wins = [t for t in ts if t['pnl'] > 0]
        gw   = sum(t['pnl'] for t in wins)
        gl   = abs(sum(t['pnl'] for t in ts if t['pnl'] <= 0))
        pf   = gw / gl if gl else float('inf')
        rows.append({
            'sym':      sym,
            'trades':   len(ts),
            'wins':     len(wins),
            'losses':   len(ts) - len(wins),
            'wr':       round(len(wins)/len(ts)*100, 1),
            'pf':       round(pf, 3),
            'net':      round(sum(t['pnl'] for t in ts), 4),
            'low_data': len(ts) < MIN_TRADES,
        })
    rows.sort(key=lambda x: x['pf'], reverse=True)
    return rows

# ─────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────
def print_report(label, stats, coins, monthly_pnl, quarterly_pnl,
                 reject, adx_bands, total_bars):
    SEP = "═" * 65
    sep = "─" * 65
    print(f"\n{SEP}")
    print(f"  VARIANT: {label}")
    print(SEP)

    if stats is None:
        print("  ⚠️  ZERO TRADES")
        return

    ok, tag = verdict(stats['profit_factor'], stats['win_rate'])

    print(f"\nAGGREGATE")
    print(f"  Total Trades      : {stats['total_trades']}")
    print(f"  Win Rate          : {stats['win_rate']}%")
    print(f"  Profit Factor     : {stats['profit_factor']}")
    print(f"  Net PnL           : ${stats['net_pnl']:,.2f}  ({stats['net_pnl_pct']}%)")
    print(f"  Gross Win         : ${stats['gross_win']:,.2f}")
    print(f"  Gross Loss        : ${stats['gross_loss']:,.2f}")
    print(f"  Avg Win           : ${stats['avg_win']:.2f}")
    print(f"  Avg Loss          : ${stats['avg_loss']:.2f}")
    print(f"  Expectancy/trade  : ${stats['expectancy']:.2f}")
    print(f"  Max Drawdown      : {stats['max_drawdown_pct']}%")
    print(f"  Sharpe            : {stats['sharpe']}")
    print(f"  Sortino           : {stats['sortino']}")
    print(f"  Avg Duration      : {stats['avg_duration_bars']} bars ({stats['avg_duration_hours']}h)")
    print(f"  Best Win Streak   : {stats['best_win_streak']}")
    print(f"  Worst Loss Streak : {stats['worst_loss_streak']}")
    print(f"  Final Equity      : ${stats['final_equity']:,.2f}")
    print(f"\n  ► VERDICT : {tag}")
    print(f"    PF={stats['profit_factor']} (≥1.10)  WR={stats['win_rate']}% (≥40%)")

    # DIAGNOSTIC 1: Long vs Short
    print(f"\n{sep}")
    print("LONG vs SHORT")
    l_ok, l_tag = verdict(stats['long_pf'],  stats['long_wr'])
    s_ok, s_tag = verdict(stats['short_pf'], stats['short_wr'])
    print(f"  LONG   trades={stats['longs']:4d}  WR={stats['long_wr']:5.1f}%  PF={stats['long_pf']:.4f}  Net=${stats['long_net']:,.2f}")
    print(f"         {l_tag}")
    print(f"  SHORT  trades={stats['shorts']:4d}  WR={stats['short_wr']:5.1f}%  PF={stats['short_pf']:.4f}  Net=${stats['short_net']:,.2f}")
    print(f"         {s_tag}")
    if not l_ok and s_ok:
        print(f"  💡 Run SHORT-ONLY — longs dragging")
    elif l_ok and not s_ok:
        print(f"  💡 Run LONG-ONLY — shorts dragging")
    elif l_ok and s_ok:
        print(f"  💡 Both directions viable")
    else:
        print(f"  ⚠️  Neither direction meets target alone")

    # DIAGNOSTIC 4: ADX band
    print(f"\n{sep}")
    print("ADX BAND SPLIT")
    ls = adx_bands['low_adx_signals']
    ms = adx_bands['mid_adx_signals']
    lw = adx_bands['low_adx_wins'] / ls * 100 if ls else 0
    mw = adx_bands['mid_adx_wins'] / ms * 100 if ms else 0
    print(f"  ADX < 20  : {ls:4d} trades  WR={lw:.1f}%")
    print(f"  ADX 20-30 : {ms:4d} trades  WR={mw:.1f}%")
    if ls and ms:
        print(f"  💡 Edge stronger in {'ADX<20' if lw > mw else 'ADX 20-30'}")

    # DIAGNOSTIC 2: Per-coin table
    print(f"\n{sep}")
    print(f"PER-COIN TABLE  (⚠ = <{MIN_TRADES} trades)")
    print(f"  {'Symbol':<22} {'Trades':>6} {'Wins':>5} {'WR%':>6} {'PF':>7} {'Net$':>10}")
    print(sep)
    for r in coins:
        flag  = "✅" if r['pf'] >= 1.10 and r['wr'] >= 40 and not r['low_data'] else "  "
        thin  = " ⚠" if r['low_data'] else ""
        pf_s  = f"{r['pf']:.3f}" if r['pf'] != float('inf') else "  ∞  "
        print(f"  {flag}{r['sym']:<20} {r['trades']:>6} {r['wins']:>5} "
              f"{r['wr']:>5.1f}% {pf_s:>7} {r['net']:>10.2f}{thin}")

    # DIAGNOSTIC 3: Quarterly
    print(f"\n{sep}")
    print("QUARTERLY PnL")
    for q in sorted(quarterly_pnl):
        v    = quarterly_pnl[q]
        bar  = "█" * min(int(abs(v)/15), 35)
        sign = "+" if v >= 0 else "-"
        print(f"  {q}  {sign}${abs(v):8.2f}  {bar}")

    # Monthly
    print(f"\n{sep}")
    print("MONTHLY PnL")
    for m in sorted(monthly_pnl):
        v    = monthly_pnl[m]
        bar  = "█" * min(int(abs(v)/10), 35)
        sign = "+" if v >= 0 else "-"
        print(f"  {m}  {sign}${abs(v):8.2f}  {bar}")

    # Filter stats
    print(f"\n{sep}")
    print("FILTER STATS")
    print(f"  Total bars: {total_bars:,}")
    for k, v in reject.items():
        pct = v / total_bars * 100 if total_bars else 0
        print(f"    {k:<14}: {v:>9,}  ({pct:.1f}%)")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  RSI Divergence + EMA21 Bounce (S2-B)")
    print("  Infinity Bot Strategy Backtest")
    print(f"  Period : 2023-07 → 2025-07  |  TF: {INTERVAL}")
    print(f"  Coins  : {len(SYMBOLS)}  |  Capital: ${CAPITAL:,.0f}")
    print(f"  Target : PF≥1.10  |  WR≥40%")
    print(f"  Workers: {FETCH_WORKERS} fetch  |  Indicators: pre-baked")
    print("=" * 65)

    # Variant A: max 6 — also fetches & bakes data
    (trades_6, eq_6, mo_6, qt_6,
     rej_6, adx6, bars_6, shared) = backtest_all(MAX_POS)
    stats_6 = compute_stats(trades_6, eq_6)
    coins_6 = per_coin_stats(trades_6)

    # Variant B: max 3 — reuses everything
    print(f"\n[Phase 5] Re-running engine for max_pos={MAX_POS_BOT} (data reused)...")
    (trades_3, eq_3, mo_3, qt_3,
     rej_3, adx3, bars_3, _) = backtest_all(MAX_POS_BOT, shared=shared)
    stats_3 = compute_stats(trades_3, eq_3)
    coins_3 = per_coin_stats(trades_3)

    print_report("MAX 6 POSITIONS (pipeline default)",
                 stats_6, coins_6, mo_6, qt_6, rej_6, adx6, bars_6)
    print_report("MAX 3 POSITIONS (bot setting)",
                 stats_3, coins_3, mo_3, qt_3, rej_3, adx3, bars_3)

    # JSON
    report = {
        "meta": {
            "strategy": "RSI Divergence + EMA21 Bounce (S2-B)",
            "period":   "2023-07 to 2025-07",
            "interval": INTERVAL,
            "capital":  CAPITAL,
            "risk_pct": RISK_PCT,
            "fee":      FEE,
            "slip":     SLIP,
            "tp_mult":  TP_MULT,
            "sl_mult":  SL_MULT,
            "adx_max":  ADX_MAX,
            "target_pf": 1.10,
            "target_wr": 40.0,
        },
        "variant_max6": {
            "aggregate":     stats_6,
            "per_coin":      coins_6,
            "monthly_pnl":   {k: round(v,4) for k,v in mo_6.items()},
            "quarterly_pnl": {k: round(v,4) for k,v in qt_6.items()},
            "adx_bands":     adx6,
            "filter_stats":  rej_6,
            "trades":        trades_6,
        },
        "variant_max3": {
            "aggregate":     stats_3,
            "per_coin":      coins_3,
            "monthly_pnl":   {k: round(v,4) for k,v in mo_3.items()},
            "quarterly_pnl": {k: round(v,4) for k,v in qt_3.items()},
            "adx_bands":     adx3,
            "filter_stats":  rej_3,
            "trades":        trades_3,
        },
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Summary TXT
    lines = []
    for label, stats, coins, mo, qt, adx in [
        ("MAX 6 POSITIONS", stats_6, coins_6, mo_6, qt_6, adx6),
        ("MAX 3 POSITIONS", stats_3, coins_3, mo_3, qt_3, adx3),
    ]:
        lines.append(f"\n{'='*65}\n  {label}\n{'='*65}")
        if stats:
            ok, tag = verdict(stats['profit_factor'], stats['win_rate'])
            lines.append(f"  Trades   : {stats['total_trades']}")
            lines.append(f"  WR       : {stats['win_rate']}%")
            lines.append(f"  PF       : {stats['profit_factor']}")
            lines.append(f"  Net PnL  : ${stats['net_pnl']:,.2f} ({stats['net_pnl_pct']}%)")
            lines.append(f"  Max DD   : {stats['max_drawdown_pct']}%")
            lines.append(f"  LONG     : WR={stats['long_wr']}%  PF={stats['long_pf']}  Net=${stats['long_net']:,.2f}")
            lines.append(f"  SHORT    : WR={stats['short_wr']}%  PF={stats['short_pf']}  Net=${stats['short_net']:,.2f}")
            ls = adx['low_adx_signals']; ms = adx['mid_adx_signals']
            lw = adx['low_adx_wins']/ls*100 if ls else 0
            mw = adx['mid_adx_wins']/ms*100 if ms else 0
            lines.append(f"  ADX<20   : {ls} trades WR={lw:.1f}%")
            lines.append(f"  ADX20-30 : {ms} trades WR={mw:.1f}%")
            lines.append(f"  VERDICT  : {tag}")
        else:
            lines.append("  ZERO TRADES")
        lines.append("\nTop 15 Coins by PF:")
        for r in coins[:15]:
            pf_s = f"{r['pf']:.3f}" if r['pf'] != float('inf') else "inf"
            thin = " [thin]" if r['low_data'] else ""
            lines.append(f"  {r['sym']:<22} PF={pf_s}  WR={r['wr']}%  T={r['trades']}{thin}")
        lines.append("\nQuarterly:")
        for q in sorted(qt):
            sign = "+" if qt[q] >= 0 else ""
            lines.append(f"  {q}  {sign}${qt[q]:.2f}")

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n\n✅ Done — backtest_summary.txt | backtest_report.json")

if __name__ == "__main__":
    main()
