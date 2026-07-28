"""
Backtest: RSI Divergence + EMA21 Bounce (S2-B)
Strategy extracted from Infinity Trading Bot v1 by Paqu
Timeframe : 30m candles
Period    : July 2023 – July 2025 (25 months)
Universe  : 98 coins (bot whitelist, symbol-fixed)
Capital   : $10,000 shared equity
Risk/trade: 0.75% of current equity
Fees      : 0.05% per side | Slippage: 0.02% per side
Max pos   : 6 concurrent (pipeline default; bot uses 3 — both reported)
TP        : 3 × ATR14
SL        : 2 × ATR14

EXTRA DIAGNOSTICS (no signal change):
  1. Long-only / Short-only separate verdicts
  2. Per-coin minimum trade flag (<20 = insufficient data)
  3. Quarterly PnL breakdown (bull vs bear phase detection)
  4. ADX band split: ADX<20 vs ADX 20-30
"""

import urllib.request, zipfile, io, csv, json, math, time, sys
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

TP_MULT     = 3.0
SL_MULT     = 2.0
ADX_MAX     = 30
VOL_MULT    = 1.2
VOL_PERIOD  = 10
LOOKBACK    = 40
MIN_BARS    = 120
MIN_TRADES  = 20   # below this → "insufficient data" flag on per-coin table

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"  # futures perpetuals, NOT spot

# ─────────────────────────────────────────────
# 98-COIN LIST (symbol-fixed)
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
            name = z.namelist()[0]
            with z.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        candles = []
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts > 10**14:
                ts //= 1000
            candles.append({
                'ts':    ts,
                'open':  float(row[1]),
                'high':  float(row[2]),
                'low':   float(row[3]),
                'close': float(row[4]),
                'vol':   float(row[5]),
            })
        return candles
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception:
        return None

def fetch_symbol(sym):
    all_candles = []
    for year in range(START_YEAR, END_YEAR + 1):
        m_start = START_MONTH if year == START_YEAR else 1
        m_end   = END_MONTH   if year == END_YEAR   else 12
        for month in range(m_start, m_end + 1):
            c = fetch_month(sym, year, month)
            if c:
                all_candles.extend(c)
    seen, out = set(), []
    for c in all_candles:
        if c['ts'] not in seen:
            seen.add(c['ts'])
            out.append(c)
    out.sort(key=lambda x: x['ts'])
    return out

# ─────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def rsi_calc(closes, period=14):
    if len(closes) < period + 2:
        return [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi_vals = []
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rs = ag / al if al != 0 else 999
        rsi_vals.append(100 - (100 / (1 + rs)))
    return [50.0] * (len(closes) - len(rsi_vals)) + rsi_vals

def atr_calc(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))
    if not trs:
        return closes[-1] * 0.005
    if len(trs) < period:
        return sum(trs) / len(trs)
    a = sum(trs[:period]) / period
    for t in trs[period:]:
        a = (a * (period - 1) + t) / period
    return a

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up   > down and up   > 0 else 0.0)
        mdm.append(down if down > up   and down > 0 else 0.0)
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        ))
    def ws(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r
    st = ws(trs, period)
    sp = ws(pdm, period)
    sm = ws(mdm, period)
    if not st:
        return 0.0
    pdi = [100 * p / t if t else 0 for p, t in zip(sp, st)]
    mdi = [100 * m / t if t else 0 for m, t in zip(sm, st)]
    dx  = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period:
        return 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]:
        adx = (adx * (period - 1) + d) / period
    return max(0.0, min(100.0, adx))

def vol_sma(volumes, period=10):
    if len(volumes) < period:
        return volumes[-1]
    return sum(volumes[-period:]) / period

# ─────────────────────────────────────────────
# SIGNAL LOGIC (mirrors get_signal exactly)
# Returns: (signal, atr, adx_val, reject_reason)
# adx_val passed back so ADX band split can use it
# ─────────────────────────────────────────────
def get_signal(candles, ci):
    closes  = [c['close'] for c in candles[:ci+1]]
    highs   = [c['high']  for c in candles[:ci+1]]
    lows    = [c['low']   for c in candles[:ci+1]]
    opens   = [c['open']  for c in candles[:ci+1]]
    volumes = [c['vol']   for c in candles[:ci+1]]

    if len(closes) < MIN_BARS:
        return None, 0.0, 0.0, 'warmup'

    atr_val = atr_calc(highs, lows, closes, 14)
    adx_val = adx_calc(highs, lows, closes, 14)
    e21     = ema(closes, 21)
    rsi     = rsi_calc(closes, 14)
    vsma    = vol_sma(volumes, VOL_PERIOD)

    cur_close = closes[-1]
    cur_open  = opens[-1]
    cur_low   = lows[-1]
    cur_high  = highs[-1]
    cur_ema21 = e21[-1]
    cur_rsi   = rsi[-1]
    cur_vol   = volumes[-1]

    if adx_val >= ADX_MAX:
        return None, atr_val, adx_val, 'adx'

    if cur_vol <= VOL_MULT * vsma:
        return None, atr_val, adx_val, 'vol'

    # LONG
    seg_lows     = lows[-(LOOKBACK+2):-2]
    seg_rsi_lows = rsi[-(LOOKBACK+2):-2]
    if seg_lows and seg_rsi_lows:
        prev_low     = min(seg_lows)
        prev_rsi_low = min(seg_rsi_lows)
        bounced_up   = (cur_low <= cur_ema21
                        and cur_close > cur_ema21
                        and cur_close > cur_open)
        long_div     = (cur_low < prev_low and cur_rsi > prev_rsi_low)
        if long_div and bounced_up:
            return 'buy', atr_val, adx_val, None

    # SHORT
    seg_highs     = highs[-(LOOKBACK+2):-2]
    seg_rsi_highs = rsi[-(LOOKBACK+2):-2]
    if seg_highs and seg_rsi_highs:
        prev_high     = max(seg_highs)
        prev_rsi_high = max(seg_rsi_highs)
        bounced_down  = (cur_high >= cur_ema21
                         and cur_close < cur_ema21
                         and cur_close < cur_open)
        short_div     = (cur_high > prev_high and cur_rsi < prev_rsi_high)
        if short_div and bounced_down:
            return 'sell', atr_val, adx_val, None

    return None, atr_val, adx_val, 'no_signal'

# ─────────────────────────────────────────────
# PORTFOLIO BACKTEST ENGINE
# ─────────────────────────────────────────────
def ts_to_quarter(ts):
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    q  = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{q}"

def backtest_all(max_pos_limit, sym_data=None, sym_idx=None, timeline=None):
    equity    = CAPITAL
    positions = {}
    trades    = []

    if sym_data is None:
        print(f"\n[Phase 1] Fetching data for {len(SYMBOLS)} symbols in parallel (20 workers)...")
        sym_data    = {}
        fetch_fails = 0
        completed   = 0
        lock_print  = __import__('threading').Lock()

        def fetch_one(sym):
            candles = fetch_symbol(sym)
            return sym, candles

        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(fetch_one, sym): sym for sym in SYMBOLS}
            for fut in as_completed(futures):
                sym, candles = fut.result()
                completed += 1
                with lock_print:
                    if len(candles) >= MIN_BARS:
                        sym_data[sym] = candles
                        print(f"  {completed:3d}/{len(SYMBOLS)}  {sym}  ({len(candles)} bars)", flush=True)
                    else:
                        fetch_fails += 1
                        print(f"  {completed:3d}/{len(SYMBOLS)}  {sym}  → skipped ({len(candles)} candles)", flush=True)

        if not sym_data:
            print("FATAL: No data fetched. Check network / geo-block.")
            sys.exit(1)
        if fetch_fails == len(SYMBOLS):
            print("FATAL: 100% fetch failure — data source likely blocked.")
            sys.exit(1)
        print(f"\n[Phase 1 done] {len(sym_data)} symbols loaded, {fetch_fails} skipped")

        print("\n[Phase 2] Building unified timeline...")
        all_ts = set()
        for candles in sym_data.values():
            for c in candles:
                all_ts.add(c['ts'])
        timeline = sorted(all_ts)
        t0 = datetime.fromtimestamp(timeline[0]/1000,  tz=timezone.utc).strftime('%Y-%m-%d')
        t1 = datetime.fromtimestamp(timeline[-1]/1000, tz=timezone.utc).strftime('%Y-%m-%d')
        print(f"  Timeline: {len(timeline):,} bars  ({t0} → {t1})")

        sym_idx = {}
        for sym, candles in sym_data.items():
            sym_idx[sym] = {c['ts']: i for i, c in enumerate(candles)}

    reject = {
        'warmup': 0, 'adx': 0, 'vol': 0,
        'no_signal': 0, 'max_pos': 0, 'in_pos': 0, 'signal': 0,
    }
    # ADX band counters (signals only)
    adx_bands = {'low_adx_signals': 0, 'low_adx_wins': 0,
                 'mid_adx_signals': 0, 'mid_adx_wins': 0}
    total_bars = 0
    monthly_pnl  = {}
    quarterly_pnl = {}

    for ts in timeline:
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
                if c['low']  <= pos['sl']:  hit = 'sl'
                elif c['high'] >= pos['tp']: hit = 'tp'
            else:
                if c['high'] >= pos['sl']:  hit = 'sl'
                elif c['low']  <= pos['tp']: hit = 'tp'

            if hit:
                exit_price = pos['tp'] if hit == 'tp' else pos['sl']
                raw_pnl    = (
                    (exit_price - pos['entry']) / pos['entry'] * pos['size_usd']
                    if pos['side'] == 'buy'
                    else (pos['entry'] - exit_price) / pos['entry'] * pos['size_usd']
                )
                cost    = pos['size_usd'] * (FEE + SLIP) * 2
                net_pnl = raw_pnl - cost
                equity += net_pnl
                dur     = (ts - pos['entry_ts']) // (30 * 60 * 1000)
                trades.append({
                    'sym':      sym,
                    'side':     pos['side'],
                    'entry':    pos['entry'],
                    'exit':     exit_price,
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
                monthly_pnl[month_key]   = monthly_pnl.get(month_key, 0.0)   + net_pnl
                quarterly_pnl[qtr_key]   = quarterly_pnl.get(qtr_key, 0.0)   + net_pnl

                # ADX band win tracking
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
        for sym, candles in sym_data.items():
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
            sig, atr_val, adx_val, reason = get_signal(candles, idx)

            if reason in reject:
                reject[reason] += 1
            elif sig is None:
                reject['no_signal'] += 1

            if sig is None:
                continue

            risk_usd = equity * RISK_PCT
            entry    = candles[idx]['close']
            sl_dist  = atr_val * SL_MULT
            tp_dist  = atr_val * TP_MULT
            if sl_dist <= 0:
                continue

            size_usd   = risk_usd / (sl_dist / entry)
            entry_cost = size_usd * (FEE + SLIP)
            equity    -= entry_cost

            tp       = entry + tp_dist if sig == 'buy' else entry - tp_dist
            sl       = entry - sl_dist if sig == 'buy' else entry + sl_dist
            adx_band = 'low' if adx_val < 20 else 'mid'   # <20 vs 20-30

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

    # Force-close remaining open positions
    for sym, pos in positions.items():
        candles    = sym_data[sym]
        exit_price = candles[-1]['close']
        last_ts    = candles[-1]['ts']
        dt         = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        month_key  = dt.strftime('%Y-%m')
        qtr_key    = ts_to_quarter(last_ts)
        raw_pnl    = (
            (exit_price - pos['entry']) / pos['entry'] * pos['size_usd']
            if pos['side'] == 'buy'
            else (pos['entry'] - exit_price) / pos['entry'] * pos['size_usd']
        )
        cost    = pos['size_usd'] * (FEE + SLIP) * 2
        net_pnl = raw_pnl - cost
        equity += net_pnl
        dur     = (last_ts - pos['entry_ts']) // (30 * 60 * 1000)
        trades.append({
            'sym':      sym,
            'side':     pos['side'],
            'entry':    pos['entry'],
            'exit':     exit_price,
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
        monthly_pnl[month_key]  = monthly_pnl.get(month_key, 0.0)  + net_pnl
        quarterly_pnl[qtr_key]  = quarterly_pnl.get(qtr_key, 0.0)  + net_pnl

    return (trades, equity, monthly_pnl, quarterly_pnl,
            reject, adx_bands, total_bars, sym_data, sym_idx, timeline)

# ─────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────
def verdict(pf, wr, label=""):
    # Target: PF>=1.10, WR>=40% (bot's own target)
    ok = pf >= 1.10 and wr >= 40.0
    tag = "✅ USABLE (PF≥1.10 & WR≥40%)" if ok else "❌ NOT USABLE"
    return ok, tag

def compute_stats(trades, equity_final):
    if not trades:
        return None
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    longs  = [t for t in trades if t['side'] == 'buy']
    shorts = [t for t in trades if t['side'] == 'sell']
    l_wins = [t for t in longs  if t['pnl'] > 0]
    s_wins = [t for t in shorts if t['pnl'] > 0]

    gross_win  = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    pf  = gross_win / gross_loss if gross_loss else float('inf')
    wr  = len(wins) / len(trades) * 100

    avg_win  = gross_win  / len(wins)   if wins   else 0
    avg_loss = gross_loss / len(losses) if losses else 0
    expectancy = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)

    returns  = [t['pnl'] for t in trades]
    mean_r   = sum(returns) / len(returns)
    std_r    = math.sqrt(sum((r - mean_r)**2 for r in returns) / len(returns)) if len(returns) > 1 else 0
    neg_r    = [r for r in returns if r < 0]
    down_std = math.sqrt(sum(r**2 for r in neg_r) / len(neg_r)) if neg_r else 0
    sharpe   = mean_r / std_r   if std_r   else 0
    sortino  = mean_r / down_std if down_std else 0

    eq = CAPITAL; peak = eq; max_dd = 0.0
    for t in trades:
        eq  += t['pnl']
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak * 100)

    best_streak = worst_streak = cur_streak = 0
    cur_win = None
    for t in trades:
        w = t['pnl'] > 0
        if w == cur_win:
            cur_streak += 1
        else:
            cur_win = w; cur_streak = 1
        if w:  best_streak  = max(best_streak,  cur_streak)
        else:  worst_streak = max(worst_streak, cur_streak)

    # Long-only stats
    l_gw  = sum(t['pnl'] for t in l_wins)
    l_gl  = abs(sum(t['pnl'] for t in longs if t['pnl'] <= 0))
    l_pf  = l_gw / l_gl if l_gl else float('inf')
    l_wr  = len(l_wins) / len(longs) * 100 if longs else 0

    # Short-only stats
    s_gw  = sum(t['pnl'] for t in s_wins)
    s_gl  = abs(sum(t['pnl'] for t in shorts if t['pnl'] <= 0))
    s_pf  = s_gw / s_gl if s_gl else float('inf')
    s_wr  = len(s_wins) / len(shorts) * 100 if shorts else 0

    net_pnl = equity_final - CAPITAL
    avg_dur = sum(t['dur'] for t in trades) / len(trades)

    return {
        'total_trades':      len(trades),
        'wins':              len(wins),
        'losses':            len(losses),
        'win_rate':          round(wr, 2),
        'profit_factor':     round(pf, 4),
        'net_pnl':           round(net_pnl, 4),
        'net_pnl_pct':       round(net_pnl / CAPITAL * 100, 2),
        'gross_win':         round(gross_win, 4),
        'gross_loss':        round(gross_loss, 4),
        'avg_win':           round(avg_win, 4),
        'avg_loss':          round(avg_loss, 4),
        'expectancy':        round(expectancy, 4),
        'max_drawdown_pct':  round(max_dd, 2),
        'sharpe':            round(sharpe, 4),
        'sortino':           round(sortino, 4),
        'avg_duration_bars': round(avg_dur, 1),
        'avg_duration_hours':round(avg_dur * 0.5, 1),
        'best_win_streak':   best_streak,
        'worst_loss_streak': worst_streak,
        'longs':             len(longs),
        'shorts':            len(shorts),
        'long_wr':           round(l_wr, 2),
        'short_wr':          round(s_wr, 2),
        'long_pf':           round(l_pf, 4),
        'short_pf':          round(s_pf, 4),
        'long_net':          round(sum(t['pnl'] for t in longs), 4),
        'short_net':         round(sum(t['pnl'] for t in shorts), 4),
        'final_equity':      round(equity_final, 4),
    }

def per_coin_stats(trades):
    coin_map = {}
    for t in trades:
        coin_map.setdefault(t['sym'], []).append(t)
    rows = []
    for sym, ts in coin_map.items():
        wins = [t for t in ts if t['pnl'] > 0]
        gl   = abs(sum(t['pnl'] for t in ts if t['pnl'] <= 0))
        gw   = sum(t['pnl'] for t in wins)
        pf   = gw / gl if gl else float('inf')
        rows.append({
            'sym':    sym,
            'trades': len(ts),
            'wins':   len(wins),
            'losses': len(ts) - len(wins),
            'wr':     round(len(wins) / len(ts) * 100, 1),
            'pf':     round(pf, 3),
            'net':    round(sum(t['pnl'] for t in ts), 4),
            'low_data': len(ts) < MIN_TRADES,
        })
    rows.sort(key=lambda x: x['pf'], reverse=True)
    return rows

# ─────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────
def print_report(label, stats, coin_rows, monthly_pnl, quarterly_pnl,
                 reject, adx_bands, total_bars):
    SEP = "═" * 65
    sep = "─" * 65
    print(f"\n{SEP}")
    print(f"  VARIANT: {label}")
    print(SEP)

    if stats is None:
        print("  ⚠️  ZERO TRADES — strategy produced no signals.")
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

    print(f"\n  ► OVERALL VERDICT : {tag}")
    print(f"    PF={stats['profit_factor']} (target≥1.10)  WR={stats['win_rate']}% (target≥40%)")

    # ── DIAGNOSTIC 1: Long vs Short separate verdicts ──
    print(f"\n{sep}")
    print("LONG vs SHORT BREAKDOWN")
    print(sep)
    l_ok, l_tag = verdict(stats['long_pf'],  stats['long_wr'],  "LONG")
    s_ok, s_tag = verdict(stats['short_pf'], stats['short_wr'], "SHORT")
    print(f"  LONG   trades={stats['longs']:4d}  WR={stats['long_wr']:5.1f}%  PF={stats['long_pf']:.4f}  Net=${stats['long_net']:,.2f}")
    print(f"         {l_tag}")
    print(f"  SHORT  trades={stats['shorts']:4d}  WR={stats['short_wr']:5.1f}%  PF={stats['short_pf']:.4f}  Net=${stats['short_net']:,.2f}")
    print(f"         {s_tag}")
    if not l_ok and s_ok:
        print(f"\n  💡 RECOMMENDATION: Run SHORT-ONLY on live bot — longs drag performance")
    elif l_ok and not s_ok:
        print(f"\n  💡 RECOMMENDATION: Run LONG-ONLY on live bot — shorts drag performance")
    elif l_ok and s_ok:
        print(f"\n  💡 Both directions viable")
    else:
        print(f"\n  ⚠️  Neither direction meets targets individually")

    # ── DIAGNOSTIC 4: ADX band split ──
    print(f"\n{sep}")
    print("ADX BAND SPLIT  (signals that hit TP or SL only)")
    print(sep)
    low_sig = adx_bands['low_adx_signals']
    mid_sig = adx_bands['mid_adx_signals']
    low_wr  = adx_bands['low_adx_wins'] / low_sig * 100 if low_sig else 0
    mid_wr  = adx_bands['mid_adx_wins'] / mid_sig * 100 if mid_sig else 0
    print(f"  ADX < 20  : {low_sig:4d} trades  WR={low_wr:.1f}%")
    print(f"  ADX 20-30 : {mid_sig:4d} trades  WR={mid_wr:.1f}%")
    if low_sig and mid_sig:
        better = "ADX<20" if low_wr > mid_wr else "ADX 20-30"
        print(f"  💡 Edge stronger in {better} band")

    # ── PER-COIN TABLE ──
    print(f"\n{sep}")
    print(f"PER-COIN TABLE  (sorted by PF  |  ⚠ = <{MIN_TRADES} trades, data thin)")
    print(f"{'Symbol':<22} {'Trades':>6} {'Wins':>5} {'WR%':>6} {'PF':>7} {'Net$':>10}  Flag")
    print(sep)
    for r in coin_rows:
        ok_flag   = "✅" if r['pf'] >= 1.10 and r['wr'] >= 40 and not r['low_data'] else "  "
        thin_flag = " ⚠️ thin" if r['low_data'] else ""
        pf_str    = f"{r['pf']:.3f}" if r['pf'] != float('inf') else "  ∞  "
        print(f"  {ok_flag}{r['sym']:<20} {r['trades']:>6} {r['wins']:>5} "
              f"{r['wr']:>5.1f}% {pf_str:>7} {r['net']:>10.2f}{thin_flag}")

    # ── DIAGNOSTIC 3: Quarterly PnL ──
    print(f"\n{sep}")
    print("QUARTERLY PnL  (bull/bear phase detection)")
    print(sep)
    for qtr in sorted(quarterly_pnl.keys()):
        v       = quarterly_pnl[qtr]
        bar_len = int(abs(v) / 15)
        bar     = "█" * min(bar_len, 35)
        sign    = "+" if v >= 0 else "-"
        print(f"  {qtr}  {sign}${abs(v):8.2f}  {bar}")

    # ── MONTHLY PnL ──
    print(f"\n{sep}")
    print("MONTHLY PnL")
    print(sep)
    for month in sorted(monthly_pnl.keys()):
        v       = monthly_pnl[month]
        bar_len = int(abs(v) / 10)
        bar     = "█" * min(bar_len, 35)
        sign    = "+" if v >= 0 else "-"
        print(f"  {month}  {sign}${abs(v):8.2f}  {bar}")

    # ── FILTER REJECTION ──
    print(f"\n{sep}")
    print("FILTER REJECTION STATS")
    print(f"  Total bars scanned : {total_bars:,}")
    accounted = sum(reject.values())
    print(f"  Accounted for      : {accounted:,}")
    for k, v in reject.items():
        pct = v / total_bars * 100 if total_bars else 0
        print(f"    {k:<14}: {v:>9,}  ({pct:.1f}%)")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def build_summary_lines(label, stats, coins, monthly, quarterly, reject, bars, adx_bands):
    lines = []
    SEP = "=" * 65
    lines.append(f"\n{SEP}")
    lines.append(f"  VARIANT: {label}")
    lines.append(SEP)
    if stats:
        ok, tag = verdict(stats['profit_factor'], stats['win_rate'])
        lines.append(f"  Trades        : {stats['total_trades']}")
        lines.append(f"  Win Rate      : {stats['win_rate']}%")
        lines.append(f"  Profit Factor : {stats['profit_factor']}")
        lines.append(f"  Net PnL       : ${stats['net_pnl']:,.2f} ({stats['net_pnl_pct']}%)")
        lines.append(f"  Max Drawdown  : {stats['max_drawdown_pct']}%")
        lines.append(f"  Sharpe        : {stats['sharpe']}")
        lines.append(f"  LONG   WR={stats['long_wr']}%  PF={stats['long_pf']}  Net=${stats['long_net']:,.2f}")
        lines.append(f"  SHORT  WR={stats['short_wr']}%  PF={stats['short_pf']}  Net=${stats['short_net']:,.2f}")
        lines.append(f"  VERDICT       : {tag}")
        low_sig = adx_bands['low_adx_signals']
        mid_sig = adx_bands['mid_adx_signals']
        low_wr  = adx_bands['low_adx_wins'] / low_sig * 100 if low_sig else 0
        mid_wr  = adx_bands['mid_adx_wins'] / mid_sig * 100 if mid_sig else 0
        lines.append(f"  ADX<20  {low_sig} trades WR={low_wr:.1f}%  |  ADX20-30  {mid_sig} trades WR={mid_wr:.1f}%")
    else:
        lines.append("  ZERO TRADES")
    lines.append("\nTop 15 Coins by PF:")
    for r in coins[:15]:
        pf_str = f"{r['pf']:.3f}" if r['pf'] != float('inf') else "inf"
        thin   = " [thin]" if r['low_data'] else ""
        lines.append(f"  {r['sym']:<22} PF={pf_str}  WR={r['wr']}%  Trades={r['trades']}{thin}")
    lines.append("\nQuarterly PnL:")
    for q in sorted(quarterly.keys()):
        sign = "+" if quarterly[q] >= 0 else ""
        lines.append(f"  {q}  {sign}${quarterly[q]:.2f}")
    return lines

def main():
    print("=" * 65)
    print("  RSI Divergence + EMA21 Bounce (S2-B)")
    print("  Infinity Bot Strategy Backtest")
    print(f"  Period : 2023-07 → 2025-07  |  TF: {INTERVAL}")
    print(f"  Coins  : {len(SYMBOLS)}  |  Capital: ${CAPITAL:,.0f}")
    print(f"  Target : PF≥1.10  |  WR≥40%")
    print("=" * 65)

    # ── VARIANT A: Max 6 (fetch data here, reuse for B) ──
    (trades_6, equity_6, monthly_6, quarterly_6,
     reject_6, adx6, bars_6,
     sym_data, sym_idx, timeline) = backtest_all(MAX_POS)

    stats_6 = compute_stats(trades_6, equity_6)
    coins_6 = per_coin_stats(trades_6)

    # ── VARIANT B: Max 3 (reuse already-fetched data) ──
    print("\n[Phase 4] Re-running for Max-3 variant (reusing data)...")
    (trades_3, equity_3, monthly_3, quarterly_3,
     reject_3, adx3, bars_3, _, _, _) = backtest_all(
        MAX_POS_BOT, sym_data=sym_data, sym_idx=sym_idx, timeline=timeline)

    stats_3 = compute_stats(trades_3, equity_3)
    coins_3 = per_coin_stats(trades_3)

    print_report("MAX 6 POSITIONS (pipeline default)",
                 stats_6, coins_6, monthly_6, quarterly_6, reject_6, adx6, bars_6)
    print_report("MAX 3 POSITIONS (bot's actual setting)",
                 stats_3, coins_3, monthly_3, quarterly_3, reject_3, adx3, bars_3)

    # ── JSON ──
    report = {
        "meta": {
            "strategy":  "RSI Divergence + EMA21 Bounce (S2-B)",
            "source":    "Infinity Trading Bot v1 by Paqu",
            "period":    "2023-07 to 2025-07",
            "interval":  INTERVAL,
            "capital":   CAPITAL,
            "risk_pct":  RISK_PCT,
            "fee":       FEE,
            "slip":      SLIP,
            "tp_mult":   TP_MULT,
            "sl_mult":   SL_MULT,
            "adx_max":   ADX_MAX,
            "vol_mult":  VOL_MULT,
            "lookback":  LOOKBACK,
            "min_trades_flag": MIN_TRADES,
            "coins":     len(SYMBOLS),
            "target_pf": 1.10,
            "target_wr": 40.0,
        },
        "variant_max6": {
            "aggregate":     stats_6,
            "per_coin":      coins_6,
            "monthly_pnl":   {k: round(v, 4) for k, v in monthly_6.items()},
            "quarterly_pnl": {k: round(v, 4) for k, v in quarterly_6.items()},
            "adx_bands":     adx6,
            "filter_stats":  reject_6,
            "trades":        trades_6,
        },
        "variant_max3": {
            "aggregate":     stats_3,
            "per_coin":      coins_3,
            "monthly_pnl":   {k: round(v, 4) for k, v in monthly_3.items()},
            "quarterly_pnl": {k: round(v, 4) for k, v in quarterly_3.items()},
            "adx_bands":     adx3,
            "filter_stats":  reject_3,
            "trades":        trades_3,
        },
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # ── Summary TXT ──
    lines = []
    lines += build_summary_lines("MAX 6 POSITIONS", stats_6, coins_6,
                                  monthly_6, quarterly_6, reject_6, bars_6, adx6)
    lines += build_summary_lines("MAX 3 POSITIONS (bot setting)", stats_3, coins_3,
                                  monthly_3, quarterly_3, reject_3, bars_3, adx3)
    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    print("\n\n✅ Done. Artifacts: backtest_summary.txt | backtest_report.json")

if __name__ == "__main__":
    main()
