"""
InfinityX ADX Variant Backtest
================================
Tests V1/V2/V3/V4/V5/V7 strategies across:
  - ADX thresholds: 22, 25, 28, 30
  - TP/SL configs:
      original  → each strategy's original TP/SL multipliers
      v8_style  → TP×1.7, SL×1.7
      v9_style  → TP×1.2, SL×2.0

Total variants: 6 strategies × 4 ADX × 3 TP/SL = 72 variants
S3 and S4 are excluded.

Data: data.binance.vision futures monthly archive
Stdlib-only, no pip installs.

Usage (via GitHub Actions matrix — see backtest.yml):
  python backtest.py --job_id <0-9> --total_jobs 10
"""

import sys, os, io, json, csv, math, zipfile, urllib.request, urllib.error
import argparse
from datetime import datetime, timezone
from collections import defaultdict

# ── Constants ─────────────────────────────────────────────────────────────────
START_YEAR, START_MONTH = 2024, 1
END_YEAR,   END_MONTH   = 2025, 6   # last fully published month as of Aug 2026

CAPITAL_START   = 10_000.0
RISK_PCT        = 0.0075   # 0.75% per trade
FEE_RATE        = 0.0005   # 0.05% per side
SLIPPAGE_RATE   = 0.0002   # 0.02% per side
MAX_POSITIONS   = 6
WARMUP_BARS     = 400

ADX_VARIANTS    = [22, 25, 28, 30]
TPSL_VARIANTS   = {
    'original': None,   # per-strategy values used
    'v8_style': (1.7, 1.7),
    'v9_style': (1.2, 2.0),
}

# ── Strategy definitions (V1-V7, no S3/S4) ────────────────────────────────────
STRATEGIES = {
    'V1': {
        'name': 'V1 · 15m Original', 'logic': 'core',
        'tf': '15m', 'tp': 3.0, 'sl': 2.0,
        'coins': ['XRPUSDT','TIAUSDT','TURBOUSDT','SEIUSDT','1000RATSUSDT',
                  '1000BONKUSDT','EIGENUSDT','APTUSDT','REZUSDT','POPCATUSDT',
                  'DOGEUSDT','AVAXUSDT','BTCUSDT','LDOUSDT','BNBUSDT',
                  'BOMEUSDT','FETUSDT','RUNEUSDT','ATOMUSDT','STXUSDT','AXSUSDT',
                  'ALGOUSDT','TRXUSDT'],
    },
    'V2': {
        'name': 'V2 · 15m Tight Exits', 'logic': 'core',
        'tf': '15m', 'tp': 2.8, 'sl': 1.7,
        'coins': ['1000RATSUSDT','XRPUSDT','TIAUSDT','TURBOUSDT','SEIUSDT',
                  '1000BONKUSDT','BTCUSDT','EIGENUSDT','APTUSDT','REZUSDT',
                  'POPCATUSDT','AVAXUSDT','DOGEUSDT','LDOUSDT','BNBUSDT',
                  'RUNEUSDT','BOMEUSDT','FETUSDT','AXSUSDT','ATOMUSDT',
                  'STXUSDT','TRXUSDT','ALGOUSDT'],
    },
    'V3': {
        'name': 'V3 · 30m Candles', 'logic': 'core',
        'tf': '30m', 'tp': 3.0, 'sl': 2.0,
        'coins': ['BTCUSDT','XRPUSDT','TIAUSDT','BNBUSDT','DOGEUSDT','SEIUSDT',
                  'APTUSDT','AVAXUSDT','FETUSDT','TRXUSDT','ALGOUSDT','STXUSDT',
                  'DOTUSDT'],
    },
    'V4': {
        'name': 'V4 · 1H Candles', 'logic': 'core',
        'tf': '1h', 'tp': 3.0, 'sl': 2.0,
        'coins': ['BTCUSDT','TRUMPUSDT','AVAXUSDT','XRPUSDT','TIAUSDT','BNBUSDT',
                  'DOGEUSDT','SEIUSDT','APTUSDT','SOLUSDT','FETUSDT','ATOMUSDT',
                  'STXUSDT','DOTUSDT','ALGOUSDT','TRXUSDT','RUNEUSDT','LDOUSDT',
                  'AXSUSDT','REZUSDT'],
    },
    'V5': {
        'name': 'V5 · 15m + RSI Confirm', 'logic': 'core_rsi',
        'tf': '15m', 'tp': 3.0, 'sl': 2.0,
        'rsi_long': (45, 70), 'rsi_short': (30, 55),
        'coins': ['TIAUSDT','XRPUSDT','TURBOUSDT','SEIUSDT','DOGEUSDT',
                  '1000RATSUSDT','APTUSDT','BTCUSDT','REZUSDT','POPCATUSDT',
                  'AVAXUSDT','BNBUSDT','FETUSDT','LDOUSDT','RUNEUSDT',
                  'BOMEUSDT','AXSUSDT','ATOMUSDT','STXUSDT','ALGOUSDT','TRXUSDT'],
    },
    'V7': {
        'name': 'V7 · 15m Clean Whitelist', 'logic': 'core',
        'tf': '15m', 'tp': 2.8, 'sl': 1.7,
        'coins': ['DOGEUSDT','TIAUSDT','XRPUSDT','SEIUSDT','APTUSDT','REZUSDT',
                  'AXSUSDT','1000XECUSDT','STXUSDT','ALGOUSDT','TRXUSDT',
                  'FETUSDT','DOTUSDT'],
    },
}

TF_TO_MS = {'15m': 15*60*1000, '30m': 30*60*1000, '1h': 60*60*1000}

# ── Data fetching ──────────────────────────────────────────────────────────────
def fetch_monthly_klines(symbol, interval, year, month):
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines/"
           f"{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    rows = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        fname = z.namelist()[0]
        with z.open(fname) as f:
            text = io.TextIOWrapper(f, encoding='utf-8')
            reader = csv.reader(text)
            for row in reader:
                if not row or row[0].startswith('open_time'): continue
                try:
                    ot = int(row[0])
                    if ot > 10**14: ot //= 1000  # microsecond guard
                    rows.append({
                        'ts':   ot,
                        'open': float(row[1]),
                        'high': float(row[2]),
                        'low':  float(row[3]),
                        'close':float(row[4]),
                        'vol':  float(row[5]),
                    })
                except (ValueError, IndexError):
                    continue
    return rows

def fetch_all_klines(symbol, interval):
    all_rows = []
    year, month = START_YEAR, START_MONTH
    while (year, month) <= (END_YEAR, END_MONTH):
        rows = fetch_monthly_klines(symbol, interval, year, month)
        all_rows.extend(rows)
        month += 1
        if month > 12: month = 1; year += 1
    all_rows.sort(key=lambda r: r['ts'])
    return all_rows

# ── Indicators ─────────────────────────────────────────────────────────────────
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

def atr_calc(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    if not trs: return closes[-1] * 0.005
    if len(trs) < period: return sum(trs) / len(trs)
    a = sum(trs[:period]) / period
    for t in trs[period:]: a = (a * (period-1) + t) / period
    return a

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3: return 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i-1]; down = lows[i-1] - lows[i]
        pdm.append(up if up > down and up > 0 else 0.0)
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
    if not st: return 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]: adx = (adx*(period-1) + d) / period
    return max(0.0, min(100.0, adx))

def slope_pct(e50):
    if len(e50) < 11 or e50[-11] == 0: return 0.0
    return (e50[-1] - e50[-11]) / e50[-11] * 100

# ── Signal evaluators ──────────────────────────────────────────────────────────
def eval_core(bars, adx_min, tp_mult, sl_mult):
    """EMA9/21 crossover + EMA50 slope + ADX gate."""
    closes = [b['close'] for b in bars]
    highs  = [b['high']  for b in bars]
    lows   = [b['low']   for b in bars]

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    adx = adx_calc(highs, lows, closes)
    sl  = slope_pct(e50)

    crossed_up   = e9[-1] > e21[-1] and e9[-2] <= e21[-2]
    crossed_down = e9[-1] < e21[-1] and e9[-2] >= e21[-2]
    trend_up     = sl > 0.05
    trend_down   = sl < -0.05
    adx_ok       = adx >= adx_min

    sig = None
    if adx_ok and trend_up   and crossed_up:   sig = 'buy'
    if adx_ok and trend_down and crossed_down:  sig = 'sell'
    if not sig: return None, None, None

    atr   = atr_calc(highs, lows, closes)
    price = closes[-1]
    return sig, tp_mult * atr, sl_mult * atr

def eval_core_rsi(bars, adx_min, tp_mult, sl_mult, rsi_long=(45,70), rsi_short=(30,55)):
    sig, tp_dist, sl_dist = eval_core(bars, adx_min, tp_mult, sl_mult)
    if not sig: return None, None, None

    closes = [b['close'] for b in bars]
    rsi = rsi_calc(closes, 14)
    lo, hi = rsi_long if sig == 'buy' else rsi_short
    if not (lo <= rsi <= hi): return None, None, None
    return sig, tp_dist, sl_dist

# ── Portfolio simulator ────────────────────────────────────────────────────────
class Portfolio:
    def __init__(self):
        self.capital   = CAPITAL_START
        self.positions = {}   # symbol -> {side, entry, tp, sl, risk_usd, qty}
        self.trades    = []
        self.equity_curve = [CAPITAL_START]

    def can_open(self, symbol):
        return symbol not in self.positions and len(self.positions) < MAX_POSITIONS

    def open(self, symbol, side, entry, tp_dist, sl_dist, ts):
        risk_usd = self.capital * RISK_PCT
        sl_pct   = sl_dist / entry
        qty      = risk_usd / (sl_pct * entry) if sl_pct > 0 else 0
        # apply fees + slippage on entry
        cost = entry * qty * (FEE_RATE + SLIPPAGE_RATE)
        self.capital -= cost
        self.positions[symbol] = {
            'side': side, 'entry': entry,
            'tp': entry + tp_dist if side == 'buy' else entry - tp_dist,
            'sl': entry - sl_dist if side == 'buy' else entry + sl_dist,
            'qty': qty, 'risk_usd': risk_usd, 'open_ts': ts,
        }

    def check_close(self, symbol, bar, ts):
        pos = self.positions.get(symbol)
        if not pos: return
        h, l = bar['high'], bar['low']
        side  = pos['side']
        tp, sl = pos['tp'], pos['sl']
        closed = None

        # SL-first candle resolution (conservative — per handoff conventions)
        if side == 'buy':
            if l <= sl: closed = ('sl', sl)
            elif h >= tp: closed = ('tp', tp)
        else:
            if h >= sl: closed = ('sl', sl)
            elif l <= tp: closed = ('tp', tp)

        if not closed: return

        reason, exit_price = closed
        qty = pos['qty']
        if side == 'buy':
            gross_pnl = (exit_price - pos['entry']) * qty
        else:
            gross_pnl = (pos['entry'] - exit_price) * qty
        fee = exit_price * qty * (FEE_RATE + SLIPPAGE_RATE)
        net_pnl = gross_pnl - fee

        self.capital += net_pnl
        self.trades.append({
            'symbol': symbol, 'side': side,
            'entry': pos['entry'], 'exit': exit_price,
            'pnl': net_pnl, 'reason': reason,
            'open_ts': pos['open_ts'], 'close_ts': ts,
            'duration_bars': 0,   # filled after
        })
        self.equity_curve.append(self.capital)
        del self.positions[symbol]

    def force_close_all(self, bar_map, ts):
        for symbol in list(self.positions.keys()):
            bar = bar_map.get(symbol)
            if bar is None: continue
            pos = self.positions[symbol]
            exit_price = bar['close']
            qty = pos['qty']
            if pos['side'] == 'buy':
                gross_pnl = (exit_price - pos['entry']) * qty
            else:
                gross_pnl = (pos['entry'] - exit_price) * qty
            fee = exit_price * qty * (FEE_RATE + SLIPPAGE_RATE)
            net_pnl = gross_pnl - fee
            self.capital += net_pnl
            self.trades.append({
                'symbol': symbol, 'side': pos['side'],
                'entry': pos['entry'], 'exit': exit_price,
                'pnl': net_pnl, 'reason': 'end_of_data',
                'open_ts': pos['open_ts'], 'close_ts': ts,
            })
        self.positions.clear()

# ── Analytics ──────────────────────────────────────────────────────────────────
def compute_stats(trades, equity_curve):
    if not trades:
        return {'total_trades': 0, 'win_rate': 0, 'profit_factor': 0,
                'net_pnl': 0, 'max_drawdown': 0, 'expectancy': 0,
                'avg_win': 0, 'avg_loss': 0}

    wins   = [t['pnl'] for t in trades if t['pnl'] > 0]
    losses = [t['pnl'] for t in trades if t['pnl'] < 0]
    longs  = [t for t in trades if t['side'] == 'buy']
    shorts = [t for t in trades if t['side'] == 'sell']

    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0)

    wr  = len(wins) / len(trades) * 100
    avg_win  = sum(wins)  / len(wins)  if wins  else 0
    avg_loss = sum(losses)/ len(losses) if losses else 0
    exp = (wr/100 * avg_win) + ((1-wr/100) * avg_loss)

    # max drawdown
    peak = equity_curve[0]; max_dd = 0
    for v in equity_curve:
        peak = max(peak, v)
        max_dd = max(max_dd, peak - v)

    long_wr  = len([t for t in longs  if t['pnl']>0])/len(longs)*100  if longs  else 0
    short_wr = len([t for t in shorts if t['pnl']>0])/len(shorts)*100 if shorts else 0

    net_pnl = equity_curve[-1] - equity_curve[0] if equity_curve else 0

    return {
        'total_trades': len(trades),
        'win_rate':     round(wr, 2),
        'profit_factor':round(pf, 3),
        'net_pnl':      round(net_pnl, 2),
        'max_drawdown': round(max_dd, 2),
        'expectancy':   round(exp, 4),
        'avg_win':      round(avg_win, 4),
        'avg_loss':     round(avg_loss, 4),
        'longs':        len(longs),
        'shorts':       len(shorts),
        'long_wr':      round(long_wr, 2),
        'short_wr':     round(short_wr, 2),
        'final_capital':round(equity_curve[-1] if equity_curve else CAPITAL_START, 2),
    }

def per_coin_stats(trades):
    coin_data = defaultdict(list)
    for t in trades:
        coin_data[t['symbol']].append(t)
    rows = []
    for sym, ts in coin_data.items():
        wins = [t['pnl'] for t in ts if t['pnl'] > 0]
        losses = [t['pnl'] for t in ts if t['pnl'] < 0]
        gp = sum(wins); gl = abs(sum(losses))
        pf = gp/gl if gl > 0 else (float('inf') if gp > 0 else 0)
        rows.append({
            'symbol': sym,
            'trades': len(ts),
            'wins':   len(wins),
            'losses': len(losses),
            'win_rate': round(len(wins)/len(ts)*100, 1) if ts else 0,
            'profit_factor': round(pf, 3),
            'net_pnl': round(sum(t['pnl'] for t in ts), 4),
        })
    rows.sort(key=lambda r: r['profit_factor'], reverse=True)
    return rows

def monthly_pnl(trades):
    monthly = defaultdict(float)
    for t in trades:
        dt = datetime.fromtimestamp(t['close_ts']/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] += t['pnl']
    return {k: round(v, 4) for k, v in sorted(monthly.items())}

# ── Single variant runner ──────────────────────────────────────────────────────
def run_variant(strat_id, strat_cfg, adx_min, tpsl_name, tpsl_val, klines_cache):
    tp_base = strat_cfg['tp']
    sl_base = strat_cfg['sl']
    if tpsl_val is not None:
        tp_mult, sl_mult = tpsl_val
    else:
        tp_mult, sl_mult = tp_base, sl_base

    logic   = strat_cfg['logic']
    coins   = strat_cfg['coins']
    tf      = strat_cfg['tf']
    tf_ms   = TF_TO_MS[tf]

    # gather all unique timestamps across all coins
    all_ts = set()
    coin_bars = {}
    for coin in coins:
        bars = klines_cache.get((coin, tf), [])
        coin_bars[coin] = bars
        for b in bars:
            all_ts.add(b['ts'])

    sorted_ts = sorted(all_ts)
    # Build index: coin -> {ts: bar_index}
    coin_idx = {}
    for coin, bars in coin_bars.items():
        coin_idx[coin] = {b['ts']: i for i, b in enumerate(bars)}

    port = Portfolio()
    filter_stats = defaultdict(int)

    for ts in sorted_ts:
        # Build current bar map for all coins at this timestamp
        current_bar_map = {}
        for coin in coins:
            idx_map = coin_idx.get(coin, {})
            if ts in idx_map:
                current_bar_map[coin] = coin_bars[coin][idx_map[ts]]

        # Check & close existing positions first (SL-first)
        for coin in list(port.positions.keys()):
            bar = current_bar_map.get(coin)
            if bar:
                port.check_close(coin, bar, ts)

        # Scan for new signals
        for coin in coins:
            if not port.can_open(coin):
                filter_stats['position_locked'] += 1
                continue
            bar = current_bar_map.get(coin)
            if not bar:
                filter_stats['no_bar'] += 1
                continue

            idx_map = coin_idx.get(coin, {})
            bar_i = idx_map.get(ts)
            if bar_i is None or bar_i < WARMUP_BARS:
                filter_stats['warmup'] += 1
                continue

            bars_window = coin_bars[coin][max(0, bar_i - 499): bar_i + 1]
            if len(bars_window) < 60:
                filter_stats['insufficient_data'] += 1
                continue

            try:
                if logic == 'core':
                    sig, tp_dist, sl_dist = eval_core(bars_window, adx_min, tp_mult, sl_mult)
                elif logic == 'core_rsi':
                    sig, tp_dist, sl_dist = eval_core_rsi(
                        bars_window, adx_min, tp_mult, sl_mult,
                        strat_cfg.get('rsi_long', (45,70)),
                        strat_cfg.get('rsi_short', (30,55)),
                    )
                else:
                    filter_stats['unknown_logic'] += 1
                    continue
            except Exception:
                filter_stats['eval_error'] += 1
                continue

            if sig is None:
                filter_stats['no_signal'] += 1
                continue

            # Entry on next bar after signal bar
            next_bar_i = bar_i + 1
            if next_bar_i >= len(coin_bars[coin]):
                filter_stats['no_next_bar'] += 1
                continue
            next_bar = coin_bars[coin][next_bar_i]
            entry_price = next_bar['open']
            if entry_price <= 0:
                filter_stats['bad_price'] += 1
                continue

            port.open(coin, sig, entry_price, tp_dist, sl_dist, next_bar['ts'])
            filter_stats['signals_fired'] += 1

    # Force-close any still-open positions at end of data
    last_bar_map = {}
    for coin in coins:
        bars = coin_bars.get(coin, [])
        if bars: last_bar_map[coin] = bars[-1]
    port.force_close_all(last_bar_map, sorted_ts[-1] if sorted_ts else 0)

    stats = compute_stats(port.trades, port.equity_curve)
    stats['filter_stats'] = dict(filter_stats)
    stats['per_coin'] = per_coin_stats(port.trades)
    stats['monthly_pnl'] = monthly_pnl(port.trades)
    stats['meets_targets'] = stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42
    stats['variant_id'] = f"{strat_id}|ADX{adx_min}|{tpsl_name}"
    stats['strategy'] = strat_id
    stats['adx_min'] = adx_min
    stats['tpsl'] = tpsl_name
    stats['tp_mult'] = tp_mult
    stats['sl_mult'] = sl_mult
    return stats

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--job_id',     type=int, default=0)
    parser.add_argument('--total_jobs', type=int, default=10)
    args = parser.parse_args()

    job_id     = args.job_id
    total_jobs = args.total_jobs

    # Build all variant tuples: (strat_id, adx_min, tpsl_name, tpsl_val)
    all_variants = []
    for strat_id in STRATEGIES:
        for adx_min in ADX_VARIANTS:
            for tpsl_name, tpsl_val in TPSL_VARIANTS.items():
                all_variants.append((strat_id, adx_min, tpsl_name, tpsl_val))

    # Assign variants to this job
    my_variants = [v for i, v in enumerate(all_variants) if i % total_jobs == job_id]
    print(f"[Job {job_id}/{total_jobs}] Running {len(my_variants)} of {len(all_variants)} variants")

    # Determine which (coin, tf) pairs this job needs
    needed = set()
    for strat_id, adx_min, tpsl_name, tpsl_val in my_variants:
        cfg = STRATEGIES[strat_id]
        for coin in cfg['coins']:
            needed.add((coin, cfg['tf']))

    # Fetch all needed kline data once
    print(f"[Job {job_id}] Fetching data for {len(needed)} (coin, tf) pairs...")
    klines_cache = {}
    failed_symbols = []
    for i, (coin, tf) in enumerate(sorted(needed)):
        print(f"  [{i+1}/{len(needed)}] {coin} {tf}...", end='', flush=True)
        try:
            bars = fetch_all_klines(coin, tf)
            klines_cache[(coin, tf)] = bars
            print(f" {len(bars)} bars")
        except Exception as e:
            print(f" FAILED: {e}")
            failed_symbols.append(f"{coin}_{tf}")
            klines_cache[(coin, tf)] = []

    # Sanity check: if >80% of symbols failed, likely geo-block — abort clearly
    if len(failed_symbols) > len(needed) * 0.8:
        print(f"ERROR: {len(failed_symbols)}/{len(needed)} symbols failed to fetch.")
        print("This looks like a data source block, not a strategy issue. Aborting.")
        sys.exit(1)

    # Run variants
    all_results = []
    for vi, (strat_id, adx_min, tpsl_name, tpsl_val) in enumerate(my_variants):
        variant_id = f"{strat_id}|ADX{adx_min}|{tpsl_name}"
        print(f"[Job {job_id}] ({vi+1}/{len(my_variants)}) {variant_id}...", flush=True)
        try:
            result = run_variant(strat_id, STRATEGIES[strat_id], adx_min, tpsl_name, tpsl_val, klines_cache)
            all_results.append(result)
            status = "✅ PASS" if result['meets_targets'] else "❌"
            print(f"  PF={result['profit_factor']} WR={result['win_rate']}% "
                  f"Trades={result['total_trades']} {status}")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback; traceback.print_exc()

    # Write output files
    out_prefix = f"job{job_id:02d}"

    # Summary text
    lines = []
    lines.append(f"InfinityX ADX Variant Backtest — Job {job_id}/{total_jobs}")
    lines.append(f"Period: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    lines.append(f"Capital: ${CAPITAL_START:,.0f} | Risk: {RISK_PCT*100}% | "
                 f"Fee: {FEE_RATE*100}%/side | Slippage: {SLIPPAGE_RATE*100}%/side")
    lines.append("="*80)

    passing = [r for r in all_results if r['meets_targets']]
    lines.append(f"\nVARIANTS PASSING (PF≥1.5 AND WR≥42%): {len(passing)}/{len(all_results)}")
    lines.append("")

    # Sort results by PF descending
    all_results.sort(key=lambda r: r['profit_factor'], reverse=True)

    for r in all_results:
        tag = "✅ PASS" if r['meets_targets'] else "❌ FAIL"
        lines.append(f"{tag} | {r['variant_id']}")
        lines.append(f"  TP×{r['tp_mult']} SL×{r['sl_mult']} | "
                     f"Trades={r['total_trades']} | WR={r['win_rate']}% | "
                     f"PF={r['profit_factor']} | Net=${r['net_pnl']:+.2f} | "
                     f"MaxDD=${r['max_drawdown']:.2f} | "
                     f"Exp={r['expectancy']:.4f}")
        lines.append(f"  Longs={r['longs']}({r['long_wr']}%WR) "
                     f"Shorts={r['shorts']}({r['short_wr']}%WR)")

        if r['per_coin']:
            lines.append("  Per-coin (top by PF):")
            for pc in r['per_coin'][:10]:
                flag = "✅" if pc['profit_factor'] >= 1.5 else "  "
                lines.append(f"    {flag} {pc['symbol']:20s} "
                             f"T={pc['trades']:3d} WR={pc['win_rate']:5.1f}% "
                             f"PF={pc['profit_factor']:.3f} Net=${pc['net_pnl']:+.4f}")

        if r['monthly_pnl']:
            lines.append("  Monthly PnL: " +
                         " | ".join(f"{m}: ${v:+.2f}" for m, v in list(r['monthly_pnl'].items())[-12:]))

        lines.append(f"  Filters: {r['filter_stats']}")
        lines.append("")

    if failed_symbols:
        lines.append(f"FAILED FETCHES: {failed_symbols}")

    summary_path = f"{out_prefix}_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("\n".join(lines))
    print(f"\nSummary written → {summary_path}")

    # JSON report
    report = {
        'meta': {
            'job_id': job_id, 'total_jobs': total_jobs,
            'period': f"{START_YEAR}-{START_MONTH:02d} / {END_YEAR}-{END_MONTH:02d}",
            'capital': CAPITAL_START, 'risk_pct': RISK_PCT,
            'fee_rate': FEE_RATE, 'slippage': SLIPPAGE_RATE,
            'adx_variants': ADX_VARIANTS,
            'tpsl_variants': list(TPSL_VARIANTS.keys()),
            'failed_fetches': failed_symbols,
        },
        'variants': all_results,
    }
    report_path = f"{out_prefix}_report.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"JSON report written → {report_path}")

if __name__ == '__main__':
    main()
