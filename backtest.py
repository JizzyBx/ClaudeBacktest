"""
G Max — Compounding Equity Backtest (Global Chronological Simulation)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Starting capital: $101 | Risk: 1% of CURRENT balance per trade | 5x leverage
Position size compounds as balance grows/shrinks — NOT fixed like earlier tests.

Rules:
  - 10-day hard close: any open trade is force-closed at exactly 10 days
    (960 x 15m bars), regardless of profit or loss. No exceptions.
  - Per-coin lock: a coin cannot open a new trade while it already has one
    open. Different coins CAN have simultaneous open trades.
  - Single shared account balance across all 96 coins — every trade's size
    depends on the balance at the moment it opens, which depends on every
    prior trade's outcome (in any coin) that closed before it.

This requires a GLOBAL chronological simulation, not independent per-coin
backtests — trades interact through the shared balance. All 96 coins' 15m
candles are merged into one timeline and processed bar-by-bar in real time
order.

Same entry logic as production: EMA50 slope + EMA9/21 cross + ADX>=22, 15m,
TP 3% / SL 15%.

stdlib only.
"""
import json, time, sys, urllib.request, zipfile, io, csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────────
START_YM     = (2024, 8)
END_YM       = (2026, 7)
TIMEFRAME    = '15m'

START_CAPITAL = 101.0
RISK_PCT      = 0.01        # 1% of CURRENT balance per trade
FEE           = 0.0004
SLIP          = 0.0005
LEVERAGE      = 5

TP_PCT        = 0.030
SL_PCT        = 0.150
MIN_BARS      = 100

MAX_HOLD_BARS = 10 * 24 * 4  # 10 days x 24h x 4 bars/hour = 960 bars, hard close

NUM_SHARDS = 8               # used only for the DATA-FETCH stage (parallel download)
WORKERS    = 16

ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT','BIDUSDT',
    'BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT','COMBOUSDT',
    'CRCLUSDT','DAMUSDT','DEFIUSDT','DIAUSDT',
    'DMCUSDT','ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT',
    'FLNCUSDT','FLUXUSDT','FXSUSDT','GLMUSDT',
    'GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT',
    'MAGICUSDT','MEGAUSDT','MILKUSDT','MOODENGUSDT','NFPUSDT',
    'NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT',
    'PIXELUSDT','PLUMEUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','QUICKUSDT','RAVEUSDT',
    'REEFUSDT','RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT',
    'SKRUSDT','SOMIUSDT','SPELLUSDT',
    'SPKUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT',
    'USUALUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT',
    'XEMUSDT','XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

# ── Data fetch ───────────────────────────────────────────────
def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out

def fetch_month(symbol, year, month, timeframe):
    url = (f"https://data.binance.vision/data/futures/um/monthly/klines/"
           f"{symbol}/{timeframe}/{symbol}-{timeframe}-{year:04d}-{month:02d}.zip")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        candles = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding='utf-8')
                for row in csv.reader(text):
                    if not row or row[0] in ('open_time',):
                        continue
                    ts = int(float(row[0]))
                    if ts > 10**14:
                        ts = ts // 1000
                    o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
                    candles.append((ts, o, h, l, c))
        return candles
    except Exception:
        return []

def fetch_symbol(symbol, timeframe):
    months = month_range(START_YM, END_YM)
    all_candles = []
    for (y, m) in months:
        all_candles.extend(fetch_month(symbol, y, m, timeframe))
    if not all_candles:
        return []
    dedup = {}
    for c in all_candles:
        dedup[c[0]] = c
    return sorted(dedup.values(), key=lambda x: x[0])

# ── Indicators (identical to production GMaxV1.py) ────────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0
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
    if not st: return 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period: return 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]:
        adx = (adx*(period-1) + d) / period
    return max(0.0, min(100.0, adx))

def raw_signal(i, closes, highs, lows, e9, e21, e50):
    if i < 10 or i >= len(closes):
        return None
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up = slope_pct > 0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None
    crossed_up = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if trend_up and not crossed_up:
        return None
    if trend_down and not crossed_down:
        return None
    adx_val = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_val < 22:
        return None
    return 'buy' if crossed_up else 'sell'

# ── Precompute per-coin: candles + indicators + signal events ─
def precompute_symbol(symbol, candles):
    """
    Returns a dict with everything needed for the global simulation:
    candle arrays (by index) and a list of (bar_index, side) signal events.
    Also builds a ts -> index map implicitly via the arrays being time-sorted.
    """
    if len(candles) < MIN_BARS:
        return None
    closes = [c[4] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    opens  = [c[1] for c in candles]
    ts_arr = [c[0] for c in candles]

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    signals = {}  # bar_index -> 'buy'/'sell', only for bars where a signal fires
    n = len(candles)
    for i in range(MIN_BARS, n - 1):
        sig = raw_signal(i, closes, highs, lows, e9, e21, e50)
        if sig:
            signals[i] = sig

    return {
        'symbol': symbol, 'ts': ts_arr, 'open': opens, 'high': highs,
        'low': lows, 'close': closes, 'n': n, 'signals': signals,
    }

# ── Global chronological simulation ──────────────────────────
def run_global_simulation(coin_data):
    """
    coin_data: dict of symbol -> precompute_symbol() result (non-None only)

    Builds one global sorted timeline of every 15m bar timestamp appearing in
    ANY coin, then walks it in order. At each global timestamp:
      1. First, for every coin with an OPEN position whose candle exists at
         this timestamp, check TP/SL/10-day-hard-close using that bar's
         high/low. Close if triggered. (Exit processing happens before new
         entries at the same timestamp, so a same-bar exit frees the coin's
         lock before that bar's entries are considered — matches "check SL/TP
         first" convention from the single-coin backtest.)
      2. Then, for every coin WITHOUT an open position that has a signal
         event at (bar_index - 1) [i.e. this bar is the entry bar, signal
         fired on the previous closed bar], open a new position sized off
         the CURRENT shared balance at this exact moment.

    Returns (trades, balance_curve) where balance_curve is a list of
    (timestamp, balance) snapshots taken after every trade closes.
    """
    # Build global sorted list of unique timestamps across all coins
    ts_set = set()
    for d in coin_data.values():
        ts_set.update(d['ts'])
    global_ts = sorted(ts_set)

    # For each coin, build ts -> bar_index lookup for O(1) access during the walk
    ts_to_idx = {}
    for sym, d in coin_data.items():
        ts_to_idx[sym] = {t: i for i, t in enumerate(d['ts'])}

    balance = START_CAPITAL
    open_positions = {}  # symbol -> {side, entry_p, entry_i, entry_ts}
    trades = []
    balance_curve = [(global_ts[0] if global_ts else 0, balance)]

    for t in global_ts:
        # ── Phase 1: process exits for coins with an open position at this ts
        for sym in list(open_positions.keys()):
            d = coin_data[sym]
            idx_map = ts_to_idx[sym]
            if t not in idx_map:
                continue
            i = idx_map[t]
            pos = open_positions[sym]
            bars_held = i - pos['entry_i']
            hi, lo, cl = d['high'][i], d['low'][i], d['close'][i]
            exit_p = None; reason = None

            if pos['side'] == 'buy':
                sl_price = pos['entry_p'] * (1 - SL_PCT)
                tp_price = pos['entry_p'] * (1 + TP_PCT)
                if lo <= sl_price:
                    exit_p, reason = sl_price, 'sl'
                elif hi >= tp_price:
                    exit_p, reason = tp_price, 'tp'
            else:
                sl_price = pos['entry_p'] * (1 + SL_PCT)
                tp_price = pos['entry_p'] * (1 - TP_PCT)
                if hi >= sl_price:
                    exit_p, reason = sl_price, 'sl'
                elif lo <= tp_price:
                    exit_p, reason = tp_price, 'tp'

            # 10-day hard close overrides TP/SL if reached first in time,
            # but TP/SL is checked first within the SAME bar (SL wins ties).
            if exit_p is None and bars_held >= MAX_HOLD_BARS:
                exit_p, reason = cl, 'max_hold_10d'
            if exit_p is None and i == d['n'] - 2:
                exit_p, reason = cl, 'end_of_data'

            if exit_p is not None:
                gross = (exit_p - pos['entry_p'])/pos['entry_p'] if pos['side'] == 'buy' \
                        else (pos['entry_p'] - exit_p)/pos['entry_p']
                net = gross - (FEE + SLIP) * 2
                pnl = pos['notional'] * net
                balance += pnl
                trades.append({
                    'symbol': sym, 'side': pos['side'],
                    'entry_ts': pos['entry_ts'], 'exit_ts': t,
                    'entry_price': pos['entry_p'], 'exit_price': exit_p,
                    'notional': round(pos['notional'], 2),
                    'balance_before': round(balance - pnl, 2),
                    'balance_after': round(balance, 2),
                    'pnl': round(pnl, 4), 'reason': reason, 'bars': bars_held,
                })
                balance_curve.append((t, balance))
                del open_positions[sym]

        # ── Phase 2: process new entries at this ts (signal fired on bar i-1)
        for sym, d in coin_data.items():
            if sym in open_positions:
                continue  # per-coin lock: already open, skip
            idx_map = ts_to_idx[sym]
            if t not in idx_map:
                continue
            i = idx_map[t]
            if i == 0:
                continue
            sig = d['signals'].get(i - 1)
            if not sig:
                continue
            entry_p = d['open'][i] * (1 + FEE + SLIP) if sig == 'buy' \
                      else d['open'][i] * (1 - FEE - SLIP)
            notional = min(balance * RISK_PCT / SL_PCT, balance * LEVERAGE)
            if notional <= 0 or balance <= 0:
                continue  # blown account, stop opening new trades
            open_positions[sym] = {
                'side': sig, 'entry_p': entry_p, 'entry_i': i,
                'entry_ts': t, 'notional': notional,
            }

    return trades, balance_curve, balance

# ── Stats ────────────────────────────────────────────────────
def stats(trades, final_balance):
    if not trades:
        return {'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
                'max_drawdown_pct': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0,
                'expectancy': 0.0, 'longs': 0, 'shorts': 0, 'monthly': {},
                'per_coin': {}, 'final_balance': final_balance,
                'return_pct': 0.0, 'max_hold_closes': 0}
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gp = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))
    net_pnl = sum(t['pnl'] for t in trades)

    trades_sorted = sorted(trades, key=lambda t: t['exit_ts'])
    peak = START_CAPITAL; max_dd_pct = 0.0
    monthly = {}
    for t in trades_sorted:
        bal = t['balance_after']
        peak = max(peak, bal)
        dd_pct = (peak - bal) / peak * 100 if peak > 0 else 0.0
        max_dd_pct = max(max_dd_pct, dd_pct)
        ym = datetime.fromtimestamp(t['exit_ts'] / 1000, tz=timezone.utc).strftime('%Y-%m')
        m = monthly.setdefault(ym, {'pnl': 0.0, 'n': 0, 'w': 0})
        m['pnl'] += t['pnl']; m['n'] += 1
        if t['pnl'] > 0: m['w'] += 1

    per_coin = {}
    for t in trades:
        d = per_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0})
        d['pnl'] += t['pnl']; d['n'] += 1
        if t['pnl'] > 0: d['w'] += 1
    for sym, d in per_coin.items():
        d['wr'] = round(d['w'] / d['n'] * 100, 2) if d['n'] else 0.0
        d['pnl'] = round(d['pnl'], 2)

    return {
        'total': len(trades),
        'win_rate': round(len(wins) / len(trades) * 100, 2),
        'profit_factor': round(gp / gl, 3) if gl > 0 else (0.0 if gp == 0 else float('inf')),
        'net_pnl': round(net_pnl, 2),
        'max_drawdown_pct': round(max_dd_pct, 2),
        'avg_win': round(gp / len(wins), 3) if wins else 0.0,
        'avg_loss': round(gl / len(losses), 3) if losses else 0.0,
        'expectancy': round(net_pnl / len(trades), 4),
        'longs': sum(1 for t in trades if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades if t['side'] == 'sell'),
        'max_hold_closes': sum(1 for t in trades if t['reason'] == 'max_hold_10d'),
        'monthly': monthly,
        'per_coin': per_coin,
        'final_balance': round(final_balance, 2),
        'return_pct': round((final_balance - START_CAPITAL) / START_CAPITAL * 100, 2),
    }

# ── Shard runner: FETCH ONLY (parallel), simulation happens in merge ──
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    result = {'shard': shard_idx, 'symbols': symbols, 'coin_candles': {}}
    t0 = time.time()

    def work(sym):
        c = fetch_symbol(sym, TIMEFRAME)
        return sym, c

    with_data = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(work, s) for s in symbols]
        for fut in as_completed(futs):
            sym, candles = fut.result()
            if len(candles) >= MIN_BARS:
                with_data.append(sym)
                result['coin_candles'][sym] = candles

    result['with_data'] = with_data
    result['elapsed'] = round(time.time() - t0, 1)
    print(f"shard {shard_idx}: fetched {len(with_data)}/{len(symbols)} coins, {result['elapsed']}s")

    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(result, f)

# ── Merge: combine all coins' data, then run ONE global simulation ────
def merge_shards():
    all_candles = {}
    total_symbols_attempted = 0

    for i in range(NUM_SHARDS):
        try:
            with open(f'shard_{i}.json') as f:
                d = json.load(f)
        except FileNotFoundError:
            print(f"WARNING: shard_{i}.json missing")
            continue
        total_symbols_attempted += len(d['symbols'])
        all_candles.update(d['coin_candles'])

    if not all_candles:
        with open('backtest_summary.txt', 'w') as f:
            f.write("❌ ERROR: 0 symbols returned data. Likely geo-block on runner region.\n"
                     "Backtest aborted — no results to report.")
        with open('backtest_report.json', 'w') as f:
            json.dump({'error': 'no data'}, f)
        print("ERROR: no data across all shards")
        return

    print(f"Precomputing signals for {len(all_candles)} coins...")
    coin_data = {}
    for sym, candles in all_candles.items():
        pc = precompute_symbol(sym, candles)
        if pc:
            coin_data[sym] = pc

    print(f"Running global chronological simulation across {len(coin_data)} coins...")
    t0 = time.time()
    trades, balance_curve, final_balance = run_global_simulation(coin_data)
    elapsed = round(time.time() - t0, 1)
    print(f"Simulation done in {elapsed}s — {len(trades)} trades, final balance ${final_balance:.2f}")

    s = stats(trades, final_balance)

    report = {
        'with_data_count': len(coin_data),
        'symbols_attempted': total_symbols_attempted,
        'start_capital': START_CAPITAL,
        'risk_pct': RISK_PCT,
        'leverage': LEVERAGE,
        'max_hold_days': 10,
        'stats': s,
    }
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    lines = []
    lines.append("=" * 70)
    lines.append("G MAX — COMPOUNDING EQUITY BACKTEST (per-coin lock, 10-day hard close)")
    lines.append(f"Period: {START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}")
    lines.append(f"Start capital: ${START_CAPITAL} | Risk: {RISK_PCT*100:.0f}% of current balance/trade")
    lines.append(f"Leverage: {LEVERAGE}x | TP: {TP_PCT*100:.1f}% | SL: {SL_PCT*100:.1f}% | Max hold: 10 days (hard)")
    lines.append(f"Coins with data: {len(coin_data)}/{total_symbols_attempted}")
    lines.append("=" * 70)

    lines.append(f"\nFinal balance:     ${s['final_balance']}")
    lines.append(f"Return:            {s['return_pct']}%")
    lines.append(f"Total trades:      {s['total']}")
    lines.append(f"Win rate:          {s['win_rate']}%")
    lines.append(f"Profit factor:     {s['profit_factor']}")
    lines.append(f"Net PnL:           ${s['net_pnl']}")
    lines.append(f"Max drawdown:      {s['max_drawdown_pct']}% (peak-to-trough on balance)")
    lines.append(f"Avg win / loss:    ${s['avg_win']} / ${s['avg_loss']}")
    lines.append(f"Expectancy:        ${s['expectancy']} per trade")
    lines.append(f"Longs / Shorts:    {s['longs']} / {s['shorts']}")
    lines.append(f"10-day hard closes: {s['max_hold_closes']} trades force-closed at the 10-day limit")
    usable = s['profit_factor'] >= 1.5 and s['win_rate'] >= 42
    lines.append(f"RECOMMENDATION:    {'✅ USABLE' if usable else '❌ NOT USABLE'} "
                 f"(needs PF>=1.5 and WR>=42%)")

    lines.append(f"\nTop 50 coins by net PnL:")
    ranked = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)[:50]
    for sym, d in ranked:
        lines.append(f"  {sym:<20} trades={d['n']:<4} wr={d['wr']:>6}%  pnl=${d['pnl']}")

    lines.append(f"\nMonthly PnL:")
    for ym in sorted(s['monthly'].keys()):
        m = s['monthly'][ym]
        lines.append(f"  {ym}: pnl=${round(m['pnl'],2):<10} n={m['n']:<4} w={m['w']}")

    lines.append("\n" + "=" * 70)
    lines.append("NOTE: position size compounds — each trade risks 1% of the CURRENT")
    lines.append("balance at the moment it opens, not the original $101. Early trades")
    lines.append("are tiny in dollar terms; later trades are sized off whatever the")
    lines.append("account has grown (or shrunk) to by that point.")

    with open('backtest_summary.txt', 'w') as f:
        f.write('\n'.join(lines))
    print('\n'.join(lines))

# ── Entry point ──────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx 0-7 | merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))
