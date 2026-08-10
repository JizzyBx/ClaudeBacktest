"""
G Max V1 — Multi-Variant Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Variants:
  A  : ADX-Scaled TP (5x) — ADX 22-35→1%, 35-50→2%, 50+→3%
  L6 : Leverage Sweep 6x  (fixed 3% TP, 15% SL)
  L7 : Leverage Sweep 7x
  L8 : Leverage Sweep 8x
  L9 : Leverage Sweep 9x
  L10: Leverage Sweep 10x
  B  : ADX≥35 filter (5x, fixed 3% TP, 15% SL)

Coins   : 117 (Universe)
Timeframe: 15m
Period  : Aug 2024 – Aug 2026
Capital : $10,000
Shards  : 20 | Workers: 16 per shard
stdlib only — no pip installs
"""

import sys, json, os, csv, io, math, time, zipfile
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Coin Universe (117) ────────────────────────────────────
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

# ── Global Config ──────────────────────────────────────────
NUM_SHARDS  = 20
WORKERS     = 16
TIMEFRAME   = '15m'
START_YM    = (2024, 8)
END_YM      = (2026, 7)   # inclusive — fetches up to July 2026
CAPITAL     = 10000.0
RISK_PCT    = 0.01        # 1% risk per trade
FEE         = 0.0005      # 0.05% taker
SLIP        = 0.0003      # 0.03% slippage
SL_PCT      = 0.150       # 15% SL (all variants)
MAX_BARS    = 960         # 10 days at 15m
MIN_BARS    = 100         # warmup

# ── Variants Definition ────────────────────────────────────
VARIANTS = {
    'A':   {'name': 'ADX-Scaled TP 5x',   'leverage': 5,  'tp_mode': 'adx_scaled', 'tp_pct': None, 'adx_min': 22},
    'L6':  {'name': 'Leverage Sweep 6x',   'leverage': 6,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L7':  {'name': 'Leverage Sweep 7x',   'leverage': 7,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L8':  {'name': 'Leverage Sweep 8x',   'leverage': 8,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L9':  {'name': 'Leverage Sweep 9x',   'leverage': 9,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L10': {'name': 'Leverage Sweep 10x',  'leverage': 10, 'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'B':   {'name': 'ADX≥35 Filter 5x',   'leverage': 5,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 35},
}
VARIANT_KEYS = list(VARIANTS.keys())  # order preserved

# ── Data Fetch ─────────────────────────────────────────────
BASE_URL = 'https://data.binance.vision/data/futures/um/monthly/klines'

def fetch_month(symbol, year, month):
    url = f'{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip'
    try:
        with urlopen(url, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        out = []
        for row in rows:
            if not row or row[0].startswith('#'): continue
            try:
                ts = int(row[0])
                if ts > 10**14: ts //= 1000
                out.append((ts, float(row[1]), float(row[2]), float(row[3]), float(row[4])))
            except (ValueError, IndexError):
                continue
        return out
    except HTTPError:
        return []
    except (URLError, Exception):
        return []

def fetch_symbol(symbol):
    all_candles = []
    y, m = START_YM
    ey, em = END_YM
    while (y, m) <= (ey, em):
        all_candles.extend(fetch_month(symbol, y, m))
        m += 1
        if m > 12: m = 1; y += 1
    # dedup + sort
    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    return sorted(seen.values(), key=lambda x: x[0])

# ── Indicators (pure Python) ───────────────────────────────
def ema(values, period):
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 3:
        return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down   and up > 0   else 0.0)
        mdm.append(down if down > up   and down > 0 else 0.0)
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
    adx = sum(dx[:period]) / period
    for d in dx[period:]: adx = (adx*(period-1) + d) / period
    return max(0.0, min(100.0, adx)), pdi[-1], mdi[-1]

def atr_calc(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if not trs: return closes[-1] * 0.005
    if len(trs) < period: return sum(trs) / len(trs)
    a = sum(trs[:period]) / period
    for t in trs[period:]: a = (a*(period-1) + t) / period
    return a

# ── Signal — Variant G ────────────────────────────────────
def check_signal(closes, highs, lows, adx_min):
    """Returns (signal, adx_val) where signal is 'buy','sell', or None."""
    if len(closes) < MIN_BARS:
        return None, 0.0

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    i   = len(closes) - 2  # last closed bar

    if i < 10:
        return None, 0.0

    # Filter 1: EMA50 slope
    slope_pct  = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None, 0.0

    # Filter 2: EMA9/21 cross on last closed bar
    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

    if trend_up and not crossed_up:   return None, 0.0
    if trend_down and not crossed_down: return None, 0.0

    # Filter 3: ADX
    adx_val, _, _ = adx_calc(highs, lows, closes, 14)
    if adx_val < adx_min:
        return None, adx_val

    sig = 'buy' if crossed_up else 'sell'
    return sig, adx_val

# ── TP resolver ────────────────────────────────────────────
def resolve_tp(tp_mode, tp_pct, adx_val):
    if tp_mode == 'fixed':
        return tp_pct
    # adx_scaled: Variant A
    if adx_val >= 50:   return 0.03
    if adx_val >= 35:   return 0.02
    return 0.01  # 22–35

# ── Backtest single symbol for one variant ─────────────────
def backtest_symbol(symbol, candles, variant_cfg):
    leverage  = variant_cfg['leverage']
    tp_mode   = variant_cfg['tp_mode']
    tp_pct_cfg = variant_cfg['tp_pct']
    adx_min   = variant_cfg['adx_min']

    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    ts     = [c[0] for c in candles]

    trades = []
    in_trade = False
    entry_p = side = entry_ts = entry_bar = None
    tp = sl = trade_tp_pct = 0.0

    for i in range(MIN_BARS, len(closes) - 1):
        if not in_trade:
            sig, adx_val = check_signal(closes[:i+1], highs[:i+1], lows[:i+1], adx_min)
            if sig is None:
                continue

            # Entry on next bar open
            ep_raw = opens[i+1]
            if sig == 'buy':
                entry_p = ep_raw * (1 + FEE + SLIP)
                sl_price = entry_p * (1 - SL_PCT)
                trade_tp_pct = resolve_tp(tp_mode, tp_pct_cfg, adx_val)
                tp_price = entry_p * (1 + trade_tp_pct)
            else:
                entry_p = ep_raw * (1 - FEE - SLIP)
                sl_price = entry_p * (1 + SL_PCT)
                trade_tp_pct = resolve_tp(tp_mode, tp_pct_cfg, adx_val)
                tp_price = entry_p * (1 - trade_tp_pct)

            notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * leverage)
            side = sig; entry_ts = ts[i+1]; entry_bar = i+1
            tp = tp_price; sl = sl_price
            in_trade = True
            stored_notional = notional
            stored_adx = adx_val
            continue

        # Check SL then TP
        bars_held = i - entry_bar
        reason = None; exit_p = None

        if side == 'buy':
            if lows[i] <= sl:
                reason = 'sl'; exit_p = sl
            elif highs[i] >= tp:
                reason = 'tp'; exit_p = tp
        else:
            if highs[i] >= sl:
                reason = 'sl'; exit_p = sl
            elif lows[i] <= tp:
                reason = 'tp'; exit_p = tp

        if reason is None and bars_held >= MAX_BARS:
            reason = 'max_hold'; exit_p = closes[i]

        if reason is None and i == len(closes) - 2:
            reason = 'end_of_data'; exit_p = closes[i]

        if reason:
            if side == 'buy':
                gross = (exit_p - entry_p) / entry_p
            else:
                gross = (entry_p - exit_p) / entry_p
            net = gross - (FEE + SLIP) * 2
            pnl = stored_notional * net

            trades.append({
                'symbol':      symbol,
                'variant':     '',  # filled by caller
                'side':        side,
                'entry_ts':    entry_ts,
                'exit_ts':     ts[i],
                'entry_price': entry_p,
                'exit_price':  exit_p,
                'pnl':         round(pnl, 4),
                'reason':      reason,
                'bars':        bars_held,
                'adx':         round(stored_adx, 1),
                'tp_pct':      round(trade_tp_pct * 100, 1),
                'notional':    round(stored_notional, 2),
            })
            in_trade = False

    return trades

# ── Stats (with Sharpe ratio) ──────────────────────────────
def calc_stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
            'net_pnl': 0.0, 'max_drawdown': 0.0, 'avg_win': 0.0,
            'avg_loss': 0.0, 'expectancy': 0.0, 'sharpe': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gp = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))

    pf = (gp / gl) if gl > 0 else (float('inf') if gp > 0 else 0.0)
    wr = len(wins) / len(trades) * 100
    avg_win  = gp / len(wins)   if wins   else 0.0
    avg_loss = gl / len(losses) if losses else 0.0
    expectancy = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)

    # Max drawdown (running equity)
    equity = 0.0; peak = 0.0; max_dd = 0.0
    sorted_trades = sorted(trades, key=lambda t: t['exit_ts'])
    for t in sorted_trades:
        equity += t['pnl']
        if equity > peak: peak = equity
        dd = peak - equity
        if dd > max_dd: max_dd = dd

    # Sharpe ratio (annualised, daily buckets)
    daily = {}
    for t in sorted_trades:
        day = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m-%d')
        daily[day] = daily.get(day, 0.0) + t['pnl']
    daily_returns = list(daily.values())
    if len(daily_returns) >= 2:
        n = len(daily_returns)
        mu = sum(daily_returns) / n
        var = sum((r - mu)**2 for r in daily_returns) / (n - 1)
        std = math.sqrt(var) if var > 0 else 0.0
        sharpe = (mu / std * math.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Monthly breakdown
    monthly = {}
    for t in sorted_trades:
        key = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m')
        if key not in monthly: monthly[key] = {'pnl': 0.0, 'n': 0, 'w': 0}
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        if t['pnl'] > 0: monthly[key]['w'] += 1

    # Per-coin breakdown
    per_coin = {}
    for t in sorted_trades:
        sym = t['symbol']
        if sym not in per_coin:
            per_coin[sym] = {'pnl': 0.0, 'n': 0, 'w': 0}
        per_coin[sym]['pnl'] += t['pnl']
        per_coin[sym]['n']   += 1
        if t['pnl'] > 0: per_coin[sym]['w'] += 1
    for sym in per_coin:
        d = per_coin[sym]
        d['wr'] = round(d['w'] / d['n'] * 100, 1) if d['n'] > 0 else 0.0
        d['pnl'] = round(d['pnl'], 2)

    return {
        'total':         len(trades),
        'win_rate':      round(wr, 2),
        'profit_factor': round(pf, 4),
        'net_pnl':       round(sum(t['pnl'] for t in trades), 2),
        'max_drawdown':  round(max_dd, 2),
        'avg_win':       round(avg_win, 2),
        'avg_loss':      round(avg_loss, 2),
        'expectancy':    round(expectancy, 4),
        'sharpe':        round(sharpe, 4),
        'longs':         sum(1 for t in trades if t['side'] == 'buy'),
        'shorts':        sum(1 for t in trades if t['side'] == 'sell'),
        'monthly':       monthly,
        'per_coin':      per_coin,
    }

# ── Shard Runner ───────────────────────────────────────────
def run_shard(shard_idx):
    t0 = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f'[Shard {shard_idx}] {len(symbols)} coins: {symbols}', flush=True)

    # Fetch all symbol data in parallel
    candle_map = {}
    def fetch_one(sym):
        data = fetch_symbol(sym)
        return sym, data

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_one, sym): sym for sym in symbols}
        for fut in as_completed(futs):
            sym, data = fut.result()
            candle_map[sym] = data
            status = f'{len(data)} bars' if data else 'NO DATA'
            print(f'[Shard {shard_idx}] {sym}: {status}', flush=True)

    # Run all variants for each symbol
    all_trades = []   # list of trade dicts with 'variant' key
    with_data  = []

    for sym in symbols:
        candles = candle_map.get(sym, [])
        if len(candles) < MIN_BARS + 10:
            print(f'[Shard {shard_idx}] SKIP {sym}: only {len(candles)} bars', flush=True)
            continue
        with_data.append(sym)
        for vk, vcfg in VARIANTS.items():
            trades = backtest_symbol(sym, candles, vcfg)
            for t in trades:
                t['variant'] = vk
            all_trades.extend(trades)
            print(f'[Shard {shard_idx}] {sym} [{vk}]: {len(trades)} trades', flush=True)

    # Per-variant stats for this shard
    variant_stats = {}
    for vk in VARIANT_KEYS:
        vtrades = [t for t in all_trades if t['variant'] == vk]
        variant_stats[vk] = calc_stats(vtrades)

    out = {
        'shard':     shard_idx,
        'symbols':   symbols,
        'with_data': with_data,
        'trades':    all_trades,
        'variant_stats': variant_stats,
        'elapsed':   round(time.time() - t0, 1),
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(out, f)
    print(f'[Shard {shard_idx}] Done in {out["elapsed"]}s — {len(all_trades)} trades total', flush=True)

# ── Merge & Report ─────────────────────────────────────────
def merge_shards():
    all_trades = []
    all_with_data = []
    all_symbols_attempted = []

    for i in range(NUM_SHARDS):
        path = f'shard_{i}.json'
        if not os.path.exists(path):
            print(f'WARNING: {path} missing', flush=True)
            continue
        with open(path) as f:
            shard = json.load(f)
        all_trades.extend(shard['trades'])
        all_with_data.extend(shard['with_data'])
        all_symbols_attempted.extend(shard['symbols'])

    # Compute per-variant final stats
    results = {}
    for vk, vcfg in VARIANTS.items():
        vtrades = [t for t in all_trades if t['variant'] == vk]
        s = calc_stats(vtrades)
        results[vk] = {
            'variant_key':  vk,
            'variant_name': vcfg['name'],
            'config':       vcfg,
            'stats':        s,
            'trades':       vtrades,
        }

    # Cross-variant coin comparison
    coin_variant_matrix = {}  # coin -> variant -> {pnl, n, wr}
    for vk in VARIANT_KEYS:
        vtrades = [t for t in all_trades if t['variant'] == vk]
        per_coin = calc_stats(vtrades)['per_coin']
        for sym, d in per_coin.items():
            if sym not in coin_variant_matrix:
                coin_variant_matrix[sym] = {}
            coin_variant_matrix[sym][vk] = d

    report = {
        'period':          f'{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}',
        'timeframe':       TIMEFRAME,
        'capital':         CAPITAL,
        'symbols_attempted': len(set(all_symbols_attempted)),
        'symbols_with_data': len(set(all_with_data)),
        'total_trades_all_variants': len(all_trades),
        'variants':        results,
        'coin_variant_matrix': coin_variant_matrix,
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # ── Human-readable summary ─────────────────────────────
    lines = []
    def w(s=''): lines.append(s)

    w('=' * 70)
    w('G MAX V1 — MULTI-VARIANT BACKTEST REPORT')
    w('=' * 70)
    w(f'Period    : {report["period"]}')
    w(f'Timeframe : {TIMEFRAME}')
    w(f'Capital   : ${CAPITAL:,.0f}')
    w(f'Coins tried / with data: {report["symbols_attempted"]} / {report["symbols_with_data"]}')
    w(f'Total trades (all variants): {report["total_trades_all_variants"]}')
    w()

    # Variant scoreboard
    w('─' * 70)
    w('VARIANT SCOREBOARD')
    w('─' * 70)
    hdr = f'{"Variant":<22} {"Trades":>7} {"WR%":>6} {"PF":>7} {"Net PnL":>10} {"Max DD":>9} {"Sharpe":>8} {"Expect":>8}'
    w(hdr)
    w('─' * 70)
    for vk in VARIANT_KEYS:
        s = results[vk]['stats']
        vname = results[vk]['variant_name']
        usable = '✅' if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42 else ('⚠️ ' if s['profit_factor'] >= 1.0 else '❌')
        w(f'{usable} {vname:<20} {s["total"]:>7} {s["win_rate"]:>6.1f} {s["profit_factor"]:>7.3f} '
          f'${s["net_pnl"]:>9,.2f} ${s["max_drawdown"]:>8,.2f} {s["sharpe"]:>8.3f} {s["expectancy"]:>8.4f}')
    w()

    # Per-variant detail
    for vk in VARIANT_KEYS:
        res  = results[vk]
        s    = res['stats']
        vcfg = res['config']
        w('=' * 70)
        w(f'VARIANT {vk}: {res["variant_name"]}')
        w(f'  Leverage: {vcfg["leverage"]}x  |  ADX min: {vcfg["adx_min"]}')
        if vcfg['tp_mode'] == 'adx_scaled':
            w(f'  TP: ADX 22-35→1%  35-50→2%  50+→3%')
        else:
            w(f'  TP: {vcfg["tp_pct"]*100:.1f}%  SL: 15%')
        w(f'  Trades: {s["total"]}  |  Longs: {s["longs"]}  Shorts: {s["shorts"]}')
        w(f'  Win Rate      : {s["win_rate"]:.2f}%')
        w(f'  Profit Factor : {s["profit_factor"]:.4f}')
        w(f'  Net PnL       : ${s["net_pnl"]:,.2f}')
        w(f'  Max Drawdown  : ${s["max_drawdown"]:,.2f}')
        w(f'  Sharpe Ratio  : {s["sharpe"]:.4f}')
        w(f'  Avg Win       : ${s["avg_win"]:.2f}')
        w(f'  Avg Loss      : ${s["avg_loss"]:.2f}')
        w(f'  Expectancy    : ${s["expectancy"]:.4f}')

        # Top 20 coins by PnL
        pc = s['per_coin']
        top = sorted(pc.items(), key=lambda x: x[1]['pnl'], reverse=True)[:20]
        w()
        w(f'  TOP 20 COINS BY PNL [{vk}]:')
        w(f'  {"Coin":<22} {"Trades":>6} {"WR%":>6} {"PnL":>10}')
        for sym, d in top:
            w(f'  {sym:<22} {d["n"]:>6} {d["wr"]:>6.1f} ${d["pnl"]:>9,.2f}')

        # Monthly PnL
        w()
        w(f'  MONTHLY PNL [{vk}]:')
        w(f'  {"Month":<10} {"Trades":>6} {"Wins":>5} {"WR%":>6} {"PnL":>10}')
        for month in sorted(s['monthly'].keys()):
            md = s['monthly'][month]
            mwr = md['w']/md['n']*100 if md['n'] > 0 else 0
            w(f'  {month:<10} {md["n"]:>6} {md["w"]:>5} {mwr:>6.1f} ${md["pnl"]:>9,.2f}')
        w()

    # Cross-variant coin table
    w('=' * 70)
    w('COIN × VARIANT MATRIX  (Net PnL per coin per variant)')
    w('─' * 70)
    hdr2 = f'{"Coin":<22}' + ''.join(f'{vk:>10}' for vk in VARIANT_KEYS)
    w(hdr2)
    w('─' * 70)
    all_coins = sorted(coin_variant_matrix.keys())
    for sym in all_coins:
        row = f'{sym:<22}'
        for vk in VARIANT_KEYS:
            d = coin_variant_matrix[sym].get(vk)
            if d:
                row += f'{d["pnl"]:>10,.1f}'
            else:
                row += f'{"—":>10}'
        w(row)
    w()

    # Recommendation
    w('=' * 70)
    w('RECOMMENDATION SUMMARY')
    w('─' * 70)
    for vk in VARIANT_KEYS:
        s = results[vk]['stats']
        if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42:
            verdict = '✅ USABLE (PF≥1.5, WR≥42%)'
        elif s['profit_factor'] >= 1.0:
            verdict = '⚠️  MARGINAL (PF≥1.0 but below targets)'
        else:
            verdict = '❌ NOT USABLE (PF<1.0)'
        w(f'{vk:<6} {results[vk]["variant_name"]:<25} → {verdict}')
    w('=' * 70)

    summary_text = '\n'.join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary_text)
    print(summary_text, flush=True)

    # Zip results
    with zipfile.ZipFile('backtest_results.zip', 'w', zipfile.ZIP_DEFLATED) as z:
        z.write('backtest_report.json')
        z.write('backtest_summary.txt')
    print('\nDone! backtest_results.zip created.', flush=True)

# ── Entry Point ────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python backtest.py <shard_idx|merge>')
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

