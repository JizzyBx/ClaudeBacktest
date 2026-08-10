"""
G Max V1 — Multi-Variant Backtest  (FAST EDITION)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Variants:
  A   : ADX-Scaled TP 5x  (ADX 22-35→1%, 35-50→2%, 50+→3%)
  L6  : Leverage Sweep 6x  (fixed 3% TP, 15% SL)
  L7  : Leverage Sweep 7x
  L8  : Leverage Sweep 8x
  L9  : Leverage Sweep 9x
  L10 : Leverage Sweep 10x
  B   : ADX≥35 filter 5x  (fixed 3% TP, 15% SL)

Coins   : 117 (Universe)
Timeframe: 15m | Period: Aug 2024 – Jul 2026
Capital : $10,000 | Shards: 10 | Workers: 32
Speed tricks:
  - ALL months for ALL coins in shard fetched in one parallel burst
  - 10s HTTP timeout, 3 retries per month
  - Per-symbol fetch timeout via future.result(timeout=90)
  - stdlib only
"""

import sys, json, os, csv, io, math, time, zipfile, threading
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
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

# ── Config ─────────────────────────────────────────────────
NUM_SHARDS = 10
WORKERS    = 32        # parallel fetch workers per shard
TIMEFRAME  = '15m'
START_YM   = (2024, 8)
END_YM     = (2026, 7)   # inclusive
CAPITAL    = 10000.0
RISK_PCT   = 0.01
FEE        = 0.0005
SLIP       = 0.0003
SL_PCT     = 0.150
MAX_BARS   = 960
MIN_BARS   = 100
HTTP_TIMEOUT = 10        # seconds per request
MAX_RETRIES  = 3

# ── Variants ───────────────────────────────────────────────
VARIANTS = {
    'A':   {'name': 'ADX-Scaled TP 5x',  'leverage': 5,  'tp_mode': 'adx_scaled', 'tp_pct': None, 'adx_min': 22},
    'L6':  {'name': 'Leverage 6x',        'leverage': 6,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L7':  {'name': 'Leverage 7x',        'leverage': 7,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L8':  {'name': 'Leverage 8x',        'leverage': 8,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L9':  {'name': 'Leverage 9x',        'leverage': 9,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'L10': {'name': 'Leverage 10x',       'leverage': 10, 'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 22},
    'B':   {'name': 'ADX≥35 Filter 5x',  'leverage': 5,  'tp_mode': 'fixed',      'tp_pct': 0.03, 'adx_min': 35},
}
VARIANT_KEYS = list(VARIANTS.keys())

# ── Build month list once ──────────────────────────────────
def all_months():
    months = []
    y, m = START_YM
    ey, em = END_YM
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12: m = 1; y += 1
    return months

MONTHS = all_months()
BASE_URL = 'https://data.binance.vision/data/futures/um/monthly/klines'

# ── Fetch one month (with retries) ────────────────────────
def fetch_month(symbol, year, month):
    url = f'{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip'
    for attempt in range(MAX_RETRIES):
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, timeout=HTTP_TIMEOUT) as r:
                data = r.read()
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                with z.open(z.namelist()[0]) as f:
                    rows = list(csv.reader(io.TextIOWrapper(f)))
            out = []
            for row in rows:
                if not row or row[0].startswith('#'): continue
                try:
                    ts = int(row[0])
                    if ts > 10**14: ts //= 1000
                    out.append((ts, float(row[1]), float(row[2]),
                                float(row[3]), float(row[4])))
                except (ValueError, IndexError):
                    continue
            return out
        except HTTPError as e:
            if e.code == 404:
                return []   # coin didn't exist yet that month
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))
    return []

# ── Fetch all months for one symbol (parallel months) ─────
def fetch_symbol(symbol):
    """Fetches all months for symbol in parallel threads."""
    month_data = {}
    with ThreadPoolExecutor(max_workers=min(len(MONTHS), 8)) as ex:
        futs = {ex.submit(fetch_month, symbol, y, m): (y, m) for y, m in MONTHS}
        for fut in as_completed(futs):
            ym = futs[fut]
            try:
                month_data[ym] = fut.result(timeout=15)
            except Exception:
                month_data[ym] = []

    # Merge in chronological order, dedup by ts
    seen = {}
    for ym in sorted(month_data):
        for c in month_data[ym]:
            seen[c[0]] = c
    return sorted(seen.values(), key=lambda x: x[0])

# ── Indicators ─────────────────────────────────────────────
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
        up   = highs[i]   - highs[i-1]
        down = lows[i-1]  - lows[i]
        pdm.append(up   if up   > down and up   > 0 else 0.0)
        mdm.append(down if down > up   and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))

    def ws(v, p):
        if len(v) < p: return []
        r = [sum(v[:p])]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r

    st = ws(trs, period)
    sp = ws(pdm, period)
    sm = ws(mdm, period)
    if not st: return 0.0

    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]

    if len(dx) < period: return 0.0
    adx = sum(dx[:period]) / period
    for d in dx[period:]: adx = (adx*(period-1) + d) / period
    return max(0.0, min(100.0, adx))

# ── Signal check ───────────────────────────────────────────
def check_signal(closes, highs, lows, adx_min):
    if len(closes) < MIN_BARS:
        return None, 0.0
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    i   = len(closes) - 2

    if i < 10: return None, 0.0

    slope = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope >  0.05
    trend_down = slope < -0.05
    if not trend_up and not trend_down: return None, 0.0

    cross_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    cross_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

    if trend_up   and not cross_up:   return None, 0.0
    if trend_down and not cross_down: return None, 0.0

    adx_val = adx_calc(highs, lows, closes, 14)
    if adx_val < adx_min: return None, adx_val

    return ('buy' if cross_up else 'sell'), adx_val

# ── TP resolver ────────────────────────────────────────────
def resolve_tp(tp_mode, tp_pct, adx_val):
    if tp_mode == 'fixed': return tp_pct
    if adx_val >= 50: return 0.03
    if adx_val >= 35: return 0.02
    return 0.01

# ── Backtest one symbol × one variant ─────────────────────
def backtest_symbol(symbol, candles, vcfg, vk):
    leverage = vcfg['leverage']
    tp_mode  = vcfg['tp_mode']
    tp_pct_c = vcfg['tp_pct']
    adx_min  = vcfg['adx_min']

    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    ts     = [c[0] for c in candles]

    trades = []
    in_trade = False
    side = entry_p = entry_ts = entry_bar = None
    tp = sl = notional = adx_stored = tp_used = 0.0

    for i in range(MIN_BARS, len(closes) - 1):
        if not in_trade:
            sig, adx_val = check_signal(closes[:i+1], highs[:i+1], lows[:i+1], adx_min)
            if sig is None: continue

            ep = opens[i+1]
            tp_pct = resolve_tp(tp_mode, tp_pct_c, adx_val)
            notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * leverage)

            if sig == 'buy':
                entry_p = ep * (1 + FEE + SLIP)
                sl = entry_p * (1 - SL_PCT)
                tp = entry_p * (1 + tp_pct)
            else:
                entry_p = ep * (1 - FEE - SLIP)
                sl = entry_p * (1 + SL_PCT)
                tp = entry_p * (1 - tp_pct)

            side = sig; entry_ts = ts[i+1]
            entry_bar = i+1; adx_stored = adx_val; tp_used = tp_pct
            in_trade = True
            continue

        bars_held = i - entry_bar
        reason = exit_p = None

        if side == 'buy':
            if lows[i]  <= sl: reason = 'sl';  exit_p = sl
            elif highs[i] >= tp: reason = 'tp';  exit_p = tp
        else:
            if highs[i] >= sl: reason = 'sl';  exit_p = sl
            elif lows[i]  <= tp: reason = 'tp';  exit_p = tp

        if reason is None and bars_held >= MAX_BARS:
            reason = 'max_hold'; exit_p = closes[i]
        if reason is None and i == len(closes) - 2:
            reason = 'end';      exit_p = closes[i]

        if reason:
            gross = ((exit_p - entry_p) / entry_p if side == 'buy'
                     else (entry_p - exit_p) / entry_p)
            net = gross - (FEE + SLIP) * 2
            pnl = notional * net
            trades.append({
                'symbol':   symbol,
                'variant':  vk,
                'side':     side,
                'entry_ts': entry_ts,
                'exit_ts':  ts[i],
                'pnl':      round(pnl, 4),
                'reason':   reason,
                'bars':     bars_held,
                'adx':      round(adx_stored, 1),
                'tp_pct':   round(tp_used * 100, 1),
            })
            in_trade = False

    return trades

# ── Stats ──────────────────────────────────────────────────
def calc_stats(trades):
    empty = {
        'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0,
        'net_pnl': 0.0, 'max_drawdown': 0.0, 'avg_win': 0.0,
        'avg_loss': 0.0, 'expectancy': 0.0, 'sharpe': 0.0,
        'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
    }
    if not trades: return empty

    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gp = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))

    pf  = (gp / gl) if gl > 0 else (float('inf') if gp > 0 else 0.0)
    wr  = len(wins) / len(trades) * 100
    aw  = gp / len(wins)   if wins   else 0.0
    al  = gl / len(losses) if losses else 0.0
    exp = (wr/100 * aw) - ((1 - wr/100) * al)

    # Max drawdown
    sorted_t = sorted(trades, key=lambda t: t['exit_ts'])
    eq = peak = max_dd = 0.0
    for t in sorted_t:
        eq += t['pnl']
        if eq > peak: peak = eq
        if peak - eq > max_dd: max_dd = peak - eq

    # Sharpe (annualised daily)
    daily = {}
    for t in sorted_t:
        day = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m-%d')
        daily[day] = daily.get(day, 0.0) + t['pnl']
    dr = list(daily.values())
    if len(dr) >= 2:
        mu  = sum(dr) / len(dr)
        std = math.sqrt(sum((r-mu)**2 for r in dr) / (len(dr)-1))
        sharpe = (mu / std * math.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Monthly
    monthly = {}
    for t in sorted_t:
        key = datetime.utcfromtimestamp(t['exit_ts']/1000).strftime('%Y-%m')
        if key not in monthly: monthly[key] = {'pnl': 0.0, 'n': 0, 'w': 0}
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        if t['pnl'] > 0: monthly[key]['w'] += 1

    # Per-coin
    per_coin = {}
    for t in sorted_t:
        s = t['symbol']
        if s not in per_coin: per_coin[s] = {'pnl': 0.0, 'n': 0, 'w': 0}
        per_coin[s]['pnl'] += t['pnl']
        per_coin[s]['n']   += 1
        if t['pnl'] > 0: per_coin[s]['w'] += 1
    for s in per_coin:
        d = per_coin[s]
        d['wr']  = round(d['w']/d['n']*100, 1) if d['n'] > 0 else 0.0
        d['pnl'] = round(d['pnl'], 2)

    return {
        'total':         len(trades),
        'win_rate':      round(wr, 2),
        'profit_factor': round(pf, 4),
        'net_pnl':       round(sum(t['pnl'] for t in trades), 2),
        'max_drawdown':  round(max_dd, 2),
        'avg_win':       round(aw, 2),
        'avg_loss':      round(al, 2),
        'expectancy':    round(exp, 4),
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
    print(f'[S{shard_idx}] {len(symbols)} coins', flush=True)

    # ── Step 1: Fetch ALL coins in parallel ──────────────
    candle_map = {}
    print(f'[S{shard_idx}] Fetching {len(symbols)} symbols × {len(MONTHS)} months...', flush=True)

    def fetch_one(sym):
        t_start = time.time()
        data = fetch_symbol(sym)
        elapsed = time.time() - t_start
        return sym, data, elapsed

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_one, sym): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                s, data, el = fut.result(timeout=90)
                candle_map[s] = data
                status = f'{len(data)} bars ({el:.1f}s)'
            except FutureTimeout:
                candle_map[sym] = []
                status = 'TIMEOUT'
            except Exception as e:
                candle_map[sym] = []
                status = f'ERR: {e}'
            print(f'[S{shard_idx}] {sym}: {status}', flush=True)

    fetch_done = time.time()
    print(f'[S{shard_idx}] Fetch done in {fetch_done-t0:.1f}s', flush=True)

    # ── Step 2: Backtest all variants ────────────────────
    all_trades = []
    skipped    = []

    for sym in symbols:
        candles = candle_map.get(sym, [])
        if len(candles) < MIN_BARS + 10:
            skipped.append(sym)
            print(f'[S{shard_idx}] SKIP {sym}: {len(candles)} bars', flush=True)
            continue

        for vk, vcfg in VARIANTS.items():
            try:
                trades = backtest_symbol(sym, candles, vcfg, vk)
                all_trades.extend(trades)
                print(f'[S{shard_idx}] {sym}[{vk}]: {len(trades)} trades', flush=True)
            except Exception as e:
                print(f'[S{shard_idx}] {sym}[{vk}] ERROR: {e}', flush=True)

    # Per-variant stats for this shard
    variant_stats = {}
    for vk in VARIANT_KEYS:
        vtrades = [t for t in all_trades if t['variant'] == vk]
        variant_stats[vk] = calc_stats(vtrades)

    elapsed = round(time.time() - t0, 1)
    out = {
        'shard':         shard_idx,
        'symbols':       symbols,
        'skipped':       skipped,
        'trades':        all_trades,
        'variant_stats': variant_stats,
        'elapsed':       elapsed,
    }
    with open(f'shard_{shard_idx}.json', 'w') as f:
        json.dump(out, f)
    print(f'[S{shard_idx}] DONE {elapsed}s | {len(all_trades)} trades | '
          f'{len(symbols)-len(skipped)}/{len(symbols)} coins', flush=True)

# ── Merge & Report ─────────────────────────────────────────
def merge_shards():
    all_trades = []
    all_symbols = []
    all_skipped = []

    for i in range(NUM_SHARDS):
        path = f'shard_{i}.json'
        if not os.path.exists(path):
            print(f'WARNING: {path} missing', flush=True)
            continue
        with open(path) as f:
            s = json.load(f)
        all_trades.extend(s['trades'])
        all_symbols.extend(s['symbols'])
        all_skipped.extend(s.get('skipped', []))

    results = {}
    for vk, vcfg in VARIANTS.items():
        vtrades = [t for t in all_trades if t['variant'] == vk]
        results[vk] = {'name': vcfg['name'], 'config': vcfg, 'stats': calc_stats(vtrades)}

    # Coin × Variant matrix
    coin_matrix = {}
    for vk in VARIANT_KEYS:
        vtrades = [t for t in all_trades if t['variant'] == vk]
        for t in vtrades:
            sym = t['symbol']
            if sym not in coin_matrix: coin_matrix[sym] = {}
            if vk not in coin_matrix[sym]:
                coin_matrix[sym][vk] = {'pnl': 0.0, 'n': 0, 'w': 0}
            coin_matrix[sym][vk]['pnl'] += t['pnl']
            coin_matrix[sym][vk]['n']   += 1
            if t['pnl'] > 0: coin_matrix[sym][vk]['w'] += 1

    # ── Build report text ──────────────────────────────
    lines = []
    def w(s=''): lines.append(s)

    W = 72
    w('=' * W)
    w('   G MAX V1 — MULTI-VARIANT BACKTEST REPORT')
    w('=' * W)
    w(f'  Period   : Aug 2024 – Jul 2026 (15m candles)')
    w(f'  Capital  : ${CAPITAL:,.0f} | SL: 15% | Max hold: 10 days')
    w(f'  Coins    : {len(set(all_symbols))} attempted | {len(set(all_symbols))-len(set(all_skipped))} with data')
    w(f'  Skipped  : {sorted(set(all_skipped))}')
    w(f'  Trades   : {len(all_trades)} total across all variants')
    w()

    # Scoreboard
    w('─' * W)
    w('  SCOREBOARD')
    w('─' * W)
    w(f'  {"Var":<6} {"Name":<22} {"Trades":>7} {"WR%":>6} {"PF":>6} '
      f'{"Net PnL":>10} {"MaxDD":>9} {"Sharpe":>7}')
    w('─' * W)
    for vk in VARIANT_KEYS:
        s = results[vk]['stats']
        nm = results[vk]['name']
        if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42:
            flag = '✅'
        elif s['profit_factor'] >= 1.0:
            flag = '⚠️ '
        else:
            flag = '❌'
        w(f'  {flag}{vk:<5} {nm:<22} {s["total"]:>7} {s["win_rate"]:>6.1f} '
          f'{s["profit_factor"]:>6.3f} ${s["net_pnl"]:>9,.0f} ${s["max_drawdown"]:>8,.0f} '
          f'{s["sharpe"]:>7.3f}')
    w()

    # Per-variant detail
    for vk in VARIANT_KEYS:
        res = results[vk]
        s   = res['stats']
        cfg = res['config']
        w('=' * W)
        w(f'  VARIANT {vk} — {res["name"]}')
        w('─' * W)
        if cfg['tp_mode'] == 'adx_scaled':
            w(f'  Leverage {cfg["leverage"]}x | ADX min {cfg["adx_min"]} | TP: 22-35→1%  35-50→2%  50+→3%')
        else:
            w(f'  Leverage {cfg["leverage"]}x | ADX min {cfg["adx_min"]} | TP: {cfg["tp_pct"]*100:.0f}% | SL: 15%')
        w(f'  Trades: {s["total"]}  Longs: {s["longs"]}  Shorts: {s["shorts"]}')
        w(f'  Win Rate      : {s["win_rate"]:.2f}%')
        w(f'  Profit Factor : {s["profit_factor"]:.4f}')
        w(f'  Net PnL       : ${s["net_pnl"]:,.2f}')
        w(f'  Max Drawdown  : ${s["max_drawdown"]:,.2f}')
        w(f'  Sharpe Ratio  : {s["sharpe"]:.4f}')
        w(f'  Avg Win/Loss  : ${s["avg_win"]:.2f} / ${s["avg_loss"]:.2f}')
        w(f'  Expectancy    : ${s["expectancy"]:.4f} per trade')

        # Top 20 coins
        pc = s['per_coin']
        top = sorted(pc.items(), key=lambda x: x[1]['pnl'], reverse=True)[:20]
        w()
        w(f'  TOP 20 COINS:')
        w(f'  {"Coin":<22} {"N":>5} {"WR%":>6} {"PnL":>10}')
        for sym, d in top:
            w(f'  {sym:<22} {d["n"]:>5} {d["wr"]:>6.1f} ${d["pnl"]:>9,.2f}')

        # Bottom 5
        bot = sorted(pc.items(), key=lambda x: x[1]['pnl'])[:5]
        w()
        w(f'  BOTTOM 5 COINS:')
        for sym, d in bot:
            w(f'  {sym:<22} {d["n"]:>5} {d["wr"]:>6.1f} ${d["pnl"]:>9,.2f}')

        # Monthly
        w()
        w(f'  MONTHLY:')
        w(f'  {"Month":<9} {"N":>5} {"W":>5} {"WR%":>6} {"PnL":>10}')
        for mo in sorted(s['monthly']):
            md = s['monthly'][mo]
            mwr = md['w']/md['n']*100 if md['n'] else 0
            w(f'  {mo:<9} {md["n"]:>5} {md["w"]:>5} {mwr:>6.1f} ${md["pnl"]:>9,.2f}')
        w()

    # Coin × Variant matrix
    w('=' * W)
    w('  COIN × VARIANT MATRIX  (Net PnL)')
    w('─' * W)
    hdr = f'  {"Coin":<22}' + ''.join(f'{vk:>9}' for vk in VARIANT_KEYS)
    w(hdr)
    w('─' * W)
    # Sort coins by total PnL across variants
    def coin_total(sym):
        return sum(coin_matrix[sym].get(vk, {}).get('pnl', 0) for vk in VARIANT_KEYS)
    for sym in sorted(coin_matrix, key=coin_total, reverse=True):
        row = f'  {sym:<22}'
        for vk in VARIANT_KEYS:
            d = coin_matrix[sym].get(vk)
            row += f'{d["pnl"]:>9,.0f}' if d else f'{"—":>9}'
        w(row)
    w()

    # Best variant per coin
    w('=' * W)
    w('  BEST VARIANT PER COIN')
    w('─' * W)
    w(f'  {"Coin":<22} {"BestVar":>8} {"PnL":>10} {"WR%":>7}')
    w('─' * W)
    for sym in sorted(coin_matrix):
        best_vk = max(coin_matrix[sym], key=lambda vk: coin_matrix[sym][vk]['pnl'])
        d = coin_matrix[sym][best_vk]
        w(f'  {sym:<22} {best_vk:>8} ${d["pnl"]:>9,.2f} {d.get("wr",0):>6.1f}%')
    w()

    # Recommendation
    w('=' * W)
    w('  VERDICT')
    w('─' * W)
    for vk in VARIANT_KEYS:
        s = results[vk]['stats']
        pf = s['profit_factor']
        if pf >= 1.5 and s['win_rate'] >= 42:
            v = f'✅ USABLE     PF={pf:.3f} WR={s["win_rate"]:.1f}% Sharpe={s["sharpe"]:.3f}'
        elif pf >= 1.0:
            v = f'⚠️  MARGINAL   PF={pf:.3f} WR={s["win_rate"]:.1f}% Sharpe={s["sharpe"]:.3f}'
        else:
            v = f'❌ FAIL       PF={pf:.3f} WR={s["win_rate"]:.1f}% Sharpe={s["sharpe"]:.3f}'
        w(f'  {vk:<6} {results[vk]["name"]:<22} → {v}')
    w('=' * W)

    text = '\n'.join(lines)
    print(text, flush=True)

    with open('backtest_summary.txt', 'w') as f:
        f.write(text)

    report = {
        'results':      {vk: {'name': results[vk]['name'],
                               'config': results[vk]['config'],
                               'stats': results[vk]['stats']} for vk in VARIANT_KEYS},
        'coin_matrix':  coin_matrix,
        'total_trades': len(all_trades),
    }
    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    with zipfile.ZipFile('backtest_results.zip', 'w', zipfile.ZIP_DEFLATED) as z:
        z.write('backtest_summary.txt')
        z.write('backtest_report.json')

    print(f'\n✅ Done! backtest_results.zip ready.', flush=True)

# ── Entry ──────────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python backtest.py <shard_idx|merge>')
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

