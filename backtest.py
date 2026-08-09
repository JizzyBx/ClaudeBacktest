"""
G Max — 6-Variant Backtest
Variants: A (ADX+DI), B (ATR-TP), C (HTF 1h), D (S/R), E (RSI gate), F (ADX28+TP5%)
Coins: 117 COINS_UNIVERSE | Timeframe: 15m | Period: 2 years
Leverage: 5x | Margin: 2% of capital per trade (% based)
Shards: 20 | Workers: 16 per shard | stdlib only
"""

import sys, json, csv, io, zipfile, urllib.request, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ── Coin Universe (117 coins) ──────────────────────────────
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

# ── Date range: 2 years back from today ───────────────────
_NOW       = datetime.now(timezone.utc)
END_YM     = (_NOW.year, _NOW.month)
_START     = _NOW - timedelta(days=730)
START_YM   = (_START.year, _START.month)

TIMEFRAME  = '15m'
HTF        = '1h'      # for Variant C

CAPITAL    = 10_000.0  # starting capital (shared across all variants independently)
MARGIN_PCT = 0.02      # 2% of current equity per trade
LEVERAGE   = 5
FEE        = 0.0005    # 0.05% per side
SLIP       = 0.0002    # 0.02% per side

MAX_BARS   = 960       # 10 days at 15m
MIN_BARS   = 100       # warmup candles needed

# ── Variant configs ────────────────────────────────────────
VARIANTS = {
    'A': {'name': 'ADX+DI Direction', 'tp': 0.030, 'sl': 0.120},
    'B': {'name': 'ATR Dynamic TP',   'tp': None,  'sl': 0.120, 'atr_mult': 2.5},
    'C': {'name': 'HTF 1h Confirm',   'tp': 0.040, 'sl': 0.120},
    'D': {'name': 'S/R Zone Filter',  'tp': 0.035, 'sl': 0.120},
    'E': {'name': 'RSI Gate',         'tp': 0.030, 'sl': 0.120},
    'F': {'name': 'ADX28 + TP5%',     'tp': 0.050, 'sl': 0.120},
}

# ── Data fetch ─────────────────────────────────────────────
BASE_URL = 'https://data.binance.vision/data/futures/um/monthly/klines'

def _months(start_ym, end_ym):
    y, m = start_ym
    ey, em = end_ym
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out

def fetch_month(symbol, year, month, tf=TIMEFRAME):
    url = f'{BASE_URL}/{symbol}/{tf}/{symbol}-{tf}-{year:04d}-{month:02d}.zip'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f))
                rows = []
                for row in reader:
                    try:
                        ts = int(row[0])
                        if ts > 10**14:
                            ts //= 1000
                        o  = float(row[1])
                        h  = float(row[2])
                        l  = float(row[3])
                        c  = float(row[4])
                        rows.append((ts, o, h, l, c))
                    except (ValueError, IndexError):
                        continue
                return rows
    except Exception:
        return []

def fetch_symbol(symbol, tf=TIMEFRAME):
    all_rows = []
    for y, m in _months(START_YM, END_YM):
        all_rows.extend(fetch_month(symbol, y, m, tf))
    seen = {}
    for row in all_rows:
        seen[row[0]] = row
    return [seen[k] for k in sorted(seen)]

# ── Indicators (pure Python) ───────────────────────────────
def ema(values, period):
    if not values:
        return []
    k = 2.0 / (period + 1)
    r = [values[0]]
    for v in values[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def atr_series(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1])
        ))
    if len(trs) < period:
        return [sum(trs)/len(trs)] * len(trs) if trs else []
    out = []
    a = sum(trs[:period]) / period
    out.append(a)
    for t in trs[period:]:
        a = (a * (period - 1) + t) / period
        out.append(a)
    # pad front so index aligns with closes[1:]
    return out

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))

def adx_full(highs, lows, closes, period=14):
    """Returns (adx, +DI, -DI). Returns (0,0,0) if not enough bars."""
    n = len(closes)
    if n < period * 3:
        return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1])
        ))
    def wilder(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1]/p + x)
        return r
    st = wilder(trs, period)
    sp = wilder(pdm, period)
    sm = wilder(mdm, period)
    if not st:
        return 0.0, 0.0, 0.0
    pdi = [100*p/t if t else 0 for p,t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m,t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi, mdi)]
    if len(dx) < period:
        return 0.0, pdi[-1] if pdi else 0.0, mdi[-1] if mdi else 0.0
    adx_v = sum(dx[:period]) / period
    for d in dx[period:]:
        adx_v = (adx_v * (period-1) + d) / period
    adx_v = max(0.0, min(100.0, adx_v))
    return adx_v, pdi[-1], mdi[-1]

# ── Signal functions per variant ───────────────────────────

def _base_filters(closes, highs, lows):
    """Shared: EMA50 slope, EMA9/21 cross. Returns (sig, e9, e21, e50, i) or None."""
    if len(closes) < MIN_BARS:
        return None
    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    i   = len(closes) - 2  # last closed bar

    if i < 10:
        return None
    slope_pct  = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    if not trend_up and not trend_down:
        return None

    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    if not crossed_up and not crossed_down:
        return None
    if trend_up   and not crossed_up:
        return None
    if trend_down and not crossed_down:
        return None

    sig = 'buy' if crossed_up else 'sell'
    return sig, e9, e21, e50, i, trend_up, trend_down

def signal_A(closes, highs, lows):
    """ADX + DI directional alignment."""
    base = _base_filters(closes, highs, lows)
    if base is None:
        return None
    sig, *_, i, trend_up, trend_down = base
    adx_v, pdi, mdi = adx_full(highs, lows, closes, 14)
    if adx_v < 22:
        return None
    if sig == 'buy'  and not (pdi > mdi):
        return None
    if sig == 'sell' and not (mdi > pdi):
        return None
    return sig

def signal_B(closes, highs, lows):
    """ATR dynamic TP — same entry as base + ADX>=22."""
    base = _base_filters(closes, highs, lows)
    if base is None:
        return None
    sig, *_, i, trend_up, trend_down = base
    adx_v, _, _ = adx_full(highs, lows, closes, 14)
    if adx_v < 22:
        return None
    return sig

def signal_C(closes, highs, lows, htf_closes, htf_highs, htf_lows):
    """HTF 1h EMA50 slope must confirm direction."""
    base = _base_filters(closes, highs, lows)
    if base is None:
        return None
    sig, *_, i, trend_up, trend_down = base
    adx_v, _, _ = adx_full(highs, lows, closes, 14)
    if adx_v < 22:
        return None
    # HTF filter
    if len(htf_closes) < 60:
        return None
    htf_e50    = ema(htf_closes, 50)
    hi         = len(htf_closes) - 2  # use closed HTF bar (no lookahead)
    if hi < 10:
        return None
    htf_slope  = (htf_e50[hi] - htf_e50[hi-10]) / htf_e50[hi-10] * 100
    htf_up     = htf_slope >  0.05
    htf_down   = htf_slope < -0.05
    if sig == 'buy'  and not htf_up:
        return None
    if sig == 'sell' and not htf_down:
        return None
    return sig

def signal_D(closes, highs, lows):
    """S/R zone: entry must be above swing low (buy) / below swing high (sell)."""
    base = _base_filters(closes, highs, lows)
    if base is None:
        return None
    sig, *_, i, trend_up, trend_down = base
    adx_v, _, _ = adx_full(highs, lows, closes, 14)
    if adx_v < 22:
        return None
    lookback = 20
    if i < lookback:
        return None
    swing_low  = min(lows[i-lookback:i])
    swing_high = max(highs[i-lookback:i])
    price      = closes[i]
    if sig == 'buy'  and not (price > swing_low  * 1.005):
        return None
    if sig == 'sell' and not (price < swing_high * 0.995):
        return None
    return sig

def signal_E(closes, highs, lows):
    """RSI gate: RSI>52 for buy, RSI<48 for sell."""
    base = _base_filters(closes, highs, lows)
    if base is None:
        return None
    sig, *_, i, trend_up, trend_down = base
    adx_v, _, _ = adx_full(highs, lows, closes, 14)
    if adx_v < 22:
        return None
    rsi_v = rsi(closes[:i+1], 14)
    if sig == 'buy'  and rsi_v <= 52:
        return None
    if sig == 'sell' and rsi_v >= 48:
        return None
    return sig

def signal_F(closes, highs, lows):
    """Stricter ADX>=28, wider TP 5%."""
    base = _base_filters(closes, highs, lows)
    if base is None:
        return None
    sig, *_, i, trend_up, trend_down = base
    adx_v, _, _ = adx_full(highs, lows, closes, 14)
    if adx_v < 28:
        return None
    return sig

# ── Single-symbol backtest ─────────────────────────────────

def backtest_symbol(symbol, candles_15m, candles_1h=None):
    """Returns dict of {variant_key: [trades]}."""
    if len(candles_15m) < MIN_BARS + 2:
        return {v: [] for v in VARIANTS}

    ts_15   = [c[0] for c in candles_15m]
    opens   = [c[1] for c in candles_15m]
    highs   = [c[2] for c in candles_15m]
    lows    = [c[3] for c in candles_15m]
    closes  = [c[4] for c in candles_15m]

    htf_closes = htf_highs = htf_lows = []
    if candles_1h:
        htf_closes = [c[4] for c in candles_1h]
        htf_highs  = [c[2] for c in candles_1h]
        htf_lows   = [c[3] for c in candles_1h]

    result = {v: [] for v in VARIANTS}

    # ATR series for variant B (aligns to closes[1:])
    atr = atr_series(highs, lows, closes, 14)

    n = len(closes)

    # one position tracker per variant
    positions = {v: None for v in VARIANTS}
    equity    = {v: CAPITAL for v in VARIANTS}

    for i in range(MIN_BARS, n - 1):
        entry_bar = i + 1
        if entry_bar >= n:
            break

        # get signal per variant at bar i (closed bar)
        sigs = {}

        cl_i = closes[:i+1]
        hi_i = highs[:i+1]
        lo_i = lows[:i+1]

        # HTF index: find last 1h bar closed before bar i's timestamp
        htf_i_end = 0
        if candles_1h:
            bar_ts = ts_15[i]
            for j in range(len(candles_1h)):
                if candles_1h[j][0] <= bar_ts:
                    htf_i_end = j + 1
                else:
                    break
            htf_i_end = min(htf_i_end, len(candles_1h))

        for vk in VARIANTS:
            pos = positions[vk]

            # ── Check exits first ──
            if pos is not None:
                p        = pos
                bars_held = i - p['entry_i']
                ep        = p['entry_price']
                side      = p['side']
                tp_price  = p['tp_price']
                sl_price  = p['sl_price']

                # Check SL first (conservative), then TP
                exited = False
                if side == 'buy':
                    if lows[i] <= sl_price:
                        exit_p = sl_price
                        reason = 'sl'
                        exited = True
                    elif highs[i] >= tp_price:
                        exit_p = tp_price
                        reason = 'tp'
                        exited = True
                else:
                    if highs[i] >= sl_price:
                        exit_p = sl_price
                        reason = 'sl'
                        exited = True
                    elif lows[i] <= tp_price:
                        exit_p = tp_price
                        reason = 'tp'
                        exited = True

                if not exited and bars_held >= MAX_BARS:
                    exit_p = closes[i]
                    reason = 'max_hold'
                    exited = True

                if exited:
                    if side == 'buy':
                        gross = (exit_p - ep) / ep
                    else:
                        gross = (ep - exit_p) / ep
                    net_pct = gross - (FEE + SLIP) * 2
                    pnl     = p['notional'] * net_pct
                    equity[vk] += pnl
                    result[vk].append({
                        'symbol':      symbol,
                        'variant':     vk,
                        'side':        side,
                        'entry_ts':    ts_15[p['entry_i']],
                        'exit_ts':     ts_15[i],
                        'entry_price': ep,
                        'exit_price':  exit_p,
                        'pnl':         pnl,
                        'reason':      reason,
                        'bars':        bars_held,
                    })
                    positions[vk] = None
                    pos = None

            # ── Check for new entry if flat ──
            if pos is not None:
                continue  # already in trade

            # compute signal
            sig = None
            if vk == 'A':
                sig = signal_A(cl_i, hi_i, lo_i)
            elif vk == 'B':
                sig = signal_B(cl_i, hi_i, lo_i)
            elif vk == 'C':
                if htf_i_end > 0:
                    sig = signal_C(
                        cl_i, hi_i, lo_i,
                        [c[4] for c in candles_1h[:htf_i_end]],
                        [c[2] for c in candles_1h[:htf_i_end]],
                        [c[3] for c in candles_1h[:htf_i_end]],
                    )
            elif vk == 'D':
                sig = signal_D(cl_i, hi_i, lo_i)
            elif vk == 'E':
                sig = signal_E(cl_i, hi_i, lo_i)
            elif vk == 'F':
                sig = signal_F(cl_i, hi_i, lo_i)

            if sig is None:
                continue

            # Entry price: next bar open adjusted for fee+slip
            ep_raw = opens[entry_bar]
            if sig == 'buy':
                ep = ep_raw * (1 + FEE + SLIP)
            else:
                ep = ep_raw * (1 - FEE - SLIP)

            # Position sizing: % of current equity
            margin    = equity[vk] * MARGIN_PCT
            notional  = margin * LEVERAGE

            # TP / SL prices
            cfg = VARIANTS[vk]
            if vk == 'B':
                # ATR-based TP
                # atr aligns to closes[1:], so atr[i-1] corresponds to close[i]
                atr_idx = i - 1
                atr_val = atr[atr_idx] if 0 <= atr_idx < len(atr) else closes[i] * 0.005
                tp_dist = atr_val * cfg['atr_mult']
                if sig == 'buy':
                    tp_price = ep + tp_dist
                    sl_price = ep * (1 - cfg['sl'])
                else:
                    tp_price = ep - tp_dist
                    sl_price = ep * (1 + cfg['sl'])
            else:
                tp_pct = cfg['tp']
                sl_pct = cfg['sl']
                if sig == 'buy':
                    tp_price = ep * (1 + tp_pct)
                    sl_price = ep * (1 - sl_pct)
                else:
                    tp_price = ep * (1 - tp_pct)
                    sl_price = ep * (1 + sl_pct)

            positions[vk] = {
                'side':        sig,
                'entry_i':     entry_bar,
                'entry_price': ep,
                'tp_price':    tp_price,
                'sl_price':    sl_price,
                'notional':    notional,
            }

    # Close any open positions at end of data
    for vk, pos in positions.items():
        if pos is not None:
            ep    = pos['entry_price']
            exit_p = closes[-1]
            side  = pos['side']
            if side == 'buy':
                gross = (exit_p - ep) / ep
            else:
                gross = (ep - exit_p) / ep
            net_pct = gross - (FEE + SLIP) * 2
            pnl     = pos['notional'] * net_pct
            equity[vk] += pnl
            result[vk].append({
                'symbol':      symbol,
                'variant':     vk,
                'side':        side,
                'entry_ts':    ts_15[pos['entry_i']],
                'exit_ts':     ts_15[-1],
                'entry_price': ep,
                'exit_price':  exit_p,
                'pnl':         pnl,
                'reason':      'end_of_data',
                'bars':        len(closes) - 1 - pos['entry_i'],
            })

    return result

# ── Stats ──────────────────────────────────────────────────

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
    gp     = sum(wins)
    gl     = abs(sum(losses))
    pf     = gp / gl if gl else float('inf')

    # max drawdown
    equity = 0.0
    peak   = 0.0
    max_dd = 0.0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        equity += t['pnl']
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # monthly
    monthly = {}
    for t in trades:
        dt  = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        if key not in monthly:
            monthly[key] = {'pnl': 0.0, 'n': 0, 'w': 0}
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        if t['pnl'] > 0:
            monthly[key]['w'] += 1

    # per coin
    per_coin = {}
    for t in trades:
        sym = t['symbol']
        if sym not in per_coin:
            per_coin[sym] = {'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0}
        per_coin[sym]['pnl'] += t['pnl']
        per_coin[sym]['n']   += 1
        if t['pnl'] > 0:
            per_coin[sym]['w'] += 1
    for v in per_coin.values():
        v['wr'] = round(v['w'] / v['n'] * 100, 1) if v['n'] else 0.0

    return {
        'total':         len(trades),
        'win_rate':      round(len(wins) / len(trades) * 100, 2),
        'profit_factor': round(pf, 4),
        'net_pnl':       round(sum(t['pnl'] for t in trades), 2),
        'max_drawdown':  round(max_dd, 2),
        'avg_win':       round(sum(wins)   / len(wins)   if wins   else 0.0, 2),
        'avg_loss':      round(sum(losses) / len(losses) if losses else 0.0, 2),
        'expectancy':    round((sum(wins) + sum(losses)) / len(trades), 2),
        'longs':         sum(1 for t in trades if t['side'] == 'buy'),
        'shorts':        sum(1 for t in trades if t['side'] == 'sell'),
        'monthly':       monthly,
        'per_coin':      per_coin,
    }

# ── Shard runner ───────────────────────────────────────────

def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} coins: {symbols}", flush=True)

    # fetch 15m data in parallel
    data_15m = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, sym, TIMEFRAME): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                data_15m[sym] = fut.result()
            except Exception as e:
                print(f"  [Shard {shard_idx}] fetch error {sym}: {e}", flush=True)
                data_15m[sym] = []

    # fetch 1h data in parallel (needed for Variant C)
    data_1h = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, sym, HTF): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                data_1h[sym] = fut.result()
            except Exception as e:
                data_1h[sym] = []

    t0     = time.time()
    all_trades   = {v: [] for v in VARIANTS}
    with_data    = []
    no_data      = []

    for sym in symbols:
        c15 = data_15m.get(sym, [])
        c1h = data_1h.get(sym, [])
        if len(c15) < MIN_BARS + 2:
            no_data.append(sym)
            continue
        with_data.append(sym)
        res = backtest_symbol(sym, c15, c1h)
        for vk, trades in res.items():
            all_trades[vk].extend(trades)

    if not with_data:
        print(f"[Shard {shard_idx}] GEO-BLOCK or no data — aborting", flush=True)
        sys.exit(1)

    variant_stats = {vk: stats(all_trades[vk]) for vk in VARIANTS}
    all_trades_flat = []
    for vk, trades in all_trades.items():
        all_trades_flat.extend(trades)

    out = {
        'shard':      shard_idx,
        'symbols':    symbols,
        'with_data':  with_data,
        'no_data':    no_data,
        'trades':     all_trades_flat,
        'variant_stats': {vk: variant_stats[vk] for vk in VARIANTS},
        'elapsed':    round(time.time() - t0, 1),
    }
    fname = f'shard_{shard_idx}.json'
    with open(fname, 'w') as f:
        json.dump(out, f)

    for vk in VARIANTS:
        st = variant_stats[vk]
        print(
            f"  [Shard {shard_idx}] {vk}: "
            f"{st['total']} trades | PF {st['profit_factor']} | "
            f"WR {st['win_rate']}% | PnL ${st['net_pnl']}",
            flush=True
        )
    print(f"[Shard {shard_idx}] done in {out['elapsed']}s — "
          f"{len(with_data)}/{len(symbols)} coins had data", flush=True)

# ── Merge ──────────────────────────────────────────────────

def merge_shards():
    all_trades  = {v: [] for v in VARIANTS}
    all_symbols = []
    with_data   = []
    no_data     = []
    total_elapsed = 0.0

    for idx in range(NUM_SHARDS):
        fname = f'shard_{idx}.json'
        try:
            with open(fname) as f:
                shard = json.load(f)
        except FileNotFoundError:
            print(f"WARNING: {fname} not found — skipping", flush=True)
            continue
        all_symbols.extend(shard.get('symbols', []))
        with_data.extend(shard.get('with_data', []))
        no_data.extend(shard.get('no_data', []))
        total_elapsed += shard.get('elapsed', 0.0)
        for t in shard.get('trades', []):
            all_trades[t['variant']].append(t)

    # compute stats per variant
    var_stats = {vk: stats(all_trades[vk]) for vk in VARIANTS}

    # Build report
    report = {
        'period':       f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe':    TIMEFRAME,
        'capital':      CAPITAL,
        'leverage':     LEVERAGE,
        'margin_pct':   MARGIN_PCT * 100,
        'fee':          FEE,
        'slip':         SLIP,
        'symbols_attempted': len(set(all_symbols)),
        'symbols_with_data': len(set(with_data)),
        'total_elapsed_s':   round(total_elapsed, 1),
        'variants':     {
            vk: {
                'config': VARIANTS[vk],
                'stats':  var_stats[vk],
            }
            for vk in VARIANTS
        },
    }

    with open('backtest_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    # Build summary text
    lines = []
    lines.append("=" * 70)
    lines.append("G MAX — 6-VARIANT BACKTEST SUMMARY")
    lines.append("=" * 70)
    lines.append(f"Period   : {report['period']}")
    lines.append(f"Timeframe: {TIMEFRAME}  |  Leverage: {LEVERAGE}x  |  Margin: {MARGIN_PCT*100:.0f}% equity/trade")
    lines.append(f"Capital  : ${CAPITAL:,.0f}  |  Fee: {FEE*100:.3f}%/side  |  Slip: {SLIP*100:.3f}%/side")
    lines.append(f"Coins    : {len(set(with_data))} with data / {len(set(all_symbols))} attempted")
    lines.append("")

    lines.append("-" * 70)
    lines.append(f"{'VAR':<5} {'NAME':<22} {'TP':>6} {'SL':>6} {'TRADES':>7} {'WR%':>7} {'PF':>7} {'NET PNL':>10} {'MAX DD':>10}")
    lines.append("-" * 70)

    for vk in VARIANTS:
        cfg = VARIANTS[vk]
        st  = var_stats[vk]
        tp_str = f"{cfg['tp']*100:.1f}%" if cfg.get('tp') else 'ATR×2.5'
        sl_str = f"{cfg['sl']*100:.1f}%"
        verdict = '✅' if st['profit_factor'] >= 1.5 and st['win_rate'] >= 42 else '❌'
        lines.append(
            f"{vk:<5} {cfg['name']:<22} {tp_str:>6} {sl_str:>6} "
            f"{st['total']:>7} {st['win_rate']:>7.1f} {st['profit_factor']:>7.4f} "
            f"${st['net_pnl']:>9,.2f} ${st['max_drawdown']:>9,.2f}  {verdict}"
        )

    lines.append("-" * 70)
    lines.append("✅ = PF>=1.5 AND WR>=42%  |  ❌ = NOT USABLE")
    lines.append("")

    # Per-variant detail
    for vk in VARIANTS:
        cfg = VARIANTS[vk]
        st  = var_stats[vk]
        lines.append("=" * 70)
        lines.append(f"VARIANT {vk} — {cfg['name'].upper()}")
        lines.append(f"  Trades: {st['total']} | WR: {st['win_rate']}% | PF: {st['profit_factor']} | PnL: ${st['net_pnl']:,.2f}")
        lines.append(f"  Avg Win: ${st['avg_win']:.2f} | Avg Loss: ${st['avg_loss']:.2f} | Expectancy: ${st['expectancy']:.2f}")
        lines.append(f"  Max Drawdown: ${st['max_drawdown']:,.2f} | Longs: {st['longs']} | Shorts: {st['shorts']}")
        lines.append("")

        # Top 30 coins by PnL
        coin_rows = sorted(st['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
        lines.append(f"  {'COIN':<22} {'TRADES':>7} {'WR%':>7} {'PNL':>10}")
        lines.append(f"  {'-'*50}")
        for sym, d in coin_rows[:30]:
            lines.append(f"  {sym:<22} {d['n']:>7} {d['wr']:>7.1f} ${d['pnl']:>9,.2f}")

        # Monthly PnL
        lines.append("")
        lines.append("  Monthly PnL:")
        for month in sorted(st['monthly'].keys()):
            m = st['monthly'][month]
            wr_m = round(m['w']/m['n']*100, 1) if m['n'] else 0.0
            lines.append(f"    {month}: ${m['pnl']:>8,.2f}  ({m['n']} trades, WR {wr_m}%)")
        lines.append("")

    # Best coins appearing in 3+ variants with PF>1
    lines.append("=" * 70)
    lines.append("CROSS-VARIANT TOP COINS (positive in 3+ variants):")
    coin_scores = {}
    for vk in VARIANTS:
        for sym, d in var_stats[vk]['per_coin'].items():
            if sym not in coin_scores:
                coin_scores[sym] = 0
            if d['pnl'] > 0:
                coin_scores[sym] += 1
    top_cross = sorted(coin_scores.items(), key=lambda x: x[1], reverse=True)
    for sym, count in top_cross[:20]:
        if count >= 3:
            lines.append(f"  {sym:<22} positive in {count}/6 variants")

    lines.append("")
    lines.append(f"Total wall time: ~{total_elapsed/60:.1f} min across {NUM_SHARDS} shards")
    lines.append("=" * 70)

    summary_text = '\n'.join(lines)
    with open('backtest_summary.txt', 'w') as f:
        f.write(summary_text)

    print(summary_text, flush=True)
    print("\nFiles written: backtest_report.json  backtest_summary.txt", flush=True)

# ── Entry point ────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == 'merge':
        merge_shards()
    else:
        run_shard(int(arg))

