"""
Backtest v8.3 — Wide SL Strategy | Variants G / H / I
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Base strategy : ADX>=22 + 50EMA slope + 9/21 EMA crossover (15m)
               + RSI-14 filter (long>=45, short<=55)

Variants:
  G : TP 3%  / SL 15%  — proven baseline (+$412, PF 1.023, DD 11%)
  H : TP 4%  / SL 15%  — higher reward, same breathing room
  I : TP 3%  / SL 15%  — G + extra quality filters (MACD + Volume + HTF)

Variant I extra filters:
  1. MACD histogram positive on long, negative on short
  2. Volume > 1.5x 20-bar average (real breakout, not chop)
  3. Higher timeframe trend: 50EMA slope over 40 bars (≈ 10h) must agree

Coins    : Full Binance USDT-M futures list (~159 symbols)
Period   : 2 years
Capital  : $10,000 | Risk 0.75%/trade | Max 5 positions
Fees     : 0.05% taker + 0.02% slippage per side
Workers  : 60 ProcessPoolExecutor
stdlib   : only

After run: cut weak coins, build final whitelist for live trading.
"""

import csv, io, json, math, os, time, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# ── Variants ──────────────────────────────────────────────────────────────
VARIANTS = {
    'G': {'tp': 0.03, 'sl': 0.15},
    'H': {'tp': 0.04, 'sl': 0.15},
    'I': {'tp': 0.03, 'sl': 0.15},   # same TP/SL as G but filtered entries
}

# ── Settings ──────────────────────────────────────────────────────────────
CAPITAL       = 10_000.0
RISK_PCT      = 0.0075
MAX_POSITIONS = 5
FEE_RATE      = 0.0005
SLIP_RATE     = 0.0002
ADX_MIN       = 22
SLOPE_THRESH  = 0.0005
COOLDOWN_BARS = 4
WORKERS       = 60
INTERVAL      = '15m'

# RSI filter
RSI_LONG_MIN  = 45
RSI_SHORT_MAX = 55
RSI_PERIOD    = 14

# Variant I extra filter settings
VOL_MULT      = 1.5    # volume must be > this x 20-bar avg
VOL_PERIOD    = 20
HTF_BARS      = 40     # 40 x 15m = 10 hours, pseudo-HTF slope window
MACD_FAST     = 12
MACD_SLOW     = 26
MACD_SIGNAL   = 9

_now = datetime.now(timezone.utc)
_em  = _now.month - 1 if _now.month > 1 else 12
_ey  = _now.year if _now.month > 1 else _now.year - 1
END_YEAR, END_MONTH = _ey, _em
START_YEAR  = END_YEAR - 2
START_MONTH = END_MONTH + 1 if END_MONTH < 12 else 1
if END_MONTH == 12:
    START_YEAR += 1

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ── Full Binance USDT-M Futures symbol list ────────────────────────────────
# Covers all major sectors. Symbols that don't exist on Binance futures
# will return 404 and be skipped automatically.
SYMBOLS = [
    # ── Anchors ──
    'BTCUSDT', 'ETHUSDT',

    # ── Layer 1 majors ──
    'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'AVAXUSDT',
    'DOTUSDT', 'ATOMUSDT', 'NEARUSDT', 'APTUSDT', 'SUIUSDT',
    'SEIUSDT', 'TIAUSDT', 'INJUSDT', 'FTMUSDT', 'ALGOUSDT',
    'ICPUSDT', 'HBARUSDT', 'XLMUSDT', 'TRXUSDT', 'LTCUSDT',
    'BCHUSDT', 'ETCUSDT', 'FILUSDT', 'EGLDUSDT', 'QNTUSDT',
    'FLOWUSDT', 'KASUSDT', 'TONUSDT', 'STXUSDT', 'THETAUSDT',
    'VETUSDT', 'XTZUSDT', 'EOSUSDT', 'NEOUSDT', 'ZILUSDT',
    'MINAUSDT', 'CFXUSDT', 'CKBUSDT', 'MOVRUSDT',

    # ── Layer 2 / Scaling ──
    'ARBUSDT', 'OPUSDT', 'MATICUSDT', 'IMXUSDT', 'STRKUSDT',
    'MANTAUSDT', 'ZETAUSDT', 'SCROLLUSDT', 'METISUSDT',
    'LRCUSDT', 'CELRUSDT',

    # ── DeFi ──
    'UNIUSDT', 'AAVEUSDT', 'LINKUSDT', 'MKRUSDT', 'CRVUSDT',
    'SNXUSDT', 'COMPUSDT', 'DYDXUSDT', 'GMXUSDT', 'JUPUSDT',
    'RUNEUSDT', 'LDOUSDT', 'RPLUSDT', 'CAKEUSDT', 'BALUSDT',
    'YFIUSDT', '1INCHUSDT', 'KAVAUSDT', 'BANDUSDT', 'SUSHIUSDT',
    'ONDOUSDT',

    # ── AI / Data ──
    'FETUSDT', 'RENDERUSDT', 'WLDUSDT', 'TAOUSDT', 'ARKMUSDT',
    'AGIXUSDT', 'OCEANUSDT', 'GRTUSDT', 'IOTXUSDT',

    # ── Meme (correct Binance futures naming) ──
    'DOGEUSDT', 'SHIBUSDT',
    '1000PEPEUSDT', '1000BONKUSDT', '1000SHIBUSDT', '1000FLOKIUSDT',
    '1000RATSUSDT', 'WIFUSDT', 'BOMEUSDT', 'NEIROUSDT', 'MEMEUSDT',
    'POPCATUSDT', 'MOGUSDT', 'ACTUSDT', 'DOGSUSDT',

    # ── Gaming / Metaverse ──
    'AXSUSDT', 'SANDUSDT', 'MANAUSDT', 'GALAUSDT', 'ENJUSDT',
    'YGGUSDT', 'GMTUSDT', 'ALICEUSDT',

    # ── Infrastructure / Storage ──
    'ARUSDT', 'STORJUSDT', 'BLURUSDT',

    # ── New / RWA / Recent listings ──
    'PYTHUSDT', 'EIGENUSDT', 'ENAUSDT', 'ETHFIUSDT',
    'NOTUSDT', 'IOUSDT', 'ZROUSDT', 'ZKUSDT',
    'LISTAUSDT', 'PENUSDT',

    # ── Political / viral ──
    'TRUMPUSDT', 'MELANIAUSDT',

    # ── Exchange tokens ──
    'OKBUSDT',

    # ── Privacy / Other established ──
    'XMRUSDT', 'ZECUSDT', 'DASHUSDT', 'BATUSDT', 'CHZUSDT',
    'ANKRUSDT', 'SKLUSDT', 'LITUSDT', 'IDUSDT', 'MAVUSDT',
    'CYBERUSDT', 'ARKUSDT', 'BEAMXUSDT',
]

# Deduplicate while preserving order
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ── Month iterator ─────────────────────────────────────────────────────────
def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

# ── Data fetching ──────────────────────────────────────────────────────────
def fetch_month(symbol, interval, year, month):
    fn  = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{symbol}/{interval}/{fn}"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            with z.open(z.namelist()[0]) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception:
        return None

def fetch_symbol_data(symbol):
    bars = []
    months_ok = 0
    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        rows = fetch_month(symbol, INTERVAL, y, m)
        if rows is None:
            continue
        months_ok += 1
        for row in rows:
            if not row or row[0] == 'open_time':
                continue
            try:
                ts = int(row[0])
                if ts > 10**14:
                    ts //= 1000
                bars.append((
                    ts,
                    float(row[1]),  # open
                    float(row[2]),  # high
                    float(row[3]),  # low
                    float(row[4]),  # close
                    float(row[5]),  # volume
                ))
            except (ValueError, IndexError):
                continue
    bars.sort(key=lambda x: x[0])
    return bars, months_ok

# ── Indicators ─────────────────────────────────────────────────────────────
def ema_series(closes, period):
    k = 2.0 / (period + 1)
    r = [closes[0]]
    for v in closes[1:]:
        r.append(v * k + r[-1] * (1 - k))
    return r

def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 2:
        return [None]*n, [None]*n, [None]*n
    pdm, mdm, trs = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down and up > 0   else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(highs[i]-lows[i],
                       abs(highs[i]-closes[i-1]),
                       abs(lows[i]-closes[i-1])))
    def ws(v, p):
        if len(v) < p: return []
        s = [sum(v[:p])]
        for x in v[p:]: s.append(s[-1] - s[-1]/p + x)
        return s
    st = ws(trs, period); sp = ws(pdm, period); sm = ws(mdm, period)
    if not st:
        return [None]*n, [None]*n, [None]*n
    pdi = [100*p/t if t else 0 for p,t in zip(sp,st)]
    mdi = [100*m/t if t else 0 for m,t in zip(sm,st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi,mdi)]
    if len(dx) < period:
        return [None]*n, [None]*n, [None]*n
    adx_v = [sum(dx[:period])/period]
    for d in dx[period:]: adx_v.append((adx_v[-1]*(period-1)+d)/period)
    pad_adx = [None]*(n - len(adx_v))
    pad_di  = [None]*(n - len(pdi) - 1)
    return (pad_adx + adx_v,
            [None] + pad_di + pdi,
            [None] + pad_di + mdi)

def rsi_series(closes, period=14):
    """Wilder RSI — matches TradingView default."""
    n = len(closes)
    if n < period + 1:
        return [None] * n
    gains, losses = [], []
    for i in range(1, n):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rsi = [None] * period
    rsi.append(100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l))
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rsi.append(100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l))
    return rsi

def macd_histogram(closes, fast=12, slow=26, signal=9):
    """Returns MACD histogram series (None-padded to len(closes))."""
    n = len(closes)
    if n < slow + signal:
        return [None] * n
    ema_f = ema_series(closes, fast)
    ema_s = ema_series(closes, slow)
    macd_line = [f - s for f, s in zip(ema_f, ema_s)]
    # signal line is EMA of macd_line starting from index (slow-1)
    sig_line = ema_series(macd_line[slow-1:], signal)
    # pad front
    pad = slow - 1 + signal - 1
    hist_raw = [m - s for m, s in zip(macd_line[slow-1+signal-1:],
                                        sig_line[signal-1:])]
    return [None] * (n - len(hist_raw)) + hist_raw

# ── Signal generation ──────────────────────────────────────────────────────
def compute_signals(bars, variant='G'):
    """
    variant G/H : ADX + EMA slope + 9/21 cross + RSI
    variant I   : all of above + MACD hist + Volume spike + HTF slope
    """
    closes  = [b[4] for b in bars]
    highs   = [b[2] for b in bars]
    lows    = [b[3] for b in bars]
    volumes = [b[5] for b in bars]
    n       = len(closes)
    warmup  = 100   # enough for all indicators to warm up
    if n < warmup:
        return []

    e9  = ema_series(closes, 9)
    e21 = ema_series(closes, 21)
    e50 = ema_series(closes, 50)
    adx_arr, _, _ = adx_series(highs, lows, closes, 14)
    rsi_arr = rsi_series(closes, RSI_PERIOD)

    # Variant I extras
    if variant == 'I':
        macd_hist = macd_histogram(closes, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    else:
        macd_hist = None

    signals = []
    for i in range(warmup, n):
        if adx_arr[i] is None or rsi_arr[i] is None:
            signals.append({'i': i, 'signal': None}); continue
        if adx_arr[i] < ADX_MIN:
            signals.append({'i': i, 'signal': None}); continue

        slope = (e50[i] - e50[i-10]) / e50[i-10]
        trend_up   = slope >  SLOPE_THRESH
        trend_down = slope < -SLOPE_THRESH
        if not trend_up and not trend_down:
            signals.append({'i': i, 'signal': None}); continue

        cross_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
        cross_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

        rsi_val = rsi_arr[i]
        sig = None

        if trend_up   and cross_up   and rsi_val >= RSI_LONG_MIN:
            sig = 'long'
        if trend_down and cross_down and rsi_val <= RSI_SHORT_MAX:
            sig = 'short'

        # ── Variant I: extra quality filters ──────────────────────────
        if sig and variant == 'I':

            # 1. MACD histogram confirmation
            if macd_hist[i] is None:
                sig = None
            elif sig == 'long'  and macd_hist[i] <= 0:
                sig = None
            elif sig == 'short' and macd_hist[i] >= 0:
                sig = None

            # 2. Volume spike: current bar > VOL_MULT x 20-bar avg
            if sig and i >= VOL_PERIOD:
                avg_vol = sum(volumes[i-VOL_PERIOD:i]) / VOL_PERIOD
                if avg_vol > 0 and volumes[i] < VOL_MULT * avg_vol:
                    sig = None

            # 3. Higher-timeframe trend: 50EMA slope over HTF_BARS
            #    Must agree with trade direction
            if sig and i >= HTF_BARS:
                htf_slope = (e50[i] - e50[i - HTF_BARS]) / e50[i - HTF_BARS]
                if sig == 'long'  and htf_slope <= 0:
                    sig = None
                if sig == 'short' and htf_slope >= 0:
                    sig = None

        signals.append({'i': i, 'signal': sig})
    return signals

# ── Backtest single coin × single variant ──────────────────────────────────
def backtest_coin_variant(symbol, bars, vname, tp_pct, sl_pct):
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    n      = len(bars)

    signals = compute_signals(bars, variant=vname)
    if not signals:
        return []

    trades       = []
    in_pos       = False
    entry_price  = 0.0
    entry_i      = 0
    pos_side     = None
    last_close_i = -999

    for sig in signals:
        bar_i = sig['i']
        if bar_i >= n:
            break

        if in_pos:
            h, l = highs[bar_i], lows[bar_i]
            hit_tp = hit_sl = False
            exit_px = 0.0

            if pos_side == 'long':
                tp_px = entry_price * (1 + tp_pct)
                sl_px = entry_price * (1 - sl_pct)
                if l <= sl_px:   hit_sl = True; exit_px = sl_px
                elif h >= tp_px: hit_tp = True; exit_px = tp_px
            else:
                tp_px = entry_price * (1 - tp_pct)
                sl_px = entry_price * (1 + sl_pct)
                if h >= sl_px:   hit_sl = True; exit_px = sl_px
                elif l <= tp_px: hit_tp = True; exit_px = tp_px

            if hit_tp or hit_sl:
                gross = ((exit_px - entry_price) / entry_price
                         if pos_side == 'long'
                         else (entry_price - exit_px) / entry_price)
                net  = gross - (FEE_RATE + SLIP_RATE) * 2
                size = (CAPITAL * RISK_PCT) / sl_pct
                trades.append({
                    'symbol'   : symbol,
                    'variant'  : vname,
                    'side'     : pos_side,
                    'entry_ts' : bars[entry_i][0],
                    'exit_ts'  : bars[bar_i][0],
                    'entry_px' : round(entry_price, 8),
                    'exit_px'  : round(exit_px, 8),
                    'result'   : 'tp' if hit_tp else 'sl',
                    'pnl_pct'  : round(net * 100, 4),
                    'pnl_usd'  : round(size * net, 4),
                    'bars_held': bar_i - entry_i,
                })
                in_pos       = False
                last_close_i = bar_i

        if not in_pos and sig['signal'] and (bar_i - last_close_i >= COOLDOWN_BARS):
            in_pos      = True
            entry_price = closes[bar_i]
            entry_i     = bar_i
            pos_side    = sig['signal']

    return trades

# ── Worker ─────────────────────────────────────────────────────────────────
def worker_task(symbol):
    try:
        bars, months = fetch_symbol_data(symbol)
        if len(bars) < 300:
            return symbol, None, f"insufficient ({len(bars)} bars)"
        result = {}
        for vname, vcfg in VARIANTS.items():
            result[vname] = backtest_coin_variant(
                symbol, bars, vname, vcfg['tp'], vcfg['sl'])
        return symbol, result, f"ok ({months}mo, {len(bars)}bars)"
    except Exception as e:
        return symbol, None, f"error: {e}"

# ── Portfolio cap ──────────────────────────────────────────────────────────
def apply_portfolio_cap(results_raw, variant_name):
    all_trades = []
    for sym, variant_map in results_raw.items():
        all_trades.extend(variant_map.get(variant_name, []))
    all_trades.sort(key=lambda t: t['entry_ts'])
    active = []; accepted = []
    for t in all_trades:
        active = [ex for ex in active if ex > t['entry_ts']]
        if len(active) < MAX_POSITIONS:
            active.append(t['exit_ts'])
            accepted.append(t)
    return accepted

# ── Stats ──────────────────────────────────────────────────────────────────
def calc_stats(trades):
    if not trades:
        return None
    wins   = [t for t in trades if t['pnl_usd'] > 0]
    losses = [t for t in trades if t['pnl_usd'] <= 0]
    total  = len(trades)
    wr     = len(wins) / total * 100
    gp     = sum(t['pnl_usd'] for t in wins)
    gl     = abs(sum(t['pnl_usd'] for t in losses))
    pf     = gp / gl if gl > 0 else float('inf')
    net    = sum(t['pnl_usd'] for t in trades)

    longs  = [t for t in trades if t['side'] == 'long']
    shorts = [t for t in trades if t['side'] == 'short']
    lw = len([t for t in longs  if t['pnl_usd'] > 0])
    sw = len([t for t in shorts if t['pnl_usd'] > 0])

    equity = CAPITAL; peak = CAPITAL; max_dd = 0.0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        equity += t['pnl_usd']
        if equity > peak: peak = equity
        dd = (peak - equity) / peak * 100
        if dd > max_dd: max_dd = dd

    avg_win  = gp / len(wins)   if wins   else 0
    avg_loss = gl / len(losses) if losses else 0
    exp      = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)

    rets   = [t['pnl_usd'] for t in trades]
    mean_r = sum(rets) / len(rets)
    std_r  = math.sqrt(sum((r-mean_r)**2 for r in rets)/len(rets)) if len(rets)>1 else 0
    sharpe = (mean_r / std_r) * math.sqrt(252*6.5*4) if std_r > 0 else 0

    return {
        'total_trades' : total,
        'win_rate'     : round(wr, 2),
        'profit_factor': round(pf, 3),
        'net_pnl'      : round(net, 2),
        'max_drawdown' : round(max_dd, 2),
        'sharpe'       : round(sharpe, 3),
        'avg_win'      : round(avg_win, 2),
        'avg_loss'     : round(avg_loss, 2),
        'expectancy'   : round(exp, 2),
        'avg_bars_held': round(sum(t['bars_held'] for t in trades)/total, 1),
        'long_trades'  : len(longs),
        'short_trades' : len(shorts),
        'long_wr'      : round(lw/len(longs)*100,  2) if longs  else 0,
        'short_wr'     : round(sw/len(shorts)*100, 2) if shorts else 0,
        'gross_profit' : round(gp, 2),
        'gross_loss'   : round(gl, 2),
    }

def monthly_pnl(trades):
    buckets = {}
    for t in trades:
        dt  = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        buckets[key] = buckets.get(key, 0) + t['pnl_usd']
    return {k: round(v, 2) for k, v in sorted(buckets.items())}

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("="*65)
    print("  Backtest v8.3 — G / H / I Variants | Full Binance Futures")
    print("="*65)
    print(f"Period    : {START_YEAR}-{START_MONTH:02d} -> {END_YEAR}-{END_MONTH:02d}")
    print(f"Symbols   : {len(SYMBOLS)}")
    print(f"Variants  : G (baseline) | H (higher TP) | I (G + MACD+Vol+HTF)")
    print(f"Max pos   : {MAX_POSITIONS}")
    print()

    results_raw  = {}
    symbol_notes = {}
    done = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker_task, sym): sym for sym in SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                symbol, result, note = fut.result()
                symbol_notes[symbol] = note
                if result is not None:
                    results_raw[symbol] = result
            except Exception as e:
                symbol_notes[sym] = f"exception: {e}"
            done += 1
            if done % 10 == 0 or done == len(SYMBOLS):
                print(f"  [{done}/{len(SYMBOLS)}] done...")

    print(f"\n[INFO] Fetch done in {time.time()-t0:.1f}s")
    print(f"[INFO] Symbols with data: {len(results_raw)}/{len(SYMBOLS)}\n")

    summary_lines = []
    report = {
        'meta': {
            'version'       : 'v8.3',
            'period'        : f"{START_YEAR}-{START_MONTH:02d} -> {END_YEAR}-{END_MONTH:02d}",
            'symbols_tested': len(results_raw),
            'variants'      : VARIANTS,
            'settings': {
                'capital'      : CAPITAL,
                'risk_pct'     : RISK_PCT,
                'fee'          : FEE_RATE,
                'slip'         : SLIP_RATE,
                'adx_min'      : ADX_MIN,
                'max_positions': MAX_POSITIONS,
                'rsi_long_min' : RSI_LONG_MIN,
                'rsi_short_max': RSI_SHORT_MAX,
                'variant_I_filters': {
                    'macd'  : f'{MACD_FAST}/{MACD_SLOW}/{MACD_SIGNAL} histogram confirm',
                    'volume': f'>{VOL_MULT}x {VOL_PERIOD}-bar avg',
                    'htf'   : f'{HTF_BARS}-bar (~{HTF_BARS*15//60}h) slope confirm',
                }
            }
        },
        'variants': {},
        'symbol_notes': symbol_notes,
    }

    for vname, vcfg in VARIANTS.items():
        tp_pct = vcfg['tp'] * 100
        sl_pct = vcfg['sl'] * 100

        capped = apply_portfolio_cap(results_raw, vname)
        stats  = calc_stats(capped)

        if not stats:
            summary_lines.append(f"\n[Variant {vname}] No trades.")
            continue

        monthly = monthly_pnl(capped)

        # Per-coin stats
        coin_stats = {}
        capped_ts  = {(t['symbol'], t['entry_ts']) for t in capped}
        for sym, vmap in results_raw.items():
            filtered = [t for t in vmap.get(vname, [])
                        if (t['symbol'], t['entry_ts']) in capped_ts]
            if filtered:
                cs = calc_stats(filtered)
                if cs:
                    coin_stats[sym] = cs

        coin_sorted = sorted(coin_stats.items(),
                             key=lambda x: x[1]['profit_factor'], reverse=True)

        usable  = stats['profit_factor'] >= 1.5 and stats['win_rate'] >= 42
        verdict = "USABLE ✓" if usable else ("CLOSE" if stats['profit_factor'] >= 1.0 else "NOT YET")

        lines = []
        lines.append(f"\n{'='*65}")
        lines.append(f"  VARIANT {vname} — TP {tp_pct:.0f}% / SL {sl_pct:.0f}%  [{verdict}]")
        if vname == 'I':
            lines.append(f"  Filters: MACD hist + Volume>{VOL_MULT}x avg + {HTF_BARS*15//60}h HTF slope")
        lines.append(f"{'='*65}")
        lines.append(f"  Trades       : {stats['total_trades']}")
        lines.append(f"  Win Rate     : {stats['win_rate']}%")
        lines.append(f"  Profit Factor: {stats['profit_factor']}")
        lines.append(f"  Net PnL      : ${stats['net_pnl']}")
        lines.append(f"  Max Drawdown : {stats['max_drawdown']}%")
        lines.append(f"  Sharpe       : {stats['sharpe']}")
        lines.append(f"  Expectancy   : ${stats['expectancy']}/trade")
        lines.append(f"  Avg Bars Held: {stats['avg_bars_held']}")
        lines.append(f"  Long  : {stats['long_trades']} trades | WR {stats['long_wr']}%")
        lines.append(f"  Short : {stats['short_trades']} trades | WR {stats['short_wr']}%")
        lines.append(f"  Gross Profit : ${stats['gross_profit']} | Loss: ${stats['gross_loss']}")

        lines.append(f"\n  -- Top 25 by Profit Factor --")
        lines.append(f"  {'Coin':<22} {'Trades':>6} {'WR%':>7} {'PF':>7} {'NetPnL':>10}")
        lines.append(f"  {'-'*57}")
        for sym, cs in coin_sorted[:25]:
            lines.append(f"  {sym:<22} {cs['total_trades']:>6} "
                         f"{cs['win_rate']:>6.1f}% {cs['profit_factor']:>7.3f} "
                         f"${cs['net_pnl']:>9.2f}")

        lines.append(f"\n  -- Bottom 10 --")
        for sym, cs in coin_sorted[-10:]:
            lines.append(f"  {sym:<22} {cs['total_trades']:>6} "
                         f"{cs['win_rate']:>6.1f}% {cs['profit_factor']:>7.3f} "
                         f"${cs['net_pnl']:>9.2f}")

        lines.append(f"\n  -- Monthly PnL --")
        for mo, pnl in monthly.items():
            bar  = '#' * min(int(abs(pnl)/15), 50)
            sign = '+' if pnl >= 0 else ''
            lines.append(f"  {mo}  {sign}${pnl:>8.2f}  {bar}")

        good = [(s, c) for s, c in coin_sorted
                if c['profit_factor'] >= 1.5 and c['win_rate'] >= 42
                and c['total_trades'] >= 8]
        lines.append(f"\n  -- Passing Coins (PF>=1.5 WR>=42% >=8 trades): {len(good)} --")
        for sym, cs in good:
            lines.append(f"  OK {sym:<22} PF={cs['profit_factor']:.3f} "
                         f"WR={cs['win_rate']:.1f}% T={cs['total_trades']}")

        # Trades per day estimate
        days = 730
        tpd = stats['total_trades'] / days
        lines.append(f"\n  Trades/day (portfolio): ~{tpd:.1f}")

        block = '\n'.join(lines)
        summary_lines.append(block)
        print(block)

        report['variants'][vname] = {
            'config'     : vcfg,
            'aggregate'  : stats,
            'monthly_pnl': monthly,
            'per_coin'   : {s: c for s, c in coin_sorted},
            'good_coins' : [s for s, _ in good],
            'verdict'    : verdict,
            'trades_per_day': round(tpd, 2),
            'trades'     : capped[-500:],
        }

    # ── Cross-variant summary ──────────────────────────────────────────────
    summary_lines.append(f"\n{'='*65}")
    summary_lines.append(f"  CROSS-VARIANT SUMMARY")
    summary_lines.append(f"{'='*65}")
    summary_lines.append(f"  {'Var':<4} {'TP':>5} {'SL':>5} {'Trades':>7} {'T/day':>6} "
                         f"{'WR%':>7} {'PF':>7} {'NetPnL':>12} {'MaxDD':>8} {'Verdict'}")
    summary_lines.append(f"  {'-'*78}")
    for vname, vdata in report['variants'].items():
        cfg = vdata['config']
        st  = vdata['aggregate']
        summary_lines.append(
            f"  {vname:<4} {cfg['tp']*100:>4.0f}% {cfg['sl']*100:>4.0f}% "
            f"{st['total_trades']:>7} {vdata['trades_per_day']:>6.1f} "
            f"{st['win_rate']:>6.1f}% {st['profit_factor']:>7.3f} "
            f"${st['net_pnl']:>10.2f} {st['max_drawdown']:>6.1f}%  {vdata['verdict']}"
        )

    # ── Whitelist recommendation ───────────────────────────────────────────
    # Coins that pass in ALL of G, H, I = strongest candidates
    summary_lines.append(f"\n{'='*65}")
    summary_lines.append(f"  WHITELIST RECOMMENDATION (passing G + H + I)")
    summary_lines.append(f"{'='*65}")
    all_good = {}
    for vname, vdata in report['variants'].items():
        for sym in vdata['good_coins']:
            all_good[sym] = all_good.get(sym, 0) + 1
    triple = sorted([s for s, c in all_good.items() if c == 3])
    double = sorted([s for s, c in all_good.items() if c == 2])
    summary_lines.append(f"  Passes all 3 variants ({len(triple)} coins): {', '.join(triple)}")
    summary_lines.append(f"  Passes 2/3 variants  ({len(double)} coins): {', '.join(double)}")

    # Symbol notes
    issues = [(s, n) for s, n in symbol_notes.items()
              if 'error' in n or 'insufficient' in n]
    summary_lines.append(f"\n-- Symbol Issues ({len(issues)}) --")
    for s, n in issues[:40]:
        summary_lines.append(f"  {s}: {n}")

    elapsed = time.time() - t0
    summary_lines.append(f"\nDone in {elapsed:.1f}s")
    print(f"\nDone in {elapsed:.1f}s")

    out = Path(os.environ.get('GITHUB_WORKSPACE', '.'))
    (out / 'backtest_summary.txt').write_text('\n'.join(summary_lines))
    (out / 'backtest_report.json').write_text(
        json.dumps(report, indent=2, default=str))
    print("[OUT] backtest_summary.txt + backtest_report.json written")

if __name__ == '__main__':
    main()

