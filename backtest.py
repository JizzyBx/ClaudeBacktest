"""
KAMA + RSI Pullback Strategy Backtest
Strategy: Kaufman Adaptive Moving Average direction + RSI 50-level momentum filter
Timeframe: 15m | Leverage: 5x | Period: 2023-08 to 2025-07
Coins: 96 from GMaxV1 universe (shorter history coins use whatever data exists)
"""

import sys
import json
import csv
import io
import zipfile
import urllib.request
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Coin List (96 coins from GMaxV1 COINS_UNIVERSE) ──────────────────────────
ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT','1000SATSUSDT',
    'A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT','ALGOUSDT','ALICEUSDT','ALPINEUSDT',
    'ARKMUSDT','ASRUSDT','ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT',
    'BELUSDT','BIDUSDT','BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT',
    'COMBOUSDT','CRCLUSDT','DAMUSDT','DEFIUSDT','DIAUSDT','DMCUSDT','ELSAUSDT',
    'ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT','FLNCUSDT','FLUXUSDT','FXSUSDT',
    'GLMUSDT','GRIFFAINUSDT','GUAUSDT','HANAUSDT','HEMIUSDT','ICXUSDT','INITUSDT',
    'IOUSDT','KITEUSDT','LABUSDT','LIGHTUSDT','LRCUSDT','LYNUSDT','MAGICUSDT',
    'MEGAUSDT','MILKUSDT','MOODENGUSDT','NFPUSDT','NMRUSDT','NOMUSDT','NOTUSDT',
    'OBOLUSDT','OPENUSDT','OPNUSDT','ORBSUSDT','PIXELUSDT','PLUMEUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','QUICKUSDT','RAVEUSDT','REEFUSDT',
    'RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT','SKRUSDT','SOMIUSDT',
    'SPELLUSDT','SPKUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT','UBUSDT','USUALUSDT',
    'VINEUSDT','VIRTUALUSDT','VVVUSDT','XEMUSDT','XRPUSDT','YBUSDT','ZECUSDT',
    'ZEREBROUSDT',
]

# ── Config ────────────────────────────────────────────────────────────────────
NUM_SHARDS  = 8
WORKERS     = 16
START_YM    = (2023, 8)   # Aug 2023
END_YM      = (2025, 7)   # Jul 2025 (2 years)
TIMEFRAME   = "15m"

CAPITAL     = 1000.0      # USD
RISK_PCT    = 0.01        # 1% risk per trade
LEVERAGE    = 5.0
FEE         = 0.0005      # 0.05% taker
SLIP        = 0.0002      # 0.02% slippage

# Strategy params
KAMA_PERIOD = 10          # KAMA efficiency ratio period (default)
KAMA_FAST   = 2           # fast EMA constant
KAMA_SLOW   = 30          # slow EMA constant
RSI_PERIOD  = 14

TP_PCT      = 0.04        # 4% take profit (price move before leverage)
SL_PCT      = 0.02        # 2% stop loss  (price move before leverage)
MAX_BARS    = 96          # max hold = 24h on 15m
MIN_BARS    = 60          # warmup bars needed

# ── Data Fetch ────────────────────────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month):
    url = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f))
                rows = []
                for row in reader:
                    if not row or not row[0].isdigit():
                        continue
                    ts = int(row[0])
                    if ts > 10**14:
                        ts //= 1000
                    o = float(row[1])
                    h = float(row[2])
                    l = float(row[3])
                    c = float(row[4])
                    rows.append((ts, o, h, l, c))
                return rows
    except Exception:
        return []

def fetch_symbol(symbol):
    all_candles = []
    y, m = START_YM
    ey, em = END_YM
    while (y, m) <= (ey, em):
        candles = fetch_month(symbol, y, m)
        all_candles.extend(candles)
        m += 1
        if m > 12:
            m = 1
            y += 1
    # deduplicate by timestamp, sort
    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    result = sorted(seen.values(), key=lambda x: x[0])
    return result

# ── Indicators (pure Python, no numpy) ───────────────────────────────────────

def calc_kama(closes, period=KAMA_PERIOD, fast=KAMA_FAST, slow=KAMA_SLOW):
    """
    Kaufman Adaptive Moving Average.
    Returns list of floats (same length as closes, nan for warmup).
    """
    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)
    n = len(closes)
    kama = [float('nan')] * n

    # first valid KAMA = close at index period-1
    if n < period:
        return kama

    kama[period - 1] = closes[period - 1]

    for i in range(period, n):
        direction = abs(closes[i] - closes[i - period])
        volatility = sum(abs(closes[j] - closes[j - 1]) for j in range(i - period + 1, i + 1))
        if volatility == 0:
            er = 0.0
        else:
            er = direction / volatility
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama[i] = kama[i - 1] + sc * (closes[i] - kama[i - 1])

    return kama

def calc_rsi(closes, period=RSI_PERIOD):
    """RSI via Wilder smoothing. Returns list same length as closes."""
    n = len(closes)
    rsi = [float('nan')] * n
    if n < period + 1:
        return rsi

    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    avg_g = sum(gains) / period
    avg_l = sum(losses) / period

    for i in range(period, n):
        if i == period:
            pass  # already calculated first avg
        else:
            diff = closes[i] - closes[i - 1]
            g = max(diff, 0.0)
            l = max(-diff, 0.0)
            avg_g = (avg_g * (period - 1) + g) / period
            avg_l = (avg_l * (period - 1) + l) / period

        if avg_l == 0:
            rsi[i] = 100.0
        else:
            rs = avg_g / avg_l
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)

    return rsi

# ── Signal ────────────────────────────────────────────────────────────────────
def signal(i, closes, highs, lows, kama, rsi):
    """
    KAMA + RSI Pullback Strategy:
    LONG:
      - close[i] > kama[i] and kama trending up (kama[i] > kama[i-3])
      - rsi[i] crossed back above 50 (rsi[i] >= 50 and rsi[i-1] < 50)
      - price was near/below kama recently (pullback occurred: any of last 5 lows touched kama)
    SHORT:
      - close[i] < kama[i] and kama trending down (kama[i] < kama[i-3])
      - rsi[i] crossed back below 50 (rsi[i] <= 50 and rsi[i-1] > 50)
      - price was near/above kama recently (pullback occurred: any of last 5 highs touched kama)
    """
    if i < 10:
        return None

    k  = kama[i]
    k3 = kama[i - 3]
    k1 = kama[i - 1]
    r  = rsi[i]
    r1 = rsi[i - 1]
    c  = closes[i]

    if any(v != v for v in [k, k3, k1, r, r1]):  # nan check
        return None

    kama_up   = k > k3
    kama_dn   = k < k3
    rsi_cross_up   = r >= 50.0 and r1 < 50.0
    rsi_cross_down = r <= 50.0 and r1 > 50.0

    # Pullback check: within last 5 bars did low touch kama (for long) or high touch kama (for short)
    look = 5
    long_pullback  = any(lows[max(0, i - j)] <= kama[i - j] * 1.003  for j in range(1, look + 1)
                         if i - j >= 0 and kama[i - j] == kama[i - j])
    short_pullback = any(highs[max(0, i - j)] >= kama[i - j] * 0.997 for j in range(1, look + 1)
                         if i - j >= 0 and kama[i - j] == kama[i - j])

    # Long: price above kama, kama trending up, pullback happened, RSI crosses back above 50
    if c > k and kama_up and rsi_cross_up and long_pullback:
        return 'buy'

    # Short: price below kama, kama trending down, pullback happened, RSI crosses back below 50
    if c < k and kama_dn and rsi_cross_down and short_pullback:
        return 'sell'

    return None

# ── Backtest Single Symbol ────────────────────────────────────────────────────
def backtest(symbol, candles):
    if len(candles) < MIN_BARS:
        return []

    timestamps = [c[0] for c in candles]
    opens      = [c[1] for c in candles]
    highs      = [c[2] for c in candles]
    lows       = [c[3] for c in candles]
    closes     = [c[4] for c in candles]

    kama = calc_kama(closes)
    rsi  = calc_rsi(closes)

    trades = []
    position = None  # {'side','entry_price','entry_ts','entry_i','notional','sl','tp'}

    notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)

    for i in range(MIN_BARS, len(candles) - 1):
        if position is None:
            sig = signal(i, closes, highs, lows, kama, rsi)
            if sig is None:
                continue

            ep = opens[i + 1]
            if sig == 'buy':
                entry_p = ep * (1 + FEE + SLIP)
                sl_p    = entry_p * (1 - SL_PCT)
                tp_p    = entry_p * (1 + TP_PCT)
            else:
                entry_p = ep * (1 - FEE - SLIP)
                sl_p    = entry_p * (1 + SL_PCT)
                tp_p    = entry_p * (1 - TP_PCT)

            position = {
                'side':       sig,
                'entry_price': entry_p,
                'entry_ts':   timestamps[i + 1],
                'entry_i':    i + 1,
                'sl':         sl_p,
                'tp':         tp_p,
                'notional':   notional,
            }
        else:
            side   = position['side']
            sl_p   = position['sl']
            tp_p   = position['tp']
            hi     = highs[i]
            lo     = lows[i]
            cl     = closes[i]
            bars   = i - position['entry_i']
            reason = None
            exit_p = None

            if side == 'buy':
                if lo <= sl_p:
                    exit_p = sl_p
                    reason = 'sl'
                elif hi >= tp_p:
                    exit_p = tp_p
                    reason = 'tp'
            else:
                if hi >= sl_p:
                    exit_p = sl_p
                    reason = 'sl'
                elif lo <= tp_p:
                    exit_p = tp_p
                    reason = 'tp'

            if reason is None and bars >= MAX_BARS:
                exit_p = cl
                reason = 'max_hold'

            if reason:
                ep = position['entry_price']
                if side == 'buy':
                    gross = (exit_p - ep) / ep
                else:
                    gross = (ep - exit_p) / ep
                net = gross - (FEE + SLIP) * 2
                pnl = position['notional'] * net * LEVERAGE

                trades.append({
                    'symbol':       symbol,
                    'side':         side,
                    'entry_ts':     position['entry_ts'],
                    'exit_ts':      timestamps[i],
                    'entry_price':  ep,
                    'exit_price':   exit_p,
                    'pnl':          round(pnl, 4),
                    'reason':       reason,
                    'bars':         bars,
                })
                position = None

    # Open position at end of data
    if position is not None:
        i = len(candles) - 1
        ep = position['entry_price']
        cl = closes[i]
        side = position['side']
        if side == 'buy':
            gross = (cl - ep) / ep
        else:
            gross = (ep - cl) / ep
        net = gross - (FEE + SLIP) * 2
        pnl = position['notional'] * net * LEVERAGE
        trades.append({
            'symbol':       symbol,
            'side':         side,
            'entry_ts':     position['entry_ts'],
            'exit_ts':      timestamps[i],
            'entry_price':  ep,
            'exit_price':   cl,
            'pnl':          round(pnl, 4),
            'reason':       'end_of_data',
            'bars':         i - position['entry_i'],
        })

    return trades

# ── Stats ─────────────────────────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0, 'profit_factor': 0, 'net_pnl': 0,
            'max_drawdown': 0, 'avg_win': 0, 'avg_loss': 0, 'expectancy': 0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }

    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gross_win  = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))

    net_pnl = sum(t['pnl'] for t in trades)
    win_rate = len(wins) / len(trades) * 100 if trades else 0
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
    avg_win  = gross_win  / len(wins)   if wins   else 0
    avg_loss = gross_loss / len(losses) if losses else 0
    expectancy = (win_rate / 100 * avg_win) - ((1 - win_rate / 100) * avg_loss)

    # Max drawdown (running equity curve)
    equity = 0
    peak = 0
    max_dd = 0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        equity += t['pnl']
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    longs  = sum(1 for t in trades if t['side'] == 'buy')
    shorts = sum(1 for t in trades if t['side'] == 'sell')

    # Monthly breakdown
    monthly = {}
    for t in trades:
        dt = datetime.fromtimestamp(t['exit_ts'] / 1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        if key not in monthly:
            monthly[key] = {'pnl': 0, 'n': 0, 'w': 0}
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        monthly[key]['w']   += 1 if t['pnl'] > 0 else 0

    # Per coin
    per_coin = {}
    for t in trades:
        sym = t['symbol']
        if sym not in per_coin:
            per_coin[sym] = {'pnl': 0, 'n': 0, 'w': 0, 'wr': 0}
        per_coin[sym]['pnl'] += t['pnl']
        per_coin[sym]['n']   += 1
        per_coin[sym]['w']   += 1 if t['pnl'] > 0 else 0
    for sym in per_coin:
        n = per_coin[sym]['n']
        per_coin[sym]['wr'] = round(per_coin[sym]['w'] / n * 100, 1) if n else 0

    return {
        'total':          len(trades),
        'win_rate':       round(win_rate, 2),
        'profit_factor':  round(pf, 3),
        'net_pnl':        round(net_pnl, 2),
        'max_drawdown':   round(max_dd, 2),
        'avg_win':        round(avg_win, 2),
        'avg_loss':       round(avg_loss, 2),
        'expectancy':     round(expectancy, 2),
        'longs':          longs,
        'shorts':         shorts,
        'monthly':        monthly,
        'per_coin':       per_coin,
    }

# ── Shard Runner ──────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    t0 = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] {len(symbols)} symbols: {symbols}", flush=True)

    all_candles = {}
    with_data   = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_symbol, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                candles = fut.result()
            except Exception as e:
                print(f"[Shard {shard_idx}] {sym} fetch error: {e}", flush=True)
                candles = []

            if len(candles) >= MIN_BARS:
                all_candles[sym] = candles
                with_data.append(sym)
                print(f"[Shard {shard_idx}] {sym}: {len(candles)} candles", flush=True)
            else:
                print(f"[Shard {shard_idx}] {sym}: insufficient data ({len(candles)} candles)", flush=True)

    if not with_data:
        print(f"[Shard {shard_idx}] ERROR: 0 symbols returned data — possible geo-block!", flush=True)

    all_trades = []
    for sym in with_data:
        trades = backtest(sym, all_candles[sym])
        all_trades.extend(trades)
        print(f"[Shard {shard_idx}] {sym}: {len(trades)} trades", flush=True)

    s = stats(all_trades)
    elapsed = time.time() - t0

    result = {
        'shard':     shard_idx,
        'symbols':   symbols,
        'with_data': with_data,
        'trades':    all_trades,
        'stats':     s,
        'elapsed':   round(elapsed, 1),
    }

    with open(f"shard_{shard_idx}.json", "w") as f:
        json.dump(result, f)

    print(f"[Shard {shard_idx}] Done in {elapsed:.1f}s — {len(all_trades)} trades, PF={s['profit_factor']}", flush=True)

# ── Merge ─────────────────────────────────────────────────────────────────────
def merge_shards():
    all_trades   = []
    all_symbols  = []
    all_with_data = []

    for i in range(NUM_SHARDS):
        fname = f"shard_{i}.json"
        if not os.path.exists(fname):
            print(f"WARNING: {fname} missing", flush=True)
            continue
        with open(fname) as f:
            d = json.load(f)
        all_trades.extend(d['trades'])
        all_symbols.extend(d['symbols'])
        all_with_data.extend(d['with_data'])

    s = stats(all_trades)

    report = {
        'strategy':     'KAMA + RSI Pullback',
        'timeframe':    TIMEFRAME,
        'period':       f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'leverage':     LEVERAGE,
        'capital':      CAPITAL,
        'tp_pct':       TP_PCT,
        'sl_pct':       SL_PCT,
        'symbols_total': len(set(all_symbols)),
        'symbols_with_data': len(set(all_with_data)),
        'stats':        s,
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Build summary text
    st = s
    usable = st['profit_factor'] >= 1.5 and st['win_rate'] >= 42.0
    verdict = "✅ USABLE" if usable else "❌ NOT USABLE"

    lines = [
        "=" * 60,
        "KAMA + RSI PULLBACK STRATEGY — BACKTEST SUMMARY",
        "=" * 60,
        f"Period    : {report['period']}",
        f"Timeframe : {TIMEFRAME}",
        f"Leverage  : {LEVERAGE}x",
        f"Capital   : ${CAPITAL:.0f}",
        f"TP / SL   : {TP_PCT*100:.1f}% / {SL_PCT*100:.1f}%",
        f"Symbols   : {report['symbols_with_data']} with data / {report['symbols_total']} attempted",
        "",
        "── AGGREGATE STATS ──────────────────────────────────",
        f"Total Trades    : {st['total']}",
        f"Win Rate        : {st['win_rate']:.2f}%",
        f"Profit Factor   : {st['profit_factor']:.3f}",
        f"Net PnL         : ${st['net_pnl']:.2f}",
        f"Max Drawdown    : ${st['max_drawdown']:.2f}",
        f"Avg Win         : ${st['avg_win']:.2f}",
        f"Avg Loss        : ${st['avg_loss']:.2f}",
        f"Expectancy      : ${st['expectancy']:.2f}",
        f"Longs / Shorts  : {st['longs']} / {st['shorts']}",
        "",
        f"RECOMMENDATION  : {verdict}",
        "",
    ]

    # Top 50 coins by PnL
    coins_sorted = sorted(st['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
    lines.append("── TOP 50 COINS BY NET PNL ──────────────────────────")
    lines.append(f"{'Symbol':<22} {'Trades':>6} {'WR%':>6} {'PnL':>10}")
    lines.append("-" * 48)
    for sym, d in coins_sorted[:50]:
        lines.append(f"{sym:<22} {d['n']:>6} {d['wr']:>5.1f}% ${d['pnl']:>9.2f}")

    # Monthly table
    lines.append("")
    lines.append("── MONTHLY PNL ──────────────────────────────────────")
    lines.append(f"{'Month':<10} {'Trades':>7} {'Wins':>5} {'PnL':>12}")
    lines.append("-" * 38)
    for month in sorted(st['monthly'].keys()):
        md = st['monthly'][month]
        lines.append(f"{month:<10} {md['n']:>7} {md['w']:>5} ${md['pnl']:>11.2f}")

    lines.append("")
    lines.append("=" * 60)

    summary = "\n".join(lines)
    with open("backtest_summary.txt", "w") as f:
        f.write(summary)

    print(summary, flush=True)
    print("\nFiles written: backtest_report.json, backtest_summary.txt", flush=True)

# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_index|merge>")
        sys.exit(1)

    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))

