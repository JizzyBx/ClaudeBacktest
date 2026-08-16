"""
KAMA + RSI Pullback Strategy Backtest — Multi-Timeframe
Timeframes: 30m, 1h, 2h, 4h
Output: ONE zip — backtest_results.zip containing:
  - backtest_report_30m.json
  - backtest_summary_30m.txt
  - backtest_report_1h.json
  - backtest_summary_1h.txt
  - backtest_report_2h.json
  - backtest_summary_2h.txt
  - backtest_report_4h.json
  - backtest_summary_4h.txt
Fix: merge file includes full trades list
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

# ── Coin List ─────────────────────────────────────────────────────────────────
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
NUM_SHARDS = 8
WORKERS    = 16
START_YM   = (2023, 8)
END_YM     = (2025, 7)

CAPITAL  = 1000.0
RISK_PCT = 0.01
LEVERAGE = 5.0
FEE      = 0.0005
SLIP     = 0.0002
TP_PCT   = 0.04
SL_PCT   = 0.02

KAMA_PERIOD = 10
KAMA_FAST   = 2
KAMA_SLOW   = 30
RSI_PERIOD  = 14

TF_CONFIGS = {
    "30m": {"max_bars": 48, "min_bars": 60},
    "1h":  {"max_bars": 24, "min_bars": 50},
    "2h":  {"max_bars": 12, "min_bars": 40},
    "4h":  {"max_bars": 6,  "min_bars": 30},
}

# Set at runtime
TIMEFRAME = None
MAX_BARS  = None
MIN_BARS  = None

# ── Data Fetch ────────────────────────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month):
    url = f"{BASE_URL}/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                rows = []
                for row in csv.reader(io.TextIOWrapper(f)):
                    if not row or not row[0].isdigit():
                        continue
                    ts = int(row[0])
                    if ts > 10**14:
                        ts //= 1000
                    rows.append((ts, float(row[1]), float(row[2]), float(row[3]), float(row[4])))
                return rows
    except Exception:
        return []

def fetch_symbol(symbol):
    all_candles = []
    y, m = START_YM
    ey, em = END_YM
    while (y, m) <= (ey, em):
        all_candles.extend(fetch_month(symbol, y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    seen = {}
    for c in all_candles:
        seen[c[0]] = c
    return sorted(seen.values(), key=lambda x: x[0])

# ── Indicators ────────────────────────────────────────────────────────────────
def calc_kama(closes):
    fast_sc = 2.0 / (KAMA_FAST + 1)
    slow_sc = 2.0 / (KAMA_SLOW + 1)
    n = len(closes)
    kama = [float('nan')] * n
    if n < KAMA_PERIOD:
        return kama
    kama[KAMA_PERIOD - 1] = closes[KAMA_PERIOD - 1]
    for i in range(KAMA_PERIOD, n):
        direction  = abs(closes[i] - closes[i - KAMA_PERIOD])
        volatility = sum(abs(closes[j] - closes[j-1]) for j in range(i - KAMA_PERIOD + 1, i + 1))
        er = direction / volatility if volatility != 0 else 0.0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama[i] = kama[i-1] + sc * (closes[i] - kama[i-1])
    return kama

def calc_rsi(closes):
    n = len(closes)
    rsi = [float('nan')] * n
    if n < RSI_PERIOD + 1:
        return rsi
    avg_g = sum(max(closes[i] - closes[i-1], 0) for i in range(1, RSI_PERIOD+1)) / RSI_PERIOD
    avg_l = sum(max(closes[i-1] - closes[i], 0) for i in range(1, RSI_PERIOD+1)) / RSI_PERIOD
    for i in range(RSI_PERIOD, n):
        if i > RSI_PERIOD:
            diff  = closes[i] - closes[i-1]
            avg_g = (avg_g * (RSI_PERIOD-1) + max(diff, 0))  / RSI_PERIOD
            avg_l = (avg_l * (RSI_PERIOD-1) + max(-diff, 0)) / RSI_PERIOD
        rsi[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    return rsi

# ── Signal ────────────────────────────────────────────────────────────────────
def signal(i, closes, highs, lows, kama, rsi):
    if i < 10:
        return None
    k, k3, r, r1 = kama[i], kama[i-3], rsi[i], rsi[i-1]
    if any(v != v for v in [k, k3, r, r1]):
        return None
    c = closes[i]
    kama_up = k > k3
    kama_dn = k < k3
    rsi_cross_up   = r >= 50.0 and r1 < 50.0
    rsi_cross_down = r <= 50.0 and r1 > 50.0
    look = 5
    long_pb  = any(lows[i-j]  <= kama[i-j] * 1.003 for j in range(1, look+1) if i-j >= 0 and kama[i-j] == kama[i-j])
    short_pb = any(highs[i-j] >= kama[i-j] * 0.997 for j in range(1, look+1) if i-j >= 0 and kama[i-j] == kama[i-j])
    if c > k and kama_up and rsi_cross_up   and long_pb:  return 'buy'
    if c < k and kama_dn and rsi_cross_down and short_pb: return 'sell'
    return None

# ── Backtest ──────────────────────────────────────────────────────────────────
def backtest(symbol, candles):
    if len(candles) < MIN_BARS:
        return []
    timestamps = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    kama = calc_kama(closes)
    rsi  = calc_rsi(closes)
    notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
    trades   = []
    position = None

    for i in range(MIN_BARS, len(candles) - 1):
        if position is None:
            sig = signal(i, closes, highs, lows, kama, rsi)
            if sig is None:
                continue
            ep = opens[i+1]
            if sig == 'buy':
                entry_p = ep * (1 + FEE + SLIP)
                sl_p    = entry_p * (1 - SL_PCT)
                tp_p    = entry_p * (1 + TP_PCT)
            else:
                entry_p = ep * (1 - FEE - SLIP)
                sl_p    = entry_p * (1 + SL_PCT)
                tp_p    = entry_p * (1 - TP_PCT)
            position = {'side': sig, 'entry_price': entry_p,
                        'entry_ts': timestamps[i+1], 'entry_i': i+1,
                        'sl': sl_p, 'tp': tp_p, 'notional': notional}
        else:
            side = position['side']
            hi, lo, cl = highs[i], lows[i], closes[i]
            bars = i - position['entry_i']
            reason = exit_p = None
            if side == 'buy':
                if lo <= position['sl']:   exit_p, reason = position['sl'], 'sl'
                elif hi >= position['tp']: exit_p, reason = position['tp'], 'tp'
            else:
                if hi >= position['sl']:   exit_p, reason = position['sl'], 'sl'
                elif lo <= position['tp']: exit_p, reason = position['tp'], 'tp'
            if reason is None and bars >= MAX_BARS:
                exit_p, reason = cl, 'max_hold'
            if reason:
                ep = position['entry_price']
                gross = (exit_p - ep) / ep if side == 'buy' else (ep - exit_p) / ep
                pnl   = position['notional'] * (gross - (FEE + SLIP) * 2) * LEVERAGE
                trades.append({'symbol': symbol, 'side': side,
                                'entry_ts': position['entry_ts'], 'exit_ts': timestamps[i],
                                'entry_price': ep, 'exit_price': exit_p,
                                'pnl': round(pnl, 4), 'reason': reason, 'bars': bars})
                position = None

    if position is not None:
        i  = len(candles) - 1
        ep = position['entry_price']
        cl = closes[i]
        side = position['side']
        gross = (cl - ep) / ep if side == 'buy' else (ep - cl) / ep
        pnl   = position['notional'] * (gross - (FEE + SLIP) * 2) * LEVERAGE
        trades.append({'symbol': symbol, 'side': side,
                        'entry_ts': position['entry_ts'], 'exit_ts': timestamps[i],
                        'entry_price': ep, 'exit_price': cl,
                        'pnl': round(pnl, 4), 'reason': 'end_of_data',
                        'bars': i - position['entry_i']})
    return trades

# ── Stats ─────────────────────────────────────────────────────────────────────
def stats(trades):
    if not trades:
        return {'total': 0, 'win_rate': 0, 'profit_factor': 0, 'net_pnl': 0,
                'max_drawdown': 0, 'avg_win': 0, 'avg_loss': 0, 'expectancy': 0,
                'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {}}
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    gw = sum(t['pnl'] for t in wins)
    gl = abs(sum(t['pnl'] for t in losses))
    net_pnl  = sum(t['pnl'] for t in trades)
    wr       = len(wins) / len(trades) * 100
    pf       = gw / gl if gl > 0 else float('inf')
    avg_win  = gw / len(wins)   if wins   else 0
    avg_loss = gl / len(losses) if losses else 0
    exp      = (wr/100 * avg_win) - ((1 - wr/100) * avg_loss)
    eq = peak = max_dd = 0
    for t in sorted(trades, key=lambda x: x['exit_ts']):
        eq += t['pnl']
        if eq > peak: peak = eq
        dd = peak - eq
        if dd > max_dd: max_dd = dd
    monthly = {}
    for t in trades:
        dt  = datetime.fromtimestamp(t['exit_ts']/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        if key not in monthly:
            monthly[key] = {'pnl': 0, 'n': 0, 'w': 0}
        monthly[key]['pnl'] += t['pnl']
        monthly[key]['n']   += 1
        monthly[key]['w']   += 1 if t['pnl'] > 0 else 0
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
    return {'total': len(trades), 'win_rate': round(wr, 2),
            'profit_factor': round(pf, 3), 'net_pnl': round(net_pnl, 2),
            'max_drawdown': round(max_dd, 2), 'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2), 'expectancy': round(exp, 2),
            'longs': sum(1 for t in trades if t['side'] == 'buy'),
            'shorts': sum(1 for t in trades if t['side'] == 'sell'),
            'monthly': monthly, 'per_coin': per_coin}

# ── Shard Runner ──────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    t0      = time.time()
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] TF={TIMEFRAME} | {len(symbols)} symbols", flush=True)
    candle_map = {}
    with_data  = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_symbol, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                candles = fut.result()
            except Exception as e:
                print(f"[Shard {shard_idx}] {sym} error: {e}", flush=True)
                candles = []
            if len(candles) >= MIN_BARS:
                candle_map[sym] = candles
                with_data.append(sym)
                print(f"[Shard {shard_idx}] {sym}: {len(candles)} candles", flush=True)
            else:
                print(f"[Shard {shard_idx}] {sym}: skip ({len(candles)})", flush=True)
    if not with_data:
        print(f"[Shard {shard_idx}] ERROR: 0 symbols returned data — possible geo-block!", flush=True)
    all_trades = []
    for sym in with_data:
        trades = backtest(sym, candle_map[sym])
        all_trades.extend(trades)
        print(f"[Shard {shard_idx}] {sym}: {len(trades)} trades", flush=True)
    s = stats(all_trades)
    with open(f"shard_{TIMEFRAME}_{shard_idx}.json", "w") as f:
        json.dump({'shard': shard_idx, 'timeframe': TIMEFRAME,
                   'symbols': symbols, 'with_data': with_data,
                   'trades': all_trades, 'stats': s,
                   'elapsed': round(time.time() - t0, 1)}, f)
    print(f"[Shard {shard_idx}] Done {time.time()-t0:.1f}s | trades={len(all_trades)} PF={s['profit_factor']}", flush=True)

# ── Merge ALL Timeframes into ONE zip ─────────────────────────────────────────
def build_summary(tf, s, symbols_with_data, symbols_total):
    usable  = s['profit_factor'] >= 1.5 and s['win_rate'] >= 42.0
    verdict = "✅ USABLE" if usable else "❌ NOT USABLE"
    coins_sorted = sorted(s['per_coin'].items(), key=lambda x: x[1]['pnl'], reverse=True)
    lines = [
        "=" * 60,
        f"KAMA + RSI PULLBACK — {tf.upper()} BACKTEST SUMMARY",
        "=" * 60,
        f"Period    : {START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        f"Timeframe : {tf}",
        f"Leverage  : {LEVERAGE}x",
        f"Capital   : ${CAPITAL:.0f}",
        f"TP / SL   : {TP_PCT*100:.1f}% / {SL_PCT*100:.1f}%",
        f"Symbols   : {symbols_with_data} with data / {symbols_total} attempted",
        "",
        "── AGGREGATE STATS ──────────────────────────────────",
        f"Total Trades    : {s['total']}",
        f"Win Rate        : {s['win_rate']:.2f}%",
        f"Profit Factor   : {s['profit_factor']:.3f}",
        f"Net PnL         : ${s['net_pnl']:.2f}",
        f"Max Drawdown    : ${s['max_drawdown']:.2f}",
        f"Avg Win         : ${s['avg_win']:.2f}",
        f"Avg Loss        : ${s['avg_loss']:.2f}",
        f"Expectancy      : ${s['expectancy']:.2f}",
        f"Longs / Shorts  : {s['longs']} / {s['shorts']}",
        "",
        f"RECOMMENDATION  : {verdict}",
        "",
        "── TOP 50 COINS BY NET PNL ──────────────────────────",
        f"{'Symbol':<22} {'Trades':>6} {'WR%':>6} {'PnL':>10}",
        "-" * 48,
    ]
    for sym, d in coins_sorted[:50]:
        lines.append(f"{sym:<22} {d['n']:>6} {d['wr']:>5.1f}% ${d['pnl']:>9.2f}")
    lines += ["", "── MONTHLY PNL ──────────────────────────────────────",
              f"{'Month':<10} {'Trades':>7} {'Wins':>5} {'PnL':>12}", "-" * 38]
    for month in sorted(s['monthly'].keys()):
        md = s['monthly'][month]
        lines.append(f"{month:<10} {md['n']:>7} {md['w']:>5} ${md['pnl']:>11.2f}")
    lines += ["", "=" * 60]
    return "\n".join(lines)

def merge_all():
    results = {}  # tf -> {trades, symbols, with_data}

    for tf in TF_CONFIGS.keys():
        all_trades    = []
        all_symbols   = []
        all_with_data = []
        for i in range(NUM_SHARDS):
            fname = f"shard_{tf}_{i}.json"
            if not os.path.exists(fname):
                print(f"WARNING: {fname} missing", flush=True)
                continue
            with open(fname) as f:
                d = json.load(f)
            all_trades.extend(d['trades'])
            all_symbols.extend(d['symbols'])
            all_with_data.extend(d['with_data'])
        results[tf] = {
            'trades':    all_trades,
            'symbols':   list(set(all_symbols)),
            'with_data': list(set(all_with_data)),
        }
        print(f"[Merge] {tf}: {len(all_trades)} trades from {len(set(all_with_data))} coins", flush=True)

    # Build one zip with all 8 files (4x json + 4x txt)
    with zipfile.ZipFile("backtest_results.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        for tf, data in results.items():
            s = stats(data['trades'])
            sw = len(data['with_data'])
            st = len(data['symbols'])

            report = {
                'strategy':          'KAMA + RSI Pullback',
                'timeframe':         tf,
                'period':            f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
                'leverage':          LEVERAGE,
                'capital':           CAPITAL,
                'tp_pct':            TP_PCT,
                'sl_pct':            SL_PCT,
                'symbols_total':     st,
                'symbols_with_data': sw,
                'recommendation':    'USABLE' if s['profit_factor'] >= 1.5 and s['win_rate'] >= 42.0 else 'NOT USABLE',
                'stats':             s,
                'trades':            data['trades'],   # ✅ full trades list
            }

            summary = build_summary(tf, s, sw, st)

            zf.writestr(f"backtest_report_{tf}.json", json.dumps(report, indent=2))
            zf.writestr(f"backtest_summary_{tf}.txt",  summary)

            print(f"[Merge] {tf} written — PF={s['profit_factor']} WR={s['win_rate']}%", flush=True)
            print(summary, flush=True)

    print("\n✅ Done: backtest_results.zip", flush=True)
    print("   Contains: backtest_report_30m.json, backtest_summary_30m.txt", flush=True)
    print("             backtest_report_1h.json,  backtest_summary_1h.txt",  flush=True)
    print("             backtest_report_2h.json,  backtest_summary_2h.txt",  flush=True)
    print("             backtest_report_4h.json,  backtest_summary_4h.txt",  flush=True)

# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python backtest.py <timeframe> <shard_index>   # run a shard")
        print("  python backtest.py merge                       # merge all TFs into 1 zip")
        print("  timeframes: 30m  1h  2h  4h")
        sys.exit(1)

    if sys.argv[1] == "merge":
        # merge doesn't need TF set — loops over all internally
        merge_all()
    else:
        tf_arg = sys.argv[1]
        if tf_arg not in TF_CONFIGS:
            print(f"ERROR: unknown timeframe '{tf_arg}'. Choose: {list(TF_CONFIGS.keys())}")
            sys.exit(1)
        if len(sys.argv) < 3:
            print("ERROR: shard index missing. e.g. python backtest.py 1h 0")
            sys.exit(1)
        TIMEFRAME = tf_arg
        MAX_BARS  = TF_CONFIGS[tf_arg]['max_bars']
        MIN_BARS  = TF_CONFIGS[tf_arg]['min_bars']
        run_shard(int(sys.argv[2]))

