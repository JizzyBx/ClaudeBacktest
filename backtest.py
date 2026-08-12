"""
Liquidity Wall Absorption Strategy — Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OHLC-only approximation of the "Liquidity Walls [TradingIQ]" concept.

NOTE ON FIDELITY:
The original TradingView indicator uses lower-timeframe (1s/1-tick) signed
volume / delta reconstruction to detect "absorption" (strong delta, weak
price response). Binance's public kline archives only provide OHLCV per
candle -- no trade-level/tick data -- so true delta cannot be reconstructed
here. This backtest instead uses an OHLC-only proxy for the same idea:

    "effort vs result" proxy:
        - volume is elevated vs recent average (effort)
        - candle body is small relative to its range (weak result / rejection)
    -> flagged as an "inefficient" (absorption) candle
    -> the candle's high (if upper wick dominant / bearish absorption) or
       low (if lower wick dominant / bullish absorption) becomes a
       projected "liquidity wall" zone, active until price closes through it

    entry: price returns to an active zone and prints a rejection candle
    back in the direction away from the zone -> enter opposite the
    absorbed side (fade back into range)

This is NOT the TradingIQ delta model. It is a stated approximation built
because only kline data is available. Results should be read as "does the
wick-rejection/volume-spike proxy have edge" -- not as a validation of the
original indicator.

Coin list: GMaxV1 COINS_UNIVERSE (117 coins)
Data: Binance USDT-M futures monthly kline archives, stdlib only
"""

import csv, io, json, sys, time, zipfile, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ══════════════════════════════════════════════════════════════════
# Coin list (from GMaxV1.py COINS_UNIVERSE)
# ══════════════════════════════════════════════════════════════════
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

NUM_SHARDS = 8
WORKERS = 16

# ══════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════
END_YM   = (2026, 7)     # last full month before "now" (Aug 2026)
START_YM = (2025, 8)     # 1 year lookback
TIMEFRAME = "15m"

CAPITAL   = 1000.0
RISK_PCT  = 0.02          # 2% of capital risked per trade (pre-leverage sizing base)
FEE       = 0.0004        # 0.04% taker, one side
SLIP      = 0.0005        # 0.05% slippage, one side
LEVERAGE  = 5

TP_PCT   = 0.025          # 2.5%
SL_PCT   = 0.0125         # 1.25%  (2:1 R:R)
MAX_BARS = 48             # 12h on 15m
MIN_BARS = 100            # warmup for volume avg / zone tracking

# Strategy-specific params
VOL_LOOKBACK   = 20       # bars for average volume
VOL_MULT       = 1.5      # volume must be >= this x average to count as "effort"
BODY_RATIO_MAX = 0.35     # body/range must be <= this to count as "weak result"
ZONE_MAX_AGE   = 200      # bars a zone stays valid even if not touched (avoid infinite stale zones)

# ══════════════════════════════════════════════════════════════════
# Data fetch
# ══════════════════════════════════════════════════════════════════
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{tf}/{symbol}-{tf}-{year}-{month:02d}.zip"

def month_range(start_ym, end_ym):
    y, m = start_ym
    out = []
    while (y, m) <= end_ym:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out

def fetch_month(symbol, year, month):
    url = BASE_URL.format(symbol=symbol, tf=TIMEFRAME, year=year, month=month)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        return []
    except Exception:
        return []

    out = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                reader = csv.reader(text)
                for row in reader:
                    if not row or row[0] in ("open_time", "opentime"):
                        continue
                    try:
                        ts = int(float(row[0]))
                        if ts > 10**14:
                            ts = ts // 1000
                        o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
                        v = float(row[5])
                        out.append((ts, o, h, l, c, v))
                    except (ValueError, IndexError):
                        continue
    except zipfile.BadZipFile:
        return []
    return out

def fetch_symbol(symbol):
    months = month_range(START_YM, END_YM)
    all_candles = []
    for (y, m) in months:
        rows = fetch_month(symbol, y, m)
        all_candles.extend(rows)
    dedup = {}
    for row in all_candles:
        dedup[row[0]] = row
    candles = [dedup[k] for k in sorted(dedup.keys())]
    return candles

# ══════════════════════════════════════════════════════════════════
# Strategy: Liquidity Wall (OHLC absorption proxy)
# ══════════════════════════════════════════════════════════════════

def avg_volume(volumes, i, lookback):
    lo = max(0, i - lookback)
    window = volumes[lo:i]
    if not window:
        return None
    return sum(window) / len(window)

def is_inefficient_candle(o, h, l, c, v, avg_vol):
    """Effort-vs-result proxy: elevated volume + small body relative to range."""
    if avg_vol is None or avg_vol <= 0:
        return None
    rng = h - l
    if rng <= 0:
        return None
    body = abs(c - o)
    body_ratio = body / rng
    if v < avg_vol * VOL_MULT:
        return None
    if body_ratio > BODY_RATIO_MAX:
        return None
    # Determine side of absorption via wick dominance
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    if upper_wick > lower_wick:
        # buying pushed up, got rejected -> wall ABOVE (bearish absorption)
        return ('above', h)
    else:
        # selling pushed down, got rejected -> wall BELOW (bullish absorption)
        return ('below', l)

def signal(i, opens, highs, lows, closes, volumes, active_zones):
    """
    Evaluated on closed bar i. Returns ('buy'|'sell'|None, updated_zones).
    active_zones: list of dicts {'side': 'above'|'below', 'price': float, 'born': i}
    Mutates and returns the zone list (caller owns state per-symbol).
    """
    o, h, l, c, v = opens[i], highs[i], lows[i], closes[i], volumes[i]
    av = avg_volume(volumes, i, VOL_LOOKBACK)

    # 1. Detect new absorption candle -> add zone
    result = is_inefficient_candle(o, h, l, c, v, av)
    if result:
        side, price = result
        active_zones.append({'side': side, 'price': price, 'born': i})

    # 2. Invalidate zones price has closed through
    still_active = []
    for z in active_zones:
        if i - z['born'] > ZONE_MAX_AGE:
            continue  # stale, drop
        if z['side'] == 'above' and c > z['price']:
            continue  # broken through upside -> invalidated
        if z['side'] == 'below' and c < z['price']:
            continue  # broken through downside -> invalidated
        still_active.append(z)
    active_zones[:] = still_active

    # 3. Check for a retest + rejection at an active zone (on THIS closed bar)
    sig = None
    for z in active_zones:
        if z['side'] == 'above':
            # price wicked up into the zone but closed back below it -> bearish rejection
            if h >= z['price'] and c < z['price'] and c < o:
                sig = 'sell'
                break
        else:
            # price wicked down into the zone but closed back above it -> bullish rejection
            if l <= z['price'] and c > z['price'] and c > o:
                sig = 'buy'
                break

    return sig, active_zones

# ══════════════════════════════════════════════════════════════════
# Backtest single symbol
# ══════════════════════════════════════════════════════════════════

def backtest(symbol, candles):
    trades = []
    if len(candles) < MIN_BARS + 5:
        return trades

    opens   = [r[1] for r in candles]
    highs   = [r[2] for r in candles]
    lows    = [r[3] for r in candles]
    closes  = [r[4] for r in candles]
    volumes = [r[5] for r in candles]
    ts      = [r[0] for r in candles]

    n = len(candles)
    active_zones = []
    in_position = False
    pos = None

    i = MIN_BARS
    while i < n - 1:
        if not in_position:
            sig, active_zones = signal(i, opens, highs, lows, closes, volumes, active_zones)
            if sig in ('buy', 'sell'):
                entry_idx = i + 1
                if entry_idx >= n:
                    break
                entry_open = opens[entry_idx]
                if sig == 'buy':
                    entry_p = entry_open * (1 + FEE + SLIP)
                    tp_p = entry_p * (1 + TP_PCT)
                    sl_p = entry_p * (1 - SL_PCT)
                else:
                    entry_p = entry_open * (1 - FEE - SLIP)
                    tp_p = entry_p * (1 - TP_PCT)
                    sl_p = entry_p * (1 + SL_PCT)
                in_position = True
                pos = {
                    'side': sig, 'entry_idx': entry_idx, 'entry_ts': ts[entry_idx],
                    'entry_price': entry_p, 'tp': tp_p, 'sl': sl_p, 'bars_held': 0,
                }
                i = entry_idx
                continue
            i += 1
            continue
        else:
            pos['bars_held'] += 1
            bar_i = i
            h, l, c = highs[bar_i], lows[bar_i], closes[bar_i]
            exit_p = None
            reason = None

            if pos['side'] == 'buy':
                if l <= pos['sl']:
                    exit_p, reason = pos['sl'], 'sl'
                elif h >= pos['tp']:
                    exit_p, reason = pos['tp'], 'tp'
            else:
                if h >= pos['sl']:
                    exit_p, reason = pos['sl'], 'sl'
                elif l <= pos['tp']:
                    exit_p, reason = pos['tp'], 'tp'

            if exit_p is None and pos['bars_held'] >= MAX_BARS:
                exit_p, reason = c, 'max_hold'

            if exit_p is None and bar_i == n - 1:
                exit_p, reason = c, 'end_of_data'

            if exit_p is not None:
                notional = min(CAPITAL * RISK_PCT / SL_PCT, CAPITAL * LEVERAGE)
                if pos['side'] == 'buy':
                    exit_adj = exit_p * (1 - FEE - SLIP)
                    gross = (exit_adj - pos['entry_price']) / pos['entry_price']
                else:
                    exit_adj = exit_p * (1 + FEE + SLIP)
                    gross = (pos['entry_price'] - exit_adj) / pos['entry_price']
                pnl = notional * gross

                trades.append({
                    'symbol': symbol,
                    'side': 'buy' if pos['side'] == 'buy' else 'sell',
                    'entry_ts': pos['entry_ts'],
                    'exit_ts': ts[bar_i],
                    'entry_price': pos['entry_price'],
                    'exit_price': exit_adj,
                    'pnl': pnl,
                    'reason': reason,
                    'bars': pos['bars_held'],
                })
                in_position = False
                pos = None
                active_zones = []  # reset context after a trade closes
            i += 1

    return trades

# ══════════════════════════════════════════════════════════════════
# Stats
# ══════════════════════════════════════════════════════════════════

def stats(trades):
    if not trades:
        return {
            'total': 0, 'win_rate': 0.0, 'profit_factor': 0.0, 'net_pnl': 0.0,
            'max_drawdown': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'expectancy': 0.0,
            'longs': 0, 'shorts': 0, 'monthly': {}, 'per_coin': {},
        }

    trades_sorted = sorted(trades, key=lambda t: t['entry_ts'])
    wins = [t for t in trades_sorted if t['pnl'] > 0]
    losses = [t for t in trades_sorted if t['pnl'] <= 0]

    gross_profit = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    net_pnl = sum(t['pnl'] for t in trades_sorted)

    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float('inf') if gross_profit > 0 else 0.0)
    win_rate = (len(wins) / len(trades_sorted)) * 100
    avg_win = (gross_profit / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    expectancy = net_pnl / len(trades_sorted)

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades_sorted:
        equity += t['pnl']
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    monthly = {}
    per_coin = {}
    for t in trades_sorted:
        dt = datetime.fromtimestamp(t['entry_ts'], tz=timezone.utc)
        key = f"{dt.year:04d}-{dt.month:02d}"
        m = monthly.setdefault(key, {'pnl': 0.0, 'n': 0, 'w': 0})
        m['pnl'] += t['pnl']; m['n'] += 1
        if t['pnl'] > 0: m['w'] += 1

        pc = per_coin.setdefault(t['symbol'], {'pnl': 0.0, 'n': 0, 'w': 0, 'wr': 0.0})
        pc['pnl'] += t['pnl']; pc['n'] += 1
        if t['pnl'] > 0: pc['w'] += 1

    for pc in per_coin.values():
        pc['wr'] = (pc['w'] / pc['n'] * 100) if pc['n'] else 0.0

    return {
        'total': len(trades_sorted),
        'win_rate': win_rate,
        'profit_factor': profit_factor if profit_factor != float('inf') else 999999.0,
        'net_pnl': net_pnl,
        'max_drawdown': max_dd,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'expectancy': expectancy,
        'longs': sum(1 for t in trades_sorted if t['side'] == 'buy'),
        'shorts': sum(1 for t in trades_sorted if t['side'] == 'sell'),
        'monthly': monthly,
        'per_coin': per_coin,
    }

# ══════════════════════════════════════════════════════════════════
# Shard runner
# ══════════════════════════════════════════════════════════════════

def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[shard {shard_idx}] {len(symbols)} symbols: {symbols}")
    start = time.time()

    all_trades = []
    with_data = []

    def process(symbol):
        candles = fetch_symbol(symbol)
        if len(candles) < MIN_BARS + 5:
            return symbol, [], False
        trades = backtest(symbol, candles)
        return symbol, trades, True

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(process, s): s for s in symbols}
        for fut in as_completed(futures):
            symbol, trades, had_data = fut.result()
            if had_data:
                with_data.append(symbol)
                all_trades.extend(trades)
                print(f"[shard {shard_idx}] {symbol}: {len(trades)} trades")
            else:
                print(f"[shard {shard_idx}] {symbol}: NO DATA")

    if not with_data:
        print(f"[shard {shard_idx}] ERROR: 0/{len(symbols)} symbols returned data. "
              f"Possible geo-block on data.binance.vision or symbol/timeframe mismatch.")

    shard_stats = stats(all_trades)
    elapsed = time.time() - start

    out = {
        'shard': shard_idx,
        'symbols': symbols,
        'with_data': with_data,
        'trades': all_trades,
        'stats': shard_stats,
        'elapsed': elapsed,
    }
    with open(f"shard_{shard_idx}.json", "w") as f:
        json.dump(out, f)
    print(f"[shard {shard_idx}] done in {elapsed:.1f}s, {len(all_trades)} trades, {len(with_data)}/{len(symbols)} had data")

# ══════════════════════════════════════════════════════════════════
# Merge
# ══════════════════════════════════════════════════════════════════

def merge_shards():
    all_trades = []
    all_symbols = []
    all_with_data = []

    for idx in range(NUM_SHARDS):
        path = f"shard_{idx}.json"
        try:
            with open(path) as f:
                shard = json.load(f)
        except FileNotFoundError:
            print(f"WARNING: {path} missing, skipping")
            continue
        all_trades.extend(shard['trades'])
        all_symbols.extend(shard['symbols'])
        all_with_data.extend(shard['with_data'])

    combined_stats = stats(all_trades)

    report = {
        'period': f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe': TIMEFRAME,
        'symbols_attempted': len(all_symbols),
        'symbols_with_data': len(all_with_data),
        'config': {
            'capital': CAPITAL, 'risk_pct': RISK_PCT, 'fee': FEE, 'slip': SLIP,
            'leverage': LEVERAGE, 'tp_pct': TP_PCT, 'sl_pct': SL_PCT,
            'max_bars': MAX_BARS, 'min_bars': MIN_BARS,
        },
        'stats': combined_stats,
    }
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    write_summary(report, all_with_data)
    print("Merge complete: backtest_report.json + backtest_summary.txt written")

def write_summary(report, with_data):
    s = report['stats']
    pf = s['profit_factor']
    wr = s['win_rate']
    usable = (pf >= 1.5 and wr >= 42)

    lines = []
    lines.append("=" * 60)
    lines.append("LIQUIDITY WALL ABSORPTION STRATEGY — BACKTEST SUMMARY")
    lines.append("(OHLC-only proxy — NOT the raw TradingIQ delta model)")
    lines.append("=" * 60)
    lines.append(f"Period:            {report['period']}")
    lines.append(f"Timeframe:         {report['timeframe']}")
    lines.append(f"Symbols attempted: {report['symbols_attempted']}")
    lines.append(f"Symbols with data: {report['symbols_with_data']}")
    lines.append("")
    lines.append(f"Config: Capital=${report['config']['capital']:.0f}  "
                  f"Risk={report['config']['risk_pct']*100:.1f}%  "
                  f"Leverage={report['config']['leverage']}x  "
                  f"Fee={report['config']['fee']*100:.3f}%  Slip={report['config']['slip']*100:.3f}%")
    lines.append(f"TP={report['config']['tp_pct']*100:.2f}%  SL={report['config']['sl_pct']*100:.2f}%  "
                  f"MaxBars={report['config']['max_bars']}  MinBars={report['config']['min_bars']}")
    lines.append("")
    lines.append("-" * 60)
    lines.append("AGGREGATE STATS")
    lines.append("-" * 60)
    lines.append(f"Total trades:      {s['total']}")
    lines.append(f"Win rate:          {s['win_rate']:.2f}%")
    lines.append(f"Profit factor:     {s['profit_factor']:.3f}")
    lines.append(f"Net PnL:           ${s['net_pnl']:.2f}")
    lines.append(f"Max drawdown:      ${s['max_drawdown']:.2f}")
    lines.append(f"Avg win:           ${s['avg_win']:.2f}")
    lines.append(f"Avg loss:          ${s['avg_loss']:.2f}")
    lines.append(f"Expectancy/trade:  ${s['expectancy']:.2f}")
    lines.append(f"Longs / Shorts:    {s['longs']} / {s['shorts']}")
    lines.append("")
    lines.append(f"RECOMMENDATION: {'✅ USABLE' if usable else '❌ NOT USABLE'}  "
                  f"(threshold: PF>=1.5 and WR>=42%)")
    lines.append("")

    lines.append("-" * 60)
    lines.append("TOP 50 COINS BY NET PNL")
    lines.append("-" * 60)
    ranked = sorted(s['per_coin'].items(), key=lambda kv: kv[1]['pnl'], reverse=True)[:50]
    lines.append(f"{'SYMBOL':<20}{'TRADES':>8}{'WR%':>8}{'PNL':>14}")
    for sym, d in ranked:
        lines.append(f"{sym:<20}{d['n']:>8}{d['wr']:>8.1f}{d['pnl']:>14.2f}")
    lines.append("")

    lines.append("-" * 60)
    lines.append("MONTHLY PNL")
    lines.append("-" * 60)
    lines.append(f"{'MONTH':<12}{'TRADES':>8}{'WINS':>8}{'PNL':>14}")
    for month in sorted(s['monthly'].keys()):
        m = s['monthly'][month]
        lines.append(f"{month:<12}{m['n']:>8}{m['w']:>8}{m['pnl']:>14.2f}")

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

# ══════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <shard_idx | merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))

