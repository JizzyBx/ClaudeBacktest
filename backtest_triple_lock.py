"""
RSI Mean Reversion Strategy v2 — Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fully OHLC-native mean-reversion strategy. No proxy/approximation needed --
RSI, EMA and candle direction are computed directly and exactly from kline
data.

v1 RESULT (for reference): PF 0.799, WR 32.4%, net -$128,532 over 1yr/96
coins with TP 2.5%/SL 1.25% (2:1 R:R). Root cause identified: a 2:1 R:R
target is wrong for mean reversion -- its edge (if any) comes from win
rate, not reward size, and 32.4% WR sits right at/below the 33.3%
breakeven line for that R:R before costs.

v2 CHANGES:
    - TP/SL tightened to 1.0% / 1.0% (1:1 R:R) -- fits the smaller, faster
      moves a genuine RSI bounce actually produces
    - EMA(200) trend filter added: longs only fire when price is above the
      EMA (fading a dip within an uptrend/range), shorts only fire when
      price is below the EMA (fading a rally within a downtrend/range).
      Avoids knife-catching against a dominant trend.
    - Max hold cut from 48 bars (12h) to 16 bars (4h) -- a reversion move
      should resolve quickly or the setup has failed.

LOGIC:
    1. RSI(14) reaches an extreme AND price is on the trend-filter-correct
       side of EMA(200):
         - oversold + price > EMA200:  watch for long
         - overbought + price < EMA200: watch for short
    2. Reversal confirmation candle (avoids catching a falling knife):
         - after oversold: wait for a bullish close (c > o) while RSI is
           still <= RSI_OVERSOLD + RSI_CONFIRM_BAND -> enter long
         - after overbought: wait for a bearish close (c < o) while RSI is
           still >= RSI_OVERBOUGHT - RSI_CONFIRM_BAND -> enter short
    3. Entry on next bar open, same TP/SL/max-hold framework as before.

Coin list: GMaxV1 COINS_UNIVERSE (96 coins with data, per last run)
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

TP_PCT   = 0.010          # 1.0%
SL_PCT   = 0.010          # 1.0%  (1:1 R:R -- fits mean reversion's win-rate-driven edge)
MAX_BARS = 16             # 4h on 15m -- reversion should resolve fast or it's not working
MIN_BARS = 210            # warmup for RSI + EMA200

# Strategy-specific params
RSI_PERIOD      = 14
RSI_OVERSOLD    = 30
RSI_OVERBOUGHT  = 70
RSI_CONFIRM_BAND = 5      # after extreme, still allow confirmation while RSI within
                           # this many points of the extreme (e.g. RSI<=35 still counts
                           # as "was oversold, now confirming" for a few bars)
RSI_CONFIRM_MAX_BARS = 6  # give up waiting for confirmation after this many bars

EMA_TREND_PERIOD = 200    # trend filter: only fade dips above this EMA (longs),
                           # only fade rallies below it (shorts) -- avoids
                           # knife-catching against the dominant trend

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
                        # Normalize to Unix seconds regardless of source unit.
                        # seconds ~1.7e9, ms ~1.7e12, us ~1.7e15, ns ~1.7e18
                        while ts > 10**11:
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
# Strategy: RSI Mean Reversion
# ══════════════════════════════════════════════════════════════════

def compute_rsi(closes, period):
    """
    Standard Wilder RSI, computed once per symbol over the full close series.
    Returns a list same length as closes; first `period` values are None
    (not enough data to warm up).
    """
    n = len(closes)
    rsi = [None] * n
    if n <= period:
        return rsi

    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        delta = closes[i] - closes[i - 1]
        gains[i] = max(delta, 0.0)
        losses[i] = max(-delta, 0.0)

    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period

    def rsi_from_avgs(ag, al):
        if al == 0:
            return 100.0
        rs = ag / al
        return 100.0 - (100.0 / (1.0 + rs))

    rsi[period] = rsi_from_avgs(avg_gain, avg_loss)

    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rsi[i] = rsi_from_avgs(avg_gain, avg_loss)

    return rsi

def compute_ema(closes, period):
    """Standard EMA. First `period-1` values are None (warmup)."""
    n = len(closes)
    ema = [None] * n
    if n < period:
        return ema
    k = 2.0 / (period + 1)
    sma = sum(closes[:period]) / period
    ema[period - 1] = sma
    for i in range(period, n):
        ema[i] = closes[i] * k + ema[i - 1] * (1 - k)
    return ema

def signal(i, opens, closes, rsi_values, ema_values, state):
    """
    Evaluated on closed bar i. Returns ('buy'|'sell'|None, updated_state).
    state: dict tracking a pending setup, e.g.
        {'side': 'long'|'short', 'triggered_at': i} or None
    Caller owns state per-symbol (reset after each trade / at start).

    Trend filter: only take long reversion setups when price is above the
    EMA (fading a dip within an uptrend/range), only take shorts when price
    is below the EMA (fading a rally within a downtrend/range). This avoids
    catching a falling knife in a strong downtrend or fighting a strong
    uptrend on the short side.
    """
    r = rsi_values[i]
    ema = ema_values[i]
    if r is None or ema is None:
        return None, state

    o, c = opens[i], closes[i]

    # 1. No pending setup: check if we just hit an extreme -> arm a watch
    if state is None:
        if r <= RSI_OVERSOLD and c > ema:
            state = {'side': 'long', 'triggered_at': i}
        elif r >= RSI_OVERBOUGHT and c < ema:
            state = {'side': 'short', 'triggered_at': i}
        return None, state

    # 2. Pending setup: check for confirmation or expiry
    bars_since = i - state['triggered_at']
    if bars_since > RSI_CONFIRM_MAX_BARS:
        return None, None  # gave up waiting, clear state

    if state['side'] == 'long':
        # keep watching only while still within the confirm band
        if r > RSI_OVERSOLD + RSI_CONFIRM_BAND:
            return None, None  # bounced too far already without a clean confirm bar, drop
        if c > o:
            return 'buy', None  # confirmed -> fire signal, clear state
        return None, state  # still waiting
    else:
        if r < RSI_OVERBOUGHT - RSI_CONFIRM_BAND:
            return None, None
        if c < o:
            return 'sell', None
        return None, state

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

    rsi_values = compute_rsi(closes, RSI_PERIOD)
    ema_values = compute_ema(closes, EMA_TREND_PERIOD)

    n = len(candles)
    setup_state = None
    in_position = False
    pos = None

    i = MIN_BARS
    while i < n - 1:
        if not in_position:
            sig, setup_state = signal(i, opens, closes, rsi_values, ema_values, setup_state)
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
                setup_state = None  # reset context after a trade closes
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
        try:
            dt = datetime.fromtimestamp(t['entry_ts'], tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            # Malformed timestamp slipped through fetch parsing — bucket it
            # separately instead of crashing the whole stats pass.
            dt = None
        key = f"{dt.year:04d}-{dt.month:02d}" if dt else "UNKNOWN"
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
    lines.append("RSI MEAN REVERSION STRATEGY v2 — BACKTEST SUMMARY")
    lines.append(f"(RSI({RSI_PERIOD}) extreme + EMA({EMA_TREND_PERIOD}) trend filter + reversal confirm)")
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
