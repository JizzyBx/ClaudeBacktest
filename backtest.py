"""
S&R Zone + Pin Bar + RSI Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Timeframe  : 15m
Data       : Binance Vision futures monthly zips (stdlib only)
Strategy   :
  1. Find proven S&R zones (2+ touches within 0.3% zone, last 100 bars)
  2. Price returns to zone
  3. Pin bar rejection forms at zone
     - Support : lower wick > 2x body, close in upper 30% of candle
     - Resistance: upper wick > 2x body, close in lower 30% of candle
  4. RSI filter
     - At support   : RSI < 45
     - At resistance: RSI > 55

Variant A — Fixed TP/SL  : TP 3.0% | SL 1.5%
Variant B — S&R Level TP : TP = next proven S&R level | SL 1.5% | fallback TP 4.0%

Leverage: 5x | Capital: $10,000 | Risk/trade: 0.75%
Fees: 0.05%/side | Slippage: 0.02%/side | Max positions: 10
"""

import json, csv, zipfile, io, urllib.request, urllib.error
import math
from datetime import datetime, timezone
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────
INTERVAL         = "15m"
START_YEAR       = 2023
START_MONTH      = 1
END_YEAR         = 2024
END_MONTH        = 12
CAPITAL          = 10_000.0
RISK_PCT         = 0.0075
FEE              = 0.0005
SLIP             = 0.0002
MAX_POS          = 10
LEVERAGE         = 5

# Variant A
VA_TP_PCT        = 0.030
VA_SL_PCT        = 0.015

# Variant B
VB_SL_PCT        = 0.015
VB_FALLBACK_TP   = 0.040

# S&R params
SR_LOOKBACK      = 100     # bars to scan for S&R levels
SR_ZONE_PCT      = 0.003   # 0.3% zone around level
SR_MIN_TOUCHES   = 2       # minimum touches to confirm level
SWING_SIDE_BARS  = 5       # bars on each side to confirm swing point

# Pin bar params
PIN_WICK_RATIO   = 2.0     # wick must be > this x body size
PIN_CLOSE_PCT    = 0.30    # close must be in top/bottom 30% of candle range

# RSI
RSI_PERIOD       = 14
RSI_SUP_MAX      = 45      # at support RSI must be below this
RSI_RES_MIN      = 55      # at resistance RSI must be above this

# Hold limit
MAX_HOLD_BARS    = 96      # 24h at 15m

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

COINS = [
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

# ── DATA FETCH ────────────────────────────────────────────
def fetch_monthly(symbol, year, month):
    ym  = f"{year}-{month:02d}"
    url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        candles = []
        for row in rows:
            if not row or row[0].startswith('o'): continue
            ts = int(row[0])
            if ts > 10**14: ts //= 1000
            candles.append({
                'ts': ts,
                'o' : float(row[1]),
                'h' : float(row[2]),
                'l' : float(row[3]),
                'c' : float(row[4]),
                'v' : float(row[5]),
            })
        return candles
    except urllib.error.HTTPError as e:
        if e.code == 404: return []
        print(f"  HTTP {e.code} {symbol} {ym}")
        return []
    except Exception as e:
        print(f"  ERR {symbol} {ym}: {e}")
        return []

def fetch_symbol(symbol):
    all_candles = []
    for y in range(START_YEAR, END_YEAR + 1):
        m0 = START_MONTH if y == START_YEAR else 1
        m1 = END_MONTH   if y == END_YEAR   else 12
        for m in range(m0, m1 + 1):
            all_candles.extend(fetch_monthly(symbol, y, m))
    all_candles.sort(key=lambda x: x['ts'])
    return all_candles

# ── INDICATORS ────────────────────────────────────────────
def calc_rsi(candles, i, period=RSI_PERIOD):
    if i < period + 1: return None
    gains = losses = 0.0
    for j in range(i - period, i):
        diff = candles[j]['c'] - candles[j-1]['c']
        if diff > 0: gains += diff
        else:        losses -= diff
    if losses == 0: return 100.0
    rs = (gains / period) / (losses / period)
    return 100 - (100 / (1 + rs))

def is_swing_high(candles, i, side_bars=SWING_SIDE_BARS):
    if i < side_bars or i >= len(candles) - side_bars: return False
    h = candles[i]['h']
    for j in range(i - side_bars, i + side_bars + 1):
        if j == i: continue
        if candles[j]['h'] >= h: return False
    return True

def is_swing_low(candles, i, side_bars=SWING_SIDE_BARS):
    if i < side_bars or i >= len(candles) - side_bars: return False
    l = candles[i]['l']
    for j in range(i - side_bars, i + side_bars + 1):
        if j == i: continue
        if candles[j]['l'] <= l: return False
    return True

def find_sr_levels(candles, i, lookback=SR_LOOKBACK):
    """
    Scan last `lookback` closed bars for proven S&R levels.
    Returns list of (price, type) where type = 'support' or 'resistance'
    Only uses bars strictly before i (no lookahead).
    """
    start = max(0, i - lookback)
    end   = i - 1  # only closed bars

    # Collect swing points
    swing_highs = []
    swing_lows  = []

    for j in range(start + SWING_SIDE_BARS, end - SWING_SIDE_BARS + 1):
        if is_swing_high(candles, j):
            swing_highs.append(candles[j]['h'])
        if is_swing_low(candles, j):
            swing_lows.append(candles[j]['l'])

    levels = []

    # Cluster swing highs → resistance levels
    used = [False] * len(swing_highs)
    for a in range(len(swing_highs)):
        if used[a]: continue
        cluster = [swing_highs[a]]
        for b in range(a + 1, len(swing_highs)):
            if used[b]: continue
            if abs(swing_highs[b] - swing_highs[a]) / swing_highs[a] <= SR_ZONE_PCT:
                cluster.append(swing_highs[b])
                used[b] = True
        if len(cluster) >= SR_MIN_TOUCHES:
            levels.append((sum(cluster) / len(cluster), 'resistance', len(cluster)))
        used[a] = True

    # Cluster swing lows → support levels
    used = [False] * len(swing_lows)
    for a in range(len(swing_lows)):
        if used[a]: continue
        cluster = [swing_lows[a]]
        for b in range(a + 1, len(swing_lows)):
            if used[b]: continue
            if abs(swing_lows[b] - swing_lows[a]) / swing_lows[a] <= SR_ZONE_PCT:
                cluster.append(swing_lows[b])
                used[b] = True
        if len(cluster) >= SR_MIN_TOUCHES:
            levels.append((sum(cluster) / len(cluster), 'support', len(cluster)))
        used[a] = True

    return levels

def price_at_level(price, level_price, zone_pct=SR_ZONE_PCT):
    return abs(price - level_price) / level_price <= zone_pct

def is_support_pin_bar(candle):
    """
    Bullish pin bar at support:
    - Lower wick > PIN_WICK_RATIO x body
    - Close in upper PIN_CLOSE_PCT of total candle range
    """
    body  = abs(candle['c'] - candle['o'])
    if body == 0: body = (candle['h'] - candle['l']) * 0.001
    lower_wick  = min(candle['o'], candle['c']) - candle['l']
    candle_range = candle['h'] - candle['l']
    if candle_range == 0: return False
    close_pct = (candle['c'] - candle['l']) / candle_range
    return (lower_wick >= PIN_WICK_RATIO * body and
            close_pct >= (1 - PIN_CLOSE_PCT))

def is_resistance_pin_bar(candle):
    """
    Bearish pin bar at resistance:
    - Upper wick > PIN_WICK_RATIO x body
    - Close in lower PIN_CLOSE_PCT of total candle range
    """
    body  = abs(candle['c'] - candle['o'])
    if body == 0: body = (candle['h'] - candle['l']) * 0.001
    upper_wick   = candle['h'] - max(candle['o'], candle['c'])
    candle_range = candle['h'] - candle['l']
    if candle_range == 0: return False
    close_pct = (candle['c'] - candle['l']) / candle_range
    return (upper_wick >= PIN_WICK_RATIO * body and
            close_pct <= PIN_CLOSE_PCT)

def find_next_sr_level(levels, entry_price, side):
    """
    For Variant B: find nearest S&R level in trade direction.
    LONG → find nearest resistance above entry
    SHORT → find nearest support below entry
    """
    if side == 'LONG':
        candidates = [
            lv for lv, lt, _ in levels
            if lt == 'resistance' and lv > entry_price * (1 + VB_SL_PCT)
        ]
        return min(candidates) if candidates else None
    else:
        candidates = [
            lv for lv, lt, _ in levels
            if lt == 'support' and lv < entry_price * (1 - VB_SL_PCT)
        ]
        return max(candidates) if candidates else None

# ── SIGNAL EXTRACTION ─────────────────────────────────────
def extract_signals(symbol, candles):
    """
    Signal bar = candles[i-1] (closed pin bar at S&R zone)
    Entry bar  = candles[i]   (enter at open)
    All S&R levels built from bars strictly before signal bar.
    """
    warmup = SR_LOOKBACK + RSI_PERIOD + SWING_SIDE_BARS + 2
    signals_a = []  # Variant A fixed TP
    signals_b = []  # Variant B S&R TP
    stats = defaultdict(int)

    for i in range(warmup, len(candles)):
        stats['total_bars'] += 1

        sig_bar    = candles[i - 1]
        entry_bar  = candles[i]
        entry_price = entry_bar['o']

        # ── RSI on signal bar ─────────────────────────────
        rsi = calc_rsi(candles, i - 1)
        if rsi is None:
            stats['no_rsi'] += 1
            continue

        # ── S&R levels from history ───────────────────────
        levels = find_sr_levels(candles, i - 1)
        if not levels:
            stats['no_sr_levels'] += 1
            continue

        # ── Check if signal bar is at any level ───────────
        matched_support    = None
        matched_resistance = None

        for lv, lt, touches in levels:
            if price_at_level(sig_bar['l'], lv) or price_at_level(sig_bar['c'], lv):
                if lt == 'support':
                    matched_support = (lv, touches)
            if price_at_level(sig_bar['h'], lv) or price_at_level(sig_bar['c'], lv):
                if lt == 'resistance':
                    matched_resistance = (lv, touches)

        if not matched_support and not matched_resistance:
            stats['not_at_level'] += 1
            continue

        fired = False

        # ── LONG: support + bullish pin + RSI < 45 ────────
        if matched_support and is_support_pin_bar(sig_bar) and rsi < RSI_SUP_MAX:
            stats['signals_long'] += 1
            fired = True

            # Variant A
            signals_a.append({
                'symbol'      : symbol,
                'side'        : 'LONG',
                'entry_ts'    : entry_bar['ts'],
                'entry_price' : entry_price,
                'tp'          : entry_price * (1 + VA_TP_PCT),
                'sl'          : entry_price * (1 - VA_SL_PCT),
                'entry_bar_i' : i,
                'level'       : matched_support[0],
                'touches'     : matched_support[1],
            })

            # Variant B — find next resistance
            next_res = find_next_sr_level(levels, entry_price, 'LONG')
            tp_b = next_res if next_res else entry_price * (1 + VB_FALLBACK_TP)
            # cap TP at 8% to avoid unrealistic targets
            tp_b = min(tp_b, entry_price * 1.08)
            signals_b.append({
                'symbol'      : symbol,
                'side'        : 'LONG',
                'entry_ts'    : entry_bar['ts'],
                'entry_price' : entry_price,
                'tp'          : tp_b,
                'sl'          : entry_price * (1 - VB_SL_PCT),
                'entry_bar_i' : i,
                'level'       : matched_support[0],
                'touches'     : matched_support[1],
            })

        # ── SHORT: resistance + bearish pin + RSI > 55 ────
        if not fired and matched_resistance and is_resistance_pin_bar(sig_bar) and rsi > RSI_RES_MIN:
            stats['signals_short'] += 1

            # Variant A
            signals_a.append({
                'symbol'      : symbol,
                'side'        : 'SHORT',
                'entry_ts'    : entry_bar['ts'],
                'entry_price' : entry_price,
                'tp'          : entry_price * (1 - VA_TP_PCT),
                'sl'          : entry_price * (1 + VA_SL_PCT),
                'entry_bar_i' : i,
                'level'       : matched_resistance[0],
                'touches'     : matched_resistance[1],
            })

            # Variant B — find next support
            next_sup = find_next_sr_level(levels, entry_price, 'SHORT')
            tp_b = next_sup if next_sup else entry_price * (1 - VB_FALLBACK_TP)
            tp_b = max(tp_b, entry_price * 0.92)
            signals_b.append({
                'symbol'      : symbol,
                'side'        : 'SHORT',
                'entry_ts'    : entry_bar['ts'],
                'entry_price' : entry_price,
                'tp'          : tp_b,
                'sl'          : entry_price * (1 + VB_SL_PCT),
                'entry_bar_i' : i,
                'level'       : matched_resistance[0],
                'touches'     : matched_resistance[1],
            })

        if not matched_support and not matched_resistance:
            pass
        elif not fired and matched_support and not is_support_pin_bar(sig_bar):
            stats['no_pin_bar'] += 1
        elif not fired and matched_resistance and not is_resistance_pin_bar(sig_bar):
            stats['no_pin_bar'] += 1

    return signals_a, signals_b, stats

# ── PORTFOLIO SIMULATION ──────────────────────────────────
def simulate_with_sizing(coin_data, raw_signals, sl_pct_ref):
    raw_signals.sort(key=lambda x: x['entry_ts'])

    equity   = CAPITAL
    open_pos = []
    closed   = []

    all_events = []
    for sym, candles in coin_data.items():
        for idx, c in enumerate(candles):
            all_events.append((c['ts'], sym, idx))
    all_events.sort()

    sig_map = defaultdict(list)
    for s in raw_signals:
        sig_map[(s['entry_ts'], s['symbol'])].append(s)

    for (ts, sym, bar_idx) in all_events:
        candles = coin_data[sym]
        bar     = candles[bar_idx]

        # ── Close positions ───────────────────────────────
        still_open = []
        for pos in open_pos:
            if pos['symbol'] != sym:
                still_open.append(pos)
                continue

            age         = bar_idx - pos['entry_bar_i']
            close_price = None
            result      = None

            if pos['side'] == 'LONG':
                if bar['h'] >= pos['tp']:
                    close_price = pos['tp'];  result = 'WIN'
                elif bar['l'] <= pos['sl']:
                    close_price = pos['sl'];  result = 'LOSS'
                elif age >= MAX_HOLD_BARS:
                    close_price = bar['c'];   result = 'TIMEOUT'
            else:
                if bar['l'] <= pos['tp']:
                    close_price = pos['tp'];  result = 'WIN'
                elif bar['h'] >= pos['sl']:
                    close_price = pos['sl'];  result = 'LOSS'
                elif age >= MAX_HOLD_BARS:
                    close_price = bar['c'];   result = 'TIMEOUT'

            if result:
                raw_ret = (close_price - pos['entry_price']) / pos['entry_price']
                if pos['side'] == 'SHORT': raw_ret = -raw_ret
                net_ret = raw_ret - (FEE + SLIP) * 2
                # use actual SL distance for sizing denominator
                sl_dist = abs(pos['tp'] - pos['entry_price']) / pos['entry_price'] if result == 'WIN' else sl_pct_ref
                pnl     = pos['risk_usd'] * LEVERAGE * (net_ret / (sl_pct_ref + FEE + SLIP))
                equity += pnl
                month_key = datetime.fromtimestamp(
                    ts / 1000, tz=timezone.utc).strftime('%Y-%m')
                closed.append({
                    'symbol'      : sym,
                    'side'        : pos['side'],
                    'entry_price' : pos['entry_price'],
                    'close_price' : close_price,
                    'entry_ts'    : pos['entry_ts'],
                    'close_ts'    : ts,
                    'result'      : result,
                    'pnl'         : round(pnl, 4),
                    'bars_held'   : age,
                    'month'       : month_key,
                })
            else:
                still_open.append(pos)
        open_pos = still_open

        # ── New signal ────────────────────────────────────
        key = (ts, sym)
        if key in sig_map:
            for sig in sig_map[key]:
                if any(p['symbol'] == sym for p in open_pos): continue
                if len(open_pos) >= MAX_POS: continue
                risk_usd = equity * RISK_PCT
                open_pos.append({
                    'symbol'      : sym,
                    'side'        : sig['side'],
                    'entry_price' : sig['entry_price'],
                    'tp'          : sig['tp'],
                    'sl'          : sig['sl'],
                    'entry_bar_i' : sig['entry_bar_i'],
                    'entry_ts'    : sig['entry_ts'],
                    'risk_usd'    : risk_usd,
                })

    return closed

# ── RESULTS ───────────────────────────────────────────────
def compute_stats(trades, capital=CAPITAL):
    if not trades:
        return None
    wins    = [t for t in trades if t['result'] == 'WIN']
    losses  = [t for t in trades if t['result'] == 'LOSS']
    timeout = [t for t in trades if t['result'] == 'TIMEOUT']
    total   = len(trades)
    wr      = len(wins) / total * 100
    gross_w = sum(t['pnl'] for t in wins)
    gross_l = abs(sum(t['pnl'] for t in losses + timeout))
    pf      = gross_w / gross_l if gross_l > 0 else float('inf')
    net_pnl = sum(t['pnl'] for t in trades)
    avg_win  = gross_w / len(wins)   if wins   else 0
    avg_loss = gross_l / len(losses) if losses else 0

    eq = [capital]
    for t in trades: eq.append(eq[-1] + t['pnl'])
    peak = eq[0]; max_dd = 0
    for e in eq:
        if e > peak: peak = e
        dd = (peak - e) / peak * 100
        if dd > max_dd: max_dd = dd

    monthly = defaultdict(float)
    for t in trades: monthly[t['month']] += t['pnl']
    mv = list(monthly.values())
    if len(mv) > 1:
        avg_m   = sum(mv) / len(mv)
        std_m   = math.sqrt(sum((x - avg_m)**2 for x in mv) / len(mv))
        neg     = [x for x in mv if x < 0]
        std_neg = math.sqrt(sum(x**2 for x in neg) / len(neg)) if neg else 0.001
        sharpe  = avg_m / std_m   if std_m  > 0 else 0
        sortino = avg_m / std_neg if std_neg > 0 else 0
    else:
        sharpe = sortino = 0

    streak_cur = streak_max_w = streak_max_l = 0
    streak_type = None
    for t in trades:
        w = t['result'] == 'WIN'
        if streak_type is None: streak_type = w; streak_cur = 1
        elif w == streak_type:  streak_cur += 1
        else: streak_type = w;  streak_cur = 1
        if w:  streak_max_w = max(streak_max_w, streak_cur)
        else:  streak_max_l = max(streak_max_l, streak_cur)

    longs  = [t for t in trades if t['side'] == 'LONG']
    shorts = [t for t in trades if t['side'] == 'SHORT']
    lwr = len([t for t in longs  if t['result']=='WIN']) / len(longs)  * 100 if longs  else 0
    swr = len([t for t in shorts if t['result']=='WIN']) / len(shorts) * 100 if shorts else 0

    coin_pf_list = []
    for sym in set(t['symbol'] for t in trades):
        st  = [t for t in trades if t['symbol'] == sym]
        sw  = sum(t['pnl'] for t in st if t['pnl'] > 0)
        sl  = abs(sum(t['pnl'] for t in st if t['pnl'] <= 0))
        pfc = sw / sl if sl > 0 else float('inf')
        wc  = len([t for t in st if t['result'] == 'WIN'])
        coin_pf_list.append({
            'symbol': sym,
            'trades': len(st),
            'wr'    : round(wc / len(st) * 100, 1),
            'pnl'   : round(sum(t['pnl'] for t in st), 2),
            'pf'    : round(pfc, 3),
        })
    coin_pf_list.sort(key=lambda x: x['pf'], reverse=True)

    return {
        'total'       : total,
        'wr'          : wr,
        'pf'          : pf,
        'net_pnl'     : net_pnl,
        'max_dd'      : max_dd,
        'sharpe'      : sharpe,
        'sortino'     : sortino,
        'avg_win'     : avg_win,
        'avg_loss'    : avg_loss,
        'streak_w'    : streak_max_w,
        'streak_l'    : streak_max_l,
        'wins'        : len(wins),
        'losses'      : len(losses),
        'timeout'     : len(timeout),
        'longs'       : len(longs),
        'shorts'      : len(shorts),
        'lwr'         : lwr,
        'swr'         : swr,
        'monthly'     : dict(monthly),
        'per_coin'    : coin_pf_list,
        'pass'        : pf >= 1.5 and wr >= 42,
    }

def print_variant(label, s):
    if s is None:
        print(f"\n{label}: ZERO TRADES")
        return
    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"{'='*60}")
    print(f"Total Trades   : {s['total']}")
    print(f"Win Rate       : {s['wr']:.1f}%")
    print(f"Profit Factor  : {s['pf']:.3f}")
    print(f"Net PnL        : ${s['net_pnl']:,.2f}")
    print(f"Max Drawdown   : {s['max_dd']:.1f}%")
    print(f"Sharpe (mo)    : {s['sharpe']:.2f}")
    print(f"Sortino (mo)   : {s['sortino']:.2f}")
    print(f"Avg Win        : ${s['avg_win']:.2f}")
    print(f"Avg Loss       : ${s['avg_loss']:.2f}")
    print(f"Max Win Streak : {s['streak_w']}")
    print(f"Max Loss Streak: {s['streak_l']}")
    print(f"Wins/Loss/TO   : {s['wins']}/{s['losses']}/{s['timeout']}")
    print(f"Longs          : {s['longs']} trades  WR {s['lwr']:.1f}%")
    print(f"Shorts         : {s['shorts']} trades  WR {s['swr']:.1f}%")
    print(f"Leverage       : {LEVERAGE}x")
    print(f"Target         : PF ≥ 1.5 | WR ≥ 42% → {'✅ PASS' if s['pass'] else '❌ FAIL'}")

    print(f"\nPER-COIN (sorted by PF):")
    print(f"{'Symbol':<22} {'Trades':>6} {'WR%':>6} {'PnL':>10} {'PF':>6}")
    print("-"*55)
    for c in s['per_coin']:
        pf_str = f"{c['pf']:.3f}" if c['pf'] != float('inf') else "inf"
        print(f"{c['symbol']:<22} {c['trades']:>6} {c['wr']:>5.1f}% {c['pnl']:>10.2f} {pf_str:>6}")

    print(f"\nMONTHLY PnL:")
    for mo in sorted(s['monthly'].keys()):
        v    = s['monthly'][mo]
        sign = '+' if v >= 0 else ''
        print(f"  {mo}: {sign}${v:,.2f}")

def save_outputs(sa, sb, filter_stats):
    lines = [
        "S&R ZONE + PIN BAR + RSI BACKTEST SUMMARY",
        f"Generated  : {datetime.now(timezone.utc).isoformat()}",
        f"Timeframe  : {INTERVAL}",
        f"Leverage   : {LEVERAGE}x",
        f"SR Lookback: {SR_LOOKBACK} bars | Zone: {SR_ZONE_PCT*100:.1f}% | Min touches: {SR_MIN_TOUCHES}",
        f"Pin ratio  : {PIN_WICK_RATIO}x | Close pct: {PIN_CLOSE_PCT*100:.0f}%",
        f"RSI period : {RSI_PERIOD} | Sup max: {RSI_SUP_MAX} | Res min: {RSI_RES_MIN}",
        "",
        "── VARIANT A (Fixed TP 3% / SL 1.5%) ──",
    ]
    if sa:
        lines += [
            f"Trades  : {sa['total']}",
            f"WR      : {sa['wr']:.1f}%",
            f"PF      : {sa['pf']:.3f}",
            f"Net PnL : ${sa['net_pnl']:,.2f}",
            f"Result  : {'PASS' if sa['pass'] else 'FAIL'}",
        ]
    else:
        lines.append("ZERO TRADES")

    lines += ["", "── VARIANT B (S&R Level TP / SL 1.5%) ──"]
    if sb:
        lines += [
            f"Trades  : {sb['total']}",
            f"WR      : {sb['wr']:.1f}%",
            f"PF      : {sb['pf']:.3f}",
            f"Net PnL : ${sb['net_pnl']:,.2f}",
            f"Result  : {'PASS' if sb['pass'] else 'FAIL'}",
        ]
    else:
        lines.append("ZERO TRADES")

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    report = {
        "meta": {
            "strategy"    : "SR_PinBar_RSI_15m",
            "leverage"    : LEVERAGE,
            "interval"    : INTERVAL,
            "sr_lookback" : SR_LOOKBACK,
            "sr_zone_pct" : SR_ZONE_PCT,
            "sr_touches"  : SR_MIN_TOUCHES,
            "pin_ratio"   : PIN_WICK_RATIO,
            "rsi_period"  : RSI_PERIOD,
            "period"      : f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "coins"       : len(COINS),
            "max_pos"     : MAX_POS,
            "risk_pct"    : RISK_PCT,
        },
        "variant_a" : {
            "config"   : {"tp_pct": VA_TP_PCT, "sl_pct": VA_SL_PCT},
            "aggregate": {k: sa[k] for k in ['total','wr','pf','net_pnl','max_dd']} if sa else {},
            "per_coin" : sa['per_coin'] if sa else [],
            "monthly"  : {k: round(v,2) for k,v in sa['monthly'].items()} if sa else {},
            "trades"   : [],
        },
        "variant_b" : {
            "config"   : {"sl_pct": VB_SL_PCT, "fallback_tp_pct": VB_FALLBACK_TP},
            "aggregate": {k: sb[k] for k in ['total','wr','pf','net_pnl','max_dd']} if sb else {},
            "per_coin" : sb['per_coin'] if sb else [],
            "monthly"  : {k: round(v,2) for k,v in sb['monthly'].items()} if sb else {},
            "trades"   : [],
        },
        "filter_stats": dict(filter_stats),
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n✅ Saved: backtest_summary.txt + backtest_report.json")

# ── MAIN ──────────────────────────────────────────────────
def run_portfolio():
    print(f"\n{'='*60}")
    print(f"S&R ZONE + PIN BAR + RSI — {INTERVAL} — {len(COINS)} coins — {LEVERAGE}x")
    print(f"Period     : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print(f"SR Lookback: {SR_LOOKBACK} | Zone: {SR_ZONE_PCT*100:.1f}% | Touches: {SR_MIN_TOUCHES}")
    print(f"Variant A  : TP {VA_TP_PCT*100:.1f}% / SL {VA_SL_PCT*100:.1f}%")
    print(f"Variant B  : S&R level TP / SL {VB_SL_PCT*100:.1f}% / fallback {VB_FALLBACK_TP*100:.1f}%")
    print(f"Max pos    : {MAX_POS}")
    print(f"{'='*60}\n")

    print("Phase 1: Fetching data...")
    coin_data = {}
    for sym in COINS:
        candles = fetch_symbol(sym)
        if len(candles) < 200:
            print(f"  SKIP {sym} — {len(candles)} candles")
            continue
        coin_data[sym] = candles
        print(f"  OK {sym}: {len(candles)} candles")

    if not coin_data:
        print("FATAL: No data fetched.")
        return

    print(f"\nFetched {len(coin_data)}/{len(COINS)} coins")
    print("\nPhase 2: Extracting signals...")

    all_a = []
    all_b = []
    filter_totals = defaultdict(int)

    for sym, candles in coin_data.items():
        sigs_a, sigs_b, stats = extract_signals(sym, candles)
        all_a.extend(sigs_a)
        all_b.extend(sigs_b)
        for k, v in stats.items():
            filter_totals[k] += v

    total_sigs = filter_totals.get('signals_long', 0) + filter_totals.get('signals_short', 0)
    print(f"  Total signals : {total_sigs}")
    print(f"  Long          : {filter_totals.get('signals_long', 0)}")
    print(f"  Short         : {filter_totals.get('signals_short', 0)}")
    print(f"  No sweep/level: {filter_totals.get('not_at_level', 0):,}")
    print(f"  No pin bar    : {filter_totals.get('no_pin_bar', 0):,}")
    print(f"  No SR levels  : {filter_totals.get('no_sr_levels', 0):,}")

    if total_sigs == 0:
        print("\n⚠️  ZERO SIGNALS")
        save_outputs(None, None, filter_totals)
        return

    print("\nPhase 3: Simulating Variant A (Fixed TP)...")
    trades_a = simulate_with_sizing(coin_data, all_a, VA_SL_PCT)
    print(f"  Closed trades: {len(trades_a)}")

    print("\nPhase 4: Simulating Variant B (S&R TP)...")
    trades_b = simulate_with_sizing(coin_data, all_b, VB_SL_PCT)
    print(f"  Closed trades: {len(trades_b)}")

    print("\nPhase 5: Results...")
    sa = compute_stats(trades_a)
    sb = compute_stats(trades_b)

    print_variant("VARIANT A — Fixed TP 3.0% / SL 1.5%", sa)
    print_variant("VARIANT B — S&R Level TP / SL 1.5%", sb)

    print("\nFILTER STATS:")
    tb = filter_totals.get('total_bars', 1)
    for k, v in filter_totals.items():
        if k == 'total_bars': continue
        print(f"  {k:<25}: {v:>8,}  ({v/tb*100:.2f}%)")

    save_outputs(sa, sb, filter_totals)

if __name__ == "__main__":
    run_portfolio()
