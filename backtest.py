"""
FVG + Liquidity Sweep Backtest
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Timeframe  : 5m
Data       : Binance Vision futures monthly zips (stdlib only)
Strategy   :
  LONG  — price sweeps below recent swing low (liquidity grab),
          closes back above it, bullish FVG exists below current price,
          enter when price retraces into FVG zone.
  SHORT — mirror opposite.
TP: 1.5% | SL: 2.5% | Leverage: 5x | Max hold: 48 bars (4h)
Capital    : $10,000 shared | Risk/trade: 0.75% | Fees: 0.05%/side | Slip: 0.02%/side
Max positions: 6 portfolio-wide
"""

import json, csv, zipfile, io, urllib.request, urllib.error
import os, math
from datetime import datetime, timezone
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────
INTERVAL        = "5m"
START_YEAR      = 2023
START_MONTH     = 1
END_YEAR        = 2024
END_MONTH       = 12
CAPITAL         = 10_000.0
RISK_PCT        = 0.0075
FEE             = 0.0005
SLIP            = 0.0002
MAX_POS         = 6
TP_PCT          = 0.015
SL_PCT          = 0.025
MAX_HOLD_BARS   = 48
SWING_LOOKBACK  = 10
FVG_MIN_GAP     = 0.0002
LEVERAGE        = 5

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
    ym = f"{year}-{month:02d}"
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
                'o': float(row[1]),
                'h': float(row[2]),
                'l': float(row[3]),
                'c': float(row[4]),
                'v': float(row[5]),
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
def swing_low(candles, i, lookback):
    if i < lookback: return None
    return min(c['l'] for c in candles[i-lookback:i])

def swing_high(candles, i, lookback):
    if i < lookback: return None
    return max(c['h'] for c in candles[i-lookback:i])

def find_bullish_fvg(candles, i):
    if i < 2: return None
    gap_bot = candles[i-2]['h']
    gap_top = candles[i]['l']
    if gap_top > gap_bot and (gap_top - gap_bot) / gap_bot >= FVG_MIN_GAP:
        return (gap_top, gap_bot)
    return None

def find_bearish_fvg(candles, i):
    if i < 2: return None
    gap_top = candles[i-2]['l']
    gap_bot = candles[i]['h']
    if gap_top > gap_bot and (gap_top - gap_bot) / gap_bot >= FVG_MIN_GAP:
        return (gap_top, gap_bot)
    return None

# ── SIGNAL EXTRACTION ─────────────────────────────────────
def extract_signals(symbol, candles):
    signals = []
    warmup = SWING_LOOKBACK + 3

    for i in range(warmup, len(candles)):
        si = i - 1
        signal_bar  = candles[si]
        entry_bar   = candles[i]
        entry_price = entry_bar['o']

        s_low  = swing_low(candles, si, SWING_LOOKBACK)
        s_high = swing_high(candles, si, SWING_LOOKBACK)
        if s_low is None or s_high is None:
            continue

        long_sweep  = signal_bar['l'] < s_low  and signal_bar['c'] > s_low
        short_sweep = signal_bar['h'] > s_high and signal_bar['c'] < s_high

        bull_fvg = bear_fvg = None

        if long_sweep:
            for fi in range(si, max(si - 20, 2), -1):
                fvg = find_bullish_fvg(candles, fi)
                if fvg: bull_fvg = fvg; break

        if short_sweep:
            for fi in range(si, max(si - 20, 2), -1):
                fvg = find_bearish_fvg(candles, fi)
                if fvg: bear_fvg = fvg; break

        if long_sweep and bull_fvg:
            gap_top, gap_bot = bull_fvg
            if entry_price <= gap_top * 1.002:
                signals.append({
                    'symbol'      : symbol,
                    'side'        : 'LONG',
                    'entry_ts'    : entry_bar['ts'],
                    'entry_price' : entry_price,
                    'tp'          : entry_price * (1 + TP_PCT),
                    'sl'          : entry_price * (1 - SL_PCT),
                    'entry_bar_i' : i,
                })

        elif short_sweep and bear_fvg:
            gap_top, gap_bot = bear_fvg
            if entry_price >= gap_bot * 0.998:
                signals.append({
                    'symbol'      : symbol,
                    'side'        : 'SHORT',
                    'entry_ts'    : entry_bar['ts'],
                    'entry_price' : entry_price,
                    'tp'          : entry_price * (1 - TP_PCT),
                    'sl'          : entry_price * (1 + SL_PCT),
                    'entry_bar_i' : i,
                })

    return signals

# ── PORTFOLIO SIMULATION ──────────────────────────────────
def simulate_with_sizing(coin_data, raw_signals):
    raw_signals.sort(key=lambda x: x['entry_ts'])

    equity   = CAPITAL
    open_pos = []
    closed   = []

    # Build full event timeline (ts, symbol, bar_idx)
    all_events = []
    for sym, candles in coin_data.items():
        for idx, c in enumerate(candles):
            all_events.append((c['ts'], sym, idx))
    all_events.sort()

    sig_by_ts_sym = defaultdict(list)
    for s in raw_signals:
        sig_by_ts_sym[(s['entry_ts'], s['symbol'])].append(s)

    for (ts, sym, bar_idx) in all_events:
        candles = coin_data[sym]
        bar     = candles[bar_idx]

        # ── Close positions for this symbol ──────────────
        still_open = []
        for pos in open_pos:
            if pos['symbol'] != sym:
                still_open.append(pos)
                continue

            age          = bar_idx - pos['entry_bar_i']
            close_price  = None
            result       = None

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
                # Leverage amplifies both gains and losses
                pnl = pos['risk_usd'] * LEVERAGE * (net_ret / (SL_PCT + FEE + SLIP))
                equity += pnl
                month_key = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m')
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

        # ── New signal at this bar ────────────────────────
        key = (ts, sym)
        if key in sig_by_ts_sym:
            for sig in sig_by_ts_sym[key]:
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
def compute_and_print_results(trades, filter_stats):
    if not trades:
        print("\n⚠️  ZERO TRADES — check filter stats:")
        print(json.dumps(dict(filter_stats), indent=2))
        save_outputs([], [], filter_stats, {})
        return

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

    # Drawdown
    eq = [CAPITAL]
    for t in trades: eq.append(eq[-1] + t['pnl'])
    peak = eq[0]; max_dd = 0
    for e in eq:
        if e > peak: peak = e
        dd = (peak - e) / peak * 100
        if dd > max_dd: max_dd = dd

    # Monthly
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

    # Streaks
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

    # Per coin
    coin_pf_list = []
    syms = set(t['symbol'] for t in trades)
    for sym in syms:
        st = [t for t in trades if t['symbol'] == sym]
        sw = sum(t['pnl'] for t in st if t['pnl'] > 0)
        sl = abs(sum(t['pnl'] for t in st if t['pnl'] <= 0))
        pf_c = sw / sl if sl > 0 else float('inf')
        wc   = len([t for t in st if t['result'] == 'WIN'])
        coin_pf_list.append({
            'symbol': sym,
            'trades': len(st),
            'wr'    : wc / len(st) * 100,
            'pnl'   : round(sum(t['pnl'] for t in st), 2),
            'pf'    : round(pf_c, 3),
        })
    coin_pf_list.sort(key=lambda x: x['pf'], reverse=True)

    # Print
    print("\n" + "="*60)
    print("AGGREGATE RESULTS")
    print("="*60)
    print(f"Total Trades   : {total}")
    print(f"Win Rate       : {wr:.1f}%")
    print(f"Profit Factor  : {pf:.3f}")
    print(f"Net PnL        : ${net_pnl:,.2f}")
    print(f"Max Drawdown   : {max_dd:.1f}%")
    print(f"Sharpe (mo)    : {sharpe:.2f}")
    print(f"Sortino (mo)   : {sortino:.2f}")
    print(f"Avg Win        : ${avg_win:.2f}")
    print(f"Avg Loss       : ${avg_loss:.2f}")
    print(f"Max Win Streak : {streak_max_w}")
    print(f"Max Loss Streak: {streak_max_l}")
    print(f"Wins/Losses/TO : {len(wins)}/{len(losses)}/{len(timeout)}")
    print(f"Longs          : {len(longs)} trades, WR {lwr:.1f}%")
    print(f"Shorts         : {len(shorts)} trades, WR {swr:.1f}%")
    print(f"Leverage       : {LEVERAGE}x")
    print(f"Target         : PF ≥ 1.5 | WR ≥ 42% → {'✅ PASS' if pf >= 1.5 and wr >= 42 else '❌ FAIL'}")

    print("\nPER-COIN (sorted by PF):")
    print(f"{'Symbol':<22} {'Trades':>6} {'WR%':>6} {'PnL':>10} {'PF':>6}")
    print("-"*55)
    for c in coin_pf_list:
        pf_str = f"{c['pf']:.3f}" if c['pf'] != float('inf') else "inf"
        print(f"{c['symbol']:<22} {c['trades']:>6} {c['wr']:>5.1f}% {c['pnl']:>10.2f} {pf_str:>6}")

    print("\nMONTHLY PnL:")
    max_abs = max(abs(v) for v in mv) if mv else 1
    for mo in sorted(monthly.keys()):
        sign = '+' if monthly[mo] >= 0 else ''
        print(f"  {mo}: {sign}${monthly[mo]:,.2f}")

    print("\nFILTER STATS:")
    tb = filter_stats.get('total_bars', 1)
    for k, v in filter_stats.items():
        if k == 'total_bars': continue
        print(f"  {k:<25}: {v:>8,}  ({v/tb*100:.1f}%)")

    save_outputs(trades, coin_pf_list, filter_stats, monthly)

def save_outputs(trades, coin_pf_list, filter_stats, monthly):
    lines = [
        "FVG + LIQUIDITY SWEEP BACKTEST SUMMARY",
        f"Generated : {datetime.now(timezone.utc).isoformat()}",
        f"Leverage  : {LEVERAGE}x",
        f"TP        : {TP_PCT*100:.1f}%  SL: {SL_PCT*100:.1f}%",
        f"Timeframe : {INTERVAL}",
        "",
    ]
    if trades:
        wins  = [t for t in trades if t['result'] == 'WIN']
        total = len(trades)
        net   = sum(t['pnl'] for t in trades)
        gw    = sum(t['pnl'] for t in wins)
        gl    = abs(sum(t['pnl'] for t in trades if t['pnl'] <= 0))
        pf    = gw / gl if gl > 0 else 0
        lines += [
            f"Trades    : {total}",
            f"Win Rate  : {len(wins)/total*100:.1f}%",
            f"PF        : {pf:.3f}",
            f"Net PnL   : ${net:,.2f}",
            f"Result    : {'PASS' if pf >= 1.5 and len(wins)/total*100 >= 42 else 'FAIL'}",
        ]
    else:
        lines.append("ZERO TRADES")

    with open("backtest_summary.txt", "w") as f:
        f.write("\n".join(lines))

    report = {
        "meta": {
            "strategy" : "FVG_LiquiditySweep_5m",
            "leverage" : LEVERAGE,
            "tp_pct"   : TP_PCT,
            "sl_pct"   : SL_PCT,
            "interval" : INTERVAL,
            "period"   : f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "coins"    : len(COINS),
            "max_pos"  : MAX_POS,
            "risk_pct" : RISK_PCT,
        },
        "aggregate"    : {},
        "per_coin"     : coin_pf_list,
        "filter_stats" : dict(filter_stats),
        "monthly"      : {k: round(v, 2) for k, v in monthly.items()},
        "trades"       : trades[:500],
    }

    if trades:
        wins  = [t for t in trades if t['result'] == 'WIN']
        total = len(trades)
        net   = sum(t['pnl'] for t in trades)
        gw    = sum(t['pnl'] for t in wins)
        gl    = abs(sum(t['pnl'] for t in trades if t['pnl'] <= 0))
        pf    = gw / gl if gl > 0 else 0
        report["aggregate"] = {
            "total_trades"  : total,
            "win_rate"      : round(len(wins)/total*100, 2),
            "profit_factor" : round(pf, 3),
            "net_pnl"       : round(net, 2),
            "max_dd_pct"    : 0,
            "sharpe"        : 0,
        }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n✅ Saved: backtest_summary.txt + backtest_report.json")

# ── MAIN ──────────────────────────────────────────────────
def run_portfolio():
    print(f"\n{'='*60}")
    print(f"FVG + LIQUIDITY SWEEP — {INTERVAL} — {len(COINS)} coins — {LEVERAGE}x leverage")
    print(f"Period : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print(f"TP: {TP_PCT*100:.1f}%  SL: {SL_PCT*100:.1f}%  MaxPos: {MAX_POS}")
    print(f"{'='*60}\n")

    print("Phase 1: Fetching data...")
    coin_data = {}
    for sym in COINS:
        candles = fetch_symbol(sym)
        if len(candles) < 100:
            print(f"  SKIP {sym} — {len(candles)} candles")
            continue
        coin_data[sym] = candles
        print(f"  OK {sym}: {len(candles)} candles")

    if not coin_data:
        print("FATAL: No data fetched.")
        return

    print(f"\nFetched {len(coin_data)}/{len(COINS)} coins")
    print("\nPhase 2: Extracting signals...")

    all_signals   = []
    filter_totals = defaultdict(int)

    for sym, candles in coin_data.items():
        sigs = extract_signals(sym, candles)
        all_signals.extend(sigs)
        # Count filter stats per coin
        warmup = SWING_LOOKBACK + 3
        filter_totals['total_bars']    += len(candles)
        filter_totals['warmup']        += min(warmup, len(candles))
        filter_totals['signals_fired'] += len(sigs)

    print(f"  Total signals: {len(all_signals)}")

    print("\nPhase 3: Portfolio simulation...")
    trades = simulate_with_sizing(coin_data, all_signals)

    print(f"  Closed trades: {len(trades)}")
    print("\nPhase 4: Results...")
    compute_and_print_results(trades, filter_totals)

if __name__ == "__main__":
    run_portfolio()
