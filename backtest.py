"""
GMax V1 Backtest — 4 Variants (VAR_D / VAR_A / VAR_B / VAR_C)
Strategy: EMA50 slope + EMA9/21 crossover + ADX(14) >= 22 — 15m timeframe
Leverage: 5x | Capital: $10,000 | Risk: 0.75% per trade
Coins: 117-coin Universe list | Period: 2 years (or full available history)
Workers: 20 parallel processes
Data: data.binance.vision monthly archives
"""

import csv, io, json, math, os, sys, time, urllib.request, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from itertools import product

# ── Variants ──────────────────────────────────────────────────────────────────
VARIANTS = {
    'VAR_D': {'tp': 0.030, 'sl': 0.150, 'label': 'VAR_D (TP 3.0% / SL 15.0%)'},
    'VAR_A': {'tp': 0.010, 'sl': 0.100, 'label': 'VAR_A (TP 1.0% / SL 10.0%)'},
    'VAR_B': {'tp': 0.005, 'sl': 0.080, 'label': 'VAR_B (TP 0.5% / SL 8.0%)'},
    'VAR_C': {'tp': 0.003, 'sl': 0.080, 'label': 'VAR_C (TP 0.3% / SL 8.0%)'},
}

# ── Universe — 117 coins ───────────────────────────────────────────────────────
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

# ── Global Settings ───────────────────────────────────────────────────────────
CAPITAL        = 10_000.0
RISK_PCT       = 0.0075          # 0.75% risk per trade
LEVERAGE       = 5
FEE_RATE       = 0.0005          # 0.05% per side
SLIP_RATE      = 0.0002          # 0.02% per side
MAX_POSITIONS  = 6
MAX_HOLD_BARS  = 960             # 10 days at 15m
INTERVAL       = '15m'
WORKERS        = 20

# ── Date range: last 2 years from today ───────────────────────────────────────
_NOW      = datetime.now(timezone.utc)
END_YEAR  = _NOW.year
END_MONTH = _NOW.month - 1 if _NOW.month > 1 else 12
END_YEAR  = END_YEAR if _NOW.month > 1 else END_YEAR - 1
START_DT  = datetime(_NOW.year - 2, _NOW.month, 1, tzinfo=timezone.utc)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _months_in_range():
    """Yield (year, month) from START_DT up to (END_YEAR, END_MONTH) inclusive."""
    y, m = START_DT.year, START_DT.month
    while (y, m) <= (END_YEAR, END_MONTH):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def _fetch_month(symbol, year, month):
    """Download one monthly kline zip. Returns list of raw rows or None on 404."""
    ym = f"{year}-{month:02d}"
    url = (
        f"https://data.binance.vision/data/futures/um/monthly/klines"
        f"/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
    except Exception:
        return None

    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f))
                for row in reader:
                    if not row or not row[0].isdigit():
                        continue
                    rows.append(row)
    except Exception:
        return None
    return rows if rows else None

def _parse_rows(rows):
    """Parse CSV rows → (open_time_ms, open, high, low, close) tuples."""
    out = []
    for row in rows:
        try:
            ts = int(row[0])
            if ts > 10**14:          # microseconds guard
                ts //= 1000
            o = float(row[1]); h = float(row[2])
            l = float(row[3]); c = float(row[4])
            out.append((ts, o, h, l, c))
        except Exception:
            continue
    return out

def fetch_symbol_data(symbol):
    """
    Fetch all available monthly klines for `symbol` within the 2-year window.
    Returns sorted list of (ts, open, high, low, close).
    Falls back gracefully — coins with fewer months of history just get fewer bars.
    """
    all_bars = []
    for year, month in _months_in_range():
        rows = _fetch_month(symbol, year, month)
        if rows is None:
            continue
        all_bars.extend(_parse_rows(rows))

    if not all_bars:
        return []

    # Deduplicate and sort
    seen = {}
    for bar in all_bars:
        seen[bar[0]] = bar
    return sorted(seen.values(), key=lambda x: x[0])

# ── Indicators (pure Python, no numpy) ───────────────────────────────────────
def _ema(values, period):
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def _adx(highs, lows, closes, period=14):
    """Returns (adx, +DI, -DI) on the full series — last value used."""
    n = len(closes)
    if n < period * 3:
        return 0.0, 0.0, 0.0

    pdm_list, mdm_list, tr_list = [], [], []
    for i in range(1, n):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_list.append(up   if up > down and up > 0   else 0.0)
        mdm_list.append(down if down > up and down > 0 else 0.0)
        tr_list.append(max(
            highs[i] - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1]),
        ))

    def _wilder(lst, p):
        if len(lst) < p:
            return []
        r = [sum(lst[:p])]
        for x in lst[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r

    atr_w = _wilder(tr_list, period)
    pdm_w = _wilder(pdm_list, period)
    mdm_w = _wilder(mdm_list, period)

    if not atr_w:
        return 0.0, 0.0, 0.0

    pdi = [100 * p / t if t else 0 for p, t in zip(pdm_w, atr_w)]
    mdi = [100 * m / t if t else 0 for m, t in zip(mdm_w, atr_w)]
    dx  = [100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(pdi, mdi)]

    if len(dx) < period:
        return 0.0, pdi[-1] if pdi else 0.0, mdi[-1] if mdi else 0.0

    adx_val = sum(dx[:period]) / period
    for d in dx[period:]:
        adx_val = (adx_val * (period - 1) + d) / period

    adx_val = max(0.0, min(100.0, adx_val))
    return adx_val, pdi[-1], mdi[-1]

# ── Signal (Variant G logic) ──────────────────────────────────────────────────
def check_signal(closes, highs, lows, i):
    """
    Evaluate G Max signal on bar index i (must be a CLOSED bar).
    Returns ('buy'|'sell'|None, reason_str, reject_stage)
    reject_stage: 0=warmup, 1=slope, 2=cross, 3=adx_dir, 4=signal
    """
    if i < 69:          # need 70 bars minimum
        return None, 'warmup', 0

    e9  = _ema(closes[:i+1], 9)
    e21 = _ema(closes[:i+1], 21)
    e50 = _ema(closes[:i+1], 50)

    # Filter 1: EMA50 slope over last 10 bars
    if i < 10:
        return None, 'warmup', 0

    slope_pct  = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05

    if not trend_up and not trend_down:
        return None, 'no_trend', 1

    # Filter 2: EMA9/21 cross on this exact closed bar
    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]

    if not (crossed_up or crossed_down):
        return None, 'no_cross', 2

    # Direction must match trend
    if trend_up and not crossed_up:
        return None, 'dir_mismatch', 2
    if trend_down and not crossed_down:
        return None, 'dir_mismatch', 2

    # Filter 3: ADX >= 22
    adx_val, _, _ = _adx(highs[:i+1], lows[:i+1], closes[:i+1], 14)
    if adx_val < 22:
        return None, 'adx_low', 3

    sig = 'buy' if crossed_up else 'sell'
    return sig, 'signal', 4

# ── Per-symbol backtest ───────────────────────────────────────────────────────
def backtest_symbol(args):
    """
    Run all 4 variants for a single symbol.
    Returns dict: symbol -> variant_key -> result_dict
    """
    symbol, bars = args

    if len(bars) < 100:
        return symbol, {vk: _empty_result(symbol, vk, 0, 0) for vk in VARIANTS}

    ts_list  = [b[0] for b in bars]
    opens    = [b[1] for b in bars]
    highs    = [b[2] for b in bars]
    lows     = [b[3] for b in bars]
    closes   = [b[4] for b in bars]
    n        = len(closes)

    results = {}

    for vk, vcfg in VARIANTS.items():
        TP_PCT = vcfg['tp']
        SL_PCT = vcfg['sl']

        equity    = CAPITAL
        pos       = None           # current open position for this symbol
        trades    = []
        gross_profit = gross_loss = 0.0

        # Rejection counters
        rej = {'warmup': 0, 'no_trend': 0, 'no_cross': 0,
               'dir_mismatch': 0, 'adx_low': 0, 'max_pos': 0,
               'in_trade': 0, 'signals': 0}
        candles_scanned = 0

        for i in range(1, n - 1):
            # i is the last closed bar; i+1 is the bar we'd enter on open
            candles_scanned += 1

            # Position management: check TP/SL/maxhold on current candle
            if pos is not None:
                c_high = highs[i]
                c_low  = lows[i]
                bars_held = i - pos['entry_bar']

                closed_trade = None
                if pos['side'] == 'buy':
                    if c_high >= pos['tp']:
                        closed_trade = ('tp', pos['tp'])
                    elif c_low <= pos['sl']:
                        closed_trade = ('sl', pos['sl'])
                    elif bars_held >= MAX_HOLD_BARS:
                        closed_trade = ('timeout', closes[i])
                else:  # sell
                    if c_low <= pos['tp']:
                        closed_trade = ('tp', pos['tp'])
                    elif c_high >= pos['sl']:
                        closed_trade = ('sl', pos['sl'])
                    elif bars_held >= MAX_HOLD_BARS:
                        closed_trade = ('timeout', closes[i])

                if closed_trade is not None:
                    reason_exit, exit_px = closed_trade
                    entry_px = pos['entry_px']
                    qty      = pos['qty']

                    if pos['side'] == 'buy':
                        raw_pnl = (exit_px - entry_px) * qty
                    else:
                        raw_pnl = (entry_px - exit_px) * qty

                    fee_cost  = (entry_px + exit_px) * qty * FEE_RATE
                    slip_cost = (entry_px + exit_px) * qty * SLIP_RATE
                    net_pnl   = raw_pnl - fee_cost - slip_cost

                    equity += net_pnl
                    if net_pnl > 0:
                        gross_profit += net_pnl
                    else:
                        gross_loss   += abs(net_pnl)

                    trades.append({
                        'symbol': symbol,
                        'side'  : pos['side'],
                        'entry' : entry_px,
                        'exit'  : exit_px,
                        'pnl'   : round(net_pnl, 6),
                        'reason': reason_exit,
                        'bars'  : bars_held,
                        'entry_ts': ts_list[pos['entry_bar']],
                        'exit_ts' : ts_list[i],
                    })
                    pos = None

            # Signal check on closed bar i
            if pos is not None:
                rej['in_trade'] += 1
                continue

            sig, reason, stage = check_signal(closes, highs, lows, i)

            if sig is None:
                rej[reason] += 1
                continue

            # Signal fired — entry on open of next bar (i+1)
            if i + 1 >= n:
                break

            entry_px   = opens[i + 1]
            risk_amt   = equity * RISK_PCT
            notional   = risk_amt / (TP_PCT + FEE_RATE * 2 + SLIP_RATE * 2) * LEVERAGE
            qty        = notional / entry_px

            if sig == 'buy':
                tp_px = entry_px * (1 + TP_PCT)
                sl_px = entry_px * (1 - SL_PCT)
            else:
                tp_px = entry_px * (1 - TP_PCT)
                sl_px = entry_px * (1 + SL_PCT)

            pos = {
                'side'      : sig,
                'entry_px'  : entry_px,
                'tp'        : tp_px,
                'sl'        : sl_px,
                'qty'       : qty,
                'entry_bar' : i + 1,
            }
            rej['signals'] += 1

        # Close any open position at end of data
        if pos is not None:
            exit_px  = closes[-1]
            entry_px = pos['entry_px']
            qty      = pos['qty']
            if pos['side'] == 'buy':
                raw_pnl = (exit_px - entry_px) * qty
            else:
                raw_pnl = (entry_px - exit_px) * qty
            fee_cost  = (entry_px + exit_px) * qty * FEE_RATE
            slip_cost = (entry_px + exit_px) * qty * SLIP_RATE
            net_pnl   = raw_pnl - fee_cost - slip_cost
            equity   += net_pnl
            if net_pnl > 0:
                gross_profit += net_pnl
            else:
                gross_loss   += abs(net_pnl)
            trades.append({
                'symbol': symbol, 'side': pos['side'],
                'entry': entry_px, 'exit': exit_px,
                'pnl': round(net_pnl, 6), 'reason': 'end_of_data',
                'bars': n - 1 - pos['entry_bar'],
                'entry_ts': ts_list[pos['entry_bar']], 'exit_ts': ts_list[-1],
            })

        results[vk] = _build_result(
            symbol, vk, trades, gross_profit, gross_loss,
            equity - CAPITAL, candles_scanned, rej, len(bars)
        )

    return symbol, results


def _empty_result(symbol, vk, candles, bars):
    return {
        'symbol': symbol, 'variant': vk,
        'trades': [], 'total': 0, 'wins': 0, 'losses': 0,
        'win_rate': 0.0, 'profit_factor': 0.0,
        'net_pnl': 0.0, 'gross_profit': 0.0, 'gross_loss': 0.0,
        'avg_win': 0.0, 'avg_loss': 0.0,
        'candles_scanned': candles, 'bars_available': bars,
        'rejections': {},
    }


def _build_result(symbol, vk, trades, gp, gl, net_pnl, candles_scanned, rej, bars_available):
    wins   = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] < 0]
    total  = len(trades)
    nw, nl = len(wins), len(losses)
    wr     = nw / total * 100 if total else 0.0
    pf     = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)
    avg_w  = sum(t['pnl'] for t in wins)  / nw if nw else 0.0
    avg_l  = sum(t['pnl'] for t in losses)/ nl if nl else 0.0

    return {
        'symbol'        : symbol,
        'variant'       : vk,
        'trades'        : trades,
        'total'         : total,
        'wins'          : nw,
        'losses'        : nl,
        'win_rate'      : round(wr, 2),
        'profit_factor' : round(pf, 4) if pf != float('inf') else 'inf',
        'net_pnl'       : round(net_pnl, 4),
        'gross_profit'  : round(gp, 4),
        'gross_loss'    : round(gl, 4),
        'avg_win'       : round(avg_w, 4),
        'avg_loss'      : round(avg_l, 4),
        'candles_scanned': candles_scanned,
        'bars_available' : bars_available,
        'rejections'    : rej,
    }

# ── Portfolio-level simulation ────────────────────────────────────────────────
def run_portfolio_simulation(all_symbol_results):
    """
    Re-run a single portfolio pass per variant respecting MAX_POSITIONS = 6.
    Merges all trades across symbols, sorts by entry_ts, enforces the cap.
    Returns per-variant aggregate stats.
    """
    portfolio_results = {}

    for vk in VARIANTS:
        # Collect all trades across all symbols for this variant
        all_trades = []
        for sym_data in all_symbol_results.values():
            if vk not in sym_data:
                continue
            for t in sym_data[vk]['trades']:
                all_trades.append(dict(t))

        # Sort by entry timestamp
        all_trades.sort(key=lambda t: t['entry_ts'])

        equity      = CAPITAL
        open_count  = 0
        accepted    = []
        skipped_cap = 0
        open_positions = []   # list of exit_ts for currently open trades

        for t in all_trades:
            # Remove positions that have already closed
            open_positions = [ep for ep in open_positions if ep > t['entry_ts']]
            open_count = len(open_positions)

            if open_count >= MAX_POSITIONS:
                skipped_cap += 1
                continue

            open_positions.append(t['exit_ts'])
            equity += t['pnl']
            accepted.append(t)

        # Aggregate stats
        wins   = [t for t in accepted if t['pnl'] > 0]
        losses = [t for t in accepted if t['pnl'] <= 0]
        gp     = sum(t['pnl'] for t in wins)
        gl     = sum(abs(t['pnl']) for t in losses)
        total  = len(accepted)
        nw, nl = len(wins), len(losses)
        wr     = nw / total * 100 if total else 0.0
        pf     = gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)

        # Drawdown
        running = 0.0; peak = 0.0; max_dd = 0.0; max_dd_pct = 0.0
        for t in accepted:
            running += t['pnl']
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = (dd / (CAPITAL + peak)) * 100

        # Monthly PnL
        monthly = {}
        for t in accepted:
            dt  = datetime.fromtimestamp(t['exit_ts'] / 1000, tz=timezone.utc)
            key = f"{dt.year}-{dt.month:02d}"
            monthly[key] = monthly.get(key, 0.0) + t['pnl']

        # Long/short split
        longs  = [t for t in accepted if t['side'] == 'buy']
        shorts = [t for t in accepted if t['side'] == 'sell']
        long_wr  = len([t for t in longs  if t['pnl'] > 0]) / len(longs)  * 100 if longs  else 0.0
        short_wr = len([t for t in shorts if t['pnl'] > 0]) / len(shorts) * 100 if shorts else 0.0

        avg_w = sum(t['pnl'] for t in wins)   / nw if nw else 0.0
        avg_l = sum(t['pnl'] for t in losses) / nl if nl else 0.0

        portfolio_results[vk] = {
            'total_trades'  : total,
            'wins'          : nw,
            'losses'        : nl,
            'win_rate'      : round(wr, 2),
            'profit_factor' : round(pf, 4) if pf != float('inf') else 'inf',
            'net_pnl'       : round(equity - CAPITAL, 4),
            'gross_profit'  : round(gp, 4),
            'gross_loss'    : round(gl, 4),
            'max_drawdown'  : round(max_dd, 4),
            'max_dd_pct'    : round(max_dd_pct, 2),
            'avg_win'       : round(avg_w, 6),
            'avg_loss'      : round(avg_l, 6),
            'longs'         : len(longs),
            'shorts'        : len(shorts),
            'long_wr'       : round(long_wr, 2),
            'short_wr'      : round(short_wr, 2),
            'skipped_cap'   : skipped_cap,
            'monthly_pnl'   : {k: round(v, 4) for k, v in sorted(monthly.items())},
            'accepted_trades': accepted,
        }

    return portfolio_results

# ── Rejection summary ─────────────────────────────────────────────────────────
def aggregate_rejections(all_symbol_results, vk):
    totals = {
        'warmup': 0, 'no_trend': 0, 'no_cross': 0,
        'dir_mismatch': 0, 'adx_low': 0, 'max_pos': 0,
        'in_trade': 0, 'signals': 0,
    }
    total_scanned = 0
    for sym_data in all_symbol_results.values():
        if vk not in sym_data:
            continue
        r = sym_data[vk].get('rejections', {})
        for k in totals:
            totals[k] += r.get(k, 0)
        total_scanned += sym_data[vk].get('candles_scanned', 0)
    return totals, total_scanned

# ── Report writer ─────────────────────────────────────────────────────────────
def write_report(all_symbol_results, portfolio):
    lines = []
    lines.append("=" * 72)
    lines.append("  GMax V1 — Backtest Report")
    lines.append(f"  Period : 2 years ending {END_YEAR}-{END_MONTH:02d}")
    lines.append(f"  Coins  : {len(COINS)} (Universe list)")
    lines.append(f"  Capital: ${CAPITAL:,.0f} | Risk: {RISK_PCT*100:.2f}% | Leverage: {LEVERAGE}x")
    lines.append(f"  Fees   : {FEE_RATE*100:.3f}%/side | Slippage: {SLIP_RATE*100:.3f}%/side")
    lines.append(f"  Max Positions: {MAX_POSITIONS} | Max Hold: {MAX_HOLD_BARS} bars")
    lines.append("=" * 72)

    for vk, vcfg in VARIANTS.items():
        p   = portfolio[vk]
        rej, total_scanned = aggregate_rejections(all_symbol_results, vk)
        usable = p['profit_factor'] != 'inf' and p['profit_factor'] >= 1.5 and p['win_rate'] >= 42.0

        lines.append("")
        lines.append("─" * 72)
        lines.append(f"  VARIANT: {vcfg['label']}")
        lines.append("─" * 72)

        lines.append("")
        lines.append("  ── Aggregate Results ──")
        lines.append(f"  Total Trades   : {p['total_trades']}")
        lines.append(f"  Wins / Losses  : {p['wins']} / {p['losses']}")
        lines.append(f"  Win Rate       : {p['win_rate']:.2f}%   (target ≥ 42%)")
        pf_disp = f"{p['profit_factor']:.4f}" if p['profit_factor'] != 'inf' else '∞'
        lines.append(f"  Profit Factor  : {pf_disp}   (target ≥ 1.5)")
        lines.append(f"  Net PnL        : ${p['net_pnl']:+,.2f}")
        lines.append(f"  Gross Profit   : ${p['gross_profit']:,.2f}")
        lines.append(f"  Gross Loss     : ${p['gross_loss']:,.2f}")
        lines.append(f"  Max Drawdown   : ${p['max_drawdown']:,.2f}  ({p['max_dd_pct']:.2f}%)")
        lines.append(f"  Avg Win        : ${p['avg_win']:+.4f}")
        lines.append(f"  Avg Loss       : ${p['avg_loss']:+.4f}")
        lines.append(f"  Longs          : {p['longs']}  WR {p['long_wr']:.2f}%")
        lines.append(f"  Shorts         : {p['shorts']}  WR {p['short_wr']:.2f}%")
        lines.append(f"  Skipped (cap)  : {p['skipped_cap']}  (max {MAX_POSITIONS} positions hit)")
        lines.append(f"  Verdict        : {'✅ USABLE (meets PF + WR targets)' if usable else '❌ NOT USABLE'}")

        lines.append("")
        lines.append("  ── Filter Rejection Stats ──")
        total_accounted = sum(rej.values())
        lines.append(f"  Total candles scanned : {total_scanned:,}")
        lines.append(f"  Warmup (< 70 bars)    : {rej['warmup']:,}")
        lines.append(f"  No trend (slope flat) : {rej['no_trend']:,}")
        lines.append(f"  No EMA cross          : {rej['no_cross']:,}")
        lines.append(f"  Dir mismatch (cross≠trend): {rej['dir_mismatch']:,}")
        lines.append(f"  ADX < 22              : {rej['adx_low']:,}")
        lines.append(f"  Already in trade      : {rej['in_trade']:,}")
        lines.append(f"  Signals fired         : {rej['signals']:,}")
        lines.append(f"  Sum check             : {total_accounted:,}  ({'OK' if total_accounted == total_scanned else 'MISMATCH — check code'})")

        lines.append("")
        lines.append("  ── Per-Coin Table (sorted by Profit Factor) ──")
        lines.append(f"  {'Symbol':<22} {'Trades':>6} {'WR%':>7} {'PF':>7} {'Net PnL':>10} {'Bars':>7}")
        lines.append("  " + "-" * 60)

        coin_rows = []
        for sym, sym_data in all_symbol_results.items():
            if vk not in sym_data:
                continue
            r = sym_data[vk]
            if r['total'] == 0:
                coin_rows.append((sym, 0, 0.0, 0.0, 0.0, r['bars_available']))
                continue
            pf_val = r['profit_factor'] if r['profit_factor'] != 'inf' else 9999.0
            coin_rows.append((sym, r['total'], r['win_rate'], pf_val, r['net_pnl'], r['bars_available']))

        coin_rows.sort(key=lambda x: x[3], reverse=True)
        for sym, tot, wr_c, pf_c, npnl, bars_av in coin_rows:
            pf_str = f"{pf_c:.4f}" if pf_c < 9999 else "∞"
            lines.append(f"  {sym:<22} {tot:>6} {wr_c:>7.2f} {pf_str:>7} {npnl:>+10.2f} {bars_av:>7}")

        lines.append("")
        lines.append("  ── Monthly PnL ──")
        for ym, mpnl in p['monthly_pnl'].items():
            bar = "█" * min(30, int(abs(mpnl) / max(1, max(abs(v) for v in p['monthly_pnl'].values())) * 30))
            sign = "+" if mpnl >= 0 else "-"
            lines.append(f"  {ym}  {sign}${abs(mpnl):>8.2f}  {bar}")

    lines.append("")
    lines.append("=" * 72)
    lines.append("  END OF REPORT")
    lines.append("=" * 72)
    return "\n".join(lines)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"[GMax Backtest] Fetching data for {len(COINS)} coins | {WORKERS} workers")
    print(f"[GMax Backtest] Period: {START_DT.strftime('%Y-%m')} → {END_YEAR}-{END_MONTH:02d}")

    # Phase 1: Fetch all data
    print("[Phase 1] Downloading kline data...")
    symbol_bars = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol_data, sym): sym for sym in COINS}
        done = 0
        for fut in as_completed(futs):
            sym  = futs[fut]
            bars = fut.result()
            symbol_bars[sym] = bars
            done += 1
            status = f"{len(bars)} bars" if bars else "NO DATA"
            print(f"  [{done:>3}/{len(COINS)}] {sym:<26} {status}")

    live_symbols = {s: b for s, b in symbol_bars.items() if len(b) >= 100}
    dead_symbols = [s for s, b in symbol_bars.items() if len(b) < 100]
    print(f"\n[Phase 1] Done. {len(live_symbols)} tradeable, {len(dead_symbols)} skipped (< 100 bars)")
    if dead_symbols:
        print(f"  Skipped: {', '.join(dead_symbols)}")

    if not live_symbols:
        print("ERROR: No data fetched — check network / data.binance.vision access")
        sys.exit(1)

    # Phase 2: Backtest per symbol (all variants in one worker call)
    print(f"\n[Phase 2] Running backtests across {len(live_symbols)} symbols × 4 variants...")
    all_symbol_results = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(backtest_symbol, (sym, bars)): sym
                for sym, bars in live_symbols.items()}
        done = 0
        for fut in as_completed(futs):
            sym, res = fut.result()
            all_symbol_results[sym] = res
            done += 1
            # Quick summary: VAR_D profit factor
            vd = res.get('VAR_D', {})
            pf_str = f"PF={vd.get('profit_factor','?')}" if vd.get('total', 0) > 0 else "0 trades"
            print(f"  [{done:>3}/{len(live_symbols)}] {sym:<26} VAR_D {pf_str}")

    # Phase 3: Portfolio simulation (enforce MAX_POSITIONS across symbols)
    print("\n[Phase 3] Portfolio simulation (max 6 concurrent positions)...")
    portfolio = run_portfolio_simulation(all_symbol_results)

    # Phase 4: Write outputs
    print("\n[Phase 4] Writing results...")
    report_txt = write_report(all_symbol_results, portfolio)

    with open("backtest_summary.txt", "w") as f:
        f.write(report_txt)
    print(report_txt)

    # JSON report
    json_report = {
        'meta': {
            'capital'      : CAPITAL,
            'risk_pct'     : RISK_PCT,
            'leverage'     : LEVERAGE,
            'fee_rate'     : FEE_RATE,
            'slip_rate'    : SLIP_RATE,
            'max_positions': MAX_POSITIONS,
            'max_hold_bars': MAX_HOLD_BARS,
            'interval'     : INTERVAL,
            'coins'        : len(COINS),
            'period_start' : START_DT.strftime('%Y-%m'),
            'period_end'   : f"{END_YEAR}-{END_MONTH:02d}",
        },
        'variants': {}
    }

    for vk in VARIANTS:
        p = portfolio[vk]
        per_coin = []
        for sym, sym_data in all_symbol_results.items():
            if vk in sym_data:
                r = sym_data[vk]
                per_coin.append({
                    'symbol'       : sym,
                    'total'        : r['total'],
                    'wins'         : r['wins'],
                    'losses'       : r['losses'],
                    'win_rate'     : r['win_rate'],
                    'profit_factor': r['profit_factor'],
                    'net_pnl'      : r['net_pnl'],
                    'bars_available': r['bars_available'],
                })
        per_coin.sort(key=lambda x: (x['profit_factor'] if x['profit_factor'] != 'inf' else 9999), reverse=True)

        rej, ts = aggregate_rejections(all_symbol_results, vk)
        json_report['variants'][vk] = {
            'aggregate'  : {k: v for k, v in p.items() if k != 'accepted_trades'},
            'per_coin'   : per_coin,
            'filter_stats': {'total_scanned': ts, **rej},
            'trades'     : p['accepted_trades'],
            'monthly_pnl': p['monthly_pnl'],
        }

    with open("backtest_report.json", "w") as f:
        json.dump(json_report, f, indent=2)

    print("\n[Done] backtest_summary.txt + backtest_report.json written.")

if __name__ == '__main__':
    main()
