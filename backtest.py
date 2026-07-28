#!/usr/bin/env python3
"""
Crypto 15M/30M Multi-Layer Confluence Strategy (v2 Tiered) - Backtest Engine
Standard Library-only Python 3.11 implementation with Parallel Worker Threads.

Repository: JizzyBx/Backtestyml
"""

import csv
import datetime
import io
import json
import math
import os
import ssl
import sys
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================================================================
# 1. CONFIGURATION & STRATEGY SPEC
# ==============================================================================

STARTING_CAPITAL = 10000.0
RISK_PER_TRADE = 0.0075  # 0.75% equity risk per trade (per handoff default)
FEE_TAKER = 0.0005       # 0.05% taker fee per side
MAX_CONCURRENT_POSITIONS = 6
MAX_WORKERS = 10         # 10 Parallel Worker Threads for fast data fetching

# Date range to backtest (Binance Futures Monthly Archives)
BACKTEST_YEARS_MONTHS = [
    (2025, 10), (2025, 11), (2025, 12),
    (2026, 1), (2026, 2), (2026, 3), (2026, 4), (2026, 5), (2026, 6)
]

# Standard 50 Coins mapped to Tiers & Specs
COIN_TIERS = {
    # Category A: Mega-Cap Majors
    'BTCUSDT': 'A', 'ETHUSDT': 'A', 'SOLUSDT': 'A', 'BNBUSDT': 'A', 'XRPUSDT': 'A',
    
    # Category B/C: L1/L2 & DeFi Networks
    'AVAXUSDT': 'B', 'NEARUSDT': 'B', 'SUIUSDT': 'B', 'APTUSDT': 'B', 'ARBUSDT': 'B',
    'OPUSDT': 'B', 'POLUSDT': 'B', 'TIAUSDT': 'B', 'INJUSDT': 'B', 'SUSD': 'B',
    'SEIUSDT': 'B', 'ATOMUSDT': 'B', 'DOTUSDT': 'B', 'ADAUSDT': 'B', 'TONUSDT': 'B',
    'LINKUSDT': 'C', 'UNIUSDT': 'C', 'AAVEUSDT': 'C', 'RENDERUSDT': 'C', 'FETUSDT': 'C',
    'TAOUSDT': 'C', 'WLDUSDT': 'C', 'PENDLEUSDT': 'C', 'ENAUSDT': 'C', 'LDOUSDT': 'C',
    'MKRUSDT': 'C', 'JUPUSDT': 'C', 'PYTHUSDT': 'C', 'ONDOUSDT': 'C', 'LTCUSDT': 'B',
    'HBARUSDT': 'B',
    
    # Category D: High-Volatility Meme Coins
    '1000DOGEUSDT': 'D', '1000SHIBUSDT': 'D', '1000PEPEUSDT': 'D', 'WIFUSDT': 'D',
    '1000BONKUSDT': 'D', '1000FLOKIUSDT': 'D', 'POPCATUSDT': 'D', 'BOMEUSDT': 'D',
    'MEMEUSDT': 'D', 'ORDIUSDT': 'D', '1000SATSUSDT': 'D', 'MEWUSDT': 'D',
    'MYROUSDT': 'D', 'NEIROUSDT': 'D', 'TURBOUSDT': 'D', 'TRUMPUSDT': 'D'
}

TIER_PARAMS = {
    'A': {'atr_mult': 1.0, 'vol_mult': 1.3, 'stoch_low': 0.25, 'stoch_high': 0.75, 'tp1_rr': 1.0, 'tp2_rr': 2.0, 'slippage': 0.0001},
    'B': {'atr_mult': 1.2, 'vol_mult': 1.5, 'stoch_low': 0.20, 'stoch_high': 0.80, 'tp1_rr': 1.0, 'tp2_rr': 2.5, 'slippage': 0.0003},
    'C': {'atr_mult': 1.2, 'vol_mult': 1.5, 'stoch_low': 0.20, 'stoch_high': 0.80, 'tp1_rr': 1.0, 'tp2_rr': 2.5, 'slippage': 0.0003},
    'D': {'atr_mult': 1.5, 'vol_mult': 1.8, 'stoch_low': 0.15, 'stoch_high': 0.85, 'tp1_rr': 1.0, 'tp2_rr': 3.5, 'slippage': 0.0006}
}

# ==============================================================================
# 2. SYMBOL HYGIENE & DATA FETCHING
# ==============================================================================

def normalize_symbol(symbol: str) -> str:
    """Applies symbol naming corrections per handoff rules."""
    s = symbol.strip().upper()
    if s.endswith('.P'):
        s = s[:-2]
    if s == 'MATICUSDT':
        s = 'POLUSDT'
    memes_needing_prefix = ['DOGEUSDT', 'SHIBUSDT', 'PEPEUSDT', 'BONKUSDT', 'FLOKIUSDT', 'SATSUSDT']
    if s in memes_needing_prefix:
        s = '1000' + s
    return s

def fetch_monthly_klines(symbol: str, interval: str, year: int, month: int):
    """
    Downloads monthly kline zip archive directly from Binance Vision static bucket.
    """
    m_str = f"{month:02d}"
    url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{m_str}.zip"
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
            data = resp.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_filename = zf.namelist()[0]
                with zf.open(csv_filename) as f:
                    lines = [line.decode('utf-8') for line in f.readlines()]
                    reader = csv.reader(lines)
                    klines = []
                    for row in reader:
                        if not row or row[0].startswith('open_time'):
                            continue
                        open_time = int(row[0])
                        if open_time > 10**14:
                            open_time //= 1000
                        
                        klines.append({
                            'ts': open_time,
                            'open': float(row[1]),
                            'high': float(row[2]),
                            'low': float(row[3]),
                            'close': float(row[4]),
                            'volume': float(row[5])
                        })
                    return klines
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        return []

# ==============================================================================
# 3. INDICATOR CALCULATIONS (PURE PYTHON STDLIB)
# ==============================================================================

def calculate_ema(prices, span):
    alpha = 2.0 / (span + 1.0)
    ema = []
    val = prices[0]
    for p in prices:
        val = p * alpha + val * (1.0 - alpha)
        ema.append(val)
    return ema

def calculate_indicators(klines):
    """Calculates ATR, Stoch RSI, Donchian, Volume SMA, and 1H 200 EMA Proxy."""
    n = len(klines)
    if n < 850:
        return None

    closes = [k['close'] for k in klines]
    highs = [k['high'] for k in klines]
    lows = [k['low'] for k in klines]
    volumes = [k['volume'] for k in klines]

    ema_200_1h = calculate_ema(closes, span=800)

    tr = []
    for i in range(n):
        if i == 0:
            tr.append(highs[i] - lows[i])
        else:
            tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])))
    
    atr = []
    window_tr = 0.0
    for i in range(n):
        window_tr += tr[i]
        if i >= 14:
            window_tr -= tr[i-14]
            atr.append(window_tr / 14.0)
        else:
            atr.append(window_tr / (i + 1))

    vol_sma = []
    vol_sum = 0.0
    for i in range(n):
        vol_sum += volumes[i]
        if i >= 20:
            vol_sum -= volumes[i-20]
            vol_sma.append(vol_sum / 20.0)
        else:
            vol_sma.append(vol_sum / (i + 1))

    gains, losses = [0.0], [0.0]
    for i in range(1, n):
        change = closes[i] - closes[i-1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    rsi = [50.0] * n
    avg_gain = sum(gains[1:15]) / 14.0 if n >= 15 else 0.0
    avg_loss = sum(losses[1:15]) / 14.0 if n >= 15 else 0.0

    for i in range(14, n):
        avg_gain = (avg_gain * 13.0 + gains[i]) / 14.0
        avg_loss = (avg_loss * 13.0 + losses[i]) / 14.0
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    stoch_rsi = [0.5] * n
    for i in range(28, n):
        sub_rsi = rsi[i-13:i+1]
        min_r, max_r = min(sub_rsi), max(sub_rsi)
        if max_r > min_r:
            stoch_rsi[i] = (rsi[i] - min_r) / (max_r - min_r)
        else:
            stoch_rsi[i] = 0.5

    donchian_low = [0.0] * n
    donchian_high = [0.0] * n
    for i in range(40, n):
        donchian_low[i] = min(lows[i-40:i])
        donchian_high[i] = max(highs[i-40:i])

    return {
        'ema_200_1h': ema_200_1h,
        'atr': atr,
        'vol_sma': vol_sma,
        'stoch_rsi': stoch_rsi,
        'donchian_low': donchian_low,
        'donchian_high': donchian_high
    }

# ==============================================================================
# 4. PARALLEL WORKER TASK
# ==============================================================================

def process_symbol_data(sym: str):
    """
    Worker task: Fetches all months for a symbol, sorts klines, and precomputes indicators.
    Executed in parallel threads.
    """
    sym_klines = []
    for yr, mo in BACKTEST_YEARS_MONTHS:
        k_month = fetch_monthly_klines(sym, "15m", yr, mo)
        if k_month:
            sym_klines.extend(k_month)
    
    if not sym_klines:
        return sym, [], None, None
        
    sym_klines.sort(key=lambda x: x['ts'])
    ind = calculate_indicators(sym_klines)
    if not ind:
        return sym, sym_klines, None, None

    kline_map = {k['ts']: (i, k) for i, k in enumerate(sym_klines)}
    return sym, sym_klines, ind, kline_map

# ==============================================================================
# 5. SIMULATION & BACKTEST ENGINE
# ==============================================================================

def run_backtest():
    symbols = [normalize_symbol(s) for s in COIN_TIERS.keys()]
    
    print("=" * 80)
    print("STARTING BINANCE FUTURES BACKTEST — 15M/30M CONFLUENCE STRATEGY (v2 TIERED)")
    print(f"Symbols: {len(symbols)} | Shared Portfolio Equity: ${STARTING_CAPITAL:,.2f}")
    print(f"Parallel Thread Workers: {MAX_WORKERS}")
    print("=" * 80)

    all_symbol_klines = {}
    symbol_indicators = {}
    symbol_kline_map = {}
    total_downloaded_candles = 0

    print(f"\n[Phase 1] Fetching & Processing Data in Parallel ({MAX_WORKERS} Workers)...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_sym = {executor.submit(process_symbol_data, sym): sym for sym in symbols}
        
        for future in as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                s_name, klines, ind, k_map = future.result()
                if klines and ind:
                    all_symbol_klines[s_name] = klines
                    symbol_indicators[s_name] = ind
                    symbol_kline_map[s_name] = k_map
                    total_downloaded_candles += len(klines)
                    print(f" [✓] {s_name:<15}: Loaded {len(klines):,} candles & computed indicators")
                else:
                    print(f" [x] {s_name:<15}: Skipped (Insufficient data or missing archives)")
            except Exception as e:
                print(f" [!] {sym:<15}: Error during execution - {e}")

    if total_downloaded_candles == 0:
        print("\nFATAL ERROR: Zero candles fetched across all symbols!")
        print("Data source is blocked or archives are unreachable. Aborting.")
        sys.exit(1)

    print(f"\n[Phase 2] Running Portfolio Backtest across {total_downloaded_candles:,} total candles...")
    
    all_timestamps = sorted(list({k['ts'] for klines in all_symbol_klines.values() for k in klines}))

    # Portfolio State
    equity = STARTING_CAPITAL
    active_positions = []
    completed_trades = []
    
    # Filter Accounting (Rule 2c)
    filter_stats = {
        'total_scanned_bars': 0,
        'warmup_none': 0,
        'max_positions_reached': 0,
        'already_in_symbol': 0,
        'rej_layer1_trend': 0,
        'rej_layer2_value': 0,
        'rej_layer3_volume': 0,
        'rej_layer3_stoch': 0,
        'signals_executed': 0
    }

    # Time-series step
    for ts in all_timestamps:
        # 1. Manage existing positions
        for pos in active_positions[:]:
            sym = pos['symbol']
            if ts not in symbol_kline_map[sym]:
                continue
            idx, bar = symbol_kline_map[sym][ts]
            tier_p = TIER_PARAMS[pos['tier']]

            high, low, close = bar['high'], bar['low'], bar['close']
            
            if pos['side'] == 'LONG':
                if low <= pos['sl_price']:
                    exit_price = pos['sl_price'] * (1.0 - tier_p['slippage'])
                    pnl = pos['units'] * (exit_price - pos['entry_price']) - (pos['units'] * exit_price * FEE_TAKER)
                    equity += pnl
                    completed_trades.append({**pos, 'exit_ts': ts, 'exit_price': exit_price, 'pnl': pnl, 'reason': 'SL'})
                    active_positions.remove(pos)
                    continue
                elif not pos['tp1_hit'] and high >= pos['tp1_price']:
                    pos['tp1_hit'] = True
                    close_units = pos['units'] * 0.5
                    exit_price = pos['tp1_price'] * (1.0 - tier_p['slippage'])
                    pnl = close_units * (exit_price - pos['entry_price']) - (close_units * exit_price * FEE_TAKER)
                    equity += pnl
                    pos['units'] -= close_units
                    pos['sl_price'] = pos['entry_price']
                elif pos['tp1_hit'] and high >= pos['tp2_price']:
                    exit_price = pos['tp2_price'] * (1.0 - tier_p['slippage'])
                    pnl = pos['units'] * (exit_price - pos['entry_price']) - (pos['units'] * exit_price * FEE_TAKER)
                    equity += pnl
                    completed_trades.append({**pos, 'exit_ts': ts, 'exit_price': exit_price, 'pnl': pnl, 'reason': 'TP2'})
                    active_positions.remove(pos)
                    continue

            elif pos['side'] == 'SHORT':
                if high >= pos['sl_price']:
                    exit_price = pos['sl_price'] * (1.0 + tier_p['slippage'])
                    pnl = pos['units'] * (pos['entry_price'] - exit_price) - (pos['units'] * exit_price * FEE_TAKER)
                    equity += pnl
                    completed_trades.append({**pos, 'exit_ts': ts, 'exit_price': exit_price, 'pnl': pnl, 'reason': 'SL'})
                    active_positions.remove(pos)
                    continue
                elif not pos['tp1_hit'] and low <= pos['tp1_price']:
                    pos['tp1_hit'] = True
                    close_units = pos['units'] * 0.5
                    exit_price = pos['tp1_price'] * (1.0 + tier_p['slippage'])
                    pnl = close_units * (pos['entry_price'] - exit_price) - (close_units * exit_price * FEE_TAKER)
                    equity += pnl
                    pos['units'] -= close_units
                    pos['sl_price'] = pos['entry_price']
                elif pos['tp1_hit'] and low <= pos['tp2_price']:
                    exit_price = pos['tp2_price'] * (1.0 + tier_p['slippage'])
                    pnl = pos['units'] * (pos['entry_price'] - exit_price) - (pos['units'] * exit_price * FEE_TAKER)
                    equity += pnl
                    completed_trades.append({**pos, 'exit_ts': ts, 'exit_price': exit_price, 'pnl': pnl, 'reason': 'TP2'})
                    active_positions.remove(pos)
                    continue

        # 2. Check signals for new entries
        for sym in symbols:
            if sym not in symbol_indicators or ts not in symbol_kline_map[sym]:
                continue

            filter_stats['total_scanned_bars'] += 1
            idx, bar = symbol_kline_map[sym][ts]
            
            if idx < 800:
                filter_stats['warmup_none'] += 1
                continue

            if len(active_positions) >= MAX_CONCURRENT_POSITIONS:
                filter_stats['max_positions_reached'] += 1
                continue

            if any(p['symbol'] == sym for p in active_positions):
                filter_stats['already_in_symbol'] += 1
                continue

            tier = COIN_TIERS.get(sym, 'B')
            tier_p = TIER_PARAMS[tier]
            ind = symbol_indicators[sym]

            close = bar['close']
            high = bar['high']
            low = bar['low']
            vol = bar['volume']

            # Lookahead bias protection for 1H trend lookup
            ema_1h = ind['ema_200_1h'][idx - 4] if idx >= 4 else ind['ema_200_1h'][idx]
            
            is_bullish = close > ema_1h
            is_bearish = close < ema_1h

            donch_low = ind['donchian_low'][idx - 1]
            donch_high = ind['donchian_high'][idx - 1]
            at_value_long = low <= donch_low
            at_value_short = high >= donch_high

            vol_sma = ind['vol_sma'][idx]
            stoch = ind['stoch_rsi'][idx]
            vol_spike = vol >= (tier_p['vol_mult'] * vol_sma)
            stoch_oversold = stoch < tier_p['stoch_low']
            stoch_overbought = stoch > tier_p['stoch_high']

            if not is_bullish and not is_bearish:
                filter_stats['rej_layer1_trend'] += 1
                continue

            if is_bullish and not at_value_long:
                filter_stats['rej_layer2_value'] += 1
                continue
            if is_bearish and not at_value_short:
                filter_stats['rej_layer2_value'] += 1
                continue

            if not vol_spike:
                filter_stats['rej_layer3_volume'] += 1
                continue

            if is_bullish and not stoch_oversold:
                filter_stats['rej_layer3_stoch'] += 1
                continue
            if is_bearish and not stoch_overbought:
                filter_stats['rej_layer3_stoch'] += 1
                continue

            filter_stats['signals_executed'] += 1
            atr = ind['atr'][idx]
            
            risk_amt = equity * RISK_PER_TRADE
            
            if is_bullish:
                sl_price = low - (tier_p['atr_mult'] * atr)
                dist = abs(close - sl_price)
                if dist == 0: continue
                units = risk_amt / dist
                entry_price = close * (1.0 + tier_p['slippage'])
                tp1_price = entry_price + (dist * tier_p['tp1_rr'])
                tp2_price = entry_price + (dist * tier_p['tp2_rr'])

                active_positions.append({
                    'symbol': sym, 'tier': tier, 'side': 'LONG', 'entry_ts': ts,
                    'entry_price': entry_price, 'sl_price': sl_price, 'tp1_price': tp1_price,
                    'tp2_price': tp2_price, 'units': units, 'tp1_hit': False
                })

            elif is_bearish:
                sl_price = high + (tier_p['atr_mult'] * atr)
                dist = abs(sl_price - close)
                if dist == 0: continue
                units = risk_amt / dist
                entry_price = close * (1.0 - tier_p['slippage'])
                tp1_price = entry_price - (dist * tier_p['tp1_rr'])
                tp2_price = entry_price - (dist * tier_p['tp2_rr'])

                active_positions.append({
                    'symbol': sym, 'tier': tier, 'side': 'SHORT', 'entry_ts': ts,
                    'entry_price': entry_price, 'sl_price': sl_price, 'tp1_price': tp1_price,
                    'tp2_price': tp2_price, 'units': units, 'tp1_hit': False
                })

    # ==============================================================================
    # 6. GENERATE REPORT & OUTPUT ARTIFACTS
    # ==============================================================================

    total_trades = len(completed_trades)
    wins = [t for t in completed_trades if t['pnl'] > 0]
    losses = [t for t in completed_trades if t['pnl'] <= 0]
    
    win_rate = (len(wins) / total_trades * 100.0) if total_trades > 0 else 0.0
    gross_profit = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
    net_pnl = gross_profit - gross_loss

    per_coin = {}
    for sym in symbols:
        c_trades = [t for t in completed_trades if t['symbol'] == sym]
        c_wins = [t for t in c_trades if t['pnl'] > 0]
        c_losses = [t for t in c_trades if t['pnl'] <= 0]
        c_gp = sum(t['pnl'] for t in c_wins)
        c_gl = abs(sum(t['pnl'] for t in c_losses))
        c_pf = (c_gp / c_gl) if c_gl > 0 else (99.0 if c_gp > 0 else 0.0)
        c_wr = (len(c_wins) / len(c_trades) * 100.0) if c_trades else 0.0
        
        per_coin[sym] = {
            'trades': len(c_trades),
            'win_rate': round(c_wr, 2),
            'profit_factor': round(c_pf, 2),
            'net_pnl': round(c_gp - c_gl, 2)
        }

    summary_txt = f"""================================================================================
BINANCE FUTURES BACKTEST SUMMARY REPORT — 15M/30M CONFLUENCE STRATEGY (v2)
================================================================================

AGGREGATE PERFORMANCE:
--------------------------------------------------------------------------------
Initial Capital:         ${STARTING_CAPITAL:,.2f}
Ending Equity:           ${equity:,.2f}
Net Profit/Loss:         ${net_pnl:,.2f} ({((equity/STARTING_CAPITAL)-1)*100:.2f}%)
Total Trades:            {total_trades}
Win Rate:                {win_rate:.2f}%
Gross Profit:            ${gross_profit:,.2f}
Gross Loss:              ${gross_loss:,.2f}
PROFIT FACTOR:           {profit_factor:.3f}

FILTER REJECTION BREAKDOWN (100% ACCOUNTING):
--------------------------------------------------------------------------------
Total Scanned Bars:      {filter_stats['total_scanned_bars']:,}
 - Warmup Period Skipped: {filter_stats['warmup_none']:,}
 - Max Portfolio Cap:     {filter_stats['max_positions_reached']:,}
 - Already In Symbol:     {filter_stats['already_in_symbol']:,}
 - Layer 1 (Trend):       {filter_stats['rej_layer1_trend']:,}
 - Layer 2 (Value Zone):  {filter_stats['rej_layer2_value']:,}
 - Layer 3 (Volume):      {filter_stats['rej_layer3_volume']:,}
 - Layer 3 (Stoch RSI):   {filter_stats['rej_layer3_stoch']:,}
 -> Executed Trades:     {filter_stats['signals_executed']:,}

PER-COIN BREAKDOWN (SORTED BY PROFIT FACTOR):
--------------------------------------------------------------------------------
Symbol          | Tier | Trades | Win Rate (%) | Net PnL ($) | Profit Factor
--------------------------------------------------------------------------------
"""
    sorted_coins = sorted(per_coin.items(), key=lambda x: x[1]['profit_factor'], reverse=True)
    for sym, stats in sorted_coins:
        tier = COIN_TIERS.get(sym, 'B')
        summary_txt += f"{sym:<15} | {tier:<4} | {stats['trades']:<6} | {stats['win_rate']:<12.2f} | {stats['net_pnl']:<11.2f} | {stats['profit_factor']:.2f}\n"

    summary_txt += f"""--------------------------------------------------------------------------------
RECOMMENDATION:
{'[PASS] Strategy meets Profit Factor >= 1.20 target.' if profit_factor >= 1.20 else '[FAIL] Profit Factor below 1.20 target. Optimization required.'}
================================================================================
"""

    with open("backtest_summary.txt", "w", encoding="utf-8") as f:
        f.write(summary_txt)

    report_json = {
        'meta': {
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'starting_capital': STARTING_CAPITAL,
            'risk_per_trade': RISK_PER_TRADE,
            'max_concurrent_positions': MAX_CONCURRENT_POSITIONS,
            'parallel_workers': MAX_WORKERS
        },
        'aggregate': {
            'total_trades': total_trades,
            'win_rate': round(win_rate, 2),
            'profit_factor': round(profit_factor, 3),
            'net_pnl': round(net_pnl, 2),
            'ending_equity': round(equity, 2)
        },
        'filter_stats': filter_stats,
        'per_coin': per_coin,
        'trades': completed_trades
    }

    with open("backtest_report.json", "w", encoding="utf-8") as f:
        json.dump(report_json, f, indent=2)

    print("\n" + summary_txt)
    print("Artifacts generated: backtest_summary.txt, backtest_report.json")

if __name__ == "__main__":
    run_backtest()
