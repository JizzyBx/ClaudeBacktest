"""
GMAX — Candle Data Pull & Market Character Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pulls 2 years of 15m candles for every coin in the universe.
No strategy. No signals. No trades.
Just raw candle stats so we know what the market actually looks like.

stdlib only — no pip installs
Output: candle_report.json + candle_summary.txt (zipped by workflow)

Usage:
  python candle_analysis.py 0       # run shard 0
  python candle_analysis.py 1       # run shard 1
  ...
  python candle_analysis.py merge   # merge all shards → final report
"""

import sys, io, csv, json, time, zipfile, math
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────────────────────
# COIN UNIVERSE  (GMaxV1 — 86 coins)
# ─────────────────────────────────────────────────────────────────────────────
ALL_SYMBOLS = [
    '1000000BOBUSDT','1000BONKUSDT','1000CATUSDT','1000RATSUSDT',
    '1000SATSUSDT','A2ZUSDT','ACHUSDT','AI16ZUSDT','AINUSDT',
    'ALGOUSDT','ALICEUSDT','ALPINEUSDT','ARKMUSDT','ASRUSDT',
    'ASTERUSDT','AUSDT','AWEUSDT','BANKUSDT','BASEDUSDT','BELUSDT',
    'BIDUSDT','BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT','COAIUSDT',
    'COMBOUSDT','CRCLUSDT','DAMUSDT','DEFIUSDT','DIAUSDT','DMCUSDT',
    'ELSAUSDT','ENAUSDT','EPICUSDT','EPTUSDT','ETHUSDT','FLNCUSDT',
    'FLUXUSDT','FXSUSDT','GLMUSDT','GRIFFAINUSDT','GUAUSDT','HANAUSDT',
    'HEMIUSDT','ICXUSDT','INITUSDT','IOUSDT','KITEUSDT','LABUSDT',
    'LIGHTUSDT','LRCUSDT','LYNUSDT','MAGICUSDT','MEGAUSDT','MILKUSDT',
    'MOODENGUSDT','NFPUSDT','NMRUSDT','NOMUSDT','NOTUSDT','OBOLUSDT',
    'OPENUSDT','OPNUSDT','ORBSUSDT','PIXELUSDT','PLUMEUSDT','POWERUSDT',
    'POWRUSDT','PTBUSDT','PUMPBTCUSDT','QUICKUSDT','RAVEUSDT','REEFUSDT',
    'RESOLVUSDT','RLSUSDT','RVVUSDT','SAGAUSDT','SANTOSUSDT','SKRUSDT',
    'SOMIUSDT','SPELLUSDT','SPKUSDT','STBLUSDT','TRUTHUSDT','TURBOUSDT',
    'UBUSDT','USUALUSDT','VINEUSDT','VIRTUALUSDT','VVVUSDT','XEMUSDT',
    'XRPUSDT','YBUSDT','ZECUSDT','ZEREBROUSDT',
]

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
NUM_SHARDS = 8
WORKERS    = 16
TIMEFRAME  = "15m"
START_YM   = (2023, 8)   # Aug 2023
END_YM     = (2025, 7)   # Jul 2025
BASE_URL   = "https://data.binance.vision/data/futures/um/monthly/klines"
TIMEOUT    = 30

# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────
def month_range(start, end):
    y, m = start
    ey, em = end
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1

def fetch_month(symbol, year, month):
    url = (f"{BASE_URL}/{symbol}/{TIMEFRAME}/"
           f"{symbol}-{TIMEFRAME}-{year}-{month:02d}.zip")
    try:
        with urlopen(url, timeout=TIMEOUT) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                reader = csv.reader(io.TextIOWrapper(f))
                rows = []
                for row in reader:
                    if not row or not row[0].lstrip('-').isdigit():
                        continue
                    ts = int(row[0])
                    if ts > 10**14:
                        ts //= 1000
                    rows.append((
                        ts,
                        float(row[1]),  # open
                        float(row[2]),  # high
                        float(row[3]),  # low
                        float(row[4]),  # close
                        float(row[5]),  # volume
                    ))
                return rows
    except Exception:
        return []

def fetch_symbol(symbol):
    raw = []
    for y, m in month_range(START_YM, END_YM):
        raw.extend(fetch_month(symbol, y, m))
    if not raw:
        return []
    seen = {}
    for c in raw:
        seen[c[0]] = c
    return sorted(seen.values(), key=lambda x: x[0])

# ─────────────────────────────────────────────────────────────────────────────
# PURE-PYTHON INDICATORS  (no numpy / pandas)
# ─────────────────────────────────────────────────────────────────────────────
def calc_ema(values, period):
    """Returns EMA list, None where not enough data."""
    out = [None] * len(values)
    k = 2.0 / (period + 1)
    prev = None
    for i, v in enumerate(values):
        if v is None:
            continue
        if prev is None:
            prev = v
        else:
            prev = v * k + prev * (1 - k)
        out[i] = prev
    return out

def calc_rsi(closes, period=14):
    out = [None] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    for i in range(period - 1, len(gains)):
        ag = sum(gains[i - period + 1:i + 1]) / period
        al = sum(losses[i - period + 1:i + 1]) / period
        if al == 0:
            out[i + 1] = 100.0
        else:
            out[i + 1] = 100.0 - 100.0 / (1.0 + ag / al)
    return out

def calc_atr(highs, lows, closes, period=14):
    """Returns list of ATR values (% of close). None where not enough data."""
    trs = [None]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    out = [None] * len(closes)
    for i in range(period, len(trs)):
        window = [t for t in trs[i - period + 1:i + 1] if t is not None]
        if len(window) == period:
            atr_val = sum(window) / period
            out[i] = atr_val / closes[i] * 100.0 if closes[i] > 0 else None
    return out

def calc_adx(highs, lows, closes, period=14):
    """Returns list of ADX values. None where not enough data."""
    n = len(closes)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_list  = [0.0] * n
    for i in range(1, n):
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr_list[i]  = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
    def wilder_smooth(data, p):
        s = [None] * len(data)
        if len(data) < p:
            return s
        s[p] = sum(data[1:p + 1])
        for i in range(p + 1, len(data)):
            s[i] = s[i - 1] - s[i - 1] / p + data[i]
        return s
    str14    = wilder_smooth(tr_list,   period)
    spdm14   = wilder_smooth(plus_dm,   period)
    smdm14   = wilder_smooth(minus_dm,  period)
    di_plus  = [None] * n
    di_minus = [None] * n
    dx_list  = [None] * n
    for i in range(period, n):
        if str14[i] and str14[i] > 0:
            di_plus[i]  = 100.0 * spdm14[i]  / str14[i]
            di_minus[i] = 100.0 * smdm14[i]  / str14[i]
            s = di_plus[i] + di_minus[i]
            dx_list[i]  = 100.0 * abs(di_plus[i] - di_minus[i]) / s if s > 0 else 0.0
    adx = [None] * n
    valid_dx = [(i, dx_list[i]) for i in range(n) if dx_list[i] is not None]
    if len(valid_dx) >= period:
        start_i = valid_dx[period - 1][0]
        adx[start_i] = sum(v for _, v in valid_dx[:period]) / period
        for j in range(period, len(valid_dx)):
            i = valid_dx[j][0]
            adx[i] = (adx[valid_dx[j - 1][0]] * (period - 1) + valid_dx[j][1]) / period
    return adx

def calc_bb_width(closes, period=20, mult=2.0):
    """Bollinger Band width as % of middle band. None where not enough data."""
    out = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1:i + 1]
        mean = sum(window) / period
        std  = (sum((v - mean) ** 2 for v in window) / period) ** 0.5
        if mean > 0:
            out[i] = (mult * 2 * std) / mean * 100.0
    return out

# ─────────────────────────────────────────────────────────────────────────────
# COIN ANALYSIS  — pure stats, no trades
# ─────────────────────────────────────────────────────────────────────────────
def analyse_coin(symbol, candles):
    n = len(candles)
    if n < 300:
        return None

    ts     = [c[0] for c in candles]
    opens  = [c[1] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    vols   = [c[5] for c in candles]

    # ── Span ─────────────────────────────────────────────────────────────────
    span_days = (ts[-1] - ts[0]) / 86_400_000.0

    # ── Price ────────────────────────────────────────────────────────────────
    total_return_pct = (closes[-1] - closes[0]) / closes[0] * 100.0
    price_max        = max(closes)
    price_min        = min(closes)
    drawdown_pct     = (price_max - price_min) / price_max * 100.0

    # ── Candle body size (% of open) ─────────────────────────────────────────
    bodies = [abs(closes[i] - opens[i]) / opens[i] * 100.0 for i in range(n)]
    avg_body_pct    = sum(bodies) / n
    median_body_pct = sorted(bodies)[n // 2]

    # Bullish vs bearish candle ratio
    bull_bars = sum(1 for i in range(n) if closes[i] >= opens[i])
    bull_ratio = bull_bars / n * 100.0

    # ── Wicks ────────────────────────────────────────────────────────────────
    upper_wicks, lower_wicks = [], []
    for i in range(n):
        body_top = max(opens[i], closes[i])
        body_bot = min(opens[i], closes[i])
        body_sz  = max(body_top - body_bot, 1e-10)
        upper_wicks.append((highs[i]  - body_top) / body_sz)
        lower_wicks.append((body_bot  - lows[i])  / body_sz)
    avg_upper_wick = sum(upper_wicks) / n
    avg_lower_wick = sum(lower_wicks) / n

    # ── Bar-to-bar moves ─────────────────────────────────────────────────────
    moves = [abs(closes[i] - closes[i - 1]) / closes[i - 1] * 100.0
             for i in range(1, n)]
    moves_s    = sorted(moves)
    avg_move   = sum(moves) / len(moves)
    move_p50   = moves_s[len(moves_s) // 2]
    move_p90   = moves_s[int(len(moves_s) * 0.90)]
    move_p95   = moves_s[int(len(moves_s) * 0.95)]
    move_p99   = moves_s[int(len(moves_s) * 0.99)]

    # ── ATR% ─────────────────────────────────────────────────────────────────
    atr_pct = calc_atr(highs, lows, closes, 14)
    atr_vals = [v for v in atr_pct if v is not None]
    avg_atr_pct = sum(atr_vals) / len(atr_vals) if atr_vals else 0.0
    max_atr_pct = max(atr_vals)                  if atr_vals else 0.0

    # ATR quartiles
    atr_s = sorted(atr_vals)
    atr_q1 = atr_s[len(atr_s) // 4]    if atr_s else 0.0
    atr_q3 = atr_s[len(atr_s) * 3 // 4] if atr_s else 0.0

    # ── ADX ──────────────────────────────────────────────────────────────────
    adx_vals_raw = calc_adx(highs, lows, closes, 14)
    adx_vals     = [v for v in adx_vals_raw if v is not None]
    avg_adx      = sum(adx_vals) / len(adx_vals) if adx_vals else 0.0
    pct_adx_gt20 = sum(1 for v in adx_vals if v > 20) / max(len(adx_vals), 1) * 100.0
    pct_adx_gt30 = sum(1 for v in adx_vals if v > 30) / max(len(adx_vals), 1) * 100.0

    # ── EMA50 trend bias ─────────────────────────────────────────────────────
    ema50 = calc_ema(closes, 50)
    pct_above_ema50 = sum(1 for i in range(50, n)
                          if ema50[i] and closes[i] > ema50[i]) / max(n - 50, 1) * 100.0

    # EMA slope: pct of time EMA50 is rising
    ema50_rising = sum(1 for i in range(51, n)
                       if ema50[i] and ema50[i - 1] and ema50[i] > ema50[i - 1])
    pct_ema50_rising = ema50_rising / max(n - 51, 1) * 100.0

    # ── RSI ──────────────────────────────────────────────────────────────────
    rsi14_raw = calc_rsi(closes, 14)
    rsi_vals  = [v for v in rsi14_raw if v is not None]
    avg_rsi            = sum(rsi_vals) / len(rsi_vals) if rsi_vals else 50.0
    rsi_oversold_pct   = sum(1 for v in rsi_vals if v < 30) / max(len(rsi_vals), 1) * 100.0
    rsi_overbought_pct = sum(1 for v in rsi_vals if v > 70) / max(len(rsi_vals), 1) * 100.0
    rsi_neutral_pct    = sum(1 for v in rsi_vals if 40 <= v <= 60) / max(len(rsi_vals), 1) * 100.0

    # ── Bollinger Band width ──────────────────────────────────────────────────
    bb_width_vals_raw = calc_bb_width(closes, 20, 2.0)
    bb_vals = [v for v in bb_width_vals_raw if v is not None]
    avg_bb_width = sum(bb_vals) / len(bb_vals) if bb_vals else 0.0
    bb_s = sorted(bb_vals)
    bb_squeeze_pct = sum(1 for v in bb_vals if v < bb_s[len(bb_s) // 4]) / max(len(bb_vals), 1) * 100.0

    # ── Consecutive run length (momentum sustainability) ─────────────────────
    runs = []
    cur = 1
    for i in range(1, n):
        same = (closes[i] > closes[i - 1]) == (closes[i - 1] > closes[i - 2]) if i > 1 else True
        if same:
            cur += 1
        else:
            runs.append(cur)
            cur = 1
    runs.append(cur)
    avg_run_bars = sum(runs) / len(runs) if runs else 1.0
    max_run_bars = max(runs)

    # ── Volume ───────────────────────────────────────────────────────────────
    avg_vol = sum(vols) / n
    vol_std = (sum((v - avg_vol) ** 2 for v in vols) / n) ** 0.5
    vol_cv  = vol_std / avg_vol if avg_vol > 0 else 0.0   # coefficient of variation

    # ── Gap frequency (open vs prev close > 0.3%) ────────────────────────────
    gaps = [abs(opens[i] - closes[i - 1]) / closes[i - 1] * 100.0
            for i in range(1, n)]
    gap_freq_pct = sum(1 for g in gaps if g > 0.3) / len(gaps) * 100.0
    avg_gap_pct  = sum(gaps) / len(gaps)

    # ── TP/SL reachability simulation ────────────────────────────────────────
    # For a range of TP/SL combos: how often does a random long entry
    # hit TP first vs SL first within 96 bars (1 day)?
    # Uses actual high/low of each bar — no strategy signal, pure market data.
    def tp_sl_sim(tp_pct, sl_pct, max_bars=96, sample_every=10):
        tp_hits = 0
        sl_hits = 0
        neither = 0
        trials  = 0
        for start in range(0, n - max_bars - 1, sample_every):
            entry = closes[start]
            tp_p  = entry * (1 + tp_pct / 100.0)
            sl_p  = entry * (1 - sl_pct / 100.0)
            result = 'neither'
            for j in range(start + 1, start + max_bars + 1):
                if lows[j]  <= sl_p:
                    result = 'sl'; break
                if highs[j] >= tp_p:
                    result = 'tp'; break
            if result == 'tp':   tp_hits += 1
            elif result == 'sl': sl_hits += 1
            else:                neither  += 1
            trials += 1
        if trials == 0:
            return {}
        return {
            'tp_hit_pct':    round(tp_hits  / trials * 100, 2),
            'sl_hit_pct':    round(sl_hits  / trials * 100, 2),
            'neither_pct':   round(neither  / trials * 100, 2),
            'trials':        trials,
        }

    tp_sl_matrix = {}
    combos = [
        ('3TP_15SL',  3.0,  15.0),
        ('3TP_10SL',  3.0,  10.0),
        ('5TP_15SL',  5.0,  15.0),
        ('5TP_10SL',  5.0,  10.0),
        ('8TP_15SL',  8.0,  15.0),
        ('10TP_20SL', 10.0, 20.0),
        ('2TP_5SL',   2.0,   5.0),
    ]
    for label, tp, sl in combos:
        tp_sl_matrix[label] = tp_sl_sim(tp, sl)

    return {
        'symbol':               symbol,
        'candles':              n,
        'span_days':            round(span_days, 1),

        # Price
        'total_return_pct':     round(total_return_pct, 2),
        'drawdown_pct':         round(drawdown_pct, 2),
        'bull_candle_ratio':    round(bull_ratio, 2),

        # Candle anatomy
        'avg_body_pct':         round(avg_body_pct, 5),
        'median_body_pct':      round(median_body_pct, 5),
        'avg_upper_wick':       round(avg_upper_wick, 3),
        'avg_lower_wick':       round(avg_lower_wick, 3),

        # Move distribution
        'avg_move_pct':         round(avg_move, 5),
        'move_p50_pct':         round(move_p50, 5),
        'move_p90_pct':         round(move_p90, 5),
        'move_p95_pct':         round(move_p95, 5),
        'move_p99_pct':         round(move_p99, 5),

        # Volatility
        'avg_atr_pct':          round(avg_atr_pct, 5),
        'max_atr_pct':          round(max_atr_pct, 5),
        'atr_q1_pct':           round(atr_q1, 5),
        'atr_q3_pct':           round(atr_q3, 5),

        # Trend
        'avg_adx':              round(avg_adx, 2),
        'pct_adx_gt20':         round(pct_adx_gt20, 2),
        'pct_adx_gt30':         round(pct_adx_gt30, 2),
        'pct_above_ema50':      round(pct_above_ema50, 2),
        'pct_ema50_rising':     round(pct_ema50_rising, 2),

        # Momentum
        'avg_run_bars':         round(avg_run_bars, 2),
        'max_run_bars':         max_run_bars,

        # RSI
        'avg_rsi':              round(avg_rsi, 2),
        'rsi_oversold_pct':     round(rsi_oversold_pct, 2),
        'rsi_overbought_pct':   round(rsi_overbought_pct, 2),
        'rsi_neutral_pct':      round(rsi_neutral_pct, 2),

        # Bollinger
        'avg_bb_width_pct':     round(avg_bb_width, 4),
        'bb_squeeze_pct':       round(bb_squeeze_pct, 2),

        # Volume
        'vol_cv':               round(vol_cv, 3),

        # Gaps
        'gap_freq_pct':         round(gap_freq_pct, 2),
        'avg_gap_pct':          round(avg_gap_pct, 5),

        # TP/SL reachability matrix
        'tp_sl_matrix':         tp_sl_matrix,
    }

# ─────────────────────────────────────────────────────────────────────────────
# SHARD RUNNER
# ─────────────────────────────────────────────────────────────────────────────
def run_shard(shard_idx):
    symbols = ALL_SYMBOLS[shard_idx::NUM_SHARDS]
    print(f"[Shard {shard_idx}] assigned {len(symbols)} coins")
    t0 = time.time()

    results = []
    failed  = []

    def process(sym):
        candles = fetch_symbol(sym)
        if len(candles) < 300:
            print(f"  SKIP {sym}: {len(candles)} candles (need 300+)")
            return None
        info = analyse_coin(sym, candles)
        if info:
            print(f"  OK   {sym}: {len(candles)} bars | "
                  f"ATR%={info['avg_atr_pct']:.4f} | "
                  f"ADX={info['avg_adx']:.1f} | "
                  f"3TP/15SL hit={info['tp_sl_matrix'].get('3TP_15SL',{}).get('tp_hit_pct','?')}%")
        return info

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process, s): s for s in symbols}
        for fut in as_completed(futs):
            r = fut.result()
            if r:
                results.append(r)
            else:
                failed.append(futs[fut])

    elapsed = round(time.time() - t0, 2)
    out = {
        'shard':   shard_idx,
        'symbols': symbols,
        'results': results,
        'failed':  failed,
        'elapsed': elapsed,
    }
    with open(f"shard_{shard_idx}.json", 'w') as f:
        json.dump(out, f)
    print(f"[Shard {shard_idx}] done — {len(results)} coins in {elapsed}s")

# ─────────────────────────────────────────────────────────────────────────────
# MERGE
# ─────────────────────────────────────────────────────────────────────────────
def merge_shards():
    all_results = []
    for i in range(NUM_SHARDS):
        try:
            with open(f"shard_{i}.json") as f:
                d = json.load(f)
            all_results.extend(d['results'])
            print(f"Loaded shard_{i}.json — {len(d['results'])} coins")
        except FileNotFoundError:
            print(f"WARNING: shard_{i}.json not found, skipping")

    if not all_results:
        print("ERROR: No shard data found. Aborting.")
        sys.exit(1)

    nc = len(all_results)

    def avg(key):
        vals = [r[key] for r in all_results if isinstance(r.get(key), (int, float))]
        return round(sum(vals) / len(vals), 5) if vals else 0.0

    def pct_coins_above(key, threshold):
        vals = [r[key] for r in all_results if isinstance(r.get(key), (int, float))]
        return round(sum(1 for v in vals if v > threshold) / max(len(vals), 1) * 100, 2)

    # Aggregate TP/SL matrix across all coins
    combo_labels = ['3TP_15SL','3TP_10SL','5TP_15SL','5TP_10SL',
                    '8TP_15SL','10TP_20SL','2TP_5SL']
    agg_tp_sl = {}
    for label in combo_labels:
        tp_hits = [r['tp_sl_matrix'][label]['tp_hit_pct']
                   for r in all_results
                   if label in r.get('tp_sl_matrix', {})
                   and r['tp_sl_matrix'][label]]
        sl_hits = [r['tp_sl_matrix'][label]['sl_hit_pct']
                   for r in all_results
                   if label in r.get('tp_sl_matrix', {})
                   and r['tp_sl_matrix'][label]]
        if tp_hits:
            agg_tp_sl[label] = {
                'avg_tp_hit_pct': round(sum(tp_hits) / len(tp_hits), 2),
                'avg_sl_hit_pct': round(sum(sl_hits) / len(sl_hits), 2),
                'coins': len(tp_hits),
            }

    aggregate = {
        'coins_analysed':       nc,
        'avg_candles':          avg('candles'),
        'avg_span_days':        avg('span_days'),

        'avg_total_return_pct': avg('total_return_pct'),
        'avg_drawdown_pct':     avg('drawdown_pct'),
        'avg_bull_candle_ratio':avg('bull_candle_ratio'),

        'avg_body_pct':         avg('avg_body_pct'),
        'avg_atr_pct':          avg('avg_atr_pct'),
        'max_atr_pct':          max(r['max_atr_pct'] for r in all_results),
        'avg_move_p90':         avg('move_p90_pct'),
        'avg_move_p99':         avg('move_p99_pct'),

        'avg_adx':              avg('avg_adx'),
        'pct_adx_gt20':         avg('pct_adx_gt20'),
        'pct_adx_gt30':         avg('pct_adx_gt30'),
        'avg_pct_above_ema50':  avg('pct_above_ema50'),
        'avg_pct_ema50_rising': avg('pct_ema50_rising'),

        'avg_run_bars':         avg('avg_run_bars'),
        'avg_rsi':              avg('avg_rsi'),
        'avg_rsi_oversold_pct': avg('rsi_oversold_pct'),
        'avg_rsi_overbought_pct': avg('rsi_overbought_pct'),

        'avg_bb_width_pct':     avg('avg_bb_width_pct'),
        'avg_bb_squeeze_pct':   avg('bb_squeeze_pct'),

        'avg_vol_cv':           avg('vol_cv'),
        'avg_gap_freq_pct':     avg('gap_freq_pct'),

        'tp_sl_matrix':         agg_tp_sl,
    }

    report = {
        'period':     f"{START_YM[0]}-{START_YM[1]:02d} to {END_YM[0]}-{END_YM[1]:02d}",
        'timeframe':  TIMEFRAME,
        'aggregate':  aggregate,
        'per_coin':   sorted(all_results, key=lambda r: r['avg_atr_pct'], reverse=True),
    }

    with open("candle_report.json", 'w') as f:
        json.dump(report, f, indent=2)

    # ── Summary TXT ──────────────────────────────────────────────────────────
    a = aggregate
    lines = []
    W = 70
    lines.append("=" * W)
    lines.append("GMAX UNIVERSE — CANDLE CHARACTER REPORT")
    lines.append(f"Period    : {report['period']}   |   Timeframe: {TIMEFRAME}")
    lines.append(f"Coins     : {nc}")
    lines.append("=" * W)

    lines.append("")
    lines.append("── PRICE & STRUCTURE ───────────────────────────────────────────────")
    lines.append(f"  Avg candles/coin      : {a['avg_candles']:.0f}")
    lines.append(f"  Avg data span         : {a['avg_span_days']:.0f} days")
    lines.append(f"  Avg total return      : {a['avg_total_return_pct']:.2f}%")
    lines.append(f"  Avg max drawdown      : {a['avg_drawdown_pct']:.2f}%")
    lines.append(f"  Avg bull candle ratio : {a['avg_bull_candle_ratio']:.2f}%")

    lines.append("")
    lines.append("── VOLATILITY ──────────────────────────────────────────────────────")
    lines.append(f"  Avg ATR% (14)         : {a['avg_atr_pct']:.5f}%")
    lines.append(f"  Max ATR% ever         : {a['max_atr_pct']:.5f}%")
    lines.append(f"  Avg bar move          : n/a (see per-coin)")
    lines.append(f"  P90 bar move avg      : {a['avg_move_p90']:.5f}%")
    lines.append(f"  P99 bar move avg      : {a['avg_move_p99']:.5f}%")
    lines.append(f"  Avg BB width%         : {a['avg_bb_width_pct']:.4f}%")
    lines.append(f"  Avg squeeze time      : {a['avg_bb_squeeze_pct']:.2f}% of bars")

    lines.append("")
    lines.append("── TREND ───────────────────────────────────────────────────────────")
    lines.append(f"  Avg ADX(14)           : {a['avg_adx']:.2f}")
    lines.append(f"  % bars ADX > 20       : {a['pct_adx_gt20']:.2f}%")
    lines.append(f"  % bars ADX > 30       : {a['pct_adx_gt30']:.2f}%")
    lines.append(f"  % bars above EMA50    : {a['avg_pct_above_ema50']:.2f}%")
    lines.append(f"  % bars EMA50 rising   : {a['avg_pct_ema50_rising']:.2f}%")
    lines.append(f"  Avg consecutive run   : {a['avg_run_bars']:.2f} bars")

    lines.append("")
    lines.append("── MOMENTUM / MEAN-REVERSION ───────────────────────────────────────")
    lines.append(f"  Avg RSI(14)           : {a['avg_rsi']:.2f}")
    lines.append(f"  RSI oversold (<30)    : {a['avg_rsi_oversold_pct']:.2f}% of bars")
    lines.append(f"  RSI overbought (>70)  : {a['avg_rsi_overbought_pct']:.2f}% of bars")

    lines.append("")
    lines.append("── VOLUME & GAPS ───────────────────────────────────────────────────")
    lines.append(f"  Volume CoV            : {a['avg_vol_cv']:.3f}  (lower = more consistent)")
    lines.append(f"  Gap freq (>0.3%)      : {a['avg_gap_freq_pct']:.2f}% of bars")

    lines.append("")
    lines.append("── TP/SL REACHABILITY MATRIX ───────────────────────────────────────")
    lines.append("  Combo          TP hit%   SL hit%   Coins")
    lines.append("  " + "-" * 44)
    for label, v in a['tp_sl_matrix'].items():
        tp_s = str(v['avg_tp_hit_pct']).rjust(7)
        sl_s = str(v['avg_sl_hit_pct']).rjust(8)
        cn_s = str(v['coins']).rjust(7)
        lines.append(f"  {label:<14} {tp_s}%  {sl_s}%  {cn_s}")

    lines.append("")
    lines.append("── PER COIN (sorted by avg ATR%) ───────────────────────────────────")
    lines.append(
        f"{'Symbol':<22} {'Bars':>6} {'ATR%':>8} {'ADX':>6} "
        f"{'P90mv':>8} {'RSI':>6} {'EMA50%':>7} "
        f"{'3TP15SL':>9} {'Ret%':>8}"
    )
    lines.append("-" * W)
    for r in sorted(all_results, key=lambda x: x['avg_atr_pct'], reverse=True):
        tp_hit = (r['tp_sl_matrix'].get('3TP_15SL') or {}).get('tp_hit_pct', '-')
        lines.append(
            f"{r['symbol']:<22} {r['candles']:>6} "
            f"{r['avg_atr_pct']:>8.5f} {r['avg_adx']:>6.1f} "
            f"{r['move_p90_pct']:>8.5f} {r['avg_rsi']:>6.1f} "
            f"{r['pct_above_ema50']:>7.1f}% "
            f"{str(tp_hit):>8}% "
            f"{r['total_return_pct']:>8.2f}%"
        )

    lines.append("")
    lines.append("Full machine-readable data → candle_report.json")
    lines.append("=" * W)

    with open("candle_summary.txt", 'w') as f:
        f.write("\n".join(lines))

    print(f"Merge complete → candle_report.json + candle_summary.txt ({nc} coins)")

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python candle_analysis.py <shard_idx|merge>")
        sys.exit(1)
    arg = sys.argv[1]
    if arg == "merge":
        merge_shards()
    else:
        run_shard(int(arg))
