"""
G Max V1 — Single-Strategy Production Build
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pure Python | Termux compatible
pip install python-binance flask requests

STRATEGY: Variant G VAR_D · 15m
  EMA50 slope filter + EMA9/21 crossover + ADX(14) >= 22
  TP: 3.0%  |  SL: 15.0%  |  Max hold: 10d (960 bars) |  Fixed levels, no trailing

COIN MODES: Whitelist (58 coins, PF>=1.5) or Universe (117 coins)
  Toggle on dashboard or via /mode in Telegram.

Engineered by Paqu
"""

import json, time, logging, threading, math, socket, requests
from urllib.parse import quote
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from flask import Flask, render_template_string, request, redirect, jsonify

BOT_DIR      = Path("/storage/emulated/0/GMaxV1")
CONFIG_PATH  = BOT_DIR / "config.json"
HISTORY_PATH = BOT_DIR / "trade_history.json"
BOT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s | %(message)s', datefmt='%I:%M %p')
log = logging.getLogger(__name__)

# ── Default Settings ──────────────────────────────────────
DEFAULT_SETTINGS = {
    'margin_usd'    : 1.0,
    'margin_percent': 2.0,
    'margin_mode'   : 'fixed',
    'leverage'      : 5,
    'margin_type'   : 'CROSSED',
    'cooldown_min'  : 5,
    'scan_every'    : 120,
    'theme'         : 'classic',
    'live_pnl'      : True,
    'share_signals' : False,
    'signal_channel': '',
}
THEMES = ['classic','cyber','aurora','solar','matrix','sunset','arceus_white','pikachu_strike']

def load_config():
    if not CONFIG_PATH.exists(): return {}
    with open(CONFIG_PATH) as f: return json.load(f)

def save_config_data(data):
    existing = load_config()
    existing.update(data)
    with open(CONFIG_PATH,'w') as f: json.dump(existing,f)

def get_settings():
    cfg = load_config()
    return {k: cfg.get(k, DEFAULT_SETTINGS[k]) for k in DEFAULT_SETTINGS}

def current_theme():
    t = load_config().get('theme','classic')
    return t if t in THEMES else 'classic'

def is_configured():
    k = load_config().get("api_key","")
    return bool(k) and k != "YOUR_KEY"

# ── Strategy G Constants ───────────────────────────────────
STRATEGY_G = {
    'name': 'G Max · 15m',
    'tf'  : '15m',
    'tp'  : 0.030,   # 3.0%
    'sl'  : 0.150,   # 15.0%
    'max_hold_bars': 960,   # 10 days at 15m candles
}
MAX_HOLD_SECONDS = STRATEGY_G['max_hold_bars'] * 15 * 60   # bars * 15m * 60s

# ── Coin Lists ─────────────────────────────────────────────
COINS_WHITELIST = [
    '1000000BOBUSDT','1000CATUSDT','1000RATSUSDT','A2ZUSDT','ACHUSDT',
    'AINUSDT','AIOTUSDT','ALGOUSDT','ALPINEUSDT','ASRUSDT','AUSDT','AWEUSDT',
    'BASEDUSDT','BELUSDT','BIDUSDT','BMTUSDT','BTRUSDT','CFXUSDT','CHIPUSDT',
    'CRCLUSDT','DAMUSDT','DIAUSDT','DMCUSDT','ENAUSDT','EPTUSDT','ETHUSDT',
    'FLNCUSDT','FXSUSDT','GLMUSDT','GUAUSDT','HANAUSDT','LIGHTUSDT','LYNUSDT',
    'MAGICUSDT','NFPUSDT','NMRUSDT','NOTUSDT','OBOLUSDT','ORBSUSDT',
    'PEOPLEUSDT','PIXELUSDT','POWERUSDT','POWRUSDT','PUNDIXUSDT','RAVEUSDT',
    'RLSUSDT','RVVUSDT','SEIUSDT','SNDKUSDT','SPELLUSDT','TRUTHUSDT',
    'TURBOUSDT','UBUSDT','VANRYUSDT','VINEUSDT','XEMUSDT','ZECUSDT',
    'ZEREBROUSDT',
]

COINS_UNIVERSE = [
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

# SYMBOLS for precision/leverage priming = full universe (superset)
SYMBOLS = list(dict.fromkeys(COINS_UNIVERSE))  # deduped, order preserved

def active_coins():
    mode = load_config().get('coin_mode', 'whitelist')
    return COINS_WHITELIST if mode == 'whitelist' else COINS_UNIVERSE

def current_mode_label():
    mode = load_config().get('coin_mode', 'whitelist')
    if mode == 'whitelist':
        return f"🎯 Whitelist Mode — {len(COINS_WHITELIST)} coins"
    return f"🌐 Universe Mode — {len(COINS_UNIVERSE)} coins"

# ── Per-coin enable/disable ────────────────────────────────
def disabled_coins_set():
    return set(load_config().get('disabled_coins', []))

def is_coin_enabled(symbol):
    return symbol not in disabled_coins_set()

def toggle_coin(symbol):
    s = disabled_coins_set()
    if symbol in s: s.discard(symbol)
    else: s.add(symbol)
    save_config_data({'disabled_coins': sorted(s)})

def enable_all_coins():
    save_config_data({'disabled_coins': []})

# ── Extra Signals (manually-added coins) ───────────────────
def extra_signals_list():
    return load_config().get('extra_signals', [])

def is_extra_signal(symbol):
    return symbol in extra_signals_list()

def all_trading_coins():
    """Live Signal coins + Extra Signal coins, deduped — this is what actually gets scanned/traded."""
    combined = list(active_coins())
    for s in extra_signals_list():
        if s not in combined:
            combined.append(s)
    return combined

def prime_symbol_precision(symbol):
    """Prime precision cache for a single coin (used when a new Extra Signal coin is added)."""
    if not client:
        return False
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                qty_p = price_p = 0
                for f in s['filters']:
                    if f['filterType']=='LOT_SIZE':
                        step=f['stepSize'].rstrip('0')
                        qty_p = len(step.split('.')[1]) if '.' in step else 0
                    if f['filterType']=='PRICE_FILTER':
                        tick=f['tickSize'].rstrip('0')
                        price_p = len(tick.split('.')[1]) if '.' in tick else 0
                PRECISION_CACHE[symbol]={'qty_p':qty_p,'price_p':price_p}
                return True
        return False
    except Exception as e:
        log.info(f"prime_symbol_precision error {symbol}: {e}")
        return False

def add_extra_signal(raw_symbol):
    """Search+add a coin to Extra Signals. Returns (status, message) where status is
    'ok' | 'exists' | 'error'."""
    symbol = (raw_symbol or '').strip().upper().replace(' ', '')
    if not symbol:
        return 'error', 'Enter a coin symbol first'
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
    if symbol in active_coins():
        return 'exists', f'{display_name(symbol)} already exists in Live Signals'
    if symbol in extra_signals_list():
        return 'exists', f'{display_name(symbol)} already exists in Extra Signals'
    ok = symbol in PRECISION_CACHE or prime_symbol_precision(symbol)
    if not ok:
        return 'error', f'{display_name(symbol)} not found on Binance Futures'
    lst = extra_signals_list()
    lst.append(symbol)
    save_config_data({'extra_signals': lst})
    s = get_settings()
    if client:
        set_leverage_once(symbol, s['leverage'])
        set_margin_type_once(symbol, s['margin_type'])
    return 'ok', f'{display_name(symbol)} added to Extra Signals'

def remove_extra_signal(symbol):
    symbol = symbol.upper()
    lst = [s for s in extra_signals_list() if s != symbol]
    save_config_data({'extra_signals': lst})

# ── Coin Sort Mode ─────────────────────────────────────────
SORT_MODES  = ['normal', 'recently_traded', 'about_to_trade']
SORT_LABELS = {
    'normal'          : '📋 Normal',
    'recently_traded' : '🔥 Recently Traded',
    'about_to_trade'  : '⚡ About to Trade',
}

def current_sort_mode():
    m = load_config().get('coin_sort_mode', 'normal')
    return m if m in SORT_MODES else 'normal'

def set_sort_mode(mode):
    save_config_data({'coin_sort_mode': mode if mode in SORT_MODES else 'normal'})

def _sort_score_recently_traded(symbol):
    t = last_trade_time.get(symbol)
    if t is None:
        return 9999999
    return (datetime.now() - t).total_seconds()

def _sort_score_about_to_trade(symbol):
    if not is_coin_enabled(symbol):
        return 99
    sg = state['last_signals'].get(symbol)
    if not sg:
        return 5
    signal  = sg.get('signal', None)
    crossed = sg.get('crossed', '')
    trend   = sg.get('trend', '')
    if signal in ('buy', 'sell'):
        return 0
    if crossed == '✅' and trend in ('↑', '↓'):
        return 1
    if trend in ('↑', '↓'):
        return 2
    return 3

# ── State ──────────────────────────────────────────────────
state = {
    'balance'        : 0.0,
    'today_pnl'      : 0.0,
    'total_pnl'      : 0.0,
    'wins'           : 0,
    'losses'         : 0,
    'trades'         : [],
    'open_positions' : {},
    'pos_orders'     : {},
    'last_signals'   : {},
    'last_update'    : '--',
    'start_time'     : datetime.now().astimezone().strftime('%b %d %I:%M %p'),
    'api_status'     : 'ok',
    'api_error'      : '',
    'last_error_time': '',
    'scan_count'     : 0,
    'next_scan'      : '--',
    'scan_requested' : False,
    'scanning_now'   : False,
    'lost_signals'   : 0,
    # Candle Sync
    'candle_sync_active' : False,
    'next_candle_time'   : '--',   # HH:MM AM/PM of next 15m candle
    'candle_sync_count'  : 0,      # how many sync-scans fired
    'last_candle_sync'   : '--',   # last time a sync scan fired
}

last_trade_time = {}   # symbol -> datetime
client = None
bot_thread = None
tg_thread = None
PRECISION_CACHE = {}
KLINES_CACHE = {}

# ── Time helpers ───────────────────────────────────────────
def _t():    return datetime.now().astimezone().strftime('%I:%M %p')
def _dt():   return datetime.now().astimezone().strftime('%b %d %I:%M %p')
def _date(): return datetime.now().astimezone().strftime('%Y-%m-%d')

# ── Candle Sync Helpers ────────────────────────────────────
CANDLE_TF_MINUTES = 15   # bot runs on 15m candles
CANDLE_TF_MS      = CANDLE_TF_MINUTES * 60 * 1000  # in milliseconds

def _binance_server_time_ms():
    """
    Fetch current time from Binance server in milliseconds.
    Falls back to local time if API is unavailable (e.g. no internet).
    Using server time ensures candle alignment is always correct regardless
    of device clock drift or timezone issues.
    """
    try:
        if client:
            t = client.futures_time()
            return int(t['serverTime'])
    except Exception:
        pass
    # Fallback: local time in ms
    return int(datetime.now().timestamp() * 1000)

def seconds_to_next_candle(tf_minutes=CANDLE_TF_MINUTES):
    """
    Return seconds until the next candle opens, using Binance server time.
    This is immune to device clock drift and timezone bugs.
    """
    now_ms    = _binance_server_time_ms()
    tf_ms     = tf_minutes * 60 * 1000
    elapsed   = now_ms % tf_ms
    remaining = tf_ms - elapsed
    return remaining / 1000.0  # return as float seconds

def next_candle_open_time(tf_minutes=CANDLE_TF_MINUTES):
    """Return the local datetime when the next candle opens (for display)."""
    secs = seconds_to_next_candle(tf_minutes)
    return datetime.now() + timedelta(seconds=secs)

def _wait_for_internet(timeout_check_interval=5):
    """
    Block until we can reach the Binance API, checking every N seconds.
    Returns True when online, sets api_status so dashboard shows offline state.
    """
    while state.get('running', True):
        try:
            requests.get("https://fapi.binance.com/fapi/v1/ping", timeout=5)
            return True
        except Exception:
            state['api_status'] = 'error'
            state['api_error']  = 'No internet — waiting to reconnect...'
            log.info("🌐 Candle Sync: no internet, retrying in 5s...")
            for _ in range(timeout_check_interval):
                if not state.get('running', True):
                    return False
                time.sleep(1)
    return False

def candle_sync_loop():
    """
    Dedicated thread that fires an extra scan exactly at each 15m candle open.

    KEY IMPROVEMENTS vs old version:
    - Uses Binance SERVER TIME for candle alignment (immune to device clock drift)
    - Recalculates sleep time every second to self-correct if internet was down
    - After recovering from no-internet, fires an immediate scan so we never
      miss a candle open signal
    - Updates next_candle_time display continuously so dashboard is always fresh
    """
    log.info("🕯️  Candle Sync: thread started (server-time mode)")
    state['candle_sync_active'] = True

    while state.get('running', True):
        # ── Step 1: Get precise seconds to next candle from Binance ──
        try:
            secs = seconds_to_next_candle()
            nxt  = next_candle_open_time()
            state['next_candle_time'] = nxt.astimezone().strftime('%I:%M %p')
            log.info(f"🕯️  Candle Sync: next 15m candle in {secs:.1f}s at {state['next_candle_time']}")
        except Exception as e:
            log.info(f"🕯️  Candle Sync: failed to get server time ({e}), retrying in 10s")
            time.sleep(10)
            continue

        # ── Step 2: Sleep in 1-second ticks, rechecking each tick ────
        # This means if we wake up late (e.g. after internet drop), we immediately
        # fire the scan instead of drifting to the next cycle.
        fired = False
        while state.get('running', True):
            try:
                secs_left = seconds_to_next_candle()
            except Exception:
                # No internet — wait and try again
                time.sleep(1)
                continue

            # Update dashboard display every tick
            nxt = next_candle_open_time()
            state['next_candle_time'] = nxt.astimezone().strftime('%I:%M %p')

            if secs_left <= 1.0:
                # Candle is opening NOW — fire the scan
                fired = True
                break

            # Sleep at most 1 second so we stay responsive
            sleep_tick = min(secs_left - 0.5, 1.0)
            for _ in range(max(1, int(sleep_tick))):
                if not state.get('running', True):
                    state['candle_sync_active'] = False
                    return
                time.sleep(1)

        if not fired:
            state['candle_sync_active'] = False
            return

        # ── Step 3: Internet check before firing ──────────────────────
        # If we have no internet right now, wait until we do then scan immediately
        # (this catches the "bot reconnects mid-candle" case)
        internet_was_down = False
        try:
            requests.get("https://fapi.binance.com/fapi/v1/ping", timeout=4)
        except Exception:
            internet_was_down = True
            log.info("🌐 Candle Sync: no internet at candle open — waiting for reconnect...")
            if not _wait_for_internet():
                state['candle_sync_active'] = False
                return
            log.info("🌐 Candle Sync: internet back! Firing catch-up scan now")

        # ── Step 4: Fire the candle-sync scan ─────────────────────────
        state['candle_sync_count'] += 1
        state['last_candle_sync'] = _t()
        extra = " [CATCH-UP after reconnect]" if internet_was_down else ""
        log.info(f"🕯️  Candle Sync SCAN #{state['candle_sync_count']} at {_t()}{extra}")
        try:
            _update_positions()
            _saved_n = state['scan_count']
            do_scan(_saved_n)
            state['scan_count'] = _saved_n   # main loop owns this counter
            _note_api_ok()
        except Exception as e:
            log.info(f"Candle sync scan error: {e}")

        # Small buffer so the next seconds_to_next_candle() lands in the new candle
        time.sleep(3)

    state['candle_sync_active'] = False

# ── Indicators ─────────────────────────────────────────────
def ema(values, period):
    k=2.0/(period+1); r=[values[0]]
    for v in values[1:]: r.append(v*k+r[-1]*(1-k))
    return r

def sma(values, period):
    if len(values)<period: return sum(values)/len(values) if values else 0.0
    return sum(values[-period:])/period

def atr_calc(highs,lows,closes,period=14):
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
    if not trs: return closes[-1]*0.005
    if len(trs)<period: return sum(trs)/len(trs)
    a=sum(trs[:period])/period
    for t in trs[period:]: a=(a*(period-1)+t)/period
    return a

def atr_series(highs,lows,closes,period=14):
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
    if len(trs)<period: return trs
    out=[]; a=sum(trs[:period])/period; out.append(a)
    for t in trs[period:]:
        a=(a*(period-1)+t)/period; out.append(a)
    return out

def rsi_calc(closes, period=14):
    if len(closes)<period+1: return 50.0
    gains=[]; losses=[]
    for i in range(1,len(closes)):
        d=closes[i]-closes[i-1]
        gains.append(max(d,0.0)); losses.append(max(-d,0.0))
    ag=sum(gains[:period])/period; al=sum(losses[:period])/period
    for i in range(period,len(gains)):
        ag=(ag*(period-1)+gains[i])/period
        al=(al*(period-1)+losses[i])/period
    if al==0: return 100.0
    rs=ag/al
    return 100-(100/(1+rs))

def adx_calc(highs,lows,closes,period=14):
    if len(closes)<period*3: return 0.0,0.0,0.0
    pdm,mdm,trs=[],[],[]
    for i in range(1,len(closes)):
        up=highs[i]-highs[i-1]; down=lows[i-1]-lows[i]
        pdm.append(up if up>down and up>0 else 0.0)
        mdm.append(down if down>up and down>0 else 0.0)
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
    def ws(v,p):
        if len(v)<p: return []
        r=[sum(v[:p])]
        for x in v[p:]: r.append(r[-1]-r[-1]/p+x)
        return r
    st=ws(trs,period); sp=ws(pdm,period); sm=ws(mdm,period)
    if not st: return 0.0,0.0,0.0
    pdi=[100*p/t if t else 0 for p,t in zip(sp,st)]
    mdi=[100*m/t if t else 0 for m,t in zip(sm,st)]
    dx=[100*abs(p-m)/(p+m) if (p+m) else 0 for p,m in zip(pdi,mdi)]
    if len(dx)<period: return 0.0,pdi[-1],mdi[-1]
    adx=sum(dx[:period])/period
    for d in dx[period:]: adx=(adx*(period-1)+d)/period
    return max(0.0,min(100.0,adx)),pdi[-1],mdi[-1]

def display_name(sym): return sym.replace('1000000','').replace('1000','').replace('USDT','')

def _record_signal(symbol, signal, reason, extra=None):
    state['last_signals'][symbol] = {
        'symbol': symbol, 'signal': signal,
        'reason': reason, 'time': _t(), **(extra or {}),
    }

# ── Signal — Variant G ─────────────────────────────────────
def check_signal_G(symbol, closes, highs, lows):
    if len(closes) < 70:
        return None, 'NOT ENOUGH DATA', {}

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    i   = len(closes) - 2   # last CLOSED bar

    # Filter 1: EMA50 slope
    if i < 10:
        return None, 'NOT ENOUGH DATA', {}
    slope_pct = (e50[i] - e50[i-10]) / e50[i-10] * 100
    trend_up   = slope_pct >  0.05
    trend_down = slope_pct < -0.05
    trend_arrow = '↑' if trend_up else ('↓' if trend_down else '→')

    if not trend_up and not trend_down:
        adx_val,_,_ = adx_calc(highs, lows, closes, 14)
        return None, 'NO TREND', {'adx': round(adx_val,1), 'trend': trend_arrow}

    # Filter 2: EMA9/21 cross on closed bar
    crossed_up   = e9[i] > e21[i] and e9[i-1] <= e21[i-1]
    crossed_down = e9[i] < e21[i] and e9[i-1] >= e21[i-1]
    crossed_icon = '✅' if (crossed_up or crossed_down) else '❌'

    if not crossed_up and not crossed_down:
        adx_val,_,_ = adx_calc(highs, lows, closes, 14)
        reason = 'WAIT ↑' if trend_up else 'WAIT ↓'
        return None, reason, {'adx': round(adx_val,1), 'trend': trend_arrow, 'crossed': crossed_icon}

    if trend_up and not crossed_up:
        adx_val,_,_ = adx_calc(highs, lows, closes, 14)
        return None, 'WAIT ↑', {'adx': round(adx_val,1), 'trend': trend_arrow, 'crossed': crossed_icon}
    if trend_down and not crossed_down:
        adx_val,_,_ = adx_calc(highs, lows, closes, 14)
        return None, 'WAIT ↓', {'adx': round(adx_val,1), 'trend': trend_arrow, 'crossed': crossed_icon}

    # Filter 3: ADX >= 22
    adx_val,_,_ = adx_calc(highs, lows, closes, 14)
    if adx_val < 22:
        return None, f'ADX {adx_val:.0f}<22', {'adx': round(adx_val,1), 'trend': trend_arrow, 'crossed': crossed_icon}

    sig = 'buy' if crossed_up else 'sell'
    return sig, sig.upper(), {'adx': round(adx_val,1), 'slope': round(slope_pct,3), 'trend': trend_arrow, 'crossed': crossed_icon}

# ── Stats helpers ──────────────────────────────────────────
def _pf(gross_profit, gross_loss):
    if gross_loss>0: return gross_profit/gross_loss
    return 0.0 if gross_profit==0 else float('inf')

def _today_str():
    return datetime.now().astimezone().strftime('%Y-%m-%d')

def overall_stats():
    today=_today_str()
    gp=gl=0.0; wins=losses=0; today_pnl=0.0
    for t in state['trades']:
        if t['status']!='closed': continue
        pnl=t['pnl']
        if pnl>0: gp+=pnl; wins+=1
        elif pnl<0: gl+=abs(pnl); losses+=1
        if t.get('closed_date')==today: today_pnl+=pnl
    total=wins+losses
    return {'total_pnl':state['total_pnl'],'today_pnl':today_pnl,'wins':wins,'losses':losses,
            'wr':wins/total*100 if total else 0,'pf':_pf(gp,gl)}

def symbol_stats(symbol=None):
    today=_today_str()
    stats = defaultdict(lambda:{'wins':0,'losses':0,'total_pnl':0.0,'today_pnl':0.0,'gp':0.0,'gl':0.0})
    for t in state['trades']:
        if t['status']!='closed': continue
        sym=t['symbol']
        if symbol and sym!=symbol: continue
        d=stats[sym]; pnl=t['pnl']
        d['total_pnl']+=pnl
        if pnl>0: d['wins']+=1; d['gp']+=pnl
        elif pnl<0: d['losses']+=1; d['gl']+=abs(pnl)
        if t.get('closed_date')==today: d['today_pnl']+=pnl
    out={}
    for sym,d in stats.items():
        total=d['wins']+d['losses']
        out[sym]={**d,'wr':d['wins']/total*100 if total else 0,'total':total,'pf':_pf(d['gp'],d['gl'])}
    if symbol:
        return out.get(symbol,{'wins':0,'losses':0,'total_pnl':0.0,'today_pnl':0.0,'wr':0,'total':0,'pf':0.0})
    return out

def coin_stats():
    stats = defaultdict(lambda:{'wins':0,'losses':0,'total_pnl':0.0,'symbol':None})
    for t in state['trades']:
        if t['status']!='closed': continue
        key = display_name(t['symbol'])
        stats[key]['total_pnl'] += t['pnl']
        stats[key]['symbol'] = t['symbol']
        if t['pnl']>0: stats[key]['wins']+=1
        elif t['pnl']<0: stats[key]['losses']+=1
    result={}
    for key,d in stats.items():
        total=d['wins']+d['losses']
        result[key]={**d,'wr':d['wins']/total*100 if total>0 else 0,'total':total}
    return sorted(result.items(),key=lambda x:x[1]['wr'],reverse=True)

def daily_pnl_series(days=14):
    from datetime import date
    totals = defaultdict(float)
    for t in state['trades']:
        if t['status']!='closed': continue
        d = t.get('closed_date')
        if d: totals[d]+=t['pnl']
    today = date.today()
    out=[]
    for i in range(days-1,-1,-1):
        d = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        label = (today - timedelta(days=i)).strftime('%m/%d')
        out.append({'date':d,'label':label,'pnl':round(totals.get(d,0.0),4)})
    return out

def advanced_stats():
    closed = [t for t in state['trades'] if t['status']=='closed']
    if not closed:
        return {'equity_curve':[], 'max_drawdown':0.0, 'max_drawdown_pct':0.0,
                'cur_streak':0, 'cur_streak_type':'--', 'max_win_streak':0, 'max_loss_streak':0,
                'avg_win':0.0, 'avg_loss':0.0, 'best_trade':None, 'worst_trade':None,
                'avg_rr':0.0, 'total_trades':0}

    equity=[]; running=0.0; peak=0.0; max_dd=0.0; max_dd_pct=0.0
    for t in closed:
        running += t['pnl']
        peak = max(peak, running)
        dd = peak - running
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = (dd/peak*100) if peak>0 else 0.0
        equity.append(round(running,4))

    wins=[t['pnl'] for t in closed if t['pnl']>0]
    losses=[t['pnl'] for t in closed if t['pnl']<0]
    avg_win = sum(wins)/len(wins) if wins else 0.0
    avg_loss = sum(losses)/len(losses) if losses else 0.0
    avg_rr = abs(avg_win/avg_loss) if avg_loss else 0.0

    cur_streak=0; cur_type='--'
    for t in reversed(closed):
        kind = 'win' if t['pnl']>0 else ('loss' if t['pnl']<0 else None)
        if kind is None: break
        if cur_type=='--': cur_type=kind
        if kind!=cur_type: break
        cur_streak+=1

    max_w=cur_w=0; max_l=cur_l=0
    for t in closed:
        if t['pnl']>0: cur_w+=1; cur_l=0; max_w=max(max_w,cur_w)
        elif t['pnl']<0: cur_l+=1; cur_w=0; max_l=max(max_l,cur_l)
        else: cur_w=cur_l=0

    best = max(closed, key=lambda t:t['pnl'])
    worst = min(closed, key=lambda t:t['pnl'])

    return {
        'equity_curve': equity, 'max_drawdown': round(max_dd,4), 'max_drawdown_pct': round(max_dd_pct,2),
        'cur_streak': cur_streak, 'cur_streak_type': cur_type,
        'max_win_streak': max_w, 'max_loss_streak': max_l,
        'avg_win': round(avg_win,4), 'avg_loss': round(avg_loss,4), 'avg_rr': round(avg_rr,2),
        'best_trade': {'symbol':display_name(best['symbol']),'pnl':best['pnl']},
        'worst_trade': {'symbol':display_name(worst['symbol']),'pnl':worst['pnl']},
        'total_trades': len(closed),
    }

# ── Binance client bootstrap ───────────────────────────────
def init_client():
    global client
    cfg = load_config()
    from binance.client import Client
    client = Client(cfg.get('api_key',''), cfg.get('api_secret',''))
    return client

def _balance():
    try:
        info = client.futures_account_balance()
        for b in info:
            if b['asset']=='USDT': return float(b['balance'])
    except Exception as e:
        _note_api_error(str(e))
    return None

def _note_api_error(msg):
    state['api_status']='error'; state['api_error']=msg[:200]
    state['last_error_time']=_t()
    log.info(f"API ERROR: {msg[:200]}")

def _note_api_ok():
    state['api_status']='ok'; state['api_error']=''

# ── Symbol precision cache ─────────────────────────────────
def prime_precision_cache():
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] in SYMBOLS:
                qty_p = price_p = 0
                for f in s['filters']:
                    if f['filterType']=='LOT_SIZE':
                        step=f['stepSize'].rstrip('0')
                        qty_p = len(step.split('.')[1]) if '.' in step else 0
                    if f['filterType']=='PRICE_FILTER':
                        tick=f['tickSize'].rstrip('0')
                        price_p = len(tick.split('.')[1]) if '.' in tick else 0
                PRECISION_CACHE[s['symbol']]={'qty_p':qty_p,'price_p':price_p}
        _note_api_ok()
    except Exception as e:
        _note_api_error(str(e))

def _round_qty(symbol, price, margin_usd, leverage):
    notional = margin_usd*leverage
    qty = notional/price
    p = PRECISION_CACHE.get(symbol,{}).get('qty_p',3)
    qty = math.floor(qty*(10**p))/(10**p)
    return qty

def _round_price(symbol, price):
    p = PRECISION_CACHE.get(symbol,{}).get('price_p',4)
    return round(price,p)

def set_leverage_once(symbol, leverage):
    try: client.futures_change_leverage(symbol=symbol, leverage=leverage)
    except Exception: pass

def set_margin_type_once(symbol, margin_type):
    try: client.futures_change_margin_type(symbol=symbol, marginType=margin_type)
    except Exception: pass

def apply_margin_type_all():
    mt = get_settings()['margin_type']
    for sym in SYMBOLS:
        set_margin_type_once(sym, mt)
    for sym in extra_signals_list():
        set_margin_type_once(sym, mt)

def effective_margin_usd(settings=None):
    s = settings or get_settings()
    if s['margin_mode']=='percent':
        bal = state['balance'] or 0.0
        return max(0.0, round(bal * s['margin_percent']/100.0, 4))
    return s['margin_usd']

# ── Kline fetch with per-scan cache ───────────────────────
def fetch_klines_cached(symbol, timeframe, limit=150):
    key=(symbol,timeframe)
    if key in KLINES_CACHE: return KLINES_CACHE[key]
    raw = client.futures_klines(symbol=symbol, interval=timeframe, limit=limit)
    closes=[float(k[4]) for k in raw]
    highs =[float(k[2]) for k in raw]
    lows  =[float(k[3]) for k in raw]
    vols  =[float(k[5]) for k in raw]
    data=(closes,highs,lows,vols)
    KLINES_CACHE[key]=data
    time.sleep(0.15)
    return data

# ── History persistence ────────────────────────────────────
def load_history():
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH) as f:
                d=json.load(f)
                state['trades']=d.get('trades',[])
                state['total_pnl']=d.get('total_pnl',0.0)
                state['wins']=d.get('wins',0)
                state['losses']=d.get('losses',0)
        except Exception as e:
            log.info(f"history load failed: {e}")

def save_history():
    try:
        with open(HISTORY_PATH,'w') as f:
            json.dump({'trades':state['trades'][-2000:],
                       'total_pnl':state['total_pnl'],
                       'wins':state['wins'],'losses':state['losses']}, f)
    except Exception as e:
        log.info(f"history save failed: {e}")

# ── Cooldown helpers ───────────────────────────────────────
def in_cooldown(symbol, cooldown_min):
    t = last_trade_time.get(symbol)
    if not t: return False
    return (datetime.now()-t) < timedelta(minutes=cooldown_min)

def coin_locked(symbol):
    return symbol in state['open_positions']

def mark_cooldown(symbol):
    last_trade_time[symbol] = datetime.now()

# ── Trade placement ────────────────────────────────────────
def _place(symbol, side, qty, price, leverage):
    if qty<=0: return False
    cfg = STRATEGY_G
    try:
        set_leverage_once(symbol, leverage)
        order_side = 'BUY' if side=='buy' else 'SELL'
        client.futures_create_order(symbol=symbol, side=order_side, type='MARKET', quantity=qty)
        entry = price
        tp_pct = cfg['tp']; sl_pct = cfg['sl']
        if side=='buy':
            tp = _round_price(symbol, entry * (1 + tp_pct))
            sl = _round_price(symbol, entry * (1 - sl_pct))
        else:
            tp = _round_price(symbol, entry * (1 - tp_pct))
            sl = _round_price(symbol, entry * (1 + sl_pct))

        tp_side = 'SELL' if side=='buy' else 'BUY'
        tp_order = client.futures_create_order(symbol=symbol, side=tp_side, type='TAKE_PROFIT_MARKET',
                                                stopPrice=tp, closePosition=True)
        sl_order = client.futures_create_order(symbol=symbol, side=tp_side, type='STOP_MARKET',
                                                stopPrice=sl, closePosition=True)

        state['open_positions'][symbol] = {
            'symbol':symbol,'side':side,'qty':qty,'entry':entry,'tp':tp,'sl':sl,
            'strategy':'G','opened':_dt(),'opened_ts':time.time(),'meta':{},
        }
        state['pos_orders'][symbol] = {'tp_id':tp_order.get('orderId'),'sl_id':sl_order.get('orderId')}
        log.info(f"OPENED {symbol} {side} qty={qty} entry={entry} tp={tp} sl={sl}")
        send_telegram(
            f"🟢 <b>G Max V1</b> opened {display_name(symbol)} {side.upper()}\n"
            f"Entry: {entry}  TP: {tp}  SL: {sl}\nQty: {qty}"
        )
        return True
    except Exception as e:
        _note_api_error(str(e))
        log.info(f"PLACE FAILED {symbol} {side}: {e}")
        return False

def close_position(symbol, reason='manual'):
    pos = state['open_positions'].get(symbol)
    if not pos: return False
    try:
        close_side = 'SELL' if pos['side']=='buy' else 'BUY'
        client.futures_create_order(symbol=symbol, side=close_side, type='MARKET',
                                     quantity=pos['qty'], reduceOnly=True)
        for oid_key in ('tp_id','sl_id'):
            oid = state['pos_orders'].get(symbol,{}).get(oid_key)
            if oid:
                try: client.futures_cancel_order(symbol=symbol, orderId=oid)
                except Exception: pass
        _finalize_close(symbol, pos, reason, exit_price=None)
        return True
    except Exception as e:
        _note_api_error(str(e))
        return False

def _finalize_close(symbol, pos, reason, exit_price=None):
    try:
        if exit_price is None:
            t = client.futures_symbol_ticker(symbol=symbol)
            exit_price = float(t['price'])
    except Exception:
        exit_price = pos['entry']
    if pos['side']=='buy':
        pnl = (exit_price-pos['entry'])*pos['qty']
    else:
        pnl = (pos['entry']-exit_price)*pos['qty']

    trade = {**pos,'status':'closed','exit':exit_price,'pnl':round(pnl,4),
              'closed':_dt(),'closed_date':_today_str(),'reason':reason}
    state['trades'].append(trade)
    state['total_pnl']+=pnl
    state['today_pnl']+=pnl
    if pnl>0: state['wins']+=1
    elif pnl<0: state['losses']+=1
    save_history()

    mark_cooldown(symbol)
    state['open_positions'].pop(symbol,None)
    state['pos_orders'].pop(symbol,None)

    emoji = '✅' if pnl>0 else ('❌' if pnl<0 else '➖')
    send_telegram(
        f"{emoji} <b>G Max V1</b> closed {display_name(symbol)} ({reason})\n"
        f"PnL: {round(pnl,3)} USDT  Entry: {pos['entry']}  Exit: {exit_price}"
    )
    log.info(f"CLOSED {symbol} pnl={round(pnl,4)} reason={reason}")

def check_existing_positions():
    try:
        positions = client.futures_position_information()
        for p in positions:
            amt = float(p['positionAmt'])
            if amt==0: continue
            symbol = p['symbol']
            if symbol not in SYMBOLS: continue
            side = 'buy' if amt>0 else 'sell'
            state['open_positions'][symbol] = {
                'symbol':symbol,'side':side,'qty':abs(amt),
                'entry':float(p['entryPrice']),'tp':None,'sl':None,
                'strategy':'G','opened':_dt(),'opened_ts':time.time(),'meta':{},
            }
    except Exception as e:
        _note_api_error(str(e))

def _update_positions():
    for symbol in list(state['open_positions'].keys()):
        pos = state['open_positions'].get(symbol)
        if not pos: continue
        try:
            pos_info = client.futures_position_information(symbol=symbol)
            amt = float(pos_info[0]['positionAmt']) if pos_info else 0.0
            if amt==0:
                pos = state['open_positions'][symbol]
                mark_price = None
                try:
                    t = client.futures_symbol_ticker(symbol=symbol)
                    mark_price = float(t['price'])
                except Exception: pass
                _finalize_close(symbol, pos, 'tp_sl_hit', exit_price=mark_price)
                continue
            if time.time() - pos.get('opened_ts', time.time()) >= MAX_HOLD_SECONDS:
                close_position(symbol, reason='max_hold')
        except Exception as e:
            _note_api_error(str(e))

# ── Scan loop ──────────────────────────────────────────────
def do_scan(n):
    KLINES_CACHE.clear()
    state['scanning_now']=True
    s = get_settings()
    bal = _balance()
    if bal is not None: state['balance']=bal
    margin = effective_margin_usd(s)
    lev, cooldown = s['leverage'], s['cooldown_min']

    coins = all_trading_coins()
    for symbol in coins:
        if symbol not in PRECISION_CACHE:
            continue
        if not is_coin_enabled(symbol):
            continue
        if coin_locked(symbol):
            continue
        if in_cooldown(symbol, cooldown):
            continue
        try:
            closes, highs, lows, _ = fetch_klines_cached(symbol, STRATEGY_G['tf'])
        except Exception as e:
            log.info(f"kline error {symbol}: {e}")
            continue
        try:
            sig, reason, extra = check_signal_G(symbol, closes, highs, lows)
        except Exception as e:
            log.info(f"signal error {symbol}: {e}")
            continue
        _record_signal(symbol, sig, reason, extra)
        if not sig: continue

        # ── Post signal to channel (always, regardless of trade placement) ──
        entry_price = closes[-1]
        cfg_tp = STRATEGY_G['tp']; cfg_sl = STRATEGY_G['sl']
        if sig == 'buy':
            tp_price = _round_price(symbol, entry_price * (1 + cfg_tp))
            sl_price = _round_price(symbol, entry_price * (1 - cfg_sl))
        else:
            tp_price = _round_price(symbol, entry_price * (1 - cfg_tp))
            sl_price = _round_price(symbol, entry_price * (1 + cfg_sl))
        send_signal_post(symbol, sig, entry_price, tp_price, sl_price, lev)
        # ────────────────────────────────────────────────────────────────────

        qty = _round_qty(symbol, closes[-1], margin, lev)
        if qty>0:
            _place(symbol, sig, qty, closes[-1], lev)

    state['scan_count']=n
    state['last_update']=_dt()
    state['scanning_now']=False

def bot_loop():
    init_client()
    prime_precision_cache()
    for sym in extra_signals_list():
        if sym not in PRECISION_CACHE:
            prime_symbol_precision(sym)
    for sym in SYMBOLS:
        set_leverage_once(sym, get_settings()['leverage'])
    for sym in extra_signals_list():
        set_leverage_once(sym, get_settings()['leverage'])
    apply_margin_type_all()
    check_existing_positions()
    load_history()

    # ── Launch Candle Sync thread ──────────────────────────
    try:
        secs_until = seconds_to_next_candle()   # uses Binance server time
        nxt_time   = next_candle_open_time().astimezone().strftime('%I:%M %p')
        log.info(f"🕯️  Candle Sync: syncing to next 15m candle in {secs_until:.1f}s (at {nxt_time}) [Binance server time]")
        state['next_candle_time'] = nxt_time
    except Exception as e:
        log.info(f"🕯️  Candle Sync: could not fetch server time at startup ({e}), sync thread will retry")
    sync_thread = threading.Thread(target=candle_sync_loop, daemon=True)
    sync_thread.start()
    # ───────────────────────────────────────────────────────

    n=0
    while state.get('running', True):
        try:
            n+=1
            _update_positions()
            do_scan(n)
        except Exception as e:
            _note_api_error(str(e))
            log.info(f"loop error: {e}")
        wait = get_settings()['scan_every']
        nxt = datetime.now()+timedelta(seconds=wait)
        state['next_scan']=nxt.astimezone().strftime('%I:%M %p')
        for _ in range(wait):
            if not state.get('running', True): break
            if state.get('scan_requested'):
                state['scan_requested']=False
                break
            time.sleep(1)

def start_bot():
    global bot_thread
    if bot_thread and bot_thread.is_alive():
        return False
    state['running']=True
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    return True

def start_tg_poll_once():
    global tg_thread
    if tg_thread and tg_thread.is_alive():
        return False
    tg_thread = threading.Thread(target=tg_poll, daemon=True)
    tg_thread.start()
    return True

def stop_bot():
    state['running']=False
    return True

def is_running():
    return bool(bot_thread and bot_thread.is_alive())

# ── Telegram core ──────────────────────────────────────────
def tg_admin():
    return int(load_config().get('tg_chat_id',0) or 0)

def _tg_api():
    tok = load_config().get('tg_token','')
    return f"https://api.telegram.org/bot{tok}" if tok else None

def get_local_ip():
    try:
        s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8",80)); ip=s.getsockname()[0]; s.close()
        return ip
    except Exception:
        return "127.0.0.1"

def menu_kb():
    return {"inline_keyboard":[
        [{"text":"📊 Status","callback_data":"status"},{"text":"💼 Positions","callback_data":"positions"}],
        [{"text":"📜 History","callback_data":"history"},{"text":"🏆 Top Coins","callback_data":"topcoins"}],
        [{"text":"⚙️ Settings","callback_data":"settings_info"},{"text":"🌐 Mode","callback_data":"mode"}],
        [{"text":"⚡ Scan Now","callback_data":"scan"},{"text":"📍 Get IP","callback_data":"getip"}],
        [{"text":"🔄 Refresh","callback_data":"refresh"}],
    ]}

def tg_send(text, markup=None):
    api=_tg_api(); admin=tg_admin()
    if not api or not admin: return
    try:
        payload={'chat_id':admin,'text':text,'parse_mode':'HTML'}
        if markup: payload['reply_markup']=json.dumps(markup)
        requests.post(f"{api}/sendMessage", data=payload, timeout=10)
    except Exception as e: log.info(f"tg_send fail: {e}")

def tg_edit(mid, text, markup=None):
    api=_tg_api(); admin=tg_admin()
    if not api or not admin: return
    try:
        payload={'chat_id':admin,'message_id':mid,'text':text,'parse_mode':'HTML'}
        if markup: payload['reply_markup']=json.dumps(markup)
        requests.post(f"{api}/editMessageText", data=payload, timeout=10)
    except Exception: pass

def tg_answer(cb_id, text=None):
    api=_tg_api()
    if not api: return
    try:
        payload={'callback_query_id':cb_id}
        if text: payload['text']=text
        requests.post(f"{api}/answerCallbackQuery", data=payload, timeout=10)
    except Exception: pass

def send_telegram(text):
    tg_send(text)

# ── Signal Channel Poster ──────────────────────────────────
def _signal_channel_id():
    """Return the configured signal channel chat ID / username, or None."""
    cfg = load_config()
    if not cfg.get('share_signals', False):
        return None
    ch = (cfg.get('signal_channel') or '').strip()
    return ch if ch else None

def send_signal_post(symbol, sig, entry_price, tp_price, sl_price, leverage):
    """
    Post a clean signal card to the configured Telegram channel.
    Called on every confirmed signal regardless of whether the bot
    actually placed a trade (e.g. no margin left, coin locked, etc.).
    """
    ch = _signal_channel_id()
    api = _tg_api()
    if not ch or not api:
        return

    direction = 'Long/Buy' if sig == 'buy' else 'Short/Sell'
    coin = display_name(symbol)

    text = (
        f"{'🟢' if sig == 'buy' else '🔴'} <b>{direction}</b>\n"
        f"<b>{coin}/USDT</b>\n\n"
        f"Entry Point - <code>{entry_price}</code>\n\n"
        f"Targets: <code>{tp_price}</code>\n"
        f"Leverage - {leverage}x\n"
        f"Stop Loss - <code>{sl_price}</code>"
    )

    try:
        payload = {
            'chat_id'   : ch,
            'text'      : text,
            'parse_mode': 'HTML',
        }
        r = requests.post(f"{api}/sendMessage", data=payload, timeout=10)
        if r.status_code == 200:
            log.info(f"📢 Signal posted to channel: {coin} {sig.upper()}")
        else:
            log.info(f"📢 Signal channel post failed {r.status_code}: {r.text[:100]}")
    except Exception as e:
        log.info(f"📢 Signal channel error: {e}")

# ── Telegram formatting ────────────────────────────────────
def fmt_status():
    lines=["🤖 <b>G MAX V1</b> — Engineered by Paqu","━━━━━━━━━━━━━━━━━━"]
    lines.append(f"💰 Balance: <code>${state['balance']:.2f}</code>")
    lines.append(f"📈 Total PnL: <code>${state['total_pnl']:+.4f}</code>")
    w,l=state['wins'],state['losses']; tot=w+l
    wr=f"{w/tot*100:.0f}%" if tot else "--"
    lines.append(f"🎯 W:{w} L:{l} WR:{wr}")
    lines.append(f"💼 Open positions: {len(state['open_positions'])}")
    lines.append(f"🌐 Mode: {current_mode_label()}")
    if state['api_status']=='error':
        lines.append(f"\n❌ <b>API ERROR:</b> {state['api_error']}")
    lines.append(f"\n🔄 Scan #{state['scan_count']} | Next: {state['next_scan']}")
    lines.append(f"🕐 Updated: {_t()}")
    return '\n'.join(lines)

def fmt_positions():
    if not state['open_positions']:
        return "💼 <b>No open positions</b>"
    lines=["💼 <b>OPEN POSITIONS</b>","━━━━━━━━━━━━━━━━━━"]
    for sym,pos in state['open_positions'].items():
        side='🟢 LONG' if pos['side']=='buy' else '🔴 SHORT'
        lines.append(
            f"💎 <b>{display_name(sym)}</b> [G] {side}\n"
            f"   Entry: <code>${pos['entry']:.5f}</code>  TP:{pos.get('tp')} SL:{pos.get('sl')}\n"
            f"   Opened: {pos.get('opened','--')}"
        )
    return '\n'.join(lines)

def fmt_history():
    if not state['trades']:
        return "📜 <b>No trades yet</b>"
    lines=["📜 <b>RECENT TRADES</b>","━━━━━━━━━━━━━━━━━━"]
    for t in reversed(state['trades'][-8:]):
        name=display_name(t['symbol'])
        icon='🟢' if t['side']=='buy' else '🔴'
        pnl=t['pnl']
        badge='✅ WIN' if pnl>0 else ('❌ LOSS' if pnl<0 else '➖')
        lines.append(f"{icon} <b>{name}</b> [G] {badge} <code>${pnl:+.4f}</code> {t.get('closed','')}")
    w,l=state['wins'],state['losses']; tot=w+l
    if tot>0:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(f"W:{w} L:{l} WR:{w/tot*100:.0f}% | Net: <code>${state['total_pnl']:+.4f}</code>")
    lines.append(f"\n📊 Full history: <code>http://{get_local_ip()}:5000/history</code>")
    return '\n'.join(lines)

def fmt_top_coins():
    stats=coin_stats()
    if not stats:
        return "🏆 <b>No completed trades yet</b>"
    lines=["🏆 <b>TOP COINS BY WIN RATE</b>","━━━━━━━━━━━━━━━━━━"]
    for i,(name,d) in enumerate(stats[:10],1):
        bar='🥇' if i==1 else ('🥈' if i==2 else ('🥉' if i==3 else f"{i}."))
        lines.append(
            f"{bar} <b>{name}</b> — {d['wr']:.0f}% WR "
            f"({d['wins']}W/{d['losses']}L) <code>${d['total_pnl']:+.4f}</code>"
        )
    return '\n'.join(lines)

def fmt_settings_info():
    s=get_settings()
    return (
        f"⚙️ <b>CURRENT SETTINGS</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💵 Margin/trade: ${s['margin_usd']}\n"
        f"⚡ Leverage: {s['leverage']}x\n"
        f"⏰ Cooldown: {s['cooldown_min']} min per coin\n"
        f"🔄 Scan every: {s['scan_every']}s\n\n"
        f"Change settings:\n"
        f"<code>http://{get_local_ip()}:5000/settings</code>"
    )

def fmt_mode():
    mode = load_config().get('coin_mode', 'whitelist')
    if mode == 'whitelist':
        return f"🎯 <b>Whitelist Mode</b> — {len(COINS_WHITELIST)} coins\nTop-performing coins only (PF ≥ 1.5)"
    return f"🌐 <b>Universe Mode</b> — {len(COINS_UNIVERSE)} coins\nFull coin universe"

def handle_tg_update(upd):
    admin=tg_admin()
    if 'message' in upd:
        msg=upd['message']
        if msg['chat']['id']!=admin: return
        text=msg.get('text','')
        if text in ['/start','/menu']: tg_send(fmt_status(),markup=menu_kb())
        elif text=='/status':    tg_send(fmt_status())
        elif text=='/positions': tg_send(fmt_positions())
        elif text=='/history':   tg_send(fmt_history())
        elif text=='/top':       tg_send(fmt_top_coins())
        elif text=='/settings':  tg_send(fmt_settings_info())
        elif text=='/mode':      tg_send(fmt_mode())
        elif text in ['/getip','/ip']:
            ip=get_local_ip(); tg_send(f"📍 Dashboard: <code>http://{ip}:5000</code>")
        elif text=='/scan':
            state['scan_requested']=True; tg_send("⚡ Scan triggered!")
        else: tg_send("Send /menu for control panel",markup=menu_kb())

    elif 'callback_query' in upd:
        cb=upd['callback_query']
        if cb['from']['id']!=admin: return
        data=cb['data']; mid=cb['message']['message_id']
        tg_answer(cb['id'])
        if   data=='status':       tg_edit(mid,fmt_status(),    markup=menu_kb())
        elif data=='positions':    tg_edit(mid,fmt_positions(), markup=menu_kb())
        elif data=='history':      tg_edit(mid,fmt_history(),   markup=menu_kb())
        elif data=='topcoins':     tg_edit(mid,fmt_top_coins(), markup=menu_kb())
        elif data=='settings_info':tg_edit(mid,fmt_settings_info(),markup=menu_kb())
        elif data=='mode':         tg_edit(mid,fmt_mode(),      markup=menu_kb())
        elif data=='refresh':      tg_edit(mid,fmt_status(),    markup=menu_kb())
        elif data=='getip':
            ip=get_local_ip()
            tg_edit(mid, f"📍 <b>Dashboard</b>\n<code>http://{ip}:5000</code>",markup=menu_kb())
        elif data=='scan':
            state['scan_requested']=True
            tg_answer(cb['id'],'⚡ Scan triggered!')
            tg_edit(mid,fmt_status(),markup=menu_kb())

def tg_poll():
    api=_tg_api()
    if not api: log.warning("📱 TG skipped (no token)"); return
    log.info("📱 Telegram polling started")
    try:
        requests.get(f"{api}/getUpdates", params={'offset':-1,'timeout':1}, timeout=5)
    except Exception: pass
    ip=get_local_ip(); time.sleep(5)
    mode_label = current_mode_label()
    tg_send(
        f"🤖 <b>G Max V1 online 🟢</b> | Engineered by Paqu\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Strategy G · 15m | TP 3% · SL 15%\n"
        f"🌐 {mode_label}\n"
        f"💵 ${get_settings()['margin_usd']} margin @ {get_settings()['leverage']}x/trade\n"
        f"🔒 1 open position per coin\n\n"
        f"📱 <code>http://{ip}:5000</code>",
        markup=menu_kb()
    )
    offset=0; fail=0
    while True:
        try:
            r=requests.get(f"{api}/getUpdates",
                params={'offset':offset,'timeout':30,
                        'allowed_updates':['message','callback_query']}, timeout=35)
            if r.status_code==200:
                fail=0
                for upd in r.json().get('result',[]):
                    offset=upd['update_id']+1
                    try: handle_tg_update(upd)
                    except Exception as e: log.error(f"TG handle: {e}")
            else:
                fail+=1; time.sleep(min(fail*5,60))
        except requests.exceptions.Timeout: pass
        except Exception as e:
            log.error(f"TG poll: {e}"); fail+=1; time.sleep(min(fail*5,60))

# ── Flask App ──────────────────────────────────────────────
app=Flask(__name__)

THEME_CSS = '''
:root{
  --bg:#070d1a; --card:#0d1526; --border:#1a2840;
  --green:#00e87a; --red:#ff4060; --yellow:#ffd700; --blue:#4499ff;
  --text:#ffffff; --textdim:#aabbdd; --textdim2:#7788aa; --faint:#445566;
  --orange:#ff8800; --tg:#229ED9;
  --green-rgb:0,232,122; --red-rgb:255,64,96; --yellow-rgb:255,215,0; --blue-rgb:68,153,255;
  --orange-rgb:255,136,0; --tg-rgb:34,158,217; --border-rgb:26,40,64; --card-rgb:13,21,38;
  --font:'Courier New',monospace;
}
body[data-theme="cyber"]{
  --bg:#05010f; --card:#12082a; --border:#2d1b54;
  --green:#00ffcc; --red:#ff2079; --yellow:#ffe93a; --blue:#9d7bff;
  --text:#f2ecff; --textdim:#b9a8e0; --textdim2:#8874b8; --faint:#4a3a72;
  --orange:#ff9d00; --tg:#229ED9;
  --green-rgb:0,255,204; --red-rgb:255,32,121; --yellow-rgb:255,233,58; --blue-rgb:157,123,255;
  --orange-rgb:255,157,0; --tg-rgb:34,158,217; --border-rgb:45,27,84; --card-rgb:18,8,42;
}
body[data-theme="aurora"]{
  --bg:#071019; --card:#0e1f2b; --border:#1c3b4d;
  --green:#2dd4bf; --red:#f43f5e; --yellow:#fbbf24; --blue:#60a5fa;
  --text:#eaf6f6; --textdim:#9fc9c9; --textdim2:#6b98a0; --faint:#33525c;
  --orange:#fb923c; --tg:#229ED9;
  --green-rgb:45,212,191; --red-rgb:244,63,94; --yellow-rgb:251,191,36; --blue-rgb:96,165,250;
  --orange-rgb:251,146,60; --tg-rgb:34,158,217; --border-rgb:28,59,77; --card-rgb:14,31,43;
}
body[data-theme="solar"]{
  --bg:#f5f7fb; --card:#ffffff; --border:#dde3ec;
  --green:#059669; --red:#dc2626; --yellow:#b45309; --blue:#2563eb;
  --text:#10151f; --textdim:#334155; --textdim2:#586173; --faint:#7c8695;
  --orange:#c2410c; --tg:#229ED9;
  --green-rgb:5,150,105; --red-rgb:220,38,38; --yellow-rgb:180,83,9; --blue-rgb:37,99,235;
  --orange-rgb:194,65,12; --tg-rgb:34,158,217; --border-rgb:221,227,236; --card-rgb:255,255,255;
}
body[data-theme="matrix"]{
  --bg:#000000; --card:#050f05; --border:#113311;
  --green:#00ff41; --red:#ff3333; --yellow:#ccff00; --blue:#33ffcc;
  --text:#d4ffd4; --textdim:#66cc66; --textdim2:#449944; --faint:#225522;
  --orange:#ffaa00; --tg:#229ED9;
  --green-rgb:0,255,65; --red-rgb:255,51,51; --yellow-rgb:204,255,0; --blue-rgb:51,255,204;
  --orange-rgb:255,170,0; --tg-rgb:34,158,217; --border-rgb:17,51,17; --card-rgb:5,15,5;
}
body[data-theme="sunset"]{
  --bg:#1a0b2e; --card:#2a1245; --border:#4a2570;
  --green:#22d3a5; --red:#ff5470; --yellow:#ffb347; --blue:#7dd3fc;
  --text:#fff0f5; --textdim:#d8a8c8; --textdim2:#a878a0; --faint:#5a3a58;
  --orange:#ff7849; --tg:#229ED9;
  --green-rgb:34,211,165; --red-rgb:255,84,112; --yellow-rgb:255,179,71; --blue-rgb:125,211,252;
  --orange-rgb:255,120,73; --tg-rgb:34,158,217; --border-rgb:74,37,112; --card-rgb:42,18,69;
}
body[data-theme="arceus_white"]{
  --bg:#f0f4ff; --card:#ffffff; --border:#c8d4f0;
  --green:#00a550; --red:#e8001c; --yellow:#b35c00; --blue:#1a56db;
  --text:#0a0f1e; --textdim:#1e2d5a; --textdim2:#3a4d80; --faint:#6b7db3;
  --orange:#c44800; --tg:#0088cc;
  --green-rgb:0,165,80; --red-rgb:232,0,28; --yellow-rgb:179,92,0; --blue-rgb:26,86,219;
  --orange-rgb:196,72,0; --tg-rgb:0,136,204; --border-rgb:200,212,240; --card-rgb:255,255,255;
  --font:'Courier New',monospace;
}
/* Arceus White: neon glow overrides */
body[data-theme="arceus_white"] .val.g,
body[data-theme="arceus_white"] .g { color:#00a550; text-shadow: 0 0 6px rgba(0,165,80,0.45), 0 0 12px rgba(0,165,80,0.2); }
body[data-theme="arceus_white"] .val.r,
body[data-theme="arceus_white"] .r { color:#e8001c; text-shadow: 0 0 6px rgba(232,0,28,0.45), 0 0 12px rgba(232,0,28,0.2); }
body[data-theme="arceus_white"] h1 { color:#1a56db; text-shadow: 0 0 8px rgba(26,86,219,0.35); }
body[data-theme="arceus_white"] .sh { color:#1a56db; text-shadow: 0 0 6px rgba(26,86,219,0.25); }
body[data-theme="arceus_white"] .card { box-shadow: 0 2px 8px rgba(26,86,219,0.10); }
body[data-theme="arceus_white"] .section { box-shadow: 0 2px 8px rgba(26,86,219,0.08); }
body[data-theme="arceus_white"] .api-ok { background:rgba(0,165,80,0.08); border-color:rgba(0,165,80,0.4); color:#00a550; }
body[data-theme="arceus_white"] .refresh { background:rgba(26,86,219,0.07); border-color:rgba(26,86,219,0.3); color:#1a56db; }
body[data-theme="arceus_white"] .badge.bg { background:rgba(0,165,80,0.12); color:#007a3d; border-color:rgba(0,165,80,0.4); }
body[data-theme="arceus_white"] .badge.bs { background:rgba(232,0,28,0.10); color:#b50016; border-color:rgba(232,0,28,0.35); }
body[data-theme="arceus_white"] .badge.bw { background:#e8edf8; color:#3a4d80; border-color:#c8d4f0; }
body[data-theme="arceus_white"] .sync-bar { background:rgba(26,86,219,0.07); border-color:rgba(26,86,219,0.3); color:#1a56db; }
body[data-theme="arceus_white"] input,
body[data-theme="arceus_white"] .btn { background:#f0f4ff; color:#0a0f1e; border-color:#c8d4f0; }
body[data-theme="arceus_white"] .linkbar a { color:#1a56db; }
body[data-theme="arceus_white"] .dim { color:#3a4d80; }

/* ═══════════════════════════════════════════════════
   ⚡ PIKACHU STRIKE — White bg, Yellow & Black combo
   ═══════════════════════════════════════════════════ */
body[data-theme="pikachu_strike"]{
  --bg:#fffef5; --card:#ffffff; --border:#e8d800;
  --green:#1a8a00; --red:#cc0000; --yellow:#d4a000; --blue:#1a1a1a;
  --text:#111111; --textdim:#2a2a00; --textdim2:#5a5200; --faint:#b8a800;
  --orange:#c45c00; --tg:#0088cc;
  --green-rgb:26,138,0; --red-rgb:204,0,0; --yellow-rgb:212,160,0; --blue-rgb:26,26,26;
  --orange-rgb:196,92,0; --tg-rgb:0,136,204; --border-rgb:232,216,0; --card-rgb:255,255,255;
}
/* Yellow accent glow on section headers */
body[data-theme="pikachu_strike"] h1 {
  color:#1a1a1a;
  text-shadow: 0 0 10px rgba(232,216,0,0.6), 0 0 20px rgba(232,216,0,0.25);
  letter-spacing:1px;
}
body[data-theme="pikachu_strike"] .sh {
  color:#1a1a1a;
  border-left: 3px solid #e8d800;
  padding-left: 6px;
}
/* Cards: white with thick yellow-black border */
body[data-theme="pikachu_strike"] .card {
  border: 2px solid #1a1a1a;
  box-shadow: 3px 3px 0px #e8d800, 0 1px 6px rgba(0,0,0,0.08);
}
body[data-theme="pikachu_strike"] .section {
  border: 1.5px solid #1a1a1a;
  box-shadow: 2px 2px 0px #e8d800;
}
/* PnL — green profit, red loss, both with glow */
body[data-theme="pikachu_strike"] .val.g,
body[data-theme="pikachu_strike"] .g {
  color:#1a8a00;
  font-weight:bold;
  text-shadow: 0 0 6px rgba(26,138,0,0.4), 0 0 12px rgba(26,138,0,0.15);
}
body[data-theme="pikachu_strike"] .val.r,
body[data-theme="pikachu_strike"] .r {
  color:#cc0000;
  font-weight:bold;
  text-shadow: 0 0 6px rgba(204,0,0,0.4), 0 0 12px rgba(204,0,0,0.15);
}
/* Yellow neon glow on key values */
body[data-theme="pikachu_strike"] .val {
  color:#1a1a1a;
}
body[data-theme="pikachu_strike"] .val.y {
  color:#c48000;
  text-shadow: 0 0 6px rgba(232,216,0,0.5);
}
/* Badges */
body[data-theme="pikachu_strike"] .badge.bg {
  background:rgba(26,138,0,0.10); color:#1a6600; border-color:rgba(26,138,0,0.5);
}
body[data-theme="pikachu_strike"] .badge.bs {
  background:rgba(204,0,0,0.09); color:#990000; border-color:rgba(204,0,0,0.4);
}
body[data-theme="pikachu_strike"] .badge.bw {
  background:#fffde0; color:#1a1a1a; border-color:#e8d800;
}
/* Buttons & inputs */
body[data-theme="pikachu_strike"] .btn {
  background:#1a1a1a; color:#e8d800; border-color:#1a1a1a;
  font-weight:bold; letter-spacing:0.5px;
}
body[data-theme="pikachu_strike"] .btn:active { background:#333; }
body[data-theme="pikachu_strike"] input {
  background:#fffef5; color:#111111; border-color:#1a1a1a;
}
/* API status & refresh bar */
body[data-theme="pikachu_strike"] .api-ok {
  background:rgba(26,138,0,0.08); border-color:rgba(26,138,0,0.4); color:#1a6600;
}
body[data-theme="pikachu_strike"] .refresh {
  background:#fffde0; border-color:#e8d800; color:#1a1a1a;
}
/* Sync bar */
body[data-theme="pikachu_strike"] .sync-bar {
  background:#fffde0; border-color:#e8d800; color:#1a1a1a;
}
body[data-theme="pikachu_strike"] .sync-dot {
  background:#e8d800;
  box-shadow: 0 0 6px rgba(232,216,0,0.9), 0 0 12px rgba(232,216,0,0.4);
}
body[data-theme="pikachu_strike"] .sync-label { color:#1a1a1a; }
/* Links */
body[data-theme="pikachu_strike"] .linkbar a { color:#1a1a1a; font-weight:bold; }
body[data-theme="pikachu_strike"] .dim { color:#5a5200; }
/* Zebra rows */
body[data-theme="pikachu_strike"] tr:nth-child(even) { background:#fffde0; }
/* Top border stripe — classic Pikachu yellow bar */
body[data-theme="pikachu_strike"] body::before,
body[data-theme="pikachu_strike"]::before {
  content:''; display:block; height:4px;
  background: linear-gradient(90deg, #1a1a1a 0%, #e8d800 40%, #1a1a1a 100%);
  position:fixed; top:0; left:0; right:0; z-index:999;
}
'''
THEME_LABELS = {'classic':'⬛ Classic','cyber':'🟣 Neon Cyber','aurora':'🌊 Aurora',
                'solar':'☀️ Solar (Light)','matrix':'💚 Matrix','sunset':'🌅 Sunset',
                'arceus_white':'⚪ Arceus White','pikachu_strike':'⚡ Pikachu Strike'}

SETUP_HTML='''<!DOCTYPE html><html><head>
<title>G Max V1 Setup</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;
  padding:20px;min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:24px;max-width:420px;width:100%}
h1{color:var(--green);font-size:18px;letter-spacing:2px;text-align:center;
  margin-bottom:4px;font-weight:bold}
.sub{text-align:center;color:var(--textdim);font-size:11px;margin-bottom:16px;font-weight:bold}
.sec{font-size:10px;color:var(--green);text-transform:uppercase;letter-spacing:2px;
  font-weight:bold;margin:14px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--border)}
label{display:block;font-size:10px;color:var(--textdim);text-transform:uppercase;
  letter-spacing:1px;margin-bottom:5px;font-weight:bold}
input{width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;
  padding:10px;color:var(--text);font-family:'Courier New',monospace;font-size:11px;
  margin-bottom:12px;outline:none}
input:focus{border-color:rgba(var(--green-rgb),0.333)}
.tgi{border-color:rgba(var(--tg-rgb),0.2)}.tgi:focus{border-color:var(--tg)}
button{width:100%;background:rgba(var(--green-rgb),0.094);border:1px solid rgba(var(--green-rgb),0.333);color:var(--green);
  padding:12px;border-radius:6px;font-family:'Courier New',monospace;
  font-size:13px;cursor:pointer;font-weight:bold;margin-top:4px}
.warn{background:rgba(var(--yellow-rgb),0.067);border:1px solid rgba(var(--yellow-rgb),0.267);border-radius:6px;
  padding:10px;font-size:11px;color:var(--yellow);margin-bottom:12px;line-height:1.8;font-weight:bold}
.tgnote{background:rgba(var(--tg-rgb),0.067);border:1px solid rgba(var(--tg-rgb),0.2);border-radius:6px;
  padding:10px;font-size:11px;color:var(--tg);margin-bottom:12px;line-height:1.8;font-weight:bold}
.err{background:rgba(var(--red-rgb),0.067);border:1px solid rgba(var(--red-rgb),0.267);border-radius:6px;
  padding:10px;font-size:12px;color:var(--red);margin-bottom:14px;font-weight:bold}
.opt{color:var(--faint);font-size:9px;text-transform:none;font-weight:normal}
</style></head><body><div class="box">
<h1>🤖 G MAX V1</h1>
<div class="sub">Strategy G · 15m · TP 3% · SL 15% | Engineered by Paqu</div>
{% if error %}<div class="err">❌ {{error}}</div>{% endif %}
<form method="POST" action="/setup">
<div class="sec">📊 Binance API</div>
<div class="warn">✅ Enable Futures only<br>❌ Never enable Withdrawals</div>
<label>API Key</label>
<input type="text" name="api_key" placeholder="Paste API key" autocomplete="off" spellcheck="false">
<label>Secret Key</label>
<input type="password" name="api_secret" placeholder="Paste Secret key">
<div class="sec">📱 Telegram <span class="opt">(optional)</span></div>
<div class="tgnote">Get alerts everywhere — @BotFather for token, @userinfobot for ID</div>
<label>Bot Token <span class="opt">(optional)</span></label>
<input type="text" name="tg_token" class="tgi" placeholder="1234567890:ABCdef...">
<label>Your User ID <span class="opt">(optional)</span></label>
<input type="text" name="tg_chat_id" class="tgi" placeholder="Numeric ID">
<button type="submit">✅ SAVE &amp; START BOT</button>
</form>
</div></body></html>'''

DASH_HTML='''<!DOCTYPE html><html><head>
<title>G Max V1</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;padding:10px;font-size:12px}
h1{color:var(--green);font-size:15px;letter-spacing:2px;text-align:center;padding:8px 0 2px;font-weight:bold}
.sub{text-align:center;color:var(--textdim);font-size:10px;margin-bottom:5px;font-weight:bold}
.refresh{text-align:center;background:rgba(var(--green-rgb),0.06);border:1px solid rgba(var(--green-rgb),0.2);border-radius:6px;
  padding:5px;font-size:11px;color:var(--green);font-weight:bold;margin-bottom:6px}
.api-err{background:rgba(var(--red-rgb),0.094);border:2px solid var(--red);border-radius:8px;padding:10px;margin-bottom:8px}
.api-err .t{color:var(--red);font-weight:bold;font-size:12px;margin-bottom:3px}
.api-err .m{color:var(--red);font-size:11px;word-break:break-all;line-height:1.5}
.api-ok{background:rgba(var(--green-rgb),0.067);border:1px solid rgba(var(--green-rgb),0.267);border-radius:8px;padding:7px;
  margin-bottom:8px;font-size:11px;color:var(--green);text-align:center;font-weight:bold}
.grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px;margin-bottom:7px}
.card{background:var(--card);border:1px solid var(--border);border-radius:7px;padding:8px 6px;text-align:center}
.lbl{font-size:9px;color:var(--textdim);text-transform:uppercase;letter-spacing:1px;font-weight:bold}
.val{font-size:17px;font-weight:bold;margin-top:3px}
.g{color:var(--green)}.r{color:var(--red)}.y{color:var(--yellow)}.b{color:var(--blue)}
.section{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:9px;margin-bottom:7px}
.sh{font-size:10px;color:var(--green);text-transform:uppercase;letter-spacing:1.5px;font-weight:bold;
  margin-bottom:7px;padding-bottom:5px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
.pos-row,.trade-row{background:var(--bg);border:1px solid var(--border);border-radius:6px;
  padding:7px 8px;margin-bottom:5px;font-size:11px}
.row-top{display:flex;justify-content:space-between;align-items:center}
.coin{font-weight:bold}
.side-l{color:var(--green)}.side-s{color:var(--red)}
.small{font-size:9.5px;color:var(--textdim2);margin-top:2px}
.empty{text-align:center;color:var(--faint);padding:14px;font-size:11px}
.linkbar{display:flex;gap:6px;margin-bottom:7px}
.linkbar a{flex:1;text-align:center;background:var(--card);border:1px solid var(--border);
  border-radius:6px;padding:8px;color:var(--blue);text-decoration:none;font-size:10px;font-weight:bold}
form.inline{display:inline}
.btns{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:7px}
.btn{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:8px 4px;
  font-family:'Courier New',monospace;font-size:10px;font-weight:bold;cursor:pointer;text-align:center;
  color:var(--text);width:100%}
.b-scan{border-color:rgba(var(--green-rgb),0.333);color:var(--green);background:rgba(var(--green-rgb),0.067)}
.b-cache{border-color:rgba(var(--yellow-rgb),0.267);color:var(--yellow);background:transparent}
.sigrow{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:4px;
  padding:4px 0;border-bottom:1px solid var(--border);font-size:10px}
.sigrow:last-child{border:none}
.badge{padding:2px 5px;border-radius:3px;font-size:9px;font-weight:bold;white-space:nowrap}
.bg{background:rgba(var(--green-rgb),0.133);color:var(--green);border:1px solid rgba(var(--green-rgb),0.333)}
.bs{background:rgba(var(--red-rgb),0.133);color:var(--red);border:1px solid rgba(var(--red-rgb),0.333)}
.bw{background:var(--border);color:var(--textdim);border:1px solid var(--border)}
.sig-cn{min-width:44px;font-weight:bold;color:var(--text);font-size:10px}
.dim{color:var(--textdim2);font-size:9px}
.b-close{background:rgba(var(--red-rgb),0.094);border:1px solid rgba(var(--red-rgb),0.333);color:var(--red);
  border-radius:4px;padding:3px 8px;font-size:9px;font-weight:bold;cursor:pointer}
.sig-cn a{color:var(--text);text-decoration:none;border-bottom:1px dotted var(--blue)}
.coin a{color:var(--text);text-decoration:none;border-bottom:1px dotted var(--blue)}
.coin-toggle{background:none;border:none;cursor:pointer;font-size:10px;padding:0 3px}
.tag{background:var(--border);border-radius:4px;padding:1px 5px;font-size:8.5px;color:var(--blue);margin-left:4px}
/* Universe toggle button */
.uni-btn-wrap{text-align:center;margin-bottom:10px}
.uni-btn{display:inline-block;width:100%;padding:14px 10px;border-radius:10px;
  font-family:'Courier New',monospace;font-size:13px;font-weight:bold;cursor:pointer;
  border:2px solid;letter-spacing:0.5px}
.uni-btn.whitelist{background:rgba(var(--green-rgb),0.094);border-color:var(--green);color:var(--green)}
.uni-btn.universe{background:rgba(var(--blue-rgb),0.094);border-color:var(--blue);color:var(--blue)}
.uni-meta{font-size:10px;color:var(--textdim2);margin-top:5px;text-align:center}
/* Candle Sync bar */
.sync-bar{background:rgba(var(--green-rgb),0.055);border:1px solid rgba(var(--green-rgb),0.25);
  border-radius:8px;padding:8px 12px;margin-top:8px;display:flex;align-items:center;
  justify-content:space-between;gap:6px;flex-wrap:wrap}
.sync-dot{width:8px;height:8px;border-radius:50%;background:var(--green);flex-shrink:0;
  box-shadow:0 0 6px rgba(var(--green-rgb),0.8)}
.sync-dot.inactive{background:var(--faint);box-shadow:none}
.sync-label{font-size:10px;font-weight:bold;color:var(--green);letter-spacing:0.5px;flex:1}
.sync-label.inactive{color:var(--faint)}
.sync-detail{font-size:9.5px;color:var(--textdim2);text-align:right}
/* Sort bar */
.sort-bar{display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-bottom:6px;margin-top:4px}
.sort-label{font-size:9px;color:var(--textdim2);font-weight:bold;margin-right:2px;white-space:nowrap}
.sort-btn{background:rgba(var(--border-rgb),0.18);border:1px solid rgba(var(--border-rgb),0.5);
  color:var(--textdim);border-radius:14px;padding:4px 9px;font-size:9.5px;cursor:pointer;
  font-family:'Courier New',monospace;white-space:nowrap;transition:all 0.15s}
.sort-btn.sort-active{background:rgba(var(--yellow-rgb),0.18);border-color:var(--yellow);
  color:var(--yellow);font-weight:bold}
.sort-hint{font-size:9px;color:var(--textdim2);margin-bottom:6px;padding:4px 8px;
  background:rgba(var(--yellow-rgb),0.07);border-left:2px solid var(--yellow);border-radius:0 4px 4px 0}
/* Extra Signals search */
.extra-search{display:flex;gap:6px;margin-bottom:8px}
.extra-search input{flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;
  color:var(--text);font-family:'Courier New',monospace;font-size:12px;padding:10px 8px;min-width:0}
.extra-search input::placeholder{color:var(--textdim2)}
.b-search{width:auto;white-space:nowrap;padding:10px 16px;border-color:rgba(var(--blue-rgb),0.333);
  color:var(--blue);background:rgba(var(--blue-rgb),0.094);font-size:11px}
.extra-msg{border-radius:6px;padding:8px;font-size:10.5px;font-weight:bold;margin-bottom:8px;text-align:center}
.extra-msg-ok{background:rgba(var(--green-rgb),0.094);border:1px solid var(--green);color:var(--green)}
.extra-msg-err{background:rgba(var(--red-rgb),0.094);border:1px solid var(--red);color:var(--red)}
/* Live PnL */
.live-pnl{font-size:11px;font-weight:bold;padding:2px 6px;border-radius:4px;white-space:nowrap}
.live-pnl-pos{color:var(--green);background:rgba(var(--green-rgb),0.1)}
.live-pnl-neg{color:var(--red);background:rgba(var(--red-rgb),0.1)}
.live-pnl-neu{color:var(--textdim2)}
/* Candle countdown */
.candle-countdown{font-size:13px;font-weight:bold;color:var(--yellow);letter-spacing:0.5px;font-variant-numeric:tabular-nums}
.candle-progress{height:3px;background:var(--border);border-radius:2px;margin-top:5px;overflow:hidden}
.candle-progress-bar{height:100%;background:var(--yellow);border-radius:2px;transition:width 1s linear}
</style>
<script>
function act(u){fetch(u,{method:'POST'}).then(function(){location.reload()})}
function closePos(sym){if(confirm('Close '+sym+' at market?'))
  fetch('/close/'+sym,{method:'POST'}).then(function(){setTimeout(function(){location.reload()},1500)})}

/* ── Live PnL polling ── */
var LIVE_PNL_ENABLED = {{live_pnl_enabled|lower}};
function pollLivePnl(){
  if(!LIVE_PNL_ENABLED) return;
  fetch('/api/live_pnl')
    .then(function(r){return r.json()})
    .then(function(data){
      var pos = data.positions || {};
      Object.keys(pos).forEach(function(sym){
        var el = document.getElementById('pnl-'+sym);
        if(!el) return;
        var d = pos[sym];
        if(d.pnl === null){ el.textContent='--'; el.className='live-pnl live-pnl-neu'; return; }
        var sign = d.pnl >= 0 ? '+' : '';
        el.textContent = sign + d.pnl.toFixed(3) + ' USDT (' + (d.pnl>=0?'+':'') + d.pct.toFixed(2) + '%)';
        el.className = 'live-pnl ' + (d.pnl > 0 ? 'live-pnl-pos' : d.pnl < 0 ? 'live-pnl-neg' : 'live-pnl-neu');
      });
    })
    .catch(function(){});
}
if(LIVE_PNL_ENABLED && document.querySelector('[id^="pnl-"]')){
  pollLivePnl();
  setInterval(pollLivePnl, 60000);
}

/* ── Candle countdown ticker ── */
var _candleSecs = null;
var _candleFetched = 0;
var _candleTickInterval = null;
var CANDLE_TOTAL = 15 * 60;

function tickCandle(){
  var timerEl = document.getElementById('candle-timer');
  var barEl   = document.getElementById('candle-bar');
  if(!timerEl || _candleSecs === null) return;
  var elapsed = (Date.now() - _candleFetched) / 1000;
  var secs = Math.max(0, _candleSecs - elapsed);
  var m = Math.floor(secs / 60);
  var s = Math.floor(secs % 60);
  timerEl.textContent = m + ':' + (s < 10 ? '0' : '') + s;
  var pct = Math.min(100, Math.max(0, (secs / CANDLE_TOTAL) * 100));
  if(barEl) barEl.style.width = pct + '%';
  if(secs < 30){
    timerEl.style.color = 'var(--green)';
    if(barEl) barEl.style.background = 'var(--green)';
  } else {
    timerEl.style.color = 'var(--yellow)';
    if(barEl) barEl.style.background = 'var(--yellow)';
  }
}

function fetchCandleSecs(){
  fetch('/api/candle_countdown')
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(d.seconds !== null && d.seconds !== undefined){
        _candleSecs = d.seconds;
        _candleFetched = Date.now();
        // Start ticking only after first successful fetch
        if(!_candleTickInterval){
          tickCandle();
          _candleTickInterval = setInterval(tickCandle, 1000);
        }
      }
    }).catch(function(){});
}

if(document.getElementById('candle-timer')){
  fetchCandleSecs();
  setInterval(fetchCandleSecs, 30000);
}
</script>
</head><body>
<h1>🤖 G MAX V1</h1>
<div class="sub">Engineered by Paqu · Strategy G · 15m · TP 3% · SL 15%</div>
<div class="refresh">🔄 Auto-refresh 30s | Scan #{{scan_count}} | Next: {{next_scan}}</div>

{% if api_status=='error' %}
<div class="api-err"><div class="t">❌ API ERROR</div><div class="m">{{api_error}}</div></div>
{% else %}
<div class="api-ok">✅ API Connected</div>
{% endif %}

<div class="grid">
<div class="card"><div class="lbl">Balance</div><div class="val b">${{'%.2f'|format(balance)}}</div></div>
<div class="card"><div class="lbl">Today PnL</div><div class="val {{'g' if today_pnl>=0 else 'r'}}">${{'%+.3f'|format(today_pnl)}}</div></div>
<div class="card"><div class="lbl">Total PnL</div><div class="val {{'g' if total_pnl>=0 else 'r'}}">${{'%+.3f'|format(total_pnl)}}</div></div>
<div class="card"><div class="lbl">Win Rate</div><div class="val y">{{wr}}</div></div>
<div class="card"><div class="lbl">Profit Factor</div><div class="val y">{{pf_display}}</div></div>
<div class="card"><div class="lbl">Open Trades</div><div class="val b">{{pos_count}}</div></div>
</div>

<a href="/data" style="display:block;text-align:center;background:var(--card);border:1px solid var(--border);
border-radius:8px;padding:10px;color:var(--blue);text-decoration:none;font-size:11px;font-weight:900;
margin-bottom:7px;letter-spacing:0.5px">📊 DATA — daily P&amp;L, drawdown, top gainers/losers &amp; more ›</a>

<div class="linkbar">
<a href="/history">📜 Full History</a>
<a href="/settings">⚙️ API &amp; Settings</a>
</div>

<div class="btns">
<form class="inline" method="POST" action="/scan" style="width:100%"><button type="submit" class="btn b-scan">⚡ Scan Now</button></form>
<form class="inline" method="POST" action="/clear/cache" style="width:100%"><button type="submit" class="btn b-cache" onclick="return confirm('Clear signal cache?')">🗑️ Clear Cache</button></form>
</div>

<!-- Universe Toggle Button -->
<div class="uni-btn-wrap">
  <form method="POST" action="/toggle_universe">
    <button type="submit" class="uni-btn {{coin_mode}}">
      {% if coin_mode=='whitelist' %}🎯 WHITELIST MODE — {{wl_count}} Coins{% else %}🌐 UNIVERSE MODE — {{universe_count}} Coins{% endif %}
    </button>
  </form>
  <div class="uni-meta">Whitelist: {{wl_count}} coins &nbsp;|&nbsp; Universe: {{universe_count}} coins &nbsp;|&nbsp; Active: <b>{{coin_mode}}</b></div>
</div>

<div class="section">
<div class="sh"><span>💼 Open Positions</span><span>{{pos_count}}</span></div>
{% if positions %}
{% for p in positions %}
<div class="pos-row" id="pos-{{p.symbol}}">
  <div class="row-top">
    <span class="coin"><a href="/coin/{{p.symbol}}">{{p.name}}</a><span class="tag">G</span></span>
    <span class="{{'side-l' if p.side=='buy' else 'side-s'}}">{{'🟢 LONG' if p.side=='buy' else '🔴 SHORT'}}</span>
    {% if live_pnl_enabled %}<span class="live-pnl live-pnl-neu" id="pnl-{{p.symbol}}">· · ·</span>{% endif %}
  </div>
  <div class="small">Entry ${{p.entry}} · TP {{p.tp}} · SL {{p.sl}} · {{p.opened}}
    <button class="b-close" style="float:right" onclick="closePos('{{p.symbol}}')">✕ Close</button>
  </div>
</div>
{% endfor %}
{% else %}<div class="empty">No open positions</div>{% endif %}
</div>

<div class="section">
<div class="sh"><span>🏆 Top Coins</span></div>
{% if top_coins %}
{% for tc in top_coins %}
<div class="small">{{loop.index}}. <a href="/coin/{{tc.symbol}}" style="color:var(--text);font-weight:900;text-decoration:none;border-bottom:1px dotted var(--blue)">{{tc.name}}</a> — {{'%.0f'|format(tc.wr)}}% WR ({{tc.wins}}W/{{tc.losses}}L) ${{'%+.3f'|format(tc.pnl)}}</div>
{% endfor %}
{% else %}<div class="empty">No completed trades yet</div>{% endif %}
</div>

<div class="section">
<div class="sh"><span>📡 Live Signals</span><span>{{active_coin_count}} coins</span></div>

<!-- Sort Bar -->
<div class="sort-bar">
  <span class="sort-label">Sort:</span>
  {% for m in sort_modes %}
  <form method="POST" action="/set_sort/{{m}}" style="display:inline;margin:0">
    <button type="submit" class="sort-btn {{'sort-active' if sort_mode==m else ''}}">
      {{sort_labels[m]}}
    </button>
  </form>
  {% endfor %}
</div>
{% if sort_mode == 'recently_traded' %}
<div class="sort-hint">🔥 Coins that traded most recently shown first</div>
{% elif sort_mode == 'about_to_trade' %}
<div class="sort-hint">⚡ Coins closest to a signal shown first — ✅ cross + trend ranks highest</div>
{% endif %}

<form method="POST" action="/enable_all_coins" style="margin-bottom:7px">
  <button type="submit" class="btn" style="border-color:rgba(var(--green-rgb),0.333);color:var(--green);
    background:rgba(var(--green-rgb),0.094);padding:6px;font-size:9.5px">🟢 Turn All Coins ON</button>
</form>
{% if signal_rows %}
{% for r in signal_rows %}
<div class="sigrow">
  <form method="POST" action="/toggle/coin/{{r.symbol}}" style="display:inline">
    <button type="submit" class="coin-toggle">{{'🟢' if r.coin_enabled else '⚪'}}</button>
  </form>
  <span class="sig-cn"><a href="/coin/{{r.symbol}}">{{r.name}}</a></span>
  <span class="badge {{r.badge}}">{{r.reason}}</span>
  {% if r.adx != '--' %}<span class="dim">ADX:{{r.adx}}</span>{% endif %}
  {% if r.trend %}<span class="dim">{{r.trend}}</span>{% endif %}
  {% if r.crossed %}<span class="dim">X:{{r.crossed}}</span>{% endif %}
  <span class="dim">{{r.time}}</span>
</div>
{% endfor %}
{% else %}<div class="empty">Scanning...</div>{% endif %}
</div>

<div class="section">
<div class="sh"><span>➕ Extra Signals</span><span>{{extra_count}} coins</span></div>

{% if extra_msg %}
<div class="extra-msg {{'extra-msg-err' if extra_msg_type in ('error','exists') else 'extra-msg-ok'}}">{{extra_msg}}</div>
{% endif %}

<form method="POST" action="/extra/add" class="extra-search">
  <input type="text" name="symbol" placeholder="Search coin e.g. BTC or BTCUSDT" autocapitalize="characters" autocomplete="off">
  <button type="submit" class="btn b-search">🔍 SEARCH &amp; ADD</button>
</form>

{% if sort_mode == 'recently_traded' %}
<div class="sort-hint">🔥 Coins that traded most recently shown first</div>
{% elif sort_mode == 'about_to_trade' %}
<div class="sort-hint">⚡ Coins closest to a signal shown first — ✅ cross + trend ranks highest</div>
{% endif %}

{% if extra_signal_rows %}
{% for r in extra_signal_rows %}
<div class="sigrow">
  <form method="POST" action="/toggle/coin/{{r.symbol}}" style="display:inline">
    <button type="submit" class="coin-toggle">{{'🟢' if r.coin_enabled else '⚪'}}</button>
  </form>
  <span class="sig-cn"><a href="/coin/{{r.symbol}}">{{r.name}}</a></span>
  <span class="badge {{r.badge}}">{{r.reason}}</span>
  {% if r.adx != '--' %}<span class="dim">ADX:{{r.adx}}</span>{% endif %}
  {% if r.trend %}<span class="dim">{{r.trend}}</span>{% endif %}
  {% if r.crossed %}<span class="dim">X:{{r.crossed}}</span>{% endif %}
  <span class="dim">{{r.time}}</span>
</div>
{% endfor %}
{% else %}<div class="empty">No extra coins yet — search above to add one</div>{% endif %}
</div>

<div class="section">
<div class="sh"><span>📜 Recent Trades</span><span>{{trade_count}} total</span></div>
{% if recent_trades %}
{% for t in recent_trades %}
<div class="trade-row">
  <div class="row-top">
    <span class="coin"><a href="/coin/{{t.symbol}}">{{t.name}}</a><span class="tag">G</span></span>
    <span class="{{'g' if t.pnl>=0 else 'r'}}">{{'✅' if t.pnl>0 else ('❌' if t.pnl<0 else '➖')}} ${{'%+.3f'|format(t.pnl)}}</span>
  </div>
  <div class="small">{{t.side}} · {{t.closed}} · {{t.reason}}</div>
</div>
{% endfor %}
{% else %}<div class="empty">No trades yet</div>{% endif %}
</div>

<!-- Candle Sync Status Bar -->
{% if candle_sync_active %}
<div class="sync-bar" style="flex-direction:column;align-items:stretch;gap:5px">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:6px;flex-wrap:wrap">
    <div style="display:flex;align-items:center;gap:7px">
      <div class="sync-dot"></div>
      <span class="sync-label">🕯️ Next 15m Candle</span>
    </div>
    <span class="sync-detail">
      At <b>{{next_candle_time}}</b> &nbsp;|&nbsp; Syncs: <b>{{candle_sync_count}}</b>
      {% if last_candle_sync != '--' %}&nbsp;|&nbsp; Last: {{last_candle_sync}}{% endif %}
    </span>
  </div>
  <div style="display:flex;align-items:center;gap:8px">
    <span id="candle-timer" class="candle-countdown">--:--</span>
    <div class="candle-progress" style="flex:1">
      <div class="candle-progress-bar" id="candle-bar" style="width:100%"></div>
    </div>
  </div>
</div>
{% else %}
<div class="sync-bar">
  <div class="sync-dot inactive"></div>
  <span class="sync-label inactive">⏳ Candle Sync: waiting for bot start...</span>
</div>
{% endif %}

<div style="text-align:center;color:var(--faint);font-size:9px;padding:10px 0">
Started {{start_time}} · {{now}}<br>Engineered by Paqu
</div>
</body></html>'''

SETTINGS_HTML='''<!DOCTYPE html><html><head>
<title>Settings — G Max V1</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;padding:16px;font-size:12px}
.box{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:18px;max-width:420px;margin:0 auto}
h1{color:var(--green);font-size:15px;text-align:center;margin-bottom:14px;letter-spacing:1.5px}
label{display:block;font-size:10px;color:var(--textdim);text-transform:uppercase;letter-spacing:1px;margin-bottom:5px;font-weight:bold}
input{width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px;
  color:var(--text);font-family:'Courier New',monospace;font-size:12px;margin-bottom:12px}
button{width:100%;background:rgba(var(--green-rgb),0.094);border:1px solid rgba(var(--green-rgb),0.333);color:var(--green);padding:12px;
  border-radius:6px;font-family:'Courier New',monospace;font-size:13px;font-weight:bold;cursor:pointer}
a.back{display:block;text-align:center;color:var(--blue);margin-top:12px;font-size:11px;text-decoration:none}
.note{font-size:10px;color:var(--textdim2);margin-bottom:12px;line-height:1.6}
.sec{font-size:10px;color:var(--green);text-transform:uppercase;letter-spacing:2px;
  font-weight:bold;margin:14px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--border)}
.ok{background:rgba(var(--green-rgb),0.067);border:1px solid rgba(var(--green-rgb),0.267);color:var(--green);border-radius:6px;
  padding:8px;text-align:center;margin-bottom:14px;font-weight:bold;font-size:11px}
.opt{color:var(--faint);font-size:9px;text-transform:none;font-weight:normal}
.theme-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:14px}
.theme-opt{position:relative}
.theme-opt input{position:absolute;opacity:0;width:100%;height:100%;margin:0;cursor:pointer}
.theme-opt label{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:9px 6px;
  text-align:center;font-size:10px;font-weight:bold;margin:0;cursor:pointer;text-transform:none;letter-spacing:0}
.theme-opt input:checked + label{border-color:var(--green);color:var(--green);background:rgba(var(--green-rgb),0.094)}
.radio-row{display:flex;gap:6px;margin-bottom:12px}
.radio-opt{flex:1;position:relative}
.radio-opt input{position:absolute;opacity:0;width:100%;height:100%;margin:0;cursor:pointer}
.radio-opt label{display:block;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:9px 6px;
  text-align:center;font-size:10px;font-weight:bold;margin:0;cursor:pointer;text-transform:none;letter-spacing:0}
.radio-opt input:checked + label{border-color:var(--blue);color:var(--blue);background:rgba(var(--blue-rgb),0.094)}
.warn2{background:rgba(var(--orange-rgb),0.094);border:1px solid rgba(var(--orange-rgb),0.333);border-radius:6px;
  padding:9px;font-size:10px;color:var(--orange);margin-bottom:12px;line-height:1.6}
</style></head><body><div class="box">
<h1>⚙️ SETTINGS</h1>
{% if saved %}<div class="ok">✅ Saved!</div>{% endif %}
<form method="POST" action="/settings">

<div class="sec">🎨 Theme</div>
<div class="theme-grid">
{% for t in themes %}
<div class="theme-opt">
  <input type="radio" name="theme" id="theme_{{t}}" value="{{t}}" {{'checked' if s.theme==t}} onchange="this.form.submit()">
  <label for="theme_{{t}}">{{theme_labels[t]}}</label>
</div>
{% endfor %}
</div>

<div class="sec">📊 Binance API</div>
<label>API Key <span class="opt">(leave blank to keep current)</span></label>
<input type="text" name="api_key" placeholder="{{'•'*10 + api_key[-6:] if api_key else 'Paste API key'}}" autocomplete="off" spellcheck="false">
<label>Secret Key <span class="opt">(leave blank to keep current)</span></label>
<input type="password" name="api_secret" placeholder="{{'Currently set' if api_key else 'Paste Secret key'}}">
<div class="sec">📱 Telegram</div>
<label>Bot Token <span class="opt">(leave blank to keep current)</span></label>
<input type="text" name="tg_token" placeholder="{{'•'*10 + tg_token[-6:] if tg_token else '1234567890:ABCdef...'}}">
<label>Your User ID <span class="opt">(leave blank to keep current)</span></label>
<input type="text" name="tg_chat_id" placeholder="{{tg_chat_id if tg_chat_id else 'Numeric ID'}}">

<div class="sec">🎛 Trading Params</div>
<label>Leverage (x)</label>
<input type="number" step="1" name="leverage" value="{{s.leverage}}">
<label>Cooldown per coin (minutes)</label>
<input type="number" step="1" name="cooldown_min" value="{{s.cooldown_min}}">
<label>Scan every (seconds)</label>
<input type="number" step="5" name="scan_every" value="{{s.scan_every}}">

<div class="sec">💵 Margin Sizing</div>
<div class="radio-row">
  <div class="radio-opt">
    <input type="radio" name="margin_mode" id="mm_fixed" value="fixed" {{'checked' if s.margin_mode=='fixed'}}>
    <label for="mm_fixed">Fixed $</label>
  </div>
  <div class="radio-opt">
    <input type="radio" name="margin_mode" id="mm_pct" value="percent" {{'checked' if s.margin_mode=='percent'}}>
    <label for="mm_pct">% of Balance</label>
  </div>
</div>
<label>Fixed margin per trade (USD)</label>
<input type="number" step="0.1" name="margin_usd" value="{{s.margin_usd}}">
<label>Margin as % of balance</label>
<input type="number" step="0.1" name="margin_percent" value="{{s.margin_percent}}">
<div class="note">Both values stay saved — switch the radio above anytime.</div>

<div class="sec">🛡 Margin Type</div>
<div class="radio-row">
  <div class="radio-opt">
    <input type="radio" name="margin_type" id="mt_cross" value="CROSSED" {{'checked' if s.margin_type=='CROSSED'}}>
    <label for="mt_cross">Cross</label>
  </div>
  <div class="radio-opt">
    <input type="radio" name="margin_type" id="mt_iso" value="ISOLATED" {{'checked' if s.margin_type=='ISOLATED'}}>
    <label for="mt_iso">Isolated</label>
  </div>
</div>
<div class="warn2">⚠️ Isolated caps a loss to that position's own margin. Cross shares margin across all positions.</div>

<div class="sec">📡 Live PnL</div>
<div class="radio-row">
  <div class="radio-opt">
    <input type="radio" name="live_pnl" id="lpnl_on" value="on" {{'checked' if s.live_pnl}}>
    <label for="lpnl_on">🟢 ON</label>
  </div>
  <div class="radio-opt">
    <input type="radio" name="live_pnl" id="lpnl_off" value="off" {{'checked' if not s.live_pnl}}>
    <label for="lpnl_off">⚪ OFF</label>
  </div>
</div>
<div class="note">When ON, open positions update PnL every 60s via live price fetch. Turn OFF when holding 20+ positions to save API calls.</div>

<div class="sec">📢 Signal Channel</div>
<div class="note">When enabled, the bot posts every confirmed signal to your Telegram channel — even if the trade is NOT placed (no margin, coin locked, etc.).<br>Channel must have your bot added as <b>Admin with Post Messages</b> permission.<br>Use channel username e.g. <code>@MySignals</code> or numeric chat ID e.g. <code>-1001234567890</code>.</div>
<div class="radio-row">
  <div class="radio-opt">
    <input type="radio" name="share_signals" id="ss_on" value="on" {{'checked' if s.share_signals}}>
    <label for="ss_on">📢 ON</label>
  </div>
  <div class="radio-opt">
    <input type="radio" name="share_signals" id="ss_off" value="off" {{'checked' if not s.share_signals}}>
    <label for="ss_off">🔇 OFF</label>
  </div>
</div>
<label>Channel Username or Chat ID</label>
<input type="text" name="signal_channel" placeholder="@YourChannel or -1001234567890" value="{{s.signal_channel}}">

<button type="submit">💾 SAVE SETTINGS</button>
</form>
<a class="back" href="/">← Back to dashboard</a>
</div></body></html>'''

HISTORY_HTML='''<!DOCTYPE html><html><head>
<title>Trade History — G Max V1</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;padding:10px;font-size:12px}
h1{color:var(--green);font-size:15px;text-align:center;padding:8px 0;letter-spacing:1.5px}
.trade-row{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:8px;margin-bottom:5px}
.row-top{display:flex;justify-content:space-between;align-items:center}
.coin{font-weight:bold}
.coin a{color:var(--text);text-decoration:none;border-bottom:1px dotted var(--blue)}
.g{color:var(--green)}.r{color:var(--red)}
.small{font-size:9.5px;color:var(--textdim2);margin-top:2px}
.tag{background:var(--border);border-radius:4px;padding:1px 5px;font-size:8.5px;color:var(--blue);margin-left:4px}
a.back{display:block;text-align:center;color:var(--blue);margin:12px 0;font-size:11px;text-decoration:none}
.empty{text-align:center;color:var(--faint);padding:20px}
</style></head><body>
<h1>📜 TRADE HISTORY</h1>
<a class="back" href="/">← Back to dashboard</a>
{% if trades %}
{% for t in trades %}
<div class="trade-row">
  <div class="row-top">
    <span class="coin"><a href="/coin/{{t.symbol}}">{{t.name}}</a><span class="tag">G</span></span>
    <span class="{{'g' if t.pnl>=0 else 'r'}}">{{'✅' if t.pnl>0 else ('❌' if t.pnl<0 else '➖')}} ${{'%+.3f'|format(t.pnl)}}</span>
  </div>
  <div class="small">{{t.side}} · entry {{t.entry}} → exit {{t.exit}} · {{t.closed}} · {{t.reason}}</div>
</div>
{% endfor %}
{% else %}<div class="empty">No trades yet</div>{% endif %}
</body></html>'''

COIN_HTML='''<!DOCTYPE html><html><head>
<title>{{coin}} — G Max V1</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;padding:10px;font-size:12px}
h1{color:var(--green);font-size:18px;text-align:center;padding:10px 0 4px;font-weight:900;letter-spacing:1px}
a.back{display:block;text-align:center;color:var(--blue);margin-bottom:10px;font-size:11px;text-decoration:none}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:8px}
.card{background:var(--card);border:1px solid var(--border);border-radius:7px;padding:8px 6px;text-align:center}
.lbl{font-size:9px;color:var(--textdim2);text-transform:uppercase;letter-spacing:1px;font-weight:900}
.val{font-size:16px;font-weight:900;margin-top:3px}
.g{color:var(--green)}.r{color:var(--red)}.y{color:var(--yellow)}.b{color:var(--blue)}
.section{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:9px;margin-bottom:8px}
.sh{font-size:10px;color:var(--green);text-transform:uppercase;letter-spacing:1.5px;font-weight:900;
  margin-bottom:7px;padding-bottom:5px;border-bottom:1px solid var(--border)}
.togbtn{border-radius:5px;padding:4px 9px;font-family:'Courier New',monospace;
  font-size:9.5px;font-weight:900;cursor:pointer;border:none}
.on{background:rgba(var(--green-rgb),0.133);border:1px solid var(--green);color:var(--green)}
.off{background:rgba(var(--red-rgb),0.133);border:1px solid var(--red);color:var(--red)}
.trade-row{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:7px 8px;margin-bottom:5px;font-size:11px}
.row-top{display:flex;justify-content:space-between;align-items:center}
.small{font-size:9.5px;color:var(--textdim2);margin-top:2px}
.empty{text-align:center;color:var(--faint);padding:14px;font-size:11px}
.tag{background:var(--border);border-radius:4px;padding:2px 6px;font-size:9.5px;color:var(--blue);font-weight:900}
form.inline{display:inline}
</style></head><body>
<h1>💎 {{coin}}</h1>
<a class="back" href="/">← Back to dashboard</a>

<div class="section">
<div class="sh">📊 Overall Stats</div>
<div class="grid">
<div class="card"><div class="lbl">Today PnL</div><div class="val {{'g' if overall.today_pnl>=0 else 'r'}}">${{'%+.3f'|format(overall.today_pnl)}}</div></div>
<div class="card"><div class="lbl">Total PnL</div><div class="val {{'g' if overall.total_pnl>=0 else 'r'}}">${{'%+.3f'|format(overall.total_pnl)}}</div></div>
<div class="card"><div class="lbl">Win Rate</div><div class="val y">{{'%.0f'|format(overall.wr) if overall.total>0 else '--'}}{{'%' if overall.total>0}}</div></div>
<div class="card"><div class="lbl">Profit Factor</div><div class="val y">{{overall.pf_display}}</div></div>
</div>
<form method="POST" action="/toggle/coin/{{symbol}}">
  <button type="submit" class="togbtn {{'on' if coin_enabled else 'off'}}" style="width:100%;padding:8px">
    {{'🟢 Enabled — tap to disable' if coin_enabled else '⚪ Disabled — tap to enable'}}
  </button>
</form>
{% if is_extra %}
<form method="POST" action="/extra/remove/{{symbol}}" style="margin-top:8px"
  onsubmit="return confirm('Remove {{coin}} from Extra Signals? This stops it trading.')">
  <button type="submit" class="togbtn off" style="width:100%;padding:14px;font-size:13px;
    border:2px solid var(--red);background:rgba(var(--red-rgb),0.133)">
    🗑️ REMOVE FROM EXTRA SIGNALS
  </button>
</form>
{% endif %}
</div>

<div class="section">
<div class="sh">📜 Trade History — {{coin}}</div>
{% if trades %}
{% for t in trades %}
<div class="trade-row">
  <div class="row-top">
    <span><b>{{t.side}}</b> <span class="tag">G</span></span>
    <span class="{{'g' if t.pnl>=0 else 'r'}}">{{'✅' if t.pnl>0 else ('❌' if t.pnl<0 else '➖')}} ${{'%+.3f'|format(t.pnl)}}</span>
  </div>
  <div class="small">entry {{t.entry}} → exit {{t.exit}} · {{t.closed}} · {{t.reason}}</div>
</div>
{% endfor %}
{% else %}<div class="empty">No trades yet for this coin</div>{% endif %}
</div>
</body></html>'''

DATA_HTML='''<!DOCTYPE html><html><head>
<title>Data Center — G Max V1</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:var(--font);padding:10px;font-size:12px}
h1{color:var(--green);font-size:16px;text-align:center;padding:8px 0 4px;font-weight:900;letter-spacing:1px}
a.back{display:block;text-align:center;color:var(--blue);margin-bottom:10px;font-size:11px;text-decoration:none}
.grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px;margin-bottom:8px}
.card{background:var(--card);border:1px solid var(--border);border-radius:7px;padding:8px 6px;text-align:center}
.lbl{font-size:8.5px;color:var(--textdim2);text-transform:uppercase;letter-spacing:0.5px;font-weight:900}
.val{font-size:14px;font-weight:900;margin-top:3px}
.g{color:var(--green)}.r{color:var(--red)}.y{color:var(--yellow)}.b{color:var(--blue)}
.section{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:8px}
.sh{font-size:10px;color:var(--green);text-transform:uppercase;letter-spacing:1.5px;font-weight:900;
  margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid var(--border)}
.dc-row{display:flex;align-items:flex-end;gap:3px;height:80px;margin-bottom:4px;overflow-x:auto}
.dc-col{flex:1;min-width:16px;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%}
.dc-bar{width:70%;border-radius:2px 2px 0 0;min-height:2px}
.dc-lbl{font-size:6.5px;color:var(--faint);text-align:center}
.eq-row{display:flex;align-items:flex-end;gap:1px;height:60px;margin-bottom:4px}
.eq-bar{flex:1;min-width:2px;border-radius:1px 1px 0 0}
.gl-row{display:flex;align-items:center;gap:6px;padding:4px 0;font-size:10px}
.gl-name{min-width:44px;font-weight:900}
.gl-name a{color:var(--text);text-decoration:none}
.gl-track{flex:1;background:var(--bg);border-radius:3px;height:12px;position:relative;overflow:hidden}
.gl-fill{height:100%;border-radius:3px}
.gl-fill.g{background:rgba(var(--green-rgb),0.4)}
.gl-fill.r{background:rgba(var(--red-rgb),0.4)}
.gl-val{min-width:52px;text-align:right;font-size:9.5px;font-weight:900}
.stat-row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--border);font-size:11px}
.stat-row:last-child{border:none}
.stat-row b{font-weight:900}
.empty{text-align:center;color:var(--faint);padding:14px;font-size:11px}
</style></head><body data-theme="{{theme}}">
<h1>📊 DATA CENTER</h1>
<a class="back" href="/">← Back to dashboard</a>

<div class="grid">
<div class="card"><div class="lbl">Total Trades</div><div class="val b">{{a.total_trades}}</div></div>
<div class="card"><div class="lbl">Max Drawdown</div><div class="val r">${{'%.2f'|format(a.max_drawdown)}}</div></div>
<div class="card"><div class="lbl">Drawdown %</div><div class="val r">{{'%.1f'|format(a.max_drawdown_pct)}}%</div></div>
<div class="card"><div class="lbl">Avg Win</div><div class="val g">${{'%.2f'|format(a.avg_win)}}</div></div>
<div class="card"><div class="lbl">Avg Loss</div><div class="val r">${{'%.2f'|format(a.avg_loss)}}</div></div>
<div class="card"><div class="lbl">Avg R:R</div><div class="val y">{{a.avg_rr}}</div></div>
<div class="card"><div class="lbl">Max Win Streak</div><div class="val g">{{a.max_win_streak}}</div></div>
<div class="card"><div class="lbl">Max Loss Streak</div><div class="val r">{{a.max_loss_streak}}</div></div>
<div class="card"><div class="lbl">Current Streak</div><div class="val {{'g' if a.cur_streak_type=='win' else 'r'}}">{{a.cur_streak}} {{a.cur_streak_type}}</div></div>
</div>

<div class="section">
<div class="sh">🏆 Best / Worst Trade</div>
{% if a.best_trade %}
<div class="stat-row"><span>Best — {{a.best_trade.symbol}}</span><b class="g">${{'%+.3f'|format(a.best_trade.pnl)}}</b></div>
<div class="stat-row"><span>Worst — {{a.worst_trade.symbol}}</span><b class="r">${{'%+.3f'|format(a.worst_trade.pnl)}}</b></div>
{% else %}<div class="empty">No trades yet</div>{% endif %}
</div>

<div class="section">
<div class="sh">📈 Equity Curve — cumulative PnL per trade</div>
{% if eq_bars %}
<div class="eq-row">
{% for b in eq_bars %}
<div class="eq-bar" style="height:{{b.pct}}%;background:{{'var(--green)' if b.val>=0 else 'var(--red)'}}"></div>
{% endfor %}
</div>
<div class="stat-row"><span>Running total</span><b class="{{'g' if a.equity_curve[-1]>=0 else 'r'}}">${{'%+.3f'|format(a.equity_curve[-1])}}</b></div>
{% else %}<div class="empty">No trades yet</div>{% endif %}
</div>

<div class="section">
<div class="sh">📆 Daily P&amp;L — last 14 days</div>
<div class="dc-row">
{% for d in daily_pnl %}
<div class="dc-col"><div class="dc-bar" style="height:{{d.bar_pct}}%;background:{{'var(--green)' if d.pnl>=0 else 'var(--red)'}}"></div></div>
{% endfor %}
</div>
<div style="display:flex;gap:3px">
{% for d in daily_pnl %}
<div class="dc-lbl" style="flex:1;min-width:16px">{{d.label}}</div>
{% endfor %}
</div>
</div>

<div class="section">
<div class="sh">🟢 Top 10 Profitable Coins</div>
{% if top_gainers %}
{% for g in top_gainers %}
<div class="gl-row">
  <span class="gl-name"><a href="/coin/{{g.symbol}}">{{g.name}}</a></span>
  <div class="gl-track"><div class="gl-fill g" style="width:{{g.bar_pct}}%"></div></div>
  <span class="gl-val g">${{'%+.2f'|format(g.pnl)}}</span>
</div>
{% endfor %}
{% else %}<div class="empty">No profitable coins yet</div>{% endif %}
</div>

<div class="section">
<div class="sh">🔴 Top 10 Losing Coins</div>
{% if top_losers %}
{% for l in top_losers %}
<div class="gl-row">
  <span class="gl-name"><a href="/coin/{{l.symbol}}">{{l.name}}</a></span>
  <div class="gl-track"><div class="gl-fill r" style="width:{{l.bar_pct}}%"></div></div>
  <span class="gl-val r">${{'%+.2f'|format(l.pnl)}}</span>
</div>
{% endfor %}
{% else %}<div class="empty">No losing coins yet</div>{% endif %}
</div>
</body></html>'''

# ── Inject theme CSS into all templates ───────────────────
for _tpl_name in ('SETUP_HTML','DASH_HTML','SETTINGS_HTML','HISTORY_HTML','COIN_HTML','DATA_HTML'):
    _tpl = globals()[_tpl_name]
    _tpl = _tpl.replace('<style>', '<style>'+THEME_CSS, 1)
    _tpl = _tpl.replace('<body>', '<body data-theme="{{theme}}">', 1)
    globals()[_tpl_name] = _tpl

# ── Flask routes ───────────────────────────────────────────
def _badge_class(sig):
    if sig=='buy': return 'bg'
    if sig=='sell': return 'bs'
    return 'bw'

def _signal_row(symbol):
    sg = state['last_signals'].get(symbol)
    if not sg:
        return {'name':display_name(symbol),'reason':'scanning...','badge':'bw',
                'adx':'--','trend':'','crossed':'','time':'',
                'symbol':symbol,'coin_enabled':is_coin_enabled(symbol)}
    return {'name':display_name(symbol),'reason':sg.get('reason','--'),
            'badge':_badge_class(sg.get('signal')),'adx':sg.get('adx','--'),
            'trend':sg.get('trend',''),'crossed':sg.get('crossed',''),
            'time':sg.get('time',''),'symbol':symbol,
            'coin_enabled':is_coin_enabled(symbol)}

@app.route('/')
def dashboard():
    if not is_configured():
        return redirect('/setup')
    coins = active_coins()
    sort_mode = current_sort_mode()
    rows_raw  = [_signal_row(sym) for sym in coins]

    if sort_mode == 'recently_traded':
        rows_raw.sort(key=lambda r: (
            0 if r['coin_enabled'] else 1,
            _sort_score_recently_traded(r['symbol'])
        ))
    elif sort_mode == 'about_to_trade':
        rows_raw.sort(key=lambda r: (
            0 if r['coin_enabled'] else 1,
            _sort_score_about_to_trade(r['symbol'])
        ))
    else:
        rows_raw.sort(key=lambda r: (0 if r['coin_enabled'] else 1))

    signal_rows = rows_raw

    extra_coins = extra_signals_list()
    extra_rows_raw = [_signal_row(sym) for sym in extra_coins]
    if sort_mode == 'recently_traded':
        extra_rows_raw.sort(key=lambda r: (
            0 if r['coin_enabled'] else 1,
            _sort_score_recently_traded(r['symbol'])
        ))
    elif sort_mode == 'about_to_trade':
        extra_rows_raw.sort(key=lambda r: (
            0 if r['coin_enabled'] else 1,
            _sort_score_about_to_trade(r['symbol'])
        ))
    else:
        extra_rows_raw.sort(key=lambda r: (0 if r['coin_enabled'] else 1))
    extra_signal_rows = extra_rows_raw
    extra_msg = request.args.get('extra_msg', '')
    extra_msg_type = request.args.get('extra_msg_type', '')

    positions=[]
    for sym,p in state['open_positions'].items():
        positions.append({'symbol':sym,'name':display_name(sym),
                           'side':p['side'],'entry':p['entry'],'tp':p.get('tp'),
                           'sl':p.get('sl'),'opened':p.get('opened','--')})
    top_coins=[]
    for name,d in coin_stats()[:10]:
        top_coins.append({'name':name,'symbol':d.get('symbol') or name,
                           'wr':d['wr'],'wins':d['wins'],'losses':d['losses'],'pnl':d['total_pnl']})
    recent=[]
    for t in reversed(state['trades'][-15:]):
        recent.append({'symbol':t['symbol'],'name':display_name(t['symbol']),
                        'side':t['side'].upper(),'pnl':t['pnl'],
                        'closed':t.get('closed',''),'reason':t.get('reason','')})
    ov = overall_stats()
    wr = f"{ov['wr']:.0f}%" if (ov['wins']+ov['losses']) else "--"
    pf_display = f"{ov['pf']:.2f}" if ov['pf'] not in (0.0, float('inf')) else ("∞" if ov['pf']==float('inf') else "--")
    coin_mode = load_config().get('coin_mode', 'whitelist')
    live_pnl_enabled = get_settings().get('live_pnl', True)

    return render_template_string(DASH_HTML,
        scan_count=state['scan_count'], next_scan=state['next_scan'],
        api_status=state['api_status'], api_error=state['api_error'],
        balance=state['balance'], total_pnl=ov['total_pnl'], today_pnl=ov['today_pnl'],
        wr=wr, pf_display=pf_display,
        signal_rows=signal_rows, positions=positions, pos_count=len(positions),
        top_coins=top_coins, recent_trades=recent, trade_count=len(state['trades']),
        coin_mode=coin_mode, wl_count=len(COINS_WHITELIST), universe_count=len(COINS_UNIVERSE),
        active_coin_count=len(coins),
        extra_signal_rows=extra_signal_rows, extra_count=len(extra_coins),
        extra_msg=extra_msg, extra_msg_type=extra_msg_type,
        start_time=state['start_time'], now=_dt(),
        candle_sync_active=state['candle_sync_active'],
        next_candle_time=state['next_candle_time'],
        candle_sync_count=state['candle_sync_count'],
        last_candle_sync=state['last_candle_sync'],
        sort_mode=sort_mode, sort_labels=SORT_LABELS, sort_modes=SORT_MODES,
        live_pnl_enabled=live_pnl_enabled,
        theme=current_theme())

@app.route('/setup', methods=['GET','POST'])
def setup():
    if request.method=='GET':
        return render_template_string(SETUP_HTML, error=None, theme=current_theme())
    api_key = request.form.get('api_key','').strip()
    api_secret = request.form.get('api_secret','').strip()
    tg_token = request.form.get('tg_token','').strip()
    tg_chat_id = request.form.get('tg_chat_id','').strip()
    if not api_key or not api_secret:
        return render_template_string(SETUP_HTML, error="API key and secret are required", theme=current_theme())
    save_config_data({'api_key':api_key,'api_secret':api_secret,
                       'tg_token':tg_token,'tg_chat_id':tg_chat_id})
    start_bot()
    start_tg_poll_once()
    return redirect('/')

@app.route('/set_sort/<mode>', methods=['POST'])
def set_sort_route(mode):
    set_sort_mode(mode)
    return redirect('/')

@app.route('/toggle_universe', methods=['POST'])
def toggle_universe():
    mode = load_config().get('coin_mode', 'whitelist')
    new_mode = 'universe' if mode == 'whitelist' else 'whitelist'
    save_config_data({'coin_mode': new_mode})
    return redirect('/')

@app.route('/toggle/coin/<symbol>', methods=['POST'])
def toggle_coin_route(symbol):
    toggle_coin(symbol)
    return redirect(request.referrer or '/')

@app.route('/enable_all_coins', methods=['POST'])
def enable_all_coins_route():
    enable_all_coins()
    return redirect(request.referrer or '/')

@app.route('/extra/add', methods=['POST'])
def extra_add_route():
    symbol = request.form.get('symbol', '')
    status, msg = add_extra_signal(symbol)
    return redirect(f'/?extra_msg={quote(msg)}&extra_msg_type={status}')

@app.route('/extra/remove/<symbol>', methods=['POST'])
def extra_remove_route(symbol):
    remove_extra_signal(symbol.upper())
    return redirect('/')

@app.route('/settings', methods=['GET','POST'])
def settings_page():
    saved=False
    if request.method=='POST':
        old_margin_type = get_settings()['margin_type']
        try:
            data={
                'margin_usd': float(request.form.get('margin_usd', 1.0)),
                'margin_percent': float(request.form.get('margin_percent', 2.0)),
                'margin_mode': request.form.get('margin_mode','fixed'),
                'leverage': int(request.form.get('leverage', 5)),
                'margin_type': request.form.get('margin_type','CROSSED'),
                'cooldown_min': int(request.form.get('cooldown_min', 5)),
                'scan_every': int(request.form.get('scan_every', 120)),
                'theme': request.form.get('theme','classic'),
                'live_pnl': request.form.get('live_pnl','on') == 'on',
                'share_signals': request.form.get('share_signals','off') == 'on',
                'signal_channel': request.form.get('signal_channel','').strip(),
            }
            if data['margin_mode'] not in ('fixed','percent'): data['margin_mode']='fixed'
            if data['margin_type'] not in ('CROSSED','ISOLATED'): data['margin_type']='CROSSED'
            if data['theme'] not in THEMES: data['theme']='classic'
            save_config_data(data)
            if data['margin_type']!=old_margin_type and client:
                threading.Thread(target=apply_margin_type_all, daemon=True).start()
        except (ValueError, TypeError):
            pass
        api_key = request.form.get('api_key','').strip()
        api_secret = request.form.get('api_secret','').strip()
        tg_token = request.form.get('tg_token','').strip()
        tg_chat_id = request.form.get('tg_chat_id','').strip()
        creds={}
        if api_key: creds['api_key']=api_key
        if api_secret: creds['api_secret']=api_secret
        if tg_token: creds['tg_token']=tg_token
        if tg_chat_id: creds['tg_chat_id']=tg_chat_id
        if creds: save_config_data(creds)
        if api_key or api_secret:
            try:
                init_client()
                state['api_status']='ok'; state['api_error']=''
            except Exception as e:
                _note_api_error(str(e))
        if tg_token or tg_chat_id:
            start_tg_poll_once()
        return redirect('/settings?saved=1')
    cfg = load_config()
    s = get_settings()
    # Merge extra fields not in DEFAULT_SETTINGS into s for template access
    s['share_signals'] = cfg.get('share_signals', False)
    s['signal_channel'] = cfg.get('signal_channel', '')
    s_obj = type('S', (), s)()  # namespace for template dot-access
    return render_template_string(SETTINGS_HTML, s=s_obj, themes=THEMES,
        theme_labels=THEME_LABELS,
        api_key=cfg.get('api_key',''), tg_token=cfg.get('tg_token',''),
        tg_chat_id=cfg.get('tg_chat_id',''), saved=request.args.get('saved')=='1',
        theme=current_theme())

@app.route('/history')
def history_page():
    trades=[]
    for t in reversed(state['trades']):
        trades.append({'symbol':t['symbol'],'name':display_name(t['symbol']),
                        'side':t['side'].upper(),'pnl':t['pnl'],'entry':t.get('entry'),
                        'exit':t.get('exit'),'closed':t.get('closed',''),
                        'reason':t.get('reason','')})
    return render_template_string(HISTORY_HTML, trades=trades, theme=current_theme())

@app.route('/data')
def data_center():
    a = advanced_stats()
    eq_bars=[]
    if a['equity_curve']:
        max_abs = max([abs(v) for v in a['equity_curve']] or [1]) or 1
        for v in a['equity_curve']:
            eq_bars.append({'val':v,'pct':max(2,round(abs(v)/max_abs*100))})

    daily = daily_pnl_series(14)
    max_abs_day = max([abs(d['pnl']) for d in daily] or [1]) or 1
    for d in daily:
        d['bar_pct'] = min(100, round(abs(d['pnl'])/max_abs_day*100))

    sym_stats = symbol_stats()
    ranked = sorted(sym_stats.items(), key=lambda x:x[1]['total_pnl'], reverse=True)
    gainers = [r for r in ranked if r[1]['total_pnl']>0][:10]
    losers  = [r for r in ranked if r[1]['total_pnl']<0][-10:][::-1]
    max_gain = max([d['total_pnl'] for _,d in gainers] or [1]) or 1
    max_loss = max([abs(d['total_pnl']) for _,d in losers] or [1]) or 1
    top_gainers=[{'symbol':sym,'name':display_name(sym),'pnl':d['total_pnl'],
                  'bar_pct':min(100,round(d['total_pnl']/max_gain*100))} for sym,d in gainers]
    top_losers=[{'symbol':sym,'name':display_name(sym),'pnl':d['total_pnl'],
                 'bar_pct':min(100,round(abs(d['total_pnl'])/max_loss*100))} for sym,d in losers]

    return render_template_string(DATA_HTML, a=a, eq_bars=eq_bars, daily_pnl=daily,
        top_gainers=top_gainers, top_losers=top_losers, theme=current_theme())

@app.route('/coin/<symbol>')
def coin_detail(symbol):
    symbol = symbol.upper()
    ov = symbol_stats(symbol)
    ov['pf_display'] = (f"{ov['pf']:.2f}" if ov['pf'] not in (0.0, float('inf'))
                         else ('∞' if ov['pf']==float('inf') else '--'))
    coin_enabled = is_coin_enabled(symbol)
    is_extra = is_extra_signal(symbol)
    trades=[]
    for t in reversed(state['trades']):
        if t['symbol']!=symbol: continue
        trades.append({'side':t['side'].upper(),'pnl':t['pnl'],
                        'entry':t.get('entry'),'exit':t.get('exit'),
                        'closed':t.get('closed',''),'reason':t.get('reason','')})
    return render_template_string(COIN_HTML, coin=display_name(symbol), symbol=symbol,
        overall=type('O', (), ov)(), coin_enabled=coin_enabled, is_extra=is_extra, trades=trades,
        theme=current_theme())

@app.route('/scan', methods=['POST','GET'])
def force_scan():
    state['scan_requested']=True
    return redirect('/')

@app.route('/close/<symbol>', methods=['POST','GET'])
def close_route(symbol):
    if client and symbol in state['open_positions']:
        close_position(symbol, reason='manual_close')
    return redirect('/')

@app.route('/clear/cache', methods=['POST','GET'])
def clear_cache():
    state['last_signals'].clear()
    return redirect('/')

@app.route('/clear/data', methods=['POST','GET'])
def clear_data():
    state['trades'].clear(); state['wins']=0; state['losses']=0
    state['total_pnl']=0.0; state['today_pnl']=0.0
    if HISTORY_PATH.exists(): HISTORY_PATH.unlink()
    return redirect('/')

@app.route('/api/state')
def api_state():
    return jsonify({
        'balance':state['balance'],'total_pnl':state['total_pnl'],
        'wins':state['wins'],'losses':state['losses'],
        'open_positions':state['open_positions'],
        'scan_count':state['scan_count'],'next_scan':state['next_scan'],
        'api_status':state['api_status'],
        'coin_mode':load_config().get('coin_mode','whitelist'),
    })

@app.route('/api/live_pnl')
def api_live_pnl():
    """Return live unrealized PnL for each open position using current mark price."""
    result = {}
    positions = state['open_positions']
    if not positions or not client:
        return jsonify({'positions': {}})
    for symbol, pos in positions.items():
        try:
            ticker = client.futures_symbol_ticker(symbol=symbol)
            mark = float(ticker['price'])
            entry = float(pos['entry'])
            qty   = float(pos['qty'])
            if pos['side'] == 'buy':
                pnl = (mark - entry) * qty
                pct = ((mark - entry) / entry) * 100
            else:
                pnl = (entry - mark) * qty
                pct = ((entry - mark) / entry) * 100
            result[symbol] = {
                'pnl': round(pnl, 4),
                'pct': round(pct, 3),
                'mark': mark,
            }
        except Exception:
            result[symbol] = {'pnl': None, 'pct': None, 'mark': None}
    return jsonify({'positions': result})

@app.route('/api/candle_countdown')
def api_candle_countdown():
    """Return seconds remaining to the next 15m candle open."""
    try:
        secs = seconds_to_next_candle()
        return jsonify({'seconds': round(secs, 1), 'next_time': state['next_candle_time']})
    except Exception as e:
        return jsonify({'seconds': None, 'error': str(e)})

# ── Main ────────────────────────────────────────────────────
if __name__ == '__main__':
    if is_configured():
        start_bot()
        start_tg_poll_once()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
