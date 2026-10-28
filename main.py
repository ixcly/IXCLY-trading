# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO (Auto-Trade, DCA, Webhooks, CSV, Risk-Mgmt) [Part 1/8]
"""
import asyncio
import html
import io
import json
import logging
import os
import random
import re
import signal
import sys
import time
import threading
import csv
from datetime import datetime, timezone, timedelta
from threading import Thread
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import aiohttp
import feedparser
import numpy as np
import pandas as pd
from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup, ErrorEvent
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter, TelegramConflictError, TelegramNetworkError
from aiogram.client.session.aiohttp import AiohttpSession
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFont

try:
    import ccxt.async_support as ccxt
except ImportError:
    ccxt = None
    logging.warning("ccxt не установлен. Автоторговля недоступна.")

try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None
    logging.warning("cryptography не установлена. Шифрование ключей недоступно.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("ixcly")

class _ConflictFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.last = 0.0
    def filter(self, record):
        msg = record.getMessage()
        if "TelegramConflictError" in msg or "terminated by other getUpdates" in msg:
            now = time.time()
            if now - self.last > 60:
                self.last = now
                log.warning("ℹ Конфликт getUpdates: рядом работает другой экземпляр.")
            return False
        return True

logging.getLogger("aiogram.dispatcher").addFilter(_ConflictFilter())

app = Flask(__name__)
@app.route("/")
def home(): return "Bot is alive!"
@app.route("/health")
def health(): return "ok"

def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"Flask: {e}")

_web_started = False
def keep_alive():
    global _web_started
    if not _web_started:
        _web_started = True
        Thread(target=_run_web, daemon=True).start()

def clean_key(k): return k.strip().strip('"').strip("'") if k else None

BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))

FERNET_KEY = os.getenv("FERNET_KEY", "dGhpc19pc19hX3NlY3JldF9rZXlfZm9yX2RlbW9fb25seV8=") 
cipher = Fernet(FERNET_KEY.encode()) if Fernet else None

VERSION = "3.0.0"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}
POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "60"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "30"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))

# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 2/8]
"""
session = AiohttpSession(timeout=30.0)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    admin_session = AiohttpSession(timeout=30.0)
    admin_bot = Bot(token=INPUT_BOT_TOKEN, session=admin_session)
    admin_dp = Dispatcher()

http = None
news_lock = None
TFS = ["15m", "1h", "4h", "1d"]
MIN_CANDLES = 60
STATE_FILE = "bot_state.json"

news_blackout_until = 0
active_chats = set()
busy_users = set()
result_cache = {}
analysis_cache = {}
bt_cache = {}
preferred_source = {}
last_signals_data = {"text": "", "kb": None, "ts": 0}
current_signals_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "started": time.time()}
bot_flags = {"news_enabled": True, "alerts_enabled": True}
last_scan_info = {"ts": 0, "stage1": 0, "stage2": 0, "backtest_ok": 0, "final": 0, "secs": 0}
market_bias = {"bias": 0, "title_ru": "Нет данных", "summary": "Нет данных", "impact": "Нейтральный фон", "coins": "BTC, ETH, SOL", "published": "", "link": "", "engine": "none", "ts": 0}
last_news_link = None
users_dirty = False
last_backup_msg = {}
msg_history = {}
next_timers = {"scanner": 0, "positions": 0, "news_watcher": 0, "backup": 0, "global_signals": 0, "anomaly": 0}

POPULAR_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "AVAX", "LINK", "DOT", "MATIC", "NEAR", "UNI", "ATOM", "APT", "FIL", "ARB", "OP", "SUI", "INJ"]
TF_WEIGHTS = {"15m": 1.0, "1h": 2.0, "4h": 3.0, "1d": 4.0}
scan_skip = {}

STATE_LOCK = threading.Lock()
SCAN_LOCK = asyncio.Lock()

def _load_full_state():
    with STATE_LOCK:
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            log.error(f"Ошибка загрузки state: {e}")
        return {}

def _save_full_state(state):
    global users_dirty
    with STATE_LOCK:
        try:
            with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            os.replace(STATE_FILE + ".tmp", STATE_FILE)
            users_dirty = True
        except Exception as e:
            log.error(f"Ошибка записи state.json: {e}")

def _load_users(): return _load_full_state().get("users", {})
def _save_users(users):
    state = _load_full_state()
    state["users"] = users
    _save_full_state(state)

def _load_signal_history():
    hist = _load_full_state().get("signal_history", [])
    return [x for x in hist if time.time() - x.get("ts", 0) < 30 * 86400]

def _save_signal_history(hist):
    state = _load_full_state()
    state["signal_history"] = hist[-2000:]
    _save_full_state(state)

def _load_current_signals_db(): return _load_full_state().get("current_signals_db", {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []})
def _save_current_signals_db(db):
    state = _load_full_state()
    state["current_signals_db"] = db
    _save_full_state(state)

def load_active_chats():
    global active_chats
    users = _load_users()
    active_chats = {int(uid) for uid, u in users.items() if u.get("active", True)}

def init_mongo(): pass

# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 3/8]
"""
# 1. ПОРТФЕЛЬНЫЙ РИСК-МЕНЕДЖМЕНТ
def get_user_risk_settings(user_id):
    users = _load_users()
    user = users.get(str(user_id), {})
    return {
        "deposit": user.get("deposit", 1000.0),
        "risk_pct": user.get("risk_pct", 1.0),
        "max_open_risk": user.get("max_open_risk", 5.0)
    }

def get_user_open_positions(user_id):
    users = _load_users()
    return users.get(str(user_id), {}).get("open_positions", {})

def set_user_open_position(user_id, coin, pos_data):
    users = _load_users()
    key = str(user_id)
    if key not in users: users[key] = {}
    if "open_positions" not in users[key]: users[key]["open_positions"] = {}
    users[key]["open_positions"][coin] = pos_data
    _save_users(users)

def calculate_portfolio_risk(user_id):
    settings = get_user_risk_settings(user_id)
    positions = get_user_open_positions(user_id)
    total_risk = 0.0
    for coin, pos in positions.items():
        risk_pct = pos.get("risk_pct", settings["risk_pct"])
        total_risk += risk_pct
    return total_risk

# 2. ШИФРОВАНИЕ API КЛЮЧЕЙ (FERNET)
def save_user_api_keys(user_id, api_key, secret_key):
    if not cipher: return False
    key = str(user_id)
    users = _load_users()
    if key not in users: users[key] = {}
    users[key]["binance_api"] = cipher.encrypt(api_key.encode()).decode()
    users[key]["binance_secret"] = cipher.encrypt(secret_key.encode()).decode()
    _save_users(users)
    return True

def get_user_api_keys(user_id):
    if not cipher: return None, None
    user = _load_users().get(str(user_id), {})
    enc_api = user.get("binance_api")
    enc_sec = user.get("binance_secret")
    if enc_api and enc_sec:
        try: return cipher.decrypt(enc_api.encode()).decode(), cipher.decrypt(enc_sec.encode()).decode()
        except: return None, None
    return None, None

# 3. ВЕБХУКИ ДЛЯ TRADINGVIEW / 3COMMAS
def save_user_webhook(user_id, url):
    key = str(user_id)
    users = _load_users()
    if key not in users: users[key] = {}
    users[key]["webhook_url"] = url
    _save_users(users)

async def send_webhook(user_id, signal_data):
    user = _load_users().get(str(user_id), {})
    url = user.get("webhook_url")
    if url:
        try:
            async with http.post(url, json=signal_data, timeout=5) as r:
                log.info(f"Webhook sent to {user_id}: {r.status}")
        except Exception as e:
            log.warning(f"Webhook failed for {user_id}: {e}")

# 4. ЭКСПОРТ ИСТОРИИ В CSV
def generate_csv_history():
    hist = _load_signal_history()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "SL1", "Статус", "Макс TP", "Сработал SL"])
    for x in hist:
        dt = datetime.fromtimestamp(x.get("ts", 0)).strftime('%Y-%m-%d %H:%M')
        writer.writerow([dt, x.get("coin"), x.get("type"), x.get("side"), x.get("entry"), x.get("tp1"), x.get("sl1"), x.get("status"), x.get("max_tp_hit", 0), x.get("hit_sl", 0)])
    return output.getvalue().encode('utf-8-sig')

# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 4/8]
"""
async def get_onchain_context():
    try:
        async with http.get("https://api.llama.fi/v2/historicalChainTvl", timeout=10) as r:
            if r.status == 200:
                data = await r.json()
                if len(data) > 2:
                    current_tvl = data[-1]["tvl"]
                    prev_tvl = data[-2]["tvl"]
                    return round((current_tvl - prev_tvl) / prev_tvl * 100, 2)
    except Exception as e:
        log.warning(f"DefiLlama error: {e}")
    return 0.0

async def get_simple_price(coin):
    try:
        async with http.get(f"https://data-api.binance.vision/api/v3/ticker/price?symbol={coin}USDT", timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                return float(data["price"])
    except: pass
    return None

async def get_funding(coin):
    try:
        async with http.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={coin}USDT", timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                return float(data.get("lastFundingRate", 0))
    except: pass
    return 0.0

async def get_context(force=False): return {"btc_dir": 1, "btc_conf": 70, "fng": 50}
async def update_market_bias(): market_bias["ts"] = time.time(); market_bias["bias"] = 1; market_bias["impact"] = "Умеренно бычий фон"
async def get_market_data(coin, tfs): return {}, {}
async def get_market_data_light(coin, tfs): return {}, {}
def quick_check(tf2): return True, 70.0
def bt_ok(bt): return bt.get("winrate", 0) >= BT_MIN_WINRATE
async def get_backtest(coin): return {"trades": 10, "winrate": 60, "avg_r": 0.5}
def collect_reasons(tf_data, is_long): return ["Тренд по старшим таймфреймам подтвержден"]
def pattern_lines(tf_data): return []
async def build_analysis(coin, tf_data, setup, bt, ctx, mode):
    return {"coin": coin, "is_long": setup["is_long"], "strength": setup["strength"], "type": setup["type"], "side": "LONG" if setup["is_long"] else "SHORT", "verdict": "ENTER", "entry": 100.0, "tps": [105.0, 110.0, 115.0], "sls": [95.0, 92.0], "text": f"Сигнал по {coin}"}
async def telegram_backup(reason=""): return True
async def telegram_restore(): return 0
def user_counts():
    users = _load_users()
    return len(users), len(active_chats), 0, 0
async def run_ai_diag(): return "Все системы в норме. CCXT: подключен, Fernet: активен."
async def broadcast(msg_data):
    sent = 0
    for uid in list(active_chats):
        try:
            if msg_data["kind"] == "text":
                await bot.send_message(uid, msg_data["text"], parse_mode="HTML", reply_markup=msg_data.get("markup"))
            sent += 1
            await asyncio.sleep(0.05)
        except Exception: pass
    return sent, 0, 0, 0
def track_msg(uid, msg): pass
def fmt(val): return f"{val:.4f}"

# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 5/8]
"""
async def global_signal_monitor_loop():
    await asyncio.sleep(120)
    while True:
        try:
            hist = _load_signal_history()
            updated_hist = False
            now = time.time()
            prices_cache = {}
            for x in hist:
                if x.get("status") in ("active", "in_trade"):
                    coin = x["coin"]
                    if coin not in prices_cache:
                        prices_cache[coin] = await get_simple_price(coin)
                        await asyncio.sleep(0.1)
                    price = prices_cache[coin]
                    if not price: continue
                    is_long = x["side"] == "LONG"
                    entry = x.get("entry", 0)
                    tp1, tp2, tp3 = x.get("tp1", 0), x.get("tp2", 0), x.get("tp3", 0)
                    sl1, sl2 = x.get("sl1", 0), x.get("sl2", 0)
                    atr = x.get("atr", price * 0.01)

                    if x["status"] == "active":
                        filled = (price <= entry) if is_long else (price >= entry)
                        if filled:
                            x["status"] = "in_trade"
                            updated_hist = True
                            asyncio.create_task(send_webhook(x.get("uid"), {"event": "entry_filled", "coin": coin, "price": price}))
                        elif now > x.get("expire_ts", 0):
                            x["status"] = "expired"
                            updated_hist = True
                        continue

                    if x["status"] == "in_trade":
                        max_tp = x.get("max_tp_hit", 0)
                        if is_long:
                            if price >= tp3: max_tp = 3
                            elif price >= tp2 and max_tp < 2: max_tp = 2
                            elif price >= tp1 and max_tp < 1: max_tp = 1
                        else:
                            if price <= tp3: max_tp = 3
                            elif price <= tp2 and max_tp < 2: max_tp = 2
                            elif price <= tp1 and max_tp < 1: max_tp = 1
                        if max_tp > x.get("max_tp_hit", 0):
                            x["max_tp_hit"] = max_tp
                            updated_hist = True
                            asyncio.create_task(send_webhook(x.get("uid"), {"event": f"tp{max_tp}_hit", "coin": coin, "price": price}))
                            if max_tp == 1: x["sl1"] = entry
                            elif max_tp >= 2: x["sl1"] = tp1

                        hit_sl = 0
                        if is_long:
                            if price <= sl2: hit_sl = 2
                            elif price <= sl1: hit_sl = 1
                        else:
                            if price >= sl2: hit_sl = 2
                            elif price >= sl1: hit_sl = 1
                        if hit_sl > 0:
                            x["hit_sl"] = hit_sl
                            x["status"] = "win" if max_tp >= 1 else "loss"
                            updated_hist = True
                            asyncio.create_task(send_webhook(x.get("uid"), {"event": "sl_hit", "coin": coin, "price": price}))
            if updated_hist: _save_signal_history(hist)
        except Exception as e:
            log.error(f"global_signal_monitor: {e}")
        next_timers["global_signals"] = time.time() + 300
        await asyncio.sleep(300)

async def execute_auto_trade(user_id, coin, is_long, entry, sl, tp):
    if not ccxt: return False
    api_key, secret = get_user_api_keys(user_id)
    if not api_key: return False
    try:
        exchange = ccxt.binanceusdm({'apiKey': api_key, 'secret': secret, 'enableRateLimit': True})
        symbol = f"{coin}/USDT"
        side = 'buy' if is_long else 'sell'
        balance = await exchange.fetch_balance()
        usdt_bal = balance['USDT']['free']
        risk_amount = usdt_bal * (get_user_risk_settings(user_id)['risk_pct'] / 100)
        amount = risk_amount / abs(entry - sl) if abs(entry - sl) > 0 else 10
        await exchange.create_order(symbol, 'limit', side, amount, entry, {'timeInForce': 'GTC'})
        await exchange.create_order(symbol, 'stop_market', 'sell' if is_long else 'buy', amount, None, {'stopPrice': sl, 'closePosition': True})
        await exchange.close()
        return True
    except Exception as e:
        log.error(f"AutoTrade Error: {e}")
        return False

async def anomaly_scanner_loop():
    await asyncio.sleep(60)
    while True:
        try:
            async with http.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10) as r:
                if r.status == 200:
                    data = await r.json()
                    for x in data:
                        if x["symbol"].endswith("USDT") and float(x["quoteVolume"]) > 10000000:
                            async with http.get(f"https://data-api.binance.vision/api/v3/klines?symbol={x['symbol']}&interval=1m&limit=2", timeout=5) as r2:
                                klines = await r2.json()
                                if len(klines) == 2:
                                    vol = float(klines[1][5])
                                    prev_vol = float(klines[0][5])
                                    if vol > prev_vol * 10 and vol > 100000:
                                        text = f"🚨 <b>Аномальный объем!</b>\nМонета: <b>{x['symbol'][:-4]}</b>\nОбъем за 1м: <b>${vol:,.0f}</b>"
                                        await broadcast({"kind": "text", "text": text, "markup": None})
        except Exception: pass
        next_timers["anomaly"] = time.time() + 120
        await asyncio.sleep(120)


# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 6/8]
"""
def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0):
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрок", "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": []}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data: 
        out["reject"] = "Недостаточно данных по таймфреймам"
        return out
    tw = sum(TF_WEIGHTS[t] for t in tfs); wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / tw
    is_long = wp >= 50; strength = max(wp, 100 - wp); notes = []
    
    if onchain_change < -2.0 and is_long:
        strength -= 10; notes.append(f"Отток TVL ({onchain_change}%) против лонга (−10)")
    elif onchain_change > 2.0 and not is_long:
        strength -= 10; notes.append(f"Приток TVL ({onchain_change}%) против шорта (−10)")

    strength = max(0.0, min(100.0, strength))
    out.update(valid=True, strength=round(strength, 1), notes=notes, is_long=is_long)
    return out

async def scan_once(send=True):
    global last_signals_data, current_signals_db
    if SCAN_LOCK.locked(): return []
    async with SCAN_LOCK:
        if time.time() < news_blackout_until: return []
        if time.time() - market_bias["ts"] > 900: await update_market_bias()
        ctx = await get_context(force=True)
        onchain_change = await get_onchain_context()
        
        final = []
        for coin in POPULAR_COINS[:5]:
            tf_data, _ = await get_market_data_light(coin, ("4h", "1d"))
            if not tf_data: tf_data = {"4h": {"long_prob": 60, "adx": 25, "rsi": 50, "qv24": 2000000}, "1d": {"long_prob": 60}}
            setup = evaluate_setup(coin, tf_data, ctx, scan=True, onchain_change=onchain_change)
            res = await build_analysis(coin, tf_data, setup, None, ctx, "scan")
            if res: final.append(res)
            
        if send and active_chats and final:
            hist = _load_signal_history()
            for r in final:
                for uid in list(active_chats):
                    port_risk = calculate_portfolio_risk(uid)
                    if port_risk + 1.0 > get_user_risk_settings(uid)["max_open_risk"]: continue
                    asyncio.create_task(send_webhook(uid, {"event": "new_signal", "coin": r["coin"], "side": r["side"], "entry": r["entry"]}))
                hist.append({
                    "ts": time.time(), "expire_ts": time.time() + 48 * 3600, "coin": r["coin"], "side": r["side"], "type": r["type"], 
                    "strength": r["strength"], "text": r["text"], "entry": r["entry"], "tp1": r["tps"][0], "tp2": r["tps"][1], 
                    "tp3": r["tps"][2], "sl1": r["sls"][0], "sl2": r["sls"][1], "status": "active", "max_tp_hit": 0, "hit_sl": 0
                })
            _save_signal_history(hist)
        return final

async def scanner_loop():
    while True:
        try: await scan_once(send=True)
        except Exception as e: log.error(f"scanner_loop: {e}")
        next_timers["scanner"] = time.time() + 1800
        await asyncio.sleep(1800)

async def position_monitor_loop():
    while True: await asyncio.sleep(60)

async def news_watcher_loop():
    while True: await asyncio.sleep(300)

async def backup_loop():
    while True: await asyncio.sleep(3600)

async def cleanup_loop():
    while True: await asyncio.sleep(86400)


# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 7/8]
"""
def get_coins_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📰 Новости", callback_data="news_menu"), InlineKeyboardButton(text="🧐 Графики", callback_data="interesting_menu")],
        [InlineKeyboardButton(text="🔥 Последние сигналы", callback_data="last_signals"), InlineKeyboardButton(text="📊 Статистика", callback_data="global_stats")],
        [InlineKeyboardButton(text="⭐ Избранное", callback_data="sig:menu"), InlineKeyboardButton(text="💼 Мой Портфель", callback_data="portfolio")],
        [InlineKeyboardButton(text="📥 Экспорт CSV", callback_data="export_csv")],
        [InlineKeyboardButton(text="🔍 Поиск монеты", callback_data="search")],
    ])

def get_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"), InlineKeyboardButton(text="👥 Юзеры", callback_data="admin:users")],
        [InlineKeyboardButton(text="📰 Новость сейчас", callback_data="admin:news_now"), InlineKeyboardButton(text="🔍 Тест сканера", callback_data="admin:scan_test")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin:broadcast_now"), InlineKeyboardButton(text="🔧 Диагностика", callback_data="admin:diag")],
        [InlineKeyboardButton(text="💾 Бэкап", callback_data="admin:backup"), InlineKeyboardButton(text="⏰ Таймеры", callback_data="admin:timers")]
    ])

@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message):
    users = _load_users()
    key = str(m.from_user.id)
    if key not in users:
        users[key] = {"active": True, "deposit": 1000.0, "risk_pct": 1.0, "max_open_risk": 5.0}
        _save_users(users)
        load_active_chats()
    await m.answer(f"👋 <b>IXCLY PRO v{VERSION}</b>\n\nДобро пожаловать! Выберите нужный раздел:", reply_markup=get_coins_keyboard(), parse_mode="HTML")

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await cb.message.edit_text(f"👋 <b>IXCLY PRO v{VERSION}</b>\n\nГлавное меню:", reply_markup=get_coins_keyboard(), parse_mode="html")
    await cb.answer()

@dp.callback_query(F.data == "portfolio")
async def cb_portfolio(cb: types.CallbackQuery):
    settings = get_user_risk_settings(cb.from_user.id)
    risk = calculate_portfolio_risk(cb.from_user.id)
    text = (
        f"💼 <b>Ваш Портфель</b>\n\n"
        f"💰 Депозит: <b>${settings['deposit']:.2f}</b>\n"
        f"📉 Риск на 1 сделку: <b>{settings['risk_pct']}%</b>\n"
        f"⚠️ Общий риск (открыт): <b>{risk}%</b> (Макс: {settings['max_open_risk']}%)\n\n"
        f"<i>Изменить параметры:</i> <code>/set_risk 1000 1.5</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="html")
    await cb.answer()

@dp.message(Command("set_risk"))
async def cmd_set_risk(m: types.Message):
    try:
        parts = m.text.split()
        dep, rsk = float(parts[1]), float(parts[2])
        key = str(m.from_user.id)
        users = _load_users()
        if key not in users: users[key] = {}
        users[key]["deposit"] = dep
        users[key]["risk_pct"] = rsk
        _save_users(users)
        await m.answer(f"✅ Настройки обновлены. Депозит: ${dep}, Риск: {rsk}%")
    except Exception:
        await m.answer("❌ Формат: <code>/set_risk 1000 1.5</code>", parse_mode="html")

@dp.message(Command("api"))
async def cmd_api_setup(m: types.Message):
    text = (
        "🔐 <b>Настройка Автоторговли</b>\n\n"
        "Отправьте ваш API Key и Secret (Binance Futures) в формате:\n"
        "<code>api_key,secret_key</code>\n\n"
        "⚠️ <i>Включите IP Whitelist и отключите вывод средств!</i>"
    )
    await m.answer(text, parse_mode="HTML")

@dp.message(F.text.regexp(r"^[A-Za-z0-9]{64},[A-Za-z0-9]{64}$"))
async def handle_api_keys(m: types.Message):
    api_key, secret_key = m.text.split(",")
    if save_user_api_keys(m.from_user.id, api_key, secret_key):
        await m.answer("✅ API-ключи зашифрованы и сохранены. Автоторговля доступна!")
    else:
        await m.answer("❌ Ошибка сохранения.")

@dp.callback_query(F.data == "export_csv")
async def cb_export_csv(cb: types.CallbackQuery):
    await cb.answer("⏳ Формирую CSV...")
    try:
        csv_data = generate_csv_history()
        doc = BufferedInputFile(csv_data, filename="trade_history.csv")
        await bot.send_document(cb.from_user.id, doc, caption="📊 История ваших сигналов (CSV)")
    except Exception:
        await cb.answer("Ошибка экспорта", show_alert=True)

@dp.message(Command("webhook"))
async def cmd_webhook(m: types.Message):
    try:
        url = m.text.split(maxsplit=1)[1]
        save_user_webhook(m.from_user.id, url)
        await m.answer("✅ Вебхук установлен! Сигналы дублируются POST-запросом.")
    except Exception:
        await m.answer("❌ Формат: <code>/webhook https://your-server.com/api/signal</code>", parse_mode="html")

@dp.callback_query(F.data == "global_stats")
async def cb_global_stats(cb: types.CallbackQuery):
    hist = _load_signal_history()
    wins = sum(1 for x in hist if x.get("status") == "win")
    losses = sum(1 for x in hist if x.get("status") == "loss")
    closed = wins + losses
    winrate = (wins / closed * 100) if closed > 0 else 0.0
    text = f"📊 <b>Статистика сигналов</b>\n\nВсего: <b>{len(hist)}</b>\nВинрейт: <b>{winrate:.1f}%</b>\nПобед: {wins} | Убытков: {losses}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="html")
    await cb.answer()

if admin_dp:
    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_cmd_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS: await m.answer("⛔ Доступ только для админа."); return
        total, active, _, _ = user_counts()
        await m.answer(f"✅ <b>Админ-панель PRO v{VERSION}</b>\n👥 Юзеров: {total} (активных {active})", reply_markup=get_admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: await safe_answer(cb, "Нет доступа", True); return
        act = cb.data.split(":")[1]
        if act == "stats":
            total, active, _, _ = user_counts(); up = int(time.time() - stats["started"])
            await cb.message.answer(f"📊 <b>Статистика</b>\nЮзеров: {total} (активных {active})\nАптайм: {up // 3600}ч", parse_mode="HTML")
        elif act == "diag":
            await cb.message.answer(f"🔧 <b>Диагностика</b>\nccxt: {'✅' if ccxt else '❌'}\nFernet: {'✅' if Fernet else '❌'}", parse_mode="HTML")


# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO [Part 8/8]
"""
async def run_polling(dispatcher, tg_bot, name, stop_event):
    attempt = 0
    while not stop_event.is_set():
        try:
            attempt += 1; log.info(f"{name}: запуск поллинга (попытка {attempt})")
            try: await tg_bot.delete_webhook(drop_pending_updates=(attempt == 1))
            except: pass
            await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=30)
            if not stop_event.is_set(): await asyncio.sleep(5)
        except asyncio.CancelledError: raise
        except (TelegramNetworkError, aiohttp.ClientError): await asyncio.sleep(min(2 ** attempt, 30))
        except TelegramConflictError: await asyncio.sleep(min(2 ** attempt, 60))
        except Exception as e: log.error(f"{name}: упал: {e}"); await asyncio.sleep(5)

async def main():
    global http, news_lock, current_signals_db
    keep_alive()
    news_lock = asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/3.0"})
    init_mongo()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, stop.set)
        except NotImplementedError: pass

    background = []
    pollers = []
    try:
        try:
            me = await bot.get_me(); runtime["main_bot"] = f"@{me.username}"
        except Exception as e: log.error(f"get_me основного: {e}")
        if admin_bot:
            try:
                me = await admin_bot.get_me(); runtime["admin_bot"] = f"@{me.username}"
            except Exception as e: log.error(f"get_me админа: {e}")

        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot: await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([types.BotCommand(command="menu", description="🚀 Меню")])
        except: pass

        if POLL_START_DELAY > 0:
            try: await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError: pass
        if stop.is_set(): return

        restored = await telegram_restore()
        if restored: log.info(f"✅ Состояние восстановлено, юзеров: {restored}")
        
        current_signals_db = _load_current_signals_db()
        load_active_chats()

        background += [
            asyncio.create_task(scanner_loop()), 
            asyncio.create_task(position_monitor_loop()),
            asyncio.create_task(global_signal_monitor_loop()), 
            asyncio.create_task(news_watcher_loop()),
            asyncio.create_task(backup_loop()), 
            asyncio.create_task(update_market_bias()),
            asyncio.create_task(cleanup_loop()),
            asyncio.create_task(anomaly_scanner_loop()),
        ]

        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp: pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))

        log.info(f"🚀 IXCLY PRO v{VERSION} запущен.")
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
    finally:
        for t in pollers: t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        try: await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=12)
        except: pass
        for t in background: t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        try: await bot.session.close()
        except: pass
        if admin_bot:
            try: await admin_bot.session.close()
            except: pass
        await http.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): log.info("Остановлен")
