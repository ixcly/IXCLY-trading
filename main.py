# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.0.0 - PRO CORE (Config, State, Init)
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
from aiogram.exceptions import (TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, 
                                TelegramRetryAfter, TelegramConflictError, TelegramNetworkError)
from aiogram.client.session.aiohttp import AiohttpSession
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFont

# --- 1. ВНЕШНИЕ БИБЛИОТЕКИ (CCXT, Fernet) ---
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

# --- 2. НАСТРОЙКА ЛОГИРОВАНИЯ И ФИЛЬТРОВ ---
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

# --- 3. FLASK ДЛЯ KEEP-ALIVE ---
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

# --- 4. УТИЛИТЫ И ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ---
def clean_key(k): return k.strip().strip('"').strip("'") if k else None

BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))

FERNET_KEY = os.getenv("FERNET_KEY", "dGhpc19pc19hX3NlY3JldF9rZXlfZm9yX2RlbW9fb25seV8=") 
cipher = Fernet(FERNET_KEY.encode()) if Fernet else None

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-r1:free")

ADMIN_IDS = set()
if ADMIN_ID:
    for x in re.split(r"[,\s]+", ADMIN_ID):
        if x.lstrip("-").isdigit(): ADMIN_IDS.add(int(x))

VERSION = "3.0.0"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}
POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "60"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "30"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))
DEPOSIT = float(os.getenv("DEPOSIT", "1000"))
RISK_PCT = 0.01

STATE_FILE = "bot_state.json"

# --- 5. ГЛОБАЛЬНЫЕ КОНСТАНТЫ И ВЕСА ---
TF_WEIGHTS = {"15m": 0.1, "1h": 0.3, "4h": 0.4, "1d": 0.2}
POPULAR_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "LINK", "AVAX", "DOT", "MATIC", "LTC"]
TFS = ["15m", "1h", "4h", "1d"]
MIN_CANDLES = 60

# --- 6. ИНИЦИАЛИЗАЦИЯ СЕССИЙ И БОТОВ ---
session = AiohttpSession(timeout=30.0)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    admin_session = AiohttpSession(timeout=30.0)
    admin_bot = Bot(token=INPUT_BOT_TOKEN, session=admin_session)
    admin_dp = Dispatcher()

# --- 7. ГЛОБАЛЬНЫЕ КЭШИ И ФЛАГИ ---
http = None
news_lock = None
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
scan_skip = {}

_onchain_cache = {"ts": 0, "tvl_change": 0.0}
_whale_cache = {"ts": 0, "bias": 0}

STATE_LOCK = threading.Lock()
SCAN_LOCK = asyncio.Lock()

# --- 8. СИСТЕМА УПРАВЛЕНИЯ СОСТОЯНИЕМ (STATE MANAGER) ---
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

def _load_users(): 
    return _load_full_state().get("users", {})

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

def _load_current_signals_db(): 
    return _load_full_state().get("current_signals_db", {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []})

def _save_current_signals_db(db):
    state = _load_full_state()
    state["current_signals_db"] = db
    _save_full_state(state)

# --- 9. ПОРТФЕЛЬНЫЙ РИСК-МЕНЕДЖМЕНТ (CORE) ---
def get_user_risk_settings(user_id):
    user = _load_users().get(str(user_id), {})
    return {
        "deposit": user.get("deposit", 1000.0),
        "risk_pct": user.get("risk_pct", 1.0),
        "max_open_risk": user.get("max_open_risk", 5.0)
    }

def calculate_portfolio_risk(user_id):
    settings = get_user_risk_settings(user_id)
    positions = get_user_open_positions(user_id)
    total_risk = 0.0
    for coin, pos in positions.items():
        risk_pct = pos.get("risk_pct", settings["risk_pct"])
        total_risk += risk_pct
    return total_risk

# --- 10. ШИФРОВАНИЕ API КЛЮЧЕЙ ---
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
        return cipher.decrypt(enc_api.encode()).decode(), cipher.decrypt(enc_sec.encode()).decode()
    return None, None

# --- 11. ВЕБХУКИ ---
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

# --- 12. ЭКСПОРТ В CSV ---
def generate_csv_history():
    hist = _load_signal_history()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "SL1", "Статус", "Макс TP", "Сработал SL"])
    for x in hist:
        dt = datetime.fromtimestamp(x.get("ts", 0)).strftime('%Y-%m-%d %H:%M')
        writer.writerow([dt, x.get("coin"), x.get("type"), x.get("side"), x.get("entry"), x.get("tp1"), x.get("sl1"), x.get("status"), x.get("max_tp_hit", 0), x.get("hit_sl", 0)])
    return output.getvalue().encode('utf-8-sig')

# --- 13. ON-CHAIN КОНТЕКСТ (DefiLlama) ---
async def get_onchain_context():
    global _onchain_cache
    if time.time() - _onchain_cache["ts"] < 3600:
        return _onchain_cache["tvl_change"]
    try:
        async with http.get("https://api.llama.fi/v2/historicalChainTvl", timeout=10) as r:
            if r.status == 200:
                data = await r.json()
                if len(data) > 2:
                    current_tvl = data[-1]["tvl"]
                    prev_tvl = data[-2]["tvl"]
                    change = (current_tvl - prev_tvl) / prev_tvl * 100
                    _onchain_cache = {"ts": time.time(), "tvl_change": round(change, 2)}
                    return change
    except Exception as e:
        log.warning(f"DefiLlama error: {e}")
    return 0.0

# --- 14. ЗАГЛУШКИ (для связности частей) ---
def get_user_open_positions(uid): return {}
def set_user_open_position(uid, coin, data): pass
def user_counts(): return (10, 5, 0, 0)
def bt_ok(bt): return True
def collect_reasons(tf_data, is_long): return []
def pattern_lines(tf_data): return []
async def get_market_data(coin, tfs): return ({"1h": {"adx": 25, "rsi": 50}}, None)
async def get_market_data_light(coin, tfs): return ({"1h": {"long_prob": 55}}, None)
async def get_backtest(coin): return {"trades": 10}
async def get_funding(coin): return 0.0001
async def telegram_backup(reason): return True
async def telegram_restore(): return 0
def load_active_chats(): pass
def init_mongo(): pass
async def run_ai_diag(): return "OK"
async def download_bytes(url): return None
async def safe_edit(msg, text, reply_markup=None, user_id=None): 
    return await bot.edit_message_text(text, chat_id=msg.chat.id, message_id=msg.message_id, reply_markup=reply_markup)
async def safe_answer(cb, text="✅", alert=False): return await cb.answer(text, show_alert=alert)
async def safe_delete(msg): pass
async def send_analysis(chat_id, res, uid): pass
def add_user_saved_signal(uid, coin, text, res): pass
def get_saved_signals_menu(uid): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
def get_saved_signal_view_keyboard(uid, coin): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
def get_admin_keyboard(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
async def broadcast(data): return (1, 0, 0, 0)
def fmt(x): return f"{x:.2f}"

# --- 15. БАЗОВЫЙ АНАЛИЗ (Quick Check) для сканера ---
def quick_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data: return None
    p4, p1d = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]
    if (p4 >= 50) != (p1d >= 50): return None
    info = tf_data["4h"]
    if info["adx"] < 15: return None
    strength = 0
    if p4 > 60 and p1d > 60: strength += 20
    if info["rsi"] < 30 or info["rsi"] > 70: strength += 10
    return (True, strength)

# --- 16. КОНТЕКСТ РЫНКА (Заглушка для следующей части) ---
async def get_context(force=False): 
    return {"fng": 50, "btc_dir": 0, "btc_conf": 50}
async def update_market_bias(): 
    pass

# --- 17. ПРОВЕРКА SETUP (Ядро логики) ---
def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0):
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрок", "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": []}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data: 
        out["reject"] = "Недостаточно данных по таймфреймам"
        return out
    tw = sum(TF_WEIGHTS[t] for t in tfs); wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / tw
    is_long = wp >= 50; side = 1 if is_long else -1; strength = max(wp, 100 - wp); notes = []
    def cs(p): return p if is_long else 100 - p
    p4, p1d = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]; reject = ""
    if (p4 >= 50) != is_long or (p1d >= 50) != is_long: reject = "4h и 1d смотрят в разные стороны"
    info = tf_data["4h"]
    if not reject and info["adx"] < 15: reject = f"Вялый рынок (ADX {info['adx']:.0f})"
    if scan and info["qv24"] < MIN_QV24: reject = reject or "Низкая ликвидность"
    if "1h" in tf_data and (tf_data["1h"]["long_prob"] >= 50) != is_long and tf_data["1h"]["confidence"] >= 65: 
        strength -= 8; notes.append("1h против направления (−8)")
    if is_long and (info["rsi"] > 75 or info["price"] - info["ema20"] > 2.5 * info["atr"]): 
        strength -= 8; notes.append("цена перегрета, риск погони (−8)")
    if (not is_long) and (info["rsi"] < 25 or info["ema20"] - info["price"] > 2.5 * info["atr"]): 
        strength -= 8; notes.append("цена перепродана, риск погони (−8)")
    if coin != "BTC" and ctx.get("btc_dir", 0) != 0:
        if ctx["btc_dir"] == side: strength += 3; notes.append("BTC в ту же сторону (+3)")
        else: pen = 10 if ctx.get("btc_conf", 50) >= 65 else 5; strength -= pen; notes.append(f"BTC против направления (−{pen})")
    fng = ctx.get("fng")
    if fng is not None:
        if fng >= 80 and is_long: strength -= 4; notes.append(f"экстремальная жадность {fng} (−4)")
        elif fng <= 20 and not is_long: strength -= 4; notes.append(f"экстремальный страх {fng} (−4)")
    if funding is not None:
        if is_long and funding > 0.0005: strength -= 5; notes.append(f"перегрет лонг: funding {funding * 100:.3f}% (−5)")
        elif (not is_long) and funding < -0.0005: strength -= 5; notes.append(f"перегрет шорт: funding {funding * 100:.3f}% (−5)")
    if onchain_change < -2.0 and is_long: strength -= 10; notes.append(f"Отток TVL ({onchain_change}%) против лонга (−10)")
    elif onchain_change > 2.0 and not is_long: strength -= 10; notes.append(f"Приток TVL ({onchain_change}%) против шорта (−10)")
    nb = market_bias.get("bias", 0); age = time.time() - market_bias.get("ts", 0); news_status = "Нейтрально"
    if age < 3 * 3600 and nb != 0:
        word = "бычий" if nb == 1 else "медвежий"
        if nb == side: strength += 7; news_status = f"✅ Подтверждают ({word} фон)"
        else: strength -= 20; news_status = f"❌ Противоречат ({word} фон)"
    strength = max(0.0, min(100.0, strength))
    if not reject and strength < CONF_FINAL: reject = f"Уверенность {strength:.0f}% ниже порога {CONF_FINAL:.0f}%"
    p15, p1h = tf_data.get("15m", {}).get("long_prob", 50), tf_data.get("1h", {}).get("long_prob", 50)
    fast = "15m" in tf_data and "1h" in tf_data and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    sig_type = "Быстрый сигнал" if fast else "Долгосрок" if long_term else "Среднесрок"
    out.update(valid=not reject, reject=reject, notes=notes, is_long=is_long, strength=round(strength, 1), type=sig_type, news_status=news_status, wp=wp, tf_probs={t: tf_data[t]["long_prob"] for t in tfs}, reasons=collect_reasons(tf_data, is_long), patterns=pattern_lines(tf_data))
    return out

# --- 18. ПОЛНЫЙ СКАНЕР (С ИНТЕГРАЦИЕЙ ВСЕХ ФИЛЬТРОВ) ---
async def scan_once(send=True):
    global last_signals_data, current_signals_db, last_scan_info
    if SCAN_LOCK.locked(): return []
    
    async with SCAN_LOCK:
        t0 = time.time()
        if time.time() < news_blackout_until: 
            log.info("Сканирование на паузе из-за экономического календаря.")
            return []

        if time.time() - market_bias["ts"] > 900: await update_market_bias()
        ctx = await get_context(force=True)
        bias_text = market_bias.get("impact", "Нейтральный фон")
        onchain_change = await get_onchain_context()
        
        stage1 = []
        async def s1(coin):
            if time.time() < scan_skip.get(coin, 0): return
            async with asyncio.Semaphore(3):
                try: 
                    tf2, _ = await get_market_data_light(coin, ("4h", "1d"))
                except LookupError: 
                    scan_skip[coin] = time.time() + 6 * 3600; return
                except Exception: return
                q = quick_check(tf2)
                if q: stage1.append((coin, tf2, q[1]))
        
        await asyncio.gather(*[s1(c) for c in POPULAR_COINS])
        stage1.sort(key=lambda x: x[2], reverse=True)
        stage1 = stage1[:18]
        
        stage2 = []
        async def s2(item):
            coin, tf2, _ = item
            async with asyncio.Semaphore(3):
                tf_data = dict(tf2)
                try: 
                    extra, _src = await get_market_data_light(coin, ("1h", "15m"))
                    tf_data.update(extra)
                except Exception: pass
                funding = await get_funding(coin)
                setup = evaluate_setup(coin, tf_data, ctx, funding, scan=True, onchain_change=onchain_change)
                if setup["valid"]: stage2.append((coin, tf_data, setup))
        
        await asyncio.gather(*[s2(i) for i in stage1])
        stage2.sort(key=lambda x: x[2]["strength"], reverse=True)
        stage2 = stage2[:8]
        
        final = []
        bt_pass = 0
        sem3 = asyncio.Semaphore(2)
        async def s3(item):
            nonlocal bt_pass
            coin, tf_data, setup = item
            async with sem3:
                bt = await get_backtest(coin)
                if bt is not None and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt): return
                bt_pass += 1
                try: 
                    res = await build_analysis(coin, tf_data, setup, bt, ctx, "scan")
                except Exception: return
                if res["verdict"] == "ENTER": final.append(res)
        
        await asyncio.gather(*[s3(i) for i in stage2])
        
        final.sort(key=lambda r: r["strength"], reverse=True)
        new_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
        for r in final: 
            new_db[r["type"]].append({"coin": r["coin"], "is_long": r["is_long"], "strength": r["strength"], "type": r["type"], "side": r["side"], "text": r["text"]})
        for k in new_db: new_db[k] = new_db[k][:10]
        current_signals_db = new_db
        _save_current_signals_db(new_db)
        
        if send and active_chats:
            now = time.time()
            hist = _load_signal_history()
            active_coins = {f"{x['coin']}:{x['side']}" for x in hist if x.get("status") in ("active", "in_trade")}
            
            for r in final:
                key = f"{r['coin']}:{'L' if r['is_long'] else 'S'}"
                if key in active_coins: continue
                
                for uid in list(active_chats):
                    port_risk = calculate_portfolio_risk(uid)
                    if port_risk + 1.0 > get_user_risk_settings(uid)["max_open_risk"]: continue
                    asyncio.create_task(send_webhook(uid, {"event": "new_signal", "coin": r["coin"], "side": r["side"], "entry": r["entry"]}))
                
                hist.append({
                    "ts": now, "expire_ts": now + 48 * 3600, "coin": r["coin"], "side": r["side"], "type": r["type"], 
                    "strength": r["strength"], "text": r["text"], "entry": r["entry"], "tp1": r["tps"][0], "tp2": r["tps"][1], 
                    "tp3": r["tps"][2], "sl1": r["sls"][0], "sl2": r["sls"][1], "status": "active", "max_tp_hit": 0, "hit_sl": 0
                })
            _save_signal_history(hist)
            
            msk_now = datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M")
            text = f"🔥 <b>Актуальные сигналы</b> · {msk_now} МСК\n\n📊 <b>Кратко о рынке:</b>\n{html.escape(bias_text)}\n\nВыберите категорию:"
            kb = [
                [InlineKeyboardButton(text=f"⚡ Быстрый ({len(current_signals_db['Быстрый сигнал'])})", callback_data="sig_list:Быстрый сигнал")],
                [InlineKeyboardButton(text=f"⏳ Среднесрок ({len(current_signals_db['Среднесрок'])})", callback_data="sig_list:Среднесрок")],
                [InlineKeyboardButton(text=f"🛡 Долгосрок ({len(current_signals_db['Долгосрок'])})", callback_data="sig_list:Долгосрок")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
            ]
            markup = InlineKeyboardMarkup(inline_keyboard=kb)
            last_signals_data = {"text": text, "kb": markup, "ts": time.time()}
            sent, _, _, _ = await broadcast({"kind": "text", "text": text, "markup": markup})
            stats["signals"] += sent
        return final

# --- 19. ЦИКЛЫ ФОНОВЫХ ЗАДАЧ ---
async def scanner_loop():
    while True:
        try:
            if bot_flags["alerts_enabled"]:
                await scan_once(send=True)
            await asyncio.sleep(300) # Скан раз в 5 минут
        except Exception as e:
            log.error(f"scanner_loop: {e}")
            await asyncio.sleep(60)

async def position_monitor_loop():
    while True:
        try:
            hist = _load_signal_history()
            updated = False
            for x in hist:
                if x.get("status") == "in_trade":
                    await check_position(x.get("uid", 0), x["coin"], x)
            if updated:
                _save_signal_history(hist)
            await asyncio.sleep(30)
        except Exception as e:
            log.error(f"position_monitor: {e}")
            await asyncio.sleep(60)

async def check_position(user_id, coin, pos):
    if pos.get("notified_close"): return
    price = await get_simple_price(coin)
    if not price: return
    side, entry, tp1, tp2, tp3, sl1 = pos["side"], pos["entry_price"], pos["tp1"], pos["tp2"], pos["tp3"], pos["sl1"]
    
    if side == "LONG":
        pnl = (price - entry) / entry * 100
        hit_tp1, hit_tp2, hit_tp3, hit_sl = price >= tp1, price >= tp2, price >= tp3, price <= sl1
    else:
        pnl = (entry - price) / entry * 100
        hit_tp1, hit_tp2, hit_tp3, hit_sl = price <= tp1, price <= tp2, price <= tp3, price >= sl1
        
    reason, urgency = None, "info"
    
    if hit_sl: 
        reason, urgency = "🛑 Сработал стоп-лосс\nРекомендую закрыть позицию.", "danger"
    elif hit_tp3: 
        reason, urgency = "✅ Достигнут TP3\nЗакрываем оставшиеся 20% позиции.", "success"
    elif hit_tp2 and not pos.get("notified_tp2"):
        pos["notified_tp2"] = True
        reason, urgency = "✅ Достигнут TP2\nЗакрываем 30% позиции. Остаток ведем по трейлингу.", "success"
    elif hit_tp1 and not pos.get("notified_tp1"):
        pos["notified_tp1"] = True
        reason, urgency = "✅ Достигнут TP1\nЗакрываем 50% позиции. Стоп переносим в безубыток.", "success"
        
    if urgency == "success": 
        set_user_open_position(user_id, coin, pos)
    if not reason: return
    
    # Здесь должна быть отправка сообщения пользователю (зависит от вашего notify модуля)
    # log.info(f"Монитор {coin}: {reason}")

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
                        passed_tp1 = (price >= tp1) if is_long else (price <= tp1)
                        
                        if filled:
                            x["status"] = "in_trade"
                            updated_hist = True
                            asyncio.create_task(send_webhook(x.get("uid"), {"event": "entry_filled", "coin": coin, "price": price}))
                        elif passed_tp1:
                            try:
                                tf_data, _ = await get_market_data(coin, ("1h",))
                                trend_ok = (tf_data["1h"]["adx"] > 20) and (tf_data["1h"]["rsi"] < 70)
                                if trend_ok:
                                    new_entry = tf_data["1h"]["ema20"]
                                    x["entry"] = new_entry
                                    x["expire_ts"] = time.time() + 12 * 3600
                                    updated_hist = True
                                else:
                                    x["status"] = "missed"
                                    updated_hist = True
                            except Exception:
                                x["status"] = "missed"
                                updated_hist = True
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

                        if not x.get("dca_done"):
                            adverse_move = (entry - price) if is_long else (price - entry)
                            if adverse_move > 1.5 * atr:
                                try:
                                    tf_data, _ = await get_market_data(coin, ("4h",))
                                    trend_ok = (tf_data["4h"]["long_prob"] > 55) if is_long else (tf_data["4h"]["long_prob"] < 45)
                                    if trend_ok:
                                        x["dca_done"] = True
                                        x["entry"] = price
                                        x["sl1"] = price - 2 * atr if is_long else price + 2 * atr
                                        updated_hist = True
                                        asyncio.create_task(send_webhook(x.get("uid"), {"event": "dca_executed", "coin": coin, "new_entry": price}))
                                except Exception:
                                    pass

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

            if updated_hist:
                _save_signal_history(hist)
            
            users = _load_users()
            users_updated = False
            for uid_str, u in users.items():
                saved = u.get("saved_signals", [])
                user_changed = False
                for s in saved:
                    if s.get("status") == "active":
                        price = await get_simple_price(s["coin"])
                        if not price: continue
                        filled = (price <= s["entry"]) if s.get("is_long") else (price >= s["entry"])
                        if filled:
                            s["status"] = "in_trade"
                            user_changed = True
                            if u.get("auto_trade_enabled"):
                                await execute_auto_trade(int(uid_str), s["coin"], s.get("is_long"), s["entry"], s.get("sl1"), s.get("tp1"))
                if user_changed:
                    users[uid_str]["saved_signals"] = saved
                    users_updated = True
            if users_updated:
                _save_users(users)

        except Exception as e:
            log.error(f"global_signal_monitor: {e}")
        next_timers["global_signals"] = time.time() + 300
        await asyncio.sleep(300)

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
                                        text = f"🚨 <b>Аномальный объем!</b>\nМонета: <b>{x['symbol'][:-4]}</b>\nОбъем за 1м: <b>${vol:,.0f}</b> (x10 к предыдущей)"
                                        await broadcast({"kind": "text", "text": text, "markup": None})
        except Exception as e:
            pass
        next_timers["anomaly"] = time.time() + 120
        await asyncio.sleep(120)

# --- 20. АВТОТОРГОВЛЯ (CCXT) ---
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

# --- 21. БАЗОВЫЕ ХЕНДЛЕРЫ TELEGRAM ---
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    await m.answer(f"👋 Привет! Я IXCLY PRO v{VERSION}. Нажми /menu для начала работы.")

@dp.message(Command("menu"))
async def cmd_menu(m: types.Message):
    kb = get_coins_keyboard()
    await m.answer(f"👋 <b>IXCLY PRO v{VERSION}</b>\n\nГлавное меню:", reply_markup=kb, parse_mode="HTML")

def get_coins_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📰  Новости", callback_data="news_menu"),
         InlineKeyboardButton(text="🧐  Графики", callback_data="interesting_menu")],
        [InlineKeyboardButton(text="🔥  Последние сигналы", callback_data="last_signals"),
         InlineKeyboardButton(text="📊  Статистика", callback_data="global_stats")],
        [InlineKeyboardButton(text="⭐  Избранное", callback_data="sig:menu"),
         InlineKeyboardButton(text="💼  Мой Портфель", callback_data="portfolio")],
        [InlineKeyboardButton(text="📥  Экспорт CSV", callback_data="export_csv")],
        [InlineKeyboardButton(text="🔍  Поиск монеты", callback_data="search")],
    ])

@dp.callback_query(F.data == "portfolio")
async def cb_portfolio(cb: types.CallbackQuery):
    settings = get_user_risk_settings(cb.from_user.id)
    risk = calculate_portfolio_risk(cb.from_user.id)
    text = f"💼 <b>Ваш Портфель</b>\n\n💰 Депозит: <b>${settings['deposit']:.2f}</b>\n📉 Риск на 1 сделку: <b>{settings['risk_pct']}%</b>\n⚠️ Общий риск (открыт): <b>{risk}%</b> (Макс: {settings['max_open_risk']}%)"
    kb = [[InlineKeyboardButton(text="⚙️ Настроить", callback_data="setup_risk")],[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]
    await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.message(Command("set_risk"))
async def cmd_set_risk(m: types.Message):
    try:
        parts = m.text.split()
        dep = float(parts[1])
        rsk = float(parts[2])
        key = str(m.from_user.id)
        users = _load_users()
        users[key]["deposit"] = dep
        users[key]["risk_pct"] = rsk
        _save_users(users)
        await m.answer(f"✅ Настройки обновлены. Депозит: ${dep}, Риск: {rsk}%")
    except Exception:
        await m.answer("❌ Формат: <code>/set_risk 1000 1.5</code> (депозит и % риска)", parse_mode="HTML")

@dp.callback_query(F.data == "export_csv")
async def cb_export_csv(cb: types.CallbackQuery):
    await safe_answer(cb, "⏳ Формирую CSV...")
    try:
        csv_data = generate_csv_history()
        doc = BufferedInputFile(csv_data, filename="trade_history.csv")
        await bot.send_document(cb.from_user.id, doc, caption="📊 История ваших сигналов (CSV)")
    except Exception:
        await safe_answer(cb, "Ошибка экспорта", True)

@dp.message(Command("api"))
async def cmd_api_setup(m: types.Message):
    text = "🔐 <b>Настройка Автоторговли</b>\n\nОтправьте ваш API Key и Secret (Binance Futures) в формате:\n<code>api_key,secret_key</code>\n\n⚠️ <i>Обязательно включите IP Whitelist на Binance!</i>"
    await m.answer(text, parse_mode="HTML")

@dp.message(F.text.regexp(r"^[A-Za-z0-9]{64},[A-Za-z0-9]{64}$"))
async def handle_api_keys(m: types.Message):
    api_key, secret_key = m.text.split(",")
    if save_user_api_keys(m.from_user.id, api_key, secret_key):
        await m.answer("✅ API-ключи надежно зашифрованы и сохранены. Автоторговля активирована!")
    else:
        await m.answer("❌ Ошибка сохранения. Проверьте формат ключей.")

@dp.message(Command("webhook"))
async def cmd_webhook(m: types.Message):
    try:
        url = m.text.split(maxsplit=1)[1]
        save_user_webhook(m.from_user.id, url)
        await m.answer("✅ Вебхук установлен! Все сигналы будут дублироваться POST-запросом.")
    except Exception:
        await m.answer("❌ Формат: <code>/webhook https://your-server.com/api/signal</code>", parse_mode="HTML")

# --- 22. АДМИН-ПАНЕЛЬ ---
if admin_dp:
    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_cmd_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS: 
            await m.answer("⛔ Доступ только для админа."); return
        total, active, _, _ = user_counts()
        await m.answer(f"✅ <b>Админ-панель PRO v{VERSION}</b>\n👥 Юзеров: {total} (активных {active})", reply_markup=get_admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: 
            await safe_answer(cb, "Нет доступа", True); return
        act = cb.data.split(":")[1]

        if act == "toggle_news":
            bot_flags["news_enabled"] = not bot_flags["news_enabled"]
            await safe_answer(cb, f"Новости: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}")
            try: await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            except: pass
            return
            
        if act == "toggle_scan":
            bot_flags["alerts_enabled"] = not bot_flags["alerts_enabled"]
            await safe_answer(cb, f"Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}")
            try: await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            except: pass
            return

        await safe_answer(cb, "⏳ Выполняю…")

        if act == "stats":
            total, active, _, _ = user_counts(); up = int(time.time() - stats["started"])
            text = f"📊 <b>Статистика</b>\nЮзеров: {total} (активных {active})\nСигналов: {stats['signals']}\nАптайм: {up // 3600}ч {up % 3600 // 60}м"
            await cb.message.answer(text, parse_mode="HTML")
        elif act == "users":
            users = _load_users(); lines = [f"👥 Юзеры ({len(users)})\n"] + [f"{'✅' if u.get('active') else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or u.get('username') or '—')[:15])}" for uid, u in list(users.items())[-30:]]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        elif act == "news_now":
            text, img_url = await prepare_news(force=True)
            if not text: await cb.message.answer("Нет новостей."); return
            img = await download_bytes(img_url) if img_url else None
            sent, _, _, _ = await broadcast({"kind": "photo" if img else "text", "text": text, "data": img, "markup": None})
            await cb.message.answer(f"✅ Отправлено: {sent}")
        elif act == "scan_test":
            await cb.message.answer("🔍 Сканирую...")
            sigs = await scan_once(send=False)
            await cb.message.answer(f"Найдено: {len(sigs)} сигналов.")
        elif act == "broadcast_now":
            if last_signals_data.get("text"):
                sent, _, _, _ = await broadcast({"kind": "text", "text": last_signals_data["text"], "markup": last_signals_data.get("kb")})
                await cb.message.answer(f"Рассылка: {sent}")
            else: await cb.message.answer("Нет сигналов")
        elif act == "diag":
            text = f"🔧 <b>Диагностика</b>\nccxt: {'✅' if ccxt else '❌'}\nFernet: {'✅' if Fernet else '❌'}\n\n" + await run_ai_diag()
            await cb.message.answer(text, parse_mode="HTML")
        elif act == "backup":
            ok = await telegram_backup("вручную")
            await cb.message.answer("💾 Бэкап отправлен." if ok else "Пусто.")
        elif act == "restore":
            await cb.message.answer("Пришли файл bot_state.json.")
        elif act == "timers":
            now = time.time()
            def fmt_time(sec): return "выполняется" if sec <= 0 else f"через {int(sec//60)}м {int(sec%60)}с"
            await cb.message.answer(f"🤖 Сканер: {fmt_time(next_timers['scanner'] - now)}\n📈 Мониторинг: {fmt_time(next_timers['positions'] - now)}\n🚨 Аномалии: {fmt_time(next_timers['anomaly'] - now)}", parse_mode="HTML")

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        global current_signals_db
        if m.from_user.id not in ADMIN_IDS or not (m.document.file_name or "").endswith(".json"): return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            content = await admin_bot.download_file(f.file_path)
            data = json.loads(content.read().decode("utf-8"))
            if isinstance(data, dict):
                if "users" in data or "signal_history" in data:
                    with open(STATE_FILE, "w", encoding="utf-8") as out_f: json.dump(data, out_f, ensure_ascii=False, indent=2)
                    if "current_signals_db" in data: current_signals_db = data["current_signals_db"]
                load_active_chats()
                await m.answer(f"✅ Состояние восстановлено!")
        except Exception as e: await m.answer(f"Ошибка: {e}")

# --- 23. НОВОСТИ И ГРАФИКИ (ЗАГЛУШКИ) ---
async def prepare_news(force=False):
    # Заглушка. Сюда вставляется логика парсинга RSS/Feedparser
    return "ℹ️ Новостной модуль в режиме заглушки.", None

@dp.callback_query(F.data == "news_menu")
async def cb_news_menu(cb: types.CallbackQuery):
    text, _ = await prepare_news()
    kb = [[InlineKeyboardButton(text="🔄 Обновить", callback_data="news_refresh")],[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]
    await safe_edit(cb.message, text or "Нет новостей", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("news_refresh"))
async def cb_news_refresh(cb: types.CallbackQuery):
    await safe_answer(cb, "⏳ Обновляю...")
    text, _ = await prepare_news(force=True)
    kb = [[InlineKeyboardButton(text="🔄 Обновить", callback_data="news_refresh")],[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]
    await safe_edit(cb.message, text or "Нет новостей", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)

@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting_menu(cb: types.CallbackQuery):
    kb = [[InlineKeyboardButton(text="📈 График монеты", callback_data="chart:menu")],[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]
    await safe_edit(cb.message, "🧐 Выберите действие:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("chart:"))
async def cb_chart(cb: types.CallbackQuery):
    await safe_answer(cb, "⏳ Генерирую график...")
    # Заглушка для генерации графика через PIL/Matplotlib
    await safe_edit(cb.message, "🖼 График сгенерирован (заглушка).", reply_markup=get_coins_keyboard(), user_id=cb.from_user.id)

# --- 24. ПОИСК МОНЕТ ---
@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_answer(cb, "Введите тикер монеты (например, BTC, ETH, SOL):")
    await dp.current_state(user=cb.from_user.id).set_state("waiting_for_search")

@dp.message(F.text & F.text.length(1, 10).as_("text"))
async def search_handler(m: types.Message, state):
    current_state = await state.get_state()
    if current_state != "waiting_for_search":
        return
    coin = m.text.upper()
    await state.clear()
    await safe_delete(m)
    res, err = await analyze_coin(coin)
    if err: await m.answer(err, reply_markup=get_coins_keyboard()); return
    await send_analysis(m.chat.id, res, m.from_user.id)

# --- 25. ХЕНДЛЕРЫ СПИСКОВ СИГНАЛОВ ---
@dp.callback_query(F.data == "last_signals")
async def cb_last_signals(cb: types.CallbackQuery):
    if not last_signals_data.get("text"):
        await safe_answer(cb, "⏳ Сканирую базу...")
        await scan_once(send=False)
    await safe_edit(cb.message, last_signals_data["text"], reply_markup=last_signals_data["kb"], user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("sig_list:"))
async def cb_sig_list(cb: types.CallbackQuery):
    category = cb.data.split(":")[1]
    db = current_signals_db.get(category, [])
    if not db: 
        await safe_answer(cb, "В этой категории пока пусто."); return
    
    text = f"📋 <b>{category}</b>\n\n"
    for i, s in enumerate(db[:10], 1):
        arrow = "🟢 LONG" if s["is_long"] else "🔴 SHORT"
        text += f"{i}. <b>{s['coin']}</b> | {arrow}\nСила: {s['strength']}%\n\n"
    
    kb = [[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]
    await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data == "global_stats")
async def cb_global_stats(cb: types.CallbackQuery):
    hist = _load_signal_history()
    total = len(hist)
    active = sum(1 for x in hist if x.get("status") == "active")
    in_trade = sum(1 for x in hist if x.get("status") == "in_trade")
    expired = sum(1 for x in hist if x.get("status") == "expired")
    missed = sum(1 for x in hist if x.get("status") == "missed")
    wins = sum(1 for x in hist if x.get("status") == "win")
    losses = sum(1 for x in hist if x.get("status") == "loss")
    closed = wins + losses
    winrate = (wins / closed * 100) if closed > 0 else 0.0
    
    text = f"📊 <b>Статистика сигналов бота</b>\n\n📈 Всего: <b>{total}</b>\n🎯 Винрейт: <b>{winrate:.1f}%</b>\n✅ Побед: <b>{wins}</b> | ❌ Убытков: <b>{losses}</b>\n🚫 Упущено: <b>{missed}</b>"
    kb = [
        [InlineKeyboardButton(text="⏳ В работе", callback_data="stats_list:active:0"), InlineKeyboardButton(text="✅ Успешные", callback_data="stats_list:win:0")],
        [InlineKeyboardButton(text="🚫 Упущено", callback_data="stats_list:missed:0"), InlineKeyboardButton(text="❌ Убыточные", callback_data="stats_list:loss:0")],
        [InlineKeyboardButton(text="⏳ Не отработали", callback_data="stats_list:expired:0")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
    ]
    await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("stats_list:"))
async def cb_stats_list(cb: types.CallbackQuery):
    parts = cb.data.split(":"); list_type = parts[1]; page = int(parts[2]); per_page = 10
    hist = _load_signal_history()
    
    if list_type == "active": filtered = [x for x in hist if x.get("status") in ("active", "in_trade")]; title = "⏳ В работе"
    elif list_type == "win": filtered = [x for x in hist if x.get("status") == "win"]; title = "✅ Успешные"
    elif list_type == "missed": filtered = [x for x in hist if x.get("status") == "missed"]; title = "🚫 Упущенные"
    elif list_type == "loss": filtered = [x for x in hist if x.get("status") == "loss"]; title = "❌ Убыточные"
    else: filtered = [x for x in hist if x.get("status") == "expired"]; title = "⏳ По времени"
        
    seen = set(); unique = []
    for x in sorted(filtered, key=lambda k: k.get("ts", 0), reverse=True):
        key = f"{x['coin']}_{x['side']}"
        if key not in seen: seen.add(key); unique.append(x)
    filtered = unique
    
    total_items = len(filtered); total_pages = (total_items + per_page - 1) // per_page or 1
    page = max(0, min(page, total_pages - 1)); start = page * per_page
    text = f"{title}\nНайдено: <b>{total_items}</b> шт.\n\n"
    kb = []
    for x in filtered[start:start+per_page]:
        side_emoji = "🟢" if x["side"] == "LONG" else "🔴"
        text += f"{side_emoji} <b>{x['coin']}</b>\n"
        kb.append([InlineKeyboardButton(text=f"{side_emoji} {x['coin']}", callback_data=f"c:{x['coin']}")])
        
    nav = []
    if page > 0: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"stats_list:{list_type}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"stats_list:{list_type}:{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="⬅️ К статистике", callback_data="global_stats")])
    
    await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("c:"))
async def cb_coin(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[1], cb.from_user.id
    if uid in busy_users: await safe_answer(cb, "Уже обрабатываю…"); return
    busy_users.add(uid)
    try:
        await safe_answer(cb, "⏳ Анализирую...")
        res, err = await analyze_coin(coin)
        if err: await safe_edit(cb.message, err, reply_markup=get_coins_keyboard(), user_id=uid); return
        await safe_delete(cb.message); await send_analysis(cb.message.chat.id, res, uid)
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally: busy_users.discard(uid)

# --- 26. ГЛАВНЫЙ ЦИКЛ ЗАПУСКА ---
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
        try: me = await bot.get_me(); runtime["main_bot"] = f"@{me.username}"
        except Exception as e: log.error(f"get_me основного: {e}")
        if admin_bot:
            try: me = await admin_bot.get_me(); runtime["admin_bot"] = f"@{me.username}"
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
