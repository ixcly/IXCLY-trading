# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v2.10.18
Полная версия без сокращений

- Точные сигналы: Среднесрок / Быстрый
- Визуальная шкала силы 🟩🟥
- Короткие читаемые новости + картинка
- Одна общая оценка влияния мировых новостей
- Чистка временных меню
- Мониторинг открытых позиций
- Полная админ-панель
- MongoDB + файловый фоллбэк
"""

import asyncio
import html
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from threading import Thread
from urllib.parse import quote

import aiohttp
import feedparser
import numpy as np
import pandas as pd
import pymongo
from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFont

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
    force=True
)
log = logging.getLogger("ixcly")

# ============================================================================
# ВЕБ-СЕРВЕР
# ============================================================================
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

@app.route("/health")
def health():
    return "ok"

def _rss_mb():
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    except Exception:
        return None

@app.route("/status")
def status_page():
    return jsonify({
        "version": VERSION,
        "uptime_s": int(time.time() - stats["started"]),
        "recipients": len(active_chats),
        "mongo_connected": users_col is not None,
        "market_bias": market_bias,
    })

def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 8080))
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"Flask error: {e}")

_web_started = False

def keep_alive():
    global _web_started
    if _web_started:
        return
    _web_started = True
    Thread(target=_run_web, daemon=True).start()

# ============================================================================
# НАСТРОЙКИ
# ============================================================================
def clean_key(key):
    if not key:
        return None
    return str(key).strip().strip('"').strip("'")

BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
MONGO_URI = clean_key(os.getenv("MONGO_URI"))

ADMIN_IDS = set()
for _x in re.split(r"[,\s]+", ADMIN_ID or ""):
    if _x.lstrip("-").isdigit():
        ADMIN_IDS.add(int(_x))

if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")

VERSION = "2.10.18"
runtime = {"main_bot": "?"}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
http: aiohttp.ClientSession = None

TFS = ["15m", "1h", "4h", "1d"]
TF_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
MIN_CANDLES = 30

POPULAR_COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "TON", "SHIB", "DOT", "LINK",
    "BCH", "NEAR", "POL", "LTC", "ICP", "UNI", "APT", "STX", "FIL", "IMX", "HBAR", "ATOM",
    "INJ", "OP", "ARB", "VET", "MKR", "SUI", "RNDR", "GRT", "THETA", "S", "SAND", "MANA",
    "GALA", "PEPE", "WIF", "BONK", "ORDI", "JUP", "TIA", "SEI", "TRX", "XLM", "ETC", "XMR",
    "AAVE", "ALGO", "EOS", "NEO", "IOTA", "ZEC", "DASH", "COMP", "SNX", "CRV", "LDO", "ENS",
    "1INCH", "CAKE", "RUNE", "KSM", "FLOW", "AXS", "CHZ", "BAT", "ENJ", "ZIL", "FTM", "KAVA",
    "EGLD", "ROSE", "AR", "FET", "WLD", "APE", "GMT", "CFX", "PYTH", "JTO", "ONDO", "FLOKI",
    "KAS", "CRO", "MNT", "TAO", "STRK", "DYDX", "GMX", "PENDLE", "SUSHI", "YFI", "BAL", "ZRX",
    "KNC", "UMA", "BAND", "API3", "SSV", "LPT", "RAY", "ORCA", "ILV", "MAGIC", "PRIME", "BEAM",
    "YGG", "ALICE", "TLM", "DUSK", "HIGH", "CHR", "C98", "AI", "NMR", "ARKM", "IO", "ATH",
    "BOME", "MEME", "SATS", "RATS", "DCR", "DGB", "XVG", "SC", "RVN", "WAVES", "ZEN", "BLAST",
    "AEVO", "ETHFI", "ENA", "W", "EIGEN", "MOVE", "PNUT", "GOAT", "ACT", "MOODENG", "TURBO",
    "MOG", "NEIRO", "DOGS", "NOT", "PIXEL", "PORTAL", "DYM", "ALT", "MANTA", "ZETA", "TNSR",
    "DRIFT", "KMNO", "WEN", "MYRO", "POPCAT", "GIGA", "MEW", "SLERF", "LUNC", "LUNA", "USTC"
]

SYMBOL_ALIASES = {
    "RNDR": ["RENDER", "RNDR"],
    "RENDER": ["RENDER", "RNDR"],
    "MATIC": ["POL", "MATIC"],
    "SATS": ["1000SATS", "SATS"],
    "RATS": ["1000RATS", "RATS"],
    "LUNC": ["1000LUNC", "LUNC"],
    "BONK": ["1000BONK", "BONK"],
    "PEPE": ["1000PEPE", "PEPE"],
    "FLOKI": ["1000FLOKI", "FLOKI"],
}

USERS_FILE = "users.json"
NEWS_HISTORY_FILE = "news_history.json"
SIGNAL_HISTORY_FILE = "signal_history.json"

active_chats = set()
busy_users = set()
result_cache = {}
last_news_link = None
news_history = []
last_signals_data = {"text": "", "kb": None, "ts": 0, "signals": []}
stats = {
    "signals": 0,
    "news_sent": 0,
    "broadcasts": 0,
    "alerts_sent": 0,
    "started": time.time(),
}
bot_flags = {
    "news_enabled": True,
    "alerts_enabled": True,
}

market_bias = {
    "bias": 0,
    "text": "Нейтральный фон",
    "ts": 0,
}

users_col = None

def init_mongo():
    global users_col
    if not MONGO_URI:
        log.warning("MONGO_URI не задан — используем файл users.json")
        return
    try:
        client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        db = client.get_database("ixcly_db")
        users_col = db["users"]
        client.server_info()
        log.info("✅ MongoDB подключена")
    except Exception as e:
        log.error(f"❌ Ошибка MongoDB: {e}")
        users_col = None

# ============================================================================
# ПОЛЬЗОВАТЕЛИ
# ============================================================================
def _load_users():
    if users_col is not None:
        try:
            return {
                str(u["_id"]): {k: v for k, v in u.items() if k != "_id"}
                for u in users_col.find()
            }
        except Exception as e:
            log.error(f"Mongo load error: {e}")
            return {}
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_users(users):
    if users_col is not None:
        try:
            for uid, data in users.items():
                users_col.update_one({"_id": str(uid)}, {"$set": data}, upsert=True)
        except Exception as e:
            log.error(f"Mongo save error: {e}")
        return
    try:
        tmp = USERS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        os.replace(tmp, USERS_FILE)
    except Exception as e:
        log.warning(f"Save users error: {e}")

def save_user_data(user_id, username, first_name):
    key = str(user_id)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if users_col is not None:
        try:
            users_col.update_one(
                {"_id": key},
                {
                    "$set": {
                        "username": username,
                        "first_name": first_name,
                        "active": True
                    },
                    "$setOnInsert": {"joined_at": now_str}
                },
                upsert=True
            )
            return True
        except Exception as e:
            log.error(f"Mongo save_user error: {e}")
            return False
    users = _load_users()
    if key in users:
        if users[key].get("active") is False:
            users[key]["active"] = True
            _save_users(users)
        return False
    users[key] = {
        "username": username,
        "first_name": first_name,
        "joined_at": now_str,
        "active": True
    }
    _save_users(users)
    return True

def get_user_saved_signals(user_id):
    users = _load_users()
    return users.get(str(user_id), {}).get("saved_signals", [])

def add_user_saved_signal(user_id, coin, text, photo_id=None):
    key = str(user_id)
    if users_col is not None:
        try:
            users_col.update_one({"_id": key}, {"$pull": {"saved_signals": {"coin": coin}}})
            users_col.update_one(
                {"_id": key},
                {"$push": {"saved_signals": {"coin": coin, "text": text, "photo_id": photo_id}}},
                upsert=True
            )
            return True
        except Exception as e:
            log.error(f"Mongo add signal error: {e}")
            return False
    users = _load_users()
    if key not in users:
        return False
    users[key].setdefault("saved_signals", [])
    users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
    users[key]["saved_signals"].append({
        "coin": coin,
        "text": text,
        "photo_id": photo_id
    })
    _save_users(users)
    return True

def del_user_saved_signal(user_id, coin):
    key = str(user_id)
    if users_col is not None:
        try:
            users_col.update_one({"_id": key}, {"$pull": {"saved_signals": {"coin": coin}}})
            return True
        except Exception as e:
            log.error(f"Mongo del signal error: {e}")
            return False
    users = _load_users()
    if key in users and "saved_signals" in users[key]:
        users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
        _save_users(users)
        return True
    return False

def get_user_open_positions(user_id):
    users = _load_users()
    return users.get(str(user_id), {}).get("open_positions", {})

def set_user_open_position(user_id, coin, data):
    key = str(user_id)
    if users_col is not None:
        try:
            if data is None:
                users_col.update_one({"_id": key}, {"$unset": {f"open_positions.{coin}": ""}})
            else:
                users_col.update_one(
                    {"_id": key},
                    {"$set": {f"open_positions.{coin}": data}},
                    upsert=True
                )
            return True
        except Exception as e:
            log.error(f"Mongo open_pos error: {e}")
            return False
    users = _load_users()
    if key not in users:
        users[key] = {}
    users[key].setdefault("open_positions", {})
    if data is None:
        users[key]["open_positions"].pop(coin, None)
    else:
        users[key]["open_positions"][coin] = data
    _save_users(users)
    return True

def get_open_position(user_id, coin):
    return get_user_open_positions(user_id).get(coin)

def set_users_active(ids, active):
    users = _load_users()
    changed = False
    for i in ids:
        k = str(i)
        if k in users and users[k].get("active", True) != active:
            users[k]["active"] = active
            changed = True
    if changed:
        _save_users(users)

def load_active_chats():
    active_chats.clear()
    for uid, u in _load_users().items():
        if isinstance(u, dict) and u.get("active", True) is not False:
            try:
                active_chats.add(int(uid))
            except ValueError:
                pass
    log.info(f"Активных получателей: {len(active_chats)}")

def user_counts():
    users = _load_users()
    today = datetime.now().date()
    active = new_today = new_week = 0
    for u in users.values():
        if not isinstance(u, dict):
            continue
        if u.get("active", True) is not False:
            active += 1
        try:
            d = datetime.strptime(u.get("joined_at", ""), "%Y-%m-%d %H:%M:%S").date()
            age = (today - d).days
            if age == 0:
                new_today += 1
            if age < 7:
                new_week += 1
        except Exception:
            pass
    return len(users), active, new_today, new_week

def _load_news_history():
    global news_history
    try:
        with open(NEWS_HISTORY_FILE, "r", encoding="utf-8") as f:
            news_history = json.load(f)
    except Exception:
        news_history = []

def _save_news_history():
    try:
        with open(NEWS_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(news_history[-80:], f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _load_signal_history():
    try:
        with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        now = time.time()
        return [x for x in data if now - x.get("ts", 0) < 24 * 3600]
    except Exception:
        return []

def _save_signal_history(data):
    try:
        with open(SIGNAL_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data[-50:], f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ============================================================================
# МИДЛВАРЬ
# ============================================================================
class UserTrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Update, data: dict):
        user = None
        chat_id = None
        if event.message:
            user = event.message.from_user
            chat_id = event.message.chat.id
        elif event.callback_query:
            user = event.callback_query.from_user
            if event.callback_query.message:
                chat_id = event.callback_query.message.chat.id
        if user and chat_id and chat_id not in active_chats:
            active_chats.add(chat_id)
            asyncio.create_task(
                asyncio.to_thread(save_user_data, user.id, user.username, user.first_name)
            )
        return await handler(event, data)

dp.update.middleware(UserTrackerMiddleware())

# ============================================================================
# КЛАВИАТУРЫ
# ============================================================================
def get_coins_keyboard(page=0):
    per_page = 12
    coins_page = POPULAR_COINS[page * per_page:(page + 1) * per_page]
    buttons = []
    for i in range(0, len(coins_page), 2):
        row = [InlineKeyboardButton(text=f"🪙 {coins_page[i]}", callback_data=f"c:{coins_page[i]}")]
        if i + 1 < len(coins_page):
            row.append(InlineKeyboardButton(text=f"🪙 {coins_page[i + 1]}", callback_data=f"c:{coins_page[i + 1]}"))
        buttons.append(row)
    total_pages = (len(POPULAR_COINS) + per_page - 1) // per_page
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"p:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page + 1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"p:{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🧐 Интересные", callback_data="interesting_menu")])
    buttons.append([InlineKeyboardButton(text="🔥 Последние сигналы", callback_data="last_signals")])
    buttons.append([InlineKeyboardButton(text="⭐ Избранное", callback_data="sig:menu")])
    buttons.append([InlineKeyboardButton(text="📊 Статистика 24ч", callback_data="show_stats")])
    buttons.append([InlineKeyboardButton(text="🔍 Поиск", callback_data="search")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def result_keyboard(coin):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"r:{coin}"),
        InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sig:quick_save:{coin}"),
        InlineKeyboardButton(text="🏠 Меню", callback_data="menu"),
    ]])

def get_saved_signals_menu(user_id):
    saved = get_user_saved_signals(user_id)
    buttons = []
    row = []
    for s in saved:
        row.append(InlineKeyboardButton(text=f"⭐ {s['coin']}", callback_data=f"sig:view:{s['coin']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="➕ Добавить", callback_data="sig:save_new")])
    if saved:
        buttons.append([InlineKeyboardButton(text="🗑 Удалить", callback_data="sig:del_menu")])
    buttons.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_delete_signals_menu(user_id):
    saved = get_user_saved_signals(user_id)
    buttons = []
    row = []
    for s in saved:
        row.append(InlineKeyboardButton(text=f"❌ {s['coin']}", callback_data=f"sig:del:{s['coin']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sig:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_save_coins_keyboard(page=0):
    per_page = 12
    coins_page = POPULAR_COINS[page * per_page:(page + 1) * per_page]
    buttons = []
    for i in range(0, len(coins_page), 2):
        row = [InlineKeyboardButton(text=coins_page[i], callback_data=f"sig:do_save:{coins_page[i]}")]
        if i + 1 < len(coins_page):
            row.append(InlineKeyboardButton(text=coins_page[i + 1], callback_data=f"sig:do_save:{coins_page[i + 1]}"))
        buttons.append(row)
    total_pages = (len(POPULAR_COINS) + per_page - 1) // per_page
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sig:save_page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sig:save_page:{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sig:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_saved_signal_view_keyboard(user_id, coin):
    pos = get_open_position(user_id, coin)
    buttons = [
        [InlineKeyboardButton(text="🔄 Обновить анализ", callback_data=f"c:{coin}")],
    ]
    if pos:
        buttons.append([InlineKeyboardButton(text="📈 Статус позиции", callback_data=f"pos:status:{coin}")])
        buttons.append([InlineKeyboardButton(text="❌ Закрыл сделку", callback_data=f"pos:close:{coin}")])
    else:
        buttons.append([InlineKeyboardButton(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sig:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard():
    news_status = "🟢 ВКЛ" if bot_flags["news_enabled"] else "🔴 ВЫКЛ"
    scan_status = "🟢 ВКЛ" if bot_flags["alerts_enabled"] else "🔴 ВЫКЛ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
            InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users"),
        ],
        [
            InlineKeyboardButton(text="📰 Новость сейчас", callback_data="admin:news_now"),
            InlineKeyboardButton(text=f"🔔 Новости: {news_status}", callback_data="admin:toggle_news"),
        ],
        [
            InlineKeyboardButton(text=f"🚨 Сканер: {scan_status}", callback_data="admin:toggle_scan"),
            InlineKeyboardButton(text="🔍 Скан-тест", callback_data="admin:scan_test"),
        ],
        [InlineKeyboardButton(text="⚡ Рассылка сразу", callback_data="admin:broadcast_now")],
        [
            InlineKeyboardButton(text="🔧 Диагностика", callback_data="admin:diag"),
            InlineKeyboardButton(text="💾 Бэкап", callback_data="admin:backup"),
        ],
        [InlineKeyboardButton(text="📥 Восстановить базу", callback_data="admin:restore")],
    ])

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ
# ============================================================================
def fmt(p):
    p = float(p)
    a = abs(p)
    if a >= 1000:
        return f"{p:,.2f}".replace(",", " ")
    if a >= 1:
        return f"{p:.4f}"
    if a >= 0.01:
        return f"{p:.5f}"
    return f"{p:.8f}"

def make_strength_bar(strength: float, length: int = 10) -> str:
    filled = max(0, min(length, int(round(strength / 100 * length))))
    return "🟩" * filled + "🟥" * (length - filled)

async def first_success(coros, timeout=12):
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        done, pending = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        for d in done:
            try:
                res = d.result()
                if res is not None:
                    return res
            except Exception:
                continue
        return None
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def http_json(url, params=None, headers=None, timeout=8):
    async with http.get(
        url,
        params=params,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.json(content_type=None)

def normalize_ticker(text):
    t = text.upper().strip()
    t = re.sub(r"[\s$/\-_]", "", t)
    if t.endswith("USDT") and len(t) > 4:
        t = t[:-4]
    return t

async def safe_delete(message):
    try:
        await message.delete()
    except Exception:
        pass

# ============================================================================
# ИНДИКАТОРЫ
# ============================================================================
def add_indicators(df):
    c = df["close"]
    df["ema20"] = c.ewm(span=20, adjust=False).mean()
    df["ema50"] = c.ewm(span=50, adjust=False).mean()
    df["ema200"] = c.ewm(span=200, adjust=False).mean()
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    rsi[(avg_loss == 0) & avg_gain.notna()] = 100
    df["rsi"] = rsi.fillna(50)
    prev = c.shift()
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean().fillna(tr.mean())
    macd_line = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = macd_line - signal_line
    return df

def calc_probability(df):
    ema20 = float(df["ema20"].iloc[-1])
    ema50 = float(df["ema50"].iloc[-1]) or 1e-12
    rsi = float(df["rsi"].iloc[-1])
    macd_hist = float(df["macd_hist"].iloc[-1])
    score = 0.0
    diff_pct = abs(ema20 - ema50) / ema50 * 100
    if ema20 > ema50:
        score += 22 + min(18, diff_pct * 12)
    else:
        score -= 22 + min(18, diff_pct * 12)
    if rsi > 55:
        score += min(22, (rsi - 55) * 1.15)
    elif rsi < 45:
        score -= min(22, (45 - rsi) * 1.15)
    score += 22 if macd_hist > 0 else -22
    long_prob = max(8.0, min(92.0, 50 + score / 2))
    return round(long_prob, 1), round(100 - long_prob, 1)

def rows_to_df(rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    df = df.drop_duplicates("ts").sort_values("ts")
    df = df[df["close"] > 0].reset_index(drop=True)
    if len(df) < MIN_CANDLES:
        raise ValueError("мало свечей")
    return add_indicators(df)

def build_tf_data(rows_by_tf):
    out = {}
    for tf, rows in rows_by_tf.items():
        df = rows_to_df(rows)
        last = df.iloc[-1]
        long_prob, _ = calc_probability(df)
        out[tf] = {
            "df": df,
            "price": float(last["close"]),
            "ema20": float(last["ema20"]),
            "ema50": float(last["ema50"]),
            "ema200": float(last["ema200"]),
            "rsi": float(last["rsi"]),
            "atr": float(last["atr"]),
            "macd_hist": float(last["macd_hist"]),
            "recent_low": float(df["low"].tail(28).min()),
            "recent_high": float(df["high"].tail(28).max()),
            "long_prob": long_prob,
        }
    return out

# ============================================================================
# ИСТОЧНИКИ ДАННЫХ
# ============================================================================
def _p_binance(j):
    if not isinstance(j, list):
        raise ValueError("bad binance response")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]

def _p_okx(j):
    if j.get("code") != "0" or not j.get("data"):
        raise ValueError("bad okx")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["data"])]

def _p_bybit(j):
    if j.get("retCode") != 0 or not j.get("result", {}).get("list"):
        raise ValueError("bad bybit")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["result"]["list"])]

EXCHANGES = [
    {
        "name": "BINANCE",
        "url": "https://data-api.binance.vision/api/v3/klines",
        "params": lambda c, tf: {"symbol": f"{c}USDT", "interval": tf, "limit": 200},
        "parse": _p_binance,
    },
    {
        "name": "OKX",
        "url": "https://www.okx.com/api/v5/market/candles",
        "params": lambda c, tf: {
            "instId": f"{c}-USDT",
            "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf],
            "limit": "200"
        },
        "parse": _p_okx,
    },
    {
        "name": "BYBIT",
        "url": "https://api.bybit.com/v5/market/kline",
        "params": lambda c, tf: {
            "category": "spot",
            "symbol": f"{c}USDT",
            "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf],
            "limit": 200
        },
        "parse": _p_bybit,
    },
]

async def _fetch_klines(ex, coin, tf):
    j = await http_json(ex["url"], params=ex["params"](coin, tf), timeout=9)
    rows = ex["parse"](j)
    if len(rows) < MIN_CANDLES:
        raise ValueError("мало свечей")
    return rows

async def fetch_from_exchange(ex, coin):
    results = await asyncio.gather(
        *[_fetch_klines(ex, coin, tf) for tf in TFS],
        return_exceptions=True
    )
    for r in results:
        if isinstance(r, Exception):
            return None
    return await asyncio.to_thread(build_tf_data, dict(zip(TFS, results))), ex["name"]

async def get_market_data(coin):
    cands = SYMBOL_ALIASES.get(coin, [coin])
    for cand in cands:
        for ex in EXCHANGES:
            try:
                res = await asyncio.wait_for(fetch_from_exchange(ex, cand), timeout=12)
                if res:
                    return res
            except Exception:
                continue
    raise LookupError(coin)

async def get_market_data_light(coin):
    return await get_market_data(coin)

async def get_simple_price(coin):
    try:
        j = await http_json(
            "https://data-api.binance.vision/api/v3/ticker/price",
            params={"symbol": f"{coin}USDT"},
            timeout=5
        )
        return float(j["price"])
    except Exception:
        try:
            j = await http_json(
                "https://api.bybit.com/v5/market/tickers",
                params={"category": "spot", "symbol": f"{coin}USDT"},
                timeout=5
            )
            return float(j["result"]["list"][0]["lastPrice"])
        except Exception:
            return None

# ============================================================================
# СТРОГИЙ ТЕХ. ФИЛЬТР + УРОВНИ
# ============================================================================
def tech_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data:
        return None
    d4 = tf_data["4h"]
    d1 = tf_data["1d"]
    p4 = d4["long_prob"]
    p1 = d1["long_prob"]
    is_long_4 = p4 >= 58
    is_long_1 = p1 >= 55
    is_short_4 = p4 <= 42
    is_short_1 = p1 <= 45
    if is_long_4 and is_long_1:
        is_long = True
        strength = (p4 + p1) / 2
    elif is_short_4 and is_short_1:
        is_long = False
        strength = ((100 - p4) + (100 - p1)) / 2
    else:
        return None
    if strength < 63:
        return None
    p15 = tf_data.get("15m", {}).get("long_prob", 50)
    p1h = tf_data.get("1h", {}).get("long_prob", 50)
    fast = False
    if is_long and p15 >= 60 and p1h >= 57:
        fast = True
    if (not is_long) and p15 <= 40 and p1h <= 43:
        fast = True
    signal_type = "Быстрый сигнал" if fast else "Среднесрок"
    return {
        "ok": True,
        "is_long": is_long,
        "strength": round(strength, 1),
        "type": signal_type,
        "tf": "4h",
        "prob": strength,
    }

def calc_levels(info, is_long):
    price = info["price"]
    atr = info["atr"] if info["atr"] and info["atr"] > 0 else price * 0.012
    ema20 = info["ema20"]
    ema50 = info["ema50"]
    recent_low = info["recent_low"]
    recent_high = info["recent_high"]
    if is_long:
        pullback = max(ema20, ema50)
        entry = min(price, pullback)
        if entry > price - 0.15 * atr:
            entry = price - 0.35 * atr
        sl1 = recent_low - 0.4 * atr
        sl2 = sl1 - 0.7 * atr
        risk = max(entry - sl1, 0.6 * atr)
        tp1 = entry + risk * 1.4
        tp2 = entry + risk * 2.3
        tp3 = entry + risk * 3.5
    else:
        pullback = min(ema20, ema50)
        entry = max(price, pullback)
        if entry < price + 0.15 * atr:
            entry = price + 0.35 * atr
        sl1 = recent_high + 0.4 * atr
        sl2 = sl1 + 0.7 * atr
        risk = max(sl1 - entry, 0.6 * atr)
        tp1 = entry - risk * 1.4
        tp2 = entry - risk * 2.3
        tp3 = entry - risk * 3.5
    return entry, [tp1, tp2, tp3], [sl1, sl2]

# ============================================================================
# НОВОСТИ + ОДНА ОБЩАЯ ОЦЕНКА
# ============================================================================
NEWS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]
WORLD_FEEDS = [
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
]

_news_cache = {"ts": 0.0, "entries": []}
_world_cache = {"ts": 0.0, "titles": []}

async def _load_feed(url):
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return []
            text = await r.text()
        feed = await asyncio.to_thread(feedparser.parse, text)
        return list(feed.entries)
    except Exception:
        return []

async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300:
        return _news_cache["entries"]
    for url in NEWS_FEEDS:
        entries = await _load_feed(url)
        if entries:
            _news_cache["ts"] = time.time()
            _news_cache["entries"] = entries[:8]
            return _news_cache["entries"]
    return _news_cache["entries"]

async def get_world_titles(n=6):
    if _world_cache["titles"] and time.time() - _world_cache["ts"] < 300:
        return _world_cache["titles"][:n]
    titles = []
    for url in WORLD_FEEDS:
        entries = await _load_feed(url)
        titles.extend([e.title for e in entries[:4]])
    if titles:
        _world_cache["ts"] = time.time()
        _world_cache["titles"] = titles
    return titles[:n]

def extract_image(entry):
    if hasattr(entry, "media_content") and entry.media_content:
        return entry.media_content[0].get("url")
    if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
        return entry.media_thumbnail[0].get("url")
    if hasattr(entry, "links"):
        for link in entry.links:
            if link.get("type", "").startswith("image"):
                return link.get("href")
    if hasattr(entry, "summary"):
        m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', entry.summary or "")
        if m:
            return m.group(1)
    return None

async def update_market_bias():
    global market_bias
    try:
        crypto_titles = [e.title for e in await get_news_entries()]
        world_titles = await get_world_titles(7)
        prompt = f"""Ты макро-аналитик. Дай ОДНУ короткую оценку влияния новостей на крипторынок на 1-3 дня.

МИРОВЫЕ НОВОСТИ:
{chr(10).join('- ' + t for t in world_titles) or '- нет данных'}

КРИПТО-НОВОСТИ:
{chr(10).join('- ' + t for t in crypto_titles) or '- нет данных'}

Ответь СТРОГО:
BIAS: BULLISH
REASON: короткое объяснение на русском (1-2 предложения)

или BIAS: BEARISH
или BIAS: NEUTRAL
"""
        text, engine = await ask_ai_named(prompt, timeout=18)
        bias = 0
        reason = "Нейтральный фон"
        if text:
            upper = text.upper()
            if "BULLISH" in upper:
                bias = 1
            elif "BEARISH" in upper:
                bias = -1
            m = re.search(r"REASON:\s*(.+)", text, re.IGNORECASE | re.DOTALL)
            if m:
                reason = m.group(1).strip()[:220]
        market_bias = {
            "bias": bias,
            "text": reason,
            "ts": time.time(),
            "engine": engine
        }
        log.info(f"Market bias → {bias} | {reason}")
    except Exception as e:
        log.warning(f"Ошибка обновления market_bias: {e}")

# ============================================================================
# ИИ
# ============================================================================
async def _ask_gemini(prompt, json_mode=False):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.15}
    }
    if json_mode:
        payload["generationConfig"]["responseMimeType"] = "application/json"
    for key in GEMINI_API_KEYS:
        try:
            async with http.post(
                url,
                json=payload,
                headers={"x-goog-api-key": key},
                timeout=aiohttp.ClientTimeout(total=18)
            ) as r:
                j = await r.json(content_type=None)
                if r.status == 200:
                    return j["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            continue
    return None

async def _ask_openai_compat(base_url, api_key, model, prompt, json_mode=False):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.15,
        "max_tokens": 1200
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    try:
        async with http.post(
            f"{base_url}/chat/completions",
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as r:
            j = await r.json(content_type=None)
            if r.status == 200 and "choices" in j:
                return j["choices"][0]["message"]["content"]
    except Exception:
        pass
    return None

async def ask_ai_named(prompt, json_mode=False, timeout=16):
    tasks = []
    if GEMINI_API_KEYS:
        tasks.append(("Gemini", lambda: _ask_gemini(prompt, json_mode)))
    if GROQ_API_KEY:
        tasks.append(("Groq", lambda: _ask_openai_compat(
            "https://api.groq.com/openai/v1", GROQ_API_KEY, "llama-3.1-8b-instant", prompt, json_mode
        )))
    if OPENROUTER_API_KEY:
        tasks.append(("OpenRouter", lambda: _ask_openai_compat(
            "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, "meta-llama/llama-3.3-70b-instruct:free", prompt, json_mode
        )))
    if not tasks:
        return None, None
    async def wrap(name, fn):
        try:
            text = await asyncio.wait_for(fn(), timeout=timeout)
            if text and len(text.strip()) > 25:
                return text, name
        except Exception:
            pass
        return None
    res = await first_success([wrap(n, f) for n, f in tasks], timeout=timeout + 3)
    return res if res else (None, None)

def has_russian(text, min_letters=25):
    return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= min_letters

def ai_to_html(text):
    text = html.escape((text or "").strip())
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    return text[:3200]

# ============================================================================
# СКАНЕР
# ============================================================================
SCAN_INTERVAL = 17 * 60

async def scan_once(send=True):
    global last_signals_data
    t0 = time.time()
    if time.time() - market_bias["ts"] > 12 * 60:
        await update_market_bias()
    bias = market_bias["bias"]
    bias_text = market_bias["text"]
    sem = asyncio.Semaphore(3)
    candidates = []

    async def check_one(coin):
        async with sem:
            try:
                tf_data, source = await get_market_data_light(coin)
                tech = tech_check(tf_data)
                if not tech or not tech["ok"]:
                    return
                if bias == 1 and not tech["is_long"]:
                    return
                if bias == -1 and tech["is_long"]:
                    return
                candidates.append({
                    "coin": coin,
                    "source": source,
                    "tech": tech,
                    "tf_data": tf_data
                })
            except Exception:
                pass

    await asyncio.gather(*[check_one(c) for c in POPULAR_COINS])
    candidates.sort(key=lambda x: x["tech"]["strength"], reverse=True)
    candidates = candidates[:5]

    signals = []
    hist = _load_signal_history()

    for cand in candidates:
        coin = cand["coin"]
        tech = cand["tech"]
        tf_data = cand["tf_data"]
        info = tf_data["4h"]
        is_long = tech["is_long"]
        entry, tps, sls = calc_levels(info, is_long)
        side_emoji = "🟢" if is_long else "🔴"
        side = "LONG" if is_long else "SHORT"
        bar = make_strength_bar(tech["strength"])
        text = (
            f"{side_emoji} <b>{side}  ·  {coin}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 Тип: <b>{tech['type']}</b>\n"
            f"💪 Сила: <b>{tech['strength']}%</b>\n"
            f"{bar}\n\n"
            f"💰 Цена: <code>{fmt(info['price'])}</code>\n"
            f"📥 Вход: <code>{fmt(entry)}</code>\n\n"
            f"🎯 <b>Тейки</b>\n"
            f"   TP1  <code>{fmt(tps[0])}</code>\n"
            f"   TP2  <code>{fmt(tps[1])}</code>\n"
            f"   TP3  <code>{fmt(tps[2])}</code>\n\n"
            f"🛡 <b>Стопы</b>\n"
            f"   SL1  <code>{fmt(sls[0])}</code>\n"
            f"   SL2  <code>{fmt(sls[1])}</code>\n\n"
            f"🌍 Фон: {html.escape(bias_text)}"
        )
        try:
            img_bytes = await asyncio.to_thread(
                draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls
            )
        except Exception:
            img_bytes = None
        signals.append({
            "coin": coin,
            "text": text,
            "img": img_bytes,
            "is_long": is_long,
            "entry": entry,
            "tps": tps,
            "sls": sls,
            "type": tech["type"],
            "strength": tech["strength"],
        })
        hist.append({
            "ts": time.time(),
            "coin": coin,
            "side": side,
            "type": tech["type"],
            "strength": tech["strength"],
            "entry": entry,
            "text": text,
        })

    _save_signal_history(hist)

    if signals and send:
        lines = [f"🔥 <b>Сигналы сканера</b> · {datetime.now().strftime('%H:%M')}\n"]
        kb_rows = []
        for s in signals:
            emoji = "🟢" if s["is_long"] else "🔴"
            lines.append(f"{emoji} <b>{s['coin']}</b> · {s['type']} · {s['strength']}%")
            kb_rows.append([
                InlineKeyboardButton(
                    text=f"{emoji} {s['coin']}",
                    callback_data=f"c:{s['coin']}"
                )
            ])
        lines.append(f"\n🌍 {html.escape(bias_text)}")
        final_text = "\n".join(lines)
        kb_rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu")])
        markup = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        last_signals_data = {
            "text": final_text,
            "kb": markup,
            "ts": time.time(),
            "signals": signals
        }
        sent, failed, _, _ = await broadcast({
            "kind": "text",
            "text": final_text,
            "markup": markup
        })
        stats["signals"] += len(signals)
        stats["alerts_sent"] += sent
        log.info(f"Сканер: {len(signals)} сигналов, отправлено {sent}, время {time.time() - t0:.1f}с")
    return signals

async def scanner_loop():
    await asyncio.sleep(25)
    while True:
        try:
            if bot_flags.get("alerts_enabled", True):
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner_loop error: {e}", exc_info=True)
        await asyncio.sleep(SCAN_INTERVAL)

# ============================================================================
# ГРАФИК
# ============================================================================
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    df = tf_data[main_tf]["df"].tail(55).reset_index(drop=True)
    width, height = 820, 440
    img = Image.new("RGB", (width, height), (17, 17, 23))
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
        font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 15)
    except Exception:
        font = ImageFont.load_default()
        font_b = font
    side = "LONG" if is_long else "SHORT"
    color = (0, 200, 90) if is_long else (220, 55, 55)
    draw.text((18, 11), f"{coin}   {main_tf}   {side}", fill=color, font=font_b)
    draw.text((18, 30), f"Entry {fmt(entry)}", fill=(190, 190, 190), font=font)
    pad_l, pad_r, pad_t, pad_b = 58, 18, 52, 32
    chart_w = width - pad_l - pad_r
    chart_h = height - pad_t - pad_b
    highs = df["high"].values
    lows = df["low"].values
    opens = df["open"].values
    closes = df["close"].values
    min_p = min(lows.min(), entry, min(sls), min(tps)) * 0.997
    max_p = max(highs.max(), entry, max(sls), max(tps)) * 1.003
    price_range = max_p - min_p or 1.0
    def y(p):
        return pad_t + chart_h - (p - min_p) / price_range * chart_h
    for i in range(5):
        py = pad_t + i * chart_h / 4
        draw.line([(pad_l, py), (width - pad_r, py)], fill=(38, 38, 48), width=1)
    n = len(df)
    candle_w = max(3, int(chart_w / n) - 1)
    for i in range(n):
        x = pad_l + i * (chart_w / n) + candle_w // 2
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        col = (0, 175, 80) if c >= o else (200, 50, 50)
        draw.line([(x, y(h)), (x, y(l))], fill=col, width=1)
        top = min(y(o), y(c))
        bot = max(y(o), y(c))
        draw.rectangle([x - candle_w // 2, top, x + candle_w // 2, bot], fill=col)
    def draw_level(price, label, col, dashed=False):
        yy = y(price)
        if dashed:
            for xx in range(pad_l, width - pad_r, 8):
                draw.line([(xx, yy), (xx + 4, yy)], fill=col, width=1)
        else:
            draw.line([(pad_l, yy), (width - pad_r, yy)], fill=col, width=2)
        draw.text((pad_l + 4, yy - 13), f"{label} {fmt(price)}", fill=col, font=font)
    draw_level(entry, "Entry", (255, 195, 0))
    for i, tp in enumerate(tps):
        draw_level(tp, f"TP{i+1}", (0, 195, 95), dashed=True)
    for i, sl in enumerate(sls):
        draw_level(sl, f"SL{i+1}", (220, 55, 55), dashed=True)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()

# ============================================================================
# BROADCAST
# ============================================================================
async def broadcast(payload, only_ids=None):
    targets = only_ids if only_ids is not None else list(active_chats)
    sent = failed = removed = 0
    removed_ids = []
    for chat_id in targets:
        try:
            if payload.get("kind") == "photo" and payload.get("data"):
                photo = BufferedInputFile(
                    payload["data"],
                    filename=payload.get("filename", "chart.png")
                )
                caption = payload.get("text", "")
                if len(caption) <= 1024:
                    await bot.send_photo(
                        chat_id, photo,
                        caption=caption,
                        parse_mode="HTML",
                        reply_markup=payload.get("markup")
                    )
                else:
                    await bot.send_photo(chat_id, photo)
                    await bot.send_message(
                        chat_id, caption[:4000],
                        parse_mode="HTML",
                        reply_markup=payload.get("markup"),
                        disable_web_page_preview=True
                    )
            else:
                await bot.send_message(
                    chat_id,
                    payload.get("text", "")[:4000],
                    parse_mode="HTML",
                    reply_markup=payload.get("markup"),
                    disable_web_page_preview=True
                )
            sent += 1
            await asyncio.sleep(0.045)
        except (TelegramForbiddenError, TelegramNotFound):
            removed += 1
            removed_ids.append(chat_id)
            active_chats.discard(chat_id)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                if payload.get("kind") == "photo" and payload.get("data"):
                    photo = BufferedInputFile(payload["data"], filename="chart.png")
                    await bot.send_photo(chat_id, photo, caption=payload.get("text", "")[:1024], parse_mode="HTML")
                else:
                    await bot.send_message(chat_id, payload.get("text", "")[:4000], parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
    if removed_ids:
        set_users_active(removed_ids, False)
    stats["broadcasts"] += 1
    return sent, failed, removed, removed_ids

# ============================================================================
# АНАЛИЗ ОДНОЙ МОНЕТЫ
# ============================================================================
async def analyze_coin(coin, user_id=None):
    try:
        tf_data, source = await get_market_data(coin)
    except LookupError:
        return None, f"⚠️ Монета <b>{coin}</b> не найдена на биржах."
    except Exception as e:
        return None, f"⚠️ Ошибка получения данных: {html.escape(str(e))}"
    tech = tech_check(tf_data)
    if tech and tech["ok"]:
        is_long = tech["is_long"]
        strength = tech["strength"]
        sig_type = tech["type"]
    else:
        d4 = tf_data["4h"]
        is_long = d4["long_prob"] >= 50
        strength = max(d4["long_prob"], 100 - d4["long_prob"])
        sig_type = "Среднесрок"
    info = tf_data["4h"]
    entry, tps, sls = calc_levels(info, is_long)
    side_emoji = "🟢" if is_long else "🔴"
    side = "LONG" if is_long else "SHORT"
    bar = make_strength_bar(strength)
    bias_text = market_bias.get("text", "Нейтральный фон")
    text = (
        f"{side_emoji} <b>{side}  ·  {coin}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Тип: <b>{sig_type}</b>\n"
        f"💪 Сила: <b>{strength}%</b>\n"
        f"{bar}\n\n"
        f"💰 Цена: <code>{fmt(info['price'])}</code>\n"
        f"📥 Вход: <code>{fmt(entry)}</code>\n\n"
        f"🎯 <b>Тейки</b>\n"
        f"   TP1  <code>{fmt(tps[0])}</code>\n"
        f"   TP2  <code>{fmt(tps[1])}</code>\n"
        f"   TP3  <code>{fmt(tps[2])}</code>\n\n"
        f"🛡 <b>Стопы</b>\n"
        f"   SL1  <code>{fmt(sls[0])}</code>\n"
        f"   SL2  <code>{fmt(sls[1])}</code>\n\n"
        f"🌍 Фон: {html.escape(bias_text)}"
    )
    try:
        img = await asyncio.to_thread(
            draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls
        )
    except Exception:
        img = None
    return {
        "text": text,
        "img": img,
        "coin": coin,
        "entry": entry,
        "tps": tps,
        "sls": sls,
        "is_long": is_long,
        "side": side,
        "price": info["price"],
        "type": sig_type,
        "strength": strength,
    }, None

# ============================================================================
# МОНИТОРИНГ ПОЗИЦИЙ
# ============================================================================
async def position_monitor_loop():
    await asyncio.sleep(55)
    while True:
        try:
            users = _load_users()
            for uid_str, u in users.items():
                positions = u.get("open_positions") or {}
                if not positions:
                    continue
                for coin, pos in list(positions.items()):
                    try:
                        await check_and_notify_position(int(uid_str), coin, pos)
                    except Exception as e:
                        log.warning(f"pos monitor {uid_str}/{coin}: {e}")
                    await asyncio.sleep(0.3)
        except Exception as e:
            log.error(f"position_monitor_loop: {e}")
        await asyncio.sleep(340)

async def check_and_notify_position(user_id, coin, pos):
    if pos.get("notified_close"):
        return
    price = await get_simple_price(coin)
    if not price:
        return
    side = pos["side"]
    entry = pos["entry_price"]
    tp1 = pos["tp1"]
    tp2 = pos["tp2"]
    tp3 = pos["tp3"]
    sl1 = pos["sl1"]
    if side == "LONG":
        pnl_pct = (price - entry) / entry * 100
        hit_tp1 = price >= tp1
        hit_tp2 = price >= tp2
        hit_tp3 = price >= tp3
        hit_sl = price <= sl1
    else:
        pnl_pct = (entry - price) / entry * 100
        hit_tp1 = price <= tp1
        hit_tp2 = price <= tp2
        hit_tp3 = price <= tp3
        hit_sl = price >= sl1
    reason = None
    urgency = "info"
    if hit_sl:
        reason = f"🛑 Сработал стоп-лосс (SL1 = {fmt(sl1)}).\nРекомендую закрыть."
        urgency = "danger"
    elif hit_tp3:
        reason = f"✅ Достигнут TP3 ({fmt(tp3)}).\nЦель полностью отработана."
        urgency = "success"
    elif hit_tp2 and not pos.get("notified_tp"):
        reason = f"✅ Достигнут TP2 ({fmt(tp2)}).\nЧастично фиксируем + стоп в безубыток."
        urgency = "success"
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
    elif hit_tp1 and not pos.get("notified_tp"):
        reason = f"✅ Достигнут TP1 ({fmt(tp1)}).\nМожно закрыть часть позиции."
        urgency = "success"
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
    if not reason and time.time() - market_bias["ts"] < 25 * 60:
        bias = market_bias["bias"]
        if side == "LONG" and bias == -1:
            reason = (
                f"⚠️ Новостной фон стал медвежьим.\n"
                f"{market_bias['text']}\n"
                f"Рекомендую зафиксировать ({pnl_pct:+.1f}%)."
            )
            urgency = "warning"
        elif side == "SHORT" and bias == 1:
            reason = (
                f"⚠️ Новостной фон стал бычьим.\n"
                f"{market_bias['text']}\n"
                f"Рекомендую закрыть шорт ({pnl_pct:+.1f}%)."
            )
            urgency = "warning"
    if not reason:
        return
    emoji = {"danger": "🛑", "success": "✅", "warning": "⚠️", "info": "ℹ️"}.get(urgency, "ℹ️")
    text = (
        f"{emoji} <b>Рекомендация по {coin}</b>\n\n"
        f"Направление: <b>{side}</b>\n"
        f"Вход: <code>{fmt(entry)}</code>\n"
        f"Сейчас: <code>{fmt(price)}</code>\n"
        f"PnL: <b>{pnl_pct:+.2f}%</b>\n\n"
        f"{reason}"
    )
    try:
        await bot.send_message(
            user_id,
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")],
                [InlineKeyboardButton(text="❌ Закрыл сделку", callback_data=f"pos:close:{coin}")],
            ])
        )
        if urgency in ("danger", "success"):
            pos["notified_close"] = True
            set_user_open_position(user_id, coin, pos)
    except Exception as e:
        log.warning(f"Не удалось отправить уведомление {user_id}: {e}")

# ============================================================================
# ХЕНДЛЕРЫ
# ============================================================================
@dp.message(Command("start", "menu"))
async def cmd_start(message: types.Message):
    save_user_data(message.from_user.id, message.from_user.username, message.from_user.first_name)
    active_chats.add(message.chat.id)
    await message.answer(
        f"👋 <b>IXCLY v{VERSION}</b>\nВыбери монету или посмотри сигналы:",
        reply_markup=get_coins_keyboard(),
        parse_mode="HTML"
    )

@dp.message(Command("panel"))
async def cmd_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён")
        return
    total, active, _, _ = user_counts()
    text = (
        f"✅ <b>Бот запущен (v{VERSION})</b>\n"
        f"Пользователей: {total} (активных {active})\n"
    )
    if total == 0:
        text += "\n⚠️ База пуста. Восстановите из бэкапа."
    await message.answer(text, reply_markup=get_admin_keyboard(), parse_mode="HTML")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    total, active, new_t, new_w = user_counts()
    uptime = int(time.time() - stats["started"])
    text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"Пользователей: {total} (активных {active})\n"
        f"Новых сегодня/неделя: {new_t}/{new_w}\n"
        f"Сигналов: {stats['signals']}\n"
        f"Новостей: {stats['news_sent']}\n"
        f"Алертов: {stats['alerts_sent']}\n"
        f"Аптайм: {uptime // 3600}ч {(uptime % 3600) // 60}м\n"
        f"Mongo: {'✅' if users_col else '❌'}\n"
        f"Bias: {market_bias['bias']} | {market_bias['text'][:90]}"
    )
    await message.answer(text, parse_mode="HTML")

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.message.answer(
        f"👋 <b>IXCLY v{VERSION}</b>\nВыбери монету или посмотри сигналы:",
        reply_markup=get_coins_keyboard(),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("p:"))
async def cb_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_reply_markup(reply_markup=get_coins_keyboard(page))
    await cb.answer()

@dp.callback_query(F.data.startswith("c:"))
async def cb_coin(cb: types.CallbackQuery):
    coin = cb.data.split(":")[1]
    uid = cb.from_user.id
    if uid in busy_users:
        await cb.answer("Уже обрабатываю предыдущий запрос…", show_alert=False)
        return
    busy_users.add(uid)
    try:
        await cb.answer("⏳ Анализирую…")
        res, err = await analyze_coin(coin, uid)
        if err:
            await cb.message.answer(err, parse_mode="HTML")
            return
        await safe_delete(cb.message)
        if res["img"]:
            await bot.send_photo(
                cb.message.chat.id,
                BufferedInputFile(res["img"], filename=f"{coin}.png"),
                caption=res["text"],
                parse_mode="HTML",
                reply_markup=result_keyboard(coin)
            )
        else:
            await cb.message.answer(
                res["text"],
                parse_mode="HTML",
                reply_markup=result_keyboard(coin),
                disable_web_page_preview=True
            )
        result_cache[uid] = {"coin": coin, "text": res["text"], "ts": time.time(), **res}
    finally:
        busy_users.discard(uid)

@dp.callback_query(F.data.startswith("r:"))
async def cb_refresh(cb: types.CallbackQuery):
    await cb_coin(cb)

@dp.callback_query(F.data == "last_signals")
async def cb_last_signals(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    if last_signals_data.get("text") and time.time() - last_signals_data.get("ts", 0) < 3600:
        await cb.message.answer(
            last_signals_data["text"],
            parse_mode="HTML",
            reply_markup=last_signals_data.get("kb")
        )
    else:
        hist = _load_signal_history()
        if not hist:
            await cb.message.answer(
                "📭 Свежих сигналов нет",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
                ])
            )
        else:
            text = "📜 <b>История сигналов за 24ч</b>\n\n"
            for h in hist[-10:][::-1]:
                emoji = "🟢" if h.get("side") == "LONG" else "🔴"
                text += f"{emoji} <b>{h['coin']}</b> · {h.get('type', '')} · {h.get('strength', 0)}%\n"
            await cb.message.answer(
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
                ])
            )
    await cb.answer()

@dp.callback_query(F.data == "show_stats")
async def cb_show_stats(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.answer("📊 Собираю статистику…")
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        data = {item["symbol"]: item for item in j}
        gainers, losers = [], []
        for coin in POPULAR_COINS:
            symbol = f"{coin}USDT"
            if symbol in data:
                ch = float(data[symbol]["priceChangePercent"])
                open_p = float(data[symbol]["openPrice"])
                last_p = float(data[symbol]["lastPrice"])
                if ch > 0:
                    gainers.append((coin, ch, open_p, last_p))
                else:
                    losers.append((coin, ch, open_p, last_p))
        gainers.sort(key=lambda x: x[1], reverse=True)
        losers.sort(key=lambda x: x[1])
        text = "📊 <b>Статистика рынка за 24 часа</b>\n\n"
        if gainers:
            text += "🟢 <b>ТОП РОСТА:</b>\n"
            for c, ch, op, lp in gainers[:10]:
                text += f"<code>{c}</code>  <b>+{ch:.1f}%</b>\n└ {fmt(op)} → {fmt(lp)}\n\n"
        if losers:
            text += "🔴 <b>ТОП ПАДЕНИЯ:</b>\n"
            for c, ch, op, lp in losers[:10]:
                text += f"<code>{c}</code>  <b>{ch:.1f}%</b>\n└ {fmt(op)} → {fmt(lp)}\n\n"
        text += f"<i>Показаны Топ-10 из {len(POPULAR_COINS)} монет.</i>"
        await cb.message.answer(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
            ])
        )
    except Exception as e:
        await cb.message.answer(f"Ошибка получения статистики: {e}")

@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.answer("🧐 Ищу интересные графики…")
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        movers = [x for x in j if x["symbol"] in [f"{c}USDT" for c in POPULAR_COINS]]
        movers.sort(key=lambda x: float(x["priceChangePercent"]), reverse=True)
        top_gainers = movers[:4]
        top_losers = movers[-4:]
        text = "🧐 <b>Интересные графики за 24ч</b>\n\n"
        text += "🟢 <b>Топ роста:</b>\n"
        for g in top_gainers:
            coin = g["symbol"].replace("USDT", "")
            ch = float(g["priceChangePercent"])
            text += f"<code>{coin}</code> +{ch:.1f}%\n"
        text += "\n🔴 <b>Топ падения:</b>\n"
        for l in top_losers:
            coin = l["symbol"].replace("USDT", "")
            ch = float(l["priceChangePercent"])
            text += f"<code>{coin}</code> {ch:.1f}%\n"
        text += "\n<i>Нажми на монету в меню для полного анализа.</i>"
        await cb.message.answer(text, parse_mode="HTML", reply_markup=get_coins_keyboard())
    except Exception:
        await cb.message.answer("Не удалось получить данные", reply_markup=get_coins_keyboard())

@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.message.answer("🔍 Напиши тикер монеты (например BTC, SOL, PEPE):")
    await cb.answer()

@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,12}$"))
async def msg_ticker(message: types.Message):
    coin = normalize_ticker(message.text)
    uid = message.from_user.id
    if uid in busy_users:
        await message.answer("Уже обрабатываю…")
        return
    busy_users.add(uid)
    try:
        res, err = await analyze_coin(coin, uid)
        if err:
            await message.answer(err, parse_mode="HTML")
            return
        if res["img"]:
            await message.answer_photo(
                BufferedInputFile(res["img"], filename=f"{coin}.png"),
                caption=res["text"],
                parse_mode="HTML",
                reply_markup=result_keyboard(coin)
            )
        else:
            await message.answer(
                res["text"],
                parse_mode="HTML",
                reply_markup=result_keyboard(coin),
                disable_web_page_preview=True
            )
        result_cache[uid] = {"coin": coin, "text": res["text"], "ts": time.time(), **res}
    finally:
        busy_users.discard(uid)

# Избранное
@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.message.answer(
        "⭐ <b>Избранные сигналы</b>",
        reply_markup=get_saved_signals_menu(cb.from_user.id),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "sig:save_new")
async def cb_sig_save_new(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "Выбери монету для сохранения:",
        reply_markup=get_save_coins_keyboard()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("sig:save_page:"))
async def cb_sig_save_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[2])
    await cb.message.edit_reply_markup(reply_markup=get_save_coins_keyboard(page))
    await cb.answer()

@dp.callback_query(F.data.startswith("sig:do_save:") | F.data.startswith("sig:quick_save:"))
async def cb_sig_do_save(cb: types.CallbackQuery):
    coin = cb.data.split(":")[-1]
    uid = cb.from_user.id
    cached = result_cache.get(uid)
    if cached and cached.get("coin") == coin and time.time() - cached.get("ts", 0) < 400:
        text = cached["text"]
    else:
        res, err = await analyze_coin(coin)
        if err:
            await cb.answer("Не удалось сохранить", show_alert=True)
            return
        text = res["text"]
    add_user_saved_signal(uid, coin, text)
    await cb.answer(f"✅ {coin} сохранён в избранное")
    if "quick_save" not in cb.data:
        await cb.message.edit_text(
            "⭐ <b>Избранные сигналы</b>",
            reply_markup=get_saved_signals_menu(uid),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("sig:view:"))
async def cb_sig_view(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    saved = get_user_saved_signals(cb.from_user.id)
    item = next((s for s in saved if s["coin"] == coin), None)
    if not item:
        await cb.answer("Сигнал не найден", show_alert=True)
        return
    await cb.message.answer(
        item["text"],
        parse_mode="HTML",
        reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin),
        disable_web_page_preview=True
    )
    await cb.answer()

@dp.callback_query(F.data == "sig:del_menu")
async def cb_sig_del_menu(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "Выбери сигнал для удаления:",
        reply_markup=get_delete_signals_menu(cb.from_user.id)
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("sig:del:"))
async def cb_sig_del(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    del_user_saved_signal(cb.from_user.id, coin)
    await cb.answer(f"🗑 {coin} удалён")
    await cb.message.edit_text(
        "⭐ <b>Избранные сигналы</b>",
        reply_markup=get_saved_signals_menu(cb.from_user.id),
        parse_mode="HTML"
    )

# Позиции
@dp.callback_query(F.data.startswith("pos:enter:"))
async def cb_pos_enter(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    uid = cb.from_user.id
    res, err = await analyze_coin(coin, uid)
    if err:
        await cb.answer("Не удалось получить актуальные уровни", show_alert=True)
        return
    data = {
        "side": res["side"],
        "entry_price": res["entry"],
        "entered_at": time.time(),
        "tp1": res["tps"][0],
        "tp2": res["tps"][1],
        "tp3": res["tps"][2],
        "sl1": res["sls"][0],
        "sl2": res["sls"][1],
        "last_price": res["price"],
        "notified_close": False,
        "notified_tp": False,
    }
    set_user_open_position(uid, coin, data)
    side_emoji = "🟢" if res["side"] == "LONG" else "🔴"
    await cb.message.answer(
        f"{side_emoji} <b>Позиция открыта</b>\n\n"
        f"Монета: <b>{coin}</b>\n"
        f"Направление: <b>{res['side']}</b>\n"
        f"Вход: <code>{fmt(res['entry'])}</code>\n\n"
        f"Я буду следить за позицией и сообщу, когда будет лучше закрыть "
        f"(с учётом цены и новостного фона).",
        parse_mode="HTML",
        reply_markup=get_saved_signal_view_keyboard(uid, coin)
    )
    await cb.answer("✅ Позиция зафиксирована")

@dp.callback_query(F.data.startswith("pos:close:"))
async def cb_pos_close(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    set_user_open_position(cb.from_user.id, coin, None)
    await cb.answer("❌ Позиция закрыта вручную")
    await cb.message.edit_reply_markup(
        reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin)
    )

@dp.callback_query(F.data.startswith("pos:status:"))
async def cb_pos_status(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    pos = get_open_position(cb.from_user.id, coin)
    if not pos:
        await cb.answer("Позиция не найдена", show_alert=True)
        return
    price = await get_simple_price(coin)
    if not price:
        await cb.answer("Не удалось получить цену", show_alert=True)
        return
    entry = pos["entry_price"]
    side = pos["side"]
    if side == "LONG":
        pnl_pct = (price - entry) / entry * 100
    else:
        pnl_pct = (entry - price) / entry * 100
    pnl_emoji = "🟢" if pnl_pct >= 0 else "🔴"
    text = (
        f"📈 <b>Статус позиции {coin}</b>\n\n"
        f"Направление: <b>{side}</b>\n"
        f"Вход: <code>{fmt(entry)}</code>\n"
        f"Сейчас: <code>{fmt(price)}</code>\n"
        f"PnL: {pnl_emoji} <b>{pnl_pct:+.2f}%</b>\n\n"
        f"TP1: <code>{fmt(pos['tp1'])}</code>\n"
        f"TP2: <code>{fmt(pos['tp2'])}</code>\n"
        f"SL1: <code>{fmt(pos['sl1'])}</code>"
    )
    await cb.message.answer(text, parse_mode="HTML")
    await cb.answer()

# Админ
@dp.callback_query(F.data.startswith("admin:"))
async def cb_admin(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    action = cb.data.split(":")[1]
    if action == "stats":
        total, active, new_t, new_w = user_counts()
        uptime = int(time.time() - stats["started"])
        text = (
            f"📊 <b>Статистика бота</b>\n\n"
            f"Версия: <code>{VERSION}</code>\n"
            f"Пользователей: <b>{total}</b> (активных {active})\n"
            f"Новых сегодня: {new_t} | за неделю: {new_w}\n"
            f"Сигналов отправлено: {stats['signals']}\n"
            f"Новостей: {stats['news_sent']}\n"
            f"Алертов: {stats['alerts_sent']}\n"
            f"Рассылок: {stats['broadcasts']}\n"
            f"Аптайм: {uptime // 3600}ч {(uptime % 3600) // 60}м\n"
            f"MongoDB: {'✅' if users_col else '❌ файл'}\n"
            f"Новости: {'🟢 ВКЛ' if bot_flags['news_enabled'] else '🔴 ВЫКЛ'}\n"
            f"Сканер: {'🟢 ВКЛ' if bot_flags['alerts_enabled'] else '🔴 ВЫКЛ'}\n"
            f"Bias: {market_bias['bias']} | {html.escape(market_bias['text'][:100])}"
        )
        await cb.message.answer(text, parse_mode="HTML")
    elif action == "users":
        users = _load_users()
        lines = [f"👥 <b>Пользователи ({len(users)})</b>\n"]
        for uid, u in list(users.items())[:45]:
            name = u.get("first_name") or u.get("username") or "—"
            active = "✅" if u.get("active", True) else "❌"
            lines.append(f"{active} <code>{uid}</code> — {html.escape(str(name)[:22])}")
        if len(users) > 45:
            lines.append(f"\n… и ещё {len(users) - 45}")
        await cb.message.answer("\n".join(lines), parse_mode="HTML")
    elif action == "toggle_news":
        bot_flags["news_enabled"] = not bot_flags["news_enabled"]
        await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
        await cb.answer(f"Новости: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}")
    elif action == "toggle_scan":
        bot_flags["alerts_enabled"] = not bot_flags["alerts_enabled"]
        await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
        await cb.answer(f"Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}")
    elif action == "news_now":
        await cb.answer("Отправляю новость…")
        try:
            entries = await get_news_entries()
            if not entries:
                await cb.message.answer("Нет свежих новостей")
                return
            entry = entries[0]
            title = entry.title
            img_url = extract_image(entry)
            short_text = (
                f"📰 <b>Свежая новость</b>\n\n"
                f"{html.escape(title)}\n\n"
                f"🌍 {html.escape(market_bias.get('text', 'Нейтральный фон'))}"
            )
            if img_url:
                try:
                    async with http.get(img_url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            img_data = await r.read()
                            await broadcast({
                                "kind": "photo",
                                "data": img_data,
                                "text": short_text,
                                "filename": "news.jpg"
                            })
                            stats["news_sent"] += 1
                            await cb.message.answer("Новость с картинкой отправлена")
                            return
                except Exception:
                    pass
            await broadcast({"kind": "text", "text": short_text})
            stats["news_sent"] += 1
            await cb.message.answer("Новость отправлена")
        except Exception as e:
            await cb.message.answer(f"Ошибка: {e}")
    elif action == "scan_test":
        await cb.answer("Запускаю тестовый скан…")
        try:
            signals = await scan_once(send=True)
            await cb.message.answer(f"✅ Скан завершён. Найдено сигналов: {len(signals)}")
        except Exception as e:
            await cb.message.answer(f"Ошибка скана: {e}")
    elif action == "broadcast_now":
        if last_signals_data.get("text"):
            sent, failed, _, _ = await broadcast({
                "kind": "text",
                "text": last_signals_data["text"],
                "markup": last_signals_data.get("kb")
            })
            await cb.message.answer(f"✅ Рассылка: {sent} доставлено, {failed} ошибок")
        else:
            await cb.message.answer("Нет сохранённых сигналов для рассылки")
    elif action == "diag":
        text = (
            f"🔧 <b>Диагностика</b>\n\n"
            f"Версия: {VERSION}\n"
            f"MongoDB: {'✅' if users_col else '❌'}\n"
            f"Активных чатов: {len(active_chats)}\n"
            f"Gemini ключей: {len(GEMINI_API_KEYS)}\n"
            f"Groq: {'✅' if GROQ_API_KEY else '❌'}\n"
            f"OpenRouter: {'✅' if OPENROUTER_API_KEY else '❌'}\n"
            f"Новости: {bot_flags['news_enabled']}\n"
            f"Сканер: {bot_flags['alerts_enabled']}\n"
            f"Market bias: {market_bias}\n"
            f"RSS peak: {_rss_mb()} MB"
        )
        await cb.message.answer(text, parse_mode="HTML")
    elif action == "backup":
        users = _load_users()
        data = json.dumps(users, ensure_ascii=False, indent=2)
        file = BufferedInputFile(data.encode("utf-8"), filename="users.json")
        await cb.message.answer_document(
            file,
            caption=f"💾 Бэкап базы\nПользователей: {len(users)}"
        )
    elif action == "restore":
        await cb.message.answer(
            "📥 <b>Восстановление базы</b>\n\n"
            "Пришли файл <code>users.json</code> (тот, который скачал через «Бэкап»).\n"
            "Я полностью заменю текущую базу.",
            parse_mode="HTML"
        )
    await cb.answer()

@dp.message(F.document)
async def handle_document(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    doc = message.document
    if not doc.file_name or not doc.file_name.lower().endswith(".json"):
        await message.answer("Нужен файл .json (users.json)")
        return
    try:
        file = await bot.get_file(doc.file_id)
        content = await bot.download_file(file.file_path)
        data = json.loads(content.read().decode("utf-8"))
        if not isinstance(data, dict):
            await message.answer("Неверный формат файла")
            return
        if users_col is not None:
            users_col.delete_many({})
            for uid, udata in data.items():
                users_col.update_one({"_id": str(uid)}, {"$set": udata}, upsert=True)
        else:
            with open(USERS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        load_active_chats()
        await message.answer(
            f"✅ База восстановлена!\n"
            f"Пользователей: {len(data)}\n"
            f"Активных чатов: {len(active_chats)}"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка восстановления: {e}")

@dp.callback_query(F.data == "ignore")
async def cb_ignore(cb: types.CallbackQuery):
    await cb.answer()

# ============================================================================
# MAIN
# ============================================================================
async def main():
    global http
    keep_alive()
    init_mongo()
    load_active_chats()
    _load_news_history()
    http = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=28),
        headers={"User-Agent": "IXCLY/2.10.18"}
    )
    asyncio.create_task(scanner_loop())
    asyncio.create_task(position_monitor_loop())
    asyncio.create_task(update_market_bias())
    runtime["main_bot"] = "запущен"
    log.info(f"🚀 IXCLY v{VERSION} стартовал | получателей: {len(active_chats)}")
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        await http.close()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен")
