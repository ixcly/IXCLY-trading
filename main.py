# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v2.10.11
- MongoDB для пользователей
- АВТОСОХРАНЕНИЕ: любой клик или сообщение сохраняет пользователя в базу
- СОХРАНЕНИЕ СИГНАЛОВ: меню "Избранное", сохранение картинок и точек входа в базу
- Сканер сигналов каждые 17 минут: ОДНО сообщение с кнопками + «Последние сигналы»
- ИСТОРИЯ СИГНАЛОВ: если нет свежих, можно посмотреть историю за 24 часа
- ИНТЕРЕСНЫЕ ГРАФИКИ: топ-4 роста и падения за 24 часа с ожидаемым сценарием
- ВОЗВРАЩЕН daily_stats_loop (ежедневная статистика в 00:00)
- МОНИТОРИНГ ОТКРЫТЫХ ПОЗИЦИЙ: кнопка «Вошёл в сделку» + уведомления когда лучше закрыть
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
from collections import Counter
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("ixcly")

# ----------------------------------------------------------------------------
# ВЕБ-СЕРВЕР ДЛЯ RENDER
# ----------------------------------------------------------------------------
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
        "peak_rss_mb": _rss_mb(),
        "version": VERSION,
        "uptime_s": int(time.time() - stats["started"]),
        "main_bot": runtime["main_bot"],
        "admin_bot": runtime["admin_bot"],
        "admin_ids_count": len(ADMIN_IDS),
        "recipients": len(active_chats),
        "mongo_connected": users_col is not None,
    })


def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 8080))
        log.info(f"🌐 Запускаю Flask на порту {port}...")
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"❌ Ошибка запуска Flask: {e}", exc_info=True)


_web_started = False


def keep_alive():
    global _web_started
    if _web_started:
        return
    _web_started = True
    Thread(target=_run_web, daemon=True).start()


# ----------------------------------------------------------------------------
# НАСТРОЙКИ
# ----------------------------------------------------------------------------
def clean_key(key):
    if not key:
        return None
    return key.strip().strip('"').strip("'")


BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
CRYPTOCOMPARE_KEY = clean_key(os.getenv("CRYPTOCOMPARE_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
MONGO_URI = clean_key(os.getenv("MONGO_URI"))

ADMIN_IDS = set()
for _x in re.split(r"[,\s]+", ADMIN_ID or ""):
    if _x.lstrip("-").isdigit():
        ADMIN_IDS.add(int(_x))

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")

VERSION = "2.10.11"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}


def _bot_id(t):
    return t.split(":")[0] if t and ":" in t else ("не задан" if not t else "неверный формат")


print(f"=== IXCLY v{VERSION} | BOT_TOKEN id={_bot_id(BOT_TOKEN)} | INPUT_BOT_TOKEN id={_bot_id(INPUT_BOT_TOKEN)} "
      f"| совпадают={bool(INPUT_BOT_TOKEN) and INPUT_BOT_TOKEN == BOT_TOKEN} | ADMIN_IDS={len(ADMIN_IDS)} ===", flush=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

input_bot = None
input_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    try:
        input_bot = Bot(token=INPUT_BOT_TOKEN)
        input_dp = Dispatcher()
    except Exception as e:
        log.error(f"Ошибка инициализации админ-бота: {e}")

http: aiohttp.ClientSession = None
analysis_sem: asyncio.Semaphore = None

TFS = ["15m", "1h", "4h", "1d"]
TF_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
MIN_CANDLES = 25

# --- СПИСОК МОНЕТ (184 шт) ---
POPULAR_COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "TON", "SHIB", "DOT", "LINK",
    "BCH", "NEAR", "POL", "LTC", "ICP", "UNI", "APT", "STX", "FIL", "IMX", "HBAR", "ATOM",
    "INJ", "OP", "ARB", "VET", "MKR", "SUI", "RNDR", "GRT", "THETA", "S", "SAND", "MANA",
    "GALA", "PEPE", "WIF", "BONK", "ORDI", "JUP", "TIA", "SEI",
    "TRX", "XLM", "ETC", "XMR", "AAVE", "ALGO", "EOS", "NEO", "IOTA", "ZEC",
    "DASH", "COMP", "SNX", "CRV", "LDO", "ENS", "1INCH", "CAKE", "RUNE", "KSM",
    "FLOW", "AXS", "CHZ", "BAT", "ENJ", "ZIL", "FTM", "KAVA", "EGLD", "ROSE",
    "AR", "FET", "WLD", "APE", "GMT", "CFX", "PYTH", "JTO", "ONDO", "FLOKI",
    "KAS", "CRO", "MNT", "TAO", "STRK", "DYDX", "GMX", "PENDLE", "SUSHI", "YFI",
    "BAL", "ZRX", "KNC", "UMA", "BAND", "API3", "SSV", "LPT", "RAY", "ORCA",
    "ILV", "MAGIC", "PRIME", "BEAM", "YGG", "ALICE", "TLM", "DUSK", "HIGH", "CHR",
    "C98", "AI", "NMR", "ARKM", "IO", "ATH", "BOME", "MEME", "SATS", "RATS",
    "DCR", "DGB", "XVG", "SC", "RVN", "WAVES", "ZEN", "BLAST", "AEVO", "ETHFI",
    "ENA", "W", "EIGEN", "MOVE", "PNUT", "GOAT", "ACT", "MOODENG", "TURBO", "MOG",
    "NEIRO", "DOGS", "NOT", "PIXEL", "PORTAL", "DYM", "ALT", "MANTA", "ZETA", "TNSR",
    "DRIFT", "KMNO", "WEN", "MYRO", "POPCAT", "GIGA", "RETARDIO", "SILLY", "PONKE", "BODEN",
    "MEW", "SLERF", "LUNC", "LUNA", "USTC", "CTSI", "COTI", "CELO", "CKB", "ONE",
    "QTUM", "IOST", "ONT", "ICX", "WAN", "ARK", "STRAX", "SYS", "DENT", "XEC"
]

SYMBOL_ALIASES = {
    "RNDR": ["RENDER", "RNDR"], "RENDER": ["RENDER", "RNDR"], "MATIC": ["POL", "MATIC"],
    "SATS": ["1000SATS", "SATS"], "RATS": ["1000RATS", "RATS"], "LUNC": ["1000LUNC", "LUNC"],
    "BONK": ["1000BONK", "BONK"], "PEPE": ["1000PEPE", "PEPE"], "FLOKI": ["1000FLOKI", "FLOKI"], "XEC": ["1000XEC", "XEC"],
}

USERS_FILE = "users.json"
NEWS_HISTORY_FILE = "news_history.json"
SIGNAL_HISTORY_FILE = "signal_history.json"
active_chats = set()
busy_users = set()
user_last_request = {}
result_cache = {}
notified_users = set()
RESULT_TTL = 90
COOLDOWN = 3
last_news_link = None
news_history = []
last_signals_data = {"text": "", "kb": None, "ts": 0}
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "alerts_sent": 0, "started": time.time(), "engines": Counter()}
bot_flags = {"news_enabled": True, "instant": True, "alerts_enabled": True}

# --- БАЗА ДАННЫХ MONGODB ---
users_col = None


def init_mongo():
    global users_col
    if MONGO_URI:
        try:
            client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, socketTimeoutMS=8000)
            db = client.get_database("ixcly_db")
            col = db["users"]
            client.server_info()
            users_col = col
            log.info("✅ Подключено к MongoDB")
        except Exception as e:
            log.error(f"❌ Ошибка подключения к MongoDB: {e}")
            users_col = None
    else:
        log.warning("⚠️ MONGO_URI не задан! Данные будут сохраняться в файл.")


# ----------------------------------------------------------------------------
# ПОЛЬЗОВАТЕЛИ
# ----------------------------------------------------------------------------
def _load_users():
    if users_col is not None:
        try:
            return {str(u["_id"]): {k: v for k, v in u.items() if k != "_id"} for u in users_col.find()}
        except Exception as e:
            log.error(f"Mongo load error: {e}")
            return {}
    else:
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


def _save_users(users):
    if users_col is not None:
        try:
            for uid, data in users.items():
                users_col.update_one({"_id": uid}, {"$set": data}, upsert=True)
        except Exception as e:
            log.error(f"Mongo save error: {e}")
    else:
        try:
            tmp = USERS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(users, f, ensure_ascii=False, indent=4)
            os.replace(tmp, USERS_FILE)
        except Exception as e:
            log.warning("Не удалось сохранить users.json: %s", e)


def save_user_data(user_id, username, first_name):
    key = str(user_id)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if users_col is not None:
        try:
            users_col.update_one(
                {"_id": key},
                {"$set": {"username": username, "first_name": first_name, "active": True}, "$setOnInsert": {"joined_at": now_str}},
                upsert=True
            )
            return True
        except Exception as e:
            log.error(f"Mongo save_user error: {e}")
            return False
    else:
        users = _load_users()
        if key in users:
            if users[key].get("active") is False:
                users[key]["active"] = True
                _save_users(users)
            return False
        users[key] = {"username": username, "first_name": first_name, "joined_at": now_str, "active": True}
        _save_users(users)
        return True


def get_user_saved_signals(user_id):
    users = _load_users()
    u = users.get(str(user_id), {})
    return u.get("saved_signals", [])


def add_user_saved_signal(user_id, coin, text, photo_id):
    key = str(user_id)
    if users_col is not None:
        try:
            users_col.update_one({"_id": key}, {"$pull": {"saved_signals": {"coin": coin}}})
            users_col.update_one({"_id": key}, {"$push": {"saved_signals": {"coin": coin, "text": text, "photo_id": photo_id}}})
            return True
        except Exception as e:
            log.error(f"Mongo add signal error: {e}")
            return False
    else:
        users = _load_users()
        if key not in users:
            return False
        if "saved_signals" not in users[key]:
            users[key]["saved_signals"] = []
        users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
        users[key]["saved_signals"].append({"coin": coin, "text": text, "photo_id": photo_id})
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
    else:
        users = _load_users()
        if key in users and "saved_signals" in users[key]:
            users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
            _save_users(users)
            return True
        return False


def get_user_open_positions(user_id):
    users = _load_users()
    u = users.get(str(user_id), {})
    return u.get("open_positions", {})


def set_user_open_position(user_id, coin, data):
    """data = dict или None (чтобы удалить)"""
    key = str(user_id)
    if users_col is not None:
        try:
            if data is None:
                users_col.update_one({"_id": key}, {"$unset": {f"open_positions.{coin}": ""}})
            else:
                users_col.update_one({"_id": key}, {"$set": {f"open_positions.{coin}": data}}, upsert=True)
            return True
        except Exception as e:
            log.error(f"Mongo open_pos error: {e}")
            return False
    else:
        users = _load_users()
        if key not in users:
            users[key] = {}
        if "open_positions" not in users[key]:
            users[key]["open_positions"] = {}
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
    changed = {}
    for i in ids:
        k = str(i)
        if k in users and users.get(k, {}).get("active", True) != active:
            users[k]["active"] = active
            changed[k] = users[k]
    if changed:
        _save_users(changed if users_col is not None else users)


def load_active_chats():
    active_chats.clear()
    for uid, u in _load_users().items():
        if isinstance(u, dict) and u.get("active", True) is False:
            continue
        try:
            active_chats.add(int(uid))
        except ValueError:
            pass
    log.info("Получателей рассылки и новостей: %d", len(active_chats))


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
            new_today += 1 if age == 0 else 0
            new_week += 1 if age < 7 else 0
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
    global news_history
    try:
        news_history = news_history[-100:]
        with open(NEWS_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(news_history, f, ensure_ascii=False, indent=4)
    except Exception as e:
        log.warning("news_history.json не сохранён: %s", e)


def _load_signal_history():
    try:
        with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = []
    now = time.time()
    return [x for x in data if now - x.get("ts", 0) < 24 * 3600]


def _save_signal_history(data):
    try:
        with open(SIGNAL_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        log.warning("signal_history.json не сохранён: %s", e)


# ----------------------------------------------------------------------------
# МИДЛВАРЬ
# ----------------------------------------------------------------------------
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
            asyncio.create_task(asyncio.to_thread(save_user_data, user.id, user.username, user.first_name))
        return await handler(event, data)


dp.update.middleware(UserTrackerMiddleware())


# ----------------------------------------------------------------------------
# КЛАВИАТУРЫ
# ----------------------------------------------------------------------------
def get_coins_keyboard(page=0):
    per_page = 12
    coins_page = POPULAR_COINS[page * per_page:(page + 1) * per_page]
    buttons = []
    for i in range(0, len(coins_page), 2):
        row = [InlineKeyboardButton(text=coins_page[i], callback_data=f"c:{coins_page[i]}")]
        if i + 1 < len(coins_page):
            row.append(InlineKeyboardButton(text=coins_page[i + 1], callback_data=f"c:{coins_page[i + 1]}"))
        buttons.append(row)

    total_pages = (len(POPULAR_COINS) + per_page - 1) // per_page
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"p:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"Стр. {page + 1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"p:{page + 1}"))
    buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="🧐 Интересные графики", callback_data="interesting_menu")])
    buttons.append([InlineKeyboardButton(text="🔥 Последние сигналы", callback_data="last_signals")])
    buttons.append([InlineKeyboardButton(text="⭐ Избранное", callback_data="sig:menu")])
    buttons.append([InlineKeyboardButton(text="📊 Статистика за 24 часа", callback_data="show_stats")])
    buttons.append([InlineKeyboardButton(text="🔍 Поиск монеты", callback_data="search")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def result_keyboard(coin):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"r:{coin}"),
        InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sig:quick_save:{coin}"),
        InlineKeyboardButton(text="📋 Меню", callback_data="menu"),
    ]])


def get_saved_signals_menu(user_id):
    saved = get_user_saved_signals(user_id)
    buttons = []
    if saved:
        row = []
        for s in saved:
            row.append(InlineKeyboardButton(text=f"🪙 {s['coin']}", callback_data=f"sig:view:{s['coin']}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

    buttons.append([InlineKeyboardButton(text="➕ Сохранить новый", callback_data="sig:save_new")])
    if saved:
        buttons.append([InlineKeyboardButton(text="🗑 Удалить сигнал", callback_data="sig:del_menu")])
    buttons.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="menu")])
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
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"sig:save_page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"Стр. {page + 1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"sig:save_page:{page + 1}"))
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
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к избранному", callback_data="sig:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ
# ----------------------------------------------------------------------------
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


async def first_success(coros, timeout):
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        pending = set(tasks)
        end = time.monotonic() + timeout
        while pending:
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            for d in done:
                try:
                    res = d.result()
                except Exception:
                    continue
                if res is not None:
                    return res
        return None
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def http_json(url, params=None, headers=None, timeout=8):
    async with http.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.json(content_type=None)


def normalize_ticker(text):
    t = text.upper().strip()
    t = re.sub(r"[\s$/\-_]", "", t)
    if t.endswith("USDT") and len(t) > 4:
        t = t[:-4]
    return t


def extract_dates_from_text(text):
    patterns = [
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?(?:,? \d{4})?\b',
        r'\b\d{1,2}[./]\d{1,2}[./]\d{2,4}\b',
        r'\b\d{4}-\d{2}-\d{2}\b'
    ]
    found = set()
    for p in patterns:
        matches = re.findall(p, text, re.IGNORECASE)
        for m in matches:
            found.add(m)
    return list(found)


async def translate_text(text, target="ru"):
    if not text:
        return text
    try:
        url = f"https://lingva.ml/api/v1/auto/{target}/{quote(text, safe='')}"
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                if "translation" in data:
                    return data["translation"]
    except Exception:
        pass
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text}
        async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                return "".join([item[0] for item in data[0]])
    except Exception:
        pass
    try:
        url = "https://api.mymemory.translated.net/get"
        params = {"q": text[:450], "langpair": f"en|{target}"}
        async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                if data.get("responseStatus") == 200:
                    return data["responseData"]["translatedText"]
    except Exception:
        pass
    try:
        url = "https://translate.argosopentech.com/translate"
        payload = {"q": text, "source": "auto", "target": target, "format": "text"}
        async with http.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                if "translatedText" in data:
                    return data["translatedText"]
    except Exception:
        pass
    return text


# ----------------------------------------------------------------------------
# ИНДИКАТОРЫ
# ----------------------------------------------------------------------------
def add_indicators(df):
    c = df["close"]
    df["ema20"] = c.ewm(span=20, adjust=False).mean()
    df["ema50"] = c.ewm(span=50, adjust=False).mean()
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
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev).abs(), (df["low"] - prev).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    df["atr"] = atr.fillna(tr.mean())
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    signal = macd.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = macd - signal
    return df


def calc_probability(df):
    ema20 = float(df["ema20"].iloc[-1])
    ema50 = float(df["ema50"].iloc[-1]) or 1e-12
    rsi = float(df["rsi"].iloc[-1])
    macd_hist = float(df["macd_hist"].iloc[-1])
    score = 0.0
    diff_pct = abs(ema20 - ema50) / ema50 * 100
    if ema20 > ema50:
        score += 20 + min(20, diff_pct * 15)
    else:
        score -= 20 + min(20, diff_pct * 15)
    if rsi > 50:
        score += min(30, (rsi - 50) * 1.2)
    else:
        score -= min(30, (50 - rsi) * 1.2)
    score += 30 if macd_hist > 0 else -30
    long_prob = max(5.0, min(95.0, 50 + score / 2))
    return round(long_prob, 2), round(100 - long_prob, 2)


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
            "rsi": float(last["rsi"]),
            "atr": float(last["atr"]),
            "macd_hist": float(last["macd_hist"]),
            "recent_low": float(df["low"].tail(24).min()),
            "recent_high": float(df["high"].tail(24).max()),
            "long_prob": long_prob,
        }
    return out


# ----------------------------------------------------------------------------
# ИСТОЧНИКИ ДАННЫХ
# ----------------------------------------------------------------------------
def _p_binance(j):
    if not isinstance(j, list):
        raise ValueError("bad response")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]


def _p_gate(j):
    if not isinstance(j, list):
        raise ValueError("bad response")
    return [[float(x[0]) * 1000, float(x[5]), float(x[3]), float(x[4]), float(x[2]), float(x[6])] for x in j]


def _p_kucoin(j):
    if j.get("code") != "200000" or not j.get("data"):
        raise ValueError("bad response")
    return [[float(x[0]) * 1000, float(x[1]), float(x[3]), float(x[4]), float(x[2]), float(x[5])] for x in reversed(j["data"])][-200:]


def _p_okx(j):
    if j.get("code") != "0" or not j.get("data"):
        raise ValueError("bad response")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["data"])]


def _p_bybit(j):
    if j.get("retCode") != 0 or not j.get("result", {}).get("list"):
        raise ValueError("bad response")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["result"]["list"])]


EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines", "params": lambda c, tf: {"symbol": f"{c}USDT", "interval": tf, "limit": 200}, "parse": _p_binance},
    {"name": "MEXC", "url": "https://api.mexc.com/api/v3/klines", "params": lambda c, tf: {"symbol": f"{c}USDT", "interval": {"1h": "60m"}.get(tf, tf), "limit": 200}, "parse": _p_binance},
    {"name": "GATE", "url": "https://api.gateio.ws/api/v4/spot/candlesticks", "params": lambda c, tf: {"currency_pair": f"{c}_USDT", "interval": tf, "limit": 200}, "parse": _p_gate},
    {"name": "KUCOIN", "url": "https://api.kucoin.com/api/v1/market/candles", "params": lambda c, tf: {"symbol": f"{c}-USDT", "type": {"15m": "15min", "1h": "1hour", "4h": "4hour", "1d": "1day"}[tf], "startAt": int(time.time() - 200 * TF_SECONDS[tf]), "endAt": int(time.time())}, "parse": _p_kucoin},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles", "params": lambda c, tf: {"instId": f"{c}-USDT", "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf], "limit": 200}, "parse": _p_okx},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline", "params": lambda c, tf: {"category": "spot", "symbol": f"{c}USDT", "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf], "limit": 200}, "parse": _p_bybit},
]


async def _fetch_klines(ex, coin, tf):
    j = await http_json(ex["url"], params=ex["params"](coin, tf), timeout=8)
    rows = ex["parse"](j)
    if len(rows) < MIN_CANDLES:
        raise ValueError("мало свечей")
    return rows


async def fetch_from_exchange(ex, coin):
    results = await asyncio.gather(*[_fetch_klines(ex, coin, tf) for tf in TFS], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            return None
    return await asyncio.to_thread(build_tf_data, dict(zip(TFS, results))), ex["name"]


async def fetch_cryptocompare(coin):
    specs = {"15m": ("histominute", 15), "1h": ("histohour", 1), "4h": ("histohour", 4), "1d": ("histoday", 1)}
    headers = {"authorization": f"Apikey {CRYPTOCOMPARE_KEY}"} if CRYPTOCOMPARE_KEY else None

    async def one(tf):
        ep, agg = specs[tf]
        j = await http_json(f"https://min-api.cryptocompare.com/data/v2/{ep}", params={"fsym": coin, "tsym": "USD", "limit": 200, "aggregate": agg}, headers=headers, timeout=10)
        if j.get("Response") != "Success":
            raise ValueError("not found")
        data = j["Data"]["Data"]
        rows = [[d["time"] * 1000, d["open"], d["high"], d["low"], d["close"], d.get("volumeto", 0)] for d in data if d.get("close", 0) > 0]
        if len(rows) < MIN_CANDLES:
            raise ValueError("мало свечей")
        return rows

    results = await asyncio.gather(*[one(tf) for tf in TFS], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            return None
    return await asyncio.to_thread(build_tf_data, dict(zip(TFS, results))), "CRYPTOCOMPARE"


preferred_source = {}


async def _fetch_named(ex, cand):
    res = await fetch_from_exchange(ex, cand)
    return (res[0], res[1], cand) if res else None


async def get_market_data(coin):
    cands = SYMBOL_ALIASES.get(coin, [coin])
    res = await first_success([_fetch_named(ex, c) for c in cands for ex in EXCHANGES], timeout=12)
    if res:
        preferred_source[coin] = (res[2], res[1])
        return res[0], res[1]
    for c in cands:
        r = await first_success([fetch_cryptocompare(c)], timeout=15)
        if r:
            return r
    raise LookupError(coin)


async def get_market_data_light(coin):
    pref = preferred_source.get(coin)
    if pref:
        cand, name = pref
        ex = next((e for e in EXCHANGES if e["name"] == name), None)
        if ex:
            try:
                res = await asyncio.wait_for(fetch_from_exchange(ex, cand), timeout=12)
            except Exception:
                res = None
            if res:
                return res
    return await get_market_data(coin)


async def get_simple_price(coin):
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/price", params={"symbol": f"{coin}USDT"}, timeout=5)
        return float(j["price"])
    except Exception:
        try:
            j = await http_json("https://api.bybit.com/v5/market/tickers", params={"category": "spot", "symbol": f"{coin}USDT"}, timeout=5)
            return float(j["result"]["list"][0]["lastPrice"])
        except Exception:
            return None


# ----------------------------------------------------------------------------
# СТАТИСТИКА И ИНТЕРЕСНЫЕ ГРАФИКИ
# ----------------------------------------------------------------------------
async def build_daily_stats():
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        data = {item['symbol']: item for item in j}
    except Exception as e:
        return f"⚠️ Ошибка при получении статистики: {e}"
    gainers, losers = [], []
    for coin in POPULAR_COINS:
        symbol = f"{coin}USDT"
        if symbol in data:
            item = data[symbol]
            change = float(item['priceChangePercent'])
            open_p = float(item['openPrice'])
            last_p = float(item['lastPrice'])
            if change > 0:
                gainers.append((coin, change, open_p, last_p))
            else:
                losers.append((coin, change, open_p, last_p))
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
    text += f"<i>Показаны Топ-10 роста и падения из {len(POPULAR_COINS)} монет.</i>"
    return text


async def daily_stats_loop():
    await asyncio.sleep(10)
    while True:
        now_utc = datetime.utcnow()
        now_msk = now_utc + timedelta(hours=3)
        next_run_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        sleep_seconds = (next_run_msk - now_msk).total_seconds()
        log.info(f"Следующая рассылка статистики через {sleep_seconds:.0f} секунд (в 00:00 МСК)")
        await asyncio.sleep(sleep_seconds)
        try:
            if active_chats:
                text = await build_daily_stats()
                payload = {"kind": "text", "text": text}
                sent, failed, removed, _ = await broadcast(payload)
                log.info(f"Ежедневная статистика разослана: {sent} доставлено, {failed} ошибок")
        except Exception as e:
            log.error(f"Ошибка при отправке ежедневной статистики: {e}")
        await asyncio.sleep(60)


async def get_interesting_coins():
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        movers = [x for x in j if x['symbol'] in [f"{c}USDT" for c in POPULAR_COINS]]
        movers.sort(key=lambda x: float(x['priceChangePercent']), reverse=True)
        top_gainers = movers[:4]
        top_losers = movers[-4:]
        return top_gainers, top_losers
    except Exception:
        return [], []


# ----------------------------------------------------------------------------
# ИИ
# ----------------------------------------------------------------------------
async def _ask_gemini(prompt, json_mode):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    if json_mode:
        payload["generationConfig"]["responseMimeType"] = "application/json"
    for key in GEMINI_API_KEYS:
        try:
            async with http.post(url, json=payload, headers={"x-goog-api-key": key}, timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
                if r.status != 200:
                    continue
                return j["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            pass
    return None


async def _ask_openai_compat(base_url, api_key, model, prompt, json_mode, extra_headers=None, use_json_format=False):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1500}
    if json_mode and use_json_format:
        payload["response_format"] = {"type": "json_object"}
    try:
        async with http.post(f"{base_url}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as r:
            j = await r.json(content_type=None)
            if r.status != 200 or "choices" not in j:
                return None
            return j["choices"][0]["message"]["content"]
    except Exception:
        return None


def _looks_like_junk(text):
    t = (text or "").strip()
    low = t.lower()
    if len(t) < 40 or low.startswith("<!doctype") or low.startswith("<html") or "pollinations" in low:
        return True
    return False


async def _ask_free_gpt(prompt, json_mode):
    system = "Ты опытный крипто-трейдер и макро-аналитик. Отвечай кратко, по делу, на русском языке."
    if json_mode:
        system += " Отвечай только валидным JSON."
    auth = {"Authorization": f"Bearer {POLLINATIONS_KEY}"} if POLLINATIONS_KEY else {}

    async def post_openai(url, timeout):
        payload = {"model": POLLINATIONS_MODEL, "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}], "temperature": 0.2}
        async with http.post(url, json=payload, headers={"Content-Type": "application/json", **auth}, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError()
            j = await r.json(content_type=None)
            return j["choices"][0]["message"]["content"]

    async def get_plain(url, timeout):
        async with http.get(url, params={"model": POLLINATIONS_MODEL, "system": system}, headers=auth, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError()
            return await r.text()

    short_prompt = quote(prompt[:3000], safe="")
    attempts = [
        ("openai", lambda: post_openai("https://text.pollinations.ai/openai", 12)),
        ("gen", lambda: get_plain(f"https://gen.pollinations.ai/text/{short_prompt}", 8)),
        ("text", lambda: get_plain(f"https://text.pollinations.ai/{short_prompt}", 6)),
    ]
    if POLLINATIONS_KEY:
        attempts.insert(0, ("genv1", lambda: post_openai("https://gen.pollinations.ai/v1/chat/completions", 12)))
    for name, fn in attempts:
        try:
            text = await fn()
            if text and not _looks_like_junk(text):
                return text
        except Exception:
            pass
    return None


async def ask_ai_named(prompt, json_mode=False, validate=None, timeout=15, free_timeout=25):
    keyed = []
    if GEMINI_API_KEYS:
        keyed.append(("Gemini", lambda: _ask_gemini(prompt, json_mode)))
    if GROQ_API_KEY:
        keyed.append(("Groq", lambda: _ask_openai_compat("https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt, json_mode, use_json_format=True)))
    if OPENROUTER_API_KEY:
        keyed.append(("OpenRouter", lambda: _ask_openai_compat("https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt, json_mode, extra_headers={"HTTP-Referer": "https://t.me/bot", "X-Title": "IXCLY"})))

    async def wrap(name, fn):
        text = await fn()
        if text and (validate is None or validate(text)):
            return text, name
        return None

    if keyed:
        res = await first_success([wrap(n, f) for n, f in keyed], timeout)
        if res:
            return res
    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt, json_mode))], free_timeout)
    if res:
        return res
    return None, None


def parse_ai_json(text):
    try:
        m = re.search(r"\{.*\}", text, re.S)
        data = json.loads(m.group(0))
        if data.get("dominant_tf") in TFS and isinstance(data.get("reason"), str):
            return data
    except Exception:
        pass
    return None


def has_russian(text, min_letters=30):
    return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= min_letters


def ai_to_html(text):
    text = html.escape(text.strip())
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    return text[:3500]


# ----------------------------------------------------------------------------
# ЛОКАЛЬНЫЙ ДВИЖОК
# ----------------------------------------------------------------------------
BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner", "upgrad", "breakout", "rebound", "recover", "accumul")
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall", "liquidat", "fraud", "scam", "delist", "arrest", "dump", "selloff", "declin", "probe", "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
BEAR_EXACT = {"ban", "bans", "banned"}
BULL_EXACT = {"etf", "buy", "launch", "launches", "record"}

COIN_WORDS = {
    "bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "ether": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL", "xrp": "XRP", "ripple": "XRP", "bnb": "BNB", "binance": "BNB", "dogecoin": "DOGE", "doge": "DOGE", "cardano": "ADA", "toncoin": "TON", "avalanche": "AVAX", "chainlink": "LINK", "polkadot": "DOT", "litecoin": "LTC", "shiba": "SHIB", "pepe": "PEPE", "sui": "SUI", "tron": "TRX", "stellar": "XLM", "aave": "AAVE", "monero": "XMR", "zcash": "ZEC", "algorand": "ALGO", "cosmos": "ATOM", "uniswap": "UNI", "aptos": "APT", "arbitrum": "ARB", "optimism": "OP", "hedera": "HBAR", "filecoin": "FIL", "injective": "INJ", "celestia": "TIA", "jupiter": "JUP", "ondo": "ONDO", "pyth": "PYTH", "floki": "FLOKI", "worldcoin": "WLD", "arweave": "AR", "render": "RNDR", "fetch": "FET", "stacks": "STX", "near": "NEAR", "sonic": "S", "kaspa": "KAS", "cronos": "CRO", "mantle": "MNT", "bittensor": "TAO", "starknet": "STRK", "dydx": "DYDX", "gmx": "GMX", "pendle": "PENDLE", "sushi": "SUSHI", "yearn": "YFI", "balancer": "BAL", "kyber": "KNC", "uma": "UMA", "band": "BAND", "api3": "API3", "livepeer": "LPT", "raydium": "RAY", "illuvium": "ILV", "magic": "MAGIC", "ygg": "YGG", "alice": "ALICE", "chromia": "CHR", "arkham": "ARKM", "bome": "BOME", "memecoin": "MEME", "ravencoin": "RVN", "waves": "WAVES", "blast": "BLAST", "aevo": "AEVO", "ethena": "ENA", "wormhole": "W", "eigenlayer": "EIGEN", "movement": "MOVE", "peanut": "PNUT", "goat": "GOAT", "moodeng": "MOODENG", "turbo": "TURBO", "mog": "MOG", "neiro": "NEIRO", "dogs": "DOGS", "notcoin": "NOT", "pixels": "PIXEL", "portal": "PORTAL", "dymension": "DYM", "altlayer": "ALT", "manta": "MANTA", "zetachain": "ZETA", "tensor": "TNSR", "drift": "DRIFT", "wen": "WEN", "popcat": "POPCAT", "slerf": "SLERF", "luna": "LUNA", "terra": "LUNA", "lunc": "LUNC", "terraclassic": "LUNC", "celo": "CELO", "ckb": "CKB", "harmony": "ONE", "qtum": "QTUM", "iost": "IOST", "ontology": "ONT", "icon": "ICX", "wanchain": "WAN", "ark": "ARK", "syscoin": "SYS", "dent": "DENT"
}

ENGINE_TAG = {"Gemini": "g", "Groq": "q", "OpenRouter": "o", "GPT (free)": "f", "local": "l"}
NEWS_LABEL = {1: ("🟢", "позитивный"), 0: ("⚪", "нейтральный"), -1: ("🔴", "негативный")}


def news_sentiment(text, threshold=2):
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w in BULL_EXACT or w.startswith(BULL_STEMS):
            score += 1
        elif w in BEAR_EXACT or w.startswith(BEAR_STEMS):
            score -= 1
    if score >= threshold:
        return 1
    if score <= -threshold:
        return -1
    return 0


def mentioned_coins(text):
    found = []
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        t = COIN_WORDS.get(w)
        if t and t not in found:
            found.append(t)
    return found[:3]


def coin_news_sentiment(coin, crypto_titles, world_titles):
    names = {w for w, t in COIN_WORDS.items() if t == coin}
    if len(coin) >= 3:
        names.add(coin.lower())
    related = [t for t in crypto_titles if names & set(re.findall(r"[a-z0-9]+", t.lower()))]
    if related:
        return news_sentiment(". ".join(related), threshold=1), True
    return news_sentiment(". ".join(crypto_titles + world_titles), threshold=2), False


def local_reason(tf, i):
    trend = "восходящий тренд" if i["ema20"] > i["ema50"] else "нисходящий тренд"
    rsi = i["rsi"]
    zone = "перекупленность" if rsi >= 70 else "перепроданность" if rsi <= 30 else "нейтральная зона"
    macd = "бычий" if i["macd_hist"] > 0 else "медвежий"
    return f"На {tf}: {trend}, RSI {rsi:.0f} ({zone}), импульс MACD {macd}."


# ----------------------------------------------------------------------------
# НОВОСТИ
# ----------------------------------------------------------------------------
NEWS_FEEDS = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
WORLD_FEEDS = ["https://feeds.bbci.co.uk/news/business/rss.xml", "https://feeds.bbci.co.uk/news/world/rss.xml"]
_news_cache = {"ts": 0.0, "entries": []}
_world_cache = {"ts": 0.0, "titles": []}


async def _load_feed(url):
    async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        if r.status != 200:
            return []
        text = await r.text()
    feed = await asyncio.to_thread(feedparser.parse, text)
    return list(feed.entries)


async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300:
        return _news_cache["entries"]
    for url in NEWS_FEEDS:
        try:
            entries = await _load_feed(url)
            if entries:
                _news_cache["ts"] = time.time()
                _news_cache["entries"] = entries[:8]
                return _news_cache["entries"]
        except Exception:
            pass
    return _news_cache["entries"]


async def get_world_entries():
    if _world_cache["titles"] and time.time() - _world_cache["ts"] < 300:
        return _world_cache["titles"]
    entries = []
    for url in WORLD_FEEDS:
        try:
            entries += (await _load_feed(url))[:4]
        except Exception:
            pass
    if entries:
        _world_cache["ts"] = time.time()
        _world_cache["titles"] = entries
    return _world_cache["titles"]


async def get_world_titles(n=4):
    entries = await get_world_entries()
    return [e.title for e in entries[:n]]


def extract_image(entry):
    mc = getattr(entry, "media_content", None)
    if mc and isinstance(mc, list) and mc[0].get("url"):
        return mc[0]["url"]
    for enc in getattr(entry, "enclosures", None) or []:
        if "image" in enc.get("type", ""):
            return enc.get("href")
    return None


async def download_bytes(url):
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                return await r.read()
    except Exception:
        pass
    return None


async def track_news(title, summary):
    coins = mentioned_coins(f"{title}. {summary}") or ["BTC", "ETH"]
    ns = news_sentiment(f"{title}. {summary}")
    if ns == 0:
        return
    prices = {}
    for c in coins:
        p = await get_simple_price(c)
        if p:
            prices[c] = p
    if not prices:
        return
    for item in news_history:
        if item.get("title") == title:
            return
    news_history.append({"ts": time.time(), "title": title, "coins": coins, "sentiment": ns, "prices": prices, "status": "pending"})
    _save_news_history()


async def check_past_news():
    if not news_history:
        return ""
    lines = []
    changed = False
    now = time.time()
    for item in news_history:
        if item.get("status") == "resolved":
            continue
        age = now - item["ts"]
        if age > 3 * 24 * 3600:
            item["status"] = "resolved"
            changed = True
            continue
        results = []
        resolved = True
        for c, entry_price in item["prices"].items():
            cur_p = await get_simple_price(c)
            if not cur_p:
                resolved = False
                continue
            diff = (cur_p - entry_price) / entry_price * 100
            results.append(f"{c} {diff:+.1f}%")
            if item["sentiment"] == 1 and diff < 1.5:
                resolved = False
            if item["sentiment"] == -1 and diff > -1.5:
                resolved = False
        if resolved and results:
            item["status"] = "resolved"
            changed = True
            side = "🟢 Рост" if item["sentiment"] == 1 else "🔴 Падение"
            lines.append(f"✅ <b>ПРОГНОЗ СБЫЛСЯ!</b> {side}\n<i>{html.escape(item['title'][:60])}...</i>\n📈 {', '.join(results)}\n")
    if changed:
        _save_news_history()
    if lines:
        return "📊 <b>Отчет по прошлым новостям:</b>\n\n" + "\n".join(lines) + "➖➖➖➖➖➖➖➖\n\n"
    return ""


async def fetch_and_analyze_news(entry):
    try:
        title = entry.title
        summary = re.sub(r"<[^>]+>", "", getattr(entry, "summary", ""))[:800]
        link = entry.link
        prompt = f"""Ты профессиональный трейдер. Проанализируй эту новость (это может быть крипто-новость ИЛИ макроэкономика, например ФРС, ставки, инфляция).
Заголовок: {title}
Описание: {summary}

ОБЯЗАТЕЛЬНО ПЕРЕВЕДИ ЗАГОЛОВОК И ТЕКСТ НА РУССКИЙ ЯЗЫК. НИКАКОГО АНГЛИЙСКОГО ТЕКСТА В ОТВЕТЕ. Без markdown.
Ответ краткий, по делу, без воды. Напиши:
📌 Что произошло (1-2 предложения).
📅 Дата и время (ЕСЛИ в новости указана конкретная дата или время события, напиши её сюда строго в формате 'ДД.ММ.ГГГГ ЧЧ:ММ (МСК)'. Если даты нет, НЕ ПИШИ этот пункт вообще).
📉 Как это влияет на рынок (позитив/негатив и почему).
🎯 Какие монеты затронуты (2-3 тикера, например BTC, ETH) и конкретный вывод: покупать, продавать или ждать.
"""
        ai_text, news_engine = await ask_ai_named(prompt, json_mode=False, validate=has_russian, timeout=20, free_timeout=25)
        if ai_text:
            body = ai_to_html(ai_text)
        else:
            ru_title = await translate_text(title)
            ru_summary = await translate_text(summary[:300])
            ns = news_sentiment(f"{title}. {summary}")
            emoji, label = NEWS_LABEL[ns]
            coins = mentioned_coins(f"{title}. {summary}") or ["BTC", "ETH"]
            action = {1: "поддержка покупателей, лонги выглядят интереснее", -1: "давление на рынок, осторожнее с лонгами", 0: "без явного перекоса, ждём подтверждения графиком"}[ns]
            dates = extract_dates_from_text(f"{title}. {summary}")
            date_str = f"\n📅 <b>Указанная дата:</b> {', '.join(dates)}" if dates else ""
            body = f"📌 <b>{html.escape(ru_title)}</b>\n\n<i>{html.escape(ru_summary)}</i>\n{date_str}\n📉 <b>Влияние:</b> {emoji} {label} фон — {action}.\n🎯 <b>Монеты:</b> {', '.join(coins)}"
        tag = ENGINE_TAG.get(news_engine if ai_text else "local", "l")
        text = f"{body}\n\n💰ВРЕМЯ ТОРГОВАТЬ💸\n\n🔗 <a href=\"{html.escape(link, quote=True)}\">Читать источник</a> {tag}"
        await track_news(title, summary)
        return text, extract_image(entry), True
    except Exception as e:
        log.exception("news error")
        return f"⚠️ Ошибка при получении новостей: {html.escape(str(e))}", None, False


async def send_with_image(chat_id, text, image_bytes, filename="img.png", markup=None):
    if image_bytes:
        photo = BufferedInputFile(image_bytes, filename=filename)
        if len(text) <= 1024:
            await bot.send_photo(chat_id, photo, caption=text, parse_mode="HTML", reply_markup=markup)
            return
        await bot.send_photo(chat_id, photo)
    await bot.send_message(chat_id, text[:4000], parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)


async def news_watcher():
    global last_news_link
    await asyncio.sleep(10)
    while True:
        try:
            crypto_entries = await get_news_entries()
            world_entries = await get_world_entries()
            all_entries = crypto_entries + world_entries
            all_entries.sort(key=lambda x: getattr(x, "published_parsed", None) or time.gmtime(0), reverse=True)
            if all_entries:
                latest = all_entries[0]
                link = latest.link
                if last_news_link is None:
                    last_news_link = link
                elif link != last_news_link:
                    last_news_link = link
                    if bot_flags["news_enabled"]:
                        check_text = await check_past_news()
                        text, image_url, ok = await fetch_and_analyze_news(latest)
                        if ok:
                            full_text = check_text + text if check_text else text
                            img = await download_bytes(image_url) if image_url else None
                            payload = {"kind": "photo" if img else "text", "text": full_text, "data": img, "filename": "news.jpg"}
                            sent, failed, removed, _ = await broadcast(payload)
                            stats["news_sent"] += sent
        except Exception as e:
            log.warning("news_watcher: %s", e)
        await asyncio.sleep(600)


# ----------------------------------------------------------------------------
# СКАНЕР
# ----------------------------------------------------------------------------
SCAN_INTERVAL = 17 * 60
ALERT_MIN_EDGE = 20
ALERT_COOLDOWN = 6 * 3600
MAX_ALERTS_PER_SCAN = 5
ALERTS_FILE = "alerts.json"
alert_history = {}


def _load_alert_history():
    try:
        with open(ALERTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            alert_history.update({k: float(v) for k, v in data.items()})
    except Exception:
        pass


def _save_alert_history():
    try:
        with open(ALERTS_FILE, "w", encoding="utf-8") as f:
            json.dump(alert_history, f)
    except Exception:
        pass


def tech_check(tf_data):
    tf = "4h" if "4h" in tf_data else list(tf_data.keys())[-1]
    d = tf_data[tf]
    p = d["long_prob"]
    is_long = p >= 50
    total = len(tf_data)
    aligned = sum(1 for t in tf_data if (tf_data[t]["long_prob"] >= 50) == is_long)
    edge = abs(p - 50)
    atr_mean = float(d["df"]["atr"].tail(50).mean())
    vol_ratio = d["atr"] / atr_mean if atr_mean > 0 else 1.0
    ok = aligned >= max(3, total - 1) and edge >= ALERT_MIN_EDGE and vol_ratio >= 0.8
    return {"ok": ok, "tf": tf, "is_long": is_long, "aligned": aligned, "total": total, "edge": edge, "prob": max(p, 100 - p)}


def calc_levels(info, is_long):
    price = info["price"]
    atr = info["atr"]
    ema20 = info["ema20"]
    if not atr or atr != atr or atr <= 0:
        atr = price * 0.01
    if is_long:
        pullback_target = price - 0.3 * atr
        if ema20 < price and ema20 > price - 0.5 * atr:
            pullback_target = ema20
        entry = pullback_target
    else:
        bounce_target = price + 0.3 * atr
        if ema20 > price and ema20 < price + 0.5 * atr:
            bounce_target = ema20
        entry = bounce_target
    if is_long:
        dist = entry - info["recent_low"] + 0.2 * atr
    else:
        dist = info["recent_high"] + 0.2 * atr - entry
    dist = min(max(dist, 1.0 * atr), 2.5 * atr)
    sign = 1 if is_long else -1
    sls = [entry - sign * (dist + k * 0.5 * atr) for k in range(3)]
    tps = [entry + sign * dist * m for m in (1, 2, 3)]
    return entry, tps, sls


async def scan_once(send=True):
    global last_signals_data
    t0 = time.time()
    try:
        entries = await asyncio.wait_for(get_news_entries(), timeout=10)
        crypto_titles = [e.title for e in entries[:8]]
    except Exception:
        crypto_titles = []
    try:
        world_titles = await asyncio.wait_for(get_world_titles(6), timeout=10)
    except Exception:
        world_titles = []
    world_block = "\n".join(f"- {t}" for t in world_titles) or "- Нет мировых новостей"
    crypto_block = "\n".join(f"- {t}" for t in crypto_titles) or "- Нет крипто-новостей"

    sem = asyncio.Semaphore(2)
    failed, local_candidates = [], []

    async def fetch_one(coin):
        async with sem:
            try:
                tf_data, source = await get_market_data_light(coin)
                t = tech_check(tf_data)
                if t["ok"]:
                    local_candidates.append({"coin": coin, "source": source, "tech": t, "tf_data": tf_data})
            except Exception:
                failed.append(coin)

    await asyncio.gather(*[fetch_one(c) for c in POPULAR_COINS])
    local_candidates.sort(key=lambda c: c["tech"]["edge"], reverse=True)

    approved_coins = []
    ai_reason = ""
    if local_candidates and send:
        cands_text = "".join(
            f"Монета {c['coin']}: ТФ {c['tech']['aligned']}/{c['tech']['total']}, перевес {c['tech']['prob']:.0f}%, "
            f"{'LONG' if c['tech']['is_long'] else 'SHORT'}.\n"
            for c in local_candidates
        )
        prompt = f"""Ты профессиональный крипто-трейдер и макро-аналитик.
Оцени макроэкономический фон и реши, какие монеты из списка реально можно брать прямо сейчас.

МИРОВЫЕ НОВОСТИ (ФРС, ставки, инфляция, геополитика):
{world_block}

КРИПТО-НОВОСТИ:
{crypto_block}

Кандидаты по технике (уже прошли фильтр):
{cands_text}

ЖЕСТКИЕ ПРАВИЛА:
1. Если в мировых новостях есть ЯВНЫЙ НЕГАТИВ (ФРС повышает ставки, высокая инфляция, обвал рынков, войны) — отклони ВСЕ LONG-сигналы. Лонг в такой ситуации — ловля падающего ножа.
2. Если в мировых новостях есть ЯВНЫЙ ПОЗИТИВ (ФРС снижает ставки, приток ликвидности, мирные соглашения) — можно одобрять LONG.
3. SHORT сигналы можно одобрять при негативном фоне.
4. Если новости нейтральные — одобряй только самые сильные по технике (перевес >30%).
5. Максимум 4 монеты.
6. Ответь ТОЛЬКО валидным JSON без markdown:
{{"approved": ["BTC", "ETH"], "reason": "краткая причина на русском"}}
Если ничего не одобряешь: {{"approved": [], "reason": "..."}}
"""
        res = await ask_ai_named(prompt, json_mode=True, timeout=25, free_timeout=30)
        if res and res[0]:
            try:
                m = re.search(r"\{.*\}", res[0], re.S)
                data = json.loads(m.group(0))
                approved = [c.upper() for c in data.get("approved", []) if isinstance(c, str)]
                ai_reason = data.get("reason", "")
                cand_map = {c["coin"]: c for c in local_candidates}
                approved_coins = [cand_map[c] for c in approved if c in cand_map][:MAX_ALERTS_PER_SCAN]
            except Exception:
                pass

        if not approved_coins and local_candidates:
            approved_coins = local_candidates[:3]
            ai_reason = "AI не ответил / fallback по силе техники"

    signals = []
    hist = _load_signal_history()

    for cand in approved_coins:
        coin = cand["coin"]
        tech = cand["tech"]
        tf_data = cand["tf_data"]
        info = tf_data[tech["tf"]]
        is_long = tech["is_long"]
        entry, tps, sls = calc_levels(info, is_long)

        side = "🟢 LONG" if is_long else "🔴 SHORT"
        text = (
            f"<b>{side}  ·  {coin}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"ТФ: <b>{tech['tf']}</b>  ·  Сила: <b>{tech['prob']:.0f}%</b>  ·  Совпадение: {tech['aligned']}/{tech['total']}\n\n"
            f"💰 Цена: <code>{fmt(info['price'])}</code>\n"
            f"📥 Вход: <code>{fmt(entry)}</code>\n\n"
            f"🎯 TP1 <code>{fmt(tps[0])}</code>   TP2 <code>{fmt(tps[1])}</code>   TP3 <code>{fmt(tps[2])}</code>\n"
            f"🛡 SL1 <code>{fmt(sls[0])}</code>   SL2 <code>{fmt(sls[1])}</code>\n\n"
            f"<i>{local_reason(tech['tf'], info)}</i>"
        )
        if ai_reason:
            text += f"\n\n🧠 <i>{html.escape(ai_reason[:180])}</i>"

        try:
            img_bytes = await asyncio.to_thread(draw_chart, coin, tf_data, tech["tf"], is_long, entry, tps, sls)
        except Exception:
            img_bytes = None

        signals.append({
            "coin": coin,
            "text": text,
            "img": img_bytes,
            "is_long": is_long,
            "prob": tech["prob"],
            "entry": entry,
        })

        hist.append({
            "ts": time.time(),
            "coin": coin,
            "side": "LONG" if is_long else "SHORT",
            "prob": tech["prob"],
            "entry": entry,
            "text": text,
        })

    hist = [x for x in hist if time.time() - x["ts"] < 24 * 3600][-50:]
    _save_signal_history(hist)

    if signals and send:
        lines = [f"🔥 <b>Сигналы сканера</b> ({datetime.now().strftime('%H:%M')})\n"]
        kb_rows = []
        for s in signals:
            lines.append(f"{'🟢' if s['is_long'] else '🔴'} <b>{s['coin']}</b> — {s['prob']:.0f}% | вход {fmt(s['entry'])}")
            kb_rows.append([InlineKeyboardButton(text=f"{'🟢' if s['is_long'] else '🔴'} {s['coin']}", callback_data=f"c:{s['coin']}")])

        lines.append(f"\n<i>{html.escape(ai_reason[:150]) if ai_reason else ''}</i>")
        text = "\n".join(lines)
        kb_rows.append([InlineKeyboardButton(text="📋 Меню", callback_data="menu")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

        last_signals_data = {"text": text, "kb": kb, "ts": time.time(), "signals": signals}

        payload = {"kind": "text", "text": text, "markup": kb}
        sent, failed, removed, _ = await broadcast(payload)
        stats["signals"] += len(signals)
        stats["alerts_sent"] += sent
        log.info(f"Сканер: {len(signals)} сигналов, отправлено {sent}, время {time.time()-t0:.1f}с")

    return signals


async def scanner_loop():
    await asyncio.sleep(15)
    while True:
        try:
            if bot_flags.get("alerts_enabled", True):
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner_loop: {e}", exc_info=True)
        await asyncio.sleep(SCAN_INTERVAL)


# ----------------------------------------------------------------------------
# ОТРИСОВКА ГРАФИКА
# ----------------------------------------------------------------------------
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    df = tf_data[main_tf]["df"].tail(60).reset_index(drop=True)
    width, height = 800, 450
    img = Image.new("RGB", (width, height), (18, 18, 24))
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
        font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
    except Exception:
        font = ImageFont.load_default()
        font_b = font

    side = "LONG" if is_long else "SHORT"
    color = (0, 200, 80) if is_long else (220, 60, 60)
    draw.text((20, 12), f"{coin}  {main_tf}  {side}", fill=color, font=font_b)
    draw.text((20, 32), f"Entry {fmt(entry)}", fill=(200, 200, 200), font=font)

    pad_l, pad_r, pad_t, pad_b = 60, 20, 60, 40
    chart_w = width - pad_l - pad_r
    chart_h = height - pad_t - pad_b

    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    opens = df["open"].values
    min_p = min(lows.min(), entry, min(sls), min(tps)) * 0.998
    max_p = max(highs.max(), entry, max(sls), max(tps)) * 1.002
    price_range = max_p - min_p or 1

    def y(p):
        return pad_t + chart_h - (p - min_p) / price_range * chart_h

    for i in range(5):
        py = pad_t + i * chart_h / 4
        draw.line([(pad_l, py), (width - pad_r, py)], fill=(40, 40, 50), width=1)

    n = len(df)
    candle_w = max(3, chart_w // n - 2)
    for i in range(n):
        x = pad_l + i * (chart_w / n) + candle_w // 2
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        col = (0, 180, 80) if c >= o else (200, 50, 50)
        draw.line([(x, y(h)), (x, y(l))], fill=col, width=1)
        top = min(y(o), y(c))
        bot = max(y(o), y(c))
        draw.rectangle([x - candle_w // 2, top, x + candle_w // 2, bot], fill=col)

    def draw_level(price, label, col, style="solid"):
        yy = y(price)
        if style == "dashed":
            for xx in range(pad_l, width - pad_r, 8):
                draw.line([(xx, yy), (xx + 4, yy)], fill=col, width=1)
        else:
            draw.line([(pad_l, yy), (width - pad_r, yy)], fill=col, width=2)
        draw.text((pad_l + 4, yy - 14), f"{label} {fmt(price)}", fill=col, font=font)

    draw_level(entry, "Entry", (255, 200, 0))
    for i, tp in enumerate(tps):
        draw_level(tp, f"TP{i+1}", (0, 200, 100), "dashed")
    for i, sl in enumerate(sls[:2]):
        draw_level(sl, f"SL{i+1}", (220, 60, 60), "dashed")

    for i in range(1, n):
        x1 = pad_l + (i - 1) * (chart_w / n) + candle_w // 2
        x2 = pad_l + i * (chart_w / n) + candle_w // 2
        draw.line([(x1, y(df["ema20"].iloc[i-1])), (x2, y(df["ema20"].iloc[i]))], fill=(80, 140, 255), width=1)
        draw.line([(x1, y(df["ema50"].iloc[i-1])), (x2, y(df["ema50"].iloc[i]))], fill=(180, 100, 255), width=1)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ----------------------------------------------------------------------------
# BROADCAST
# ----------------------------------------------------------------------------
async def broadcast(payload, only_ids=None):
    targets = only_ids if only_ids is not None else list(active_chats)
    sent = failed = removed = 0
    removed_ids = []

    for chat_id in targets:
        try:
            if payload.get("kind") == "photo" and payload.get("data"):
                photo = BufferedInputFile(payload["data"], filename=payload.get("filename", "img.png"))
                if len(payload.get("text", "")) <= 1024:
                    await bot.send_photo(chat_id, photo, caption=payload["text"], parse_mode="HTML",
                                         reply_markup=payload.get("markup"))
                else:
                    await bot.send_photo(chat_id, photo)
                    await bot.send_message(chat_id, payload["text"][:4000], parse_mode="HTML",
                                           reply_markup=payload.get("markup"), disable_web_page_preview=True)
            else:
                await bot.send_message(chat_id, payload.get("text", "")[:4000], parse_mode="HTML",
                                       reply_markup=payload.get("markup"), disable_web_page_preview=True)
            sent += 1
            await asyncio.sleep(0.05)
        except (TelegramForbiddenError, TelegramNotFound):
            removed += 1
            removed_ids.append(chat_id)
            active_chats.discard(chat_id)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                if payload.get("kind") == "photo" and payload.get("data"):
                    photo = BufferedInputFile(payload["data"], filename=payload.get("filename", "img.png"))
                    await bot.send_photo(chat_id, photo, caption=payload.get("text", "")[:1024], parse_mode="HTML",
                                         reply_markup=payload.get("markup"))
                else:
                    await bot.send_message(chat_id, payload.get("text", "")[:4000], parse_mode="HTML",
                                           reply_markup=payload.get("markup"), disable_web_page_preview=True)
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1

    if removed_ids:
        set_users_active(removed_ids, False)
    stats["broadcasts"] += 1
    return sent, failed, removed, removed_ids


# ----------------------------------------------------------------------------
# АНАЛИЗ МОНЕТЫ
# ----------------------------------------------------------------------------
async def analyze_coin(coin, user_id=None):
    try:
        tf_data, source = await get_market_data(coin)
    except LookupError:
        return None, f"⚠️ Монета <b>{coin}</b> не найдена на биржах."
    except Exception as e:
        return None, f"⚠️ Ошибка данных: {html.escape(str(e))}"

    best_tf = max(tf_data.keys(), key=lambda t: abs(tf_data[t]["long_prob"] - 50))
    info = tf_data[best_tf]
    is_long = info["long_prob"] >= 50
    entry, tps, sls = calc_levels(info, is_long)

    prompt = f"""Монета {coin}. Данные:
15m: long_prob={tf_data['15m']['long_prob']:.0f}% RSI={tf_data['15m']['rsi']:.0f}
1h: long_prob={tf_data['1h']['long_prob']:.0f}% RSI={tf_data['1h']['rsi']:.0f}
4h: long_prob={tf_data['4h']['long_prob']:.0f}% RSI={tf_data['4h']['rsi']:.0f}
1d: long_prob={tf_data['1d']['long_prob']:.0f}% RSI={tf_data['1d']['rsi']:.0f}

Доминирующий ТФ: {best_tf}. Направление: {'LONG' if is_long else 'SHORT'}.
Напиши кратко (3-5 предложений) на русском: почему сейчас {'лонг' if is_long else 'шорт'}, ключевые уровни, риски. Без markdown."""
    ai_text, engine = await ask_ai_named(prompt, validate=has_russian, timeout=12, free_timeout=18)
    reason = ai_to_html(ai_text) if ai_text else local_reason(best_tf, info)
    tag = ENGINE_TAG.get(engine or "local", "l")

    side_emoji = "🟢 LONG" if is_long else "🔴 SHORT"
    side_text = "LONG" if is_long else "SHORT"
    prob = max(info["long_prob"], 100 - info["long_prob"])

    text = (
        f"<b>{side_emoji}  ·  {coin}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Источник: <code>{source}</code>  ·  ТФ: <b>{best_tf}</b>  ·  Вероятность: <b>{prob:.0f}%</b>\n\n"
        f"💰 <b>Текущая цена:</b>  <code>{fmt(info['price'])}</code>\n"
        f"📥 <b>Рекомендуемый вход:</b>  <code>{fmt(entry)}</code>\n\n"
        f"🎯 <b>Тейк-профиты</b>\n"
        f"   TP1  <code>{fmt(tps[0])}</code>\n"
        f"   TP2  <code>{fmt(tps[1])}</code>\n"
        f"   TP3  <code>{fmt(tps[2])}</code>\n\n"
        f"🛡 <b>Стоп-лоссы</b>\n"
        f"   SL1  <code>{fmt(sls[0])}</code>\n"
        f"   SL2  <code>{fmt(sls[1])}</code>\n\n"
        f"📌 <b>Анализ</b>\n"
        f"{reason}\n\n"
        f"<i>{tag}</i>"
    )

    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, best_tf, is_long, entry, tps, sls)
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
        "side": side_text,
        "prob": prob,
        "price": info["price"],
    }, None


# ----------------------------------------------------------------------------
# МОНИТОРИНГ ОТКРЫТЫХ ПОЗИЦИЙ
# ----------------------------------------------------------------------------
async def position_monitor_loop():
    await asyncio.sleep(40)
    while True:
        try:
            users = _load_users()
            for uid_str, u in users.items():
                open_pos = u.get("open_positions") or {}
                if not open_pos:
                    continue
                for coin, pos in list(open_pos.items()):
                    try:
                        await check_and_notify_position(int(uid_str), coin, pos)
                    except Exception as e:
                        log.warning(f"pos monitor {uid_str}/{coin}: {e}")
                    await asyncio.sleep(0.3)
        except Exception as e:
            log.error(f"position_monitor_loop: {e}")
        await asyncio.sleep(360)


async def check_and_notify_position(user_id, coin, pos):
    if pos.get("notified_close"):
        return

    price = await get_simple_price(coin)
    if not price:
        return

    side = pos["side"]
    entry = pos["entry_price"]
    tp1, tp2, tp3 = pos["tp1"], pos["tp2"], pos["tp3"]
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
        reason = (
            f"Сработал стоп-лосс (SL1 = {fmt(sl1)}).\n"
            f"Цена ушла против позиции. Рекомендую закрыть, чтобы не увеличивать убыток."
        )
        urgency = "danger"
    elif hit_tp3:
        reason = (
            f"Достигнут TP3 ({fmt(tp3)}).\n"
            f"Цель полностью отработана. Можно фиксировать всю позицию."
        )
        urgency = "success"
    elif hit_tp2 and not pos.get("notified_tp"):
        reason = (
            f"Достигнут TP2 ({fmt(tp2)}).\n"
            f"Хороший момент частично зафиксировать прибыль (например 50–70%) и перенести стоп в безубыток."
        )
        urgency = "success"
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
    elif hit_tp1 and not pos.get("notified_tp"):
        reason = (
            f"Достигнут TP1 ({fmt(tp1)}).\n"
            f"Можно закрыть часть позиции и перенести стоп ближе к входу."
        )
        urgency = "success"
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)

    if not reason:
        try:
            tf_data, _ = await get_market_data_light(coin)
            tf4 = tf_data.get("4h") or list(tf_data.values())[-1]
            long_prob = tf4["long_prob"]
            rsi = tf4["rsi"]

            if side == "LONG" and long_prob < 30 and rsi > 65:
                reason = (
                    f"На 4h вероятность лонга упала до {long_prob:.0f}%, RSI {rsi:.0f}.\n"
                    f"Появились признаки разворота. Лучше зафиксировать текущий результат ({pnl_pct:+.1f}%)."
                )
                urgency = "warning"
            elif side == "SHORT" and long_prob > 70 and rsi < 35:
                reason = (
                    f"На 4h вероятность лонга выросла до {long_prob:.0f}%, RSI {rsi:.0f}.\n"
                    f"Медвежий импульс ослаб. Рекомендую закрыть шорт ({pnl_pct:+.1f}%)."
                )
                urgency = "warning"
        except Exception:
            pass

    if not reason:
        pos["last_price"] = price
        set_user_open_position(user_id, coin, pos)
        return

    emoji = {"danger": "🛑", "success": "✅", "warning": "⚠️", "info": "ℹ️"}.get(urgency, "ℹ️")
    text = (
        f"{emoji} <b>Рекомендация по позиции {coin}</b>\n\n"
        f"Направление: <b>{side}</b>\n"
        f"Вход: <code>{fmt(entry)}</code>\n"
        f"Сейчас: <code>{fmt(price)}</code>\n"
        f"PnL: <b>{pnl_pct:+.2f}%</b>\n\n"
        f"<b>Почему сейчас:</b>\n{reason}"
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


# ----------------------------------------------------------------------------
# ХЕНДЛЕРЫ
# ----------------------------------------------------------------------------
@dp.message(Command("start", "menu"))
async def cmd_start(message: types.Message):
    save_user_data(message.from_user.id, message.from_user.username, message.from_user.first_name)
    active_chats.add(message.chat.id)
    await message.answer(
        f"👋 <b>IXCLY v{VERSION}</b>\nВыбери монету или посмотри сигналы:",
        reply_markup=get_coins_keyboard(),
        parse_mode="HTML"
    )


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
        f"Сигналов отправлено: {stats['signals']}\n"
        f"Новостей: {stats['news_sent']}\n"
        f"Алертов: {stats['alerts_sent']}\n"
        f"Аптайм: {uptime // 3600}ч {(uptime % 3600) // 60}м\n"
        f"Mongo: {'✅' if users_col else '❌'}"
    )
    await message.answer(text, parse_mode="HTML")


@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await cb.message.edit_text(
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
        await cb.answer("Анализирую…")
        res, err = await analyze_coin(coin, uid)
        if err:
            await cb.message.answer(err, parse_mode="HTML")
            return
        if res["img"]:
            await send_with_image(cb.message.chat.id, res["text"], res["img"], f"{coin}.png", result_keyboard(coin))
        else:
            await cb.message.answer(res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin), disable_web_page_preview=True)
        result_cache[uid] = {"coin": coin, "text": res["text"], "ts": time.time()}
    finally:
        busy_users.discard(uid)


@dp.callback_query(F.data.startswith("r:"))
async def cb_refresh(cb: types.CallbackQuery):
    coin = cb.data.split(":")[1]
    await cb_coin(cb)


@dp.callback_query(F.data == "last_signals")
async def cb_last_signals(cb: types.CallbackQuery):
    if last_signals_data.get("text") and time.time() - last_signals_data.get("ts", 0) < 3600:
        await cb.message.answer(last_signals_data["text"], parse_mode="HTML", reply_markup=last_signals_data.get("kb"))
    else:
        hist = _load_signal_history()
        if not hist:
            await cb.answer("Свежих сигналов нет", show_alert=True)
            return
        text = "📜 <b>История сигналов за 24ч</b>\n\n"
        for h in hist[-10:][::-1]:
            side = "🟢" if h.get("side") == "LONG" else "🔴"
            text += f"{side} <b>{h['coin']}</b> {h.get('prob', 0):.0f}% вход {fmt(h.get('entry', 0))}\n"
        await cb.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 Меню", callback_data="menu")]]))
    await cb.answer()


@dp.callback_query(F.data == "show_stats")
async def cb_show_stats(cb: types.CallbackQuery):
    await cb.answer("Собираю статистику…")
    text = await build_daily_stats()
    await cb.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 Меню", callback_data="menu")]]))


@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting(cb: types.CallbackQuery):
    await cb.answer("Ищу интересные графики…")
    gainers, losers = await get_interesting_coins()
    text = "🧐 <b>Интересные графики за 24ч</b>\n\n"
    text += "🟢 <b>Топ роста:</b>\n"
    for g in gainers:
        coin = g["symbol"].replace("USDT", "")
        ch = float(g["priceChangePercent"])
        text += f"<code>{coin}</code> +{ch:.1f}%\n"
    text += "\n🔴 <b>Топ падения:</b>\n"
    for l in losers:
        coin = l["symbol"].replace("USDT", "")
        ch = float(l["priceChangePercent"])
        text += f"<code>{coin}</code> {ch:.1f}%\n"
    text += "\n<i>Нажми на монету в меню для полного анализа.</i>"
    await cb.message.answer(text, parse_mode="HTML", reply_markup=get_coins_keyboard())


@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await cb.message.answer("🔍 Напиши тикер монеты (например BTC или SOL):")
    await cb.answer()


@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,10}$"))
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
            await send_with_image(message.chat.id, res["text"], res["img"], f"{coin}.png", result_keyboard(coin))
        else:
            await message.answer(res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin), disable_web_page_preview=True)
        result_cache[uid] = {"coin": coin, "text": res["text"], "ts": time.time()}
    finally:
        busy_users.discard(uid)


# --- ИЗБРАННОЕ ---
@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("⭐ <b>Избранные сигналы</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), parse_mode="HTML")
    await cb.answer()


@dp.callback_query(F.data == "sig:save_new")
async def cb_sig_save_new(cb: types.CallbackQuery):
    await cb.message.edit_text("Выбери монету для сохранения:", reply_markup=get_save_coins_keyboard())
    await cb.answer()


@dp.callback_query(F.data.startswith("sig:save_page:"))
async def cb_sig_save_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[2])
    await cb.message.edit_reply_markup(reply_markup=get_save_coins_keyboard(page))
    await cb.answer()


@dp.callback_query(F.data.startswith("sig:do_save:"))
async def cb_sig_do_save(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    uid = cb.from_user.id
    cached = result_cache.get(uid)
    if cached and cached.get("coin") == coin and time.time() - cached.get("ts", 0) < 300:
        text = cached["text"]
        photo_id = None
    else:
        res, err = await analyze_coin(coin)
        if err:
            await cb.answer(err[:100], show_alert=True)
            return
        text = res["text"]
        photo_id = None
    add_user_saved_signal(uid, coin, text, photo_id)
    await cb.answer(f"✅ {coin} сохранён")
    await cb.message.edit_text("⭐ <b>Избранные сигналы</b>", reply_markup=get_saved_signals_menu(uid), parse_mode="HTML")


@dp.callback_query(F.data.startswith("sig:quick_save:"))
async def cb_sig_quick_save(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    uid = cb.from_user.id
    cached = result_cache.get(uid)
    text = cached["text"] if cached and cached.get("coin") == coin else f"Сигнал {coin}"
    add_user_saved_signal(uid, coin, text, None)
    await cb.answer(f"✅ {coin} добавлен в избранное")


@dp.callback_query(F.data.startswith("sig:view:"))
async def cb_sig_view(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    saved = get_user_saved_signals(cb.from_user.id)
    item = next((s for s in saved if s["coin"] == coin), None)
    if not item:
        await cb.answer("Не найдено", show_alert=True)
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
    await cb.message.edit_text("Выбери сигнал для удаления:", reply_markup=get_delete_signals_menu(cb.from_user.id))
    await cb.answer()


@dp.callback_query(F.data.startswith("sig:del:"))
async def cb_sig_del(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    del_user_saved_signal(cb.from_user.id, coin)
    await cb.answer(f"🗑 {coin} удалён")
    await cb.message.edit_text("⭐ <b>Избранные сигналы</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), parse_mode="HTML")


# --- ПОЗИЦИИ ---
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
        f"Я буду следить за позицией и сообщу, когда будет лучше закрыть.",
        parse_mode="HTML",
        reply_markup=get_saved_signal_view_keyboard(uid, coin)
    )
    await cb.answer("✅ Позиция зафиксирована")


@dp.callback_query(F.data.startswith("pos:close:"))
async def cb_pos_close(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    set_user_open_position(cb.from_user.id, coin, None)
    await cb.answer("❌ Позиция закрыта вручную")
    await cb.message.edit_reply_markup(reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin))


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


@dp.callback_query(F.data == "ignore")
async def cb_ignore(cb: types.CallbackQuery):
    await cb.answer()


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
async def main():
    global http, analysis_sem
    keep_alive()
    init_mongo()
    load_active_chats()
    _load_news_history()
    _load_alert_history()

    http = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers={"User-Agent": "IXCLY/2.10"}
    )
    analysis_sem = asyncio.Semaphore(3)

    asyncio.create_task(news_watcher())
    asyncio.create_task(scanner_loop())
    asyncio.create_task(daily_stats_loop())
    asyncio.create_task(position_monitor_loop())

    runtime["main_bot"] = "запущен"
    log.info(f"🚀 IXCLY v{VERSION} стартовал. Получателей: {len(active_chats)}")

    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        await http.close()
        await bot.session.close()
        if input_bot:
            await input_bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен")
