# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v2.10.33
"""

import asyncio
import html
import io
import json
import logging
import os
import re
import signal
import sys
import time
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
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter, TelegramConflictError, TelegramNetworkError
from aiogram.client.session.aiohttp import AiohttpSession
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFont

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
                log.warning("ℹ getUpdates-конфликт: рядом работает другой экземпляр бота.")
            return False
        if "Sleep for" in msg and "try again" in msg:
            return False
        return True


logging.getLogger("aiogram.dispatcher").addFilter(_ConflictFilter())

# ====================== ВЕБ ======================
app = Flask(__name__)


@app.route("/")
def home():
    return "Bot is alive!"


@app.route("/health")
def health():
    return "ok"


@app.route("/status")
def status_page():
    return jsonify({
        "version": VERSION,
        "uptime_s": int(time.time() - stats["started"]),
        "recipients": len(active_chats),
        "main_bot": runtime["main_bot"],
        "admin_bot": runtime["admin_bot"],
        "news_engine": market_bias.get("engine"),
        "news_title": market_bias.get("title_ru", "")[:80],
    })


def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 10000)) 
        log.info(f"🌐 Flask keep-alive запущен на порту {port}")
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"Flask: {e}")


_web_started = False


def keep_alive():
    global _web_started
    if not _web_started:
        _web_started = True
        Thread(target=_run_web, daemon=True).start()


# ====================== НАСТРОЙКИ ======================
def clean_key(k):
    return k.strip().strip('"').strip("'") if k else None


BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
MONGO_URI = clean_key(os.getenv("MONGO_URI"))

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

ADMIN_IDS = set()
if ADMIN_ID:
    for x in re.split(r"[,\s]+", ADMIN_ID):
        if x.lstrip("-").isdigit():
            ADMIN_IDS.add(int(x))

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не задан")

VERSION = "2.10.33"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}

POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

session = AiohttpSession(timeout=30.0)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    admin_session = AiohttpSession(timeout=30.0)
    admin_bot = Bot(token=INPUT_BOT_TOKEN, session=admin_session)
    admin_dp = Dispatcher()
    log.info("Админ-бот активирован")
elif INPUT_BOT_TOKEN:
    log.warning("INPUT_BOT_TOKEN совпадает с BOT_TOKEN: админ-бот выключен")

http = None
news_lock = None

TFS = ["15m", "1h", "4h", "1d"]
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
    "DRIFT", "KMNO", "WEN", "MYRO", "POPCAT", "GIGA", "MEW", "SLERF", "LUNC", "LUNA", "USTC",
    "CTSI", "COTI", "CELO", "CKB", "ONE", "QTUM", "IOST", "ONT", "ICX", "WAN", "ARK", "STRAX"
]

SYMBOL_ALIASES = {
    "RNDR": ["RENDER", "RNDR"], "MATIC": ["POL", "MATIC"],
    "PEPE": ["1000PEPE", "PEPE"], "BONK": ["1000BONK", "BONK"],
    "FLOKI": ["1000FLOKI", "FLOKI"], "SATS": ["1000SATS", "SATS"],
}

USERS_FILE = "users.json"
SIGNAL_HISTORY_FILE = "signal_history.json"

active_chats = set()
busy_users = set()
result_cache = {}
last_signals_data = {"text": "", "kb": None, "ts": 0}
current_signals_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "started": time.time()}
bot_flags = {"news_enabled": True, "alerts_enabled": True}
market_bias = {
    "bias": 0,
    "title_ru": "Нет данных",
    "summary": "Нет данных",
    "impact": "Нейтральный фон",
    "coins": "BTC, ETH, SOL",
    "published": "",
    "link": "",
    "engine": "none",
    "ts": 0,
}
last_news_link = None
users_dirty = False
last_backup_msg = {}
last_user_msg = {} 

next_timers = {"scanner": 0, "positions": 0, "news_watcher": 0, "backup": 0}
users_col = None


def init_mongo():
    global users_col
    users_col = None
    log.warning("⚠ MongoDB отключена. Пользователи хранятся в users.json")


# ====================== ПОЛЬЗОВАТЕЛИ ======================
def _load_users():
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_users(users):
    global users_dirty
    try:
        with open(USERS_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        os.replace(USERS_FILE + ".tmp", USERS_FILE)
        users_dirty = True
    except Exception as e:
        log.error(f"Ошибка записи users.json: {e}")


def save_user_data(user_id, username, first_name):
    key = str(user_id)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    users = _load_users()
    if key not in users:
        users[key] = {"username": username, "first_name": first_name, "joined_at": now, "active": True}
    else:
        users[key]["username"] = username
        users[key]["first_name"] = first_name
        users[key]["active"] = True
    _save_users(users)


def mark_users_inactive(ids):
    users = _load_users()
    changed = False
    for i in ids:
        k = str(i)
        if k in users and users[k].get("active", True):
            users[k]["active"] = False
            changed = True
    if changed:
        _save_users(users)


def get_user_saved_signals(user_id):
    return _load_users().get(str(user_id), {}).get("saved_signals", [])


def add_user_saved_signal(user_id, coin, text):
    key = str(user_id)
    users = _load_users()
    if key not in users:
        users[key] = {"active": True}
    users[key].setdefault("saved_signals", [])
    users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
    users[key]["saved_signals"].append({"coin": coin, "text": text})
    _save_users(users)


def del_user_saved_signal(user_id, coin):
    key = str(user_id)
    users = _load_users()
    if key in users and "saved_signals" in users[key]:
        users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
        _save_users(users)


def get_user_open_positions(user_id):
    return _load_users().get(str(user_id), {}).get("open_positions", {})


def set_user_open_position(user_id, coin, data):
    key = str(user_id)
    users = _load_users()
    if key not in users:
        users[key] = {}
    users[key].setdefault("open_positions", {})
    if data is None:
        users[key]["open_positions"].pop(coin, None)
    else:
        users[key]["open_positions"][coin] = data
    _save_users(users)


def get_open_position(user_id, coin):
    return get_user_open_positions(user_id).get(coin)


def load_active_chats():
    active_chats.clear()
    for uid, u in _load_users().items():
        if isinstance(u, dict) and u.get("active", True):
            try:
                active_chats.add(int(uid))
            except Exception:
                pass
    log.info("Получателей рассылки: %d", len(active_chats))


def user_counts():
    users = _load_users()
    total = len(users)
    active = sum(1 for u in users.values() if isinstance(u, dict) and u.get("active", True))
    return total, active, 0, 0


def _load_signal_history():
    try:
        with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [x for x in data if time.time() - x.get("ts", 0) < 86400]
    except Exception:
        return []


def _save_signal_history(data):
    try:
        with open(SIGNAL_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data[-40:], f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ====================== БЭКАП БАЗЫ ======================
async def telegram_backup(reason="auto"):
    if not admin_bot or not ADMIN_IDS:
        return False
    users = _load_users()
    if not users:
        return False
    data = json.dumps(users, ensure_ascii=False).encode("utf-8")
    ok = False
    for aid in ADMIN_IDS:
        try:
            msg = await admin_bot.send_document(
                aid, BufferedInputFile(data, "users.json"),
                caption=f"💾 Бэкап базы · {len(users)} польз. · {reason} · {datetime.now().strftime('%d.%m %H:%M')}",
                disable_notification=True)
            try:
                await admin_bot.unpin_all_chat_messages(aid)
                await admin_bot.pin_chat_message(aid, msg.message_id, disable_notification=True)
            except Exception:
                pass
            old = last_backup_msg.get(aid)
            if old:
                try:
                    await admin_bot.delete_message(aid, old)
                except Exception:
                    pass
            last_backup_msg[aid] = msg.message_id
            ok = True
        except Exception as e:
            log.warning(f"Бэкап админу {aid} не отправлен: {e}")
    return ok


async def telegram_restore():
    if not admin_bot or not ADMIN_IDS:
        return 0
    for aid in ADMIN_IDS:
        try:
            chat = await admin_bot.get_chat(aid)
            pm = getattr(chat, "pinned_message", None)
            doc = getattr(pm, "document", None) if pm else None
            if not doc or not (doc.file_name or "").endswith(".json"):
                continue
            f = await admin_bot.get_file(doc.file_id)
            buf = await admin_bot.download_file(f.file_path)
            data = json.loads(buf.read().decode("utf-8"))
            if isinstance(data, dict) and data:
                current = _load_users()
                for k, v in data.items():
                    if k not in current:
                        current[k] = v
                    else:
                        for field in ["open_positions", "saved_signals"]:
                            if field in v:
                                if field not in current[k]:
                                    current[k][field] = v[field]
                                else:
                                    if isinstance(v[field], dict) and isinstance(current[k][field], dict):
                                        for sub_k, sub_v in v[field].items():
                                            if sub_k not in current[k][field]:
                                                current[k][field][sub_k] = sub_v
                                    elif isinstance(v[field], list) and isinstance(current[k][field], list):
                                        existing = {item.get('coin') for item in current[k][field] if isinstance(item, dict)}
                                        for item in v[field]:
                                            if isinstance(item, dict) and item.get('coin') not in existing:
                                                current[k][field].append(item)
                _save_users(current)
                last_backup_msg[aid] = pm.message_id
                return len(data)
        except Exception as e:
            log.warning(f"Восстановление не удалось: {e}")
    return 0


async def backup_loop():
    global users_dirty
    await asyncio.sleep(60)
    while True:
        try:
            if users_dirty and await telegram_backup("авто"):
                users_dirty = False
        except Exception as e:
            log.warning(f"backup_loop: {e}")
        next_timers["backup"] = time.time() + 60
        await asyncio.sleep(60)


# ====================== МИДЛВАРЬ ======================
class UserTrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
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

# ====================== ОБРАБОТЧИК ОШИБОК ======================
@dp.errors()
async def errors_handler(update: types.Update, exception: Exception):
    if isinstance(exception, TelegramNetworkError):
        log.warning(f"Сетевая ошибка Telegram: {exception}")
        try:
            if update.message:
                await update.message.answer("⏳ Пожалуйста, подождите... Соединение нестабильно.")
            elif update.callback_query:
                await update.callback_query.answer("⏳ Пожалуйста, подождите...", show_alert=True)
        except Exception:
            pass
        return True
    return False

# ====================== КЛАВИАТУРЫ ======================
MENU_KB = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]])

def get_coins_keyboard():
    buttons = [
        [InlineKeyboardButton(text="📰  Новости", callback_data="news_menu")],
        [InlineKeyboardButton(text="🧐  Интересные графики", callback_data="interesting_menu")],
        [InlineKeyboardButton(text="🔥  Последние сигналы", callback_data="last_signals")],
        [InlineKeyboardButton(text="⭐  Избранное", callback_data="sig:menu")],
        [InlineKeyboardButton(text="📊  Статистика 24ч", callback_data="show_stats")],
        [InlineKeyboardButton(text="🔍  Поиск монеты", callback_data="search")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_search_coins_keyboard(page=0):
    per = 8
    coins = POPULAR_COINS[page * per:(page + 1) * per]
    buttons = []
    for i in range(0, len(coins), 2):
        row = [InlineKeyboardButton(text=f"• {coins[i]}", callback_data=f"c:{coins[i]}")]
        if i + 1 < len(coins):
            row.append(InlineKeyboardButton(text=f"• {coins[i + 1]}", callback_data=f"c:{coins[i + 1]}"))
        buttons.append(row)
    total = (len(POPULAR_COINS) + per - 1) // per
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅ Назад", callback_data=f"search_page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1} / {total}", callback_data="ignore"))
    if page < total - 1:
        nav.append(InlineKeyboardButton(text="Вперёд ➡", callback_data=f"search_page:{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def result_keyboard(coin):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"r:{coin}"),
            InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sig:quick_save:{coin}"),
        ],
        [InlineKeyboardButton(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
    ])

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
    buttons.append([InlineKeyboardButton(text="➕ Добавить новый", callback_data="sig:save_new")])
    if saved:
        buttons.append([InlineKeyboardButton(text="🗑 Удалить", callback_data="sig:del_menu")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
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
    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="sig:menu")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_save_coins_keyboard(page=0):
    per = 8
    coins = POPULAR_COINS[page * per:(page + 1) * per]
    buttons = []
    for i in range(0, len(coins), 2):
        row = [InlineKeyboardButton(text=coins[i], callback_data=f"sig:do_save:{coins[i]}")]
        if i + 1 < len(coins):
            row.append(InlineKeyboardButton(text=coins[i + 1], callback_data=f"sig:do_save:{coins[i + 1]}"))
        buttons.append(row)
    total = (len(POPULAR_COINS) + per - 1) // per
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅", callback_data=f"sig:save_page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total}", callback_data="ignore"))
    if page < total - 1:
        nav.append(InlineKeyboardButton(text="➡", callback_data=f"sig:save_page:{page + 1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="sig:menu")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_saved_signal_view_keyboard(user_id, coin):
    pos = get_open_position(user_id, coin)
    buttons = [[InlineKeyboardButton(text="🔄 Обновить анализ", callback_data=f"c:{coin}")]]
    if pos:
        buttons.append([InlineKeyboardButton(text="📈 Статус позиции", callback_data=f"pos:status:{coin}")])
        buttons.append([InlineKeyboardButton(text="❌ Закрыл сделку", callback_data=f"pos:close:{coin}")])
    else:
        buttons.append([InlineKeyboardButton(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад в избранное", callback_data="sig:menu")])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard():
    news = "🟢 ВКЛ" if bot_flags["news_enabled"] else "🔴 ВЫКЛ"
    scan = "🟢 ВКЛ" if bot_flags["alerts_enabled"] else "🔴 ВЫКЛ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
         InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users")],
        [InlineKeyboardButton(text="📰 Новость сейчас", callback_data="admin:news_now"),
         InlineKeyboardButton(text=f"🔔 Новости: {news}", callback_data="admin:toggle_news")],
        [InlineKeyboardButton(text=f"🚨 Сканер: {scan}", callback_data="admin:toggle_scan"),
         InlineKeyboardButton(text="🔍 Скан-тест", callback_data="admin:scan_test")],
        [InlineKeyboardButton(text="⚡ Рассылка сигналов", callback_data="admin:broadcast_now")],
        [InlineKeyboardButton(text="🔧 Диагностика", callback_data="admin:diag"),
         InlineKeyboardButton(text="💾 Бэкап", callback_data="admin:backup")],
        [InlineKeyboardButton(text="📥 Восстановить", callback_data="admin:restore"),
         InlineKeyboardButton(text="⏳ Таймеры", callback_data="admin:timers")],
    ])


# ====================== ВСПОМОГАТЕЛЬНЫЕ ======================
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

def has_cyr(text, n=1):
    return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= n

def shorten(text, n):
    text = " ".join(str(text).split())
    if len(text) <= n:
        return text
    cut = text[:n]
    m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if m >= n * 0.5:
        return cut[:m + 1]
    return cut.rsplit(" ", 1)[0].rstrip(",;:-— ") + "…"

def make_strength_bar(strength: float, length: int = 10) -> str:
    filled = max(0, min(length, int(round(strength / 100 * length))))
    return "🟩" * filled + "🟥" * (length - filled)

def get_leverage_recommendation(strength: float, sig_type: str) -> tuple:
    if sig_type == "Быстрый сигнал":
        if strength >= 80: return "X25–X50", "1–2%"
        elif strength >= 70: return "X15–X35", "1–2%"
        else: return "X10–X25", "1–1.5%"
    else:
        if strength >= 80: return "X15–X30", "1–2%"
        elif strength >= 70: return "X10–X20", "1–1.5%"
        else: return "X5–X15", "0.5–1%"

def get_moscow_time_from_entry(entry) -> str:
    msk = timezone(timedelta(hours=3))
    try:
        published = None
        if getattr(entry, "published_parsed", None):
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "updated_parsed", None):
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "published", None):
            try:
                published = parsedate_to_datetime(entry.published)
                if published.tzinfo is None:
                    published = published.replace(tzinfo=timezone.utc)
            except Exception:
                pass
        if published:
            return published.astimezone(msk).strftime("%d.%m.%Y %H:%M") + " МСК"
    except Exception:
        pass
    return datetime.now(msk).strftime("%d.%m.%Y %H:%M") + " МСК"

async def first_success(coros, timeout=11):
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

async def http_json(url, params=None, timeout=8):
    async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.json(content_type=None)

async def download_bytes(url):
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                return await r.read()
    except Exception:
        pass
    return None

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

async def safe_answer(cb, text=None, alert=False):
    try:
        await cb.answer(text, show_alert=alert)
    except Exception:
        pass

async def cleanup_prev_menu(user_id):
    old = last_user_msg.pop(user_id, None)
    if old:
        await safe_delete(old)

async def safe_edit(message: types.Message, text: str, reply_markup=None, parse_mode="HTML", user_id=None):
    try:
        if message.photo or message.video or message.document or message.animation:
            await message.delete()
            new_msg = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id:
                last_user_msg[user_id] = new_msg
            return new_msg
        else:
            await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            return message
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            return message
        if "no text in the message" in err or "there is no text" in err:
            try:
                await message.delete()
            except Exception:
                pass
            new_msg = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id:
                last_user_msg[user_id] = new_msg
            return new_msg
        return message
    except Exception:
        return message


# ====================== ИНДИКАТОРЫ ======================
def add_indicators(df):
    c = df["close"]
    df["ema20"] = c.ewm(span=20, adjust=False).mean()
    df["ema50"] = c.ewm(span=50, adjust=False).mean()
    df["ema200"] = c.ewm(span=200, adjust=False).mean()
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi"] = (100 - 100 / (1 + rs)).fillna(50)
    prev = c.shift()
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev).abs(), (df["low"] - prev).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean().fillna(tr.mean())
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["macd_hist"] = macd - macd.ewm(span=9, adjust=False).mean()
    return df

def calc_probability(df):
    ema20 = float(df["ema20"].iloc[-1])
    ema50 = float(df["ema50"].iloc[-1]) or 1e-9
    rsi = float(df["rsi"].iloc[-1])
    macd_h = float(df["macd_hist"].iloc[-1])
    score = 0.0
    diff = abs(ema20 - ema50) / ema50 * 100
    if ema20 > ema50:
        score += 25 + min(18, diff * 10)
    else:
        score -= 25 + min(18, diff * 10)
    if rsi > 55:
        score += min(22, (rsi - 55) * 1.2)
    elif rsi < 45:
        score -= min(22, (45 - rsi) * 1.2)
    score += 22 if macd_h > 0 else -22
    long_p = max(8.0, min(92.0, 50 + score / 2))
    return round(long_p, 1), round(100 - long_p, 1)

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
        long_p, _ = calc_probability(df)
        out[tf] = {
            "df": df, "price": float(last["close"]),
            "ema20": float(last["ema20"]), "ema50": float(last["ema50"]), "ema200": float(last["ema200"]),
            "rsi": float(last["rsi"]), "atr": float(last["atr"]), "macd_hist": float(last["macd_hist"]),
            "recent_low": float(df["low"].tail(28).min()), "recent_high": float(df["high"].tail(28).max()),
            "long_prob": long_p,
        }
    return out

def _p_binance(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]

def _p_okx(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["data"])]

def _p_bybit(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["result"]["list"])]

EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines",
     "params": lambda c, tf: {"symbol": f"{c}USDT", "interval": tf, "limit": 180}, "parse": _p_binance},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles",
     "params": lambda c, tf: {"instId": f"{c}-USDT", "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf], "limit": "180"}, "parse": _p_okx},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline",
     "params": lambda c, tf: {"category": "spot", "symbol": f"{c}USDT", "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf], "limit": 180}, "parse": _p_bybit},
]

async def _fetch_klines(ex, coin, tf):
    j = await http_json(ex["url"], params=ex["params"](coin, tf), timeout=9)
    rows = ex["parse"](j)
    if len(rows) < MIN_CANDLES:
        raise ValueError("мало")
    return rows

async def fetch_from_exchange(ex, coin):
    results = await asyncio.gather(*[_fetch_klines(ex, coin, tf) for tf in TFS], return_exceptions=True)
    if any(isinstance(r, Exception) for r in results):
        return None
    return await asyncio.to_thread(build_tf_data, dict(zip(TFS, results))), ex["name"]

async def get_market_data(coin):
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        for ex in EXCHANGES:
            try:
                res = await asyncio.wait_for(fetch_from_exchange(ex, cand), timeout=11)
                if res:
                    return res
            except Exception:
                continue
    raise LookupError(coin)

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

def tech_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data:
        return None
    d4 = tf_data["4h"]
    d1 = tf_data["1d"]
    p4, p1 = d4["long_prob"], d1["long_prob"]
    if p4 >= 58 and p1 >= 55:
        is_long = True
        strength = (p4 + p1) / 2
    elif p4 <= 42 and p1 <= 45:
        is_long = False
        strength = ((100 - p4) + (100 - p1)) / 2
    else:
        return None
    
    if strength < 63:
        return None
    
    p15 = tf_data.get("15m", {}).get("long_prob", 50)
    p1h = tf_data.get("1h", {}).get("long_prob", 50)
    
    fast = (is_long and p15 >= 60 and p1h >= 57) or (not is_long and p15 <= 40 and p1h <= 43)
    long_term = (is_long and p1 >= 70) or (not is_long and p1 <= 30)
    
    if long_term and not fast:
        sig_type = "Долгосрок"
    elif fast:
        sig_type = "Быстрый сигнал"
    else:
        sig_type = "Среднесрок"
        
    return {"ok": True, "is_long": is_long, "strength": round(strength, 1), "type": sig_type}

def calc_levels(info, is_long):
    price = info["price"]
    atr = info["atr"] if info["atr"] > 0 else price * 0.012
    ema20 = info["ema20"]
    ema50 = info["ema50"]
    recent_low = info["recent_low"]
    recent_high = info["recent_high"]
    if is_long:
        entry = min(price, max(ema20, ema50))
        if entry > price - 0.2 * atr:
            entry = price - 0.35 * atr
        sl1 = recent_low - 0.4 * atr
        risk = max(entry - sl1, 0.6 * atr)
        tp1 = entry + risk * 1.4
        tp2 = entry + risk * 2.3
        tp3 = entry + risk * 3.5
        sl2 = sl1 - 0.7 * atr
    else:
        entry = max(price, min(ema20, ema50))
        if entry < price + 0.2 * atr:
            entry = price + 0.35 * atr
        sl1 = recent_high + 0.4 * atr
        risk = max(sl1 - entry, 0.6 * atr)
        tp1 = entry - risk * 1.4
        tp2 = entry - risk * 2.3
        tp3 = entry - risk * 3.5
        sl2 = sl1 + 0.7 * atr
    return entry, [tp1, tp2, tp3], [sl1, sl2]


# ====================== ИИ ======================
def _looks_like_junk(text):
    t = (text or "").strip()
    low = t.lower()
    if len(t) < 30: return True
    if low.startswith("<!doctype") or low.startswith("<html"): return True
    if "pollinations" in low or "deprecat" in low or "rate limit" in low: return True
    return False

async def _ask_gemini(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    for key in GEMINI_API_KEYS:
        try:
            async with http.post(url, json=payload, headers={"x-goog-api-key": key}, timeout=aiohttp.ClientTimeout(total=18)) as r:
                j = await r.json(content_type=None)
                if r.status == 200:
                    return j["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            pass
    return None

async def _ask_openai_compat(name, base, key, model, prompt, extra_headers=None):
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra_headers: headers.update(extra_headers)
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1100}
    try:
        async with http.post(f"{base}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json(content_type=None)
            if r.status == 200:
                return j["choices"][0]["message"]["content"]
    except Exception:
        pass
    return None

async def _ask_free_gpt(prompt):
    system = "Ты опытный крипто-аналитик. Отвечай на русском языке, кратко."
    short = quote(prompt[:3000], safe="")
    try:
        async with http.get(f"https://text.pollinations.ai/{short}", params={"model": POLLINATIONS_MODEL, "system": system}, timeout=aiohttp.ClientTimeout(total=14)) as r:
            if r.status == 200:
                text = await r.text()
                if not _looks_like_junk(text):
                    return text
    except Exception:
        pass
    return None

async def ask_ai_named(prompt, timeout=20, validate=None, free_timeout=30):
    providers = []
    if GEMINI_API_KEYS: providers.append(("Gemini", lambda: _ask_gemini(prompt)))
    if GROQ_API_KEY: providers.append(("Groq", lambda: _ask_openai_compat("Groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt)))
    if OPENROUTER_API_KEY: providers.append(("OpenRouter", lambda: _ask_openai_compat("OpenRouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt)))

    async def wrap(name, fn):
        try:
            t = await fn()
        except Exception:
            return None
        if t and len(t) > 20 and (validate is None or validate(t)):
            return t, name
        return None

    if providers:
        res = await first_success([wrap(n, f) for n, f in providers], timeout=timeout)
        if res: return res
    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt))], timeout=free_timeout)
    return res if res else (None, None)

async def run_ai_diag():
    prompt = "Напиши одно предложение о биткоине."
    lines = ["🔧 <b>Проверка ИИ</b>", ""]
    async def timed(name, fn):
        t0 = time.time()
        try: r = await asyncio.wait_for(fn(), 28)
        except Exception: r = None
        return f"{'✅' if r else '❌'} {name} · {int((time.time() - t0) * 1000)} мс"

    tests = []
    if GEMINI_API_KEYS: tests.append(timed("Gemini", lambda: _ask_gemini(prompt)))
    else: lines.append("➖ Gemini: нет ключа")
    if GROQ_API_KEY: tests.append(timed("Groq", lambda: _ask_openai_compat("Groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt)))
    else: lines.append("➖ Groq: нет ключа")
    if OPENROUTER_API_KEY: tests.append(timed("OpenRouter", lambda: _ask_openai_compat("OpenRouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt)))
    else: lines.append("➖ OpenRouter: нет ключа")
    tests.append(timed("Бесплатный GPT", lambda: _ask_free_gpt(prompt)))
    lines += await asyncio.gather(*tests)
    return "\n".join(lines)


# ====================== НОВОСТИ ======================
BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner", "upgrad", "breakout", "rebound", "recover", "accumul")
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall", "liquidat", "fraud", "scam", "delist", "arrest", "dump", "selloff", "declin", "probe", "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
COIN_WORDS = {"bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL", "xrp": "XRP", "bnb": "BNB", "doge": "DOGE", "ada": "ADA", "ton": "TON", "avax": "AVAX", "link": "LINK"}

def news_sentiment(text, threshold=2):
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w.startswith(BULL_STEMS): score += 1
        elif w.startswith(BEAR_STEMS): score -= 1
    if score >= threshold: return 1
    if score <= -threshold: return -1
    return 0

def mentioned_coins(text):
    found = []
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        t = COIN_WORDS.get(w)
        if t and t not in found: found.append(t)
    return found[:4]

async def translate_text(text, target="ru"):
    text = (text or "").strip()
    if not text: return text
    try:
        async with http.get("https://translate.googleapis.com/translate_a/single", params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text[:1800]}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                out = "".join(part[0] for part in data[0] if part and part[0])
                if has_cyr(out, 3): return out
    except Exception: pass
    return text

NEWS_FEEDS = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
WORLD_FEEDS = ["https://feeds.bbci.co.uk/news/business/rss.xml"]
_news_cache = {"ts": 0, "entries": []}
_world_cache = {"ts": 0, "titles": []}

async def _load_feed(url):
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: return []
            text = await r.text()
        feed = await asyncio.to_thread(feedparser.parse, text)
        return list(feed.entries)
    except Exception: return []

async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300: return _news_cache["entries"]
    for url in NEWS_FEEDS:
        entries = await _load_feed(url)
        if entries:
            _news_cache["ts"] = time.time()
            _news_cache["entries"] = entries[:5]
            return _news_cache["entries"]
    return _news_cache["entries"]

async def get_world_titles(n=4):
    if _world_cache["titles"] and time.time() - _world_cache["ts"] < 300: return _world_cache["titles"][:n]
    titles = []
    for url in WORLD_FEEDS:
        entries = await _load_feed(url)
        titles.extend([e.title for e in entries[:3]])
    if titles:
        _world_cache["ts"] = time.time()
        _world_cache["titles"] = titles
    return titles[:n]

def extract_image(entry):
    if getattr(entry, "media_content", None): return entry.media_content[0].get("url")
    if getattr(entry, "media_thumbnail", None): return entry.media_thumbnail[0].get("url")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', getattr(entry, "summary", "") or "")
    return m.group(1) if m else None

def clean_summary(raw):
    t = html.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    return " ".join(t.split())[:700]

def parse_news_ai(text):
    t = (text or "").replace("**", "")
    pat = re.compile(r"(?im)^[\s\-\*•]*(BIAS|TITLE_RU|SUMMARY|IMPACT|COINS)\s*[:：]\s*")
    ms = list(pat.finditer(t))
    out = {}
    for i, m in enumerate(ms):
        end = ms[i + 1].start() if i + 1 < len(ms) else len(t)
        out[m.group(1).upper()] = re.sub(r"\s+", " ", t[m.end():end]).strip()
    return out

def _valid_news_ai(text):
    p = parse_news_ai(text)
    return has_cyr(p.get("TITLE_RU", ""), 6) and has_cyr(p.get("SUMMARY", ""), 20)

async def update_market_bias(force=False):
    global market_bias
    async with news_lock:
        try:
            entries = await get_news_entries()
            if not entries: return
            entry = entries[0]
            link = getattr(entry, "link", "")
            now = time.time()
            if not force and market_bias.get("link") == link: return

            title = html.unescape(getattr(entry, "title", "") or "")
            summary = clean_summary(getattr(entry, "summary", ""))
            published = get_moscow_time_from_entry(entry)
            
            prompt = f"Новость: {title}\nТекст: {summary}\nПереведи на русский и разбери в формате:\nBIAS: BULLISH/BEARISH/NEUTRAL\nTITLE_RU: перевод\nSUMMARY: суть\nIMPACT: влияние\nCOINS: тикеры"
            text, engine = await ask_ai_named(prompt, validate=_valid_news_ai)
            parsed = parse_news_ai(text) if text else {}

            if text and _valid_news_ai(text):
                up = parsed.get("BIAS", "").upper()
                bias = 1 if "BULL" in up else -1 if "BEAR" in up else 0
                title_ru = parsed["TITLE_RU"][:170]
                summ = shorten(parsed["SUMMARY"], 380)
                impact = shorten(parsed["IMPACT"], 260)
                coins = parsed.get("COINS", "BTC, ETH")
                eng = engine
            else:
                title_ru = await translate_text(title)
                summ = shorten(title_ru, 380)
                bias = news_sentiment(f"{title}. {summary}")
                impact = "Нейтральное влияние" if bias == 0 else "Позитив" if bias == 1 else "Негатив"
                coins = ", ".join(mentioned_coins(f"{title}. {summary}") or ["BTC", "ETH"])
                eng = "fallback"

            market_bias = {"bias": bias, "title_ru": title_ru, "summary": summ, "impact": impact, "coins": coins, "published": published, "link": link, "engine": eng, "ts": now, "ai_try": now}
        except Exception as e:
            log.warning(f"update_market_bias: {e}")

def format_news_post(nb):
    emoji = {1: "🟢", 0: "⚪", -1: "🔴"}.get(nb.get("bias", 0), "⚪")
    label = {1: "Позитивно", 0: "Нейтрально", -1: "Негативно"}.get(nb.get("bias", 0), "Нейтрально")
    text = f"📰 <b>{html.escape(nb.get('title_ru', ''))}</b>\n🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n📌 <b>Суть</b>\n{html.escape(nb.get('summary', ''))}\n\n📊 <b>Влияние</b>\n{emoji} <b>{label}.</b> {html.escape(nb.get('impact', ''))}\n\n🪙 <b>Монеты:</b> {html.escape(nb.get('coins', ''))}"
    if nb.get("link"): text += f"\n\n🔗 <a href=\"{html.escape(nb['link'], quote=True)}\">Источник</a>"
    return text

async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries or market_bias.get("engine") == "none": return None, None
    return format_news_post(market_bias), extract_image(entries[0])

async def news_watcher_loop():
    global last_news_link
    await asyncio.sleep(40)
    while True:
        try:
            entries = await get_news_entries()
            if entries:
                link = entries[0].link
                if last_news_link is None: last_news_link = link
                elif link != last_news_link:
                    last_news_link = link
                    if bot_flags["news_enabled"] and active_chats:
                        text, img_url = await prepare_news(force=True)
                        if text:
                            img = await download_bytes(img_url) if img_url else None
                            payload = {"kind": "photo" if img else "text", "text": text, "data": img, "markup": MENU_KB}
                            await broadcast(payload)
        except Exception as e:
            log.warning(f"news_watcher: {e}")
        next_timers["news_watcher"] = time.time() + 600
        await asyncio.sleep(600)


# ====================== СКАНЕР ======================
SCAN_INTERVAL = 17 * 60
SIGNAL_COOLDOWN = 3 * 3600
signal_sent_at = {}
scan_skip = {}

async def scan_once(send=True):
    global last_signals_data, current_signals_db
    if time.time() - market_bias["ts"] > 900: await update_market_bias()
    bias = market_bias["bias"] if time.time() - market_bias["ts"] < 3 * 3600 else 0
    bias_text = market_bias.get("impact", "Нейтральный фон")
    sem = asyncio.Semaphore(3)
    candidates = []

    async def check(coin):
        if time.time() < scan_skip.get(coin, 0): return
        async with sem:
            try:
                tf_data, source = await get_market_data(coin)
                tech = tech_check(tf_data)
                if not tech: return
                if bias == 1 and not tech["is_long"]: return
                if bias == -1 and tech["is_long"]: return
                candidates.append({"coin": coin, "tech": tech, "tf_data": tf_data})
            except Exception: pass

    await asyncio.gather(*[check(c) for c in POPULAR_COINS])
    candidates.sort(key=lambda x: x["tech"]["strength"], reverse=True)
    
    log.info(f"Сканер: найдено {len(candidates)} подходящих сигналов (bias={bias})")

    # 1. Сохраняем ВСЕ текущие сигналы в БД (без учета кулдауна)
    new_signals_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
    for cand in candidates:
        coin, tech, tf_data = cand["coin"], cand["tech"], cand["tf_data"]
        info = tf_data["4h"]
        is_long = tech["is_long"]
        entry, tps, sls = calc_levels(info, is_long)
        side_emoji = "🟢" if is_long else "🔴"
        side = "LONG" if is_long else "SHORT"
        bar = make_strength_bar(tech["strength"])
        lev, risk = get_leverage_recommendation(tech["strength"], tech["type"])
        text = (f"{side_emoji} <b>{side}  ·  {coin}</b>\n━━━━━━━━━━━━━━━━━━━━\n📌 Тип: <b>{tech['type']}</b>\n💪 Сила: <b>{tech['strength']}%</b>\n{bar}\n\n"
                f"💰 Цена: <code>{fmt(info['price'])}</code>\n📥 Вход: <code>{fmt(entry)}</code>\n\n🎯 <b>Тейки</b>\n   TP1  <code>{fmt(tps[0])}</code>\n   TP2  <code>{fmt(tps[1])}</code>\n   TP3  <code>{fmt(tps[2])}</code>\n\n"
                f"🛡 <b>Стопы</b>\n   SL1  <code>{fmt(sls[0])}</code>\n   SL2  <code>{fmt(sls[1])}</code>\n\n⚖ <b>Плечо:</b> {lev}\n💵 <b>Риск:</b> {risk} от депозита\n\n🌍 Фон: {html.escape(bias_text)}")
        sig_dict = {"coin": coin, "text": text, "is_long": is_long, "entry": entry, "tps": tps, "sls": sls, "type": tech["type"], "strength": tech["strength"], "side": side}
        new_signals_db[tech["type"]].append(sig_dict)

    # Ограничиваем топ-10
    for k in new_signals_db: new_signals_db[k] = new_signals_db[k][:10]
    current_signals_db = new_signals_db

    # 2. Фильтруем для рассылки (кулдаун 3 часа)
    to_broadcast = []
    if send:
        now = time.time()
        for cand in candidates:
            key = f"{cand['coin']}:{'L' if cand['tech']['is_long'] else 'S'}"
            if now - signal_sent_at.get(key, 0) >= SIGNAL_COOLDOWN:
                to_broadcast.append(cand)
                
    to_broadcast = to_broadcast[:4] # Максимум 4 штуки в одном сообщении

    if to_broadcast and send:
        now = time.time()
        hist = _load_signal_history()
        for cand in to_broadcast:
            key = f"{cand['coin']}:{'L' if cand['tech']['is_long'] else 'S'}"
            signal_sent_at[key] = now
            
            # Ищем полный текст для сохранения в историю
            for s in new_signals_db[cand['tech']['type']]:
                if s['coin'] == cand['coin']:
                    hist.append({"ts": now, "coin": s["coin"], "side": s["side"], "type": s["type"], "strength": s["strength"], "text": s["text"]})
                    break
        _save_signal_history(hist)

        msk_now = datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M")
        text = f"🔥 <b>Сигналы</b> · {msk_now} МСК\n\n📊 <b>Кратко о рынке:</b>\n{html.escape(bias_text)}\n\nВыберите категорию:"
        kb = [
            [InlineKeyboardButton(text=f"⚡ Быстрый ({len(current_signals_db['Быстрый сигнал'])})", callback_data="sig_list:Быстрый сигнал")],
            [InlineKeyboardButton(text=f"⏳ Среднесрок ({len(current_signals_db['Среднесрок'])})", callback_data="sig_list:Среднесрок")],
            [InlineKeyboardButton(text=f"🛡 Долгосрок ({len(current_signals_db['Долгосрок'])})", callback_data="sig_list:Долгосрок")],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
        ]
        markup = InlineKeyboardMarkup(inline_keyboard=kb)
        last_signals_data = {"text": text, "kb": markup, "ts": time.time()}
        await broadcast({"kind": "text", "text": text, "markup": markup})
        stats["signals"] += len(to_broadcast)
    return to_broadcast

async def scanner_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats: await scan_once(send=True)
        except Exception as e: log.error(f"scanner: {e}")
        next_timers["scanner"] = time.time() + SCAN_INTERVAL
        await asyncio.sleep(SCAN_INTERVAL)


# ====================== ГРАФИК ======================
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    df = tf_data[main_tf]["df"].tail(50).reset_index(drop=True)
    w, h = 800, 420
    img = Image.new("RGB", (w, h), (16, 16, 22))
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
        font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 15)
    except Exception:
        font = font_b = ImageFont.load_default()
    side = "LONG" if is_long else "SHORT"
    col = (0, 200, 90) if is_long else (220, 50, 50)
    draw.text((18, 10), f"{coin}  {main_tf}  {side}", fill=col, font=font_b)
    pad_l, pad_r, pad_t, pad_b = 55, 15, 45, 30
    cw, ch = w - pad_l - pad_r, h - pad_t - pad_b
    highs, lows = df["high"].values, df["low"].values
    mn = min(lows.min(), entry, min(sls), min(tps)) * 0.997
    mx = max(highs.max(), entry, max(sls), max(tps)) * 1.003
    rng = mx - mn or 1
    def y(p): return pad_t + ch - (p - mn) / rng * ch
    n = len(df)
    cw_c = max(3, int(cw / n) - 1)
    for i in range(n):
        x = pad_l + i * (cw / n) + cw_c // 2
        o, hh, ll, c = df["open"].iloc[i], highs[i], lows[i], df["close"].iloc[i]
        color = (0, 170, 80) if c >= o else (200, 45, 45)
        draw.line([(x, y(hh)), (x, y(ll))], fill=color, width=1)
        top, bot = min(y(o), y(c)), max(y(o), y(c))
        draw.rectangle([x - cw_c // 2, top, x + cw_c // 2, bot], fill=color)
    def lvl(price, label, color, dash=False):
        yy = y(price)
        if dash:
            for xx in range(pad_l, w - pad_r, 7): draw.line([(xx, yy), (xx + 3, yy)], fill=color, width=1)
        else: draw.line([(pad_l, yy), (w - pad_r, yy)], fill=color, width=2)
        draw.text((pad_l + 3, yy - 12), f"{label} {fmt(price)}", fill=color, font=font)
    lvl(entry, "Entry", (255, 190, 0))
    for i, tp in enumerate(tps): lvl(tp, f"TP{i + 1}", (0, 190, 90), True)
    for i, sl in enumerate(sls): lvl(sl, f"SL{i + 1}", (220, 50, 50), True)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ====================== BROADCAST + АНАЛИЗ ======================
async def broadcast(payload, only_ids=None):
    targets = list(only_ids) if only_ids else list(active_chats)
    sent = failed = 0
    dead = []
    for cid in targets:
        for _ in range(2):
            try:
                if payload.get("kind") == "photo" and payload.get("data"):
                    photo = BufferedInputFile(payload["data"], filename="image.png")
                    text = payload.get("text", "")
                    if len(text) <= 1024: await bot.send_photo(cid, photo, caption=text, parse_mode="HTML", reply_markup=payload.get("markup"))
                    else:
                        await bot.send_photo(cid, photo)
                        await bot.send_message(cid, text[:4000], parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
                else:
                    await bot.send_message(cid, payload.get("text", "")[:4000], parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
                sent += 1
                break
            except TelegramRetryAfter as e: await asyncio.sleep(e.retry_after + 1)
            except (TelegramForbiddenError, TelegramNotFound):
                dead.append(cid)
                break
            except Exception:
                failed += 1
                break
        await asyncio.sleep(0.04)
    if dead:
        for cid in dead: active_chats.discard(cid)
        await asyncio.to_thread(mark_users_inactive, dead)
    stats["broadcasts"] += 1
    return sent, failed, len(dead), []

async def analyze_coin(coin, user_id=None):
    try:
        tf_data, source = await get_market_data(coin)
    except Exception:
        return None, f"⚠ Не удалось получить данные по <b>{html.escape(coin)}</b>"
    tech = tech_check(tf_data)
    if tech: is_long, strength, sig_type = tech["is_long"], tech["strength"], tech["type"]
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
    lev, risk = get_leverage_recommendation(strength, sig_type)
    text = (f"{side_emoji} <b>{side}  ·  {coin}</b>\n━━━━━━━━━━━━━━━━━━━━\n📌 Тип: <b>{sig_type}</b>\n💪 Сила: <b>{strength}%</b>\n{bar}\n\n"
            f"💰 Цена: <code>{fmt(info['price'])}</code>\n📥 Вход: <code>{fmt(entry)}</code>\n\n🎯 <b>Тейки</b>\n   TP1  <code>{fmt(tps[0])}</code>\n   TP2  <code>{fmt(tps[1])}</code>\n   TP3  <code>{fmt(tps[2])}</code>\n\n"
            f"🛡 <b>Стопы</b>\n   SL1  <code>{fmt(sls[0])}</code>\n   SL2  <code>{fmt(sls[1])}</code>\n\n⚖ <b>Плечо:</b> {lev}\n💵 <b>Риск:</b> {risk} от депозита\n\n🌍 Фон: {html.escape(market_bias.get('impact', 'Нейтральный'))}")
    try: img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception: img = None
    return {"text": text, "img": img, "coin": coin, "entry": entry, "tps": tps, "sls": sls, "is_long": is_long, "side": side, "price": info["price"], "type": sig_type, "strength": strength}, None


# ====================== МОНИТОРИНГ ПОЗИЦИЙ ======================
async def position_monitor_loop():
    await asyncio.sleep(50)
    while True:
        try:
            users = _load_users()
            for uid_str, u in users.items():
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    try: await check_position(int(uid_str), coin, pos)
                    except Exception: pass
                    await asyncio.sleep(0.3)
        except Exception as e: log.error(f"pos monitor: {e}")
        next_timers["positions"] = time.time() + 320
        await asyncio.sleep(320)

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
    if hit_sl: reason, urgency = "🛑 Сработал стоп-лосс\nРекомендую закрыть позицию.", "danger"
    elif hit_tp3: reason, urgency = "✅ Достигнут TP3\nМожно фиксировать всю позицию.", "success"
    elif hit_tp2 and not pos.get("notified_tp"): reason, urgency = "✅ Достигнут TP2\nЧастично фиксируем.", "success"
    elif hit_tp1 and not pos.get("notified_tp"): reason, urgency = "✅ Достигнут TP1\nМожно закрыть часть.", "success"
    if urgency == "success": pos["notified_tp"] = True; set_user_open_position(user_id, coin, pos)
    if not reason: return
    text = f"{'🛑' if urgency == 'danger' else '✅'} <b>Рекомендация по {coin}</b>\n\nНаправление: <b>{side}</b>\nВход: <code>{fmt(entry)}</code>\nСейчас: <code>{fmt(price)}</code>\nPnL: <b>{pnl:+.2f}%</b>\n\n{html.escape(reason)}"
    try:
        await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")], [InlineKeyboardButton(text="❌ Закрыл", callback_data=f"pos:close:{coin}")]]))
        if urgency in ("danger", "success"): pos["notified_close"] = True; set_user_open_position(user_id, coin, pos)
    except Exception: pass


# ====================== ХЕНДЛЕРЫ ======================
@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message):
    await asyncio.to_thread(save_user_data, m.from_user.id, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    await cleanup_prev_menu(m.from_user.id)
    welcome_text = ("👋 <b>Привет! Добро пожаловать в IXCLY Trading Bot!</b>\n\nЯ твой персональный крипто-помощник.\n\n"
                    "📊 <b>Умный анализ</b> — покажу, куда движется цена.\n📈 <b>Слежение за сделками</b> — нажми «Вошёл в сделку», и я буду следить.\n"
                    "🔥 <b>Готовые сигналы</b> — автоматически ищу перспективные монеты.\n📰 <b>Новости и фон</b> — объясняю простым языком.\n"
                    "⭐ <b>Твоё избранное</b> — сохраняй идеи.\n\n⚠️ <i>Криптовалюта — это рискованно!</i>\n\n👇 Выбери раздел в меню ниже:")
    msg = await m.answer(welcome_text, reply_markup=get_coins_keyboard(), parse_mode="HTML")
    last_user_msg[m.from_user.id] = msg

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, f"👋 <b>IXCLY v{VERSION}</b>\n\nГлавное меню:", reply_markup=get_coins_keyboard(), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("search_page:"))
async def cb_search_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    try: await cb.message.edit_reply_markup(reply_markup=get_search_coins_keyboard(page))
    except Exception: pass
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("c:"))
async def cb_coin(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[1], cb.from_user.id
    if uid in busy_users: await safe_answer(cb, "Уже обрабатываю…"); return
    busy_users.add(uid)
    try:
        await safe_answer(cb, "⏳ Анализирую…")
        res, err = await analyze_coin(coin)
        if err: await safe_edit(cb.message, err, user_id=uid); return
        await cb.message.delete()
        if res["img"]: msg = await cb.message.answer_photo(BufferedInputFile(res["img"], f"{coin}.png"), caption=res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin))
        else: msg = await cb.message.answer(res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin), disable_web_page_preview=True)
        last_user_msg[uid] = msg
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally: busy_users.discard(uid)

@dp.callback_query(F.data.startswith("r:"))
async def cb_refresh(cb: types.CallbackQuery):
    await cb_coin(cb)

@dp.callback_query(F.data == "news_menu")
async def cb_news_menu(cb: types.CallbackQuery):
    uid = cb.from_user.id
    await safe_answer(cb, "📰 Загружаю новость…")
    await safe_edit(cb.message, "⏳ Перевожу и анализирую новость…", user_id=uid)
    try:
        text, img_url = await prepare_news()
        if not text: await safe_edit(cb.message, "📭 Свежих новостей пока нет", reply_markup=MENU_KB, user_id=uid); return
        img = await download_bytes(img_url) if img_url else None
        await cb.message.delete()
        if img:
            if len(text) <= 1024: msg = await cb.message.answer_photo(BufferedInputFile(img, "news.jpg"), caption=text, parse_mode="HTML", reply_markup=MENU_KB)
            else:
                await cb.message.answer_photo(BufferedInputFile(img, "news.jpg"))
                msg = await cb.message.answer(text, parse_mode="HTML", reply_markup=MENU_KB, disable_web_page_preview=True)
        else: msg = await cb.message.answer(text, parse_mode="HTML", reply_markup=MENU_KB, disable_web_page_preview=True)
        last_user_msg[uid] = msg
    except Exception as e:
        log.error(f"news error: {e}")
        await safe_edit(cb.message, "Ошибка при загрузке новости.", reply_markup=MENU_KB, user_id=uid)

@dp.callback_query(F.data == "last_signals")
async def cb_last(cb: types.CallbackQuery):
    if last_signals_data.get("text") and time.time() - last_signals_data.get("ts", 0) < 3600:
        await safe_edit(cb.message, last_signals_data["text"], reply_markup=last_signals_data.get("kb"), user_id=cb.from_user.id)
    else:
        text = f"🔥 <b>Сигналы</b>\n\n📊 <b>Кратко о рынке:</b>\n{html.escape(market_bias.get('impact', 'Нейтральный фон'))}\n\nВыберите категорию:"
        kb = [
            [InlineKeyboardButton(text=f"⚡ Быстрый ({len(current_signals_db['Быстрый сигнал'])})", callback_data="sig_list:Быстрый сигнал")],
            [InlineKeyboardButton(text=f"⏳ Среднесрок ({len(current_signals_db['Среднесрок'])})", callback_data="sig_list:Среднесрок")],
            [InlineKeyboardButton(text=f"🛡 Долгосрок ({len(current_signals_db['Долгосрок'])})", callback_data="sig_list:Долгосрок")],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
        ]
        await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("sig_list:"))
async def cb_sig_list(cb: types.CallbackQuery):
    category = cb.data.split(":")[1]
    sigs = current_signals_db.get(category, [])[:10]
    
    if not sigs:
        text = f"📋 <b>Сигналы: {category}</b>\n\n<i>В данный момент сигналов в этой категории нет. Рынок находится в нейтральной зоне или новостной фон блокирует входы.</i>"
        kb = [[InlineKeyboardButton(text="⬅ Назад к обзору", callback_data="last_signals")]]
        await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
        await safe_answer(cb, "Пусто", False)
        return

    text = f"📋 <b>Сигналы: {category}</b>\n\n"
    kb = []
    for s in sigs:
        emoji = "🟢" if s["is_long"] else "🔴"
        text += f"{emoji} <b>{s['coin']}</b> · Сила: {s['strength']}%\n"
        kb.append([InlineKeyboardButton(text=f"{emoji} {s['coin']}", callback_data=f"c:{s['coin']}")])
    kb.append([InlineKeyboardButton(text="⬅ Назад к обзору", callback_data="last_signals")])
    await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data == "show_stats")
async def cb_stats(cb: types.CallbackQuery):
    await safe_answer(cb, "📊 Собираю…")
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        data = {x["symbol"]: x for x in j}
        gainers, losers = [], []
        for c in POPULAR_COINS:
            if f"{c}USDT" in data:
                ch = float(data[f"{c}USDT"]["priceChangePercent"])
                (gainers if ch > 0 else losers).append((c, ch))
        gainers.sort(key=lambda x: x[1], reverse=True)
        losers.sort(key=lambda x: x[1])
        text = "📊 <b>Топ за 24ч</b>\n\n🟢 <b>Рост:</b>\n" + "\n".join([f"<code>{c}</code> +{ch:.1f}%" for c, ch in gainers[:8]])
        text += "\n\n🔴 <b>Падение:</b>\n" + "\n".join([f"<code>{c}</code> {ch:.1f}%" for c, ch in losers[:8]])
        await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    except Exception as e:
        await safe_edit(cb.message, f"Ошибка: {html.escape(str(e))}", reply_markup=MENU_KB, user_id=cb.from_user.id)

@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting(cb: types.CallbackQuery):
    await safe_answer(cb, "🧐 Ищу…")
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=9)
        movers = [x for x in j if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
        movers.sort(key=lambda x: float(x["priceChangePercent"]), reverse=True)
        text = "🧐 <b>Интересные за 24ч</b>\n\n🟢 Топ роста:\n" + "\n".join([f"<code>{x['symbol'][:-4]}</code> +{float(x['priceChangePercent']):.1f}%" for x in movers[:4]])
        text += "\n\n🔴 Топ падения:\n" + "\n".join([f"<code>{x['symbol'][:-4]}</code> {float(x['priceChangePercent']):.1f}%" for x in movers[-4:]])
        await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    except Exception:
        await safe_edit(cb.message, "Не удалось получить данные", reply_markup=MENU_KB, user_id=cb.from_user.id)

@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_edit(cb.message, "🔍 <b>Выбери монету из списка</b>\n<i>Или просто напиши тикер сообщением:</i>", reply_markup=get_search_coins_keyboard(0), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,12}$"))
async def msg_ticker(m: types.Message):
    coin, uid = normalize_ticker(m.text), m.from_user.id
    if uid in busy_users: return
    busy_users.add(uid)
    await cleanup_prev_menu(uid)
    try:
        res, err = await analyze_coin(coin)
        if err: msg = await m.answer(err, parse_mode="HTML"); last_user_msg[uid] = msg; return
        if res["img"]: msg = await m.answer_photo(BufferedInputFile(res["img"], f"{coin}.png"), caption=res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin))
        else: msg = await m.answer(res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin), disable_web_page_preview=True)
        last_user_msg[uid] = msg
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally: busy_users.discard(uid)

@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data == "sig:save_new")
async def cb_sig_save_new(cb: types.CallbackQuery):
    await cb.message.edit_text("Выбери монету:", reply_markup=get_save_coins_keyboard())
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("sig:save_page:"))
async def cb_sig_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[2])
    try: await cb.message.edit_reply_markup(reply_markup=get_save_coins_keyboard(page))
    except Exception: pass
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("sig:do_save:") | F.data.startswith("sig:quick_save:"))
async def cb_sig_save(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[-1], cb.from_user.id
    cached = result_cache.get(uid)
    text = cached["text"] if cached and cached.get("coin") == coin else f"Сигнал {coin}"
    await asyncio.to_thread(add_user_saved_signal, uid, coin, text)
    asyncio.create_task(telegram_backup("сохранение"))
    await safe_answer(cb, f"✅ {coin} сохранён")
    if "quick_save" not in cb.data: await cb.message.edit_text("⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(uid), parse_mode="HTML")

@dp.callback_query(F.data.startswith("sig:view:"))
async def cb_sig_view(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    item = next((s for s in get_user_saved_signals(cb.from_user.id) if s["coin"] == coin), None)
    if not item: await safe_answer(cb, "Не найдено", True); return
    await safe_edit(cb.message, item["text"], reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data == "sig:del_menu")
async def cb_sig_del_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Выбери для удаления:", reply_markup=get_delete_signals_menu(cb.from_user.id))
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("sig:del:"))
async def cb_sig_del(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    await asyncio.to_thread(del_user_saved_signal, cb.from_user.id, coin)
    asyncio.create_task(telegram_backup("удаление"))
    await safe_answer(cb, f"🗑 {coin} удалён")
    await cb.message.edit_text("⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), parse_mode="HTML")

@dp.callback_query(F.data.startswith("pos:enter:"))
async def cb_pos_enter(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[2], cb.from_user.id
    await safe_answer(cb, "⏳ Фиксирую…")
    res, err = await analyze_coin(coin)
    if err: await safe_edit(cb.message, "Не удалось получить уровни", user_id=uid); return
    data = {"side": res["side"], "entry_price": res["entry"], "entered_at": time.time(), "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1], "notified_close": False, "notified_tp": False}
    await asyncio.to_thread(set_user_open_position, uid, coin, data)
    asyncio.create_task(telegram_backup("открытие сделки"))
    text = f"{'🟢' if res['side'] == 'LONG' else '🔴'} <b>Позиция открыта</b>\n\nМонета: <b>{coin}</b>\nНаправление: <b>{res['side']}</b>\nВход: <code>{fmt(res['entry'])}</code>\n\nБуду следить и сообщу, когда лучше закрыть."
    await safe_edit(cb.message, text, reply_markup=get_saved_signal_view_keyboard(uid, coin), user_id=uid)

@dp.callback_query(F.data.startswith("pos:close:"))
async def cb_pos_close(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    await asyncio.to_thread(set_user_open_position, cb.from_user.id, coin, None)
    asyncio.create_task(telegram_backup("закрытие сделки"))
    await safe_answer(cb, "❌ Позиция закрыта")
    try: await cb.message.edit_reply_markup(reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin))
    except Exception: pass

@dp.callback_query(F.data.startswith("pos:status:"))
async def cb_pos_status(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    pos = get_open_position(cb.from_user.id, coin)
    if not pos: await safe_answer(cb, "Позиция не найдена", True); return
    price = await get_simple_price(coin)
    if not price: await safe_answer(cb, "Нет цены", True); return
    entry = pos["entry_price"]
    pnl = ((price - entry) / entry * 100) if pos["side"] == "LONG" else ((entry - price) / entry * 100)
    text = f"📈 <b>Статус {coin}</b>\n\nНаправление: <b>{pos['side']}</b>\nВход: <code>{fmt(entry)}</code>\nСейчас: <code>{fmt(price)}</code>\nPnL: <b>{pnl:+.2f}%</b>\n\nTP1: <code>{fmt(pos['tp1'])}</code>\nTP2: <code>{fmt(pos['tp2'])}</code>\nSL1: <code>{fmt(pos['sl1'])}</code>"
    await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data == "ignore")
async def cb_ignore(cb: types.CallbackQuery):
    await safe_answer(cb)


# ====================== АДМИН-БОТ ======================
if admin_dp:
    @admin_dp.errors()
    async def admin_errors_handler(update: types.Update, exception: Exception):
        if isinstance(exception, TelegramNetworkError): return True
        return False

    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_cmd_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            await m.answer(f"⛔ Доступ только для администратора.\n\nВаш ID: <code>{m.from_user.id}</code>", parse_mode="HTML"); return
        total, active, _, _ = user_counts()
        text = f"✅ <b>Админ-панель IXCLY v{VERSION}</b>\n👥 Пользователей: {total} (активных {active})"
        if total == 0: text += "\n\n⚠ База пуста. Нажмите «📥 Восстановить»."
        await m.answer(text, reply_markup=get_admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            await safe_answer(cb, "Нет доступа", True); return
        act = cb.data.split(":")[1]

        if act == "toggle_news":
            bot_flags["news_enabled"] = not bot_flags["news_enabled"]
            await safe_answer(cb, f"Новости: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}")
            try: await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            except Exception: pass
            return
        if act == "toggle_scan":
            bot_flags["alerts_enabled"] = not bot_flags["alerts_enabled"]
            await safe_answer(cb, f"Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}")
            try: await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            except Exception: pass
            return

        await safe_answer(cb, "⏳ Выполняю…")

        if act == "stats":
            total, active, _, _ = user_counts()
            up = int(time.time() - stats["started"])
            text = f"📊 <b>Статистика</b>\n\nВерсия: {VERSION}\nЮзеров: {total} (активных {active})\nСигналов: {stats['signals']}\nНовостей: {stats['news_sent']}\nАптайм: {up // 3600}ч {up % 3600 // 60}м\nНовость: {market_bias.get('engine')} · bias {market_bias['bias']}\n{html.escape(market_bias.get('title_ru', '')[:70])}"
            await cb.message.answer(text, parse_mode="HTML")
        elif act == "users":
            users = _load_users()
            lines = [f"👥 Пользователи ({len(users)})\n"] + [f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or u.get('username') or '—')[:18])}" for uid, u in list(users.items())[-30:]]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        elif act == "news_now":
            try:
                text, img_url = await prepare_news(force=True)
                if not text: await cb.message.answer("Свежих новостей нет"); return
                img = await download_bytes(img_url) if img_url else None
                sent, failed, _, _ = await broadcast({"kind": "photo" if img else "text", "text": text, "data": img, "markup": MENU_KB})
                stats["news_sent"] += sent
                await cb.message.answer(f"✅ Отправлено: {sent}, ошибок: {failed}")
            except Exception as e: await cb.message.answer(f"Ошибка: {html.escape(str(e))}")
        elif act == "scan_test":
            sigs = await scan_once(send=False)
            if sigs:
                lines = [f"{'🟢' if s['is_long'] else '🔴'} {s['coin']} · {s['type']} · {s['strength']}%" for s in sigs]
                await cb.message.answer("🔍 Тест сканера (без рассылки)\n\n" + "\n".join(lines))
            else:
                await cb.message.answer("🔍 Тест сканера: сильных сигналов сейчас нет (или фон новостей их блокирует)")
        elif act == "broadcast_now":
            if last_signals_data.get("text"):
                sent, _, _, _ = await broadcast({"kind": "text", "text": last_signals_data["text"], "markup": last_signals_data.get("kb")})
                await cb.message.answer(f"Рассылка: {sent}")
            else: await cb.message.answer("Нет сигналов")
        elif act == "diag":
            text = f"🔧 <b>Диагностика</b>\n\nЧатов: {len(active_chats)}\nADMIN_IDS: {len(ADMIN_IDS)}\nЗадержка: {POLL_START_DELAY} с\n\n" + await run_ai_diag()
            await cb.message.answer(text, parse_mode="HTML")
        elif act == "backup":
            ok = await telegram_backup("вручную")
            await cb.message.answer("💾 Бэкап отправлен." if ok else "База пуста.")
        elif act == "restore":
            await cb.message.answer("Пришли файл users.json.")
        elif act == "timers":
            now = time.time()
            def fmt_time(sec):
                if sec <= 0: return "выполняется сейчас"
                m, s = divmod(int(sec), 60)
                return f"через {m} мин {s} сек" if m else f"через {s} сек"
            lines = ["⏳ <b>Таймеры:</b>\n",
                     f"🤖 Сканер: {fmt_time(next_timers['scanner'] - now)}",
                     f"📈 Мониторинг: {fmt_time(next_timers['positions'] - now)}",
                     f"📰 Новости: {fmt_time(next_timers['news_watcher'] - now)}",
                     f"💾 Бэкап: {fmt_time(next_timers['backup'] - now)}"]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        if m.from_user.id not in ADMIN_IDS or not (m.document.file_name or "").endswith(".json"): return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            content = await admin_bot.download_file(f.file_path)
            data = json.loads(content.read().decode("utf-8"))
            current = _load_users()
            added = 0
            for k, v in data.items():
                if k not in current and isinstance(v, dict):
                    current[k] = v
                    added += 1
            _save_users(current)
            load_active_chats()
            await m.answer(f"✅ Восстановлено: +{added}, всего {len(current)}.")
        except Exception as e:
            await m.answer(f"Ошибка: {html.escape(str(e))}", parse_mode="HTML")


# ====================== MAIN ======================
async def run_polling(dispatcher, tg_bot, name, stop_event):
    attempt = 0
    while not stop_event.is_set():
        try:
            attempt += 1
            log.info(f"{name}: запуск поллинга (попытка {attempt})")
            try: await tg_bot.delete_webhook(drop_pending_updates=True)
            except Exception: pass
            await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=30)
            if not stop_event.is_set():
                log.warning(f"{name}: start_polling завершился. Перезапуск...")
                await asyncio.sleep(5)
        except asyncio.CancelledError: raise
        except TelegramNetworkError as e:
            log.warning(f"{name}: сетевая ошибка: {e}")
            try: await tg_bot.session.close()
            except Exception: pass
            await asyncio.sleep(min(2 ** attempt, 30))
        except TelegramConflictError as e:
            log.warning(f"{name}: конфликт getUpdates: {e}")
            try: await tg_bot.session.close()
            except Exception: pass
            await asyncio.sleep(min(2 ** attempt, 60))
        except Exception as e:
            log.error(f"{name}: упал: {e}")
            await asyncio.sleep(5)

async def main():
    global http, news_lock
    keep_alive()
    news_lock = asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/2.10"})
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
            me = await bot.get_me()
            runtime["main_bot"] = f"@{me.username}"
        except Exception as e: log.error(f"get_me основного: {e}")
        if admin_bot:
            try:
                me = await admin_bot.get_me()
                runtime["admin_bot"] = f"@{me.username}"
            except Exception as e: log.error(f"get_me админа: {e}")

        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot: await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.delete_my_commands()
        except Exception: pass

        if POLL_START_DELAY > 0:
            try: await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError: pass
        if stop.is_set(): return

        restored = await telegram_restore()
        if restored: log.info(f"✅ База восстановлена: {restored}")
        load_active_chats()

        background += [
            asyncio.create_task(scanner_loop()),
            asyncio.create_task(position_monitor_loop()),
            asyncio.create_task(news_watcher_loop()),
            asyncio.create_task(backup_loop()),
            asyncio.create_task(update_market_bias()),
        ]

        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp:
            pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))

        log.info(f"🚀 IXCLY v{VERSION} запущен.")
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
    finally:
        for t in pollers: t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        try: await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=12)
        except Exception: pass
        for t in background: t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        try: await bot.session.close()
        except Exception: pass
        if admin_bot:
            try: await admin_bot.session.close()
            except Exception: pass
        await http.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): log.info("Остановлен")
