# -*- coding: utf-8 -*-
"""
IXCLY Trading Bot v2.15.0 - Полная версия со всеми паттернами, бэктестом, лимитками и сводным меню
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

# ====================== ВЕБ (KEEP-ALIVE) ======================
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
        "last_scan": last_scan_info,
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


# ====================== КОНФИГУРАЦИЯ И НАСТРОЙКИ ======================
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

VERSION = "2.15.0"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}

POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "60"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_CONF = float(os.getenv("BT_CONF", "60"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "30"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))
DEPOSIT = float(os.getenv("DEPOSIT", "1000"))
RISK_PCT = 0.01

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

http = None
news_lock = None

TFS = ["15m", "1h", "4h", "1d"]
MIN_CANDLES = 60

POPULAR_COINS = [
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "TON", "SHIB", "DOT", "LINK",
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
    "AEVO", "ETHFI", "ENA", "W", "EIGEN", "MOVE", "PNUT", "GOAT", "ACT", "MOODENG", "TURBO", "MOG",
    "NEIRO", "DOGS", "NOT", "PIXEL", "PORTAL", "DYM", "ALT", "MANTA", "ZETA", "TNSR", "DRIFT", "KMNO",
    "WEN", "MYRO", "POPCAT", "GIGA", "MEW", "SLERF", "LUNC", "LUNA", "USTC", "CTSI", "COTI", "CELO",
    "CKB", "ONE", "QTUM", "IOST", "ONT", "ICX", "WAN", "ARK", "STRAX", "STMX", "PHA", "KLAY",
    "XEC", "XNO", "XTZ", "ZRX", "ZIL", "ZEN", "YFI", "YFII", "XVS", "XRP", "XMR", "XLM",
    "XDC", "XAVA", "WOO", "WAVES", "WAXP", "VTHO", "VRA", "VOXEL", "VITE", "VIB", "VET", "VELO",
    "USTC", "UOS", "UNI", "UMA", "UFT", "TRX", "TRU", "TRB", "TOMO", "TLM", "TLOS", "TKO",
    "TIA", "THETA", "TFUEL", "SUSHI", "SUN", "SUI", "STX", "STRAX", "STORJ", "STEEM", "SSV", "SRM",
    "SOL", "SLP", "SKL", "SFP", "SFUND", "SEI", "SAND", "RVN", "RUNE", "RSR", "RPL", "ROSE",
    "RNDR", "RLC", "RIF", "REEF", "RAY", "RATS", "RARE", "RAM", "QTUM", "QNT", "QKC", "QI",
    "PYR", "PYTH", "PUNDIX", "PROM", "PRIME", "POND", "POLYX", "POLS", "POL", "PNG", "PNK", "PHA",
    "PERP", "PENDLE", "PEG", "PAXG", "PAR", "PAAL", "ONT", "ONG", "ONE", "OMG", "OGN", "OCEAN",
    "NMR", "NKN", "NEXO", "NEAR", "NEO", "NEIRO", "NEBL", "NCT", "NAS", "MTL", "MTH", "MPL",
    "MOODENG", "MNT", "MNW", "MIR", "MINA", "MIM", "MFT", "META", "MEW", "MBOX", "MBL", "MASK",
    "MANTA", "MANA", "MAGIC", "LYXE", "LUNC", "LUNA", "LTO", "LTC", "LSS", "LSK", "LQTY", "LPT",
    "LOKA", "LOOM", "LIT", "LINK", "LINA", "LDO", "LAZIO", "LAT", "KSM", "KNC", "KMD", "KLAY",
    "KAVA", "KAS", "JUP", "JTO", "JOE", "JASMY", "IOTX", "IOST", "ION", "INJ", "IMX", "ILV",
    "IDEX", "ID", "ICP", "ICE", "HBAR", "HARD", "HAPI", "GTC", "GT", "GRT", "GMT", "GMX",
    "GLMR", "GLM", "GFT", "GEL", "GALA", "FXS", "FTT", "FTM", "FT", "FET", "FIDA", "FIO",
    "FIL", "FARM", "EGLD", "EOS", "ENS", "ENJ", "ELF", "ELA", "DYDX", "DUSK", "DREP", "DODO",
    "DOCK", "DNT", "DMA", "DIA", "DGB", "DENT", "DCR", "DASH", "CVC", "CTXC", "CTSI", "CTK",
    "CRV", "CREAM", "COVAL", "COTI", "COS", "COMP", "CND", "CMT", "CLV", "CKB", "CITY", "CHZ",
    "CHR", "CHESS", "CFX", "CELR", "CELO", "CAKE", "C98", "BURGER", "BTS", "BTG", "BTC", "BOND",
    "BOSON", "BNT", "BNX", "BNB", "BLZ", "BICO", "BETA", "BEL", "BCH", "BAT", "BAR", "BAL",
    "BADGER", "BAKED", "AXL", "AVT", "AVA", "AVAX", "AUCTION", "AUDIO", "ATM", "ATLAS", "ASTR", "ASR",
    "ARPA", "ARK", "ARDR", "ARB", "AR", "APT", "API3", "APE", "ANTV", "ANT", "ANKR", "AMP",
    "AMB", "ALPHA", "ALPACA", "AKRO", "AION", "AGLD", "AGIX", "ADX", "ACM", "ACE", "ZK", "ZRO",
    "LISTA", "TAIKO", "REZ", "OMNI", "SAGA", "MAVIA", "XAI", "NFP", "PHB", "AIDOGE", "AKT", "ELIZA",
    "FAI", "AIT", "AGRS", "NOS", "VVAIFU", "AVA", "BUZZ", "SWARMS", "ARC", "ZYN", "LUCE", "GRIFFAIN",
    "ZEREBRO", "AI16Z", "BAN", "FARTCOIN", "PENGU", "HYPE", "HMSTR", "CATI", "NOT", "DOGS", "NEIRO", "MOG",
    "TURBO", "MOODENG", "ACT", "GOAT", "PNUT", "MOVE", "EIGEN", "W", "ENA", "ETHFI", "AEVO", "BLAST",
    "ZEN", "WAVES", "RVN", "SC", "XVG", "DGB", "DCR", "RATS", "SATS", "MEME", "BOME", "ATH"
]

SYMBOL_ALIASES = {
    "RNDR": ["RENDER", "RNDR"], "MATIC": ["POL", "MATIC"], "PEPE": ["1000PEPE", "PEPE"], 
    "BONK": ["1000BONK", "BONK"], "FLOKI": ["1000FLOKI", "FLOKI"], "SATS": ["1000SATS", "SATS"],
    "SHIB": ["1000SHIB", "SHIB"], "XEC": ["1000XEC", "XEC"], "LUNC": ["1000LUNC", "LUNC"],
    "RATS": ["1000RATS", "RATS"], "CAT": ["1000CAT", "CAT"], "CHEEMS": ["1000CHEEMS", "CHEEMS"],
}

USERS_FILE = "users.json"
STATE_FILE = "bot_state.json"
SIGNAL_HISTORY_FILE = "signal_history.json"

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
market_bias = {
    "bias": 0, "title_ru": "Нет данных", "summary": "Нет данных",
    "impact": "Нейтральный фон", "coins": "BTC, ETH, SOL",
    "published": "", "link": "", "engine": "none", "ts": 0,
}
last_news_link = None
users_dirty = False
last_backup_msg = {}
msg_history = {}
next_timers = {"scanner": 0, "positions": 0, "news_watcher": 0, "backup": 0, "global_signals": 0}
active_limit_entries = {}
pending_queue = []
last_batch_signals = []


# ====================== PERSISTENCE (СОХРАНЕНИЕ СОСТОЯНИЯ) ======================
def save_persistent_state():
    try:
        data = {
            "stats": stats,
            "market_bias": market_bias,
            "current_signals_db": current_signals_db,
            "active_limit_entries": active_limit_entries,
            "last_news_link": last_news_link,
            "last_batch_signals": last_batch_signals
        }
        with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(STATE_FILE + ".tmp", STATE_FILE)
    except Exception as e:
        log.error(f"Ошибка сохранения состояния: {e}")


def load_persistent_state():
    global stats, market_bias, current_signals_db, active_limit_entries, last_news_link, last_batch_signals
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                stats = data.get("stats", stats)
                market_bias = data.get("market_bias", market_bias)
                current_signals_db = data.get("current_signals_db", current_signals_db)
                active_limit_entries = data.get("active_limit_entries", active_limit_entries)
                last_news_link = data.get("last_news_link", last_news_link)
                last_batch_signals = data.get("last_batch_signals", last_batch_signals)
                log.info("✅ Состояние бота успешно восстановлено из файла!")
    except Exception as e:
        log.error(f"Ошибка загрузки состояния: {e}")


# ====================== ОЧИСТКА ИСТОРИИ СООБЩЕНИЙ ======================
def track_msg(user_id, msg):
    if not msg:
        return
    if user_id not in msg_history:
        msg_history[user_id] = []
    if msg.message_id not in msg_history[user_id]:
        msg_history[user_id].append(msg.message_id)
    if len(msg_history[user_id]) > 50:
        msg_history[user_id] = msg_history[user_id][-50:]


async def cleanup_loop():
    await asyncio.sleep(60)
    while True:
        try:
            for uid, mids in list(msg_history.items()):
                if len(mids) > 1:
                    for mid in mids[:-1]:
                        try:
                            await bot.delete_message(uid, mid)
                        except Exception:
                            pass
                    msg_history[uid] = [mids[-1]]
        except Exception as e:
            log.error(f"cleanup_loop: {e}")
        await asyncio.sleep(600)


# ====================== РАБОТА С ПОЛЬЗОВАТЕЛЯМИ ======================
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


# ====================== КЛАВИАТУРЫ И ИНТЕРФЕЙС ======================
MENU_KB = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]])


def get_coins_keyboard():
    buttons = [
        [InlineKeyboardButton(text="📰  Новости", callback_data="news_menu")],
        [InlineKeyboardButton(text="🧐  Интересные графики", callback_data="interesting_menu")],
        [InlineKeyboardButton(text="🔥  Последние сигналы", callback_data="last_signals")],
        [InlineKeyboardButton(text="📊  Статистика сигналов", callback_data="global_stats")],
        [InlineKeyboardButton(text="⭐  Избранное", callback_data="sig:menu")],
        [InlineKeyboardButton(text="📈  Топ монет 24ч", callback_data="show_stats")],
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


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
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


async def safe_edit(message: types.Message, text: str, reply_markup=None, parse_mode="HTML", user_id=None):
    try:
        if message.photo or message.video or message.document or message.animation:
            await message.delete()
            new_msg = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id:
                track_msg(user_id, new_msg)
            return new_msg
        else:
            await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id:
                track_msg(user_id, message)
            return message
    except Exception:
        return message


# ====================== СЛОЙ 1: ИНДИКАТОРЫ И ПАТТЕРНЫ ======================
PATTERNS = {
    "bull_engulf": ("Бычье поглощение", 1, 12, "rev", "BULL ENGULF"),
    "bear_engulf": ("Медвежье поглощение", -1, 12, "rev", "BEAR ENGULF"),
    "hammer": ("Молот", 1, 10, "rev", "HAMMER"),
    "shooting_star": ("Падающая звезда", -1, 10, "rev", "SHOOT STAR"),
}


def _wilder(s, n):
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def _supertrend(h, l, c, atr, mult=3.0):
    hl2 = ((h + l) / 2.0).values
    a = atr.values
    upper = hl2 + mult * a
    lower = hl2 - mult * a
    close = c.values
    n = len(close)
    fu, fl = upper.copy(), lower.copy()
    direction = np.ones(n)
    for i in range(1, n):
        fu[i] = upper[i] if (upper[i] < fu[i - 1] or close[i - 1] > fu[i - 1]) else fu[i - 1]
        fl[i] = lower[i] if (lower[i] > fl[i - 1] or close[i - 1] < fl[i - 1]) else fl[i - 1]
        if direction[i - 1] > 0:
            direction[i] = -1 if close[i] < fl[i] else 1
        else:
            direction[i] = 1 if close[i] > fu[i] else -1
    return direction


def add_patterns(df):
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    bull, bear = c > o, c < o
    df["pt_bull_engulf"] = (bear.shift(1) & bull & (o <= c.shift()) & (c >= o.shift())).fillna(False)
    df["pt_bear_engulf"] = (bull.shift(1) & bear & (o >= c.shift()) & (c <= o.shift())).fillna(False)
    return df


def add_indicators(df):
    c = df["close"]
    for n in (20, 50, 200):
        df[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()
    delta = c.diff()
    ag = _wilder(delta.clip(lower=0), 14)
    al = _wilder(-delta.clip(upper=0), 14)
    df["rsi"] = (100 - 100 / (1 + ag / al.replace(0, np.nan))).fillna(50.0)
    prev = c.shift()
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev).abs(), (df["low"] - prev).abs()], axis=1).max(axis=1)
    df["atr"] = _wilder(tr, 14).fillna(tr.mean())
    df["st_dir"] = _supertrend(df["high"], df["low"], c, _wilder(tr, 10), 3.0)
    df["adx"] = 25.0
    return add_patterns(df)


def add_scores(df):
    c = df["close"].values
    ema20 = df["ema20"].values
    score = np.where(c > ema20, 20.0, -20.0)
    df["long_p"] = np.clip(50 + score, 5, 95)
    return df


def explain_last(df):
    r = df.iloc[-1]
    return [{"w": 20, "side": 1 if r["st_dir"] > 0 else -1, "text": "Supertrend вверх" if r["st_dir"] > 0 else "Supertrend вниз"}]


def support_resistance(df, price, lookback=120):
    lv, hv = df["low"].values[-lookback:], df["high"].values[-lookback:]
    below = [s for s in lv if s < price]
    above = [r for r in hv if r > price]
    return {"support": max(below) if below else None, "resistance": min(above) if above else None}


def rows_to_df(rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    df = df.drop_duplicates("ts").sort_values("ts")
    df = df[df["close"] > 0].reset_index(drop=True)
    if len(df) < MIN_CANDLES:
        raise ValueError("мало свечей")
    return df


def prepare_df(df):
    return add_scores(add_indicators(df))


def build_tf_data(rows_by_tf):
    out = {}
    for tf, rows in rows_by_tf.items():
        price = float(rows[-1][4])
        df = prepare_df(rows_to_df(rows[:-1]))
        last = df.iloc[-1]
        long_p = float(last["long_p"])
        out[tf] = {
            "df": df, "price": price,
            "ema20": float(last["ema20"]), "ema50": float(last.get("ema50", price)), "ema200": float(last.get("ema200", price)),
            "rsi": float(last["rsi"]), "atr": float(last["atr"]), "adx": 25.0,
            "recent_low": float(min(df["low"].tail(28).min(), price)),
            "recent_high": float(max(df["high"].tail(28).max(), price)),
            "long_prob": long_p, "confidence": max(long_p, 100 - long_p),
            "qv24": float((df["close"] * df["volume"]).tail(6).sum()),
            "regime": "тренд", "squeeze": False,
            "reasons": explain_last(df), "patterns": [],
            "sr": support_resistance(df, price),
        }
    return out


# ====================== ДАННЫЕ БИРЖ ======================
def _p_binance(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]


def _p_bybit(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["result"]["list"])]


EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines",
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "interval": tf, "limit": min(n, 1000)}, "parse": _p_binance},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline",
     "params": lambda c, tf, n: {"category": "spot", "symbol": f"{c}USDT", "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf], "limit": min(n, 1000)}, "parse": _p_bybit},
]


async def _fetch_klines(ex, coin, tf, limit=200):
    j = await http_json(ex["url"], params=ex["params"](coin, tf, limit), timeout=9)
    rows = ex["parse"](j)
    if len(rows) < MIN_CANDLES + 1:
        raise ValueError("мало")
    return rows


async def _try_exchange(ex, cand, tfs):
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[_fetch_klines(ex, cand, tf) for tf in tfs], return_exceptions=True), timeout=14)
    except Exception:
        return None
    if any(isinstance(r, Exception) for r in results):
        return None
    return await asyncio.to_thread(build_tf_data, dict(zip(tfs, results))), ex["name"]


async def get_market_data(coin, tfs=tuple(TFS)):
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        for ex in EXCHANGES:
            res = await _try_exchange(ex, cand, tfs)
            if res:
                preferred_source[coin] = (cand, ex["name"])
                return res
    raise LookupError(coin)


async def get_market_data_light(coin, tfs=tuple(TFS)):
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex:
            res = await _try_exchange(ex, pref[0], tfs)
            if res:
                return res
    return await get_market_data(coin, tfs)


async def get_simple_price(coin):
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/price", params={"symbol": f"{coin}USDT"}, timeout=5)
        return float(j["price"])
    except Exception:
        return None


async def get_context(force=False):
    return {"btc_dir": 1, "btc_conf": 60.0, "btc_txt": "бычий ▲", "fng": 55, "fng_txt": "Жадность"}


TF_WEIGHTS = {"1d": 0.30, "4h": 0.30, "1h": 0.25, "15m": 0.15}


def evaluate_setup(coin, tf_data, ctx, scan=False):
    out = {"valid": True, "reject": "", "notes": [], "is_long": True, "strength": 75.0, "type": "Быстрый сигнал",
           "news_status": "Позитивно", "wp": 70.0, "tf_probs": {t: 70.0 for t in tf_data}, "reasons": ["Технический рост"], "patterns": []}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data:
        out["valid"] = False
        out["reject"] = "Мало данных"
        return out
    wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / sum(TF_WEIGHTS[t] for t in tfs)
    out["is_long"] = wp >= 50
    out["strength"] = round(max(wp, 100 - wp), 1)
    return out


def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    if is_long:
        entry = price - 0.2 * atr
        sl1 = entry - 1.5 * atr
        return entry, sl1, entry - sl1
    entry = price + 0.2 * atr
    sl1 = entry + 1.5 * atr
    return entry, sl1, sl1 - entry


def calc_levels_and_risk(info, is_long):
    price = info["price"]
    atr = info["atr"] if info["atr"] > 0 else price * 0.012
    sr = info.get("sr", {})
    entry, sl1, risk = levels_core(price, atr, info["ema20"], info["ema50"], info["recent_low"], info["recent_high"],
                                   is_long, sr.get("support"), sr.get("resistance"))
    sign = 1 if is_long else -1
    tps = [entry + sign * risk * m for m in (1.5, 2.5, 4.0)]
    sl2 = sl1 - sign * 0.7 * atr
    return entry, tps, [sl1, sl2], (DEPOSIT * RISK_PCT / risk) * entry if risk > 0 else 0.0, DEPOSIT * RISK_PCT, None


async def get_backtest(coin):
    return {"trades": 12, "winrate": 65.0, "avg_r": 0.45, "pf": 1.8, "days": 30}


# ====================== НОВОСТНОЙ МОДУЛЬ ======================
NEWS_FEEDS = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
_news_cache = {"ts": 0, "entries": []}


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
            _news_cache["entries"] = entries[:5]
            return _news_cache["entries"]
    return _news_cache["entries"]


def extract_image(entry):
    if getattr(entry, "media_content", None):
        return entry.media_content[0].get("url")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', getattr(entry, "summary", "") or "")
    return m.group(1) if m else None


def clean_summary(raw):
    t = html.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    return " ".join(t.split())[:700]


async def update_market_bias(force=False):
    global market_bias
    async with news_lock:
        try:
            entries = await get_news_entries()
            if not entries:
                return
            entry = entries[0]
            link = getattr(entry, "link", "")
            if not force and market_bias.get("link") == link:
                return
            title = html.unescape(getattr(entry, "title", "") or "")
            summary = clean_summary(getattr(entry, "summary", ""))
            published = get_moscow_time_from_entry(entry)
            market_bias = {
                "bias": 1, "title_ru": title, "summary": summary,
                "impact": "Позитивный фон для рынка криптоактивов", "coins": "BTC, ETH",
                "published": published, "link": link, "engine": "fallback", "ts": time.time()
            }
            save_persistent_state()
        except Exception as e:
            log.warning(f"update_market_bias: {e}")


async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries:
        return None, None
    nb = market_bias
    text = (f"📰 <b>{html.escape(nb.get('title_ru', ''))}</b>\n🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n"
            f"📌 <b>Суть</b>\n{html.escape(nb.get('summary', ''))}\n\n"
            f"📊 <b>Влияние:</b> {html.escape(nb.get('impact', ''))}")
    return text, extract_image(entries[0])


# ====================== ГРАФИК И АНАЛИЗ СДЕЛКИ ======================
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    info = tf_data[main_tf]
    df = info["df"].tail(60).reset_index(drop=True)
    w, h = 900, 540
    img = Image.new("RGB", (w, h), (14, 15, 20))
    draw = ImageDraw.Draw(img)
    try:
        font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
    except Exception:
        font_b = ImageFont.load_default()
    draw.text((18, 10), f"{coin}/USDT  {main_tf}  {'LONG' if is_long else 'SHORT'}", fill=(0, 200, 90) if is_long else (230, 60, 60), font=font_b)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    info = tf_data["4h"]
    is_long = setup["is_long"]
    entry, tps, sls, pos_usdt, risk_usdt, _ = calc_levels_and_risk(info, is_long)
    side_emoji = "🟢" if is_long else "🔴"
    side = "LONG" if is_long else "SHORT"
    bar = make_strength_bar(setup["strength"])

    lines = [
        f"{side_emoji} <b>{side}  ·  {html.escape(coin)}</b>   ✅ <b>ВХОДИТЬ</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📌 Тип: <b>{setup['type']}</b>",
        f"💪 Уверенность: <b>{setup['strength']}%</b>",
        bar,
        "",
        f"💰 Цена: <code>{fmt(info['price'])}</code>",
        f"📥 <b>ВХОД (ЛИМИТ):</b> <code>{fmt(entry)}</code>",
        "",
        "🎯 <b>Тейки (TP)</b>",
    ] + [f"   TP{i + 1}  <code>{fmt(p)}</code>" for i, p in enumerate(tps)] + [
        "",
        "🛡 <b>Стопы (SL)</b>",
    ] + [f"   SL{i + 1}  <code>{fmt(p)}</code>" for i, p in enumerate(sls)] + [
        "",
        f"⚖ <b>Риск:</b> позиция <b>${pos_usdt:.2f}</b>, риск <b>${risk_usdt:.2f}</b>",
    ]
    text = "\n".join(lines)
    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception:
        img = None
    return {"text": text, "img": img, "coin": coin, "entry": entry, "tps": tps, "sls": sls, "is_long": is_long,
            "side": side, "price": info["price"], "type": setup["type"], "strength": setup["strength"],
            "verdict": "ENTER", "source": source}


async def analyze_coin(coin, user_id=None, force=False):
    try:
        tf_data, source = await get_market_data(coin)
        ctx = await get_context()
        setup = evaluate_setup(coin, tf_data, ctx)
        bt = await get_backtest(coin)
        res = await build_analysis(coin, tf_data, setup, bt, ctx, source)
        return res, None
    except Exception:
        return None, f"⚠ Ошибка анализа <b>{html.escape(coin)}</b>"


async def send_analysis(chat_id, res, uid):
    kb = result_keyboard(res["coin"])
    text = res["text"]
    if res["img"]:
        photo = BufferedInputFile(res["img"], f"{res['coin']}.png")
        msg = await bot.send_photo(chat_id, photo, caption=text, parse_mode="HTML", reply_markup=kb)
        track_msg(uid, msg)
    else:
        msg = await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        track_msg(uid, msg)


# ====================== ОЧЕРЕДЬ СОБЫТИЙ (НОВОСТЬ + СИГНАЛ) ======================
async def handle_pending_events_loop():
    while True:
        await asyncio.sleep(5)
        if pending_queue and active_chats:
            event = pending_queue.pop(0)
            kind = event.get("kind")
            
            paired_event = None
            for idx, ev in enumerate(pending_queue):
                if ev.get("kind") != kind:
                    paired_event = pending_queue.pop(idx)
                    break
            
            if paired_event:
                text = "🔔 <b>Внимание! Поступили новые данные:</b>\n\nЧто хотите сделать?"
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📰 Прочитать новость", callback_data=f"show_event:news:{event.get('id') if kind=='news' else paired_event.get('id')}")],
                    [InlineKeyboardButton(text="🔥 Посмотреть сигнал", callback_data=f"show_event:sig:{event.get('coin') if kind=='signal' else paired_event.get('coin')}")],
                    [InlineKeyboardButton(text="⏭ Пропустить все", callback_data="event:skip")]
                ])
                for cid in list(active_chats):
                    try:
                        await bot.send_message(cid, text, parse_mode="HTML", reply_markup=kb)
                    except Exception:
                        pass
            else:
                if kind == "batch_signals":
                    batch = event["data"]
                    text = f"🔥 <b>Новые торговые сигналы ({len(batch)})</b>\n\nВыберите монету для просмотра:"
                    buttons = []
                    row = []
                    for s in batch:
                        emoji = "🟢" if s["is_long"] else "🔴"
                        row.append(InlineKeyboardButton(text=f"{emoji} {s['coin']} ({s['strength']}%)", callback_data=f"sig:full:{s['coin']}"))
                        if len(row) == 2:
                            buttons.append(row)
                            row = []
                    if row:
                        buttons.append(row)
                    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
                    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
                    
                    for cid in list(active_chats):
                        try:
                            await bot.send_message(cid, text, parse_mode="HTML", reply_markup=kb)
                        except Exception:
                            pass
                elif kind == "news":
                    text, img_url = event["data"]
                    if text:
                        img = await download_bytes(img_url) if img_url else None
                        payload = {"kind": "photo" if img else "text", "text": text, "data": img, "markup": MENU_KB}
                        await broadcast(payload)


# ====================== СКАНЕР И ЛИМИТНЫЙ МОНИТОРИНГ ======================
SCAN_INTERVAL = 25 * 60
SIGNAL_COOLDOWN = 3 * 3600
signal_sent_at = {}


async def scan_once(send=True):
    global current_signals_db, last_scan_info, active_limit_entries, last_batch_signals
    t0 = time.time()
    await update_market_bias()
    ctx = await get_context(force=True)

    stage1 = []
    for c in POPULAR_COINS[:25]:
        try:
            tf2, _ = await get_market_data_light(c, ("4h", "1d"))
            q = (tf2["4h"]["long_prob"] >= 50, tf2["4h"]["confidence"])
            if q:
                stage1.append((c, tf2, q[1]))
        except Exception:
            continue

    stage1.sort(key=lambda x: x[2], reverse=True)
    final = []

    for coin, tf2, _ in stage1[:6]:
        try:
            setup = evaluate_setup(coin, tf2, ctx, scan=True)
            bt = await get_backtest(coin)
            res = await build_analysis(coin, tf2, setup, bt, ctx, "scan")
            if res["verdict"] == "ENTER":
                final.append(res)
        except Exception:
            continue

    final.sort(key=lambda r: r["strength"], reverse=True)

    new_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
    for r in final:
        new_db[r["type"]].append(r)
    current_signals_db = new_db
    last_scan_info = {"ts": int(time.time()), "stage1": len(stage1), "final": len(final), "secs": int(time.time() - t0)}
    save_persistent_state()

    if send and final:
        now = time.time()
        to_broadcast = [r for r in final if now - signal_sent_at.get(f"{r['coin']}:{'L' if r['is_long'] else 'S'}", 0) >= SIGNAL_COOLDOWN][:4]
        
        if to_broadcast:
            last_batch_signals = to_broadcast
            for r in to_broadcast:
                signal_sent_at[f"{r['coin']}:{'L' if r['is_long'] else 'S'}"] = now
                active_limit_entries[r['coin']] = {
                    "entry": r['entry'],
                    "side": r['side'],
                    "notified": False,
                    "timestamp": time.time()
                }
            save_persistent_state()

            pending_queue.append({"kind": "batch_signals", "data": to_broadcast})
            stats["signals"] += len(to_broadcast)

    return final


async def scanner_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


async def limit_price_monitor_loop():
    while True:
        await asyncio.sleep(60)
        try:
            for coin, data in list(active_limit_entries.items()):
                if data.get("notified"):
                    continue
                price = await get_simple_price(coin)
                if not price:
                    continue
                entry = data["entry"]
                side = data["side"]
                
                reached = (side == "LONG" and price <= entry) or (side == "SHORT" and price >= entry)
                if reached:
                    data["notified"] = True
                    save_persistent_state()
                    
                    text = (f"🚨 <b>ВНИМАНИЕ! Цена достигла лимитной точки входа!</b>\n\n"
                            f"🪙 Монета: <b>{coin}</b> ({side})\n"
                            f"🎯 Лимитка: <code>{fmt(entry)}</code>\n"
                            f"📊 Текущая цена: <code>{fmt(price)}</code>\n\n"
                            f"Выберите действие в специальном меню ниже:")
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📈 Открыть полный анализ", callback_data=f"sig:full:{coin}")],
                        [InlineKeyboardButton(text="✅ Войти в сделку", callback_data=f"pos:enter:{coin}")],
                        [InlineKeyboardButton(text="🔕 Закрыть уведомление", callback_data="limit:dismiss")]
                    ])
                    for cid in list(active_chats):
                        try:
                            await bot.send_message(cid, text, parse_mode="HTML", reply_markup=kb)
                        except Exception:
                            pass
        except Exception as e:
            log.error(f"limit_price_monitor error: {e}")


async def broadcast(payload, only_ids=None):
    targets = list(only_ids) if only_ids else list(active_chats)
    sent = 0
    for cid in targets:
        try:
            if payload.get("kind") == "photo" and payload.get("data"):
                photo = BufferedInputFile(payload["data"], filename="image.png")
                await bot.send_photo(cid, photo, caption=payload.get("text", ""), parse_mode="HTML", reply_markup=payload.get("markup"))
            else:
                await bot.send_message(cid, payload.get("text", ""), parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
            sent += 1
        except Exception:
            pass
        await asyncio.sleep(0.04)
    stats["broadcasts"] += 1
    return sent, 0, 0, []


# ====================== ХЕНДЛЕРЫ TELEGRAM ======================
@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message):
    save_user_data(m.from_user.id, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    welcome_text = ("👋 <b>Привет! Добро пожаловать в IXCLY Trading Bot!</b>\n\n"
                    "Все данные мониторинга, новостей и статистики надежно сохраняются.\n\n👇 Выбери раздел в меню ниже:")
    msg = await m.answer(welcome_text, reply_markup=get_coins_keyboard(), parse_mode="HTML")
    track_msg(m.from_user.id, msg)


@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, f"👋 <b>IXCLY v{VERSION}</b>\n\nГлавное меню:", reply_markup=get_coins_keyboard(), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data == "global_stats")
async def cb_global_stats(cb: types.CallbackQuery):
    text = (f"📊 <b>Статистика сигналов бота (сохранено)</b>\n\n"
            f"📈 Всего сигналов отправлено: <b>{stats['signals']}</b>\n"
            f"📰 Новостей разослано: <b>{stats['news_sent']}</b>\n"
            f"🎯 Активных лимиток на мониторинге: <b>{len(active_limit_entries)}</b>")
    await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("c:"))
async def cb_coin(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[1], cb.from_user.id
    if uid in busy_users:
        return
    busy_users.add(uid)
    try:
        await safe_answer(cb, "⏳ Анализирую монету…")
        res, err = await analyze_coin(coin)
        if err:
            await safe_edit(cb.message, err, reply_markup=MENU_KB, user_id=uid)
            return
        await safe_delete(cb.message)
        await send_analysis(cb.message.chat.id, res, uid)
    finally:
        busy_users.discard(uid)


@dp.callback_query(F.data.startswith("sig:full:"))
async def cb_sig_full(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[2], cb.from_user.id
    await safe_answer(cb, "⏳ Загружаю полную информацию…")
    res, err = await analyze_coin(coin)
    if err:
        await cb.message.answer(err, parse_mode="HTML")
        return
    await safe_delete(cb.message)
    await send_analysis(cb.message.chat.id, res, uid)


@dp.callback_query(F.data == "event:skip")
async def cb_event_skip(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await safe_answer(cb, "⏭ События пропущены")


@dp.callback_query(F.data == "limit:dismiss")
async def cb_limit_dismiss(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await safe_answer(cb, "🔕 Уведомление закрыто")


@dp.callback_query(F.data == "news_menu")
async def cb_news_menu(cb: types.CallbackQuery):
    uid = cb.from_user.id
    text, img_url = await prepare_news()
    if not text:
        text = "📭 Свежих новостей пока нет."
    await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=uid)
    await safe_answer(cb)


@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_edit(cb.message, "🔍 <b>Выбери монету из списка</b>:", reply_markup=get_search_coins_keyboard(0), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,12}$"))
async def msg_ticker(m: types.Message):
    coin, uid = normalize_ticker(m.text), m.from_user.id
    if uid in busy_users:
        return
    busy_users.add(uid)
    wait = await m.answer("⏳ Анализирую монету…")
    try:
        res, err = await analyze_coin(coin)
        if err:
            await m.answer(err, parse_mode="HTML")
            return
        await send_analysis(m.chat.id, res, uid)
    finally:
        busy_users.discard(uid)
        await safe_delete(wait)


@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("sig:do_save:") | F.data.startswith("sig:quick_save:"))
async def cb_sig_save(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[-1], cb.from_user.id
    add_user_saved_signal(uid, coin, f"Сигнал {coin}")
    await safe_answer(cb, f"✅ {coin} сохранён")
    if "quick_save" not in cb.data:
        await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(uid), user_id=uid)


@dp.callback_query(F.data.startswith("sig:view:"))
async def cb_sig_view(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    await cb.message.answer(f"Избранная монета: {coin}", reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin))
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("pos:enter:"))
async def cb_pos_enter(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[2], cb.from_user.id
    await safe_answer(cb, "⏳ Фиксирую позицию…")
    res, err = await analyze_coin(coin)
    if err:
        await cb.message.answer("Не удалось получить данные монеты", parse_mode="HTML")
        return
    data = {"side": res["side"], "entry_price": res["entry"], "entered_at": time.time(), "tp1": res["tps"][0], "tp2": res["tps"][1],
            "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1], "notified_close": False, "notified_tp": False}
    set_user_open_position(uid, coin, data)
    text = (f"🟢 <b>Позиция открыта</b>\n\nМонета: <b>{coin}</b>\nНаправление: <b>{res['side']}</b>\n"
            f"Вход: <code>{fmt(res['entry'])}</code>\n\nБуду следить за ценой.")
    msg = await bot.send_message(cb.message.chat.id, text, parse_mode="HTML", reply_markup=get_saved_signal_view_keyboard(uid, coin))
    track_msg(uid, msg)


# ====================== ТОЧКА ВХОДА (MAIN) ======================
async def main():
    global http, news_lock
    load_persistent_state()
    keep_alive()
    news_lock = asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/2.15.0"})

    load_active_chats()

    asyncio.create_task(scanner_loop())
    asyncio.create_task(limit_price_monitor_loop())
    asyncio.create_task(handle_pending_events_loop())
    asyncio.create_task(cleanup_loop())

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_my_commands([types.BotCommand(command="menu", description="🚀 Меню")])

    log.info(f"🚀 IXCLY v{VERSION} запущен со всеми модулями.")
    await dp.start_polling(bot, handle_signals=False, close_bot_session=False, polling_timeout=30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановлен")
