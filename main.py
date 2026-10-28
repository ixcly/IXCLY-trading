# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v2.11.4 - исправлены версии моделей ИИ и дублирование рассылки
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
                log.warning("ℹ getUpdates-конфликт: рядом работает другой экземпляр бота (при деплое Render это нормально).")
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

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-chat:free")
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

ADMIN_IDS = set()
if ADMIN_ID:
    for x in re.split(r"[,\s]+", ADMIN_ID):
        if x.lstrip("-").isdigit():
            ADMIN_IDS.add(int(x))

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не задан")

VERSION = "2.11.4"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}

POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

# ---- пороги движка сигналов ----
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
elif INPUT_BOT_TOKEN:
    log.warning("INPUT_BOT_TOKEN совпадает с BOT_TOKEN: админ-бот выключен")

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
    "RNDR": ["RENDER", "RNDR"], 
    "MATIC": ["POL", "MATIC"],
    "PEPE": ["1000PEPE", "PEPE"], 
    "BONK": ["1000BONK", "BONK"],
    "FLOKI": ["1000FLOKI", "FLOKI"], 
    "SATS": ["1000SATS", "SATS"],
    "SHIB": ["1000SHIB", "SHIB"],
    "XEC": ["1000XEC", "XEC"],
    "LUNC": ["1000LUNC", "LUNC"],
    "RATS": ["1000RATS", "RATS"],
    "CAT": ["1000CAT", "CAT"],
    "CHEEMS": ["1000CHEEMS", "CHEEMS"],
}

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
users_col = None


def init_mongo():
    global users_col
    users_col = None
    log.warning("⚠ MongoDB отключена. Пользователи и стейт хранятся в bot_state.json (бэкап в админ-бот)")


# ====================== ПОЛНЫЙ СТЕЙТ И БАЗА ДАННЫХ ======================
STATE_FILE = "bot_state.json"

def _load_full_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log.error(f"Ошибка загрузки state: {e}")
    return {}

def _save_full_state(state):
    global users_dirty
    try:
        with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(STATE_FILE + ".tmp", STATE_FILE)
        users_dirty = True
    except Exception as e:
        log.error(f"Ошибка записи state.json: {e}")

def _load_users():
    data = _load_full_state()
    return data.get("users", {})

def _save_users(users):
    state = _load_full_state()
    state["users"] = users
    _save_full_state(state)

def _load_signal_history():
    data = _load_full_state()
    hist = data.get("signal_history", [])
    return [x for x in hist if time.time() - x.get("ts", 0) < 30 * 86400]

def _save_signal_history(hist):
    state = _load_full_state()
    state["signal_history"] = hist[-2000:]
    _save_full_state(state)

def _load_current_signals_db():
    data = _load_full_state()
    return data.get("current_signals_db", {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []})

def _save_current_signals_db(db):
    state = _load_full_state()
    state["current_signals_db"] = db
    _save_full_state(state)


# ====================== ОЧИСТКА ИСТОРИИ ======================
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


# ====================== ПОЛЬЗОВАТЕЛИ ======================
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


# ====================== БЭКАП ПОЛНОГО СТЕЙТА ======================
async def telegram_backup(reason="auto"):
    if not admin_bot or not ADMIN_IDS:
        return False
    state = _load_full_state()
    if not state:
        return False
    
    users_count = len(state.get("users", {}))
    signals_count = len(state.get("signal_history", []))
    
    data = json.dumps(state, ensure_ascii=False).encode("utf-8")
    ok = False
    for aid in ADMIN_IDS:
        try:
            msg = await admin_bot.send_document(
                aid, BufferedInputFile(data, "bot_state.json"),
                caption=f"💾 ПОЛНЫЙ БЭКАП БОТА · Пользователей: {users_count} · Сигналов в истории: {signals_count} · Причина: {reason} · {datetime.now().strftime('%d.%m %H:%M')}",
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
            log.warning(f"Полный бэкап админу {aid} не отправлен: {e}")
    return ok


async def telegram_restore():
    global current_signals_db
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
                with open(STATE_FILE, "w", encoding="utf-8") as out_f:
                    json.dump(data, out_f, ensure_ascii=False, indent=2)
                
                if "current_signals_db" in data:
                    current_signals_db = data["current_signals_db"]
                
                last_backup_msg[aid] = pm.message_id
                users_len = len(data.get("users", {}))
                hist_len = len(data.get("signal_history", []))
                log.info(f"✅ Успешно восстановлено из бэкапа: пользователей: {users_len}, история сигналов: {hist_len}")
                return users_len
        except Exception as e:
            log.warning(f"Восстановление полного стейта не удалось: {e}")
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
async def errors_handler(event: ErrorEvent):
    exc = event.exception
    update = event.update
    if isinstance(exc, TelegramNetworkError):
        log.warning(f"Сетевая ошибка Telegram: {exc}")
        try:
            if update.message:
                await update.message.answer("⏳ Соединение нестабильно, подождите несколько секунд и повторите.")
            elif update.callback_query:
                await update.callback_query.answer("⏳ Соединение нестабильно, повторите.", show_alert=True)
        except Exception:
            pass
        return True
    if isinstance(exc, TelegramBadRequest) and ("query is too old" in str(exc).lower() or "message is not modified" in str(exc).lower()):
        return True
    log.error(f"Необработанная ошибка в обработчике: {exc}", exc_info=exc)
    return True


# ====================== КЛАВИАТУРЫ ======================
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
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            if user_id:
                track_msg(user_id, message)
            return message
        if "no text in the message" in err or "there is no text" in err:
            try:
                await message.delete()
            except Exception:
                pass
            new_msg = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id:
                track_msg(user_id, new_msg)
            return new_msg
        return message
    except Exception:
        return message


# =====================================================================
#                    СЛОЙ 1: ИНДИКАТОРЫ И СВЕЧНЫЕ ПАТТЕРНЫ
# =====================================================================
PATTERNS = {
    "bull_engulf": ("Бычье поглощение", 1, 12, "rev", "BULL ENGULF"),
    "bear_engulf": ("Медвежье поглощение", -1, 12, "rev", "BEAR ENGULF"),
    "hammer": ("Молот", 1, 10, "rev", "HAMMER"),
    "shooting_star": ("Падающая звезда", -1, 10, "rev", "SHOOT STAR"),
    "morning_star": ("Утренняя звезда", 1, 15, "rev", "MORNING STAR"),
    "evening_star": ("Вечерняя звезда", -1, 15, "rev", "EVENING STAR"),
    "three_white": ("Три белых солдата", 1, 12, "cont", "3 WHITE"),
    "three_black": ("Три чёрные вороны", -1, 12, "cont", "3 BLACK"),
    "piercing": ("Просвет в облаках", 1, 8, "rev", "PIERCING"),
    "dark_cloud": ("Завеса из тёмных облаков", -1, 8, "rev", "DARK CLOUD"),
    "bull_harami": ("Бычье харами", 1, 5, "rev", "BULL HARAMI"),
    "bear_harami": ("Медвежье харами", -1, 5, "rev", "BEAR HARAMI"),
    "tweezer_bottom": ("Пинцет (дно)", 1, 6, "rev", "TWEEZER BOT"),
    "tweezer_top": ("Пинцет (вершина)", -1, 6, "rev", "TWEEZER TOP"),
    "marubozu_bull": ("Бычий марубозу", 1, 6, "cont", "MARUBOZU+"),
    "marubozu_bear": ("Медвежий марубозу", -1, 6, "cont", "MARUBOZU-"),
    "doji": ("Доджи (неопределённость)", 0, 0, "neutral", "DOJI"),
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
    body = (c - o).abs()
    rng = (h - l).replace(0, np.nan)
    upper = h - np.maximum(o, c)
    lower = np.minimum(o, c) - l
    avg_body = body.rolling(10, min_periods=3).mean().fillna(body)
    bull, bear = c > o, c < o

    def sh(s, k=1):
        return s.shift(k, fill_value=False)

    p = {}
    p["doji"] = body <= 0.1 * rng
    p["hammer"] = (lower >= 2 * body) & (upper <= 0.15 * rng) & (body > 0.05 * rng)
    p["shooting_star"] = (upper >= 2 * body) & (lower <= 0.15 * rng) & (body > 0.05 * rng)
    p["bull_engulf"] = sh(bear) & bull & (o <= c.shift()) & (c >= o.shift()) & (body > body.shift())
    p["bear_engulf"] = sh(bull) & bear & (o >= c.shift()) & (c <= o.shift()) & (body > body.shift())
    p["morning_star"] = (sh(bear, 2) & (body.shift(2) > avg_body.shift(2)) & (body.shift(1) < 0.5 * avg_body.shift(1))
                         & bull & (c > (o.shift(2) + c.shift(2)) / 2))
    p["evening_star"] = (sh(bull, 2) & (body.shift(2) > avg_body.shift(2)) & (body.shift(1) < 0.5 * avg_body.shift(1))
                         & bear & (c < (o.shift(2) + c.shift(2)) / 2))
    p["three_white"] = (bull & sh(bull) & sh(bull, 2) & (c > c.shift()) & (c.shift() > c.shift(2))
                        & (o > o.shift()) & (o.shift() > o.shift(2)) & (o < c.shift()) & (o.shift() < c.shift(2))
                        & (body > 0.6 * avg_body))
    p["three_black"] = (bear & sh(bear) & sh(bear, 2) & (c < c.shift()) & (c.shift() < c.shift(2))
                        & (o < o.shift()) & (o.shift() < o.shift(2)) & (o > c.shift()) & (o.shift() > c.shift(2))
                        & (body > 0.6 * avg_body))
    p["piercing"] = sh(bear) & bull & (o < c.shift()) & (c > (o.shift() + c.shift()) / 2) & (c < o.shift())
    p["dark_cloud"] = sh(bull) & bear & (o > c.shift()) & (c < (o.shift() + c.shift()) / 2) & (c > o.shift())
    p["bull_harami"] = sh(bear) & bull & (o > c.shift()) & (c < o.shift()) & (body < 0.6 * body.shift())
    p["bear_harami"] = sh(bull) & bear & (o < c.shift()) & (c > o.shift()) & (body < 0.6 * body.shift())
    tol = 0.1 * df["atr"]
    p["tweezer_bottom"] = sh(bear) & bull & ((l - l.shift()).abs() <= tol)
    p["tweezer_top"] = sh(bull) & bear & ((h - h.shift()).abs() <= tol)
    p["marubozu_bull"] = bull & ((upper + lower) <= 0.1 * rng) & (body >= 1.3 * avg_body)
    p["marubozu_bear"] = bear & ((upper + lower) <= 0.1 * rng) & (body >= 1.3 * avg_body)
    for name, s in p.items():
        df[f"pt_{name}"] = s.fillna(False).astype(bool)
    return df


def pattern_scores(df):
    c, ema20, rsi, bbp = df["close"], df["ema20"], df["rsi"], df["bb_pct"]
    down_ctx = ((c < ema20) | (rsi < 45) | (bbp < 0.35)).values
    up_ctx = ((c > ema20) | (rsi > 55) | (bbp > 0.65)).values
    trend_up = (df["ema20"] > df["ema50"]).values
    score = np.zeros(len(df))
    for name, (_ru, side, wgt, kind, _en) in PATTERNS.items():
        if side == 0:
            continue
        flag = df[f"pt_{name}"].values.astype(float)
        if kind == "rev":
            ctx = np.where(down_ctx if side > 0 else up_ctx, 1.0, 0.4)
        else:
            ctx = np.where(trend_up if side > 0 else ~trend_up, 1.0, 0.5)
        score += side * wgt * flag * ctx
    return np.clip(score, -25, 25)


def add_indicators(df):
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    for n in (9, 20, 50, 200):
        df[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()

    delta = c.diff()
    ag = _wilder(delta.clip(lower=0), 14)
    al = _wilder(-delta.clip(upper=0), 14)
    rs = ag / al.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    df["rsi"] = rsi.where(al != 0, 100.0).fillna(50.0)
    rmin, rmax = df["rsi"].rolling(14).min(), df["rsi"].rolling(14).max()
    stoch = (df["rsi"] - rmin) / (rmax - rmin).replace(0, np.nan)
    df["stoch_k"] = stoch.rolling(3).mean().fillna(0.5)

    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["macd"] = macd
    df["macd_sig"] = macd.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_sig"]

    prev = c.shift()
    tr = pd.concat([h - l, (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    df["atr"] = _wilder(tr, 14).fillna(tr.mean())
    up, down = h.diff(), -l.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    atr_safe = df["atr"].replace(0, np.nan)
    df["plus_di"] = (100 * _wilder(plus_dm, 14) / atr_safe).fillna(0)
    df["minus_di"] = (100 * _wilder(minus_dm, 14) / atr_safe).fillna(0)
    dx = 100 * (df["plus_di"] - df["minus_di"]).abs() / (df["plus_di"] + df["minus_di"]).replace(0, np.nan)
    df["adx"] = _wilder(dx.fillna(0), 14)

    mid = c.rolling(20).mean()
    std = c.rolling(20).std()
    upper_b, lower_b = mid + 2 * std, mid - 2 * std
    df["bb_pct"] = ((c - lower_b) / (upper_b - lower_b).replace(0, np.nan)).fillna(0.5)
    bw = ((upper_b - lower_b) / mid.replace(0, np.nan))
    df["squeeze"] = (bw <= bw.rolling(100, min_periods=30).quantile(0.15)).fillna(False)

    atr10 = _wilder(tr, 10).fillna(tr.mean())
    df["st_dir"] = _supertrend(h, l, c, atr10, 3.0)

    tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
    span_a = ((tenkan + kijun) / 2).shift(26)
    span_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
    top, bot = np.maximum(span_a, span_b), np.minimum(span_a, span_b)
    df["tenkan"], df["kijun"] = tenkan.fillna(c), kijun.fillna(c)
    df["ichi_pos"] = np.where(top.isna(), 0, np.where(c > top, 1, np.where(c < bot, -1, 0)))

    tp = (h + l + c) / 3
    df["vwap"] = ((tp * v).rolling(20).sum() / v.rolling(20).sum().replace(0, np.nan)).fillna(c)
    raw = tp * v
    pos = raw.where(tp > tp.shift(), 0.0)
    neg = raw.where(tp < tp.shift(), 0.0)
    df["mfi"] = (100 - 100 / (1 + pos.rolling(14).sum() / neg.rolling(14).sum().replace(0, np.nan))).fillna(50)
    sma_tp = tp.rolling(20).mean()
    mad = tp.rolling(20).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    df["cci"] = ((tp - sma_tp) / (0.015 * mad.replace(0, np.nan))).fillna(0)
    hh, ll = h.rolling(14).max(), l.rolling(14).min()
    df["wr"] = (-100 * (hh - c) / (hh - ll).replace(0, np.nan)).fillna(-50)

    df["obv"] = (np.sign(c.diff()) * v).fillna(0).cumsum()
    df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
    df["rel_vol"] = (v / v.rolling(20).mean().replace(0, np.nan)).fillna(1.0)
    df["vol_delta"] = ((c - o) / (h - l).replace(0, np.nan) * v).fillna(0)

    std50 = c.rolling(50).std().replace(0, np.nan)
    df["z_score"] = ((c - df["ema50"]) / std50).fillna(0)
    df["momentum"] = c.pct_change(10).fillna(0)

    df["brk_up"] = (c > h.shift(1).rolling(20).max()).fillna(False)
    df["brk_dn"] = (c < l.shift(1).rolling(20).min()).fillna(False)
    low_min, high_max = l.shift(1).rolling(14).min(), h.shift(1).rolling(14).max()
    rsi_min, rsi_max = df["rsi"].shift(1).rolling(14).min(), df["rsi"].shift(1).rolling(14).max()
    df["div_bull"] = ((l <= low_min) & (df["rsi"] > rsi_min + 3)).fillna(False)
    df["div_bear"] = ((h >= high_max) & (df["rsi"] < rsi_max - 3)).fillna(False)

    return add_patterns(df)


def add_scores(df):
    n = len(df)
    c = df["close"].values
    adx = df["adx"].values
    trending, ranging = adx >= 25, adx < 20

    def mix(t, r):
        return np.where(trending, t, np.where(ranging, r, (t + r) / 2.0))

    ema20, ema50, ema200 = df["ema20"].values, df["ema50"].values, df["ema200"].values
    hist = df["macd_hist"].values
    hist_prev = np.concatenate([[hist[0]], hist[:-1]])

    stack = np.where((c > ema20) & (ema20 > ema50), 20, np.where((c < ema20) & (ema20 < ema50), -20, 0))
    stack = stack + np.where(c > ema200, 10, -10)
    st = np.where(df["st_dir"].values > 0, 20, -20)
    ichi = df["ichi_pos"].values * 15 + np.where(df["tenkan"].values > df["kijun"].values, 5, -5)
    macd_s = np.where(hist > 0, 8, -8) + np.where(hist > hist_prev, 4, -4)
    di = np.where(adx >= 20, np.where(df["plus_di"].values > df["minus_di"].values, 10, -10), 0)
    trend = np.clip(stack + st + ichi + macd_s + di, -100, 100)

    rsi, sk, cci = df["rsi"].values, df["stoch_k"].values, df["cci"].values
    wr, mfi, bbp = df["wr"].values, df["mfi"].values, df["bb_pct"].values
    z, mom_roc = df["z_score"].values, df["momentum"].values
    zeros = np.zeros(n)
    mom = (mix(np.clip((rsi - 50) * 0.8, -20, 20), np.clip((50 - rsi) * 0.9, -25, 25))
           + mix((sk - 0.5) * 30, (0.5 - sk) * 40)
           + mix(np.clip(cci / 10, -15, 15), np.clip(-cci / 8, -20, 20))
           + mix(((wr + 50) / 50) * 8, -((wr + 50) / 50) * 12)
           + mix((mfi - 50) / 50 * 8, (50 - mfi) / 50 * 12)
           + mix(np.where(bbp > 1, 10, np.where(bbp < 0, -10, (bbp - 0.5) * 10)), np.clip((0.5 - bbp) * 40, -20, 20))
           + mix(zeros, np.clip(-z * 7, -15, 15))
           + mix(np.clip(mom_roc * 400, -10, 10), zeros))
    mom = np.clip(mom, -100, 100)

    o = df["open"].values
    pat = pattern_scores(df)
    vol_conf = np.where(df["rel_vol"].values > 1.5, np.where(c > o, 10, -10), 0)
    obv_s = np.where(df["obv"].values > df["obv_ema"].values, 8, -8)
    div = np.where(df["div_bull"].values, 15, 0) - np.where(df["div_bear"].values, 15, 0)
    brk = np.where(df["brk_up"].values, 10, 0) - np.where(df["brk_dn"].values, 10, 0)
    vwap_s = np.where(c > df["vwap"].values, 5, -5)
    pa = np.clip((pat + vol_conf + obv_s + div + brk + vwap_s) * 1.3, -100, 100)

    w_t = np.where(trending, 0.5, np.where(ranging, 0.25, 0.4))
    w_m = np.where(trending, 0.2, np.where(ranging, 0.45, 0.3))
    score = w_t * trend + w_m * mom + (1 - w_t - w_m) * pa
    df["s_trend"], df["s_mom"], df["s_pa"] = trend, mom, pa
    df["long_p"] = np.nan_to_num(np.clip(50 + score / 2, 5, 95), nan=50.0)
    return df


def explain_last(df):
    r = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else r
    out = []

    def add(w, side, text):
        out.append({"w": w, "side": side, "text": text})

    c = r["close"]
    add(20, 1 if r["st_dir"] > 0 else -1, "Supertrend вверх" if r["st_dir"] > 0 else "Supertrend вниз")
    if c > r["ema20"] > r["ema50"] > r["ema200"]:
        add(22, 1, "Цена выше EMA 20/50/200")
    elif c < r["ema20"] < r["ema50"] < r["ema200"]:
        add(22, -1, "Цена ниже EMA 20/50/200")
    if r["ichi_pos"] > 0:
        add(16, 1, "Цена выше облака Ichimoku")
    elif r["ichi_pos"] < 0:
        add(16, -1, "Цена ниже облака Ichimoku")
    if r["adx"] >= 25:
        add(15, 1 if r["plus_di"] > r["minus_di"] else -1, f"Сильный тренд (ADX {r['adx']:.0f})")
    if r["macd_hist"] > 0 and r["macd_hist"] > prev["macd_hist"]:
        add(10, 1, "MACD: бычий импульс растёт")
    elif r["macd_hist"] < 0 and r["macd_hist"] < prev["macd_hist"]:
        add(10, -1, "MACD: медвежий импульс растёт")
    if r["rsi"] < 30:
        add(14, 1, f"RSI перепродан ({r['rsi']:.0f})")
    elif r["rsi"] > 70:
        add(14, -1, f"RSI перекуплен ({r['rsi']:.0f})")
    if r["div_bull"]:
        add(18, 1, "Бычья дивергенция RSI")
    if r["div_bear"]:
        add(18, -1, "Медвежья дивергенция RSI")
    if r["bb_pct"] < 0:
        add(12, 1, "Цена под нижней полосой Боллинджера")
    elif r["bb_pct"] > 1:
        add(12, -1, "Цена над верхней полосой Боллинджера")
    if r["rel_vol"] > 1.5:
        add(12, 1 if c > r["open"] else -1, f"Всплеск объёма ×{r['rel_vol']:.1f}")
    if r["brk_up"]:
        add(14, 1, "Пробой 20-свечного максимума")
    if r["brk_dn"]:
        add(14, -1, "Пробой 20-свечного минимума")
    add(6, 1 if r["obv"] > r["obv_ema"] else -1, "OBV выше средней (приток)" if r["obv"] > r["obv_ema"] else "OBV ниже средней (отток)")
    for name, (ru, side, wgt, _k, _en) in PATTERNS.items():
        if side != 0 and bool(r[f"pt_{name}"]):
            add(wgt + 8, side, ru)
    return out


def last_patterns(df, depth=2):
    res = []
    for i in range(1, min(depth, len(df)) + 1):
        r = df.iloc[-i]
        for name, (ru, side, _w, _k, en) in PATTERNS.items():
            if bool(r[f"pt_{name}"]):
                res.append((ru, side, en))
    seen, out = set(), []
    for item in res:
        if item[0] not in seen:
            seen.add(item[0])
            out.append(item)
    return out


def support_resistance(df, price, k=3, lookback=120):
    hv, lv = df["high"].values[-lookback:], df["low"].values[-lookback:]
    n = len(hv)
    res, sup = [], []
    for i in range(k, n - k):
        if hv[i] == hv[i - k:i + k + 1].max():
            res.append(float(hv[i]))
        if lv[i] == lv[i - k:i + k + 1].min():
            sup.append(float(lv[i]))
    below = [s for s in sup if s < price]
    above = [r for r in res if r > price]
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
        adx = float(last["adx"])
        out[tf] = {
            "df": df, "price": price,
            "ema20": float(last["ema20"]), "ema50": float(last["ema50"]), "ema200": float(last["ema200"]),
            "rsi": float(last["rsi"]), "atr": float(last["atr"]), "adx": adx,
            "macd_hist": float(last["macd_hist"]), "bb_pct": float(last["bb_pct"]),
            "z_score": float(last["z_score"]), "vol_delta": float(last["vol_delta"]),
            "recent_low": float(min(df["low"].tail(28).min(), price)),
            "recent_high": float(max(df["high"].tail(28).max(), price)),
            "long_prob": long_p, "confidence": max(long_p, 100 - long_p),
            "qv24": float((df["close"] * df["volume"]).tail(6).sum()),
            "regime": "тренд" if adx >= 25 else "флэт" if adx < 20 else "переход",
            "squeeze": bool(last["squeeze"]),
            "reasons": explain_last(df), "patterns": last_patterns(df),
            "sr": support_resistance(df, price),
        }
    return out


# =====================================================================
#                              ДАННЫЕ БИРЖ
# =====================================================================
def _p_binance(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]


def _p_okx(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["data"])]


def _p_bybit(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["result"]["list"])]


EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines",
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "interval": tf, "limit": min(n, 1000)}, "parse": _p_binance},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles",
     "params": lambda c, tf, n: {"instId": f"{c}-USDT", "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf], "limit": str(min(n, 300))}, "parse": _p_okx},
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


async def fetch_history(coin, tf="4h", limit=1000):
    order, seen = [], set()
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex:
            order.append((pref[0], ex))
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        for ex in EXCHANGES:
            order.append((cand, ex))
    for cand, ex in order:
        key = (cand, ex["name"])
        if key in seen:
            continue
        seen.add(key)
        try:
            rows = await _fetch_klines(ex, cand, tf, limit)
        except Exception:
            continue
        if len(rows) >= 300:
            return rows
    return None


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


# ====================== КОНТЕКСТ РЫНКА ======================
_ctx_cache = {"ts": 0, "data": {}}
_funding_cache = {}


async def get_funding(coin):
    c = _funding_cache.get(coin)
    if c and time.time() - c[0] < 600:
        return c[1]
    val = None
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        try:
            j = await http_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": f"{cand}USDT"}, timeout=5)
            val = float(j["result"]["list"][0]["fundingRate"])
            break
        except Exception:
            continue
    _funding_cache[coin] = (time.time(), val)
    return val


async def get_context(force=False):
    if not force and time.time() - _ctx_cache["ts"] < 600 and _ctx_cache["data"]:
        return _ctx_cache["data"]
    ctx = {"btc_dir": 0, "btc_conf": 50.0, "btc_txt": "нет данных", "fng": None, "fng_txt": ""}
    try:
        tf, _ = await get_market_data_light("BTC", ("4h", "1d"))
        p = (tf["4h"]["long_prob"] + tf["1d"]["long_prob"]) / 2
        ctx["btc_dir"] = 1 if p >= 55 else -1 if p <= 45 else 0
        ctx["btc_conf"] = max(p, 100 - p)
        ctx["btc_txt"] = "бычий ▲" if ctx["btc_dir"] == 1 else "медвежий ▼" if ctx["btc_dir"] == -1 else "флэт ▬"
    except Exception as e:
        log.info(f"context BTC: {e}")
    try:
        j = await http_json("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
        ctx["fng"] = int(j["data"][0]["value"])
        ctx["fng_txt"] = str(j["data"][0].get("value_classification", ""))
    except Exception:
        pass
    _ctx_cache.update(ts=time.time(), data=ctx)
    return ctx


# =====================================================================
#                 СЛОЙ 2-4: МУЛЬТИ-ТАЙМФРЕЙМ И КОНТЕКСТ
# =====================================================================
TF_WEIGHTS = {"1d": 0.30, "4h": 0.30, "1h": 0.25, "15m": 0.15}


def quick_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data:
        return None
    p4, p1 = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]
    if (p4 >= 50) != (p1 >= 50):
        return None
    wp = (p4 + p1) / 2
    strength = max(wp, 100 - wp)
    if strength < CONF_STAGE1 or tf_data["4h"]["adx"] < 15 or tf_data["4h"]["qv24"] < MIN_QV24:
        return None
    return wp >= 50, strength


def collect_reasons(tf_data, is_long, tfs=("1d", "4h", "1h"), limit=5):
    side = 1 if is_long else -1
    bucket = {}
    for tf in tfs:
        if tf not in tf_data:
            continue
        for rsn in tf_data[tf]["reasons"]:
            if rsn["side"] == side:
                b = bucket.setdefault(rsn["text"], {"w": 0, "tfs": []})
                b["w"] += rsn["w"]
                b["tfs"].append(tf)
    items = sorted(bucket.items(), key=lambda kv: kv[1]["w"], reverse=True)[:limit]
    return [f"{t} ({', '.join(b['tfs'])})" for t, b in items]


def pattern_lines(tf_data):
    lines = []
    for tf in ("1d", "4h", "1h"):
        if tf in tf_data and tf_data[tf]["patterns"]:
            names = ", ".join(p[0] for p in tf_data[tf]["patterns"][:3])
            lines.append(f"{tf}: {names}")
    return lines


def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False):
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрок",
           "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": []}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data:
        out["reject"] = "Недостаточно данных по таймфреймам"
        return out
    tw = sum(TF_WEIGHTS[t] for t in tfs)
    wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / tw
    is_long = wp >= 50
    side = 1 if is_long else -1
    strength = max(wp, 100 - wp)
    notes = []

    def cs(p):
        return p if is_long else 100 - p

    p4, p1d = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]
    reject = ""
    if (p4 >= 50) != is_long or (p1d >= 50) != is_long:
        reject = "4h и 1d смотрят в разные стороны"
    info = tf_data["4h"]
    if not reject and info["adx"] < 15:
        reject = f"Вялый рынок (ADX {info['adx']:.0f})"
    if scan and info["qv24"] < MIN_QV24:
        reject = reject or "Низкая ликвидность"

    if "1h" in tf_data and (tf_data["1h"]["long_prob"] >= 50) != is_long and tf_data["1h"]["confidence"] >= 65:
        strength -= 8
        notes.append("1h против направления (−8)")
    if is_long and (info["rsi"] > 75 or info["price"] - info["ema20"] > 2.5 * info["atr"]):
        strength -= 8
        notes.append("цена перегрета, риск погони (−8)")
    if (not is_long) and (info["rsi"] < 25 or info["ema20"] - info["price"] > 2.5 * info["atr"]):
        strength -= 8
        notes.append("цена перепродана, риск погони (−8)")

    if coin != "BTC" and ctx.get("btc_dir", 0) != 0:
        if ctx["btc_dir"] == side:
            strength += 3
            notes.append("BTC в ту же сторону (+3)")
        else:
            pen = 10 if ctx.get("btc_conf", 50) >= 65 else 5
            strength -= pen
            notes.append(f"BTC против направления (−{pen})")
    fng = ctx.get("fng")
    if fng is not None:
        if fng >= 80 and is_long:
            strength -= 4
            notes.append(f"экстремальная жадность {fng} (−4)")
        elif fng <= 20 and not is_long:
            strength -= 4
            notes.append(f"экстремальный страх {fng} (−4)")
    if funding is not None:
        if is_long and funding > 0.0005:
            strength -= 5
            notes.append(f"перегрет лонг: funding {funding * 100:.3f}% (−5)")
        elif (not is_long) and funding < -0.0005:
            strength -= 5
            notes.append(f"перегрет шорт: funding {funding * 100:.3f}% (−5)")

    nb = market_bias.get("bias", 0)
    age = time.time() - market_bias.get("ts", 0)
    news_status = "Нейтрально"
    if age < 3 * 3600 and nb != 0:
        word = "бычий" if nb == 1 else "медвежий"
        if nb == side:
            strength += 7
            news_status = f"✅ Подтверждают ({word} фон)"
        else:
            strength -= 20
            news_status = f"❌ Противоречат ({word} фон)"

    strength = max(0.0, min(100.0, strength))
    if not reject and strength < CONF_FINAL:
        reject = f"Уверенность {strength:.0f}% ниже порога {CONF_FINAL:.0f}%"

    p15, p1h = tf_data.get("15m", {}).get("long_prob", 50), tf_data.get("1h", {}).get("long_prob", 50)
    fast = "15m" in tf_data and "1h" in tf_data and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    sig_type = "Быстрый сигнал" if fast else "Долгосрок" if long_term else "Среднесрок"

    out.update(valid=not reject, reject=reject, notes=notes, is_long=is_long, strength=round(strength, 1),
               type=sig_type, news_status=news_status, wp=wp,
               tf_probs={t: tf_data[t]["long_prob"] for t in tfs},
               reasons=collect_reasons(tf_data, is_long), patterns=pattern_lines(tf_data))
    return out


# =====================================================================
#                 УРОВНИ, РИСК, БЭКТЕСТ (СЛОЙ 5)
# =====================================================================
def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    if is_long:
        entry = min(price, max(ema20, ema50))
        if entry > price - 0.2 * atr:
            entry = price - 0.35 * atr
        if support and price - 1.5 * atr <= support <= price - 0.15 * atr:
            entry = max(entry, support + 0.1 * atr)
            entry = min(entry, price - 0.15 * atr)
        sl1 = min(rec_low - 0.5 * atr, entry - 0.8 * atr)
        sl1 = max(sl1, entry - 2.5 * atr)
        return entry, sl1, entry - sl1
    
    entry = max(price, min(ema20, ema50))
    if entry < price + 0.2 * atr:
        entry = price + 0.35 * atr
    if resistance and price + 0.15 * atr <= resistance <= price + 1.5 * atr:
        entry = min(entry, resistance - 0.1 * atr)
        entry = max(entry, price + 0.15 * atr)
    sl1 = max(rec_high + 0.5 * atr, entry + 0.8 * atr)
    sl1 = min(sl1, entry + 2.5 * atr)
    return entry, sl1, sl1 - entry


def calc_levels_and_risk(info, is_long, sig_type="Среднесрок"):
    price = info["price"]
    atr = info["atr"] if info["atr"] > 0 else price * 0.012
    sr = info.get("sr", {})
    entry, sl1, risk = levels_core(price, atr, info["ema20"], info["ema50"], info["recent_low"], info["recent_high"],
                                   is_long, sr.get("support"), sr.get("resistance"))
    sign = 1 if is_long else -1
    tps = [entry + sign * risk * m for m in (1.5, 2.5, 4.0)]
    sl2 = sl1 - sign * 0.7 * atr
    risk_amount = DEPOSIT * RISK_PCT
    pos_usdt = (risk_amount / risk) * entry if risk > 0 else 0.0
    
    if sig_type == "Быстрый сигнал":
        leverage = "x15-x25"
    elif sig_type == "Долгосрок":
        leverage = "x5-x10"
    else:
        leverage = "x10-x20"

    near = None
    opp = sr.get("resistance") if is_long else sr.get("support")
    if opp and abs(opp - entry) < 1.2 * risk:
        near = "ближайший уровень слишком близко к входу: цель может не отработать"
    return entry, tps, [sl1, sl2], pos_usdt, risk_amount, leverage, near


def backtest_df(df, conf_min=BT_CONF, rr1=1.5, fill_bars=6, max_hold=30, fee=0.001):
    n = len(df)
    if n < 150:
        return None
    lp = df["long_p"].values
    adx, atr = df["adx"].values, df["atr"].values
    c, h, l = df["close"].values, df["high"].values, df["low"].values
    e20, e50 = df["ema20"].values, df["ema50"].values
    low28 = df["low"].rolling(28).min().values
    high28 = df["high"].rolling(28).max().values
    results = []
    i = 80
    while i < n - 2:
        p = lp[i]
        conf = max(p, 100 - p)
        if conf < conf_min or adx[i] < 15 or atr[i] <= 0 or np.isnan(low28[i]):
            i += 1
            continue
        is_long = p >= 50
        entry, sl, risk = levels_core(c[i], atr[i], e20[i], e50[i], low28[i], high28[i], is_long)
        if risk <= 0:
            i += 1
            continue
        tp = entry + rr1 * risk if is_long else entry - rr1 * risk
        filled = None
        for j in range(i + 1, min(n, i + 1 + fill_bars)):
            if (is_long and l[j] <= entry) or ((not is_long) and h[j] >= entry):
                filled = j
                break
        if filled is None:
            i += 1
            continue
        res, last_k = None, filled
        for k in range(filled, min(n, filled + max_hold)):
            last_k = k
            hit_sl = (l[k] <= sl) if is_long else (h[k] >= sl)
            hit_tp = (h[k] >= tp) if is_long else (l[k] <= tp)
            if hit_sl:
                res = -1.0
                break
            if hit_tp:
                res = rr1
                break
        if res is None:
            last = c[last_k]
            res = ((last - entry) if is_long else (entry - last)) / risk
            res = max(-1.0, min(rr1, res))
        res -= fee * entry / risk
        results.append(res)
        i = last_k + 1
    if not results:
        return {"trades": 0, "winrate": 0.0, "avg_r": 0.0, "pf": 0.0, "days": round(n * 4 / 24)}
    arr = np.array(results)
    wins, losses = arr[arr > 0], arr[arr <= 0]
    pf = float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else 9.9
    return {"trades": len(arr), "winrate": round(float((arr > 0).mean() * 100), 1), "avg_r": round(float(arr.mean()), 2),
            "pf": round(min(pf, 9.9), 2), "days": round(n * 4 / 24)}


def run_backtest_rows(rows):
    df = prepare_df(rows_to_df(rows[:-1]))
    return backtest_df(df)


async def get_backtest(coin):
    c = bt_cache.get(coin)
    if c and time.time() - c[0] < 6 * 3600:
        return c[1]
    bt = None
    try:
        rows = await fetch_history(coin, "4h", 1000)
        if rows:
            bt = await asyncio.to_thread(run_backtest_rows, rows)
    except Exception as e:
        log.info(f"backtest {coin}: {e}")
    bt_cache[coin] = (time.time(), bt)
    return bt


def bt_ok(bt):
    return bool(bt) and bt["trades"] >= BT_MIN_TRADES and bt["avg_r"] >= BT_MIN_AVG_R and bt["winrate"] >= BT_MIN_WINRATE


# ====================== ИИ ======================
def _looks_like_junk(text):
    t = (text or "").strip()
    low = t.lower()
    if len(t) < 30:
        return True
    if low.startswith("<!doctype") or low.startswith("<html"):
        return True
    if "pollinations" in low or "deprecat" in low or "rate limit" in low:
        return True
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
                log.warning(f"Gemini {r.status}: {str(j)[:120]}")
        except Exception as e:
            log.warning(f"Gemini: {e}")
    return None


async def _ask_openai_compat(name, base, key, model, prompt, extra_headers=None):
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1100}
    try:
        async with http.post(f"{base}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json(content_type=None)
            if r.status == 200:
                return j["choices"][0]["message"]["content"]
            log.warning(f"{name} {r.status}: {str(j)[:120]}")
    except Exception as e:
        log.warning(f"{name}: {e}")
    return None


async def _ask_free_gpt(prompt):
    system = "Ты опытный крипто-аналитик. Отвечай на русском языке, подробно и по делу."
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
    if GEMINI_API_KEYS:
        providers.append(("Gemini", lambda: _ask_gemini(prompt)))
    if GROQ_API_KEY:
        providers.append(("Groq", lambda: _ask_openai_compat("Groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt)))
    if OPENROUTER_API_KEY:
        providers.append(("OpenRouter", lambda: _ask_openai_compat("OpenRouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt)))

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
        if res:
            return res
    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt))], timeout=free_timeout)
    return res if res else (None, None)


async def run_ai_diag():
    prompt = "Напиши одно предложение о биткоине."
    lines = ["🔧 <b>Проверка ИИ</b>", ""]

    async def timed(name, fn):
        t0 = time.time()
        try:
            r = await asyncio.wait_for(fn(), 28)
        except Exception:
            r = None
        return f"{'✅' if r else '❌'} {name} · {int((time.time() - t0) * 1000)} мс"

    tests = []
    if GEMINI_API_KEYS:
        tests.append(timed("Gemini", lambda: _ask_gemini(prompt)))
    else:
        lines.append("➖ Gemini: нет ключа")
    if GROQ_API_KEY:
        tests.append(timed("Groq", lambda: _ask_openai_compat("Groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt)))
    else:
        lines.append("➖ Groq: нет ключа")
    if OPENROUTER_API_KEY:
        tests.append(timed("OpenRouter", lambda: _ask_openai_compat("OpenRouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt)))
    else:
        lines.append("➖ OpenRouter: нет ключа")
    tests.append(timed("Бесплатный GPT", lambda: _ask_free_gpt(prompt)))
    lines += await asyncio.gather(*tests)
    return "\n".join(lines)


_ai_cache = {}


def _parse_analyst(text):
    try:
        m = re.search(r"\{.*\}", text or "", re.S)
        d = json.loads(m.group(0))
        verdict = str(d.get("verdict", "")).upper().strip()
        comment = str(d.get("comment", "")).strip()
        if verdict in ("ENTER", "WAIT") and has_cyr(comment, 15):
            return {"verdict": verdict, "risk": str(d.get("risk", "средний")).strip()[:20], "comment": shorten(comment, 320)}
    except Exception:
        pass
    return None


async def ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data):
    key = (coin, setup["is_long"])
    c = _ai_cache.get(key)
    if c and time.time() - c[0] < 1200:
        return c[1]
    side = "LONG" if setup["is_long"] else "SHORT"
    tfp = ", ".join(f"{t} {setup['tf_probs'][t]:.0f}%" for t in ("1d", "4h", "1h", "15m") if t in setup["tf_probs"])
    bt_txt = (f"{bt['trades']} сделок, TP1 {bt['winrate']}%, ср. R {bt['avg_r']:+.2f}, PF {bt['pf']}" if bt else "нет данных")
    prompt = f"""Ты опытный крипто-трейдер и риск-менеджер. Оцени торговую идею и дай вердикт.
Монета: {coin}. Направление: {side}. Тип: {setup['type']}. Уверенность модели: {setup['strength']}%.
Шанс лонга по таймфреймам: {tfp}. Режим 4h: {tf_data['4h']['regime']} (ADX {tf_data['4h']['adx']:.0f}).
Причины: {'; '.join(setup['reasons']) or 'нет'}.
Свечные паттерны: {'; '.join(setup['patterns']) or 'нет'}.
Бэктест 4h на истории монеты: {bt_txt}.
Новостной фон: {setup['news_status']}; последняя новость: {market_bias.get('title_ru', '')[:100]}.
Контекст: BTC {ctx.get('btc_txt')}, Fear&Greed {ctx.get('fng')} {ctx.get('fng_txt')}.
Поправки модели: {'; '.join(setup['notes']) or 'нет'}.
Вход {fmt(entry)}, TP1 {fmt(tps[0])}, SL {fmt(sls[0])}.
Дай ENTER только если идея выглядит качественной; при сомнениях WAIT.
Верни СТРОГО JSON без markdown: {{"verdict":"ENTER или WAIT","risk":"низкий или средний или высокий","comment":"1-2 предложения по-русски: главный довод и главный риск"}}"""
    text, engine = await ask_ai_named(prompt, validate=lambda t: _parse_analyst(t) is not None, timeout=22, free_timeout=25)
    res = _parse_analyst(text) if text else None
    if res:
        res["engine"] = engine
    _ai_cache[key] = (time.time(), res)
    return res


# ====================== НОВОСТИ ======================
BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner", "upgrad", "breakout", "rebound", "recover", "accumul")
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall", "liquidat", "fraud", "scam", "delist", "arrest", "dump", "selloff", "declin", "probe", "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
COIN_WORDS = {"bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL", "xrp": "XRP", "bnb": "BNB", "doge": "DOGE", "ada": "ADA", "ton": "TON", "avax": "AVAX", "link": "LINK", "zcash": "ZEC", "zec": "ZEC"}


def news_sentiment(text, threshold=2):
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w.startswith(BULL_STEMS):
            score += 1
        elif w.startswith(BEAR_STEMS):
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
    return found[:4]


async def translate_text(text, target="ru"):
    text = (text or "").strip()
    if not text:
        return text
    try:
        async with http.get("https://translate.googleapis.com/translate_a/single", params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text[:1800]}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                out = "".join(part[0] for part in data[0] if part and part[0])
                if has_cyr(out, 3):
                    return out
    except Exception:
        pass
    return text


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
    if getattr(entry, "media_thumbnail", None):
        return entry.media_thumbnail[0].get("url")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', getattr(entry, "summary", "") or "")
    return m.group(1) if m else None


def clean_summary(raw):
    t = html.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    t = re.sub(r"The post .*? appeared first on .*", "", t, flags=re.S)
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
    return has_cyr(p.get("TITLE_RU", ""), 6) and has_cyr(p.get("SUMMARY", ""), 20) and has_cyr(p.get("IMPACT", ""), 10)


IMPACT_TEMPLATES = {
    1: "Новостной фон оценивается как позитивный (бычий). Наблюдается приток интереса к рисковым активам, что может поддержать спрос на криптовалюту и привести к росту цен на ключевые активы (BTC, ETH). Рекомендуется следить за точками входа в лонг.",
    -1: "Новостной фон оценивается как негативный (медвежий). Возможны локальные распродажи, рост волатильности и уход инвесторов в защищённые активы. Стоит проявлять осторожность и строго соблюдать риск-менеджмент.",
    0: "Новостной фон нейтрален. Рынок находится в стадии консолидации, участники ожидают новых триггеров для формирования тренда. В таких условиях сигналы могут требовать дополнительного подтверждения.",
}


async def update_market_bias(force=False):
    global market_bias
    async with news_lock:
        try:
            entries = await get_news_entries()
            if not entries:
                return
            entry = entries[0]
            link = getattr(entry, "link", "")
            now = time.time()
            if not force and market_bias.get("link") == link:
                market_bias["ts"] = now
                return

            title = html.unescape(getattr(entry, "title", "") or "")
            summary = clean_summary(getattr(entry, "summary", ""))
            published = get_moscow_time_from_entry(entry)

            prompt = f"""Новость: {title}
Текст: {summary}
Переведи на русский и разбери строго в формате (каждое поле с новой строки):
BIAS: BULLISH или BEARISH или NEUTRAL
TITLE_RU: точный перевод заголовка
SUMMARY: 2-3 предложения: что произошло, кто участвует, какие цифры
IMPACT: 3-4 предложения: подробный анализ влияния новости на рынок, оценка фона (бычий или медвежий), на какие активы и почему повлияет в первую очередь
COINS: тикеры через запятую"""
            text, engine = await ask_ai_named(prompt, validate=_valid_news_ai)
            parsed = parse_news_ai(text) if text else {}

            if text and _valid_news_ai(text):
                up = parsed.get("BIAS", "").upper()
                bias = 1 if "BULL" in up else -1 if "BEAR" in up else 0
                title_ru = parsed["TITLE_RU"][:170]
                summ = shorten(parsed["SUMMARY"], 380)
                impact = shorten(parsed["IMPACT"], 500)
                coins = parsed.get("COINS", "BTC, ETH")
                eng = engine
            else:
                title_ru = await translate_text(title)
                summ = shorten(await translate_text(summary) if summary else title_ru, 380)
                bias = news_sentiment(f"{title}. {summary}")
                impact = IMPACT_TEMPLATES[bias]
                coins = ", ".join(mentioned_coins(f"{title}. {summary}") or ["BTC", "ETH"])
                eng = "fallback"

            market_bias = {"bias": bias, "title_ru": title_ru, "summary": summ, "impact": impact, "coins": coins,
                           "published": published, "link": link, "engine": eng, "ts": now}
        except Exception as e:
            log.warning(f"update_market_bias: {e}")


def format_news_post(nb):
    emoji = {1: "🟢", 0: "⚪", -1: "🔴"}.get(nb.get("bias", 0), "⚪")
    label = {1: "Позитивно", 0: "Нейтрально", -1: "Негативно"}.get(nb.get("bias", 0), "Нейтрально")
    text = (f"📰 <b>{html.escape(nb.get('title_ru', ''))}</b>\n🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n"
            f"📌 <b>Суть</b>\n{html.escape(nb.get('summary', ''))}\n\n"
            f"📊 <b>Влияние</b>\n{emoji} <b>{label}.</b> {html.escape(nb.get('impact', ''))}\n\n"
            f"🪙 <b>Монеты:</b> {html.escape(nb.get('coins', ''))}")
    if nb.get("link"):
        text += f"\n\n🔗 <a href=\"{html.escape(nb['link'], quote=True)}\">Источник</a>"
    return text


async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries or market_bias.get("engine") == "none":
        return None, None
    return format_news_post(market_bias), extract_image(entries[0])


async def news_watcher_loop():
    global last_news_link
    await asyncio.sleep(40)
    while True:
        try:
            entries = await get_news_entries()
            if entries:
                link = entries[0].link
                if last_news_link is None:
                    last_news_link = link
                elif link != last_news_link:
                    last_news_link = link
                    if bot_flags["news_enabled"] and active_chats:
                        text, img_url = await prepare_news(force=True)
                        if text:
                            img = await download_bytes(img_url) if img_url else None
                            payload = {"kind": "photo" if img else "text", "text": text, "data": img, "markup": MENU_KB}
                            sent, _, _, _ = await broadcast(payload)
                            stats["news_sent"] += sent
        except Exception as e:
            log.warning(f"news_watcher: {e}")
        next_timers["news_watcher"] = time.time() + 600
        await asyncio.sleep(600)


# ====================== ГРАФИК ======================
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    info = tf_data[main_tf]
    df = info["df"].tail(60).reset_index(drop=True)
    price = info["price"]
    w, h = 900, 540
    img = Image.new("RGB", (w, h), (14, 15, 20))
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
        font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
    except Exception:
        font = font_b = ImageFont.load_default()
    side = "LONG" if is_long else "SHORT"
    col = (0, 200, 90) if is_long else (230, 60, 60)
    draw.text((18, 10), f"{coin}/USDT  {main_tf}  {side}", fill=col, font=font_b)
    pad_l, pad_r, pad_t = 62, 18, 42
    pr_bottom, vol_top, vol_bottom = 405, 425, 520
    cw = w - pad_l - pad_r
    ch = pr_bottom - pad_t
    highs, lows = df["high"].values, df["low"].values
    levels = [entry, price] + list(tps) + list(sls)
    mn = min(lows.min(), min(levels)) * 0.998
    mx = max(highs.max(), max(levels)) * 1.002
    rng = mx - mn or 1.0

    def y(p):
        return pad_t + ch - (p - mn) / rng * ch

    n = len(df)
    step = cw / n
    cwid = max(3, int(step * 0.7))
    for i in range(n):
        x = pad_l + i * step + step / 2
        o, hh, ll, c = df["open"].iloc[i], highs[i], lows[i], df["close"].iloc[i]
        color = (0, 170, 80) if c >= o else (205, 50, 50)
        draw.line([(x, y(hh)), (x, y(ll))], fill=color, width=1)
        top, bot = min(y(o), y(c)), max(y(o), y(c))
        draw.rectangle([x - cwid / 2, top, x + cwid / 2, max(bot, top + 1)], fill=color)
    for col_name, colr in (("ema20", (255, 205, 60)), ("ema50", (80, 150, 255))):
        pts = [(pad_l + i * step + step / 2, y(v)) for i, v in enumerate(df[col_name].values)]
        draw.line(pts, fill=colr, width=2)
    vmax = df["volume"].max() or 1.0
    for i in range(n):
        x = pad_l + i * step + step / 2
        vh = (vol_bottom - vol_top) * (df["volume"].iloc[i] / vmax)
        color = (0, 120, 60) if df["close"].iloc[i] >= df["open"].iloc[i] else (140, 40, 40)
        draw.rectangle([x - cwid / 2, vol_bottom - vh, x + cwid / 2, vol_bottom], fill=color)
    sr = info.get("sr", {})
    for lvl_p, lbl in ((sr.get("support"), "SUP"), (sr.get("resistance"), "RES")):
        if lvl_p:
            for xx in range(pad_l, w - pad_r, 10):
                draw.line([(xx, y(lvl_p)), (xx + 4, y(lvl_p))], fill=(130, 130, 150), width=1)
            draw.text((w - pad_r - 80, y(lvl_p) - 14), f"{lbl} {fmt(lvl_p)}", fill=(150, 150, 170), font=font)

    def lvl(p, label, color, dash=False):
        yy = y(p)
        if dash:
            for xx in range(pad_l, w - pad_r, 7):
                draw.line([(xx, yy), (xx + 3, yy)], fill=color, width=1)
        else:
            draw.line([(pad_l, yy), (w - pad_r, yy)], fill=color, width=2)
        draw.text((pad_l + 4, yy - 14), f"{label} {fmt(p)}", fill=color, font=font)

    lvl(price, "Price", (230, 230, 230), True)
    lvl(entry, "Entry", (255, 190, 0))
    for i, tp in enumerate(tps):
        lvl(tp, f"TP{i + 1}", (0, 190, 90), True)
    for i, sl in enumerate(sls):
        lvl(sl, f"SL{i + 1}", (230, 60, 60), True)
    if info["patterns"]:
        draw.text((pad_l + cw - 190, 12), "Pattern: " + info["patterns"][0][2], fill=(255, 210, 120), font=font_b)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# =====================================================================
#                  СБОРКА АНАЛИЗА
# =====================================================================
def local_verdict(setup, bt):
    if not setup["valid"]:
        return "WAIT"
    if bt is None:
        return "ENTER" if setup["strength"] >= CONF_FINAL + 4 else "WAIT"
    return "ENTER" if (bt_ok(bt) or bt["trades"] < BT_MIN_TRADES) and setup["strength"] >= CONF_FINAL else "WAIT"


async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    info = tf_data["4h"]
    is_long = setup["is_long"]
    entry, tps, sls, pos_usdt, risk_usdt, leverage, near = calc_levels_and_risk(info, is_long, setup["type"])

    verdict, comment, risk_lvl, engine = "WAIT", "", "средний", ""
    if not setup["valid"]:
        comment = setup["reject"]
    else:
        ai = await ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data)
        if ai:
            verdict, comment, risk_lvl, engine = ai["verdict"], ai["comment"], ai["risk"], ai.get("engine", "")
        else:
            verdict = local_verdict(setup, bt)
            comment = "ИИ-аналитик недоступен: решение принято по правилам модели и истории."
        
        if bt and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt):
            verdict = "WAIT"
            comment = "История этой монеты по такой стратегии слабая, вход не рекомендую. " + comment
        if verdict == "ENTER" and near:
            comment = (comment + " Но " + near + ".").strip()

    side_emoji = "🟢" if is_long else "🔴"
    side = "LONG" if is_long else "SHORT"
    head = "✅ <b>ВХОДИТЬ</b>" if verdict == "ENTER" else "⏳ <b>ЖДАТЬ</b>"
    bar = make_strength_bar(setup["strength"])

    def pct(p):
        return f"{(p - entry) / entry * 100:+.1f}%"

    lines = [
        f"{side_emoji} <b>{side}  ·  {html.escape(coin)}</b>   {head}",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📌 Тип: <b>{setup['type']}</b>   ·   Режим 4h: {info['regime']}",
        f"💪 Уверенность: <b>{setup['strength']}%</b>",
        bar,
        "",
        f"📰 Новости: {setup['news_status']}",
        f"🌍 BTC: {ctx.get('btc_txt')}" + (f"  ·  Fear&Greed: {ctx['fng']} ({html.escape(ctx.get('fng_txt', ''))})" if ctx.get("fng") is not None else ""),
    ]
    if setup["reasons"]:
        lines += ["", "📈 <b>Почему:</b>"] + [f"• {html.escape(r)}" for r in setup["reasons"]]
    if setup["patterns"]:
        lines += ["", "🕯 <b>Свечи:</b> " + html.escape("; ".join(setup["patterns"]))]
    if info["squeeze"]:
        lines.append("🎯 Сжатие волатильности: возможен резкий импульс")
    lines += [
        "",
        f"💰 Текущая цена: <code>{fmt(info['price'])}</code>",
        f"📥 <b>ВХОД (ТОЛЬКО ЛИМИТНЫЙ ОРДЕР!):</b> <code>{fmt(entry)}</code>",
        "⚠️ <i>Не входите по рынку (Market). Ждите, пока цена откатится до указанного уровня!</i>",
        "",
        "🎯 <b>Тейки (TP)</b>",
    ] + [f"   TP{i + 1}  <code>{fmt(p)}</code>  <i>{pct(p)}</i>" for i, p in enumerate(tps)] + [
        "",
        "🛡 <b>Стопы (SL)</b>",
    ] + [f"   SL{i + 1}  <code>{fmt(p)}</code>  <i>{pct(p)}</i>" for i, p in enumerate(sls)] + [
        "",
        f"⚖ <b>Risk Gate:</b> плечо <b>{leverage}</b>, позиция <b>${pos_usdt:.2f}</b>, риск <b>${risk_usdt:.2f}</b> ({RISK_PCT * 100:.0f}% от ${DEPOSIT:.0f})",
    ]
    if bt:
        lines.append(f"🧪 <b>Бэктест 4h ({bt['days']} дн.):</b> сделок {bt['trades']}, TP1 {bt['winrate']}%, ср. {bt['avg_r']:+.2f}R, PF {bt['pf']}")
    else:
        lines.append("🧪 Бэктест: данных недостаточно")
    if setup["notes"]:
        lines.append("⚙ Поправки: " + html.escape("; ".join(setup["notes"])))
    if comment:
        lines += ["", f"💬 <b>Аналитик ({html.escape(risk_lvl)} риск):</b> {html.escape(comment)}"]
    lines += ["", "⚠ <i>Не является финансовой рекомендацией. Бэктест не гарантирует будущий результат.</i>"]
    text = "\n".join(lines)

    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception as e:
        log.info(f"chart {coin}: {e}")
        img = None
    return {"text": text, "img": img, "coin": coin, "entry": entry, "tps": tps, "sls": sls, "is_long": is_long,
            "side": side, "price": info["price"], "type": setup["type"], "strength": setup["strength"],
            "verdict": verdict, "source": source}


async def analyze_coin(coin, user_id=None, force=False):
    cached = analysis_cache.get(coin)
    if cached and not force and time.time() - cached[0] < cached[1]:
        return cached[2], None
    try:
        tf_data, source = await get_market_data(coin)
    except Exception:
        return None, f"⚠ Не удалось получить данные по <b>{html.escape(coin)}</b>"
    try:
        ctx = await get_context()
        funding = await get_funding(coin)
        setup = evaluate_setup(coin, tf_data, ctx, funding)
        bt = await get_backtest(coin)
        res = await asyncio.wait_for(build_analysis(coin, tf_data, setup, bt, ctx, source), timeout=75)
    except Exception as e:
        log.error(f"analyze {coin}: {e}")
        return None, f"⚠ Ошибка анализа <b>{html.escape(coin)}</b>, попробуйте ещё раз"
    analysis_cache[coin] = (time.time(), 180, res)
    if len(analysis_cache) > 60:
        for k in sorted(analysis_cache, key=lambda x: analysis_cache[x][0])[:20]:
            analysis_cache.pop(k, None)
    return res, None


async def send_analysis(chat_id, res, uid):
    kb = result_keyboard(res["coin"])
    text = res["text"]
    if res["img"]:
        photo = BufferedInputFile(res["img"], f"{res['coin']}.png")
        if len(text) <= 1024:
            msg = await bot.send_photo(chat_id, photo, caption=text, parse_mode="HTML", reply_markup=kb)
            track_msg(uid, msg)
        else:
            m1 = await bot.send_photo(chat_id, photo)
            track_msg(uid, m1)
            msg = await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
            track_msg(uid, msg)
    else:
        msg = await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        track_msg(uid, msg)


# ====================== СКАНЕР ======================
SCAN_INTERVAL = 25 * 60
SIGNAL_COOLDOWN = 3 * 3600
signal_sent_at = {}
scan_skip = {}


async def scan_once(send=True):
    global last_signals_data, current_signals_db, last_scan_info
    t0 = time.time()
    if time.time() - market_bias["ts"] > 900:
        await update_market_bias()
    ctx = await get_context(force=True)
    bias_text = market_bias.get("impact", "Нейтральный фон")
    sem = asyncio.Semaphore(3)

    stage1 = []

    async def s1(coin):
        if time.time() < scan_skip.get(coin, 0):
            return
        async with sem:
            try:
                tf2, _ = await get_market_data_light(coin, ("4h", "1d"))
            except LookupError:
                scan_skip[coin] = time.time() + 6 * 3600
                return
            except Exception:
                return
            q = quick_check(tf2)
            if q:
                stage1.append((coin, tf2, q[1]))

    await asyncio.gather(*[s1(c) for c in POPULAR_COINS])
    stage1.sort(key=lambda x: x[2], reverse=True)
    stage1 = stage1[:18]

    stage2 = []

    async def s2(item):
        coin, tf2, _ = item
        async with sem:
            tf_data = dict(tf2)
            try:
                extra, _src = await get_market_data_light(coin, ("1h", "15m"))
                tf_data.update(extra)
            except Exception:
                pass
            funding = await get_funding(coin)
            setup = evaluate_setup(coin, tf_data, ctx, funding, scan=True)
            if setup["valid"]:
                stage2.append((coin, tf_data, setup))

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
            if bt is not None and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt):
                return
            bt_pass += 1
            try:
                res = await build_analysis(coin, tf_data, setup, bt, ctx, "scan")
            except Exception as e:
                log.info(f"scan build {coin}: {e}")
                return
            if res["verdict"] == "ENTER":
                final.append(res)
                analysis_cache[coin] = (time.time(), 1200, res)

    await asyncio.gather(*[s3(i) for i in stage2])
    final.sort(key=lambda r: r["strength"], reverse=True)

    new_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
    for r in final:
        new_db[r["type"]].append({"coin": r["coin"], "is_long": r["is_long"], "strength": r["strength"], "type": r["type"],
                                  "side": r["side"], "text": r["text"]})
    for k in new_db:
        new_db[k] = new_db[k][:10]
    
    current_signals_db = new_db
    _save_current_signals_db(new_db)

    last_scan_info = {"ts": int(time.time()), "stage1": len(stage1), "stage2": len(stage2),
                      "backtest_ok": bt_pass, "final": len(final), "secs": int(time.time() - t0)}
    log.info(f"Сканер: этап1 {len(stage1)} → этап2 {len(stage2)} → бэктест {bt_pass} → итог {len(final)} за {last_scan_info['secs']} с")

    if send and active_chats:
        now = time.time()
        hist = _load_signal_history()
        
        for r in final:
            key = f"{r['coin']}:{'L' if r['is_long'] else 'S'}"
            if now - signal_sent_at.get(key, 0) >= SIGNAL_COOLDOWN:
                signal_sent_at[key] = now
                hist.append({
                    "ts": now, 
                    "coin": r["coin"], 
                    "side": r["side"], 
                    "type": r["type"], 
                    "strength": r["strength"], 
                    "text": r["text"],
                    "entry": r["entry"],
                    "tp1": r["tps"][0],
                    "sl1": r["sls"][0],
                    "status": "active"
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


async def scanner_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner: {e}")
        next_timers["scanner"] = time.time() + SCAN_INTERVAL
        await asyncio.sleep(SCAN_INTERVAL)


async def global_signal_monitor_loop():
    await asyncio.sleep(120)
    while True:
        try:
            hist = _load_signal_history()
            updated = False
            for x in hist:
                if x.get("status") == "active":
                    price = await get_simple_price(x["coin"])
                    if price:
                        is_long = x["side"] == "LONG"
                        tp1 = x.get("tp1", 0)
                        sl1 = x.get("sl1", 0)
                        if is_long:
                            if price >= tp1:
                                x["status"] = "win"
                                updated = True
                            elif price <= sl1:
                                x["status"] = "loss"
                                updated = True
                        else:
                            if price <= tp1:
                                x["status"] = "win"
                                updated = True
                            elif price >= sl1:
                                x["status"] = "loss"
                                updated = True
                    await asyncio.sleep(0.2)
            if updated:
                _save_signal_history(hist)
        except Exception as e:
            log.error(f"global_signal_monitor: {e}")
        next_timers["global_signals"] = time.time() + 600
        await asyncio.sleep(600)


# ====================== BROADCAST ======================
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
                    if len(text) <= 1024:
                        msg = await bot.send_photo(cid, photo, caption=text, parse_mode="HTML", reply_markup=payload.get("markup"))
                        track_msg(cid, msg)
                    else:
                        msg_photo = await bot.send_photo(cid, photo)
                        track_msg(cid, msg_photo)
                        msg = await bot.send_message(cid, text[:4000], parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
                        track_msg(cid, msg)
                else:
                    msg = await bot.send_message(cid, payload.get("text", "")[:4000], parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
                    track_msg(cid, msg)
                sent += 1
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except (TelegramForbiddenError, TelegramNotFound):
                dead.append(cid)
                break
            except Exception:
                failed += 1
                break
        await asyncio.sleep(0.04)
    if dead:
        for cid in dead:
            active_chats.discard(cid)
        await asyncio.to_thread(mark_users_inactive, dead)
    stats["broadcasts"] += 1
    return sent, failed, len(dead), []


# ====================== МОНИТОРИНГ ПОЗИЦИЙ ======================
async def position_monitor_loop():
    await asyncio.sleep(50)
    while True:
        try:
            users = _load_users()
            for uid_str, u in users.items():
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    try:
                        await check_position(int(uid_str), coin, pos)
                    except Exception:
                        pass
                    await asyncio.sleep(0.3)
        except Exception as e:
            log.error(f"pos monitor: {e}")
        next_timers["positions"] = time.time() + 320
        await asyncio.sleep(320)


async def check_position(user_id, coin, pos):
    if pos.get("notified_close"):
        return
    price = await get_simple_price(coin)
    if not price:
        return
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
        reason, urgency = "✅ Достигнут TP3\nМожно фиксировать всю позицию.", "success"
    elif hit_tp2 and not pos.get("notified_tp"):
        reason, urgency = "✅ Достигнут TP2\nЧастично фиксируем.", "success"
    elif hit_tp1 and not pos.get("notified_tp"):
        reason, urgency = "✅ Достигнут TP1\nМожно закрыть часть.", "success"
    if urgency == "success":
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
    if not reason:
        return
    text = (f"{'🛑' if urgency == 'danger' else '✅'} <b>Рекомендация по {coin}</b>\n\nНаправление: <b>{side}</b>\n"
            f"Вход: <code>{fmt(entry)}</code>\nСейчас: <code>{fmt(price)}</code>\nPnL: <b>{pnl:+.2f}%</b>\n\n{html.escape(reason)}")
    try:
        msg = await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")],
            [InlineKeyboardButton(text="❌ Закрыл", callback_data=f"pos:close:{coin}")]]))
        track_msg(user_id, msg)
        if urgency in ("danger", "success"):
            pos["notified_close"] = True
            set_user_open_position(user_id, coin, pos)
    except Exception:
        pass


# ====================== ХЕНДЛЕРЫ ======================
@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message):
    await asyncio.to_thread(save_user_data, m.from_user.id, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    welcome_text = ("👋 <b>Привет! Добро пожаловать в IXCLY Trading Bot!</b>\n\nЯ твой персональный крипто-помощник.\n\n"
                    "📊 <b>Умный анализ</b> — 20+ индикаторов, свечные паттерны, 4 таймфрейма и проверка на истории.\n"
                    "📈 <b>Слежение за сделками</b> — нажми «Вошёл в сделку», и я буду следить.\n"
                    "🔥 <b>Готовые сигналы</b> — присылаю только те, что прошли все фильтры и бэктест.\n"
                    "📰 <b>Новости и фон</b> — объясняю простым языком.\n"
                    "⭐ <b>Твоё избранное</b> — сохраняй идеи.\n\n⚠ <i>Криптовалюта — это рискованно!</i>\n\n👇 Выбери раздел в меню ниже:")
    msg = await m.answer(welcome_text, reply_markup=get_coins_keyboard(), parse_mode="HTML")
    track_msg(m.from_user.id, msg)


@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, f"👋 <b>IXCLY v{VERSION}</b>\n\nГлавное меню:", reply_markup=get_coins_keyboard(), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data == "global_stats")
async def cb_global_stats(cb: types.CallbackQuery):
    hist = _load_signal_history()
    total = len(hist)
    wins = sum(1 for x in hist if x.get("status") == "win")
    losses = sum(1 for x in hist if x.get("status") == "loss")
    active = sum(1 for x in hist if x.get("status") == "active")
    
    closed = wins + losses
    winrate = (wins / closed * 100) if closed > 0 else 0.0
    
    fast_wins = sum(1 for x in hist if x.get("status") == "win" and x.get("type") == "Быстрый сигнал")
    fast_total = sum(1 for x in hist if x.get("status") in ("win", "loss") and x.get("type") == "Быстрый сигнал")
    fast_wr = (fast_wins / fast_total * 100) if fast_total > 0 else 0.0

    text = (
        f"📊 <b>Статистика сигналов бота</b>\n\n"
        f"📈 Всего отправлено: <b>{total}</b>\n"
        f"⏳ В работе (ждут TP/SL): <b>{active}</b>\n\n"
        f"✅ Успешных (TP1): <b>{wins}</b>\n"
        f"❌ Убыточных (SL1): <b>{losses}</b>\n"
        f"🎯 Общий винрейт: <b>{winrate:.1f}%</b>\n"
    )
    if fast_total > 0:
        text += f"⚡ Винрейт (Быстрые): <b>{fast_wr:.1f}%</b> (из {fast_total})\n"
    
    text += f"\n<i>* Статистика ведется автоматически. Учитываются сделки, где цена дошла до TP1 или SL1.</i>"
    
    await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("search_page:"))
async def cb_search_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    try:
        await cb.message.edit_reply_markup(reply_markup=get_search_coins_keyboard(page))
    except Exception:
        pass
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("c:"))
async def cb_coin(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[1], cb.from_user.id
    if uid in busy_users:
        await safe_answer(cb, "Уже обрабатываю…")
        return
    busy_users.add(uid)
    try:
        await safe_answer(cb, "⏳ Анализирую (индикаторы, свечи, бэктест)…")
        res, err = await analyze_coin(coin)
        if err:
            await safe_edit(cb.message, err, reply_markup=MENU_KB, user_id=uid)
            return
        await safe_delete(cb.message)
        await send_analysis(cb.message.chat.id, res, uid)
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally:
        busy_users.discard(uid)


@dp.callback_query(F.data.startswith("r:"))
async def cb_refresh(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[1], cb.from_user.id
    if uid in busy_users:
        await safe_answer(cb, "Уже обрабатываю…")
        return
    busy_users.add(uid)
    try:
        await safe_answer(cb, "🔄 Пересчитываю…")
        res, err = await analyze_coin(coin, force=True)
        if err:
            await safe_edit(cb.message, err, reply_markup=MENU_KB, user_id=uid)
            return
        await safe_delete(cb.message)
        await send_analysis(cb.message.chat.id, res, uid)
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally:
        busy_users.discard(uid)


@dp.callback_query(F.data == "news_menu")
async def cb_news_menu(cb: types.CallbackQuery):
    uid = cb.from_user.id
    await safe_answer(cb, "📰 Загружаю новость…")
    await safe_edit(cb.message, "⏳ Перевожу и анализирую новость…", user_id=uid)
    try:
        text, img_url = await prepare_news()
        if not text:
            await safe_edit(cb.message, "📭 Свежих новостей пока нет", reply_markup=MENU_KB, user_id=uid)
            return
        img = await download_bytes(img_url) if img_url else None
        await safe_delete(cb.message)
        if img:
            if len(text) <= 1024:
                msg = await cb.message.answer_photo(BufferedInputFile(img, "news.jpg"), caption=text, parse_mode="HTML", reply_markup=MENU_KB)
            else:
                msg_photo = await cb.message.answer_photo(BufferedInputFile(img, "news.jpg"))
                track_msg(uid, msg_photo)
                msg = await cb.message.answer(text, parse_mode="HTML", reply_markup=MENU_KB, disable_web_page_preview=True)
        else:
            msg = await cb.message.answer(text, parse_mode="HTML", reply_markup=MENU_KB, disable_web_page_preview=True)
        track_msg(uid, msg)
    except Exception as e:
        log.error(f"news error: {e}")
        await safe_edit(cb.message, "Ошибка при загрузке новости.", reply_markup=MENU_KB, user_id=uid)


def _signals_overview_text_kb():
    text = f"🔥 <b>Сигналы</b>\n\n📊 <b>Кратко о рынке:</b>\n{html.escape(market_bias.get('impact', 'Нейтральный фон'))}\n\nВыберите категорию:"
    kb = [
        [InlineKeyboardButton(text=f"⚡ Быстрый ({len(current_signals_db['Быстрый сигнал'])})", callback_data="sig_list:Быстрый сигнал")],
        [InlineKeyboardButton(text=f"⏳ Среднесрок ({len(current_signals_db['Среднесрок'])})", callback_data="sig_list:Среднесрок")],
        [InlineKeyboardButton(text=f"🛡 Долгосрок ({len(current_signals_db['Долгосрок'])})", callback_data="sig_list:Долгосрок")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=kb)


@dp.callback_query(F.data == "last_signals")
async def cb_last(cb: types.CallbackQuery):
    if last_signals_data.get("text") and time.time() - last_signals_data.get("ts", 0) < 3600:
        await safe_edit(cb.message, last_signals_data["text"], reply_markup=last_signals_data.get("kb"), user_id=cb.from_user.id)
    else:
        text, kb = _signals_overview_text_kb()
        await safe_edit(cb.message, text, reply_markup=kb, user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("sig_list:"))
async def cb_sig_list(cb: types.CallbackQuery):
    category = cb.data.split(":")[1]
    sigs = current_signals_db.get(category, [])[:10]
    if not sigs:
        text = (f"📋 <b>Сигналы: {category}</b>\n\n<i>Сейчас сигналов в этой категории нет. Это нормально: бот присылает только те идеи, "
                f"что прошли индикаторы, 4 таймфрейма, контекст рынка и бэктест.</i>")
        kb = [[InlineKeyboardButton(text="⬅ Назад к обзору", callback_data="last_signals")]]
        await safe_edit(cb.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), user_id=cb.from_user.id)
        await safe_answer(cb, "Пусто", False)
        return
    text = f"📋 <b>Сигналы: {category}</b>\n\n"
    kb = []
    for s in sigs:
        emoji = "🟢" if s["is_long"] else "🔴"
        text += f"{emoji} <b>{s['coin']}</b> · Conf: {s['strength']}%\n"
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
        gainers = sorted([x for x in movers if float(x["priceChangePercent"]) > 0], key=lambda x: float(x["priceChangePercent"]), reverse=True)
        losers = sorted([x for x in movers if float(x["priceChangePercent"]) < 0], key=lambda x: float(x["priceChangePercent"]))
        rg = random.sample(gainers[:15], min(5, len(gainers[:15])))
        rl = random.sample(losers[:15], min(5, len(losers[:15])))
        rg.sort(key=lambda x: float(x["priceChangePercent"]), reverse=True)
        rl.sort(key=lambda x: float(x["priceChangePercent"]))
        text = "🧐 <b>Интересные за 24ч</b>\n\n🟢 <b>Топ роста:</b>\n"
        for x in rg:
            text += f"<code>{x['symbol'][:-4]}</code> +{float(x['priceChangePercent']):.1f}%\n"
        text += "\n🔴 <b>Топ падения:</b>\n"
        for x in rl:
            text += f"<code>{x['symbol'][:-4]}</code> {float(x['priceChangePercent']):.1f}%\n"
        await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    except Exception as e:
        log.error(f"interesting error: {e}")
        await safe_edit(cb.message, "Не удалось получить данные", reply_markup=MENU_KB, user_id=cb.from_user.id)


@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_edit(cb.message, "🔍 <b>Выбери монету из списка</b>\n<i>Или просто напиши тикер сообщением:</i>", reply_markup=get_search_coins_keyboard(0), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,12}$"))
async def msg_ticker(m: types.Message):
    coin, uid = normalize_ticker(m.text), m.from_user.id
    if uid in busy_users:
        return
    busy_users.add(uid)
    wait = None
    try:
        wait = await m.answer("⏳ Анализирую (индикаторы, свечи, бэктест)…")
        res, err = await analyze_coin(coin)
        if err:
            msg = await m.answer(err, parse_mode="HTML")
            track_msg(uid, msg)
            return
        await send_analysis(m.chat.id, res, uid)
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally:
        busy_users.discard(uid)
        if wait:
            await safe_delete(wait)


@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data == "sig:save_new")
async def cb_sig_save_new(cb: types.CallbackQuery):
    await safe_edit(cb.message, "Выбери монету:", reply_markup=get_save_coins_keyboard(), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("sig:save_page:"))
async def cb_sig_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[2])
    try:
        await cb.message.edit_reply_markup(reply_markup=get_save_coins_keyboard(page))
    except Exception:
        pass
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("sig:do_save:") | F.data.startswith("sig:quick_save:"))
async def cb_sig_save(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[-1], cb.from_user.id
    cached = result_cache.get(uid)
    text = cached["text"] if cached and cached.get("coin") == coin else f"Сигнал {coin}"
    await asyncio.to_thread(add_user_saved_signal, uid, coin, text)
    asyncio.create_task(telegram_backup("сохранение"))
    await safe_answer(cb, f"✅ {coin} сохранён")
    if "quick_save" not in cb.data:
        await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(uid), user_id=uid)


@dp.callback_query(F.data.startswith("sig:view:"))
async def cb_sig_view(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    item = next((s for s in get_user_saved_signals(cb.from_user.id) if s["coin"] == coin), None)
    if not item:
        await safe_answer(cb, "Не найдено", True)
        return
    await safe_edit(cb.message, item["text"][:4000], reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data == "sig:del_menu")
async def cb_sig_del_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, "Выбери для удаления:", reply_markup=get_delete_signals_menu(cb.from_user.id), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data.startswith("sig:del:"))
async def cb_sig_del(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    await asyncio.to_thread(del_user_saved_signal, cb.from_user.id, coin)
    asyncio.create_task(telegram_backup("удаление"))
    await safe_answer(cb, f"🗑 {coin} удалён")
    await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), user_id=cb.from_user.id)


@dp.callback_query(F.data.startswith("pos:enter:"))
async def cb_pos_enter(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[2], cb.from_user.id
    await safe_answer(cb, "⏳ Фиксирую…")
    res, err = await analyze_coin(coin)
    if err:
        await cb.message.answer("Не удалось получить уровни, попробуйте ещё раз")
        return
    data = {"side": res["side"], "entry_price": res["entry"], "entered_at": time.time(), "tp1": res["tps"][0], "tp2": res["tps"][1],
            "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1], "notified_close": False, "notified_tp": False}
    await asyncio.to_thread(set_user_open_position, uid, coin, data)
    asyncio.create_task(telegram_backup("открытие сделки"))
    text = (f"{'🟢' if res['side'] == 'LONG' else '🔴'} <b>Позиция открыта</b>\n\nМонета: <b>{coin}</b>\nНаправление: <b>{res['side']}</b>\n"
            f"Вход: <code>{fmt(res['entry'])}</code>\n\nБуду следить и сообщу, когда лучше закрыть.")
    msg = await bot.send_message(cb.message.chat.id, text, parse_mode="HTML", reply_markup=get_saved_signal_view_keyboard(uid, coin))
    track_msg(uid, msg)


@dp.callback_query(F.data.startswith("pos:close:"))
async def cb_pos_close(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    await asyncio.to_thread(set_user_open_position, cb.from_user.id, coin, None)
    asyncio.create_task(telegram_backup("закрытие сделки"))
    await safe_answer(cb, "❌ Позиция закрыта")
    try:
        await cb.message.edit_reply_markup(reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin))
    except Exception:
        pass


@dp.callback_query(F.data.startswith("pos:status:"))
async def cb_pos_status(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    pos = get_open_position(cb.from_user.id, coin)
    if not pos:
        await safe_answer(cb, "Позиция не найдена", True)
        return
    price = await get_simple_price(coin)
    if not price:
        await safe_answer(cb, "Нет цены", True)
        return
    entry = pos["entry_price"]
    pnl = ((price - entry) / entry * 100) if pos["side"] == "LONG" else ((entry - price) / entry * 100)
    text = (f"📈 <b>Статус {coin}</b>\n\nНаправление: <b>{pos['side']}</b>\nВход: <code>{fmt(entry)}</code>\nСейчас: <code>{fmt(price)}</code>\n"
            f"PnL: <b>{pnl:+.2f}%</b>\n\nTP1: <code>{fmt(pos['tp1'])}</code>\nTP2: <code>{fmt(pos['tp2'])}</code>\nSL1: <code>{fmt(pos['sl1'])}</code>")
    await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data == "ignore")
async def cb_ignore(cb: types.CallbackQuery):
    await safe_answer(cb)


# ====================== АДМИН-БОТ ======================
if admin_dp:
    @admin_dp.errors()
    async def admin_errors_handler(event: ErrorEvent):
        if isinstance(event.exception, TelegramNetworkError):
            return True
        log.error(f"Ошибка админ-бота: {event.exception}", exc_info=event.exception)
        return True

    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_cmd_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            await m.answer(f"⛔ Доступ только для администратора.\n\nВаш ID: <code>{m.from_user.id}</code>", parse_mode="HTML")
            return
        total, active, _, _ = user_counts()
        text = f"✅ <b>Админ-панель IXCLY v{VERSION}</b>\n👥 Пользователей: {total} (активных {active})"
        if total == 0:
            text += "\n\n⚠ База пуста. Нажмите «📥 Восстановить»."
        await m.answer(text, reply_markup=get_admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            await safe_answer(cb, "Нет доступа", True)
            return
        act = cb.data.split(":")[1]

        if act == "toggle_news":
            bot_flags["news_enabled"] = not bot_flags["news_enabled"]
            await safe_answer(cb, f"Новости: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}")
            try:
                await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            except Exception:
                pass
            return
        if act == "toggle_scan":
            bot_flags["alerts_enabled"] = not bot_flags["alerts_enabled"]
            await safe_answer(cb, f"Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}")
            try:
                await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            except Exception:
                pass
            return

        await safe_answer(cb, "⏳ Выполняю…")

        if act == "stats":
            total, active, _, _ = user_counts()
            up = int(time.time() - stats["started"])
            ls = last_scan_info
            text = (f"📊 <b>Статистика</b>\n\nВерсия: {VERSION}\nЮзеров: {total} (активных {active})\nСигналов: {stats['signals']}\n"
                    f"Новостей: {stats['news_sent']}\nАптайм: {up // 3600}ч {up % 3600 // 60}м\n"
                    f"Новость: {market_bias.get('engine')} · bias {market_bias['bias']}\n{html.escape(market_bias.get('title_ru', '')[:70])}\n\n"
                    f"🔍 <b>Последний скан:</b> этап1 {ls['stage1']} → этап2 {ls['stage2']} → бэктест {ls['backtest_ok']} → итог {ls['final']} ({ls['secs']} с)")
            await cb.message.answer(text, parse_mode="HTML")
        elif act == "users":
            users = _load_users()
            lines = [f"👥 Пользователи ({len(users)})\n"] + [
                f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or u.get('username') or '—')[:18])}"
                for uid, u in list(users.items())[-30:]]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        elif act == "news_now":
            try:
                text, img_url = await prepare_news(force=True)
                if not text:
                    await cb.message.answer("Свежих новостей нет")
                    return
                img = await download_bytes(img_url) if img_url else None
                sent, failed, _, _ = await broadcast({"kind": "photo" if img else "text", "text": text, "data": img, "markup": MENU_KB})
                stats["news_sent"] += sent
                await cb.message.answer(f"✅ Отправлено: {sent}, ошибок: {failed}")
            except Exception as e:
                await cb.message.answer(f"Ошибка: {html.escape(str(e))}")
        elif act == "scan_test":
            await cb.message.answer("🔍 Запускаю полный скан (до нескольких минут), рассылки не будет…")
            sigs = await scan_once(send=False)
            ls = last_scan_info
            head = f"этап1 {ls['stage1']} → этап2 {ls['stage2']} → бэктест {ls['backtest_ok']} → итог {ls['final']} ({ls['secs']} с)"
            if sigs:
                lines = [f"{'🟢' if s['is_long'] else '🔴'} {s['coin']} · {s['type']} · Conf {s['strength']}%" for s in sigs]
                await cb.message.answer(f"🔍 Тест сканера\n{head}\n\n" + "\n".join(lines))
            else:
                await cb.message.answer(f"🔍 Тест сканера: сигналов нет\n{head}\n\nЭто нормально: пороги строгие.")
        elif act == "broadcast_now":
            if last_signals_data.get("text"):
                sent, _, _, _ = await broadcast({"kind": "text", "text": last_signals_data["text"], "markup": last_signals_data.get("kb")})
                await cb.message.answer(f"Рассылка: {sent}")
            else:
                await cb.message.answer("Нет сигналов")
        elif act == "diag":
            text = (f"🔧 <b>Диагностика</b>\n\nЧатов: {len(active_chats)}\nADMIN_IDS: {len(ADMIN_IDS)}\nЗадержка: {POLL_START_DELAY} с\n"
                    f"Пороги: CONF_STAGE1={CONF_STAGE1:.0f}, CONF_FINAL={CONF_FINAL:.0f}, бэктест ≥{BT_MIN_TRADES} сделок, ср.R ≥{BT_MIN_AVG_R}\n\n")
            text += await run_ai_diag()
            await cb.message.answer(text, parse_mode="HTML")
        elif act == "backup":
            ok = await telegram_backup("вручную")
            await cb.message.answer("💾 Полный бэкап отправлен в админку." if ok else "Состояние пусто.")
        elif act == "restore":
            await cb.message.answer("Пришли файл bot_state.json или users.json.")
        elif act == "timers":
            now = time.time()

            def fmt_time(sec):
                if sec <= 0:
                    return "выполняется сейчас"
                m, s = divmod(int(sec), 60)
                return f"через {m} мин {s} сек" if m else f"через {s} сек"

            lines = ["⏳ <b>Таймеры:</b>\n",
                     f"🤖 Сканер: {fmt_time(next_timers['scanner'] - now)}",
                     f"📈 Мониторинг: {fmt_time(next_timers['positions'] - now)}",
                     f"📰 Новости: {fmt_time(next_timers['news_watcher'] - now)}",
                     f"📊 Статистика: {fmt_time(next_timers['global_signals'] - now)}",
                     f"💾 Бэкап: {fmt_time(next_timers['backup'] - now)}"]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        global current_signals_db
        if m.from_user.id not in ADMIN_IDS or not (m.document.file_name or "").endswith(".json"):
            return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            content = await admin_bot.download_file(f.file_path)
            data = json.loads(content.read().decode("utf-8"))
            
            if isinstance(data, dict):
                if "users" in data or "signal_history" in data:
                    with open(STATE_FILE, "w", encoding="utf-8") as out_f:
                        json.dump(data, out_f, ensure_ascii=False, indent=2)
                    if "current_signals_db" in data:
                        current_signals_db = data["current_signals_db"]
                else:
                    current_state = _load_full_state()
                    current_state["users"] = data
                    _save_full_state(current_state)

                load_active_chats()
                await m.answer(f"✅ Полное состояние успешно восстановлено!")
        except Exception as e:
            await m.answer(f"Ошибка: {html.escape(str(e))}", parse_mode="HTML")


# ====================== MAIN ======================
async def run_polling(dispatcher, tg_bot, name, stop_event):
    attempt = 0
    while not stop_event.is_set():
        try:
            attempt += 1
            log.info(f"{name}: запуск поллинга (попытка {attempt})")
            try:
                await tg_bot.delete_webhook(drop_pending_updates=(attempt == 1))
            except Exception:
                pass
            await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=30)
            if not stop_event.is_set():
                log.warning(f"{name}: start_polling завершился. Перезапуск через 5 сек...")
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except (TelegramNetworkError, aiohttp.ClientError) as e:
            log.warning(f"{name}: сетевая ошибка: {e}")
            await asyncio.sleep(min(2 ** attempt, 30))
        except TelegramConflictError as e:
            log.warning(f"{name}: конфликт getUpdates: {e}")
            await asyncio.sleep(min(2 ** attempt, 60))
        except Exception as e:
            log.error(f"{name}: упал: {e}")
            await asyncio.sleep(5)


async def main():
    global http, news_lock, current_signals_db
    keep_alive()
    news_lock = asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/2.11"})
    init_mongo()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    background = []
    pollers = []
    try:
        try:
            me = await bot.get_me()
            runtime["main_bot"] = f"@{me.username}"
        except Exception as e:
            log.error(f"get_me основного: {e}")
        if admin_bot:
            try:
                me = await admin_bot.get_me()
                runtime["admin_bot"] = f"@{me.username}"
            except Exception as e:
                log.error(f"get_me админа: {e}")

        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot:
                await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([
                types.BotCommand(command="menu", description="🚀 Меню")
            ])
        except Exception:
            pass

        if POLL_START_DELAY > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError:
                pass
        if stop.is_set():
            return

        restored = await telegram_restore()
        if restored:
            log.info(f"✅ Состояние полностью восстановлено, юзеров: {restored}")
        
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
        ]

        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp:
            pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))

        log.info(f"🚀 IXCLY v{VERSION} запущен.")
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
    finally:
        for t in pollers:
            t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        try:
            await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=12)
        except Exception:
            pass
        for t in background:
            t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        try:
            await bot.session.close()
        except Exception:
            pass
        if admin_bot:
            try:
                await admin_bot.session.close()
            except Exception:
                pass
        await http.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановлен")
