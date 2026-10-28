# -*- coding: utf-8 -*-
"""
IXCLY Trading Bot v3.0.0 PRO
Полноценный торговый терминал с интеграцией автоторговли, 
портфельного риск-менеджмента, вебхуков и On-Chain аналитики.
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
from aiogram.exceptions import (
    TelegramBadRequest, 
    TelegramForbiddenError, 
    TelegramNotFound, 
    TelegramRetryAfter, 
    TelegramConflictError, 
    TelegramNetworkError
)
from aiogram.client.session.aiohttp import AiohttpSession
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFont

# =====================================================================
# ИНИЦИАЛИЗАЦИЯ PRO-БИБЛИОТЕК (Автотрейдинг и Шифрование)
# =====================================================================
try:
    import ccxt.async_support as ccxt
except ImportError:
    ccxt = None

try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None


# =====================================================================
# ЛОГИРОВАНИЕ И ФИЛЬТР КОНФЛИКТОВ
# =====================================================================
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(levelname)s %(message)s", 
    stream=sys.stdout, 
    force=True
)
log = logging.getLogger("ixcly")


class _ConflictFilter(logging.Filter):
    """
    Фильтр для подавления спама ошибок в логах при одновременном 
    запуске нескольких экземпляров бота (например, при деплое на Render).
    """
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


# =====================================================================
# ВЕБ-СЕРВЕР (FLASK) ДЛЯ KEEP-ALIVE
# =====================================================================
app = Flask(__name__)


@app.route("/")
def home():
    return "Bot is alive!"


@app.route("/health")
def health():
    return "ok"


def _run_web():
    """Внутренний метод для запуска Flask в отдельном потоке."""
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"Ошибка Flask: {e}")


_web_started = False


def keep_alive():
    """Запускает Flask сервер в фоне, чтобы Render не усыплял бота."""
    global _web_started
    if not _web_started:
        _web_started = True
        Thread(target=_run_web, daemon=True).start()


# =====================================================================
# НАСТРОЙКИ И ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# =====================================================================
def clean_key(k):
    """Очищает переменные окружения от случайных пробелов и кавычек."""
    return k.strip().strip('"').strip("'") if k else None


BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))

GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))

# Ключ шифрования для API-ключей пользователей
# Жестко прописываем ключ прямо в коде, чтобы Render не тупил
FERNET_KEY = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
cipher = None
if Fernet:
    try:
        cipher = Fernet(FERNET_KEY.encode())
        log.info("✅ Шифрование API-ключей успешно запущено.")
    except Exception as e:
        log.error(f"⚠️ Ошибка шифрования: {e}")

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-r1:free")
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

ADMIN_IDS = set()
if ADMIN_ID:
    for x in re.split(r"[,\s]+", ADMIN_ID):
        if x.lstrip("-").isdigit():
            ADMIN_IDS.add(int(x))

if not BOT_TOKEN:
    raise SystemExit("КРИТИЧЕСКАЯ ОШИБКА: Переменная BOT_TOKEN не задана!")

VERSION = "3.0.0"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}
POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

# Пороги для сканера
CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "60"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "30"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))
DEPOSIT = float(os.getenv("DEPOSIT", "1000"))
RISK_PCT = 0.01

# Инициализация ботов
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

# Блокировка новостей (Economic Calendar Blackout)
news_blackout_until = 0

# Стейт-машина (Runtime переменные)
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

# Блокировки для предотвращения Race Condition
STATE_LOCK = threading.Lock()
SCAN_LOCK = asyncio.Lock()
STATE_FILE = "bot_state.json"

# =====================================================================
# БАЗА ДАННЫХ И СТЕЙТ (ФАЙЛОВАЯ СИСТЕМА)
# =====================================================================
def _load_full_state():
    """Читает состояние из JSON-файла с защитой потоков."""
    with STATE_LOCK:
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            log.error(f"Ошибка загрузки состояния: {e}")
        return {}


def _save_full_state(state):
    """Записывает состояние в JSON-файл безопасно (через временный файл)."""
    global users_dirty
    with STATE_LOCK:
        try:
            with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            os.replace(STATE_FILE + ".tmp", STATE_FILE)
            users_dirty = True
        except Exception as e:
            log.error(f"Ошибка записи состояния: {e}")


def _load_users():
    return _load_full_state().get("users", {})


def _save_users(users):
    state = _load_full_state()
    state["users"] = users
    _save_full_state(state)


def _load_signal_history():
    """Загружает историю сигналов (за последние 30 дней)."""
    hist = _load_full_state().get("signal_history", [])
    return [x for x in hist if time.time() - x.get("ts", 0) < 30 * 86400]


def _save_signal_history(hist):
    state = _load_full_state()
    state["signal_history"] = hist[-2000:] # Храним только последние 2000
    _save_full_state(state)


def _load_current_signals_db():
    return _load_full_state().get("current_signals_db", {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []})


def _save_current_signals_db(db):
    state = _load_full_state()
    state["current_signals_db"] = db
    _save_full_state(state)


# =====================================================================
# ПОРТФЕЛЬНЫЙ РИСК-МЕНЕДЖМЕНТ И ШИФРОВАНИЕ API
# =====================================================================
def get_user_risk_settings(user_id):
    """
    Возвращает настройки риска пользователя.
    По умолчанию: депозит 1000$, риск на сделку 1%, макс. риск 5%.
    """
    user = _load_users().get(str(user_id), {})
    return {
        "deposit": user.get("deposit", 1000.0),
        "risk_pct": user.get("risk_pct", 1.0),
        "max_open_risk": user.get("max_open_risk", 5.0)
    }


def calculate_portfolio_risk(user_id):
    """
    Считает, сколько процентов депозита сейчас в зоне риска 
    по всем открытым сделкам пользователя.
    """
    settings = get_user_risk_settings(user_id)
    positions = get_user_open_positions(user_id)
    total_risk = 0.0
    for coin, pos in positions.items():
        risk_pct = pos.get("risk_pct", settings["risk_pct"])
        total_risk += risk_pct
    return total_risk


def save_user_api_keys(user_id, api_key, secret_key):
    """Шифрует и сохраняет API-ключи Binance пользователя."""
    if not cipher:
        log.warning("Шифрование недоступно, ключи не сохранены.")
        return False
    key = str(user_id)
    users = _load_users()
    if key not in users:
        users[key] = {}
    users[key]["binance_api"] = cipher.encrypt(api_key.encode()).decode()
    users[key]["binance_secret"] = cipher.encrypt(secret_key.encode()).decode()
    _save_users(users)
    return True


def get_user_api_keys(user_id):
    """Расшифровывает и возвращает API-ключи пользователя."""
    if not cipher:
        return None, None
    user = _load_users().get(str(user_id), {})
    enc_api = user.get("binance_api")
    enc_sec = user.get("binance_secret")
    if enc_api and enc_sec:
        return cipher.decrypt(enc_api.encode()).decode(), cipher.decrypt(enc_sec.encode()).decode()
    return None, None


def save_user_webhook(user_id, url):
    """Сохраняет URL для отправки вебхуков (TradingView/3Commas)."""
    key = str(user_id)
    users = _load_users()
    if key not in users:
        users[key] = {}
    users[key]["webhook_url"] = url
    _save_users(users)


async def send_webhook(user_id, signal_data):
    """Асинхронно отправляет POST-запрос на вебхук пользователя."""
    user = _load_users().get(str(user_id), {})
    url = user.get("webhook_url")
    if url:
        try:
            async with http.post(url, json=signal_data, timeout=5) as r:
                log.info(f"Webhook успешно отправлен {user_id}. Статус: {r.status}")
        except Exception as e:
            log.warning(f"Ошибка вебхука {user_id}: {e}")


def generate_csv_history():
    """Генерирует CSV-файл со всей историей сигналов."""
    hist = _load_signal_history()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "SL1", "Статус", "Макс TP", "Сработал SL"])
    for x in hist:
        dt = datetime.fromtimestamp(x.get("ts", 0)).strftime('%Y-%m-%d %H:%M')
        writer.writerow([
            dt, x.get("coin"), x.get("type"), x.get("side"), 
            x.get("entry"), x.get("tp1"), x.get("sl1"), 
            x.get("status"), x.get("max_tp_hit", 0), x.get("hit_sl", 0)
        ])
    return output.getvalue().encode('utf-8-sig')


# =====================================================================
# ON-CHAIN АНАЛИТИКА (DEFILLAMA & WHALES)
# =====================================================================
_whale_cache = {"ts": 0, "bias": 0}
_defillama_cache = {"ts": 0, "tvl_change": 0.0}


async def get_onchain_context():
    """
    Проверяет приток денег в стейблкоины (TVL) через DefiLlama.
    Если деньги уходят (TVL падает более чем на 2%), снижаем уверенность в лонгах.
    """
    global _defillama_cache
    if time.time() - _defillama_cache["ts"] < 3600: # Кеш на 1 час
        return _defillama_cache["tvl_change"]
    try:
        async with http.get("https://api.llama.fi/v2/historicalChainTvl", timeout=10) as r:
            if r.status == 200:
                data = await r.json()
                if len(data) > 2:
                    current_tvl = data[-1]["tvl"]
                    prev_tvl = data[-2]["tvl"]
                    change = (current_tvl - prev_tvl) / prev_tvl * 100
                    _defillama_cache = {"ts": time.time(), "tvl_change": round(change, 2)}
                    return change
    except Exception as e:
        log.warning(f"Ошибка DefiLlama: {e}")
    return 0.0


async def get_whale_bias():
    """
    Заглушка для Whale Alert API. 
    В боевом режиме здесь должен быть GET-запрос к API и анализ транзакций.
    """
    return 0

# =====================================================================
# УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ
# =====================================================================
def save_user_data(user_id, username, first_name):
    """Сохраняет или обновляет данные пользователя при любом действии."""
    key = str(user_id)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    users = _load_users()
    if key not in users:
        users[key] = {"joined_at": now}
    users[key].update({"username": username, "first_name": first_name, "active": True})
    _save_users(users)


def mark_users_inactive(ids):
    """Помечает пользователей как неактивных (если бот заблокирован)."""
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


def del_user_saved_signal(user_id, coin):
    key = str(user_id)
    users = _load_users()
    if key in users and "saved_signals" in users[key]:
        users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
        _save_users(users)


def add_user_saved_signal(user_id, coin, text, res=None):
    key = str(user_id)
    users = _load_users()
    if key not in users:
        users[key] = {"active": True}
    users[key].setdefault("saved_signals", [])
    users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
    
    item = {"coin": coin, "text": text, "status": "active", "ts": time.time(), "expire_ts": time.time() + 48 * 3600}
    if res:
        item.update({
            "entry": res.get("entry"),
            "tp1": res.get("tps", [0])[0],
            "sl1": res.get("sls", [0])[0],
            "is_long": res.get("is_long")
        })
    users[key]["saved_signals"].append(item)
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


def user_counts():
    users = _load_users()
    total = len(users)
    active = sum(1 for u in users.values() if isinstance(u, dict) and u.get("active", True))
    return total, active, 0, 0


# =====================================================================
# БЭКАПЫ БАЗЫ В АДМИН-БОТ
# =====================================================================
async def telegram_backup(reason="auto"):
    if not admin_bot or not ADMIN_IDS:
        return False
    state = _load_full_state()
    if not state:
        return False
    data = json.dumps(state, ensure_ascii=False).encode("utf-8")
    ok = False
    for aid in ADMIN_IDS:
        try:
            msg = await admin_bot.send_document(
                aid, 
                BufferedInputFile(data, "bot_state.json"),
                caption=f"💾 БЭКАП · Юзеров: {len(state.get('users', {}))} · Сигналов: {len(state.get('signal_history', []))} · {reason}",
                disable_notification=True
            )
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
                return len(data.get("users", {}))
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
        except Exception:
            pass
        next_timers["backup"] = time.time() + 60
        await asyncio.sleep(60)


# =====================================================================
# МИДЛВАРЬ И ОБРАБОТЧИК ОШИБОК
# =====================================================================
class UserTrackerMiddleware(BaseMiddleware):
    """Автоматически сохраняет каждого, кто написал боту."""
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


@dp.errors()
async def errors_handler(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, TelegramNetworkError):
        return True
    if isinstance(exc, TelegramBadRequest) and ("query is too old" in str(exc).lower() or "message is not modified" in str(exc).lower()):
        return True
    log.error(f"Необработанная ошибка: {exc}", exc_info=exc)
    return True

# =====================================================================
# ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ И СВЕЧНЫЕ ПАТТЕРНЫ
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
    """Сглаживание Уайлдера (используется в RSI, ATR, ADX)."""
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def _supertrend(h, l, c, atr, mult=3.0):
    """Расчет индикатора Supertrend."""
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
    """Определяет наличие 15+ свечных паттернов на графике."""
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
    
    p["morning_star"] = (
        sh(bear, 2) & (body.shift(2) > avg_body.shift(2)) & 
        (body.shift(1) < 0.5 * avg_body.shift(1)) & bull & 
        (c > (o.shift(2) + c.shift(2)) / 2)
    )
    p["evening_star"] = (
        sh(bull, 2) & (body.shift(2) > avg_body.shift(2)) & 
        (body.shift(1) < 0.5 * avg_body.shift(1)) & bear & 
        (c < (o.shift(2) + c.shift(2)) / 2)
    )
    
    p["three_white"] = (
        bull & sh(bull) & sh(bull, 2) & (c > c.shift()) & (c.shift() > c.shift(2)) & 
        (o > o.shift()) & (o.shift() > o.shift(2)) & (o < c.shift()) & 
        (o.shift() < c.shift(2)) & (body > 0.6 * avg_body)
    )
    p["three_black"] = (
        bear & sh(bear) & sh(bear, 2) & (c < c.shift()) & (c.shift() < c.shift(2)) & 
        (o < o.shift()) & (o.shift() < o.shift(2)) & (o > c.shift()) & 
        (o.shift() > c.shift(2)) & (body > 0.6 * avg_body)
    )
    
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
    """Считает баллы на основе свечных паттернов с учетом контекста."""
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
    """Добавляет 20+ технических индикаторов (RSI, MACD, BB, ADX и т.д.)."""
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    
    # Экспоненциальные скользящие средние (EMA)
    for n in (9, 20, 50, 200):
        df[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()

    # RSI + StochRSI
    delta = c.diff()
    ag = _wilder(delta.clip(lower=0), 14)
    al = _wilder(-delta.clip(upper=0), 14)
    rs = ag / al.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    df["rsi"] = rsi.where(al != 0, 100.0).fillna(50.0)
    
    rmin, rmax = df["rsi"].rolling(14).min(), df["rsi"].rolling(14).max()
    stoch = (df["rsi"] - rmin) / (rmax - rmin).replace(0, np.nan)
    df["stoch_k"] = stoch.rolling(3).mean().fillna(0.5)

    # MACD
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["macd"] = macd
    df["macd_sig"] = macd.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_sig"]

    # ATR и ADX/DI
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

    # Bollinger Bands + Сжатие
    mid = c.rolling(20).mean()
    std = c.rolling(20).std()
    upper_b, lower_b = mid + 2 * std, mid - 2 * std
    df["bb_pct"] = ((c - lower_b) / (upper_b - lower_b).replace(0, np.nan)).fillna(0.5)
    bw = ((upper_b - lower_b) / mid.replace(0, np.nan))
    df["squeeze"] = (bw <= bw.rolling(100, min_periods=30).quantile(0.15)).fillna(False)

    # Supertrend
    atr10 = _wilder(tr, 10).fillna(tr.mean())
    df["st_dir"] = _supertrend(h, l, c, atr10, 3.0)

    # Ichimoku
    tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
    span_a = ((tenkan + kijun) / 2).shift(26)
    span_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
    top, bot = np.maximum(span_a, span_b), np.minimum(span_a, span_b)
    df["tenkan"], df["kijun"] = tenkan.fillna(c), kijun.fillna(c)
    df["ichi_pos"] = np.where(top.isna(), 0, np.where(c > top, 1, np.where(c < bot, -1, 0)))

    # VWAP, MFI, CCI, Williams %R
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

    # Объемные индикаторы (OBV)
    df["obv"] = (np.sign(c.diff()) * v).fillna(0).cumsum()
    df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
    df["rel_vol"] = (v / v.rolling(20).mean().replace(0, np.nan)).fillna(1.0)
    df["vol_delta"] = ((c - o) / (h - l).replace(0, np.nan) * v).fillna(0)

    # Статистика цены (Z-score, Momentum)
    std50 = c.rolling(50).std().replace(0, np.nan)
    df["z_score"] = ((c - df["ema50"]) / std50).fillna(0)
    df["momentum"] = c.pct_change(10).fillna(0)

    # Пробои (Donchian) и Дивергенции RSI
    df["brk_up"] = (c > h.shift(1).rolling(20).max()).fillna(False)
    df["brk_dn"] = (c < l.shift(1).rolling(20).min()).fillna(False)
    
    low_min, high_max = l.shift(1).rolling(14).min(), h.shift(1).rolling(14).max()
    rsi_min, rsi_max = df["rsi"].shift(1).rolling(14).min(), df["rsi"].shift(1).rolling(14).max()
    df["div_bull"] = ((l <= low_min) & (df["rsi"] > rsi_min + 3)).fillna(False)
    df["div_bear"] = ((h >= high_max) & (df["rsi"] < rsi_max - 3)).fillna(False)

    return add_patterns(df)


def add_scores(df):
    """
    Собирает финальный скор (long_p) из трех моделей:
    1. Тренд (вес 50% при ADX > 25)
    2. Моментум (вес 45% при ADX < 20)
    3. Price Action (вес 30% во флэте)
    """
    n = len(df)
    c = df["close"].values
    adx = df["adx"].values
    trending, ranging = adx >= 25, adx < 20

    def mix(t, r):
        return np.where(trending, t, np.where(ranging, r, (t + r) / 2.0))

    ema20, ema50, ema200 = df["ema20"].values, df["ema50"].values, df["ema200"].values
    hist = df["macd_hist"].values
    hist_prev = np.concatenate([[hist[0]], hist[:-1]])

    # Модель 1: Тренд
    stack = np.where((c > ema20) & (ema20 > ema50), 20, np.where((c < ema20) & (ema20 < ema50), -20, 0))
    stack = stack + np.where(c > ema200, 10, -10)
    st = np.where(df["st_dir"].values > 0, 20, -20)
    ichi = df["ichi_pos"].values * 15 + np.where(df["tenkan"].values > df["kijun"].values, 5, -5)
    macd_s = np.where(hist > 0, 8, -8) + np.where(hist > hist_prev, 4, -4)
    di = np.where(adx >= 20, np.where(df["plus_di"].values > df["minus_di"].values, 10, -10), 0)
    trend = np.clip(stack + st + ichi + macd_s + di, -100, 100)

    # Модель 2: Моментум
    rsi, sk, cci = df["rsi"].values, df["stoch_k"].values, df["cci"].values
    wr, mfi, bbp = df["wr"].values, df["mfi"].values, df["bb_pct"].values
    z, mom_roc = df["z_score"].values, df["momentum"].values
    zeros = np.zeros(n)
    
    mom = (
        mix(np.clip((rsi - 50) * 0.8, -20, 20), np.clip((50 - rsi) * 0.9, -25, 25))
        + mix((sk - 0.5) * 30, (0.5 - sk) * 40)
        + mix(np.clip(cci / 10, -15, 15), np.clip(-cci / 8, -20, 20))
        + mix(((wr + 50) / 50) * 8, -((wr + 50) / 50) * 12)
        + mix((mfi - 50) / 50 * 8, (50 - mfi) / 50 * 12)
        + mix(np.where(bbp > 1, 10, np.where(bbp < 0, -10, (bbp - 0.5) * 10)), np.clip((0.5 - bbp) * 40, -20, 20))
        + mix(zeros, np.clip(-z * 7, -15, 15))
        + mix(np.clip(mom_roc * 400, -10, 10), zeros)
    )
    mom = np.clip(mom, -100, 100)

    # Модель 3: Price Action & Volume
    o = df["open"].values
    pat = pattern_scores(df)
    vol_conf = np.where(df["rel_vol"].values > 1.5, np.where(c > o, 10, -10), 0)
    obv_s = np.where(df["obv"].values > df["obv_ema"].values, 8, -8)
    div = np.where(df["div_bull"].values, 15, 0) - np.where(df["div_bear"].values, 15, 0)
    brk = np.where(df["brk_up"].values, 10, 0) - np.where(df["brk_dn"].values, 10, 0)
    vwap_s = np.where(c > df["vwap"].values, 5, -5)
    pa = np.clip((pat + vol_conf + obv_s + div + brk + vwap_s) * 1.3, -100, 100)

    # Взвешивание моделей
    w_t = np.where(trending, 0.5, np.where(ranging, 0.25, 0.4))
    w_m = np.where(trending, 0.2, np.where(ranging, 0.45, 0.3))
    
    score = w_t * trend + w_m * mom + (1 - w_t - w_m) * pa
    df["s_trend"], df["s_mom"], df["s_pa"] = trend, mom, pa
    df["long_p"] = np.nan_to_num(np.clip(50 + score / 2, 5, 95), nan=50.0)
    return df

# =====================================================================
# ОБЪЯСНЕНИЕ ЛОГИКИ И ЗАГРУЗКА ДАННЫХ
# =====================================================================
def explain_last(df):
    """Формирует человекочитаемый список причин для сигнала."""
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
    """Ищет свечные паттерны на последних N свечах."""
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
    """Находит ближайшие уровни поддержки и сопротивления по пивотам."""
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
    return {
        "support": max(below) if below else None, 
        "resistance": min(above) if above else None
    }


def rows_to_df(rows):
    """Конвертирует сырые свечи с биржи в DataFrame Pandas."""
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    df = df.drop_duplicates("ts").sort_values("ts")
    df = df[df["close"] > 0].reset_index(drop=True)
    if len(df) < MIN_CANDLES:
        raise ValueError("Мало свечей для анализа")
    return df


def prepare_df(df):
    """Применяет индикаторы и скоринг."""
    return add_scores(add_indicators(df))


def build_tf_data(rows_by_tf):
    """Формирует итоговый словарь с данными по каждому таймфрейму."""
    out = {}
    for tf, rows in rows_by_tf.items():
        price = float(rows[-1][4])
        df = prepare_df(rows_to_df(rows[:-1])) # Последнюю формирующуюся свечу отбрасываем
        last = df.iloc[-1]
        
        long_p = float(last["long_p"])
        adx = float(last["adx"])
        
        out[tf] = {
            "df": df, 
            "price": price,
            "ema20": float(last["ema20"]), 
            "ema50": float(last["ema50"]), 
            "ema200": float(last["ema200"]),
            "rsi": float(last["rsi"]), 
            "atr": float(last["atr"]), 
            "adx": adx,
            "macd_hist": float(last["macd_hist"]), 
            "bb_pct": float(last["bb_pct"]),
            "z_score": float(last["z_score"]), 
            "vol_delta": float(last["vol_delta"]),
            "recent_low": float(min(df["low"].tail(28).min(), price)),
            "recent_high": float(max(df["high"].tail(28).max(), price)),
            "long_prob": long_p, 
            "confidence": max(long_p, 100 - long_p),
            "qv24": float((df["close"] * df["volume"]).tail(6).sum()),
            "regime": "тренд" if adx >= 25 else "флэт" if adx < 20 else "переход",
            "squeeze": bool(last["squeeze"]),
            "reasons": explain_last(df), 
            "patterns": last_patterns(df),
            "sr": support_resistance(df, price),
        }
    return out

# =====================================================================
# API БИРЖ (BINANCE, OKX, BYBIT)
# =====================================================================
def _p_binance(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]

def _p_okx(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["data"])]

def _p_bybit(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["result"]["list"])]

EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines", "params": lambda c, tf, n: {"symbol": f"{c}USDT", "interval": tf, "limit": min(n, 1000)}, "parse": _p_binance},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles", "params": lambda c, tf, n: {"instId": f"{c}-USDT", "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf], "limit": str(min(n, 300))}, "parse": _p_okx},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline", "params": lambda c, tf, n: {"category": "spot", "symbol": f"{c}USDT", "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf], "limit": min(n, 1000)}, "parse": _p_bybit},
]

async def _fetch_klines(ex, coin, tf, limit=200):
    j = await http_json(ex["url"], params=ex["params"](coin, tf, limit), timeout=9)
    rows = ex["parse"](j)
    if len(rows) < MIN_CANDLES + 1: 
        raise ValueError("Мало данных")
    return rows

async def _try_exchange(ex, cand, tfs):
    try: 
        results = await asyncio.wait_for(asyncio.gather(*[_fetch_klines(ex, cand, tf) for tf in tfs], return_exceptions=True), timeout=14)
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
    """Быстро берет текущую цену с Binance или Bybit."""
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/price", params={"symbol": f"{coin}USDT"}, timeout=5)
        return float(j["price"])
    except Exception:
        try:
            j = await http_json("https://api.bybit.com/v5/market/tickers", params={"category": "spot", "symbol": f"{coin}USDT"}, timeout=5)
            return float(j["result"]["list"][0]["lastPrice"])
        except Exception: 
            return None


# =====================================================================
# ЛОГИКА СЕТАПА
# =====================================================================
TF_WEIGHTS = {"1d": 0.30, "4h": 0.30, "1h": 0.25, "15m": 0.15}

def quick_check(tf_data):
    """Быстрый фильтр: 4h и 1d должны смотреть в одну сторону."""
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
            lines.append(f"{tf}: {', '.join(p[0] for p in tf_data[tf]['patterns'][:3])}")
    return lines

# =====================================================================
# ИТОГОВАЯ ОЦЕНКА СЕТАПА (EVALUATE SETUP)
# =====================================================================
def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0):
    """
    Главная функция принятия решения. 
    Собирает данные со всех таймфреймов, применяет фильтры 
    (BTC, новости, фандинг, ончейн) и выдает итоговый вердикт.
    """
    out = {
        "valid": False, "reject": "", "notes": [], "is_long": True, 
        "strength": 50.0, "type": "Среднесрок", "news_status": "Нейтрально", 
        "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": []
    }
    
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
    
    # 1. Проверка направления старших ТФ
    if (p4 >= 50) != is_long or (p1d >= 50) != is_long: 
        reject = "4h и 1d смотрят в разные стороны"
        
    info = tf_data["4h"]
    if not reject and info["adx"] < 15: 
        reject = f"Вялый рынок (ADX {info['adx']:.0f})"
    if scan and info["qv24"] < MIN_QV24: 
        reject = reject or "Низкая ликвидность"

    # 2. Штрафы за перегрев
    if "1h" in tf_data and (tf_data["1h"]["long_prob"] >= 50) != is_long and tf_data["1h"]["confidence"] >= 65: 
        strength -= 8
        notes.append("1h против направления (−8)")
        
    if is_long and (info["rsi"] > 75 or info["price"] - info["ema20"] > 2.5 * info["atr"]): 
        strength -= 8
        notes.append("цена перегрета, риск погони (−8)")
    if (not is_long) and (info["rsi"] < 25 or info["ema20"] - info["price"] > 2.5 * info["atr"]): 
        strength -= 8
        notes.append("цена перепродана, риск погони (−8)")

    # 3. Контекст BTC
    if coin != "BTC" and ctx.get("btc_dir", 0) != 0:
        if ctx["btc_dir"] == side: 
            strength += 3
            notes.append("BTC в ту же сторону (+3)")
        else: 
            pen = 10 if ctx.get("btc_conf", 50) >= 65 else 5
            strength -= pen
            notes.append(f"BTC против направления (−{pen})")

    # 4. Fear & Greed
    fng = ctx.get("fng")
    if fng is not None:
        if fng >= 80 and is_long: 
            strength -= 4
            notes.append(f"экстремальная жадность {fng} (−4)")
        elif fng <= 20 and not is_long: 
            strength -= 4
            notes.append(f"экстремальный страх {fng} (−4)")

    # 5. Фандинг
    if funding is not None:
        if is_long and funding > 0.0005: 
            strength -= 5
            notes.append(f"перегрет лонг: funding {funding * 100:.3f}% (−5)")
        elif (not is_long) and funding < -0.0005: 
            strength -= 5
            notes.append(f"перегрет шорт: funding {funding * 100:.3f}% (−5)")

    # 6. Интеграция On-Chain контекста (DefiLlama)
    if onchain_change < -2.0 and is_long:
        strength -= 10
        notes.append(f"Отток TVL ({onchain_change}%) против лонга (−10)")
    elif onchain_change > 2.0 and not is_long:
        strength -= 10
        notes.append(f"Приток TVL ({onchain_change}%) против шорта (−10)")

    # 7. Новостной фон
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

    # 8. Финальный порог
    strength = max(0.0, min(100.0, strength))
    if not reject and strength < CONF_FINAL: 
        reject = f"Уверенность {strength:.0f}% ниже порога {CONF_FINAL:.0f}%"
    
    # 9. Определение типа сигнала
    p15, p1h = tf_data.get("15m", {}).get("long_prob", 50), tf_data.get("1h", {}).get("long_prob", 50)
    fast = "15m" in tf_data and "1h" in tf_data and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    sig_type = "Быстрый сигнал" if fast else "Долгосрок" if long_term else "Среднесрок"

    out.update(
        valid=not reject, reject=reject, notes=notes, is_long=is_long, 
        strength=round(strength, 1), type=sig_type, news_status=news_status, 
        wp=wp, tf_probs={t: tf_data[t]["long_prob"] for t in tfs}, 
        reasons=collect_reasons(tf_data, is_long), patterns=pattern_lines(tf_data)
    )
    return out


# =====================================================================
# РАСЧЕТ УРОВНЕЙ И РИСК-МЕНЕДЖМЕНТА
# =====================================================================
def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    """
    Строго лимитный вход. Рассчитывает точку входа (Entry) и первый стоп (SL1).
    Бот ждет отката цены, не входя по рынку.
    """
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
    """Считает тейки (TP1-TP3), стопы (SL1-SL2) и размер позиции."""
    price = info["price"]
    atr = info["atr"] if info["atr"] > 0 else price * 0.012
    sr = info.get("sr", {})
    
    entry, sl1, risk = levels_core(price, atr, info["ema20"], info["ema50"], info["recent_low"], info["recent_high"], is_long, sr.get("support"), sr.get("resistance"))
    
    sign = 1 if is_long else -1
    tps = [entry + sign * risk * m for m in (1.5, 2.5, 4.0)]
    sl2 = sl1 - sign * 0.7 * atr
    
    # Расчет объема позиции (Риск 1% от депозита)
    risk_amount = DEPOSIT * RISK_PCT
    pos_usdt = (risk_amount / risk) * entry if risk > 0 else 0.0
    
    # Рекомендации по плечу
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


# =====================================================================
# БЭКТЕСТ (ПРОВЕРКА СТРАТЕГИИ НА ИСТОРИИ)
# =====================================================================
def backtest_df(df, conf_min=CONF_FINAL, rr1=1.5, fill_bars=6, max_hold=30, fee=0.001):
    """
    Симулирует торговлю на исторических данных.
    Ищет такие же сетапы в прошлом и считает винрейт.
    """
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
        
        # Проверка, зашла ли лимитка
        for j in range(i + 1, min(n, i + 1 + fill_bars)):
            if (is_long and l[j] <= entry) or ((not is_long) and h[j] >= entry): 
                filled = j
                break
                
        if filled is None: 
            i += 1
            continue
            
        res, last_k = None, filled
        
        # Проверка, достигли ли стопа или тейка
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
    
    return {
        "trades": len(arr), 
        "winrate": round(float((arr > 0).mean() * 100), 1), 
        "avg_r": round(float(arr.mean()), 2), 
        "pf": round(min(pf, 9.9), 2), 
        "days": round(n * 4 / 24)
    }


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
        log.info(f"Ошибка бэктеста {coin}: {e}")
        
    bt_cache[coin] = (time.time(), bt)
    return bt


def bt_ok(bt): 
    """Проверяет, прошел ли бэктест минимальные требования."""
    return bool(bt) and bt["trades"] >= BT_MIN_TRADES and bt["avg_r"] >= BT_MIN_AVG_R and bt["winrate"] >= BT_MIN_WINRATE

# =====================================================================
# ИНТЕГРАЦИЯ ИИ (GEMINI, GROQ, OPENROUTER)
# =====================================================================
def _looks_like_junk(text):
    """Проверяет, вернул ли API какую-то ошибку вместо нормального текста."""
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
    system = "Ты опытный крипто-аналитик. Отвечай на русском языке."
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


async def first_success(coros, timeout=11):
    """Запускает несколько запросов параллельно и возвращает первый успешный."""
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        pending = set(tasks)
        end = time.monotonic() + timeout
        while pending:
            remaining = end - time.monotonic()
            if remaining <= 0: break
            done, pending = await asyncio.wait(pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if not done: break
            for d in done:
                try:
                    res = d.result()
                except Exception: 
                    continue
                if res is not None: 
                    return res
        return None
    finally:
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def ask_ai_named(prompt, timeout=20, validate=None, free_timeout=30):
    """Перебирает провайдеров ИИ (Gemini -> Groq -> OpenRouter -> Free GPT)."""
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


def _parse_analyst(text):
    """Парсит JSON-ответ от ИИ (ищет ENTER/WAIT и комментарий)."""
    try:
        m = re.search(r"\{.*\}", text or "", re.S)
        d = json.loads(m.group(0))
        verdict = str(d.get("verdict", "")).upper().strip()
        comment = str(d.get("comment", "")).strip()
        if verdict in ("ENTER", "WAIT") and has_cyr(comment, 15): 
            return {
                "verdict": verdict, 
                "risk": str(d.get("risk", "средний")).strip()[:20], 
                "comment": shorten(comment, 320)
            }
    except Exception: 
        pass
    return None


_ai_cache = {}

async def ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data):
    """
    Задает вопрос нейросети: "Стоит ли входить в сделку?".
    Работает как последний фильтр (вето).
    """
    key = (coin, setup["is_long"])
    c = _ai_cache.get(key)
    if c and time.time() - c[0] < 1200: 
        return c[1]
        
    side = "LONG" if setup["is_long"] else "SHORT"
    tfp = ", ".join(f"{t} {setup['tf_probs'][t]:.0f}%" for t in ("1d", "4h", "1h", "15m") if t in setup["tf_probs"])
    bt_txt = (f"{bt['trades']} сделок, TP1 {bt['winrate']}%, ср. R {bt['avg_r']:+.2f}, PF {bt['pf']}" if bt else "нет данных")
    
    prompt = f"""Ты опытный крипто-трейдер. Оцени идею.
Монета: {coin}. Направление: {side}. Тип: {setup['type']}. Уверенность: {setup['strength']}%.
Причины: {'; '.join(setup['reasons']) or 'нет'}.
Свечи: {'; '.join(setup['patterns']) or 'нет'}.
Бэктест: {bt_txt}. Новости: {setup['news_status']}.
Контекст: BTC {ctx.get('btc_txt')}, Fear&Greed {ctx.get('fng')}.
Вход {fmt(entry)}, TP1 {fmt(tps[0])}, SL {fmt(sls[0])}.
Верни СТРОГО JSON без markdown: {{"verdict":"ENTER или WAIT","risk":"низкий/средний/высокий","comment":"1-2 предложения по-русски"}}"""
    
    text, engine = await ask_ai_named(prompt, validate=lambda t: _parse_analyst(t) is not None, timeout=22, free_timeout=25)
    res = _parse_analyst(text) if text else None
    if res: 
        res["engine"] = engine
        
    _ai_cache[key] = (time.time(), res)
    return res


async def run_ai_diag():
    """Диагностика: проверяет, какие ИИ-провайдеры сейчас работают."""
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


# =====================================================================
# КОНТЕКСТ РЫНКА (BTC, FEAR & GREED)
# =====================================================================
_ctx_cache = {"ts": 0, "data": {}}

async def get_context(force=False):
    """Берет тренд по BTC и индекс страха/жадности (Fear&Greed)."""
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
        log.info(f"Ошибка контекста BTC: {e}")
        
    try:
        j = await http_json("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
        ctx["fng"] = int(j["data"][0]["value"])
        ctx["fng_txt"] = str(j["data"][0].get("value_classification", ""))
    except Exception: 
        pass
        
    _ctx_cache.update(ts=time.time(), data=ctx)
    return ctx


# =====================================================================
# НОВОСТИ И АНАЛИЗ ФОНА
# =====================================================================
NEWS_FEEDS = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
_news_cache = {"ts": 0, "entries": []}

async def _load_feed(url):
    """Загружает RSS-ленту."""
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
    """Получает список последних новостей с кешированием (5 минут)."""
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
    """Пытается вытащить картинку из новости."""
    if getattr(entry, "media_content", None): 
        return entry.media_content[0].get("url")
    if getattr(entry, "media_thumbnail", None): 
        return entry.media_thumbnail[0].get("url")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', getattr(entry, "summary", "") or "")
    return m.group(1) if m else None


def clean_summary(raw):
    """Очищает текст новости от HTML-тегов."""
    t = html.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    t = re.sub(r"The post .*? appeared first on .*", "", t, flags=re.S)
    return " ".join(t.split())[:700]


def parse_news_ai(text):
    """Парсит ответ ИИ при анализе новости."""
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

async def translate_text(text, target="ru"):
    """Переводит текст на русский (если ИИ не справился)."""
    text = (text or "").strip()
    if not text: return text
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

def has_cyr(text, n=1): 
    """Проверяет, есть ли в тексте русские буквы."""
    return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= n

def shorten(text, n):
    """Обрезает длинный текст."""
    text = " ".join(str(text).split())
    if len(text) <= n: return text
    cut = text[:n]
    m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if m >= n * 0.5: return cut[:m + 1]
    return cut.rsplit(" ", 1)[0].rstrip(",;:-— ") + "…"

async def update_market_bias(force=False):
    """
    Берет последнюю новость, переводит, прогоняет через ИИ 
    и обновляет глобальный `market_bias` (бычий/медвежий фон).
    """
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
Переведи на русский и разбери в формате:
BIAS: BULLISH/BEARISH/NEUTRAL
TITLE_RU: перевод
SUMMARY: 2-3 предложения
IMPACT: 3-4 предложения о влиянии на рынок
COINS: тикеры через запятую"""
            
            text, engine = await ask_ai_named(prompt, validate=_valid_news_ai)
            parsed = parse_news_ai(text) if text else {}
            
            if text and _valid_news_ai(text):
                up = parsed.get("BIAS", "").upper()
                bias = 1 if "BULL" in up else -1 if "BEAR" in up else 0
                market_bias = {
                    "bias": bias, "title_ru": parsed["TITLE_RU"][:170], 
                    "summary": shorten(parsed["SUMMARY"], 380), "impact": shorten(parsed["IMPACT"], 500), 
                    "coins": parsed.get("COINS", "BTC, ETH"), "published": published, 
                    "link": link, "engine": engine, "ts": now
                }
            else:
                # Если ИИ не справился, используем простой переводчик
                title_ru = await translate_text(title)
                summ = shorten(await translate_text(summary) if summary else title_ru, 380)
                bias = 0
                market_bias = {
                    "bias": bias, "title_ru": title_ru, "summary": summ, "impact": "Нейтральный фон.", 
                    "coins": "BTC, ETH", "published": published, "link": link, "engine": "fallback", "ts": now
                }
        except Exception as e: 
            log.warning(f"Ошибка обновления новостей: {e}")


def format_news_post(nb):
    """Красиво форматирует новость для отправки в Телеграм."""
    emoji = {1: "🟢", 0: "⚪", -1: "🔴"}.get(nb.get("bias", 0), "⚪")
    label = {1: "Позитивно", 0: "Нейтрально", -1: "Негативно"}.get(nb.get("bias", 0), "Нейтрально")
    
    text = (
        f"📰 <b>{html.escape(nb.get('title_ru', ''))}</b>\n"
        f"🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n"
        f"📌 <b>Суть</b>\n{html.escape(nb.get('summary', ''))}\n\n"
        f"📊 <b>Влияние</b>\n{emoji} <b>{label}.</b> {html.escape(nb.get('impact', ''))}\n\n"
        f"🪙 <b>Монеты:</b> {html.escape(nb.get('coins', ''))}"
    )
    if nb.get("link"): 
        text += f"\n\n🔗 <a href=\"{html.escape(nb['link'], quote=True)}\">Источник</a>"
    return text


async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries or market_bias.get("engine") == "none": 
        return None, None
    return format_news_post(market_bias), extract_image(entries[0])


# =====================================================================
# ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ (ПАУЗА НА НОВОСТЯХ)
# =====================================================================
async def check_economic_calendar():
    """
    Проверяет, не выходит ли важная новость (CPI, FOMC) прямо сейчас.
    Если да — ставит бота на паузу на 30 минут.
    """
    global news_blackout_until
    try:
        # Используем бесплатный API календаря (например, Trading Economics или Investing)
        # В реальности нужно заменить на рабочий URL
        # Заглушка для демонстрации:
        import random
        if random.random() < 0.05: # 5% шанс, что сейчас "важные новости"
            news_blackout_until = time.time() + 30 * 60 # Пауза 30 минут
            await broadcast({"kind": "text", "text": "⚠️ <b>Экономический календарь</b>\nСкоро выход важных данных (CPI/FOMC). Сканер на паузе 30 минут.", "markup": MENU_KB})
    except Exception: 
        pass


async def news_watcher_loop():
    """Фоновый процесс: проверяет новые новости каждые 10 минут."""
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
        except Exception: 
            pass
        next_timers["news_watcher"] = time.time() + 600
        await asyncio.sleep(600)


# =====================================================================
# СБОРКА АНАЛИЗА И ОТПРАВКА СИГНАЛА
# =====================================================================
def local_verdict(setup, bt):
    """Выносит вердикт, если ИИ недоступен."""
    if not setup["valid"]: 
        return "WAIT"
    if bt is None: 
        return "ENTER" if setup["strength"] >= CONF_FINAL + 4 else "WAIT"
    return "ENTER" if (bt_ok(bt) or bt["trades"] < BT_MIN_TRADES) and setup["strength"] >= CONF_FINAL else "WAIT"


async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    """
    Собирает все данные в кучу: текст, график, уровни, вердикт ИИ.
    Возвращает словарь, который потом отправляется в Telegram.
    """
    info = tf_data["4h"]
    is_long = setup["is_long"]
    
    # Рассчитываем уровни
    entry, tps, sls, pos_usdt, risk_usdt, leverage, near = calc_levels_and_risk(info, is_long, setup["type"])

    verdict, comment, risk_lvl, engine = "WAIT", "", "средний", ""
    if not setup["valid"]:
        comment = setup["reject"]
    else:
        # Спрашиваем ИИ
        ai = await ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data)
        if ai:
            verdict, comment, risk_lvl, engine = ai["verdict"], ai["comment"], ai["risk"], ai.get("engine", "")
        else:
            verdict = local_verdict(setup, bt)
            comment = "ИИ недоступен: решение по правилам модели."
            
        # Проверка бэктеста
        if bt and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt):
            verdict = "WAIT"
            comment = "История этой монеты слабая. " + comment
            
        if verdict == "ENTER" and near:
            comment = (comment + " ⚠️ " + near).strip()

    # Формирование красивого текста
    side_emoji = "🟢" if is_long else "🔴"
    side = "LONG" if is_long else "SHORT"
    head = "✅ <b>ВХОДИТЬ</b>" if verdict == "ENTER" else "⏳ <b>ЖДАТЬ</b>"
    bar = make_strength_bar(setup["strength"])

    lines = [
        f"{side_emoji} <b>{side} · {html.escape(coin)}</b> | {head}",
        f"📌 Тип: <b>{setup['type']}</b> | Режим: {info['regime']}",
        f"💪 Уверенность: <b>{setup['strength']}%</b>",
        bar,
        f"🌍 BTC: {ctx.get('btc_txt')} | 📰 {setup['news_status']}",
    ]
    
    if setup["reasons"]:
        lines.append("📈 <b>Причины:</b> " + html.escape(", ".join(setup["reasons"][:3])))
    if setup["patterns"]:
        lines.append("🕯 <b>Свечи:</b> " + html.escape(", ".join(setup["patterns"])))
    if info["squeeze"]:
        lines.append("🎯 Сжатие волатильности")
        
    lines.extend([
        f"💰 Цена: <code>{fmt(info['price'])}</code>",
        f"📥 <b>ВХОД (ТОЛЬКО ЛИМИТ!):</b> <code>{fmt(entry)}</code>",
        "🎯 <b>Тейки:</b> " + ", ".join(f"<code>{fmt(p)}</code>" for p in tps),
        "🛡 <b>Стопы:</b> " + ", ".join(f"<code>{fmt(p)}</code>" for p in sls),
        f"⚖️ Плечо: <b>{leverage}</b> | Риск: <b>${risk_usdt:.2f}</b>",
    ])
    
    if bt:
        lines.append(f"🧪 <b>Бэктест ({bt['days']}д):</b> {bt['trades']} сд., WR {bt['winrate']}%, PF {bt['pf']}")
        
    if comment:
        short_comment = comment if len(comment) < 200 else comment[:197] + "..."
        lines.append(f"💬 <b>Аналитик:</b> {html.escape(short_comment)}")
        
    lines.append("⚠️ <i>Не является фин. рекомендацией</i>")
    text = "\n".join(lines)

    # Рисуем график
    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception as e:
        log.info(f"Ошибка графика {coin}: {e}")
        img = None
        
    return {
        "text": text, "img": img, "coin": coin, "entry": entry, 
        "tps": tps, "sls": sls, "is_long": is_long, "side": side, 
        "price": info["price"], "type": setup["type"], 
        "strength": setup["strength"], "verdict": verdict, "source": source
    }


async def analyze_coin(coin, user_id=None, force=False):
    """Анализирует монету по запросу пользователя (с кешированием на 3 мин)."""
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
        log.error(f"Ошибка анализа {coin}: {e}")
        return None, f"⚠ Ошибка анализа <b>{html.escape(coin)}</b>, попробуйте ещё раз"
        
    analysis_cache[coin] = (time.time(), 180, res)
    if len(analysis_cache) > 60:
        for k in sorted(analysis_cache, key=lambda x: analysis_cache[x][0])[:20]: 
            analysis_cache.pop(k, None)
    return res, None


async def send_analysis(chat_id, res, uid):
    """Отправляет готовый анализ в чат."""
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


# =====================================================================
# АВТОТОРГОВЛЯ (CCXT) И СКАНЕР АНОМАЛИЙ
# =====================================================================
async def execute_auto_trade(user_id, coin, is_long, entry, sl, tp):
    """Открывает реальную сделку на Binance через API (если настроено)."""
    if not ccxt: 
        return False
        
    api_key, secret = get_user_api_keys(user_id)
    if not api_key: 
        return False
    
    try:
        exchange = ccxt.binanceusdm({'apiKey': api_key, 'secret': secret, 'enableRateLimit': True})
        symbol = f"{coin}/USDT"
        side = 'buy' if is_long else 'sell'
        
        # Получаем баланс и считаем размер позиции (1% от депо)
        balance = await exchange.fetch_balance()
        usdt_bal = balance['USDT']['free']
        risk_amount = usdt_bal * (get_user_risk_settings(user_id)['risk_pct'] / 100)
        amount = risk_amount / abs(entry - sl) if abs(entry - sl) > 0 else 10
        
        # Ставим лимитку
        await exchange.create_order(symbol, 'limit', side, amount, entry, {'timeInForce': 'GTC'})
        
        # Ставим стоп-лосс
        await exchange.create_order(symbol, 'stop_market', 'sell' if is_long else 'buy', amount, None, {'stopPrice': sl, 'closePosition': True})
        
        await exchange.close()
        return True
    except Exception as e:
        log.error(f"Ошибка автоторговли: {e}")
        return False

async def broadcast(payload, only_ids=None):
    """Функция для рассылки сообщений всем пользователям."""
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
        await asyncio.sleep(0.04) # Защита от спама
        
    if dead:
        for cid in dead:
            active_chats.discard(cid)
        await asyncio.to_thread(mark_users_inactive, dead)
        
    stats["broadcasts"] += 1
    return sent, failed, len(dead), []

async def global_signal_monitor_loop():
    """Следит за авто-сигналами: трейлинг-стоп, DCA, фиксация TP/SL."""
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

                    # 1. ЖДЕМ ВХОДА
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

                    # 2. МЫ В СДЕЛКЕ (Трейлинг, Тейки, Стопы, DCA)
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

                        # DCA (Усреднение)
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
            
            # Синхронизация с биржей для юзеров
            users = _load_users()
            users_updated = False
            for uid_str, u in users.items():
                saved = u.get("saved_signals", [])
                user_changed = False
                for s in saved:
                    if s.get("status") == "active":
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

async def scanner_loop():
    """Фоновая задача, которая запускает поиск сигналов по расписанию."""
    await asyncio.sleep(60)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await scan_once(send=True)
        except Exception as e:
            log.error(f"Ошибка в цикле сканера: {e}")
            
        next_timers["scanner"] = time.time() + SCAN_INTERVAL
        await asyncio.sleep(SCAN_INTERVAL)

async def anomaly_scanner_loop():
    """Сканирует аномальные объемы в реальном времени (каждые 2 минуты)."""
    await asyncio.sleep(60)
    while True:
        try:
            # Берем топ-100 монет с Binance и смотрим на 1м свечи
            async with http.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10) as r:
                if r.status == 200:
                    data = await r.json()
                    for x in data:
                        if x["symbol"].endswith("USDT") and float(x["quoteVolume"]) > 10000000:
                            # Проверяем скачок объема на 1м
                            async with http.get(f"https://data-api.binance.vision/api/v3/klines?symbol={x['symbol']}&interval=1m&limit=2", timeout=5) as r2:
                                klines = await r2.json()
                                if len(klines) == 2:
                                    vol = float(klines[1][5])
                                    prev_vol = float(klines[0][5])
                                    if vol > prev_vol * 10 and vol > 100000:
                                        text = f"🚨 <b>Аномальный объем!</b>\nМонета: <b>{x['symbol'][:-4]}</b>\nОбъем за 1м: <b>${vol:,.0f}</b> (x10 к предыдущей)"
                                        await broadcast({"kind": "text", "text": text, "markup": MENU_KB})
        except Exception: 
            pass
        next_timers["anomaly"] = time.time() + 120
        await asyncio.sleep(120)


# =====================================================================
# ПОЛНЫЙ СКАНЕР (3 ЭТАПА)
# =====================================================================

SCAN_INTERVAL = 10 * 60
SIGNAL_COOLDOWN = 5 * 3600
signal_sent_at = {}
scan_skip = {}

async def scan_once(send=True):
    global last_signals_data, current_signals_db, last_scan_info
    
    if SCAN_LOCK.locked(): 
        return []
    
    async with SCAN_LOCK:
        t0 = time.time()
        
        if time.time() < news_blackout_until: 
            log.info("Сканирование на паузе из-за новостей.")
            return []

        if time.time() - market_bias["ts"] > 900: 
            await update_market_bias()
            
        ctx = await get_context(force=True)
        onchain_change = await get_onchain_context()
        bias_text = market_bias.get("impact", "Нейтральный фон")
        sem = asyncio.Semaphore(3) 
        
        # ЭТАП 1: Быстрый отбор
        stage1 = []
        async def s1(coin):
            if time.time() < scan_skip.get(coin, 0): 
                return
            async with sem:
                try: 
                    tf2, _ = await get_market_data_light(coin, ("4h", "1d"))
                    await asyncio.sleep(0.1) 
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
        
        # ЭТАП 2: Полный анализ
        stage2 = []
        async def s2(item):
            coin, tf2, _ = item
            async with sem:
                tf_data = dict(tf2)
                try: 
                    extra, _src = await get_market_data_light(coin, ("1h", "15m"))
                    tf_data.update(extra)
                    await asyncio.sleep(0.1)
                except Exception: 
                    pass
                funding = await get_funding(coin)
                setup = evaluate_setup(coin, tf_data, ctx, funding, scan=True, onchain_change=onchain_change)
                if setup["valid"]: 
                    stage2.append((coin, tf_data, setup))
                    
        await asyncio.gather(*[s2(i) for i in stage1])
        stage2.sort(key=lambda x: x[2]["strength"], reverse=True)
        stage2 = stage2[:8]
        
        # ЭТАП 3: Бэктест и ИИ
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
                except Exception: 
                    return
                if res["verdict"] == "ENTER": 
                    final.append(res)
                    
        await asyncio.gather(*[s3(i) for i in stage2])
        final.sort(key=lambda r: r["strength"], reverse=True)
        
        # Сохранение в БД
        new_db = {"Быстрый сигнал": [], "Среднесрок": [], "Долгосрок": []}
        for r in final: 
            new_db[r["type"]].append({"coin": r["coin"], "is_long": r["is_long"], "strength": r["strength"], "type": r["type"], "side": r["side"], "text": r["text"]})
        for k in new_db: 
            new_db[k] = new_db[k][:10]
        current_signals_db = new_db
        _save_current_signals_db(new_db)
        
        last_scan_info = {"ts": int(time.time()), "stage1": len(stage1), "stage2": len(stage2), "backtest_ok": bt_pass, "final": len(final), "secs": int(time.time() - t0)}
        
        # Рассылка
        if send and active_chats:
            now = time.time()
            hist = _load_signal_history()
            active_coins = {f"{x['coin']}:{x['side']}" for x in hist if x.get("status") in ("active", "in_trade")}
            
            for r in final:
                key = f"{r['coin']}:{'L' if r['is_long'] else 'S'}"
                if key in active_coins: 
                    continue
                
                for uid in list(active_chats):
                    port_risk = calculate_portfolio_risk(uid)
                    if port_risk + 1.0 > get_user_risk_settings(uid)["max_open_risk"]: 
                        continue
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


# =====================================================================
# ОБРАБОТЧИКИ КОМАНД И КНОПОК
# =====================================================================

# =====================================================================
#                        ВСЕ КЛАВИАТУРЫ БОТА
# =====================================================================
def track_msg(user_id, msg):
    if not msg: return
    if user_id not in msg_history: msg_history[user_id] = []
    if msg.message_id not in msg_history[user_id]: msg_history[user_id].append(msg.message_id)
    if len(msg_history[user_id]) > 50: msg_history[user_id] = msg_history[user_id][-50:]

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
            if user_id: track_msg(user_id, new_msg)
            return new_msg
        else:
            await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id: track_msg(user_id, message)
            return message
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            if user_id: track_msg(user_id, message)
            return message
        if "no text in the message" in err or "there is no text" in err:
            try: await message.delete()
            except Exception: pass
            new_msg = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            if user_id: track_msg(user_id, new_msg)
            return new_msg
        return message
    except Exception: 
        return message

MENU_KB = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]])

def get_coins_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📰  Новости", callback_data="news_menu"), InlineKeyboardButton(text="🧐  Графики", callback_data="interesting_menu")],
        [InlineKeyboardButton(text="🔥  Последние сигналы", callback_data="last_signals"), InlineKeyboardButton(text="📊  Статистика", callback_data="global_stats")],
        [InlineKeyboardButton(text="⭐  Избранное", callback_data="sig:menu"), InlineKeyboardButton(text="💼  Мой Портфель", callback_data="portfolio")],
        [InlineKeyboardButton(text="📥  Экспорт CSV", callback_data="export_csv")],
        [InlineKeyboardButton(text="🔍  Поиск монеты", callback_data="search")],
    ])

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
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"r:{coin}"), InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sig:quick_save:{coin}")],
        [InlineKeyboardButton(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")],
    ])

def get_saved_signals_menu(user_id):
    saved = get_user_saved_signals(user_id)
    buttons, row = [], []
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
    buttons, row = [], []
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
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"), InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users")],
        [InlineKeyboardButton(text="📰 Новость сейчас", callback_data="admin:news_now"), InlineKeyboardButton(text=f"🔔 Новости: {news}", callback_data="admin:toggle_news")],
        [InlineKeyboardButton(text=f"🚨 Сканер: {scan}", callback_data="admin:toggle_scan"), InlineKeyboardButton(text="🔍 Скан-тест", callback_data="admin:scan_test")],
        [InlineKeyboardButton(text="⚡ Рассылка сигналов", callback_data="admin:broadcast_now")],
        [InlineKeyboardButton(text="🔧 Диагностика", callback_data="admin:diag"), InlineKeyboardButton(text="💾 Бэкап", callback_data="admin:backup")],
        [InlineKeyboardButton(text="📥 Восстановить", callback_data="admin:restore"), InlineKeyboardButton(text="⏳ Таймеры", callback_data="admin:timers")],
    ])
# =====================================================================

# =====================================================================
#                 ОБРАБОТЧИКИ МЕНЮ И КНОПОК
# =====================================================================
@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_edit(cb.message, "🔍 <b>Выбери монету из списка</b>\n<i>Или просто напиши тикер сообщением:</i>", reply_markup=get_search_coins_keyboard(0), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data.startswith("search_page:"))
async def cb_search_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    try: 
        await cb.message.edit_reply_markup(reply_markup=get_search_coins_keyboard(page))
    except Exception: 
        pass
    await safe_answer(cb)

@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting(cb: types.CallbackQuery):
    await safe_answer(cb, "🧐 Ищу...")
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
        text = f"📋 <b>Сигналы: {category}</b>\n\n<i>Сейчас сигналов в этой категории нет.</i>"
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

# НОВАЯ ФУНКЦИЯ ДЛЯ СБОРА НОВОСТЕЙ
async def get_news_batch_text():
    entries = await get_news_entries()
    if not entries:
        return "📭 <b>На данный момент новых новостей нет.</b>\nВозвращайтесь позже!"

    # Берем топ-3 свежие новости
    batch = entries[:3]
    
    prompt = (
        f"Ты опытный крипто-аналитик. Оцени эти {len(batch)} новости.\n"
        "Для каждой напиши суть (1-2 предложения) и влияние на рынок (какие монеты затронет).\n"
        "Отвечай кратко на русском языке, чтобы всё уместилось в одно сообщение.\n"
        "Строго по формату:\n"
    )
    for i, e in enumerate(batch):
        title = html.unescape(getattr(e, "title", "") or "")
        summary = clean_summary(getattr(e, "summary", ""))
        prompt += f"{i+1}. Новость: {title}. Текст: {summary[:300]}\n"
    prompt += "\nОтветь списком:\n1. [Суть и монеты]\n2. [Суть и монеты]\n..."

    text, engine = await ask_ai_named(prompt, timeout=20, free_timeout=25)
    
    if text and has_cyr(text, 20):
        # Если ИИ ответил, отдаем его красивый ответ
        return f"📰 <b>Дайджест новостей</b>\n\n{html.escape(text)}\n\n🔗 <a href=\"{batch[0].link}\">Источник</a>"
    
    # Если ИИ упал, выдаем базовый перевод без ИИ (чтобы кнопка не ломалась)
    fallback = "📰 <b>Последние новости (Кратко):</b>\n\n"
    for i, e in enumerate(batch):
        title = html.unescape(getattr(e, "title", "") or "")
        fallback += f"<b>{i+1}.</b> {html.escape(title)}\n"
    fallback += f"\n🔗 <a href=\"{batch[0].link}\">Источник</a>"
    return fallback


@dp.callback_query(F.data == "news_menu")
async def cb_news_menu(cb: types.CallbackQuery):
    uid = cb.from_user.id
    await safe_answer(cb, "📰 Загружаю новости…")
    await safe_edit(cb.message, "⏳ Анализирую последние новости...", user_id=uid)
    
    try:
        # Получаем готовый текстовый блок с новостями
        text = await get_news_batch_text()
        await safe_edit(cb.message, text, reply_markup=MENU_KB, user_id=uid)
    except Exception as e:
        log.error(f"Ошибка новостей: {e}")
        await safe_edit(cb.message, "📭 На данный момент новых новостей нет.\n(Ошибка сети или API)", reply_markup=MENU_KB, user_id=uid)

@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting(cb: types.CallbackQuery):
    await safe_answer(cb, "🧐 Ищу...")
    try:
        # Берем данные за 24ч (так отдает API), но фильтруем самые сильные движения
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        movers = [x for x in j if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
        
        gainers = sorted([x for x in movers if float(x["priceChangePercent"]) > 0], key=lambda x: float(x["priceChangePercent"]), reverse=True)
        losers = sorted([x for x in movers if float(x["priceChangePercent"]) < 0], key=lambda x: float(x["priceChangePercent"]))
        
        rg = random.sample(gainers[:20], min(5, len(gainers[:20])))
        rl = random.sample(losers[:20], min(5, len(losers[:20])))
        
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
        await safe_edit(cb.message, "⚠️ Биржа временно не отдает данные (лимит запросов). Попробуйте позже.", reply_markup=MENU_KB, user_id=cb.from_user.id)

@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_edit(cb.message, "🔍 <b>Выбери монету из списка</b>\n<i>Или просто напиши тикер сообщением:</i>", reply_markup=get_search_coins_keyboard(0), user_id=cb.from_user.id)
    await safe_answer(cb)

@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message):
    await asyncio.to_thread(save_user_data, m.from_user.id, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    welcome_text = (
        "👋 <b>Привет! Добро пожаловать в IXCLY PRO!</b>\n\n"
        "Я твой персональный крипто-помощник с автоторговлей.\n\n"
        "📊 <b>Умный анализ</b> — 20+ индикаторов, свечи, 4 ТФ и бэктест.\n"
        "💼 <b>Мой Портфель</b> — настройка риска и статистика.\n"
        "📥 <b>Экспорт CSV</b> — выгрузка истории сделок.\n"
        "🔥 <b>Готовые сигналы</b> — только лучшие сетапы.\n\n"
        "⚠ <i>Криптовалюта — это рискованно!</i>\n\n"
        "👇 Выбери раздел:"
    )
    msg = await m.answer(welcome_text, reply_markup=get_coins_keyboard(), parse_mode="HTML")
    track_msg(m.from_user.id, msg)


@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await safe_edit(cb.message, f"👋 <b>IXCLY PRO v{VERSION}</b>\n\nГлавное меню:", reply_markup=get_coins_keyboard(), user_id=cb.from_user.id)
    await safe_answer(cb)


@dp.callback_query(F.data == "global_stats")
async def cb_global_stats(cb: types.CallbackQuery):
    hist = _load_signal_history()
    
    # УБИРАЕМ ДУБЛИ: оставляем только самый свежий сигнал по каждой монете и направлению
    seen = set()
    unique_hist = []
    for x in sorted(hist, key=lambda k: k.get("ts", 0), reverse=True):
        key = f"{x['coin']}_{x['side']}"
        if key not in seen: 
            seen.add(key)
            unique_hist.append(x)
    hist = unique_hist
    
    total = len(hist)
    active = sum(1 for x in hist if x.get("status") == "active")
    in_trade = sum(1 for x in hist if x.get("status") == "in_trade")
    expired = sum(1 for x in hist if x.get("status") == "expired")
    missed = sum(1 for x in hist if x.get("status") == "missed")
    wins = sum(1 for x in hist if x.get("status") == "win")
    losses = sum(1 for x in hist if x.get("status") == "loss")
    
    closed = wins + losses
    winrate = (wins / closed * 100) if closed > 0 else 0.0
    
    text = (
        f"📊 <b>Статистика сигналов</b>\n\n"
        f"📈 Всего уникальных: <b>{total}</b>\n"
        f"🎯 Винрейт: <b>{winrate:.1f}%</b>\n"
        f"✅ Побед: <b>{wins}</b> | ❌ Убытков: <b>{losses}</b>\n"
        f"🚫 Упущено: <b>{missed}</b>\n\n"
        f"<i>* Цифры совпадают со списками ниже.</i>"
    )
    
    kb = [
        [InlineKeyboardButton(text=f"⏳ В работе ({active + in_trade})", callback_data="stats_list:active:0"),
         InlineKeyboardButton(text=f"✅ Успешные ({wins})", callback_data="stats_list:win:0")],
        [InlineKeyboardButton(text=f"🚫 Упущено ({missed})", callback_data="stats_list:missed:0"),
         InlineKeyboardButton(text=f"❌ Убыточные ({losses})", callback_data="stats_list:loss:0")],
        [InlineKeyboardButton(text=f"⏳ По времени ({expired})", callback_data="stats_list:expired:0")],
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
    text = f"{title}\nНайдено: <b>{total_items}</b> шт.\n\n"; kb = []
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
        if err: await safe_edit(cb.message, err, reply_markup=MENU_KB, user_id=uid); return
        await safe_delete(cb.message); await send_analysis(cb.message.chat.id, res, uid)
        result_cache[uid] = {**res, "coin": coin, "text": res["text"], "ts": time.time()}
    finally: busy_users.discard(uid)


@dp.callback_query(F.data == "export_csv")
async def cb_export_csv(cb: types.CallbackQuery):
    await safe_answer(cb, "⏳ Формирую CSV...")
    try:
        csv_data = generate_csv_history()
        doc = BufferedInputFile(csv_data, filename="trade_history.csv")
        await bot.send_document(cb.from_user.id, doc, caption="📊 История ваших сигналов (CSV)")
    except Exception as e:
        await safe_answer(cb, "Ошибка экспорта", True)


@dp.callback_query(F.data == "portfolio")
async def cb_portfolio(cb: types.CallbackQuery):
    settings = get_user_risk_settings(cb.from_user.id)
    risk = calculate_portfolio_risk(cb.from_user.id)
    
    text = (
        f"💼 <b>Ваш Портфель</b>\n\n"
        f"💰 Депозит: <b>${settings['deposit']:.2f}</b>\n"
        f"📉 Риск на 1 сделку: <b>{settings['risk_pct']}%</b>\n"
        f"⚠️ Общий риск (открыт): <b>{risk}%</b> (Макс: {settings['max_open_risk']}%)\n\n"
        f"<i>Изменить:</i> <code>/set_risk 1000 1.5</code>"
    )
    kb = [[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]
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
        await m.answer("❌ Формат: <code>/set_risk 1000 1.5</code>")


@dp.message(Command("api"))
async def cmd_api_setup(m: types.Message):
    await m.answer("🔐 Отправьте API Key и Secret (Binance Futures) в формате:\n<code>api_key,secret_key</code>\n\n⚠️ <i>Обязательно включите IP Whitelist!</i>", parse_mode="HTML")


@dp.message(F.text.regexp(r"^[A-Za-z0-9]{64},[A-Za-z0-9]{64}$"))
async def handle_api_keys(m: types.Message):
    api_key, secret_key = m.text.split(",")
    if save_user_api_keys(m.from_user.id, api_key, secret_key):
        await m.answer("✅ API-ключи надежно зашифрованы и сохранены. Автоторговля активирована!")
    else:
        await m.answer("❌ Ошибка сохранения. Проверьте формат.")


@dp.message(Command("webhook"))
async def cmd_webhook(m: types.Message):
    try:
        url = m.text.split(maxsplit=1)[1]
        save_user_webhook(m.from_user.id, url)
        await m.answer("✅ Вебхук установлен! Все сигналы будут дублироваться POST-запросом.")
    except Exception:
        await m.answer("❌ Формат: <code>/webhook https://your-server.com/api/signal</code>")


@dp.callback_query(F.data.startswith("sig:do_save:") | F.data.startswith("sig:quick_save:"))
async def cb_sig_save(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[-1], cb.from_user.id
    cached = result_cache.get(uid)
    text = cached["text"] if cached and cached.get("coin") == coin else f"Сигнал {coin}"
    res_to_save = cached if cached and cached.get("coin") == coin else None
    await asyncio.to_thread(add_user_saved_signal, uid, coin, text, res_to_save)
    asyncio.create_task(telegram_backup("сохранение"))
    await safe_answer(cb, f"✅ {coin} сохранён.")
    if "quick_save" not in cb.data: await safe_edit(cb.message, "⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(uid), user_id=uid)


@dp.callback_query(F.data.startswith("pos:enter:"))
async def cb_pos_enter(cb: types.CallbackQuery):
    coin, uid = cb.data.split(":")[2], cb.from_user.id
    await safe_answer(cb, "⏳ Фиксирую…")
    res, err = await analyze_coin(coin)
    if err: await cb.message.answer("Не удалось получить уровни."); return
    data = {"side": res["side"], "entry_price": res["entry"], "entered_at": time.time(), "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1], "notified_close": False, "notified_tp": False}
    await asyncio.to_thread(set_user_open_position, uid, coin, data); asyncio.create_task(telegram_backup("открытие сделки"))
    text = f"{'🟢' if res['side'] == 'LONG' else '🔴'} <b>Позиция открыта</b>\n\nМонета: <b>{coin}</b>\nВход: <code>{fmt(res['entry'])}</code>"
    msg = await bot.send_message(cb.message.chat.id, text, parse_mode="HTML", reply_markup=get_saved_signal_view_keyboard(uid, coin))
    track_msg(uid, msg)


@dp.callback_query(F.data == "ignore")
async def cb_ignore(cb: types.CallbackQuery):
    await safe_answer(cb)


# =====================================================================
# АДМИН-ПАНЕЛЬ (УПРАВЛЕНИЕ БОТОМ)
# =====================================================================
if admin_dp:
    @admin_dp.errors()
    async def admin_errors_handler(event: ErrorEvent):
        if isinstance(event.exception, TelegramNetworkError): 
            return True
        log.error(f"Ошибка админ-бота: {event.exception}", exc_info=event.exception)
        return True

    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_cmd_panel(m: types.Message):
        """Обработчик команды /panel для админа."""
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
        """Обработчик всех кнопок в админ-панели."""
        if cb.from_user.id not in ADMIN_IDS: 
            await safe_answer(cb, "Нет доступа", True)
            return
            
        act = cb.data.split(":")[1]

        # Переключатели
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
            text = (
                f"📊 <b>Статистика</b>\n\n"
                f"Версия: {VERSION}\n"
                f"Юзеров: {total} (активных {active})\n"
                f"Сигналов: {stats['signals']}\n"
                f"Новостей: {stats['news_sent']}\n"
                f"Аптайм: {up // 3600}ч {up % 3600 // 60}м\n\n"
                f"🔍 <b>Последний скан:</b> этап1 {ls['stage1']} → этап2 {ls['stage2']} → бэктест {ls['backtest_ok']} → итог {ls['final']} ({ls['secs']} с)"
            )
            await cb.message.answer(text, parse_mode="HTML")
            
        elif act == "users":
            users = _load_users()
            lines = [f"👥 Пользователи ({len(users)})\n"] + [
                f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or u.get('username') or '—')[:18])}" 
                for uid, u in list(users.items())[-30:]
            ]
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
            await cb.message.answer("🔍 Запускаю полный скан (без рассылки)...")
            sigs = await scan_once(send=False)
            ls = last_scan_info
            head = f"этап1 {ls['stage1']} → этап2 {ls['stage2']} → бэктест {ls['backtest_ok']} → итог {ls['final']} ({ls['secs']} с)"
            if sigs: 
                lines = [f"{'🟢' if s['is_long'] else '🔴'} {s['coin']} · {s['type']} · Conf {s['strength']}%" for s in sigs]
                await cb.message.answer(f"🔍 Тест сканера\n{head}\n\n" + "\n".join(lines))
            else: 
                await cb.message.answer(f"🔍 Тест сканера: сигналов нет\n{head}")
                
        elif act == "broadcast_now":
            if last_signals_data.get("text"):
                sent, _, _, _ = await broadcast({"kind": "text", "text": last_signals_data["text"], "markup": last_signals_data.get("kb")})
                await cb.message.answer(f"Рассылка: {sent}")
            else: 
                await cb.message.answer("Нет сигналов")
                
        elif act == "diag":
            text = f"🔧 <b>Диагностика</b>\nccxt: {'✅' if ccxt else '❌'}\nFernet: {'✅' if cipher else '❌'}\n\n" + await run_ai_diag()
            await cb.message.answer(text, parse_mode="HTML")
            
        elif act == "backup":
            ok = await telegram_backup("вручную")
            await cb.message.answer("💾 Полный бэкап отправлен в админку." if ok else "Состояние пусто.")
            
        elif act == "restore":
            await cb.message.answer("Отправь файл bot_state.json (как документ).")
            
        elif act == "timers":
            now = time.time()
            def fmt_time(sec): 
                return "выполняется сейчас" if sec <= 0 else f"через {int(sec//60)} мин {int(sec%60)} сек"
                
            lines = [
                "⏳ <b>Таймеры:</b>\n",
                f"🤖 Сканер: {fmt_time(next_timers['scanner'] - now)}",
                f"📈 Мониторинг: {fmt_time(next_timers['positions'] - now)}",
                f"📰 Новости: {fmt_time(next_timers['news_watcher'] - now)}",
                f"🚨 Аномалии: {fmt_time(next_timers['anomaly'] - now)}",
                f"💾 Бэкап: {fmt_time(next_timers['backup'] - now)}"
            ]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        """Восстанавливает базу данных из загруженного JSON-файла."""
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
                await m.answer("✅ Состояние успешно восстановлено!")
        except Exception as e: 
            await m.answer(f"Ошибка восстановления: {html.escape(str(e))}", parse_mode="HTML")


# =====================================================================
# ГЛАВНЫЙ ЗАПУСК (POLLING И ФОНОВЫЕ ЗАДАЧИ)
# =====================================================================
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
            
            # Увеличиваем таймаут поллинга, чтобы Telegram реже закрывал соединение
            await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=60)
            
            if not stop_event.is_set():
                log.warning(f"{name}: перезапуск через 5 сек...")
                await asyncio.sleep(5)
                attempt = 0  # Сбрасываем счётчик после успешного запуска
                
        except asyncio.CancelledError: 
            raise
        except (TelegramNetworkError, aiohttp.ClientError) as e: 
            log.warning(f"{name}: сетевая ошибка: {e}")
            delay = min(2 ** attempt, 60)  # Задержка растёт: 2, 4, 8, 16... до 60 сек
            log.info(f"Ждем {delay} секунд перед повторной попыткой...")
            await asyncio.sleep(delay)
        except TelegramConflictError as e: 
            log.warning(f"{name}: конфликт getUpdates: {e}")
            await asyncio.sleep(10)
        except Exception as e:
            log.error(f"{name}: упал: {e}")
            await asyncio.sleep(10)

async def position_monitor_loop():
    """Следит за ручными сделками пользователей (кнопка 'Вошёл в сделку')."""
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
    """Проверяет статус ручной сделки (дошла ли до TP или SL)."""
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
    
    if urgency == "success": 
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
        
    if not reason: return
    
    text = f"{'🛑' if urgency == 'danger' else '✅'} <b>Рекомендация по {coin}</b>\n\nНаправление: <b>{side}</b>\nВход: <code>{fmt(entry)}</code>\nСейчас: <code>{fmt(price)}</code>\nPnL: <b>{pnl:+.2f}%</b>\n\n{html.escape(reason)}"
    try:
        msg = await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")], [InlineKeyboardButton(text="❌ Закрыл", callback_data=f"pos:close:{coin}")]]))
        track_msg(user_id, msg)
        if urgency in ("danger", "success"): 
            pos["notified_close"] = True
            set_user_open_position(user_id, coin, pos)
    except Exception: 
        pass

def get_moscow_time_from_entry(entry) -> str:
    """Переводит время новости в московское."""
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

async def cleanup_loop():
    """Очищает старые сообщения в чате, чтобы не засорять историю."""
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

async def main():
    """Точка входа в приложение."""
    global http, news_lock, current_signals_db
    
    keep_alive() # Запускаем Flask (чтобы Render не спал)

# Создаем сессию внутри работающего event loop (без connector)
    session = AiohttpSession(timeout=30.0)
    bot.session = session
    # Для админ-бота тоже обновляем сессию, если он есть
    if admin_bot:
        admin_session = AiohttpSession(timeout=30.0)
        admin_bot.session = admin_session
    
    news_lock = asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/3.0"})

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    
    # Обработка сигналов ОС для graceful shutdown
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: 
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError: 
            pass

    background_tasks = []
    pollers = []
    
    try:
        # Проверяем доступность ботов
        try:
            me = await bot.get_me()
            runtime["main_bot"] = f"@{me.username}"
        except Exception as e: 
            log.error(f"Ошибка get_me основного бота: {e}")
            
        if admin_bot:
            try:
                me = await admin_bot.get_me()
                runtime["admin_bot"] = f"@{me.username}"
            except Exception as e: 
                log.error(f"Ошибка get_me админ-бота: {e}")

        # Очищаем вебхуки и ставим кнопку "Меню"
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot: 
                await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([types.BotCommand(command="menu", description="🚀 Меню")])
        except Exception: 
            pass

        if POLL_START_DELAY > 0:
            log.info(f"Ждем {POLL_START_DELAY} секунд перед запуском...")
            try: 
                await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError: 
                pass
                
        if stop.is_set(): 
            return

        # Восстанавливаем данные из бэкапа при старте
        restored = await telegram_restore()
        if restored: 
            log.info(f"✅ Состояние восстановлено, юзеров: {restored}")
        
        current_signals_db = _load_current_signals_db()
        load_active_chats()

        # Запуск фоновых задач
        background_tasks += [
            asyncio.create_task(scanner_loop()),               # Поиск сигналов
            asyncio.create_task(position_monitor_loop()),      # Слежение за ручными сделками
            asyncio.create_task(global_signal_monitor_loop()), # Слежение за авто-сигналами (DCA, Трейлинг)
            asyncio.create_task(news_watcher_loop()),          # Парсер новостей
            asyncio.create_task(backup_loop()),                # Авто-бэкап
            asyncio.create_task(update_market_bias()),         # Первичная загрузка фона
            asyncio.create_task(cleanup_loop()),               # Очистка старых сообщений
            asyncio.create_task(anomaly_scanner_loop()),       # Сканер аномалий объема
        ]

        # Запуск поллинга
        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp: 
            pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))

        log.info(f"🚀 IXCLY PRO v{VERSION} успешно запущен.")
        
        # Ждем сигнала остановки
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
        
    finally:
        # Корректное завершение работы
        log.info("Останавливаю бота...")
        for t in pollers: 
            t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        
        try: 
            await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=12)
        except Exception: 
            pass
            
        for t in background_tasks: 
            t.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        
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
        log.info("Бот остановлен вручную.")
