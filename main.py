# -*- coding: utf-8 -*-
"""
IXCLY Trading Bot v3.1 PRO

- 20+ индикаторов, свечные паттерны, 4 таймфрейма, бэктест, ИИ-аналитик
- Единый экран: фото + подпись в ОДНОМ сообщении, везде кнопка «Назад»
- Портфель, личная статистика сделок, экспорт CSV, вебхуки, Mini App (Telegram WebApp)
- Сигналы проверяются по свечам (вход лимиткой, TP1-3, трейлинг стопа)
- Всё состояние уходит в бэкап админ-боту и восстанавливается при старте
"""

import asyncio
import base64
import csv
import gc
import hashlib
import hmac
import html
import io
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from threading import Thread
from urllib.parse import quote, parse_qsl

import aiohttp
import feedparser
import numpy as np
import pandas as pd
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramBadRequest, TelegramConflictError, TelegramForbiddenError,
    TelegramNetworkError, TelegramNotFound, TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import (BufferedInputFile, ErrorEvent, InlineKeyboardButton, InlineKeyboardMarkup,
                           MenuButtonWebApp, WebAppInfo)
from flask import Flask, Response, jsonify, request, send_file
from PIL import Image, ImageDraw, ImageFont

try:
    import ccxt.async_support as ccxt
except ImportError:
    ccxt = None

try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None


class PortfolioStates(StatesGroup):
    waiting_for_deposit = State()


class AutotradeStates(StatesGroup):
    waiting_for_api_key = State()


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("ixcly")


class _ConflictFilter(logging.Filter):
    """Конфликт getUpdates при деплое Render безвреден: одна строка раз в минуту."""

    def __init__(self):
        super().__init__()
        self.last = 0.0

    def filter(self, record):
        msg = record.getMessage()
        if "TelegramConflictError" in msg or "terminated by other getUpdates" in msg:
            now = time.time()
            if now - self.last > 60:
                self.last = now
                log.warning("ℹ Конфликт getUpdates: рядом работает другой экземпляр (при деплое Render это нормально).")
            return False
        if "Sleep for" in msg and "try again" in msg:
            return False
        return True


logging.getLogger("aiogram.dispatcher").addFilter(_ConflictFilter())

# =====================================================================
# ВЕБ-СЕРВЕР (keep-alive для Render + Mini App)
# =====================================================================
app = Flask(__name__)


def _rss_mb():
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    except Exception:
        return None


@app.route("/")
def home():
    return "Bot is alive!"


@app.route("/health")
def health():
    return "ok"


@app.route("/status")
def status_page():
    return jsonify({
        "version": VERSION, "uptime_s": int(time.time() - stats["started"]), "recipients": len(active_chats),
        "main_bot": runtime["main_bot"], "admin_bot": runtime["admin_bot"], "news_engine": market_bias.get("engine"),
        "last_scan": last_scan_info, "peak_rss_mb": _rss_mb(),
    })


def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 10000))
        log.info(f"🌐 Flask keep-alive на порту {port}")
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"Flask: {e}")


_web_started = False


def keep_alive():
    global _web_started
    if not _web_started:
        _web_started = True
        Thread(target=_run_web, daemon=True).start()


# =====================================================================
# НАСТРОЙКИ
# =====================================================================
def clean_key(k):
    return k.strip().strip('"').strip("'") if k else None


BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))
GEMINI_MODEL = clean_key(os.getenv("GEMINI_MODEL"))
GROQ_MODEL = clean_key(os.getenv("GROQ_MODEL"))
OPENROUTER_MODEL = clean_key(os.getenv("OPENROUTER_MODEL"))
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

if not BOT_TOKEN:
    raise SystemExit("КРИТИЧЕСКАЯ ОШИБКА: переменная BOT_TOKEN не задана!")

ADMIN_IDS = set()
for _x in re.split(r"[,\s]+", ADMIN_ID or ""):
    if _x.lstrip("-").isdigit():
        ADMIN_IDS.add(int(_x))


def make_cipher():
    """Ключ шифрования API-ключей: FERNET_KEY, иначе производный от BOT_TOKEN."""
    if not Fernet:
        return None
    raw = clean_key(os.getenv("FERNET_KEY"))
    if not raw:
        log.warning("FERNET_KEY не задан: ключи шифруются от BOT_TOKEN. Задайте FERNET_KEY в переменных Render.")
    if raw:
        try:
            return Fernet(raw.encode())
        except Exception as e:
            log.warning(f"FERNET_KEY некорректен ({e}), использую ключ, производный от BOT_TOKEN")
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(("ixcly:" + BOT_TOKEN).encode()).digest()))


cipher = make_cipher()

VERSION = "3.1"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}
POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "55"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "23"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))
SCAN_MAX_COINS = int(os.getenv("SCAN_MAX_COINS", "150"))
DEPOSIT = float(os.getenv("DEPOSIT", "1000"))
RISK_PCT = 1.0
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))  # сканер каждые 5 минут
SIGNAL_COOLDOWN = 6 * 3600

# --- Mini App ---
_wu = clean_key(os.getenv("WEBAPP_URL")) or (
    os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/") if os.getenv("RENDER_EXTERNAL_URL") else None)
WEBAPP_URL = (_wu.rstrip("/") if _wu.rstrip("/").endswith("/app") else _wu.rstrip("/") + "/app") if _wu else None
MAIN_LOOP = None
MODE_TYPES = {"aggressive": ("Краткосрочный", "Среднесрочный"), "balanced": ("Среднесрочный", "Долгосрочный"),
              "conservative": ("Долгосрочный",)}
MODE_NAMES = {"aggressive": "🟢 Агрессивный", "balanced": "🟡 Сбалансированный", "conservative": "🔴 Консервативный"}
MODE_CONF = {"aggressive": 60, "balanced": 66, "conservative": 80}
NOTIFY_NAMES = {"signals": "Сигналы", "news": "Новости", "anomalies": "Аномалии объёма",
                "pos_updates": "Отчёты по сделкам", "briefing": "Утренний брифинг"}

bot = Bot(token=BOT_TOKEN, session=AiohttpSession(timeout=60.0))
dp = Dispatcher()
admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    admin_bot = Bot(token=INPUT_BOT_TOKEN, session=AiohttpSession(timeout=60.0))
    admin_dp = Dispatcher()
elif INPUT_BOT_TOKEN:
    log.warning("INPUT_BOT_TOKEN совпадает с BOT_TOKEN: админ-бот выключен")

http = None
news_lock = None
scan_lock = None

TFS = ["15m", "1h", "4h", "1d"]
MIN_CANDLES = 60

_COIN_TEXT = """
BTC ETH BNB SOL XRP DOGE ADA AVAX TON SHIB DOT LINK BCH NEAR POL LTC ICP UNI APT STX FIL IMX HBAR ATOM
INJ OP ARB VET MKR SUI RNDR GRT THETA S SAND MANA GALA PEPE WIF BONK ORDI JUP TIA SEI TRX XLM ETC XMR
AAVE ALGO EOS NEO IOTA ZEC DASH COMP SNX CRV LDO ENS 1INCH CAKE RUNE KSM FLOW AXS CHZ BAT ENJ ZIL FTM KAVA
EGLD ROSE AR FET WLD APE GMT CFX PYTH JTO ONDO FLOKI KAS CRO MNT TAO STRK DYDX GMX PENDLE SUSHI YFI BAL ZRX
KNC UMA BAND API3 SSV LPT RAY ORCA ILV MAGIC PRIME BEAM YGG ALICE TLM DUSK HIGH CHR C98 AI NMR ARKM IO ATH
BOME MEME SATS RATS DCR DGB XVG SC RVN WAVES ZEN BLAST AEVO ETHFI ENA W EIGEN MOVE PNUT GOAT ACT MOODENG
TURBO MOG NEIRO DOGS NOT PIXEL PORTAL DYM ALT MANTA ZETA TNSR DRIFT KMNO WEN MYRO POPCAT GIGA MEW SLERF
LUNC LUNA USTC CTSI COTI CELO CKB ONE QTUM IOST ONT ICX WAN ARK STRAX STMX PHA KLAY XEC XNO XTZ YFII XVS
XDC XAVA WOO WAXP VTHO VRA VOXEL VITE VIB VELO UOS UFT TRU TRB TOMO TLOS TKO TFUEL SUN STORJ STEEM SRM SLP
SKL SFP SFUND RSR RPL RLC RIF REEF RARE RAM QNT QKC QI PYR PUNDIX PROM POND POLYX POLS PNG PNK PERP PEG
PAXG PAR PAAL ONG OMG OGN OCEAN NKN NEXO NEBL NCT NAS MTL MTH MPL MNW MIR MINA MIM MFT META MBOX MBL MASK
LYXE LTO LSS LSK LQTY LOKA LOOM LIT LINA LAZIO LAT KMD JOE JASMY IOTX ION IDEX ID ICE HARD HAPI GTC GT GLMR
GLM GFT GEL FXS FTT FT FIDA FIO FARM ELF ELA DREP DODO DOCK DNT DMA DIA DENT CVC CTXC CTK CREAM COVAL COS
CND CMT CLV CITY CHESS CELR BURGER BTS BTG BOND BOSON BNT BNX BLZ BICO BETA BEL BAR BADGER BAKED AXL AVT
AVA AUCTION AUDIO ATM ATLAS ASTR ASR ARPA ARDR ANTV ANT ANKR AMP AMB ALPHA ALPACA AKRO AION AGLD AGIX ADX
ACM ACE ZK ZRO LISTA TAIKO REZ OMNI SAGA MAVIA XAI NFP PHB AIDOGE AKT ELIZA FAI AIT AGRS NOS VVAIFU BUZZ
SWARMS ARC ZYN LUCE GRIFFAIN ZEREBRO AI16Z BAN FARTCOIN PENGU HYPE HMSTR CATI
"""
POPULAR_COINS = list(dict.fromkeys(_COIN_TEXT.split()))

CORRELATION_GROUPS = [
    ["ETH", "ARB", "OP", "MATIC", "STRK", "ZK", "MANTA"],
    ["SOL", "JUP", "PYTH", "WIF", "BONK"],
    ["DOGE", "SHIB", "PEPE", "FLOKI", "BOME", "MEME"],
    ["AVAX", "JOE", "GMX"],
    ["BNB", "CAKE"],
    ["LINK", "PYTH", "BAND"],
    ["UNI", "SUSHI", "1INCH"],
    ["ATOM", "OSMO", "TIA", "INJ", "SEI"],
    ["NEAR", "FET", "RNDR", "AGIX"],
]

SYMBOL_ALIASES = {
    "RNDR": ["RENDER", "RNDR"], "MATIC": ["POL", "MATIC"],
    "PEPE": ["PEPE", "1000PEPE"], "BONK": ["BONK", "1000BONK"], "FLOKI": ["FLOKI", "1000FLOKI"],
    "SATS": ["SATS", "1000SATS"], "SHIB": ["SHIB", "1000SHIB"], "XEC": ["XEC", "1000XEC"],
    "LUNC": ["LUNC", "1000LUNC"], "RATS": ["RATS", "1000RATS"],
}

# =====================================================================
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# =====================================================================
active_chats = set()
busy_users = set()
user_res = {}
analysis_cache = {}
bt_cache = {}
preferred_source = {}
fetch_errors = {}
signal_sent_at = {}
anomaly_sent_at = {}
scan_skip = {}
nav_stack = {}
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "started": time.time()}
bot_flags = {"news_enabled": True, "alerts_enabled": True}
last_scan_info = {"ts": 0, "stage1": 0, "stage2": 0, "backtest_ok": 0, "final": 0, "secs": 0}
market_bias = {"bias": 0, "title_ru": "Нет данных", "summary": "", "impact": "Нейтральный фон", "coins": "BTC, ETH",
               "published": "", "link": "", "engine": "none", "ts": 0}
last_news_link = None
last_backup_msg = {}
next_timers = {"scanner": 0, "positions": 0, "pos_updates": 0, "news_watcher": 0, "backup": 0, "global_signals": 0, "anomaly": 0}
exchange_bans = {}
news_blackout_until = 0.0

# =====================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЯ (память + файл + бэкап в Telegram)
# =====================================================================
STATE_FILE = "bot_state.json"
STATE_LOCK = threading.RLock()
STATE = {"users": {}, "signal_history": [], "analysis_db": {}, "last_msgs": {}, "persist": {}, "meta": {"saved_at": 0}, "promos": {}, "ads": {}}
state_dirty = False
backup_dirty = False
last_backup_ts = 0.0
STATE_KEYS = ("users", "signal_history", "analysis_db", "last_msgs", "persist", "promos", "ads")


def state_touch(important=True):
    global state_dirty, backup_dirty
    state_dirty = True
    if important:
        backup_dirty = True


def collect_persist():
    return {
        "flags": dict(bot_flags),
        "stats": {k: v for k, v in stats.items() if k != "started"},
        "market_bias": dict(market_bias),
        "last_news_link": last_news_link,
        "signal_sent_at": dict(signal_sent_at),
        "anomaly_sent_at": dict(anomaly_sent_at),
        "last_scan_info": dict(last_scan_info),
        "bt_cache": {c: [v[0], v[1]] for c, v in bt_cache.items() if v[1]},
    }


def apply_persist(p):
    global last_news_link
    if not isinstance(p, dict):
        return
    for k, v in (p.get("flags") or {}).items():
        if k in bot_flags:
            bot_flags[k] = bool(v)
    for k, v in (p.get("stats") or {}).items():
        if k in stats and k != "started":
            stats[k] = v
    mb = p.get("market_bias")
    if isinstance(mb, dict) and mb.get("engine") not in (None, "none"):
        market_bias.update(mb)
    last_news_link = p.get("last_news_link") or last_news_link
    signal_sent_at.update({k: float(v) for k, v in (p.get("signal_sent_at") or {}).items()})
    anomaly_sent_at.update({k: float(v) for k, v in (p.get("anomaly_sent_at") or {}).items()})
    if isinstance(p.get("last_scan_info"), dict):
        last_scan_info.update(p["last_scan_info"])
    for c, v in (p.get("bt_cache") or {}).items():
        if isinstance(v, list) and len(v) == 2:
            bt_cache[c] = (float(v[0]), v[1])


def restore_state(data, force=False):
    """Применяет состояние (из файла или бэкапа). Берёт более свежую версию, если force=False."""
    if not isinstance(data, dict) or not data:
        return 0
    if "users" not in data and all(isinstance(v, dict) for v in data.values()):
        data = {"users": data}
    with STATE_LOCK:
        local_ts = STATE["meta"].get("saved_at", 0)
        remote_ts = (data.get("meta") or {}).get("saved_at", 0)
        if not force and STATE["users"] and local_ts >= remote_ts:
            return 0
        for k in STATE_KEYS:
            if k in data and data[k] is not None:
                STATE[k] = data[k]
        STATE["meta"]["saved_at"] = max(local_ts, remote_ts)
        n = len(STATE["users"])
    apply_persist(STATE.get("persist"))
    state_touch(False)
    return n


def load_state_file():
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            restore_state(json.load(f), force=True)
    except Exception as e:
        log.error(f"Не удалось прочитать {STATE_FILE}: {e}")


def flush_state():
    """Пишет состояние на диск, возвращает JSON-строку (для бэкапа)."""
    global state_dirty
    with STATE_LOCK:
        STATE["persist"] = collect_persist()
        STATE["meta"] = {"saved_at": time.time(), "version": VERSION}
        data = json.dumps(STATE, ensure_ascii=False, default=str)
        try:
            with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
                f.write(data)
            os.replace(STATE_FILE + ".tmp", STATE_FILE)
            state_dirty = False
        except Exception as e:
            log.error(f"Ошибка записи состояния: {e}")
    return data


async def state_flush_loop():
    while True:
        await asyncio.sleep(15)
        if state_dirty:
            await asyncio.to_thread(flush_state)


# ---------------- пользователи ----------------
def _users():
    return STATE["users"]


def save_user_data(user_id, username, first_name):
    key = str(user_id)
    with STATE_LOCK:
        new = key not in STATE["users"]
        u = STATE["users"].setdefault(key, {"joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        u.update({"username": username, "first_name": first_name, "active": True})
    _init_trial(user_id)
    state_touch(new)


def mark_users_inactive(ids):
    with STATE_LOCK:
        for i in ids:
            u = STATE["users"].get(str(i))
            if u:
                u["active"] = False
    state_touch(False)


def load_active_chats():
    active_chats.clear()
    for uid, u in _users().items():
        if isinstance(u, dict) and u.get("active", True):
            try:
                active_chats.add(int(uid))
            except ValueError:
                pass
    log.info("Получателей рассылки: %d", len(active_chats))


def user_counts():
    users = _users()
    return len(users), sum(1 for u in users.values() if isinstance(u, dict) and u.get("active", True))


def get_user_notif_settings(user_id):
    u = _users().get(str(user_id), {})
    return {
        "signals": u.get("notify_signals", True),
        "news": u.get("notify_news", True),
        "anomalies": u.get("notify_anomalies", True),
        "pos_updates": u.get("notify_pos_updates", True),
        "briefing": u.get("notify_briefing", True),
    }


def get_user_saved_signals(user_id):
    return list(_users().get(str(user_id), {}).get("saved_signals", []))


def add_user_saved_signal(user_id, coin, text, res=None):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(user_id), {"active": True})
        lst = [s for s in u.get("saved_signals", []) if s.get("coin") != coin]
        item = {"coin": coin, "text": text, "status": "active", "ts": time.time(), "expire_ts": time.time() + 48 * 3600}
        if res and res.get("entry"):
            item.update({"entry": res["entry"], "tp1": res["tps"][0], "sl1": res["sls"][0], "is_long": res["is_long"]})
        lst.append(item)
        u["saved_signals"] = lst
    state_touch()


def del_user_saved_signal(user_id, coin):
    with STATE_LOCK:
        u = STATE["users"].get(str(user_id))
        if u and "saved_signals" in u:
            u["saved_signals"] = [s for s in u["saved_signals"] if s.get("coin") != coin]
    state_touch()


def get_user_open_positions(user_id):
    return dict(_users().get(str(user_id), {}).get("open_positions", {}))


def get_open_position(user_id, coin):
    return get_user_open_positions(user_id).get(coin)


def set_user_open_position(user_id, coin, data):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(user_id), {"active": True})
        pos = u.setdefault("open_positions", {})
        if data is None:
            pos.pop(coin, None)
        else:
            pos[coin] = data
    state_touch()


# ---------------- история сигналов и архив анализов ----------------
def load_signal_history():
    with STATE_LOCK:
        return [x for x in STATE["signal_history"] if time.time() - x.get("ts", 0) < 30 * 86400]


def save_signal_history(hist):
    hist = hist[-2000:]
    for x in hist[:-150]:
        x.pop("text", None)
    with STATE_LOCK:
        STATE["signal_history"] = hist
    state_touch()


def save_analysis_db(coin, res):
    item = {k: v for k, v in res.items() if k != "img"}
    with STATE_LOCK:
        STATE["analysis_db"][coin] = {"ts": time.time(), "data": item}
        if len(STATE["analysis_db"]) > 150:
            for k in sorted(STATE["analysis_db"], key=lambda c: STATE["analysis_db"][c]["ts"])[:30]:
                STATE["analysis_db"].pop(k, None)
    state_touch(False)


def load_analysis_db(coin):
    return STATE["analysis_db"].get(coin)


# =====================================================================
# ПОРТФЕЛЬ, API-КЛЮЧИ, ВЕБХУКИ, CSV, ON-CHAIN
# =====================================================================
def get_user_risk_settings(user_id):
    u = _users().get(str(user_id), {})
    return {"deposit": float(u.get("deposit", DEPOSIT)), "risk_pct": float(u.get("risk_pct", RISK_PCT)),
            "max_open_risk": float(u.get("max_open_risk", 5.0))}


def calculate_portfolio_risk(user_id):
    s = get_user_risk_settings(user_id)
    return sum(p.get("risk_pct", s["risk_pct"]) for p in get_user_open_positions(user_id).values())


def set_user_field(user_id, **fields):
    with STATE_LOCK:
        STATE["users"].setdefault(str(user_id), {"active": True}).update(fields)
    state_touch()


def save_user_api_keys(user_id, exchange, api_key, secret_key):
    if not cipher:
        return False
    set_user_field(user_id, exchange=exchange,
                   **{f"{exchange}_api": cipher.encrypt(api_key.encode()).decode(),
                      f"{exchange}_secret": cipher.encrypt(secret_key.encode()).decode()})
    return True


def get_user_api_keys(user_id, exchange=None):
    u = _users().get(str(user_id), {})
    if not exchange:
        exchange = u.get("exchange", "binance")
    api_f, sec_f = f"{exchange}_api", f"{exchange}_secret"
    if not cipher or not u.get(api_f):
        return None, None
    try:
        return cipher.decrypt(u[api_f].encode()).decode(), cipher.decrypt(u[sec_f].encode()).decode()
    except Exception:
        return None, None


async def send_webhook_all(event):
    """Дублирует событие сигнала POST-запросом всем пользователям, у кого задан вебхук."""
    for uid, u in list(_users().items()):
        url = u.get("webhook_url")
        if url:
            try:
                async with http.post(url, json=event, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    log.info(f"Webhook {uid}: {r.status}")
            except Exception as e:
                log.warning(f"Webhook {uid}: {e}")


def generate_csv_history():
    with STATE_LOCK:
        hist = list(STATE["signal_history"])
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "TP2", "TP3", "SL1", "Статус", "Макс TP", "Уверенность"])
    for x in hist:
        w.writerow([datetime.fromtimestamp(x.get("ts", 0)).strftime("%Y-%m-%d %H:%M"), x.get("coin"), x.get("type"),
                    x.get("side"), x.get("entry"), x.get("tp1"), x.get("tp2"), x.get("tp3"), x.get("sl1"),
                    x.get("status"), x.get("max_tp_hit", 0), x.get("strength")])
    return out.getvalue().encode("utf-8-sig")


_defillama_cache = {"ts": 0.0, "change": 0.0}


async def get_onchain_context():
    """Изменение суммарного TVL за сутки (DefiLlama)."""
    if time.time() - _defillama_cache["ts"] < 3600:
        return _defillama_cache["change"]
    try:
        data = await http_json("https://api.llama.fi/v2/historicalChainTvl", timeout=10)
        if len(data) > 2 and data[-2]["tvl"]:
            ch = (data[-1]["tvl"] - data[-2]["tvl"]) / data[-2]["tvl"] * 100
            _defillama_cache.update(ts=time.time(), change=round(ch, 2))
    except Exception as e:
        log.info(f"DefiLlama: {e}")
        _defillama_cache["ts"] = time.time()
    return _defillama_cache["change"]


# =====================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================================================================
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


def make_strength_bar(strength, length=10):
    filled = max(0, min(length, int(round(strength / 100 * length))))
    return "🟩" * filled + "🟥" * (length - filled)


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


def strip_tags(t):
    return html.unescape(re.sub(r"<[^>]+>", "", t or ""))


def vis_len(t):
    return len(strip_tags(t))


def trim_html(text, limit):
    if vis_len(text) <= limit:
        return text
    return html.escape(strip_tags(text)[:limit - 1]) + "…"


def fit_caption(parts, limit=1000):
    """parts: [(приоритет, строка)]. Пока не влезает в лимит, убираем строки с наибольшим приоритетом."""
    items = list(parts)

    def total():
        return sum(vis_len(line) + 1 for _, line in items)

    while total() > limit and items:
        worst = max(p for p, _ in items)
        if worst <= 1:
            break
        idx = max(i for i, (p, _) in enumerate(items) if p == worst)
        items.pop(idx)
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(line for _, line in items)).strip()
    return trim_html(text, limit)


def normalize_ticker(text):
    t = re.sub(r"[\s$/\-_]", "", text.upper().strip())
    return t[:-4] if t.endswith("USDT") and len(t) > 4 else t


def get_moscow_time_from_entry(entry):
    msk = timezone(timedelta(hours=3))
    try:
        published = None
        if getattr(entry, "published_parsed", None):
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "updated_parsed", None):
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "published", None):
            published = parsedate_to_datetime(entry.published)
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
        if published:
            return published.astimezone(msk).strftime("%d.%m.%Y %H:%M") + " МСК"
    except Exception:
        pass
    return datetime.now(msk).strftime("%d.%m.%Y %H:%M") + " МСК"


async def first_success(coros, timeout=11):
    """Первый НЕпустой результат; провайдер с ошибкой остальных не отменяет."""
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


# =====================================================================
# ЕДИНЫЙ ЭКРАН: отправка, очистка старых сообщений, навигация «Назад»
# =====================================================================
def get_tracked(chat_id):
    return list(STATE["last_msgs"].get(str(chat_id), []))


def set_tracked(chat_id, ids):
    with STATE_LOCK:
        STATE["last_msgs"][str(chat_id)] = list(ids)[-4:]
    state_touch(False)


async def delete_ids(chat_id, ids):
    ids = [i for i in set(ids) if i]
    if not ids:
        return
    try:
        await bot.delete_messages(chat_id=chat_id, message_ids=ids[:100])
        return
    except Exception:
        pass
    for i in ids[:30]:
        try:
            await bot.delete_message(chat_id, i)
        except Exception:
            pass


async def purge_chat(chat_id, upto_id, count=100):
    ids = list(range(max(1, upto_id - count), upto_id + 1))
    try:
        await bot.delete_messages(chat_id=chat_id, message_ids=ids)
    except Exception:
        pass


async def _send_new(chat_id, text, kb, photo):
    if TG_SEM is None:
        return await _send_new_raw(chat_id, text, kb, photo)
    async with TG_SEM:
        return await _send_new_raw(chat_id, text, kb, photo)


async def _send_new_raw(chat_id, text, kb, photo):
    try:
        if photo:
            return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=trim_html(text, 1024),
                                        parse_mode="HTML", reply_markup=kb)
        return await bot.send_message(chat_id, trim_html(text, 4000), parse_mode="HTML", reply_markup=kb,
                                      disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "parse" not in str(e).lower() and "entit" not in str(e).lower():
            raise
        plain = strip_tags(text)
        if photo:
            return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=plain[:1024], reply_markup=kb)
        return await bot.send_message(chat_id, plain[:4000], reply_markup=kb)


async def present(chat_id, text, kb=None, photo=None, edit_msg=None):
    """Показывает экран ОДНИМ сообщением и стирает все предыдущие сообщения бота в этом чате."""
    old = set(get_tracked(chat_id))
    if edit_msg is not None:
        old.add(edit_msg.message_id)
    msg = None
    has_media = edit_msg is not None and bool(edit_msg.photo or edit_msg.video or edit_msg.document or edit_msg.animation)
    if photo is None and edit_msg is not None and not has_media:
        try:
            await bot.edit_message_text(trim_html(text, 4000), chat_id=chat_id, message_id=edit_msg.message_id,
                                        reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
            msg = edit_msg
        except TelegramBadRequest as e:
            if "not modified" in str(e).lower():
                msg = edit_msg
        except (TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter):
            raise
        except Exception:
            msg = None
    if msg is None:
        msg = await _send_new(chat_id, text, kb, photo)
    await delete_ids(chat_id, [i for i in old if i != msg.message_id])
    set_tracked(chat_id, [msg.message_id])
    return msg


async def show_loading(chat_id, edit_msg, text):
    try:
        if edit_msg is None:
            return await present(chat_id, text)
        if edit_msg.photo:
            await bot.edit_message_caption(chat_id=chat_id, message_id=edit_msg.message_id, caption=text,
                                           parse_mode="HTML", reply_markup=None)
        else:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=edit_msg.message_id, parse_mode="HTML")
    except Exception:
        pass
    return edit_msg


def nav_reset(uid, *screens):
    nav_stack[uid] = [("menu",)] + list(screens)


def nav_push(uid, screen):
    st = nav_stack.setdefault(uid, [("menu",)])
    if st[-1] != screen:
        st.append(screen)
    if len(st) > 25:
        del st[1:-24]


def nav_replace(uid, screen):
    st = nav_stack.setdefault(uid, [("menu",)])
    if len(st) > 1 and st[-1][0] == screen[0]:
        st[-1] = screen
    else:
        nav_push(uid, screen)


def nav_back(uid):
    st = nav_stack.setdefault(uid, [("menu",)])
    if len(st) > 1:
        st.pop()
    return st[-1]


def nav_row(back=True):
    row = []
    if back:
        row.append(InlineKeyboardButton(text="⬅ Назад", callback_data="nav:back"))
    row.append(InlineKeyboardButton(text="🏠 Меню", callback_data="menu"))
    return row


def kb_nav(rows=None, back=True):
    return InlineKeyboardMarkup(inline_keyboard=list(rows or []) + [nav_row(back)])


# =====================================================================
# МИДЛВАРЬ И ОБРАБОТЧИК ОШИБОК
# =====================================================================
class UserTrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user, chat_id = None, None
        if event.message:
            user, chat_id = event.message.from_user, event.message.chat.id
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
    if isinstance(exc, TelegramBadRequest) and any(s in str(exc).lower() for s in ("query is too old", "not modified", "message to delete not found")):
        return True
    log.error(f"Необработанная ошибка: {exc}", exc_info=exc)
    return True


# =====================================================================
# ИНДИКАТОРЫ И СВЕЧНЫЕ ПАТТЕРНЫ
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
    upper, lower = hl2 + mult * a, hl2 - mult * a
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
    p["three_white"] = (bull & sh(bull) & sh(bull, 2) & (c > c.shift()) & (c.shift() > c.shift(2)) & (o > o.shift())
                        & (o.shift() > o.shift(2)) & (o < c.shift()) & (o.shift() < c.shift(2)) & (body > 0.6 * avg_body))
    p["three_black"] = (bear & sh(bear) & sh(bear, 2) & (c < c.shift()) & (c.shift() < c.shift(2)) & (o < o.shift())
                        & (o.shift() < o.shift(2)) & (o > c.shift()) & (o.shift() > c.shift(2)) & (body > 0.6 * avg_body))
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
    ag, al = _wilder(delta.clip(lower=0), 14), _wilder(-delta.clip(upper=0), 14)
    rsi = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    df["rsi"] = rsi.where(al != 0, 100.0).fillna(50.0)
    rmin, rmax = df["rsi"].rolling(14).min(), df["rsi"].rolling(14).max()
    df["stoch_k"] = ((df["rsi"] - rmin) / (rmax - rmin).replace(0, np.nan)).rolling(3).mean().fillna(0.5)

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

    mid, std = c.rolling(20).mean(), c.rolling(20).std()
    upper_b, lower_b = mid + 2 * std, mid - 2 * std
    df["bb_pct"] = ((c - lower_b) / (upper_b - lower_b).replace(0, np.nan)).fillna(0.5)
    bw = (upper_b - lower_b) / mid.replace(0, np.nan)
    df["squeeze"] = (bw <= bw.rolling(100, min_periods=30).quantile(0.15)).fillna(False)

    df["st_dir"] = _supertrend(h, l, c, _wilder(tr, 10).fillna(tr.mean()), 3.0)

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
    pos_f, neg_f = raw.where(tp > tp.shift(), 0.0), raw.where(tp < tp.shift(), 0.0)
    df["mfi"] = (100 - 100 / (1 + pos_f.rolling(14).sum() / neg_f.rolling(14).sum().replace(0, np.nan))).fillna(50)
    std_dev = tp.rolling(20).std().replace(0, np.nan)
    df["cci"] = ((tp - tp.rolling(20).mean()) / (0.015 * std_dev)).fillna(0)
    hh, ll = h.rolling(14).max(), l.rolling(14).min()
    df["wr"] = (-100 * (hh - c) / (hh - ll).replace(0, np.nan)).fillna(-50)

    df["obv"] = (np.sign(c.diff()) * v).fillna(0).cumsum()
    df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
    df["rel_vol"] = (v / v.rolling(20).mean().replace(0, np.nan)).fillna(1.0)
    df["vol_delta"] = ((c - o) / (h - l).replace(0, np.nan) * v).fillna(0)
    df["z_score"] = ((c - df["ema50"]) / c.rolling(50).std().replace(0, np.nan)).fillna(0)
    df["momentum"] = c.pct_change(10).fillna(0)

    df["brk_up"] = (c > h.shift(1).rolling(20).max()).fillna(False)
    df["brk_dn"] = (c < l.shift(1).rolling(20).min()).fillna(False)
    low_min, high_max = l.shift(1).rolling(14).min(), h.shift(1).rolling(14).max()
    rsi_min, rsi_max = df["rsi"].shift(1).rolling(14).min(), df["rsi"].shift(1).rolling(14).max()
    df["div_bull"] = ((l <= low_min) & (df["rsi"] > rsi_min + 3)).fillna(False)
    df["div_bear"] = ((h >= high_max) & (df["rsi"] < rsi_max - 3)).fillna(False)
    return add_patterns(df)


def add_scores(df):
    """Три модели (тренд / моментум / price action) с весами по режиму рынка. long_p 5..95."""
    n = len(df)
    c, adx = df["close"].values, df["adx"].values
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
    z, roc = df["z_score"].values, df["momentum"].values
    zeros = np.zeros(n)
    mom = np.clip(
        mix(np.clip((rsi - 50) * 0.8, -20, 20), np.clip((50 - rsi) * 0.9, -25, 25))
        + mix((sk - 0.5) * 30, (0.5 - sk) * 40)
        + mix(np.clip(cci / 10, -15, 15), np.clip(-cci / 8, -20, 20))
        + mix(((wr + 50) / 50) * 8, -((wr + 50) / 50) * 12)
        + mix((mfi - 50) / 50 * 8, (50 - mfi) / 50 * 12)
        + mix(np.where(bbp > 1, 10, np.where(bbp < 0, -10, (bbp - 0.5) * 10)), np.clip((0.5 - bbp) * 40, -20, 20))
        + mix(zeros, np.clip(-z * 7, -15, 15))
        + mix(np.clip(roc * 400, -10, 10), zeros), -100, 100)

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
    obv_up = r["obv"] > r["obv_ema"]
    add(6, 1 if obv_up else -1, "OBV выше средней (приток)" if obv_up else "OBV ниже средней (отток)")
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
    res, sup = [], []
    for i in range(k, len(hv) - k):
        if hv[i] == hv[i - k:i + k + 1].max():
            res.append(float(hv[i]))
        if lv[i] == lv[i - k:i + k + 1].min():
            sup.append(float(lv[i]))
    below, above = [s for s in sup if s < price], [r for r in res if r > price]
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
    """Индикаторы по ЗАКРЫТЫМ свечам (формирующуюся отбрасываем), цена живая."""
    out = {}
    for tf, rows in rows_by_tf.items():
        price = float(rows[-1][4])
        df = prepare_df(rows_to_df(rows[:-1]))
        last = df.iloc[-1]
        long_p, adx = float(last["long_p"]), float(last["adx"])
        out[tf] = {
            "df": df, "price": price,
            "ema20": float(last["ema20"]), "ema50": float(last["ema50"]), "ema200": float(last["ema200"]),
            "rsi": float(last["rsi"]), "atr": float(last["atr"]), "adx": adx,
            "macd_hist": float(last["macd_hist"]), "bb_pct": float(last["bb_pct"]),
            "recent_low": float(min(df["low"].tail(28).min(), price)),
            "recent_high": float(max(df["high"].tail(28).max(), price)),
            "long_prob": long_p, "confidence": max(long_p, 100 - long_p),
            "qv24": float((df["close"] * df["volume"]).tail(6).sum()),
            "regime": "тренд" if adx >= 25 else "флэт" if adx < 20 else "переход",
            "squeeze": bool(last["squeeze"]),
            "reasons": explain_last(df), "patterns": last_patterns(df), "sr": support_resistance(df, price),
        }
    return out


# =====================================================================
# БИРЖИ: свечи, цены, история
# =====================================================================
def _p_binance(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]


def _p_okx(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["data"]]


def _p_bybit(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["result"]["list"]]


def _p_kucoin(j):
    return [[float(x[0]) * 1000, float(x[1]), float(x[3]), float(x[4]), float(x[2]), float(x[5])] for x in j["data"]]


def _p_gate(j):
    return [[float(x[0]) * 1000, float(x[5]), float(x[3]), float(x[4]), float(x[2]), float(x[6])] for x in j]


def _p_bitget(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["data"]]


def _p_htx(j):
    return [[float(x["id"]) * 1000, float(x["open"]), float(x["high"]), float(x["low"]), float(x["close"]), float(x["amount"])]
            for x in j["data"]]


EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines", "parse": _p_binance,
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "interval": tf, "limit": min(n, 1000)}},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline", "parse": _p_bybit,
     "params": lambda c, tf, n: {"category": "spot", "symbol": f"{c}USDT", "limit": min(n, 1000),
                                 "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf]}},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles", "parse": _p_okx,
     "params": lambda c, tf, n: {"instId": f"{c}-USDT", "limit": str(min(n, 300)),
                                 "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf]}},
    {"name": "MEXC", "url": "https://api.mexc.com/api/v3/klines", "parse": _p_binance,
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "limit": min(n, 1000),
                                 "interval": {"15m": "15m", "1h": "60m", "4h": "4h", "1d": "1d"}[tf]}},
    {"name": "KUCOIN", "url": "https://api.kucoin.com/api/v1/market/candles", "parse": _p_kucoin,
     "params": lambda c, tf, n: {"symbol": f"{c}-USDT", "type": {"15m": "15min", "1h": "1hour", "4h": "4hour", "1d": "1day"}[tf]}},
    {"name": "GATE", "url": "https://api.gateio.ws/api/v4/spot/candlesticks", "parse": _p_gate,
     "params": lambda c, tf, n: {"currency_pair": f"{c}_USDT", "limit": min(n, 1000), "interval": tf}},
    {"name": "BITGET", "url": "https://api.bitget.com/api/v2/spot/market/candles", "parse": _p_bitget,
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "limit": min(n, 1000),
                                 "granularity": {"15m": "15min", "1h": "1h", "4h": "4h", "1d": "1day"}[tf]}},
    {"name": "HTX", "url": "https://api.huobi.pro/market/history/kline", "parse": _p_htx,
     "params": lambda c, tf, n: {"symbol": f"{c.lower()}usdt", "size": min(n, 2000),
                                 "period": {"15m": "15min", "1h": "60min", "4h": "4hour", "1d": "1day"}[tf]}},
]


async def _fetch_klines(ex, coin, tf, limit=200, min_rows=None):
    j = await http_json(ex["url"], params=ex["params"](coin, tf, limit), timeout=9)
    rows = ex["parse"](j)
    rows.sort(key=lambda r: r[0])
    if len(rows) < (min_rows or MIN_CANDLES + 1):
        raise ValueError(f"мало свечей ({len(rows)})")
    return rows


def _note_error(coin, ex_name, err):
    fetch_errors.setdefault(coin, []).append(f"{ex_name}: {str(err)[:40]}")
    fetch_errors[coin] = fetch_errors[coin][-12:]


async def _try_exchange(ex, cand, tfs, coin=None):
    if time.time() < exchange_bans.get(ex["name"], 0):
        return None
    coin = coin or cand
    try:
        first = await _fetch_klines(ex, cand, tfs[0])
    except Exception as e:
        _note_error(coin, ex["name"], e)
        if any(code in str(e) for code in ("429", "418", "403", "451")):
            exchange_bans[ex["name"]] = time.time() + 300
            log.warning(f"⚠ {ex['name']} ограничил доступ ({e}). Пауза 5 минут.")
        return None
    results = [first]
    if len(tfs) > 1:
        rest = await asyncio.gather(*[_fetch_klines(ex, cand, tf) for tf in tfs[1:]], return_exceptions=True)
        for r in rest:
            if isinstance(r, Exception):
                _note_error(coin, ex["name"], r)
                return None
            results.append(r)
    return await asyncio.to_thread(build_tf_data, dict(zip(tfs, results))), ex["name"]


async def get_market_data(coin, tfs=tuple(TFS)):
    fetch_errors.pop(coin, None)
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]):
            res = await _try_exchange(ex, cand, tfs, coin)
            if res:
                preferred_source[coin] = (cand, ex["name"])
                return res
    raise LookupError(coin)


async def get_market_data_light(coin, tfs=tuple(TFS)):
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex:
            res = await _try_exchange(ex, pref[0], tfs, coin)
            if res:
                return res
            preferred_source.pop(coin, None)
    return await get_market_data(coin, tfs)


async def fetch_history(coin, tf="4h", limit=1000, min_rows=300):
    """Свечи с первой ответившей биржи. Запросы параллельно, не больше 3 одновременно, 429 → бан биржи."""
    order, seen = [], set()
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex:
            order.append((pref[0], ex))
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]):
            order.append((cand, ex))
    order = [(c, ex) for c, ex in order
             if (c, ex["name"]) not in seen and not seen.add((c, ex["name"])) and time.time() >= exchange_bans.get(ex["name"], 0)]
    sem = asyncio.Semaphore(3)

    async def one(cand, ex):
        async with sem:
            try:
                return await _fetch_klines(ex, cand, tf, limit, min_rows=min_rows)
            except Exception as err:
                if any(code in str(err) for code in ("429", "418", "403", "451")):
                    exchange_bans[ex["name"]] = time.time() + 300
                raise

    tasks = [asyncio.create_task(one(c, ex)) for c, ex in order]
    try:
        for coro in asyncio.as_completed(tasks):
            try:
                return await coro
            except Exception:
                continue
        return None
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def get_simple_price(coin):
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        for url, params, path in (
            ("https://data-api.binance.vision/api/v3/ticker/price", {"symbol": f"{cand}USDT"}, lambda j: j["price"]),
            ("https://api.bybit.com/v5/market/tickers", {"category": "spot", "symbol": f"{cand}USDT"}, lambda j: j["result"]["list"][0]["lastPrice"]),
            ("https://www.okx.com/api/v5/market/ticker", {"instId": f"{cand}-USDT"}, lambda j: j["data"][0]["last"]),
            ("https://api.mexc.com/api/v3/ticker/price", {"symbol": f"{cand}USDT"}, lambda j: j["price"]),
        ):
            try:
                return float(path(await http_json(url, params=params, timeout=5)))
            except Exception:
                continue
    for url, path in (
        (f"https://min-api.cryptocompare.com/data/price?fsym={coin}&tsyms=USDT", lambda j: j["USDT"]),
        (f"https://api.coinbase.com/v2/prices/{coin}-USD/spot", lambda j: j["data"]["amount"]),
        (f"https://api.kraken.com/0/public/Ticker?pair={coin}USDT", lambda j: list(j["result"].values())[0]["c"][0]),
    ):
        try:
            return float(path(await http_json(url, timeout=6)))
        except Exception:
            continue
    return None


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


_ctx_cache = {"ts": 0.0, "data": {}}


async def get_context(force=False):
    """Тренд BTC и индекс Fear & Greed."""
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
        log.info(f"Контекст BTC: {e}")
    try:
        j = await http_json("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
        ctx["fng"] = int(j["data"][0]["value"])
        ctx["fng_txt"] = str(j["data"][0].get("value_classification", ""))
    except Exception:
        pass
    _ctx_cache.update(ts=time.time(), data=ctx)
    return ctx


# =====================================================================
# ИИ: авто-подбор моделей, провайдеры, бесплатный GPT, аналитик
# =====================================================================
MODEL_CACHE = {"gemini": [], "groq": [], "openrouter": [], "ts": 0.0}
BAD_MODELS = {}
LAST_MODEL = {}
ENV_MODELS = {"gemini": GEMINI_MODEL, "groq": GROQ_MODEL, "openrouter": OPENROUTER_MODEL}
FALLBACK_MODELS = {
    "gemini": ["gemini-2.5-flash", "gemini-flash-latest", "gemini-2.0-flash", "gemini-2.5-flash-lite"],
    "groq": ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-20b", "gemma2-9b-it"],
    "openrouter": ["meta-llama/llama-3.3-70b-instruct:free", "deepseek/deepseek-chat-v3-0324:free",
                   "google/gemini-2.0-flash-exp:free", "qwen/qwen-2.5-72b-instruct:free", "mistralai/mistral-7b-instruct:free"],
}


def _ver(s):
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0


def pick_gemini_models(payload):
    names = []
    for m in payload.get("models", []):
        n = str(m.get("name", "")).replace("models/", "")
        if "generateContent" not in m.get("supportedGenerationMethods", []) or "flash" not in n:
            continue
        if any(x in n for x in ("image", "tts", "audio", "live", "embedding", "exp", "preview", "thinking", "vision", "robotics")):
            continue
        names.append(n)
    names.sort(key=lambda n: (not n.endswith("latest"), "lite" in n, -_ver(n)))
    return names[:5]


def pick_groq_models(payload):
    bad = ("whisper", "guard", "tts", "playai", "distil", "embed", "orpheus", "safeguard")
    ids = [m["id"] for m in payload.get("data", []) if m.get("active", True) and not any(b in m["id"].lower() for b in bad)]
    pref = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama", "qwen", "gemma"]

    def rank(i):
        for idx, p in enumerate(pref):
            if p == i:
                return idx - 100
            if p in i:
                return idx
        return 99

    return sorted(ids, key=rank)[:5]


def pick_openrouter_models(payload):
    fam = ["llama-3.3-70b", "gemini", "deepseek", "qwen", "mistral", "gpt-oss", "llama"]
    free = []
    for m in payload.get("data", []):
        mid = str(m.get("id", ""))
        pr = m.get("pricing") or {}
        try:
            is_free = float(pr.get("prompt", 1)) == 0 and float(pr.get("completion", 1)) == 0
        except (TypeError, ValueError):
            is_free = mid.endswith(":free")
        if not is_free or any(x in mid.lower() for x in ("vision", "image", "audio", "embed", "moderation", "guard")):
            continue
        rank = next((i for i, f in enumerate(fam) if f in mid.lower()), 99)
        free.append((rank, -int(m.get("context_length") or 0), mid))
    return [x[2] for x in sorted(free)[:6]]


async def discover_models(force=False):
    if not force and time.time() - MODEL_CACHE["ts"] < 6 * 3600:
        return
    MODEL_CACHE["ts"] = time.time()

    async def get(url, headers=None, params=None):
        async with http.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.json(content_type=None)

    async def gem():
        if not GEMINI_API_KEYS:
            return []
        return pick_gemini_models(await get("https://generativelanguage.googleapis.com/v1beta/models",
                                            {"x-goog-api-key": GEMINI_API_KEYS[0]}, {"pageSize": 200}))

    async def grq():
        if not GROQ_API_KEY:
            return []
        return pick_groq_models(await get("https://api.groq.com/openai/v1/models", {"Authorization": f"Bearer {GROQ_API_KEY}"}))

    async def opr():
        if not OPENROUTER_API_KEY:
            return []
        return pick_openrouter_models(await get("https://openrouter.ai/api/v1/models", {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}))

    res = await asyncio.gather(gem(), grq(), opr(), return_exceptions=True)
    for name, r in zip(("gemini", "groq", "openrouter"), res):
        if isinstance(r, list) and r:
            MODEL_CACHE[name] = r
        elif isinstance(r, Exception):
            log.info(f"Подбор моделей {name}: {r}")


async def models_for(prov):
    await discover_models()
    seq = []
    for m in [LAST_MODEL.get(prov), ENV_MODELS.get(prov)] + MODEL_CACHE[prov] + FALLBACK_MODELS[prov]:
        if m and m not in seq and BAD_MODELS.get((prov, m), 0) < time.time():
            seq.append(m)
    return seq[:6]


def _mark_bad(prov, model):
    BAD_MODELS[(prov, model)] = time.time() + 6 * 3600
    MODEL_CACHE["ts"] = min(MODEL_CACHE["ts"], time.time() - 6 * 3600 + 600)


async def _ask_gemini(prompt):
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    for key in GEMINI_API_KEYS:
        for model in await models_for("gemini"):
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            try:
                async with http.post(url, json=payload, headers={"x-goog-api-key": key}, timeout=aiohttp.ClientTimeout(total=18)) as r:
                    j = await r.json(content_type=None)
                    if r.status == 200:
                        LAST_MODEL["gemini"] = model
                        return j["candidates"][0]["content"]["parts"][0]["text"]
                    log.warning(f"Gemini/{model} {r.status}: {str(j)[:100]}")
                    if r.status in (401, 403):
                        break
                    if r.status in (400, 404):
                        _mark_bad("gemini", model)
            except Exception as e:
                log.warning(f"Gemini/{model}: {e}")
    return None


async def _ask_openai_compat(prov, base, key, prompt, extra_headers=None):
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    for model in await models_for(prov):
        payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1100}
        try:
            async with http.post(f"{base}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
                if r.status == 200:
                    txt = j["choices"][0]["message"]["content"]
                    if txt:
                        LAST_MODEL[prov] = model
                        return txt
                    continue
                log.warning(f"{prov}/{model} {r.status}: {str(j)[:100]}")
                if r.status in (401, 403):
                    return None
                if r.status in (400, 404):
                    _mark_bad(prov, model)
        except Exception as e:
            log.warning(f"{prov}/{model}: {e}")
    return None


def _looks_like_junk(text):
    t = (text or "").strip()
    low = t.lower()
    return len(t) < 30 or low.startswith("<!doctype") or low.startswith("<html") or any(x in low for x in ("pollinations", "deprecat", "rate limit"))


async def _ask_free_gpt(prompt):
    system = "Ты опытный крипто-аналитик. Отвечай на русском языке, кратко и по делу."
    auth = {"Authorization": f"Bearer {POLLINATIONS_KEY}"} if POLLINATIONS_KEY else {}

    async def post_openai(url, timeout):
        payload = {"model": POLLINATIONS_MODEL, "temperature": 0.2,
                   "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
        async with http.post(url, json=payload, headers={"Content-Type": "application/json", **auth},
                             timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return (await r.json(content_type=None))["choices"][0]["message"]["content"]

    async def get_plain(url, timeout):
        async with http.get(url, params={"model": POLLINATIONS_MODEL, "system": system}, headers=auth,
                            timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.text()

    short = quote(prompt[:3000], safe="")
    attempts = [lambda: post_openai("https://text.pollinations.ai/openai", 14),
                lambda: get_plain(f"https://gen.pollinations.ai/text/{short}", 10),
                lambda: get_plain(f"https://text.pollinations.ai/{short}", 8)]
    if POLLINATIONS_KEY:
        attempts.insert(0, lambda: post_openai("https://gen.pollinations.ai/v1/chat/completions", 14))
    for fn in attempts:
        try:
            text = await fn()
            if text and not _looks_like_junk(text):
                return text
        except Exception as e:
            log.info(f"Free GPT: {e}")
    return None


async def ask_ai_named(prompt, timeout=20, validate=None, free_timeout=30):
    """(текст, провайдер). Ключевые провайдеры параллельно, затем бесплатный GPT."""
    providers = []
    if GEMINI_API_KEYS:
        providers.append(("Gemini", lambda: _ask_gemini(prompt)))
    if GROQ_API_KEY:
        providers.append(("Groq", lambda: _ask_openai_compat("groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, prompt)))
    if OPENROUTER_API_KEY:
        providers.append(("OpenRouter", lambda: _ask_openai_compat("openrouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, prompt,
                                                                    {"HTTP-Referer": "https://t.me/bot", "X-Title": "IXCLY"})))

    async def wrap(name, fn):
        try:
            t = await fn()
        except Exception:
            return None
        return (t, name) if t and len(t) > 20 and (validate is None or validate(t)) else None

    if providers:
        res = await first_success([wrap(n, f) for n, f in providers], timeout=timeout)
        if res:
            return res
        log.warning("Ключевые ИИ не дали пригодного ответа, пробую бесплатный GPT")
    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt))], timeout=free_timeout)
    return res if res else (None, None)


_ai_cache = {}


# =====================================================================
# НОВОСТИ
# =====================================================================
BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner", "upgrad", "breakout", "rebound", "recover", "accumul")
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall", "liquidat", "fraud", "scam",
              "delist", "arrest", "dump", "selloff", "declin", "probe", "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
COIN_WORDS = {"bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL", "xrp": "XRP",
              "bnb": "BNB", "doge": "DOGE", "ada": "ADA", "ton": "TON", "avax": "AVAX", "link": "LINK", "zcash": "ZEC"}


def news_sentiment(text, threshold=2):
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w.startswith(BULL_STEMS):
            score += 1
        elif w.startswith(BEAR_STEMS):
            score -= 1
    return 1 if score >= threshold else -1 if score <= -threshold else 0


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
        async with http.get("https://translate.googleapis.com/translate_a/single",
                            params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text[:1800]},
                            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                out = "".join(part[0] for part in data[0] if part and part[0])
                if has_cyr(out, 3):
                    return out
    except Exception:
        pass
    return text


NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/", "https://www.theblock.co/rss.xml", "https://cointelegraph.com/rss",
    "https://decrypt.co/feed", "https://cryptoslate.com/feed/", "https://bitcoinmagazine.com/.rss/full/",
    "https://www.dlnews.com/arc/outboundfeeds/rss/", "https://blockworks.co/feed", "https://thedefiant.io/feed",
    "https://u.today/rss", "https://finance.yahoo.com/rss/cryptocurrency", "https://watcher.guru/news/feed",
    "https://www.cryptopolitan.com/feed/", "https://blog.coinmarketcap.com/feed/", "https://forklog.com/feed",
    "https://bits.media/rss/news/", "https://coinspot.io/feed/", "https://www.rbc.ru/crypto/rss",
]
_news_cache = {"ts": 0.0, "entries": []}
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


async def _load_feed(url):
    try:
        async with http.get(url, headers=BROWSER_UA, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return []
            text = await r.text()
        return list((await asyncio.to_thread(feedparser.parse, text)).entries)
    except Exception:
        return []


async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300:
        return _news_cache["entries"]
    results = await asyncio.gather(*[_load_feed(u) for u in NEWS_FEEDS], return_exceptions=True)
    entries = [e for r in results if isinstance(r, list) for e in r]
    if entries:
        entries.sort(key=lambda x: getattr(x, "published_parsed", None) or time.gmtime(0), reverse=True)
        _news_cache.update(ts=time.time(), entries=entries[:10])
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
    ms = list(re.finditer(r"(?im)^[\s\-\*•]*(BIAS|TITLE_RU|SUMMARY|IMPACT|COINS)\s*[:：]\s*", t))
    out = {}
    for i, m in enumerate(ms):
        end = ms[i + 1].start() if i + 1 < len(ms) else len(t)
        out[m.group(1).upper()] = re.sub(r"\s+", " ", t[m.end():end]).strip()
    return out


def _valid_news_ai(text):
    p = parse_news_ai(text)
    return has_cyr(p.get("TITLE_RU", ""), 6) and has_cyr(p.get("SUMMARY", ""), 20) and has_cyr(p.get("IMPACT", ""), 10)


IMPACT_TEMPLATES = {
    1: "Фон позитивный: интерес к рисковым активам может поддержать спрос и рост цен на BTC, ETH и альткоины.",
    -1: "Фон негативный: возможны распродажи и рост волатильности. Соблюдайте риск-менеджмент.",
    0: "Фон нейтральный: рынок ждёт новых триггеров, сигналам нужно дополнительное подтверждение.",
}


async def update_market_bias(force=False):
    global market_bias
    async with news_lock:
        try:
            entries = await get_news_entries()
            if not entries:
                return
            entry = entries[0]
            link, now = getattr(entry, "link", ""), time.time()
            if not force and market_bias.get("link") == link and market_bias.get("engine") not in ("none", "fallback"):
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
IMPACT: 2-3 предложения о влиянии на рынок: фон (бычий или медвежий), какие активы и почему
COINS: тикеры через запятую"""
            text, engine = await ask_ai_named(prompt, validate=_valid_news_ai)
            if text and _valid_news_ai(text):
                p = parse_news_ai(text)
                up = p.get("BIAS", "").upper()
                market_bias = {"bias": 1 if "BULL" in up else -1 if "BEAR" in up else 0, "title_ru": p["TITLE_RU"][:170],
                               "summary": shorten(p["SUMMARY"], 380), "impact": shorten(p["IMPACT"], 500),
                               "coins": p.get("COINS", "BTC, ETH"), "published": published, "link": link, "engine": engine, "ts": now}
            else:
                bias = news_sentiment(f"{title}. {summary}")
                title_ru = await translate_text(title)
                market_bias = {"bias": bias, "title_ru": title_ru[:170],
                               "summary": shorten(await translate_text(summary) if summary else title_ru, 380),
                               "impact": IMPACT_TEMPLATES[bias], "coins": ", ".join(mentioned_coins(f"{title} {summary}") or ["BTC", "ETH"]),
                               "published": published, "link": link, "engine": "fallback", "ts": now}
            state_touch(False)
        except Exception as e:
            log.warning(f"Ошибка обновления новостей: {e}")


def format_news_post(nb, limit=1000):
    emoji = {1: "🟢", 0: "⚪", -1: "🔴"}.get(nb.get("bias", 0), "⚪")
    label = {1: "Позитивно", 0: "Нейтрально", -1: "Негативно"}.get(nb.get("bias", 0), "Нейтрально")
    link = f"\n\n🔗 <a href=\"{html.escape(nb['link'], quote=True)}\">Источник</a>" if nb.get("link") else ""
    s_len, i_len = 300, 360
    while True:
        text = (f"📰 <b>{html.escape(shorten(nb.get('title_ru', ''), 140))}</b>\n🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n"
                f"📌 <b>Суть</b>\n{html.escape(shorten(nb.get('summary', ''), s_len))}\n\n"
                f"📊 <b>Влияние</b>\n{emoji} <b>{label}.</b> {html.escape(shorten(nb.get('impact', ''), i_len))}\n\n"
                f"🪙 <b>Монеты:</b> {html.escape(nb.get('coins', ''))}{link}")
        if vis_len(text) <= limit or s_len <= 80:
            return text
        s_len, i_len = s_len - 40, i_len - 40


async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries or market_bias.get("engine") == "none":
        return None, None
    return format_news_post(market_bias), extract_image(entries[0])


async def get_news_batch_text():
    entries = await get_news_entries()
    if not entries:
        return "📭 <b>Свежих новостей пока нет.</b>"
    batch = entries[:3]
    prompt = ("Ты опытный крипто-аналитик. Разбери эти новости. Для каждой строго по шаблону на русском, без markdown:\n"
              "Новость N:\n📰 Суть: 1-2 предложения\n📊 Влияние: Бычье/Медвежье/Нейтральное и почему\n🪙 Монеты: тикеры через запятую\n\nНовости:\n")
    for i, e in enumerate(batch):
        prompt += f"{i + 1}. [{get_moscow_time_from_entry(e)}] {html.unescape(getattr(e, 'title', '') or '')}. {clean_summary(getattr(e, 'summary', ''))[:200]}\n"
    links = " · ".join(f"<a href=\"{html.escape(getattr(e, 'link', ''), quote=True)}\">{i + 1}</a>" for i, e in enumerate(batch))
    text, _engine = await ask_ai_named(prompt, timeout=30, free_timeout=30)
    if text and has_cyr(text, 30):
        return trim_html(f"📰 <b>Дайджест новостей</b>\n\n{html.escape(text)}\n\n🔗 Источники: {links}", 4000)
    out = "📰 <b>Последние новости (кратко):</b>\n\n"
    for i, e in enumerate(batch):
        out += f"<b>{i + 1}.</b> {html.escape(await translate_text(html.unescape(getattr(e, 'title', '') or '')))}\n\n"
    return out + f"🔗 Источники: {links}"


# =====================================================================
# ОЦЕНКА СЕТАПА, УРОВНИ, БЭКТЕСТ
# =====================================================================
TF_WEIGHTS = {"1d": 0.30, "4h": 0.30, "1h": 0.25, "15m": 0.15}


def quick_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data:
        return None
    wp = (tf_data["4h"]["long_prob"] + tf_data["1d"]["long_prob"]) / 2
    strength = max(wp, 100 - wp)
    if strength < CONF_STAGE1 or tf_data["4h"]["adx"] < 10:
        return None
    return wp >= 50, strength


def collect_reasons(tf_data, is_long, tfs=("1d", "4h", "1h"), limit=5):
    side = 1 if is_long else -1
    bucket = {}
    for tf in tfs:
        for rsn in tf_data.get(tf, {}).get("reasons", []):
            if rsn["side"] == side:
                b = bucket.setdefault(rsn["text"], {"w": 0, "tfs": []})
                b["w"] += rsn["w"]
                b["tfs"].append(tf)
    items = sorted(bucket.items(), key=lambda kv: kv[1]["w"], reverse=True)[:limit]
    return [f"{t} ({', '.join(b['tfs'])})" for t, b in items]


def pattern_lines(tf_data):
    return [f"{tf}: {', '.join(p[0] for p in tf_data[tf]['patterns'][:3])}"
            for tf in ("1d", "4h", "1h") if tf in tf_data and tf_data[tf]["patterns"]]


def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    """Вход строго лимиткой на откате. Возвращает entry, sl1, risk."""
    if is_long:
        entry = min(price, max(ema20, ema50))
        if entry > price - 0.2 * atr:
            entry = price - 0.35 * atr
        if support and price - 1.5 * atr <= support <= price - 0.15 * atr:
            entry = min(max(entry, support + 0.1 * atr), price - 0.15 * atr)
        sl1 = max(min(rec_low - 0.5 * atr, entry - 0.8 * atr), entry - 2.5 * atr)
        return entry, sl1, entry - sl1
    entry = max(price, min(ema20, ema50))
    if entry < price + 0.2 * atr:
        entry = price + 0.35 * atr
    if resistance and price + 0.15 * atr <= resistance <= price + 1.5 * atr:
        entry = max(min(entry, resistance - 0.1 * atr), price + 0.15 * atr)
    sl1 = min(max(rec_high + 0.5 * atr, entry + 0.8 * atr), entry + 2.5 * atr)
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
    atr_pct = (atr / price) * 100
    if atr_pct > 3.0:
        leverage = "x3-x5 (Высокая волатильность ⚠)"
    elif atr_pct > 1.5:
        leverage = "x5-x10 (Средняя волатильность)"
    else:
        leverage = "x10-x20 (Низкая волатильность)"
    liq_x20 = entry * (1 - 0.0475) if is_long else entry * (1 + 0.0475)
    opp = sr.get("resistance") if is_long else sr.get("support")
    near = "ближайший уровень слишком близко к входу, цель может не отработать" if opp and abs(opp - entry) < 1.2 * risk else None
    return entry, tps, [sl1, sl2], risk, leverage, near, liq_x20


def backtest_df(df, conf_min=CONF_FINAL, rr1=1.5, fill_bars=6, max_hold=30, fee=0.001):
    """Симуляция стратегии на истории: лимитный вход с проверкой исполнения, TP1/SL, комиссии."""
    n = len(df)
    if n < 150:
        return None
    lp, adx, atr = df["long_p"].values, df["adx"].values, df["atr"].values
    c, h, l = df["close"].values, df["high"].values, df["low"].values
    e20, e50 = df["ema20"].values, df["ema50"].values
    low28, high28 = df["low"].rolling(28).min().values, df["high"].rolling(28).max().values
    results, i = [], 80
    while i < n - 2:
        p = lp[i]
        if max(p, 100 - p) < conf_min or adx[i] < 15 or atr[i] <= 0 or np.isnan(low28[i]):
            i += 1
            continue
        is_long = p >= 50
        entry, sl, risk = levels_core(c[i], atr[i], e20[i], e50[i], low28[i], high28[i], is_long)
        if risk <= 0:
            i += 1
            continue
        tp = entry + rr1 * risk if is_long else entry - rr1 * risk
        filled = next((j for j in range(i + 1, min(n, i + 1 + fill_bars))
                       if (is_long and l[j] <= entry) or ((not is_long) and h[j] >= entry)), None)
        if filled is None:
            i += 1
            continue
        res, last_k = None, filled
        for k in range(filled, min(n, filled + max_hold)):
            last_k = k
            if (l[k] <= sl) if is_long else (h[k] >= sl):
                res = -1.0
                break
            if (h[k] >= tp) if is_long else (l[k] <= tp):
                res = rr1
                break
        if res is None:
            res = max(-1.0, min(rr1, (((c[last_k] - entry) if is_long else (entry - c[last_k])) / risk)))
        results.append(res - fee * entry / risk)
        i = last_k + 1
    days = round(n * 4 / 24)
    if not results:
        return {"trades": 0, "winrate": 0.0, "avg_r": 0.0, "pf": 0.0, "days": days}
    arr = np.array(results)
    wins, losses = arr[arr > 0], arr[arr <= 0]
    pf = float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else 9.9
    return {"trades": len(arr), "winrate": round(float((arr > 0).mean() * 100), 1), "avg_r": round(float(arr.mean()), 2),
            "pf": round(min(pf, 9.9), 2), "days": days}


def run_backtest_rows(rows):
    return backtest_df(prepare_df(rows_to_df(rows[:-1])))


async def get_backtest(coin):
    if len(bt_cache) > 200:
        bt_cache.clear()
    c = bt_cache.get(coin)
    if c and time.time() - c[0] < 6 * 3600:
        return c[1]
    bt = None
    try:
        rows = await fetch_history(coin, "4h", 1000)
        if rows:
            bt = await asyncio.to_thread(run_backtest_rows, rows)
    except Exception as e:
        log.info(f"Бэктест {coin}: {e}")
    bt_cache[coin] = (time.time(), bt)
    return bt


def bt_ok(bt):
    return (bool(bt) and bt["trades"] >= BT_MIN_TRADES and bt["avg_r"] >= BT_MIN_AVG_R
            and bt["winrate"] >= BT_MIN_WINRATE and bt["pf"] >= float(os.getenv("BT_MIN_PF", "1.15")))


# =====================================================================
# ГРАФИК (PNG)
# =====================================================================
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
    col = (0, 200, 90) if is_long else (230, 60, 60)
    draw.text((18, 10), f"{coin}/USDT  {main_tf}  {'LONG' if is_long else 'SHORT'}", fill=col, font=font_b)
    pad_l, pad_r, pad_t, pr_bottom, vol_top, vol_bottom = 62, 18, 42, 405, 425, 520
    cw, ch = w - pad_l - pad_r, pr_bottom - pad_t
    highs, lows = df["high"].values, df["low"].values
    levels = [entry, price] + list(tps) + list(sls)
    mn, mx = min(lows.min(), min(levels)) * 0.998, max(highs.max(), max(levels)) * 1.002
    rng = mx - mn or 1.0

    def y(p):
        return pad_t + ch - (p - mn) / rng * ch

    n = len(df)
    step = cw / n
    cwid = max(3, int(step * 0.7))
    for i in range(n):
        x = pad_l + i * step + step / 2
        o, c = df["open"].iloc[i], df["close"].iloc[i]
        color = (0, 170, 80) if c >= o else (205, 50, 50)
        draw.line([(x, y(highs[i])), (x, y(lows[i]))], fill=color, width=1)
        top, bot = min(y(o), y(c)), max(y(o), y(c))
        draw.rectangle([x - cwid / 2, top, x + cwid / 2, max(bot, top + 1)], fill=color)
    for name, colr in (("ema20", (255, 205, 60)), ("ema50", (80, 150, 255))):
        draw.line([(pad_l + i * step + step / 2, y(v)) for i, v in enumerate(df[name].values)], fill=colr, width=2)
    vmax = df["volume"].max() or 1.0
    for i in range(n):
        x = pad_l + i * step + step / 2
        vh = (vol_bottom - vol_top) * (df["volume"].iloc[i] / vmax)
        color = (0, 120, 60) if df["close"].iloc[i] >= df["open"].iloc[i] else (140, 40, 40)
        draw.rectangle([x - cwid / 2, vol_bottom - vh, x + cwid / 2, vol_bottom], fill=color)
    recent_df = df.tail(50)
    swing_high, swing_low = recent_df["high"].max(), recent_df["low"].min()
    diff = swing_high - swing_low
    if diff > 0:
        for lbl, k in (("0.236", 0.236), ("0.382", 0.382), ("0.500", 0.5), ("0.618", 0.618), ("0.786", 0.786)):
            yy = y(swing_high - diff * k)
            draw.line([(pad_l, yy), (w - pad_r, yy)], fill=(80, 80, 110), width=1)
            draw.text((pad_l + 4, yy - 14), f"Fib {lbl}", fill=(120, 120, 160), font=font)
    for lvl_p, lbl in ((info["sr"].get("support"), "SUP"), (info["sr"].get("resistance"), "RES")):
        if lvl_p:
            for xx in range(pad_l, w - pad_r, 10):
                draw.line([(xx, y(lvl_p)), (xx + 4, y(lvl_p))], fill=(130, 130, 150), width=1)
            draw.text((w - pad_r - 90, y(lvl_p) - 14), f"{lbl} {fmt(lvl_p)}", fill=(150, 150, 170), font=font)

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
        draw.text((pad_l + cw - 200, 12), "Pattern: " + info["patterns"][0][2], fill=(255, 210, 120), font=font_b)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# =====================================================================
# АНАЛИЗ МОНЕТЫ
# =====================================================================
def local_verdict(setup, bt):
    if not setup["valid"]:
        return "WAIT"
    if bt is None:
        return "ENTER" if setup["strength"] >= CONF_FINAL + 4 else "WAIT"
    return "ENTER" if (bt_ok(bt) or bt["trades"] < BT_MIN_TRADES) and setup["strength"] >= CONF_FINAL else "WAIT"


def age_text(ts):
    sec = max(0, int(time.time() - ts))
    return f"{sec // 3600} ч {sec % 3600 // 60} мин назад" if sec >= 3600 else f"{sec // 60} мин назад"


def render_caption(res, uid=None):
    """Короткая карточка сигнала для Telegram-уведомления (≤1024 символов)."""
    coin = html.escape(res["coin"])
    if res.get("price_only"):
        return fit_caption([(0, f"💰 <b>{coin}</b>: <code>{fmt(res['price'])}</code>"),
                            (1, "ℹ Свечи недоступны, анализ невозможен.")])
    entry = res["entry"]

    def pct(p):
        return f"{(p - entry) / entry * 100:+.1f}%"

    s = get_user_risk_settings(uid) if uid else {"deposit": DEPOSIT, "risk_pct": RISK_PCT}
    risk_usdt = s["deposit"] * s["risk_pct"] / 100
    pos_usdt = risk_usdt / res["risk_dist"] * entry if res["risk_dist"] > 0 else 0.0
    verdict = "✅ <b>ВХОДИТЬ</b>" if res["verdict"] == "ENTER" else "⏳ <b>ЖДАТЬ</b>"
    emoji = "🟢" if res["is_long"] else "🔴"
    parts = []
    if res.get("stale"):
        parts.append((0, f"🕓 <i>Из архива: {age_text(res['ts'])}</i>"))
    parts += [
        (0, f"{emoji} <b>{res['side']} · {coin}</b>   {verdict}"),
        (1, f"📌 {res['type']} · Conf <b>{res['strength']}%</b> · {res['regime']}"),
        (1, make_strength_bar(res["strength"])),
    ]
    if res.get("fake_breakout"):
        parts.append((1, "⚠️ <b>Ложный пробой (тонкий объем)</b>"))
    parts += [
        (2, f"📰 {html.escape(res['news_status'])}"),
        (3, f"🌍 BTC {res['btc_txt']}" + (f" · F&G {res['fng']}" if res.get("fng") is not None else "")),
        (7, ""),
        (0, f"📥 Вход <b>(только ЛИМИТ)</b>: <code>{fmt(entry)}</code>"),
        (1, f"💰 Цена сейчас: <code>{fmt(res['price'])}</code>"),
        (7, ""),
    ]
    parts += [(0, f"🎯 TP{i + 1} <code>{fmt(p)}</code> <i>{pct(p)}</i>") for i, p in enumerate(res["tps"])]
    parts += [(0, f"🛡 SL{i + 1} <code>{fmt(p)}</code> <i>{pct(p)}</i>") for i, p in enumerate(res["sls"])]
    parts.append((7, ""))
    parts.append((2, f"⚖ {res['leverage']} · позиция ${pos_usdt:.0f} · риск ${risk_usdt:.0f}"))
    if res.get("liq_x20"):
        parts.append((3, f"⚠ <i>При плече x20 ликвидация: <code>{fmt(res['liq_x20'])}</code></i>"))
    if res.get("bt"):
        b = res["bt"]
        parts.append((3, f"🧪 Бэктест: {b['trades']} сд · TP1 {b['winrate']}% · {b['avg_r']:+.2f}R"))
    if res.get("comment"):
        parts.append((4, f"💬 {html.escape(shorten(res['comment'], 170))}"))
    parts.append((8, "⚠ <i>Не фин. рекомендация</i>"))
    return fit_caption(parts, 1000)


# =====================================================================
# ФИЛЬТРЫ РЫНКА v2: OI+цена, ложные пробои и сбор ликвидности, стакан,
# сжатие, сессии, обвал BTC, история funding, Chandelier, ИИ по сырым данным
# =====================================================================
OI_TXT = {
    "new_longs": "рост OI и цены: открываются новые лонги",
    "new_shorts": "рост OI при падении цены: открываются новые шорты",
    "short_cover": "OI падает при росте цены: шорты закрываются, импульс слабее",
    "long_liq": "OI падает при падении цены: лонги закрываются",
}
OI_DELTA = {  # (LONG, SHORT)
    "new_longs": (6, -6), "new_shorts": (-6, 6), "short_cover": (-3, -5), "long_liq": (-5, 3),
}


async def oi_price_state(coin):
    """Матрица OI + цена за текущие 15 минут: понимаем, кто открывает позиции."""
    try:
        async with http.get("https://fapi.binance.com/futures/data/openInterestHist",
                            params={"symbol": f"{coin}USDT", "period": "15m", "limit": 2},
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return None, 0.0, 0.0
            data = await r.json(content_type=None)
        if len(data) < 2:
            return None, 0.0, 0.0
        oi_old, oi_new = float(data[0]["sumOpenInterest"]), float(data[1]["sumOpenInterest"])
        oi_pct = (oi_new - oi_old) / oi_old * 100 if oi_old else 0.0
        kl = await http_json("https://fapi.binance.com/fapi/v1/klines",
                             params={"symbol": f"{coin}USDT", "interval": "15m", "limit": 1}, timeout=5)
        o, c = float(kl[-1][1]), float(kl[-1][4])
        p_pct = (c - o) / o * 100 if o else 0.0
    except Exception as e:
        log.info(f"OI {coin}: {e}")
        return None, 0.0, 0.0
    if oi_pct > 0.5 and p_pct > 0.2:
        st = "new_longs"
    elif oi_pct > 0.5 and p_pct < -0.2:
        st = "new_shorts"
    elif oi_pct < -0.5 and p_pct > 0.2:
        st = "short_cover"
    elif oi_pct < -0.5 and p_pct < -0.2:
        st = "long_liq"
    else:
        st = None
    return st, round(oi_pct, 2), round(p_pct, 2)


async def order_book_imbalance(coin, price):
    """Перевес сил в стакане: доля бидов в ±2% от цены, ближние уровни весят больше (меньше спуфинга)."""
    try:
        ent = _ob_cache.get(coin)
        if ent and time.time() - ent[0] < 30:
            j = ent[1]
        else:
            j = await http_json("https://fapi.binance.com/fapi/v1/depth", params={"symbol": f"{coin}USDT", "limit": 100}, timeout=5)
            _ob_cache[coin] = (time.time(), j)
    except Exception as e:
        log.info(f"Стакан {coin}: {e}")
        return None
    def w(p):
        return max(0.2, 1 - abs(p - price) / price / 0.02)
    bids = sum(float(p) * float(q) * w(float(p)) for p, q in j.get("bids", []) if float(p) >= price * 0.98)
    asks = sum(float(p) * float(q) * w(float(p)) for p, q in j.get("asks", []) if float(p) <= price * 1.02)
    tot = bids + asks
    return round(bids / tot, 3) if tot else None


async def funding_trend(coin):
    """История funding (последние 4 периода, от старых к новым)."""
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        try:
            j = await http_json("https://api.bybit.com/v5/market/funding/history",
                                params={"category": "linear", "symbol": f"{cand}USDT", "limit": 4}, timeout=5)
            rates = [float(x["fundingRate"]) for x in j["result"]["list"]][::-1]
            if len(rates) >= 3:
                return rates
        except Exception:
            continue
    return []


_btc_move = {"ts": 0.0, "pct": 0.0}


async def btc_move_15m():
    """Изменение BTC за последние 15 минут (5-минутные свечи), кэш 60 секунд."""
    if time.time() - _btc_move["ts"] < 60:
        return _btc_move["pct"]
    pct = 0.0
    try:
        kl = await http_json("https://data-api.binance.vision/api/v3/klines",
                             params={"symbol": "BTCUSDT", "interval": "5m", "limit": 4}, timeout=5)
        pct = (float(kl[-1][4]) - float(kl[-3][1])) / float(kl[-3][1]) * 100
    except Exception as e:
        log.info(f"BTC 15m: {e}")
    _btc_move.update(ts=time.time(), pct=round(pct, 2))
    return _btc_move["pct"]


def detect_traps(df, lookback=20):
    """Ложные пробои и сбор ликвидности по последней ЗАКРЫТОЙ свече относительно диапазона lookback."""
    if df is None or len(df) < lookback + 2:
        return {}
    last = df.iloc[-1]
    prev = df.iloc[-lookback - 1:-1]
    hi_lvl, lo_lvl = float(prev["high"].max()), float(prev["low"].min())
    rng = float(last["high"] - last["low"])
    if rng <= 0:
        return {}
    body = abs(float(last["close"] - last["open"]))
    upper = float(last["high"] - max(last["open"], last["close"]))
    lower = float(min(last["open"], last["close"]) - last["low"])
    rv = float(last.get("rel_vol", 1.0))
    out = {"levels": (round(hi_lvl, 6), round(lo_lvl, 6)), "body_ratio": round(body / rng, 2), "rel_vol": round(rv, 2)}
    eps = max(body, rng * 0.05)
    # ложный пробой: фитиль за уровнем, тело вернулось внутрь диапазона на большом объёме
    if last["high"] > hi_lvl and last["close"] < hi_lvl and rv >= 1.3:
        out["fake_up"] = True
    if last["low"] < lo_lvl and last["close"] > lo_lvl and rv >= 1.3:
        out["fake_dn"] = True
    # сбор ликвидности: длинный фитиль за уровнем, закрытие обратно (без требования объёма)
    if last["low"] < lo_lvl and last["close"] > lo_lvl and lower >= 2 * eps and upper <= body + eps:
        out["sweep_dn"] = True
    if last["high"] > hi_lvl and last["close"] < hi_lvl and upper >= 2 * eps and lower <= body + eps:
        out["sweep_up"] = True
    return out


def session_info(now_msk):
    """Сессии по МСК: (название, поправка к уверенности)."""
    h, wd = now_msk.hour, now_msk.weekday()
    if wd >= 5:
        return "выходные", -4
    if h < 3:
        return "ночь (тонкий стакан)", -6
    if h < 10:
        return "Азия", -2
    if h < 16:
        return "Европа", 1
    return "Европа + США", 2


async def chandelier_stop(coin, is_long):
    """Chandelier Exit: максимум (минимум) 22 часовых свечей минус (плюс) 3 ATR."""
    rows = await hourly_rows(coin)
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    prev = df["close"].shift()
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev).abs(), (df["low"] - prev).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(22).mean().iloc[-1])
    if not atr or atr != atr:
        return None
    if is_long:
        return float(df["high"].tail(22).max() - 3 * atr)
    return float(df["low"].tail(22).min() + 3 * atr)


def weekly_trend(tf_data):
    """Недельный тренд из дневных свечей (без новых запросов): 1 бычий, −1 медвежий, 0 нет."""
    df = tf_data.get("1d", {}).get("df")
    if df is None or len(df) < 12 * 7:
        return 0
    closes = df["close"].values[::-1][::7][::-1]
    if len(closes) < 12:
        return 0
    ws = pd.Series(closes)
    e10 = ws.ewm(span=10, adjust=False).mean().iloc[-1]
    e20 = ws.ewm(span=20, adjust=False).mean().iloc[-1]
    last = closes[-1]
    if last > e10 > e20:
        return 1
    if last < e10 < e20:
        return -1
    return 0


def smc_context(tf_data, is_long):
    """FVG (имбаланс) и ордерблок на 1h: цена вернулась в зону — возможная точка входа по направлению."""
    h1 = tf_data.get("1h", {}).get("df")
    if h1 is None or len(h1) < 40:
        return 0, None
    d = h1.tail(60).reset_index(drop=True)
    hi, lo, op, cl = d["high"].values, d["low"].values, d["open"].values, d["close"].values
    px = cl[-1]
    n = len(d)
    for i in range(n - 1, max(1, n - 40), -1):
        if is_long and lo[i] > hi[i - 2] and hi[i - 2] <= px <= lo[i]:
            return 6, f"FVG 1h: цена вернулась в имбаланс {fmt(hi[i - 2])}–{fmt(lo[i])}"
        if (not is_long) and hi[i] < lo[i - 2] and lo[i - 2] >= px >= hi[i]:
            return 6, f"FVG 1h: цена вернулась в имбаланс {fmt(hi[i])}–{fmt(lo[i - 2])}"
    for j in range(n - 6, 20, -1):
        if is_long and cl[j] < op[j]:
            if cl[j + 1:j + 4].max() > hi[j - 20:j].max() and lo[j] <= px <= hi[j]:
                return 6, f"Ордерблок 1h {fmt(lo[j])}–{fmt(hi[j])}: откат к зоне импульса"
        if (not is_long) and cl[j] > op[j]:
            if cl[j + 1:j + 4].min() < lo[j - 20:j].min() and lo[j] <= px <= hi[j]:
                return 6, f"Ордерблок 1h {fmt(lo[j])}–{fmt(hi[j])}: откат к зоне импульса"
    return 0, None


async def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0.0, conf_threshold=CONF_FINAL):
    """Мульти-ТФ оценка и фильтры v2. valid=True только если прошли все гейты."""
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрочный",
           "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": [],
           "fake_breakout": False, "extras": {}}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data:
        out["reject"] = "Недостаточно данных по таймфреймам"
        return out
    wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / sum(TF_WEIGHTS[t] for t in tfs)
    is_long = wp >= 50
    side = 1 if is_long else -1
    strength = max(wp, 100 - wp)
    notes, pos_reasons = [], []

    def cs(p):
        return p if is_long else 100 - p

    p4, p1d = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]
    info = tf_data["4h"]
    h1 = tf_data.get("1h")
    reject = ""
    if (p4 >= 50) != is_long or (p1d >= 50) != is_long:
        reject = "4h и 1d смотрят в разные стороны"
    agree = sum(1 for t in tfs if cs(tf_data[t]["long_prob"]) >= 55)
    squeeze4 = bool(info.get("squeeze"))
    atr_cap = 0.09 if squeeze4 else 0.06  # высокий ATR после сжатия — ещё не перегрев
    ichi_now = int(info["df"].iloc[-1]["ichi_pos"]) if info.get("df") is not None else 0
    if not reject and info["adx"] < 18 and ichi_now == 0:
        reject = f"Пила: слабый тренд (ADX {info['adx']:.0f}) внутри облака Ишимоку"
    if not reject and info["adx"] < 15:
        reject = f"Вялый рынок (ADX {info['adx']:.0f})"
    if not reject and agree < min(3, len(tfs)):
        reject = f"Таймфреймы не согласны ({agree}/{len(tfs)})"
    if not reject and info["atr"] / info["price"] > atr_cap:
        reject = f"Слишком высокая волатильность (ATR > {atr_cap * 100:.0f}%)"

    if h1 and (h1["long_prob"] >= 50) != is_long and h1["confidence"] >= 65:
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

    # Обвал / памп BTC прямо сейчас (flash crash): альты летят вместе с ним
    btc15 = 0.0
    if coin != "BTC":
        btc15 = await btc_move_15m()
        if is_long and btc15 <= -2.5:
            reject = reject or f"BTC обвал {btc15:.1f}% за 15 минут: лонги запрещены"
        elif (not is_long) and btc15 >= 2.5:
            reject = reject or f"BTC памп +{btc15:.1f}% за 15 минут: шорты запрещены"
        elif (is_long and btc15 <= -1.5) or ((not is_long) and btc15 >= 1.5):
            strength -= 5
            notes.append(f"BTC резко против за 15 мин ({btc15:+.1f}%) (−5)")

    fng = ctx.get("fng")
    if fng is not None and ((fng >= 80 and is_long) or (fng <= 20 and not is_long)):
        strength -= 4
        notes.append(f"экстремальный {'страх' if fng <= 20 else 'жадность'} {fng} (−4)")

    # История funding: тренд за 3 периода важнее одного значения
    fr = await funding_trend(coin)
    if fr:
        rising = fr[-1] > fr[-2] > fr[-3]
        falling = fr[-1] < fr[-2] < fr[-3]
        if is_long:
            if rising and fr[-1] > 0.0003:
                strength -= 6
                notes.append(f"funding растёт 3 периода подряд ({fr[-1] * 100:.3f}%): пузырь лонгов (−6)")
            elif falling and fr[-1] < -0.0002:
                strength += 4
                pos_reasons.append("Funding уходит в минус: паника, шорты переплачивают")
        else:
            if rising and fr[-1] > 0.0003:
                strength += 4
                pos_reasons.append("Funding растёт: лонги переплачивают, перегрев лонгов")
            elif falling and fr[-1] < -0.0002:
                strength -= 6
                notes.append("funding 3 периода в минус: шорты переполнены, риск выкупа (−6)")

    # Матрица OI + цена
    oi_state, oi_pct, p_pct = await oi_price_state(coin)
    if oi_state:
        d = OI_DELTA[oi_state][0 if is_long else 1]
        strength += d
        notes.append(f"OI+цена: {OI_TXT[oi_state]} ({d:+d})")
        if d > 0:
            pos_reasons.append(OI_TXT[oi_state])

    # Стакан: перевес сил (вместо одиночной «стены»)
    imb = None
    if not reject and strength >= conf_threshold - 8:
        imb = await order_book_imbalance(coin, info["price"])
    if imb is not None:
        if (is_long and imb < 0.40) or ((not is_long) and imb > 0.60):
            strength -= 5
            notes.append(f"стакан: перевес {'продавцов' if is_long else 'покупателей'} ({imb:.0%}) (−5)")
        elif (is_long and imb > 0.60) or ((not is_long) and imb < 0.40):
            strength += 4
            pos_reasons.append(f"Стакан: перевес {'покупателей' if is_long else 'продавцов'} {max(imb, 1 - imb):.0%}")

    if onchain_change < -2.0 and is_long:
        strength -= 10
        notes.append(f"отток TVL {onchain_change}% против лонга (−10)")
    elif onchain_change > 2.0 and not is_long:
        strength -= 10
        notes.append(f"приток TVL {onchain_change}% против шорта (−10)")

    # Сессии по МСК вместо одного штрафа за ночь
    sess_name, sess_adj = session_info(datetime.now(timezone(timedelta(hours=3))))
    if sess_adj:
        strength += sess_adj
        notes.append(f"сессия: {sess_name} ({sess_adj:+d})")
    h1df = h1.get("df") if h1 else None
    if sess_name == "Азия" and h1df is not None and (h1df.iloc[-1]["brk_up"] or h1df.iloc[-1]["brk_dn"]):
        strength -= 3
        notes.append("Азия + пробой: такие пробои часто ложные (−3)")

    # Ложные пробои и сбор ликвидности (1h, а если нет — 4h)
    tdf = h1df if h1df is not None else info.get("df")
    traps = detect_traps(tdf) if tdf is not None else {}
    lv_hi, lv_lo = traps.get("levels", (0, 0))
    if traps.get("fake_up"):
        if is_long:
            strength -= 15
            notes.append(f"ложный пробой вверх у {lv_hi}: объём и тело не удержали уровень (−15)")
        else:
            strength += 5
            pos_reasons.append(f"Ложный пробой вверх у {lv_hi}: покупатели не удержали уровень")
    if traps.get("fake_dn"):
        if not is_long:
            strength -= 15
            notes.append(f"ложный пробой вниз у {lv_lo}: объём и тело не удержали уровень (−15)")
        else:
            strength += 5
            pos_reasons.append(f"Ложный пробой вниз у {lv_lo}: продавцы не удержали уровень")
    if traps.get("sweep_dn"):
        if is_long:
            strength += 8
            pos_reasons.append(f"Сбор ликвидности ниже {lv_lo} и выкуп: стопы розницы собраны")
        else:
            strength -= 4
            notes.append("сбор ликвидности вниз: против шорта (−4)")
    if traps.get("sweep_up"):
        if not is_long:
            strength += 8
            pos_reasons.append(f"Сбор ликвидности выше {lv_hi} и продажа: стопы лонгов собраны")
        else:
            strength -= 4
            notes.append("сбор ликвидности вверх: против лонга (−4)")
    out["fake_breakout"] = bool(traps.get("fake_up") or traps.get("fake_dn"))

    # Сжатие Боллинджера: импульс близко, направление по EMA 4h
    if squeeze4 and (info["ema20"] > info["ema50"]) == is_long:
        strength += 4
        pos_reasons.append("Сжатие Боллинджера на 4h: импульс близко, направление совпадает с EMA")

    smc_delta, smc_reason = smc_context(tf_data, is_long)
    if smc_delta:
        strength += smc_delta
        if smc_reason:
            pos_reasons.append(smc_reason)
    wk = weekly_trend(tf_data)
    if wk:
        if (wk == 1) == is_long:
            strength += 3
            pos_reasons.append("Недельный тренд совпадает с направлением")
        else:
            strength -= 6
            notes.append("недельный тренд против направления (−6)")

    nb = market_bias.get("bias", 0)
    news_status = "Нейтрально"
    if time.time() - market_bias.get("ts", 0) < 3 * 3600 and nb != 0:
        word = "бычий" if nb == 1 else "медвежий"
        if nb == side:
            strength += 7
            news_status = f"✅ Подтверждают ({word} фон)"
        else:
            strength -= 20
            news_status = f"❌ Противоречат ({word} фон)"

    strength = max(0.0, min(100.0, strength))
    if not reject and strength < conf_threshold:
        reject = f"Уверенность {strength:.0f}% ниже порога {conf_threshold:.0f}%"

    p15 = tf_data.get("15m", {}).get("long_prob", 50)
    p1h = h1["long_prob"] if h1 else 50
    fast = "15m" in tf_data and h1 is not None and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    reasons = (pos_reasons + collect_reasons(tf_data, is_long))[:6]
    out.update(
        valid=not reject, reject=reject, notes=notes, is_long=is_long, strength=round(strength, 1),
        type="Краткосрочный" if fast else "Долгосрочный" if long_term else "Среднесрочный",
        news_status=news_status, wp=wp, tf_probs={t: tf_data[t]["long_prob"] for t in tfs},
        reasons=reasons, patterns=pattern_lines(tf_data),
        extras={"oi": oi_state, "oi_pct": oi_pct, "price_pct": p_pct, "imbalance": imb, "funding": fr[-1] if fr else None,
                "btc15": btc15, "session": sess_name, "traps": {k: v for k, v in traps.items() if k != "levels"}})
    return out


def _parse_analyst(text):
    try:
        d = json.loads(re.search(r"\{.*\}", text or "", re.S).group(0))
        verdict, comment = str(d.get("verdict", "")).upper().strip(), str(d.get("comment", "")).strip()
        div = str(d.get("divergence", "none")).lower().strip()
        if verdict in ("ENTER", "WAIT") and has_cyr(comment, 15):
            return {"verdict": verdict, "risk": str(d.get("risk", "средний")).strip()[:20],
                    "divergence": div if div in ("bull", "bear") else "none", "comment": shorten(comment, 320)}
    except Exception:
        pass
    return None


async def ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data):
    """ИИ видит сырые OHLCV и RSI последних 30 свечей 4h: сам ищет дивергенции, вердикт ENTER/WAIT."""
    key = (coin, setup["is_long"])
    c = _ai_cache.get(key)
    if c and time.time() - c[0] < 1200:
        return c[1]
    df = tf_data["4h"]["df"].tail(30)
    block = "\n".join(f"{r.open:.6g} {r.high:.6g} {r.low:.6g} {r.close:.6g} v{r.volume:.0f} rsi{r.rsi:.0f}" for r in df.itertuples())
    bt_txt = f"{bt['trades']} сделок, TP1 {bt['winrate']}%, ср. R {bt['avg_r']:+.2f}, PF {bt['pf']}" if bt else "нет данных"
    prompt = f"""Ты крипто-трейдер. Проверь идею по СЫРЫМ данным 4h (последние 30 свечей, колонки: open high low close volume RSI).
{block}

Монета {coin}, направление {'LONG' if setup['is_long'] else 'SHORT'}, Conf {setup['strength']}%.
Вход {fmt(entry)}, TP1 {fmt(tps[0])}, стоп {fmt(sls[0])}. Бэктест 4h: {bt_txt}.
Выводы модели: {'; '.join(setup['reasons']) or 'нет'}. Фильтры: {'; '.join(setup['notes']) or 'нет'}.
Задачи: 1) есть ли дивергенция цены и RSI (новый максимум/минимум цены без подтверждения RSI)? 2) не прошёл ли уже большую часть движения? 3) решение.
Используй ТОЛЬКО цены из данных, не придумывай уровни.
Верни СТРОГО JSON без markdown: {{"verdict":"ENTER или WAIT","risk":"низкий или средний или высокий","divergence":"bull или bear или none","comment":"1-2 предложения по-русски"}}"""
    text, engine = await ask_ai_named(prompt, validate=lambda t: _parse_analyst(t) is not None, timeout=22, free_timeout=25)
    res = _parse_analyst(text) if text else None
    if res:
        res["engine"] = engine
        if res["divergence"] != "none":
            res["comment"] = shorten(f"Дивергенция {'бычья' if res['divergence'] == 'bull' else 'медвежья'} по RSI. {res['comment']}", 320)
    _ai_cache[key] = (time.time(), res)
    return res



async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    info = tf_data["4h"]
    is_long = setup["is_long"]
    entry, tps, sls, risk_dist, leverage, near, liq_x20 = calc_levels_and_risk(info, is_long)
    verdict, comment, risk_lvl, engine = "WAIT", "", "средний", ""
    if not setup["valid"]:
        comment = setup["reject"]
    else:
        ai = await ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data)
        if ai:
            verdict, comment, risk_lvl, engine = ai["verdict"], ai["comment"], ai["risk"], ai.get("engine", "")
        else:
            verdict, comment = local_verdict(setup, bt), "ИИ недоступен: решение по правилам модели и истории."
        if bt and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt):
            verdict, comment = "WAIT", "История этой монеты по такой стратегии слабая. " + comment
        if verdict == "ENTER" and near:
            comment = f"{comment} ⚠ {near}".strip()
    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception as e:
        log.info(f"График {coin}: {e}")
        img = None
    res = {
        "coin": coin, "ts": time.time(), "source": source, "img": img,
        "is_long": is_long, "side": "LONG" if is_long else "SHORT", "verdict": verdict, "type": setup["type"],
        "strength": setup["strength"], "regime": info["regime"], "news_status": setup["news_status"],
        "btc_txt": ctx.get("btc_txt", "нет данных"), "fng": ctx.get("fng"), "price": info["price"],
        "entry": entry, "tps": tps, "sls": sls, "risk_dist": risk_dist, "leverage": leverage,
        "reasons": setup["reasons"], "patterns": setup["patterns"], "notes": setup["notes"],
        "squeeze": info["squeeze"], "bt": bt, "comment": comment, "risk_lvl": risk_lvl, "engine": engine,
        "liq_x20": liq_x20, "fake_breakout": setup.get("fake_breakout", False),
        "long_prob": {t: round(tf_data[t]["long_prob"], 1) for t in tfs_list(tf_data)},
        "adx": round(info["adx"], 1), "rsi": round(info["rsi"], 1),
        "sr": info.get("sr", {}),
    }
    res["text"] = render_caption(res)
    return res


def tfs_list(tf_data):
    return [t for t in TFS if t in tf_data]


def fetch_error_text(coin):
    errs = fetch_errors.get(coin, [])
    tried = ", ".join(e["name"] for e in EXCHANGES)
    base = f"Монета <b>{html.escape(coin)}</b> не найдена ни на одной бирже ({tried})."
    return base + ("\n\n<i>Причины: " + html.escape("; ".join(errs[-4:])) + "</i>" if errs else "")


async def analyze_coin(coin, uid=None, force=False):
    """(res, error). Свежий анализ → архив → только цена → ошибка."""
    cached = analysis_cache.get(coin)
    if cached and not force and time.time() - cached[0] < cached[1]:
        return cached[2], None
    try:
        tf_data, source = await get_market_data(coin)
        ctx = await get_context()
        funding = await get_funding(coin)
        user_conf = CONF_FINAL
        if uid:
            user_conf = float(_users().get(str(uid), {}).get("conf_final", CONF_FINAL))
        setup = await evaluate_setup(coin, tf_data, ctx, funding, onchain_change=await get_onchain_context(), conf_threshold=user_conf)
        bt = await get_backtest(coin)
        res = await asyncio.wait_for(build_analysis(coin, tf_data, setup, bt, ctx, source), timeout=80)
        res["conf_used"] = user_conf
        analysis_cache[coin] = (time.time(), 60, res)
        save_analysis_db(coin, res)
        if len(analysis_cache) > 80:
            for k in sorted(analysis_cache, key=lambda x: analysis_cache[x][0])[:30]:
                analysis_cache.pop(k, None)
        return res, None
    except LookupError:
        pass
    except Exception as e:
        log.error(f"Ошибка анализа {coin}: {e}", exc_info=True)
    old = load_analysis_db(coin)
    if old and time.time() - old["ts"] < 12 * 3600:
        res = dict(old["data"], stale=True, img=None)
        res["text"] = render_caption(res)
        return res, None
    price = await get_simple_price(coin)
    if price:
        return {"coin": coin, "price": price, "price_only": True, "ts": time.time(), "img": None, "source": "API цен"}, None
    return None, "🚫 " + fetch_error_text(coin)


def remember_res(uid, coin, res):
    d = user_res.setdefault(uid, {})
    d[coin] = (time.time(), res)
    for k in sorted(d, key=lambda c: d[c][0])[:-8]:
        d.pop(k, None)


def get_user_res(uid, coin):
    v = user_res.get(uid, {}).get(coin)
    return v[1] if v and time.time() - v[0] < 1800 else None


# =====================================================================
# СИГНАЛЫ: выборка, сканер, симуляция, живой мониторинг
# =====================================================================
SIGNAL_TTL = 12 * 3600          # сигнал без входа живёт 12 часов
INVALID_ATR_MULT = 2.5          # цена ушла от входа дальше этого (в ATR) — вход потерян
signal_notify_log = {}          # coin:side -> ts отправки (анти-дубль уведомлений)


def get_active_signals():
    """Актуальные сигналы: один на монету+сторону, без протухших и аннулированных."""
    now = time.time()
    active, seen = [], set()
    for x in sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True):
        key = f"{x['coin']}_{x['side']}"
        if x.get("status") not in ("active", "in_trade") or key in seen:
            continue
        if x.get("status") == "active" and now - x.get("ts", 0) > SIGNAL_TTL:
            continue
        seen.add(key)
        active.append(x)
    return active


def signals_for(uid, category="Все"):
    items = get_active_signals()
    if category == "Все":
        mode = _users().get(str(uid), {}).get("trade_mode", "balanced") if uid else "balanced"
        return [x for x in items if x.get("type") in MODE_TYPES.get(mode, MODE_TYPES["balanced"])]
    if category.startswith("mode:"):
        return [x for x in items if x.get("type") in MODE_TYPES.get(category[5:], ())]
    return [x for x in items if x.get("type") == category]


async def get_scan_universe():
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
        qv = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    except Exception as e:
        log.info(f"Universe: {e}")
        return POPULAR_COINS[:SCAN_MAX_COINS]
    uni = []
    for c in POPULAR_COINS:
        for cand in SYMBOL_ALIASES.get(c, [c]):
            v = qv.get(f"{cand}USDT", 0)
            if v >= MIN_QV24:
                uni.append((c, v))
                break
    uni.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in uni[:SCAN_MAX_COINS]] or POPULAR_COINS[:SCAN_MAX_COINS]


def recent_results(n=20):
    hist = sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True)
    return [x for x in hist if x.get("status") in ("win", "loss")][:n]


def adaptive_conf_bonus():
    r = recent_results(20)
    if len(r) < 8:
        return 0
    wr = sum(1 for x in r if x["status"] == "win") / len(r)
    return 10 if wr < 0.35 else 6 if wr < 0.45 else 0


def streak_pause():
    r = recent_results(3)
    return len(r) == 3 and all(x["status"] == "loss" for x in r) and time.time() - r[0]["ts"] < 6 * 3600


def coin_recently_lost(coin, hours=24):
    now = time.time()
    return any(x["coin"] == coin and x.get("status") == "loss" and now - x.get("ts", 0) < hours * 3600
               for x in load_signal_history())


def set_blackout(secs):
    global news_blackout_until
    news_blackout_until = time.time() + secs


async def btc_shock():
    try:
        tf, _ = await get_market_data_light("BTC", ("1h",))
        last = tf["1h"]["df"].iloc[-1]
        return (last["high"] - last["low"]) > 2.5 * last["atr"]
    except Exception:
        return False


async def check_macro_calendar():
    """Важные новости USD (ФРС, CPI и т.п.) в окне ±30 минут."""
    try:
        async with http.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return False
            events = await r.json(content_type=None)
        now = datetime.now(timezone.utc)
        for event in events:
            if event.get("impact") == "High" and event.get("country") == "USD":
                diff = (datetime.fromisoformat(event["date"].replace("Z", "+00:00")) - now).total_seconds()
                if -1800 < diff < 1800:
                    log.warning(f"Макро-новость: {event.get('title')}")
                    return True
    except Exception as e:
        log.warning(f"Ошибка проверки календаря: {e}")
    return False


def signal_recipients(coin, side):
    """Кому отправлять уведомления о сигнале: включивших сигналы."""
    return [int(uid) for uid, u in list(_users().items())
            if isinstance(u, dict) and u.get("active", True) and u.get("notify_signals", True) and has_access(int(uid))]


async def scan_once(send=True):
    """Сканер: быстрый этап по 4h/1d, затем 1h/15m, бэктест, ИИ. Отсев с причинами."""
    if scan_lock.locked():
        return []
    async with scan_lock:
        t0 = time.time()
        if time.time() < news_blackout_until or streak_pause():
            return []
        if await check_macro_calendar():
            set_blackout(1800)
            await notify_admin("🏦 Макро-новость (ФРС/CPI): сканер на паузе 30 минут.")
            return []
        if await btc_shock():
            set_blackout(1800)
            await notify_admin("⚠ Шок BTC: сканер на паузе 30 минут.")
            return []

        if time.time() - market_bias["ts"] > 900:
            await update_market_bias()
        ctx = await get_context(force=True)
        onchain = await get_onchain_context()
        universe = await get_scan_universe()
        # Порог сканера = самый мягкий среди получателей, остальных отфильтруем при рассылке
        scan_conf = min([CONF_FINAL] + [float(u.get("conf_final", CONF_FINAL)) for u in _users().values()
                                        if isinstance(u, dict) and u.get("active", True) and u.get("notify_signals", True)])
        rej = {}

        def _rej(k):
            rej[k] = rej.get(k, 0) + 1

        sem = asyncio.Semaphore(3)
        stage1, stage2, final, bt_pass = [], [], [], 0

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
                await asyncio.sleep(0.15)

        await asyncio.gather(*[s1(c) for c in universe])
        stage1 = sorted(stage1, key=lambda x: x[2], reverse=True)[:30]

        async def s2(item):
            sc, tf2, _ = item
            if coin_recently_lost(sc):
                _rej("недавний убыток по монете")
                return
            async with sem:
                tf_data = dict(tf2)
                try:
                    extra, _src = await get_market_data_light(sc, ("1h", "15m"))
                    tf_data.update(extra)
                except Exception:
                    pass
                setup = await evaluate_setup(sc, tf_data, ctx, await get_funding(sc), scan=True,
                                             onchain_change=onchain, conf_threshold=scan_conf + adaptive_conf_bonus())
                if setup["valid"]:
                    stage2.append((sc, tf_data, setup))
                else:
                    _rej(setup["reject"][:28])

        await asyncio.gather(*[s2(i) for i in stage1])

        sem3 = asyncio.Semaphore(2)

        async def s3(item):
            nonlocal bt_pass
            coin, tf_data, setup = item
            async with sem3:
                bt = await get_backtest(coin)
                if bt is not None and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt):
                    _rej("бэктест слабый")
                    return
                bt_pass += 1
                try:
                    res = await build_analysis(coin, tf_data, setup, bt, ctx, "scan")
                except Exception as e:
                    log.info(f"Сборка {coin}: {e}")
                    return
                if res["verdict"] == "ENTER":
                    final.append(res)
                    analysis_cache[coin] = (time.time(), 60, res)
                    save_analysis_db(coin, res)
                else:
                    _rej("ИИ: ЖДАТЬ")

        await asyncio.gather(*[s3(i) for i in stage2])
        final.sort(key=lambda r: r["strength"], reverse=True)
        last_scan_info.update(ts=int(time.time()), stage1=len(stage1), stage2=len(stage2), backtest_ok=bt_pass,
                              final=len(final), secs=int(time.time() - t0))
        state_touch(False)
        log.info(f"Сканер: {len(universe)} → {len(stage1)} → {len(stage2)} → бэктест {bt_pass} → итог {len(final)}")

        new_items = []
        if send:
            now = time.time()
            hist = load_signal_history()
            busy = {f"{x['coin']}:{x['side']}" for x in hist if x.get("status") in ("active", "in_trade")}
            used_groups = set()
            for r in final:
                key = f"{r['coin']}:{r['side']}"
                if key in busy or now - signal_sent_at.get(f"{r['coin']}:{'L' if r['is_long'] else 'S'}", 0) < SIGNAL_COOLDOWN:
                    continue
                if len(new_items) >= 3 or abs(r["entry"] - r["price"]) / r["price"] > 0.03:
                    continue
                grp = next((i for i, g in enumerate(CORRELATION_GROUPS) if r["coin"] in g), None)
                if grp is not None:
                    if (grp, r["side"]) in used_groups:
                        continue
                    used_groups.add((grp, r["side"]))
                signal_sent_at[f"{r['coin']}:{'L' if r['is_long'] else 'S'}"] = now
                item = {"ts": now, "expire_ts": now + SIGNAL_TTL, "coin": r["coin"], "side": r["side"], "type": r["type"],
                        "strength": r["strength"], "entry": r["entry"], "tp1": r["tps"][0], "tp2": r["tps"][1],
                        "tp3": r["tps"][2], "sl1": r["sls"][0], "sl2": r["sls"][1], "status": "active",
                        "max_tp_hit": 0, "hit_sl": 0, "price": r["price"], "sent_to": [], "is_long": r["is_long"],
                        "tf_probs": r.get("tf_probs") or {}, "text": r["text"], "reasons": [re.sub(r"\s*\(.*\)$", "", s) for s in r.get("reasons", [])][:6], "hour": datetime.now(timezone(timedelta(hours=3))).hour, "daily": False, "mfe": 0.0, "mae": 0.0}
                hist.append(item)
                new_items.append((item, r))
            if new_items:
                save_signal_history(hist)
                stats["signals"] += len(new_items)
                for item, r in new_items:
                    await deliver_new_signal(item, r)
        if send:
            top = "; ".join(f"{k}: {v}" for k, v in sorted(rej.items(), key=lambda kv: -kv[1])[:5])
            await notify_admin(f"🔍 <b>Скан завершён</b> за {last_scan_info['secs']} с\n"
                               f"Монет: {len(universe)} → {len(stage1)} → {len(stage2)} · бэктест {bt_pass} · ✅ <b>{len(final)}</b>\n"
                               f"🧾 Отсев: {top or '—'}")
        gc.collect()
        return final


def webapp_link(coin=None, tab=None):
    base = WEBAPP_URL or f"https://t.me/{(runtime['main_bot'] or '').lstrip('@') or 'ixcly'}"
    q = []
    if coin:
        q.append(f"coin={coin}")
    if tab:
        q.append(f"tab={tab}")
    return base + (("?" + "&".join(q)) if q else "")


def simulate_signal(x, rows, now=None):
    """Статус по 15m свечам: ожидание входа лимиткой → в сделке → TP1-3 с трейлингом."""
    now = now or time.time()
    is_long = x["side"] == "LONG"
    entry, tp1, tp2, tp3, sl = x["entry"], x["tp1"], x["tp2"], x["tp3"], x["sl1"]
    t0 = x["ts"] * 1000
    state, max_tp = "waiting", 0
    for ts, _o, h, l, _c, _v in rows:
        if ts < t0:
            continue
        if state == "waiting":
            if (is_long and h >= tp1 and l > entry) or ((not is_long) and l <= tp1 and h < entry):
                return "missed", 0, 0
            if (l <= entry) if is_long else (h >= entry):
                state = "in_trade"
                if (l <= sl) if is_long else (h >= sl):
                    return "loss", 0, 1
            continue
        if (l <= sl) if is_long else (h >= sl):
            return ("win" if max_tp >= 1 else "loss"), max_tp, 1
        lvl = (3 if h >= tp3 else 2 if h >= tp2 else 1 if h >= tp1 else 0) if is_long else \
              (3 if l <= tp3 else 2 if l <= tp2 else 1 if l <= tp1 else 0)
        if lvl > max_tp:
            max_tp = lvl
            sl = entry if max_tp == 1 else tp1
            if max_tp == 3:
                return "win", 3, 0
    if state == "waiting":
        return ("expired", 0, 0) if now > x.get("expire_ts", 0) else ("active", 0, 0)
    if now > x["ts"] + 7 * 86400:
        return ("win" if max_tp >= 1 else "expired"), max_tp, 0
    return "in_trade", max_tp, 0


async def notify_signal_holders(coin, side, text, kb=None, extra_uids=()):
    """Уведомление тем, кто получил сигнал или держит по нему позицию / сохранил."""
    uids = set(int(x) for x in extra_uids)
    for uid, u in list(_users().items()):
        if not isinstance(u, dict):
            continue
        if coin in (u.get("open_positions") or {}) or any(s.get("coin") == coin for s in u.get("saved_signals", [])):
            uids.add(int(uid))
    for uid in uids:
        try:
            await _send_new(uid, text, kb or kb_nav(back=False), None)
            await asyncio.sleep(0.05)
        except Exception as e:
            log.info(f"notify {uid}: {e}")


def live_check_signal(x, live):
    """Правила ликвидности сигнала по свежему анализу. Возвращает причину или None."""
    price = live["price"]
    is_long = x["side"] == "LONG"
    sign = 1 if is_long else -1
    atr = live.get("atr") or price * 0.01
    if sign * (price - x["sl1"]) <= 0:
        return f"Цена пробила стоп-уровень ({fmt(x['sl1'])}) до входа — структура сломана."
    if abs(price - x["entry"]) > INVALID_ATR_MULT * atr and x.get("status") == "active":
        return f"Цена ушла на {abs(price - x['entry']) / atr:.1f} ATR от входа, вход потерян."
    p4, p1h = live["long_prob"].get("4h", 50), live["long_prob"].get("1h", 50)
    cs4 = p4 if is_long else 100 - p4
    cs1h = p1h if is_long else 100 - p1h
    if cs4 < 45 and cs1h < 40:
        return "Тренд развернулся против сигнала (4h и 1h). Идея больше не работает."
    return None


async def live_refresh_coin(coin):
    """Живое обновление одной монеты: свежие свечи, цена, вероятности. Пишет в live-кэш."""
    try:
        tf_data, source = await get_market_data_light(coin, ("15m", "1h", "4h", "1d"))
    except Exception as e:
        log.info(f"live {coin}: {e}")
        return None
    live = {"ts": time.time(), "price": tf_data["4h"]["price"], "atr": tf_data["4h"]["atr"],
            "long_prob": {t: round(tf_data[t]["long_prob"], 1) for t in tf_data},
            "rsi": {t: round(tf_data[t]["rsi"], 1) for t in tf_data},
            "adx": round(tf_data["4h"]["adx"], 1), "regime": tf_data["4h"]["regime"], "source": source}
    LIVE[coin] = live
    return live


LIVE = {}


async def live_monitor_loop():
    """Каждые 30 с: обновляет данные по каждой активной монете и проверяет ликвидность сигналов."""
    await asyncio.sleep(40)
    while True:
        try:
            hist = load_signal_history()
            open_coins = {c for u in _users().values() if isinstance(u, dict)
                          for c in (u.get("open_positions") or {})}
            watch = {x["coin"] for x in hist if x.get("status") in ("active", "in_trade")} | open_coins
            changed = False
            for coin in sorted(watch)[:40]:
                live = await live_refresh_coin(coin)
                await asyncio.sleep(0.2)
                if not live:
                    continue
                for x in hist:
                    if x["coin"] != coin or x.get("status") not in ("active", "in_trade"):
                        continue
                    x["live_price"] = live["price"]
                    x["live_ts"] = live["ts"]
                    if x.get("status") == "active" and time.time() - x["ts"] > SIGNAL_TTL:
                        x["status"], changed = "expired", True
                        continue
                    reason = live_check_signal(x, live)
                    if reason:
                        await invalidate_signal(x, reason)
                        changed = True
                    else:
                        changed = True
            if changed:
                save_signal_history(hist)
        except Exception as e:
            log.error(f"live_monitor: {e}", exc_info=True)
        next_timers["global_signals"] = time.time() + 30
        await asyncio.sleep(30)


async def scanner_loop():
    await asyncio.sleep(45)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner: {e}", exc_info=True)
            await notify_admin(f"🚨 <b>Ошибка сканера:</b>\n<code>{html.escape(str(e))}</code>")
        next_timers["scanner"] = time.time() + SCAN_INTERVAL
        await asyncio.sleep(SCAN_INTERVAL)


# =====================================================================
# ПОЗИЦИИ, ЛИЧНАЯ СТАТИСТИКА, СОПРОВОЖДЕНИЕ
# =====================================================================
def open_tracked_position(uid, coin, res):
    """Ставит сделку на сопровождение: лимитка ждёт входа, затем TP/SL/трейлинг и итог в статистику."""
    if get_open_position(uid, coin):
        return False
    set_user_open_position(uid, coin, {
        "side": res["side"], "entry_price": res["entry"], "created_at": time.time(), "entered_at": time.time(),
        "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1],
        "sl_orig": res["sls"][0], "risk_pct": get_user_risk_settings(uid)["risk_pct"],
        "notified_close": False, "waiting_entry": True, "peak_pnl": 0.0})
    return True


def record_trade_result(uid, coin, pos, exit_price, reason):
    """Итог сделки: TP1 = 50% позиции, TP2 = 30%, остаток по цене выхода."""
    if pos.get("waiting_entry"):
        return None
    entry, side = pos["entry_price"], pos["side"]
    sign = 1 if side == "LONG" else -1
    pnl_at = lambda p: sign * (p - entry) / entry * 100
    h3 = pos.get("hit_tp3")
    h2 = pos.get("hit_tp2") or h3
    h1 = pos.get("hit_tp1") or h2
    parts, rest = [], 1.0
    if h1:
        parts.append((0.5, pnl_at(pos["tp1"])))
        rest -= 0.5
    if h2:
        parts.append((0.3, pnl_at(pos["tp2"])))
        rest -= 0.3
    parts.append((rest, pnl_at(exit_price)))
    pnl = sum(w * p for w, p in parts) - FEE_PCT
    pnl -= (max(0.0, time.time() - pos.get("entered_at", time.time())) / 3600 / 8) * 0.01  # средний фандинг, оценка
    dist = abs(entry - pos.get("sl_orig", pos["sl1"])) / entry * 100
    r = pnl / dist if dist > 0 else 0.0
    s = get_user_risk_settings(uid)
    now = time.time()
    rec = {"coin": coin, "side": side, "entry": entry, "exit": exit_price, "pnl": round(pnl, 2), "r": round(r, 2),
           "usd": round(s["deposit"] * s["risk_pct"] / 100 * r, 2),
           "result": "win" if (h1 or pnl > 0.05) else "loss" if pnl < -0.05 else "be",
           "reason": reason, "max_tp": 3 if h3 else 2 if h2 else 1 if h1 else 0,
           "peak": round(pos.get("peak_pnl", 0.0), 2), "opened_at": pos.get("entered_at", now), "closed_at": now,
           "hold_h": round((now - pos.get("entered_at", now)) / 3600, 1)}
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(uid), {"active": True})
        u["trade_log"] = (u.get("trade_log", []) + [rec])[-300:]
    state_touch()
    return rec


def render_trade_card(rec):
    icon = {"win": "✅", "loss": "❌", "be": "➖"}[rec["result"]]
    return (f"{icon} <b>Итог сделки: {rec['coin']} ({rec['side']})</b>\n"
            f"Вход: <code>{fmt(rec['entry'])}</code> → Выход: <code>{fmt(rec['exit'])}</code>\n"
            f"Результат: <b>{rec['pnl']:+.2f}%</b> · {rec['r']:+.2f}R · {rec['usd']:+.2f}$\n"
            f"Удержание: {rec['hold_h']} ч · взято целей: {rec['max_tp']} · {rec['reason']}")


async def close_position_manual(uid, coin):
    pos = get_open_position(uid, coin)
    rec = None
    if pos and not pos.get("waiting_entry"):
        price = await get_simple_price(coin)
        if price:
            rec = record_trade_result(uid, coin, pos, price, "закрыто вручную")
    set_user_open_position(uid, coin, None)
    if rec:
        await _tell(uid, render_trade_card(rec))
    return rec


def personal_stats(uid):
    t = list(_users().get(str(uid), {}).get("trade_log", []))
    n = len(t)
    if not n:
        return {"n": 0}
    wins = sum(1 for x in t if x["result"] == "win")
    losses = sum(1 for x in t if x["result"] == "loss")
    closed = wins + losses
    gp = sum(x["pnl"] for x in t if x["pnl"] > 0)
    gl = -sum(x["pnl"] for x in t if x["pnl"] < 0)
    kind, streak = t[-1]["result"], 0
    for x in reversed(t):
        if x["result"] != kind:
            break
        streak += 1
    coins = {}
    for x in t:
        coins[x["coin"]] = coins.get(x["coin"], 0) + x["pnl"]
    rk = sorted(coins.items(), key=lambda kv: kv[1])

    def swr(sd):
        s = [x for x in t if x["side"] == sd and x["result"] != "be"]
        return round(sum(1 for x in s if x["result"] == "win") / len(s) * 100) if s else None

    acc, dd, peak = 0.0, 0.0, 0.0
    for x in t:
        acc += x["usd"]
        peak = max(peak, acc)
        dd = max(dd, peak - acc)
    eq, run = [], 0.0
    for x in t[-40:]:
        run += x["usd"]
        eq.append(round(run, 2))
    return {"n": n, "wins": wins, "losses": losses, "be": n - closed,
            "wr": round(wins / closed * 100, 1) if closed else 0.0,
            "pnl": round(sum(x["pnl"] for x in t), 2), "usd": round(sum(x["usd"] for x in t), 2),
            "avg_r": round(sum(x["r"] for x in t) / n, 2), "pf": round(gp / gl, 2) if gl else None,
            "streak": streak, "streak_kind": kind, "best": max(t, key=lambda x: x["pnl"]),
            "worst": min(t, key=lambda x: x["pnl"]),
            "top": [c for c in reversed(rk) if c[1] > 0][:3], "bottom": [c for c in rk if c[1] < 0][:3],
            "wr_long": swr("LONG"), "wr_short": swr("SHORT"), "max_dd": round(dd, 2),
            "expectancy": round((wins * (gp / wins if wins else 0) - losses * (gl / losses if losses else 0)) / closed, 2) if closed else 0.0,
            "avg_hold": round(sum(x["hold_h"] for x in t) / n, 1), "equity": eq, "last": t[-10:][::-1]}


def render_mystats(uid):
    s = personal_stats(uid)
    if not s["n"]:
        return ("📈 <b>Моя статистика</b>\n\n<i>Пока нет закрытых сделок. Сохраните сигнал или нажмите «Вошёл в сделку» — "
                "итог запишется сам, когда сделка закроется.</i>")
    kind = {"win": "побед", "loss": "убытков", "be": "в ноль"}[s["streak_kind"]]
    dash = lambda v: "—" if v is None else v
    lines = ["📈 <b>Моя статистика</b>\n",
             f"Сделок: <b>{s['n']}</b> · ✅ {s['wins']} · ❌ {s['losses']} · ➖ {s['be']}",
             f"🎯 Винрейт: <b>{s['wr']}%</b> · PF: <b>{dash(s['pf'])}</b> · Мат. ожидание: <b>{s['expectancy']:+.2f}R</b>",
             f"💵 Итого: <b>{s['usd']:+.2f}$</b> ({s['pnl']:+.2f}%) · ср. {s['avg_r']:+.2f}R",
             f"📉 Макс. просадка: <b>{s['max_dd']:.2f}$</b>",
             f"🔥 Серия: {s['streak']} {kind} подряд",
             f"↕ Лонг {dash(s['wr_long'])}% · Шорт {dash(s['wr_short'])}%",
             f"⏱ Среднее удержание: {s['avg_hold']} ч", "",
             f"🏆 Лучшая: {s['best']['coin']} {s['best']['pnl']:+.2f}%",
             f"💀 Худшая: {s['worst']['coin']} {s['worst']['pnl']:+.2f}%"]
    if s["top"]:
        lines.append("➕ Лучшие: " + ", ".join(f"{c} {p:+.1f}%" for c, p in s["top"]))
    if s["bottom"]:
        lines.append("➖ Слабые: " + ", ".join(f"{c} {p:+.1f}%" for c, p in s["bottom"]))
    lines += ["", "<b>Последние сделки:</b>"] + [
        f"{'✅' if x['result'] == 'win' else '❌' if x['result'] == 'loss' else '➖'} {x['coin']} {x['side']}: "
        f"<code>{fmt(x['entry'])}</code> → <code>{fmt(x['exit'])}</code> · {x['pnl']:+.2f}%" for x in s["last"][:6]]
    return "\n".join(lines)


async def check_position(user_id, coin, pos):
    """Сопровождение сделки: вход лимиткой, безубыток, TP1-3, трейлинг, предупреждения, итог."""
    if pos.get("notified_close"):
        return
    price = await get_simple_price(coin)
    if not price:
        return
    side, entry = pos["side"], pos["entry_price"]
    sign = 1 if side == "LONG" else -1
    pnl = sign * (price - entry) / entry * 100

    if pos.get("waiting_entry"):
        newer = next((x for x in get_active_signals() if x["coin"] == coin and x["side"] == side
                      and abs(x["entry"] - entry) / entry > 0.003), None)
        if newer and pos.get("warned_entry") != round(newer["entry"], 8):
            pos["warned_entry"] = round(newer["entry"], 8)
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"⚠ <b>Лимит сдвинулся: {coin} ({side})</b>\nВаш ордер: <code>{fmt(entry)}</code>\n"
                                 f"Новый сигнал по монете: вход <code>{fmt(newer['entry'])}</code>.\n"
                                 f"Ваш ордер не обновлён автоматически: проверьте его на бирже.")
        if sign * (price - entry) <= 0:
            pos["waiting_entry"] = False
            pos["entered_at"] = time.time()
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"🎯 <b>Лимитный ордер сработал: {coin}</b>\n\nНаправление: <b>{side}</b>\n"
                                 f"Цена входа: <code>{fmt(entry)}</code>\n🚀 Сделка на сопровождении.",
                        kb_nav([[InlineKeyboardButton(text="📈 Открыть", web_app=WebAppInfo(url=webapp_link(coin)))]], back=False)
                        if WEBAPP_URL else None)
        elif time.time() - pos.get("created_at", time.time()) > 24 * 3600:
            set_user_open_position(user_id, coin, None)
            await _tell(user_id, f"⌛ <b>Ордер отменён: {coin}</b>\nЦена не дошла до входа за 24 часа.")
        return

    pos["peak_pnl"] = max(pos.get("peak_pnl", 0.0), pnl)
    for k in ("hit_tp1", "hit_tp2", "hit_tp3"):
        pos.setdefault(k, False)
    current_sl = pos["sl1"]
    reason, urgency, exit_price, why = None, "info", price, "закрытие"

    if not pos["hit_tp1"] and pnl >= 1.5 and current_sl != entry:
        pos["sl1"] = entry
        current_sl = entry
        set_user_open_position(user_id, coin, pos)
        await _tell(user_id, f"🛡 <b>Безубыток: {coin}</b>\nЦена в плюсе на {pnl:.2f}%. Стоп перенесён на вход.")

    if pos["hit_tp1"] and not pos["hit_tp3"]:
        ch = await chandelier_stop(coin, side == "LONG")
        if ch:
            new_sl = max(ch, entry) if side == "LONG" else min(ch, entry)
            better = new_sl > current_sl if side == "LONG" else new_sl < current_sl
            if better:
                pos["sl1"] = current_sl = new_sl
                set_user_open_position(user_id, coin, pos)

    if pnl <= -5.0 and not pos.get("notified_urgent"):
        pos["notified_urgent"] = True
        set_user_open_position(user_id, coin, pos)
        await _tell(user_id, f"🚨 <b>{coin} идёт против вас:</b> {pnl:+.2f}%. Будьте готовы к стопу.")

    tp1, tp2, tp3 = pos["tp1"], pos["tp2"], pos["tp3"]
    if sign * (price - current_sl) <= 0:
        reason = ("🛑 Сработал трейлинг-стоп, часть прибыли зафиксирована." if current_sl != pos.get("sl_orig") and pos.get("hit_tp1")
                  else "🛑 Сработал стоп-лосс.")
        exit_price, urgency = current_sl, "success" if pos.get("hit_tp1") else "danger"
        pos["notified_close"] = True
        why = "стоп / трейлинг"
    elif sign * (price - tp3) >= 0 and not pos["hit_tp3"]:
        reason, urgency = "🏆 Достигнут TP3. Сделка полностью закрыта.", "success"
        pos["hit_tp3"] = True
        exit_price, pos["notified_close"], why = tp3, True, "TP3"
    elif sign * (price - tp2) >= 0 and not pos["hit_tp2"]:
        reason, urgency = "✅ Достигнут TP2. Стоп подтянут на TP1.", "success"
        pos["hit_tp2"] = True
        pos["sl1"] = tp1
        set_user_open_position(user_id, coin, pos)
    elif sign * (price - tp1) >= 0 and not pos["hit_tp1"]:
        reason, urgency = "✅ Достигнут TP1. Фиксируем 50%, стоп в безубыток.", "success"
        pos["hit_tp1"] = True
        pos["sl1"] = entry
        set_user_open_position(user_id, coin, pos)

    if not reason and not pos.get("notified_close") and time.time() - pos.get("last_warn", 0) > 3600:
        warn = None
        live = LIVE.get(coin)
        if live and time.time() - live["ts"] < 300:
            p1h = live["long_prob"].get("1h", 50)
            p15 = live["long_prob"].get("15m", 50)
            against = (lambda p: 100 - p) if side == "LONG" else (lambda p: p)
            if against(p1h) >= 60 and against(p15) >= 58:
                warn = "Рынок разворачивается против сделки (1h и 15m)."
        if not warn and pos["peak_pnl"] >= 1.0 and pnl <= pos["peak_pnl"] * 0.5 and not pos["hit_tp1"]:
            warn = f"Отдана половина прибыли (пик {pos['peak_pnl']:+.1f}%, сейчас {pnl:+.1f}%)."
        if warn:
            pos["last_warn"] = time.time()
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"⚠️ <b>Рекомендую закрыть: {coin} ({side})</b>\n\n{warn}\n"
                                 f"Сейчас: <code>{fmt(price)}</code> · PnL <b>{pnl:+.2f}%</b>")

    if reason:
        text = (f"{'🏆' if urgency == 'success' else '🛑'} <b>Сопровождение: {coin} ({side})</b>\n"
                f"Вход: <code>{fmt(entry)}</code> | Сейчас: <code>{fmt(price)}</code> | PnL <b>{pnl:+.2f}%</b>\n\n{reason}")
        kb = None
        if pos.get("notified_close"):
            rec = record_trade_result(user_id, coin, pos, exit_price, why)
            set_user_open_position(user_id, coin, None)
            if rec:
                text += "\n\n" + render_trade_card(rec)
                share = f"Я закрыл {coin} ({side}) с результатом {rec['pnl']:+.2f}% в IXCLY PRO! 🔥"
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📈 Моя статистика", callback_data="mystats")],
                    [InlineKeyboardButton(text="📤 Поделиться", url=f"https://t.me/share/url?text={quote(share)}")]])
        else:
            set_user_open_position(user_id, coin, pos)
        await _tell(user_id, text, kb)
    else:
        set_user_open_position(user_id, coin, pos)


async def position_monitor_loop():
    await asyncio.sleep(50)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                if not has_access(int(uid_str)):
                    continue
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    try:
                        await check_position(int(uid_str), coin, pos)
                    except Exception as e:
                        log.info(f"check_position {coin}: {e}")
                    await asyncio.sleep(0.3)
        except Exception as e:
            log.error(f"pos monitor: {e}")
        next_timers["positions"] = time.time() + 60
        await asyncio.sleep(60)


async def _tell(uid, text, kb=None):
    try:
        await _send_new(uid, trim_html(text, 4000), kb or kb_nav(back=False), None)
    except Exception as e:
        log.info(f"notify {uid}: {e}")


async def daily_digest_loop():
    """Ежедневная сводка в 21:00 МСК: итоги дня по каждому пользователю с закрытыми сделками."""
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=21, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        day_start = (datetime.now(timezone(timedelta(hours=3))).replace(hour=0, minute=0, second=0, microsecond=0)).timestamp()
        for uid_str, u in list(_users().items()):
            if not isinstance(u, dict) or not get_user_notif_settings(uid_str)["pos_updates"]:
                continue
            today = [x for x in u.get("trade_log", []) if x["closed_at"] >= day_start]
            if not today:
                continue
            usd = sum(x["usd"] for x in today)
            wins = sum(1 for x in today if x["result"] == "win")
            await _tell(int(uid_str), f"🌙 <b>Итоги дня</b>\nСделок: {len(today)} · ✅ {wins} · ❌ {len(today) - wins}\n"
                                      f"Результат: <b>{usd:+.2f}$</b>")
        await asyncio.sleep(60)


# =====================================================================
# НОВОСТИ ПРОТИВ ПОЗИЦИЙ, АЛЕРТЫ, БРИФИНГ
# =====================================================================
_news_alerted = set()


async def check_news_conflicts_with_signals():
    nb, link = market_bias.get("bias", 0), market_bias.get("link")
    if nb == 0 or not link:
        return
    for uid_str, u in list(_users().items()):
        if not isinstance(u, dict) or not get_user_notif_settings(uid_str)["signals"]:
            continue
        for coin, pos in (u.get("open_positions") or {}).items():
            if pos.get("waiting_entry") or pos.get("notified_close"):
                continue
            against = (nb == 1 and pos["side"] == "SHORT") or (nb == -1 and pos["side"] == "LONG")
            if not against or (link, uid_str, coin) in _news_alerted:
                continue
            _news_alerted.add((link, uid_str, coin))
            if re.search(rf"\b{re.escape(coin)}\b", f"{market_bias.get('title_ru', '')} {market_bias.get('coins', '')}".upper()):
                await _tell(int(uid_str), f"🚨 <b>Новость касается {coin} против вашей сделки ({pos['side']})</b>\n\n"
                                          f"📰 {html.escape(shorten(market_bias.get('title_ru', ''), 120))}\n"
                                          f"Рассмотрите закрытие позиции.")
    if len(_news_alerted) > 2000:
        _news_alerted.clear()


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
                    state_touch(False)
                    await update_market_bias(force=True)
                    ttl_news = (getattr(entries[0], "title", "") or "")
                    if CRITICAL_NEWS.search(ttl_news.lower()):
                        set_blackout(3600)
                        await notify_admin(f"🚨 Критичная новость: сигналы на паузу 1 час.\n{ttl_news}")
                    await check_news_conflicts_with_signals()
                    if bot_flags["news_enabled"] and active_chats:
                        text, img_url = await prepare_news()
                        if text:
                            img = await download_bytes(img_url) if img_url else None
                            sent = 0
                            for cid in list(active_chats):
                                if not get_user_notif_settings(cid)["news"]:
                                    continue
                                try:
                                    await _send_new(cid, text, kb_nav([[InlineKeyboardButton(text="🗑 Скрыть", callback_data="del_msg")]], back=False), img)
                                    sent += 1
                                    await asyncio.sleep(0.05)
                                except Exception:
                                    pass
                            stats["news_sent"] += sent
        except Exception as e:
            log.warning(f"news_watcher: {e}")
        next_timers["news_watcher"] = time.time() + 600
        await asyncio.sleep(600)
        
        
async def broadcast_market_alert(title, reason, advice, pause_mins=60):
    """Умная рассылка о волатильности/паузе для обычных пользователей."""
    text = (f"<b>{title}</b>\n\n"
            f"📝 <b>Что произошло:</b>\n{reason}\n\n"
            f"💡 <b>Что делать:</b>\n{advice}\n\n"
            f"⏳ <i>Сканер торговых сигналов приостановлен на {pause_mins} мин. для вашей безопасности.</i>")
    
    # Кнопка, чтобы сразу открыть приложение
    kb = kb_nav([[InlineKeyboardButton(text="📈 Открыть приложение", web_app=WebAppInfo(url=webapp_link()))]], back=False) if WEBAPP_URL else None
    
    # Отправляем только тем, у кого включены сигналы (или новости)
    targets = [uid for uid in active_chats if get_user_notif_settings(uid)["signals"]]
    
    sent = 0
    for cid in targets:
        try:
            await _send_new(cid, text, kb, None)
            sent += 1
            await asyncio.sleep(0.05) # Защита от лимитов Telegram
        except Exception as e:
            pass
            
    log.info(f"Оповещение о волатильности отправлено {sent} пользователям.")


async def price_alerts_loop():
    await asyncio.sleep(60)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                alerts = u.get("price_alerts", []) if isinstance(u, dict) else []
                if not alerts:
                    continue
                triggered = []
                for alert in alerts:
                    price = await get_simple_price(alert["coin"])
                    if not price:
                        continue
                    if (alert["op"] == ">" and price >= alert["price"]) or (alert["op"] == "<" and price <= alert["price"]):
                        triggered.append(alert)
                        await _tell(int(uid_str), f"🔔 <b>Алерт сработал: {alert['coin']}</b>\n"
                                                  f"Условие: <code>{alert['op']} {fmt(alert['price'])}</code>\n"
                                                  f"Текущая цена: <code>{fmt(price)}</code>",
                                    kb_nav([[InlineKeyboardButton(text="📈 Открыть", web_app=WebAppInfo(url=webapp_link(alert['coin'])))]], back=False)
                                    if WEBAPP_URL else None)
                if triggered:
                    with STATE_LOCK:
                        u["price_alerts"] = [a for a in alerts if a not in triggered]
                    state_touch()
        except Exception as e:
            log.error(f"price_alerts_loop: {e}")
        await asyncio.sleep(120)


async def morning_briefing_loop():
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=8, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        try:
            ctx = await get_context(force=True)
            fng = ctx.get("fng")
            text = (f"☀️ <b>Доброе утро!</b>\n\n🌍 <b>BTC:</b> {ctx.get('btc_txt')}\n"
                    f"😨 <b>F&G:</b> {fng if fng is not None else '—'} {ctx.get('fng_txt', '')}\n"
                    f"🔥 <b>Активных сигналов:</b> {len(get_active_signals())}\n\n<i>Удачной торговли!</i>" + ad_block())
            for cid in list(active_chats):
                if get_user_notif_settings(cid)["briefing"]:
                    try:
                        await _send_new(cid, text, kb_nav([[InlineKeyboardButton(text="📈 Открыть", web_app=WebAppInfo(url=webapp_link(tab="signals")))]], back=False)
                                        if WEBAPP_URL else None, None)
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"Ошибка утреннего брифинга: {e}")
        await asyncio.sleep(60)


# =====================================================================
# ЛИКВИДНОСТЬ СИГНАЛОВ ПРИ ОБНОВЛЕНИИ И ПОЛНОЕ ОБНОВЛЕНИЕ ДАННЫХ
# =====================================================================
refresh_job = {"running": False, "done": 0, "total": 0, "removed": 0, "ts": 0.0}


async def drop_waiting_signals(coin, res):
    """Если анализ монеты сейчас даёт «ЖДАТЬ» — сигнал по ней снимается с уведомлением."""
    if res.get("price_only") or res.get("verdict") != "WAIT" or res.get("stale"):
        return 0
    n = 0
    hist = load_signal_history()
    for x in hist:
        if x["coin"] == coin and x.get("status") == "active":
            await invalidate_signal(x, f"Обновлённый анализ: рекомендация «ЖДАТЬ». {html.escape(shorten(res.get('comment', ''), 160))}")
            n += 1
    if n:
        save_signal_history(hist)
    return n


async def refresh_all_data(uid):
    """Пересчёт всех монет пользователя (сигналы, избранное, позиции) с нуля."""
    coins = [x["coin"] for x in get_active_signals()] + [s["coin"] for s in get_user_saved_signals(uid)] \
        + list(get_user_open_positions(uid))
    coins = list(dict.fromkeys(coins))[:40]
    before = len(get_active_signals())
    refresh_job.update(running=True, done=0, total=len(coins), removed=0)
    analysis_cache.clear()
    _funding_cache.clear()
    _ai_cache.clear()
    _ctx_cache["ts"] = 0
    _defillama_cache["ts"] = 0
    _news_cache["ts"] = 0
    for c in coins:
        bt_cache.pop(c, None)
    sem = asyncio.Semaphore(3)

    async def one(c):
        async with sem:
            try:
                res, _ = await asyncio.wait_for(analyze_coin(c, uid=uid, force=True), 120)
                if res and not res.get("price_only"):
                    remember_res(uid, c, res)
                    await drop_waiting_signals(c, res)
                await live_refresh_coin(c)
            except Exception as e:
                log.info(f"refresh {c}: {e}")
            refresh_job["done"] += 1

    try:
        await get_context(force=True)
        await update_market_bias()
        await asyncio.gather(*[one(c) for c in coins])
    finally:
        refresh_job.update(running=False, ts=time.time(), removed=max(0, before - len(get_active_signals())))


# =====================================================================
# АНОМАЛЬНЫЙ ОБЪЁМ
# =====================================================================
async def anomaly_scan_once():
    j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
    vol = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    coins = [c for c in POPULAR_COINS[:150] if vol.get(f"{c}USDT", 0) >= 10_000_000]
    found = []
    sem = asyncio.Semaphore(4)

    async def check(coin):
        async with sem:
            try:
                k = await http_json("https://data-api.binance.vision/api/v3/klines",
                                    params={"symbol": f"{coin}USDT", "interval": "1m", "limit": 22}, timeout=6)
            except Exception:
                return
        if len(k) < 22:
            return
        closed = k[:-1]
        last, base = closed[-1], closed[:-1]
        avg = float(np.mean([float(x[7]) for x in base]))
        qv = float(last[7])
        if avg > 0 and qv / avg >= 10 and qv >= 250_000:
            found.append((coin, qv, qv / avg, (float(last[4]) / float(last[1]) - 1) * 100))

    await asyncio.gather(*[check(c) for c in coins])
    now = time.time()
    fresh = sorted([a for a in found if now - anomaly_sent_at.get(a[0], 0) > 2 * 3600], key=lambda a: a[2], reverse=True)[:3]
    for coin, qv, ratio, chg in fresh:
        anomaly_sent_at[coin] = now
        state_touch(False)
        text = (f"🚨 <b>Аномальный объём: {coin}</b>\nОборот за минуту: <b>${qv:,.0f}</b> (×{ratio:.0f})\n"
                f"Цена за минуту: <b>{chg:+.2f}%</b>")
        for cid in list(active_chats):
            if get_user_notif_settings(cid)["anomalies"]:
                await _tell(cid, text, webapp_kb(coin))


async def anomaly_scanner_loop():
    await asyncio.sleep(90)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await anomaly_scan_once()
        except Exception as e:
            log.info(f"anomaly: {e}")
        next_timers["anomaly"] = time.time() + 180
        await asyncio.sleep(180)


# =====================================================================
# MINI APP: API
# =====================================================================
def tg_auth(init):
    """Проверка подписи initData от Telegram (HMAC-SHA256)."""
    try:
        pairs = dict(parse_qsl(init, keep_blank_values=True))
        got = pairs.pop("hash")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(hmac.new(secret, check.encode(), hashlib.sha256).hexdigest(), got):
            return None
        if time.time() - int(pairs.get("auth_date", 0)) > 86400:
            return None
        return json.loads(pairs["user"])
    except Exception:
        return None


@app.route("/app")
def mini_page():
    return send_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp.html"))


API_CACHE_TTL = {"me": 4, "signals": 4, "portfolio": 4, "stats": 5, "mystats": 5}
API_CACHE = {}


# --- API ДЛЯ ОПЛАТЫ В WEBAPP ---
from aiogram.types import LabeledPrice

@app.route("/api/create_stars_invoice", methods=["POST"])
def api_create_stars_invoice():
    d = request.get_json(silent=True) or {}
    u = tg_auth(d.get("initData", ""))
    if not u:
        return Response('{"error":"Откройте из Telegram"}', 401, mimetype="application/json")
    
    try:
        days = int(d.get("days", 30))
        if days not in PLANS:
            return jsonify({"error": "Неверный тариф"})
            
        price = PLANS[days]
        title = PLAN_TITLE[days]

        async def _create():
            # Создаем ссылку на счет для Telegram Stars
            return await bot.create_invoice_link(
                title=f"IXCLY PRO · {title}",
                description=f"Доступ к сигналам и аналитике на {days} дней",
                payload=f"plan:{days}",
                currency="XTR", # XTR - это звезды
                prices=[LabeledPrice(label=title, amount=price)],
                provider_token="" 
            )
            
        link = asyncio.run_coroutine_threadsafe(_create(), MAIN_LOOP).result(10)
        return jsonify({"link": link})
        
    except Exception as e:
        log.error(f"Ошибка создания счета: {e}")
        return jsonify({"error": "Ошибка сервера при создании счета"})


@app.route("/api/<act>", methods=["POST"])
def mini_api(act):
    d = request.get_json(silent=True) or {}
    u = tg_auth(d.get("initData", ""))
    if not u:
        return Response('{"error":"Откройте приложение из Telegram"}', 401, mimetype="application/json")
    
    key = (u["id"], act, json.dumps({k: v for k, v in d.items() if k != "initData"}, sort_keys=True, default=str))
    
    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
    # Если это админ, ставим ttl = 0 (кэш выключен). Иначе берем стандартный TTL.
    ttl = 0 if u["id"] in ADMIN_IDS else API_CACHE_TTL.get(act)
    
    if ttl and key in API_CACHE and time.time() - API_CACHE[key][0] < ttl:
        return Response(API_CACHE[key][1], mimetype="application/json")
    
    try:
        out = asyncio.run_coroutine_threadsafe(mini_dispatch(u["id"], u, act, d), MAIN_LOOP).result(120)
    except (ValueError, TypeError):
        out = {"error": "Проверьте введённые данные"}
    except Exception as e:
        log.warning(f"mini_api {act}: {e}")
        out = {"error": "Ошибка сервера, попробуйте ещё раз"}
        
    body = json.dumps(out, ensure_ascii=False, default=str)
    
    # Сохраняем в кэш только если ttl > 0 (то есть для обычных юзеров)
    if ttl and not (isinstance(out, dict) and out.get("error")):
        API_CACHE[key] = (time.time(), body)
        if len(API_CACHE) > 500:
            API_CACHE.clear()
            
    return Response(body, mimetype="application/json")


def _sig_item(x, live=True):
    item = {k: x.get(k) for k in ("coin", "side", "type", "strength", "entry", "tp1", "tp2", "tp3", "sl1", "status", "ts",
                                  "max_tp_hit", "cancel_reason", "live_price")}
    item["sent"] = len(x.get("sent_to", []))
    if live and x.get("coin") in LIVE:
        lv = LIVE[x["coin"]]
        item["live_price"] = lv["price"]
        item["live_age"] = int(time.time() - lv["ts"])
        item["live_long_prob"] = lv["long_prob"]
    dist = None
    if item.get("live_price") and item.get("entry"):
        dist = round((item["live_price"] - item["entry"]) / item["entry"] * 100, 2)
    item["dist_pct"] = dist
    return item


def _enrich_user_positions(uid):
    out = {}
    for c, p in get_user_open_positions(uid).items():
        lv = LIVE.get(c)
        price = (lv["price"] if lv else None) or p["entry_price"]
        sign = 1 if p["side"] == "LONG" else -1
        out[c] = dict(p, price=price, pnl=round(sign * (price - p["entry_price"]) / p["entry_price"] * 100, 2))
    return out


async def coin_payload(uid, coin, force=False):
    if ANALYZE_SEM is not None:
        async with ANALYZE_SEM:
            res, err = await analyze_coin(coin, uid=uid, force=force)
    else:
        res, err = await analyze_coin(coin, uid=uid, force=force)
    if err:
        return {"error": strip_tags(err)[:300]}
    remember_res(uid, coin, res)
    if res.get("price_only"):
        return {"price_only": True, "price": res["price"], "coin": coin}
    out = {k: v for k, v in res.items() if k not in ("img", "text", "reasons_raw")}
    out["img"] = ("data:image/png;base64," + base64.b64encode(res["img"]).decode()) if res.get("img") else None
    s = get_user_risk_settings(uid)
    out["risk_usdt"] = s["deposit"] * s["risk_pct"] / 100
    out["pos_usdt"] = out["risk_usdt"] / res["risk_dist"] * res["entry"] if res["risk_dist"] > 0 else 0.0
    out["in_fav"] = any(x["coin"] == coin for x in get_user_saved_signals(uid))
    out["position"] = bool(get_open_position(uid, coin))
    out["live"] = LIVE.get(coin)
    pos_now = get_open_position(uid, coin)
    sig_now = next((x for x in get_active_signals() if x["coin"] == coin), None)
    cur_sl = (pos_now or {}).get("sl1") or (sig_now or {}).get("sl1")
    orig_sl = (pos_now or {}).get("sl_orig") or (sig_now or {}).get("sl1")
    out["current_sl"] = cur_sl
    out["trailing"] = bool(cur_sl and orig_sl and abs(cur_sl - orig_sl) > 1e-12)
    out["signal_active"] = any(x["coin"] == coin for x in get_active_signals())
    out["corr"] = None
    for oc, od in get_user_open_positions(uid).items():
        if oc != coin and od["side"] == res["side"] and any(oc in g and coin in g for g in CORRELATION_GROUPS):
            out["corr"] = f"Уже открыт {od['side']} по {oc}: монеты коррелируют"
    return out


async def mini_dispatch(uid, u, act, d):
    _users().setdefault(str(uid), {"active": True})["last_seen"] = time.time()
    if act not in FREE_ACTS and not has_access(uid):
        return {"error": access_closed_text(uid), "closed": True, "access": access_payload(uid),
                "bot": runtime["main_bot"], "ads_support": ads_get()["support"]}
    if uid not in active_chats:
        active_chats.add(uid)
        await asyncio.to_thread(save_user_data, uid, u.get("username"), u.get("first_name"))
    uu = _users().get(str(uid), {})
    coin = normalize_ticker(str(d.get("coin", ""))) if d.get("coin") else None

    if act == "me":
        mode = uu.get("trade_mode", "balanced")
        return {"name": u.get("first_name") or "трейдер", "version": VERSION, "risk": get_user_risk_settings(uid),
                "mode": mode, "conf": MODE_CONF.get(mode, CONF_FINAL), "notif": get_user_notif_settings(uid),
                "saved": [_sig_item(s, False) for s in get_user_saved_signals(uid)],
                "alerts": uu.get("price_alerts", []), "webhook": uu.get("webhook_url", ""),
                "news": market_bias, "ctx": await get_context(), "coins": POPULAR_COINS[:150], "access": access_payload(uid), "bot": runtime["main_bot"],
                "counts": {"signals": len(signals_for(uid)), "saved": len(get_user_saved_signals(uid)),
                           "positions": len(get_user_open_positions(uid))}}

    if act == "signals":
        flt = d.get("flt", "Все")
        keys = ("Все", "Краткосрочный", "Среднесрочный", "Долгосрочный", "mode:aggressive", "mode:balanced", "mode:conservative")
        return {"items": [_sig_item(x) for x in signals_for(uid, flt)[:50]],
                "counts": {k: len(signals_for(uid, k)) for k in keys},
                "refresh": dict(refresh_job), "market": market_bias.get("impact", "")}

    if act == "refresh_all":
        if not refresh_job["running"]:
            asyncio.create_task(refresh_all_data(uid))
        return {"started": True}

    if act == "refresh_status":
        return dict(refresh_job)

    if act == "price" and coin:
        STREAM_COINS[coin] = time.time()
        p = PRICES.get(coin)
        out = {"price": p["price"] if p else None, "ts": p["ts"] if p else None}
        pos = uu.get("open_positions", {}).get(coin)
        if pos and p:
            sign = 1 if pos["side"] == "LONG" else -1
            out["pnl"] = round(sign * (p["price"] - pos["entry_price"]) / pos["entry_price"] * 100, 2)
            out["sl"] = pos.get("sl1")
            out["trailing"] = abs(pos.get("sl1", 0) - pos.get("sl_orig", pos.get("sl1", 0))) > 1e-12
        return out

    if act == "ref_link":
        cnt = sum(1 for x in _users().values() if isinstance(x, dict) and x.get("referrer") == uid)
        return {"link": f"https://t.me/{runtime['main_bot'].lstrip('@')}?start=ref_{uid}", "count": cnt}

    if act == "promo_apply":
        return {"msg": apply_promo(uid, str(d.get("code", "")))}

    if act == "coin" and coin:
        if d.get("force") and time.time() - COIN_RL.get(uid, 0) < 5:
            return {"error": "Подождите 5 секунд перед пересчётом"}
        if d.get("force"):
            COIN_RL[uid] = time.time()
        return await coin_payload(uid, coin, force=bool(d.get("force")))

    if act == "live" and coin:
        lv = LIVE.get(coin)
        if not lv or time.time() - lv["ts"] > 30:
            lv = await live_refresh_coin(coin)
        return {"live": lv}

    if act == "movers":
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        mv = [(x["symbol"][:-4], float(x["priceChangePercent"])) for x in j
              if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
        up = sorted([m for m in mv if m[1] > 0], key=lambda m: -m[1])[:10]
        dn = sorted([m for m in mv if m[1] < 0], key=lambda m: m[1])[:10]
        return {"up": up, "down": dn}

    if act == "portfolio":
        return {"risk": get_user_risk_settings(uid), "open_risk": calculate_portfolio_risk(uid), "mode": uu.get("trade_mode", "balanced"),
                "positions": _enrich_user_positions(uid), "saved": [_sig_item(s, False) for s in get_user_saved_signals(uid)],
                "history": uu.get("trade_history", [])[-15:]}

    if act == "mystats":
        return personal_stats(uid)

    if act == "stats":
        uniq, seen = [], set()
        for x in sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True):
            if f"{x['coin']}_{x['side']}" not in seen:
                seen.add(f"{x['coin']}_{x['side']}")
                uniq.append(x)
        cnt = {s: sum(1 for x in uniq if x.get("status") == s) for s in ("win", "missed", "loss", "expired", "cancelled")}
        cnt["active"] = len(get_active_signals())
        lists = {s: [_sig_item(x, False) for x in uniq if x.get("status") == s][:30] for s in ("win", "missed", "loss", "expired", "cancelled")}
        lists["active"] = [_sig_item(x) for x in get_active_signals()][:30]
        closed = cnt["win"] + cnt["loss"]
        return {"counts": cnt, "lists": lists, "total": len(uniq),
                "wr": cnt["win"] / closed * 100 if closed else 0.0}

    if act == "news":
        return {"bias": market_bias, "html": await get_news_batch_text()}

    if act == "toggle" and d.get("key") in NOTIFY_NAMES:
        k = d["key"]
        new = not uu.get(f"notify_{k}", True)
        set_user_field(uid, **{f"notify_{k}": new})
        return {"msg": f"{NOTIFY_NAMES[k]}: {'вкл' if new else 'выкл'}"}

    if act == "set_deposit":
        v = float(d.get("value", 0))
        if v <= 0:
            return {"error": "Депозит должен быть больше 0"}
        set_user_field(uid, deposit=v)
        return {"msg": "Депозит сохранён"}

    if act == "set_risk":
        v = float(d.get("value", 0))
        if not 0.1 <= v <= 10:
            return {"error": "Риск от 0.1 до 10%"}
        set_user_field(uid, risk_pct=v)
        return {"msg": "Риск сохранён"}

    if act == "set_mode" and d.get("value") in MODE_NAMES:
        m = d["value"]
        set_user_field(uid, trade_mode=m, conf_final=MODE_CONF[m])
        return {"msg": "Режим обновлён"}

    if act == "fav_add" and coin:
        res = get_user_res(uid, coin) or (await analyze_coin(coin, uid=uid))[0]
        if not res or res.get("price_only"):
            return {"error": "Нет данных по монете"}
        await asyncio.to_thread(add_user_saved_signal, uid, coin, render_caption(res, uid), res)
        open_tracked_position(uid, coin, res)
        return {"msg": f"{coin} сохранён и поставлен на сопровождение"}

    if act == "fav_del" and coin:
        await asyncio.to_thread(del_user_saved_signal, uid, coin)
        return {"msg": "Удалено из избранного"}

    if act == "pos_enter" and coin:
        res = get_user_res(uid, coin) or (await analyze_coin(coin, uid=uid))[0]
        if not res or res.get("price_only"):
            return {"error": "Данные устарели, обновите анализ"}
        for oc, od in get_user_open_positions(uid).items():
            if oc != coin and od.get("side") == res["side"] and any(oc in g and coin in g for g in CORRELATION_GROUPS):
                return {"error": f"Уже открыт {od['side']} по {oc}: {coin} сильно коррелирует"}
        if not open_tracked_position(uid, coin, res):
            return {"error": "Сделка по монете уже открыта"}
        await _tell(uid, f"⏳ <b>Лимитный ордер: {coin} ({res['side']})</b>\nЖдём вход: <code>{fmt(res['entry'])}</code>\n"
                         f"Сопровождение включено: вход, TP1-3, стоп и итог придут сюда.")
        return {"msg": "Ордер поставлен, ждём вход"}

    if act == "pos_close" and coin:
        rec = await close_position_manual(uid, coin)
        return {"msg": f"Закрыто: {rec['pnl']:+.2f}%" if rec else "Позиция закрыта"}

    if act == "alert_add" and coin:
        price = float(d.get("price", 0))
        if price <= 0:
            return {"error": "Неверная цена"}
        cur = await get_simple_price(coin)
        op = d.get("op") or (">" if cur is None or price >= cur else "<")
        with STATE_LOCK:
            lst = STATE["users"].setdefault(str(uid), {"active": True}).setdefault("price_alerts", [])
            if len(lst) >= 20:
                return {"error": "Максимум 20 алертов"}
            lst.append({"coin": coin, "op": op, "price": price})
        state_touch()
        return {"msg": "Алерт создан"}

    if act == "alert_del":
        with STATE_LOCK:
            lst = STATE["users"].get(str(uid), {}).get("price_alerts", [])
            i = int(d.get("idx", -1))
            if 0 <= i < len(lst):
                lst.pop(i)
        state_touch()
        return {"msg": "Удалено"}

    if act == "webhook":
        url = str(d.get("url", "")).strip()
        if url and not url.startswith("http"):
            return {"error": "Ссылка должна начинаться с http"}
        set_user_field(uid, webhook_url=url)
        return {"msg": "Вебхук сохранён" if url else "Вебхук отключён"}

    if act == "csv":
        await bot.send_document(uid, BufferedInputFile(generate_csv_history(), "signals_history.csv"), caption="📊 История сигналов (CSV)")
        return {"msg": "Файл отправлен в чат"}

    return {"error": "Неизвестное действие"}


# =====================================================================
# БОТ: /start, уведомления, кнопки
# =====================================================================
def bot_main_keyboard():
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🚀 Открыть IXCLY App", web_app=WebAppInfo(url=webapp_link()))])
    rows.append([InlineKeyboardButton(text="🔥 Сигналы", web_app=WebAppInfo(url=webapp_link(tab="signals")))] if WEBAPP_URL else
                [InlineKeyboardButton(text="🔥 Сигналы", callback_data="noop")])
    rows.append([InlineKeyboardButton(text="🔔 Уведомления", callback_data="notif:menu")])
    rows.append([InlineKeyboardButton(text="📄 Документы", callback_data="show_docs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


WELCOME_BOT = ("👋 <b>IXCLY PRO</b>\n\nВся работа теперь в мини-приложении: сигналы, графики, портфель, статистика.\n"
               "Сюда я присылаю уведомления: новые сигналы, сопровождение сделок, итоги и снятые идеи.")


def notif_keyboard(uid):
    s = get_user_notif_settings(uid)
    rows = [[InlineKeyboardButton(text=f"{'🟢' if s[k] else '🔴'} {NOTIFY_NAMES[k]}", callback_data=f"notif:{k}")]
            for k in ("signals", "pos_updates", "news", "anomalies", "briefing")]
    rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="notif:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


ANALYZE_SEM = None
DOCS_PRIVACY = "https://telegra.ph/POLITIKA-KONFIDENCIALNOSTI-IXCLY-PRO-10-31"
DOCS_OFFER = "https://telegra.ph/PUBLICHNAYA-OFERTA-POLZOVATELSKOE-SOGLASHENIE-IXCLY-PRO-10-31"
DISCOUNT_OFFERS = {}
CRITICAL_NEWS = re.compile(r"hack|exploit|\bsec\b|lawsuit|delist|halt|bankrupt|arrest|sanction|взлом|запрет")


@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    parts = (m.text or "").split()
    if len(parts) > 1 and parts[1].startswith("ref_") and parts[1][4:].isdigit():
        ref = int(parts[1][4:])
        if ref != uid and "referrer" not in _users().get(str(uid), {}):
            set_user_field(uid, referrer=ref)
    await asyncio.to_thread(save_user_data, uid, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    try:
        await m.delete()
    except Exception:
        pass
    await welcome_for(m.chat.id, m.from_user.id)


from PIL import ImageFilter, ImageChops

# =====================================================================
# КАРТОЧКИ СИГНАЛОВ (PNG в стиле «КОЛЛ ДНЯ»)
# =====================================================================
GOLD = (250, 190, 40)


def _pfmt(v):
    v = float(v)
    if abs(v) >= 1000:
        return f"{v:,.0f}".replace(",", " ")
    return fmt(v)
TEXT_GREY = (130, 137, 165)


def _font(size, bold=False):
    try:
        return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans%s.ttf" % ("-Bold" if bold else ""), size)
    except Exception:
        return ImageFont.load_default()


def _tw(draw, text, font):
    try:
        return draw.textlength(text, font=font)
    except Exception:
        return font.getsize(text)[0]


def _fit(draw, text, font, maxw):
    t = text
    while t and _tw(draw, t, font) > maxw:
        t = t[:-2]
    return t if t == text else (t.rstrip() + "…")


def _dash(d, x0, x1, y, color, w=2, dash=14, gap=10):
    x = x0
    while x < x1:
        d.line((x, y, min(x + dash, x1), y), fill=color, width=w)
        x += dash + gap


def _closes_plot(d, closes, box, levels):
    """Линия цены и уровни. levels: [(значение, подпись, цвет, пунктир)]."""
    x0, y0, x1, y1 = box
    vals = list(closes) + [v for v, *_ in levels]
    vmin, vmax = min(vals) * 0.998, max(vals) * 1.002
    rng = (vmax - vmin) or 1.0

    def ym(v):
        return y1 - (v - vmin) / rng * (y1 - y0)

    if len(closes) >= 2:
        n = len(closes)
        pts = [(x0 + i * (x1 - x0) / (n - 1), ym(v)) for i, v in enumerate(closes)]
        d.polygon(pts + [(x1, y1), (x0, y1)], fill=(22, 27, 52))
        d.line(pts, fill=(96, 136, 255), width=4)
        lx, ly = pts[-1]
    else:
        lx, ly = x1, ym(levels[0][0])
    font = _font(23, True)
    for v, label, color, dashed in levels:
        y = ym(v)
        if dashed:
            _dash(d, x0, x1, y, color)
        else:
            d.line((x0, y, x1, y), fill=color, width=3)
        tw = _tw(d, f"{label} {_pfmt(v)}", font)
        d.rounded_rectangle((x1 + 22, y - 19, x1 + 22 + tw + 22, y + 19), radius=18, fill=color)
        d.text((x1 + 33, y - 15), f"{label} {_pfmt(v)}", fill=(10, 11, 18), font=font)
    return lx, ly, ym


def render_signal_card(x, closes, price, title="СИГНАЛ"):
    """Карточка: заголовок, цена входа, график с уровнями, почему, «Ошибаюсь, если», футер."""
    W, H = 1024, 1280
    long = x["is_long"]
    accent = (45, 226, 198) if long else (255, 90, 110)
    img = Image.new("RGB", (W, H), (10, 11, 18))
    glow = Image.new("RGB", (W, H), (0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((520, -260, 1320, 520), fill=(12, 70, 62) if long else (70, 16, 30))
    glow = glow.filter(ImageFilter.GaussianBlur(120))
    img = ImageChops.add(img, glow)
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, W, 8), fill=GOLD)

    msk = datetime.now(timezone(timedelta(hours=3)))
    f_badge, f_small, f_head = _font(30, True), _font(28), _font(64, True)
    bw = _tw(d, f"{title} · {x['coin']}", f_badge) + 52
    d.rounded_rectangle((58, 50, 58 + bw, 110), radius=30, outline=GOLD, width=3)
    d.text((84, 62), f"{title} · {x['coin']}", fill=GOLD, font=f_badge)
    dt = msk.strftime("%d.%m · %H:%M")
    d.text((966 - _tw(d, dt, f_small), 66), dt, fill=TEXT_GREY, font=f_small)

    d.text((58, 150), f"{x['coin']} {'пойдёт вверх' if long else 'пойдёт вниз'}", fill=(245, 247, 255), font=f_head)
    d.text((58, 232), _pfmt(x["entry"]), fill=accent, font=_font(150, True))
    d.text((58, 404), f"{'лонг' if long else 'шорт'} лимит от {_pfmt(x['entry'])} · сейчас {_pfmt(price or x['entry'])}",
           fill=TEXT_GREY, font=_font(30))

    levels = [(x["entry"], "вход", accent, False), (x["tp1"], "цель", accent, True), (x["sl1"], "отмена", (150, 150, 165), True)]
    lx, ly, ym = _closes_plot(d, closes or [], (58, 470, 680, 780), levels)
    d.ellipse((lx - 9, ly - 9, lx + 9, ly + 9), fill=(255, 255, 255))
    d.line((lx + 14, ly, 680, ym(x["tp1"])), fill=accent, width=3)

    d.rounded_rectangle((58, 815, 966, 1050), radius=24, fill=(16, 19, 32), outline=(40, 46, 70), width=2)
    d.text((88, 835), "ПОЧЕМУ", fill=TEXT_GREY, font=_font(22, True))
    reasons = [re.sub(r"\s*\(.*\)$", "", r) for r in (x.get("reasons") or [])][:4] or ["Совпали тренд, импульс и объём на 4h/1d"]
    f_r = _font(26)
    for i, r in enumerate(reasons):
        y = 878 + i * 42
        d.ellipse((88, y, 120, y + 32), outline=GOLD, width=2)
        d.text((97 if i < 9 else 92, y + 4), str(i + 1), fill=GOLD, font=_font(20, True))
        d.text((140, y + 3), _fit(d, r, f_r, 966 - 140 - 36), fill=(235, 238, 250), font=f_r)

    d.rounded_rectangle((58, 1075, 966, 1155), radius=24, fill=(30, 14, 20), outline=(120, 40, 60), width=2)
    inv = f"Ошибаюсь, если {'уйдём ниже' if long else 'уйдём выше'} {_pfmt(x['sl1'])}"
    d.text((88, 1095), inv, fill=(250, 240, 244), font=_font(34, True))

    d.ellipse((58, 1190, 128, 1260), outline=GOLD, width=3)
    d.text((76, 1207), "IX", fill=GOLD, font=_font(26, True))
    d.text((150, 1204), "IXCLY PRO", fill=(245, 247, 255), font=_font(36, True))
    foot = f"Conf {x['strength']}% · {x['type']}"
    d.text((966 - _tw(d, foot, _font(26)), 1210), foot, fill=TEXT_GREY, font=_font(26))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


async def _closes(coin):
    rows = await fetch_history(coin, "4h", 60, min_rows=20)
    return [r[4] for r in rows][-60:] if rows else []


def signal_caption(x, uid, title="СИГНАЛ"):
    s = get_user_risk_settings(uid)
    risk_usd = s["deposit"] * s["risk_pct"] / 100
    dist = abs(x["entry"] - x["sl1"]) / x["entry"] * 100
    pos = risk_usd / (dist / 100) if dist > 0 else 0
    head = f"🔥 <b>{title}</b>\n" if title != "СИГНАЛ" else ""
    e = "🟢" if x["is_long"] else "🔴"
    return (f"{head}{e} <b>{x['side']} · {html.escape(x['coin'])}</b> · {x['type']} · Conf <b>{x['strength']}%</b>\n"
            f"📥 Вход <b>лимит</b> <code>{fmt(x['entry'])}</code>\n"
            f"🎯 TP1 <code>{fmt(x['tp1'])}</code> · TP2 <code>{fmt(x['tp2'])}</code> · TP3 <code>{fmt(x['tp3'])}</code>\n"
            f"🛡 Отмена <code>{fmt(x['sl1'])}</code> (−{dist:.2f}%)\n"
            f"💼 Для вашего депозита: позиция ~<b>${pos:,.0f}</b> (≈ {pos / x['entry']:.4g} {html.escape(x['coin'])}), риск <b>${risk_usd:.0f}</b>\n"
            f"<i>Не финансовая рекомендация. Вход только лимитом.</i>")


def signal_keyboard(x):
    s = "L" if x["is_long"] else "S"
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="📈 Открыть в приложении", web_app=WebAppInfo(url=webapp_link(x["coin"])))])
    rows.append([InlineKeyboardButton(text="✅ Вошёл", callback_data=f"in:{s}:{x['coin']}"),
                 InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sv:{s}:{x['coin']}")])
    rows.append([InlineKeyboardButton(text="🗑 Скрыть", callback_data="del_msg")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def webapp_kb(coin):
    rows = [[InlineKeyboardButton(text="📈 Открыть в приложении", web_app=WebAppInfo(url=webapp_link(coin)))]] if WEBAPP_URL else []
    return InlineKeyboardMarkup(inline_keyboard=rows + [[InlineKeyboardButton(text="🗑 Скрыть", callback_data="del_msg")]])


async def deliver_new_signal(item, r, title="СИГНАЛ"):
    """Карточка сигнала (PNG) + персональная подпись каждому, кто включил сигналы и чей порог ниже силы."""
    closes = await _closes(item["coin"])
    card = await asyncio.to_thread(render_signal_card, item, closes, r.get("price") or item["entry"], title)
    for cid in signal_recipients(item["coin"], item["side"]):
        u = _users().get(str(cid), {})
        if item["strength"] < float(u.get("conf_final", CONF_FINAL)):
            continue
        try:
            await _send_new(cid, signal_caption(item, cid, title), signal_keyboard(item), card)
            item["sent_to"].append(cid)
            await asyncio.sleep(0.05)
        except Exception as e:
            log.info(f"Ошибка отправки сигнала {cid}: {e}")
    state_touch()
    asyncio.create_task(send_webhook_all({"event": "new_signal", "coin": item["coin"], "side": item["side"],
                                          "entry": item["entry"], "tp1": item["tp1"], "sl1": item["sl1"]}))


def sig_as_res(x):
    return {"side": x["side"], "is_long": x["is_long"], "entry": x["entry"], "coin": x["coin"],
            "tps": [x["tp1"], x["tp2"], x["tp3"]], "sls": [x["sl1"], x.get("sl2", x["sl1"])], "price": x.get("live_price", x["entry"])}


async def handle_signal_button(cb, data):
    uid = cb.from_user.id
    action, s, coin = data.split(":", 2)
    side = "LONG" if s == "L" else "SHORT"
    x = next((y for y in get_active_signals() if y["coin"] == coin and y["side"] == side), None)
    if not x:
        return await safe_answer(cb, "Сигнал уже неактуален", True)
    res = sig_as_res(x)
    if action == "in":
        for oc, od in get_user_open_positions(uid).items():
            if oc != coin and od["side"] == side and any(oc in g and coin in g for g in CORRELATION_GROUPS):
                return await safe_answer(cb, f"Уже открыт {side} по {oc}: монеты коррелируют", True)
        if not open_tracked_position(uid, coin, res):
            return await safe_answer(cb, "Сделка по монете уже открыта")
        await safe_answer(cb, "Лимит поставлен")
        await _tell(uid, f"⏳ Сопровождение {coin} {side}: вход <code>{fmt(x['entry'])}</code>, TP1-3 и итог придут сюда.")
    else:
        await asyncio.to_thread(add_user_saved_signal, uid, coin, signal_caption(x, uid), res)
        await safe_answer(cb, "Сохранено в избранное")


# =====================================================================
# РЕЗУЛЬТАТЫ СИГНАЛОВ: R, MFE/MAE, живые уведомления
# =====================================================================
R_BY_TP = {0: 0.0, 1: 0.75, 2: 1.8, 3: 2.3}  # TP1 1.5R (50%), TP2 2.5R (30%), TP3 4R (20%); остаток — по стопу


def signal_r(status, max_tp):
    if status == "loss":
        return -1.0
    if status == "win":
        return R_BY_TP.get(max_tp, 0.75)
    return 0.0


def excursion(x, rows, end_ms=None):
    """MFE/MAE в R с момента сигнала, но не позже момента закрытия (end_ms, мс)."""
    risk = abs(x["entry"] - x["sl1"]) or 1e-9
    is_long = x.get("is_long", x.get("side") == "LONG")
    t0 = x["ts"] * 1000
    mfe = mae = 0.0
    for ts, _o, h, l, _c, _v in rows:
        if ts < t0 or (end_ms is not None and ts > end_ms):
            continue
        if is_long:
            mfe = max(mfe, (h - x["entry"]) / risk)
            mae = min(mae, (l - x["entry"]) / risk)
        else:
            mfe = max(mfe, (x["entry"] - l) / risk)
            mae = min(mae, (x["entry"] - h) / risk)
    return round(mfe, 2), round(mae, 2)


async def notify_sig_result(x, status, max_tp):
    r = x.get("r", 0.0)
    head = f"{x['coin']} {x['side']}"
    if status == "win":
        text = f"✅ <b>{head}: {'TP3' if max_tp == 3 else 'TP2' if max_tp == 2 else 'TP1'} взят</b>\nРезультат: <b>{r:+.2f}R</b>"
    elif status == "loss":
        text = f"❌ <b>{head}: стоп</b>\nРезультат: <b>{r:+.2f}R</b>. Риск по идее закрыт."
    elif status == "missed":
        text = f"🚫 <b>{head}: цель взята без входа</b>. Идея упущена, новых входов не ищем."
    else:
        text = f"⌛ <b>{head}: вход не исполнился за срок</b>. Идея закрыта без результата."
    await notify_signal_holders(x["coin"], x["side"], text, kb=kb_nav(back=False), extra_uids=x.get("sent_to", []))


async def global_signal_monitor_loop():
    """Итоги и промежуточные события по свечам 15m: вход, TP1-3, стоп, MFE/MAE, R."""
    await asyncio.sleep(120)
    while True:
        try:
            hist = load_signal_history()
            changed = False
            rows_cache = {}
            for x in hist:
                if x.get("status") not in ("active", "in_trade"):
                    continue
                coin = x["coin"]
                if coin not in rows_cache:
                    rows_cache[coin] = await fetch_history(coin, "15m", 400, min_rows=40)
                    await asyncio.sleep(0.2)
                rows = rows_cache[coin]
                if not rows:
                    continue
                status, max_tp, hit_sl = simulate_signal(x, rows)
                end_ms = time.time() * 1000 if status in ("win", "loss", "missed", "expired") else None
                mfe, mae = excursion(x, rows, end_ms)
                if mfe > x.get("mfe", 0) or mae < x.get("mae", 0):
                    x["mfe"], x["mae"] = max(mfe, x.get("mfe", 0)), min(mae, x.get("mae", 0))
                    changed = True
                prev_status, prev_tp = x["status"], x.get("max_tp_hit", 0)
                if (status, max_tp, hit_sl) == (prev_status, prev_tp, x.get("hit_sl", 0)):
                    continue
                x.update(status=status, max_tp_hit=max_tp, hit_sl=hit_sl)
                changed = True
                if prev_status == "active" and status == "in_trade":
                    await notify_signal_holders(coin, x["side"], f"🎯 <b>{coin} {x['side']}: лимит исполнен</b>\n"
                                                                 f"Вход <code>{fmt(x['entry'])}</code>. Ведём до TP1.",
                                                kb=kb_nav(back=False), extra_uids=x.get("sent_to", []))
                if max_tp > prev_tp and status == "in_trade":
                    await notify_signal_holders(coin, x["side"], f"🎯 <b>{coin} {x['side']}: достигнут TP{max_tp}</b>\n"
                                                                 f"Стоп переносится за цену входа.",
                                                kb=kb_nav(back=False), extra_uids=x.get("sent_to", []))
                if status in ("win", "loss", "missed", "expired") and prev_status != status:
                    x["closed_ts"] = time.time()
                    x["r"] = signal_r(status, max_tp)
                    await notify_sig_result(x, status, max_tp)
                    await notify_admin(f"📊 <b>{coin} {x['side']}:</b> {status} · {x['r']:+.2f}R (TP {max_tp})")
                    asyncio.create_task(send_webhook_all({"event": f"closed_{status}", "coin": coin, "side": x["side"], "r": x["r"]}))
            if changed:
                save_signal_history(hist)
        except Exception as e:
            log.error(f"global_signal_monitor: {e}", exc_info=True)
        await asyncio.sleep(120)


async def invalidate_signal(x, reason):
    """Сигнал больше не ликвиден: снимаем из списков и сообщаем получателям и держателям."""
    x["status"] = "cancelled"
    x["cancel_reason"] = reason
    x["closed_ts"] = time.time()
    x["r"] = 0.0
    text = (f"🚫 <b>Сигнал снят: {x['coin']} ({x['side']})</b>\n\n{reason}\n"
            f"Вход был <code>{fmt(x['entry'])}</code>. Не входите по этой идее.")
    await notify_signal_holders(x["coin"], x["side"], text, kb=webapp_kb(x["coin"]), extra_uids=x.get("sent_to", []))
    for uid, u in list(_users().items()):
        pos = (u.get("open_positions") or {}).get(x["coin"])
        if pos and pos.get("waiting_entry") and pos["side"] == x["side"]:
            set_user_open_position(int(uid), x["coin"], None)


# =====================================================================
# КОЛЛ ДНЯ (ежедневно 08:00 МСК)
# =====================================================================
daily_call_state = {"date": None}


async def daily_call_loop():
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=8, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        try:
            today = datetime.now(timezone(timedelta(hours=3))).date().isoformat()
            cands = get_active_signals()
            if cands and daily_call_state["date"] != today:
                best = max(cands, key=lambda x: x["strength"])
                best["daily"] = True
                closes = await _closes(best["coin"])
                lv = LIVE.get(best["coin"])
                price = lv["price"] if lv else best["entry"]
                card = await asyncio.to_thread(render_signal_card, best, closes, price, "КОЛЛ ДНЯ")
                for cid in signal_recipients(best["coin"], best["side"]):
                    try:
                        await _send_new(cid, signal_caption(best, cid, "КОЛЛ ДНЯ"), signal_keyboard(best), card)
                        best["sent_to"].append(cid)
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
                daily_call_state["date"] = today
                state_touch()
        except Exception as e:
            log.error(f"daily_call: {e}", exc_info=True)
        await asyncio.sleep(60)


# =====================================================================
# АНАЛИТИКА СИГНАЛОВ
# =====================================================================
def _sig_closed_all():
    with STATE_LOCK:
        hist = list(STATE["signal_history"])
    out = []
    for x in hist:
        if x.get("status") in ("win", "loss"):
            if x.get("r") is None:
                x["r"] = signal_r(x["status"], x.get("max_tp_hit", 0))
            out.append(x)
    return sorted(out, key=lambda x: x.get("closed_ts", x["ts"]))


def _agg(items):
    n = len(items)
    if not n:
        return None
    wins = sum(1 for x in items if x["status"] == "win")
    rs = [x.get("r", 0.0) for x in items]
    gp = sum(r for r in rs if r > 0)
    gl = -sum(r for r in rs if r < 0)
    return {"n": n, "wr": round(wins / n * 100, 1), "avg_r": round(sum(rs) / n, 2),
            "sum_r": round(sum(rs), 2), "pf": round(gp / gl, 2) if gl else None}


def analytics_summary():
    with STATE_LOCK:
        allsig = list(STATE["signal_history"])
    closed = _sig_closed_all()
    created = len(allsig)
    cnt = {s: sum(1 for x in allsig if x.get("status") == s) for s in
           ("win", "loss", "missed", "expired", "cancelled", "active", "in_trade")}
    entered = cnt["win"] + cnt["loss"] + cnt["in_trade"]
    total = _agg(closed)
    curve, cum, peak, dd = [], 0.0, 0.0, 0.0
    for x in closed:
        cum += x["r"]
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
        curve.append(round(cum, 2))
    mw = ml = cw = cl = 0
    for x in closed:
        if x["status"] == "win":
            cw, cl = cw + 1, 0
        else:
            cl, cw = cl + 1, 0
        mw, ml = max(mw, cw), max(ml, cl)
    bands = [(0, 60, "50–60%"), (60, 70, "60–70%"), (70, 80, "70–80%"), (80, 101, "80%+")]
    calib = []
    for lo, hi, label in bands:
        grp = [x for x in closed if lo <= x["strength"] < hi]
        if grp:
            a = _agg(grp)
            a["label"] = label
            a["avg_conf"] = round(sum(x["strength"] for x in grp) / len(grp), 1)
            calib.append(a)
    sessions = {"день 08–22": [x for x in closed if 8 <= x.get("hour", 12) < 22],
                "ночь 22–08": [x for x in closed if not (8 <= x.get("hour", 12) < 22)]}
    coins = {}
    for x in closed:
        coins.setdefault(x["coin"], []).append(x)
    coin_rows = sorted([(c, _agg(v)) for c, v in coins.items()], key=lambda kv: kv[1]["sum_r"], reverse=True)
    reasons = {}
    for x in closed:
        for rsn in set(x.get("reasons") or []):
            reasons.setdefault(rsn, []).append(x)
    reason_rows = [(k, _agg(v)) for k, v in reasons.items() if len(v) >= 3]
    reason_rows.sort(key=lambda kv: kv[1]["avg_r"], reverse=True)
    thresholds = [(t, _agg([x for x in closed if x["strength"] >= t])) for t in (60, 65, 70, 75, 80)]
    thresholds = [(t, a) for t, a in thresholds if a]
    mfe = round(sum(x.get("mfe", 0) for x in closed) / len(closed), 2) if closed else 0.0
    mae = round(sum(x.get("mae", 0) for x in closed) / len(closed), 2) if closed else 0.0
    hold = round(sum((x.get("closed_ts", x["ts"]) - x["ts"]) for x in closed) / len(closed) / 3600, 1) if closed else 0.0
    daily = _agg([x for x in closed if x.get("daily")])
    return {"created": created, "cnt": cnt, "entered": entered,
            "fill_rate": round(entered / created * 100, 1) if created else 0.0,
            "miss_rate": round(cnt["missed"] / (entered + cnt["missed"]) * 100, 1) if (entered + cnt["missed"]) else 0.0,
            "total": total, "curve": curve[-120:], "max_dd": round(dd, 2), "streak_w": mw, "streak_l": ml,
            "bands": calib, "sessions": {k: _agg(v) for k, v in sessions.items()},
            "types": {t: _agg([x for x in closed if x["type"] == t]) for t in ("Краткосрочный", "Среднесрочный", "Долгосрочный")},
            "sides": {s: _agg([x for x in closed if x["side"] == s]) for s in ("LONG", "SHORT")},
            "coins_top": coin_rows[:5], "coins_bottom": coin_rows[-5:][::-1] if len(coin_rows) > 5 else [],
            "reasons_top": reason_rows[:6], "reasons_bottom": reason_rows[-3:][::-1] if len(reason_rows) > 6 else [],
            "thresholds": thresholds, "mfe": mfe, "mae": mae, "hold_h": hold, "daily": daily}


def _fa(a):
    if not a:
        return "нет данных"
    pf = "—" if a["pf"] is None else a["pf"]
    return f"{a['n']} сд · WR <b>{a['wr']}%</b> · ср. <b>{a['avg_r']:+.2f}R</b> · Σ {a['sum_r']:+.2f}R · PF {pf}"


def analytics_text(s):
    t = s["total"]
    if not t:
        return ("📊 <b>Аналитика сигналов</b>\n\nЗакрытых сигналов пока нет. Данные появятся после первых результатов: "
                f"создано {s['created']}, активных {s['cnt']['active'] + s['cnt']['in_trade']}.")
    lines = ["📊 <b>Аналитика сигналов IXCLY</b>", "",
             f"Создано: <b>{s['created']}</b> · вошли: <b>{s['entered']}</b> ({s['fill_rate']}%) · упущено: {s['miss_rate']}%",
             f"✅ {s['cnt']['win']} · ❌ {s['cnt']['loss']} · ⌛ {s['cnt']['expired']} · 🚫 {s['cnt']['cancelled']}", "",
             f"<b>Итог по закрытым:</b> {_fa(t)}",
             f"Макс. просадка: <b>{s['max_dd']:.2f}R</b> · серии: победы {s['streak_w']}, убытки {s['streak_l']}",
             f"Средний MFE <b>{s['mfe']:+.2f}R</b> / MAE <b>{s['mae']:+.2f}R</b> · удержание {s['hold_h']} ч", "",
             "<b>Калибровка по уверенности</b> (проверка, что Conf честный):"]
    for b in s["bands"]:
        lines.append(f"• {b['label']} (ср. {b['avg_conf']}%): WR <b>{b['wr']}%</b>, {b['avg_r']:+.2f}R, {b['n']} сд")
    lines += ["", "<b>Порог уверенности</b> (что было бы, если фильтровать):"]
    for th, a in s["thresholds"]:
        lines.append(f"• от {th}%: WR {a['wr']}% · {a['avg_r']:+.2f}R · {a['n']} сд")
    lines += ["", "<b>Типы:</b>"]
    lines += [f"• {k}: {_fa(v)}" for k, v in s["types"].items() if v]
    lines += ["", f"<b>Лонг</b>: {_fa(s['sides']['LONG'])}", f"<b>Шорт</b>: {_fa(s['sides']['SHORT'])}"]
    for k, a in s["sessions"].items():
        if a:
            lines.append(f"<b>{k}</b>: WR {a['wr']}% · {a['avg_r']:+.2f}R · {a['n']} сд")
    if s["daily"]:
        lines.append(f"<b>Коллы дня</b>: WR {s['daily']['wr']}% · {s['daily']['avg_r']:+.2f}R · {s['daily']['n']} сд")
    if s["coins_top"]:
        lines += ["", "<b>Лучшие монеты (Σ R):</b> " + ", ".join(f"{c} {a['sum_r']:+.1f}R" for c, a in s["coins_top"])]
    if s["coins_bottom"]:
        lines.append("<b>Худшие монеты:</b> " + ", ".join(f"{c} {a['sum_r']:+.1f}R" for c, a in s["coins_bottom"]))
    if s["reasons_top"]:
        lines += ["", "<b>Факторы, которые работают</b> (ср. R, от 3 сделок):"]
        lines += [f"• {k}: {a['avg_r']:+.2f}R · WR {a['wr']}% · {a['n']} сд" for k, a in s["reasons_top"][:5]]
    if s["reasons_bottom"]:
        lines += ["<b>Факторы, которые ломают:</b>"]
        lines += [f"• {k}: {a['avg_r']:+.2f}R · WR {a['wr']}% · {a['n']} сд" for k, a in s["reasons_bottom"][:3]]
    return "\n".join(lines)


def render_analytics_png(s):
    W, H = 1024, 760
    img = Image.new("RGB", (W, H), (10, 11, 18))
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, W, 8), fill=GOLD)
    d.text((58, 40), "Кривая результата сигналов, R", fill=(245, 247, 255), font=_font(40, True))
    t = s["total"]
    if t:
        d.text((58, 96), f"Σ {t['sum_r']:+.2f}R · WR {t['wr']}% · {t['n']} сделок · просадка {s['max_dd']:.2f}R",
               fill=TEXT_GREY, font=_font(26))
    box = (58, 150, 966, 460)
    curve = s["curve"] or [0]
    vmin, vmax = min(min(curve), 0), max(max(curve), 0.5)
    rng = (vmax - vmin) or 1
    x0, y0, x1, y1 = box
    zero_y = y1 - (0 - vmin) / rng * (y1 - y0)
    d.line((x0, zero_y, x1, zero_y), fill=(60, 66, 96), width=2)
    pts = [(x0 + i * (x1 - x0) / max(1, len(curve) - 1), y1 - (v - vmin) / rng * (y1 - y0)) for i, v in enumerate(curve)]
    if len(pts) > 1:
        d.polygon(pts + [(x1, zero_y), (x0, zero_y)], fill=(18, 48, 52) if curve[-1] >= 0 else (50, 20, 30))
        d.line(pts, fill=(45, 226, 198) if curve[-1] >= 0 else (255, 90, 110), width=4)
    d.text((58, 480), "Калибровка: уверенность → фактический винрейт", fill=(245, 247, 255), font=_font(30, True))
    bands = s["bands"]
    bx0, bw = 90, 200
    for i, b in enumerate(bands):
        x = bx0 + i * 225
        hgt = int(b["wr"] / 100 * 200)
        d.rectangle((x, 920 - 200, x + bw, 920 - 200 + 200), outline=(40, 46, 70), width=2)
        d.rectangle((x, 920 - hgt, x + bw, 920), fill=(96, 136, 255))
        d.text((x, 935), b["label"], fill=TEXT_GREY, font=_font(24))
        d.text((x, 720 - hgt), f"{b['wr']}%", fill=(245, 247, 255), font=_font(26, True))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


@dp.message(Command("analytics"))
async def cmd_analytics(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    if m.from_user.id not in ADMIN_IDS and m.from_user.id not in active_chats:
        return
    s = await asyncio.to_thread(analytics_summary)
    await _send_new(m.chat.id, analytics_text(s), kb_nav(back=False), None)
    if s["total"]:
        png = await asyncio.to_thread(render_analytics_png, s)
        await bot.send_photo(m.chat.id, BufferedInputFile(png, "analytics.png"), caption="📈 Кривая результата и калибровка")




from aiogram.types import LabeledPrice

# =====================================================================
# ДОСТУП: пробный период, подписка (Telegram Stars), промокоды, бесплатный доступ, реклама
# =====================================================================
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))
PLANS = {30: 399, 90: 1000, 365: 3500}  # дни: цена в Telegram Stars (XTR)
PLAN_TITLE = {30: "Месяц", 90: "3 месяца", 365: "Год"}
FEE_PCT = 0.1                            # комиссия туда-обратно, % от позиции
FREE_ACTS = {"me", "access", "promo_apply", "support"}
TG_SEM = None                            # создаётся в main (не привязано к event loop при импорте)
COIN_RL = {}                             # uid -> ts последнего пересчёта монеты
_ob_cache = {}
_hourly_cache = {}
ADS_DEFAULT = {"on": False, "text": "", "url": "", "contact": "@ixcly", "support": "@ixcly"}


def ads_get():
    a = dict(ADS_DEFAULT)
    a.update(STATE.get("ads") or {})
    return a


def _init_trial(uid):
    """Новому пользователю — пробный период; старым (до обновления) тоже даём, если доступа ещё не было."""
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(uid), {"active": True})
        if "access" not in u:
            u["access"] = {"until": time.time() + TRIAL_DAYS * 86400, "kind": "trial", "free": False, "paid_ever": False}
    state_touch(False)


def has_access(uid):
    if int(uid) in ADMIN_IDS:
        return True
    a = _users().get(str(uid), {}).get("access") or {}
    if a.get("free"):
        return True
    return time.time() < a.get("until", 0)


def grant_days(uid, days, kind):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(uid), {"active": True})
        a = u.setdefault("access", {"until": 0, "kind": kind, "free": False, "paid_ever": False})
        if days >= 36500 or days == -1:
            a["until"] = 9999999999
            a["kind"] = "lifetime"
            a["free"] = True
        elif a.get("until", 0) > 9000000000:
            pass
        else:
            base = max(time.time(), a.get("until", 0))
            a["until"] = base + days * 86400
            a["kind"] = kind
            a["free"] = False
        if kind == "paid":
            a["paid_ever"] = True
    state_touch()


def access_payload(uid):
    a = _users().get(str(uid), {}).get("access") or {}
    ok = has_access(uid)
    until = a.get("until", 0)
    left = max(0, until - time.time())
    if int(uid) in ADMIN_IDS:
        status, days_left = "Админ", 9999
    elif until > 9000000000 or a.get("free"):
        status, days_left = "Навсегда", 9999
    elif ok:
        status = "пробный период" if a.get("kind") == "trial" else "подписка"
        days_left = int(-(-left // 86400))
    else:
        status, days_left = "закрыт", 0
    return {"ok": ok, "status": status, "days_left": days_left, "hours_left": int(left // 3600) % 24,
            "until": until, "free": bool(a.get("free")),
            "plans": [{"days": d, "stars": s, "title": PLAN_TITLE[d]} for d, s in PLANS.items()],
            "support": ads_get()["support"], 
            "ad": ads_get()["text"] if ads_get()["on"] else "",
            "ad_url": ads_get()["url"] if ads_get()["on"] else ""} # <-- ДОБАВИЛИ ССЫЛКУ


def access_closed_text(uid):
    a = access_payload(uid)
    if a["status"] == "закрыт" or not a["ok"]:
        return "🔒 Доступ закрыт. Оплатите подписку в боте: /buy"
    return "Доступ закрыт"


def ad_block():
    a = ads_get()
    if not a["on"] or not a["text"]:
        return ""
    link = f"\n🔗 {html.escape(a['url'])}" if a["url"] else ""
    return f"\n\n📢 <i>{html.escape(a['text'])}</i>{link}\nРеклама: {html.escape(a['contact'])}"


def paywall_kb():
    rows = [[InlineKeyboardButton(text=f"💳 {PLAN_TITLE[d]} · {s}⭐", callback_data=f"buy:{d}")] for d, s in PLANS.items()]
    rows.append([InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo:help")])
    rows.append([InlineKeyboardButton(text=f"🆘 Поддержка {ads_get()['support']}", url=f"https://t.me/{ads_get()['support'].lstrip('@')}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_plan_invoice(uid, days):
    if days not in PLANS:
        return
    off = DISCOUNT_OFFERS.get(uid)
    stars = PLANS[days]
    if off and time.time() < off["until"]:
        stars = max(1, round(stars * (100 - off["pct"]) / 100))
    await bot.send_invoice(chat_id=uid, title=f"IXCLY PRO · {PLAN_TITLE[days]}",
                           description=f"Доступ к сигналам, сопровождению сделок и аналитике на {days} дн.",
                           payload=f"plan:{days}", currency="XTR",
                           prices=[LabeledPrice(label=f"{PLAN_TITLE[days]}", amount=stars)], provider_token="")


def hourly_rows_cached_key(coin):
    return coin


async def hourly_rows(coin):
    """1h свечи для Chandelier: кэш на 60 секунд, чтобы не бить по бирже на каждую сделку."""
    ent = _hourly_cache.get(coin)
    if ent and time.time() - ent[0] < 60:
        return ent[1]
    rows = await fetch_history(coin, "1h", 60, min_rows=30)
    _hourly_cache[coin] = (time.time(), rows)
    return rows


def normalize_signals():
    """Старые сигналы без поля is_long (до обновления) — дополняем, чтобы монитор не падал."""
    with STATE_LOCK:
        for x in STATE["signal_history"]:
            if isinstance(x, dict):
                x.setdefault("is_long", x.get("side") == "LONG")
                x.setdefault("sl1", x.get("sl1", 0.0))


async def access_reminder_loop():
    """Напоминания об оплате в 12:00 и 20:00 МСК: пробный период и просроченный доступ (тем, кто не платил)."""
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        slots = [msk.replace(hour=h, minute=0, second=0, microsecond=0) for h in (12, 20)]
        nxt = next((t for t in slots if t > msk), slots[0] + timedelta(days=1))
        await asyncio.sleep((nxt - msk).total_seconds())
        for uid_str, u in list(_users().items()):
            try:
                uid = int(uid_str)
                if not isinstance(u, dict) or uid in ADMIN_IDS:
                    continue
                a = u.get("access") or {}
                if a.get("free"):
                    continue
                left = a.get("until", 0) - time.time()
                if a.get("kind") == "trial":
                    if 0 < left <= 2 * 86400:
                        text = f"⏳ <b>Пробный доступ заканчивается через {int(left // 3600)} ч.</b>\nОплатите подписку, чтобы сигналы не прерывались."
                    elif left <= 0:
                        text = "🔒 <b>Пробный доступ закончился.</b> Сигналы и сопровождение отключены. Оплатите подписку, чтобы вернуть доступ."
                    else:
                        continue
                elif left <= 0 and not a.get("paid_ever"):
                    text = "🔒 <b>Доступ закрыт.</b> Оплатите подписку, чтобы вернуть сигналы."
                else:
                    continue
                await _tell(uid, text, paywall_kb())
                await asyncio.sleep(0.05)
            except Exception as e:
                log.info(f"reminder {uid_str}: {e}")


async def pre_checkout_handler(q: types.PreCheckoutQuery):
    try:
        await q.answer(ok=True)
    except Exception as e:
        log.warning(f"pre_checkout: {e}")


@dp.pre_checkout_query()
async def on_pre_checkout(q: types.PreCheckoutQuery):
    await pre_checkout_handler(q)


@dp.message(F.successful_payment)
async def on_paid(m: types.Message):
    try:
        days = int(m.successful_payment.invoice_payload.split(":")[1])
    except Exception:
        days = 30
    buyer = _users().get(str(m.from_user.id), {})
    first_payment = not (buyer.get("access") or {}).get("paid_ever")
    grant_days(m.from_user.id, days, "paid")
    DISCOUNT_OFFERS.pop(m.from_user.id, None)
    ref = buyer.get("referrer")
    if ref and first_payment:
        grant_days(ref, 7, "referral")
        await _tell(ref, "🎁 Ваш друг оплатил подписку. <b>+7 дней</b> начислено вам.")
    await _send_new(m.chat.id, f"✅ <b>Оплата получена.</b> Доступ продлён на {days} дн.\n"
                               f"Откройте приложение, сигналы уже доступны.", kb_nav(back=False), None)
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, f"💰 Оплата: {m.from_user.id} · {days} дн. · {m.successful_payment.total_amount}⭐")
        except Exception:
            pass


@dp.message(Command("buy"))
async def cmd_buy(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    a = access_payload(m.from_user.id)
    await _send_new(m.chat.id, f"💳 <b>Подписка IXCLY PRO</b>\nСтатус: <b>{a['status']}</b>"
                               f"{' · осталось ' + str(a['days_left']) + ' дн.' if a['ok'] and not a['free'] else ''}\n\n"
                               f"Оплата звёздами Telegram (⭐). Выберите срок:", paywall_kb(), None)


def account_age_hours(uid):
    try:
        joined = _users().get(str(uid), {}).get("joined_at")
        return (datetime.now() - datetime.strptime(joined, "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600
    except Exception:
        return 999.0


def apply_promo(uid, code):
    """Промокод. Аккаунт младше 24 часов получает максимум 1 день — против накрутки твинков."""
    code = code.strip().upper()
    with STATE_LOCK:
        promo = STATE["promos"].get(code)
        if not promo or promo.get("uses_left", 0) <= 0 or (promo.get("expires") and time.time() > promo["expires"]):
            return "❌ Промокод не найден или закончился."
        if uid in promo.get("used", []):
            return "ℹ Вы уже использовали этот промокод."
        days, note = promo["days"], ""
        if account_age_hours(uid) < 24 and days > 1:
            days, note = 1, " (новый аккаунт: 1 день)"
        promo["uses_left"] -= 1
        promo.setdefault("used", []).append(uid)
    grant_days(uid, days, "promo")
    state_touch()
    return f"🎉 Промокод активирован: +{days} дн.{note}"


@dp.message(Command("promo"))
async def cmd_promo(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    parts = (m.text or "").split()
    if len(parts) < 2:
        return await _send_new(m.chat.id, "🎟 Введите: <code>/promo КОД</code>", kb_nav(back=False), None)
    await _send_new(m.chat.id, apply_promo(m.from_user.id, parts[1]), kb_nav(back=False), None)


if admin_dp:
    def _is_admin(m):
        return m.from_user.id in ADMIN_IDS

    @admin_dp.message(Command("grant"))
    async def adm_grant(m: types.Message):
        if not _is_admin(m):
            return
        try:
            _, uid, days = m.text.split()
            grant_days(int(uid), int(days), "grant")
            await m.answer(f"✅ Пользователю {uid} добавлено {days} дн.")
        except ValueError:
            await m.answer("Формат: <code>/grant UID ДНИ</code>", parse_mode="HTML")

    @admin_dp.message(Command("lifetime", "forever"))
    async def adm_lifetime(m: types.Message):
        if not _is_admin(m):
            return
        try:
            uid = int(m.text.split()[1])
            grant_days(uid, -1, "lifetime")
            await m.answer(f"✅ Пользователю {uid} выдан доступ НАВСЕГДА.")
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/lifetime UID</code>", parse_mode="HTML")

    @admin_dp.message(Command("revoke"))
    async def adm_revoke(m: types.Message):
        if not _is_admin(m):
            return
        try:
            uid = int(m.text.split()[1])
            with STATE_LOCK:
                u = STATE["users"].setdefault(str(uid), {"active": True})
                u["access"] = {"until": 0, "kind": "trial", "free": False, "paid_ever": (u.get("access") or {}).get("paid_ever", False)}
            state_touch()
            await m.answer(f"⛔ Доступ пользователя {uid} закрыт.")
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/revoke UID</code>", parse_mode="HTML")

    @admin_dp.message(Command("free"))
    async def adm_free(m: types.Message):
        """/free UID on|off — бесплатный доступ без подписки (без срока)."""
        if not _is_admin(m):
            return
        try:
            _, uid, mode = m.text.split()
            with STATE_LOCK:
                a = STATE["users"].setdefault(str(int(uid)), {"active": True}).setdefault("access", {"until": 0, "kind": "grant", "free": False, "paid_ever": False})
                a["free"] = mode.lower() == "on"
            state_touch()
            await m.answer(f"{'🆓' if a['free'] else '🔒'} Бесплатный доступ {uid}: {'включён' if a['free'] else 'выключен'}")
        except ValueError:
            await m.answer("Формат: <code>/free UID on|off</code>", parse_mode="HTML")

    @admin_dp.message(Command("promo_add"))
    async def adm_promo_add(m: types.Message):
        """/promo_add КОД ДНИ ИСПОЛЬЗОВАНИЙ [ДНЕЙ_ДО_ИСТЕЧЕНИЯ]"""
        if not _is_admin(m):
            return
        try:
            parts = m.text.split()
            code, days, uses = parts[1].upper(), int(parts[2]), int(parts[3])
            exp = time.time() + int(parts[4]) * 86400 if len(parts) > 4 else None
            with STATE_LOCK:
                STATE["promos"][code] = {"days": days, "uses_left": uses, "expires": exp, "used": []}
            state_touch()
            await m.answer(f"🎟 Промокод {code}: {days} дн., использований {uses}" + (f", до {int(parts[4])} дн." if exp else ""))
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/promo_add КОД ДНИ ИСПОЛЬЗОВАНИЙ [ДНЕЙ_ДО_ИСТЕЧЕНИЯ]</code>", parse_mode="HTML")

    @admin_dp.message(Command("promo_del"))
    async def adm_promo_del(m: types.Message):
        if not _is_admin(m):
            return
        code = (m.text.split()[1] if len(m.text.split()) > 1 else "").upper()
        with STATE_LOCK:
            ok = STATE["promos"].pop(code, None) is not None
        state_touch()
        await m.answer("🗑 Удалён." if ok else "Не найден.")

    @admin_dp.message(Command("promos"))
    async def adm_promos(m: types.Message):
        if not _is_admin(m):
            return
        lines = [f"{k}: {v['days']} дн., осталось {v['uses_left']}" for k, v in STATE["promos"].items()] or ["Промокодов нет"]
        await m.answer("🎟 <b>Промокоды</b>\n" + "\n".join(lines), parse_mode="HTML")

    @admin_dp.message(Command("ad_set"))
    async def adm_ad_set(m: types.Message):
        """/ad_set текст | ссылка(необязательно)"""
        if not _is_admin(m):
            return
        body = m.text.split(" ", 1)[1] if " " in m.text else ""
        text, _, url = body.partition("|")
        with STATE_LOCK:
            STATE["ads"] = dict(ads_get(), on=True, text=text.strip(), url=url.strip())
        state_touch()
        await m.answer("📢 Реклама включена и будет показана в приложении и утреннем брифинге.")

    @admin_dp.message(Command("ad_off"))
    async def adm_ad_off(m: types.Message):
        if not _is_admin(m):
            return
        with STATE_LOCK:
            STATE["ads"] = dict(ads_get(), on=False)
        state_touch()
        await m.answer("📢 Реклама выключена.")

    @admin_dp.message(Command("contact"))
    async def adm_contact(m: types.Message):
        """/contact ads|support @ник — кому писать по рекламе и в поддержку"""
        if not _is_admin(m):
            return
        try:
            _, kind, nick = m.text.split()
            key = "contact" if kind == "ads" else "support"
            with STATE_LOCK:
                STATE["ads"] = dict(ads_get(), **{key: nick})
            state_touch()
            await m.answer(f"✅ {'Реклама' if key == 'contact' else 'Поддержка'}: {nick}")
        except ValueError:
            await m.answer("Формат: <code>/contact ads @ник</code> или <code>/contact support @ник</code>", parse_mode="HTML")

    @admin_dp.message(Command("who"))
    async def adm_who(m: types.Message):
        if not _is_admin(m):
            return
        try:
            uid = int(m.text.split()[1])
            a = access_payload(uid)
            u = _users().get(str(uid), {})
            await m.answer(f"👤 {uid} · {html.escape(str(u.get('first_name') or '—'))}\nСтатус: {a['status']}, осталось {a['days_left']} дн., "
                           f"бесплатно: {'да' if a['free'] else 'нет'}")
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/who UID</code>", parse_mode="HTML")

    @admin_dp.message(Command("admin_help"))
    async def adm_help(m: types.Message):
        if not _is_admin(m):
            return
        await m.answer("<b>Команды админа</b>\n/grant UID ДНИ — добавить дни\n/revoke UID — закрыть доступ\n"
                       "/free UID on|off — бесплатный доступ\n/who UID — статус\n"
                       "/promo_add КОД ДНИ ИСП [ДНЕЙ] · /promo_del КОД · /promos\n"
                       "/ad_set текст | ссылка · /ad_off\n/contact ads @ник · /contact support @ник", parse_mode="HTML")



async def welcome_for(chat_id, uid):
    if has_access(uid):
        await bot.send_message(chat_id, WELCOME_BOT, parse_mode="HTML", reply_markup=bot_main_keyboard())
    else:
        await bot.send_message(chat_id, "🔒 <b>Доступ закрыт.</b>\nПробный период закончился или не активирован. "
                                        "Оплатите подписку, чтобы получать сигналы и сопровождение сделок.",
                               parse_mode="HTML", reply_markup=paywall_kb())


@dp.message()
async def fallback_message(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    coin = normalize_ticker(m.text or "") if m.text and re.fullmatch(r"[A-Za-z0-9]{2,12}", m.text.strip()) else None
    if coin and WEBAPP_URL:
        await bot.send_message(m.chat.id, f"🔍 <b>{html.escape(coin)}</b>: открыть анализ в приложении",
                               parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                   InlineKeyboardButton(text="📈 Открыть анализ", web_app=WebAppInfo(url=webapp_link(coin)))]]))
    else:
        await welcome_for(m.chat.id, m.from_user.id)


@dp.callback_query()
async def on_callback(cb: types.CallbackQuery, state: FSMContext):
    data = cb.data or ""
    uid = cb.from_user.id
    msg = cb.message
    try:
        if data == "show_docs" and msg:
            await safe_answer(cb)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Политика конфиденциальности", url=DOCS_PRIVACY)],
                [InlineKeyboardButton(text="Пользовательское соглашение (оферта)", url=DOCS_OFFER)],
                [InlineKeyboardButton(text=f"🆘 Поддержка {ads_get()['support']}", url=f"https://t.me/{ads_get()['support'].lstrip('@')}")]])
            return await msg.answer("📄 <b>Документы IXCLY PRO</b>\nВыберите нужный пункт:", reply_markup=kb, parse_mode="HTML")
        if data.startswith("buy:"):
            await safe_answer(cb)
            return await send_plan_invoice(uid, int(data.split(":")[1]))
        if data == "promo:help":
            await safe_answer(cb)
            return await _send_new(msg.chat.id if msg else uid, "🎟 Отправьте: <code>/promo КОД</code>", kb_nav(back=False), None)
        if data[:3] in ("in:", "sv:"):
            return await handle_signal_button(cb, data)
        if data == "del_msg" and msg:
            await safe_answer(cb, "Скрыто")
            return await safe_delete(msg)
        if data == "notif:menu" and msg:
            await safe_answer(cb)
            return await msg.edit_text("🔔 <b>Уведомления</b>\nВыберите, что присылать в чат:", parse_mode="HTML",
                                       reply_markup=notif_keyboard(uid))
        if data == "notif:back" and msg:
            await safe_answer(cb)
            return await msg.edit_text(WELCOME_BOT, parse_mode="HTML", reply_markup=bot_main_keyboard())
        if data.startswith("notif:") and data.split(":")[1] in NOTIFY_NAMES:
            k = data.split(":")[1]
            cur = _users().get(str(uid), {}).get(f"notify_{k}", True)
            set_user_field(uid, **{f"notify_{k}": not cur})
            await safe_answer(cb, f"{NOTIFY_NAMES[k]}: {'вкл' if not cur else 'выкл'}")
            if msg:
                await msg.edit_reply_markup(reply_markup=notif_keyboard(uid))
            return
        await safe_answer(cb)
    except Exception as e:
        log.info(f"callback {data}: {e}")
        await safe_answer(cb)


# =====================================================================
# АДМИН-БОТ
# =====================================================================
def admin_keyboard():
    B = InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[
        [B(text="📊 Статистика", callback_data="admin:stats"), B(text="👥 Пользователи", callback_data="admin:users")],
        [B(text="📰 Новость сейчас", callback_data="admin:news_now"),
         B(text=f"🔔 Новости: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}", callback_data="admin:toggle_news")],
        [B(text=f"🚨 Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}", callback_data="admin:toggle_scan"),
         B(text="🔍 Скан-тест", callback_data="admin:scan_test")],
        [B(text="🔧 Диагностика", callback_data="admin:diag"), B(text="💾 Бэкап", callback_data="admin:backup")],
        [B(text="📥 Восстановить", callback_data="admin:restore"), B(text="⏳ Таймеры", callback_data="admin:timers")],
    ])


async def notify_admin(text):
    if not admin_bot:
        return
    for aid in ADMIN_IDS:
        try:
            await admin_bot.send_message(aid, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            log.info(f"Админу {aid}: {e}")


async def telegram_backup(reason="auto"):
    global backup_dirty, last_backup_ts
    if not admin_bot or not ADMIN_IDS:
        return False
    data = (await asyncio.to_thread(flush_state)).encode("utf-8")
    ok = False
    caption = (f"💾 БЭКАП · юзеров {len(STATE['users'])} · сигналов {len(STATE['signal_history'])} · "
               f"{reason} · {datetime.now().strftime('%d.%m %H:%M')}")
    for aid in ADMIN_IDS:
        try:
            msg = await admin_bot.send_document(aid, BufferedInputFile(data, "bot_state.json"), caption=caption, disable_notification=True)
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
    if ok:
        backup_dirty, last_backup_ts = False, time.time()
    return ok


async def telegram_restore():
    if not admin_bot or not ADMIN_IDS:
        return 0
    for aid in ADMIN_IDS:
        try:
            pm = getattr(await admin_bot.get_chat(aid), "pinned_message", None)
            doc = getattr(pm, "document", None) if pm else None
            if not doc or not (doc.file_name or "").endswith(".json"):
                continue
            f = await admin_bot.get_file(doc.file_id)
            data = json.loads((await admin_bot.download_file(f.file_path)).read().decode("utf-8"))
            n = restore_state(data)
            last_backup_msg[aid] = pm.message_id
            if n:
                return n
        except Exception as e:
            log.warning(f"Восстановление из бэкапа не удалось: {e}")
    return 0


async def backup_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if backup_dirty and time.time() - last_backup_ts > 120:
                await telegram_backup("авто")
        except Exception as e:
            log.warning(f"backup_loop: {e}")
        next_timers["backup"] = time.time() + 30
        await asyncio.sleep(30)


if admin_dp:
    @admin_dp.errors()
    async def admin_errors_handler(event: ErrorEvent):
        if not isinstance(event.exception, TelegramNetworkError):
            log.error(f"Ошибка админ-бота: {event.exception}", exc_info=event.exception)
        return True

    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            return await m.answer(f"⛔ Доступ только для администратора.\nВаш ID: <code>{m.from_user.id}</code>", parse_mode="HTML")
        total, active = user_counts()
        await m.answer(f"✅ <b>Админ-панель IXCLY v{VERSION}</b>\n👥 Пользователей: {total} (активных {active})",
                       reply_markup=admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            return await safe_answer(cb, "Нет доступа", True)
        act = cb.data.split(":")[1]
        if act in ("toggle_news", "toggle_scan"):
            key = "news_enabled" if act == "toggle_news" else "alerts_enabled"
            bot_flags[key] = not bot_flags[key]
            state_touch()
            await safe_answer(cb, f"{'Новости' if key == 'news_enabled' else 'Сканер'}: {'ВКЛ' if bot_flags[key] else 'ВЫКЛ'}")
            try:
                await cb.message.edit_reply_markup(reply_markup=admin_keyboard())
            except Exception:
                pass
            return
        await safe_answer(cb, "⏳ Выполняю…")
        if act == "stats":
            total, active = user_counts()
            up = int(time.time() - stats["started"])
            await cb.message.answer(f"📊 Пользователей: {total} (активных {active})\nАптайм: {up // 3600} ч\n"
                                    f"Скан: {last_scan_info['stage1']} → {last_scan_info['stage2']} → {last_scan_info['final']}")
        elif act == "users":
            lines = [f"👥 Пользователи ({len(_users())})"] + [
                f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or '—')[:18])}"
                for uid, u in list(_users().items())[-30:] if isinstance(u, dict)]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        elif act == "news_now":
            text, img_url = await prepare_news(force=True)
            if not text:
                return await cb.message.answer("Свежих новостей нет")
            img = await download_bytes(img_url) if img_url else None
            sent = 0
            for cid in list(active_chats):
                try:
                    await _send_new(cid, text, None, img)
                    sent += 1
                except Exception:
                    pass
            await cb.message.answer(f"✅ Отправлено: {sent}")
        elif act == "scan_test":
            await cb.message.answer("🔍 Скан без рассылки…")
            sigs = await scan_once(send=False)
            await cb.message.answer(f"Итог: {len(sigs)} · " + ", ".join(s["coin"] for s in sigs[:10]) if sigs else "Сигналов нет")
        elif act == "diag":
            await cb.message.answer(f"ccxt: {'✅' if ccxt else '❌'} · шифрование: {'✅' if cipher else '❌'}\n"
                                    f"Чатов: {len(active_chats)} · WEBAPP: {WEBAPP_URL or 'не задан'}\n"
                                    f"Пороги: stage1 {CONF_STAGE1:.0f}, final {CONF_FINAL:.0f}")
        elif act == "backup":
            await cb.message.answer("💾 Бэкап отправлен." if await telegram_backup("вручную") else "Бэкап не отправлен.")
        elif act == "restore":
            await cb.message.answer("Отправьте файл bot_state.json как документ.")
        elif act == "timers":
            now = time.time()
            await cb.message.answer("⏳ <b>Таймеры:</b>\n" + "\n".join(
                f"{n}: через {max(0, int(next_timers[k] - now))} с" for n, k in (
                    ("🤖 Сканер", "scanner"), ("📈 Позиции", "positions"), ("📰 Новости", "news_watcher"),
                    ("📊 Сигналы/live", "global_signals"), ("🚨 Аномалии", "anomaly"), ("💾 Бэкап", "backup"))), parse_mode="HTML")

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        if m.from_user.id not in ADMIN_IDS or not (m.document.file_name or "").endswith(".json"):
            return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            data = json.loads((await admin_bot.download_file(f.file_path)).read().decode("utf-8"))
            n = restore_state(data, force=True)
            load_active_chats()
            await asyncio.to_thread(flush_state)
            await m.answer(f"✅ Восстановлено: пользователей {n}, сигналов {len(STATE['signal_history'])}.")
        except Exception as e:
            await m.answer(f"Ошибка восстановления: {html.escape(str(e))}", parse_mode="HTML")


# =====================================================================
# ОБСЛУЖИВАНИЕ ПАМЯТИ, ЗАПУСК
# =====================================================================
def _release_memory():
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


async def memory_janitor_loop():
    await asyncio.sleep(300)
    while True:
        try:
            now = time.time()
            for uid in list(user_res):
                d = user_res[uid]
                for c in [c for c, (ts, _) in d.items() if now - ts > 1800]:
                    d.pop(c, None)
                if not d:
                    user_res.pop(uid, None)
            for c in [c for c, v in list(LIVE.items()) if now - v["ts"] > 1800]:
                LIVE.pop(c, None)
            for c in [c for c, v in analysis_cache.items() if now - v[0] > 600]:
                analysis_cache.pop(c, None)
            for store, ttl in ((_ai_cache, 3600), (_funding_cache, 3600)):
                for k in [k for k, v in store.items() if now - v[0] > ttl]:
                    store.pop(k, None)
            for store in (scan_skip, exchange_bans, BAD_MODELS):
                for k in [k for k, t in store.items() if t < now]:
                    store.pop(k, None)
            if len(fetch_errors) > 100:
                fetch_errors.clear()
            if len(nav_stack) > 1000:
                nav_stack.clear()
            await asyncio.to_thread(_release_memory)
        except Exception as e:
            log.warning(f"janitor: {e}")
        await asyncio.sleep(600)


async def churn_offer_loop():
    """Раз в сутки в 10:00 МСК: тем, кто скоро уйдёт и давно не заходил, — скидка 30% на 24 часа."""
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=10, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        now = time.time()
        for uid_str, u in list(_users().items()):
            try:
                if not isinstance(u, dict) or not uid_str.isdigit():
                    continue
                uid = int(uid_str)
                a = u.get("access") or {}
                if a.get("free") or uid in ADMIN_IDS:
                    continue
                left = a.get("until", 0) - now
                idle = now - u.get("last_seen", 0)
                if -3 * 86400 < left <= 86400 and idle > 3 * 86400:
                    DISCOUNT_OFFERS[uid] = {"pct": 30, "until": now + 86400}
                    await _tell(uid, "🎁 <b>Скучаем!</b> Скидка 30% на продление — 24 часа.\nВыберите тариф в /buy.", paywall_kb())
                    await asyncio.sleep(0.05)
            except Exception as e:
                log.info(f"churn {uid_str}: {e}")


async def heartbeat_loop():
    while True:
        await asyncio.sleep(600)
        log.info("♥ аптайм %d мин, RAM %s МБ, получателей %d, сигналов %d, live %d",
                 (time.time() - stats["started"]) // 60, _rss_mb(), len(active_chats), len(STATE["signal_history"]), len(LIVE))


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
                log.warning(f"{name}: поллинг завершился, перезапуск через 5 сек")
                await asyncio.sleep(5)
                attempt = 0
        except asyncio.CancelledError:
            raise
        except (TelegramNetworkError, aiohttp.ClientError, ConnectionError) as e:
            log.warning(f"{name}: сетевая ошибка ({e}), повтор через {min(2 ** attempt, 60)} сек")
            await asyncio.sleep(min(2 ** attempt, 60))
        except TelegramConflictError:
            await asyncio.sleep(min(2 ** attempt, 30))
        except Exception as e:
            log.error(f"{name}: непредвиденная ошибка: {e}")
            await asyncio.sleep(10)




# =====================================================================
# ЦЕНА В РЕАЛЬНОМ ВРЕМЕНИ: цикл цен и поток Server-Sent Events для приложения
# =====================================================================
PRICES = {}         # coin -> {"price", "ts"}
STREAM_COINS = {}   # coin -> ts последнего открытия экрана монеты


async def price_feed_loop():
    """Цена каждые 8 секунд для монет из сигналов, позиций и открытых экранов приложения."""
    await asyncio.sleep(30)
    while True:
        try:
            now = time.time()
            for c in [c for c, t in list(STREAM_COINS.items()) if now - t > 90]:
                STREAM_COINS.pop(c, None)
            watch = {x["coin"] for x in get_active_signals()}
            for u in list(_users().values()):
                if isinstance(u, dict):
                    watch |= set((u.get("open_positions") or {}).keys())
            watch |= set(list(STREAM_COINS.keys()))
            for coin in sorted(watch)[:30]:
                p = await get_simple_price(coin)
                if p:
                    PRICES[coin] = {"price": p, "ts": time.time()}
                await asyncio.sleep(0.1)
        except Exception as e:
            log.info(f"price_feed: {e}")
        await asyncio.sleep(8)



async def main():
    global http, news_lock, scan_lock, MAIN_LOOP, TG_SEM, ANALYZE_SEM
    MAIN_LOOP = asyncio.get_running_loop()
    keep_alive()
    news_lock, scan_lock = asyncio.Lock(), asyncio.Lock()
    TG_SEM = asyncio.Semaphore(20)
    ANALYZE_SEM = asyncio.Semaphore(3)
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/3.1"})
    load_state_file()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    background, pollers = [], []
    try:
        for tg_bot, key in ((bot, "main_bot"), (admin_bot, "admin_bot")):
            if tg_bot:
                try:
                    runtime[key] = f"@{(await tg_bot.get_me()).username}"
                except Exception as e:
                    log.error(f"get_me ({key}): {e}")
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot:
                await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([types.BotCommand(command="start", description="🚀 Открыть IXCLY")])
            if WEBAPP_URL:
                await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="IXCLY App", web_app=WebAppInfo(url=webapp_link())))
                log.info(f"Mini App: {WEBAPP_URL}")
            else:
                log.warning("WEBAPP_URL не задан: мини-приложение недоступно из бота")
        except Exception as e:
            log.warning(f"Инициализация бота: {e}")

        if POLL_START_DELAY > 0:
            log.info(f"Ждём {POLL_START_DELAY} с, пока остановится предыдущий экземпляр…")
            try:
                await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError:
                pass
        if stop.is_set():
            return

        restored = await telegram_restore()
        if restored:
            log.info(f"✅ Состояние восстановлено из бэкапа, пользователей: {restored}")
        load_active_chats()
        normalize_signals()
        for _uid in list(_users()):
            if str(_uid).isdigit():
                _init_trial(int(_uid))
        gc.freeze()
        asyncio.create_task(discover_models())

        background = [asyncio.create_task(c()) for c in (
            scanner_loop, live_monitor_loop, price_feed_loop, global_signal_monitor_loop, position_monitor_loop,
            news_watcher_loop, price_alerts_loop, morning_briefing_loop, daily_digest_loop, daily_call_loop,
            anomaly_scanner_loop, state_flush_loop, access_reminder_loop, churn_offer_loop, backup_loop, heartbeat_loop, memory_janitor_loop)]
        background.append(asyncio.create_task(update_market_bias()))

        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp:
            pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))
        log.info(f"🚀 IXCLY PRO v{VERSION} запущен")
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
    except Exception as e:
        log.error(f"Критическая ошибка при запуске: {e}", exc_info=True)
    finally:
        log.info("Останавливаю бота…")
        for t in pollers:
            t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        try:
            await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=15)
        except Exception:
            await asyncio.to_thread(flush_state)
        for t in background:
            t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        for tg_bot in (bot, admin_bot):
            if tg_bot:
                try:
                    await tg_bot.session.close()
                except Exception:
                    pass
        await http.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен")
