# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v2.10.22
- Новости полностью на русском + суть + влияние
- Время по Москве
- Полный список монет
- Просторное меню
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
from datetime import datetime
from threading import Thread

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

# ====================== ВЕБ ======================
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
        "mongo": users_col is not None,
        "bias": market_bias,
    })

def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 8080))
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
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
MONGO_URI = clean_key(os.getenv("MONGO_URI"))

ADMIN_IDS = set()
if ADMIN_ID:
    for x in re.split(r"[,\s]+", ADMIN_ID):
        if x.lstrip("-").isdigit():
            ADMIN_IDS.add(int(x))

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не задан")

VERSION = "2.10.22"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN:
    admin_bot = Bot(token=INPUT_BOT_TOKEN)
    admin_dp = Dispatcher()
    log.info("Админ-бот активирован")

http = None

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
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "started": time.time()}
bot_flags = {"news_enabled": True, "alerts_enabled": True}
market_bias = {
    "bias": 0,
    "title_ru": "Нет данных",
    "summary": "Нет данных",
    "impact": "Нейтральный фон",
    "coins": "BTC, ETH, SOL",
    "ts": 0
}

users_col = None

def init_mongo():
    global users_col
    if not MONGO_URI:
        return
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
        users_col = client["ixcly_db"]["users"]
        client.server_info()
        log.info("✅ MongoDB подключена")
    except Exception as e:
        log.error(f"Mongo: {e}")
        users_col = None

# ====================== ПОЛЬЗОВАТЕЛИ ======================
def _load_users():
    if users_col is not None:
        try:
            return {str(u["_id"]): {k: v for k, v in u.items() if k != "_id"} for u in users_col.find()}
        except Exception:
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
        except Exception:
            pass
        return
    try:
        with open(USERS_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        os.replace(USERS_FILE + ".tmp", USERS_FILE)
    except Exception:
        pass

def save_user_data(user_id, username, first_name):
    key = str(user_id)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if users_col is not None:
        try:
            users_col.update_one(
                {"_id": key},
                {"$set": {"username": username, "first_name": first_name, "active": True},
                 "$setOnInsert": {"joined_at": now}},
                upsert=True
            )
        except Exception:
            pass
        return
    users = _load_users()
    if key not in users:
        users[key] = {"username": username, "first_name": first_name, "joined_at": now, "active": True}
        _save_users(users)

def get_user_saved_signals(user_id):
    return _load_users().get(str(user_id), {}).get("saved_signals", [])

def add_user_saved_signal(user_id, coin, text):
    key = str(user_id)
    if users_col is not None:
        try:
            users_col.update_one({"_id": key}, {"$pull": {"saved_signals": {"coin": coin}}})
            users_col.update_one({"_id": key}, {"$push": {"saved_signals": {"coin": coin, "text": text}}}, upsert=True)
        except Exception:
            pass
        return
    users = _load_users()
    if key not in users:
        return
    users[key].setdefault("saved_signals", [])
    users[key]["saved_signals"] = [s for s in users[key]["saved_signals"] if s.get("coin") != coin]
    users[key]["saved_signals"].append({"coin": coin, "text": text})
    _save_users(users)

def del_user_saved_signal(user_id, coin):
    key = str(user_id)
    if users_col is not None:
        try:
            users_col.update_one({"_id": key}, {"$pull": {"saved_signals": {"coin": coin}}})
        except Exception:
            pass
        return
    users = _load_users()
    if key in users:
        users[key]["saved_signals"] = [s for s in users[key].get("saved_signals", []) if s.get("coin") != coin]
        _save_users(users)

def get_user_open_positions(user_id):
    return _load_users().get(str(user_id), {}).get("open_positions", {})

def set_user_open_position(user_id, coin, data):
    key = str(user_id)
    if users_col is not None:
        try:
            if data is None:
                users_col.update_one({"_id": key}, {"$unset": {f"open_positions.{coin}": ""}})
            else:
                users_col.update_one({"_id": key}, {"$set": {f"open_positions.{coin}": data}}, upsert=True)
        except Exception:
            pass
        return
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
if admin_dp:
    admin_dp.update.middleware(UserTrackerMiddleware())

# ====================== КЛАВИАТУРЫ ======================
def get_coins_keyboard(page=0):
    per = 8
    coins = POPULAR_COINS[page*per:(page+1)*per]
    buttons = []
    for i in range(0, len(coins), 2):
        row = [InlineKeyboardButton(text=f"• {coins[i]}", callback_data=f"c:{coins[i]}")]
        if i+1 < len(coins):
            row.append(InlineKeyboardButton(text=f"• {coins[i+1]}", callback_data=f"c:{coins[i+1]}"))
        buttons.append(row)
    total = (len(POPULAR_COINS) + per - 1) // per
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"p:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1} / {total}", callback_data="ignore"))
    if page < total-1:
        nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"p:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="────────────", callback_data="ignore")])
    buttons.append([InlineKeyboardButton(text="📰  Новости", callback_data="news_menu")])
    buttons.append([InlineKeyboardButton(text="🧐  Интересные графики", callback_data="interesting_menu")])
    buttons.append([InlineKeyboardButton(text="🔥  Последние сигналы", callback_data="last_signals")])
    buttons.append([InlineKeyboardButton(text="⭐  Избранное", callback_data="sig:menu")])
    buttons.append([InlineKeyboardButton(text="📊  Статистика 24ч", callback_data="show_stats")])
    buttons.append([InlineKeyboardButton(text="🔍  Поиск монеты", callback_data="search")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def result_keyboard(coin):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"r:{coin}"),
            InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sig:quick_save:{coin}"),
        ],
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
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sig:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_save_coins_keyboard(page=0):
    per = 8
    coins = POPULAR_COINS[page*per:(page+1)*per]
    buttons = []
    for i in range(0, len(coins), 2):
        row = [InlineKeyboardButton(text=coins[i], callback_data=f"sig:do_save:{coins[i]}")]
        if i+1 < len(coins):
            row.append(InlineKeyboardButton(text=coins[i+1], callback_data=f"sig:do_save:{coins[i+1]}"))
        buttons.append(row)
    total = (len(POPULAR_COINS) + per - 1) // per
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sig:save_page:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total}", callback_data="ignore"))
    if page < total-1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sig:save_page:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sig:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_saved_signal_view_keyboard(user_id, coin):
    pos = get_open_position(user_id, coin)
    buttons = [[InlineKeyboardButton(text="🔄 Обновить анализ", callback_data=f"c:{coin}")]]
    if pos:
        buttons.append([InlineKeyboardButton(text="📈 Статус позиции", callback_data=f"pos:status:{coin}")])
        buttons.append([InlineKeyboardButton(text="❌ Закрыл сделку", callback_data=f"pos:close:{coin}")])
    else:
        buttons.append([InlineKeyboardButton(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в избранное", callback_data="sig:menu")])
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
        [InlineKeyboardButton(text="⚡ Рассылка", callback_data="admin:broadcast_now")],
        [InlineKeyboardButton(text="🔧 Диагностика", callback_data="admin:diag"),
         InlineKeyboardButton(text="💾 Бэкап", callback_data="admin:backup")],
        [InlineKeyboardButton(text="📥 Восстановить", callback_data="admin:restore")],
    ])

# ====================== ВСПОМОГАТЕЛЬНЫЕ ======================
def fmt(p):
    p = float(p)
    a = abs(p)
    if a >= 1000: return f"{p:,.2f}".replace(",", " ")
    if a >= 1: return f"{p:.4f}"
    if a >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"

def make_strength_bar(strength: float, length: int = 10) -> str:
    filled = max(0, min(length, int(round(strength / 100 * length))))
    return "🟩" * filled + "🟥" * (length - filled)

async def first_success(coros, timeout=11):
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        for d in done:
            try:
                res = d.result()
                if res is not None:
                    return res
            except Exception:
                pass
        return None
    finally:
        for t in tasks:
            t.cancel()

async def http_json(url, params=None, timeout=8):
    async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
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

# ====================== ИНДИКАТОРЫ ======================
def add_indicators(df):
    c = df["close"]
    df["ema20"] = c.ewm(span=20, adjust=False).mean()
    df["ema50"] = c.ewm(span=50, adjust=False).mean()
    df["ema200"] = c.ewm(span=200, adjust=False).mean()
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi"] = (100 - 100 / (1 + rs)).fillna(50)
    prev = c.shift()
    tr = pd.concat([df["high"]-df["low"], (df["high"]-prev).abs(), (df["low"]-prev).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1/14, adjust=False).mean().fillna(tr.mean())
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
     "params": lambda c, tf: {"instId": f"{c}-USDT", "bar": {"15m":"15m","1h":"1H","4h":"4H","1d":"1D"}[tf], "limit":"180"}, "parse": _p_okx},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline",
     "params": lambda c, tf: {"category": "spot", "symbol": f"{c}USDT", "interval": {"15m":"15","1h":"60","4h":"240","1d":"D"}[tf], "limit": 180}, "parse": _p_bybit},
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
    return {
        "ok": True,
        "is_long": is_long,
        "strength": round(strength, 1),
        "type": "Быстрый сигнал" if fast else "Среднесрок",
    }

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

# ====================== НОВОСТИ ======================
NEWS_FEEDS = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
WORLD_FEEDS = ["https://feeds.bbci.co.uk/news/business/rss.xml"]

_news_cache = {"ts": 0, "entries": []}
_world_cache = {"ts": 0, "titles": []}

async def _load_feed(url):
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=9)) as r:
            if r.status != 200:
                return []
            text = await r.text()
        feed = await asyncio.to_thread(feedparser.parse, text)
        return list(feed.entries)
    except Exception:
        return []

async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 280:
        return _news_cache["entries"]
    for url in NEWS_FEEDS:
        entries = await _load_feed(url)
        if entries:
            _news_cache["ts"] = time.time()
            _news_cache["entries"] = entries[:6]
            return _news_cache["entries"]
    return _news_cache["entries"]

async def get_world_titles(n=5):
    if _world_cache["titles"] and time.time() - _world_cache["ts"] < 280:
        return _world_cache["titles"][:n]
    titles = []
    for url in WORLD_FEEDS:
        entries = await _load_feed(url)
        titles.extend([e.title for e in entries[:3]])
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
        entries = await get_news_entries()
        crypto = [e.title for e in entries]
        world = await get_world_titles(5)

        prompt = f"""Ты профессиональный крипто-аналитик. 
Переведи и разбери новость на русском языке.

Новости:
{chr(10).join('- ' + t for t in crypto[:3])}

Мировые:
{chr(10).join('- ' + t for t in world)}

Ответь СТРОГО в формате:

BIAS: BULLISH / BEARISH / NEUTRAL

TITLE_RU: перевод заголовка на русский

SUMMARY: 2-4 предложения — в чём суть новости и почему это важно

IMPACT: как это влияет на крипторынок (1-2 предложения)

COINS: какие монеты больше всего затронет (BTC, ETH, SOL, ZEC и т.д.)
"""
        text, _ = await ask_ai_named(prompt, timeout=18)
        
        bias = 0
        title_ru = "Новость"
        summary = "Нет данных"
        impact = "Нейтральный фон"
        coins = "BTC, ETH, SOL"

        if text:
            upper = text.upper()
            if "BULLISH" in upper:
                bias = 1
            elif "BEARISH" in upper:
                bias = -1

            m = re.search(r"TITLE_RU:\s*(.+)", text, re.I)
            if m:
                title_ru = m.group(1).strip().split("\n")[0][:130]

            m = re.search(r"SUMMARY:\s*(.+?)(?=IMPACT:|$)", text, re.I | re.S)
            if m:
                summary = m.group(1).strip()[:380]

            m = re.search(r"IMPACT:\s*(.+?)(?=COINS:|$)", text, re.I | re.S)
            if m:
                impact = m.group(1).strip()[:220]

            m = re.search(r"COINS:\s*(.+)", text, re.I)
            if m:
                coins = m.group(1).strip()[:90]

        market_bias = {
            "bias": bias,
            "title_ru": title_ru,
            "summary": summary,
            "impact": impact,
            "coins": coins,
            "ts": time.time()
        }
        log.info(f"Bias обновлён: {bias} | {title_ru}")
    except Exception as e:
        log.warning(f"bias error: {e}")

# ====================== ИИ ======================
async def _ask_gemini(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    for key in GEMINI_API_KEYS:
        try:
            async with http.post(url, json=payload, headers={"x-goog-api-key": key}, timeout=aiohttp.ClientTimeout(total=16)) as r:
                j = await r.json(content_type=None)
                if r.status == 200:
                    return j["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            continue
    return None

async def _ask_openai_compat(base, key, model, prompt):
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1100}
    try:
        async with http.post(f"{base}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=18)) as r:
            j = await r.json(content_type=None)
            if r.status == 200:
                return j["choices"][0]["message"]["content"]
    except Exception:
        pass
    return None

async def ask_ai_named(prompt, timeout=15):
    tasks = []
    if GEMINI_API_KEYS:
        tasks.append(("Gemini", lambda: _ask_gemini(prompt)))
    if GROQ_API_KEY:
        tasks.append(("Groq", lambda: _ask_openai_compat("https://api.groq.com/openai/v1", GROQ_API_KEY, "llama-3.1-8b-instant", prompt)))
    if OPENROUTER_API_KEY:
        tasks.append(("OpenRouter", lambda: _ask_openai_compat("https://openrouter.ai/api/v1", OPENROUTER_API_KEY, "meta-llama/llama-3.3-70b-instruct:free", prompt)))
    if not tasks:
        return None, None
    async def wrap(name, fn):
        try:
            t = await asyncio.wait_for(fn(), timeout=timeout)
            if t and len(t) > 20:
                return t, name
        except Exception:
            pass
        return None
    res = await first_success([wrap(n, f) for n, f in tasks], timeout=timeout + 2)
    return res if res else (None, None)

# ====================== СКАНЕР ======================
SCAN_INTERVAL = 17 * 60

async def scan_once(send=True):
    global last_signals_data
    if time.time() - market_bias["ts"] > 900:
        await update_market_bias()
    bias = market_bias["bias"]
    bias_text = market_bias.get("impact", "Нейтральный фон")
    sem = asyncio.Semaphore(3)
    candidates = []

    async def check(coin):
        async with sem:
            try:
                tf_data, source = await get_market_data(coin)
                tech = tech_check(tf_data)
                if not tech:
                    return
                if bias == 1 and not tech["is_long"]:
                    return
                if bias == -1 and tech["is_long"]:
                    return
                candidates.append({"coin": coin, "source": source, "tech": tech, "tf_data": tf_data})
            except Exception:
                pass

    await asyncio.gather(*[check(c) for c in POPULAR_COINS])
    candidates.sort(key=lambda x: x["tech"]["strength"], reverse=True)
    candidates = candidates[:4]

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
            img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
        except Exception:
            img = None
        signals.append({
            "coin": coin, "text": text, "img": img,
            "is_long": is_long, "entry": entry, "tps": tps, "sls": sls,
            "type": tech["type"], "strength": tech["strength"]
        })
        hist.append({
            "ts": time.time(), "coin": coin, "side": side,
            "type": tech["type"], "strength": tech["strength"], "text": text
        })

    _save_signal_history(hist)

    if signals and send:
        lines = [f"🔥 <b>Сигналы</b> · {datetime.now().strftime('%H:%M')} МСК\n"]
        kb = []
        for s in signals:
            emoji = "🟢" if s["is_long"] else "🔴"
            lines.append(f"{emoji} <b>{s['coin']}</b> · {s['type']} · {s['strength']}%")
            kb.append([InlineKeyboardButton(text=f"{emoji} {s['coin']}", callback_data=f"c:{s['coin']}")])
        lines.append(f"\n🌍 {html.escape(bias_text)}")
        text = "\n".join(lines)
        kb.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu")])
        markup = InlineKeyboardMarkup(inline_keyboard=kb)
        last_signals_data = {"text": text, "kb": markup, "ts": time.time()}
        await broadcast({"kind": "text", "text": text, "markup": markup})
        stats["signals"] += len(signals)
    return signals

async def scanner_loop():
    await asyncio.sleep(20)
    while True:
        try:
            if bot_flags["alerts_enabled"]:
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner: {e}")
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
    cw = w - pad_l - pad_r
    ch = h - pad_t - pad_b
    highs = df["high"].values
    lows = df["low"].values
    mn = min(lows.min(), entry, min(sls), min(tps)) * 0.997
    mx = max(highs.max(), entry, max(sls), max(tps)) * 1.003
    rng = mx - mn or 1
    def y(p):
        return pad_t + ch - (p - mn) / rng * ch
    n = len(df)
    cw_c = max(3, int(cw / n) - 1)
    for i in range(n):
        x = pad_l + i * (cw / n) + cw_c // 2
        o, hh, ll, c = df["open"].iloc[i], highs[i], lows[i], df["close"].iloc[i]
        color = (0, 170, 80) if c >= o else (200, 45, 45)
        draw.line([(x, y(hh)), (x, y(ll))], fill=color, width=1)
        top, bot = min(y(o), y(c)), max(y(o), y(c))
        draw.rectangle([x - cw_c//2, top, x + cw_c//2, bot], fill=color)
    def lvl(price, label, color, dash=False):
        yy = y(price)
        if dash:
            for xx in range(pad_l, w - pad_r, 7):
                draw.line([(xx, yy), (xx + 3, yy)], fill=color, width=1)
        else:
            draw.line([(pad_l, yy), (w - pad_r, yy)], fill=color, width=2)
        draw.text((pad_l + 3, yy - 12), f"{label} {fmt(price)}", fill=color, font=font)
    lvl(entry, "Entry", (255, 190, 0))
    for i, tp in enumerate(tps):
        lvl(tp, f"TP{i+1}", (0, 190, 90), True)
    for i, sl in enumerate(sls):
        lvl(sl, f"SL{i+1}", (220, 50, 50), True)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()

# ====================== BROADCAST + АНАЛИЗ ======================
async def broadcast(payload, only_ids=None):
    targets = only_ids or list(active_chats)
    sent = failed = 0
    for cid in targets:
        try:
            if payload.get("kind") == "photo" and payload.get("data"):
                photo = BufferedInputFile(payload["data"], filename="chart.png")
                if len(payload.get("text", "")) <= 1024:
                    await bot.send_photo(cid, photo, caption=payload["text"], parse_mode="HTML", reply_markup=payload.get("markup"))
                else:
                    await bot.send_photo(cid, photo)
                    await bot.send_message(cid, payload["text"][:4000], parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
            else:
                await bot.send_message(cid, payload.get("text", "")[:4000], parse_mode="HTML", reply_markup=payload.get("markup"), disable_web_page_preview=True)
            sent += 1
            await asyncio.sleep(0.04)
        except (TelegramForbiddenError, TelegramNotFound):
            active_chats.discard(cid)
        except Exception:
            failed += 1
    stats["broadcasts"] += 1
    return sent, failed, 0, []

async def analyze_coin(coin, user_id=None):
    try:
        tf_data, source = await get_market_data(coin)
    except Exception:
        return None, f"⚠️ Не удалось получить данные по <b>{coin}</b>"
    tech = tech_check(tf_data)
    if tech:
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
        f"🌍 Фон: {html.escape(market_bias.get('impact', 'Нейтральный'))}"
    )
    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception:
        img = None
    return {
        "text": text, "img": img, "coin": coin,
        "entry": entry, "tps": tps, "sls": sls,
        "is_long": is_long, "side": side,
        "price": info["price"], "type": sig_type, "strength": strength
    }, None

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
        await asyncio.sleep(320)

async def check_position(user_id, coin, pos):
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
        pnl = (price - entry) / entry * 100
        hit_tp1 = price >= tp1
        hit_tp2 = price >= tp2
        hit_tp3 = price >= tp3
        hit_sl = price <= sl1
    else:
        pnl = (entry - price) / entry * 100
        hit_tp1 = price <= tp1
        hit_tp2 = price <= tp2
        hit_tp3 = price <= tp3
        hit_sl = price >= sl1
    reason = None
    urgency = "info"
    if hit_sl:
        reason = f"🛑 Сработал стоп-лосс\nРекомендую закрыть позицию."
        urgency = "danger"
    elif hit_tp3:
        reason = f"✅ Достигнут TP3\nМожно фиксировать всю позицию."
        urgency = "success"
    elif hit_tp2 and not pos.get("notified_tp"):
        reason = f"✅ Достигнут TP2\nЧастично фиксируем + стоп в безубыток."
        urgency = "success"
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
    elif hit_tp1 and not pos.get("notified_tp"):
        reason = f"✅ Достигнут TP1\nМожно закрыть часть позиции."
        urgency = "success"
        pos["notified_tp"] = True
        set_user_open_position(user_id, coin, pos)
    elif time.time() - market_bias["ts"] < 1800:
        bias = market_bias["bias"]
        if side == "LONG" and bias == -1:
            reason = f"⚠️ Новостной фон стал медвежьим\n{market_bias.get('impact')}\nЛучше зафиксировать ({pnl:+.1f}%)."
            urgency = "warning"
        elif side == "SHORT" and bias == 1:
            reason = f"⚠️ Новостной фон стал бычьим\n{market_bias.get('impact')}\nЛучше закрыть шорт ({pnl:+.1f}%)."
            urgency = "warning"
    if not reason:
        return
    text = (
        f"{'🛑' if urgency=='danger' else '✅' if urgency=='success' else '⚠️'} <b>Рекомендация по {coin}</b>\n\n"
        f"Направление: <b>{side}</b>\n"
        f"Вход: <code>{fmt(entry)}</code>\n"
        f"Сейчас: <code>{fmt(price)}</code>\n"
        f"PnL: <b>{pnl:+.2f}%</b>\n\n"
        f"{reason}"
    )
    try:
        await bot.send_message(user_id, text, parse_mode="HTML",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")],
                                   [InlineKeyboardButton(text="❌ Закрыл", callback_data=f"pos:close:{coin}")]
                               ]))
        if urgency in ("danger", "success"):
            pos["notified_close"] = True
            set_user_open_position(user_id, coin, pos)
    except Exception:
        pass

# ====================== ХЕНДЛЕРЫ ======================
@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message):
    save_user_data(m.from_user.id, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    await m.answer(
        f"👋 <b>IXCLY v{VERSION}</b>\n\nВыбери монету:",
        reply_markup=get_coins_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.message.answer(
        f"👋 <b>IXCLY v{VERSION}</b>\n\nВыбери монету:",
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
        await cb.answer("Уже обрабатываю…")
        return
    busy_users.add(uid)
    try:
        await cb.answer("⏳ Анализирую…")
        res, err = await analyze_coin(coin)
        if err:
            await cb.message.answer(err, parse_mode="HTML")
            return
        await safe_delete(cb.message)
        if res["img"]:
            await bot.send_photo(cb.message.chat.id, BufferedInputFile(res["img"], f"{coin}.png"),
                                 caption=res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin))
        else:
            await cb.message.answer(res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin), disable_web_page_preview=True)
        result_cache[uid] = {"coin": coin, "text": res["text"], "ts": time.time(), **res}
    finally:
        busy_users.discard(uid)

@dp.callback_query(F.data.startswith("r:"))
async def cb_refresh(cb: types.CallbackQuery):
    await cb_coin(cb)

@dp.callback_query(F.data == "news_menu")
async def cb_news_menu(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.answer("📰 Загружаю и перевожу новость…")
    try:
        if time.time() - market_bias.get("ts", 0) > 600:
            await update_market_bias()

        entries = await get_news_entries()
        if not entries:
            await cb.message.answer("📭 Свежих новостей пока нет", reply_markup=get_coins_keyboard())
            return

        entry = entries[0]
        img_url = extract_image(entry)

        title_ru = market_bias.get("title_ru", entry.title)
        summary = market_bias.get("summary", "Нет подробностей")
        impact = market_bias.get("impact", "Нейтральный фон")
        coins = market_bias.get("coins", "BTC, ETH, SOL")
        moscow_time = datetime.now().strftime("%H:%M") + " МСК"

        text = (
            f"📰 <b>Последняя новость</b>\n"
            f"<i>{moscow_time}</i>\n\n"
            f"<b>{html.escape(title_ru)}</b>\n\n"
            f"📌 <b>Суть:</b>\n{html.escape(summary)}\n\n"
            f"📊 <b>Влияние на рынок:</b>\n{html.escape(impact)}\n\n"
            f"🪙 <b>Какие монеты затронет:</b>\n{html.escape(coins)}"
        )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ])

        if img_url:
            try:
                async with http.get(img_url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        img_data = await r.read()
                        await cb.message.answer_photo(
                            BufferedInputFile(img_data, "news.jpg"),
                            caption=text,
                            parse_mode="HTML",
                            reply_markup=kb
                        )
                        return
            except Exception:
                pass

        await cb.message.answer(text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        await cb.message.answer(f"Ошибка загрузки новости: {e}", reply_markup=get_coins_keyboard())

@dp.callback_query(F.data == "last_signals")
async def cb_last(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    if last_signals_data.get("text") and time.time() - last_signals_data.get("ts", 0) < 3600:
        await cb.message.answer(last_signals_data["text"], parse_mode="HTML", reply_markup=last_signals_data.get("kb"))
    else:
        hist = _load_signal_history()
        if not hist:
            await cb.message.answer("📭 Свежих сигналов нет", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]))
        else:
            text = "📜 <b>История за 24ч</b>\n\n"
            for h in hist[-8:][::-1]:
                emoji = "🟢" if h.get("side") == "LONG" else "🔴"
                text += f"{emoji} <b>{h['coin']}</b> · {h.get('type','')} · {h.get('strength',0)}%\n"
            await cb.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]))
    await cb.answer()

@dp.callback_query(F.data == "show_stats")
async def cb_stats(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.answer("📊 Собираю…")
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        data = {x["symbol"]: x for x in j}
        gainers, losers = [], []
        for c in POPULAR_COINS:
            s = f"{c}USDT"
            if s in data:
                ch = float(data[s]["priceChangePercent"])
                (gainers if ch > 0 else losers).append((c, ch))
        gainers.sort(key=lambda x: x[1], reverse=True)
        losers.sort(key=lambda x: x[1])
        text = "📊 <b>Топ за 24ч</b>\n\n🟢 <b>Рост:</b>\n"
        for c, ch in gainers[:8]:
            text += f"<code>{c}</code> +{ch:.1f}%\n"
        text += "\n🔴 <b>Падение:</b>\n"
        for c, ch in losers[:8]:
            text += f"<code>{c}</code> {ch:.1f}%\n"
        await cb.message.answer(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]]))
    except Exception as e:
        await cb.message.answer(f"Ошибка: {e}")

@dp.callback_query(F.data == "interesting_menu")
async def cb_interesting(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.answer("🧐 Ищу…")
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=9)
        movers = [x for x in j if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
        movers.sort(key=lambda x: float(x["priceChangePercent"]), reverse=True)
        text = "🧐 <b>Интересные за 24ч</b>\n\n🟢 Топ роста:\n"
        for x in movers[:4]:
            text += f"<code>{x['symbol'][:-4]}</code> +{float(x['priceChangePercent']):.1f}%\n"
        text += "\n🔴 Топ падения:\n"
        for x in movers[-4:]:
            text += f"<code>{x['symbol'][:-4]}</code> {float(x['priceChangePercent']):.1f}%\n"
        await cb.message.answer(text, parse_mode="HTML", reply_markup=get_coins_keyboard())
    except Exception:
        await cb.message.answer("Не удалось получить данные")

@dp.callback_query(F.data == "search")
async def cb_search(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.message.answer("🔍 Напиши тикер (BTC, SOL, PEPE…):")
    await cb.answer()

@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,12}$"))
async def msg_ticker(m: types.Message):
    coin = normalize_ticker(m.text)
    uid = m.from_user.id
    if uid in busy_users:
        return
    busy_users.add(uid)
    try:
        res, err = await analyze_coin(coin)
        if err:
            await m.answer(err, parse_mode="HTML")
            return
        if res["img"]:
            await m.answer_photo(BufferedInputFile(res["img"], f"{coin}.png"), caption=res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin))
        else:
            await m.answer(res["text"], parse_mode="HTML", reply_markup=result_keyboard(coin), disable_web_page_preview=True)
        result_cache[uid] = {"coin": coin, "text": res["text"], "ts": time.time(), **res}
    finally:
        busy_users.discard(uid)

# Избранное
@dp.callback_query(F.data == "sig:menu")
async def cb_sig_menu(cb: types.CallbackQuery):
    await safe_delete(cb.message)
    await cb.message.answer("⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), parse_mode="HTML")
    await cb.answer()

@dp.callback_query(F.data == "sig:save_new")
async def cb_sig_save_new(cb: types.CallbackQuery):
    await cb.message.edit_text("Выбери монету:", reply_markup=get_save_coins_keyboard())
    await cb.answer()

@dp.callback_query(F.data.startswith("sig:save_page:"))
async def cb_sig_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[2])
    await cb.message.edit_reply_markup(reply_markup=get_save_coins_keyboard(page))
    await cb.answer()

@dp.callback_query(F.data.startswith("sig:do_save:") | F.data.startswith("sig:quick_save:"))
async def cb_sig_save(cb: types.CallbackQuery):
    coin = cb.data.split(":")[-1]
    uid = cb.from_user.id
    cached = result_cache.get(uid)
    text = cached["text"] if cached and cached.get("coin") == coin else f"Сигнал {coin}"
    add_user_saved_signal(uid, coin, text)
    await cb.answer(f"✅ {coin} сохранён")
    if "quick_save" not in cb.data:
        await cb.message.edit_text("⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(uid), parse_mode="HTML")

@dp.callback_query(F.data.startswith("sig:view:"))
async def cb_sig_view(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    saved = get_user_saved_signals(cb.from_user.id)
    item = next((s for s in saved if s["coin"] == coin), None)
    if not item:
        await cb.answer("Не найдено", show_alert=True)
        return
    await cb.message.answer(item["text"], parse_mode="HTML",
                            reply_markup=get_saved_signal_view_keyboard(cb.from_user.id, coin),
                            disable_web_page_preview=True)
    await cb.answer()

@dp.callback_query(F.data == "sig:del_menu")
async def cb_sig_del_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Выбери для удаления:", reply_markup=get_delete_signals_menu(cb.from_user.id))
    await cb.answer()

@dp.callback_query(F.data.startswith("sig:del:"))
async def cb_sig_del(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    del_user_saved_signal(cb.from_user.id, coin)
    await cb.answer(f"🗑 {coin} удалён")
    await cb.message.edit_text("⭐ <b>Избранное</b>", reply_markup=get_saved_signals_menu(cb.from_user.id), parse_mode="HTML")

# Позиции
@dp.callback_query(F.data.startswith("pos:enter:"))
async def cb_pos_enter(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    uid = cb.from_user.id
    res, err = await analyze_coin(coin)
    if err:
        await cb.answer("Не удалось получить уровни", show_alert=True)
        return
    data = {
        "side": res["side"], "entry_price": res["entry"],
        "entered_at": time.time(),
        "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2],
        "sl1": res["sls"][0], "sl2": res["sls"][1],
        "notified_close": False, "notified_tp": False
    }
    set_user_open_position(uid, coin, data)
    await cb.message.answer(
        f"{'🟢' if res['side']=='LONG' else '🔴'} <b>Позиция открыта</b>\n\n"
        f"Монета: <b>{coin}</b>\nНаправление: <b>{res['side']}</b>\nВход: <code>{fmt(res['entry'])}</code>\n\n"
        f"Буду следить и сообщу, когда лучше закрыть.",
        parse_mode="HTML", reply_markup=get_saved_signal_view_keyboard(uid, coin)
    )
    await cb.answer("✅ Зафиксировал")

@dp.callback_query(F.data.startswith("pos:close:"))
async def cb_pos_close(cb: types.CallbackQuery):
    coin = cb.data.split(":")[2]
    set_user_open_position(cb.from_user.id, coin, None)
    await cb.answer("❌ Позиция закрыта")
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
        await cb.answer("Нет цены", show_alert=True)
        return
    entry = pos["entry_price"]
    pnl = ((price - entry) / entry * 100) if pos["side"] == "LONG" else ((entry - price) / entry * 100)
    text = (
        f"📈 <b>Статус {coin}</b>\n\n"
        f"Направление: <b>{pos['side']}</b>\n"
        f"Вход: <code>{fmt(entry)}</code>\n"
        f"Сейчас: <code>{fmt(price)}</code>\n"
        f"PnL: <b>{pnl:+.2f}%</b>\n\n"
        f"TP1: <code>{fmt(pos['tp1'])}</code>\n"
        f"TP2: <code>{fmt(pos['tp2'])}</code>\n"
        f"SL1: <code>{fmt(pos['sl1'])}</code>"
    )
    await cb.message.answer(text, parse_mode="HTML")
    await cb.answer()

@dp.callback_query(F.data == "ignore")
async def cb_ignore(cb: types.CallbackQuery):
    await cb.answer()

# ====================== АДМИН-БОТ ======================
if admin_dp:

    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_cmd_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            await m.answer("⛔ Доступ только для администратора")
            return
        total, active, _, _ = user_counts()
        text = f"✅ <b>Админ-панель IXCLY v{VERSION}</b>\n👥 Пользователей: {total} (активных {active})"
        if total == 0:
            text += "\n\n⚠️ База пуста"
        await m.answer(text, reply_markup=get_admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            await cb.answer("Нет доступа", show_alert=True)
            return
        act = cb.data.split(":")[1]

        if act == "stats":
            total, active, _, _ = user_counts()
            up = int(time.time() - stats["started"])
            text = (
                f"📊 <b>Статистика</b>\n\n"
                f"Версия: {VERSION}\n"
                f"Юзеров: {total} (активных {active})\n"
                f"Сигналов: {stats['signals']}\n"
                f"Аптайм: {up//3600}ч {up%3600//60}м\n"
                f"Bias: {market_bias['bias']} | {market_bias.get('title_ru', '')[:60]}"
            )
            await cb.message.answer(text, parse_mode="HTML")

        elif act == "users":
            users = _load_users()
            lines = [f"👥 Пользователи ({len(users)})\n"]
            for uid, u in list(users.items())[:30]:
                name = u.get("first_name") or u.get("username") or "—"
                lines.append(f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(name)[:18])}")
            await cb.message.answer("\n".join(lines), parse_mode="HTML")

        elif act == "toggle_news":
            bot_flags["news_enabled"] = not bot_flags["news_enabled"]
            await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            await cb.answer(f"Новости: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}")

        elif act == "toggle_scan":
            bot_flags["alerts_enabled"] = not bot_flags["alerts_enabled"]
            await cb.message.edit_reply_markup(reply_markup=get_admin_keyboard())
            await cb.answer(f"Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}")

        elif act == "news_now":
            await cb.answer("Отправляю…")
            try:
                if time.time() - market_bias.get("ts", 0) > 300:
                    await update_market_bias()
                title_ru = market_bias.get("title_ru", "Новость")
                summary = market_bias.get("summary", "")
                impact = market_bias.get("impact", "")
                coins = market_bias.get("coins", "")
                short = (
                    f"📰 <b>{html.escape(title_ru)}</b>\n\n"
                    f"{html.escape(summary)}\n\n"
                    f"📊 {html.escape(impact)}\n"
                    f"🪙 {html.escape(coins)}"
                )
                await broadcast({"kind": "text", "text": short})
                stats["news_sent"] += 1
                await cb.message.answer("Новость отправлена")
            except Exception as e:
                await cb.message.answer(f"Ошибка: {e}")

        elif act == "scan_test":
            await cb.answer("Сканирую…")
            sigs = await scan_once(send=True)
            await cb.message.answer(f"Готово. Сигналов: {len(sigs)}")

        elif act == "broadcast_now":
            if last_signals_data.get("text"):
                sent, _, _, _ = await broadcast({"kind": "text", "text": last_signals_data["text"], "markup": last_signals_data.get("kb")})
                await cb.message.answer(f"Рассылка: {sent}")
            else:
                await cb.message.answer("Нет сигналов")

        elif act == "diag":
            text = (
                f"🔧 Диагностика\n\n"
                f"Mongo: {'✅' if users_col else '❌'}\n"
                f"Чатов: {len(active_chats)}\n"
                f"Gemini: {len(GEMINI_API_KEYS)}\n"
                f"ADMIN_IDS: {ADMIN_IDS}\n"
                f"Bias: {market_bias.get('title_ru')}"
            )
            await cb.message.answer(text)

        elif act == "backup":
            users = _load_users()
            data = json.dumps(users, ensure_ascii=False, indent=2).encode()
            await cb.message.answer_document(BufferedInputFile(data, "users.json"), caption=f"Бэкап · {len(users)} юзеров")

        elif act == "restore":
            await cb.message.answer("Пришли файл users.json")

        await cb.answer()

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        if not m.document.file_name.endswith(".json"):
            return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            content = await admin_bot.download_file(f.file_path)
            data = json.loads(content.read().decode())
            if users_col is not None:
                users_col.delete_many({})
                for uid, ud in data.items():
                    users_col.update_one({"_id": str(uid)}, {"$set": ud}, upsert=True)
            else:
                with open(USERS_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            load_active_chats()
            await m.answer(f"✅ Восстановлено: {len(data)} пользователей")
        except Exception as e:
            await m.answer(f"Ошибка: {e}")

# ====================== MAIN ======================
async def main():
    global http
    keep_alive()
    init_mongo()
    load_active_chats()

    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/2.10"})

    asyncio.create_task(scanner_loop())
    asyncio.create_task(position_monitor_loop())
    asyncio.create_task(update_market_bias())

    log.info(f"🚀 IXCLY v{VERSION} запущен | монет: {len(POPULAR_COINS)}")
    
    tasks = [dp.start_polling(bot)]
    if admin_bot and admin_dp:
        tasks.append(admin_dp.start_polling(admin_bot))

    try:
        await asyncio.gather(*tasks)
    finally:
        await http.close()
        await bot.session.close()
        if admin_bot:
            await admin_bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановлен")
