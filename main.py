# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.5
- ВНИМАНИЕ: Токены поменяны местами в коде (т.к. на Render они перепутаны)
- Исправлено падение при рассылке
"""
import asyncio
import html
import io
import json
import logging
import os
import re
import time
from datetime import datetime
from threading import Thread

import aiohttp
import feedparser
import numpy as np
import pandas as pd
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramNotFound
from flask import Flask
from PIL import Image, ImageDraw, ImageFont

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ixcly")

# ----------------------------------------------------------------------------
# ВЕБ-СЕРВЕР ДЛЯ RENDER
# ----------------------------------------------------------------------------
app = Flask("")

@app.route("/")
def home():
    return "Bot is alive!"

def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

def keep_alive():
    Thread(target=_run_web, daemon=True).start()


# ----------------------------------------------------------------------------
# НАСТРОЙКИ
# ----------------------------------------------------------------------------
def clean_key(key):
    if not key: return None
    return key.strip().strip('"').strip("'")

# --- ВРЕМЕННЫЙ СВАП ТОКЕНОВ (из-за путаницы на Render) ---
# Теперь основной бот берет токен из INPUT_BOT_TOKEN, а админский из BOT_TOKEN
BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN")) 
INPUT_BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))

GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
CRYPTOCOMPARE_KEY = clean_key(os.getenv("CRYPTOCOMPARE_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")

if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения INPUT_BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Инициализация админ-бота
input_bot = None
input_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    try:
        input_bot = Bot(token=INPUT_BOT_TOKEN)
        input_dp = Dispatcher()
    except Exception as e:
        log.error(f"Ошибка инициализации админ-бота: {e}")
        input_bot = None
        input_dp = None

http: aiohttp.ClientSession = None
analysis_sem: asyncio.Semaphore = None

TFS = ["15m", "1h", "4h", "1d"]
TF_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
MIN_CANDLES = 25

POPULAR_COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "TON", "SHIB", "DOT", "LINK",
    "BCH", "NEAR", "POL", "LTC", "ICP", "UNI", "APT", "STX", "FIL", "IMX", "HBAR", "ATOM",
    "INJ", "OP", "ARB", "VET", "MKR", "SUI", "RNDR", "GRT", "THETA", "S", "SAND", "MANA",
    "GALA", "PEPE", "WIF", "BONK", "ORDI", "JUP", "TIA", "SEI",
]

USERS_FILE = "users.json"
active_chats = set()
busy_users = set()
user_last_request = {}
result_cache = {}
RESULT_TTL = 90
COOLDOWN = 3
last_news_link = None


# ----------------------------------------------------------------------------
# ПОЛЬЗОВАТЕЛИ
# ----------------------------------------------------------------------------
def _load_users():
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_user_data(user_id, username, first_name):
    users = _load_users()
    key = str(user_id)
    if key in users:
        return False
    users[key] = {
        "username": username,
        "first_name": first_name,
        "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=4)
    except Exception as e:
        log.warning("Не удалось сохранить users.json: %s", e)
    return True


def load_active_chats():
    for uid in _load_users():
        try:
            active_chats.add(int(uid))
        except ValueError:
            pass
    log.info("Подписчиков на новости: %d", len(active_chats))


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
    buttons.append([InlineKeyboardButton(text="🔍 Поиск монеты", callback_data="search")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def result_keyboard(coin):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"r:{coin}"),
        InlineKeyboardButton(text="📋 Меню", callback_data="menu"),
    ]])


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
    async with http.get(url, params=params, headers=headers,
                        timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.json(content_type=None)


def normalize_ticker(text):
    t = text.upper().strip()
    t = re.sub(r"[\s$/\-_]", "", t)
    if t.endswith("USDT") and len(t) > 4:
        t = t[:-4]
    return t

async def translate_text(text, target="ru"):
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {"client": "gtx", "sl": "en", "tl": target, "dt": "t", "q": text}
        async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                return "".join([item[0] for item in data[0]])
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

    prev
