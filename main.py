# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v2.8
- Цепочка ИИ: Gemini/Groq/OpenRouter (с ключами) -> бесплатный GPT без ключа (Pollinations) -> локальный движок
- Локальный движок работает ВСЕГДА (мульти-ТФ + волатильность + новостной сентимент) и даёт ENTER/WAIT
- Мировые + крипто-новости идут в анализ каждой монеты
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
from urllib.parse import quote

import aiohttp
import feedparser
import numpy as np
import pandas as pd
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramForbiddenError, TelegramNotFound
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
    if not key:
        return None
    return key.strip().strip('"').strip("'")


BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
CRYPTOCOMPARE_KEY = clean_key(os.getenv("CRYPTOCOMPARE_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))  # необязательно (бесплатный ключ на enter.pollinations.ai)
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))

# gemini-1.5-flash отключён Google, поэтому по умолчанию актуальная модель
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")

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
notified_users = set()
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


# --- МУЛЬТИ-ПЕРЕВОДЧИК (4 ПРОВАЙДЕРА) ---
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
    rows = [[float(x[0]) * 1000, float(x[1]), float(x[3]), float(x[4]), float(x[2]), float(x[5])]
            for x in reversed(j["data"])]
    return rows[-200:]


def _p_okx(j):
    if j.get("code") != "0" or not j.get("data"):
        raise ValueError("bad response")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in reversed(j["data"])]


def _p_bybit(j):
    if j.get("retCode") != 0 or not j.get("result", {}).get("list"):
        raise ValueError("bad response")
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])]
            for x in reversed(j["result"]["list"])]


EXCHANGES = [
    {
        "name": "BINANCE",
        "url": "https://data-api.binance.vision/api/v3/klines",
        "params": lambda c, tf: {"symbol": f"{c}USDT", "interval": tf, "limit": 200},
        "parse": _p_binance,
    },
    {
        "name": "MEXC",
        "url": "https://api.mexc.com/api/v3/klines",
        "params": lambda c, tf: {"symbol": f"{c}USDT", "interval": {"1h": "60m"}.get(tf, tf), "limit": 200},
        "parse": _p_binance,
    },
    {
        "name": "GATE",
        "url": "https://api.gateio.ws/api/v4/spot/candlesticks",
        "params": lambda c, tf: {"currency_pair": f"{c}_USDT", "interval": tf, "limit": 200},
        "parse": _p_gate,
    },
    {
        "name": "KUCOIN",
        "url": "https://api.kucoin.com/api/v1/market/candles",
        "params": lambda c, tf: {
            "symbol": f"{c}-USDT",
            "type": {"15m": "15min", "1h": "1hour", "4h": "4hour", "1d": "1day"}[tf],
            "startAt": int(time.time() - 200 * TF_SECONDS[tf]),
            "endAt": int(time.time()),
        },
        "parse": _p_kucoin,
    },
    {
        "name": "OKX",
        "url": "https://www.okx.com/api/v5/market/candles",
        "params": lambda c, tf: {"instId": f"{c}-USDT", "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf], "limit": 200},
        "parse": _p_okx,
    },
    {
        "name": "BYBIT",
        "url": "https://api.bybit.com/v5/market/kline",
        "params": lambda c, tf: {"category": "spot", "symbol": f"{c}USDT",
                                 "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf], "limit": 200},
        "parse": _p_bybit,
    },
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
            log.info("%s %s: %s", ex["name"], coin, r)
            return None
    return await asyncio.to_thread(build_tf_data, dict(zip(TFS, results))), ex["name"]


async def fetch_cryptocompare(coin):
    specs = {"15m": ("histominute", 15), "1h": ("histohour", 1), "4h": ("histohour", 4), "1d": ("histoday", 1)}
    headers = {"authorization": f"Apikey {CRYPTOCOMPARE_KEY}"} if CRYPTOCOMPARE_KEY else None

    async def one(tf):
        ep, agg = specs[tf]
        j = await http_json(
            f"https://min-api.cryptocompare.com/data/v2/{ep}",
            params={"fsym": coin, "tsym": "USD", "limit": 200, "aggregate": agg},
            headers=headers, timeout=10,
        )
        if j.get("Response") != "Success":
            raise ValueError("not found")
        data = j["Data"]["Data"]
        rows = [[d["time"] * 1000, d["open"], d["high"], d["low"], d["close"], d.get("volumeto", 0)]
                for d in data if d.get("close", 0) > 0]
        if len(rows) < MIN_CANDLES:
            raise ValueError("мало свечей")
        return rows

    results = await asyncio.gather(*[one(tf) for tf in TFS], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            log.info("CRYPTOCOMPARE %s: %s", coin, r)
            return None
    return await asyncio.to_thread(build_tf_data, dict(zip(TFS, results))), "CRYPTOCOMPARE"


async def get_market_data(coin):
    res = await first_success([fetch_from_exchange(ex, coin) for ex in EXCHANGES], timeout=12)
    if res:
        return res
    res = await first_success([fetch_cryptocompare(coin)], timeout=15)
    if res:
        return res
    raise LookupError(coin)


# ----------------------------------------------------------------------------
# ИИ: ключевые провайдеры -> бесплатный GPT без ключа
# ----------------------------------------------------------------------------
async def _ask_gemini(prompt, json_mode):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    if json_mode:
        payload["generationConfig"]["responseMimeType"] = "application/json"
    for key in GEMINI_API_KEYS:
        try:
            async with http.post(url, json=payload, headers={"x-goog-api-key": key},
                                 timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
                if r.status != 200:
                    log.warning(f"Gemini API Error ({r.status}): {str(j)[:100]}")
                    continue
                return j["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            log.warning("Gemini timeout/error: %s", e)
    return None


async def _ask_openai_compat(base_url, api_key, model, prompt, json_mode, extra_headers=None, use_json_format=False):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}],
               "temperature": 0.2, "max_tokens": 900}
    if json_mode and use_json_format:
        payload["response_format"] = {"type": "json_object"}
    try:
        async with http.post(f"{base_url}/chat/completions", json=payload, headers=headers,
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
            j = await r.json(content_type=None)
            if r.status != 200 or "choices" not in j:
                log.warning(f"{base_url} Error ({r.status}): {str(j)[:100]}")
                return None
            return j["choices"][0]["message"]["content"]
    except Exception as e:
        log.warning("%s timeout/error: %s", base_url, e)
        return None


def _looks_like_junk(text):
    """Бесплатные сервисы иногда отдают уведомления/HTML/ошибки вместо ответа."""
    t = (text or "").strip()
    low = t.lower()
    if len(t) < 40:
        return True
    if low.startswith("<!doctype") or low.startswith("<html"):
        return True
    if "pollinations" in low or "deprecat" in low or "rate limit" in low:
        return True
    return False


async def _ask_free_gpt(prompt, json_mode):
    """GPT без ключа (Pollinations). Несколько эндпоинтов подряд, первый нормальный ответ побеждает."""
    system = "Ты опытный крипто-трейдер и макро-аналитик. Отвечай кратко, по делу, на русском языке."
    if json_mode:
        system += " Отвечай только валидным JSON без markdown и пояснений."
    auth = {"Authorization": f"Bearer {POLLINATIONS_KEY}"} if POLLINATIONS_KEY else {}

    async def post_openai(url, timeout):
        payload = {
            "model": POLLINATIONS_MODEL,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
            "temperature": 0.2,
        }
        async with http.post(url, json=payload, headers={"Content-Type": "application/json", **auth},
                             timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            j = await r.json(content_type=None)
            return j["choices"][0]["message"]["content"]

    async def get_plain(url, timeout):
        async with http.get(url, params={"model": POLLINATIONS_MODEL, "system": system}, headers=auth,
                            timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.text()

    short_prompt = quote(prompt[:3000], safe="")
    attempts = [
        ("text.pollinations.ai/openai", lambda: post_openai("https://text.pollinations.ai/openai", 8)),
        ("gen.pollinations.ai/text", lambda: get_plain(f"https://gen.pollinations.ai/text/{short_prompt}", 6)),
        ("text.pollinations.ai", lambda: get_plain(f"https://text.pollinations.ai/{short_prompt}", 5)),
    ]
    if POLLINATIONS_KEY:
        attempts.insert(0, ("gen.pollinations.ai/v1", lambda: post_openai("https://gen.pollinations.ai/v1/chat/completions", 8)))

    for name, fn in attempts:
        try:
            text = await fn()
            if text and not _looks_like_junk(text):
                return text
            log.info("Free GPT %s: пустой/мусорный ответ", name)
        except Exception as e:
            log.info("Free GPT %s: %s", name, e)
    return None


async def ask_ai_named(prompt, json_mode=False, validate=None, timeout=12, free_timeout=20):
    """Возвращает (text, provider_name) или (None, None)."""
    keyed = []
    if GEMINI_API_KEYS:
        keyed.append(("Gemini", lambda: _ask_gemini(prompt, json_mode)))
    if GROQ_API_KEY:
        keyed.append(("Groq", lambda: _ask_openai_compat(
            "https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt, json_mode, use_json_format=True)))
    if OPENROUTER_API_KEY:
        keyed.append(("OpenRouter", lambda: _ask_openai_compat(
            "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt, json_mode,
            extra_headers={"HTTP-Referer": "https://t.me/bot", "X-Title": "IXCLY"})))

    async def wrap(name, fn):
        text = await fn()
        if text and (validate is None or validate(text)):
            return text, name
        return None

    if keyed:
        res = await first_success([wrap(n, f) for n, f in keyed], timeout)
        if res:
            log.info("ИИ ответил: %s", res[1])
            return res
        log.warning("Ключевые ИИ не ответили, переключаюсь на бесплатный GPT без ключа")

    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt, json_mode))], free_timeout)
    if res:
        log.info("ИИ ответил: %s", res[1])
        return res
    log.error("❌ Все ИИ недоступны, работает локальный движок.")
    return None, None


async def ask_ai(prompt, json_mode=False, validate=None, timeout=12, free_timeout=20):
    text, _ = await ask_ai_named(prompt, json_mode, validate, timeout, free_timeout)
    return text


def parse_ai_json(text):
    try:
        m = re.search(r"\{.*\}", text, re.S)
        data = json.loads(m.group(0))
        if data.get("dominant_tf") in TFS and isinstance(data.get("reason"), str) and data["reason"].strip():
            return data
    except Exception:
        pass
    return None


def has_russian(text, min_letters=30):
    return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= min_letters


def ai_to_html(text):
    text = html.escape(text.strip())
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?m)^#+\s*", "", text)
    return text[:3500]


# ----------------------------------------------------------------------------
# ЛОКАЛЬНЫЙ ДВИЖОК (работает всегда, без интернета-ИИ)
# ----------------------------------------------------------------------------
BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner",
              "upgrad", "breakout", "rebound", "recover", "accumul")
BULL_EXACT = {"etf", "buy", "launch", "launches", "record"}
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall",
              "liquidat", "fraud", "scam", "delist", "arrest", "dump", "selloff", "declin", "probe",
              "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
BEAR_EXACT = {"ban", "bans", "banned"}

COIN_WORDS = {
    "bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "ether": "ETH", "eth": "ETH", "solana": "SOL",
    "sol": "SOL", "xrp": "XRP", "ripple": "XRP", "bnb": "BNB", "binance": "BNB", "dogecoin": "DOGE",
    "doge": "DOGE", "cardano": "ADA", "toncoin": "TON", "avalanche": "AVAX", "chainlink": "LINK",
    "polkadot": "DOT", "litecoin": "LTC", "shiba": "SHIB", "pepe": "PEPE", "sui": "SUI",
}

# Метка движка в конце сообщения (для отладки): g=Gemini, q=Groq, o=OpenRouter, f=бесплатный GPT, l=локальный
ENGINE_TAG = {"Gemini": "g", "Groq": "q", "OpenRouter": "o", "GPT (free)": "f", "local": "l"}

NEWS_LABEL = {1: ("🟢", "позитивный"), 0: ("⚪", "нейтральный"), -1: ("🔴", "негативный")}


def news_sentiment(text):
    """+1 / 0 / -1 по ключевым словам."""
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w in BULL_EXACT or w.startswith(BULL_STEMS):
            score += 1
        elif w in BEAR_EXACT or w.startswith(BEAR_STEMS):
            score -= 1
    if score >= 2:
        return 1
    if score <= -2:
        return -1
    return 0


def mentioned_coins(text):
    found = []
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        t = COIN_WORDS.get(w)
        if t and t not in found:
            found.append(t)
    return found[:3]


def local_reason(tf, i):
    trend = "восходящий тренд (EMA20 выше EMA50)" if i["ema20"] > i["ema50"] else "нисходящий тренд (EMA20 ниже EMA50)"
    rsi = i["rsi"]
    zone = "перекупленность" if rsi >= 70 else "перепроданность" if rsi <= 30 else "нейтральная зона"
    macd = "бычий" if i["macd_hist"] > 0 else "медвежий"
    return f"На {tf}: {trend}, RSI {rsi:.0f} ({zone}), импульс MACD {macd}."


def local_verdict(tf_data, news_text):
    """Конкретный вывод без ИИ: согласованность ТФ + сила сигнала + волатильность + новости."""
    tf = "4h" if "4h" in tf_data else list(tf_data.keys())[-1]
    d = tf_data[tf]
    p = d["long_prob"]
    is_long = p >= 50
    aligned = sum(1 for t in tf_data if (tf_data[t]["long_prob"] >= 50) == is_long)
    total = len(tf_data)
    edge = abs(p - 50)

    atr_mean = float(d["df"]["atr"].tail(50).mean())
    vol_ratio = d["atr"] / atr_mean if atr_mean > 0 else 1.0

    ns = news_sentiment(news_text)
    news_ok = ns == 0 or (ns > 0) == is_long

    need = max(3, total - 1)
    enter = aligned >= need and edge >= 12 and vol_ratio >= 0.8 and news_ok
    if enter:
        entry_reason = f"Тренд совпадает на {aligned}/{total} ТФ, волатильность в норме, новости не против."
    elif aligned < need:
        entry_reason = f"Таймфреймы расходятся ({aligned}/{total}), лучше дождаться согласованности."
    elif edge < 12:
        entry_reason = "Сигнал слабый, преимущества у быков или медведей нет."
    elif vol_ratio < 0.8:
        entry_reason = "Низкая волатильность (флэт), движения может не хватить до целей."
    else:
        entry_reason = "Новостной фон идёт против направления сигнала."

    label = NEWS_LABEL[ns][1]
    return {
        "dominant_tf": tf,
        "reason": local_reason(tf, d),
        "entry_status": "ENTER" if enter else "WAIT",
        "entry_reason": entry_reason,
        "news": ns,
    }


# ----------------------------------------------------------------------------
# НОВОСТИ (крипто + мировые)
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
        except Exception as e:
            log.info("RSS %s: %s", url, e)
    return _news_cache["entries"]


async def get_world_titles(n=4):
    if _world_cache["titles"] and time.time() - _world_cache["ts"] < 600:
        return _world_cache["titles"]
    titles = []
    for url in WORLD_FEEDS:
        try:
            entries = await _load_feed(url)
            titles += [e.title for e in entries[:n // 2 + 1]]
        except Exception as e:
            log.info("RSS %s: %s", url, e)
    if titles:
        _world_cache["ts"] = time.time()
        _world_cache["titles"] = titles[:n]
    return _world_cache["titles"]


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


async def fetch_and_analyze_news():
    try:
        entries = await get_news_entries()
        if not entries:
            return "❌ Свежих новостей пока нет.", None, False
        news = entries[0]
        title = news.title
        summary = re.sub(r"<[^>]+>", "", getattr(news, "summary", ""))[:800]
        link = news.link

        prompt = f"""Ты профессиональный трейдер. Проанализируй эту новость и напиши краткий пост для Telegram СТРОГО НА РУССКОМ ЯЗЫКЕ.
Заголовок: {title}
Описание: {summary}

ОБЯЗАТЕЛЬНО ПЕРЕВЕДИ ЗАГОЛОВОК И ТЕКСТ НА РУССКИЙ ЯЗЫК. НИКАКОГО АНГЛИЙСКОГО ТЕКСТА В ОТВЕТЕ. Без markdown.
Ответ краткий, по делу, без воды. Напиши:
📌 Что произошло (1-2 предложения).
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
            action = {1: "поддержка покупателей, лонги выглядят интереснее",
                      -1: "давление на рынок, осторожнее с лонгами",
                      0: "без явного перекоса, ждём подтверждения графиком"}[ns]
            body = (
                f"📌 <b>{html.escape(ru_title)}</b>\n\n"
                f"<i>{html.escape(ru_summary)}</i>\n\n"
                f"📉 <b>Влияние:</b> {emoji} {label} фон — {action}.\n"
                f"🎯 <b>Монеты:</b> {', '.join(coins)}"
            )

        tag = ENGINE_TAG.get(news_engine if ai_text else "local", "l")
        text = f"{body}\n\n💰ВРЕМЯ ТОРГОВАТЬ💸\n\n🔗 <a href=\"{html.escape(link, quote=True)}\">Читать источник</a> {tag}"
        return text, extract_image(news), True
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
    await bot.send_message(chat_id, text[:4000], parse_mode="HTML", reply_markup=markup,
                           disable_web_page_preview=True)


async def news_watcher():
    global last_news_link
    await asyncio.sleep(10)
    while True:
        try:
            entries = await get_news_entries()
            if entries:
                link = entries[0].link
                if last_news_link is None:
                    last_news_link = link
                elif link != last_news_link:
                    last_news_link = link
                    text, image_url, ok = await fetch_and_analyze_news()
                    if ok:
                        img = await download_bytes(image_url) if image_url else None
                        for chat_id in list(active_chats):
                            try:
                                await send_with_image(chat_id, text, img, "news.jpg")
                            except (TelegramNotFound, TelegramForbiddenError):
                                active_chats.discard(chat_id)
                            except Exception as e:
                                if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                                    active_chats.discard(chat_id)
                            await asyncio.sleep(0.05)
        except Exception as e:
            log.warning("news_watcher: %s", e)
        await asyncio.sleep(600)


# ----------------------------------------------------------------------------
# КАРТИНКА (только латиница: шрифт на сервере может не поддерживать кириллицу)
# ----------------------------------------------------------------------------
def get_font(size, bold=False):
    names = (["DejaVuSans-Bold.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "arialbd.ttf"]
             if bold else
             ["DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "arial.ttf"])
    for n in names:
        try:
            return ImageFont.truetype(n, size)
        except Exception:
            pass
    try:
        return ImageFont.load_default(size)
    except TypeError:
        return ImageFont.load_default()


def _dashed(d, y, x0, x1, color, dash=10, gap=6):
    x = x0
    while x < x1:
        d.line([(x, y), (min(x + dash, x1), y)], fill=color, width=2)
        x += dash + gap


def generate_trade_image(coin, is_long, df, entry, tps, sls, tf, long_prob, source, entry_status="WAIT"):
    W, H = 800, 1050
    green, red, gray, white, grid = "#089981", "#F23645", "#787B86", "#D1D4DC", "#2a2e39"
    img = Image.new("RGB", (W, H), "#131722")
    d = ImageDraw.Draw(img)
    f_logo, f_title = get_font(34, True), get_font(54, True)
    f_text, f_bold, f_small = get_font(24), get_font(30, True), get_font(18)
    main = green if is_long else red

    d.text((40, 30), "ixcly trade", font=f_logo, fill=white)
    d.text((40, 85), f"{coin}/USDT", font=f_title, fill=white)
    side = "LONG" if is_long else "SHORT"
    d.text((W - 40 - d.textlength(side, font=f_title), 85), side, font=f_title, fill=main)
    d.text((40, 165), f"TF {tf}  |  Long prob {long_prob}%  |  {source}", font=f_text, fill=gray)

    # баннер статуса входа
    banner = green if entry_status == "ENTER" else "#F3A300"
    d.rectangle([40, 210, W - 40, 262], fill=banner)
    d.text((60, 220), f"STATUS: {entry_status}", font=f_bold, fill="#000000")

    x0, x1, y0, y1 = 40, W - 40, 295, 650
    cd = df.tail(40).reset_index(drop=True)
    vals = [float(cd["low"].min()), float(cd["high"].max()), tps[0], sls[0], entry]
    lo, hi = min(vals), max(vals)
    pad = (hi - lo) * 0.08 or hi * 0.01
    lo, hi = lo - pad, hi + pad

    def Y(p):
        return y1 - (p - lo) / (hi - lo) * (y1 - y0)

    for k in range(6):
        gy = y0 + k * (y1 - y0) / 5
        d.line([(x0, gy), (x1, gy)], fill=grid)

    plot_r = x1 - 70
    n = len(cd)
    step = (plot_r - x0) / n
    bw = max(3, int(step * 0.6))
    for i in range(n):
        row = cd.iloc[i]
        cx = x0 + step * (i + 0.5)
        col = green if row["close"] >= row["open"] else red
        d.line([(cx, Y(row["high"])), (cx, Y(row["low"]))], fill=col, width=2)
        top, bot = sorted([Y(row["open"]), Y(row["close"])])
        if bot - top < 2:
            bot = top + 2
        d.rectangle([cx - bw / 2, top, cx + bw / 2, bot], fill=col)

    levels = [("ENT", entry, white)]
    levels += [(f"TP{i + 1}", p, green) for i, p in enumerate(tps)]
    levels += [(f"SL{i + 1}", p, red) for i, p in enumerate(sls)]
    for name, p, col in levels:
        if lo <= p <= hi:
            y = Y(p)
            _dashed(d, y, x0, plot_r, col)
            d.text((plot_r + 6, y - 10), name, font=f_small, fill=col)

    d.text((40, 680), "Entry", font=f_text, fill=gray)
    d.text((200, 676), fmt(entry), font=f_bold, fill=white)
    d.text((40, 750), "Take Profit", font=f_text, fill=gray)
    d.text((420, 750), "Stop Loss", font=f_text, fill=gray)
    for i in range(3):
        yy = 800 + i * 60
        d.text((40, yy), f"TP{i + 1}  {fmt(tps[i])}", font=f_bold, fill=green)
        d.text((420, yy), f"SL{i + 1}  {fmt(sls[i])}", font=f_bold, fill=red)
    d.text((40, 1000), "Not financial advice", font=f_small, fill=gray)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ----------------------------------------------------------------------------
# АНАЛИЗ МОНЕТЫ
# ----------------------------------------------------------------------------
def calc_levels(info, is_long):
    entry = info["price"]
    atr = info["atr"]
    if not atr or atr != atr or atr <= 0:
        atr = entry * 0.01
    if is_long:
        dist = entry - info["recent_low"] + 0.2 * atr
    else:
        dist = info["recent_high"] + 0.2 * atr - entry
    dist = min(max(dist, 1.0 * atr), 2.5 * atr)
    sign = 1 if is_long else -1
    sls = [entry - sign * (dist + k * 0.5 * atr) for k in range(3)]
    tps = [entry + sign * dist * m for m in (1, 2, 3)]
    return entry, tps, sls


def shorten(text, n):
    """Обрезает по границе предложения/слова, без обрывов на полуслове."""
    text = " ".join(str(text).split())
    if len(text) <= n:
        return text
    cut = text[:n]
    m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if m >= n * 0.5:
        return cut[:m + 1]
    return cut.rsplit(" ", 1)[0].rstrip(",;:-— ") + "…"


def strength_label(p):
    if p >= 70:
        return "сильный сигнал"
    if p >= 58:
        return "умеренный сигнал"
    return "слабый сигнал"


async def build_signal(coin, status, force=False):
    cached = result_cache.get(coin)
    if cached and not force and time.time() - cached[0] < RESULT_TTL:
        return cached[1], cached[2]

    await status(f"📡 Загружаю свечи {coin}...")
    tf_data, source = await get_market_data(coin)

    await status("🌍 Читаю мировые и крипто-новости...")
    try:
        entries = await asyncio.wait_for(get_news_entries(), timeout=6)
        crypto_titles = [e.title for e in entries[:5]]
    except Exception:
        crypto_titles = []
    try:
        world_titles = await asyncio.wait_for(get_world_titles(4), timeout=6)
    except Exception:
        world_titles = []
    news_text = ". ".join(crypto_titles + world_titles)

    local = local_verdict(tf_data, news_text)

    tf_text = "".join(
        f"[{tf}] цена {fmt(d['price'])}, RSI {d['rsi']:.1f}, EMA20 {fmt(d['ema20'])}, EMA50 {fmt(d['ema50'])}, "
        f"ATR {fmt(d['atr'])}, MACD hist {d['macd_hist']:.6g}, шанс лонга {d['long_prob']}%\n"
        for tf, d in tf_data.items()
    )
    crypto_block = "\n".join(f"- {t}" for t in crypto_titles) or "- нет данных"
    world_block = "\n".join(f"- {t}" for t in world_titles) or "- нет данных"

    prompt = f"""Ты профессиональный крипто-трейдер и макро-аналитик. Проанализируй {coin} по индикаторам:
{tf_text}
Крипто-новости:
{crypto_block}
Мировые новости (экономика, политика):
{world_block}

Задача:
1. Выбери ОДИН доминирующий таймфрейм (15m, 1h, 4h или 1d).
2. Оцени новостной фон именно для {coin}: BULLISH, BEARISH или NEUTRAL.
3. Оцени волатильность и согласованность таймфреймов. Флэт, хаос, расхождение ТФ или новости против тренда -> "WAIT". Чёткий тренд на большинстве ТФ, нормальная волатильность и новости не против -> "ENTER".

Верни СТРОГО JSON без markdown:
{{"dominant_tf": "4h", "reason": "Простое обоснование на русском, максимум 150 символов, без перечисления индикаторов и цифр", "news_sentiment": "BULLISH или BEARISH или NEUTRAL", "entry_status": "ENTER или WAIT", "entry_reason": "Конкретный вывод на русском, одно короткое предложение, максимум 90 символов"}}"""

    await status("🧠 Формирую вывод...")
    raw, engine = await ask_ai_named(prompt, json_mode=True, validate=lambda t: parse_ai_json(t) is not None)
    ai = parse_ai_json(raw) if raw else None

    if ai:
        tf = ai["dominant_tf"]
        reason = ai["reason"].strip()
        entry_status = str(ai.get("entry_status", "")).upper()
        if entry_status not in ("ENTER", "WAIT"):
            entry_status = local["entry_status"]
        entry_reason = str(ai.get("entry_reason") or local["entry_reason"])
        ns = {"BULLISH": 1, "BEARISH": -1, "NEUTRAL": 0}.get(str(ai.get("news_sentiment", "")).upper(), local["news"])
    else:
        engine = "local"
        tf = local["dominant_tf"]
        reason = local["reason"]
        entry_status = local["entry_status"]
        entry_reason = local["entry_reason"]
        ns = local["news"]

    reason = shorten(reason, 180)
    entry_reason = shorten(entry_reason, 100)

    info = tf_data[tf]
    long_prob, short_prob = calc_probability(info["df"])
    is_long = long_prob >= 50
    entry, tps, sls = calc_levels(info, is_long)

    direction = "🟢 <b>LONG</b>" if is_long else "🔴 <b>SHORT</b>"
    green_n = max(0, min(10, round(long_prob / 10)))
    bar = "🟩" * green_n + "🟥" * (10 - green_n)
    multi = "  ·  ".join(f"{t} {tf_data[t]['long_prob']:.0f}%" for t in TFS)
    n_emoji, n_label = NEWS_LABEL[ns]
    tag = ENGINE_TAG.get(engine, "l")

    if entry_status == "ENTER":
        head, lv = "✅ <b>ВХОДИТЬ</b>", ""
    else:
        head, lv = "⏳ <b>ЖДАТЬ</b>", " <i>(если войти)</i>"

    def pct(p):
        return f"{(p - entry) / entry * 100:+.1f}%"

    tp_lines = "\n".join(f"TP{i + 1} · <code>{fmt(p)}</code>  <i>{pct(p)}</i>" for i, p in enumerate(tps))
    sl_lines = "\n".join(f"SL{i + 1} · <code>{fmt(p)}</code>  <i>{pct(p)}</i>" for i, p in enumerate(sls))

    text = (
        f"📊 <b>{coin}/USDT</b> · {tf}\n"
        f"<i>Данные: {source}</i>\n\n"
        f"{head}\n"
        f"<i>{html.escape(entry_reason)}</i>\n\n"
        f"{direction} · {strength_label(max(long_prob, short_prob))}\n"
        f"{bar}\n"
        f"Лонг <b>{long_prob:.0f}%</b>  ·  Шорт <b>{short_prob:.0f}%</b>\n\n"
        f"🕐 <b>По таймфреймам</b> <i>(шанс лонга)</i>\n"
        f"{multi}\n\n"
        f"💵 <b>Вход</b> · <code>{fmt(entry)}</code>\n\n"
        f"🎯 <b>Цели</b>{lv}\n{tp_lines}\n\n"
        f"🛑 <b>Стопы</b>\n{sl_lines}\n\n"
        f"🌍 Новости: {n_emoji} {n_label}\n\n"
        f"💬 {html.escape(reason)}\n\n"
        f"⚠️ <i>Не является финансовой рекомендацией</i> {tag}"
    )
    png = await asyncio.to_thread(generate_trade_image, coin, is_long, info["df"], entry, tps, sls,
                                  tf, long_prob, source, entry_status)
    result_cache[coin] = (time.time(), text, png)
    return text, png


async def process_signal(chat_id, user_id, coin, force=False):
    now = time.time()
    if user_id in busy_users:
        await bot.send_message(chat_id, "⏳ Я ещё считаю предыдущий запрос...")
        return
    if now - user_last_request.get(user_id, 0) < COOLDOWN:
        await bot.send_message(chat_id, "⏳ Подождите пару секунд...")
        return
    user_last_request[user_id] = now
    busy_users.add(user_id)

    loading = await bot.send_message(chat_id, f"⏳ Запускаю анализ {coin}...")
    last_status = {"t": ""}

    async def status(t):
        if t == last_status["t"]:
            return
        last_status["t"] = t
        try:
            await loading.edit_text(t)
        except Exception:
            pass

    try:
        async with analysis_sem:
            text, png = await asyncio.wait_for(build_signal(coin, status, force), timeout=70)
        await send_with_image(chat_id, text, png, f"{coin}.png", result_keyboard(coin))
    except LookupError:
        await bot.send_message(
            chat_id,
            f"❌ Монета <b>{html.escape(coin)}</b> не найдена. Проверьте тикер (например: BTC, WIF, PEPE).",
            parse_mode="HTML", reply_markup=get_coins_keyboard(0))
    except asyncio.TimeoutError:
        await bot.send_message(chat_id, "⏳ Источники данных не ответили вовремя. Попробуйте ещё раз.")
    except Exception as e:
        log.exception("process_signal")
        await bot.send_message(chat_id, f"⚠️ Ошибка: {html.escape(str(e))[:200]}")
    finally:
        busy_users.discard(user_id)
        try:
            await loading.delete()
        except Exception:
            pass


# ----------------------------------------------------------------------------
# ХЕНДЛЕРЫ ОСНОВНОГО БОТА
# ----------------------------------------------------------------------------
@dp.message(Command("start", "menu"))
async def start_handler(message: types.Message):
    active_chats.add(message.chat.id)

    user_name = message.from_user.first_name or "User"
    username = f"@{message.from_user.username}" if message.from_user.username else "Нет юзернейма"

    is_new = save_user_data(message.from_user.id, message.from_user.username, user_name)

    if is_new and ADMIN_ID and message.from_user.id not in notified_users:
        notified_users.add(message.from_user.id)
        try:
            await bot.send_message(
                int(ADMIN_ID),
                f"👤 <b>Новый пользователь!</b>\n\nИмя: {html.escape(user_name)}\nЮзернейм: {html.escape(username)}",
                parse_mode="HTML")
        except Exception:
            pass

    await message.answer(
        f"👋 Привет, {html.escape(user_name)} ({html.escape(username)}).\n"
        f"Выбери монету из списка или просто напиши тикер (например: <code>WIF</code>):",
        parse_mode="HTML", reply_markup=get_coins_keyboard(0))


@dp.message(Command("news"))
async def news_handler(message: types.Message):
    active_chats.add(message.chat.id)
    loading = await message.answer("⏳ Запрашиваю и анализирую последнюю новость...")
    try:
        text, image_url, _ = await fetch_and_analyze_news()
        img = await download_bytes(image_url) if image_url else None
        await send_with_image(message.chat.id, text, img, "news.jpg")
    finally:
        try:
            await loading.delete()
        except Exception:
            pass


@dp.message(F.text)
async def text_handler(message: types.Message):
    if message.text.startswith("/"):
        return
    coin = normalize_ticker(message.text)
    if not re.fullmatch(r"[A-Z0-9]{1,15}", coin):
        await message.answer("⚠️ Неверный формат. Введите тикер, например: <code>BTC</code>",
                             parse_mode="HTML", reply_markup=get_coins_keyboard(0))
        return
    active_chats.add(message.chat.id)
    await process_signal(message.chat.id, message.from_user.id, coin)


@dp.callback_query(F.data)
async def callback_handler(cb: types.CallbackQuery):
    try:
        await cb.answer()
    except Exception:
        pass
    if not cb.message:
        return
    data = cb.data
    chat_id = cb.message.chat.id

    if data == "ignore":
        return
    if data == "menu":
        user_name = cb.from_user.first_name or "User"
        username = f"@{cb.from_user.username}" if cb.from_user.username else "Нет юзернейма"
        await bot.send_message(
            chat_id, f"👋 Привет, {html.escape(user_name)} ({html.escape(username)}).\nВыбери монету:",
            parse_mode="HTML", reply_markup=get_coins_keyboard(0))
    elif data.startswith("p:"):
        try:
            await cb.message.edit_reply_markup(reply_markup=get_coins_keyboard(int(data[2:])))
        except Exception:
            pass
    elif data == "search":
        await bot.send_message(chat_id, "🔍 <b>Поиск</b>\n\nНапиши тикер (например: <code>WIF</code>), и я найду его.",
                               parse_mode="HTML")
    elif data.startswith("c:"):
        await process_signal(chat_id, cb.from_user.id, data[2:])
    elif data.startswith("r:"):
        await process_signal(chat_id, cb.from_user.id, data[2:], force=True)


# ----------------------------------------------------------------------------
# ХЕНДЛЕРЫ ВТОРОГО (АДМИН) БОТА ДЛЯ РАССЫЛОК
# ----------------------------------------------------------------------------
if input_dp:
    @input_dp.message(Command("start"))
    async def admin_start_handler(message: types.Message):
        await message.answer("👋 Привет, админ! Отправь сюда любой текст или фото, и я мгновенно разошлю это всем пользователям основного бота.")

    @input_dp.message()
    async def handle_broadcast(message: types.Message):
        if message.text and message.text.startswith('/'):
            return

        status_msg = await message.answer("⏳ Начинаю мгновенную рассылку...")
        success = 0

        photo_bytes = None
        if message.photo:
            try:
                file_info = await input_bot.get_file(message.photo[-1].file_id)
                file_data = await input_bot.download_file(file_info.file_path)
                photo_bytes = file_data.read()
            except Exception as e:
                log.error(f"Ошибка скачивания фото: {e}")

        for chat_id in list(active_chats):
            try:
                if photo_bytes:
                    await bot.send_photo(chat_id, photo=BufferedInputFile(photo_bytes, "photo.jpg"),
                                         caption=message.html_text, parse_mode="HTML")
                elif message.text:
                    await bot.send_message(chat_id, text=message.html_text, parse_mode="HTML")

                success += 1
                await asyncio.sleep(0.05)
            except (TelegramNotFound, TelegramForbiddenError):
                active_chats.discard(chat_id)
                log.warning(f"Пользователь {chat_id} недоступен. Удалён из рассылки.")
            except Exception as e:
                if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                    active_chats.discard(chat_id)

        await status_msg.edit_text(f"✅ Рассылка завершена!\nДоставлено: {success} чел.")


# ----------------------------------------------------------------------------
# ЗАПУСК
# ----------------------------------------------------------------------------
async def main():
    global http, analysis_sem
    keep_alive()
    http = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (compatible; IXCLYBot/2.8)"})
    analysis_sem = asyncio.Semaphore(4)
    load_active_chats()
    log.info("Бот запущен...")
    watcher = None
    try:
        await bot.set_my_commands([
            types.BotCommand(command="menu", description="🚀 Меню"),
            types.BotCommand(command="news", description="📰 Новости"),
        ])
        watcher = asyncio.create_task(news_watcher())

        if input_bot:
            try:
                log.info("Админ-бот (рассылка) запущен...")
                await input_bot.delete_webhook(drop_pending_updates=True)
                asyncio.create_task(input_dp.start_polling(input_bot))
            except Exception as e:
                log.error(f"ОШИБКА ЗАПУСКА АДМИН-БОТА: {e}. Основной бот продолжит работу.")

        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        if watcher:
            watcher.cancel()
        await http.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
