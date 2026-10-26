# -*- coding: utf-8 -*-
"""
IXCLY Trading bot v3.1
- Мгновенная рассылка из второго (админского) бота
- Новости: строгий перевод на русский
- Свечи: Binance, MEXC, Gate, KuCoin, OKX, Bybit + CryptoCompare
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

BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN")) # Токен для админ-бота
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
CRYPTOCOMPARE_KEY = clean_key(os.getenv("CRYPTOCOMPARE_KEY"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")

if not BOT_TOKEN:
    raise SystemExit("Не задана переменная окружения BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Инициализация админ-бота
input_bot = Bot(token=INPUT_BOT_TOKEN) if INPUT_BOT_TOKEN else None
input_dp = Dispatcher() if INPUT_BOT_TOKEN else None

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
# ИИ
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


async def ask_ai(prompt, json_mode=False, validate=None, timeout=30):
    providers = []
    if GEMINI_API_KEYS:
        providers.append(lambda: _ask_gemini(prompt, json_mode))
    if GROQ_API_KEY:
        providers.append(lambda: _ask_openai_compat(
            "https://api.groq.com/openai/v1", GROQ_API_KEY, GROQ_MODEL, prompt, json_mode, use_json_format=True))
    if OPENROUTER_API_KEY:
        providers.append(lambda: _ask_openai_compat(
            "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, OPENROUTER_MODEL, prompt, json_mode,
            extra_headers={"HTTP-Referer": "https://t.me/bot", "X-Title": "IXCLY"}))
    if not providers:
        return None

    async def wrap(fn):
        text = await fn()
        if text and (validate is None or validate(text)):
            return text
        return None

    res = await first_success([wrap(p) for p in providers], timeout)
    if not res:
        log.error("❌ ВСЕ ИИ ПРОВАЙДЕРЫ ОТКАЗАЛИ. Проверьте API-ключи и лимиты.")
    return res


def parse_ai_json(text):
    try:
        m = re.search(r"\{.*\}", text, re.S)
        data = json.loads(m.group(0))
        if data.get("dominant_tf") in TFS and isinstance(data.get("reason"), str) and data["reason"].strip():
            return data
    except Exception:
        pass
    return None


def ai_to_html(text):
    text = html.escape(text.strip())
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?m)^#+\s*", "", text)
    return text[:3500]


# ----------------------------------------------------------------------------
# НОВОСТИ
# ----------------------------------------------------------------------------
NEWS_FEEDS = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
_news_cache = {"ts": 0.0, "entries": []}


async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300:
        return _news_cache["entries"]
    for url in NEWS_FEEDS:
        try:
            async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                text = await r.text()
            feed = await asyncio.to_thread(feedparser.parse, text)
            if feed.entries:
                _news_cache["ts"] = time.time()
                _news_cache["entries"] = list(feed.entries[:5])
                return _news_cache["entries"]
        except Exception as e:
            log.info("RSS %s: %s", url, e)
    return _news_cache["entries"]


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

ОБЯЗАТЕЛЬНО ПЕРЕВЕДИ ЗАГОЛОВОК И ТЕКСТ НА РУССКИЙ ЯЗЫК. НИКАКОГО АНГЛИЙСКОГО ТЕКСТА В ОТВЕТЕ.
Ответ должен быть кратким, по делу, без воды и без слов "Суть" и "ИИ".
Напиши:
1. Что произошло (1-2 предложения на русском).
2. Как это влияет на рынок (позитив/негатив и почему, на русском).
3. Какие монеты затронуты (2-3 тикера, например BTC, ETH).
"""
        ai_text = await ask_ai(prompt, json_mode=False, timeout=40)
        
        if ai_text:
            body = ai_to_html(ai_text)
        else:
            ru_title = await translate_text(title)
            ru_summary = await translate_text(summary[:300])
            
            body = (
                f"📌 <b>{html.escape(ru_title)}</b>\n\n"
                f"<i>{html.escape(ru_summary)}...</i>\n\n"
                f"📉 <b>Влияние:</b> Требуется ручная оценка (нейросети перегружены).\n"
                f"🎯 <b>Монеты:</b> Общий фон рынка."
            )

        text = f"{body}\n\n💰ВРЕМЯ ТОРГОВАТЬ💸\n\n🔗 <a href=\"{html.escape(link, quote=True)}\">Читать источник</a>"
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
                            except Exception as e:
                                if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                                    active_chats.discard(chat_id)
                            await asyncio.sleep(0.05)
        except Exception as e:
            log.warning("news_watcher: %s", e)
        await asyncio.sleep(600)


# ----------------------------------------------------------------------------
# КАРТИНКА
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


def generate_trade_image(coin, is_long, df, entry, tps, sls, tf, long_prob, source):
    W, H = 800, 1000
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

    x0, x1, y0, y1 = 40, W - 40, 215, 610
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

    d.text((40, 640), "Entry", font=f_text, fill=gray)
    d.text((200, 636), fmt(entry), font=f_bold, fill=white)
    d.text((40, 710), "Take Profit", font=f_text, fill=gray)
    d.text((420, 710), "Stop Loss", font=f_text, fill=gray)
    for i in range(3):
        yy = 760 + i * 60
        d.text((40, yy), f"TP{i + 1}  {fmt(tps[i])}", font=f_bold, fill=green)
        d.text((420, yy), f"SL{i + 1}  {fmt(sls[i])}", font=f_bold, fill=red)
    d.text((40, 950), "Not financial advice", font=f_small, fill=gray)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ----------------------------------------------------------------------------
# АНАЛИЗ МОНЕТЫ
# ----------------------------------------------------------------------------
def local_reason(tf, i):
    trend = "восходящий тренд (EMA20 выше EMA50)" if i["ema20"] > i["ema50"] else "нисходящий тренд (EMA20 ниже EMA50)"
    rsi = i["rsi"]
    zone = "перекупленность" if rsi >= 70 else "перепроданность" if rsi <= 30 else "нейтральная зона"
    macd = "бычий" if i["macd_hist"] > 0 else "медвежий"
    return f"На {tf}: {trend}, RSI {rsi:.0f} ({zone}), импульс MACD {macd}."


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

    await status("🧮 Считаю индикаторы и читаю новости...")
    try:
        entries = await asyncio.wait_for(get_news_entries(), timeout=6)
        news_titles = "\n".join(e.title for e in entries[:3]) or "Нет свежих новостей."
    except Exception:
        news_titles = "Нет свежих новостей."

    tf_text = "".join(
        f"[{tf}] цена {fmt(d['price'])}, RSI {d['rsi']:.1f}, EMA20 {fmt(d['ema20'])}, EMA50 {fmt(d['ema50'])}, "
        f"ATR {fmt(d['atr'])}, MACD hist {d['macd_hist']:.6g}, шанс лонга {d['long_prob']}%\n"
        for tf, d in tf_data.items()
    )
    prompt = f"""Ты трейдинг-аналитик. Проанализируй {coin} по индикаторам:
{tf_text}
Последние новости (учти влияние):
{news_titles}

Выбери таймфрейм, на котором сигнал самый надёжный (один из 15m, 1h, 4h, 1d).
Верни СТРОГО JSON без markdown: {{"dominant_tf": "4h", "reason": "Краткое обоснование на русском, 1-2 предложения, с учётом новостей."}}"""

    await status("🤖 Формирую вывод...")
    raw = await ask_ai(prompt, json_mode=True, validate=lambda t: parse_ai_json(t) is not None)
    ai = parse_ai_json(raw) if raw else None

    if ai:
        tf = ai["dominant_tf"]
        reason = ai["reason"].strip()
    else:
        tf = "4h"
        reason = local_reason(tf, tf_data[tf])
    reason = reason[:350]

    info = tf_data[tf]
    long_prob, short_prob = calc_probability(info["df"])
    is_long = long_prob >= 50
    entry, tps, sls = calc_levels(info, is_long)

    direction = "🟢 LONG" if is_long else "🔴 SHORT"
    multi = " | ".join(f"{t} {tf_data[t]['long_prob']:.0f}%" for t in TFS)
    text = (
        f"<b>📊 {coin}/USDT</b> · {tf}\n"
        f"<i>Данные: {source}</i>\n\n"
        f"🎯 <b>{direction}</b> ({strength_label(max(long_prob, short_prob))})\n"
        f"Лонг: <b>{long_prob}%</b> · Шорт: <b>{short_prob}%</b>\n"
        f"<i>Шанс лонга по ТФ: {multi}</i>\n\n"
        f"💵 Вход: <code>{fmt(entry)}</code>\n\n"
        f"🚀 <b>Цели:</b>\n"
        f"├ TP1: <code>{fmt(tps[0])}</code>\n"
        f"├ TP2: <code>{fmt(tps[1])}</code>\n"
        f"└ TP3: <code>{fmt(tps[2])}</code>\n\n"
        f"🛑 <b>Стопы:</b>\n"
        f"├ SL1: <code>{fmt(sls[0])}</code>\n"
        f"├ SL2: <code>{fmt(sls[1])}</code>\n"
        f"└ SL3: <code>{fmt(sls[2])}</code>\n\n"
        f"📰 <i>{html.escape(reason)}</i>\n\n"
        f"⚠️ <i>Не является финансовой рекомендацией!</i>"
    )
    png = await asyncio.to_thread(generate_trade_image, coin, is_long, info["df"], entry, tps, sls, tf, long_prob, source)
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
            text, png = await asyncio.wait_for(build_signal(coin, status, force), timeout=40)
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
    is_new = save_user_data(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if is_new and ADMIN_ID:
        try:
            await bot.send_message(
                int(ADMIN_ID),
                f"👤 <b>Новый пользователь!</b>\n\nID: <code>{message.from_user.id}</code>\n"
                f"Имя: {html.escape(message.from_user.first_name or '')}",
                parse_mode="HTML")
        except Exception:
            pass
    await message.answer("👋 Выбери монету из списка или просто напиши тикер (например: <code>WIF</code>):",
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
        await bot.send_message(chat_id, "Выбери монету:", reply_markup=get_coins_keyboard(0))
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
        # Игнорируем команды
        if message.text and message.text.startswith('/'):
            return
            
        status_msg = await message.answer("⏳ Начинаю мгновенную рассылку...")
        success = 0
        
        # Копируем сообщение (с текстом, фото или видео) всем пользователям
        for chat_id in list(active_chats):
            try:
                await bot.copy_message(
                    chat_id=chat_id, 
                    from_chat_id=message.chat.id, 
                    message_id=message.message_id
                )
                success += 1
                await asyncio.sleep(0.05) # Задержка от бана Telegram
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
    http = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (compatible; IXCLYBot/2.0)"})
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
        
        # Запуск админ-бота параллельно с основным
        if input_bot:
            log.info("Админ-бот (рассылка) запущен...")
            # Очищаем вебхук админ-бота, чтобы не было конфликтов
            await input_bot.delete_webhook(drop_pending_updates=True)
            asyncio.create_task(input_dp.start_polling(input_bot))
            
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        if watcher:
            watcher.cancel()
        await http.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
