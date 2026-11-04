# -*- coding: utf-8 -*-
"""Standalone IXCLY NEWS publisher. Does not import or modify the trading bot."""
from __future__ import annotations

import asyncio
import calendar
from contextlib import contextmanager
import hashlib
import html
import io
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import feedparser
from aiogram import Bot
from aiogram.types import BufferedInputFile
from PIL import Image, ImageDraw, ImageFont
from ixcly_news import assess_crypto_news

BOT_TOKEN = os.getenv("IXCLY_NEWS_BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("IXCLY_NEWS_CHANNEL_ID", "@ixcly_news").strip()
CHANNEL_URL = os.getenv("IXCLY_NEWS_CHANNEL_URL", "https://t.me/ixcly_news").strip()
DB_PATH = os.getenv("IXCLY_NEWS_DB", "ixcly_news_state.sqlite3")
POLL_SECONDS = max(300, int(os.getenv("IXCLY_NEWS_POLL_SECONDS", "900")))
LOCAL_TZ = timezone(timedelta(hours=int(os.getenv("IXCLY_NEWS_TZ_OFFSET", "2"))))
MIN_ARTICLE_CHARS = 110
MAX_AGE_HOURS = 36
LOG = logging.getLogger("ixcly_news")
STATE_CHANGE_CALLBACK = None


def set_state_callback(callback):
    global STATE_CHANGE_CALLBACK
    STATE_CHANGE_CALLBACK = callback


def _state_changed():
    if callable(STATE_CHANGE_CALLBACK):
        try:
            STATE_CHANGE_CALLBACK()
        except Exception:
            LOG.exception("Could not persist publisher state into IXCLY backup")

FEEDS = {
    "crypto": [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://cointelegraph.com/rss",
        "https://decrypt.co/feed",
        "https://cryptoslate.com/feed/",
        "https://bitcoinmagazine.com/.rss/full/",
        "https://blockworks.co/feed",
    ],
    "russia": [
        "https://forklog.com/feed",
        "https://bits.media/rss/news/",
        "https://www.rbc.ru/crypto/rss",
    ],
}

CATEGORY_RULES = {
    "₿ Bitcoin и макроэкономика": ("crypto", ("bitcoin", "биткоин", "btc", "halving", "майнинг", "резерв", "reserve")),
    "🏦 Биржи, ETF и институционалы": ("crypto", ("binance", "coinbase", "exchange", "бирж", "etf", "spot fund", "institutional", "институцион", "custody", "listing", "листинг")),
    "🧩 Альткоины и DeFi": ("crypto", ("ethereum", "эфир", "solana", "defi", "stablecoin", "стейблкоин", "token", "токен", "altcoin", "альткоин", "layer 2")),
    "⚖️ Регулирование и безопасность": ("crypto", ("sec", "cftc", "регулирован", "регулятор", "закон", "взлом", "hack", "exploit", "уязвим", "санкц", "lawsuit", "суд", "ликвидац")),
}



def _clean(value: str, limit: int = 5000) -> str:
    value = html.unescape(re.sub(r"<[^>]*>", " ", str(value or "")))
    value = re.sub(r"https?://\S+", "", value)
    value = re.sub(r"[\t\r\f\v ]+", " ", value)
    value = re.sub(r"\n\s*\n+", "\n\n", value)
    return value.strip()[:limit]


def _published(entry) -> float:
    for key in ("published_parsed", "updated_parsed"):
        value = getattr(entry, key, None)
        if value:
            try:
                return float(calendar.timegm(value))
            except Exception:
                pass
    for key in ("published", "updated"):
        value = getattr(entry, key, "")
        if value:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except Exception:
                pass
    return 0.0


def _category(title: str, summary: str, feed_group: str) -> str:
    hay = f" {title} {summary} ".lower()
    for category, (_, terms) in CATEGORY_RULES.items():
        if any(k in hay for k in terms):
            return category
    return "₿ Bitcoin и макроэкономика"



def _extract_image(entry) -> str:
    for item in (getattr(entry, "media_content", None) or []):
        if isinstance(item, dict) and str(item.get("url", "")).startswith("https://"):
            return item["url"]
    for item in (getattr(entry, "media_thumbnail", None) or []):
        if isinstance(item, dict) and str(item.get("url", "")).startswith("https://"):
            return item["url"]
    for item in (getattr(entry, "links", None) or []):
        if isinstance(item, dict) and "image" in str(item.get("type", "")) and str(item.get("href", "")).startswith("https://"):
            return item["href"]
    return ""


@contextmanager
def _db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS posted (fingerprint TEXT PRIMARY KEY, category TEXT, title TEXT, posted_at TEXT)")
        con.execute("CREATE TABLE IF NOT EXISTS daily (day TEXT PRIMARY KEY, count INTEGER NOT NULL DEFAULT 0, morning INTEGER NOT NULL DEFAULT 0, btc INTEGER NOT NULL DEFAULT 0, recap INTEGER NOT NULL DEFAULT 0)")
        columns = {row[1] for row in con.execute("PRAGMA table_info(daily)").fetchall()}
        if "longterm" not in columns:
            con.execute("ALTER TABLE daily ADD COLUMN longterm INTEGER NOT NULL DEFAULT 0")
        yield con
        con.commit()
    finally:
        con.close()


def export_state() -> dict:
    """Снимок дедупликации и дневного расписания для общего Telegram-бэкапа IXCLY."""
    with _db() as con:
        posted = con.execute("SELECT fingerprint,category,title,posted_at FROM posted ORDER BY posted_at DESC LIMIT 1000").fetchall()
        daily = con.execute("SELECT day,count,morning,btc,recap,longterm FROM daily ORDER BY day DESC LIMIT 90").fetchall()
    return {"posted": [list(x) for x in posted], "daily": [list(x) for x in daily]}


def import_state(data: dict):
    if not isinstance(data, dict):
        return
    with _db() as con:
        for row in data.get("posted", [])[:1000]:
            if isinstance(row, (list, tuple)) and len(row) == 4:
                con.execute("INSERT OR IGNORE INTO posted(fingerprint,category,title,posted_at) VALUES(?,?,?,?)", tuple(row))
        for row in data.get("daily", [])[:90]:
            if isinstance(row, (list, tuple)) and len(row) == 6:
                con.execute("INSERT OR REPLACE INTO daily(day,count,morning,btc,recap,longterm) VALUES(?,?,?,?,?,?)", tuple(row))


def _fingerprint(title: str, link: str) -> str:
    return hashlib.sha256((title.lower().strip() + "|" + link.strip()).encode()).hexdigest()


def _already_posted(fp: str) -> bool:
    with _db() as con:
        return con.execute("SELECT 1 FROM posted WHERE fingerprint=?", (fp,)).fetchone() is not None


def _can_publish_news() -> bool:
    """Legacy compatibility hook; quality score and dedupe now control publication."""
    return True



def _today_count() -> int:
    day = datetime.now(LOCAL_TZ).date().isoformat()
    with _db() as con:
        row = con.execute("SELECT count FROM daily WHERE day=?", (day,)).fetchone()
    return int(row[0]) if row else 0


def _mark_posted(fp: str, category: str, title: str):
    day = datetime.now(LOCAL_TZ).date().isoformat()
    now = datetime.now(LOCAL_TZ).isoformat()
    with _db() as con:
        con.execute("INSERT OR IGNORE INTO posted VALUES (?, ?, ?, ?)", (fp, category, title[:250], now))
        con.execute("INSERT INTO daily(day,count) VALUES(?,1) ON CONFLICT(day) DO UPDATE SET count=count+1", (day,))
    _state_changed()


def _mark_routine(kind: str):
    day = datetime.now(LOCAL_TZ).date().isoformat()
    with _db() as con:
        con.execute("INSERT OR IGNORE INTO daily(day,count) VALUES(?,0)", (day,))
        con.execute(f"UPDATE daily SET {kind}=1, count=count+1 WHERE day=? AND {kind}=0", (day,))
    _state_changed()


def _today_posts():
    day = datetime.now(LOCAL_TZ).date().isoformat()
    with _db() as con:
        rows = con.execute("SELECT category,title FROM posted WHERE substr(posted_at,1,10)=? ORDER BY posted_at", (day,)).fetchall()
    return rows


def _routine_done(kind: str) -> bool:
    day = datetime.now(LOCAL_TZ).date().isoformat()
    with _db() as con:
        row = con.execute(f"SELECT {kind} FROM daily WHERE day=?", (day,)).fetchone()
    return bool(row and row[0])


async def _fetch_feed(session: aiohttp.ClientSession, url: str, group: str):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12), allow_redirects=True) as response:
            if response.status != 200:
                return []
            raw = await response.read()
        parsed = await asyncio.to_thread(feedparser.parse, raw)
        output = []
        for entry in parsed.entries[:30]:
            title = _clean(getattr(entry, "title", ""), 240)
            summary = _clean(getattr(entry, "summary", "") or getattr(entry, "description", ""), 5000)
            if not title or len(summary) < MIN_ARTICLE_CHARS:
                continue
            published = _published(entry)
            if not published or time.time() - published > MAX_AGE_HOURS * 3600:
                continue
            link = str(getattr(entry, "link", "") or "")
            if not link.startswith("https://"):
                continue
            output.append({"title": title, "summary": summary, "link": link, "published": published,
                           "image": _extract_image(entry), "category": _category(title, summary, group),
                           "domain": (urlparse(link).netloc or "").lower().removeprefix("www."),
                           "fingerprint": _fingerprint(title, link)})
        return output
    except Exception as exc:
        LOG.debug("feed fetch failed %s: %s", url, exc)
        return []


HIGH_IMPACT_TERMS = (
    "hack", "exploit", "stolen", "breach", "ban", "banned", "arrest", "bankrupt",
    "insolvency", "lawsuit", "approved", "approval", "etf", "санкц", "запрет",
    "взлом", "украл", "арест", "банкрот", "иск", "одобр", "миллиард", "обруш",
)
OFFICIAL_DOMAINS = (
    "openai.com", "sec.gov", "cftc.gov", "federalreserve.gov", "treasury.gov",
    "justice.gov", "cbr.ru", "gov.ru", "europa.eu", "ec.europa.eu",
)


def _tokens(text: str):
    return {x for x in re.findall(r"[a-zа-яё0-9]{3,}", text.lower()) if x not in {"the", "and", "for", "with", "from", "that", "this"}}


def _has_independent_confirmation(item: dict, items: list[dict]) -> bool:
    domain = item.get("domain", "")
    if any(domain == d or domain.endswith("." + d) for d in OFFICIAL_DOMAINS):
        return True
    a = _tokens(item["title"])
    for other in items:
        if other is item or other.get("domain") == domain:
            continue
        b = _tokens(other["title"])
        if not a or not b:
            continue
        overlap = len(a & b) / max(1, min(len(a), len(b)))
        if overlap >= 0.30:
            return True
    return False


async def _translate_to_ru(session: aiohttp.ClientSession, text: str) -> str:
    text = _clean(text, 5000)
    if re.search(r"[А-Яа-яЁё]", text):
        return text
    if not text:
        return ""
    try:
        async with session.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "auto", "tl": "ru", "dt": "t", "q": text[:1800]},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status != 200:
                return ""
            data = await response.json(content_type=None)
        translated = "".join(part[0] for part in data[0] if part and part[0])
        return translated if re.search(r"[А-Яа-яЁё]", translated) else ""
    except Exception:
        return ""


async def collect_news(session: aiohttp.ClientSession):
    jobs = [_fetch_feed(session, url, group) for group, urls in FEEDS.items() for url in urls]
    results = await asyncio.gather(*jobs, return_exceptions=True)
    items = [item for result in results if isinstance(result, list) for item in result]
    items.sort(key=lambda x: x["published"], reverse=True)
    unique, seen, title_tokens = [], set(), []
    for item in items:
        key = item["fingerprint"]
        if key in seen or _already_posted(key):
            continue
        seen.add(key)
        quality = assess_crypto_news(item["title"], item["summary"], item.get("domain", ""))
        if not quality["publish"]:
            LOG.info("Editorial filter rejected (%s/100): %s", quality["score"], item["title"])
            continue
        tokens = _tokens(item["title"])
        if any(tokens and len(tokens & old) / max(1, min(len(tokens), len(old))) >= .72 for old in title_tokens):
            LOG.info("Near-duplicate rejected: %s", item["title"])
            continue
        title_tokens.append(tokens)
        item["editorial"] = quality
        haystack = (item["title"] + " " + item["summary"]).lower()
        if any(term in haystack for term in HIGH_IMPACT_TERMS) and not _has_independent_confirmation(item, items):
            LOG.info("Skipping uncorroborated high-impact claim: %s", item["title"])
            continue
        title_ru = await _translate_to_ru(session, item["title"])
        summary_ru = await _translate_to_ru(session, item["summary"])
        # Never publish an English-only fallback or a translation failure as a Russian post.
        if not title_ru or not summary_ru or len(summary_ru) < MIN_ARTICLE_CHARS:
            continue
        item["title_original"] = item["title"]
        item["summary_original"] = item["summary"]
        item["title"] = title_ru
        item["summary"] = summary_ru
        unique.append(item)
    return unique


def _format_news(item: dict) -> str:
    title = html.escape(item["title"])
    summary = html.escape(item["summary"][:500].rsplit(" ", 1)[0])
    published = datetime.fromtimestamp(item["published"], LOCAL_TZ).strftime("%d.%m.%Y %H:%M") if item["published"] else "Сегодня"
    assets = sorted(set(re.findall(r"\b(?:BTC|ETH|SOL|XRP|BNB|ADA|DOGE|TON|AVAX|LINK|DOT|LTC|TRX|USDT|USDC)\b", (item.get("title_original", "") + " " + item.get("summary_original", "")).upper())))
    asset_text = ", ".join(assets) if assets else "Конкретные активы не выделены источником"
    source = html.escape(item.get("link", ""), quote=True)
    text = (f"<b>₿ {title}</b>\n\n"
            f"🧩 <b>Что произошло</b>\n{summary}\n\n"
            f"📊 <b>Почему это важно</b>\nСобытие относится к криптовалютному рынку; фактический эффект будет зависеть от подтверждения информации и реакции рынка.\n\n"
            f"🪙 <b>Какие активы затронуты</b>\n{html.escape(asset_text)}\n\n"
            f"⚡ <b>Что отслеживать дальше</b>\nОфициальные обновления по событию, подтверждение независимыми источниками, динамику цены и объёма затронутых активов.\n\n"
            f"🔎 <b>Источник</b>\n<a href=\"{source}\">Оригинальная публикация</a>\n"
            f"<i>Опубликовано: {published} (местное время)</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📲 <a href=\"{html.escape(CHANNEL_URL, quote=True)}\">Подписаться на IXCLY NEWS</a>")
    return text



def make_cover(title: str, category: str) -> bytes:
    image = Image.new("RGB", (1200, 675), (7, 13, 27))
    draw = ImageDraw.Draw(image)
    for x in range(1200):
        t = x / 1199
        draw.line((x, 0, x, 675), fill=(int(8 + 8*t), int(17 + 20*t), int(34 + 42*t)))
    try:
        bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 47)
        regular = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 26)
    except OSError:
        bold = regular = ImageFont.load_default()
    draw.rounded_rectangle((65, 58, 440, 125), 18, fill=(17, 56, 91), outline=(49, 143, 215), width=2)
    draw.text((88, 72), "IXCLY NEWS", font=regular, fill=(242, 248, 255))
    draw.text((75, 190), category[:55], font=regular, fill=(116, 190, 235))
    words, lines, line = title.split(), [], ""
    for word in words:
        candidate = (line + " " + word).strip()
        if draw.textbbox((0, 0), candidate, font=bold)[2] > 1040 and line:
            lines.append(line); line = word
        else:
            line = candidate
        if len(lines) >= 4: break
    if line and len(lines) < 4: lines.append(line)
    y = 260
    for part in lines[:4]:
        draw.text((75, y), part, font=bold, fill=(255, 255, 255)); y += 70
    draw.text((78, 605), "КРИПТОВАЛЮТЫ · ТЕХНОЛОГИИ · АНАЛИТИКА", font=regular, fill=(155, 185, 215))
    output = io.BytesIO(); image.save(output, "JPEG", quality=88, optimize=True)
    return output.getvalue()


async def _download_image(session, url: str):
    if not url.startswith("https://"):
        return None
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
            if response.status != 200 or "image" not in response.headers.get("Content-Type", ""):
                return None
            data = await response.read()
        if len(data) > 7_000_000:
            return None
        # Validate and normalize image bytes to Telegram-friendly JPEG.
        with Image.open(io.BytesIO(data)) as im:
            im = im.convert("RGB"); im.thumbnail((1600, 1000))
            out = io.BytesIO(); im.save(out, "JPEG", quality=88)
            return out.getvalue()
    except Exception:
        return None


async def publish(bot: Bot, session: aiohttp.ClientSession, text: str, title: str, category: str, image_url: str = ""):
    # Ensure every publication ends with the same channel subscription link.
    footer = ("━━━━━━━━━━━━━━\n"
              f"📲 <a href=\"{html.escape(CHANNEL_URL, quote=True)}\">Подписаться на IXCLY NEWS</a>")
    if "Подписаться на IXCLY NEWS" not in text:
        text = text.rstrip() + "\n\n" + footer
    photo = await _download_image(session, image_url) if image_url else None
    if not photo:
        photo = make_cover(title, category)
    if len(text) <= 1000:
        await bot.send_photo(CHANNEL_ID, BufferedInputFile(photo, filename="ixcly_news.jpg"), caption=text, parse_mode="HTML")
    else:
        # Telegram photo captions are limited to 1024 characters. Keep full text intact;
        # the channel link remains at the bottom of the full text message.
        caption = f"{category}\n\n<b>{html.escape(title[:180])}</b>"
        await bot.send_photo(CHANNEL_ID, BufferedInputFile(photo, filename="ixcly_news.jpg"), caption=caption, parse_mode="HTML")
        await bot.send_message(CHANNEL_ID, text, parse_mode="HTML", disable_web_page_preview=True)


async def publish_news_batch(bot: Bot, session: aiohttp.ClientSession):
    now_hour = datetime.now(LOCAL_TZ).hour
    if not (7 <= now_hour <= 23):
        return 0
    items = await collect_news(session)
    # Balanced feed: limit each pass to avoid flooding, and rotate categories where possible.
    chosen, categories = [], set()
    for item in items:
        if item["category"] not in categories or not chosen:
            chosen.append(item); categories.add(item["category"])
        if len(chosen) >= 1:
            break
    sent = 0
    for item in chosen:
        try:
            await publish(bot, session, _format_news(item), item["title"], item["category"], item["image"])
            _mark_posted(item["fingerprint"], item["category"], item["title"])
            sent += 1
            await asyncio.sleep(2)
        except Exception:
            LOG.exception("Telegram publish failed for %s", item["title"])
            break
    return sent


def _rsi(closes, period=14):
    if len(closes) <= period:
        return None
    changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(x, 0.0) for x in changes[-period:]]
    losses = [max(-x, 0.0) for x in changes[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


async def _btc_technical_snapshot(session: aiohttp.ClientSession):
    try:
        async with session.get("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=60", timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            rows = await r.json()
        closes = [float(row[4]) for row in rows if len(row) > 4]
        if len(closes) < 21:
            return None
        sma20 = sum(closes[-20:]) / 20
        sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
        change7 = (closes[-1] / closes[-8] - 1) * 100 if len(closes) >= 8 else None
        change30 = (closes[-1] / closes[-31] - 1) * 100 if len(closes) >= 31 else None
        return {"sma20": sma20, "sma50": sma50, "rsi14": _rsi(closes),
                "change7": change7, "change30": change30}
    except Exception:
        return None


async def btc_outlook(bot: Bot, session: aiohttp.ClientSession):
    """Scenario-based context, not a price target or certainty claim."""
    if _routine_done("btc"):
        return
    try:
        async with session.get("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT", timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return
            data = await r.json()
        price = float(data["lastPrice"]); change = float(data["priceChangePercent"])
        high = float(data["highPrice"]); low = float(data["lowPrice"]); volume = float(data["quoteVolume"])
    except Exception:
        LOG.warning("BTC market data unavailable; skipping outlook")
        return
    technical = await _btc_technical_snapshot(session)
    if change >= 1.5:
        context = "За последние 24 часа BTC показывает положительную динамику. Важно проверить, удержится ли спрос и подтвердится ли движение объёмом."
    elif change <= -1.5:
        context = "За последние 24 часа BTC находится под давлением. Важно оценить, стабилизируется ли цена и вернутся ли покупатели."
    else:
        context = "За последние 24 часа изменение BTC умеренное. Рынок пока не демонстрирует однозначного краткосрочного направления."
    technical_text = "Технические данные: дневные свечи временно недоступны."
    if technical:
        technical_text = (f"Изменение за 7 дней: {technical['change7']:+.2f}% · за 30 дней: {technical['change30']:+.2f}%\n"
                          f"SMA20: ${technical['sma20']:,.0f}" + (f" · SMA50: ${technical['sma50']:,.0f}" if technical['sma50'] else "") +
                          (f" · RSI14: {technical['rsi14']:.1f}" if technical['rsi14'] is not None else ""))
    text = ("₿ <b>BITCOIN: ПОДРОБНЫЙ ОБЗОР И ТРИ СЦЕНАРИЯ</b>\n\n"
            f"Текущая цена: <b>${price:,.0f}</b>\nИзменение за 24 часа: <b>{change:+.2f}%</b>\n"
            f"Диапазон 24ч: ${low:,.0f} — ${high:,.0f}\nОбъём BTCUSDT за 24ч: ${volume:,.0f}\n{technical_text}\n\n"
            f"📌 <b>Контекст</b>\n{context}\n\n"
            "🟢 <b>Бычий сценарий:</b> цена удерживает локальные поддержки, спрос и объёмы расширяются — возможно продолжение восстановления.\n\n"
            "🟡 <b>Боковой сценарий:</b> объёмы снижаются, цена остаётся в диапазоне — вероятна консолидация.\n\n"
            "🔴 <b>Медвежий сценарий:</b> поддержка теряется, продажи усиливаются — возрастает риск дальнейшей коррекции.\n\n"
            "🔎 <b>Что отслеживать:</b> потоки спотовых ETF, макроданные, ликвидность, открытый интерес и реакцию цены на ключевые уровни.\n\n"
            "Это сценарии, а не обещание движения цены. Горизонт — ближайшие дни; долгосрочная картина пересматривается по мере новых данных.\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📲 <a href=\"{html.escape(CHANNEL_URL, quote=True)}\">Подписаться на IXCLY NEWS</a>")
    await publish(bot, session, text, "Bitcoin: три сценария рынка", "₿ Bitcoin и рынок")
    _mark_routine("btc")


async def btc_longterm_outlook(bot: Bot, session: aiohttp.ClientSession):
    """Weekly structural outlook for 2027–2030; deliberately avoids invented price targets."""
    if _routine_done("longterm"):
        return
    text = (
        "🔭 <b>BITCOIN 2027–2030: ЧТО МОЖЕТ ОПРЕДЕЛИТЬ СЛЕДУЮЩИЕ ГОДЫ</b>\n\n"
        "Долгосрочная траектория Bitcoin будет зависеть не от одного события, а от сочетания спроса, ликвидности, регулирования и устойчивости инфраструктуры. Точные цены на несколько лет вперёд надёжно определить невозможно.\n\n"
        "🟢 <b>Сценарий расширения:</b> институциональный доступ развивается, регулирование становится понятнее, инфраструктура хранения и расчётов укрепляется. Это может поддержать долгосрочный спрос, но не исключает глубоких коррекций.\n\n"
        "🟡 <b>Сценарий зрелого рынка:</b> Bitcoin постепенно интегрируется в финансовую систему, а доходность становится более зависимой от макроэкономики и потоков капитала. Возможны длительные периоды бокового движения.\n\n"
        "🔴 <b>Сценарий давления:</b> жёсткие ограничения, ухудшение глобальной ликвидности, снижение интереса инвесторов или системные сбои могут привести к продолжительным просадкам.\n\n"
        "📌 <b>Что отслеживать:</b> институциональные потоки, регулирование в США/ЕС/РФ, использование сети, ликвидность, конкуренцию со стороны других активов и последствия следующего халвинга.\n\n"
        "Это карта возможных условий, а не ценовой прогноз и не инвестиционная рекомендация. Пересматриваем её по мере появления новых фактов.\n\n"
        "━━━━━━━━━━━━━━\n"
        f"📲 <a href=\"{html.escape(CHANNEL_URL, quote=True)}\">Подписаться на IXCLY NEWS</a>"
    )
    await publish(bot, session, text, "Bitcoin 2027–2030: долгосрочные сценарии", "🔭 Долгосрочная аналитика")
    _mark_routine("longterm")


async def daily_digest(bot: Bot, session: aiohttp.ClientSession):
    if _routine_done("recap"):
        return
    posts = _today_posts()
    if not posts:
        return
    lines = ["📊 <b>ИТОГИ ДНЯ: КРИПТОРЫНОК И ТЕХНОЛОГИИ</b>"]
    for category, title in posts[-5:]:
        lines.append(f"\n• <b>{html.escape(title)}</b>\n{html.escape(category)}")
    lines.extend(["\n\nВывод: оценивайте новости в контексте ликвидности, макроэкономики и реакции рынка. Отдельный заголовок не задаёт направление цены.",
                  "\n\n━━━━━━━━━━━━━━", f"📲 <a href=\"{html.escape(CHANNEL_URL, quote=True)}\">Подписаться на IXCLY NEWS</a>"])
    text = "".join(lines)[:1000]
    await publish(bot, session, text, "Итоги дня", "📊 Итоги дня")
    _mark_routine("recap")


async def run_publisher(stop_event: asyncio.Event | None = None):
    """Работает как фоновая задача внутри IXCLY Trading, без отдельного веб-сервиса."""
    if not BOT_TOKEN:
        raise RuntimeError("Set IXCLY_NEWS_BOT_TOKEN in Render Environment")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    bot = Bot(BOT_TOKEN)
    headers = {"User-Agent": "IXCLY-NEWS/1.0 (+https://t.me/ixcly_news)"}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            me = await bot.get_me()
            LOG.info("IXCLY NEWS publisher started as @%s; channel=%s", me.username, CHANNEL_ID)
            while stop_event is None or not stop_event.is_set():
                try:
                    now = datetime.now(LOCAL_TZ)
                    hour = now.hour
                    if 7 <= hour <= 10 and not _routine_done("btc"):
                        await btc_outlook(bot, session)
                    if 18 <= hour <= 21 and not _routine_done("recap"):
                        await daily_digest(bot, session)
                    if now.weekday() == 6 and 11 <= hour <= 14 and not _routine_done("longterm"):
                        await btc_longterm_outlook(bot, session)
                    await publish_news_batch(bot, session)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOG.exception("publisher loop error")
                if stop_event is None:
                    await asyncio.sleep(POLL_SECONDS)
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
                    except asyncio.TimeoutError:
                        pass
    finally:
        await bot.session.close()


async def main():
    await run_publisher()

if __name__ == "__main__":
    asyncio.run(main())
