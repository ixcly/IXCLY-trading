import asyncio
import os
import ccxt
import pandas as pd
import numpy as np
import feedparser
import json
import requests
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from flask import Flask
from threading import Thread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- ВЕБ-СЕРВЕР ДЛЯ RENDER ---
app = Flask('')
@app.route('/')
def home():
    return "Bot is alive!"

def run():
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

def keep_alive():
    t = Thread(target=run)
    t.start()
# ---------------------------------------------------------

BOT_TOKEN = os.getenv('BOT_TOKEN')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

exchange = ccxt.okx({'options': {'defaultType': 'swap'}})
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

active_chats = set()
last_news_link = None

# --- РУЧНОЙ РАСЧЕТ ИНДИКАТОРОВ ---
def calc_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def calc_rsi(series, length=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=length).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=length).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_atr(df, length=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.rolling(length).mean()
# ---------------------------------------------------------

def get_coins_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🟡 BTC", callback_data="BTCUSDT"), InlineKeyboardButton(text="🔷 ETH", callback_data="ETHUSDT")],
        [InlineKeyboardButton(text="🟣 SOL", callback_data="SOLUSDT"), InlineKeyboardButton(text="🟡 BNB", callback_data="BNBUSDT")],
        [InlineKeyboardButton(text="🐕 DOGE", callback_data="DOGEUSDT"), InlineKeyboardButton(text="🔵 XRP", callback_data="XRPUSDT")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def animate_loading(message: types.Message):
    phrases = [
        "📡 Сканируем рыночные данные (15m, 1h, 4h, 1d)...",
        "📰 Анализируем мировые новости и настроения...",
        "📊 Рассчитываем индикаторы на всех таймфреймах...",
        "🧠 Нейросеть ищет точку входа и доминирующий тренд...",
        "⚖️ Рассчитываем уровни риска (3 TP / 3 SL)...",
        "⚙️ Завершаем глубокий анализ данных..."
    ]
    try:
        i = 0
        while True:
            await asyncio.sleep(4)
            await message.edit_text(phrases[i % len(phrases)])
            i += 1
    except asyncio.CancelledError:
        pass
    except Exception:
        pass

async def analyze_news_with_ai(title, summary):
    prompt = f"""
    Ты профессиональный крипто-аналитик. Прочитай эту новость и напиши пост для Telegram на русском языке.
    
    Заголовок: {title}
    Описание: {summary}
    
    В ответе напиши (без лишнего текста, сразу по делу):
    📌 Суть: (что произошло, 1-2 предложения)
    📉 Влияние на рынок: (позитив/негатив и почему)
    🎯 Затронутые монеты: (выбери из списка: BTC, ETH, SOL, BNB, DOGE, XRP, на какие монеты это повлияет больше всего и почему)
    
    В самом конце добавь: 💰ВРЕМЯ ТОРГОВАТЬ💸
    Пиши с эмодзи, кратко и понятно для трейдера.
    """
    models_to_try = ["gemini-3.8-flash", "gemini-3.8-flash-medium", "gemini-3.5-flash"]
    session = requests.Session()
    
    for model_name in models_to_try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY}
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"thinkingConfig": {"thinkingLevel": "LOW"}}}
        
        try:
            response = await asyncio.to_thread(session.post, url, headers=headers, json=payload, timeout=(15, 60))
            res_data = response.json()
            if 'candidates' in res_data:
                return res_data['candidates'][0]['content']['parts'][0]['text']
        except Exception as e:
            print(f"Ошибка при анализе новости через {model_name}: {e}")
            continue
            
    return f"📰 {title}\n\n(Не удалось получить разбор от ИИ)\n\n💰ВРЕМЯ ТОРГОВАТЬ💸"

async def news_watcher():
    global last_news_link
    await asyncio.sleep(10)
    while True:
        try:
            feed = await asyncio.to_thread(feedparser.parse, 'https://cointelegraph.com/rss')
            if feed.entries:
                latest_news = feed.entries[0]
                news_link = latest_news.link
                news_title = latest_news.title
                news_summary = getattr(latest_news, 'summary', '')
                
                if news_link != last_news_link:
                    last_news_link = news_link
                    image_url = None
                    if hasattr(latest_news, 'media_content'):
                        image_url = latest_news.media_content[0]['url']
                    elif hasattr(latest_news, 'enclosures') and latest_news.enclosures:
                        if 'image' in latest_news.enclosures[0].get('type', ''):
                            image_url = latest_news.enclosures[0].get('href')
                    
                    ai_analysis_text = await analyze_news_with_ai(news_title, news_summary)
                    final_text = f"{ai_analysis_text}\n\n🔗 <a href='{news_link}'>Читать источник</a>"
                    
                    for chat_id in list(active_chats):
                        try:
                            if image_url:
                                await bot.send_photo(chat_id=chat_id, photo=image_url, caption=final_text, parse_mode='HTML')
                            else:
                                await bot.send_message(chat_id=chat_id, text=final_text, parse_mode='HTML', disable_web_page_preview=False)
                        except Exception as e:
                            print(f"Не удалось отправить новость в чат {chat_id}: {e}")
        except Exception as e:
            print(f"Ошибка при чтении новостей: {e}")
        await asyncio.sleep(900)

# --- СБОР ДАННЫХ СО ВСЕХ ТАЙМФРЕЙМОВ ---
async def get_multi_tf_data(symbol):
    timeframes = ['15m', '1h', '4h', '1d']
    data_summary = {}
    
    for tf in timeframes:
        try:
            ohlcv = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, tf, limit=100)
            if not ohlcv: continue
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['ema20'] = calc_ema(df['close'], 20)
            df['ema50'] = calc_ema(df['close'], 50)
            df['rsi'] = calc_rsi(df['close'], 14)
            df['atr'] = calc_atr(df, 14)
            
            data_summary[tf] = {
                'price': df['close'].iloc[-1],
                'ema20': df['ema20'].iloc[-1],
                'ema50': df['ema50'].iloc[-1],
                'rsi': df['rsi'].iloc[-1],
                'atr': df['atr'].iloc[-1]
            }
        except Exception as e:
            print(f"Ошибка при получении данных {tf}: {e}")
            
    return data_summary

async def get_signal_data(symbol):
    try:
        # Приводим тикер к формату OKX (например, BTC/USDT:USDT)
        okx_symbol = f"{symbol[:-4]}/USDT:USDT" if symbol.endswith('USDT') else symbol
        
        # Получаем данные по всем ТФ
        tf_data = await get_multi_tf_data(okx_symbol)
        if not tf_data:
            return "❌ Не удалось получить данные с биржи."
            
        # Формируем строку для промпта
        tf_text = ""
        for tf, data in tf_data.items():
            tf_text += f"""
            [{tf}] Цена: {data['price']:.4f}, RSI: {data['rsi']:.2f}, EMA20: {data['ema20']:.4f}, EMA50: {data['ema50']:.4f}, ATR: {data['atr']:.4f}
            """

        news_feed = await asyncio.to_thread(feedparser.parse, 'https://cointelegraph.com/rss')
        news_titles = [entry.title for entry in news_feed.entries[:5]]
        news_text = "\n".join(news_titles) if news_titles else "Новостей нет."

        prompt = f"""
        Ты профессиональный трейдер. Проанализируй данные по монете {symbol} на РАЗНЫХ таймфреймах.
        
        Данные:
        {tf_text}
        
        Последние новости:
        {news_text}
        
        Твоя задача: сделать общую оценку и выбрать ОДИН доминирующий таймфрейм, который сейчас наиболее точно показывает направление движения цены.
        
        Верни СТРОГО JSON без лишнего текста:
        {{
            "long_prob": 70, 
            "short_prob": 30, 
            "dominant_tf": "1h", 
            "reason": "Краткое обоснование на русском (1-2 предложения), учитывая выбранный таймфрейм, общий тренд и новости."
        }}
        """

        models_to_try = ["gemini-3.8-flash", "gemini-3.8-flash-medium", "gemini-3.5-flash"]
        session = requests.Session()
        retry_strategy = Retry(total=2, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        res_data = None
        last_error_message = ""

        for model_name in models_to_try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
            headers = {'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY}
            payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"thinkingConfig": {"thinkingLevel": "LOW"}}}

            try:
                response = await asyncio.to_thread(session.post, url, headers=headers, json=payload, timeout=(15, 90))
                res_data = response.json()
                if 'error' in res_data and res_data['error'].get('code') == 503:
                    last_error_message = res_data['error'].get('message', 'Модель перегружена')
                    continue
                if 'error' in res_data:
                    last_error_message = res_data['error'].get('message', 'Неизвестная ошибка')
                    continue
                break
            except Exception as e:
                last_error_message = str(e)
                continue

        if res_data is None or 'error' in res_data:
            return f"⚠️ Не удалось получить ответ. Причина: {last_error_message}"

        try:
            raw_text = res_data['candidates'][0]['content']['parts'][0]['text']
            clean_response = raw_text.replace('```json', '').replace('```', '').strip()
            ai_data = json.loads(clean_response)
        except Exception:
            return "⚠️ Нейросеть вернула некорректный формат."

        long_prob = ai_data.get("long_prob", 50)
        short_prob = ai_data.get("short_prob", 50)
        dominant_tf = ai_data.get("dominant_tf", "4h")
        reason = ai_data.get("reason", "Анализ завершен.")

        # Проверка, что ИИ вернул существующий ТФ (чтобы не сломать математику)
        if dominant_tf not in tf_data:
            dominant_tf = "4h" if "4h" in tf_data else list(tf_data.keys())[-1]

        # Берем данные именно того таймфрейма, который выбрал ИИ
        tf_info = tf_data[dominant_tf]
        entry = tf_info['price']
        atr = tf_info['atr']

        # Расчет уровней: 3 TP и 3 SL
        if long_prob > short_prob:
            direction = "🟢 LONG"
            # Для Лонга стопы
