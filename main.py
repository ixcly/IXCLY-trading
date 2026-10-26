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

# Хранилище пользователей и последняя новость
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

# --- УМНЫЙ ТЕКСТ ДЛЯ ОЖИДАНИЯ ---
async def animate_loading(message: types.Message):
    phrases = [
        "📡 Сканируем рыночные данные...",
        "📰 Анализируем мировые новости и настроения...",
        "📊 Рассчитываем индикаторы RSI, EMA и ATR...",
        "🧠 Нейросеть формирует среднесрочный вердикт...",
        "⚖️ Оцениваем риски и потенциальную доходность...",
        "⚙️ Завершаем глубокий анализ данных..."
    ]
    try:
        i = 0
        while True:
            await asyncio.sleep(5) # Ждем 5 секунд
            await message.edit_text(phrases[i % len(phrases)])
            i += 1
    except asyncio.CancelledError:
        pass # Задача отменена (анализ завершен)
    except Exception as e:
        print(f"Ошибка при обновлении текста: {e}")

# --- ФОНОВАЯ ЗАДАЧА ДЛЯ НОВОСТЕЙ ---
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
                
                if news_link != last_news_link:
                    last_news_link = news_link
                    
                    image_url = None
                    if hasattr(latest_news, 'media_content'):
                        image_url = latest_news.media_content[0]['url']
                    elif hasattr(latest_news, 'enclosures') and latest_news.enclosures:
                        if 'image' in latest_news.enclosures[0].get('type', ''):
                            image_url = latest_news.enclosures[0].get('href')
                    
                    text = f"📰 <b>Срочные новости:</b>\n\n{news_title}\n\n💰<b>ВРЕМЯ ТОРГОВАТЬ</b>💸\n\n🔗 <a href='{news_link}'>Читать полностью</a>"
                    
                    for chat_id in list(active_chats):
                        try:
                            if image_url:
                                await bot.send_photo(chat_id=chat_id, photo=image_url, caption=text, parse_mode='HTML')
                            else:
                                await bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', disable_web_page_preview=False)
                        except Exception as e:
                            print(f"Не удалось отправить новость в чат {chat_id}: {e}")
                            
        except Exception as e:
            print(f"Ошибка при чтении новостей: {e}")
            
        await asyncio.sleep(900) # Ждем 15 минут
# ---------------------------------------------------------

async def get_signal_data(symbol):
    okx_symbol = f"{symbol[:-4]}/USDT:USDT"
    try:
        ohlcv = await asyncio.to_thread(exchange.fetch_ohlcv, okx_symbol, '4h', limit=100)
        if not ohlcv:
            return f"❌ Нет данных по тикеру {okx_symbol}."

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        df['ema20'] = calc_ema(df['close'], 20)
        df['ema50'] = calc_ema(df['close'], 50)
        df['rsi'] = calc_rsi(df['close'], 14)
        df['atr'] = calc_atr(df, 14)

        current_price = df['close'].iloc[-1]
        current_ema20 = df['ema20'].iloc[-1]
        current_ema50 = df['ema50'].iloc[-1]
        current_rsi = df['rsi'].iloc[-1]
        current_atr = df['atr'].iloc[-1]

        news_feed = await asyncio.to_thread(feedparser.parse, 'https://cointelegraph.com/rss')
        news_titles = [entry.title for entry in news_feed.entries[:5]]
        news_text = "\n".join(news_titles) if news_titles else "Новостей нет."

        prompt = f"""
        Ты профессиональный среднесрочный трейдер (свинг-трейдер). Твоя задача — дать стабильный прогноз на 4-часовом таймфрейме (4H).
        Игнорируй краткосрочный рыночный шум и мелкие откаты. Оценивай общий тренд.
        
        Монета: {symbol}
        Текущая цена: {current_price}
        RSI (14): {current_rsi:.2f}
        EMA20 (быстрая): {current_ema20:.2f}
        EMA50 (медленная): {current_ema50:.2f}
        ATR (волатильность): {current_atr:.2f}
        
        Последние новости (общий фон):
        {news_text}
        
        Верни ТОЛЬКО JSON без лишнего текста: 
        {{"long_prob": 70, "short_prob": 30, "reason": "Краткое обоснование на русском (1-2 предложения). Укажи, куда движется среднесрочный тренд."}}
        """

        models_to_try = ["gemini-3.8-flash", "gemini-3.8-flash-medium", "gemini-3.5-flash"]

        session = requests.Session()
        retry_strategy = Retry(total=2, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

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
            except requests.exceptions.Timeout:
                last_error_message = "Таймаут ожидания ответа (90 секунд)."
                continue
            except Exception as e:
                last_error_message = str(e)
                continue

        if res_data is None or 'error' in res_data:
            return f"⚠️ Не удалось получить ответ. Причина: {last_error_message}. Попробуйте позже."

        try:
            raw_text = res_data['candidates'][0]['content']['parts'][0]['text']
        except (KeyError, IndexError):
            return "⚠️ Нейросеть вернула пустой ответ. Попробуйте позже。"

        clean_response = raw_text.replace('```json', '').replace('```', '').strip()
        try:
            ai_data = json.loads(clean_response)
        except json.JSONDecodeError:
            return "⚠️ Нейросеть вернула некорректный формат."

        long_prob = ai_data.get("long_prob", 50)
        short_prob = ai_data.get("short_prob", 50)
        reason = ai_data.get("reason", "Анализ завершен。")

        if long_prob > short_prob:
            direction = "🟢 LONG"
            entry = current_price
            sl = entry - (1.5 * current_atr)
            tp = entry + (3.0 * current_atr)
        else:
            direction = "🔴 SHORT"
            entry = current_price
            sl = entry + (1.5 * current_atr)
            tp = entry - (3.0 * current_atr)

        return (
            f"📊 <b>Среднесрочный анализ по {symbol} (4H)</b>\n\n"
            f"🤖 <b>Вердикт:</b> {direction}\n"
            f"Вероятность Лонга: <b>{long_prob}%</b>\n"
            f"Вероятность Шорта: <b>{short_prob}%</b>\n\n"
            f"Текущая цена: <code>{current_price:.4f}</code>\n"
            f"Точка входа: <code>{entry:.4f}</code>\n"
            f"🛑 Стоп-лосс: <code>{sl:.4f}</code>\n"
            f"🎯 Тейк-профит: <code>{tp:.4f}</code>\n\n"
            f"📰 <b>Обоснование:</b> <i>{reason}</i>\n\n"
            f"<i>Не является финансовой рекомендацией!</i>"
        )
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return f"⚠️ Системная ошибка: {str(e)}"

@dp.message(Command('start', 'menu'))
@dp.message(F.text == "🚀 Открыть меню")
async def start_handler(message: types.Message):
    active_chats.add(message.chat.id)
    
    reply_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Открыть меню")]],
        resize_keyboard=True,
        is_persistent=True
    )
    await message.answer("Привет! Нажми кнопку ниже, чтобы выбрать монету.", reply_markup=reply_kb)
    await message.answer("👋 Выбери монету для анализа:", reply_markup=get_coins_keyboard())

@dp.callback_query(F.data)
async def process_callback(callback_query: types.CallbackQuery):
    # Сразу убираем "часики" с кнопки в Telegram
    await callback_query.answer()
    
    symbol = callback_query.data
    
    # Отправляем первое сообщение о поиске
    loading_msg = await callback_query.message.answer(f"⏳ Запрос {symbol}...")
    
    # Запускаем задачу, которая будет менять текст каждые 5 секунд
    animation_task = asyncio.create_task(animate_loading(loading_msg))
    
    # Ждем, пока нейросеть и биржа выдадут результат
    result = await get_signal_data(symbol)
    
    # Останавливаем анимацию текста
    animation_task.cancel()
    
    # Удаляем сообщение с поиском
    try:
        await loading_msg.delete()
    except Exception as e:
        print(f"Не удалось удалить сообщение: {e}")
        
    # Отправляем чистый финальный результат
    await callback_query.message.answer(result, parse_mode='HTML')

async def main():
    keep_alive()
    print("Бот запущен...")
    asyncio.create_task(news_watcher())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Критическая ошибка при запуске: {e}")

if __name__ == '__main__':
    asyncio.run(main())
