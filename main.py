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

# --- РУЧНОЙ РАСЧЕТ ИНДИКАТОРОВ (вместо pandas_ta) ---
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

def get_coins_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🟡 BTC", callback_data="BTCUSDT"), InlineKeyboardButton(text="🔷 ETH", callback_data="ETHUSDT")],
        [InlineKeyboardButton(text="🟣 SOL", callback_data="SOLUSDT"), InlineKeyboardButton(text="🟡 BNB", callback_data="BNBUSDT")],
        [InlineKeyboardButton(text="🐕 DOGE", callback_data="DOGEUSDT"), InlineKeyboardButton(text="🔵 XRP", callback_data="XRPUSDT")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def get_signal_data(symbol):
    okx_symbol = f"{symbol[:-4]}/USDT:USDT"
    try:
        ohlcv = await asyncio.to_thread(exchange.fetch_ohlcv, okx_symbol, '15m', limit=100)
        if not ohlcv:
            return f"❌ Нет данных по тикеру {okx_symbol}."

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Считаем индикаторы вручную
        df['ema20'] = calc_ema(df['close'], 20)
        df['rsi'] = calc_rsi(df['close'], 14)
        df['atr'] = calc_atr(df, 14)

        current_price = df['close'].iloc[-1]
        current_ema = df['ema20'].iloc[-1]
        current_rsi = df['rsi'].iloc[-1]
        current_atr = df['atr'].iloc[-1]

        news_feed = await asyncio.to_thread(feedparser.parse, 'https://cointelegraph.com/rss')
        news_titles = [entry.title for entry in news_feed.entries[:5]]
        news_text = "\n".join(news_titles) if news_titles else "Новостей нет."

        prompt = f"""
        Ты профессиональный трейдер. Оцени данные и дай прогноз.
        Монета: {symbol}
        Текущая цена: {current_price}
        RSI: {current_rsi:.2f}
        EMA20: {current_ema:.2f}
        ATR: {current_atr:.2f}
        Последние новости:
        {news_text}
        
        Верни ТОЛЬКО JSON без лишнего текста: 
        {{"long_prob": 70, "short_prob": 30, "reason": "Краткое обоснование на русском (1-2 предложения)"}}
        """

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        headers = {'Content-Type': 'application/json'}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        
        response = await asyncio.to_thread(requests.post, url, headers=headers, json=payload, timeout=25)
        res_data = response.json()
        
        raw_text = res_data['candidates'][0]['content']['parts'][0]['text']
        clean_response = raw_text.replace('```json', '').replace('```', '').strip()
        ai_data = json.loads(clean_response)

        long_prob = ai_data.get("long_prob", 50)
        short_prob = ai_data.get("short_prob", 50)
        reason = ai_data.get("reason", "Анализ завершен.")

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
            f"📊 <b>Анализ по {symbol}</b>\n\n"
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
        return f"⚠️ Ошибка: {str(e)}"

@dp.message(Command('start', 'menu'))
@dp.message(F.text == "🚀 Открыть меню")
async def start_handler(message: types.Message):
    reply_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Открыть меню")]],
        resize_keyboard=True,
        is_persistent=True
    )
    await message.answer("Привет! Нажми кнопку ниже, чтобы выбрать монету.", reply_markup=reply_kb)
    await message.answer("👋 Выбери монету для анализа:", reply_markup=get_coins_keyboard())

@dp.callback_query(F.data)
async def process_callback(callback_query: types.CallbackQuery):
    symbol = callback_query.data
    await callback_query.message.answer(f"⏳ Запрос {symbol}... Ожидайте примерно 5-10 секунд.")
    result = await get_signal_data(symbol)
    await callback_query.message.answer(result, parse_mode='HTML')
    await callback_query.answer()

async def main():
    keep_alive()
    print("Бот запущен...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
