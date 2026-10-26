import asyncio
import os
import ccxt
import pandas as pd
import numpy as np
import feedparser
import json
import requests
import io
from PIL import Image, ImageDraw, ImageFont
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
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

# --- ФУНКЦИЯ ГЕНЕРАЦИИ КАРТИНКИ ---
def generate_trade_image(symbol, direction, entry, tp1, tp2, tp3, sl1, sl2, sl3, tf, long_prob):
    # Создаем черное полотно 800x800
    img = Image.new('RGB', (800, 800), color='#111111')
    draw = ImageDraw.Draw(img)
    
    # Пытаемся загрузить шрифты (на Render обычно есть DejaVu)
    try:
        font_logo = ImageFont.truetype("DejaVuSans-Bold.ttf", 40)
        font_title = ImageFont.truetype("DejaVuSans-Bold.ttf", 60)
        font_text = ImageFont.truetype("DejaVuSans.ttf", 30)
        font_bold = ImageFont.truetype("DejaVuSans-Bold.ttf", 30)
    except:
        font_logo = font_title = font_text = font_bold = ImageFont.load_default()

    # Цвета
    color_green = '#00FF7F'
    color_red = '#FF4500'
    color_white = '#FFFFFF'
    color_gray = '#888888'
    
    main_color = color_green if "LONG" in direction else color_red

    # Логотип
    draw.text((40, 40), "ixcly trade", font=font_logo, fill=color_white)
    
    # Монета и направление
    draw.text((40, 120), f"{symbol}", font=font_title, fill=color_white)
    draw.text((450, 135), f"{direction.split(' ')[1]}", font=font_logo, fill=main_color)
    
    # Таймфрейм и вероятность
    draw.text((40, 200), f"TF: {tf}  |  Вероятность: {long_prob}%", font=font_text, fill=color_gray)

    # Рисуем фейковый график (для визуала)
    graph_y = 260
    draw.line((40, graph_y+100, 760, graph_y-100 if "LONG" in direction else graph_y+150), fill=main_color, width=4)
    for i in range(10):
        x = 40 + i * 80
        y = graph_y + np.random.randint(-50, 50)
        draw.rectangle([x, y-20, x+20, y+20], fill=main_color)

    # Уровни
    y_offset = 450
    draw.text((40, y_offset), "Точка входа:", font=font_text, fill=color_gray)
    draw.text((300, y_offset), f"{entry:.4f}", font=font_bold, fill=color_white)
    
    y_offset += 60
    draw.text((40, y_offset), "Тейк-профиты:", font=font_text, fill=color_gray)
    draw.text((300, y_offset), f"TP1: {tp1:.4f}", font=font_bold, fill=color_green)
    draw.text((550, y_offset), f"TP2: {tp2:.4f}", font=font_bold, fill=color_green)
    draw.text((40, y_offset+40), f"TP3: {tp3:.4f}", font=font_bold, fill=color_green)
    
    y_offset += 120
    draw.text((40, y_offset), "Стоп-лоссы:", font=font_text, fill=color_gray)
    draw.text((300, y_offset), f"SL1: {sl1:.4f}", font=font_bold, fill=color_red)
    draw.text((550, y_offset), f"SL2: {sl2:.4f}", font=font_bold, fill=color_red)
    draw.text((40, y_offset+40), f"SL3: {sl3:.4f}", font=font_bold, fill=color_red)

    # Сохраняем в буфер
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return buf
# ---------------------------------------------------------

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
        "⚙️ Система ищет точку входа и доминирующий тренд...",
        "⚖️ Рассчитываем уровни риска (3 TP / 3 SL)...",
        "📈 Завершаем глубокий анализ данных..."
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

async def fetch_analysis(prompt):
    models_to_try = ["gemini-3.8-flash", "gemini-3.8-flash-medium", "gemini-3.5-flash"]
    session = requests.Session()
    retry_strategy = Retry(total=2, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)

    last_error_message = "Неизвестная ошибка"
    for model_name in models_to_try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY}
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"thinkingConfig": {"thinkingLevel": "LOW"}}}
        
        try:
            response = await asyncio.to_thread(session.post, url, headers=headers, json=payload, timeout=(15, 90))
            res_data = response.json()
            
            if 'error' in res_data and res_data['error'].get('code') == 503:
                last_error_message = res_data['error'].get('message', 'Сервер перегружен')
                continue
            if 'error' in res_data:
                last_error_message = res_data['error'].get('message', 'Неизвестная ошибка')
                continue
            
            return res_data['candidates'][0]['content']['parts'][0]['text']
        except Exception as e:
            last_error_message = str(e)
            continue
            
    return f"ERROR: {last_error_message}"

async def analyze_news(title, summary):
    prompt = f"""
    Ты профессиональный крипто-аналитик. Прочитай эту новость и напиши пост для Telegram на русском языке.
    Заголовок: {title}
    Описание: {summary}
    
    В ответе напиши (без лишнего текста):
    📌 Суть: (что произошло)
    📉 Влияние на рынок: (позитив/негатив)
    🎯 Затронутые монеты: (из списка: BTC, ETH, SOL, BNB, DOGE, XRP)
    
    В конце добавь: 💰ВРЕМЯ ТОРГОВАТЬ💸
    """
    result = await fetch_analysis(prompt)
    if result.startswith("ERROR:"):
        return f"📰 {title}\n\n(Не удалось получить разбор новости)\n\n💰ВРЕМЯ ТОРГОВАТЬ💸"
    return result

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
        okx_symbol = f"{symbol[:-4]}/USDT:USDT" if symbol.endswith('USDT') else symbol
        
        tf_data = await get_multi_tf_data(okx_symbol)
        if not tf_data:
            return "❌ Не удалось получить данные с биржи.", None
            
        tf_text = ""
        for tf, data in tf_data.items():
            tf_text += f"[{tf}] Цена: {data['price']:.4f}, RSI: {data['rsi']:.2f}, EMA20: {data['ema20']:.4f}, EMA50: {data['ema50']:.4f}, ATR: {data['atr']:.4f}\n"

        prompt = f"""
        Ты профессиональный трейдер. Проанализируй данные по монете {symbol} на разных таймфреймах.
        Данные:
        {tf_text}
        Твоя задача: выбрать ОДИН доминирующий таймфрейм, который сейчас наиболее точно показывает направление движения цены.
        
        Верни СТРОГО JSON без лишнего текста:
        {{"long_prob": 70, "short_prob": 30, "dominant_tf": "1h", "reason": "Краткое обоснование на русском."}}
        """

        raw_text = await fetch_analysis(prompt)
        if raw_text.startswith("ERROR:"):
            return f"⚠️ Ошибка получения данных: {raw_text}", None

        try:
            clean_response = raw_text.replace('```json', '').replace('```', '').strip()
            ai_data = json.loads(clean_response)
        except Exception:
            return "⚠️ Ошибка обработки данных.", None

        long_prob = ai_data.get("long_prob", 50)
        short_prob = ai_data.get("short_prob", 50)
        dominant_tf = ai_data.get("dominant_tf", "4h")
        reason = ai_data.get("reason", "Анализ завершен.")

        if dominant_tf not in tf_data:
            dominant_tf = "4h" if "4h" in tf_data else list(tf_data.keys())[-1]

        tf_info = tf_data[dominant_tf]
        entry = tf_info['price']
        atr = tf_info['atr']

        if long_prob > short_prob:
            direction = "🟢 LONG"
            sl1, sl2, sl3 = entry - (0.5 * atr), entry - (1.0 * atr), entry - (1.5 * atr)
            tp1, tp2, tp3 = entry + (1.0 * atr), entry + (2.0 * atr), entry + (3.0 * atr)
        else:
            direction = "🔴 SHORT"
            sl1, sl2, sl3 = entry + (0.5 * atr), entry + (1.0 * atr), entry + (1.5 * atr)
            tp1, tp2, tp3 = entry - (1.0 * atr), entry - (2.0 * atr), entry - (3.0 * atr)

        # Красивый, структурированный текст
        formatted_text = (
            f"<b>📊 Анализ монеты: {symbol}</b>\n\n"
            f"🎯 <b>Рекомендация:</b> {direction} (Вероятность: {max(long_prob, short_prob)}%)\n"
            f"⏱ <b>Рабочий таймфрейм:</b> {dominant_tf}\n"
            f"💵 <b>Текущая цена:</b> <code>{entry:.4f}</code>\n\n"
            f"🚀 <b>Цели (Take Profit):</b>\n"
            f"├ TP1: <code>{tp1:.4f}</code>\n"
            f"├ TP2: <code>{tp2:.4f}</code>\n"
            f"└ TP3: <code>{tp3:.4f}</code>\n\n"
            f"🛑 <b>Стоп-лоссы (Stop Loss):</b>\n"
            f"├ SL1: <code>{sl1:.4f}</code>\n"
            f"├ SL2: <code>{sl2:.4f}</code>\n"
            f"└ SL3: <code>{sl3:.4f}</code>\n\n"
            f"📰 <b>Обоснование:</b>\n"
            f"<i>{reason}</i>\n\n"
            f"⚠️ <i>Не является финансовой рекомендацией!</i>"
        )
        
        # Генерируем картинку
        image_buf = generate_trade_image(symbol, direction, entry, tp1, tp2, tp3, sl1, sl2, sl3, dominant_tf, max(long_prob, short_prob))
        
        return formatted_text, image_buf

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return f"⚠️ Системная ошибка: {str(e)}", None

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
                    
                    ai_analysis_text = await analyze_news(news_title, news_summary)
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
    await callback_query.answer()
    symbol = callback_query.data
    loading_msg = await callback_query.message.answer(f"⏳ Запрос {symbol}...")
    animation_task = asyncio.create_task(animate_loading(loading_msg))
    
    text_result, image_buf = await get_signal_data(symbol)
    
    animation_task.cancel()
    try:
        await loading_msg.delete()
    except Exception:
        pass

    if image_buf:
        # Отправляем картинку с подписью
        photo = BufferedInputFile(image_buf.read(), filename=f"{symbol}.png")
        await callback_query.message.answer_photo(photo=photo, caption=text_result, parse_mode='HTML')
    else:
        # Если картинка не сгенерировалась, отправляем просто текст
        await callback_query.message.answer(text_result, parse_mode='HTML')

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
