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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile, ReplyKeyboardRemove
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
GEMINI_API_KEYS = [k.strip() for k in os.getenv('GEMINI_API_KEYS', '').split(',') if k.strip()]

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
AION_API_KEY = os.getenv('AION_API_KEY')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')

exchange = ccxt.okx({'options': {'defaultType': 'swap'}})
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

active_chats = set()
last_news_link = None

# --- ФУНКЦИЯ ГЕНЕРАЦИИ КАРТИНКИ ---
def generate_trade_image(symbol, direction, entry, tp1, tp2, tp3, sl1, sl2, sl3, tf, long_prob):
    width, height = 800, 1000
    bg_color = '#131722'
    grid_color = '#2a2e39'
    text_white = '#D1D4DC'
    text_gray = '#787B86'
    color_green = '#089981'
    color_red = '#F23645'
    
    img = Image.new('RGB', (width, height), color=bg_color)
    draw = ImageDraw.Draw(img)
    
    try:
        font_logo = ImageFont.truetype("DejaVuSans-Bold.ttf", 36)
        font_title = ImageFont.truetype("DejaVuSans-Bold.ttf", 55)
        font_text = ImageFont.truetype("DejaVuSans.ttf", 26)
        font_bold = ImageFont.truetype("DejaVuSans-Bold.ttf", 28)
    except:
        font_logo = font_title = font_text = font_bold = ImageFont.load_default()

    main_color = color_green if "LONG" in direction else color_red

    for i in range(0, width, 80):
        draw.line([(i, 0), (i, height)], fill=grid_color, width=1)
    for i in range(0, height, 80):
        draw.line([(0, i), (width, i)], fill=grid_color, width=1)

    draw.text((40, 40), "ixcly trade", font=font_logo, fill=text_white)
    draw.text((40, 110), f"{symbol}", font=font_title, fill=text_white)
    
    dir_text = "LONG" if "LONG" in direction else "SHORT"
    draw.text((500, 125), dir_text, font=font_title, fill=main_color)
    
    draw.text((40, 190), f"TF: {tf}  |  Вероятность: {long_prob}%", font=font_text, fill=text_gray)

    chart_top = 240
    chart_bottom = 450
    chart_height = chart_bottom - chart_top
    mid_y = chart_top + chart_height // 2
    
    current_y = mid_y
    trend_step = 4 if "SHORT" in direction else -4

    for i in range(25):
        x = 40 + i * 28
        current_y += trend_step + np.random.randint(-15, 15)
        current_y = max(chart_top + 30, min(chart_bottom - 30, current_y))
        
        body_height = np.random.randint(10, 35)
        wick_height = body_height + np.random.randint(10, 25)
        
        candle_color = color_green if np.random.rand() > 0.4 else color_red
        
        draw.line([(x + 6, current_y - wick_height//2), (x + 6, current_y + wick_height//2)], fill=candle_color, width=2)
        draw.rectangle([x, current_y - body_height//2, x + 12, current_y + body_height//2], fill=candle_color)

    y_offset = 520
    
    draw.text((40, y_offset), "Цена входа:", font=font_text, fill=text_gray)
    draw.text((250, y_offset-5), f"{entry:.4f}", font=font_bold, fill=text_white)
    
    y_offset += 80
    draw.text((40, y_offset), "Take Profit:", font=font_text, fill=text_gray)
    draw.text((250, y_offset), f"TP1: {tp1:.4f}", font=font_bold, fill=color_green)
    draw.text((250, y_offset+45), f"TP2: {tp2:.4f}", font=font_bold, fill=color_green)
    draw.text((250, y_offset+90), f"TP3: {tp3:.4f}", font=font_bold, fill=color_green)
    
    y_offset += 170
    draw.text((40, y_offset), "Stop Loss:", font=font_text, fill=text_gray)
    draw.text((250, y_offset), f"SL1: {sl1:.4f}", font=font_bold, fill=color_red)
    draw.text((250, y_offset+45), f"SL2: {sl2:.4f}", font=font_bold, fill=color_red)
    draw.text((250, y_offset+90), f"SL3: {sl3:.4f}", font=font_bold, fill=color_red)

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
        [InlineKeyboardButton(text="👑 BTC", callback_data="BTCUSDT"), InlineKeyboardButton(text="💎 ETH", callback_data="ETHUSDT")],
        [InlineKeyboardButton(text="⚡ SOL", callback_data="SOLUSDT"), InlineKeyboardButton(text="🔶 BNB", callback_data="BNBUSDT")],
        [InlineKeyboardButton(text="🐶 DOGE", callback_data="DOGEUSDT"), InlineKeyboardButton(text="🌊 XRP", callback_data="XRPUSDT")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def animate_loading(message: types.Message):
    phrases = [
        "📡 Сканируем рыночные данные...",
        "📰 Анализируем мировые новости...",
        "📊 Рассчитываем индикаторы...",
        "⚙️ Ищем точку входа...",
        "⚖️ Рассчитываем TP и SL...",
        "📈 Завершаем анализ..."
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

# --- УНИВЕРСАЛЬНЫЙ ЗАПРОС К OPENAI-СОВМЕСТИМЫМ API ---
async def fetch_openai_compatible(prompt, base_url, api_key, model, extra_headers=None):
    if not api_key:
        print(f"Пропуск {base_url}: нет API-ключа.", flush=True)
        return None
    url = f"{base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    if extra_headers:
        headers.update(extra_headers)
        
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    try:
        response = await asyncio.to_thread(requests.post, url, headers=headers, json=payload, timeout=(15, 90))
        res_data = response.json()
        
        if 'error' in res_data:
            print(f"Ошибка от {base_url}: {res_data['error']}", flush=True)
            return None
            
        if 'choices' not in res_data:
            print(f"Неожиданный ответ от {base_url}: {res_data}", flush=True)
            return None
            
        return res_data['choices'][0]['message']['content']
    except Exception as e:
        print(f"Сбой соединения с {base_url}: {e}", flush=True)
        return None

# --- ЗАПРОС К GEMINI ---
async def fetch_gemini(prompt):
    if not GEMINI_API_KEYS:
        print("Пропуск Gemini: нет API-ключей.", flush=True)
        return None
    models_to_try = ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-2.0-flash-lite"]
    headers = {'Content-Type': 'application/json'}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    for key in GEMINI_API_KEYS:
        headers['x-goog-api-key'] = key
        for model_name in models_to_try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
            try:
                response = await asyncio.to_thread(requests.post, url, headers=headers, json=payload, timeout=(15, 90))
                res_data = response.json()
                
                if 'error' in res_data:
                    error_msg = res_data['error'].get('message', '')
                    if 'exceeded your current quota' in error_msg.lower() or 'quota' in error_msg.lower():
                        print(f"Ключ Gemini ...{key[-4:]} исчерпан.", flush=True)
                        break
                    if 'high demand' in error_msg.lower() or 'overloaded' in error_msg.lower():
                        print(f"Модель {model_name} перегружена.", flush=True)
                        continue
                    return None
                
                return res_data['candidates'][0]['content']['parts'][0]['text']
            except Exception as e:
                print(f"Сбой при запросе к Gemini {model_name}: {e}", flush=True)
                continue
    return None

# --- ГЛАВНАЯ ФУНКЦИЯ СБОРА АНАЛИЗА ---
async def fetch_analysis(prompt):
    # 1. Google Gemini
    result = await fetch_gemini(prompt)
    if result:
        print("✅ Успешный ответ от Google Gemini", flush=True)
        return result

    # 2. Groq
    result = await fetch_openai_compatible(prompt, "https://api.groq.com/openai/v1", GROQ_API_KEY, "llama-3.3-70b-versatile")
    if result:
        print("✅ Успешный ответ от Groq", flush=True)
        return result

    # 3. OpenRouter
    or_headers = {
        "HTTP-Referer": "https://t.me/ixcly_trade_bot", 
        "X-Title": "IXCLY Trade Bot"
    }
    result = await fetch_openai_compatible(prompt, "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, "google/gemini-2.0-flash-exp:free", or_headers)
    if result:
        print("✅ Успешный ответ от OpenRouter", flush=True)
        return result

    # 4. Aion Labs
    result = await fetch_openai_compatible(prompt, "https://api.aionlabs.ai/v1", AION_API_KEY, "aion-labs/aion-2.0")
    if result:
        print("✅ Успешный ответ от Aion Labs", flush=True)
        return result

    # 5. Cloudflare Workers AI
    if CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID:
        cf_url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/v1"
        result = await fetch_openai_compatible(prompt, cf_url, CLOUDFLARE_API_TOKEN, "@cf/meta/llama-3.1-8b-instruct")
        if result:
            print("✅ Успешный ответ от Cloudflare Workers AI", flush=True)
            return result
    else:
        print("Пропуск Cloudflare: нет токена или Account ID.", flush=True)

    # Если все системы отказали
    print("❌ Все системы отказали.", flush=True)
    return "ERROR: Все доступные системы перегружены или исчерпали лимиты. Попробуйте позже."

async def analyze_news(title, summary):
    prompt = f"""
    Ты профессиональный крипто-аналитик. Прочитай эту новость и напиши пост для Telegram на русском языке.
    Заголовок: {title}
    Описание: {summary}
    
    В ответе напиши (без лишнего текста, НЕ ИСПОЛЬЗУЙ слово "Суть"):
    📌 (что произошло)
    📉 Влияние на рынок: (позитив/негатив)
    🎯 Затронутые монеты: (из списка: BTC, ETH, SOL, BNB, DOGE, XRP)
    
    В конце добавь: 💰ВРЕМЯ ТОРГОВАТЬ💸
    """
    result = await fetch_analysis(prompt)
    if result.startswith("ERROR:"):
        return f"📰 {title}\n\n(Не удалось получить разбор новости: {result})\n\n💰ВРЕМЯ ТОРГОВАТЬ💸"
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
            
            # Ищем локальные минимумы и максимумы за последние 24 свечи
            recent_low = df['low'].iloc[-24:].min()
            recent_high = df['high'].iloc[-24:].max()
            
            data_summary[tf] = {
                'price': df['close'].iloc[-1],
                'ema20': df['ema20'].iloc[-1],
                'ema50': df['ema50'].iloc[-1],
                'rsi': df['rsi'].iloc[-1],
                'atr': df['atr'].iloc[-1],
                'recent_low': recent_low,
                'recent_high': recent_high
            }
        except Exception as e:
            print(f"Ошибка при получении данных {tf}: {e}", flush=True)
            
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
        recent_low = tf_info['recent_low']
        recent_high = tf_info['recent_high']

        # --- УЛУЧШЕННАЯ ЛОГИКА РАССТАНОВКИ УРОВНЕЙ ---
        if long_prob > short_prob:
            direction = "🟢 LONG"
            
            # SL1 ставим за локальный минимум, отступая 0.2 ATR для защиты от ложного пробоя
            sl1 = recent_low - (0.2 * atr)
            # SL2 и SL3 ставим на расстоянии 1.5 и 2.0 ATR от входа
            sl2 = entry - (1.5 * atr)
            sl3 = entry - (2.0 * atr)
            
            # Определяем риск как разницу между входом и SL1
            risk = entry - sl1
            
            # TP ставим по системе Risk/Reward: 1:1, 1:2, 1:3
            tp1 = entry + (1.0 * risk)
            tp2 = entry + (2.0 * risk)
            tp3 = entry + (3.0 * risk)
            
        else:
            direction = "🔴 SHORT"
            
            # SL1 ставим за локальный максимум, отступая 0.2 ATR для защиты от ложного пробоя
            sl1 = recent_high + (0.2 * atr)
            # SL2 и SL3 ставим на расстоянии 1.5 и 2.0 ATR от входа
            sl2 = entry + (1.5 * atr)
            sl3 = entry + (2.0 * atr)
            
            # Определяем риск как разницу между SL1 и входом
            risk = sl1 - entry
            
            # TP ставим по системе Risk/Reward: 1:1, 1:2, 1:3
            tp1 = entry - (1.0 * risk)
            tp2 = entry - (2.0 * risk)
            tp3 = entry - (3.0 * risk)
        # ------------------------------------------------

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
            f"├ SL1: <code>{sl1:.4f}</code> (За локальным уровнем)\n"
            f"├ SL2: <code>{sl2:.4f}</code>\n"
            f"└ SL3: <code>{sl3:.4f}</code>\n\n"
            f"📰 <b>Обоснование:</b>\n"
            f"<i>{reason}</i>\n\n"
            f"⚠️ <i>Не является финансовой рекомендацией!</i>"
        )
        
        image_buf = generate_trade_image(symbol, direction, entry, tp1, tp2, tp3, sl1, sl2, sl3, dominant_tf, max(long_prob, short_prob))
        
        return formatted_text, image_buf

    except Exception as e:
        print(f"ERROR: {str(e)}", flush=True)
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
                                img_resp = await asyncio.to_thread(requests.get, image_url, timeout=15)
                                if img_resp.status_code == 200:
                                    photo_file = BufferedInputFile(img_resp.content, filename="news.jpg")
                                    await bot.send_photo(chat_id=chat_id, photo=photo_file, caption=final_text, parse_mode='HTML')
                                else:
                                    await bot.send_message(chat_id=chat_id, text=final_text, parse_mode='HTML', disable_web_page_preview=False)
                            else:
                                await bot.send_message(chat_id=chat_id, text=final_text, parse_mode='HTML', disable_web_page_preview=False)
                        except Exception as e:
                            print(f"Не удалось отправить новость в чат {chat_id}: {e}", flush=True)
        except Exception as e:
            print(f"Ошибка при чтении новостей: {e}", flush=True)
        await asyncio.sleep(900)

# --- ХЕНДЛЕРЫ КОМАНД ---

@dp.message(Command('start', 'menu'))
async def start_handler(message: types.Message):
    active_chats.add(message.chat.id)
    await message.answer("👋 Главное меню активировано. Выбери монету для анализа:", reply_markup=ReplyKeyboardRemove())
    await message.answer("👇 Нажми на кнопку ниже:", reply_markup=get_coins_keyboard())

@dp.message(Command('news'))
async def handle_news_command(message: types.Message):
    active_chats.add(message.chat.id)
    msg = await message.answer("⏳ Запрашиваем и анализируем последнюю новость...")
    
    try:
        feed = await asyncio.to_thread(feedparser.parse, 'https://cointelegraph.com/rss')
        if feed.entries:
            latest_news = feed.entries[0]
            news_title = latest_news.title
            news_summary = getattr(latest_news, 'summary', '')
            news_link = latest_news.link
            
            image_url = None
            if hasattr(latest_news, 'media_content'):
                image_url = latest_news.media_content[0]['url']
            elif hasattr(latest_news, 'enclosures') and latest_news.enclosures:
                if 'image' in latest_news.enclosures[0].get('type', ''):
                    image_url = latest_news.enclosures[0].get('href')
            
            ai_analysis_text = await analyze_news(news_title, news_summary)
            final_text = f"{ai_analysis_text}\n\n🔗 <a href='{news_link}'>Читать источник</a>"
            
            await msg.delete()
            if image_url:
                img_resp = await asyncio.to_thread(requests.get, image_url, timeout=15)
                if img_resp.status_code == 200:
                    photo_file = BufferedInputFile(img_resp.content, filename="news.jpg")
                    await message.answer_photo(photo=photo_file, caption=final_text, parse_mode='HTML')
                else:
                    await message.answer(final_text, parse_mode='HTML', disable_web_page_preview=False)
            else:
                await message.answer(final_text, parse_mode='HTML', disable_web_page_preview=False)
        else:
            await msg.edit_text("❌ Свежих новостей пока нет.")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка при получении новостей: {e}")

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
        photo = BufferedInputFile(image_buf.read(), filename=f"{symbol}.png")
        await callback_query.message.answer_photo(photo=photo, caption=text_result, parse_mode='HTML')
    else:
        await callback_query.message.answer(text_result, parse_mode='HTML')

async def main():
    keep_alive()
    print("Бот запущен...", flush=True)
    
    await bot.set_my_commands([
        types.BotCommand(command="menu", description="🚀 Открыть меню"),
        types.BotCommand(command="news", description="📰 Последние новости")
    ])
    
    asyncio.create_task(news_watcher())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Критическая ошибка при запуске: {e}", flush=True)

if __name__ == '__main__':
    asyncio.run(main())
