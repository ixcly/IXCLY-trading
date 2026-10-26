import asyncio
import os
import ccxt
import pandas as pd
import numpy as np
import feedparser
import json
import requests
import io
import time
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
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
ADMIN_ID = os.getenv('ADMIN_ID') 

# --- ТОЛЬКО САМЫЕ НАДЕЖНЫЕ БИРЖИ ---
ex_opts = {'defaultType': 'swap', 'timeout': 7000}
exchange_list = [
    {'name': 'OKX', 'ex': ccxt.okx(ex_opts)},
    {'name': 'BYBIT', 'ex': ccxt.bybit(ex_opts)},
    {'name': 'MEXC', 'ex': ccxt.mexc(ex_opts)},
    {'name': 'GATE', 'ex': ccxt.gate(ex_opts)}
]

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

active_chats = set()
waiting_for_search = set()
last_news_link = None
last_request_time = 0

POPULAR_COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "TON", "SHIB", "DOT", "LINK",
    "BCH", "NEAR", "POL", "LTC", "ICP", "UNI", "APT", "STX", "FIL", "IMX", "HBAR", "ATOM",
    "INJ", "OP", "ARB", "VET", "MKR", "SUI", "RNDR", "GRT", "THETA", "S", "SAND", "MANA",
    "GALA", "PEPE", "WIF", "BONK", "ORDI", "JUP", "TIA", "SEI"
]

USERS_FILE = 'users.json'

def save_user_data(user_id, username, first_name):
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            users = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        users = {}

    user_id_str = str(user_id)
    is_new = False
    if user_id_str not in users:
        is_new = True
        users[user_id_str] = {
            "username": username,
            "first_name": first_name,
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=4)
    return is_new

def get_coins_keyboard(page=0):
    per_page = 12
    start = page * per_page
    end = start + per_page
    coins_page = POPULAR_COINS[start:end]
    
    buttons = []
    for i in range(0, len(coins_page), 2):
        row = [InlineKeyboardButton(text=f"{coins_page[i]}", callback_data=f"{coins_page[i]}USDT")]
        if i + 1 < len(coins_page):
            row.append(InlineKeyboardButton(text=f"{coins_page[i+1]}", callback_data=f"{coins_page[i+1]}USDT"))
        buttons.append(row)
        
    nav_row = []
    total_pages = (len(POPULAR_COINS) + per_page - 1) // per_page
    if page > 0: nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page_{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"Стр. {page+1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1: nav_row.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"page_{page+1}"))
    buttons.append(nav_row)
    
    buttons.append([InlineKeyboardButton(text="🔍 Поиск монеты", callback_data="search_coin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def calc_exact_probability(df):
    ema20 = float(df['ema20'].iloc[-1])
    ema50 = float(df['ema50'].iloc[-1])
    rsi = float(df['rsi'].iloc[-1])
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = float(macd.iloc[-1] - signal.iloc[-1])

    score = 0
    if ema20 > ema50:
        diff_pct = ((ema20 - ema50) / ema50) * 100
        score += 20 + min(20, diff_pct * 15)
    else:
        diff_pct = ((ema50 - ema20) / ema50) * 100
        score -= 20 + min(20, diff_pct * 15)
        
    if rsi > 50: score += min(30, (rsi - 50) * 1.2)
    else: score -= min(30, (50 - rsi) * 1.2)
        
    if macd_hist > 0: score += 30
    else: score -= 30
        
    long_prob = 50 + (score / 2)
    long_prob = max(5.0, min(95.0, long_prob))
    short_prob = 100 - long_prob
    return round(long_prob, 2), round(short_prob, 2)

def generate_trade_image(symbol, direction, entry, tp1, tp2, tp3, sl1, sl2, sl3, tf, long_prob):
    width, height = 800, 1000
    img = Image.new('RGB', (width, height), color='#131722')
    draw = ImageDraw.Draw(img)
    try:
        font_logo = ImageFont.truetype("DejaVuSans-Bold.ttf", 36)
        font_title = ImageFont.truetype("DejaVuSans-Bold.ttf", 55)
        font_text = ImageFont.truetype("DejaVuSans.ttf", 26)
        font_bold = ImageFont.truetype("DejaVuSans-Bold.ttf", 28)
    except:
        font_logo = font_title = font_text = font_bold = ImageFont.load_default()

    main_color = '#089981' if "LONG" in direction else '#F23645'
    for i in range(0, width, 80): draw.line([(i, 0), (i, height)], fill='#2a2e39', width=1)
    for i in range(0, height, 80): draw.line([(0, i), (width, i)], fill='#2a2e39', width=1)

    draw.text((40, 40), "ixcly trade", font=font_logo, fill='#D1D4DC')
    draw.text((40, 110), f"{symbol}", font=font_title, fill='#D1D4DC')
    draw.text((450, 125), "LONG" if "LONG" in direction else "SHORT", font=font_title, fill=main_color)
    draw.text((40, 190), f"TF: {tf}  |  Вероятность: {long_prob}%", font=font_text, fill='#787B86')

    current_y = 345
    trend_step = 4 if "SHORT" in direction else -4
    for i in range(25):
        x = 40 + i * 28
        current_y += trend_step + np.random.randint(-15, 15)
        current_y = max(270, min(420, current_y))
        body_h = np.random.randint(10, 35)
        wick_h = body_h + np.random.randint(10, 25)
        c_color = '#089981' if np.random.rand() > 0.4 else '#F23645'
        draw.line([(x + 6, current_y - wick_h//2), (x + 6, current_y + wick_h//2)], fill=c_color, width=2)
        draw.rectangle([x, current_y - body_h//2, x + 12, current_y + body_h//2], fill=c_color)

    y_offset = 520
    draw.text((40, y_offset), "Цена входа:", font=font_text, fill='#787B86')
    draw.text((250, y_offset-5), f"{entry:.6f}", font=font_bold, fill='#D1D4DC')
    y_offset += 80
    draw.text((40, y_offset), "Take Profit:", font=font_text, fill='#787B86')
    draw.text((250, y_offset), f"TP1: {tp1:.6f}", font=font_bold, fill='#089981')
    draw.text((250, y_offset+45), f"TP2: {tp2:.6f}", font=font_bold, fill='#089981')
    draw.text((250, y_offset+90), f"TP3: {tp3:.6f}", font=font_bold, fill='#089981')
    y_offset += 170
    draw.text((40, y_offset), "Stop Loss:", font=font_text, fill='#787B86')
    draw.text((250, y_offset), f"SL1: {sl1:.6f}", font=font_bold, fill='#F23645')
    draw.text((250, y_offset+45), f"SL2: {sl2:.6f}", font=font_bold, fill='#F23645')
    draw.text((250, y_offset+90), f"SL3: {sl3:.6f}", font=font_bold, fill='#F23645')

    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return buf

def calc_ema(series, length): return series.ewm(span=length, adjust=False).mean()
def calc_rsi(series, length=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=length).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=length).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))
def calc_atr(df, length=14):
    hl = df['high'] - df['low']
    hc = np.abs(df['high'] - df['close'].shift())
    lc = np.abs(df['low'] - df['close'].shift())
    tr = np.max(pd.concat([hl, hc, lc], axis=1), axis=1)
    return tr.rolling(length).mean()

async def animate_loading(message: types.Message, symbol_name="Монета"):
    phrases = [
        f"📡 Сканируем {symbol_name}...",
        "⚡ Мгновенный поиск...",
        "📊 Считаем индикаторы...",
        "⚖️ Готовим уровни..."
    ]
    try:
        i = 0
        while True:
            await asyncio.sleep(1.5)
            await message.edit_text(phrases[i % len(phrases)])
            i += 1
    except asyncio.CancelledError:
        pass
    except Exception: pass

# --- API ЗАПРОСЫ С ЖЕСТКИМ ТАЙМАУТОМ ---
async def fetch_openai_compatible(prompt, base_url, api_key, model, extra_headers=None):
    if not api_key: return None
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers: headers.update(extra_headers)
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1}
    try:
        response = await asyncio.to_thread(requests.post, f"{base_url}/chat/completions", headers=headers, json=payload, timeout=(5, 10))
        res_data = response.json()
        if 'error' in res_data or 'choices' not in res_data: return None
        return res_data['choices'][0]['message']['content']
    except Exception: return None

async def fetch_gemini(prompt):
    if not GEMINI_API_KEYS: return None
    headers = {'Content-Type': 'application/json'}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    for key in GEMINI_API_KEYS:
        headers['x-goog-api-key'] = key
        try:
            res = await asyncio.to_thread(requests.post, "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent", headers=headers, json=payload, timeout=(5, 10))
            res_data = res.json()
            if 'error' in res_data: continue 
            return res_data['candidates'][0]['content']['parts'][0]['text']
        except Exception: continue
    return None

async def fetch_analysis(prompt):
    try:
        result = await asyncio.wait_for(fetch_gemini(prompt), timeout=4.0)
        if result: return result
    except asyncio.TimeoutError: pass
        
    try:
        result = await asyncio.wait_for(fetch_openai_compatible(prompt, "https://api.groq.com/openai/v1", GROQ_API_KEY, "llama-3.1-8b-instant"), timeout=4.0)
        if result: return result
    except asyncio.TimeoutError: pass
        
    try:
        result = await asyncio.wait_for(fetch_openai_compatible(prompt, "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, "meta-llama/llama-3.1-8b-instruct:free", {"HTTP-Referer": "https://t.me/bot", "X-Title": "Bot"}), timeout=4.0)
        if result: return result
    except asyncio.TimeoutError: pass
        
    return "ERROR"

# --- ПОИСК НА БИРЖАХ И COINGECKO ---
async def check_exchange(ex_name, ex_instance, base_coin):
    for symbol in [f"{base_coin}/USDT:USDT", f"{base_coin}/USDT"]:
        try:
            tf_data = {}
            for tf in ['15m', '1h', '4h', '1d']:
                # Убрали fetch_ticker, сразу запрашиваем свечи. Timeout увеличен до 6 сек.
                ohlcv = await asyncio.wait_for(asyncio.to_thread(ex_instance.fetch_ohlcv, symbol, tf, limit=100), timeout=6.0)
                if not ohlcv: raise Exception("No data")
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['ema20'] = calc_ema(df['close'], 20)
                df['ema50'] = calc_ema(df['close'], 50)
                df['rsi'] = calc_rsi(df['close'], 14)
                df['atr'] = calc_atr(df, 14)
                
                tf_data[tf] = {
                    'df': df, 
                    'price': float(df['close'].iloc[-1]), 
                    'ema20': float(df['ema20'].iloc[-1]),
                    'ema50': float(df['ema50'].iloc[-1]), 
                    'rsi': float(df['rsi'].iloc[-1]), 
                    'atr': float(df['atr'].iloc[-1]),
                    'recent_low': float(df['low'].iloc[-24:].min()), 
                    'recent_high': float(df['high'].iloc[-24:].max())
                }
            if tf_data: return tf_data, ex_name
        except Exception: continue
    return None, None

async def fetch_coingecko(base_coin):
    try:
        # Таймаут CoinGecko увеличен до 10 секунд
        url = f"https://api.coingecko.com/api/v3/search?query={base_coin}"
        res = await asyncio.to_thread(requests.get, url, timeout=10)
        if res.status_code != 200: return None, None
        coins = res.json().get('coins', [])
        if not coins: return None, None
        
        coin_id = coins[0]['id']
        chart_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=7&interval=daily"
        chart_res = await asyncio.to_thread(requests.get, chart_url, timeout=10)
        if chart_res.status_code != 200: return None, None
        
        prices = chart_res.json().get('prices', [])
        if not prices: return None, None
        
        df = pd.DataFrame(prices, columns=['timestamp', 'close'])
        df['open'] = df['close'].shift(1).fillna(df['close'])
        df['high'] = df[['open', 'close']].max(axis=1)
        df['low'] = df[['open', 'close']].min(axis=1)
        df['volume'] = 0
        
        df['ema20'] = calc_ema(df['close'], 20)
        df['ema50'] = calc_ema(df['close'], 50)
        df['rsi'] = calc_rsi(df['close'], 14)
        df['atr'] = calc_atr(df, 14)
        
        last = df.iloc[-1]
        return {
            'df': df, 
            'price': float(last['close']), 
            'ema20': float(last['ema20']), 
            'ema50': float(last['ema50']), 
            'rsi': float(last['rsi']), 
            'atr': float(last['atr']), 
            'recent_low': float(df['low'].iloc[-7:].min()), 
            'recent_high': float(df['high'].iloc[-7:].max())
        }, 'CoinGecko'
    except Exception: 
        return None, None

async def process_signal(chat_id, symbol):
    global last_request_time
    now = time.time()
    if now - last_request_time < 2:
        await bot.send_message(chat_id, "⏳ Подождите пару секунд...")
        return
    last_request_time = now
    
    loading_msg = await bot.send_message(chat_id, f"⏳ Запускаем анализ {symbol}...")
    animation_task = asyncio.create_task(animate_loading(loading_msg, symbol))
    
    try:
        async def run_analysis():
            base_coin = symbol.replace('USDT', '')
            
            tasks = [check_exchange(item['name'], item['ex'], base_coin) for item in exchange_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            tf_data, found_exchange = None, None
            for res in results:
                if isinstance(res, tuple) and len(res) == 2 and res[0] is not None:
                    tf_data, found_exchange = res
                    break
            
            if not tf_data:
                cg_result = await fetch_coingecko(base_coin)
                if cg_result and cg_result[0]:
                    tf_data, found_exchange = cg_result
                else:
                    raise ValueError(f"Монета {base_coin} не найдена на биржах и в CoinGecko.")
                
            tf_text = "".join([f"[{tf}] RSI: {d['rsi']:.2f}, EMA20: {d['ema20']:.6f}, EMA50: {d['ema50']:.6f}, ATR: {d['atr']:.6f}\n" for tf, d in tf_data.items()])

            prompt = f"""
            Проанализируй {base_coin}:
            {tf_text}
            Верни СТРОГО JSON: {{"dominant_tf": "1h", "reason": "Краткое обоснование (1 предложение)."}}
            """
            
            raw_text = await fetch_analysis(prompt)
            
            if raw_text == "ERROR":
                dominant_tf = "4h" if "4h" in tf_data else "1d"
                reason = "Анализ выполнен локальной математической моделью."
            else:
                try:
                    clean = raw_text.replace('```json', '').replace('```', '').strip()
                    ai_data = json.loads(clean)
                    dominant_tf = ai_data.get("dominant_tf", "4h")
                    reason = ai_data.get("reason", "Анализ завершен.")
                except Exception:
                    dominant_tf = "4h"
                    reason = "Анализ выполнен на основе индикаторов."

            if dominant_tf not in tf_data: dominant_tf = list(tf_data.keys())[-1]

            tf_info = tf_data[dominant_tf]
            df = tf_info['df']
            
            entry = float(tf_info['price'])
            atr = float(tf_info['atr'])
            recent_low = float(tf_info['recent_low'])
            recent_high = float(tf_info['recent_high'])

            long_prob, short_prob = calc_exact_probability(df)

            if long_prob > 50:
                direction = "🟢 LONG"
                sl1 = float(recent_low - (0.2 * atr))
                sl2 = float(entry - (1.5 * atr))
                sl3 = float(entry - (2.0 * atr))
                risk = float(entry - sl1)
                tp1 = float(entry + (1.0 * risk))
                tp2 = float(entry + (2.0 * risk))
                tp3 = float(entry + (3.0 * risk))
            else:
                direction = "🔴 SHORT"
                sl1 = float(recent_high + (0.2 * atr))
                sl2 = float(entry + (1.5 * atr))
                sl3 = float(entry + (2.0 * atr))
                risk = float(sl1 - entry)
                tp1 = float(entry - (1.0 * risk))
                tp2 = float(entry - (2.0 * risk))
                tp3 = float(entry - (3.0 * risk))

            formatted_text = (
                f"<b>📊 Анализ: {symbol}</b>\n"
                f"<i>Данные: {found_exchange.upper()}</i>\n\n"
                f"🎯 <b>Вердикт:</b> {direction}\n"
                f"Вероятность Лонга: <b>{long_prob}%</b>\n"
                f"Вероятность Шорта: <b>{short_prob}%</b>\n"
                f"⏱ <b>Таймфрейм:</b> {dominant_tf}\n"
                f"💵 <b>Цена:</b> <code>{entry:.6f}</code>\n\n"
                f"🚀 <b>Цели (TP):</b>\n"
                f"├ TP1: <code>{tp1:.6f}</code>\n"
                f"├ TP2: <code>{tp2:.6f}</code>\n"
                f"└ TP3: <code>{tp3:.6f}</code>\n\n"
                f"🛑 <b>Стопы (SL):</b>\n"
                f"├ SL1: <code>{sl1:.6f}</code>\n"
                f"├ SL2: <code>{sl2:.6f}</code>\n"
                f"└ SL3: <code>{sl3:.6f}</code>\n\n"
                f"📰 <b>Обоснование:</b>\n<i>{reason}</i>\n\n"
                f"⚠️ <i>Не является финансовой рекомендацией!</i>"
            )
            
            image_buf = generate_trade_image(symbol, direction, entry, tp1, tp2, tp3, sl1, sl2, sl3, dominant_tf, long_prob)
            return formatted_text, image_buf

        # Общий таймаут на всё: 25 секунд (с запасом для медленного Render)
        formatted_text, image_buf = await asyncio.wait_for(run_analysis(), timeout=25.0)
        await bot.send_photo(chat_id=chat_id, photo=BufferedInputFile(image_buf.read(), filename=f"{symbol}.png"), caption=formatted_text, parse_mode='HTML')
        
    except asyncio.TimeoutError:
        await bot.send_message(chat_id, "⏳ Биржи не ответили вовремя. Попробуйте еще раз.")
    except ValueError as ve:
        await bot.send_message(chat_id, f"❌ {str(ve)}")
    except Exception as e:
        await bot.send_message(chat_id, f"⚠️ Ошибка: {str(e)}")
    finally:
        animation_task.cancel()
        try: await loading_msg.delete()
        except Exception: pass

async def news_watcher():
    global last_news_link
    await asyncio.sleep(10)
    while True:
        try:
            feed = await asyncio.to_thread(feedparser.parse, 'https://cointelegraph.com/rss')
            if feed.entries:
                latest_news = feed.entries[0]
                if latest_news.link != last_news_link:
                    last_news_link = latest_news.link
                    prompt = f"Напиши пост для Telegram на русском по новости: {latest_news.title}. Описание: {getattr(latest_news, 'summary', '')}. Укажи влияние и 2-3 монеты. В конце добавь 💰ВРЕМЯ ТОРГОВАТЬ💸."
                    ai_text = await fetch_analysis(prompt)
                    if ai_text != "ERROR":
                        final_text = f"{ai_text}\n\n🔗 <a href='{latest_news.link}'>Источник</a>"
                        for chat_id in list(active_chats):
                            try: await bot.send_message(chat_id=chat_id, text=final_text, parse_mode='HTML')
                            except Exception: pass
        except Exception: pass
        await asyncio.sleep(900)

# --- ХЕНДЛЕРЫ ---
@dp.message(Command('start', 'menu'))
async def start_handler(message: types.Message):
    active_chats.add(message.chat.id)
    waiting_for_search.discard(message.chat.id)
    
    is_new = save_user_data(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if is_new and ADMIN_ID:
        try:
            await bot.send_message(int(ADMIN_ID), f"👤 <b>Новый пользователь!</b>\n\nID: <code>{message.from_user.id}</code>\nИмя: {message.from_user.first_name}")
        except Exception: pass
        
    await message.answer("👋 Выбери монету из списка или нажми «🔍 Поиск монеты»:", reply_markup=get_coins_keyboard(0))

@dp.message(Command('news'))
async def handle_news_command(message: types.Message):
    active_chats.add(message.chat.id)
    await message.answer("⏳ Запрашиваем последнюю новость...")

@dp.message(F.text)
async def handle_search_text(message: types.Message):
    if message.text.startswith('/'): return 
        
    if message.chat.id in waiting_for_search:
        waiting_for_search.remove(message.chat.id)
        coin = message.text.upper().replace('USDT', '').replace('/', '').strip()
        
        if not coin.isalnum():
            await message.answer("⚠️ Неверный формат. Введите тикер.", reply_markup=get_coins_keyboard(0))
            return
            
        await process_signal(message.chat.id, f"{coin}USDT")
        await message.answer("Выбери следующую монету:", reply_markup=get_coins_keyboard(0))
    else:
        await message.answer("Используй кнопки или нажми /menu.", reply_markup=get_coins_keyboard(0))

@dp.callback_query(F.data)
async def process_callback(callback_query: types.CallbackQuery):
    data = callback_query.data
    chat_id = callback_query.message.chat.id
    
    if data == "ignore": 
        await callback_query.answer()
        return
        
    if data.startswith("page_"):
        page = int(data.split("_")[1])
        await callback_query.message.edit_reply_markup(reply_markup=get_coins_keyboard(page))
        await callback_query.answer()
        return
        
    if data == "search_coin":
        waiting_for_search.add(chat_id)
        await callback_query.message.answer("🔍 <b>Режим поиска</b>\n\nНапиши тикер (например: <code>WIF</code>), и я найду его.", parse_mode='HTML')
        await callback_query.answer()
        return
        
    waiting_for_search.discard(chat_id)
    await callback_query.answer()
    
    symbol = data
    await process_signal(chat_id, symbol)

async def main():
    keep_alive()
    print("Бот запущен...", flush=True)
    await bot.set_my_commands([
        types.BotCommand(command="menu", description="🚀 Меню"),
        types.BotCommand(command="news", description="📰 Новости")
    ])
    asyncio.create_task(news_watcher())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Критическая ошибка: {e}", flush=True)

if __name__ == '__main__':
    asyncio.run(main())
