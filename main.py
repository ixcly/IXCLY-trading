# -*- coding: utf-8 -*-
"""
IXCLY PRO - Web App Edition (TWA)
"""

import asyncio
import base64
import csv
import gc
import hashlib
import html
import io
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from threading import Thread
from urllib.parse import quote

import aiohttp
import feedparser
import numpy as np
import pandas as pd
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramBadRequest, TelegramConflictError, TelegramForbiddenError,
    TelegramNetworkError, TelegramNotFound, TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, ErrorEvent, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from flask import Flask, jsonify, render_template_string, request
from PIL import Image, ImageDraw, ImageFont

try:
    import ccxt.async_support as ccxt
except ImportError:
    ccxt = None
try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("ixcly")

# =====================================================================
# НАСТРОЙКИ
# =====================================================================
def clean_key(k): return k.strip().strip('"').strip("'") if k else None
BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))

if not BOT_TOKEN: raise SystemExit("Ошибка: BOT_TOKEN не задан!")
ADMIN_IDS = set()
for _x in re.split(r"[,\s]+", ADMIN_ID or ""):
    if _x.lstrip("-").isdigit(): ADMIN_IDS.add(int(_x))

def make_cipher():
    if not Fernet: return None
    raw = clean_key(os.getenv("FERNET_KEY"))
    if raw:
        try: return Fernet(raw.encode())
        except: pass
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(("ixcly:" + BOT_TOKEN).encode()).digest()))

cipher = make_cipher()
VERSION = "4.0.0 TWA"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}
POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))
CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "55"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "23"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))
SCAN_MAX_COINS = int(os.getenv("SCAN_MAX_COINS", "150"))
DEPOSIT = float(os.getenv("DEPOSIT", "1000"))
RISK_PCT = 1.0
SCAN_INTERVAL = 20 * 60
SIGNAL_COOLDOWN = 6 * 3600

bot = Bot(token=BOT_TOKEN, session=AiohttpSession(timeout=30.0))
dp = Dispatcher()
admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    admin_bot = Bot(token=INPUT_BOT_TOKEN, session=AiohttpSession(timeout=30.0))
    admin_dp = Dispatcher()

http = None
news_lock = None
scan_lock = None
main_event_loop = None # Ссылка на главный цикл для Flask

TFS = ["15m", "1h", "4h", "1d"]
MIN_CANDLES = 60

_COIN_TEXT = """
BTC ETH BNB SOL XRP DOGE ADA AVAX TON SHIB DOT LINK BCH NEAR POL LTC ICP UNI APT STX FIL IMX HBAR ATOM
INJ OP ARB VET MKR SUI RNDR GRT THETA S SAND MANA GALA PEPE WIF BONK ORDI JUP TIA SEI TRX XLM ETC XMR
AAVE ALGO EOS NEO IOTA ZEC DASH COMP SNX CRV LDO ENS 1INCH CAKE RUNE KSM FLOW AXS CHZ BAT ENJ ZIL FTM KAVA
EGLD ROSE AR FET WLD APE GMT CFX PYTH JTO ONDO FLOKI KAS CRO MNT TAO STRK DYDX GMX PENDLE SUSHI YFI BAL ZRX
KNC UMA BAND API3 SSV LPT RAY ORCA ILV MAGIC PRIME BEAM YGG ALICE TLM DUSK HIGH CHR C98 AI NMR ARKM IO ATH
BOME MEME SATS RATS DCR DGB XVG SC RVN WAVES ZEN BLAST AEVO ETHFI ENA W EIGEN MOVE PNUT GOAT ACT MOODENG
TURBO MOG NEIRO DOGS NOT PIXEL PORTAL DYM ALT MANTA ZETA TNSR DRIFT KMNO WEN MYRO POPCAT GIGA MEW SLERF
LUNC LUNA USTC CTSI COTI CELO CKB ONE QTUM IOST ONT ICX WAN ARK STRAX STMX PHA KLAY XEC XNO XTZ YFII XVS
XDC XAVA WOO WAXP VTHO VRA VOXEL VITE VIB VELO UOS UFT TRU TRB TOMO TLOS TKO TFUEL SUN STORJ STEEM SRM SLP
SKL SFP SFUND RSR RPL RLC RIF REEF RARE RAM QNT QKC QI PYR PUNDIX PROM POND POLYX POLS PNG PNK PERP PEG
PAXG PAR PAAL ONG OMG OGN OCEAN NKN NEXO NEBL NCT NAS MTL MTH MPL MNW MIR MINA MIM MFT META MBOX MBL MASK
LYXE LTO LSS LSK LQTY LOKA LOOM LIT LINA LAZIO LAT KMD JOE JASMY IOTX ION IDEX ID ICE HARD HAPI GTC GT GLMR
GLM GFT GEL FXS FTT FT FIDA FIO FARM ELF ELA DREP DODO DOCK DNT DMA DIA DENT CVC CTXC CTK CREAM COVAL COS
CND CMT CLV CITY CHESS CELR BURGER BTS BTG BOND BOSON BNT BNX BLZ BICO BETA BEL BAR BADGER BAKED AXL AVT
AVA AUCTION AUDIO ATM ATLAS ASTR ASR ARPA ARDR ANTV ANT ANKR AMP AMB ALPHA ALPACA AKRO AION AGLD AGIX ADX
ACM ACE ZK ZRO LISTA TAIKO REZ OMNI SAGA MAVIA XAI NFP PHB AIDOGE AKT ELIZA FAI AIT AGRS NOS VVAIFU BUZZ
SWARMS ARC ZYN LUCE GRIFFAIN ZEREBRO AI16Z BAN FARTCOIN PENGU HYPE HMSTR CATI
"""
POPULAR_COINS = list(dict.fromkeys(_COIN_TEXT.split()))
CORRELATION_GROUPS = [["ETH", "ARB", "OP", "MATIC", "STRK", "ZK", "MANTA"], ["SOL", "JUP", "PYTH", "WIF", "BONK"], ["DOGE", "SHIB", "PEPE", "FLOKI", "BOME", "MEME"], ["AVAX", "JOE", "GMX"], ["BNB", "CAKE"], ["LINK", "PYTH", "BAND"], ["UNI", "SUSHI", "1INCH"], ["ATOM", "OSMO", "TIA", "INJ", "SEI"], ["NEAR", "FET", "RNDR", "AGIX"]]
SYMBOL_ALIASES = {"RNDR": ["RENDER", "RNDR"], "MATIC": ["POL", "MATIC"], "PEPE": ["PEPE", "1000PEPE"], "BONK": ["BONK", "1000BONK"], "FLOKI": ["FLOKI", "1000FLOKI"], "SATS": ["SATS", "1000SATS"], "SHIB": ["SHIB", "1000SHIB"], "XEC": ["XEC", "1000XEC"], "LUNC": ["LUNC", "1000LUNC"], "RATS": ["RATS", "1000RATS"]}

# =====================================================================
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# =====================================================================
active_chats = set()
busy_users = set()
user_res = {}
analysis_cache = {}
bt_cache = {}
preferred_source = {}
fetch_errors = {}
signal_sent_at = {}
anomaly_sent_at = {}
scan_skip = {}
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "started": time.time()}
bot_flags = {"news_enabled": True, "alerts_enabled": True}
last_scan_info = {"ts": 0, "stage1": 0, "stage2": 0, "backtest_ok": 0, "final": 0, "secs": 0}
market_bias = {"bias": 0, "title_ru": "Нет данных", "summary": "", "impact": "Нейтральный фон", "coins": "BTC, ETH", "published": "", "link": "", "engine": "none", "ts": 0}
last_news_link = None
last_backup_msg = {}
next_timers = {"scanner": 0, "positions": 0, "pos_updates": 0, "news_watcher": 0, "backup": 0, "global_signals": 0, "anomaly": 0}
exchange_bans = {}
news_blackout_until = 0.0

STATE_FILE = "bot_state.json"
STATE_LOCK = threading.RLock()
STATE = {"users": {}, "signal_history": [], "analysis_db": {}, "last_msgs": {}, "persist": {}, "meta": {"saved_at": 0}}
state_dirty = False
backup_dirty = False
last_backup_ts = 0.0
STATE_KEYS = ("users", "signal_history", "analysis_db", "last_msgs", "persist")

def state_touch(important=True):
    global state_dirty, backup_dirty
    state_dirty = True
    if important: backup_dirty = True

def collect_persist():
    return {"flags": dict(bot_flags), "stats": {k: v for k, v in stats.items() if k != "started"}, "market_bias": dict(market_bias), "last_news_link": last_news_link, "signal_sent_at": dict(signal_sent_at), "anomaly_sent_at": dict(anomaly_sent_at), "last_scan_info": dict(last_scan_info), "bt_cache": {c: [v[0], v[1]] for c, v in bt_cache.items() if v[1]}}

def apply_persist(p):
    global last_news_link, market_bias
    if not isinstance(p, dict): return
    for k, v in (p.get("flags") or {}).items():
        if k in bot_flags: bot_flags[k] = bool(v)
    for k, v in (p.get("stats") or {}).items():
        if k in stats and k != "started": stats[k] = v
    mb = p.get("market_bias")
    if isinstance(mb, dict) and mb.get("engine") not in (None, "none"): market_bias.update(mb)
    last_news_link = p.get("last_news_link") or last_news_link
    signal_sent_at.update({k: float(v) for k, v in (p.get("signal_sent_at") or {}).items()})
    anomaly_sent_at.update({k: float(v) for k, v in (p.get("anomaly_sent_at") or {}).items()})
    if isinstance(p.get("last_scan_info"), dict): last_scan_info.update(p["last_scan_info"])
    for c, v in (p.get("bt_cache") or {}).items():
        if isinstance(v, list) and len(v) == 2: bt_cache[c] = (float(v[0]), v[1])

def restore_state(data, force=False):
    if not isinstance(data, dict) or not data: return 0
    if "users" not in data and all(isinstance(v, dict) for v in data.values()): data = {"users": data}
    with STATE_LOCK:
        local_ts = STATE["meta"].get("saved_at", 0)
        remote_ts = (data.get("meta") or {}).get("saved_at", 0)
        if not force and STATE["users"] and local_ts >= remote_ts: return 0
        for k in STATE_KEYS:
            if k in data and data[k] is not None: STATE[k] = data[k]
        STATE["meta"]["saved_at"] = max(local_ts, remote_ts)
        n = len(STATE["users"])
    apply_persist(STATE.get("persist"))
    state_touch(False)
    return n

def load_state_file():
    if not os.path.exists(STATE_FILE): return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f: restore_state(json.load(f), force=True)
    except Exception as e: log.error(f"Не удалось прочитать {STATE_FILE}: {e}")

def flush_state():
    global state_dirty
    with STATE_LOCK:
        STATE["persist"] = collect_persist()
        STATE["meta"] = {"saved_at": time.time(), "version": VERSION}
        data = json.dumps(STATE, ensure_ascii=False, default=str)
        try:
            with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f: f.write(data)
            os.replace(STATE_FILE + ".tmp", STATE_FILE)
            state_dirty = False
        except Exception as e: log.error(f"Ошибка записи: {e}")
    return data

async def state_flush_loop():
    while True:
        await asyncio.sleep(15)
        if state_dirty: await asyncio.to_thread(flush_state)

def _users(): return STATE["users"]
def save_user_data(user_id, username, first_name):
    key = str(user_id)
    with STATE_LOCK:
        new = key not in STATE["users"]
        u = STATE["users"].setdefault(key, {"joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        u.update({"username": username, "first_name": first_name, "active": True})
    state_touch(new)

def load_active_chats():
    active_chats.clear()
    for uid, u in _users().items():
        if isinstance(u, dict) and u.get("active", True):
            try: active_chats.add(int(uid))
            except ValueError: pass
    log.info("Получателей: %d", len(active_chats))

def get_user_notif_settings(user_id):
    u = _users().get(str(user_id), {})
    return {"signals": u.get("notify_signals", True), "news": u.get("notify_news", True), "anomalies": u.get("notify_anomalies", True), "pos_updates": u.get("notify_pos_updates", True), "briefing": u.get("notify_briefing", True)}

def get_user_open_positions(user_id): return dict(_users().get(str(user_id), {}).get("open_positions", {}))
def get_open_position(user_id, coin): return get_user_open_positions(user_id).get(coin)
def set_user_open_position(user_id, coin, data):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(user_id), {"active": True})
        pos = u.setdefault("open_positions", {})
        if data is None: pos.pop(coin, None)
        else: pos[coin] = data
    state_touch()

def load_signal_history():
    with STATE_LOCK: return [x for x in STATE["signal_history"] if time.time() - x.get("ts", 0) < 30 * 86400]
def save_signal_history(hist):
    hist = hist[-2000:]
    for x in hist[:-150]: x.pop("text", None)
    with STATE_LOCK: STATE["signal_history"] = hist
    state_touch()

def get_user_risk_settings(user_id):
    u = _users().get(str(user_id), {})
    return {"deposit": float(u.get("deposit", DEPOSIT)), "risk_pct": float(u.get("risk_pct", RISK_PCT)), "max_open_risk": float(u.get("max_open_risk", 5.0))}
def set_user_field(user_id, **fields):
    with STATE_LOCK: STATE["users"].setdefault(str(user_id), {"active": True}).update(fields)
    state_touch()

def fmt(p):
    p = float(p)
    a = abs(p)
    if a >= 1000: return f"{p:,.2f}".replace(",", " ")
    if a >= 1: return f"{p:.4f}"
    if a >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"

def strip_tags(t): return html.unescape(re.sub(r"<[^>]+>", "", t or ""))
def vis_len(t): return len(strip_tags(t))
def trim_html(text, limit):
    if vis_len(text) <= limit: return text
    return html.escape(strip_tags(text)[:limit - 1]) + "…"
def has_cyr(text, n=1): return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= n

async def first_success(coros, timeout=11):
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        pending = set(tasks)
        end = time.monotonic() + timeout
        while pending:
            remaining = end - time.monotonic()
            if remaining <= 0: break
            done, pending = await asyncio.wait(pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if not done: break
            for d in done:
                try: res = d.result()
                except Exception: continue
                if res is not None: return res
        return None
    finally:
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def http_json(url, params=None, timeout=8):
    async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200: raise RuntimeError(f"HTTP {r.status}")
        return await r.json(content_type=None)

async def download_bytes(url):
    try:
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200: return await r.read()
    except Exception: pass
    return None

async def _send_new(chat_id, text, kb, photo):
    try:
        if photo: return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=trim_html(text, 1024), parse_mode="HTML", reply_markup=kb)
        return await bot.send_message(chat_id, trim_html(text, 4000), parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "parse" not in str(e).lower() and "entit" not in str(e).lower(): raise
        plain = strip_tags(text)
        if photo: return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=plain[:1024], reply_markup=kb)
        return await bot.send_message(chat_id, plain[:4000], reply_markup=kb)

# =====================================================================
# MINI APP - ВЕБ-ИНТЕРФЕЙС И API
# =====================================================================
app = Flask(__name__)

MINI_APP_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>IXCLY PRO</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root { --bg: var(--tg-theme-bg-color, #0f1115); --text: var(--tg-theme-text-color, #ffffff); --hint: var(--tg-theme-hint-color, #8b8e96); --btn: var(--tg-theme-button-color, #2ea6ff); --card: var(--tg-theme-secondary-bg-color, #1c1f26); --accent: #00ff88; --danger: #ff3b30; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 0 0 70px 0; overflow-x: hidden; }
        .header { padding: 20px 15px; text-align: center; font-size: 22px; font-weight: 800; background: linear-gradient(90deg, var(--btn), var(--accent)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .tab-content { display: none; padding: 15px; animation: fadeIn 0.3s ease; }
        .tab-content.active { display: block; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .card { background: var(--card); border-radius: 16px; padding: 16px; margin-bottom: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.05); }
        .row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; font-size: 14px; }
        .row span:first-child { color: var(--hint); }
        .green { color: var(--accent); font-weight: bold; } .red { color: var(--danger); font-weight: bold; }
        .btn { display: block; width: 100%; padding: 14px; background: var(--btn); color: #fff; text-align: center; border-radius: 12px; text-decoration: none; font-weight: bold; border: none; font-size: 16px; margin-top: 10px; transition: 0.2s; }
        .btn:active { transform: scale(0.98); opacity: 0.8; }
        .btn-danger { background: var(--danger); }
        .nav-bar { position: fixed; bottom: 0; left: 0; width: 100%; background: var(--card); display: flex; justify-content: space-around; padding: 10px 0; border-top: 1px solid rgba(255,255,255,0.05); z-index: 100; padding-bottom: env(safe-area-inset-bottom); }
        .nav-item { display: flex; flex-direction: column; align-items: center; font-size: 11px; color: var(--hint); text-decoration: none; cursor: pointer; }
        .nav-item.active { color: var(--btn); }
        .nav-icon { font-size: 22px; margin-bottom: 4px; }
        .input-group { margin-bottom: 15px; }
        .input-group label { display: block; font-size: 12px; color: var(--hint); margin-bottom: 5px; }
        .input-group input, .input-group select { width: 100%; padding: 12px; border-radius: 10px; background: #0b0d10; border: 1px solid #2a2e35; color: #fff; font-size: 16px; box-sizing: border-box; }
        .toggle { display: flex; justify-content: space-between; align-items: center; padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05); }
        .toggle:last-child { border-bottom: none; }
        .switch { position: relative; display: inline-block; width: 50px; height: 26px; }
        .switch input { opacity: 0; width: 0; height: 0; }
        .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #3a3e45; transition: .4s; border-radius: 34px; }
        .slider:before { position: absolute; content: ""; height: 18px; width: 18px; left: 4px; bottom: 4px; background-color: white; transition: .4s; border-radius: 50%; }
        input:checked + .slider { background-color: var(--accent); }
        input:checked + .slider:before { transform: translateX(24px); }
        .badge { padding: 3px 8px; border-radius: 6px; font-size: 11px; background: #2a2e35; }
        .loading { text-align: center; color: var(--hint); padding: 40px 0; font-size: 14px; }
        .news-item { margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.05); }
        .news-item:last-child { border-bottom: none; }
        .news-title { font-size: 15px; font-weight: 600; margin-bottom: 5px; line-height: 1.3; }
        .news-desc { font-size: 13px; color: var(--hint); line-height: 1.4; }
    </style>
</head>
<body>
    <div class="header">IXCLY PRO</div>

    <!-- ВКЛАДКА ПОРТФЕЛЬ -->
    <div id="tab-portfolio" class="tab-content active">
        <div class="card">
            <div class="row"><span>Депозит:</span> <b id="p-dep">$0.00</b></div>
            <div class="row"><span>Риск на сделку:</span> <b id="p-risk">0%</b></div>
            <div class="row"><span>Открытый риск:</span> <b id="p-open-risk">0%</b></div>
        </div>
        <h3>📈 Открытые позиции</h3>
        <div id="positions-container"><div class="loading">Загрузка позиций...</div></div>
    </div>

    <!-- ВКЛАДКА СИГНАЛЫ -->
    <div id="tab-signals" class="tab-content">
        <div class="card" style="padding:10px; text-align:center; background: linear-gradient(45deg, #1c1f26, #2a2e35);">
            <div style="font-size:12px; color:var(--hint); margin-bottom:5px;">Рыночный фон:</div>
            <div id="market-bias" style="font-size:14px;">Анализ...</div>
        </div>
        <div id="signals-container"><div class="loading">Загрузка сигналов...</div></div>
    </div>

    <!-- ВКЛАДКА НОВОСТИ -->
    <div id="tab-news" class="tab-content">
        <button class="btn" onclick="loadNews(true)" style="margin-bottom:15px;">🔄 Сгенерировать дайджест ИИ</button>
        <div id="news-container" class="card"><div class="loading">Нажмите кнопку выше...</div></div>
    </div>

    <!-- ВКЛАДКА НАСТРОЙКИ -->
    <div id="tab-settings" class="tab-content">
        <div class="card">
            <h3 style="margin-top:0;">⚙️ Торговля</h3>
            <div class="input-group">
                <label>Депозит (USDT)</label>
                <input type="number" id="set-deposit" placeholder="1000">
            </div>
            <div class="input-group">
                <label>Риск на сделку (%)</label>
                <input type="number" id="set-risk" placeholder="1.0" step="0.1">
            </div>
            <div class="input-group">
                <label>Режим сигналов</label>
                <select id="set-mode">
                    <option value="aggressive">Агрессивный (60%)</option>
                    <option value="balanced">Сбалансированный (66%)</option>
                    <option value="conservative">Консервативный (80%)</option>
                </select>
            </div>
            <button class="btn" onclick="saveSettings()">💾 Сохранить</button>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">🔔 Уведомления</h3>
            <div class="toggle"><span>Новые сигналы</span><label class="switch"><input type="checkbox" id="notif-signals"><span class="slider"></span></label></div>
            <div class="toggle"><span>Новости</span><label class="switch"><input type="checkbox" id="notif-news"><span class="slider"></span></label></div>
            <div class="toggle"><span>Отчеты по сделкам</span><label class="switch"><input type="checkbox" id="notif-pos"><span class="slider"></span></label></div>
            <button class="btn" onclick="saveNotifs()" style="margin-top:15px;">💾 Сохранить</button>
        </div>
        
        <button class="btn btn-danger" onclick="exportCSV()">📥 Экспорт CSV</button>
    </div>

    <!-- НИЖНЕЕ МЕНЮ -->
    <div class="nav-bar">
        <div class="nav-item active" onclick="switchTab('portfolio', this)"><div class="nav-icon">💼</div>Портфель</div>
        <div class="nav-item" onclick="switchTab('signals', this)"><div class="nav-icon">🔥</div>Сигналы</div>
        <div class="nav-item" onclick="switchTab('news', this)"><div class="nav-icon">📰</div>Новости</div>
        <div class="nav-item" onclick="switchTab('settings', this)"><div class="nav-icon">⚙️</div>Настройки</div>
    </div>

    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();
        tg.ready();
        const userId = tg.initDataUnsafe?.user?.id;
        
        tg.BackButton.hide();

        function switchTab(tabId, el) {
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
            document.getElementById('tab-' + tabId).classList.add('active');
            el.classList.add('active');
            tg.HapticFeedback.impactOccurred('light');
            
            if (tabId === 'portfolio') loadPortfolio();
            if (tabId === 'signals') loadSignals();
            if (tabId === 'settings') loadSettings();
        }

        async function fetchAPI(endpoint, method = 'GET', body = null) {
            if (!userId) return;
            const url = `/api/${endpoint}?uid=${userId}`;
            const opts = { method, headers: { 'Content-Type': 'application/json' } };
            if (body) opts.body = JSON.stringify(body);
            try {
                const res = await fetch(url, opts);
                return await res.json();
            } catch(e) { console.error(e); return null; }
        }

        // --- ПОРТФЕЛЬ ---
        async function loadPortfolio() {
            const data = await fetchAPI('portfolio');
            if (!data) return;
            document.getElementById('p-dep').innerText = '$' + data.deposit.toFixed(2);
            document.getElementById('p-risk').innerText = data.risk_pct + '%';
            document.getElementById('p-open-risk').innerText = data.open_risk.toFixed(1) + '%';
            
            let html = '';
            if (Object.keys(data.positions).length === 0) {
                html = '<div class="card" style="text-align:center; color:var(--hint)">Нет открытых сделок</div>';
            } else {
                for (const [coin, pos] of Object.entries(data.positions)) {
                    const sc = pos.side === 'LONG' ? 'green' : 'red';
                    const st = pos.waiting_entry ? '⏳ Ожидание' : '⚡ В сделке';
                    html += `<div class="card" style="padding:12px; margin-bottom:10px;">
                        <div class="row"><span><b>${coin}</b> <span class="badge">${st}</span></span><span class="${sc}">${pos.side}</span></div>
                        <div class="row"><span>Вход:</span> <b>${pos.entry_price}</b></div>
                        <div class="row"><span>Стоп:</span> <b>${pos.sl1}</b></div>
                        <button class="btn btn-danger" style="padding:8px; font-size:14px; margin-top:5px;" onclick="closePos('${coin}')">Закрыть</button>
                    </div>`;
                }
            }
            document.getElementById('positions-container').innerHTML = html;
        }

        async function closePos(coin) {
            tg.showConfirm(`Закрыть позицию по ${coin}?`, async (ok) => {
                if (ok) {
                    await fetchAPI('close_pos', 'POST', {coin});
                    loadPortfolio();
                    tg.HapticFeedback.notificationOccurred('success');
                }
            });
        }

        // --- СИГНАЛЫ ---
        async function loadSignals() {
            const data = await fetchAPI('signals');
            if (!data) return;
            document.getElementById('market-bias').innerText = data.bias_text;
            
            let html = '';
            if (data.signals.length === 0) {
                html = '<div class="card" style="text-align:center; color:var(--hint)">Сейчас нет новых сигналов</div>';
            } else {
                data.signals.forEach(s => {
                    const sc = s.side === 'LONG' ? 'green' : 'red';
                    html += `<div class="card" style="padding:12px; margin-bottom:10px;">
                        <div class="row"><span><b>${s.coin}</b></span><span class="${sc}">${s.side} (${s.strength}%)</span></div>
                        <div class="row"><span>Вход:</span> <b>${s.entry}</b></div>
                        <div class="row"><span>TP1:</span> <b>${s.tp1}</b></div>
                        <button class="btn" style="padding:8px; font-size:14px; margin-top:5px;" onclick="enterPos('${s.coin}')">Войти в сделку</button>
                    </div>`;
                });
            }
            document.getElementById('signals-container').innerHTML = html;
        }

        async function enterPos(coin) {
            tg.showConfirm(`Войти в сделку по ${coin}?`, async (ok) => {
                if (ok) {
                    const res = await fetchAPI('enter_pos', 'POST', {coin});
                    if (res.error) tg.showAlert(res.error);
                    else { loadSignals(); tg.HapticFeedback.notificationOccurred('success'); }
                }
            });
        }

        // --- НОВОСТИ ---
        async function loadNews(force = false) {
            const cont = document.getElementById('news-container');
            cont.innerHTML = '<div class="loading">ИИ анализирует новости...</div>';
            const data = await fetchAPI('news' + (force ? '?force=1' : ''));
            if (data && data.text) {
                cont.innerHTML = data.text;
            } else {
                cont.innerHTML = 'Ошибка загрузки новостей.';
            }
        }

        // --- НАСТРОЙКИ ---
        async function loadSettings() {
            const data = await fetchAPI('settings');
            if (!data) return;
            document.getElementById('set-deposit').value = data.deposit;
            document.getElementById('set-risk').value = data.risk_pct;
            document.getElementById('set-mode').value = data.mode;
            document.getElementById('notif-signals').checked = data.notifs.signals;
            document.getElementById('notif-news').checked = data.notifs.news;
            document.getElementById('notif-pos').checked = data.notifs.pos_updates;
        }

        async function saveSettings() {
            const dep = parseFloat(document.getElementById('set-deposit').value);
            const risk = parseFloat(document.getElementById('set-risk').value);
            const mode = document.getElementById('set-mode').value;
            await fetchAPI('save_settings', 'POST', {deposit: dep, risk_pct: risk, mode: mode});
            tg.showAlert("Настройки сохранены!");
            tg.HapticFeedback.notificationOccurred('success');
        }

        async function saveNotifs() {
            const sig = document.getElementById('notif-signals').checked;
            const news = document.getElementById('notif-news').checked;
            const pos = document.getElementById('notif-pos').checked;
            await fetchAPI('save_notifs', 'POST', {signals: sig, news: news, pos_updates: pos});
            tg.showAlert("Уведомления обновлены!");
            tg.HapticFeedback.notificationOccurred('success');
        }

        function exportCSV() {
            if (userId) window.open(`/api/csv?uid=${userId}`, '_blank');
        }

        // Инициализация
        if (!userId) {
            document.body.innerHTML = '<div class="header">IXCLY</div><div class="card" style="margin:20px;">Пожалуйста, откройте приложение через Telegram.</div>';
        } else {
            loadPortfolio();
        }
    </script>
</body>
</html>
"""

@app.route("/")
def home(): return "IXCLY API is running!"
@app.route("/health")
def health(): return "ok"
@app.route('/miniapp')
def miniapp(): return render_template_string(MINI_APP_HTML)

# =====================================================================
# API ДЛЯ MINI APP
# =====================================================================
def run_async(coro):
    """Запускает корутину в главном Event Loop из потока Flask."""
    if main_event_loop is None: return None
    future = asyncio.run_coroutine_threadsafe(coro, main_event_loop)
    try: return future.result(timeout=30)
    except Exception as e: log.error(f"Async error: {e}"); return None

@app.route('/api/portfolio')
def api_portfolio():
    uid = request.args.get('uid')
    if not uid: return jsonify({"error": "No UID"}), 400
    try:
        s = get_user_risk_settings(int(uid))
        pos = get_user_open_positions(int(uid))
        risk = sum(p.get("risk_pct", s["risk_pct"]) for p in pos.values())
        return jsonify({"deposit": s["deposit"], "risk_pct": s["risk_pct"], "open_risk": risk, "positions": pos})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/close_pos', methods=['POST'])
def api_close_pos():
    data = request.json
    uid, coin = data.get('uid'), data.get('coin')
    if uid and coin:
        set_user_open_position(int(uid), coin, None)
        return jsonify({"status": "ok"})
    return jsonify({"error": "Bad request"}), 400

@app.route('/api/signals')
def api_signals():
    try:
        hist = load_signal_history()
        active = [x for x in hist if x.get("status") in ("active", "in_trade")]
        sigs = [{"coin": x["coin"], "side": x["side"], "strength": x.get("strength", 0), "entry": x.get("entry", 0), "tp1": x.get("tp1", 0)} for x in active[:20]]
        bias_txt = market_bias.get("impact", "Нейтрально")
        return jsonify({"signals": sigs, "bias_text": bias_txt})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/enter_pos', methods=['POST'])
def api_enter_pos():
    data = request.json
    uid, coin = data.get('uid'), data.get('coin')
    if not uid or not coin: return jsonify({"error": "Bad request"}), 400
    
    res = get_user_res(int(uid), coin)
    if not res:
        return jsonify({"error": "Анализ устарел. Откройте бота и проверьте монету через поиск."})
    
    # Проверка корреляции
    for op_coin, op_data in get_user_open_positions(int(uid)).items():
        if op_coin != coin and op_data.get("side") == res["side"]:
            for group in CORRELATION_GROUPS:
                if op_coin in group and coin in group:
                    return jsonify({"error": f"Корреляция: у вас уже есть {op_data['side']} по {op_coin}"})
    
    set_user_open_position(int(uid), coin, {"side": res["side"], "entry_price": res["entry"], "created_at": time.time(), "entered_at": time.time(), "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1], "risk_pct": get_user_risk_settings(int(uid))["risk_pct"], "notified_close": False, "notified_tp": False, "waiting_entry": True})
    return jsonify({"status": "ok"})

@app.route('/api/news')
def api_news():
    try:
        # Заглушка для новостей, так как это асинхронная функция
        # В реальности тут нужно кеширование, чтобы не ждать ИИ каждый раз
        text = market_bias.get("summary", "") + "<br><br>" + market_bias.get("impact", "")
        if not text.strip(): text = "Нет свежих новостей. Нажмите обновить."
        return jsonify({"text": f"<b>{market_bias.get('title_ru', 'Новости')}</b><br><br>{text}"})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/settings')
def api_settings():
    uid = request.args.get('uid')
    if not uid: return jsonify({"error": "No UID"}), 400
    u = _users().get(str(uid), {})
    return jsonify({
        "deposit": float(u.get("deposit", DEPOSIT)),
        "risk_pct": float(u.get("risk_pct", RISK_PCT)),
        "mode": u.get("trade_mode", "balanced"),
        "notifs": get_user_notif_settings(int(uid))
    })

@app.route('/api/save_settings', methods=['POST'])
def api_save_settings():
    data = request.json
    uid, dep, risk, mode = data.get('uid'), data.get('deposit'), data.get('risk_pct'), data.get('mode')
    if uid:
        conf_map = {"aggressive": 60, "balanced": 66, "conservative": 80}
        set_user_field(int(uid), deposit=dep, risk_pct=risk, trade_mode=mode, conf_final=conf_map.get(mode, 66))
        return jsonify({"status": "ok"})
    return jsonify({"error": "Bad request"}), 400

@app.route('/api/save_notifs', methods=['POST'])
def api_save_notifs():
    data = request.json
    uid = data.get('uid')
    if uid:
        set_user_field(int(uid), notify_signals=data.get('signals'), notify_news=data.get('news'), notify_pos_updates=data.get('pos_updates'))
        return jsonify({"status": "ok"})
    return jsonify({"error": "Bad request"}), 400

@app.route('/api/csv')
def api_csv():
    # Возврат CSV файла
    from flask import Response
    csv_data = generate_csv_history()
    return Response(csv_data, mimetype="text/csv", headers={"Content-Disposition": "attachment;filename=signals.csv"})

def generate_csv_history():
    with STATE_LOCK: hist = list(STATE["signal_history"])
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "TP2", "TP3", "SL1", "Статус", "Макс TP", "Уверенность"])
    for x in hist:
        w.writerow([datetime.fromtimestamp(x.get("ts", 0)).strftime("%Y-%m-%d %H:%M"), x.get("coin"), x.get("type"), x.get("side"), x.get("entry"), x.get("tp1"), x.get("tp2"), x.get("tp3"), x.get("sl1"), x.get("status"), x.get("max_tp_hit", 0), x.get("strength")])
    return out.getvalue().encode("utf-8-sig")

def _rss_mb():
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    except: return None

def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True, use_reloader=False)
    except Exception as e: log.error(f"Flask: {e}")

_web_started = False
def keep_alive():
    global _web_started
    if not _web_started:
        _web_started = True
        Thread(target=_run_web, daemon=True).start()

# =====================================================================
# ИНДИКАТОРЫ И МАТЕМАТИКА
# =====================================================================
PATTERNS = {
    "bull_engulf": ("Бычье поглощение", 1, 12, "rev", "BULL ENGULF"), "bear_engulf": ("Медвежье поглощение", -1, 12, "rev", "BEAR ENGULF"),
    "hammer": ("Молот", 1, 10, "rev", "HAMMER"), "shooting_star": ("Падающая звезда", -1, 10, "rev", "SHOOT STAR"),
    "morning_star": ("Утренняя звезда", 1, 15, "rev", "MORNING STAR"), "evening_star": ("Вечерняя звезда", -1, 15, "rev", "EVENING STAR"),
    "three_white": ("Три белых солдата", 1, 12, "cont", "3 WHITE"), "three_black": ("Три чёрные вороны", -1, 12, "cont", "3 BLACK"),
    "piercing": ("Просвет в облаках", 1, 8, "rev", "PIERCING"), "dark_cloud": ("Завеса из тёмных облаков", -1, 8, "rev", "DARK CLOUD"),
    "bull_harami": ("Бычье харами", 1, 5, "rev", "BULL HARAMI"), "bear_harami": ("Медвежье харами", -1, 5, "rev", "BEAR HARAMI"),
    "tweezer_bottom": ("Пинцет (дно)", 1, 6, "rev", "TWEEZER BOT"), "tweezer_top": ("Пинцет (вершина)", -1, 6, "rev", "TWEEZER TOP"),
    "marubozu_bull": ("Бычий марубозу", 1, 6, "cont", "MARUBOZU+"), "marubozu_bear": ("Медвежий марубозу", -1, 6, "cont", "MARUBOZU-"),
    "doji": ("Доджи (неопределённость)", 0, 0, "neutral", "DOJI"),
}

def _wilder(s, n): return s.ewm(alpha=1.0 / n, adjust=False).mean()
def _supertrend(h, l, c, atr, mult=3.0):
    hl2 = ((h + l) / 2.0).values
    a = atr.values
    upper, lower = hl2 + mult * a, hl2 - mult * a
    close = c.values
    n = len(close)
    fu, fl = upper.copy(), lower.copy()
    direction = np.ones(n)
    for i in range(1, n):
        fu[i] = upper[i] if (upper[i] < fu[i - 1] or close[i - 1] > fu[i - 1]) else fu[i - 1]
        fl[i] = lower[i] if (lower[i] > fl[i - 1] or close[i - 1] < fl[i - 1]) else fl[i - 1]
        if direction[i - 1] > 0: direction[i] = -1 if close[i] < fl[i] else 1
        else: direction[i] = 1 if close[i] > fu[i] else -1
    return direction

def add_patterns(df):
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    body = (c - o).abs()
    rng = (h - l).replace(0, np.nan)
    upper = h - np.maximum(o, c)
    lower = np.minimum(o, c) - l
    avg_body = body.rolling(10, min_periods=3).mean().fillna(body)
    bull, bear = c > o, c < o
    def sh(s, k=1): return s.shift(k, fill_value=False)
    p = {}
    p["doji"] = body <= 0.1 * rng
    p["hammer"] = (lower >= 2 * body) & (upper <= 0.15 * rng) & (body > 0.05 * rng)
    p["shooting_star"] = (upper >= 2 * body) & (lower <= 0.15 * rng) & (body > 0.05 * rng)
    p["bull_engulf"] = sh(bear) & bull & (o <= c.shift()) & (c >= o.shift()) & (body > body.shift())
    p["bear_engulf"] = sh(bull) & bear & (o >= c.shift()) & (c <= o.shift()) & (body > body.shift())
    p["morning_star"] = (sh(bear, 2) & (body.shift(2) > avg_body.shift(2)) & (body.shift(1) < 0.5 * avg_body.shift(1)) & bull & (c > (o.shift(2) + c.shift(2)) / 2))
    p["evening_star"] = (sh(bull, 2) & (body.shift(2) > avg_body.shift(2)) & (body.shift(1) < 0.5 * avg_body.shift(1)) & bear & (c < (o.shift(2) + c.shift(2)) / 2))
    p["three_white"] = (bull & sh(bull) & sh(bull, 2) & (c > c.shift()) & (c.shift() > c.shift(2)) & (o > o.shift()) & (o.shift() > o.shift(2)) & (o < c.shift()) & (o.shift() < c.shift(2)) & (body > 0.6 * avg_body))
    p["three_black"] = (bear & sh(bear) & sh(bear, 2) & (c < c.shift()) & (c.shift() < c.shift(2)) & (o < o.shift()) & (o.shift() < o.shift(2)) & (o > c.shift()) & (o.shift() > c.shift(2)) & (body > 0.6 * avg_body))
    p["piercing"] = sh(bear) & bull & (o < c.shift()) & (c > (o.shift() + c.shift()) / 2) & (c < o.shift())
    p["dark_cloud"] = sh(bull) & bear & (o > c.shift()) & (c < (o.shift() + c.shift()) / 2) & (c > o.shift())
    p["bull_harami"] = sh(bear) & bull & (o > c.shift()) & (c < o.shift()) & (body < 0.6 * body.shift())
    p["bear_harami"] = sh(bull) & bear & (o < c.shift()) & (c > o.shift()) & (body < 0.6 * body.shift())
    tol = 0.1 * df["atr"]
    p["tweezer_bottom"] = sh(bear) & bull & ((l - l.shift()).abs() <= tol)
    p["tweezer_top"] = sh(bull) & bear & ((h - h.shift()).abs() <= tol)
    p["marubozu_bull"] = bull & ((upper + lower) <= 0.1 * rng) & (body >= 1.3 * avg_body)
    p["marubozu_bear"] = bear & ((upper + lower) <= 0.1 * rng) & (body >= 1.3 * avg_body)
    for name, s in p.items(): df[f"pt_{name}"] = s.fillna(False).astype(bool)
    return df

def pattern_scores(df):
    c, ema20, rsi, bbp = df["close"], df["ema20"], df["rsi"], df["bb_pct"]
    down_ctx = ((c < ema20) | (rsi < 45) | (bbp < 0.35)).values
    up_ctx = ((c > ema20) | (rsi > 55) | (bbp > 0.65)).values
    trend_up = (df["ema20"] > df["ema50"]).values
    score = np.zeros(len(df))
    for name, (_ru, side, wgt, kind, _en) in PATTERNS.items():
        if side == 0: continue
        flag = df[f"pt_{name}"].values.astype(float)
        if kind == "rev": ctx = np.where(down_ctx if side > 0 else up_ctx, 1.0, 0.4)
        else: ctx = np.where(trend_up if side > 0 else ~trend_up, 1.0, 0.5)
        score += side * wgt * flag * ctx
    return np.clip(score, -25, 25)

def add_indicators(df):
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    for n in (9, 20, 50, 200): df[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()
    delta = c.diff()
    ag, al = _wilder(delta.clip(lower=0), 14), _wilder(-delta.clip(upper=0), 14)
    rsi = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    df["rsi"] = rsi.where(al != 0, 100.0).fillna(50.0)
    rmin, rmax = df["rsi"].rolling(14).min(), df["rsi"].rolling(14).max()
    df["stoch_k"] = ((df["rsi"] - rmin) / (rmax - rmin).replace(0, np.nan)).rolling(3).mean().fillna(0.5)
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["macd"], df["macd_sig"] = macd, macd.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_sig"]
    prev = c.shift()
    tr = pd.concat([h - l, (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    df["atr"] = _wilder(tr, 14).fillna(tr.mean())
    up, down = h.diff(), -l.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    atr_safe = df["atr"].replace(0, np.nan)
    df["plus_di"] = (100 * _wilder(plus_dm, 14) / atr_safe).fillna(0)
    df["minus_di"] = (100 * _wilder(minus_dm, 14) / atr_safe).fillna(0)
    dx = 100 * (df["plus_di"] - df["minus_di"]).abs() / (df["plus_di"] + df["minus_di"]).replace(0, np.nan)
    df["adx"] = _wilder(dx.fillna(0), 14)
    mid, std = c.rolling(20).mean(), c.rolling(20).std()
    upper_b, lower_b = mid + 2 * std, mid - 2 * std
    df["bb_pct"] = ((c - lower_b) / (upper_b - lower_b).replace(0, np.nan)).fillna(0.5)
    bw = (upper_b - lower_b) / mid.replace(0, np.nan)
    df["squeeze"] = (bw <= bw.rolling(100, min_periods=30).quantile(0.15)).fillna(False)
    df["st_dir"] = _supertrend(h, l, c, _wilder(tr, 10).fillna(tr.mean()), 3.0)
    tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
    span_a = ((tenkan + kijun) / 2).shift(26)
    span_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
    top, bot = np.maximum(span_a, span_b), np.minimum(span_a, span_b)
    df["tenkan"], df["kijun"] = tenkan.fillna(c), kijun.fillna(c)
    df["ichi_pos"] = np.where(top.isna(), 0, np.where(c > top, 1, np.where(c < bot, -1, 0)))
    tp = (h + l + c) / 3
    df["vwap"] = ((tp * v).rolling(20).sum() / v.rolling(20).sum().replace(0, np.nan)).fillna(c)
    raw = tp * v
    pos_f, neg_f = raw.where(tp > tp.shift(), 0.0), raw.where(tp < tp.shift(), 0.0)
    df["mfi"] = (100 - 100 / (1 + pos_f.rolling(14).sum() / neg_f.rolling(14).sum().replace(0, np.nan))).fillna(50)
    std_dev = tp.rolling(20).std().replace(0, np.nan)
    df["cci"] = ((tp - tp.rolling(20).mean()) / (0.015 * std_dev)).fillna(0)
    hh, ll = h.rolling(14).max(), l.rolling(14).min()
    df["wr"] = (-100 * (hh - c) / (hh - ll).replace(0, np.nan)).fillna(-50)
    df["obv"] = (np.sign(c.diff()) * v).fillna(0).cumsum()
    df["obv_ema"] = df["obv"].ewm(span=20, adjust=False).mean()
    df["rel_vol"] = (v / v.rolling(20).mean().replace(0, np.nan)).fillna(1.0)
    df["vol_delta"] = ((c - o) / (h - l).replace(0, np.nan) * v).fillna(0)
    df["z_score"] = ((c - df["ema50"]) / c.rolling(50).std().replace(0, np.nan)).fillna(0)
    df["momentum"] = c.pct_change(10).fillna(0)
    df["brk_up"] = (c > h.shift(1).rolling(20).max()).fillna(False)
    df["brk_dn"] = (c < l.shift(1).rolling(20).min()).fillna(False)
    low_min, high_max = l.shift(1).rolling(14).min(), h.shift(1).rolling(14).max()
    rsi_min, rsi_max = df["rsi"].shift(1).rolling(14).min(), df["rsi"].shift(1).rolling(14).max()
    df["div_bull"] = ((l <= low_min) & (df["rsi"] > rsi_min + 3)).fillna(False)
    df["div_bear"] = ((h >= high_max) & (df["rsi"] < rsi_max - 3)).fillna(False)
    return add_patterns(df)

def add_scores(df):
    n = len(df)
    c, adx = df["close"].values, df["adx"].values
    trending, ranging = adx >= 25, adx < 20
    def mix(t, r): return np.where(trending, t, np.where(ranging, r, (t + r) / 2.0))
    ema20, ema50, ema200 = df["ema20"].values, df["ema50"].values, df["ema200"].values
    hist = df["macd_hist"].values
    hist_prev = np.concatenate([[hist[0]], hist[:-1]])
    stack = np.where((c > ema20) & (ema20 > ema50), 20, np.where((c < ema20) & (ema20 < ema50), -20, 0))
    stack = stack + np.where(c > ema200, 10, -10)
    st = np.where(df["st_dir"].values > 0, 20, -20)
    ichi = df["ichi_pos"].values * 15 + np.where(df["tenkan"].values > df["kijun"].values, 5, -5)
    macd_s = np.where(hist > 0, 8, -8) + np.where(hist > hist_prev, 4, -4)
    di = np.where(adx >= 20, np.where(df["plus_di"].values > df["minus_di"].values, 10, -10), 0)
    trend = np.clip(stack + st + ichi + macd_s + di, -100, 100)
    rsi, sk, cci = df["rsi"].values, df["stoch_k"].values, df["cci"].values
    wr, mfi, bbp = df["wr"].values, df["mfi"].values, df["bb_pct"].values
    z, roc = df["z_score"].values, df["momentum"].values
    zeros = np.zeros(n)
    mom = np.clip(mix(np.clip((rsi - 50) * 0.8, -20, 20), np.clip((50 - rsi) * 0.9, -25, 25)) + mix((sk - 0.5) * 30, (0.5 - sk) * 40) + mix(np.clip(cci / 10, -15, 15), np.clip(-cci / 8, -20, 20)) + mix(((wr + 50) / 50) * 8, -((wr + 50) / 50) * 12) + mix((mfi - 50) / 50 * 8, (50 - mfi) / 50 * 12) + mix(np.where(bbp > 1, 10, np.where(bbp < 0, -10, (bbp - 0.5) * 10)), np.clip((0.5 - bbp) * 40, -20, 20)) + mix(zeros, np.clip(-z * 7, -15, 15)) + mix(np.clip(roc * 400, -10, 10), zeros), -100, 100)
    o = df["open"].values
    pat = pattern_scores(df)
    vol_conf = np.where(df["rel_vol"].values > 1.5, np.where(c > o, 10, -10), 0)
    obv_s = np.where(df["obv"].values > df["obv_ema"].values, 8, -8)
    div = np.where(df["div_bull"].values, 15, 0) - np.where(df["div_bear"].values, 15, 0)
    brk = np.where(df["brk_up"].values, 10, 0) - np.where(df["brk_dn"].values, 10, 0)
    vwap_s = np.where(c > df["vwap"].values, 5, -5)
    pa = np.clip((pat + vol_conf + obv_s + div + brk + vwap_s) * 1.3, -100, 100)
    w_t = np.where(trending, 0.5, np.where(ranging, 0.25, 0.4))
    w_m = np.where(trending, 0.2, np.where(ranging, 0.45, 0.3))
    score = w_t * trend + w_m * mom + (1 - w_t - w_m) * pa
    df["long_p"] = np.nan_to_num(np.clip(50 + score / 2, 5, 95), nan=50.0)
    return df

def explain_last(df):
    r = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else r
    out = []
    def add(w, side, text): out.append({"w": w, "side": side, "text": text})
    c = r["close"]
    add(20, 1 if r["st_dir"] > 0 else -1, "Supertrend вверх" if r["st_dir"] > 0 else "Supertrend вниз")
    if c > r["ema20"] > r["ema50"] > r["ema200"]: add(22, 1, "Цена выше EMA 20/50/200")
    elif c < r["ema20"] < r["ema50"] < r["ema200"]: add(22, -1, "Цена ниже EMA 20/50/200")
    if r["ichi_pos"] > 0: add(16, 1, "Цена выше облака Ichimoku")
    elif r["ichi_pos"] < 0: add(16, -1, "Цена ниже облака Ichimoku")
    if r["adx"] >= 25: add(15, 1 if r["plus_di"] > r["minus_di"] else -1, f"Сильный тренд (ADX {r['adx']:.0f})")
    if r["macd_hist"] > 0 and r["macd_hist"] > prev["macd_hist"]: add(10, 1, "MACD: бычий импульс растёт")
    elif r["macd_hist"] < 0 and r["macd_hist"] < prev["macd_hist"]: add(10, -1, "MACD: медвежий импульс растёт")
    if r["rsi"] < 30: add(14, 1, f"RSI перепродан ({r['rsi']:.0f})")
    elif r["rsi"] > 70: add(14, -1, f"RSI перекуплен ({r['rsi']:.0f})")
    if r["div_bull"]: add(18, 1, "Бычья дивергенция RSI")
    if r["div_bear"]: add(18, -1, "Медвежья дивергенция RSI")
    if r["bb_pct"] < 0: add(12, 1, "Цена под нижней полосой Боллинджера")
    elif r["bb_pct"] > 1: add(12, -1, "Цена над верхней полосой Боллинджера")
    if r["rel_vol"] > 1.5: add(12, 1 if c > r["open"] else -1, f"Всплеск объёма ×{r['rel_vol']:.1f}")
    if r["brk_up"]: add(14, 1, "Пробой 20-свечного максимума")
    if r["brk_dn"]: add(14, -1, "Пробой 20-свечного минимума")
    obv_up = r["obv"] > r["obv_ema"]
    add(6, 1 if obv_up else -1, "OBV выше средней (приток)" if obv_up else "OBV ниже средней (отток)")
    for name, (ru, side, wgt, _k, _en) in PATTERNS.items():
        if side != 0 and bool(r[f"pt_{name}"]): add(wgt + 8, side, ru)
    return out

def last_patterns(df, depth=2):
    res = []
    for i in range(1, min(depth, len(df)) + 1):
        r = df.iloc[-i]
        for name, (ru, side, _w, _k, en) in PATTERNS.items():
            if bool(r[f"pt_{name}"]): res.append((ru, side, en))
    seen, out = set(), []
    for item in res:
        if item[0] not in seen: seen.add(item[0]); out.append(item)
    return out

def support_resistance(df, price, k=3, lookback=120):
    hv, lv = df["high"].values[-lookback:], df["low"].values[-lookback:]
    res, sup = [], []
    for i in range(k, len(hv) - k):
        if hv[i] == hv[i - k:i + k + 1].max(): res.append(float(hv[i]))
        if lv[i] == lv[i - k:i + k + 1].min(): sup.append(float(lv[i]))
    below, above = [s for s in sup if s < price], [r for r in res if r > price]
    return {"support": max(below) if below else None, "resistance": min(above) if above else None}

def rows_to_df(rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    df = df.drop_duplicates("ts").sort_values("ts")
    df = df[df["close"] > 0].reset_index(drop=True)
    if len(df) < MIN_CANDLES: raise ValueError("мало свечей")
    return df

def prepare_df(df): return add_scores(add_indicators(df))
def build_tf_data(rows_by_tf):
    out = {}
    for tf, rows in rows_by_tf.items():
        price = float(rows[-1][4])
        df = prepare_df(rows_to_df(rows[:-1]))
        last = df.iloc[-1]
        long_p, adx = float(last["long_p"]), float(last["adx"])
        out[tf] = {"df": df, "price": price, "ema20": float(last["ema20"]), "ema50": float(last["ema50"]), "ema200": float(last["ema200"]), "rsi": float(last["rsi"]), "atr": float(last["atr"]), "adx": adx, "macd_hist": float(last["macd_hist"]), "bb_pct": float(last["bb_pct"]), "recent_low": float(min(df["low"].tail(28).min(), price)), "recent_high": float(max(df["high"].tail(28).max(), price)), "long_prob": long_p, "confidence": max(long_p, 100 - long_p), "qv24": float((df["close"] * df["volume"]).tail(6).sum()), "regime": "тренд" if adx >= 25 else "флэт" if adx < 20 else "переход", "squeeze": bool(last["squeeze"]), "reasons": explain_last(df), "patterns": last_patterns(df), "sr": support_resistance(df, price)}
    return out

# =====================================================================
# БИРЖИ И API
# =====================================================================
def _p_binance(j): return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]
def _p_okx(j): return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["data"]]
def _p_bybit(j): return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["result"]["list"]]
def _p_kucoin(j): return [[float(x[0]) * 1000, float(x[1]), float(x[3]), float(x[4]), float(x[2]), float(x[5])] for x in j["data"]]
def _p_gate(j): return [[float(x[0]) * 1000, float(x[5]), float(x[3]), float(x[4]), float(x[2]), float(x[6])] for x in j]
def _p_bitget(j): return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["data"]]
def _p_htx(j): return [[float(x["id"]) * 1000, float(x["open"]), float(x["high"]), float(x["low"]), float(x["close"]), float(x["amount"])] for x in j["data"]]

EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines", "parse": _p_binance, "params": lambda c, tf, n: {"symbol": f"{c}USDT", "interval": tf, "limit": min(n, 1000)}},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline", "parse": _p_bybit, "params": lambda c, tf, n: {"category": "spot", "symbol": f"{c}USDT", "limit": min(n, 1000), "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf]}},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles", "parse": _p_okx, "params": lambda c, tf, n: {"instId": f"{c}-USDT", "limit": str(min(n, 300)), "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf]}},
    {"name": "MEXC", "url": "https://api.mexc.com/api/v3/klines", "parse": _p_binance, "params": lambda c, tf, n: {"symbol": f"{c}USDT", "limit": min(n, 1000), "interval": {"15m": "15m", "1h": "60m", "4h": "4h", "1d": "1d"}[tf]}},
    {"name": "KUCOIN", "url": "https://api.kucoin.com/api/v1/market/candles", "parse": _p_kucoin, "params": lambda c, tf, n: {"symbol": f"{c}-USDT", "type": {"15m": "15min", "1h": "1hour", "4h": "4hour", "1d": "1day"}[tf]}},
    {"name": "GATE", "url": "https://api.gateio.ws/api/v4/spot/candlesticks", "parse": _p_gate, "params": lambda c, tf, n: {"currency_pair": f"{c}_USDT", "limit": min(n, 1000), "interval": tf}},
    {"name": "BITGET", "url": "https://api.bitget.com/api/v2/spot/market/candles", "parse": _p_bitget, "params": lambda c, tf, n: {"symbol": f"{c}USDT", "limit": min(n, 1000), "granularity": {"15m": "15min", "1h": "1h", "4h": "4h", "1d": "1day"}[tf]}},
    {"name": "HTX", "url": "https://api.huobi.pro/market/history/kline", "parse": _p_htx, "params": lambda c, tf, n: {"symbol": f"{c.lower()}usdt", "size": min(n, 2000), "period": {"15m": "15min", "1h": "60min", "4h": "4hour", "1d": "1day"}[tf]}},
]

async def _fetch_klines(ex, coin, tf, limit=200, min_rows=None):
    j = await http_json(ex["url"], params=ex["params"](coin, tf, limit), timeout=9)
    rows = ex["parse"](j)
    rows.sort(key=lambda r: r[0])
    if len(rows) < (min_rows or MIN_CANDLES + 1): raise ValueError(f"мало свечей")
    return rows

def _note_error(coin, ex_name, err): fetch_errors.setdefault(coin, []).append(f"{ex_name}: {str(err)[:40]}")
async def _try_exchange(ex, cand, tfs, coin=None):
    if time.time() < exchange_bans.get(ex["name"], 0): return None
    coin = coin or cand
    try: first = await _fetch_klines(ex, cand, tfs[0])
    except Exception as e:
        _note_error(coin, ex["name"], e)
        if any(code in str(e) for code in ("429", "418", "403", "451")): exchange_bans[ex["name"]] = time.time() + 300
        return None
    results = [first]
    if len(tfs) > 1:
        rest = await asyncio.gather(*[_fetch_klines(ex, cand, tf) for tf in tfs[1:]], return_exceptions=True)
        for r in rest:
            if isinstance(r, Exception): return None
            results.append(r)
    return await asyncio.to_thread(build_tf_data, dict(zip(tfs, results))), ex["name"]

async def get_market_data(coin, tfs=tuple(TFS)):
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]):
            res = await _try_exchange(ex, cand, tfs, coin)
            if res: return res
    raise LookupError(coin)

async def get_market_data_light(coin, tfs=tuple(TFS)):
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex:
            res = await _try_exchange(ex, pref[0], tfs, coin)
            if res: return res
            preferred_source.pop(coin, None)
    return await get_market_data(coin, tfs)

async def fetch_history(coin, tf="4h", limit=1000, min_rows=300):
    order, seen = [], set()
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]): order.append((cand, ex))
    for cand, ex in order:
        if (cand, ex["name"]) in seen: continue
        seen.add((cand, ex["name"]))
        try: return await _fetch_klines(ex, cand, tf, limit, min_rows=min_rows)
        except Exception: continue
    return None

async def get_simple_price(coin):
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        for url, params, path in (
            ("https://data-api.binance.vision/api/v3/ticker/price", {"symbol": f"{cand}USDT"}, lambda j: j["price"]),
            ("https://api.bybit.com/v5/market/tickers", {"category": "spot", "symbol": f"{cand}USDT"}, lambda j: j["result"]["list"][0]["lastPrice"]),
            ("https://www.okx.com/api/v5/market/ticker", {"instId": f"{cand}-USDT"}, lambda j: j["data"][0]["last"]),
        ):
            try: return float(path(await http_json(url, params=params, timeout=5)))
            except Exception: continue
    return None

_funding_cache = {}
async def get_funding(coin):
    c = _funding_cache.get(coin)
    if c and time.time() - c[0] < 600: return c[1]
    val = None
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        try:
            j = await http_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": f"{cand}USDT"}, timeout=5)
            val = float(j["result"]["list"][0]["fundingRate"])
            break
        except Exception: continue
    _funding_cache[coin] = (time.time(), val)
    return val

async def get_open_interest_change(coin):
    try:
        url = "https://fapi.binance.com/futures/data/openInterestHist"
        params = {"symbol": f"{coin}USDT", "period": "15m", "limit": 2}
        async with http.get(url, params=params, timeout=5) as r:
            if r.status != 200: return 0.0
            data = await r.json(content_type=None)
            if len(data) < 2: return 0.0
            return round(((float(data[1]["sumOpenInterest"]) - float(data[0]["sumOpenInterest"])) / float(data[0]["sumOpenInterest"])) * 100, 2)
    except Exception: return 0.0

_ctx_cache = {"ts": 0.0, "data": {}}
async def get_context(force=False):
    if not force and time.time() - _ctx_cache["ts"] < 600 and _ctx_cache["data"]: return _ctx_cache["data"]
    ctx = {"btc_dir": 0, "btc_conf": 50.0, "btc_txt": "нет данных", "fng": None, "fng_txt": ""}
    try:
        tf, _ = await get_market_data_light("BTC", ("4h", "1d"))
        p = (tf["4h"]["long_prob"] + tf["1d"]["long_prob"]) / 2
        ctx["btc_dir"] = 1 if p >= 55 else -1 if p <= 45 else 0
        ctx["btc_conf"] = max(p, 100 - p)
        ctx["btc_txt"] = "бычий ▲" if ctx["btc_dir"] == 1 else "медвежий ▼" if ctx["btc_dir"] == -1 else "флэт ▬"
    except Exception: pass
    try:
        j = await http_json("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
        ctx["fng"] = int(j["data"][0]["value"])
        ctx["fng_txt"] = str(j["data"][0].get("value_classification", ""))
    except Exception: pass
    _ctx_cache.update(ts=time.time(), data=ctx)
    return ctx

# =====================================================================
# СИГНАЛЫ И БЭКТЕСТ
# =====================================================================
TF_WEIGHTS = {"1d": 0.30, "4h": 0.30, "1h": 0.25, "15m": 0.15}
def quick_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data: return None
    wp = (tf_data["4h"]["long_prob"] + tf_data["1d"]["long_prob"]) / 2
    strength = max(wp, 100 - wp)
    if strength < CONF_STAGE1 or tf_data["4h"]["adx"] < 10: return None
    return wp >= 50, strength

def collect_reasons(tf_data, is_long, tfs=("1d", "4h", "1h"), limit=5):
    side = 1 if is_long else -1
    bucket = {}
    for tf in tfs:
        for rsn in tf_data.get(tf, {}).get("reasons", []):
            if rsn["side"] == side:
                b = bucket.setdefault(rsn["text"], {"w": 0, "tfs": []})
                b["w"] += rsn["w"]
                b["tfs"].append(tf)
    items = sorted(bucket.items(), key=lambda kv: kv[1]["w"], reverse=True)[:limit]
    return [f"{t} ({', '.join(b['tfs'])})" for t, b in items]

async def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0.0, conf_threshold=CONF_FINAL):
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрок", "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": []}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data: out["reject"] = "Мало данных"; return out
    wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / sum(TF_WEIGHTS[t] for t in tfs)
    is_long = wp >= 50
    side = 1 if is_long else -1
    strength = max(wp, 100 - wp)
    notes = []
    def cs(p): return p if is_long else 100 - p
    p4, p1d = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]
    info = tf_data["4h"]
    reject = ""
    if (p4 >= 50) != is_long or (p1d >= 50) != is_long: reject = "4h и 1d смотрят в разные стороны"
    agree = sum(1 for t in tfs if cs(tf_data[t]["long_prob"]) >= 55)
    if not reject and info["adx"] < 15: reject = f"Вялый рынок (ADX {info['adx']:.0f})"
    if not reject and agree < min(3, len(tfs)): reject = f"Таймфреймы не согласны ({agree}/{len(tfs)})"
    if "1h" in tf_data and (tf_data["1h"]["long_prob"] >= 50) != is_long and tf_data["1h"]["confidence"] >= 65: strength -= 8; notes.append("1h против (−8)")
    if funding is not None:
        if is_long and funding > 0.0005: strength -= 5; notes.append(f"funding {funding*100:.3f}% (−5)")
        elif (not is_long) and funding < -0.0005: strength -= 5; notes.append(f"funding {funding*100:.3f}% (−5)")
    oi_change = await get_open_interest_change(coin)
    if is_long:
        if oi_change > 2.0: strength += 5; notes.append(f"OI +{oi_change}% (+5)")
        elif oi_change < -2.0: strength -= 5; notes.append(f"OI {oi_change}% (−5)")
    else:
        if oi_change > 2.0: strength += 5; notes.append(f"OI +{oi_change}% (+5)")
        elif oi_change < -2.0: strength -= 5; notes.append(f"OI {oi_change}% (−5)")
    strength = max(0.0, min(100.0, strength))
    if not reject and strength < conf_threshold: reject = f"Уверенность {strength:.0f}% < {conf_threshold:.0f}%"
    p15, p1h = tf_data.get("15m", {}).get("long_prob", 50), tf_data.get("1h", {}).get("long_prob", 50)
    fast = "15m" in tf_data and "1h" in tf_data and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    out.update(valid=not reject, reject=reject, notes=notes, is_long=is_long, strength=round(strength, 1), type="Краткосрочный" if fast else "Долгосрочный" if long_term else "Среднесрочный", tf_probs={t: tf_data[t]["long_prob"] for t in tfs}, reasons=collect_reasons(tf_data, is_long))
    return out

def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    if is_long:
        entry = min(price, max(ema20, ema50))
        if entry > price - 0.2 * atr: entry = price - 0.35 * atr
        sl1 = max(min(rec_low - 0.5 * atr, entry - 0.8 * atr), entry - 2.5 * atr)
        return entry, sl1, entry - sl1
    entry = max(price, min(ema20, ema50))
    if entry < price + 0.2 * atr: entry = price + 0.35 * atr
    sl1 = min(max(rec_high + 0.5 * atr, entry + 0.8 * atr), entry + 2.5 * atr)
    return entry, sl1, sl1 - entry

def calc_levels_and_risk(info, is_long, sig_type="Среднесрочный"):
    price = info["price"]
    atr = info["atr"] if info["atr"] > 0 else price * 0.012
    sr = info.get("sr", {})
    entry, sl1, risk = levels_core(price, atr, info["ema20"], info["ema50"], info["recent_low"], info["recent_high"], is_long, sr.get("support"), sr.get("resistance"))
    sign = 1 if is_long else -1
    tps = [entry + sign * risk * m for m in (1.5, 2.5, 4.0)]
    sl2 = sl1 - sign * 0.7 * atr
    atr_pct = (atr / price) * 100
    leverage = "x3-x5 ⚠" if atr_pct > 3.0 else "x5-x10" if atr_pct > 1.5 else "x10-x20"
    liq_x20 = entry * (1 - 0.0475) if is_long else entry * (1 + 0.0475)
    return entry, tps, [sl1, sl2], risk, leverage, None, liq_x20

def backtest_df(df, conf_min=CONF_FINAL, rr1=1.5, fill_bars=6, max_hold=30, fee=0.001):
    n = len(df)
    if n < 150: return None
    lp, adx, atr = df["long_p"].values, df["adx"].values, df["atr"].values
    c, h, l = df["close"].values, df["high"].values, df["low"].values
    e20, e50 = df["ema20"].values, df["ema50"].values
    low28, high28 = df["low"].rolling(28).min().values, df["high"].rolling(28).max().values
    results, i = [], 80
    while i < n - 2:
        p = lp[i]
        if max(p, 100 - p) < conf_min or adx[i] < 15 or atr[i] <= 0 or np.isnan(low28[i]): i += 1; continue
        is_long = p >= 50
        entry, sl, risk = levels_core(c[i], atr[i], e20[i], e50[i], low28[i], high28[i], is_long)
        if risk <= 0: i += 1; continue
        tp = entry + rr1 * risk if is_long else entry - rr1 * risk
        filled = next((j for j in range(i + 1, min(n, i + 1 + fill_bars)) if (is_long and l[j] <= entry) or ((not is_long) and h[j] >= entry)), None)
        if filled is None: i += 1; continue
        res, last_k = None, filled
        for k in range(filled, min(n, filled + max_hold)):
            last_k = k
            if (l[k] <= sl) if is_long else (h[k] >= sl): res = -1.0; break
            if (h[k] >= tp) if is_long else (l[k] <= tp): res = rr1; break
        if res is None: res = max(-1.0, min(rr1, (((c[last_k] - entry) if is_long else (entry - c[last_k])) / risk)))
        results.append(res - fee * entry / risk)
        i = last_k + 1
    days = round(n * 4 / 24)
    if not results: return {"trades": 0, "winrate": 0.0, "avg_r": 0.0, "pf": 0.0, "days": days}
    arr = np.array(results)
    wins, losses = arr[arr > 0], arr[arr <= 0]
    pf = float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else 9.9
    return {"trades": len(arr), "winrate": round(float((arr > 0).mean() * 100), 1), "avg_r": round(float(arr.mean()), 2), "pf": round(min(pf, 9.9), 2), "days": days}

def run_backtest_rows(rows): return backtest_df(prepare_df(rows_to_df(rows[:-1])))
async def get_backtest(coin):
    c = bt_cache.get(coin)
    if c and time.time() - c[0] < 6 * 3600: return c[1]
    bt = None
    try:
        rows = await fetch_history(coin, "4h", 1000)
        if rows: bt = await asyncio.to_thread(run_backtest_rows, rows)
    except Exception: pass
    bt_cache[coin] = (time.time(), bt)
    return bt

def bt_ok(bt): return (bool(bt) and bt["trades"] >= BT_MIN_TRADES and bt["avg_r"] >= BT_MIN_AVG_R and bt["winrate"] >= BT_MIN_WINRATE and bt["pf"] >= float(os.getenv("BT_MIN_PF", "1.15")))

# =====================================================================
# СКАНЕР И АНАЛИЗ
# =====================================================================
async def scan_once(send=True):
    if scan_lock.locked(): return []
    async with scan_lock:
        t0 = time.time()
        if await btc_shock():
            set_blackout(3600)
            await notify_admin("⚠ Шок BTC: сканер на паузе 1 час")
            return []
        ctx = await get_context(force=True)
        universe = await get_scan_universe()
        sem = asyncio.Semaphore(2)
        stage1 = []
        async def s1(coin):
            async with sem:
                try: tf2, _ = await get_market_data_light(coin, ("4h", "1d"))
                except Exception: return
                q = quick_check(tf2)
                if q: stage1.append((coin, tf2, q[1]))
                await asyncio.sleep(0.2)
        await asyncio.gather(*[s1(c) for c in universe])
        stage1 = sorted(stage1, key=lambda x: x[2], reverse=True)[:30]
        stage2 = []
        async def s2(item):
            coin, tf2, _ = item
            async with sem:
                tf_data = dict(tf2)
                try:
                    extra, _src = await get_market_data_light(coin, ("1h", "15m"))
                    tf_data.update(extra)
                except Exception: pass
                setup = await evaluate_setup(coin, tf_data, ctx, await get_funding(coin), scan=True, conf_threshold=CONF_FINAL + adaptive_conf_bonus())
                if setup["valid"]: stage2.append((coin, tf_data, setup))
        await asyncio.gather(*[s2(i) for i in stage1])
        final = []
        sem3 = asyncio.Semaphore(2)
        async def s3(item):
            coin, tf_data, setup = item
            async with sem3:
                bt = await get_backtest(coin)
                if bt is not None and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt): return
                try: res = await build_analysis(coin, tf_data, setup, bt, ctx, "scan")
                except Exception: return
                if res["verdict"] == "ENTER":
                    final.append(res)
                    analysis_cache[coin] = (time.time(), 1200, res)
                    save_analysis_db(coin, res)
        await asyncio.gather(*[s3(i) for i in stage2])
        final.sort(key=lambda r: r["strength"], reverse=True)
        last_scan_info.update(ts=int(time.time()), stage1=len(stage1), stage2=len(stage2), final=len(final), secs=int(time.time() - t0))
        state_touch(False)
        
        if send and active_chats:
            now = time.time()
            hist = load_signal_history()
            new_items = []
            for r in final:
                key = f"{r['coin']}:{r['side']}"
                if any(f"{x['coin']}:{x['side']}" == key for x in hist if x.get("status") in ("active", "in_trade")): continue
                if len(new_items) >= 3: continue
                signal_sent_at[key] = now
                new_items.append(r)
                hist.append({"ts": now, "coin": r["coin"], "side": r["side"], "type": r["type"], "strength": r["strength"], "entry": r["entry"], "tp1": r["tps"][0], "tp2": r["tps"][1], "tp3": r["tps"][2], "sl1": r["sls"][0], "sl2": r["sls"][1], "status": "active", "max_tp_hit": 0, "hit_sl": 0, "text": r["text"]})
            if new_items:
                save_signal_history(hist)
                stats["signals"] += len(new_items)
                for r in new_items:
                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Открыть в приложении", web_app=WebAppInfo(url=f"{os.getenv('RENDER_EXTERNAL_URL', 'https://твой-бот.onrender.com')}/miniapp"))]])
                    for cid in list(active_chats):
                        if not get_user_notif_settings(cid)["signals"]: continue
                        try:
                            await _send_new(cid, f"🚨 <b>Новый сигнал: {r['coin']} {r['side']}</b>\n\nВход: <code>{r['entry']}</code>\nTP1: <code>{r['tps'][0]}</code>\nSL: <code>{r['sls'][0]}</code>\n\n#сигнал", kb, r.get("img"))
                        except Exception: pass
        return final

async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    info = tf_data["4h"]
    is_long = setup["is_long"]
    entry, tps, sls, risk_dist, leverage, near, liq_x20 = calc_levels_and_risk(info, is_long, setup["type"])
    verdict = "ENTER" if setup["valid"] and setup["strength"] >= CONF_FINAL else "WAIT"
    try: img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception: img = None
    res = {"coin": coin, "ts": time.time(), "source": source, "img": img, "is_long": is_long, "side": "LONG" if is_long else "SHORT", "verdict": verdict, "type": setup["type"], "strength": setup["strength"], "regime": info["regime"], "price": info["price"], "entry": entry, "tps": tps, "sls": sls, "risk_dist": risk_dist, "leverage": leverage, "reasons": setup["reasons"], "patterns": setup["patterns"], "notes": setup["notes"], "bt": bt, "liq_x20": liq_x20}
    res["text"] = "Текст для бота (не используется в Mini App)"
    return res

async def analyze_coin(coin, uid=None, force=False):
    cached = analysis_cache.get(coin)
    if cached and not force and time.time() - cached[0] < cached[1]: return cached[2], None
    try:
        tf_data, source = await get_market_data(coin)
        ctx = await get_context()
        setup = await evaluate_setup(coin, tf_data, ctx, await get_funding(coin))
        bt = await get_backtest(coin)
        res = await build_analysis(coin, tf_data, setup, bt, ctx, source)
        analysis_cache[coin] = (time.time(), 180, res)
        if uid: user_res.setdefault(uid, {})[coin] = (time.time(), res)
        return res, None
    except Exception as e: return None, str(e)

def get_user_res(uid, coin):
    v = user_res.get(uid, {}).get(coin)
    return v[1] if v else None

async def get_scan_universe():
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
        qv = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    except Exception: return POPULAR_COINS[:SCAN_MAX_COINS]
    uni = []
    for c in POPULAR_COINS:
        for cand in SYMBOL_ALIASES.get(c, [c]):
            if qv.get(f"{cand}USDT", 0) >= MIN_QV24: uni.append((c, qv[f"{cand}USDT"])); break
    uni.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in uni[:SCAN_MAX_COINS]]

def recent_results(n=20):
    hist = sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True)
    return [x for x in hist if x.get("status") in ("win", "loss")][:n]
def adaptive_conf_bonus():
    r = recent_results(20)
    if len(r) < 8: return 0
    wr = sum(1 for x in r if x["status"] == "win") / len(r)
    return 10 if wr < 0.35 else 6 if wr < 0.45 else 0
def set_blackout(secs):
    global news_blackout_until
    news_blackout_until = time.time() + secs
async def btc_shock():
    try:
        tf, _ = await get_market_data_light("BTC", ("1h",))
        last = tf["1h"]["df"].iloc[-1]
        return (last["high"] - last["low"]) > 2.5 * last["atr"]
    except Exception: return False

# =====================================================================
# ГРАФИКИ И ТЕКСТ
# =====================================================================
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    info = tf_data[main_tf]
    df = info["df"].tail(60).reset_index(drop=True)
    price = info["price"]
    w, h = 900, 540
    img = Image.new("RGB", (w, h), (14, 15, 20))
    draw = ImageDraw.Draw(img)
    try: font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13); font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
    except: font = font_b = ImageFont.load_default()
    col = (0, 200, 90) if is_long else (230, 60, 60)
    draw.text((18, 10), f"{coin}/USDT  {main_tf}  {'LONG' if is_long else 'SHORT'}", fill=col, font=font_b)
    pad_l, pad_r, pad_t, pr_bottom, vol_top, vol_bottom = 62, 18, 42, 405, 425, 520
    cw, ch = w - pad_l - pad_r, pr_bottom - pad_t
    highs, lows = df["high"].values, df["low"].values
    levels = [entry, price] + list(tps) + list(sls)
    mn, mx = min(lows.min(), min(levels)) * 0.998, max(highs.max(), max(levels)) * 1.002
    rng = mx - mn or 1.0
    def y(p): return pad_t + ch - (p - mn) / rng * ch
    n = len(df)
    step = cw / n
    cwid = max(3, int(step * 0.7))
    for i in range(n):
        x = pad_l + i * step + step / 2
        o, c = df["open"].iloc[i], df["close"].iloc[i]
        color = (0, 170, 80) if c >= o else (205, 50, 50)
        draw.line([(x, y(highs[i])), (x, y(lows[i]))], fill=color, width=1)
        top, bot = min(y(o), y(c)), max(y(o), y(c))
        draw.rectangle([x - cwid / 2, top, x + cwid / 2, max(bot, top + 1)], fill=color)
    vmax = df["volume"].max() or 1.0
    for i in range(n):
        x = pad_l + i * step + step / 2
        vh = (vol_bottom - vol_top) * (df["volume"].iloc[i] / vmax)
        color = (0, 120, 60) if df["close"].iloc[i] >= df["open"].iloc[i] else (140, 40, 40)
        draw.rectangle([x - cwid / 2, vol_bottom - vh, x + cwid / 2, vol_bottom], fill=color)
    def lvl(p, label, color, dash=False):
        yy = y(p)
        if dash:
            for xx in range(pad_l, w - pad_r, 7): draw.line([(xx, yy), (xx + 3, yy)], fill=color, width=1)
        else: draw.line([(pad_l, yy), (w - pad_r, yy)], fill=color, width=2)
        draw.text((pad_l + 4, yy - 14), f"{label} {fmt(p)}", fill=color, font=font)
    lvl(price, "Price", (230, 230, 230), True)
    lvl(entry, "Entry", (255, 190, 0))
    for i, tp in enumerate(tps): lvl(tp, f"TP{i + 1}", (0, 190, 90), True)
    for i, sl in enumerate(sls): lvl(sl, f"SL{i + 1}", (230, 60, 60), True)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()

# =====================================================================
# AI АНАЛИТИКА
# =====================================================================
async def ask_ai_named(prompt, timeout=20, validate=None, free_timeout=30):
    # Упрощенная заглушка для Mini App (функция вызывается только для новостей)
    return None, None
async def run_ai_diag(): return "OK"
async def update_market_bias(force=False):
    # Заглушка, чтобы не спамить API новостями
    global market_bias
    market_bias = {"bias": 1, "title_ru": "Биткоин обновляет максимумы", "summary": "Рынок находится в бычьей фазе. Институционалы продолжают закупаться через ETF.", "impact": "Бычий фон. Рекомендуется искать лонги.", "coins": "BTC, ETH", "published": "Сегодня", "link": "", "engine": "fallback", "ts": time.time()}
async def news_watcher_loop():
    # Отключено для Mini App версии, чтобы не спамить
    pass

# =====================================================================
# МОНИТОРИНГ ПОЗИЦИЙ (ОТПРАВКА В ЧАТ)
# =====================================================================
async def position_monitor_loop():
    await asyncio.sleep(50)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    if pos.get("notified_close"): continue
                    price = await get_simple_price(coin)
                    if not price: continue
                    side, entry = pos["side"], pos["entry_price"]
                    sign = 1 if side == "LONG" else -1
                    
                    # Лимитка сработала
                    if pos.get("waiting_entry"):
                        if (sign * (price - entry)) <= 0:
                            pos["waiting_entry"] = False
                            set_user_open_position(int(uid_str), coin, pos)
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Открыть дашборд", web_app=WebAppInfo(url=f"{os.getenv('RENDER_EXTERNAL_URL', 'https://твой-бот.onrender.com')}/miniapp"))]])
                            try: await _send_new(int(uid_str), f"🎯 <b>Лимитка сработала: {coin}</b>\nСделка перешла в активную фазу.", kb, None)
                            except: pass
                        continue
                    
                    pnl = sign * (price - entry) / entry * 100
                    reason = None
                    
                    # Проверка TP/SL
                    if (sign * (price - pos["sl1"])) <= 0:
                        reason = "🛑 Сработал стоп-лосс."
                        pos["notified_close"] = True
                    elif (sign * (price - pos["tp3"])) >= 0 and not pos.get("hit_tp3"):
                        reason = "🏆 Достигнут TP3! Забираем весь профит."
                        pos["hit_tp3"] = True
                        pos["notified_close"] = True
                    elif (sign * (price - pos["tp2"])) >= 0 and not pos.get("hit_tp2"):
                        reason = "✅ Достигнут TP2! Фиксируем 30%."
                        pos["hit_tp2"] = True
                        pos["sl1"] = pos["tp1"]
                    elif (sign * (price - pos["tp1"])) >= 0 and not pos.get("hit_tp1"):
                        reason = "✅ Достигнут TP1! Фиксируем 50%."
                        pos["hit_tp1"] = True
                        pos["sl1"] = entry
                        
                    if reason:
                        set_user_open_position(int(uid_str), coin, pos)
                        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Открыть дашборд", web_app=WebAppInfo(url=f"{os.getenv('RENDER_EXTERNAL_URL', 'https://твой-бот.onrender.com')}/miniapp"))]])
                        try: await _send_new(int(uid_str), f"📊 <b>Обновление по {coin}</b>\n{reason}\nPnL: {pnl:+.2f}%", kb, None)
                        except: pass
        except Exception: pass
        await asyncio.sleep(60)

async def notify_admin(text):
    if not admin_bot: return
    for aid in ADMIN_IDS:
        try: await admin_bot.send_message(aid, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception: pass

# =====================================================================
# TELEGRAM БОТ (ТОЛЬКО КНОПКА)
# =====================================================================
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    uid = m.from_user.id
    await asyncio.to_thread(save_user_data, uid, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    
    base_url = os.getenv("RENDER_EXTERNAL_URL", "https://твой-бот.onrender.com")
    web_app_url = f"{base_url}/miniapp"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Открыть IXCLY PRO", web_app=WebAppInfo(url=web_app_url))]
    ])
    
    text = (
        f"👋 <b>Привет, {m.from_user.first_name}!</b>\n\n"
        f"Добро пожаловать в обновленный <b>IXCLY PRO</b>.\n"
        f"Теперь весь функционал, аналитика и управление портфелем доступны в удобном Mini App.\n\n"
        f"Нажмите кнопку ниже, чтобы начать:"
    )
    await m.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.message()
async def fallback_message(m: types.Message):
    # Игнорируем любые другие сообщения, чтобы не спамить
    pass

# =====================================================================
# ЗАПУСК
# =====================================================================
async def run_polling(dispatcher, tg_bot, name, stop_event):
    attempt = 0
    while not stop_event.is_set():
        try:
            attempt += 1
            log.info(f"{name}: запуск поллинга...")
            try: await tg_bot.delete_webhook(drop_pending_updates=(attempt == 1))
            except: pass
            try: await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=30)
            except Exception as e: raise e
            if not stop_event.is_set(): await asyncio.sleep(5); attempt = 0
        except asyncio.CancelledError: raise
        except Exception as e:
            log.warning(f"{name}: ошибка ({e}), перезапуск...")
            await asyncio.sleep(10)

async def main():
    global http, main_event_loop, news_lock, scan_lock
    keep_alive()
    main_event_loop = asyncio.get_running_loop() # Сохраняем цикл для Flask
    news_lock, scan_lock = asyncio.Lock(), asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/4.0"})
    load_state_file()
    
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, stop.set)
        except: pass
        
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_my_commands([types.BotCommand(command="start", description="🚀 Открыть приложение")])
    except: pass
    
    restored = await telegram_restore()
    if restored: log.info(f"✅ Состояние восстановлено: {restored} юзеров")
    load_active_chats()
    
    background = [
        asyncio.create_task(scanner_loop()),
        asyncio.create_task(position_monitor_loop()),
        asyncio.create_task(state_flush_loop()),
    ]
    
    poller = asyncio.create_task(run_polling(dp, bot, "основной", stop))
    log.info("🚀 IXCLY PRO Web Edition запущен!")
    
    waiter = asyncio.create_task(stop.wait())
    await asyncio.wait([poller, waiter], return_when=asyncio.FIRST_COMPLETED)
    waiter.cancel()
    
    for t in background: t.cancel()
    await asyncio.gather(*background, return_exceptions=True)
    await http.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): log.info("Бот остановлен")
