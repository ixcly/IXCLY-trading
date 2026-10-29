# -*- coding: utf-8 -*-
"""
IXCLY Trading Bot v3.1.0 PRO
- Mini App Dashboard (Web App)
- Анализ Open Interest (OI)
- Макро-календарь (ФРС/CPI)
- Полное сопровождение сделки (трейлинг + ранний БУ)
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

class PortfolioStates(StatesGroup):
    waiting_for_deposit = State()
class AutotradeStates(StatesGroup):
    waiting_for_api_key = State()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("ixcly")

class _ConflictFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.last = 0.0
    def filter(self, record):
        msg = record.getMessage()
        if "TelegramConflictError" in msg or "terminated by other getUpdates" in msg:
            now = time.time()
            if now - self.last > 60:
                self.last = now
                log.warning("ℹ Конфликт getUpdates: рядом работает другой экземпляр.")
            return False
        if "Sleep for" in msg and "try again" in msg:
            return False
        return True
logging.getLogger("aiogram.dispatcher").addFilter(_ConflictFilter())

# =====================================================================
# ВЕБ-СЕРВЕР (keep-alive + Mini App)
# =====================================================================
app = Flask(__name__)

MINI_APP_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>IXCLY Dashboard</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root { --bg: var(--tg-theme-bg-color, #1c1c1e); --text: var(--tg-theme-text-color, #ffffff); --hint: var(--tg-theme-hint-color, #8e8e93); --btn: var(--tg-theme-button-color, #007aff); --btn-text: var(--tg-theme-button-text-color, #ffffff); --card: var(--tg-theme-secondary-bg-color, #2c2c2e); }
        body { font-family: -apple-system, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 15px; }
        h1, h2 { margin: 0 0 10px 0; font-size: 20px; }
        .card { background: var(--card); border-radius: 12px; padding: 15px; margin-bottom: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .row { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; }
        .row span:first-child { color: var(--hint); }
        .green { color: #34c759; font-weight: bold; } .red { color: #ff3b30; font-weight: bold; }
        .btn { display: block; width: 100%; padding: 12px; background: var(--btn); color: var(--btn-text); text-align: center; border-radius: 10px; text-decoration: none; font-weight: bold; border: none; font-size: 16px; margin-top: 10px; }
        .badge { padding: 2px 6px; border-radius: 4px; font-size: 10px; background: #3a3a3c; margin-left: 5px; }
        .loading { text-align: center; color: var(--hint); margin-top: 50px; }
    </style>
</head>
<body>
    <div id="app"><div class="loading">Загрузка данных...</div></div>
    <script>
        const tg = window.Telegram.WebApp;
        tg.expand(); tg.ready();
        const userId = tg.initDataUnsafe?.user?.id;
        tg.BackButton.show(); tg.BackButton.onClick(() => tg.close());
        async function loadData() {
            if (!userId) { document.getElementById('app').innerHTML = '<div class="card">Ошибка авторизации.</div>'; return; }
            try {
                const res = await fetch(`/api/miniapp_data?uid=${userId}`);
                const data = await res.json();
                if (data.error) { document.getElementById('app').innerHTML = `<div class="card">${data.error}</div>`; return; }
                renderUI(data);
            } catch (e) { document.getElementById('app').innerHTML = '<div class="card">Ошибка сети.</div>'; }
        }
        function renderUI(data) {
            let h = `<h1>💼 Портфель</h1><div class="card">
                <div class="row"><span>Депозит:</span> <b>$${data.deposit.toFixed(2)}</b></div>
                <div class="row"><span>Риск:</span> <b>${data.risk_pct}%</b></div>
                <div class="row"><span>Режим:</span> <b>${data.mode}</b></div></div>`;
            h += `<h2>📈 Позиции</h2>`;
            if (Object.keys(data.positions).length === 0) h += `<div class="card" style="text-align:center; color:var(--hint)">Нет сделок</div>`;
            else {
                for (const [c, p] of Object.entries(data.positions)) {
                    const sc = p.side === 'LONG' ? 'green' : 'red';
                    const st = p.waiting_entry ? '⏳ Ожидание' : '⚡ В сделке';
                    h += `<div class="card"><div class="row"><span><b>${c}</b> <span class="badge">${st}</span></span><span class="${sc}">${p.side}</span></div>
                          <div class="row"><span>Вход:</span> <b>${p.entry_price}</b></div>
                          <div class="row"><span>Стоп:</span> <b>${p.sl1}</b></div></div>`;
                }
            }
            h += `<h2>🔥 Сигналы</h2>`;
            if (data.signals.length === 0) h += `<div class="card" style="text-align:center; color:var(--hint)">Нет сигналов</div>`;
            else {
                data.signals.slice(0, 5).forEach(s => {
                    const sc = s.side === 'LONG' ? 'green' : 'red';
                    h += `<div class="card"><div class="row"><span><b>${s.coin}</b></span><span class="${sc}">${s.side} (${s.strength}%)</span></div>
                          <div class="row"><span>Вход:</span> <b>${s.entry}</b></div><div class="row"><span>TP1:</span> <b>${s.tp1}</b></div></div>`;
                });
            }
            h += `<button class="btn" onclick="tg.close()">Вернуться в бот</button>`;
            document.getElementById('app').innerHTML = h;
        }
        loadData();
    </script>
</body>
</html>
"""

@app.route("/")
def home(): return "Bot is alive!"
@app.route("/health")
def health(): return "ok"
@app.route('/miniapp')
def miniapp(): return render_template_string(MINI_APP_HTML)
@app.route('/status')
def status_page(): return jsonify({"version": VERSION, "uptime_s": int(time.time() - stats["started"]), "recipients": len(active_chats), "peak_rss_mb": _rss_mb()})

@app.route('/api/miniapp_data')
def miniapp_data():
    uid = request.args.get('uid')
    if not uid: return jsonify({"error": "Не передан ID"})
    try:
        us = get_user_risk_settings(int(uid))
        pos = get_user_open_positions(int(uid))
        hist = load_signal_history()
        sigs = [{"coin": x["coin"], "side": x["side"], "strength": x.get("strength", 0), "entry": x.get("entry", 0), "tp1": x.get("tp1", 0)} for x in hist if x.get("status") in ("active", "in_trade")][:10]
        u = _users().get(str(uid), {})
        return jsonify({"deposit": us["deposit"], "risk_pct": us["risk_pct"], "mode": u.get("trade_mode", "balanced"), "positions": pos, "signals": sigs})
    except Exception as e:
        log.error(f"Ошибка API MiniApp: {e}")
        return jsonify({"error": "Ошибка сервера"})

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
# НАСТРОЙКИ
# =====================================================================
try:
    FONT_MAIN = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
    FONT_BOLD = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
except: FONT_MAIN = FONT_BOLD = ImageFont.load_default()

def clean_key(k): return k.strip().strip('"').strip("'") if k else None
BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))
GEMINI_MODEL = clean_key(os.getenv("GEMINI_MODEL"))
GROQ_MODEL = clean_key(os.getenv("GROQ_MODEL"))
OPENROUTER_MODEL = clean_key(os.getenv("OPENROUTER_MODEL"))
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

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

VERSION = "3.1.0"
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
# СОСТОЯНИЕ
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
nav_stack = {}
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
def mark_users_inactive(ids):
    with STATE_LOCK:
        for i in ids:
            u = STATE["users"].get(str(i))
            if u: u["active"] = False
    state_touch(False)
def load_active_chats():
    active_chats.clear()
    for uid, u in _users().items():
        if isinstance(u, dict) and u.get("active", True):
            try: active_chats.add(int(uid))
            except ValueError: pass
    log.info("Получателей: %d", len(active_chats))
def user_counts():
    users = _users()
    return len(users), sum(1 for u in users.values() if isinstance(u, dict) and u.get("active", True))
def get_user_notif_settings(user_id):
    u = _users().get(str(user_id), {})
    return {"signals": u.get("notify_signals", True), "news": u.get("notify_news", True), "anomalies": u.get("notify_anomalies", True), "pos_updates": u.get("notify_pos_updates", True), "briefing": u.get("notify_briefing", True)}
def get_user_saved_signals(user_id): return list(_users().get(str(user_id), {}).get("saved_signals", []))
def add_user_saved_signal(user_id, coin, text, res=None):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(user_id), {"active": True})
        lst = [s for s in u.get("saved_signals", []) if s.get("coin") != coin]
        item = {"coin": coin, "text": text, "status": "active", "ts": time.time(), "expire_ts": time.time() + 48 * 3600}
        if res and res.get("entry"): item.update({"entry": res["entry"], "tp1": res["tps"][0], "sl1": res["sls"][0], "is_long": res["is_long"]})
        lst.append(item)
        u["saved_signals"] = lst
    state_touch()
def del_user_saved_signal(user_id, coin):
    with STATE_LOCK:
        u = STATE["users"].get(str(user_id))
        if u and "saved_signals" in u: u["saved_signals"] = [s for s in u["saved_signals"] if s.get("coin") != coin]
    state_touch()
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
def save_analysis_db(coin, res):
    item = {k: v for k, v in res.items() if k != "img"}
    with STATE_LOCK:
        STATE["analysis_db"][coin] = {"ts": time.time(), "data": item}
        if len(STATE["analysis_db"]) > 150:
            for k in sorted(STATE["analysis_db"], key=lambda c: STATE["analysis_db"][c]["ts"])[:30]: STATE["analysis_db"].pop(k, None)
    state_touch(False)
def load_analysis_db(coin): return STATE["analysis_db"].get(coin)

def get_user_risk_settings(user_id):
    u = _users().get(str(user_id), {})
    return {"deposit": float(u.get("deposit", DEPOSIT)), "risk_pct": float(u.get("risk_pct", RISK_PCT)), "max_open_risk": float(u.get("max_open_risk", 5.0))}
def calculate_portfolio_risk(user_id):
    s = get_user_risk_settings(user_id)
    return sum(p.get("risk_pct", s["risk_pct"]) for p in get_user_open_positions(user_id).values())
def set_user_field(user_id, **fields):
    with STATE_LOCK: STATE["users"].setdefault(str(user_id), {"active": True}).update(fields)
    state_touch()
def save_user_api_keys(user_id, exchange, api_key, secret_key):
    if not cipher: return False
    set_user_field(user_id, exchange=exchange, **{f"{exchange}_api": cipher.encrypt(api_key.encode()).decode(), f"{exchange}_secret": cipher.encrypt(secret_key.encode()).decode()})
    return True
def get_user_api_keys(user_id, exchange=None):
    u = _users().get(str(user_id), {})
    if not exchange: exchange = u.get("exchange", "binance")
    api_f, sec_f = f"{exchange}_api", f"{exchange}_secret"
    if not cipher or not u.get(api_f): return None, None
    try: return cipher.decrypt(u[api_f].encode()).decode(), cipher.decrypt(u[sec_f].encode()).decode()
    except Exception: return None, None

async def send_webhook_all(event):
    for uid, u in list(_users().items()):
        url = u.get("webhook_url")
        if url:
            try:
                async with http.post(url, json=event, timeout=aiohttp.ClientTimeout(total=5)) as r: pass
            except Exception: pass

def generate_csv_history():
    with STATE_LOCK: hist = list(STATE["signal_history"])
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "TP2", "TP3", "SL1", "Статус", "Макс TP", "Уверенность"])
    for x in hist:
        w.writerow([datetime.fromtimestamp(x.get("ts", 0)).strftime("%Y-%m-%d %H:%M"), x.get("coin"), x.get("type"), x.get("side"), x.get("entry"), x.get("tp1"), x.get("tp2"), x.get("tp3"), x.get("sl1"), x.get("status"), x.get("max_tp_hit", 0), x.get("strength")])
    return out.getvalue().encode("utf-8-sig")

_defillama_cache = {"ts": 0.0, "change": 0.0}
async def get_onchain_context():
    if time.time() - _defillama_cache["ts"] < 3600: return _defillama_cache["change"]
    try:
        data = await http_json("https://api.llama.fi/v2/historicalChainTvl", timeout=10)
        if len(data) > 2 and data[-2]["tvl"]:
            ch = (data[-1]["tvl"] - data[-2]["tvl"]) / data[-2]["tvl"] * 100
            _defillama_cache.update(ts=time.time(), change=round(ch, 2))
    except Exception as e: _defillama_cache["ts"] = time.time()
    return _defillama_cache["change"]

def fmt(p):
    p = float(p)
    a = abs(p)
    if a >= 1000: return f"{p:,.2f}".replace(",", " ")
    if a >= 1: return f"{p:.4f}"
    if a >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"
def make_strength_bar(strength, length=10):
    filled = max(0, min(length, int(round(strength / 100 * length))))
    return "🟩" * filled + "🟥" * (length - filled)
def has_cyr(text, n=1): return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= n
def shorten(text, n):
    text = " ".join(str(text).split())
    if len(text) <= n: return text
    cut = text[:n]
    m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if m >= n * 0.5: return cut[:m + 1]
    return cut.rsplit(" ", 1)[0].rstrip(",;:-— ") + "…"
def strip_tags(t): return html.unescape(re.sub(r"<[^>]+>", "", t or ""))
def vis_len(t): return len(strip_tags(t))
def trim_html(text, limit):
    if vis_len(text) <= limit: return text
    return html.escape(strip_tags(text)[:limit - 1]) + "…"
def fit_caption(parts, limit=1000):
    items = list(parts)
    def total(): return sum(vis_len(line) + 1 for _, line in items)
    while total() > limit and items:
        worst = max(p for p, _ in items)
        if worst <= 1: break
        idx = max(i for i, (p, _) in enumerate(items) if p == worst)
        items.pop(idx)
    return trim_html(re.sub(r"\n{3,}", "\n\n", "\n".join(line for _, line in items)).strip(), limit)
def normalize_ticker(text):
    t = re.sub(r"[\s$/\-_]", "", text.upper().strip())
    return t[:-4] if t.endswith("USDT") and len(t) > 4 else t
def get_moscow_time_from_entry(entry):
    msk = timezone(timedelta(hours=3))
    try:
        published = None
        if getattr(entry, "published_parsed", None): published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "updated_parsed", None): published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "published", None):
            published = parsedate_to_datetime(entry.published)
            if published.tzinfo is None: published = published.replace(tzinfo=timezone.utc)
        if published: return published.astimezone(msk).strftime("%d.%m.%Y %H:%M") + " МСК"
    except Exception: pass
    return datetime.now(msk).strftime("%d.%m.%Y %H:%M") + " МСК"

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
async def safe_delete(message):
    try: await message.delete()
    except Exception: pass
async def safe_answer(cb, text=None, alert=False):
    try: await cb.answer(text, show_alert=alert)
    except Exception: pass

def get_tracked(chat_id): return list(STATE["last_msgs"].get(str(chat_id), []))
def set_tracked(chat_id, ids):
    with STATE_LOCK: STATE["last_msgs"][str(chat_id)] = list(ids)[-4:]
    state_touch(False)
async def delete_ids(chat_id, ids):
    ids = [i for i in set(ids) if i]
    if not ids: return
    try:
        await bot.delete_messages(chat_id=chat_id, message_ids=ids[:100])
        return
    except Exception: pass
    for i in ids[:30]:
        try: await bot.delete_message(chat_id, i)
        except Exception: pass
async def purge_chat(chat_id, upto_id, count=100):
    ids = list(range(max(1, upto_id - count), upto_id + 1))
    try: await bot.delete_messages(chat_id=chat_id, message_ids=ids)
    except Exception: pass
async def _send_new(chat_id, text, kb, photo):
    try:
        if photo: return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=trim_html(text, 1024), parse_mode="HTML", reply_markup=kb)
        return await bot.send_message(chat_id, trim_html(text, 4000), parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "parse" not in str(e).lower() and "entit" not in str(e).lower(): raise
        plain = strip_tags(text)
        if photo: return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=plain[:1024], reply_markup=kb)
        return await bot.send_message(chat_id, plain[:4000], reply_markup=kb)
async def present(chat_id, text, kb=None, photo=None, edit_msg=None):
    old = set(get_tracked(chat_id))
    if edit_msg is not None: old.add(edit_msg.message_id)
    msg = None
    has_media = edit_msg is not None and bool(edit_msg.photo or edit_msg.video or edit_msg.document or edit_msg.animation)
    if photo is None and edit_msg is not None and not has_media:
        try:
            await bot.edit_message_text(trim_html(text, 4000), chat_id=chat_id, message_id=edit_msg.message_id, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
            msg = edit_msg
        except TelegramBadRequest as e:
            if "not modified" in str(e).lower(): msg = edit_msg
        except (TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter): raise
        except Exception: msg = None
    if msg is None: msg = await _send_new(chat_id, text, kb, photo)
    await delete_ids(chat_id, [i for i in old if i != msg.message_id])
    set_tracked(chat_id, [msg.message_id])
    return msg
async def show_loading(chat_id, edit_msg, text):
    try:
        if edit_msg is None: return await present(chat_id, text)
        if edit_msg.photo: await bot.edit_message_caption(chat_id=chat_id, message_id=edit_msg.message_id, caption=text, parse_mode="HTML", reply_markup=None)
        else: await bot.edit_message_text(text, chat_id=chat_id, message_id=edit_msg.message_id, parse_mode="HTML")
    except Exception: pass
    return edit_msg

def nav_reset(uid, *screens): nav_stack[uid] = [("menu",)] + list(screens)
def nav_push(uid, screen):
    st = nav_stack.setdefault(uid, [("menu",)])
    if st[-1] != screen: st.append(screen)
    if len(st) > 25: del st[1:-24]
def nav_replace(uid, screen):
    st = nav_stack.setdefault(uid, [("menu",)])
    if len(st) > 1 and st[-1][0] == screen[0]: st[-1] = screen
    else: nav_push(uid, screen)
def nav_back(uid):
    st = nav_stack.setdefault(uid, [("menu",)])
    if len(st) > 1: st.pop()
    return st[-1]
def nav_row(back=True):
    row = []
    if back: row.append(InlineKeyboardButton(text="⬅ Назад", callback_data="nav:back"))
    row.append(InlineKeyboardButton(text="🏠 Меню", callback_data="menu"))
    return row
def kb_nav(rows=None, back=True): return InlineKeyboardMarkup(inline_keyboard=list(rows or []) + [nav_row(back)])

class UserTrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user, chat_id = None, None
        if event.message: user, chat_id = event.message.from_user, event.message.chat.id
        elif event.callback_query:
            user = event.callback_query.from_user
            if event.callback_query.message: chat_id = event.callback_query.message.chat.id
        if user and chat_id and chat_id not in active_chats:
            active_chats.add(chat_id)
            asyncio.create_task(asyncio.to_thread(save_user_data, user.id, user.username, user.first_name))
        return await handler(event, data)
dp.update.middleware(UserTrackerMiddleware())

@dp.errors()
async def errors_handler(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, TelegramNetworkError): return True
    if isinstance(exc, TelegramBadRequest) and any(s in str(exc).lower() for s in ("query is too old", "not modified", "message to delete not found")): return True
    log.error(f"Ошибка: {exc}", exc_info=exc)
    return True

# =====================================================================
# ИНДИКАТОРЫ
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
# БИРЖИ
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
    if len(rows) < (min_rows or MIN_CANDLES + 1): raise ValueError(f"мало свечей ({len(rows)})")
    return rows
def _note_error(coin, ex_name, err):
    fetch_errors.setdefault(coin, []).append(f"{ex_name}: {str(err)[:40]}")
    fetch_errors[coin] = fetch_errors[coin][-12:]
async def _try_exchange(ex, cand, tfs, coin=None):
    if time.time() < exchange_bans.get(ex["name"], 0): return None
    coin = coin or cand
    try: first = await _fetch_klines(ex, cand, tfs[0])
    except Exception as e:
        _note_error(coin, ex["name"], e)
        if any(code in str(e) for code in ("429", "418", "403", "451")):
            exchange_bans[ex["name"]] = time.time() + 300
            log.warning(f"⚠ {ex['name']} ограничил доступ ({e}). Пауза 5 минут.")
        return None
    results = [first]
    if len(tfs) > 1:
        rest = await asyncio.gather(*[_fetch_klines(ex, cand, tf) for tf in tfs[1:]], return_exceptions=True)
        for r in rest:
            if isinstance(r, Exception):
                _note_error(coin, ex["name"], r)
                return None
            results.append(r)
    return await asyncio.to_thread(build_tf_data, dict(zip(tfs, results))), ex["name"]
async def get_market_data(coin, tfs=tuple(TFS)):
    fetch_errors.pop(coin, None)
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]):
            res = await _try_exchange(ex, cand, tfs, coin)
            if res:
                preferred_source[coin] = (cand, ex["name"])
                return res
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
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex: order.append((pref[0], ex))
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]): order.append((cand, ex))
    for cand, ex in order:
        if (cand, ex["name"]) in seen or time.time() < exchange_bans.get(ex["name"], 0): continue
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
            ("https://api.mexc.com/api/v3/ticker/price", {"symbol": f"{cand}USDT"}, lambda j: j["price"]),
        ):
            try: return float(path(await http_json(url, params=params, timeout=5)))
            except Exception: continue
    for url, path in ((f"https://min-api.cryptocompare.com/data/price?fsym={coin}&tsyms=USDT", lambda j: j["USDT"]), (f"https://api.coinbase.com/v2/prices/{coin}-USD/spot", lambda j: j["data"]["amount"]), (f"https://api.kraken.com/0/public/Ticker?pair={coin}USDT", lambda j: list(j["result"].values())[0]["c"][0])):
        try: return float(path(await http_json(url, timeout=6)))
        except Exception: continue
    return None

_funding_cache = {}
async def check_order_book_walls(coin, current_price, is_long):
    try:
        url = "https://fapi.binance.com/fapi/v1/depth"
        j = await http_json(url, params={"symbol": f"{coin}USDT", "limit": 50}, timeout=5)
        if not j or "bids" not in j or "asks" not in j: return None
        bids = [(float(p), float(q)) for p, q in j["bids"]]
        asks = [(float(p), float(q)) for p, q in j["asks"]]
        max_bid_wall = max((p * q for p, q in bids if current_price * 0.98 <= p <= current_price), default=0)
        max_ask_wall = max((p * q for p, q in asks if current_price <= p <= current_price * 1.02), default=0)
        wall_size = 500_000
        if is_long and max_ask_wall > wall_size: return f"Стена на продажу (${max_ask_wall/1000:.0f}K) прямо над ценой. Риск отскока."
        if not is_long and max_bid_wall > wall_size: return f"Стена на покупку (${max_bid_wall/1000:.0f}K) прямо под ценой. Риск отскока."
    except Exception as e: pass
    return None
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
            oi_old = float(data[0]["sumOpenInterest"])
            oi_new = float(data[1]["sumOpenInterest"])
            return round(((oi_new - oi_old) / oi_old) * 100, 2)
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
def pattern_lines(tf_data):
    return [f"{tf}: {', '.join(p[0] for p in tf_data[tf]['patterns'][:3])}" for tf in ("1d", "4h", "1h") if tf in tf_data and tf_data[tf]["patterns"]]

async def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0.0, conf_threshold=CONF_FINAL):
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрок", "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": []}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data:
        out["reject"] = "Недостаточно данных по таймфреймам"
        return out
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
    if not reject and info["atr"] / info["price"] > 0.06: reject = "Слишком высокая волатильность (ATR > 6%)"
    if "1h" in tf_data and (tf_data["1h"]["long_prob"] >= 50) != is_long and tf_data["1h"]["confidence"] >= 65: strength -= 8; notes.append("1h против направления (−8)")
    if is_long and (info["rsi"] > 75 or info["price"] - info["ema20"] > 2.5 * info["atr"]): strength -= 8; notes.append("цена перегрета, риск погони (−8)")
    if (not is_long) and (info["rsi"] < 25 or info["ema20"] - info["price"] > 2.5 * info["atr"]): strength -= 8; notes.append("цена перепродана, риск погони (−8)")
    if coin != "BTC" and ctx.get("btc_dir", 0) != 0:
        if ctx["btc_dir"] == side: strength += 3; notes.append("BTC в ту же сторону (+3)")
        else: strength -= 10 if ctx.get("btc_conf", 50) >= 65 else 5; notes.append(f"BTC против направления")
    fng = ctx.get("fng")
    if fng is not None and ((fng >= 80 and is_long) or (fng <= 20 and not is_long)): strength -= 4; notes.append(f"экстремальный {'страх' if fng <= 20 else 'жадность'} {fng} (−4)")
    if funding is not None:
        if is_long and funding > 0.0005: strength -= 5; notes.append(f"перегрет лонг: funding {funding * 100:.3f}% (−5)")
        elif (not is_long) and funding < -0.0005: strength -= 5; notes.append(f"перегрет шорт: funding {funding * 100:.3f}% (−5)")
    oi_change = await get_open_interest_change(coin)
    if is_long:
        if oi_change > 2.0: strength += 5; notes.append(f"OI растет (+{oi_change}%) (+5)")
        elif oi_change < -2.0: strength -= 5; notes.append(f"OI падает ({oi_change}%) (−5)")
    else:
        if oi_change > 2.0: strength += 5; notes.append(f"OI растет (+{oi_change}%) (+5)")
        elif oi_change < -2.0: strength -= 5; notes.append(f"OI падает ({oi_change}%) (−5)")
    if onchain_change < -2.0 and is_long: strength -= 10; notes.append(f"отток TVL {onchain_change}% против лонга (−10)")
    elif onchain_change > 2.0 and not is_long: strength -= 10; notes.append(f"приток TVL {onchain_change}% против шорта (−10)")
    msk = timezone(timedelta(hours=3))
    now_msk = datetime.now(msk)
    if now_msk.weekday() >= 5 or now_msk.hour < 8 or now_msk.hour >= 22: strength -= 7; notes.append("Низкая ликвидность (−7)")
    fake_breakout = False
    df_info = info.get("df")
    if df_info is not None and len(df_info) > 0:
        last_candle = df_info.iloc[-1]
        if last_candle["brk_up"] or last_candle["brk_dn"]:
            if last_candle["rel_vol"] < 0.8: fake_breakout = True; strength -= 15; notes.append("Ложный пробой (−15)")
    if not reject and strength >= conf_threshold - 5:
        wall_warning = await check_order_book_walls(coin, info["price"], is_long)
        if wall_warning: strength -= 10; notes.append(f"🐋 Стакан: {wall_warning} (−10)")
    nb = market_bias.get("bias", 0)
    news_status = "Нейтрально"
    if time.time() - market_bias.get("ts", 0) < 3 * 3600 and nb != 0:
        word = "бычий" if nb == 1 else "медвежий"
        if nb == side: strength += 7; news_status = f"✅ Подтверждают ({word} фон)"
        else: strength -= 20; news_status = f"❌ Противоречат ({word} фон)"
    strength = max(0.0, min(100.0, strength))
    if not reject and strength < conf_threshold: reject = f"Уверенность {strength:.0f}% ниже порога {conf_threshold:.0f}%"
    p15, p1h = tf_data.get("15m", {}).get("long_prob", 50), tf_data.get("1h", {}).get("long_prob", 50)
    fast = "15m" in tf_data and "1h" in tf_data and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    out.update(valid=not reject, reject=reject, notes=notes, is_long=is_long, strength=round(strength, 1), type="Краткосрочный" if fast else "Долгосрочный" if long_term else "Среднесрочный", news_status=news_status, wp=wp, tf_probs={t: tf_data[t]["long_prob"] for t in tfs}, reasons=collect_reasons(tf_data, is_long), patterns=pattern_lines(tf_data), fake_breakout=fake_breakout)
    return out

def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    if is_long:
        entry = min(price, max(ema20, ema50))
        if entry > price - 0.2 * atr: entry = price - 0.35 * atr
        if support and price - 1.5 * atr <= support <= price - 0.15 * atr: entry = min(max(entry, support + 0.1 * atr), price - 0.15 * atr)
        sl1 = max(min(rec_low - 0.5 * atr, entry - 0.8 * atr), entry - 2.5 * atr)
        return entry, sl1, entry - sl1
    entry = max(price, min(ema20, ema50))
    if entry < price + 0.2 * atr: entry = price + 0.35 * atr
    if resistance and price + 0.15 * atr <= resistance <= price + 1.5 * atr: entry = max(min(entry, resistance - 0.1 * atr), price + 0.15 * atr)
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
    if atr_pct > 3.0: leverage = "x3-x5 (Высокая волатильность ⚠)"
    elif atr_pct > 1.5: leverage = "x5-x10 (Средняя волатильность)"
    else: leverage = "x10-x20 (Низкая волатильность)"
    liq_x20 = entry * (1 - 0.0475) if is_long else entry * (1 + 0.0475)
    opp = sr.get("resistance") if is_long else sr.get("support")
    near = "ближайший уровень слишком близко к входу" if opp and abs(opp - entry) < 1.2 * risk else None
    return entry, tps, [sl1, sl2], risk, leverage, near, liq_x20

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
    if len(bt_cache) > 200: bt_cache.clear()
    c = bt_cache.get(coin)
    if c and time.time() - c[0] < 6 * 3600: return c[1]
    bt = None
    try:
        rows = await fetch_history(coin, "4h", 1000)
        if rows: bt = await asyncio.to_thread(run_backtest_rows, rows)
    except Exception: pass
    bt_cache[coin] = (time.time(), bt)
    return bt
def bt_ok(bt):
    return (bool(bt) and bt["trades"] >= BT_MIN_TRADES and bt["avg_r"] >= BT_MIN_AVG_R and bt["winrate"] >= BT_MIN_WINRATE and bt["pf"] >= float(os.getenv("BT_MIN_PF", "1.15")))

MODEL_CACHE = {"gemini": [], "groq": [], "openrouter": [], "ts": 0.0}
BAD_MODELS = {}
LAST_MODEL = {}
ENV_MODELS = {"gemini": GEMINI_MODEL, "groq": GROQ_MODEL, "openrouter": OPENROUTER_MODEL}
FALLBACK_MODELS = {"gemini": ["gemini-2.5-flash", "gemini-flash-latest", "gemini-2.0-flash", "gemini-2.5-flash-lite"], "groq": ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-20b", "gemma2-9b-it"], "openrouter": ["meta-llama/llama-3.3-70b-instruct:free", "deepseek/deepseek-chat-v3-0324:free", "google/gemini-2.0-flash-exp:free", "qwen/qwen-2.5-72b-instruct:free", "mistralai/mistral-7b-instruct:free"]}
def _ver(s):
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0
def pick_gemini_models(payload):
    names = []
    for m in payload.get("models", []):
        n = str(m.get("name", "")).replace("models/", "")
        if "generateContent" not in m.get("supportedGenerationMethods", []) or "flash" not in n: continue
        if any(x in n for x in ("image", "tts", "audio", "live", "embedding", "exp", "preview", "thinking", "vision", "robotics")): continue
        names.append(n)
    names.sort(key=lambda n: (not n.endswith("latest"), "lite" in n, -_ver(n)))
    return names[:5]
def pick_groq_models(payload):
    bad = ("whisper", "guard", "tts", "playai", "distil", "embed", "orpheus", "safeguard")
    ids = [m["id"] for m in payload.get("data", []) if m.get("active", True) and not any(b in m["id"].lower() for b in bad)]
    pref = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama", "qwen", "gemma"]
    def rank(i):
        for idx, p in enumerate(pref):
            if p == i: return idx - 100
            if p in i: return idx
        return 99
    return sorted(ids, key=rank)[:5]
def pick_openrouter_models(payload):
    fam = ["llama-3.3-70b", "gemini", "deepseek", "qwen", "mistral", "gpt-oss", "llama"]
    free = []
    for m in payload.get("data", []):
        mid = str(m.get("id", ""))
        pr = m.get("pricing") or {}
        try: is_free = float(pr.get("prompt", 1)) == 0 and float(pr.get("completion", 1)) == 0
        except (TypeError, ValueError): is_free = mid.endswith(":free")
        if not is_free or any(x in mid.lower() for x in ("vision", "image", "audio", "embed", "moderation", "guard")): continue
        rank = next((i for i, f in enumerate(fam) if f in mid.lower()), 99)
        free.append((rank, -int(m.get("context_length") or 0), mid))
    return [x[2] for x in sorted(free)[:6]]
async def discover_models(force=False):
    if not force and time.time() - MODEL_CACHE["ts"] < 6 * 3600: return
    MODEL_CACHE["ts"] = time.time()
    async def get(url, headers=None, params=None):
        async with http.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: raise RuntimeError(f"HTTP {r.status}")
            return await r.json(content_type=None)
    async def gem():
        if not GEMINI_API_KEYS: return []
        return pick_gemini_models(await get("https://generativelanguage.googleapis.com/v1beta/models", {"x-goog-api-key": GEMINI_API_KEYS[0]}, {"pageSize": 200}))
    async def grq():
        if not GROQ_API_KEY: return []
        return pick_groq_models(await get("https://api.groq.com/openai/v1/models", {"Authorization": f"Bearer {GROQ_API_KEY}"}))
    async def opr():
        if not OPENROUTER_API_KEY: return []
        return pick_openrouter_models(await get("https://openrouter.ai/api/v1/models", {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}))
    res = await asyncio.gather(gem(), grq(), opr(), return_exceptions=True)
    for name, r in zip(("gemini", "groq", "openrouter"), res):
        if isinstance(r, list) and r: MODEL_CACHE[name] = r
    log.info(f"Модели ИИ: { {k: v for k, v in MODEL_CACHE.items() if k != 'ts'} }")
async def models_for(prov):
    await discover_models()
    seq = []
    for m in [LAST_MODEL.get(prov), ENV_MODELS.get(prov)] + MODEL_CACHE[prov] + FALLBACK_MODELS[prov]:
        if m and m not in seq and BAD_MODELS.get((prov, m), 0) < time.time(): seq.append(m)
    return seq[:6]
def _mark_bad(prov, model):
    BAD_MODELS[(prov, model)] = time.time() + 6 * 3600
    MODEL_CACHE["ts"] = min(MODEL_CACHE["ts"], time.time() - 6 * 3600 + 600)
async def _ask_gemini(prompt):
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    for key in GEMINI_API_KEYS:
        for model in await models_for("gemini"):
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            try:
                async with http.post(url, json=payload, headers={"x-goog-api-key": key}, timeout=aiohttp.ClientTimeout(total=18)) as r:
                    j = await r.json(content_type=None)
                    if r.status == 200:
                        LAST_MODEL["gemini"] = model
                        return j["candidates"][0]["content"]["parts"][0]["text"]
                    if r.status in (401, 403): break
                    if r.status in (400, 404): _mark_bad("gemini", model)
            except Exception: pass
    return None
async def _ask_openai_compat(prov, base, key, prompt, extra_headers=None):
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra_headers: headers.update(extra_headers)
    for model in await models_for(prov):
        payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1100}
        try:
            async with http.post(f"{base}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
                if r.status == 200:
                    txt = j["choices"][0]["message"]["content"]
                    if txt: LAST_MODEL[prov] = model; return txt
                    continue
                if r.status in (401, 403): return None
                if r.status in (400, 404): _mark_bad(prov, model)
        except Exception: pass
    return None
def _looks_like_junk(text):
    t = (text or "").strip()
    low = t.lower()
    return len(t) < 30 or low.startswith("<!doctype") or low.startswith("<html") or any(x in low for x in ("pollinations", "deprecat", "rate limit"))
async def _ask_free_gpt(prompt):
    system = "Ты опытный крипто-аналитик. Отвечай на русском языке, кратко и по делу."
    auth = {"Authorization": f"Bearer {POLLINATIONS_KEY}"} if POLLINATIONS_KEY else {}
    async def post_openai(url, timeout):
        payload = {"model": POLLINATIONS_MODEL, "temperature": 0.2, "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
        async with http.post(url, json=payload, headers={"Content-Type": "application/json", **auth}, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200: raise RuntimeError(f"HTTP {r.status}")
            return (await r.json(content_type=None))["choices"][0]["message"]["content"]
    async def get_plain(url, timeout):
        async with http.get(url, params={"model": POLLINATIONS_MODEL, "system": system}, headers=auth, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200: raise RuntimeError(f"HTTP {r.status}")
            return await r.text()
    short = quote(prompt[:3000], safe="")
    attempts = [lambda: post_openai("https://text.pollinations.ai/openai", 14), lambda: get_plain(f"https://gen.pollinations.ai/text/{short}", 10), lambda: get_plain(f"https://text.pollinations.ai/{short}", 8)]
    if POLLINATIONS_KEY: attempts.insert(0, lambda: post_openai("https://gen.pollinations.ai/v1/chat/completions", 14))
    for fn in attempts:
        try:
            text = await fn()
            if text and not _looks_like_junk(text): return text
        except Exception: pass
    return None
async def ask_ai_named(prompt, timeout=20, validate=None, free_timeout=30):
    providers = []
    if GEMINI_API_KEYS: providers.append(("Gemini", lambda: _ask_gemini(prompt)))
    if GROQ_API_KEY: providers.append(("Groq", lambda: _ask_openai_compat("groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, prompt)))
    if OPENROUTER_API_KEY: providers.append(("OpenRouter", lambda: _ask_openai_compat("openrouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, prompt, {"HTTP-Referer": "https://t.me/bot", "X-Title": "IXCLY"})))
    async def wrap(name, fn):
        try: t = await fn()
        except Exception: return None
        return (t, name) if t and len(t) > 20 and (validate is None or validate(t)) else None
    if providers:
        res = await first_success([wrap(n, f) for n, f in providers], timeout=timeout)
        if res: return res
    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt))], timeout=free_timeout)
    return res if res else (None, None)
async def run_ai_diag():
    prompt = "Напиши одно предложение о биткоине."
    lines = ["🔧 <b>Проверка ИИ</b>", ""]
    async def timed(name, fn):
        t0 = time.time()
        try: r = await asyncio.wait_for(fn(), 28)
        except Exception: r = None
        model = LAST_MODEL.get(name.split()[0].lower(), "")
        return f"{'✅' if r else '❌'} {name} {model} · {int((time.time() - t0) * 1000)} мс"
    tests = []
    if GEMINI_API_KEYS: tests.append(timed("Gemini", lambda: _ask_gemini(prompt)))
    else: lines.append("➖ Gemini: нет ключа")
    if GROQ_API_KEY: tests.append(timed("Groq", lambda: _ask_openai_compat("groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, prompt)))
    else: lines.append("➖ Groq: нет ключа")
    if OPENROUTER_API_KEY: tests.append(timed("OpenRouter", lambda: _ask_openai_compat("openrouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, prompt)))
    else: lines.append("➖ OpenRouter: нет ключа")
    tests.append(timed("Бесплатный GPT", lambda: _ask_free_gpt(prompt)))
    return "\n".join(lines + list(await asyncio.gather(*tests)))
_ai_cache = {}
def _parse_analyst(text):
    try:
        d = json.loads(re.search(r"\{.*\}", text or "", re.S).group(0))
        verdict, comment = str(d.get("verdict", "")).upper().strip(), str(d.get("comment", "")).strip()
        if verdict in ("ENTER", "WAIT") and has_cyr(comment, 15):
            return {"verdict": verdict, "risk": str(d.get("risk", "средний")).strip()[:20], "comment": shorten(comment, 320)}
    except Exception: pass
    return None
async def ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data):
    key = (coin, setup["is_long"])
    c = _ai_cache.get(key)
    if c and time.time() - c[0] < 1200: return c[1]
    tfp = ", ".join(f"{t} {setup['tf_probs'][t]:.0f}%" for t in ("1d", "4h", "1h", "15m") if t in setup["tf_probs"])
    bt_txt = f"{bt['trades']} сделок, TP1 {bt['winrate']}%, ср. R {bt['avg_r']:+.2f}, PF {bt['pf']}" if bt else "нет данных"
    prompt = f"""Ты опытный крипто-трейдер и риск-менеджер. Оцени идею и дай вердикт.
Монета: {coin}. Направление: {'LONG' if setup['is_long'] else 'SHORT'}. Тип: {setup['type']}. Уверенность модели: {setup['strength']}%.
Шанс лонга по ТФ: {tfp}. Режим 4h: {tf_data['4h']['regime']} (ADX {tf_data['4h']['adx']:.0f}).
Причины: {'; '.join(setup['reasons']) or 'нет'}. Свечи: {'; '.join(setup['patterns']) or 'нет'}.
Бэктест 4h: {bt_txt}. Новости: {setup['news_status']}. BTC {ctx.get('btc_txt')}, Fear&Greed {ctx.get('fng')}.
Поправки: {'; '.join(setup['notes']) or 'нет'}. Вход {fmt(entry)}, TP1 {fmt(tps[0])}, SL {fmt(sls[0])}.
ENTER только если идея качественная, при сомнениях WAIT.
Верни СТРОГО JSON без markdown: {{"verdict":"ENTER или WAIT","risk":"низкий или средний или высокий","comment":"1-2 предложения по-русски: главный довод и главный риск"}}"""
    text, engine = await ask_ai_named(prompt, validate=lambda t: _parse_analyst(t) is not None, timeout=22, free_timeout=25)
    res = _parse_analyst(text) if text else None
    if res: res["engine"] = engine
    _ai_cache[key] = (time.time(), res)
    return res

BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner", "upgrad", "breakout", "rebound", "recover", "accumul")
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall", "liquidat", "fraud", "scam", "delist", "arrest", "dump", "selloff", "declin", "probe", "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
COIN_WORDS = {"bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL", "xrp": "XRP", "bnb": "BNB", "doge": "DOGE", "ada": "ADA", "ton": "TON", "avax": "AVAX", "link": "LINK", "zcash": "ZEC"}
def news_sentiment(text, threshold=2):
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w.startswith(BULL_STEMS): score += 1
        elif w.startswith(BEAR_STEMS): score -= 1
    return 1 if score >= threshold else -1 if score <= -threshold else 0
def mentioned_coins(text):
    found = []
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        t = COIN_WORDS.get(w)
        if t and t not in found: found.append(t)
    return found[:4]
async def translate_text(text, target="ru"):
    text = (text or "").strip()
    if not text: return text
    try:
        async with http.get("https://translate.googleapis.com/translate_a/single", params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text[:1800]}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                out = "".join(part[0] for part in data[0] if part and part[0])
                if has_cyr(out, 3): return out
    except Exception: pass
    return text

NEWS_FEEDS = ["https://www.coindesk.com/arc/outboundfeeds/rss/", "https://www.theblock.co/rss.xml", "https://cointelegraph.com/rss", "https://decrypt.co/feed", "https://cryptoslate.com/feed/", "https://bitcoinmagazine.com/.rss/full/", "https://www.dlnews.com/arc/outboundfeeds/rss/", "https://blockworks.co/feed", "https://thedefiant.io/feed", "https://u.today/rss", "https://finance.yahoo.com/rss/cryptocurrency", "https://watcher.guru/news/feed", "https://www.cryptopolitan.com/feed/", "https://blog.coinmarketcap.com/feed/", "https://forklog.com/feed", "https://bits.media/rss/news/", "https://coinspot.io/feed/", "https://www.rbc.ru/crypto/rss"]
_news_cache = {"ts": 0.0, "entries": []}
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
async def _load_feed(url):
    try:
        async with http.get(url, headers=BROWSER_UA, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: return []
            text = await r.text()
        return list((await asyncio.to_thread(feedparser.parse, text)).entries)
    except Exception: return []
async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300: return _news_cache["entries"]
    results = await asyncio.gather(*[_load_feed(u) for u in NEWS_FEEDS], return_exceptions=True)
    entries = [e for r in results if isinstance(r, list) for e in r]
    if entries:
        entries.sort(key=lambda x: getattr(x, "published_parsed", None) or time.gmtime(0), reverse=True)
        _news_cache.update(ts=time.time(), entries=entries[:10])
    return _news_cache["entries"]
def extract_image(entry):
    if getattr(entry, "media_content", None): return entry.media_content[0].get("url")
    if getattr(entry, "media_thumbnail", None): return entry.media_thumbnail[0].get("url")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', getattr(entry, "summary", "") or "")
    return m.group(1) if m else None
def clean_summary(raw):
    t = html.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    t = re.sub(r"The post .*? appeared first on .*", "", t, flags=re.S)
    return " ".join(t.split())[:700]
def parse_news_ai(text):
    t = (text or "").replace("**", "")
    ms = list(re.finditer(r"(?im)^[\s\-\*•]*(BIAS|TITLE_RU|SUMMARY|IMPACT|COINS)\s*[:：]\s*", t))
    out = {}
    for i, m in enumerate(ms):
        end = ms[i + 1].start() if i + 1 < len(ms) else len(t)
        out[m.group(1).upper()] = re.sub(r"\s+", " ", t[m.end():end]).strip()
    return out
def _valid_news_ai(text):
    p = parse_news_ai(text)
    return has_cyr(p.get("TITLE_RU", ""), 6) and has_cyr(p.get("SUMMARY", ""), 20) and has_cyr(p.get("IMPACT", ""), 10)
IMPACT_TEMPLATES = {1: "Фон позитивный: интерес к рисковым активам может поддержать спрос и рост цен на BTC, ETH и альткоины. Следите за точками входа в лонг.", -1: "Фон негативный: возможны распродажи и рост волатильности. Соблюдайте риск-менеджмент и избегайте погони за ценой.", 0: "Фон нейтральный: рынок ждёт новых триггеров, сигналам нужно дополнительное подтверждение."}
async def update_market_bias(force=False):
    global market_bias
    async with news_lock:
        try:
            entries = await get_news_entries()
            if not entries: return
            entry = entries[0]
            link, now = getattr(entry, "link", ""), time.time()
            if not force and market_bias.get("link") == link and market_bias.get("engine") not in ("none", "fallback"):
                market_bias["ts"] = now
                return
            title = html.unescape(getattr(entry, "title", "") or "")
            summary = clean_summary(getattr(entry, "summary", ""))
            published = get_moscow_time_from_entry(entry)
            prompt = f"""Новость: {title}
Текст: {summary}
Переведи на русский и разбери строго в формате (каждое поле с новой строки):
BIAS: BULLISH или BEARISH или NEUTRAL
TITLE_RU: точный перевод заголовка
SUMMARY: 2-3 предложения: что произошло, кто участвует, какие цифры
IMPACT: 2-3 предложения о влиянии на рынок: фон (бычий или медвежий), какие активы и почему
COINS: тикеры через запятую"""
            text, engine = await ask_ai_named(prompt, validate=_valid_news_ai)
            if text and _valid_news_ai(text):
                p = parse_news_ai(text)
                up = p.get("BIAS", "").upper()
                market_bias = {"bias": 1 if "BULL" in up else -1 if "BEAR" in up else 0, "title_ru": p["TITLE_RU"][:170], "summary": shorten(p["SUMMARY"], 380), "impact": shorten(p["IMPACT"], 500), "coins": p.get("COINS", "BTC, ETH"), "published": published, "link": link, "engine": engine, "ts": now}
            else:
                bias = news_sentiment(f"{title}. {summary}")
                title_ru = await translate_text(title)
                market_bias = {"bias": bias, "title_ru": title_ru[:170], "summary": shorten(await translate_text(summary) if summary else title_ru, 380), "impact": IMPACT_TEMPLATES[bias], "coins": ", ".join(mentioned_coins(f"{title} {summary}") or ["BTC", "ETH"]), "published": published, "link": link, "engine": "fallback", "ts": now}
            state_touch(False)
        except Exception: pass
def format_news_post(nb, limit=1000):
    emoji = {1: "🟢", 0: "⚪", -1: "🔴"}.get(nb.get("bias", 0), "⚪")
    label = {1: "Позитивно", 0: "Нейтрально", -1: "Негативно"}.get(nb.get("bias", 0), "Нейтрально")
    link = f"\n\n🔗 <a href=\"{html.escape(nb['link'], quote=True)}\">Источник</a>" if nb.get("link") else ""
    s_len, i_len = 300, 360
    while True:
        text = (f"📰 <b>{html.escape(shorten(nb.get('title_ru', ''), 140))}</b>\n🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n"
                f"📌 <b>Суть</b>\n{html.escape(shorten(nb.get('summary', ''), s_len))}\n\n"
                f"📊 <b>Влияние</b>\n{emoji} <b>{label}.</b> {html.escape(shorten(nb.get('impact', ''), i_len))}\n\n"
                f"🪙 <b>Монеты:</b> {html.escape(nb.get('coins', ''))}{link}")
        if vis_len(text) <= limit or s_len <= 80: return text
        s_len, i_len = s_len - 40, i_len - 40
async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries or market_bias.get("engine") == "none": return None, None
    return format_news_post(market_bias), extract_image(entries[0])
async def get_news_batch_text():
    entries = await get_news_entries()
    if not entries: return "📭 <b>Свежих новостей пока нет.</b> Загляните позже."
    batch = entries[:3]
    prompt = ("Ты опытный крипто-аналитик. Разбери эти новости. Для каждой строго по шаблону на русском, без markdown:\nНовость N:\n📰 Суть: 1-2 предложения\n📊 Влияние: Бычье/Медвежье/Нейтральное и почему\n🪙 Монеты: тикеры через запятую\n\nНовости:\n")
    for i, e in enumerate(batch): prompt += f"{i + 1}. [{get_moscow_time_from_entry(e)}] {html.unescape(getattr(e, 'title', '') or '')}. {clean_summary(getattr(e, 'summary', ''))[:200]}\n"
    links = " · ".join(f"<a href=\"{html.escape(getattr(e, 'link', ''), quote=True)}\">{i + 1}</a>" for i, e in enumerate(batch))
    text, _engine = await ask_ai_named(prompt, timeout=30, free_timeout=30)
    if text and has_cyr(text, 30): return trim_html(f"📰 <b>Дайджест новостей</b>\n\n{html.escape(text)}\n\n🔗 Источники: {links}", 4000)
    out = "📰 <b>Последние новости (кратко):</b>\n\n"
    for i, e in enumerate(batch): out += f"<b>{i + 1}.</b> {html.escape(await translate_text(html.unescape(getattr(e, 'title', '') or '')))}\n\n"
    return out + f"🔗 Источники: {links}"
async def news_watcher_loop():
    global last_news_link
    await asyncio.sleep(40)
    while True:
        try:
            entries = await get_news_entries()
            if entries:
                link = entries[0].link
                if last_news_link is None: last_news_link = link
                elif link != last_news_link:
                    last_news_link = link
                    state_touch(False)
                    await update_market_bias(force=True)
                    await check_news_conflicts_with_signals()
                    if bot_flags["news_enabled"] and active_chats:
                        text, img_url = await prepare_news()
                        if text:
                            img = await download_bytes(img_url) if img_url else None
                            kb = kb_nav([[InlineKeyboardButton(text="🗑 Удалить новость", callback_data="del_msg")]], back=False)
                            sent, *_ = await broadcast({"text": text, "photo": img, "kb": kb, "nav": []}, notif_type="news")
                            stats["news_sent"] += sent
        except Exception: pass
        next_timers["news_watcher"] = time.time() + 600
        await asyncio.sleep(600)
_news_alerted = set()
def _parse_news_verdicts(text):
    try:
        arr = json.loads(re.search(r"\[.*\]", text or "", re.S).group(0))
        out = {}
        for d in arr:
            coin = str(d.get("coin", "")).upper().strip()
            action = str(d.get("action", "")).upper().strip()
            impact = str(d.get("impact", "")).upper().strip()
            reason = str(d.get("reason", "")).strip()
            if coin and action in ("CLOSE", "HOLD") and impact in ("NONE", "WEAK", "STRONG") and has_cyr(reason, 5):
                out[coin] = {"action": action, "impact": impact, "reason": shorten(reason, 220)}
        return out or None
    except Exception: return None
async def check_news_conflicts_with_signals():
    nb, link = market_bias.get("bias", 0), market_bias.get("link")
    if nb == 0 or not link: return
    if len(_news_alerted) > 2000: _news_alerted.clear()
    targets = []
    for uid_str, u in list(_users().items()):
        if not get_user_notif_settings(uid_str)["signals"]: continue
        for coin, pos in (u.get("open_positions") or {}).items():
            if pos.get("waiting_entry") or pos.get("notified_close"): continue
            against = (nb == 1 and pos["side"] == "SHORT") or (nb == -1 and pos["side"] == "LONG")
            if against and (link, uid_str, coin) not in _news_alerted: targets.append((uid_str, coin, pos))
    if not targets: return
    coins = sorted({c for _, c, _ in targets})
    sides = {c: sorted({p["side"] for _, cc, p in targets if cc == c}) for c in coins}
    pos_txt = "; ".join(f"{c} ({'/'.join(sides[c])})" for c in coins)
    news_txt = (f"Заголовок: {market_bias.get('title_ru')}\nСуть: {market_bias.get('summary')}\nВлияние: {market_bias.get('impact')}\nУпомянутые монеты: {market_bias.get('coins')}")
    prompt = f"""Ты риск-менеджер крипто-трейдера. Вышла новость, общий фон: {'бычий' if nb == 1 else 'медвежий'}.
{news_txt}
Открытые позиции против фона: {pos_txt}.
Для КАЖДОЙ монеты реши, надо ли закрывать позицию.
Правила: CLOSE только если новость напрямую касается этой монеты или это рыночный шок. Иначе HOLD.
Верни СТРОГО JSON-массив без markdown: [{{"coin":"ETH","impact":"NONE или WEAK или STRONG","action":"CLOSE или HOLD","reason":"1 предложение по-русски"}}]"""
    text, _eng = await ask_ai_named(prompt, validate=lambda t: _parse_news_verdicts(t) is not None, timeout=22, free_timeout=25)
    verdicts = _parse_news_verdicts(text) if text else None
    if verdicts is None:
        blob = f"{market_bias.get('title_ru', '')} {market_bias.get('summary', '')} {market_bias.get('coins', '')}".upper()
        verdicts = {c: {"action": "CLOSE", "impact": "STRONG", "reason": "Монета упомянута в новости."} for c in coins if re.search(rf"\b{re.escape(c)}\b", blob)}
    for uid_str, coin, pos in targets:
        _news_alerted.add((link, uid_str, coin))
        v = verdicts.get(coin)
        if not v or v["action"] != "CLOSE" or v["impact"] != "STRONG": continue
        text = (f"🚨 <b>Закройте позицию: {html.escape(coin)} ({pos['side']})</b>\n\n📰 {html.escape(shorten(market_bias.get('title_ru', ''), 120))}\nФон: <b>{'Бычий 🟢' if nb == 1 else 'Медвежий 🔴'}</b>\n\n💡 {html.escape(v['reason'])}")
        kb = kb_nav([[InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")], [InlineKeyboardButton(text="❌ Закрыл вручную", callback_data=f"pos:close:{coin}")]], back=False)
        try:
            await _send_new(int(uid_str), text, kb, None)
            await asyncio.sleep(0.05)
        except Exception: pass

# =====================================================================
# ГРАФИК
# =====================================================================
def draw_chart(coin, tf_data, main_tf, is_long, entry, tps, sls):
    info = tf_data[main_tf]
    df = info["df"].tail(60).reset_index(drop=True)
    price = info["price"]
    w, h = 900, 540
    img = Image.new("RGB", (w, h), (14, 15, 20))
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
        font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
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
    for name, colr in (("ema20", (255, 205, 60)), ("ema50", (80, 150, 255))):
        draw.line([(pad_l + i * step + step / 2, y(v)) for i, v in enumerate(df[name].values)], fill=colr, width=2)
    vmax = df["volume"].max() or 1.0
    for i in range(n):
        x = pad_l + i * step + step / 2
        vh = (vol_bottom - vol_top) * (df["volume"].iloc[i] / vmax)
        color = (0, 120, 60) if df["close"].iloc[i] >= df["open"].iloc[i] else (140, 40, 40)
        draw.rectangle([x - cwid / 2, vol_bottom - vh, x + cwid / 2, vol_bottom], fill=color)
    recent_df = df.tail(50)
    swing_high, swing_low = recent_df["high"].max(), recent_df["low"].min()
    diff = swing_high - swing_low
    if diff > 0:
        fib_levels = {"0.236": swing_high - diff * 0.236, "0.382": swing_high - diff * 0.382, "0.500": swing_high - diff * 0.500, "0.618": swing_high - diff * 0.618, "0.786": swing_high - diff * 0.786}
        for lbl, lvl in fib_levels.items():
            yy = y(lvl)
            draw.line([(pad_l, yy), (w - pad_r, yy)], fill=(80, 80, 110), width=1)
            draw.text((pad_l + 4, yy - 14), f"Fib {lbl}", fill=(120, 120, 160), font=font)
    for lvl_p, lbl in ((info["sr"].get("support"), "SUP"), (info["sr"].get("resistance"), "RES")):
        if lvl_p:
            for xx in range(pad_l, w - pad_r, 10): draw.line([(xx, y(lvl_p)), (xx + 4, y(lvl_p))], fill=(130, 130, 150), width=1)
            draw.text((w - pad_r - 90, y(lvl_p) - 14), f"{lbl} {fmt(lvl_p)}", fill=(150, 150, 170), font=font)
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
    if info["patterns"]: draw.text((pad_l + cw - 200, 12), "Pattern: " + info["patterns"][0][2], fill=(255, 210, 120), font=font_b)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()

def local_verdict(setup, bt):
    if not setup["valid"]: return "WAIT"
    if bt is None: return "ENTER" if setup["strength"] >= CONF_FINAL + 4 else "WAIT"
    return "ENTER" if (bt_ok(bt) or bt["trades"] < BT_MIN_TRADES) and setup["strength"] >= CONF_FINAL else "WAIT"
def age_text(ts):
    sec = max(0, int(time.time() - ts))
    return f"{sec // 3600} ч {sec % 3600 // 60} мин назад" if sec >= 3600 else f"{sec // 60} мин назад"
def render_caption(res, uid=None):
    coin = html.escape(res["coin"])
    if res.get("price_only"): return fit_caption([(0, f"💰 <b>{coin}</b>: <code>{fmt(res['price'])}</code>"), (1, f"ℹ Свечи недоступны. Цена: {html.escape(res.get('source', ''))}."), (2, "Попробуйте позже.")])
    entry = res["entry"]
    def pct(p): return f"{(p - entry) / entry * 100:+.1f}%"
    s = get_user_risk_settings(uid) if uid else {"deposit": DEPOSIT, "risk_pct": RISK_PCT}
    risk_usdt = s["deposit"] * s["risk_pct"] / 100
    pos_usdt = risk_usdt / res["risk_dist"] * entry if res["risk_dist"] > 0 else 0.0
    verdict = "✅ <b>ВХОДИТЬ</b>" if res["verdict"] == "ENTER" else "⏳ <b>ЖДАТЬ</b>"
    emoji = "🟢" if res["is_long"] else "🔴"
    parts = []
    if res.get("stale"): parts.append((0, f"🕓 <i>Из архива: {age_text(res['ts'])} (свежие данные недоступны)</i>"))
    corr_warning = None
    if uid:
        open_pos = get_user_open_positions(uid)
        for op_coin, op_data in open_pos.items():
            if op_coin == res["coin"]: continue
            if op_data["side"] == res["side"]:
                for group in CORRELATION_GROUPS:
                    if op_coin in group and res["coin"] in group: corr_warning = f"⚠️ <b>Риск:</b> У вас уже есть {op_data['side']} по {op_coin}. {res['coin']} коррелирует. Не удваивайте риск!"; break
            if corr_warning: break
    if corr_warning: parts.append((0, corr_warning))
    parts += [(0, f"{emoji} <b>{res['side']} · {coin}</b>   {verdict}"), (1, f"📌 {res['type']} · Conf <b>{res['strength']}%</b> · {res['regime']}"), (1, make_strength_bar(res["strength"]))]
    if res.get("fake_breakout"): parts.append((1, "⚠️ <b>Осторожно, ложный пробой</b>"))
    parts += [(2, f"📰 {html.escape(res['news_status'])}"), (3, f"🌍 BTC {res['btc_txt']}" + (f" · F&G {res['fng']}" if res.get("fng") is not None else "")), (7, ""), (0, f"📥 Вход <b>(только ЛИМИТ)</b>: <code>{fmt(entry)}</code>"), (1, f"💰 Цена сейчас: <code>{fmt(res['price'])}</code>"), (7, "")]
    parts += [(0, f"🎯 TP{i + 1} <code>{fmt(p)}</code> <i>{pct(p)}</i>") for i, p in enumerate(res["tps"])]
    parts += [(0, f"🛡 SL{i + 1} <code>{fmt(p)}</code> <i>{pct(p)}</i>") for i, p in enumerate(res["sls"])]
    parts.append((7, ""))
    parts.append((2, f"⚖ {res['leverage']} · позиция ${pos_usdt:.0f} · риск ${risk_usdt:.0f}"))
    if res.get("liq_x20"): parts.append((3, f"⚠ <i>При плече x20 ликвидация: <code>{fmt(res['liq_x20'])}</code></i>"))
    if res.get("bt"): parts.append((3, f"🧪 Бэктест: {res['bt']['trades']} сд · TP1 {res['bt']['winrate']}% · {res['bt']['avg_r']:+.2f}R"))
    if res.get("comment"): parts.append((4, f"💬 {html.escape(shorten(res['comment'], 170))}"))
    parts.append((8, "⚠ <i>Не фин. рекомендация</i>"))
    return fit_caption(parts, 1000)
def render_details(res):
    if res.get("price_only"): return render_caption(res)
    parts = [(0, f"📋 <b>Детали · {html.escape(res['coin'])}</b>"), (7, "")]
    if res.get("reasons"): parts.append((1, "📈 <b>Почему:</b>")); parts += [(1 if i < 3 else 3, f"• {html.escape(r)}") for i, r in enumerate(res["reasons"])]
    if res.get("patterns"): parts.append((2, "🕯 <b>Свечи:</b> " + html.escape("; ".join(res["patterns"]))))
    if res.get("squeeze"): parts.append((4, "🎯 Сжатие волатильности"))
    if res.get("notes"): parts.append((3, "⚙ Поправки: " + html.escape("; ".join(res["notes"]))))
    if res.get("bt"): parts.append((2, f"🧪 Бэктест 4h ({res['bt']['days']} дн.): сделок {res['bt']['trades']}, TP1 {res['bt']['winrate']}%, ср. {res['bt']['avg_r']:+.2f}R, PF {res['bt']['pf']}"))
    if res.get("comment"): parts.append((1, f"💬 <b>Аналитик ({html.escape(res.get('risk_lvl', 'средний'))} риск):</b> {html.escape(res['comment'])}"))
    parts.append((4, "⚠ <i>Входите ТОЛЬКО лимитным ордером.</i>"))
    return fit_caption(parts, 1000)

async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    info = tf_data["4h"]
    is_long = setup["is_long"]
    entry, tps, sls, risk_dist, leverage, near, liq_x20 = calc_levels_and_risk(info, is_long, setup["type"])
    verdict, comment, risk_lvl, engine = "WAIT", "", "средний", ""
    if not setup["valid"]: comment = setup["reject"]
    else:
        ai = await ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data)
        if ai: verdict, comment, risk_lvl, engine = ai["verdict"], ai["comment"], ai["risk"], ai.get("engine", "")
        else: verdict, comment = local_verdict(setup, bt), "ИИ недоступен: решение по правилам модели."
        if bt and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt): verdict, comment = "WAIT", "История слабая. " + comment
        if verdict == "ENTER" and near: comment = f"{comment} ⚠ {near}".strip()
    try: img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception: img = None
    res = {"coin": coin, "ts": time.time(), "source": source, "img": img, "is_long": is_long, "side": "LONG" if is_long else "SHORT", "verdict": verdict, "type": setup["type"], "strength": setup["strength"], "regime": info["regime"], "news_status": setup["news_status"], "btc_txt": ctx.get("btc_txt", "нет данных"), "fng": ctx.get("fng"), "price": info["price"], "entry": entry, "tps": tps, "sls": sls, "risk_dist": risk_dist, "leverage": leverage, "reasons": setup["reasons"], "patterns": setup["patterns"], "notes": setup["notes"], "squeeze": info["squeeze"], "bt": bt, "comment": comment, "risk_lvl": risk_lvl, "engine": engine, "liq_x20": liq_x20, "fake_breakout": setup.get("fake_breakout", False)}
    res["text"] = render_caption(res)
    return res
def fetch_error_text(coin):
    errs = fetch_errors.get(coin, [])
    tried = ", ".join(e["name"] for e in EXCHANGES)
    return f"Монета <b>{html.escape(coin)}</b> не найдена ни на одной бирже ({tried})." + ("\n\n<i>Причины: " + html.escape("; ".join(errs[-4:])) + "</i>" if errs else "") + "\n\nПроверьте тикер."
async def analyze_coin(coin, uid=None, force=False):
    cached = analysis_cache.get(coin)
    if cached and not force and time.time() - cached[0] < cached[1]: return cached[2], None
    try:
        tf_data, source = await get_market_data(coin)
        ctx = await get_context()
        funding = await get_funding(coin)
        user_conf = CONF_FINAL
        if uid: user_conf = float(_users().get(str(uid), {}).get("conf_final", CONF_FINAL))
        setup = await evaluate_setup(coin, tf_data, ctx, funding, onchain_change=await get_onchain_context(), conf_threshold=user_conf)
        bt = await get_backtest(coin)
        res = await asyncio.wait_for(build_analysis(coin, tf_data, setup, bt, ctx, source), timeout=80)
        analysis_cache[coin] = (time.time(), 180, res)
        save_analysis_db(coin, res)
        if len(analysis_cache) > 60:
            for k in sorted(analysis_cache, key=lambda x: analysis_cache[x][0])[:20]: analysis_cache.pop(k, None)
        return res, None
    except LookupError: pass
    except Exception as e: log.error(f"Ошибка анализа {coin}: {e}", exc_info=True)
    old = load_analysis_db(coin)
    if old and time.time() - old["ts"] < 12 * 3600:
        res = dict(old["data"], stale=True, img=None)
        res["text"] = render_caption(res)
        return res, None
    price = await get_simple_price(coin)
    if price: return {"coin": coin, "price": price, "price_only": True, "ts": time.time(), "img": None, "source": "публичный API"}, None
    return None, "🚫 " + fetch_error_text(coin)
def remember_res(uid, coin, res):
    d = user_res.setdefault(uid, {})
    d[coin] = (time.time(), res)
    for k in sorted(d, key=lambda c: d[c][0])[:-6]: d.pop(k, None)
def get_user_res(uid, coin):
    v = user_res.get(uid, {}).get(coin)
    return v[1] if v and time.time() - v[0] < 1800 else None

# =====================================================================
# СИГНАЛЫ
# =====================================================================
def get_active_signals():
    hist = load_signal_history()
    now = time.time()
    active, seen = [], set()
    for x in sorted(hist, key=lambda k: k.get("ts", 0), reverse=True):
        key = f"{x['coin']}_{x['side']}"
        if x.get("status") in ("active", "in_trade") and key not in seen:
            if x.get("status") == "active" and (now - x.get("ts", 0) > 12 * 3600): continue
            seen.add(key)
            active.append(x)
    return active
async def get_scan_universe():
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
        qv = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    except Exception: return POPULAR_COINS[:SCAN_MAX_COINS]
    uni = []
    for c in POPULAR_COINS:
        for cand in SYMBOL_ALIASES.get(c, [c]):
            v = qv.get(f"{cand}USDT", 0)
            if v >= MIN_QV24: uni.append((c, v)); break
    uni.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in uni[:SCAN_MAX_COINS]] or POPULAR_COINS[:SCAN_MAX_COINS]
def signals_overview():
    active_signals = get_active_signals()
    cnt = {t: sum(1 for x in active_signals if x.get("type") == t) for t in ("Краткосрочный", "Среднесрочный", "Долгосрочный")}
    text = f"🔥 <b>Актуальные сигналы</b>\n\n📊 <b>Кратко о рынке:</b>\n{html.escape(shorten(market_bias.get('impact', 'Нейтральный фон'), 500))}\n\nВыберите категорию:"
    rows = [[InlineKeyboardButton(text=f"📋 Все сигналы ({len(active_signals)})", callback_data="sig_list:Все")], [InlineKeyboardButton(text=f"⚡ Краткосрочный ({cnt['Краткосрочный']})", callback_data="sig_list:Краткосрочный")], [InlineKeyboardButton(text=f"⏳ Среднесрочный ({cnt['Среднесрочный']})", callback_data="sig_list:Среднесрок")], [InlineKeyboardButton(text=f"🛡 Долгосрочный ({cnt['Долгосрочный']})", callback_data="sig_list:Долгосрок")], [InlineKeyboardButton(text="⚙️ По типу торговли", callback_data="sig_modes")], [InlineKeyboardButton(text="🔄 Обновить меню", callback_data="last_signals")]]
    return text, kb_nav(rows)
def recent_results(n=20):
    hist = sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True)
    return [x for x in hist if x.get("status") in ("win", "loss")][:n]
def adaptive_conf_bonus():
    r = recent_results(20)
    if len(r) < 8: return 0
    wr = sum(1 for x in r if x["status"] == "win") / len(r)
    return 10 if wr < 0.35 else 6 if wr < 0.45 else 0
def streak_pause():
    r = recent_results(3)
    return len(r) == 3 and all(x["status"] == "loss" for x in r) and time.time() - r[0]["ts"] < 6 * 3600
def coin_recently_lost(coin, hours=24):
    now = time.time()
    return any(x["coin"] == coin and x.get("status") == "loss" and now - x.get("ts", 0) < hours * 3600 for x in load_signal_history())
def set_blackout(secs):
    global news_blackout_until
    news_blackout_until = time.time() + secs
async def btc_shock():
    try:
        tf, _ = await get_market_data_light("BTC", ("1h",))
        last = tf["1h"]["df"].iloc[-1]
        return (last["high"] - last["low"]) > 2.5 * last["atr"]
    except Exception: return False
async def check_macro_calendar():
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: return False
            events = await r.json(content_type=None)
        now = datetime.now(timezone.utc)
        for event in events:
            if event.get("impact") == "High" and event.get("country") == "USD":
                event_time = datetime.fromisoformat(event["date"].replace("Z", "+00:00"))
                diff = (event_time - now).total_seconds()
                if -1800 < diff < 1800: return True
    except Exception: pass
    return False

async def scan_once(send=True):
    if scan_lock.locked(): return []
    async with scan_lock:
        t0 = time.time()
        if time.time() < news_blackout_until: return []
        if streak_pause(): return []
        if await check_macro_calendar():
            set_blackout(3600)
            await notify_admin("🏦 <b>Макро-новость (ФРС/CPI)!</b>\nСканер на паузе 1 час.")
            return []
        if await btc_shock():
            set_blackout(3600)
            await notify_admin("⚠ Шок BTC: сканер на паузе 1 час")
            return []
        if time.time() - market_bias["ts"] > 900: await update_market_bias()
        ctx = await get_context(force=True)
        onchain = await get_onchain_context()
        universe = await get_scan_universe()
        sem = asyncio.Semaphore(2)
        stage1 = []
        async def s1(coin):
            if time.time() < scan_skip.get(coin, 0): return
            async with sem:
                try: tf2, _ = await get_market_data_light(coin, ("4h", "1d"))
                except LookupError: scan_skip[coin] = time.time() + 6 * 3600; return
                except Exception: return
                q = quick_check(tf2)
                if q: stage1.append((coin, tf2, q[1]))
                await asyncio.sleep(0.2)
        await asyncio.gather(*[s1(c) for c in universe])
        stage1 = sorted(stage1, key=lambda x: x[2], reverse=True)[:30]
        stage2 = []
        async def s2(item):
            coin, tf2, _ = item
            if coin_recently_lost(coin): return
            async with sem:
                tf_data = dict(tf2)
                try:
                    extra, _src = await get_market_data_light(coin, ("1h", "15m"))
                    tf_data.update(extra)
                except Exception: pass
                setup = await evaluate_setup(coin, tf_data, ctx, await get_funding(coin), scan=True, onchain_change=onchain, conf_threshold=CONF_FINAL + adaptive_conf_bonus())
                if setup["valid"]: stage2.append((coin, tf_data, setup))
        await asyncio.gather(*[s2(i) for i in stage1])
        bt_pass = 0
        final = []
        sem3 = asyncio.Semaphore(2)
        async def s3(item):
            nonlocal bt_pass
            coin, tf_data, setup = item
            async with sem3:
                bt = await get_backtest(coin)
                if bt is not None and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt): return
                bt_pass += 1
                try: res = await build_analysis(coin, tf_data, setup, bt, ctx, "scan")
                except Exception: return
                if res["verdict"] == "ENTER":
                    final.append(res)
                    analysis_cache[coin] = (time.time(), 1200, res)
                    save_analysis_db(coin, res)
        await asyncio.gather(*[s3(i) for i in stage2])
        final.sort(key=lambda r: r["strength"], reverse=True)
        last_scan_info.update(ts=int(time.time()), stage1=len(stage1), stage2=len(stage2), backtest_ok=bt_pass, final=len(final), secs=int(time.time() - t0))
        state_touch(False)
        log.info(f"Сканер: {len(universe)} → {len(stage1)} → {len(stage2)} → тест {bt_pass} → итог {len(final)} за {last_scan_info['secs']} с")
        if send and active_chats:
            now = time.time()
            hist = load_signal_history()
            busy = {f"{x['coin']}:{x['side']}" for x in hist if x.get("status") in ("active", "in_trade")}
            new_items = []
            used_groups = set()
            for r in final:
                key = f"{r['coin']}:{r['side']}"
                if key in busy or now - signal_sent_at.get(f"{r['coin']}:{'L' if r['is_long'] else 'S'}", 0) < SIGNAL_COOLDOWN: continue
                if len(new_items) >= 3 or abs(r["entry"] - r["price"]) / r["price"] > 0.03: continue
                grp = next((i for i, g in enumerate(CORRELATION_GROUPS) if r["coin"] in g), None)
                if grp is not None:
                    if (grp, r["side"]) in used_groups: continue
                    used_groups.add((grp, r["side"]))
                signal_sent_at[f"{r['coin']}:{'L' if r['is_long'] else 'S'}"] = now
                new_items.append(r)
                hist.append({"ts": now, "expire_ts": now + 6 * 3600, "coin": r["coin"], "side": r["side"], "type": r["type"], "strength": r["strength"], "entry": r["entry"], "tp1": r["tps"][0], "tp2": r["tps"][1], "tp3": r["tps"][2], "sl1": r["sls"][0], "sl2": r["sls"][1], "status": "active", "max_tp_hit": 0, "hit_sl": 0, "text": r["text"]})
            if new_items:
                save_signal_history(hist)
                stats["signals"] += len(new_items)
                for r in new_items:
                    kb = coin_keyboard(r["coin"])
                    for cid in list(active_chats):
                        if not get_user_notif_settings(cid)["signals"]: continue
                        try:
                            await _send_new(cid, r["text"], kb, r.get("img"))
                            await asyncio.sleep(0.05)
                        except Exception: pass
                    asyncio.create_task(send_webhook_all({"event": "new_signal", "coin": r["coin"], "side": r["side"], "entry": r["entry"], "tp1": r["tps"][0], "sl1": r["sls"][0]}))
        if send: await notify_admin(f"🔍 <b>Скан завершён</b> за {last_scan_info['secs']} с\nМонет: {len(universe)} → {len(stage1)} → {len(stage2)}\nБэктест: {bt_pass} · ✅ <b>Итог: {len(final)}</b>")
        gc.collect()
        return final
async def auto_trade_for_users(r): return
def simulate_signal(x, rows, now=None):
    now = now or time.time()
    is_long = x["side"] == "LONG"
    entry, tp1, tp2, tp3, sl = x["entry"], x["tp1"], x["tp2"], x["tp3"], x["sl1"]
    t0 = x["ts"] * 1000
    state, max_tp = "waiting", 0
    for ts, _o, h, l, _c, _v in rows:
        if ts < t0: continue
        if state == "waiting":
            if (is_long and h >= tp1 and l > entry) or ((not is_long) and l <= tp1 and h < entry): return "missed", 0, 0
            if (l <= entry) if is_long else (h >= entry):
                state = "in_trade"
                if (l <= sl) if is_long else (h >= sl): return "loss", 0, 1
            continue
        if (l <= sl) if is_long else (h >= sl): return ("win" if max_tp >= 1 else "loss"), max_tp, 1
        lvl = (3 if h >= tp3 else 2 if h >= tp2 else 1 if h >= tp1 else 0) if is_long else (3 if l <= tp3 else 2 if l <= tp2 else 1 if l <= tp1 else 0)
        if lvl > max_tp:
            max_tp = lvl
            sl = entry if max_tp == 1 else tp1
            if max_tp == 3: return "win", 3, 0
    if state == "waiting": return ("expired", 0, 0) if now > x.get("expire_ts", 0) else ("active", 0, 0)
    if now > x["ts"] + 7 * 86400: return ("win" if max_tp >= 1 else "expired"), max_tp, 0
    return "in_trade", max_tp, 0
async def global_signal_monitor_loop():
    await asyncio.sleep(120)
    while True:
        try:
            hist = load_signal_history()
            changed = False
            rows_cache = {}
            for x in hist:
                if x.get("status") not in ("active", "in_trade"): continue
                coin = x["coin"]
                if coin not in rows_cache:
                    rows_cache[coin] = await fetch_history(coin, "15m", 400, min_rows=40)
                    await asyncio.sleep(0.2)
                rows = rows_cache[coin]
                if not rows: continue
                status, max_tp, hit_sl = simulate_signal(x, rows)
                if (status, max_tp, hit_sl) != (x["status"], x.get("max_tp_hit", 0), x.get("hit_sl", 0)):
                    prev = x["status"]
                    x.update(status=status, max_tp_hit=max_tp, hit_sl=hit_sl)
                    changed = True
                    if status in ("win", "loss") and prev != status:
                        await notify_admin(f"📊 <b>{coin} {x['side']} закрыт:</b> {'✅ WIN' if status == 'win' else '❌ LOSS'} (TP: {max_tp})")
                        asyncio.create_task(send_webhook_all({"event": f"closed_{status}", "coin": coin, "side": x["side"]}))
            if changed: save_signal_history(hist)
            await update_saved_signals_status(rows_cache)
        except Exception: pass
        next_timers["global_signals"] = time.time() + 300
        await asyncio.sleep(300)
async def update_saved_signals_status(rows_cache):
    changed = False
    for uid, u in list(_users().items()):
        for s in u.get("saved_signals", []):
            if s.get("status") != "active" or not s.get("entry"): continue
            coin = s["coin"]
            rows = rows_cache.get(coin) or await fetch_history(coin, "15m", 400, min_rows=40)
            rows_cache[coin] = rows
            if not rows: continue
            filled = any((l <= s["entry"]) if s.get("is_long") else (h >= s["entry"]) for ts, _o, h, l, _c, _v in rows if ts >= s["ts"] * 1000)
            if filled: s["status"], changed = "in_trade", True
            elif time.time() > s.get("expire_ts", 0): s["status"], changed = "expired", True
    if changed: state_touch()
async def anomaly_scan_once():
    j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
    vol = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    coins = [c for c in POPULAR_COINS[:150] if vol.get(f"{c}USDT", 0) >= 10_000_000]
    found = []
    sem = asyncio.Semaphore(4)
    async def check(coin):
        async with sem:
            try: k = await http_json("https://data-api.binance.vision/api/v3/klines", params={"symbol": f"{coin}USDT", "interval": "1m", "limit": 22}, timeout=6)
            except Exception: return
        if len(k) < 22: return
        closed = k[:-1]
        last, base = closed[-1], closed[:-1]
        avg = float(np.mean([float(x[7]) for x in base]))
        qv = float(last[7])
        if avg > 0 and qv / avg >= 10 and qv >= 250_000: found.append((coin, qv, qv / avg, (float(last[4]) / float(last[1]) - 1) * 100))
    await asyncio.gather(*[check(c) for c in coins])
    now = time.time()
    fresh = sorted([a for a in found if now - anomaly_sent_at.get(a[0], 0) > 2 * 3600], key=lambda a: a[2], reverse=True)[:3]
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 Удалить сообщение", callback_data="del_msg")]])
    for coin, qv, ratio, chg in fresh:
        anomaly_sent_at[coin] = now
        state_touch(False)
        text = f"🚨 <b>Аномальный объём: {coin}</b>\nОборот за минуту: <b>${qv:,.0f}</b> (×{ratio:.0f} к среднему)\nЦена за минуту: <b>{chg:+.2f}%</b> {'🟢' if chg >= 0 else '🔴'}"
        await broadcast({"text": text, "kb": kb, "clean": False}, notif_type="anomalies")
async def anomaly_scanner_loop():
    await asyncio.sleep(90)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats: await anomaly_scan_once()
        except Exception: pass
        next_timers["anomaly"] = time.time() + 180
        await asyncio.sleep(180)
async def scanner_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats: await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner: {e}", exc_info=True)
            await notify_admin(f"🚨 <b>Ошибка сканера:</b>\n<code>{html.escape(str(e))}</code>")
        next_timers["scanner"] = time.time() + SCAN_INTERVAL
        await asyncio.sleep(SCAN_INTERVAL)

async def send_position_update(uid, coin, pos):
    if pos.get("notified_close") or pos.get("waiting_entry"): return
    price = await get_simple_price(coin)
    if not price: return
    entry, side = pos["entry_price"], pos["side"]
    sign = 1 if side == "LONG" else -1
    pnl = sign * (price - entry) / entry * 100
    advice = ""
    trend_1h_long, trend_4h_long = True, True
    try:
        tf_data, _ = await get_market_data_light(coin, ("1h", "4h"))
        trend_1h_long = tf_data["1h"]["long_prob"] >= 50
        trend_4h_long = tf_data["4h"]["long_prob"] >= 50
    except Exception: pass
    is_trend_against_1h = (side == "LONG" and not trend_1h_long) or (side == "SHORT" and trend_1h_long)
    is_trend_against_4h = (side == "LONG" and not trend_4h_long) or (side == "SHORT" and trend_4h_long)
    if pos.get("hit_tp2"): advice = "🔥 <b>Сделка в отличном плюсе.</b> Трейлинг-стоп защищает вашу прибыль. Держим до TP3."
    elif pos.get("hit_tp1"):
        if is_trend_against_1h: advice = "⚠️ <b>TP1 взят, но тренд на 1h развернулся против нас.</b> Бот подтягивает трейлинг-стоп."
        else: advice = "✅ <b>TP1 взят.</b> Стоп в безубытке. Включен трейлинг. Держим до TP2."
    elif pnl > 1.5: advice = "🛡 <b>Сделка в плюсе!</b> Скоро бот переведет стоп в безубыток (при 1.5%)."
    elif is_trend_against_4h and pnl < 0: advice = "🚨 <b>Глобальный тренд (4h) против вас, PnL в минусе.</b> Высокий риск!"
    elif is_trend_against_1h and pnl < 0: advice = "⚠️ <b>Тренд на 1h против вас.</b> Подтяните стоп-лосс поближе."
    else: advice = "⏳ <b>Тренд в силе.</b> Держим позицию по плану, ждем отработки целей."
    text = f"📊 <b>Отчет по сделке: {coin} ({side})</b>\n\nВход: <code>{fmt(entry)}</code> → Сейчас: <code>{fmt(price)}</code>\nТекущий PnL: <b>{pnl:+.2f}%</b>\nТекущий стоп: <code>{fmt(pos['sl1'])}</code>\n\n💡 <b>Статус:</b> {advice}"
    kb = kb_nav([[InlineKeyboardButton(text="📈 Открыть график", callback_data=f"c:{coin}")], [InlineKeyboardButton(text="❌ Закрыл вручную", callback_data=f"pos:close:{coin}")]], back=False)
    try: await _send_new(uid, text, kb, None)
    except Exception: pass

async def position_update_loop():
    await asyncio.sleep(120)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                if not get_user_notif_settings(uid_str)["pos_updates"]: continue
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    try: await send_position_update(int(uid_str), coin, pos)
                    except Exception: pass
                    await asyncio.sleep(0.5)
        except Exception: pass
        next_timers["pos_updates"] = time.time() + (20 * 60)
        await asyncio.sleep(20 * 60)

async def position_monitor_loop():
    await asyncio.sleep(50)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    try: await check_position(int(uid_str), coin, pos)
                    except Exception: pass
                    await asyncio.sleep(0.3)
        except Exception: pass
        next_timers["positions"] = time.time() + 320
        await asyncio.sleep(320)

async def check_position(user_id, coin, pos):
    if pos.get("notified_close"): return
    price = await get_simple_price(coin)
    if not price: return
    side, entry = pos["side"], pos["entry_price"]
    sign = 1 if side == "LONG" else -1
    pnl = sign * (price - entry) / entry * 100
    if pos.get("waiting_entry"):
        hit_entry = (sign * (price - entry)) <= 0
        if hit_entry:
            pos["waiting_entry"] = False
            pos["entered_at"] = time.time()
            set_user_open_position(user_id, coin, pos)
            text = f"🎯 <b>Лимитный ордер сработал: {coin}</b>\n\nНаправление: <b>{side}</b>\nЦена входа: <code>{fmt(entry)}</code>\n🚀 <b>Сделка активирована!</b> Бот начинает полное сопровождение."
            kb = kb_nav([[InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")]], back=False)
            try: await _send_new(user_id, text, kb, None)
            except Exception: pass
            return
        elif time.time() - pos.get("created_at", time.time()) > 24 * 3600:
            set_user_open_position(user_id, coin, None)
            try: await _send_new(user_id, f"⌛ <b>Ордер отменен: {coin}</b>\nЦена не дошла до входа за 24 часа.", kb_nav(back=False), None)
            except Exception: pass
        return
    if "hit_tp1" not in pos: pos["hit_tp1"] = False
    if "hit_tp2" not in pos: pos["hit_tp2"] = False
    if "hit_tp3" not in pos: pos["hit_tp3"] = False
    current_sl = pos["sl1"]
    if not pos["hit_tp1"] and pnl >= 1.5 and current_sl != entry:
        pos["sl1"] = entry
        set_user_open_position(user_id, coin, pos)
        await _send_new(user_id, f"🛡 <b>Защита прибыли: {coin}</b>\n\nЦена ушла в плюс на <b>{pnl:.2f}%</b>.\nСтоп-лосс передвинут в безубыток (Entry).", kb_nav(back=False), None)
        current_sl = entry
    if pos["hit_tp1"] and not pos["hit_tp3"]:
        if side == "LONG":
            new_sl = price * 0.985
            if new_sl > current_sl and new_sl > entry:
                pos["sl1"] = new_sl
                set_user_open_position(user_id, coin, pos)
                current_sl = new_sl
        elif side == "SHORT":
            new_sl = price * 1.015
            if new_sl < current_sl and new_sl < entry:
                pos["sl1"] = new_sl
                set_user_open_position(user_id, coin, pos)
                current_sl = new_sl
    if pnl <= -5.0 and not pos.get("notified_urgent"):
        text = f"🚨 <b>СРОЧНО! {coin} идет против вас!</b>\nУбыток: <b>{pnl:+.2f}%</b>. Будьте готовы к стопу."
        await _send_new(user_id, text, kb_nav(back=False), None)
        pos["notified_urgent"] = True
        set_user_open_position(user_id, coin, pos)
    tp1, tp2, tp3 = pos["tp1"], pos["tp2"], pos["tp3"]
    hit_tp1 = sign * (price - tp1) >= 0
    hit_tp2 = sign * (price - tp2) >= 0
    hit_tp3 = sign * (price - tp3) >= 0
    hit_sl = sign * (price - current_sl) <= 0
    reason, urgency = None, "info"
    if hit_sl:
        if current_sl >= entry: reason, urgency = f"🛑 Сработал Трейлинг-стоп на уровне <code>{fmt(current_sl)}</code>. Вы зафиксировали профит!", "success"
        else: reason, urgency = "🛑 Сработал стоп-лосс. Позиция закрыта.", "danger"
        pos["notified_close"] = True
    elif hit_tp3 and not pos["hit_tp3"]:
        reason, urgency = "🏆 Достигнут <b>TP3</b>! Забираем весь профит.", "success"
        pos["hit_tp3"] = True
        pos["notified_close"] = True
    elif hit_tp2 and not pos["hit_tp2"]:
        reason, urgency = "✅ Достигнут <b>TP2</b>! Фиксируем 30%. Стоп подтягивается.", "success"
        pos["hit_tp2"] = True
        pos["sl1"] = tp1 if side == "LONG" else tp1
        set_user_open_position(user_id, coin, pos)
    elif hit_tp1 and not pos["hit_tp1"]:
        reason, urgency = "✅ Достигнут <b>TP1</b>! Фиксируем 50%. Стоп переведен в безубыток.", "success"
        pos["hit_tp1"] = True
        pos["sl1"] = entry
        set_user_open_position(user_id, coin, pos)
    if pos.get("hit_tp2") and not pos.get("notified_close"):
        try:
            tf_data, _ = await get_market_data_light(coin, ("1h", "4h"))
            is_global_against = (side == "LONG" and tf_data["4h"]["long_prob"] < 50) or (side == "SHORT" and tf_data["4h"]["long_prob"] > 50)
            if is_global_against:
                reason, urgency = "⚠️ <b>Глобальный тренд развернулся!</b> Рекомендуем закрыть остаток.", "danger"
                pos["notified_close"] = True
        except Exception: pass
    if reason:
        text = f"{'🏆' if urgency == 'success' else '🛑'} <b>Сопровождение: {coin} ({side})</b>\n\nВход: <code>{fmt(entry)}</code> | Сейчас: <code>{fmt(price)}</code>\nPnL: <b>{pnl:+.2f}%</b>\n\n{reason}"
        kb = kb_nav([[InlineKeyboardButton(text="📈 Статус", callback_data=f"pos:status:{coin}")], [InlineKeyboardButton(text="❌ Закрыть остаток", callback_data=f"pos:close:{coin}")]], back=False)
        try: await _send_new(user_id, text, kb, None)
        except Exception: pass
        set_user_open_position(user_id, coin, pos)

async def broadcast(payload, only_ids=None, notif_type=None):
    targets = list(only_ids) if only_ids else list(active_chats)
    if notif_type: targets = [cid for cid in targets if get_user_notif_settings(cid).get(notif_type, True)]
    sent = failed = 0
    dead = []
    for cid in targets:
        for _ in range(2):
            try:
                if payload.get("clean", True):
                    await present(cid, payload["text"], payload.get("kb"), payload.get("photo") if vis_len(payload["text"]) <= 1024 else None)
                    if payload.get("nav") is not None: nav_reset(cid, *payload["nav"])
                else: await _send_new(cid, payload["text"], payload.get("kb"), None)
                sent += 1
                break
            except TelegramRetryAfter as e: await asyncio.sleep(e.retry_after + 1)
            except (TelegramForbiddenError, TelegramNotFound):
                dead.append(cid)
                break
            except Exception:
                failed += 1
                break
        await asyncio.sleep(0.04)
    if dead:
        for cid in dead: active_chats.discard(cid)
        await asyncio.to_thread(mark_users_inactive, dead)
    stats["broadcasts"] += 1
    state_touch(False)
    return sent, failed, len(dead)

async def notify_admin(text):
    if not admin_bot: return
    for aid in ADMIN_IDS:
        try: await admin_bot.send_message(aid, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception: pass
async def telegram_backup(reason="auto"):
    global backup_dirty, last_backup_ts
    if not admin_bot or not ADMIN_IDS: return False
    data = (await asyncio.to_thread(flush_state)).encode("utf-8")
    ok = False
    caption = f"💾 БЭКАП · юзеров {len(STATE['users'])} · сигналов {len(STATE['signal_history'])} · анализов {len(STATE['analysis_db'])} · {reason} · {datetime.now().strftime('%d.%m %H:%M')}"
    for aid in ADMIN_IDS:
        try:
            msg = await admin_bot.send_document(aid, BufferedInputFile(data, "bot_state.json"), caption=caption, disable_notification=True)
            try:
                await admin_bot.unpin_all_chat_messages(aid)
                await admin_bot.pin_chat_message(aid, msg.message_id, disable_notification=True)
            except Exception: pass
            old = last_backup_msg.get(aid)
            if old:
                try: await admin_bot.delete_message(aid, old)
                except Exception: pass
            last_backup_msg[aid] = msg.message_id
            ok = True
        except Exception: pass
    if ok: backup_dirty, last_backup_ts = False, time.time()
    return ok
async def telegram_restore():
    if not admin_bot or not ADMIN_IDS: return 0
    for aid in ADMIN_IDS:
        try:
            pm = getattr(await admin_bot.get_chat(aid), "pinned_message", None)
            doc = getattr(pm, "document", None) if pm else None
            if not doc or not (doc.file_name or "").endswith(".json"): continue
            f = await admin_bot.get_file(doc.file_id)
            data = json.loads((await admin_bot.download_file(f.file_path)).read().decode("utf-8"))
            n = restore_state(data)
            last_backup_msg[aid] = pm.message_id
            if n: return n
        except Exception: pass
    return 0
async def backup_loop():
    await asyncio.sleep(60)
    while True:
        try:
            if backup_dirty and time.time() - last_backup_ts > 120: await telegram_backup("авто")
        except Exception: pass
        next_timers["backup"] = time.time() + 30
        await asyncio.sleep(30)

# =====================================================================
# КЛАВИАТУРЫ
# =====================================================================
def get_main_keyboard():
    B = InlineKeyboardButton
    base_url = os.getenv("RENDER_EXTERNAL_URL", "https://твой-бот.onrender.com")
    web_app_url = f"{base_url}/miniapp"
    return InlineKeyboardMarkup(inline_keyboard=[
        [B(text="🚀 Открыть Дашборд (Web App)", web_app=WebAppInfo(url=web_app_url))],
        [B(text="📰  Новости", callback_data="news_menu"), B(text="🧐  Графики", callback_data="interesting_menu")],
        [B(text="🔥  Последние сигналы", callback_data="last_signals"), B(text="📊  Статистика", callback_data="global_stats")],
        [B(text="⭐  Избранное", callback_data="sig:menu"), B(text="💼  Мой Портфель", callback_data="portfolio")],
        [B(text="⚙️ Настройки", callback_data="settings_menu"), B(text="📥  Экспорт CSV", callback_data="export_csv")],
        [B(text="🔍  Поиск монеты", callback_data="search")],
    ])

def coin_grid(coins, cb_prefix, page, per=8, page_cb="search_page"):
    B = InlineKeyboardButton
    chunk = coins[page * per:(page + 1) * per]
    rows = []
    for i in range(0, len(chunk), 2): rows.append([B(text=f"• {c}", callback_data=f"{cb_prefix}{c}") for c in chunk[i:i + 2]])
    total = (len(coins) + per - 1) // per
    nav = []
    if page > 0: nav.append(B(text="⬅", callback_data=f"{page_cb}:{page - 1}"))
    nav.append(B(text=f"{page + 1}/{total}", callback_data="ignore"))
    if page < total - 1: nav.append(B(text="➡", callback_data=f"{page_cb}:{page + 1}"))
    return rows + [nav]
def coin_keyboard(coin, view="main"):
    B = InlineKeyboardButton
    if view == "main":
        rows = [[B(text="🔄 Обновить", callback_data=f"r:{coin}"), B(text="💾 Сохранить", callback_data=f"sig:quick_save:{coin}")], [B(text="📋 Детали", callback_data=f"view:details:{coin}")], [B(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")], [B(text="🗑 Скрыть сообщение", callback_data="del_msg")]]
    else: rows = [[B(text="⬅ К сигналу", callback_data=f"view:main:{coin}")]]
    return kb_nav(rows)
def fav_keyboard(uid):
    B = InlineKeyboardButton
    saved = get_user_saved_signals(uid)
    rows, row = [], []
    for s in saved:
        icon = {"in_trade": "📈", "expired": "⌛"}.get(s.get("status"), "⭐")
        row.append(B(text=f"{icon} {s['coin']}", callback_data=f"sig:view:{s['coin']}"))
        if len(row) == 2: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([B(text="➕ Добавить новый", callback_data="sig:save_new")])
    if saved: rows.append([B(text="🗑 Удалить", callback_data="sig:del_menu")])
    return kb_nav(rows)
def fav_view_keyboard(uid, coin):
    B = InlineKeyboardButton
    rows = [[B(text="🔄 Обновить анализ", callback_data=f"c:{coin}")]]
    if get_open_position(uid, coin): rows += [[B(text="📈 Статус позиции", callback_data=f"pos:status:{coin}")], [B(text="❌ Закрыл сделку", callback_data=f"pos:close:{coin}")]]
    else: rows.append([B(text="✅ Вошёл в сделку", callback_data=f"pos:enter:{coin}")])
    return kb_nav(rows)
def admin_keyboard():
    B = InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[[B(text="📊 Статистика", callback_data="admin:stats"), B(text="👥 Пользователи", callback_data="admin:users")], [B(text="📰 Новость сейчас", callback_data="admin:news_now"), B(text=f"🔔 Новости: {'🟢 ВКЛ' if bot_flags['news_enabled'] else '🔴 ВЫКЛ'}", callback_data="admin:toggle_news")], [B(text=f"🚨 Сканер: {'🟢 ВКЛ' if bot_flags['alerts_enabled'] else '🔴 ВЫКЛ'}", callback_data="admin:toggle_scan"), B(text="🔍 Скан-тест", callback_data="admin:scan_test")], [B(text="⚡ Рассылка сигналов", callback_data="admin:broadcast_now")], [B(text="🔧 Диагностика", callback_data="admin:diag"), B(text="💾 Бэкап", callback_data="admin:backup")], [B(text="📥 Восстановить", callback_data="admin:restore"), B(text="⏳ Таймеры", callback_data="admin:timers")], [B(text="📜 CSV сигналов", callback_data="admin:csv")]])

# =====================================================================
# ЭКРАНЫ
# =====================================================================
WELCOME = ("👋 <b>Привет! Добро пожаловать в IXCLY PRO!</b>\n\nЯ твой персональный крипто-помощник.\n\n📊 <b>Умный анализ</b>: 20+ индикаторов, свечи, 4 таймфрейма и бэктест.\n🔥 <b>Готовые сигналы</b>: только сетапы, прошедшие все фильтры.\n💼 <b>Портфель</b>: свой депозит и риск на сделку.\n📥 <b>CSV</b>: выгрузка истории сигналов.\n\n⚠ <i>Криптовалюта — это рискованно!</i>\n\n👇 Выбери раздел:")
async def screen_coin(chat_id, uid, coin, edit_msg, force):
    if uid in busy_users: return
    busy_users.add(uid)
    try:
        res = None if force else get_user_res(uid, coin)
        if res is None:
            await show_loading(chat_id, edit_msg, f"⏳ Анализирую <b>{html.escape(coin)}</b>: индикаторы, свечи, бэктест…")
            res, err = await analyze_coin(coin, uid=uid, force=force)
            if err: return await present(chat_id, err, kb_nav())
            remember_res(uid, coin, res)
        kb = coin_keyboard(coin) if not res.get("price_only") else kb_nav()
        await present(chat_id, render_caption(res, uid), kb, photo=res.get("img"), edit_msg=edit_msg)
    finally: busy_users.discard(uid)
async def screen_news(chat_id, uid, edit_msg):
    await show_loading(chat_id, edit_msg, "⏳ Анализирую последние новости…")
    try: text = await get_news_batch_text()
    except Exception: text = "📭 Не удалось загрузить новости."
    kb = kb_nav([[InlineKeyboardButton(text="🗑 Удалить новость", callback_data="del_msg")]])
    await present(chat_id, text, kb, edit_msg=edit_msg)
async def screen_interesting(chat_id, uid, edit_msg):
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        movers = [x for x in j if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
        gain = sorted([x for x in movers if float(x["priceChangePercent"]) > 0], key=lambda x: float(x["priceChangePercent"]), reverse=True)[:20]
        lose = sorted([x for x in movers if float(x["priceChangePercent"]) < 0], key=lambda x: float(x["priceChangePercent"]))[:20]
        rg = sorted(random.sample(gain, min(5, len(gain))), key=lambda x: -float(x["priceChangePercent"]))
        rl = sorted(random.sample(lose, min(5, len(lose))), key=lambda x: float(x["priceChangePercent"]))
        text = "🧐 <b>Интересные за 24ч</b>\n\n🟢 <b>Топ роста:</b>\n" + "".join(f"<code>{x['symbol'][:-4]}</code> +{float(x['priceChangePercent']):.1f}%\n" for x in rg) + "\n🔴 <b>Топ падения:</b>\n" + "".join(f"<code>{x['symbol'][:-4]}</code> {float(x['priceChangePercent']):.1f}%\n" for x in rl) + f"\n\n🕒 <i>Обновлено: {datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M:%S')} МСК</i>"
        kb = kb_nav([[InlineKeyboardButton(text="🔄 Обновить", callback_data="interesting_menu")]])
    except Exception:
        text = "⚠ Биржа временно не отдаёт данные."
        kb = kb_nav([[InlineKeyboardButton(text="🔄 Обновить", callback_data="interesting_menu")]])
    await present(chat_id, text, kb, edit_msg=edit_msg)
def screen_sig_modes():
    text = "⚙️ <b>Сигналы по типу торговли</b>\n\nВыберите режим для отображения сигналов:"
    rows = [[InlineKeyboardButton(text="🟢 Агрессивный (от 60%)", callback_data="sig_list:mode:aggressive")], [InlineKeyboardButton(text="🟡 Сбалансированный (от 66%)", callback_data="sig_list:mode:balanced")], [InlineKeyboardButton(text="🔴 Консервативный (от 80%)", callback_data="sig_list:mode:conservative")]]
    return text, kb_nav(rows)
def screen_gstats():
    hist = load_signal_history()
    uniq, seen = [], set()
    for x in sorted(hist, key=lambda k: k.get("ts", 0), reverse=True):
        if f"{x['coin']}_{x['side']}" not in seen: seen.add(f"{x['coin']}_{x['side']}"); uniq.append(x)
    active_cnt = len(get_active_signals())
    cnt = {s: sum(1 for x in uniq if x.get("status") == s) for s in ("win", "loss", "missed", "expired")}
    closed = cnt["win"] + cnt["loss"]
    wr = cnt["win"] / closed * 100 if closed else 0.0
    exp_r = (cnt["win"] * 1.5 - cnt["loss"]) / closed if closed else 0.0
    text = f"📊 <b>Статистика сигналов</b>\n\n📈 Всего уникальных: <b>{len(uniq)}</b>\n🎯 Винрейт: <b>{wr:.1f}%</b> · ср. результат <b>{exp_r:+.2f}R</b>\n✅ Побед: <b>{cnt['win']}</b> | ❌ Убытков: <b>{cnt['loss']}</b>\n🚫 Упущено: <b>{cnt['missed']}</b>\n\n<i>* Считается по свечам: вход лимиткой, TP1 засчитывается как победа.</i>"
    B = InlineKeyboardButton
    rows = [[B(text=f"⏳ В работе ({active_cnt})", callback_data="stats_list:active:0"), B(text=f"✅ Успешные ({cnt['win']})", callback_data="stats_list:win:0")], [B(text=f"🚫 Упущено ({cnt['missed']})", callback_data="stats_list:missed:0"), B(text=f"❌ Убыточные ({cnt['loss']})", callback_data="stats_list:loss:0")], [B(text=f"⌛ По времени ({cnt['expired']})", callback_data="stats_list:expired:0")]]
    return text, kb_nav(rows)
def screen_stats_list(list_type, page, per=10):
    titles = {"active": "⏳ В работе", "win": "✅ Успешные", "missed": "🚫 Упущенные", "loss": "❌ Убыточные", "expired": "⌛ По времени"}
    if list_type == "active": items = get_active_signals()
    else:
        want = (list_type,)
        items, seen = [], set()
        for x in sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True):
            if x.get("status") in want and f"{x['coin']}_{x['side']}" not in seen: seen.add(f"{x['coin']}_{x['side']}"); items.append(x)
    pages = (len(items) + per - 1) // per or 1
    page = max(0, min(page, pages - 1))
    B = InlineKeyboardButton
    rows = [[B(text=f"{'🟢' if x['side'] == 'LONG' else '🔴'} {x['coin']}", callback_data=f"c:{x['coin']}")] for x in items[page * per:(page + 1) * per]]
    nav = []
    if page > 0: nav.append(B(text="⬅", callback_data=f"stats_list:{list_type}:{page - 1}"))
    nav.append(B(text=f"{page + 1}/{pages}", callback_data="ignore"))
    if page < pages - 1: nav.append(B(text="➡", callback_data=f"stats_list:{list_type}:{page + 1}"))
    return f"{titles.get(list_type, '')}\nНайдено: <b>{len(items)}</b>", kb_nav(rows + [nav])
def screen_sig_list(uid, category):
    active = get_active_signals()
    if category == "Все":
        mode = _users().get(str(uid), {}).get("trade_mode", "balanced")
        if mode == "aggressive": active = [x for x in active if x.get("type") in ("Краткосрочный", "Среднесрочный")]
        elif mode == "balanced": active = [x for x in active if x.get("type") in ("Среднесрочный", "Долгосрочный")]
        elif mode == "conservative": active = [x for x in active if x.get("type") == "Долгосрочный"]
        title = f"Все сигналы (режим {mode})"
    elif category.startswith("mode:"):
        mode = category.split(":")[1]
        if mode == "aggressive": active = [x for x in active if x.get("type") in ("Краткосрочный", "Среднесрочный")]; title = "Режим: 🟢 Агрессивный"
        elif mode == "balanced": active = [x for x in active if x.get("type") in ("Среднесрочный", "Долгосрочный")]; title = "Режим: 🟡 Сбалансированный"
        elif mode == "conservative": active = [x for x in active if x.get("type") == "Долгосрочный"]; title = "Режим: 🔴 Консервативный"
        else: active = []; title = f"Режим: {mode}"
    elif category != "Все": active = [x for x in active if x.get("type") == category]; title = f"Сигналы: {category}"
    else: title = "Сигналы: Все"
    if not active: return f"📋 <b>{title}</b>\n\n<i>Сейчас активных сигналов нет.</i>", kb_nav()
    rows = []
    for x in active[:20]:
        icon = "🟢" if x['side'] == 'LONG' else "🔴"
        c_type = "⚡" if x['type'] == "Краткосрочный" else "⏳" if x['type'] == "Среднесрочный" else "🛡"
        rows.append([InlineKeyboardButton(text=f"{icon} {x['coin']} · {c_type} {x.get('strength', 0)}%", callback_data=f"c:{x['coin']}")])
    return f"📋 <b>{title}</b>\n\nВыберите монету:", kb_nav(rows)
def get_portfolio_keyboard():
    B = InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[[B(text="💰  Изменить депозит", callback_data="set_deposit")], [B(text="⚙️  Режим торговли", callback_data="set_mode")], [B(text="🤖  Автоторговля", callback_data="autotrade_menu")], [B(text="📜  История моих входов", callback_data="trade_history")], [B(text="⬅  Назад в меню", callback_data="menu")]])
def screen_portfolio(uid):
    s = get_user_risk_settings(uid)
    u = _users().get(str(uid), {})
    mode = u.get("trade_mode", "balanced")
    mode_txt = {"aggressive": "🟢 Агрессивный", "balanced": "🟡 Сбалансированный", "conservative": "🔴 Консервативный"}.get(mode, "🟡 Сбалансированный")
    auto = "🟢 ВКЛ" if u.get("auto_trade_enabled") else "🔴 ВЫКЛ"
    exch = u.get("exchange", "")
    keys = f"✅ {exch.title()}" if exch and u.get(f"{exch}_api") else "❌ нет"
    text = f"💼 <b>Ваш портфель</b>\n\n💰 Депозит: <b>${s['deposit']:.2f}</b>\n📉 Риск на сделку: <b>{s['risk_pct']}%</b>\n⚙️ Режим: <b>{mode_txt}</b>\n🤖 Автоторговля: <b>{auto}</b> · API: {keys}\n⚠ Открытый риск: <b>{calculate_portfolio_risk(uid):.1f}%</b>\n\n<i>Выберите, что изменить:</i>"
    return text, get_portfolio_keyboard()
def screen_settings(uid):
    s = get_user_notif_settings(uid)
    B = InlineKeyboardButton
    rows = [[B(text=f"Сигналы (сетапы): {'🟢 ВКЛ' if s['signals'] else '🔴 ВЫКЛ'}", callback_data="toggle_setting:signals")], [B(text=f"Новости: {'🟢 ВКЛ' if s['news'] else '🔴 ВЫКЛ'}", callback_data="toggle_setting:news")], [B(text=f"Аномалии объема: {'🟢 ВКЛ' if s['anomalies'] else '🔴 ВЫКЛ'}", callback_data="toggle_setting:anomalies")], [B(text=f"Отчеты по 20 мин: {'🟢 ВКЛ' if s['pos_updates'] else '🔴 ВЫКЛ'}", callback_data="toggle_setting:pos_updates")], [B(text=f"Утренний брифинг: {'🟢 ВКЛ' if s['briefing'] else '🔴 ВЫКЛ'}", callback_data="toggle_setting:briefing")]]
    return "⚙️ <b>Настройки уведомлений</b>\n\nВы можете отключить типы сообщений. 🟢 <i>Включено</i> | 🔴 <i>Выключено</i>", kb_nav(rows)
def screen_trade_history(uid):
    th = _users().get(str(uid), {}).get("trade_history", [])
    if not th: return "📜 <b>История моих входов</b>\n\n<i>Вы еще не входили в сделки.</i>", kb_nav()
    text = "📜 <b>История моих входов</b>\n\n"
    for t in reversed(th[-15:]):
        dt = datetime.fromtimestamp(t["entered_at"]).strftime("%d.%m %H:%M")
        icon = "🟢" if t["side"] == "LONG" else "🔴"
        text += f"{icon} <b>{t['coin']}</b> ({dt})\nВход: <code>{fmt(t['entry'])}</code> | TP1: <code>{fmt(t['tp1'])}</code> | SL: <code>{fmt(t['sl1'])}</code>\n\n"
    return trim_html(text, 4000), kb_nav()
async def _show_exchange_menu(chat_id, msg):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🟢 Binance", callback_data="at:ex:binance")], [InlineKeyboardButton(text="🟡 Bybit", callback_data="at:ex:bybit")], [InlineKeyboardButton(text="🔵 BingX", callback_data="at:ex:bingx")], [InlineKeyboardButton(text="⬅ Назад в портфель", callback_data="portfolio")]])
    await present(chat_id, "🤖 <b>Автоторговля</b>\n\nЗдесь вы подключаете биржу — бот будет открывать сделки автоматически.\n\n🔐 Ключ даёт право выставлять ордера. Ключи шифруются (Fernet).\n\n🛡 <b>Безопасность:</b>\n• Отключите «Разрешить вывод» (Withdraw)\n• Включите белый список IP\n\nВыберите биржу:", kb, edit_msg=msg)
def _exchange_instructions(exchange):
    if exchange == "binance": return "🟢 <b>Binance — как получить API-ключ</b>\n\n1️⃣ Войдите на <b>binance.com</b> → Профиль → <b>API Management</b>\n2️⃣ <b>Create API</b> → «System generated»\n3️⃣ Пройдите верификацию\n4️⃣ В настройках ключа:\n   • ✅ <b>Enable Futures</b>\n   • ❌ <b>Enable Withdrawals — ВЫКЛЮЧИТЕ</b>\n5️⃣ Скопируйте <b>API Key</b> и <b>Secret Key</b>\n\n📩 Отправьте в формате:\n<code>API_KEY,SECRET_KEY</code>"
    if exchange == "bybit": return "🟡 <b>Bybit — как получить API-ключ</b>\n\n1️⃣ Войдите на <b>bybit.com</b> → Профиль → <b>API</b>\n2️⃣ <b>Create New Key</b> → «System-generated API Keys»\n3️⃣ Тип: <b>Unified Trading</b>\n4️⃣ В правах:\n   • ✅ <b>Read-Write → Contract Trading</b>\n   • ❌ <b>Withdraw — ВЫКЛЮЧИТЕ</b>\n5️⃣ Скопируйте <b>API Key</b> и <b>Secret</b>\n\n📩 Отправьте в формате:\n<code>API_KEY,SECRET</code>"
    return "🔵 <b>BingX — как получить API-ключ</b>\n\n1️⃣ Войдите на <b>bingx.com</b> → Профиль → <b>API Management</b>\n2️⃣ <b>Create API</b> → <b>Perpetual Futures</b>\n3️⃣ В правах:\n   • ✅ <b>Read + Trade</b>\n   • ❌ <b>Withdraw — ВЫКЛЮЧИТЕ</b>\n4️⃣ Сохраните <b>API Key</b> и <b>Secret Key</b>\n\n📩 Отправьте в формате:\n<code>API_KEY,SECRET_KEY</code>"
async def show_screen(chat_id, uid, screen, edit_msg=None, force=False):
    kind = screen[0]
    if kind == "menu": return await present(chat_id, WELCOME, get_main_keyboard(), edit_msg=edit_msg)
    if kind == "search": return await present(chat_id, "🔍 <b>Выбери монету</b>\n<i>или напиши тикер сообщением:</i>", InlineKeyboardMarkup(inline_keyboard=coin_grid(POPULAR_COINS, "c:", screen[1]) + [nav_row()]), edit_msg=edit_msg)
    if kind == "coin": return await screen_coin(chat_id, uid, screen[1], edit_msg, force)
    if kind == "signals": text, kb = signals_overview(); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "sig_list": text, kb = screen_sig_list(uid, screen[1]); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "sig_modes": text, kb = screen_sig_modes(); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "news": return await screen_news(chat_id, uid, edit_msg)
    if kind == "interesting": return await screen_interesting(chat_id, uid, edit_msg)
    if kind == "gstats": text, kb = screen_gstats(); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "stats_list": text, kb = screen_stats_list(screen[1], screen[2]); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "fav": return await present(chat_id, "⭐ <b>Избранное</b>", fav_keyboard(uid), edit_msg=edit_msg)
    if kind == "fav_save": return await present(chat_id, "➕ <b>Какую монету сохранить?</b>", InlineKeyboardMarkup(inline_keyboard=coin_grid(POPULAR_COINS, "sig:do_save:", screen[1], page_cb="sig:save_page") + [nav_row()]), edit_msg=edit_msg)
    if kind == "fav_del": return await present(chat_id, "🗑 <b>Что удалить?</b>", kb_nav([[InlineKeyboardButton(text=f"❌ {s['coin']}", callback_data=f"sig:del:{s['coin']}")] for s in get_user_saved_signals(uid)]), edit_msg=edit_msg)
    if kind == "fav_view":
        item = next((s for s in get_user_saved_signals(uid) if s["coin"] == screen[1]), None)
        text = item["text"] if item else "Сигнал не найден."
        status = {"in_trade": "\n\n📈 <i>Цена достигла входа: сделка в работе.</i>", "expired": "\n\n⌛ <i>Срок идеи истёк.</i>"}.get((item or {}).get("status"), "")
        return await present(chat_id, text + status, fav_view_keyboard(uid, screen[1]), edit_msg=edit_msg)
    if kind == "portfolio": text, kb = screen_portfolio(uid); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "settings": text, kb = screen_settings(uid); return await present(chat_id, text, kb, edit_msg=edit_msg)
    if kind == "pos_status": return await present(chat_id, await position_status_text(uid, screen[1]), kb_nav(), edit_msg=edit_msg)

async def position_status_text(uid, coin):
    pos = get_open_position(uid, coin)
    if not pos: return "Позиция не найдена."
    price = await get_simple_price(coin)
    if not price: return "Не удалось получить цену."
    entry = pos["entry_price"]
    side = pos["side"]
    if pos.get("waiting_entry"):
        dist = abs(price - entry) / entry * 100
        return f"⏳ <b>Ожидание входа: {coin}</b>\n\nНаправление: <b>{side}</b>\nЦелевой лимит: <code>{fmt(entry)}</code>\nЦена сейчас: <code>{fmt(price)}</code>\nРасстояние: <b>{dist:.2f}%</b>\n\n<i>Сделка еще не открыта. Бот ждет касания цены входа.</i>"
    pnl = (price - entry) / entry * 100 * (1 if side == "LONG" else -1)
    t1 = "✅ " if pos.get("hit_tp1") else "⏳ "
    t2 = "✅ " if pos.get("hit_tp2") else "⏳ "
    t3 = "✅ " if pos.get("hit_tp3") else "⏳ "
    current_sl = pos["sl1"]
    sl_note = " (в безубытке)" if current_sl == entry else " (на TP1)" if current_sl == pos["tp1"] else ""
    return f"📈 <b>Статус {coin}</b>\n\nНаправление: <b>{side}</b>\nВход: <code>{fmt(entry)}</code>\nСейчас: <code>{fmt(price)}</code>\nPnL: <b>{pnl:+.2f}%</b>\n\n<b>Цели:</b>\n{t1} TP1: <code>{fmt(pos['tp1'])}</code>\n{t2} TP2: <code>{fmt(pos['tp2'])}</code>\n{t3} TP3: <code>{fmt(pos['tp3'])}</code>\n\n🛡 Текущий стоп: <code>{fmt(current_sl)}</code>{sl_note}"

# =====================================================================
# ХЕНДЛЕРЫ
# =====================================================================
@dp.message(PortfolioStates.waiting_for_deposit)
async def process_deposit(m: types.Message, state: FSMContext):
    try:
        amount = float((m.text or "").replace(",", ".").strip())
        if amount <= 0: raise ValueError
    except Exception:
        await m.answer("❌ Введите положительное число, например <code>1000</code>", parse_mode="HTML")
        return
    uid = m.from_user.id
    set_user_field(uid, deposit=amount)
    await safe_delete(m)
    await state.clear()
    text, kb = screen_portfolio(uid)
    await present(m.chat.id, f"✅ Депозит обновлён: <b>${amount:.2f}</b>\n\n{text}", kb)

@dp.message(AutotradeStates.waiting_for_api_key)
async def process_api_key(m: types.Message, state: FSMContext):
    data = await state.get_data()
    exchange = data.get("exchange", "binance")
    parts = [p.strip() for p in (m.text or "").split(",")]
    if len(parts) < 2 or len(parts[0]) < 10 or len(parts[1]) < 10:
        await m.answer("❌ Неверный формат. Отправьте так:\n<code>API_KEY,SECRET_KEY</code>", parse_mode="HTML")
        return
    api_key, secret = parts[0], parts[1]
    uid = m.from_user.id
    await safe_delete(m)
    ok = save_user_api_keys(uid, exchange, api_key, secret)
    await state.clear()
    text, kb = screen_portfolio(uid)
    if ok:
        set_user_field(uid, auto_trade_enabled=True)
        await present(m.chat.id, f"✅ <b>Ключи {exchange.title()} сохранены и зашифрованы.</b>\nАвтоторговля включена.\n\n{text}", kb)
    else: await present(m.chat.id, "❌ Шифрование недоступно, ключи не сохранены.\n\n" + text, kb)

@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    await asyncio.to_thread(save_user_data, uid, m.from_user.username, m.from_user.first_name)
    active_chats.add(m.chat.id)
    await purge_chat(m.chat.id, m.message_id)
    nav_reset(uid)
    await present(m.chat.id, WELCOME, get_main_keyboard())

@dp.message(Command("set_risk"))
async def cmd_set_risk(m: types.Message):
    uid = m.from_user.id
    await safe_delete(m)
    try:
        dep, rsk = float((m.text.split()[1]).replace(",", ".")), float((m.text.split()[2]).replace(",", "."))
        if dep <= 0 or not 0.1 <= rsk <= 10: raise ValueError
        set_user_field(uid, deposit=dep, risk_pct=rsk)
        text = f"✅ Настройки обновлены. Депозит: ${dep:g}, риск: {rsk:g}%."
    except Exception: text = "❌ Формат: <code>/set_risk 1000 1.5</code> (депозит > 0, риск от 0.1 до 10%)"
    await present(m.chat.id, text, kb_nav())

@dp.message(Command("api"))
async def cmd_api(m: types.Message):
    await safe_delete(m)
    await present(m.chat.id, "🔐 Отправьте API Key и Secret Binance Futures:\n<code>api_key,secret_key</code>\n\n⚠ Права: только чтение и торговля, <b>БЕЗ вывода средств</b>.", kb_nav())

@dp.message(Command("autotrade"))
async def cmd_autotrade(m: types.Message):
    await safe_delete(m)
    await present(m.chat.id, "⛔ <b>Автоторговля временно недоступна.</b>\nПользуйтесь ручными сигналами и анализом.", kb_nav())

@dp.message(Command("webhook"))
async def cmd_webhook(m: types.Message):
    await safe_delete(m)
    parts = m.text.split(maxsplit=1)
    if len(parts) == 2 and parts[1].startswith("http"):
        set_user_field(m.from_user.id, webhook_url=parts[1].strip())
        text = "✅ Вебхук установлен: новые сигналы будут дублироваться POST-запросом."
    else: text = "❌ Формат: <code>/webhook https://your-server.com/api/signal</code>"
    await present(m.chat.id, text, kb_nav())

@dp.message(F.text.regexp(r"^[A-Za-z0-9]{2,12}$"))
async def msg_ticker(m: types.Message):
    uid, coin = m.from_user.id, normalize_ticker(m.text)
    await safe_delete(m)
    nav_push(uid, ("coin", coin))
    await show_screen(m.chat.id, uid, ("coin", coin))

@dp.callback_query()
async def on_callback(cb: types.CallbackQuery, state: FSMContext):
    if not cb.message: return await safe_answer(cb)
    uid, chat_id, msg, data = cb.from_user.id, cb.message.chat.id, cb.message, cb.data or ""
    simple = {"news_menu": ("news",), "interesting_menu": ("interesting",), "last_signals": ("signals",), "global_stats": ("gstats",), "sig:menu": ("fav",), "portfolio": ("portfolio",), "search": ("search", 0), "sig:save_new": ("fav_save", 0), "sig:del_menu": ("fav_del",), "settings_menu": ("settings",), "sig_modes": ("sig_modes",)}
    try:
        if data == "ignore": return await safe_answer(cb)
        if data.startswith("toggle_setting:"):
            key = data.split(":", 1)[1]
            u = _users().get(str(uid), {})
            set_user_field(uid, **{f"notify_{key}": not u.get(f"notify_{key}", True)})
            await safe_answer(cb, "✅ Настройка сохранена")
            text, kb = screen_settings(uid)
            return await present(chat_id, text, kb, edit_msg=msg)
        if data == "del_msg":
            await safe_answer(cb, "Удалено")
            return await safe_delete(msg)
        if data == "trade_history":
            await safe_answer(cb)
            text, kb = screen_trade_history(uid)
            return await present(chat_id, text, kb, edit_msg=msg)
        if data == "set_deposit":
            await safe_answer(cb)
            await state.set_state(PortfolioStates.waiting_for_deposit)
            return await present(chat_id, "💰 <b>Изменение депозита</b>\n\nОтправьте сумму вашего депозита в USDT.", kb_nav(back=True), edit_msg=msg)
        if data == "set_mode":
            await safe_answer(cb)
            u = _users().get(str(uid), {})
            cur = u.get("trade_mode", "balanced")
            cur_txt = {"aggressive": "🟢 Агрессивный", "balanced": "🟡 Сбалансированный", "conservative": "🔴 Консервативный"}.get(cur, "🟡 Сбалансированный")
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🟢 Агрессивный · 60%", callback_data="mode:aggressive")], [InlineKeyboardButton(text="🟡 Сбалансированный · 66%", callback_data="mode:balanced")], [InlineKeyboardButton(text="🔴 Консервативный · 80%", callback_data="mode:conservative")], [InlineKeyboardButton(text="⬅ Назад в портфель", callback_data="portfolio")]])
            return await present(chat_id, f"⚙️ <b>Режим торговли</b>\n\nСейчас активен: <b>{cur_txt}</b>\n\nРежим определяет минимальную уверенность модели:\n🟢 <b>Агрессивный</b> — 60%\n🟡 <b>Сбалансированный</b> — 66%\n🔴 <b>Консервативный</b> — 80%", kb, edit_msg=msg)
        if data.startswith("mode:"):
            mode = data.split(":", 1)[1]
            conf_map = {"aggressive": 60, "balanced": 66, "conservative": 80}
            if mode not in conf_map: return await safe_answer(cb, "Неизвестный режим", True)
            set_user_field(uid, trade_mode=mode, conf_final=conf_map[mode])
            await safe_answer(cb, "✅ Режим обновлён")
            text, kb = screen_portfolio(uid)
            return await present(chat_id, text, kb, edit_msg=msg)
        if data == "autotrade_menu":
            await safe_answer(cb, "⛔ Временно недоступно", show_alert=True)
            await present(chat_id, "⛔ <b>Автоторговля временно недоступна.</b>", kb_nav(), edit_msg=msg)
            return
        if data == "at:off":
            set_user_field(uid, auto_trade_enabled=False)
            await safe_answer(cb, "🔴 Автоторговля выключена")
            text, kb = screen_portfolio(uid)
            return await present(chat_id, text, kb, edit_msg=msg)
        if data.startswith("at:ex:"):
            exchange = data.split(":", 2)[2]
            if exchange not in ("binance", "bybit", "bingx"): return await safe_answer(cb, "Неизвестная биржа", True)
            await safe_answer(cb)
            await state.update_data(exchange=exchange)
            await state.set_state(AutotradeStates.waiting_for_api_key)
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Назад к выбору биржи", callback_data="autotrade_menu_sel")], [InlineKeyboardButton(text="🏠 В портфель", callback_data="portfolio")]])
            return await present(chat_id, _exchange_instructions(exchange), kb, edit_msg=msg)
        if data == "menu":
            await state.clear()
            nav_reset(uid)
            await safe_answer(cb)
            return await show_screen(chat_id, uid, ("menu",), msg)
        if data == "nav:back":
            await state.clear()
            await safe_answer(cb)
            return await show_screen(chat_id, uid, nav_back(uid), msg)
        if data in simple:
            await state.clear()
            await safe_answer(cb, "⏳ Загружаю…" if data == "news_menu" else None)
            nav_push(uid, simple[data])
            return await show_screen(chat_id, uid, simple[data], msg)
        if data.startswith("search_page:") or data.startswith("sig:save_page:"):
            kind = "search" if data.startswith("search_page:") else "fav_save"
            screen = (kind, int(data.rsplit(":", 1)[1]))
            await safe_answer(cb)
            nav_replace(uid, screen)
            return await show_screen(chat_id, uid, screen, msg)
        if data.startswith("sig_list:"):
            await safe_answer(cb)
            screen = ("sig_list", data.split(":", 1)[1])
            nav_push(uid, screen)
            return await show_screen(chat_id, uid, screen, msg)
        if data.startswith("stats_list:"):
            _, t, p = data.split(":")
            await safe_answer(cb)
            screen = ("stats_list", t, int(p))
            nav_replace(uid, screen)
            return await show_screen(chat_id, uid, screen, msg)
        if data.startswith("c:"):
            coin = data.split(":", 1)[1]
            await safe_answer(cb, "⏳ Анализирую…")
            nav_push(uid, ("coin", coin))
            return await show_screen(chat_id, uid, ("coin", coin), msg)
        if data.startswith("r:"):
            await safe_answer(cb, "🔄 Пересчитываю…")
            return await show_screen(chat_id, uid, ("coin", data.split(":", 1)[1]), msg, force=True)
        if data.startswith("view:"):
            _, view, coin = data.split(":")
            res = get_user_res(uid, coin)
            if not res: return await safe_answer(cb, "Данные устарели, нажмите «Обновить»", True)
            text = render_details(res) if view == "details" else render_caption(res, uid)
            await safe_answer(cb)
            try:
                if msg.photo: await bot.edit_message_caption(chat_id=chat_id, message_id=msg.message_id, caption=text, parse_mode="HTML", reply_markup=coin_keyboard(coin, view))
                else: await bot.edit_message_text(text, chat_id=chat_id, message_id=msg.message_id, parse_mode="HTML", reply_markup=coin_keyboard(coin, view))
            except TelegramBadRequest: pass
            return
        if data.startswith("sig:view:"):
            await safe_answer(cb)
            screen = ("fav_view", data.split(":")[2])
            nav_push(uid, screen)
            return await show_screen(chat_id, uid, screen, msg)
        if data.startswith("sig:do_save:") or data.startswith("sig:quick_save:"):
            coin = data.split(":")[-1]
            res = get_user_res(uid, coin)
            text = render_caption(res, uid) if res and not res.get("price_only") else f"Сигнал {coin}"
            await asyncio.to_thread(add_user_saved_signal, uid, coin, text, res if res and not res.get("price_only") else None)
            await safe_answer(cb, f"✅ {coin} сохранён")
            if data.startswith("sig:do_save:"):
                nav_reset(uid, ("fav",))
                await show_screen(chat_id, uid, ("fav",), msg)
            return
        if data.startswith("sig:del:"):
            await asyncio.to_thread(del_user_saved_signal, uid, data.split(":")[2])
            await safe_answer(cb, "🗑 Удалено")
            return await show_screen(chat_id, uid, ("fav_del",), msg)
        if data.startswith("pos:enter:"):
            coin = data.split(":")[2]
            res = get_user_res(uid, coin)
            open_positions = get_user_open_positions(uid)
            for op_coin, op_data in open_positions.items():
                if op_coin == coin: continue
                if op_data.get("side") == res["side"]:
                    for group in CORRELATION_GROUPS:
                        if op_coin in group and coin in group:
                            await safe_answer(cb, f"⚠️ У вас уже открыт {op_data['side']} по {op_coin}. {coin} сильно коррелирует с ним!", show_alert=True)
                            return
            set_user_open_position(uid, coin, {"side": res["side"], "entry_price": res["entry"], "created_at": time.time(), "entered_at": time.time(), "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1], "risk_pct": get_user_risk_settings(uid)["risk_pct"], "notified_close": False, "notified_tp": False, "waiting_entry": True})
            with STATE_LOCK:
                u = STATE["users"].setdefault(str(uid), {"active": True})
                th = u.setdefault("trade_history", [])
                th.append({"coin": coin, "side": res["side"], "entry": res["entry"], "tp1": res["tps"][0], "tp2": res["tps"][1], "tp3": res["tps"][2], "sl1": res["sls"][0], "entered_at": time.time()})
                u["trade_history"] = th[-50:]
            state_touch()
            text_to_save = render_caption(res, uid)
            await asyncio.to_thread(add_user_saved_signal, uid, coin, text_to_save, res)
            text = f"⏳ <b>Лимитный ордер выставлен</b>\n\nМонета: <b>{coin}</b>\nНаправление: <b>{res['side']}</b>\nОжидаем откат к цене: <code>{fmt(res['entry'])}</code>\n\n<i>Бот ждет, пока цена дойдет до уровня входа.</i>"
            await safe_answer(cb, f"⏳ Ждем цену входа...")
            return await present(chat_id, text, fav_view_keyboard(uid, coin), edit_msg=msg)
        if data.startswith("pos:close:"):
            coin = data.split(":")[2]
            set_user_open_position(uid, coin, None)
            hist = load_signal_history()
            for x in hist:
                if x["coin"] == coin and x.get("status") in ("active", "in_trade"): x["status"] = "closed_manual"
            save_signal_history(hist)
            await safe_answer(cb, "❌ Позиция закрыта")
            return await show_screen(chat_id, uid, nav_stack.get(uid, [("menu",)])[-1], msg)
        if data.startswith("pos:status:"):
            await safe_answer(cb)
            screen = ("pos_status", data.split(":")[2])
            nav_push(uid, screen)
            return await show_screen(chat_id, uid, screen, msg)
        if data == "export_csv":
            await safe_answer(cb, "⏳ Формирую CSV…")
            return await bot.send_document(chat_id, BufferedInputFile(generate_csv_history(), "signals_history.csv"), caption="📊 История сигналов бота (CSV)")
        await safe_answer(cb)
    except (TelegramForbiddenError, TelegramNotFound): raise
    except Exception as e:
        log.error(f"Callback {data}: {e}", exc_info=True)
        await safe_answer(cb, "⚠ Ошибка, попробуйте ещё раз", True)
        try: await present(chat_id, "⚠ Что-то пошло не так. Вернитесь в меню и повторите.", kb_nav(back=False), edit_msg=msg)
        except Exception: pass

@dp.message()
async def fallback_message(m: types.Message):
    await safe_delete(m)
    nav_reset(m.from_user.id)
    await present(m.chat.id, WELCOME, get_main_keyboard())

# =====================================================================
# АДМИН-БОТ
# =====================================================================
if admin_dp:
    @admin_dp.errors()
    async def admin_errors_handler(event: ErrorEvent):
        if not isinstance(event.exception, TelegramNetworkError): log.error(f"Ошибка админ-бота: {event.exception}", exc_info=event.exception)
        return True
    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS: return await m.answer(f"⛔ Доступ только для администратора.\n\nВаш ID: <code>{m.from_user.id}</code>", parse_mode="HTML")
        total, active = user_counts()
        text = f"✅ <b>Админ-панель IXCLY v{VERSION}</b>\n👥 Пользователей: {total} (активных {active})"
        if total == 0: text += "\n\n⚠ База пуста. Нажмите «📥 Восстановить»."
        await m.answer(text, reply_markup=admin_keyboard(), parse_mode="HTML")
    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS: return await safe_answer(cb, "Нет доступа", True)
        act = cb.data.split(":")[1]
        if act in ("toggle_news", "toggle_scan"):
            key = "news_enabled" if act == "toggle_news" else "alerts_enabled"
            bot_flags[key] = not bot_flags[key]
            state_touch()
            await safe_answer(cb, f"{'Новости' if key == 'news_enabled' else 'Сканер'}: {'ВКЛ' if bot_flags[key] else 'ВЫКЛ'}")
            try: await cb.message.edit_reply_markup(reply_markup=admin_keyboard())
            except Exception: pass
            return
        await safe_answer(cb, "⏳ Выполняю…")
        if act == "stats":
            total, active = user_counts()
            up, ls = int(time.time() - stats["started"]), last_scan_info
            await cb.message.answer(f"📊 <b>Статистика</b>\nПользователей: {total} (активных {active})\nАптайм: {up // 3600} ч {up % 3600 // 60} мин\nСигналов: {stats['signals']} · Новостей: {stats['news_sent']}\nПоследний скан: {ls['stage1']} → {ls['stage2']} → {ls['final']}", parse_mode="HTML")
        elif act == "users":
            lines = [f"👥 Пользователи ({len(_users())})\n"] + [f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or u.get('username') or '—')[:18])}" for uid, u in list(_users().items())[-30:]]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        elif act == "news_now":
            text, img_url = await prepare_news(force=True)
            if not text: return await cb.message.answer("Свежих новостей нет")
            img = await download_bytes(img_url) if img_url else None
            kb = kb_nav([[InlineKeyboardButton(text="🗑 Удалить новость", callback_data="del_msg")]], back=False)
            sent, failed, _ = await broadcast({"text": text, "photo": img, "kb": kb, "nav": []})
            stats["news_sent"] += sent
            await cb.message.answer(f"✅ Отправлено: {sent}, ошибок: {failed}")
        elif act == "scan_test":
            await cb.message.answer("🔍 Полный скан без рассылки…")
            sigs = await scan_once(send=False)
            ls = last_scan_info
            head = f"{ls['stage1']} → {ls['stage2']} → бэктест {ls['backtest_ok']} → итог {ls['final']} ({ls['secs']} с)"
            await cb.message.answer("🔍 Тест сканера\n" + head + ("\n\n" + "\n".join(f"{'🟢' if s['is_long'] else '🔴'} {s['coin']} · {s['type']} · Conf {s['strength']}%" for s in sigs) if sigs else "\n\nСигналов нет (пороги строгие)."))
        elif act == "broadcast_now":
            text, kb = signals_overview()
            sent, *_ = await broadcast({"text": text, "kb": kb, "nav": [("signals",)]})
            await cb.message.answer(f"Рассылка: {sent}")
        elif act == "diag":
            await cb.message.answer(f"🔧 <b>Диагностика</b>\nccxt: {'✅' if ccxt else '❌'} · шифрование: {'✅' if cipher else '❌'}\nЧатов: {len(active_chats)} · задержка старта {POLL_START_DELAY} с\nПороги: stage1 {CONF_STAGE1:.0f}, final {CONF_FINAL:.0f}, бэктест ≥{BT_MIN_TRADES} сд, R ≥{BT_MIN_AVG_R}\n\n" + await run_ai_diag(), parse_mode="HTML")
        elif act == "backup": await cb.message.answer("💾 Полный бэкап отправлен выше." if await telegram_backup("вручную") else "Состояние пусто.")
        elif act == "restore": await cb.message.answer("Отправьте файл bot_state.json (как документ).")
        elif act == "csv": await cb.message.answer_document(BufferedInputFile(generate_csv_history(), "signals_history.csv"))
        elif act == "timers":
            now = time.time()
            def ft(sec): return "сейчас" if sec <= 0 else f"через {int(sec // 60)} мин {int(sec % 60)} сек"
            await cb.message.answer("⏳ <b>Таймеры:</b>\n\n" + "\n".join(f"{n}: {ft(next_timers[k] - now)}" for n, k in (("🤖 Сканер", "scanner"), ("📈 Позиции", "positions"), ("📰 Новости", "news_watcher"), ("📊 Сигналы", "global_signals"), ("🚨 Аномалии", "anomaly"), ("💾 Бэкап", "backup"))), parse_mode="HTML")
    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        if m.from_user.id not in ADMIN_IDS or not (m.document.file_name or "").endswith(".json"): return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            data = json.loads((await admin_bot.download_file(f.file_path)).read().decode("utf-8"))
            n = restore_state(data, force=True)
            load_active_chats()
            await asyncio.to_thread(flush_state)
            await m.answer(f"✅ Состояние восстановлено: пользователей {n}, сигналов {len(STATE['signal_history'])}.")
        except Exception as e: await m.answer(f"Ошибка восстановления: {html.escape(str(e))}", parse_mode="HTML")

# =====================================================================
# ЗАПУСК
# =====================================================================
async def run_polling(dispatcher, tg_bot, name, stop_event):
    attempt = 0
    while not stop_event.is_set():
        try:
            attempt += 1
            log.info(f"{name}: запуск поллинга (попытка {attempt})")
            try: await tg_bot.delete_webhook(drop_pending_updates=(attempt == 1))
            except Exception: pass
            try: await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=30)
            except Exception as poll_err: log.warning(f"{name}: Ошибка поллинга: {poll_err}"); raise poll_err
            if not stop_event.is_set():
                log.warning(f"{name}: поллинг завершился, перезапуск через 5 сек")
                await asyncio.sleep(5)
                attempt = 0
        except asyncio.CancelledError: raise
        except (TelegramNetworkError, aiohttp.ClientError, ConnectionError) as e:
            log.warning(f"{name}: сетевая ошибка ({e}), повтор через {min(2 ** attempt, 60)} сек")
            await asyncio.sleep(min(2 ** attempt, 60))
        except TelegramConflictError: await asyncio.sleep(min(2 ** attempt, 30))
        except Exception as e: log.error(f"{name}: непредвиденная ошибка: {e}"); await asyncio.sleep(10)

async def morning_briefing_loop():
    await asyncio.sleep(15)
    while True:
        now_utc = datetime.now(timezone.utc)
        msk_now = now_utc.astimezone(timezone(timedelta(hours=3)))
        target = msk_now.replace(hour=8, minute=0, second=0, microsecond=0)
        if msk_now >= target: target += timedelta(days=1)
        await asyncio.sleep((target - msk_now).total_seconds())
        try:
            ctx = await get_context(force=True)
            btc_txt = ctx.get("btc_txt", "нет данных")
            fng = ctx.get("fng")
            fng_txt = ctx.get("fng_txt", "")
            active_sigs = len(get_active_signals())
            gainers_txt = "нет данных"
            try:
                j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
                movers = [x for x in j if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
                gainers = sorted([x for x in movers if float(x["priceChangePercent"]) > 0], key=lambda x: float(x["priceChangePercent"]), reverse=True)[:3]
                gainers_txt = ", ".join([f"{x['symbol'][:-4]} (+{float(x['priceChangePercent']):.1f}%)" for x in gainers]) or "нет"
            except Exception: pass
            fng_str = f"{fng} ({fng_txt})" if fng is not None else "нет данных"
            text = f"☀️ <b>Доброе утро!</b>\n\n🌍 <b>BTC:</b> {btc_txt}\n😨 <b>F&G:</b> {fng_str}\n🔥 <b>Активных сигналов:</b> {active_sigs}\n🚀 <b>Топ роста:</b> {gainers_txt}\n\n<i>Удачной торговли сегодня!</i>"
            kb = kb_nav([[InlineKeyboardButton(text="🔥 Сигналы", callback_data="last_signals")]], back=False)
            await broadcast({"text": text, "kb": kb, "nav": [("menu",)]}, only_ids=active_chats, notif_type="briefing")
        except Exception: pass
        await asyncio.sleep(60)

async def price_alerts_loop():
    await asyncio.sleep(60)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                alerts = u.get("price_alerts", [])
                if not alerts: continue
                triggered = []
                for alert in alerts:
                    coin = alert["coin"]
                    price = await get_simple_price(coin)
                    if not price: continue
                    op, target = alert["op"], alert["price"]
                    is_hit = (op == ">" and price >= target) or (op == "<" and price <= target)
                    if is_hit:
                        triggered.append(alert)
                        text = f"🔔 <b>Алерт сработал!</b>\n\nМонета: <b>{coin}</b>\nУсловие: <code>{op} {fmt(target)}</code>\nТекущая цена: <code>{fmt(price)}</code>"
                        kb = kb_nav([[InlineKeyboardButton(text="📈 Анализ", callback_data=f"c:{coin}")]], back=False)
                        try: await _send_new(int(uid_str), text, kb, None)
                        except Exception: pass
                if triggered:
                    with STATE_LOCK: u["price_alerts"] = [a for a in alerts if a not in triggered]
                    state_touch()
        except Exception: pass
        await asyncio.sleep(120)

async def heartbeat_loop():
    while True:
        await asyncio.sleep(600)
        log.info("♥ аптайм %d мин, пик RAM %s МБ, получателей %d, сигналов %d", (time.time() - stats["started"]) // 60, _rss_mb(), len(active_chats), len(STATE["signal_history"]))

def _release_memory():
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception: pass

async def memory_janitor_loop():
    await asyncio.sleep(300)
    while True:
        try:
            now = time.time()
            for uid in list(user_res):
                d = user_res[uid]
                for c in [c for c, (ts, _) in d.items() if now - ts > 1800]: d.pop(c, None)
                if not d: user_res.pop(uid, None)
            for c in [c for c, v in analysis_cache.items() if now - v[0] > max(v[1], 600)]: analysis_cache.pop(c, None)
            for store, ttl in ((_ai_cache, 3600), (_funding_cache, 3600)):
                for k in [k for k, v in store.items() if now - v[0] > ttl]: store.pop(k, None)
            for store in (scan_skip, exchange_bans, BAD_MODELS):
                for k in [k for k, t in store.items() if t < now]: store.pop(k, None)
            for store, ttl in ((signal_sent_at, 2 * 86400), (anomaly_sent_at, 86400)):
                for k in [k for k, t in store.items() if now - t > ttl]: store.pop(k, None)
            if len(fetch_errors) > 100: fetch_errors.clear()
            if len(nav_stack) > 1000: nav_stack.clear()
            if len(preferred_source) > 600: preferred_source.clear()
            await asyncio.to_thread(_release_memory)
        except Exception: pass
        await asyncio.sleep(600)

async def invalidate_flipped_signals():
    hist = load_signal_history()
    changed = False
    for x in hist:
        if x.get("status") != "active": continue
        try: tf2, _ = await get_market_data_light(x["coin"], ("4h", "1d"))
        except Exception: continue
        p = (tf2["4h"]["long_prob"] + tf2["1d"]["long_prob"]) / 2
        if (x["side"] == "LONG" and p < 47) or (x["side"] == "SHORT" and p > 53):
            x["status"], changed = "cancelled", True
            for uid_str, u in list(_users().items()):
                pos = (u.get("open_positions") or {}).get(x["coin"])
                if pos and pos.get("waiting_entry") and pos["side"] == x["side"]:
                    set_user_open_position(int(uid_str), x["coin"], None)
                    try: await _send_new(int(uid_str), f"🚫 <b>Идея отменена: {x['coin']} ({x['side']})</b>\n\nРынок развернулся до входа. Лимитный ордер снят.", kb_nav(back=False), None)
                    except Exception: pass
    if changed: save_signal_history(hist)

async def main():
    global http, news_lock, scan_lock
    keep_alive()
    news_lock, scan_lock = asyncio.Lock(), asyncio.Lock()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/3.0"})
    load_state_file()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, stop.set)
        except NotImplementedError: pass
    background, pollers = [], []
    try:
        for tg_bot, key in ((bot, "main_bot"), (admin_bot, "admin_bot")):
            if tg_bot:
                try: runtime[key] = f"@{(await tg_bot.get_me()).username}"
                except Exception as e: log.error(f"get_me ({key}): {e}")
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot: await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([types.BotCommand(command="menu", description="🚀 Меню")])
        except Exception: pass
        if POLL_START_DELAY > 0:
            log.info(f"Ждём {POLL_START_DELAY} с, пока остановится предыдущий экземпляр…")
            try: await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError: pass
        if stop.is_set(): return
        restored = await telegram_restore()
        if restored: log.info(f"✅ Состояние восстановлено из бэкапа, пользователей: {restored}")
        load_active_chats()
        gc.freeze()
        asyncio.create_task(discover_models())
        background, pollers = [], []
        background += [asyncio.create_task(c()) for c in (scanner_loop, position_monitor_loop, position_update_loop, global_signal_monitor_loop, news_watcher_loop, backup_loop, anomaly_scanner_loop, state_flush_loop, heartbeat_loop, morning_briefing_loop, price_alerts_loop, memory_janitor_loop)]
        background.append(asyncio.create_task(update_market_bias()))
        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp: pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))
        log.info(f"🚀 IXCLY PRO v{VERSION} запущен")
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
    finally:
        log.info("Останавливаю бота…")
        for t in pollers: t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        try: await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=15)
        except Exception: await asyncio.to_thread(flush_state)
        for t in background: t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        for tg_bot in (bot, admin_bot):
            if tg_bot:
                try: await tg_bot.session.close()
                except Exception: pass
        await http.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): log.info("Бот остановлен")
