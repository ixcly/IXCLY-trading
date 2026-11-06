# -*- coding: utf-8 -*-
"""
IXCLY Trading Bot v5.2 ADAPTIVE QUANT ENGINE

- 20+ индикаторов, свечные паттерны, 4 таймфрейма, бэктест, ИИ-аналитик
- Единый экран: фото + подпись в ОДНОМ сообщении, везде кнопка «Назад»
- Портфель, личная статистика сделок, экспорт CSV, вебхуки, Mini App (Telegram WebApp)
- Сигналы проверяются по свечам (вход лимиткой, TP1-3, трейлинг стопа)
- Всё состояние уходит в бэкап админ-боту и восстанавливается при старте
"""

import asyncio
import base64
import csv
import gc
import hashlib
import hmac
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
import tempfile
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from threading import Thread
from urllib.parse import quote, parse_qsl, urlparse
import ipaddress
import socket

import aiohttp
import feedparser
from ixcly_news import extract_rss_text, parse_article_html, assess_news_coverage, cap_critical_score
import ixcly_billing as billing  # заказы Stars, проверка платежей, статусы доступа
import numpy as np
import pandas as pd
from ixcly_intelligence import (score_signal, market_regime, volume_anomaly, risk_position_size, indicator_snapshot, breakout_assessment, anti_fomo_assessment, news_risk_assessment, market_panic_assessment, scanner_rank_score, signal_memory_summary, adaptive_scanner_profile, atr_target_multipliers, atr_stop_multiplier, breakeven_trigger_reached, close_warning_confirmed,
                               scan_diagnostic_summary, is_memory_guard_reason, cleanup as cleanup_intelligence,
                               forecast_scenarios, early_movement_assessment, liquidation_risk_assessment,
                               signal_quality_breakdown, strategy_performance, self_audit_summary, quant_intelligence_stack, portfolio_var_es, correlation_clusters,
                               drawdown_guard, daily_loss_guard, emergency_risk_lock, trade_opportunity_ranking, portfolio_risk_dashboard, strategy_lab_report, signal_decay_assessment,
                               multi_asset_lead_lag_engine, correlation_breakdown, market_breadth_engine, adverse_selection_detector, post_entry_validation, chasing_protection, trade_recovery_assessment, adaptive_exit_assessment, walk_forward_validation, strategy_retirement, cross_exchange_watch, risk_budget_allocator,
                               probability_calibration_report, loss_autopsy, missed_opportunity_analysis, parameter_stability_test, parameter_sensitivity_from_history, signed_cvd_from_agg_trades, normalize_macro_calendar_events, macro_event_shield)
from ixcly_brain import (market_fingerprint, signal_fingerprint, decision_brain, liquidity_path,
                        news_reaction_assessment, strategy_dna, counterfactual_lab, replay_signal,
                        regime_transition_assessment, signal_fragility, evidence_conflict_map, score_calibration)
import ixcly_paper as _paper_engine
paper_snapshot = _paper_engine.snapshot
paper_create_account = _paper_engine.create_account
paper_select_account = _paper_engine.select_account
paper_open_position = _paper_engine.open_position
paper_close_position = _paper_engine.close_position
paper_set_settings = _paper_engine.set_settings
paper_reset_account = _paper_engine.reset_account
# Compatibility guard: old deployments may have an outdated ixcly_paper.py.
if not callable(getattr(_paper_engine, "analytics", None)):
    def _paper_analytics_compat(store, uid, account_id=None):
        user = _paper_engine.ensure_user(store, uid)
        aid = str(account_id or user.get("active_account") or "main")
        acc = next((a for a in user.get("accounts", []) if str(a.get("id")) == aid), None)
        if not acc:
            raise ValueError("Виртуальный счёт не найден")
        history = list(acc.get("history") or [])[-500:]
        pnls = [float(x.get("pnl", 0) or 0) for x in history]
        wins = [x for x in pnls if x > 1e-9]
        losses = [x for x in pnls if x < -1e-9]
        gross_profit, gross_loss = sum(wins), -sum(losses)
        return {"account_id": aid, "account_name": acc.get("name"), "trades": len(pnls),
                "wins": len(wins), "losses": len(losses), "breakeven": len(pnls)-len(wins)-len(losses),
                "win_rate": round(len(wins)/len(pnls)*100, 2) if pnls else 0.0,
                "net_pnl": round(sum(pnls), 2), "avg_pnl": round(sum(pnls)/len(pnls), 2) if pnls else 0.0,
                "profit_factor": round(gross_profit/gross_loss, 2) if gross_loss else (None if not gross_profit else "∞"),
                "max_drawdown_pct": float(acc.get("max_drawdown_pct", 0) or 0),
                "equity_curve": [], "history": history[-10:]}
    paper_analytics = _paper_analytics_compat
else:
    paper_analytics = _paper_engine.analytics
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramBadRequest, TelegramConflictError, TelegramForbiddenError,
    TelegramNetworkError, TelegramNotFound, TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import (BufferedInputFile, ErrorEvent, InlineKeyboardButton, InlineKeyboardMarkup,
                           LabeledPrice, MenuButtonWebApp, WebAppInfo)
from flask import Flask, Response, jsonify, request, send_file
from PIL import Image, ImageDraw, ImageFont

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
    """Конфликт getUpdates при деплое Render безвреден: одна строка раз в минуту."""

    def __init__(self):
        super().__init__()
        self.last = 0.0

    def filter(self, record):
        msg = record.getMessage()
        if "TelegramConflictError" in msg or "terminated by other getUpdates" in msg:
            now = time.time()
            if now - self.last > 60:
                self.last = now
                log.warning("ℹ Конфликт getUpdates: рядом работает другой экземпляр (при деплое Render это нормально).")
            return False
        if "Sleep for" in msg and "try again" in msg:
            return False
        return True


logging.getLogger("aiogram.dispatcher").addFilter(_ConflictFilter())

# =====================================================================
# ВЕБ-СЕРВЕР (keep-alive для Render + Mini App)
# =====================================================================
app = Flask(__name__)


def _rss_mb():
    """Peak RSS in MB (Linux Render)."""
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    except Exception:
        return None


def _current_rss_mb():
    """Current resident memory, unlike ru_maxrss which is a lifetime peak."""
    try:
        with open("/proc/self/status", "r", encoding="ascii") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except Exception:
        pass
    return None


@app.route("/")
def home():
    return "Bot is alive!"


@app.route("/health")
def health():
    return "ok"


@app.route("/status")
def status_page():
    return jsonify({
        "version": VERSION, "uptime_s": int(time.time() - stats["started"]), "recipients": len(active_chats),
        "main_bot": runtime["main_bot"], "admin_bot": runtime["admin_bot"], "news_engine": market_bias.get("engine"),
        "last_scan": (None if is_memory_guard_reason(last_scan_info.get("diagnostic", "")) else dict(last_scan_info)),
        "peak_rss_mb": _rss_mb(), "rss_mb": _current_rss_mb(),
    })


def _run_web():
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    try:
        port = int(os.environ.get("PORT", 10000))
        log.info(f"🌐 Flask keep-alive на порту {port}")
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception as e:
        log.error(f"Flask: {e}")


_web_started = False


def keep_alive():
    global _web_started
    if not _web_started:
        _web_started = True
        Thread(target=_run_web, daemon=True).start()


# =====================================================================
# НАСТРОЙКИ
# =====================================================================
def clean_key(k):
    return k.strip().strip('"').strip("'") if k else None


BOT_TOKEN = clean_key(os.getenv("BOT_TOKEN"))
INPUT_BOT_TOKEN = clean_key(os.getenv("INPUT_BOT_TOKEN"))
NEWS_CHANNEL_URL = clean_key(os.getenv("NEWS_CHANNEL_URL")) or "https://t.me/ixcly_news"
ADMIN_ID = clean_key(os.getenv("ADMIN_ID"))
GEMINI_API_KEYS = [clean_key(k) for k in os.getenv("GEMINI_API_KEYS", "").split(",") if clean_key(k)]
GROQ_API_KEY = clean_key(os.getenv("GROQ_API_KEY"))
OPENROUTER_API_KEY = clean_key(os.getenv("OPENROUTER_API_KEY"))
FINNHUB_API_KEY = clean_key(os.getenv("FINNHUB_API_KEY"))
FINANCECALENDAR_API_KEY = clean_key(os.getenv("FINANCECALENDAR_API_KEY"))
POLLINATIONS_KEY = clean_key(os.getenv("POLLINATIONS_KEY"))
GEMINI_MODEL = clean_key(os.getenv("GEMINI_MODEL"))
GROQ_MODEL = clean_key(os.getenv("GROQ_MODEL"))
OPENROUTER_MODEL = clean_key(os.getenv("OPENROUTER_MODEL"))
POLLINATIONS_MODEL = os.getenv("POLLINATIONS_MODEL", "openai")

if not BOT_TOKEN:
    raise SystemExit("КРИТИЧЕСКАЯ ОШИБКА: переменная BOT_TOKEN не задана!")

ADMIN_IDS = set()
for _x in re.split(r"[,\s]+", ADMIN_ID or ""):
    if _x.lstrip("-").isdigit():
        ADMIN_IDS.add(int(_x))


def make_cipher():
    """Ключ шифрования API-ключей: FERNET_KEY, иначе производный от BOT_TOKEN."""
    if not Fernet:
        return None
    raw = clean_key(os.getenv("FERNET_KEY"))
    if not raw:
        log.warning("FERNET_KEY не задан: ключи шифруются от BOT_TOKEN. Задайте FERNET_KEY в переменных Render.")
    if raw:
        try:
            return Fernet(raw.encode())
        except Exception as e:
            log.warning(f"FERNET_KEY некорректен ({e}), использую ключ, производный от BOT_TOKEN")
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(("ixcly:" + BOT_TOKEN).encode()).digest()))


cipher = make_cipher()

VERSION = "5.2.0-ADAPTIVE-QUANT-ENGINE"
runtime = {"main_bot": "?", "admin_bot": "не запущен"}
POLL_START_DELAY = int(os.getenv("POLL_START_DELAY", "45" if os.getenv("RENDER") else "0"))

CONF_STAGE1 = float(os.getenv("CONF_STAGE1", "55"))
CONF_FINAL = float(os.getenv("CONF_FINAL", "66"))
BT_MIN_TRADES = int(os.getenv("BT_MIN_TRADES", "5"))
BT_MIN_AVG_R = float(os.getenv("BT_MIN_AVG_R", "0.10"))
BT_MIN_WINRATE = float(os.getenv("BT_MIN_WINRATE", "23"))
MIN_QV24 = float(os.getenv("MIN_QV24", "1000000"))
SCAN_MAX_COINS = int(os.getenv("SCAN_MAX_COINS", "90"))
DEPOSIT = float(os.getenv("DEPOSIT", "1000"))
RISK_PCT = 1.0
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))  # сканер каждые 5 минут
SIGNAL_COOLDOWN = 6 * 3600

# --- Mini App ---
_wu = clean_key(os.getenv("WEBAPP_URL")) or (
    os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/") if os.getenv("RENDER_EXTERNAL_URL") else None)
WEBAPP_URL = (_wu.rstrip("/") if _wu.rstrip("/").endswith("/app") else _wu.rstrip("/") + "/app") if _wu else None
MAIN_LOOP = None
MODE_TYPES = {"aggressive": ("Краткосрочный", "Среднесрочный"), "balanced": ("Среднесрочный", "Долгосрочный"),
              "conservative": ("Долгосрочный",)}
MODE_NAMES = {"aggressive": "🟢 Агрессивный", "balanced": "🟡 Сбалансированный", "conservative": "🔴 Консервативный"}
MODE_CONF = {"aggressive": 60, "balanced": 66, "conservative": 80}
NOTIFY_NAMES = {"signals": "Сигналы", "news": "Новости", "anomalies": "Аномалии объёма",
                "pos_updates": "Отчёты по сделкам", "briefing": "Утренний брифинг",
                "liquidations": "Кластеры ликвидаций"}

bot = Bot(token=BOT_TOKEN, session=AiohttpSession(timeout=60.0))
dp = Dispatcher()
admin_bot = None
admin_dp = None
if INPUT_BOT_TOKEN and INPUT_BOT_TOKEN != BOT_TOKEN:
    admin_bot = Bot(token=INPUT_BOT_TOKEN, session=AiohttpSession(timeout=60.0))
    admin_dp = Dispatcher()
elif INPUT_BOT_TOKEN:
    log.warning("INPUT_BOT_TOKEN совпадает с BOT_TOKEN: админ-бот выключен")

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

CORRELATION_GROUPS = [
    ["ETH", "ARB", "OP", "MATIC", "STRK", "ZK", "MANTA"],
    ["SOL", "JUP", "PYTH", "WIF", "BONK"],
    ["DOGE", "SHIB", "PEPE", "FLOKI", "BOME", "MEME"],
    ["AVAX", "JOE", "GMX"],
    ["BNB", "CAKE"],
    ["LINK", "PYTH", "BAND"],
    ["UNI", "SUSHI", "1INCH"],
    ["ATOM", "OSMO", "TIA", "INJ", "SEI"],
    ["NEAR", "FET", "RNDR", "AGIX"],
]

SYMBOL_ALIASES = {
    "RNDR": ["RENDER", "RNDR"], "MATIC": ["POL", "MATIC"],
    "PEPE": ["PEPE", "1000PEPE"], "BONK": ["BONK", "1000BONK"], "FLOKI": ["FLOKI", "1000FLOKI"],
    "SATS": ["SATS", "1000SATS"], "SHIB": ["SHIB", "1000SHIB"], "XEC": ["XEC", "1000XEC"],
    "LUNC": ["LUNC", "1000LUNC"], "RATS": ["RATS", "1000RATS"],
}

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
nav_stack = {}
stats = {"signals": 0, "news_sent": 0, "broadcasts": 0, "started": time.time()}
bot_flags = {"news_enabled": True, "alerts_enabled": True}
last_scan_info = {"ts": 0, "stage1": 0, "stage2": 0, "backtest_ok": 0, "final": 0, "sent": 0, "secs": 0,
                  "bt_rejected": 0, "bt_unavailable": 0, "bt_errors": 0, "bt_error_examples": [],
                  "analysis_errors": 0, "analysis_wait": 0, "quality_rejected": 0, "analysis_error_examples": [],
                  "diagnostic": "Сканирование ещё не выполнялось", "reasons": [],
                  "rss_mb": None, "peak_rss_mb": None, "memory_trim": {}}
last_no_signal_notice = 0.0
scanner_ranking = []  # top-50 кандидатов последнего прохода, без свечных массивов
market_bias = {"bias": 0, "title_ru": "Нет данных", "summary": "", "impact": "Нейтральный фон", "coins": "BTC, ETH",
               "published": "", "link": "", "engine": "none", "ts": 0}
last_news_link = None
last_backup_msg = {}
next_timers = {"scanner": 0, "positions": 0, "pos_updates": 0, "news_watcher": 0, "backup": 0, "global_signals": 0, "anomaly": 0}
exchange_bans = {}
news_blackout_until = 0.0
news_blackout_kind, news_blackout_since = "misc", 0.0
# Публичный поток принудительных ликвидаций Binance USD-M Futures.
LIQUIDATIONS = []
_liq_last_alert = {}
_liq_lock = asyncio.Lock()
_macro_cache = {"ts": 0.0, "events": [], "status": "not_configured", "source": "Finnhub", "blocked": False, "error": None}
_alert_sent = {}

# =====================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЯ (память + файл + бэкап в Telegram)
# =====================================================================
# Render: /var/data доступен только при корректно подключённом и доступном диске.
# На Render Free при PermissionError используем временный каталог, чтобы бот не падал.
_REQUESTED_DATA_DIR = (os.getenv("IXCLY_DATA_DIR") or os.getenv("RENDER_DISK_PATH") or ".").strip() or "."
_DATA_DIR = _REQUESTED_DATA_DIR
_DATA_DIR_FALLBACK = False
try:
    os.makedirs(_DATA_DIR, exist_ok=True)
except PermissionError as _data_dir_error:
    _DATA_DIR = os.getenv("IXCLY_FALLBACK_DATA_DIR") or os.path.join(tempfile.gettempdir(), "ixcly_data")
    os.makedirs(_DATA_DIR, exist_ok=True)
    _DATA_DIR_FALLBACK = True
    log.warning("Нет доступа к IXCLY_DATA_DIR=%s (%s). Используется временное хранилище %s; данные могут исчезнуть после перезапуска/деплоя.",
                _REQUESTED_DATA_DIR, _data_dir_error, _DATA_DIR)
STATE_FILE = os.path.join(_DATA_DIR, "bot_state.json")
LEGACY_STATE_FILE = os.path.join(os.getcwd(), "bot_state.json")
LOCAL_BACKUP_FILES = (STATE_FILE + ".bak", STATE_FILE + ".bak2")
BACKUP_INTERVAL = max(900, int(os.getenv("IXCLY_BACKUP_INTERVAL", "900")))  # 15 минут по умолчанию
STATE_LOCK = threading.RLock()
STATE = {"users": {}, "signal_history": [], "analysis_db": {}, "last_msgs": {}, "persist": {}, "meta": {"saved_at": 0}, "promos": {}, "ads": {}, "paper": {}, "support_tickets": {}, "orders": {}, "payments": {}, "news_publisher_state": {}, "quant_price_history": {}, "quant_observations": [], "quant_parameter_history": {}}
state_dirty = False
backup_dirty = False
last_backup_ts = 0.0
STATE_KEYS = ("users", "signal_history", "analysis_db", "last_msgs", "persist", "promos", "ads", "paper", "support_tickets",
              "orders", "payments", "news_publisher_state", "quant_price_history", "quant_observations", "quant_parameter_history")  # durable quant data survives restarts


def state_touch(important=True):
    global state_dirty, backup_dirty
    state_dirty = True
    if important:
        backup_dirty = True


def collect_persist():
    return {
        "flags": dict(bot_flags),
        "stats": {k: v for k, v in stats.items() if k != "started"},
        "market_bias": dict(market_bias),
        "last_news_link": last_news_link,
        "signal_sent_at": dict(signal_sent_at),
        "anomaly_sent_at": dict(anomaly_sent_at),
        "last_scan_info": dict(last_scan_info),
        "bt_cache": {c: [v[0], v[1]] for c, v in bt_cache.items() if v[1]},
    }


def apply_persist(p):
    global last_news_link
    if not isinstance(p, dict):
        return
    for k, v in (p.get("flags") or {}).items():
        if k in bot_flags:
            bot_flags[k] = bool(v)
    for k, v in (p.get("stats") or {}).items():
        if k in stats and k != "started":
            stats[k] = v
    mb = p.get("market_bias")
    if isinstance(mb, dict) and mb.get("engine") not in (None, "none"):
        market_bias.update(mb)
    last_news_link = p.get("last_news_link") or last_news_link
    signal_sent_at.update({k: float(v) for k, v in (p.get("signal_sent_at") or {}).items()})
    anomaly_sent_at.update({k: float(v) for k, v in (p.get("anomaly_sent_at") or {}).items()})
    if isinstance(p.get("last_scan_info"), dict):
        last_scan_info.update(p["last_scan_info"])
    for c, v in (p.get("bt_cache") or {}).items():
        if isinstance(v, list) and len(v) == 2:
            bt_cache[c] = (float(v[0]), v[1])


def restore_state(data, force=False):
    """Применяет состояние (из файла или бэкапа). Берёт более свежую версию, если force=False."""
    if not isinstance(data, dict) or not data:
        return 0
    if "users" not in data and all(isinstance(v, dict) for v in data.values()):
        data = {"users": data}
    with STATE_LOCK:
        local_ts = STATE["meta"].get("saved_at", 0)
        remote_ts = (data.get("meta") or {}).get("saved_at", 0)
        if not force and STATE["users"] and local_ts >= remote_ts:
            return 0
        for k in STATE_KEYS:
            if k in data and data[k] is not None:
                STATE[k] = data[k]
        STATE["meta"]["saved_at"] = max(local_ts, remote_ts)
        n = len(STATE["users"])
    apply_persist(STATE.get("persist"))
    state_touch(False)
    return n


def _read_state_candidate(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or not isinstance(data.get("users", {}), dict):
            raise ValueError("неверная структура состояния")
        return data
    except Exception as e:
        log.warning(f"Файл состояния {path} недоступен: {e}")
        return None


def load_state_file():
    """Загружает основной файл, затем локальные резервные копии и legacy-путь."""
    candidates = [STATE_FILE, *LOCAL_BACKUP_FILES]
    if os.path.abspath(LEGACY_STATE_FILE) != os.path.abspath(STATE_FILE):
        candidates.append(LEGACY_STATE_FILE)
    for path in candidates:
        if not os.path.exists(path):
            continue
        data = _read_state_candidate(path)
        if data is None:
            continue
        n = restore_state(data, force=True)
        log.info(f"Состояние загружено из {path}; пользователей: {n}")
        if path != STATE_FILE:
            state_touch(False)
        return n
    log.warning("Локальное состояние не найдено; при старте будет выполнена попытка восстановления из Telegram.")
    return 0


def _rotate_local_backups():
    """Сохраняет две предыдущие версии перед атомарной заменой основного JSON."""
    try:
        if os.path.exists(LOCAL_BACKUP_FILES[0]):
            import shutil
            shutil.copy2(LOCAL_BACKUP_FILES[0], LOCAL_BACKUP_FILES[1])
        if os.path.exists(STATE_FILE):
            import shutil
            shutil.copy2(STATE_FILE, LOCAL_BACKUP_FILES[0])
    except Exception as e:
        log.warning(f"Не удалось обновить локальные резервные копии: {e}")


def flush_state():
    """Пишет состояние на диск, возвращает JSON-строку (для бэкапа)."""
    global state_dirty
    with STATE_LOCK:
        STATE["persist"] = collect_persist()
        STATE["meta"] = {"saved_at": time.time(), "version": VERSION}
        data = None
        for attempt in range(3):
            try:
                data = json.dumps(STATE, ensure_ascii=False, default=str)
                break
            except RuntimeError as e:
                if attempt == 2:
                    log.error(f"Не удалось сделать снимок STATE после 3 попыток: {e}")
                else:
                    time.sleep(0.05)
        if data is None:
            return ""
        try:
            with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            _rotate_local_backups()
            os.replace(STATE_FILE + ".tmp", STATE_FILE)
            state_dirty = False
        except Exception as e:
            log.error(f"Ошибка записи состояния: {e}")
    return data


async def state_flush_loop():
    while True:
        await asyncio.sleep(15)
        if state_dirty:
            await asyncio.to_thread(flush_state)


# ---------------- пользователи ----------------
def _users():
    return STATE["users"]


def save_user_data(user_id, username, first_name):
    key = str(user_id)
    with STATE_LOCK:
        new = key not in STATE["users"]
        u = STATE["users"].setdefault(key, {"joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        u.update({"username": username, "first_name": first_name, "active": True})
    _init_trial(user_id)
    state_touch(new)


def mark_users_inactive(ids):
    with STATE_LOCK:
        for i in ids:
            u = STATE["users"].get(str(i))
            if u:
                u["active"] = False
    state_touch(False)


def load_active_chats():
    active_chats.clear()
    for uid, u in _users().items():
        if isinstance(u, dict) and u.get("active", True):
            try:
                active_chats.add(int(uid))
            except ValueError:
                pass
    log.info("Получателей рассылки: %d", len(active_chats))


def user_counts():
    users = _users()
    return len(users), sum(1 for u in users.values() if isinstance(u, dict) and u.get("active", True))


def get_user_notif_settings(user_id):
    u = _users().get(str(user_id), {})
    return {
        "signals": u.get("notify_signals", True),
        "news": u.get("notify_news", True),
        "anomalies": u.get("notify_anomalies", True),
        "pos_updates": u.get("notify_pos_updates", True),
        "briefing": u.get("notify_briefing", True),
        "liquidations": u.get("notify_liquidations", True),
    }


def get_user_saved_signals(user_id):
    return list(_users().get(str(user_id), {}).get("saved_signals", []))


def add_user_saved_signal(user_id, coin, text, res=None):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(user_id), {"active": True})
        lst = [s for s in u.get("saved_signals", []) if s.get("coin") != coin]
        item = {"coin": coin, "text": text, "status": "active", "ts": time.time(), "expire_ts": time.time() + 48 * 3600}
        if res and res.get("entry"):
            item.update({"entry": res["entry"], "tp1": res["tps"][0], "sl1": res["sls"][0], "is_long": res["is_long"]})
        lst.append(item)
        u["saved_signals"] = lst
    state_touch()


def del_user_saved_signal(user_id, coin):
    with STATE_LOCK:
        u = STATE["users"].get(str(user_id))
        if u and "saved_signals" in u:
            u["saved_signals"] = [s for s in u["saved_signals"] if s.get("coin") != coin]
    state_touch()


def get_user_open_positions(user_id):
    return dict(_users().get(str(user_id), {}).get("open_positions", {}))


def get_open_position(user_id, coin):
    return get_user_open_positions(user_id).get(coin)


def set_user_open_position(user_id, coin, data):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(user_id), {"active": True})
        pos = u.setdefault("open_positions", {})
        if data is None:
            pos.pop(coin, None)
        else:
            pos[coin] = data
    state_touch()


# ---------------- история сигналов и архив анализов ----------------
def load_signal_history():
    """Возвращает весь сохранённый архив сигналов; очистка по 30 дням скрывала историю."""
    with STATE_LOCK:
        return list(STATE.get("signal_history", []))


def quant_closed_trade_history(coin=None, limit=500):
    """Closed user trades only; open orders/positions are excluded from model learning."""
    rows = []
    for uid, user in list(_users().items()):
        if not isinstance(user, dict):
            continue
        for item in user.get("trade_log", []) or []:
            if not isinstance(item, dict) or not item.get("closed_at"):
                continue
            if coin and str(item.get("coin", "")).upper() != str(coin).upper():
                continue
            row = dict(item)
            row["status"] = "closed"
            rows.append(row)
    rows.sort(key=lambda x: float(x.get("closed_at", 0) or 0))
    return rows[-max(1, min(2000, int(limit))):]


def save_signal_history(hist):
    hist = hist[-2000:]
    for x in hist[:-150]:
        x.pop("text", None)
    with STATE_LOCK:
        STATE["signal_history"] = hist
    state_touch()


def save_analysis_db(coin, res):
    item = {k: v for k, v in res.items() if k != "img"}
    with STATE_LOCK:
        STATE["analysis_db"][coin] = {"ts": time.time(), "data": item}
        if len(STATE["analysis_db"]) > 150:
            for k in sorted(STATE["analysis_db"], key=lambda c: STATE["analysis_db"][c]["ts"])[:30]:
                STATE["analysis_db"].pop(k, None)
    state_touch(False)


def load_analysis_db(coin):
    return STATE["analysis_db"].get(coin)


# =====================================================================
# ПОРТФЕЛЬ, API-КЛЮЧИ, ВЕБХУКИ, CSV, ON-CHAIN
# =====================================================================
def get_user_risk_settings(user_id):
    u = _users().get(str(user_id), {})
    return {"deposit": float(u.get("deposit", DEPOSIT)), "risk_pct": float(u.get("risk_pct", RISK_PCT)),
            "max_open_risk": float(u.get("max_open_risk", 5.0))}


def calculate_portfolio_risk(user_id):
    s = get_user_risk_settings(user_id)
    return sum(p.get("risk_pct", s["risk_pct"]) for p in get_user_open_positions(user_id).values())


def set_user_field(user_id, **fields):
    with STATE_LOCK:
        STATE["users"].setdefault(str(user_id), {"active": True}).update(fields)
    state_touch()




def safe_webhook(url):
    """Разрешает только HTTPS URL с публичным DNS-адресом (защита от базового SSRF)."""
    try:
        u = urlparse(str(url or "").strip())
        if u.scheme != "https" or not u.hostname or u.username or u.password:
            return False
        if u.port not in (None, 443):
            return False
        for ai in socket.getaddrinfo(u.hostname, 443, type=socket.SOCK_STREAM):
            ip = ipaddress.ip_address(ai[4][0])
            if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
                    or ip.is_multicast or ip.is_unspecified):
                return False
        return True
    except Exception:
        return False


async def send_webhook_all(event):
    """Отправляет события только на HTTPS webhook с публичным адресом."""
    for uid, u in list(_users().items()):
        url = u.get("webhook_url") if isinstance(u, dict) else None
        if not url:
            continue
        try:
            if not await asyncio.to_thread(safe_webhook, url):
                log.warning("Webhook %s отклонён: небезопасный URL", uid)
                continue
            async with http.post(url, json=event, timeout=aiohttp.ClientTimeout(total=5), allow_redirects=False) as r:
                log.info(f"Webhook {uid}: {r.status}")
        except Exception as e:
            log.warning(f"Webhook {uid}: {e}")


def generate_csv_history():
    with STATE_LOCK:
        hist = list(STATE["signal_history"])
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Дата", "Монета", "Тип", "Сторона", "Вход", "TP1", "TP2", "TP3", "SL1", "Статус", "Макс TP", "Уверенность"])
    for x in hist:
        w.writerow([datetime.fromtimestamp(x.get("ts", 0)).strftime("%Y-%m-%d %H:%M"), x.get("coin"), x.get("type"),
                    x.get("side"), x.get("entry"), x.get("tp1"), x.get("tp2"), x.get("tp3"), x.get("sl1"),
                    x.get("status"), x.get("max_tp_hit", 0), x.get("strength")])
    return out.getvalue().encode("utf-8-sig")


_defillama_cache = {"ts": 0.0, "change": 0.0}


async def get_onchain_context():
    """Изменение суммарного TVL за сутки (DefiLlama)."""
    if time.time() - _defillama_cache["ts"] < 3600:
        return _defillama_cache["change"]
    try:
        data = await http_json("https://api.llama.fi/v2/historicalChainTvl", timeout=10)
        if len(data) > 2 and data[-2]["tvl"]:
            ch = (data[-1]["tvl"] - data[-2]["tvl"]) / data[-2]["tvl"] * 100
            _defillama_cache.update(ts=time.time(), change=round(ch, 2))
    except Exception as e:
        log.info(f"DefiLlama: {e}")
        _defillama_cache["ts"] = time.time()
    return _defillama_cache["change"]


# =====================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================================================================
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


def make_strength_bar(strength, length=10):
    filled = max(0, min(length, int(round(strength / 100 * length))))
    return "🟩" * filled + "🟥" * (length - filled)


def has_cyr(text, n=1):
    return len(re.findall(r"[А-Яа-яЁё]", text or "")) >= n


def shorten(text, n):
    text = " ".join(str(text).split())
    if len(text) <= n:
        return text
    cut = text[:n]
    m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if m >= n * 0.5:
        return cut[:m + 1]
    return cut.rsplit(" ", 1)[0].rstrip(",;:-— ") + "…"


def strip_tags(t):
    return html.unescape(re.sub(r"<[^>]+>", "", t or ""))


def vis_len(t):
    return len(strip_tags(t))


def trim_html(text, limit):
    if vis_len(text) <= limit:
        return text
    return html.escape(strip_tags(text)[:limit - 1]) + "…"


def fit_caption(parts, limit=1000):
    """parts: [(приоритет, строка)]. Пока не влезает в лимит, убираем строки с наибольшим приоритетом."""
    items = list(parts)

    def total():
        return sum(vis_len(line) + 1 for _, line in items)

    while total() > limit and items:
        worst = max(p for p, _ in items)
        if worst <= 1:
            break
        idx = max(i for i, (p, _) in enumerate(items) if p == worst)
        items.pop(idx)
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(line for _, line in items)).strip()
    return trim_html(text, limit)


def normalize_ticker(text):
    t = re.sub(r"[\s$/\-_]", "", text.upper().strip())
    return t[:-4] if t.endswith("USDT") and len(t) > 4 else t


def get_moscow_time_from_entry(entry):
    msk = timezone(timedelta(hours=3))
    try:
        published = None
        if getattr(entry, "published_parsed", None):
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "updated_parsed", None):
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        elif getattr(entry, "published", None):
            published = parsedate_to_datetime(entry.published)
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
        if published:
            return published.astimezone(msk).strftime("%d.%m.%Y %H:%M") + " МСК"
    except Exception:
        pass
    return datetime.now(msk).strftime("%d.%m.%Y %H:%M") + " МСК"


async def first_success(coros, timeout=11):
    """Первый НЕпустой результат; провайдер с ошибкой остальных не отменяет."""
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


async def http_json(url, params=None, timeout=8):
    async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.json(content_type=None)


async def download_bytes(url):
    """Скачивает только реальные изображения разумного размера."""
    if not _normalise_image_url(url):
        return None
    try:
        async with http.get(url, headers=BROWSER_UA, timeout=aiohttp.ClientTimeout(total=10),
                            allow_redirects=True) as r:
            if r.status != 200:
                return None
            content_type = r.headers.get("Content-Type", "").lower()
            if not content_type.startswith("image/"):
                return None
            length = int(r.headers.get("Content-Length", "0") or 0)
            if length and length > 8 * 1024 * 1024:
                return None
            data = await r.read()
            if not data or len(data) > 8 * 1024 * 1024:
                return None
            if not (data.startswith(b"\xff\xd8\xff") or data.startswith(b"\x89PNG") or
                    data.startswith((b"GIF87a", b"GIF89a", b"RIFF")) or data.startswith(b"BM")):
                return None
            return data
    except Exception as exc:
        log.debug("Не удалось скачать изображение новости: %s", exc)
    return None


async def safe_delete(message):
    try:
        await message.delete()
    except Exception:
        pass


async def safe_answer(cb, text=None, alert=False):
    try:
        await cb.answer(text, show_alert=alert)
    except Exception:
        pass


# =====================================================================
# ЕДИНЫЙ ЭКРАН: отправка, очистка старых сообщений, навигация «Назад»
# =====================================================================
def get_tracked(chat_id):
    return list(STATE["last_msgs"].get(str(chat_id), []))


def set_tracked(chat_id, ids):
    with STATE_LOCK:
        STATE["last_msgs"][str(chat_id)] = list(ids)[-4:]
    state_touch(False)


async def delete_ids(chat_id, ids):
    ids = [i for i in set(ids) if i]
    if not ids:
        return
    try:
        await bot.delete_messages(chat_id=chat_id, message_ids=ids[:100])
        return
    except Exception:
        pass
    for i in ids[:30]:
        try:
            await bot.delete_message(chat_id, i)
        except Exception:
            pass


async def purge_chat(chat_id, upto_id, count=100):
    ids = list(range(max(1, upto_id - count), upto_id + 1))
    try:
        await bot.delete_messages(chat_id=chat_id, message_ids=ids)
    except Exception:
        pass


async def _send_new(chat_id, text, kb, photo):
    """Отправка с повторами при временных сетевых ошибках Telegram."""
    last_error = None
    for attempt in range(3):
        try:
            if TG_SEM is None:
                return await _send_new_raw(chat_id, text, kb, photo)
            async with TG_SEM:
                return await _send_new_raw(chat_id, text, kb, photo)
        except TelegramRetryAfter as exc:
            last_error = exc
            await asyncio.sleep(min(float(getattr(exc, "retry_after", 2)) + 0.5, 15))
        except (TelegramNetworkError, asyncio.TimeoutError, aiohttp.ClientError) as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
        except Exception:
            raise
    raise last_error or RuntimeError("Telegram delivery failed")


async def _send_new_raw(chat_id, text, kb, photo):
    try:
        if photo:
            return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=trim_html(text, 1024),
                                        parse_mode="HTML", reply_markup=kb)
        return await bot.send_message(chat_id, trim_html(text, 4000), parse_mode="HTML", reply_markup=kb,
                                      disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "parse" not in str(e).lower() and "entit" not in str(e).lower():
            raise
        plain = strip_tags(text)
        if photo:
            return await bot.send_photo(chat_id, BufferedInputFile(photo, "chart.png"), caption=plain[:1024], reply_markup=kb)
        return await bot.send_message(chat_id, plain[:4000], reply_markup=kb)


async def present(chat_id, text, kb=None, photo=None, edit_msg=None):
    """Показывает экран ОДНИМ сообщением и стирает все предыдущие сообщения бота в этом чате."""
    old = set(get_tracked(chat_id))
    if edit_msg is not None:
        old.add(edit_msg.message_id)
    msg = None
    has_media = edit_msg is not None and bool(edit_msg.photo or edit_msg.video or edit_msg.document or edit_msg.animation)
    if photo is None and edit_msg is not None and not has_media:
        try:
            await bot.edit_message_text(trim_html(text, 4000), chat_id=chat_id, message_id=edit_msg.message_id,
                                        reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
            msg = edit_msg
        except TelegramBadRequest as e:
            if "not modified" in str(e).lower():
                msg = edit_msg
        except (TelegramForbiddenError, TelegramNotFound, TelegramRetryAfter):
            raise
        except Exception:
            msg = None
    if msg is None:
        msg = await _send_new(chat_id, text, kb, photo)
    await delete_ids(chat_id, [i for i in old if i != msg.message_id])
    set_tracked(chat_id, [msg.message_id])
    return msg


async def show_loading(chat_id, edit_msg, text):
    try:
        if edit_msg is None:
            return await present(chat_id, text)
        if edit_msg.photo:
            await bot.edit_message_caption(chat_id=chat_id, message_id=edit_msg.message_id, caption=text,
                                           parse_mode="HTML", reply_markup=None)
        else:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=edit_msg.message_id, parse_mode="HTML")
    except Exception:
        pass
    return edit_msg


def nav_reset(uid, *screens):
    nav_stack[uid] = [("menu",)] + list(screens)


def nav_push(uid, screen):
    st = nav_stack.setdefault(uid, [("menu",)])
    if st[-1] != screen:
        st.append(screen)
    if len(st) > 25:
        del st[1:-24]


def nav_replace(uid, screen):
    st = nav_stack.setdefault(uid, [("menu",)])
    if len(st) > 1 and st[-1][0] == screen[0]:
        st[-1] = screen
    else:
        nav_push(uid, screen)


def nav_back(uid):
    st = nav_stack.setdefault(uid, [("menu",)])
    if len(st) > 1:
        st.pop()
    return st[-1]


def nav_row(back=True):
    row = []
    if back:
        row.append(InlineKeyboardButton(text="⬅ Назад", callback_data="nav:back"))
    row.append(InlineKeyboardButton(text="🏠 Меню", callback_data="menu"))
    return row


def kb_nav(rows=None, back=True):
    return InlineKeyboardMarkup(inline_keyboard=list(rows or []) + [nav_row(back)])


# =====================================================================
# МИДЛВАРЬ И ОБРАБОТЧИК ОШИБОК
# =====================================================================
class UserTrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user, chat_id = None, None
        if event.message:
            user, chat_id = event.message.from_user, event.message.chat.id
        elif event.callback_query:
            user = event.callback_query.from_user
            if event.callback_query.message:
                chat_id = event.callback_query.message.chat.id
        if user and chat_id and chat_id not in active_chats:
            active_chats.add(chat_id)
            asyncio.create_task(asyncio.to_thread(save_user_data, user.id, user.username, user.first_name))
        return await handler(event, data)


dp.update.middleware(UserTrackerMiddleware())


@dp.errors()
async def errors_handler(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, TelegramNetworkError):
        return True
    if isinstance(exc, TelegramBadRequest) and any(s in str(exc).lower() for s in ("query is too old", "not modified", "message to delete not found")):
        return True
    log.error(f"Необработанная ошибка: {exc}", exc_info=exc)
    return True


# =====================================================================
# ИНДИКАТОРЫ И СВЕЧНЫЕ ПАТТЕРНЫ
# =====================================================================
PATTERNS = {
    "bull_engulf": ("Бычье поглощение", 1, 12, "rev", "BULL ENGULF"),
    "bear_engulf": ("Медвежье поглощение", -1, 12, "rev", "BEAR ENGULF"),
    "hammer": ("Молот", 1, 10, "rev", "HAMMER"),
    "shooting_star": ("Падающая звезда", -1, 10, "rev", "SHOOT STAR"),
    "morning_star": ("Утренняя звезда", 1, 15, "rev", "MORNING STAR"),
    "evening_star": ("Вечерняя звезда", -1, 15, "rev", "EVENING STAR"),
    "three_white": ("Три белых солдата", 1, 12, "cont", "3 WHITE"),
    "three_black": ("Три чёрные вороны", -1, 12, "cont", "3 BLACK"),
    "piercing": ("Просвет в облаках", 1, 8, "rev", "PIERCING"),
    "dark_cloud": ("Завеса из тёмных облаков", -1, 8, "rev", "DARK CLOUD"),
    "bull_harami": ("Бычье харами", 1, 5, "rev", "BULL HARAMI"),
    "bear_harami": ("Медвежье харами", -1, 5, "rev", "BEAR HARAMI"),
    "tweezer_bottom": ("Пинцет (дно)", 1, 6, "rev", "TWEEZER BOT"),
    "tweezer_top": ("Пинцет (вершина)", -1, 6, "rev", "TWEEZER TOP"),
    "marubozu_bull": ("Бычий марубозу", 1, 6, "cont", "MARUBOZU+"),
    "marubozu_bear": ("Медвежий марубозу", -1, 6, "cont", "MARUBOZU-"),
    "doji": ("Доджи (неопределённость)", 0, 0, "neutral", "DOJI"),
}


def _wilder(s, n):
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


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
        if direction[i - 1] > 0:
            direction[i] = -1 if close[i] < fl[i] else 1
        else:
            direction[i] = 1 if close[i] > fu[i] else -1
    return direction


def add_patterns(df):
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    body = (c - o).abs()
    rng = (h - l).replace(0, np.nan)
    upper = h - np.maximum(o, c)
    lower = np.minimum(o, c) - l
    avg_body = body.rolling(10, min_periods=3).mean().fillna(body)
    bull, bear = c > o, c < o

    def sh(s, k=1):
        return s.shift(k, fill_value=False)

    p = {}
    p["doji"] = body <= 0.1 * rng
    p["hammer"] = (lower >= 2 * body) & (upper <= 0.15 * rng) & (body > 0.05 * rng)
    p["shooting_star"] = (upper >= 2 * body) & (lower <= 0.15 * rng) & (body > 0.05 * rng)
    p["bull_engulf"] = sh(bear) & bull & (o <= c.shift()) & (c >= o.shift()) & (body > body.shift())
    p["bear_engulf"] = sh(bull) & bear & (o >= c.shift()) & (c <= o.shift()) & (body > body.shift())
    p["morning_star"] = (sh(bear, 2) & (body.shift(2) > avg_body.shift(2)) & (body.shift(1) < 0.5 * avg_body.shift(1))
                         & bull & (c > (o.shift(2) + c.shift(2)) / 2))
    p["evening_star"] = (sh(bull, 2) & (body.shift(2) > avg_body.shift(2)) & (body.shift(1) < 0.5 * avg_body.shift(1))
                         & bear & (c < (o.shift(2) + c.shift(2)) / 2))
    p["three_white"] = (bull & sh(bull) & sh(bull, 2) & (c > c.shift()) & (c.shift() > c.shift(2)) & (o > o.shift())
                        & (o.shift() > o.shift(2)) & (o < c.shift()) & (o.shift() < c.shift(2)) & (body > 0.6 * avg_body))
    p["three_black"] = (bear & sh(bear) & sh(bear, 2) & (c < c.shift()) & (c.shift() < c.shift(2)) & (o < o.shift())
                        & (o.shift() < o.shift(2)) & (o > c.shift()) & (o.shift() > c.shift(2)) & (body > 0.6 * avg_body))
    p["piercing"] = sh(bear) & bull & (o < c.shift()) & (c > (o.shift() + c.shift()) / 2) & (c < o.shift())
    p["dark_cloud"] = sh(bull) & bear & (o > c.shift()) & (c < (o.shift() + c.shift()) / 2) & (c > o.shift())
    p["bull_harami"] = sh(bear) & bull & (o > c.shift()) & (c < o.shift()) & (body < 0.6 * body.shift())
    p["bear_harami"] = sh(bull) & bear & (o < c.shift()) & (c > o.shift()) & (body < 0.6 * body.shift())
    tol = 0.1 * df["atr"]
    p["tweezer_bottom"] = sh(bear) & bull & ((l - l.shift()).abs() <= tol)
    p["tweezer_top"] = sh(bull) & bear & ((h - h.shift()).abs() <= tol)
    p["marubozu_bull"] = bull & ((upper + lower) <= 0.1 * rng) & (body >= 1.3 * avg_body)
    p["marubozu_bear"] = bear & ((upper + lower) <= 0.1 * rng) & (body >= 1.3 * avg_body)
    for name, s in p.items():
        df[f"pt_{name}"] = s.fillna(False).astype(bool)
    return df


def pattern_scores(df):
    c, ema20, rsi, bbp = df["close"], df["ema20"], df["rsi"], df["bb_pct"]
    down_ctx = ((c < ema20) | (rsi < 45) | (bbp < 0.35)).values
    up_ctx = ((c > ema20) | (rsi > 55) | (bbp > 0.65)).values
    trend_up = (df["ema20"] > df["ema50"]).values
    score = np.zeros(len(df))
    for name, (_ru, side, wgt, kind, _en) in PATTERNS.items():
        if side == 0:
            continue
        flag = df[f"pt_{name}"].values.astype(float)
        if kind == "rev":
            ctx = np.where(down_ctx if side > 0 else up_ctx, 1.0, 0.4)
        else:
            ctx = np.where(trend_up if side > 0 else ~trend_up, 1.0, 0.5)
        score += side * wgt * flag * ctx
    return np.clip(score, -25, 25)


def add_indicators(df):
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    for n in (9, 20, 50, 200):
        df[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()

    delta = c.diff()
    ag, al = _wilder(delta.clip(lower=0), 14), _wilder(-delta.clip(upper=0), 14)
    rsi = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    df["rsi"] = rsi.where(al != 0, 100.0).fillna(50.0)
    rmin, rmax = df["rsi"].rolling(14).min(), df["rsi"].rolling(14).max()
    df["stoch_k"] = ((df["rsi"] - rmin) / (rmax - rmin).replace(0, np.nan)).rolling(3).mean().fillna(0.5)

    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["macd"] = macd
    df["macd_sig"] = macd.ewm(span=9, adjust=False).mean()
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

    # Дополнительные контекстные индикаторы: поток денег, каналы и режим волатильности.
    mf_mult = (((c - l) - (h - c)) / (h - l).replace(0, np.nan)).fillna(0.0)
    mf_vol = mf_mult * v
    df["cmf"] = (mf_vol.rolling(20).sum() / v.rolling(20).sum().replace(0, np.nan)).fillna(0.0)
    kc_mid = c.ewm(span=20, adjust=False).mean()
    kc_atr = df["atr"]
    df["kc_upper"] = kc_mid + 1.5 * kc_atr
    df["kc_lower"] = kc_mid - 1.5 * kc_atr
    df["kc_pos"] = ((c - df["kc_lower"]) /
                    (df["kc_upper"] - df["kc_lower"]).replace(0, np.nan)).clip(-1, 2).fillna(0.5)
    dc_hi = h.shift(1).rolling(20).max()
    dc_lo = l.shift(1).rolling(20).min()
    df["donchian_pos"] = ((c - dc_lo) / (dc_hi - dc_lo).replace(0, np.nan)).clip(-1, 2).fillna(0.5)
    atr_pct = (df["atr"] / c.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    df["atr_pct"] = atr_pct.fillna(0.0)
    df["volatility_percentile"] = atr_pct.rolling(100, min_periods=30).rank(pct=True).fillna(0.5)
    df["volume_pressure"] = (((c - o) / (h - l).replace(0, np.nan)) *
                             (v / v.rolling(20).mean().replace(0, np.nan))).clip(-5, 5).fillna(0.0)

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
    """Три модели (тренд / моментум / price action) с весами по режиму рынка. long_p 5..95."""
    n = len(df)
    c, adx = df["close"].values, df["adx"].values
    trending, ranging = adx >= 25, adx < 20

    def mix(t, r):
        return np.where(trending, t, np.where(ranging, r, (t + r) / 2.0))

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
    mom = np.clip(
        mix(np.clip((rsi - 50) * 0.8, -20, 20), np.clip((50 - rsi) * 0.9, -25, 25))
        + mix((sk - 0.5) * 30, (0.5 - sk) * 40)
        + mix(np.clip(cci / 10, -15, 15), np.clip(-cci / 8, -20, 20))
        + mix(((wr + 50) / 50) * 8, -((wr + 50) / 50) * 12)
        + mix((mfi - 50) / 50 * 8, (50 - mfi) / 50 * 12)
        + mix(np.where(bbp > 1, 10, np.where(bbp < 0, -10, (bbp - 0.5) * 10)), np.clip((0.5 - bbp) * 40, -20, 20))
        + mix(zeros, np.clip(-z * 7, -15, 15))
        + mix(np.clip(roc * 400, -10, 10), zeros), -100, 100)

    o = df["open"].values
    pat = pattern_scores(df)
    vol_conf = np.where(df["rel_vol"].values > 1.5, np.where(c > o, 10, -10), 0)
    obv_s = np.where(df["obv"].values > df["obv_ema"].values, 8, -8)
    div = np.where(df["div_bull"].values, 15, 0) - np.where(df["div_bear"].values, 15, 0)
    brk = np.where(df["brk_up"].values, 10, 0) - np.where(df["brk_dn"].values, 10, 0)
    vwap_s = np.where(c > df["vwap"].values, 5, -5)
    # CMF подтверждает направление денежного потока; Donchian/Keltner оценивают положение цены.
    cmf_s = np.clip(df["cmf"].values * 35, -8, 8)
    donchian_s = np.clip((df["donchian_pos"].values - 0.5) * 10, -5, 5)
    kc_s = np.clip((df["kc_pos"].values - 0.5) * 6, -3, 3)
    vol_pressure_s = np.clip(df["volume_pressure"].values * 1.5, -6, 6)
    legacy_pa = (pat + vol_conf + obv_s + div + brk + vwap_s) * 1.3
    new_context = (cmf_s + donchian_s + kc_s + vol_pressure_s) * 0.45
    pa = np.clip(legacy_pa + new_context, -100, 100)

    w_t = np.where(trending, 0.5, np.where(ranging, 0.25, 0.4))
    w_m = np.where(trending, 0.2, np.where(ranging, 0.45, 0.3))
    score = w_t * trend + w_m * mom + (1 - w_t - w_m) * pa
    df["long_p"] = np.nan_to_num(np.clip(50 + score / 2, 5, 95), nan=50.0)
    return df


def explain_last(df):
    r = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else r
    out = []

    def add(w, side, text):
        out.append({"w": w, "side": side, "text": text})

    c = r["close"]
    add(20, 1 if r["st_dir"] > 0 else -1, "Supertrend вверх" if r["st_dir"] > 0 else "Supertrend вниз")
    if c > r["ema20"] > r["ema50"] > r["ema200"]:
        add(22, 1, "Цена выше EMA 20/50/200")
    elif c < r["ema20"] < r["ema50"] < r["ema200"]:
        add(22, -1, "Цена ниже EMA 20/50/200")
    if r["ichi_pos"] > 0:
        add(16, 1, "Цена выше облака Ichimoku")
    elif r["ichi_pos"] < 0:
        add(16, -1, "Цена ниже облака Ichimoku")
    if r["adx"] >= 25:
        add(15, 1 if r["plus_di"] > r["minus_di"] else -1, f"Сильный тренд (ADX {r['adx']:.0f})")
    if r["macd_hist"] > 0 and r["macd_hist"] > prev["macd_hist"]:
        add(10, 1, "MACD: бычий импульс растёт")
    elif r["macd_hist"] < 0 and r["macd_hist"] < prev["macd_hist"]:
        add(10, -1, "MACD: медвежий импульс растёт")
    if r["rsi"] < 30:
        add(14, 1, f"RSI перепродан ({r['rsi']:.0f})")
    elif r["rsi"] > 70:
        add(14, -1, f"RSI перекуплен ({r['rsi']:.0f})")
    if r["div_bull"]:
        add(18, 1, "Бычья дивергенция RSI")
    if r["div_bear"]:
        add(18, -1, "Медвежья дивергенция RSI")
    if r.get("cmf", 0) >= 0.08:
        add(8, 1, f"CMF: приток капитала ({r['cmf']:+.2f})")
    elif r.get("cmf", 0) <= -0.08:
        add(8, -1, f"CMF: отток капитала ({r['cmf']:+.2f})")
    if r.get("volatility_percentile", 0.5) >= 0.90:
        add(7, 0, "Волатильность в верхних 10% истории: риск резких колебаний")
    elif r.get("volatility_percentile", 0.5) <= 0.15:
        add(5, 0, "Низкая волатильность: возможна фаза сжатия")
    if r.get("donchian_pos", 0.5) >= 0.95:
        add(7, 1, "Цена у верхней границы канала Donchian")
    elif r.get("donchian_pos", 0.5) <= 0.05:
        add(7, -1, "Цена у нижней границы канала Donchian")
    if r["bb_pct"] < 0:
        add(12, 1, "Цена под нижней полосой Боллинджера")
    elif r["bb_pct"] > 1:
        add(12, -1, "Цена над верхней полосой Боллинджера")
    if r["rel_vol"] > 1.5:
        add(12, 1 if c > r["open"] else -1, f"Всплеск объёма ×{r['rel_vol']:.1f}")
    if r["brk_up"]:
        add(14, 1, "Пробой 20-свечного максимума")
    if r["brk_dn"]:
        add(14, -1, "Пробой 20-свечного минимума")
    obv_up = r["obv"] > r["obv_ema"]
    add(6, 1 if obv_up else -1, "OBV выше средней (приток)" if obv_up else "OBV ниже средней (отток)")
    for name, (ru, side, wgt, _k, _en) in PATTERNS.items():
        if side != 0 and bool(r[f"pt_{name}"]):
            add(wgt + 8, side, ru)
    return out


def last_patterns(df, depth=2):
    res = []
    for i in range(1, min(depth, len(df)) + 1):
        r = df.iloc[-i]
        for name, (ru, side, _w, _k, en) in PATTERNS.items():
            if bool(r[f"pt_{name}"]):
                res.append((ru, side, en))
    seen, out = set(), []
    for item in res:
        if item[0] not in seen:
            seen.add(item[0])
            out.append(item)
    return out


def support_resistance(df, price, k=3, lookback=120):
    hv, lv = df["high"].values[-lookback:], df["low"].values[-lookback:]
    res, sup = [], []
    for i in range(k, len(hv) - k):
        if hv[i] == hv[i - k:i + k + 1].max():
            res.append(float(hv[i]))
        if lv[i] == lv[i - k:i + k + 1].min():
            sup.append(float(lv[i]))
    below, above = [s for s in sup if s < price], [r for r in res if r > price]
    return {"support": max(below) if below else None, "resistance": min(above) if above else None}


def rows_to_df(rows):
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    df = df.drop_duplicates("ts").sort_values("ts")
    df = df[df["close"] > 0].reset_index(drop=True)
    if len(df) < MIN_CANDLES:
        raise ValueError("мало свечей")
    return df


def prepare_df(df):
    return add_scores(add_indicators(df))


def build_tf_data(rows_by_tf):
    """Индикаторы по ЗАКРЫТЫМ свечам (формирующуюся отбрасываем), цена живая."""
    out = {}
    for tf, rows in rows_by_tf.items():
        price = float(rows[-1][4])
        df = prepare_df(rows_to_df(rows[:-1]))
        last = df.iloc[-1]
        long_p, adx = float(last["long_p"]), float(last["adx"])
        out[tf] = {
            "df": df, "price": price,
            "ema20": float(last["ema20"]), "ema50": float(last["ema50"]), "ema200": float(last["ema200"]),
            "rsi": float(last["rsi"]), "atr": float(last["atr"]), "adx": adx,
            "macd_hist": float(last["macd_hist"]), "bb_pct": float(last["bb_pct"]),
            "recent_low": float(min(df["low"].tail(28).min(), price)),
            "recent_high": float(max(df["high"].tail(28).max(), price)),
            "long_prob": long_p, "confidence": max(long_p, 100 - long_p),
            "qv24": float((df["close"] * df["volume"]).tail(6).sum()),
            "regime": "тренд" if adx >= 25 else "флэт" if adx < 20 else "переход",
            "squeeze": bool(last["squeeze"]),
            "reasons": explain_last(df), "patterns": last_patterns(df), "sr": support_resistance(df, price),
        }
    return out


# =====================================================================
# БИРЖИ: свечи, цены, история
# =====================================================================
def _p_binance(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]


def _p_okx(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["data"]]


def _p_bybit(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["result"]["list"]]


def _p_kucoin(j):
    return [[float(x[0]) * 1000, float(x[1]), float(x[3]), float(x[4]), float(x[2]), float(x[5])] for x in j["data"]]


def _p_gate(j):
    return [[float(x[0]) * 1000, float(x[5]), float(x[3]), float(x[4]), float(x[2]), float(x[6])] for x in j]


def _p_bitget(j):
    return [[float(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j["data"]]


def _p_htx(j):
    return [[float(x["id"]) * 1000, float(x["open"]), float(x["high"]), float(x["low"]), float(x["close"]), float(x["amount"])]
            for x in j["data"]]


EXCHANGES = [
    {"name": "BINANCE", "url": "https://data-api.binance.vision/api/v3/klines", "parse": _p_binance,
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "interval": tf, "limit": min(n, 1000)}},
    {"name": "BYBIT", "url": "https://api.bybit.com/v5/market/kline", "parse": _p_bybit,
     "params": lambda c, tf, n: {"category": "spot", "symbol": f"{c}USDT", "limit": min(n, 1000),
                                 "interval": {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}[tf]}},
    {"name": "OKX", "url": "https://www.okx.com/api/v5/market/candles", "parse": _p_okx,
     "params": lambda c, tf, n: {"instId": f"{c}-USDT", "limit": str(min(n, 300)),
                                 "bar": {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}[tf]}},
    {"name": "MEXC", "url": "https://api.mexc.com/api/v3/klines", "parse": _p_binance,
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "limit": min(n, 1000),
                                 "interval": {"15m": "15m", "1h": "60m", "4h": "4h", "1d": "1d"}[tf]}},
    {"name": "KUCOIN", "url": "https://api.kucoin.com/api/v1/market/candles", "parse": _p_kucoin,
     "params": lambda c, tf, n: {"symbol": f"{c}-USDT", "type": {"15m": "15min", "1h": "1hour", "4h": "4hour", "1d": "1day"}[tf]}},
    {"name": "GATE", "url": "https://api.gateio.ws/api/v4/spot/candlesticks", "parse": _p_gate,
     "params": lambda c, tf, n: {"currency_pair": f"{c}_USDT", "limit": min(n, 1000), "interval": tf}},
    {"name": "BITGET", "url": "https://api.bitget.com/api/v2/spot/market/candles", "parse": _p_bitget,
     "params": lambda c, tf, n: {"symbol": f"{c}USDT", "limit": min(n, 1000),
                                 "granularity": {"15m": "15min", "1h": "1h", "4h": "4h", "1d": "1day"}[tf]}},
    {"name": "HTX", "url": "https://api.huobi.pro/market/history/kline", "parse": _p_htx,
     "params": lambda c, tf, n: {"symbol": f"{c.lower()}usdt", "size": min(n, 2000),
                                 "period": {"15m": "15min", "1h": "60min", "4h": "4hour", "1d": "1day"}[tf]}},
]


async def _fetch_klines(ex, coin, tf, limit=200, min_rows=None):
    j = await http_json(ex["url"], params=ex["params"](coin, tf, limit), timeout=9)
    rows = ex["parse"](j)
    rows.sort(key=lambda r: r[0])
    if len(rows) < (min_rows or MIN_CANDLES + 1):
        raise ValueError(f"мало свечей ({len(rows)})")
    return rows


def _note_error(coin, ex_name, err):
    fetch_errors.setdefault(coin, []).append(f"{ex_name}: {str(err)[:40]}")
    fetch_errors[coin] = fetch_errors[coin][-12:]


async def _try_exchange(ex, cand, tfs, coin=None):
    if time.time() < exchange_bans.get(ex["name"], 0):
        return None
    coin = coin or cand
    try:
        first = await _fetch_klines(ex, cand, tfs[0])
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
            if res:
                return res
            preferred_source.pop(coin, None)
    return await get_market_data(coin, tfs)


async def fetch_history(coin, tf="4h", limit=1000, min_rows=300):
    """Свечи с первой ответившей биржи. Запросы параллельно, не больше 3 одновременно, 429 → бан биржи."""
    order, seen = [], set()
    pref = preferred_source.get(coin)
    if pref:
        ex = next((e for e in EXCHANGES if e["name"] == pref[1]), None)
        if ex:
            order.append((pref[0], ex))
    for ex in EXCHANGES:
        for cand in SYMBOL_ALIASES.get(coin, [coin]):
            order.append((cand, ex))
    order = [(c, ex) for c, ex in order
             if (c, ex["name"]) not in seen and not seen.add((c, ex["name"])) and time.time() >= exchange_bans.get(ex["name"], 0)]
    sem = asyncio.Semaphore(2)

    async def one(cand, ex):
        async with sem:
            try:
                return await _fetch_klines(ex, cand, tf, limit, min_rows=min_rows)
            except Exception as err:
                if any(code in str(err) for code in ("429", "418", "403", "451")):
                    exchange_bans[ex["name"]] = time.time() + 300
                raise

    tasks = [asyncio.create_task(one(c, ex)) for c, ex in order]
    try:
        for coro in asyncio.as_completed(tasks):
            try:
                return await coro
            except Exception:
                continue
        return None
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def get_simple_price(coin):
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        for url, params, path in (
            ("https://data-api.binance.vision/api/v3/ticker/price", {"symbol": f"{cand}USDT"}, lambda j: j["price"]),
            ("https://api.bybit.com/v5/market/tickers", {"category": "spot", "symbol": f"{cand}USDT"}, lambda j: j["result"]["list"][0]["lastPrice"]),
            ("https://www.okx.com/api/v5/market/ticker", {"instId": f"{cand}-USDT"}, lambda j: j["data"][0]["last"]),
            ("https://api.mexc.com/api/v3/ticker/price", {"symbol": f"{cand}USDT"}, lambda j: j["price"]),
        ):
            try:
                return float(path(await http_json(url, params=params, timeout=5)))
            except Exception:
                continue
    for url, path in (
        (f"https://min-api.cryptocompare.com/data/price?fsym={coin}&tsyms=USDT", lambda j: j["USDT"]),
        (f"https://api.coinbase.com/v2/prices/{coin}-USD/spot", lambda j: j["data"]["amount"]),
        (f"https://api.kraken.com/0/public/Ticker?pair={coin}USDT", lambda j: list(j["result"].values())[0]["c"][0]),
    ):
        try:
            return float(path(await http_json(url, timeout=6)))
        except Exception:
            continue
    return None


_funding_cache = {}


async def get_funding(coin):
    c = _funding_cache.get(coin)
    if c and time.time() - c[0] < 600:
        return c[1]
    val = None
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        try:
            j = await http_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": f"{cand}USDT"}, timeout=5)
            val = float(j["result"]["list"][0]["fundingRate"])
            break
        except Exception:
            continue
    _funding_cache[coin] = (time.time(), val)
    _trim_timed_cache(_funding_cache, 120)
    return val


_ctx_cache = {"ts": 0.0, "data": {}}


async def get_context(force=False):
    """Тренд BTC и индекс Fear & Greed."""
    if not force and time.time() - _ctx_cache["ts"] < 600 and _ctx_cache["data"]:
        return _ctx_cache["data"]
    ctx = {"btc_dir": 0, "btc_conf": 50.0, "btc_txt": "нет данных", "fng": None, "fng_txt": ""}
    try:
        tf, _ = await get_market_data_light("BTC", ("4h", "1d"))
        p = (tf["4h"]["long_prob"] + tf["1d"]["long_prob"]) / 2
        ctx["btc_dir"] = 1 if p >= 55 else -1 if p <= 45 else 0
        ctx["btc_conf"] = max(p, 100 - p)
        ctx["btc_txt"] = "бычий ▲" if ctx["btc_dir"] == 1 else "медвежий ▼" if ctx["btc_dir"] == -1 else "флэт ▬"
        btc4 = tf.get("4h", {})
        ctx["btc_atr_pct"] = round(float(btc4.get("atr", 0) or 0) / max(1e-12, float(btc4.get("price", 0) or 0)) * 100, 3)
        ctx["btc_regime"] = market_regime(btc4.get("df")).get("name", "unknown")
    except Exception as e:
        log.info(f"Контекст BTC: {e}")
    try:
        j = await http_json("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
        ctx["fng"] = int(j["data"][0]["value"])
        ctx["fng_txt"] = str(j["data"][0].get("value_classification", ""))
    except Exception:
        pass
    _ctx_cache.update(ts=time.time(), data=ctx)
    return ctx


# =====================================================================
# ИИ: авто-подбор моделей, провайдеры, бесплатный GPT, аналитик
# =====================================================================
MODEL_CACHE = {"gemini": [], "groq": [], "openrouter": [], "ts": 0.0}
BAD_MODELS = {}
LAST_MODEL = {}
ENV_MODELS = {"gemini": GEMINI_MODEL, "groq": GROQ_MODEL, "openrouter": OPENROUTER_MODEL}
FALLBACK_MODELS = {
    "gemini": ["gemini-2.5-flash", "gemini-flash-latest", "gemini-2.0-flash", "gemini-2.5-flash-lite"],
    "groq": ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-20b", "gemma2-9b-it"],
    "openrouter": ["meta-llama/llama-3.3-70b-instruct:free", "deepseek/deepseek-chat-v3-0324:free",
                   "google/gemini-2.0-flash-exp:free", "qwen/qwen-2.5-72b-instruct:free", "mistralai/mistral-7b-instruct:free"],
}


def _ver(s):
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0


def pick_gemini_models(payload):
    names = []
    for m in payload.get("models", []):
        n = str(m.get("name", "")).replace("models/", "")
        if "generateContent" not in m.get("supportedGenerationMethods", []) or "flash" not in n:
            continue
        if any(x in n for x in ("image", "tts", "audio", "live", "embedding", "exp", "preview", "thinking", "vision", "robotics")):
            continue
        names.append(n)
    names.sort(key=lambda n: (not n.endswith("latest"), "lite" in n, -_ver(n)))
    return names[:5]


def pick_groq_models(payload):
    bad = ("whisper", "guard", "tts", "playai", "distil", "embed", "orpheus", "safeguard")
    ids = [m["id"] for m in payload.get("data", []) if m.get("active", True) and not any(b in m["id"].lower() for b in bad)]
    pref = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama", "qwen", "gemma"]

    def rank(i):
        for idx, p in enumerate(pref):
            if p == i:
                return idx - 100
            if p in i:
                return idx
        return 99

    return sorted(ids, key=rank)[:5]


def pick_openrouter_models(payload):
    fam = ["llama-3.3-70b", "gemini", "deepseek", "qwen", "mistral", "gpt-oss", "llama"]
    free = []
    for m in payload.get("data", []):
        mid = str(m.get("id", ""))
        pr = m.get("pricing") or {}
        try:
            is_free = float(pr.get("prompt", 1)) == 0 and float(pr.get("completion", 1)) == 0
        except (TypeError, ValueError):
            is_free = mid.endswith(":free")
        if not is_free or any(x in mid.lower() for x in ("vision", "image", "audio", "embed", "moderation", "guard")):
            continue
        rank = next((i for i, f in enumerate(fam) if f in mid.lower()), 99)
        free.append((rank, -int(m.get("context_length") or 0), mid))
    return [x[2] for x in sorted(free)[:6]]


async def discover_models(force=False):
    if not force and time.time() - MODEL_CACHE["ts"] < 6 * 3600:
        return
    MODEL_CACHE["ts"] = time.time()

    async def get(url, headers=None, params=None):
        async with http.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.json(content_type=None)

    async def gem():
        if not GEMINI_API_KEYS:
            return []
        return pick_gemini_models(await get("https://generativelanguage.googleapis.com/v1beta/models",
                                            {"x-goog-api-key": GEMINI_API_KEYS[0]}, {"pageSize": 200}))

    async def grq():
        if not GROQ_API_KEY:
            return []
        return pick_groq_models(await get("https://api.groq.com/openai/v1/models", {"Authorization": f"Bearer {GROQ_API_KEY}"}))

    async def opr():
        if not OPENROUTER_API_KEY:
            return []
        return pick_openrouter_models(await get("https://openrouter.ai/api/v1/models", {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}))

    res = await asyncio.gather(gem(), grq(), opr(), return_exceptions=True)
    for name, r in zip(("gemini", "groq", "openrouter"), res):
        if isinstance(r, list) and r:
            MODEL_CACHE[name] = r
        elif isinstance(r, Exception):
            log.info(f"Подбор моделей {name}: {r}")


async def models_for(prov):
    await discover_models()
    seq = []
    for m in [LAST_MODEL.get(prov), ENV_MODELS.get(prov)] + MODEL_CACHE[prov] + FALLBACK_MODELS[prov]:
        if m and m not in seq and BAD_MODELS.get((prov, m), 0) < time.time():
            seq.append(m)
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
                    log.warning(f"Gemini/{model} {r.status}: {str(j)[:100]}")
                    if r.status in (401, 403):
                        break
                    if r.status in (400, 404):
                        _mark_bad("gemini", model)
            except Exception as e:
                log.warning(f"Gemini/{model}: {e}")
    return None


async def _ask_openai_compat(prov, base, key, prompt, extra_headers=None):
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    for model in await models_for(prov):
        payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 1100}
        try:
            async with http.post(f"{base}/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
                if r.status == 200:
                    txt = j["choices"][0]["message"]["content"]
                    if txt:
                        LAST_MODEL[prov] = model
                        return txt
                    continue
                log.warning(f"{prov}/{model} {r.status}: {str(j)[:100]}")
                if r.status in (401, 403):
                    return None
                if r.status in (400, 404):
                    _mark_bad(prov, model)
        except Exception as e:
            log.warning(f"{prov}/{model}: {e}")
    return None


def _looks_like_junk(text):
    t = (text or "").strip()
    low = t.lower()
    return len(t) < 30 or low.startswith("<!doctype") or low.startswith("<html") or any(x in low for x in ("pollinations", "deprecat", "rate limit"))


async def _ask_free_gpt(prompt):
    system = "Ты опытный крипто-аналитик. Отвечай на русском языке, кратко и по делу."
    auth = {"Authorization": f"Bearer {POLLINATIONS_KEY}"} if POLLINATIONS_KEY else {}

    async def post_openai(url, timeout):
        payload = {"model": POLLINATIONS_MODEL, "temperature": 0.2,
                   "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
        async with http.post(url, json=payload, headers={"Content-Type": "application/json", **auth},
                             timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return (await r.json(content_type=None))["choices"][0]["message"]["content"]

    async def get_plain(url, timeout):
        async with http.get(url, params={"model": POLLINATIONS_MODEL, "system": system}, headers=auth,
                            timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.text()

    short = quote(prompt[:3000], safe="")
    attempts = [lambda: post_openai("https://text.pollinations.ai/openai", 14),
                lambda: get_plain(f"https://gen.pollinations.ai/text/{short}", 10),
                lambda: get_plain(f"https://text.pollinations.ai/{short}", 8)]
    if POLLINATIONS_KEY:
        attempts.insert(0, lambda: post_openai("https://gen.pollinations.ai/v1/chat/completions", 14))
    for fn in attempts:
        try:
            text = await fn()
            if text and not _looks_like_junk(text):
                return text
        except Exception as e:
            log.info(f"Free GPT: {e}")
    return None


async def ask_ai_named(prompt, timeout=20, validate=None, free_timeout=30):
    """(текст, провайдер). Ключевые провайдеры параллельно, затем бесплатный GPT."""
    providers = []
    if GEMINI_API_KEYS:
        providers.append(("Gemini", lambda: _ask_gemini(prompt)))
    if GROQ_API_KEY:
        providers.append(("Groq", lambda: _ask_openai_compat("groq", "https://api.groq.com/openai/v1", GROQ_API_KEY, prompt)))
    if OPENROUTER_API_KEY:
        providers.append(("OpenRouter", lambda: _ask_openai_compat("openrouter", "https://openrouter.ai/api/v1", OPENROUTER_API_KEY, prompt,
                                                                    {"HTTP-Referer": "https://t.me/bot", "X-Title": "IXCLY"})))

    async def wrap(name, fn):
        try:
            t = await fn()
        except Exception:
            return None
        return (t, name) if t and len(t) > 20 and (validate is None or validate(t)) else None

    if providers:
        res = await first_success([wrap(n, f) for n, f in providers], timeout=timeout)
        if res:
            return res
        log.warning("Ключевые ИИ не дали пригодного ответа, пробую бесплатный GPT")
    res = await first_success([wrap("GPT (free)", lambda: _ask_free_gpt(prompt))], timeout=free_timeout)
    return res if res else (None, None)


_ai_cache = {}


# =====================================================================
# НОВОСТИ
# =====================================================================
BULL_STEMS = ("surg", "soar", "rall", "approv", "adopt", "inflow", "bullish", "gain", "jump", "partner", "upgrad", "breakout", "rebound", "recover", "accumul")
BEAR_STEMS = ("hack", "exploit", "crash", "plung", "lawsuit", "sues", "outflow", "bearish", "drop", "fall", "liquidat", "fraud", "scam",
              "delist", "arrest", "dump", "selloff", "declin", "probe", "investigat", "collaps", "bankrupt", "fear", "slump", "tumbl", "recession", "tariff")
COIN_WORDS = {"bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL", "xrp": "XRP",
              "bnb": "BNB", "doge": "DOGE", "ada": "ADA", "ton": "TON", "avax": "AVAX", "link": "LINK", "zcash": "ZEC"}


def news_sentiment(text, threshold=2):
    score = 0
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        if w.startswith(BULL_STEMS):
            score += 1
        elif w.startswith(BEAR_STEMS):
            score -= 1
    return 1 if score >= threshold else -1 if score <= -threshold else 0


def mentioned_coins(text):
    found = []
    for w in re.findall(r"[a-z]+", (text or "").lower()):
        t = COIN_WORDS.get(w)
        if t and t not in found:
            found.append(t)
    return found[:4]


async def translate_text(text, target="ru"):
    text = (text or "").strip()
    if not text:
        return text
    try:
        async with http.get("https://translate.googleapis.com/translate_a/single",
                            params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text[:1800]},
                            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                out = "".join(part[0] for part in data[0] if part and part[0])
                if has_cyr(out, 3):
                    return out
    except Exception:
        pass
    return text


NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/", "https://www.theblock.co/rss.xml", "https://cointelegraph.com/rss",
    "https://decrypt.co/feed", "https://cryptoslate.com/feed/", "https://bitcoinmagazine.com/.rss/full/",
    "https://www.dlnews.com/arc/outboundfeeds/rss/", "https://blockworks.co/feed", "https://thedefiant.io/feed",
    "https://u.today/rss", "https://finance.yahoo.com/rss/cryptocurrency", "https://watcher.guru/news/feed",
    "https://www.cryptopolitan.com/feed/", "https://blog.coinmarketcap.com/feed/", "https://forklog.com/feed",
    "https://bits.media/rss/news/", "https://coinspot.io/feed/", "https://www.rbc.ru/crypto/rss",
]
_news_cache = {"ts": 0.0, "entries": []}
_news_article_cache = {}  # URL -> enriched source data; short TTL avoids repeated publisher requests.
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


async def _load_feed(url):
    try:
        async with http.get(url, headers=BROWSER_UA, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return []
            text = await r.text()
        return list((await asyncio.to_thread(feedparser.parse, text)).entries)
    except Exception:
        return []


async def get_news_entries():
    if _news_cache["entries"] and time.time() - _news_cache["ts"] < 300:
        return _news_cache["entries"]
    results = await asyncio.gather(*[_load_feed(u) for u in NEWS_FEEDS], return_exceptions=True)
    entries = [e for r in results if isinstance(r, list) for e in r]
    if entries:
        entries.sort(key=lambda x: getattr(x, "published_parsed", None) or time.gmtime(0), reverse=True)
        _news_cache.update(ts=time.time(), entries=entries[:10])
    return _news_cache["entries"]


async def enrich_news_entry(entry):
    """Получает доступный текст новости: RSS -> публичная HTML-страница -> метаданные."""
    url = str(getattr(entry, "link", "") or "").strip()
    now = time.time()
    cached = _news_article_cache.get(url)
    if url and cached and now - cached.get("ts", 0) < 900:
        return dict(cached)

    rss_text = extract_rss_text(entry)
    best_text = rss_text
    method = "rss_content" if len(rss_text) >= 60 else "rss_title_only"
    description = rss_text[:1200]
    page_title = ""

    # Полную страницу запрашиваем только когда RSS не дал содержательного текста.
    if len(rss_text) < 500 and url.startswith(("https://", "http://")):
        try:
            async with http.get(url, headers=BROWSER_UA,
                                timeout=aiohttp.ClientTimeout(total=7),
                                allow_redirects=True) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                if response.status == 200 and "html" in content_type:
                    raw = await response.content.read(800_000)
                    charset = response.charset or "utf-8"
                    page = raw.decode(charset, errors="replace")
                    extracted = parse_article_html(page)
                    page_title = extracted.get("title", "")
                    page_body = extracted.get("body", "") or ""
                    page_desc = extracted.get("description", "") or ""
                    if len(page_body) > len(best_text):
                        best_text = page_body
                        method = extracted.get("method", "html_article")
                    elif len(page_desc) > len(best_text):
                        best_text = page_desc
                        method = "open_graph_or_meta_description"
                    if page_desc:
                        description = page_desc[:1200]
                elif response.status in (401, 402, 403, 451):
                    method = "publisher_restricted"
                elif response.status in (429, 503):
                    method = "publisher_rate_limited"
        except Exception as exc:
            log.debug("Не удалось извлечь текст новости %s: %s", url, exc)
            if not rss_text:
                method = "page_fetch_failed"

    coverage = assess_news_coverage(best_text)
    result = {"text": best_text[:12000], "description": description,
              "page_title": page_title, "method": method,
              "level": coverage["level"], "confidence": coverage["confidence"],
              "text_chars": coverage["text_chars"], "ts": now}
    if url:
        _news_article_cache[url] = result
        # Keep memory bounded on the 512 MB service.
        if len(_news_article_cache) > 60:
            oldest = sorted(_news_article_cache, key=lambda k: _news_article_cache[k].get("ts", 0))[:20]
            for key in oldest:
                _news_article_cache.pop(key, None)
    return result


def _normalise_image_url(url):
    url = html.unescape(str(url or "").strip())
    if url.startswith("//"):
        url = "https:" + url
    if not url or not url.lower().startswith(("https://", "http://")):
        return None
    if any(x in url.lower() for x in ("gravatar.com", "pixel", "spacer.gif", "logo.svg")):
        return None
    return url


def extract_image(entry):
    """Извлекает изображение из разных форматов RSS/Atom."""
    candidates = []
    for item in (getattr(entry, "media_content", None) or []):
        if isinstance(item, dict):
            candidates.append(item.get("url") or item.get("href"))
    for item in (getattr(entry, "media_thumbnail", None) or []):
        if isinstance(item, dict):
            candidates.append(item.get("url") or item.get("href"))
    for item in (getattr(entry, "links", None) or []):
        if isinstance(item, dict) and (item.get("rel") == "enclosure" or "image" in str(item.get("type", "")).lower()):
            candidates.append(item.get("href"))
    for item in (getattr(entry, "enclosures", None) or []):
        if isinstance(item, dict):
            candidates.append(item.get("href") or item.get("url"))
    html_parts = [getattr(entry, "summary", "") or ""]
    for item in (getattr(entry, "content", None) or []):
        if isinstance(item, dict):
            html_parts.append(item.get("value", ""))
    for part in html_parts:
        part = str(part)
        candidates.extend(re.findall(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', part, flags=re.I))
        candidates.extend(re.findall(r'<source[^>]+src=["\']([^"\']+)["\']', part, flags=re.I))
    for candidate in candidates:
        url = _normalise_image_url(candidate)
        if url:
            return url
    return None


async def resolve_news_image(entry):
    """RSS-картинка или og:image/twitter:image со страницы публикации."""
    image_url = extract_image(entry)
    if image_url:
        return image_url
    article_url = _normalise_image_url(getattr(entry, "link", ""))
    if not article_url:
        return None
    try:
        async with http.get(article_url, headers=BROWSER_UA,
                            timeout=aiohttp.ClientTimeout(total=6),
                            allow_redirects=True) as response:
            if response.status != 200 or "html" not in response.headers.get("Content-Type", "").lower():
                return None
            page = (await response.text(errors="ignore"))[:500_000]
        patterns = (
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
            r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']',
        )
        for pattern in patterns:
            match = re.search(pattern, page, flags=re.I)
            if match:
                url = _normalise_image_url(match.group(1))
                if url:
                    return url
    except Exception as exc:
        log.debug("Не удалось получить изображение новости: %s", exc)
    return None


def clean_summary(raw):
    t = html.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    t = re.sub(r"The post .*? appeared first on .*", "", t, flags=re.S | re.I)
    t = re.sub(r"https?://\S+", " ", t)
    return " ".join(t.split())[:900]


def clean_news_field(value, max_chars=360, max_sentences=2):
    """Удаляет нумерацию, пустые пункты, повторы и чрезмерно длинные ответы ИИ."""
    value = html.unescape(strip_tags(str(value or "")))
    value = re.sub(r"(?i)^(?:суть|summary|impact|влияние|coins|монеты)\s*[:：-]\s*", "", value)
    value = re.sub(r"(?m)^\s*(?:[-*•]|\d+[.)])\s*", "", value)
    value = re.sub(r"(?<!\w)\d+[.)](?=\s|$)", " ", value)
    value = re.sub(r"\s+", " ", value).strip(" \t\r\n.;,:—-")
    sentences = re.split(r"(?<=[.!?])\s+", value)
    unique, seen = [], set()
    for sentence in sentences:
        sentence = sentence.strip(" \t\r\n—-")
        key = re.sub(r"[^a-zа-яё0-9]", "", sentence.lower())
        if len(key) < 8 or key in seen:
            continue
        seen.add(key)
        unique.append(sentence)
        if len(unique) >= max_sentences:
            break
    value = " ".join(unique) if unique else value
    return shorten(value, max_chars) if value else ""


def parse_news_ai(text):
    t = (text or "").replace("**", "").replace("```", "")
    ms = list(re.finditer(r"(?im)^\s*(BIAS|TITLE_RU|SUMMARY|IMPACT|COINS)\s*[:：]\s*", t))
    out = {}
    for i, match in enumerate(ms):
        finish = ms[i + 1].start() if i + 1 < len(ms) else len(t)
        out[match.group(1).upper()] = re.sub(r"\s+", " ", t[match.end():finish]).strip()
    return out


def _valid_news_ai(text):
    p = parse_news_ai(text)
    bias_ok = any(x in p.get("BIAS", "").upper() for x in ("BULL", "BEAR", "NEUTRAL", "БЫЧ", "МЕДВ", "НЕЙТР"))
    summary = p.get("SUMMARY", "").strip()
    impact = p.get("IMPACT", "").strip()
    # Если модель снова вернула нумерованные пункты/смешала поля — не публикуем этот ответ,
    # а используем короткий перевод оригинального RSS-текста.
    malformed = bool(re.search(r"(?<!\w)\d+[.)]\s|акции\s+Binance", summary + " " + impact, re.I))
    return (bias_ok and has_cyr(p.get("TITLE_RU", ""), 6)
            and 20 <= len(summary) <= 600 and 10 <= len(impact) <= 450
            and not malformed and bool(p.get("COINS", "").strip()))


IMPACT_TEMPLATES = {
    1: "Фон позитивный: интерес к рисковым активам может поддержать спрос и рост цен на BTC, ETH и альткоины.",
    -1: "Фон негативный: возможны распродажи и рост волатильности. Соблюдайте риск-менеджмент.",
    0: "Фон нейтральный: рынок ждёт новых триггеров, сигналам нужно дополнительное подтверждение.",
}


async def update_market_bias(force=False):
    global market_bias
    async with news_lock:
        try:
            entries = await get_news_entries()
            if not entries:
                return
            entry = entries[0]
            link, now = getattr(entry, "link", ""), time.time()
            if not force and market_bias.get("link") == link and market_bias.get("engine") not in ("none", "fallback", "preliminary"):
                market_bias["ts"] = now
                return
            title = html.unescape(getattr(entry, "title", "") or "").strip()
            source = await enrich_news_entry(entry)
            title = title or str(source.get("page_title", "") or "").strip()
            summary = source.get("text", "") or source.get("description", "") or ""
            published = get_moscow_time_from_entry(entry)
            level = source.get("level", "title_only")
            confidence = source.get("confidence", "low")
            source_method = source.get("method", "rss_title_only")

            if level == "title_only":
                # Заголовок можно показать и перевести, но нельзя выдавать его за фактический разбор.
                title_ru = await translate_text(title)
                summary_ru = "Предварительно по заголовку: " + clean_news_field(title_ru, 220, 1)
                impact_ru = ("По одному заголовку нельзя надёжно определить направление рынка. "
                             "Нужны подтверждение в других источниках и реакция цены и объёма.")
                bias = 0
                coins = ", ".join(mentioned_coins(title)) or "Не указаны в источнике"
                engine = "preliminary"
                title_ru = clean_news_field(title_ru, 140, 1) or title[:140]
            else:
                prompt = f"""Ты редактор новостной ленты крипторынка. Используй ТОЛЬКО факты из переданного источника.
Не додумывай суммы, виновников, последствия, тикеры и причинно-следственные связи.
Не называй криптовалюты акциями. Если актив не назван прямо, укажи в COINS: Не указаны.
Не повторяй заголовок в SUMMARY. Не используй нумерацию и списки.
УРОВЕНЬ ИСТОЧНИКА: {level}; символов текста: {source.get('text_chars', 0)}; метод: {source_method}.
Если источник неполный, обязательно назови вывод предварительным и не заполняй пробелы догадками.
Верни ровно 5 полей, каждое с новой строки:
BIAS: BULLISH или BEARISH или NEUTRAL
TITLE_RU: точный перевод заголовка, до 140 символов
SUMMARY: 1-2 коротких предложения, только подтверждённые факты, до 320 символов
IMPACT: 1-2 коротких предложения, осторожное объяснение возможного влияния, до 240 символов
COINS: только тикеры, прямо подтверждённые текстом, через запятую; иначе Не указаны

ЗАГОЛОВОК: {title[:350]}
ТЕКСТ ИСТОЧНИКА: {summary[:6000]}"""
                text, engine = await ask_ai_named(prompt, validate=_valid_news_ai)
                if text and _valid_news_ai(text):
                    p = parse_news_ai(text)
                    up = p.get("BIAS", "").upper()
                    bias = 1 if ("BULL" in up or "БЫЧ" in up) else -1 if ("BEAR" in up or "МЕДВ" in up) else 0
                    summary_ru = clean_news_field(p.get("SUMMARY", ""), 320, 2)
                    impact_ru = clean_news_field(p.get("IMPACT", ""), 240, 2)
                    allowed = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TON", "AVAX", "LINK", "DOT", "LTC", "TRX", "USDT", "USDC"}
                    raw_coins = p.get("COINS", "").upper()
                    coin_tokens = [x for x in re.findall(r"\b[A-Z]{2,8}\b", raw_coins) if x in allowed]
                    source_coins = mentioned_coins(f"{title} {summary}")
                    coins = ", ".join(dict.fromkeys(coin_tokens or source_coins)) or "Не указаны в источнике"
                    if not summary_ru:
                        summary_ru = clean_news_field(await translate_text(summary or title), 320, 2)
                    if not impact_ru:
                        impact_ru = IMPACT_TEMPLATES[bias]
                    title_ru = clean_news_field(p.get("TITLE_RU", ""), 140, 1)
                else:
                    # Резервный путь: только доступный RSS/страничный текст, без выдуманных деталей.
                    bias = news_sentiment(f"{title}. {summary}") if len(summary) >= 180 else 0
                    title_ru = clean_news_field(await translate_text(title), 140, 1)
                    summary_ru = clean_news_field(await translate_text(summary), 320, 2) if summary else "Недостаточно данных для пересказа."
                    impact_ru = ("Оценка предварительная: направление рынка требует подтверждения ценой, объёмом и независимыми источниками."
                                 if level == "partial" else IMPACT_TEMPLATES[bias])
                    coins = ", ".join(mentioned_coins(f"{title} {summary}")) or "Не указаны в источнике"
                    engine = "fallback"
                if level == "partial":
                    summary_ru = "Предварительно: " + (summary_ru or "доступна только часть материала")
                    impact_ru = "Предварительная оценка. " + (impact_ru or "Нужны дополнительные подтверждения.")

            market_bias = {"bias": bias, "title_ru": title_ru,
                           "summary": summary_ru, "impact": impact_ru, "coins": coins,
                           "published": published, "link": link, "engine": engine, "ts": now,
                           "analysis_level": level, "confidence": confidence,
                           "source_method": source_method, "source_text_chars": source.get("text_chars", 0)}
            # Severity/relevance is computed locally from available source text; it never
            # treats a missing body as proof of a neutral event or triggers a panic by itself.
            crit_score, _crit_reason = classify_critical(title, summary)
            # A headline-only/partial record must not trigger a market-wide panic state.
            crit_score = cap_critical_score(crit_score, level)
            market_bias["severity"] = "critical" if crit_score >= 7 else "high" if crit_score >= 5 else "medium" if crit_score >= 3 else "low"
            market_bias["critical_score"] = int(crit_score)
            state_touch(False)
        except Exception as e:
            log.warning(f"Ошибка обновления новостей: {e}")


def format_news_post(nb, limit=1000):
    """Короткая новостная карточка в одном стиле для Telegram."""
    emoji = {1: "🟢", 0: "⚪", -1: "🔴"}.get(nb.get("bias", 0), "⚪")
    level = nb.get("analysis_level", "full")
    label = {1: "Позитивно", 0: "Нейтрально", -1: "Негативно"}.get(nb.get("bias", 0), "Нейтрально")
    if level == "title_only":
        emoji, label = "⚪", "Недостаточно данных · предварительно"
    elif level == "partial":
        label = "Предварительно · " + label.lower()
    link = f'\n\n🔗 <a href="{html.escape(nb["link"], quote=True)}">Источник</a>' if nb.get("link") else ""
    title = clean_news_field(nb.get("title_ru", ""), 140, 1)
    summary = clean_news_field(nb.get("summary", ""), 300, 2) or "В источнике недостаточно подробностей для краткого пересказа."
    impact = clean_news_field(nb.get("impact", ""), 220, 2) or IMPACT_TEMPLATES.get(nb.get("bias", 0), IMPACT_TEMPLATES[0])
    coins = clean_news_field(nb.get("coins", ""), 90, 1) or "Не указаны в источнике"
    coverage = (f"\n<em>Основа: {html.escape(str(nb.get('source_method', 'RSS')))} · "
                f"уверенность: {html.escape(str(nb.get('confidence', 'low')))} · "
                f"текста: {int(nb.get('source_text_chars', 0) or 0)} знаков</em>"
                if level in ("partial", "title_only") else "")
    text = (f"📰 <b>{html.escape(title)}</b>\n"
            f"🕒 <i>{html.escape(nb.get('published', ''))}</i>\n\n"
            f"📌 <b>Суть</b>\n{html.escape(summary)}\n\n"
            f"📊 <b>Влияние</b>\n{emoji} <b>{label}.</b> {html.escape(impact)}{coverage}\n\n"
            f"🪙 <b>Монеты:</b> {html.escape(coins)}{link}")
    return trim_html(text, limit)


async def prepare_news(force=False):
    await update_market_bias(force=force)
    entries = await get_news_entries()
    if not entries or market_bias.get("engine") == "none":
        return None, None
    # Используем картинку именно той новости, которую разобрал ИИ.
    target_link = market_bias.get("link")
    entry = next((e for e in entries if getattr(e, "link", None) == target_link), entries[0])
    image_url = await resolve_news_image(entry)
    image_data = await download_bytes(image_url) if image_url else None
    return format_news_post(market_bias), image_data


async def get_news_batch_text():
    """Детерминированный дайджест: без свободной генерации, пустых пунктов и выдуманных активов."""
    entries = await get_news_entries()
    if not entries:
        return "📭 <b>Свежих новостей пока нет.</b>"
    out = ["📰 <b>Последние новости рынка</b>"]
    used = set()
    for entry in entries[:3]:
        title = html.unescape(getattr(entry, "title", "") or "").strip()
        source = await enrich_news_entry(entry)
        summary = source.get("text", "") or source.get("description", "") or ""
        if not title:
            continue
        title_ru = clean_news_field(await translate_text(title), 150, 1) or title[:150]
        summary_ru = clean_news_field(await translate_text(summary), 260, 2) if summary else ""
        sentiment = news_sentiment(f"{title} {summary}") if len(summary) >= 180 else 0
        if source.get("level") == "title_only":
            label = "⚪ Предварительно · только заголовок, данных недостаточно"
        elif source.get("level") == "partial":
            label = "⚪ Предварительная оценка · источник неполный"
        else:
            label = "🟢 Позитивный фон" if sentiment > 0 else "🔴 Негативный фон" if sentiment < 0 else "⚪ Нейтрально"
        coins = ", ".join(mentioned_coins(f"{title} {summary}")) or "Не указаны в источнике"
        link = getattr(entry, "link", "") or ""
        key = (title.lower(), link)
        if key in used:
            continue
        used.add(key)
        out.append(f"\n<b>{html.escape(title_ru)}</b>")
        if summary_ru:
            out.append(html.escape(summary_ru))
        out.append(f"{label} · Монеты: {html.escape(coins)}")
        if source.get("level") != "full":
            out.append(f"<i>Источник: {html.escape(str(source.get('method', 'RSS')))} · уверенность: {html.escape(str(source.get('confidence', 'low')))}</i>")
        if link:
            out.append(f'<a href="{html.escape(link, quote=True)}">Источник</a>')
    return trim_html("\n\n".join(out), 3900)


# =====================================================================
# ОЦЕНКА СЕТАПА, УРОВНИ, БЭКТЕСТ
# =====================================================================
TF_WEIGHTS = {"1d": 0.30, "4h": 0.30, "1h": 0.25, "15m": 0.15}


def quick_check(tf_data):
    if "4h" not in tf_data or "1d" not in tf_data:
        return None
    wp = (tf_data["4h"]["long_prob"] + tf_data["1d"]["long_prob"]) / 2
    strength = max(wp, 100 - wp)
    if strength < CONF_STAGE1 or tf_data["4h"]["adx"] < 10:
        return None
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
    return [f"{tf}: {', '.join(p[0] for p in tf_data[tf]['patterns'][:3])}"
            for tf in ("1d", "4h", "1h") if tf in tf_data and tf_data[tf]["patterns"]]


def levels_core(price, atr, ema20, ema50, rec_low, rec_high, is_long, support=None, resistance=None):
    """Вход строго лимиткой на откате. Возвращает entry, sl1, risk."""
    if is_long:
        entry = min(price, max(ema20, ema50))
        if entry > price - 0.2 * atr:
            entry = price - 0.35 * atr
        if support and price - 1.5 * atr <= support <= price - 0.15 * atr:
            entry = min(max(entry, support + 0.1 * atr), price - 0.15 * atr)
        sl1 = max(min(rec_low - 0.5 * atr, entry - 0.8 * atr), entry - 2.5 * atr)
        return entry, sl1, entry - sl1
    entry = max(price, min(ema20, ema50))
    if entry < price + 0.2 * atr:
        entry = price + 0.35 * atr
    if resistance and price + 0.15 * atr <= resistance <= price + 1.5 * atr:
        entry = max(min(entry, resistance - 0.1 * atr), price + 0.15 * atr)
    sl1 = min(max(rec_high + 0.5 * atr, entry + 0.8 * atr), entry + 2.5 * atr)
    return entry, sl1, sl1 - entry


def calc_levels_and_risk(info, is_long):
    price = info["price"]
    atr = info["atr"] if info["atr"] > 0 else price * 0.012
    sr = info.get("sr", {})
    entry, sl1, risk = levels_core(price, atr, info["ema20"], info["ema50"], info["recent_low"], info["recent_high"],
                                   is_long, sr.get("support"), sr.get("resistance"))
    sign = 1 if is_long else -1
    atr_pct = (atr / price) * 100 if price > 0 else 0.0
    # Stop не уже динамического ATR-минимума, но исходный swing-уровень сохраняет
    # право сделать его шире. Верхняя граница ограничивает чрезмерный риск.
    adx = float(info.get("adx", 20) or 20)
    stop_mult = atr_stop_multiplier(adx, atr_pct)
    risk = min(max(risk, atr * stop_mult), atr * 3.0)
    sl1 = entry - sign * risk
    tp_multipliers = atr_target_multipliers(adx, atr_pct)
    tps = [entry + sign * risk * m for m in tp_multipliers]
    sl2 = sl1 - sign * 0.7 * atr
    if atr_pct > 3.0:
        leverage = "x3-x5 (Высокая волатильность ⚠)"
    elif atr_pct > 1.5:
        leverage = "x5-x10 (Средняя волатильность)"
    else:
        leverage = "x10-x20 (Низкая волатильность)"
    liq_x20 = entry * (1 - 0.0475) if is_long else entry * (1 + 0.0475)
    opp = sr.get("resistance") if is_long else sr.get("support")
    near = "ближайший уровень слишком близко к входу, цель может не отработать" if opp and abs(opp - entry) < 1.2 * risk else None
    return entry, tps, [sl1, sl2], risk, leverage, near, liq_x20


def backtest_df(df, conf_min=CONF_FINAL, rr1=1.5, fill_bars=6, max_hold=30, fee=0.001):
    """Симуляция стратегии на истории: лимитный вход с проверкой исполнения, TP1/SL, комиссии."""
    n = len(df)
    if n < 150:
        return None
    lp, adx, atr = df["long_p"].values, df["adx"].values, df["atr"].values
    c, h, l = df["close"].values, df["high"].values, df["low"].values
    e20, e50 = df["ema20"].values, df["ema50"].values
    low28, high28 = df["low"].rolling(28).min().values, df["high"].rolling(28).max().values
    results, i = [], 80
    while i < n - 2:
        p = lp[i]
        if max(p, 100 - p) < conf_min or adx[i] < 15 or atr[i] <= 0 or np.isnan(low28[i]):
            i += 1
            continue
        is_long = p >= 50
        entry, sl, risk = levels_core(c[i], atr[i], e20[i], e50[i], low28[i], high28[i], is_long)
        if risk <= 0:
            i += 1
            continue
        atr_pct_i = (atr[i] / c[i] * 100) if c[i] > 0 else 0.0
        risk = min(max(risk, atr[i] * atr_stop_multiplier(adx[i], atr_pct_i)), atr[i] * 3.0)
        sl = entry - risk if is_long else entry + risk
        tp1_mult = atr_target_multipliers(adx[i], atr_pct_i)[0]
        tp = entry + tp1_mult * risk if is_long else entry - tp1_mult * risk
        filled = next((j for j in range(i + 1, min(n, i + 1 + fill_bars))
                       if (is_long and l[j] <= entry) or ((not is_long) and h[j] >= entry)), None)
        if filled is None:
            i += 1
            continue
        res, last_k = None, filled
        for k in range(filled, min(n, filled + max_hold)):
            last_k = k
            if (l[k] <= sl) if is_long else (h[k] >= sl):
                res = -1.0
                break
            if (h[k] >= tp) if is_long else (l[k] <= tp):
                res = rr1
                break
        if res is None:
            res = max(-1.0, min(rr1, (((c[last_k] - entry) if is_long else (entry - c[last_k])) / risk)))
        results.append(res - fee * entry / risk)
        i = last_k + 1
    days = round(n * 4 / 24)
    if not results:
        return {"trades": 0, "winrate": 0.0, "avg_r": 0.0, "pf": 0.0, "days": days}
    arr = np.array(results)
    wins, losses = arr[arr > 0], arr[arr <= 0]
    pf = float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else 9.9
    return {"trades": len(arr), "winrate": round(float((arr > 0).mean() * 100), 1), "avg_r": round(float(arr.mean()), 2),
            "pf": round(min(pf, 9.9), 2), "days": days}


def run_backtest_rows(rows):
    return backtest_df(prepare_df(rows_to_df(rows[:-1])))


async def get_backtest(coin):
    if len(bt_cache) > 120:
        _trim_timed_cache(bt_cache, 90)
    c = bt_cache.get(coin)
    if c and time.time() - c[0] < 6 * 3600:
        return c[1]
    bt = None
    try:
        rows = await fetch_history(coin, "4h", 1000)
        if rows:
            bt = await asyncio.to_thread(run_backtest_rows, rows)
    except Exception as e:
        log.info(f"Бэктест {coin}: {e}")
    bt_cache[coin] = (time.time(), bt)
    return bt


def bt_ok(bt):
    return (bool(bt) and bt["trades"] >= BT_MIN_TRADES and bt["avg_r"] >= BT_MIN_AVG_R
            and bt["winrate"] >= BT_MIN_WINRATE and bt["pf"] >= float(os.getenv("BT_MIN_PF", "1.15")))


# =====================================================================
# ГРАФИК (PNG)
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
    except Exception:
        font = font_b = ImageFont.load_default()
    col = (0, 200, 90) if is_long else (230, 60, 60)
    draw.text((18, 10), f"{coin}/USDT  {main_tf}  {'LONG' if is_long else 'SHORT'}", fill=col, font=font_b)
    pad_l, pad_r, pad_t, pr_bottom, vol_top, vol_bottom = 62, 18, 42, 405, 425, 520
    cw, ch = w - pad_l - pad_r, pr_bottom - pad_t
    highs, lows = df["high"].values, df["low"].values
    levels = [entry, price] + list(tps) + list(sls)
    mn, mx = min(lows.min(), min(levels)) * 0.998, max(highs.max(), max(levels)) * 1.002
    rng = mx - mn or 1.0

    def y(p):
        return pad_t + ch - (p - mn) / rng * ch

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
        for lbl, k in (("0.236", 0.236), ("0.382", 0.382), ("0.500", 0.5), ("0.618", 0.618), ("0.786", 0.786)):
            yy = y(swing_high - diff * k)
            draw.line([(pad_l, yy), (w - pad_r, yy)], fill=(80, 80, 110), width=1)
            draw.text((pad_l + 4, yy - 14), f"Fib {lbl}", fill=(120, 120, 160), font=font)
    for lvl_p, lbl in ((info["sr"].get("support"), "SUP"), (info["sr"].get("resistance"), "RES")):
        if lvl_p:
            for xx in range(pad_l, w - pad_r, 10):
                draw.line([(xx, y(lvl_p)), (xx + 4, y(lvl_p))], fill=(130, 130, 150), width=1)
            draw.text((w - pad_r - 90, y(lvl_p) - 14), f"{lbl} {fmt(lvl_p)}", fill=(150, 150, 170), font=font)

    def lvl(p, label, color, dash=False):
        yy = y(p)
        if dash:
            for xx in range(pad_l, w - pad_r, 7):
                draw.line([(xx, yy), (xx + 3, yy)], fill=color, width=1)
        else:
            draw.line([(pad_l, yy), (w - pad_r, yy)], fill=color, width=2)
        draw.text((pad_l + 4, yy - 14), f"{label} {fmt(p)}", fill=color, font=font)

    lvl(price, "Price", (230, 230, 230), True)
    lvl(entry, "Entry", (255, 190, 0))
    for i, tp in enumerate(tps):
        lvl(tp, f"TP{i + 1}", (0, 190, 90), True)
    for i, sl in enumerate(sls):
        lvl(sl, f"SL{i + 1}", (230, 60, 60), True)
    if info["patterns"]:
        draw.text((pad_l + cw - 200, 12), "Pattern: " + info["patterns"][0][2], fill=(255, 210, 120), font=font_b)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# =====================================================================
# АНАЛИЗ МОНЕТЫ
# =====================================================================
def local_verdict(setup, bt):
    if not setup["valid"]:
        return "WAIT"
    if bt is None:
        return "ENTER" if setup["strength"] >= CONF_FINAL + 4 else "WAIT"
    return "ENTER" if (bt_ok(bt) or bt["trades"] < BT_MIN_TRADES) and setup["strength"] >= CONF_FINAL else "WAIT"


def age_text(ts):
    sec = max(0, int(time.time() - ts))
    return f"{sec // 3600} ч {sec % 3600 // 60} мин назад" if sec >= 3600 else f"{sec // 60} мин назад"


def price_change_pct(side, entry, price):
    """Направленная доходность цены: LONG и SHORT используют одну формулу."""
    entry, price = float(entry), float(price)
    if entry <= 0:
        return 0.0
    sign = 1.0 if str(side).upper() == "LONG" else -1.0
    return sign * (price - entry) / entry * 100.0


def rebase_signal_prices(res, new_entry):
    """Переносит уровни TP/SL на новый вход, сохраняя исходные относительные расстояния."""
    old_entry = float(res.get("entry") or 0)
    new_entry = float(new_entry)
    if old_entry <= 0 or new_entry <= 0:
        raise ValueError("Цена входа должна быть больше нуля")
    out = dict(res)
    for key in ("tps", "sls"):
        levels = res.get(key) or []
        out[key] = [new_entry * (float(level) / old_entry) for level in levels]
    out["entry"] = new_entry
    if out.get("price") is not None:
        out["entry_override"] = True
    # risk_dist — доля риска от нового входа до первого стопа
    sls = out.get("sls") or []
    out["risk_dist"] = abs(new_entry - float(sls[0])) / new_entry if sls else float(res.get("risk_dist") or 0)
    return out


def render_caption(res, uid=None):
    """Короткая карточка сигнала для Telegram-уведомления (≤1024 символов)."""
    coin = html.escape(res["coin"])
    if res.get("price_only"):
        return fit_caption([(0, f"💰 <b>{coin}</b>: <code>{fmt(res['price'])}</code>"),
                            (1, "ℹ Свечи недоступны, анализ невозможен.")])
    entry = res["entry"]

    def pct(p):
        return f"{price_change_pct(res['side'], entry, p):+.1f}%"

    s = get_user_risk_settings(uid) if uid else {"deposit": DEPOSIT, "risk_pct": RISK_PCT}
    risk_usdt = s["deposit"] * s["risk_pct"] / 100
    pos_usdt = risk_usdt / res["risk_dist"] * entry if res["risk_dist"] > 0 else 0.0
    verdict = "✅ <b>ВХОДИТЬ</b>" if res["verdict"] == "ENTER" else "⏳ <b>ЖДАТЬ</b>"
    emoji = "🟢" if res["is_long"] else "🔴"
    parts = []
    if res.get("stale"):
        parts.append((0, f"🕓 <i>Из архива: {age_text(res['ts'])}</i>"))
    parts += [
        (0, f"{emoji} <b>{res['side']} · {coin}</b>   {verdict}"),
        (1, f"📌 {res['type']} · Conf <b>{res['strength']}%</b> · {res['regime']}"),
        (1, make_strength_bar(res["strength"])),
    ]
    _quant = res.get("quant") or {}
    _bayes = _quant.get("bayesian_probability") or {}
    if _bayes.get("long_pct") is not None and _bayes.get("short_pct") is not None:
        parts.append((2, f"🧮 Bayesian LONG/SHORT: {_bayes['long_pct']:.1f}% / {_bayes['short_pct']:.1f}% · выборка {_bayes.get('closed_sample', 0)} закрытых"))
    if _quant.get("risk_score") is not None:
        parts.append((2, f"🛡 Risk Score: {_quant.get('risk_score')}/100 · {_quant.get('risk_state', 'YELLOW')}"))
    if _quant.get("monte_carlo", {}).get("tp_before_sl_pct") is not None:
        parts.append((3, f"🎲 Monte Carlo: TP раньше SL в {_quant['monte_carlo']['tp_before_sl_pct']:.1f}% сценариев (модель)"))
    if res.get("fake_breakout"):
        parts.append((1, "⚠️ <b>Ложный пробой (тонкий объем)</b>"))
    parts += [
        (2, f"📰 {html.escape(res['news_status'])}"),
        (3, f"🌍 BTC {res['btc_txt']}" + (f" · F&G {res['fng']}" if res.get("fng") is not None else "")),
        (7, ""),
        (0, f"📥 Вход <b>(только ЛИМИТ)</b>: <code>{fmt(entry)}</code>"),
        (1, f"💰 Цена сейчас: <code>{fmt(res['price'])}</code>"),
        (7, ""),
    ]
    parts += [(0, f"🎯 TP{i + 1} <code>{fmt(p)}</code> <i>{pct(p)}</i>") for i, p in enumerate(res["tps"])]
    parts += [(0, f"🛡 SL{i + 1} <code>{fmt(p)}</code> <i>{pct(p)}</i>") for i, p in enumerate(res["sls"])]
    parts.append((7, ""))
    parts.append((2, f"⚖ {res['leverage']} · позиция ${pos_usdt:.0f} · риск ${risk_usdt:.0f}"))
    if res.get("liq_x20"):
        parts.append((3, f"⚠ <i>При плече x20 ликвидация: <code>{fmt(res['liq_x20'])}</code></i>"))
    if res.get("bt"):
        b = res["bt"]
        parts.append((3, f"🧪 Бэктест: {b['trades']} сд · TP1 {b['winrate']}% · {b['avg_r']:+.2f}R"))
    if res.get("comment"):
        parts.append((4, f"💬 {html.escape(shorten(res['comment'], 170))}"))
    parts.append((8, "⚠ <i>Не фин. рекомендация</i>"))
    return fit_caption(parts, 1000)


# =====================================================================
# ФИЛЬТРЫ РЫНКА v2: OI+цена, ложные пробои и сбор ликвидности, стакан,
# сжатие, сессии, обвал BTC, история funding, Chandelier, ИИ по сырым данным
# =====================================================================
OI_TXT = {
    "new_longs": "рост OI и цены: открываются новые лонги",
    "new_shorts": "рост OI при падении цены: открываются новые шорты",
    "short_cover": "OI падает при росте цены: шорты закрываются, импульс слабее",
    "long_liq": "OI падает при падении цены: лонги закрываются",
}
OI_DELTA = {  # (LONG, SHORT)
    "new_longs": (6, -6), "new_shorts": (-6, 6), "short_cover": (-3, -5), "long_liq": (-5, 3),
}


async def oi_price_state(coin):
    """Матрица OI + цена за текущие 15 минут: понимаем, кто открывает позиции."""
    try:
        async with http.get("https://fapi.binance.com/futures/data/openInterestHist",
                            params={"symbol": f"{coin}USDT", "period": "15m", "limit": 2},
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return None, 0.0, 0.0
            data = await r.json(content_type=None)
        if len(data) < 2:
            return None, 0.0, 0.0
        oi_old, oi_new = float(data[0]["sumOpenInterest"]), float(data[1]["sumOpenInterest"])
        oi_pct = (oi_new - oi_old) / oi_old * 100 if oi_old else 0.0
        kl = await http_json("https://fapi.binance.com/fapi/v1/klines",
                             params={"symbol": f"{coin}USDT", "interval": "15m", "limit": 1}, timeout=5)
        o, c = float(kl[-1][1]), float(kl[-1][4])
        p_pct = (c - o) / o * 100 if o else 0.0
    except Exception as e:
        log.info(f"OI {coin}: {e}")
        return None, 0.0, 0.0
    if oi_pct > 0.5 and p_pct > 0.2:
        st = "new_longs"
    elif oi_pct > 0.5 and p_pct < -0.2:
        st = "new_shorts"
    elif oi_pct < -0.5 and p_pct > 0.2:
        st = "short_cover"
    elif oi_pct < -0.5 and p_pct < -0.2:
        st = "long_liq"
    else:
        st = None
    return st, round(oi_pct, 2), round(p_pct, 2)


_cross_exchange_price_cache = {}


async def cross_exchange_price_snapshot(coin):
    """Low-frequency cross-exchange spot comparison for shortlisted signal candidates."""
    cached = _cross_exchange_price_cache.get(coin)
    if cached and time.time() - cached[0] < 90:
        return cached[1]
    selected = [x for x in EXCHANGES if x.get("name") in ("BINANCE", "BYBIT", "OKX", "MEXC")]
    async def fetch_one(ex):
        try:
            rows = await _fetch_klines(ex, coin, "15m", limit=30, min_rows=5)
            return ex["name"], float(rows[-1][4])
        except Exception as exc:
            log.debug("Cross exchange %s %s: %s", coin, ex.get("name"), exc)
            return ex.get("name"), None
    results = await asyncio.gather(*[fetch_one(ex) for ex in selected[:4]], return_exceptions=True)
    quotes = {name: price for result in results if isinstance(result, tuple) for name, price in [result] if price and price > 0}
    report = cross_exchange_watch(quotes, max_spread_pct=.35)
    report["quotes"] = quotes
    _cross_exchange_price_cache[coin] = (time.time(), report)
    if len(_cross_exchange_price_cache) > 80:
        for key in sorted(_cross_exchange_price_cache, key=lambda k: _cross_exchange_price_cache[k][0])[:20]:
            _cross_exchange_price_cache.pop(key, None)
    return report


_cvd_cache = {}
_ob_snapshot_history = {}
_cvd_request_times = []
_cvd_lock = asyncio.Lock()


async def fetch_futures_cvd(coin, limit=500):
    """Signed-aggressor CVD proxy from Binance USD-M aggregate trades (public endpoint)."""
    coin = normalize_ticker(str(coin or ""))
    if not coin:
        return {"status": "invalid_symbol"}
    now = time.time()
    cached = _cvd_cache.get(coin)
    if cached and now - cached[0] < 60:
        return cached[1]
    async with _cvd_lock:
        cached = _cvd_cache.get(coin)
        if cached and now - cached[0] < 60:
            return cached[1]
        recent = [t for t in _cvd_request_times if now - t < 60]
        _cvd_request_times[:] = recent
        if len(recent) >= 4:
            return {"status": "deferred_rate_budget", "source": "Binance USD-M aggTrades"}
        _cvd_request_times.append(now)
        try:
            rows = await http_json("https://fapi.binance.com/fapi/v1/aggTrades",
                params={"symbol": f"{coin}USDT", "limit": max(100, min(1000, int(limit)))}, timeout=7)
            result = signed_cvd_from_agg_trades(rows if isinstance(rows, list) else [])
            result["source"] = "Binance USD-M aggTrades"
            _cvd_cache[coin] = (now, result)
            if len(_cvd_cache) > 80:
                for key in sorted(_cvd_cache, key=lambda k: _cvd_cache[k][0])[:20]:
                    _cvd_cache.pop(key, None)
            return result
        except Exception as e:
            result = {"status": "unavailable", "source": "Binance USD-M aggTrades", "error": str(e)[:140]}
            _cvd_cache[coin] = (now, result)
            return result


async def order_book_quality_snapshot(coin, price):
    """Best bid/ask, spread and visible near-touch depth from the cached Binance book."""
    if not price or price <= 0:
        return {"status":"insufficient_data"}
    try:
        ent = _ob_cache.get(coin)
        if ent and time.time() - ent[0] < 30:
            book = ent[1]
        else:
            book = await http_json("https://fapi.binance.com/fapi/v1/depth", params={"symbol":f"{coin}USDT","limit":100}, timeout=5)
            _ob_cache[coin] = (time.time(), book)
            _trim_timed_cache(_ob_cache, 80)
        bids = [(float(p),float(q)) for p,q in book.get("bids",[]) if float(p)>0 and float(q)>0]
        asks = [(float(p),float(q)) for p,q in book.get("asks",[]) if float(p)>0 and float(q)>0]
        if not bids or not asks: return {"status":"insufficient_data"}
        best_bid=max(p for p,q in bids); best_ask=min(p for p,q in asks)
        mid=(best_bid+best_ask)/2
        depth=sum(p*q for p,q in bids if p>=price*.995)+sum(p*q for p,q in asks if p<=price*1.005)
        _sample={"ts":time.time(),"depth_notional":float(depth)}
        _hist=_ob_snapshot_history.setdefault(coin,[]); _hist.append(_sample)
        if len(_hist)>20: del _hist[:-20]
        if len(_ob_snapshot_history)>100:
            for _k in list(_ob_snapshot_history)[:20]: _ob_snapshot_history.pop(_k,None)
        return {"status":"ok","best_bid":best_bid,"best_ask":best_ask,"spread_pct":(best_ask-best_bid)/mid*100,
                "depth_notional":depth,"source":"Binance futures depth","ts":time.time()}
    except Exception as exc:
        log.debug("Order book quality %s: %s", coin, exc)
        return {"status":"unavailable","reason":str(exc)[:120]}


async def order_book_imbalance(coin, price):
    """Перевес сил в стакане: доля бидов в ±2% от цены, ближние уровни весят больше (меньше спуфинга)."""
    try:
        ent = _ob_cache.get(coin)
        if ent and time.time() - ent[0] < 30:
            j = ent[1]
        else:
            j = await http_json("https://fapi.binance.com/fapi/v1/depth", params={"symbol": f"{coin}USDT", "limit": 100}, timeout=5)
            _ob_cache[coin] = (time.time(), j)
            _trim_timed_cache(_ob_cache, 80)
    except Exception as e:
        log.info(f"Стакан {coin}: {e}")
        return None
    def w(p):
        return max(0.2, 1 - abs(p - price) / price / 0.02)
    bids = sum(float(p) * float(q) * w(float(p)) for p, q in j.get("bids", []) if float(p) >= price * 0.98)
    asks = sum(float(p) * float(q) * w(float(p)) for p, q in j.get("asks", []) if float(p) <= price * 1.02)
    tot = bids + asks
    return round(bids / tot, 3) if tot else None


async def funding_trend(coin):
    """История funding (последние 4 периода, от старых к новым)."""
    for cand in SYMBOL_ALIASES.get(coin, [coin]):
        try:
            j = await http_json("https://api.bybit.com/v5/market/funding/history",
                                params={"category": "linear", "symbol": f"{cand}USDT", "limit": 4}, timeout=5)
            rates = [float(x["fundingRate"]) for x in j["result"]["list"]][::-1]
            if len(rates) >= 3:
                return rates
        except Exception:
            continue
    return []


_btc_move = {"ts": 0.0, "pct": 0.0}


async def btc_move_15m():
    """Изменение BTC за последние 15 минут (5-минутные свечи), кэш 60 секунд."""
    if time.time() - _btc_move["ts"] < 60:
        return _btc_move["pct"]
    pct = 0.0
    try:
        kl = await http_json("https://data-api.binance.vision/api/v3/klines",
                             params={"symbol": "BTCUSDT", "interval": "5m", "limit": 4}, timeout=5)
        pct = (float(kl[-1][4]) - float(kl[-3][1])) / float(kl[-3][1]) * 100
    except Exception as e:
        log.info(f"BTC 15m: {e}")
    _btc_move.update(ts=time.time(), pct=round(pct, 2))
    return _btc_move["pct"]


def detect_traps(df, lookback=20):
    """Ложные пробои и сбор ликвидности по последней ЗАКРЫТОЙ свече относительно диапазона lookback."""
    if df is None or len(df) < lookback + 2:
        return {}
    last = df.iloc[-1]
    prev = df.iloc[-lookback - 1:-1]
    hi_lvl, lo_lvl = float(prev["high"].max()), float(prev["low"].min())
    rng = float(last["high"] - last["low"])
    if rng <= 0:
        return {}
    body = abs(float(last["close"] - last["open"]))
    upper = float(last["high"] - max(last["open"], last["close"]))
    lower = float(min(last["open"], last["close"]) - last["low"])
    rv = float(last.get("rel_vol", 1.0))
    out = {"levels": (round(hi_lvl, 6), round(lo_lvl, 6)), "body_ratio": round(body / rng, 2), "rel_vol": round(rv, 2)}
    eps = max(body, rng * 0.05)
    # ложный пробой: фитиль за уровнем, тело вернулось внутрь диапазона на большом объёме
    if last["high"] > hi_lvl and last["close"] < hi_lvl and rv >= 1.3:
        out["fake_up"] = True
    if last["low"] < lo_lvl and last["close"] > lo_lvl and rv >= 1.3:
        out["fake_dn"] = True
    # сбор ликвидности: длинный фитиль за уровнем, закрытие обратно (без требования объёма)
    if last["low"] < lo_lvl and last["close"] > lo_lvl and lower >= 2 * eps and upper <= body + eps:
        out["sweep_dn"] = True
    if last["high"] > hi_lvl and last["close"] < hi_lvl and upper >= 2 * eps and lower <= body + eps:
        out["sweep_up"] = True
    return out


def session_info(now_msk):
    """Сессии по МСК: (название, поправка к уверенности)."""
    h, wd = now_msk.hour, now_msk.weekday()
    if wd >= 5:
        return "выходные", -4
    if h < 3:
        return "ночь (тонкий стакан)", -6
    if h < 10:
        return "Азия", -2
    if h < 16:
        return "Европа", 1
    return "Европа + США", 2


def _chandelier_from_rows(rows, is_long):
    """Синхронная pandas-математика Chandelier Exit (вызывается через asyncio.to_thread,
    чтобы не блокировать event loop на каждой открытой позиции при каждом цикле мониторинга)."""
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    prev = df["close"].shift()
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev).abs(), (df["low"] - prev).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(22).mean().iloc[-1])
    if not atr or atr != atr:
        return None
    if is_long:
        return float(df["high"].tail(22).max() - 3 * atr)
    return float(df["low"].tail(22).min() + 3 * atr)


async def chandelier_stop(coin, is_long):
    """Chandelier Exit: максимум (минимум) 22 часовых свечей минус (плюс) 3 ATR."""
    rows = await hourly_rows(coin)
    if not rows:
        return None
    return await asyncio.to_thread(_chandelier_from_rows, rows, is_long)


def weekly_trend(tf_data):
    """Недельный тренд из дневных свечей (без новых запросов): 1 бычий, −1 медвежий, 0 нет."""
    df = tf_data.get("1d", {}).get("df")
    if df is None or len(df) < 12 * 7:
        return 0
    closes = df["close"].values[::-1][::7][::-1]
    if len(closes) < 12:
        return 0
    ws = pd.Series(closes)
    e10 = ws.ewm(span=10, adjust=False).mean().iloc[-1]
    e20 = ws.ewm(span=20, adjust=False).mean().iloc[-1]
    last = closes[-1]
    if last > e10 > e20:
        return 1
    if last < e10 < e20:
        return -1
    return 0


def smc_context(tf_data, is_long):
    """FVG (имбаланс) и ордерблок на 1h: цена вернулась в зону — возможная точка входа по направлению."""
    h1 = tf_data.get("1h", {}).get("df")
    if h1 is None or len(h1) < 40:
        return 0, None
    d = h1.tail(60).reset_index(drop=True)
    hi, lo, op, cl = d["high"].values, d["low"].values, d["open"].values, d["close"].values
    px = cl[-1]
    n = len(d)
    for i in range(n - 1, max(1, n - 40), -1):
        if is_long and lo[i] > hi[i - 2] and hi[i - 2] <= px <= lo[i]:
            return 6, f"FVG 1h: цена вернулась в имбаланс {fmt(hi[i - 2])}–{fmt(lo[i])}"
        if (not is_long) and hi[i] < lo[i - 2] and lo[i - 2] >= px >= hi[i]:
            return 6, f"FVG 1h: цена вернулась в имбаланс {fmt(hi[i])}–{fmt(lo[i - 2])}"
    for j in range(n - 6, 20, -1):
        if is_long and cl[j] < op[j]:
            if cl[j + 1:j + 4].max() > hi[j - 20:j].max() and lo[j] <= px <= hi[j]:
                return 6, f"Ордерблок 1h {fmt(lo[j])}–{fmt(hi[j])}: откат к зоне импульса"
        if (not is_long) and cl[j] > op[j]:
            if cl[j + 1:j + 4].min() < lo[j - 20:j].min() and lo[j] <= px <= hi[j]:
                return 6, f"Ордерблок 1h {fmt(lo[j])}–{fmt(hi[j])}: откат к зоне импульса"
    return 0, None


async def evaluate_setup(coin, tf_data, ctx, funding=None, scan=False, onchain_change=0.0, conf_threshold=CONF_FINAL):
    """Мульти-ТФ оценка и фильтры v2. valid=True только если прошли все гейты."""
    out = {"valid": False, "reject": "", "notes": [], "is_long": True, "strength": 50.0, "type": "Среднесрочный",
           "news_status": "Нейтрально", "wp": 50.0, "tf_probs": {}, "reasons": [], "patterns": [],
           "fake_breakout": False, "extras": {}}
    tfs = [t for t in TF_WEIGHTS if t in tf_data]
    if "4h" not in tf_data or "1d" not in tf_data:
        out["reject"] = "Недостаточно данных по таймфреймам"
        return out
    wp = sum(TF_WEIGHTS[t] * tf_data[t]["long_prob"] for t in tfs) / sum(TF_WEIGHTS[t] for t in tfs)
    is_long = wp >= 50
    side = 1 if is_long else -1
    strength = max(wp, 100 - wp)
    notes, pos_reasons = [], []

    def cs(p):
        return p if is_long else 100 - p

    p4, p1d = tf_data["4h"]["long_prob"], tf_data["1d"]["long_prob"]
    info = tf_data["4h"]
    h1 = tf_data.get("1h")
    reject = ""
    # Быстрый разворотный SHORT: не требуем, чтобы дневка уже полностью
    # развернулась, если 1h и 15m подтверждают сильный импульс продавцов.
    # Это ранний режим, а не обход проверок качества/риска.
    m15 = tf_data.get("15m")
    early_short = bool(
        not is_long
        and h1 and m15
        and float(h1.get("long_prob", 50)) <= 35
        and float(m15.get("long_prob", 50)) <= 32
        and float(h1.get("adx", 0) or 0) >= 16
        and float(info.get("adx", 0) or 0) >= 12
        and float(p4) <= 55
        and float(p1d) <= 68
    )
    if early_short:
        notes.append("EARLY SHORT: сильный медвежий импульс 1h+15m; дневной тренд ещё может запаздывать")
    if (p4 >= 50) != is_long or (p1d >= 50) != is_long:
        if early_short:
            # Старший ТФ остаётся контекстом риска, но не блокирует резкий
            # подтверждённый младшими ТФ импульс автоматически.
            notes.append("Старшие ТФ расходятся: ранний SHORT требует уменьшенного риска и ретеста")
        else:
            reject = "4h и 1d смотрят в разные стороны"
    agree = sum(1 for t in tfs if cs(tf_data[t]["long_prob"]) >= 55)
    squeeze4 = bool(info.get("squeeze"))
    atr_cap = 0.09 if squeeze4 else 0.06  # высокий ATR после сжатия — ещё не перегрев
    ichi_now = int(info["df"].iloc[-1]["ichi_pos"]) if info.get("df") is not None else 0
    if not reject and info["adx"] < 18 and ichi_now == 0:
        reject = f"Пила: слабый тренд (ADX {info['adx']:.0f}) внутри облака Ишимоку"
    if not reject and info["adx"] < 15:
        reject = f"Вялый рынок (ADX {info['adx']:.0f})"
    if not reject and agree < min(3, len(tfs)):
        reject = f"Таймфреймы не согласны ({agree}/{len(tfs)})"
    if not reject and info["atr"] / info["price"] > atr_cap:
        reject = f"Слишком высокая волатильность (ATR > {atr_cap * 100:.0f}%)"

    if h1 and (h1["long_prob"] >= 50) != is_long and h1["confidence"] >= 65:
        strength -= 8
        notes.append("1h против направления (−8)")
    if is_long and (info["rsi"] > 75 or info["price"] - info["ema20"] > 2.5 * info["atr"]):
        strength -= 8
        notes.append("цена перегрета, риск погони (−8)")
    if (not is_long) and (info["rsi"] < 25 or info["ema20"] - info["price"] > 2.5 * info["atr"]):
        strength -= 8
        notes.append("цена перепродана, риск погони (−8)")

    if coin != "BTC" and ctx.get("btc_dir", 0) != 0:
        if ctx["btc_dir"] == side:
            strength += 3
            notes.append("BTC в ту же сторону (+3)")
        else:
            pen = 10 if ctx.get("btc_conf", 50) >= 65 else 5
            strength -= pen
            notes.append(f"BTC против направления (−{pen})")

    # Обвал / памп BTC прямо сейчас (flash crash): альты летят вместе с ним
    btc15 = 0.0
    if coin != "BTC":
        btc15 = await btc_move_15m()
        if is_long and btc15 <= -2.5:
            reject = reject or f"BTC обвал {btc15:.1f}% за 15 минут: лонги запрещены"
        elif (not is_long) and btc15 >= 2.5:
            reject = reject or f"BTC памп +{btc15:.1f}% за 15 минут: шорты запрещены"
        elif (is_long and btc15 <= -1.5) or ((not is_long) and btc15 >= 1.5):
            strength -= 5
            notes.append(f"BTC резко против за 15 мин ({btc15:+.1f}%) (−5)")

    fng = ctx.get("fng")
    if fng is not None and ((fng >= 80 and is_long) or (fng <= 20 and not is_long)):
        strength -= 4
        notes.append(f"экстремальный {'страх' if fng <= 20 else 'жадность'} {fng} (−4)")

    # История funding: тренд за 3 периода важнее одного значения
    fr = await funding_trend(coin)
    if fr:
        rising = fr[-1] > fr[-2] > fr[-3]
        falling = fr[-1] < fr[-2] < fr[-3]
        if is_long:
            if rising and fr[-1] > 0.0003:
                strength -= 6
                notes.append(f"funding растёт 3 периода подряд ({fr[-1] * 100:.3f}%): пузырь лонгов (−6)")
            elif falling and fr[-1] < -0.0002:
                strength += 4
                pos_reasons.append("Funding уходит в минус: паника, шорты переплачивают")
        else:
            if rising and fr[-1] > 0.0003:
                strength += 4
                pos_reasons.append("Funding растёт: лонги переплачивают, перегрев лонгов")
            elif falling and fr[-1] < -0.0002:
                strength -= 6
                notes.append("funding 3 периода в минус: шорты переполнены, риск выкупа (−6)")

    # Матрица OI + цена
    oi_state, oi_pct, p_pct = await oi_price_state(coin)
    if oi_state:
        d = OI_DELTA[oi_state][0 if is_long else 1]
        strength += d
        notes.append(f"OI+цена: {OI_TXT[oi_state]} ({d:+d})")
        if d > 0:
            pos_reasons.append(OI_TXT[oi_state])

    # Стакан: перевес сил (вместо одиночной «стены»)
    imb = None
    if not reject and strength >= conf_threshold - 8:
        imb = await order_book_imbalance(coin, info["price"])
    if imb is not None:
        if (is_long and imb < 0.40) or ((not is_long) and imb > 0.60):
            strength -= 5
            notes.append(f"стакан: перевес {'продавцов' if is_long else 'покупателей'} ({imb:.0%}) (−5)")
        elif (is_long and imb > 0.60) or ((not is_long) and imb < 0.40):
            strength += 4
            pos_reasons.append(f"Стакан: перевес {'покупателей' if is_long else 'продавцов'} {max(imb, 1 - imb):.0%}")

    if onchain_change < -2.0 and is_long:
        strength -= 10
        notes.append(f"отток TVL {onchain_change}% против лонга (−10)")
    elif onchain_change > 2.0 and not is_long:
        strength -= 10
        notes.append(f"приток TVL {onchain_change}% против шорта (−10)")

    # Сессии по МСК вместо одного штрафа за ночь
    sess_name, sess_adj = session_info(datetime.now(timezone(timedelta(hours=3))))
    if sess_adj:
        strength += sess_adj
        notes.append(f"сессия: {sess_name} ({sess_adj:+d})")
    h1df = h1.get("df") if h1 else None
    if sess_name == "Азия" and h1df is not None and (h1df.iloc[-1]["brk_up"] or h1df.iloc[-1]["brk_dn"]):
        strength -= 3
        notes.append("Азия + пробой: такие пробои часто ложные (−3)")

    # Ложные пробои и сбор ликвидности (1h, а если нет — 4h).
    # Дополнительная оценка использует только последнюю закрытую свечу.
    tdf = h1df if h1df is not None else info.get("df")
    traps = detect_traps(tdf) if tdf is not None else {}
    breakout = breakout_assessment(tdf)
    if breakout.get("state", "").startswith("fake_"):
        strength -= 8
        notes.append("подтверждён риск ложного пробоя (−8)")
    elif breakout.get("state") in ("breakout_up", "breakout_down"):
        if (breakout.get("direction") == "LONG") == is_long:
            strength += 3
            pos_reasons.append("Пробой подтверждён закрытием свечи и объёмом")
        else:
            strength -= 4
            notes.append("пробой направлен против сетапа (−4)")

    # Anti-FOMO: мягкий штраф за растянутую цену, блок только в экстремуме.
    anti_fomo = anti_fomo_assessment(info.get("df"), "LONG" if is_long else "SHORT")
    if anti_fomo.get("penalty"):
        strength -= anti_fomo["penalty"]
        notes.append(f"Anti-FOMO: {anti_fomo['reason']} (−{anti_fomo['penalty']})")
    if anti_fomo.get("blocked"):
        reject = reject or "Anti-FOMO: вход чрезмерно растянут, ожидаем откат"

    # Компактный снимок индикаторов для карточки сигнала и Signal Memory.
    indicators = indicator_snapshot(info.get("df"))
    lv_hi, lv_lo = traps.get("levels", (0, 0))
    if traps.get("fake_up"):
        if is_long:
            strength -= 15
            notes.append(f"ложный пробой вверх у {lv_hi}: объём и тело не удержали уровень (−15)")
        else:
            strength += 5
            pos_reasons.append(f"Ложный пробой вверх у {lv_hi}: покупатели не удержали уровень")
    if traps.get("fake_dn"):
        if not is_long:
            strength -= 15
            notes.append(f"ложный пробой вниз у {lv_lo}: объём и тело не удержали уровень (−15)")
        else:
            strength += 5
            pos_reasons.append(f"Ложный пробой вниз у {lv_lo}: продавцы не удержали уровень")
    if traps.get("sweep_dn"):
        if is_long:
            strength += 8
            pos_reasons.append(f"Сбор ликвидности ниже {lv_lo} и выкуп: стопы розницы собраны")
        else:
            strength -= 4
            notes.append("сбор ликвидности вниз: против шорта (−4)")
    if traps.get("sweep_up"):
        if not is_long:
            strength += 8
            pos_reasons.append(f"Сбор ликвидности выше {lv_hi} и продажа: стопы лонгов собраны")
        else:
            strength -= 4
            notes.append("сбор ликвидности вверх: против лонга (−4)")
    out["fake_breakout"] = bool(traps.get("fake_up") or traps.get("fake_dn"))

    # Сжатие Боллинджера: импульс близко, направление по EMA 4h
    if squeeze4 and (info["ema20"] > info["ema50"]) == is_long:
        strength += 4
        pos_reasons.append("Сжатие Боллинджера на 4h: импульс близко, направление совпадает с EMA")

    smc_delta, smc_reason = smc_context(tf_data, is_long)
    if smc_delta:
        strength += smc_delta
        if smc_reason:
            pos_reasons.append(smc_reason)
    wk = weekly_trend(tf_data)
    if wk:
        if (wk == 1) == is_long:
            strength += 3
            pos_reasons.append("Недельный тренд совпадает с направлением")
        else:
            strength -= 6
            notes.append("недельный тренд против направления (−6)")

    nb = market_bias.get("bias", 0)
    news_status = "Нейтрально"
    news_risk = news_risk_assessment(market_bias, coin)
    if time.time() - market_bias.get("ts", 0) < 3 * 3600 and news_risk.get("relevant"):
        if nb != 0:
            word = "бычий" if nb == 1 else "медвежий"
            if nb == side:
                strength += 4
                news_status = f"✅ Подтверждают ({word} фон)"
            else:
                penalty = min(12, max(3, int(news_risk.get("penalty", 5))))
                strength -= penalty
                news_status = f"⚠️ Противоречат ({word} фон, риск {news_risk.get('severity', 'medium')})"
                notes.append(f"Новостной риск для {coin}: −{penalty}")
        elif news_risk.get("severity") in ("high", "critical"):
            penalty = min(6, max(2, int(news_risk.get("penalty", 4)) // 2))
            strength -= penalty
            news_status = f"⚠️ Негативное событие ({news_risk.get('severity')})"
            notes.append(f"Новостной риск для {coin}: −{penalty}")

    # Market Panic Detector: повышает порог осторожности, но не выключает сканер
    # из-за одного заголовка или обычной коррекции.
    btc_atr_pct = 0.0
    try:
        btc_info = ctx.get("btc_atr_pct")
        btc_atr_pct = float(btc_info or 0.0)
    except (TypeError, ValueError):
        pass
    panic = market_panic_assessment(btc15, btc_atr_pct)
    if panic["level"] in ("elevated", "high"):
        penalty = 3 if panic["level"] == "elevated" else 6
        strength -= penalty
        notes.append(f"Market Panic {panic['level']}: BTC {btc15:+.2f}% за 15м (−{penalty})")

    strength = max(0.0, min(100.0, strength))
    if not reject and strength < conf_threshold:
        reject = f"Уверенность {strength:.0f}% ниже порога {conf_threshold:.0f}%"

    p15 = tf_data.get("15m", {}).get("long_prob", 50)
    p1h = h1["long_prob"] if h1 else 50
    fast = "15m" in tf_data and h1 is not None and cs(p15) >= 58 and cs(p1h) >= 56
    long_term = cs(p1d) >= 64 and cs(p4) >= 60
    reasons = (pos_reasons + collect_reasons(tf_data, is_long))[:6]
    out.update(
        valid=not reject, reject=reject, notes=notes, is_long=is_long, strength=round(strength, 1),
        type="Краткосрочный" if fast else "Долгосрочный" if long_term else "Среднесрочный",
        news_status=news_status, wp=wp, tf_probs={t: tf_data[t]["long_prob"] for t in tfs},
        reasons=reasons, patterns=pattern_lines(tf_data),
        extras={"oi": oi_state, "oi_pct": oi_pct, "price_pct": p_pct, "imbalance": imb,
                "funding": fr[-1] if fr else funding, "funding_history": fr[-4:],
                "oi_change_pct": oi_pct, "btc15": btc15, "market_panic": panic,
                "news_risk": news_risk, "anti_fomo": anti_fomo, "breakout": breakout,
                "indicators": indicators, "tf_consensus": f"{agree}/{len(tfs)}",
                "session": sess_name, "traps": {k: v for k, v in traps.items() if k != "levels"},
                "signal_phase": "EARLY SHORT" if early_short else ("CONFIRMED" if not reject else "WAIT"),
                "early_short": early_short})
    return out


def _parse_analyst(text):
    try:
        d = json.loads(re.search(r"\{.*\}", text or "", re.S).group(0))
        verdict, comment = str(d.get("verdict", "")).upper().strip(), str(d.get("comment", "")).strip()
        div = str(d.get("divergence", "none")).lower().strip()
        if verdict in ("ENTER", "WAIT") and has_cyr(comment, 15):
            return {"verdict": verdict, "risk": str(d.get("risk", "средний")).strip()[:20],
                    "divergence": div if div in ("bull", "bear") else "none", "comment": shorten(comment, 320)}
    except Exception:
        pass
    return None


async def ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data):
    """ИИ видит сырые OHLCV и RSI последних 30 свечей 4h: сам ищет дивергенции, вердикт ENTER/WAIT."""
    key = (coin, setup["is_long"])
    c = _ai_cache.get(key)
    if c and time.time() - c[0] < 1200:
        return c[1]
    df = tf_data["4h"]["df"].tail(30)
    block = "\n".join(f"{r.open:.6g} {r.high:.6g} {r.low:.6g} {r.close:.6g} v{r.volume:.0f} rsi{r.rsi:.0f}" for r in df.itertuples())
    bt_txt = f"{bt['trades']} сделок, TP1 {bt['winrate']}%, ср. R {bt['avg_r']:+.2f}, PF {bt['pf']}" if bt else "нет данных"
    prompt = f"""Ты крипто-трейдер. Проверь идею по СЫРЫМ данным 4h (последние 30 свечей, колонки: open high low close volume RSI).
{block}

Монета {coin}, направление {'LONG' if setup['is_long'] else 'SHORT'}, Conf {setup['strength']}%.
Вход {fmt(entry)}, TP1 {fmt(tps[0])}, стоп {fmt(sls[0])}. Бэктест 4h: {bt_txt}.
Выводы модели: {'; '.join(setup['reasons']) or 'нет'}. Фильтры: {'; '.join(setup['notes']) or 'нет'}.
Задачи: 1) есть ли дивергенция цены и RSI (новый максимум/минимум цены без подтверждения RSI)? 2) не прошёл ли уже большую часть движения? 3) решение.
Используй ТОЛЬКО цены из данных, не придумывай уровни.
Верни СТРОГО JSON без markdown: {{"verdict":"ENTER или WAIT","risk":"низкий или средний или высокий","divergence":"bull или bear или none","comment":"1-2 предложения по-русски"}}"""
    text, engine = await ask_ai_named(prompt, validate=lambda t: _parse_analyst(t) is not None, timeout=22, free_timeout=25)
    res = _parse_analyst(text) if text else None
    if res:
        res["engine"] = engine
        if res["divergence"] != "none":
            res["comment"] = shorten(f"Дивергенция {'бычья' if res['divergence'] == 'bull' else 'медвежья'} по RSI. {res['comment']}", 320)
    _ai_cache[key] = (time.time(), res)
    _trim_timed_cache(_ai_cache, 128)
    return res



def record_quant_missed_candidate(res):
    """Store only ENTER candidates vetoed by quant gates for delayed, non-leaky counterfactual evaluation."""
    try:
        now = time.time(); coin = str(res.get("coin", "")).upper(); side = str(res.get("side", "")).upper()
        entry = float(res.get("entry", 0) or 0); stop = float((res.get("sls") or [0])[0] or 0); target = float((res.get("tps") or [0])[0] or 0)
        if not coin or side not in ("LONG", "SHORT") or min(entry, stop, target) <= 0:
            return False
        rows = STATE.setdefault("quant_observations", [])
        bucket = int(now // 900)
        if any(x.get("coin") == coin and x.get("side") == side and int(float(x.get("ts", 0) or 0) // 900) == bucket for x in rows[-100:]):
            return False
        rows.append({"id": f"{coin}:{side}:{int(now)}", "coin": coin, "side": side, "ts": now,
            "entry": entry, "stop": stop, "target": target, "status": "pending", "taken": False,
            "was_valid": False, "gate": (res.get("quant") or {}).get("risk_state"),
            "risk_flags": list((res.get("quant") or {}).get("risk_flags") or [])[:8],
            "strategy_score": float((res.get("quant") or {}).get("risk_score", res.get("strength", 0)) or 0)})
        del rows[:-300]
        state_touch(True)
        return True
    except Exception as e:
        log.debug("quant missed candidate store: %s", e)
        return False


async def evaluate_quant_missed_observations(limit=4):
    """Evaluate matured rejected candidates using later closed 15m candles; stop wins same-candle ties."""
    now = time.time(); rows = STATE.setdefault("quant_observations", [])
    pending = [x for x in rows if x.get("status") == "pending" and now - float(x.get("ts", 0) or 0) >= 4 * 3600]
    changed = False
    for item in pending[:max(1, min(8, int(limit)))]:
        try:
            frames, _source = await get_market_data_light(item["coin"], ("15m",))
            frame = (frames.get("15m") or {}).get("df")
            if frame is None or frame.empty:
                continue
            ts_key = "ts" if "ts" in frame.columns else "timestamp" if "timestamp" in frame.columns else None
            if not ts_key:
                continue
            times = pd.to_numeric(frame[ts_key], errors="coerce")
            start_ms = float(item["ts"]) * 1000
            sample = frame.loc[times >= start_ms]
            if sample.empty:
                continue
            side = item["side"]; stop = float(item["stop"]); target = float(item["target"])
            outcome = "no_touch"; outcome_ts = None
            for _, bar in sample.iterrows():
                high = float(bar["high"]); low = float(bar["low"])
                stop_hit = low <= stop if side == "LONG" else high >= stop
                target_hit = high >= target if side == "LONG" else low <= target
                if stop_hit and target_hit:
                    outcome = "stop_first_same_candle"; outcome_ts = float(bar[ts_key]) / 1000; break
                if stop_hit:
                    outcome = "stop_first"; outcome_ts = float(bar[ts_key]) / 1000; break
                if target_hit:
                    outcome = "target_first"; outcome_ts = float(bar[ts_key]) / 1000; break
            item.update(status="evaluated", outcome=outcome, outcome_ts=outcome_ts,
                was_valid=(outcome == "target_first"), evaluated_at=now,
                evaluation_note="Counterfactual only; no actual order or fill is implied.")
            changed = True
            await asyncio.sleep(0.15)
        except Exception as e:
            log.debug("missed opportunity eval %s: %s", item.get("coin"), e)
    if changed:
        state_touch(True)
    return sum(x.get("status") == "pending" for x in rows)


async def quant_observation_loop():
    await asyncio.sleep(90)
    while True:
        try:
            await evaluate_quant_missed_observations(limit=4)
        except Exception as e:
            log.warning("Quant observation loop: %s", e)
        await asyncio.sleep(300)


async def build_analysis(coin, tf_data, setup, bt, ctx, source):
    info = tf_data["4h"]
    is_long = setup["is_long"]
    entry, tps, sls, risk_dist, leverage, near, liq_x20 = calc_levels_and_risk(info, is_long)
    verdict, comment, risk_lvl, engine = "WAIT", "", "средний", ""
    if not setup["valid"]:
        comment = setup["reject"]
    else:
        ai = await ai_analyst(coin, setup, bt, ctx, entry, tps, sls, tf_data)
        if ai:
            verdict, comment, risk_lvl, engine = ai["verdict"], ai["comment"], ai["risk"], ai.get("engine", "")
        else:
            verdict, comment = local_verdict(setup, bt), "ИИ недоступен: решение по правилам модели и истории."
        if bt and bt["trades"] >= BT_MIN_TRADES and not bt_ok(bt):
            verdict, comment = "WAIT", "История этой монеты по такой стратегии слабая. " + comment
        if verdict == "ENTER" and near:
            comment = f"{comment} ⚠ {near}".strip()
    try:
        img = await asyncio.to_thread(draw_chart, coin, tf_data, "4h", is_long, entry, tps, sls)
    except Exception as e:
        log.info(f"График {coin}: {e}")
        img = None
    res = {
        "coin": coin, "ts": time.time(), "source": source, "img": img,
        "is_long": is_long, "side": "LONG" if is_long else "SHORT", "verdict": verdict, "type": setup["type"],
        "strength": setup["strength"], "regime": info["regime"], "news_status": setup["news_status"],
        "btc_txt": ctx.get("btc_txt", "нет данных"), "fng": ctx.get("fng"), "price": info["price"],
        "entry": entry, "tps": tps, "sls": sls, "risk_dist": risk_dist, "atr": info.get("atr"), "leverage": leverage,
        "reasons": setup["reasons"], "patterns": setup["patterns"], "notes": setup["notes"],
        "squeeze": info["squeeze"], "bt": bt, "comment": comment, "risk_lvl": risk_lvl, "engine": engine,
        "liq_x20": liq_x20, "fake_breakout": setup.get("fake_breakout", False),
        "long_prob": {t: round(tf_data[t]["long_prob"], 1) for t in tfs_list(tf_data)},
        "adx": round(info["adx"], 1), "rsi": round(info["rsi"], 1),
        "indicators": setup.get("extras", {}).get("indicators", {}),
        "funding_rate": setup.get("extras", {}).get("funding"),
        "oi_change_pct": setup.get("extras", {}).get("oi_change_pct"),
        "tf_consensus": setup.get("extras", {}).get("tf_consensus"),
        "signal_phase": setup.get("extras", {}).get("signal_phase", "CONFIRMED"),
        "early_short": bool(setup.get("extras", {}).get("early_short")),
        "news_risk": setup.get("extras", {}).get("news_risk", {}),
        "market_panic": setup.get("extras", {}).get("market_panic", {}),
        "anti_fomo": setup.get("extras", {}).get("anti_fomo", {}),
        "breakout": setup.get("extras", {}).get("breakout", {}),
        "atr_target_profile": list(atr_target_multipliers(info.get("adx"), (info.get("atr", 0) / info.get("price", 1) * 100))),
        "atr_stop_multiplier": atr_stop_multiplier(info.get("adx"), (info.get("atr", 0) / info.get("price", 1) * 100)),
        "sr": info.get("sr", {}),
    }
    # IXCLY Decision Brain: explainable research layer; does not execute orders.
    try:
        _brain_df = info.get("df")
        _brain_news = setup.get("extras", {}).get("news_risk", {})
        _brain_early = early_movement_assessment(
            _brain_df, setup.get("extras", {}).get("oi_pct"),
            setup.get("extras", {}).get("imbalance"),
            setup.get("extras", {}).get("funding"),
            volume_anomaly(_brain_df) if _brain_df is not None else None)
        _brain_crowd = liquidation_risk_assessment(
            setup.get("extras", {}).get("funding"),
            setup.get("extras", {}).get("oi_pct"),
            setup.get("extras", {}).get("price_pct"))
        _brain_ctx = {"news_risk": _brain_news}
        _brain_ctx["orderbook_bid_share"] = setup.get("extras", {}).get("imbalance")
        res["market_fingerprint"] = market_fingerprint(coin, _brain_df, load_signal_history())
        res["signal_dna"] = signal_fingerprint(res, setup, _brain_ctx)
        res["decision_brain"] = decision_brain(res["signal_dna"], _brain_early, _brain_crowd,
                                                 _brain_news, res.get("bt"))
        res["regime_transition"] = regime_transition_assessment(_brain_df) if _brain_df is not None else {"status": "insufficient_data"}
        res["signal_fragility"] = signal_fragility(res["signal_dna"])
        res["evidence_conflict_map"] = evidence_conflict_map(
            res.get("side"), {"orderbook_bid_share": setup.get("extras", {}).get("imbalance"),
                               "funding": setup.get("extras", {}).get("funding"),
                               "news_bias": setup.get("extras", {}).get("news_bias")}, _brain_df)
        # Only severe, low-score contradictions can downgrade an ENTER; legacy filters remain intact.
        if res.get("verdict") == "ENTER" and res["decision_brain"].get("action") == "BLOCK":
            res["verdict"] = "WAIT"
            res["comment"] = "Decision Brain: " + res["decision_brain"].get("reason", "повышенный риск")
    except Exception as _brain_err:
        log.debug("Decision Brain analysis skipped for %s: %s", coin, _brain_err)
    # IXCLY 5.0 Quant Intelligence: run bounded independent models in the actual signal path.
    _verdict_before_quant = res.get("verdict")
    try:
        _qextra = setup.get("extras", {}) if isinstance(setup, dict) else {}
        _book_quality = await order_book_quality_snapshot(coin, float(info.get("price") or entry))
        _liq_now = time.time()
        _liq_recent = [x for x in LIQUIDATIONS if x.get("coin") == coin and _liq_now-float(x.get("ts",0) or 0) <= 3600]
        _liq_baseline = [x for x in LIQUIDATIONS if x.get("coin") == coin and 3600 < _liq_now-float(x.get("ts",0) or 0) <= 25*3600]
        _liq_notional = sum(float(x.get("notional",0) or 0) for x in _liq_recent) if _liq_recent else None
        _liq_average = (sum(float(x.get("notional",0) or 0) for x in _liq_baseline)/24) if _liq_baseline else None
        _cross_exchange = await cross_exchange_price_snapshot(coin) if setup.get("strength", 0) >= 78 else {"status":"deferred_for_resource_budget"}
        _cvd_data = await fetch_futures_cvd(coin) if setup.get("strength", 0) >= 70 else {"status":"deferred_for_resource_budget","source":"Binance USD-M aggTrades"}
        if time.time() - float(_macro_cache.get("ts", 0) or 0) > 900:
            await check_macro_calendar()
        _notional = (DEPOSIT * RISK_PCT / 100 / risk_dist * entry) if risk_dist > 0 else None
        _depth_ratio = (_book_quality.get("depth_notional", 0) / _notional) if _notional and _book_quality.get("status") == "ok" else None
        _qstack = quant_intelligence_stack(
            tf_data, side=res.get("side", "LONG"), entry=entry, stop=(sls[0] if sls else None),
            targets=tps, history=quant_closed_trade_history(coin, 500), funding=_qextra.get("funding"),
            oi_change=_qextra.get("oi_change_pct", _qextra.get("oi_pct")),
            orderbook_bid_share=_qextra.get("imbalance"), news_risk=res.get("news_risk"),
            run_simulation=bool(setup.get("strength", 0) >= 78), leverage=leverage,
            spread_pct=_book_quality.get("spread_pct") if _book_quality.get("status") == "ok" else None,
            depth_ratio=_depth_ratio, depth_notional=_book_quality.get("depth_notional") if _book_quality.get("status") == "ok" else None,
            notional=_notional, cross_exchange_data=_cross_exchange,
            funding_history=_qextra.get("funding_history"), liquidation_notional=_liq_notional,
            average_liquidation_notional=_liq_average, cvd_data=_cvd_data,
            macro_events=list(_macro_cache.get("events") or []), macro_feed_status=_macro_cache.get("status"),
            orderbook_snapshots=list(_ob_snapshot_history.get(coin, [])),
            liquidation_events=[x for x in LIQUIDATIONS if x.get("coin")==coin and time.time()-float(x.get("ts",0) or 0)<86400],
            signal_context={"ts":res.get("ts"),"entry":entry,"structure_valid":bool(setup.get("valid"))},
            oi_price_change=_qextra.get("price_pct"))
        res["quant"] = _qstack
        res["probability"] = _qstack.get("bayesian_probability")
        res["risk_score"] = _qstack.get("risk_score")
        res["risk_state"] = _qstack.get("risk_state")
        res["cross_exchange"] = _qstack.get("cross_exchange")
        # RED always blocks. YELLOW is WAIT until a fresh confirmation; do not manufacture an entry.
        if res.get("verdict") == "ENTER" and _qstack.get("risk_state") in ("RED", "YELLOW"):
            res["verdict"] = "WAIT"
            flags = ", ".join(_qstack.get("risk_flags") or []) or "недостаточно подтверждений"
            res["comment"] = f"Quant Risk Gate {_qstack.get('risk_state')}: {flags}. Требуется повторное подтверждение. " + str(res.get("comment", ""))
        _strategy_type = str(setup.get("type", "unknown"))[:80]
        _strategy_history = [x for x in quant_closed_trade_history(None, 2000) if str(x.get("strategy_type", "unknown")) == _strategy_type]
        _strategy_wf = walk_forward_validation(_strategy_history, min_trades=30)
        _retirement = strategy_retirement({"trades":_strategy_wf.get("test",{}).get("trades",0),
                                           "expectancy_r":_strategy_wf.get("test",{}).get("expectancy_r")}, min_trades=30)
        res["strategy_retirement"] = {**_retirement, "walk_forward":_strategy_wf}
        _retired = STATE.setdefault("quant_retired_strategies", {})
        _retired_until = float((_retired.get(_strategy_type) or {}).get("until", 0) or 0)
        if _retirement.get("status") == "retire":
            _retired[_strategy_type] = {"until":time.time()+7*86400,"expectancy_r":_retirement.get("expectancy_r"),"sample":_retirement.get("sample")}
            state_touch(False)
            _retired_until = _retired[_strategy_type]["until"]
        if _retired_until > time.time() and res.get("verdict") == "ENTER":
            res["verdict"] = "WAIT"
            res["comment"] = "Strategy Retirement: стратегия временно отключена из-за отрицательной OOS-эффективности. " + str(res.get("comment", ""))
    except Exception as _quant_err:
        log.warning("Quant Intelligence skipped for %s: %s", coin, _quant_err)
        res["quant"] = {"status": "error", "reason": str(_quant_err)[:180], "risk_state": "YELLOW"}
        if res.get("verdict") == "ENTER":
            res["verdict"] = "WAIT"
            res["comment"] = "Quant-проверка не завершилась: вход временно не подтверждён. " + str(res.get("comment", ""))
    if _verdict_before_quant == "ENTER" and res.get("verdict") == "WAIT" and (res.get("quant") or {}).get("risk_state") in ("RED", "YELLOW"):
        record_quant_missed_candidate(res)
    res["text"] = render_caption(res)
    return res


def tfs_list(tf_data):
    return [t for t in TFS if t in tf_data]


def fetch_error_text(coin):
    errs = fetch_errors.get(coin, [])
    tried = ", ".join(e["name"] for e in EXCHANGES)
    base = f"Монета <b>{html.escape(coin)}</b> не найдена ни на одной бирже ({tried})."
    return base + ("\n\n<i>Причины: " + html.escape("; ".join(errs[-4:])) + "</i>" if errs else "")


async def analyze_coin(coin, uid=None, force=False):
    """(res, error). Свежий анализ → архив → только цена → ошибка."""
    cached = analysis_cache.get(coin)
    if cached and not force and time.time() - cached[0] < cached[1]:
        return cached[2], None
    try:
        tf_data, source = await get_market_data(coin)
        ctx = await get_context()
        funding = await get_funding(coin)
        user_conf = CONF_FINAL
        if uid:
            user_conf = float(_users().get(str(uid), {}).get("conf_final", CONF_FINAL))
        setup = await evaluate_setup(coin, tf_data, ctx, funding, onchain_change=await get_onchain_context(), conf_threshold=user_conf)
        bt = await get_backtest(coin)
        res = await asyncio.wait_for(build_analysis(coin, tf_data, setup, bt, ctx, source), timeout=80)
        res["conf_used"] = user_conf
        analysis_cache[coin] = (time.time(), 60, res)
        save_analysis_db(coin, res)
        _trim_timed_cache(analysis_cache, 50)
        return res, None
    except LookupError:
        pass
    except Exception as e:
        log.error(f"Ошибка анализа {coin}: {e}", exc_info=True)
    old = load_analysis_db(coin)
    if old and time.time() - old["ts"] < 12 * 3600:
        res = dict(old["data"], stale=True, img=None)
        res["text"] = render_caption(res)
        return res, None
    price = await get_simple_price(coin)
    if price:
        return {"coin": coin, "price": price, "price_only": True, "ts": time.time(), "img": None, "source": "API цен"}, None
    return None, "🚫 " + fetch_error_text(coin)


def remember_res(uid, coin, res):
    d = user_res.setdefault(uid, {})
    d[coin] = (time.time(), res)
    for k in sorted(d, key=lambda c: d[c][0])[:-8]:
        d.pop(k, None)


def get_user_res(uid, coin):
    v = user_res.get(uid, {}).get(coin)
    return v[1] if v and time.time() - v[0] < 1800 else None


# =====================================================================
# СИГНАЛЫ: выборка, сканер, симуляция, живой мониторинг
# =====================================================================
SIGNAL_TTL = 12 * 3600          # сигнал без входа живёт 12 часов
INVALID_ATR_MULT = 2.5          # цена ушла от входа дальше этого (в ATR) — вход потерян
signal_notify_log = {}          # coin:side -> ts отправки (анти-дубль уведомлений)


def get_active_signals():
    """Актуальные сигналы: один на монету+сторону, без протухших и аннулированных."""
    now = time.time()
    active, seen = [], set()
    for x in sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True):
        key = f"{x['coin']}_{x['side']}"
        # "in_trade" is an already-open / tracked position, not a fresh signal.
        # Keep the Signals feed strictly for actionable, unfilled active signals.
        if x.get("status") != "active" or key in seen:
            continue
        if x.get("status") == "active" and now - x.get("ts", 0) > SIGNAL_TTL:
            continue
        seen.add(key)
        active.append(x)
    return active


def signals_for(uid, category="Все"):
    items = get_active_signals()
    if category == "Все":
        return items
    if category.startswith("mode:"):
        return [x for x in items if x.get("type") in MODE_TYPES.get(category[5:], ())]
    return [x for x in items if x.get("type") == category]


async def get_scan_universe():
    try:
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
        qv = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    except Exception as e:
        log.info(f"Universe: {e}")
        return POPULAR_COINS[:SCAN_MAX_COINS]
    uni = []
    for c in POPULAR_COINS:
        for cand in SYMBOL_ALIASES.get(c, [c]):
            v = qv.get(f"{cand}USDT", 0)
            if v >= MIN_QV24:
                uni.append((c, v))
                break
    uni.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in uni[:SCAN_MAX_COINS]] or POPULAR_COINS[:SCAN_MAX_COINS]


def recent_results(n=20):
    hist = sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True)
    return [x for x in hist if x.get("status") in ("win", "loss")][:n]


def adaptive_conf_bonus(regime=None):
    # Обучение только на закрытых сигналах; при малой выборке параметры не меняются.
    profile = adaptive_scanner_profile(load_signal_history(), CONF_FINAL, regime=regime)
    return int(profile.get("adjustment", 0))


def streak_pause():
    r = recent_results(3)
    return len(r) == 3 and all(x["status"] == "loss" for x in r) and time.time() - r[0]["ts"] < 6 * 3600


def coin_recently_lost(coin, hours=24):
    now = time.time()
    return any(x["coin"] == coin and x.get("status") == "loss" and now - x.get("ts", 0) < hours * 3600
               for x in load_signal_history())


def set_blackout(secs, kind="misc"):
    global news_blackout_until, news_blackout_kind, news_blackout_since
    news_blackout_until = max(news_blackout_until, time.time() + secs)
    news_blackout_kind, news_blackout_since = kind, time.time()


def alert_once(kind, ttl):
    now = time.time()
    if now - _alert_sent.get(kind, 0) < ttl:
        return False
    _alert_sent[kind] = now
    return True


async def maybe_lift_blackout():
    """Снимает только новостную паузу, если через 15 минут BTC не отреагировал."""
    global news_blackout_until
    if news_blackout_kind != "news" or time.time() < news_blackout_since + 900:
        return False
    if abs(await btc_move_15m()) < 0.6:
        news_blackout_until = 0.0
        if alert_once("blackout_lift", 3600):
            await notify_admin("✅ Новостная пауза снята раньше срока: BTC спокоен.")
        return True
    return False


async def btc_shock():
    try:
        tf, _ = await get_market_data_light("BTC", ("1h",))
        last = tf["1h"]["df"].iloc[-1]
        closed_ago = time.time() - (float(last["ts"]) / 1000 + 3600)
        return closed_ago < 1800 and (last["high"] - last["low"]) > 2.5 * last["atr"]
    except Exception:
        return False


async def check_macro_calendar():
    """High-impact calendar: FinanceCalendar first, Finnhub fallback; exact timestamps only."""
    now = time.time()
    if now - float(_macro_cache.get("ts", 0) or 0) <= 900:
        return bool(_macro_cache.get("blocked"))
    _macro_cache.update(ts=now, events=[], blocked=False, error=None)
    today = datetime.now(timezone.utc).date()
    relevant_countries = {"US", "EU", "GB", "UK", "JP", "CN", "CA", "AU", "CH", "DE"}
    raw = []; source = None; stale_rows = 0
    try:
        if FINANCECALENDAR_API_KEY:
            params = {"from": (today - timedelta(days=1)).isoformat(), "to": (today + timedelta(days=2)).isoformat(),
                      "impact": "high", "country": "US,GB,EU,DE,JP,CN,CA,AU,CH", "category": "economic-indicators,central-banks-monetary-policy",
                      "limit": 300, "format": "json"}
            async with http.get("https://www.financecalendar.com/wp-json/fc/v1/calendar", params=params,
                headers={"X-API-Key": FINANCECALENDAR_API_KEY}, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                payload = await resp.json(content_type=None)
                if resp.status != 200:
                    raise RuntimeError(f"FinanceCalendar HTTP {resp.status}")
            raw = payload.get("events", []) if isinstance(payload, dict) else []
            source = "FinanceCalendar"
        elif FINNHUB_API_KEY:
            payload = await http_json("https://finnhub.io/api/v1/calendar/economic",
                params={"from": (today - timedelta(days=1)).isoformat(), "to": (today + timedelta(days=2)).isoformat(),
                        "token": FINNHUB_API_KEY}, timeout=12)
            raw = payload.get("economicCalendar", []) if isinstance(payload, dict) else []
            source = "Finnhub"
        else:
            _macro_cache.update(status="not_configured", error="Set FINANCECALENDAR_API_KEY or FINNHUB_API_KEY")
            return False

        normalized = normalize_macro_calendar_events(raw if isinstance(raw, list) else [], source=source,
            relevant_countries=relevant_countries)
        events = normalized["events"]
        _macro_cache.update(events=events, status="ok", source=source, error=None,
            stale_rows=normalized["stale_rows"], untimed_rows=normalized["untimed_rows"], calendar_rows=normalized["raw_rows"])
        shield = macro_event_shield(events, now=now, window_minutes=30, feed_status="ok")
        _macro_cache["blocked"] = bool(shield.get("blocked"))
        return _macro_cache["blocked"]
    except Exception as e:
        _macro_cache.update(status="error", error=str(e)[:180], events=[], blocked=False, source=source or "unavailable")
        log.warning("Macro calendar unavailable: %s", e)
        return False


def signal_recipients(coin, side):
    """Кому отправлять уведомления о сигнале: включившим сигналы и имеющим доступ."""
    out = []
    for uid, u in list(_users().items()):
        try:
            iid = int(uid)
            # Новые торговые сигналы — критические события. Не скрываем их из-за
            # пользовательского фильтра уведомлений/порога confidence.
            if isinstance(u, dict) and u.get("active", True) and has_access(iid):
                out.append(iid)
        except (TypeError, ValueError):
            continue
    return out


def format_no_signal_message(reason, details=""):
    """Читабельный Telegram-отчёт без потери диагностических чисел."""
    raw = str(details or "")
    reason_text = html.escape(str(reason or "Сетапы не прошли итоговую проверку."))
    scan_match = re.search(
        r"Скан:\s*(\d+)\s*монет\s*→\s*(\d+)\s*кандидатов\s*→\s*(\d+)\s*сетапов\s*→\s*(\d+)\s*готовых\s*→\s*(\d+)\s*опубликовано",
        raw, re.I)
    stats_match = re.search(
        r"Бэктест отклонил:\s*(\d+);\s*ошибок получения истории:\s*(\d+);\s*ошибок анализа:\s*(\d+);\s*ЖДАТЬ:\s*(\d+);\s*качество ниже порога:\s*(\d+);\s*без истории:\s*(\d+)",
        raw, re.I)
    reject_match = re.search(r"Основной отсев:\s*(.*)$", raw, re.I | re.S)
    lines = [
        "ℹ️ <b>Новых сигналов пока нет</b>",
        "",
        "🧭 <b>Итог сканирования</b>",
        ("<i>Ни один сетап не прошёл все проверки для публикации.</i>"
         if stats_match else f"<i>{reason_text}</i>"),
    ]
    if scan_match:
        a, b, c, d, e = scan_match.groups()
        lines += [
            f"🔎 Проверено монет: <b>{a}</b>",
            f"├ Кандидаты: <b>{b}</b>",
            f"├ Подтверждённые сетапы: <b>{c}</b>",
            f"├ Готовы к публикации: <b>{d}</b>",
            f"└ Опубликовано: <b>{e}</b>",
        ]
    if stats_match:
        bt, hist_err, analysis_err, wait, quality, no_history = map(int, stats_match.groups())
        lines += [
            "",
            "📊 <b>Почему сетапы не прошли</b>",
            f"• Слабый результат бэктеста: <b>{bt}</b>",
            f"• Рекомендация «ЖДАТЬ»: <b>{wait}</b>",
            f"• Недостаточно истории: <b>{no_history}</b>",
            f"• Качество ниже порога: <b>{quality}</b>",
            f"• Ошибки истории / анализа: <b>{hist_err} / {analysis_err}</b>",
        ]
    if reject_match:
        entries = []
        for part in reject_match.group(1).strip().rstrip(".").split(";"):
            part = part.strip()
            m = re.match(r"(.+?):\s*(\d+)$", part)
            if m:
                entries.append((m.group(1).strip(), int(m.group(2))))
        entries.sort(key=lambda x: x[1], reverse=True)
        if entries:
            lines += ["", "🧱 <b>Главные фильтры отбора</b>"]
            for name, count in entries[:5]:
                lines.append(f"• {html.escape(name)} — <b>{count}</b>")
            if len(entries) > 5:
                lines.append(f"<i>Ещё фильтров: {len(entries) - 5}</i>")
    lines += [
        "",
        "🛡️ <b>Почему это нормально</b>",
        "Бот не создаёт сигнал ради количества. Если бэктест слабый, таймфреймы спорят или вход запоздал — сетап остаётся за пределами публикации.",
        "",
        "💡 <i>Ожидание — тоже решение. Следующий сигнал появится только после прохождения проверок.</i>",
    ]
    return "\n".join(lines)


async def notify_no_signal_users(reason, details=""):
    """Редкое персональное уведомление; внутренние события RAM доступны только администраторам."""
    global last_no_signal_notice
    if is_memory_guard_reason(reason):
        log.info("RAM guard: уведомление подписчикам подавлено (только админ-панель)")
        return
    now = time.time()
    if now - last_no_signal_notice < 1800:
        return
    last_no_signal_notice = now
    technical_issue = "ошибка" in str(reason).lower()
    disclaimer = ("\n\n⚠️ <b>Техническое замечание:</b> обнаруженная ошибка записана в журнал и проверяется отдельно."
                  if technical_issue else "")
    body = format_no_signal_message(reason, details) + disclaimer
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📈 Открыть Mini App", web_app=WebAppInfo(url=webapp_link(tab="signals")))]] ) if WEBAPP_URL else None
    for cid in signal_recipients("", ""):
        try:
            await _send_new(cid, body, kb, None)
            await asyncio.sleep(0.03)
        except Exception as e:
            log.debug("No-signal notice %s: %s", cid, e)


async def scan_once(send=True):
    """Единый проход сканера: сначала блокировки, затем сетевые проверки и анализ."""
    global scanner_ranking
    if scan_lock is None or scan_lock.locked():
        return []
    async with scan_lock:
        t0 = time.time()
        def _scan_paused(reason):
            last_scan_info.update(ts=int(time.time()), secs=int(time.time() - t0), diagnostic=reason,
                                  universe=0, stage1=0, stage2=0, backtest_ok=0, final=0, sent=0, reasons=[],
                                  rss_mb=_current_rss_mb(), peak_rss_mb=_rss_mb(),
                                  bt_rejected=0, bt_unavailable=0, bt_errors=0, bt_error_examples=[],
                                  analysis_errors=0, analysis_wait=0, quality_rejected=0, analysis_error_examples=[])
            state_touch(False)
        rss_before_trim = _current_rss_mb()
        trim_info = {}
        trim_at = float(os.getenv("RAM_TRIM_MB", "390"))
        if rss_before_trim is not None and rss_before_trim >= trim_at:
            trim_info = _memory_pressure_cleanup(force=True)
        rss_now = _current_rss_mb()
        if trim_info:
            last_scan_info["memory_trim"] = trim_info
        if rss_now is not None and rss_now >= float(os.getenv("RAM_SCAN_STOP_MB", "440")):
            _scan_paused(f"Сканирование остановлено защитой памяти: RAM {rss_now:.1f} MB достигла порога")
            last_scan_info["memory_trim"] = trim_info
            log.error("Сканер пропущен: RAM %.1f MB достигла защитного порога (до очистки %s MB)",
                      rss_now, rss_before_trim)
            if alert_once("ram_guard_log", 1800):
                log.error("RAM guard зафиксирован в админ-панели: %.1f MB; очистка: %s",
                          rss_now, trim_info.get("cache_sizes", {}))
            return []
        if time.time() < news_blackout_until:
            if not await maybe_lift_blackout():
                _scan_paused("Пауза после критической новости: ждём снижения новостного риска")
                if send:
                    await notify_no_signal_users(last_scan_info["diagnostic"])
                return []
        # Не останавливаем сканер после серии убыточных сигналов:
        # каждый новый сетап оценивается обычными правилами отбора.
        if await check_macro_calendar():
            _scan_paused("Пауза из-за важной макроэкономической публикации (ФРС/CPI)")
            if send:
                await notify_no_signal_users(last_scan_info["diagnostic"])
            set_blackout(1800, "macro")
            if alert_once("macro", 2 * 3600):
                await notify_admin("🏦 Макро-новость (ФРС/CPI): сканер на паузе 30 минут.")
                asyncio.create_task(broadcast_market_alert(
                    title="🏦 Внимание: макро-новость (ФРС/CPI)",
                    reason="В окне важных данных США возможны резкие свечи, расширение спредов и ложные пробои.",
                    advice="Не открывайте новые сделки за 15 минут до и после публикации. Дождитесь стабилизации рынка.",
                    pause_mins=30))
            return []
        if await btc_shock():
            _scan_paused("Пауза из-за резкого движения BTC и повышенной волатильности")
            if send:
                await notify_no_signal_users(last_scan_info["diagnostic"])
            set_blackout(1800, "shock")
            if alert_once("shock", 2 * 3600):
                await notify_admin("⚠ Шок BTC: сканер на паузе 30 минут.")
                asyncio.create_task(broadcast_market_alert(
                    title="⚡ Шок волатильности на рынке",
                    reason="Последняя закрытая часовая свеча BTC превышает 2.5 ATR и ещё находится в окне актуальности.",
                    advice="Не открывайте новые сделки до стабилизации BTC.", pause_mins=30))
            return []
        if time.time() - market_bias["ts"] > 900:
            await update_market_bias()
        ctx = await get_context(force=True)
        onchain = await get_onchain_context()
        universe = await get_scan_universe()
        # Порог сканера = самый мягкий среди получателей, остальных отфильтруем при рассылке
        scan_conf = min([CONF_FINAL] + [float(u.get("conf_final", CONF_FINAL)) for u in _users().values()
                                        if isinstance(u, dict) and u.get("active", True) and u.get("notify_signals", True)])
        rej = {}

        def _rej(k):
            rej[k] = rej.get(k, 0) + 1

        sem = asyncio.Semaphore(2)
        stage1, stage2, final, bt_pass = [], [], [], 0
        bt_rejected = 0
        bt_unavailable = 0
        bt_errors = 0
        bt_error_examples = []
        analysis_errors = 0
        analysis_wait = 0
        quality_rejected = 0
        analysis_error_examples = []

        async def s1(coin):
            if time.time() < scan_skip.get(coin, 0):
                _rej("временный cooldown недоступной монеты")
                return
            async with sem:
                try:
                    tf2, _ = await get_market_data_light(coin, ("4h", "1d"))
                except LookupError:
                    _rej("монета недоступна на выбранном рынке")
                    scan_skip[coin] = time.time() + 6 * 3600
                    return
                except Exception as e:
                    _rej("ошибка/нет данных биржи")
                    log.debug("Stage1 %s: %s", coin, e)
                    return
                q = quick_check(tf2)
                if q:
                    rank = scanner_rank_score(tf2, q[1])
                    stage1.append((coin, tf2, rank))
                else:
                    if "4h" not in tf2 or "1d" not in tf2:
                        _rej("нет данных 4h/1d")
                    elif max((tf2["4h"]["long_prob"] + tf2["1d"]["long_prob"]) / 2, 100 - (tf2["4h"]["long_prob"] + tf2["1d"]["long_prob"]) / 2) < CONF_STAGE1:
                        _rej(f"сила ниже Stage 1 ({CONF_STAGE1:.0f})")
                    elif tf2["4h"].get("adx", 0) < 10:
                        _rej("слабый тренд ADX<10")
                    else:
                        _rej("не прошёл первичный фильтр")
                await asyncio.sleep(0.15)

        await asyncio.gather(*[s1(c) for c in universe])
        stage1 = sorted(stage1, key=lambda x: x[2], reverse=True)[:50]
        scanner_ranking = [{"coin": coin, "rank": idx + 1, "score": round(score, 1),
                            "direction": "LONG" if tf2.get("4h", {}).get("long_prob", 50) >= 50 else "SHORT",
                            "adx": round(float(tf2.get("4h", {}).get("adx", 0) or 0), 1),
                            "rsi": round(float(tf2.get("4h", {}).get("rsi", 50) or 50), 1)}
                           for idx, (coin, tf2, score) in enumerate(stage1)]

        async def s2(item):
            sc, tf2, _ = item
            # Повторный сетап по монете после убытка не блокируется:
            # каждое новое направление проходит обычные проверки с нуля.
            async with sem:
                tf_data = dict(tf2)
                try:
                    extra, _src = await get_market_data_light(sc, ("1h", "15m"))
                    tf_data.update(extra)
                except Exception:
                    pass
                setup = await evaluate_setup(sc, tf_data, ctx, await get_funding(sc), scan=True,
                                             onchain_change=onchain, conf_threshold=scan_conf + adaptive_conf_bonus(ctx.get("btc_regime")))
                if setup["valid"]:
                    stage2.append((sc, tf_data, setup))
                else:
                    _rej(setup.get("reject") or "не прошёл фильтр сетапа")

        await asyncio.gather(*[s2(i) for i in stage1])
        stage1_count = len(stage1)
        stage2_count = len(stage2)
        # Stage 2 owns its own timeframe dictionaries; Stage 1 dataframes are no longer needed.
        stage1.clear()
        gc.collect()

        sem3 = asyncio.Semaphore(2)

        async def s3(item):
            nonlocal bt_pass, bt_rejected, bt_unavailable, bt_errors, analysis_errors, analysis_wait, quality_rejected
            coin, tf_data, setup = item
            async with sem3:
                try:
                    bt = await get_backtest(coin)
                except Exception as e:
                    bt = None
                    bt_errors += 1
                    if len(bt_error_examples) < 3:
                        bt_error_examples.append(f"{coin}: {type(e).__name__}: {str(e)[:180]}")
                    _rej("ошибка получения истории бэктеста")
                    log.exception("Ошибка получения бэктеста %s", coin)
                else:
                    if bt is not None and not isinstance(bt, dict):
                        bt_errors += 1
                        if len(bt_error_examples) < 3:
                            bt_error_examples.append(f"{coin}: некорректный формат результата бэктеста")
                        _rej("некорректный результат бэктеста")
                        bt = None
                    if bt is None or bt.get("trades", 0) < BT_MIN_TRADES:
                        bt_unavailable += 1
                try:
                    if bt is not None and bt.get("trades", 0) >= BT_MIN_TRADES and not bt_ok(bt):
                        bt_rejected += 1
                        _rej("бэктест слабый")
                        return
                except Exception as e:
                    bt_errors += 1
                    if len(bt_error_examples) < 3:
                        bt_error_examples.append(f"{coin}: {type(e).__name__}: {str(e)[:180]}")
                    _rej("ошибка проверки бэктеста")
                    log.exception("Ошибка проверки бэктеста %s", coin)
                    bt = None
                    bt_unavailable += 1
                bt_pass += 1
                try:
                    res = await build_analysis(coin, tf_data, setup, bt, ctx, "scan")
                    # Дополнительный детерминированный контроль качества: score факторов, не вероятность прибыли.
                    primary_tf = tf_data.get("4h") or tf_data.get("1h") or {}
                    primary_df = primary_tf.get("df") if isinstance(primary_tf, dict) else None
                    res["market_regime"] = market_regime(primary_df)
                    res["volume_anomaly"] = volume_anomaly(primary_df)
                    res["quality_score"] = score_signal(res)
                    res["quality_label"] = ("A" if res["quality_score"] >= 82 else "B" if res["quality_score"] >= 72
                                             else "C" if res["quality_score"] >= 62 else "D")
                    min_quality = float(os.getenv("MIN_SIGNAL_QUALITY", "62"))
                    # Reconcile every fresh scan: WAIT, opposite side, low quality or lost entry.
                    reconcile_res = dict(res)
                    if res["verdict"] == "ENTER" and res["quality_score"] < min_quality:
                        reconcile_res["verdict"] = "WAIT"
                        reconcile_res["comment"] = f"Качество сигнала {res['quality_score']:.0f}/100 ниже порога {min_quality:.0f}."
                    await drop_waiting_signals(coin, reconcile_res)
                    if res["verdict"] == "ENTER" and res["quality_score"] >= min_quality:
                        final.append(res)
                        analysis_cache[coin] = (time.time(), 60, res)
                        _trim_timed_cache(analysis_cache, 50)
                        save_analysis_db(coin, res)
                    elif res["verdict"] != "ENTER":
                        analysis_wait += 1
                        _rej("сетап получил рекомендацию ЖДАТЬ")
                    else:
                        quality_rejected += 1
                        _rej(f"качество ниже порога {min_quality:.0f} ({res['quality_score']:.0f})")
                except Exception as e:
                    analysis_errors += 1
                    if len(analysis_error_examples) < 3:
                        analysis_error_examples.append(f"{coin}: {type(e).__name__}: {str(e)[:180]}")
                    _rej("ошибка построения анализа")
                    log.exception("Ошибка построения анализа %s", coin)
                    return

        # Never keep every candidate's 4-TF dataframe alive for the entire backtest stage.
        while stage2:
            batch = stage2[:2]
            del stage2[:2]
            await asyncio.gather(*[s3(i) for i in batch])
            batch.clear()
            if (_current_rss_mb() or 0) >= float(os.getenv("RAM_TRIM_MB", "390")):
                _memory_pressure_cleanup(force=True)
        final.sort(key=lambda r: r["strength"], reverse=True)
        # Only the three publishable candidates need their generated preview image retained.
        keep_images = 3 if send else 0
        for extra_result in final[keep_images:]:
            extra_result["img"] = None
        last_scan_info.update(ts=int(time.time()), stage1=stage1_count, stage2=stage2_count, backtest_ok=bt_pass,
                              final=len(final), secs=int(time.time() - t0))
        state_touch(False)
        log.info(f"Сканер: {len(universe)} → {stage1_count} → {stage2_count} → бэктест {bt_pass} → итог {len(final)}")

        new_items = []
        if send:
            now = time.time()
            hist = load_signal_history()
            busy = {f"{x['coin']}:{x['side']}" for x in hist if x.get("status") in ("active", "in_trade")}
            used_groups = set()
            for r in final:
                key = f"{r['coin']}:{r['side']}"
                if key in busy:
                    _rej("по монете уже есть активный сигнал")
                    continue
                if now - signal_sent_at.get(f"{r['coin']}:{'L' if r['is_long'] else 'S'}", 0) < SIGNAL_COOLDOWN:
                    _rej("период ожидания после прошлого сигнала")
                    continue
                if len(new_items) >= 3:
                    _rej("лимит 3 новых сигнала за проход")
                    continue
                if abs(r["entry"] - r["price"]) / max(r["price"], 1e-12) > 0.03:
                    _rej("цена ушла от точки входа более чем на 3%")
                    continue
                grp = next((i for i, g in enumerate(CORRELATION_GROUPS) if r["coin"] in g), None)
                if grp is not None:
                    if (grp, r["side"]) in used_groups:
                        _rej("корреляционный фильтр: уже выбран похожий актив")
                        continue
                    used_groups.add((grp, r["side"]))
                signal_sent_at[f"{r['coin']}:{'L' if r['is_long'] else 'S'}"] = now
                item = {"ts": now, "expire_ts": now + SIGNAL_TTL, "coin": r["coin"], "side": r["side"], "type": r["type"],
                        "strength": r["strength"], "entry": r["entry"], "tp1": r["tps"][0], "tp2": r["tps"][1],
                        "tp3": r["tps"][2], "sl1": r["sls"][0], "sl2": r["sls"][1], "status": "active",
                        "max_tp_hit": 0, "hit_sl": 0, "price": r["price"], "sent_to": [], "is_long": r["is_long"],
                        "tf_probs": r.get("tf_probs") or {}, "text": r["text"], "quality_score": r.get("quality_score"),
                        "quality_label": r.get("quality_label"), "signal_phase": r.get("signal_phase", "CONFIRMED"),
                        "market_regime": r.get("market_regime"),
                        "volume_anomaly": r.get("volume_anomaly"),
                        "market_fingerprint": (r.get("market_fingerprint") or {}).get("features", {}),
                        "signal_dna": r.get("signal_dna"), "decision_brain": r.get("decision_brain"),
                        "regime_transition": r.get("regime_transition"),
                        "signal_fragility": r.get("signal_fragility"),
                        "evidence_conflict_map": r.get("evidence_conflict_map"),
                        "memory_features": {"regime": (r.get("market_regime") or {}).get("name", "unknown"),
                                             "quality": r.get("quality_score"), "atr_pct": (r.get("indicators") or {}).get("atr_pct"),
                                             "rsi": r.get("rsi"), "adx": r.get("adx"), "btc_context": r.get("btc_txt"),
                                             "breakout": (r.get("breakout") or {}).get("state")},
                        "reasons": [re.sub(r"\s*\(.*\)$", "", s) for s in r.get("reasons", [])][:6], "hour": datetime.now(timezone(timedelta(hours=3))).hour, "daily": False, "mfe": 0.0, "mae": 0.0}
                hist.append(item)
                new_items.append((item, r))
            if new_items:
                save_signal_history(hist)
                stats["signals"] += len(new_items)
                for item, r in new_items:
                    await deliver_new_signal(item, r)
        top_items = sorted(rej.items(), key=lambda kv: -kv[1])[:6]
        top = "; ".join(f"{k}: {v}" for k, v in top_items)
        sent_count = len(new_items)
        if sent_count:
            diagnostic = f"Найдено и опубликовано сигналов: {sent_count}"
        elif final:
            diagnostic = "Сетапы найдены, но не прошли ограничения публикации (активный сигнал, cooldown, корреляция или лимит за проход)"
        elif not stage1_count:
            diagnostic = "Кандидаты не прошли первичный фильтр силы/ADX или не получены данные 4h/1d"
        elif not stage2_count:
            diagnostic = "Первичные кандидаты есть, но сетапы не подтвердились на дополнительных таймфреймах/фильтрах"
        else:
            diagnostic = scan_diagnostic_summary(
                bt_rejected=bt_rejected, analysis_errors=analysis_errors, wait_count=analysis_wait,
                quality_rejected=quality_rejected, bt_unavailable=bt_unavailable, bt_errors=bt_errors)
        last_scan_info.update(ts=int(time.time()), stage1=stage1_count, stage2=stage2_count, backtest_ok=bt_pass,
                              universe=len(universe), final=len(final), sent=sent_count, secs=int(time.time() - t0),
                              rss_mb=_current_rss_mb(), peak_rss_mb=_rss_mb(),
                              bt_rejected=bt_rejected, bt_unavailable=bt_unavailable, bt_errors=bt_errors,
                              bt_error_examples=bt_error_examples, analysis_errors=analysis_errors, analysis_wait=analysis_wait,
                              quality_rejected=quality_rejected, analysis_error_examples=analysis_error_examples,
                              diagnostic=diagnostic, reasons=[{"reason": k, "count": v} for k, v in top_items])
        state_touch(False)
        log.info("Сканер: %s → %s → %s → backtest %s → final %s → sent %s; %s",
                 len(universe), stage1_count, stage2_count, bt_pass, len(final), sent_count, diagnostic)
        if send:
            technical_lines = ("\n\n<b>Технические ошибки:</b>\n" + "\n".join(
                html.escape(str(e)) for e in (bt_error_examples + analysis_error_examples)[:5])
                if bt_error_examples or analysis_error_examples else "")
            await notify_admin(f"🔍 <b>Скан завершён</b> за {last_scan_info['secs']} с\n"
                               f"Монет: {len(universe)} → {stage1_count} → {stage2_count} · к анализу допущено {bt_pass} · итог {len(final)} · отправлено {sent_count}\n"
                               f"🧾 Причина: {html.escape(diagnostic)}\nОтсев: {html.escape(top or '—')}" + technical_lines)
            if sent_count == 0:
                breakdown = (f"Бэктест отклонил: {bt_rejected}; ошибок получения истории: {bt_errors}; "
                             f"ошибок анализа: {analysis_errors}; ЖДАТЬ: {analysis_wait}; "
                             f"качество ниже порога: {quality_rejected}; без истории: {bt_unavailable}.")
                await notify_no_signal_users(diagnostic,
                    f"Скан: {len(universe)} монет → {stage1_count} кандидатов → {stage2_count} сетапов → "
                    f"{len(final)} готовых → 0 опубликовано. {breakdown} "
                    + ("Основной отсев: " + top if top else "Фильтры рынка пока не нашли подходящих входов."))
        gc.collect()
        return final


def webapp_link(coin=None, tab=None):
    base = WEBAPP_URL or f"https://t.me/{(runtime['main_bot'] or '').lstrip('@') or 'ixcly'}"
    q = []
    if coin:
        q.append(f"coin={coin}")
    if tab:
        q.append(f"tab={tab}")
    return base + (("?" + "&".join(q)) if q else "")


def simulate_signal(x, rows, now=None):
    """Статус по 15m свечам: ожидание входа лимиткой → в сделке → TP1-3 с трейлингом."""
    now = now or time.time()
    is_long = x["side"] == "LONG"
    entry, tp1, tp2, tp3, sl = x["entry"], x["tp1"], x["tp2"], x["tp3"], x["sl1"]
    t0 = x["ts"] * 1000
    state, max_tp = "waiting", 0
    breakeven_armed = False
    for ts, _o, h, l, _c, _v in rows:
        if ts < t0:
            continue
        if state == "waiting":
            if (is_long and h >= tp1 and l > entry) or ((not is_long) and l <= tp1 and h < entry):
                return "missed", 0, 0
            if (l <= entry) if is_long else (h >= entry):
                state = "in_trade"
                if (l <= sl) if is_long else (h >= sl):
                    return "loss", 0, 1
            continue
        if (l <= sl) if is_long else (h >= sl):
            return ("win" if max_tp >= 1 else "loss"), max_tp, 1
        # Arm only after a full +2.5% favorable excursion; the new stop applies from the next candle.
        if (h >= entry * 1.025) if is_long else (l <= entry * 0.975):
            breakeven_armed = True
        if max_tp == 1 and breakeven_armed:
            sl = entry
        lvl = (3 if h >= tp3 else 2 if h >= tp2 else 1 if h >= tp1 else 0) if is_long else \
              (3 if l <= tp3 else 2 if l <= tp2 else 1 if l <= tp1 else 0)
        if lvl > max_tp:
            max_tp = lvl
            sl = tp1 if max_tp >= 2 else entry if (breakeven_armed and max_tp >= 1) else x["sl1"]
            if max_tp == 3:
                return "win", 3, 0
    if state == "waiting":
        return ("expired", 0, 0) if now > x.get("expire_ts", 0) else ("active", 0, 0)
    if now > x["ts"] + 7 * 86400:
        return ("win" if max_tp >= 1 else "expired"), max_tp, 0
    return "in_trade", max_tp, 0


async def notify_signal_holders(coin, side, text, kb=None, extra_uids=(), order_key=None):
    """Notify signal holders once; suppress global duplicates for tracked/closed personal orders."""
    uids = set(int(x) for x in extra_uids)
    for uid, u in list(_users().items()):
        if not isinstance(u, dict):
            continue
        if coin in (u.get("open_positions") or {}) or any(s.get("coin") == coin for s in u.get("saved_signals", [])):
            uids.add(int(uid))
    for uid in uids:
        try:
            tracked = get_open_position(uid, coin)
            if tracked and tracked.get("side") == side:
                # Personal position monitor owns entry/TP/SL/close messages; avoid duplicate global copies.
                continue
            if order_key and order_key in _users().get(str(uid), {}).get("closed_order_keys", []):
                # A position monitor has already closed this exact signal order for this user.
                continue
            if not has_access(uid) or not get_user_notif_settings(uid)["pos_updates"]:
                continue
            await _send_new(uid, text, kb or kb_nav(back=False), None)
            await asyncio.sleep(0.05)
        except Exception as e:
            log.info(f"notify {uid}: {e}")


def live_check_signal(x, live):
    """Правила ликвидности сигнала по свежему анализу. Возвращает причину или None."""
    price = live["price"]
    is_long = x["side"] == "LONG"
    sign = 1 if is_long else -1
    atr = live.get("atr") or price * 0.01
    if sign * (price - x["sl1"]) <= 0:
        return f"Цена пробила стоп-уровень ({fmt(x['sl1'])}) до входа — структура сломана."
    if abs(price - x["entry"]) > INVALID_ATR_MULT * atr and x.get("status") == "active":
        return f"Цена ушла на {abs(price - x['entry']) / atr:.1f} ATR от входа, вход потерян."
    p4, p1h = live["long_prob"].get("4h", 50), live["long_prob"].get("1h", 50)
    cs4 = p4 if is_long else 100 - p4
    cs1h = p1h if is_long else 100 - p1h
    if cs4 < 45 and cs1h < 40:
        return "Тренд развернулся против сигнала (4h и 1h). Идея больше не работает."
    return None


async def live_refresh_coin(coin):
    """Живое обновление одной монеты: свежие свечи, цена, вероятности. Пишет в live-кэш."""
    try:
        tf_data, source = await get_market_data_light(coin, ("15m", "1h", "4h", "1d"))
    except Exception as e:
        log.info(f"live {coin}: {e}")
        return None
    _close4h = tf_data["4h"].get("df")
    _change24h = None
    try:
        _closes = _close4h["close"].astype(float).tolist()
        if len(_closes) >= 7 and _closes[-7] > 0:
            _change24h = round((_closes[-1] / _closes[-7] - 1) * 100, 4)
    except Exception:
        pass
    live = {"ts": time.time(), "price": tf_data["4h"]["price"], "atr": tf_data["4h"]["atr"], "change_pct": _change24h,
            "long_prob": {t: round(tf_data[t]["long_prob"], 1) for t in tf_data},
            "rsi": {t: round(tf_data[t]["rsi"], 1) for t in tf_data},
            "adx": round(tf_data["4h"]["adx"], 1), "regime": tf_data["4h"]["regime"], "source": source}
    LIVE[coin] = live
    try:
        # One observation per 5-minute bucket makes cross-asset returns timestamp-aligned.
        series = LIVE_PRICE_HISTORY.setdefault(coin, [])
        bucket = int(live["ts"] // 300) * 300
        point = (bucket, float(live["price"]))
        if series and int(series[-1][0]) == bucket:
            series[-1] = point
        elif not series or bucket > int(series[-1][0]):
            series.append(point)
        if len(series) > 576:  # 48h at 5m resolution
            del series[:-576]
        if len(LIVE_PRICE_HISTORY) > 100:
            oldest = sorted(LIVE_PRICE_HISTORY, key=lambda k: LIVE_PRICE_HISTORY[k][-1][0] if LIVE_PRICE_HISTORY.get(k) else 0)[:20]
            for key in oldest:
                LIVE_PRICE_HISTORY.pop(key, None)
        _persist_quant_price_history()
    except Exception as _history_err:
        log.debug("quant history update %s: %s", coin, _history_err)
    return live


LIVE = {}
LIVE_PRICE_HISTORY = {}
_quant_history_last_persist = 0.0

def restore_quant_price_history():
    """Restore bounded 5m close snapshots from the durable STATE store."""
    LIVE_PRICE_HISTORY.clear()
    raw = STATE.get("quant_price_history", {})
    if not isinstance(raw, dict):
        return 0
    for coin, rows in list(raw.items())[:100]:
        clean = []
        for row in (rows or [])[-576:]:
            try:
                ts, price = int(row[0]), float(row[1])
                if ts > 0 and np.isfinite(price) and price > 0:
                    clean.append((ts, price))
            except Exception:
                continue
        if clean:
            LIVE_PRICE_HISTORY[str(coin).upper()] = clean
    return len(LIVE_PRICE_HISTORY)


def _persist_quant_price_history(force=False):
    global _quant_history_last_persist
    now = time.time()
    if not force and now - _quant_history_last_persist < 300:
        return
    with STATE_LOCK:
        STATE["quant_price_history"] = {c: list(rows[-576:]) for c, rows in list(LIVE_PRICE_HISTORY.items())[-100:] if rows}
    _quant_history_last_persist = now
    state_touch(False)


def build_synchronized_asset_returns(max_assets=24, min_points=31):
    """Greedily build one cohort sharing exact 5m timestamps; never align by array index."""
    if "BTC" not in LIVE_PRICE_HISTORY:
        return {}
    maps = {asset: {int(ts): float(price) for ts, price in rows if price > 0}
            for asset, rows in LIVE_PRICE_HISTORY.items()}
    btc = maps.get("BTC", {})
    if len(btc) < min_points + 1:
        return {}
    candidates = []
    for asset, points in maps.items():
        if asset == "BTC":
            continue
        common = set(btc).intersection(points)
        if len(common) >= min_points + 1:
            candidates.append((len(common), asset, points))
    candidates.sort(reverse=True)
    common = set(btc)
    selected = []
    for _, asset, points in candidates:
        trial = common.intersection(points)
        if len(trial) >= min_points + 1:
            common = trial
            selected.append((asset, points))
        if len(selected) >= max(1, int(max_assets)):
            break
    if not selected:
        return {}
    timestamps = sorted(common)[-577:]
    btc_prices = np.asarray([btc[t] for t in timestamps], dtype=float)
    btc_returns = (np.diff(btc_prices) / np.maximum(btc_prices[:-1], 1e-12)).tolist()
    out = {"BTC": btc_returns}
    for asset, points in selected:
        prices = np.asarray([points[t] for t in timestamps], dtype=float)
        out[asset] = (np.diff(prices) / np.maximum(prices[:-1], 1e-12)).tolist()
    return out


async def live_monitor_loop():
    """Каждые 30 с: обновляет данные по каждой активной монете и проверяет ликвидность сигналов."""
    await asyncio.sleep(40)
    while True:
        try:
            hist = load_signal_history()
            await retry_failed_signal_deliveries(hist)
            open_coins = {c for u in _users().values() if isinstance(u, dict)
                          for c in (u.get("open_positions") or {})}
            watch = {x["coin"] for x in hist if x.get("status") in ("active", "in_trade")} | open_coins
            changed = False
            for coin in sorted(watch)[:40]:
                live = await live_refresh_coin(coin)
                await asyncio.sleep(0.2)
                if not live:
                    continue
                for x in hist:
                    if x["coin"] != coin or x.get("status") not in ("active", "in_trade"):
                        continue
                    x["live_price"] = live["price"]
                    x["live_ts"] = live["ts"]
                    if x.get("status") == "active":
                        decay = signal_decay_assessment(x, current_price=live.get("price"), atr=live.get("atr"), ttl_seconds=SIGNAL_TTL)
                        x["decay_score"] = decay.get("decay_score")
                        if not decay.get("valid", True):
                            await invalidate_signal(x, "Сигнал потерял актуальность: " + ", ".join(decay.get("reasons") or ["условия изменились"]))
                            changed = True
                            continue
                    reason = live_check_signal(x, live)
                    if reason:
                        await invalidate_signal(x, reason)
                        changed = True
            if changed:
                save_signal_history(hist)
        except Exception as e:
            log.error(f"live_monitor: {e}", exc_info=True)
        next_timers["global_signals"] = time.time() + 30
        await asyncio.sleep(30)


async def scanner_loop():
    await asyncio.sleep(45)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await scan_once(send=True)
        except Exception as e:
            log.error(f"scanner: {e}", exc_info=True)
            await notify_admin(f"🚨 <b>Ошибка сканера:</b>\n<code>{html.escape(str(e))}</code>")
        next_timers["scanner"] = time.time() + SCAN_INTERVAL
        await asyncio.sleep(SCAN_INTERVAL)


# =====================================================================
# ПОЗИЦИИ, ЛИЧНАЯ СТАТИСТИКА, СОПРОВОЖДЕНИЕ
# =====================================================================
def record_trade_event(uid, coin, pos, event, note="", price=None):
    """Сохраняет журнал жизненного цикла ордера/сделки, не только итоговую статистику."""
    now = time.time()
    item = {
        "ts": now, "coin": str(coin), "side": pos.get("side"), "event": str(event),
        "entry": pos.get("entry_price"), "price": price,
        "tp1": pos.get("tp1"), "tp2": pos.get("tp2"), "tp3": pos.get("tp3"),
        "sl": pos.get("sl1"), "order_key": pos.get("order_key"), "note": str(note or "")[:240],
    }
    with STATE_LOCK:
        user = STATE["users"].setdefault(str(uid), {"active": True})
        events = user.setdefault("trade_events", [])
        events.append(item)
        user["trade_events"] = events[-500:]
    state_touch()
    return item


def _tracked_order_key(coin, res):
    """Stable key for one signal limit order; a closed signal cannot be re-announced as a fresh order."""
    identity = res.get("signal_id") or res.get("signal_ts") or res.get("ts") or res.get("created_at")
    if identity is None:
        identity = f"{float(res['entry']):.8f}"
    raw = f"{str(coin).upper()}|{res['side']}|{identity}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _ensure_tracked_order_key(uid, coin, pos):
    """Backfill stable IDs for positions created by older IXCLY builds."""
    if pos.get("order_key"):
        return pos["order_key"]
    candidate = next((x for x in sorted(load_signal_history(), key=lambda z: z.get("ts", 0), reverse=True)
                      if x.get("coin") == coin and x.get("side") == pos.get("side") and
                      abs(float(x.get("entry", 0)) - float(pos.get("entry_price", 0))) /
                      max(1e-12, float(pos.get("entry_price", 0))) <= 0.005), None)
    if candidate:
        key = _tracked_order_key(coin, sig_as_res(candidate))
    else:
        key = _tracked_order_key(coin, {"side": pos["side"], "entry": pos["entry_price"],
                                        "created_at": pos.get("created_at", 0)})
    pos["order_key"] = key
    set_user_open_position(uid, coin, pos)
    return key


def _remember_closed_order(uid, pos):
    key = pos.get("order_key")
    if not key:
        return
    with STATE_LOCK:
        user = STATE["users"].setdefault(str(uid), {"active": True})
        closed = user.setdefault("closed_order_keys", [])
        if key not in closed:
            closed.append(key)
            del closed[:-80]
    state_touch()


def open_tracked_position(uid, coin, res):
    """Create one virtual tracked limit order with strict portfolio-risk caps."""
    if get_open_position(uid, coin):
        return False
    res = dict(res)
    if res.get("verdict") and res.get("verdict") != "ENTER":
        return False
    if res.get("risk_state") and res.get("risk_state") != "GREEN":
        return False
    if res.get("stale") or res.get("price_only"):
        return False
    if res.get("ts") and time.time() - float(res.get("ts", 0) or 0) > 300:
        return False
    side = str(res.get("side", "")).upper()
    try:
        entry = float(res.get("entry")); stops = [float(x) for x in (res.get("sls") or [])]
        targets = [float(x) for x in (res.get("tps") or [])]
    except (TypeError, ValueError):
        return False
    if side not in ("LONG", "SHORT") or not np.isfinite(entry) or entry <= 0 or len(stops) < 1 or len(targets) != 3:
        return False
    stop = stops[0]
    if not np.isfinite(stop) or (side == "LONG" and stop >= entry) or (side == "SHORT" and stop <= entry):
        return False
    if any(not np.isfinite(x) or (x <= entry if side == "LONG" else x >= entry) for x in targets):
        return False
    _current_price = float(res.get("price") or 0)
    _atr = float(res.get("atr") or 0)
    if _current_price > 0 and _atr > 0:
        _chase = chasing_protection(_current_price, entry, _atr, side)
        if _chase.get("status") == "ok" and not _chase.get("allow"):
            return False
    user_state = _users().get(str(uid), {}) if isinstance(_users().get(str(uid), {}), dict) else {}
    open_now = list((user_state.get("open_positions") or {}).values())
    risk_settings = get_user_risk_settings(uid)
    _closed_log = [x for x in (user_state.get("trade_log", []) or []) if x.get("closed_at")]
    _equity_now = next((float(x.get("equity_after")) for x in reversed(_closed_log) if x.get("equity_after")), float(risk_settings.get("deposit", DEPOSIT)))
    _day_start = time.time() - 86400
    _day_losses = [float(x.get("usd", 0) or 0) for x in _closed_log if float(x.get("closed_at", 0) or 0) >= _day_start]
    _daily_guard = daily_loss_guard(_day_losses, _equity_now, float(user_state.get("max_daily_loss_pct", 3.0) or 3.0)) if _day_losses else {"locked":False}
    _equity_curve = [float(x.get("equity_after")) for x in _closed_log if x.get("equity_after")]
    _drawdown = drawdown_guard(_equity_curve, float(user_state.get("max_drawdown_pct", 12.0) or 12.0)) if len(_equity_curve) >= 3 else {"locked":False}
    if emergency_risk_lock(None, _drawdown, _daily_guard, None, True).get("locked"):
        return False
    requested_risk = float(res.get("risk_pct") or risk_settings.get("risk_pct", RISK_PCT))
    max_total = float(user_state.get("max_open_risk_pct", 5.0) or 5.0)
    total_open = sum(max(0.0, float(x.get("risk_pct", 0) or 0)) for x in open_now)
    same_side = sum(max(0.0, float(x.get("risk_pct", 0) or 0)) for x in open_now if str(x.get("side", "")).upper() == side)
    group_risk = sum(max(0.0, float(x.get("risk_pct", 0) or 0)) for x in open_now
                     if str(x.get("side", "")).upper() == side and any(coin in group and x.get("coin") in group for group in CORRELATION_GROUPS))
    remaining_budget = min(max_total-total_open, max_total*.65-same_side, max_total*.50-group_risk)
    if remaining_budget <= 0:
        log.info("Tracked position rejected by risk cap uid=%s coin=%s total=%.2f same_side=%.2f group=%.2f", uid, coin, total_open, same_side, group_risk)
        return False
    allocation = risk_budget_allocator(float(risk_settings.get("deposit", DEPOSIT)),
        [{"coin":coin,"quality":float(res.get("strength",50) or 50),"correlation_penalty":1.0 if group_risk else 0.0}],
        total_risk_pct=remaining_budget)
    allocated = float((allocation.get("allocations") or [{}])[0].get("risk_pct", 0) or 0)
    requested_risk = min(requested_risk, allocated)
    if requested_risk <= 0:
        return False
    if not (res.get("signal_id") or res.get("signal_ts")):
        candidate = next((x for x in get_active_signals() if x.get("coin") == coin and
                          x.get("side") == res.get("side") and
                          abs(float(x.get("entry", 0)) - float(res.get("entry", 0))) / max(1e-12, float(res.get("entry", 0))) <= 0.005), None)
        if candidate:
            res["signal_ts"] = candidate.get("ts")
    order_key = _tracked_order_key(coin, res)
    closed_keys = _users().get(str(uid), {}).get("closed_order_keys", [])
    if order_key in closed_keys:
        return False
    _prob_data = res.get("probability") or (res.get("quant") or {}).get("bayesian_probability") or {}
    _predicted_probability = (_prob_data.get("long_pct" if side == "LONG" else "short_pct") / 100.0
                              if isinstance(_prob_data, dict) and isinstance(_prob_data.get("long_pct" if side == "LONG" else "short_pct"), (int, float)) else None)
    position = {
        "order_key": order_key,
        "side": side, "entry_price": entry, "created_at": time.time(), "entered_at": None,
        "position_mode": "virtual_tracked", "risk_pct": requested_risk,
        "risk_budget_requested_pct": float(res.get("risk_pct") or risk_settings.get("risk_pct", RISK_PCT)),
        "risk_budget_allocated_pct": requested_risk,
        "strategy_type": str(res.get("type", "unknown"))[:80],
        "strategy_score": float((res.get("quant") or {}).get("risk_score", res.get("strength", 0)) or 0),
        "quant_parameters": {"risk_state": (res.get("quant") or {}).get("risk_state"),
            "risk_score": (res.get("quant") or {}).get("risk_score"),
            "entry_quality": (res.get("quant") or {}).get("entry_quality_score"),
            "model_confidence": (res.get("quant") or {}).get("model_confidence")},
        "predicted_probability": _predicted_probability,
        "entry_source": "user" if res.get("entry_override") else "signal",
        "tp1": targets[0], "tp2": targets[1], "tp3": targets[2], "sl1": stops[0], "sl2": stops[1] if len(stops) > 1 else stops[0],
        "sl_orig": stops[0],
        "notified_close": False, "waiting_entry": True, "peak_pnl": 0.0}
    set_user_open_position(uid, coin, position)
    record_trade_event(uid, coin, position, "limit_created", "Пользователь подтвердил вход по сигналу")
    return True


def record_trade_result(uid, coin, pos, exit_price, reason):
    """Итог сделки: TP1 = 50% позиции, TP2 = 30%, остаток по цене выхода."""
    if pos.get("waiting_entry"):
        return None
    _remember_closed_order(uid, pos)
    entry, side = pos["entry_price"], pos["side"]
    sign = 1 if side == "LONG" else -1
    pnl_at = lambda p: price_change_pct(side, entry, p)
    h3 = pos.get("hit_tp3")
    h2 = pos.get("hit_tp2") or h3
    h1 = pos.get("hit_tp1") or h2
    parts, rest = [], 1.0
    if h1:
        parts.append((0.5, pnl_at(pos["tp1"])))
        rest -= 0.5
    if h2:
        parts.append((0.3, pnl_at(pos["tp2"])))
        rest -= 0.3
    parts.append((rest, pnl_at(exit_price)))
    pnl = sum(w * p for w, p in parts) - FEE_PCT
    pnl -= (max(0.0, time.time() - pos.get("entered_at", time.time())) / 3600 / 8) * 0.01  # средний фандинг, оценка
    dist = abs(entry - pos.get("sl_orig", pos["sl1"])) / entry * 100
    r = pnl / dist if dist > 0 else 0.0
    s = get_user_risk_settings(uid)
    now = time.time()
    rec = {"coin": coin, "side": side, "entry": entry, "exit": exit_price, "order_key": pos.get("order_key"), "pnl": round(pnl, 2), "r": round(r, 2),
           "usd": round(s["deposit"] * s["risk_pct"] / 100 * r, 2),
           "result": "win" if (h1 or pnl > 0.05) else "loss" if pnl < -0.05 else "be",
           "reason": reason, "strategy_type": pos.get("strategy_type", "unknown"), "max_tp": 3 if h3 else 2 if h2 else 1 if h1 else 0,
           "peak": round(pos.get("peak_pnl", 0.0), 2), "opened_at": pos.get("entered_at", now), "closed_at": now,
           "predicted_probability": pos.get("predicted_probability"),
           "strategy_score": pos.get("strategy_score"),
           "quant_parameters": pos.get("quant_parameters"),
           "adverse_selection": pos.get("adverse_selection"),
           "post_fill_move_pct": pos.get("post_fill_move_pct"),
           "hold_h": round((now - pos.get("entered_at", now)) / 3600, 1)}
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(uid), {"active": True})
        prior_log = list(u.get("trade_log", []) or [])
        prior_usd = sum(float(x.get("usd", 0) or 0) for x in prior_log)
        rec["equity_before"] = round(float(s["deposit"]) + prior_usd, 2)
        rec["equity_after"] = round(rec["equity_before"] + float(rec["usd"]), 2)
        rec["portfolio_return_pct"] = round(float(rec["usd"]) / max(1e-9, rec["equity_before"]) * 100, 5)
        u["trade_log"] = (prior_log + [rec])[-300:]
    record_trade_event(uid, coin, pos, "closed", reason, price=exit_price)
    state_touch()
    return rec


def render_trade_card(rec):
    icon = {"win": "✅", "loss": "❌", "be": "➖"}[rec["result"]]
    return (f"{icon} <b>Итог сделки: {rec['coin']} ({rec['side']})</b>\n"
            f"Вход: <code>{fmt(rec['entry'])}</code> → Выход: <code>{fmt(rec['exit'])}</code>\n"
            f"Результат: <b>{rec['pnl']:+.2f}%</b> · {rec['r']:+.2f}R · {rec['usd']:+.2f}$\n"
            f"Удержание: {rec['hold_h']} ч · взято целей: {rec['max_tp']} · {rec['reason']}")


async def close_position_manual(uid, coin):
    pos = get_open_position(uid, coin)
    rec = None
    if pos:
        _ensure_tracked_order_key(uid, coin, pos)
    if pos and pos.get("waiting_entry"):
        _remember_closed_order(uid, pos)
        record_trade_event(uid, coin, pos, "limit_cancelled", "Пользователь отменил лимитный вход")
    if pos and not pos.get("waiting_entry"):
        price = await get_simple_price(coin)
        if price:
            rec = record_trade_result(uid, coin, pos, price, "закрыто вручную")
    if pos and not rec:
        _remember_closed_order(uid, pos)
    set_user_open_position(uid, coin, None)
    if rec:
        await _tell(uid, render_trade_card(rec))
    return rec


def personal_stats(uid):
    t = list(_users().get(str(uid), {}).get("trade_log", []))
    n = len(t)
    if not n:
        return {"n": 0}
    wins = sum(1 for x in t if x["result"] == "win")
    losses = sum(1 for x in t if x["result"] == "loss")
    closed = wins + losses
    gp = sum(x["pnl"] for x in t if x["pnl"] > 0)
    gl = -sum(x["pnl"] for x in t if x["pnl"] < 0)
    kind, streak = t[-1]["result"], 0
    for x in reversed(t):
        if x["result"] != kind:
            break
        streak += 1
    coins = {}
    for x in t:
        coins[x["coin"]] = coins.get(x["coin"], 0) + x["pnl"]
    rk = sorted(coins.items(), key=lambda kv: kv[1])

    def swr(sd):
        s = [x for x in t if x["side"] == sd and x["result"] != "be"]
        return round(sum(1 for x in s if x["result"] == "win") / len(s) * 100) if s else None

    acc, dd, peak = 0.0, 0.0, 0.0
    for x in t:
        acc += x["usd"]
        peak = max(peak, acc)
        dd = max(dd, peak - acc)
    eq, run = [], 0.0
    for x in t[-40:]:
        run += x["usd"]
        eq.append(round(run, 2))
    return {"n": n, "wins": wins, "losses": losses, "be": n - closed,
            "wr": round(wins / closed * 100, 1) if closed else 0.0,
            "pnl": round(sum(x["pnl"] for x in t), 2), "usd": round(sum(x["usd"] for x in t), 2),
            "avg_r": round(sum(x["r"] for x in t) / n, 2), "pf": round(gp / gl, 2) if gl else None,
            "streak": streak, "streak_kind": kind, "best": max(t, key=lambda x: x["pnl"]),
            "worst": min(t, key=lambda x: x["pnl"]),
            "top": [c for c in reversed(rk) if c[1] > 0][:3], "bottom": [c for c in rk if c[1] < 0][:3],
            "wr_long": swr("LONG"), "wr_short": swr("SHORT"), "max_dd": round(dd, 2),
            "expectancy": round((wins * (gp / wins if wins else 0) - losses * (gl / losses if losses else 0)) / closed, 2) if closed else 0.0,
            "avg_hold": round(sum(x["hold_h"] for x in t) / n, 1), "equity": eq, "last": t[-10:][::-1]}


def render_mystats(uid):
    s = personal_stats(uid)
    if not s["n"]:
        return ("📈 <b>Моя статистика</b>\n\n<i>Пока нет закрытых сделок. Сохраните сигнал или нажмите «Вошёл в сделку» — "
                "итог запишется сам, когда сделка закроется.</i>")
    kind = {"win": "побед", "loss": "убытков", "be": "в ноль"}[s["streak_kind"]]
    dash = lambda v: "—" if v is None else v
    lines = ["📈 <b>Моя статистика</b>\n",
             f"Сделок: <b>{s['n']}</b> · ✅ {s['wins']} · ❌ {s['losses']} · ➖ {s['be']}",
             f"🎯 Винрейт: <b>{s['wr']}%</b> · PF: <b>{dash(s['pf'])}</b> · Мат. ожидание: <b>{s['expectancy']:+.2f}R</b>",
             f"💵 Итого: <b>{s['usd']:+.2f}$</b> ({s['pnl']:+.2f}%) · ср. {s['avg_r']:+.2f}R",
             f"📉 Макс. просадка: <b>{s['max_dd']:.2f}$</b>",
             f"🔥 Серия: {s['streak']} {kind} подряд",
             f"↕ Лонг {dash(s['wr_long'])}% · Шорт {dash(s['wr_short'])}%",
             f"⏱ Среднее удержание: {s['avg_hold']} ч", "",
             f"🏆 Лучшая: {s['best']['coin']} {s['best']['pnl']:+.2f}%",
             f"💀 Худшая: {s['worst']['coin']} {s['worst']['pnl']:+.2f}%"]
    if s["top"]:
        lines.append("➕ Лучшие: " + ", ".join(f"{c} {p:+.1f}%" for c, p in s["top"]))
    if s["bottom"]:
        lines.append("➖ Слабые: " + ", ".join(f"{c} {p:+.1f}%" for c, p in s["bottom"]))
    lines += ["", "<b>Последние сделки:</b>"] + [
        f"{'✅' if x['result'] == 'win' else '❌' if x['result'] == 'loss' else '➖'} {x['coin']} {x['side']}: "
        f"<code>{fmt(x['entry'])}</code> → <code>{fmt(x['exit'])}</code> · {x['pnl']:+.2f}%" for x in s["last"][:6]]
    return "\n".join(lines)


async def check_position(user_id, coin, pos):
    """Сопровождение сделки: вход лимиткой, безубыток +2,5%, TP1-3, трейлинг, предупреждения, итог."""
    _ensure_tracked_order_key(user_id, coin, pos)
    if pos.get("notified_close"):
        return
    cached = PRICES.get(coin)
    price = (float(cached.get("price", 0) or 0)
             if cached and time.time() - float(cached.get("ts", 0) or 0) <= 25 else None)
    if not price:
        live_cached = LIVE.get(coin)
        if live_cached and time.time() - float(live_cached.get("ts", 0) or 0) <= 45:
            price = float(live_cached.get("price", 0) or 0)
    if not price:
        price = await get_simple_price(coin)
    if not price:
        # Don't pretend the position is being monitored when no fresh quote is available.
        stale_since = float(pos.get("price_stale_since", 0) or 0)
        if not stale_since:
            pos["price_stale_since"] = time.time()
            set_user_open_position(user_id, coin, pos)
        elif time.time() - stale_since >= 60 and not pos.get("price_stale_warned"):
            pos["price_stale_warned"] = True
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"🚨 <b>Нет свежей цены для {coin}</b>\\n"
                                 "Сопровождение временно не может проверить стопы и цели. "
                                 "Проверьте позицию непосредственно на бирже.")
        return
    if pos.get("price_stale_since"):
        pos.pop("price_stale_since", None)
        pos.pop("price_stale_warned", None)
        set_user_open_position(user_id, coin, pos)
    side, entry = pos["side"], pos["entry_price"]
    sign = 1 if side == "LONG" else -1
    pnl = price_change_pct(side, entry, price)

    # Measure adverse selection on virtual fills after fixed observation windows.
    if not pos.get("waiting_entry") and pos.get("entered_at"):
        elapsed = time.time() - float(pos.get("entered_at") or time.time())
        if elapsed >= 60 and not pos.get("post_entry_validated"):
            validation = post_entry_validation(entry, price, pos.get("sl_orig", pos.get("sl1")), side)
            pos["post_entry_validated"] = True
            pos["post_entry_validation"] = validation
            set_user_open_position(user_id, coin, pos)
            record_trade_event(user_id, coin, pos, "post_entry_validation", str(validation.get("status")), price=price)
        if elapsed >= 300 and not pos.get("adverse_selection_assessed"):
            adverse = adverse_selection_detector(entry, price, side)
            pos["adverse_selection_assessed"] = True
            pos["adverse_selection"] = bool(adverse.get("adverse"))
            pos["post_fill_move_pct"] = adverse.get("post_fill_move_pct")
            set_user_open_position(user_id, coin, pos)
            record_trade_event(user_id, coin, pos, "adverse_selection_review", str(adverse.get("post_fill_move_pct")), price=price)
            if adverse.get("adverse") and abs(float(adverse.get("post_fill_move_pct", 0) or 0)) >= .35:
                await _tell(user_id, f"⚠️ <b>Проверка качества входа {coin}</b>\nПосле виртуального исполнения цена сместилась против позиции на {abs(float(adverse.get('post_fill_move_pct'))):.2f}%. Проверьте исполнение и актуальность сценария.")

    if pos.get("waiting_entry"):
        newer = next((x for x in get_active_signals() if x["coin"] == coin and x["side"] == side
                      and abs(x["entry"] - entry) / entry > 0.003), None)
        if newer and pos.get("warned_entry") != round(newer["entry"], 8):
            pos["warned_entry"] = round(newer["entry"], 8)
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"⚠ <b>Лимит сигнала изменился: {coin} ({side})</b>\n"
                                 f"Ваша сохранённая цена: <code>{fmt(entry)}</code>\n"
                                 f"Новая цена сигнала: <code>{fmt(newer['entry'])}</code>.\n"
                                 f"Ваша цена не менялась. Если хотите перенести лимит, измените её в Mini App и проверьте ордер на бирже.")
        if sign * (price - entry) <= 0:
            pos["waiting_entry"] = False
            pos["entered_at"] = time.time()
            notify_entry = not pos.get("entry_notified", False)
            pos["entry_notified"] = True
            set_user_open_position(user_id, coin, pos)
            record_trade_event(user_id, coin, pos, "entry_filled", "Лимитный вход исполнен", price=price)
            if notify_entry:
                await _tell(user_id, f"🎯 <b>Лимитный ордер сработал: {coin}</b>\n\nНаправление: <b>{side}</b>\n"
                                     f"Цена входа: <code>{fmt(entry)}</code>\n🚀 Сделка на сопровождении.",
                            kb_nav([[InlineKeyboardButton(text="📈 Открыть", web_app=WebAppInfo(url=webapp_link(coin)))]], back=False)
                            if WEBAPP_URL else None)
        elif time.time() - pos.get("created_at", time.time()) > 24 * 3600:
            _remember_closed_order(user_id, pos)
            record_trade_event(user_id, coin, pos, "limit_expired", "Цена не дошла до входа за 24 часа", price=price)
            set_user_open_position(user_id, coin, None)
            await _tell(user_id, f"⌛ <b>Ордер отменён: {coin}</b>\nЦена не дошла до входа за 24 часа.")
        return

    pos["peak_pnl"] = max(pos.get("peak_pnl", 0.0), pnl)
    for k in ("hit_tp1", "hit_tp2", "hit_tp3"):
        pos.setdefault(k, False)
    current_sl = pos["sl1"]
    reason, urgency, exit_price, why = None, "info", price, "закрытие"

    # Migrate positions from older builds that moved SL to entry at TP1 (before +2.5%).
    if (not pos.get("breakeven_moved") and not pos.get("hit_tp2") and
            abs(float(current_sl) - float(entry)) <= max(1e-12, abs(float(entry)) * 1e-9)):
        original_sl = float(pos.get("sl_orig", current_sl))
        if (side == "LONG" and original_sl < entry) or (side == "SHORT" and original_sl > entry):
            if not breakeven_trigger_reached(pnl, 2.5):
                pos["sl1"] = current_sl = original_sl
                set_user_open_position(user_id, coin, pos)
            else:
                pos["breakeven_moved"] = True
                set_user_open_position(user_id, coin, pos)

    if breakeven_trigger_reached(pnl, 2.5) and not pos.get("breakeven_moved"):
        if (side == "LONG" and current_sl < entry) or (side == "SHORT" and current_sl > entry):
            pos["sl1"] = entry
            current_sl = entry
            pos["breakeven_moved"] = True
            set_user_open_position(user_id, coin, pos)
            record_trade_event(user_id, coin, pos, "breakeven", "Стоп перенесён на цену входа", price=price)
            await _tell(user_id, f"🛡 <b>Защита безубытка: {coin}</b>\nПрибыль достигла +2,5% ({pnl:+.2f}%). Стоп перенесён на цену входа.")

    if pos["hit_tp1"] and (pos.get("breakeven_moved") or pos.get("hit_tp2")) and not pos["hit_tp3"]:
        ch = await chandelier_stop(coin, side == "LONG")
        if ch:
            new_sl = max(ch, entry) if side == "LONG" else min(ch, entry)
            better = new_sl > current_sl if side == "LONG" else new_sl < current_sl
            if better:
                pos["sl1"] = current_sl = new_sl
                set_user_open_position(user_id, coin, pos)
                record_trade_event(user_id, coin, pos, "trailing_stop", "Трейлинг-стоп подтянут", price=price)

    if pnl <= -5.0 and not pos.get("notified_urgent"):
        pos["notified_urgent"] = True
        set_user_open_position(user_id, coin, pos)
        await _tell(user_id, f"🚨 <b>{coin} идёт против вас:</b> {pnl:+.2f}%. Будьте готовы к стопу.")

    # Ранние предупреждения по расходованию исходного риска: 50/75/90% пути к стопу.
    stop_ref = float(pos.get("sl1") or pos.get("sl_orig") or entry)
    stop_distance_pct = abs(entry - stop_ref) / max(abs(entry), 1e-12) * 100
    adverse_pct = max(0.0, -pnl)
    if stop_distance_pct > 0:
        risk_used = adverse_pct / stop_distance_pct * 100
        for threshold in (50, 75, 90):
            flag = f"risk_warn_{threshold}"
            if risk_used >= threshold and not pos.get(flag):
                pos[flag] = True
                set_user_open_position(user_id, coin, pos)
                icon = "🚨" if threshold >= 90 else "⚠️"
                await _tell(user_id, f"{icon} <b>{coin}: использовано {threshold}% расстояния до стопа</b>\\n"
                                     f"Текущий PnL: <b>{pnl:+.2f}%</b> · цена <code>{fmt(price)}</code>\\n"
                                     "Проверьте реальную позицию и защитные ордера на бирже.")
    else:
        risk_used = 0.0

    # Отдельно отслеживаем ухудшение/разворот вероятностей на живых таймфреймах.
    live_state = LIVE.get(coin) or {}
    probs = live_state.get("long_prob") or {}
    if probs and time.time() - float(live_state.get("ts", 0) or 0) <= 90:
        directional = {tf: (float(v) if side == "LONG" else 100.0 - float(v))
                       for tf, v in probs.items() if tf in ("15m", "1h", "4h")}
        adverse_tfs = [tf for tf, value in directional.items() if value <= 40]
        reversal = len(adverse_tfs) >= 2 and ("1h" in adverse_tfs or "4h" in adverse_tfs)
        if reversal and not pos.get("reversal_warned"):
            pos["reversal_warned"] = True
            pos["reversal_warned_at"] = time.time()
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"🚨 <b>Возможный разворот против {coin} ({side})</b>\\n"
                                 f"Слабые таймфреймы: {', '.join(adverse_tfs)}. "
                                 f"PnL: <b>{pnl:+.2f}%</b>.\\n"
                                 "Сигнал требует повторной оценки. Проверьте стоп и реальную позицию на бирже.")
        elif not reversal and pos.get("reversal_warned"):
            pos["reversal_warned"] = False
            set_user_open_position(user_id, coin, pos)
        deterioration = len(adverse_tfs) >= 1
        if deterioration and not pos.get("trend_deterioration_warned"):
            pos["trend_deterioration_warned"] = True
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"⚠️ <b>Сигнал {coin} ослабевает</b>\\n"
                                 f"Направленная вероятность ухудшилась на: {', '.join(adverse_tfs)}. "
                                 "Проверьте ситуацию.")
        elif not deterioration and pos.get("trend_deterioration_warned"):
            pos["trend_deterioration_warned"] = False
            set_user_open_position(user_id, coin, pos)
        directional_values = list(directional.values())
        momentum_score = float(np.mean(directional_values)) if directional_values else None
        exit_eval = adaptive_exit_assessment(pos, price, momentum_score, structure_valid=not reversal)
        recovery_eval = trade_recovery_assessment(pos, price, structure_valid=not reversal, side_prob=momentum_score)
        pos["adaptive_exit_state"] = exit_eval.get("action")
        pos["recovery_state"] = recovery_eval.get("status")
        if exit_eval.get("action") == "review_full_exit" and not pos.get("critical_exit_warned"):
            pos["critical_exit_warned"] = True
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"🔴 <b>Критический риск разворота: {coin} ({side})</b>\nПервоначальная структура выглядит разрушенной. Немедленно переоцените риск и рассмотрите закрытие позиции согласно своему торговому плану. Бот не закрывает реальную биржевую позицию.")
        elif exit_eval.get("action") == "consider_partial_profit" and not pos.get("partial_profit_warned"):
            pos["partial_profit_warned"] = True
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"🟠 <b>Ослабление импульса: {coin}</b>\nПозиция в прибыли, импульс ослабевает. Рассмотрите частичную фиксацию согласно своему плану.")
        elif exit_eval.get("action") == "hold_to_plan" and (pos.get("critical_exit_warned") or pos.get("partial_profit_warned")):
            pos["critical_exit_warned"] = False
            pos["partial_profit_warned"] = False
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"🟢 <b>Риск по {coin} снизился</b>\nНаправленные таймфреймы восстановились. Предыдущая оценка риска ослаблена; продолжайте следовать торговому плану.")

    tp1, tp2, tp3 = pos["tp1"], pos["tp2"], pos["tp3"]
    if sign * (price - current_sl) <= 0:
        reason = ("🛑 Сработал трейлинг-стоп, часть прибыли зафиксирована." if current_sl != pos.get("sl_orig") and pos.get("hit_tp1")
                  else "🛑 Сработал стоп-лосс.")
        exit_price, urgency = current_sl, "success" if pos.get("hit_tp1") else "danger"
        pos["notified_close"] = True
        why = "стоп / трейлинг"
    elif sign * (price - tp3) >= 0 and not pos["hit_tp3"]:
        reason, urgency = "🏆 Достигнут TP3. Сделка полностью закрыта.", "success"
        pos["hit_tp3"] = True
        record_trade_event(user_id, coin, pos, "tp3", "Достигнута третья цель", price=price)
        exit_price, pos["notified_close"], why = tp3, True, "TP3"
    elif sign * (price - tp2) >= 0 and not pos["hit_tp2"]:
        reason, urgency = "✅ Достигнут TP2. Стоп подтянут на TP1.", "success"
        pos["hit_tp2"] = True
        pos["sl1"] = tp1
        set_user_open_position(user_id, coin, pos)
        record_trade_event(user_id, coin, pos, "tp2", "Достигнута вторая цель; стоп подтянут к TP1", price=price)
    elif sign * (price - tp1) >= 0 and not pos["hit_tp1"]:
        reason, urgency = "✅ Достигнут TP1. Первая цель зафиксирована; стоп переносится в безубыток только после +2,5%.", "success"
        pos["hit_tp1"] = True
        set_user_open_position(user_id, coin, pos)
        record_trade_event(user_id, coin, pos, "tp1", "Достигнута первая цель", price=price)

    if not reason and not pos.get("notified_close") and time.time() - pos.get("last_warn", 0) > 6 * 3600:
        live = LIVE.get(coin)
        if (live and time.time() - live.get("ts", 0) < 300 and pnl <= -1.5 and
                close_warning_confirmed(live.get("long_prob", {}), side)):
            pos["last_warn"] = time.time()
            pos["last_warn_signature"] = "4tf_adverse"
            set_user_open_position(user_id, coin, pos)
            await _tell(user_id, f"⚠️ <b>Повышенный риск: {coin} ({side})</b>\n\n"
                                 "Все четыре таймфрейма (15m, 1h, 4h и 1d) подтверждают движение против позиции. "
                                 "Это предупреждение для проверки риска, а не рекомендация автоматически закрывать сделку.\n"
                                 f"Цена: <code>{fmt(price)}</code> · PnL <b>{pnl:+.2f}%</b>")

    if reason:
        text = (f"{'🏆' if urgency == 'success' else '🛑'} <b>Сопровождение: {coin} ({side})</b>\n"
                f"Вход: <code>{fmt(entry)}</code> | Сейчас: <code>{fmt(price)}</code> | PnL <b>{pnl:+.2f}%</b>\n\n{reason}")
        kb = None
        if pos.get("notified_close"):
            rec = record_trade_result(user_id, coin, pos, exit_price, why)
            set_user_open_position(user_id, coin, None)
            if rec:
                text += "\n\n" + render_trade_card(rec)
                share = f"Я закрыл {coin} ({side}) с результатом {rec['pnl']:+.2f}% в IXCLY PRO! 🔥"
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📈 Моя статистика", callback_data="mystats")],
                    [InlineKeyboardButton(text="📤 Поделиться", url=f"https://t.me/share/url?text={quote(share)}")]])
        else:
            set_user_open_position(user_id, coin, pos)
        await _tell(user_id, text, kb)
    else:
        set_user_open_position(user_id, coin, pos)


async def position_monitor_loop():
    await asyncio.sleep(50)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                # Уже открытая пользователем позиция сопровождается до закрытия,
                # даже если Premium/VIP истёк после входа.
                try:
                    uid = int(uid_str)
                except (TypeError, ValueError):
                    continue
                for coin, pos in list((u.get("open_positions") or {}).items()):
                    try:
                        await check_position(uid, coin, pos)
                    except Exception as e:
                        log.info(f"check_position {coin}: {e}")
                    await asyncio.sleep(0.05)
        except Exception as e:
            log.error(f"pos monitor: {e}")
        next_timers["positions"] = time.time() + 10
        await asyncio.sleep(10)


async def _tell(uid, text, kb=None):
    try:
        await _send_new(uid, trim_html(text, 4000), kb or kb_nav(back=False), None)
    except Exception as e:
        log.info(f"notify {uid}: {e}")


async def daily_digest_loop():
    """Ежедневная сводка в 21:00 МСК: итоги дня по каждому пользователю с закрытыми сделками."""
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=21, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        day_start = (datetime.now(timezone(timedelta(hours=3))).replace(hour=0, minute=0, second=0, microsecond=0)).timestamp()
        for uid_str, u in list(_users().items()):
            if not isinstance(u, dict) or not get_user_notif_settings(uid_str)["pos_updates"]:
                continue
            today = [x for x in u.get("trade_log", []) if x["closed_at"] >= day_start]
            if not today:
                continue
            usd = sum(x["usd"] for x in today)
            wins = sum(1 for x in today if x["result"] == "win")
            await _tell(int(uid_str), f"🌙 <b>Итоги дня</b>\nСделок: {len(today)} · ✅ {wins} · ❌ {len(today) - wins}\n"
                                      f"Результат: <b>{usd:+.2f}$</b>")
        await asyncio.sleep(60)


# =====================================================================
# НОВОСТИ ПРОТИВ ПОЗИЦИЙ, АЛЕРТЫ, БРИФИНГ
# =====================================================================
_news_alerted = set()


async def check_news_conflicts_with_signals():
    nb, link = market_bias.get("bias", 0), market_bias.get("link")
    # A broad/medium headline is not enough to suggest an exit; require a critical, fresh, coin-specific event.
    if (nb == 0 or not link or market_bias.get("severity") != "critical" or
            int(market_bias.get("critical_score", 0) or 0) < 7 or time.time() - market_bias.get("ts", 0) > 900):
        return
    for uid_str, u in list(_users().items()):
        if not isinstance(u, dict) or not get_user_notif_settings(uid_str)["signals"]:
            continue
        for coin, pos in (u.get("open_positions") or {}).items():
            if pos.get("waiting_entry") or pos.get("notified_close"):
                continue
            against = (nb == 1 and pos["side"] == "SHORT") or (nb == -1 and pos["side"] == "LONG")
            if not against or (link, uid_str, coin) in _news_alerted:
                continue
            _news_alerted.add((link, uid_str, coin))
            if re.search(rf"\b{re.escape(coin)}\b", f"{market_bias.get('title_ru', '')} {market_bias.get('coins', '')}".upper()):
                await _tell(int(uid_str), f"🚨 <b>Критическая новость по {coin}</b>\n\n"
                                          f"📰 {html.escape(shorten(market_bias.get('title_ru', ''), 120))}\n"
                                          "Новость имеет противоположный рыночный фон и прямо относится к монете. "
                                          "Проверьте источник и уровни риска; это не команда и не рекомендация закрывать позицию.")
    if len(_news_alerted) > 2000:
        _news_alerted.clear()


async def news_watcher_loop():
    global last_news_link
    await asyncio.sleep(40)
    while True:
        try:
            if not bot_flags.get("news_enabled", True):
                next_timers["news_watcher"] = time.time() + 600
                await asyncio.sleep(600)
                continue
            entries = await get_news_entries()
            if entries:
                if last_news_link is None:
                    last_news_link = entries[0].link
                elif entries[0].link != last_news_link:
                    fresh = []
                    for e in entries:
                        if getattr(e, "link", None) == last_news_link:
                            break
                        fresh.append(e)
                    last_news_link = entries[0].link
                    state_touch(False)
                    await update_market_bias(force=True)
                    await check_news_conflicts_with_signals()
                    scored = []
                    for e in fresh[:5]:
                        title = html.unescape(getattr(e, "title", "") or "")
                        summary = clean_summary(getattr(e, "summary", "") or "")
                        result = classify_critical(title, summary)
                        scored.append((result, e, title))
                        log.info("crit %s: %s (%s)", result[0], title[:180], result[1])
                    if scored:
                        (score, _), event, title = max(scored, key=lambda item: item[0][0])
                        if score >= 3:
                            move = await btc_move_15m()
                            # Пауза только при подтверждённой реакции рынка: сильная новость
                            # без движения цены снижает уверенность отдельных активов, но
                            # не выключает весь сканер.
                            confirmed_panic = (score >= 7 and abs(move) >= 1.8) or (score >= 9 and abs(move) >= 1.2)
                            await notify_admin(
                                f"📰 Новостной риск (балл {score}); BTC за 15 минут: {move:+.2f}%. "
                                f"Глобальная пауза: {'да, 20 минут' if confirmed_panic else 'нет, сигналы продолжают анализироваться'}.\n"
                                f"{html.escape(title)}")
                            if confirmed_panic:
                                set_blackout(1200, "news")
                                if alert_once("crit_news", 3 * 3600):
                                    title_ru = market_bias["title_ru"] if market_bias.get("link") == getattr(event, "link", None) else await translate_text(title)
                                    asyncio.create_task(broadcast_market_alert(
                                        title="🚨 Подтверждённый рыночный шок",
                                        reason=f"<i>{html.escape(shorten(title_ru, 150))}</i>\nBTC за 15 минут: {move:+.2f}%",
                                        advice="Риск временно повышен. Дождитесь стабилизации; новостной заголовок сам по себе не считается сигналом к продаже.",
                                        pause_mins=20))
                    # Editorial news are published by the standalone IXCLY NEWS service.
                    # Never send the legacy news feed to private chats (premium or free).
        except Exception as e:
            log.warning(f"news_watcher: {e}", exc_info=True)
        next_timers["news_watcher"] = time.time() + 600
        await asyncio.sleep(600)
async def broadcast_market_alert(title, reason, advice, pause_mins=60):
    """Умная рассылка о волатильности/паузе для обычных пользователей."""
    text = (f"<b>{title}</b>\n\n"
            f"📝 <b>Что произошло:</b>\n{reason}\n\n"
            f"💡 <b>Что делать:</b>\n{advice}\n\n"
            f"⏳ <i>Сканер торговых сигналов приостановлен на {pause_mins} мин. для вашей безопасности.</i>")
    
    # Кнопка, чтобы сразу открыть приложение
    kb = kb_nav([[InlineKeyboardButton(text="📈 Открыть приложение", web_app=WebAppInfo(url=webapp_link()))]], back=False) if WEBAPP_URL else None
    
    # Отправляем только тем, у кого включены сигналы (или новости)
    targets = [uid for uid in active_chats if get_user_notif_settings(uid)["signals"] and has_access(uid)]
    
    sent = 0
    for cid in targets:
        try:
            await _send_new(cid, text, kb, None)
            sent += 1
            await asyncio.sleep(0.05) # Защита от лимитов Telegram
        except Exception as e:
            pass
            
    log.info(f"Оповещение о волатильности отправлено {sent} пользователям.")


async def price_alerts_loop():
    await asyncio.sleep(60)
    while True:
        try:
            for uid_str, u in list(_users().items()):
                alerts = u.get("price_alerts", []) if isinstance(u, dict) else []
                if not alerts:
                    continue
                triggered = []
                for alert in alerts:
                    price = await get_simple_price(alert["coin"])
                    if not price:
                        continue
                    if (alert["op"] == ">" and price >= alert["price"]) or (alert["op"] == "<" and price <= alert["price"]):
                        triggered.append(alert)
                        await _tell(int(uid_str), f"🔔 <b>Алерт сработал: {alert['coin']}</b>\n"
                                                  f"Условие: <code>{alert['op']} {fmt(alert['price'])}</code>\n"
                                                  f"Текущая цена: <code>{fmt(price)}</code>",
                                    kb_nav([[InlineKeyboardButton(text="📈 Открыть", web_app=WebAppInfo(url=webapp_link(alert['coin'])))]], back=False)
                                    if WEBAPP_URL else None)
                if triggered:
                    with STATE_LOCK:
                        u["price_alerts"] = [a for a in alerts if a not in triggered]
                    state_touch()
        except Exception as e:
            log.error(f"price_alerts_loop: {e}")
        await asyncio.sleep(120)


async def morning_briefing_loop():
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=8, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        try:
            ctx = await get_context(force=True)
            fng = ctx.get("fng")
            text = (f"☀️ <b>Доброе утро!</b>\n\n🌍 <b>BTC:</b> {ctx.get('btc_txt')}\n"
                    f"😨 <b>F&G:</b> {fng if fng is not None else '—'} {ctx.get('fng_txt', '')}\n"
                    f"🔥 <b>Активных сигналов:</b> {len(get_active_signals())}\n\n<i>Удачной торговли!</i>" + ad_block())
            for cid in list(active_chats):
                if get_user_notif_settings(cid)["briefing"]:
                    try:
                        await _send_new(cid, text, kb_nav([[InlineKeyboardButton(text="📈 Открыть", web_app=WebAppInfo(url=webapp_link(tab="signals")))]], back=False)
                                        if WEBAPP_URL else None, None)
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"Ошибка утреннего брифинга: {e}")
        await asyncio.sleep(60)


# =====================================================================
# ЛИКВИДНОСТЬ СИГНАЛОВ ПРИ ОБНОВЛЕНИИ И ПОЛНОЕ ОБНОВЛЕНИЕ ДАННЫХ
# =====================================================================
refresh_job = {"running": False, "done": 0, "total": 0, "removed": 0, "ts": 0.0}


async def drop_waiting_signals(coin, res):
    """Reconcile published unfilled signals against a fresh analysis; stale cards never stay active."""
    if res.get("price_only") or res.get("stale"):
        return 0
    n = 0
    hist = load_signal_history()
    fresh_side = str(res.get("side") or ("LONG" if res.get("is_long") else "SHORT")).upper()
    fresh_verdict = str(res.get("verdict") or "WAIT").upper()
    try:
        current_price = float(res.get("price") or 0)
    except (TypeError, ValueError):
        current_price = 0.0
    for x in hist:
        if x.get("coin") != coin:
            continue
        status = str(x.get("status") or "").lower()
        if status not in ("active", "in_trade"):
            continue
        # Never cancel a genuinely opened position. "in_trade" is invalidated
        # only while its linked position is still waiting for a limit entry.
        if status == "in_trade":
            waiting = any(
                str((u.get("open_positions") or {}).get(coin, {}).get("side") or "").upper()
                == str(x.get("side") or "").upper()
                and bool((u.get("open_positions") or {}).get(coin, {}).get("waiting_entry"))
                for u in _users().values() if isinstance(u, dict)
            )
            if not waiting:
                continue
        reason = None
        if fresh_verdict != "ENTER":
            reason = f"Новый анализ: рекомендация «ЖДАТЬ». {shorten(res.get('comment', ''), 160)}"
        elif fresh_side in ("LONG", "SHORT") and fresh_side != x.get("side"):
            reason = f"Новое направление анализа изменилось на {fresh_side}; прежний {x.get('side')} больше не подтверждён."
        elif current_price > 0 and float(x.get("entry") or 0) > 0:
            drift = abs(current_price - float(x["entry"])) / current_price
            if drift > 0.03:
                reason = f"Цена ушла от опубликованного входа на {drift * 100:.2f}% — вход потерян, догонять движение нельзя."
        if reason:
            await invalidate_signal(x, reason)
            n += 1
    if n:
        save_signal_history(hist)
    return n


async def refresh_all_data(uid):
    """Пересчёт всех монет пользователя (сигналы, избранное, позиции) с нуля."""
    coins = [x["coin"] for x in get_active_signals()] + [s["coin"] for s in get_user_saved_signals(uid)] \
        + list(get_user_open_positions(uid))
    coins = list(dict.fromkeys(coins))[:40]
    before = len(get_active_signals())
    refresh_job.update(running=True, done=0, total=len(coins), removed=0)
    analysis_cache.clear()
    _funding_cache.clear()
    _ai_cache.clear()
    _ctx_cache["ts"] = 0
    _defillama_cache["ts"] = 0
    _news_cache["ts"] = 0
    for c in coins:
        bt_cache.pop(c, None)
    sem = asyncio.Semaphore(3)

    async def one(c):
        async with sem:
            try:
                res, _ = await asyncio.wait_for(analyze_coin(c, uid=uid, force=True), 120)
                if res and not res.get("price_only"):
                    remember_res(uid, c, res)
                    await drop_waiting_signals(c, res)
                await live_refresh_coin(c)
            except Exception as e:
                log.info(f"refresh {c}: {e}")
            refresh_job["done"] += 1

    try:
        await get_context(force=True)
        await update_market_bias()
        await asyncio.gather(*[one(c) for c in coins])
    finally:
        refresh_job.update(running=False, ts=time.time(), removed=max(0, before - len(get_active_signals())))


# =====================================================================
# АНОМАЛЬНЫЙ ОБЪЁМ
# =====================================================================
async def anomaly_scan_once():
    j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=12)
    vol = {x["symbol"]: float(x["quoteVolume"]) for x in j if x["symbol"].endswith("USDT")}
    coins = [c for c in POPULAR_COINS[:150] if vol.get(f"{c}USDT", 0) >= 10_000_000]
    found = []
    sem = asyncio.Semaphore(4)

    async def check(coin):
        async with sem:
            try:
                k = await http_json("https://data-api.binance.vision/api/v3/klines",
                                    params={"symbol": f"{coin}USDT", "interval": "1m", "limit": 22}, timeout=6)
            except Exception:
                return
        if len(k) < 22:
            return
        closed = k[:-1]
        last, base = closed[-1], closed[:-1]
        avg = float(np.mean([float(x[7]) for x in base]))
        qv = float(last[7])
        if avg > 0 and qv / avg >= 10 and qv >= 250_000:
            found.append((coin, qv, qv / avg, (float(last[4]) / float(last[1]) - 1) * 100))

    await asyncio.gather(*[check(c) for c in coins])
    now = time.time()
    fresh = sorted([a for a in found if now - anomaly_sent_at.get(a[0], 0) > 2 * 3600], key=lambda a: a[2], reverse=True)[:3]
    for coin, qv, ratio, chg in fresh:
        anomaly_sent_at[coin] = now
        state_touch(False)
        text = (f"🚨 <b>Аномальный объём: {coin}</b>\nОборот за минуту: <b>${qv:,.0f}</b> (×{ratio:.0f})\n"
                f"Цена за минуту: <b>{chg:+.2f}%</b>")
        for cid in list(active_chats):
            if get_user_notif_settings(cid)["anomalies"]:
                await _tell(cid, text, webapp_kb(coin))


async def liquidation_stream_loop():
    """Поток Binance force orders: объединяет ликвидации в минутные кластеры."""
    await asyncio.sleep(8)
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    while True:
        try:
            async with http.ws_connect(url, heartbeat=25, receive_timeout=45, max_msg_size=2**20) as ws:
                log.info("Liquidation stream connected")
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    try:
                        raw = json.loads(msg.data)
                        orders = raw if isinstance(raw, list) else [raw]
                        for ev in orders:
                            o = ev.get("o", {})
                            symbol = str(o.get("s", ""))
                            if not symbol.endswith("USDT"):
                                continue
                            coin = symbol[:-4]
                            qty, price = float(o.get("q", 0)), float(o.get("p", 0))
                            notional = qty * price
                            if notional < 10000:
                                continue
                            side = str(o.get("S", "")).upper()
                            item = {"coin": coin, "side": "LONG" if side == "SELL" else "SHORT",
                                    "notional": round(notional, 2), "price": price,
                                    "ts": float(o.get("T", time.time() * 1000)) / 1000}
                            LIQUIDATIONS.append(item)
                            if len(LIQUIDATIONS) > 250:
                                del LIQUIDATIONS[:-250]
                            # Сигналим только по крупному событию и не чаще 5 минут на тикер/сторону.
                            key = (coin, item["side"])
                            if notional >= 250000 and time.time() - _liq_last_alert.get(key, 0) >= 300:
                                _liq_last_alert[key] = time.time()
                                label = "лонг-позиции" if item["side"] == "LONG" else "шорт-позиции"
                                message = (f"💥 <b>Крупная ликвидация {coin}</b>\n"
                                           f"Ликвидированы {label} примерно на <b>${notional:,.0f}</b>.\n"
                                           f"Цена события: <code>{fmt(price)}</code>. Это индикатор давления, не торговый сигнал.")
                                for uid_text, user_data in list(_users().items()):
                                    try:
                                        uid = int(uid_text)
                                        if (isinstance(user_data, dict) and user_data.get("active", True)
                                                and has_access(uid) and get_user_notif_settings(uid)["liquidations"]):
                                            await _tell(uid, message, webapp_kb(coin))
                                    except (TypeError, ValueError):
                                        continue
                    except (ValueError, TypeError, KeyError) as e:
                        log.debug("bad liquidation event: %s", e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Liquidation stream reconnect: %s", e)
        await asyncio.sleep(5)


async def anomaly_scanner_loop():
    await asyncio.sleep(90)
    while True:
        try:
            if bot_flags["alerts_enabled"] and active_chats:
                await anomaly_scan_once()
        except Exception as e:
            log.info(f"anomaly: {e}")
        next_timers["anomaly"] = time.time() + 180
        await asyncio.sleep(180)


# =====================================================================
# MINI APP: API
# =====================================================================
def tg_auth(init):
    """Проверка подписи initData от Telegram (HMAC-SHA256)."""
    try:
        pairs = dict(parse_qsl(init, keep_blank_values=True))
        got = pairs.pop("hash")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(hmac.new(secret, check.encode(), hashlib.sha256).hexdigest(), got):
            return None
        if time.time() - int(pairs.get("auth_date", 0)) > 86400:
            return None
        return json.loads(pairs["user"])
    except Exception:
        return None


@app.route("/app")
def mini_page():
    # Mini App is updated on deploy; prevent Telegram's embedded browser from reusing stale HTML.
    response = send_file(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp.html"),
        conditional=False, max_age=0,
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


API_CACHE_TTL = {"me": 4, "signals": 4, "portfolio": 4, "stats": 5, "mystats": 5}
API_CACHE = {}


# --- API ДЛЯ ОПЛАТЫ В WEBAPP ---


_INVOICE_RL = {}  # uid -> ts последнего запроса счёта (антиспам)


@app.route("/api/create_stars_invoice", methods=["POST"])
def api_create_stars_invoice():
    d = request.get_json(silent=True) or {}
    u = tg_auth(d.get("initData", ""))
    if not u:
        return Response('{"error":"Откройте из Telegram"}', 401, mimetype="application/json")
    try:
        uid = int(u["id"])
        days = int(d.get("days", 0))
        expected = d.get("expected_stars")
        expected = int(expected) if expected is not None else None
    except (TypeError, ValueError, KeyError):
        return jsonify({"error": "Неверные данные тарифа"})
    if days not in PLANS:
        return jsonify({"error": "Неверный тариф. Обновите приложение и выберите тариф заново."})
    now = time.time()
    if now - _INVOICE_RL.get(uid, 0) < 1.5:
        return jsonify({"error": "Слишком часто. Подождите секунду и повторите."})
    _INVOICE_RL[uid] = now
    if len(_INVOICE_RL) > 2000:
        _INVOICE_RL.clear()

    async def _create():
        a = access_payload(uid)
        if not a["can_buy"]:
            return {"error": billing.ERROR_TEXT["lifetime"]}
        price = price_for_user(uid, days)
        # Цена в окне подтверждения должна совпадать с серверной (скидка могла закончиться)
        if expected is not None and expected != price:
            return {"error": "Цена тарифа изменилась. Проверьте новую стоимость и подтвердите оплату ещё раз.",
                    "price_changed": True, "stars": price}
        with STATE_LOCK:
            order = billing.create_order(STATE, uid, days, price, PLANS, source="app")
        state_touch()
        title = PLAN_TITLE[days]
        link = await bot.create_invoice_link(
            title=f"IXCLY PRO · {title}",
            description=f"Доступ к сигналам и аналитике на {days} дн.",
            payload=billing.payload_for(order),   # пользователь/тариф/сумма — в серверном заказе
            currency="XTR",
            prices=[LabeledPrice(label=title, amount=price)],
            provider_token="")
        return {"link": link, "order_id": order["id"], "days": days, "stars": price, "title": title,
                "new_until": billing.new_expiry(_users().get(str(uid), {}).get("access"), days)}

    try:
        result = asyncio.run_coroutine_threadsafe(_create(), MAIN_LOOP).result(15)
        return jsonify(result)
    except Exception as e:
        log.error(f"Ошибка создания счета uid={uid}: {e}")
        return jsonify({"error": "Не удалось создать счёт. Деньги не списаны, попробуйте ещё раз."})


@app.route("/api/<act>", methods=["POST"])
def mini_api(act):
    d = request.get_json(silent=True) or {}
    u = tg_auth(d.get("initData", ""))
    if not u:
        return Response('{"error":"Откройте приложение из Telegram"}', 401, mimetype="application/json")
    
    key = (u["id"], act, json.dumps({k: v for k, v in d.items() if k != "initData"}, sort_keys=True, default=str))
    
    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
    # Если это админ, ставим ttl = 0 (кэш выключен). Иначе берем стандартный TTL.
    ttl = 0 if u["id"] in ADMIN_IDS else API_CACHE_TTL.get(act)
    
    if ttl and key in API_CACHE and time.time() - API_CACHE[key][0] < ttl:
        return Response(API_CACHE[key][1], mimetype="application/json")
    
    try:
        out = asyncio.run_coroutine_threadsafe(mini_dispatch(u["id"], u, act, d), MAIN_LOOP).result(120)
    except (ValueError, TypeError):
        out = {"error": "Проверьте введённые данные"}
    except Exception as e:
        log.warning(f"mini_api {act}: {e}")
        out = {"error": "Ошибка сервера, попробуйте ещё раз"}
        
    body = json.dumps(out, ensure_ascii=False, default=str)
    
    # Сохраняем в кэш только если ttl > 0 (то есть для обычных юзеров)
    if ttl and not (isinstance(out, dict) and out.get("error")):
        API_CACHE[key] = (time.time(), body)
        if len(API_CACHE) > 500:
            API_CACHE.clear()
            
    return Response(body, mimetype="application/json")


def _sig_item(x, live=True):
    item = {k: x.get(k) for k in ("coin", "side", "type", "strength", "entry", "tp1", "tp2", "tp3", "sl1", "status", "ts",
                                  "max_tp_hit", "cancel_reason", "live_price", "quality_score", "quality_label", "market_regime", "volume_anomaly")}
    item["sent"] = len(x.get("sent_to", []))
    if live and x.get("coin") in LIVE:
        lv = LIVE[x["coin"]]
        item["live_price"] = lv["price"]
        item["live_age"] = int(time.time() - lv["ts"])
        item["live_long_prob"] = lv["long_prob"]
    dist = None
    if item.get("live_price") and item.get("entry"):
        dist = round(price_change_pct(item.get("side"), item["entry"], item["live_price"]), 2)
    item["dist_pct"] = dist
    return item


def _enrich_user_positions(uid):
    out = {}
    for c, p in get_user_open_positions(uid).items():
        lv = LIVE.get(c)
        price = (lv["price"] if lv else None) or p["entry_price"]
        sign = 1 if p["side"] == "LONG" else -1
        out[c] = dict(p, price=price, pnl=round(price_change_pct(p["side"], p["entry_price"], price), 2))
    return out


def _correlation_from_closes(coin, symbols, closes):
    returns = {c: pd.Series(pd.Series(v).pct_change().dropna().tail(48).to_numpy()) for c, v in closes.items()}
    out = {}
    base = returns.get(coin)
    if base is not None:
        for c in symbols:
            if c == coin or c not in returns:
                continue
            n = min(len(base), len(returns[c]))
            if n >= 20:
                a, b = base.iloc[-n:], returns[c].iloc[-n:]
                corr = float(a.corr(b))
                if np.isfinite(corr):
                    out[c] = round(corr, 2)
    return out


async def correlation_snapshot(coin, peers=None):
    """Оценка 1h корреляции доходностей к BTC и ближайшей тематической монете."""
    peers = list(peers or [])
    group = next((g for g in CORRELATION_GROUPS if coin in g), [])
    peer = next((x for x in group if x != coin), "BTC")
    symbols = list(dict.fromkeys([coin, peer, "BTC"]))[:3]
    try:
        frames = await asyncio.gather(*[get_market_data_light(c, ("1h",)) for c in symbols])
        closes = {}
        for c, item in zip(symbols, frames):
            df = item[0].get("1h", {}).get("df") if item and item[0] else None
            if df is not None and len(df) >= 30:
                closes[c] = df["close"].astype(float).to_numpy().copy()  # копия: поток не держит ссылку на общий df
        # pct_change/corr — синхронная pandas-математика, выносим из event loop
        return await asyncio.to_thread(_correlation_from_closes, coin, symbols, closes)
    except Exception as e:
        log.debug("correlation %s: %s", coin, e)
        return {}


async def coin_payload(uid, coin, force=False):
    if ANALYZE_SEM is not None:
        async with ANALYZE_SEM:
            res, err = await analyze_coin(coin, uid=uid, force=force)
    else:
        res, err = await analyze_coin(coin, uid=uid, force=force)
    if err:
        return {"error": strip_tags(err)[:300]}

    # One source of truth: a fresh WAIT/opposite-side/overextended analysis
    # invalidates an unfilled active signal before the coin card is rendered.
    if not res.get("price_only") and not res.get("stale"):
        removed = await drop_waiting_signals(coin, res)
        if removed:
            # Do not let a cached detail response continue to advertise a signal
            # that was just invalidated by this very analysis.
            res = dict(res)
            res["signal_active"] = False
            res["signal_consistent"] = False
            res["verdict"] = "WAIT"
            res["comment"] = "Сигнал снят: свежий анализ больше не подтверждает вход."

    remember_res(uid, coin, res)
    if res.get("price_only"):
        return {"price_only": True, "price": res["price"], "coin": coin}
    out = {k: v for k, v in res.items() if k not in ("img", "text", "reasons_raw")}
    out["img"] = ("data:image/png;base64," + base64.b64encode(res["img"]).decode()) if res.get("img") else None
    s = get_user_risk_settings(uid)
    out["risk_usdt"] = s["deposit"] * s["risk_pct"] / 100
    out["pos_usdt"] = out["risk_usdt"] / res["risk_dist"] * res["entry"] if res["risk_dist"] > 0 else 0.0
    out["in_fav"] = any(x["coin"] == coin for x in get_user_saved_signals(uid))
    out["position"] = bool(get_open_position(uid, coin))
    out["live"] = LIVE.get(coin)
    pos_now = get_open_position(uid, coin)
    out["waiting_entry"] = bool(pos_now and pos_now.get("waiting_entry"))
    if pos_now:
        out["entry"] = float(pos_now.get("entry_price", out["entry"]))
        out["tps"] = [pos_now.get("tp1"), pos_now.get("tp2"), pos_now.get("tp3")]
        out["sls"] = [pos_now.get("sl1"), pos_now.get("sl2")]
        out["side"] = pos_now.get("side", out["side"])
        effective_risk = abs(float(out["entry"]) - float(pos_now.get("sl1") or out["entry"])) / float(out["entry"])
        out["risk_dist"] = effective_risk
        out["pos_usdt"] = out["risk_usdt"] / effective_risk * float(out["entry"]) if effective_risk > 0 else 0.0
    sig_now = next((x for x in get_active_signals() if x["coin"] == coin), None)
    # The detail card must describe the same signal record as the signals list.
    # A fresh analysis may enrich diagnostics, but cannot silently replace the
    # side, confidence or entry levels of an already-published signal.
    if sig_now and not pos_now:
        out["side"] = sig_now.get("side", out.get("side"))
        out["is_long"] = out["side"] == "LONG"
        out["verdict"] = "ENTER" if sig_now.get("status") == "active" else "WAIT"
        out["strength"] = sig_now.get("strength", out.get("strength"))
        out["type"] = sig_now.get("type", out.get("type"))
        out["entry"] = sig_now.get("entry", out.get("entry"))
        out["tps"] = [sig_now.get("tp1"), sig_now.get("tp2"), sig_now.get("tp3")]
        out["sls"] = [sig_now.get("sl1"), sig_now.get("sl2")]
        out["signal_ts"] = sig_now.get("ts")
        out["signal_status"] = sig_now.get("status")
        out["signal_consistent"] = True
        out["comment"] = ("Показаны параметры действующего сигнала из общего списка."
                          if sig_now.get("status") == "active" else
                          "Сигнал уже сопровождается; повторно входить по нему нельзя.")
    else:
        out["signal_consistent"] = not bool(sig_now)
    cur_sl = (pos_now or {}).get("sl1") or (sig_now or {}).get("sl1")
    orig_sl = (pos_now or {}).get("sl_orig") or (sig_now or {}).get("sl1")
    out["current_sl"] = cur_sl
    out["trailing"] = bool(cur_sl and orig_sl and abs(cur_sl - orig_sl) > 1e-12)
    # Only a currently actionable, unfilled signal earns the "в сигналах" badge.
    out["signal_active"] = any(
        x["coin"] == coin and x.get("status") == "active"
        for x in get_active_signals()
    )
    out["corr"] = None
    out["correlation"] = await correlation_snapshot(coin)
    for oc, od in get_user_open_positions(uid).items():
        if oc != coin and od["side"] == res["side"] and any(oc in g and coin in g for g in CORRELATION_GROUPS):
            out["corr"] = f"Уже открыт {od['side']} по {oc}: монеты коррелируют"
    return out


def _safe_float(value, default=0.0):
    try:
        value = float(value)
        return value if np.isfinite(value) else default
    except (TypeError, ValueError):
        return default


_ai_center_cache = {}


async def ai_center_snapshot(uid, coin="BTC"):
    """One bounded snapshot assembled from existing feeds; no parallel scanner."""
    coin = normalize_ticker(str(coin or "BTC")) or "BTC"
    cache_key = (str(uid), coin)
    cached = _ai_center_cache.get(cache_key)
    if cached and time.time() - cached[0] < 60:
        return cached[1]
    try:
        tf_data, source = await get_market_data_light(coin, ("15m", "1h", "4h", "1d"))
    except Exception:
        tf_data, source = {}, None
    funding = await get_funding(coin)
    funding_history = await funding_trend(coin)
    oi_state, oi_pct, oi_price_pct = await oi_price_state(coin)
    price = None
    info = tf_data.get("4h") or tf_data.get("1h") or {}
    try: price = float(info.get("price") or info["df"]["close"].iloc[-1])
    except Exception: price = await get_simple_price(coin)
    book = await order_book_imbalance(coin, price) if price else None
    book_quality = await order_book_quality_snapshot(coin, price) if price else {"status":"insufficient_data"}
    df = info.get("df")
    regime = market_regime(df) if df is not None else {"name":"unknown","volatility":"unknown"}
    vol = volume_anomaly(df) if df is not None else None
    early = early_movement_assessment(df, oi_pct, book, funding, vol)
    news_risk = news_risk_assessment(market_bias, coin)
    forecast = forecast_scenarios(tf_data, news_risk=news_risk)
    crowd = liquidation_risk_assessment(funding, oi_pct, oi_price_pct)
    active = next((x for x in get_active_signals() if x.get("coin")==coin), None)
    quality = signal_quality_breakdown(active) if active else None
    hist = load_signal_history()
    mem = signal_memory_summary(hist, 120)
    recent_liqs = [x for x in LIQUIDATIONS if time.time()-x.get("ts",0)<=3600]
    coin_liqs = [x for x in recent_liqs if x.get("coin")==coin]
    brain_hist = [x for x in hist if x.get("coin") == coin]
    dna = market_fingerprint(coin, df, hist) if df is not None else {"status": "insufficient_data"}
    liquidity = liquidity_path(df, book) if df is not None else {"status": "insufficient_data", "levels": []}
    reaction = news_reaction_assessment({**(market_bias or {}), **(news_risk or {})}, df) if df is not None else {"status": "insufficient_data"}
    strategy_profile = strategy_dna(hist, 5)
    counterfactual = counterfactual_lab(brain_hist, 5)
    active_payload = _sig_item(active) if active else None
    brain_signal = (active.get("signal_dna") or signal_fingerprint(
        active or {}, context={"volume_anomaly": vol, "orderbook_bid_share": book, "news_risk": news_risk})
        if active else {"score": None, "factors": [], "conflicts": []})
    brain_decision = decision_brain(brain_signal, early, crowd, news_risk, forecast) if active else {"action": "WAIT", "reason": "Нет активного сигнала для оценки", "trade_execution": False}
    active_side = (active or {}).get("side")
    if not active_side and df is not None:
        try: active_side = "LONG" if float(df["close"].iloc[-1]) >= float(df["close"].iloc[-6]) else "SHORT"
        except Exception: active_side = None
    phase_transition = regime_transition_assessment(df) if df is not None else {"status": "insufficient_data"}
    fragility = signal_fragility(brain_signal) if active else {"status": "no_active_signal", "fragility": None}
    try: btc_context_move = await btc_move_15m()
    except Exception: btc_context_move = None
    evidence_map = evidence_conflict_map(active_side, {"orderbook_bid_share": book, "funding": funding,
        "news_bias": market_bias.get("bias") if isinstance(market_bias, dict) else None,
        "btc_change_pct": btc_context_move}, df) if df is not None else {"status": "insufficient_data"}
    calibration = score_calibration(hist, 8)
    _closed_quant_history = quant_closed_trade_history(None, 2000)
    probability_calibration = probability_calibration_report(_closed_quant_history, min_sample=20)
    _loss_rows = [loss_autopsy(x) for x in _closed_quant_history if float(x.get("r", 0) or 0) < 0]
    loss_autopsy_report = {"status": "ok" if _loss_rows else "insufficient_data", "losses": len(_loss_rows), "recent": _loss_rows[-10:]}
    _user_quant = _users().get(str(uid), {}) if isinstance(_users().get(str(uid), {}), dict) else {}
    _user_trade_log = list(_user_quant.get("trade_log", []) or [])
    _user_positions = list((_user_quant.get("open_positions", {}) or {}).values())
    _exchange_risk = {"status":"disabled_by_design"}
    _liq_distance_data = None
    try:
        _cvd_snapshot = await fetch_futures_cvd(coin)
        if time.time() - float(_macro_cache.get("ts", 0) or 0) > 900:
            await check_macro_calendar()
        quant_view = quant_intelligence_stack(tf_data, side=(active or {}).get("side", "LONG"),
            entry=(active or {}).get("entry"), stop=((active or {}).get("sls") or [None])[0],
            targets=(active or {}).get("tps"), history=quant_closed_trade_history(coin, 500), funding=funding, oi_change=oi_pct,
            orderbook_bid_share=book, news_risk=news_risk, run_simulation=False, portfolio_history=_user_trade_log,
            leverage=(active or {}).get("leverage"), funding_history=funding_history,
            spread_pct=book_quality.get("spread_pct") if book_quality.get("status") == "ok" else None,
            depth_notional=book_quality.get("depth_notional") if book_quality.get("status") == "ok" else None,
            liquidation_notional=sum(float(x.get("notional",0) or 0) for x in coin_liqs) if coin_liqs else None,
            cvd_data=_cvd_snapshot, macro_events=list(_macro_cache.get("events") or []),
            macro_feed_status=_macro_cache.get("status"), liquidation_distance_data=_liq_distance_data,
            positions=_user_positions, orderbook_snapshots=list(_ob_snapshot_history.get(coin, [])),
            liquidation_events=[x for x in LIQUIDATIONS if x.get("coin")==coin and time.time()-float(x.get("ts",0) or 0)<86400],
            signal_context=active or None, oi_price_change=oi_price_pct)
    except Exception as _qerr:
        log.debug("AI center quant snapshot %s: %s", coin, _qerr)
        quant_view = {"status":"error","risk_state":"YELLOW","reason":str(_qerr)[:160]}
    try:
        _asset_returns = build_synchronized_asset_returns(max_assets=24, min_points=31)
        quant_view["market_breadth"] = market_breadth_engine({k: v.get("change_pct") for k, v in LIVE.items() if isinstance(v, dict) and v.get("change_pct") is not None})
        quant_view["multi_asset_lead_lag"] = multi_asset_lead_lag_engine(_asset_returns, "BTC")
        quant_view["correlation_clusters"] = correlation_clusters(_asset_returns)
        if coin in _asset_returns and "BTC" in _asset_returns:
            quant_view["correlation_breakdown"] = correlation_breakdown({"asset":_asset_returns[coin], "BTC":_asset_returns["BTC"]}, "BTC")
        else:
            quant_view["correlation_breakdown"] = {"status":"insufficient_data","reason":"BTC/asset synchronized history unavailable"}
    except Exception as _corr_err:
        quant_view["multi_asset_lead_lag"] = {"status":"error"}
        quant_view["correlation_clusters"] = {"status":"error"}
        quant_view["correlation_breakdown"] = {"status":"error"}
    try:
        portfolio_dashboard = portfolio_risk_dashboard(_user_positions, _user_trade_log, get_user_risk_settings(uid).get("deposit"))
        strategy_lab = strategy_lab_report(_user_trade_log)
    except Exception as _portfolio_err:
        log.debug("Quant portfolio dashboard %s: %s", uid, _portfolio_err)
        portfolio_dashboard, strategy_lab = {"status":"error"}, {"status":"error"}
    result = {"coin":coin,"source":source,"price":price,"regime":regime,"volume_anomaly":vol,
            "early_movement":early,"forecast":forecast,"funding":funding,"oi_state":oi_state,
            "oi_change_pct":oi_pct,"oi_price_change_pct":oi_price_pct,"orderbook_bid_share":book,"orderbook_quality":book_quality,
            "crowding_risk":crowd,"news_risk":news_risk,"news":{"title":market_bias.get("title_ru"),
            "bias":market_bias.get("bias"),"severity":market_bias.get("severity"),"ts":market_bias.get("ts")},
            "active_signal":active_payload,"quality":quality,"memory":mem,
            "market_dna":dna,"liquidity_path":liquidity,"news_reaction":reaction,
            "signal_dna":brain_signal,"decision_brain":brain_decision,
            "regime_transition":phase_transition,"signal_fragility":fragility,
            "evidence_conflict_map":evidence_map,"score_calibration":calibration,
            "strategy_dna":strategy_profile,"counterfactual_lab":counterfactual,
            "liquidations_1h":{"count":len(coin_liqs),"notional":round(sum(float(x.get("notional",0) or 0) for x in coin_liqs),2)},
            "system_memory":self_audit_summary(hist,30),"quant":quant_view,
            "probability_calibration":probability_calibration, "loss_autopsy":loss_autopsy_report,
            "parameter_stability":parameter_sensitivity_from_history(_closed_quant_history),
            "missed_opportunities":missed_opportunity_analysis(STATE.get("quant_observations", [])),
            "macro_calendar":{"status":_macro_cache.get("status"),"source":_macro_cache.get("source"),"blocked":_macro_cache.get("blocked"),"events":list(_macro_cache.get("events") or [])[:10],"stale_rows":_macro_cache.get("stale_rows",0),"calendar_rows":_macro_cache.get("calendar_rows",0),"attribution_url":"https://www.financecalendar.com" if _macro_cache.get("source")=="FinanceCalendar" else None},
            "portfolio_risk":portfolio_dashboard,"strategy_lab":strategy_lab,"generated_at":time.time()}
    _ai_center_cache[cache_key] = (time.time(), result)
    if len(_ai_center_cache) > 24:
        oldest = sorted(_ai_center_cache.items(), key=lambda kv: kv[1][0])[:8]
        for key, _ in oldest: _ai_center_cache.pop(key, None)
    return result


def _format_ai_center(x):
    f=x.get("forecast") or {}; e=x.get("early_movement") or {}; cr=x.get("crowding_risk") or {}
    q=x.get("quality") or {}; nr=x.get("news_risk") or {}
    def px(v): return fmt(v) if isinstance(v,(int,float)) and v>0 else "—"
    lines=[f"🧠 <b>IXCLY AI Command Center · {html.escape(x.get('coin','BTC'))}</b>",
           f"Цена: <code>{px(x.get('price'))}</code> · режим: <b>{html.escape(str((x.get('regime') or {}).get('name','unknown')))}</b>",
           f"Раннее движение: <b>{e.get('score',0)}/100 · {'наблюдать' if e.get('state')=='watch' else 'тихо'}</b>",
           "Факторы: "+html.escape(', '.join(e.get('reasons') or ['нет подтверждений'])),
           f"Сценарии: вверх триггер <code>{px(f.get('bull_trigger'))}</code> → цель <code>{px(f.get('bull_target'))}</code>; вниз <code>{px(f.get('bear_trigger'))}</code> → <code>{px(f.get('bear_target'))}</code>",
           f"Funding: {x.get('funding') if x.get('funding') is not None else '—'} · OI: {x.get('oi_change_pct')}% · bids стакана: {x.get('orderbook_bid_share')}",
           f"Риск перегруженности: <b>{html.escape(str(cr.get('level','low')))}</b> (лонги {cr.get('long_crowding_risk',0)}/100 · шорты {cr.get('short_crowding_risk',0)}/100)",
           f"Новостной риск: {html.escape(str(nr.get('severity','low')))} · ликвидации {x.get('liquidations_1h',{}).get('count',0)} за час"]
    if q: lines.append(f"Активный сигнал: качество {q.get('score')}/100 · класс {q.get('label')}")
    else: lines.append("Активного сигнала по монете нет")
    lines.append(f"Signal Memory: {x.get('memory',{}).get('win_rate','—')}% · {x.get('memory',{}).get('trades',0)} сделок")
    lines.append("⚠️ Сценарии и риск — аналитические оценки, не гарантия результата и не команда на вход.")
    return "\n".join(lines)


async def mini_dispatch(uid, u, act, d):
    _users().setdefault(str(uid), {"active": True})["last_seen"] = time.time()
    if act not in FREE_ACTS and not has_access(uid):
        return {"error": access_closed_text(uid), "closed": True, "access": access_payload(uid),
                "bot": runtime["main_bot"]}
    if uid not in active_chats:
        active_chats.add(uid)
        await asyncio.to_thread(save_user_data, uid, u.get("username"), u.get("first_name"))
    uu = _users().get(str(uid), {})
    coin = normalize_ticker(str(d.get("coin", ""))) if d.get("coin") else None

    if act == "me":
        mode = uu.get("trade_mode", "balanced")
        return {"name": u.get("first_name") or "трейдер", "version": VERSION, "risk": get_user_risk_settings(uid),
                "mode": mode, "conf": MODE_CONF.get(mode, CONF_FINAL), "notif": get_user_notif_settings(uid),
                "saved": [_sig_item(s, False) for s in get_user_saved_signals(uid)],
                "alerts": uu.get("price_alerts", []), "webhook": uu.get("webhook_url", ""),
                "news": market_bias, "ctx": await get_context(), "coins": POPULAR_COINS[:150], "access": access_payload(uid), "bot": runtime["main_bot"],
                "counts": {"signals": len(signals_for(uid)), "saved": len(get_user_saved_signals(uid)),
                           "positions": len(get_user_open_positions(uid))}}

    if act == "order_status":
        # Опрос статуса заказа после оплаты: Mini App ждёт подтверждения от бота и обновляет подписку
        with STATE_LOCK:
            view = billing.order_view(STATE, uid, str(d.get("order_id", ""))[:40])
        if not view:
            return {"error": "Заказ не найден"}
        return {"order": view, "access": access_payload(uid)}

    if act == "ai_center":
        return await ai_center_snapshot(uid, coin or "BTC")

    if act == "intel":
        # Лёгкий снимок без дополнительного фонового сканера и без хранения свечных массивов.
        active = get_active_signals()[:30]
        heat = []
        for coin, item in list(LIVE.items())[:120]:
            if not isinstance(item, dict):
                continue
            heat.append({"coin": coin, "price": item.get("price"), "change_pct": item.get("change_pct"),
                         "long_prob": item.get("long_prob")})
        heat.sort(key=lambda x: abs(_safe_float(x.get("change_pct"))), reverse=True)
        breadth = market_breadth_engine({x["coin"]: x.get("change_pct") for x in heat if x.get("change_pct") is not None})
        # Общий режим берём только из реального BTC OHLC-анализа.
        # Не подменяем его новостным bias и не выводим техническое "unknown" пользователю.
        btc_cached = analysis_cache.get("BTC")
        btc_result = btc_cached[2] if isinstance(btc_cached, tuple) and len(btc_cached) >= 3 else {}
        btc_regime = btc_result.get("market_regime") if isinstance(btc_result, dict) else None
        regime_code = btc_regime.get("name") if isinstance(btc_regime, dict) else None
        regime_names = {"uptrend": "Бычий тренд", "downtrend": "Медвежий тренд", "range": "Боковик"}
        regime_label = regime_names.get(regime_code, "Нет данных")
        memory = signal_memory_summary(load_signal_history(), 120)
        profile = adaptive_scanner_profile(load_signal_history(), CONF_FINAL, regime=regime_code)
        ctx_now = await get_context()
        panic = market_panic_assessment(await btc_move_15m(), ctx_now.get("btc_atr_pct", 0))
        return {"regime": regime_label, "market": market_bias.get("title_ru", "Нет данных"),
                "news_risk": {"severity": market_bias.get("severity", "low"),
                              "critical_score": market_bias.get("critical_score", 0),
                              "coins": market_bias.get("coins", "")},
                "memory": memory, "adaptive": profile, "market_panic": panic, "ranking": scanner_ranking[:50],
                "signals": [{"coin": x.get("coin"), "side": x.get("side"), "quality": x.get("quality_score"),
                             "grade": x.get("quality_label"), "regime": x.get("market_regime"),
                             "volume_anomaly": x.get("volume_anomaly")} for x in active],
                "heatmap": heat[:20], "market_breadth": breadth, "cache_items": len(analysis_cache), "live_items": len(LIVE),
                "rss_mb": _current_rss_mb(), "peak_rss_mb": _rss_mb(),
                "runtime_memory": _runtime_memory_snapshot() if uid in ADMIN_IDS else None,
                "last_scan": _scan_info_for_user(uid)}

    if act == "risk_calc":
        try:
            entry = float(d.get("entry", 0))
            stop = float(d.get("stop", 0))
            side = str(d.get("side", "LONG")).upper()
            settings = get_user_risk_settings(uid)
            if entry <= 0 or stop <= 0 or entry == stop or side not in ("LONG", "SHORT"):
                return {"error": "Проверьте цену входа, Stop Loss и направление"}
            if (side == "LONG" and stop >= entry) or (side == "SHORT" and stop <= entry):
                return {"error": "Stop Loss должен быть ниже входа для LONG и выше для SHORT"}
            open_risk = calculate_portfolio_risk(uid)
            remaining = max(0.0, settings["max_open_risk"] - open_risk)
            requested = float(d.get("risk_pct", settings["risk_pct"]))
            if not 0.1 <= requested <= 10:
                return {"error": "Риск должен быть от 0.1 до 10%"}
            effective = min(requested, remaining) if remaining > 0 else 0.0
            if effective <= 0:
                return {"error": "Лимит общего открытого риска уже исчерпан"}
            sizing = risk_position_size(settings["deposit"], effective, entry, stop,
                                        max_notional=settings["deposit"] * 3.0)
            return {"side": side, "risk_pct": round(effective, 2), "requested_risk_pct": requested,
                    "open_risk_pct": round(open_risk, 2), "remaining_risk_pct": round(remaining, 2),
                    "max_open_risk_pct": settings["max_open_risk"], "warning": effective < requested, **sizing}
        except (TypeError, ValueError) as e:
            return {"error": str(e) or "Некорректные параметры риска"}

    if act == "paper_analytics":
        try:
            with STATE_LOCK:
                return paper_analytics(STATE.setdefault("paper", {}), uid, d.get("account_id"))
        except ValueError as e:
            return {"error": str(e)}

    if act == "max_dashboard":
        now = time.time()
        user_data = _users().get(str(uid), {})
        trade_log = list(user_data.get("trade_log", []))[-300:]
        trade_wins = sum(1 for x in trade_log if x.get("result") == "win")
        trade_losses = sum(1 for x in trade_log if x.get("result") == "loss")
        closed_trades = trade_wins + trade_losses
        signal_history = load_signal_history()
        unique, seen_keys = [], set()
        for item in sorted(signal_history, key=lambda x: x.get("ts", 0), reverse=True):
            key = f"{item.get('coin')}:{item.get('side')}"
            if key not in seen_keys:
                seen_keys.add(key)
                unique.append(item)
        sw = sum(1 for x in unique if x.get("status") == "win")
        sl = sum(1 for x in unique if x.get("status") == "loss")
        closed_signals = sw + sl
        movers = []
        for symbol, item in list(LIVE.items())[:150]:
            if isinstance(item, dict):
                movers.append({"coin": symbol, "change_pct": item.get("change_pct"), "price": item.get("price"), "ts": item.get("ts")})
        movers.sort(key=lambda x: abs(_safe_float(x.get("change_pct"))), reverse=True)
        alerts = list(user_data.get("price_alerts", []))[:20]
        active_locks = sum(1 for x in unique if x.get("status") in ("active", "in_trade"))
        recent_cooldowns = sum(1 for ts in signal_sent_at.values() if now - ts < SIGNAL_COOLDOWN)
        backup_age = round(now - last_backup_ts) if last_backup_ts else None
        scan_ts = float(last_scan_info.get("ts", 0) or 0)
        return {
            "performance": {"trades": len(trade_log), "wins": trade_wins, "losses": trade_losses,
                            "win_rate": round(trade_wins / closed_trades * 100, 1) if closed_trades else 0.0,
                            "net_pnl_pct": round(sum(float(x.get("pnl", 0) or 0) for x in trade_log), 2)},
            "system_signals": {"total": len(unique), "wins": sw, "losses": sl,
                               "win_rate": round(sw / closed_signals * 100, 1) if closed_signals else 0.0},
            "scanner": {"last_scan_age": round(now - scan_ts) if scan_ts else None,
                        "last_scan": dict(last_scan_info), "active_signals": len(get_active_signals()),
                        "live_coins": len(LIVE), "movers": movers[:8]},
            "alerts": alerts,
            "dedupe": {"enabled": True, "active_signal_locks": active_locks,
                       "recent_cooldowns": recent_cooldowns, "cooldown_seconds": SIGNAL_COOLDOWN},
            "monitor": {"uptime_seconds": round(now - stats.get("started", now)),
                        "news_engine": market_bias.get("engine", "none"),
                        "main_bot_configured": bool(runtime.get("main_bot")),
                        "admin_backup_configured": bool(ADMIN_IDS),
                        "backup_bot": "admin" if admin_bot else "main",
                        "storage_path": STATE_FILE,
                        "local_backup_files": sum(1 for x in LOCAL_BACKUP_FILES if os.path.exists(x)),
                        "backup_interval_seconds": BACKUP_INTERVAL,
                        "backup_age_seconds": backup_age, "backup_pending": bool(backup_dirty),
                        "backup_status": "нужен ADMIN_IDS" if not ADMIN_IDS else ("ожидает отправки" if backup_dirty else ("выполнен" if backup_age is not None else "ещё не выполнялся")),
                        "scan_age_seconds": round(now - scan_ts) if scan_ts else None},
        }

    if act == "signals":
        flt = d.get("flt", "Все")
        keys = ("Все", "Краткосрочный", "Среднесрочный", "Долгосрочный", "mode:aggressive", "mode:balanced", "mode:conservative")
        return {"items": [_sig_item(x) for x in signals_for(uid, flt)[:50]],
                "counts": {k: len(signals_for(uid, k)) for k in keys},
                "refresh": dict(refresh_job), "market": market_bias.get("impact", "")}

    if act == "refresh_all":
        if not refresh_job["running"]:
            asyncio.create_task(refresh_all_data(uid))
        return {"started": True}

    if act == "refresh_status":
        return dict(refresh_job)

    if act == "price" and coin:
        STREAM_COINS[coin] = time.time()
        p = PRICES.get(coin)
        out = {"price": p["price"] if p else None, "ts": p["ts"] if p else None}
        pos = uu.get("open_positions", {}).get(coin)
        if pos and p:
            sign = 1 if pos["side"] == "LONG" else -1
            out["pnl"] = round(price_change_pct(pos["side"], pos["entry_price"], p["price"]), 2)
            out["sl"] = pos.get("sl1")
            out["trailing"] = abs(pos.get("sl1", 0) - pos.get("sl_orig", pos.get("sl1", 0))) > 1e-12
        return out

    if act == "ref_link":
        cnt = sum(1 for x in _users().values() if isinstance(x, dict) and x.get("referrer") == uid)
        return {"link": f"https://t.me/{runtime['main_bot'].lstrip('@')}?start=ref_{uid}", "count": cnt}

    if act == "promo_apply":
        return {"msg": apply_promo(uid, str(d.get("code", "")))}

    if act == "coin" and coin:
        if d.get("force") and time.time() - COIN_RL.get(uid, 0) < 5:
            return {"error": "Подождите 5 секунд перед пересчётом"}
        if d.get("force"):
            COIN_RL[uid] = time.time()
        return await coin_payload(uid, coin, force=bool(d.get("force")))

    if act == "live" and coin:
        lv = LIVE.get(coin)
        if not lv or time.time() - lv["ts"] > 30:
            lv = await live_refresh_coin(coin)
        return {"live": lv}

    if act == "movers":
        j = await http_json("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
        mv = [(x["symbol"][:-4], float(x["priceChangePercent"])) for x in j
              if x["symbol"].endswith("USDT") and x["symbol"][:-4] in POPULAR_COINS]
        up = sorted([m for m in mv if m[1] > 0], key=lambda m: -m[1])[:10]
        dn = sorted([m for m in mv if m[1] < 0], key=lambda m: m[1])[:10]
        return {"up": up, "down": dn}

    # Paper Trading: отдельное логическое хранилище STATE["paper"], входит в штатный backup/restore.
    if act == "paper_get":
        prices = {}
        for c, item in list(PRICES.items()):
            try: prices[c] = float(item.get("price"))
            except Exception: pass
        for c, item in list(LIVE.items()):
            try: prices[c] = float(item.get("price"))
            except Exception: pass
        with STATE_LOCK:
            paper_store = STATE.setdefault("paper", {})
            was_new = str(uid) not in paper_store or not any(a.get("id") == "main" for a in paper_store.get(str(uid), {}).get("accounts", []))
            result = paper_snapshot(paper_store, uid, prices)
            if was_new:
                state_touch()
        return result

    if act == "paper_create":
        try:
            with STATE_LOCK:
                acc = paper_create_account(STATE.setdefault("paper", {}), uid, d.get("name", "Тестовая стратегия"), d.get("deposit", 1000), d.get("kind", "strategy"))
                state_touch()
            return {"msg": "Виртуальный счёт создан", "account": acc}
        except ValueError as e:
            return {"error": str(e)}

    if act == "paper_select":
        try:
            with STATE_LOCK:
                aid = paper_select_account(STATE.setdefault("paper", {}), uid, d.get("account_id", "main"))
                state_touch()
            return {"msg": "Активный Paper-счёт изменён", "account_id": aid}
        except ValueError as e:
            return {"error": str(e)}

    if act == "paper_settings":
        try:
            with STATE_LOCK:
                paper_set_settings(STATE.setdefault("paper", {}), uid, d.get("risk_pct"), d.get("fee_pct"), d.get("slippage_pct"), d.get("account_id"))
                state_touch()
            return {"msg": "Настройки Paper Trading сохранены"}
        except ValueError as e:
            return {"error": str(e)}

    if act == "paper_open" and coin:
        try:
            res = get_user_res(uid, coin)
            if not res or res.get("price_only"):
                res, err = await analyze_coin(coin, uid=uid)
                if err or not res or res.get("price_only"):
                    return {"error": "Нет свежего анализа для виртуального входа"}
            entry = d.get("entry", res.get("entry"))
            stop = d.get("stop", (res.get("sls") or [None])[0])
            targets = d.get("targets") or res.get("tps") or []
            if isinstance(targets, str): targets = [x.strip() for x in targets.split(",") if x.strip()]
            _paper_price = float(res.get("price") or entry)
            _paper_book = await order_book_quality_snapshot(coin, _paper_price)
            _execution_context = ({"spread_pct":_paper_book.get("spread_pct"),
                                  "depth_notional":_paper_book.get("depth_notional"),
                                  "max_participation":.10}
                                 if _paper_book.get("status") == "ok" else None)
            with STATE_LOCK:
                pos = paper_open_position(STATE.setdefault("paper", {}), uid, coin, res.get("side"), entry, stop, targets,
                                          d.get("risk_pct"), d.get("account_id"), execution_context=_execution_context)
                state_touch()
            _fill_msg = (f"частичное виртуальное исполнение {pos.get('fill_ratio_pct',0):.1f}%" if pos.get("fill_status") == "partial" else "виртуальное исполнение")
            return {"msg": f"Paper-позиция {coin}: {_fill_msg}", "position": pos}
        except ValueError as e:
            return {"error": str(e)}

    if act == "paper_close" and coin:
        try:
            px_item = PRICES.get(coin) or LIVE.get(coin) or {}
            px = d.get("exit_price") or px_item.get("price")
            if not px:
                px = await get_simple_price(coin)
            if not px:
                return {"error": "Нет актуальной цены для закрытия Paper-позиции"}
            with STATE_LOCK:
                rec = paper_close_position(STATE.setdefault("paper", {}), uid, coin, px, d.get("account_id"), d.get("reason", "manual"))
                state_touch()
            return {"msg": f"Paper-сделка закрыта: {rec['pnl']:+.2f} USDT", "trade": rec}
        except ValueError as e:
            return {"error": str(e)}

    if act == "paper_reset":
        try:
            with STATE_LOCK:
                acc = paper_reset_account(STATE.setdefault("paper", {}), uid, d.get("account_id"), d.get("deposit"))
                state_touch()
            return {"msg": "Paper-счёт сброшен", "account": acc}
        except ValueError as e:
            return {"error": str(e)}

    if act == "portfolio":
        return {"risk": get_user_risk_settings(uid), "open_risk": calculate_portfolio_risk(uid), "mode": uu.get("trade_mode", "balanced"),
                "positions": _enrich_user_positions(uid), "saved": [_sig_item(s, False) for s in get_user_saved_signals(uid)],
                "history": uu.get("trade_log", [])[-15:],
                "events": uu.get("trade_events", [])[-30:]}

    if act == "mystats":
        return personal_stats(uid)

    if act == "stats":
        # Полная история сигналов: не схлопываем повторные сигналы по одной монете/стороне.
        archive = sorted(load_signal_history(), key=lambda k: k.get("ts", 0), reverse=True)
        statuses = ("win", "missed", "loss", "expired", "cancelled")
        cnt = {status: sum(1 for item in archive if item.get("status") == status) for status in statuses}
        active = get_active_signals()
        cnt["active"] = len(active)
        lists = {status: [_sig_item(item, False) for item in archive if item.get("status") == status][:30]
                 for status in statuses}
        lists["active"] = [_sig_item(item) for item in active][:30]
        closed = cnt["win"] + cnt["loss"]
        return {"counts": cnt, "lists": lists, "total": len(archive),
                "wr": cnt["win"] / closed * 100 if closed else 0.0}

    if act == "liquidations":
        now = time.time()
        recent = [x for x in LIQUIDATIONS if now - x.get("ts", 0) <= 3600]
        return {"items": recent[-30:], "updated": now}

    if act == "news":
        return {"bias": market_bias, "html": await get_news_batch_text()}

    if act == "toggle" and d.get("key") in NOTIFY_NAMES:
        k = d["key"]
        new = not uu.get(f"notify_{k}", True)
        set_user_field(uid, **{f"notify_{k}": new})
        return {"msg": f"{NOTIFY_NAMES[k]}: {'вкл' if new else 'выкл'}"}

    if act == "set_deposit":
        v = float(d.get("value", 0))
        if v <= 0:
            return {"error": "Депозит должен быть больше 0"}
        set_user_field(uid, deposit=v)
        return {"msg": "Депозит сохранён"}

    if act == "set_risk":
        v = float(d.get("value", 0))
        if not 0.1 <= v <= 10:
            return {"error": "Риск от 0.1 до 10%"}
        set_user_field(uid, risk_pct=v)
        return {"msg": "Риск сохранён"}

    if act == "set_mode" and d.get("value") in MODE_NAMES:
        m = d["value"]
        set_user_field(uid, trade_mode=m, conf_final=MODE_CONF[m])
        return {"msg": "Режим обновлён"}

    if act == "fav_add" and coin:
        res = get_user_res(uid, coin) or (await analyze_coin(coin, uid=uid))[0]
        if not res or res.get("price_only"):
            return {"error": "Нет данных по монете"}
        await asyncio.to_thread(add_user_saved_signal, uid, coin, render_caption(res, uid), res)
        tracked = open_tracked_position(uid, coin, res)
        if tracked:
            return {"msg": f"{coin} сохранён; виртуальное сопровождение включено"}
        return {"msg": f"{coin} сохранён в избранное, но сопровождение не включено: сигнал не прошёл проверку входа или лимит риска"}

    if act == "fav_del" and coin:
        await asyncio.to_thread(del_user_saved_signal, uid, coin)
        return {"msg": "Удалено из избранного"}

    if act in ("pos_enter", "pos_set_entry") and coin:
        res = get_user_res(uid, coin) or (await analyze_coin(coin, uid=uid))[0]
        if not res or res.get("price_only"):
            return {"error": "Данные устарели, обновите анализ"}
        raw_entry = d.get("entry_price", d.get("entry"))
        if raw_entry not in (None, ""):
            try:
                custom_entry = float(str(raw_entry).replace(",", ".").strip())
                if not (custom_entry > 0 and custom_entry < 1e15):
                    raise ValueError
                res = rebase_signal_prices(res, custom_entry)
                res["entry_override"] = True
            except (TypeError, ValueError):
                return {"error": "Введите корректную цену входа больше нуля"}
        elif act == "pos_set_entry":
            return {"error": "Укажите новую цену входа"}
        existing = get_open_position(uid, coin)
        if act == "pos_set_entry":
            if not existing or not existing.get("waiting_entry"):
                return {"error": "Изменять можно только ожидающий лимитный ордер"}
            existing.update({"entry_price": float(res["entry"]), "tp1": res["tps"][0], "tp2": res["tps"][1],
                             "tp3": res["tps"][2], "sl1": res["sls"][0], "sl2": res["sls"][1],
                             "sl_orig": res["sls"][0], "entry_source": "user", "warned_entry": None})
            set_user_open_position(uid, coin, existing)
            await _tell(uid, f"✏️ <b>Лимит обновлён: {coin} ({res['side']})</b>\nНовая цена входа: <code>{fmt(res['entry'])}</code>\nTP/SL пересчитаны. Проверьте и обновите ордер на бирже.")
            return {"msg": "Цена лимитного ордера и уровни TP/SL обновлены"}
        for oc, od in get_user_open_positions(uid).items():
            if oc != coin and od.get("side") == res["side"] and any(oc in g and coin in g for g in CORRELATION_GROUPS):
                return {"error": f"Уже открыт {od['side']} по {oc}: {coin} сильно коррелирует"}
        if not open_tracked_position(uid, coin, res):
            if get_open_position(uid, coin):
                return {"error": "Сделка по монете уже открыта"}
            return {"error": "Вход отклонён: сигнал устарел, риск не GREEN, лимит портфеля превышен или сигнал уже закрывался. Обновите анализ и проверьте риск."}
        await _tell(uid, f"⏳ <b>Лимитный ордер: {coin} ({res['side']})</b>\nЦена входа: <code>{fmt(res['entry'])}</code>\n"
                         f"Сопровождение включено. Если цена входа отличается от вашей биржи, измените её в Mini App.")
        return {"msg": "Ордер поставлен, ждём вход"}

    if act == "pos_close" and coin:
        rec = await close_position_manual(uid, coin)
        return {"msg": f"Закрыто: {rec['pnl']:+.2f}%" if rec else "Позиция закрыта"}

    if act == "alert_add" and coin:
        price = float(d.get("price", 0))
        if price <= 0:
            return {"error": "Неверная цена"}
        cur = await get_simple_price(coin)
        op = d.get("op") or (">" if cur is None or price >= cur else "<")
        with STATE_LOCK:
            lst = STATE["users"].setdefault(str(uid), {"active": True}).setdefault("price_alerts", [])
            if len(lst) >= 20:
                return {"error": "Максимум 20 алертов"}
            lst.append({"coin": coin, "op": op, "price": price})
        state_touch()
        return {"msg": "Алерт создан"}

    if act == "alert_del":
        with STATE_LOCK:
            lst = STATE["users"].get(str(uid), {}).get("price_alerts", [])
            i = int(d.get("idx", -1))
            if 0 <= i < len(lst):
                lst.pop(i)
        state_touch()
        return {"msg": "Удалено"}

    if act == "webhook":
        url = str(d.get("url", "")).strip()
        if url and not await asyncio.to_thread(safe_webhook, url):
            return {"error": "Разрешены только HTTPS webhook на публичных адресах (без localhost и внутренних IP)"}
        set_user_field(uid, webhook_url=url)
        return {"msg": "Вебхук сохранён" if url else "Вебхук отключён"}

    if act == "csv":
        await bot.send_document(uid, BufferedInputFile(generate_csv_history(), "signals_history.csv"), caption="📊 История сигналов (CSV)")
        return {"msg": "Файл отправлен в чат"}

    return {"error": "Неизвестное действие"}


# =====================================================================
# БОТ: /start, уведомления, кнопки
# =====================================================================
def bot_main_keyboard():
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🚀 Открыть IXCLY App", web_app=WebAppInfo(url=webapp_link()))])
    rows.append([InlineKeyboardButton(text="🔥 Сигналы", web_app=WebAppInfo(url=webapp_link(tab="signals")))] if WEBAPP_URL else
                [InlineKeyboardButton(text="🔥 Сигналы", callback_data="noop")])
    rows.append([InlineKeyboardButton(text="💎 Подписка", callback_data="sub:open")])
    rows.append([InlineKeyboardButton(text="📰 IXCLY NEWS · Новости крипторынка", url=NEWS_CHANNEL_URL)])
    rows.append([InlineKeyboardButton(text="🔔 Уведомления", callback_data="notif:menu")])
    rows.append([InlineKeyboardButton(text="🎓 Обучение новичка", callback_data="tutorial:start")])
    rows.append([InlineKeyboardButton(text="📘 Инструкция и риски", callback_data="guide:open")])
    rows.append([InlineKeyboardButton(text="📄 Документы", callback_data="show_docs")])
    rows.append([InlineKeyboardButton(text="🆘 Поддержка", callback_data="support_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


WELCOME_BOT = ("👋 <b>IXCLY PRO</b>\n\nВся работа теперь в мини-приложении: сигналы, графики, портфель, статистика.\n"
               "Сюда я присылаю уведомления: новые сигналы, сопровождение сделок, итоги и снятые идеи.\n\n"
               f"📰 <b>Читайте IXCLY NEWS</b> — новости криптовалют России и мира, Bitcoin, GPT и аналитика: "
               f"<a href=\"{NEWS_CHANNEL_URL}\">@ixcly_news</a>")


# Пошаговое обучение хранит только статус/номер урока в существующем STATE пользователя.
# Никаких новых зависимостей или отдельного хранилища не требуется.
TUTORIAL_STEPS = [
    ("👋 Добро пожаловать в IXCLY", 
     "IXCLY помогает следить за крипторынком, изучать сигналы и вести статистику.\n\n"
     "Основная работа проходит в Mini App. Откройте его кнопкой «Открыть IXCLY App» в главном меню.\n\n"
     "Обучение займёт несколько минут. Оно объяснит основные разделы и безопасный порядок работы."),
    ("🔥 1. Сигналы", 
     "Откройте вкладку «Сигналы». Здесь собраны торговые идеи, которые прошли фильтры сканера.\n\n"
     "Используйте фильтры, чтобы смотреть нужные статусы. Нажмите на монету, чтобы открыть подробный разбор.\n\n"
     "Сигнал — это аналитическая идея, а не приказ покупать или продавать и не гарантия результата."),
    ("🔎 2. Как читать карточку монеты", 
     "В карточке изучите направление LONG/SHORT, цену входа, цели TP и уровень Stop Loss. Ниже могут быть причины, индикаторы, таймфреймы, риск и результаты исторической проверки.\n\n"
     "LONG означает сценарий роста, SHORT — сценарий снижения. TP — цели фиксации, SL — уровень ограничения убытка.\n\n"
     "Оценка качества/score не является вероятностью прибыли. Если статус WAIT или данные противоречат друг другу — не торопитесь."),
    ("💼 3. Портфель и риск", 
     "Во вкладке «Портфель» можно указать свой депозит и риск на сделку, выбрать режим и просматривать сопровождаемые позиции.\n\n"
     "Начните с небольшого риска и проверьте, что понимаете размер возможного убытка до любого решения. Не увеличивайте риск ради отыгрыша."),
    ("🧪 4. Paper Trading — тренировка без денег", 
     "В разделе «Портфель» находится PAPER TRADING. Это виртуальный счёт: можно открыть тестовую позицию, создать отдельный счёт/стратегию, посмотреть виртуальный баланс и историю.\n\n"
     "Paper Trading не отправляет реальные ордера на биржу. Используйте его, чтобы потренироваться и проверить подход до принятия решений на реальном рынке."),
    ("📊 5. Аналитика", 
     "Во вкладке «Аналитика» смотрите личные результаты и общую статистику системы: успешные/убыточные сделки, историю, просадку и другие показатели.\n\n"
     "Одна или несколько сделок не показывают надёжность стратегии. Смотрите на выборку целиком и учитывайте комиссии, проскальзывание и рыночные условия."),
    ("🧠 6. Раздел «Ещё»", 
     "Во вкладке «Ещё» доступны AI Command Center, новости рынка, настройки уведомлений, приглашение друзей, поддержка и документы.\n\n"
     "AI Command Center показывает сценарии, раннее движение, funding/OI и риск перегруженности. Это дополнительная аналитика, а не автоматическое управление сделками."),
    ("🛡 7. Безопасный порядок работы", 
     "Рекомендуемый порядок:\n1) изучить рынок и новость;\n2) открыть сигнал и прочитать причины;\n3) проверить вход, TP, SL и риск;\n4) при необходимости сначала потренироваться в Paper Trading;\n5) вести статистику и не менять план под влиянием эмоций.\n\n"
     "Криптовалюты волатильны. При сомнениях лучше пропустить идею. IXCLY не гарантирует прибыль и не заменяет ваше решение."),
]


def tutorial_keyboard(step):
    step = max(0, min(int(step), len(TUTORIAL_STEPS) - 1))
    rows = []
    nav = []
    if step > 0:
        nav.append(InlineKeyboardButton(text="⬅ Назад", callback_data=f"tutorial:step:{step-1}"))
    if step < len(TUTORIAL_STEPS) - 1:
        nav.append(InlineKeyboardButton(text="Далее ➡", callback_data=f"tutorial:step:{step+1}"))
    else:
        nav.append(InlineKeyboardButton(text="✅ Завершить", callback_data="tutorial:done"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="⏭ Пропустить обучение", callback_data="tutorial:skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tutorial_message(step):
    step = max(0, min(int(step), len(TUTORIAL_STEPS) - 1))
    title, body = TUTORIAL_STEPS[step]
    return (f"🎓 <b>Обучение IXCLY · {step + 1}/{len(TUTORIAL_STEPS)}</b>\n\n"
            f"<b>{html.escape(title)}</b>\n\n{html.escape(body)}")


async def show_tutorial(target, step=0, edit=False):
    text = tutorial_message(step)
    markup = tutorial_keyboard(step)
    if edit:
        try:
            return await target.edit_text(text, parse_mode="HTML", reply_markup=markup)
        except Exception:
            pass
    return await target.answer(text, parse_mode="HTML", reply_markup=markup)


def notif_keyboard(uid):
    s = get_user_notif_settings(uid)
    rows = [[InlineKeyboardButton(text=f"{'🟢' if s[k] else '🔴'} {NOTIFY_NAMES[k]}", callback_data=f"notif:{k}")]
            for k in ("signals", "pos_updates", "news", "anomalies", "briefing", "liquidations")]
    rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="notif:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


ANALYZE_SEM = None
DOCS_PRIVACY = "https://telegra.ph/POLITIKA-KONFIDENCIALNOSTI-IXCLY-PRO-10-31"
DOCS_OFFER = "https://telegra.ph/PUBLICHNAYA-OFERTA-POLZOVATELSKOE-SOGLASHENIE-IXCLY-PRO-10-31"
DISCOUNT_OFFERS = {}
_CRIT_EVENTS = [
    (re.compile(r"\b(hacked|exploited|drained|rug ?pull|stolen|breached?)\b", re.I), 3),
    (re.compile(r"\b(insolven\w*|bankrupt\w*|delist(ed|s|ing)?|depegg?ed|halts? (withdrawals?|trading|deposits?))\b", re.I), 3),
    (re.compile(r"\bsec\b.{0,40}\b(sues?|charges?|lawsuit)\b|\b(sues?|charges?)\b.{0,40}\bsec\b", re.I), 3),
    (re.compile(r"\b(arrested|sanctioned|seized|indicted)\b", re.I), 2),
    (re.compile(r"\b(взлом\w*|хакер\w*|банкротств\w*|делистинг\w*|заморозил\w*)\b", re.I), 3),
]
_CRIT_CONTEXT = re.compile(r"\b(bitcoin|btc|ethereum|eth|binance|coinbase|bybit|okx|kraken|exchange|stablecoin|usdt|usdc|tether|defi|protocol|bridge|wallet|биржа|биржи|стейблкоин\w*|биткоин\w*)\b", re.I)
_CRIT_NOISE = re.compile(r"\b(reward hacking|ai models?|llms?|chatgpt|openai|explained|guide|how to|opinion|podcast|weekly|price prediction)\b|искусственн\w+ интеллект", re.I)
_AMOUNT = re.compile(r"\$\s?(\d+(?:[.,]\d+)?)\s?(million|billion|m|b|млн|млрд)\b", re.I)


def classify_critical(title, summary=""):
    text = f"{title}. {summary[:300]}"
    if _CRIT_NOISE.search(text):
        return 0, "шум"
    score = sum(weight for rx, weight in _CRIT_EVENTS if rx.search(text))
    if not score or not _CRIT_CONTEXT.search(text):
        return 0, "нет события или крипто-контекста"
    match = _AMOUNT.search(text)
    if match:
        amount = float(match.group(1).replace(",", "."))
        if match.group(2).lower() in ("billion", "b", "млрд"):
            amount *= 1000
        score += 3 if amount >= 100 else 2 if amount >= 20 else 0
    return score, "ok"


@dp.message(Command("start", "menu"))
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    user_key = str(uid)
    is_first_start = user_key not in _users()
    parts = (m.text or "").split()
    referral_uid = None
    if len(parts) > 1 and parts[1].startswith("ref_") and parts[1][4:].isdigit():
        candidate = int(parts[1][4:])
        if candidate != uid:
            referral_uid = candidate
    await asyncio.to_thread(save_user_data, uid, m.from_user.username, m.from_user.first_name)
    if referral_uid and "referrer" not in _users().get(user_key, {}):
        set_user_field(uid, referrer=referral_uid)
    # Welcome discount is assigned once, at first registration, and persisted
    # in STATE so restarts or repeated /start commands cannot renew it.
    if is_first_start:
        user_now = _users().get(user_key, {})
        referred = bool(user_now.get("referrer"))
        set_user_field(uid, welcome_offer={
            "pct": 20 if referred else 10,
            "kind": "referral" if referred else "welcome",
            "created_at": time.time(),
            "until": time.time() + 3 * 86400,
            "used": False,
        })
    active_chats.add(m.chat.id)
    try:
        await m.delete()
    except Exception:
        pass
    if len(parts) > 1 and parts[1].lower() in ("support", "help", "helpdesk"):
        return await show_support_menu(m)
    user_state = _users().get(user_key, {})
    if is_first_start and not user_state.get("onboarding_done") and not user_state.get("onboarding_skipped"):
        set_user_field(uid, onboarding_status="offered", onboarding_started_at=time.time())
        intro = ("👋 <b>Добро пожаловать в IXCLY PRO!</b>\n\n"
                 "Вы впервые здесь? Я могу провести короткое обучение: покажу, где искать сигналы, "
                 "как читать анализ, пользоваться портфелем, статистикой и виртуальным Paper Trading.\n\n"
                 "Выберите, как продолжить:")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎓 Начать обучение", callback_data="tutorial:start")],
            [InlineKeyboardButton(text="⏭ Пропустить обучение", callback_data="tutorial:skip")],
            [InlineKeyboardButton(text="📰 IXCLY NEWS · Новости крипторынка", url=NEWS_CHANNEL_URL)],
        ])
        return await m.answer(intro, parse_mode="HTML", reply_markup=kb)
    await welcome_for(m.chat.id, m.from_user.id)


from PIL import ImageFilter, ImageChops

# =====================================================================
# КАРТОЧКИ СИГНАЛОВ (PNG в стиле «КОЛЛ ДНЯ»)
# =====================================================================
GOLD = (250, 190, 40)


def _pfmt(v):
    v = float(v)
    if abs(v) >= 1000:
        return f"{v:,.0f}".replace(",", " ")
    return fmt(v)
TEXT_GREY = (130, 137, 165)


def _font(size, bold=False):
    try:
        return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans%s.ttf" % ("-Bold" if bold else ""), size)
    except Exception:
        return ImageFont.load_default()


def _tw(draw, text, font):
    try:
        return draw.textlength(text, font=font)
    except Exception:
        return font.getsize(text)[0]


def _fit(draw, text, font, maxw):
    t = text
    while t and _tw(draw, t, font) > maxw:
        t = t[:-2]
    return t if t == text else (t.rstrip() + "…")


def _dash(d, x0, x1, y, color, w=2, dash=14, gap=10):
    x = x0
    while x < x1:
        d.line((x, y, min(x + dash, x1), y), fill=color, width=w)
        x += dash + gap


def _closes_plot(d, closes, box, levels):
    """Линия цены и уровни. levels: [(значение, подпись, цвет, пунктир)]."""
    x0, y0, x1, y1 = box
    vals = list(closes) + [v for v, *_ in levels]
    vmin, vmax = min(vals) * 0.998, max(vals) * 1.002
    rng = (vmax - vmin) or 1.0

    def ym(v):
        return y1 - (v - vmin) / rng * (y1 - y0)

    if len(closes) >= 2:
        n = len(closes)
        pts = [(x0 + i * (x1 - x0) / (n - 1), ym(v)) for i, v in enumerate(closes)]
        d.polygon(pts + [(x1, y1), (x0, y1)], fill=(22, 27, 52))
        d.line(pts, fill=(96, 136, 255), width=4)
        lx, ly = pts[-1]
    else:
        lx, ly = x1, ym(levels[0][0])
    font = _font(23, True)
    for v, label, color, dashed in levels:
        y = ym(v)
        if dashed:
            _dash(d, x0, x1, y, color)
        else:
            d.line((x0, y, x1, y), fill=color, width=3)
        tw = _tw(d, f"{label} {_pfmt(v)}", font)
        d.rounded_rectangle((x1 + 22, y - 19, x1 + 22 + tw + 22, y + 19), radius=18, fill=color)
        d.text((x1 + 33, y - 15), f"{label} {_pfmt(v)}", fill=(10, 11, 18), font=font)
    return lx, ly, ym


def render_signal_card(x, closes, price, title="СИГНАЛ"):
    """Карточка: заголовок, цена входа, график с уровнями, почему, «Ошибаюсь, если», футер."""
    W, H = 1024, 1280
    long = x["is_long"]
    accent = (45, 226, 198) if long else (255, 90, 110)
    img = Image.new("RGB", (W, H), (10, 11, 18))
    glow = Image.new("RGB", (W, H), (0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse((520, -260, 1320, 520), fill=(12, 70, 62) if long else (70, 16, 30))
    glow = glow.filter(ImageFilter.GaussianBlur(120))
    img = ImageChops.add(img, glow)
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, W, 8), fill=GOLD)

    msk = datetime.now(timezone(timedelta(hours=3)))
    f_badge, f_small, f_head = _font(30, True), _font(28), _font(64, True)
    bw = _tw(d, f"{title} · {x['coin']}", f_badge) + 52
    d.rounded_rectangle((58, 50, 58 + bw, 110), radius=30, outline=GOLD, width=3)
    d.text((84, 62), f"{title} · {x['coin']}", fill=GOLD, font=f_badge)
    dt = msk.strftime("%d.%m · %H:%M")
    d.text((966 - _tw(d, dt, f_small), 66), dt, fill=TEXT_GREY, font=f_small)

    d.text((58, 150), f"{x['coin']} {'пойдёт вверх' if long else 'пойдёт вниз'}", fill=(245, 247, 255), font=f_head)
    d.text((58, 232), _pfmt(x["entry"]), fill=accent, font=_font(150, True))
    d.text((58, 404), f"{'лонг' if long else 'шорт'} лимит от {_pfmt(x['entry'])} · сейчас {_pfmt(price or x['entry'])}",
           fill=TEXT_GREY, font=_font(30))

    levels = [(x["entry"], "вход", accent, False), (x["tp1"], "цель", accent, True), (x["sl1"], "отмена", (150, 150, 165), True)]
    lx, ly, ym = _closes_plot(d, closes or [], (58, 470, 680, 780), levels)
    d.ellipse((lx - 9, ly - 9, lx + 9, ly + 9), fill=(255, 255, 255))
    d.line((lx + 14, ly, 680, ym(x["tp1"])), fill=accent, width=3)

    d.rounded_rectangle((58, 815, 966, 1050), radius=24, fill=(16, 19, 32), outline=(40, 46, 70), width=2)
    d.text((88, 835), "ПОЧЕМУ", fill=TEXT_GREY, font=_font(22, True))
    reasons = [re.sub(r"\s*\(.*\)$", "", r) for r in (x.get("reasons") or [])][:4] or ["Совпали тренд, импульс и объём на 4h/1d"]
    f_r = _font(26)
    for i, r in enumerate(reasons):
        y = 878 + i * 42
        d.ellipse((88, y, 120, y + 32), outline=GOLD, width=2)
        d.text((97 if i < 9 else 92, y + 4), str(i + 1), fill=GOLD, font=_font(20, True))
        d.text((140, y + 3), _fit(d, r, f_r, 966 - 140 - 36), fill=(235, 238, 250), font=f_r)

    d.rounded_rectangle((58, 1075, 966, 1155), radius=24, fill=(30, 14, 20), outline=(120, 40, 60), width=2)
    inv = f"Ошибаюсь, если {'уйдём ниже' if long else 'уйдём выше'} {_pfmt(x['sl1'])}"
    d.text((88, 1095), inv, fill=(250, 240, 244), font=_font(34, True))

    d.ellipse((58, 1190, 128, 1260), outline=GOLD, width=3)
    d.text((76, 1207), "IX", fill=GOLD, font=_font(26, True))
    d.text((150, 1204), "IXCLY PRO", fill=(245, 247, 255), font=_font(36, True))
    foot = f"Conf {x['strength']}% · {x['type']}"
    d.text((966 - _tw(d, foot, _font(26)), 1210), foot, fill=TEXT_GREY, font=_font(26))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


async def _closes(coin):
    rows = await fetch_history(coin, "4h", 60, min_rows=20)
    return [r[4] for r in rows][-60:] if rows else []


def signal_caption(x, uid, title="СИГНАЛ"):
    s = get_user_risk_settings(uid)
    risk_usd = s["deposit"] * s["risk_pct"] / 100
    dist = abs(x["entry"] - x["sl1"]) / x["entry"] * 100
    pos = risk_usd / (dist / 100) if dist > 0 else 0
    head = f"🔥 <b>{title}</b>\n" if title != "СИГНАЛ" else ""
    e = "🟢" if x["is_long"] else "🔴"
    return (f"{head}{e} <b>{x['side']} · {html.escape(x['coin'])}</b> · {x['type']} · Conf <b>{x['strength']}%</b>\n"
            f"📥 Вход <b>лимит</b> <code>{fmt(x['entry'])}</code>\n"
            f"🎯 TP1 <code>{fmt(x['tp1'])}</code> · TP2 <code>{fmt(x['tp2'])}</code> · TP3 <code>{fmt(x['tp3'])}</code>\n"
            f"🛡 Отмена <code>{fmt(x['sl1'])}</code> (−{dist:.2f}%)\n"
            f"💼 Для вашего депозита: позиция ~<b>${pos:,.0f}</b> (≈ {pos / x['entry']:.4g} {html.escape(x['coin'])}), риск <b>${risk_usd:.0f}</b>\n"
            f"<i>Не финансовая рекомендация. Вход только лимитом.</i>")


def signal_keyboard(x):
    s = "L" if x["is_long"] else "S"
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="📈 Открыть в приложении", web_app=WebAppInfo(url=webapp_link(x["coin"])))])
    rows.append([InlineKeyboardButton(text="👁 Отслеживать вход", callback_data=f"in:{s}:{x['coin']}"),
                 InlineKeyboardButton(text="💾 Сохранить", callback_data=f"sv:{s}:{x['coin']}")])
    rows.append([InlineKeyboardButton(text="🗑 Скрыть", callback_data="del_msg")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def webapp_kb(coin):
    rows = [[InlineKeyboardButton(text="📈 Открыть в приложении", web_app=WebAppInfo(url=webapp_link(coin)))]] if WEBAPP_URL else []
    return InlineKeyboardMarkup(inline_keyboard=rows + [[InlineKeyboardButton(text="🗑 Скрыть", callback_data="del_msg")]])


async def deliver_new_signal(item, r, title="СИГНАЛ"):
    """Карточка сигнала (PNG) + персональная подпись каждому, кто включил сигналы и чей порог ниже силы."""
    closes = await _closes(item["coin"])
    card = await asyncio.to_thread(render_signal_card, item, closes, r.get("price") or item["entry"], title)
    delivered, failed = 0, []
    for cid in signal_recipients(item["coin"], item["side"]):
        try:
            await _send_new(cid, signal_caption(item, cid, title), signal_keyboard(item), card)
            if cid not in item["sent_to"]:
                item["sent_to"].append(cid)
            delivered += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed.append(cid)
            log.warning("Не доставлен новый сигнал %s пользователю %s: %s", item["coin"], cid, e)
    item["delivery"] = {"attempted": delivered + len(failed), "delivered": delivered,
                        "failed": failed[:100], "retry_count": 0,
                        "retry_after": time.time() + 30, "last_attempt": time.time()}
    state_touch()
    if not delivered:
        await notify_admin(f"🚨 <b>Сигнал {html.escape(item['coin'])} создан, но не доставлен ни одному пользователю.</b> "
                           "Проверьте доступы, подписки и ошибки Telegram.")
    elif failed:
        await notify_admin(f"⚠️ <b>Доставка сигнала {html.escape(item['coin'])}:</b> "
                           f"доставлено {delivered}, ошибок {len(failed)}. Запланированы повторы.")
    asyncio.create_task(send_webhook_all({"event": "new_signal", "coin": item["coin"], "side": item["side"],
                                          "entry": item["entry"], "tp1": item["tp1"], "sl1": item["sl1"]}))


async def retry_failed_signal_deliveries(hist):
    """Повторяет недоставленные сигналы ограниченное число раз; не блокирует основной цикл."""
    now = time.time()
    changed = False
    for item in hist:
        delivery = item.get("delivery") or {}
        failed = list(delivery.get("failed") or [])
        if (not failed or item.get("status") not in ("active", "in_trade")
                or now < float(delivery.get("retry_after", 0) or 0)
                or int(delivery.get("retry_count", 0) or 0) >= 5):
            continue
        remaining = []
        for uid in failed[:30]:
            try:
                u = _users().get(str(uid), {})
                if not isinstance(u, dict) or not u.get("active", True) or not has_access(int(uid)):
                    continue
                await _send_new(int(uid), signal_caption(item, int(uid)), signal_keyboard(item), None)
                if int(uid) not in item.setdefault("sent_to", []):
                    item["sent_to"].append(int(uid))
                delivery["delivered"] = int(delivery.get("delivered", 0)) + 1
            except Exception as exc:
                remaining.append(uid)
                log.warning("Повторная доставка сигнала %s пользователю %s не удалась: %s",
                            item.get("coin"), uid, exc)
            await asyncio.sleep(0.05)
        delivery["failed"] = remaining
        delivery["retry_count"] = int(delivery.get("retry_count", 0) or 0) + 1
        delivery["retry_after"] = now + min(300, 30 * (delivery["retry_count"] + 1))
        delivery["last_attempt"] = now
        changed = True
        if remaining and delivery["retry_count"] >= 5:
            await notify_admin(f"🚨 <b>Не удалось доставить сигнал {html.escape(str(item.get('coin')))}</b> "
                               f"{len(remaining)} пользователям после 5 повторов.")
    if changed:
        save_signal_history(hist)


def sig_as_res(x):
    return {"side": x["side"], "is_long": x["is_long"], "entry": x["entry"], "coin": x["coin"],
            "signal_id": x.get("id"), "signal_ts": x.get("ts"),
            "tps": [x["tp1"], x["tp2"], x["tp3"]], "sls": [x["sl1"], x.get("sl2", x["sl1"])], "price": x.get("live_price", x["entry"])}


async def handle_signal_button(cb, data):
    uid = cb.from_user.id
    action, s, coin = data.split(":", 2)
    side = "LONG" if s == "L" else "SHORT"
    x = next((y for y in get_active_signals() if y["coin"] == coin and y["side"] == side), None)
    if not x:
        return await safe_answer(cb, "Сигнал уже неактуален", True)
    res = sig_as_res(x)
    if action == "in":
        for oc, od in get_user_open_positions(uid).items():
            if oc != coin and od["side"] == side and any(oc in g and coin in g for g in CORRELATION_GROUPS):
                return await safe_answer(cb, f"Уже открыт {side} по {oc}: монеты коррелируют", True)
        if not open_tracked_position(uid, coin, res):
            message = ("Сделка по монете уже открыта" if get_open_position(uid, coin)
                       else "Вход отклонён: сигнал устарел, риск не GREEN или лимит портфеля превышен.")
            return await safe_answer(cb, message, True)
        await safe_answer(cb, "Включено виртуальное сопровождение")
        await _tell(uid, f"👁 <b>Виртуальное сопровождение включено: {coin} {side}</b>\\n"
                         f"Ожидаемая цена входа: <code>{fmt(x['entry'])}</code>.\\n"
                         "⚠️ Бот не размещает ордер на бирже и не управляет реальной позицией. "
                         "Выставьте и проверьте ордер, стоп и цели непосредственно на своей бирже. "
                         "Здесь отслеживается только рыночная цена.")
    else:
        await asyncio.to_thread(add_user_saved_signal, uid, coin, signal_caption(x, uid), res)
        await safe_answer(cb, "Сохранено в избранное")


# =====================================================================
# РЕЗУЛЬТАТЫ СИГНАЛОВ: R, MFE/MAE, живые уведомления
# =====================================================================
R_BY_TP = {0: 0.0, 1: 0.75, 2: 1.8, 3: 2.3}  # TP1 1.5R (50%), TP2 2.5R (30%), TP3 4R (20%); остаток — по стопу


def signal_r(status, max_tp):
    if status == "loss":
        return -1.0
    if status == "win":
        return R_BY_TP.get(max_tp, 0.75)
    return 0.0


def excursion(x, rows, end_ms=None):
    """MFE/MAE в R с момента сигнала, но не позже момента закрытия (end_ms, мс)."""
    risk = abs(x["entry"] - x["sl1"]) or 1e-9
    is_long = x.get("is_long", x.get("side") == "LONG")
    t0 = x["ts"] * 1000
    mfe = mae = 0.0
    for ts, _o, h, l, _c, _v in rows:
        if ts < t0 or (end_ms is not None and ts > end_ms):
            continue
        if is_long:
            mfe = max(mfe, (h - x["entry"]) / risk)
            mae = min(mae, (l - x["entry"]) / risk)
        else:
            mfe = max(mfe, (x["entry"] - l) / risk)
            mae = min(mae, (x["entry"] - h) / risk)
    return round(mfe, 2), round(mae, 2)


async def notify_sig_result(x, status, max_tp):
    r = x.get("r", 0.0)
    head = f"{x['coin']} {x['side']}"
    if status == "win":
        text = f"✅ <b>{head}: {'TP3' if max_tp == 3 else 'TP2' if max_tp == 2 else 'TP1'} взят</b>\nРезультат: <b>{r:+.2f}R</b>"
    elif status == "loss":
        text = f"❌ <b>{head}: стоп</b>\nРезультат: <b>{r:+.2f}R</b>. Риск по идее закрыт."
    elif status == "missed":
        text = f"🚫 <b>{head}: цель взята без входа</b>. Идея упущена, новых входов не ищем."
    else:
        text = f"⌛ <b>{head}: вход не исполнился за срок</b>. Идея закрыта без результата."
    await notify_signal_holders(x["coin"], x["side"], text, kb=kb_nav(back=False), extra_uids=x.get("sent_to", []),
                                order_key=_tracked_order_key(x["coin"], sig_as_res(x)))


async def global_signal_monitor_loop():
    """Итоги и промежуточные события по свечам 15m: вход, TP1-3, стоп, MFE/MAE, R."""
    await asyncio.sleep(30)
    while True:
        try:
            hist = load_signal_history()
            changed = False
            rows_cache = {}
            for x in hist:
                if x.get("status") not in ("active", "in_trade"):
                    continue
                coin = x["coin"]
                if coin not in rows_cache:
                    rows_cache[coin] = await fetch_history(coin, "15m", 400, min_rows=40)
                    await asyncio.sleep(0.2)
                rows = rows_cache[coin]
                if not rows:
                    continue
                status, max_tp, hit_sl = simulate_signal(x, rows)
                end_ms = time.time() * 1000 if status in ("win", "loss", "missed", "expired") else None
                mfe, mae = excursion(x, rows, end_ms)
                if mfe > x.get("mfe", 0) or mae < x.get("mae", 0):
                    x["mfe"], x["mae"] = max(mfe, x.get("mfe", 0)), min(mae, x.get("mae", 0))
                    changed = True
                prev_status, prev_tp = x["status"], x.get("max_tp_hit", 0)
                if (status, max_tp, hit_sl) == (prev_status, prev_tp, x.get("hit_sl", 0)):
                    continue
                x.update(status=status, max_tp_hit=max_tp, hit_sl=hit_sl)
                changed = True
                if prev_status == "active" and status == "in_trade":
                    await notify_signal_holders(coin, x["side"], f"🎯 <b>{coin} {x['side']}: лимит исполнен</b>\n"
                                                                 f"Вход <code>{fmt(x['entry'])}</code>. Ведём до TP1.",
                                                kb=kb_nav(back=False), extra_uids=x.get("sent_to", []),
                                                order_key=_tracked_order_key(coin, sig_as_res(x)))
                if max_tp > prev_tp and status == "in_trade":
                    await notify_signal_holders(coin, x["side"], f"🎯 <b>{coin} {x['side']}: достигнут TP{max_tp}</b>\n"
                                                                 "Стоп переносится в безубыток только после подтверждённого движения цены на +2,5%.",
                                                kb=kb_nav(back=False), extra_uids=x.get("sent_to", []),
                                                order_key=_tracked_order_key(coin, sig_as_res(x)))
                if status in ("win", "loss", "missed", "expired") and prev_status != status:
                    x["closed_ts"] = time.time()
                    x["r"] = signal_r(status, max_tp)
                    await notify_sig_result(x, status, max_tp)
                    await notify_admin(f"📊 <b>{coin} {x['side']}:</b> {status} · {x['r']:+.2f}R (TP {max_tp})")
                    asyncio.create_task(send_webhook_all({"event": f"closed_{status}", "coin": coin, "side": x["side"], "r": x["r"]}))
            if changed:
                save_signal_history(hist)
        except Exception as e:
            log.error(f"global_signal_monitor: {e}", exc_info=True)
        await asyncio.sleep(30)


async def invalidate_signal(x, reason):
    """Сигнал больше не ликвиден: снимаем из списков и сообщаем получателям и держателям."""
    x["status"] = "cancelled"
    x["cancel_reason"] = reason
    x["closed_ts"] = time.time()
    x["r"] = 0.0
    text = (f"🚫 <b>Сигнал снят: {x['coin']} ({x['side']})</b>\n\n{reason}\n"
            f"Вход был <code>{fmt(x['entry'])}</code>. Не входите по этой идее.")
    await notify_signal_holders(x["coin"], x["side"], text, kb=webapp_kb(x["coin"]), extra_uids=x.get("sent_to", []),
                                order_key=_tracked_order_key(x["coin"], sig_as_res(x)))
    for uid, u in list(_users().items()):
        pos = (u.get("open_positions") or {}).get(x["coin"])
        if pos and pos.get("waiting_entry") and pos["side"] == x["side"]:
            try:
                await _tell(int(uid), f"🚫 <b>ВАЖНО: вход по {x['coin']} ({x['side']}) отменён</b>\\n"
                                     f"{reason}\\n"
                                     "Ожидающий виртуальный ордер снят из сопровождения. "
                                     "Если вы выставляли реальный лимит на бирже, отмените его там вручную.")
            except Exception as exc:
                log.warning("Не удалось уведомить %s об отмене %s: %s", uid, x["coin"], exc)
            _remember_closed_order(int(uid), pos)
            set_user_open_position(int(uid), x["coin"], None)


# =====================================================================
# КОЛЛ ДНЯ (ежедневно 08:00 МСК)
# =====================================================================
daily_call_state = {"date": None}


async def daily_call_loop():
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=8, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        try:
            today = datetime.now(timezone(timedelta(hours=3))).date().isoformat()
            cands = get_active_signals()
            if cands and daily_call_state["date"] != today:
                best = max(cands, key=lambda x: x["strength"])
                best["daily"] = True
                closes = await _closes(best["coin"])
                lv = LIVE.get(best["coin"])
                price = lv["price"] if lv else best["entry"]
                card = await asyncio.to_thread(render_signal_card, best, closes, price, "КОЛЛ ДНЯ")
                for cid in signal_recipients(best["coin"], best["side"]):
                    try:
                        await _send_new(cid, signal_caption(best, cid, "КОЛЛ ДНЯ"), signal_keyboard(best), card)
                        best["sent_to"].append(cid)
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
                daily_call_state["date"] = today
                state_touch()
        except Exception as e:
            log.error(f"daily_call: {e}", exc_info=True)
        await asyncio.sleep(60)


# =====================================================================
# АНАЛИТИКА СИГНАЛОВ
# =====================================================================
def _sig_closed_all():
    with STATE_LOCK:
        hist = list(STATE["signal_history"])
    out = []
    for x in hist:
        if x.get("status") in ("win", "loss"):
            if x.get("r") is None:
                x["r"] = signal_r(x["status"], x.get("max_tp_hit", 0))
            out.append(x)
    return sorted(out, key=lambda x: x.get("closed_ts", x["ts"]))


def _agg(items):
    n = len(items)
    if not n:
        return None
    wins = sum(1 for x in items if x["status"] == "win")
    rs = [x.get("r", 0.0) for x in items]
    gp = sum(r for r in rs if r > 0)
    gl = -sum(r for r in rs if r < 0)
    return {"n": n, "wr": round(wins / n * 100, 1), "avg_r": round(sum(rs) / n, 2),
            "sum_r": round(sum(rs), 2), "pf": round(gp / gl, 2) if gl else None}


def analytics_summary():
    with STATE_LOCK:
        allsig = list(STATE["signal_history"])
    closed = _sig_closed_all()
    created = len(allsig)
    cnt = {s: sum(1 for x in allsig if x.get("status") == s) for s in
           ("win", "loss", "missed", "expired", "cancelled", "active", "in_trade")}
    entered = cnt["win"] + cnt["loss"] + cnt["in_trade"]
    total = _agg(closed)
    curve, cum, peak, dd = [], 0.0, 0.0, 0.0
    for x in closed:
        cum += x["r"]
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
        curve.append(round(cum, 2))
    mw = ml = cw = cl = 0
    for x in closed:
        if x["status"] == "win":
            cw, cl = cw + 1, 0
        else:
            cl, cw = cl + 1, 0
        mw, ml = max(mw, cw), max(ml, cl)
    bands = [(0, 60, "50–60%"), (60, 70, "60–70%"), (70, 80, "70–80%"), (80, 101, "80%+")]
    calib = []
    for lo, hi, label in bands:
        grp = [x for x in closed if lo <= x["strength"] < hi]
        if grp:
            a = _agg(grp)
            a["label"] = label
            a["avg_conf"] = round(sum(x["strength"] for x in grp) / len(grp), 1)
            calib.append(a)
    sessions = {"день 08–22": [x for x in closed if 8 <= x.get("hour", 12) < 22],
                "ночь 22–08": [x for x in closed if not (8 <= x.get("hour", 12) < 22)]}
    coins = {}
    for x in closed:
        coins.setdefault(x["coin"], []).append(x)
    coin_rows = sorted([(c, _agg(v)) for c, v in coins.items()], key=lambda kv: kv[1]["sum_r"], reverse=True)
    reasons = {}
    for x in closed:
        for rsn in set(x.get("reasons") or []):
            reasons.setdefault(rsn, []).append(x)
    reason_rows = [(k, _agg(v)) for k, v in reasons.items() if len(v) >= 3]
    reason_rows.sort(key=lambda kv: kv[1]["avg_r"], reverse=True)
    thresholds = [(t, _agg([x for x in closed if x["strength"] >= t])) for t in (60, 65, 70, 75, 80)]
    thresholds = [(t, a) for t, a in thresholds if a]
    mfe = round(sum(x.get("mfe", 0) for x in closed) / len(closed), 2) if closed else 0.0
    mae = round(sum(x.get("mae", 0) for x in closed) / len(closed), 2) if closed else 0.0
    hold = round(sum((x.get("closed_ts", x["ts"]) - x["ts"]) for x in closed) / len(closed) / 3600, 1) if closed else 0.0
    daily = _agg([x for x in closed if x.get("daily")])
    return {"created": created, "cnt": cnt, "entered": entered,
            "fill_rate": round(entered / created * 100, 1) if created else 0.0,
            "miss_rate": round(cnt["missed"] / (entered + cnt["missed"]) * 100, 1) if (entered + cnt["missed"]) else 0.0,
            "total": total, "curve": curve[-120:], "max_dd": round(dd, 2), "streak_w": mw, "streak_l": ml,
            "bands": calib, "sessions": {k: _agg(v) for k, v in sessions.items()},
            "types": {t: _agg([x for x in closed if x["type"] == t]) for t in ("Краткосрочный", "Среднесрочный", "Долгосрочный")},
            "sides": {s: _agg([x for x in closed if x["side"] == s]) for s in ("LONG", "SHORT")},
            "coins_top": coin_rows[:5], "coins_bottom": coin_rows[-5:][::-1] if len(coin_rows) > 5 else [],
            "reasons_top": reason_rows[:6], "reasons_bottom": reason_rows[-3:][::-1] if len(reason_rows) > 6 else [],
            "thresholds": thresholds, "mfe": mfe, "mae": mae, "hold_h": hold, "daily": daily}


def _fa(a):
    if not a:
        return "нет данных"
    pf = "—" if a["pf"] is None else a["pf"]
    return f"{a['n']} сд · WR <b>{a['wr']}%</b> · ср. <b>{a['avg_r']:+.2f}R</b> · Σ {a['sum_r']:+.2f}R · PF {pf}"


def analytics_text(s):
    t = s["total"]
    if not t:
        return ("📊 <b>Аналитика сигналов</b>\n\nЗакрытых сигналов пока нет. Данные появятся после первых результатов: "
                f"создано {s['created']}, активных {s['cnt']['active'] + s['cnt']['in_trade']}.")
    lines = ["📊 <b>Аналитика сигналов IXCLY</b>", "",
             f"Создано: <b>{s['created']}</b> · вошли: <b>{s['entered']}</b> ({s['fill_rate']}%) · упущено: {s['miss_rate']}%",
             f"✅ {s['cnt']['win']} · ❌ {s['cnt']['loss']} · ⌛ {s['cnt']['expired']} · 🚫 {s['cnt']['cancelled']}", "",
             f"<b>Итог по закрытым:</b> {_fa(t)}",
             f"Макс. просадка: <b>{s['max_dd']:.2f}R</b> · серии: победы {s['streak_w']}, убытки {s['streak_l']}",
             f"Средний MFE <b>{s['mfe']:+.2f}R</b> / MAE <b>{s['mae']:+.2f}R</b> · удержание {s['hold_h']} ч", "",
             "<b>Калибровка по уверенности</b> (проверка, что Conf честный):"]
    for b in s["bands"]:
        lines.append(f"• {b['label']} (ср. {b['avg_conf']}%): WR <b>{b['wr']}%</b>, {b['avg_r']:+.2f}R, {b['n']} сд")
    lines += ["", "<b>Порог уверенности</b> (что было бы, если фильтровать):"]
    for th, a in s["thresholds"]:
        lines.append(f"• от {th}%: WR {a['wr']}% · {a['avg_r']:+.2f}R · {a['n']} сд")
    lines += ["", "<b>Типы:</b>"]
    lines += [f"• {k}: {_fa(v)}" for k, v in s["types"].items() if v]
    lines += ["", f"<b>Лонг</b>: {_fa(s['sides']['LONG'])}", f"<b>Шорт</b>: {_fa(s['sides']['SHORT'])}"]
    for k, a in s["sessions"].items():
        if a:
            lines.append(f"<b>{k}</b>: WR {a['wr']}% · {a['avg_r']:+.2f}R · {a['n']} сд")
    if s["daily"]:
        lines.append(f"<b>Коллы дня</b>: WR {s['daily']['wr']}% · {s['daily']['avg_r']:+.2f}R · {s['daily']['n']} сд")
    if s["coins_top"]:
        lines += ["", "<b>Лучшие монеты (Σ R):</b> " + ", ".join(f"{c} {a['sum_r']:+.1f}R" for c, a in s["coins_top"])]
    if s["coins_bottom"]:
        lines.append("<b>Худшие монеты:</b> " + ", ".join(f"{c} {a['sum_r']:+.1f}R" for c, a in s["coins_bottom"]))
    if s["reasons_top"]:
        lines += ["", "<b>Факторы, которые работают</b> (ср. R, от 3 сделок):"]
        lines += [f"• {k}: {a['avg_r']:+.2f}R · WR {a['wr']}% · {a['n']} сд" for k, a in s["reasons_top"][:5]]
    if s["reasons_bottom"]:
        lines += ["<b>Факторы, которые ломают:</b>"]
        lines += [f"• {k}: {a['avg_r']:+.2f}R · WR {a['wr']}% · {a['n']} сд" for k, a in s["reasons_bottom"][:3]]
    return "\n".join(lines)


def render_analytics_png(s):
    W, H = 1024, 760
    img = Image.new("RGB", (W, H), (10, 11, 18))
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, W, 8), fill=GOLD)
    d.text((58, 40), "Кривая результата сигналов, R", fill=(245, 247, 255), font=_font(40, True))
    t = s["total"]
    if t:
        d.text((58, 96), f"Σ {t['sum_r']:+.2f}R · WR {t['wr']}% · {t['n']} сделок · просадка {s['max_dd']:.2f}R",
               fill=TEXT_GREY, font=_font(26))
    box = (58, 150, 966, 460)
    curve = s["curve"] or [0]
    vmin, vmax = min(min(curve), 0), max(max(curve), 0.5)
    rng = (vmax - vmin) or 1
    x0, y0, x1, y1 = box
    zero_y = y1 - (0 - vmin) / rng * (y1 - y0)
    d.line((x0, zero_y, x1, zero_y), fill=(60, 66, 96), width=2)
    pts = [(x0 + i * (x1 - x0) / max(1, len(curve) - 1), y1 - (v - vmin) / rng * (y1 - y0)) for i, v in enumerate(curve)]
    if len(pts) > 1:
        d.polygon(pts + [(x1, zero_y), (x0, zero_y)], fill=(18, 48, 52) if curve[-1] >= 0 else (50, 20, 30))
        d.line(pts, fill=(45, 226, 198) if curve[-1] >= 0 else (255, 90, 110), width=4)
    d.text((58, 480), "Калибровка: уверенность → фактический винрейт", fill=(245, 247, 255), font=_font(30, True))
    bands = s["bands"]
    bx0, bw = 90, 200
    for i, b in enumerate(bands):
        x = bx0 + i * 225
        hgt = int(b["wr"] / 100 * 200)
        d.rectangle((x, 920 - 200, x + bw, 920 - 200 + 200), outline=(40, 46, 70), width=2)
        d.rectangle((x, 920 - hgt, x + bw, 920), fill=(96, 136, 255))
        d.text((x, 935), b["label"], fill=TEXT_GREY, font=_font(24))
        d.text((x, 720 - hgt), f"{b['wr']}%", fill=(245, 247, 255), font=_font(26, True))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


@dp.message(Command("analytics"))
async def cmd_analytics(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    if m.from_user.id not in ADMIN_IDS and m.from_user.id not in active_chats:
        return
    s = await asyncio.to_thread(analytics_summary)
    await _send_new(m.chat.id, analytics_text(s), kb_nav(back=False), None)
    if s["total"]:
        png = await asyncio.to_thread(render_analytics_png, s)
        await bot.send_photo(m.chat.id, BufferedInputFile(png, "analytics.png"), caption="📈 Кривая результата и калибровка")






# =====================================================================
# ДОСТУП: пробный период, подписка (Telegram Stars), промокоды, бесплатный доступ, реклама
# =====================================================================
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))
# Более понятная лестница цен в Telegram Stars: выгоднее при оплате за срок.
PLANS = {30: 299, 90: 799, 365: 2499}  # дни: цена в Telegram Stars (XTR)
PLAN_TITLE = {30: "Месяц", 90: "3 месяца", 365: "Год"}
FEE_PCT = 0.1                            # комиссия туда-обратно, % от позиции


def discount_offer_for(uid):
    """Возвращает лучшее действующее персональное предложение пользователя."""
    now = time.time()
    user = _users().get(str(uid), {})
    offer = user.get("welcome_offer") or {}
    candidates = []
    if (not offer.get("used") and int(offer.get("pct", 0) or 0) > 0
            and float(offer.get("until", 0) or 0) > now):
        candidates.append(offer)
    # Старое предложение возврата сохраняем для совместимости.
    legacy = DISCOUNT_OFFERS.get(int(uid)) or DISCOUNT_OFFERS.get(uid)
    if legacy and float(legacy.get("until", 0) or 0) > now:
        candidates.append(legacy)
    return max(candidates, key=lambda x: int(x.get("pct", 0) or 0)) if candidates else None


def price_for_user(uid, days):
    base = int(PLANS[days])
    offer = discount_offer_for(uid)
    if not offer:
        return base
    pct = max(0, min(80, int(offer.get("pct", 0) or 0)))
    return max(1, int(round(base * (100 - pct) / 100)))


def plan_prices_for_user(uid):
    offer = discount_offer_for(uid)
    pct = max(0, min(80, int(offer.get("pct", 0) or 0))) if offer else 0
    until = int(offer.get("until", 0)) if offer else 0
    kind = offer.get("kind", "offer") if offer else ""
    return [{"days": d, "stars": price_for_user(uid, d), "original_stars": s,
             "discount_pct": pct, "offer_until": until,
             "offer_label": ("Скидка 20% за приглашение" if kind == "referral" else
                             "Приветственная скидка 10%" if kind == "welcome" else
                             f"Персональная скидка {pct}%" if pct else "") ,
             "title": PLAN_TITLE[d]} for d, s in PLANS.items()]
FREE_ACTS = {"me", "access", "promo_apply", "support", "order_status"}
TG_SEM = None                            # создаётся в main (не привязано к event loop при импорте)
COIN_RL = {}                             # uid -> ts последнего пересчёта монеты
_ob_cache = {}
_hourly_cache = {}
ADS_DEFAULT = {"on": False, "text": "", "url": "", "contact": "@ixcly"}


def ads_get():
    a = dict(ADS_DEFAULT)
    a.update(STATE.get("ads") or {})
    return a


def _init_trial(uid):
    """Новому пользователю — пробный период; старым (до обновления) тоже даём, если доступа ещё не было."""
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(uid), {"active": True})
        if "access" not in u:
            u["access"] = {"until": time.time() + TRIAL_DAYS * 86400, "kind": "trial", "free": False, "paid_ever": False}
    state_touch(False)


def has_access(uid):
    if int(uid) in ADMIN_IDS:
        return True
    a = _users().get(str(uid), {}).get("access") or {}
    if a.get("free"):
        return True
    return time.time() < a.get("until", 0)


def grant_days(uid, days, kind):
    with STATE_LOCK:
        u = STATE["users"].setdefault(str(uid), {"active": True})
        a = u.setdefault("access", {"until": 0, "kind": kind, "free": False, "paid_ever": False})
        if days >= 36500 or days == -1:
            a["until"] = 9999999999
            a["kind"] = "lifetime"
            a["free"] = True
        elif a.get("until", 0) > 9000000000:
            pass
        else:
            now_ts = time.time()
            base = max(now_ts, a.get("until", 0))
            # Бонусные дни (промокод/реферал/админ) не должны «понижать» действующую оплаченную подписку
            keep_paid = a.get("kind") == "paid" and a.get("until", 0) > now_ts and kind != "paid"
            a["until"] = base + days * 86400
            if not keep_paid:
                a["kind"] = kind
            a["free"] = False
        if kind == "paid":
            a["paid_ever"] = True
    state_touch()


def access_payload(uid):
    """Единое описание доступа для бота и Mini App (старые поля сохранены для совместимости)."""
    a = _users().get(str(uid), {}).get("access") or {}
    is_admin = int(uid) in ADMIN_IDS
    now = time.time()
    snap = billing.access_snapshot(a, now, is_admin)
    ok = has_access(uid)
    status = {"admin": "Админ", "lifetime": "Навсегда", "trial": "пробный период",
              "active": "подписка", "expired": "закрыт"}[snap["code"]]
    is_vip = is_admin or bool(a.get("free")) or a.get("kind") in ("paid", "lifetime")
    ad = ads_get()
    show_ad = bool(ad.get("on") and not is_vip)
    offer = discount_offer_for(uid)
    plans = plan_prices_for_user(uid)
    for p in plans:
        # Дата окончания ПОСЛЕ покупки — показываем в окне подтверждения до оплаты
        p["new_until"] = billing.new_expiry(a, p["days"], now)
    return {"ok": ok, "status": status, "code": snap["code"], "label": snap["label"], "hint": snap["hint"],
            "days_left": snap["days_left"], "hours_left": snap["hours_left"],
            "until": a.get("until", 0), "free": bool(a.get("free")), "lifetime": snap["lifetime"], "vip": is_vip,
            "can_buy": snap["can_buy"], "paid_ever": bool(a.get("paid_ever")),
            "plans": plans,
            "offer": ({"pct": int(offer.get("pct", 0)), "until": int(offer.get("until", 0)),
                       "label": plans[0].get("offer_label", "")} if offer else None),
            "risk": billing.RISK_TEXT, "risk_title": billing.RISK_TITLE,
            "ad": ad["text"] if show_ad else "",
            "ad_url": ad["url"] if show_ad else ""}


def access_closed_text(uid):
    a = access_payload(uid)
    if a["status"] == "закрыт" or not a["ok"]:
        return "🔒 Доступ закрыт. Откройте раздел «Подписка» и выберите тариф (или команда /buy в боте)."
    return "Доступ закрыт"


def ad_block():
    a = ads_get()
    if not a["on"] or not a["text"]:
        return ""
    link = f"\n🔗 {html.escape(a['url'])}" if a["url"] else ""
    return f"\n\n📢 <i>{html.escape(a['text'])}</i>{link}\nРеклама: {html.escape(a['contact'])}"


def _fmt_msk(ts, with_time=False):
    """Дата по МСК — единый формат дат подписки в боте."""
    try:
        d = datetime.fromtimestamp(float(ts), timezone(timedelta(hours=3)))
        return d.strftime("%d.%m.%Y %H:%M МСК" if with_time else "%d.%m.%Y")
    except Exception:
        return "—"


def risk_html():
    """Компактное предупреждение о рисках (единый текст из ixcly_billing)."""
    return f"⚠️ <b>{billing.RISK_TITLE}</b>\n<i>{html.escape(billing.RISK_TEXT)}</i>"


def _plan_line(p):
    price = f"<b>{p['stars']}⭐</b>" + (f" <s>{p['original_stars']}⭐</s>" if p.get("discount_pct") else "")
    return f"• {html.escape(p['title'])} · {p['days']} дн. — {price}"


def subscription_text(uid):
    """Экран «Подписка»: статус, срок и тарифы. Доступен и при активной подписке (продление)."""
    a = access_payload(uid)
    lines = ["💎 <b>Подписка IXCLY PRO</b>", "", f"Статус: <b>{html.escape(a['label'])}</b>"]
    if not a["lifetime"]:
        if a["ok"]:
            lines.append(f"Действует до: <b>{_fmt_msk(a['until'], True)}</b> · осталось {a['days_left']} дн.")
        elif a["until"]:
            lines.append(f"Закончилась: {_fmt_msk(a['until'])}")
    if a.get("offer"):
        lines.append(f"🎁 <b>{html.escape(a['offer']['label'])}</b> — действует ограниченное время.")
    lines.append(html.escape(a["hint"]))
    if a["can_buy"]:
        lines += ["", "<b>Тарифы</b> (оплата Telegram Stars ⭐):"] + [_plan_line(p) for p in a["plans"]]
    lines += ["", risk_html()]
    return "\n".join(lines)


def subscription_kb(uid):
    a = access_payload(uid)
    rows = []
    if a["can_buy"]:
        for p in a["plans"]:
            rows.append([InlineKeyboardButton(text=f"💳 {p['title']} · {p['stars']}⭐", callback_data=f"buy:{p['days']}")])
        rows.append([InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo:help")])
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🚀 Подписка в приложении", web_app=WebAppInfo(url=webapp_link(tab="sub")))])
    rows.append([InlineKeyboardButton(text="📘 Инструкция и риски", callback_data="guide:open")])
    rows.append([InlineKeyboardButton(text="🆘 Поддержка", callback_data="support_menu")])
    return kb_nav(rows, back=False)


async def show_subscription(chat_id, uid, edit_msg=None):
    return await present(chat_id, subscription_text(uid), subscription_kb(uid), edit_msg=edit_msg)


def paywall_kb(uid=None):
    """Тарифы для закрытого доступа (напоминания, /start без доступа)."""
    rows = [[InlineKeyboardButton(text=f"💳 {PLAN_TITLE[d]} · {price_for_user(uid, d) if uid is not None else s}⭐", callback_data=f"buy:{d}")] for d, s in PLANS.items()]
    rows.append([InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo:help")])
    rows.append([InlineKeyboardButton(text="📰 IXCLY NEWS · Новости крипторынка", url=NEWS_CHANNEL_URL)])
    rows.append([InlineKeyboardButton(text="📘 Инструкция и риски", callback_data="guide:open")])
    rows.append([InlineKeyboardButton(text="🆘 Поддержка", callback_data="support_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def buy_confirm_text(uid, days):
    """Подтверждение ДО оплаты: тариф, срок, стоимость и дата окончания."""
    a = access_payload(uid)
    p = next((x for x in a["plans"] if x["days"] == days), None)
    if not p:
        return None
    price = f"<b>{p['stars']}⭐</b>" + (f" <s>{p['original_stars']}⭐</s>" if p.get("discount_pct") else "")
    how = "продлит текущий срок" if a["ok"] else "откроет доступ сразу после оплаты"
    return ("🧾 <b>Подтверждение оплаты</b>\n\n"
            f"Тариф: <b>{html.escape(p['title'])}</b>\nСрок: <b>{p['days']} дн.</b>\nСтоимость: {price}\n"
            f"Доступ до: <b>{_fmt_msk(p.get('new_until'))}</b> ({how})\n\n"
            "Оплата выполняется Telegram Stars (⭐). Списание произойдёт только после подтверждения в окне Telegram.\n\n"
            + risk_html())


async def show_buy_confirm(chat_id, uid, days, edit_msg=None):
    a = access_payload(uid)
    if not a["can_buy"]:
        return await present(chat_id, "ℹ️ " + billing.ERROR_TEXT["lifetime"], subscription_kb(uid), edit_msg=edit_msg)
    text = buy_confirm_text(uid, days)
    if not text:
        return await show_subscription(chat_id, uid, edit_msg=edit_msg)
    price = price_for_user(uid, days)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Оплатить {price}⭐", callback_data=f"pay:{days}")],
        [InlineKeyboardButton(text="⬅ К тарифам", callback_data="sub:open"), InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    return await present(chat_id, text, kb, edit_msg=edit_msg)


GUIDE_TEXT = ("📘 <b>Инструкция IXCLY PRO</b>\n\n"
              "1. Откройте приложение кнопкой «🚀 Открыть IXCLY App».\n"
              "2. «Сигналы» — идеи, прошедшие фильтры сканера. Нажмите на монету: вход, цели TP, Stop Loss и причины.\n"
              "3. «Портфель» — укажите депозит и риск на сделку. Paper Trading — виртуальный счёт без реальных ордеров.\n"
              "4. «Аналитика» — ваши результаты и статистика системы.\n"
              "5. «Подписка» — статус доступа, тарифы и оплата Telegram Stars.\n"
              "6. Уведомления включаются в боте (🔔) и в разделе «Ещё».\n\n"
              "<b>Безопасный порядок:</b> изучить рынок → прочитать причины сигнала → проверить вход, TP, SL и риск → "
              "при сомнениях пропустить идею.\n\n")


async def show_guide(chat_id, uid, edit_msg=None):
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🚀 Открыть IXCLY App", web_app=WebAppInfo(url=webapp_link()))])
    rows.append([InlineKeyboardButton(text="🎓 Обучение новичка", callback_data="tutorial:start")])
    rows.append([InlineKeyboardButton(text="💎 Подписка", callback_data="sub:open")])
    return await present(chat_id, GUIDE_TEXT + risk_html(), kb_nav(rows, back=False), edit_msg=edit_msg)


async def send_plan_invoice(uid, days):
    """Создаёт сохранённый заказ и отправляет счёт. Сумма и тариф берутся из заказа, а не от клиента."""
    if days not in PLANS:
        return await _send_new(uid, "❌ Такого тарифа нет. Откройте раздел «Подписка» и выберите тариф заново.",
                               kb_nav(back=False), None)
    if not access_payload(uid)["can_buy"]:
        return await _send_new(uid, "ℹ️ " + billing.ERROR_TEXT["lifetime"], kb_nav(back=False), None)
    stars = price_for_user(uid, days)
    try:
        with STATE_LOCK:
            order = billing.create_order(STATE, uid, days, stars, PLANS, source="bot")
        state_touch()
        await bot.send_invoice(chat_id=uid, title=f"IXCLY PRO · {PLAN_TITLE[days]}",
                               description=f"Доступ к сигналам, сопровождению сделок и аналитике на {days} дн.",
                               payload=billing.payload_for(order), currency="XTR",
                               prices=[LabeledPrice(label=f"{PLAN_TITLE[days]}", amount=stars)], provider_token="")
    except Exception as e:
        log.warning("send_invoice %s: %s", uid, e)
        await _send_new(uid, "⚠️ Не удалось создать счёт. Деньги не списаны. Попробуйте ещё раз через минуту "
                             "или напишите в поддержку.", paywall_kb(uid), None)


def hourly_rows_cached_key(coin):
    return coin


async def hourly_rows(coin):
    """1h свечи для Chandelier: кэш на 60 секунд, чтобы не бить по бирже на каждую сделку."""
    ent = _hourly_cache.get(coin)
    if ent and time.time() - ent[0] < 60:
        return ent[1]
    rows = await fetch_history(coin, "1h", 60, min_rows=30)
    _hourly_cache[coin] = (time.time(), rows)
    _trim_timed_cache(_hourly_cache, 80)
    return rows


def normalize_signals():
    """Старые сигналы без поля is_long (до обновления) — дополняем, чтобы монитор не падал."""
    with STATE_LOCK:
        for x in STATE["signal_history"]:
            if isinstance(x, dict):
                x.setdefault("is_long", x.get("side") == "LONG")
                x.setdefault("sl1", x.get("sl1", 0.0))


async def access_reminder_loop():
    """Напоминания об оплате в 12:00 и 20:00 МСК: пробный период и просроченный доступ (тем, кто не платил)."""
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        slots = [msk.replace(hour=h, minute=0, second=0, microsecond=0) for h in (12, 20)]
        nxt = next((t for t in slots if t > msk), slots[0] + timedelta(days=1))
        await asyncio.sleep((nxt - msk).total_seconds())
        for uid_str, u in list(_users().items()):
            try:
                uid = int(uid_str)
                if not isinstance(u, dict) or uid in ADMIN_IDS:
                    continue
                a = u.get("access") or {}
                if a.get("free"):
                    continue
                left = a.get("until", 0) - time.time()
                if a.get("kind") == "trial":
                    if 0 < left <= 2 * 86400:
                        text = f"⏳ <b>Пробный доступ заканчивается через {int(left // 3600)} ч.</b>\nОплатите подписку, чтобы сигналы не прерывались."
                    elif left <= 0:
                        text = "🔒 <b>Пробный доступ закончился.</b> Сигналы и сопровождение отключены. Оплатите подписку, чтобы вернуть доступ."
                    else:
                        continue
                elif left <= 0 and not a.get("paid_ever"):
                    text = "🔒 <b>Доступ закрыт.</b> Оплатите подписку, чтобы вернуть сигналы."
                else:
                    continue
                await _tell(uid, text, paywall_kb(uid))
                await asyncio.sleep(0.05)
            except Exception as e:
                log.info(f"reminder {uid_str}: {e}")


def _purge_api_cache(uid):
    """После оплаты сбрасываем кэш API пользователя, чтобы Mini App сразу увидел новый статус."""
    try:
        for k in [k for k in list(API_CACHE.keys()) if k and k[0] == uid]:
            API_CACHE.pop(k, None)
    except Exception:
        pass


async def _refund_stars(uid, charge_id):
    """Возврат Stars (нужен aiogram с refund_star_payment). Возвращает True при успехе."""
    fn = getattr(bot, "refund_star_payment", None)
    if fn is None:
        return False
    try:
        ok = bool(await fn(user_id=int(uid), telegram_payment_charge_id=str(charge_id)))
        if ok:
            with STATE_LOCK:
                rec = STATE.get("payments", {}).get(str(charge_id))
                if rec:
                    rec["refunded"] = True
            state_touch()
        return ok
    except Exception as e:
        log.warning("refund_star_payment %s: %s", charge_id, e)
        return False


async def pre_checkout_handler(q: types.PreCheckoutQuery):
    """Проверка ДО списания: пользователь, заказ, сумма, валюта, повторная оплата, бессрочный доступ."""
    try:
        kind = billing.parse_payload(q.invoice_payload)
        if kind[0] == "ord":
            with STATE_LOCK:
                ok, code, _ = billing.validate_payment(STATE, kind[1], q.from_user.id, q.currency, q.total_amount, PLANS)
        elif kind[0] == "legacy":
            days, stars = kind[1], kind[2]
            ok = (q.currency == billing.CURRENCY and days in PLANS and 1 <= int(q.total_amount) <= int(PLANS[days])
                  and (stars is None or stars == int(q.total_amount)))
            code = "ok" if ok else "amount_mismatch"
        else:
            ok, code = False, "unknown_order"
        if ok and not access_payload(q.from_user.id)["can_buy"]:
            ok, code = False, "lifetime"
        if ok:
            await q.answer(ok=True)
        else:
            log.info("pre_checkout отклонён uid=%s code=%s", q.from_user.id, code)
            await q.answer(ok=False, error_message=billing.ERROR_TEXT.get(code, "Платёж не прошёл проверку. Попробуйте ещё раз."))
    except Exception as e:
        log.warning(f"pre_checkout: {e}")
        try:
            await q.answer(ok=False, error_message="Не удалось проверить платёж. Деньги не списаны, попробуйте ещё раз.")
        except Exception:
            pass


@dp.pre_checkout_query()
async def on_pre_checkout(q: types.PreCheckoutQuery):
    await pre_checkout_handler(q)


@dp.message(F.successful_payment)
async def on_paid(m: types.Message):
    """Успешная оплата: идемпотентно (charge_id), с проверкой пользователя/тарифа/суммы и немедленным сохранением."""
    sp = m.successful_payment
    uid = m.from_user.id
    charge_id = sp.telegram_payment_charge_id
    first_payment, ref, lifetime_skip = False, None, False
    with STATE_LOCK:
        res = billing.settle_payment(STATE, sp.invoice_payload, uid, charge_id, sp.currency, sp.total_amount, PLANS)
        if res["status"] == "ok":
            buyer = _users().get(str(uid), {})
            if billing.is_lifetime(buyer.get("access")):
                lifetime_skip = True        # доступ уже бессрочный — начислять нечего, вернём Stars
            else:
                first_payment = not (buyer.get("access") or {}).get("paid_ever")
                ref = buyer.get("referrer")
                grant_days(uid, res["order"]["days"], "paid")
                if buyer.get("welcome_offer"):
                    buyer["welcome_offer"]["used"] = True
                DISCOUNT_OFFERS.pop(uid, None)
    status = res["status"]
    if status == "duplicate":
        log.info("Повторная доставка платежа %s пропущена", charge_id)
        return
    state_touch()
    # Деньги → сразу на диск: рестарт между оплатой и записью не приведёт к потере или двойному начислению
    await asyncio.to_thread(flush_state)
    _purge_api_cache(uid)

    if status == "ok" and not lifetime_skip:
        order = res["order"]
        if ref and first_payment:
            grant_days(ref, 7, "referral")
            await _tell(ref, "🎁 Ваш друг оплатил подписку. <b>+7 дней</b> начислено вам.")
        a = access_payload(uid)
        until_line = "Подписка: <b>бессрочно</b>" if a["lifetime"] else f"Подписка активна до: <b>{_fmt_msk(a['until'], True)}</b>"
        rows = [[InlineKeyboardButton(text="🚀 Открыть IXCLY App", web_app=WebAppInfo(url=webapp_link()))]] if WEBAPP_URL else []
        rows.append([InlineKeyboardButton(text="💎 Моя подписка", callback_data="sub:open")])
        await present(m.chat.id, "✅ <b>Оплата получена</b>\n\n"
                                 f"Тариф: <b>{html.escape(PLAN_TITLE.get(order['days'], str(order['days']) + ' дн.'))}</b> · {order['days']} дн.\n"
                                 f"Списано: <b>{order['stars']}⭐</b>\n{until_line}\n"
                                 f"Заказ: <code>{order['id']}</code>\n\nСпасибо! Сигналы и сопровождение сделок доступны.",
                      kb_nav(rows, back=False))
        for aid in ADMIN_IDS:
            try:
                await bot.send_message(aid, f"💰 Оплата: {uid} · {order['days']} дн. · {sp.total_amount}⭐ · заказ {order['id']} · {str(charge_id)[:14]}…")
            except Exception:
                pass
        return

    # Платёж не засчитан (повторная оплата заказа, чужой заказ, неверная сумма, бессрочный доступ)
    code = "lifetime" if lifetime_skip else res.get("code", "unknown_order")
    log.error("Платёж не начислен uid=%s code=%s charge=%s payload=%s", uid, code, charge_id, sp.invoice_payload)
    refunded = await _refund_stars(uid, charge_id)
    tail = ("Stars возвращены автоматически." if refunded else
            "Платёж передан на проверку. Если Stars не вернутся автоматически, напишите в поддержку и укажите номер платежа:\n"
            f"<code>{html.escape(str(charge_id))}</code>")
    await _tell(uid, "⚠️ <b>Платёж получен, но подписка не начислена</b>\n"
                     f"{html.escape(billing.ERROR_TEXT.get(code, 'Платёж не удалось сопоставить с заказом.'))}\n\n{tail}",
                paywall_kb(uid))
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, f"⚠️ Платёж не начислен: uid {uid} · {code} · {sp.total_amount}⭐ · "
                                        f"возврат: {'да' if refunded else 'НЕТ, нужен вручную'} · charge {charge_id}")
        except Exception:
            pass


@dp.message(Command("buy", "subscribe"))
async def cmd_buy(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    await show_subscription(m.chat.id, m.from_user.id)


@dp.message(Command("guide", "instruction", "disclaimer"))
async def cmd_guide(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    await show_guide(m.chat.id, m.from_user.id)


def account_age_hours(uid):
    try:
        joined = _users().get(str(uid), {}).get("joined_at")
        return (datetime.now() - datetime.strptime(joined, "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600
    except Exception:
        return 999.0


def apply_promo(uid, code):
    """Промокод. Аккаунт младше 24 часов получает максимум 1 день — против накрутки твинков."""
    code = code.strip().upper()
    with STATE_LOCK:
        promo = STATE["promos"].get(code)
        if not promo or promo.get("uses_left", 0) <= 0 or (promo.get("expires") and time.time() > promo["expires"]):
            return "❌ Промокод не найден или закончился."
        if uid in promo.get("used", []):
            return "ℹ Вы уже использовали этот промокод."
        days, note = promo["days"], ""
        if account_age_hours(uid) < 24 and days > 1:
            days, note = 1, " (новый аккаунт: 1 день)"
        promo["uses_left"] -= 1
        promo.setdefault("used", []).append(uid)
    grant_days(uid, days, "promo")
    state_touch()
    return f"🎉 Промокод активирован: +{days} дн.{note}"


@dp.message(Command("promo"))
async def cmd_promo(m: types.Message):
    try:
        await m.delete()
    except Exception:
        pass
    parts = (m.text or "").split()
    if len(parts) < 2:
        return await _send_new(m.chat.id, "🎟 Введите: <code>/promo КОД</code>", kb_nav(back=False), None)
    await _send_new(m.chat.id, apply_promo(m.from_user.id, parts[1]), kb_nav(back=False), None)


@dp.message(Command("ad_set", "add_set"))
async def main_ad_set(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    body = m.text.split(" ", 1)[1] if " " in (m.text or "") else ""
    ad_text, _, url = body.partition("|")
    ad_text, url = ad_text.strip(), url.strip()
    if not ad_text:
        return await m.answer("Формат: <code>/add_set Текст рекламы | https://example.com</code>", parse_mode="HTML")
    if url and urlparse(url).scheme != "https":
        return await m.answer("Ссылка должна начинаться с https://", parse_mode="HTML")
    with STATE_LOCK:
        STATE["ads"] = dict(ads_get(), on=True, text=ad_text, url=url)
    state_touch()
    await m.answer("📢 Реклама включена: главная страница и раздел сигналов Mini App.")


@dp.message(Command("ad_off"))
async def main_ad_off(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    with STATE_LOCK:
        STATE["ads"] = dict(ads_get(), on=False)
    state_touch()
    await m.answer("📢 Реклама выключена.")


if admin_dp:
    def _is_admin(m):
        return m.from_user.id in ADMIN_IDS

    @admin_dp.message(Command("grant"))
    async def adm_grant(m: types.Message):
        if not _is_admin(m):
            return
        try:
            _, uid, days = m.text.split()
            grant_days(int(uid), int(days), "grant")
            await m.answer(f"✅ Пользователю {uid} добавлено {days} дн.")
        except ValueError:
            await m.answer("Формат: <code>/grant UID ДНИ</code>", parse_mode="HTML")

    @admin_dp.message(Command("lifetime", "forever"))
    async def adm_lifetime(m: types.Message):
        if not _is_admin(m):
            return
        try:
            uid = int(m.text.split()[1])
            grant_days(uid, -1, "lifetime")
            await m.answer(f"✅ Пользователю {uid} выдан доступ НАВСЕГДА.")
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/lifetime UID</code>", parse_mode="HTML")

    @admin_dp.message(Command("revoke"))
    async def adm_revoke(m: types.Message):
        if not _is_admin(m):
            return
        try:
            uid = int(m.text.split()[1])
            with STATE_LOCK:
                u = STATE["users"].setdefault(str(uid), {"active": True})
                u["access"] = {"until": 0, "kind": "trial", "free": False, "paid_ever": (u.get("access") or {}).get("paid_ever", False)}
            state_touch()
            await m.answer(f"⛔ Доступ пользователя {uid} закрыт.")
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/revoke UID</code>", parse_mode="HTML")

    @admin_dp.message(Command("free"))
    async def adm_free(m: types.Message):
        """/free UID on|off — бесплатный доступ без подписки (без срока)."""
        if not _is_admin(m):
            return
        try:
            _, uid, mode = m.text.split()
            with STATE_LOCK:
                a = STATE["users"].setdefault(str(int(uid)), {"active": True}).setdefault("access", {"until": 0, "kind": "grant", "free": False, "paid_ever": False})
                a["free"] = mode.lower() == "on"
            state_touch()
            await m.answer(f"{'🆓' if a['free'] else '🔒'} Бесплатный доступ {uid}: {'включён' if a['free'] else 'выключен'}")
        except ValueError:
            await m.answer("Формат: <code>/free UID on|off</code>", parse_mode="HTML")

    @admin_dp.message(Command("promo_add"))
    async def adm_promo_add(m: types.Message):
        """/promo_add КОД ДНИ ИСПОЛЬЗОВАНИЙ [ДНЕЙ_ДО_ИСТЕЧЕНИЯ]"""
        if not _is_admin(m):
            return
        try:
            parts = m.text.split()
            code, days, uses = parts[1].upper(), int(parts[2]), int(parts[3])
            exp = time.time() + int(parts[4]) * 86400 if len(parts) > 4 else None
            with STATE_LOCK:
                STATE["promos"][code] = {"days": days, "uses_left": uses, "expires": exp, "used": []}
            state_touch()
            await m.answer(f"🎟 Промокод {code}: {days} дн., использований {uses}" + (f", до {int(parts[4])} дн." if exp else ""))
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/promo_add КОД ДНИ ИСПОЛЬЗОВАНИЙ [ДНЕЙ_ДО_ИСТЕЧЕНИЯ]</code>", parse_mode="HTML")

    @admin_dp.message(Command("promo_del"))
    async def adm_promo_del(m: types.Message):
        if not _is_admin(m):
            return
        code = (m.text.split()[1] if len(m.text.split()) > 1 else "").upper()
        with STATE_LOCK:
            ok = STATE["promos"].pop(code, None) is not None
        state_touch()
        await m.answer("🗑 Удалён." if ok else "Не найден.")

    @admin_dp.message(Command("promos"))
    async def adm_promos(m: types.Message):
        if not _is_admin(m):
            return
        lines = [f"{k}: {v['days']} дн., осталось {v['uses_left']}" for k, v in STATE["promos"].items()] or ["Промокодов нет"]
        await m.answer("🎟 <b>Промокоды</b>\n" + "\n".join(lines), parse_mode="HTML")

    @admin_dp.message(Command("ad_set", "add_set"))
    async def adm_ad_set(m: types.Message):
        """/ad_set текст | ссылка(необязательно)"""
        if not _is_admin(m):
            return
        body = m.text.split(" ", 1)[1] if " " in m.text else ""
        text, _, url = body.partition("|")
        text, url = text.strip(), url.strip()
        if not text:
            return await m.answer("Формат: <code>/add_set Текст рекламы | https://example.com</code>", parse_mode="HTML")
        if url and urlparse(url).scheme != "https":
            return await m.answer("Ссылка должна начинаться с https://", parse_mode="HTML")
        with STATE_LOCK:
            STATE["ads"] = dict(ads_get(), on=True, text=text, url=url)
        state_touch()
        await m.answer("📢 Реклама включена и будет показана в приложении и утреннем брифинге.")

    @admin_dp.message(Command("ad_off"))
    async def adm_ad_off(m: types.Message):
        if not _is_admin(m):
            return
        with STATE_LOCK:
            STATE["ads"] = dict(ads_get(), on=False)
        state_touch()
        await m.answer("📢 Реклама выключена.")

    @admin_dp.message(Command("contact"))
    async def adm_contact(m: types.Message):
        """/contact ads @ник — контакт по рекламе; поддержка работает только через бота."""
        if not _is_admin(m):
            return
        try:
            _, kind, nick = m.text.split()
            if kind != "ads":
                return await m.answer("Поддержка обслуживается только внутри бота. Для рекламы: <code>/contact ads @ник</code>", parse_mode="HTML")
            with STATE_LOCK:
                STATE["ads"] = dict(ads_get(), contact=nick)
            state_touch()
            await m.answer(f"✅ Контакт по рекламе: {html.escape(nick)}")
        except ValueError:
            await m.answer("Формат: <code>/contact ads @ник</code>", parse_mode="HTML")

    @admin_dp.message(Command("who"))
    async def adm_who(m: types.Message):
        if not _is_admin(m):
            return
        try:
            uid = int(m.text.split()[1])
            a = access_payload(uid)
            u = _users().get(str(uid), {})
            await m.answer(f"👤 {uid} · {html.escape(str(u.get('first_name') or '—'))}\nСтатус: {html.escape(a['label'])}, осталось {a['days_left']} дн., "
                           f"бесплатно: {'да' if a['free'] else 'нет'}")
        except (ValueError, IndexError):
            await m.answer("Формат: <code>/who UID</code>", parse_mode="HTML")

    @admin_dp.message(Command("orders"))
    async def adm_orders(m: types.Message):
        """/orders [UID] — последние заказы и их статусы (незавершённые заказы сохраняются)."""
        if not _is_admin(m):
            return
        parts = (m.text or "").split()
        uid = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        with STATE_LOCK:
            rows = [dict(o) for o in STATE.get("orders", {}).values() if uid is None or o.get("uid") == uid]
        rows.sort(key=lambda o: o.get("created", 0), reverse=True)
        lines = [f"<code>{o['id']}</code> · {o['uid']} · {o['days']} дн. · {o['stars']}⭐ · {o['status']}" for o in rows[:15]]
        await m.answer("🧾 <b>Заказы</b>\n" + ("\n".join(lines) if lines else "Заказов нет"), parse_mode="HTML")

    @admin_dp.message(Command("admin_help"))
    async def adm_help(m: types.Message):
        if not _is_admin(m):
            return
        await m.answer("<b>Команды админа</b>\n/grant UID ДНИ — добавить дни\n/revoke UID — закрыть доступ\n"
                       "/free UID on|off — бесплатный доступ\n/who UID — статус\n/orders [UID] — заказы и оплаты\n"
                       "/promo_add КОД ДНИ ИСП [ДНЕЙ] · /promo_del КОД · /promos\n"
                       "/ad_set или /add_set текст | ссылка · /ad_off\n/contact ads @ник · поддержка — через бота", parse_mode="HTML")



async def welcome_for(chat_id, uid, edit_msg=None):
    """Главное меню (/start и кнопка «🏠 Меню»). Единый экран: заменяет предыдущее сообщение бота."""
    offer = discount_offer_for(uid)
    offer_notice = ""
    if offer:
        pct = int(offer.get("pct", 0))
        hours = max(1, int((float(offer.get("until", 0)) - time.time()) / 3600))
        label = "за приглашение" if offer.get("kind") == "referral" else "для новых пользователей"
        offer_notice = f"\n\n🎁 <b>Ваша скидка {pct}% {label}!</b> Действует ещё {hours} ч. Цены уже снижены в разделе «Подписка»."
    if has_access(uid):
        return await present(chat_id, WELCOME_BOT + offer_notice, bot_main_keyboard(), edit_msg=edit_msg)
    return await present(chat_id, "🔒 <b>Доступ закрыт.</b>\nПробный период закончился или не активирован. "
                         "Оплатите подписку, чтобы получать сигналы и сопровождение сделок.\n\n"
                         f"📰 <b>Новости доступны всем:</b> подписывайтесь на <a href=\"{NEWS_CHANNEL_URL}\">IXCLY NEWS</a> — крипторынок, Bitcoin, Россия и GPT." + offer_notice,
                         paywall_kb(uid), edit_msg=edit_msg)


@dp.message(Command("tutorial", "learn"))
async def tutorial_command(m: types.Message):
    """Позволяет повторно открыть обучение в любой момент."""
    await show_tutorial(m, 0, edit=False)


def support_faq_answer(question):
    """Точный FAQ IXCLY: сначала определяем намерение, затем отвечаем только на него.

    Не используем широкие ключи вроде «подпис»/«доступ» как универсальную тему:
    именно из-за этого раньше разные вопросы получали один и тот же ответ.
    Неизвестные вопросы возвращаем в ticket flow (None), не выдумывая ответ.
    """
    import re

    q = (question or "").lower().replace("ё", "е").strip()
    q = re.sub(r"[\t\r\n]+", " ", q)
    q = re.sub(r"\s+", " ", q)
    if not q:
        return None

    def has(*patterns):
        return any(re.search(pattern, q) for pattern in patterns)

    # 1. Способ оплаты — проверяем раньше общей темы подписки.
    if has(r"оплат.{0,25}(только|лишь).{0,15}(звезд|stars)",
           r"(только|лишь).{0,15}(звезд|stars).{0,20}(оплат|плат)",
           r"(не|без).{0,10}(звезд|stars)",
           r"(звезд|stars).{0,10}\??$",
           r"(картой|карт|переводом|перевод|криптой|криптовалютой|usdt|paypal)",
           r"(карт|перевод|usdt|крипт|paypal).{0,25}(оплат|плат)",
           r"(оплат|плат).{0,25}(карт|перевод|usdt|крипт|paypal)",
           r"какими способами.{0,20}(оплат|плат)",
           r"способ.{0,15}оплат"):
        return ("💳 <b>Способы оплаты</b>\n"
                "Сейчас в IXCLY подключена оплата подписки через Telegram Stars (XTR). "
                "Оплата банковской картой, переводом или криптовалютой в текущей версии не предусмотрена.")

    # 2. Что именно даёт подписка — не инструкция по покупке.
    if has(r"что.{0,15}(дает|даёт|включает|получ|предостав).{0,20}подпис",
           r"(зачем|для чего).{0,20}подпис",
           r"преимуществ.{0,20}подпис",
           r"(что|какие).{0,15}(будет|станет).{0,20}(доступно|доступен).{0,20}подпис",
           r"(польза|выгода).{0,20}подпис"):
        return ("⭐ <b>Что даёт подписка IXCLY</b>\n"
                "Подписка открывает платный доступ к функциям IXCLY на выбранный срок: "
                "получению торговых сигналов и сопровождению сделок. Доступ действует "
                "30, 90 или 365 дней — в зависимости от выбранного тарифа. "
                "Подписка не гарантирует прибыль от торговли.")

    # 3. Сроки и стоимость — отдельный вопрос.
    if has(r"(сколько|какая цена|цена|стоимост|тариф|сколько стоит).{0,25}(подпис|месяц|год|дней)",
           r"(подпис|тариф).{0,25}(цена|стоимост|сколько стоит)"):
        return ("📅 <b>Тарифы IXCLY</b>\n"
                "Доступны три срока: 30 дней — 299 Stars, 90 дней — 799 Stars, "
                "365 дней — 2499 Stars. Если у вас действует персональная скидка, "
                "цена будет показана в разделе «Подписка» перед оплатой.")

    # 4. VIP — отдельный тип доступа, а не синоним любого вопроса о подписке.
    if has(r"\bvip\b", r"вип"):
        return ("🔐 <b>VIP-доступ</b>\n"
                "В IXCLY VIP-статус присваивается активной платной подписке, "
                "бессрочному или специальному бесплатному доступу. Для VIP отключается "
                "реклама, если рекламный блок включён. Проверить свой статус и срок можно в профиле.")

    # 5. Paper Trading.
    if has(r"paper", r"бумажн", r"виртуальн.{0,15}сдел", r"демо.{0,15}торг", r"симулятор"):
        return ("🧪 <b>Paper Trading</b>\n"
                "Это режим тренировочной торговли: сделки виртуальные, реальные ордера "
                "на биржу не отправляются. Его можно использовать для проверки стратегии "
                "и ведения тестового портфеля. Откройте раздел Paper Trading в Mini App.")

    # 6. Сигналы и их содержание.
    if has(r"сигнал", r"точк.{0,10}вход", r"stop.?loss", r"take.?profit", r"тейк", r"стоп.?лосс",
           r"частот.{0,15}(сигнал|сдел)", r"когда.{0,15}(приход|выход).{0,10}сигнал"):
        return ("📊 <b>Торговые сигналы</b>\n"
                "В карточке сигнала указываются торговое направление, зона входа, Stop Loss "
                "и уровни Take Profit. Откройте раздел сигналов в Mini App, чтобы смотреть "
                "актуальные идеи и детали сделки. Сигналы являются аналитическими подсказками, "
                "а не гарантией доходности.")

    # 7. Настройки и уведомления.
    if has(r"настройк", r"уведом", r"оповещ", r"alerts", r"звук", r"notification"):
        return ("⚙️ <b>Настройки и уведомления</b>\n"
                "Откройте настройки IXCLY/Mini App и проверьте доступные переключатели "
                "уведомлений. Также убедитесь, что Telegram не запрещает уведомления "
                "для приложения и чата с ботом.")

    # 8. Общая тема подписки — только если пользователь не спросил о конкретной детали.
    if has(r"подпис", r"купить доступ", r"продлить доступ", r"тариф"):
        return ("⭐ <b>Подписка IXCLY</b>\n"
                "В разделе «Подписка» можно посмотреть доступные сроки и персональную цену, "
                "выбрать тариф и оплатить его Telegram Stars. После подтверждения платежа "
                "доступ активируется автоматически.")

    # 9. Реферальная программа и промокоды.
    if has(r"реферал", r"приглас", r"скидк", r"промокод", r"промо"):
        return ("🎁 <b>Реферальная программа и скидки</b>\n"
                "Доступные персональные скидки и предложения отображаются в разделе «Подписка». "
                "Точные условия вашего предложения смотрите там же перед оплатой.")

    # 10. Техническая проблема.
    if has(r"ошибк", r"не работает", r"не открывает", r"завис", r"баг", r"не запуска"):
        return ("🛠️ <b>Техническая проблема</b>\n"
                "Напишите, какой раздел IXCLY не работает и что именно происходит. "
                "Если можете, приложите скриншот. Если проблему не удастся определить "
                "по описанию, обращение будет передано агенту поддержки.")

    return None

SUPPORT_CATEGORIES = {
    "subscription": "Подписка и оплата",
    "signals": "Сигналы и торговые идеи",
    "miniapp": "Mini App / приложение",
    "notifications": "Уведомления",
    "paper": "Paper Trading",
    "other": "Другой вопрос",
}
SUPPORT_WAITING_AGENT = {}


def support_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Подписка и оплата", callback_data="support:cat:subscription")],
        [InlineKeyboardButton(text="📊 Сигналы и торговые идеи", callback_data="support:cat:signals")],
        [InlineKeyboardButton(text="📱 Mini App / приложение", callback_data="support:cat:miniapp")],
        [InlineKeyboardButton(text="🔔 Уведомления", callback_data="support:cat:notifications")],
        [InlineKeyboardButton(text="🧪 Paper Trading", callback_data="support:cat:paper")],
        [InlineKeyboardButton(text="❔ Другой вопрос", callback_data="support:cat:other")],
    ])


async def show_support_menu(target):
    text = ("🆘 <b>Поддержка IXCLY</b>\n\n"
            "Выберите тему обращения. Я помогу с частыми вопросами или оформлю обращение прямо здесь, в боте.\n"
            "Личные контакты для обращения не нужны — ответ придёт в этот чат.")
    if isinstance(target, types.Message):
        return await target.answer(text, parse_mode="HTML", reply_markup=support_menu_keyboard())
    return await target.edit_text(text, parse_mode="HTML", reply_markup=support_menu_keyboard())


async def create_support_ticket(m: types.Message, question: str, category: str = "other"):
    tickets = STATE.setdefault("support_tickets", {})
    ticket_id = f"{int(time.time())}{random.randint(1000, 9999)}"
    category_label = SUPPORT_CATEGORIES.get(category, SUPPORT_CATEGORIES["other"])
    ticket = {"id": ticket_id, "uid": m.from_user.id, "chat_id": m.chat.id,
              "username": m.from_user.username or "", "name": m.from_user.full_name,
              "category": category_label, "question": (question or "")[:3000],
              "status": "open", "created_at": time.time(), "answer": ""}
    tickets[ticket_id] = ticket
    if len(tickets) > 500:
        for old_id, old in sorted(tickets.items(), key=lambda kv: float(kv[1].get("created_at", 0)))[:100]:
            if old.get("status") != "open": tickets.pop(old_id, None)
    state_touch(True)
    await m.answer(f"✅ <b>Обращение #{ticket_id} создано.</b>\nТема: {html.escape(category_label)}\n\n"
                   "Ответ поддержки придёт сюда же, в этот чат. Ваши личные контакты не публикуются.", parse_mode="HTML",
                   reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🆘 Меню поддержки", callback_data="support_menu")]]))


@dp.message(Command("market"))
async def cmd_market(m: types.Message):
    x = await ai_center_snapshot(m.from_user.id, "BTC")
    await m.answer(_format_ai_center(x), parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("forecast"))
async def cmd_forecast(m: types.Message):
    parts=(m.text or "").split(maxsplit=1); coin=normalize_ticker(parts[1]) if len(parts)>1 else "BTC"
    x=await ai_center_snapshot(m.from_user.id, coin or "BTC")
    f=x.get("forecast") or {}
    if f.get("status")!="ok": return await m.answer("Недостаточно рыночных данных для сценарного прогноза.")
    await m.answer(_format_ai_center(x),parse_mode="HTML")


@dp.message(Command("whales"))
async def cmd_whales(m: types.Message):
    parts=(m.text or "").split(maxsplit=1); coin=normalize_ticker(parts[1]) if len(parts)>1 else "BTC"
    rows=[]
    for cand in SYMBOL_ALIASES.get(coin or "BTC",[coin or "BTC"]):
        try:
            rows=await http_json("https://data-api.binance.vision/api/v3/aggTrades",params={"symbol":f"{cand}USDT","limit":500},timeout=7)
            if rows: break
        except Exception: continue
    trades=[]
    for t in rows or []:
        try:
            value=float(t.get("p",0))*float(t.get("q",0))
            if value>=100000:
                trades.append((value,"SELL" if t.get("m") else "BUY",float(t.get("p",0))))
        except Exception: pass
    trades=sorted(trades,reverse=True)[:8]
    if not trades: return await m.answer(f"🐋 {coin}: в последних 500 агрегированных сделках Binance не найдено сделок от $100k. Это не означает отсутствие крупных сделок на других площадках.")
    body="\n".join(f"{'🔴' if side=='SELL' else '🟢'} {side} · ${value:,.0f} · цена {fmt(price)}" for value,side,price in trades)
    await m.answer(f"🐋 <b>Крупные сделки {coin}</b> · Binance Spot\n{body}\n\nПорог $100k; это поток крупных сделок, не идентификация конкретного кита.",parse_mode="HTML")


@dp.message(Command("liquidations"))
async def cmd_liquidations(m: types.Message):
    now=time.time(); rows=[x for x in LIQUIDATIONS if now-float(x.get("ts",0))<=3600]
    rows=sorted(rows,key=lambda x:float(x.get("notional",0) or 0),reverse=True)[:12]
    if not rows: return await m.answer("💥 За последний час в подключённом потоке Binance Futures нет записанных событий ликвидаций.")
    body="\n".join(f"{x.get('coin','?')} · {'лонги' if x.get('side')=='LONG' else 'шорты'} · ${float(x.get('notional',0) or 0):,.0f}" for x in rows)
    await m.answer("💥 <b>Ликвидации за 1 час</b>\n"+body,parse_mode="HTML")


@dp.message(Command("radar"))
async def cmd_radar(m: types.Message):
    ranked=list(scanner_ranking or [])[:10]
    anomalies=[]
    for c,v in list(LIVE.items()):
        if not isinstance(v,dict) or time.time()-float(v.get("ts",0))>300: continue
        ch=abs(_safe_float(v.get("change_pct")))
        if ch>=2: anomalies.append((c,ch,v.get("price")))
    anomalies.sort(key=lambda z:-z[1])
    lines=["📡 <b>IXCLY Radar</b>"]
    lines += [f"#{x.get('rank',i+1)} {x.get('coin')} · {x.get('direction')} · {float(x.get('score',0)):.1f}/100" for i,x in enumerate(ranked[:8])]
    if anomalies: lines += ["\n<b>Быстрые движения (LIVE ≤5 мин)</b>"]+[f"{c}: {ch:+.2f}%" for c,ch,_ in anomalies[:6]]
    if not ranked and not anomalies: lines.append("Пока нет свежих подтверждённых кандидатов. Радар не запускает отдельный тяжёлый скан.")
    await m.answer("\n".join(lines),parse_mode="HTML")


@dp.message(Command("risk"))
async def cmd_risk(m: types.Message):
    uid=m.from_user.id; cfg=get_user_risk_settings(uid); used=calculate_portfolio_risk(uid)
    await m.answer(f"🛡 <b>Контроль риска</b>\nДепозит: {cfg['deposit']:.2f} USDT\nРиск на сделку: {cfg['risk_pct']:.2f}%\nЛимит открытого риска: {cfg['max_open_risk']:.2f}%\nИспользовано: {used:.2f}%\nОстаток: {max(0,cfg['max_open_risk']-used):.2f}%\nОткрытых позиций: {len(get_user_open_positions(uid))}\n\nРасчёт не создаёт ордера.",parse_mode="HTML")


@dp.message(Command("ai_report"))
async def cmd_ai_report(m: types.Message):
    parts=(m.text or "").split(maxsplit=1); coin=normalize_ticker(parts[1]) if len(parts)>1 else "BTC"
    x=await ai_center_snapshot(m.from_user.id,coin or "BTC")
    await m.answer(_format_ai_center(x),parse_mode="HTML")
    audit=x.get("system_memory") or {}
    if audit.get("findings"): await m.answer("🧪 <b>Self-Audit</b>\n"+"\n".join("• "+html.escape(z) for z in audit["findings"]),parse_mode="HTML")


@dp.message(Command("strategy_stats"))
async def cmd_strategy_stats(m: types.Message):
    rows=strategy_performance(load_signal_history(),"type",3)
    if not rows: return await m.answer("Недостаточно закрытых сигналов для сравнения стратегий.")
    body="\n".join(f"{html.escape(str(x['name']))}: {x['trades']} сделок · WR {x['win_rate']}% · {x['avg_r']:+.2f}R/сделку"+("" if x['sample_ok'] else " · малая выборка") for x in rows)
    await m.answer("🧩 <b>Результативность типов стратегий</b>\n"+body,parse_mode="HTML")


@dp.message(Command("accuracy"))
async def cmd_accuracy(m: types.Message):
    hist=load_signal_history(); audit=self_audit_summary(hist,30)
    regimes=strategy_performance(hist,"market_regime",3)
    body="\n".join(audit.get("findings",[]))
    if regimes: body+="\n\n<b>По режимам рынка</b>\n"+"\n".join(f"{html.escape(str(x['name']))}: {x['win_rate']}% · {x['trades']}" for x in regimes)
    await m.answer("🎯 <b>Точность и калибровка</b>\n"+body+"\n\nОценка строится по закрытым сигналам, а не по незавершённым прогнозам.",parse_mode="HTML")


@dp.message(Command("brain"))
async def cmd_brain(m: types.Message):
    parts = (m.text or "").split(maxsplit=1)
    coin = normalize_ticker(parts[1]) if len(parts) > 1 else "BTC"
    x = await ai_center_snapshot(m.from_user.id, coin or "BTC")
    d = x.get("decision_brain") or {}
    dna = x.get("market_dna") or {}
    cf = x.get("counterfactual_lab") or {}
    await m.answer(
        f"🧠 <b>IXCLY Decision Brain · {html.escape(coin or 'BTC')}</b>\n"
        f"Решение: <b>{html.escape(str(d.get('action','WAIT')))}</b> · DNA {d.get('score','—')}\n"
        f"Причина: {html.escape(str(d.get('reason','нет данных')))}\n"
        f"Отпечаток рынка: {html.escape(str(dna.get('trend','нет данных')))} · похожих состояний {dna.get('historical_matches',0)}\n"
        f"Counterfactual: {cf.get('trades',0)} закрытых результатов · средний R {cf.get('avg_r','—')}\n"
        "Это аналитика, не исполнение сделок.", parse_mode="HTML")


@dp.message(Command("dna"))
async def cmd_dna(m: types.Message):
    rows = strategy_dna(load_signal_history(), 5)
    if not rows:
        return await m.answer("Недостаточно закрытых сигналов для профиля стратегий.")
    body = "\n".join(
        f"{html.escape(x['dimension'])} · {html.escape(x['name'])}: {x['trades']} сделок, WR {x['win_rate']}%, {x['avg_r']:+.2f}R · {html.escape(x['profile'])}"
        for x in rows[:12])
    await m.answer("🧬 <b>IXCLY Strategy DNA</b>\n" + body, parse_mode="HTML")


@dp.message(Command("replay"))
async def cmd_replay(m: types.Message):
    parts = (m.text or "").split(maxsplit=1)
    coin = normalize_ticker(parts[1]) if len(parts) > 1 else "BTC"
    candidates = [x for x in load_signal_history() if x.get("coin") == (coin or "BTC")]
    if not candidates:
        return await m.answer("Для этой монеты нет сохранённого сигнала для воспроизведения.")
    signal_item = max(candidates, key=lambda x: x.get("ts", 0))
    rows = await fetch_history(coin or "BTC", "15m", 300, 60)
    if not rows:
        return await m.answer("Не удалось получить свечи для Market Replay.")
    replay = replay_signal(signal_item, rows)
    await m.answer(
        f"⏪ <b>Market Replay · {html.escape(coin or 'BTC')}</b>\n"
        f"Сигнал: {html.escape(str(signal_item.get('side')))} · вход {fmt(signal_item.get('entry'))}\n"
        f"Воспроизведение свечей после сигнала: <b>{html.escape(str(replay.get('result')))}</b>\n"
        f"Вход активирован: {'да' if replay.get('entered') else 'нет'} · TP: {', '.join(map(str,replay.get('targets_hit',[]))) or 'нет'}\n"
        f"Покрытие истории: {'полное' if replay.get('coverage_complete') else 'частичное — часть свечей до сигнала отсутствует'}\n"
        "OHLC не раскрывает порядок движений внутри свечи; при касании стопа и цели в одной свече применяется консервативное правило стоп первым.",
        parse_mode="HTML")


@dp.message(Command("cognitive"))
async def cmd_cognitive(m: types.Message):
    parts = (m.text or "").split(maxsplit=1)
    coin = normalize_ticker(parts[1]) if len(parts) > 1 else "BTC"
    x = await ai_center_snapshot(m.from_user.id, coin or "BTC")
    phase = x.get("regime_transition") or {}; fragile = x.get("signal_fragility") or {}
    evidence = x.get("evidence_conflict_map") or {}; calibration = x.get("score_calibration") or {}
    bands = calibration.get("bands") or []
    band_text = " · ".join(f"{b['band']}: WR {b['win_rate']}% / {b['trades']}" for b in bands[:5]) or "нет выборки"
    await m.answer(
        f"🧠 <b>IXCLY Cognitive Diagnostics · {html.escape(coin or 'BTC')}</b>\n"
        f"Фаза рынка: <b>{html.escape(str(phase.get('label', 'нет данных')))}</b> · {phase.get('recent_range_vs_previous', '—')}x диапазон\n"
        f"Хрупкость сигнала: <b>{fragile.get('level', 'нет данных')}</b> · зависимость: {html.escape(str(fragile.get('dominant_dependency', '—')))}\n"
        f"Конфликт источников: <b>{html.escape(str(evidence.get('state', 'нет данных')))}</b> · {evidence.get('conflict_pct', '—')}%\n"
        f"Score audit ({calibration.get('scored_closed_trades', 0)} закрытых): {html.escape(band_text)}\n"
        "Score не является вероятностью. Диагностика не исполняет сделки.", parse_mode="HTML")


@dp.message(Command("paper_report"))
async def cmd_paper_report(m: types.Message):
    try:
        with STATE_LOCK: a=paper_analytics(STATE.setdefault("paper",{}),m.from_user.id)
    except Exception as e: return await m.answer("Paper Trading: "+html.escape(str(e)))
    await m.answer(f"📒 <b>Paper Trading · {html.escape(str(a.get('account_name') or 'счёт'))}</b>\nСделок: {a.get('trades',0)} · WR: {a.get('win_rate',0)}%\nNet PnL: {a.get('net_pnl',0):+.2f} USDT\nСредний результат: {a.get('avg_pnl',0):+.2f} USDT\nProfit Factor: {a.get('profit_factor','—')}\nMax drawdown: {a.get('max_drawdown_pct',0):.2f}%\n\nТолько виртуальные сделки.",parse_mode="HTML")


@dp.message(Command("scan_report"))
async def main_admin_scan_report(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    x = last_scan_info
    if not x.get("ts"):
        return await m.answer("Сканирование ещё не выполнялось.")
    errors = (x.get("bt_error_examples") or []) + (x.get("analysis_error_examples") or [])
    body = ("🔎 <b>Отчёт последнего сканирования</b>\n"
            f"Монеты: {x.get('universe', 0)} → Stage 1: {x.get('stage1', 0)} → Stage 2: {x.get('stage2', 0)}\n"
            f"К анализу допущено: {x.get('backtest_ok', 0)} · слабый бэктест: {x.get('bt_rejected', 0)} · без истории: {x.get('bt_unavailable', 0)} · ошибок истории: {x.get('bt_errors', 0)}\n"
            f"Ошибок анализа: {x.get('analysis_errors', 0)} · ЖДАТЬ: {x.get('analysis_wait', 0)} · качество: {x.get('quality_rejected', 0)}\n"
            f"Готово: {x.get('final', 0)} · опубликовано: {x.get('sent', 0)}\n"
            f"RAM при скане: {x.get('rss_mb') or _current_rss_mb()} MB · пик процесса: {_rss_mb()} MB\n"
            f"Последняя очистка RAM: {html.escape(str(x.get('memory_trim') or 'не требовалась'))}\n"
            f"Причина: {html.escape(str(x.get('diagnostic', '—')))}")
    if errors:
        body += "\n\n<b>Технические ошибки:</b>\n" + "\n".join(html.escape(str(e)) for e in errors[:3])
    await m.answer(body, parse_mode="HTML")


@dp.message(Command("ram_report"))
async def main_admin_ram_report(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    x = _runtime_memory_snapshot()
    caches = x.get("cache_sizes", {})
    trim = x.get("last_trim") or {}
    protected = x.get("protected_history", {})
    await m.answer(
        "🧠 <b>IXCLY: память сервера</b>\n"
        f"RAM: {x.get('rss_mb')} MB · пик процесса: {x.get('peak_rss_mb')} MB\n"
        f"Порог очистки: {x.get('trim_at_mb')} MB · остановки: {x.get('stop_at_mb')} MB\n"
        f"Кэши: analysis {caches.get('analysis', 0)}, backtest {caches.get('backtest', 0)}, "
        f"AI {caches.get('ai', 0)}, funding {caches.get('funding', 0)}, "
        f"orderbook {caches.get('orderbook', 0)}, hourly {caches.get('hourly', 0)}, "
        f"intelligence {caches.get('intelligence', 0)}, API {caches.get('api', 0)}\n"
        f"Защищённые данные (не очищаются): сигналы {protected.get('signals', 0)}, "
        f"сделки {protected.get('trade_records', 0)}, Paper-история {protected.get('paper_records', 0)}\n"
        f"Последняя очистка: {html.escape(str(trim) if trim else 'ещё не выполнялась')}", parse_mode="HTML")


@dp.message(Command("tickets"))
async def main_admin_tickets(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    opened = [t for t in STATE.setdefault("support_tickets", {}).values() if t.get("status") == "open"]
    if not opened:
        return await m.answer("Открытых обращений нет.")
    for t in sorted(opened, key=lambda x: float(x.get("created_at", 0)))[:30]:
        await m.answer(f"🆘 <b>#{html.escape(t['id'])}</b> · {html.escape(str(t.get('category', 'Другое')))}\n{html.escape(str(t.get('question',''))[:2500])}\nОтвет: <code>/reply {t['id']} текст</code>", parse_mode="HTML")


@dp.message(Command("reply"))
async def main_admin_reply(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        return
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3:
        return await m.answer("Формат: /reply ID текст ответа")
    ticket = STATE.setdefault("support_tickets", {}).get(parts[1])
    if not ticket:
        return await m.answer("Обращение не найдено.")
    answer = parts[2][:3000]
    try:
        await bot.send_message(int(ticket["chat_id"]), "🧑‍💻 <b>Ответ поддержки IXCLY</b>\n" + html.escape(answer), parse_mode="HTML")
    except Exception as e:
        return await m.answer(f"Не удалось доставить ответ: {html.escape(str(e)[:300])}")
    ticket.update({"status": "answered", "answer": answer, "answered_at": time.time(), "answered_by": m.from_user.id})
    state_touch(True)
    await m.answer("✅ Ответ отправлен пользователю.")


@dp.message(Command("support", "helpdesk"))
async def support_command(m: types.Message):
    await show_support_menu(m)


@dp.message()
async def fallback_message(m: types.Message):
    text_in = (m.text or m.caption or "").strip()
    if text_in and not text_in.startswith("/"):
        if m.from_user.id in SUPPORT_WAITING_AGENT:
            category = SUPPORT_WAITING_AGENT.pop(m.from_user.id, "other")
            return await create_support_ticket(m, text_in, category)
        answer = support_faq_answer(text_in)
        if answer:
            return await m.answer(answer)
        if any(x in text_in.lower() for x in ("поддержка:", "вопрос поддержке", "оператор:", "агент:", "help:")):
            return await create_support_ticket(m, text_in, "other")
        if not re.fullmatch(r"[A-Za-z0-9]{2,12}", text_in):
            return await m.answer("Чтобы написать в поддержку, откройте меню 🆘 Поддержка и выберите тему обращения.",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🆘 Открыть поддержку", callback_data="support_menu")]]))
    try:
        await m.delete()
    except Exception:
        pass
    coin = normalize_ticker(m.text or "") if m.text and re.fullmatch(r"[A-Za-z0-9]{2,12}", m.text.strip()) else None
    if coin and WEBAPP_URL:
        await bot.send_message(m.chat.id, f"🔍 <b>{html.escape(coin)}</b>: открыть анализ в приложении",
                               parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                   InlineKeyboardButton(text="📈 Открыть анализ", web_app=WebAppInfo(url=webapp_link(coin)))]]))
    else:
        await welcome_for(m.chat.id, m.from_user.id)


@dp.callback_query()
async def on_callback(cb: types.CallbackQuery, state: FSMContext):
    data = cb.data or ""
    uid = cb.from_user.id
    msg = cb.message
    try:
        if data == "tutorial:start" and msg:
            await safe_answer(cb)
            set_user_field(uid, onboarding_status="in_progress", onboarding_step=0)
            return await show_tutorial(msg, 0, edit=True)
        if data.startswith("tutorial:step:") and msg:
            await safe_answer(cb)
            try:
                step = int(data.rsplit(":", 1)[1])
            except (TypeError, ValueError):
                step = 0
            step = max(0, min(step, len(TUTORIAL_STEPS) - 1))
            set_user_field(uid, onboarding_status="in_progress", onboarding_step=step)
            return await show_tutorial(msg, step, edit=True)
        if data in ("tutorial:skip", "tutorial:done") and msg:
            await safe_answer(cb, "Обучение пропущено" if data.endswith("skip") else "Обучение завершено")
            skipped = data.endswith("skip")
            set_user_field(uid, onboarding_done=not skipped, onboarding_skipped=skipped,
                           onboarding_status="skipped" if skipped else "completed",
                           onboarding_completed_at=time.time())
            try:
                await msg.edit_text("🎓 <b>Обучение " + ("пропущено." if skipped else "завершено!") +
                                    "</b>\n\nВ любой момент его можно открыть командой /tutorial или кнопкой «Обучение новичка».",
                                    parse_mode="HTML")
            except Exception:
                pass
            return await welcome_for(msg.chat.id, uid)
        if data in ("support_menu", "support_auto", "support_agent") and msg:
            await safe_answer(cb)
            SUPPORT_WAITING_AGENT.pop(uid, None)
            return await show_support_menu(msg)
        if data.startswith("support:cat:") and msg:
            await safe_answer(cb)
            category = data.split(":", 2)[2]
            if category not in SUPPORT_CATEGORIES:
                category = "other"
            SUPPORT_WAITING_AGENT[uid] = category
            return await msg.answer(f"✍️ <b>{html.escape(SUPPORT_CATEGORIES[category])}</b>\n\n"
                                    "Опишите вопрос одним сообщением. Можно добавить детали ошибки и шаги, после которых она возникла.\n"
                                    "Я создам обращение, а ответ придёт сюда же.", parse_mode="HTML",
                                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="support_menu")]]))
        if data == "show_docs" and msg:
            await safe_answer(cb)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Политика конфиденциальности", url=DOCS_PRIVACY)],
                [InlineKeyboardButton(text="Пользовательское соглашение (оферта)", url=DOCS_OFFER)],
                [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support_menu")]])
            return await msg.answer("📄 <b>Документы IXCLY PRO</b>\nВыберите нужный пункт:", reply_markup=kb, parse_mode="HTML")
        if data in ("menu", "nav:back"):
            # Кнопка «🏠 Меню» (под новостями, сигналами, уведомлениями) — то же главное меню, что и /start
            await safe_answer(cb)
            await state.clear()
            SUPPORT_WAITING_AGENT.pop(uid, None)
            return await welcome_for(msg.chat.id if msg else uid, uid, edit_msg=msg)
        if data == "sub:open":
            await safe_answer(cb)
            return await show_subscription(msg.chat.id if msg else uid, uid, edit_msg=msg)
        if data == "guide:open":
            await safe_answer(cb)
            return await show_guide(msg.chat.id if msg else uid, uid, edit_msg=msg)
        if data.startswith(("buy:", "pay:")):
            await safe_answer(cb)
            try:
                days = int(data.split(":")[1])
            except (TypeError, ValueError, IndexError):
                days = 0
            if days not in PLANS:
                return await show_subscription(msg.chat.id if msg else uid, uid, edit_msg=msg)
            if data.startswith("buy:"):
                # Сначала подтверждение: тариф, срок, стоимость и дата окончания
                return await show_buy_confirm(msg.chat.id if msg else uid, uid, days, edit_msg=msg)
            return await send_plan_invoice(uid, days)
        if data == "promo:help":
            await safe_answer(cb)
            return await _send_new(msg.chat.id if msg else uid, "🎟 Отправьте: <code>/promo КОД</code>", kb_nav(back=False), None)
        if data[:3] in ("in:", "sv:"):
            return await handle_signal_button(cb, data)
        if data == "del_msg" and msg:
            await safe_answer(cb, "Скрыто")
            return await safe_delete(msg)
        if data == "notif:menu" and msg:
            await safe_answer(cb)
            return await msg.edit_text("🔔 <b>Уведомления</b>\nВыберите, что присылать в чат:", parse_mode="HTML",
                                       reply_markup=notif_keyboard(uid))
        if data == "notif:back" and msg:
            await safe_answer(cb)
            return await welcome_for(msg.chat.id, uid, edit_msg=msg)
        if data.startswith("notif:") and data.split(":")[1] in NOTIFY_NAMES:
            k = data.split(":")[1]
            cur = _users().get(str(uid), {}).get(f"notify_{k}", True)
            set_user_field(uid, **{f"notify_{k}": not cur})
            await safe_answer(cb, f"{NOTIFY_NAMES[k]}: {'вкл' if not cur else 'выкл'}")
            if msg:
                await msg.edit_reply_markup(reply_markup=notif_keyboard(uid))
            return
        await safe_answer(cb)
    except Exception as e:
        log.info(f"callback {data}: {e}")
        await safe_answer(cb)


# =====================================================================
# АДМИН-БОТ
# =====================================================================
def admin_keyboard():
    B = InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[
        [B(text="📊 Статистика", callback_data="admin:stats"), B(text="👥 Пользователи", callback_data="admin:users")],
        [B(text="📰 Статус IXCLY NEWS", callback_data="admin:news_now"),
         B(text=f"🧠 Риск новостей: {'ВКЛ' if bot_flags['news_enabled'] else 'ВЫКЛ'}", callback_data="admin:toggle_news")],
        [B(text=f"🚨 Сканер: {'ВКЛ' if bot_flags['alerts_enabled'] else 'ВЫКЛ'}", callback_data="admin:toggle_scan"),
         B(text="🔍 Скан-тест", callback_data="admin:scan_test")],
        [B(text="🔧 Диагностика", callback_data="admin:diag"), B(text="💾 Бэкап", callback_data="admin:backup")],
        [B(text="📥 Восстановить", callback_data="admin:restore"), B(text="⏳ Таймеры", callback_data="admin:timers")],
        [B(text="🧠 Память / RAM", callback_data="admin:ram_report")],
    ])


async def notify_admin(text):
    sender = admin_bot or bot
    if not ADMIN_IDS:
        return
    for aid in ADMIN_IDS:
        try:
            await sender.send_message(aid, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            log.info(f"Админу {aid}: {e}")


async def telegram_backup(reason="auto"):
    global backup_dirty, last_backup_ts
    backup_bot = admin_bot or bot
    if not backup_bot or not ADMIN_IDS:
        log.warning("Telegram backup не настроен: нужны ADMIN_IDS и доступный BOT_TOKEN.")
        return False
    data = (await asyncio.to_thread(flush_state)).encode("utf-8")
    if not data:
        return False
    ok = False
    caption = (f"💾 БЭКАП · юзеров {len(STATE['users'])} · сигналов {len(STATE['signal_history'])} · "
               f"{reason} · {datetime.now().strftime('%d.%m %H:%M')}")
    for aid in ADMIN_IDS:
        try:
            msg = await backup_bot.send_document(aid, BufferedInputFile(data, "bot_state.json"), caption=caption, disable_notification=True)
            # Сначала закрепляем новый файл; старый не трогаем, пока новый не закреплён.
            await backup_bot.pin_chat_message(aid, msg.message_id, disable_notification=True)
            old = last_backup_msg.get(aid)
            if old and old != msg.message_id:
                try:
                    await backup_bot.unpin_chat_message(aid, old)
                    await backup_bot.delete_message(aid, old)
                except Exception:
                    pass
            last_backup_msg[aid] = msg.message_id
            ok = True
        except Exception as e:
            log.warning(f"Бэкап админу {aid} не отправлен: {e}")
    if ok:
        backup_dirty, last_backup_ts = False, time.time()
    return ok


async def telegram_restore():
    backup_bot = admin_bot or bot
    if not backup_bot or not ADMIN_IDS:
        return 0
    for aid in ADMIN_IDS:
        try:
            pm = getattr(await backup_bot.get_chat(aid), "pinned_message", None)
            doc = getattr(pm, "document", None) if pm else None
            if not doc or not (doc.file_name or "").endswith(".json"):
                continue
            f = await backup_bot.get_file(doc.file_id)
            data = json.loads((await backup_bot.download_file(f.file_path)).read().decode("utf-8"))
            n = restore_state(data)
            last_backup_msg[aid] = pm.message_id
            if n:
                return n
        except Exception as e:
            log.warning(f"Восстановление из бэкапа не удалось: {e}")
    return 0


async def backup_loop():
    await asyncio.sleep(30)
    while True:
        try:
            now = time.time()
            # Бэкап не только после изменений, но и периодически — даже если бот простаивает.
            due = (last_backup_ts <= 0 or now - last_backup_ts >= BACKUP_INTERVAL)
            if (backup_dirty and now - last_backup_ts >= 120) or due:
                sent = await telegram_backup("автоматический" if backup_dirty else "плановый")
                if not sent and admin_bot and ADMIN_IDS:
                    log.error("Автоматический Telegram-бэкап не подтверждён; данные оставлены в очереди на повтор.")
        except Exception as e:
            log.warning(f"backup_loop: {e}")
        next_timers["backup"] = time.time() + 30
        await asyncio.sleep(30)


@dp.message(Command("backup_status"))
async def backup_status_command(m: types.Message):
    """Показывает состояние локального файла и Telegram-бэкапа."""
    if m.from_user.id not in ADMIN_IDS:
        return await m.answer("⛔ Команда доступна только администратору.")
    try:
        state_exists = os.path.isfile(STATE_FILE)
        state_size = os.path.getsize(STATE_FILE) if state_exists else 0
        state_mtime = os.path.getmtime(STATE_FILE) if state_exists else 0
        local_baks = [p for p in LOCAL_BACKUP_FILES if os.path.isfile(p)]
        local_bak_size = sum(os.path.getsize(p) for p in local_baks)
        age = max(0, int(time.time() - last_backup_ts)) if last_backup_ts else None
        if not ADMIN_IDS:
            tg_status = "не настроен: добавьте ADMIN_IDS"
        elif age is None:
            tg_status = "после запуска ещё не подтверждён в этой сессии"
        else:
            tg_status = f"последний подтверждённый бэкап примерно {age // 3600} ч {(age % 3600) // 60} мин назад"
        disk_env = os.getenv("IXCLY_DATA_DIR") or os.getenv("RENDER_DISK_PATH")
        if _DATA_DIR_FALLBACK:
            storage = (f"временный fallback: {html.escape(_DATA_DIR)} "
                       f"(нет прав на {html.escape(_REQUESTED_DATA_DIR)}; данные не постоянные)")
        elif disk_env:
            storage = f"настроенный путь: {html.escape(_DATA_DIR)}"
        else:
            storage = f"локальная файловая система: {html.escape(_DATA_DIR)} (не постоянное хранилище)"
        saved = datetime.fromtimestamp(state_mtime).strftime("%d.%m.%Y %H:%M:%S") if state_mtime else "нет файла"
        text = (
            "💾 <b>IXCLY — статус резервного копирования</b>\n\n"
            f"📁 <b>Хранилище:</b> {storage}\n"
            f"📄 <b>Файл состояния:</b> {'есть' if state_exists else 'отсутствует'}\n"
            f"📦 <b>Размер состояния:</b> {state_size / 1024:.1f} KB\n"
            f"🕒 <b>Изменён:</b> {saved}\n"
            f"🗂 <b>Локальных копий:</b> {len(local_baks)} ({local_bak_size / 1024:.1f} KB)\n"
            f"☁️ <b>Telegram:</b> {tg_status}\n"
            f"⏱ <b>Интервал:</b> {BACKUP_INTERVAL // 60} мин\n"
            f"🔄 <b>Есть несохранённые изменения:</b> {'да' if backup_dirty else 'нет/не отмечено'}\n\n"
            "Важно: статус последнего бэкапа отслеживается в текущем процессе. После перезапуска он может отображаться как неизвестный, пока новая копия не будет подтверждена."
        )
        await m.answer(text, parse_mode="HTML")
    except Exception as e:
        log.exception("backup_status_command failed")
        await m.answer(f"Не удалось прочитать статус бэкапа: {html.escape(str(e)[:250])}")


if admin_dp:
    @admin_dp.errors()
    async def admin_errors_handler(event: ErrorEvent):
        if not isinstance(event.exception, TelegramNetworkError):
            log.error(f"Ошибка админ-бота: {event.exception}", exc_info=event.exception)
        return True

    @admin_dp.message(Command("tickets"))
    async def admin_support_tickets(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        opened = [t for t in STATE.setdefault("support_tickets", {}).values() if t.get("status") == "open"]
        if not opened:
            return await m.answer("Открытых обращений нет.")
        for t in sorted(opened, key=lambda x: float(x.get("created_at", 0)))[:30]:
            await m.answer(f"🆘 <b>#{html.escape(t['id'])}</b> · {html.escape(str(t.get('name',''))[:60])} · {html.escape(str(t.get('category', 'Другое')))}\n"
                           f"{html.escape(str(t.get('question',''))[:2500])}\n\nОтвет: <code>/reply {t['id']} текст</code>", parse_mode="HTML")

    @admin_dp.message(Command("reply"))
    async def admin_support_reply(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        parts = (m.text or "").split(maxsplit=2)
        if len(parts) < 3:
            return await m.answer("Формат: /reply ID текст ответа")
        ticket = STATE.setdefault("support_tickets", {}).get(parts[1])
        if not ticket:
            return await m.answer("Обращение не найдено.")
        answer = parts[2][:3000]
        try:
            await bot.send_message(int(ticket['chat_id']), "🧑‍💻 <b>Ответ поддержки IXCLY</b>\n" + html.escape(answer), parse_mode="HTML")
        except Exception as e:
            return await m.answer(f"Не удалось доставить ответ: {html.escape(str(e)[:300])}")
        ticket.update({"status": "answered", "answer": answer, "answered_at": time.time(), "answered_by": m.from_user.id})
        state_touch(True)
        await m.answer(f"✅ Ответ отправлен пользователю по обращению {parts[1]}.")

    @admin_dp.message(Command("close_ticket"))
    async def admin_support_close(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            return
        parts = (m.text or "").split(maxsplit=1)
        if len(parts) < 2:
            return await m.answer("Формат: /close_ticket ID")
        ticket = STATE.setdefault("support_tickets", {}).get(parts[1])
        if not ticket:
            return await m.answer("Обращение не найдено.")
        ticket["status"] = "closed"
        ticket["closed_at"] = time.time()
        state_touch(True)
        await m.answer("✅ Обращение закрыто.")

    @admin_dp.message(Command("panel", "start", "admin"))
    async def admin_panel(m: types.Message):
        if m.from_user.id not in ADMIN_IDS:
            return await m.answer(f"⛔ Доступ только для администратора.\nВаш ID: <code>{m.from_user.id}</code>", parse_mode="HTML")
        total, active = user_counts()
        await m.answer(f"✅ <b>Админ-панель IXCLY v{VERSION}</b>\n👥 Пользователей: {total} (активных {active})",
                       reply_markup=admin_keyboard(), parse_mode="HTML")

    @admin_dp.callback_query(F.data.startswith("admin:"))
    async def admin_cb(cb: types.CallbackQuery):
        if cb.from_user.id not in ADMIN_IDS:
            return await safe_answer(cb, "Нет доступа", True)
        act = cb.data.split(":")[1]
        if act in ("toggle_news", "toggle_scan"):
            key = "news_enabled" if act == "toggle_news" else "alerts_enabled"
            bot_flags[key] = not bot_flags[key]
            state_touch()
            await safe_answer(cb, f"{'Риск-анализ новостей' if key == 'news_enabled' else 'Сканер'}: {'ВКЛ' if bot_flags[key] else 'ВЫКЛ'}")
            try:
                await cb.message.edit_reply_markup(reply_markup=admin_keyboard())
            except Exception:
                pass
            return
        await safe_answer(cb, "⏳ Выполняю…")
        if act == "stats":
            total, active = user_counts()
            up = int(time.time() - stats["started"])
            await cb.message.answer(f"📊 Пользователей: {total} (активных {active})\nАптайм: {up // 3600} ч\n"
                                    f"Скан: {last_scan_info['stage1']} → {last_scan_info['stage2']} → {last_scan_info['final']}")
        elif act == "users":
            lines = [f"👥 Пользователи ({len(_users())})"] + [
                f"{'✅' if u.get('active', True) else '❌'} <code>{uid}</code> {html.escape(str(u.get('first_name') or '—')[:18])}"
                for uid, u in list(_users().items())[-30:] if isinstance(u, dict)]
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        elif act == "news_now":
            news_token_ready = bool(clean_key(os.getenv("IXCLY_NEWS_BOT_TOKEN")))
            await cb.message.answer(
                "📰 <b>IXCLY NEWS</b>\n"
                f"Канал: <a href=\"{html.escape(NEWS_CHANNEL_URL, quote=True)}\">@ixcly_news</a>\n"
                f"Новостной бот: {'токен настроен' if news_token_ready else 'токен ещё не добавлен в Render'}\n\n"
                "Обычные новости не рассылаются в личные сообщения IXCLY Trading. "
                "Публикации выходят в новостном канале. Здесь остаётся только отдельный риск-анализ критических событий.",
                parse_mode="HTML", disable_web_page_preview=True)
        elif act == "scan_test":
            await cb.message.answer("🔍 Скан без рассылки…")
            sigs = await scan_once(send=False)
            await cb.message.answer(f"Итог: {len(sigs)} · " + ", ".join(s["coin"] for s in sigs[:10]) if sigs else "Сигналов нет")
        elif act == "diag":
            await cb.message.answer(f"Market data: native async HTTP · шифрование: {'✅' if cipher else '❌'}\n"
                                    f"Чатов: {len(active_chats)} · WEBAPP: {WEBAPP_URL or 'не задан'}\n"
                                    f"Пороги: stage1 {CONF_STAGE1:.0f}, final {CONF_FINAL:.0f}\n"
                                    f"RAM: {_current_rss_mb()} MB · пик: {_rss_mb()} MB")
        elif act == "ram_report":
            x = _runtime_memory_snapshot()
            c = x.get("cache_sizes", {})
            trim = x.get("last_trim") or {}
            protected = x.get("protected_history", {})
            await cb.message.answer(
                "🧠 <b>IXCLY: память сервера</b>\n"
                f"RAM: {x.get('rss_mb')} MB · пик процесса: {x.get('peak_rss_mb')} MB\n"
                f"Порог очистки: {x.get('trim_at_mb')} MB · остановки: {x.get('stop_at_mb')} MB\n"
                f"Кэши: analysis {c.get('analysis', 0)}, backtest {c.get('backtest', 0)}, AI {c.get('ai', 0)}, "
                f"funding {c.get('funding', 0)}, orderbook {c.get('orderbook', 0)}, hourly {c.get('hourly', 0)}, "
                f"intelligence {c.get('intelligence', 0)}, API {c.get('api', 0)}\n"
                f"Защищённые данные: сигналы {protected.get('signals', 0)}, сделки {protected.get('trade_records', 0)}, "
                f"Paper-история {protected.get('paper_records', 0)}\n"
                f"Последняя очистка: {html.escape(str(trim) if trim else 'ещё не выполнялась')}", parse_mode="HTML")
        elif act == "backup":
            await cb.message.answer("💾 Бэкап отправлен." if await telegram_backup("вручную") else "Бэкап не отправлен.")
        elif act == "restore":
            await cb.message.answer("Отправьте файл bot_state.json как документ.")
        elif act == "timers":
            now = time.time()
            await cb.message.answer("⏳ <b>Таймеры:</b>\n" + "\n".join(
                f"{n}: через {max(0, int(next_timers[k] - now))} с" for n, k in (
                    ("🤖 Сканер", "scanner"), ("📈 Позиции", "positions"), ("📰 Новости", "news_watcher"),
                    ("📊 Сигналы/live", "global_signals"), ("🚨 Аномалии", "anomaly"), ("💾 Бэкап", "backup"))), parse_mode="HTML")

    @admin_dp.message(F.document)
    async def admin_on_document(m: types.Message):
        if m.from_user.id not in ADMIN_IDS or not (m.document.file_name or "").endswith(".json"):
            return
        try:
            f = await admin_bot.get_file(m.document.file_id)
            data = json.loads((await backup_bot.download_file(f.file_path)).read().decode("utf-8"))
            n = restore_state(data, force=True)
            load_active_chats()
            await asyncio.to_thread(flush_state)
            await m.answer(f"✅ Восстановлено: пользователей {n}, сигналов {len(STATE['signal_history'])}.")
        except Exception as e:
            await m.answer(f"Ошибка восстановления: {html.escape(str(e))}", parse_mode="HTML")


# =====================================================================
# ОБСЛУЖИВАНИЕ ПАМЯТИ, ЗАПУСК
# =====================================================================
def _trim_timed_cache(store, max_items):
    """Bound timestamp-first caches without discarding the newest useful entries."""
    try:
        while len(store) > max_items:
            oldest = min(store, key=lambda k: float((store[k][0] if isinstance(store[k], (tuple, list)) else 0) or 0))
            store.pop(oldest, None)
    except Exception:
        pass


MEMORY_TRIM_INFO = {}


def _runtime_memory_snapshot():
    with STATE_LOCK:
        protected = {
            "signals": len(STATE.get("signal_history", [])),
            "trade_records": sum(len(u.get("trade_log", [])) for u in STATE.get("users", {}).values() if isinstance(u, dict)),
            "paper_records": sum(len(a.get("history", [])) for u in STATE.get("paper", {}).values()
                                  for a in (u.get("accounts", []) if isinstance(u, dict) else [])),
        }
    return {
        "rss_mb": _current_rss_mb(), "peak_rss_mb": _rss_mb(),
        "trim_at_mb": float(os.getenv("RAM_TRIM_MB", "390")),
        "stop_at_mb": float(os.getenv("RAM_SCAN_STOP_MB", "440")),
        "cache_sizes": {
            "analysis": len(analysis_cache), "backtest": len(bt_cache), "ai": len(_ai_cache),
            "funding": len(_funding_cache), "orderbook": len(_ob_cache), "hourly": len(_hourly_cache),
            "intelligence": cleanup_intelligence.__globals__.get("CACHE_MAX", 0) and len(cleanup_intelligence.__globals__.get("_cache", {})),
            "ai_center": len(_ai_center_cache) if "_ai_center_cache" in globals() else 0,
            "api": len(API_CACHE) if "API_CACHE" in globals() else 0,
        },
        "last_trim": dict(MEMORY_TRIM_INFO or last_scan_info.get("memory_trim") or {}),
        "protected_history": protected,
    }


def _scan_info_for_user(uid):
    diagnostic = str(last_scan_info.get("diagnostic", ""))
    if uid not in ADMIN_IDS and is_memory_guard_reason(diagnostic):
        return None
    return dict(last_scan_info)


def _memory_pressure_cleanup(force=False):
    """Prune volatile/recomputable caches only. Never clear signal_history, trade_log, paper history, or stats."""
    global MEMORY_TRIM_INFO
    before = _current_rss_mb()
    trim_at = float(os.getenv("RAM_TRIM_MB", "390"))
    if not force and (before is None or before < trim_at):
        return {"before_mb": before, "after_mb": before, "trimmed": False, "cache_sizes": {}}
    now = time.time()
    stores = (
        (analysis_cache, 24, 600), (bt_cache, 90, 6 * 3600), (_ai_cache, 64, 1200),
        (_funding_cache, 60, 3600), (_ob_cache, 40, 600), (_hourly_cache, 40, 600),
    )
    for store, max_items, ttl in stores:
        for key, value in list(store.items()):
            try:
                stamp = float(value[0])
            except Exception:
                stamp = 0
            if now - stamp > ttl:
                store.pop(key, None)
        _trim_timed_cache(store, max_items)
    try:
        cleanup_intelligence(now)
    except Exception:
        pass
    for key, value in list(_ai_center_cache.items()):
        if now - float(value[0]) > 300:
            _ai_center_cache.pop(key, None)
    if len(fetch_errors) > 40:
        fetch_errors.clear()  # transient API error diagnostics only; not signal or trade history
    if len(API_CACHE) > 120:
        # API responses are reproducible and short-lived; retain only a bounded recent subset.
        for key in list(API_CACHE)[:len(API_CACHE) - 120]:
            API_CACHE.pop(key, None)
    _release_memory()
    after = _current_rss_mb()
    MEMORY_TRIM_INFO = {"before_mb": before, "after_mb": after, "trimmed": True,
                        "cache_sizes": {"analysis": len(analysis_cache), "backtest": len(bt_cache),
                                        "ai": len(_ai_cache), "ai_center": len(_ai_center_cache), "funding": len(_funding_cache),
                                        "orderbook": len(_ob_cache), "hourly": len(_hourly_cache),
                                        "api": len(API_CACHE)}}
    last_scan_info["memory_trim"] = dict(MEMORY_TRIM_INFO)
    return dict(MEMORY_TRIM_INFO)


def _release_memory():
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


async def memory_janitor_loop():
    await asyncio.sleep(300)
    while True:
        try:
            now = time.time()
            for uid in list(user_res):
                d = user_res[uid]
                for c in [c for c, (ts, _) in d.items() if now - ts > 1800]:
                    d.pop(c, None)
                if not d:
                    user_res.pop(uid, None)
            for c in [c for c, v in list(LIVE.items()) if now - v["ts"] > 1800]:
                LIVE.pop(c, None)
            for c in [c for c, v in analysis_cache.items() if now - v[0] > 600]:
                analysis_cache.pop(c, None)
            cleanup_intelligence(now)
            for key, value in list(_ai_center_cache.items()):
                if now - float(value[0]) > 300:
                    _ai_center_cache.pop(key, None)
            for store, ttl in ((_ai_cache, 3600), (_funding_cache, 3600), (_ob_cache, 600), (_hourly_cache, 600)):
                for k in [k for k, v in store.items() if isinstance(v, (tuple, list)) and v and now - v[0] > ttl]:
                    store.pop(k, None)
            for k, ts in list(_alert_sent.items()):
                if now - ts > 24 * 3600:
                    _alert_sent.pop(k, None)
            for store in (scan_skip, exchange_bans, BAD_MODELS):
                for k in [k for k, t in store.items() if t < now]:
                    store.pop(k, None)
            if len(fetch_errors) > 100:
                fetch_errors.clear()  # transient fetch-error buffer only
            if len(nav_stack) > 1000:
                nav_stack.clear()  # UI navigation stack only
            if (_current_rss_mb() or 0) >= float(os.getenv("RAM_TRIM_MB", "390")):
                await asyncio.to_thread(_memory_pressure_cleanup, True)
            else:
                await asyncio.to_thread(_release_memory)
        except Exception as e:
            log.warning(f"janitor: {e}")
        await asyncio.sleep(300)


async def churn_offer_loop():
    """Раз в сутки в 10:00 МСК: тем, кто скоро уйдёт и давно не заходил, — скидка 30% на 24 часа."""
    while True:
        msk = datetime.now(timezone(timedelta(hours=3)))
        target = msk.replace(hour=10, minute=0, second=0, microsecond=0)
        if msk >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - msk).total_seconds())
        now = time.time()
        for uid_str, u in list(_users().items()):
            try:
                if not isinstance(u, dict) or not uid_str.isdigit():
                    continue
                uid = int(uid_str)
                a = u.get("access") or {}
                if a.get("free") or uid in ADMIN_IDS:
                    continue
                left = a.get("until", 0) - now
                idle = now - u.get("last_seen", 0)
                if -3 * 86400 < left <= 86400 and idle > 3 * 86400:
                    DISCOUNT_OFFERS[uid] = {"pct": 30, "until": now + 86400}
                    await _tell(uid, "🎁 <b>Скучаем!</b> Скидка 30% на продление — 24 часа.\nВыберите тариф в /buy.", paywall_kb(uid))
                    await asyncio.sleep(0.05)
            except Exception as e:
                log.info(f"churn {uid_str}: {e}")


async def heartbeat_loop():
    while True:
        await asyncio.sleep(600)
        log.info("♥ аптайм %d мин, RAM %s МБ, получателей %d, сигналов %d, live %d",
                 (time.time() - stats["started"]) // 60, _rss_mb(), len(active_chats), len(STATE["signal_history"]), len(LIVE))


async def run_polling(dispatcher, tg_bot, name, stop_event):
    attempt = 0
    while not stop_event.is_set():
        try:
            attempt += 1
            log.info(f"{name}: запуск поллинга (попытка {attempt})")
            try:
                await tg_bot.delete_webhook(drop_pending_updates=(attempt == 1))
            except Exception:
                pass
            await dispatcher.start_polling(tg_bot, handle_signals=False, close_bot_session=False, polling_timeout=30)
            if not stop_event.is_set():
                log.warning(f"{name}: поллинг завершился, перезапуск через 5 сек")
                await asyncio.sleep(5)
                attempt = 0
        except asyncio.CancelledError:
            raise
        except (TelegramNetworkError, aiohttp.ClientError, ConnectionError) as e:
            log.warning(f"{name}: сетевая ошибка ({e}), повтор через {min(2 ** attempt, 60)} сек")
            await asyncio.sleep(min(2 ** attempt, 60))
        except TelegramConflictError:
            await asyncio.sleep(min(2 ** attempt, 30))
        except Exception as e:
            log.error(f"{name}: непредвиденная ошибка: {e}")
            await asyncio.sleep(10)




# =====================================================================
# ЦЕНА В РЕАЛЬНОМ ВРЕМЕНИ: цикл цен и поток Server-Sent Events для приложения
# =====================================================================
PRICES = {}         # coin -> {"price", "ts"}
STREAM_COINS = {}   # coin -> ts последнего открытия экрана монеты


async def price_feed_loop():
    """Цена каждые 8 секунд для монет из сигналов, позиций и открытых экранов приложения."""
    await asyncio.sleep(30)
    while True:
        try:
            now = time.time()
            for c in [c for c, t in list(STREAM_COINS.items()) if now - t > 90]:
                STREAM_COINS.pop(c, None)
            watch = {x["coin"] for x in get_active_signals()}
            for u in list(_users().values()):
                if isinstance(u, dict):
                    watch |= set((u.get("open_positions") or {}).keys())
            watch |= set(list(STREAM_COINS.keys()))
            for coin in sorted(watch)[:30]:
                p = await get_simple_price(coin)
                if p:
                    PRICES[coin] = {"price": p, "ts": time.time()}
                await asyncio.sleep(0.1)
        except Exception as e:
            log.info(f"price_feed: {e}")
        await asyncio.sleep(8)



async def news_publisher_loop(stop_event):
    """Запускает отдельного Telegram-бота новостей внутри того же Render-процесса."""
    token = clean_key(os.getenv("IXCLY_NEWS_BOT_TOKEN"))
    if not token:
        log.warning("IXCLY NEWS не запущен: добавьте IXCLY_NEWS_BOT_TOKEN в Environment Render")
        return
    os.environ.setdefault("IXCLY_NEWS_CHANNEL_ID", "@ixcly_news")
    os.environ.setdefault("IXCLY_NEWS_CHANNEL_URL", NEWS_CHANNEL_URL)
    os.environ.setdefault("IXCLY_NEWS_DB", os.path.join(_DATA_DIR, "ixcly_news_state.sqlite3"))
    from ixcly_news_bot import run_publisher, export_state, import_state, set_state_callback
    import_state(STATE.get("news_publisher_state") or {})
    def _persist_news_state():
        STATE["news_publisher_state"] = export_state()
        state_touch(True)
    set_state_callback(_persist_news_state)
    while not stop_event.is_set():
        try:
            await run_publisher(stop_event)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("IXCLY NEWS publisher failed to initialize; retrying in 60 seconds")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass


async def main():
    global http, news_lock, scan_lock, MAIN_LOOP, TG_SEM, ANALYZE_SEM
    MAIN_LOOP = asyncio.get_running_loop()
    keep_alive()
    news_lock, scan_lock = asyncio.Lock(), asyncio.Lock()
    TG_SEM = asyncio.Semaphore(20)
    ANALYZE_SEM = asyncio.Semaphore(3)
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), headers={"User-Agent": "IXCLY/3.1"})
    load_state_file()
    restore_quant_price_history()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    background, pollers = [], []
    try:
        for tg_bot, key in ((bot, "main_bot"), (admin_bot, "admin_bot")):
            if tg_bot:
                try:
                    runtime[key] = f"@{(await tg_bot.get_me()).username}"
                except Exception as e:
                    log.error(f"get_me ({key}): {e}")
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            if admin_bot:
                await admin_bot.delete_webhook(drop_pending_updates=True)
            await bot.set_my_commands([
                types.BotCommand(command="start", description="🚀 Открыть IXCLY"),
                types.BotCommand(command="tutorial", description="🎓 Обучение новичка"),
                types.BotCommand(command="market", description="🧠 AI-анализ рынка"),
                types.BotCommand(command="forecast", description="🔮 Сценарии монеты"),
                types.BotCommand(command="radar", description="📡 Радар движений"),
                types.BotCommand(command="whales", description="🐋 Крупные сделки"),
                types.BotCommand(command="liquidations", description="💥 Ликвидации за час"),
                types.BotCommand(command="risk", description="🛡 Мой риск"),
                types.BotCommand(command="ai_report", description="📊 Полный AI-отчёт"),
                types.BotCommand(command="strategy_stats", description="🧩 Статистика стратегий"),
                types.BotCommand(command="accuracy", description="🎯 Точность сигналов"),
                types.BotCommand(command="paper_report", description="📒 Paper Trading отчёт"),
            ])
            if WEBAPP_URL:
                await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="IXCLY App", web_app=WebAppInfo(url=webapp_link())))
                log.info(f"Mini App: {WEBAPP_URL}")
            else:
                log.warning("WEBAPP_URL не задан: мини-приложение недоступно из бота")
        except Exception as e:
            log.warning(f"Инициализация бота: {e}")

        if POLL_START_DELAY > 0:
            log.info(f"Ждём {POLL_START_DELAY} с, пока остановится предыдущий экземпляр…")
            try:
                await asyncio.wait_for(stop.wait(), timeout=POLL_START_DELAY)
            except asyncio.TimeoutError:
                pass
        if stop.is_set():
            return

        restored = await telegram_restore()
        if restored:
            log.info(f"✅ Состояние восстановлено из бэкапа, пользователей: {restored}")
        restored_quant = restore_quant_price_history()
        log.info("Quant market history restored: %s assets", restored_quant)
        load_active_chats()
        normalize_signals()
        for _uid in list(_users()):
            if str(_uid).isdigit():
                _init_trial(int(_uid))
        # После каждого запуска создаём свежую контрольную точку после восстановления.
        state_touch(True)
        gc.freeze()
        asyncio.create_task(discover_models())

        background = [asyncio.create_task(c()) for c in (
            scanner_loop, live_monitor_loop, price_feed_loop, global_signal_monitor_loop, position_monitor_loop,
            news_watcher_loop, price_alerts_loop, morning_briefing_loop, daily_digest_loop, daily_call_loop,
            anomaly_scanner_loop, liquidation_stream_loop, state_flush_loop, access_reminder_loop, churn_offer_loop, backup_loop, heartbeat_loop, memory_janitor_loop, quant_observation_loop)]
        background.append(asyncio.create_task(update_market_bias()))
        if clean_key(os.getenv("IXCLY_NEWS_BOT_TOKEN")):
            background.append(asyncio.create_task(news_publisher_loop(stop)))
        else:
            log.warning("IXCLY NEWS отключён: не задан IXCLY_NEWS_BOT_TOKEN; торговый бот продолжает работу")

        pollers.append(asyncio.create_task(run_polling(dp, bot, "основной бот", stop)))
        if admin_bot and admin_dp:
            pollers.append(asyncio.create_task(run_polling(admin_dp, admin_bot, "админ-бот", stop)))
        log.info(f"🚀 IXCLY PRO v{VERSION} запущен")
        waiter = asyncio.create_task(stop.wait())
        await asyncio.wait(pollers + [waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
    except Exception as e:
        log.error(f"Критическая ошибка при запуске: {e}", exc_info=True)
    finally:
        log.info("Останавливаю бота…")
        for t in pollers:
            t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        try:
            _persist_quant_price_history(force=True)
            await asyncio.wait_for(telegram_backup("перед остановкой"), timeout=15)
        except Exception:
            await asyncio.to_thread(flush_state)
        for t in background:
            t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        for tg_bot in (bot, admin_bot):
            if tg_bot:
                try:
                    await tg_bot.session.close()
                except Exception:
                    pass
        await http.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Бот остановлен")
