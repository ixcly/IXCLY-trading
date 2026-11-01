# -*- coding: utf-8 -*-
"""IXCLY lightweight signal intelligence. No ML frameworks; designed for 512 MB RAM."""
from __future__ import annotations
import math
import time
from collections import OrderedDict

CACHE_MAX = 256
CACHE_TTL = 180
_cache = OrderedDict()


def _num(value, default=0.0):
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def bounded_cache_get(key, now=None):
    now = time.time() if now is None else now
    item = _cache.get(key)
    if not item:
        return None
    if now - item[0] > CACHE_TTL:
        _cache.pop(key, None)
        return None
    _cache.move_to_end(key)
    return item[1]


def bounded_cache_set(key, value, now=None):
    now = time.time() if now is None else now
    _cache[key] = (now, value)
    _cache.move_to_end(key)
    while len(_cache) > CACHE_MAX:
        _cache.popitem(last=False)


def score_signal(res):
    """Deterministic setup-quality score 0..100, not a win probability."""
    base = max(0.0, min(100.0, _num(res.get('strength'), 0)))
    score = base * 0.50
    is_long = bool(res.get('is_long', str(res.get('side', '')).upper() in ('LONG', 'BUY')))
    probs = res.get('tf_probs') or res.get('long_prob') or {}
    vals = [_num(v, -1) for v in probs.values()]
    vals = [v for v in vals if 0 <= v <= 100]
    if vals:
        aligned = sum((v >= 50) == is_long for v in vals) / len(vals)
        score += aligned * 22
        if len(vals) >= 3 and aligned < 0.5:
            score -= 7
    bt = res.get('bt') or {}
    trades = int(_num(bt.get('trades'), 0))
    wr = _num(bt.get('win_rate', bt.get('winrate', 0)), 0)
    if trades >= 20: score += 7
    elif trades >= 10: score += 5
    elif trades >= 5: score += 2
    if trades >= 10 and wr:
        score += max(-5, min(5, (wr - 50) * 0.2))
    if res.get('fake_breakout'): score -= 20
    regime = str((res.get('market_regime') or {}).get('name') or res.get('regime') or '').lower()
    if 'range' in regime or 'sideway' in regime or 'боков' in regime: score -= 5
    if ('downtrend' in regime and is_long) or ('uptrend' in regime and not is_long): score -= 7
    risk = str(res.get('risk_lvl') or '').lower()
    if 'высок' in risk or 'high' in risk: score -= 8
    entry = _num(res.get('entry'))
    sls = res.get('sls') or []
    sl = _num(sls[0]) if sls else 0
    if entry > 0 and sl > 0:
        dist = abs(entry - sl) / entry * 100
        if dist > 8: score -= 10
        elif dist < 0.15: score -= 6
        else: score += 5
    rr = _num(res.get('rr'), 0)
    if not rr and entry > 0 and sl > 0:
        tps = res.get('tps') or []
        if tps:
            rr = abs(_num(tps[0]) - entry) / abs(entry - sl)
    if rr >= 2: score += 7
    elif rr >= 1.5: score += 3
    elif rr and rr < 1.2: score -= 10
    anomaly = res.get('volume_anomaly')
    if isinstance(anomaly, dict) and _num(anomaly.get('ratio')) >= 2.5: score += 2
    return round(max(0.0, min(100.0, score)), 1)


def market_regime(df):
    """Classify OHLCV dataframe using close/EMA and ATR%; returns small JSON-safe dict."""
    try:
        if df is None or len(df) < 30:
            return {'name': 'unknown', 'volatility': 'unknown', 'trend_pct': None}
        close = df['close'].astype(float)
        last = float(close.iloc[-1])
        ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])
        trend = (ema20 / ema50 - 1) * 100 if ema50 else 0
        high, low = df['high'].astype(float), df['low'].astype(float)
        prev = close.shift(1)
        tr = (high - low).to_frame('hl')
        tr['hc'] = (high - prev).abs()
        tr['lc'] = (low - prev).abs()
        atr = float(tr.max(axis=1).rolling(14).mean().iloc[-1])
        atr_pct = atr / last * 100 if last else 0
        if abs(trend) >= 0.8:
            name = 'uptrend' if trend > 0 else 'downtrend'
        else:
            name = 'range'
        vol = 'high' if atr_pct >= 3 else 'medium' if atr_pct >= 1 else 'low'
        return {'name': name, 'volatility': vol, 'trend_pct': round(trend, 3), 'atr_pct': round(atr_pct, 3)}
    except Exception:
        return {'name': 'unknown', 'volatility': 'unknown', 'trend_pct': None}


def volume_anomaly(df, lookback=30, threshold=2.5):
    try:
        if df is None or len(df) < lookback + 2 or 'volume' not in df:
            return None
        v = df['volume'].astype(float)
        base = float(v.iloc[-lookback-1:-1].median())
        current = float(v.iloc[-1])
        if base <= 0:
            return None
        ratio = current / base
        if ratio < threshold:
            return None
        return {'ratio': round(ratio, 2), 'volume': current, 'baseline': base}
    except Exception:
        return None


def liquidity_snapshot(order_book):
    """Summarize bid/ask depth. Input is ccxt order book dict; no retained raw book."""
    try:
        bids = order_book.get('bids') or []
        asks = order_book.get('asks') or []
        bid_usd = sum(_num(p) * _num(q) for p, q, *_ in bids[:20])
        ask_usd = sum(_num(p) * _num(q) for p, q, *_ in asks[:20])
        total = bid_usd + ask_usd
        return {'bid_depth_usdt': round(bid_usd, 2), 'ask_depth_usdt': round(ask_usd, 2),
                'imbalance': round((bid_usd - ask_usd) / total, 4) if total else 0.0}
    except Exception:
        return {'bid_depth_usdt': 0.0, 'ask_depth_usdt': 0.0, 'imbalance': 0.0}


def funding_oi_snapshot(funding=None, open_interest=None, previous_oi=None):
    f = _num(funding, float('nan'))
    oi = _num(open_interest, float('nan'))
    prev = _num(previous_oi, float('nan'))
    return {'funding_rate': round(f, 6) if math.isfinite(f) else None,
            'open_interest': round(oi, 2) if math.isfinite(oi) else None,
            'oi_change_pct': round((oi / prev - 1) * 100, 2) if math.isfinite(oi) and math.isfinite(prev) and prev > 0 else None}


def risk_position_size(deposit, risk_pct, entry, stop, max_notional=None):
    deposit, risk_pct, entry, stop = map(_num, (deposit, risk_pct, entry, stop))
    if deposit <= 0 or entry <= 0 or stop <= 0 or not 0 < risk_pct <= 10 or entry == stop:
        raise ValueError('Некорректные параметры риска')
    risk_cash = deposit * risk_pct / 100
    distance = abs(entry - stop) / entry
    notional = risk_cash / distance
    if max_notional is not None:
        notional = min(notional, max(0.0, _num(max_notional)))
    return {'risk_usdt': round(risk_cash, 2), 'position_usdt': round(notional, 2),
            'quantity': round(notional / entry, 10), 'stop_distance_pct': round(distance * 100, 4)}


def cleanup(now=None):
    now = time.time() if now is None else now
    for key, (ts, _) in list(_cache.items()):
        if now - ts > CACHE_TTL:
            _cache.pop(key, None)
    while len(_cache) > CACHE_MAX:
        _cache.popitem(last=False)
