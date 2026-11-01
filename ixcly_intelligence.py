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
    """Deterministic quality score 0..100; never invents a probability of profit."""
    base = max(0.0, min(100.0, _num(res.get('strength'), 0)))
    score = base * 0.55
    probs = res.get('long_prob') or res.get('tf_probs') or {}
    vals = [_num(v, -1) for v in probs.values()]
    vals = [v for v in vals if 0 <= v <= 100]
    if len(vals) >= 2:
        aligned = sum((v >= 50) == (vals[0] >= 50) for v in vals) / len(vals)
        score += aligned * 18
    bt = res.get('bt') or {}
    trades = int(_num(bt.get('trades'), 0))
    if trades >= 10:
        score += 8
    elif trades >= 5:
        score += 5
    elif trades:
        score += 2
    if res.get('fake_breakout'):
        score -= 18
    regime = str(res.get('regime') or '').lower()
    if 'sideway' in regime or 'боков' in regime:
        score -= 5
    risk = str(res.get('risk_lvl') or '').lower()
    if 'высок' in risk or 'high' in risk:
        score -= 8
    entry, sl = _num(res.get('entry')), _num((res.get('sls') or [0])[0])
    if entry > 0 and sl > 0:
        dist = abs(entry - sl) / entry * 100
        if dist > 8:
            score -= 10
        elif dist < 0.15:
            score -= 6
        else:
            score += 5
    rr = _num(res.get('rr'), 0)
    if rr >= 2:
        score += 6
    elif rr and rr < 1.2:
        score -= 8
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



def indicator_snapshot(df):
    """Compact, JSON-safe snapshot of existing technical indicators."""
    try:
        if df is None or len(df) < 25:
            return {}
        r = df.iloc[-1]
        price = _num(r.get('close'))
        atr = _num(r.get('atr'))
        return {
            'rsi': round(_num(r.get('rsi')), 2),
            'adx': round(_num(r.get('adx')), 2),
            'plus_di': round(_num(r.get('plus_di')), 2),
            'minus_di': round(_num(r.get('minus_di')), 2),
            'vwap': round(_num(r.get('vwap')), 10),
            'vwap_side': 'above' if price > _num(r.get('vwap')) else 'below',
            'obv_trend': 'inflow' if _num(r.get('obv')) > _num(r.get('obv_ema')) else 'outflow',
            'atr_pct': round(atr / price * 100, 3) if price > 0 else None,
            'rel_volume': round(_num(r.get('rel_vol'), 1), 2),
            'bull_divergence': bool(r.get('div_bull', False)),
            'bear_divergence': bool(r.get('div_bear', False)),
            'squeeze': bool(r.get('squeeze', False)),
        }
    except Exception:
        return {}


def breakout_assessment(df, lookback=20):
    """Closed-candle breakout/fakeout check; never treats an intrabar wick as confirmation."""
    try:
        if df is None or len(df) < lookback + 2:
            return {'state': 'unknown', 'score': 0, 'level': None}
        last = df.iloc[-1]
        prev = df.iloc[-lookback-1:-1]
        hi, lo = float(prev['high'].max()), float(prev['low'].min())
        o, h, l, c = map(float, (last['open'], last['high'], last['low'], last['close']))
        rng = max(1e-12, h - l)
        body_ratio = abs(c - o) / rng
        relv = _num(last.get('rel_vol'), 1.0)
        atr = max(_num(last.get('atr')), 1e-12)
        fake_up = h > hi and c < hi and relv >= 1.2
        fake_down = l < lo and c > lo and relv >= 1.2
        confirmed_up = c > hi + 0.05 * atr and body_ratio >= 0.45 and relv >= 1.15
        confirmed_down = c < lo - 0.05 * atr and body_ratio >= 0.45 and relv >= 1.15
        if fake_up:
            state, direction, score, level = 'fake_up', 'SHORT', -1, hi
        elif fake_down:
            state, direction, score, level = 'fake_down', 'LONG', -1, lo
        elif confirmed_up:
            state, direction, score, level = 'breakout_up', 'LONG', 1, hi
        elif confirmed_down:
            state, direction, score, level = 'breakout_down', 'SHORT', 1, lo
        else:
            state, direction, score, level = 'range', None, 0, None
        return {'state': state, 'direction': direction, 'score': score,
                'level': round(level, 10) if level is not None else None,
                'body_ratio': round(body_ratio, 3), 'rel_volume': round(relv, 2)}
    except Exception:
        return {'state': 'unknown', 'score': 0, 'level': None}


def anti_fomo_assessment(df, side=None):
    """Penalize stretched entries; only block exceptionally extended conditions."""
    try:
        if df is None or len(df) < 25:
            return {'blocked': False, 'penalty': 0, 'stretch_atr': 0.0, 'reason': ''}
        r = df.iloc[-1]
        price, ema, atr = _num(r.get('close')), _num(r.get('ema20')), _num(r.get('atr'))
        if price <= 0 or atr <= 0:
            return {'blocked': False, 'penalty': 0, 'stretch_atr': 0.0, 'reason': ''}
        direction = str(side or ('LONG' if price >= ema else 'SHORT')).upper()
        stretch = max(0.0, (price - ema) / atr) if direction == 'LONG' else max(0.0, (ema - price) / atr)
        rsi = _num(r.get('rsi'), 50)
        extreme_rsi = (direction == 'LONG' and rsi >= 82) or (direction == 'SHORT' and rsi <= 18)
        blocked = stretch >= 4.2 and extreme_rsi
        penalty = 14 if stretch >= 3.2 else 9 if stretch >= 2.6 else 0
        if extreme_rsi:
            penalty += 5
        reason = (f'цена удалена от EMA20 на {stretch:.1f} ATR; RSI {rsi:.0f}'
                  if penalty or blocked else '')
        return {'blocked': bool(blocked), 'penalty': min(20, penalty),
                'stretch_atr': round(stretch, 2), 'reason': reason}
    except Exception:
        return {'blocked': False, 'penalty': 0, 'stretch_atr': 0.0, 'reason': ''}


def news_risk_assessment(news, coin=None, now=None):
    """Relevance-aware news risk: coin-specific by default, broad only for market-wide events."""
    now = time.time() if now is None else now
    news = news if isinstance(news, dict) else {}
    age = max(0.0, now - _num(news.get('ts')))
    if not news or age > 6 * 3600:
        return {'risk': 0, 'severity': 'none', 'scope': 'none', 'penalty': 0, 'relevant': False}
    title = str(news.get('title_ru') or '')
    summary = str(news.get('summary') or '')
    impact = str(news.get('impact') or '')
    coins_raw = str(news.get('coins') or '').upper()
    tokens = {x for x in coins_raw.replace(';', ',').split(',') if x and x != 'НЕ УКАЗАНЫ В ИСТОЧНИКЕ'}
    text = f'{title} {summary} {impact}'.lower()
    global_terms = ('bitcoin', 'btc', 'crypto market', 'крипторын', 'рынок криптовалют', 'системн', 'бирж', 'binance')
    broad = any(x in text for x in global_terms) or len(tokens) >= 4
    target = str(coin or '').upper()
    relevant_coin = bool(target and (target in tokens or target == 'BTC' and broad))
    relevant = broad or relevant_coin
    if not relevant:
        return {'risk': 0, 'severity': 'low', 'scope': 'unrelated', 'penalty': 0, 'relevant': False}
    severity = str(news.get('severity') or '').lower()
    if severity not in ('critical', 'high', 'medium', 'low'):
        severity = 'high' if any(x in text for x in ('hack', 'взлом', 'банкрот', 'insolven', 'depeg', 'замороз')) else 'medium'
    base = {'critical': 90, 'high': 65, 'medium': 35, 'low': 12}[severity]
    recency = max(0.25, 1.0 - age / (6 * 3600))
    risk = round(base * recency)
    penalty = min(8 if broad else 14, round(risk * (0.10 if broad else 0.20)))
    return {'risk': risk, 'severity': severity, 'scope': 'market' if broad else 'coin',
            'penalty': penalty, 'relevant': True, 'age_min': round(age / 60)}


def market_panic_assessment(btc_change_pct=0.0, btc_atr_pct=0.0, breadth_down_pct=None):
    """Risk-state detector; warning state is not itself a global trading shutdown."""
    move = abs(_num(btc_change_pct))
    atrp = _num(btc_atr_pct)
    breadth = _num(breadth_down_pct, -1.0)
    score = min(100, int(move * 18 + max(0.0, atrp - 1.5) * 12 + (max(0.0, breadth) * 0.25)))
    level = 'critical' if score >= 75 or move >= 4.0 else 'high' if score >= 50 or move >= 2.5 else 'elevated' if score >= 25 else 'normal'
    return {'score': score, 'level': level, 'btc_change_pct': round(_num(btc_change_pct), 2),
            'btc_atr_pct': round(atrp, 3)}


def scanner_rank_score(tf_data, quick_strength=50.0):
    """Transparent candidate ranking from existing OHLCV only; not a profit probability."""
    try:
        a, b = tf_data.get('4h') or {}, tf_data.get('1d') or {}
        p4, p1 = _num(a.get('long_prob'), 50), _num(b.get('long_prob'), 50)
        aligned = (p4 >= 50) == (p1 >= 50)
        adx = _num(a.get('adx'))
        qv = _num(a.get('qv24'))
        df = a.get('df')
        ind = indicator_snapshot(df)
        score = _num(quick_strength, 50) * 0.55
        score += 18 if aligned else -8
        score += min(10, max(0, (adx - 12) * 0.35))
        score += min(8, max(0, math.log10(max(1.0, qv)) - 6) * 2)
        score += 4 if ind.get('rel_volume', 1) >= 1.5 else 0
        score -= 10 if ind.get('rsi', 50) > 82 or ind.get('rsi', 50) < 18 else 0
        return round(max(0, min(100, score)), 2)
    except Exception:
        return round(max(0, min(100, _num(quick_strength, 50))), 2)


def signal_memory_summary(history, limit=120):
    """Small explainable outcome memory derived from already-persisted signal history."""
    closed = [x for x in (history or []) if x.get('status') in ('win', 'loss')]
    closed.sort(key=lambda x: _num(x.get('closed_ts') or x.get('ts')), reverse=True)
    closed = closed[:max(1, min(300, int(limit)))]
    n = len(closed)
    wins = sum(1 for x in closed if x.get('status') == 'win')
    avg_r = sum(_num(x.get('r')) for x in closed) / n if n else 0.0
    groups = {}
    for x in closed:
        regime = str((x.get('market_regime') or {}).get('name') if isinstance(x.get('market_regime'), dict) else x.get('memory_regime') or 'unknown')
        key = regime or 'unknown'
        g = groups.setdefault(key, {'trades': 0, 'wins': 0, 'r_sum': 0.0})
        g['trades'] += 1
        g['wins'] += int(x.get('status') == 'win')
        g['r_sum'] += _num(x.get('r'))
    by_regime = {k: {'trades': v['trades'], 'win_rate': round(v['wins'] / v['trades'] * 100, 1),
                     'avg_r': round(v['r_sum'] / v['trades'], 3)} for k, v in groups.items()}
    return {'trades': n, 'wins': wins, 'losses': n - wins,
            'win_rate': round(wins / n * 100, 1) if n else None,
            'avg_r': round(avg_r, 3) if n else None, 'by_regime': by_regime}


def adaptive_scanner_profile(history, default_threshold=66, regime=None):
    """Conservative adaptive threshold; prefers matching regime stats when sample is sufficient."""
    mem = signal_memory_summary(history, 120)
    n, wr = mem['trades'], mem['win_rate']
    selected = mem.get('by_regime', {}).get(str(regime)) if regime else None
    used_regime = bool(selected and selected.get('trades', 0) >= 8)
    if used_regime:
        n, wr = selected['trades'], selected['win_rate']
    delta = 0
    min_sample = 8 if used_regime else 12
    if n >= min_sample and wr is not None:
        delta = 6 if wr < 35 else 3 if wr < 45 else -1 if wr >= 65 else 0
    return {'threshold': round(max(55, min(82, _num(default_threshold, 66) + delta)), 1),
            'adjustment': delta, 'sample': n, 'win_rate': wr, 'regime': regime or 'all',
            'mode': 'defensive' if delta > 0 else 'selective' if delta < 0 else 'balanced'}



def atr_stop_multiplier(adx=None, atr_pct=None):
    """Minimum ATR distance for structural stop, widened for volatile markets."""
    trend = _num(adx)
    vol = _num(atr_pct)
    if vol >= 4.0:
        return 2.0
    if vol >= 2.0:
        return 1.65
    if trend >= 30:
        return 1.4
    if trend < 18:
        return 1.15
    return 1.3

def atr_target_multipliers(adx=None, atr_pct=None):
    """Volatility/trend-aware TP multiples; SL remains structure + ATR based in main.py."""
    trend = _num(adx)
    vol = _num(atr_pct)
    if trend >= 30 and vol < 4:
        return (1.8, 3.0, 4.5)
    if trend < 18 or vol >= 4:
        return (1.25, 2.0, 3.0)
    return (1.5, 2.5, 4.0)

def cleanup(now=None):
    now = time.time() if now is None else now
    for key, (ts, _) in list(_cache.items()):
        if now - ts > CACHE_TTL:
            _cache.pop(key, None)
    while len(_cache) > CACHE_MAX:
        _cache.popitem(last=False)
