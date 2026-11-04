# -*- coding: utf-8 -*-
"""IXCLY 5.0 Quant Intelligence. Bounded NumPy models; no ML frameworks; designed for 512 MB RAM."""
from __future__ import annotations
import math
import time
from datetime import datetime, timezone
from collections import OrderedDict
import numpy as np

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



def breakeven_trigger_reached(pnl_pct, trigger_pct=2.5):
    """Move a protective stop to entry only after the configured directional profit."""
    return _num(pnl_pct, 0.0) >= _num(trigger_pct, 2.5)


def close_warning_confirmed(long_probabilities, side, min_short_tf=70.0, min_slow_tf=65.0):
    """Require all four timeframes to confirm adverse direction before a risk warning."""
    if not isinstance(long_probabilities, dict) or str(side).upper() not in ("LONG", "SHORT"):
        return False
    against = (lambda value: 100.0 - _num(value, 50.0)) if str(side).upper() == "LONG" else (lambda value: _num(value, 50.0))
    required = ("15m", "1h", "4h", "1d")
    if any(tf not in long_probabilities for tf in required):
        return False
    scores = {tf: against(long_probabilities[tf]) for tf in required}
    return (scores["15m"] >= min_short_tf and scores["1h"] >= min_short_tf and
            scores["4h"] >= min_slow_tf and scores["1d"] >= min_slow_tf)


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


def forecast_scenarios(tf_data, side=None, news_risk=None):
    """Build ATR-based conditional scenarios; levels are not claimed probabilities."""
    if not isinstance(tf_data, dict):
        return {"status": "insufficient_data", "message": "Недостаточно таймфреймов"}
    info = tf_data.get("4h") or tf_data.get("1h") or {}
    df = info.get("df")
    try:
        price = _num(info.get("price"), _num(df["close"].iloc[-1]) if df is not None and len(df) else 0)
        atr = _num(info.get("atr"), _num(df["atr"].iloc[-1]) if df is not None and "atr" in df and len(df) else 0)
    except Exception:
        price, atr = _num(info.get("price")), _num(info.get("atr"))
    probs = {k: _num(v.get("long_prob"), 50) for k, v in (tf_data or {}).items() if k in ("15m", "1h", "4h", "1d") and isinstance(v, dict)}
    if price <= 0 or atr <= 0:
        return {"status": "insufficient_data", "message": "Нет корректной цены или ATR"}
    weights = {"15m": .1, "1h": .2, "4h": .3, "1d": .4}
    available = [(k, weights[k]) for k in weights if k in probs]
    weighted = (sum(probs[k] * w for k, w in available) / sum(w for _, w in available)) if available else 50
    direction = str(side or ("LONG" if weighted >= 55 else "SHORT" if weighted <= 45 else "NEUTRAL")).upper()
    aligned = sum(1 for v in probs.values() if (v >= 50) == (direction == "LONG")) if direction != "NEUTRAL" else 0
    total = len(probs)
    strength = max(weighted, 100-weighted)
    risk = (news_risk or {}).get("severity", "low")
    mult = 0.8 if risk in ("high", "critical") else 1.0
    return {"status": "ok", "price": round(price, 10), "atr": round(atr, 10), "atr_pct": round(atr/price*100, 3),
            "direction": direction, "tf_alignment": f"{aligned}/{total}" if total else "нет данных",
            "strength_index": round(strength, 1), "bull_trigger": round(price + atr*.35, 10),
            "bear_trigger": round(price - atr*.35, 10), "bull_target": round(price + atr*1.8*mult, 10),
            "bear_target": round(price - atr*1.8*mult, 10), "range_low": round(price-atr*.65, 10),
            "range_high": round(price+atr*.65, 10), "news_risk": risk,
            "note": "Условные сценарии по ATR и таймфреймам; триггер требует закрытия свечи, это не гарантия движения."}


def early_movement_assessment(df, oi_change_pct=None, book_imbalance=None, funding_rate=None, vol_anomaly=None):
    """Conservative watch-only detector. It never creates an entry signal."""
    score, reasons = 0, []
    anomaly = vol_anomaly or volume_anomaly(df)
    if anomaly:
        ratio = _num(anomaly.get("ratio"))
        if ratio >= 2.0:
            score += 30; reasons.append(f"объём x{ratio:.1f}")
    oi = _num(oi_change_pct, float("nan"))
    if math.isfinite(oi) and abs(oi) >= .5:
        score += 20; reasons.append(f"OI {oi:+.2f}%")
    book = _num(book_imbalance, float("nan"))
    if math.isfinite(book) and abs(book-.5) >= .12:
        score += 20; reasons.append(f"перевес стакана {book:.0%} bids")
    funding = _num(funding_rate, float("nan"))
    if math.isfinite(funding) and abs(funding) >= .0005:
        score += 10; reasons.append(f"funding {funding*100:.3f}%")
    regime = market_regime(df)
    if regime.get("volatility") == "high":
        score += 10; reasons.append("повышенная волатильность")
    try:
        if df is not None and len(df) >= 3:
            prev = _num(df["close"].iloc[-2]); last = _num(df["close"].iloc[-1])
            atr = _num(df["atr"].iloc[-1]) if "atr" in df else 0
            if prev > 0 and atr > 0 and abs(last-prev)/atr >= .7:
                score += 15; reasons.append("ускорение цены относительно ATR")
    except Exception:
        pass
    score = min(100, score)
    state = "watch" if score >= 45 and len(reasons) >= 2 else "quiet"
    return {"score": score, "state": state, "reasons": reasons[:5], "volume": anomaly,
            "message": "Возможное движение — наблюдать, не входить автоматически" if state == "watch" else "Подтверждений раннего движения недостаточно"}


def liquidation_risk_assessment(funding=None, oi_change_pct=None, price_change_pct=None):
    """Crowding/flush risk proxy. Not a liquidation-price map."""
    f, oi, px = _num(funding, float("nan")), _num(oi_change_pct, float("nan")), _num(price_change_pct, float("nan"))
    long_risk = short_risk = 0
    reasons = []
    if math.isfinite(f):
        if f >= .0005: long_risk += 30; reasons.append("положительный высокий funding: перегрузка лонгов")
        elif f <= -.0005: short_risk += 30; reasons.append("отрицательный высокий funding: перегрузка шортов")
    if math.isfinite(oi) and math.isfinite(px):
        if oi > .7 and px < -.4: long_risk += 35; reasons.append("OI растёт на падении цены")
        if oi > .7 and px > .4: short_risk += 25; reasons.append("OI растёт на росте цены: риск поздних шортов")
        if oi < -.7 and px < -.4: long_risk += 15; reasons.append("OI снижается вместе с ценой: возможное закрытие лонгов")
        if oi < -.7 and px > .4: short_risk += 15; reasons.append("OI снижается на росте цены: возможное закрытие шортов")
    risk = max(long_risk, short_risk)
    return {"long_crowding_risk": min(100,long_risk), "short_crowding_risk": min(100,short_risk),
            "level": "high" if risk >= 55 else "medium" if risk >= 25 else "low",
            "reasons": reasons, "note": "Оценка перегруженности по funding/OI/цене, не реальные уровни ликвидаций."}


def signal_quality_breakdown(res):
    """Human-readable components behind the existing 0..100 signal score."""
    score = score_signal(res)
    probs = res.get("tf_probs") or res.get("long_prob") or {}
    vals = [_num(v, -1) for v in probs.values() if 0 <= _num(v, -1) <= 100] if isinstance(probs, dict) else []
    aligned = sum(1 for v in vals if (v >= 50) == (vals[0] >= 50))/len(vals)*100 if vals else None
    bt = res.get("bt") or {}
    return {"score": score, "timeframe_alignment_pct": round(aligned,1) if aligned is not None else None,
            "backtest_trades": int(_num(bt.get("trades"))), "risk_level": res.get("risk_lvl"),
            "regime": (res.get("regime") or (res.get("market_regime") or {}).get("name")),
            "fake_breakout": bool(res.get("fake_breakout")),
            "label": "A" if score>=82 else "B" if score>=72 else "C" if score>=62 else "D"}


def strategy_performance(history, key="type", min_trades=3):
    groups = {}
    for item in history or []:
        if item.get("status") not in ("win", "loss"):
            continue
        raw_label = item.get(key)
        if isinstance(raw_label, dict):
            raw_label = raw_label.get("name") or "unknown"
        if not raw_label and key != "market_regime":
            raw_label = (item.get("market_regime") or {}).get("name") if isinstance(item.get("market_regime"), dict) else item.get("market_regime")
        label = str(raw_label or "unknown")[:32]
        row = groups.setdefault(label, {"name": label, "trades": 0, "wins": 0, "losses": 0, "r": 0.0})
        row["trades"] += 1
        row["wins"] += item.get("status") == "win"
        row["losses"] += item.get("status") == "loss"
        row["r"] += _num(item.get("r"), 0)
    out = []
    for row in groups.values():
        row["win_rate"] = round(row["wins"]/row["trades"]*100,1)
        row["avg_r"] = round(row["r"]/row["trades"],2)
        row["sample_ok"] = row["trades"] >= min_trades
        row["r"] = round(row["r"],2)
        out.append(row)
    return sorted(out,key=lambda x:(x["sample_ok"],x["avg_r"],x["trades"]),reverse=True)[:12]


def self_audit_summary(history, limit=30):
    closed = [x for x in (history or []) if x.get("status") in ("win", "loss")][-max(1,int(limit)):]
    if not closed:
        return {"trades":0,"findings":["Недостаточно закрытых сигналов для аудита."],"recommendation":"Продолжать сбор данных без изменения параметров."}
    wins = [x for x in closed if x.get("status")=="win"]
    losses = [x for x in closed if x.get("status")=="loss"]
    avg_win = sum(_num(x.get("r"),0) for x in wins)/len(wins) if wins else 0
    avg_loss = sum(_num(x.get("r"),0) for x in losses)/len(losses) if losses else 0
    findings = [f"Закрытых сигналов: {len(closed)}, win rate {len(wins)/len(closed)*100:.1f}%."]
    if len(closed)<15: findings.append("Выборка мала: адаптацию стратегий пока не усиливать.")
    if losses: findings.append(f"Средний R убыточных сигналов: {avg_loss:.2f}R.")
    if wins: findings.append(f"Средний R прибыльных сигналов: {avg_win:.2f}R.")
    return {"trades":len(closed),"wins":len(wins),"losses":len(losses),"win_rate":round(len(wins)/len(closed)*100,1),
            "avg_win_r":round(avg_win,2),"avg_loss_r":round(avg_loss,2),"findings":findings,
            "recommendation":"Проверять ошибки по режимам рынка; не менять пороги без достаточной выборки."}

def cleanup(now=None):
    now = time.time() if now is None else now
    for key, (ts, _) in list(_cache.items()):
        if now - ts > CACHE_TTL:
            _cache.pop(key, None)
    while len(_cache) > CACHE_MAX:
        _cache.popitem(last=False)


def scan_diagnostic_summary(bt_rejected=0, analysis_errors=0, wait_count=0,
                            quality_rejected=0, bt_unavailable=0, bt_errors=0):
    """Build a truthful no-signal summary; never conflate backtest rejects with exceptions."""
    parts = []
    if bt_rejected:
        parts.append(f"слабый бэктест: {int(bt_rejected)}")
    if bt_errors:
        parts.append(f"ошибка получения истории бэктеста: {int(bt_errors)}")
    if analysis_errors:
        parts.append(f"ошибка построения анализа: {int(analysis_errors)}")
    if wait_count:
        parts.append(f"рекомендация ЖДАТЬ: {int(wait_count)}")
    if quality_rejected:
        parts.append(f"качество ниже порога: {int(quality_rejected)}")
    if bt_unavailable:
        parts.append(f"недостаточно истории для бэктеста: {int(bt_unavailable)}")
    if not parts:
        return "Новых сигналов нет: сетапы не прошли итоговую проверку."
    return "Новых сигналов нет. Причины: " + "; ".join(parts) + "."


def is_memory_guard_reason(reason):
    """True for internal RAM-guard events that must never be broadcast to subscribers."""
    value = str(reason or "").strip().lower()
    return ("защитой памяти" in value or "ram guard" in value or
            "достигла защитного порога" in value or "memory guard" in value)

# =====================================================================
# IXCLY 5.0 QUANTUM INTELLIGENCE — bounded, explainable research models
# All functions are defensive: missing/invalid inputs return explicit
# insufficient-data states and never fabricate market observations.
# =====================================================================

def _series(df, key='close'):
    try:
        vals = np.asarray(df[key], dtype=float)
        return vals[np.isfinite(vals)]
    except Exception:
        return np.asarray([], dtype=float)


def _returns(values):
    a = np.asarray(values, dtype=float)
    if len(a) < 2:
        return np.asarray([], dtype=float)
    p = a[:-1]
    r = np.divide(a[1:] - p, p, out=np.zeros_like(p), where=np.abs(p) > 1e-12)
    return r[np.isfinite(r)]


def _qstats(values):
    a = np.asarray(values, dtype=float)
    a = a[np.isfinite(a)]
    if not len(a):
        return {'n': 0}
    return {'n': int(len(a)), 'mean': float(np.mean(a)), 'std': float(np.std(a)),
            'median': float(np.median(a)), 'p05': float(np.quantile(a, .05)),
            'p95': float(np.quantile(a, .95)), 'min': float(np.min(a)), 'max': float(np.max(a))}


def bayesian_signal_probability(evidence, history=None, prior_alpha=2.0, prior_beta=2.0):
    """Beta-Bernoulli directional posterior; history must contain closed outcomes only."""
    hist = [x for x in (history or []) if x.get('closed_at') or x.get('status') in ('closed','tp','sl')]
    wins = sum(1 for x in hist if float(x.get('r', x.get('result_r', 0)) or 0) > 0)
    losses = sum(1 for x in hist if float(x.get('r', x.get('result_r', 0)) or 0) < 0)
    neutral = max(0, len(hist) - wins - losses)
    alpha, beta = float(prior_alpha) + wins, float(prior_beta) + losses
    long_ev = max(0.0, min(1.0, float((evidence or {}).get('long', 0.5))))
    short_ev = max(0.0, min(1.0, float((evidence or {}).get('short', 0.5))))
    # Temper likelihood contribution to avoid treating correlated indicators as independent samples.
    strength = max(0.0, min(4.0, float((evidence or {}).get('independent_strength', 1.0))))
    la, lb = alpha + strength * long_ev, beta + strength * (1-long_ev)
    sa, sb = alpha + strength * short_ev, beta + strength * (1-short_ev)
    lp, sp = la/(la+lb), sa/(sa+sb)
    norm = lp + sp
    return {'status':'ok' if len(hist) >= 10 else 'prior_dominated', 'long_pct':round(100*lp/norm,2),
            'short_pct':round(100*sp/norm,2), 'posterior_long':round(lp,4), 'posterior_short':round(sp,4),
            'closed_sample':len(hist), 'neutral_sample':neutral, 'credible_interval_note':'Approximate posterior; calibration requires out-of-sample outcomes.'}


def monte_carlo_trade_simulator(df, entry, stop, targets=None, side='LONG', simulations=1200, horizon=48, seed=41):
    r = _returns(_series(df))[-500:]
    if len(r) < 30 or not entry or not stop or entry <= 0 or stop <= 0:
        return {'status':'insufficient_data','simulations':0}
    rng = np.random.default_rng(seed)
    sims = min(2500, max(200, int(simulations)))
    h = min(240, max(8, int(horizon)))
    # Circular moving-block bootstrap preserves short-range return dependence.
    block = min(5, max(2, int(round(math.sqrt(len(r)) / 2))))
    paths = np.empty((sims, h), dtype=float)
    for row in range(sims):
        sampled=[]
        while len(sampled)<h:
            start=int(rng.integers(0,len(r)))
            sampled.extend(r[(start+np.arange(block)) % len(r)].tolist())
        paths[row]=np.asarray(sampled[:h],dtype=float)
    paths = np.cumprod(1+np.clip(paths, -.25, .25), axis=1) * float(entry)
    is_long = str(side).upper() == 'LONG'
    stop_hit = paths <= stop if is_long else paths >= stop
    first_stop = np.any(stop_hit, axis=1)
    final_ret = (paths[:,-1]/entry-1) * (1 if is_long else -1)
    target_hit = np.zeros(sims, dtype=bool)
    target = (targets or [None])[0]
    tp_before_sl = None
    if target:
        target_mask = paths >= target if is_long else paths <= target
        target_hit = np.any(target_mask, axis=1)
        stop_idx=np.where(stop_hit.any(axis=1),stop_hit.argmax(axis=1),h+1)
        target_idx=np.where(target_mask.any(axis=1),target_mask.argmax(axis=1),h+1)
        # Same-bar stop/target collision is treated conservatively as stop-first.
        tp_before_sl = float(np.mean(target_idx < stop_idx))*100
    return {'status':'ok','simulations':sims,'horizon_bars':h,'median_return_pct':round(float(np.median(final_ret)*100),3),
            'p05_return_pct':round(float(np.quantile(final_ret,.05)*100),3),'p95_return_pct':round(float(np.quantile(final_ret,.95)*100),3),
            'stop_touch_pct':round(float(np.mean(first_stop)*100),2),'target_touch_pct':round(float(np.mean(target_hit)*100),2),
            'tp_before_sl_pct':round(tp_before_sl,2) if tp_before_sl is not None else None,
            'method':'historical circular block bootstrap; same-bar collision is stop-first; excludes live queue/latency'}


def regime_switching_model(df):
    r = _returns(_series(df))[-180:]
    if len(r) < 30: return {'status':'insufficient_data','regime':'unknown','confidence':0}
    def classify(window):
        short, long = window[-12:], window[-60:]
        vol_s, vol_l = float(np.std(short)), float(np.std(long)) + 1e-12
        drift = float(np.mean(short)/(np.std(short)+1e-12))
        if abs(float(np.sum(short))) > 3.5*vol_l and vol_s/vol_l > 2.2: name='panic'
        elif vol_s/vol_l > 1.7: name='high_volatility'
        elif vol_s/vol_l < .55: name='volatility_compression'
        elif abs(drift) > .7 and abs(float(np.mean(long))) > 0.15*vol_l: name='strong_trend'
        elif abs(drift) > .25: name='weak_trend'
        else: name='range'
        return name,vol_s/vol_l,drift
    current,vol_ratio,drift=classify(r)
    states=[]
    for end in range(60,len(r)+1,6):
        states.append(classify(r[max(0,end-60):end])[0])
    transitions={}
    for a,b in zip(states,states[1:]):
        transitions.setdefault(a,{}).setdefault(b,0); transitions[a][b]+=1
    probabilities={a:{b:round(n/sum(row.values()),3) for b,n in row.items()} for a,row in transitions.items()}
    return {'status':'ok','regime':current,'vol_ratio':round(vol_ratio,3),'trend_z':round(drift,3),
            'confidence':round(min(95,50+abs(drift)*20+abs(vol_ratio-1)*15),1),
            'transition_probabilities':probabilities,'transition_sample':len(states),
            'method':'rolling heuristic state model; not a fitted hidden-Markov model'}


def kalman_trend_filter(df, process_noise=1e-5, measurement_noise=1e-3):
    x=_series(df)
    if len(x)<12: return {'status':'insufficient_data'}
    level=float(x[0]); velocity=0.0; p00=p11=1.0; q=float(process_noise); noise=float(measurement_noise)
    for z in x[-300:]:
        pred=level+velocity; p00,p01,p11=p00+p11+q, p11, p11+q
        k0=p00/(p00+noise); k1=p01/(p00+noise); residual=float(z)-pred
        level=pred+k0*residual; velocity=velocity+k1*residual; p00=(1-k0)*p00; p11=p11-k1*p01
    return {'status':'ok','level':level,'velocity':velocity,'direction':'LONG' if velocity>0 else 'SHORT' if velocity<0 else 'FLAT',
            'distance_pct':round((float(x[-1])/level-1)*100,4) if level else None}


def hurst_exponent(df, max_lag=20):
    x=_series(df)
    if len(x)<80: return {'status':'insufficient_data','hurst':None}
    lags=np.arange(2,min(max_lag,len(x)//4))
    tau=[]
    for lag in lags:
        d=x[lag:]-x[:-lag]; sd=float(np.std(d));
        if sd>0: tau.append((math.log(int(lag)),math.log(sd)))
    if len(tau)<5: return {'status':'insufficient_data','hurst':None}
    slope=float(np.polyfit([z[0] for z in tau],[z[1] for z in tau],1)[0])
    return {'status':'ok','hurst':round(max(0,min(1,slope)),3),'interpretation':'persistent' if slope>.55 else 'mean_reverting' if slope<.45 else 'mixed',
            'warning':'No standalone entry signal.'}


def entropy_detector(df, bins=12, window=100):
    r=_returns(_series(df))[-window:]
    if len(r)<30: return {'status':'insufficient_data','entropy':None}
    hist,_=np.histogram(r,bins=min(20,max(5,int(bins))),density=False); p=hist[hist>0]/len(r)
    h=float(-np.sum(p*np.log(p))/math.log(len(hist))) if len(hist)>1 else 0
    return {'status':'ok','entropy':round(h,4),'chaos':'high' if h>.82 else 'medium' if h>.62 else 'low', 'wait_bias':bool(h>.88)}


def multi_horizon_forecast(tf_data):
    out={}
    for tf, label in [('15m','15m'),('1h','1h'),('4h','4h'),('1d','24h')]:
        df=(tf_data or {}).get(tf,{}).get('df'); r=_returns(_series(df))[-80:] if df is not None else np.array([])
        if len(r)<20: out[label]={'status':'insufficient_data'}; continue
        mu=float(np.mean(r[-20:])); sd=float(np.std(r[-40:]))
        out[label]={'status':'ok','direction':'LONG' if mu>sd*.08 else 'SHORT' if mu < -sd*.08 else 'NEUTRAL',
                    'drift_pct':round(mu*100,4),'vol_pct':round(sd*100,4),'confidence':round(min(90,50+abs(mu)/(sd+1e-12)*12),1)}
    return {'status':'ok' if any(v.get('status')=='ok' for v in out.values()) else 'insufficient_data','horizons':out}


def distribution_shift_detector(df, baseline=100, recent=20):
    r=_returns(_series(df))
    if len(r)<baseline+recent: return {'status':'insufficient_data','shift':None}
    a,b=r[-(baseline+recent):-recent],r[-recent:]
    scale=float(np.std(a))+1e-12; mean_shift=abs(float(np.mean(b)-np.mean(a)))/scale
    vol_shift=float(np.std(b))/(float(np.std(a))+1e-12)
    score=min(100,mean_shift*25+abs(math.log(max(vol_shift,1e-6)))*25)
    return {'status':'ok','score':round(score,1),'shift':score>=55,'mean_shift_sigma':round(mean_shift,3),'vol_ratio':round(vol_shift,3)}


def extreme_value_risk_model(df, tail_q=.95):
    r=np.abs(_returns(_series(df)))
    if len(r)<40: return {'status':'insufficient_data'}
    q=float(np.quantile(r,tail_q)); tail=r[r>=q]; excess=tail-q
    mean_ex=float(np.mean(excess)) if len(excess) else 0.0; var_ex=float(np.var(excess)) if len(excess)>1 else 0.0
    cv2=var_ex/(mean_ex*mean_ex+1e-18); xi=max(-.25,min(.45,.5*(1-1/max(cv2,1e-9)))) if mean_ex>0 else 0.0
    beta=max(1e-12,mean_ex*(1-xi))
    return {'status':'ok','threshold_pct':round(q*100,4),'tail_mean_pct':round(float(np.mean(tail))*100,4),
            'tail_observations':int(len(tail)),'max_move_pct':round(float(np.max(r))*100,4),
            'gpd_shape_xi':round(xi,4),'gpd_scale':round(beta,8),'method':'empirical threshold + method-of-moments GPD tail approximation; small samples are unstable'}


def mean_reversion_probability(df):
    x=_series(df)
    if len(x)<40 or x[-1]<=0: return {'status':'insufficient_data'}
    mean=float(np.mean(x[-40:])); sd=float(np.std(x[-40:]))+1e-12; z=(float(x[-1])-mean)/sd
    p=1/(1+math.exp(-min(8,max(-8,abs(z)-1)))) if abs(z)>1 else .5
    return {'status':'ok','zscore':round(z,3),'reversion_probability_pct':round(p*100,1),'direction':'down_to_mean' if z>1 else 'up_to_mean' if z< -1 else 'no_extreme',
            'warning':'Heuristic probability proxy; requires calibration.'}


def signed_cvd_from_agg_trades(rows):
    """Calculate rolling taker-signed quote-volume delta from Binance aggTrades rows."""
    buy_quote = sell_quote = 0.0
    buy_count = sell_count = 0
    first_ts = last_ts = None
    for trade in (rows or []):
        try:
            price = float(trade.get('p', 0) or 0); qty = float(trade.get('q', 0) or 0)
            if not np.isfinite(price) or not np.isfinite(qty) or price <= 0 or qty <= 0: continue
            quote = price * qty
            if bool(trade.get('m')):
                sell_quote += quote; sell_count += 1
            else:
                buy_quote += quote; buy_count += 1
            ts = int(trade.get('T', 0) or 0)
            if ts:
                first_ts = ts if first_ts is None else min(first_ts, ts)
                last_ts = ts if last_ts is None else max(last_ts, ts)
        except Exception:
            continue
    total = buy_quote + sell_quote
    if total <= 0:
        return {'status':'insufficient_data','source':'signed aggTrades'}
    delta = buy_quote - sell_quote
    return {'status':'ok','sample_trades':buy_count+sell_count,
        'buy_aggressor_quote':round(buy_quote,2),'sell_aggressor_quote':round(sell_quote,2),
        'delta_quote':round(delta,2),'delta_pct':round(delta/total*100,3),
        'buy_trades':buy_count,'sell_trades':sell_count,
        'from_ts':first_ts/1000 if first_ts else None,'to_ts':last_ts/1000 if last_ts else None,
        'method':'signed quote volume over sampled aggregate trades; rolling sample, not exchange-provided cumulative CVD'}


def volume_price_divergence_matrix(df, oi_change=None, cvd=None, liquidity_change=None):
    c=_series(df,'close'); v=_series(df,'volume')
    if len(c)<25 or len(v)<25: return {'status':'insufficient_data'}
    price=float(c[-1]/c[-11]-1); volume=float(np.mean(v[-5:])/(np.mean(v[-25:-5])+1e-12))
    return {'status':'ok','price_change_pct':round(price*100,3),'volume_ratio':round(volume,3),
            'price_volume_divergence':bool(price>0 and volume<.8 or price<0 and volume<.8),
            'oi_change_pct':oi_change,'cvd':cvd if cvd is not None else None,'liquidity_change':liquidity_change,
            'data_availability':{'oi':oi_change is not None,'cvd':cvd is not None,'liquidity':liquidity_change is not None}}


def multi_asset_lead_lag_engine(asset_returns, benchmark='BTC', max_lag=4):
    base=np.asarray((asset_returns or {}).get(benchmark,[]),dtype=float); out=[]
    if len(base)<30: return {'status':'insufficient_data','leaders':[]}
    for asset, values in (asset_returns or {}).items():
        if asset==benchmark: continue
        a=np.asarray(values,dtype=float); n=min(len(a),len(base)); a,b=a[-n:],base[-n:]
        if n<30: continue
        best=(0,None)
        for lag in range(-min(max_lag,4),min(max_lag,4)+1):
            aa=a[max(0,lag):n+min(0,lag)]; bb=b[max(0,-lag):n-min(0,-lag)]
            if len(aa)>10 and np.std(aa)>0 and np.std(bb)>0:
                corr=float(np.corrcoef(aa,bb)[0,1])
                if abs(corr)>abs(best[0]): best=(corr,lag)
        out.append({'asset':asset,'correlation':round(best[0],3),'lag_bars':best[1]})
    return {'status':'ok','benchmark':benchmark,'leaders':sorted(out,key=lambda x:abs(x['correlation']),reverse=True)[:20]}


def volatility_forecast(df, lam=.94):
    r=_returns(_series(df))[-300:]
    if len(r)<30: return {'status':'insufficient_data'}
    var=float(np.var(r[:min(30,len(r))]))
    for x in r: var=lam*var+(1-lam)*float(x*x)
    return {'status':'ok','ewma_vol_pct':round(math.sqrt(max(var,0))*100,4),'variance':var,'method':'EWMA; GARCH parameters not fitted without a validated calibration sample'}


def garch_volatility_forecast(df):
    """Small grid-fit GARCH(1,1); constrained alpha+beta<0.99 for stability."""
    r=_returns(_series(df))[-300:]
    if len(r)<60: return {'status':'insufficient_data'}
    candidates=[]
    sample_var=float(np.var(r))+1e-12
    for alpha in (.04,.07,.10,.13,.16):
        for beta in (.72,.78,.84,.90,.94):
            if alpha+beta>=.99: continue
            omega=sample_var*(1-alpha-beta); var=sample_var; ll=0.0
            for value in r:
                var=max(omega+alpha*value*value+beta*var,1e-14)
                ll += math.log(var)+(value*value/var)
            candidates.append((ll,alpha,beta,omega,var))
    if not candidates: return {'status':'insufficient_data'}
    ll,a,b,o,v=min(candidates,key=lambda x:x[0])
    return {'status':'ok','garch_vol_pct':round(math.sqrt(v)*100,4),'alpha':a,'beta':b,'omega':o,
            'persistence':round(a+b,3),'fit_observations':len(r),'method':'bounded grid-fit Gaussian GARCH(1,1)'}


def fractal_market_analysis(tf_data):
    rows=[]
    for tf,item in (tf_data or {}).items():
        df=item.get('df'); r=_returns(_series(df)) if df is not None else np.array([])
        if len(r)>=20: rows.append({'timeframe':tf,'trend':float(np.mean(r[-20:])),'vol':float(np.std(r[-40:])) if len(r)>=40 else float(np.std(r))})
    if len(rows)<2: return {'status':'insufficient_data','timeframes':rows}
    signs=[np.sign(x['trend']) for x in rows]
    return {'status':'ok','timeframes':rows,'alignment_pct':round(sum(1 for s in signs if s==signs[0])/len(signs)*100,1),
            'fractal_consistency':'aligned' if all(s==signs[0] for s in signs) else 'mixed'}


def sequential_evidence_accumulator(factors, prior_log_odds=0.0, threshold=2.2):
    # Each factor is a bounded signed log-likelihood contribution; caller must avoid duplicate/correlated inputs.
    score=float(prior_log_odds); used=[]
    for f in (factors or [])[:20]:
        try: v=max(-1.5,min(1.5,float(f.get('log_lr',0))))
        except Exception: continue
        score+=v; used.append(str(f.get('name','factor')))
    prob=1/(1+math.exp(-max(-12,min(12,score))))
    return {'status':'ok' if used else 'insufficient_data','log_odds':round(score,3),'long_probability_pct':round(prob*100,1),
            'decision':'LONG' if score>=threshold else 'SHORT' if score<=-threshold else 'WAIT','evidence_count':len(used),'evidence':used}


def limit_order_quality(spread_pct=None, depth_ratio=None, distance_atr=None):
    if spread_pct is None or depth_ratio is None: return {'status':'insufficient_data','score':None}
    score=100-min(45,float(spread_pct)*180)-min(35,max(0,1-float(depth_ratio))*35)-min(20,max(0,float(distance_atr or 0))*8)
    return {'status':'ok','score':round(max(0,score),1),'fillable':'high' if score>=75 else 'medium' if score>=50 else 'low'}


def adverse_selection_detector(price_before, price_after, side, horizon=1):
    if not price_before or not price_after or price_before<=0: return {'status':'insufficient_data'}
    move=(float(price_after)/float(price_before)-1)*100
    adverse=move<0 if str(side).upper()=='LONG' else move>0
    return {'status':'ok','adverse':adverse,'post_fill_move_pct':round(move,4),'sample_note':'Requires stored post-fill observations for a reliable rate.'}


def entry_zone_optimizer(price, atr, side, support=None, resistance=None):
    if not price or not atr or price<=0 or atr<=0: return {'status':'insufficient_data'}
    pad=.18*atr
    if str(side).upper()=='LONG': lo=max(price-0.65*atr,support or 0); hi=price+0.10*atr
    else: lo=price-0.10*atr; hi=min(price+0.65*atr,resistance or float('inf'))
    return {'status':'ok','zone_low':round(lo,10),'zone_high':round(hi,10),'reference_price':price,'atr_buffer':round(pad,10)}


def post_entry_validation(entry, current, stop, side, max_adverse_pct=.8):
    if not entry or not current or not stop: return {'status':'insufficient_data'}
    adverse=(entry-current)/entry*100 if str(side).upper()=='LONG' else (current-entry)/entry*100
    invalid=(current<=stop if str(side).upper()=='LONG' else current>=stop)
    return {'status':'invalidated' if invalid else 'adverse' if adverse>max_adverse_pct else 'valid','adverse_move_pct':round(adverse,3)}


def order_book_pressure(bid_share):
    if bid_share is None: return {'status':'insufficient_data'}
    x=max(0,min(1,float(bid_share)))
    return {'status':'ok','bid_share_pct':round(x*100,1),'pressure':'buy' if x>.56 else 'sell' if x<.44 else 'balanced'}


def spread_intelligence(bid, ask, mid=None):
    if not bid or not ask or bid<=0 or ask<bid: return {'status':'insufficient_data'}
    m=float(mid or (bid+ask)/2); return {'status':'ok','spread_pct':round((ask-bid)/m*100,5),'mid':m}


def slippage_forecast(spread_pct=None, volatility_pct=None, notional=None, depth_notional=None):
    if spread_pct is None or volatility_pct is None: return {'status':'insufficient_data'}
    impact=0 if not notional or not depth_notional else min(2,float(notional)/(float(depth_notional)+1e-12)*.1)
    return {'status':'ok','estimated_slippage_pct':round(float(spread_pct)/2+float(volatility_pct)*.08+impact,5),'assumption':'proxy; calibrate against actual fills'}


def partial_fill_controller(requested, filled, min_ratio=.95):
    if requested is None or filled is None or requested<=0: return {'status':'insufficient_data'}
    ratio=max(0,min(1,float(filled)/float(requested)))
    return {'status':'filled' if ratio>=min_ratio else 'partial' if ratio>0 else 'unfilled','fill_ratio_pct':round(ratio*100,2),'remaining':max(0,float(requested)-float(filled))}


def limit_order_expiration(created_ts, now=None, ttl_seconds=300, market_valid=True):
    if not created_ts: return {'status':'insufficient_data','age_seconds':None,'ttl_seconds':ttl_seconds}
    age=max(0,(float(now or time.time())-float(created_ts)))
    return {'status':'expired' if age>ttl_seconds or not market_valid else 'active','age_seconds':round(age,1),'ttl_seconds':ttl_seconds}


def chasing_protection(price, planned_entry, atr, side, max_atr=.75):
    if not price or not planned_entry or not atr or atr<=0: return {'status':'insufficient_data','allow':False}
    dist=(price-planned_entry)/atr*(1 if str(side).upper()=='LONG' else -1)
    return {'status':'ok','allow':dist<=max_atr,'distance_atr':round(dist,3),'reason':'price_chased' if dist>max_atr else 'within_entry_band'}


def portfolio_var_es(returns_matrix, confidence=.95):
    a=np.asarray(returns_matrix,dtype=float)
    if a.ndim!=2 or a.shape[0]<30 or a.shape[1]<1: return {'status':'insufficient_data'}
    port=np.mean(a,axis=1); loss=-port; var=float(np.quantile(loss,confidence)); tail=loss[loss>=var]
    return {'status':'ok','historical_var_pct':round(var*100,4),'expected_shortfall_pct':round(float(np.mean(tail))*100,4),'confidence':confidence,'observations':len(port)}


def correlation_clusters(returns_by_asset, threshold=.75):
    names=[k for k,v in (returns_by_asset or {}).items() if len(v)>=30]
    if len(names)<2: return {'status':'insufficient_data','clusters':[[x] for x in names]}
    n=min(len(returns_by_asset[x]) for x in names); mat=np.array([returns_by_asset[x][-n:] for x in names],dtype=float); corr=np.corrcoef(mat)
    groups=[]; used=set()
    for i,name in enumerate(names):
        if name in used: continue
        group=[name]; used.add(name)
        for j,other in enumerate(names):
            if other not in used and abs(float(corr[i,j]))>=threshold: group.append(other); used.add(other)
        groups.append(group)
    return {'status':'ok','clusters':groups,'threshold':threshold}


def drawdown_guard(equity_curve, max_dd_pct=12):
    a=np.asarray(equity_curve,dtype=float)
    if len(a)<2 or np.any(a<=0): return {'status':'insufficient_data','locked':False}
    peaks=np.maximum.accumulate(a); dd=(peaks-a)/peaks*100; worst=float(np.max(dd))
    return {'status':'ok','max_drawdown_pct':round(worst,3),'locked':worst>=max_dd_pct,'limit_pct':max_dd_pct}


def daily_loss_guard(closed_pnl, equity, limit_pct=3):
    if equity is None or equity<=0: return {'status':'insufficient_data','locked':False}
    loss=max(0,-sum(float(x) for x in (closed_pnl or [])))
    pct=loss/float(equity)*100
    return {'status':'ok','daily_loss_pct':round(pct,3),'locked':pct>=limit_pct,'limit_pct':limit_pct}


def leverage_risk_engine(leverage, stop_distance_pct, maintenance_margin_pct=None):
    if not leverage or not stop_distance_pct or leverage<=0 or stop_distance_pct<=0: return {'status':'insufficient_data'}
    loss_pct=float(leverage)*float(stop_distance_pct)
    liq_buffer=None if maintenance_margin_pct is None else max(0,100/float(leverage)-float(maintenance_margin_pct))
    return {'status':'ok','equity_loss_at_stop_pct':round(loss_pct,3),'estimated_liquidation_buffer_pct':round(liq_buffer,3) if liq_buffer is not None else None,
            'warning':loss_pct>=25 or liq_buffer is not None and stop_distance_pct>=liq_buffer}


def liquidation_distance(entry, mark, side, leverage, maintenance_margin_rate=None):
    if not all([entry,mark,leverage]) or min(entry,mark,leverage)<=0 or maintenance_margin_rate is None:
        return {'status':'insufficient_data','distance_pct':None}
    # Simplified isolated-margin approximation only; exchange tiers/fees/funding are not included.
    mm=float(maintenance_margin_rate); lev=float(leverage)
    liq=entry*(1-1/lev+mm) if str(side).upper()=='LONG' else entry*(1+1/lev-mm)
    distance=abs(mark-liq)/mark*100
    return {'status':'estimated','liquidation_price':round(liq,10),'distance_pct':round(distance,3),'warning':distance<5,'method':'simplified; not an exchange-confirmed liquidation price'}


def risk_budget_allocator(equity, candidates, total_risk_pct=3):
    valid=[x for x in (candidates or []) if float(x.get('quality',0) or 0)>0]
    if not valid or not equity or equity<=0: return {'status':'insufficient_data','allocations':[]}
    weights=np.array([max(0,float(x.get('quality',0)))/(1+max(0,float(x.get('correlation_penalty',0)))) for x in valid]); weights=weights/(weights.sum() or 1)
    return {'status':'ok','allocations':[{'coin':x.get('coin'),'risk_pct':round(float(total_risk_pct*w),3),'risk_usd':round(float(equity)*float(total_risk_pct*w)/100,2)} for x,w in zip(valid,weights)]}


def emergency_risk_lock(data_quality=None, drawdown=None, daily_loss=None, volatility_shock=None, exchange_healthy=True):
    reasons=[]
    if data_quality and data_quality.get('status') in ('poor','invalid'): reasons.append('data_integrity')
    if drawdown and drawdown.get('locked'): reasons.append('drawdown_limit')
    if daily_loss and daily_loss.get('locked'): reasons.append('daily_loss_limit')
    if volatility_shock and volatility_shock.get('shock'): reasons.append('volatility_shock')
    if not exchange_healthy: reasons.append('exchange_unavailable')
    return {'locked':bool(reasons),'status':'RED' if reasons else 'GREEN','reasons':reasons}


def loss_autopsy(trade):
    if not trade or float(trade.get('r',trade.get('result_r',0)) or 0)>=0: return {'status':'not_a_loss'}
    return {'status':'ok','loss_r':float(trade.get('r',trade.get('result_r',0)) or 0),'regime':trade.get('regime'),
            'side':trade.get('side'),'entry_reason':trade.get('reason'),'diagnostic_flags':[k for k in ('late_entry','low_volume','regime_shift','news_conflict','high_spread','stale_data') if trade.get(k)]}


def missed_opportunity_analysis(observations):
    rows=[x for x in (observations or []) if x.get('was_valid') and not x.get('taken')]
    return {'status':'ok','missed_count':len(rows),'examples':rows[-10:],'note':'Requires stored counterfactual observations; absence is not interpreted as zero misses.'}


def strategy_attribution(trades, factors=None):
    closed=[x for x in (trades or []) if x.get('closed_at') or x.get('status')=='closed']
    out={}
    for f in (factors or ['regime','side','entry_quality','volume_state']):
        groups={}
        for t in closed:
            key=str(t.get(f,'unknown')); groups.setdefault(key,[]).append(float(t.get('r',t.get('result_r',0)) or 0))
        out[f]={k:{'n':len(v),'avg_r':round(float(np.mean(v)),3)} for k,v in groups.items() if len(v)>=3}
    return {'status':'ok' if closed else 'insufficient_data','trades':len(closed),'attribution':out}


def out_of_sample_testing(history, split=.7, min_test=10):
    h=[x for x in (history or []) if x.get('closed_at') or x.get('status')=='closed']
    h.sort(key=lambda x:float(x.get('closed_at',x.get('ts',0)) or 0)); cut=int(len(h)*split); train,test=h[:cut],h[cut:]
    def stats(rows):
        r=[float(x.get('r',x.get('result_r',0)) or 0) for x in rows]
        return {'n':len(r),'expectancy_r':round(float(np.mean(r)),3) if r else None,'win_rate_pct':round(sum(v>0 for v in r)/len(r)*100,1) if r else None}
    return {'status':'ok' if len(test)>=min_test else 'insufficient_test_sample','train':stats(train),'test':stats(test),'chronological':True}


def parameter_stability_test(results, min_trials=5):
    """Legacy score-dispersion diagnostic; not a parameter perturbation backtest."""
    vals=[float(x) for x in (results or []) if x is not None and np.isfinite(float(x))]
    if len(vals)<min_trials: return {'status':'insufficient_data','note':'Need more closed observations.'}
    med=float(np.median(vals)); spread=float(np.std(vals)); return {'status':'ok','median':round(med,3),'std':round(spread,3),'stable':spread<=max(.25,abs(med)*.5),'trials':len(vals),'method':'score_dispersion_proxy'}


def parameter_sensitivity_from_history(history, thresholds=(45,50,55,60,65,70,75,80), min_sample=10):
    """Tests entry-score threshold sensitivity on closed outcomes; no parameter fitting on future rows."""
    rows=[]
    for x in (history or []):
        if not x.get('closed_at') or x.get('strategy_score') is None or x.get('r') is None:
            continue
        try:
            score=float(x['strategy_score']); result=float(x['r'])
            if np.isfinite(score) and np.isfinite(result): rows.append((score,result,float(x.get('closed_at',0) or 0)))
        except Exception: continue
    rows.sort(key=lambda z:z[2])
    if len(rows)<min_sample:
        return {'status':'insufficient_data','observations':len(rows),'method':'historical entry-score threshold sensitivity'}
    variants=[]
    for threshold in thresholds:
        sample=[r for score,r,_ in rows if score>=float(threshold)]
        variants.append({'threshold':float(threshold),'trades':len(sample),
            'expectancy_r':round(float(np.mean(sample)),4) if sample else None,
            'win_rate_pct':round(float(np.mean(np.asarray(sample)>0)*100),2) if sample else None})
    usable=[x for x in variants if x['trades']>=min_sample and x['expectancy_r'] is not None]
    if len(usable)<3:
        return {'status':'insufficient_threshold_coverage','observations':len(rows),'variants':variants,'method':'historical entry-score threshold sensitivity'}
    exp=np.asarray([x['expectancy_r'] for x in usable],dtype=float)
    signs={1 if x>0 else -1 if x<0 else 0 for x in exp}
    return {'status':'ok','observations':len(rows),'variants':variants,'stable_sign':len(signs)<=1,
            'expectancy_std_r':round(float(np.std(exp)),4),'method':'closed-trade score threshold sensitivity; not a full re-optimization or causal parameter test'}


def regime_specific_performance(history):
    return strategy_attribution(history, ['regime'])


def false_signal_database(history):
    bad=[x for x in (history or []) if x.get('status')=='closed' and float(x.get('r',x.get('result_r',0)) or 0)<0]
    return {'status':'ok','count':len(bad),'recent':bad[-20:],'reason_counts':{}}


def strategy_retirement(performance, min_trades=30, min_expectancy=-.05):
    n=int((performance or {}).get('trades',0) or 0); exp=(performance or {}).get('expectancy_r')
    retire=bool(n>=min_trades and exp is not None and float(exp)<min_expectancy)
    return {'status':'retire' if retire else 'active' if n>=min_trades else 'observe','sample':n,'expectancy_r':exp,'automatic_execution':False}


def data_integrity_guard(df, timeframe=None, max_age_seconds=None, now=None):
    if df is None: return {'status':'invalid','reasons':['missing_frame'],'score':0}
    reasons=[]; score=100
    try:
        required={'open','high','low','close','volume'}; missing=required-set(df.columns)
        if missing: reasons.append('missing_columns:'+','.join(sorted(missing))); score-=60
        if not missing:
            raw={k:np.asarray(df[k],dtype=float) for k in required}
            if any(np.any(~np.isfinite(v)) for v in raw.values()): reasons.append('non_finite_ohlcv'); score-=50
            if any(np.any(v<=0) for k,v in raw.items() if k!='volume'): reasons.append('non_positive_price'); score-=50
            if np.any(raw['volume']<0): reasons.append('negative_volume'); score-=30
            if np.any(raw['high']<raw['low']): reasons.append('invalid_high_low'); score-=40
            if np.any(raw['close']>raw['high']*1.000001) or np.any(raw['close']<raw['low']*.999999): reasons.append('close_outside_range'); score-=25
        if len(df)<30: reasons.append('too_few_bars'); score-=30
        ts_key='ts' if 'ts' in df.columns else 'timestamp' if 'timestamp' in df.columns else None
        if ts_key and len(df)>2:
            ts=np.asarray(df[ts_key],dtype=float); dif=np.diff(ts)
            if np.any(dif<=0): reasons.append('non_monotonic_timestamps'); score-=35
            positive=dif[dif>0]
            if len(positive)>2 and np.any(positive>np.median(positive)*2.5): reasons.append('missing_candles'); score-=15
            if max_age_seconds and len(ts):
                last=float(ts[-1]); last=last/1000 if last>1e11 else last
                age=float(now or time.time())-last
                if age>max_age_seconds: reasons.append('stale_data'); score-=50
        elif max_age_seconds:
            reasons.append('timestamp_unavailable'); score-=10
    except Exception: reasons.append('schema_error'); score=0
    return {'status':'poor' if score<60 else 'warning' if reasons else 'good','score':max(0,score),'reasons':reasons,'timeframe':timeframe}


def cross_exchange_watch(quotes, max_spread_pct=0.5):
    vals=[(k,float(v)) for k,v in (quotes or {}).items() if v and float(v)>0]
    if len(vals)<2: return {'status':'insufficient_data','opportunity':False}
    low=min(vals,key=lambda x:x[1]); high=max(vals,key=lambda x:x[1]); spread=(high[1]/low[1]-1)*100
    return {'status':'ok','buy_exchange':low[0],'sell_exchange':high[0],'spread_pct':round(spread,4),'opportunity':spread>=max_spread_pct,
            'note':'Gross price difference only; fees, transfer latency and inventory constraints excluded.'}


def liquidation_cascade_forecast(funding=None, oi_change=None, liquidation_notional=None, average_notional=None):
    if funding is None and oi_change is None and liquidation_notional is None: return {'status':'insufficient_data'}
    score=0
    if funding is not None: score+=min(30,abs(float(funding))*10000)
    if oi_change is not None: score+=min(30,abs(float(oi_change))*2)
    if liquidation_notional is not None and average_notional: score+=min(40,float(liquidation_notional)/(float(average_notional)+1e-12)*20)
    return {'status':'ok','cascade_risk_score':round(min(100,score),1),'level':'high' if score>=65 else 'medium' if score>=35 else 'low','forecast':'risk proxy, not a liquidation map'}


def weekend_market_model(timestamp=None):
    dt=datetime.fromtimestamp(float(timestamp or time.time()),tz=timezone.utc)
    return {'status':'ok','weekend':dt.weekday()>=5,'utc_weekday':dt.weekday(),'note':'Calendar context only; learn weekend edge from closed trades.'}


def session_intelligence(timestamp=None):
    dt=datetime.fromtimestamp(float(timestamp or time.time()),tz=timezone.utc); h=dt.hour
    session='asia' if 0<=h<8 else 'europe' if 7<=h<16 else 'us' if 13<=h<22 else 'transition'
    return {'status':'ok','session':session,'utc_hour':h,'overlap':7<=h<8 or 13<=h<16}


def normalize_macro_calendar_events(raw_events, source='unknown', relevant_countries=None):
    """Normalize only high-impact events with exact, trustworthy UTC instants; never invent midnight."""
    allowed=set(relevant_countries or {'US','EU','GB','UK','JP','CN','CA','AU','CH','DE'})
    events=[]; stale_rows=0; untimed_rows=0
    for ev in (raw_events or []):
        try:
            impact=str(ev.get('impact','')).strip().lower()
            country=str(ev.get('country') or '').strip().upper()
            category=str(ev.get('category','economic-indicators')).lower()
            if impact not in ('high','3','critical') or country not in allowed: continue
            if category not in ('economic-indicators','central-banks-monetary-policy'): continue
            if bool(ev.get('data_stale')) or str(ev.get('status','')).lower() in ('cancelled','released'):
                stale_rows += int(bool(ev.get('data_stale'))); continue
            raw_time=(ev.get('release_at_utc') or ev.get('time_utc')) if source=='FinanceCalendar' else ev.get('time')
            raw_time=str(raw_time or '').strip()
            if not raw_time or ('T' not in raw_time and not __import__('re').search(r'\d{2}:\d{2}',raw_time)):
                untimed_rows += 1; continue
            dt=datetime.fromisoformat(raw_time.replace('Z','+00:00'))
            if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
            events.append({'ts':dt.timestamp(),'impact':'high','country':country,
                'event':str(ev.get('name') or ev.get('title') or ev.get('event') or 'Macro event')[:180],
                'category':category,'status':ev.get('status'),'data_stale':False,'last_verified_at':ev.get('last_verified_at'),
                'source':source,'source_url':ev.get('url') or ev.get('source_url')})
        except Exception:
            continue
    events.sort(key=lambda x:x['ts'])
    return {'events':events,'stale_rows':stale_rows,'untimed_rows':untimed_rows,'raw_rows':len(raw_events or [])}


def macro_event_shield(events, now=None, window_minutes=60, feed_status=None):
    if feed_status not in (None, 'ok'):
        return {'status':'no_event_feed','blocked':False,'events':[],'source_status':feed_status,'note':'Trusted macro calendar unavailable; absence of events is not interpreted as a safe window.'}
    if not events:
        return {'status':'ok' if feed_status == 'ok' else 'no_event_feed','blocked':False,'events':[],'note':'No high-impact event in the loaded calendar window.' if feed_status == 'ok' else 'No trusted macro calendar is connected.'}
    now=float(now or time.time()); fresh=[]
    for e in (events or []):
        try:
            ts=float(e.get('ts')); impact=str(e.get('impact','')).lower()
            if impact in ('high','critical') and abs(ts-now)<=window_minutes*60: fresh.append(e)
        except Exception: continue
    return {'status':'ok','blocked':bool(fresh),'events':fresh[:10],'note':'Only blocks when trusted timestamped event data is supplied.'}


def volatility_compression_scanner(df):
    r=_returns(_series(df))
    if len(r)<60: return {'status':'insufficient_data'}
    recent=float(np.std(r[-10:])); baseline=float(np.std(r[-60:-10]))+1e-12; ratio=recent/baseline
    return {'status':'ok','compression_ratio':round(ratio,3),'compressed':ratio<.55,'potential_expansion':'watch' if ratio<.55 else 'normal'}


def market_breadth_engine(asset_changes):
    vals=[float(v) for v in (asset_changes or {}).values() if v is not None and np.isfinite(float(v))]
    if not vals: return {'status':'insufficient_data'}
    up=sum(v>0 for v in vals); down=sum(v<0 for v in vals)
    return {'status':'ok','assets':len(vals),'advance_pct':round(up/len(vals)*100,1),'decline_pct':round(down/len(vals)*100,1),
            'breadth':'positive' if up>down else 'negative' if down>up else 'neutral'}


def trade_opportunity_ranking(candidates):
    rows=[]
    for x in (candidates or []):
        try:
            score=float(x.get('edge',0))-float(x.get('risk',0))-float(x.get('cost',0))-float(x.get('correlation_penalty',0))
            rows.append({'coin':x.get('coin'),'score':round(score,3),'eligible':bool(x.get('data_ok')) and score>0})
        except Exception: continue
    return {'status':'ok','ranking':sorted(rows,key=lambda z:z['score'],reverse=True)[:30]}


def quant_intelligence_stack(tf_data, side='LONG', entry=None, stop=None, targets=None, history=None,
                             funding=None, oi_change=None, orderbook_bid_share=None, news_risk=None, run_simulation=False, portfolio_history=None, leverage=None, spread_pct=None, depth_ratio=None, depth_notional=None, notional=None, cross_exchange_data=None, funding_history=None, liquidation_notional=None, average_liquidation_notional=None, cvd_data=None, macro_events=None, macro_feed_status=None, liquidation_distance_data=None):
    """Bounded orchestration called by the signal pipeline; no external data is fabricated."""
    frames=(tf_data or {})
    base=frames.get('4h') or {}
    df=base.get('df')
    tf_seconds={'15m':900,'1h':3600,'4h':14400,'1d':86400}
    quality_by_tf={tf:(data_integrity_guard((frames.get(tf) or {}).get('df'),tf,max_age_seconds=tf_seconds[tf]*3) if (frames.get(tf) or {}).get('df') is not None else {'status':'invalid','score':0,'reasons':['missing_frame']}) for tf in ('15m','1h','4h','1d')}
    quality=quality_by_tf.get('4h',{'status':'invalid','score':0,'reasons':['missing_4h']})
    missing_tfs=[tf for tf,v in quality_by_tf.items() if v.get('status') in ('poor','invalid')]
    regime=regime_switching_model(df) if df is not None else {'status':'insufficient_data','regime':'unknown'}
    kalman=kalman_trend_filter(df) if df is not None else {'status':'insufficient_data'}
    hurst=hurst_exponent(df) if df is not None else {'status':'insufficient_data'}
    entropy=entropy_detector(df) if df is not None else {'status':'insufficient_data'}
    vol=volatility_forecast(df) if df is not None else {'status':'insufficient_data'}
    garch=garch_volatility_forecast(df) if df is not None else {'status':'insufficient_data'}
    shift=distribution_shift_detector(df) if df is not None else {'status':'insufficient_data'}
    tail=extreme_value_risk_model(df) if df is not None else {'status':'insufficient_data'}
    reversion=mean_reversion_probability(df) if df is not None else {'status':'insufficient_data'}
    _cvd_pct=(cvd_data or {}).get('delta_pct') if isinstance(cvd_data,dict) and (cvd_data or {}).get('status')=='ok' else None
    divergence=volume_price_divergence_matrix(df, oi_change=oi_change, cvd=_cvd_pct) if df is not None else {'status':'insufficient_data'}
    fractal=fractal_market_analysis(tf_data)
    horizons=multi_horizon_forecast(tf_data)
    compression=volatility_compression_scanner(df) if df is not None else {'status':'insufficient_data'}
    trap=liquidity_trap_detector(df) if df is not None else {'status':'insufficient_data'}
    breakout_failure=breakout_failure_probability(df) if df is not None else {'status':'insufficient_data'}
    funding_state=funding_convergence(funding_history)
    smart_money=smart_money_footprint(df,funding,oi_change,orderbook_bid_share,cvd=_cvd_pct) if df is not None else {'status':'insufficient_data'}
    _liq_ratio=(float(liquidation_notional)/(float(average_liquidation_notional)+1e-12)) if liquidation_notional is not None and average_liquidation_notional else None
    shock=volatility_shock_detector(df,oi_change,_liq_ratio) if df is not None else {'status':'insufficient_data','shock':False}
    manipulation=market_manipulation_watch(df) if df is not None else {'status':'insufficient_data'}
    scenario_engine=scenario_simulation_engine(float(base.get('price') or entry or 0),float(base.get('atr') or 0),side) if base.get('atr') else {'status':'insufficient_data','scenarios':[]}
    book=order_book_pressure(orderbook_bid_share)
    evidence=[]
    if kalman.get('status')=='ok': evidence.append({'name':'kalman','log_lr':.45 if (kalman['direction']=='LONG')==(side=='LONG') else -.45})
    if horizons.get('horizons',{}).get('4h',{}).get('status')=='ok': evidence.append({'name':'4h_forecast','log_lr':.35 if horizons['horizons']['4h']['direction']==side else -.35})
    if entropy.get('wait_bias'): evidence.append({'name':'entropy','log_lr':-.8})
    if regime.get('regime')=='panic': evidence.append({'name':'panic','log_lr':-.9})
    sequential=sequential_evidence_accumulator(evidence)
    prob=bayesian_signal_probability({'long':.65 if sequential.get('decision')=='LONG' else .35,'short':.65 if sequential.get('decision')=='SHORT' else .35,'independent_strength':min(1.5,len(evidence)*0.35)},history)
    mc=monte_carlo_trade_simulator(df,entry,stop,targets,side,simulations=600,horizon=36) if run_simulation and entry and stop and df is not None else {'status':'deferred_for_resource_budget'}
    entry_guard=chasing_protection(float(base.get('price') or entry or 0),entry,float(base.get('atr') or 0),side) if entry and base.get('atr') else {'status':'insufficient_data','allow':False}
    # Execution quality: only fields backed by supplied exchange data are scored.
    entry_zone=entry_zone_optimizer(float(base.get('price') or entry or 0),float(base.get('atr') or 0),side,
                                    (base.get('sr') or {}).get('support'),(base.get('sr') or {}).get('resistance')) if base.get('atr') else {'status':'insufficient_data'}
    stop_distance_pct=(abs(float(entry)-float(stop))/float(entry)*100) if entry and stop and float(entry)>0 else None
    leverage_risk=leverage_risk_engine(leverage,stop_distance_pct) if leverage and stop_distance_pct else {'status':'insufficient_data','reason':'leverage/stop inputs unavailable'}
    liq_distance=liquidation_distance_data if isinstance(liquidation_distance_data,dict) else {'status':'insufficient_data','reason':'exchange-reported liquidation data unavailable'}
    chase=entry_guard
    spread=spread_intelligence((float(base.get('price') or 0)-float(base.get('price') or 0)*float(spread_pct or 0)/200) if spread_pct is not None else None,
                               (float(base.get('price') or 0)+float(base.get('price') or 0)*float(spread_pct or 0)/200) if spread_pct is not None else None)
    limit_quality=limit_order_quality(spread_pct,depth_ratio)
    vol_pct=vol.get('ewma_vol_pct') if vol.get('status')=='ok' else None
    slip=slippage_forecast(spread_pct,vol_pct,notional,depth_notional)
    fill_state=partial_fill_controller(None,None)
    order_expiry=limit_order_expiration(None,market_valid=False)
    portfolio_history = portfolio_history or []
    portfolio_returns=[float(x.get('portfolio_return_pct'))/100 for x in portfolio_history if x.get('portfolio_return_pct') is not None]
    portfolio_var=portfolio_var_es(np.asarray(portfolio_returns[-120:]).reshape(-1,1)) if len(portfolio_returns)>=30 else {'status':'insufficient_data','reason':'portfolio return series unavailable'}
    equity_curve=[float(x.get('equity_after')) for x in portfolio_history if x.get('equity_after') is not None]
    dd=drawdown_guard(equity_curve) if len(equity_curve)>=3 else {'status':'insufficient_data','locked':False,'reason':'equity curve unavailable'}
    today_cut=time.time()-86400
    today_losses=[float(x.get('usd',0) or 0) for x in portfolio_history if float(x.get('closed_at',x.get('ts',0)) or 0)>=today_cut]
    _equity=next((float(x.get('equity_before')) for x in reversed(portfolio_history) if x.get('equity_before')),None)
    daily_guard=daily_loss_guard(today_losses,_equity,3.0) if today_losses and _equity else {'status':'insufficient_data','locked':False,'reason':'equity basis unavailable'}
    risk_lock=emergency_risk_lock(quality,dd,daily_guard,shock,True)
    oos=out_of_sample_testing(history or [])
    attribution=strategy_attribution(history or [])
    false_signals=false_signal_database(history or [])
    strategy_status=strategy_retirement({'trades':oos.get('train',{}).get('n',0),'expectancy_r':oos.get('train',{}).get('expectancy_r')})
    session=session_intelligence(); weekend=weekend_market_model()
    cascade=liquidation_cascade_forecast(funding,oi_change,liquidation_notional,average_liquidation_notional)
    breadth=market_breadth_engine({})
    macro=macro_event_shield(macro_events or [], window_minutes=30, feed_status=macro_feed_status)
    cross_exchange=cross_exchange_data if isinstance(cross_exchange_data,dict) else {'status':'insufficient_data'}
    risk_flags=[]
    if quality.get('status')!='good' or missing_tfs: risk_flags.append('data_quality:'+','.join(missing_tfs or ['4h_warning']))
    if entropy.get('wait_bias'): risk_flags.append('high_entropy')
    if shift.get('shift'): risk_flags.append('distribution_shift')
    if regime.get('regime')=='panic': risk_flags.append('panic_regime')
    if shock.get('shock'): risk_flags.append('volatility_shock')
    if trap.get('trap_risk')=='high': risk_flags.append('liquidity_trap')
    if breakout_failure.get('failure_probability_pct',0)>=75: risk_flags.append('breakout_failure_risk')
    if manipulation.get('suspicious_activity'): risk_flags.append('suspicious_market_activity')
    if entry_guard.get('status')=='ok' and not entry_guard.get('allow'): risk_flags.append('chasing')
    if news_risk and news_risk.get('severity')=='critical': risk_flags.append('critical_news')
    if macro.get('blocked'): risk_flags.append('high_impact_macro_event')
    elif macro.get('status') != 'ok': risk_flags.append('macro_calendar_unavailable')
    if cross_exchange.get('status')=='ok' and float(cross_exchange.get('spread_pct',0) or 0)>=1.0: risk_flags.append('cross_exchange_dislocation')
    if risk_lock.get('locked'):
        risk_flags.extend([x for x in risk_lock.get('reasons',[]) if x not in risk_flags])
    score=max(0,min(100,100-18*len(risk_flags)-max(0,tail.get('threshold_pct',0)-2)*2))
    gate='RED' if risk_lock.get('locked') or quality.get('status') in ('poor','invalid') or '4h' in missing_tfs or len(risk_flags)>=3 else 'YELLOW' if risk_flags else 'GREEN'
    model_confidence=round(min(95,30+min(100,int(prob.get('closed_sample',0)))*.35+len(evidence)*7),1)
    entry_quality=limit_quality.get('score') if limit_quality.get('status')=='ok' else None
    reversal_risk=round(max(float(breakout_failure.get('failure_probability_pct',0) or 0),
                            float(reversion.get('reversion_probability_pct',0) or 0) if reversion.get('direction')!='no_extreme' else 0),1)
    return {'status':'ok','data_quality':quality,'data_quality_by_tf':quality_by_tf,'regime_switching':regime,'kalman':kalman,'hurst':hurst,'entropy':entropy,
            'volatility_forecast':vol,'garch_volatility':garch,'distribution_shift':shift,'extreme_value_risk':tail,'mean_reversion':reversion,
            'volume_price_matrix':divergence,'fractal':fractal,'multi_horizon':horizons,'compression':compression,
            'liquidity_trap':trap,'breakout_failure':breakout_failure,'funding_convergence':funding_state,
            'smart_money_footprint':smart_money,'cvd':cvd_data if isinstance(cvd_data,dict) else {'status':'unavailable'},'volatility_shock':shock,
            'market_manipulation_watch':manipulation,'scenario_engine':scenario_engine,
            'order_book_pressure':book,'sequential_evidence':sequential,'bayesian_probability':prob,'monte_carlo':mc,
            'entry_guard':entry_guard,'entry_zone':entry_zone,'limit_order_quality':limit_quality,
            'leverage_risk':leverage_risk,'liquidation_distance':liq_distance,
            'adverse_selection':{'status':'awaiting_fill_history'},'post_entry_validation':{'status':'not_in_trade'},
            'spread_intelligence':spread,'slippage_forecast':slip,'partial_fill':fill_state,'limit_order_expiration':order_expiry,
            'portfolio_var_es':portfolio_var,'drawdown_guard':dd,'daily_loss_guard':daily_guard,'emergency_risk_lock':risk_lock,
            'out_of_sample':oos,'strategy_attribution':attribution,'false_signal_database':false_signals,
            'strategy_retirement':strategy_status,'liquidation_cascade':cascade,'market_breadth':breadth,
            'macro_event_shield':macro,'cross_exchange':cross_exchange,'session':session,'weekend':weekend,
            'model_confidence':model_confidence,'entry_quality_score':entry_quality,'reversal_risk_pct':reversal_risk,
            'risk_score':score,'risk_state':gate,'risk_flags':risk_flags}


def liquidity_trap_detector(df, lookback=30):
    """Detects sweep-and-reject patterns from OHLCV only; liquidation levels are not inferred."""
    try:
        high=_series(df,'high'); low=_series(df,'low'); close=_series(df,'close'); vol=_series(df,'volume')
        if min(len(high),len(low),len(close),len(vol))<lookback+3: return {'status':'insufficient_data'}
        prior_high=float(np.max(high[-lookback-2:-2])); prior_low=float(np.min(low[-lookback-2:-2]))
        swept_high=high[-1]>prior_high and close[-1]<prior_high
        swept_low=low[-1]<prior_low and close[-1]>prior_low
        vr=float(vol[-1]/(np.mean(vol[-lookback-1:-1])+1e-12))
        return {'status':'ok','sweep_high':bool(swept_high),'sweep_low':bool(swept_low),'volume_ratio':round(vr,2),
                'trap_risk':'high' if (swept_high or swept_low) and vr>=1.5 else 'watch' if swept_high or swept_low else 'low',
                'note':'OHLCV proxy; does not observe actual liquidation clusters.'}
    except Exception: return {'status':'insufficient_data'}


def smart_money_footprint(df, funding=None, oi_change=None, bid_share=None, cvd=None, liquidation_flow=None):
    div=volume_price_divergence_matrix(df,oi_change,cvd)
    if div.get('status')!='ok': return {'status':'insufficient_data'}
    flags=[]
    if div.get('price_change_pct',0)>0 and div.get('volume_ratio',0)<.8: flags.append('price_up_volume_weak')
    if div.get('price_change_pct',0)<0 and div.get('volume_ratio',0)<.8: flags.append('price_down_volume_weak')
    if oi_change is not None and float(oi_change)>0 and div.get('price_change_pct',0)>0: flags.append('price_and_oi_rising')
    if bid_share is not None and float(bid_share)>.60: flags.append('bid_depth_dominant')
    if funding is not None and abs(float(funding))>.001: flags.append('crowded_funding')
    return {'status':'ok','footprints':flags,'cvd_available':cvd is not None,'liquidation_flow_available':liquidation_flow is not None,
            'interpretation':'possible positioning/absorption clues only; no verified identification of large traders'}


def volatility_shock_detector(df, oi_change=None, liquidation_ratio=None):
    v=volatility_forecast(df); r=_returns(_series(df))
    if len(r)<40: return {'status':'insufficient_data','shock':False}
    recent=float(np.std(r[-5:])); baseline=float(np.std(r[-40:-5]))+1e-12
    score=recent/baseline; flags=[]
    if score>=2.5: flags.append('realized_volatility_spike')
    if oi_change is not None and abs(float(oi_change))>=8: flags.append('open_interest_shock')
    if liquidation_ratio is not None and float(liquidation_ratio)>=3: flags.append('liquidation_surge')
    return {'status':'ok','shock':bool(flags),'volatility_ratio':round(score,2),'flags':flags,'ewma':v}


def correlation_breakdown(asset_returns, benchmark='BTC', window=30, baseline=120):
    a=np.asarray((asset_returns or {}).get('asset',[]),dtype=float); b=np.asarray((asset_returns or {}).get(benchmark,[]),dtype=float)
    n=min(len(a),len(b))
    if n<window+baseline: return {'status':'insufficient_data'}
    recent=float(np.corrcoef(a[-window:],b[-window:])[0,1]); prior=float(np.corrcoef(a[-window-baseline:-window],b[-window-baseline:-window])[0,1])
    if not np.isfinite(recent) or not np.isfinite(prior): return {'status':'insufficient_data'}
    return {'status':'ok','recent_corr':round(recent,3),'baseline_corr':round(prior,3),'breakdown':abs(recent-prior)>=.45}


def market_manipulation_watch(df, orderbook_snapshots=None):
    trap=liquidity_trap_detector(df); r=_returns(_series(df)); flags=[]
    if len(r)>=30:
        z=abs(float(r[-1]-np.mean(r[-30:])))/(float(np.std(r[-30:]))+1e-12)
        if z>=4 and trap.get('volume_ratio',0)<1.2: flags.append('large_price_move_without_volume_confirmation')
    if trap.get('trap_risk')=='high': flags.append('repeated_or_fresh_liquidity_sweep')
    if orderbook_snapshots and len(orderbook_snapshots)>=2:
        before=float(orderbook_snapshots[-2] or 0); after=float(orderbook_snapshots[-1] or 0)
        if before>0 and abs(after-before)/before>.65: flags.append('rapid_depth_change')
    return {'status':'ok' if trap.get('status')=='ok' else 'insufficient_data','suspicious_activity':bool(flags),'risk_flags':flags,
            'wording':'suspicious market activity; manipulation is not proven'}


def signal_decay_assessment(signal, current_price=None, now=None, ttl_seconds=43200, atr=None):
    if not signal: return {'status':'insufficient_data','valid':False}
    now=float(now or time.time()); age=max(0,now-float(signal.get('ts',now) or now)); reasons=[]; score=100
    if age>ttl_seconds: score=0; reasons.append('age_expired')
    if current_price and signal.get('entry') and atr and atr>0:
        dist=abs(float(current_price)-float(signal['entry']))/float(atr)
        if dist>2.5: score-=45; reasons.append('entry_zone_lost')
    if signal.get('invalidated') or signal.get('status') in ('invalidated','expired','closed'): score=0; reasons.append('status_invalid')
    return {'status':'ok','decay_score':max(0,score),'valid':score>0,'age_seconds':round(age),'reasons':reasons}


def trade_recovery_assessment(position, current_price, structure_valid=None, side_prob=None):
    if not position or not current_price: return {'status':'insufficient_data'}
    side=str(position.get('side','LONG')).upper(); entry=float(position.get('entry_price',position.get('entry',0)) or 0)
    if entry<=0: return {'status':'insufficient_data'}
    pnl=(float(current_price)/entry-1)*100*(1 if side=='LONG' else -1)
    broken=structure_valid is False or (position.get('sl1') and (float(current_price)<=float(position['sl1']) if side=='LONG' else float(current_price)>=float(position['sl1'])))
    return {'status':'invalidated' if broken else 'under_pressure' if pnl<-1.5 else 'recovering' if pnl>0 else 'holding',
            'pnl_pct':round(pnl,3),'structure_valid':structure_valid,'side_probability':side_prob,
            'averaging_down':False,'risk_increase':False}


def adaptive_exit_assessment(position, current_price, momentum_score=None, structure_valid=None):
    if not position or not current_price: return {'status':'insufficient_data'}
    side=str(position.get('side','LONG')).upper(); entry=float(position.get('entry_price',position.get('entry',0)) or 0)
    if entry<=0: return {'status':'insufficient_data'}
    pnl=(float(current_price)/entry-1)*100*(1 if side=='LONG' else -1)
    if structure_valid is False: action='review_full_exit'
    elif momentum_score is not None and momentum_score<25 and pnl>0: action='consider_partial_profit'
    elif momentum_score is not None and momentum_score<15: action='reassess_position'
    else: action='hold_to_plan'
    return {'status':'ok','action':action,'pnl_pct':round(pnl,3),'momentum_score':momentum_score,'no_auto_close':True}


def scenario_simulation_engine(price, atr, side=None, horizon_bars=12):
    if not price or not atr or price<=0 or atr<=0: return {'status':'insufficient_data','scenarios':[]}
    p=float(price); a=float(atr); h=max(1,min(48,int(horizon_bars)))
    return {'status':'ok','scenarios':[
        {'name':'bullish','direction':'LONG','activation':round(p+a*.25,10),'confirmation':round(p+a*.5,10),'target':round(p+a*1.5,10),'cancel':round(p-a*.5,10),'risk':'failed breakout / volatility expansion'},
        {'name':'bearish','direction':'SHORT','activation':round(p-a*.25,10),'confirmation':round(p-a*.5,10),'target':round(p-a*1.5,10),'cancel':round(p+a*.5,10),'risk':'short squeeze / reclaim'},
        {'name':'neutral','direction':'WAIT','activation':None,'confirmation':'range resolution required','target':None,'cancel':'new directional confirmation','risk':'chop and false breakouts'}],
        'horizon_bars':h,'decision':'WAIT' if side is None else 'COMPARE_BOTH','note':'Scenario levels are ATR-based hypotheses, not forecasts.'}


def portfolio_risk_dashboard(positions, closed_history=None, equity=None):
    rows=[x for x in (positions or []) if isinstance(x,dict)]
    risks=[max(0,float(x.get('risk_pct',0) or 0)) for x in rows]
    long=sum(r for x,r in zip(rows,risks) if str(x.get('side','')).upper()=='LONG')
    short=sum(r for x,r in zip(rows,risks) if str(x.get('side','')).upper()=='SHORT')
    closed=closed_history or []
    dd=drawdown_guard([float(x['equity_after']) for x in closed if x.get('equity_after')]) if sum(bool(x.get('equity_after')) for x in closed)>=3 else {'status':'insufficient_data'}
    return {'status':'ok','open_positions':len(rows),'tracked_risk_pct':round(sum(risks),3),'long_risk_pct':round(long,3),'short_risk_pct':round(short,3),
            'largest_position_risk_pct':round(max(risks) if risks else 0,3),'drawdown':dd,'equity_available':equity is not None,
            'note':'Tracked virtual risk only; not exchange account exposure.'}


def strategy_lab_report(history):
    closed=[x for x in (history or []) if x.get('closed_at')]
    wf=walk_forward_validation(closed)
    retirement=strategy_retirement({'trades':wf.get('train',{}).get('trades',0),'expectancy_r':wf.get('train',{}).get('expectancy_r')})
    return {'status':'ok' if closed else 'insufficient_data','out_of_sample':out_of_sample_testing(closed),
            'walk_forward':wf,'probability_calibration':probability_calibration_report(closed),
            'attribution':strategy_attribution(closed),'regime_performance':regime_specific_performance(closed),
            'strategy_retirement':retirement,'false_signals':false_signal_database(closed),'loss_autopsies':[loss_autopsy(x) for x in closed if float(x.get('r',0) or 0)<0][-10:],
            'missed_opportunities':missed_opportunity_analysis([x for x in closed if x.get('missed_opportunity')]),
            'parameter_stability':parameter_sensitivity_from_history(closed)}


def walk_forward_validation(history, min_trades=20, train_fraction=.70):
    """Chronological train/test split; never shuffles future outcomes into training."""
    rows=[x for x in (history or []) if x.get('closed_at') and x.get('r') is not None]
    rows.sort(key=lambda x:float(x.get('closed_at',0) or 0))
    cut=int(len(rows)*max(.5,min(.85,float(train_fraction))))
    train,test=rows[:cut],rows[cut:]
    def metrics(items):
        r=np.asarray([float(x.get('r',0) or 0) for x in items],dtype=float)
        if not len(r): return {'trades':0,'expectancy_r':None,'win_rate_pct':None,'profit_factor':None}
        gains=float(np.sum(r[r>0])); losses=abs(float(np.sum(r[r<0])))
        return {'trades':len(r),'expectancy_r':round(float(np.mean(r)),4),'win_rate_pct':round(float(np.mean(r>0)*100),2),
                'profit_factor':round(gains/losses,3) if losses else None}
    a,b=metrics(train),metrics(test)
    degradation=None if a['expectancy_r'] is None or b['expectancy_r'] is None else round(a['expectancy_r']-b['expectancy_r'],4)
    return {'status':'ok' if len(test)>=max(5,min_trades//3) else 'insufficient_test_sample','train':a,'test':b,
            'expectancy_degradation_r':degradation,'chronological':True,'leakage_safe':True}


def probability_calibration_report(history, min_sample=20):
    rows=[]
    for x in (history or []):
        p=x.get('predicted_probability')
        if p is None or not x.get('closed_at'): continue
        try:
            p=max(0,min(1,float(p))); y=1.0 if float(x.get('r',0) or 0)>0 else 0.0
            rows.append((p,y))
        except Exception: continue
    if not rows: return {'status':'insufficient_data','sample':0,'brier_score':None,'bins':[]}
    brier=float(np.mean([(p-y)**2 for p,y in rows])); bins=[]
    for lo in (0,.2,.4,.6,.8):
        group=[(p,y) for p,y in rows if lo<=p<lo+.2 or lo==.8 and p<=1]
        if group: bins.append({'range':f'{int(lo*100)}-{int((lo+.2)*100)}','n':len(group),'predicted_pct':round(float(np.mean([p for p,_ in group]))*100,1),'realized_win_pct':round(float(np.mean([y for _,y in group]))*100,1)})
    return {'status':'ok' if len(rows)>=min_sample else 'small_sample','sample':len(rows),'brier_score':round(brier,5),'bins':bins,
            'note':'Only closed trades with probability recorded at entry are included.'}


def funding_convergence(funding_history):
    vals=[float(x) for x in (funding_history or []) if x is not None and np.isfinite(float(x))]
    if len(vals)<3: return {'status':'insufficient_data'}
    last=vals[-1]; prev=vals[-2]
    easing=abs(last)<abs(prev)
    return {'status':'ok','current':last,'previous':prev,'converging_to_zero':easing,
            'crowding':'longs' if last>0 else 'shorts' if last<0 else 'neutral',
            'state':'crowding_easing' if easing else 'crowding_building'}


def breakout_failure_probability(df):
    b=breakout_assessment(df)
    if b.get('state')=='unknown': return {'status':'insufficient_data'}
    if b.get('state','').startswith('fake_'): p=85.0
    elif b.get('state','').startswith('breakout_'):
        body=float(b.get('body_ratio',0)); vol=float(b.get('rel_volume',0))
        p=max(5.0,min(65.0,55-body*35-max(0,vol-1)*8))
    else: p=50.0
    return {'status':'ok','failure_probability_pct':round(p,1),'breakout_state':b.get('state'),
            'confirmation_score':round(100-p,1),'method':'OHLCV heuristic; not a calibrated classifier'}
