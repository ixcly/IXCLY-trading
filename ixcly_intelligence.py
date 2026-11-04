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

# ---------------------------------------------------------------------------
# IXCLY Quant Intelligence: calibrated direction, entry timing and portfolio
# diagnostics. Deterministic and dependency-light for Render 512 MB.
# ---------------------------------------------------------------------------

def _closed_outcomes(history, limit=500):
    rows = []
    for item in (history or [])[-max(1, int(limit)):]:
        if not isinstance(item, dict) or str(item.get('status', '')).lower() not in ('win', 'loss', 'be', 'closed'):
            continue
        try:
            r = float(item.get('r'))
            if math.isfinite(r):
                rows.append((item, r))
        except (TypeError, ValueError):
            continue
    return rows


def _raw_direction_probability(item):
    probs = item.get('long_prob') or item.get('tf_probs') or {}
    if not isinstance(probs, dict):
        return None
    weights = {'1d': .30, '4h': .30, '1h': .25, '15m': .15}
    vals = [(float(v), weights.get(str(k).lower(), .10)) for k, v in probs.items()
            if k in weights and _num(v, -1) >= 0 and _num(v, 101) <= 100]
    if not vals:
        return None
    den = sum(w for _, w in vals)
    return sum(v*w for v, w in vals) / den if den else None


def probability_calibration(history, raw_long_probability, side, min_sample=20):
    """Empirical directional calibration from closed trades; never calls a score a probability without evidence."""
    side = str(side or '').upper()
    if side not in ('LONG', 'SHORT') or raw_long_probability is None:
        return {'status': 'insufficient_data', 'calibrated_probability': None, 'sample': 0,
                'note': 'Нет пригодной исходной оценки направления.'}
    raw = _num(raw_long_probability, 50.0)
    raw_conf = raw if side == 'LONG' else 100.0 - raw
    samples = []
    for item, r in _closed_outcomes(history):
        p_long = _raw_direction_probability(item)
        if p_long is None:
            p_long = _num(item.get('raw_long_probability'), -1)
        if not 0 <= p_long <= 100:
            continue
        historical_side = str(item.get('side') or '').upper()
        if historical_side not in ('LONG', 'SHORT'):
            continue
        confidence = p_long if historical_side == 'LONG' else 100.0 - p_long
        if abs(confidence - raw_conf) <= 10:
            samples.append(1.0 if r > 0 else 0.0 if r < 0 else 0.5)
    if len(samples) < min_sample:
        return {'status': 'warming_up', 'raw_confidence': round(raw_conf, 1),
                'calibrated_probability': None, 'sample': len(samples), 'required': min_sample,
                'note': 'Недостаточно закрытых сопоставимых сделок; показывается только сырая оценка модели.'}
    # Beta(2,2) smoothing prevents extreme 0/100% outputs on finite samples.
    wins = sum(samples)
    calibrated = (wins + 2.0) / (len(samples) + 4.0) * 100.0
    return {'status': 'calibrated', 'raw_confidence': round(raw_conf, 1),
            'calibrated_probability': round(calibrated, 1), 'sample': len(samples),
            'wins_equivalent': round(wins, 1), 'note': 'Эмпирическая частота исходов в похожем диапазоне оценок; не гарантия результата.'}


def directional_confidence(tf_data, side, history=None, min_calibration_sample=20):
    """Combine timeframe direction with disagreement penalty and optional empirical calibration."""
    weights = {'1d': .30, '4h': .30, '1h': .25, '15m': .15}
    rows = [(tf, _num((tf_data or {}).get(tf, {}).get('long_prob'), -1), w)
            for tf, w in weights.items() if isinstance((tf_data or {}).get(tf), dict)]
    rows = [(tf, p, w) for tf, p, w in rows if 0 <= p <= 100]
    if not rows:
        return {'status': 'insufficient_data', 'raw_long_probability': None,
                'directional_confidence': None, 'calibration': {'status': 'insufficient_data'}}
    den = sum(w for _, _, w in rows)
    raw_long = sum(p*w for _, p, w in rows) / den
    chosen = str(side or ('LONG' if raw_long >= 50 else 'SHORT')).upper()
    raw_conf = raw_long if chosen == 'LONG' else 100.0 - raw_long
    aligned = sum(1 for _, p, _ in rows if (p >= 50) == (chosen == 'LONG'))
    coverage = len(rows)
    values = [p if chosen == 'LONG' else 100.0-p for _, p, _ in rows]
    dispersion = (sum((v - sum(values)/len(values))**2 for v in values)/len(values))**.5 if values else 50
    agreement = aligned / coverage * 100.0
    # This is model confidence, not win probability. Penalize disagreement and dispersion.
    confidence = max(0.0, min(100.0, raw_conf - max(0.0, 75.0-agreement)*.22 - max(0.0, dispersion-12.0)*.18))
    calibration = probability_calibration(history or [], raw_long, chosen, min_calibration_sample)
    return {'status': 'ok', 'side': chosen, 'raw_long_probability': round(raw_long, 1),
            'raw_directional_confidence': round(raw_conf, 1), 'directional_confidence': round(confidence, 1),
            'timeframe_agreement_pct': round(agreement, 1), 'timeframe_dispersion': round(dispersion, 1),
            'timeframes_used': coverage, 'timeframes': {tf: round(p, 1) for tf, p, _ in rows},
            'calibration': calibration,
            'note': 'Directional confidence — согласованность модели. Калиброванная вероятность появляется только при достаточной выборке закрытых сделок.'}


def entry_timing_assessment(signal, tf_data=None):
    """Score whether the proposed entry is timely rather than merely directionally correct."""
    s = signal or {}
    price, entry = _num(s.get('price')), _num(s.get('entry'))
    side = str(s.get('side') or '').upper()
    info = (tf_data or {}).get('4h') or (tf_data or {}).get('1h') or {}
    atr = _num(info.get('atr'))
    score, reasons, blocked = 70.0, [], False
    if price <= 0 or entry <= 0 or side not in ('LONG', 'SHORT'):
        return {'status': 'insufficient_data', 'score': None, 'blocked': True,
                'reasons': ['Нет цены или корректной точки входа.']}
    distance_pct = abs(entry-price)/price*100.0
    atr_pct = atr/price*100.0 if atr > 0 else None
    if atr_pct and atr_pct > 0:
        distance_atr = distance_pct/atr_pct
        if distance_atr > 1.5:
            score -= 35; blocked = True; reasons.append('Вход слишком далеко от текущей цены относительно ATR.')
        elif distance_atr > .9:
            score -= 20; reasons.append('Цена уже существенно ушла от расчётной зоны.')
        elif distance_atr > .45:
            score -= 8; reasons.append('Умеренное удаление от точки входа.')
        else:
            reasons.append('Точка входа находится вблизи текущей цены.')
    elif distance_pct > 1.5:
        score -= 25; reasons.append('Значительное расстояние от цены; ATR недоступен.')
    anti = s.get('anti_fomo') or {}
    if anti.get('blocked'):
        score -= 30; blocked = True; reasons.append('Anti-FOMO обнаружил перегретое движение.')
    elif _num(anti.get('penalty')) > 0:
        score -= min(18, _num(anti.get('penalty'))); reasons.append('Есть признаки растянутого движения.')
    breakout = s.get('breakout') or {}
    state = str(breakout.get('state') or '')
    if state.startswith('fake_'):
        score -= 25; blocked = True; reasons.append('Обнаружен вероятный ложный пробой.')
    elif state in ('breakout_up', 'breakout_down'):
        reasons.append('Пробой требует подтверждения удержанием уровня.')
    if not atr_pct:
        score -= 5; reasons.append('ATR недоступен — оценка времени входа менее надёжна.')
    score = round(max(0.0, min(100.0, score)), 1)
    if score < 35: blocked = True
    return {'status': 'ok', 'score': score, 'blocked': blocked,
            'distance_pct': round(distance_pct, 3), 'distance_atr': round(distance_pct/atr_pct, 2) if atr_pct else None,
            'atr_pct': round(atr_pct, 3) if atr_pct else None, 'reasons': reasons,
            'note': 'Оценка качества момента входа, а не прогноз направления.'}


def reversal_probability_assessment(side, tf_data=None, regime_transition=None, evidence_map=None,
                                    early_movement=None, breakout=None):
    """Heuristic early-warning score for adverse reversal; explicitly not a calibrated probability."""
    side = str(side or '').upper()
    if side not in ('LONG', 'SHORT'):
        return {'status': 'insufficient_data', 'risk_score': None, 'level': 'unknown'}
    weights = {'15m': .15, '1h': .25, '4h': .30, '1d': .30}
    opposing = 0.0; coverage = 0; details = []
    for tf, weight in weights.items():
        row = (tf_data or {}).get(tf) or {}
        p = _num(row.get('long_prob'), -1)
        if not 0 <= p <= 100: continue
        coverage += 1
        against = 100-p if side == 'LONG' else p
        if against >= 55:
            opposing += weight * min(1.0, (against-50)/35)
        details.append({'timeframe': tf, 'adverse_direction_score': round(against, 1)})
    score = (opposing / max(sum(weights[t] for t in weights if isinstance((tf_data or {}).get(t), dict) and (tf_data or {}).get(t, {}).get('long_prob') is not None), .01))*38 if coverage else 0
    state = (regime_transition or {}).get('state')
    if state == 'direction_reversal_watch': score += 22
    elif state == 'volatility_expansion': score += 8
    conflict = _num((evidence_map or {}).get('conflict_pct'))
    score += min(18, conflict*.18)
    early = early_movement or {}
    if early.get('direction') in ('LONG', 'SHORT') and early.get('direction') != side:
        score += 12
    if breakout and str(breakout.get('state') or '').startswith('fake_'):
        score += 12
    score = round(max(0.0, min(100.0, score)), 1)
    level = 'critical' if score >= 75 else 'high' if score >= 55 else 'watch' if score >= 35 else 'low'
    return {'status': 'ok' if coverage else 'insufficient_data', 'risk_score': score,
            'level': level, 'opposing_timeframes': details,
            'note': 'Эвристический риск разворота для раннего предупреждения; не вероятность будущего события.'}


def adaptive_confidence_threshold(history, base=66.0, regime=None, min_sample=20):
    """Raise/lower signal threshold only when closed-outcome sample is adequate."""
    closed = _closed_outcomes(history, 500)
    selected = [(x, r) for x, r in closed if not regime or str((x.get('market_regime') or {}).get('name', x.get('regime'))) == str(regime)]
    sample = selected if len(selected) >= min_sample else closed
    scope = 'regime' if sample is selected and len(selected) >= min_sample else 'all'
    if len(sample) < min_sample:
        return {'threshold': round(max(55.0, min(85.0, _num(base, 66))), 1),
                'adjustment': 0, 'sample': len(sample), 'scope': 'insufficient_sample',
                'note': f'Адаптация выключена: нужно минимум {min_sample} закрытых сделок.'}
    wins = sum(1 for _, r in sample if r > 0)
    win_rate = wins/len(sample)*100
    avg_r = sum(r for _, r in sample)/len(sample)
    delta = 8 if win_rate < 40 or avg_r < -.20 else 4 if win_rate < 48 or avg_r < 0 else -2 if win_rate >= 62 and avg_r > .25 else 0
    threshold = max(55.0, min(85.0, _num(base, 66) + delta))
    return {'threshold': round(threshold, 1), 'adjustment': delta, 'sample': len(sample),
            'scope': scope, 'win_rate': round(win_rate, 1), 'avg_r': round(avg_r, 3),
            'note': 'Порог меняется только по закрытым сделкам; автообучение не меняет стратегию без достаточной выборки.'}


def portfolio_exposure_assessment(history, correlation_groups=None, max_same_side=2):
    """Read-only portfolio heatmap from active/in-trade signal records; not exchange account truth."""
    groups = correlation_groups or []
    active = [x for x in (history or []) if isinstance(x, dict)
              and str(x.get('status', '')).lower() in ('active', 'in_trade')]
    rows, group_counts, side_counts = [], {}, {'LONG': 0, 'SHORT': 0}
    for item in active:
        coin = str(item.get('coin') or '')
        side = str(item.get('side') or '').upper()
        if side not in side_counts: continue
        side_counts[side] += 1
        group = next((f'group_{i+1}' for i, g in enumerate(groups) if coin in g), coin)
        key = f'{group}:{side}'; group_counts[key] = group_counts.get(key, 0)+1
        rows.append({'coin': coin, 'side': side, 'group': group,
                     'quality': _num(item.get('quality_score')), 'status': item.get('status')})
    crowded = [k for k, n in group_counts.items() if n > 1]
    dominant = max(side_counts, key=side_counts.get) if active else None
    imbalance = max(side_counts.values())/max(1, len(rows))*100 if rows else 0
    risk_level = 'high' if len(crowded) or max(side_counts.values()) > max_same_side else 'elevated' if imbalance >= 75 and len(rows) >= 3 else 'normal'
    return {'status': 'ok', 'active_signals': len(rows), 'side_counts': side_counts,
            'dominant_side': dominant, 'directional_imbalance_pct': round(imbalance, 1),
            'correlated_clusters': crowded, 'risk_level': risk_level, 'positions': rows,
            'note': 'Снимок только по сигналам IXCLY; для фактической экспозиции биржи нужна синхронизация с account positions.'}


def no_trade_decision(confidence, entry_timing, reversal, adaptive, min_confidence=66.0):
    """Explicit WAIT gate with reasons; missing diagnostics do not masquerade as positive evidence."""
    reasons = []
    c = _num((confidence or {}).get('directional_confidence'), -1)
    threshold = _num((adaptive or {}).get('threshold'), min_confidence)
    if c < 0:
        reasons.append('Недостаточно данных для оценки направления.')
    elif c < threshold:
        reasons.append(f'Уверенность направления {c:.1f} ниже адаптивного порога {threshold:.1f}.')
    if (entry_timing or {}).get('blocked'):
        reasons.extend((entry_timing or {}).get('reasons') or ['Момент входа заблокирован.'])
    reversal_score = _num((reversal or {}).get('risk_score'), -1)
    if reversal_score >= 75:
        reasons.append('Критически высокий риск движения против выбранного направления.')
    cal = (confidence or {}).get('calibration') or {}
    if cal.get('status') == 'calibrated' and _num(cal.get('calibrated_probability'), 100) < 42:
        reasons.append('Историческая калибровка похожих прогнозов неудовлетворительна.')
    return {'action': 'WAIT' if reasons else 'REVIEW', 'blocked': bool(reasons),
            'threshold': round(threshold, 1), 'reasons': list(dict.fromkeys(reasons)),
            'note': 'REVIEW означает допуск к остальным фильтрам, а не автоматическое разрешение сделки.'}
