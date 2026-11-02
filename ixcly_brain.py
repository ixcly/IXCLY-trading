# -*- coding: utf-8 -*-
"""IXCLY Decision Brain: lightweight, deterministic market research layer.
Standard library only. No order execution, network access, or ML framework.
"""
from __future__ import annotations
import math
import time
from collections import defaultdict

VERSION = "1.1-COGNITIVE"


def _n(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _col(df, name):
    try:
        return [_n(x) for x in df[name].tolist()]
    except Exception:
        return []


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _pct(a, b):
    return (a / b - 1.0) * 100.0 if b else 0.0


def _closed(history):
    return [x for x in (history or []) if isinstance(x, dict)
            and str(x.get("status", "")).lower() in ("win", "loss", "be", "closed")
            and x.get("r") is not None]


def market_fingerprint(coin, df, history=None):
    """Compact per-coin behavior profile; historical matches only use stored comparable features."""
    close, high, low, vol = (_col(df, k) for k in ("close", "high", "low", "volume"))
    if len(close) < 20:
        return {"status": "insufficient_data", "coin": coin, "samples": len(close)}
    n = min(20, len(close))
    ret1 = _pct(close[-1], close[-2]) if len(close) > 1 else 0.0
    ret5 = _pct(close[-1], close[-6]) if len(close) > 5 else 0.0
    ret20 = _pct(close[-1], close[-20]) if len(close) > 19 else 0.0
    ranges = [max(0.0, h-l) / c * 100 for h, l, c in zip(high[-n:], low[-n:], close[-n:]) if c]
    base_vol = _mean(vol[-21:-1])
    vol_ratio = vol[-1] / base_vol if base_vol > 0 and vol else 0.0
    trend = "up" if ret20 > 1.0 else "down" if ret20 < -1.0 else "flat"
    volatility = _mean(ranges)
    features = {"ret1_pct": round(ret1, 3), "ret5_pct": round(ret5, 3),
                "ret20_pct": round(ret20, 3), "vol_ratio": round(vol_ratio, 3),
                "range_pct": round(volatility, 3)}
    prior = []
    for item in (history or [])[-500:]:
        fp = item.get("market_fingerprint") if isinstance(item, dict) else None
        if isinstance(fp, dict) and item.get("coin") == coin:
            prior.append(fp)
    similar = []
    for fp in prior:
        keys = ("ret5_pct", "ret20_pct", "vol_ratio", "range_pct")
        d = sum(abs(_n(features[k])- _n(fp.get(k))) / max(1.0, abs(_n(features[k])), abs(_n(fp.get(k)))) for k in keys) / len(keys)
        if d <= 0.35:
            similar.append(fp)
    return {"status": "ok", "coin": coin, "trend": trend, "volatility_pct": round(volatility, 3),
            "volume_ratio": round(vol_ratio, 2), "features": features,
            "historical_matches": len(similar), "sample_note": "Сравнения доступны только для сохранённых сопоставимых отпечатков."}


def signal_fingerprint(signal, setup=None, context=None):
    s, c = signal or {}, context or {}
    factors = []
    strength = _n(s.get("strength", (setup or {}).get("strength", 0)))
    factors.append({"name": "Техническая сила", "value": round(strength), "weight": 0.30})
    consensus = str(s.get("tf_consensus") or (setup or {}).get("extras", {}).get("tf_consensus", ""))
    try:
        a, b = consensus.split("/", 1)
        mtf = 100 * _n(a) / max(1, _n(b))
    except Exception:
        mtf = 0
    factors.append({"name": "Согласие таймфреймов", "value": round(mtf), "weight": 0.20})
    bt = s.get("bt") or {}
    bt_score = min(100, max(0, _n(bt.get("winrate")) * 0.7 + max(0, _n(bt.get("avg_r")))*15))
    factors.append({"name": "Исторический бэктест", "value": round(bt_score), "weight": 0.15,
                    "available": bool(bt)})
    vol = c.get("volume_anomaly") or {}
    vol_score = min(100, max(0, _n(vol.get("score"), 0)))
    factors.append({"name": "Объём", "value": round(vol_score), "weight": 0.10,
                    "available": bool(vol)})
    book = c.get("orderbook_bid_share")
    side = str(s.get("side", "LONG")).upper()
    book_score = (min(100, max(0, 50 + ( _n(book, .5)-.5)*400)) if side == "LONG"
                  else min(100, max(0, 50 + (.5-_n(book, .5))*400))) if book is not None else 50
    factors.append({"name": "Стакан", "value": round(book_score), "weight": 0.10,
                    "available": book is not None})
    news = c.get("news_risk") or s.get("news_risk") or {}
    news_penalty = min(100, max(0, _n(news.get("penalty")) * 5))
    factors.append({"name": "Новостной риск", "value": round(100-news_penalty), "weight": 0.15})
    available_weight = sum(x["weight"] for x in factors if x.get("available", True))
    score = sum(x["value"]*x["weight"] for x in factors if x.get("available", True)) / available_weight if available_weight else 0
    conflicts = []
    if news.get("severity") in ("high", "critical"):
        conflicts.append("Повышенный новостной риск")
    if book is not None and ((side == "LONG" and _n(book) < .42) or (side == "SHORT" and _n(book) > .58)):
        conflicts.append("Стакан против направления")
    if mtf and mtf < 40:
        conflicts.append("Слабое согласие таймфреймов")
    return {"score": round(score, 1), "grade": "A" if score >= 80 else "B" if score >= 68 else "C" if score >= 55 else "D",
            "factors": factors, "conflicts": conflicts,
            "explanation": "Оценка нормируется по доступным факторам; отсутствующие данные не считаются нулём."}


def decision_brain(signal_dna, early=None, crowd=None, news=None, forecast=None):
    """Explainable action selector. It never opens a trade."""
    dna = signal_dna or {}
    early, crowd, news = early or {}, crowd or {}, news or {}
    score = _n(dna.get("score"))
    conflicts = list(dna.get("conflicts") or [])
    severe = news.get("severity") == "critical" and _n(news.get("penalty")) >= 7
    crowd_level = str(crowd.get("level", "low")).lower()
    if severe:
        action, why = "BLOCK", "Критический прямой новостной риск требует повторной оценки."
    elif score < 48 or len(conflicts) >= 3:
        action, why = "WAIT", "Недостаточно согласованных факторов."
    elif crowd_level in ("high", "extreme") and score < 78:
        action, why = "WAIT", "Рынок перегружен в одном направлении."
    elif str((early or {}).get("state", "")).lower() == "watch" and score < 72:
        action, why = "WATCH", "Есть признаки раннего движения, но подтверждения ещё недостаточны."
    elif score >= 78 and not conflicts:
        action, why = "ENTER_CANDIDATE", "Сильная согласованность факторов; это кандидат на проверку существующими правилами."
    else:
        action, why = "RECHECK", "Нужна повторная проверка после обновления свечей и потоков."
    return {"action": action, "score": round(score, 1), "reason": why,
            "conflicts": conflicts, "trade_execution": False,
            "note": "Decision Brain не заменяет текущие фильтры и не отправляет ордера."}


def liquidity_path(df, orderbook=None, max_levels=5):
    close, high, low, vol = (_col(df, k) for k in ("close", "high", "low", "volume"))
    if len(close) < 12:
        return {"status": "insufficient_data", "levels": []}
    price = close[-1]
    atr = _mean([h-l for h, l in zip(high[-14:], low[-14:])])
    candidates = []
    for i in range(max(1, len(close)-80), len(close)-2):
        if high[i] >= max(high[max(0, i-2):i+3]):
            candidates.append(("resistance", high[i], vol[i] if i < len(vol) else 0))
        if low[i] <= min(low[max(0, i-2):i+3]):
            candidates.append(("support", low[i], vol[i] if i < len(vol) else 0))
    merged = []
    tol = max(atr * .25, price * .001)
    for kind, level, weight in sorted(candidates, key=lambda x: abs(x[1]-price)):
        if (level-price)*(1 if kind == "resistance" else -1) < -tol:
            continue
        existing = next((x for x in merged if x["kind"] == kind and abs(x["price"]-level) <= tol), None)
        if existing:
            existing["weight"] += weight
        else:
            merged.append({"kind": kind, "price": round(level, 8), "weight": weight})
    merged.sort(key=lambda x: abs(x["price"]-price))
    bid = _n(orderbook, .5)
    return {"status": "ok", "price": price, "atr": round(atr, 8),
            "orderbook_bias": "buyers" if bid >= .56 else "sellers" if bid <= .44 else "balanced",
            "levels": [{**x, "distance_pct": round(_pct(x["price"], price), 2)} for x in merged[:max_levels]],
            "note": "Уровни рассчитаны по локальным pivot high/low, это зоны внимания, а не подтверждённые кластеры ликвидности."}


def news_reaction_assessment(news, df):
    news = news or {}
    close = _col(df, "close")
    if len(close) < 4:
        return {"status": "insufficient_data", "divergence": False}
    move = _pct(close[-1], close[-4])
    bias = _n(news.get("bias"))
    relevant = bool(news.get("relevant") or news.get("severity") in ("high", "critical"))
    divergence = relevant and ((bias > 0 and move < -0.5) or (bias < 0 and move > 0.5))
    return {"status": "ok", "news_bias": "bullish" if bias > 0 else "bearish" if bias < 0 else "neutral",
            "observed_move_pct": round(move, 2), "divergence": divergence,
            "label": "Расхождение ожидания и реакции" if divergence else "Явного расхождения нет",
            "note": "Сравнение текущей краткосрочной реакции с новостным уклоном; причинность не утверждается."}


def strategy_dna(history, min_sample=5):
    groups = defaultdict(list)
    for x in _closed(history):
        keys = (("strategy", str(x.get("type") or x.get("strategy") or "unknown")),
                ("regime", str(x.get("market_regime") or x.get("regime") or "unknown")),
                ("coin", str(x.get("coin") or "unknown")))
        for key in keys:
            groups[key].append(x)
    out = []
    for (kind, name), rows in groups.items():
        rs = [_n(x.get("r")) for x in rows]
        wins = sum(r > 0 for r in rs)
        out.append({"dimension": kind, "name": name, "trades": len(rs),
                    "win_rate": round(wins/len(rs)*100, 1), "avg_r": round(_mean(rs), 3),
                    "sample_ok": len(rs) >= min_sample,
                    "profile": "positive" if _mean(rs) > .15 and len(rs) >= min_sample else
                               "weak" if _mean(rs) < -.15 and len(rs) >= min_sample else "unproven"})
    return sorted(out, key=lambda x: (x["sample_ok"], x["avg_r"], x["trades"]), reverse=True)[:30]


def counterfactual_lab(history, min_sample=5):
    rows = _closed(history)
    if not rows:
        return {"status": "no_data", "trades": 0, "findings": ["Пока нет закрытых результатов для анализа."]}
    rs = [_n(x.get("r")) for x in rows]
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r < 0]
    findings = []
    winner_mfe = [_n(x.get("mfe")) for x in rows if _n(x.get("r")) > 0 and x.get("mfe") is not None]
    loser_mfe = [_n(x.get("mfe")) for x in rows if _n(x.get("r")) <= 0 and x.get("mfe") is not None]
    loser_mae = [_n(x.get("mae")) for x in rows if _n(x.get("r")) <= 0 and x.get("mae") is not None]
    if len(rows) < min_sample:
        findings.append(f"Малая выборка: {len(rows)} из {min_sample} желательных закрытых результатов.")
    if losses and abs(_mean(losses)) > max(.1, _mean(wins) if wins else 0):
        findings.append("Средний убыток превышает среднюю прибыль: проверьте стопы и отбор входов.")
    if len(loser_mfe) >= min_sample and _mean(loser_mfe) >= 0.7:
        findings.append("Убыточные сигналы часто сначала проходили в прибыльную сторону: проверьте фиксацию частичной прибыли и сопровождение.")
    if len(winner_mfe) >= min_sample and _mean(winner_mfe) > 1.5:
        findings.append("Прибыльные сделки показывают заметный благоприятный ход: можно отдельно проверить, не слишком ли рано фиксируются цели.")
    if len(wins) >= min_sample and len(losses) >= min_sample:
        findings.append("Достаточно результатов для сравнения признаков прибыльных и убыточных сделок.")
    if not findings:
        findings.append("Недостаточно данных для надёжных контрфактических выводов; система не будет придумывать альтернативный результат.")
    return {"status": "ok", "trades": len(rows), "win_rate": round(len(wins)/len(rows)*100, 1),
            "avg_r": round(_mean(rs), 3), "avg_win_r": round(_mean(wins), 3) if wins else 0,
            "avg_loss_r": round(_mean(losses), 3) if losses else 0,
            "avg_winner_mfe_r": round(_mean(winner_mfe), 2) if winner_mfe else None,
            "avg_loser_mfe_r": round(_mean(loser_mfe), 2) if loser_mfe else None,
            "avg_loser_mae_r": round(_mean(loser_mae), 2) if loser_mae else None,
            "findings": findings,
            "note": "Анализирует фактические MFE/MAE и R. Не заявляет альтернативный исход без полного набора свечей."}


def replay_signal(signal, candles):
    """Chronological OHLC replay; if SL and TP are touched in one candle, conservative SL-first."""
    s = signal or {}
    side = str(s.get("side", "LONG")).upper()
    entry = _n(s.get("entry"))
    stop = _n(s.get("sl1") or s.get("sl"))
    targets = s.get("tps") or [s.get("tp1"), s.get("tp2"), s.get("tp3")]
    targets = [_n(x) for x in targets if _n(x) > 0]
    if side not in ("LONG", "SHORT") or entry <= 0 or stop <= 0:
        return {"status": "invalid_signal"}
    active, result, hit = False, "not_triggered", []
    signal_ts_ms = _n(s.get("ts")) * (1000 if _n(s.get("ts")) < 10_000_000_000 else 1)
    checked = 0
    valid_ts = []
    for candle in candles or []:
        try:
            ts, op, hi, lo, cl = [_n(candle[i]) for i in range(5)]
        except Exception:
            continue
        valid_ts.append(ts)
        if signal_ts_ms and ts < signal_ts_ms:
            continue
        checked += 1
        if not active:
            active = lo <= entry <= hi
            if not active:
                continue
        stop_hit = lo <= stop if side == "LONG" else hi >= stop
        target_hits = [i+1 for i, tp in enumerate(targets) if (hi >= tp if side == "LONG" else lo <= tp)]
        if stop_hit:
            result = "stop"
            break
        if target_hits:
            hit.extend(x for x in target_hits if x not in hit)
            if len(hit) == len(targets):
                result = "all_targets"
                break
    coverage_complete = bool(valid_ts) and (not signal_ts_ms or min(valid_ts) <= signal_ts_ms)
    return {"status": "ok", "entered": active, "result": result, "targets_hit": hit,
            "candles_checked": checked, "coverage_complete": coverage_complete,
            "conservative_same_candle_rule": "stop_first",
            "note": "OHLC replay has intrabar ambiguity; stop-first is used when stop and target share a candle. Coverage is partial if requested candles begin after signal time."}



def regime_transition_assessment(df):
    """Detect a possible phase change using only completed recent OHLCV windows."""
    close, high, low, vol = (_col(df, k) for k in ("close", "high", "low", "volume"))
    n = min(len(close), len(high), len(low), len(vol))
    if n < 32:
        return {"status": "insufficient_data", "bars": n,
                "note": "Для оценки перехода режима нужны минимум 32 свечи."}
    close, high, low, vol = close[-32:], high[-32:], low[-32:], vol[-32:]
    def window_stats(start, end):
        cs, hs, ls, vs = close[start:end], high[start:end], low[start:end], vol[start:end]
        returns = [_pct(cs[i], cs[i-1]) for i in range(1, len(cs)) if cs[i-1]]
        ranges = [max(0.0, h-l)/c*100 for h, l, c in zip(hs, ls, cs) if c]
        return {"drift_pct": _pct(cs[-1], cs[0]) if cs and cs[0] else 0.0,
                "range_pct": _mean(ranges), "abs_return_pct": _mean([abs(x) for x in returns]),
                "volume": _mean(vs)}
    old, recent = window_stats(0, 24), window_stats(24, 32)
    vol_ratio = recent["range_pct"] / max(old["range_pct"], 1e-9)
    move_ratio = recent["abs_return_pct"] / max(old["abs_return_pct"], 1e-9)
    sign_old = 1 if old["drift_pct"] > .15 else -1 if old["drift_pct"] < -.15 else 0
    sign_new = 1 if recent["drift_pct"] > .15 else -1 if recent["drift_pct"] < -.15 else 0
    if vol_ratio >= 1.65 and move_ratio >= 1.45:
        state, label = "volatility_expansion", "Резкое расширение диапазона"
    elif sign_old and sign_new and sign_old != sign_new and abs(recent["drift_pct"]) >= .25:
        state, label = "direction_reversal_watch", "Возможная смена направления"
    elif vol_ratio <= .62:
        state, label = "compression", "Сжатие волатильности"
    elif move_ratio >= 1.5:
        state, label = "momentum_acceleration", "Ускорение движения"
    else:
        state, label = "stable_or_unclear", "Устойчивый режим или недостаточно признаков перехода"
    return {"status": "ok", "state": state, "label": label,
            "recent_range_vs_previous": round(vol_ratio, 2),
            "recent_move_intensity_vs_previous": round(move_ratio, 2),
            "previous_drift_pct": round(old["drift_pct"], 2),
            "recent_drift_pct": round(recent["drift_pct"], 2),
            "volume_ratio_recent_vs_previous": round(recent["volume"]/max(old["volume"], 1e-9), 2),
            "note": "Сравнение двух завершённых окон свечей; это раннее предупреждение, не подтверждение нового тренда."}


def signal_fragility(signal_dna):
    """Leave-one-factor-out sensitivity; reveals signals dominated by one input."""
    factors = [x for x in (signal_dna or {}).get("factors", [])
               if isinstance(x, dict) and x.get("available", True)]
    if len(factors) < 2:
        return {"status": "insufficient_data", "fragility": None,
                "note": "Недостаточно независимых факторов для теста устойчивости."}
    base_weights = [_n(x.get("weight")) for x in factors]
    base_total = sum(base_weights)
    if base_total <= 0:
        return {"status": "insufficient_data", "fragility": None}
    base = sum(_n(x.get("value"))*w for x, w in zip(factors, base_weights))/base_total
    deltas = []
    for i, factor in enumerate(factors):
        remain = [(x, w) for j, (x, w) in enumerate(zip(factors, base_weights)) if j != i and w > 0]
        denom = sum(w for _, w in remain)
        alt = sum(_n(x.get("value"))*w for x, w in remain)/denom if denom else base
        deltas.append({"factor": str(factor.get("name") or "unknown"), "score_without": round(alt, 1),
                       "score_delta": round(alt-base, 1), "weight_share_pct": round(base_weights[i]/base_total*100, 1)})
    dominant = max(deltas, key=lambda x: abs(x["score_delta"]))
    fragility = max(abs(x["score_delta"]) for x in deltas)
    level = "high" if fragility >= 12 else "medium" if fragility >= 6 else "low"
    return {"status": "ok", "base_score": round(base, 1), "fragility": round(fragility, 1),
            "level": level, "dominant_dependency": dominant["factor"],
            "leave_one_out": sorted(deltas, key=lambda x: abs(x["score_delta"]), reverse=True),
            "note": "Проверяет чувствительность оценки к удалению одного фактора; не измеряет вероятность успеха."}


def evidence_conflict_map(side, context=None, df=None):
    """Independent directional evidence map with explicit coverage and disagreement."""
    c = context or {}
    side = str(side or "").upper()
    if side not in ("LONG", "SHORT"):
        close = _col(df, "close") if df is not None else []
        side = "LONG" if len(close) >= 6 and close[-1] >= close[-6] else "SHORT" if len(close) >= 6 else ""
    if not side:
        return {"status": "insufficient_data", "evidence": [], "coverage": 0}
    rows = []
    def add(name, direction, weight, detail):
        if direction in ("LONG", "SHORT"):
            rows.append({"name": name, "direction": direction, "weight": weight, "detail": detail})
    close = _col(df, "close") if df is not None else []
    if len(close) >= 6 and close[-6]:
        move = _pct(close[-1], close[-6])
        if abs(move) >= .15:
            add("Краткосрочная цена", "LONG" if move > 0 else "SHORT", 1.0, f"{move:+.2f}% за 5 интервалов")
    bid = c.get("orderbook_bid_share")
    if bid is not None:
        b = _n(bid, .5)
        if b >= .54: add("Стакан", "LONG", .8, f"bid {b*100:.1f}%")
        elif b <= .46: add("Стакан", "SHORT", .8, f"bid {b*100:.1f}%")
        else: rows.append({"name":"Стакан", "direction":"NEUTRAL", "weight":.3, "detail":f"bid {b*100:.1f}%"})
    funding = c.get("funding")
    if funding is not None:
        f = _n(funding)
        # Crowded longs/shorts are a contrarian risk, not a standalone directional forecast.
        if f >= .0005: add("Funding перегружает лонги", "SHORT", .45, f"{f*100:.4f}%")
        elif f <= -.0005: add("Funding перегружает шорты", "LONG", .45, f"{f*100:.4f}%")
    bias = c.get("news_bias")
    if bias is not None:
        b = _n(bias)
        if abs(b) >= .2: add("Новостной уклон", "LONG" if b > 0 else "SHORT", .6, f"bias {b:+.2f}")
    btc = c.get("btc_change_pct")
    if btc is not None and abs(_n(btc)) >= .25:
        add("Контекст BTC", "LONG" if _n(btc) > 0 else "SHORT", .5, f"{_n(btc):+.2f}%")
    total = sum(x["weight"] for x in rows)
    support = sum(x["weight"] for x in rows if x["direction"] == side)
    oppose = sum(x["weight"] for x in rows if x["direction"] not in (side, "NEUTRAL"))
    denom = support + oppose
    ratio = oppose/denom if denom else 0.0
    return {"status": "ok" if rows else "insufficient_data", "side": side,
            "support_weight": round(support, 2), "opposition_weight": round(oppose, 2),
            "conflict_pct": round(ratio*100, 1) if denom else None,
            "agreement_pct": round(support/denom*100, 1) if denom else None,
            "coverage": len(rows), "evidence": rows,
            "state": "conflict" if ratio >= .45 else "mixed" if ratio >= .25 else "aligned" if rows else "unknown",
            "note": "Источники имеют разную силу и не независимы полностью; карта показывает расхождения, а не торговый приказ."}


def score_calibration(history, min_sample=8):
    """Audit historical score bands against realized closed outcomes; score is not probability."""
    buckets = [(0, 49, "0–49"), (50, 59, "50–59"), (60, 69, "60–69"),
               (70, 79, "70–79"), (80, 100, "80–100")]
    rows = []
    for item in _closed(history):
        dna = item.get("signal_dna") or {}
        score = dna.get("score", item.get("quality_score"))
        if score is None:
            continue
        score = _n(score, -1)
        if not 0 <= score <= 100:
            continue
        rows.append((score, _n(item.get("r"))))
    out = []
    for lo, hi, label in buckets:
        sample = [r for score, r in rows if lo <= score <= hi]
        if not sample:
            continue
        wins = sum(r > 0 for r in sample)
        out.append({"band": label, "trades": len(sample), "win_rate": round(wins/len(sample)*100, 1),
                    "avg_r": round(_mean(sample), 3), "sample_ok": len(sample) >= min_sample})
    return {"status": "ok" if rows else "no_data", "scored_closed_trades": len(rows),
            "bands": out, "min_sample": min_sample,
            "note": "Score 0–100 не является вероятностью. Таблица показывает только фактические результаты закрытых сигналов с сохранённым score."}
