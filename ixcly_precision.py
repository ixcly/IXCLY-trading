# -*- coding: utf-8 -*-
"""IXCLY Precision Lab: independent, deterministic signal diagnostics.

Uses only observed OHLCV candles. No invented probabilities, order-book data,
exchange connections, or automatic trade blocking. Bounded CPU and memory.
"""
from __future__ import annotations
import math
import numpy as np


def _finite(value):
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _round(value, digits=2):
    return round(float(value), digits) if value is not None and math.isfinite(float(value)) else None


def candle_diagnostics(df, side, window=96):
    """Measure directional efficiency, wick pressure, volume participation,
    volatility shock and recent candle consistency, without look-ahead.
    """
    missing = {'status': 'insufficient_data', 'observations': 0, 'notes': ['Нужны >= 32 корректных свечи OHLCV']}
    if df is None or not all(k in df for k in ('open', 'high', 'low', 'close', 'volume')):
        return missing
    try:
        frame = df[['open', 'high', 'low', 'close', 'volume']].tail(max(32, min(int(window), 256)))
        a = frame.to_numpy(dtype=np.float64, copy=True)
        if len(a) < 32 or not np.isfinite(a).all():
            return missing
        o, h, l, c, v = a.T
        if np.any(c <= 0) or np.any(h < np.maximum(o, c)) or np.any(l > np.minimum(o, c)) or np.any(l <= 0) or np.any(v < 0):
            return {'status': 'invalid_data', 'observations': len(a), 'notes': ['Некорректные OHLCV']}
        side = str(side).upper()
        if side not in ('LONG', 'SHORT'):
            return {'status': 'invalid_side', 'observations': len(a), 'notes': ['Неизвестное направление']}
        direction = 1 if side == 'LONG' else -1
        changes = np.diff(c)
        path = float(np.abs(changes[-24:]).sum())
        efficiency = abs(float(c[-1] - c[-25])) / path if path > 0 else 0.0
        returns = np.diff(np.log(c))
        recent_vol = float(np.std(returns[-12:]))
        baseline_vol = float(np.std(returns[-48:-12]))
        vol_ratio = recent_vol / baseline_vol if baseline_vol > 1e-12 else None
        rng = np.maximum(h - l, 1e-12)
        upper_wick = (h - np.maximum(o, c)) / rng
        lower_wick = (np.minimum(o, c) - l) / rng
        adverse_wick = upper_wick if direction > 0 else lower_wick
        supportive_wick = lower_wick if direction > 0 else upper_wick
        wick_pressure = float(np.mean(adverse_wick[-12:]) - np.mean(supportive_wick[-12:]))
        aligned = float(np.mean(direction * changes[-12:] > 0))
        volume_base = float(np.median(v[-25:-1]))
        volume_ratio = float(v[-1] / volume_base) if volume_base > 0 else None
        high_break = bool(c[-1] > float(np.max(h[-21:-1]))) if direction > 0 else bool(c[-1] < float(np.min(l[-21:-1])))
        distance_pct = float(direction * (c[-1] / c[-13] - 1) * 100)
        notes = []
        if vol_ratio is not None and vol_ratio >= 2.2:
            notes.append('Резкий рост реализованной волатильности')
        if wick_pressure > .18:
            notes.append('Сильные встречные тени свечей')
        if efficiency < .22:
            notes.append('Низкая эффективность тренда: возможен шум/флэт')
        if aligned < .42:
            notes.append('Последние свечи чаще идут против направления')
        if high_break and volume_ratio is not None and volume_ratio < .7:
            notes.append('Пробой без подтверждения объёмом')
        return {'status': 'ok', 'observations': len(a), 'directional_efficiency': _round(efficiency, 3),
                'directional_candle_share': _round(aligned, 3), 'adverse_wick_pressure': _round(wick_pressure, 3),
                'volatility_expansion': _round(vol_ratio), 'relative_volume': _round(volume_ratio),
                'breakout_20': high_break, 'directional_12bar_move_pct': _round(distance_pct), 'notes': notes}
    except (TypeError, ValueError, KeyError, IndexError, OverflowError):
        return {'status': 'invalid_data', 'observations': 0, 'notes': ['Не удалось проверить OHLCV']}


def execution_geometry(side, entry, stop, targets, fee_bps=10.0, slippage_bps=5.0):
    """Compute gross/net R multiples and minimum break-even win rate, not a win forecast."""
    side = str(side).upper()
    entry, stop = _finite(entry), _finite(stop)
    if side not in ('LONG', 'SHORT') or entry is None or stop is None or entry <= 0 or stop <= 0:
        return {'status': 'insufficient_data'}
    direction = 1 if side == 'LONG' else -1
    risk = direction * (entry - stop)
    if risk <= 0:
        return {'status': 'invalid_stop', 'reason': 'Стоп должен находиться на убыточной стороне входа'}
    fee = max(0, _finite(fee_bps) or 0)
    slip = max(0, _finite(slippage_bps) or 0)
    # Fee/slippage apply on both entry and exit; approximate as entry-relative cost.
    round_trip_cost = 2 * (fee + slip) / 10000 * entry
    net_loss = risk + round_trip_cost
    out = []
    for target in list(targets or [])[:3]:
        tp = _finite(target)
        if tp is None or tp <= 0:
            continue
        gain = direction * (tp - entry)
        net_gain = gain - round_trip_cost
        rr = gain / risk
        net_rr = net_gain / net_loss
        breakeven = net_loss / (net_gain + net_loss) if net_gain > 0 else None
        out.append({'target': tp, 'gross_rr': _round(rr), 'net_rr': _round(net_rr),
                    'break_even_win_rate_pct': _round(breakeven * 100) if breakeven is not None else None})
    return {'status': 'ok', 'risk_pct': _round(risk / entry * 100),
            'round_trip_cost_pct': _round(round_trip_cost / entry * 100, 3),
            'targets': out, 'assumption': 'Комиссии и проскальзывание — настраиваемые допущения, не фактическое исполнение'}



def position_risk_plan(equity, entry, stop, risk_budget_pct=0.5, max_notional_pct=25):
    """Paper-only sizing estimate, never executes orders or assumes leverage."""
    equity, entry, stop = _finite(equity), _finite(entry), _finite(stop)
    if any(x is None or x <= 0 for x in (equity, entry, stop)) or entry == stop:
        return {'status': 'insufficient_data'}
    risk_budget_pct = min(5., max(0., _finite(risk_budget_pct) or 0.))
    max_notional_pct = min(100., max(0., _finite(max_notional_pct) or 0.))
    budget = equity * risk_budget_pct / 100
    qty = min(budget / abs(entry-stop), equity * max_notional_pct / 100 / entry)
    return {'status': 'ok', 'quantity': _round(qty, 8),
            'notional': _round(qty * entry), 'stop_loss_budget': _round(qty * abs(entry-stop)),
            'equity_risk_cap_pct': risk_budget_pct, 'notional_cap_pct': max_notional_pct,
            'paper_only': True, 'excludes_gaps_and_funding': True}


def robust_data_health(df):
    """Detect stale/duplicated or suspicious candles without fabricating missing samples."""
    if df is None or not all(k in df for k in ('open','high','low','close','volume')):
        return {'status':'insufficient_data'}
    try:
        a = df[['open','high','low','close','volume']].tail(128).to_numpy(dtype=float)
        if len(a) < 16:
            return {'status':'insufficient_data'}
        finite = np.isfinite(a).all(axis=1)
        invalid = (~finite) | (a[:,1] < np.maximum(a[:,0],a[:,3])) | (a[:,2] > np.minimum(a[:,0],a[:,3])) | (a[:,2] <= 0) | (a[:,4] < 0)
        unchanged = (np.diff(a[:,3]) == 0) & (np.diff(a[:,4]) == 0)
        repeated = int(np.sum(unchanged))
        invalid_count = int(np.sum(invalid))
        notes = []
        if invalid_count:
            notes.append('Некорректные или отсутствующие значения OHLCV')
        if repeated >= 8:
            notes.append('Много повторяющихся свечей цены и объёма')
        return {'status': 'warning' if notes else 'ok', 'observations':len(a),
                'invalid_candles':invalid_count, 'repeated_adjacent':repeated,
                'notes':notes}
    except (ValueError, TypeError, KeyError, IndexError):
        return {'status':'invalid_data'}


def stop_stress_test(side, entry, stop, atr_pct=None, gap_multipliers=(1, 1.5, 2)):
    """Illustrate potential stop overshoot under hypothetical ATR-sized gaps."""
    entry, stop, atr_pct = _finite(entry), _finite(stop), _finite(atr_pct)
    if entry is None or stop is None or atr_pct is None or entry <= 0 or atr_pct <= 0:
        return {'status':'insufficient_data'}
    side = str(side).upper()
    if side not in ('LONG','SHORT') or (side == 'LONG' and stop >= entry) or (side == 'SHORT' and stop <= entry):
        return {'status':'invalid_stop'}
    stop_loss_pct = abs(entry-stop)/entry*100
    cases = [{'gap_atr':float(mult), 'hypothetical_loss_pct':_round(stop_loss_pct + float(mult)*atr_pct)}
             for mult in list(gap_multipliers)[:5] if _finite(mult) is not None and float(mult) >= 0]
    return {'status':'ok', 'planned_stop_pct':_round(stop_loss_pct), 'stress_cases':cases,
            'note':'Сценарии стресс-теста, не прогноз распределения гэпов'}

def precision_report(df, signal, fee_bps=10.0, slippage_bps=5.0):
    """Compact, JSON-safe report; informational only, never blocks signal publication."""
    side = str(signal.get('side') or ('LONG' if signal.get('is_long') else 'SHORT')).upper()
    candles = candle_diagnostics(df, side)
    geometry = execution_geometry(side, signal.get('entry'), (signal.get('sls') or [None])[0],
                                  signal.get('tps') or [], fee_bps, slippage_bps)
    health = robust_data_health(df)
    flags = list(candles.get('notes') or []) + list(health.get('notes') or [])
    if geometry.get('status') == 'ok':
        t = geometry.get('targets') or []
        if t and t[0]['net_rr'] is not None and t[0]['net_rr'] < 1:
            flags.append('Первый TP даёт менее 1R после расчётных издержек')
    elif geometry.get('status') == 'invalid_stop':
        flags.append('Неверная геометрия защитного стопа')
    return {'version': 1, 'status': 'ok' if candles.get('status') == 'ok' else 'partial',
            'candles': candles, 'execution': geometry, 'data_health': health, 'warnings': flags[:8],
            'is_probability': False, 'advisory_only': True}
