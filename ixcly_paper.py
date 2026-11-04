# -*- coding: utf-8 -*-
"""IXCLY MAX 2.0 Paper Trading engine. Standard-library only; no exchange order execution."""
from __future__ import annotations
import copy, time, uuid

MAX_ACCOUNTS_PER_USER = 5
MAX_HISTORY_PER_ACCOUNT = 500
DEFAULT_DEPOSIT = 1000.0


def _num(value, name, minimum=0.0, maximum=1e12):
    try:
        n = float(str(value).replace(',', '.'))
    except (TypeError, ValueError):
        raise ValueError(f"Некорректное значение: {name}")
    if not (minimum <= n <= maximum):
        raise ValueError(f"Недопустимое значение: {name}")
    return n


def _account(uid, name='Основной', deposit=DEFAULT_DEPOSIT, primary=False):
    now = time.time()
    return {
        'id': 'main' if primary else uuid.uuid4().hex[:10], 'name': str(name)[:32],
        'initial_balance': float(deposit), 'balance': float(deposit), 'equity': float(deposit),
        'realized_pnl': 0.0, 'unrealized_pnl': 0.0, 'peak_equity': float(deposit),
        'max_drawdown_pct': 0.0, 'risk_pct': 1.0, 'fee_pct': 0.1, 'slippage_pct': 0.02,
        'positions': {}, 'history': [], 'created_at': now, 'updated_at': now,
        'primary': bool(primary), 'owner': str(uid),
    }


def ensure_user(store, uid):
    key = str(uid)
    user = store.setdefault(key, {'accounts': [], 'active_account': 'main'})
    accounts = user.setdefault('accounts', [])
    if not any(a.get('id') == 'main' for a in accounts):
        accounts.insert(0, _account(uid, primary=True))
    if not any(a.get('id') == user.get('active_account') for a in accounts):
        user['active_account'] = 'main'
    return user


def _get_account(store, uid, account_id=None):
    user = ensure_user(store, uid)
    aid = str(account_id or user.get('active_account') or 'main')
    acc = next((a for a in user['accounts'] if a.get('id') == aid), None)
    if not acc:
        raise ValueError('Виртуальный счёт не найден')
    return user, acc


def snapshot(store, uid, prices=None):
    prices = prices or {}
    user = ensure_user(store, uid)
    out = []
    for acc in user['accounts']:
        item = copy.deepcopy(acc)
        unrealized = 0.0
        for coin, p in item.get('positions', {}).items():
            px = prices.get(coin)
            if px and float(px) > 0:
                direction = 1 if p['side'] == 'LONG' else -1
                unrealized += (float(px) - p['entry']) * direction * p['qty']
                p['mark_price'] = float(px)
                p['pnl_pct'] = round(direction * (float(px) - p['entry']) / p['entry'] * 100, 3)
            else:
                unrealized += float(p.get('unrealized_pnl', 0.0))
        item['unrealized_pnl'] = round(unrealized, 8)
        item['equity'] = round(float(item['balance']) + unrealized, 8)
        item['open_count'] = len(item.get('positions', {}))
        item['history'] = item.get('history', [])[-20:]
        out.append(item)
    active = next((a for a in out if a['id'] == user.get('active_account')), out[0])
    return {'accounts': out, 'active': active, 'active_account': user.get('active_account', 'main')}


def create_account(store, uid, name, deposit, kind='strategy'):
    user = ensure_user(store, uid)
    if len(user['accounts']) >= MAX_ACCOUNTS_PER_USER:
        raise ValueError(f'Максимум {MAX_ACCOUNTS_PER_USER} виртуальных счетов')
    amount = _num(deposit, 'депозит', 10, 1e9)
    clean_name = str(name or 'Тестовая стратегия').strip()[:32] or 'Тестовая стратегия'
    acc = _account(uid, clean_name, amount, primary=False)
    acc['kind'] = 'strategy' if kind == 'strategy' else 'test'
    user['accounts'].append(acc)
    user['active_account'] = acc['id']
    return copy.deepcopy(acc)


def select_account(store, uid, account_id):
    user = ensure_user(store, uid)
    if not any(a.get('id') == str(account_id) for a in user['accounts']):
        raise ValueError('Виртуальный счёт не найден')
    user['active_account'] = str(account_id)
    return str(account_id)


def open_position(store, uid, coin, side, entry, stop, targets, risk_pct=None, account_id=None, fee_pct=None, slippage_pct=None, execution_context=None):
    user, acc = _get_account(store, uid, account_id)
    coin = str(coin).upper().strip()
    side = str(side).upper().strip()
    if not coin or len(coin) > 20 or side not in ('LONG', 'SHORT'):
        raise ValueError('Некорректная монета или направление')
    if coin in acc['positions']:
        raise ValueError(f'{coin}: позиция уже открыта на этом счёте')
    e = _num(entry, 'вход', 1e-12, 1e12)
    sl = _num(stop, 'Stop Loss', 1e-12, 1e12)
    if (side == 'LONG' and sl >= e) or (side == 'SHORT' and sl <= e):
        raise ValueError('Stop Loss должен находиться за ценой входа')
    tps = [_num(x, 'Take Profit', 1e-12, 1e12) for x in (targets or [])[:3]]
    if not tps:
        raise ValueError('Нужен хотя бы один Take Profit')
    if (side == 'LONG' and any(x <= e for x in tps)) or (side == 'SHORT' and any(x >= e for x in tps)):
        raise ValueError('Take Profit должен быть по направлению сделки')
    risk = _num(risk_pct if risk_pct is not None else acc.get('risk_pct', 1), 'риск', 0.1, 5.0)
    fee = _num(fee_pct if fee_pct is not None else acc.get('fee_pct', .1), 'комиссия', 0, 2)
    slip = _num(slippage_pct if slippage_pct is not None else acc.get('slippage_pct', .02), 'проскальзывание', 0, 2)
    risk_cash = float(acc['equity']) * risk / 100
    stop_distance = abs(e - sl)
    qty = risk_cash / stop_distance
    notional = qty * e
    if notional > float(acc['equity']) * 10:
        raise ValueError('Слишком большой размер позиции относительно виртуального капитала')
    fill_report = None
    if isinstance(execution_context, dict):
        fill_report = simulate_order_execution(
            side, qty, e, order_type='LIMIT', maker_fee_pct=fee, taker_fee_pct=fee,
            spread_pct=execution_context.get('spread_pct'),
            depth_notional=execution_context.get('depth_notional'),
            latency_ms=execution_context.get('latency_ms'),
            available_notional=execution_context.get('depth_notional'),
            max_participation=execution_context.get('max_participation', .10))
        if fill_report.get('filled_qty', 0) <= 0:
            raise ValueError('Недостаточно видимой ликвидности для виртуального исполнения')
        qty = float(fill_report['filled_qty'])
        notional = qty * e
        entry_exec = float(fill_report['execution_price']) * (1 + slip / 100 if side == 'LONG' else 1 - slip / 100)
    else:
        entry_exec = e * (1 + slip / 100 if side == 'LONG' else 1 - slip / 100)
    entry_fee = abs(qty * entry_exec) * fee / 100
    acc['balance'] -= entry_fee
    pos = {'coin': coin, 'side': side, 'entry': entry_exec, 'signal_entry': e, 'stop': sl,
           'targets': tps, 'qty': qty, 'notional': notional, 'risk_pct': risk,
           'risk_cash': risk_cash, 'entry_fee': entry_fee, 'fee_pct': fee, 'slippage_pct': slip,
           'fill_status': (fill_report or {}).get('status', 'assumed_full'),
           'fill_ratio_pct': (fill_report or {}).get('fill_ratio_pct', 100.0),
           'remaining_qty': (fill_report or {}).get('remaining_qty', 0.0),
           'execution_model': fill_report or {'status':'legacy_simplified','note':'No live order-book context supplied'},
           'opened_at': time.time(), 'account_id': acc['id'], 'unrealized_pnl': 0.0}
    acc['positions'][coin] = pos
    acc['updated_at'] = time.time()
    return copy.deepcopy(pos)


def close_position(store, uid, coin, exit_price, account_id=None, reason='manual'):
    user, acc = _get_account(store, uid, account_id)
    coin = str(coin).upper().strip()
    pos = acc['positions'].get(coin)
    if not pos:
        raise ValueError('Открытая Paper-позиция не найдена')
    px = _num(exit_price, 'цена выхода', 1e-12, 1e12)
    side_sign = 1 if pos['side'] == 'LONG' else -1
    exit_exec = px * (1 - pos['slippage_pct'] / 100 if pos['side'] == 'LONG' else 1 + pos['slippage_pct'] / 100)
    gross = (exit_exec - pos['entry']) * side_sign * pos['qty']
    exit_fee = abs(exit_exec * pos['qty']) * pos['fee_pct'] / 100
    net = gross - exit_fee
    acc['balance'] += net
    rec = {'coin': coin, 'side': pos['side'], 'entry': pos['entry'], 'exit': exit_exec,
           'qty': pos['qty'], 'gross_pnl': gross, 'fees': pos['entry_fee'] + exit_fee,
           'pnl': net - pos['entry_fee'], 'pnl_pct_equity': (net - pos['entry_fee']) / max(1e-9, acc['initial_balance']) * 100,
           'reason': str(reason)[:32], 'opened_at': pos['opened_at'], 'closed_at': time.time(), 'account_id': acc['id']}
    acc['history'].append(rec)
    if len(acc['history']) > MAX_HISTORY_PER_ACCOUNT:
        del acc['history'][:-MAX_HISTORY_PER_ACCOUNT]
    del acc['positions'][coin]
    acc['realized_pnl'] = round(float(acc['balance']) - float(acc['initial_balance']), 8)
    acc['updated_at'] = time.time()
    return copy.deepcopy(rec)


def set_settings(store, uid, risk_pct=None, fee_pct=None, slippage_pct=None, account_id=None):
    _, acc = _get_account(store, uid, account_id)
    if risk_pct is not None: acc['risk_pct'] = _num(risk_pct, 'риск', .1, 5)
    if fee_pct is not None: acc['fee_pct'] = _num(fee_pct, 'комиссия', 0, 2)
    if slippage_pct is not None: acc['slippage_pct'] = _num(slippage_pct, 'проскальзывание', 0, 2)
    acc['updated_at'] = time.time()
    return copy.deepcopy(acc)


def reset_account(store, uid, account_id=None, deposit=None):
    user, acc = _get_account(store, uid, account_id)
    if acc.get('positions'):
        raise ValueError('Сначала закройте все открытые позиции')
    amount = _num(deposit if deposit is not None else acc['initial_balance'], 'депозит', 10, 1e9)
    fresh = _account(uid, acc['name'], amount, primary=bool(acc.get('primary')))
    fresh.update({k: acc[k] for k in ('id', 'kind', 'risk_pct', 'fee_pct', 'slippage_pct') if k in acc})
    idx = user['accounts'].index(acc)
    user['accounts'][idx] = fresh
    return copy.deepcopy(fresh)


def simulate_order_execution(side, quantity, reference_price, order_type='LIMIT',
                             maker_fee_pct=0.02, taker_fee_pct=0.06,
                             spread_pct=None, depth_notional=None, latency_ms=None,
                             available_notional=None, max_participation=0.10):
    """Conservative paper fill model. Missing book/latency inputs remain explicitly unknown."""
    side = str(side).upper()
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Некорректная сторона исполнения')
    qty = _num(quantity, 'количество', 1e-12, 1e15)
    px = _num(reference_price, 'цена', 1e-12, 1e15)
    maker = str(order_type).upper() == 'LIMIT'
    fee = _num(maker_fee_pct if maker else taker_fee_pct, 'комиссия', 0, 2)
    if spread_pct is not None:
        spread = _num(spread_pct, 'спред', 0, 20)
    else:
        spread = None
    notional = qty * px
    fill_ratio = 1.0
    if available_notional is not None:
        available = _num(available_notional, 'доступная ликвидность', 0, 1e18)
        fill_ratio = min(1.0, available / max(notional, 1e-12) * max(0.01, min(1.0, max_participation)))
    filled_qty = qty * fill_ratio
    if filled_qty <= 0:
        return {'status': 'unfilled', 'filled_qty': 0.0, 'remaining_qty': qty, 'fee_pct': fee,
                'spread_pct': spread, 'latency_ms': latency_ms, 'data_quality': 'partial_inputs'}
    # Half-spread plus conservative depth impact. Do not invent a latency penalty without measured latency.
    impact_pct = 0.0
    if depth_notional is not None:
        depth = _num(depth_notional, 'глубина стакана', 1e-12, 1e18)
        impact_pct = min(1.0, (filled_qty * px / depth) ** 0.5 * 0.10)
    cost_pct = (spread / 2 if spread is not None else 0.0) + impact_pct
    if latency_ms is not None:
        latency_ms = _num(latency_ms, 'задержка', 0, 600000)
    # Conservative adverse direction: buy pays up, sell receives less.
    direction = 1 if side == 'LONG' else -1
    exec_px = px * (1 + direction * cost_pct / 100)
    return {'status': 'filled' if fill_ratio >= .999 else 'partial', 'requested_qty': qty,
            'filled_qty': filled_qty, 'remaining_qty': max(0.0, qty-filled_qty),
            'fill_ratio_pct': round(fill_ratio*100, 2), 'reference_price': px,
            'execution_price': exec_px, 'fee_pct': fee, 'estimated_fee': filled_qty*exec_px*fee/100,
            'spread_pct': spread, 'impact_pct': round(impact_pct, 5), 'latency_ms': latency_ms,
            'data_quality': 'measured_inputs' if spread is not None and depth_notional is not None and latency_ms is not None else 'partial_inputs',
            'note': 'Paper simulation only; not a confirmed exchange fill.'}


def conservative_candle_exit(side, stop, target, candle_high, candle_low):
    """If stop and target are both touched in one OHLC candle, assume stop first."""
    side = str(side).upper()
    if side == 'LONG':
        stop_hit = candle_low <= stop
        target_hit = candle_high >= target
    elif side == 'SHORT':
        stop_hit = candle_high >= stop
        target_hit = candle_low <= target
    else:
        raise ValueError('Некорректная сторона')
    if stop_hit:
        return {'status': 'stop', 'price': float(stop), 'ambiguous': bool(target_hit), 'conservative': True}
    if target_hit:
        return {'status': 'target', 'price': float(target), 'ambiguous': False, 'conservative': True}
    return {'status': 'open', 'price': None, 'ambiguous': False, 'conservative': True}
