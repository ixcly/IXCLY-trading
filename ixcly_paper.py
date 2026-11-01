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


def open_position(store, uid, coin, side, entry, stop, targets, risk_pct=None, account_id=None, fee_pct=None, slippage_pct=None):
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
    entry_exec = e * (1 + slip / 100 if side == 'LONG' else 1 - slip / 100)
    entry_fee = notional * fee / 100
    acc['balance'] -= entry_fee
    pos = {'coin': coin, 'side': side, 'entry': entry_exec, 'signal_entry': e, 'stop': sl,
           'targets': tps, 'qty': qty, 'notional': notional, 'risk_pct': risk,
           'risk_cash': risk_cash, 'entry_fee': entry_fee, 'fee_pct': fee, 'slippage_pct': slip,
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


def analytics(store, uid, account_id=None):
    """Bounded performance summary for one Paper account; no market/API calls."""
    user, acc = _get_account(store, uid, account_id)
    history = list(acc.get('history') or [])[-MAX_HISTORY_PER_ACCOUNT:]
    pnls = [float(x.get('pnl', 0.0) or 0.0) for x in history]
    wins = [x for x in pnls if x > 1e-9]
    losses = [x for x in pnls if x < -1e-9]
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    equity = float(acc.get('initial_balance', 0.0))
    peak = equity
    max_dd = 0.0
    curve = [round(equity, 2)]
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak * 100)
        curve.append(round(equity, 2))
    n = len(pnls)
    return {
        'account_id': acc.get('id'), 'account_name': acc.get('name'), 'trades': n,
        'wins': len(wins), 'losses': len(losses),
        'breakeven': n - len(wins) - len(losses),
        'win_rate': round(len(wins) / n * 100, 2) if n else 0.0,
        'net_pnl': round(sum(pnls), 2),
        'avg_pnl': round(sum(pnls) / n, 2) if n else 0.0,
        'profit_factor': round(gross_profit / gross_loss, 2) if gross_loss else (None if not gross_profit else '∞'),
        'max_drawdown_pct': round(max_dd, 2), 'equity_curve': curve[-41:],
        'history': history[-10:],
    }
