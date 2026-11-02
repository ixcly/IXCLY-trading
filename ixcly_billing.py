# -*- coding: utf-8 -*-
"""IXCLY billing: заказы Telegram Stars, проверка платежей, защита от повторов, статусы доступа.

Модуль не зависит от aiogram/Flask и не пишет на диск: он работает с переданным словарём STATE
(под STATE_LOCK вызывающего кода). Так логику денег можно проверить unit-тестами без бота.

Модель:
  заказ (order)   — «пользователь U хочет тариф D дней за S Stars», создаётся ДО открытия счёта;
  платёж (payment) — факт успешной оплаты, ключ — telegram_payment_charge_id (уникален в Telegram).
Счёт Telegram содержит только payload "ord:<order_id>": пользователь, тариф и сумма берутся из
серверного заказа, а не из данных клиента.
"""
import secrets
import time

RISK_TITLE = "Предупреждение о рисках"
RISK_TEXT = ("Торговля криптовалютами сопряжена с высоким уровнем риска. Сигналы и аналитика IXCLY "
             "предоставляются исключительно в информационных целях и не гарантируют доходность. "
             "Любая сделка может привести к существенным финансовым потерям. Все решения вы "
             "принимаете самостоятельно и на свой риск.")

CURRENCY = "XTR"
ORDER_TTL = 3600            # незавершённый заказ живёт 1 час (сохраняется, чтобы оплату можно было доделать)
MAX_PENDING_PER_USER = 5    # защита от накопления «мусорных» заказов
MAX_ORDERS = 3000           # сколько заказов хранить (старые завершённые вытесняются)
MAX_PAYMENTS = 6000         # сколько платежей хранить для дедупликации/аудита
LIFETIME_TS = 9000000000    # access.until выше этого значения — бессрочный доступ

# Понятные сообщения пользователю (ключ — код проверки)
ERROR_TEXT = {
    "bad_currency": "Неверная валюта платежа. Подписка оплачивается только в Telegram Stars (⭐).",
    "unknown_order": "Заказ не найден или устарел. Откройте раздел «Подписка» и выберите тариф заново.",
    "wrong_user": "Этот счёт создан для другого аккаунта. Откройте «Подписка» в своём аккаунте и выберите тариф.",
    "amount_mismatch": "Сумма платежа не совпадает с заказом. Платёж не выполнен — создайте новый счёт.",
    "bad_plan": "Такого тарифа нет. Откройте раздел «Подписка» и выберите тариф заново.",
    "already_paid": "Этот заказ уже оплачен. Подписка активна — откройте раздел «Подписка», чтобы проверить срок.",
    "expired": "Срок действия счёта истёк. Откройте раздел «Подписка» и создайте новый счёт.",
    "lifetime": "У вас бессрочный доступ — оплата не требуется.",
}


def _stores(state):
    return state.setdefault("orders", {}), state.setdefault("payments", {})


def _now(now):
    return time.time() if now is None else float(now)


def is_lifetime(access):
    a = access or {}
    return bool(a.get("free")) or float(a.get("until", 0) or 0) > LIFETIME_TS


# ---------------------------------------------------------------- заказы
def _trim(state, now):
    """Помечает просроченные заказы и ограничивает размер хранилища (старые завершённые удаляются первыми)."""
    orders, payments = _stores(state)
    for o in orders.values():
        if o.get("status") == "pending" and now > float(o.get("expires", 0)):
            o["status"] = "expired"
    if len(orders) > MAX_ORDERS:
        # сначала вытесняем завершённые/просроченные самые старые; оплаченные — в последнюю очередь
        order_rank = {"expired": 0, "paid": 1, "pending": 2}
        victims = sorted(orders.items(), key=lambda kv: (order_rank.get(kv[1].get("status"), 0), kv[1].get("created", 0)))
        for oid, _ in victims[:len(orders) - MAX_ORDERS]:
            orders.pop(oid, None)
    if len(payments) > MAX_PAYMENTS:
        old = sorted(payments.items(), key=lambda kv: kv[1].get("ts", 0))
        for cid, _ in old[:len(payments) - MAX_PAYMENTS]:
            payments.pop(cid, None)


def create_order(state, uid, days, stars, plans, source="app", now=None):
    """Создаёт (или переиспользует свежий) незавершённый заказ. Возвращает словарь заказа."""
    now = _now(now)
    days, stars, uid = int(days), int(stars), int(uid)
    if days not in plans:
        raise ValueError("bad_plan")
    # цена не может быть выше базовой и не меньше 1 Star
    if stars < 1 or stars > int(plans[days]):
        raise ValueError("bad_amount")
    orders, _ = _stores(state)
    _trim(state, now)
    mine = [o for o in orders.values() if o.get("uid") == uid and o.get("status") == "pending"]
    for o in mine:
        if o["days"] == days and o["stars"] == stars:
            return o                       # тот же тариф и цена — не плодим заказы
    if len(mine) >= MAX_PENDING_PER_USER:
        oldest = min(mine, key=lambda o: o.get("created", 0))
        oldest["status"] = "expired"
    oid = secrets.token_hex(6)
    while oid in orders:
        oid = secrets.token_hex(6)
    order = {"id": oid, "uid": uid, "days": days, "stars": stars, "status": "pending",
             "source": source, "created": now, "expires": now + ORDER_TTL}
    orders[oid] = order
    return order


def payload_for(order):
    return f"ord:{order['id']}"


def parse_payload(payload):
    """Возвращает ('ord', order_id) | ('legacy', days, stars) | (None,)."""
    parts = str(payload or "").split(":")
    try:
        if parts[0] == "ord" and len(parts) == 2 and parts[1]:
            return ("ord", parts[1])
        if parts[0] == "plan" and len(parts) in (2, 3):
            days = int(parts[1])
            stars = int(parts[2]) if len(parts) == 3 else None
            return ("legacy", days, stars)
    except (ValueError, IndexError):
        pass
    return (None,)


# ---------------------------------------------------------------- проверка
def validate_payment(state, order_id, payer_uid, currency, amount, plans, now=None, final=False):
    """Проверяет платёж. Возвращает (ok, code, order).

    final=False — стадия pre_checkout (до списания): просроченный заказ отклоняется.
    final=True  — деньги уже списаны: заказ, истёкший между подтверждением и доставкой, всё равно
                  засчитывается (пользователь заплатил за реальный заказ), но чужой заказ, неверная сумма
                  или уже оплаченный заказ — нет.
    """
    now = _now(now)
    if currency != CURRENCY:
        return False, "bad_currency", None
    orders, _ = _stores(state)
    order = orders.get(str(order_id))
    if not order:
        return False, "unknown_order", None
    if int(order.get("uid", 0)) != int(payer_uid):
        return False, "wrong_user", order
    if order.get("days") not in plans:
        return False, "bad_plan", order
    if int(amount) != int(order.get("stars", -1)) or int(amount) > int(plans[order["days"]]):
        return False, "amount_mismatch", order
    status = order.get("status")
    if status == "paid":
        return False, "already_paid", order
    if not final and (status == "expired" or now > float(order.get("expires", 0))):
        return False, "expired", order
    return True, "ok", order


def settle_payment(state, payload, payer_uid, charge_id, currency, amount, plans, now=None):
    """Фиксирует успешный платёж ровно один раз.

    Результат: {"status": ...} где status:
      ok              — платёж принят, вызывающий код начисляет дни (order["days"]);
      duplicate       — этот charge_id уже обработан (повторная доставка), ничего не делаем;
      duplicate_order — заказ уже был оплачен другим платежом: деньги списаны повторно → нужен возврат;
      rejected        — платёж не подходит под заказ (code), деньги списаны → возврат/разбор вручную.
    """
    now = _now(now)
    orders, payments = _stores(state)
    charge_id = str(charge_id or "")
    if not charge_id:
        return {"status": "rejected", "code": "unknown_order", "order": None}
    if charge_id in payments:
        return {"status": "duplicate", "payment": payments[charge_id], "order": orders.get(payments[charge_id].get("order_id"))}

    kind = parse_payload(payload)
    if kind[0] == "ord":
        ok, code, order = validate_payment(state, kind[1], payer_uid, currency, amount, plans, now, final=True)
    elif kind[0] == "legacy":
        # Старые счета (до заказов): уникальность даёт charge_id; пользователь — тот, кто заплатил.
        days, stars = kind[1], kind[2]
        if currency != CURRENCY:
            ok, code, order = False, "bad_currency", None
        elif days not in plans or int(amount) < 1 or int(amount) > int(plans[days]) or (stars is not None and stars != int(amount)):
            ok, code, order = False, "amount_mismatch", None
        else:
            oid = "legacy-" + secrets.token_hex(5)
            order = {"id": oid, "uid": int(payer_uid), "days": days, "stars": int(amount), "status": "pending",
                     "source": "legacy", "created": now, "expires": now}
            orders[oid] = order
            ok, code = True, "ok"
    else:
        ok, code, order = False, "unknown_order", None

    rec = {"charge_id": charge_id, "order_id": (order or {}).get("id"), "uid": int(payer_uid),
           "days": (order or {}).get("days"), "stars": int(amount), "ts": now}
    if ok:
        order["status"] = "paid"
        order["paid_at"] = now
        order["charge_id"] = charge_id
        rec["status"] = "granted"
        payments[charge_id] = rec
        _trim(state, now)
        return {"status": "ok", "order": order, "payment": rec}
    rec["status"] = "duplicate_order" if code == "already_paid" else "rejected"
    rec["reason"] = code
    payments[charge_id] = rec
    _trim(state, now)
    return {"status": "duplicate_order" if code == "already_paid" else "rejected", "code": code, "order": order, "payment": rec}


def order_view(state, uid, order_id):
    """Безопасное представление заказа для Mini App (только владельцу)."""
    orders, _ = _stores(state)
    o = orders.get(str(order_id))
    if not o or int(o.get("uid", 0)) != int(uid):
        return None
    status = o.get("status")
    if status == "pending" and time.time() > float(o.get("expires", 0)):
        status = "expired"
    return {"id": o["id"], "status": status, "days": o.get("days"), "stars": o.get("stars")}


# ---------------------------------------------------------------- статус доступа
def access_snapshot(access, now=None, is_admin=False):
    """Единое описание статуса доступа: код, понятная подпись, срок, можно ли оплатить."""
    now = _now(now)
    a = access or {}
    until = float(a.get("until", 0) or 0)
    kind = a.get("kind")
    paid_ever = bool(a.get("paid_ever"))
    if is_admin:
        return {"code": "admin", "label": "Администратор", "active": True, "lifetime": True,
                "until": 0, "days_left": 9999, "hours_left": 0, "can_buy": True,
                "hint": "Полный доступ. Оплата доступна для проверки."}
    if is_lifetime(a):
        return {"code": "lifetime", "label": "Бессрочный доступ", "active": True, "lifetime": True,
                "until": 0, "days_left": 9999, "hours_left": 0, "can_buy": False,
                "hint": "Оплата не требуется."}
    left = max(0.0, until - now)
    days_left = int(-(-left // 86400))
    hours_left = int(left // 3600) % 24
    if now < until:
        if kind == "trial" and not paid_ever:
            return {"code": "trial", "label": "Пробный период", "active": True, "lifetime": False,
                    "until": until, "days_left": days_left, "hours_left": hours_left, "can_buy": True,
                    "hint": "Оформите подписку заранее — оставшиеся дни пробного периода сохранятся."}
        return {"code": "active", "label": "Подписка активна", "active": True, "lifetime": False,
                "until": until, "days_left": days_left, "hours_left": hours_left, "can_buy": True,
                "hint": "Новый тариф продлевает текущий срок, дни не теряются."}
    label = ("Подписка закончилась" if paid_ever else
             "Пробный период закончился" if kind == "trial" else "Доступ закрыт")
    return {"code": "expired", "label": label, "active": False, "lifetime": False,
            "until": until, "days_left": 0, "hours_left": 0, "can_buy": True,
            "hint": "Выберите тариф, чтобы вернуть сигналы и сопровождение сделок."}


def new_expiry(access, days, now=None):
    """Дата окончания после покупки days дней (для показа ДО оплаты)."""
    now = _now(now)
    a = access or {}
    if is_lifetime(a):
        return None
    return max(now, float(a.get("until", 0) or 0)) + int(days) * 86400
