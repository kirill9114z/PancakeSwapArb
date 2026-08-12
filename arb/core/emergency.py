"""Аварийное закрытие позиции на MEXC, когда DEX-своп (хедж) не прошёл.

Задача модуля - вернуть инвентарь токена к состоянию ДО сделки, приняв убыток в
USDT. Решение о том, ЗАПУСКАТЬ ли закрытие, принимается в arb.core.execution по
типу ошибки свопа; здесь - только механика самого закрытия.
"""
import asyncio
import time

from arb.exchanges.mexc_web import place_limit_order
from config import (
    MEXC_EMERGENCY_BALANCE_SAFETY,
    MEXC_EMERGENCY_DUST_RATIO,
    MEXC_EMERGENCY_FILL_TIMEOUT_SECONDS,
    MEXC_EMERGENCY_MAX_ATTEMPTS,
    MEXC_EMERGENCY_ORDER_CHECK_DELAY_SECONDS,
    MEXC_EMERGENCY_THIN_BOOK_BUFFER_PCT,
    MEXC_ORDER_POLL_INTERVAL_SECONDS,
)

from .orderbook import marketable_price


class EmergencyCloseMixin:
    """Возврат позиции к нейтральной. Подмешивается в Arbitrage."""

    # === Аварийное закрытие позиции на MEXC =========================================
    # Контекст: сделка состоит из двух ног - ордер на MEXC и хедж-своп на DEX. Если
    # первая исполнилась, а вторая нет, инвентарь токена уехал в одну сторону и ничем
    # не захеджирован. Задача этих методов - вернуть инвентарь токена к состоянию ДО
    # сделки, приняв убыток в USDT.
    #
    #   BUY_MEXC  не захеджирован -> на MEXC лишние токены -> ПРОДАЁМ их обратно
    #   SELL_MEXC не захеджирован -> на MEXC не хватает токенов -> ВЫКУПАЕМ их обратно
    #
    # Принципиально: ордер должен СМЕСТИ стакан немедленно, а не встать в очередь.
    # Пассивная лимитка здесь - худший вариант: она может не исполниться, при этом
    # заблокирует баланс, а бот продолжит считать возможности на инвентаре, которого
    # нет. Поэтому цена берётся от последнего уровня стакана, нужного для набора
    # объёма, и сдвигается в невыгодную нам сторону на MEXC_EMERGENCY_PRICE_BUFFER_PCT.


    @staticmethod
    def _extract_order_id(order):
        """Разбор ответа place_limit_order. Возвращает (order_id, причина_отказа).

        Раньше здесь стояло просто `if order: order['data']`, из-за чего два самых
        вероятных отказа проходили проверку и падали в KeyError, который дальше
        глотался общим except в _make_trade_impl:
          - сетевая ошибка -> place_limit_order возвращает {"error": ...}, а это
            ИСТИННОЕ значение;
          - отказ биржи (не хватает баланса, объём ниже минимального) -> приходит
            HTTP 200 с телом без 'data', raise_for_status его не видит.
        Оба случая на пути аварийного закрытия означали незакрытую позицию без
        единого уведомления."""
        if order is False:
            return None, "u_id токен MEXC устарел - обновите его в настройках бота"
        if order is None:
            return None, "MEXC не вернул currencyId для пары"
        if not isinstance(order, dict):
            return None, f"неожиданный ответ MEXC: {order!r}"
        if 'error' in order:
            return None, f"сетевая ошибка при отправке ордера: {order['error']}"
        order_id = order.get('data')
        if not order_id:
            return None, f"MEXC отклонил ордер: {order}"
        return order_id, None

    async def _await_order_result(self, order_id, timeout) -> float:
        """Ждёт исполнения ордера, по таймауту ОТМЕНЯЕТ остаток и возвращает
        фактически исполненный объём.

        Отмена обязательна: неотменённый остаток продолжает висеть в стакане и
        держать заблокированным баланс, а бот тем временем считает, что позиция
        закрыта на столько, сколько увидел при единственной проверке."""
        deadline = time.time() + timeout
        filled = 0.0
        while time.time() < deadline:
            try:
                order_status = await self.exchange.fetch_order(order_id, self.pair)
            except Exception as e:
                print(f"[{self.pair}] аварийный ордер {order_id}: не удалось прочитать статус: {e}")
                await asyncio.sleep(MEXC_ORDER_POLL_INTERVAL_SECONDS)
                continue
            filled = float(order_status.get('filled') or 0)
            if order_status.get('status') in ('closed', 'canceled'):
                return filled
            await asyncio.sleep(MEXC_ORDER_POLL_INTERVAL_SECONDS)

        try:
            await self.exchange.cancel_order(order_id, self.pair)
        except Exception as e:
            print(f"[{self.pair}] не удалось отменить остаток аварийного ордера {order_id}: {e}")
        try:
            order_status = await self.exchange.fetch_order(order_id, self.pair)
            filled = float(order_status.get('filled') or 0)
        except Exception as e:
            print(f"[{self.pair}] не удалось перечитать аварийный ордер {order_id} после отмены: {e}")
        return filled

    async def _emergency_close_position(self, symbol, u_id, session, close_is_sell, qty):
        """Сметает стакан встречными ордерами, пока не вернёт qty токенов.

        Возвращает (закрыто, остаток, список_проблем). Остаток > 0 означает, что
        позиция закрыта НЕ полностью и нужно вмешательство руками."""
        target = float(qty)
        if target <= 0:
            return 0.0, 0.0, []

        dust = target * MEXC_EMERGENCY_DUST_RATIO
        closed_total = 0.0
        problems = []

        for attempt in range(1, MEXC_EMERGENCY_MAX_ATTEMPTS + 1):
            remaining = target - closed_total
            if remaining <= dust:
                break

            asks, bids = await self._fetch_orderbook_raw(session)
            levels = bids if close_is_sell else asks
            if not levels:
                problems.append(f"попытка {attempt}: стакан недоступен или пуст")
                break

            price, enough_depth = marketable_price(levels, remaining, close_is_sell)
            if price is None:
                problems.append(f"попытка {attempt}: не удалось вычислить цену по стакану")
                break
            if not enough_depth:
                problems.append(
                    f"попытка {attempt}: видимой глубины стакана не хватает на {remaining:.8f}, "
                    f"ставлю цену с увеличенным буфером {MEXC_EMERGENCY_THIN_BOOK_BUFFER_PCT}%")

            qty_now = remaining
            if not close_is_sell:
                # ВЫКУП: биржа резервирует цена * количество. Аварийная цена заведомо
                # хуже той, по которой мы продавали, поэтому выручки от продажи на
                # полный выкуп не хватает - нужен свободный запас USDT на аккаунте.
                # Берём именно свежий баланс, а не self.balance_usdt_mexc: тот уже
                # оптимистично уменьшен в _reserve_balances_for_trade.
                usdt_free, _ = await self._safe_fetch_balance()
                budget = usdt_free * MEXC_EMERGENCY_BALANCE_SAFETY
                affordable = budget / price if price > 0 else 0.0
                if affordable < qty_now:
                    problems.append(
                        f"попытка {attempt}: свободного USDT (${usdt_free:.2f}) хватает только на "
                        f"{affordable:.8f} из {qty_now:.8f} токенов по цене {price}")
                    qty_now = affordable
            if qty_now <= dust:
                break

            print(f"[{self.pair}] аварийное закрытие, попытка {attempt}: "
                  f"{'SELL' if close_is_sell else 'BUY'} {qty_now} по {price} "
                  f"(глубины {'хватает' if enough_depth else 'НЕ хватает'})")

            order = await place_limit_order(symbol, price, qty_now, close_is_sell, u_id, session)
            order_id, reason = self._extract_order_id(order)
            if order_id is None:
                problems.append(f"попытка {attempt}: {reason}")
                # Останавливать пару здесь не нужно: любой отказ выставить ордер
                # означает незакрытый остаток, а решение об остановке принимается
                # единообразно в handle_swap по итоговому остатку.
                break

            got = await self._await_order_result(order_id, MEXC_EMERGENCY_FILL_TIMEOUT_SECONDS)
            closed_total += got
            if got <= 0:
                problems.append(f"попытка {attempt}: ордер {order_id} не исполнился вообще")

            if target - closed_total > dust and attempt < MEXC_EMERGENCY_MAX_ATTEMPTS:
                await asyncio.sleep(MEXC_EMERGENCY_ORDER_CHECK_DELAY_SECONDS)

        remaining = max(0.0, target - closed_total)
        return closed_total, remaining, problems
