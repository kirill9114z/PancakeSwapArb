"""Исполнение сделки: ордер на MEXC + хеджирующий своп на DEX.

Самая опасная часть бота: между исполнением ноги на MEXC и подтверждением свопа
позиция ничем не захеджирована. Отсюда все страховки - сериализация сделок
(_trade_lock), резервирование баланса до отправки, безусловная синхронизация
балансов после, разбор исхода свопа строго по SwapResult и запуск аварийного
закрытия там, где своп точно не прошёл.
"""
import asyncio
import time

from arb.dex.types import SwapErrorType
from arb.exchanges.mexc_web import place_limit_order
from config import (
    MEXC_EMERGENCY_DUST_RATIO,
    MEXC_ORDER_FILL_TIMEOUT_SECONDS,
    MEXC_ORDER_POLL_INTERVAL_SECONDS,
    USDT_CONTRACT,
)


class TradeExecutionMixin:
    """Жизненный цикл одной сделки. Подмешивается в Arbitrage."""

    async def _fetch_status(self, order_id):
        """fetch_order + запоминание исполненного объёма. Нужно, чтобы страховка в
        _make_trade_impl знала, сколько уже исполнено на MEXC, даже если следующий же
        сетевой вызов упадёт с исключением."""
        status = await self.exchange.fetch_order(order_id, self.pair)
        try:
            self._mexc_filled = float(status.get('filled') or 0)
        except (TypeError, ValueError, AttributeError):
            pass
        return status

    async def handle_swap(self, val, status, best, symbol, u_id, session):
        """Разбор результата DEX-свопа (хеджа) после того, как нога на MEXC уже исполнена.

        ВАЖНО про историю: раньше здесь стояло `val.error_type in [val.FATAL_NO_GAS, ...]`,
        но val - это SwapResult, а константы FATAL_* лежат в SwapErrorType, который в этот
        модуль вообще не импортировался. То есть на КАЖДОЙ неудаче свопа тут вылетал
        AttributeError, который молча съедался общим `except Exception` в _make_trade_impl.
        Результат: аварийное закрытие позиции обратно на MEXC не срабатывало НИКОГДА -
        позиция оставалась незахеджированной, а в логи падала одна строка про
        'SwapResult object has no attribute FATAL_NO_GAS'.

        Теперь разбор строится не на списке "фатальных" типов (любой пропущенный тип
        означал бы молчаливое оставление открытой позиции), а на трёх исходах:
          success             -> уведомление;
          FATAL_UNKNOWN_STATUS -> транзакция отправлена, итог НЕИЗВЕСТЕН: закрывать
                                  позицию нельзя, иначе при последующем исполнении свопа
                                  мы получим вторую позицию вместо закрытия первой;
          любой другой провал  -> своп точно не прошёл, закрываем позицию на MEXC.
        """
        # Отмечаем сразу на входе: дальше исход свопа разбирается здесь, и страховке
        # в _make_trade_impl вмешиваться уже не нужно - иначе она могла бы запустить
        # второй, дублирующий откат поверх этого.
        self._hedge_settled = True

        if val.success:
            direction = 'SELL_MX -> BUY_PNK' if best['type'] == 'SELL_MEXC' else 'SELL_PNK -> BUY_MX'
            print(f'{direction} | TOKEN: {self.pair} | ХЭШ: {val.tx_hash}')
            await self.send_notification(
                f"🔔 Новая сделка! {self.pair}\n"
                f"Тип: {'Продажа' if best['type'] == 'SELL_MEXC' else 'Покупка'}\n"
                f"Объем: {status['filled']:.2f}\n"
                f"Прибыль: ${best['profit']:.2f}\n"
                f"Хэш: {val.tx_hash}"
            )
            await self.update_balances()
            return

        print(f"❌ DEX swap FAILED: {val.error_type.value} - {val.error_msg}")

        if val.error_type == SwapErrorType.FATAL_UNKNOWN_STATUS:
            # Единственный исход, при котором автозакрытие ЗАПРЕЩЕНО: своп мог и пройти.
            await self.send_notification(
                f"‼️ PAIR: {self.pair}\n"
                f"Статус DEX-свопа НЕИЗВЕСТЕН: {val.error_msg}\n"
                f"Хэш: {val.tx_hash}\n"
                f"Позиция на MEXC ({status['filled']}) НЕ закрывается автоматически - "
                f"обратная сделка при успешном свопе открыла бы вторую позицию вместо закрытия.\n"
                f"Проверьте транзакцию вручную и закройте позицию сами."
            )
            await self.update_balances()
            return

        # Своп точно не исполнился -> нога на MEXC висит незахеджированной, закрываем её.
        # Объём закрытия - всегда РЕАЛЬНО исполненный на MEXC (status['filled']), а не
        # запрошенный best['volume']: при частичном исполнении они не совпадают, и
        # закрытие по плановому объёму либо отклонится по балансу, либо зацепит
        # посторонний инвентарь. Раньше ветка BUY_MEXC ошибочно использовала volume.
        filled = float(status['filled'])
        # BUY_MEXC: купили токены на MEXC, хедж не продал их на DEX -> продаём на MEXC.
        # SELL_MEXC: продали токены на MEXC, хедж не выкупил их на DEX -> выкупаем на MEXC.
        close_is_sell = best['type'] == 'BUY_MEXC'
        action_text = "обратную продажу" if close_is_sell else "обратный выкуп"

        # Уведомление ДО выставления ордера: если place_limit_order упрётся в таймаут
        # или бросит исключение, сообщение о начале отката всё равно уже ушло.
        await self.send_notification(
            f"❌ PAIR: {self.pair}\n"
            f"⚠️ DEX swap не удался ({val.error_type.value})\n"
            f"Выполняю {action_text} на {filled} токен по стакану для закрытия позиции..."
        )

        try:
            closed, remaining, problems = await self._emergency_close_position(
                symbol, u_id, session, close_is_sell, filled)
        except Exception as e:
            # Путь отката не имеет права падать молча в общий except _make_trade_impl -
            # именно так позиция и оставалась открытой без единого уведомления.
            print(f"[{self.pair}] _emergency_close_position упал: {e}")
            await self.send_notification(
                f"‼️ КРИТИЧНО! Закрытие позиции на MEXC упало с ошибкой: {e}\n"
                f"Пара: {self.pair}\n"
                f"Незакрытая позиция ~{filled} токен - закройте вручную."
            )
            await self.update_balances()
            # Состояние позиции неизвестно - продолжать торговать нельзя.
            await self._stop_pair(f"Аварийное закрытие позиции упало с ошибкой: {e}")
            return

        returned_pct = (closed / filled * 100) if filled else 0.0
        verb = 'продали' if close_is_sell else 'выкупили'
        report = (f"Аварийное закрытие {self.pair}: {verb} {closed:.8f} из {filled:.8f}\n"
                  f"Возвращено: {returned_pct:.2f}%")
        if problems:
            report += "\n\nЗамечания:\n" + "\n".join(f"• {p}" for p in problems)

        if remaining <= filled * MEXC_EMERGENCY_DUST_RATIO:
            # Позиция восстановлена - инвентарь снова нейтрален, торгуем дальше.
            await self.send_notification(report)
            await self.update_balances()
            return

        # Остаток не закрыт. Дальше торговать НЕЛЬЗЯ, и дело не в самом остатке:
        # маркетабельный ордер с буфером MEXC_EMERGENCY_PRICE_BUFFER_PCT обязан
        # исполниться сразу, поэтому его неисполнение почти всегда означает
        # структурную причину (торги по паре остановлены, делистинг, нарушение шага
        # цены или минимального объёма, отвалившийся доступ к API) - такое не
        # чинится повторами. При этом нейтральность позиции нарушена, а нигде
        # больше она не отслеживается: следующая же итерация analyze_opportunities
        # спокойно откроет новую сделку поверх непокрытого риска, и экспозиция
        # начнёт складываться. Поэтому останавливаем пару целиком.
        await self.send_notification(
            "‼️ ПОЗИЦИЯ ЗАКРЫТА НЕ ПОЛНОСТЬЮ\n" + report +
            f"\n\nНезакрытый остаток: {remaining:.8f} токен - закройте вручную."
        )
        await self.update_balances()
        await self._stop_pair(
            f"Не удалось закрыть позицию после неудачного DEX-свопа.\n"
            f"Незакрытый остаток: {remaining:.8f} токен ({returned_pct:.2f}% возвращено).\n"
            f"Торговля остановлена, чтобы не открывать новые сделки поверх "
            f"непокрытой позиции."
        )
        return

    def _hedge_buy_usdt_amount(self, best, filled):
        """Сколько USDT потратить на PancakeSwap, чтобы выкупить ~filled токенов
        (хедж для SELL_MEXC). Раньше здесь всегда использовалась цена best['dex'],
        зафиксированная в момент ПРИНЯТИЯ решения (несколько секунд назад, до того как
        ордер на MEXC исполнился) - к моменту реального свопа реальная цена в пуле
        могла заметно отличаться, из-за чего в пул уходило не то количество USDT,
        которое было нужно, и хедж расходился с фактической продажей на MEXC.
        Теперь берём актуальную цену из вебсокета на момент исполнения, если она
        доступна (не устарела/не равна 0), и используем цену решения только как fallback."""
        current_price = self.pancakce.buy
        price_to_use = current_price if current_price and current_price > 0 else best['dex']
        return price_to_use * filled

    def _is_in_empty_fill_cooldown(self, trade_type: str) -> bool:
        """True, если по этой стороне (BUY_MEXC/SELL_MEXC) недавно уже была попытка,
        которая вообще не наполнилась на MEXC (filled=0, ордер снят по таймауту).
        Не даёт analyze_opportunities немедленно открыть почти идентичный ордер на
        той же (ещё не изменившейся) книге, пока MEXC/DEX не успели отразить
        предыдущую отмену - см. _mark_empty_fill и историю бага в шапке класса."""
        last_ts = self.last_alert.get(trade_type)
        return last_ts is not None and (time.time() - last_ts) < self.empty_fill_cooldown

    def _mark_empty_fill(self, trade_type: str):
        self.last_alert[trade_type] = time.time()

    def _reserve_balances_for_trade(self, best):
        """Оптимистично резервирует баланс под ЗАПРОШЕННЫЙ объём сделки СРАЗУ при
        входе в make_trade - до того как ордер на MEXC вообще отправлен.
        Без этого self.balance_* остаются неизменными на всё время жизни сделки
        (MEXC-ордер + DEX-хедж, реально может занимать секунды), и если анализ
        успеет пересчитаться до того как make_trade вернётся (см. _trade_lock),
        он увидит ту же "свободную" ёмкость и насчитает ещё одну сделку поверх ещё
        не разрешившейся. Это грубая прикидка ДО реального исполнения - точная
        синхронизация с реальностью происходит в make_trade() через
        update_balances() в finally, независимо от исхода."""
        try:
            volume = float(best['volume'])
            if best['type'] == 'BUY_MEXC':
                # Покупаем volume токенов на MEXC за mexc_price -> тратим USDT на MEXC;
                # хедж продаёт volume токенов из DEX-кошелька.
                self.balance_usdt_mexc = max(0.0, self.balance_usdt_mexc - volume * float(best['mexc_price']))
                self.balance_token_dex = max(0.0, self.balance_token_dex - volume)
            else:
                # Продаём volume токенов на MEXC; хедж покупает их обратно на DEX,
                # тратя ~volume*best['dex'] USDT из DEX-кошелька.
                self.balance_token_mexc = max(0.0, self.balance_token_mexc - volume)
                self.balance_usdc_dex_bsc = max(0.0, self.balance_usdc_dex_bsc - volume * float(best['dex']))
        except (KeyError, TypeError, ValueError) as e:
            print(f"[{self.pair}] _reserve_balances_for_trade: не удалось зарезервировать баланс: {e}")

    async def make_trade(self, best, session):
        """Тонкая обёртка над _make_trade_impl: сериализует сделки по паре
        (_trade_lock), резервирует баланс ДО начала (см. _reserve_balances_for_trade)
        и ГАРАНТИРОВАННО синхронизирует баланс с реальностью ПОСЛЕ - независимо от
        исхода (успех/частичный филл/полный отказ), а не только на success-ветках
        handle_swap как было раньше. Именно отсутствие безусловной синхронизации
        после "тихих" исходов (например, ордер вообще не наполнился и просто
        отменился) и было причиной того, что следующая итерация analyze_opportunities
        находила "ту же" возможность и открывала повторный почти идентичный ордер."""
        if self._trade_lock.locked():
            print(f"[{self.pair}] make_trade: предыдущая сделка ещё не завершена, пропускаем повторный вызов")
            return
        async with self._trade_lock:
            self._reserve_balances_for_trade(best)
            # Сброс состояния текущей сделки для страховки в _make_trade_impl.
            self._mexc_filled = 0.0
            self._hedge_settled = False
            try:
                await self._make_trade_impl(best, session)
            finally:
                await self.update_balances()

    async def _make_trade_impl(self, best, session):
        curr_pair = self.pair.split('/')
        symbol = f"{curr_pair[0]}_{curr_pair[1]}"
        u_id = self.db.get_uid(self.pair)
        try:
            if best['type'] == 'SELL_MEXC':
                order = await place_limit_order(symbol, best['price'], best['volume'], True, u_id, session)
                if order == False:
                    # Раньше здесь гасился только self.running - задача monitoring_price
                    # продолжала работать, gather в main.py не возвращался, и пара
                    # оставалась полуживой вместо чистой остановки. См. _stop_pair.
                    await self._stop_pair('U_id токен MEXC устарел - откройте настройки и укажите новый')
                    return
                print(f"ОРДЕРД {order}")
                if order:
                    order_id = order['data']
                    tim = time.time()
                    while True:
                        status = await self._fetch_status(order_id)
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] == 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            self._mark_empty_fill(best['type'])
                            return
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] > 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            # Перечитываем статус ПОСЛЕ отмены: filled из проверки чуть выше
                            # мог устареть, если ордер успел дозаполниться в промежутке между
                            # той проверкой и cancel_order - хедж должен идти по финальному,
                            # уже замороженному отменой объёму, а не по устаревшему снимку.
                            status = await self._fetch_status(order_id)
                            if status['filled'] > 0:
                                val = await self.pancakce.swap_fast_async(USDT_CONTRACT, self.address, self._hedge_buy_usdt_amount(best, status['filled']))
                                await self.handle_swap(val, status, best, symbol, u_id, session)
                            else:
                                self._mark_empty_fill(best['type'])
                            return
                        if status['status'] == 'closed':
                            print(f"STATUS CLOSED: {status}")
                            break
                        if status['status'] == "canceled":
                            if status['filled'] > 0:
                                val = await self.pancakce.swap_fast_async(USDT_CONTRACT, self.address, self._hedge_buy_usdt_amount(best, status['filled']))
                                await self.handle_swap(val, status, best, symbol, u_id, session)
                            else:
                                self._mark_empty_fill(best['type'])
                            return
                        await asyncio.sleep(MEXC_ORDER_POLL_INTERVAL_SECONDS)
                    val = await self.pancakce.swap_fast_async(USDT_CONTRACT, self.address, self._hedge_buy_usdt_amount(best, status['filled']))
                    await self.handle_swap(val, status, best, symbol, u_id, session)
                    return
            else:
                order = await place_limit_order(symbol, best["price"], best['volume'], False, u_id, session)
                if order == False:
                    # См. комментарий в ветке SELL_MEXC выше.
                    await self._stop_pair('U_id токен MEXC устарел - откройте настройки и укажите новый')
                    return
                print(f"ОРДЕРД {order}")
                if order:
                    order_id = order['data']
                    tim = time.time()
                    while True:
                        status = await self._fetch_status(order_id)
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] == 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            self._mark_empty_fill(best['type'])
                            return
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] > 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            # Перечитываем статус ПОСЛЕ отмены - см. аналогичный комментарий
                            # в ветке SELL_MEXC выше.
                            status = await self._fetch_status(order_id)
                            if status['filled'] > 0:
                                val = await self.pancakce.swap_fast_async(self.address, USDT_CONTRACT, status['filled'])
                                await self.handle_swap(val, status, best, symbol, u_id, session)
                            else:
                                self._mark_empty_fill(best['type'])
                            return
                        if status['status'] == 'closed':
                            print(f"STATUS CLOSED: {status}")
                            break
                        if status['status'] == "canceled":
                            if status['filled'] > 0:
                                val = await self.pancakce.swap_fast_async(self.address, USDT_CONTRACT, status['filled'])
                                await self.handle_swap(val, status, best, symbol, u_id, session)
                            else:
                                self._mark_empty_fill(best['type'])
                            return
                        await asyncio.sleep(MEXC_ORDER_POLL_INTERVAL_SECONDS)
                    val = await self.pancakce.swap_fast_async(self.address, USDT_CONTRACT, status['filled'])
                    await self.handle_swap(val, status, best, symbol, u_id, session)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f'Error in _make_trade_impl: {e}')
            # Страховка последнего рубежа. Раньше здесь был только print, и это делало
            # любой сбой ПОСЛЕ исполнения ноги на MEXC (упавший fetch_order,
            # cancel_order, place_limit_order) молчаливой открытой позицией: до
            # handle_swap управление не доходило, откат не запускался, уведомления
            # не было. Своп теперь исключений не бросает (см. swap_fast_async), но
            # MEXC-часть по-прежнему может, поэтому разбираем ситуацию явно.
            if self._mexc_filled > 0 and not self._hedge_settled:
                print(f"[{self.pair}] сбой при {self._mexc_filled} исполненных на MEXC "
                      f"до разбора хеджа - запускаю аварийное закрытие")
                await self.send_notification(
                    f"‼️ PAIR: {self.pair}\n"
                    f"Сбой в ходе сделки: {e}\n"
                    f"На MEXC исполнено {self._mexc_filled}, хедж не подтверждён - "
                    f"закрываю позицию по стакану."
                )
                try:
                    close_is_sell = best['type'] == 'BUY_MEXC'
                    closed, remaining, problems = await self._emergency_close_position(
                        symbol, u_id, session, close_is_sell, self._mexc_filled)
                    report = (f"Аварийное закрытие {self.pair}: "
                              f"{closed:.8f} из {self._mexc_filled:.8f}")
                    if problems:
                        report += "\n\nЗамечания:\n" + "\n".join(f"• {p}" for p in problems)
                    await self.send_notification(report)
                except Exception as close_error:
                    remaining = self._mexc_filled
                    print(f"[{self.pair}] аварийное закрытие из страховки упало: {close_error}")
                    await self.send_notification(
                        f"‼️ КРИТИЧНО! Аварийное закрытие упало: {close_error}\n"
                        f"Незакрытая позиция ~{self._mexc_filled} токен {self.pair}"
                    )
                await self.update_balances()
                if remaining > self._mexc_filled * MEXC_EMERGENCY_DUST_RATIO:
                    await self._stop_pair(
                        f"Сбой в ходе сделки ({e}) и позицию закрыть не удалось.\n"
                        f"Незакрытый остаток: {remaining:.8f} токен."
                    )
