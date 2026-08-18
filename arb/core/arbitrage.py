"""Arbitrage - объект одной торговой пары: анализ возможностей и остановка пары.

Здесь остались сборка объекта, поиск арбитражных возможностей
(analyze_opportunities + run_analysis_loop) и остановка пары. Всё остальное
разнесено по ответственностям и подмешивается миксинами:
    arb.core.balances    - балансы на MEXC и в кошельке BSC;
    arb.core.market_data - стакан MEXC и оценка комиссий;
    arb.core.execution   - исполнение сделки и разбор исхода хеджа;
    arb.core.emergency   - аварийное закрытие позиции, если хедж не прошёл.

На одну пару создаётся ровно один экземпляр (см. arb.core.runner), со своим
кошельком, своим клиентом MEXC и своим объектом пула OkxTrade.
"""
import asyncio
import time

from eth_account import Account
from web3 import AsyncWeb3, Web3

from arb.exchanges.mexc_web import get_session
from arb.paths import (
    ERC20_ABI_PATH,
    MULTICALL_ABI_PATH,
    ROUTER_ABI_PATH,
    load_json,
)
from config import (
    ALERT_COOLDOWN_SECONDS,
    ANALYZE_RESTART_DELAY_SECONDS,
    DEFAULT_GLOBAL_SPREAD_PCT,
    DEX_LOCAL_PROJECTION_ENABLED,
    MIN_PROFIT_CHANGE_USD,
    MULTICALL3_ADDRESS,
    PROFIT_THRESHOLD_USD,
    TRADE_EMPTY_FILL_COOLDOWN_SECONDS,
    WBNB_ADDRESS,
    test_mode,
)

from .balances import BalancesMixin
from .emergency import EmergencyCloseMixin
from .execution import TradeExecutionMixin
from .market_data import MarketDataMixin


class Arbitrage(BalancesMixin, MarketDataMixin, EmergencyCloseMixin, TradeExecutionMixin):
    """Одна торговая пара: анализ спреда MEXC <-> PancakeSwap и запуск сделок."""

    def __init__(self, exchange, pair, pancakce, privat_key, address, rpc, bot, chat_id, db, max_volume):
        self.exchange = exchange
        self.pair = pair
        self.address = address
        self.pancakce = pancakce
        self.private_key = privat_key
        self.owner = Account.from_key(self.private_key)
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc))
        self.bot = bot
        self.chat_id = chat_id
        self.db = db

        self.running = True

        self.max_volume = max_volume
        self.balance_usdt_mexc = 0
        self.balance_token_mexc = 0
        self.balance_token_dex = 0
        self.balance_usdc_dex_bsc = 0
        self.native_token = 0


        self.Is_enough_balance_for_fee = True

        self.usdc_contracts = {}
        self._withdrawal_fee_cache = {}
        self._cache_lock = asyncio.Lock()

        self.PROFIT_THRESHOLD = PROFIT_THRESHOLD_USD
        self.GLOBAL_SPREAD = DEFAULT_GLOBAL_SPREAD_PCT

        self.last_alert = {}
        self.alert_cooldown = ALERT_COOLDOWN_SECONDS
        self.min_profit_change = MIN_PROFIT_CHANGE_USD
        self._dex_price_stale_notified = False

        # === Защита от "погони" за собственным ордером (см. историю бага: одна и та
        # же возможность несколько раз подряд открывалась почти идентичным ордером,
        # пока предыдущий ещё не разрешился) ===
        # Лок сериализует make_trade по этой паре. Сейчас analyze_opportunities и так
        # вызывает make_trade через await в одном и том же цикле - параллельного вызова
        # быть не может, - но это дешёвая структурная страховка на случай будущего
        # рефакторинга (например, если хедж когда-нибудь вынесут в отдельный task ради
        # скорости) и явная документация инварианта "одна сделка по паре одновременно".
        self._trade_lock = asyncio.Lock()
        # last_alert переиспользуется как {trade_type: ts последней попытки с filled=0}.
        # ВАЖНО: alert_cooldown (300с, см. config) для этого не годится - это анти-спам
        # алертов, а не пауза перед следующей реальной попыткой сделки. Используем
        # отдельную короткую константу.
        self.empty_fill_cooldown = TRADE_EMPTY_FILL_COOLDOWN_SECONDS

        # Состояние ТЕКУЩЕЙ сделки для страховки в _make_trade_impl. Сериализовано
        # через _trade_lock, поэтому хватает обычных полей без блокировки.
        # _mexc_filled  - сколько реально исполнено на MEXC (то, что требует хеджа);
        # _hedge_settled - дошли ли мы до handle_swap, который разбирает исход свопа
        #                  и при необходимости откатывает позицию.
        # Если исключение прилетело при _mexc_filled > 0 и _hedge_settled == False,
        # значит нога на MEXC исполнена, а её судьбу никто не разобрал.
        self._mexc_filled = 0.0
        self._hedge_settled = False

        self.smart_router_abi = load_json(ROUTER_ABI_PATH)
        self.erc20_abi = load_json(ERC20_ABI_PATH)
        self.multicall_abi = load_json(MULTICALL_ABI_PATH)


        self.multicall_address = MULTICALL3_ADDRESS
        self.WBNB = Web3.to_checksum_address(WBNB_ADDRESS)
        self.tes = self.pair.split('/')
        self.symbol = f"{self.tes[0]}_{self.tes[1]}"
        # Кеш цены BNB в USD для перевода стоимости газа в доллары (см.
        # MarketDataMixin._bnb_price_usd). _last_fee_update - момент последнего
        # обновления кеша, _fee_lock - чтобы параллельные вызовы не устроили
        # несколько запросов тикера подряд.
        self._last_fee_update = 0
        self._fee_lock = asyncio.Lock()
        self._bnb_price_usd_cache = 0.0

        # Журнал исполнений: запись ТЕКУЩЕЙ сделки, заполняется по ходу
        # _make_trade_impl / handle_swap и пишется в БД один раз в make_trade.finally.
        # Сериализовано через _trade_lock, поэтому обычного поля достаточно.
        self._trade_record = None


    async def send_notification(self, message: str):
        if self.bot and self.chat_id:
            try:
                await self.bot.send_message(self.chat_id, message)
            except Exception as e:
                print(f"Ошибка отправки уведомления: {e}")
        print(f'SEND NOTIF: {message}')

    # Примечание: раньше здесь был метод send_opportunity_alert, который отправлял
    # найденную возможность в Telegram с inline-кнопкой для ручного подтверждения
    # (колбэк execute_arbitrage в bot.py). Он никогда не вызывался в текущей авто-
    # торговой логике (сделки исполняются сразу через make_trade), а обработчик в
    # bot.py дёргал несуществующий метод check_now_profit. Убрано как мёртвый/
    # нерабочий код, чтобы не создавать иллюзию рабочей функции подтверждения сделок.

    async def analyze_opportunities(self):
        print(f'Start 1')
        session = await get_session()
        if session is None:
            await asyncio.sleep(3)
            session = await get_session()
            if session is None:
                session = await get_session()

        spread = self.db.get_pair_spread(self.pair)
        curr_spread = spread if spread else self.db.get_global_spread()
        print(f'SPREAD {self.pair}: {curr_spread} and {spread}')
        try:
            while self.running == True:
                t = time.time()
                ask_amounts, ask_costs, ask_avg, bid_amounts, bid_costs, bid_avg = await self.get_price_mexc(session)
                if ask_amounts is None:
                    continue
                okx_sell_price = self.pancakce.sell
                if okx_sell_price == 0:
                    print(f'Twen')
                    if not self._dex_price_stale_notified:
                        self._dex_price_stale_notified = True
                        await self.send_notification(
                            f'⏸ {self.pair}: цена DEX недоступна/устарела (нет свежих свопов в пуле), '
                            f'торговля по паре приостановлена до появления новой цены'
                        )
                    continue

                candidates = []
                for i in range(len(ask_amounts)):
                    volume = ask_amounts[i] if ((ask_amounts[i] * ask_avg[i][0]) <= self.balance_usdt_mexc) else (self.balance_usdt_mexc/ask_avg[i][0])
                    mexc_price = ask_avg[i][0]
                    price = ask_avg[i][1]

                    # BUY_MEXC: покупаем token на MEXC, хеджируем ПРОДАЖЕЙ volume токенов
                    # на DEX -> нужна цена продажи именно под этот объём, а не плоский
                    # self.pancakce.sell один и тот же для любого уровня стакана.
                    dex_quote = self.pancakce.quote_local(volume, is_buy=False)
                    if dex_quote is None:
                        # Фоллбэк на плоскую цену: это mid-цена пула ± маркап, комиссии
                        # пула в ней НЕТ - её обязан вычесть _calc_buy_mecx_fee.
                        okx_effective_price = float(okx_sell_price)
                        dex_needs_confirm = False
                        dex_fee_included = False
                        dex_impact_pct = None
                    else:
                        okx_effective_price = dex_quote['effective_price']
                        dex_needs_confirm = dex_quote['needs_confirmation']
                        dex_fee_included = dex_quote['fee_included']
                        dex_impact_pct = dex_quote['impact_pct']

                    profit = (okx_effective_price - mexc_price) * volume
                    if (float(volume) * float(mexc_price) <= self.max_volume) and float(volume) <= float(self.balance_token_dex):
                        spread = ((okx_effective_price - mexc_price) / mexc_price) * 100
                        if float(spread) >= float(curr_spread if not test_mode else -10):
                            candidates.append({
                                'type': 'BUY_MEXC',
                                'volume': volume,
                                'mexc_price': mexc_price,
                                'dex': okx_effective_price,
                                'price': price,
                                'profit': profit,
                                'spread': spread,
                                'level': i + 1,
                                'time': time.time() - t,
                                'dex_needs_confirm': dex_needs_confirm,
                                'dex_fee_included': dex_fee_included,
                                'dex_impact_pct': dex_impact_pct,
                                'dex_price_source': 'flat' if dex_quote is None else 'local',
                            })

                okx_buy_price = self.pancakce.buy
                if okx_buy_price == 0:
                    print(f'Nomber 2')
                    if not self._dex_price_stale_notified:
                        self._dex_price_stale_notified = True
                        await self.send_notification(
                            f'⏸ {self.pair}: цена DEX недоступна/устарела (нет свежих свопов в пуле), '
                            f'торговля по паре приостановлена до появления новой цены'
                        )
                    continue

                if self._dex_price_stale_notified:
                    self._dex_price_stale_notified = False
                    await self.send_notification(f'▶️ {self.pair}: цена DEX снова актуальна, торговля возобновлена')

                for i in range(len(bid_amounts)):
                    volume = bid_amounts[i] if ((bid_amounts[i] * okx_buy_price) <= self.balance_usdc_dex_bsc) else (self.balance_usdc_dex_bsc / okx_buy_price)
                    mexc_price = bid_avg[i][0]
                    price = bid_avg[i][1]

                    # SELL_MEXC: продаём token на MEXC, хеджируем ПОКУПКОЙ volume токенов
                    # на DEX. Реальное исполнение (_hedge_buy_usdt_amount) тратит
                    # volume * self.pancakce.buy USDT и берёт что получится (exact-in) -
                    # проецируем именно эту сумму, чтобы совпадать с тем, что реально
                    # отправится в своп.
                    usdt_seed = volume * okx_buy_price
                    dex_quote = self.pancakce.quote_local(usdt_seed, is_buy=True)
                    if dex_quote is None:
                        # См. комментарий в ветке BUY_MEXC выше: в плоской цене
                        # комиссии пула нет, её вычтет _calc_buy_mecx_fee.
                        okx_effective_price = float(okx_buy_price)
                        dex_needs_confirm = False
                        dex_fee_included = False
                        dex_impact_pct = None
                    else:
                        okx_effective_price = dex_quote['effective_price']
                        dex_needs_confirm = dex_quote['needs_confirmation']
                        dex_fee_included = dex_quote['fee_included']
                        dex_impact_pct = dex_quote['impact_pct']

                    profit = (mexc_price - okx_effective_price) * volume
                    if (float(okx_buy_price) * float(volume) <= self.max_volume) and (volume <= self.balance_token_mexc):
                        spread = ((mexc_price - okx_effective_price) / okx_effective_price) * 100
                        if float(spread) >= float(curr_spread if not test_mode else -10):
                            candidates.append({
                                'type': 'SELL_MEXC',
                                'volume': volume,
                                'mexc_price': mexc_price,
                                'dex': okx_effective_price,
                                'price': price,
                                'profit': profit,
                                'spread': spread,
                                'level': i+1,
                                'time': time.time() - t,
                                'dex_needs_confirm': dex_needs_confirm,
                                'dex_fee_included': dex_fee_included,
                                'dex_impact_pct': dex_impact_pct,
                                'dex_price_source': 'flat' if dex_quote is None else 'local',
                            })

                if candidates:
                    best = max(candidates, key=lambda x: x['profit'])

                    # Защита от "погони" за собственным ордером: если по этой стороне
                    # только что была попытка, которая вообще не наполнилась на MEXC
                    # (см. _mark_empty_fill в make_trade), даём книге/балансу немного
                    # времени отразить это, вместо того чтобы тут же открывать почти
                    # идентичный ордер на той же, ещё не изменившейся цене.
                    if self._is_in_empty_fill_cooldown(best['type']):
                        continue

                    # Локальная V3-модель (в пределах текущего тика) может занижать
                    # реальный price impact для крупных сделок - если quote_local()
                    # отметил needs_confirmation, делаем ОДИН ограниченный по времени
                    # on-chain запрос ТОЛЬКО под победившего кандидата (не под все уровни
                    # стакана) и ПЕРЕД отправкой ордера на MEXC. При таймауте/ошибке не
                    # берём сделку вслепую - пропускаем итерацию и идём дальше, чтобы не
                    # потерять момент на MEXC, ожидая RPC, с ценой, в которой не уверены.
                    if DEX_LOCAL_PROJECTION_ENABLED and best.get('dex_needs_confirm'):
                        is_buy_confirm = best['type'] == 'SELL_MEXC'
                        confirm_amount = (best['volume'] * okx_buy_price) if is_buy_confirm else best['volume']
                        confirmed_price = await self.pancakce.confirm_price_onchain(confirm_amount, is_buy_confirm)
                        if confirmed_price is None:
                            print(f"[{self.pair}] on-chain confirm не ответил вовремя - "
                                  f"пропускаем итерацию, не берём сделку по неподтверждённой цене")
                            continue
                        best['dex'] = confirmed_price
                        # Роутер возвращает РЕАЛЬНЫЙ amountOut, то есть комиссия пула в
                        # подтверждённой цене тоже уже учтена - вычитать её отдельно нельзя.
                        best['dex_fee_included'] = True
                        best['dex_price_source'] = 'onchain'
                        if best['type'] == 'BUY_MEXC':
                            best['profit'] = (confirmed_price - best['mexc_price']) * best['volume']
                        else:
                            best['profit'] = (best['mexc_price'] - confirmed_price) * best['volume']

                    fee = await self._calc_buy_mecx_fee(
                        best['volume'], best['mexc_price'], best.get('dex_fee_included', False))
                    best['fee'] = float(fee)
                    best['profit'] -= float(fee)
                    print(f'BEST: {best}')
                    if best['profit'] >= self.PROFIT_THRESHOLD and not test_mode:
                        print(f'10: {best}')
                        # await self.make_trade(best, session)
                else:
                    continue
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[{self.pair}] analyze_opportunities crashed: {e}")
            await self.send_notification(
                f'⚠️ {self.pair}: ошибка анализа: {e}. Перезапуск через {ANALYZE_RESTART_DELAY_SECONDS} сек'
            )
            await asyncio.sleep(ANALYZE_RESTART_DELAY_SECONDS)
            # Раньше тут стояло self.running = False, затем сразу self.running = True -
            # но это НЕ перезапускало анализ, а просто выставляло флаг в мёртвой корутине:
            # analyze_opportunities после этого блока завершался (return), и больше
            # НИКТО не вызывал его повторно, хотя self.running выглядел как True, а
            # pancake.monitoring_price() продолжал работать как будто всё в порядке.
            # Реальный перезапуск теперь делает run_analysis_loop() ниже.

    async def run_analysis_loop(self):
        """Обёртка над analyze_opportunities, которая ДЕЙСТВИТЕЛЬНО перезапускает
        анализ после любого необработанного исключения, пока self.running == True.
        main.py должен запускать задачу именно через этот метод, а не через
        analyze_opportunities() напрямую."""
        while self.running:
            try:
                await self.analyze_opportunities()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[{self.pair}] run_analysis_loop: unexpected error: {e}")
                await asyncio.sleep(5)
            else:
                # analyze_opportunities вернулся нормально (например, после того как
                # сам поймал исключение и заснул на 30 сек) - если бот всё ещё должен
                # работать, просто запускаем анализ снова.
                if self.running:
                    await asyncio.sleep(1)

    async def _stop_pair(self, reason: str):
        """Полностью останавливает работу бота по ЭТОЙ паре (остальные пары не трогает).

        Гасить нужно ДВА независимых флага, иначе остановка выходит половинчатой:
          self.running          -> выходят analyze_opportunities и run_analysis_loop;
          self.pancakce.running -> выходит WS-цикл monitoring_price.
        main.py ждёт обе задачи через asyncio.gather, поэтому если погасить только
        self.running, задача мониторинга цены продолжит крутиться вечно, gather
        никогда не вернётся, и finally в monitor_pair (отмена задач, закрытие
        ccxt-клиента, удаление из active_arbitrage_instances) не отработает -
        пара повиснет в полуживом состоянии вместо чистой остановки.
        Идемпотентен: повторный вызов ничего не делает и не шлёт второе уведомление."""
        if not self.running and not getattr(self.pancakce, 'running', False):
            return
        self.running = False
        try:
            self.pancakce.running = False
        except Exception as e:
            print(f"[{self.pair}] не удалось остановить monitoring_price: {e}")
        print(f"[{self.pair}] ТОРГОВЛЯ ПО ПАРЕ ОСТАНОВЛЕНА: {reason}")
        await self.send_notification(
            f"🛑 Торговля по паре {self.pair} ОСТАНОВЛЕНА\n\n{reason}\n\n"
            f"Остальные пары продолжают работать. После разбора запустите бота заново."
        )
