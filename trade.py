import asyncio
import time
import json
import ccxt.async_support as ccxt
from aiogram.client.session import aiohttp
from eth_account import Account
from eth_abi import decode
from web3.exceptions import ContractLogicError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pancake_trade import OkxTrade
from web3 import AsyncWeb3, Web3

from exchange import place_limit_order, get_session
from config import (
    USDT_CONTRACT,
    WBNB_ADDRESS,
    MULTICALL3_ADDRESS,
    PROFIT_THRESHOLD_USD,
    DEFAULT_GLOBAL_SPREAD_PCT,
    ALERT_COOLDOWN_SECONDS,
    MIN_PROFIT_CHANGE_USD,
    MEXC_ORDERBOOK_DEPTH_LEVELS,
    BALANCE_SAFETY_FACTOR,
    USDT_DECIMALS,
    NATIVE_DECIMALS,
    MEXC_TAKER_FEE_RATE,
    DEFAULT_PANCAKE_FEE_RATE,
    FEE_FLAT_USD,
    MEXC_ORDER_FILL_TIMEOUT_SECONDS,
    MEXC_ORDER_POLL_INTERVAL_SECONDS,
    MEXC_EMERGENCY_BUY_PRICE,
    MEXC_EMERGENCY_SELL_PRICE_FACTOR,
    MEXC_EMERGENCY_ORDER_CHECK_DELAY_SECONDS,
    ANALYZE_RESTART_DELAY_SECONDS,
    BALANCE_FETCH_MAX_RETRIES,
    BALANCE_FETCH_RETRY_DELAY_SECONDS,
    MEXC_DEPTH_HTTP_TIMEOUT_SECONDS,
    test_mode
)


class Arbitrage:
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

        with open('pancake_router_v2_abi.json', 'r') as f:
            self.smart_router_abi = json.load(f)

        with open('erc20_abi.json', 'r') as f:
            self.erc20_abi = json.load(f)

        with open('multicall_abi.json', 'r') as f:
            self.multicall_abi = json.load(f)


        self.multicall_address = MULTICALL3_ADDRESS
        self.WBNB = Web3.to_checksum_address(WBNB_ADDRESS)
        self.tes = self.pair.split('/')
        self.symbol = f"{self.tes[0]}_{self.tes[1]}"
        self._last_fee_update = 0
        self._fee_lock = asyncio.Lock()


    def compute_prefix_stats_with_max_sum(self, order_book_levels, max_sum):
        """Вычисляет кумулятивные суммы с учетом MAX_SUM, агрегируя ордера.
        Возвращает:
            - cum_amounts: список накопленных объемов
            - cum_costs: список накопленных стоимостей
            - avg_prices: список кортежей (средняя цена, цена текущего ордера)
        """
        prices, volumes = zip(*order_book_levels) if order_book_levels else ([], [])
        cum_amounts, cum_costs, avg_prices = [], [], []
        total_amount, total_cost = 0.0, 0.0

        for price, volume in zip(prices, volumes):
            remaining = max(0, max_sum - total_cost)
            if remaining <= 0:
                break

            available_volume = min(volume, remaining / price)
            new_cost = total_cost + available_volume * price

            if new_cost > max_sum:
                available_volume = (max_sum - total_cost) / price
                new_cost = total_cost + available_volume * price

            total_amount += available_volume
            total_cost = new_cost

            avg_price = total_cost / total_amount if total_amount else price
            avg_prices.append((avg_price, price)) 

            cum_amounts.append(total_amount)
            cum_costs.append(total_cost)

            if total_cost >= max_sum:
                break

        return cum_amounts, cum_costs, avg_prices

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

    async def _safe_fetch_balance(self, max_retries=BALANCE_FETCH_MAX_RETRIES, delay=BALANCE_FETCH_RETRY_DELAY_SECONDS):
        for attempt in range(max_retries):
            try:
                balance = await self.exchange.fetch_balance()
                token_name = self.pair.split("/")[0]
                if 'USDT' in balance['total'] and token_name in balance['total']:
                    return float(balance['total']['USDT']), float(balance['total'][token_name])
                return 0.0, 0.0
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                if attempt + 1 < max_retries:
                    await asyncio.sleep(delay)
            except Exception as e:
                print(f'ERROR SAFE_FETCH: {e}')
                break
        return 0.0, 0.0  

    async def _safe_get_bnb_balance(self, max_retries=BALANCE_FETCH_MAX_RETRIES, delay=BALANCE_FETCH_RETRY_DELAY_SECONDS):
        for attempt in range(1, max_retries + 1):
            try:
                raw_balance = await self.w3.eth.get_balance(  
                    self.w3.to_checksum_address(self.owner.address)
                )
                return raw_balance / (10 ** NATIVE_DECIMALS)
            except Exception as e:
                print(f"RPC error in (attempt {attempt}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(delay)
        return 0.0

    async def _safe_get_erc20_balance(self, address, decimals, max_retries=BALANCE_FETCH_MAX_RETRIES, delay=BALANCE_FETCH_RETRY_DELAY_SECONDS):
        addr = self.w3.to_checksum_address(address) 
        contract = self.w3.eth.contract(address=addr, abi=self.erc20_abi)
        for attempt in range(1, max_retries + 1):
            try:
                raw: int = await contract.functions.balanceOf(
                    self.w3.to_checksum_address(self.owner.address)
                ).call()
                return raw / (10 ** decimals)
            except ContractLogicError as e:
                print(f"Contract error: {e}")
                break
            except Exception as e:
                print(f"RPC error in (attempt {attempt}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(delay)
        return 0.0

    async def _get_dex_balances(self, token_decimals):
        # self.address = contract_bsc токена пары из БД (раньше здесь был захардкожен LAB).
        token_balance, usdc_balance = await asyncio.gather(
            self._safe_get_erc20_balance(self.address, token_decimals),
            self._safe_get_erc20_balance(USDT_CONTRACT, USDT_DECIMALS)
        )
        return token_balance, usdc_balance

    async def update_balances(self):
        try:
            t = time.time()
            results = await asyncio.gather(
                self._safe_fetch_balance(),
                self._safe_get_bnb_balance(),
                self._get_dex_balances(NATIVE_DECIMALS),
                return_exceptions=True
            )

            task_names = ['MEXC', 'BNB', 'DEX']
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"[{task_names[i]}] balance task failed: {result}")
                    return False

            mexc_balances, bnb_balance, dex_balances = results
            self.balance_usdt_mexc, self.balance_token_mexc = mexc_balances
            self.balance_usdt_mexc *= BALANCE_SAFETY_FACTOR
            self.balance_token_mexc *= BALANCE_SAFETY_FACTOR
            self.native_token = bnb_balance * BALANCE_SAFETY_FACTOR
            self.balance_token_dex, self.balance_usdc_dex_bsc = dex_balances
            self.balance_usdc_dex_bsc *= BALANCE_SAFETY_FACTOR
            self.balance_token_dex *= BALANCE_SAFETY_FACTOR

            print(f"MEXC USDT: {self.balance_usdt_mexc} | Token: {self.balance_token_mexc}")
            print(f"BNB: {self.native_token}")
            print(f"DEX Token: {self.balance_token_dex} | USDT: {self.balance_usdc_dex_bsc}")
            print(f'Update time: {time.time() - t:.3f}s')
            return True

        except Exception as e:
            print(f"Failed to update balances: {e}")
            return False

   

    async def _multicall_balances_with_native(self,
                                              token_addresses: list,
                                              wallet_address: str,
                                              token_decimals_map: dict):
        mc = self.w3.eth.contract(
            address=self.w3.to_checksum_address(self.multicall_address),
            abi=self.multicall_abi
        )

        wallet_addr = self.w3.to_checksum_address(wallet_address)
        calls = []

        for ta in token_addresses:
            token_contract = self.w3.eth.contract(
                address=self.w3.to_checksum_address(ta),
                abi= self.erc20_abi
            )
            call_data = token_contract.encode_abi(
                abi_element_identifier='balanceOf',
                args=[wallet_addr]
            )
            calls.append((self.w3.to_checksum_address(ta), call_data))

        calls.append((wallet_addr, "0x"))

        try:
            result = await mc.functions.aggregate(calls).call()
            return_data_list = result[1]
        except Exception as e:
            print(f"Multicall failed (with native) : {e}")
            token_vals = await asyncio.gather(
                *(self._safe_get_erc20_balance(addr, token_decimals_map.get(addr, NATIVE_DECIMALS))
                  for addr in token_addresses)
            )
            native = await self._safe_get_bnb_balance()
            return dict(zip(token_addresses, token_vals)), native

        balances = {}
        native_balance = None

        for idx, addr in enumerate(token_addresses):
            raw_bytes = return_data_list[idx]
            try:
                if isinstance(raw_bytes, str) and raw_bytes.startswith("0x"):
                    data_bytes = bytes.fromhex(raw_bytes[2:])
                else:
                    data_bytes = raw_bytes
                raw_int = decode(['uint256'], data_bytes)[0]
                decimals = token_decimals_map.get(addr, NATIVE_DECIMALS)
                balances[addr] = raw_int / (10 ** decimals)
            except Exception as e:
                print(f"Failed parse token {addr}: {e}")
                balances[addr] = 0.0

        
        native_balance = await self.w3.eth.get_balance(wallet_addr) / (10 ** NATIVE_DECIMALS)
        return balances, native_balance


    async def get_price_mexc(self, session):
        u_id = self.db.get_uid(self.pair)
        if session is None:
            session = await get_session()
        headers = {
            "Referer": f"https://www.mexc.com/exchange/{self.symbol}",
            "Cookie": f"uc_token={u_id}; u_id={u_id};",
            "X-Requested-With": "XMLHttpRequest",
        }
        ask = []
        bids = []
        try:
            params = {"symbol": str(self.symbol)}
            if session is not None:
                async with session.get(
                    f"https://www.mexc.com/api/platform/spot/market/depth",
                    headers=headers,
                    params=params,
                    timeout=MEXC_DEPTH_HTTP_TIMEOUT_SECONDS,
                ) as resp:
                    data = await resp.json()
                    k = 0
                    for i in data['data']['data']['asks']:
                        ask.append([float(i['p']), float(i['q'])])
                        k += 1
                        if k == MEXC_ORDERBOOK_DEPTH_LEVELS:
                            break
                    b = 0
                    for i in data['data']['data']['bids']:
                        bids.append([float(i['p']), float(i['q'])])
                        b += 1
                        if b == MEXC_ORDERBOOK_DEPTH_LEVELS:
                            break
            else:
                return None, None, None, None, None, None
        except Exception as e:
            if str(e) == 'Session is closed':
                return None, None, None, None, None, None
            if str(e) == "'data'":
                await self.send_notification(f'Нет пары на MEXC: {self.pair}\nОстановите скрипт и удалите пару')
                return None, None, None, None, None, None
            print(f'UNKNOWERROR get_price: {e} ')
            return None, None, None, None, None, None
        ask_amounts, ask_costs, ask_avg = self.compute_prefix_stats_with_max_sum(ask, self.max_volume if self.max_volume is not None else self.balance_usdt_mexc)
        bid_amounts, bid_costs, bid_avg = self.compute_prefix_stats_with_max_sum(bids, self.max_volume if self.max_volume is not None else self.balance_usdc_dex_bsc)
        return ask_amounts, ask_costs, ask_avg, bid_amounts, bid_costs, bid_avg





    async def _calc_buy_mecx_fee(self, volume, price):
        # Раньше комиссия PancakeSwap оценивалась захардкоженной ставкой 0.025%,
        # хотя реальные тиры комиссий V3 на BSC - 0.01%/0.05%/0.25%/0.3%, и для
        # многих (особенно низколиквидных) пар используется именно старший тир.
        # Это делало предварительную оценку прибыли систематически оптимистичной.
        # Теперь берём комиссию РЕАЛЬНО найденного пула (кэшируется в OkxTrade
        # после последнего свопа), а если она ещё не известна - используем
        # консервативную оценку 0.25% вместо заниженной 0.025%.
        pancake_fee_rate = getattr(self.pancakce, 'last_fee_rate', None) or 0.0025
        fee_mexc = float(volume) * float(price) * 0.0005
        fee_panckake = float(volume) * float(price) * pancake_fee_rate
        fee = fee_mexc + fee_panckake + 0.01
        return fee

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
                    okx_effective_price = float(okx_sell_price)

                    profit = (okx_effective_price - mexc_price) * volume
                    if (float(volume) * float(mexc_price) <= self.max_volume) and float(volume) <= float(self.balance_token_dex):
                        spread = ((okx_effective_price - mexc_price) / mexc_price) * 100
                        if float(spread) >= float(curr_spread):                 
                            candidates.append({
                                'type': 'BUY_MEXC',
                                'volume': volume,
                                'mexc_price': mexc_price,
                                'dex': okx_effective_price,
                                'price': price,
                                'profit': profit,
                                'spread': spread,
                                'level': i + 1,
                                'time': time.time() - t
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
                    okx_effective_price = float(okx_buy_price)

                    profit = (mexc_price - okx_effective_price) * volume
                    if (float(okx_buy_price) * float(volume) <= self.max_volume) and (volume <= self.balance_token_mexc):
                        spread = ((mexc_price - okx_effective_price) / okx_effective_price) * 100
                        if float(spread) >= float(curr_spread):    
                            candidates.append({
                                'type': 'SELL_MEXC',
                                'volume': volume,
                                'mexc_price': mexc_price,
                                'dex': okx_effective_price,
                                'price': price,
                                'profit': profit,
                                'spread': spread,
                                'level': i+1,
                                'time': time.time() - t
                            })
                if test_mode:
                    print(f'CANDIDATES: {candidates}')

                if candidates and not test_mode:
                    best = max(candidates, key=lambda x: x['profit'])
                    fee = await self._calc_buy_mecx_fee(best['volume'], best['mexc_price'])
                    print(f'BEST: {best}')
                    if best['type'] == 'BUY_MEXC' and not test_mode:
                        best['profit'] -= float(fee)
                        if best['profit'] >= self.PROFIT_THRESHOLD:
                            print(f'10: {best}')
                            # await self.make_trade(best, session)
                    else:
                        best['profit'] -= float(fee)
                        if best['profit'] >= self.PROFIT_THRESHOLD:
                            print(f'10: {best}')
                            await self.make_trade(best, session)
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

    async def handle_swap(self, val, status, best, symbol, u_id, session):
        if best['type'] == 'SELL_MEXC':
            if val.success:
                print(f'SELL_MX -> BUY_PNK | TOKEN: {self.pair} | ХЭШ: {val.tx_hash}')
                notification_text = (
                    f"🔔 Новая сделка! {self.pair}\n"
                    f"Тип: {'Продажа' if best['type'] == 'SELL_MEXC' else 'Покупка'}\n"
                    f"Объем: {status['filled']:.2f}\n"
                    f"Прибыль: ${best['profit']:.2f}\n"
                    f"Хэш: {val.tx_hash}"
                )
                await self.send_notification(notification_text)
                await self.update_balances()
            elif val.error_type in [
                val.FATAL_NO_GAS,
                val.FATAL_TOKEN_ISSUE,
                val.FATAL_NO_LIQUIDITY
            ]:
                print(f"❌ DEX swap FATAL: {val.error_type.value} - {val.error_msg}")

                await self.send_notification(
                    f"❌ PAIR: {self.pair}"
                    f"⚠️ DEX swap не удался ({val.error_type.value})\n"
                    f"Выполняю обратную покупку на MEXC для закрытия позиции..."
                )
                order = await place_limit_order(symbol, MEXC_EMERGENCY_BUY_PRICE, status['filled'], False, u_id, session)
                if order:
                    order_id = order['data']
                    await asyncio.sleep(MEXC_EMERGENCY_ORDER_CHECK_DELAY_SECONDS)
                    status2 = status = await self.exchange.fetch_order(order_id, self.pair)
                    await self.send_notification(f'Усешно купили {status2['filled']} | продали {status['filled']}\nВозвращенно: {(status2['filled']/status['filled']) * 100:.2f}%')
                    await self.update_balances()
                else:
                    await self.send_notification(
                        f"❌ КРИТИЧНО! Обратная покупка на MEXC не удалась!\n"
                        f"Срочно купите вручную ~{status['filled']} токен {self.pair} и перезапустите бота"
                    )
                    await self.update_balances()
            else:
                await self.send_notification(
                    f"⚠️ DEX swap unknow error: {val.error_msg}\n"
                    f"Проверьте баланс вручную"
                )
                await self.update_balances()
                return

        else:
            if val.success:
                print(f'SELL_PNK -> BUY_MX | TOKEN: {self.pair} | ХЭШ: {val.tx_hash}')
                notification_text = (
                    f"🔔 Новая сделка! {self.pair}\n"
                    f"Тип: {'Продажа' if best['type'] == 'SELL_MEXC' else 'Покупка'}\n"
                    f"Объем: {status['filled']:.2f}\n"
                    f"Прибыль: ${best['profit']:.2f}\n"
                    f"Хэш: {val.tx_hash}"
                )
                await self.send_notification(notification_text)
                await self.update_balances()
                return
            elif val.error_type in [
                val.FATAL_NO_GAS,
                val.FATAL_TOKEN_ISSUE,
                val.FATAL_NO_LIQUIDITY
            ]:
                print(f"❌ DEX swap FATAL: {val.error_type.value} - {val.error_msg}")

                await self.send_notification(
                    f"❌ PAIR: {self.pair}"
                    f"⚠️ DEX swap не удался ({val.error_type.value})\n"
                    f"Выполняю обратную продажу на MEXC для закрытия позиции..."
                )
                order = await place_limit_order(
                    symbol,
                    best['price'] * MEXC_EMERGENCY_SELL_PRICE_FACTOR,
                    best['volume'],
                    True,
                    u_id,
                    session,
                )
                if order:
                    order_id = order['data']
                    await asyncio.sleep(MEXC_EMERGENCY_ORDER_CHECK_DELAY_SECONDS)
                    status2 = status = await self.exchange.fetch_order(order_id, self.pair)
                    await self.send_notification(f'Усешно продали {status2['filled']} | купили {status['filled']}\nВозвращенно: {(status2['filled']/status['filled']) * 100:.2f}%')
                    await self.update_balances()
                    return
                else:
                    await self.send_notification(
                        f"❌ КРИТИЧНО! Обратная покупка на MEXC не удалась!\n"
                        f"Срочно купите вручную ~{status['filled']} токен {self.pair}"
                    )
                    return
            else:
                await self.send_notification(
                    f"⚠️ DEX swap unknow error: {val.error_msg}\n"
                    f"Проверьте баланс вручную"
                )
                await self.update_balances()
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

    async def make_trade(self, best, session):
        curr_pair = self.pair.split('/')
        symbol = f"{curr_pair[0]}_{curr_pair[1]}"
        u_id = self.db.get_uid(self.pair)
        try:
            if best['type'] == 'SELL_MEXC':
                order = await place_limit_order(symbol, best['price'], best['volume'], True, u_id, session)
                if order == False:
                    await self.send_notification('Скрипт остановлен, U_id токен устарел, нажмите настройки и поменяйте на новый')
                    self.running = False
                print(f"ОРДЕРД {order}")
                if order:
                    order_id = order['data']
                    tim = time.time()
                    while True:
                        status = await self.exchange.fetch_order(order_id, self.pair)
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] == 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            return
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] > 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            val = await self.pancakce.swap_universal_async(USDT_CONTRACT, self.address, self._hedge_buy_usdt_amount(best, status['filled']))
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        if status['status'] == 'closed':
                            print(f"STATUS CLOSED: {status}")
                            break
                        if status['status'] == "canceled":
                            val = await self.pancakce.swap_universal_async(USDT_CONTRACT, self.address, self._hedge_buy_usdt_amount(best, status['filled']))
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        await asyncio.sleep(MEXC_ORDER_POLL_INTERVAL_SECONDS)
                    val = await self.pancakce.swap_universal_async(USDT_CONTRACT, self.address, self._hedge_buy_usdt_amount(best, status['filled']))
                    await self.handle_swap(val, status, best, symbol, u_id, session)
                    return
            else:
                order = await place_limit_order(symbol, best["price"], best['volume'], False, u_id, session)
                if order == False:
                    await self.send_notification(
                        'Скрипт остановлен, U_id токен устарел, нажмите настройки и поменяйте на новый')
                    self.running = False
                print(f"ОРДЕРД {order}")
                if order:
                    order_id = order['data']
                    tim = time.time()
                    while True:
                        status = await self.exchange.fetch_order(order_id, self.pair)
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] == 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            return
                        if time.time() - tim >= MEXC_ORDER_FILL_TIMEOUT_SECONDS and status['status'] == 'open' and status['filled'] > 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            val = await self.pancakce.swap_universal_async(self.address, USDT_CONTRACT, status['filled'])
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        if status['status'] == 'closed':
                            print(f"STATUS CLOSED: {status}")
                            break
                        if status['status'] == "canceled":
                            val = await self.pancakce.swap_universal_async(self.address, USDT_CONTRACT, status['filled'])
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        await asyncio.sleep(MEXC_ORDER_POLL_INTERVAL_SECONDS)
                    val = await self.pancakce.swap_universal_async(self.address, USDT_CONTRACT, status['filled'])
                    await self.handle_swap(val, status, best, symbol, u_id, session)
                    return
        except Exception as e:
            print(f'Error in make_trade: {e}')
