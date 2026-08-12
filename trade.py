import asyncio
import math
import time
import json
import ccxt.async_support as ccxt
from aiogram.client.session import aiohttp
from eth_account import Account
from eth_abi import decode
from web3.exceptions import ContractLogicError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pancake_trade import OkxTrade, SwapErrorType
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
    MEXC_EMERGENCY_PRICE_BUFFER_PCT,
    MEXC_EMERGENCY_THIN_BOOK_BUFFER_PCT,
    MEXC_EMERGENCY_MAX_ATTEMPTS,
    MEXC_EMERGENCY_FILL_TIMEOUT_SECONDS,
    MEXC_EMERGENCY_DUST_RATIO,
    MEXC_EMERGENCY_BALANCE_SAFETY,
    MEXC_EMERGENCY_ORDER_CHECK_DELAY_SECONDS,
    ANALYZE_RESTART_DELAY_SECONDS,
    BALANCE_FETCH_MAX_RETRIES,
    BALANCE_FETCH_RETRY_DELAY_SECONDS,
    MEXC_DEPTH_HTTP_TIMEOUT_SECONDS,
    DEX_LOCAL_PROJECTION_ENABLED,
    TRADE_EMPTY_FILL_COOLDOWN_SECONDS,
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


    async def _fetch_orderbook_raw(self, session, depth_levels=MEXC_ORDERBOOK_DEPTH_LEVELS):
        """Сырые уровни стакана MEXC: (asks, bids), каждый - список [цена, объём],
        отсортированный от лучшей цены. Вынесено из get_price_mexc, чтобы аварийное
        закрытие позиции читало стакан ровно тем же способом, что и анализ, и не
        заводило второй, расходящийся с ним источник цены.
        Возвращает (None, None) при любой ошибке."""
        u_id = self.db.get_uid(self.pair)
        if session is None:
            session = await get_session()
        if session is None:
            return None, None
        headers = {
            "Referer": f"https://www.mexc.com/exchange/{self.symbol}",
            "Cookie": f"uc_token={u_id}; u_id={u_id};",
            "X-Requested-With": "XMLHttpRequest",
        }
        ask = []
        bids = []
        try:
            params = {"symbol": str(self.symbol)}
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
                    if k == depth_levels:
                        break
                b = 0
                for i in data['data']['data']['bids']:
                    bids.append([float(i['p']), float(i['q'])])
                    b += 1
                    if b == depth_levels:
                        break
        except Exception as e:
            if str(e) == 'Session is closed':
                return None, None
            if str(e) == "'data'":
                await self.send_notification(f'Нет пары на MEXC: {self.pair}\nОстановите скрипт и удалите пару')
                return None, None
            print(f'UNKNOWERROR get_price: {e} ')
            return None, None
        return ask, bids

    async def get_price_mexc(self, session):
        ask, bids = await self._fetch_orderbook_raw(session)
        if ask is None or bids is None:
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

                    # BUY_MEXC: покупаем token на MEXC, хеджируем ПРОДАЖЕЙ volume токенов
                    # на DEX -> нужна цена продажи именно под этот объём, а не плоский
                    # self.pancakce.sell один и тот же для любого уровня стакана.
                    dex_quote = self.pancakce.quote_local(volume, is_buy=False)
                    if dex_quote is None:
                        okx_effective_price = float(okx_sell_price)
                        dex_needs_confirm = False
                    else:
                        okx_effective_price = dex_quote['effective_price']
                        dex_needs_confirm = dex_quote['needs_confirmation']

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
                        okx_effective_price = float(okx_buy_price)
                        dex_needs_confirm = False
                    else:
                        okx_effective_price = dex_quote['effective_price']
                        dex_needs_confirm = dex_quote['needs_confirmation']

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
                        if best['type'] == 'BUY_MEXC':
                            best['profit'] = (confirmed_price - best['mexc_price']) * best['volume']
                        else:
                            best['profit'] = (best['mexc_price'] - confirmed_price) * best['volume']

                    fee = await self._calc_buy_mecx_fee(best['volume'], best['mexc_price'])
                    print(f'BEST: {best}')
                    if best['type'] == 'BUY_MEXC':
                        best['profit'] -= float(fee)
                        if best['profit'] >= self.PROFIT_THRESHOLD and not test_mode:
                            print(f'10: {best}')
                            # await self.make_trade(best, session)
                    else:
                        best['profit'] -= float(fee)
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

    @staticmethod
    def _price_decimals(levels) -> int:
        """Число знаков после запятой у цен в стакане. Цены из стакана заведомо
        соответствуют шагу цены инструмента, поэтому округление нашей агрессивной
        цены до той же точности защищает от отказа биржи по нарушению тика."""
        decimals = 0
        for price, _ in levels[:10]:
            text = repr(float(price))
            if '.' in text and 'e' not in text and 'E' not in text:
                decimals = max(decimals, len(text.split('.')[1]))
        return min(decimals, 12)

    def _marketable_price(self, levels, qty, close_is_sell):
        """Цена, которая гарантированно сметёт стакан на объём qty.

        Идём вглубь стакана, пока накопленного объёма не хватит на qty, берём цену
        последнего задетого уровня и сдвигаем её в невыгодную сторону на буфер -
        так ордер пересекает спред и забирает все уровни до нужного включительно.
        Если видимой глубины не хватило, буфер берётся увеличенный: за пределами
        MEXC_ORDERBOOK_DEPTH_LEVELS уровней ликвидность может найтись.

        Возвращает (price, enough_depth) либо (None, False), если стакан пуст."""
        if not levels:
            return None, False

        cumulative = 0.0
        touch_price = float(levels[0][0])
        enough_depth = False
        for price, volume in levels:
            touch_price = float(price)
            cumulative += float(volume)
            if cumulative >= qty:
                enough_depth = True
                break

        buffer_pct = MEXC_EMERGENCY_PRICE_BUFFER_PCT if enough_depth else MEXC_EMERGENCY_THIN_BOOK_BUFFER_PCT
        if close_is_sell:
            # Продаём в биды: цена НИЖЕ самого глубокого нужного бида.
            price = touch_price * (1 - buffer_pct / 100)
        else:
            # Выкупаем аски: цена ВЫШЕ самого глубокого нужного аска.
            price = touch_price * (1 + buffer_pct / 100)

        # Округляем всегда в сторону БОЛЬШЕЙ агрессии, чтобы округление не сделало
        # ордер вдруг непересекающим спред.
        #
        # Точность берём из стакана: его цены заведомо соответствуют шагу цены
        # инструмента, и лишние знаки могут привести к отказу по нарушению тика.
        # Но у этого есть вырожденный случай: если все видимые уровни оказались
        # круглыми (например единственный уровень 0.1), точность выйдет в 1 знак,
        # и округление вверх превратит 0.102 в 0.2 - переплата 100% вместо
        # задуманных 2%. Поэтому добавляем знаки ТОЛЬКО тогда, когда округление по
        # точности стакана сдвигает цену больше, чем на сам буфер. В нормальном
        # стакане это условие выполняется сразу и точность остаётся биржевой.
        target = price
        price = None
        for decimals in range(self._price_decimals(levels), 13):
            factor = 10 ** decimals
            candidate = (math.floor(target * factor) / factor if close_is_sell
                         else math.ceil(target * factor) / factor)
            if candidate > 0 and abs(candidate - target) <= target * (buffer_pct / 100):
                price = candidate
                break
        if price is None or price <= 0:
            return None, False
        return price, enough_depth

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

            price, enough_depth = self._marketable_price(levels, remaining, close_is_sell)
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
