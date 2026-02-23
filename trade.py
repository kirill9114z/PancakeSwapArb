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
USDT_CONTRACTS = '0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d'


cached = {}
class Arbitrage:
    # def __init__(self, exchange, pair, pancakce, db, chat_id, bot, privat_key):
    def __init__(self, exchange, pair, pancakce, privat_key, address, rpc, bot, chat_id, db, max_volume):
        self.exchange = exchange
        self.pair = pair
        self.address = address
        self.pancakce = pancakce
        self.private_key = privat_key
        self.owner = Account.from_key(self.private_key)
        # self.owner = Keypair.from_base58_string(self.private_key)
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

        self.PROFIT_THRESHOLD = 0.2
        self.GLOBAL_SPREAD = -0.1

        self.last_alert = {}
        self.alert_cooldown = 300
        self.min_profit_change = 1

        with open('pancake_router_v2_abi.json', 'r') as f:
            self.smart_router_abi = json.load(f)

        with open('erc20_abi.json', 'r') as f:
            self.erc20_abi = json.load(f)

        with open('multicall_abi.json', 'r') as f:
            self.multicall_abi = json.load(f)


        self.multicall_address = "0xcA11bde05977b3631167028862bE2a173976CA11"
        self.WBNB = Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
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

            # Корректировка объема, если сумма превышает max_sum
            if new_cost > max_sum:
                available_volume = (max_sum - total_cost) / price
                new_cost = total_cost + available_volume * price

            # Обновляем итоговые значения
            total_amount += available_volume
            total_cost = new_cost

            # Сохраняем среднюю цену и цену текущего ордера
            avg_price = total_cost / total_amount if total_amount else price
            avg_prices.append((avg_price, price))  # Кортеж из двух значений

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

    async def send_opportunity_alert(self, opportunity):
        """Отправляет сообщение об арбитражной возможности с кнопкой"""
        if not self.bot or not self.chat_id:
            print("Bot or chat_id not initialized. Cannot send message.")
            return

        opp_type = opportunity['type']
        volume = opportunity['volume']
        mexc_price = opportunity['mexc_price']
        okx_price = opportunity['dex_price']
        profit = opportunity['profit']
        spread = opportunity['spread']

        # Определяем направление сделки
        direction = "MEXC⬆️ → DEX⬇️" if opp_type == 'BUY_MEXC' else "OKX⬆️ → DEX⬇️"

        def format_price(p):
            if float(p) == 0:
                return "0"
            if float(p) < 0.0001:
                s = f"{p:.20f}"  # Преобразуем в строку с 20 знаками
                s_clean = s.rstrip('0').rstrip('.')  # Убираем хвостовые нули

                if '.' in s_clean and s_clean.split('.')[0] == '0':
                    fractional = s_clean.split('.')[1]
                    zeros = 0
                    # Считаем последовательные нули
                    for char in fractional:
                        if char == '0':
                            zeros += 1
                        else:
                            break
                    # Если нулей >=5 и есть значащие цифры
                    if zeros >= 4 and zeros < len(fractional):
                        return f"0.{{{zeros}}}{fractional[zeros:zeros + 8]}"  # Берем до 8 значащих цифр
                return f"{s_clean:.8f}".rstrip('0')
            else:
                # Для обычных цен убираем лишние нули
                return f"{p:.8f}".rstrip('0')

        mexc_price_str = format_price(mexc_price)
        okx_price_str = format_price(okx_price)

        # Создаем текст сообщения
        message_text = (
            f"🚀 *Арбитражная возможность!*\n\n"
            f"*Направление:* {direction}\n"
            f"*Пара:* {self.pair}\n"
            f"*Объем:* {volume:.6f}\n"
            f"*Цена MEXC:* {mexc_price_str}\n"
            f"*Цена OKX:* {okx_price_str}\n"
            f"*Прибыль:* ${profit:.4f}\n"
            f"*Спред:* {spread:.4f}%\n"
        )

        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message_text,
                parse_mode="Markdown",
            )
        except Exception as e:
            print(f"Error sending message: {e}")

    async def _safe_fetch_balance(self, max_retries=3, delay=5):
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
        return 0.0, 0.0  # ← всегда tuple!

    async def _safe_get_bnb_balance(self, max_retries=3, delay=5):
        for attempt in range(1, max_retries + 1):
            try:
                raw_balance = await self.w3.eth.get_balance(  # ← rpc, не w3
                    self.w3.to_checksum_address(self.owner.address)
                )
                return raw_balance / (10 ** 18)
            except Exception as e:
                print(f"RPC error in (attempt {attempt}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(delay)
        return 0.0

    async def _safe_get_erc20_balance(self, address, decimals, max_retries=3, delay=5):
        addr = self.w3.to_checksum_address(address)  # ← rpc, не w3
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
        # ✅ Передаём корутины (не await заранее), gather сам параллельно запускает
        token_balance, usdc_balance = await asyncio.gather(
            self._safe_get_erc20_balance("0xF74548802f4c700315F019FdE17178b392EE4444", token_decimals),  # ← token_address!
            self._safe_get_erc20_balance(USDT_CONTRACTS, 18)
        )
        return token_balance, usdc_balance

    async def update_balances(self):
        try:
            t = time.time()
            results = await asyncio.gather(
                self._safe_fetch_balance(),
                self._safe_get_bnb_balance(),
                self._get_dex_balances(18),
                return_exceptions=True
            )

            # Проверка на ошибки в задачах
            task_names = ['MEXC', 'BNB', 'DEX']
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"[{task_names[i]}] balance task failed: {result}")
                    return False

            mexc_balances, bnb_balance, dex_balances = results
            self.balance_usdt_mexc, self.balance_token_mexc = mexc_balances
            self.balance_usdt_mexc *= 0.99
            self.balance_token_mexc *= 0.99
            self.native_token = bnb_balance * 0.99
            self.balance_token_dex, self.balance_usdc_dex_bsc = dex_balances
            self.balance_usdc_dex_bsc *= 0.99
            self.balance_token_dex *= 0.99

            print(f"MEXC USDT: {self.balance_usdt_mexc} | Token: {self.balance_token_mexc}")
            print(f"BNB: {self.native_token}")
            print(f"DEX Token: {self.balance_token_dex} | USDT: {self.balance_usdc_dex_bsc}")
            print(f'Update time: {time.time() - t:.3f}s')
            return True

        except Exception as e:
            print(f"Failed to update balances: {e}")
            return False

    # async def update_balances(self):
    #     try:
    #         t0 = time.time()
    #         pair_data = self.db.get_pair_data(self.pair)
    #
    #         token_addresses = [
    #             pair_data['contract_bsc'],  # ваш токен
    #             USDT_CONTRACTS
    #         ]
    #         decimals_map = {
    #             token_addresses[0]: pair_data['decimals'],
    #             token_addresses[1]: 18
    #         }
    #
    #         task_exchange = asyncio.create_task(self._safe_fetch_balance())
    #         task_multicall = asyncio.create_task(
    #             self._multicall_balances_with_native(token_addresses, self.owner.address, decimals_map)
    #         )
    #
    #         mexc_res, multicall_res = await asyncio.gather(task_exchange, task_multicall, return_exceptions=True)
    #
    #         if isinstance(mexc_res, Exception):
    #             print("MEXC task error:", mexc_res)
    #             mexc_res = (0.0, 0.0)
    #
    #         if isinstance(multicall_res, Exception):
    #             print("Multicall task error:", multicall_res)
    #             token_dict = {addr: 0.0 for addr in token_addresses}
    #             native_val = await self._safe_get_bnb_balance()
    #         else:
    #             token_dict, native_val = multicall_res
    #             if native_val is None:
    #                 native_val = await self._safe_get_bnb_balance()
    #
    #         # if native_val < 0.0007: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #         #     self.running = False
    #         #     await self.send_notification(f'Осталось мало BNB, пополните баланс.')
    #
    #         self.balance_usdt_mexc, self.balance_token_mexc = mexc_res
    #         self.native_token = native_val
    #         self.balance_token_dex = token_dict.get(token_addresses[0], 0.0)
    #         self.balance_usdc_dex_bsc = token_dict.get(token_addresses[1], 0.0)
    #
    #         print(f"MEXC {self.balance_usdt_mexc, self.balance_token_mexc}")
    #         print(f"Native: {self.native_token}")
    #         print(f"Dex: {self.balance_token_dex, self.balance_usdc_dex_bsc}")
    #         print(f"TIME: {time.time() - t0}")
    #
    #         return True
    #     except Exception as e:
    #         print(f"Failed to update balances: {e}")
    #         return False


    # async def update_balances(self):
    #     try:
    #         self.balance_usdt_mexc, self.balance_token_mexc = 10000, 500
    #         self.native_token = 0.1
    #         self.balance_token_dex = 10000
    #         self.balance_usdc_dex_bsc = 500
    #         return True
    #     except Exception as e:
    #         print(f"Failed to update balances: {e}")
    #         return False


    # async def _safe_fetch_balance(self, max_retries: int = 2, delay: float = 0.5):
    #     """
    #     Получить балансы с биржи (USDT и токен).
    #     """
    #     for attempt in range(1, max_retries + 1):
    #         try:
    #             # таймаут на случай зависания
    #             res = await asyncio.wait_for(self.exchange.fetch_balance(), timeout=6)
    #             base_symbol = self.pair.split("/")[0]
    #             usdt = float(res['total'].get('USDT', 0))
    #             token = float(res['total'].get(base_symbol, 0))
    #             return usdt, token
    #         except asyncio.TimeoutError:
    #             print(f"fetch_balance timeout attempt {attempt}")
    #         except Exception as e:
    #             print(f"ERROR SAFE_FETCH attempt {attempt}: {e}")
    #         if attempt < max_retries:
    #             await asyncio.sleep(delay)
    #     return 0.0, 0.0
    #
    # async def _safe_get_bnb_balance(self, max_retries: int = 3, delay: float = 1.0):
    #     """
    #     Получить баланс нативной монеты (BNB).
    #     """
    #     for attempt in range(1, max_retries + 1):
    #         try:
    #             raw = await asyncio.wait_for(
    #                 self.w3.eth.get_balance(self.w3.to_checksum_address(self.owner.address)),
    #                 timeout=5
    #             )
    #             return raw / (10 ** 18)
    #         except Exception as e:
    #             print(f"RPC error in BNB (attempt {attempt}): {e}")
    #         if attempt < max_retries:
    #             await asyncio.sleep(delay)
    #     return 0.0

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

        # токены
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
            # _, return_data_list = result
            return_data_list = result[1]
        except Exception as e:
            print(f"Multicall failed (with native) : {e}")
            # fallback
            token_vals = await asyncio.gather(
                *(self._safe_get_erc20_balance(addr, token_decimals_map.get(addr, 18))
                  for addr in token_addresses)
            )
            native = await self._safe_get_bnb_balance()
            return dict(zip(token_addresses, token_vals)), native

        balances = {}
        native_balance = None

        # парсим токены
        for idx, addr in enumerate(token_addresses):
            raw_bytes = return_data_list[idx]
            try:
                # преобразование
                if isinstance(raw_bytes, str) and raw_bytes.startswith("0x"):
                    data_bytes = bytes.fromhex(raw_bytes[2:])
                else:
                    data_bytes = raw_bytes
                raw_int = decode(['uint256'], data_bytes)[0]
                decimals = token_decimals_map.get(addr, 18)
                balances[addr] = raw_int / (10 ** decimals)
            except Exception as e:
                print(f"Failed parse token {addr}: {e}")
                balances[addr] = 0.0

        # парсим нативку (последний)
        # raw_native_bytes = return_data_list[len(token_addresses)]
        # try:
        #     if isinstance(raw_native_bytes, str) and raw_native_bytes.startswith("0x"):
        #         data_bytes = bytes.fromhex(raw_native_bytes[2:])
        #     else:
        #         data_bytes = raw_native_bytes
        #     print(f'Nat: {data_bytes}\n\n{raw_native_bytes}\n{token_addresses}')
        #     native_int = decode(['uint256'], data_bytes)[0]
        #     print(f'Res nat: {native_int / (10 ** 18)}')
        #     native_balance = native_int / (10 ** 18)
        # except Exception as e:
        #     print(f"Failed parse native balance: {e}")
        #     native_balance = None
        # Альтернатива: получить нативный баланс отдельным вызовом
        native_balance = await self.w3.eth.get_balance(wallet_addr) / (10 ** 18)
        return balances, native_balance

    # async def _safe_get_erc20_balance(self, address: str, decimals: int,
    #                                   max_retries: int = 3, delay: float = 1.0):
    #     """
    #     Индивидуальный вызов баланса ERC-20. Используется как fallback.
    #     """
    #     addr = self.w3.to_checksum_address(address)
    #     contract = self.w3.eth.contract(address=addr, abi=self.erc20_abi)
    #     for attempt in range(1, max_retries + 1):
    #         try:
    #             raw = await asyncio.wait_for(
    #                 contract.functions.balanceOf(self.w3.to_checksum_address(self.owner.address)).call(),
    #                 timeout=5
    #             )
    #             return raw / (10 ** decimals)
    #         except asyncio.TimeoutError:
    #             print(f"ERC20 balance timeout attempt {attempt} for {address}")
    #         except Exception as e:
    #             print(f"RPC error ERC20 (attempt {attempt}) for {address}: {e}")
    #         if attempt < max_retries:
    #             await asyncio.sleep(delay)
    #     return 0.0


    async def get_price_mexc(self, session):
        # session = await get_session()
        u_id = self.db.get_uid(self.pair)
        if session is None:
            session = await get_session()
        # u_id = self.db.get_uid()
        headers = {
            "Referer": f"https://www.mexc.com/exchange/{self.symbol}",
            "Cookie": f"uc_token={u_id}; u_id={u_id};",
            "X-Requested-With": "XMLHttpRequest",
        }
        ask = []
        bids = []
        try:
            # params = {"symbol": str(self.symbol), "type": "step0"}
            params = {"symbol": str(self.symbol)}
            if session is not None:
                async with session.get(f"https://www.mexc.com/api/platform/spot/market/depth", headers=headers, params=params, timeout=5) as resp:
                    data = await resp.json()
                    k = 0
                    for i in data['data']['data']['asks']:
                        ask.append([float(i['p']), float(i['q'])])
                        k += 1
                        if k == 7:
                            break
                    b = 0
                    for i in data['data']['data']['bids']:
                        bids.append([float(i['p']), float(i['q'])])
                        b += 1
                        if b == 7:
                            break
            else:
                return None, None, None, None, None, None
        # except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        #     print(f"Error fetching vcoinId get_price: {e}")
        #     return None, None, None, None, None, None
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
        fee_mexc = float(volume) * float(price) * 0.0005
        fee_panckake = float(volume) * float(price) * 0.00025
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
                # Получаем данные стакана с MEXC
                ask_amounts, ask_costs, ask_avg, bid_amounts, bid_costs, bid_avg = await self.get_price_mexc(session)
                if ask_amounts is None:
                    continue
                # Используем максимальную цену OKX как эталон для продажи
                okx_sell_price = self.pancakce.sell
                if okx_sell_price == 0:
                    print(f'Twen')
                    continue

                candidates = []
                for i in range(len(ask_amounts)):
                    volume = ask_amounts[i] if ((ask_amounts[i] * ask_avg[i][0]) <= self.balance_usdt_mexc) else (self.balance_usdt_mexc/ask_avg[i][0])
                    mexc_price = ask_avg[i][0]
                    price = ask_avg[i][1]
                    # Рассчитываем комиссии и проскальзывание для OKX
                    # okx_effective_price = float(okx_sell_price) * 0.99
                    okx_effective_price = float(okx_sell_price)

                    # Рассчитываем прибыль
                    profit = (okx_effective_price - mexc_price) * volume
                    if (float(volume) * float(mexc_price) <= self.max_volume) and float(volume) <= float(self.balance_token_dex):
                        spread = ((okx_effective_price - mexc_price) / mexc_price) * 100
                        if float(spread) >= float(curr_spread):                  #СПРЕД !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
                    continue

                # Анализируем BIDS (покупка на OKX -> продажа на MEXC)
                for i in range(len(bid_amounts)):
                    volume = bid_amounts[i] if ((bid_amounts[i] * okx_buy_price) <= self.balance_usdc_dex_bsc) else (self.balance_usdc_dex_bsc / okx_buy_price)
                    mexc_price = bid_avg[i][0]  # Средняя цена продажи на MEXC
                    price = bid_avg[i][1]
                    # Используем минимальную цену OKX для покупки
                    okx_effective_price = float(okx_buy_price)

                    # Рассчитываем прибыль
                    profit = (mexc_price - okx_effective_price) * volume
                    if (float(okx_buy_price) * float(volume) <= self.max_volume) and (volume <= self.balance_token_mexc):
                    # if (float(okx_buy_price) * float(volume) <= 200):
                        spread = ((mexc_price - okx_effective_price) / okx_effective_price) * 100
                        if float(spread) >= float(curr_spread):       #СПРЕД !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

                if candidates:
                    best = max(candidates, key=lambda x: x['profit'])
                    # fee = calculate_total_gas_cost(id3[best['chain_id']], cureent, best['volume'])
                    # best['profit'] = best['profit'] - fee
                    # print(f'1: {best}')
                    fee = await self._calc_buy_mecx_fee(best['volume'], best['mexc_price'])
                    if best['type'] == 'BUY_MEXC':
                        best['profit'] -= float(fee)
                        if best['profit'] >= self.PROFIT_THRESHOLD:
                            # print(f'BUY {best} | {fee}')
                            # await self.send_opportunity_alert(best)
                            print(f'10: {best}')
                            await self.make_trade(best, session)
                    else:
                        best['profit'] -= float(fee)
                        if best['profit'] >= self.PROFIT_THRESHOLD:
                            # print(f'SEll | {best} | {fee}')
                            # await self.send_opportunity_alert(best)
                            print(f'10: {best}')
                            await self.make_trade(best, session)
                else:
                    continue
        except Exception as e:
            self.running = False
            await self.send_notification(f'Произошла ошибка: {e}. Перезапуск через 30 сек')
            await asyncio.sleep(5)
            self.running = True
        # finally:
        # await session.close()

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

                # Автоматическая обратная покупка на MEXC (фиксируем убыток)
                await self.send_notification(
                    f"❌ PAIR: {self.pair}"
                    f"⚠️ DEX swap не удался ({val.error_type.value})\n"
                    f"Выполняю обратную покупку на MEXC для закрытия позиции..."
                )
                order = await place_limit_order(symbol, 10000, status['filled'], False, u_id, session)
                if order:
                    order_id = order['data']
                    await asyncio.sleep(1)
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
                # Неизвестная ошибка
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

                # Автоматическая обратная покупка на MEXC (фиксируем убыток)
                await self.send_notification(
                    f"❌ PAIR: {self.pair}"
                    f"⚠️ DEX swap не удался ({val.error_type.value})\n"
                    f"Выполняю обратную продажу на MEXC для закрытия позиции..."
                )
                order = await place_limit_order(symbol, best['price']*0.9, best['volume'], True, u_id, session)
                if order:
                    order_id = order['data']
                    await asyncio.sleep(1)
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
                # Неизвестная ошибка
                await self.send_notification(
                    f"⚠️ DEX swap unknow error: {val.error_msg}\n"
                    f"Проверьте баланс вручную"
                )
                await self.update_balances()
                return

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
                        if time.time() - tim >= 3 and status['status'] == 'open' and status['filled'] == 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            return
                        if time.time() - tim >= 3 and status['status'] == 'open' and status['filled'] > 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            val = await self.pancakce.swap_universal_async(USDT_CONTRACTS, self.address, best['dex'] * status['filled'])
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        if status['status'] == 'closed':
                            print(f"STATUS CLOSED: {status}")
                            break
                        if status['status'] == "canceled":
                            val = await self.pancakce.swap_universal_async(USDT_CONTRACTS, self.address, best['dex'] * status['filled'])
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        await asyncio.sleep(0.05)
                    val = await self.pancakce.swap_universal_async(USDT_CONTRACTS, self.address, best['dex'] * status['filled'])
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
                        if time.time() - tim >= 3 and status['status'] == 'open' and status['filled'] == 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            return
                        if time.time() - tim >= 3 and status['status'] == 'open' and status['filled'] > 0:
                            await self.exchange.cancel_order(order_id, self.pair)
                            val = await self.pancakce.swap_universal_async(self.address, USDT_CONTRACTS, status['filled'])
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        if status['status'] == 'closed':
                            print(f"STATUS CLOSED: {status}")
                            break
                        if status['status'] == "canceled":
                            val = await self.pancakce.swap_universal_async(self.address, USDT_CONTRACTS, status['filled'])
                            await self.handle_swap(val, status, best, symbol, u_id, session)
                            return
                        await asyncio.sleep(0.05)
                    # 3) DEX swap
                    val = await self.pancakce.swap_universal_async(self.address, USDT_CONTRACTS, status['filled'])
                    await self.handle_swap(val, status, best, symbol, u_id, session)
                    return
        except Exception as e:
            print(f'Error in make_trade: {e}')
