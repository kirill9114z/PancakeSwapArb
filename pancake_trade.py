import json
from time import time
from typing import Tuple, List, Optional
from enum import Enum

import aiohttp
from web3 import AsyncWeb3, AsyncHTTPProvider, Web3
from eth_account import Account

import asyncio
from decimal import Decimal, getcontext
import os
import websockets

from config import (
    USDT_CONTRACT,
    WBNB_ADDRESS,
    PANCAKE_SMART_ROUTER,
    DEFAULT_PANCAKE_FEE_RATE,
    PRICE_OUTLIER_THRESHOLD,
    STALE_PRICE_SECONDS,
    DEX_BUY_MARKUP,
    DEX_SELL_MARKDOWN,
)


_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


class SwapErrorType(Enum):
    SUCCESS = "success"
    RETRYABLE_SLIPPAGE = "retryable_slippage"
    RETRYABLE_IMPACT = "retryable_impact"
    RETRYABLE_LIQUIDITY = "retryable_liquidity"
    RETRYABLE_GAS = "retryable_gas"
    RETRYABLE_NONCE = "retryable_nonce"
    FATAL_NO_GAS = "fatal_no_gas"
    FATAL_TOKEN_ISSUE = "fatal_token_issue"
    FATAL_NO_LIQUIDITY = "fatal_no_liquidity"


class SwapResult:
    def __init__(self, success: bool, tx_hash: Optional[str] = None,
                 error_type: SwapErrorType = SwapErrorType.SUCCESS,
                 error_msg: str = ""):
        self.success = success
        self.tx_hash = tx_hash
        self.error_type = error_type
        self.error_msg = error_msg


class OkxTrade:
    def __init__(self, pair, address, abi, dec2, rak, private_key, wss, rpc, db):
        self.pair = pair
        self.address = address
        self.decimals_in = 18
        self.decimals_out = dec2
        self.private_key = private_key
        self.rak = rak
        self.wss = wss
        self.w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(wss))
        loop = asyncio.get_event_loop()
        self.USDT_ADDRESS = USDT_CONTRACT


        self.rpc = AsyncWeb3(AsyncHTTPProvider(rpc))
        self.db = db
        self.account = self.rpc.eth.account.from_key(private_key)
        self.from_addr = Web3.to_checksum_address(self.account.address)
        self.router_addr = Web3.to_checksum_address(PANCAKE_SMART_ROUTER)
        with open('pancake_router_v2_abi.json', 'r') as f:
            self.smart_router_abi = json.load(f)
        self.router = self.rpc.eth.contract(address=self.router_addr, abi=self.smart_router_abi)

        with open('erc20_abi.json', 'r') as f:
            self.erc20_abi = json.load(f)
        self.WBNB = Web3.to_checksum_address(WBNB_ADDRESS)

        abi_filepath = os.path.join("pair_abi", f"{self.pair.split('/')[0]}.json")
        with open(abi_filepath, 'r', encoding='utf-8') as f:
            self.abi = json.load(f)

        self.buy = 0
        self.sell = 0

        self.running = False
        self.backoff = 1
        self._swap_lock = asyncio.Lock()

        # Watchdog устаревшей цены: если buy/sell давно не обновлялись — гасим котировки.
        self.last_price_update_ts = time()
        self._stale_notified = False
        self.STALE_PRICE_SECONDS = STALE_PRICE_SECONDS

        # Комиссия последнего реально найденного пула (доля, напр. 0.0025 = 0.25%).
        # Используется в Arbitrage._calc_buy_mecx_fee, чтобы не оценивать прибыль по
        # захардкоженной комиссии, которая может быть в разы ниже реальной комиссии пула.
        self.last_fee_rate = None


    async def get_session(self) -> aiohttp.ClientSession:
        global _session
        async with _session_lock:
            if _session is None or _session.closed:
                connector = aiohttp.TCPConnector(
                    limit=50,
                    ttl_dns_cache=300,
                )

                _session = aiohttp.ClientSession(
                    connector=connector,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Origin": "https://www.mexc.com",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    },
                    timeout=aiohttp.ClientTimeout(
                        total=15,
                        connect=6,
                        sock_read=6
                    ),
                    trust_env=False
                )
        return _session

    async def close_session(self):
        global _session
        async with _session_lock:
            if _session and not _session.closed:
                await _session.close()

    async def side(self, pool):
        token0 = await pool.functions.token0().call()
        token1 = await pool.functions.token1().call()
        print(f'TOKEN1 {token0} | TOKEN2 {token1}')
        if token1 == self.USDT_ADDRESS:
            return False
        if token0 == self.USDT_ADDRESS:
            return False
        else:
            return None


    async def sqrtPriceX96_to_price(self, sqrtPriceX96: int) -> Decimal:
        sqrt_price = Decimal(sqrtPriceX96) / (Decimal(2) ** 96)
        price = sqrt_price * sqrt_price
        return price

    async def adjust_for_decimals(self, price, dec0: int, dec1: int) -> Decimal:
        exp = dec0 - dec1
        return price * (Decimal(10) ** exp)

    async def handle_event(self, e, side1):
        args = e["args"]
        amount0 = args['amount0']
        amount1 = args['amount1']
        sqrtPriceX96 = args["sqrtPriceX96"]
        raw_price = await self.sqrtPriceX96_to_price(sqrtPriceX96)
        price_corr = 1 / (await self.adjust_for_decimals(raw_price, self.decimals_in, self.decimals_out) if side1
                      else await self.adjust_for_decimals(raw_price, self.decimals_in, self.decimals_out))
        price_corr = float(price_corr)

        # Как раньше: обновляем цену только если отклонение от rak <= порога
        # (по умолчанию 50%). Резкий одиночный скачок отбрасываем сразу,
        # без ожидания N подтверждений — в тонком пуле сделки огромкие и
        # ждать 2–3 «похожих» свопа подряд значит терять время.
        if abs(price_corr - self.rak) <= self.rak * PRICE_OUTLIER_THRESHOLD:
            self.last_price_update_ts = time()
            self._stale_notified = False
            if amount0 < 0:
                self.rak = price_corr
                self.buy = price_corr * DEX_BUY_MARKUP
            if amount1 < 0:
                self.rak = price_corr
                self.sell = price_corr * DEX_SELL_MARKDOWN

    async def get_rak(self, side, contract):

        try:
            has_slot0 = any(item.get("type") == "function" and item.get("name") == "slot0" for item in self.abi)

            if has_slot0:
                # ===== V3 pool =====
                slot0_data = await contract.functions.slot0().call()
                sqrt_price_x96 = slot0_data[0]
                price = (sqrt_price_x96 / (2 ** 96)) ** 2
                print(f'TRUE | {price}')
                return float(price if side else 1 / price)

            else:
                # ===== V2 pair =====
                r0, r1, _ = await contract.functions.getReserves().call()
                price_token1_in_token0 = (r0 / (10 ** self.decimals_in)) / (r1 / (10 ** self.decimals_out))
                price_token0_in_token1 = 1 / price_token1_in_token0
                print(f'ELSE: 0 {price_token1_in_token0} | 1 {price_token0_in_token1}')

                return float(price_token0_in_token1 if side else price_token1_in_token0)

        except Exception as e:
            print(f"Error in execute rak price: {e}")
            return 1.0

    async def monitoring_price(self):
        self.backoff = 1
        print("Start 2")

        pool = self.rpc.eth.contract(address=self.address, abi=self.abi)
        side = await self.side(pool)
        self.rak = await self.get_rak(side, pool)
        # Сразу выставляем buy/sell из свежепрочитанной цены пула, чтобы не
        # ждать первого живого свопа - иначе buy/sell остаются 0 с момента
        # __init__, и trade.py тут же шлёт "цена DEX недоступна/устарела",
        # хотя актуальная цена только что была получена напрямую из пула.
        self.buy = self.rak * DEX_BUY_MARKUP
        self.sell = self.rak * DEX_SELL_MARKDOWN
        self.last_price_update_ts = time()
        print(f"rak: {self.rak} {self.pair}")
        if side is None:
            print("side1 is None, невозможна работа")
            return

        await self.w3.provider.connect()
        swap_filter = await pool.events.Swap.create_filter(from_block="latest")

        try:
            while self.running:
                try:
                    if not await self.w3.provider.is_connected():
                        await self.w3.provider.disconnect()
                        await self.w3.provider.connect()
                        swap_filter = await pool.events.Swap.create_filter(from_block="latest")
                        self.backoff = 1

                    events = await swap_filter.get_new_entries()
                    for e in events:
                        if not self.running:
                            break
                        await self.handle_event(e, side) 

                    # Watchdog устаревшей цены: если в пуле долго не было свопов,
                    # self.buy/self.sell могут отражать давно неактуальную цену.
                    # Раньше бот продолжал бы торговать против неё бесконечно -
                    # теперь просто "отключаем" котировки (buy=sell=0), и
                    # analyze_opportunities в trade.py уже умеет пропускать
                    # итерации при нулевой цене (см. проверки okx_sell_price/okx_buy_price == 0).
                    if (self.buy != 0 or self.sell != 0) and (time() - self.last_price_update_ts) > self.STALE_PRICE_SECONDS:
                        if not self._stale_notified:
                            print(f"[{self.pair}] Цена не обновлялась {time() - self.last_price_update_ts:.0f}s - "
                                  f"приостанавливаем торговлю по паре до следующего свопа в пуле")
                            self._stale_notified = True
                        self.buy = 0
                        self.sell = 0

                    await asyncio.sleep(0.05) 

                except (websockets.ConnectionClosedError, OSError) as e:
                    print(f"WS disconnected: {e}. Reconnecting in {self.backoff}s")
                    try:
                        await self.w3.provider.disconnect()
                    except Exception:
                        pass

                    await asyncio.sleep(self.backoff)
                    self.backoff = min(self.backoff * 2, 30)

                    await self.w3.provider.connect()
                    swap_filter = await pool.events.Swap.create_filter(from_block="latest")

                except Exception as e:
                    print("Unexpected WS error:", e)
                    await asyncio.sleep(1)
        finally:
            try:
                await self.w3.provider.disconnect()
            except Exception as e:
                print(f"Не получилось закрыть соединения вебсоккета, ошибка: {e}")
            print(f"Monitoring price stopped for {self.address}")

    async def _get_tx_status(self, tx_hash) -> Optional[bool]:
        """Проверяет, была ли транзакция уже замайнена.
        True = успех, False = revert, None = ещё не найдена в сети."""
        try:
            receipt = await self.rpc.eth.get_transaction_receipt(tx_hash)
            if receipt is None:
                return None
            return receipt.status == 1
        except Exception:
            return None

    async def _await_previous_tx(self, tx_hash_hex: str) -> Optional[bool]:
        """Даёт ранее отправленной транзакции шанс подтвердиться, прежде чем
        решать, нужно ли отправлять новую. Без этого разрыв соединения именно
        в момент wait_for_transaction_receipt приводил к повторной отправке
        того же свопа (два реальных свопа на один вызов) — это и вызывало
        дублирующиеся транзакции в кошельке."""
        for _ in range(3):
            status = await self._get_tx_status(tx_hash_hex)
            if status is not None:
                return status
            await asyncio.sleep(4)
        return None

    async def swap_universal_async(
            self,
            token_in: str,
            token_out: str,
            amount_in_human: float,
            slippage: float = 0.005,
            max_price_impact: float = 0.5,
            min_profit_percent: float = 0.0,
            max_retries: int = 3
    ):
      async with self._swap_lock:
        last_tx_hash: Optional[str] = None
        for attempt in range(max_retries):
            try:
                if last_tx_hash is not None:
                    status = await self._await_previous_tx(last_tx_hash)
                    if status is True:
                        print(f"Своп уже был выполнен ранее (tx={last_tx_hash}), повторная отправка НЕ требуется")
                        return SwapResult(success=True, tx_hash=last_tx_hash)
                    if status is None:
                        print(f"Не удалось подтвердить статус предыдущей tx {last_tx_hash} — прекращаем попытки, чтобы не отправить дубликат свопа")
                        return SwapResult(
                            success=False,
                            error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                            error_msg=f"Unknown status of previously sent tx {last_tx_hash}; aborting to avoid a duplicate on-chain swap"
                        )
                    # status is False (revert) - предыдущая tx не прошла, безопасно готовим новую попытку
                    last_tx_hash = None

                token_in_addr = Web3.to_checksum_address(token_in)
                token_out_addr = Web3.to_checksum_address(token_out)

                token_in_contract = self.rpc.eth.contract(address=token_in_addr, abi=self.erc20_abi)
                decimals = await token_in_contract.functions.decimals().call()
                amount_in = int(amount_in_human * (10 ** int(decimals)))


                nonce = await self.rpc.eth.get_transaction_count(self.from_addr)
                allowance = await token_in_contract.functions.allowance(self.from_addr, self.router_addr).call()
                if allowance < amount_in:
                    tx = await token_in_contract.functions.approve(self.router_addr, 2**100).build_transaction({
                        'from': self.from_addr,
                        'nonce': nonce,
                        'gasPrice': await self.rpc.eth.gas_price,
                        'gas': 100000,
                    })
                    signed = await asyncio.to_thread(Account.sign_transaction, tx, self.private_key)
                    txh = await self.rpc.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = await self.rpc.eth.wait_for_transaction_receipt(txh)
                    if receipt.status != 1:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            return SwapResult(
                                success=False,
                                error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                                error_msg="Approve failed after retries")
                    nonce += 1

                debug: List[str] = []

                candidate_paths = [
                    [token_in_addr, token_out_addr],
                    [token_in_addr, self.WBNB, token_out_addr],
                ]

                amount_out_est_v3 = 0
                v3_fee_used: Optional[int] = None
                common_fees = [100, 500, 2500, 3000]

                async def try_v3_fee(fee: int):
                    nonlocal amount_out_est_v3, v3_fee_used
                    try:
                        params = (
                            token_in_addr,
                            token_out_addr,
                            fee,
                            self.from_addr,
                            amount_in,
                            1,
                            0
                        )
                        res = await self.router.functions.exactInputSingle(params).call({'from': self.from_addr})
                        res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
                        debug.append(f"V3 exactInputSingle fee {fee} -> {res_val}")
                        if res_val > amount_out_est_v3:
                            amount_out_est_v3 = res_val
                            v3_fee_used = fee
                    except Exception as e:
                        debug.append(f"V3 fee {fee} failed: {e}")

                await asyncio.gather(*(try_v3_fee(f) for f in common_fees))

                amount_out_est_v2 = 0
                v2_path_used = None

                async def try_v2_path(path: List[str]):
                    nonlocal amount_out_est_v2, v2_path_used
                    try:
                        res = await self.router.functions.swapExactTokensForTokens(amount_in, 1, path, self.from_addr).call(
                            {'from': self.from_addr})
                        res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
                        debug.append(f"V2 path {path} -> {res_val}")
                        if res_val > amount_out_est_v2:
                            amount_out_est_v2 = res_val
                            v2_path_used = path
                    except Exception as e:
                        debug.append(f"V2 path {path} failed: {e}")

                await asyncio.gather(*(try_v2_path(p) for p in candidate_paths))

                if amount_out_est_v3 == 0 and amount_out_est_v2 == 0:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(3)
                        continue
                    else:
                        return SwapResult(
                            success=False,
                            error_type=SwapErrorType.FATAL_NO_LIQUIDITY,
                            error_msg="No V2/V3 liquidity after retries"
                        )

                if amount_out_est_v3 >= amount_out_est_v2:
                    chosen_type = 'v3'
                    chosen_amount_out_est = amount_out_est_v3
                    debug.append(f"Chosen V3 (fee {v3_fee_used}) amount_out_est {chosen_amount_out_est/(10**self.decimals_out)}")
                    self.last_fee_rate = v3_fee_used / 1_000_000 if v3_fee_used is not None else None
                else:
                    chosen_type = 'v2'
                    chosen_amount_out_est = amount_out_est_v2
                    debug.append(f"Chosen V2 path {v2_path_used} amount_out_est {chosen_amount_out_est/(10**self.decimals_out)}")
                    self.last_fee_rate = DEFAULT_PANCAKE_FEE_RATE  # V2: обычно 0.25%

                amount_out_min = int(chosen_amount_out_est * (1 - slippage))

                if min_profit_percent > 0:
                    MIN_OUTPUT_PERCENT = 0.97  

                    expected_output_human = chosen_amount_out_est / (10 ** self.decimals_out)
                    min_output_human = expected_output_human * MIN_OUTPUT_PERCENT
                    min_output_wei = int(min_output_human * (10 ** self.decimals_out))

                    amount_out_min = min_output_wei  

                    if chosen_amount_out_est < min_output_wei:
                        raise Exception(
                            f"Слишком большой slippage! "
                            f"{expected_output_human:.2f} → минимум {min_output_human:.2f}, "
                            f"получено {chosen_amount_out_est / (10 ** self.decimals_out):.2f}"
                        )

                if chosen_type == 'v2':
                    try:
                        factory_v2 = await self.router.functions.factoryV2().call()

                        if factory_v2:
                            factory_abi = [
                                {"constant": True,
                                 "inputs": [{"name": "tokenA", "type": "address"}, {"name": "tokenB", "type": "address"}],
                                 "name": "getPair", "outputs": [{"name": "pair", "type": "address"}], "type": "function"}
                            ]
                            factory = self.rpc.eth.contract(address=factory_v2, abi=factory_abi)
                            for i in range(len(v2_path_used) - 1):
                                a = v2_path_used[i]
                                b = v2_path_used[i + 1]
                                try:
                                    pair_addr = await factory.functions.getPair(a, b).call()
                                except Exception:
                                    pair_addr = None
                                if not pair_addr or int(pair_addr, 16) == 0:
                                    raise Exception(f"V2 pair for hop {a}->{b} not found (pair addr zero). Aborting for safety.")

                                pair_abi = [
                                    {"constant": True, "inputs": [], "name": "getReserves",
                                     "outputs": [{"name": "_reserve0", "type": "uint112"}, {"name": "_reserve1", "type": "uint112"},
                                                 {"name": "_blockTimestampLast", "type": "uint32"}], "type": "function"},
                                    {"constant": True, "inputs": [], "name": "token0", "outputs": [{"name": "", "type": "address"}],
                                     "type": "function"},
                                    {"constant": True, "inputs": [], "name": "token1", "outputs": [{"name": "", "type": "address"}],
                                     "type": "function"},
                                ]
                                pair = self.rpc.eth.contract(address=pair_addr, abi=pair_abi)
                                token0 = await pair.functions.token0().call()
                                r0, r1, _ = await pair.functions.getReserves().call()
                                if token0.lower() == a.lower():
                                    reserve_in = r0
                                    reserve_out = r1
                                else:
                                    reserve_in = r1
                                    reserve_out = r0

                                fee_multiplier_num = 10000 - 25  
                                numerator = amount_in * fee_multiplier_num * reserve_out
                                denominator = reserve_in * 10000 + amount_in * fee_multiplier_num
                                estimated_by_pair = numerator // denominator if denominator > 0 else 0

                                price_before = (reserve_out / (10 ** self.decimals_out)) / (
                                            reserve_in / (10 ** self.decimals_in)) if reserve_in > 0 else float('inf')
                                price_after = ((reserve_out - estimated_by_pair) / (10 ** self.decimals_out)) / (
                                            (reserve_in + amount_in) / (10 ** self.decimals_in)) if (reserve_in + amount_in) > 0 else 0
                                impact = abs(price_after - price_before) / price_before if price_before not in (0, float('inf')) else 1.0

                                print(f"V2 hop {a}->{b}: est_out {estimated_by_pair / (10 ** self.decimals_out)}, impact {impact:.6f}")
                                if impact > max_price_impact:
                                    if attempt < max_retries - 1:
                                        new_impact_limit = min(max_price_impact * (1.5 ** (attempt + 1)), 1.0)
                                        print(
                                            f"Impact {impact:.4f} > {max_price_impact:.4f}, retry with limit {new_impact_limit:.4f}")
                                        raise Exception(f"RETRYABLE_IMPACT: {impact:.4f}")
                                    else:
                                        return SwapResult(
                                            success=False,
                                            error_type=SwapErrorType.FATAL_NO_LIQUIDITY,
                                            error_msg=f"Price impact {impact:.4f} too high"
                                        )
                    except Exception as e:
                        if "RETRYABLE_IMPACT" in str(e):
                            max_price_impact = min(max_price_impact * 1.5, 1.0)
                            slippage = min(slippage + 0.005, 0.05)
                            await asyncio.sleep(2)
                            continue
                        elif attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        else:
                            return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_LIQUIDITY,
                                              error_msg=str(e))

                if chosen_type == 'v3':
                    if v3_fee_used is None:
                        raise Exception("No usable V3 fee / estimation found though chosen_type==v3")
                    params_exec = (
                        token_in_addr,
                        token_out_addr,
                        v3_fee_used,
                        self.from_addr,
                        amount_in,
                        amount_out_min,
                        0
                    )
                    txn_func = self.router.functions.exactInputSingle(params_exec)
                else:
                    txn_func = self.router.functions.swapExactTokensForTokens(
                        amount_in,
                        amount_out_min,
                        v2_path_used,
                        self.from_addr
                    )

                try:
                    gas_est = await txn_func.estimate_gas({'from': self.from_addr})
                except Exception as e:
                    if "insufficient funds" in str(e).lower():
                        return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_GAS,
                                          error_msg=f"No gas: {e}")
                    gas_est = 400000

                txn = await txn_func.build_transaction({
                    'from': self.from_addr,
                    'gas': int(gas_est * 1.3),
                    'gasPrice': await self.rpc.eth.gas_price,
                    'nonce': nonce,
                })

                signed_txn = await asyncio.to_thread(Account.sign_transaction, txn, self.private_key)
                tx_hash = await self.rpc.eth.send_raw_transaction(signed_txn.raw_transaction)
                last_tx_hash = tx_hash.hex()
                receipt = await self.rpc.eth.wait_for_transaction_receipt(tx_hash)

                if receipt.status != 1:
                    last_tx_hash = None
                    if attempt < max_retries - 1:
                        slippage = min(slippage + 0.05, 0.3)
                        await asyncio.sleep(3)
                        continue
                    else:
                        return SwapResult(
                            success=False,
                            error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                            error_msg=f"Swap reverted: {tx_hash.hex()}"
                        )

                return SwapResult(success=True, tx_hash=tx_hash.hex())

            except Exception as e:
                print(f'Attempt {attempt + 1} failed: {e}')
                if attempt < max_retries - 1:
                    slippage = min(slippage + 0.05, 0.3)
                    max_price_impact = min(max_price_impact * 1.5, 1.0)
                    await asyncio.sleep(2 ** attempt)
                else:
                    if last_tx_hash is not None:
                        return SwapResult(
                            success=False,
                            error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                            error_msg=f"Unknown final status of tx {last_tx_hash}: {e}"
                        )
                    return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_LIQUIDITY, error_msg=str(e))

        return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_LIQUIDITY,
                          error_msg="Max retries exceeded")
