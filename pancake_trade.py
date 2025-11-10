import json
from time import time
from typing import Tuple, List, Optional

import aiohttp
from web3 import AsyncWeb3, AsyncHTTPProvider, Web3
from eth_account import Account

import asyncio
from decimal import Decimal, getcontext
from config import RPC_BSC


# Сессия с connection pool и таймаутами
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

class OkxTrade:
    def __init__(self, pair, address, abi, dec2, rak, private_key, wss, rpc, db):
        self.pair = pair
        self.abi = abi
        self.address = address
        self.decimals_in = 18
        self.decimals_out = dec2
        self.private_key = private_key
        self.rak = rak
        self.ws_url = wss
        # self.w3 = Web3(Web3.LegacyWebSocketProvider("wss://bnb-mainnet.g.alchemy.com/v2/HrpyTKu0jGPtMyZvD5iCV"))
        self.w3 = Web3(Web3.LegacyWebSocketProvider(wss))
        self.rpc = AsyncWeb3(AsyncHTTPProvider(rpc))
        self.db = db
        self.account = self.rpc.eth.account.from_key(private_key)
        self.from_addr = Web3.to_checksum_address(self.account.address)
        self.router_addr = Web3.to_checksum_address("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")
        with open('pancake_router_v2_abi.json', 'r') as f:
            self.smart_router_abi = json.load(f)
        self.router = self.rpc.eth.contract(address=self.router_addr, abi=self.smart_router_abi)

        with open('erc20_abi.json', 'r') as f:
            self.erc20_abi = json.load(f)
        self.WBNB = Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")




        self.buy = self.rak
        self.sell = self.rak


    async def get_session(self) -> aiohttp.ClientSession:
        global _session
        async with _session_lock:
            if _session is None or _session.closed:
                # Создаем коннектор с настройками прокси
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
        token0 = pool.functions.token0().call()
        token1 = pool.functions.token1().call()
        if token1 == "0x55d398326f99059fF775485246999027B3197955":
            return True
        if token0 == "0x55d398326f99059fF775485246999027B3197955":
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
        price_corr = (await self.adjust_for_decimals(raw_price, self.decimals_in, self.decimals_out) if side1
                      else 1 / await self.adjust_for_decimals(raw_price, self.decimals_in, self.decimals_out))

        if abs(float(price_corr) - self.rak) <= self.rak * 0.05:
            if amount0 < 0:
                self.rak = float(price_corr)
                self.sell = float(price_corr)
            if amount1 < 0:
                self.rak = float(price_corr)
                self.buy = float(price_corr)

    async def monitoring_price(self):
        pool = self.w3.eth.contract(address=self.address, abi=self.abi)
        swap_filter = pool.events.Swap.create_filter(from_block='latest')
        side = await self.side(pool)
        if side is None:
            print("side1 is None, невозможна работа")
            return
        while True:
            try:
                events = swap_filter.get_new_entries()
                for e in events:
                    await self.handle_event(e, side)
                await asyncio.sleep(0.05)
            except Exception as exc:
                print("Ошибка при получении новых записей:", exc)

    async def swap_universal_async(
            self,
            token_in: str,
            token_out: str,
            amount_in_human: float,
            slippage: float = 0.05,
            max_price_impact: float = 0.1,
            min_profit_percent: float = 0.0,
    ):

        # загрузка ABI (блокирующая) — выполняем в отдельном потоке
        with open('pancake_router_v2_abi.json', 'r') as f:
            smart_router_abi = json.load(f)
        with open('erc20_abi.json', 'r') as f:
            erc20_abi = json.load(f)

        router = self.rpc.eth.contract(address=self.router_addr, abi=smart_router_abi)
        t = time()
        token_in_addr = Web3.to_checksum_address(token_in)
        token_out_addr = Web3.to_checksum_address(token_out)

        token_in_contract = self.rpc.eth.contract(address=token_in_addr, abi=erc20_abi)
        decimals = await token_in_contract.functions.decimals().call()
        amount_in = int(amount_in_human * (10 ** int(decimals)))


        # --- nonce & allowance ---
        nonce = await self.rpc.eth.get_transaction_count(self.from_addr)
        allowance = await token_in_contract.functions.allowance(self.from_addr, self.router_addr).call()
        if allowance < amount_in:
            # build approve tx (build_transaction синхронный, но недолго)
            tx = await token_in_contract.functions.approve(self.router_addr, amount_in).build_transaction({
                'from': self.from_addr,
                'nonce': nonce,
                'gasPrice': await self.rpc.eth.gas_price,
                'gas': 100000,
            })
            # sign in thread (blocking)
            signed = await asyncio.to_thread(Account.sign_transaction, tx, self.private_key)
            txh = await self.rpc.eth.send_raw_transaction(signed.raw_transaction)
            receipt = await self.rpc.eth.wait_for_transaction_receipt(txh)
            if receipt.status != 1:
                raise Exception("Approve failed")
            nonce += 1

        debug: List[str] = []

        candidate_paths = [
            [token_in_addr, token_out_addr],
            [token_in_addr, self.WBNB, token_out_addr],
        ]

        # === 1) Estimate V3 (exactInputSingle) concurrently for fees ===
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
                # Contract call is awaitable in AsyncWeb3
                res = await router.functions.exactInputSingle(params).call({'from': self.from_addr})
                # res может быть int или tuple
                res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
                debug.append(f"V3 exactInputSingle fee {fee} -> {res_val}")
                # обновляем общий максимум
                if res_val > amount_out_est_v3:
                    amount_out_est_v3 = res_val
                    v3_fee_used = fee
            except Exception as e:
                debug.append(f"V3 fee {fee} failed: {e}")

        await asyncio.gather(*(try_v3_fee(f) for f in common_fees))

        # === 2) Estimate V2 (swapExactTokensForTokens) concurrently for paths ===
        amount_out_est_v2 = 0
        v2_path_used = None

        async def try_v2_path(path: List[str]):
            nonlocal amount_out_est_v2, v2_path_used
            try:
                res = await router.functions.swapExactTokensForTokens(amount_in, 1, path, self.from_addr).call(
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
            for m in debug:
                print(m)
            raise Exception("All estimation methods failed: no V3 or V2 estimate available.")

        if amount_out_est_v3 >= amount_out_est_v2:
            chosen_type = 'v3'
            chosen_amount_out_est = amount_out_est_v3
            debug.append(f"Chosen V3 (fee {v3_fee_used}) amount_out_est {chosen_amount_out_est/self.decimals_out}")
        else:
            chosen_type = 'v2'
            chosen_amount_out_est = amount_out_est_v2
            debug.append(f"Chosen V2 path {v2_path_used} amount_out_est {chosen_amount_out_est/self.decimals_out}")

        for m in debug:
            print(m)
        amount_out_min = int(chosen_amount_out_est * (1 - slippage))

        # Проверка min_profit_percent
        if min_profit_percent > 0:
            expected_profit_fraction = min_profit_percent / 100.0
            approx_required = int(amount_in * (1 + expected_profit_fraction) * (10 ** (self.decimals_out - self.decimals_in)))
            if chosen_amount_out_est < approx_required:
                raise Exception(
                    f"Estimated output {chosen_amount_out_est / (10 ** self.decimals_out)} does not satisfy min_profit_percent={min_profit_percent}%")

        # Если V2 — проверяем пары и price impact (последовательно, т.к. читаем состояние)
        if chosen_type == 'v2':
            try:
                factory_v2 = await router.functions.factoryV2().call()
            except Exception:
                factory_v2 = None

            if factory_v2:
                factory_abi = [
                    {"constant": True,
                     "inputs": [{"name": "tokenA", "type": "address"}, {"name": "tokenB", "type": "address"}],
                     "name": "getPair", "outputs": [{"name": "pair", "type": "address"}], "type": "function"}
                ]
                factory = self.rpc.eth.contract(address=factory_v2, abi=factory_abi)
                # проверяем каждую пару
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

                    fee_multiplier_num = 10000 - 25  # 0.25% fee
                    numerator = amount_in * fee_multiplier_num * reserve_out
                    denominator = reserve_in * 10000 + amount_in * fee_multiplier_num
                    estimated_by_pair = numerator // denominator if denominator > 0 else 0

                    price_before = (reserve_out / (10 ** self.decimals_out)) / (
                                reserve_in / (10 ** self.decimals_in)) if reserve_in > 0 else float('inf')
                    price_after = ((reserve_out - estimated_by_pair) / (10 ** self.decimals_out)) / (
                                (reserve_in + amount_in) / (10 ** self.decimals_in)) if (reserve_in + amount_in) > 0 else 0
                    impact = abs(price_after - price_before) / price_before if price_before not in (
                    0, float('inf')) else 1.0

                    print(f"V2 hop {a}->{b}: est_out {estimated_by_pair / (10 ** self.decimals_out)}, impact {impact:.6f}")
                    if impact > max_price_impact:
                        raise Exception(
                            f"Price impact {impact:.6f} on hop {a}->{b} exceeds limit {max_price_impact}. Aborting for safety.")
            else:
                raise Exception(
                    "factoryV2 not available from router — cannot check V2 pair impacts. Aborting for safety.")

        # --- подготовка транзакции: exactInputSingle или swapExactTokensForTokens ---
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
            txn_func = router.functions.exactInputSingle(params_exec)
        else:
            txn_func = router.functions.swapExactTokensForTokens(
                amount_in,
                amount_out_min,
                v2_path_used,
                self.from_addr
            )

        # оценка газа (async)
        try:
            gas_est = await txn_func.estimate_gas({'from': self.from_addr})
        except Exception:
            gas_est = 400000

        txn = await txn_func.build_transaction({
            'from': self.from_addr,
            'gas': int(gas_est * 1.3),
            'gasPrice': await self.rpc.eth.gas_price,
            'nonce': nonce,
        })

        # sign (in thread) и отправка
        signed_txn = await asyncio.to_thread(Account.sign_transaction, txn, self.private_key)
        tx_hash = await self.rpc.eth.send_raw_transaction(signed_txn.raw_transaction)
        receipt = await self.rpc.eth.wait_for_transaction_receipt(tx_hash)

        if receipt.status != 1:
            raise Exception(f"Swap failed, tx hash: {tx_hash.hex()}")

        gas_used = receipt.gasUsed
        fee = Web3.from_wei(gas_used * (await self.rpc.eth.gas_price), 'ether')


        # извлечение фактического вывода (парсим события Transfer) — синхронная логика через to_thread
        actual_out = 0
        token_out_contract = self.rpc.eth.contract(address=token_out_addr, abi=erc20_abi)
        for log in receipt.logs:
            try:
                # process_log — синхронная функция в web3.py, вызываем в потоке
                processed = await asyncio.to_thread(token_out_contract.events.Transfer().process_log, log)
                if processed['args']['to'].lower() == self.from_addr.lower():
                    actual_out = processed['args']['value']
                    break
            except Exception:
                continue

        if actual_out > 0:
            print(f"Transaction successful! TxHash: {tx_hash.hex()} | fee: {fee} BNB Actual | output: {actual_out / (10 ** self.decimals_out)} | TIME {time() - t}")

        return tx_hash.hex(), float(fee)
