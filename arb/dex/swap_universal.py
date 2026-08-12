"""Универсальный своп: сам ищет маршрут, работает для любой пары токенов.

Медленный путь (~8-12 RPC round-trip до отправки): перебирает 4 fee-тира V3 и
2 пути V2, оценивает price impact, ретраит с эскалацией слиппеджа. Используется
как фоллбэк, когда быстрый путь (arb.dex.swap_fast) неприменим или выключен.
"""
import asyncio
from typing import List, Optional

from eth_account import Account
from web3 import Web3

from config import DEFAULT_PANCAKE_FEE_RATE

from .types import SwapErrorType, SwapResult


class UniversalSwapMixin:
    """Своп с поиском маршрута. Подмешивается в OkxTrade."""

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
                            error_type=SwapErrorType.FATAL_UNKNOWN_STATUS,
                            tx_hash=last_tx_hash,
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
                            error_type=SwapErrorType.FATAL_UNKNOWN_STATUS,
                            tx_hash=last_tx_hash,
                            error_msg=f"Unknown final status of tx {last_tx_hash}: {e}"
                        )
                    return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_LIQUIDITY, error_msg=str(e))

        return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_LIQUIDITY,
                          error_msg="Max retries exceeded")
