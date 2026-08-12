"""Чтение балансов: MEXC (через ccxt), нативный BNB и ERC20 в кошельке на BSC.

Все методы отказоустойчивы по построению: при сетевой ошибке возвращают 0.0
после ретраев, а не бросают исключение - вызывающий код (analyze_opportunities,
make_trade) не должен падать из-за одного неотвеченного RPC.
"""
import asyncio
import time

import ccxt.async_support as ccxt
from eth_abi import decode
from web3.exceptions import ContractLogicError

from config import (
    BALANCE_FETCH_MAX_RETRIES,
    BALANCE_FETCH_RETRY_DELAY_SECONDS,
    BALANCE_SAFETY_FACTOR,
    NATIVE_DECIMALS,
    USDT_CONTRACT,
    USDT_DECIMALS,
)


class BalancesMixin:
    """Балансы пары на обеих площадках. Подмешивается в Arbitrage."""

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
