"""Выдача роутеру allowance на токены пула.

Разовая, но обязательная подготовка: без неё confirm_price_onchain падает с
revert 'STF' даже на read-only .call(), а первый реальный своп всё равно
потребовал бы approve.
"""
import asyncio

from eth_account import Account
from web3 import Web3


class RouterAllowanceMixin:
    """Approve роутеру. Подмешивается в OkxTrade."""

    async def _ensure_token_allowance(self, token_addr: str, min_allowance: int = 2 ** 100) -> bool:
        """Проверяет allowance токена к роутеру и при нехватке отправляет approve()
        (РЕАЛЬНАЯ on-chain транзакция, ждём receipt). Логика идентична инлайновой
        проверке в swap_universal_async (специально не рефакторил их в одну функцию -
        не хотел трогать уже проверенный реальный торговый путь ради этого фикса).
        Approve нужен на весь баланс токена сразу (2**100 ~ бесконечность) - делается
        по факту ОДИН РАЗ НАВСЕГДА на пару кошелёк+токен+роутер, дальше allowance
        просто лежит в сети между перезапусками бота."""
        try:
            token_contract = self.rpc.eth.contract(address=Web3.to_checksum_address(token_addr), abi=self.erc20_abi)
            allowance = await token_contract.functions.allowance(self.from_addr, self.router_addr).call()
            if allowance >= min_allowance:
                return True

            print(f"[{self.pair}] allowance для {token_addr} -> router недостаточен ({allowance}), "
                  f"отправляю approve() (реальная tx, разово)")
            nonce = await self.rpc.eth.get_transaction_count(self.from_addr)
            tx = await token_contract.functions.approve(self.router_addr, 2 ** 100).build_transaction({
                'from': self.from_addr,
                'nonce': nonce,
                'gasPrice': await self.rpc.eth.gas_price,
                'gas': 100000,
            })
            signed = await asyncio.to_thread(Account.sign_transaction, tx, self.private_key)
            tx_hash = await self.rpc.eth.send_raw_transaction(signed.raw_transaction)
            receipt = await self.rpc.eth.wait_for_transaction_receipt(tx_hash)
            ok = receipt.status == 1
            print(f"[{self.pair}] approve({token_addr}) -> {'OK' if ok else 'REVERTED'}, tx={tx_hash.hex()}")
            return ok
        except Exception as e:
            print(f"[{self.pair}] _ensure_token_allowance({token_addr}) failed: {e}")
            return False

    async def _ensure_router_allowances(self):
        """Разово при старте пары гарантирует approve роутеру на ОБА токена пула.
        Нужно и для confirm_price_onchain (иначе его exactInputSingle().call() штатно
        рвётся 'STF' - TransferHelper.safeTransferFrom требует allowance даже для
        read-only .call(), а не только для реальной транзакции), и в любом случае
        понадобится перед первой настоящей сделкой в swap_universal_async - просто
        делаем это заранее, а не откладываем до первого реального свопа."""
        if self.token0_addr is None or self.token1_addr is None:
            return
        for token_addr in (self.token0_addr, self.token1_addr):
            await self._ensure_token_allowance(token_addr)
