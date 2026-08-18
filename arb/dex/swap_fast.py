"""Быстрый своп через УЖЕ ИЗВЕСТНЫЙ пул - горячий путь хеджа.

Всё, что можно узнать заранее (chainId, decimals, allowance, nonce, цена газа,
маршрут), прогревается в prepare_fast_swap() и поддерживается фоново в
monitoring_price, поэтому до отправки транзакции остаётся ~1 round-trip вместо
~8-12 у arb.dex.swap_universal. Если условия быстрого пути не выполнены -
молча делегирует в универсальный своп.
"""
import asyncio
from decimal import Decimal, localcontext
from time import time
from typing import Optional, Tuple

from eth_account import Account
from web3 import AsyncHTTPProvider, AsyncWeb3, Web3

from config import (
    DEFAULT_PANCAKE_FEE_RATE,
    DEX_ONCHAIN_CONFIRM_TIMEOUT_SECONDS,
    SWAP_DEADLINE_SECONDS,
    SWAP_FAST_MAX_SEND_RETRIES,
    SWAP_FAST_PATH_ENABLED,
    SWAP_FAST_SLIPPAGE,
    SWAP_GAS_LIMIT_V2,
    SWAP_GAS_LIMIT_V3,
    SWAP_GAS_PRICE_MAX_AGE_SECONDS,
    SWAP_GAS_PRICE_MAX_GWEI,
    SWAP_GAS_PRICE_PREMIUM,
    SWAP_PRIVATE_TX_RPC,
    SWAP_RECEIPT_TIMEOUT_SECONDS,
    SWAP_STRICT_NEXT_BLOCK,
)

from .types import SwapErrorType, SwapResult


class FastSwapMixin:
    """Быстрый своп. Подмешивается в OkxTrade."""

    # === Быстрый своп через УЖЕ ИЗВЕСТНЫЙ пул ==========================================
    # swap_universal_async выше остаётся нетронутым и работает как фоллбэк: он ничего не
    # знает о пуле заранее и потому каждый раз заново ищет маршрут. Для хеджа это лишнее -
    # своп всегда идёт ровно через тот пул, за которым бот и так следит по WS, и все
    # параметры маршрута уже лежат в self (pool_kind, v3_fee_ppm, token0/token1).
    #
    # Что убрано из горячего пути по сравнению с swap_universal_async:
    #   decimals()              -> _decimals_cache (значение неизменно)
    #   allowance()             -> _ensure_router_allowances() при старте
    #   6x eth_call маршрута    -> quote_local() локально, без сети
    #   eth_estimateGas         -> фиксированный лимит газа (маршрут всегда один)
    #   eth_chainId             -> _chain_id, прогрет при старте
    #   eth_gasPrice            -> кеш, обновляемый фоново в monitoring_price
    #   eth_getTransactionCount -> локальный счётчик _next_nonce
    #   V2 factory/getPair/token0/getReserves -> удалено, дублирует котировку роутера
    # Остаётся eth_sendRawTransaction + опрос receipt, то есть ~1 round-trip до попадания
    # в мемпул вместо ~8 (V3) / ~12 (V2).
    #
    # Что добавлено:
    #   - deadline через multicall(uint256, bytes[]) - прямой вызов SmartRouter его не
    #     принимает, и без обёртки транзакция получается бессрочной;
    #   - опциональный строгий режим multicall(bytes32 previousBlockhash, bytes[]);
    #   - премия к цене газа и потолок;
    #   - приватная (MEV-защищённая) отправка;
    #   - жёсткий таймаут на receipt;
    #   - НЕТ эскалации слиппеджа и НЕТ sleep между попытками.

    async def prepare_fast_swap(self):
        """Разовый прогрев всего, что swap_fast_async иначе спрашивал бы у сети перед
        каждым свопом. Вызывается из monitoring_price при старте пары, после
        _init_local_quote_state и _ensure_router_allowances. Ошибка тут не валит бота:
        _fast_ready остаётся False, и swap_fast_async просто делегирует в
        swap_universal_async, то есть поведение откатывается к прежнему."""
        if not SWAP_FAST_PATH_ENABLED:
            print(f"[{self.pair}] быстрый своп выключен (SWAP_FAST_PATH_ENABLED=False) - "
                  f"используется swap_universal_async")
            return
        if not self._local_quote_ready or self.token0_addr is None or self.token1_addr is None:
            print(f"[{self.pair}] prepare_fast_swap: состояние пула не готово "
                  f"(_local_quote_ready={self._local_quote_ready}) - быстрый своп недоступен, "
                  f"свопы пойдут через swap_universal_async")
            return
        if self.pool_kind == 'v3' and self.v3_fee_ppm is None:
            print(f"[{self.pair}] prepare_fast_swap: неизвестен fee-тир V3-пула - быстрый своп недоступен")
            return

        try:
            self._chain_id = await self.rpc.eth.chain_id

            # decimals обоих токенов пула - вместо чтения перед каждым свопом.
            # Заодно снимает захардкоженное предположение "у стейбла 18 знаков".
            for token_addr in (self.token0_addr, self.token1_addr):
                erc20 = self.rpc.eth.contract(address=Web3.to_checksum_address(token_addr), abi=self.erc20_abi)
                self._decimals_cache[token_addr.lower()] = int(await erc20.functions.decimals().call())

            if await self._sync_nonce() is None:
                print(f"[{self.pair}] prepare_fast_swap: не удалось получить nonce - быстрый своп недоступен")
                return
            if await self._refresh_gas_price() is None:
                print(f"[{self.pair}] prepare_fast_swap: не удалось получить цену газа - быстрый своп недоступен")
                return

            if SWAP_PRIVATE_TX_RPC:
                self._private_w3 = AsyncWeb3(AsyncHTTPProvider(SWAP_PRIVATE_TX_RPC))
                print(f"[{self.pair}] приватная отправка транзакций включена: {SWAP_PRIVATE_TX_RPC}")
            else:
                print(f"[{self.pair}] ВНИМАНИЕ: SWAP_PRIVATE_TX_RPC не задан - свопы уходят в публичный "
                      f"мемпул и видны сэндвич-ботам")

            self._fast_ready = True
            print(f"[{self.pair}] Быстрый своп готов: pool_kind={self.pool_kind}, "
                  f"fee_ppm={self.v3_fee_ppm}, chain_id={self._chain_id}, nonce={self._next_nonce}, "
                  f"gas_price={self._gas_price_wei / 1e9:.3f} gwei, decimals={self._decimals_cache}, "
                  f"deadline_mode={'previousBlockhash' if SWAP_STRICT_NEXT_BLOCK else 'timestamp'}")
        except Exception as e:
            self._fast_ready = False
            print(f"[{self.pair}] prepare_fast_swap failed: {e} - свопы пойдут через swap_universal_async")

    async def _sync_nonce(self) -> Optional[int]:
        """Приводит локальный счётчик nonce в соответствие с сетью. block='pending',
        а не 'latest' как в swap_universal_async: 'latest' не учитывает собственные
        ещё не замайненные транзакции и потому может выдать уже использованный nonce."""
        try:
            self._next_nonce = await self.rpc.eth.get_transaction_count(self.from_addr, 'pending')
            return self._next_nonce
        except Exception as e:
            print(f"[{self.pair}] _sync_nonce failed: {e}")
            return None

    async def _refresh_gas_price(self) -> Optional[int]:
        """Обновляет кеш цены газа: базовая цена сети + премия, с потолком.
        Вызывается фоново из monitoring_price, чтобы горячий путь свопа не платил
        за этот round-trip."""
        try:
            base = await self.rpc.eth.gas_price
        except Exception as e:
            print(f"[{self.pair}] _refresh_gas_price failed: {e}")
            return None
        price = int(base * SWAP_GAS_PRICE_PREMIUM)
        if SWAP_GAS_PRICE_MAX_GWEI > 0:
            price = min(price, int(SWAP_GAS_PRICE_MAX_GWEI * 1e9))
        self._gas_price_wei = price
        self._gas_price_ts = time()
        return price

    async def _gas_price_for_swap(self) -> Optional[int]:
        """Цена газа для отправки. Обычно берётся из кеша (0 round-trip); в сеть идём,
        только если кеш протух настолько, что доверять ему опаснее, чем потерять
        один round-trip."""
        if self._gas_price_wei is not None and (time() - self._gas_price_ts) <= SWAP_GAS_PRICE_MAX_AGE_SECONDS:
            return self._gas_price_wei
        return await self._refresh_gas_price() or self._gas_price_wei

    @staticmethod
    def _to_raw(amount_human: float, decimals: int) -> int:
        """human -> wei без потери точности. int(float * 10**18) в swap_universal_async
        теряет младшие разряды (у float всего ~15-16 значащих цифр) и может округлить
        сумму ВВЕРХ - при попытке продать весь баланс это даёт реверт по нехватке средств.
        Decimal(str(x)) берёт ровно ту десятичную запись, которую видит человек."""
        with localcontext() as ctx:
            ctx.prec = 60
            return int(Decimal(str(amount_human)).scaleb(decimals).to_integral_value(rounding='ROUND_DOWN'))

    def _decimals_of(self, token_addr: str) -> Optional[int]:
        return self._decimals_cache.get(token_addr.lower())

    def _fast_route_direction(self, token_in_addr: str, token_out_addr: str) -> Optional[bool]:
        """Применим ли быстрый путь к этой паре токенов, и если да - is_buy.
        is_buy=True: тратим стейбл, получаем торгуемый токен (та же семантика, что у
        quote_local). None означает "быстрый путь не применим" - вызывающий обязан
        делегировать в swap_universal_async, который умеет искать произвольный маршрут."""
        if not (SWAP_FAST_PATH_ENABLED and self._fast_ready and self._local_quote_ready):
            return None
        if self.token0_addr is None or self.token1_addr is None or self.usdt_is_token0 is None:
            return None
        if self.pool_kind == 'v3' and self.v3_fee_ppm is None:
            return None
        # Быстрый путь знает ровно один маршрут - прямой своп через наблюдаемый пул.
        # Любая другая пара токенов (в т.ч. маршрут через WBNB) - не сюда.
        pool_tokens = {self.token0_addr.lower(), self.token1_addr.lower()}
        if {token_in_addr.lower(), token_out_addr.lower()} != pool_tokens:
            return None
        stable_addr = (self.token0_addr if self.usdt_is_token0 else self.token1_addr).lower()
        return token_in_addr.lower() == stable_addr

    async def _quote_onchain_amount_out_raw(self, token_in_addr: str, token_out_addr: str,
                                            amount_in_raw: int) -> Optional[int]:
        """Один eth_call к роутеру по ИЗВЕСТНОМУ маршруту - используется, только когда
        локальной оценке доверять нельзя (quote_local вернул None или пометил
        needs_confirmation, т.е. сделка выходит за пределы текущего тика V3)."""
        try:
            async def _call():
                if self.pool_kind == 'v3':
                    params = (token_in_addr, token_out_addr, self.v3_fee_ppm,
                              self.from_addr, amount_in_raw, 1, 0)
                    res = await self.router.functions.exactInputSingle(params).call({'from': self.from_addr})
                else:
                    res = await self.router.functions.swapExactTokensForTokens(
                        amount_in_raw, 1, [token_in_addr, token_out_addr], self.from_addr
                    ).call({'from': self.from_addr})
                return int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)

            return await asyncio.wait_for(_call(), timeout=DEX_ONCHAIN_CONFIRM_TIMEOUT_SECONDS)
        except Exception as e:
            print(f"[{self.pair}] _quote_onchain_amount_out_raw failed: {e}")
            return None

    async def _estimate_amount_out_raw(self, amount_in_human: float, amount_in_raw: int, is_buy: bool,
                                       token_in_addr: str, token_out_addr: str,
                                       decimals_out: int) -> Tuple[Optional[int], str]:
        """Ожидаемый выход свопа в wei - основа для amountOutMin.
        Сначала пробуем локальную модель (0 round-trip). В сеть идём только если она
        сама себе не доверяет: для V3 её математика точна лишь в пределах текущего тика,
        и завышенная оценка здесь означала бы завышенный amountOutMin, то есть
        гарантированный реверт уже отправленной транзакции."""
        q = self.quote_local(amount_in_human, is_buy)
        if q is not None and not q['needs_confirmation']:
            # Берём именно amount_out (чистый выход AMM), а не effective_price:
            # в effective_price уже вшит защитный буфер DEX_BUY_MARKUP/SELL_MARKDOWN,
            # который к ожидаемому выходу свопа отношения не имеет.
            return self._to_raw(q['amount_out'], decimals_out), 'local'
        return await self._quote_onchain_amount_out_raw(token_in_addr, token_out_addr, amount_in_raw), 'onchain'

    async def _build_swap_calldata(self, token_in_addr: str, token_out_addr: str,
                                   amount_in_raw: int, amount_out_min_raw: int) -> Optional[str]:
        """Calldata свопа, обёрнутая в multicall ради deadline.
        exactInputSingle/swapExactTokensForTokens у SmartRouter deadline НЕ принимают -
        он есть только у multicall. Прямой вызов (как в swap_universal_async) означает
        транзакцию без срока годности, которая может исполниться спустя минуты по
        давно ушедшей цене."""
        try:
            if self.pool_kind == 'v3':
                params = (token_in_addr, token_out_addr, self.v3_fee_ppm,
                          self.from_addr, amount_in_raw, amount_out_min_raw, 0)
                inner = self.router.encode_abi(abi_element_identifier='exactInputSingle', args=[params])
            else:
                inner = self.router.encode_abi(
                    abi_element_identifier='swapExactTokensForTokens',
                    args=[amount_in_raw, amount_out_min_raw, [token_in_addr, token_out_addr], self.from_addr])
            inner_bytes = Web3.to_bytes(hexstr=inner)

            if SWAP_STRICT_NEXT_BLOCK:
                # "Следующий блок или ничего": контракт требует
                # blockhash(block.number-1) == переданный хеш. Стоит +1 round-trip
                # и оплаченный газ за реверт, если не успели в блок.
                block = await self.rpc.eth.get_block('latest')
                return self.router.encode_abi(
                    abi_element_identifier='multicall(bytes32,bytes[])',
                    args=[bytes(block['hash']), [inner_bytes]])

            deadline = int(time()) + int(SWAP_DEADLINE_SECONDS)
            return self.router.encode_abi(
                abi_element_identifier='multicall(uint256,bytes[])',
                args=[deadline, [inner_bytes]])
        except Exception as e:
            print(f"[{self.pair}] _build_swap_calldata failed: {e}")
            return None

    async def _send_raw_tx(self, raw_tx) -> None:
        """Отправка подписанной транзакции: сначала в приватный релей (если задан),
        при его отказе - в обычный RPC. Дубликат свопа так создать нельзя: повторно
        уходит ТА ЖЕ подписанная транзакция с тем же хешем и nonce, а замайнена она
        может быть только один раз."""
        if self._private_w3 is not None:
            try:
                await self._private_w3.eth.send_raw_transaction(raw_tx)
                return
            except Exception as e:
                print(f"[{self.pair}] приватная отправка не удалась ({e}) - повторяю через основной RPC")
        await self.rpc.eth.send_raw_transaction(raw_tx)

    async def swap_fast_async(self, token_in: str, token_out: str, amount_in_human: float,
                              slippage: float = SWAP_FAST_SLIPPAGE) -> SwapResult:
        """Быстрый своп через известный пул. Если быстрый путь неприменим (выключен,
        не прогрет или токены не совпадают с наблюдаемым пулом) - прозрачно делегирует
        в swap_universal_async, поэтому вызывающему коду не нужно знать про режимы.

        ГАРАНТИЯ: этот метод НИКОГДА не бросает исключение (кроме CancelledError) и
        всегда возвращает SwapResult. На это опирается trade.py: разбор исхода сделки
        и аварийное закрытие позиции идут только через SwapResult, и вылетевшее отсюда
        исключение означало бы незахеджированную позицию без отката и без уведомления."""
        try:
            token_in_addr = Web3.to_checksum_address(token_in)
            token_out_addr = Web3.to_checksum_address(token_out)

            is_buy = self._fast_route_direction(token_in_addr, token_out_addr)
            decimals_in = self._decimals_of(token_in_addr)
            decimals_out = self._decimals_of(token_out_addr)
            if is_buy is None or decimals_in is None or decimals_out is None:
                # Делегируем ВНЕ _swap_lock: swap_universal_async берёт этот же лок сам.
                return await self.swap_universal_async(token_in, token_out, amount_in_human)

            async with self._swap_lock:
                return await self._swap_fast_impl(token_in_addr, token_out_addr, amount_in_human,
                                                  is_buy, decimals_in, decimals_out, slippage)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Сюда попадают только сбои ДО отправки транзакции: разбор адресов,
            # взятие лока, а также всё, что мог бросить swap_universal_async
            # (внутри него уже отправленная tx отдаётся как FATAL_UNKNOWN_STATUS).
            # _swap_fast_impl свои исключения обрабатывает сам и сюда не пропускает.
            print(f"[{self.pair}] swap_fast_async: непредвиденная ошибка до отправки: {e}")
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                              error_msg=f"Своп не отправлен: {e}")

    async def _swap_fast_impl(self, token_in_addr: str, token_out_addr: str, amount_in_human: float,
                              is_buy: bool, decimals_in: int, decimals_out: int,
                              slippage: float) -> SwapResult:
        """Страховка: ЛЮБОЕ исключение превращается в SwapResult.

        Вызывающий код в trade.py разбирает исход сделки только по SwapResult - если
        отсюда вылетит исключение, оно уйдёт в общий except _make_trade_impl, а
        handle_swap не вызовется вообще: нога на MEXC останется исполненной и
        незахеджированной, без отката и без уведомления.

        Тип ошибки выбирается по тому, была ли транзакция уже ОТПРАВЛЕНА:
        после отправки её итог неизвестен, и автоматически закрывать позицию нельзя
        (см. FATAL_UNKNOWN_STATUS), до отправки - своп точно не состоялся и откат
        безопасен."""
        sent = {'tx_hash': None}
        try:
            return await self._swap_fast_unguarded(
                token_in_addr, token_out_addr, amount_in_human,
                is_buy, decimals_in, decimals_out, slippage, sent)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if sent['tx_hash']:
                print(f"[{self.pair}] swap_fast: исключение ПОСЛЕ отправки tx={sent['tx_hash']}: {e}")
                return SwapResult(success=False, error_type=SwapErrorType.FATAL_UNKNOWN_STATUS,
                                  tx_hash=sent['tx_hash'],
                                  error_msg=f"Исключение после отправки tx {sent['tx_hash']}: {e}")
            print(f"[{self.pair}] swap_fast: исключение ДО отправки транзакции: {e}")
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                              error_msg=f"Своп не отправлен: {e}")

    async def _swap_fast_unguarded(self, token_in_addr: str, token_out_addr: str, amount_in_human: float,
                                   is_buy: bool, decimals_in: int, decimals_out: int,
                                   slippage: float, sent: dict) -> SwapResult:
        t_start = time()

        amount_in_raw = self._to_raw(amount_in_human, decimals_in)
        if amount_in_raw <= 0:
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                              error_msg=f"amount_in={amount_in_human} слишком мал для {decimals_in} decimals")

        est_out_raw, est_source = await self._estimate_amount_out_raw(
            amount_in_human, amount_in_raw, is_buy, token_in_addr, token_out_addr, decimals_out)
        if not est_out_raw or est_out_raw <= 0:
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_LIQUIDITY,
                              error_msg=f"не удалось оценить выход свопа (источник {est_source})")
        amount_out_min_raw = max(1, int(est_out_raw * (1 - slippage)))

        data = await self._build_swap_calldata(token_in_addr, token_out_addr,
                                               amount_in_raw, amount_out_min_raw)
        if data is None:
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                              error_msg="не удалось собрать calldata свопа")

        gas_price = await self._gas_price_for_swap()
        if gas_price is None:
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_NO_GAS,
                              error_msg="нет цены газа")
        gas_limit = SWAP_GAS_LIMIT_V3 if self.pool_kind == 'v3' else SWAP_GAS_LIMIT_V2

        tx_hash = None
        tx_hash_hex: Optional[str] = None
        max_send_attempts = max(1, int(SWAP_FAST_MAX_SEND_RETRIES))
        for attempt in range(max_send_attempts):
            if self._next_nonce is None and await self._sync_nonce() is None:
                return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                                  error_msg="не удалось получить nonce")
            nonce = self._next_nonce

            txn = {
                'chainId': self._chain_id,
                'from': self.from_addr,
                'to': self.router_addr,
                'value': 0,
                'gas': gas_limit,
                'gasPrice': gas_price,
                'nonce': nonce,
                'data': data,
            }
            signed = await asyncio.to_thread(Account.sign_transaction, txn, self.private_key)
            # Хеш известен ДО отправки и зависит только от подписи - поэтому даже при
            # обрыве связи на send мы точно знаем, что именно искать в сети.
            tx_hash = signed.hash
            tx_hash_hex = tx_hash.hex()

            try:
                await self._send_raw_tx(signed.raw_transaction)
                # С этого момента транзакция в сети: любая последующая ошибка
                # означает НЕИЗВЕСТНЫЙ итог, а не безопасный отказ - см. _swap_fast_impl.
                sent['tx_hash'] = tx_hash_hex
                self._next_nonce = nonce + 1
                break
            except Exception as e:
                msg = str(e).lower()
                if 'already known' in msg or 'known transaction' in msg:
                    # Та же самая транзакция уже в мемпуле - отправлять нечего,
                    # просто дожидаемся её receipt.
                    sent['tx_hash'] = tx_hash_hex
                    self._next_nonce = nonce + 1
                    break
                if ('nonce' in msg or 'replacement' in msg) and attempt < max_send_attempts - 1:
                    # Локальный счётчик разошёлся с сетью. Ресинк и НЕМЕДЛЕННЫЙ повтор:
                    # sleep здесь означал бы, что нога на MEXC висит незахеджированной.
                    print(f"[{self.pair}] swap_fast: nonce разошёлся ({e}), ресинк и повтор")
                    await self._sync_nonce()
                    continue
                await self._sync_nonce()
                return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                                  error_msg=f"send failed: {e}")
        else:
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                              error_msg="не удалось отправить транзакцию")

        sent_at = time()
        try:
            receipt = await asyncio.wait_for(
                self.rpc.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=SWAP_RECEIPT_TIMEOUT_SECONDS, poll_latency=0.1),
                timeout=SWAP_RECEIPT_TIMEOUT_SECONDS + 5)
        except Exception as e:
            # Транзакция ОТПРАВЛЕНА, итог неизвестен. Переотправлять нельзя (создали бы
            # второй реальный своп), автоматически откатывать позицию на MEXC - тоже
            # (см. FATAL_UNKNOWN_STATUS). Нужна ручная проверка по хешу.
            print(f"[{self.pair}] swap_fast: receipt не получен за {SWAP_RECEIPT_TIMEOUT_SECONDS}s "
                  f"(tx={tx_hash_hex}): {e}")
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_UNKNOWN_STATUS,
                              tx_hash=tx_hash_hex,
                              error_msg=f"Статус tx {tx_hash_hex} неизвестен (таймаут receipt): {e}")

        if receipt.status != 1:
            # Реверт почти всегда означает, что цена ушла и amountOutMin не выполнился,
            # то есть возможности больше нет. Повтор с увеличенным слиппеджем (как в
            # swap_universal_async) здесь сознательно НЕ делается - см. SWAP_FAST_SLIPPAGE.
            print(f"[{self.pair}] swap_fast: РЕВЕРТ tx={tx_hash_hex}, "
                  f"amount_out_min={amount_out_min_raw} (оценка {est_source}), "
                  f"{time() - sent_at:.2f}s в сети")
            return SwapResult(success=False, error_type=SwapErrorType.FATAL_TOKEN_ISSUE,
                              tx_hash=tx_hash_hex,
                              error_msg=f"Swap reverted: {tx_hash_hex}")

        # Комиссия реально использованного пула - для _calc_buy_mecx_fee в trade.py,
        # так же как это делает swap_universal_async.
        self.last_fee_rate = (self.v3_fee_ppm / 1_000_000) if self.pool_kind == 'v3' else DEFAULT_PANCAKE_FEE_RATE
        # Фактический расход газа - туда же, в оценку стоимости следующего хеджа.
        self.last_gas_used = int(receipt.gasUsed)
        print(f"[{self.pair}] swap_fast OK tx={tx_hash_hex} | подготовка {sent_at - t_start:.3f}s "
              f"(оценка {est_source}) | подтверждение {time() - sent_at:.2f}s | "
              f"gas_price {gas_price / 1e9:.3f} gwei | gas_used {receipt.gasUsed}")
        return SwapResult(success=True, tx_hash=tx_hash_hex,
                          gas_used=int(receipt.gasUsed), gas_price_wei=int(gas_price),
                          amount_out_est_raw=est_out_raw, quote_source=est_source)
