import json
from time import time
from typing import Tuple, List, Optional
from enum import Enum

import aiohttp
from web3 import AsyncWeb3, AsyncHTTPProvider, Web3
from eth_account import Account

import asyncio
from decimal import Decimal, getcontext, localcontext
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
    DEX_LOCAL_PROJECTION_ENABLED,
    DEX_V3_IMPACT_CONFIRM_THRESHOLD_PCT,
    DEX_ONCHAIN_CONFIRM_TIMEOUT_SECONDS,
    DEX_V3_LIQUIDITY_RESYNC_SECONDS,
    SWAP_FAST_PATH_ENABLED,
    SWAP_FAST_SLIPPAGE,
    SWAP_DEADLINE_SECONDS,
    SWAP_STRICT_NEXT_BLOCK,
    SWAP_GAS_PRICE_PREMIUM,
    SWAP_GAS_PRICE_MAX_GWEI,
    SWAP_GAS_PRICE_REFRESH_SECONDS,
    SWAP_GAS_PRICE_MAX_AGE_SECONDS,
    SWAP_GAS_LIMIT_V3,
    SWAP_GAS_LIMIT_V2,
    SWAP_RECEIPT_TIMEOUT_SECONDS,
    SWAP_PRIVATE_TX_RPC,
    SWAP_FAST_MAX_SEND_RETRIES,
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
    # Транзакция ОТПРАВЛЕНА, но её итог неизвестен (таймаут ожидания receipt, обрыв
    # связи). Принципиально отличается от остальных FATAL_*: своп мог и пройти, и не
    # пройти. Вызывающий код НЕ должен на это автоматически закрывать позицию обратно
    # на MEXC - если своп всё-таки исполнится, обратная сделка создаст вторую
    # (уже никем не захеджированную) позицию вместо того чтобы закрыть первую.
    # Правильная реакция - громкий алерт и ручная проверка по tx_hash.
    FATAL_UNKNOWN_STATUS = "fatal_unknown_status"


class SwapResult:
    def __init__(self, success: bool, tx_hash: Optional[str] = None,
                 error_type: SwapErrorType = SwapErrorType.SUCCESS,
                 error_msg: str = ""):
        self.success = success
        self.tx_hash = tx_hash
        self.error_type = error_type
        self.error_msg = error_msg


class OkxTrade:
    def __init__(self, pair, address, abi, dec2, rak, private_key, wss, rpc, db, token_contract=None):
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
        # Реальный контракт ТОРГУЕМОГО токена этой конкретной пары (contract_bsc из БД).
        # Используется как основной способ понять, какой из token0/token1 пула -
        # "стейбл" (см. side()/_init_local_quote_state): раньше это определялось ТОЛЬКО
        # сверкой с глобальной константой USDT_CONTRACT, а у пула на деле может быть
        # другой стейблкоин (напр. реальный Binance-Peg USDT вместо USD1 из конфига) -
        # тогда сверка молча проваливалась (side() -> None -> monitoring_price() падал
        # в return сразу после первого чтения цены, WS-подписка на Swap вообще не
        # стартовала, self.rak/buy/sell замирали навсегда). Если token_contract не
        # передан - логика откатывается на старое поведение (сверка с USDT_CONTRACT).
        self.token_contract = token_contract


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

        # === Локальная (без RPC) проекция цены под объём сделки, см. quote_local() ===
        # Тип пула, за которым мы наблюдаем через WS ('v3' или 'v2') - определяется один
        # раз в _init_local_quote_state() по наличию slot0() в ABI, как и в get_rak().
        self.pool_kind: Optional[str] = None
        self.token0_addr: Optional[str] = None
        self.token1_addr: Optional[str] = None
        self.usdt_is_token0: Optional[bool] = None
        # V3: комиссия пула (millionths, напр. 500 = 0.05%) и текущее состояние тика.
        self.v3_fee_ppm: Optional[int] = None
        self.v3_sqrt_price_x96: Optional[int] = None
        self.v3_liquidity: Optional[int] = None
        self.v3_tick: Optional[int] = None
        # V2: реальные резервы пула (raw, wei), обновляются инкрементально по Swap-событиям.
        self.v2_reserve0_raw: Optional[int] = None
        self.v2_reserve1_raw: Optional[int] = None
        self._local_quote_ready = False
        self._last_liquidity_resync_ts = 0.0
        self._logged_fallback_reasons: set = set()

        # === Прогретое состояние для быстрого свопа, см. swap_fast_async() ==============
        # Всё, что swap_universal_async спрашивает у сети ПЕРЕД КАЖДЫМ свопом, хотя оно
        # либо неизменно, либо меняется медленно. Заполняется один раз в
        # prepare_fast_swap() при старте пары и поддерживается в цикле monitoring_price,
        # то есть вне горячего пути отправки сделки.
        self._fast_ready = False
        self._chain_id: Optional[int] = None
        # decimals ERC20 неизменны - спрашивать их перед каждым свопом бессмысленно.
        self._decimals_cache: dict = {}
        # Локальный счётчик nonce: eth_getTransactionCount в горячем пути - это лишний
        # round-trip, а с block='latest' (как в swap_universal_async) он ещё и не видит
        # свои же pending-транзакции. Ресинк - в prepare_fast_swap, после ошибок
        # отправки и фоново в monitoring_price, когда своп не выполняется.
        self._next_nonce: Optional[int] = None
        self._gas_price_wei: Optional[int] = None
        self._gas_price_ts: float = 0.0
        # Отдельный клиент для приватной (MEV-защищённой) отправки транзакций, если
        # SWAP_PRIVATE_TX_RPC задан. Используется ТОЛЬКО для eth_sendRawTransaction.
        self._private_w3: Optional[AsyncWeb3] = None


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

        # Основной способ: сверка с реальным контрактом ТОРГУЕМОГО токена этой пары
        # (contract_bsc из БД) - надёжно независимо от того, какой именно стейблкоин
        # использует конкретный пул. Раньше здесь была ТОЛЬКО сверка с глобальной
        # USDT_ADDRESS - если у пула стейбл отличался от захардкоженного в конфиге
        # (см. историю бага для LAB/USDT: пул на реальном USDT, а конфиг ждал другой
        # адрес), эта функция возвращала None, и monitoring_price() падал в return
        # сразу после первого чтения цены, даже не подписавшись на Swap-события.
        token_lc = self.token_contract.lower() if self.token_contract else None
        if token_lc and (token0.lower() == token_lc or token1.lower() == token_lc):
            return False

        # Фоллбэк на старое поведение, если token_contract не передан.
        if token1 == self.USDT_ADDRESS or token0 == self.USDT_ADDRESS:
            return False

        return None


    async def sqrtPriceX96_to_price(self, sqrtPriceX96: int) -> Decimal:
        sqrt_price = Decimal(sqrtPriceX96) / (Decimal(2) ** 96)
        price = sqrt_price * sqrt_price
        return price

    async def adjust_for_decimals(self, price, dec0: int, dec1: int) -> Decimal:
        exp = dec0 - dec1
        return price * (Decimal(10) ** exp)

    async def handle_event(self, e, side1):
        # Диспетчер по типу пула - раньше эта функция безусловно читала
        # args["sqrtPriceX96"] (V3-only), из-за чего для V2-пары она бы падала на
        # каждом Swap-событии (KeyError) и цена/резервы никогда бы не обновлялись
        # после первого чтения при старте. Теперь два явных пути.
        if self.pool_kind == 'v2':
            await self._handle_v2_swap_event(e["args"])
        else:
            await self._handle_v3_swap_event(e["args"], side1)

    async def _handle_v3_swap_event(self, args, side1):
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

            # Кэшируем свежее состояние тика для quote_local() - тот же outlier-гейт,
            # что и для self.rak, чтобы не подмешивать в локальную модель выброс.
            if self._local_quote_ready:
                self.v3_sqrt_price_x96 = sqrtPriceX96
                self.v3_liquidity = args.get('liquidity', self.v3_liquidity)
                self.v3_tick = args.get('tick', self.v3_tick)

    async def _handle_v2_swap_event(self, args):
        # Стандартный V2 Swap: amount0In/amount1In/amount0Out/amount1Out - дельты
        # резервов пары. Обновляем резервы инкрементально (без лишнего RPC) и
        # пересчитываем rak/buy/sell из них же, тем же способом, что get_rak()
        # использует при старте.
        if self.v2_reserve0_raw is None or self.v2_reserve1_raw is None:
            return
        amount0_in = args.get('amount0In', 0)
        amount1_in = args.get('amount1In', 0)
        amount0_out = args.get('amount0Out', 0)
        amount1_out = args.get('amount1Out', 0)
        new_reserve0 = self.v2_reserve0_raw + amount0_in - amount0_out
        new_reserve1 = self.v2_reserve1_raw + amount1_in - amount1_out
        if new_reserve0 <= 0 or new_reserve1 <= 0:
            return

        price_token1_in_token0 = (new_reserve0 / (10 ** self.decimals_in)) / (new_reserve1 / (10 ** self.decimals_out))
        price_corr = float(1 / price_token1_in_token0)

        if abs(price_corr - self.rak) <= self.rak * PRICE_OUTLIER_THRESHOLD:
            self.v2_reserve0_raw = new_reserve0
            self.v2_reserve1_raw = new_reserve1
            self.last_price_update_ts = time()
            self._stale_notified = False
            self.rak = price_corr
            self.buy = price_corr * DEX_BUY_MARKUP
            self.sell = price_corr * DEX_SELL_MARKDOWN
        # Если отклонение похоже на выброс - резервы НЕ обновляем (событие
        # отбрасывается целиком, включая эффект на локальные резервы), иначе
        # разовая аномалия постоянно испортит базу для следующих котировок.

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

    # === Локальная (без RPC) проекция DEX-цены под объём сделки =========================
    # Идея: self.rak/self.buy/self.sell - это mid-цена пула ± плоский маркап, одинаковый
    # для любого объёма. Реальный price impact в AMM растёт нелинейно с объёмом
    # относительно ликвидности, поэтому для сравнения с конкретным уровнем стакана MEXC
    # (конкретный объём) нужна цена, спроецированная именно под этот объём.
    # quote_local() считает её без сети по состоянию, которое и так уже трекается через
    # WS (резервы V2 / L+sqrtPriceX96 V3). confirm_price_onchain() - тяжёлая, но точная
    # проверка одним RPC-вызовом, вызывается только под победившего кандидата.

    async def _init_local_quote_state(self, pool):
        try:
            token0 = await pool.functions.token0().call()
            token1 = await pool.functions.token1().call()
            self.token0_addr = token0
            self.token1_addr = token1

            # Основной способ определить, какой токен - "стейбл": сверка с реальным
            # контрактом ТОРГУЕМОГО токена этой пары (token_contract = contract_bsc из
            # БД). Надёжно независимо от того, какой стейблкоин использует конкретный
            # пул - в отличие от сверки с одной глобальной USDT_CONTRACT (см. side()).
            token_lc = self.token_contract.lower() if self.token_contract else None
            usdt_lc = self.USDT_ADDRESS.lower()
            if token_lc and token0.lower() == token_lc:
                self.usdt_is_token0 = False   # token0 - сам торгуемый токен -> token1 - стейбл
            elif token_lc and token1.lower() == token_lc:
                self.usdt_is_token0 = True    # token1 - сам торгуемый токен -> token0 - стейбл
            elif token0.lower() == usdt_lc:
                self.usdt_is_token0 = True
            elif token1.lower() == usdt_lc:
                self.usdt_is_token0 = False
            else:
                print(f"[{self.pair}] _init_local_quote_state: не удалось определить стейбл-сторону пула "
                      f"(token_contract={self.token_contract}, USDT_CONTRACT={self.USDT_ADDRESS}, "
                      f"token0={token0}, token1={token1}) - локальная проекция цены отключена для этой пары")
                return

            has_slot0 = any(item.get("type") == "function" and item.get("name") == "slot0" for item in self.abi)

            if has_slot0:
                self.pool_kind = 'v3'
                self.v3_fee_ppm = await pool.functions.fee().call()
                self.v3_liquidity = await pool.functions.liquidity().call()
                slot0_data = await pool.functions.slot0().call()
                self.v3_sqrt_price_x96 = slot0_data[0]
                self.v3_tick = slot0_data[1] if len(slot0_data) > 1 else None
                self._last_liquidity_resync_ts = time()
            else:
                self.pool_kind = 'v2'
                r0, r1, _ = await pool.functions.getReserves().call()
                self.v2_reserve0_raw = r0
                self.v2_reserve1_raw = r1

            self._local_quote_ready = True
            print(f"[{self.pair}] Локальная проекция цены готова: pool_kind={self.pool_kind}, "
                  f"usdt_is_token0={self.usdt_is_token0}")
        except Exception as e:
            print(f"[{self.pair}] _init_local_quote_state failed: {e} - локальная проекция цены отключена, "
                  f"analyze_opportunities будет использовать плоский buy/sell как раньше")
            self._local_quote_ready = False

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

    async def _resync_v3_liquidity(self, pool):
        try:
            self.v3_liquidity = await pool.functions.liquidity().call()
            self._last_liquidity_resync_ts = time()
        except Exception as e:
            print(f"[{self.pair}] liquidity() resync failed: {e}")

    def _virtual_v3_reserves_human(self) -> Optional[Tuple[float, float]]:
        """"Виртуальные" резервы V3-пула в пределах ТЕКУЩЕГО тика: x=L/sqrtP, y=L*sqrtP.
        Внутри активного тика V3-пул математически неотличим от V2-пары с такими
        резервами (Uniswap V3 whitepaper, 6.1) - это и позволяет переиспользовать одну
        constant-product формулу для V2 и V3. За пределами тика точность падает, поэтому
        quote_local() дополнительно считает impact_pct и просит on-chain подтверждение
        при большом отклонении."""
        if self.v3_sqrt_price_x96 is None or self.v3_liquidity is None or self.usdt_is_token0 is None:
            return None
        try:
            sqrt_p = Decimal(self.v3_sqrt_price_x96) / (Decimal(2) ** 96)
            if sqrt_p <= 0:
                return None
            L = Decimal(self.v3_liquidity)
            reserve0_raw = L / sqrt_p
            reserve1_raw = L * sqrt_p
            dec0, dec1 = (18, self.decimals_out) if self.usdt_is_token0 else (self.decimals_out, 18)
            reserve0_human = float(reserve0_raw / (Decimal(10) ** dec0))
            reserve1_human = float(reserve1_raw / (Decimal(10) ** dec1))
        except (ArithmeticError, ValueError, TypeError):
            return None
        return (reserve0_human, reserve1_human) if self.usdt_is_token0 else (reserve1_human, reserve0_human)

    def _v2_reserves_human(self) -> Optional[Tuple[float, float]]:
        if self.v2_reserve0_raw is None or self.v2_reserve1_raw is None or self.usdt_is_token0 is None:
            return None
        dec0, dec1 = (18, self.decimals_out) if self.usdt_is_token0 else (self.decimals_out, 18)
        r0 = self.v2_reserve0_raw / (10 ** dec0)
        r1 = self.v2_reserve1_raw / (10 ** dec1)
        return (r0, r1) if self.usdt_is_token0 else (r1, r0)

    @staticmethod
    def _constant_product_quote(reserve_in: float, reserve_out: float, amount_in: float, fee_rate: float) -> float:
        """x*y=k с комиссией на входе. Используется и для реальных резервов V2, и для
        "виртуальных" резервов V3 в пределах текущего тика - см. _virtual_v3_reserves_human."""
        if reserve_in <= 0 or reserve_out <= 0 or amount_in <= 0:
            return 0.0
        amount_in_after_fee = amount_in * (1 - fee_rate)
        denom = reserve_in + amount_in_after_fee
        if denom <= 0:
            return 0.0
        return (amount_in_after_fee * reserve_out) / denom

    def _log_fallback_once(self, reason_key: str, message: str):
        if reason_key in self._logged_fallback_reasons:
            return
        self._logged_fallback_reasons.add(reason_key)
        print(f"[{self.pair}] quote_local -> fallback на плоский buy/sell ({reason_key}): {message}")

    def quote_local(self, amount_in_human: float, is_buy: bool) -> Optional[dict]:
        """Локальная (без RPC) оценка эффективной DEX-цены под конкретный объём.
        is_buy=True: тратим amount_in_human USDT, получаем токен (аналог self.buy).
        is_buy=False: тратим amount_in_human токена, получаем USDT (аналог self.sell).
        Возвращает None, если состояние ещё не готово (в analyze_opportunities в этом
        случае нужно откатиться на плоский self.buy/self.sell как раньше). Каждая
        причина возврата None логируется РОВНО ОДИН РАЗ (см. _log_fallback_once) -
        иначе она молча тонет в потоке 'BEST: ...' из analyze_opportunities и создаёт
        впечатление, что новая логика работает, хотя на деле всегда идёт fallback."""
        if not DEX_LOCAL_PROJECTION_ENABLED:
            return None
        if not self._local_quote_ready:
            self._log_fallback_once('not_ready', "_local_quote_ready=False (см. сообщение "
                                     "_init_local_quote_state failed / 'ни token0 ни token1' в логах при старте)")
            return None
        if amount_in_human <= 0:
            return None

        if self.pool_kind == 'v3':
            reserves = self._virtual_v3_reserves_human()
            fee_rate = (self.v3_fee_ppm / 1_000_000) if self.v3_fee_ppm is not None else DEFAULT_PANCAKE_FEE_RATE
        else:
            reserves = self._v2_reserves_human()
            fee_rate = DEFAULT_PANCAKE_FEE_RATE

        if reserves is None:
            self._log_fallback_once('no_reserves', f"резервы недоступны (pool_kind={self.pool_kind}, "
                                     f"v3_liquidity={self.v3_liquidity}, v3_sqrt_price_x96={self.v3_sqrt_price_x96}, "
                                     f"v2_reserve0={self.v2_reserve0_raw}, v2_reserve1={self.v2_reserve1_raw})")
            return None
        reserve_usdt, reserve_token = reserves
        if reserve_usdt <= 0 or reserve_token <= 0:
            self._log_fallback_once('bad_reserves', f"резервы <= 0 (usdt={reserve_usdt}, token={reserve_token})")
            return None
        mid_price = reserve_usdt / reserve_token

        if is_buy:
            amount_out = self._constant_product_quote(reserve_usdt, reserve_token, amount_in_human, fee_rate)
            if amount_out <= 0:
                return None
            effective_price = amount_in_human / amount_out
        else:
            amount_out = self._constant_product_quote(reserve_token, reserve_usdt, amount_in_human, fee_rate)
            if amount_out <= 0:
                return None
            effective_price = amount_out / amount_in_human

        # impact_pct считаем ДО буфера DEX_BUY_MARKUP/SELL_MARKDOWN - это чистый AMM
        # price impact от объёма сделки. Раньше буфер попадал в impact_pct раньше этой
        # строки, из-за чего impact был искусственно завышен на фиксированную величину
        # буфера независимо от объёма - искажало и сравнение с порогом подтверждения,
        # и диагностику (impact_pct переставал отражать реальный размер проскальзывания).
        impact_pct = abs(effective_price - mid_price) / mid_price * 100 if mid_price > 0 else 0.0
        needs_confirmation = self.pool_kind == 'v3' and impact_pct >= DEX_V3_IMPACT_CONFIRM_THRESHOLD_PCT
        effective_price *= DEX_BUY_MARKUP if is_buy else DEX_SELL_MARKDOWN

        return {
            'amount_out': amount_out,
            'effective_price': effective_price,
            'mid_price': mid_price,
            'impact_pct': impact_pct,
            'needs_confirmation': needs_confirmation,
            'pool_kind': self.pool_kind,
        }

    async def confirm_price_onchain(self, amount_in_human: float, is_buy: bool,
                                     timeout: float = DEX_ONCHAIN_CONFIRM_TIMEOUT_SECONDS) -> Optional[float]:
        """Единичный, жёстко ограниченный по времени on-chain запрос реальной котировки
        под конкретный объём - подтверждение quote_local() ПЕРЕД тем как брать сделку в
        работу (а не после, как в swap_universal_async на этапе реального свопа).
        Намеренно бьёт только в известный пул/fee-тир (без перебора маршрутов, как в
        swap_universal_async), чтобы уложиться в маленький таймаут.
        Возвращает None при таймауте/ошибке - вызывающий код обязан пропустить сделку
        на этой итерации, а не доверять неподтверждённой локальной модели."""
        if self.token0_addr is None or self.token1_addr is None or self.usdt_is_token0 is None:
            return None
        if amount_in_human <= 0:
            return None

        token_usdt = self.token0_addr if self.usdt_is_token0 else self.token1_addr
        token_other = self.token1_addr if self.usdt_is_token0 else self.token0_addr
        if is_buy:
            token_in, token_out, decimals_in, decimals_out = token_usdt, token_other, 18, self.decimals_out
        else:
            token_in, token_out, decimals_in, decimals_out = token_other, token_usdt, self.decimals_out, 18

        amount_in_raw = int(amount_in_human * (10 ** decimals_in))
        if amount_in_raw <= 0:
            return None

        async def _call():
            token_in_addr = Web3.to_checksum_address(token_in)
            token_out_addr = Web3.to_checksum_address(token_out)
            if self.pool_kind == 'v3' and self.v3_fee_ppm is not None:
                params = (token_in_addr, token_out_addr, self.v3_fee_ppm, self.from_addr, amount_in_raw, 1, 0)
                res = await self.router.functions.exactInputSingle(params).call({'from': self.from_addr})
            else:
                path = [token_in_addr, token_out_addr]
                res = await self.router.functions.swapExactTokensForTokens(
                    amount_in_raw, 1, path, self.from_addr
                ).call({'from': self.from_addr})
            return int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)

        try:
            amount_out_raw = await asyncio.wait_for(_call(), timeout=timeout)
        except Exception as e:
            print(f"[{self.pair}] confirm_price_onchain: {e}")
            return None

        amount_out_human = amount_out_raw / (10 ** decimals_out)
        if amount_out_human <= 0:
            return None
        return (amount_in_human / amount_out_human) if is_buy else (amount_out_human / amount_in_human)

    async def monitoring_price(self):
        self.backoff = 1
        print("Start 2")

        pool = self.rpc.eth.contract(address=self.address, abi=self.abi)
        side = await self.side(pool)
        self.rak = await self.get_rak(side, pool)
        # Инициализируем состояние для локальной (без RPC) проекции цены под объём -
        # см. quote_local(). Делается один раз при старте; не критично для базовой
        # работы бота, поэтому ошибка тут только гасит фичу (_local_quote_ready=False),
        # а не валит мониторинг цены.
        await self._init_local_quote_state(pool)
        # Разовый (навсегда, между перезапусками бота allowance сохраняется в сети)
        # approve роутеру на оба токена пула - без него confirm_price_onchain падает
        # с revert 'STF' (TransferHelper.safeTransferFrom требует allowance даже для
        # read-only .call()), а реальный своп в swap_universal_async всё равно
        # потребовал бы approve при первой сделке. ВНИМАНИЕ: это реальная on-chain
        # транзакция (небольшой газ в BNB) - отправляется независимо от test_mode,
        # т.к. test_mode влияет только на решение "торговать или нет" в trade.py,
        # а не на подготовительные шаги здесь.
        await self._ensure_router_allowances()
        # Прогреваем всё, что иначе пришлось бы спрашивать у сети перед каждым свопом
        # (chainId, decimals, nonce, цена газа, приватный релей) - см. swap_fast_async.
        # Обязательно ПОСЛЕ _init_local_quote_state и _ensure_router_allowances:
        # быстрый путь опирается на pool_kind/v3_fee_ppm/token0/token1 и на уже
        # выданный роутеру allowance.
        await self.prepare_fast_swap()
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

                    # Периодический ресинк liquidity() для V3 - Swap-события несут
                    # свежие sqrtPriceX96/liquidity/tick, но не ловят изменение
                    # ликвидности от Mint/Burn (добавление/вывод LP), на которые
                    # бот не подписан. Раз в DEX_V3_LIQUIDITY_RESYNC_SECONDS сверяем
                    # через отдельный RPC-вызов, вне горячего пути анализа сделок.
                    if (self.pool_kind == 'v3' and self._local_quote_ready
                            and (time() - self._last_liquidity_resync_ts) > DEX_V3_LIQUIDITY_RESYNC_SECONDS):
                        await self._resync_v3_liquidity(pool)

                    # Фоновое поддержание состояния быстрого свопа. Смысл ровно в том,
                    # чтобы эти два RPC-запроса случались ЗДЕСЬ, а не в горячем пути
                    # отправки сделки. Nonce трогаем только когда своп не выполняется:
                    # внутри _swap_lock счётчик ведёт сам swap_fast_async, и ресинк
                    # посреди отправки затёр бы его.
                    if self._fast_ready and (time() - self._gas_price_ts) > SWAP_GAS_PRICE_REFRESH_SECONDS:
                        await self._refresh_gas_price()
                        if not self._swap_lock.locked():
                            await self._sync_nonce()

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
        print(f"[{self.pair}] swap_fast OK tx={tx_hash_hex} | подготовка {sent_at - t_start:.3f}s "
              f"(оценка {est_source}) | подтверждение {time() - sent_at:.2f}s | "
              f"gas_price {gas_price / 1e9:.3f} gwei | gas_used {receipt.gasUsed}")
        return SwapResult(success=True, tx_hash=tx_hash_hex)
