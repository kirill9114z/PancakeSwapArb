"""OkxTrade - объект одного пула PancakeSwap, по одному на торговую пару.

Здесь остались только сборка объекта (__init__), HTTP-сессия и главный цикл
monitoring_price: подписка на события Swap по вебсокету, watchdog устаревшей
цены и фоновое поддержание прогретого состояния для быстрого свопа.

Остальное разнесено по ответственностям и подмешивается миксинами:
    arb.dex.pool_state      - чтение цены и резервов пула, обработка Swap-событий;
    arb.dex.allowances      - approve роутеру;
    arb.dex.quoting         - quote_local / confirm_price_onchain;
    arb.dex.swap_fast       - быстрый своп через известный пул;
    arb.dex.swap_universal  - универсальный своп с поиском маршрута (фоллбэк).
"""
import asyncio
from time import time
from typing import Optional

import aiohttp
import websockets
from web3 import AsyncHTTPProvider, AsyncWeb3, Web3

from arb.paths import ERC20_ABI_PATH, ROUTER_ABI_PATH, load_json, pair_abi_path
from config import (
    DEX_BUY_MARKUP,
    DEX_SELL_MARKDOWN,
    DEX_V3_LIQUIDITY_RESYNC_SECONDS,
    PANCAKE_SMART_ROUTER,
    STALE_PRICE_SECONDS,
    SWAP_GAS_PRICE_REFRESH_SECONDS,
    USDT_CONTRACT,
    WBNB_ADDRESS,
)

from .allowances import RouterAllowanceMixin
from .pool_state import PoolStateMixin
from .quoting import LocalQuoteMixin
from .swap_fast import FastSwapMixin
from .swap_universal import UniversalSwapMixin
from .types import SwapErrorType, SwapResult  # noqa: F401  (реэкспорт для старых импортов)

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


class OkxTrade(PoolStateMixin, RouterAllowanceMixin, LocalQuoteMixin,
               FastSwapMixin, UniversalSwapMixin):
    """Один пул PancakeSwap: цена по WS, котировки под объём, исполнение свопа."""

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
        self.smart_router_abi = load_json(ROUTER_ABI_PATH)
        self.router = self.rpc.eth.contract(address=self.router_addr, abi=self.smart_router_abi)

        self.erc20_abi = load_json(ERC20_ABI_PATH)
        self.WBNB = Web3.to_checksum_address(WBNB_ADDRESS)

        self.abi = load_json(pair_abi_path(self.pair))

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
