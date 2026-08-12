"""Чтение и поддержание состояния наблюдаемого пула PancakeSwap.

Отвечает за один вопрос: "какая сейчас цена в пуле и какие у него резервы".
Сюда входит определение стороны пула (side / usdt_is_token0), первое чтение цены
(get_rak), применение WS-событий Swap к локальному состоянию (V2-резервы,
V3 sqrtPriceX96/liquidity/tick) и периодический ресинк ликвидности.

Сам WS-цикл живёт в OkxTrade.monitoring_price (arb.dex.pancake) - здесь только
обработка того, что этот цикл принёс.
"""
from decimal import Decimal
from time import time

from config import (
    DEX_BUY_MARKUP,
    DEX_SELL_MARKDOWN,
    PRICE_OUTLIER_THRESHOLD,
)


class PoolStateMixin:
    """Состояние пула. Подмешивается в OkxTrade, все атрибуты создаются в его __init__."""

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

    async def _resync_v3_liquidity(self, pool):
        try:
            self.v3_liquidity = await pool.functions.liquidity().call()
            self._last_liquidity_resync_ts = time()
        except Exception as e:
            print(f"[{self.pair}] liquidity() resync failed: {e}")
