"""Котировка DEX под конкретный объём сделки.

Два уровня: quote_local() считает эффективную цену локально, без единого
сетевого запроса, по состоянию пула из arb.dex.pool_state; confirm_price_onchain()
делает один ограниченный по времени eth_call, когда локальной модели доверять
нельзя. Плоские self.buy/self.sell (mid-цена ± маркап) остаются фоллбэком.
"""
import asyncio
from decimal import Decimal
from typing import Optional, Tuple

from web3 import Web3

from config import (
    DEFAULT_PANCAKE_FEE_RATE,
    DEX_BUY_MARKUP,
    DEX_LOCAL_PROJECTION_ENABLED,
    DEX_ONCHAIN_CONFIRM_TIMEOUT_SECONDS,
    DEX_SELL_MARKDOWN,
    DEX_V3_IMPACT_CONFIRM_THRESHOLD_PCT,
)


class LocalQuoteMixin:
    """Локальная и on-chain котировка. Подмешивается в OkxTrade."""

    # === Локальная (без RPC) проекция DEX-цены под объём сделки =========================
    # Идея: self.rak/self.buy/self.sell - это mid-цена пула ± плоский маркап, одинаковый
    # для любого объёма. Реальный price impact в AMM растёт нелинейно с объёмом
    # относительно ликвидности, поэтому для сравнения с конкретным уровнем стакана MEXC
    # (конкретный объём) нужна цена, спроецированная именно под этот объём.
    # quote_local() считает её без сети по состоянию, которое и так уже трекается через
    # WS (резервы V2 / L+sqrtPriceX96 V3). confirm_price_onchain() - тяжёлая, но точная
    # проверка одним RPC-вызовом, вызывается только под победившего кандидата.

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
            # Цена при объёме, стремящемся к нулю: комиссия пула уже учтена, price impact
            # ещё нет. Это и есть точка отсчёта для impact_pct - см. комментарий ниже.
            reference_price = mid_price / (1 - fee_rate) if fee_rate < 1 else mid_price
        else:
            amount_out = self._constant_product_quote(reserve_token, reserve_usdt, amount_in_human, fee_rate)
            if amount_out <= 0:
                return None
            effective_price = amount_out / amount_in_human
            reference_price = mid_price * (1 - fee_rate)

        # impact_pct считаем ДО буфера DEX_BUY_MARKUP/SELL_MARKDOWN - это чистый AMM
        # price impact от объёма сделки. Раньше буфер попадал в impact_pct раньше этой
        # строки, из-за чего impact был искусственно завышен на фиксированную величину
        # буфера независимо от объёма - искажало и сравнение с порогом подтверждения,
        # и диагностику (impact_pct переставал отражать реальный размер проскальзывания).
        #
        # ВТОРАЯ (более дорогая) версия той же ошибки: точкой отсчёта был mid_price, то есть
        # цена БЕЗ комиссии пула, а effective_price комиссию уже содержит (она вшита в
        # _constant_product_quote через amount_in * (1 - fee_rate)). Из-за этого impact_pct
        # никогда не опускался ниже ставки пула - при объёме -> 0 он равнялся ровно
        # 0.05% / 0.25% / 0.3% вместо нуля. С порогом подтверждения в 0.01% (и даже 0.3%)
        # это означало needs_confirmation=True ВСЕГДА и для ЛЮБОГО объёма, то есть:
        #   - analyze_opportunities дёргал confirm_price_onchain на каждой итерации;
        #   - swap_fast._estimate_amount_out_raw для V3 никогда не брал локальную оценку и
        #     ходил в сеть перед каждым свопом, обнуляя весь смысл быстрого пути.
        # Теперь отсчёт идёт от reference_price - цены того же свопа с той же комиссией, но
        # при объёме -> 0. impact_pct стал ЧИСТЫМ проскальзыванием от размера сделки, и
        # порог DEX_V3_IMPACT_CONFIRM_THRESHOLD_PCT наконец означает то, что написано в
        # его комментарии: "насколько далеко сделка уводит цену от текущей точки".
        impact_pct = (abs(effective_price - reference_price) / reference_price * 100
                      if reference_price > 0 else 0.0)
        needs_confirmation = self.pool_kind == 'v3' and impact_pct >= DEX_V3_IMPACT_CONFIRM_THRESHOLD_PCT
        effective_price *= DEX_BUY_MARKUP if is_buy else DEX_SELL_MARKDOWN

        return {
            'amount_out': amount_out,
            'effective_price': effective_price,
            'mid_price': mid_price,
            # Цена без price impact, но с комиссией пула - точка отсчёта для impact_pct.
            'reference_price': reference_price,
            'impact_pct': impact_pct,
            'needs_confirmation': needs_confirmation,
            'pool_kind': self.pool_kind,
            # Комиссия пула УЖЕ вычтена из effective_price. Флаг нужен вызывающему коду
            # (arb.core.market_data._calc_buy_mecx_fee), чтобы не вычесть её второй раз:
            # при fallback на плоские self.buy/self.sell комиссии в цене нет, и там
            # вычитать её обязательно.
            'fee_included': True,
            'fee_rate': fee_rate,
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
