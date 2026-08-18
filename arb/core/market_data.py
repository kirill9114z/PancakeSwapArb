"""Рыночные данные MEXC: стакан и оценка комиссий.

Стакан читается ровно одним способом (_fetch_orderbook_raw) и для анализа
возможностей, и для аварийного закрытия позиции - чтобы не завести второй,
расходящийся с первым источник цены.
"""
import time

from arb.exchanges.mexc_web import get_session
from config import (
    BNB_PRICE_FALLBACK_USD,
    BNB_PRICE_REFRESH_SECONDS,
    DEFAULT_PANCAKE_FEE_RATE,
    FEE_FLAT_USD,
    MEXC_DEPTH_HTTP_TIMEOUT_SECONDS,
    MEXC_ORDERBOOK_DEPTH_LEVELS,
    MEXC_TAKER_FEE_RATE,
    SWAP_GAS_USED_ESTIMATE_V2,
    SWAP_GAS_USED_ESTIMATE_V3,
)

from .orderbook import compute_prefix_stats_with_max_sum


class MarketDataMixin:
    """Стакан и комиссии. Подмешивается в Arbitrage."""

    async def _fetch_orderbook_raw(self, session, depth_levels=MEXC_ORDERBOOK_DEPTH_LEVELS):
        """Сырые уровни стакана MEXC: (asks, bids), каждый - список [цена, объём],
        отсортированный от лучшей цены. Вынесено из get_price_mexc, чтобы аварийное
        закрытие позиции читало стакан ровно тем же способом, что и анализ, и не
        заводило второй, расходящийся с ним источник цены.
        Возвращает (None, None) при любой ошибке."""
        u_id = self.db.get_uid(self.pair)
        if session is None:
            session = await get_session()
        if session is None:
            return None, None
        headers = {
            "Referer": f"https://www.mexc.com/exchange/{self.symbol}",
            "Cookie": f"uc_token={u_id}; u_id={u_id};",
            "X-Requested-With": "XMLHttpRequest",
        }
        ask = []
        bids = []
        try:
            params = {"symbol": str(self.symbol)}
            async with session.get(
                f"https://www.mexc.com/api/platform/spot/market/depth",
                headers=headers,
                params=params,
                timeout=MEXC_DEPTH_HTTP_TIMEOUT_SECONDS,
            ) as resp:
                data = await resp.json()
                k = 0
                for i in data['data']['data']['asks']:
                    ask.append([float(i['p']), float(i['q'])])
                    k += 1
                    if k == depth_levels:
                        break
                b = 0
                for i in data['data']['data']['bids']:
                    bids.append([float(i['p']), float(i['q'])])
                    b += 1
                    if b == depth_levels:
                        break
        except Exception as e:
            if str(e) == 'Session is closed':
                return None, None
            if str(e) == "'data'":
                await self.send_notification(f'Нет пары на MEXC: {self.pair}\nОстановите скрипт и удалите пару')
                return None, None
            print(f'UNKNOWERROR get_price: {e} ')
            return None, None
        return ask, bids

    async def get_price_mexc(self, session):
        ask, bids = await self._fetch_orderbook_raw(session)
        if ask is None or bids is None:
            return None, None, None, None, None, None
        ask_amounts, ask_costs, ask_avg = compute_prefix_stats_with_max_sum(ask, self.max_volume if self.max_volume is not None else self.balance_usdt_mexc)
        bid_amounts, bid_costs, bid_avg = compute_prefix_stats_with_max_sum(bids, self.max_volume if self.max_volume is not None else self.balance_usdc_dex_bsc)
        return ask_amounts, ask_costs, ask_avg, bid_amounts, bid_costs, bid_avg





    async def _bnb_price_usd(self) -> float:
        """Цена BNB в USD для перевода стоимости газа в доллары. Кешируется на
        BNB_PRICE_REFRESH_SECONDS: значение нужно только для оценки прибыли, а лишний
        сетевой вызов в цикле анализа (несколько раз в секунду) недопустим.
        При любой ошибке возвращает намеренно ЗАВЫШЕННЫЙ BNB_PRICE_FALLBACK_USD -
        переоценить стоимость газа и пропустить сделку безопаснее, чем недооценить."""
        now = time.time()
        if self._bnb_price_usd_cache and (now - self._last_fee_update) < BNB_PRICE_REFRESH_SECONDS:
            return self._bnb_price_usd_cache
        async with self._fee_lock:
            # Пока ждали лок, цену мог обновить другой вызов.
            if self._bnb_price_usd_cache and (time.time() - self._last_fee_update) < BNB_PRICE_REFRESH_SECONDS:
                return self._bnb_price_usd_cache
            try:
                ticker = await self.exchange.fetch_ticker('BNB/USDT')
                price = float(ticker['last'] or ticker['close'])
                if price > 0:
                    self._bnb_price_usd_cache = price
                    self._last_fee_update = time.time()
                    return price
            except Exception as e:
                print(f"[{self.pair}] не удалось прочитать цену BNB: {e} - "
                      f"беру запасную ${BNB_PRICE_FALLBACK_USD}")
            # Запоминаем и время неудачи тоже: иначе при недоступном тикере мы бы
            # ходили за ним на КАЖДОЙ итерации анализа.
            self._last_fee_update = time.time()
            self._bnb_price_usd_cache = self._bnb_price_usd_cache or BNB_PRICE_FALLBACK_USD
            return self._bnb_price_usd_cache

    async def _estimate_gas_cost_usd(self) -> float:
        """Стоимость газа одного хеджирующего свопа в USD.

        Раньше на её месте стояла константа в один цент (FEE_FLAT_USD), то есть
        стоимость хеджа считалась одинаковой при любой цене газа в сети. При
        PROFIT_THRESHOLD_USD = 0.2 это заметная доля порога, и ошибка в обе стороны:
        при дорогом газе бот брал убыточные сделки, при дешёвом - зря пропускал
        прибыльные.

        Считаем по ФАКТИЧЕСКИ израсходованному газу (last_gas_used из receipt
        последнего свопа), а не по SWAP_GAS_LIMIT_*: лимит намеренно завышен с
        запасом, а на BSC неизрасходованный газ возвращается."""
        gas_used = getattr(self.pancakce, 'last_gas_used', None)
        if not gas_used:
            gas_used = (SWAP_GAS_USED_ESTIMATE_V3 if getattr(self.pancakce, 'pool_kind', None) == 'v3'
                        else SWAP_GAS_USED_ESTIMATE_V2)
        # Цена газа уже прогрета фоново в monitoring_price - в сеть за ней не идём.
        gas_price_wei = getattr(self.pancakce, '_gas_price_wei', None)
        if not gas_price_wei:
            return FEE_FLAT_USD
        gas_cost_bnb = (gas_used * gas_price_wei) / 1e18
        return gas_cost_bnb * await self._bnb_price_usd()

    async def _calc_buy_mecx_fee(self, volume, price, dex_fee_included: bool = False):
        """Издержки сделки в USD: комиссия MEXC + (если нужно) комиссия пула + газ.

        Про dex_fee_included - самое важное здесь. quote_local() считает выход свопа
        по constant-product С комиссией пула (amount_in * (1 - fee_rate)), то есть
        возвращаемый effective_price её УЖЕ содержит, и в profit кандидата она уже
        вычтена. Раньше эта функция вычитала комиссию пула безусловно - то есть
        второй раз. На объёме $100 и пуле 0.25% это лишние $0.25 при пороге прибыли
        $0.2: порог фактически удваивался, и прибыльные сделки молча отсеивались.
        Теперь комиссия пула вычитается ТОЛЬКО когда цена DEX пришла не из
        quote_local/confirm_price_onchain, а из плоских self.pancakce.buy/sell -
        там это чистая mid-цена ± маркап, комиссии в ней нет.

        Про ставку пула: раньше здесь было захардкожено 0.025%, хотя реальные тиры V3
        на BSC - 0.01%/0.05%/0.25%/0.3%, и низколиквидные пары обычно сидят на старшем.
        Берём ставку РЕАЛЬНО использованного пула (кэш в OkxTrade после свопа),
        иначе консервативный DEFAULT_PANCAKE_FEE_RATE."""
        notional = float(volume) * float(price)
        fee_mexc = notional * MEXC_TAKER_FEE_RATE

        if dex_fee_included:
            fee_pancake = 0.0
        else:
            pancake_fee_rate = getattr(self.pancakce, 'last_fee_rate', None) or DEFAULT_PANCAKE_FEE_RATE
            fee_pancake = notional * pancake_fee_rate

        gas_usd = await self._estimate_gas_cost_usd()
        return fee_mexc + fee_pancake + gas_usd + FEE_FLAT_USD
