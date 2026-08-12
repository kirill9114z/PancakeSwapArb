"""Рыночные данные MEXC: стакан и оценка комиссий.

Стакан читается ровно одним способом (_fetch_orderbook_raw) и для анализа
возможностей, и для аварийного закрытия позиции - чтобы не завести второй,
расходящийся с первым источник цены.
"""
from arb.exchanges.mexc_web import get_session
from config import (
    MEXC_DEPTH_HTTP_TIMEOUT_SECONDS,
    MEXC_ORDERBOOK_DEPTH_LEVELS,
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





    async def _calc_buy_mecx_fee(self, volume, price):
        # Раньше комиссия PancakeSwap оценивалась захардкоженной ставкой 0.025%,
        # хотя реальные тиры комиссий V3 на BSC - 0.01%/0.05%/0.25%/0.3%, и для
        # многих (особенно низколиквидных) пар используется именно старший тир.
        # Это делало предварительную оценку прибыли систематически оптимистичной.
        # Теперь берём комиссию РЕАЛЬНО найденного пула (кэшируется в OkxTrade
        # после последнего свопа), а если она ещё не известна - используем
        # консервативную оценку 0.25% вместо заниженной 0.025%.
        pancake_fee_rate = getattr(self.pancakce, 'last_fee_rate', None) or 0.0025
        fee_mexc = float(volume) * float(price) * 0.0005
        fee_panckake = float(volume) * float(price) * pancake_fee_rate
        fee = fee_mexc + fee_panckake + 0.01
        return fee
