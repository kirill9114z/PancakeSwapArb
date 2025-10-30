import json
import aiohttp
from typing import Optional
from eth_account import Account
from web3 import Web3

import asyncio
from decimal import Decimal, getcontext
import time

# Сессия с connection pool и таймаутами
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

class OkxTrade:
    def __init__(self, pair, address, abi, dec1, dec2, rak):
        self.pair = pair
        self.abi = abi
        self.address = address
        self.decimals1 = dec1
        self.decimals2 = dec2
        self.rak = rak
        self.ws_url = "wss://bnb-mainnet.g.alchemy.com/v2/HrpyTKu0jGPtMyZvD5iCV"
        self.w3 = Web3(Web3.LegacyWebSocketProvider("wss://bnb-mainnet.g.alchemy.com/v2/HrpyTKu0jGPtMyZvD5iCV"))

        self.buy = self.rak
        self.sell = self.rak


    async def get_session(self) -> aiohttp.ClientSession:
        global _session
        async with _session_lock:
            if _session is None or _session.closed:
                # Создаем коннектор с настройками прокси
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
        token0 = pool.functions.token0().call()
        token1 = pool.functions.token1().call()
        if token1 == "0x55d398326f99059fF775485246999027B3197955":
            return True
        if token0 == "0x55d398326f99059fF775485246999027B3197955":
            return False
        else:
            return None

    async def sqrtPriceX96_to_price(self, sqrtPriceX96: int) -> Decimal:
        sqrt_price = Decimal(sqrtPriceX96) / (Decimal(2) ** 96)
        price = sqrt_price * sqrt_price
        return price

    async def adjust_for_decimals(self, price, dec0: int, dec1: int) -> Decimal:
        exp = dec0 - dec1
        return price * (Decimal(10) ** exp)

    async def handle_event(self, e, side1):
        args = e["args"]
        amount0 = args['amount0']
        amount1 = args['amount1']
        sqrtPriceX96 = args["sqrtPriceX96"]
        raw_price = await self.sqrtPriceX96_to_price(sqrtPriceX96)
        price_corr = (await self.adjust_for_decimals(raw_price, self.decimals1, self.decimals2) if side1
                      else 1 / await self.adjust_for_decimals(raw_price, self.decimals1, self.decimals2))

        if abs(float(price_corr) - self.rak) <= self.rak * 0.05:
            if amount0 < 0:
                self.rak = float(price_corr)
                self.sell = float(price_corr)
            if amount1 < 0:
                self.rak = float(price_corr)
                self.buy = float(price_corr)

    async def monitoring_price(self):
        pool = self.w3.eth.contract(address=self.address, abi=self.abi)
        swap_filter = pool.events.Swap.create_filter(from_block='latest')
        side = await self.side(pool)
        if side is None:
            print("side1 is None, невозможна работа")
            # можно либо завершить, либо подождать и пробовать снова
            return
        while True:
            try:
                events = swap_filter.get_new_entries()
                for e in events:
                    await self.handle_event(e, side)
                await asyncio.sleep(0.01)
            except Exception as exc:
                print("Ошибка при получении новых записей:", exc)

    async def swap(self):
        pass



