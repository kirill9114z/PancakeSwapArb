import sys
import asyncio
from trade import Arbitrage
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError
from database import Database
import logging
import ccxt.async_support as ccxt
from web3.middleware import ExtraDataToPOAMiddleware
from config import API_KEY_MEXC, API_SECRET_MEXC, RPC_ETH, RPC_BSC, RPC_BASE
from abi_contract import abi

from pancake_trade import OkxTrade
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ArbitrageBot")

active_arbitrage_instances = {}
async def monitor_pair(pair_name, db, chat_id, bot):
    """Запускает мониторинг для конкретной пары"""
    logger.info(f"Starting monitoring for {pair_name}")

    # Получаем контракты для пары
    # contracts = db.get_pair_contracts(pair_name)

    pair_data = db.get_pair_data(pair_name)
    # abi = pair_data['abi']
    rak = 1.623
    mexc_client = ccxt.mexc({
        'apiKey': pair_data['mexc_api_key'],
        'secret': pair_data['mexc_api_secret'],
        'enableRateLimit': True,
        'timeout': 30000
    })
    # def __init__(self, exchange, pair, pancakce, privat_key, address, rpc, bot, chat_id, db):
    pancake = OkxTrade(pair_name, pair_data['address_contract'], pair_data['abi'], pair_data['decimals'], rak, pair_data["private_key"], pair_data["websocket"], pair_data['rpc'] ,db)
    arbitrage = Arbitrage(mexc_client, pair_name, pancake, pair_data["private_key"], pair_data['contract_bsc'],pair_data["rpc"], bot, chat_id, db, pair_data['volume'])
    active_arbitrage_instances[pair_name] = arbitrage
    arbitrage.running = True
    pancake.running = True
    # Создаем экземпляр арбитражного бота для пары
    await asyncio.sleep(10)
    await arbitrage.update_balances()
    try:
        tasks = []
        logger.info(f"We start analyzing to {pair_name}")
        task1 = asyncio.create_task(pancake.monitoring_price())
        task2 = asyncio.create_task(arbitrage.analyze_opportunities())
        tasks.append(task2)
        tasks.append(task1)
        await asyncio.gather(task1, task2)
    except Exception as e:
        logger.exception(f"Error monitoring {pair_name}: {str(e)}")
    finally:
        logger.info(f"Monitoring stopped for {pair_name}")
        pancake.running = False
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if pair_name in active_arbitrage_instances:
            del active_arbitrage_instances[pair_name]




async def main(chat_id, bot):
    try:
        db = Database()
        pairs = db.get_all_pairs()
        if not pairs:
            logger.warning("No pairs found in database. Add pairs via Telegram bot.")
            return

        # mexc_client = ccxt.mexc({
        #     'apiKey': API_KEY_MEXC,
        #     'secret': API_SECRET_MEXC,  # НУЖНО БУДЕТ ИМПОРТИРОВАТЬ ИЗ КОНФИГА АПИ_КЕЙ И АПИСИКРЕТ
        #     'enableRateLimit': True,
        #     'timeout': 30000
        # })
        # w3_providers = {
        #     1: AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_ETH)),
        #     8453: AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_BASE)),
        #     56: AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_BSC))
        # }
        # for w3 in w3_providers.values():
        #     w3.middleware_onion.inject(ExtraDataToPOAMiddleware, name='ExtraDataToPOA', layer=0)
        # Создаем задачи для мониторинга каждой пары
        tasks = []
        for pair in pairs:
            task = asyncio.create_task(monitor_pair(pair, db, chat_id, bot))
            tasks.append(task)
        # Запускаем все задачи параллельно
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.exception(f"Critical error in main: {e}")

