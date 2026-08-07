import sys
import asyncio
from trade import Arbitrage
from database import Database
import logging
import ccxt.async_support as ccxt

from pancake_trade import OkxTrade
from config import INITIAL_RAK_PRICE, MEXC_CCXT_TIMEOUT_MS, PAIR_STARTUP_DELAY_SECONDS
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


    pair_data = db.get_pair_data(pair_name)
    rak = INITIAL_RAK_PRICE
    mexc_client = ccxt.mexc({
        'apiKey': pair_data['mexc_api_key'],
        'secret': pair_data['mexc_api_secret'],
        'enableRateLimit': True,
        'timeout': MEXC_CCXT_TIMEOUT_MS
    })
    pancake = OkxTrade(pair_name, pair_data['address_contract'], pair_data['abi'], pair_data['decimals'], rak, pair_data["private_key"], pair_data["websocket"], pair_data['rpc'] ,db)
    arbitrage = Arbitrage(mexc_client, pair_name, pancake, pair_data["private_key"], pair_data['contract_bsc'],pair_data["rpc"], bot, chat_id, db, pair_data['volume'])
    active_arbitrage_instances[pair_name] = arbitrage
    arbitrage.running = True
    pancake.running = True
    await asyncio.sleep(PAIR_STARTUP_DELAY_SECONDS)
    await arbitrage.update_balances()
    try:
        tasks = []
        logger.info(f"We start analyzing to {pair_name}")
        task1 = asyncio.create_task(pancake.monitoring_price())
        task2 = asyncio.create_task(arbitrage.run_analysis_loop())
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

        try:
            await mexc_client.close()
        except Exception as e:
            logger.warning(f"Failed to close mexc client for {pair_name}: {e}")




async def main(chat_id, bot):
    try:
        db = Database()
        pairs = db.get_all_pairs()
        if not pairs:
            logger.warning("No pairs found in database. Add pairs via Telegram bot.")
            return
        tasks = []
        for pair in pairs:
            task = asyncio.create_task(monitor_pair(pair, db, chat_id, bot))
            tasks.append(task)
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.exception(f"Critical error in main: {e}")
