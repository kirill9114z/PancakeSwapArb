"""Точка входа: python run.py

Поднимает Telegram-интерфейс (arb.telegram.bot). Сама торговля запускается уже
из него - кнопкой "🚀 Запустить бота", которая создаёт задачу arb.core.runner.main.
"""
import asyncio

from arb.telegram.bot import main1

if __name__ == "__main__":
    asyncio.run(main1())
