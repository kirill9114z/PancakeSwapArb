"""Пути к файлам проекта в одном месте.

Раньше пути были разбросаны по коду строковыми литералами относительно текущей
директории ('pancake_router_v2_abi.json', os.path.join('pair_abi', ...),
'arbitrage_bot.db'), поэтому бот работал только при запуске ровно из корня
репозитория. Здесь всё считается от корня проекта, так что запуск из любой
директории читает те же самые файлы.
"""
import json
from pathlib import Path

# arb/paths.py -> arb/ -> корень репозитория
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Общие ABI (одинаковы для всех пар, лежат в git).
ABI_DIR = PROJECT_ROOT / "abi"
ROUTER_ABI_PATH = ABI_DIR / "pancake_router_v2_abi.json"
ERC20_ABI_PATH = ABI_DIR / "erc20_abi.json"
MULTICALL_ABI_PATH = ABI_DIR / "multicall_abi.json"

# ABI конкретных пулов - сохраняются Telegram-ботом при добавлении пары.
PAIR_ABI_DIR = PROJECT_ROOT / "pair_abi"

# SQLite с парами, ключами и настройками спреда.
DB_PATH = PROJECT_ROOT / "arbitrage_bot.db"


def load_json(path):
    """Чтение JSON-файла (ABI) с явной кодировкой."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pair_abi_path(pair_name: str) -> Path:
    """ABI пула для пары 'TOKEN/USDT' -> pair_abi/TOKEN.json."""
    return PAIR_ABI_DIR / f"{pair_name.split('/')[0]}.json"
