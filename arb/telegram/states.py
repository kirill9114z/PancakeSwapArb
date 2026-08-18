"""Состояния FSM Telegram-интерфейса.

Вынесены из bot.py отдельно, чтобы список шагов диалога (особенно длинная
цепочка добавления пары) читался одним экраном, без 500 строк обработчиков.
"""
from aiogram.fsm.state import State, StatesGroup


class Form(StatesGroup):
    SETTINGS = State()
    GLOBAL_SPREAD_INPUT = State()
    INDIVIDUAL_SPREAD_SELECT = State()
    INDIVIDUAL_SPREAD_INPUT = State()
    PAIRS_MENU = State()
    ADD_PAIR_NAME = State()
    ADD_PAIR_CONTRACT = State()
    ADD_PAIR_DECIMALS = State()
    REMOVE_PAIR_SELECT = State()
    UID_INPUT = State()
    PRIVATE_KEY_INPUT = State()
    ADD_PAIR_MEXC_API_KEY = State()
    ADD_PAIR_MEXC_API_SECRET = State()
    ADD_PAIR_MEXC_UID = State()
    ADD_PAIR_PRIVATE_KEY = State()
    ADD_PAIR_RPC = State()
    ADD_PAIR_WEBSOCKET = State()
    ADD_PAIR_ADDRESS_CONTRACT = State()
    ADD_PAIR_ABI_CONTRACT = State()
    ADD_PAIR_MAX_VOL = State()
    UPDATE_UID_PAIR = State()
    UID_INPUT2 = State()
    STATS_SELECT = State()
