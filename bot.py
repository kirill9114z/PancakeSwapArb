import logging

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder
import aiofiles
import os
from aiogram.types import ContentType
from database import Database
import asyncio
from exchange import get_session
from main import main as arb_main
from main import active_arbitrage_instances

from config import BOT_TOKEN, SPREAD_INPUT_MIN_PCT, SPREAD_INPUT_MAX_PCT

db = Database()
arb_task: asyncio.Task | None = None
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


bot_process = None
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
chat_id = 0


@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    global chat_id
    builder = ReplyKeyboardBuilder()
    builder.button(text="⚙️ Настройки")
    builder.button(text="🔄 Пары")
    builder.button(text="🚀 Запустить бота")
    builder.button(text="🛑 Остановить бота")
    builder.adjust(2, 2)
    chat_id = message.chat.id

    await message.answer(
        "Добро пожаловать в Arbitrage Bot!\nВыберите действие:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(Form.SETTINGS)

@dp.message(Form.UID_INPUT)
async def process_uid(message: types.Message, state: FSMContext):
    """Обработчик ввода U_ID"""
    uid_value = message.text.strip()
    print(f"New id: {uid_value}")
    db.set_uid(uid_value)
    await message.answer(f"✅ U_ID успешно сохранен: {uid_value}")
    await message.answer("🔐 Введите свой private_key:")
    await state.set_state(Form.PRIVATE_KEY_INPUT)
@dp.message(Form.PRIVATE_KEY_INPUT)
async def process_private_key(message: types.Message, state: FSMContext):
    raw_key = message.text.strip()
    db.set_private_key(raw_key)
    await message.answer("✅ private_key сохранён безопасно!")
    return await cmd_start(message, state)

@dp.message(Form.SETTINGS, F.text == "🚀 Запустить бота")
async def start_arbitrage_bot(message: types.Message):
    global arb_task, bot
    pairs = db.get_all_pairs()
    if not pairs:
        return await message.answer(
            "❌ Нельзя запустить бота без торговых пар!\n"
            "Добавьте пары через меню \"🔄 Пары\""
        )
    if arb_task and not arb_task.done():
        return await message.answer("✅ Бот уже запущен")
    arb_task = asyncio.create_task(arb_main(chat_id, bot))
    await message.answer("🚀 Арбитражный бот успешно запущен!")


@dp.message(Form.SETTINGS, F.text == "🛑 Остановить бота")
async def stop_arbitrage_bot(message: types.Message):
    global arb_task
    if arb_task is None:
        await message.answer("🛑 Арбитражный бот уже остановлен!")
    print(f'Lst: {active_arbitrage_instances}')
    pairs = db.get_all_pairs()
    for pair in pairs:
        try:
            arb = active_arbitrage_instances[pair]
            arb.running = False
        except KeyError:
            continue
    arb_task.cancel()
    arb_task = None
    await message.answer("🛑 Арбитражный бот остановлен!")




@dp.message(Form.SETTINGS, F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.button(text="🌐 Глобальный спред")
    builder.button(text="🎯 Индивидуальный спред")
    builder.button(text="🆔 Обновить U_ID")
    builder.button(text="🔙 Назад")
    builder.adjust(2, 2)

    await message.answer(
        "Выберите действие",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )


@dp.message(Form.SETTINGS, F.text == "🌐 Глобальный спред")
async def global_spread_start(message: types.Message, state: FSMContext):
    current = db.get_global_spread()
    await message.answer(
        f"Текущий глобальный спред: {current}%\n"
        f"Введите новое значение ({SPREAD_INPUT_MIN_PCT} < spread < {SPREAD_INPUT_MAX_PCT}):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(Form.GLOBAL_SPREAD_INPUT)


@dp.message(Form.GLOBAL_SPREAD_INPUT)
async def global_spread_set(message: types.Message, state: FSMContext):
    try:
        spread = float(message.text)
        if SPREAD_INPUT_MIN_PCT < spread < SPREAD_INPUT_MAX_PCT:
            db.set_global_spread(spread)
            await message.answer(f"✅ Глобальный спред установлен: {spread}%")
            return await cmd_start(message, state)
        raise ValueError()
    except:
        await message.answer(
            f"❌ Ошибка! Введите число между {SPREAD_INPUT_MIN_PCT} и {SPREAD_INPUT_MAX_PCT}:"
        )


@dp.message(Form.SETTINGS, F.text == "🎯 Индивидуальный спред")
async def individual_spread_start(message: types.Message, state: FSMContext):
    pairs = db.get_all_pairs()
    if not pairs:
        await message.answer("ℹ️ Сначала добавьте пары через меню 'Пары'")
        return await settings_menu(message, state)

    builder = ReplyKeyboardBuilder()
    for pair in pairs:
        builder.button(text=pair)
    builder.button(text="🔙 Назад")
    builder.adjust(2)

    await message.answer(
        "Выберите пару для настройки:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(Form.INDIVIDUAL_SPREAD_SELECT)


@dp.message(Form.INDIVIDUAL_SPREAD_SELECT)
async def individual_spread_select(message: types.Message, state: FSMContext):
    pair = message.text.upper()
    pairs = db.get_all_pairs()

    if pair == "🔙 НАЗАД":
        return await settings_menu(message, state)

    if pair not in pairs:
        await message.answer("❌ Пара не найдена! Выберите из списка:")
        return

    await state.update_data(selected_pair=pair)
    current = db.get_pair_spread(pair) or "не установлен"
    await message.answer(
        f"Текущий спред для {pair}: {current}%\n"
        f"Введите новое значение ({SPREAD_INPUT_MIN_PCT} < spread < {SPREAD_INPUT_MAX_PCT}):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(Form.INDIVIDUAL_SPREAD_INPUT)


@dp.message(Form.INDIVIDUAL_SPREAD_INPUT)
async def individual_spread_set(message: types.Message, state: FSMContext):
    try:
        spread = float(message.text)
        if not (SPREAD_INPUT_MIN_PCT < spread < SPREAD_INPUT_MAX_PCT):
            raise ValueError()

        data = await state.get_data()
        pair = data['selected_pair']
        db.set_pair_spread(pair, spread)
        await message.answer(f"✅ Спред для {pair} установлен: {spread}%")
        return await cmd_start(message, state)
    except:
        await message.answer(
            f"❌ Ошибка! Введите число между {SPREAD_INPUT_MIN_PCT} и {SPREAD_INPUT_MAX_PCT}:"
        )

@dp.message(Form.SETTINGS, F.text == "🆔 Обновить U_ID")
async def update_uid(message: types.Message, state: FSMContext):
    pairs = db.get_all_pairs()
    if not pairs:
        await message.answer("ℹ️ Сначала добавьте пары через меню 'Пары'")
        return await settings_menu(message, state)
    builder = ReplyKeyboardBuilder()
    for pair in pairs:
        builder.button(text=pair)
    builder.button(text="🔙 Назад")
    builder.adjust(2)

    await message.answer(
        "Выберите пару для обновления U_ID:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(Form.UPDATE_UID_PAIR)

@dp.message(Form.UPDATE_UID_PAIR)
async def individual_spread_select(message: types.Message, state: FSMContext):
    pair = message.text.upper()
    pairs = db.get_all_pairs()

    if pair == "🔙 НАЗАД":
        return await settings_menu(message, state)

    if pair not in pairs:
        await message.answer("❌ Пара не найдена! Выберите из списка:")
        return

    await state.update_data(selected_pair1=pair)
    await message.answer(
        f"Введите новый U_ID", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(Form.UID_INPUT2)

@dp.message(Form.UID_INPUT2)
async def individual_spread_set(message: types.Message, state: FSMContext):
    try:
        u_id = str(message.text)
        data = await state.get_data()
        pair = data['selected_pair1']
        res = db.update_pair_mexc_uid(pair, u_id)
        if res:
            await message.answer(f"✅ {pair} U_ID установлен")
            return await cmd_start(message, state)
        else:
            await message.answer(f'❌ U_ID не установлен, какая то ошибка я хз')
    except Exception as e:
        await message.answer(f"❌ Произошла ошибка: {e}")
        await state.clear()
        await cmd_start(message, state)



# =====================
# УПРАВЛЕНИЕ ПАРАМИ
# =====================

@dp.message(Form.SETTINGS, F.text == "🔄 Пары")
async def pairs_menu(message: types.Message, state: FSMContext):
    builder = ReplyKeyboardBuilder()
    builder.button(text="➕ Добавить пару")
    builder.button(text="➖ Удалить пару")
    builder.button(text="🔙 Назад")
    builder.adjust(2)

    await message.answer(
        "Управление торговыми парами:",
        reply_markup=builder.as_markup(resize_keyboard=True))
    await state.set_state(Form.PAIRS_MENU)


@dp.message(Form.PAIRS_MENU, F.text == "➕ Добавить пару")
async def add_pair_name(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите название пары (например: BTC/USDT):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(Form.ADD_PAIR_NAME)


@dp.message(Form.ADD_PAIR_NAME)
async def process_pair_name(message: types.Message, state: FSMContext):
    pair = message.text.upper()

    if db.get_pair_data(pair):
        await message.answer(f"❌ Пара {pair} уже существует!")
        return await cmd_start(message, state)

    await state.update_data(new_pair=pair)
    await message.answer("💵 Укажите максимальную сумму сделки в USDT:")
    await state.set_state(Form.ADD_PAIR_MAX_VOL)


@dp.message(Form.ADD_PAIR_MAX_VOL)
async def process_pair_volume(message: types.Message, state: FSMContext):
    try:
        volume = int(message.text.strip())

        await state.update_data(max_volume=volume)
        await message.answer("Введите контракт токена в сети BSC:")
        await state.set_state(Form.ADD_PAIR_CONTRACT)
    except ValueError:
        await message.answer("❌ Ошибка! Введите целое число:")


@dp.message(Form.ADD_PAIR_CONTRACT)
async def process_pair_contract(message: types.Message, state: FSMContext):
    contract = message.text.strip()
    await state.update_data(contract_bsc=contract)
    await message.answer("Введите decimals монеты (например: 18):")
    await state.set_state(Form.ADD_PAIR_DECIMALS)


@dp.message(Form.ADD_PAIR_DECIMALS)
async def process_pair_decimals(message: types.Message, state: FSMContext):
    try:
        decimals = int(message.text.strip())
        await state.update_data(decimals=decimals)
        await message.answer("Введите адрес контракта пула:")
        await state.set_state(Form.ADD_PAIR_ADDRESS_CONTRACT)
    except ValueError:
        await message.answer("❌ Ошибка! Введите целое число:")


@dp.message(Form.ADD_PAIR_ADDRESS_CONTRACT)
async def process_pair_addrcontract(message: types.Message, state: FSMContext):
    try:
        address_contract = message.text.strip()
        await state.update_data(address_contract=address_contract)
        await message.answer("Введите ABI контракта:")
        await state.set_state(Form.ADD_PAIR_ABI_CONTRACT)
    except ValueError:
        await message.answer("❌ Ошибка! Введите правильное значение:")


@dp.message(Form.ADD_PAIR_ABI_CONTRACT, F.content_type.in_({ContentType.TEXT, ContentType.DOCUMENT}))
async def process_pair_abi(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        pair_name = data.get('new_pair')

        if not pair_name:
            await message.answer("❌ Ошибка: название пары не найдено. Начните процесс добавления заново.")
            await state.finish()
            return

        abi_content = ""

        if message.content_type == ContentType.TEXT:
            abi_content = message.text.strip()

        elif message.content_type == ContentType.DOCUMENT:
            if message.document.mime_type not in ['text/plain', 'application/json']:
                await message.answer("❌ Пожалуйста, отправьте ABI в виде текстового файла (.txt или .json)")
                return

            file_id = message.document.file_id
            file = await message.bot.get_file(file_id)
            file_path = file.file_path

            downloaded_file = await message.bot.download_file(file_path)
            abi_content = downloaded_file.read().decode('utf-8')

        if not abi_content:
            await message.answer("❌ Получен пустой ABI. Пожалуйста, отправьте корректный ABI контракта.")
            return

        pair = pair_name.split('/')[0]
        abi_filename = f"{pair}.json"
        abi_folder = "pair_abi"
        abi_filepath = os.path.join(abi_folder, abi_filename)

        os.makedirs(abi_folder, exist_ok=True)

        try:
            async with aiofiles.open(abi_filepath, 'w', encoding='utf-8') as f:
                await f.write(abi_content)
        except Exception as e:
            await message.answer(f"❌ Ошибка при сохранении файла ABI: {str(e)}")
            return

        await state.update_data(abi=abi_filename)

        await message.answer("✅ ABI контракта успешно сохранен!\nВведите Api_key MEXC:")
        await state.set_state(Form.ADD_PAIR_MEXC_API_KEY)

    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке ABI: {str(e)}")

@dp.message(Form.ADD_PAIR_MEXC_API_KEY)
async def process_mexc_api_key(message: types.Message, state: FSMContext):
    api_key = message.text.strip()
    await state.update_data(mexc_api_key=api_key)
    await message.answer("Введите Api_secret MEXC:")
    await state.set_state(Form.ADD_PAIR_MEXC_API_SECRET)


@dp.message(Form.ADD_PAIR_MEXC_API_SECRET)
async def process_mexc_api_secret(message: types.Message, state: FSMContext):
    api_secret = message.text.strip()
    await state.update_data(mexc_api_secret=api_secret)
    await message.answer("Введите U_ID MEXC:")
    await state.set_state(Form.ADD_PAIR_MEXC_UID)


@dp.message(Form.ADD_PAIR_MEXC_UID)
async def process_mexc_uid(message: types.Message, state: FSMContext):
    mexc_uid = message.text.strip()
    await state.update_data(mexc_uid=mexc_uid)
    await message.answer("Введите private_key:")
    await state.set_state(Form.ADD_PAIR_PRIVATE_KEY)


@dp.message(Form.ADD_PAIR_PRIVATE_KEY)
async def process_private_key(message: types.Message, state: FSMContext):
    private_key = message.text.strip()
    await state.update_data(private_key=private_key)
    await message.answer("Введите RPC:")
    await state.set_state(Form.ADD_PAIR_RPC)


@dp.message(Form.ADD_PAIR_RPC)
async def process_rpc(message: types.Message, state: FSMContext):
    rpc = message.text.strip()
    await state.update_data(rpc=rpc)
    await message.answer("Введите Websocket:")
    await state.set_state(Form.ADD_PAIR_WEBSOCKET)


@dp.message(Form.ADD_PAIR_WEBSOCKET)
async def process_websocket(message: types.Message, state: FSMContext):
    websocket = message.text.strip()
    data = await state.get_data()
    if db.add_pair_v2(
            data['new_pair'],
            data['contract_bsc'],
            data['decimals'],
            data['address_contract'],
            data['abi'],
            data['mexc_api_key'],
            data['mexc_api_secret'],
            data['mexc_uid'],
            data['private_key'],
            data['rpc'],
            websocket,
            data['max_volume']
    ):
        await message.answer(f"✅ Пара *{data['new_pair']}* успешно добавлена!", parse_mode="Markdown")
    else:
        await message.answer(f"❌ Ошибка при добавлении пары {data['new_pair']}!")

    await cmd_start(message, state)


@dp.message(Form.PAIRS_MENU, F.text == "➖ Удалить пару")
async def remove_pair_start(message: types.Message, state: FSMContext):
    pairs = db.get_all_pairs()
    if not pairs:
        await message.answer("ℹ️ Нет пар для удаления")
        return await pairs_menu(message, state)

    builder = ReplyKeyboardBuilder()
    for pair in pairs:
        builder.button(text=pair)
    builder.button(text="🔙 Назад")
    builder.adjust(2)

    await message.answer(
        "Выберите пару для удаления:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )

    await state.set_state(Form.REMOVE_PAIR_SELECT)

@dp.message(Form.REMOVE_PAIR_SELECT, F.text == "🔙 Назад")
async def remove_pair_back(message: types.Message, state: FSMContext):
    await state.clear()
    await cmd_start(message, state)

@dp.message(Form.REMOVE_PAIR_SELECT)
async def process_remove_pair(message: types.Message, state: FSMContext):
    pair = message.text.upper()
    if db.remove_pair_v2(pair):
        await message.answer(f"✅ Пара {pair} удалена!")
    else:
        await message.answer(f"❌ Пара {pair} не найдена или не удалена.")
    await state.clear()
    await cmd_start(message, state)


@dp.message(Form.REMOVE_PAIR_SELECT, F.text == "🔙 Назад")
async def back_from_remove_pair(message: types.Message, state: FSMContext):
    # из меню удаления → возвращаемся в меню «Пары»
    await pairs_menu(message, state)

@dp.message(Form.PAIRS_MENU, F.text == "🔙 Назад")
async def back_from_pairs(message: types.Message, state: FSMContext):
    # из меню «Пары» → в «Настройки»
    await settings_menu(message, state)

@dp.message(Form.SETTINGS, F.text == "🔙 Назад")
async def back_from_settings(message: types.Message, state: FSMContext):
    # из «Настройки» → в главное меню
    await cmd_start(message, state)


# =====================
# ЗАПУСК БОТА
# =====================

# Примечание: раньше здесь был обработчик колбэка "execute_..." для ручного
# подтверждения сделки inline-кнопкой. Он был нерабочим (вызывал несуществующий
# arbitrage.check_now_profit и db.get_pair_contracts со столбцами, которых нет в
# текущей схеме БД - см. Database._init_db) и никогда не мог сработать, так как
# ни один код в этом проекте больше не отправляет кнопки с callback_data
# "execute_...". Сделки сейчас выполняются полностью автоматически внутри
# Arbitrage.make_trade. Убрано как мёртвый/нерабочий код.
async def main1():
    await dp.start_polling(bot)


async def request_new_uid(chat_id: int):
    await bot.send_message(
        chat_id,
        "⚠️ Ваш U_ID устарел. Пожалуйста, введите новый U_ID:"
    )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main1())
