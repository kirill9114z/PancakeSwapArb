"""Telegram-интерфейс: единственная точка управления ботом.

Здесь только UI и CRUD пар в БД - никакой торговой логики. Запуск/остановка
торговли сводится к созданию/отмене задачи arb.core.runner.main.

Все обработчики намеренно живут в одном модуле: aiogram регистрирует их в
порядке объявления через общий Dispatcher, и разнос по файлам менял бы порядок
разрешения хендлеров.
"""
import html
import logging
import time

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import ReplyKeyboardBuilder
import aiofiles
import os
from aiogram.types import ContentType
from arb.storage.database import Database
import asyncio
from arb.core.runner import main as arb_main
from arb.core.runner import active_arbitrage_instances
from arb.core.journal import outcome_label
from arb.paths import PAIR_ABI_DIR
from arb.telegram.states import Form

from config import BOT_TOKEN, SPREAD_INPUT_MIN_PCT, SPREAD_INPUT_MAX_PCT

db = Database()
arb_task: asyncio.Task | None = None

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
    builder.button(text="📊 Статистика")
    builder.adjust(2, 2, 1)
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
# СТАТИСТИКА (журнал исполнений, arb/core/journal.py)
# =====================
# Все агрегаты считаются запросом к таблице trades в момент показа - нигде не
# хранятся. Причина в Database.get_trade_stats: журнал единственный источник
# истины, а хранимая сумма разъезжается с ним при падениях и замораживает в себе
# старую формулу расчёта прибыли.

def _fmt_usd(value) -> str:
    """Деньги со знаком. Знак важен: минус в отчёте должен бросаться в глаза."""
    if value is None:
        return "—"
    return f"{value:+.4f}$" if value else "0.0000$"


def _fmt_ts(ts) -> str:
    return time.strftime('%d.%m.%Y %H:%M', time.localtime(ts)) if ts else "—"


def _fmt_secs(value) -> str:
    return f"{value:.3f} с" if value else "—"


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Русское склонение после числительного: 1 сделка, 2 сделки, 5 сделок."""
    tail = abs(int(n)) % 100
    if 11 <= tail <= 14:
        return many
    tail %= 10
    if tail == 1:
        return one
    if 2 <= tail <= 4:
        return few
    return many


def _trades_word(n: int) -> str:
    return f"{n} {_plural(n, 'сделка', 'сделки', 'сделок')}"


def _fmt_trade(trade: dict) -> str:
    """Одна сделка одной строкой - для 'самой прибыльной' и 'самой убыточной'."""
    if not trade:
        return "—"
    return (f"{_fmt_usd(trade.get('realized_pnl_usd'))} · {trade.get('trade_type') or '?'} · "
            f"объём {trade.get('mexc_filled') or 0:.6f} · {_fmt_ts(trade.get('ts'))}")


def _render_pair_stats(stats: dict) -> str:
    if not stats or not stats.get('trades'):
        return ("📊 По этой паре сделок ещё не было.\n\n"
                "Журнал заполняется только когда бот реально торгует: проверьте, что "
                "<code>test_mode = False</code> в config.py и вызов make_trade в "
                "analyze_opportunities не закомментирован.")

    lines = [f"📊 <b>{html.escape(str(stats['pair']))}</b>",
             f"<i>{_fmt_ts(stats['first_ts'])} — {_fmt_ts(stats['last_ts'])}</i>", ""]

    lines += [
        f"Всего попыток: <b>{stats['trades']}</b>",
        f"Оборот: {stats['volume_usd']:.2f}$",
        "",
        f"💰 <b>Итог: {_fmt_usd(stats['pnl_usd'])}</b>",
        f"  ├ заработано: {_fmt_usd(stats['profit_usd'])} ({_trades_word(stats['win_trades'])})",
        f"  └ потеряно: {_fmt_usd(stats['loss_usd'])} ({_trades_word(stats['loss_trades'])})",
        # Газ ОТДЕЛЬНОЙ строкой, а не веткой разбивки выше: он уже вычтен внутри
        # прибыли каждой сделки (est_fee_usd в TradeRecord.estimate_pnl включает
        # стоимость газа). Веткой "├ газ" он читался бы как третье слагаемое итога,
        # то есть как ещё один вычет поверх - и арифметика в отчёте не сходилась бы.
        f"⛽ Газ (уже учтён в итоге): {stats['gas_usd']:.4f}$",
    ]
    # Сделки с неизвестным итогом в сумму не попали вообще - если их много,
    # итоговое число ничего не значит, и это надо показать, а не спрятать.
    if stats['pnl_unknown_trades']:
        lines.append(f"⚠️ Не учтено в итоге (неизвестный исход): "
                     f"<b>{_trades_word(stats['pnl_unknown_trades'])}</b>")

    lines += ["", f"🎯 Лучшая: {_fmt_trade(stats.get('best'))}",
              f"📉 Худшая: {_fmt_trade(stats.get('worst'))}", ""]

    if stats['hedge_success_pct'] is not None:
        lines.append(f"🔗 Хедж прошёл: <b>{stats['hedge_success_pct']:.1f}%</b> "
                     f"({stats['hedged']} из {stats['reached_swap']} дошедших до свопа)")
    lines += [
        f"⏱ Среднее время сделки: {_fmt_secs(stats['avg_seconds'])}",
        f"⏱ Среднее время анализа: {_fmt_secs(stats['avg_analysis_seconds'])}",
        "",
        "<b>Исходы:</b>",
    ]
    for outcome, count in stats['by_outcome'].items():
        lines.append(f"  {html.escape(outcome_label(outcome))}: {count}")
    return "\n".join(lines)


def _render_overall_stats(stats: dict, by_pair: list) -> str:
    if not stats or not stats.get('trades'):
        return ("📊 Сделок в журнале пока нет.\n\n"
                "Журнал заполняется только когда бот реально торгует: проверьте, что "
                "<code>test_mode = False</code> в config.py и вызов make_trade в "
                "analyze_opportunities не закомментирован.")

    lines = [
        "🌍 <b>Общая статистика</b>",
        f"<i>{_fmt_ts(stats['first_ts'])} — {_fmt_ts(stats['last_ts'])}</i>",
        "",
        f"Всего попыток: <b>{_trades_word(stats['trades'])}</b>",
        f"💰 <b>Общая прибыль: {_fmt_usd(stats['pnl_usd'])}</b>",
        f"⛽ Газ (уже учтён в итоге): {stats['gas_usd']:.4f}$",
    ]
    if stats['pnl_unknown_trades']:
        lines.append(f"⚠️ Не учтено в итоге (неизвестный исход): "
                     f"<b>{_trades_word(stats['pnl_unknown_trades'])}</b>")
    lines += ["", "<b>По парам:</b>"]

    for item in by_pair:
        lines.append(
            f"  {html.escape(item['pair'])}: <b>{_fmt_usd(item['pnl_usd'])}</b> "
            f"({_trades_word(item['trades'])}, ✅ {item['hedged']})")
    return "\n".join(lines)


def _stats_keyboard(pairs) -> ReplyKeyboardBuilder:
    builder = ReplyKeyboardBuilder()
    builder.button(text="🌍 Общая статистика")
    for pair in pairs:
        builder.button(text=pair)
    builder.button(text="🔙 Назад")
    builder.adjust(1, 2)
    return builder


@dp.message(Form.SETTINGS, F.text == "📊 Статистика")
async def stats_menu(message: types.Message, state: FSMContext):
    pairs = db.get_all_pairs()
    await message.answer(
        "Выберите пару или посмотрите сводку по всем:",
        reply_markup=_stats_keyboard(pairs).as_markup(resize_keyboard=True)
    )
    await state.set_state(Form.STATS_SELECT)


@dp.message(Form.STATS_SELECT, F.text == "🔙 Назад")
async def stats_back(message: types.Message, state: FSMContext):
    await cmd_start(message, state)


@dp.message(Form.STATS_SELECT, F.text == "🌍 Общая статистика")
async def stats_overall(message: types.Message, state: FSMContext):
    await message.answer(
        _render_overall_stats(db.get_trade_stats(), db.get_profit_by_pair()),
        parse_mode="HTML"
    )


@dp.message(Form.STATS_SELECT)
async def stats_for_pair(message: types.Message, state: FSMContext):
    pair = message.text.upper()
    if pair not in db.get_all_pairs():
        await message.answer("❌ Пара не найдена! Выберите из списка:")
        return
    # Меню намеренно НЕ закрывается: после отчёта пользователь обычно смотрит
    # следующую пару, и возврат в главное меню каждый раз только мешает.
    await message.answer(_render_pair_stats(db.get_trade_stats(pair)), parse_mode="HTML")


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
        abi_filepath = os.path.join(PAIR_ABI_DIR, abi_filename)

        os.makedirs(PAIR_ABI_DIR, exist_ok=True)

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
