"""
Диагностика quote_local() / confirm_price_onchain() без запуска всего бота.

Подключается к пулу ОДНОЙ пары из БД, один раз читает текущее состояние (rak,
резервы/liquidity) и печатает таблицу: локальная (без RPC) оценка цены под разные
объёмы сделки против реальной on-chain котировки от роутера (тот же вызов, что
используется в реальном свопе). Не отправляет никаких транзакций - только
.call() (staticcall), газ не тратится, баланс не трогается.

Использование (из корня репозитория):
    python -m arb.tools.debug_quote_local [ИМЯ_ПАРЫ]

Если имя не указано - берётся первая пара из БД.
"""
import asyncio
import sys

from arb.storage.database import Database
from arb.dex.pancake import OkxTrade
from config import INITIAL_RAK_PRICE


async def check_pair(pair_name: str, sizes_usdt, sizes_token):
    db = Database()
    pair_data = db.get_pair_data(pair_name)
    if not pair_data:
        print(f"Пара '{pair_name}' не найдена в БД. Доступные пары: {db.get_all_pairs()}")
        return

    pancake = OkxTrade(
        pair_name,
        pair_data['address_contract'],
        pair_data['abi'],
        pair_data['decimals'],
        INITIAL_RAK_PRICE,
        pair_data['private_key'],
        pair_data['websocket'],
        pair_data['rpc'],
        db,
        token_contract=pair_data['contract_bsc'],
    )

    pool = pancake.rpc.eth.contract(address=pancake.address, abi=pancake.abi)

    print("Читаю состояние пула...")
    side = await pancake.side(pool)
    pancake.rak = await pancake.get_rak(side, pool)
    await pancake._init_local_quote_state(pool)

    print("Проверяю allowance к роутеру для обоих токенов пула "
          "(нужно для confirm_price_onchain - иначе там будет revert 'STF'; "
          "если approve ещё не выдан, отправится РЕАЛЬНАЯ tx с небольшим газом)...")
    await pancake._ensure_router_allowances()

    print("=" * 78)
    print(f"Пара: {pair_name}")
    print(f"pool_kind: {pancake.pool_kind} | usdt_is_token0: {pancake.usdt_is_token0} | "
          f"_local_quote_ready: {pancake._local_quote_ready}")
    print(f"rak (mid-цена из get_rak, для сверки): {pancake.rak}")
    if pancake.pool_kind == 'v3':
        print(f"fee_ppm: {pancake.v3_fee_ppm} | liquidity: {pancake.v3_liquidity} | tick: {pancake.v3_tick}")
    elif pancake.pool_kind == 'v2':
        print(f"reserve0_raw: {pancake.v2_reserve0_raw} | reserve1_raw: {pancake.v2_reserve1_raw}")
    print("=" * 78)

    if not pancake._local_quote_ready:
        print("Локальная модель не готова (см. сообщение об ошибке выше) - дальше проверять нечего.")
        return

    print("\n--- ПОКУПКА токена за USDT (is_buy=True, аналог self.pancakce.buy) ---")
    print(f"{'USDT':>10} | {'local price':>14} | {'impact%':>8} | {'confirm?':>8} | {'on-chain price':>15} | {'diff%':>7}")
    for usdt_amount in sizes_usdt:
        await _print_row(pancake, usdt_amount, is_buy=True)

    print("\n--- ПРОДАЖА токена за USDT (is_buy=False, аналог self.pancakce.sell) ---")
    print(f"{'TOKEN':>10} | {'local price':>14} | {'impact%':>8} | {'confirm?':>8} | {'on-chain price':>15} | {'diff%':>7}")
    for token_amount in sizes_token:
        await _print_row(pancake, token_amount, is_buy=False)

    print("\nГотово. 'local price' - то, что использует analyze_opportunities без похода в сеть.")
    print("'on-chain price' - реальная котировка от роутера (то, что реально исполнится).")
    print("Большой diff% на маленьких объёмах = повод перепроверить формулу/калибровку.")
    print("confirm=True означает, что именно на этом объёме бот в реальном цикле пойдёт в сеть.")


async def _print_row(pancake: OkxTrade, amount: float, is_buy: bool):
    q = pancake.quote_local(amount, is_buy=is_buy)
    if q is None:
        print(f"{amount:>10.2f} | quote_local вернул None")
        return
    confirmed = await pancake.confirm_price_onchain(amount, is_buy=is_buy, timeout=5.0)
    if confirmed is not None:
        diff = (confirmed - q['effective_price']) / q['effective_price'] * 100
        confirmed_str = f"{confirmed:.8f}"
        diff_str = f"{diff:+.3f}"
    else:
        confirmed_str = "N/A (ошибка/нет ликвидности)"
        diff_str = "-"
    print(f"{amount:>10.2f} | {q['effective_price']:>14.8f} | {q['impact_pct']:>7.3f}% | "
          f"{str(q['needs_confirmation']):>8} | {confirmed_str:>15} | {diff_str:>6}%")


if __name__ == '__main__':
    pair_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if not pair_arg:
        _db = Database()
        _pairs = _db.get_all_pairs()
        if not _pairs:
            print("В БД нет пар.")
            sys.exit(1)
        pair_arg = _pairs[0]
        print(f"Имя пары не указано, беру первую из БД: {pair_arg}")

    # Небольшой разброс объёмов - от заведомо мелких до крупных, чтобы увидеть
    # рост impact_pct и момент, когда включается needs_confirmation.
    _sizes_usdt = [5, 20, 100, 500, 2000, 10000]
    _sizes_token = [5, 20, 100, 500, 2000, 10000]

    asyncio.run(check_pair(pair_arg, _sizes_usdt, _sizes_token))
