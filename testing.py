from web3 import Web3
import time
import json

from web3 import Web3
import time
import json
from eth_abi import encode # или другой ABI-энкодер


from web3 import Web3
import time
import json


def swap_universal(
        rpc_url: str,
        private_key: str,
        token_in: str,
        token_out: str,
        amount_in_human: float,
        slippage: float = 0.05,
):
    """
    Выполняет свап через PancakeSwap Smart Router (0x13f4...Dd4), используя универсальные методы.
    Аргументы:
      rpc_url — URL RPC BSC
      private_key — приватный ключ
      token_in — адрес входного токена
      token_out — адрес выходного токена
      amount_in_human — количество токена входного в «человеческом» формате
      slippage — допустимое проскальзывание (например, 0.05 = 5 %)
      use_fee_on_transfer — если токен с комиссией (для V2 совместимости)
    Возвращает: (tx_hash, fee_paid_in_BNB)
    """
    # 1. Подключение
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    account = w3.eth.account.from_key(private_key)
    from_addr = Web3.to_checksum_address(account.address)

    # 2. Smart Router настройка
    router_addr = Web3.to_checksum_address("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")
    with open('pancake_router_v2_abi.json', 'r') as f:
        smart_router_abi = json.load(f)
    # smart_router_abi = [...]  # Ваш ABI здесь

    router = w3.eth.contract(address=router_addr, abi=smart_router_abi)

    # 3. ERC-20 ABI
    with open('erc20_abi.json', 'r') as f:
        erc20_abi = json.load(f)

    token_in_addr = Web3.to_checksum_address(token_in)
    token_out_addr = Web3.to_checksum_address(token_out)

    WBNB = Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")

    # 4. Получение decimals для токенов
    token_in_contract = w3.eth.contract(address=token_in_addr, abi=erc20_abi)

    decimals_in = 18
    decimals_out = 18

    amount_in = int(amount_in_human * (10 ** decimals_in))

    # 5. Approve для Smart Router
    nonce = w3.eth.get_transaction_count(from_addr)
    allowance = token_in_contract.functions.allowance(from_addr, router_addr).call()

    if allowance < amount_in:
        tx = token_in_contract.functions.approve(router_addr, amount_in).build_transaction({
            'from': from_addr,
            'nonce': nonce,
            'gasPrice': w3.eth.gas_price,
        })
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        txh = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(txh)
        if receipt.status != 1:
            raise Exception("Approve failed")
        nonce += 1  # Увеличиваем nonce после approve

    # 6. Оценка выхода через статический вызов
    # Для Smart Router используем прямой путь [token_in, token_out]
    # Router сам выберет лучший пул (V2, V3 или stable)
    path = [token_in_addr, token_out_addr]
    amount_out_est = 0

    try:
        # СПОСОБ 1: Попробуем использовать exactInputSingle для оценки (V3 pools)
        # Нужно указать fee - попробуем самые распространенные
        common_fees = [100, 500, 2500, 3000]  # 0.01%, 0.05%, 0.25%, 0.3%

        for fee in common_fees:
            try:
                params = {
                    'tokenIn': token_in_addr,
                    'tokenOut': token_out_addr,
                    'fee': fee,
                    'recipient': from_addr,
                    'amountIn': amount_in,
                    'amountOutMinimum': 1,  # минимальное, но не нулевое значение
                    'sqrtPriceLimitX96': 0
                }
                amount_out_est = router.functions.exactInputSingle(params).call()
                if amount_out_est > 0:
                    print(f'Estimated output (V3 fee {fee}): {amount_out_est / (10 ** decimals_out)}')
                    break
            except:
                continue

        # СПОСОБ 3: Если все else fails, используем прямой swap метод с минимальным amountOutMin
        if amount_out_est == 0:
            try:
                amount_out_est = router.functions.swapExactTokensForTokens(
                    amount_in,
                    1,  # НЕ НУЛЬ, а минимальное значение
                    path,
                    from_addr
                ).call()
                print(f'Estimated output (Direct): {amount_out_est / (10 ** decimals_out)}')
            except Exception as e:
                print(f"All estimation methods failed: {e}")
                amount_out_est = 0

    except Exception as e:
        print(f"Estimation completely failed: {e}")
        amount_out_est = 0

    # 7. Расчет минимального выхода с учетом slippage
    if amount_out_est > 0:
        amount_out_min = int(amount_out_est * (1 - slippage))
        print(f'Minimum output with slippage: {amount_out_min / (10 ** decimals_out)}')
    else:
        amount_out_min = int(amount_in * 0.8)  # адаптируйте этот множитель
        print(f'Using conservative minimum: {amount_out_min / (10 ** decimals_out)}')
    time.sleep(30000)


    # Используем универсальный метод swapExactTokensForTokens
    # Smart Router сам выберет оптимальный маршрут через V2/V3/Stable пулы
    txn = router.functions.swapExactTokensForTokens(
        amount_in,
        amount_out_min,
        path,  # Простой путь - роутер сам найдет оптимальный маршрут
        from_addr,
    ).build_transaction({
        'from': from_addr,
        'gasPrice': w3.eth.gas_price,
        'nonce': nonce,
    })

    # 9. Подписать и отправить
    signed_txn = w3.eth.account.sign_transaction(txn, private_key=private_key)
    tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

    if receipt.status != 1:
        raise Exception(f"Swap failed, tx hash: {tx_hash.hex()}")

    # 10. Вычислить комиссию
    gas_used = receipt.gasUsed
    fee = w3.from_wei(gas_used * w3.eth.gas_price, 'ether')

    print(f"Transaction successful! TxHash: {tx_hash.hex()}, fee: {fee} BNB")

    # 11. Получить фактическое количество полученных токенов из логов
    actual_out = 0
    for log in receipt.logs:
        try:
            # Парсим Transfer событие для выходного токена
            if log['address'].lower() == token_out_addr.lower():
                transfer_event = w3.eth.contract(
                    address=token_out_addr,
                    abi=erc20_abi
                ).events.Transfer().process_log(log)
                if transfer_event['args']['to'].lower() == from_addr.lower():
                    actual_out = transfer_event['args']['value']
                    break
        except:
            continue

    if actual_out > 0:
        print(f'Actual output: {actual_out / (10 ** decimals_out)}')

    return tx_hash.hex(), fee
# def swap_universal(
#     rpc_url: str,
#     private_key: str,
#     token_in: str,
#     token_out: str,
#     amount_in_human: float,
#     slippage: float = 0.05,
# ):
#     """
#     Попытка свапа через Smart Router (универсальный), с перебором маршрутов и минимальным выходом.
#     Возвращает (tx_hash, fee_in_BNB).
#     """
#
#     w3 = Web3(Web3.HTTPProvider(rpc_url))
#     account = w3.eth.account.from_key(private_key)
#     from_addr = Web3.to_checksum_address(account.address)
#
#     # Адрес Smart Router (тот, что ты используешь)
#     router_addr = Web3.to_checksum_address("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")
#     with open('pancake_router_v2_abi.json', 'r') as f:
#         smart_router_abi = json.load(f)
#     router = w3.eth.contract(address=router_addr, abi=smart_router_abi)
#
#     # ERC-20 ABI
#     with open('erc20_abi.json', 'r') as f:
#         erc20_abi = json.load(f)
#
#     token_in_addr = Web3.to_checksum_address(token_in)
#     token_out_addr = Web3.to_checksum_address(token_out)
#
#     token_in_contract = w3.eth.contract(address=token_in_addr, abi=erc20_abi)
#     token_out_contract = w3.eth.contract(address=token_out_addr, abi=erc20_abi)
#
#     # Получаем реальные decimals токенов
#     decimals_in = 18
#     decimals_out = 18
#
#     # Перевод “человеческое” значение в базовую единицу
#     amount_in = int(amount_in_human * (10 ** decimals_in))
#
#     # 1. Approve / разрешение на списание токенов Smart Router
#     nonce = w3.eth.get_transaction_count(from_addr)
#     allowance = token_in_contract.functions.allowance(from_addr, router_addr).call()
#     if allowance < amount_in:
#         tx = token_in_contract.functions.approve(router_addr, amount_in).build_transaction({
#             'from': from_addr,
#             'nonce': nonce,
#             'gasPrice': w3.eth.gas_price,
#         })
#         signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
#         txh = w3.eth.send_raw_transaction(signed.raw_transaction)
#         receipt = w3.eth.wait_for_transaction_receipt(txh)
#         if receipt.status != 1:
#             raise Exception("Approve failed")
#         nonce += 1
#
#     # 2. Генерация candidate маршрутов (path) для оценки
#     WBNB = Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
#     # Можно расширить список промежуточных токенов по мере необходимости
#     candidates = []
#     candidates.append([token_in_addr, token_out_addr])
#     best_path = None
#     best_estimated = 0
#
#     # 3. Оценка каждого candidate маршрута
#     for path in candidates:
#         try:
#             # Используем call() метода swapExactTokensForTokens (если он поддерживается) для оценки
#             # или другой метод оценки (exactInput / другие), в зависимости от ABI
#             # Здесь я предполагаю, что smart_router содержит метод `swapExactTokensForTokens` для оценки
#             estimated = router.functions.swapExactTokensForTokens(
#                 amount_in,
#                 0,
#                 path,
#                 from_addr
#             ).call()[-1]
#             print("Path:", path, " => estimated output:", estimated / (10 ** decimals_out))
#             time.sleep(300)
#             if estimated > best_estimated:
#                 best_estimated = estimated
#                 best_path = path
#         except Exception as e:
#             # оценка не удалась — пропускаем
#             # print("Path", path, "estimation failed:", e)
#             print(f'111: {e} {candidates}')
#             pass
#
#     # if best_path is None:
#     #     raise Exception("No valid path found")
#
#     print("Selected best path:", best_path, "with estimate:", best_estimated / (10 ** decimals_out))
#
#     # 4. Вычисление минимального выхода с учётом slippage
#     amount_out_min = int(best_estimated * (1 - slippage))
#     if amount_out_min <= 0:
#         amount_out_min = 1
#
#
#     txn = router.functions.swapExactTokensForTokens(
#         amount_in,
#         amount_out_min,
#         best_path,
#         from_addr
#     ).build_transaction({
#         'from': from_addr,
#         'gasPrice': w3.eth.gas_price,
#         'nonce': nonce,
#     })
#
#     signed_txn = w3.eth.account.sign_transaction(txn, private_key=private_key)
#     tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)
#     receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
#
#     if receipt.status != 1:
#         raise Exception(f"Swap failed, tx hash: {tx_hash.hex()}")
#
#     gas_used = receipt.gasUsed
#     fee = w3.from_wei(gas_used * w3.eth.gas_price, 'ether')
#
#     print(f"Transaction succeeded: {tx_hash.hex()}, fee: {fee} BNB")
#
#     # 7. Определяем фактический полученный выход токена через события Transfer
#     actual_out = 0
#     for log in receipt.logs:
#         try:
#             if log['address'].lower() == token_out_addr.lower():
#                 transfer_event = w3.eth.contract(address=token_out_addr, abi=erc20_abi).events.Transfer().process_log(log)
#                 if transfer_event['args']['to'].lower() == from_addr.lower():
#                     actual_out = transfer_event['args']['value']
#                     break
#         except:
#             continue
#
#     if actual_out > 0:
#         print("Actual output:", actual_out / (10 ** decimals_out))
#
#     return tx_hash.hex(), fee

if __name__ == "__main__":
    from config import RPC_BSC
    rpc_url = RPC_BSC
    private_key = "0x698fd17a5f9deca8a842d457f0a82edadced4175d4e498926d6f85f766973d42"
    token_in = "0x55d398326f99059fF775485246999027B3197955"
    token_out = "0x6985884C4392D348587B19cb9eAAf157F13271cd"
    amount_in_human = 0.5
    res, fee = swap_universal(rpc_url, private_key, token_in, token_out, amount_in_human)
    print(f'Result for swap: {res}\nFee: {fee}')