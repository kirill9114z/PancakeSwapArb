

import asyncio
import json
import time
from typing import Tuple, List, Optional

from web3 import AsyncWeb3, AsyncHTTPProvider, Web3
from eth_account import Account
# если нужно: from eth_account.signers.local import LocalAccount

async def swap_universal_async(
    rpc_url: str,
    private_key: str,
    token_in: str,
    token_out: str,
    amount_in_human: float,
    slippage: float = 0.08,
    max_price_impact: float = 0.1,
    min_profit_percent: float = 0.0,
):

    # --- init async web3 and account ---
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    account = Account.from_key(private_key)
    from_addr = Web3.to_checksum_address(account.address)

    # загрузка ABI (блокирующая) — выполняем в отдельном потоке
    with open('pancake_router_v2_abi.json', 'r') as f:
        smart_router_abi = json.load(f)
    with open('erc20_abi.json', 'r') as f:
        erc20_abi = json.load(f)

    router_addr = Web3.to_checksum_address("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")
    router = w3.eth.contract(address=router_addr, abi=smart_router_abi)
    WBNB = Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
    t = time.time()
    token_in_addr = Web3.to_checksum_address(token_in)
    token_out_addr = Web3.to_checksum_address(token_out)


    # decimals — при желании можно получить через contract.functions.decimals().call()
    decimals_in = 18
    decimals_out = 18

    amount_in = int(amount_in_human * (10 ** decimals_in))

    token_in_contract = w3.eth.contract(address=token_in_addr, abi=erc20_abi)

    # --- nonce & allowance ---
    nonce = await w3.eth.get_transaction_count(from_addr)
    allowance = await token_in_contract.functions.allowance(from_addr, router_addr).call()

    if allowance < amount_in:
        # build approve tx (build_transaction синхронный, но недолго)
        tx = token_in_contract.functions.approve(router_addr, amount_in).build_transaction({
            'from': from_addr,
            'nonce': nonce,
            'gasPrice': await w3.eth.gas_price,
        })
        # sign in thread (blocking)
        signed = await asyncio.to_thread(Account.sign_transaction, tx, private_key)
        txh = await w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = await w3.eth.wait_for_transaction_receipt(txh)
        if receipt.status != 1:
            raise Exception("Approve failed")
        nonce += 1

    debug: List[str] = []

    candidate_paths = [
        [token_in_addr, token_out_addr],
        [token_in_addr, WBNB, token_out_addr],
    ]

    # === 1) Estimate V3 (exactInputSingle) concurrently for fees ===
    amount_out_est_v3 = 0
    v3_fee_used: Optional[int] = None
    common_fees = [100, 500, 2500, 3000]

    async def try_v3_fee(fee: int):
        nonlocal amount_out_est_v3, v3_fee_used
        try:
            params = (
                token_in_addr,
                token_out_addr,
                fee,
                from_addr,
                amount_in,
                1,
                0
            )
            # Contract call is awaitable in AsyncWeb3
            res = await router.functions.exactInputSingle(params).call({'from': from_addr})
            # res может быть int или tuple
            res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
            debug.append(f"V3 exactInputSingle fee {fee} -> {res_val}")
            # обновляем общий максимум
            if res_val > amount_out_est_v3:
                amount_out_est_v3 = res_val
                v3_fee_used = fee
        except Exception as e:
            debug.append(f"V3 fee {fee} failed: {e}")

    await asyncio.gather(*(try_v3_fee(f) for f in common_fees))

    # === 2) Estimate V2 (swapExactTokensForTokens) concurrently for paths ===
    amount_out_est_v2 = 0
    v2_path_used = None

    async def try_v2_path(path: List[str]):
        nonlocal amount_out_est_v2, v2_path_used
        try:
            res = await router.functions.swapExactTokensForTokens(amount_in, 1, path, from_addr).call({'from': from_addr})
            res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
            debug.append(f"V2 path {path} -> {res_val}")
            if res_val > amount_out_est_v2:
                amount_out_est_v2 = res_val
                v2_path_used = path
        except Exception as e:
            debug.append(f"V2 path {path} failed: {e}")

    await asyncio.gather(*(try_v2_path(p) for p in candidate_paths))

    if amount_out_est_v3 == 0 and amount_out_est_v2 == 0:
        for m in debug:
            print(m)
        raise Exception("All estimation methods failed: no V3 or V2 estimate available.")

    if amount_out_est_v3 >= amount_out_est_v2:
        chosen_type = 'v3'
        chosen_amount_out_est = amount_out_est_v3
        debug.append(f"Chosen V3 (fee {v3_fee_used}) amount_out_est {chosen_amount_out_est}")
    else:
        chosen_type = 'v2'
        chosen_amount_out_est = amount_out_est_v2
        debug.append(f"Chosen V2 path {v2_path_used} amount_out_est {chosen_amount_out_est}")

    print(f'1: {amount_out_est_v3/10**18}\n2:{amount_out_est_v2/10**18}')
    for m in debug:
        print(m)
    print(f"Chosen type: {chosen_type}, expected out: {chosen_amount_out_est / (10**decimals_out)}")
    amount_out_min = int(chosen_amount_out_est * (1 - slippage))
    print(f"amount_out_min with slippage {slippage}: {amount_out_min / (10**decimals_out)}")

    # Проверка min_profit_percent
    if min_profit_percent > 0:
        expected_profit_fraction = min_profit_percent / 100.0
        approx_required = int(amount_in * (1 + expected_profit_fraction) * (10 ** (decimals_out - decimals_in)))
        if chosen_amount_out_est < approx_required:
            raise Exception(f"Estimated output {chosen_amount_out_est / (10**decimals_out)} does not satisfy min_profit_percent={min_profit_percent}%")

    # Если V2 — проверяем пары и price impact (последовательно, т.к. читаем состояние)
    if chosen_type == 'v2':
        try:
            factory_v2 = await router.functions.factoryV2().call()
        except Exception:
            factory_v2 = None

        if factory_v2:
            factory_abi = [
                {"constant": True, "inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"}], "name":"getPair","outputs":[{"name":"pair","type":"address"}], "type":"function"}
            ]
            factory = w3.eth.contract(address=factory_v2, abi=factory_abi)
            # проверяем каждую пару
            for i in range(len(v2_path_used)-1):
                a = v2_path_used[i]
                b = v2_path_used[i+1]
                try:
                    pair_addr = await factory.functions.getPair(a, b).call()
                except Exception:
                    pair_addr = None
                if not pair_addr or int(pair_addr, 16) == 0:
                    raise Exception(f"V2 pair for hop {a}->{b} not found (pair addr zero). Aborting for safety.")

                pair_abi = [
                    {"constant": True, "inputs": [], "name":"getReserves", "outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}], "type":"function"},
                    {"constant": True, "inputs": [], "name":"token0", "outputs":[{"name":"","type":"address"}], "type":"function"},
                    {"constant": True, "inputs": [], "name":"token1", "outputs":[{"name":"","type":"address"}], "type":"function"},
                ]
                pair = w3.eth.contract(address=pair_addr, abi=pair_abi)
                token0 = await pair.functions.token0().call()
                r0, r1, _ = await pair.functions.getReserves().call()
                if token0.lower() == a.lower():
                    reserve_in = r0
                    reserve_out = r1
                else:
                    reserve_in = r1
                    reserve_out = r0

                fee_multiplier_num = 10000 - 25  # 0.25% fee
                numerator = amount_in * fee_multiplier_num * reserve_out
                denominator = reserve_in * 10000 + amount_in * fee_multiplier_num
                estimated_by_pair = numerator // denominator if denominator > 0 else 0

                price_before = (reserve_out / (10**decimals_out)) / (reserve_in / (10**decimals_in)) if reserve_in > 0 else float('inf')
                price_after = ((reserve_out - estimated_by_pair) / (10**decimals_out)) / ((reserve_in + amount_in) / (10**decimals_in)) if (reserve_in + amount_in) > 0 else 0
                impact = abs(price_after - price_before) / price_before if price_before not in (0, float('inf')) else 1.0

                print(f"V2 hop {a}->{b}: est_out {estimated_by_pair/(10**decimals_out)}, impact {impact:.6f}")
                if impact > max_price_impact:
                    raise Exception(f"Price impact {impact:.6f} on hop {a}->{b} exceeds limit {max_price_impact}. Aborting for safety.")
        else:
            raise Exception("factoryV2 not available from router — cannot check V2 pair impacts. Aborting for safety.")

    # --- подготовка транзакции: exactInputSingle или swapExactTokensForTokens ---
    if chosen_type == 'v3':
        if v3_fee_used is None:
            raise Exception("No usable V3 fee / estimation found though chosen_type==v3")
        params_exec = (
            token_in_addr,
            token_out_addr,
            v3_fee_used,
            from_addr,
            amount_in,
            amount_out_min,
            0
        )
        txn_func =  router.functions.exactInputSingle(params_exec)
    else:
        txn_func =  router.functions.swapExactTokensForTokens(
            amount_in,
            amount_out_min,
            v2_path_used,
            from_addr
        )

    # оценка газа (async)
    try:
        gas_est = await txn_func.estimate_gas({'from': from_addr})
    except Exception:
        gas_est = 400000

    txn = await txn_func.build_transaction({
        'from': from_addr,
        'gas': int(gas_est * 1.1),
        'gasPrice': await w3.eth.gas_price,
        'nonce': nonce,
    })

    # sign (in thread) и отправка
    signed_txn = await asyncio.to_thread(Account.sign_transaction, txn, private_key)
    tx_hash = await w3.eth.send_raw_transaction(signed_txn.raw_transaction)
    receipt = await w3.eth.wait_for_transaction_receipt(tx_hash)

    if receipt.status != 1:
        raise Exception(f"Swap failed, tx hash: {tx_hash.hex()}")

    gas_used = receipt.gasUsed
    fee = Web3.from_wei(gas_used * (await w3.eth.gas_price), 'ether')

    print(f"Transaction successful! TxHash: {tx_hash.hex()}, fee: {fee} BNB")

    # извлечение фактического вывода (парсим события Transfer) — синхронная логика через to_thread
    actual_out = 0
    token_out_contract = w3.eth.contract(address=token_out_addr, abi=erc20_abi)
    for log in receipt.logs:
        try:
            # process_log — синхронная функция в web3.py, вызываем в потоке
            processed = await asyncio.to_thread(token_out_contract.events.Transfer().process_log, log)
            if processed['args']['to'].lower() == from_addr.lower():
                actual_out = processed['args']['value']
                break
        except Exception:
            continue

    if actual_out > 0:
        print(f'Actual output: {actual_out / (10 ** decimals_out)} TIME {time.time() - t}')

    return tx_hash.hex(), float(fee)


async def main():
    from config import RPC_BSC
    rpc_url = RPC_BSC
    private_key = "0x698fd17a5f9deca8a842d457f0a82edadced4175d4e498926d6f85f766973d42"
    token_in = "0x55d398326f99059fF775485246999027B3197955"
    token_out = "0x0A8D6C86e1bcE73fE4D0bD531e1a567306836EA5"
    amount_in_human = 0.15
    res, fee = await swap_universal_async(rpc_url, private_key, token_in, token_out, amount_in_human)
    print(f'Result for swap: {res}\nFee: {fee}')

if __name__ == "__main__":
    asyncio.run(main())
