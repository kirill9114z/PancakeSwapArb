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

# НОВЫЕ параметры: max_price_impact (доля, 0.02 = 2%), min_profit_percent (например 0.5 = 0.5%)
from web3 import Web3
import json
from typing import Tuple

def swap_universal(
        rpc_url: str,
        private_key: str,
        token_in: str,
        token_out: str,
        amount_in_human: float,
        slippage: float = 0.08,
        max_price_impact: float = 0.1,
        min_profit_percent: float = 0.0,
):

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    account = w3.eth.account.from_key(private_key)
    from_addr = Web3.to_checksum_address(account.address)

    router_addr = Web3.to_checksum_address("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")
    with open('pancake_router_v2_abi.json', 'r') as f:
        smart_router_abi = json.load(f)
    router = w3.eth.contract(address=router_addr, abi=smart_router_abi)

    with open('erc20_abi.json', 'r') as f:
        erc20_abi = json.load(f)

    token_in_addr = Web3.to_checksum_address(token_in)
    token_out_addr = Web3.to_checksum_address(token_out)
    WBNB = Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")


    decimals_in = 18
    decimals_out = 18

    amount_in = int(amount_in_human * (10 ** decimals_in))

    # --- approve if needed ---
    token_in_contract = w3.eth.contract(address=token_in_addr, abi=erc20_abi)
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
        nonce += 1


    debug = []

    # candidate paths to try for V2/native router swap
    candidate_paths = [
        [token_in_addr, token_out_addr],
        [token_in_addr, WBNB, token_out_addr],
    ]

    # === 1) Estimate V3 (exactInputSingle) ===
    amount_out_est_v3 = 0
    v3_fee_used = None
    common_fees = [100, 500, 2500, 3000]  # try common ticks
    for fee in common_fees:
        try:
            # params tuple: (tokenIn, tokenOut, fee, recipient, amountIn, amountOutMinimum, sqrtPriceLimitX96)
            params = (
                token_in_addr,
                token_out_addr,
                fee,
                from_addr,
                amount_in,
                1,  # minimal non-zero for simulation
                0
            )
            res = router.functions.exactInputSingle(params).call({'from': from_addr})
            # res may be int or tuple
            res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
            debug.append(f"V3 exactInputSingle fee {fee} -> {res_val}")
            if res_val > amount_out_est_v3:
                amount_out_est_v3 = res_val
                v3_fee_used = fee
        except Exception as e:
            debug.append(f"V3 fee {fee} failed: {e}")
            continue

    # === 2) Estimate V2 (swapExactTokensForTokens via router for candidate paths) ===
    amount_out_est_v2 = 0
    v2_path_used = None
    for path in candidate_paths:
        try:
            res = router.functions.swapExactTokensForTokens(amount_in, 1, path, from_addr).call({'from': from_addr})
            # res may be uint or tuple - router returns amountOut (per ABI)
            res_val = int(res[0]) if isinstance(res, (list, tuple)) and len(res) >= 1 else int(res)
            debug.append(f"V2 path {path} -> {res_val}")
            if res_val > amount_out_est_v2:
                amount_out_est_v2 = res_val
                v2_path_used = path
        except Exception as e:
            debug.append(f"V2 path {path} failed: {e}")
            continue

    # if both zero -> error
    if amount_out_est_v3 == 0 and amount_out_est_v2 == 0:
        for m in debug:
            print(m)
        raise Exception("All estimation methods failed: no V3 or V2 estimate available. Check allowance / router methods / token compatibility.")

    # decide which is best
    chosen_type = None
    chosen_amount_out_est = 0
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

    # === Safety checks ===
    # compute amount_out_min by slippage
    amount_out_min = int(chosen_amount_out_est * (1 - slippage))
    print(f"amount_out_min with slippage {slippage}: {amount_out_min / (10**decimals_out)}")

    # min profit percent: rough check in token_out units (note: exact profit needs stable conversion)
    if min_profit_percent > 0:
        expected_profit_fraction = min_profit_percent / 100.0
        # rough check: require chosen_amount_out_est >= amount_in * (1 + expected_profit_fraction) adjusted for decimals if different
        # convert amount_in to token_out scale approximately: amount_in * 10^(decimals_out - decimals_in)
        approx_required = int(amount_in * (1 + expected_profit_fraction) * (10 ** (decimals_out - decimals_in)))
        if chosen_amount_out_est < approx_required:
            raise Exception(f"Estimated output {chosen_amount_out_est / (10**decimals_out)} does not satisfy min_profit_percent={min_profit_percent}% (required approx {approx_required/(10**decimals_out)})")

    # If chosen_type == 'v2' -> check price impact for every hop in chosen v2 path (to avoid tiny-pool effect)
    if chosen_type == 'v2':
        # attempt to get factoryV2 from router
        try:
            factory_v2 = router.functions.factoryV2().call()
        except Exception:
            factory_v2 = None

        if factory_v2:
            # minimal factory abi
            factory_abi = [
                {"constant": True, "inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"}], "name":"getPair","outputs":[{"name":"pair","type":"address"}], "type":"function"}
            ]
            factory = w3.eth.contract(address=factory_v2, abi=factory_abi)
            # check every hop (pair) in v2_path_used
            for i in range(len(v2_path_used)-1):
                a = v2_path_used[i]
                b = v2_path_used[i+1]
                try:
                    pair_addr = factory.functions.getPair(a, b).call()
                except Exception:
                    pair_addr = None
                if not pair_addr or int(pair_addr, 16) == 0:
                    # missing pair — can't compute impact -> refuse to do blind swap
                    raise Exception(f"V2 pair for hop {a}->{b} not found (pair addr zero). Aborting for safety.")
                # minimal pair abi
                pair_abi = [
                    {"constant": True, "inputs": [], "name":"getReserves", "outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}], "type":"function"},
                    {"constant": True, "inputs": [], "name":"token0", "outputs":[{"name":"","type":"address"}], "type":"function"},
                    {"constant": True, "inputs": [], "name":"token1", "outputs":[{"name":"","type":"address"}], "type":"function"},
                ]
                pair = w3.eth.contract(address=pair_addr, abi=pair_abi)
                token0 = pair.functions.token0().call()
                r0, r1, _ = pair.functions.getReserves().call()
                if token0.lower() == a.lower():
                    reserve_in = r0
                    reserve_out = r1
                else:
                    reserve_in = r1
                    reserve_out = r0

                # Estimate per-hop output by UniswapV2 formula (fee 0.25% assumed)
                fee_multiplier_num = 10000 - 25
                numerator = amount_in * fee_multiplier_num * reserve_out
                denominator = reserve_in * 10000 + amount_in * fee_multiplier_num
                estimated_by_pair = numerator // denominator if denominator > 0 else 0

                # price impact approx
                # convert to price terms (careful with decimals — approximate)
                price_before = (reserve_out / (10**decimals_out)) / (reserve_in / (10**decimals_in)) if reserve_in > 0 else float('inf')
                price_after = ((reserve_out - estimated_by_pair) / (10**decimals_out)) / ((reserve_in + amount_in) / (10**decimals_in)) if (reserve_in + amount_in) > 0 else 0
                impact = abs(price_after - price_before) / price_before if price_before not in (0, float('inf')) else 1.0

                print(f"V2 hop {a}->{b}: est_out {estimated_by_pair/(10**decimals_out)}, impact {impact:.6f}")
                if impact > max_price_impact:
                    raise Exception(f"Price impact {impact:.6f} on hop {a}->{b} exceeds limit {max_price_impact}. Aborting for safety.")
        else:
            raise Exception("factoryV2 not available from router — cannot check V2 pair impacts. Aborting for safety.")

    # === Build & send transaction ONLY via chosen_type ===
    if chosen_type == 'v3':
        # Build params tuple and call exactInputSingle with amountOutMinimum = amount_out_min
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
        txn_func = router.functions.exactInputSingle(params_exec)
    else:
        # chosen_type == 'v2'
        # use chosen v2 path v2_path_used
        txn_func = router.functions.swapExactTokensForTokens(
            amount_in,
            amount_out_min,
            v2_path_used,
            from_addr
        )

    # gas estimate
    try:
        gas_est = txn_func.estimate_gas({'from': from_addr})
    except Exception:
        gas_est = 400000
    txn = txn_func.build_transaction({
        'from': from_addr,
        'gas': int(gas_est * 1.1),
        'gasPrice': w3.eth.gas_price,
        'nonce': nonce,
    })

    signed_txn = w3.eth.account.sign_transaction(txn, private_key=private_key)
    tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

    if receipt.status != 1:
        raise Exception(f"Swap failed, tx hash: {tx_hash.hex()}")

    gas_used = receipt.gasUsed
    fee = w3.from_wei(gas_used * w3.eth.gas_price, 'ether')

    print(f"Transaction successful! TxHash: {tx_hash.hex()}, fee: {fee} BNB")

    # parse actual output (Transfer to from_addr)
    actual_out = 0
    for log in receipt.logs:
        try:
            if log['address'].lower() == token_out_addr.lower():
                transfer_event = w3.eth.contract(address=token_out_addr, abi=erc20_abi).events.Transfer().process_log(log)
                if transfer_event['args']['to'].lower() == from_addr.lower():
                    actual_out = transfer_event['args']['value']
                    break
        except Exception:
            continue

    if actual_out > 0:
        print(f'Actual output: {actual_out / (10 ** decimals_out)}')

    return tx_hash.hex(), fee


if __name__ == "__main__":
    from config import RPC_BSC
    rpc_url = RPC_BSC
    private_key = "0x698fd17a5f9deca8a842d457f0a82edadced4175d4e498926d6f85f766973d42"
    token_in = "0x55d398326f99059fF775485246999027B3197955"
    token_out = "0x0A8D6C86e1bcE73fE4D0bD531e1a567306836EA5"
    amount_in_human = 0.15
    res, fee = swap_universal(rpc_url, private_key, token_in, token_out, amount_in_human)
    print(f'Result for swap: {res}\nFee: {fee}')
