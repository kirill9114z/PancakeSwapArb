"""Результат DEX-свопа и классификация его ошибок.

Вынесено отдельным модулем, потому что на этих двух типах держится весь разбор
исхода сделки в arb.core.execution: своп никогда не бросает исключение и всегда
возвращает SwapResult, а решение "закрывать позицию на MEXC или нет" принимается
по SwapErrorType.
"""
from enum import Enum
from typing import Optional


class SwapErrorType(Enum):
    SUCCESS = "success"
    RETRYABLE_SLIPPAGE = "retryable_slippage"
    RETRYABLE_IMPACT = "retryable_impact"
    RETRYABLE_LIQUIDITY = "retryable_liquidity"
    RETRYABLE_GAS = "retryable_gas"
    RETRYABLE_NONCE = "retryable_nonce"
    FATAL_NO_GAS = "fatal_no_gas"
    FATAL_TOKEN_ISSUE = "fatal_token_issue"
    FATAL_NO_LIQUIDITY = "fatal_no_liquidity"
    # Транзакция ОТПРАВЛЕНА, но её итог неизвестен (таймаут ожидания receipt, обрыв
    # связи). Принципиально отличается от остальных FATAL_*: своп мог и пройти, и не
    # пройти. Вызывающий код НЕ должен на это автоматически закрывать позицию обратно
    # на MEXC - если своп всё-таки исполнится, обратная сделка создаст вторую
    # (уже никем не захеджированную) позицию вместо того чтобы закрыть первую.
    # Правильная реакция - громкий алерт и ручная проверка по tx_hash.
    FATAL_UNKNOWN_STATUS = "fatal_unknown_status"


class SwapResult:
    def __init__(self, success: bool, tx_hash: Optional[str] = None,
                 error_type: SwapErrorType = SwapErrorType.SUCCESS,
                 error_msg: str = ""):
        self.success = success
        self.tx_hash = tx_hash
        self.error_type = error_type
        self.error_msg = error_msg
