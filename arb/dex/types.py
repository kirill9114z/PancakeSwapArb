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
    """Исход свопа. Поля после error_msg - только для журнала исполнений
    (arb.core.journal): на логику разбора сделки они не влияют и всегда могут быть
    None, если своп не дошёл до receipt. Значения по умолчанию обязательны -
    SwapResult конструируется примерно в двух десятках мест."""

    def __init__(self, success: bool, tx_hash: Optional[str] = None,
                 error_type: SwapErrorType = SwapErrorType.SUCCESS,
                 error_msg: str = "",
                 gas_used: Optional[int] = None,
                 gas_price_wei: Optional[int] = None,
                 amount_out_est_raw: Optional[int] = None,
                 quote_source: Optional[str] = None):
        self.success = success
        self.tx_hash = tx_hash
        self.error_type = error_type
        self.error_msg = error_msg
        self.gas_used = gas_used
        self.gas_price_wei = gas_price_wei
        # Сколько выхода ОЖИДАЛОСЬ (в wei) и откуда взялась оценка ('local'/'onchain').
        # Нужно, чтобы по журналу сравнивать прогноз quote_local с реальностью.
        self.amount_out_est_raw = amount_out_est_raw
        self.quote_source = quote_source
