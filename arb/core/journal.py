"""Журнал исполнений: одна запись на каждую попытку сделки.

Зачем это нужно. Балансы (`Arbitrage.balance_*`) перечитываются с бирж после каждой
сделки и затирают собой всю историю, а в логах остаются только print'ы, которые никто
не агрегирует. Из-за этого на главный вопрос - зарабатывает бот или нет - ответить
нечем, и все пороги (`PROFIT_THRESHOLD_USD`, `DEX_BUY_MARKUP`, `SWAP_FAST_SLIPPAGE`)
приходится подбирать на глаз. Эта таблица закрывает ровно эту дыру: по ней считаются
доля успешно прошедших хеджей, реальное проскальзывание относительно прогноза
`quote_local`, стоимость аварийных закрытий и латентность каждого шага сделки.

Пишется ОДНА строка на попытку - включая попытки, которые вообще не наполнились на
MEXC, и те, что закончились аварийным закрытием: без неудачных попыток статистика
бессмысленна, потому что именно они и стоят денег.

Запись живёт как `Arbitrage._trade_record`, заполняется по ходу дела в
arb.core.execution и сохраняется ровно один раз - в `make_trade`, в блоке finally,
чтобы строка появилась в БД независимо от того, чем сделка кончилась.
"""
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

from config import MEXC_TAKER_FEE_RATE

# Человекочитаемые названия исходов сделки. Живут здесь, рядом с кодом, который
# эти исходы и проставляет (arb.core.execution), чтобы UI и логика не разъехались:
# добавили новый outcome - подпись к нему сразу видно тут же.
OUTCOME_LABELS = {
    'hedged': '✅ Захеджировано',
    'mexc_empty': '⚪ Ордер не наполнился',
    'mexc_rejected': '🚫 MEXC отклонил ордер',
    'swap_unknown': '❓ Своп: итог неизвестен',
    'emergency_closed': '🔄 Аварийно закрыто',
    'emergency_partial': '‼️ Закрыто не полностью',
    'error': '💥 Ошибка в ходе сделки',
    'unknown': '❔ Не классифицировано',
}


def outcome_label(outcome: Optional[str]) -> str:
    return OUTCOME_LABELS.get(outcome, f'❔ {outcome or "нет данных"}')


@dataclass
class TradeRecord:
    """Всё, что известно об одной попытке сделки. Поля заполняются по мере
    прохождения этапов, поэтому почти все опциональны: сделка может закончиться
    на любом из них."""

    pair: str
    trade_type: str                       # BUY_MEXC / SELL_MEXC
    ts: float = field(default_factory=time.time)

    # --- Что решили сделать (снимок кандидата из analyze_opportunities) ---
    level: Optional[int] = None           # какой уровень стакана победил
    planned_volume: Optional[float] = None
    planned_mexc_price: Optional[float] = None   # средняя цена по префиксу стакана
    planned_limit_price: Optional[float] = None  # цена, по которой реально ставим лимит
    planned_dex_price: Optional[float] = None
    dex_price_source: Optional[str] = None       # local / onchain / flat
    dex_impact_pct: Optional[float] = None
    planned_spread_pct: Optional[float] = None
    planned_profit_usd: Optional[float] = None   # уже за вычетом est_fee_usd
    est_fee_usd: Optional[float] = None
    analysis_seconds: Optional[float] = None     # сколько заняла итерация анализа

    # --- Нога на MEXC ---
    mexc_order_id: Optional[str] = None
    mexc_filled: Optional[float] = None
    mexc_avg_price: Optional[float] = None
    mexc_status: Optional[str] = None

    # --- Нога на DEX (хедж) ---
    swap_amount_in: Optional[float] = None
    swap_tx_hash: Optional[str] = None
    swap_error_type: Optional[str] = None
    swap_error_msg: Optional[str] = None
    swap_quote_source: Optional[str] = None      # local / onchain - откуда взяли amountOutMin
    gas_used: Optional[int] = None
    gas_price_gwei: Optional[float] = None
    gas_cost_usd: Optional[float] = None

    # --- Чем всё кончилось ---
    # mexc_rejected     - биржа не приняла ордер / сеть не ответила
    # mexc_empty        - ордер не наполнился вообще, откатывать нечего
    # hedged            - обе ноги прошли, позиция нейтральна
    # swap_unknown      - транзакция отправлена, итог неизвестен (ручной разбор)
    # emergency_closed  - своп не прошёл, позицию вернули обратно на MEXC
    # emergency_partial - вернули не полностью, пара остановлена
    # error             - исключение по ходу сделки
    outcome: Optional[str] = None
    emergency_closed_qty: Optional[float] = None
    emergency_remaining_qty: Optional[float] = None
    # Средняя цена, по которой позицию вернули обратно на MEXC. Нужна именно для
    # оценки убытка: без неё аварийное закрытие невозможно отличить от удачной
    # сделки, потому что плановая цена DEX в записи остаётся та же самая.
    emergency_avg_price: Optional[float] = None

    # --- Итог в деньгах ---
    # ОЦЕНКА, а не факт: точная цена исполнения свопа требует разбора логов receipt,
    # чего мы не делаем. Считается от фактического mexc_filled и той цены DEX, по
    # которой хедж отправлялся. Для сравнения сделок между собой и для оценки общего
    # знака результата этого достаточно; для бухгалтерии - нет.
    realized_pnl_usd: Optional[float] = None

    # --- Латентность по шагам, секунды от начала сделки ---
    t_order_placed: Optional[float] = None
    t_mexc_settled: Optional[float] = None
    t_swap_done: Optional[float] = None
    t_total: Optional[float] = None

    notes: Optional[str] = None

    # Момент входа в make_trade - база для всех t_* выше. В БД не пишется.
    _t0: float = field(default_factory=time.time, repr=False)

    def mark(self, field_name: str):
        """Проставляет отметку времени относительно начала сделки."""
        setattr(self, field_name, round(time.time() - self._t0, 4))

    def apply_candidate(self, best: dict):
        """Переносит в запись снимок решения, принятого analyze_opportunities."""
        self.level = best.get('level')
        self.planned_volume = _f(best.get('volume'))
        self.planned_mexc_price = _f(best.get('mexc_price'))
        self.planned_limit_price = _f(best.get('price'))
        self.planned_dex_price = _f(best.get('dex'))
        self.dex_price_source = best.get('dex_price_source')
        self.dex_impact_pct = _f(best.get('dex_impact_pct'))
        self.planned_spread_pct = _f(best.get('spread'))
        self.planned_profit_usd = _f(best.get('profit'))
        self.est_fee_usd = _f(best.get('fee'))
        self.analysis_seconds = _f(best.get('time'))

    def apply_swap_result(self, val, gas_cost_usd: Optional[float] = None):
        """Переносит в запись исход свопа. `val` - SwapResult."""
        self.swap_tx_hash = val.tx_hash
        self.swap_quote_source = getattr(val, 'quote_source', None)
        self.gas_used = getattr(val, 'gas_used', None)
        gas_price_wei = getattr(val, 'gas_price_wei', None)
        if gas_price_wei:
            self.gas_price_gwei = round(gas_price_wei / 1e9, 6)
        if not val.success:
            self.swap_error_type = val.error_type.value if val.error_type else None
            self.swap_error_msg = (val.error_msg or '')[:500]
        if gas_cost_usd is not None:
            self.gas_cost_usd = round(gas_cost_usd, 8)

    def estimate_pnl(self):
        """Оценка результата сделки в USD. Ветвится по outcome, и это принципиально.

        Первая версия этого метода считала PnL одинаково для всех исходов - по
        плановой цене DEX на фактически исполненном объёме. Для аварийно закрытой
        сделки это давало ПЛЮС примерно в размере ожидавшейся прибыли, хотя своп не
        состоялся и позицию возвращали себе в убыток. Ошибка не в точности, а в
        ЗНАКЕ: сложив такой журнал, можно увидеть прибыльного бота там, где его нет,
        то есть получить ровно противоположное тому, ради чего журнал заводился."""
        mexc_price = self.mexc_avg_price or self.planned_mexc_price

        # Ордер не наполнился / биржа его не приняла: денег не двигалось.
        if self.outcome in ('mexc_empty', 'mexc_rejected') or not self.mexc_filled:
            self.realized_pnl_usd = 0.0
            return

        # Итог свопа неизвестен или сделка упала до разбора хеджа - любое число
        # здесь было бы выдумкой. NULL честнее нуля: ноль сложится в сумму как
        # "ничего не потеряли", а это как раз те строки, которые надо смотреть руками.
        if self.outcome in ('swap_unknown', 'error', 'unknown') or not mexc_price:
            self.realized_pnl_usd = None
            return

        # Обе ноги прошли - считаем как задумано, по фактическому объёму.
        if self.outcome == 'hedged':
            if not self.planned_dex_price:
                self.realized_pnl_usd = None
                return
            if self.trade_type == 'BUY_MEXC':
                gross = (self.planned_dex_price - mexc_price) * self.mexc_filled
            else:
                gross = (mexc_price - self.planned_dex_price) * self.mexc_filled
            self.realized_pnl_usd = round(gross - self._scaled_fee(), 8)
            return

        # Аварийное закрытие: своп не прошёл, позицию вернули встречным ордером по
        # заведомо худшей цене. Прибыли тут нет по построению - есть стоимость
        # возврата. Считаем её по цене, которой реально закрывались, плюс комиссия
        # MEXC за ОБЕ ноги (вход и возврат) и газ, если транзакция всё-таки ушла.
        closed = self.emergency_closed_qty or 0.0
        close_price = self.emergency_avg_price
        if not closed or not close_price:
            self.realized_pnl_usd = None
            return
        if self.trade_type == 'BUY_MEXC':
            # Купили дорого на MEXC, продали обратно ещё дешевле.
            gross = (close_price - mexc_price) * closed
        else:
            # Продали на MEXC, выкупили обратно дороже.
            gross = (mexc_price - close_price) * closed
        fees = 2 * closed * mexc_price * MEXC_TAKER_FEE_RATE + (self.gas_cost_usd or 0.0)
        self.realized_pnl_usd = round(gross - fees, 8)

    def _scaled_fee(self) -> float:
        """est_fee_usd посчитана на ПЛАНОВЫЙ объём - масштабируем под фактический,
        иначе частичный фил выглядит убыточнее, чем он есть."""
        fee = self.est_fee_usd or 0.0
        if self.planned_volume and self.mexc_filled:
            fee *= min(1.0, self.mexc_filled / self.planned_volume)
        return fee

    def to_row(self) -> dict:
        row = {k: v for k, v in asdict(self).items() if not k.startswith('_')}
        row['ts_iso'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.ts))
        return row


def _f(value) -> Optional[float]:
    """float() без падения на None/мусоре - запись в журнал не имеет права
    уронить сделку."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
