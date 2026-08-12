"""Чистые вычисления по стакану MEXC. Без сети, без состояния.

Здесь нет ни одного обращения к бирже или кошельку - только математика над уже
полученными уровнями стакана, поэтому функции одинаково используются и в анализе
возможностей (arb.core.market_data), и в аварийном закрытии позиции
(arb.core.emergency), и их легко проверить отдельно от бота.
"""
import math

from config import (
    MEXC_EMERGENCY_PRICE_BUFFER_PCT,
    MEXC_EMERGENCY_THIN_BOOK_BUFFER_PCT,
)

def compute_prefix_stats_with_max_sum(order_book_levels, max_sum):
    """Вычисляет кумулятивные суммы с учетом MAX_SUM, агрегируя ордера.
    Возвращает:
        - cum_amounts: список накопленных объемов
        - cum_costs: список накопленных стоимостей
        - avg_prices: список кортежей (средняя цена, цена текущего ордера)
    """
    prices, volumes = zip(*order_book_levels) if order_book_levels else ([], [])
    cum_amounts, cum_costs, avg_prices = [], [], []
    total_amount, total_cost = 0.0, 0.0

    for price, volume in zip(prices, volumes):
        remaining = max(0, max_sum - total_cost)
        if remaining <= 0:
            break

        available_volume = min(volume, remaining / price)
        new_cost = total_cost + available_volume * price

        if new_cost > max_sum:
            available_volume = (max_sum - total_cost) / price
            new_cost = total_cost + available_volume * price

        total_amount += available_volume
        total_cost = new_cost

        avg_price = total_cost / total_amount if total_amount else price
        avg_prices.append((avg_price, price)) 

        cum_amounts.append(total_amount)
        cum_costs.append(total_cost)

        if total_cost >= max_sum:
            break

    return cum_amounts, cum_costs, avg_prices

def price_decimals(levels) -> int:
    """Число знаков после запятой у цен в стакане. Цены из стакана заведомо
    соответствуют шагу цены инструмента, поэтому округление нашей агрессивной
    цены до той же точности защищает от отказа биржи по нарушению тика."""
    decimals = 0
    for price, _ in levels[:10]:
        text = repr(float(price))
        if '.' in text and 'e' not in text and 'E' not in text:
            decimals = max(decimals, len(text.split('.')[1]))
    return min(decimals, 12)

def marketable_price(levels, qty, close_is_sell):
    """Цена, которая гарантированно сметёт стакан на объём qty.

    Идём вглубь стакана, пока накопленного объёма не хватит на qty, берём цену
    последнего задетого уровня и сдвигаем её в невыгодную сторону на буфер -
    так ордер пересекает спред и забирает все уровни до нужного включительно.
    Если видимой глубины не хватило, буфер берётся увеличенный: за пределами
    MEXC_ORDERBOOK_DEPTH_LEVELS уровней ликвидность может найтись.

    Возвращает (price, enough_depth) либо (None, False), если стакан пуст."""
    if not levels:
        return None, False

    cumulative = 0.0
    touch_price = float(levels[0][0])
    enough_depth = False
    for price, volume in levels:
        touch_price = float(price)
        cumulative += float(volume)
        if cumulative >= qty:
            enough_depth = True
            break

    buffer_pct = MEXC_EMERGENCY_PRICE_BUFFER_PCT if enough_depth else MEXC_EMERGENCY_THIN_BOOK_BUFFER_PCT
    if close_is_sell:
        # Продаём в биды: цена НИЖЕ самого глубокого нужного бида.
        price = touch_price * (1 - buffer_pct / 100)
    else:
        # Выкупаем аски: цена ВЫШЕ самого глубокого нужного аска.
        price = touch_price * (1 + buffer_pct / 100)

    # Округляем всегда в сторону БОЛЬШЕЙ агрессии, чтобы округление не сделало
    # ордер вдруг непересекающим спред.
    #
    # Точность берём из стакана: его цены заведомо соответствуют шагу цены
    # инструмента, и лишние знаки могут привести к отказу по нарушению тика.
    # Но у этого есть вырожденный случай: если все видимые уровни оказались
    # круглыми (например единственный уровень 0.1), точность выйдет в 1 знак,
    # и округление вверх превратит 0.102 в 0.2 - переплата 100% вместо
    # задуманных 2%. Поэтому добавляем знаки ТОЛЬКО тогда, когда округление по
    # точности стакана сдвигает цену больше, чем на сам буфер. В нормальном
    # стакане это условие выполняется сразу и точность остаётся биржевой.
    target = price
    price = None
    for decimals in range(price_decimals(levels), 13):
        factor = 10 ** decimals
        candidate = (math.floor(target * factor) / factor if close_is_sell
                     else math.ceil(target * factor) / factor)
        if candidate > 0 and abs(candidate - target) <= target * (buffer_pct / 100):
            price = candidate
            break
    if price is None or price <= 0:
        return None, False
    return price, enough_depth
