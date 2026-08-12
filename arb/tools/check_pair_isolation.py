"""
Диагностика: проверяет, что у разных торговых пар в БД не пересекаются
критичные поля (private_key, mexc_uid, address_contract, mexc_api_key).
Если пересечение есть - две "независимые" пары на самом деле торгуют
одним и тем же кошельком/аккаунтом MEXC, и их сделки будут влиять друг на друга.

Запуск из корня репозитория (там, где лежит рабочая arbitrage_bot.db):
    python -m arb.tools.check_pair_isolation
    python -m arb.tools.check_pair_isolation /path/to/arbitrage_bot.db
"""
import sqlite3
import sys
from collections import defaultdict

from arb.paths import DB_PATH as DEFAULT_DB_PATH

# На Windows консоль иногда использует cp1251 и не может напечатать emoji/⚠ -
# переключаемся на UTF-8 с заменой недопустимых символов, чтобы скрипт не падал.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_DB_PATH)

FIELDS_TO_CHECK = [
    "private_key",
    "mexc_uid",
    "mexc_api_key",
    "address_contract",
    "contract_bsc",
    "websocket",
    "rpc",
]


def main():
    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as e:
        print(f"Не удалось открыть {DB_PATH}: {e}")
        sys.exit(1)

    try:
        rows = conn.execute(
            f"SELECT name, {', '.join(FIELDS_TO_CHECK)} FROM pairs"
        ).fetchall()
    except sqlite3.Error as e:
        print(f"Не удалось прочитать таблицу pairs из {DB_PATH}: {e}")
        sys.exit(1)

    if not rows:
        print("В таблице pairs нет ни одной пары.")
        return

    print(f"Найдено пар: {len(rows)}\n")

    problems_found = False
    for field_idx, field in enumerate(FIELDS_TO_CHECK, start=1):
        value_to_pairs = defaultdict(list)
        for row in rows:
            pair_name = row[0]
            value = row[field_idx]
            if value:
                value_to_pairs[value].append(pair_name)

        for value, pairs_with_value in value_to_pairs.items():
            if len(pairs_with_value) > 1:
                problems_found = True
                masked = value[:6] + "..." + value[-4:] if len(value) > 12 else value
                print(f"⚠️  Поле '{field}' совпадает у пар {pairs_with_value}: {masked}")

    if not problems_found:
        print("✅ Пересечений не найдено - пары полностью изолированы друг от друга.")
    else:
        print(
            "\nНайдены пересечения выше. Это значит, что перечисленные пары "
            "используют один и тот же кошелёк/аккаунт/RPC/вебсокет и их сделки "
            "будут влиять друг на друга (например, одна пара может расходовать "
            "баланс или nonce, которым пользуется другая)."
        )

    conn.close()


if __name__ == "__main__":
    main()
