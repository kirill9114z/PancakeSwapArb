"""SQLite-хранилище: торговые пары с ключами, глобальный и индивидуальный спред.

Схема создаётся при первом обращении (_init_db). Путь к файлу БД по умолчанию
берётся из arb.paths, чтобы бот и диагностические скрипты работали с одной и той
же базой независимо от текущей директории запуска.
"""
import sqlite3
from contextlib import closing

from arb.paths import DB_PATH


class Database:
    def __init__(self, db_name=str(DB_PATH)):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        with closing(self._get_connection()) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pairs (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    contract_bsc TEXT NOT NULL,
                    decimals INTEGER NOT NULL,
                    address_contract TEXT NOT NULL,
                    abi TEXT NOT NULL,
                    mexc_api_key TEXT NOT NULL,
                    mexc_api_secret TEXT NOT NULL,
                    mexc_uid TEXT NOT NULL,
                    private_key TEXT NOT NULL,
                    rpc TEXT NOT NULL,
                    websocket TEXT NOT NULL,
                    volume INTEGER NOT NULL
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS spreads (
                    id INTEGER PRIMARY KEY,
                    pair_id INTEGER,
                    value REAL NOT NULL,
                    FOREIGN KEY(pair_id) REFERENCES pairs(id)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS global_spread (
                    id INTEGER PRIMARY KEY,
                    value REAL NOT NULL
                )
            ''')

            # Журнал исполнений - см. arb/core/journal.py. Одна строка на попытку
            # сделки, включая неудачные: именно они и стоят денег, без них статистика
            # бессмысленна. Все поля кроме id/ts/pair/trade_type допускают NULL -
            # сделка может закончиться на любом этапе.
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    ts_iso TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    trade_type TEXT NOT NULL,

                    level INTEGER,
                    planned_volume REAL,
                    planned_mexc_price REAL,
                    planned_limit_price REAL,
                    planned_dex_price REAL,
                    dex_price_source TEXT,
                    dex_impact_pct REAL,
                    planned_spread_pct REAL,
                    planned_profit_usd REAL,
                    est_fee_usd REAL,
                    analysis_seconds REAL,

                    mexc_order_id TEXT,
                    mexc_filled REAL,
                    mexc_avg_price REAL,
                    mexc_status TEXT,

                    swap_amount_in REAL,
                    swap_tx_hash TEXT,
                    swap_error_type TEXT,
                    swap_error_msg TEXT,
                    swap_quote_source TEXT,
                    gas_used INTEGER,
                    gas_price_gwei REAL,
                    gas_cost_usd REAL,

                    outcome TEXT,
                    emergency_closed_qty REAL,
                    emergency_remaining_qty REAL,
                    emergency_avg_price REAL,
                    realized_pnl_usd REAL,

                    t_order_placed REAL,
                    t_mexc_settled REAL,
                    t_swap_done REAL,
                    t_total REAL,

                    notes TEXT
                )
            ''')
            # Разбор журнала всегда идёт либо "последние N сделок по паре", либо
            # "все неудачи за период" - под оба сценария по индексу.
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_pair_ts ON trades (pair, ts DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades (outcome)')

            if not conn.execute('SELECT 1 FROM global_spread').fetchone():
                conn.execute('INSERT INTO global_spread (value) VALUES (1.0)')
            conn.commit()

    def _get_connection(self):
        return sqlite3.connect(self.db_name)

    def clear_database(self):
        with closing(self._get_connection()) as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("DROP TABLE IF EXISTS pairs")
            conn.execute("DROP TABLE IF EXISTS spreads")
            conn.execute("DROP TABLE IF EXISTS global_spread")
            conn.execute("DROP TABLE IF EXISTS uid")
            conn.execute("DROP TABLE IF EXISTS settings")
            conn.execute("PRAGMA foreign_keys = ON")
            conn.commit()
        self._init_db()
        return True

    def set_uid_update_flag(self, value: bool):
        with closing(self._get_connection()) as conn:
            conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
                         ('uid_update_flag', str(int(value))))
            conn.commit()

    def get_uid_update_flag(self) -> bool:
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('SELECT value FROM settings WHERE key = ?', ('uid_update_flag',))
            result = cursor.fetchone()
            return bool(int(result[0])) if result else False

    def set_uid(self, uid_value):
        """Устанавливает U_ID (всегда одна запись)"""
        with closing(self._get_connection()) as conn:
            conn.execute('DELETE FROM uid')  
            conn.execute('INSERT INTO uid (value) VALUES (?)', (uid_value,))
            conn.commit()

    def get_uid(self, name):
        """Получает текущий U_ID"""
        with closing(self._get_connection()) as conn:
            cursor = conn.execute(
                '''SELECT mexc_uid 
                FROM pairs WHERE name = ?''',
                (name.upper(),)
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def add_pair(self, name, contracts):
        """
        Добавляет пару с тремя контрактами
        :param contracts: кортеж (ethereum, base, bsc)
        """
        with closing(self._get_connection()) as conn:
            try:
                conn.execute(
                    '''INSERT INTO pairs 
                    (name, contract_ethereum, contract_base, contract_bsc) 
                    VALUES (?, ?, ?, ?)''',
                    (name.upper(), contracts[0], contracts[1], contracts[2])
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_pair_contracts(self, name):
        """Возвращает контракты для пары в виде словаря"""
        with closing(self._get_connection()) as conn:
            cursor = conn.execute(
                '''SELECT contract_ethereum, contract_base, contract_bsc 
                FROM pairs WHERE name = ?''',
                (name.upper(),)
            )
            result = cursor.fetchone()
            if result:
                return {
                    'ethereum': result[0],
                    'base': result[1],
                    'bsc': result[2]
                }
            return None

    def remove_pair(self, name):
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('DELETE FROM pairs WHERE name = ?', (name.upper(),))
            conn.commit()
            return cursor.rowcount > 0

    def get_all_pairs(self):
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('SELECT name FROM pairs')
            return [row[0] for row in cursor.fetchall()]

    def set_global_spread(self, value):
        with closing(self._get_connection()) as conn:
            conn.execute('UPDATE global_spread SET value = ?', (value,))
            conn.commit()

    def get_global_spread(self):
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('SELECT value FROM global_spread')
            return cursor.fetchone()[0]

    def set_pair_spread(self, pair_name, value):
        with closing(self._get_connection()) as conn:
            pair_id = conn.execute('SELECT id FROM pairs WHERE name = ?',
                                   (pair_name.upper(),)).fetchone()
            if pair_id:
                pair_id = pair_id[0]
                conn.execute('DELETE FROM spreads WHERE pair_id = ?', (pair_id,))
                conn.execute('INSERT INTO spreads (pair_id, value) VALUES (?, ?)',
                             (pair_id, value))
                conn.commit()
                return True
            return False

    def get_pair_spread(self, pair_name):
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('''
                SELECT s.value FROM spreads s
                JOIN pairs p ON p.id = s.pair_id
                WHERE p.name = ?
            ''', (pair_name.upper(),))
            result = cursor.fetchone()
            return result[0] if result else None

    def set_private_key(self, encrypted_key: str):
        with closing(self._get_connection()) as conn:
            print(f'Private Key: {encrypted_key}')
            conn.execute('UPDATE uid SET private_key_encrypted = ?', (encrypted_key,))
            conn.commit()

    def get_private_key(self) -> str | None:
        with closing(self._get_connection()) as conn:
            row = conn.execute('SELECT private_key_encrypted FROM uid LIMIT 1').fetchone()
            return row[0] if row else None

    def add_pair_v2(self, name, contract_bsc, decimals, address_contract, abi, mexc_api_key, mexc_api_secret, mexc_uid,
                    private_key, rpc,
                    websocket, volume):
        """Добавляет пару с новыми параметрами"""
        with closing(self._get_connection()) as conn:
            try:
                conn.execute(
                    '''INSERT INTO pairs 
                    (name, contract_bsc, decimals, address_contract, abi, mexc_api_key, mexc_api_secret, mexc_uid, private_key, rpc, websocket, volume) 
                    VALUES (:name, :contract_bsc, :decimals, :address_contract, :abi, :mexc_api_key, :mexc_api_secret, :mexc_uid, :private_key, :rpc, :websocket, :volume)''',
                    {
                        'name': name.upper(),
                        'contract_bsc': contract_bsc,
                        'decimals': decimals,
                        'address_contract': address_contract,
                        'abi': abi,
                        'mexc_api_key': mexc_api_key,
                        'mexc_api_secret': mexc_api_secret,
                        'mexc_uid': mexc_uid,
                        'private_key': private_key,
                        'rpc': rpc,
                        'websocket': websocket,
                        'volume': volume
                    }
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_pair_data(self, name):
        """Возвращает все данные пары"""
        with closing(self._get_connection()) as conn:
            cursor = conn.execute(
                '''SELECT contract_bsc, decimals, address_contract, abi, mexc_api_key, mexc_api_secret, mexc_uid, private_key, rpc, websocket, volume 
                FROM pairs WHERE name = ?''',
                (name.upper(),)
            )
            result = cursor.fetchone()
            if result:
                return {
                    'contract_bsc': result[0],
                    'decimals': result[1],
                    'address_contract': result[2],
                    'abi': result[3],
                    'mexc_api_key': result[4],
                    'mexc_api_secret': result[5],
                    'mexc_uid': result[6],
                    'private_key': result[7],
                    'rpc': result[8],
                    'websocket': result[9],
                    'volume': result[10]
                }
            return None

    def remove_pair_v2(self, name):
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('DELETE FROM pairs WHERE name = ?', (name.upper(),))
            conn.commit()
            return cursor.rowcount > 0



    # ==================== Журнал исполнений (arb/core/journal.py) ====================

    def log_trade(self, row: dict) -> bool:
        """Пишет одну строку журнала. НИКОГДА не бросает исключение: вызывается из
        finally в make_trade, и падение записи в журнал не имеет права ни отменить
        синхронизацию балансов, ни превратиться в незакрытую позицию.

        Неизвестные ключи молча отбрасываются - чтобы добавление поля в TradeRecord
        не роняло бота на старой БД до применения миграции."""
        try:
            with closing(self._get_connection()) as conn:
                known = {r[1] for r in conn.execute('PRAGMA table_info(trades)')}
                data = {k: v for k, v in row.items() if k in known}
                if not data:
                    return False
                columns = ', '.join(data)
                placeholders = ', '.join(f':{k}' for k in data)
                conn.execute(f'INSERT INTO trades ({columns}) VALUES ({placeholders})', data)
                conn.commit()
                return True
        except Exception as e:
            print(f"Не удалось записать сделку в журнал: {e}")
            return False

    def get_recent_trades(self, pair_name: str = None, limit: int = 50) -> list:
        """Последние записи журнала, свежие первыми. Для отчёта в Telegram и
        разбора руками."""
        try:
            with closing(self._get_connection()) as conn:
                conn.row_factory = sqlite3.Row
                if pair_name:
                    cursor = conn.execute(
                        'SELECT * FROM trades WHERE pair = ? ORDER BY ts DESC LIMIT ?',
                        (pair_name.upper(), limit))
                else:
                    cursor = conn.execute(
                        'SELECT * FROM trades ORDER BY ts DESC LIMIT ?', (limit,))
                return [dict(r) for r in cursor.fetchall()]
        except Exception as e:
            print(f"Не удалось прочитать журнал сделок: {e}")
            return []

    def get_trade_stats(self, pair_name: str = None, since_ts: float = 0) -> dict:
        """Полная сводка по журналу. pair_name=None - по всем парам сразу.

        Это и есть ответ на вопрос "бот зарабатывает или нет". Два показателя тут
        важнее остальных:
          - доля hedged среди попыток: именно неудачные хеджи запускают аварийное
            закрытие с буфером MEXC_EMERGENCY_PRICE_BUFFER_PCT, и одно такое
            закрытие съедает прибыль десятка удачных сделок;
          - РАЗДЕЛЬНЫЕ суммы прибыли и убытка: одно итоговое число прячет ситуацию
            "заработал $50 на 200 сделках, потерял $48 на трёх", а это совершенно
            другой диагноз, чем "заработал $2 ровно".

        Про NULL в realized_pnl_usd: у сделок с неизвестным итогом (swap_unknown,
        error) прибыль намеренно не считается - см. TradeRecord.estimate_pnl. SUM их
        игнорирует, поэтому отдельно возвращается pnl_unknown_trades: если это число
        заметное, итоговой сумме доверять нельзя, пока их не разберут руками.

        Агрегаты СЧИТАЮТСЯ НА ЧТЕНИИ, а не хранятся отдельной таблицей: журнал -
        единственный источник истины, а хранимый running total неизбежно разъезжается
        с ним при падениях и, что важнее, замораживает в себе старую (возможно
        ошибочную) формулу расчёта прибыли. Пересчёт по SQL позволяет исправить
        формулу и получить корректную историю задним числом."""
        try:
            with closing(self._get_connection()) as conn:
                where = 'WHERE ts >= ?'
                params = [since_ts]
                if pair_name:
                    where += ' AND pair = ?'
                    params.append(pair_name.upper())

                by_outcome = {
                    row[0]: row[1] for row in conn.execute(
                        f'SELECT outcome, COUNT(*) FROM trades {where} '
                        f'GROUP BY outcome ORDER BY COUNT(*) DESC', params)
                }

                row = conn.execute(f'''
                    SELECT
                        COUNT(*),
                        COUNT(realized_pnl_usd),
                        COALESCE(SUM(realized_pnl_usd), 0),
                        COALESCE(SUM(CASE WHEN realized_pnl_usd > 0 THEN realized_pnl_usd END), 0),
                        COALESCE(SUM(CASE WHEN realized_pnl_usd < 0 THEN realized_pnl_usd END), 0),
                        SUM(CASE WHEN realized_pnl_usd > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN realized_pnl_usd < 0 THEN 1 ELSE 0 END),
                        COALESCE(SUM(gas_cost_usd), 0),
                        COALESCE(SUM(mexc_filled * COALESCE(mexc_avg_price, planned_mexc_price)), 0),
                        AVG(t_total),
                        AVG(analysis_seconds),
                        MIN(ts), MAX(ts)
                    FROM trades {where}''', params).fetchone()

                stats = {
                    'pair': pair_name.upper() if pair_name else None,
                    'trades': row[0],
                    'pnl_counted_trades': row[1],
                    'pnl_unknown_trades': row[0] - row[1],
                    'pnl_usd': row[2],
                    'profit_usd': row[3],
                    'loss_usd': row[4],
                    'win_trades': row[5] or 0,
                    'loss_trades': row[6] or 0,
                    'gas_usd': row[7],
                    'volume_usd': row[8],
                    'avg_seconds': row[9],
                    'avg_analysis_seconds': row[10],
                    'first_ts': row[11],
                    'last_ts': row[12],
                    'by_outcome': by_outcome,
                    'hedged': by_outcome.get('hedged', 0),
                }
                # Доля успешных хеджей считается от попыток, которые ДОШЛИ до свопа:
                # ордер, не наполнившийся на MEXC, до хеджа не добирается и портить
                # этот показатель не должен.
                reached_swap = sum(c for o, c in by_outcome.items()
                                   if o not in ('mexc_empty', 'mexc_rejected'))
                stats['reached_swap'] = reached_swap
                stats['hedge_success_pct'] = (stats['hedged'] / reached_swap * 100
                                              if reached_swap else None)

                stats['best'] = self._extreme_trade(conn, where, params, 'DESC')
                stats['worst'] = self._extreme_trade(conn, where, params, 'ASC')
                return stats
        except Exception as e:
            print(f"Не удалось посчитать статистику журнала: {e}")
            return {}

    @staticmethod
    def _extreme_trade(conn, where: str, params: list, direction: str):
        """Самая прибыльная (DESC) или самая убыточная (ASC) сделка выборки."""
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            f'SELECT * FROM trades {where} AND realized_pnl_usd IS NOT NULL '
            f'ORDER BY realized_pnl_usd {direction} LIMIT 1', params).fetchone()
        conn.row_factory = None
        return dict(row) if row else None

    def get_profit_by_pair(self, since_ts: float = 0) -> list:
        """Прибыль и число сделок в разрезе пар, прибыльные первыми.
        Для сводного отчёта: одна строка на пару."""
        try:
            with closing(self._get_connection()) as conn:
                return [
                    {'pair': r[0], 'trades': r[1], 'pnl_usd': r[2], 'hedged': r[3]}
                    for r in conn.execute('''
                        SELECT pair,
                               COUNT(*),
                               COALESCE(SUM(realized_pnl_usd), 0),
                               SUM(CASE WHEN outcome = 'hedged' THEN 1 ELSE 0 END)
                        FROM trades WHERE ts >= ?
                        GROUP BY pair
                        ORDER BY 3 DESC''', (since_ts,))
                ]
        except Exception as e:
            print(f"Не удалось посчитать прибыль по парам: {e}")
            return []

    def update_pair_mexc_uid(self, pair_name: str, new_uid: str) -> bool:
        with closing(self._get_connection()) as conn:
            try:
                cursor = conn.execute(
                    'SELECT id FROM pairs WHERE name = ?',
                    (pair_name.upper(),)
                )
                result = cursor.fetchone()

                if not result:
                    print(f"Пара '{pair_name}' не найдена в базе данных")
                    return False

                cursor = conn.execute(
                    'UPDATE pairs SET mexc_uid = ? WHERE name = ?',
                    (new_uid, pair_name.upper())
                )

                if cursor.rowcount == 0:
                    print(f"Не удалось обновить U_ID для пары '{pair_name}'")
                    return False

                conn.commit()
                print(f"✅ U_ID успешно обновлен для пары '{pair_name}': {new_uid}")
                return True

            except sqlite3.Error as e:
                print(f"Ошибка базы данных при обновлении U_ID для '{pair_name}': {e}")
                return False


