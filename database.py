import sqlite3
from contextlib import closing


class Database:
    def __init__(self, db_name='arbitrage_bot.db'):
        self.db_name = db_name
        self._init_db()

    # database.py - обновленная структура
    def _init_db(self):
        with closing(self._get_connection()) as conn:
            # НОВАЯ таблица пар с расширенными полями
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pairs (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    contract_bsc TEXT NOT NULL,
                    decimals INTEGER NOT NULL,
                    mexc_api_key TEXT NOT NULL,
                    mexc_api_secret TEXT NOT NULL,
                    mexc_uid TEXT NOT NULL,
                    private_key TEXT NOT NULL,
                    rpc TEXT NOT NULL,
                    websocket TEXT NOT NULL
                )
            ''')

            # Остальные таблицы без изменений
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

            # Инициализация глобального спреда
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

    # Добавляем методы для работы с флагом обновления UID
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
            conn.execute('DELETE FROM uid')  # Очищаем предыдущее значение
            conn.execute('INSERT INTO uid (value) VALUES (?)', (uid_value,))
            conn.commit()

    def get_uid(self):
        """Получает текущий U_ID"""
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('SELECT value FROM uid LIMIT 1')
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
                # Удаляем старый спред если существует
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

    # database.py - новые методы
    def add_pair_v2(self, name, contract_bsc, decimals, mexc_api_key, mexc_api_secret, mexc_uid, private_key, rpc,
                    websocket):
        """Добавляет пару с новыми параметрами"""
        with closing(self._get_connection()) as conn:
            try:
                conn.execute(
                    '''INSERT INTO pairs 
                    (name, contract_bsc, decimals, mexc_api_key, mexc_api_secret, mexc_uid, private_key, rpc, websocket) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (name.upper(), contract_bsc, decimals, mexc_api_key, mexc_api_secret, mexc_uid, private_key, rpc,
                     websocket)
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_pair_data(self, name):
        """Возвращает все данные пары"""
        with closing(self._get_connection()) as conn:
            cursor = conn.execute(
                '''SELECT contract_bsc, decimals, mexc_api_key, mexc_api_secret, mexc_uid, private_key, rpc, websocket 
                FROM pairs WHERE name = ?''',
                (name.upper(),)
            )
            result = cursor.fetchone()
            if result:
                return {
                    'contract_bsc': result[0],
                    'decimals': result[1],
                    'mexc_api_key': result[2],
                    'mexc_api_secret': result[3],
                    'mexc_uid': result[4],
                    'private_key': result[5],
                    'rpc': result[6],
                    'websocket': result[7]
                }
            return None

    def remove_pair_v2(self, name):
        """Удаляет пару"""
        with closing(self._get_connection()) as conn:
            cursor = conn.execute('DELETE FROM pairs WHERE name = ?', (name.upper(),))
            conn.commit()
            return cursor.rowcount > 0


if __name__ == "__main__":
    d = Database()
    d.clear_database()
