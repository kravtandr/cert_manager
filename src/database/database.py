import os
import sqlite3
import threading
from datetime import datetime
from typing import Dict, Iterator, List

import pandas as pd

from ..utils import SystemOptimizer, logger


class StreamingCertificateDatabase:
    """Потоковая база данных с автоматической оптимизацией"""

    def __init__(
        self,
        db_path: str = None,
        write_buffer_size: int = None,
        num_clients: int = 1000,
        num_certificates: int = 2000,
    ):
        if db_path is None:
            db_path = os.getenv("DB_PATH", "certificates.db")

        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self.db_path = db_path

        # Автоматическая оптимизация если параметры не заданы
        if write_buffer_size is None:
            logger.info("🤖 Автоматическая оптимизация системы:")
            optimal_settings = SystemOptimizer.get_optimal_settings(
                num_clients, num_certificates
            )
            self.write_buffer_size = optimal_settings["write_buffer_size"]
            self.chunk_size = optimal_settings["chunk_size"]
        else:
            logger.info("Не используется автоматическая оптимизация:")
            self.write_buffer_size = write_buffer_size
            self.chunk_size = 50000  # Значение по умолчанию

        self.connection = None
        self._write_lock = threading.Lock()

        # Буферы для батчевой записи
        self._client_buffer: List[Dict] = []
        self._certificate_buffer: List[Dict] = []
        self._assignment_buffer: List[Dict] = []

    def connect(self):
        """Подключение с оптимизацией для больших данных"""
        try:
            self.connection = sqlite3.connect(
                self.db_path, check_same_thread=False, timeout=30.0
            )
            self.connection.row_factory = sqlite3.Row

            # Оптимизация SQLite для больших данных
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging
            cursor.execute("PRAGMA synchronous = NORMAL")  # Баланс скорости/надежности
            cursor.execute("PRAGMA cache_size = -64000")  # 64MB кэш
            cursor.execute("PRAGMA temp_store = MEMORY")  # Временные таблицы в памяти
            cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB memory-mapped I/O

            logger.info(f"Подключение к БД {self.db_path} с оптимизацией")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise

    def create_tables(self):
        """Создание таблиц с оптимизированными индексами"""
        try:
            cursor = self.connection.cursor()

            # Отключаем автокоммит для ускорения
            cursor.execute("BEGIN TRANSACTION")

            # Таблица клиентов
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS clients (
                    client_id INTEGER PRIMARY KEY
                ) WITHOUT ROWID
            """
            )

            # Таблица сертификатов
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS certificates (
                    certificate_id TEXT PRIMARY KEY
                ) WITHOUT ROWID
            """
            )

            # Таблица назначений с оптимизированными индексами
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS client_certificates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER NOT NULL,
                    certificate_id TEXT NOT NULL,
                    expiry_date DATE NOT NULL,
                    FOREIGN KEY (client_id) REFERENCES clients (client_id),
                    FOREIGN KEY (certificate_id) REFERENCES certificates (certificate_id)
                )
            """
            )

            # Составные индексы для оптимизации запросов
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_client_cert_expiry_composite
                ON client_certificates(client_id, expiry_date, certificate_id)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_expiry_date
                ON client_certificates(expiry_date)
            """
            )

            cursor.execute("COMMIT")
            logger.info("Таблицы и индексы созданы")

        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.error(f"Ошибка создания таблиц: {e}")
            raise

    def insert_clients_batch(self, clients: List[Dict]):
        """Потоковая вставка клиентов"""
        self._client_buffer.extend(clients)

        if len(self._client_buffer) >= self.write_buffer_size:
            self._flush_clients()

    def insert_certificates_batch(self, certificates: List[Dict]):
        """Потоковая вставка сертификатов"""
        self._certificate_buffer.extend(certificates)

        if len(self._certificate_buffer) >= self.write_buffer_size:
            self._flush_certificates()

    def insert_assignments_batch(self, assignments: List[Dict]):
        """Потоковая вставка назначений"""
        self._assignment_buffer.extend(assignments)

        if len(self._assignment_buffer) >= self.write_buffer_size:
            self._flush_assignments()

    def _flush_clients(self):
        """Сброс буфера клиентов в БД"""
        if not self._client_buffer:
            return

        with self._write_lock:
            try:
                cursor = self.connection.cursor()
                cursor.execute("BEGIN TRANSACTION")

                cursor.executemany(
                    """
                    INSERT OR REPLACE INTO clients (client_id) VALUES (?)
                """,
                    [(client["client_id"],) for client in self._client_buffer],
                )

                cursor.execute("COMMIT")
                logger.info(f"Записано {len(self._client_buffer):,} клиентов")
                self._client_buffer.clear()

            except Exception as e:
                cursor.execute("ROLLBACK")
                logger.error(f"Ошибка записи клиентов: {e}")
                raise

    def _flush_certificates(self):
        """Сброс буфера сертификатов в БД"""
        if not self._certificate_buffer:
            return

        with self._write_lock:
            try:
                cursor = self.connection.cursor()
                cursor.execute("BEGIN TRANSACTION")

                cursor.executemany(
                    """
                    INSERT OR REPLACE INTO certificates (certificate_id) VALUES (?)
                """,
                    [(cert["certificate_id"],) for cert in self._certificate_buffer],
                )

                cursor.execute("COMMIT")
                logger.info(f"Записано {len(self._certificate_buffer):,} сертификатов")
                self._certificate_buffer.clear()

            except Exception as e:
                cursor.execute("ROLLBACK")
                logger.error(f"Ошибка записи сертификатов: {e}")
                raise

    def _flush_assignments(self):
        """Сброс буфера назначений в БД"""
        if not self._assignment_buffer:
            return

        with self._write_lock:
            try:
                cursor = self.connection.cursor()
                cursor.execute("BEGIN TRANSACTION")

                cursor.executemany(
                    """
                    INSERT INTO client_certificates
                    (client_id, certificate_id, expiry_date) VALUES (?, ?, ?)
                """,
                    [
                        (a["client_id"], a["certificate_id"], a["expiry_date"])
                        for a in self._assignment_buffer
                    ],
                )

                cursor.execute("COMMIT")
                logger.info(f"Записано {len(self._assignment_buffer):,} назначений")
                self._assignment_buffer.clear()

            except Exception as e:
                cursor.execute("ROLLBACK")
                logger.error(f"Ошибка записи назначений: {e}")
                raise

    def flush_all(self):
        """Принудительный сброс всех буферов"""
        logger.info("Финальный сброс всех буферов...")
        self._flush_clients()
        self._flush_certificates()
        self._flush_assignments()
        logger.info("Все буферы сброшены")

    def get_active_certificates_streaming(
        self, chunk_size: int = None
    ) -> Iterator[pd.DataFrame]:
        """Потоковое получение активных сертификатов"""
        if chunk_size is None:
            chunk_size = self.chunk_size

        try:
            current_date = datetime.now().date()
            query = """
                SELECT
                    c.client_id,
                    cert.certificate_id,
                    cc.expiry_date,
                    ROUND((julianday(cc.expiry_date) - julianday(date('now'))), 0) as days_until_expiry
                FROM clients c
                JOIN client_certificates cc ON c.client_id = cc.client_id
                JOIN certificates cert ON cc.certificate_id = cert.certificate_id
                WHERE cc.expiry_date > ?
                ORDER BY c.client_id, cc.expiry_date
                LIMIT ? OFFSET ?
            """

            offset = 0
            while True:
                df = pd.read_sql_query(
                    query, self.connection, params=[current_date, chunk_size, offset]
                )

                if df.empty:
                    break

                yield df
                offset += chunk_size

                if len(df) < chunk_size:  # Последний чанк
                    break

        except Exception as e:
            logger.error(f"Ошибка потокового запроса: {e}")
            raise

    def close(self):
        """Закрытие с финальным сбросом буферов"""
        self.flush_all()
        if self.connection:
            # Оптимизация и очистка
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA optimize")  # Оптимизация индексов
            try:
                cursor.execute("VACUUM")  # Очистка и дефрагментация
            except sqlite3.OperationalError:
                # VACUUM нельзя выполнить в транзакции
                pass

            self.connection.close()
            logger.info("Соединение с БД закрыто с оптимизацией")
