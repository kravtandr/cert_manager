"""
Модуль с SQL-запросами для работы с сертификатами

"""

import logging
import sqlite3
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class CertificateQueries:
    """Класс с SQL-запросами для анализа сертификатов"""

    def __init__(self, db_path: str = "certificates.db"):
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """Подключение к БД"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row

    def close(self):
        """Закрытие соединения"""
        if self.connection:
            self.connection.close()

    def get_active_certificates_all_users(self) -> pd.DataFrame:
        """
        ОСНОВНОЙ ЗАПРОС: Получение активных сертификатов для всех пользователей
        """
        current_date = datetime.now().date()
        query = """
            SELECT
                cc.client_id,
                cc.certificate_id,
                cc.expiry_date,
                ROUND((julianday(cc.expiry_date) - julianday(?)), 0) as days_until_expiry
            FROM client_certificates cc
            WHERE cc.expiry_date > ?
            ORDER BY cc.client_id, cc.expiry_date
        """

        return pd.read_sql_query(
            query, self.connection, params=[current_date, current_date]
        )
