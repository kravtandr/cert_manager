"""
Тесты для работы с базой данных
"""

import pytest
import sqlite3
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

from src.database import StreamingCertificateDatabase


class TestStreamingCertificateDatabase:
    """Тесты для StreamingCertificateDatabase"""

    def test_connect_and_create_tables(self, temp_db_path):
        """Тест подключения и создания таблиц"""
        db = StreamingCertificateDatabase(db_path=temp_db_path)
        db.connect()
        
        assert db.connection is not None
        
        db.create_tables()
        
        # Проверяем что таблицы созданы
        cursor = db.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        assert 'clients' in tables
        assert 'certificates' in tables
        assert 'client_certificates' in tables
        
        db.close()

    def test_insert_clients_batch(self, test_db):
        """Тест вставки клиентов батчами"""
        clients = [{"client_id": i} for i in range(1, 6)]
        test_db.insert_clients_batch(clients)
        test_db.flush_all()
        
        # Проверяем что клиенты добавлены
        cursor = test_db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        
        assert count == 5

    def test_insert_certificates_batch(self, test_db):
        """Тест вставки сертификатов батчами"""
        certificates = [{"certificate_id": f"cert-{i}"} for i in range(1, 4)]
        test_db.insert_certificates_batch(certificates)
        test_db.flush_all()
        
        # Проверяем что сертификаты добавлены
        cursor = test_db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM certificates")
        count = cursor.fetchone()[0]
        
        assert count == 3

    def test_insert_assignments_batch(self, test_db_with_data):
        """Тест вставки назначений батчами"""
        assignments = [
            {
                "client_id": 1,
                "certificate_id": "cert-1",
                "expiry_date": date.today() + timedelta(days=30)
            },
            {
                "client_id": 2,
                "certificate_id": "cert-2",
                "expiry_date": date.today() + timedelta(days=60)
            }
        ]
        
        test_db_with_data.insert_assignments_batch(assignments)
        test_db_with_data.flush_all()
        
        # Проверяем что назначения добавлены
        cursor = test_db_with_data.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM client_certificates")
        count = cursor.fetchone()[0]
        
        assert count == 2

    def test_get_active_certificates_streaming(self, test_db_with_data):
        """Тест получения активных сертификатов потоково"""
        # Добавляем тестовые назначения с активными и неактивными сертификатами
        today = date.today()
        assignments = [
            {
                "client_id": 1,
                "certificate_id": "cert-1",
                "expiry_date": today + timedelta(days=30)  # Активный
            },
            {
                "client_id": 2,
                "certificate_id": "cert-2",
                "expiry_date": today - timedelta(days=30)  # Неактивный
            },
            {
                "client_id": 3,
                "certificate_id": "cert-3",
                "expiry_date": today + timedelta(days=60)  # Активный
            }
        ]
        
        test_db_with_data.insert_assignments_batch(assignments)
        test_db_with_data.flush_all()
        
        # Получаем активные сертификаты
        active_count = 0
        for chunk_df in test_db_with_data.get_active_certificates_streaming():
            active_count += len(chunk_df)
            # Проверяем структуру DataFrame
            assert 'client_id' in chunk_df.columns
            assert 'certificate_id' in chunk_df.columns
            assert 'expiry_date' in chunk_df.columns
            assert 'days_until_expiry' in chunk_df.columns
        
        # Должно быть 2 активных сертификата
        assert active_count == 2

    def test_buffering_mechanism(self, temp_db_path):
        """Тест механизма буферизации"""
        db = StreamingCertificateDatabase(
            db_path=temp_db_path,
            write_buffer_size=3  # Маленький буфер
        )
        db.connect()
        db.create_tables()
        
        # Добавляем клиентов, но не флашим
        clients = [{"client_id": i} for i in range(1, 3)]
        db.insert_clients_batch(clients)
        
        # Проверяем что в БД еще ничего нет (данные в буфере)
        cursor = db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        assert count == 0
        
        # Добавляем еще одного клиента (буфер должен сброситься)
        db.insert_clients_batch([{"client_id": 3}])
        
        # Теперь данные должны быть в БД
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        assert count == 3
        
        db.close()

    def test_flush_all(self, test_db):
        """Тест принудительного сброса всех буферов"""
        # Добавляем данные в буферы
        clients = [{"client_id": 1}]
        certificates = [{"certificate_id": "cert-1"}]
        
        test_db.insert_clients_batch(clients)
        test_db.insert_certificates_batch(certificates)
        
        # Проверяем что данных в БД нет
        cursor = test_db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM clients")
        assert cursor.fetchone()[0] == 0
        
        cursor.execute("SELECT COUNT(*) FROM certificates")
        assert cursor.fetchone()[0] == 0
        
        # Принудительно сбрасываем буферы
        test_db.flush_all()
        
        # Проверяем что данные появились в БД
        cursor.execute("SELECT COUNT(*) FROM clients")
        assert cursor.fetchone()[0] == 1
        
        cursor.execute("SELECT COUNT(*) FROM certificates")
        assert cursor.fetchone()[0] == 1

    def test_database_optimization_pragmas(self, temp_db_path):
        """Тест оптимизационных настроек SQLite"""
        db = StreamingCertificateDatabase(db_path=temp_db_path)
        db.connect()
        
        cursor = db.connection.cursor()
        
        # Проверяем что оптимизационные PRAGMA установлены
        cursor.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0] == 'wal'
        
        cursor.execute("PRAGMA synchronous")
        assert cursor.fetchone()[0] == 1  # NORMAL
        
        cursor.execute("PRAGMA cache_size")
        assert cursor.fetchone()[0] == -64000  # 64MB кэш
        
        db.close()

    def test_indexes_created(self, test_db):
        """Тест создания индексов"""
        cursor = test_db.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        
        # Проверяем наличие кастомных индексов
        assert 'idx_client_cert_expiry_composite' in indexes
        assert 'idx_expiry_date' in indexes 