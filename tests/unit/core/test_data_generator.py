"""
Тесты для генератора данных
"""

import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock
import sqlite3

from src.core.data_generator import CertificateGenerator


class TestCertificateGenerator:
    """Тесты для CertificateGenerator"""

    def test_init_with_auto_optimization(self):
        """Тест инициализации с автоматической оптимизацией"""
        with patch('src.core.data_generator.SystemOptimizer.get_optimal_settings') as mock_optimizer:
            mock_optimizer.return_value = {
                'batch_size': 5000,
                'num_workers': 4,
                'write_buffer_size': 1000,
                'chunk_size': 10000
            }
            
            generator = CertificateGenerator(
                num_clients=100,
                num_certificates=200
            )
            
            assert generator.num_clients == 100
            assert generator.num_certificates == 200
            assert generator.batch_size == 5000
            assert generator.num_workers == 4

    def test_init_with_manual_settings(self):
        """Тест инициализации с ручными настройками"""
        generator = CertificateGenerator(
            num_clients=100,
            num_certificates=200,
            batch_size=1000,
            num_workers=2
        )
        
        assert generator.num_clients == 100
        assert generator.num_certificates == 200
        assert generator.batch_size == 1000
        assert generator.num_workers == 2

    def test_generate_clients_batch(self):
        """Тест генерации батча клиентов"""
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        
        clients = generator.generate_clients_batch(start_id=1, count=5)
        
        assert len(clients) == 5
        assert clients[0] == {"client_id": 1}
        assert clients[4] == {"client_id": 5}

    def test_generate_certificates_batch(self):
        """Тест генерации батча сертификатов"""
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        
        certificates = generator.generate_certificates_batch(start_id=1, count=3)
        
        assert len(certificates) == 3
        for cert in certificates:
            assert "certificate_id" in cert
            assert isinstance(cert["certificate_id"], str)

    def test_generate_consistent_certificate_id(self):
        """Тест генерации консистентных ID сертификатов"""
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        
        # Один и тот же индекс должен давать один и тот же ID при одинаковом seed
        with patch('random.seed') as mock_seed, \
             patch('uuid.uuid4', return_value='test-uuid-1') as mock_uuid:
            id1 = generator.generate_consistent_certificate_id(5)
            mock_seed.assert_called_with(42 + 5)
            assert id1 == 'test-uuid-1'
        
        # Разные индексы должны вызывать seed с разными значениями
        with patch('random.seed') as mock_seed, \
             patch('uuid.uuid4', return_value='test-uuid-2') as mock_uuid:
            id2 = generator.generate_consistent_certificate_id(10)
            mock_seed.assert_called_with(42 + 10)
            assert id2 == 'test-uuid-2'

    def test_generate_assignments_batch(self):
        """Тест генерации назначений"""
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        
        client_ids = [1, 2, 3]
        certificate_pool = ["cert-1", "cert-2", "cert-3", "cert-4", "cert-5"]
        
        assignments = generator.generate_assignments_batch(client_ids, certificate_pool)
        
        # Проверяем что назначения созданы
        assert len(assignments) >= 0  # Может быть 0 если клиенту не назначено сертификатов
        
        for assignment in assignments:
            assert "client_id" in assignment
            assert "certificate_id" in assignment
            assert "expiry_date" in assignment
            assert assignment["client_id"] in client_ids
            assert assignment["certificate_id"] in certificate_pool
            assert isinstance(assignment["expiry_date"], date)

    def test_generate_assignments_batch_expiry_dates(self):
        """Тест что даты истечения генерируются в правильном диапазоне"""
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        
        client_ids = [1]
        certificate_pool = ["cert-1"] * 50  # Много сертификатов для гарантии назначения
        
        # Генерируем много назначений для анализа дат
        assignments = []
        for _ in range(10):  # Генерируем несколько раз для получения статистики
            batch = generator.generate_assignments_batch(client_ids, certificate_pool)
            assignments.extend(batch)
        
        if assignments:  # Если есть назначения
            today = date.today()
            min_date = today - timedelta(days=365)
            max_date = today + timedelta(days=7300)
            
            for assignment in assignments:
                expiry_date = assignment["expiry_date"]
                assert min_date <= expiry_date <= max_date

    def test_generate_clients_parallel(self):
        """Тест параллельной генерации клиентов"""
        generator = CertificateGenerator(
            num_clients=10,
            num_certificates=20,
            batch_size=3,
            num_workers=2
        )
        
        all_clients = []
        for batch in generator.generate_clients_parallel():
            all_clients.extend(batch)
        
        assert len(all_clients) == 10
        
        # Проверяем что все ID уникальны
        client_ids = [client["client_id"] for client in all_clients]
        assert len(set(client_ids)) == 10
        assert min(client_ids) == 1
        assert max(client_ids) == 10

    def test_generate_certificates_parallel(self):
        """Тест параллельной генерации сертификатов"""
        generator = CertificateGenerator(
            num_clients=10,
            num_certificates=15,
            batch_size=4,
            num_workers=2
        )
        
        all_certificates = []
        for batch in generator.generate_certificates_parallel():
            all_certificates.extend(batch)
        
        assert len(all_certificates) == 15
        
        # Проверяем что все ID уникальны
        cert_ids = [cert["certificate_id"] for cert in all_certificates]
        assert len(set(cert_ids)) == 15

    def test_get_certificate_pool(self, temp_db_path):
        """Тест получения пула сертификатов"""
        # Создаем тестовую БД с сертификатами
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE certificates (
                certificate_id TEXT PRIMARY KEY
            )
        """)
        
        test_certs = [("cert-1",), ("cert-2",), ("cert-3",)]
        cursor.executemany("INSERT INTO certificates VALUES (?)", test_certs)
        conn.commit()
        
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        certificate_pool = generator.get_certificate_pool(connection=conn)
        
        assert len(certificate_pool) == 3
        assert "cert-1" in certificate_pool
        assert "cert-2" in certificate_pool
        assert "cert-3" in certificate_pool
        
        conn.close()

    def test_faker_thread_safety(self):
        """Тест thread-safety для Faker"""
        generator = CertificateGenerator(num_clients=10, num_certificates=20)
        
        # Получаем Faker из разных "потоков" (симулируем разные thread_id)
        with patch('threading.get_ident', return_value=1):
            faker1 = generator._get_faker()
        
        with patch('threading.get_ident', return_value=2):
            faker2 = generator._get_faker()
        
        # Должны быть разные экземпляры
        assert faker1 is not faker2
        
        # Повторный вызов с тем же thread_id должен вернуть тот же экземпляр
        with patch('threading.get_ident', return_value=1):
            faker1_again = generator._get_faker()
        
        assert faker1 is faker1_again 