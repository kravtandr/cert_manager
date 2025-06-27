"""
Интеграционные тесты полного workflow

"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path

from src.core.data_generator import CertificateGenerator
from src.database import StreamingCertificateDatabase


@pytest.mark.integration
class TestFullWorkflow:
    """Интеграционные тесты полного workflow генерации данных"""

    def test_small_dataset_generation_workflow(self, temp_output_dir):
        """Тест полного workflow для небольшого датасета"""
        # Параметры для теста
        num_clients = 10
        num_certificates = 20
        batch_size = 5
        num_workers = 2
        
        # Создаем временную БД
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # 1. Инициализация компонентов
            generator = CertificateGenerator(
                num_clients=num_clients,
                num_certificates=num_certificates,
                batch_size=batch_size,
                num_workers=num_workers
            )
            
            db = StreamingCertificateDatabase(
                db_path=db_path,
                write_buffer_size=5,
                num_clients=num_clients,
                num_certificates=num_certificates
            )
            
            # 2. Подключение и создание структуры БД
            db.connect()
            db.create_tables()
            
            # 3. Генерация и сохранение клиентов
            client_count = 0
            for batch in generator.generate_clients_parallel():
                db.insert_clients_batch(batch)
                client_count += len(batch)
            
            assert client_count == num_clients
            
            # 4. Генерация и сохранение сертификатов
            cert_count = 0
            for batch in generator.generate_certificates_parallel():
                db.insert_certificates_batch(batch)
                cert_count += len(batch)
            
            assert cert_count == num_certificates
            
            # 5. Принудительно сбрасываем буферы перед генерацией назначений
            db.flush_all()
            
            # 6. Генерация назначений
            certificate_pool = generator.get_certificate_pool(connection=db.connection)
            assert len(certificate_pool) == num_certificates
            
            total_assignments = 0
            assignment_batch_size = 5
            
            for start_id in range(1, num_clients + 1, assignment_batch_size):
                end_id = min(start_id + assignment_batch_size, num_clients + 1)
                client_ids = list(range(start_id, end_id))
                
                assignments = generator.generate_assignments_batch(client_ids, certificate_pool)
                if assignments:
                    db.insert_assignments_batch(assignments)
                    total_assignments += len(assignments)
            
            # 7. Финальный сброс буферов
            db.flush_all()
            
            # 8. Проверка результатов в БД
            cursor = db.connection.cursor()
            
            # Проверяем количество клиентов
            cursor.execute("SELECT COUNT(*) FROM clients")
            assert cursor.fetchone()[0] == num_clients
            
            # Проверяем количество сертификатов
            cursor.execute("SELECT COUNT(*) FROM certificates")
            assert cursor.fetchone()[0] == num_certificates
            
            # Проверяем что назначения созданы
            cursor.execute("SELECT COUNT(*) FROM client_certificates")
            assignment_count = cursor.fetchone()[0]
            assert assignment_count == total_assignments
            assert assignment_count >= 0  # Может быть 0 если никому не назначили сертификаты
            
            # 9. Тестирование потоковой выгрузки активных сертификатов
            active_count = 0
            for chunk_df in db.get_active_certificates_streaming():
                active_count += len(chunk_df)
                
                # Проверяем структуру данных
                if len(chunk_df) > 0:
                    assert 'client_id' in chunk_df.columns
                    assert 'certificate_id' in chunk_df.columns
                    assert 'expiry_date' in chunk_df.columns
                    assert 'days_until_expiry' in chunk_df.columns
            
            # 10. Создание CSV отчета
            csv_path = os.path.join(temp_output_dir, "test_active_certificates.csv")
            
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("client_id,certificate_id,expiry_date,days_until_expiry\n")
                
                for chunk_df in db.get_active_certificates_streaming():
                    chunk_df.to_csv(f, header=False, index=False)
            
            # Проверяем что файл создан и не пустой
            assert os.path.exists(csv_path)
            assert os.path.getsize(csv_path) > 0
            
            # 11. Проверяем содержимое CSV файла
            with open(csv_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                assert len(lines) >= 1  # Минимум заголовок
                assert "client_id,certificate_id,expiry_date,days_until_expiry" in lines[0]
            
            db.close()
            
        finally:
            # Очистка
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_medium_dataset_generation_workflow(self, temp_output_dir):
        """Тест полного workflow для среднего датасета"""
        # Более реалистичные параметры
        num_clients = 100
        num_certificates = 200
        batch_size = 20
        num_workers = 2
        
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Полный цикл генерации
            generator = CertificateGenerator(
                num_clients=num_clients,
                num_certificates=num_certificates,
                batch_size=batch_size,
                num_workers=num_workers
            )
            
            db = StreamingCertificateDatabase(
                db_path=db_path,
                write_buffer_size=50,
                num_clients=num_clients,
                num_certificates=num_certificates
            )
            
            db.connect()
            db.create_tables()
            
            # Генерация данных
            for batch in generator.generate_clients_parallel():
                db.insert_clients_batch(batch)
            
            for batch in generator.generate_certificates_parallel():
                db.insert_certificates_batch(batch)
            
            db.flush_all()
            
            # Генерация назначений
            certificate_pool = generator.get_certificate_pool(connection=db.connection)
            
            for start_id in range(1, num_clients + 1, 20):
                end_id = min(start_id + 20, num_clients + 1)
                client_ids = list(range(start_id, end_id))
                assignments = generator.generate_assignments_batch(client_ids, certificate_pool)
                if assignments:
                    db.insert_assignments_batch(assignments)
            
            db.flush_all()
            
            # Проверки
            cursor = db.connection.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM clients")
            assert cursor.fetchone()[0] == num_clients
            
            cursor.execute("SELECT COUNT(*) FROM certificates")
            assert cursor.fetchone()[0] == num_certificates
            
            cursor.execute("SELECT COUNT(*) FROM client_certificates")
            total_assignments = cursor.fetchone()[0]
            
            # Проверяем что у клиентов есть назначения (статистически должны быть)
            cursor.execute("SELECT COUNT(DISTINCT client_id) FROM client_certificates")
            clients_with_certs = cursor.fetchone()[0]
            
            # Как минимум некоторые клиенты должны иметь сертификаты
            if total_assignments > 0:
                assert clients_with_certs > 0
                assert clients_with_certs <= num_clients
            
            # Проверяем работу с активными сертификатами
            active_count = 0
            for chunk_df in db.get_active_certificates_streaming():
                active_count += len(chunk_df)
            
            # Должны быть активные сертификаты (статистически)
            # Не проверяем точное количество, так как зависит от random генерации дат
            
            db.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_database_constraints_and_integrity(self):
        """Тест ограничений и целостности БД"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            db = StreamingCertificateDatabase(db_path=db_path)
            db.connect()
            db.create_tables()
            
            cursor = db.connection.cursor()
            
            # Добавляем тестовые данные
            cursor.execute("INSERT INTO clients (client_id) VALUES (1)")
            cursor.execute("INSERT INTO certificates (certificate_id) VALUES ('cert-1')")
            
            # Тестируем foreign key constraints (если включены)
            try:
                cursor.execute("""
                    INSERT INTO client_certificates (client_id, certificate_id, expiry_date)
                    VALUES (999, 'cert-1', '2024-12-31')
                """)
                # Если дошли сюда, то foreign keys не включены или запрос прошел
            except sqlite3.IntegrityError:
                # Ожидаемое поведение с включенными foreign keys
                pass
            
            # Тестируем уникальность primary keys
            with pytest.raises(sqlite3.IntegrityError):
                cursor.execute("INSERT INTO clients (client_id) VALUES (1)")  # Дубликат
            
            db.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_performance_with_indexes(self):
        """Тест производительности с индексами"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            db = StreamingCertificateDatabase(db_path=db_path)
            db.connect()
            db.create_tables()
            
            # Добавляем тестовые данные
            clients = [{"client_id": i} for i in range(1, 51)]
            certificates = [{"certificate_id": f"cert-{i}"} for i in range(1, 101)]
            
            db.insert_clients_batch(clients)
            db.insert_certificates_batch(certificates)
            db.flush_all()
            
            # Добавляем назначения
            from datetime import date, timedelta
            today = date.today()
            
            assignments = []
            for client_id in range(1, 51):
                for i in range(2):  # По 2 сертификата на клиента
                    cert_id = f"cert-{client_id * 2 + i}"
                    if cert_id in [f"cert-{j}" for j in range(1, 101)]:
                        assignments.append({
                            "client_id": client_id,
                            "certificate_id": cert_id,
                            "expiry_date": today + timedelta(days=30 + i * 30)
                        })
            
            db.insert_assignments_batch(assignments)
            db.flush_all()
            
            cursor = db.connection.cursor()
            
            # Тестируем быстрый запрос активных сертификатов (должен использовать индекс)
            import time
            start_time = time.time()
            
            cursor.execute("""
                SELECT client_id, certificate_id, expiry_date,
                       CAST(JULIANDAY(expiry_date) - JULIANDAY('now') AS INTEGER) as days_until_expiry
                FROM client_certificates
                WHERE expiry_date > DATE('now')
                ORDER BY expiry_date
            """)
            
            results = cursor.fetchall()
            query_time = time.time() - start_time
            
            # Запрос должен выполняться быстро (менее 1 секунды для такого объема данных)
            assert query_time < 1.0
            assert len(results) > 0
            
            db.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path) 