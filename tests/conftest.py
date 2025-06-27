"""
Общие fixtures для тестов

"""

import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from src.api.handlers import app
from src.database import StreamingCertificateDatabase


@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    """Временная база данных для тестов"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    
    yield db_path
    
    # Очистка после теста
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def temp_output_dir() -> Generator[str, None, None]:
    """Временная директория для выходных файлов"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def test_db(temp_db_path: str) -> Generator[StreamingCertificateDatabase, None, None]:
    """База данных для тестов"""
    db = StreamingCertificateDatabase(
        db_path=temp_db_path,
        write_buffer_size=100,  # Маленький буфер для тестов
        num_clients=10,
        num_certificates=20
    )
    db.connect()
    db.create_tables()
    
    yield db
    
    db.close()


@pytest.fixture
def test_db_with_data(test_db: StreamingCertificateDatabase) -> StreamingCertificateDatabase:
    """База данных с тестовыми данными"""
    # Добавляем тестовые данные
    clients = [{"client_id": i} for i in range(1, 6)]
    certificates = [{"certificate_id": f"cert-{i}"} for i in range(1, 11)]
    
    test_db.insert_clients_batch(clients)
    test_db.insert_certificates_batch(certificates)
    test_db.flush_all()
    
    return test_db


@pytest.fixture
def api_client() -> TestClient:
    """HTTP клиент для тестирования API"""
    return TestClient(app)


@pytest.fixture
def sample_generation_request():
    """Пример запроса на генерацию"""
    return {
        "num_clients": 10,
        "num_certificates": 20,
        "batch_size": 5,
        "num_workers": 2,
        "write_buffer_size": 10
    } 