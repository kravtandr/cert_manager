"""
Тесты для API обработчиков

"""

import pytest
import json
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from src.api.handlers import app, generation_status


@pytest.fixture
def client():
    """HTTP клиент для тестирования"""
    return TestClient(app)


class TestHealthCheck:
    """Тесты для health check"""

    def test_health_check(self, client):
        """Тест проверки состояния системы"""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "status" in data
        assert data["status"] == "healthy"
        # Может содержать timestamp или другие поля в зависимости от реализации


class TestGenerationAPI:
    """Тесты для API генерации данных"""

    def test_get_generation_status_initial(self, client):
        """Тест получения начального статуса генерации"""
        response = client.get("/generation/status")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "is_running" in data
        assert "progress" in data
        assert "total" in data
        assert "current_stage" in data
        assert isinstance(data["is_running"], bool)

    @patch('src.api.handlers.run_generation_task')
    def test_start_generation_default_params(self, mock_task, client):
        """Тест запуска генерации с параметрами по умолчанию"""
        response = client.post("/generation/start")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "Генерация запущена" in data["message"]

    @patch('src.api.handlers.run_generation_task')
    def test_start_generation_custom_params(self, mock_task, client):
        """Тест запуска генерации с кастомными параметрами"""
        request_data = {
            "num_clients": 500,
            "num_certificates": 1000,
            "batch_size": 100,
            "num_workers": 4
        }
        
        response = client.post(
            "/generation/start",
            json=request_data
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "Генерация запущена" in data["message"]

    def test_start_generation_invalid_params(self, client):
        """Тест запуска генерации с некорректными параметрами"""
        # Слишком много клиентов
        request_data = {
            "num_clients": 20000000,  # Больше лимита
            "num_certificates": 1000
        }
        
        response = client.post(
            "/generation/start",
            json=request_data
        )
        
        assert response.status_code == 422  # Validation error

    @patch('src.api.handlers.generation_status')
    def test_start_generation_already_running(self, mock_status, client):
        """Тест запуска генерации когда она уже выполняется"""
        mock_status.__getitem__.return_value = True  # is_running = True
        
        response = client.post("/generation/start")
        
        assert response.status_code == 409  # Conflict
        data = response.json()
        assert "уже выполняется" in data["detail"]


class TestActiveCertificatesAPI:
    """Тесты для API активных сертификатов"""

    @patch('src.api.handlers.StreamingCertificateDatabase')
    def test_get_active_certificates_default_params(self, mock_db_class, client):
        """Тест получения активных сертификатов с параметрами по умолчанию"""
        # Настройка мока БД
        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        
        # Мок для get_active_certificates_paged
        mock_db_instance.connection.cursor.return_value.fetchone.return_value = [100]  # total_count
        mock_db_instance.connection.cursor.return_value.fetchall.return_value = [
            (1, "cert-1", "2024-12-31", 30),
            (2, "cert-2", "2024-11-30", 60)
        ]
        
        response = client.get("/certificates/active")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "certificates" in data
        assert "total_count" in data
        assert "page" in data
        assert "page_size" in data
        assert "has_next" in data

    @patch('src.api.handlers.StreamingCertificateDatabase')
    def test_get_active_certificates_with_pagination(self, mock_db_class, client):
        """Тест получения активных сертификатов с пагинацией"""
        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        
        response = client.get("/certificates/active?page=2&page_size=50")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["page"] == 2
        assert data["page_size"] == 50

    @patch('src.api.handlers.StreamingCertificateDatabase')
    def test_get_active_certificates_with_client_filter(self, mock_db_class, client):
        """Тест получения активных сертификатов с фильтром по клиенту"""
        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        
        response = client.get("/certificates/active?client_id_filter=123")
        
        assert response.status_code == 200

    def test_get_active_certificates_invalid_page(self, client):
        """Тест с некорректным номером страницы"""
        response = client.get("/certificates/active?page=0")
        
        assert response.status_code == 422  # Validation error

    def test_get_active_certificates_invalid_page_size(self, client):
        """Тест с некорректным размером страницы"""
        response = client.get("/certificates/active?page_size=20000")
        
        assert response.status_code == 422  # Validation error


class TestDownloadAPI:
    """Тесты для API скачивания отчетов"""

    @patch('os.path.exists', return_value=True)
    def test_download_active_certificates_file_exists(self, mock_exists, client):
        """Тест скачивания отчета когда файл существует"""
        response = client.get("/certificates/active/download")
        
        assert response.status_code == 200

    @patch('os.path.exists', return_value=False)
    def test_download_active_certificates_file_not_exists(self, mock_exists, client):
        """Тест скачивания отчета когда файл не существует"""
        response = client.get("/certificates/active/download")
        
        assert response.status_code == 404
        data = response.json()
        assert "не найден" in data["detail"]


class TestClientCertificateAssignmentsAPI:
    """Тесты для API связей клиент-сертификат"""

    @patch('src.api.handlers.StreamingCertificateDatabase')
    def test_get_client_certificate_assignments(self, mock_db_class, client):
        """Тест получения связей клиент-сертификат"""
        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        
        # Настройка мока для запроса
        mock_db_instance.connection.cursor.return_value.fetchone.return_value = [50]  # total_count
        mock_db_instance.connection.cursor.return_value.fetchall.return_value = [
            (1, 1, "cert-1", "2024-12-31", True, 30),
            (2, 2, "cert-2", "2024-11-30", False, -10)
        ]
        
        response = client.get("/client-certificates")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "assignments" in data
        assert "total_count" in data
        assert "skip" in data
        assert "limit" in data
        assert "has_next" in data

    @patch('src.api.handlers.StreamingCertificateDatabase')
    def test_get_client_certificate_assignments_with_filters(self, mock_db_class, client):
        """Тест получения связей с фильтрами"""
        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        
        response = client.get("/client-certificates?client_id_filter=123&active_only=true")
        
        assert response.status_code == 200


class TestRequestValidation:
    """Тесты для валидации запросов"""

    def test_generation_request_validation_min_values(self, client):
        """Тест валидации минимальных значений"""
        request_data = {
            "num_clients": 0,  # Меньше минимума
            "num_certificates": 0  # Меньше минимума
        }
        
        response = client.post("/generation/start", json=request_data)
        
        assert response.status_code == 422

    def test_generation_request_validation_max_values(self, client):
        """Тест валидации максимальных значений"""
        request_data = {
            "num_clients": 50000000,  # Больше максимума
            "num_certificates": 50000000  # Больше максимума
        }
        
        response = client.post("/generation/start", json=request_data)
        
        assert response.status_code == 422

    def test_pagination_validation(self, client):
        """Тест валидации пагинации"""
        # Некорректный skip
        response = client.get("/client-certificates?skip=-1")
        assert response.status_code == 422
        
        # Некорректный limit
        response = client.get("/client-certificates?limit=20000")
        assert response.status_code == 422 