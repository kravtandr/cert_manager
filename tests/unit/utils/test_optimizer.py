"""
Тесты для системного оптимизатора
"""

import pytest
from unittest.mock import patch, MagicMock

from src.utils.optimizer import SystemOptimizer


class TestSystemOptimizer:
    """Тесты для SystemOptimizer"""

    def test_get_optimal_settings_small_dataset(self):
        """Тест оптимизации для небольшого датасета"""
        with patch('src.utils.optimizer.cpu_count', return_value=4), \
             patch('src.utils.optimizer.psutil.virtual_memory') as mock_memory:
            
            # Настройка мока памяти (8GB системы)
            mock_memory.return_value.total = 8 * 1024**3
            mock_memory.return_value.available = 6 * 1024**3
            
            settings = SystemOptimizer.get_optimal_settings(100, 200)
            
            assert isinstance(settings, dict)
            assert 'num_workers' in settings
            assert 'batch_size' in settings
            assert 'write_buffer_size' in settings
            assert 'chunk_size' in settings
            
            # Проверяем разумные значения для небольшого датасета
            assert 1 <= settings['num_workers'] <= 4
            assert settings['batch_size'] > 0
            assert settings['write_buffer_size'] > 0

    def test_get_optimal_settings_large_dataset(self):
        """Тест оптимизации для большого датасета"""
        with patch('src.utils.optimizer.cpu_count', return_value=16), \
             patch('src.utils.optimizer.psutil.virtual_memory') as mock_memory:
            
            # Настройка мока памяти (32GB системы)
            mock_memory.return_value.total = 32 * 1024**3
            mock_memory.return_value.available = 24 * 1024**3
            
            settings = SystemOptimizer.get_optimal_settings(100000, 200000)
            
            # Для больших датасетов должны быть больше значения
            assert settings['num_workers'] >= 4
            assert settings['batch_size'] >= 10000

    def test_get_system_info(self):
        """Тест получения информации о системе"""
        with patch('src.utils.optimizer.cpu_count', return_value=8), \
             patch('src.utils.optimizer.psutil.virtual_memory') as mock_memory:
            
            mock_memory.return_value.total = 16 * 1024**3
            mock_memory.return_value.available = 12 * 1024**3
            
            info = SystemOptimizer._get_system_info()
            
            assert info['cpu_cores'] == 8
            assert info['memory_gb'] == 16.0
            assert info['available_memory_gb'] == 12.0

    def test_estimate_data_size(self):
        """Тест оценки размера данных"""
        data_estimates = SystemOptimizer._estimate_data_size(1000, 2000)
        
        assert 'estimated_data_size_gb' in data_estimates
        assert 'avg_certs_per_client' in data_estimates
        assert 'total_possible_assignments' in data_estimates
        assert 'target_memory_per_batch_mb' in data_estimates
        
        assert data_estimates['estimated_data_size_gb'] > 0
        assert data_estimates['avg_certs_per_client'] > 0

    def test_calculate_optimal_workers(self):
        """Тест расчета оптимального количества потоков"""
        # Маленький датасет
        workers = SystemOptimizer._calculate_optimal_workers(0.5, 8)
        assert 1 <= workers <= 4
        
        # Средний датасет
        workers = SystemOptimizer._calculate_optimal_workers(5.0, 8)
        assert workers <= 8
        
        # Большой датасет
        workers = SystemOptimizer._calculate_optimal_workers(50.0, 16)
        assert workers <= 16

    def test_calculate_optimal_batch_size(self):
        """Тест расчета оптимального размера батча"""
        system_info = {
            'memory_gb': 8.0,
            'available_memory_gb': 6.0,
            'cpu_cores': 4
        }
        data_estimates = {
            'target_memory_per_batch_mb': 100.0,
            'estimated_data_size_gb': 1.0,
            'avg_certs_per_client': 5.0,
            'total_possible_assignments': 5000
        }
        
        batch_size = SystemOptimizer._calculate_optimal_batch_size(
            system_info, data_estimates, 1000
        )
        
        assert batch_size > 0
        assert isinstance(batch_size, int)

    def test_calculate_write_buffer_size(self):
        """Тест расчета размера буфера записи"""
        buffer_size = SystemOptimizer._calculate_write_buffer_size(10000)
        
        assert buffer_size > 0
        assert buffer_size <= 10000
        assert isinstance(buffer_size, int)

    def test_calculate_chunk_size(self):
        """Тест расчета размера чанка"""
        # Маленький датасет
        chunk_size = SystemOptimizer._calculate_chunk_size(1000)
        assert 1000 <= chunk_size <= 10000
        
        # Большой датасет
        chunk_size = SystemOptimizer._calculate_chunk_size(10000000)
        assert chunk_size >= 50000 