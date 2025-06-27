"""
Тесты для монитора памяти
"""

import pytest
from unittest.mock import patch, MagicMock

from src.utils.memory_monitor import MemoryMonitor


class TestMemoryMonitor:
    """Тесты для MemoryMonitor"""

    @patch('src.utils.memory_monitor.psutil.Process')
    def test_get_memory_usage(self, mock_process):
        """Тест получения использования памяти"""
        # Настройка мока
        mock_memory_info = MagicMock()
        mock_memory_info.rss = 100 * 1024 * 1024  # 100MB в байтах
        mock_process.return_value.memory_info.return_value = mock_memory_info
        
        memory_mb = MemoryMonitor.get_memory_usage()
        
        assert memory_mb == 100.0
        mock_process.assert_called_once()

    @patch('src.utils.memory_monitor.logger')
    @patch('src.utils.memory_monitor.MemoryMonitor.get_memory_usage')
    def test_log_memory_usage(self, mock_get_memory, mock_logger):
        """Тест логирования использования памяти"""
        # Настройка мока
        mock_get_memory.return_value = 150.5
        
        MemoryMonitor.log_memory_usage("тестовая операция")
        
        mock_get_memory.assert_called_once()
        mock_logger.info.assert_called_once_with(
            "Память после тестовая операция: 150.50 MB"
        )

    @patch('src.utils.memory_monitor.psutil.Process')
    def test_get_memory_usage_different_values(self, mock_process):
        """Тест с разными значениями памяти"""
        test_cases = [
            (50 * 1024 * 1024, 50.0),    # 50MB
            (1024 * 1024 * 1024, 1024.0),  # 1GB
            (512 * 1024, 0.5),            # 512KB
        ]
        
        for rss_bytes, expected_mb in test_cases:
            mock_memory_info = MagicMock()
            mock_memory_info.rss = rss_bytes
            mock_process.return_value.memory_info.return_value = mock_memory_info
            
            memory_mb = MemoryMonitor.get_memory_usage()
            assert memory_mb == expected_mb 