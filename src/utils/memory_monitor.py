import psutil

from .my_logger import logger


class MemoryMonitor:
    """Мониторинг использования памяти"""

    @staticmethod
    def get_memory_usage():
        """Получение текущего использования памяти в MB"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024

    @staticmethod
    def log_memory_usage(operation: str):
        """Логирование использования памяти"""
        memory_mb = MemoryMonitor.get_memory_usage()
        logger.info(f"Память после {operation}: {memory_mb:.2f} MB")
