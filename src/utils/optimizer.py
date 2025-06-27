from multiprocessing import cpu_count
from typing import Dict

import psutil


class SystemOptimizer:
    """Автоматическая оптимизация параметров на основе характеристик системы"""

    @staticmethod
    def get_optimal_settings(num_clients: int, num_certificates: int) -> Dict:
        """Автоматическое определение оптимальных настроек"""
        system_info = SystemOptimizer._get_system_info()
        data_estimates = SystemOptimizer._estimate_data_size(
            num_clients, num_certificates
        )

        num_workers = SystemOptimizer._calculate_optimal_workers(
            data_estimates["estimated_data_size_gb"], system_info["cpu_cores"]
        )
        batch_size = SystemOptimizer._calculate_optimal_batch_size(
            system_info, data_estimates, num_clients
        )
        write_buffer_size = SystemOptimizer._calculate_write_buffer_size(batch_size)
        chunk_size = SystemOptimizer._calculate_chunk_size(
            data_estimates["total_possible_assignments"]
        )

        return {
            "num_workers": num_workers,
            "batch_size": batch_size,
            "write_buffer_size": write_buffer_size,
            "chunk_size": chunk_size,
            "estimated_data_size_gb": data_estimates["estimated_data_size_gb"],
            "target_memory_per_batch_mb": data_estimates["target_memory_per_batch_mb"],
            "total_possible_assignments": int(
                data_estimates["total_possible_assignments"]
            ),
        }

    @staticmethod
    def _get_system_info() -> Dict:
        """Получение информации о системе"""
        return {
            "cpu_cores": cpu_count(),
            "memory_gb": psutil.virtual_memory().total / (1024**3),
            "available_memory_gb": psutil.virtual_memory().available / (1024**3),
        }

    @staticmethod
    def _estimate_data_size(num_clients: int, num_certificates: int) -> Dict:
        """Оценка размера данных"""
        avg_certs_per_client = (
            min(10, num_certificates / num_clients) if num_clients > 0 else 10
        )
        estimated_data_size_gb = (
            (num_clients * 50)
            + (num_certificates * 100)
            + (num_clients * avg_certs_per_client * 150)
        ) / (1024**3)

        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        target_memory_per_batch_mb = max(
            10, min(250, available_memory_gb * 1024 * 0.15)
        )

        return {
            "estimated_data_size_gb": estimated_data_size_gb,
            "avg_certs_per_client": avg_certs_per_client,
            "total_possible_assignments": num_clients * avg_certs_per_client,
            "target_memory_per_batch_mb": target_memory_per_batch_mb,
        }

    @staticmethod
    def _calculate_optimal_workers(
        estimated_data_size_gb: float, cpu_cores: int
    ) -> int:
        """Расчет оптимального количества потоков"""
        if estimated_data_size_gb < 1:
            return min(cpu_cores, 4)
        elif estimated_data_size_gb < 10:
            return min(cpu_cores, 8)
        elif estimated_data_size_gb < 100:
            return min(cpu_cores, 12)
        else:
            return cpu_cores

    @staticmethod
    def _calculate_optimal_batch_size(
        system_info: Dict, data_estimates: Dict, num_clients: int
    ) -> int:
        """Расчет оптимального размера батча"""
        memory_gb = system_info["memory_gb"]
        target_memory_per_batch_mb = data_estimates["target_memory_per_batch_mb"]

        # Примерный размер одной записи в памяти
        estimated_record_size_bytes = 200
        optimal_batch_from_memory = int(
            target_memory_per_batch_mb * 1024 * 1024 / estimated_record_size_bytes
        )

        # Ограничиваем batch_size разумными пределами
        if memory_gb < 2:
            return min(optimal_batch_from_memory, 5000, max(1000, num_clients // 200))
        elif memory_gb < 8:
            return min(optimal_batch_from_memory, 50000, max(5000, num_clients // 100))
        elif memory_gb < 32:
            return min(optimal_batch_from_memory, 200000, max(10000, num_clients // 50))
        else:
            return min(
                optimal_batch_from_memory, 1000000, max(50000, num_clients // 20)
            )

    @staticmethod
    def _calculate_write_buffer_size(batch_size: int) -> int:
        """Расчет размера буфера записи"""
        return min(batch_size // 2, 100000, max(1000, batch_size // 4))

    @staticmethod
    def _calculate_chunk_size(total_possible_assignments: float) -> int:
        """Расчет размера чанка"""
        if total_possible_assignments < 50000:
            return min(10000, max(1000, int(total_possible_assignments // 5)))
        elif total_possible_assignments < 500000:
            return min(25000, max(5000, int(total_possible_assignments // 10)))
        elif total_possible_assignments < 5000000:
            return min(50000, max(10000, int(total_possible_assignments // 20)))
        elif total_possible_assignments < 50000000:
            return min(100000, max(25000, int(total_possible_assignments // 50)))
        else:
            return min(200000, max(50000, int(total_possible_assignments // 100)))
