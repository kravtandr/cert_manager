"""
Система управления сертификатами клиентов (Высокопроизводительная версия)
Поддержка больших объемов данных с многопоточностью и потоковой обработкой

"""

import gc
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Generator, List

from faker import Faker
from tqdm import tqdm

from ..database import StreamingCertificateDatabase
from ..utils import MemoryMonitor, SystemOptimizer, logger


class CertificateGenerator:
    """генератор данных с автоматической оптимизацией"""

    def __init__(
        self,
        num_clients: int = 1000,
        num_certificates: int = 2000,
        batch_size: int = None,
        num_workers: int = None,
    ):
        self.num_clients = num_clients
        self.num_certificates = num_certificates

        # Автоматическая оптимизация если параметры не заданы
        if batch_size is None or num_workers is None:
            logger.info("🤖 Автоматическая оптимизация системы:")
            optimal_settings = SystemOptimizer.get_optimal_settings(
                num_clients, num_certificates
            )
            self.batch_size = batch_size or optimal_settings["batch_size"]
            self.num_workers = num_workers or optimal_settings["num_workers"]
        else:
            logger.info("Не используется автоматическая оптимизация:")
            self.batch_size = batch_size
            self.num_workers = num_workers

        logger.info(
            f"Инициализация генератора: {num_clients:,} клиентов, "
            f"{num_certificates:,} сертификатов"
        )
        logger.info(
            f"Размер батча: {self.batch_size:,}, Рабочих потоков: {self.num_workers}"
        )

        # Thread-safe генераторы Faker для каждого потока
        self._faker_instances: Dict[int, Faker] = {}
        self._lock = threading.Lock()

    def _get_faker(self) -> Faker:
        """Получение thread-safe экземпляра Faker"""
        thread_id = threading.get_ident()
        if thread_id not in self._faker_instances:
            with self._lock:
                if thread_id not in self._faker_instances:
                    self._faker_instances[thread_id] = Faker("ru_RU")
        return self._faker_instances[thread_id]

    def generate_clients_batch(self, start_id: int, count: int) -> List[Dict]:
        """Генерация батча клиентов в одном потоке"""
        clients = []
        for i in range(start_id, start_id + count):
            client = {"client_id": i}
            clients.append(client)
        return clients

    def generate_certificates_batch(self, start_id: int, count: int) -> List[Dict]:
        """Генерация батча сертификатов в одном потоке"""
        certificates = []

        # Используем индекс для генерации консистентных ID
        for i in range(count):
            certificate_index = start_id - 1 + i  # start_id начинается с 1
            cert_id = self.generate_consistent_certificate_id(certificate_index)
            certificate = {"certificate_id": cert_id}
            certificates.append(certificate)
        return certificates

    def generate_assignments_batch(
        self, client_ids: List[int], certificate_pool: List[str]
    ) -> List[Dict]:
        """Генерация назначений для батча клиентов"""
        assignments = []

        for client_id in client_ids:
            # Каждый клиент может иметь от 0 до 20 сертификатов
            num_certs = random.randint(0, 20)

            # Каждый клиент может получить любые сертификаты из пула (один сертификат может быть у нескольких клиентов)
            client_certificates = random.sample(
                certificate_pool, min(num_certs, len(certificate_pool))
            )

            for certificate_id in client_certificates:
                # Период действия от 3 месяцев до 20 лет (в днях)
                validity_days = random.randint(90, 7300)

                # Генерируем дату истечения
                # от 365 дней в прошлом и до 7300 дней вперед, перевес в валидные сертификаты
                expiry_date = datetime.now().date() + timedelta(
                    days=random.randint(-365, validity_days)
                )

                assignment = {
                    "client_id": client_id,
                    "certificate_id": certificate_id,
                    "expiry_date": expiry_date,
                }

                assignments.append(assignment)

        return assignments

    def generate_clients_parallel(self) -> Generator[List[Dict], None, None]:
        """Параллельная генерация клиентов батчами"""
        logger.info(f"Генерация {self.num_clients:,} клиентов параллельно...")

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # Создаем задачи для батчей
            futures = []
            for start_id in range(1, self.num_clients + 1, self.batch_size):
                count = min(self.batch_size, self.num_clients - start_id + 1)
                future = executor.submit(self.generate_clients_batch, start_id, count)
                futures.append(future)

            # Получаем результаты по мере готовности
            with tqdm(total=len(futures), desc="Генерация клиентов") as pbar:
                for future in as_completed(futures):
                    batch = future.result()
                    yield batch
                    pbar.update(1)

                    # Принудительная очистка памяти
                    if pbar.n % 10 == 0:
                        gc.collect()

    def generate_certificates_parallel(self) -> Generator[List[Dict], None, None]:
        """Параллельная генерация сертификатов батчами"""
        logger.info(f"Генерация {self.num_certificates:,} сертификатов параллельно...")

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # Создаем задачи для батчей
            futures = []
            remaining = self.num_certificates
            start_id = 1

            while remaining > 0:
                count = min(self.batch_size, remaining)
                future = executor.submit(
                    self.generate_certificates_batch, start_id, count
                )
                futures.append(future)
                start_id += count
                remaining -= count

            # Получаем результаты по мере готовности
            with tqdm(total=len(futures), desc="Генерация сертификатов") as pbar:
                for future in as_completed(futures):
                    batch = future.result()
                    yield batch
                    pbar.update(1)

                    # Принудительная очистка памяти
                    if pbar.n % 10 == 0:
                        gc.collect()

    def generate_consistent_certificate_id(self, index: int) -> str:
        """Генерация консистентного ID сертификата по индексу"""
        # Используем индекс для создания уникального, но воспроизводимого ID
        random.seed(42 + index)  # Seed на основе индекса
        cert_id = str(uuid.uuid4())
        return cert_id

    def get_certificate_pool(self, connection=None) -> List[str]:
        """Получение пула ID сертификатов из базы данных для назначений"""
        logger.info("Получение пула сертификатов из базы данных...")

        if connection is None:
            # Если соединение не передано, создаем временное
            import sqlite3

            connection = sqlite3.connect("data/certificates.db")
            should_close = True
        else:
            should_close = False

        try:
            cursor = connection.cursor()
            cursor.execute("SELECT certificate_id FROM certificates")
            rows = cursor.fetchall()
            certificate_ids = [row[0] for row in rows]

            logger.info(
                f"Получено {len(certificate_ids):,} ID сертификатов из базы данных"
            )
            return certificate_ids

        finally:
            if should_close:
                connection.close()


def main():
    """Основная функция с высокопроизводительной обработкой"""
    logger.info("=== Запуск системы управления сертификатами ===")
    MemoryMonitor.log_memory_usage("старт")

    try:
        # Получаем параметры из переменных окружения или используем значения по умолчанию
        num_clients = int(os.getenv("NUM_CLIENTS", "1000"))
        num_certificates = int(os.getenv("NUM_CERTIFICATES", "2000"))

        # Опциональные параметры
        manual_batch_size = os.getenv("BATCH_SIZE")
        manual_num_workers = os.getenv("NUM_WORKERS")
        manual_buffer_size = os.getenv("BUFFER_SIZE")

        batch_size = int(manual_batch_size) if manual_batch_size else None
        num_workers = int(manual_num_workers) if manual_num_workers else None
        buffer_size = int(manual_buffer_size) if manual_buffer_size else None

        logger.info(
            f"Параметры: {num_clients:,} клиентов, {num_certificates:,} сертификатов"
        )

        # 1. Инициализация генератора данных
        generator = CertificateGenerator(
            num_clients=num_clients,
            num_certificates=num_certificates,
            batch_size=batch_size,  # None = автоматическая оптимизация
            num_workers=num_workers,  # None = автоматическая оптимизация
        )

        # 2. Инициализация БД
        db = StreamingCertificateDatabase(
            write_buffer_size=buffer_size,  # None = автоматическая оптимизация
            num_clients=num_clients,
            num_certificates=num_certificates,
        )
        db.connect()
        db.create_tables()
        MemoryMonitor.log_memory_usage("инициализация")

        # 3. Потоковая генерация и запись клиентов
        logger.info("=== Потоковая обработка клиентов ===")
        start_time = time.time()

        for batch in generator.generate_clients_parallel():
            db.insert_clients_batch(batch)

        elapsed = time.time() - start_time
        logger.info(f"Клиенты обработаны за {elapsed:.2f} сек")
        MemoryMonitor.log_memory_usage("клиенты")

        # 4. Потоковая генерация и запись сертификатов
        logger.info("=== Потоковая обработка сертификатов ===")
        start_time = time.time()

        for batch in generator.generate_certificates_parallel():
            db.insert_certificates_batch(batch)

        elapsed = time.time() - start_time
        logger.info(f"Сертификаты обработаны за {elapsed:.2f} сек")
        MemoryMonitor.log_memory_usage("сертификаты")

        # 5. Генерация назначений сертификатов по клиентам
        logger.info("=== Генерация назначений ===")
        # Сначала принудительно сбрасываем все буферы
        db._flush_clients()
        db._flush_certificates()
        certificate_pool = generator.get_certificate_pool(db.connection)

        # Батчевая обработка назначений
        assignment_batch_size = generator.batch_size
        for start_id in tqdm(
            range(1, num_clients + 1, assignment_batch_size), desc="Назначения"
        ):
            end_id = min(start_id + assignment_batch_size, num_clients + 1)
            client_ids = list(range(start_id, end_id))

            assignments = generator.generate_assignments_batch(
                client_ids, certificate_pool
            )
            db.insert_assignments_batch(assignments)

            # Очистка памяти каждые 10 батчей
            if (start_id // assignment_batch_size) % 10 == 0:
                gc.collect()

        MemoryMonitor.log_memory_usage("назначения")

        # 6. Финальный сброс буферов
        db.flush_all()

        # 7. Потоковое получение активных сертификатов
        logger.info("=== Потоковое получение активных сертификатов ===")
        output_dir = os.getenv("OUTPUT_DIR", "output")
        Path(output_dir).mkdir(exist_ok=True)

        active_certs_file = f"{output_dir}/active_certificates.csv"
        total_active = 0

        with open(active_certs_file, "w", encoding="utf-8") as f:
            f.write("client_id,certificate_id,expiry_date,days_until_expiry\n")

            for chunk_df in db.get_active_certificates_streaming():
                chunk_df.to_csv(f, header=False, index=False)
                total_active += len(chunk_df)

                if total_active % 100000 == 0:
                    logger.info(f"Обработано {total_active:,} активных сертификатов")

        logger.info(f"Всего активных сертификатов: {total_active:,}")
        logger.info(f"Результат сохранен в {active_certs_file}")

        # 8. Статистика производительности
        MemoryMonitor.log_memory_usage("завершение")

        db.close()
        logger.info("=== Высокопроизводительная обработка завершена успешно ===")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
