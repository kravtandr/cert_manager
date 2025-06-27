import os
from pathlib import Path

from .my_logger import logger


def save_to_csv_streaming(data_generator, output_dir: str = None):
    """Потоковое сохранение в CSV без загрузки всех данных в память"""
    if output_dir is None:
        output_dir = os.getenv("OUTPUT_DIR", "output")

    try:
        Path(output_dir).mkdir(exist_ok=True)

        # Потоковая запись клиентов
        clients_file = f"{output_dir}/clients.csv"
        with open(clients_file, "w", encoding="utf-8") as f:
            f.write("client_id\n")  # Заголовок

            for batch in data_generator.generate_clients_parallel():
                for client in batch:
                    f.write(f"{client['client_id']}\n")

        logger.info(f"Клиенты сохранены в {clients_file}")

        # Потоковая запись сертификатов
        certificates_file = f"{output_dir}/certificates.csv"
        with open(certificates_file, "w", encoding="utf-8") as f:
            f.write("certificate_id\n")  # Заголовок

            for batch in data_generator.generate_certificates_parallel():
                for cert in batch:
                    f.write(f"{cert['certificate_id']}\n")

        logger.info(f"Сертификаты сохранены в {certificates_file}")

    except Exception as e:
        logger.error(f"Ошибка потокового сохранения в CSV: {e}")
        raise
