import logging
import os

# Настройка логирования с поддержкой Docker
log_dir = os.getenv("LOG_DIR", ".")
log_file = os.path.join(log_dir, "certificate_management.log")

# Создаем директорию для логов если её нет
os.makedirs(log_dir, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
