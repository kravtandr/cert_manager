#!/usr/bin/env python3
"""
Скрипт запуска FastAPI сервера для управления сертификатами

"""

import os

import uvicorn

if __name__ == "__main__":
    # Настройки из переменных окружения
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    workers = int(os.getenv("API_WORKERS", "1"))
    reload = os.getenv("API_RELOAD", "true").lower() == "true"

    print(f"🚀 Запуск API управления сертификатами на {host}:{port}")
    print(f"📊 Рабочих процессов: {workers}")
    print(f"🔄 Автоперезагрузка: {reload}")

    uvicorn.run(
        "src.api:app",
        host=host,
        port=port,
        workers=workers if not reload else 1,  # Reload работает только с 1 worker
        reload=reload,
        log_level="info",
        access_log=True,
    )
