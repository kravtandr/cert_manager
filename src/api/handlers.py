"""
FastAPI приложение для управления сертификатами клиентов
Предоставляет API для генерации данных и получения активных сертификатов

"""

import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from ..core import CertificateGenerator
from ..database import StreamingCertificateDatabase
from ..utils import MemoryMonitor, SystemOptimizer, logger
from .models import (
    ActiveCertificate,
    ActiveCertificatesResponse,
    ClientCertificateAssignment,
    ClientCertificateAssignmentsResponse,
    GenerationRequest,
    GenerationStatus,
)

# Глобальные переменные для отслеживания состояния
generation_status = {
    "is_running": False,
    "progress": 0,
    "total": 0,
    "current_stage": "",
    "start_time": None,
    "error": None,
}

# Пул потоков для фоновых задач
executor = ThreadPoolExecutor(max_workers=2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    logger.info("🚀 Запуск API управления сертификатами")

    # Создаем необходимые директории
    os.makedirs("data", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    yield

    logger.info("🛑 Остановка API управления сертификатами")
    executor.shutdown(wait=True)


app = FastAPI(
    title="API управления сертификатами",
    description="Высокопроизводительная система для генерации и управления сертификатами клиентов",
    version="1.0.0",
    lifespan=lifespan,
)


def update_progress(stage: str, current: int, total: int):
    """Обновление прогресса генерации"""
    global generation_status
    generation_status.update(
        {"current_stage": stage, "progress": current, "total": total}
    )


def run_generation_task(request: GenerationRequest):
    """Фоновая задача генерации данных"""
    global generation_status

    try:
        total_items = request.num_clients + request.num_certificates
        generation_status.update(
            {
                "is_running": True,
                "progress": 0,
                "total": total_items,
                "current_stage": "Инициализация",
                "start_time": datetime.now(),
                "error": None,
            }
        )

        logger.info(
            f"Начинаем генерацию: {request.num_clients:,} клиентов, {request.num_certificates:,} сертификатов"
        )

        # 1. Инициализация генератора
        total_value = generation_status.get("total", 0)
        total = int(total_value) if total_value is not None and isinstance(total_value, (int, str)) else 0
        update_progress("Инициализация генератора", 0, total)
        generator = CertificateGenerator(
            num_clients=request.num_clients,
            num_certificates=request.num_certificates,
            batch_size=request.batch_size,
            num_workers=request.num_workers,
        )

        # 2. Инициализация БД
        update_progress("Инициализация базы данных", 0, total)
        db = StreamingCertificateDatabase(
            db_path="data/certificates.db",
            write_buffer_size=request.write_buffer_size,
            num_clients=request.num_clients,
            num_certificates=request.num_certificates,
        )
        db.connect()
        db.create_tables()

        # 3. Генерация клиентов
        update_progress("Генерация клиентов", 0, total)
        client_count = 0

        for batch in generator.generate_clients_parallel():
            db.insert_clients_batch(batch)
            client_count += len(batch)
            update_progress("Генерация клиентов", client_count, total)

        # 4. Генерация сертификатов
        update_progress("Генерация сертификатов", client_count, total)
        cert_count = 0

        for batch in generator.generate_certificates_parallel():
            db.insert_certificates_batch(batch)
            cert_count += len(batch)
            update_progress(
                "Генерация сертификатов", client_count + cert_count, total
            )

        # 5. Генерация назначений
        update_progress("Генерация назначений", client_count + cert_count, total)
        # Сначала принудительно сбрасываем все буферы
        db._flush_clients()
        db._flush_certificates()
        certificate_pool = generator.get_certificate_pool(connection=db.connection)

        logger.info(
            f"Получен пул из {len(certificate_pool):,} сертификатов для назначений"
        )

        assignment_batch_size = generator.batch_size
        total_assignments = 0

        for start_id in range(1, request.num_clients + 1, assignment_batch_size):
            end_id = min(start_id + assignment_batch_size, request.num_clients + 1)
            client_ids = list(range(start_id, end_id))

            assignments = generator.generate_assignments_batch(
                client_ids, certificate_pool
            )
            db.insert_assignments_batch(assignments)
            total_assignments += len(assignments)

        logger.info(f"Создано {total_assignments:,} назначений сертификатов")

        # 6. Финальная обработка
        update_progress("Финализация", total, total)
        db.flush_all()

        # 7. Генерация отчета по активным сертификатам
        update_progress("Создание отчета", total, total)
        output_dir = "output"
        Path(output_dir).mkdir(exist_ok=True)

        active_certs_file = f"{output_dir}/active_certificates.csv"
        total_active = 0

        with open(active_certs_file, "w", encoding="utf-8") as f:
            f.write("client_id,certificate_id,expiry_date,days_until_expiry\n")

            for chunk_df in db.get_active_certificates_streaming():
                chunk_df.to_csv(f, header=False, index=False)
                total_active += len(chunk_df)

        db.close()

        logger.info(
            f"Генерация завершена успешно. Активных сертификатов: {total_active:,}"
        )

        generation_status.update(
            {
                "is_running": False,
                "current_stage": f"Завершено. Активных сертификатов: {total_active:,}",
                "progress": generation_status["total"],
            }
        )

    except Exception as e:
        logger.error(f"Ошибка генерации: {e}")
        generation_status.update(
            {"is_running": False, "error": str(e), "current_stage": "Ошибка"}
        )


@app.post(
    "/generation/start",
    response_model=Dict[str, str],
    summary="Запуск генерации данных",
)
async def start_generation(
    background_tasks: BackgroundTasks, request: GenerationRequest = None
):
    """
    Запуск генерации данных о клиентах и сертификатах.

    Генерация выполняется в фоновом режиме. Используйте /generation/status для отслеживания прогресса.
    Если тело запроса не передано, используются значения по умолчанию.
    """
    global generation_status

    if generation_status["is_running"]:
        raise HTTPException(status_code=409, detail="Генерация уже выполняется")

    # Если запрос не передан, используем значения по умолчанию
    if request is None:
        request = GenerationRequest()

    # Проверяем системные ресурсы
    optimal_settings = SystemOptimizer.get_optimal_settings(
        request.num_clients, request.num_certificates
    )

    logger.info(f"Запуск генерации с параметрами: {request}")
    logger.info(f"Рекомендуемые настройки: {optimal_settings}")

    # Запускаем генерацию в фоне
    background_tasks.add_task(run_generation_task, request)

    return {
        "message": "Генерация запущена",
        "status": "started",
        "estimated_data_size_gb": f"{optimal_settings['estimated_data_size_gb']:.2f}",
    }


@app.get(
    "/generation/status", response_model=GenerationStatus, summary="Статус генерации"
)
async def get_generation_status():
    """
    Получение текущего статуса генерации данных.

    Возвращает информацию о прогрессе, текущем этапе и возможных ошибках.
    """
    global generation_status

    # Расчет оставшегося времени
    estimated_time_remaining = None
    if (
        generation_status["is_running"]
        and generation_status["start_time"]
        and generation_status["progress"] > 0
    ):
        elapsed = (datetime.now() - generation_status["start_time"]).total_seconds()
        progress_ratio = generation_status["progress"] / generation_status["total"]

        if progress_ratio > 0:
            total_estimated = elapsed / progress_ratio
            estimated_time_remaining = int(total_estimated - elapsed)

    return GenerationStatus(
        **generation_status, estimated_time_remaining=estimated_time_remaining
    )


@app.get(
    "/certificates/active",
    response_model=ActiveCertificatesResponse,
    summary="Получение активных сертификатов",
)
async def get_active_certificates(
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=10000, description="Размер страницы"),
    client_id_filter: Optional[int] = Query(None, description="Фильтр по ID клиента"),
):
    """
    Получение списка клиентов с активными сертификатами.

    Возвращает постраничный список активных сертификатов с возможностью фильтрации.
    """
    db_path = "data/certificates.db"

    if not os.path.exists(db_path):
        raise HTTPException(
            status_code=404,
            detail="База данных не найдена. Сначала запустите генерацию данных.",
        )

    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row

        current_date = datetime.now().date()
        offset = (page - 1) * page_size

        # Основной запрос
        where_clause = "WHERE cc.expiry_date > ?"
        params = [str(current_date)]

        if client_id_filter:
            where_clause += " AND c.client_id = ?"
            params.append(str(client_id_filter))

        query = f"""
            SELECT
                c.client_id,
                cert.certificate_id,
                cc.expiry_date,
                ROUND((julianday(cc.expiry_date) - julianday(date('now'))), 0) as days_until_expiry
            FROM clients c
            JOIN client_certificates cc ON c.client_id = cc.client_id
            JOIN certificates cert ON cc.certificate_id = cert.certificate_id
            {where_clause}
            ORDER BY c.client_id, cc.expiry_date
            LIMIT ? OFFSET ?
        """

        # Параметры: WHERE clause параметры + LIMIT + OFFSET
        final_params = params + [page_size, offset]

        df = pd.read_sql_query(query, connection, params=final_params)

        # Подсчет общего количества
        count_query = f"""
            SELECT COUNT(*) as total
            FROM clients c
            JOIN client_certificates cc ON c.client_id = cc.client_id
            JOIN certificates cert ON cc.certificate_id = cert.certificate_id
            {where_clause}
        """

        count_params = params  # Используем те же параметры что и в основном запросе

        total_count = pd.read_sql_query(
            count_query, connection, params=count_params
        ).iloc[0]["total"]

        connection.close()

        # Преобразование в модели Pydantic
        certificates = []
        for _, row in df.iterrows():
            # Безопасное преобразование days_until_expiry
            days_until_expiry = 0
            if (
                pd.notna(row["days_until_expiry"])
                and row["days_until_expiry"] is not None
            ):
                days_until_expiry = int(row["days_until_expiry"])

            certificates.append(
                ActiveCertificate(
                    client_id=int(row["client_id"]),
                    certificate_id=str(row["certificate_id"]),
                    expiry_date=str(row["expiry_date"]),
                    days_until_expiry=days_until_expiry,
                )
            )

        has_next = (page * page_size) < total_count

        return ActiveCertificatesResponse(
            certificates=certificates,
            total_count=total_count,
            page=page,
            page_size=page_size,
            has_next=has_next,
        )

    except Exception as e:
        logger.error(f"Ошибка получения активных сертификатов: {e}")
        raise HTTPException(
            status_code=500, detail=f"Ошибка получения данных: {str(e)}"
        )


@app.get(
    "/client-certificates",
    response_model=ClientCertificateAssignmentsResponse,
    summary="Получение связей клиент-сертификат",
)
async def get_client_certificate_assignments(
    skip: int = Query(0, ge=0, description="Количество записей для пропуска (offset)"),
    limit: int = Query(
        100, ge=1, le=10000, description="Максимальное количество записей"
    ),
    client_id_filter: Optional[int] = Query(None, description="Фильтр по ID клиента"),
    active_only: bool = Query(
        False, description="Показать только активные сертификаты"
    ),
):
    """
    Получение связей между клиентами и сертификатами с пагинацией.

    Возвращает список всех назначений сертификатов с информацией о статусе и времени до истечения.
    """
    db_path = "data/certificates.db"

    if not os.path.exists(db_path):
        raise HTTPException(
            status_code=404,
            detail="База данных не найдена. Сначала запустите генерацию данных.",
        )

    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row

        current_date = datetime.now().date()

        # Базовый запрос
        where_conditions = []
        params = []

        # Фильтр по клиенту
        if client_id_filter:
            where_conditions.append("cc.client_id = ?")
            params.append(str(client_id_filter))

        # Фильтр только активные
        if active_only:
            where_conditions.append("cc.expiry_date > ?")
            params.append(str(current_date))

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Основной запрос с пагинацией
        query = f"""
            SELECT
                cc.id,
                cc.client_id,
                cc.certificate_id,
                cc.expiry_date,
                CASE WHEN cc.expiry_date > ? THEN 1 ELSE 0 END as is_active,
                CASE WHEN cc.expiry_date > ?
                     THEN ROUND((julianday(cc.expiry_date) - julianday(?)), 0)
                     ELSE NULL
                END as days_until_expiry
            FROM client_certificates cc
            {where_clause}
            ORDER BY cc.client_id, cc.expiry_date DESC
            LIMIT ? OFFSET ?
        """

        # Параметры для основного запроса
        query_params = (
            [str(current_date), str(current_date), str(current_date)] + params + [limit, skip]
        )

        df = pd.read_sql_query(query, connection, params=query_params)

        # Подсчет общего количества
        count_query = f"""
            SELECT COUNT(*) as total
            FROM client_certificates cc
            {where_clause}
        """

        count_params = params
        total_count = pd.read_sql_query(
            count_query, connection, params=count_params
        ).iloc[0]["total"]

        connection.close()

        # Преобразование в модели Pydantic
        assignments = []
        for _, row in df.iterrows():
            # Безопасное преобразование days_until_expiry
            days_until_expiry = None
            if (
                pd.notna(row["days_until_expiry"])
                and row["days_until_expiry"] is not None
            ):
                days_until_expiry = int(row["days_until_expiry"])

            assignments.append(
                ClientCertificateAssignment(
                    id=int(row["id"]),
                    client_id=int(row["client_id"]),
                    certificate_id=str(row["certificate_id"]),
                    expiry_date=str(row["expiry_date"]),
                    is_active=bool(row["is_active"]),
                    days_until_expiry=days_until_expiry,
                )
            )

        has_next = (skip + limit) < total_count

        return ClientCertificateAssignmentsResponse(
            assignments=assignments,
            total_count=total_count,
            skip=skip,
            limit=limit,
            has_next=has_next,
        )

    except Exception as e:
        logger.error(f"Ошибка получения связей клиент-сертификат: {e}")
        raise HTTPException(
            status_code=500, detail=f"Ошибка получения данных: {str(e)}"
        )


@app.get(
    "/certificates/active/download", summary="Скачать отчет по активным сертификатам"
)
async def download_active_certificates():
    """
    Скачивание полного отчета по активным сертификатам в формате CSV.

    Возвращает файл со всеми активными сертификатами.
    """
    csv_file = "output/active_certificates.csv"

    if not os.path.exists(csv_file):
        raise HTTPException(
            status_code=404,
            detail="Файл отчета не найден. Сначала запустите генерацию данных.",
        )

    return FileResponse(
        path=csv_file, filename="active_certificates.csv", media_type="text/csv"
    )


@app.get("/health", summary="Проверка состояния системы")
async def health_check():
    """Проверка состояния системы и ресурсов"""
    memory_usage = MemoryMonitor.get_memory_usage()

    return {
        "status": "healthy",
        "memory_usage_mb": round(memory_usage, 2),
        "generation_running": generation_status["is_running"],
        "database_exists": os.path.exists("data/certificates.db"),
    }


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
