# Многоэтапная сборка для оптимизации размера образа
FROM python:3.11-slim as builder

# Установка системных зависимостей для сборки
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Создание виртуального окружения
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Копирование requirements и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Финальный образ
FROM python:3.11-slim

# Создание пользователя без root прав (security best practice)
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Установка только runtime зависимостей
RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Копирование виртуального окружения из builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Создание рабочей директории
WORKDIR /app

# Копирование исходного кода
COPY src src


# Создание директорий для данных
RUN mkdir -p /app/output /app/data /app/logs && \
    chown -R appuser:appuser /app

# Переключение на непривилегированного пользователя
USER appuser

# Переменные окружения
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# API настройки
ENV API_HOST=0.0.0.0
ENV API_PORT=8000
ENV API_WORKERS=1
ENV API_RELOAD=false

# Открытие порта
EXPOSE 8000

# Healthcheck для API
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Точка входа - запуск FastAPI сервера
CMD ["python", "src/api/main.py"] 