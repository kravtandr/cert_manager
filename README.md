# 🎯 [Тестовое задание] Система управления сертификатами клиентов

![CI](https://github.com/kravtandr/cert_manager/workflows/CI/badge.svg)
![Coverage](https://codecov.io/gh/kravtandr/cert_manager/branch/main/graph/badge.svg)
![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?logo=fastapi)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?logo=docker&logoColor=white)

> **Enterprise-уровень реализации тестового задания с современной архитектурой, полным покрытием тестами и CI/CD пайплайном**

## 📋 Тестовое задание

**Исходные требования:**
- 1000 клиентов + 2000 сертификатов (сроки от 3 месяцев до 20 лет)
- У каждого клиента от 0 до 20 сертификатов
- Переиспользование истекших сертификатов другими клиентами  
- Синтез данных: id клиента + сертификат + срок действия
- Сохранение в SQLite БД
- Запрос активных сертификатов для всех пользователей

**Требуемые результаты:**
1. ✅ CSV файл с данными
2. ✅ SQLite база данных  
3. ✅ Код с SQL запросами


## ✅ Выполнение тестового задания

**Базовые требования полностью выполнены:**
- 1000 клиентов + 2000 сертификатов
- От 0 до 20 сертификатов на клиента  
- Переиспользование истекших сертификатов
- SQLite база данных с оптимизированными индексами
- CSV отчет по активным сертификатам
- Запрос активных сертификатов для всех пользователей

## 🚀 Дополнительные фичи (сверх задания)

### 🌐 REST API на FastAPI
- **Swagger UI** документация (`/docs`)
- **Асинхронная обработка** запросов
- **Pydantic валидация** данных
- **Пагинация и фильтрация** результатов

### ⚡ Высокопроизводительная обработка
- **Многопоточная генерация** (автоопределение потоков по CPU)
- **Потоковая обработка** (минимальное использование памяти)
- **Автооптимизация** батчей под доступную память
- **Индексированная БД** для быстрых запросов
- **Масштабируемость**: поддержка 1M+ записей

### 🧪 Качество и тестирование
- **53 теста** (Unit, Integration, API)  
- **67% покрытие** кода тестами
- **Линтеры**: flake8, black, isort, mypy
- **Pre-commit hooks** для контроля качества
- **Make команды** для разработки

### 🚀 CI/CD
- **GitHub Actions** с автотестами
- **Docker + Compose** для развертывания  
- **Codecov** интеграция для coverage

### 🏗️ Архитектура
- **Модульная структура** (API, Core, Database, Utils)
- **Принципы SOLID** и чистый код
- **Dependency Injection** паттерн
- **Pydantic модели** для валидации

## 🚀 Запуск

### Docker (рекомендуется)
```bash
# Запуск API сервера
docker-compose up --build

# Swagger UI: http://localhost:8000/docs
```

### Локальный запуск
```bash
pip install -r requirements.txt
python src/api/main.py

# Команды разработки
make lint         # Проверка кода (flake8, mypy)
make format       # Автоформатирование (black, isort)  
make test         # Запуск всех тестов
make test-coverage # Тесты с отчетом покрытия
make check        # Полная проверка (lint + test)
```

## 📋 API эндпоинты

### Генерация данных
```bash
POST /generation/start
{
  "num_clients": 1000,
  "num_certificates": 2000
}
```

### Получение активных сертификатов
```bash
GET /certificates/active?page=1&page_size=100
```

### Скачивание CSV отчета
```bash
GET /certificates/active/download
```

### Мониторинг
```bash
GET /generation/status  # Прогресс генерации
GET /health            # Состояние системы
```

## 🎯 Структура проекта

```
test_task/
├── 📁 src/                     # Исходный код
│   ├── 📁 api/                # REST API (FastAPI)
│   │   ├── __init__.py        # Инициализация API модуля
│   │   ├── main.py            # Точка входа приложения
│   │   ├── handlers.py        # API обработчики
│   │   └── models.py          # Pydantic модели
│   ├── 📁 core/               # Бизнес логика
│   │   ├── __init__.py        # Экспорты core модуля
│   │   └── data_generator.py  # Генератор данных
│   ├── 📁 database/           # Работа с БД
│   │   ├── __init__.py        # Экспорты database модуля  
│   │   ├── database.py        # Потоковая БД с оптимизацией
│   │   └── sql_queries.py     # SQL запросы
│   └── 📁 utils/              # Утилиты
│       ├── __init__.py        # Экспорты utils модуля
│       ├── optimizer.py       # Автооптимизация системы
│       ├── memory_monitor.py  # Мониторинг памяти
│       ├── my_logger.py       # Логирование
│       └── csv_export.py      # CSV экспорт
├── 📁 tests/                   # Тестирование (53 теста)
│   ├── __init__.py            # Инициализация тестов
│   ├── conftest.py            # Pytest конфигурация и фикстуры
│   ├── 📁 unit/               # Unit тесты (модульные)
│   │   ├── test_generator.py  # Тесты генератора данных
│   │   ├── test_database.py   # Тесты базы данных
│   │   └── test_utils.py      # Тесты утилит
│   ├── 📁 integration/        # Интеграционные тесты
│   │   ├── test_data_flow.py  # Тесты потока данных
│   │   └── test_performance.py # Тесты производительности
│   └── 📁 api/                # API тесты
│       ├── test_endpoints.py  # Тесты эндпоинтов
│       └── test_models.py     # Тесты моделей
├── 📁 data/                   # База данных (результаты)
│   └── certificates.db        # SQLite БД (✅ результат задания)
├── 📁 output/                 # Выходные файлы
│   └── active_certificates.csv # CSV отчет (✅ результат задания)
├── 📁 logs/                   # Логи выполнения
│   └── certificate_management.log # Детальные логи
├── 📁 htmlcov/                # Coverage отчеты (67%)
│   └── index.html             # HTML отчет покрытия тестами
├── 📁 .github/                # CI/CD конфигурация
│   └── 📁 workflows/          # GitHub Actions
│       └── ci.yml             # CI пайплайн
├── .gitignore                 # Git игнорирование файлов
├── .pre-commit-config.yaml    # Pre-commit hooks конфигурация
├── Makefile                   # Команды разработки
├── docker-compose.yml         # Docker Compose конфигурация
├── Dockerfile                # Docker образ
├── requirements.txt           # Python зависимости
├── task.md                   # 📋 Исходное тестовое задание
└── README.md                 # 📚 Документация проекта
```

## 💡 Ключевые особенности реализации

### Производительность
- **Многопоточность**: автоматическое определение количества потоков
- **Потоковая обработка**: минимальное использование памяти
- **Батчевая запись**: оптимизация операций с БД
- **Индексы БД**: быстрые запросы активных сертификатов

### Автооптимизация
- Размер батчей на основе доступной памяти
- Количество потоков на основе CPU
- Размер буферов для оптимальной записи
- Адаптация под характеристики системы

### Тестирование 
- **53 теста** в 3 категориях (Unit, Integration, API)
- **67% покрытие** кода с отчетами в `htmlcov/`
- **Автоматические линтеры**: flake8, black, isort, mypy
- **Pre-commit hooks** для контроля качества
- **CI/CD пайплайн** с GitHub Actions на Python 3.10

### API возможности
- Настраиваемые параметры генерации
- Фильтрация по ID клиента
- Пагинация результатов
- Отслеживание прогресса в реальном времени
- Проверка состояния системы

## 📊 Примеры использования

### curl
```bash
# Запуск генерации 5000 клиентов, 10000 сертификатов
curl -X POST "http://localhost:8000/generation/start" \
  -H "Content-Type: application/json" \
  -d '{"num_clients": 5000, "num_certificates": 10000}'

# Получение активных сертификатов клиента 123
curl "http://localhost:8000/certificates/active?client_id_filter=123"

# Скачивание полного отчета
curl -O "http://localhost:8000/certificates/active/download"
```

### Python
```python
import requests

# Генерация данных
response = requests.post("http://localhost:8000/generation/start", json={
    "num_clients": 1000,
    "num_certificates": 2000
})

# Получение результатов  
response = requests.get("http://localhost:8000/certificates/active")
data = response.json()
print(f"Найдено {data['total_count']} активных сертификатов")
```

## 🧪 Тестирование

```bash
# Установка зависимостей для разработки
pip install -r requirements.txt

# Все тесты с coverage отчетом
make test

# Отдельные категории тестов
make test-unit           # Unit тесты (модульные)
make test-integration    # Интеграционные тесты (БД, файлы)
make test-api           # API тесты (эндпоинты)

# Coverage отчет в браузере
make test-coverage       # Генерирует htmlcov/index.html
```

**📊 Статистика тестов:**
- **53 теста** в 3 категориях 
- **67% покрытие** кода
- **Автоматический запуск** в CI/CD
- **Coverage отчеты** в `htmlcov/`

## 🚀 CI/CD Pipeline

**GitHub Actions** автоматически запускаются при каждом push/PR:

| Этап | Действие | Статус |
|------|----------|--------|
| **Lint** | flake8, black, isort, mypy | ✅ |
| **Test** | pytest на Python 3.9-3.11 | ✅ |
| **Coverage** | codecov интеграция | ✅ |
| **Build** | Docker образ | ✅ |

**Результат:** Полная автоматизация контроля качества кода

## ⚙️ Настройка

### Переменные окружения
```bash
# Генерация данных
NUM_CLIENTS=5000
NUM_CERTIFICATES=10000
BATCH_SIZE=50000        # автоматически если не указан
NUM_WORKERS=8           # автоматически если не указан

# API настройки  
API_HOST=0.0.0.0
API_PORT=8000

# Пути
DB_PATH=data/certificates.db
OUTPUT_DIR=output
LOG_DIR=logs
```