.PHONY: help lint format check test clean install

# Переменные
PYTHON = python3
PIP = pip3
SRC_DIR = src
VENV = .venv

help: ## Показать справку
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Установить зависимости
	$(PIP) install -r requirements.txt

lint: ## Запустить линтеры
	@echo "🔍 Запуск flake8..."
	flake8 $(SRC_DIR)
	@echo "🔍 Запуск mypy..."
	mypy $(SRC_DIR)
	@echo "✅ Линтинг завершен!"

format: ## Форматировать код
	@echo "🎨 Сортировка импортов с isort..."
	isort $(SRC_DIR)
	@echo "🎨 Форматирование с black..."
	black $(SRC_DIR)
	@echo "✅ Форматирование завершено!"

format-check: ## Проверить форматирование без изменений
	@echo "🔍 Проверка сортировки импортов..."
	isort --check-only $(SRC_DIR)
	@echo "🔍 Проверка форматирования..."
	black --check $(SRC_DIR)
	@echo "✅ Проверка форматирования завершена!"

check: format-check lint ## Полная проверка кода
	@echo "🎯 Все проверки пройдены!"

fix: format lint ## Исправить и проверить код
	@echo "🛠️ Код исправлен и проверен!"

test: ## Запустить все тесты
	@echo "🧪 Запуск тестов..."
	PYTHONPATH=. pytest

test-unit: ## Запустить только unit тесты
	@echo "🧪 Запуск unit тестов..."
	PYTHONPATH=. pytest tests/unit/ -v

test-integration: ## Запустить только интеграционные тесты
	@echo "🧪 Запуск интеграционных тестов..."
	PYTHONPATH=. pytest tests/integration/ -v -m integration

test-api: ## Запустить только API тесты
	@echo "🧪 Запуск API тестов..."
	PYTHONPATH=. pytest tests/api/ -v -m api

test-coverage: ## Запустить тесты с coverage
	@echo "🧪 Запуск тестов с coverage..."
	PYTHONPATH=. pytest --cov=src --cov-report=html --cov-report=term-missing
	@echo "📊 Coverage отчет создан в htmlcov/"

test-slow: ## Запустить медленные тесты
	@echo "🧪 Запуск медленных тестов..."
	PYTHONPATH=. pytest -v -m slow

clean: ## Очистить временные файлы
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".mypy_cache" -delete
	find . -type d -name ".pytest_cache" -delete
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -name "coverage.xml" -delete
	find . -name ".coverage" -delete
	@echo "🧹 Временные файлы удалены!"

# Команды для CI/CD
ci-check: ## Проверки для CI/CD
	@echo "🚀 Запуск проверок для CI/CD..."
	flake8 $(SRC_DIR) --format=json --output-file=flake8-report.json || true
	mypy $(SRC_DIR) --json-report=mypy-report.json || true
	black --check $(SRC_DIR)
	isort --check-only $(SRC_DIR)
	pytest --cov=src --cov-report=xml --cov-fail-under=80
	@echo "✅ CI/CD проверки завершены!" 