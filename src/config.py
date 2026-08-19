"""Загрузка секретов API из файла .env.

Единственная точка инициализации CLIENT_ID/CLIENT_SECRET для всех модулей:
collector.py берёт эти константы при каждом запросе к API STALZONE.

Как работает:
1. Загружает <корень проекта>/config/.env через python-dotenv
   (сам .env в git не попадает — для шаблона есть .env.example);
2. Кладёт значения в переменные окружения;
3. Экспортирует константы CLIENT_ID/CLIENT_SECRET. Если хотя бы одна
   отсутствует — процесс падает с понятной ошибкой ещё до запуска логики,
   чтобы не уходить в полурабочее состояние.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Модуль лежит в src/, поэтому .env ищем на уровень выше: <корень>/config/.env
load_dotenv(Path(__file__).resolve().parent.parent / "config" / ".env")

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("CLIENT_ID и CLIENT_SECRET должны быть заданы в файле config/.env")
