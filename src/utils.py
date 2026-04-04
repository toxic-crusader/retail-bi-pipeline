"""Вспомогательные функции: пути проекта и файловая система."""

from __future__ import annotations

from pathlib import Path


def resolve_project_root() -> Path:
    """Определяет корень проекта относительно расположения модуля, а не cwd."""
    return Path(__file__).resolve().parents[1]


def ensure_directory(path: Path) -> Path:
    """Создаёт каталог (включая родительские) и возвращает его путь."""
    path.mkdir(parents=True, exist_ok=True)
    return path
