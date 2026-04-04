"""Экспорт витрин: parquet/CSV-бандлы, Excel-workbook для DataLens, JSON-сводка."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .io_utils import save_dataframe, save_json


EXCEL_MAX_ROWS = 1_048_576


def export_table_bundle(
    tables: dict[str, pd.DataFrame],
    destination_dir: Path,
    cfg: PipelineConfig,
) -> list[str]:
    """Экспортирует набор таблиц в указанный каталог и возвращает список файлов.

    Каждая таблица сохраняется как parquet, а при включённой опции
    `export_csv` дополнительно записывается и в CSV для ручной проверки
    и загрузки в BI-инструменты.
    """
    exported: list[str] = []
    for name, frame in tables.items():
        path = destination_dir / f"{name}.parquet"
        save_dataframe(frame, path, export_csv=cfg.export_csv)
        exported.append(str(path.relative_to(cfg.project_root)))
        if cfg.export_csv:
            exported.append(str(path.with_suffix(".csv").relative_to(cfg.project_root)))
    return exported


def export_summary(payload: dict[str, Any], path: Path) -> None:
    """Сохраняет итоговую JSON-сводку о выполнении пайплайна."""
    save_json(payload, path)


def _prepare_frame_for_excel(frame: pd.DataFrame) -> pd.DataFrame:
    """Приводит object-колонки к string, чтобы openpyxl не падал на смешанных типах."""
    safe_frame = frame.copy()
    for column in safe_frame.select_dtypes(include=["object"]).columns:
        safe_frame[column] = safe_frame[column].astype("string")
    return safe_frame


def export_excel_workbook(
    tables: dict[str, pd.DataFrame],
    destination_path: Path,
    cfg: PipelineConfig,
) -> str | None:
    """Собирает все BI-витрины в один Excel-workbook для загрузки в DataLens.

    Каждая таблица записывается на отдельный лист. Если ``export_excel``
    выключен в конфигурации — возвращает ``None`` без создания файла.

    Args:
        tables: словарь ``{имя_листа: DataFrame}`` с измерениями и фактами.
        destination_path: путь к выходному ``.xlsx`` файлу.
        cfg: конфигурация pipeline.

    Returns:
        Абсолютный путь к созданному workbook или ``None``.

    Raises:
        ValueError: если таблица превышает лимит строк Excel (1 048 576).
    """
    if not cfg.export_excel:
        return None

    destination_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(destination_path, engine="openpyxl") as writer:
        for sheet_name, frame in tables.items():
            if len(frame) > EXCEL_MAX_ROWS:
                raise ValueError(
                    f"Table '{sheet_name}' has {len(frame)} rows and does not fit into one Excel sheet"
                )
            safe_frame = _prepare_frame_for_excel(frame)
            safe_frame.to_excel(writer, sheet_name=sheet_name, index=False)

    return str(destination_path.resolve())
