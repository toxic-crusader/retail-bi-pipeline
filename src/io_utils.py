"""Ввод-вывод: чтение Excel-источника, сохранение parquet/csv/json."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .utils import ensure_directory


def prepare_project_dirs(cfg: PipelineConfig) -> None:
    """Создаёт структуру каталогов проекта (raw, interim, processed, qa, reports)."""
    for path in (
        cfg.raw_dir,
        cfg.interim_dir,
        cfg.processed_dir,
        cfg.qa_dir,
        cfg.notebooks_dir,
        cfg.reports_dir,
    ):
        ensure_directory(path)


def find_source_file(cfg: PipelineConfig) -> Path:
    """Ищет ``Retail.xlsx`` в допустимых путях; кидает FileNotFoundError если нет."""
    for candidate in cfg.source_candidates:
        if candidate.exists():
            return candidate
    expected = "\n".join(f"- {path}" for path in cfg.source_candidates)
    raise FileNotFoundError(
        f"Retail.xlsx not found. Place the source file in one of these locations:\n{expected}"
    )


def load_retail_data(cfg: PipelineConfig) -> tuple[pd.DataFrame, Path]:
    """Читает ``Retail.xlsx`` и приводит типы (datetime, Int64, boolean).

    Returns:
        Кортеж ``(df, source_path)`` — DataFrame с 10 колонками
        и абсолютный путь к прочитанному файлу.
    """
    source_path = find_source_file(cfg)
    df = pd.read_excel(
        source_path,
        sheet_name=cfg.sheet_name,
        dtype={
            "Invoice": "string",
            "StockCode": "string",
            "Description": "string",
            "Country": "string",
            "Channel": "string",
        },
    )
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Customer ID"] = pd.to_numeric(df["Customer ID"], errors="coerce")
    if "rnd" in df.columns:
        df["rnd"] = (
            df["rnd"]
            .replace({"True": True, "False": False, "true": True, "false": False})
            .astype("boolean")
        )
    return df, source_path


def save_dataframe(df: pd.DataFrame, path: Path, *, export_csv: bool = False) -> None:
    """Сохраняет DataFrame в parquet (и опционально CSV).

    Object-колонки приводятся к string перед записью, чтобы pyarrow
    не падал на смешанных типах в summary-таблицах.
    В CSV булевы колонки записываются как ``0``/``1`` —
    DataLens (ClickHouse) надёжно парсит только числовой формат.
    """
    ensure_directory(path.parent)
    safe_df = df.copy()
    for column in safe_df.select_dtypes(include=["object"]).columns:
        safe_df[column] = safe_df[column].astype("string")
    safe_df.to_parquet(path, index=False)
    if export_csv:
        csv_df = safe_df.copy()
        for column in csv_df.select_dtypes(include=["bool", "boolean"]).columns:
            csv_df[column] = csv_df[column].astype("Int64")
        csv_df.to_csv(
            path.with_suffix(".csv"),
            index=False,
            encoding="utf-8-sig",
            float_format="%.2f",
        )


def save_json(payload: Any, path: Path) -> None:
    """Записывает JSON-объект в файл (UTF-8, indent=2)."""
    ensure_directory(path.parent)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
