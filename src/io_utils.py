from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .utils import ensure_directory


def prepare_project_dirs(cfg: PipelineConfig) -> None:
    """Создаёт все рабочие каталоги проекта, если они ещё не существуют.

    Функция вызывается в начале пайплайна, чтобы все последующие шаги
    могли безопасно записывать файлы в raw/interim/processed/qa/reports.
    """
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
    """Находит исходный файл `Retail.xlsx` по списку допустимых путей.

    Если файл отсутствует во всех ожидаемых местах, выбрасывается
    `FileNotFoundError` с подсказкой, куда его нужно положить.
    """
    for candidate in cfg.source_candidates:
        if candidate.exists():
            return candidate
    expected = "\n".join(f"- {path}" for path in cfg.source_candidates)
    raise FileNotFoundError(
        f"Retail.xlsx not found. Place the source file in one of these locations:\n{expected}"
    )


def load_retail_data(cfg: PipelineConfig) -> tuple[pd.DataFrame, Path]:
    """Читает Excel-источник и приводит базовые типы колонок.

    Помимо загрузки листа функция приводит даты, числовые поля и
    технический признак `rnd` к совместимым типам pandas, чтобы
    последующие шаги пайплайна работали детерминированно.
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
    """Сохраняет таблицу в parquet и, опционально, в CSV.

    Перед записью функция нормализует `object`-колонки в строковый тип,
    чтобы избежать проблем сериализации при экспорте в parquet через
    `pyarrow`, особенно на смешанных summary-таблицах.
    """
    ensure_directory(path.parent)
    safe_df = df.copy()
    for column in safe_df.select_dtypes(include=["object"]).columns:
        safe_df[column] = safe_df[column].astype("string")
    safe_df.to_parquet(path, index=False)
    if export_csv:
        safe_df.to_csv(path.with_suffix(".csv"), index=False, encoding="utf-8-sig")


def save_json(payload: Any, path: Path) -> None:
    """Сохраняет произвольный JSON-совместимый объект в UTF-8 файл."""
    ensure_directory(path.parent)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
