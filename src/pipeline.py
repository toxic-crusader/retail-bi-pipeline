"""Оркестратор: запуск полного audit-first pipeline одной командой.

Модуль связывает все шаги проекта — от чтения ``Retail.xlsx`` до записи
Excel-workbook с витринами для DataLens. Может запускаться как CLI-скрипт
(``python -m src.pipeline``) или вызываться программно из notebook.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .classification import classify_line_type
from .config import PipelineConfig
from .dimensions import (
    attach_product_names,
    build_country_dimension,
    build_customer_dimension,
    build_date_dimension,
    build_product_dimension,
)
from .export import export_excel_workbook, export_summary, export_table_bundle
from .facts import build_fact_tables
from .io_utils import load_retail_data, prepare_project_dirs, save_dataframe
from .normalization import apply_normalization
from .qa import build_qa_artifacts

LOGGER = logging.getLogger(__name__)


@dataclass
class PipelineRunResult:
    """Содержит ключевые результаты завершённого запуска пайплайна."""

    source_path: str
    exported_processed_files: list[str]
    exported_qa_files: list[str]
    datalens_workbook_path: str | None
    summary_path: str


def configure_logging() -> None:
    """Настраивает единый формат и уровень логирования для CLI-запуска."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def run_pipeline(cfg: PipelineConfig | None = None) -> PipelineRunResult:
    """Выполняет полный audit-first pipeline от загрузки до экспорта витрин.

    Последовательность: подготовка директорий → чтение источника →
    нормализация → классификация → измерения → факты → QA → экспорт.

    Args:
        cfg: конфигурация pipeline. Если ``None``, используются значения
            по умолчанию из ``PipelineConfig()``.

    Returns:
        ``PipelineRunResult`` с путями ко всем экспортированным артефактам.
    """
    cfg = cfg or PipelineConfig()
    prepare_project_dirs(cfg)

    LOGGER.info("Loading source data")
    raw_df, source_path = load_retail_data(cfg)
    LOGGER.info("Source rows: %s", len(raw_df))

    LOGGER.info("Filtering rnd=True rows")
    rnd_mask = raw_df["rnd"].fillna(False).astype(bool)
    rnd_filtered_df = raw_df.loc[rnd_mask].copy()
    clean_df = raw_df.loc[~rnd_mask].copy()
    LOGGER.info("Removed %s rnd rows, %s rows remaining", len(rnd_filtered_df), len(clean_df))

    LOGGER.info("Normalizing raw data")
    normalized_df = apply_normalization(clean_df, cfg)

    LOGGER.info("Classifying rows")
    audited_df = classify_line_type(normalized_df, cfg)

    LOGGER.info("Building dimensions")
    dim_product = build_product_dimension(audited_df, cfg)
    audited_df = attach_product_names(audited_df, dim_product)
    dim_customer = build_customer_dimension(audited_df, cfg)
    dim_date = build_date_dimension(audited_df)
    dim_country = build_country_dimension(audited_df, cfg)

    LOGGER.info("Saving interim audited layer")
    save_dataframe(
        audited_df,
        cfg.interim_dir / "retail_audited_lines.parquet",
        export_csv=False,
    )

    LOGGER.info("Building fact tables")
    fact_tables = build_fact_tables(audited_df)

    processed_tables = {
        **fact_tables,
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_date": dim_date,
        "dim_country": dim_country,
    }

    LOGGER.info("Building QA artifacts")
    qa_tables, qa_summary = build_qa_artifacts(
        raw_df, audited_df, fact_tables, cfg, rnd_filtered_df=rnd_filtered_df,
    )

    LOGGER.info("Exporting processed and QA layers")
    processed_files = export_table_bundle(processed_tables, cfg.processed_dir, cfg)
    qa_files = export_table_bundle(qa_tables, cfg.qa_dir, cfg)

    LOGGER.info("Exporting DataLens workbook")
    datalens_workbook_path = export_excel_workbook(
        processed_tables,
        cfg.datalens_workbook_path,
        cfg,
    )

    summary_payload: dict[str, Any] = {
        "source_path": str(source_path),
        "project_root": str(cfg.project_root),
        "processed_files": processed_files,
        "qa_files": qa_files,
        "datalens_workbook_path": datalens_workbook_path,
        "datalens_workbook_sheets": list(processed_tables.keys()),
        "qa_summary": qa_summary,
    }
    summary_path = cfg.reports_dir / "pipeline_run_summary.json"
    export_summary(summary_payload, summary_path)

    return PipelineRunResult(
        source_path=str(source_path),
        exported_processed_files=processed_files,
        exported_qa_files=qa_files,
        datalens_workbook_path=datalens_workbook_path,
        summary_path=str(summary_path.relative_to(cfg.project_root)),
    )


def main() -> int:
    """Служит CLI-точкой входа и возвращает код завершения процесса."""
    configure_logging()
    try:
        result = run_pipeline()
    except Exception:
        LOGGER.exception("Pipeline failed")
        return 1

    LOGGER.info("Pipeline finished successfully")
    LOGGER.info("Source: %s", result.source_path)
    LOGGER.info("Processed files: %s", len(result.exported_processed_files))
    LOGGER.info("QA files: %s", len(result.exported_qa_files))
    if result.datalens_workbook_path:
        LOGGER.info("DataLens workbook: %s", result.datalens_workbook_path)
    LOGGER.info("Summary: %s", result.summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
