"""Генерация QA-артефактов: контрольные таблицы и числовая сводка.

Модуль собирает 17 аудитных таблиц (дубликаты, возвраты, экстремумы,
пропуски и т.д.) и компактный JSON-summary для автоматизированной проверки.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .config import PipelineConfig
from .profiling import (
    build_basic_profile,
    build_country_mapping_table,
    build_extreme_rows,
    build_last_month_summary,
    build_line_type_summary,
    build_missingness_profile,
    build_stock_description_issues,
    build_text_noise_summary,
    find_anonymous_transactions,
    find_bad_debt_candidates,
    find_business_duplicates,
    find_exact_duplicates,
    find_missing_description_rows,
    find_return_candidates,
    find_service_code_rows,
    find_zero_price_rows,
)


def build_reconciliation_table(
    raw_df: pd.DataFrame,
    audited_df: pd.DataFrame,
    fact_tables: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Контрольная сверка строк: raw → deduped → (sales + returns + service)."""
    deduped = audited_df.loc[~audited_df["is_duplicate"]]
    return pd.DataFrame(
        [
            {"stage": "raw_rows", "row_count": int(len(raw_df))},
            {"stage": "deduped_rows", "row_count": int(len(deduped))},
            {"stage": "fact_sales_lines", "row_count": int(len(fact_tables["fact_sales_lines"]))},
            {"stage": "fact_return_lines", "row_count": int(len(fact_tables["fact_return_lines"]))},
            {
                "stage": "fact_service_lines",
                "row_count": int(len(fact_tables["fact_service_lines"])),
            },
            {
                "stage": "processed_total",
                "row_count": int(
                    len(fact_tables["fact_sales_lines"])
                    + len(fact_tables["fact_return_lines"])
                    + len(fact_tables["fact_service_lines"])
                ),
            },
        ]
    )


def build_qa_artifacts(
    raw_df: pd.DataFrame,
    audited_df: pd.DataFrame,
    fact_tables: dict[str, pd.DataFrame],
    cfg: PipelineConfig,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Генерирует 17 QA-таблиц и компактный JSON-summary.

    Args:
        raw_df: сырой DataFrame (до нормализации).
        audited_df: аудированный DataFrame (после classify_line_type).
        fact_tables: словарь из ``build_fact_tables``.
        cfg: конфигурация pipeline.

    Returns:
        Кортеж ``(qa_tables, qa_summary)``:
        - ``qa_tables`` — словарь ``{имя: DataFrame}`` для экспорта в parquet/csv.
        - ``qa_summary`` — компактный dict для JSON-отчёта (строки по слоям,
          распределение по line_type, число строк в каждом факте).
    """
    qa_tables = {
        "raw_profile": build_basic_profile(raw_df),
        "raw_missingness": build_missingness_profile(raw_df),
        "duplicate_rows": find_exact_duplicates(raw_df),
        "business_duplicate_rows": find_business_duplicates(raw_df, cfg),
        "return_candidates": find_return_candidates(raw_df),
        "bad_debt_candidates": find_bad_debt_candidates(raw_df),
        "service_code_candidates": find_service_code_rows(raw_df, cfg),
        "zero_price_rows": find_zero_price_rows(raw_df),
        "anonymous_transactions": find_anonymous_transactions(raw_df),
        "missing_description_rows": find_missing_description_rows(raw_df),
        "stock_description_issues": build_stock_description_issues(audited_df),
        "text_noise_summary": build_text_noise_summary(raw_df),
        "country_normalization": build_country_mapping_table(audited_df),
        "extreme_rows": build_extreme_rows(raw_df),
        "last_month_summary": build_last_month_summary(raw_df),
        "line_type_summary": build_line_type_summary(audited_df),
        "raw_processed_reconciliation": build_reconciliation_table(raw_df, audited_df, fact_tables),
    }

    summary = {
        "raw_row_count": int(len(raw_df)),
        "deduped_row_count": int((~audited_df["is_duplicate"]).sum()),
        "line_type_counts": {
            str(key): int(value)
            for key, value in audited_df["line_type"].value_counts(dropna=False).to_dict().items()
        },
        "fact_row_counts": {name: int(len(frame)) for name, frame in fact_tables.items()},
    }
    return qa_tables, summary
