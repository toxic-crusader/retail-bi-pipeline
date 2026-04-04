"""Построение факт-таблиц: продажи, в��звраты, сервисные строки.

Модуль разделяет аудированный слой на три факта по ``line_type``,
переименовывает колонки в BI-совместимый формат и добавляет
предрассчитанные агрегаты на уровне инвойса.
"""

from __future__ import annotations

import pandas as pd

FACT_COLUMNS = [
    "Invoice",
    "InvoiceDate",
    "customer_id_norm",
    "channel_norm",
    "country_norm",
    "stock_code_norm",
    "description_norm",
    "product_name_canonical",
    "Quantity",
    "Price",
    "line_amount",
    "line_type",
    "is_service_line",
    "is_anonymous_customer",
]


def _prepare_fact_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Выбирает нужные колонки и переименовывает их в BI-формат.

    Добавляет ``is_uk`` как быстрый фильтр UK vs International.
    """
    fact = df[FACT_COLUMNS].copy()
    fact = fact.rename(
        columns={
            "Invoice": "invoice_id",
            "InvoiceDate": "invoice_date",
            "customer_id_norm": "customer_id",
            "channel_norm": "channel",
            "Quantity": "quantity",
            "Price": "unit_price",
        }
    )

    # Дата без времени — в источнике время всегда 00:00:00
    fact["invoice_date"] = fact["invoice_date"].dt.date

    fact["is_uk"] = fact["country_norm"].eq("United Kingdom")

    # Единое имя FK для связи с dim_country.country_name
    fact = fact.rename(columns={"country_norm": "country_name"})

    return fact


def _add_invoice_aggregates(fact: pd.DataFrame) -> pd.DataFrame:
    """Добавляет ``invoice_total`` и ``invoice_item_count`` через группировку.

    Эти метрики нужны для анализа среднего чека в DataLens без
    LOD-выражений — значения предрассчитаны на каждой строке.
    """
    inv_agg = (
        fact.groupby("invoice_id", dropna=False)
        .agg(
            invoice_total=("line_amount", "sum"),
            invoice_item_count=("invoice_id", "size"),
        )
    )
    return fact.merge(inv_agg, on="invoice_id", how="left")


def build_fact_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Строит три факт-таблицы из аудир��ванного слоя.

    Перед разделением снимаются полные дубликаты (``is_duplicate``).
    Строки распределяются по ``line_type``:

    - ``fact_sales_lines`` — line_type = ``sale``.
    - ``fact_return_lines`` — line_type = ``return``.
    - ``fact_service_lines`` — всё остальное (shipping, discount, bad_debt и др.).

    Args:
        df: ��удированный DataFrame с колонками ``is_duplicate`` и ``line_type``.

    Returns:
        Словарь ``{имя_таблицы: DataFrame}`` с тремя фактами.
        Каждый факт содержит 17 колонок (включая ``is_uk``,
        ``invoice_total``, ``invoice_item_count``).
    """
    deduped = df.loc[~df["is_duplicate"]].copy()
    fact = _prepare_fact_frame(deduped)

    fact_sales = fact.loc[fact["line_type"].eq("sale")].reset_index(drop=True)
    fact_returns = fact.loc[fact["line_type"].eq("return")].reset_index(drop=True)
    fact_service = fact.loc[~fact["line_type"].isin({"sale", "return"})].reset_index(drop=True)

    fact_sales = _add_invoice_aggregates(fact_sales)
    fact_returns = _add_invoice_aggregates(fact_returns)
    fact_service = _add_invoice_aggregates(fact_service)

    return {
        "fact_sales_lines": fact_sales,
        "fact_return_lines": fact_returns,
        "fact_service_lines": fact_service,
    }
