# File: src/facts.py
from __future__ import annotations

import pandas as pd

FACT_COLUMNS = [
    "raw_record_id",
    "Invoice",
    "InvoiceDate",
    "customer_id_norm",
    "channel_norm",
    "Country",
    "country_norm",
    "StockCode",
    "stock_code_norm",
    "Description",
    "description_norm",
    "product_name_canonical",
    "Quantity",
    "Price",
    "line_amount",
    "line_type",
    "is_duplicate",
    "is_business_duplicate",
    "is_service_line",
    "is_anonymous_customer",
]


def _prepare_fact_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит рабочий слой к единой схеме факт-таблиц.

    На этом этапе выбираются только нужные столбцы и выполняется
    переименование в более читаемый BI-совместимый формат.
    """
    fact = df[FACT_COLUMNS].copy()
    fact = fact.rename(
        columns={
            "Invoice": "invoice_id",
            "InvoiceDate": "invoice_date",
            "customer_id_norm": "customer_id",
            "channel_norm": "channel",
            "Country": "country_raw",
            "StockCode": "stock_code_raw",
            "Description": "description_raw",
            "Quantity": "quantity",
            "Price": "unit_price",
        }
    )

    fact["is_uk"] = fact["country_norm"].eq("United Kingdom")

    return fact


def _add_invoice_aggregates(fact: pd.DataFrame) -> pd.DataFrame:
    """Добавляет предрассчитанные агрегаты на уровне инвойса."""
    inv_agg = (
        fact.groupby("invoice_id", dropna=False)
        .agg(
            invoice_total=("line_amount", "sum"),
            invoice_item_count=("invoice_id", "size"),
        )
    )
    return fact.merge(inv_agg, on="invoice_id", how="left")


def build_fact_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Строит три итоговые факт-таблицы: продажи, возвраты и сервисные строки.

    Перед разделением удаляются полные дубликаты, затем строки
    распределяются по фактам в соответствии с вычисленным `line_type`.
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
