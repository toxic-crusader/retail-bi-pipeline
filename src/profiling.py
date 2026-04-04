from __future__ import annotations

from typing import Any

import pandas as pd

from .config import PipelineConfig


def build_basic_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Строит базовый профиль исходного набора данных.

    В профиль входят размер таблицы, число уникальных инвойсов и клиентов,
    диапазон дат и проверка временной детализации `InvoiceDate`.
    """
    metrics: list[dict[str, Any]] = [
        {"metric": "row_count", "value": int(len(df))},
        {"metric": "column_count", "value": int(df.shape[1])},
        {"metric": "invoice_unique", "value": int(df["Invoice"].nunique(dropna=True))},
        {
            "metric": "customer_unique_non_null",
            "value": int(df["Customer ID"].nunique(dropna=True)),
        },
        {"metric": "country_unique", "value": int(df["Country"].nunique(dropna=True))},
        {"metric": "date_min", "value": str(df["InvoiceDate"].min())},
        {"metric": "date_max", "value": str(df["InvoiceDate"].max())},
        {
            "metric": "invoice_time_unique",
            "value": int(df["InvoiceDate"].dt.time.nunique()),
        },
    ]
    return pd.DataFrame(metrics)


def build_missingness_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Считает пропуски по всем колонкам и возвращает итоговую сводку."""
    missing = df.isna().sum()
    return (
        pd.DataFrame(
            {
                "column_name": missing.index,
                "missing_count": missing.values,
                "missing_pct": (missing.values / len(df)).round(6),
            }
        )
        .sort_values(["missing_count", "column_name"], ascending=[False, True])
        .reset_index(drop=True)
    )


def find_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Возвращает все строки, участвующие в полных дубликатах."""
    return df.loc[df.duplicated(keep=False)].copy()


def find_business_duplicates(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Возвращает строки-дубликаты по заданному бизнес-ключу."""
    return df.loc[df.duplicated(subset=list(cfg.business_key_columns), keep=False)].copy()


def find_return_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """Находит кандидатов в возвраты по комбинированному правилу."""
    invoice = df["Invoice"].astype("string").fillna("").str.upper()
    mask = (df["Quantity"] < 0) | invoice.str.startswith("C")
    return df.loc[mask].copy()


def find_bad_debt_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """Находит строки bad debt и бухгалтерских корректировок."""
    invoice = df["Invoice"].astype("string").fillna("").str.upper()
    stock = df["StockCode"].astype("string").fillna("").str.upper().str.strip()
    mask = invoice.str.startswith("A") | (df["Price"] < 0) | stock.eq("B")
    return df.loc[mask].copy()


def find_zero_price_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Возвращает все строки с нулевой ценой."""
    return df.loc[df["Price"] == 0].copy()


def find_service_code_rows(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Отбирает строки с явными нетоварными и служебными кодами."""
    stock = df["StockCode"].astype("string").fillna("").str.upper().str.strip()
    service_mask = (
        stock.isin(cfg.shipping_codes)
        | stock.isin(cfg.discount_codes)
        | stock.isin(cfg.manual_adjustment_codes)
        | stock.isin(cfg.commission_codes)
        | stock.eq("B")
        | stock.isin(cfg.test_codes)
        | stock.eq("GIFT")
        | stock.str.startswith(cfg.gift_prefix)
    )
    return df.loc[service_mask].copy()


def find_anonymous_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Возвращает строки без идентификатора клиента."""
    return df.loc[df["Customer ID"].isna()].copy()


def find_missing_description_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Возвращает строки с отсутствующим описанием товара."""
    return df.loc[df["Description"].isna()].copy()


def build_stock_description_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Строит сводку конфликтов между кодами товаров и их описаниями.

    Функция одновременно ищет два класса проблем:
    один код товара с несколькими описаниями и одно описание с несколькими
    кодами товара. Эти конфликты важны для построения `DimProduct`.
    """
    stock_issue = (
        df.groupby("stock_code_norm", dropna=False)["description_norm"]
        .nunique(dropna=True)
        .rename("variant_count")
        .reset_index()
    )
    stock_issue = stock_issue.loc[stock_issue["variant_count"] > 1].copy()
    stock_issue["issue_type"] = "stock_code_to_many_descriptions"
    stock_issue["entity"] = stock_issue["stock_code_norm"]

    desc_issue = (
        df.groupby("description_norm", dropna=False)["stock_code_norm"]
        .nunique(dropna=True)
        .rename("variant_count")
        .reset_index()
    )
    desc_issue = desc_issue.loc[desc_issue["variant_count"] > 1].copy()
    desc_issue["issue_type"] = "description_to_many_stock_codes"
    desc_issue["entity"] = desc_issue["description_norm"]

    combined = pd.concat(
        [
            stock_issue[["issue_type", "entity", "variant_count"]],
            desc_issue[["issue_type", "entity", "variant_count"]],
        ],
        ignore_index=True,
    )
    return combined.sort_values(
        ["issue_type", "variant_count", "entity"],
        ascending=[True, False, True],
    )


def build_text_noise_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Строит summary по ключевым признакам текстового шума в raw-слое."""
    description = df["Description"].astype("string")
    stock_code = df["StockCode"].astype("string")
    return pd.DataFrame(
        [
            {
                "metric": "description_trim_changed",
                "value": int((description != description.str.strip()).sum()),
            },
            {
                "metric": "description_multiple_spaces",
                "value": int(description.str.contains(r"\s{2,}", regex=True, na=False).sum()),
            },
            {
                "metric": "stock_code_not_upper",
                "value": int((stock_code != stock_code.str.upper()).sum()),
            },
        ]
    )


def build_country_mapping_table(df: pd.DataFrame) -> pd.DataFrame:
    """Возвращает таблицу соответствия сырых и нормализованных стран."""
    return (
        df[["Country", "country_norm"]]
        .drop_duplicates()
        .rename(columns={"Country": "country_raw"})
        .sort_values(["country_norm", "country_raw"])
        .reset_index(drop=True)
    )


def build_extreme_rows(df: pd.DataFrame, *, top_n: int = 20) -> pd.DataFrame:
    """Собирает экстремальные строки по абсолютным значениям количества и цены.

    Результат нужен для audit-first анализа: он позволяет быстро увидеть,
    являются ли выбросы ошибкой загрузки, bulk-операцией или корректировкой.
    """
    with_amount = df.copy()
    with_amount["abs_quantity"] = with_amount["Quantity"].abs()
    with_amount["abs_price"] = with_amount["Price"].abs()
    quantity_rows = with_amount.nlargest(top_n, "abs_quantity").assign(extreme_metric="quantity")
    price_rows = with_amount.nlargest(top_n, "abs_price").assign(extreme_metric="price")
    columns = [
        "extreme_metric",
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "Price",
        "Customer ID",
        "Country",
        "Channel",
    ]
    return pd.concat([quantity_rows[columns], price_rows[columns]], ignore_index=True)


def build_last_month_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Проверяет, является ли последний календарный месяц в источнике полным."""
    max_date = df["InvoiceDate"].max()
    month_start = max_date.to_period("M").to_timestamp()
    month_end = max_date.to_period("M").end_time.normalize()
    return pd.DataFrame(
        [
            {
                "max_invoice_date": max_date.normalize(),
                "last_month_start": month_start,
                "calendar_month_end": month_end,
                "is_last_month_complete": bool(max_date.normalize() == month_end),
            }
        ]
    )


def build_line_type_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Строит агрегированную сводку по финальной классификации `line_type`."""
    summary = (
        df.groupby("line_type", dropna=False)
        .agg(
            row_count=("line_type", "size"),
            line_amount_sum=("line_amount", "sum"),
            quantity_sum=("Quantity", "sum"),
        )
        .reset_index()
        .sort_values("row_count", ascending=False)
    )
    return summary
