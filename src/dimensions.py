# File: src/dimensions.py
from __future__ import annotations

import re

import numpy as np
import pandas as pd

from .config import PipelineConfig


def build_product_dimension(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Строит измерение товаров с каноническим названием и категорией для каждого SKU.

    Каноническое имя выбирается по наиболее частому валидному описанию,
    приоритетно среди товарных продаж и возвратов. Категория определяется
    по ключевым словам в каноническом имени.
    """
    candidate_mask = (
        df["description_norm"].notna()
        & ~df["description_norm"].eq("")
        & ~df["is_description_placeholder"]
    )
    sales_candidates = df.loc[candidate_mask & df["line_type"].isin(["sale", "return"])].copy()
    fallback_candidates = df.loc[candidate_mask].copy()

    def most_frequent_name(source: pd.DataFrame) -> pd.Series:
        """Возвращает наиболее частое нормализованное имя для каждого кода товара."""
        if source.empty:
            return pd.Series(dtype="string")
        counts = (
            source.groupby(["stock_code_norm", "description_norm"])
            .size()
            .rename("freq")
            .reset_index()
            .sort_values(
                ["stock_code_norm", "freq", "description_norm"], ascending=[True, False, True]
            )
        )
        return counts.drop_duplicates(subset=["stock_code_norm"]).set_index("stock_code_norm")[
            "description_norm"
        ]

    preferred = most_frequent_name(sales_candidates)
    fallback = most_frequent_name(fallback_candidates)
    canonical = preferred.combine_first(fallback)

    product_dim = (
        df.groupby("stock_code_norm", dropna=False)
        .agg(
            description_variant_count=("description_norm", "nunique"),
            source_row_count=("stock_code_norm", "size"),
            first_seen_date=("InvoiceDate", "min"),
            last_seen_date=("InvoiceDate", "max"),
        )
        .reset_index()
    )
    product_dim["product_name_canonical"] = product_dim["stock_code_norm"].map(canonical)
    product_dim["product_name_canonical"] = product_dim["product_name_canonical"].fillna(
        "UNKNOWN PRODUCT"
    )
    product_dim["is_service_code"] = product_dim["stock_code_norm"].isin(
        cfg.shipping_codes
        | cfg.discount_codes
        | cfg.manual_adjustment_codes
        | cfg.commission_codes
        | frozenset({"B", "GIFT"})
        | cfg.test_codes
    ) | product_dim["stock_code_norm"].fillna("").str.startswith(cfg.gift_prefix)

    product_dim["product_category"] = product_dim["product_name_canonical"].apply(
        lambda name: _classify_product_category(name, cfg.product_category_keywords)
    )
    product_dim.loc[product_dim["is_service_code"], "product_category"] = "Service / Non-product"

    return product_dim.sort_values("stock_code_norm").reset_index(drop=True)


def _classify_product_category(name: str, keywords_map: dict[str, list[str]]) -> str:
    """Определяет категорию товара по ключевым словам в названии."""
    if not name or name == "UNKNOWN PRODUCT":
        return "Other"
    upper = name.upper()
    for category, keywords in keywords_map.items():
        for kw in keywords:
            if re.search(r"\b" + re.escape(kw) + r"\b", upper):
                return category
    return "Other"


def attach_product_names(df: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    """Обогащает рабочий слой каноническими названиями из `DimProduct`."""
    return df.merge(
        dim_product[["stock_code_norm", "product_name_canonical"]],
        on="stock_code_norm",
        how="left",
    )


def build_customer_dimension(
    df: pd.DataFrame,
    cfg: PipelineConfig,
) -> pd.DataFrame:
    """Строит измерение клиентов с метриками выручки и сегментацией.

    Для идентифицированных клиентов агрегируются канал, даты активности,
    число инвойсов, число стран и показатели выручки. Анонимные транзакции
    складываются в отдельную агрегированную запись.
    """
    clean = df.loc[~df["is_duplicate"]].copy()
    sales_mask = clean["line_type"].eq("sale")

    identified = clean.loc[~clean["is_anonymous_customer"]].copy()

    customer_base = (
        identified.groupby("customer_id_norm", dropna=False)
        .agg(
            channel=(
                "channel_norm",
                lambda s: s.mode().iat[0]
                if not s.mode().empty
                else cfg.unknown_channel_label.lower(),
            ),
            first_invoice_date=("InvoiceDate", "min"),
            last_invoice_date=("InvoiceDate", "max"),
            invoice_count=("Invoice", "nunique"),
            country_count=("country_norm", "nunique"),
        )
        .reset_index()
        .rename(columns={"customer_id_norm": "customer_id"})
    )

    identified_sales = clean.loc[~clean["is_anonymous_customer"] & sales_mask]
    revenue_agg = (
        identified_sales.groupby("customer_id_norm", dropna=False)
        .agg(
            total_revenue=("line_amount", "sum"),
            total_quantity=("Quantity", "sum"),
            order_count=("Invoice", "nunique"),
        )
        .reset_index()
        .rename(columns={"customer_id_norm": "customer_id"})
    )
    revenue_agg["avg_order_value"] = (
        revenue_agg["total_revenue"] / revenue_agg["order_count"].replace(0, np.nan)
    ).round(2)

    customer_dim = customer_base.merge(revenue_agg, on="customer_id", how="left")
    for col in ["total_revenue", "total_quantity", "avg_order_value"]:
        customer_dim[col] = customer_dim[col].fillna(0.0)
    customer_dim["order_count"] = customer_dim["order_count"].fillna(0).astype("Int64")
    customer_dim["is_anonymous_customer"] = False

    customer_dim["customer_tier"] = _assign_customer_tier(customer_dim["total_revenue"])

    anon_sales = clean.loc[clean["is_anonymous_customer"] & sales_mask]
    anonymous = pd.DataFrame(
        [
            {
                "customer_id": cfg.anonymous_customer_label,
                "channel": cfg.unknown_channel_label.lower(),
                "first_invoice_date": clean.loc[
                    clean["is_anonymous_customer"], "InvoiceDate"
                ].min(),
                "last_invoice_date": clean.loc[
                    clean["is_anonymous_customer"], "InvoiceDate"
                ].max(),
                "invoice_count": int(
                    clean.loc[clean["is_anonymous_customer"], "Invoice"].nunique()
                ),
                "country_count": int(
                    clean.loc[clean["is_anonymous_customer"], "country_norm"].nunique()
                ),
                "total_revenue": float(anon_sales["line_amount"].sum()) if len(anon_sales) else 0.0,
                "total_quantity": int(anon_sales["Quantity"].sum()) if len(anon_sales) else 0,
                "order_count": int(anon_sales["Invoice"].nunique()) if len(anon_sales) else 0,
                "avg_order_value": round(
                    float(anon_sales["line_amount"].sum())
                    / max(int(anon_sales["Invoice"].nunique()), 1),
                    2,
                )
                if len(anon_sales)
                else 0.0,
                "is_anonymous_customer": True,
                "customer_tier": "Anonymous",
            }
        ]
    )
    return pd.concat([customer_dim, anonymous], ignore_index=True)


def _assign_customer_tier(revenue: pd.Series) -> pd.Series:
    """Сегментирует клиентов на Top / Medium / Low по квартилям выручки."""
    tier = pd.Series("Low", index=revenue.index, dtype="string")
    q75 = revenue.quantile(0.75)
    q25 = revenue.quantile(0.25)
    tier[revenue >= q75] = "Top"
    tier[(revenue >= q25) & (revenue < q75)] = "Medium"
    return tier


def build_date_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """Строит календарное измерение на дневом уровне по диапазону источника."""
    calendar = pd.DataFrame(
        {"date": pd.date_range(df["InvoiceDate"].min(), df["InvoiceDate"].max(), freq="D")}
    )
    calendar["year"] = calendar["date"].dt.year
    calendar["quarter"] = calendar["date"].dt.quarter
    calendar["month"] = calendar["date"].dt.month
    calendar["month_name"] = calendar["date"].dt.month_name()
    calendar["year_month"] = calendar["date"].dt.to_period("M").astype("string")
    calendar["month_start"] = calendar["date"].dt.to_period("M").dt.to_timestamp()
    calendar["is_month_end"] = calendar["date"].dt.is_month_end
    max_date = df["InvoiceDate"].max().normalize()
    last_month_end = max_date.to_period("M").end_time.normalize()
    calendar["is_last_incomplete_month"] = calendar["year_month"].eq(
        str(max_date.to_period("M"))
    ) & (max_date != last_month_end)
    return calendar


def build_country_dimension(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Строит справочник стран с региональной группировкой."""
    country_dim = (
        df[["country_norm"]]
        .drop_duplicates()
        .rename(columns={"country_norm": "country_name"})
        .sort_values("country_name")
        .reset_index(drop=True)
    )
    region_map = cfg.country_region_map
    country_dim["region"] = country_dim["country_name"].map(region_map).fillna("Other")
    country_dim["is_uk"] = country_dim["country_name"].eq("United Kingdom")
    return country_dim
