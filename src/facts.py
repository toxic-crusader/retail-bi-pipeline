"""Построение факт-таблиц: продажи, возвраты, сервисные строки.

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


def _prepare_fact_frame(df: pd.DataFrame, dim_country: pd.DataFrame) -> pd.DataFrame:
    """Выбирает нужные колонки и переименовывает их в BI-формат.

    Добавляет ``is_uk``, ``year_month`` и ``region`` как raw-поля для
    кросс-фильтрации DataLens (алиасы требуют физические поля на фактах,
    а не UI-группировки).
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

    # year_month формируется ДО приведения даты к .date
    fact["year_month"] = fact["invoice_date"].dt.strftime("%Y-%m")
    # Дата без времени — в источнике время всегда 00:00:00
    fact["invoice_date"] = fact["invoice_date"].dt.date

    fact["is_uk"] = fact["country_norm"].eq("United Kingdom")

    # Округление денежных полей — убираем float-хвосты для BI
    fact["unit_price"] = fact["unit_price"].round(2)
    fact["line_amount"] = fact["line_amount"].round(2)

    # Единое имя FK для связи с dim_country.country_name
    fact = fact.rename(columns={"country_norm": "country_name"})

    # region через lookup из dim_country с integrity-check
    country_to_region = dict(zip(dim_country["country_name"], dim_country["region"]))
    missing = set(fact["country_name"].dropna().unique()) - set(country_to_region)
    if missing:
        raise ValueError(
            f"country_name values missing in dim_country: {sorted(missing)}"
        )
    fact["region"] = fact["country_name"].map(country_to_region)

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
    inv_agg["invoice_total"] = inv_agg["invoice_total"].round(2)
    return fact.merge(inv_agg, on="invoice_id", how="left")


def build_fact_tables(df: pd.DataFrame, dim_country: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Строит три факт-таблицы из аудированного слоя.

    Перед разделением снимаются полные дубликаты (``is_duplicate``).
    Строки распределяются по ``line_type``:

    - ``fact_sales_lines`` — line_type = ``sale``.
    - ``fact_return_lines`` — line_type = ``return``.
    - ``fact_service_lines`` — всё остальное (shipping, discount, bad_debt и др.).

    Args:
        df: аудированный DataFrame с колонками ``is_duplicate`` и ``line_type``.

    Returns:
        Словарь ``{имя_таблицы: DataFrame}`` с тремя фактами.
        Каждый факт содержит 17 колонок (включая ``is_uk``,
        ``invoice_total``, ``invoice_item_count``).
    """
    deduped = df.loc[~df["is_duplicate"]].copy()
    fact = _prepare_fact_frame(deduped, dim_country)

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


def build_daily_summary(
    fact_tables: dict[str, pd.DataFrame],
    dim_product: pd.DataFrame,
    dim_country: pd.DataFrame,
) -> pd.DataFrame:
    """Строит агрегированную сводку ``fact_daily_summary``.

    Гранулярность: ``invoice_date × country_name × channel × product_category``.
    Объединяет продажи, возвраты и сервисные операции в одной таблице,
    чтобы в DataLens можно было считать кросс-метрики (доля возвратов,
    net revenue и пр.) без мульти-датасетных формул.
    """
    group_keys = ["invoice_date", "country_name", "channel", "product_category"]

    sales = fact_tables["fact_sales_lines"].copy()
    returns = fact_tables["fact_return_lines"].copy()
    service = fact_tables["fact_service_lines"].copy()

    # Подтягиваем product_category из dim_product
    cat_map = dim_product[["stock_code_norm", "product_category"]].drop_duplicates()
    sales = sales.merge(cat_map, on="stock_code_norm", how="left")
    returns = returns.merge(cat_map, on="stock_code_norm", how="left")
    service = service.merge(cat_map, on="stock_code_norm", how="left")

    sales["product_category"] = sales["product_category"].fillna("Other")
    returns["product_category"] = returns["product_category"].fillna("Other")
    service["product_category"] = service["product_category"].fillna("Other")

    # Агрегация продаж
    agg_sales = (
        sales.groupby(group_keys, dropna=False)
        .agg(
            sales_amount=("line_amount", "sum"),
            sales_orders=("invoice_id", "nunique"),
            sales_customers=("customer_id", "nunique"),
            sales_quantity=("quantity", "sum"),
        )
        .reset_index()
    )

    # Агрегация возвратов
    agg_returns = (
        returns.groupby(group_keys, dropna=False)
        .agg(
            returns_amount_abs=("line_amount", "sum"),
            returns_orders=("invoice_id", "nunique"),
        )
        .reset_index()
    )
    agg_returns["returns_amount_abs"] = -agg_returns["returns_amount_abs"]

    # Агрегация сервисных: доставка и скидки отдельно
    shipping = service.loc[service["line_type"] == "shipping"]
    discounts = service.loc[service["line_type"] == "discount"]

    agg_shipping = (
        shipping.groupby(group_keys, dropna=False)
        .agg(shipping_amount=("line_amount", "sum"))
        .reset_index()
    )

    agg_discounts = (
        discounts.groupby(group_keys, dropna=False)
        .agg(discounts_amount_abs=("line_amount", "sum"))
        .reset_index()
    )
    agg_discounts["discounts_amount_abs"] = -agg_discounts["discounts_amount_abs"]

    # Агрегация отменённых продаж (same-day cancellations)
    cancelled = service.loc[
        (service["line_type"] == "cancelled_sale") & (service["line_amount"] > 0)
    ]

    agg_cancelled = (
        cancelled.groupby(group_keys, dropna=False)
        .agg(
            cancelled_sales_amount=("line_amount", "sum"),
            cancelled_sales_orders=("invoice_id", "nunique"),
        )
        .reset_index()
    )

    # Сборка: FULL OUTER MERGE — все факты на равных
    summary = agg_sales.merge(agg_returns, on=group_keys, how="outer")
    summary = summary.merge(agg_shipping, on=group_keys, how="outer")
    summary = summary.merge(agg_discounts, on=group_keys, how="outer")
    summary = summary.merge(agg_cancelled, on=group_keys, how="outer")

    # Заполняем пропуски нулями (срез мог быть только в одном из фактов)
    fill_cols = [
        "sales_amount", "sales_orders", "sales_customers", "sales_quantity",
        "returns_amount_abs", "returns_orders",
        "shipping_amount", "discounts_amount_abs",
        "cancelled_sales_amount", "cancelled_sales_orders",
    ]
    summary[fill_cols] = summary[fill_cols].fillna(0)

    # Типизация
    for col in [
        "sales_amount", "returns_amount_abs",
        "shipping_amount", "discounts_amount_abs",
        "cancelled_sales_amount",
    ]:
        summary[col] = summary[col].round(2)

    for col in [
        "sales_orders", "sales_customers", "sales_quantity", "returns_orders",
        "cancelled_sales_orders",
    ]:
        summary[col] = summary[col].astype(int)

    # Raw-поля для кросс-фильтрации DataLens
    summary["year_month"] = pd.to_datetime(summary["invoice_date"]).dt.strftime("%Y-%m")

    country_to_region = dict(zip(dim_country["country_name"], dim_country["region"]))
    missing = set(summary["country_name"].dropna().unique()) - set(country_to_region)
    if missing:
        raise ValueError(
            f"country_name values missing in dim_country: {sorted(missing)}"
        )
    summary["region"] = summary["country_name"].map(country_to_region)
    summary["is_uk"] = summary["country_name"].eq("United Kingdom")

    return summary.sort_values(group_keys).reset_index(drop=True)
