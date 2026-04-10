"""Классификация строк по типу операции (``line_type``).

На основе бизнес-флагов (дубликат, возврат, bad debt, сервисный код и др.)
каждая строка получает один из десяти классов. Приоритет правил зафиксирован:
сначала отсекаются бухгалтерские и служебные строки, затем возвраты,
и только оставшиеся с положительным количеством и ценой считаются продажами.
"""

from __future__ import annotations

import pandas as pd

from .config import PipelineConfig


def apply_business_flags(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Вычисляет набор bool-флагов для последующей классификации.

    Args:
        df: нормализованный DataFrame (после ``apply_normalization``).
        cfg: конфигурация с наборами служебных кодов.

    Returns:
        Копия DataFrame с добавленными колонками: ``is_duplicate``,
        ``is_business_duplicate``, ``is_anonymous_customer``,
        ``is_return_candidate``, ``is_bad_debt``, ``is_shipping``,
        ``is_discount``, ``is_manual_adjustment``, ``is_commission_fee``,
        ``is_gift_voucher``, ``is_test``.
    """
    out = df.copy()
    invoice = out["Invoice"].astype("string").fillna("").str.strip().str.upper()
    stock = out["stock_code_norm"].fillna("")
    exact_duplicate_columns = [
        column
        for column in [
            "Invoice",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "Price",
            "Customer ID",
            "Country",
            "Channel",
            "rnd",
        ]
        if column in out.columns
    ]

    out["is_duplicate"] = out.duplicated(subset=exact_duplicate_columns, keep="first")
    out["is_business_duplicate"] = out.duplicated(
        subset=list(cfg.business_key_columns),
        keep="first",
    )
    out["is_anonymous_customer"] = out["Customer ID"].isna()
    out["is_return_candidate"] = (out["Quantity"] < 0) | invoice.str.startswith("C")
    out["is_bad_debt"] = invoice.str.startswith("A") | (out["Price"] < 0) | stock.eq("B")
    out["is_shipping"] = stock.isin(cfg.shipping_codes)
    out["is_discount"] = stock.isin(cfg.discount_codes)
    out["is_manual_adjustment"] = stock.isin(cfg.manual_adjustment_codes)
    out["is_commission_fee"] = stock.isin(cfg.commission_codes)
    out["is_gift_voucher"] = stock.str.startswith(cfg.gift_prefix) | stock.eq("GIFT")
    out["is_test"] = stock.isin(cfg.test_codes)
    return out


def classify_line_type(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Присваивает каждой строке итоговый класс ``line_type``.

    Порядок приоритета (первый сработавший побеждает):
    bad_debt → shipping → discount → commission_fee → manual_adjustment →
    gift_voucher → test → return → sale → unknown.

    Args:
        df: нормализованный DataFrame (после ``apply_normalization``).
        cfg: конфигурация pipeline.

    Returns:
        DataFrame с добавленными колонками ``line_type`` (string)
        и ``is_service_line`` (bool — True для всего, кроме sale/return).
    """
    out = apply_business_flags(df, cfg)
    line_type = pd.Series("unknown", index=out.index, dtype="string")

    line_type = line_type.mask(out["is_bad_debt"], "bad_debt")
    line_type = line_type.mask(line_type.eq("unknown") & out["is_shipping"], "shipping")
    line_type = line_type.mask(line_type.eq("unknown") & out["is_discount"], "discount")
    line_type = line_type.mask(
        line_type.eq("unknown") & out["is_commission_fee"],
        "commission_fee",
    )
    line_type = line_type.mask(
        line_type.eq("unknown") & out["is_manual_adjustment"],
        "manual_adjustment",
    )
    line_type = line_type.mask(
        line_type.eq("unknown") & out["is_gift_voucher"],
        "gift_voucher",
    )
    line_type = line_type.mask(line_type.eq("unknown") & out["is_test"], "test")
    line_type = line_type.mask(
        line_type.eq("unknown") & out["is_return_candidate"],
        "return",
    )
    sale_mask = (
        line_type.eq("unknown")
        & (out["Quantity"] > 0)
        & (out["Price"] > 0)
        & out["stock_code_norm"].notna()
    )
    line_type = line_type.mask(sale_mask, "sale")

    out["line_type"] = line_type
    out["is_service_line"] = ~out["line_type"].isin({"sale", "return"})
    return out


def reclassify_same_day_cancellations(df: pd.DataFrame) -> pd.DataFrame:
    """Переклассифицирует same-day cancellation pairs как ``cancelled_sale``.

    Находит пары строк sale + return в одном и том же дне, у которых
    совпадают customer_id, stock_code, абсолютное количество и абсолютная
    сумма. Такие пары — это не реальные продажи и возвраты, а отменённые
    в день оформления заказы. Обе строки переклассифицируются в
    ``cancelled_sale``, что выводит их из ``fact_sales_lines`` и
    ``fact_return_lines`` в ``fact_service_lines``.

    Если для ключа количество sale-строк не совпадает с количеством
    return-строк, переклассификация НЕ выполняется (это частичная
    отмена или другой случай, не same-day cancel).

    Args:
        df: аудированный DataFrame после ``classify_line_type`` и
            дедупликации, с колонками ``line_type``, ``customer_id_norm``,
            ``is_anonymous_customer``, ``InvoiceDate``, ``stock_code_norm``,
            ``Quantity``, ``line_amount``, ``is_service_line``.

    Note:
        Анонимные транзакции (``is_anonymous_customer == True``)
        исключены из поиска пар. Причина: у всех анонимов
        ``customer_id_norm == "ANONYMOUS"``, и при группировке они
        объединятся в одного "супер-клиента", что даст false positives
        (два разных покупателя без ID, один купил, другой вернул
        тот же товар в тот же день — это не same-day cancellation).

    Returns:
        Копия DataFrame с обновлённым ``line_type`` для пар
        same-day cancellations. Колонка ``is_service_line`` также
        пересчитывается (True для ``cancelled_sale``).
    """
    import numpy as np

    out = df.copy()

    sale_mask = out["line_type"].eq("sale") & ~out["is_anonymous_customer"]
    return_mask = out["line_type"].eq("return") & ~out["is_anonymous_customer"]

    sales = out.loc[sale_mask].copy()
    sales["_orig_idx"] = sales.index

    returns = out.loc[return_mask].copy()
    returns["_orig_idx"] = returns.index

    sales["_match_date"] = sales["InvoiceDate"].dt.date
    returns["_match_date"] = returns["InvoiceDate"].dt.date
    sales["_abs_qty"] = sales["Quantity"].abs()
    returns["_abs_qty"] = returns["Quantity"].abs()
    sales["_abs_amt"] = sales["line_amount"].abs().round(2)
    returns["_abs_amt"] = returns["line_amount"].abs().round(2)

    key_cols = [
        "customer_id_norm",
        "_match_date",
        "stock_code_norm",
        "_abs_qty",
        "_abs_amt",
    ]

    sales_count = (
        sales.groupby(key_cols, dropna=False)
        .size()
        .rename("n_sales")
        .reset_index()
    )
    returns_count = (
        returns.groupby(key_cols, dropna=False)
        .size()
        .rename("n_returns")
        .reset_index()
    )

    paired = sales_count.merge(returns_count, on=key_cols, how="inner")
    paired = paired.loc[paired["n_sales"] == paired["n_returns"]]

    if paired.empty:
        return out

    to_mark_sales = sales.merge(paired[key_cols], on=key_cols, how="inner")
    to_mark_returns = returns.merge(paired[key_cols], on=key_cols, how="inner")

    sales_idx = to_mark_sales["_orig_idx"].to_numpy()
    returns_idx = to_mark_returns["_orig_idx"].to_numpy()

    out.loc[sales_idx, "line_type"] = "cancelled_sale"
    out.loc[returns_idx, "line_type"] = "cancelled_sale"

    all_idx = np.concatenate([sales_idx, returns_idx])
    out.loc[all_idx, "is_service_line"] = True

    return out
