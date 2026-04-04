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
