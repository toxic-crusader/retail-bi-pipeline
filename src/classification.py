# File: src/classification.py
from __future__ import annotations

import pandas as pd

from .config import PipelineConfig


def apply_business_flags(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Вычисляет набор базовых бизнес-флагов для дальнейшей классификации.

    На этом этапе помечаются дубликаты, анонимные строки, кандидаты в
    возвраты, bad debt, доставку, скидки, ручные корректировки, комиссии,
    подарочные сертификаты и тестовые записи.
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
    """Присваивает каждой строке итоговый класс `line_type`.

    Классификация выполняется по приоритетным правилам: сначала bad debt
    и сервисные строки, затем возвраты, и только после этого обычные
    товарные продажи. В результате формируются `line_type` и агрегирующий
    флаг `is_service_line`.
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
