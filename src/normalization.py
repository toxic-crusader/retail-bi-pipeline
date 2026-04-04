"""Нормализация сырых данных: текст, коды, география, клиентские идентификаторы.

Модуль превращает «грязный» raw-слой в рабочий DataFrame с вычисляемыми
полями (``stock_code_norm``, ``country_norm``, ``line_amount`` и др.),
на которых строятся классификация и измерения.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from .config import PipelineConfig

MULTISPACE_RE = re.compile(r"\s+")
PLACEHOLDER_DESC_RE = re.compile(
    r"^(?:\?|FOUND|MISSING|DAMAGED|DAMAGES|FAULTY|SMASHED|AMENDMENT|ADJUSTMENT|"
    r"UPDATE|CHECK|CHECKED|SHORT|LOST\??|WET|MIA|BROKEN|THROWN AWAY|AMAZON|"
    r"WEBSITE FIXED|TEMP ADJUSTMENT|DOTCOM|SOLD AS SET ON DOTCOM)$"
)


def _normalize_string(series: pd.Series) -> pd.Series:
    """Схлопывает повторяющиеся пробелы и обрезает края (общий helper)."""
    as_string = series.astype("string")
    as_string = as_string.str.replace(MULTISPACE_RE, " ", regex=True)
    as_string = as_string.str.strip()
    return as_string


def normalize_stock_code(series: pd.Series) -> pd.Series:
    """Приводит коды SKU к верхнему регистру с очисткой пробелов."""
    return _normalize_string(series).str.upper()


def normalize_description(series: pd.Series) -> pd.Series:
    """Приводит описания товаров к верхнему регистру с очисткой пробелов."""
    return _normalize_string(series).str.upper()


def normalize_country(series: pd.Series, cfg: PipelineConfig) -> pd.Series:
    """Нормализует названия стран через словарь ``cfg.country_map``.

    Сначала выполняется trim + upper для сопоставления с ключами словаря,
    затем нестандартные значения (``EIRE``, ``RSA``, ``USA``) заменяются
    каноническими. Страны, не попавшие в словарь, сохраняются как есть.
    """
    normalized = _normalize_string(series)
    upper = normalized.str.upper()
    mapped = upper.map(cfg.country_map)
    return mapped.fillna(normalized)


def normalize_channel(series: pd.Series, cfg: PipelineConfig) -> pd.Series:
    """Приводит канал к нижнему регистру; пропуски → ``unknown``."""
    normalized = _normalize_string(series).str.lower()
    return normalized.fillna(cfg.unknown_channel_label.lower())


def normalize_customer_id(series: pd.Series, cfg: PipelineConfig) -> pd.Series:
    """Преобразует числовой ``Customer ID`` в строковый ключ для dim_customer.

    Заполненные значения становятся строками вида ``"12345"``,
    пропуски — меткой ``ANONYMOUS``.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series(cfg.anonymous_customer_label, index=series.index, dtype="string")
    present = numeric.notna()
    out.loc[present] = numeric.loc[present].astype("Int64").astype("string")
    return out


def mark_description_placeholder(series: pd.Series) -> pd.Series:
    """Помечает описания-«заглушки»: пустые, мусорные и служебные пометки.

    К «заглушкам» относятся NA, пустые строки и совпадения с regex
    (``DAMAGED``, ``FOUND``, ``CHECK``, ``MISSING`` и т.д.) — это не
    названия товаров, а комментарии оператора.
    """
    desc = normalize_description(series)
    return desc.isna() | desc.eq("") | desc.str.fullmatch(PLACEHOLDER_DESC_RE)


def apply_normalization(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Создаёт рабочий слой с нормализованными полями поверх сырого DataFrame.

    Args:
        df: сырой DataFrame после ``load_retail_data`` (10 исходных колонок).
        cfg: конфигурация с правилами нормализации.

    Returns:
        Копия DataFrame с дополнительными колонками:
        ``raw_record_id``, ``stock_code_norm``, ``description_norm``,
        ``country_norm``, ``channel_norm``, ``customer_id_norm``,
        ``line_amount``, ``is_description_placeholder``.
    """
    out = df.copy()
    out["raw_record_id"] = np.arange(1, len(out) + 1)
    out["stock_code_norm"] = normalize_stock_code(out["StockCode"])
    out["description_norm"] = normalize_description(out["Description"])
    out["country_norm"] = normalize_country(out["Country"], cfg)
    out["channel_norm"] = normalize_channel(out["Channel"], cfg)
    out["customer_id_norm"] = normalize_customer_id(out["Customer ID"], cfg)
    out["line_amount"] = out["Quantity"].astype(float) * out["Price"]
    out["is_description_placeholder"] = mark_description_placeholder(out["Description"])
    return out
