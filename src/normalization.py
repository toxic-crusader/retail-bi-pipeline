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
    """Выполняет базовую текстовую очистку строковой серии.

    Нормализация включает приведение к pandas string dtype, схлопывание
    повторяющихся пробелов и обрезку пробелов по краям. Это общий helper
    для кодов, описаний, стран и каналов.
    """
    as_string = series.astype("string")
    as_string = as_string.str.replace(MULTISPACE_RE, " ", regex=True)
    as_string = as_string.str.strip()
    return as_string


def normalize_stock_code(series: pd.Series) -> pd.Series:
    """Нормализует коды SKU до верхнего регистра и чистого строкового вида."""
    return _normalize_string(series).str.upper()


def normalize_description(series: pd.Series) -> pd.Series:
    """Нормализует описания товаров для сопоставления и канонизации."""
    return _normalize_string(series).str.upper()


def normalize_country(series: pd.Series, cfg: PipelineConfig) -> pd.Series:
    """Нормализует названия стран с применением словаря канонизации.

    Сначала выполняется базовая текстовая очистка, затем специальные
    значения вроде `EIRE` и `RSA` переводятся в BI-совместимый формат.
    """
    normalized = _normalize_string(series)
    upper = normalized.str.upper()
    mapped = upper.map(cfg.country_map)
    return mapped.fillna(normalized)


def normalize_channel(series: pd.Series, cfg: PipelineConfig) -> pd.Series:
    """Нормализует канал и заполняет пропуски канонической меткой unknown."""
    normalized = _normalize_string(series).str.lower()
    return normalized.fillna(cfg.unknown_channel_label.lower())


def normalize_customer_id(series: pd.Series, cfg: PipelineConfig) -> pd.Series:
    """Преобразует идентификатор клиента в строковый ключ измерения.

    Заполненные значения приводятся к целому строковому представлению,
    а пропуски переводятся в специальную метку анонимного покупателя.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series(cfg.anonymous_customer_label, index=series.index, dtype="string")
    present = numeric.notna()
    out.loc[present] = numeric.loc[present].astype("Int64").astype("string")
    return out


def mark_description_placeholder(series: pd.Series) -> pd.Series:
    """Помечает описания, которые выглядят как технические подписи или мусор.

    Используется для отделения нормальных товарных наименований от
    служебных пометок вроде `DAMAGED`, `FOUND`, `CHECK` и подобных.
    """
    desc = normalize_description(series)
    return desc.isna() | desc.eq("") | desc.str.fullmatch(PLACEHOLDER_DESC_RE)


def apply_normalization(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    """Применяет полный набор нормализаций к сырому DataFrame.

    Функция создаёт рабочий слой с вычисляемыми полями: нормализованные
    коды и описания, нормализованную географию, канал, клиентский ключ,
    сумму строки и флаг технического описания товара.
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
