# Retail BI Audit-First Pipeline

Проект подготавливает `Retail.xlsx` к BI-отчёту в Yandex DataLens. Логика построена как audit-first pipeline: сначала профиль и поиск аномалий, затем нормализация, классификация строк, построение витрин и QA-артефактов.

## Что делает проект

- читает `Retail.xlsx`;
- выполняет профилирование и фиксирует ключевые аномалии;
- нормализует `StockCode`, `Description`, `Country`, `Channel`, `Customer ID`;
- классифицирует строки в `line_type`;
- строит витрины:
  - `fact_sales_lines`
  - `fact_return_lines`
  - `fact_service_lines`
  - `dim_product`
  - `dim_customer`
  - `dim_date`
  - `dim_country`
- сохраняет QA-таблицы и итоговую сводку запуска;
- предоставляет notebook `retail_audit_and_pipeline.ipynb` как пошаговый walkthrough.

## Структура проекта

```text
project_root/
  data/
    raw/
    interim/
    processed/
    qa/
  retail_audit_and_pipeline.ipynb
  notes/
    retail_data_audit.md
    retail_bi_target_model.md
  reports/
    pipeline_run_summary.json
  src/
    __init__.py
    config.py
    io_utils.py
    profiling.py
    normalization.py
    classification.py
    dimensions.py
    facts.py
    qa.py
    export.py
    pipeline.py
    utils.py
  pyproject.toml
  uv-install.bat
  uv-make.bat
  uv-clean.bat
  uv-run.bat
  Retail.xlsx
```

## Где должен лежать `Retail.xlsx`

Предпочтительный путь:

- `data/raw/Retail.xlsx`

Поддерживается и текущий вариант:

- `Retail.xlsx` в корне проекта

Pipeline сначала ищет файл в `data/raw/`, затем в корне проекта.

## Подготовка окружения

1. Установить `uv`:

```bat
uv-install.bat
```

2. Создать `.venv` и установить зависимости:

```bat
uv-make.bat
```

## Запуск pipeline

Локальный сценарий проверки:

```bat
uv-run.bat
```

`uv-run.bat` запускает:

```bat
uv run python -m src.pipeline
```

## Что генерируется после запуска

`data/interim/`

- `retail_audited_lines.parquet`

`data/processed/`

- `fact_sales_lines.parquet` и `.csv`
- `fact_return_lines.parquet` и `.csv`
- `fact_service_lines.parquet` и `.csv`
- `dim_product.parquet` и `.csv`
- `dim_customer.parquet` и `.csv`
- `dim_date.parquet` и `.csv`
- `dim_country.parquet` и `.csv`

`data/qa/`

- `raw_profile.parquet` и `.csv`
- `raw_missingness.parquet` и `.csv`
- `duplicate_rows.parquet` и `.csv`
- `business_duplicate_rows.parquet` и `.csv`
- `return_candidates.parquet` и `.csv`
- `bad_debt_candidates.parquet` и `.csv`
- `service_code_candidates.parquet` и `.csv`
- `zero_price_rows.parquet` и `.csv`
- `anonymous_transactions.parquet` и `.csv`
- `missing_description_rows.parquet` и `.csv`
- `stock_description_issues.parquet` и `.csv`
- `text_noise_summary.parquet` и `.csv`
- `country_normalization.parquet` и `.csv`
- `extreme_rows.parquet` и `.csv`
- `last_month_summary.parquet` и `.csv`
- `line_type_summary.parquet` и `.csv`
- `raw_processed_reconciliation.parquet` и `.csv`

`reports/`

- `pipeline_run_summary.json`

## Что показывает notebook

Notebook `retail_audit_and_pipeline.ipynb`:

- загружает конфиг и исходные данные;
- показывает размер, поля, типы, период;
- пошагово проверяет все обязательные аномалии;
- кратко объясняет, что найдено и какое решение принято в pipeline;
- вызывает внешний pipeline;
- показывает превью итоговых витрин и QA-результатов.

Вся основная логика живёт в `src/`, а notebook только вызывает эти модули.

## Как преподавателю проверить проект локально

1. Убедиться, что `Retail.xlsx` лежит в проекте.
2. Запустить `uv-install.bat`, если `uv` ещё не установлен.
3. Запустить `uv-make.bat`.
4. Запустить `uv-run.bat`.
5. Проверить появление файлов в `data/processed`, `data/qa`, `reports`.
6. Открыть notebook и сравнить audit-first walkthrough с фактическими артефактами pipeline.
