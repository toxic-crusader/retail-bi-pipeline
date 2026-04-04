# Retail.xlsx — Сводка по анализу данных

> **Статус: анализ и pipeline завершены. Далее — доработка модели и переход к DataLens.**

## Общая информация

| Параметр | Значение |
|---|---|
| Источник | `data/raw/Retail.xlsx` |
| Строк | 947 217 |
| Столбцов | 10 |
| Период | 01.12.2009 — 27.10.2011 (~23 месяца, октябрь неполный) |
| Уникальных инвойсов | 48 753 |
| Уникальных клиентов | 5 695 (+ 214K анонимных транзакций) |
| Уникальных стран | 43 (UK = ~91.8%) |

## Реализованный pipeline

Код: `src/` (config, normalization, classification, dimensions, facts, export, qa, profiling, pipeline, io_utils, utils)
Notebook: `retail_audit_and_pipeline.ipynb` — audit-first журнал с 13 разделами

### Что делает pipeline

1. Загрузка из Excel → нормализация текста и кодов → классификация `line_type`
2. Снятие полных дубликатов (59 730 строк)
3. Разделение на 3 факт-таблицы: sales, returns, service
4. Построение 4 измерений: product, customer, date, country
5. Экспорт: Parquet + CSV + Excel workbook (7 листов) + JSON summary
6. 17 QA-артефактов в `data/qa/`

### Финальный артефакт для DataLens

`data/processed/retail_datalens_export.xlsx` — один файл, 7 листов:
- `fact_sales_lines`, `fact_return_lines`, `fact_service_lines`
- `dim_product`, `dim_customer`, `dim_date`, `dim_country`

## Запланированные доработки pipeline (до перехода к DataLens)

1. **dim_country**: добавить `region`, `is_uk`
2. **dim_customer**: добавить `total_revenue`, `avg_order_value`, `customer_tier`
3. **dim_product**: добавить `product_category` (по ключевым словам)
4. **Факты**: добавить `is_uk`
5. **Агрегаты по инвойсу**: `invoice_total`, `invoice_item_count`

## Вычисляемые поля для DataLens (не в Python)

| Поле | Формула | Где |
|---|---|---|
| Revenue | уже есть `line_amount` | факты |
| Net Revenue | SUM(sales) + SUM(returns) | DataLens |
| Year / Month / Quarter | уже есть в `dim_date` | измерение |
| Return Rate | returns / sales | DataLens |

## Ключевые метрики для дашборда

- Общая и чистая выручка, динамика по месяцам
- Количество заказов, средний чек
- Уникальные клиенты, клиентская сегментация
- Распределение по странам / регионам / каналам
- Top-N товаров и категорий
- Анализ возвратов
- UK vs International
