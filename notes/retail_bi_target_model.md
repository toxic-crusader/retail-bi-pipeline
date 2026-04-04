# Модель данных для Yandex DataLens — текущий статус и план доработок

> **Статус: базовая модель ГОТОВА, требуются доработки для упрощения BI.**

## 1. Текущая структура (реализовано)

Финальный артефакт: `data/processed/retail_datalens_export.xlsx` — один workbook с 7 листами.

### Факт-таблицы

| Таблица | Строк | Назначение |
|---|---:|---|
| `fact_sales_lines` | ~860K (после дедупликации) | Товарные продажи |
| `fact_return_lines` | ~20K | Возвраты и сторно |
| `fact_service_lines` | ~7.5K | Доставка, скидки, комиссии, корректировки, bad debt, тесты |

Схема фактов (27 колонок): `raw_record_id`, `invoice_id`, `invoice_date`, `customer_id`, `channel`, `country_raw`, `country_norm`, `stock_code_raw`, `stock_code_norm`, `description_raw`, `description_norm`, `product_name_canonical`, `quantity`, `unit_price`, `line_amount`, `line_type`, `is_duplicate`, `is_business_duplicate`, `is_service_line`, `is_anonymous_customer`.

### Измерения

| Таблица | Колонок | Ключ | Назначение |
|---|---:|---|---|
| `dim_product` | 7 | `stock_code_norm` | Канонический справочник SKU |
| `dim_customer` | 7 | `customer_id` | Клиент + канал + базовые метрики |
| `dim_date` | 9 | `date` | Календарь с флагами |
| `dim_country` | 1 | `country_name` | Нормализованная география |

### Связи (JOIN keys)

- `fact.stock_code_norm → dim_product.stock_code_norm`
- `fact.customer_id → dim_customer.customer_id`
- `fact.invoice_date → dim_date.date`
- `fact.country_norm → dim_country.country_name`

## 2. Что нужно доработать в pipeline

### 2.1. Обогатить `dim_country`

Сейчас: 1 колонка `country_name`.

Добавить:
- `region` — географический регион (Western Europe, Scandinavia, Middle East и т.д.)
- `is_uk` — флаг UK (91.8% данных, нужен почти на каждом дашборде)

### 2.2. Обогатить `dim_customer` предрассчитанными метриками

Сейчас: `customer_id`, `channel`, даты, `invoice_count`, `country_count`, `is_anonymous_customer`.

Добавить:
- `total_revenue` — суммарная выручка клиента
- `total_quantity` — суммарное количество единиц
- `avg_order_value` — средний чек
- `rfm_segment` или `customer_tier` — сегмент (Top / Medium / Low / Anonymous)

### 2.3. Добавить товарные категории в `dim_product`

Сейчас: `stock_code_norm`, `product_name_canonical`, `is_service_code` и метаданные.

Добавить:
- `product_category` — базовая категория по ключевым словам Description (HOME DECOR, KITCHEN, BAGS, CHRISTMAS, STATIONERY и т.д.)

### 2.4. Добавить `is_uk` в факт-таблицы

Простой bool-флаг для быстрой фильтрации UK vs International без JOIN на dim_country.

### 2.5. Предрассчитать агрегаты на уровне инвойса

Отдельный лист или колонки в фактах:
- `invoice_total` — сумма по инвойсу
- `invoice_item_count` — количество позиций в инвойсе

Это упростит анализ среднего чека в DataLens без LOD-выражений.

## 3. Основные KPI для дашборда

### Товарный контур (fact_sales_lines + fact_return_lines)

- Валовая выручка (gross revenue)
- Чистая выручка (net revenue = sales - returns)
- Количество заказов
- Средний чек
- Количество уникальных клиентов
- Продажи по странам / регионам / каналам
- Top-N товаров по выручке
- Динамика по месяцам (с отсечкой неполного октября 2011)
- Клиентская сегментация

### Сервисный контур (fact_service_lines)

- Доставка (объём и доля)
- Скидки
- Комиссии
- Возвраты (отдельный анализ причин)

## 4. Требования к дашборду (из задания)

- Минимум 2 страницы/вкладки с навигацией
- Интерактивность и кросс-фильтрация
- Описание бизнес-смысла в сворачиваемом блоке на первой вкладке
- Описание метрик и почему они выбраны
- Проблемы бизнеса со ссылками на визуализации
- Управленческие решения со ссылками на данные
- Какие дополнительные срезы были бы полезны

## 5. Что НЕ делать в Python

- Не строить сводные таблицы — это работа DataLens
- Не делать визуализации в notebook
- Не усложнять модель SCD Type 2
- Не создавать избыточные вычисляемые поля, которые тривиально делаются в DataLens
