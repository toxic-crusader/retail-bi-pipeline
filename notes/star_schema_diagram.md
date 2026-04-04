# Звёздная схема: связи таблиц

```
                          +------------------+
                          |   dim_product    |
                          |------------------|
                          | stock_code_norm  |  <-- PK
                          | product_name_    |
                          |   canonical      |
                          | product_category |
                          | is_service_code  |
                          | ...              |
                          +--------+---------+
                                   |
                                   | stock_code_norm
                                   |
+------------------+     +---------+-------------------+     +------------------+
|   dim_customer   |     |     fact_sales_lines        |     |    dim_date      |
|------------------|     |     fact_return_lines       |     |------------------|
| customer_id      | <-- | --- fact_service_lines ---- | --> | date             |  <-- PK
| channel          |     |-----------------------------|     | year             |
| customer_tier    |     | invoice_id                  |     | quarter          |
| total_revenue    |     | invoice_date                |     | month            |
| avg_order_value  |     | customer_id           (FK)  |     | month_name       |
| order_count      |     | stock_code_norm       (FK)  |     | year_month       |
| is_anonymous_    |     | country_name          (FK)  |     | is_last_         |
|   customer       |     | invoice_date          (FK)  |     |   incomplete_    |
| ...              |     | quantity                    |     |   month          |
+------------------+     | unit_price                  |     +------------------+
        PK: customer_id  | line_amount                 |
                          | invoice_total               |
                          | invoice_item_count          |
                          | is_uk                       |
                          | ...                         |
                          +--------+--------------------+
                                   |
                                   | country_name
                                   |
                          +--------+---------+
                          |   dim_country    |
                          |------------------|
                          | country_name     |  <-- PK
                          | region           |
                          | is_uk            |
                          +------------------+
```

## Связи для настройки в DataLens

| # | Таблица-факт (FK)         | Поле факта          | Таблица-измерение | Поле измерения     | Тип связи |
|---|---------------------------|---------------------|-------------------|--------------------|-----------|
| 1 | fact_sales_lines          | `stock_code_norm`   | dim_product       | `stock_code_norm`  | LEFT JOIN |
| 2 | fact_sales_lines          | `customer_id`       | dim_customer      | `customer_id`      | LEFT JOIN |
| 3 | fact_sales_lines          | `country_name`      | dim_country       | `country_name`     | LEFT JOIN |
| 4 | fact_sales_lines          | `invoice_date`      | dim_date          | `date`             | LEFT JOIN |

Те же 4 связи повторить для `fact_return_lines` и `fact_service_lines`.

## Визуально

```
dim_product ──── stock_code_norm ────┐
                                     │
dim_customer ─── customer_id ────────┤
                                     ├──── FACT TABLES (x3)
dim_country ──── country_name ───────┤
                                     │
dim_date ─────── date ═ invoice_date ┘
```
