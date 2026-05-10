# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

## Требования

- **Docker** + **Docker Compose**
- ~4 ГБ свободной RAM, ~3 ГБ места на диске
- Bash (для `run.sh`)
- Опционально: **DBeaver** для просмотра данных

## Запуск с нуля

Одна команда — поднимает всю инфраструктуру, отправляет данные, применяет FK, печатает результат:

```bash
./run.sh
```
Сразу пометка - в начале чистятся volume-ы!

После завершения должны появиться счётчики:

```
  table_name   | rows
---------------+-------
 dim_customers | 10000
 dim_products  |  1149
 dim_sellers   | 10000
 dim_stores    |  9998
 dim_suppliers | 10000
 fact_sales    | 10000
```

`fact_sales = 10000` — все продажи доехали. Размеры dim'ов отражают уникальность бизнес-ключей в исходных CSV (mock-данные с почти уникальными email'ами).

## Доступ к сервисам

- **Flink Web UI:** http://localhost:8082 — там видна running-job, граф source→sinks, метрики и checkpoints.
- **PostgreSQL** через DBeaver / psql:
  - Host: `localhost`, Port: `5434`
  - DB: `bigdata_lab3`, User: `flink_user`, Pass: `flink_pass`
  - Схема: `star`

## Проверка целостности

```bash
docker exec -it bdf_postgres psql -U flink_user -d bigdata_lab3 -c "
SELECT
  COUNT(*)                                            AS total_facts,
  COUNT(*) FILTER (WHERE c.customer_email IS NULL)    AS orphan_customers,
  COUNT(*) FILTER (WHERE s.seller_email   IS NULL)    AS orphan_sellers,
  COUNT(*) FILTER (WHERE p.product_key    IS NULL)    AS orphan_products,
  COUNT(*) FILTER (WHERE st.store_key     IS NULL)    AS orphan_stores,
  COUNT(*) FILTER (WHERE sup.supplier_email IS NULL)  AS orphan_suppliers
FROM star.fact_sales f
LEFT JOIN star.dim_customers  c   ON f.customer_email = c.customer_email
LEFT JOIN star.dim_sellers    s   ON f.seller_email   = s.seller_email
LEFT JOIN star.dim_products   p   ON f.product_key    = p.product_key
LEFT JOIN star.dim_stores     st  ON f.store_key      = st.store_key
LEFT JOIN star.dim_suppliers  sup ON f.supplier_email = sup.supplier_email;
"
```

Все `orphan_*` должны быть `0`.

## Пример аналитического запроса

```sql
SELECT
  p.product_brand,
  p.product_category,
  COUNT(*)                                  AS sales,
  SUM(f.sale_total_price)::numeric(14,2)    AS revenue
FROM star.fact_sales f
JOIN star.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_brand, p.product_category
ORDER BY revenue DESC
LIMIT 10;
```

## Остановка

```bash
docker compose --profile producer down
docker compose --profile producer down -v
```

## Технические заметки

**Streaming-семантика:** Flink-джоба не завершается после прочтения 10000 сообщений — это streaming, она работает бесконечно. Если перезапустить producer (например `docker compose --profile producer up producer`), Flink ту же дельту дольёт: dim'ы перезапишутся через UPSERT (PK не позволят задублировать), `fact_sales` увеличится в 2 раза (append-only).

**FK применяются после заливки.** В streaming dim-записи и fact-записи приходят параллельно — fact может прийти раньше dim, и FK выкинул бы ошибку. Поэтому FK добавляются скриптом `sql/02_foreign_keys.sql` уже после того, как поток заглох.

**JDBC connector в Flink** работает в upsert-режиме при наличии `PRIMARY KEY ... NOT ENFORCED` в Flink-таблице — он генерирует `INSERT ... ON CONFLICT DO UPDATE`. Для `fact_sales` PK не задан → append-only INSERT.

**Чекпоинты Flink** — раз в 10 секунд (см. `flink-job/job.sql`). Они нужны для exactly-once семантики JDBC sink'а: Kafka offset'ы коммитятся вместе с записью батча в Postgres.