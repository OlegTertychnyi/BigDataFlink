#!/usr/bin/env bash
# =====================================================================
# полный запуск пайплайна с нуля.
#
# Что делает:
#   1. Останавливаются старые контейнеры и чистится volume
#   2. Билдятся образы (Flink с jar'ами, producer на Python)
#   3. Поднимается инфраструктура: Zookeeper, Kafka, Postgres, Flink JM/TM
#   4. Запускается Flink SQL job — она начинает слушать Kafka
#   5. Запускается producer — он шлёт 10000 JSON-сообщений в Kafka
#   6. Ожидается  пока всё дольётся в Postgres
#   7. Применяются foreign keys на star schema
#   8. Печатаются счётчики и проверяет orphan'ов
# =====================================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "==> [1/8] Stop & clean previous run"
docker compose --profile producer down -v --remove-orphans

echo "==> [2/8] Build images"
docker compose build

echo "==> [3/8] Start infrastructure (zookeeper, kafka, postgres, flink)"
docker compose up -d
echo "    Waiting 25s for services to become healthy..."
sleep 25
docker compose ps

echo "==> [4/8] Submit Flink SQL job"
docker exec bdf_flink_jm /opt/flink/bin/sql-client.sh -f /opt/flink/job/job.sql
echo "    Job submitted. Waiting 5s for job to be RUNNING..."
sleep 5

echo "==> [5/8] Start producer (CSV → Kafka)"
docker compose --profile producer up producer

echo "==> [6/8] Wait 15s for Flink to flush remaining batches"
sleep 15

echo "==> [7/8] Apply foreign keys"
docker exec -i bdf_postgres psql -U flink_user -d bigdata_lab3 < sql/02_foreign_keys.sql

echo "==> [8/8] Final check"
docker exec bdf_postgres psql -U flink_user -d bigdata_lab3 -c "
SELECT 'dim_customers'  AS table_name, COUNT(*) AS rows FROM star.dim_customers
UNION ALL SELECT 'dim_sellers',   COUNT(*) FROM star.dim_sellers
UNION ALL SELECT 'dim_products',  COUNT(*) FROM star.dim_products
UNION ALL SELECT 'dim_stores',    COUNT(*) FROM star.dim_stores
UNION ALL SELECT 'dim_suppliers', COUNT(*) FROM star.dim_suppliers
UNION ALL SELECT 'fact_sales',    COUNT(*) FROM star.fact_sales
ORDER BY table_name;
"

echo ""
echo "==> DONE. Open Flink UI: http://localhost:8082"
echo "    Postgres: localhost:5434, db=bigdata_lab3, user=flink_user, pass=flink_pass"