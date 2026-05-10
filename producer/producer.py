"""
CSV → Kafka producer для BigDataFlink.

Читает все CSV из /data, каждую строку преобразует в JSON-сообщение
и отправляет в Kafka-топик. Перед отправкой добавляет синтетические
ключи product_key и store_key, которые Flink будет использовать
как PK для dim_products и dim_stores.
"""
import csv
import glob
import json
import logging
import os
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "mock_data_stream")
DATA_DIR = os.getenv("DATA_DIR", "/data")
DELAY_MS = int(os.getenv("DELAY_MS", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("producer")

def parse_int(value):
    """'123' -> 123, '' -> None, 'abc' -> None"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_float(value):
    """'1.23' -> 1.23, '$1.23' -> 1.23 (стрипаем валюту)"""
    if value is None or value == "":
        return None
    cleaned = str(value).replace("$", "").replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    """'1/15/2024' или '2024-01-15' -> '2024-01-15' (ISO для Flink)"""
    if value is None or value == "":
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def s(value):
    """Нормализация строк: '' -> None, иначе строка как есть"""
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None

def row_to_message(row: dict) -> dict:
    """
    Принимает dict от csv.DictReader, возвращает плоский JSON-словарь
    с типизированными значениями и сгенерированными ключами для dim'ов.
    """
    product_name = s(row.get("product_name"))
    product_brand = s(row.get("product_brand"))
    store_name = s(row.get("store_name"))
    store_city = s(row.get("store_city"))

    product_key = (
        f"{product_name}||{product_brand}"
        if product_name and product_brand
        else None
    )
    store_key = (
        f"{store_name}||{store_city}"
        if store_name and store_city
        else None
    )

    return {
        "source_id": parse_int(row.get("id")),

        "customer_email": s(row.get("customer_email")),
        "customer_first_name": s(row.get("customer_first_name")),
        "customer_last_name": s(row.get("customer_last_name")),
        "customer_age": parse_int(row.get("customer_age")),
        "customer_country": s(row.get("customer_country")),
        "customer_postal_code": s(row.get("customer_postal_code")),
        "customer_pet_type": s(row.get("customer_pet_type")),
        "customer_pet_name": s(row.get("customer_pet_name")),
        "customer_pet_breed": s(row.get("customer_pet_breed")),

        "seller_email": s(row.get("seller_email")),
        "seller_first_name": s(row.get("seller_first_name")),
        "seller_last_name": s(row.get("seller_last_name")),
        "seller_country": s(row.get("seller_country")),
        "seller_postal_code": s(row.get("seller_postal_code")),

        "product_key": product_key,
        "product_name": product_name,
        "product_brand": product_brand,
        "product_category": s(row.get("product_category")),
        "pet_category": s(row.get("pet_category")),
        "product_price": parse_float(row.get("product_price")),
        "product_weight": parse_float(row.get("product_weight")),
        "product_color": s(row.get("product_color")),
        "product_size": s(row.get("product_size")),
        "product_material": s(row.get("product_material")),
        "product_description": s(row.get("product_description")),
        "product_rating": parse_float(row.get("product_rating")),
        "product_reviews": parse_int(row.get("product_reviews")),
        "product_release_date": parse_date(row.get("product_release_date")),
        "product_expiry_date": parse_date(row.get("product_expiry_date")),

        "store_key": store_key,
        "store_name": store_name,
        "store_location": s(row.get("store_location")),
        "store_city": store_city,
        "store_state": s(row.get("store_state")),
        "store_country": s(row.get("store_country")),
        "store_phone": s(row.get("store_phone")),
        "store_email": s(row.get("store_email")),

        "supplier_email": s(row.get("supplier_email")),
        "supplier_name": s(row.get("supplier_name")),
        "supplier_contact": s(row.get("supplier_contact")),
        "supplier_phone": s(row.get("supplier_phone")),
        "supplier_address": s(row.get("supplier_address")),
        "supplier_city": s(row.get("supplier_city")),
        "supplier_country": s(row.get("supplier_country")),

        "sale_date": parse_date(row.get("sale_date")),
        "sale_quantity": parse_int(row.get("sale_quantity")),
        "sale_total_price": parse_float(row.get("sale_total_price")),
    }

def connect_kafka(retries: int = 30, delay_s: int = 2) -> KafkaProducer:
    last_err = None
    for i in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=50,
                compression_type="gzip",
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable as e:
            last_err = e
            log.warning("Kafka not ready (%d/%d), retrying in %ds...", i, retries, delay_s)
            time.sleep(delay_s)
    raise RuntimeError(f"Could not connect to Kafka after {retries} retries: {last_err}")

def main():
    log.info("=== CSV → Kafka producer ===")
    log.info("Topic:     %s", TOPIC)
    log.info("Data dir:  %s", DATA_DIR)
    log.info("Delay:     %d ms between messages", DELAY_MS)

    pattern = os.path.join(DATA_DIR, "MOCK_DATA*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        log.error("No CSV files found in %s (pattern: MOCK_DATA*.csv)", DATA_DIR)
        log.info("Listing %s:", DATA_DIR)
        for f in os.listdir(DATA_DIR):
            log.info("  - %s", f)
        sys.exit(1)
    log.info("Found %d CSV file(s):", len(files))
    for f in files:
        log.info("  - %s", os.path.basename(f))

    producer = connect_kafka()

    total_sent = 0
    delay_s = DELAY_MS / 1000.0

    for path in files:
        fname = os.path.basename(path)
        sent_in_file = 0
        log.info("Processing %s ...", fname)
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                msg = row_to_message(row)
                key = str(msg.get("source_id") or "")
                producer.send(TOPIC, key=key, value=msg)
                sent_in_file += 1
                total_sent += 1
                if delay_s > 0:
                    time.sleep(delay_s)
                if total_sent % 1000 == 0:
                    log.info("... %d messages sent", total_sent)
        log.info("Finished %s: %d messages", fname, sent_in_file)

    log.info("Flushing buffer ...")
    producer.flush()
    producer.close()
    log.info("=== DONE: %d messages sent to topic '%s' ===", total_sent, TOPIC)


if __name__ == "__main__":
    main()