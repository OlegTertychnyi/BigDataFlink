CREATE SCHEMA IF NOT EXISTS star;

DROP TABLE IF EXISTS star.fact_sales CASCADE;
DROP TABLE IF EXISTS star.dim_customers CASCADE;
DROP TABLE IF EXISTS star.dim_sellers CASCADE;
DROP TABLE IF EXISTS star.dim_products CASCADE;
DROP TABLE IF EXISTS star.dim_stores CASCADE;
DROP TABLE IF EXISTS star.dim_suppliers CASCADE;

CREATE TABLE star.dim_customers (
    customer_email VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    country VARCHAR(100),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100)
);

CREATE TABLE star.dim_sellers (
    seller_email VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(50)
);

CREATE TABLE star.dim_products (
    product_key VARCHAR(500) PRIMARY KEY,
    product_name VARCHAR(255),
    product_brand VARCHAR(255),
    product_category VARCHAR(100),
    pet_category VARCHAR(100),
    price NUMERIC(12, 2),
    weight NUMERIC(10, 3),
    color VARCHAR(50),
    size VARCHAR(50),
    material VARCHAR(100),
    description TEXT,
    rating NUMERIC(3, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE star.dim_stores (
    store_key VARCHAR(500) PRIMARY KEY,
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(255)
);

CREATE TABLE star.dim_suppliers (
    supplier_email VARCHAR(255) PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

CREATE TABLE star.fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    customer_email VARCHAR(255),
    seller_email VARCHAR(255),
    product_key VARCHAR(500),
    store_key VARCHAR(500),
    supplier_email VARCHAR(255),
    sale_date DATE,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(14, 2),
    source_id INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_customer ON star.fact_sales (customer_email);
CREATE INDEX idx_fact_seller ON star.fact_sales (seller_email);
CREATE INDEX idx_fact_product ON star.fact_sales (product_key);
CREATE INDEX idx_fact_store ON star.fact_sales (store_key);
CREATE INDEX idx_fact_supplier ON star.fact_sales (supplier_email);
CREATE INDEX idx_fact_date ON star.fact_sales (sale_date);

DO $$
BEGIN
    RAISE NOTICE '==> Star schema created: 5 dim tables + 1 fact table';
END $$;