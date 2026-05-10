SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'pipeline.name' = 'kafka-to-postgres-star';

CREATE TABLE kafka_source (
    source_id              INT,

    customer_email         STRING,
    customer_first_name    STRING,
    customer_last_name     STRING,
    customer_age           INT,
    customer_country       STRING,
    customer_postal_code   STRING,
    customer_pet_type      STRING,
    customer_pet_name      STRING,
    customer_pet_breed     STRING,

    seller_email           STRING,
    seller_first_name      STRING,
    seller_last_name       STRING,
    seller_country         STRING,
    seller_postal_code     STRING,

    product_key            STRING,
    product_name           STRING,
    product_brand          STRING,
    product_category       STRING,
    pet_category           STRING,
    product_price          DECIMAL(12, 2),
    product_weight         DECIMAL(10, 3),
    product_color          STRING,
    product_size           STRING,
    product_material       STRING,
    product_description    STRING,
    product_rating         DECIMAL(3, 2),
    product_reviews        INT,
    product_release_date   DATE,
    product_expiry_date    DATE,

    store_key              STRING,
    store_name             STRING,
    store_location         STRING,
    store_city             STRING,
    store_state            STRING,
    store_country          STRING,
    store_phone            STRING,
    store_email            STRING,

    supplier_email         STRING,
    supplier_name          STRING,
    supplier_contact       STRING,
    supplier_phone         STRING,
    supplier_address       STRING,
    supplier_city          STRING,
    supplier_country       STRING,

    sale_date              DATE,
    sale_quantity          INT,
    sale_total_price       DECIMAL(14, 2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'mock_data_stream',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-star-loader',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

CREATE TABLE sink_dim_customers (
    customer_email   STRING,
    first_name       STRING,
    last_name        STRING,
    age              INT,
    country          STRING,
    postal_code      STRING,
    pet_type         STRING,
    pet_name         STRING,
    pet_breed        STRING,
    PRIMARY KEY (customer_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_lab3',
    'table-name' = 'star.dim_customers',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
);

CREATE TABLE sink_dim_sellers (
    seller_email  STRING,
    first_name    STRING,
    last_name     STRING,
    country       STRING,
    postal_code   STRING,
    PRIMARY KEY (seller_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_lab3',
    'table-name' = 'star.dim_sellers',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
);

CREATE TABLE sink_dim_products (
    product_key       STRING,
    product_name      STRING,
    product_brand     STRING,
    product_category  STRING,
    pet_category      STRING,
    price             DECIMAL(12, 2),
    weight            DECIMAL(10, 3),
    color             STRING,
    size              STRING,
    material          STRING,
    description       STRING,
    rating            DECIMAL(3, 2),
    reviews           INT,
    release_date      DATE,
    expiry_date       DATE,
    PRIMARY KEY (product_key) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_lab3',
    'table-name' = 'star.dim_products',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
);

CREATE TABLE sink_dim_stores (
    store_key       STRING,
    store_name      STRING,
    store_location  STRING,
    store_city      STRING,
    store_state     STRING,
    store_country   STRING,
    store_phone     STRING,
    store_email     STRING,
    PRIMARY KEY (store_key) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_lab3',
    'table-name' = 'star.dim_stores',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
);

CREATE TABLE sink_dim_suppliers (
    supplier_email    STRING,
    supplier_name     STRING,
    supplier_contact  STRING,
    supplier_phone    STRING,
    supplier_address  STRING,
    supplier_city     STRING,
    supplier_country  STRING,
    PRIMARY KEY (supplier_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_lab3',
    'table-name' = 'star.dim_suppliers',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
);

CREATE TABLE sink_fact_sales (
    customer_email     STRING,
    seller_email       STRING,
    product_key        STRING,
    store_key          STRING,
    supplier_email     STRING,
    sale_date          DATE,
    sale_quantity      INT,
    sale_total_price   DECIMAL(14, 2),
    source_id          INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_lab3',
    'table-name' = 'star.fact_sales',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
);

EXECUTE STATEMENT SET
BEGIN

    INSERT INTO sink_dim_customers
    SELECT
        customer_email,
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_country,
        customer_postal_code,
        customer_pet_type,
        customer_pet_name,
        customer_pet_breed
    FROM kafka_source
    WHERE customer_email IS NOT NULL;

    INSERT INTO sink_dim_sellers
    SELECT
        seller_email,
        seller_first_name,
        seller_last_name,
        seller_country,
        seller_postal_code
    FROM kafka_source
    WHERE seller_email IS NOT NULL;

    INSERT INTO sink_dim_products
    SELECT
        product_key,
        product_name,
        product_brand,
        product_category,
        pet_category,
        product_price,
        product_weight,
        product_color,
        product_size,
        product_material,
        product_description,
        product_rating,
        product_reviews,
        product_release_date,
        product_expiry_date
    FROM kafka_source
    WHERE product_key IS NOT NULL;

    INSERT INTO sink_dim_stores
    SELECT
        store_key,
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email
    FROM kafka_source
    WHERE store_key IS NOT NULL;

    INSERT INTO sink_dim_suppliers
    SELECT
        supplier_email,
        supplier_name,
        supplier_contact,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    FROM kafka_source
    WHERE supplier_email IS NOT NULL;

    INSERT INTO sink_fact_sales
    SELECT
        customer_email,
        seller_email,
        product_key,
        store_key,
        supplier_email,
        sale_date,
        sale_quantity,
        sale_total_price,
        source_id
    FROM kafka_source;

END;