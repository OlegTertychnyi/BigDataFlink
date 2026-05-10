
ALTER TABLE star.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_customer;
ALTER TABLE star.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_seller;
ALTER TABLE star.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_product;
ALTER TABLE star.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_store;
ALTER TABLE star.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_supplier;

ALTER TABLE star.fact_sales
    ADD CONSTRAINT fk_fact_customer
    FOREIGN KEY (customer_email)
    REFERENCES star.dim_customers(customer_email);

ALTER TABLE star.fact_sales
    ADD CONSTRAINT fk_fact_seller
    FOREIGN KEY (seller_email)
    REFERENCES star.dim_sellers(seller_email);

ALTER TABLE star.fact_sales
    ADD CONSTRAINT fk_fact_product
    FOREIGN KEY (product_key)
    REFERENCES star.dim_products(product_key);

ALTER TABLE star.fact_sales
    ADD CONSTRAINT fk_fact_store
    FOREIGN KEY (store_key)
    REFERENCES star.dim_stores(store_key);

ALTER TABLE star.fact_sales
    ADD CONSTRAINT fk_fact_supplier
    FOREIGN KEY (supplier_email)
    REFERENCES star.dim_suppliers(supplier_email);