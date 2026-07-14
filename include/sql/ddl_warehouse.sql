-- Camada warehouse: modelo dimensional (star schema) usado pelas consultas
-- analíticas. Reconstruída a cada execução a partir da raw (abordagem
-- "full refresh" — adequada pro volume de um portfólio; em produção com
-- volume maior isso viraria incremental por partição de data).

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_id     VARCHAR(40) PRIMARY KEY,
    customer_name   VARCHAR(200),
    customer_email  VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_id      VARCHAR(40) PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    cost_price      NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id        VARCHAR(40) PRIMARY KEY,
    customer_id     VARCHAR(40) NOT NULL REFERENCES warehouse.dim_customers(customer_id),
    product_id      VARCHAR(40) NOT NULL REFERENCES warehouse.dim_products(product_id),
    order_date      DATE NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    revenue         NUMERIC(12, 2) NOT NULL,
    margin          NUMERIC(12, 2) NOT NULL,
    status          VARCHAR(20) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON warehouse.fact_orders (order_date);
