-- Reconstroi as dimensões e a fato a partir da raw carregada nesta execução.
-- Full refresh: TRUNCATE + INSERT (sem FK entre as tabelas — ver ddl_warehouse.sql
-- — então a ordem aqui é só por clareza, não por constraint).

TRUNCATE TABLE warehouse.fact_orders;
TRUNCATE TABLE warehouse.dim_customers;
TRUNCATE TABLE warehouse.dim_products;

INSERT INTO warehouse.dim_products (product_id, product_name, category, cost_price)
SELECT DISTINCT product_id, product_name, category, cost_price
FROM raw.products;

INSERT INTO warehouse.dim_customers (customer_id, customer_name, customer_email)
SELECT DISTINCT ON (customer_id) customer_id, customer_name, customer_email
FROM raw.orders
ORDER BY customer_id, loaded_at DESC;

INSERT INTO warehouse.fact_orders
    (order_id, customer_id, product_id, order_date, quantity, unit_price, revenue, margin, status)
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    (o.quantity * o.unit_price)                         AS revenue,
    (o.quantity * (o.unit_price - p.cost_price))         AS margin,
    o.status
FROM raw.orders o
JOIN raw.products p ON p.product_id = o.product_id;
