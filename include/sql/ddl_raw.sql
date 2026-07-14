-- Camada raw: espelha a origem quase sem transformação (1 tabela por fonte).
-- Recriada a cada execução (TRUNCATE + INSERT) — a raw aqui é só um buffer
-- de staging pro dia sendo processado, não um histórico bruto permanente.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        VARCHAR(40) PRIMARY KEY,
    customer_id     VARCHAR(40) NOT NULL,
    customer_name   VARCHAR(200),
    customer_email  VARCHAR(200),
    product_id      VARCHAR(40) NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    order_date      DATE NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id      VARCHAR(40) PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    cost_price      NUMERIC(10, 2) NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT now()
);
