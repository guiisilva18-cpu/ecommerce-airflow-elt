"""DAG diário: extrai pedidos/produtos (fontes fictícias), carrega numa
camada raw no Postgres, transforma num star schema (warehouse) e roda
checks de qualidade antes de considerar o dia "pronto".

    extract_products ─┐
                       ├─> load_raw_products ─┐
    extract_orders ────┘                     ├─> transform_star_schema ─> run_quality_checks
                        > load_raw_orders ────┘

`create_schema` roda primeiro e sozinho (idempotente, `CREATE ... IF NOT
EXISTS`) — não depende do dia sendo processado.

Todas as tasks de banco usam a connection `warehouse_postgres` (configurada
via variável de ambiente no docker-compose, ver README) em vez de
credenciais hardcoded.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task

INCLUDE_DIR = Path(__file__).parent.parent / "include"
POSTGRES_CONN_ID = "warehouse_postgres"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_sql_file(hook, caminho: Path) -> None:
    hook.run(caminho.read_text(encoding="utf-8"))


@dag(
    dag_id="ecommerce_daily_elt",
    description="ELT diário: pedidos/produtos fictícios -> Postgres raw -> star schema -> data quality",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["elt", "ecommerce", "portfolio"],
    doc_md=__doc__,
)
def ecommerce_daily_elt():

    @task
    def create_schema() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        _run_sql_file(hook, INCLUDE_DIR / "sql" / "ddl_raw.sql")
        _run_sql_file(hook, INCLUDE_DIR / "sql" / "ddl_warehouse.sql")

    @task
    def extract_products() -> list[dict]:
        """Em produção: leitura de um arquivo de catálogo dropado por outro
        sistema. Aqui: catálogo fictício fixo (ver include/extract/fake_source.py)."""
        from include.extract.fake_source import fetch_products

        return fetch_products()

    @task
    def extract_orders(logical_date=None) -> list[dict]:
        """Em produção: chamada paginada a uma API de pedidos — `retries`/
        `retry_delay` (default_args) cobrem falhas transitórias de rede.
        Aqui: pedidos fictícios gerados de forma determinística para o dia
        do DAG run (`logical_date`), pra facilitar reprocessamento/backfill."""
        from include.extract.fake_source import fetch_orders

        dia = logical_date.date() if logical_date else pendulum.today("UTC").date()
        return fetch_orders(dia)

    @task
    def load_raw_products(products: list[dict]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("TRUNCATE TABLE raw.products")
        linhas = [
            (p["product_id"], p["product_name"], p["category"], p["cost_price"])
            for p in products
        ]
        hook.insert_rows(
            table="raw.products",
            rows=linhas,
            target_fields=["product_id", "product_name", "category", "cost_price"],
        )
        return len(linhas)

    @task
    def load_raw_orders(orders: list[dict]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("TRUNCATE TABLE raw.orders")
        linhas = [
            (
                o["order_id"], o["customer_id"], o["customer_name"], o["customer_email"],
                o["product_id"], o["quantity"], o["unit_price"], o["status"], o["order_date"],
            )
            for o in orders
        ]
        hook.insert_rows(
            table="raw.orders",
            rows=linhas,
            target_fields=[
                "order_id", "customer_id", "customer_name", "customer_email",
                "product_id", "quantity", "unit_price", "status", "order_date",
            ],
        )
        return len(linhas)

    @task
    def transform_star_schema(_raiz_products: int, _raiz_orders: int) -> None:
        """Recebe as contagens de load_raw_* só pra forçar a ordem de
        execução (o transform lê da raw, então não pode rodar antes dela
        estar carregada) — os valores em si não são usados."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        _run_sql_file(hook, INCLUDE_DIR / "sql" / "transform_fact_orders.sql")

    @task
    def run_quality_checks() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from include.quality.checks import run_all_checks

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        def fetch_scalar(query: str):
            return hook.get_first(query)[0]

        run_all_checks(fetch_scalar)

    schema_pronto = create_schema()

    produtos = extract_products()
    pedidos = extract_orders()

    qtd_produtos = load_raw_products(produtos)
    qtd_pedidos = load_raw_orders(pedidos)

    schema_pronto >> [qtd_produtos, qtd_pedidos]

    transformado = transform_star_schema(qtd_produtos, qtd_pedidos)
    transformado >> run_quality_checks()


ecommerce_daily_elt()
