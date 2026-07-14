"""Teste de integração: roda o DDL + transform de verdade contra um Postgres
real e confere que os checks de qualidade passam com dado bom e detectam
dado quebrado. Não depende do Airflow (usa psycopg2 puro) — só precisa de
um Postgres acessível via as variáveis padrão da libpq (PGHOST, PGPORT,
PGDATABASE, PGUSER, PGPASSWORD).

Pulado automaticamente se não houver Postgres disponível (ex: rodando os
testes localmente sem Docker). No CI, um serviço Postgres é provisionado e
essas variáveis são exportadas — ver .github/workflows/ci.yml.
"""
from datetime import date
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from include.extract.fake_source import fetch_orders, fetch_products  # noqa: E402
from include.quality.checks import DataQualityError, run_all_checks  # noqa: E402

SQL_DIR = Path(__file__).parent.parent / "include" / "sql"


@pytest.fixture()
def conexao():
    try:
        conn = psycopg2.connect(connect_timeout=3)
    except Exception as e:  # pragma: no cover - só acontece sem Postgres disponível
        pytest.skip(f"Postgres não disponível para o teste de integração: {e}")
    conn.autocommit = True
    yield conn
    conn.close()


def _rodar_arquivo_sql(conexao, caminho: Path) -> None:
    with conexao.cursor() as cur:
        cur.execute(caminho.read_text(encoding="utf-8"))


def _carregar_dados_ficticios(conexao, dia: date) -> tuple[int, int]:
    produtos = fetch_products()
    pedidos = fetch_orders(dia)

    with conexao.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw.products")
        cur.executemany(
            "INSERT INTO raw.products (product_id, product_name, category, cost_price) "
            "VALUES (%(product_id)s, %(product_name)s, %(category)s, %(cost_price)s)",
            produtos,
        )
        cur.execute("TRUNCATE TABLE raw.orders")
        cur.executemany(
            "INSERT INTO raw.orders "
            "(order_id, customer_id, customer_name, customer_email, product_id, quantity, unit_price, status, order_date) "
            "VALUES (%(order_id)s, %(customer_id)s, %(customer_name)s, %(customer_email)s, "
            "%(product_id)s, %(quantity)s, %(unit_price)s, %(status)s, %(order_date)s)",
            pedidos,
        )

    return len(produtos), len(pedidos)


def _fetch_scalar(conexao):
    def _fn(query: str):
        with conexao.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    return _fn


def test_ddl_e_transform_produzem_star_schema_consistente(conexao):
    _rodar_arquivo_sql(conexao, SQL_DIR / "ddl_raw.sql")
    _rodar_arquivo_sql(conexao, SQL_DIR / "ddl_warehouse.sql")

    n_produtos, n_pedidos = _carregar_dados_ficticios(conexao, date(2026, 3, 10))
    assert n_pedidos > 0

    _rodar_arquivo_sql(conexao, SQL_DIR / "transform_fact_orders.sql")

    fetch_scalar = _fetch_scalar(conexao)
    assert fetch_scalar("SELECT COUNT(*) FROM warehouse.dim_products") == n_produtos
    assert fetch_scalar("SELECT COUNT(*) FROM warehouse.fact_orders") == n_pedidos
    assert fetch_scalar("SELECT COUNT(*) FROM warehouse.fact_orders WHERE revenue <= 0") == 0

    # Não deve levantar — o dado gerado é consistente por construção.
    run_all_checks(fetch_scalar)


def test_checks_detectam_pedido_orfao(conexao):
    _rodar_arquivo_sql(conexao, SQL_DIR / "ddl_raw.sql")
    _rodar_arquivo_sql(conexao, SQL_DIR / "ddl_warehouse.sql")

    _carregar_dados_ficticios(conexao, date(2026, 3, 11))
    _rodar_arquivo_sql(conexao, SQL_DIR / "transform_fact_orders.sql")

    with conexao.cursor() as cur:
        # Quebra a integridade de propósito: um pedido apontando pra um
        # cliente que não existe em dim_customers.
        cur.execute(
            "INSERT INTO warehouse.fact_orders "
            "(order_id, customer_id, product_id, order_date, quantity, unit_price, revenue, margin, status) "
            "SELECT 'O-ORFAO-001', 'CLIENTE-INEXISTENTE', product_id, order_date, 1, 10.0, 10.0, 5.0, 'completed' "
            "FROM warehouse.dim_products LIMIT 1"
        )

    fetch_scalar = _fetch_scalar(conexao)
    with pytest.raises(DataQualityError, match="sem cliente correspondente"):
        run_all_checks(fetch_scalar)
