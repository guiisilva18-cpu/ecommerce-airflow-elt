"""Checks de qualidade rodados depois do transform, antes de considerar a
carga do dia "boa". Recebem um `fetch_scalar` (qualquer callable
`str -> valor da primeira coluna/linha`) em vez de uma conexão de banco
diretamente — assim dá pra testar as regras com um double simples, sem
precisar de Postgres de verdade (ver tests/test_quality_checks.py).
"""
from typing import Callable

FetchScalar = Callable[[str], object]


class DataQualityError(Exception):
    """Levantado quando um check falha — a task correspondente falha junto,
    então a execução do dia fica marcada como falha em vez de "sucesso"
    silencioso com dado ruim."""


def check_row_counts(fetch_scalar: FetchScalar) -> None:
    raw_orders = fetch_scalar("SELECT COUNT(*) FROM raw.orders")
    fact_orders = fetch_scalar("SELECT COUNT(*) FROM warehouse.fact_orders")

    if raw_orders == 0:
        raise DataQualityError("raw.orders está vazia — a extração não trouxe nenhum pedido.")
    if fact_orders != raw_orders:
        raise DataQualityError(
            f"warehouse.fact_orders tem {fact_orders} linhas, esperado {raw_orders} "
            "(mesmo total de raw.orders — o transform não deveria descartar nem duplicar pedidos)."
        )


def check_no_null_keys(fetch_scalar: FetchScalar) -> None:
    nulos = fetch_scalar(
        "SELECT COUNT(*) FROM warehouse.fact_orders "
        "WHERE customer_id IS NULL OR product_id IS NULL OR order_id IS NULL"
    )
    if nulos > 0:
        raise DataQualityError(f"{nulos} linha(s) em fact_orders com chave nula (customer_id/product_id/order_id).")


def check_referential_integrity(fetch_scalar: FetchScalar) -> None:
    orfaos_cliente = fetch_scalar(
        "SELECT COUNT(*) FROM warehouse.fact_orders f "
        "LEFT JOIN warehouse.dim_customers c ON c.customer_id = f.customer_id "
        "WHERE c.customer_id IS NULL"
    )
    if orfaos_cliente > 0:
        raise DataQualityError(f"{orfaos_cliente} pedido(s) em fact_orders sem cliente correspondente em dim_customers.")

    orfaos_produto = fetch_scalar(
        "SELECT COUNT(*) FROM warehouse.fact_orders f "
        "LEFT JOIN warehouse.dim_products p ON p.product_id = f.product_id "
        "WHERE p.product_id IS NULL"
    )
    if orfaos_produto > 0:
        raise DataQualityError(f"{orfaos_produto} pedido(s) em fact_orders sem produto correspondente em dim_products.")


def check_non_negative_values(fetch_scalar: FetchScalar) -> None:
    invalidos = fetch_scalar(
        "SELECT COUNT(*) FROM warehouse.fact_orders WHERE quantity <= 0 OR unit_price < 0 OR revenue < 0"
    )
    if invalidos > 0:
        raise DataQualityError(f"{invalidos} pedido(s) com quantidade/preço/receita inválidos (<= 0).")


def run_all_checks(fetch_scalar: FetchScalar) -> None:
    check_row_counts(fetch_scalar)
    check_no_null_keys(fetch_scalar)
    check_referential_integrity(fetch_scalar)
    check_non_negative_values(fetch_scalar)
