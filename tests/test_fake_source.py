from datetime import date

from include.extract.fake_source import fetch_orders, fetch_products


def test_fetch_products_returns_stable_catalog():
    produtos = fetch_products()
    assert len(produtos) > 0
    ids = [p["product_id"] for p in produtos]
    assert len(ids) == len(set(ids)), "product_id deve ser único no catálogo"
    for p in produtos:
        assert p["cost_price"] > 0


def test_fetch_orders_is_deterministic_per_day():
    dia = date(2026, 3, 10)
    primeira = fetch_orders(dia)
    segunda = fetch_orders(dia)
    assert primeira == segunda, "mesma data deve gerar exatamente os mesmos pedidos (reprodutibilidade)"


def test_fetch_orders_different_days_differ():
    pedidos_dia1 = fetch_orders(date(2026, 3, 10))
    pedidos_dia2 = fetch_orders(date(2026, 3, 11))
    assert pedidos_dia1 != pedidos_dia2


def test_fetch_orders_references_valid_products():
    produtos_validos = {p["product_id"] for p in fetch_products()}
    pedidos = fetch_orders(date(2026, 3, 10))
    assert len(pedidos) > 0
    for pedido in pedidos:
        assert pedido["product_id"] in produtos_validos
        assert pedido["quantity"] > 0
        assert pedido["unit_price"] > 0
        assert pedido["status"] in {"completed", "cancelled", "refunded", "pending"}
