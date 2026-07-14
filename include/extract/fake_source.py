"""Fontes de dados 100% fictícias, usadas no lugar de uma API/arquivo real.

Em produção, `fetch_orders` seria uma chamada HTTP paginada a uma API de
pedidos (com retry/backoff tratado pelos `retries`/`retry_delay` da task no
Airflow, não aqui) e `fetch_products` seria a leitura de um arquivo que um
sistema de catálogo deixa cair diariamente numa pasta/bucket. Isoladas aqui
pra deixar claro que só essas duas funções mudariam numa integração real —
o resto do DAG (load, transform, checks) não sabe nem se importa de onde os
dados vieram.

A geração é determinística (seed = data do pedido) só pra facilitar
demonstração e testes — reexecutar o mesmo dia sempre gera os mesmos dados.
"""
import random
from datetime import date, timedelta

CATEGORIES = ["Eletrônicos", "Casa", "Livros", "Esporte", "Beleza", "Brinquedos"]

PRODUCTS = [
    {"product_id": "P001", "product_name": "Fone Bluetooth X200", "category": "Eletrônicos", "cost_price": 45.00},
    {"product_id": "P002", "product_name": "Carregador USB-C 30W", "category": "Eletrônicos", "cost_price": 18.50},
    {"product_id": "P003", "product_name": "Caixa de Som Portátil", "category": "Eletrônicos", "cost_price": 60.00},
    {"product_id": "P004", "product_name": "Jogo de Panelas 5 Peças", "category": "Casa", "cost_price": 90.00},
    {"product_id": "P005", "product_name": "Luminária de Mesa LED", "category": "Casa", "cost_price": 22.00},
    {"product_id": "P006", "product_name": "Kit Organizadores 3un", "category": "Casa", "cost_price": 15.00},
    {"product_id": "P007", "product_name": "Romance Best-seller", "category": "Livros", "cost_price": 12.00},
    {"product_id": "P008", "product_name": "Livro Infantil Ilustrado", "category": "Livros", "cost_price": 9.00},
    {"product_id": "P009", "product_name": "Tapete de Yoga", "category": "Esporte", "cost_price": 28.00},
    {"product_id": "P010", "product_name": "Garrafa Térmica 1L", "category": "Esporte", "cost_price": 20.00},
    {"product_id": "P011", "product_name": "Corda de Pular", "category": "Esporte", "cost_price": 8.00},
    {"product_id": "P012", "product_name": "Kit Skincare Facial", "category": "Beleza", "cost_price": 35.00},
    {"product_id": "P013", "product_name": "Secador de Cabelo", "category": "Beleza", "cost_price": 55.00},
    {"product_id": "P014", "product_name": "Quebra-cabeça 500 Peças", "category": "Brinquedos", "cost_price": 17.00},
    {"product_id": "P015", "product_name": "Carrinho de Controle Remoto", "category": "Brinquedos", "cost_price": 48.00},
]

_STATUS_WEIGHTS = [("completed", 0.78), ("cancelled", 0.10), ("refunded", 0.05), ("pending", 0.07)]

_FIRST_NAMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Hugo",
                "Isabela", "João", "Karina", "Lucas", "Mariana", "Nicolas", "Olívia"]
_LAST_NAMES = ["Silva", "Souza", "Costa", "Pereira", "Oliveira", "Rocha", "Almeida", "Barbosa"]


def fetch_products() -> list[dict]:
    """Simula a leitura do catálogo de produtos (arquivo estável, não muda por dia)."""
    return list(PRODUCTS)


def _pick_status(rng: random.Random) -> str:
    r = rng.random()
    acumulado = 0.0
    for status, peso in _STATUS_WEIGHTS:
        acumulado += peso
        if r <= acumulado:
            return status
    return _STATUS_WEIGHTS[-1][0]


def fetch_orders(order_date: date, n_customers: int = 220) -> list[dict]:
    """Simula pedidos de um dia. Cada cliente fictício faz 0-3 pedidos."""
    rng = random.Random(order_date.toordinal())
    orders = []
    order_seq = 0

    for customer_idx in range(n_customers):
        customer_id = f"C{customer_idx:05d}"
        customer_name = f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"
        customer_email = f"{customer_name.lower().replace(' ', '.')}@example.com"

        n_orders = rng.choices([0, 1, 2, 3], weights=[0.55, 0.30, 0.10, 0.05])[0]
        for _ in range(n_orders):
            order_seq += 1
            produto = rng.choice(PRODUCTS)
            orders.append({
                "order_id": f"O{order_date.strftime('%Y%m%d')}{order_seq:05d}",
                "customer_id": customer_id,
                "customer_name": customer_name,
                "customer_email": customer_email,
                "product_id": produto["product_id"],
                "quantity": rng.randint(1, 4),
                "unit_price": round(produto["cost_price"] * rng.uniform(1.4, 2.2), 2),
                "status": _pick_status(rng),
                "order_date": order_date,
            })

    return orders


if __name__ == "__main__":
    ontem = date.today() - timedelta(days=1)
    pedidos = fetch_orders(ontem)
    print(f"{len(pedidos)} pedidos fictícios gerados para {ontem}")
    print(pedidos[:3])
