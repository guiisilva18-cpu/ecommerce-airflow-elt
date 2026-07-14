import pytest

from include.quality.checks import (
    DataQualityError,
    check_no_null_keys,
    check_non_negative_values,
    check_referential_integrity,
    check_row_counts,
    run_all_checks,
)


def _fetch_scalar_sequence(valores: list[object]):
    """Double de `fetch_scalar` que devolve os valores em sequência, na
    mesma ordem em que cada check dispara suas queries. Evita casar
    substring de SQL (frágil quando uma query é prefixo de outra)."""
    it = iter(valores)

    def fetch_scalar(query: str):
        try:
            return next(it)
        except StopIteration:
            raise AssertionError(f"mais queries do que valores esperados no teste (query: {query!r})")

    return fetch_scalar


def test_check_row_counts_passa_quando_bate():
    fetch_scalar = _fetch_scalar_sequence([100, 100])  # raw.orders, fact_orders
    check_row_counts(fetch_scalar)  # não deve levantar


def test_check_row_counts_falha_quando_raw_vazia():
    fetch_scalar = _fetch_scalar_sequence([0, 0])
    with pytest.raises(DataQualityError, match="raw.orders está vazia"):
        check_row_counts(fetch_scalar)


def test_check_row_counts_falha_quando_contagens_divergem():
    fetch_scalar = _fetch_scalar_sequence([100, 97])
    with pytest.raises(DataQualityError, match="97"):
        check_row_counts(fetch_scalar)


def test_check_no_null_keys_falha_quando_ha_nulos():
    fetch_scalar = _fetch_scalar_sequence([3])
    with pytest.raises(DataQualityError, match="chave nula"):
        check_no_null_keys(fetch_scalar)


def test_check_referential_integrity_falha_com_cliente_orfao():
    fetch_scalar = _fetch_scalar_sequence([2, 0])  # órfãos de cliente, depois de produto
    with pytest.raises(DataQualityError, match="sem cliente correspondente"):
        check_referential_integrity(fetch_scalar)


def test_check_referential_integrity_falha_com_produto_orfao():
    fetch_scalar = _fetch_scalar_sequence([0, 4])
    with pytest.raises(DataQualityError, match="sem produto correspondente"):
        check_referential_integrity(fetch_scalar)


def test_check_non_negative_values_falha_com_valor_invalido():
    fetch_scalar = _fetch_scalar_sequence([1])
    with pytest.raises(DataQualityError):
        check_non_negative_values(fetch_scalar)


def test_run_all_checks_passa_quando_tudo_ok():
    # ordem: raw count, fact count, nulos, órfãos cliente, órfãos produto, valores inválidos
    fetch_scalar = _fetch_scalar_sequence([50, 50, 0, 0, 0, 0])
    run_all_checks(fetch_scalar)  # não deve levantar


def test_run_all_checks_para_no_primeiro_erro():
    # raw vazia -> nem chega a checar o resto
    fetch_scalar = _fetch_scalar_sequence([0, 0])
    with pytest.raises(DataQualityError, match="raw.orders está vazia"):
        run_all_checks(fetch_scalar)
