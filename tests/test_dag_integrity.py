"""Testes de integridade de DAG — padrão comum em times de dados pra pegar
erro bobo (import quebrado, task sem retry, DAG sem tag/dono) antes de virar
problema em produção. Precisa do Airflow instalado; noAirflow só roda
oficialmente em Linux/macOS (ver README), então este arquivo é validado no
CI (Ubuntu), não necessariamente no ambiente local de quem está lendo isso.
"""
import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_FOLDER = "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    # Airflow 3.x: `DagBag` não tem mais `include_examples` — sem
    # AIRFLOW__CORE__LOAD_EXAMPLES=true no ambiente, só os DAGs de
    # `dag_folder` são carregados de qualquer forma.
    return DagBag(dag_folder=DAGS_FOLDER)


def test_dagbag_tem_pelo_menos_um_dag(dagbag):
    assert len(dagbag.dags) > 0, "nenhum DAG encontrado em dags/"


def test_nenhum_erro_de_import(dagbag):
    assert not dagbag.import_errors, f"erros de import: {dagbag.import_errors}"


def test_dag_principal_carrega_com_as_tasks_esperadas(dagbag):
    dag = dagbag.dags.get("ecommerce_daily_elt")
    assert dag is not None

    task_ids = set(dag.task_ids)
    esperado = {
        "create_schema", "extract_products", "extract_orders",
        "load_raw_products", "load_raw_orders",
        "transform_star_schema", "run_quality_checks",
    }
    assert esperado <= task_ids


def test_todos_os_dags_tem_tags(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} está sem tags"


def test_todas_as_tasks_tem_retry_configurado(dagbag):
    for dag_id, dag in dagbag.dags.items():
        for t in dag.tasks:
            assert t.retries and t.retries >= 1, f"{dag_id}.{t.task_id} sem retries configurado"


def test_dags_nao_tem_ciclo(dagbag):
    for dag in dagbag.dags.values():
        dag.check_cycle()  # levanta AirflowDagCycleException se houver dependência circular


def test_dag_nao_roda_catchup_por_acidente(dagbag):
    """catchup=True por engano faria o Airflow tentar rodar todo o
    histórico desde start_date na primeira ativação — comum causa de
    surpresa desagradável em produção."""
    dag = dagbag.dags.get("ecommerce_daily_elt")
    assert dag.catchup is False
