# E-commerce Airflow ELT

Pipeline diário orquestrado com **Apache Airflow 3** (TaskFlow API): extrai
pedidos e catálogo de produtos (fontes fictícias), carrega numa camada
`raw` no Postgres, transforma num **star schema** e roda **checks de
qualidade** antes de considerar o dia pronto. Dados e fontes são
fictícios — o objetivo é demonstrar orquestração, modelagem dimensional e
testes automatizados de um pipeline de dados.

```
                 ┌──────────────────┐
                 │  create_schema   │  (DDL idempotente)
                 └────────┬─────────┘
           ┌──────────────┴──────────────┐
  ┌────────▼────────┐           ┌────────▼────────┐
  │ extract_products │           │  extract_orders  │
  └────────┬────────┘           └────────┬────────┘
  ┌────────▼────────┐           ┌────────▼────────┐
  │load_raw_products │           │ load_raw_orders  │
  └────────┬────────┘           └────────┬────────┘
           └──────────────┬──────────────┘
                 ┌─────────▼──────────┐
                 │ transform_star_schema │  (raw -> dim/fact)
                 └─────────┬──────────┘
                 ┌─────────▼──────────┐
                 │ run_quality_checks │  (falha a task se o dado vier ruim)
                 └────────────────────┘
```

## Por que estas decisões

**TaskFlow API (`@dag`/`@task`) em vez de `PythonOperator` clássico.**
Menos boilerplate, passagem de dados entre tasks via retorno de função
(XCom automático) em vez de `ti.xcom_push`/`xcom_pull` manual — é o padrão
recomendado pelo próprio Airflow desde a 2.0.

**Connection via `AIRFLOW_CONN_WAREHOUSE_POSTGRES` (env var), não hardcoded.**
Nenhuma credencial no código — nem fixa, nem em `Variable`/`Connection`
cadastrada manualmente na UI. Isso também é o que faz o mesmo código
funcionar sem alteração no CI (onde a mesma env var poderia apontar pro
Postgres de serviço do GitHub Actions).

**Extração isolada em `include/extract/fake_source.py`.**
As tasks `extract_*` só chamam essas funções — é a ÚNICA parte do projeto
que mudaria numa integração real (API de pedidos de verdade, arquivo de
catálogo de verdade). O resto do DAG (load, transform, checks) não sabe
nem se importa de onde o dado veio.

**Full refresh (`TRUNCATE` + `INSERT`) em vez de incremental.**
Volume de portfólio não justifica a complexidade de merge/upsert
incremental. Documentado no SQL como a primeira coisa a trocar se o volume
crescesse (particionar por `order_date` e só reprocessar o dia).

**`run_quality_checks` como task própria, depois do transform.**
Separar qualidade de dado de transformação deixa claro *o que* está sendo
garantido (contagem bate, sem chave nula, sem órfão contra as dimensões,
sem valor negativo) e falha a **task**, não só loga um warning — um
pipeline "verde" nunca deveria significar "rodou e talvez o dado esteja
ruim". Ver [`include/quality/checks.py`](include/quality/checks.py).

**Checks testáveis sem banco.** `checks.py` recebe um `fetch_scalar`
(qualquer callable), não uma conexão — os testes unitários passam um dublê
em memória; só o teste de integração (`tests/test_sql_transform.py`) usa
Postgres de verdade. Ver seção de testes abaixo.

## Estrutura

- [`dags/ecommerce_daily_elt.py`](dags/ecommerce_daily_elt.py) — o DAG.
- [`include/extract/`](include/extract/) — fonte de dados fictícia (a única parte "trocável" numa integração real).
- [`include/sql/`](include/sql/) — DDL da raw/warehouse e o transform.
- [`include/quality/checks.py`](include/quality/checks.py) — regras de qualidade, testáveis isoladamente.
- [`tests/`](tests/) — unitários (sem banco/Airflow), integridade de DAG e integração SQL (com Postgres real).
- `docker-compose.yml` + `Dockerfile` — sobem o Airflow (LocalExecutor) + Postgres localmente.
- `.github/workflows/ci.yml` — roda toda a suíte de testes a cada push.

## Como rodar

```bash
cp .env.example .env   # preencha FERNET_KEY (comando abaixo) e o resto pode ficar como está
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

docker compose up airflow-init   # cria o banco de metadados e o usuário admin
docker compose up                # sobe tudo (apiserver, scheduler, dag-processor, triggerer)
```

UI em `http://localhost:8080` (login: `airflow` / `airflow`, definidos no
`.env`). Ative o DAG `ecommerce_daily_elt` e dispare uma execução manual —
ou espere o schedule diário. O warehouse fica exposto em `localhost:5433`
pra conectar um cliente SQL (usuário/senha/banco: `warehouse`).

> A parte de orquestração (docker-compose) não foi testada num Docker real
> neste ambiente de desenvolvimento (sandbox sem Docker disponível) — o
> `docker-compose.yml` é adaptado diretamente do
> [template oficial do Airflow 3.3](https://airflow.apache.org/docs/apache-airflow/3.3.0/docker-compose.yaml),
> trocando CeleryExecutor+Redis por LocalExecutor. A lógica do pipeline em
> si (SQL, checks, DAG) é validada pelos testes abaixo, que rodam de
> verdade no CI. Se algo estiver errado na orquestração, abra uma issue.

## Testes

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/ -v
```

- `test_fake_source.py` / `test_quality_checks.py` — puros, sem Airflow nem banco, rodam em qualquer máquina.
- `test_dag_integrity.py` — precisa do Airflow instalado (`pip install apache-airflow`); confere que o DAG importa sem erro, tem tags/retries configurados e não tem dependência circular. Airflow só é suportado oficialmente em Linux/macOS.
- `test_sql_transform.py` — precisa de um Postgres acessível (variáveis `PG*` da libpq); roda o DDL + transform de verdade e confere que os checks pegam tanto o caso bom quanto um pedido órfão inserido de propósito.

O [CI](.github/workflows/ci.yml) roda os três num Postgres de serviço do
GitHub Actions a cada push — é a validação de ponta a ponta deste projeto.

## Stack

Apache Airflow 3.3 (TaskFlow API), PostgreSQL 16, Docker Compose, pytest,
GitHub Actions.

## Sobre os dados

Catálogo de produtos, clientes e pedidos são gerados por
[`include/extract/fake_source.py`](include/extract/fake_source.py) com
seed determinística por data — não representam nenhuma empresa real.

## Licença

MIT — ver [LICENSE](LICENSE).
