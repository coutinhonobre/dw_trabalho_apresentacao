"""Bootstrap: na primeira vez que o scheduler do Airflow sobe, dispara
`carga_inicial_dw` e, na sequência, `ml_clusterizacao_regras_associacao`
automaticamente - sem precisar de `docker exec ... airflow dags trigger`
manual (ver README.md, seções "Airflow — carga inicial" e "Airflow —
clusterização...").

`schedule="@once"` dispara exatamente uma vez, no instante em que o DAG é
visto pela primeira vez pelo scheduler. Como o Airflow deste projeto roda
`standalone` sem volume de metadados (SQLite dentro do container, perdido a
cada `docker compose down`/`--force-recreate` - ver docker-compose.yml), os
metadados SEMPRE nascem vazios junto com o Postgres, então "a primeira vez
que o scheduler vê este DAG" coincide com "os containers acabaram de subir".
`is_paused_upon_creation=False` é necessário para isso funcionar sem
intervenção: um DAG pausado não é disparado automaticamente pelo scheduler,
só manualmente.

A primeira task confere se `fato_acidentes` já tem linhas antes de disparar
qualquer coisa - cobre o caso de reiniciar o MESMO container (sem recriar: a
task não teria porque rodar de novo, já tem dado) sem precisar de outro
mecanismo de estado.

IMPORTANTE: o scheduler vê este DAG (e portanto dispara o `@once`) muito
antes do banco `dw` existir de verdade - o Postgres ainda está processando
o `db/02_data.sql` do OLTP (pode levar dezenas de minutos com o dataset
completo) quando o Airflow já terminou de subir. Por isso a primeira task
usa `retries`/`retry_delay` do Airflow (não um sleep manual) pra tolerar
"database dw does not exist" e ir tentando de novo até o Postgres estar
pronto, em vez de falhar de cara.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.exceptions import AirflowSkipException

# Cobre até ~1h esperando o banco `dw`/schema do data mart ficarem prontos -
# mesma ordem de grandeza do AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT
# usado pelas outras DAGs (ver docker-compose.yml).
RETRIES_ESPERANDO_DW = 120
RETRY_DELAY = timedelta(seconds=30)


def _checar_dados_existentes():
    conn = psycopg2.connect(host="postgres", port=5432, dbname="dw", user="postgres", password="postgres")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.fato_acidentes')")
            if cur.fetchone()[0] is None:
                print("public.fato_acidentes ainda não existe (schema do data mart não aplicado) - prosseguindo com a carga.")
                return
            cur.execute("SELECT count(*) FROM fato_acidentes")
            total = cur.fetchone()[0]
    finally:
        conn.close()

    if total > 0:
        raise AirflowSkipException(f"fato_acidentes já tem {total} linhas - pulando carga automática.")
    print("fato_acidentes existe mas está vazio - prosseguindo com a carga.")


with DAG(
    dag_id="bootstrap_carga_e_ml",
    description="Dispara carga_inicial_dw + ml_clusterizacao_regras_associacao automaticamente na primeira subida dos containers, se o data mart ainda estiver vazio",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args={"retries": RETRIES_ESPERANDO_DW, "retry_delay": RETRY_DELAY},
    tags=["dw", "bootstrap"],
) as dag:
    t_checar = PythonOperator(
        task_id="checar_dados_existentes",
        python_callable=_checar_dados_existentes,
    )
    t_carga_inicial = TriggerDagRunOperator(
        task_id="disparar_carga_inicial_dw",
        trigger_dag_id="carga_inicial_dw",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        fail_when_dag_is_paused=True,
    )
    t_ml = TriggerDagRunOperator(
        task_id="disparar_ml_clusterizacao_regras_associacao",
        trigger_dag_id="ml_clusterizacao_regras_associacao",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        fail_when_dag_is_paused=True,
    )

    t_checar >> t_carga_inicial >> t_ml
