"""Clusterização de municípios (KMeans) + regras de associação (Apriori) por
cluster, sobre o data mart em estrela (schema `public`, banco `dw`).

Camada 5 do projeto (ver README.md) - lê só `fato_acidentes`/`dim_local`/
`dim_tempo`/`dim_classificacao_acidente` (nunca o `datatran` OLTP nem o
`corporativo`) e grava em `public.cluster_municipio`/`public.regra_associacao`,
consumidas pelo painel do Metabase. Disparo manual, depois da
`carga_inicial_dw` (ou de uma `carga_incremental_dw`) - pode ser rerodada a
qualquer momento pra atualizar clusters/regras sem reprocessar o ETL inteiro.

A lógica em si mora em `ml/clusterizacao_regras_associacao.py` (fora de
`airflow/dags/`, reaproveitável como script de linha de comando fora do
Airflow também - ver README.md) - `docker-compose.yml` monta esse diretório
dentro do container do Airflow como `airflow/dags/ml`.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parent / "ml"))


def _rodar_clusterizacao_regras_associacao(**context):
    from clusterizacao_regras_associacao import rodar_pipeline

    rodar_pipeline(
        host="postgres",
        dbname="dw",
        output_dir="/opt/airflow/dags/ml/output",
        persist=True,
        ano=context["params"]["ano"],
    )


with DAG(
    dag_id="ml_clusterizacao_regras_associacao",
    description="Clusterização de municípios (KMeans) + regras de associação (Apriori) por cluster, de um ano por vez",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["dw", "ml", "clusterizacao", "regras-associacao"],
    # Os 20 anos juntos (~2,2M ocorrências) fazem o Apriori por cluster
    # estourar a memória disponível no container do Airflow (ver nota em
    # ml/clusterizacao_regras_associacao.py:rodar_pipeline) - roda um ano por
    # vez; troque o valor ao disparar a DAG ("Trigger DAG w/ config") pra
    # analisar outro ano.
    params={"ano": 2024},
) as dag:
    t_ml = PythonOperator(
        task_id="rodar_clusterizacao_regras_associacao",
        python_callable=_rodar_clusterizacao_regras_associacao,
    )
