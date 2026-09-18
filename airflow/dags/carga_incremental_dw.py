"""Carga incremental do Data Warehouse.

O corte de "o que é novo" é sempre pela tabela de fato do corporativo
(corporativo.ocorrencias) — não por data, pelo maior id de origem já
presente (`MAX(id_ocorrencia_origem)`), já que o `id` do datatran é
sequencial dentro do arquivo carregado. Fluxo:

1. staging: só toca o datatran para trazer as poucas linhas com id >
   watermark — a única parte que realmente lê o banco transacional nesta DAG,
   e só as linhas novas (acidente + vítimas + classificações).
2. corporativo (DW normalizado): recopia os catálogos (pequenos, upsert
   idempotente), resolve geografia só das combinações que apareceram na
   staging, e insere as ocorrências novas (append-only).
3. data_marting (estrela): nunca toca o datatran — dims são atualizadas a
   partir do corporativo, fato é inserido a partir do corporativo + dos dims
   já atualizados, filtrando sempre pelas ocorrências que vieram da staging
   desta execução.
4. limpeza da staging.
"""
from __future__ import annotations

from datetime import datetime

import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator

from common_etl import (
    COLUNAS_CLASSIFICACAO,
    NAO_INFORMADO,
    copiar_categorias_vitima,
    copiar_classificacoes_validas,
    copiar_tipos_classificacao,
    get_or_create_classificacao,
    get_pg_conn,
    inserir_corporativo_classificacao,
    inserir_corporativo_ocorrencia,
    inserir_corporativo_vitima,
    refresh_dim_local,
    resolver_local_acidente,
)

# ==================================================================
# FASE 1 — staging: só as ocorrências novas, por watermark no corporativo
# ==================================================================


def stage_acidentes():
    pg_conn = get_pg_conn("dw")
    oltp_conn = get_pg_conn("datatran")
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(id_ocorrencia_origem::int), 0) FROM corporativo.ocorrencias "
                "WHERE sistema_origem = 'PRF-DATATRAN'"
            )
            watermark = cur.fetchone()[0]
            cur.execute("TRUNCATE staging.stg_acidentes, staging.stg_acidente_vitima, staging.stg_acidente_atributo")

        with oltp_conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.data, a.horario, u.sigla, u.nome, m.nome, r.numero, loc.km, a.veiculos
                FROM acidentes a
                LEFT JOIN local_acidente la ON la.id = a.local_id
                LEFT JOIN municipio m ON m.id = la.municipio_id
                LEFT JOIN uf u ON u.sigla = m.uf_sigla
                LEFT JOIN localizacao loc ON loc.id = la.localizacao_id
                LEFT JOIN rodovia r ON r.numero = loc.rodovia_numero
                WHERE a.id > %s
            """, (watermark,))
            novos = cur.fetchall()
            ids_novos = [r[0] for r in novos]

            vitimas, atributos = [], []
            if ids_novos:
                fmt = ",".join(["%s"] * len(ids_novos))
                cur.execute(f"SELECT acidente_id, categoria, quantidade FROM acidente_vitima WHERE acidente_id IN ({fmt})", ids_novos)
                vitimas = cur.fetchall()
                cur.execute(f"SELECT acidente_id, tipo_atributo, valor FROM acidente_atributo WHERE acidente_id IN ({fmt})", ids_novos)
                atributos = cur.fetchall()

        if novos:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO staging.stg_acidentes
                        (id, data, horario, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km, veiculos)
                    VALUES %s
                """, novos)
                if vitimas:
                    psycopg2.extras.execute_values(
                        cur, "INSERT INTO staging.stg_acidente_vitima (acidente_id, categoria, quantidade) VALUES %s", vitimas,
                    )
                if atributos:
                    psycopg2.extras.execute_values(
                        cur, "INSERT INTO staging.stg_acidente_atributo (acidente_id, tipo_atributo, valor) VALUES %s", atributos,
                    )
        pg_conn.commit()
        print(f"watermark={watermark}, {len(novos)} ocorrências novas staged")
    finally:
        oltp_conn.close()
        pg_conn.close()


# ==================================================================
# FASE 2 — corporativo: catálogos, geografia e fato só das chaves novas
# ==================================================================


def atualizar_corporativo_catalogos():
    """Catálogos são pequenos (9 tipos, ~60 valores, 5 categorias) - recopiar
    tudo a cada execução é mais simples que filtrar por staging e não
    sobrecarrega o datatran (upsert idempotente, ver common_etl.copiar_*)."""
    oltp_conn = get_pg_conn("datatran")
    pg_conn = get_pg_conn("dw")
    try:
        with oltp_conn.cursor() as oltp_cur, pg_conn.cursor() as pg_cur:
            n_categorias = copiar_categorias_vitima(oltp_cur, pg_cur)
            n_tipos = copiar_tipos_classificacao(oltp_cur, pg_cur)
            n_valores = copiar_classificacoes_validas(oltp_cur, pg_cur)
        pg_conn.commit()
        print(f"corporativo catálogos: {n_categorias} categorias_vitima, {n_tipos} tipos_classificacao, {n_valores} classificacoes_validas")
    finally:
        oltp_conn.close()
        pg_conn.close()


def atualizar_corporativo_geografia():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT uf_sigla, uf_nome, municipio_nome, rodovia_numero, km
                FROM staging.stg_acidentes
                WHERE uf_sigla IS NOT NULL
            """)
            combos = cur.fetchall()
            for uf_sigla, uf_nome, municipio_nome, rodovia_numero, km in combos:
                resolver_local_acidente(cur, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km)
        pg_conn.commit()
        print(f"corporativo.locais_acidente: {len(combos)} combinações novas processadas")
    finally:
        pg_conn.close()


def carregar_corporativo_ocorrencias():
    """Append-only, direto da staging (que já é o delta) — mesma lógica de
    inserção usada na carga inicial, sem DELETE prévio."""
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, horario, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km, veiculos
                FROM staging.stg_acidentes
            """)
            staged = cur.fetchall()
            cur.execute("SELECT acidente_id, categoria, quantidade FROM staging.stg_acidente_vitima")
            vitimas = cur.fetchall()
            cur.execute("SELECT acidente_id, tipo_atributo, valor FROM staging.stg_acidente_atributo")
            atributos = cur.fetchall()

        vitimas_por_acidente = {}
        for aid, categoria, quantidade in vitimas:
            vitimas_por_acidente.setdefault(aid, []).append((categoria, quantidade))
        atributos_por_acidente = {}
        for aid, tipo, valor in atributos:
            atributos_por_acidente.setdefault(aid, []).append((tipo, valor))

        with pg_conn.cursor() as cur:
            for (oid, data, horario, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km, veiculos) in staged:
                id_local = resolver_local_acidente(cur, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km)
                id_ocorrencia = inserir_corporativo_ocorrencia(cur, oid, data, horario, id_local, veiculos)
                for categoria, quantidade in vitimas_por_acidente.get(oid, []):
                    inserir_corporativo_vitima(cur, id_ocorrencia, categoria, quantidade)
                for tipo, valor in atributos_por_acidente.get(oid, []):
                    inserir_corporativo_classificacao(cur, id_ocorrencia, tipo, valor)
        pg_conn.commit()
        print(f"corporativo.ocorrencias: {len(staged)} ocorrências novas inseridas")
    finally:
        pg_conn.close()


# ==================================================================
# FASE 3 — data_marting: dims a partir do corporativo, fato a partir das
# ocorrências que vieram da staging nesta execução.
# ==================================================================


def atualizar_marting_dim_local():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT o.id_local
                FROM corporativo.ocorrencias o
                JOIN staging.stg_acidentes s ON s.id::text = o.id_ocorrencia_origem
                WHERE o.id_local IS NOT NULL
            """)
            locais = [r[0] for r in cur.fetchall()]
            for id_local in locais:
                refresh_dim_local(cur, id_local)
        pg_conn.commit()
        print(f"dim_local: {len(locais)} linhas atualizadas")
    finally:
        pg_conn.close()


def atualizar_marting_dim_classificacao():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT o.id_ocorrencia
                FROM corporativo.ocorrencias o
                JOIN staging.stg_acidentes s ON s.id::text = o.id_ocorrencia_origem
            """)
            novas = [r[0] for r in cur.fetchall()]
            cur.execute("""
                SELECT oc.id_ocorrencia, oc.tipo_classificacao, oc.valor
                FROM corporativo.ocorrencia_classificacao oc
                JOIN corporativo.ocorrencias o ON o.id_ocorrencia = oc.id_ocorrencia
                JOIN staging.stg_acidentes s ON s.id::text = o.id_ocorrencia_origem
            """)
            classif_rows = cur.fetchall()

        classif_por_ocorrencia = {}
        for id_ocorrencia, tipo, valor in classif_rows:
            classif_por_ocorrencia.setdefault(id_ocorrencia, {})[tipo] = valor

        vistos = set()
        with pg_conn.cursor() as cur:
            for id_ocorrencia in novas:
                valores = classif_por_ocorrencia.get(id_ocorrencia, {})
                chave = tuple((valores.get(c) or NAO_INFORMADO) for c in COLUNAS_CLASSIFICACAO)
                if chave in vistos:
                    continue
                vistos.add(chave)
                get_or_create_classificacao(cur, valores)
        pg_conn.commit()
        print(f"dim_classificacao_acidente: {len(vistos)} combinações novas processadas")
    finally:
        pg_conn.close()


def carregar_marting_fato_acidentes():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT t.id_tempo, dt.id_dim_tempo FROM corporativo.tempos t JOIN dim_tempo dt ON dt.data = t.data")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_local_original, id_dim_local FROM dim_local WHERE sistema_origem = 'PRF-DATATRAN'")
            local_map = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute("""
                SELECT o.id_ocorrencia, o.id_ocorrencia_origem, o.id_tempo, o.id_local, o.veiculos
                FROM corporativo.ocorrencias o
                JOIN staging.stg_acidentes s ON s.id::text = o.id_ocorrencia_origem
            """)
            novas = cur.fetchall()
            ids_novas = [r[0] for r in novas]

            vitima_rows, classif_rows = [], []
            if ids_novas:
                fmt = ",".join(["%s"] * len(ids_novas))
                cur.execute(f"SELECT id_ocorrencia, categoria, quantidade FROM corporativo.ocorrencia_vitima WHERE id_ocorrencia IN ({fmt})", ids_novas)
                vitima_rows = cur.fetchall()
                cur.execute(f"SELECT id_ocorrencia, tipo_classificacao, valor FROM corporativo.ocorrencia_classificacao WHERE id_ocorrencia IN ({fmt})", ids_novas)
                classif_rows = cur.fetchall()

        vitimas_por_ocorrencia = {}
        for id_ocorrencia, categoria, quantidade in vitima_rows:
            vitimas_por_ocorrencia.setdefault(id_ocorrencia, {})[categoria] = quantidade
        classif_por_ocorrencia = {}
        for id_ocorrencia, tipo, valor in classif_rows:
            classif_por_ocorrencia.setdefault(id_ocorrencia, {})[tipo] = valor

        rows = []
        cache_classificacao = {}
        with pg_conn.cursor() as cur:
            for id_ocorrencia, id_origem, id_tempo, id_local, veiculos in novas:
                valores_classificacao = classif_por_ocorrencia.get(id_ocorrencia, {})
                chave = tuple((valores_classificacao.get(c) or NAO_INFORMADO) for c in COLUNAS_CLASSIFICACAO)
                id_dim_classificacao = cache_classificacao.get(chave)
                if id_dim_classificacao is None:
                    id_dim_classificacao = get_or_create_classificacao(cur, valores_classificacao)
                    cache_classificacao[chave] = id_dim_classificacao

                vit = vitimas_por_ocorrencia.get(id_ocorrencia, {})
                mortos = vit.get("mortos", 0)
                feridos_leves = vit.get("feridos_leves", 0)
                feridos_graves = vit.get("feridos_graves", 0)
                ilesos = vit.get("ilesos", 0)
                ignorados = vit.get("ignorados", 0)
                feridos = feridos_leves + feridos_graves
                pessoas = mortos + feridos_leves + feridos_graves + ilesos + ignorados

                rows.append((
                    "PRF-DATATRAN", id_origem,
                    tempo_map.get(id_tempo),
                    local_map.get(str(id_local)) if id_local else None,
                    id_dim_classificacao,
                    veiculos, mortos, feridos_leves, feridos_graves, ilesos, ignorados, feridos, pessoas,
                ))

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fato_acidentes
                        (sistema_origem, id_ocorrencia_original, id_dim_tempo, id_dim_local, id_dim_classificacao,
                         veiculos, mortos, feridos_leves, feridos_graves, ilesos, ignorados, feridos, pessoas)
                    VALUES %s
                """, rows)
            pg_conn.commit()
        print(f"fato_acidentes: {len(rows)} linhas novas inseridas")
    finally:
        pg_conn.close()


def limpar_staging():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("TRUNCATE staging.stg_acidentes, staging.stg_acidente_vitima, staging.stg_acidente_atributo")
        pg_conn.commit()
        print("staging limpa")
    finally:
        pg_conn.close()


with DAG(
    dag_id="carga_incremental_dw",
    description="Carga incremental: staging (watermark no corporativo) -> corporativo (append-only) -> data_marting (estrela)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dw", "etl", "carga-incremental"],
) as dag:

    t_stage = PythonOperator(task_id="stage_acidentes", python_callable=stage_acidentes)

    t_corp_catalogos = PythonOperator(task_id="atualizar_corporativo_catalogos", python_callable=atualizar_corporativo_catalogos)
    t_corp_geografia = PythonOperator(task_id="atualizar_corporativo_geografia", python_callable=atualizar_corporativo_geografia)
    t_corp_ocorrencias = PythonOperator(task_id="carregar_corporativo_ocorrencias", python_callable=carregar_corporativo_ocorrencias)

    t_mart_dim_local = PythonOperator(task_id="atualizar_marting_dim_local", python_callable=atualizar_marting_dim_local)
    t_mart_dim_classificacao = PythonOperator(task_id="atualizar_marting_dim_classificacao", python_callable=atualizar_marting_dim_classificacao)
    t_mart_fato_acidentes = PythonOperator(task_id="carregar_marting_fato_acidentes", python_callable=carregar_marting_fato_acidentes)

    t_limpar_staging = PythonOperator(task_id="limpar_staging", python_callable=limpar_staging)

    t_stage >> [t_corp_catalogos, t_corp_geografia] >> t_corp_ocorrencias
    t_corp_ocorrencias >> [t_mart_dim_local, t_mart_dim_classificacao] >> t_mart_fato_acidentes >> t_limpar_staging
