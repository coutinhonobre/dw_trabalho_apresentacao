"""Primeira carga do Data Warehouse.

Pipeline em duas camadas:
1. corporativo (DW normalizado, Inmon): lê `datatran` (OLTP, Postgres) direto,
   copia os catálogos (categorias de vítima, tipos e valores de
   classificação), resolve a hierarquia de geografia/via e grava o fato
   operacional (ocorrencias/ocorrencia_vitima/ocorrencia_classificacao).
2. data_marting (estrela, Kimball): lê só o corporativo — nunca o datatran —
   e monta dim_local/dim_classificacao_acidente/fato_acidentes para consulta
   de BI.
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
# FASE 1 — corporativo (DW normalizado)
# ==================================================================


def carregar_corporativo_catalogos():
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


def carregar_corporativo_geografia():
    """Resolve toda a hierarquia (uf -> municipio, rodovia -> localizacao ->
    local_acidente) a partir das combinações distintas já usadas por
    local_acidente no datatran — feito antes da carga das ocorrências pra não
    disputar a criação da mesma uf/município em paralelo dentro da mesma
    ocorrência."""
    oltp_conn = get_pg_conn("datatran")
    pg_conn = get_pg_conn("dw")
    try:
        with oltp_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT u.sigla, u.nome, m.nome, r.numero, loc.km
                FROM local_acidente la
                JOIN municipio m ON m.id = la.municipio_id
                JOIN uf u ON u.sigla = m.uf_sigla
                JOIN localizacao loc ON loc.id = la.localizacao_id
                JOIN rodovia r ON r.numero = loc.rodovia_numero
            """)
            combos = cur.fetchall()

        with pg_conn.cursor() as cur:
            for uf_sigla, uf_nome, municipio_nome, rodovia_numero, km in combos:
                resolver_local_acidente(cur, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km)
        pg_conn.commit()
        print(f"corporativo.locais_acidente: {len(combos)} combinações processadas")
    finally:
        oltp_conn.close()
        pg_conn.close()


def carregar_corporativo_ocorrencias():
    oltp_conn = get_pg_conn("datatran")
    pg_conn = get_pg_conn("dw")
    try:
        with oltp_conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.data, a.horario, a.veiculos,
                       u.sigla, u.nome, m.nome, r.numero, loc.km
                FROM acidentes a
                LEFT JOIN local_acidente la ON la.id = a.local_id
                LEFT JOIN municipio m ON m.id = la.municipio_id
                LEFT JOIN uf u ON u.sigla = m.uf_sigla
                LEFT JOIN localizacao loc ON loc.id = la.localizacao_id
                LEFT JOIN rodovia r ON r.numero = loc.rodovia_numero
            """)
            acidentes = cur.fetchall()
            cur.execute("SELECT acidente_id, categoria, quantidade FROM acidente_vitima")
            vitimas = cur.fetchall()
            cur.execute("SELECT acidente_id, tipo_atributo, valor FROM acidente_atributo")
            atributos = cur.fetchall()

        vitimas_por_acidente = {}
        for aid, categoria, quantidade in vitimas:
            vitimas_por_acidente.setdefault(aid, []).append((categoria, quantidade))
        atributos_por_acidente = {}
        for aid, tipo, valor in atributos:
            atributos_por_acidente.setdefault(aid, []).append((tipo, valor))

        with pg_conn.cursor() as cur:
            # carga inicial: recria o fato do zero (idêntico ao padrão do
            # projeto de referência para vendas/compras)
            cur.execute("DELETE FROM corporativo.ocorrencia_vitima")
            cur.execute("DELETE FROM corporativo.ocorrencia_classificacao")
            cur.execute("DELETE FROM corporativo.ocorrencias")

            for (oid, data, horario, veiculos, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km) in acidentes:
                id_local = resolver_local_acidente(cur, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km)
                id_ocorrencia = inserir_corporativo_ocorrencia(cur, oid, data, horario, id_local, veiculos)
                for categoria, quantidade in vitimas_por_acidente.get(oid, []):
                    inserir_corporativo_vitima(cur, id_ocorrencia, categoria, quantidade)
                for tipo, valor in atributos_por_acidente.get(oid, []):
                    inserir_corporativo_classificacao(cur, id_ocorrencia, tipo, valor)
        pg_conn.commit()
        print(f"corporativo.ocorrencias: {len(acidentes)} ocorrências carregadas")
    finally:
        oltp_conn.close()
        pg_conn.close()


# ==================================================================
# FASE 2 — data_marting (estrela), lida só do corporativo
# ==================================================================


def carregar_marting_dim_local():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT id_local FROM corporativo.ocorrencias WHERE id_local IS NOT NULL")
            locais = [r[0] for r in cur.fetchall()]
            for id_local in locais:
                refresh_dim_local(cur, id_local)
        pg_conn.commit()
        print(f"dim_local: {len(locais)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_dim_classificacao():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id_ocorrencia FROM corporativo.ocorrencias")
            todas = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id_ocorrencia, tipo_classificacao, valor FROM corporativo.ocorrencia_classificacao")
            classif_rows = cur.fetchall()

        classif_por_ocorrencia = {}
        for id_ocorrencia, tipo, valor in classif_rows:
            classif_por_ocorrencia.setdefault(id_ocorrencia, {})[tipo] = valor

        vistos = set()
        with pg_conn.cursor() as cur:
            for id_ocorrencia in todas:
                valores = classif_por_ocorrencia.get(id_ocorrencia, {})
                chave = tuple((valores.get(c) or NAO_INFORMADO) for c in COLUNAS_CLASSIFICACAO)
                if chave in vistos:
                    continue
                vistos.add(chave)
                get_or_create_classificacao(cur, valores)
        pg_conn.commit()
        print(f"dim_classificacao_acidente: {len(vistos)} combinações distintas processadas")
    finally:
        pg_conn.close()


def carregar_marting_fato_acidentes():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM fato_acidentes")

            cur.execute("SELECT t.id_tempo, dt.id_dim_tempo FROM corporativo.tempos t JOIN dim_tempo dt ON dt.data = t.data")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_local_original, id_dim_local FROM dim_local WHERE sistema_origem = 'PRF-DATATRAN'")
            local_map = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute("SELECT id_ocorrencia, id_ocorrencia_origem, id_tempo, id_local, veiculos FROM corporativo.ocorrencias")
            ocorrencias = cur.fetchall()
            cur.execute("SELECT id_ocorrencia, categoria, quantidade FROM corporativo.ocorrencia_vitima")
            vitima_rows = cur.fetchall()
            cur.execute("SELECT id_ocorrencia, tipo_classificacao, valor FROM corporativo.ocorrencia_classificacao")
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
            for id_ocorrencia, id_origem, id_tempo, id_local, veiculos in ocorrencias:
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
                    tempo_map[id_tempo],
                    local_map.get(str(id_local)) if id_local else None,
                    id_dim_classificacao,
                    veiculos, mortos, feridos_leves, feridos_graves, ilesos, ignorados, feridos, pessoas,
                ))

            psycopg2.extras.execute_values(cur, """
                INSERT INTO fato_acidentes
                    (sistema_origem, id_ocorrencia_original, id_dim_tempo, id_dim_local, id_dim_classificacao,
                     veiculos, mortos, feridos_leves, feridos_graves, ilesos, ignorados, feridos, pessoas)
                VALUES %s
            """, rows)
        pg_conn.commit()
        print(f"fato_acidentes: {len(rows)} linhas carregadas a partir do corporativo")
    finally:
        pg_conn.close()


with DAG(
    dag_id="carga_inicial_dw",
    description="Primeira carga: datatran -> corporativo (DW normalizado, Inmon) -> data_marting (estrela, Kimball)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dw", "etl", "carga-inicial"],
) as dag:

    t_corp_catalogos = PythonOperator(task_id="carregar_corporativo_catalogos", python_callable=carregar_corporativo_catalogos)
    t_corp_geografia = PythonOperator(task_id="carregar_corporativo_geografia", python_callable=carregar_corporativo_geografia)
    t_corp_ocorrencias = PythonOperator(task_id="carregar_corporativo_ocorrencias", python_callable=carregar_corporativo_ocorrencias)

    t_mart_dim_local = PythonOperator(task_id="carregar_marting_dim_local", python_callable=carregar_marting_dim_local)
    t_mart_dim_classificacao = PythonOperator(task_id="carregar_marting_dim_classificacao", python_callable=carregar_marting_dim_classificacao)
    t_mart_fato_acidentes = PythonOperator(task_id="carregar_marting_fato_acidentes", python_callable=carregar_marting_fato_acidentes)

    # catálogos e geografia são independentes entre si; ocorrências depende
    # dos dois (precisa do FK de classificação/vítima válido e do local já
    # resolvido)
    [t_corp_catalogos, t_corp_geografia] >> t_corp_ocorrencias
    t_corp_ocorrencias >> [t_mart_dim_local, t_mart_dim_classificacao] >> t_mart_fato_acidentes
