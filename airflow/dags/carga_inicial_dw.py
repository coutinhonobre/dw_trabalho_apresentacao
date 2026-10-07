"""Primeira carga do Data Warehouse.

Pipeline em duas camadas:
1. corporativo (DW normalizado, Inmon): lê `datatran` (OLTP, Postgres) direto,
   copia os catálogos (categorias de vítima, os 8 catálogos de
   classificação), resolve a hierarquia de geografia/via e grava o fato
   operacional (ocorrencias/ocorrencia_vitima/ocorrencia_tracado_via) - as 8
   classificações vão direto como colunas de `ocorrencias`.
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
    TAMANHO_LOTE,
    carregar_mapa_local_acidente,
    carregar_mapa_tempo,
    copiar_catalogos_classificacao,
    copiar_categorias_vitima,
    copiar_tracados_via_validos,
    get_or_create_classificacao,
    get_pg_conn,
    inserir_corporativo_ocorrencia,
    inserir_corporativo_tracado_via,
    inserir_corporativo_vitima,
    refresh_dim_local,
    resolver_geografia_em_lote,
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
            n_classificacoes = copiar_catalogos_classificacao(oltp_cur, pg_cur)
            n_tracados = copiar_tracados_via_validos(oltp_cur, pg_cur)
        pg_conn.commit()
        print(f"corporativo catálogos: {n_categorias} categorias_vitima, {n_classificacoes} valores "
              f"nos 8 catálogos de classificação, {n_tracados} tracados_via_validos")
    finally:
        oltp_conn.close()
        pg_conn.close()


def carregar_corporativo_geografia():
    """Resolve toda a hierarquia (uf -> municipio, rodovia -> localizacao ->
    local_acidente) a partir das combinações distintas já usadas por
    local_acidente no datatran — feito antes da carga das ocorrências pra não
    disputar a criação da mesma uf/município em paralelo dentro da mesma
    ocorrência. Resolução em lote (resolver_geografia_em_lote) em vez de 1
    round-trip por combinação - com 368k+ combinações no dataset completo, o
    loop antigo levava mais de 1h só nesta etapa."""
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
            resolver_geografia_em_lote(cur, combos)
        pg_conn.commit()
        print(f"corporativo.locais_acidente: {len(combos)} combinações processadas")
    finally:
        oltp_conn.close()
        pg_conn.close()


def carregar_corporativo_ocorrencias():
    """Processa `acidentes` (2,2M+ linhas nos 20 anos de dataset) em lotes de
    TAMANHO_LOTE, paginando por `a.id` (mesmo watermark que
    carga_incremental_dw.py usa pro delta) em vez de um fetchall() da tabela
    inteira - ler tudo de uma vez (+ os dicts de pivot de vítima/traçado) pro
    dataset completo já causou OOM na task do Airflow. Cada lote resolve
    vítimas/traçados só dos ids daquele lote (WHERE ... = ANY(%s)) e comita
    sua própria transação.

    id_tempo/id_local são resolvidos por lookup num mapa carregado uma vez só
    (carregar_mapa_tempo/carregar_mapa_local_acidente) em vez de 1 (ou até 5)
    round-trip(s) por ocorrência - geografia e calendário já estão totalmente
    resolvidos nas etapas anteriores da DAG, então isso nunca cria nada
    novo, só consulta."""
    oltp_conn = get_pg_conn("datatran")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            # carga inicial: recria o fato do zero (idêntico ao padrão do
            # projeto de referência para vendas/compras)
            cur.execute("DELETE FROM corporativo.ocorrencia_vitima")
            cur.execute("DELETE FROM corporativo.ocorrencia_tracado_via")
            cur.execute("DELETE FROM corporativo.ocorrencias")
            mapa_tempo = carregar_mapa_tempo(cur)
            mapa_local = carregar_mapa_local_acidente(cur)
        pg_conn.commit()

        cols_classificacao = ", ".join(f"a.{c}" for c in COLUNAS_CLASSIFICACAO)
        total = 0
        ultimo_id = 0
        while True:
            with oltp_conn.cursor() as cur:
                cur.execute(f"""
                    SELECT a.id, a.data, a.horario, a.veiculos,
                           u.sigla, u.nome, m.nome, r.numero, loc.km,
                           {cols_classificacao}
                    FROM acidentes a
                    LEFT JOIN local_acidente la ON la.id = a.local_id
                    LEFT JOIN municipio m ON m.id = la.municipio_id
                    LEFT JOIN uf u ON u.sigla = m.uf_sigla
                    LEFT JOIN localizacao loc ON loc.id = la.localizacao_id
                    LEFT JOIN rodovia r ON r.numero = loc.rodovia_numero
                    WHERE a.id > %s
                    ORDER BY a.id
                    LIMIT {TAMANHO_LOTE}
                """, (ultimo_id,))
                lote = cur.fetchall()
            if not lote:
                break
            ids_lote = [row[0] for row in lote]
            ultimo_id = ids_lote[-1]

            with oltp_conn.cursor() as cur:
                cur.execute(
                    "SELECT acidente_id, categoria, quantidade FROM acidente_vitima WHERE acidente_id = ANY(%s)",
                    (ids_lote,),
                )
                vitimas = cur.fetchall()
                cur.execute(
                    "SELECT acidente_id, valor FROM acidente_tracado_via WHERE acidente_id = ANY(%s)",
                    (ids_lote,),
                )
                tracados = cur.fetchall()

            vitimas_por_acidente = {}
            for aid, categoria, quantidade in vitimas:
                vitimas_por_acidente.setdefault(aid, []).append((categoria, quantidade))
            tracados_por_acidente = {}
            for aid, valor in tracados:
                tracados_por_acidente.setdefault(aid, []).append(valor)

            with pg_conn.cursor() as cur:
                for row in lote:
                    oid, data, horario, veiculos, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km = row[:9]
                    classificacao = dict(zip(COLUNAS_CLASSIFICACAO, row[9:]))
                    id_tempo = mapa_tempo[data]
                    id_local = mapa_local.get((uf_sigla, municipio_nome, rodovia_numero, km))
                    id_ocorrencia = inserir_corporativo_ocorrencia(cur, oid, id_tempo, horario, id_local, veiculos, classificacao)
                    for categoria, quantidade in vitimas_por_acidente.get(oid, []):
                        inserir_corporativo_vitima(cur, id_ocorrencia, categoria, quantidade)
                    for valor in tracados_por_acidente.get(oid, []):
                        inserir_corporativo_tracado_via(cur, id_ocorrencia, valor)
            pg_conn.commit()
            total += len(lote)
            print(f"corporativo.ocorrencias: lote até id={ultimo_id} ({total} ocorrências carregadas até agora)")
        print(f"corporativo.ocorrencias: {total} ocorrências carregadas (total)")
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
    """Mesmo motivo de lote de carregar_corporativo_ocorrencias:
    corporativo.ocorrencias já tem a escala do dataset completo (2,2M+
    linhas) - paginar por id_ocorrencia em vez de fetchall() da tabela
    inteira."""
    pg_conn = get_pg_conn("dw")
    try:
        cols_classificacao = ", ".join(COLUNAS_CLASSIFICACAO)
        vistos = set()
        total = 0
        ultimo_id = 0
        while True:
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"SELECT id_ocorrencia, {cols_classificacao} FROM corporativo.ocorrencias "
                    f"WHERE id_ocorrencia > %s ORDER BY id_ocorrencia LIMIT {TAMANHO_LOTE}",
                    (ultimo_id,),
                )
                lote = cur.fetchall()
            if not lote:
                break
            ultimo_id = lote[-1][0]
            total += len(lote)

            with pg_conn.cursor() as cur:
                for linha in lote:
                    valores = dict(zip(COLUNAS_CLASSIFICACAO, linha[1:]))
                    chave = tuple((valores.get(c) or NAO_INFORMADO) for c in COLUNAS_CLASSIFICACAO)
                    if chave in vistos:
                        continue
                    vistos.add(chave)
                    get_or_create_classificacao(cur, valores)
            pg_conn.commit()
        print(f"dim_classificacao_acidente: {len(vistos)} combinações distintas processadas ({total} ocorrências lidas)")
    finally:
        pg_conn.close()


def carregar_marting_fato_acidentes():
    """Mesmo motivo de lote das duas funções acima - corporativo.ocorrencias/
    ocorrencia_vitima/ocorrencia_tracado_via na escala do dataset completo
    (2,2M+ linhas). tempo_map/local_map são pequenos (1 linha por dia/local)
    e continuam carregados uma vez só; o resto pagina por id_ocorrencia."""
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM fato_acidente_tracado_via")
            cur.execute("DELETE FROM fato_acidentes")
            cur.execute("SELECT t.id_tempo, dt.id_dim_tempo FROM corporativo.tempos t JOIN dim_tempo dt ON dt.data = t.data")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_local_original, id_dim_local FROM dim_local WHERE sistema_origem = 'PRF-DATATRAN'")
            local_map = {r[0]: r[1] for r in cur.fetchall()}
        pg_conn.commit()

        cols_classificacao = ", ".join(COLUNAS_CLASSIFICACAO)
        cache_classificacao = {}
        total_fato = 0
        total_bridge = 0
        ultimo_id = 0
        while True:
            with pg_conn.cursor() as cur:
                cur.execute(
                    f"SELECT id_ocorrencia, id_ocorrencia_origem, id_tempo, id_local, veiculos, {cols_classificacao} "
                    f"FROM corporativo.ocorrencias WHERE id_ocorrencia > %s ORDER BY id_ocorrencia LIMIT {TAMANHO_LOTE}",
                    (ultimo_id,),
                )
                lote = cur.fetchall()
            if not lote:
                break
            ids_lote = [r[0] for r in lote]
            ultimo_id = ids_lote[-1]

            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT id_ocorrencia, categoria, quantidade FROM corporativo.ocorrencia_vitima WHERE id_ocorrencia = ANY(%s)",
                    (ids_lote,),
                )
                vitima_rows = cur.fetchall()
                cur.execute(
                    "SELECT id_ocorrencia, valor FROM corporativo.ocorrencia_tracado_via WHERE id_ocorrencia = ANY(%s)",
                    (ids_lote,),
                )
                tracado_rows = cur.fetchall()

            vitimas_por_ocorrencia = {}
            for id_ocorrencia, categoria, quantidade in vitima_rows:
                vitimas_por_ocorrencia.setdefault(id_ocorrencia, {})[categoria] = quantidade
            tracado_por_ocorrencia = {}
            for id_ocorrencia, valor in tracado_rows:
                tracado_por_ocorrencia.setdefault(id_ocorrencia, []).append(valor)

            rows = []
            ids_ocorrencia_por_linha = []
            with pg_conn.cursor() as cur:
                for linha in lote:
                    id_ocorrencia, id_origem, id_tempo, id_local, veiculos = linha[:5]
                    valores_classificacao = dict(zip(COLUNAS_CLASSIFICACAO, linha[5:]))
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
                    ids_ocorrencia_por_linha.append(id_ocorrencia)

                # fetch=True + RETURNING: o Postgres devolve as linhas na MESMA
                # ordem do VALUES de entrada - zip direto com ids_ocorrencia_por_linha
                # pra mapear id_ocorrencia -> id_fato_acidente sem round-trip extra.
                ids_fato = psycopg2.extras.execute_values(cur, """
                    INSERT INTO fato_acidentes
                        (sistema_origem, id_ocorrencia_original, id_dim_tempo, id_dim_local, id_dim_classificacao,
                         veiculos, mortos, feridos_leves, feridos_graves, ilesos, ignorados, feridos, pessoas)
                    VALUES %s
                    RETURNING id_fato_acidente
                """, rows, fetch=True)

                bridge_rows = []
                for id_ocorrencia, (id_fato_acidente,) in zip(ids_ocorrencia_por_linha, ids_fato):
                    for valor in tracado_por_ocorrencia.get(id_ocorrencia, []):
                        bridge_rows.append((id_fato_acidente, valor))
                if bridge_rows:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO fato_acidente_tracado_via (id_fato_acidente, valor) VALUES %s
                    """, bridge_rows)
            pg_conn.commit()
            total_fato += len(rows)
            total_bridge += len(bridge_rows)
        print(f"fato_acidentes: {total_fato} linhas carregadas a partir do corporativo "
              f"({total_bridge} linhas em fato_acidente_tracado_via)")
    finally:
        pg_conn.close()


with DAG(
    dag_id="carga_inicial_dw",
    description="Primeira carga: datatran -> corporativo (DW normalizado, Inmon) -> data_marting (estrela, Kimball)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,  # disparado por bootstrap_carga_e_ml, que falha se este DAG estiver pausado
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
