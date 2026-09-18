"""Helpers compartilhados pelas DAGs carga_inicial_dw e carga_incremental_dw.

Pipeline: datatran (OLTP) -> staging -> corporativo (DW normalizado, Inmon)
-> data_marting (estrela, Kimball). Os dims/fato do data_marting nunca são
lidos direto do datatran — só do corporativo, para não bater duas vezes no
banco transacional pela mesma linha.

Ao contrário do projeto de referência (DW_pos_atividades, Mercearia+Northwind
em bancos separados MySQL/Postgres), aqui há uma única fonte, e ela já é
Postgres — todas as conexões são psycopg2, sem pymysql.
"""
from __future__ import annotations

import psycopg2

COLUNAS_CLASSIFICACAO = [
    "causa_acidente", "tipo_acidente", "classificacao_acidente", "fase_dia",
    "sentido_via", "condicao_metereologica", "tipo_pista", "tracado_via", "uso_solo",
]

NAO_INFORMADO = "Não informado"


def get_pg_conn(dbname):
    return psycopg2.connect(host="postgres", port=5432, user="postgres", password="postgres", dbname=dbname)


def get_id_tempo(cur, data):
    """corporativo.tempos/dim_tempo cobrem 2000-01-01 a 2035-12-31 (gerados
    no DDL) — qualquer data do datatran cai nesse intervalo."""
    cur.execute("SELECT id_tempo FROM corporativo.tempos WHERE data = %s", (data,))
    return cur.fetchone()[0]


def get_id_dim_tempo(cur, data):
    cur.execute("SELECT id_dim_tempo FROM dim_tempo WHERE data = %s", (data,))
    return cur.fetchone()[0]


# ----------------------------------------------------------------
# corporativo — lookups simples de Geografia e Via (existem ou não pela
# chave natural; nenhuma delas precisa de histórico versionado, ver
# dw/dw_postgres.sql).
# ----------------------------------------------------------------

def get_or_create_uf(cur, sigla, nome):
    cur.execute(
        "INSERT INTO corporativo.ufs (sigla, nome) VALUES (%s, %s) ON CONFLICT (sigla) DO NOTHING RETURNING id_uf",
        (sigla, nome),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id_uf FROM corporativo.ufs WHERE sigla = %s", (sigla,))
    return cur.fetchone()[0]


def get_or_create_municipio(cur, nome, id_uf):
    cur.execute(
        "INSERT INTO corporativo.municipios (nome, id_uf) VALUES (%s, %s) "
        "ON CONFLICT (nome, id_uf) DO NOTHING RETURNING id_municipio",
        (nome, id_uf),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id_municipio FROM corporativo.municipios WHERE nome = %s AND id_uf = %s", (nome, id_uf))
    return cur.fetchone()[0]


def get_or_create_rodovia(cur, numero):
    cur.execute(
        "INSERT INTO corporativo.rodovias (numero) VALUES (%s) ON CONFLICT (numero) DO NOTHING RETURNING id_rodovia",
        (numero,),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id_rodovia FROM corporativo.rodovias WHERE numero = %s", (numero,))
    return cur.fetchone()[0]


def get_or_create_localizacao(cur, id_rodovia, km):
    cur.execute(
        "INSERT INTO corporativo.localizacoes (id_rodovia, km) VALUES (%s, %s) "
        "ON CONFLICT (id_rodovia, km) DO NOTHING RETURNING id_localizacao",
        (id_rodovia, km),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id_localizacao FROM corporativo.localizacoes WHERE id_rodovia = %s AND km = %s", (id_rodovia, km))
    return cur.fetchone()[0]


def get_or_create_local_acidente(cur, id_municipio, id_localizacao):
    cur.execute(
        "INSERT INTO corporativo.locais_acidente (id_municipio, id_localizacao) VALUES (%s, %s) "
        "ON CONFLICT (id_municipio, id_localizacao) DO NOTHING RETURNING id_local",
        (id_municipio, id_localizacao),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute(
        "SELECT id_local FROM corporativo.locais_acidente WHERE id_municipio = %s AND id_localizacao = %s",
        (id_municipio, id_localizacao),
    )
    return cur.fetchone()[0]


def resolver_local_acidente(cur, uf_sigla, uf_nome, municipio_nome, rodovia_numero, km):
    """Resolve a hierarquia inteira (uf -> municipio, rodovia -> localizacao
    -> local_acidente) a partir dos valores já achatados vindos do datatran
    (ou da staging). Retorna None se uf/br/km vierem NULL na fonte — mesmas 5
    ocorrências sem local que o OLTP já documenta (db/01_schema.sql)."""
    if uf_sigla is None or municipio_nome is None or rodovia_numero is None or km is None:
        return None
    id_uf = get_or_create_uf(cur, uf_sigla, uf_nome)
    id_municipio = get_or_create_municipio(cur, municipio_nome, id_uf)
    id_rodovia = get_or_create_rodovia(cur, rodovia_numero)
    id_localizacao = get_or_create_localizacao(cur, id_rodovia, km)
    return get_or_create_local_acidente(cur, id_municipio, id_localizacao)


# ----------------------------------------------------------------
# corporativo — cópia dos catálogos do OLTP (algoritmo 2 do metadados: o
# corporativo não inventa valor válido, só espelha o que o datatran já
# validou).
# ----------------------------------------------------------------

def copiar_categorias_vitima(oltp_cur, pg_cur):
    oltp_cur.execute("SELECT categoria, descricao FROM categoria_vitima")
    linhas = oltp_cur.fetchall()
    for categoria, descricao in linhas:
        pg_cur.execute(
            "INSERT INTO corporativo.categorias_vitima (categoria, descricao) VALUES (%s, %s) "
            "ON CONFLICT (categoria) DO UPDATE SET descricao = EXCLUDED.descricao",
            (categoria, descricao),
        )
    return len(linhas)


def copiar_tipos_classificacao(oltp_cur, pg_cur):
    oltp_cur.execute("SELECT tipo_atributo, descricao, obrigatorio FROM tipo_atributo")
    linhas = oltp_cur.fetchall()
    for tipo_atributo, descricao, obrigatorio in linhas:
        pg_cur.execute(
            "INSERT INTO corporativo.tipos_classificacao (tipo_classificacao, descricao, obrigatorio) "
            "VALUES (%s, %s, %s) ON CONFLICT (tipo_classificacao) DO UPDATE SET "
            "descricao = EXCLUDED.descricao, obrigatorio = EXCLUDED.obrigatorio",
            (tipo_atributo, descricao, obrigatorio),
        )
    return len(linhas)


def copiar_classificacoes_validas(oltp_cur, pg_cur):
    oltp_cur.execute("SELECT tipo_atributo, valor FROM atributo_valor_valido")
    linhas = oltp_cur.fetchall()
    for tipo_atributo, valor in linhas:
        pg_cur.execute(
            "INSERT INTO corporativo.classificacoes_validas (tipo_classificacao, valor) VALUES (%s, %s) "
            "ON CONFLICT (tipo_classificacao, valor) DO NOTHING",
            (tipo_atributo, valor),
        )
    return len(linhas)


# ----------------------------------------------------------------
# corporativo — fato operacional (ocorrencias / ocorrencia_vitima /
# ocorrencia_classificacao), sempre append-only.
# ----------------------------------------------------------------

def inserir_corporativo_ocorrencia(cur, id_origem, data, horario, id_local, veiculos):
    id_tempo = get_id_tempo(cur, data)
    cur.execute(
        "INSERT INTO corporativo.ocorrencias (id_ocorrencia_origem, id_tempo, horario, id_local, veiculos) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id_ocorrencia",
        (str(id_origem), id_tempo, horario, id_local, veiculos),
    )
    return cur.fetchone()[0]


def inserir_corporativo_vitima(cur, id_ocorrencia, categoria, quantidade):
    cur.execute(
        "INSERT INTO corporativo.ocorrencia_vitima (id_ocorrencia, categoria, quantidade) VALUES (%s, %s, %s)",
        (id_ocorrencia, categoria, quantidade),
    )


def inserir_corporativo_classificacao(cur, id_ocorrencia, tipo_classificacao, valor):
    cur.execute(
        "INSERT INTO corporativo.ocorrencia_classificacao (id_ocorrencia, tipo_classificacao, valor) "
        "VALUES (%s, %s, %s)",
        (id_ocorrencia, tipo_classificacao, valor),
    )


# ----------------------------------------------------------------
# data_marting — dims e fato sempre lidos do corporativo (nunca do datatran).
# ----------------------------------------------------------------

def refresh_dim_local(cur, id_local):
    """Desnormaliza município + UF + localização + rodovia numa única linha
    de dim_local (algoritmo 9 do metadados) — evita JOIN em tempo de consulta
    no BI. Conformada com corporativo.locais_acidente via id_local_original."""
    cur.execute("""
        SELECT m.nome, u.sigla, u.nome, r.numero, l.km
        FROM corporativo.locais_acidente la
        JOIN corporativo.municipios m ON m.id_municipio = la.id_municipio
        JOIN corporativo.ufs u ON u.id_uf = m.id_uf
        JOIN corporativo.localizacoes l ON l.id_localizacao = la.id_localizacao
        JOIN corporativo.rodovias r ON r.id_rodovia = l.id_rodovia
        WHERE la.id_local = %s
    """, (id_local,))
    r = cur.fetchone()
    if r is None:
        return
    nome_municipio, sigla_uf, nome_uf, numero_rodovia, km = r
    cur.execute("""
        INSERT INTO dim_local
            (sistema_origem, id_local_original, nome_municipio, sigla_uf, nome_uf, numero_rodovia, km)
        VALUES ('PRF-DATATRAN', %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sistema_origem, id_local_original) DO UPDATE SET
            nome_municipio = EXCLUDED.nome_municipio, sigla_uf = EXCLUDED.sigla_uf,
            nome_uf = EXCLUDED.nome_uf, numero_rodovia = EXCLUDED.numero_rodovia, km = EXCLUDED.km
    """, (str(id_local), nome_municipio, sigla_uf, nome_uf, numero_rodovia, km))


def get_or_create_classificacao(cur, valores):
    """Get-or-create da linha da dimensão junk cuja combinação das 9 flags
    bate com `valores` (algoritmo 7 do metadados). NULL vira 'Não informado'
    antes de comparar/inserir — Postgres trata NULL como distinto de NULL na
    UNIQUE, então duas ocorrências sem, por exemplo, condicao_metereologica
    criariam duas linhas "iguais" sem essa normalização."""
    vals = tuple((valores.get(c) or NAO_INFORMADO) for c in COLUNAS_CLASSIFICACAO)
    cols_sql = ", ".join(COLUNAS_CLASSIFICACAO)
    where = " AND ".join(f"{c} = %s" for c in COLUNAS_CLASSIFICACAO)

    cur.execute(f"SELECT id_dim_classificacao FROM dim_classificacao_acidente WHERE {where}", vals)
    r = cur.fetchone()
    if r:
        return r[0]

    placeholders = ", ".join(["%s"] * len(COLUNAS_CLASSIFICACAO))
    cur.execute(
        f"INSERT INTO dim_classificacao_acidente ({cols_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT ({cols_sql}) DO NOTHING RETURNING id_dim_classificacao",
        vals,
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute(f"SELECT id_dim_classificacao FROM dim_classificacao_acidente WHERE {where}", vals)
    return cur.fetchone()[0]
