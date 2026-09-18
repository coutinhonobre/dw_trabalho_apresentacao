"""Provisiona o Metabase via API: usuário admin, conexão com o banco `dw`,
mapa customizado dos estados do Brasil e o dashboard "Painel PRF - Acidentes
de Trânsito", com queries nativas sobre o data mart em estrela
(fato_acidentes/dim_tempo/dim_local/dim_classificacao_acidente) e dois
filtros de dashboard (Período e UF) encadeados via variáveis SQL nativas.

Roda uma vez (serviço `metabase-setup` do docker-compose, sem restart) e é
idempotente: se o dashboard já existe, não recria nada.
"""
from __future__ import annotations

import sys
import time

import requests

MB_URL = "http://metabase:3000"
# E-mails com domínio genérico de propósito (não usa gov.br - domínio real
# do governo brasileiro - nem referencia "PRF" na senha, pra não misturar
# credencial fictícia de dev com o nome de uma instituição real).
ADMIN_EMAIL = "admin@metabase.com"
ADMIN_PASSWORD = "Metabase123!"
# Usuário padrão (não-admin) para uso do dia a dia — só visualiza os painéis,
# não administra o Metabase nem gerencia conexões/usuários. Credenciais
# simples de propósito (mesmo padrão de postgres/postgres e airflow/airflow
# usado no resto do projeto); o Metabase exige e-mail como login, então
# "metabase" vira o local-part do e-mail.
STANDARD_EMAIL = "metabase@metabase.com"
STANDARD_PASSWORD = "metabase"
DB_NAME = "DW - PRF Acidentes"
COLLECTION_NAME = "PRF - Acidentes de Trânsito"
DASHBOARD_NAME = "Painel PRF - Acidentes de Trânsito"

BRAZIL_STATES_GEOJSON_URL = (
    "https://raw.githubusercontent.com/codeforgermany/click_that_hood/"
    "main/public/data/brazil-states.geojson"
)

DIA_SEMANA_CASE = """CASE dt.dia_semana
        WHEN 'Monday' THEN 'Segunda'
        WHEN 'Tuesday' THEN 'Terça'
        WHEN 'Wednesday' THEN 'Quarta'
        WHEN 'Thursday' THEN 'Quinta'
        WHEN 'Friday' THEN 'Sexta'
        WHEN 'Saturday' THEN 'Sábado'
        WHEN 'Sunday' THEN 'Domingo'
    END"""

DIA_SEMANA_ORDER = """CASE dt.dia_semana
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END"""

# dim_tempo.nome_mes vem de TO_CHAR(..., 'TMMonth'), que depende do locale do
# Postgres do container (normalmente en_US) - traduzido aqui a partir do
# número do mês para não depender de locale.
MES_CASE = """CASE dt.mes
        WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
    END"""

# ----------------------------------------------------------------
# Filtros do dashboard (Período + UF): cada um é uma variável SQL nativa
# opcional ([[ ... ]] some se o parâmetro não for preenchido) mais um
# parâmetro no nível do dashboard, ligados um ao outro em build_dashboard()
# via parameter_mappings. Testado ao vivo contra um Metabase real antes de
# aplicar nos 15 cards - ver histórico do projeto.
# ----------------------------------------------------------------

FILTER_PERIODO_SQL = "[[AND dt.data >= {{data_inicio}}]] [[AND dt.data <= {{data_fim}}]]"
FILTER_UF_SQL = "[[AND dl.sigla_uf = {{uf}}]]"

TAG_DEFS = {
    "data_inicio": {"id": "tag-data-inicio", "name": "data_inicio", "display-name": "Data inicial", "type": "date"},
    "data_fim": {"id": "tag-data-fim", "name": "data_fim", "display-name": "Data final", "type": "date"},
    "uf": {"id": "tag-uf", "name": "uf", "display-name": "UF", "type": "text"},
}

PARAM_DEFS = {
    "data_inicio": {"id": "param-data-inicio", "name": "Data inicial", "slug": "data_inicio", "type": "date/single"},
    "data_fim": {"id": "param-data-fim", "name": "Data final", "slug": "data_fim", "type": "date/single"},
    "uf": {"id": "param-uf", "name": "UF", "slug": "uf", "type": "string/="},
}

# Filtros que se aplicam a cada card. Mapa e ranking de UF ficam de fora do
# filtro de UF de propósito - filtrar "ocorrências por UF" por uma única UF
# específica colapsaria o gráfico a 1 linha, o que não agrega nada.
FILTERS_PERIODO = ["data_inicio", "data_fim"]
FILTERS_PERIODO_UF = ["data_inicio", "data_fim", "uf"]


def tags_for(keys):
    return {k: TAG_DEFS[k] for k in keys}


def wait_ready():
    print("Aguardando Metabase ficar pronto...", flush=True)
    for _ in range(120):
        try:
            r = requests.get(f"{MB_URL}/api/health", timeout=3)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise SystemExit("Metabase não ficou pronto a tempo")


def get_session() -> str:
    """Faz o setup inicial (1a vez) ou login (reexecuções) e devolve o
    token de sessão."""
    props = requests.get(f"{MB_URL}/api/session/properties", timeout=10).json()
    setup_token = props.get("setup-token")

    if setup_token:
        print("Rodando setup inicial do Metabase...", flush=True)
        r = requests.post(f"{MB_URL}/api/setup", json={
            "token": setup_token,
            "user": {
                "first_name": "Admin",
                "last_name": "PRF",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
            "prefs": {
                "site_name": "PRF Analytics",
                "site_locale": "pt-BR",
                "allow_tracking": False,
            },
        }, timeout=30)
        r.raise_for_status()
        return r.json()["id"]

    print("Setup já realizado antes, autenticando...", flush=True)
    r = requests.post(f"{MB_URL}/api/session", json={
        "username": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def set_brazil_states_map(session: "MB"):
    print("Registrando mapa dos estados do Brasil...", flush=True)
    session.put("/api/setting/custom-geojson", json={
        "value": {
            "brazil_states": {
                "name": "Brasil (Estados)",
                "url": BRAZIL_STATES_GEOJSON_URL,
                "region_key": "sigla",
                "region_name": "name",
                "builtin": False,
            }
        }
    })


def get_or_create_standard_user(session: "MB"):
    """Cria o usuário padrão (grupo 'All Users', sem privilégio de admin) se
    ainda não existir. Ele entra automaticamente no grupo 'All Users', que já
    tem acesso de leitura à coleção raiz (onde a coleção do painel é criada)
    - não precisa de permissão extra."""
    users = session.get("/api/user")["data"]
    if any(u["email"] == STANDARD_EMAIL for u in users):
        print(f"Usuário padrão '{STANDARD_EMAIL}' já existe.", flush=True)
        return
    print(f"Criando usuário padrão '{STANDARD_EMAIL}'...", flush=True)
    session.post("/api/user", json={
        "first_name": "Analista",
        "last_name": "PRF",
        "email": STANDARD_EMAIL,
        "password": STANDARD_PASSWORD,
    })


def get_or_create_collection(session: "MB") -> int:
    collections = session.get("/api/collection")
    for c in collections:
        if c["name"] == COLLECTION_NAME:
            return c["id"]
    c = session.post("/api/collection", json={
        "name": COLLECTION_NAME,
        "description": "Painéis de BI sobre o data mart de acidentes de trânsito (PRF).",
        "color": "#509EE3",
    })
    return c["id"]


class MB:
    def __init__(self, base_url: str, session_token: str):
        self.base_url = base_url
        self.headers = {"X-Metabase-Session": session_token, "Content-Type": "application/json"}

    def get(self, path):
        r = requests.get(f"{self.base_url}{path}", headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path, json):
        r = requests.post(f"{self.base_url}{path}", headers=self.headers, json=json, timeout=60)
        r.raise_for_status()
        return r.json()

    def put(self, path, json):
        r = requests.put(f"{self.base_url}{path}", headers=self.headers, json=json, timeout=60)
        r.raise_for_status()
        if r.text:
            return r.json()
        return None


def make_card(mb: MB, database_id: int, collection_id: int, name: str, sql: str, display: str, viz: dict, filters: list) -> dict:
    native = {"query": sql}
    if filters:
        native["template-tags"] = tags_for(filters)
    card = mb.post("/api/card", json={
        "name": name,
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "native": native,
            "database": database_id,
        },
        "display": display,
        "visualization_settings": viz,
    })
    return {"id": card["id"], "filters": filters}


def build_cards(mb: MB, database_id: int, collection_id: int) -> dict:
    cards = {}

    cards["total_ocorrencias"] = make_card(
        mb, database_id, collection_id, "Total de Ocorrências",
        f"""
        SELECT count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_UF,
    )
    cards["total_obitos"] = make_card(
        mb, database_id, collection_id, "Total de Óbitos",
        f"""
        SELECT sum(fa.mortos) AS obitos
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_UF,
    )
    cards["total_feridos"] = make_card(
        mb, database_id, collection_id, "Total de Feridos",
        f"""
        SELECT sum(fa.feridos) AS feridos
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_UF,
    )
    cards["total_veiculos"] = make_card(
        mb, database_id, collection_id, "Veículos Envolvidos",
        f"""
        SELECT sum(fa.veiculos) AS veiculos
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_UF,
    )
    cards["taxa_letalidade"] = make_card(
        mb, database_id, collection_id, "Letalidade (óbitos / pessoas envolvidas)",
        f"""
        SELECT round(100.0 * sum(fa.mortos) / NULLIF(sum(fa.pessoas), 0), 2) AS letalidade_pct
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_UF,
    )

    cards["mapa_obitos_uf"] = make_card(
        mb, database_id, collection_id, "Mapa: Óbitos por UF",
        f"""
        SELECT dl.sigla_uf AS uf, sum(fa.mortos) AS obitos
        FROM fato_acidentes fa
        JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY dl.sigla_uf
        """,
        "map", {
            "map.type": "region",
            "map.region": "brazil_states",
            "map.metric_column": "obitos",
            "map.dimension_column": "uf",
        }, FILTERS_PERIODO,
    )
    cards["top10_uf_ocorrencias"] = make_card(
        mb, database_id, collection_id, "Top 10 UF por Ocorrências",
        f"""
        SELECT dl.sigla_uf AS uf, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY dl.sigla_uf
        ORDER BY ocorrencias DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["uf"], "graph.metrics": ["ocorrencias"]}, FILTERS_PERIODO,
    )

    cards["ocorrencias_por_mes"] = make_card(
        mb, database_id, collection_id, "Ocorrências por Mês",
        f"""
        SELECT {MES_CASE} AS mes, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dt.mes
        ORDER BY dt.mes
        """,
        "line", {
            "graph.dimensions": ["mes"], "graph.metrics": ["ocorrencias"],
            "graph.x_axis.scale": "ordinal",
        }, FILTERS_PERIODO_UF,
    )
    cards["ocorrencias_por_dia_semana"] = make_card(
        mb, database_id, collection_id, "Ocorrências por Dia da Semana",
        f"""
        SELECT {DIA_SEMANA_CASE} AS dia_semana, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dt.dia_semana
        ORDER BY {DIA_SEMANA_ORDER}
        """,
        "bar", {
            "graph.dimensions": ["dia_semana"], "graph.metrics": ["ocorrencias"],
            "graph.x_axis.scale": "ordinal",
        }, FILTERS_PERIODO_UF,
    )

    cards["top_causas"] = make_card(
        mb, database_id, collection_id, "Top 10 Causas de Acidente",
        f"""
        SELECT dc.causa_acidente AS causa, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_classificacao_acidente dc ON dc.id_dim_classificacao = fa.id_dim_classificacao
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE dc.causa_acidente <> 'Não informado' {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dc.causa_acidente
        ORDER BY ocorrencias DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["causa"], "graph.metrics": ["ocorrencias"]}, FILTERS_PERIODO_UF,
    )
    cards["condicao_metereologica"] = make_card(
        mb, database_id, collection_id, "Acidentes por Condição Meteorológica",
        f"""
        SELECT dc.condicao_metereologica AS condicao, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_classificacao_acidente dc ON dc.id_dim_classificacao = fa.id_dim_classificacao
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dc.condicao_metereologica
        ORDER BY ocorrencias DESC
        """,
        "bar", {"graph.dimensions": ["condicao"], "graph.metrics": ["ocorrencias"]}, FILTERS_PERIODO_UF,
    )
    cards["fase_dia"] = make_card(
        mb, database_id, collection_id, "Acidentes por Fase do Dia",
        f"""
        SELECT dc.fase_dia AS fase, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_classificacao_acidente dc ON dc.id_dim_classificacao = fa.id_dim_classificacao
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dc.fase_dia
        ORDER BY ocorrencias DESC
        """,
        "pie", {"pie.dimension": "fase", "pie.metric": "ocorrencias"}, FILTERS_PERIODO_UF,
    )

    cards["top_rodovias_obitos"] = make_card(
        mb, database_id, collection_id, "Top 10 Rodovias (BR) por Óbitos",
        f"""
        SELECT dl.numero_rodovia AS br, sum(fa.mortos) AS obitos
        FROM fato_acidentes fa
        JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dl.numero_rodovia
        ORDER BY obitos DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["br"], "graph.metrics": ["obitos"]}, FILTERS_PERIODO_UF,
    )
    cards["tipo_pista"] = make_card(
        mb, database_id, collection_id, "Acidentes por Tipo de Pista",
        f"""
        SELECT dc.tipo_pista AS tipo, count(*) AS ocorrencias
        FROM fato_acidentes fa
        JOIN dim_classificacao_acidente dc ON dc.id_dim_classificacao = fa.id_dim_classificacao
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        LEFT JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dc.tipo_pista
        ORDER BY ocorrencias DESC
        """,
        "pie", {"pie.dimension": "tipo", "pie.metric": "ocorrencias"}, FILTERS_PERIODO_UF,
    )

    cards["resumo_uf"] = make_card(
        mb, database_id, collection_id, "Resumo por UF",
        f"""
        SELECT
            dl.sigla_uf AS uf, dl.nome_uf AS estado,
            count(*) AS ocorrencias,
            sum(fa.mortos) AS obitos,
            sum(fa.feridos) AS feridos,
            sum(fa.veiculos) AS veiculos,
            round(100.0 * sum(fa.mortos) / NULLIF(sum(fa.pessoas), 0), 2) AS letalidade_pct
        FROM fato_acidentes fa
        JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_UF_SQL}
        GROUP BY dl.sigla_uf, dl.nome_uf
        ORDER BY ocorrencias DESC
        """,
        "table", {}, FILTERS_PERIODO_UF,
    )

    return cards


def dashcard(id_, card, row, col, size_x, size_y):
    entry = {"id": id_, "card_id": card["id"], "row": row, "col": col, "size_x": size_x, "size_y": size_y}
    if card["filters"]:
        entry["parameter_mappings"] = [
            {
                "parameter_id": PARAM_DEFS[f]["id"],
                "card_id": card["id"],
                "target": ["variable", ["template-tag", f]],
            }
            for f in card["filters"]
        ]
    return entry


def build_dashboard(mb: MB, collection_id: int, cards: dict) -> int:
    dash = mb.post("/api/dashboard", json={
        "name": DASHBOARD_NAME,
        "collection_id": collection_id,
        "description": (
            "Painel operacional de acidentes de trânsito em rodovias federais "
            "(PRF) - visão geográfica, temporal e por causa/condição. Filtros: "
            "Período e UF, no topo do painel."
        ),
    })
    dash_id = dash["id"]

    # grade de 24 colunas (padrão do Metabase)
    dashcards = [
        # linha 0: KPIs
        dashcard(-1, cards["total_ocorrencias"], 0, 0, 6, 3),
        dashcard(-2, cards["total_obitos"], 0, 6, 6, 3),
        dashcard(-3, cards["total_feridos"], 0, 12, 6, 3),
        dashcard(-4, cards["total_veiculos"], 0, 18, 6, 3),
        # linha 1: letalidade + mapa + ranking UF
        dashcard(-5, cards["taxa_letalidade"], 3, 0, 4, 8),
        dashcard(-6, cards["mapa_obitos_uf"], 3, 4, 12, 8),
        dashcard(-7, cards["top10_uf_ocorrencias"], 3, 16, 8, 8),
        # linha 2: temporal
        dashcard(-8, cards["ocorrencias_por_mes"], 11, 0, 12, 7),
        dashcard(-9, cards["ocorrencias_por_dia_semana"], 11, 12, 12, 7),
        # linha 3: causas e condições
        dashcard(-10, cards["top_causas"], 18, 0, 8, 7),
        dashcard(-11, cards["condicao_metereologica"], 18, 8, 8, 7),
        dashcard(-12, cards["fase_dia"], 18, 16, 8, 7),
        # linha 4: via
        dashcard(-13, cards["top_rodovias_obitos"], 25, 0, 12, 7),
        dashcard(-14, cards["tipo_pista"], 25, 12, 12, 7),
        # linha 5: tabela detalhada
        dashcard(-15, cards["resumo_uf"], 32, 0, 24, 10),
    ]
    mb.put(f"/api/dashboard/{dash_id}", json={
        "dashcards": dashcards,
        "parameters": list(PARAM_DEFS.values()),
    })
    return dash_id


def main():
    wait_ready()
    token = get_session()
    mb = MB(MB_URL, token)

    get_or_create_standard_user(mb)

    dashboards = mb.get("/api/dashboard")
    existing = [d for d in dashboards if d["name"] == DASHBOARD_NAME]
    if existing:
        print(f"Dashboard '{DASHBOARD_NAME}' já existe (id={existing[0]['id']}). Nada a fazer.", flush=True)
        print(f"Acesse: http://localhost:3000/dashboard/{existing[0]['id']}", flush=True)
        return

    dbs = mb.get("/api/database")["data"]
    db_id = next((d["id"] for d in dbs if d["name"] == DB_NAME), None)
    if db_id is None:
        print(f"Criando conexão com o banco '{DB_NAME}'...", flush=True)
        db = mb.post("/api/database", json={
            "engine": "postgres",
            "name": DB_NAME,
            "details": {
                "host": "postgres",
                "port": 5432,
                "dbname": "dw",
                "user": "postgres",
                "password": "postgres",
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": "public",
            },
            "is_full_sync": True,
        })
        db_id = db["id"]
        print("Aguardando sincronização do schema...", flush=True)
        for _ in range(60):
            info = mb.get(f"/api/database/{db_id}")
            if info.get("initial_sync_status") == "complete":
                break
            time.sleep(3)
    else:
        print(f"Banco '{DB_NAME}' já existe (id={db_id}).", flush=True)

    set_brazil_states_map(mb)

    collection_id = get_or_create_collection(mb)
    print(f"Coleção '{COLLECTION_NAME}' (id={collection_id}).", flush=True)

    print("Criando as consultas (cards) do painel...", flush=True)
    cards = build_cards(mb, db_id, collection_id)

    print("Montando o dashboard...", flush=True)
    dash_id = build_dashboard(mb, collection_id, cards)

    print("=" * 70, flush=True)
    print(f"Painel pronto: http://localhost:3000/dashboard/{dash_id}", flush=True)
    print(f"Login padrão (visualização):  {STANDARD_EMAIL} / {STANDARD_PASSWORD}", flush=True)
    print(f"Login admin (administração):  {ADMIN_EMAIL} / {ADMIN_PASSWORD}", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"Erro HTTP: {e.response.status_code} {e.response.text}", file=sys.stderr)
        raise
