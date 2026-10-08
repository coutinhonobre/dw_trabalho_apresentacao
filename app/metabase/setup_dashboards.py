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

# "Cluster 0"/"Cluster 1" não diz nada pra quem lê o painel, e o número que o
# KMeans atribui a cada cluster é arbitrário (pode trocar numa próxima
# rodada da DAG ml_clusterizacao_regras_associacao, nada garante que o
# cluster de maior letalidade continue sendo o "0"). Em vez de hardcodar um
# nome por número, calcula o rótulo toda vez que o card roda, ranqueando os
# clusters de cluster_municipio pela letalidade média (a métrica de risco
# mais direta) - sempre consistente com os dados atuais, mesmo se o próximo
# treino do KMeans inverter os números. CTE reaproveitada pelos 3 cards de
# ML (join por `cluster`, válido porque cluster_municipio/regra_associacao
# são gravados juntos no mesmo rodar_pipeline - ver
# ml/clusterizacao_regras_associacao.py:persistir_no_banco).
ROTULO_CLUSTER_CTE = """
WITH cluster_base AS (
    SELECT cluster, avg(taxa_letalidade) AS letal_media
    FROM cluster_municipio
    GROUP BY cluster
),
cluster_ranked AS (
    SELECT cluster, letal_media,
           row_number() OVER (ORDER BY letal_media DESC) AS pos,
           count(*) OVER () AS total
    FROM cluster_base
),
cluster_rotulo AS (
    SELECT cluster, pos,
           CASE
               WHEN total = 1 THEN 'Único grupo'
               WHEN total = 2 AND pos = 1 THEN 'Maior Letalidade'
               WHEN total = 2 AND pos = 2 THEN 'Menor Letalidade'
               WHEN pos = 1 THEN 'Letalidade Mais Alta'
               WHEN pos = total THEN 'Letalidade Mais Baixa'
               ELSE 'Letalidade Intermediária'
           END AS rotulo
    FROM cluster_ranked
)"""

# ----------------------------------------------------------------
# Filtros do dashboard (Período + UF): cada um é uma variável SQL nativa
# opcional ([[ ... ]] some se o parâmetro não for preenchido) mais um
# parâmetro no nível do dashboard, ligados um ao outro em build_dashboard()
# via parameter_mappings. Testado ao vivo contra um Metabase real antes de
# aplicar nos 15 cards - ver histórico do projeto.
# ----------------------------------------------------------------

# Paleta do painel (tema "problemática de acidentes" - abre com vermelho),
# validada com a skill de dataviz (rotação do palette default pra abrir em
# vermelho mantendo os 7 pares adjacentes já validados + 1 par novo
# vermelho-azul, checado à parte - CVD/contraste/normal-vision todos PASS
# nos dois modos, ver histórico do projeto). OSS Metabase não deixa recolorir
# globalmente (`application-colors` é feature paga "whitelabel" - testado,
# API recusa: "recurso :whitelabel não está disponível"), então aplicado por
# card via `series_settings`, que funciona na versão grátis.
COR_VERMELHO = "#e34948"
COR_LARANJA = "#eb6834"
COR_AZUL = "#2a78d6"
COR_AGUA = "#1baf7a"
COR_AMARELO = "#eda100"
COR_MAGENTA = "#e87ba4"
COR_VERDE = "#008300"
COR_VIOLETA = "#4a3aa7"


def cor_serie(nome_coluna, cor):
    """visualization_settings pra pintar a única série de um card bar/row/line
    de `nome_coluna` (a métrica) - mesma cor em todo card que mostra a mesma
    métrica (ex. "ocorrencias" sempre vermelho), pra reforçar identidade
    visual em vez de cor arbitrária por gráfico."""
    return {"series_settings": {nome_coluna: {"color": cor}}}


# Cores por rótulo de cluster_rotulo (ROTULO_CLUSTER_CTE acima) - vermelho/
# azul é o par diverging validado pela skill de dataviz (polos quente/frio
# que leem como opostos, em vez do clássico vermelho/verde, ruim pra
# daltonismo) - aqui faz sentido de verdade porque os rótulos SÃO uma
# polaridade (mais x menos letal), não categorias arbitrárias.
COR_ROTULO_CLUSTER = {
    "Único grupo": COR_VERMELHO,
    "Maior Letalidade": COR_VERMELHO,
    "Menor Letalidade": COR_AZUL,
    "Letalidade Mais Alta": COR_VERMELHO,
    "Letalidade Mais Baixa": COR_AZUL,
    "Letalidade Intermediária": COR_AMARELO,
}


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
            # Rampa sequencial vermelha (claro->escuro = menos->mais óbitos) -
            # chave certa é "map.colors" (array), não "color" (singular, que
            # a API aceita sem erro mas o renderer não lê - confirmado lendo
            # o bundle map-renderer.js do próprio Metabase, já que esse
            # setting não aparece documentado em lugar nenhum). Validado como
            # rampa sequencial pela skill de dataviz (monotonicidade de
            # luminosidade + mesma matiz) - o degrau mais claro quase sumir
            # no branco é esperado aqui (zero óbitos), diferente de uma
            # rampa ordinal.
            "map.colors": ["#fde0df", "#f2a6a3", "#e34948", "#a32f2e", "#6b1f1e"],
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
        "row", {"graph.dimensions": ["uf"], "graph.metrics": ["ocorrencias"], **cor_serie("ocorrencias", COR_LARANJA)}, FILTERS_PERIODO,
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
            **cor_serie("ocorrencias", COR_VERMELHO),
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
            **cor_serie("ocorrencias", COR_VERMELHO),
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
        "row", {"graph.dimensions": ["causa"], "graph.metrics": ["ocorrencias"], **cor_serie("ocorrencias", COR_VERMELHO)}, FILTERS_PERIODO_UF,
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
        "bar", {"graph.dimensions": ["condicao"], "graph.metrics": ["ocorrencias"], **cor_serie("ocorrencias", COR_AMARELO)}, FILTERS_PERIODO_UF,
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
        "pie", {
            "pie.dimension": "fase", "pie.metric": "ocorrencias",
            "pie.colors": {"Pleno dia": COR_AMARELO, "Plena Noite": COR_VIOLETA, "Amanhecer": COR_LARANJA, "Anoitecer": COR_MAGENTA},
        }, FILTERS_PERIODO_UF,
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
        "row", {"graph.dimensions": ["br"], "graph.metrics": ["obitos"], **cor_serie("obitos", COR_VERMELHO)}, FILTERS_PERIODO_UF,
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
        "pie", {
            "pie.dimension": "tipo", "pie.metric": "ocorrencias",
            "pie.colors": {"Simples": COR_VERMELHO, "Dupla": COR_AZUL, "Múltipla": COR_AGUA, "Não informado": COR_MAGENTA},
        }, FILTERS_PERIODO_UF,
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

    # ---- Camada 5 (ML): cluster_municipio / regra_associacao, gravadas pelo
    # DAG ml_clusterizacao_regras_associacao - sem os filtros Período/UF do
    # resto do painel (tabelas de resultado, não ligadas a dim_tempo/dim_local).
    # Cards aparecem vazios/com erro até o DAG rodar ao menos uma vez.
    cards["municipios_por_cluster"] = make_card(
        mb, database_id, collection_id, "Municípios por Cluster",
        ROTULO_CLUSTER_CTE + """
        SELECT cr.rotulo, count(*) AS municipios
        FROM cluster_municipio cm
        JOIN cluster_rotulo cr ON cr.cluster = cm.cluster
        GROUP BY cr.rotulo, cr.pos
        ORDER BY cr.pos
        """,
        # Só 1 dimensão (rotulo) + 1 métrica - Metabase pinta esse tipo de bar
        # chart com UMA cor pra série inteira (não por categoria do eixo x,
        # diferente de pie chart - confirmado nos outros gráficos de barra
        # single-dimension do painel, todos com barras na mesma cor), então
        # aqui é cor_serie("municipios", ...) mesmo, não COR_ROTULO_CLUSTER
        # (essa só se aplica onde "rotulo" é de fato uma 2a dimensão/série,
        # como em "perfil_risco_cluster" abaixo).
        "bar", {"graph.dimensions": ["rotulo"], "graph.metrics": ["municipios"], **cor_serie("municipios", COR_VIOLETA)}, [],
    )
    # Tabela larga (11 colunas decimais cru, 2 linhas) cortava na tela e era
    # difícil de comparar os clusters de cabeça - virou gráfico de barras
    # agrupado (1 barra por cluster, por métrica), só com as proporções
    # (0-1, mesma escala - por isso veiculos_medio/municipios/ocorrencias
    # ficaram de fora: unidades diferentes, não cabem no mesmo eixo).
    # round(...*100,1): mesmo padrão _pct das outras cards do painel
    # (ex. letalidade_pct em "Resumo por UF"), mas aqui como 2a dimensão
    # (metrica) em vez de uma coluna por métrica.
    cards["perfil_risco_cluster"] = make_card(
        mb, database_id, collection_id, "Perfil de Risco por Cluster (%)",
        ROTULO_CLUSTER_CTE + """
        SELECT x.metrica, cr.rotulo, x.valor_pct
        FROM (
            SELECT 1 AS ordem, 'Letalidade' AS metrica, cluster, round(100.0 * avg(taxa_letalidade), 1) AS valor_pct FROM cluster_municipio GROUP BY cluster
            UNION ALL
            SELECT 2, 'Feridos Graves', cluster, round(100.0 * avg(taxa_feridos_graves), 1) FROM cluster_municipio GROUP BY cluster
            UNION ALL
            SELECT 3, 'Fim de Semana', cluster, round(100.0 * avg(prop_fim_semana), 1) FROM cluster_municipio GROUP BY cluster
            UNION ALL
            SELECT 4, 'Período Noturno', cluster, round(100.0 * avg(prop_noite), 1) FROM cluster_municipio GROUP BY cluster
            UNION ALL
            SELECT 5, 'Chuva', cluster, round(100.0 * avg(prop_chuva), 1) FROM cluster_municipio GROUP BY cluster
            UNION ALL
            SELECT 6, 'Pista Simples', cluster, round(100.0 * avg(prop_pista_simples), 1) FROM cluster_municipio GROUP BY cluster
            UNION ALL
            SELECT 7, 'Concentração de Causa', cluster, round(100.0 * avg(concentracao_causa), 1) FROM cluster_municipio GROUP BY cluster
        ) x
        JOIN cluster_rotulo cr ON cr.cluster = x.cluster
        ORDER BY x.ordem, cr.pos
        """,
        # 2 dimensões (metrica + rotulo) - aqui "rotulo" é de fato a 2a série
        # (legenda/breakout) do gráfico agrupado, então series_settings por
        # VALOR de rotulo funciona (diferente de "municipios_por_cluster"
        # acima, que só tem 1 dimensão).
        "bar", {
            "graph.dimensions": ["metrica", "rotulo"], "graph.metrics": ["valor_pct"],
            "series_settings": {k: {"color": v} for k, v in COR_ROTULO_CLUSTER.items()},
        }, [],
    )
    cards["top_regras_por_cluster"] = make_card(
        mb, database_id, collection_id, "Top Regras de Associação por Cluster",
        ROTULO_CLUSTER_CTE + """
        SELECT cr.rotulo, ra.antecedente, ra.consequente, ra.suporte_pct, ra.confianca_pct, ra.lift
        FROM (
            SELECT
                cluster, antecedente, consequente,
                round(suporte * 100, 1) AS suporte_pct,
                round(confianca * 100, 1) AS confianca_pct,
                round(lift, 2) AS lift,
                row_number() OVER (PARTITION BY cluster ORDER BY lift DESC) AS posicao
            FROM regra_associacao
        ) ra
        JOIN cluster_rotulo cr ON cr.cluster = ra.cluster
        WHERE ra.posicao <= 5
        ORDER BY cr.pos, ra.lift DESC
        """,
        "table", {}, [],
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
        # linha 6: ML - clusterização de municípios + regras de associação
        dashcard(-16, cards["municipios_por_cluster"], 42, 0, 8, 8),
        dashcard(-17, cards["perfil_risco_cluster"], 42, 8, 16, 8),
        dashcard(-18, cards["top_regras_por_cluster"], 50, 0, 24, 10),
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
