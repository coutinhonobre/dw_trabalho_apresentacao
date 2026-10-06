#!/usr/bin/env python3
"""
Clusterização de "clientes" + Regras de Associação por grupo, sobre o data
mart em estrela (`fato_acidentes`/`dim_local`/`dim_tempo`/
`dim_classificacao_acidente`, banco `dw`, schema `public`).

Não há clientes/produtos no domínio PRF-DATATRAN, então o enunciado é
adaptado ao dataset disponível:

  - "Cliente" = município (nome_municipio + sigla_uf) - a unidade geográfica
    que concentra ocorrências. Cada município ganha um perfil de risco
    (letalidade, gravidade, volume, condições predominantes) e é agrupado
    por KMeans.
  - "Produto" = cada classificação de uma ocorrência (causa, tipo de
    acidente, condição meteorológica, fase do dia, tipo de pista, traçado da
    via, uso do solo, sentido da via, classificação quanto a vítimas) - uma
    ocorrência é uma "cesta" com até 9 itens, um por tipo de classificação.

Pipeline (ensemble clusterização + regras de associação):
  1. Carrega o grão do fato (1 linha = 1 ocorrência) com município e as 9
     classificações.
  2. Agrega por município -> features de perfil de risco -> KMeans (com
     escolha de k por silhueta) -> cada município cai em um cluster.
  3. Para cada cluster, agrupa as cestas das ocorrências dos municípios que
     caem nele e roda Apriori + regras de associação (confiança/lift) -
     cada cluster produz suas próprias regras, mais específicas do que
     rodar Apriori na base inteira de uma vez.
  4. Grava clusters e regras de volta no banco (`public.cluster_municipio`,
     `public.regra_associacao`) e em CSV/PNG em ml/output/, para uso na
     apresentação e no Metabase.

Uso:
    python3 ml/clusterizacao_regras_associacao.py [opções]

Requer Postgres do `docker-compose.yml` no ar e a carga do data mart feita
(ver README.md, seção Airflow).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from matplotlib.colors import LinearSegmentedColormap
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

SCRIPT_DIR = Path(__file__).resolve().parent

# Paleta categórica/sequencial de referência (ver skill dataviz do projeto):
# slot 1 azul / slot 2 laranja para séries categóricas, rampa azul clara->escura
# pra magnitude (sequencial), em vez da colormap default do matplotlib.
COR_SERIE_1 = "#2a78d6"
COR_SERIE_2 = "#eb6834"
RAMPA_SEQUENCIAL_AZUL = LinearSegmentedColormap.from_list(
    "azul_sequencial", ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]
)

# Valores que representam "sem informação" nas 9 classificações - descartados
# das cestas, senão dominam os itemsets frequentes sem serem acionáveis.
VALORES_NAO_INFORMATIVOS = {"Não informado", "(null)", "Ignorada", "Ignorado"}

# 8 das 9 classificações: colunas escalares de dim_classificacao_acidente.
# tracado_via NÃO está aqui - desde 2017 é multivalorado (0 a N por
# ocorrência) e vive à parte, em fato_acidente_tracado_via (bridge table,
# ver data_marting/dw_postgres.sql) - tratado separadamente em
# carregar_dataset()/construir_cestas() abaixo.
COLUNAS_CLASSIFICACAO = [
    ("causa_acidente", "causa"),
    ("tipo_acidente", "tipo_acidente"),
    ("classificacao_acidente", "classificacao"),
    ("fase_dia", "fase_dia"),
    ("sentido_via", "sentido_via"),
    ("condicao_metereologica", "clima"),
    ("tipo_pista", "tipo_pista"),
    ("uso_solo", "uso_solo"),
]
PREFIXO_TRACADO_VIA = "tracado_via"

FEATURES_CLUSTER = [
    "log_n_ocorrencias",
    "taxa_letalidade",
    "taxa_feridos_graves",
    "veiculos_medio",
    "prop_fim_semana",
    "prop_noite",
    "prop_chuva",
    "prop_pista_simples",
    "concentracao_causa",
]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", default="dw")
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="postgres")
    p.add_argument("--min-ocorrencias", type=int, default=30,
                    help="município só entra na clusterização com pelo menos N ocorrências (default: 30)")
    p.add_argument("--k", type=int, default=None,
                    help="número fixo de clusters; se omitido, escolhe o melhor k em 2..8 pela silhueta")
    p.add_argument("--min-support", type=float, default=0.05)
    p.add_argument("--min-lift", type=float, default=1.1)
    p.add_argument("--min-confidence", type=float, default=0.3)
    p.add_argument("--top-n-rules", type=int, default=15,
                    help="quantas regras por cluster manter no relatório final (default: 15)")
    p.add_argument("--output-dir", default=str(SCRIPT_DIR / "output"))
    p.add_argument("--no-persist", action="store_true",
                    help="não grava cluster_municipio/regra_associacao de volta no Postgres")
    return p.parse_args()


def carregar_dataset(conn) -> pd.DataFrame:
    colunas_sql = ", ".join(f"dc.{col}" for col, _ in COLUNAS_CLASSIFICACAO)
    query = f"""
        SELECT
            fa.id_fato_acidente,
            dl.nome_municipio,
            dl.sigla_uf,
            dt.flag_fim_semana,
            fa.pessoas,
            fa.mortos,
            fa.feridos_graves,
            fa.feridos_leves,
            fa.veiculos,
            {colunas_sql}
        FROM fato_acidentes fa
        JOIN dim_local dl ON dl.id_dim_local = fa.id_dim_local
        JOIN dim_tempo dt ON dt.id_dim_tempo = fa.id_dim_tempo
        JOIN dim_classificacao_acidente dc ON dc.id_dim_classificacao = fa.id_dim_classificacao
    """
    df = pd.read_sql(query, conn)
    df["municipio_chave"] = df["nome_municipio"] + " - " + df["sigla_uf"]
    return df


def carregar_tracado_via(conn) -> dict:
    """fato_acidente_tracado_via é uma bridge table (0 a N linhas por
    ocorrência) - carregada à parte do resto das classificações."""
    df = pd.read_sql("SELECT id_fato_acidente, valor FROM fato_acidente_tracado_via", conn)
    mapa = {}
    for id_fato, valor in df.itertuples(index=False):
        mapa.setdefault(id_fato, []).append(valor)
    return mapa


def construir_perfil_municipios(df: pd.DataFrame, min_ocorrencias: int) -> pd.DataFrame:
    def concentracao_causa(serie):
        return serie.value_counts(normalize=True).iloc[0]

    perfil = df.groupby("municipio_chave").agg(
        nome_municipio=("nome_municipio", "first"),
        sigla_uf=("sigla_uf", "first"),
        n_ocorrencias=("municipio_chave", "size"),
        n_pessoas=("pessoas", "sum"),
        n_mortos=("mortos", "sum"),
        n_feridos_graves=("feridos_graves", "sum"),
        n_veiculos=("veiculos", "sum"),
        prop_fim_semana=("flag_fim_semana", "mean"),
    )
    perfil["prop_noite"] = df.groupby("municipio_chave")["fase_dia"].apply(
        lambda s: s.isin(["Plena noite", "Anoitecer"]).mean()
    )
    perfil["prop_chuva"] = df.groupby("municipio_chave")["condicao_metereologica"].apply(
        lambda s: (s == "Chuva").mean()
    )
    perfil["prop_pista_simples"] = df.groupby("municipio_chave")["tipo_pista"].apply(
        lambda s: (s == "Simples").mean()
    )
    perfil["concentracao_causa"] = df.groupby("municipio_chave")["causa_acidente"].apply(concentracao_causa)

    perfil["taxa_letalidade"] = (perfil["n_mortos"] / perfil["n_pessoas"]).fillna(0.0)
    perfil["taxa_feridos_graves"] = (perfil["n_feridos_graves"] / perfil["n_pessoas"]).fillna(0.0)
    perfil["veiculos_medio"] = perfil["n_veiculos"] / perfil["n_ocorrencias"]
    perfil["log_n_ocorrencias"] = np.log1p(perfil["n_ocorrencias"])

    perfil = perfil[perfil["n_ocorrencias"] >= min_ocorrencias].copy()
    return perfil


def escolher_k_e_clusterizar(perfil: pd.DataFrame, k_fixo: int | None, output_dir: Path):
    X = StandardScaler().fit_transform(perfil[FEATURES_CLUSTER])

    if k_fixo:
        melhor_k = k_fixo
        silhuetas = {}
    else:
        silhuetas = {}
        for k in range(2, 9):
            km = KMeans(n_clusters=k, random_state=42, n_init=10).fit(X)
            silhuetas[k] = silhouette_score(X, km.labels_)
        melhor_k = max(silhuetas, key=silhuetas.get)

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(list(silhuetas.keys()), list(silhuetas.values()), marker="o", color=COR_SERIE_1)
        ax.axvline(melhor_k, color=COR_SERIE_2, linestyle="--", label=f"k escolhido = {melhor_k}")
        ax.set_xlabel("k (número de clusters)")
        ax.set_ylabel("Silhouette score")
        ax.set_title("Escolha de k para KMeans dos municípios")
        ax.legend()
        fig.tight_layout()
        fig.savefig(output_dir / "silhouette_por_k.png", dpi=150)
        plt.close(fig)

    km_final = KMeans(n_clusters=melhor_k, random_state=42, n_init=10).fit(X)
    perfil = perfil.copy()
    perfil["cluster"] = km_final.labels_
    return perfil, melhor_k, silhuetas


def plotar_perfil_clusters(perfil: pd.DataFrame, output_dir: Path):
    colunas_perfil = [
        "taxa_letalidade", "taxa_feridos_graves", "veiculos_medio",
        "prop_fim_semana", "prop_noite", "prop_chuva", "prop_pista_simples",
        "concentracao_causa",
    ]
    medias = perfil.groupby("cluster")[colunas_perfil].mean()
    medias_norm = (medias - medias.min()) / (medias.max() - medias.min() + 1e-9)

    fig, ax = plt.subplots(figsize=(9, 0.6 * len(medias_norm) + 2))
    im = ax.imshow(medias_norm.values, cmap=RAMPA_SEQUENCIAL_AZUL, aspect="auto")
    ax.set_xticks(range(len(colunas_perfil)))
    ax.set_xticklabels(colunas_perfil, rotation=40, ha="right")
    ax.set_yticks(range(len(medias_norm)))
    ax.set_yticklabels([f"cluster {c}" for c in medias_norm.index])
    for i in range(medias_norm.shape[0]):
        for j in range(medias_norm.shape[1]):
            cor_texto = "white" if medias_norm.values[i, j] > 0.6 else "#0b0b0b"
            ax.text(j, i, f"{medias.values[i, j]:.2f}", ha="center", va="center", fontsize=8, color=cor_texto)
    fig.colorbar(im, ax=ax, label="intensidade relativa (min-max por coluna, entre clusters)")
    ax.set_title("Perfil médio por cluster de município")
    fig.savefig(output_dir / "perfil_clusters.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def construir_cestas(df: pd.DataFrame, mapa_cluster: pd.Series, mapa_tracado_via: dict) -> pd.DataFrame:
    df = df.copy()
    df["cluster"] = df["municipio_chave"].map(mapa_cluster)
    df = df.dropna(subset=["cluster"])
    df["cluster"] = df["cluster"].astype(int)

    colunas = [c for c, _ in COLUNAS_CLASSIFICACAO] + ["id_fato_acidente"]
    itens_por_linha = []
    for row in df[colunas].itertuples(index=False):
        itens = []
        for col, prefixo in COLUNAS_CLASSIFICACAO:
            valor = getattr(row, col)
            if valor not in VALORES_NAO_INFORMATIVOS:
                itens.append(f"{prefixo}={valor}")
        for valor in mapa_tracado_via.get(row.id_fato_acidente, []):
            itens.append(f"{PREFIXO_TRACADO_VIA}={valor}")
        itens_por_linha.append(itens)
    df["cesta"] = itens_por_linha
    return df[["cluster", "cesta"]]


def regras_por_cluster(cestas: pd.DataFrame, min_support, min_lift, min_confidence, top_n) -> pd.DataFrame:
    resultados = []
    for cluster, grupo in cestas.groupby("cluster"):
        transacoes = grupo["cesta"].tolist()
        if len(transacoes) < 50:
            continue

        te = TransactionEncoder()
        te_ary = te.fit(transacoes).transform(transacoes)
        df_onehot = pd.DataFrame(te_ary, columns=te.columns_)

        itemsets = apriori(df_onehot, min_support=min_support, use_colnames=True)
        if itemsets.empty:
            continue

        regras = association_rules(itemsets, metric="lift", min_threshold=min_lift, num_itemsets=len(transacoes))
        regras = regras[regras["confidence"] >= min_confidence]
        if regras.empty:
            continue

        regras = regras.sort_values("lift", ascending=False).head(top_n).copy()
        regras["cluster"] = cluster
        regras["n_ocorrencias_cluster"] = len(transacoes)
        regras["antecedents"] = regras["antecedents"].apply(lambda s: ", ".join(sorted(s)))
        regras["consequents"] = regras["consequents"].apply(lambda s: ", ".join(sorted(s)))
        resultados.append(regras[[
            "cluster", "n_ocorrencias_cluster", "antecedents", "consequents",
            "support", "confidence", "lift",
        ]])

    if not resultados:
        return pd.DataFrame(columns=[
            "cluster", "n_ocorrencias_cluster", "antecedents", "consequents",
            "support", "confidence", "lift",
        ])
    return pd.concat(resultados, ignore_index=True)


def persistir_no_banco(conn, perfil: pd.DataFrame, regras: pd.DataFrame):
    """Grava em `public.cluster_municipio`/`public.regra_associacao` - tabelas
    já criadas pelo DDL do data mart (data_marting/dw_postgres.sql), não aqui."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE public.cluster_municipio")
        cur.execute("TRUNCATE public.regra_associacao RESTART IDENTITY")

        psycopg2.extras.execute_values(cur, """
            INSERT INTO public.cluster_municipio
                (nome_municipio, sigla_uf, cluster, n_ocorrencias, taxa_letalidade,
                 taxa_feridos_graves, veiculos_medio, prop_fim_semana, prop_noite,
                 prop_chuva, prop_pista_simples, concentracao_causa)
            VALUES %s
        """, list(perfil[[
            "nome_municipio", "sigla_uf", "cluster", "n_ocorrencias", "taxa_letalidade",
            "taxa_feridos_graves", "veiculos_medio", "prop_fim_semana", "prop_noite",
            "prop_chuva", "prop_pista_simples", "concentracao_causa",
        ]].itertuples(index=False, name=None)))

        if not regras.empty:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO public.regra_associacao
                    (cluster, n_ocorrencias_cluster, antecedente, consequente, suporte, confianca, lift)
                VALUES %s
            """, list(regras[[
                "cluster", "n_ocorrencias_cluster", "antecedents", "consequents",
                "support", "confidence", "lift",
            ]].itertuples(index=False, name=None)))
    conn.commit()


def rodar_pipeline(
    host="localhost", port=5432, dbname="dw", user="postgres", password="postgres",
    min_ocorrencias=30, k=None, min_support=0.05, min_lift=1.1, min_confidence=0.3,
    top_n_rules=15, output_dir=None, persist=True,
):
    """Roda o pipeline completo (carrega -> perfila -> clusteriza -> regras de
    associação -> grava). Função reutilizável por trás do CLI (`main`) e da
    DAG do Airflow (`ml_clusterizacao_regras_associacao`)."""
    output_dir = Path(output_dir) if output_dir else SCRIPT_DIR / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    try:
        print("Carregando fato_acidentes + dimensões...")
        df = carregar_dataset(conn)
        mapa_tracado_via = carregar_tracado_via(conn)
        print(f"  {len(df)} ocorrências, {df['municipio_chave'].nunique()} municípios distintos, "
              f"{len(mapa_tracado_via)} com traçado de via registrado")

        print(f"Construindo perfil por município (min. {min_ocorrencias} ocorrências)...")
        perfil = construir_perfil_municipios(df, min_ocorrencias)
        print(f"  {len(perfil)} municípios após o filtro de volume mínimo")

        print("Clusterizando municípios (KMeans)...")
        perfil, k_escolhido, silhuetas = escolher_k_e_clusterizar(perfil, k, output_dir)
        if silhuetas:
            print(f"  silhueta por k: {silhuetas}")
        print(f"  k escolhido: {k_escolhido}")
        print(perfil.groupby("cluster").size().rename("n_municipios"))

        plotar_perfil_clusters(perfil, output_dir)
        perfil.reset_index().to_csv(output_dir / "clusters_municipios.csv", index=False)

        print("Construindo cestas de classificação por ocorrência...")
        cestas = construir_cestas(df, perfil["cluster"], mapa_tracado_via)

        print("Aplicando Apriori + regras de associação por cluster...")
        regras = regras_por_cluster(cestas, min_support, min_lift, min_confidence, top_n_rules)
        regras.to_csv(output_dir / "regras_associacao_por_cluster.csv", index=False)
        print(f"  {len(regras)} regras geradas no total")
        if not regras.empty:
            with pd.option_context("display.max_colwidth", 60, "display.width", 160):
                print(regras.to_string(index=False))

        if persist:
            print("Gravando cluster_municipio e regra_associacao no banco dw...")
            persistir_no_banco(conn, perfil.reset_index(), regras)

        print(f"\nSaídas em {output_dir}/:")
        print("  clusters_municipios.csv, regras_associacao_por_cluster.csv")
        print("  silhouette_por_k.png, perfil_clusters.png")
    finally:
        conn.close()


def main():
    args = parse_args()
    rodar_pipeline(
        host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password,
        min_ocorrencias=args.min_ocorrencias, k=args.k, min_support=args.min_support,
        min_lift=args.min_lift, min_confidence=args.min_confidence, top_n_rules=args.top_n_rules,
        output_dir=args.output_dir, persist=not args.no_persist,
    )


if __name__ == "__main__":
    main()
