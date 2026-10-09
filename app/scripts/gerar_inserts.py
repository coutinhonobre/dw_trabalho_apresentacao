#!/usr/bin/env python3
"""
Le todos os arquivos dataset/datatranAAAA.csv (ISO-8859-1, separado por ';')
de todos os anos disponíveis e gera os INSERTs para o schema normalizado de
db/01_schema.sql, um arquivo por ano (db/02-NN-AAAA.sql) mais um arquivo de
catálogos compartilhados (db/02-00-catalogos.sql).

Por que por ano, e não um `02_data.sql` só: os catálogos (uf, município,
local_acidente etc.) são globais - um `local_id` é reaproveitado entre anos
- mas as 2,2M+ linhas de ocorrência não têm essa dependência entre si. Um
arquivo por ano, cada um com seu próprio BEGIN/COMMIT, permite: (1) carregar
só um ano pra testar rápido (ex.: `psql ... < db/02-00-catalogos.sql &&
psql ... < db/02-01-2007.sql`) sem esperar os outros 19; (2) uma falha num
ano só desfaz aquele ano - não todos os 2,2M+ linhas de uma vez, como
acontecia com o `BEGIN;...COMMIT;` único do arquivo monolítico anterior.
`docker-compose.yml` monta cada arquivo gerado como um script
`docker-entrypoint-initdb.d` próprio, na ordem numérica do prefixo.

O "id" original do CSV não serve de chave primária global: a PRF reiniciou a
numeração em alguns anos (ranges se sobrepõem entre arquivos) e existem
alguns valores corrompidos (notação científica, ex. "1e+05"). Por isso o
script gera um id sintético sequencial para a tabela `acidentes`, único e
crescente across todos os anos (não reinicia a cada arquivo), mesmo com a
carga split por ano.

Uso:
    python3 scripts/gerar_inserts.py
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
DB_DIR = ROOT / "db"
CATALOGOS_PATH = DB_DIR / "02-00-catalogos.sql"

BATCH_SIZE = 500

CATEGORIAS_VITIMA = {
    "mortos": "Mortos",
    "feridos_leves": "Feridos leves",
    "feridos_graves": "Feridos graves",
    "ilesos": "Ilesos",
    "ignorados": "Ignorados",
}

# As 8 classificações de valor único (coluna em `acidentes` == coluna no
# CSV == nome do catálogo + "_valido" - ver db/01_schema.sql). tracado_via
# NÃO está aqui: a partir de 2017 o CSV passou a concatenar múltiplos
# valores com ';' nesse campo (ex. "Reta;Curva;Viaduto"), então é tratado à
# parte (split_tracado_via abaixo), em tabela associativa própria - ver nota
# "DESCOBERTA" em db/01_schema.sql.
COLUNAS_CLASSIFICACAO = [
    "causa_acidente", "tipo_acidente", "classificacao_acidente", "fase_dia",
    "sentido_via", "condicao_metereologica", "tipo_pista", "uso_solo",
]

# A PRF não manteve maiúscula/acento consistentes nesses 4 campos entre anos
# do CSV (ex.: "Ceu Claro" num ano, "Céu Claro" noutro) - sem normalizar, viram
# duas linhas distintas no catálogo (mesmo significado, strings diferentes),
# diluindo contagens/regras de associação e duplicando categoria em gráfico.
# Mapeamento levantado empírico comparando valores após dobrar
# maiúscula/acento (ver histórico do projeto) - lado esquerdo é descartado em
# favor do direito.
NORMALIZACAO_CLASSIFICACAO = {
    "causa_acidente": {
        "Ingestão de álcool": "Ingestão de Álcool",
        "Transitar no acostamento": "Transitar no Acostamento",
        "Defeito na via": "Defeito na Via",
        "Velocidade incompatível": "Velocidade Incompatível",
        "Ultrapassagem indevida": "Ultrapassagem Indevida",
    },
    "tipo_acidente": {
        "Danos eventuais": "Danos Eventuais",
        "Atropelamento de animal": "Atropelamento de Animal",
        "Derramamento de carga": "Derramamento de Carga",
        "Colisão transversal": "Colisão Transversal",
    },
    "fase_dia": {
        "Plena noite": "Plena Noite",
    },
    "condicao_metereologica": {
        "Ceu Claro": "Céu Claro",
        "Ignorada": "Ignorado",
        "Nevoeiro/neblina": "Nevoeiro/Neblina",
    },
}


def normalizar_classificacao(coluna, valor):
    if valor is None:
        return None
    return NORMALIZACAO_CLASSIFICACAO.get(coluna, {}).get(valor, valor)


def split_tracado_via(valor):
    """"Reta;Curva;Viaduto" -> ["Reta", "Curva", "Viaduto"], sem duplicatas e
    preservando a ordem. Pré-2017 o campo já vem com um valor só (lista de 1)."""
    partes = [p.strip() for p in valor.split(";")]
    vistos = []
    for p in partes:
        if p and p not in vistos:
            vistos.append(p)
    return vistos

UF_NOMES = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas", "BA": "Bahia",
    "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
    "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco", "PI": "Piauí",
    "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul",
    "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina", "SP": "São Paulo",
    "SE": "Sergipe", "TO": "Tocantins",
}


def sql_str(value):
    return "'" + value.replace("'", "''") + "'"


def null_if(value):
    if value is None:
        return None
    value = value.strip()
    return None if value == "" or value == "(null)" else value


def to_iso_date(data_raw):
    """Normaliza data_inversa para 'AAAA-MM-DD'.

    O formato muda ao longo dos anos do dataset: dd/mm/aaaa (2007-2011),
    aaaa-mm-dd (2012-2015 e 2017+) e dd/mm/aa com ano de 2 dígitos (2016).
    """
    data_raw = data_raw.strip()
    if "-" in data_raw:
        return data_raw
    d, m, y = data_raw.split("/")
    if len(y) == 2:
        y = "20" + y
    return f"{y}-{m}-{d}"


def find_csv_files():
    files = sorted(DATASET_DIR.glob("datatran*.csv"))
    if not files:
        raise SystemExit(f"Nenhum CSV encontrado em {DATASET_DIR}")
    return files


def load_rows(csv_path):
    # Ano do arquivo de origem (ex. "datatran2007.csv" -> 2007) - não é o
    # mesmo que `ano` extraído de `data_inversa` (uma ocorrência registrada
    # em 31/12 pode cair no arquivo do ano seguinte por atraso de
    # processamento da PRF; é rara mas existe). Usado só pra decidir em qual
    # arquivo 02-NN-AAAA.sql a linha cai - o split é por ARQUIVO DE ORIGEM,
    # não por data da ocorrência.
    ano_arquivo = int(csv_path.stem[-4:])

    with csv_path.open(encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    seen = set()
    dedup = []
    for row in rows:
        rid = row["id"]
        if rid in seen:
            continue
        seen.add(rid)
        # Alguns anos (2016+) usam vírgula como separador decimal no km;
        # normaliza para ponto, que é o formato aceito pelo SQL.
        row["km"] = row["km"].replace(",", ".")
        row["_ano_arquivo"] = ano_arquivo
        dedup.append(row)
    return dedup


def load_all_rows():
    all_rows = []
    for csv_path in find_csv_files():
        file_rows = load_rows(csv_path)
        print(f"  {csv_path.name}: {len(file_rows)} ocorrências")
        all_rows.extend(file_rows)
    return all_rows


def write_batched(out, header, row_sql_list):
    for i in range(0, len(row_sql_list), BATCH_SIZE):
        chunk = row_sql_list[i:i + BATCH_SIZE]
        out.write(header)
        out.write(",\n".join(chunk))
        out.write(";\n\n")


def main():
    print("Lendo CSVs de", DATASET_DIR)
    rows = load_all_rows()

    ufs = set()
    municipios = set()
    rodovias = set()
    localizacoes = set()
    valores_validos = {coluna: set() for coluna in COLUNAS_CLASSIFICACAO}
    tracados_via = set()
    calendario = {}

    for row in rows:
        uf = null_if(row["uf"])
        municipio = null_if(row["municipio"])
        br = null_if(row["br"])
        km = null_if(row["km"])

        if uf:
            ufs.add(uf)
        if municipio and uf:
            municipios.add((municipio, uf))
        if br:
            rodovias.add(int(br))
        if br and km:
            localizacoes.add((int(br), km))

        for coluna in COLUNAS_CLASSIFICACAO:
            valor = normalizar_classificacao(coluna, null_if(row[coluna]))
            if valor:
                valores_validos[coluna].add(valor)

        tracado_via = null_if(row["tracado_via"])
        if tracado_via:
            tracados_via.update(split_tracado_via(tracado_via))

        data_iso = to_iso_date(row["data_inversa"])
        calendario[data_iso] = (row["dia_semana"], int(data_iso[:4]))

    with CATALOGOS_PATH.open("w", encoding="utf-8") as out:
        out.write("-- Gerado automaticamente por scripts/gerar_inserts.py "
                   "a partir de dataset/datatran*.csv (todos os anos) - "
                   "catálogos compartilhados, carregar antes de qualquer "
                   "02-NN-AAAA.sql (ver cabeçalho do script)\n")
        out.write("BEGIN;\n\n")

        out.write("-- uf\n")
        out.write("INSERT INTO uf (sigla, nome) VALUES\n")
        out.write(",\n".join(
            f"    ({sql_str(uf)}, {sql_str(UF_NOMES.get(uf, uf))})"
            for uf in sorted(ufs)
        ))
        out.write(";\n\n")

        out.write("-- rodovia\n")
        out.write("INSERT INTO rodovia (numero) VALUES\n")
        out.write(",\n".join(f"    ({n})" for n in sorted(rodovias)))
        out.write(";\n\n")

        localizacao_id = {
            loc: i + 1
            for i, loc in enumerate(sorted(localizacoes, key=lambda t: (t[0], float(t[1]))))
        }

        out.write("-- localizacao (id explícito para poder referenciar em acidentes sem subquery)\n")
        out.write("INSERT INTO localizacao (id, rodovia_numero, km) VALUES\n")
        out.write(",\n".join(
            f"    ({lid}, {br}, {km})"
            for (br, km), lid in sorted(localizacao_id.items(), key=lambda kv: kv[1])
        ))
        out.write(";\n\n")
        out.write(f"SELECT setval('localizacao_id_seq', {len(localizacao_id)});\n\n")

        municipio_id = {
            (nome, uf): i + 1
            for i, (nome, uf) in enumerate(sorted(municipios))
        }

        out.write("-- municipio (id explícito para poder referenciar em acidentes sem subquery)\n")
        out.write("INSERT INTO municipio (id, nome, uf_sigla) VALUES\n")
        out.write(",\n".join(
            f"    ({mid}, {sql_str(nome)}, {sql_str(uf)})"
            for (nome, uf), mid in sorted(municipio_id.items(), key=lambda kv: kv[1])
        ))
        out.write(";\n\n")
        out.write(f"SELECT setval('municipio_id_seq', {len(municipio_id)});\n\n")

        # local_acidente depende de municipio_id/localizacao_id já resolvidos:
        # primeiro descobre os pares (mid, lid) usados por ocorrência, depois
        # atribui os ids explícitos.
        pares_local = set()
        for row in rows:
            uf = null_if(row["uf"])
            municipio = null_if(row["municipio"])
            mid = municipio_id.get((municipio, uf)) if (municipio and uf) else None
            br = null_if(row["br"])
            km = null_if(row["km"])
            lid = localizacao_id.get((int(br), km)) if (br and km) else None
            if mid is not None and lid is not None:
                pares_local.add((mid, lid))

        local_acidente_id = {
            par: i + 1
            for i, par in enumerate(sorted(pares_local))
        }

        out.write("-- local_acidente (id explícito para poder referenciar em acidentes sem subquery)\n")
        out.write("INSERT INTO local_acidente (id, municipio_id, localizacao_id) VALUES\n")
        out.write(",\n".join(
            f"    ({lacid}, {mid}, {lid})"
            for (mid, lid), lacid in sorted(local_acidente_id.items(), key=lambda kv: kv[1])
        ))
        out.write(";\n\n")
        out.write(f"SELECT setval('local_acidente_id_seq', {len(local_acidente_id)});\n\n")

        for coluna in COLUNAS_CLASSIFICACAO:
            valores = valores_validos[coluna]
            out.write(f"-- {coluna}_valido ({len(valores)} valores distintos)\n")
            out.write(f"INSERT INTO {coluna}_valido (valor) VALUES\n")
            out.write(",\n".join(f"    ({sql_str(v)})" for v in sorted(valores)))
            out.write(";\n\n")

        out.write(f"-- tracado_via_valido ({len(tracados_via)} valores distintos, após split por ';')\n")
        out.write("INSERT INTO tracado_via_valido (valor) VALUES\n")
        out.write(",\n".join(f"    ({sql_str(v)})" for v in sorted(tracados_via)))
        out.write(";\n\n")

        out.write("-- calendario\n")
        out.write("INSERT INTO calendario (data, dia_semana, ano) VALUES\n")
        out.write(",\n".join(
            f"    ({sql_str(data)}, {sql_str(dia)}, {ano})"
            for data, (dia, ano) in sorted(calendario.items())
        ))
        out.write(";\n\n")

        out.write("-- categoria_vitima\n")
        out.write("INSERT INTO categoria_vitima (categoria, descricao) VALUES\n")
        out.write(",\n".join(
            f"    ({sql_str(cat)}, {sql_str(desc)})"
            for cat, desc in CATEGORIAS_VITIMA.items()
        ))
        out.write(";\n\n")

        out.write("COMMIT;\n")

    # ------------------------------------------------------------------
    # Um arquivo por ano de origem (acidentes, já com as 8 classificações
    # como colunas, + acidente_vitima + acidente_tracado_via) - `rid`
    # continua sequencial e único GLOBALMENTE (não reinicia por ano), só o
    # arquivo de destino muda.
    # ------------------------------------------------------------------
    anos = sorted({row["_ano_arquivo"] for row in rows})
    por_ano = {
        ano: {"acidentes": [], "vitima": [], "tracado_via": [], "n": 0}
        for ano in anos
    }

    acidentes_colunas = (
        "id, data, horario, local_id, veiculos, " + ", ".join(COLUNAS_CLASSIFICACAO)
    )

    for rid, row in enumerate(rows, start=1):
        bucket = por_ano[row["_ano_arquivo"]]
        bucket["n"] += 1

        data_iso = to_iso_date(row["data_inversa"])
        horario = row["horario"]
        uf = null_if(row["uf"])
        municipio = null_if(row["municipio"])
        mid = municipio_id.get((municipio, uf)) if (municipio and uf) else None
        br = null_if(row["br"])
        km = null_if(row["km"])
        lid = localizacao_id.get((int(br), km)) if (br and km) else None
        lacid = local_acidente_id.get((mid, lid)) if (mid is not None and lid is not None) else None

        values = [
            str(rid),
            sql_str(data_iso),
            sql_str(horario),
            str(lacid) if lacid is not None else "NULL",
            row["veiculos"],
        ]
        for coluna in COLUNAS_CLASSIFICACAO:
            valor = normalizar_classificacao(coluna, null_if(row[coluna]))
            values.append(sql_str(valor) if valor else "NULL")
        bucket["acidentes"].append("    (" + ", ".join(values) + ")")

        for categoria in CATEGORIAS_VITIMA:
            quantidade = int(row[categoria])
            if quantidade > 0:
                bucket["vitima"].append(f"    ({rid}, {sql_str(categoria)}, {quantidade})")

        tracado_via = null_if(row["tracado_via"])
        if tracado_via:
            for valor in split_tracado_via(tracado_via):
                bucket["tracado_via"].append(f"    ({rid}, {sql_str(valor)})")

    for seq, ano in enumerate(anos, start=1):
        bucket = por_ano[ano]
        out_path = DB_DIR / f"02-{seq:02d}-{ano}.sql"
        with out_path.open("w", encoding="utf-8") as out:
            out.write(f"-- Gerado automaticamente por scripts/gerar_inserts.py a partir de "
                       f"dataset/datatran{ano}.csv - requer db/02-00-catalogos.sql já carregado\n")
            out.write("BEGIN;\n\n")

            out.write(f"-- acidentes ({bucket['n']} ocorrências de {ano})\n")
            write_batched(out, f"INSERT INTO acidentes ({acidentes_colunas}) VALUES\n",
                          bucket["acidentes"])

            out.write(f"-- acidente_vitima ({len(bucket['vitima'])} linhas, só quantidade > 0)\n")
            write_batched(out, "INSERT INTO acidente_vitima (acidente_id, categoria, quantidade) VALUES\n",
                          bucket["vitima"])

            out.write(f"-- acidente_tracado_via ({len(bucket['tracado_via'])} linhas)\n")
            write_batched(out, "INSERT INTO acidente_tracado_via (acidente_id, valor) VALUES\n",
                          bucket["tracado_via"])

            out.write("COMMIT;\n")
        print(f"  {out_path.name}: {bucket['n']} ocorrências")

    print(f"OK: {len(rows)} ocorrências, {len(anos)} anos, {len(municipios)} municípios, "
          f"{len(localizacoes)} localizações, {len(pares_local)} locais de acidente, "
          f"{len(calendario)} datas -> {CATALOGOS_PATH} + {len(anos)} arquivos 02-NN-AAAA.sql")


if __name__ == "__main__":
    main()
