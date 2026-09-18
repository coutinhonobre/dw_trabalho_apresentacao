#!/usr/bin/env python3
"""
Le dataset/datatran2007.csv (ISO-8859-1, separado por ';') e gera
db/02_data.sql com os INSERTs para o schema normalizado de db/01_schema.sql.

Uso:
    python3 scripts/gerar_inserts.py
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "dataset" / "datatran2007.csv"
OUT_PATH = ROOT / "db" / "02_data.sql"

BATCH_SIZE = 500

CATEGORIAS_VITIMA = {
    "mortos": "Mortos",
    "feridos_leves": "Feridos leves",
    "feridos_graves": "Feridos graves",
    "ilesos": "Ilesos",
    "ignorados": "Ignorados",
}

# (tipo_atributo, coluna no CSV, descricao, obrigatorio no CSV original)
ATRIBUTOS = [
    ("causa_acidente", "causa_acidente", "Causa do acidente", True),
    ("tipo_acidente", "tipo_acidente", "Tipo do acidente", True),
    ("classificacao_acidente", "classificacao_acidente", "Classificação do acidente", False),
    ("fase_dia", "fase_dia", "Fase do dia", False),
    ("sentido_via", "sentido_via", "Sentido da via", True),
    ("condicao_metereologica", "condicao_metereologica", "Condição meteorológica", False),
    ("tipo_pista", "tipo_pista", "Tipo de pista", True),
    ("tracado_via", "tracado_via", "Traçado da via", True),
    ("uso_solo", "uso_solo", "Uso do solo", True),
]

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


def to_iso_date(data_br):
    d, m, y = data_br.split("/")
    return f"{y}-{m}-{d}"


def load_rows():
    with CSV_PATH.open(encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    seen = set()
    dedup = []
    for row in rows:
        rid = row["id"]
        if rid in seen:
            continue
        seen.add(rid)
        dedup.append(row)
    return dedup


def write_batched(out, header, row_sql_list):
    for i in range(0, len(row_sql_list), BATCH_SIZE):
        chunk = row_sql_list[i:i + BATCH_SIZE]
        out.write(header)
        out.write(",\n".join(chunk))
        out.write(";\n\n")


def main():
    rows = load_rows()

    ufs = set()
    municipios = set()
    rodovias = set()
    localizacoes = set()
    valores_validos = set()  # (tipo_atributo, valor)
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

        for tipo_atributo, coluna, _descricao, _obrigatorio in ATRIBUTOS:
            valor = null_if(row[coluna])
            if valor:
                valores_validos.add((tipo_atributo, valor))

        data_iso = to_iso_date(row["data_inversa"])
        calendario[data_iso] = (row["dia_semana"], int(row["ano"]))

    with OUT_PATH.open("w", encoding="utf-8") as out:
        out.write("-- Gerado automaticamente por scripts/gerar_inserts.py "
                   "a partir de dataset/datatran2007.csv\n")
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

        out.write("-- tipo_atributo\n")
        out.write("INSERT INTO tipo_atributo (tipo_atributo, descricao, obrigatorio) VALUES\n")
        out.write(",\n".join(
            f"    ({sql_str(tipo_atributo)}, {sql_str(descricao)}, {str(obrigatorio).upper()})"
            for tipo_atributo, _coluna, descricao, obrigatorio in ATRIBUTOS
        ))
        out.write(";\n\n")

        out.write(f"-- atributo_valor_valido ({len(valores_validos)} pares distintos)\n")
        out.write("INSERT INTO atributo_valor_valido (tipo_atributo, valor) VALUES\n")
        out.write(",\n".join(
            f"    ({sql_str(tipo_atributo)}, {sql_str(valor)})"
            for tipo_atributo, valor in sorted(valores_validos)
        ))
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

        out.write(f"-- acidentes ({len(rows)} ocorrências, deduplicadas por id) - cabeçalho enxuto\n")
        acidentes_header = (
            "INSERT INTO acidentes (id, data, horario, local_id, veiculos) VALUES\n"
        )

        acidentes_values = []
        acidente_vitima_values = []
        acidente_atributo_values = []
        for row in rows:
            rid = int(row["id"])
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
            acidentes_values.append("    (" + ", ".join(values) + ")")

            for categoria in CATEGORIAS_VITIMA:
                quantidade = int(row[categoria])
                if quantidade > 0:
                    acidente_vitima_values.append(
                        f"    ({rid}, {sql_str(categoria)}, {quantidade})"
                    )

            for tipo_atributo, coluna, _descricao, _obrigatorio in ATRIBUTOS:
                valor = null_if(row[coluna])
                if valor:
                    acidente_atributo_values.append(
                        f"    ({rid}, {sql_str(tipo_atributo)}, {sql_str(valor)})"
                    )

        write_batched(out, acidentes_header, acidentes_values)

        out.write(f"-- acidente_vitima ({len(acidente_vitima_values)} linhas, só quantidade > 0)\n")
        acidente_vitima_header = (
            "INSERT INTO acidente_vitima (acidente_id, categoria, quantidade) VALUES\n"
        )
        write_batched(out, acidente_vitima_header, acidente_vitima_values)

        out.write(f"-- acidente_atributo ({len(acidente_atributo_values)} linhas, o \"itens_venda\" das classificações)\n")
        acidente_atributo_header = (
            "INSERT INTO acidente_atributo (acidente_id, tipo_atributo, valor) VALUES\n"
        )
        write_batched(out, acidente_atributo_header, acidente_atributo_values)

        out.write("COMMIT;\n")

    print(f"OK: {len(rows)} ocorrências, {len(municipios)} municípios, "
          f"{len(localizacoes)} localizações, {len(pares_local)} locais de acidente, "
          f"{len(calendario)} datas -> {OUT_PATH}")


if __name__ == "__main__":
    main()
