# dw_trabalho_apresentacao

Quatro camadas sobre o dataset de acidentes de trânsito da PRF (`dataset/datatran2007.csv`):

1. **Transacional (OLTP)** — normalizado até a 4a Forma Normal (4FN), banco `datatran`.
2. **Data Warehouse corporativo** — modelado à Inmon (orientado por assunto, integrado,
   não-volátil, variante no tempo), schema `corporativo` do banco `dw`.
3. **Data mart em estrela** — modelado à Kimball, derivado do corporativo, schema
   `public` do banco `dw`.
4. **BI (Metabase)** — painel "Painel PRF - Acidentes de Trânsito" sobre o data
   mart, provisionado automaticamente via API.

As três primeiras vivem no mesmo servidor Postgres (`docker-compose.yml`); a
carga do transacional para o DW é feita por duas DAGs do Airflow
(`airflow/dags/`).

## Estrutura

```
dataset/
  datatran2007.csv          dataset original (PRF, 1 linha = 1 ocorrência)
db/
  01_schema.sql              DDL normalizado do OLTP (comentários com as DFs e a justificativa da 4FN)
  02_data.sql                INSERTs gerados a partir do CSV (não editar à mão, ver scripts/)
scripts/
  gerar_inserts.py           lê o CSV e gera db/02_data.sql
dw/                          camada corporativa integrada (Inmon: por assunto, normalizada)
  dw_postgres.sql           schema normalizado do DW (schema `corporativo` no banco `dw`)
  staging_postgres.sql      schema de staging (schema `staging` no banco `dw`), usado pela carga incremental
  metadados_postgres.sql    schema do catálogo de metadados (schema `metadados` no banco `dw`)
  metadados_seed_postgres.sql  inserts de metadados (execução manual, ver abaixo)
data_marting/                camada de data mart (dimensional, dependente do corporativo)
  dw_postgres.sql           schema do data mart em estrela (Postgres, banco `dw`)
  demo_check_dw.sql         queries de conferência dos dados carregados
airflow/
  dags/common_etl.py             helpers compartilhados pelas duas DAGs
  dags/carga_inicial_dw.py       DAG da primeira carga do DW (ver seção Airflow)
  dags/carga_incremental_dw.py   DAG da carga incremental (ver seção Airflow)
diagrams/
  der_transacional.drawio       DER do modelo OLTP (db/01_schema.sql)
  dw_modelo_corporativo.drawio  diagrama do DW normalizado por assunto (dw/dw_postgres.sql)
  dw_modelo_estrela.drawio      diagrama do data mart em estrela (data_marting/dw_postgres.sql)
  metadados_modelo.drawio       diagrama do catálogo de metadados
metabase/
  Dockerfile                   imagem do container de provisionamento (metabase-setup)
  setup_dashboards.py          cria conexão, mapa do Brasil e o dashboard via API do Metabase
docker-compose.yml            sobe o Postgres (datatran + dw), o Airflow e o Metabase
start.sh                      recria tudo do zero (schema + dados)
```

## Subindo o ambiente

```bash
./start.sh
```

Isso recria os containers do zero (`docker compose down -v && up -d --force-recreate`)
e reprocessa todos os scripts de inicialização. Não há volume persistente — os
bancos sempre nascem limpos, com schema e dados reconstruídos a cada execução.

Postgres sobe em `localhost:5432` com dois bancos:

- `datatran` (usuário/senha `postgres`/`postgres`) — schema OLTP + dados do CSV.
- `dw` — schemas `corporativo`, `staging`, `metadados` e `public` (data mart),
  criados por `dw/dw_postgres.sql` (que executa `CREATE DATABASE dw; \c dw`
  antes de tudo) e pelos scripts seguintes, na ordem numerada dos volumes do
  `docker-compose.yml`.

Airflow sobe em `http://localhost:8080` (usuário/senha `airflow`/`airflow`,
pode levar 1-2 min pra ficar disponível).

Metabase sobe em `http://localhost:3000`. O container `metabase-setup` roda
uma vez, espera o Metabase ficar pronto e provisiona tudo sozinho via API —
ver seção BI abaixo. Dois logins:

- `metabase@metabase.com` / `metabase` — usuário padrão, só visualiza os painéis.
- `admin@metabase.com` / `Metabase123!` — admin, criado pelo setup inicial (conexão de banco, mapas, usuários).

## Camada 1 — Transacional (OLTP), banco `datatran`

Ver `db/01_schema.sql` para o detalhamento completo das dependências
funcionais e da decomposição 4FN. Resumo: `acidentes` (cabeçalho) + `calendario`
+ `local_acidente` (município + localização) + `acidente_vitima` (categoria
de vítima, tabela associativa 4FN) + `acidente_atributo` (as 9 classificações
em padrão atributo-valor/EAV, catalogadas em `atributo_valor_valido`). Inclui
também `veiculo_envolvido`/`pessoa_envolvida`, extensão conceitual de
granularidade mais fina que fica vazia nesta fonte (o CSV só traz contagem
agregada).

Diagrama: `diagrams/der_transacional.drawio`.

## Camada 2 — Data Warehouse corporativo (Inmon), schema `corporativo` no banco `dw`

Modelo corporativo do Inmon: orientado a assunto, integrado, não-volátil e
variante no tempo. Organizado em 6 assuntos, independentes de qualquer
particularidade do OLTP:

- **Tempo** — `tempos` (calendário gerado uma vez via `generate_series`,
  2000-2035; referência compartilhada, `ocorrencias` guarda a data via FK)
- **Geografia e Via** — `ufs`, `municipios`, `rodovias`, `localizacoes`, `locais_acidente`
- **Classificação do Acidente** — `tipos_classificacao`, `classificacoes_validas`
  (catálogo atributo-valor, copiado do OLTP)
- **Vítimas** — `categorias_vitima` (catálogo, copiado do OLTP)
- **Ocorrências** — `ocorrencias` (fato operacional, cabeçalho enxuto),
  `ocorrencia_vitima`, `ocorrencia_classificacao`
- **Veículos e Pessoas** — `veiculos_envolvidos`, `pessoas_envolvidas`
  (extensão conceitual, tabelas vazias — mesma justificativa do OLTP)

Integrado: `ocorrencias` carrega `sistema_origem` (fixo em `'PRF-DATATRAN'`
hoje) + `id_ocorrencia_origem` (chave natural do CSV) ao lado da chave
substituta corporativa (sequência própria) — isso é o que permite empilhar
`datatran2008.csv`, `datatran2009.csv` etc. no mesmo `corporativo.ocorrencias`
ano após ano sem colidir PKs nem reprocessar histórico, mesmo havendo hoje uma
única fonte real.

Não-volátil / variante no tempo: `ocorrencias`/`ocorrencia_vitima`/
`ocorrencia_classificacao` são append-only — uma ocorrência carregada nunca
sofre `UPDATE`. Diferente do projeto de referência que inspirou esta
estrutura (Mercearia+Northwind, onde cliente/funcionário/produto exigem
histórico versionado via SCD2), aqui **nenhuma** tabela de apoio precisa de
`upsert_historizado`: nome de município, sigla de UF etc. não mudam no
dataset de origem, então todas usam upsert simples (existe ou não pela chave
natural). A única coisa genuinamente variante no tempo é a chegada de
ocorrências novas.

Diagrama: `diagrams/dw_modelo_corporativo.drawio`.

## Camada 3 — Data mart em estrela (Kimball), banco `dw`, schema `public`

Grão do fato: 1 linha por ocorrência (mesmo grão do CSV e de
`corporativo.ocorrencias` — não há granularidade por vítima ou por veículo
aqui, porque a fonte não tem).

- `dim_tempo` — mesma populada via `generate_series`, conformada com `corporativo.tempos`
- `dim_local` — município + UF + rodovia + km desnormalizados numa única
  linha (evita 4 JOINs em tempo de consulta no BI), conformada com
  `corporativo.locais_acidente`
- `dim_classificacao_acidente` — **dimensão junk**: uma linha por combinação
  realmente observada das 9 classificações do acidente (causa, tipo,
  classificação, fase do dia, sentido da via, condição meteorológica, tipo de
  pista, traçado da via, uso do solo), em vez de 9 dimensões separadas ou 9 FKs
  soltas no fato. Cada campo usa `'Não informado'` no lugar de `NULL` para a
  `UNIQUE` funcionar (Postgres trata `NULL` como distinto de `NULL`)
- `fato_acidentes` — `mortos`, `feridos_leves`, `feridos_graves`, `ilesos`,
  `ignorados` pivotados a partir de `corporativo.ocorrencia_vitima`, mais
  `feridos` e `pessoas` **pré-calculados** (ao contrário do OLTP/corporativo,
  que nunca guardam esses totais — aqui a redundância é desejada, é o que
  torna o fato somável direto num BI)

Diagrama: `diagrams/dw_modelo_estrela.drawio`.

Conferir a carga:

```bash
docker exec -i datatran_postgres psql -U postgres -d dw < data_marting/demo_check_dw.sql
```

## Metadados — schema `metadados` dentro do banco `dw`

Catálogo que documenta a origem e a regra de transformação de cada campo do
DW: tabelas/campos do `datatran`, tabelas/campos do corporativo e do data
mart, algoritmos de ETL e a linhagem entre eles
(`integracao_transacional_dw`). Schema genérico (`dw/metadados_postgres.sql`,
domain-agnostic); o conteúdo específico deste projeto é inserido à parte,
propositalmente fora do `docker-entrypoint-initdb.d`:

```bash
docker exec -i datatran_postgres psql -U postgres -d dw < dw/metadados_seed_postgres.sql
```

O seed documenta 14 tabelas / 46 campos do `datatran`, 18 tabelas (14
corporativo + 4 data mart) / 111 campos do DW e 76 linhas de linhagem —
incluindo os campos sem origem transacional, atribuídos pelo próprio ETL
(`sistema_origem` fixo, sentinela `'Não informado'`, totais recalculados),
ancorados em um `dado_externo` de "Regras de Negócio do ETL".

Diagrama: `diagrams/metadados_modelo.drawio`.

## Airflow — carga inicial

DAG `carga_inicial_dw`, disparo manual (`schedule=None`), duas fases:

1. **corporativo** — `carregar_corporativo_catalogos` (copia
   categorias/tipos/valores válidos do `datatran`) e
   `carregar_corporativo_geografia` (resolve toda a hierarquia
   uf→município, rodovia→localização→local a partir das combinações
   distintas do OLTP) rodam em paralelo; depois
   `carregar_corporativo_ocorrencias` lê `acidentes` +
   `acidente_vitima` + `acidente_atributo` do `datatran` e grava o fato
   operacional (`DELETE` + reinsert completo, é carga inicial).
2. **data_marting** — nunca toca o `datatran`; lê só o corporativo e monta
   `dim_local`/`dim_classificacao_acidente`/`fato_acidentes`
   (`dim_tempo` já vem populada por `data_marting/dw_postgres.sql`, via
   `generate_series`).

```bash
docker exec dw_airflow airflow dags trigger carga_inicial_dw
```

`carregar_corporativo_ocorrencias` processa as 127671 linhas num loop Python
síncrono (sem batch); leva de 5 a 10 minutos numa máquina ociosa, mas pode
passar de 20 minutos se a máquina estiver com pouca RAM livre (o gargalo real
costuma ser memória/swap do host, não CPU — confira com `vm_stat`/`top -l 1`
antes de desconfiar do código se a carga demorar muito). Isso não é
travamento. Por isso o `docker-compose.yml` aumenta
`AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT` para 3600s (padrão do
Airflow é 300s): uma task Python longa e só com round-trips de banco não tem
ponto natural pra heartbeat, e o padrão mata a task achando que travou antes
dela terminar numa máquina mais carregada.

## Airflow — carga incremental

DAG `carga_incremental_dw`. O corte de "o que é novo" é pelo maior
`id_ocorrencia_origem` já presente em `corporativo.ocorrencias` (watermark),
não por data — o `id` do datatran é sequencial dentro do arquivo carregado.

1. **staging** (schema `staging` no banco `dw`) — só toca o `datatran`, traz
   as ocorrências com `id > watermark` (cabeçalho + vítimas + classificações).
2. **corporativo** — recopia os catálogos (pequenos, upsert idempotente),
   resolve geografia só das combinações novas da staging, insere as
   ocorrências novas (append-only, sem `DELETE`).
3. **data_marting** — dims atualizadas a partir do corporativo, fato inserido
   a partir das ocorrências que vieram da staging nesta execução.
4. **limpeza** — `limpar_staging` esvazia a staging no final.

```bash
docker exec dw_airflow airflow dags trigger carga_incremental_dw
```

## BI — Metabase, "Painel PRF — Acidentes de Trânsito"

`metabase/setup_dashboards.py` provisiona tudo via API do Metabase assim que
o container `metabase` fica pronto (sem clicar em nada na UI):

1. Cria o usuário admin (`admin@metabase.com` / `Metabase123!`), o usuário
   padrão de uso do dia a dia (`metabase@metabase.com` / `metabase` — grupo
   "All Users", sem acesso de administração) e a conexão com o banco `dw`
   (schema `public`, o data mart).
2. Registra um **mapa customizado dos estados do Brasil** em
   `Admin > Maps` — o Metabase não vem com esse mapa por padrão (só tem EUA e
   Mundo). Usa um GeoJSON público de 27 features com a sigla de cada estado
   (`codeforgermany/click_that_hood`), casando com `dim_local.sigla_uf` via
   `map.region_key`.
3. Cria uma coleção "PRF - Acidentes de Trânsito" e, dentro dela, 14
   consultas nativas (SQL) sobre `fato_acidentes`/`dim_tempo`/`dim_local`/
   `dim_classificacao_acidente`.
4. Monta o dashboard com todas elas já posicionadas numa grade de 24
   colunas.

O painel (pensando como analista da PRF, focado em onde/quando/por que
priorizar fiscalização):

- **KPIs**: total de ocorrências, óbitos, feridos, veículos envolvidos e
  taxa de letalidade (óbitos / pessoas envolvidas).
- **Mapa coroplético de óbitos por UF** — o pedido original ("mapa por
  estado"). É um *region map*, não um pin map: cada UF é colorida pela soma
  de `mortos`, usando o GeoJSON custom do passo 2.
- **Ranking de UFs por número de ocorrências** (não só óbitos — MG e SC
  lideram em volume, nem sempre coincide com quem lidera em óbitos).
- **Ocorrências por mês** e **por dia da semana** — sexta e sábado
  concentram bem mais ocorrências que o meio de semana, informação
  operacional real para escala de patrulhamento.
- **Top 10 causas de acidente**, **condição meteorológica** e **fase do
  dia** — a maioria dos acidentes é de dia e com céu claro (não é falta de
  visibilidade o principal fator neste dataset).
- **Top 10 rodovias (BR) por óbitos** e **tipo de pista** — BR-101 e BR-116
  concentram bem mais óbitos que as demais.

Todas as queries são nativas (SQL puro), então rodam mesmo sem o Metabase
"conhecer" o schema de antemão — os números só aparecem depois que a
`carga_inicial_dw` for disparada (ver seção acima); antes disso os cards
aparecem vazios, e não precisam ser recriados depois, só recarregar a
página.

`docker compose logs -f metabase-setup` mostra o progresso e, no final, a
URL exata do dashboard (`http://localhost:3000/dashboard/<id>`) — o id não é
fixo, depende da ordem de criação dentro do Metabase (que já vem com um
banco de exemplo pré-carregado).

Rodar de novo (`docker compose up metabase-setup`, ou implícito no
`start.sh`) é idempotente: se o dashboard já existe, o script não faz nada.

## Sobre o dataset (transacional)

`datatran2007.csv` está em **ISO-8859-1**, separado por `;`, com valores
ausentes representados pela string literal `(null)`. Cada linha é uma
ocorrência (acidente) já com contagens agregadas de pessoas/vítimas/veículos.
O CSV tem 4 pares de linhas duplicadas pelo mesmo `id`; o script de carga
mantém a primeira ocorrência de cada `id` e descarta a repetição. Detalhes
completos da modelagem 4FN estão no cabeçalho de `db/01_schema.sql`.

## Regerando os INSERTs do OLTP

Só é necessário se o CSV mudar. Requer apenas Python 3 (sem dependências
externas):

```bash
python3 scripts/gerar_inserts.py
```

Isso reescreve `db/02_data.sql` (~28 MB, INSERTs em lotes de 500 linhas) a
partir de `dataset/datatran2007.csv`.
# dw_trabalho_apresentacao
