# dw_trabalho_apresentacao

Cinco camadas sobre o dataset de acidentes de trânsito da PRF
(`dataset/datatran2007.csv` a `dataset/datatran2026.csv`, um CSV por ano):

1. **Transacional (OLTP)** — normalizado até a 4a Forma Normal (4FN), banco `datatran`.
2. **Data Warehouse corporativo** — modelado à Inmon (orientado por assunto, integrado,
   não-volátil, variante no tempo), schema `corporativo` do banco `dw`.
3. **Data mart em estrela** — modelado à Kimball, derivado do corporativo, schema
   `public` do banco `dw`.
4. **BI (Metabase)** — painel "Painel PRF - Acidentes de Trânsito" sobre o data
   mart, provisionado automaticamente via API.
5. **Machine Learning (`ml/`)** — clusterização de municípios + regras de
   associação entre classificações de acidente, por cluster, sobre o data
   mart.

As três primeiras vivem no mesmo servidor Postgres (`docker-compose.yml`); a
carga do transacional para o DW é feita por duas DAGs do Airflow
(`airflow/dags/`).

## Estrutura

```
dataset/
  datatran2007.csv ... datatran2026.csv   dataset original (PRF, 1 linha = 1 ocorrência, 1 CSV por ano)
db/
  01_schema.sql              DDL normalizado do OLTP (comentários com as DFs e a justificativa da 4FN)
  02-00-catalogos.sql         INSERTs dos catálogos compartilhados (uf/município/local_acidente/etc.),
                               gerados a partir do CSV (não editar à mão, ver scripts/)
  02-NN-AAAA.sql               INSERTs de acidentes/vítimas/classificações de UM ano (1 arquivo por ano
                               em dataset/, carrega depois de 02-00-catalogos.sql - ver scripts/)
scripts/
  gerar_inserts.py           lê os CSVs e gera db/02-00-catalogos.sql + um db/02-NN-AAAA.sql por ano
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
ml/
  requirements.txt             pandas, scikit-learn, mlxtend, psycopg2-binary, sqlalchemy, matplotlib
  clusterizacao_regras_associacao.py  clusterização (KMeans) de municípios + regras de
                                associação (Apriori) por cluster, sobre o data mart
  output/                      CSVs e PNGs gerados pelo script acima (gitignored)
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
Depois que o Postgres termina de carregar, o próprio Airflow dispara a carga
do DW e a clusterização/regras de associação sozinho (DAG
`bootstrap_carga_e_ml` — ver seção "Airflow — bootstrap automático"); não
precisa rodar `airflow dags trigger` à mão.

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
funcionais e da decomposição 4FN. Resumo: `acidentes` (cabeçalho + as 8
classificações de valor único, cada uma sua própria coluna/FK pra um
catálogo dedicado - `causa_acidente_valido`, `tipo_acidente_valido`, ...,
`uso_solo_valido`) + `calendario` + `local_acidente` (município +
localização) + `acidente_vitima` (categoria de vítima, tabela associativa
4FN) + `acidente_tracado_via` (a 9a classificação, `tracado_via` - tabela
associativa própria, igual a `acidente_vitima`: a PRF passou a admitir mais
de um traçado por ocorrência a partir de 2017, então não cabe como coluna
escalar das outras 8 - ver nota "DESCOBERTA" no cabeçalho do arquivo).

As 8 classificações de valor único **não** usam o padrão atributo-valor
(EAV) - uma versão anterior deste schema usou um único par genérico
`acidente_atributo`/`atributo_valor_valido` pra todas, revertido por ser um
antipadrão reconhecido quando o conjunto de atributos é fixo e conhecido de
antemão (nunca extensível em runtime): perde tipagem por coluna e torna
`NOT NULL` por atributo impossível de declarar no banco. 8 colunas tipadas,
cada uma com FK pro catálogo certo, mantêm a mesma integridade referencial
com tipagem de verdade - `NOT NULL` só em `sentido_via` (única das 8 com
zero valores nulos confirmados nas 2.237.189 ocorrências de 2007-2026; as
outras 7 têm nulos reais no dado). Ver nota completa no cabeçalho de
`db/01_schema.sql`.

Diagrama: `diagrams/der_transacional.drawio`.

## Camada 2 — Data Warehouse corporativo (Inmon), schema `corporativo` no banco `dw`

Modelo corporativo do Inmon: orientado a assunto, integrado, não-volátil e
variante no tempo. Organizado em 5 assuntos, independentes de qualquer
particularidade do OLTP:

- **Tempo** — `tempos` (calendário gerado uma vez via `generate_series`,
  2000-2035; referência compartilhada, `ocorrencias` guarda a data via FK)
- **Geografia e Via** — `ufs`, `municipios`, `rodovias`, `localizacoes`, `locais_acidente`
- **Classificação do Acidente** — `tipos_classificacao`, `classificacoes_validas`
  (catálogo atributo-valor, copiado do OLTP) + `tracados_via_validos`,
  `ocorrencia_tracado_via` (9a classificação, multivalorada desde 2017)
- **Vítimas** — `categorias_vitima` (catálogo, copiado do OLTP)
- **Ocorrências** — `ocorrencias` (fato operacional, cabeçalho enxuto),
  `ocorrencia_vitima`, `ocorrencia_classificacao`

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
  realmente observada de 8 das 9 classificações do acidente (causa, tipo,
  classificação, fase do dia, sentido da via, condição meteorológica, tipo de
  pista, uso do solo), em vez de 8 dimensões separadas ou 8 FKs soltas no
  fato. Cada campo usa `'Não informado'` no lugar de `NULL` para a `UNIQUE`
  funcionar (Postgres trata `NULL` como distinto de `NULL`)
- `fato_acidente_tracado_via` — **bridge table** (padrão Kimball) para a 9a
  classificação (`tracado_via`): desde 2017 uma ocorrência pode ter mais de
  um traçado (ex. "Reta;Curva"), então não cabe como coluna escalar da junk
  dimension acima - 0 a N linhas por fato, sem peso de alocação
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

`carregar_corporativo_ocorrencias` processa cada ocorrência num loop Python
síncrono (sem batch) - com o `datatran2007.csv` isolado (127671 linhas) leva
de 5 a 10 minutos numa máquina ociosa; com todos os anos do dataset
carregados (`dataset/datatran*.csv`, soma na casa dos milhões de linhas) a
carga é proporcionalmente mais longa, de dezenas de minutos a algumas horas,
dependendo da máquina (o gargalo real costuma ser memória/swap do host, não
CPU — confira com `vm_stat`/`top -l 1` antes de desconfiar do código se a
carga demorar muito). Isso não é travamento. Por isso o `docker-compose.yml`
aumenta `AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT` para 3600s
(padrão do Airflow é 300s): uma task Python longa e só com round-trips de
banco não tem ponto natural pra heartbeat, e o padrão mata a task achando
que travou antes dela terminar numa máquina mais carregada.

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

## Airflow — clusterização + regras de associação (ML)

DAG `ml_clusterizacao_regras_associacao`, uma única task que chama
`rodar_pipeline()` de `ml/clusterizacao_regras_associacao.py` (mesmo código do
CLI standalone, ver seção "Machine Learning" abaixo) com `host="postgres"`
(rede interna do Docker Compose, não `localhost`). Disparo manual, depois de
pelo menos uma `carga_inicial_dw` (precisa de `fato_acidentes` populado):

```bash
docker exec dw_airflow airflow dags trigger ml_clusterizacao_regras_associacao
```

Regrava `public.cluster_municipio`/`public.regra_associacao` do zero
(TRUNCATE + insert) e os PNGs em `ml/output/` — pode ser disparada de novo a
qualquer momento (ex.: depois de uma `carga_incremental_dw`) sem reprocessar
o resto do DW. `docker-compose.yml` monta `./ml` dentro do container do
Airflow (`/opt/airflow/dags/ml`) e instala `pandas`/`scikit-learn`/`mlxtend`/
`matplotlib` nele via `_PIP_ADDITIONAL_REQUIREMENTS` (além do `psycopg2-binary`
já usado pelas outras DAGs).

## Airflow — bootstrap automático

Os três disparos manuais acima (`carga_inicial_dw`, depois
`ml_clusterizacao_regras_associacao`) acontecem sozinhos na primeira subida
dos containers, via DAG `bootstrap_carga_e_ml`
(`airflow/dags/bootstrap_carga_e_ml.py`) - não precisa rodar nenhum
`docker exec ... airflow dags trigger` à mão depois de um `start.sh`.

Funciona porque este Airflow roda `standalone` sem volume de metadados
(SQLite dentro do container, perdido a cada `docker compose down`/
`--force-recreate` - ver `docker-compose.yml`): os metadados SEMPRE nascem
vazios junto com o Postgres, então `schedule="@once"` + `is_paused_upon_creation=False`
disparam o DAG sozinho assim que o scheduler sobe, exatamente uma vez por
ambiente recriado do zero. A primeira task confere se `fato_acidentes` já
tem linhas antes de disparar qualquer coisa (cobre o caso de só reiniciar o
mesmo container, sem recriar - não refaz a carga à toa). As duas tasks
seguintes usam `TriggerDagRunOperator(wait_for_completion=True)` pra
disparar `carga_inicial_dw` e, só depois dela terminar com sucesso,
`ml_clusterizacao_regras_associacao` - a mesma dependência sequencial que os
dois `docker exec` manuais acima expressam, só que dentro do próprio
Airflow.

Continua possível disparar cada DAG manualmente a qualquer momento (ex.:
depois de editar o pipeline de ML e querer só re-rodar aquela parte) - o
bootstrap só cobre a carga inicial do zero.

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
3. Cria uma coleção "PRF - Acidentes de Trânsito" e, dentro dela, 17
   consultas nativas (SQL) sobre `fato_acidentes`/`dim_tempo`/`dim_local`/
   `dim_classificacao_acidente` + `cluster_municipio`/`regra_associacao`.
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
- **Municípios por Cluster**, **Perfil de Risco por Cluster** e **Top Regras
  de Associação por Cluster** (última linha do painel) — saída da DAG
  `ml_clusterizacao_regras_associacao` (seção acima): quantos municípios caem
  em cada cluster de risco, o perfil médio de cada um, e as regras de
  associação (causa/clima/tipo de pista etc.) mais fortes (maior lift)
  específicas de cada cluster.

Todas as queries são nativas (SQL puro), então rodam mesmo sem o Metabase
"conhecer" o schema de antemão — os números só aparecem depois que a
`carga_inicial_dw` for disparada (ver seção acima; os 3 cards de ML também
precisam da `ml_clusterizacao_regras_associacao`); antes disso os cards
aparecem vazios/com erro, e não precisam ser recriados depois, só recarregar
a página.

`docker compose logs -f metabase-setup` mostra o progresso e, no final, a
URL exata do dashboard (`http://localhost:3000/dashboard/<id>`) — o id não é
fixo, depende da ordem de criação dentro do Metabase (que já vem com um
banco de exemplo pré-carregado).

Rodar de novo (`docker compose up metabase-setup`, ou implícito no
`start.sh`) é idempotente: se o dashboard já existe, o script não faz nada.

## Machine Learning — clusterização + regras de associação, `ml/`

Atividade da disciplina: combinar clusterização de "clientes" com Regras de
Associação (ensemble clusterização + Apriori) — primeiro segmentar, depois
descobrir padrões de associação específicos de cada segmento, em vez de
rodar Apriori na base inteira de uma vez.

O dataset de origem (PRF-DATATRAN, acidentes de trânsito) não tem
clientes/produtos, então o enunciado foi adaptado ao domínio disponível:

- **"Cliente" = município** (`nome_municipio` + `sigla_uf`). Cada município
  com pelo menos `--min-ocorrencias` (default 30) ganha um perfil de risco -
  taxa de letalidade, taxa de feridos graves, veículos médios por
  ocorrência, proporção de ocorrências em fim de semana/período noturno/
  chuva/pista simples, concentração da causa principal e volume - e é
  agrupado por **KMeans** (k escolhido automaticamente pelo melhor
  *silhouette score* entre 2 e 8, salvo em `output/silhouette_por_k.png`).
- **"Produto" = cada classificação de uma ocorrência** (causa, tipo de
  acidente, classificação quanto a vítimas, fase do dia, sentido da via,
  condição meteorológica, tipo de pista, traçado da via, uso do solo) - uma
  ocorrência é uma "cesta" com até 9 itens (um por tipo de classificação;
  valores não informativos como `Ignorado`/`Não informado` são descartados
  da cesta). Dentro de cada cluster de municípios, as cestas das suas
  ocorrências alimentam **Apriori** (`mlxtend`) e **regras de associação**
  (ordenadas por *lift*, filtradas por confiança mínima) - cada cluster
  descobre os próprios padrões de causa/condição/tipo de acidente, em vez de
  um conjunto genérico de regras para o Brasil inteiro.

Forma recomendada - via Airflow, direto no ambiente do `docker-compose.yml`
(ver seção "Airflow — clusterização + regras de associação" acima), já grava
em `public.cluster_municipio`/`public.regra_associacao` pro Metabase:

```bash
docker exec dw_airflow airflow dags trigger ml_clusterizacao_regras_associacao
```

Ou standalone, fora do Airflow (útil pra iterar nos parâmetros sem re-disparar
DAG, ou rodar fora do `docker-compose.yml`):

```bash
cd ml
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 clusterizacao_regras_associacao.py
```

Standalone, por padrão conecta em `localhost:5432` (mesmas credenciais do
`docker-compose.yml`; a DAG usa `host="postgres"`, a rede interna do Compose)
e exige a carga do data mart já feita (`fato_acidentes` populado - ver seção
Airflow acima). Use `--help` para ver todos os parâmetros
(`--min-ocorrencias`, `--k`, `--min-support`, `--min-lift`,
`--min-confidence`, `--top-n-rules`, `--no-persist`).

Saídas em `ml/output/`:

- `clusters_municipios.csv` — um município por linha, com perfil + cluster.
- `regras_associacao_por_cluster.csv` — até `--top-n-rules` regras por
  cluster (antecedente, consequente, suporte, confiança, lift).
- `silhouette_por_k.png` — escolha de k.
- `perfil_clusters.png` — heatmap do perfil médio de cada cluster.

O script também grava os mesmos resultados em duas tabelas novas no banco
`dw` (schema `public`, recriadas do zero a cada execução): `cluster_municipio`
e `regra_associacao` — ficam disponíveis para consulta/BI do mesmo jeito que
o resto do data mart (desligável com `--no-persist`).

## Sobre o dataset (transacional)

`dataset/datatranAAAA.csv` (um arquivo por ano, 2007-2026) está em
**ISO-8859-1**, separado por `;`, com valores ausentes representados pela
string literal `(null)`. Cada linha é uma ocorrência (acidente) já com
contagens agregadas de pessoas/vítimas/veículos. Há linhas duplicadas pelo
mesmo `id` dentro de um mesmo CSV; o script de carga mantém a primeira
ocorrência de cada `id` (por arquivo) e descarta a repetição - o `id`
original não serve de chave global (a PRF reinicia a numeração entre anos),
por isso `acidentes.id` é um id sintético sequencial, único e crescente
entre TODOS os anos (não reinicia por ano, mesmo com a carga dividida em um
arquivo por ano - ver seção seguinte). Detalhes completos da modelagem 4FN,
incluindo a decomposição de `tracado_via` (multivalorado desde 2017), estão
no cabeçalho de `db/01_schema.sql`.

## Regerando os INSERTs do OLTP

Só é necessário se algum CSV em `dataset/` mudar (ou um novo ano for
adicionado). Requer apenas Python 3 (sem dependências externas):

```bash
python3 scripts/gerar_inserts.py
```

Isso reescreve `db/02-00-catalogos.sql` (catálogos compartilhados entre
anos: uf, município, local_acidente, classificações válidas etc.) e um
`db/02-NN-AAAA.sql` por CSV encontrado em `dataset/` (~1.1 GB somados, todos
os anos de 2007 a 2026, INSERTs em lotes de 500 linhas). Um arquivo por ano
em vez de um `02_data.sql` monolítico, de propósito:

- **Carga parcial rápida pra testar** - só os catálogos + os anos que
  interessam, sem esperar os outros. Ex.: só 2007 e 2008 num Postgres
  isolado:
  ```bash
  docker run --rm -d --name pg_teste -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=datatran \
    -v "$PWD/db/01_schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro" \
    -v "$PWD/db/02-00-catalogos.sql:/docker-entrypoint-initdb.d/02-00-catalogos.sql:ro" \
    -v "$PWD/db/02-01-2007.sql:/docker-entrypoint-initdb.d/02-01-2007.sql:ro" \
    -v "$PWD/db/02-02-2008.sql:/docker-entrypoint-initdb.d/02-02-2008.sql:ro" \
    postgres:16
  ```
- **Isolamento de falha** - cada arquivo tem seu próprio `BEGIN`/`COMMIT`;
  um problema nos dados de um ano só desfaz aquele ano, não os outros 19
  (o `02_data.sql` antigo era uma transação única pra tudo).

`docker-compose.yml` monta cada arquivo gerado como um script
`docker-entrypoint-initdb.d` próprio (`02-00-catalogos.sql` antes de
qualquer `02-NN-AAAA.sql`, ordem entre os anos não importa). Se a lista de
anos em `dataset/` mudar, os volumes em `docker-compose.yml` (serviço
`postgres`) precisam ser atualizados manualmente pra acompanhar.
