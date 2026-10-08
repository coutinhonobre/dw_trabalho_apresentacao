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
- **Classificação do Acidente** — 8 catálogos dedicados (`causas_acidente`,
  `tipos_acidente`, ..., `usos_solo`), um por classificação de valor único,
  espelhando a mesma decomposição do OLTP (ver nota "REMODELAGEM" abaixo) +
  `tracados_via_validos`, `ocorrencia_tracado_via` (9a classificação,
  multivalorada desde 2017)
- **Vítimas** — `categorias_vitima` (catálogo, copiado do OLTP)
- **Ocorrências** — `ocorrencias` (fato operacional, cabeçalho enxuto + as 8
  classificações como colunas próprias, cada uma FK pro catálogo certo),
  `ocorrencia_vitima`, `ocorrencia_tracado_via`

**REMODELAGEM**: a 1a versão desta camada mantinha as 8 classificações como
atributo-valor genérico (EAV: `tipos_classificacao`/`classificacoes_validas`/
`ocorrencia_classificacao`), espelhando o EAV que o OLTP tinha então. Quando o
OLTP trocou esse EAV por colunas tipadas (ver seção "Camada 1" acima), o
motivo documentado lá - antipadrão quando o conjunto de atributos é fixo e
conhecido de antemão - vale igual aqui: manter EAV no corporativo depois que
a fonte deixou de ser EAV só reintroduziria a mesma perda de tipagem/`NOT
NULL`, sem ganho de integração em troca. Remodelado pra 8 colunas tipadas,
cada uma FK pro catálogo dedicado certo, mesma decomposição do OLTP. Ver nota
completa no cabeçalho de `dw/dw_postgres.sql`.

Integrado: `ocorrencias` carrega `sistema_origem` (fixo em `'PRF-DATATRAN'`
hoje) + `id_ocorrencia_origem` (chave natural do CSV) ao lado da chave
substituta corporativa (sequência própria) — isso é o que permite empilhar
`datatran2008.csv`, `datatran2009.csv` etc. no mesmo `corporativo.ocorrencias`
ano após ano sem colidir PKs nem reprocessar histórico, mesmo havendo hoje uma
única fonte real.

Não-volátil / variante no tempo: `ocorrencias`/`ocorrencia_vitima` são
append-only — uma ocorrência carregada nunca sofre `UPDATE`. Diferente do
projeto de referência que inspirou esta
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

O seed documenta 17 tabelas / 41 campos do `datatran`, 21 tabelas (corporativo
+ data mart) / 109 campos do DW e 72 linhas de linhagem — incluindo os campos
sem origem transacional, atribuídos pelo próprio ETL (`sistema_origem` fixo,
sentinela `'Não informado'`, totais recalculados), ancorados em um
`dado_externo` de "Regras de Negócio do ETL". Atualizado junto com a
remodelagem do EAV→colunas tipadas (seções "Camada 1"/"Camada 2" acima) - as
tabelas antigas (`tipo_atributo`, `atributo_valor_valido`, `acidente_atributo`
no OLTP; `tipos_classificacao`/`classificacoes_validas`/
`ocorrencia_classificacao` no corporativo) não existem mais em nenhuma
camada, nem no seed.

Diagrama: `diagrams/metadados_modelo.drawio`.

## Airflow — carga inicial

DAG `carga_inicial_dw`, disparo manual (`schedule=None`), duas fases:

1. **corporativo** — `carregar_corporativo_catalogos` (copia
   categorias/classificações válidas do `datatran`, incluindo os 8 catálogos
   de classificação) e `carregar_corporativo_geografia` (resolve toda a
   hierarquia uf→município, rodovia→localização→local a partir das
   combinações distintas do OLTP, em lote - ver abaixo) rodam em paralelo;
   depois `carregar_corporativo_ocorrencias` lê `acidentes` +
   `acidente_vitima` + `acidente_tracado_via` do `datatran` (as 8
   classificações vêm direto como colunas de `acidentes`, não mais de uma
   tabela separada) e grava o fato operacional (`DELETE` + reinsert
   completo, é carga inicial).
2. **data_marting** — nunca toca o `datatran`; lê só o corporativo e monta
   `dim_local`/`dim_classificacao_acidente`/`fato_acidentes`
   (`dim_tempo` já vem populada por `data_marting/dw_postgres.sql`, via
   `generate_series`).

```bash
docker exec dw_airflow airflow dags trigger carga_inicial_dw
```

**Processamento em lote**: `carregar_corporativo_ocorrencias` e as duas
funções do data_marting processam `TAMANHO_LOTE` (5000) ocorrências por vez,
paginando por id, em vez de um `fetchall()` da tabela inteira - com todos os
anos carregados (2,2M+ linhas), ler tudo de uma vez + os dicts de pivot de
vítima/traçado em memória já causou OOM (`SIGKILL`) numa task do Airflow.
`carregar_corporativo_geografia` resolve as ~368k combinações distintas de
geografia via upsert em lote (`resolver_geografia_em_lote`, `execute_values`)
em vez de round-trip por combinação - o loop antigo levava mais de 1h só
nessa etapa; `id_tempo`/`id_local` na carga de ocorrências vêm de mapas
carregados uma vez (`carregar_mapa_tempo`/`carregar_mapa_local_acidente`) em
vez de round-trip por ocorrência. Com isso, os 2,2M+ linhas completos levam
em torno de 1h (medido); por ano isolado (ex. só `datatran2007.csv`, ~127k
linhas) é bem mais rápido. O gargalo real costuma ser mesmo assim
memória/round-trips de banco, não CPU - por isso o `docker-compose.yml`
aumenta `AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT` para 3600s
(padrão do Airflow é 300s): uma task Python longa e com muitos round-trips de
banco não tem ponto natural pra heartbeat, e o padrão mata a task achando que
travou antes dela terminar numa máquina mais carregada.

**Dois problemas de infraestrutura do Airflow 3.x achados rodando a carga
completa (não são bugs do código de carga em si), os dois já corrigidos:**

1. **JWT do heartbeat expira antes da task terminar.** No Airflow 3.x a task
   autentica os próprios heartbeats contra o api-server via um JWT de vida
   curta (`[execution_api] jwt_expiration_time`, default 600s = 10min). A
   reemissão automática desse token ("JWT reissue middleware") não se mostrou
   confiável aqui - quando falha, os heartbeats passam a ser rejeitados com
   403 ("Signature has expired"), e depois de 3 falhas seguidas o supervisor
   do Airflow **mata a task** (SIGTERM depois SIGKILL) achando que ela
   travou, mesmo ela viva e progredindo normal - sem nenhum erro no log da
   própria task (o motivo só aparece no log do scheduler, não no log da
   task). `AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT` (acima)
   **não resolve isso sozinho** - é uma config diferente (timeout de
   detecção de travamento, não de autenticação do heartbeat em si). Corrigido
   com `AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME: "3600"` no
   `docker-compose.yml`.
2. **Runs concorrentes corrompendo a carga.** Nenhuma das duas DAGs de carga
   tinha `max_active_runs` definido. Combinado com o problema 1 (uma task
   "morta" sem erro visível leva quem está esperando - ex. a DAG de bootstrap,
   ver seção abaixo - a retentar o disparo), isso permitiu DUAS runs de
   `carga_inicial_dw` rodando ao mesmo tempo, uma fazendo `DELETE` em
   `corporativo.ocorrencias` enquanto a outra estava no meio do insert - as
   duas falham. Corrigido com `max_active_runs=1` nas duas DAGs
   (`carga_inicial_dw`/`carga_incremental_dw`): uma segunda tentativa de
   disparo agora fica na fila em vez de rodar em paralelo.

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
(rede interna do Docker Compose, não `localhost`). A DAG tem
`params={"ano": 2024}` - os 20 anos juntos (~2,2M ocorrências) estouram a
memória do Apriori por cluster (ver seção "Machine Learning" abaixo); pra
analisar outro ano, dispara com "Trigger DAG w/ config" trocando o valor.
Disparo manual, depois de pelo menos uma `carga_inicial_dw` (precisa de
`fato_acidentes` populado):

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
3. Cria uma coleção "PRF - Acidentes de Trânsito" e, dentro dela, 18
   consultas nativas (SQL) sobre `fato_acidentes`/`dim_tempo`/`dim_local`/
   `dim_classificacao_acidente` + `cluster_municipio`/`regra_associacao`.
4. Monta o dashboard com todas elas já posicionadas numa grade de 24
   colunas, com cores aplicadas por card (ver "Paleta de cores" abaixo).

**Importante ao editar `metabase/setup_dashboards.py`**: diferente de
`airflow/dags` (bind mount, qualquer edição já reflete no container),
`metabase/Dockerfile` faz `COPY setup_dashboards.py .` - a imagem só pega
uma mudança no script depois de `docker compose build metabase-setup`.
Reaplicar (depois de editar o script):

```bash
docker compose build metabase-setup
# apaga o dashboard atual + os cards (senão o setup só vê "dashboard já
# existe" e não recria nada - idempotência por nome) via API do Metabase,
# depois:
docker compose rm -f metabase-setup && docker compose up -d metabase-setup
```

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
- **Municípios por Cluster**, **Perfil de Risco por Cluster (%)** e **Top
  Regras de Associação por Cluster** (última linha do painel) — saída da DAG
  `ml_clusterizacao_regras_associacao` (seção acima): quantos municípios caem
  em cada cluster de risco, o perfil médio de cada um (gráfico de barras
  agrupado por métrica - taxa de letalidade, feridos graves, proporção fim de
  semana/noite/chuva/pista simples, concentração de causa - todas em %; não é
  mais uma tabela larga de 11 colunas decimais cruas, que cortava na tela), e
  as regras de associação (causa/clima/tipo de pista etc., 1 antecedente -> 1
  consequente - ver `--max-itemset-len` na seção "Machine Learning") mais
  fortes (maior lift) específicas de cada cluster, com suporte/confiança em
  percentual.

  Os clusters são identificados por **rótulo calculado**, não pelo número
  arbitrário que o KMeans atribui ("Cluster 0"/"Cluster 1" não diz nada pra
  quem lê o painel, e esse número pode trocar numa próxima rodada da DAG).
  Uma CTE SQL compartilhada pelos 3 cards (`ROTULO_CLUSTER_CTE` em
  `metabase/setup_dashboards.py`) ranqueia os clusters de
  `cluster_municipio` pela letalidade média toda vez que o card roda -
  "Maior Letalidade"/"Menor Letalidade" (ou variações pra k > 2) -, sempre
  consistente com os dados atuais.

Todas as queries são nativas (SQL puro), então rodam mesmo sem o Metabase
"conhecer" o schema de antemão — os números só aparecem depois que a
`carga_inicial_dw` for disparada (ver seção acima; os 3 cards de ML também
precisam da `ml_clusterizacao_regras_associacao`); antes disso os cards
aparecem vazios/com erro, e não precisam ser recriados depois, só recarregar
a página.

### Paleta de cores

Tema "problemática de acidentes" (abre com vermelho), validado com a skill de
dataviz do projeto (CVD/contraste/lightness-band, não só "achei bonito" -
`node scripts/validate_palette.js` dessa skill). OSS Metabase **não** deixa
recolorir globalmente - `application-colors` é feature paga ("whitelabel"; a
API recusa com "recurso :whitelabel não está disponível" na versão grátis).
Por isso as cores são aplicadas por card:

- **Gráficos de série única** (bar/row/line) — `visualization_settings.series_settings.<coluna>.color`.
  Mesma métrica = mesma cor em todo o painel (ex. "ocorrencias" sempre
  vermelho), pra reforçar identidade visual em vez de cor arbitrária por
  gráfico.
- **Pizza** (`fase_dia`, `tipo_pista`) — `visualization_settings.pie.colors`,
  um dict `{valor_da_categoria: cor}`.
- **Barra agrupada** (`perfil_risco_cluster`, com `rotulo` como 2a dimensão/
  série) — mesmo `series_settings`, mas chaveado pelo *valor* do rótulo
  ("Maior Letalidade" → vermelho, "Menor Letalidade" → azul - o par
  diverging validado pela skill, polos quente/frio que leem como opostos,
  em vez do clássico vermelho/verde, ruim pra daltonismo; faz sentido aqui
  porque os rótulos são de fato uma polaridade de risco, não categorias
  arbitrárias).
- **Mapa coroplético** (`mapa_obitos_uf`) — `visualization_settings.map.colors`
  (array, rampa sequencial), **não** `color` (singular - a API aceita sem
  erro mas o renderer não lê; a chave certa foi confirmada lendo o bundle
  `map-renderer.js` de dentro do próprio `metabase.jar`, já que não aparece
  documentada em lugar nenhum). Rampa vermelha clara→escura (menos→mais
  óbitos) - validada como *sequencial* pela skill (monotonicidade de
  luminosidade + mesma matiz; o degrau mais claro quase sumir no branco é
  esperado aqui, representa "zero óbitos" - diferente de uma rampa ordinal,
  que exigiria contraste mínimo até no degrau mais claro).

Gráficos single-dimension sem breakout (ex. `municipios_por_cluster`, que só
tem `rotulo` como dimensão e `municipios` como métrica) pintam a série
inteira com UMA cor - Metabase não colore por categoria do eixo x sem uma 2a
dimensão/breakout, diferente de pizza.

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
`--min-confidence`, `--max-itemset-len`, `--top-n-rules`, `--no-persist`,
`--ano`).

**`--ano` (recomendado)**: restringe a clusterização/regras a um único ano em
vez dos 20 juntos. Com o dataset completo (~2,2M ocorrências), o Apriori por
cluster (`TransactionEncoder` + `apriori`) soma com o resto do processo
Python (pandas/sklearn/mlxtend já carregados) mais memória do que o container
do Airflow costuma ter disponível, e a task morre com `SIGKILL` sem
traceback. Um ano por vez (~70-190k ocorrências, variando por ano) cabe
confortavelmente - e como efeito colateral permite comparar clusters/regras
entre anos. A DAG expõe isso como parâmetro (`params={"ano": 2024}` em
`airflow/dags/ml_clusterizacao_regras_associacao.py`) - pra analisar outro
ano, dispara a DAG com "Trigger DAG w/ config" trocando o valor.

**`--max-itemset-len`** (default 2, ou seja regras 1 antecedente -> 1
consequente): o Apriori encontra closure por especialização - uma vez que um
par forte existe (ex. causa=X -> tipo_acidente=Y), toda superset dele (+1
item de contexto) também passa o filtro de suporte/lift, inundando o "top N
por lift" com N variações do mesmo achado em vez de N achados diferentes, e
itemsets grandes viram texto longo demais pra uma tabela de dashboard. Regras
espelhadas (A->C e C->A, que sempre têm o mesmo lift - a fórmula é simétrica)
também são descartadas automaticamente, mantendo só a direção de maior
confiança de cada par.

A implementação do `apriori()` (`mlxtend`) usa `low_memory=True`: o caminho
padrão da lib monta um array denso 3D (linhas x combinações x tamanho do
itemset) pra avaliar todos os itemsets de um tamanho de uma vez - com ~1M+
transações por cluster isso sozinho já passa de alguns GB. `low_memory=True`
troca isso por um gerador que avalia uma combinação por vez (3-6x mais lento,
mesmo resultado estatístico - confirmado comparando os dois modos num
dataset sintético).

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

**Maiúscula/acento inconsistente entre anos**: a PRF não manteve
maiúscula/acento consistentes em 4 das 8 classificações
(`causa_acidente`, `tipo_acidente`, `fase_dia`, `condicao_metereologica`) -
ex. "Ceu Claro" num ano do CSV, "Céu Claro" noutro, mesmo significado. Sem
normalizar, isso vira duas linhas de catálogo distintas (uma delas, "Céu
Claro"/"Ceu Claro", respondia sozinha por ~750 mil das 2,2M ocorrências),
duplicando categoria em gráfico e diluindo suporte/confiança nas regras de
associação (o Apriori trata como dois itens diferentes). `NORMALIZACAO_CLASSIFICACAO`
em `scripts/gerar_inserts.py` mapeia cada variante pra uma forma canônica
antes de gerar os INSERTs - afeta só a geração a partir daqui pra frente
(regenerar + recarregar, ver seção seguinte, pra uma base já carregada
herdar a correção).

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
