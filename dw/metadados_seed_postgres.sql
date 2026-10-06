\c dw

-- Seed do catálogo de metadados para este projeto (execução manual, ver
-- README: não roda em docker-entrypoint-initdb.d de propósito, igual ao
-- projeto de referência). Documenta 12 tabelas / 33 campos transacionais
-- (banco `datatran`, ../db/01_schema.sql), 12 tabelas / 52 campos do
-- corporativo (Inmon) e 4 tabelas / 43 campos do data mart em estrela, com a
-- linhagem completa entre eles.
--
-- PENDENTE: tracado_via_valido/acidente_tracado_via (OLTP),
-- corporativo.tracados_via_validos/ocorrencia_tracado_via e
-- fato_acidente_tracado_via (data mart) - introduzidas depois deste seed
-- (ver nota "DESCOBERTA" em ../db/01_schema.sql) - ainda não catalogadas
-- aqui. O campo `tracado_via` de dim_classificacao_acidente foi removido
-- deste seed (deixou de existir na tabela).
--
-- `veiculo_envolvido`/`pessoa_envolvida` (OLTP) e seus espelhos
-- `corporativo.veiculos_envolvidos`/`pessoas_envolvidas` foram REMOVIDOS do
-- schema (e por isso daqui) - eram "extensão conceitual", permanentemente
-- vazios, sem nenhum código de ETL escrevendo neles. Documentar metadado
-- pra tabela que não existe mais não faz sentido.

INSERT INTO metadados.sistema_transacional (id_sistema, nome_sistema, descricao, plataforma) VALUES
(1, 'DATATRAN', 'Sistema transacional de acidentes de trânsito em rodovias federais (PRF)', 'PostgreSQL 16');

INSERT INTO metadados.tipo_campo (id_tipo_campo, nome_tipo_campo, descricao) VALUES
(1, 'Inteiro', 'Valores numéricos inteiros'),
(2, 'Texto', 'Cadeias de caracteres'),
(3, 'Data', 'Datas sem hora'),
(4, 'Data/Hora', 'Datas com hora (timestamp) ou apenas hora'),
(5, 'Decimal', 'Valores numéricos com casas decimais'),
(6, 'Booleano', 'Valores lógicos (verdadeiro/falso)'),
(7, 'Binário', 'Dados binários (imagens, arquivos)');

INSERT INTO metadados.tabela_transacional (id_tabela, id_sistema, nome_tabela, descricao) VALUES
(1, 1, 'uf', 'Sigla/nome de unidade federativa'),
(2, 1, 'rodovia', 'Número de BR'),
(3, 1, 'localizacao', 'Ponto físico (rodovia, km) na malha'),
(4, 1, 'municipio', 'Município, chave natural (nome, uf)'),
(5, 1, 'local_acidente', 'Consolida município + localização de uma ocorrência'),
(6, 1, 'tipo_atributo', 'Catálogo dos 9 tipos de classificação de uma ocorrência'),
(7, 1, 'atributo_valor_valido', 'Catálogo dos valores válidos por tipo de classificação'),
(8, 1, 'calendario', 'Calendário com dia da semana e ano por data de ocorrência'),
(9, 1, 'categoria_vitima', 'Catálogo das 5 categorias de vítima'),
(10, 1, 'acidentes', 'Cabeçalho da ocorrência (1 linha = 1 acidente)'),
(11, 1, 'acidente_vitima', 'Contagem de vítimas por categoria e ocorrência'),
(12, 1, 'acidente_atributo', 'Classificação atributo-valor por ocorrência');

INSERT INTO metadados.campo_transacional (id_campo, id_tabela, id_tipo_campo, nome_campo, descricao, mascara_campo, tamanho_campo, casa_decimal) VALUES
(1, 1, 2, 'sigla', NULL, NULL, 2, NULL),
(2, 1, 2, 'nome', NULL, NULL, 30, NULL),
(3, 2, 1, 'numero', NULL, NULL, NULL, NULL),
(4, 3, 1, 'id', NULL, NULL, NULL, NULL),
(5, 3, 1, 'rodovia_numero', NULL, NULL, NULL, NULL),
(6, 3, 5, 'km', NULL, NULL, 6, 1),
(7, 4, 1, 'id', NULL, NULL, NULL, NULL),
(8, 4, 2, 'nome', NULL, NULL, 60, NULL),
(9, 4, 2, 'uf_sigla', NULL, NULL, 2, NULL),
(10, 5, 1, 'id', NULL, NULL, NULL, NULL),
(11, 5, 1, 'municipio_id', NULL, NULL, NULL, NULL),
(12, 5, 1, 'localizacao_id', NULL, NULL, NULL, NULL),
(13, 6, 2, 'tipo_atributo', NULL, NULL, 30, NULL),
(14, 6, 2, 'descricao', NULL, NULL, 60, NULL),
(15, 6, 6, 'obrigatorio', NULL, NULL, NULL, NULL),
(16, 7, 2, 'tipo_atributo', NULL, NULL, 30, NULL),
(17, 7, 2, 'valor', NULL, NULL, 60, NULL),
(18, 8, 3, 'data', NULL, NULL, NULL, NULL),
(19, 8, 2, 'dia_semana', NULL, NULL, 15, NULL),
(20, 8, 1, 'ano', NULL, NULL, NULL, NULL),
(21, 9, 2, 'categoria', NULL, NULL, 20, NULL),
(22, 9, 2, 'descricao', NULL, NULL, 30, NULL),
(23, 10, 1, 'id', NULL, NULL, NULL, NULL),
(24, 10, 3, 'data', NULL, NULL, NULL, NULL),
(25, 10, 4, 'horario', NULL, NULL, NULL, NULL),
(26, 10, 1, 'local_id', NULL, NULL, NULL, NULL),
(27, 10, 1, 'veiculos', NULL, NULL, NULL, NULL),
(28, 11, 1, 'acidente_id', NULL, NULL, NULL, NULL),
(29, 11, 2, 'categoria', NULL, NULL, 20, NULL),
(30, 11, 1, 'quantidade', NULL, NULL, NULL, NULL),
(31, 12, 1, 'acidente_id', NULL, NULL, NULL, NULL),
(32, 12, 2, 'tipo_atributo', NULL, NULL, 30, NULL),
(33, 12, 2, 'valor', NULL, NULL, 100, NULL);

INSERT INTO metadados.assunto_dw (id_assunto, nome_assunto, descricao) VALUES
(1, 'Tempo', 'Calendário corporativo (corporativo.tempos)'),
(2, 'Geografia e Via', 'UF, município, rodovia, localização e local do acidente'),
(3, 'Classificação do Acidente', 'Catálogo atributo-valor das 9 classificações de uma ocorrência'),
(4, 'Vítimas', 'Catálogo das categorias de vítima'),
(5, 'Ocorrências', 'Fato operacional do corporativo: cabeçalho, vítimas e classificações por ocorrência'),
(7, 'Dimensões do Data Mart', 'Dimensões conformadas do data mart em estrela'),
(8, 'Fato Acidentes', 'Fato dimensional do data mart em estrela');

INSERT INTO metadados.tabela_dw (id_tabela_dw, id_assunto, nome_tabela, tipo_tabela, grao, periodicidade_carga) VALUES
(1, 1, 'corporativo.tempos', 'Corporativo', '1 linha por dia', 'Estática (gerada uma vez)'),
(2, 2, 'corporativo.ufs', 'Corporativo', '1 linha por UF', 'Sob demanda (upsert simples)'),
(3, 2, 'corporativo.municipios', 'Corporativo', '1 linha por município', 'Sob demanda (upsert simples)'),
(4, 2, 'corporativo.rodovias', 'Corporativo', '1 linha por rodovia', 'Sob demanda (upsert simples)'),
(5, 2, 'corporativo.localizacoes', 'Corporativo', '1 linha por (rodovia, km)', 'Sob demanda (upsert simples)'),
(6, 2, 'corporativo.locais_acidente', 'Corporativo', '1 linha por (município, localização)', 'Sob demanda (upsert simples)'),
(7, 3, 'corporativo.tipos_classificacao', 'Corporativo', '1 linha por tipo de classificação', 'Sob demanda (cópia do catálogo OLTP)'),
(8, 3, 'corporativo.classificacoes_validas', 'Corporativo', '1 linha por (tipo, valor) válido', 'Sob demanda (cópia do catálogo OLTP)'),
(9, 4, 'corporativo.categorias_vitima', 'Corporativo', '1 linha por categoria de vítima', 'Sob demanda (cópia do catálogo OLTP)'),
(10, 5, 'corporativo.ocorrencias', 'Corporativo', '1 linha por ocorrência', 'Incremental (watermark por id de origem)'),
(11, 5, 'corporativo.ocorrencia_vitima', 'Corporativo', '1 linha por (ocorrência, categoria de vítima)', 'Incremental, append-only'),
(12, 5, 'corporativo.ocorrencia_classificacao', 'Corporativo', '1 linha por (ocorrência, tipo de classificação)', 'Incremental, append-only'),
(15, 7, 'dim_tempo', 'Dimensão', '1 linha por dia', 'Estática (gerada uma vez)'),
(16, 7, 'dim_local', 'Dimensão', '1 linha por local de acidente (conformada com corporativo.locais_acidente)', 'Incremental'),
(17, 7, 'dim_classificacao_acidente', 'Dimensão', '1 linha por combinação observada das 9 classificações (junk dimension)', 'Incremental'),
(18, 8, 'fato_acidentes', 'Fato', '1 linha por ocorrência', 'Incremental (watermark por id de origem)');

INSERT INTO metadados.campo_dw (id_campo, id_tabela_dw, id_tipo_campo, nome_campo, descricao, papel_campo, tamanho_campo, casa_decimal) VALUES
-- corporativo.tempos (1)
(1, 1, 1, 'id_tempo', NULL, 'PK', NULL, NULL),
(2, 1, 3, 'data', NULL, 'Atributo', NULL, NULL),
(3, 1, 1, 'dia', NULL, 'Atributo', NULL, NULL),
(4, 1, 1, 'mes', NULL, 'Atributo', NULL, NULL),
(5, 1, 2, 'nome_mes', NULL, 'Atributo', 20, NULL),
(6, 1, 1, 'trimestre', NULL, 'Atributo', NULL, NULL),
(7, 1, 1, 'ano', NULL, 'Atributo', NULL, NULL),
(8, 1, 2, 'dia_semana', NULL, 'Atributo', 20, NULL),
(9, 1, 6, 'flag_fim_semana', NULL, 'Atributo', NULL, NULL),
-- corporativo.ufs (2)
(10, 2, 1, 'id_uf', NULL, 'PK', NULL, NULL),
(11, 2, 2, 'sigla', NULL, 'Atributo', 2, NULL),
(12, 2, 2, 'nome', NULL, 'Atributo', 30, NULL),
(13, 2, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.municipios (3)
(14, 3, 1, 'id_municipio', NULL, 'PK', NULL, NULL),
(15, 3, 2, 'nome', NULL, 'Atributo', 60, NULL),
(16, 3, 1, 'id_uf', NULL, 'FK', NULL, NULL),
(17, 3, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.rodovias (4)
(18, 4, 1, 'id_rodovia', NULL, 'PK', NULL, NULL),
(19, 4, 1, 'numero', NULL, 'Atributo', NULL, NULL),
(20, 4, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.localizacoes (5)
(21, 5, 1, 'id_localizacao', NULL, 'PK', NULL, NULL),
(22, 5, 1, 'id_rodovia', NULL, 'FK', NULL, NULL),
(23, 5, 5, 'km', NULL, 'Atributo', 6, 1),
(24, 5, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.locais_acidente (6)
(25, 6, 1, 'id_local', NULL, 'PK', NULL, NULL),
(26, 6, 1, 'id_municipio', NULL, 'FK', NULL, NULL),
(27, 6, 1, 'id_localizacao', NULL, 'FK', NULL, NULL),
(28, 6, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.tipos_classificacao (7)
(29, 7, 2, 'tipo_classificacao', NULL, 'PK', 30, NULL),
(30, 7, 2, 'descricao', NULL, 'Atributo', 60, NULL),
(31, 7, 6, 'obrigatorio', NULL, 'Atributo', NULL, NULL),
(32, 7, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.classificacoes_validas (8)
(33, 8, 2, 'tipo_classificacao', NULL, 'PK', 30, NULL),
(34, 8, 2, 'valor', NULL, 'PK', 60, NULL),
(35, 8, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.categorias_vitima (9)
(36, 9, 2, 'categoria', NULL, 'PK', 20, NULL),
(37, 9, 2, 'descricao', NULL, 'Atributo', 30, NULL),
(38, 9, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.ocorrencias (10)
(39, 10, 1, 'id_ocorrencia', NULL, 'PK', NULL, NULL),
(40, 10, 2, 'sistema_origem', NULL, 'Atributo', 20, NULL),
(41, 10, 2, 'id_ocorrencia_origem', NULL, 'Atributo', 50, NULL),
(42, 10, 1, 'id_tempo', NULL, 'FK', NULL, NULL),
(43, 10, 4, 'horario', NULL, 'Atributo', NULL, NULL),
(44, 10, 1, 'id_local', NULL, 'FK', NULL, NULL),
(45, 10, 1, 'veiculos', NULL, 'Atributo', NULL, NULL),
(46, 10, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- corporativo.ocorrencia_vitima (11)
(47, 11, 1, 'id_ocorrencia', NULL, 'PK', NULL, NULL),
(48, 11, 2, 'categoria', NULL, 'PK', 20, NULL),
(49, 11, 1, 'quantidade', NULL, 'Atributo', NULL, NULL),
-- corporativo.ocorrencia_classificacao (12)
(50, 12, 1, 'id_ocorrencia', NULL, 'PK', NULL, NULL),
(51, 12, 2, 'tipo_classificacao', NULL, 'PK', 30, NULL),
(52, 12, 2, 'valor', NULL, 'FK', 100, NULL),
-- dim_tempo (15)
(68, 15, 1, 'id_dim_tempo', NULL, 'PK', NULL, NULL),
(69, 15, 3, 'data', NULL, 'Atributo', NULL, NULL),
(70, 15, 1, 'dia', NULL, 'Atributo', NULL, NULL),
(71, 15, 1, 'mes', NULL, 'Atributo', NULL, NULL),
(72, 15, 2, 'nome_mes', NULL, 'Atributo', 20, NULL),
(73, 15, 1, 'trimestre', NULL, 'Atributo', NULL, NULL),
(74, 15, 1, 'ano', NULL, 'Atributo', NULL, NULL),
(75, 15, 2, 'dia_semana', NULL, 'Atributo', 20, NULL),
(76, 15, 6, 'flag_fim_semana', NULL, 'Atributo', NULL, NULL),
-- dim_local (16)
(77, 16, 1, 'id_dim_local', NULL, 'PK', NULL, NULL),
(78, 16, 2, 'sistema_origem', NULL, 'Atributo', 20, NULL),
(79, 16, 2, 'id_local_original', NULL, 'Atributo', 50, NULL),
(80, 16, 2, 'nome_municipio', NULL, 'Atributo', 60, NULL),
(81, 16, 2, 'sigla_uf', NULL, 'Atributo', 2, NULL),
(82, 16, 2, 'nome_uf', NULL, 'Atributo', 30, NULL),
(83, 16, 1, 'numero_rodovia', NULL, 'Atributo', NULL, NULL),
(84, 16, 5, 'km', NULL, 'Atributo', 6, 1),
(85, 16, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- dim_classificacao_acidente (17)
(86, 17, 1, 'id_dim_classificacao', NULL, 'PK', NULL, NULL),
(87, 17, 2, 'causa_acidente', NULL, 'Atributo', 60, NULL),
(88, 17, 2, 'tipo_acidente', NULL, 'Atributo', 60, NULL),
(89, 17, 2, 'classificacao_acidente', NULL, 'Atributo', 60, NULL),
(90, 17, 2, 'fase_dia', NULL, 'Atributo', 60, NULL),
(91, 17, 2, 'sentido_via', NULL, 'Atributo', 60, NULL),
(92, 17, 2, 'condicao_metereologica', NULL, 'Atributo', 60, NULL),
(93, 17, 2, 'tipo_pista', NULL, 'Atributo', 60, NULL),
(95, 17, 2, 'uso_solo', NULL, 'Atributo', 60, NULL),
(96, 17, 4, 'data_carga', NULL, 'Atributo', NULL, NULL),
-- fato_acidentes (18)
(97, 18, 1, 'id_fato_acidente', NULL, 'PK', NULL, NULL),
(98, 18, 2, 'sistema_origem', NULL, 'Atributo', 20, NULL),
(99, 18, 2, 'id_ocorrencia_original', NULL, 'Atributo', 50, NULL),
(100, 18, 1, 'id_dim_tempo', NULL, 'FK', NULL, NULL),
(101, 18, 1, 'id_dim_local', NULL, 'FK', NULL, NULL),
(102, 18, 1, 'id_dim_classificacao', NULL, 'FK', NULL, NULL),
(103, 18, 1, 'veiculos', NULL, 'Atributo', NULL, NULL),
(104, 18, 1, 'mortos', NULL, 'Atributo', NULL, NULL),
(105, 18, 1, 'feridos_leves', NULL, 'Atributo', NULL, NULL),
(106, 18, 1, 'feridos_graves', NULL, 'Atributo', NULL, NULL),
(107, 18, 1, 'ilesos', NULL, 'Atributo', NULL, NULL),
(108, 18, 1, 'ignorados', NULL, 'Atributo', NULL, NULL),
(109, 18, 1, 'feridos', 'Pré-calculado: feridos_leves + feridos_graves', 'Atributo', NULL, NULL),
(110, 18, 1, 'pessoas', 'Pré-calculado: soma das 5 categorias de vítima', 'Atributo', NULL, NULL),
(111, 18, 4, 'data_carga', NULL, 'Atributo', NULL, NULL);

INSERT INTO metadados.dado_externo (id_dado_externo, nome_dado_externo, descricao, periodicidade) VALUES
(1, 'Calendário Gregoriano', 'Referência de datas usada para popular corporativo.tempos e dim_tempo via generate_series, sem vínculo com o sistema transacional', 'Estática'),
(2, 'Regras de Negócio do ETL', 'Âncora para campos do DW cujo valor é atribuído diretamente pelo processo de ETL, sem correspondência em nenhuma coluna transacional (ex: sistema_origem fixo, sentinela ''Não informado'', totais recalculados)', 'Estática');

INSERT INTO metadados.dado_externo_conteudo (id_conteudo, id_dado_externo, conteudo, data, complemento) VALUES
(1, 1, '2000-01-01 a 2035-12-31', NULL, 'Intervalo coberto por corporativo.tempos e dim_tempo (admite anos de datatran anteriores e futuros ao 2007 hoje disponível)'),
(2, 2, 'Constantes e regras do ETL', NULL, 'sistema_origem fixo (''PRF-DATATRAN''), sentinela ''Não informado'' na dim_classificacao_acidente, e os totais feridos/pessoas recalculados no fato');

INSERT INTO metadados.algoritmo_etl (id_algoritmo, nome_algoritmo, descricao, referencia_codigo) VALUES
(1, 'Geração de calendário via generate_series', 'Gera uma linha por dia no intervalo definido, extraindo dia/mês/ano/trimestre/dia da semana', 'dw/dw_postgres.sql, data_marting/dw_postgres.sql'),
(2, 'Cópia de catálogo do OLTP', 'Upsert simples (existe ou não pela chave natural) copiando um catálogo de apoio do datatran para o corporativo, sem alteração de valores', 'airflow/dags/common_etl.py:get_or_create_*'),
(3, 'Marcação de sistema de origem', 'Grava a constante ''PRF-DATATRAN'' como sistema_origem, preparando o schema para múltiplos anos/fontes futuras', 'airflow/dags/common_etl.py'),
(4, 'Pivot de contagem de vítimas por categoria', 'Converte as linhas EAV de ocorrencia_vitima/acidente_vitima em colunas aditivas (mortos, feridos_leves, feridos_graves, ilesos, ignorados) no fato', 'airflow/dags/common_etl.py:carregar_marting_fato_acidentes'),
(5, 'Cálculo de totais pré-agregados', 'pessoas = soma das 5 categorias; feridos = feridos_leves + feridos_graves, calculados no momento da carga do fato (ao contrário do OLTP/corporativo, que nunca armazenam o total)', 'airflow/dags/common_etl.py:carregar_marting_fato_acidentes'),
(6, 'Resolução de chave substituta via lookup', 'Busca a chave substituta (surrogate key) na tabela/dimensão de destino a partir da chave natural vinda da origem (ex: data -> id_tempo, (município,localização) -> id_local)', 'airflow/dags/common_etl.py'),
(7, 'Get-or-create de combinação (dimensão junk)', 'Busca a linha de dim_classificacao_acidente cuja combinação das 9 flags bate com a da ocorrência; insere uma linha nova só se a combinação ainda não existir. Substitui NULL por ''Não informado'' antes de comparar/inserir para a UNIQUE funcionar', 'airflow/dags/common_etl.py:get_or_create_classificacao'),
(8, 'Watermark por maior id de origem', 'Corte da carga incremental: MAX(id_ocorrencia_origem) já presente em corporativo.ocorrencias por sistema_origem, evitando reler o datatran inteiro a cada execução', 'airflow/dags/carga_incremental_dw.py'),
(9, 'Desnormalização de geografia', 'Junta município + UF + localização + rodovia (4 tabelas do CORPORATIVO, já integradas - não do transacional) em uma única linha de dim_local, evitando JOINs em tempo de consulta no BI', 'airflow/dags/common_etl.py:refresh_dim_local');

-- Colunas: (id_campo_transacional, id_campo_dw_origem, id_campo_dw_destino, id_dado_externo_conteudo, id_algoritmo)
--
-- Regra seguida à risca: uma linha documenta o campo IMEDIATAMENTE anterior
-- no pipeline, nunca "pula" uma camada. Os campos de dim_local/
-- dim_classificacao_acidente/fato_acidentes (data_marting) por isso apontam
-- para id_campo_dw_origem (um campo do CORPORATIVO), nunca direto para
-- id_campo_transacional - o ETL real (airflow/dags/common_etl.py) só lê
-- essas três tabelas a partir de `corporativo.*`, nunca do `datatran`. Só as
-- tabelas do corporativo (Geografia/Classificação/Vítimas/Ocorrências) têm
-- de fato `id_campo_transacional` preenchido, porque são elas que leem o
-- datatran diretamente.
INSERT INTO metadados.integracao_transacional_dw (id_campo_transacional, id_campo_dw_origem, id_campo_dw_destino, id_dado_externo_conteudo, id_algoritmo) VALUES
-- corporativo: Geografia e Via (fonte = datatran)
(1, NULL, 11, NULL, NULL),
(2, NULL, 12, NULL, NULL),
(8, NULL, 15, NULL, NULL),
(9, NULL, 16, NULL, 6),
(3, NULL, 19, NULL, NULL),
(5, NULL, 22, NULL, 6),
(6, NULL, 23, NULL, NULL),
(11, NULL, 26, NULL, 6),
(12, NULL, 27, NULL, 6),
-- corporativo: Classificação do Acidente (fonte = datatran)
(13, NULL, 29, NULL, 2),
(14, NULL, 30, NULL, 2),
(15, NULL, 31, NULL, 2),
(16, NULL, 33, NULL, 2),
(17, NULL, 34, NULL, 2),
-- corporativo: Vítimas (fonte = datatran)
(21, NULL, 36, NULL, 2),
(22, NULL, 37, NULL, 2),
-- corporativo: Ocorrências (fonte = datatran)
(NULL, NULL, 40, 2, 3),
(23, NULL, 41, NULL, NULL),
(24, NULL, 42, NULL, 6),
(25, NULL, 43, NULL, NULL),
(26, NULL, 44, NULL, 6),
(27, NULL, 45, NULL, NULL),
(28, NULL, 47, NULL, 6),
(29, NULL, 48, NULL, NULL),
(30, NULL, 49, NULL, NULL),
(31, NULL, 50, NULL, 6),
(32, NULL, 51, NULL, NULL),
(33, NULL, 52, NULL, NULL),
-- dim_tempo (calendário, sem origem transacional NEM corporativa - gerado
-- de forma independente e idêntica nas duas camadas, ver algoritmo 1)
(NULL, NULL, 69, 1, 1),
(NULL, NULL, 70, 1, 1),
(NULL, NULL, 71, 1, 1),
(NULL, NULL, 72, 1, 1),
(NULL, NULL, 73, 1, 1),
(NULL, NULL, 74, 1, 1),
(NULL, NULL, 75, 1, 1),
(NULL, NULL, 76, 1, 1),
-- dim_local (fonte = CORPORATIVO, nunca o datatran - ver refresh_dim_local)
(NULL, NULL, 78, 2, 3),                 -- sistema_origem: constante do ETL só no mart (locais_acidente não guarda sistema_origem)
(NULL, 25, 79, NULL, NULL),             -- corporativo.locais_acidente.id_local -> id_local_original
(NULL, 15, 80, NULL, NULL),             -- corporativo.municipios.nome -> nome_municipio
(NULL, 11, 81, NULL, NULL),             -- corporativo.ufs.sigla -> sigla_uf
(NULL, 12, 82, NULL, NULL),             -- corporativo.ufs.nome -> nome_uf
(NULL, 19, 83, NULL, NULL),             -- corporativo.rodovias.numero -> numero_rodovia
(NULL, 23, 84, NULL, 9),                -- corporativo.localizacoes.km -> km (algoritmo 9: junta as 4 tabelas do corporativo)
-- dim_classificacao_acidente (fonte = CORPORATIVO, pivot das 9 classificações)
(NULL, 52, 87, NULL, 7),
(NULL, 52, 88, NULL, 7),
(NULL, 52, 89, NULL, 7),
(NULL, 52, 90, NULL, 7),
(NULL, 52, 91, NULL, 7),
(NULL, 52, 92, NULL, 7),
(NULL, 52, 93, NULL, 7),
(NULL, 52, 95, NULL, 7),
-- fato_acidentes (fonte = CORPORATIVO, nunca o datatran - ver
-- carregar_marting_fato_acidentes/carga_incremental_dw.py)
(NULL, NULL, 98, 2, 3),                 -- sistema_origem: constante 'PRF-DATATRAN' gravada direto no INSERT do fato, não lida do corporativo
(NULL, 41, 99, NULL, NULL),             -- corporativo.ocorrencias.id_ocorrencia_origem -> id_ocorrencia_original
(NULL, 42, 100, NULL, 6),               -- corporativo.ocorrencias.id_tempo -> id_dim_tempo (lookup corporativo.tempos <-> dim_tempo)
(NULL, 44, 101, NULL, 6),               -- corporativo.ocorrencias.id_local -> id_dim_local (lookup dim_local)
(NULL, 52, 102, NULL, 7),               -- corporativo.ocorrencia_classificacao.valor -> id_dim_classificacao (get-or-create da combinação)
(NULL, 45, 103, NULL, NULL),            -- corporativo.ocorrencias.veiculos -> veiculos
(NULL, 49, 104, NULL, 4),               -- corporativo.ocorrencia_vitima.quantidade -> mortos (pivot)
(NULL, 49, 105, NULL, 4),               -- corporativo.ocorrencia_vitima.quantidade -> feridos_leves (pivot)
(NULL, 49, 106, NULL, 4),               -- corporativo.ocorrencia_vitima.quantidade -> feridos_graves (pivot)
(NULL, 49, 107, NULL, 4),               -- corporativo.ocorrencia_vitima.quantidade -> ilesos (pivot)
(NULL, 49, 108, NULL, 4),               -- corporativo.ocorrencia_vitima.quantidade -> ignorados (pivot)
(NULL, NULL, 109, 2, 5),                -- feridos: calculado no momento da carga, não copiado de nenhum campo
(NULL, NULL, 110, 2, 5);                -- pessoas: idem
