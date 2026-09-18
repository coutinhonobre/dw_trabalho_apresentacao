-- ============================================================================
-- Modelo transacional (OLTP) para dataset/datatran2007.csv (PRF - acidentes
-- de trânsito em rodovias federais), normalizado até a 4a Forma Normal (4FN).
-- ============================================================================
--
-- Grão do CSV original: 1 linha = 1 ocorrência (acidente), já com contagens
-- agregadas de pessoas/vítimas/veículos. O único atributo genuinamente
-- multivalorado da linha original é a contagem de vítimas por categoria
-- (mortos/feridos_leves/feridos_graves/ilesos/ignorados), que o CSV
-- achatou em 5 colunas separadas em vez de uma lista (categoria, quantidade)
-- - por isso vira uma tabela associativa própria (`acidente_vitima`, ver
-- abaixo), e é justamente essa decomposição que caracteriza a 4FN aqui
-- (eliminar a dependência multivalorada id ->-> categoria_vitima). Os demais
-- atributos são de fato escalares e de valor único por ocorrência.
--
-- Dependências funcionais identificadas na tabela plana original e como cada
-- uma foi resolvida:
--
--   id -> data_inversa, horario, br, km, municipio, uf, causa_acidente,
--         tipo_acidente, classificacao_acidente, fase_dia, sentido_via,
--         condicao_metereologica, tipo_pista, tracado_via, uso_solo, veiculos
--     -> continuam como atributos de `acidentes`, dependem só da chave.
--
--   mortos, feridos_leves, feridos_graves, ilesos, ignorados   (grupo
--     repetitivo: 5 colunas são, na prática, o mesmo atributo - "quantidade
--     de pessoas" - variando só na categoria; viola até a 1FN, que exige
--     eliminar grupos repetitivos)
--     -> viram linhas em `acidente_vitima (acidente_id, categoria,
--        quantidade)`, uma tabela associativa clássica entre `acidentes` e
--        `categoria_vitima` (uma ocorrência tem várias categorias de vítima;
--        uma categoria aparece em várias ocorrências - N:N resolvido com
--        tabela intermediária e o atributo próprio da associação
--        `quantidade`). Só entram linhas com quantidade > 0.
--
--   pessoas = soma de mortos+feridos_leves+feridos_graves+ilesos+ignorados
--   feridos = feridos_leves + feridos_graves
--     (dependência funcional válida, mas de um valor CALCULADO a partir de
--     outros atributos da própria linha - redundância pura. Confirmado nos
--     dados: `feridos` bate com a soma em 100% das ~127 mil linhas; `pessoas`
--     diverge da soma em 18 linhas, prova de que manter o total guardado ao
--     lado das partes permite as duas ficarem inconsistentes)
--     -> removidos de `acidentes`; recalculados sob demanda com
--        SUM(quantidade) em `acidente_vitima` (ver view `acidentes_resumo`
--        no fim deste arquivo).
--
--   data_inversa -> dia_semana, ano   (DF transitiva: id -> data -> dia_semana/ano;
--     dia_semana e ano não dependem de id, e sim só da data)
--     -> extraídos para `calendario` (chave = data), removendo a redundância
--        de repetir "Segunda"/"2007" em toda ocorrência daquele dia.
--
--   (municipio, uf) -> (nenhum outro atributo do CSV, mas é uma entidade
--     geográfica própria e o nome do município se repete por ocorrência)
--     -> extraído para `municipio`, chave natural composta (nome, uf_sigla)
--        (nomes de município se repetem entre UFs diferentes - ex.: "Belém"
--        existe em mais de um estado no dataset - uf sozinha não determina
--        município nem vice-versa), exposta por uma chave substituta
--        (id serial) só para simplificar a FK em `acidentes` (ver abaixo).
--
--   municipio -> uf   (DF transitiva que a 1a versão deste schema deixou
--     passar: cada município pertence a exatamente um estado, então uf é
--     100% determinado pelo município - confirmado nos dados: toda ocorrência
--     com município preenchido também tem uf preenchida e sempre igual à uf
--     do município. Guardar `uf_sigla` como coluna própria de `acidentes`,
--     além de já vir embutida na FK composta para `municipio`, duplicava o
--     mesmo fato em dois lugares (anomalia de atualização: nada impede os
--     dois valores divergirem). Por isso `acidentes` referencia só
--     `municipio_id` - a uf de uma ocorrência é obtida via
--     `acidentes -> municipio -> uf`, nunca armazenada direto.
--
--   uf -> (nome do estado, não vem no CSV mas é 1:1 com a sigla)
--     -> extraído para `uf`.
--
--   br -> (nenhum atributo descritivo no CSV além do próprio número)
--     -> extraído para `rodovia` para documentar/validar os números de BR
--        válidos e permitir estender com atributos no futuro.
--
--   (br, km) -> (nenhum atributo descritivo no CSV, mas é um ponto físico
--     real na rodovia que se repete entre ocorrências)
--     -> extraído para `localizacao` (chave natural (rodovia_numero, km),
--        exposta por id serial pelo mesmo motivo de `municipio`). Reuso
--        real confirmado nos dados: 127666 ocorrências com br/km preenchidos
--        caem em só 51359 pontos (rodovia,km) distintos (~2,5 ocorrências
--        por ponto em média) - sem essa tabela, cada ocorrência repetia o
--        par (rodovia, km) por inteiro.
--
--     Por que `municipio_id` NÃO virou coluna de `localizacao` nem
--     determina `localizacao_id` (isso continua uma FK independente, ver
--     `local_acidente` abaixo): testei essa hipótese e os dados não
--     sustentam - 8302 dos 51359 pontos (rodovia,km) aparecem associados a
--     MAIS DE UM município em ocorrências diferentes (16%). Isso é ruído de
--     medição (km só tem uma casa decimal, ~100m de precisão - perto de
--     divisa municipal o mesmo km marcado fica ora num município, ora no
--     vizinho), não uma dependência funcional de verdade. Forçar
--     (rodovia,km) -> município inventaria uma regra que 1 em cada 6 pontos
--     contradiz.
--
--   (municipio_id, localizacao_id) -> (o par "onde" de uma ocorrência,
--     município + ponto na rodovia, junto)
--     -> a pedido explícito, consolidei os dois numa única tabela
--        `local_acidente (id, municipio_id, localizacao_id)`, e `acidentes`
--        passa a ter 1 FK só (`local_id`) em vez de duas. Diferente da
--        tentativa (rodovia,km) -> município acima, aqui NÃO estou
--        afirmando uma dependência funcional entre os dois - só agrupando o
--        PAR já reportado por ocorrência numa entidade própria, com reuso
--        real: 127666 ocorrências com ambos preenchidos caem em 64600
--        pares distintos (~2x, 19135 pares reaproveitados por mais de uma
--        ocorrência).
--
--   causa_acidente, tipo_acidente, classificacao_acidente, fase_dia,
--   sentido_via, condicao_metereologica, tipo_pista, tracado_via, uso_solo
--     (9 classificações, 1 valor só por ocorrência - igual a
--     `vendas.vendedor_id`/`vendas.forma_pagamento_id` num ERP: FK direta no
--     cabeçalho, não é grupo repetitivo)
--     -> modeladas como atributo-valor: `acidentes` referencia cada uma via
--        linhas em `acidente_atributo (acidente_id, tipo_atributo, valor)`
--        em vez de 9 colunas/9 tabelas separadas. `tipo_atributo` documenta
--        os 9 tipos existentes; `atributo_valor_valido (tipo_atributo,
--        valor)` substitui as 9 tabelas de apoio antigas como catálogo
--        único, e `acidente_atributo` tem FK composta
--        (tipo_atributo, valor) -> atributo_valor_valido, preservando a
--        integridade referencial por tipo (não dá pra gravar "Chuva" como
--        valor de `tipo_pista`, por exemplo - a combinação tem que existir
--        no catálogo).
--
--     Trade-off aceito conscientemente: o EAV troca 9 colunas tipadas (com
--     NOT NULL declarativo em 6 delas) por uma estrutura genérica onde
--     "obrigatório" vira só um metadado informativo em `tipo_atributo`
--     (coluna `obrigatorio`), sem constraint do banco garantindo que todo
--     acidente tem, por exemplo, `causa_acidente` preenchida - isso é a
--     fraqueza clássica do padrão atributo-valor (perde tipagem/constraint
--     por atributo em troca de um cabeçalho enxuto e extensível sem alterar
--     schema). `acidentes` fica só com os atributos que continuam 100%
--     estruturais: id, data, horario, local, veículos.
--
-- Algumas ocorrências do CSV trazem "(null)" literal para uf/br/km,
-- classificacao_acidente, fase_dia e condicao_metereologica - tratadas como
-- NULL de fato. Há 5 ocorrências (de 127675) com município preenchido mas uf
-- ausente na fonte; como `municipio` exige uf (não pode ter uf indefinida
-- dentro da sua própria chave natural), essas 5 ocorrências ficam com
-- `municipio_id` NULL em vez de inventar um estado.
-- ============================================================================

CREATE TABLE uf (
    sigla CHAR(2) PRIMARY KEY,
    nome  VARCHAR(30) NOT NULL
);

CREATE TABLE rodovia (
    numero SMALLINT PRIMARY KEY
);

CREATE TABLE localizacao (
    id             SERIAL PRIMARY KEY,
    rodovia_numero SMALLINT NOT NULL REFERENCES rodovia (numero),
    km             NUMERIC(6,1) NOT NULL,
    UNIQUE (rodovia_numero, km)
);

CREATE TABLE municipio (
    id       SERIAL PRIMARY KEY,
    nome     VARCHAR(60) NOT NULL,
    uf_sigla CHAR(2) NOT NULL REFERENCES uf (sigla),
    UNIQUE (nome, uf_sigla)
);

-- Consolida o "onde" de uma ocorrência (município + ponto na rodovia) numa
-- única entidade, referenciada por `acidentes.local_id` (1 FK em vez de 2).
-- Só existe linha para pares onde os dois lados são conhecidos - as 5
-- ocorrências sem uf/br/km na fonte ficam com `acidentes.local_id` NULL.
CREATE TABLE local_acidente (
    id             SERIAL PRIMARY KEY,
    municipio_id   INTEGER NOT NULL REFERENCES municipio (id),
    localizacao_id INTEGER NOT NULL REFERENCES localizacao (id),
    UNIQUE (municipio_id, localizacao_id)
);

-- Meta-catálogo: os 9 tipos de classificação que uma ocorrência tem.
-- `obrigatorio` documenta quais eram NOT NULL na versão anterior (colunas
-- tipadas) - aqui é só metadado, não é imposto pelo banco (ver trade-off
-- no cabeçalho deste arquivo).
CREATE TABLE tipo_atributo (
    tipo_atributo VARCHAR(30) PRIMARY KEY,
    descricao     VARCHAR(60) NOT NULL,
    obrigatorio   BOOLEAN NOT NULL DEFAULT TRUE
);

-- Catálogo único dos valores válidos por tipo (substitui as antigas
-- causa_acidente/tipo_acidente/classificacao_acidente/fase_dia/sentido_via/
-- condicao_metereologica/tipo_pista/tracado_via/uso_solo).
CREATE TABLE atributo_valor_valido (
    tipo_atributo VARCHAR(30) NOT NULL REFERENCES tipo_atributo (tipo_atributo),
    valor         VARCHAR(60) NOT NULL,
    PRIMARY KEY (tipo_atributo, valor)
);

CREATE TABLE calendario (
    data        DATE PRIMARY KEY,
    dia_semana  VARCHAR(15) NOT NULL,
    ano         SMALLINT NOT NULL
);

CREATE TABLE categoria_vitima (
    categoria VARCHAR(20) PRIMARY KEY,
    descricao VARCHAR(30) NOT NULL
);

-- Cabeçalho enxuto: só o que é estrutural (chave, quando, onde, quantos
-- veículos). As 9 classificações saíram daqui e viraram linhas em
-- `acidente_atributo` (ver abaixo) - mesmo papel que `itens_venda` tem para
-- `vendas` num ERP.
CREATE TABLE acidentes (
    id                      INTEGER PRIMARY KEY,
    data                    DATE NOT NULL REFERENCES calendario (data),
    horario                 TIME NOT NULL,

    local_id                INTEGER REFERENCES local_acidente (id),

    veiculos                SMALLINT NOT NULL
);

-- Tabela associativa: uma ocorrência tem várias categorias de vítima, uma
-- categoria aparece em várias ocorrências (N:N), com atributo próprio da
-- associação (`quantidade`). Só existe linha para categoria com quantidade > 0
-- (ex.: um acidente "Sem Vítimas" não gera nenhuma linha aqui).
CREATE TABLE acidente_vitima (
    acidente_id INTEGER NOT NULL REFERENCES acidentes (id),
    categoria   VARCHAR(20) NOT NULL REFERENCES categoria_vitima (categoria),
    quantidade  SMALLINT NOT NULL CHECK (quantidade > 0),
    PRIMARY KEY (acidente_id, categoria)
);

-- O "itens_venda" das classificações: uma linha por (acidente, tipo de
-- classificação). FK composta garante que o par (tipo_atributo, valor)
-- existe no catálogo - não dá pra gravar um valor fora do domínio daquele
-- tipo.
CREATE TABLE acidente_atributo (
    acidente_id   INTEGER NOT NULL REFERENCES acidentes (id),
    tipo_atributo VARCHAR(30) NOT NULL,
    valor         VARCHAR(60) NOT NULL,
    PRIMARY KEY (acidente_id, tipo_atributo),
    FOREIGN KEY (tipo_atributo, valor) REFERENCES atributo_valor_valido (tipo_atributo, valor)
);

CREATE INDEX idx_acidentes_data ON acidentes (data);
CREATE INDEX idx_acidentes_local ON acidentes (local_id);
CREATE INDEX idx_local_acidente_municipio ON local_acidente (municipio_id);
CREATE INDEX idx_local_acidente_localizacao ON local_acidente (localizacao_id);
CREATE INDEX idx_municipio_uf ON municipio (uf_sigla);
CREATE INDEX idx_localizacao_rodovia ON localizacao (rodovia_numero);
CREATE INDEX idx_acidente_vitima_categoria ON acidente_vitima (categoria);
CREATE INDEX idx_acidente_atributo_tipo_valor ON acidente_atributo (tipo_atributo, valor);

-- Consulta a uf/município/rodovia/km de uma ocorrência (não são mais
-- colunas próprias de `acidentes`):
--   SELECT a.id, m.uf_sigla, m.nome AS municipio, r.numero AS br, l.km
--   FROM acidentes a
--   JOIN local_acidente la ON la.id = a.local_id
--   JOIN municipio m ON m.id = la.municipio_id
--   JOIN localizacao l ON l.id = la.localizacao_id
--   JOIN rodovia r ON r.numero = l.rodovia_numero;

-- `pessoas` e `feridos` do CSV original não são colunas: são somas
-- calculadas sob demanda a partir de `acidente_vitima`, nunca armazenadas.
CREATE VIEW acidentes_resumo AS
SELECT
    a.*,
    COALESCE(SUM(v.quantidade), 0) AS pessoas,
    COALESCE(SUM(v.quantidade) FILTER (
        WHERE v.categoria IN ('feridos_leves', 'feridos_graves')
    ), 0) AS feridos
FROM acidentes a
LEFT JOIN acidente_vitima v ON v.acidente_id = a.id
GROUP BY a.id;

-- View de conveniência: "despivota" acidente_atributo de volta em colunas,
-- pra quem quer consultar sem lidar com o EAV na mão toda vez.
CREATE VIEW acidentes_classificados AS
SELECT
    a.*,
    MAX(CASE WHEN t.tipo_atributo = 'causa_acidente' THEN t.valor END) AS causa_acidente,
    MAX(CASE WHEN t.tipo_atributo = 'tipo_acidente' THEN t.valor END) AS tipo_acidente,
    MAX(CASE WHEN t.tipo_atributo = 'classificacao_acidente' THEN t.valor END) AS classificacao_acidente,
    MAX(CASE WHEN t.tipo_atributo = 'fase_dia' THEN t.valor END) AS fase_dia,
    MAX(CASE WHEN t.tipo_atributo = 'sentido_via' THEN t.valor END) AS sentido_via,
    MAX(CASE WHEN t.tipo_atributo = 'condicao_metereologica' THEN t.valor END) AS condicao_metereologica,
    MAX(CASE WHEN t.tipo_atributo = 'tipo_pista' THEN t.valor END) AS tipo_pista,
    MAX(CASE WHEN t.tipo_atributo = 'tracado_via' THEN t.valor END) AS tracado_via,
    MAX(CASE WHEN t.tipo_atributo = 'uso_solo' THEN t.valor END) AS uso_solo
FROM acidentes a
LEFT JOIN acidente_atributo t ON t.acidente_id = a.id
GROUP BY a.id;

-- ============================================================================
-- EXTENSÃO CONCEITUAL: granularidade do sistema real por trás deste CSV
-- ============================================================================
--
-- `datatran2007.csv` já vem agregado por ocorrência (contagens de
-- pessoas/veículos), mas o nome "datatran" é historicamente usado pela PRF
-- para a exportação por PESSOA e por VEÍCULO envolvido - ou seja, o sistema
-- operacional real que registra um acidente de trânsito não para na
-- ocorrência: ele sabe, individualmente, quem estava em qual veículo e em
-- que condição ficou. `acidente_vitima` é a melhor decomposição possível
-- DESTE CSV (que só tem a contagem agregada); `veiculo_envolvido` e
-- `pessoa_envolvida` abaixo são a granularidade real que o sistema teria.
--
-- IMPORTANTE: estas duas tabelas ficam vazias em `02_data.sql`. Não crio
-- linha de veículo ou de pessoa aqui porque isso seria inventar dado que a
-- fonte não tem (este CSV não traz placa, tipo de veículo, idade, sexo etc.
-- por indivíduo) - só a contagem agregada, já 100% representada em
-- `acidente_vitima`/`acidentes.veiculos`. Elas ficam no schema para
-- documentar a forma real do sistema e para já existir o lugar certo de
-- carregar caso uma fonte por pessoa/veículo apareça (nesse caso,
-- `acidente_vitima.quantidade` passaria a ser um COUNT(*) derivado de
-- `pessoa_envolvida`, em vez de valor armazenado).
CREATE TABLE veiculo_envolvido (
    id              SERIAL PRIMARY KEY,
    acidente_id     INTEGER NOT NULL REFERENCES acidentes (id),
    tipo_veiculo    VARCHAR(30),
    placa           VARCHAR(10),
    marca_modelo    VARCHAR(60),
    ano_fabricacao  SMALLINT
);

CREATE TABLE pessoa_envolvida (
    id                   SERIAL PRIMARY KEY,
    acidente_id          INTEGER NOT NULL REFERENCES acidentes (id),
    veiculo_envolvido_id INTEGER REFERENCES veiculo_envolvido (id), -- NULL = pedestre
    categoria            VARCHAR(20) NOT NULL REFERENCES categoria_vitima (categoria),
    tipo_envolvimento    VARCHAR(20), -- Condutor / Passageiro / Pedestre
    idade                SMALLINT,
    sexo                 CHAR(1)
);

CREATE INDEX idx_veiculo_envolvido_acidente ON veiculo_envolvido (acidente_id);
CREATE INDEX idx_pessoa_envolvida_acidente ON pessoa_envolvida (acidente_id);
CREATE INDEX idx_pessoa_envolvida_veiculo ON pessoa_envolvida (veiculo_envolvido_id);
