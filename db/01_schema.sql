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
--     -> removidos de `acidentes`; quem precisa do total consulta
--        SUM(quantidade) em `acidente_vitima` na hora (ex.:
--        `SELECT acidente_id, SUM(quantidade) FROM acidente_vitima GROUP BY
--        acidente_id`) - não fica guardado em lugar nenhum, nem em view.
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
--   sentido_via, condicao_metereologica, tipo_pista, uso_solo
--     (8 classificações, 1 valor só por ocorrência - igual a
--     `vendas.vendedor_id`/`vendas.forma_pagamento_id` num ERP: FK direta no
--     cabeçalho, não é grupo repetitivo)
--     -> UMA VERSÃO ANTERIOR deste schema modelou isso como atributo-valor
--        genérico (`acidente_atributo (acidente_id, tipo_atributo, valor)` +
--        catálogo único `atributo_valor_valido`) - um EAV. Revertido: EAV é
--        um antipadrão conhecido (Karwin, "SQL Antipatterns", cap. 6)
--        especificamente quando o conjunto de atributos é FIXO e conhecido
--        de antemão (não é extensível em runtime por usuário) - exatamente
--        o caso aqui, sempre as mesmas 8 classificações. O preço do EAV: tudo
--        vira VARCHAR (perde tipagem por atributo), `NOT NULL` por atributo
--        fica impossível de declarar no banco (vira só metadado informativo,
--        nunca imposto), e até `SELECT causa_acidente` deixa de ser uma
--        coluna e passa a exigir join+pivot.
--     -> MODELO ATUAL: 8 colunas próprias em `acidentes`, cada uma FK pra um
--        catálogo pequeno e dedicado (`causa_acidente_valido`,
--        `tipo_acidente_valido`, ..., `uso_solo_valido`) - mesma integridade
--        referencial por tipo que o EAV tinha (não dá pra gravar "Chuva" em
--        `tipo_pista`, a FK aponta pro catálogo certo), só que com tipagem e
--        `NOT NULL` de verdade onde os dados sustentam. Verificado empírico
--        nas 2.237.189 linhas dos 20 CSVs (não só a suposição "obrigatorio"
--        do CSV original, que o EAV levava como metadado sem checar): só
--        `sentido_via` nunca é nulo (0 ocorrências); as outras 7 têm de 2
--        (`causa_acidente`) a 70 (`fase_dia`) linhas nulas - por isso só
--        `sentido_via` é `NOT NULL`, as demais 7 ficam `NULL`-áveis.
--        Continua 4FN: cada FK é uma dependência funcional direta e única de
--        `acidentes.id`, sem grupo repetitivo nem dependência multivalorada -
--        a mesma garantia que o EAV já tinha, só que agora com o banco
--        aplicando a tipagem em vez de confiar na aplicação.
--
--   tracado_via
--     (a 9a classificação do CSV original, tratada à parte das outras 8 -
--     ver abaixo)
--     -> DESCOBERTA ao carregar os anos 2017-2026 (o schema original, de
--        2025, só tinha visto 2007): a PRF passou a permitir MAIS DE UM
--        traçado por ocorrência nesse período, concatenados com ';' num
--        único campo do CSV (ex.: "Reta;Curva;Viaduto"). 100.497 das
--        2.237.154 ocorrências (4,5%, crescendo de ~5.500/ano em 2017 para
--        16-18 mil/ano em 2023-2025) têm esse formato. Guardar a string
--        concatenada como um `valor` comum de `acidente_atributo` - como
--        uma primeira versão deste schema chegou a fazer, só alargando a
--        coluna para caber - reintroduz exatamente o problema que
--        `acidente_vitima` (abaixo) já resolve para as 5 contagens de
--        vítima: um grupo repetitivo disfarçado de escalar, violando a 1FN.
--     -> tratada como a segunda tabela associativa genuína do schema,
--        mesma forma de `acidente_vitima`: `acidente_tracado_via
--        (acidente_id, valor)`, FK para um catálogo próprio
--        `tracado_via_valido (valor)` (13 valores distintos observados,
--        máximo 22 caracteres - catálogo dedicado, mesmo padrão dos outros
--        8 (`causa_acidente_valido` etc.), só que referenciado por uma
--        tabela associativa em vez de uma coluna, porque é multivalorado.
--        Uma ocorrência sem traçado múltiplo mantém 1 linha aqui; uma com
--        "Reta;Curva;Viaduto" vira 3. Quem quer o formato ';'-separado
--        original consulta `string_agg(valor, ';')` em
--        `acidente_tracado_via` direto, na hora - não existe view pra isso.
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

-- 8 catálogos dedicados (um por classificação de valor único) - substituem
-- o antigo par genérico tipo_atributo/atributo_valor_valido (EAV, ver nota
-- no cabeçalho). Larguras calculadas do dado real (20 CSVs, 2.237.189 linhas).
CREATE TABLE causa_acidente_valido (
    valor VARCHAR(100) PRIMARY KEY  -- maior observado: 78 (rótulos 2017+)
);

CREATE TABLE tipo_acidente_valido (
    valor VARCHAR(60) PRIMARY KEY  -- maior observado: 42
);

CREATE TABLE classificacao_acidente_valido (
    valor VARCHAR(30) PRIMARY KEY  -- maior observado: 19
);

CREATE TABLE fase_dia_valido (
    valor VARCHAR(20) PRIMARY KEY  -- maior observado: 11
);

CREATE TABLE sentido_via_valido (
    valor VARCHAR(20) PRIMARY KEY  -- maior observado: 13
);

CREATE TABLE condicao_metereologica_valido (
    valor VARCHAR(30) PRIMARY KEY  -- maior observado: 16
);

CREATE TABLE tipo_pista_valido (
    valor VARCHAR(20) PRIMARY KEY  -- maior observado: 8
);

CREATE TABLE uso_solo_valido (
    valor VARCHAR(20) PRIMARY KEY  -- maior observado: 6
);

-- Catálogo da 9a classificação (tracado_via) - multivalorada, à parte das 8
-- acima - ver nota "DESCOBERTA" no cabeçalho deste arquivo. 13 valores
-- distintos, máximo 22 caracteres.
CREATE TABLE tracado_via_valido (
    valor VARCHAR(30) PRIMARY KEY
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

-- Cabeçalho + as 8 classificações de valor único, cada uma sua própria FK
-- (ver nota no cabeçalho deste arquivo - por que colunas tipadas em vez de
-- EAV). `NOT NULL` só em `sentido_via`: é a única das 8 com 0 linhas nulas
-- nas 2.237.189 ocorrências verificadas; as outras 7 têm nulos reais no
-- dado (de 2 a 70 linhas, a depender da coluna), então ficam NULL-áveis -
-- constraint que reflete o dado de verdade, não a documentação original do
-- CSV (que dizia "obrigatório" pra várias que não são, na prática).
CREATE TABLE acidentes (
    id                      INTEGER PRIMARY KEY,
    data                    DATE NOT NULL REFERENCES calendario (data),
    horario                 TIME NOT NULL,

    local_id                INTEGER REFERENCES local_acidente (id),

    veiculos                SMALLINT NOT NULL,

    causa_acidente          VARCHAR(100) REFERENCES causa_acidente_valido (valor),
    tipo_acidente           VARCHAR(60) REFERENCES tipo_acidente_valido (valor),
    classificacao_acidente  VARCHAR(30) REFERENCES classificacao_acidente_valido (valor),
    fase_dia                VARCHAR(20) REFERENCES fase_dia_valido (valor),
    sentido_via             VARCHAR(20) NOT NULL REFERENCES sentido_via_valido (valor),
    condicao_metereologica  VARCHAR(30) REFERENCES condicao_metereologica_valido (valor),
    tipo_pista              VARCHAR(20) REFERENCES tipo_pista_valido (valor),
    uso_solo                VARCHAR(20) REFERENCES uso_solo_valido (valor)
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

-- Tabela associativa (mesmo papel de `acidente_vitima`): uma ocorrência tem
-- de 0 a N traçados de via, um traçado aparece em várias ocorrências (N:N).
-- Sem atributo próprio da associação (ao contrário de acidente_vitima, que
-- tem `quantidade`) - é só presença/ausência do valor naquela ocorrência.
CREATE TABLE acidente_tracado_via (
    acidente_id INTEGER NOT NULL REFERENCES acidentes (id),
    valor       VARCHAR(30) NOT NULL REFERENCES tracado_via_valido (valor),
    PRIMARY KEY (acidente_id, valor)
);

CREATE INDEX idx_acidentes_data ON acidentes (data);
CREATE INDEX idx_acidentes_local ON acidentes (local_id);
CREATE INDEX idx_acidentes_causa ON acidentes (causa_acidente);
CREATE INDEX idx_acidentes_tipo ON acidentes (tipo_acidente);
CREATE INDEX idx_acidentes_classificacao ON acidentes (classificacao_acidente);
CREATE INDEX idx_acidentes_fase_dia ON acidentes (fase_dia);
CREATE INDEX idx_acidentes_sentido_via ON acidentes (sentido_via);
CREATE INDEX idx_acidentes_condicao_metereologica ON acidentes (condicao_metereologica);
CREATE INDEX idx_acidentes_tipo_pista ON acidentes (tipo_pista);
CREATE INDEX idx_acidentes_uso_solo ON acidentes (uso_solo);
CREATE INDEX idx_local_acidente_municipio ON local_acidente (municipio_id);
CREATE INDEX idx_local_acidente_localizacao ON local_acidente (localizacao_id);
CREATE INDEX idx_municipio_uf ON municipio (uf_sigla);
CREATE INDEX idx_localizacao_rodovia ON localizacao (rodovia_numero);
CREATE INDEX idx_acidente_vitima_categoria ON acidente_vitima (categoria);
CREATE INDEX idx_acidente_tracado_via_valor ON acidente_tracado_via (valor);
