\c dw

-- Área intermediária para a carga incremental (ver
-- airflow/dags/carga_incremental_dw.py): tabelas truncadas a cada execução,
-- só têm dado enquanto a DAG está rodando. O corte de "o que é novo" é feito
-- pela DAG contra `corporativo.ocorrencias` (watermark = MAIOR
-- id_ocorrencia_origem já carregado), não aqui.
--
-- Layout já achatado (join feito na origem `datatran`, não no destino) para a
-- carga poder inserir direto em `corporativo` sem repetir os JOINs de
-- geografia/via a cada tabela consumidora.

CREATE SCHEMA staging;

-- As 8 classificações de valor único vêm achatadas direto nesta tabela
-- (mesma decomposição do OLTP/corporativo - ver nota "REMODELAGEM" em
-- dw/dw_postgres.sql), não numa staging atributo-valor separada: já chegam
-- como colunas do próprio `acidentes`/`corporativo.ocorrencias`, então não há
-- pivot a fazer antes de gravar.
CREATE TABLE staging.stg_acidentes (
    id           INTEGER,
    data         DATE,
    horario      TIME,
    uf_sigla     CHAR(2),
    uf_nome      VARCHAR(30),
    municipio_nome VARCHAR(60),
    rodovia_numero SMALLINT,
    km           NUMERIC(6,1),
    veiculos     SMALLINT,

    causa_acidente          VARCHAR(100),
    tipo_acidente           VARCHAR(60),
    classificacao_acidente  VARCHAR(30),
    fase_dia                VARCHAR(20),
    sentido_via              VARCHAR(20),
    condicao_metereologica  VARCHAR(30),
    tipo_pista               VARCHAR(20),
    uso_solo                VARCHAR(20)
);

CREATE TABLE staging.stg_acidente_vitima (
    acidente_id INTEGER,
    categoria   VARCHAR(20),
    quantidade  SMALLINT
);

-- tracado_via é multivalorado desde 2017 (ver nota "DESCOBERTA" em
-- db/01_schema.sql) - staging própria, à parte das 8 classificações
-- escalares acima.
CREATE TABLE staging.stg_acidente_tracado_via (
    acidente_id INTEGER,
    valor       VARCHAR(30)
);
