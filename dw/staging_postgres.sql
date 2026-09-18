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

CREATE TABLE staging.stg_acidentes (
    id           INTEGER,
    data         DATE,
    horario      TIME,
    uf_sigla     CHAR(2),
    uf_nome      VARCHAR(30),
    municipio_nome VARCHAR(60),
    rodovia_numero SMALLINT,
    km           NUMERIC(6,1),
    veiculos     SMALLINT
);

CREATE TABLE staging.stg_acidente_vitima (
    acidente_id INTEGER,
    categoria   VARCHAR(20),
    quantidade  SMALLINT
);

CREATE TABLE staging.stg_acidente_atributo (
    acidente_id   INTEGER,
    tipo_atributo VARCHAR(30),
    valor         VARCHAR(60)
);
