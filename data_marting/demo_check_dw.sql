-- Demo: contagem do DW, pra comparar antes/depois de cada carga.
-- Rodar: docker exec -i dw_postgres psql -U postgres -d dw < data_marting/demo_check_dw.sql

SELECT
    (SELECT COUNT(*) FROM fato_acidentes) AS total_fato_acidentes,
    (SELECT SUM(mortos) FROM fato_acidentes) AS total_mortos,
    (SELECT SUM(pessoas) FROM fato_acidentes) AS total_pessoas,
    (SELECT COUNT(*) FROM dim_local) AS total_dim_local,
    (SELECT COUNT(*) FROM dim_classificacao_acidente) AS total_dim_classificacao,
    (SELECT COUNT(DISTINCT ano) FROM dim_tempo dt JOIN fato_acidentes fa ON fa.id_dim_tempo = dt.id_dim_tempo) AS anos_carregados;

-- Conferência cruzada com o corporativo (devem bater 1:1 no grão de ocorrência)
SELECT
    (SELECT COUNT(*) FROM corporativo.ocorrencias) AS total_corporativo_ocorrencias,
    (SELECT COUNT(*) FROM fato_acidentes) AS total_mart_fato_acidentes;
