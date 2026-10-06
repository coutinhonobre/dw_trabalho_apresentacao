#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "=================================================================="
echo "ATENÇÃO: isso vai inserir o dataset COMPLETO da PRF (2007-2026,"
echo "~2,2 milhões de ocorrências, ~1,1 GB de SQL em db/02-*.sql)."
echo
echo "Enquanto o Postgres roda os scripts de init (schema + catálogos +"
echo "os 20 arquivos por ano), ele SÓ aceita conexão local"
echo "(socket Unix) - nenhum cliente externo (DBeaver, psql do host, etc.)"
echo "consegue conectar na porta 5432 até terminar. Isso é comportamento"
echo "padrão da imagem oficial do Postgres (evita ler dado parcial no meio"
echo "da carga), não é falha - se seu cliente der erro de conexão/EOF"
echo "nesse meio tempo, é só esperar."
echo
echo "Leva de dezenas de minutos a algumas horas, dependendo da máquina."
echo "Acompanhe o progresso com:"
echo "  docker logs -f datatran_postgres"
echo "=================================================================="
echo

echo "Recriando containers do zero (schema + dados serão reprocessados)..."
docker compose down -v
docker compose up -d --force-recreate

echo
echo "Airflow em http://localhost:8080 (pode levar 1-2 min pra ficar disponível)."
echo "Usuário/senha do Airflow: airflow / airflow."
echo "Metabase em http://localhost:3000 (login padrão: metabase@metabase.com / metabase)."
echo

# ------------------------------------------------------------------
# Espera o Postgres terminar TODOS os scripts de init (01-schema,
# 02-00-catalogos, os 20 02-NN-AAAA, 03-dw-schema, 04-metadados,
# 05-staging, 06-data-marting) antes de seguir. A imagem oficial do
# Postgres imprime essa linha EXATAMENTE UMA VEZ, assim que o último
# script termina e antes de reiniciar o servidor de verdade (com TCP
# habilitado) - é o sinal mais confiável de "acabou", mais confiável que
# testar a porta 5432 (o proxy de rede do Docker Desktop aceita a conexão
# TCP mesmo com o Postgres ainda rodando só no socket Unix interno).
# ------------------------------------------------------------------
echo "Aguardando o banco transacional (schema + 20 anos) terminar de carregar..."
echo "Isso pode levar de dezenas de minutos a algumas horas, dependendo da máquina."
INICIO=$(date +%s)
while true; do
    if docker logs datatran_postgres 2>&1 | grep -q "PostgreSQL init process complete"; then
        break
    fi
    pg_status=$(docker inspect -f '{{.State.Status}}' datatran_postgres 2>/dev/null || echo "desconhecido")
    if [ "$pg_status" != "running" ]; then
        echo
        echo "ERRO: o container datatran_postgres parou (status: $pg_status) antes de terminar a carga."
        echo "Veja os logs: docker logs datatran_postgres"
        exit 1
    fi
    printf "."
    sleep 10
done
FIM=$(date +%s)
DURACAO=$((FIM - INICIO))
echo
echo "=================================================================="
echo "CARGA DO BANCO TRANSACIONAL CONCLUÍDA em $((DURACAO / 60)) min $((DURACAO % 60))s."
echo "Postgres agora aceita conexão externa em localhost:5432 (bancos datatran e dw)."
echo "=================================================================="
echo
echo "Depois do Postgres subir, popule o catálogo de metadados (não roda automaticamente):"
echo "  docker exec -i datatran_postgres psql -U postgres -d dw < dw/metadados_seed_postgres.sql"
echo
echo "A carga inicial do DW e a clusterização/regras de associação (ML) disparam"
echo "sozinhas assim que o Airflow sobe (DAG bootstrap_carga_e_ml) - acompanhe em"
echo "http://localhost:8080. Pode levar de dezenas de minutos a algumas horas com"
echo "o dataset completo (ver README, seção 'Airflow - carga inicial'). Para"
echo "disparar de novo manualmente (ex.: outro ambiente, ou só uma das duas):"
echo "  docker exec dw_airflow airflow dags trigger carga_inicial_dw"
echo "  docker exec dw_airflow airflow dags trigger ml_clusterizacao_regras_associacao"
echo
echo "O container metabase-setup provisiona o painel 'Painel PRF - Acidentes de"
echo "Trânsito' sozinho assim que o Metabase sobe (roda uma vez e sai - acompanhe"
echo "com 'docker compose logs -f metabase-setup', o link do dashboard aparece no"
echo "final do log). Os cards rodam a query toda vez que a página é aberta, então"
echo "eles aparecem vazios/com erro até a carga terminar - não precisa reprovisionar"
echo "nada, é só recarregar o painel depois."
