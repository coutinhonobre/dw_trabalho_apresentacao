#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Recriando containers do zero (schema + dados serão reprocessados)..."
docker compose down -v
docker compose up -d --force-recreate

echo "Pronto. Postgres em localhost:5432 (bancos datatran e dw)."
echo "Airflow em http://localhost:8080 (pode levar 1-2 min pra ficar disponível)."
echo "Usuário/senha do Airflow: airflow / airflow."
echo "Metabase em http://localhost:3000 (login padrão: metabase@metabase.com / metabase)."
echo
echo "Depois do Postgres subir, popule o catálogo de metadados (não roda automaticamente):"
echo "  docker exec -i datatran_postgres psql -U postgres -d dw < dw/metadados_seed_postgres.sql"
echo
echo "Para disparar a primeira carga do DW:"
echo "  docker exec dw_airflow airflow dags trigger carga_inicial_dw"
echo
echo "O container metabase-setup provisiona o painel 'Painel PRF - Acidentes de"
echo "Trânsito' sozinho assim que o Metabase sobe (roda uma vez e sai - acompanhe"
echo "com 'docker compose logs -f metabase-setup', o link do dashboard aparece no"
echo "final do log). Os cards rodam a query toda vez que a página é aberta, então"
echo "eles aparecem vazios até você disparar a carga_inicial_dw - não precisa"
echo "reprovisionar nada, é só recarregar o painel depois."
