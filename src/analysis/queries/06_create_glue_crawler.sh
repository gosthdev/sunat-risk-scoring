#!/usr/bin/env bash
# Fase 3 - Ejecuta los crawlers de Glue sobre silver/ y gold/ y las tablas del modelo.
# Los crawlers y la base sunat_ssco los define Terraform (infra/services): este script
# no los crea ni los modifica. Si falta alguno, aplica infra/services primero.
#
# Uso:
#   bash src/analysis/queries/06_create_glue_crawler.sh
#
# Requisito: silver/ y gold/ ya deben tener datos (corre antes los jobs 02 a 05).
set -euo pipefail

GLUE_DATABASE="${GLUE_DATABASE:-sunat_ssco}"

# Nombres definidos en infra/services/main.tf.
CRAWLERS=(
  "sunat-ssco-silver-crawler"
  "sunat-ssco-gold-crawler"
  "sunat-ssco-model-inputs-crawler"
  "sunat-ssco-predictions-crawler"
  "sunat-ssco-model-runs-crawler"
  "sunat-ssco-model-metrics-crawler"
)

for name in "${CRAWLERS[@]}"; do
  if ! aws glue get-crawler --name "$name" >/dev/null 2>&1; then
    echo "ERROR: el crawler $name no existe. Aplica infra/services (Terraform) primero." >&2
    exit 1
  fi
done

run_crawler() {
  local name="$1"
  echo "Iniciando crawler: $name"
  aws glue start-crawler --name "$name"

  while true; do
    state=$(aws glue get-crawler --name "$name" --query 'Crawler.State' --output text)
    [[ "$state" == "READY" ]] && break
    echo "  $name -> $state"
    sleep 15
  done

  status=$(aws glue get-crawler --name "$name" --query 'Crawler.LastCrawl.Status' --output text)
  echo "Crawler $name terminó con estado: $status"
  if [[ "$status" != "SUCCEEDED" ]]; then
    error_msg=$(aws glue get-crawler --name "$name" --query 'Crawler.LastCrawl.ErrorMessage' --output text 2>/dev/null || true)
    if [[ -n "$error_msg" && "$error_msg" != "None" ]]; then
      echo "ERROR en Crawler $name: $error_msg" >&2
    else
      echo "Revisa los logs del crawler en CloudWatch" >&2
    fi
    exit 1
  fi
}

for name in "${CRAWLERS[@]}"; do
  run_crawler "$name"
done

echo
echo "Tablas en el catálogo ($GLUE_DATABASE):"
aws glue get-tables --database-name "$GLUE_DATABASE" --query 'TableList[].Name' --output table

table_count=$(aws glue get-tables --database-name "$GLUE_DATABASE" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
echo "Total de tablas registradas en $GLUE_DATABASE: $table_count"

if [[ -z "$table_count" || "$table_count" == "None" || "$table_count" -lt 1 ]]; then
  echo "ERROR: Glue crawlers terminaron pero no crearon tablas en la base de datos '$GLUE_DATABASE'!" >&2
  echo "Verifique que los datos en S3 contengan archivos Parquet válidos." >&2
  exit 1
fi
