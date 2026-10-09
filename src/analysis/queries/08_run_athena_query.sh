#!/usr/bin/env bash
# Fase 3 - Corre un archivo .sql en Athena y baja el resultado como CSV para el notebook.
#
# Uso:
#   ./08_run_athena_query.sh src/analysis/queries/ruc_activos_por_departamento.sql
# Salida: data/results/<nombre_de_la_query>.csv
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Uso: $0 ruta/a/consulta.sql [ruta/a/consulta2.sql ...]" >&2
  exit 1
fi

if [[ -z "${GLUE_DATABASE:-}" ]]; then
  if aws glue get-database --name "sunat_ssco" >/dev/null 2>&1; then
    GLUE_DATABASE="sunat_ssco"
  else
    GLUE_DATABASE="ssco_catalog"
  fi
fi

if [[ -z "${ATHENA_WORKGROUP:-}" ]]; then
  if aws athena get-work-group --work-group "sunat-ssco" >/dev/null 2>&1; then
    ATHENA_WORKGROUP="sunat-ssco"
  else
    ATHENA_WORKGROUP="ssco-workgroup"
  fi
fi
OUT_DIR="${OUT_DIR:-data/results}"
mkdir -p "$OUT_DIR"

for SQL_FILE in "$@"; do
  if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: Archivo SQL no encontrado: $SQL_FILE" >&2
    exit 1
  fi

  name="$(basename "$SQL_FILE" .sql)"

  query_id=$(aws athena start-query-execution \
    --work-group "$ATHENA_WORKGROUP" \
    --query-execution-context "Database=${GLUE_DATABASE}" \
    --query-string "$(cat "$SQL_FILE")" \
    --query 'QueryExecutionId' --output text)
  echo "Query $name -> $query_id"

  while true; do
    state=$(aws athena get-query-execution --query-execution-id "$query_id" \
      --query 'QueryExecution.Status.State' --output text)
    case "$state" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$query_id" \
          --query 'QueryExecution.Status.StateChangeReason' --output text >&2
        exit 1 ;;
      *) sleep 2 ;;
    esac
  done

  output_location=$(aws athena get-query-execution --query-execution-id "$query_id" \
    --query 'QueryExecution.ResultConfiguration.OutputLocation' --output text)
  aws s3 cp "$output_location" "${OUT_DIR}/${name}.csv" --quiet

  scanned=$(aws athena get-query-execution --query-execution-id "$query_id" \
    --query 'QueryExecution.Statistics.DataScannedInBytes' --output text)
  echo "Listo: ${OUT_DIR}/${name}.csv (bytes escaneados: ${scanned})"
done
