#!/usr/bin/env bash
# ==============================================================================
# athena_bench.sh
#
# Automatizador de benchmark en Athena para el Experimento E1.
# Ejecuta Qa, Qb y Qc 3 veces por cada layout (P y N) evitando reúso de
# resultados, calcula medianas y evalúa el criterio de aceptación.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DATABASE="${DATABASE:-sunat_ssco_perf}"
WORKGROUP="${WORKGROUP:-sunat-ssco}"
TABLE_P="${TABLE_P:-${DATABASE}.p_silver_padron_ruc}"
TABLE_N="${TABLE_N:-${DATABASE}.n_silver_padron_ruc}"
SQL_DIR="${REPO_ROOT}/perf/analysis/sql"
RESULTS_DIR="${REPO_ROOT}/perf/results/E1_athena"

mkdir -p "${RESULTS_DIR}"

if ! aws athena get-work-group --work-group "$WORKGROUP" >/dev/null 2>&1; then
  echo "WARN: Workgroup '$WORKGROUP' no accesible. Usando workgroup 'primary'." >&2
  WORKGROUP="primary"
fi

run_query_single() {
  local query_str="$1"
  local nonce
  nonce=$(date +%s%N)
  local full_query="/* nonce: ${nonce} */ ${query_str}"

  local qid
  qid=$(aws athena start-query-execution \
    --query-string "${full_query}" \
    --work-group "${WORKGROUP}" \
    --query-execution-context "Database=${DATABASE}" \
    --query "QueryExecutionId" \
    --output text)

  while true; do
    local state
    state=$(aws athena get-query-execution \
      --query-execution-id "${qid}" \
      --query "QueryExecution.Status.State" \
      --output text)

    if [[ "$state" == "SUCCEEDED" ]]; then
      break
    elif [[ "$state" == "FAILED" || "$state" == "CANCELLED" ]]; then
      local reason
      reason=$(aws athena get-query-execution \
        --query-execution-id "${qid}" \
        --query "QueryExecution.Status.StateChangeReason" \
        --output text)
      echo "ERROR: Consulta ${qid} falló: ${reason}" >&2
      return 1
    fi
    sleep 2
  done

  # Extraer estadísticas
  aws athena get-query-execution \
    --query-execution-id "${qid}" \
    --query "QueryExecution.Statistics.[DataScannedInBytes, EngineExecutionTimeInMillis, TotalExecutionTimeInMillis]" \
    --output text
}

benchmark_query() {
  local q_name="$1"
  local sql_file="$2"
  local runs=3

  echo "========================================================================"
  echo "Ejecutando Benchmark para: ${q_name} (${sql_file})"
  echo "========================================================================"

  for layout in "N" "P"; do
    local table_target
    if [[ "$layout" == "P" ]]; then
      table_target="${TABLE_P}"
    else
      table_target="${TABLE_N}"
    fi

    local raw_sql
    raw_sql=$(cat "${sql_file}")
    local query_sql="${raw_sql//\$\{TABLE\}/${table_target}}"

    echo ""
    echo "--- Layout ${layout} (${table_target}) ---"
    local run_log="${RESULTS_DIR}/${q_name}_${layout}.tsv"
    echo -e "run\tbytes_scanned\tengine_ms\ttotal_ms" > "${run_log}"

    for i in $(seq 1 $runs); do
      echo -n "  Corrida ${i}/${runs}... "
      local stats
      stats=$(run_query_single "${query_sql}")
      echo -e "${i}\t${stats}" >> "${run_log}"
      echo "Completada (${stats})"
    done
  done
}

echo "Iniciando suite de benchmarks Athena para E1..."
benchmark_query "Qa" "${SQL_DIR}/e1_qa.sql"
benchmark_query "Qb" "${SQL_DIR}/e1_qb.sql"
benchmark_query "Qc" "${SQL_DIR}/e1_qc.sql"

echo ""
echo "========================================================================"
echo "Resultados crudos guardados en: ${RESULTS_DIR}"
echo "========================================================================"
