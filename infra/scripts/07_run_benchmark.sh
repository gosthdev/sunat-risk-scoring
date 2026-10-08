#!/usr/bin/env bash
# ==============================================================================
# 07_run_benchmark.sh
#
# Benchmark de escritura Bronze: corre 01_ingest_bronze.py DOS veces en
# EMR Serverless y compara el tiempo total:
#   1. unpartitioned : escritura Parquet sin partitionBy
#   2. partitioned   : escritura Parquet con partitionBy("anio", "mes")
#
# Las salidas van a un árbol aislado (s3://<BRONZE_BUCKET>/benchmark/<run_id>/...)
# para NO tocar el bronze de producción.
#
# Métricas por corrida:
#   - wall_clock_s : segundos desde start-job-run hasta estado terminal
#                    (incluye cola/arranque del worker) -> "tiempo total"
#   - exec_s       : totalExecutionDurationSeconds reportado por EMR Serverless
#   - n_objects / size_bytes : objetos y bytes escritos en S3 (best-effort)
#
# Variables opcionales (mismos defaults que 05_submit_spark_steps.sh):
#   EMR_APPLICATION_ID, EMR_APPLICATION_NAME, EMR_EXECUTION_ROLE_ARN,
#   ARTIFACTS_BUCKET, DATALAKE_BUCKET, RAW_BUCKET, BRONZE_BUCKET, SILVER_BUCKET,
#   GOLD_BUCKET, AWS_REGION, POLL_INTERVAL_SECONDS
#   SKIP_UPLOAD=1   -> no empaqueta ni sube common.zip / 01_ingest_bronze.py
#   KEEP_OUTPUT=1   -> no borra el árbol benchmark/<run_id>/ al terminar
#   RESULTS_DIR     -> dónde guardar benchmark_results.csv (default: ./benchmark_results)
#
# Uso: bash infra/scripts/07_run_benchmark.sh
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JOBS_DIR="$REPO_ROOT/src/spark/jobs"

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-15}"
EMR_APPLICATION_NAME="${EMR_APPLICATION_NAME:-sunat-ssco-spark}"
DATALAKE_BUCKET="${DATALAKE_BUCKET:-sunat-risk-scoring}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
RAW_BUCKET="${RAW_BUCKET:-sunat-risk-scoring-raw}"
BRONZE_BUCKET="${BRONZE_BUCKET:-sunat-risk-scoring-bronze}"
SILVER_BUCKET="${SILVER_BUCKET:-sunat-risk-scoring-silver}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"
RESULTS_DIR="${RESULTS_DIR:-./benchmark_results}"
SKIP_UPLOAD="${SKIP_UPLOAD:-0}"
KEEP_OUTPUT="${KEEP_OUTPUT:-0}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
BENCH_PREFIX="benchmark/${RUN_ID}"
PY_FILES_URI="s3://${ARTIFACTS_BUCKET}/jobs/common.zip"
ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/01_ingest_bronze.py"
LOG_URI="s3://${ARTIFACTS_BUCKET}/emr-logs/"

command -v aws >/dev/null || { echo "ERROR: aws CLI no encontrado." >&2; exit 1; }
command -v jq  >/dev/null || { echo "ERROR: jq no encontrado." >&2; exit 1; }

# ------------------------------------------------------------------------------
# 1. Resolución de aplicación y rol EMR (misma lógica que 05_submit_spark_steps.sh)
# ------------------------------------------------------------------------------
if [[ -z "${EMR_APPLICATION_ID:-}" ]]; then
  echo "INFO: buscando aplicación EMR '$EMR_APPLICATION_NAME' en '$AWS_REGION'..."
  EMR_APPLICATION_ID=$(aws emr-serverless list-applications \
    --region "$AWS_REGION" \
    --query "applications[?name=='$EMR_APPLICATION_NAME' && state!='TERMINATED'].id | [0]" \
    --output text 2>/dev/null || true)
  if [[ -z "$EMR_APPLICATION_ID" || "$EMR_APPLICATION_ID" == "None" ]]; then
    echo "ERROR: no se encontró la aplicación EMR '$EMR_APPLICATION_NAME'." >&2
    exit 1
  fi
fi

if [[ -z "${EMR_EXECUTION_ROLE_ARN:-}" ]]; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
  if [[ -z "$ACCOUNT_ID" || "$ACCOUNT_ID" == "None" ]]; then
    echo "ERROR: define EMR_EXECUTION_ROLE_ARN (no se pudo inferir)." >&2
    exit 1
  fi
  EMR_EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/sunat-ssco-emr-serverless-role"
fi

# ------------------------------------------------------------------------------
# 2. Empaquetar y subir artefactos (para que EMR use la versión parametrizada)
# ------------------------------------------------------------------------------
if [[ "$SKIP_UPLOAD" != "1" ]]; then
  echo "INFO: empaquetando y subiendo common.zip y 01_ingest_bronze.py..."
  bash "$JOBS_DIR/package_jobs.sh"
  aws s3 cp "$JOBS_DIR/common.zip" "$PY_FILES_URI" --region "$AWS_REGION"
  aws s3 cp "$JOBS_DIR/01_ingest_bronze.py" "$ENTRY_POINT" --region "$AWS_REGION"
fi

# ------------------------------------------------------------------------------
# 3. Función que corre UNA ejecución y registra métricas
# ------------------------------------------------------------------------------
mkdir -p "$RESULTS_DIR"
RESULTS_CSV="$RESULTS_DIR/benchmark_results.csv"
if [[ ! -f "$RESULTS_CSV" ]]; then
  echo "run_id,mode,job_run_id,state,wall_clock_s,exec_s,n_objects,size_bytes" > "$RESULTS_CSV"
fi

# Resultado de la última corrida (globales para el resumen final)
LAST_WALL=0; LAST_EXEC=0; LAST_OBJS=0; LAST_BYTES=0

run_ingest() {
  local mode="$1"
  local output_root="s3://${BRONZE_BUCKET}/${BENCH_PREFIX}/${mode}"

  local spark_params
  spark_params="--py-files ${PY_FILES_URI}"
  for var in DATALAKE_BUCKET RAW_BUCKET BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET; do
    spark_params+=" --conf spark.emr-serverless.driverEnv.${var}=${!var}"
    spark_params+=" --conf spark.executorEnv.${var}=${!var}"
  done

  local job_driver
  job_driver=$(jq -n \
    --arg entry "$ENTRY_POINT" \
    --arg params "$spark_params" \
    --arg mode "$mode" \
    --arg out "$output_root" \
    '{sparkSubmit: {
        entryPoint: $entry,
        entryPointArguments: ["--partition-mode", $mode, "--output-root", $out],
        sparkSubmitParameters: $params}}')

  local overrides
  overrides=$(jq -n --arg log "$LOG_URI" \
    '{monitoringConfiguration: {s3MonitoringConfiguration: {logUri: $log}}}')

  echo ""
  echo "------------------------------------------------------------------------"
  echo "Benchmark [$mode] -> $output_root"
  echo "------------------------------------------------------------------------"

  local t_start t_end job_run_id state
  t_start=$(date +%s)

  job_run_id=$(aws emr-serverless start-job-run \
    --region "$AWS_REGION" \
    --application-id "$EMR_APPLICATION_ID" \
    --execution-role-arn "$EMR_EXECUTION_ROLE_ARN" \
    --name "sunat-ssco-bench-${mode}-${RUN_ID}" \
    --job-driver "$job_driver" \
    --configuration-overrides "$overrides" \
    --query 'jobRunId' --output text)
  echo "Job Run ID: $job_run_id"

  while true; do
    state=$(aws emr-serverless get-job-run \
      --region "$AWS_REGION" \
      --application-id "$EMR_APPLICATION_ID" \
      --job-run-id "$job_run_id" \
      --query 'jobRun.state' --output text)
    echo "[$(date '+%H:%M:%S')] $mode | $state"
    case "$state" in
      SUCCESS) break ;;
      FAILED|CANCELLED)
        t_end=$(date +%s)
        echo "ERROR: la corrida '$mode' terminó en $state." >&2
        aws emr-serverless get-job-run --region "$AWS_REGION" \
          --application-id "$EMR_APPLICATION_ID" --job-run-id "$job_run_id" \
          --query 'jobRun.stateDetails' --output text >&2 || true
        echo "${RUN_ID},${mode},${job_run_id},${state},$((t_end - t_start)),,," >> "$RESULTS_CSV"
        echo "Logs: ${LOG_URI}applications/${EMR_APPLICATION_ID}/jobs/${job_run_id}/" >&2
        exit 1
        ;;
      *) sleep "$POLL_INTERVAL_SECONDS" ;;
    esac
  done
  t_end=$(date +%s)

  LAST_WALL=$((t_end - t_start))
  LAST_EXEC=$(aws emr-serverless get-job-run \
    --region "$AWS_REGION" \
    --application-id "$EMR_APPLICATION_ID" \
    --job-run-id "$job_run_id" \
    --query 'jobRun.totalExecutionDurationSeconds' --output text 2>/dev/null || echo "")
  [[ "$LAST_EXEC" == "None" ]] && LAST_EXEC=""

  # Tamaño/cantidad de objetos escritos (best-effort: requiere s3:ListBucket en bronze)
  LAST_OBJS=""; LAST_BYTES=""
  local summary
  if summary=$(aws s3 ls "${output_root}/" --recursive --summarize --region "$AWS_REGION" 2>/dev/null); then
    LAST_OBJS=$(awk '/Total Objects:/ {print $3}' <<<"$summary")
    LAST_BYTES=$(awk '/Total Size:/ {print $3}' <<<"$summary")
  else
    echo "WARN: no se pudo listar $output_root (¿permisos s3:ListBucket?)."
  fi

  echo "${RUN_ID},${mode},${job_run_id},${state},${LAST_WALL},${LAST_EXEC},${LAST_OBJS},${LAST_BYTES}" >> "$RESULTS_CSV"
  echo "✓ [$mode] wall_clock=${LAST_WALL}s exec=${LAST_EXEC:-n/a}s objetos=${LAST_OBJS:-n/a} bytes=${LAST_BYTES:-n/a}"
}

# ------------------------------------------------------------------------------
# 4. Ejecutar ambas variantes (sin particionar primero, luego particionado)
# ------------------------------------------------------------------------------
echo "========================================================================"
echo "Benchmark Bronze | run_id=${RUN_ID} | app=${EMR_APPLICATION_ID} | región=${AWS_REGION}"
echo "========================================================================"

run_ingest "unpartitioned"
UNPART_WALL=$LAST_WALL; UNPART_EXEC=$LAST_EXEC; UNPART_OBJS=$LAST_OBJS; UNPART_BYTES=$LAST_BYTES

run_ingest "partitioned"
PART_WALL=$LAST_WALL; PART_EXEC=$LAST_EXEC; PART_OBJS=$LAST_OBJS; PART_BYTES=$LAST_BYTES

# ------------------------------------------------------------------------------
# 5. Resumen
# ------------------------------------------------------------------------------
echo ""
echo "========================================================================"
echo "RESUMEN (run_id=${RUN_ID})"
printf "%-15s %-14s %-10s %-10s %-14s\n" "modo" "wall_clock_s" "exec_s" "objetos" "bytes"
printf "%-15s %-14s %-10s %-10s %-14s\n" "unpartitioned" "$UNPART_WALL" "${UNPART_EXEC:-n/a}" "${UNPART_OBJS:-n/a}" "${UNPART_BYTES:-n/a}"
printf "%-15s %-14s %-10s %-10s %-14s\n" "partitioned"   "$PART_WALL"   "${PART_EXEC:-n/a}"   "${PART_OBJS:-n/a}"   "${PART_BYTES:-n/a}"
if [[ "$UNPART_WALL" -gt 0 ]]; then
  DIFF=$((PART_WALL - UNPART_WALL))
  PCT=$(awk -v p="$PART_WALL" -v u="$UNPART_WALL" 'BEGIN { printf "%.1f", (p - u) * 100 / u }')
  echo "Diferencia (partitioned - unpartitioned): ${DIFF}s (${PCT}%)"
fi
echo "Resultados acumulados en: $RESULTS_CSV"
echo "NOTA: una sola corrida por modo es ruidosa; repite el script para obtener un promedio."
echo "========================================================================"

# ------------------------------------------------------------------------------
# 6. Limpieza de la salida del benchmark (no es bronze de producción)
# ------------------------------------------------------------------------------
if [[ "$KEEP_OUTPUT" != "1" ]]; then
  echo "Limpiando s3://${BRONZE_BUCKET}/${BENCH_PREFIX}/ (KEEP_OUTPUT=1 para conservar)..."
  aws s3 rm "s3://${BRONZE_BUCKET}/${BENCH_PREFIX}/" --recursive --region "$AWS_REGION" --only-show-errors || \
    echo "WARN: no se pudo limpiar la salida del benchmark."
fi