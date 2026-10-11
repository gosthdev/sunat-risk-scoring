#!/usr/bin/env bash
# ==============================================================================
# 05_submit_spark_steps.sh
#
# Orquestador de ejecución secuencial para los 5 steps de Spark sobre
# AWS EMR Serverless:
#   1. 01_ingest_bronze.py
#   2. 02_clean_silver.py
#   3. 03_feature_gold.py
#   4. 04_regional_gold.py
#   5. 05_scoring_dataset_gold.py
#
# Variables de entorno requeridas:
#   - EMR_APPLICATION_ID     : ID de la aplicación EMR Serverless
#   - EMR_EXECUTION_ROLE_ARN : ARN del rol de ejecución de EMR
#   - ARTIFACTS_BUCKET       : Nombre del bucket de soporte (jobs, logs)
#   - DATALAKE_BUCKET        : Nombre del bucket de datos (raw, bronze, silver, gold)
#
# Variables opcionales:
#   - AWS_REGION             : Región AWS (por defecto: us-east-1)
#   - POLL_INTERVAL_SECONDS  : Intervalo de sondeo en segundos (por defecto: 15)
#   - STEP_MAX_ATTEMPTS      : Intentos por step antes de abortar (por defecto: 2)
#   - RETRY_BACKOFF_SECONDS  : Espera entre intentos (por defecto: 30)
# ==============================================================================

set -euo pipefail

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-15}"
STEP_MAX_ATTEMPTS="${STEP_MAX_ATTEMPTS:-2}"
RETRY_BACKOFF_SECONDS="${RETRY_BACKOFF_SECONDS:-30}"
EMR_APPLICATION_NAME="${EMR_APPLICATION_NAME:-sunat-ssco-spark}"
DATALAKE_BUCKET="${DATALAKE_BUCKET:-sunat-risk-scoring-raw}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
RAW_BUCKET="${RAW_BUCKET:-sunat-risk-scoring-raw}"
BRONZE_BUCKET="${BRONZE_BUCKET:-sunat-risk-scoring-bronze}"
SILVER_BUCKET="${SILVER_BUCKET:-sunat-risk-scoring-silver}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"
# Permite sobreescribir shuffle.partitions vía env var (útil para perf testing)
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-200}"

# ------------------------------------------------------------------------------
# 1. Resolución y Validación de variables
# ------------------------------------------------------------------------------
MISSING_VARS=0

# Auto-descubrimiento de EMR_APPLICATION_ID por nombre si no fue especificada
if [[ -z "${EMR_APPLICATION_ID:-}" ]]; then
  echo "INFO: 'EMR_APPLICATION_ID' no fue provista. Buscando aplicación '$EMR_APPLICATION_NAME' en región '$AWS_REGION'..."
  EMR_APPLICATION_ID=$(aws emr-serverless list-applications \
    --region "$AWS_REGION" \
    --query "applications[?name=='$EMR_APPLICATION_NAME' && state!='TERMINATED'].id | [0]" \
    --output text 2>/dev/null || true)

  if [[ -z "$EMR_APPLICATION_ID" || "$EMR_APPLICATION_ID" == "None" ]]; then
    echo "ERROR: No se encontró una aplicación EMR Serverless activa con el nombre '$EMR_APPLICATION_NAME'." >&2
    MISSING_VARS=1
  else
    echo "✓ Aplicación EMR detectada automáticamente: $EMR_APPLICATION_ID"
  fi
fi

# Auto-inferencia de EMR_EXECUTION_ROLE_ARN si no fue especificada
if [[ -z "${EMR_EXECUTION_ROLE_ARN:-}" ]]; then
  echo "INFO: 'EMR_EXECUTION_ROLE_ARN' no provista. Infiriendo rol desde caller-identity..."
  ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
  if [[ -n "$ACCOUNT_ID" && "$ACCOUNT_ID" != "None" ]]; then
    EMR_EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/sunat-ssco-emr-serverless-role"
    echo "✓ Rol EMR inferido: $EMR_EXECUTION_ROLE_ARN"
  else
    echo "ERROR: La variable de entorno 'EMR_EXECUTION_ROLE_ARN' no está definida y no se pudo inferir." >&2
    MISSING_VARS=1
  fi
fi

if [[ -z "${ARTIFACTS_BUCKET:-}" ]]; then
  echo "ERROR: La variable de entorno 'ARTIFACTS_BUCKET' no está definida." >&2
  MISSING_VARS=1
fi

if [[ -z "${DATALAKE_BUCKET:-}" ]]; then
  echo "ERROR: La variable de entorno 'DATALAKE_BUCKET' no está definida." >&2
  MISSING_VARS=1
fi

if [[ "$MISSING_VARS" -ne 0 ]]; then
  echo "Por favor define todas las variables de entorno requeridas antes de ejecutar este script." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# 2. Definición y Selección de Steps
# ------------------------------------------------------------------------------
ONLY_STEPS="${ONLY_STEPS:-}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --only-steps|--only-step|--steps|--step)
      ONLY_STEPS="$2"
      shift 2
      ;;
    *)
      if [[ "$1" =~ ^0[1-6] ]]; then
        ONLY_STEPS="$1"
        shift
      else
        echo "WARN: Parámetro desconocido: $1" >&2
        shift
      fi
      ;;
  esac
done

ALL_STEPS=(
  "01_ingest_bronze"
  "02_clean_silver"
  "03_feature_gold"
  "04_regional_gold"
  "05_scoring_dataset_gold"
)

# Steps que solo se ejecutan si se piden con --only-steps (no forman parte del pipeline base).
OPTIONAL_STEPS=(
  "06_dataset_curado"
)
KNOWN_STEPS=("${ALL_STEPS[@]}" "${OPTIONAL_STEPS[@]}")

if [[ -n "${ONLY_STEPS}" ]]; then
  STEPS=()
  NORMALIZED_STEPS=$(echo "${ONLY_STEPS}" | tr ',' ' ')
  for raw_s in ${NORMALIZED_STEPS}; do
    clean_s="${raw_s%.py}"
    clean_s="$(echo "$clean_s" | xargs)"
    if [[ -z "$clean_s" ]]; then
      continue
    fi
    for full_step in "${KNOWN_STEPS[@]}"; do
      if [[ "$clean_s" == "$full_step" || "$full_step" == "${clean_s}"* || "$clean_s" == "job${full_step:0:2}"* ]]; then
        clean_s="$full_step"
        break
      fi
    done
    STEPS+=("$clean_s")
  done
else
  STEPS=("${ALL_STEPS[@]}")
fi

TOTAL_STEPS="${#STEPS[@]}"
PY_FILES_URI="s3://${ARTIFACTS_BUCKET}/jobs/common.zip"
LOG_URI="s3://${ARTIFACTS_BUCKET}/emr-logs/"

echo "========================================================================"
echo "Iniciando Pipeline EMR Serverless (${TOTAL_STEPS} step(s))"
echo "Aplicación EMR ID : ${EMR_APPLICATION_ID}"
echo "Región AWS        : ${AWS_REGION}"
echo "Bucket Artefactos : ${ARTIFACTS_BUCKET}"
echo "Bucket DataLake   : ${DATALAKE_BUCKET}"
echo "Steps a ejecutar  : ${STEPS[*]}"
echo "Total de Steps    : ${TOTAL_STEPS}"
echo "========================================================================"

# ------------------------------------------------------------------------------
# 3. Rutas de salida y publicación
#
# Los steps de Silver y Gold escriben en <bucket>/_staging/... y solo cuando el
# job termina bien se publican a la ruta viva con `aws s3 sync --delete`. Si un
# job falla a medias, la ruta viva (la que lee Athena) conserva la última
# versión válida.
# ------------------------------------------------------------------------------
IS_SINGLE_BUCKET=0
if [[ "$RAW_BUCKET" == "$BRONZE_BUCKET" && "$BRONZE_BUCKET" == "$SILVER_BUCKET" && "$SILVER_BUCKET" == "$GOLD_BUCKET" ]]; then
  IS_SINGLE_BUCKET=1
fi

# live_uri <bucket> <capa> <subruta>
live_uri() {
  if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
    echo "s3://$1/$2/$3"
  else
    echo "s3://$1/$3"
  fi
}

# staging_uri <bucket> <capa> <subruta>  (espejo de staging_de en s3_paths.py)
staging_uri() {
  if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
    echo "s3://$1/_staging/$2/$3"
  else
    echo "s3://$1/_staging/$3"
  fi
}

# Salidas que cada step deja en staging: "<bucket> <capa> <subruta> <opcional 0|1>".
# Las opcionales (cuarentena) pueden no existir si no hubo filas rechazadas.
step_outputs() {
  case "$1" in
    02_clean_silver)
      echo "$SILVER_BUCKET silver padron_ruc 0"
      echo "$SILVER_BUCKET silver ordenes_compra 0"
      echo "$SILVER_BUCKET silver quarantine_padron_ruc 1"
      echo "$SILVER_BUCKET silver quarantine_ordenes_compra 1"
      echo "$SILVER_BUCKET silver quality_report 0"
      ;;
    03_feature_gold) echo "$GOLD_BUCKET gold ruc_features 0" ;;
    04_regional_gold) echo "$GOLD_BUCKET gold regional_summary 0" ;;
    05_scoring_dataset_gold) echo "$GOLD_BUCKET gold scoring_dataset 0" ;;
  esac
}

s3_prefix_has_objects() {
  [[ -n "$(aws s3 ls "$1/" 2>/dev/null | head -n 1)" ]]
}

clear_staging() {
  local bucket layer sub optional
  while read -r bucket layer sub optional; do
    [[ -z "$bucket" ]] && continue
    aws s3 rm "$(staging_uri "$bucket" "$layer" "$sub")/" --recursive --only-show-errors >/dev/null 2>&1 || true
  done < <(step_outputs "$1")
}

# Publica staging -> ruta viva. Primero verifica que toda salida obligatoria exista:
# si falta alguna no se toca nada de lo publicado.
publish_step() {
  local step="$1" bucket layer sub optional staging live
  while read -r bucket layer sub optional; do
    [[ -z "$bucket" ]] && continue
    staging="$(staging_uri "$bucket" "$layer" "$sub")"
    if [[ "$optional" -eq 0 ]] && ! s3_prefix_has_objects "$staging"; then
      echo "✗ ERROR: '${step}' terminó sin dejar ${staging}; no se publica nada." >&2
      return 1
    fi
  done < <(step_outputs "$step")

  while read -r bucket layer sub optional; do
    [[ -z "$bucket" ]] && continue
    staging="$(staging_uri "$bucket" "$layer" "$sub")"
    live="$(live_uri "$bucket" "$layer" "$sub")"
    if s3_prefix_has_objects "$staging"; then
      echo "Publicando ${staging} -> ${live}"
      aws s3 sync "${staging}/" "${live}/" --delete --only-show-errors
    else
      echo "Sin ${staging} (salida opcional): se limpia ${live}"
      aws s3 rm "${live}/" --recursive --only-show-errors >/dev/null 2>&1 || true
    fi
  done < <(step_outputs "$step")

  clear_staging "$step"
}

# ------------------------------------------------------------------------------
# 4. Ejecución de un step (un intento) y monitoreo
# ------------------------------------------------------------------------------
# run_step_once <step>: devuelve 0 si el Job Run termina en SUCCESS, 1 en otro caso.
run_step_once() {
  local step="$1"
  local SCRIPT_NAME="${step}.py"
  local ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/${SCRIPT_NAME}"
  local JOB_NAME="sunat-ssco-${step}"
  local JOB_RUN_ID STATE TIMESTAMP SPARK_PARAMS

  # Parámetros spark-submit: empaquetado común, variables de buckets y dimensionamiento controlado
  SPARK_PARAMS="--py-files ${PY_FILES_URI} \
--conf spark.driver.cores=4 \
--conf spark.driver.memory=14g \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=14g \
--conf spark.dynamicAllocation.maxExecutors=3 \
--conf spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS} \
--conf spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED \
--conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED \
--conf spark.sql.parquet.enableVectorizedReader=false \
--conf spark.sql.parquet.mergeSchema=true \
--conf spark.emr-serverless.driverEnv.DATALAKE_BUCKET=${DATALAKE_BUCKET} \
--conf spark.executorEnv.DATALAKE_BUCKET=${DATALAKE_BUCKET} \
--conf spark.emr-serverless.driverEnv.RAW_BUCKET=${RAW_BUCKET} \
--conf spark.executorEnv.RAW_BUCKET=${RAW_BUCKET} \
--conf spark.emr-serverless.driverEnv.BRONZE_BUCKET=${BRONZE_BUCKET} \
--conf spark.executorEnv.BRONZE_BUCKET=${BRONZE_BUCKET} \
--conf spark.emr-serverless.driverEnv.SILVER_BUCKET=${SILVER_BUCKET} \
--conf spark.executorEnv.SILVER_BUCKET=${SILVER_BUCKET} \
--conf spark.emr-serverless.driverEnv.GOLD_BUCKET=${GOLD_BUCKET} \
--conf spark.executorEnv.GOLD_BUCKET=${GOLD_BUCKET} \
--conf spark.emr-serverless.driverEnv.DATASET_VERSION=${DATASET_VERSION:-v1}"

  # Iniciar Job Run en EMR Serverless
  JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --region "${AWS_REGION}" \
    --application-id "${EMR_APPLICATION_ID}" \
    --execution-role-arn "${EMR_EXECUTION_ROLE_ARN}" \
    --name "${JOB_NAME}" \
    --job-driver "{
      \"sparkSubmit\": {
        \"entryPoint\": \"${ENTRY_POINT}\",
        \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
      }
    }" \
    --configuration-overrides "{
      \"monitoringConfiguration\": {
        \"s3MonitoringConfiguration\": {
          \"logUri\": \"${LOG_URI}\"
        }
      }
    }" \
    --query 'jobRunId' \
    --output text) || { echo "✗ No se pudo lanzar el Job Run de '${step}'." >&2; return 1; }

  echo "Job Run ID asignado: ${JOB_RUN_ID}"
  echo "Esperando finalización (sondeo cada ${POLL_INTERVAL_SECONDS}s)..."

  # Bucle de sondeo hasta estado terminal
  while true; do
    STATE=$(aws emr-serverless get-job-run \
      --region "${AWS_REGION}" \
      --application-id "${EMR_APPLICATION_ID}" \
      --job-run-id "${JOB_RUN_ID}" \
      --query 'jobRun.state' \
      --output text) || { echo "✗ No se pudo consultar el estado del Job Run ${JOB_RUN_ID}." >&2; return 1; }

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${TIMESTAMP}] Step: ${step} | Estado: ${STATE}"

    case "${STATE}" in
      SUCCESS)
        echo "✓ Step '${step}' completado con éxito."
        return 0
        ;;
      FAILED|CANCELLED|CANCELLING)
        echo "✗ El step '${step}' terminó con estado '${STATE}'." >&2
        echo "Detalles del error:" >&2
        aws emr-serverless get-job-run \
          --region "${AWS_REGION}" \
          --application-id "${EMR_APPLICATION_ID}" \
          --job-run-id "${JOB_RUN_ID}" \
          --query 'jobRun.stateDetails' \
          --output text 2>/dev/null || true
        echo "Revisar logs en: ${LOG_URI}applications/${EMR_APPLICATION_ID}/jobs/${JOB_RUN_ID}/" >&2
        return 1
        ;;
      SUBMITTED|PENDING|SCHEDULED|RUNNING)
        sleep "${POLL_INTERVAL_SECONDS}"
        ;;
      *)
        echo "Estado inesperado '${STATE}'. Reintentando en ${POLL_INTERVAL_SECONDS}s..."
        sleep "${POLL_INTERVAL_SECONDS}"
        ;;
    esac
  done
}

# ------------------------------------------------------------------------------
# 5. Ejecución secuencial con reintentos
# ------------------------------------------------------------------------------
CURRENT_STEP=0

for step in "${STEPS[@]}"; do
  CURRENT_STEP=$((CURRENT_STEP + 1))

  echo ""
  echo "------------------------------------------------------------------------"
  echo "[${CURRENT_STEP}/${TOTAL_STEPS}] Lanzando: ${step}"
  echo "------------------------------------------------------------------------"

  ok=0
  for attempt in $(seq 1 "${STEP_MAX_ATTEMPTS}"); do
    echo "Intento ${attempt}/${STEP_MAX_ATTEMPTS} de '${step}'"
    # Un intento fallido pudo dejar archivos a medias en staging.
    clear_staging "$step"
    if run_step_once "$step"; then
      ok=1
      break
    fi
    if [[ "$attempt" -lt "$STEP_MAX_ATTEMPTS" ]]; then
      echo "Reintentando '${step}' en ${RETRY_BACKOFF_SECONDS}s..."
      sleep "${RETRY_BACKOFF_SECONDS}"
    fi
  done

  if [[ "$ok" -ne 1 ]]; then
    echo "✗ ERROR: '${step}' falló tras ${STEP_MAX_ATTEMPTS} intento(s)." >&2
    echo "  La ruta publicada no se modificó: Athena sigue sirviendo la última versión válida." >&2
    clear_staging "$step"
    exit 1
  fi

  publish_step "$step" || exit 1
done

echo ""
echo "========================================================================"
echo "✓ Pipeline completado con éxito: Todos los ${TOTAL_STEPS} step(s) finalizaron en SUCCESS."
echo "========================================================================"
