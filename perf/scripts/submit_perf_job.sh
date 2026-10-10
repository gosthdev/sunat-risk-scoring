#!/usr/bin/env bash
# ==============================================================================
# submit_perf_job.sh
#
# Lanzador de jobs Spark sobre la aplicación EMR Serverless de pruebas
# (sunat-ssco-perf-spark), soportando perfiles C1, C3, C7 y guardando
# los metadatos de ejecución (get-job-run JSON) y Git SHA.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
EMR_APPLICATION_NAME="${EMR_APPLICATION_NAME:-sunat-ssco-perf-spark}"
EMR_APPLICATION_ID="${EMR_APPLICATION_ID:-}"
EMR_EXECUTION_ROLE_ARN="${EMR_EXECUTION_ROLE_ARN:-}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
RESULTS_DIR="${REPO_ROOT}/perf/results"

PROFILE="C3"
SCRIPT_NAME="perf_runner.py"
JOB_ARGS=""
RUN_NAME="perf-run"
EXP_TAG="misc"
EXECUTOR_DISK_ARG=""
S3A_BUFFER_ARG=""
S3A_MULTIPART_SIZE_ARG=""
EXTRA_CONFS=()
DRY_RUN=false

usage() {
  echo "Uso: $0 [opciones]"
  echo "Opciones:"
  echo "  --profile <C1|C3|C7>               Perfil de recursos (default: C3)"
  echo "  --executor-disk <disk>             Tamaño de disco del executor (ej. 100g, 200g)"
  echo "  --s3a-buffer <bytebuffer|array|disk> Modo de buffer S3A Fast Upload (default: bytebuffer)"
  echo "  --s3a-multipart-size <size>        Tamaño de partición S3A multipart (default: 64M)"
  echo "  --conf <clave=valor>               Configuración Spark adicional (repetible)"
  echo "  --script <nombre_script.py>        Script de entrada (default: perf_runner.py)"
  echo "  --job-args \"<argumentos>\"          Argumentos pasados al script"
  echo "  --run-name <nombre>                Nombre identificador del job run"
  echo "  --exp-tag <E1|E2|scale|...>        Etiqueta del experimento para organizar resultados"
  echo "  --raw-bucket <bucket>              Bucket para RAW_BUCKET"
  echo "  --bronze-bucket <bucket>           Bucket para BRONZE_BUCKET"
  echo "  --silver-bucket <bucket>           Bucket para SILVER_BUCKET"
  echo "  --gold-bucket <bucket>             Bucket para GOLD_BUCKET"
  echo "  --dry-run                          Solo imprime los parámetros sin lanzar el job en AWS"
  exit 1
}

RAW_BUCKET_VAL="${RAW_BUCKET:-sunat-risk-scoring-raw}"
BRONZE_BUCKET_VAL="${BRONZE_BUCKET:-sunat-risk-scoring-perf-36p}"
SILVER_BUCKET_VAL="${SILVER_BUCKET:-sunat-risk-scoring-perf-36p}"
GOLD_BUCKET_VAL="${GOLD_BUCKET:-sunat-risk-scoring-perf-36p}"
CONDITION="${CONDITION:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile|-p)
      PROFILE="$2"
      shift 2
      ;;
    --executor-disk)
      EXECUTOR_DISK_ARG="$2"
      shift 2
      ;;
    --s3a-buffer)
      S3A_BUFFER_ARG="$2"
      shift 2
      ;;
    --s3a-multipart-size)
      S3A_MULTIPART_SIZE_ARG="$2"
      shift 2
      ;;
    --conf|--extra-conf)
      EXTRA_CONFS+=("$2")
      shift 2
      ;;
    --script|-s)
      SCRIPT_NAME="$2"
      shift 2
      ;;
    --job-args)
      JOB_ARGS="$2"
      shift 2
      ;;
    --run-name)
      RUN_NAME="$2"
      shift 2
      ;;
    --exp-tag)
      EXP_TAG="$2"
      shift 2
      ;;
    --condition)
      CONDITION="$2"
      shift 2
      ;;
    --raw-bucket)
      RAW_BUCKET_VAL="$2"
      shift 2
      ;;
    --bronze-bucket)
      BRONZE_BUCKET_VAL="$2"
      shift 2
      ;;
    --silver-bucket)
      SILVER_BUCKET_VAL="$2"
      shift 2
      ;;
    --gold-bucket)
      GOLD_BUCKET_VAL="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      usage
      ;;
  esac
done

# Defaults base de Spark (recursos equivalentes a perfil C3)
SPARK_DRIVER_CORES="${SPARK_DRIVER_CORES:-4}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-14g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-4}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-14g}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-3}"
SPARK_DYNAMIC_ALLOCATION="${SPARK_DYNAMIC_ALLOCATION:-false}"
SPARK_EXECUTOR_DISK="${SPARK_EXECUTOR_DISK:-100g}"
S3A_FAST_UPLOAD_BUFFER="${S3A_FAST_UPLOAD_BUFFER:-bytebuffer}"
S3A_MULTIPART_SIZE="${S3A_MULTIPART_SIZE:-64M}"

# Cargar perfil
PROFILE_ENV_FILE="${REPO_ROOT}/perf/env/${PROFILE,,}.env"
if [[ ! -f "$PROFILE_ENV_FILE" ]]; then
  PROFILE_ENV_FILE="${REPO_ROOT}/perf/env/${PROFILE,,}.env.example"
fi
if [[ -f "$PROFILE_ENV_FILE" ]]; then
  echo "[submit_perf_job] Cargando perfil desde ${PROFILE_ENV_FILE}"
  # shellcheck source=/dev/null
  source "$PROFILE_ENV_FILE"

  # Si el perfil cargado referencia otro perfil de recursos (ej. PROFILE="C3" en e2.env),
  # cargar también dicho archivo de hardware si difiere del actual
  TARGET_PROFILE_LOWER="${PROFILE,,}"
  CURRENT_BASENAME="${PROFILE_ENV_FILE##*/}"
  CURRENT_PREFIX="${CURRENT_BASENAME%.env*}"
  if [[ -n "$TARGET_PROFILE_LOWER" && "$TARGET_PROFILE_LOWER" != "$CURRENT_PREFIX" ]]; then
    SUB_PROFILE_FILE="${REPO_ROOT}/perf/env/${TARGET_PROFILE_LOWER}.env"
    if [[ ! -f "$SUB_PROFILE_FILE" ]]; then
      SUB_PROFILE_FILE="${REPO_ROOT}/perf/env/${TARGET_PROFILE_LOWER}.env.example"
    fi
    if [[ -f "$SUB_PROFILE_FILE" ]]; then
      echo "[submit_perf_job] Cargando perfil base referenciado desde ${SUB_PROFILE_FILE}"
      # shellcheck source=/dev/null
      source "$SUB_PROFILE_FILE"
    fi
  fi
else
  echo "WARN: No se encontró archivo de perfil para $PROFILE. Usando defaults C3."
fi

# Fallback final garantizado para todas las variables requeridas
SPARK_DRIVER_CORES="${SPARK_DRIVER_CORES:-4}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-14g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-4}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-14g}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-3}"
SPARK_DYNAMIC_ALLOCATION="${SPARK_DYNAMIC_ALLOCATION:-false}"
SPARK_EXECUTOR_DISK="${SPARK_EXECUTOR_DISK:-100g}"
S3A_FAST_UPLOAD_BUFFER="${S3A_FAST_UPLOAD_BUFFER:-bytebuffer}"
S3A_MULTIPART_SIZE="${S3A_MULTIPART_SIZE:-64M}"

# Sobrescrituras explícitas por línea de comandos
if [[ -n "$EXECUTOR_DISK_ARG" ]]; then
  SPARK_EXECUTOR_DISK="$EXECUTOR_DISK_ARG"
fi
if [[ -n "$S3A_BUFFER_ARG" ]]; then
  S3A_FAST_UPLOAD_BUFFER="$S3A_BUFFER_ARG"
fi
if [[ -n "$S3A_MULTIPART_SIZE_ARG" ]]; then
  S3A_MULTIPART_SIZE="$S3A_MULTIPART_SIZE_ARG"
fi

# Detectar Application ID si no fue provista
if [[ -z "$EMR_APPLICATION_ID" ]]; then
  EMR_APPLICATION_ID=$(aws emr-serverless list-applications \
    --region "$AWS_REGION" \
    --query "applications[?name=='$EMR_APPLICATION_NAME' && state!='TERMINATED'].id | [0]" \
    --output text 2>/dev/null || true)
fi

# Detectar Execution Role si no fue provisto
if [[ -z "$EMR_EXECUTION_ROLE_ARN" ]]; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
  if [[ -n "$ACCOUNT_ID" && "$ACCOUNT_ID" != "None" ]]; then
    EMR_EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/sunat-ssco-perf-emr-role"
  fi
fi

GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

ENTRY_POINT="s3://${ARTIFACTS_BUCKET}/jobs/perf/${SCRIPT_NAME}"
PY_FILES="s3://${ARTIFACTS_BUCKET}/jobs/perf/common.zip"
LOG_URI="s3://${ARTIFACTS_BUCKET}/emr-logs/"

SPARK_PARAMS="--py-files ${PY_FILES} \
--conf spark.driver.cores=${SPARK_DRIVER_CORES} \
--conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
--conf spark.executor.cores=${SPARK_EXECUTOR_CORES} \
--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
--conf spark.executor.instances=${SPARK_EXECUTOR_INSTANCES} \
--conf spark.dynamicAllocation.enabled=${SPARK_DYNAMIC_ALLOCATION} \
--conf spark.emr-serverless.executor.disk=${SPARK_EXECUTOR_DISK} \
--conf spark.hadoop.fs.s3a.fast.upload=true \
--conf spark.hadoop.fs.s3a.fast.upload.buffer=${S3A_FAST_UPLOAD_BUFFER} \
--conf spark.hadoop.fs.s3a.multipart.size=${S3A_MULTIPART_SIZE} \
--conf spark.emr-serverless.driverEnv.RAW_BUCKET=${RAW_BUCKET_VAL} \
--conf spark.driverEnv.RAW_BUCKET=${RAW_BUCKET_VAL} \
--conf spark.executorEnv.RAW_BUCKET=${RAW_BUCKET_VAL} \
--conf spark.emr-serverless.driverEnv.BRONZE_BUCKET=${BRONZE_BUCKET_VAL} \
--conf spark.driverEnv.BRONZE_BUCKET=${BRONZE_BUCKET_VAL} \
--conf spark.executorEnv.BRONZE_BUCKET=${BRONZE_BUCKET_VAL} \
--conf spark.emr-serverless.driverEnv.SILVER_BUCKET=${SILVER_BUCKET_VAL} \
--conf spark.driverEnv.SILVER_BUCKET=${SILVER_BUCKET_VAL} \
--conf spark.executorEnv.SILVER_BUCKET=${SILVER_BUCKET_VAL} \
--conf spark.emr-serverless.driverEnv.GOLD_BUCKET=${GOLD_BUCKET_VAL} \
--conf spark.driverEnv.GOLD_BUCKET=${GOLD_BUCKET_VAL} \
--conf spark.executorEnv.GOLD_BUCKET=${GOLD_BUCKET_VAL}"

for extra_conf in "${EXTRA_CONFS[@]}"; do
  SPARK_PARAMS="${SPARK_PARAMS} --conf ${extra_conf}"
done

# Parse entry point arguments to JSON array
ARGS_ARRAY="[]"
if [[ -n "$JOB_ARGS" ]]; then
  read -ra SPLIT_ARGS <<< "$JOB_ARGS"
  ARGS_JSON=$(printf '%s\n' "${SPLIT_ARGS[@]}" | jq -R . | jq -s .)
  ARGS_ARRAY="$ARGS_JSON"
fi

# Inferir Condition si no fue provista explícitamente
if [[ -z "$CONDITION" ]]; then
  if [[ "$RUN_NAME" =~ warmup ]]; then
    CONDITION="warmup"
  elif [[ "$RUN_NAME" =~ make-layouts|make_layouts ]]; then
    CONDITION="setup"
  elif [[ "$RUN_NAME" =~ compare-outputs|compare_outputs|count-layers|audit-layers ]]; then
    CONDITION="utility"
  elif [[ "$RUN_NAME" =~ -([A-Za-z]+)[0-9]*$ ]]; then
    matched="${BASH_REMATCH[1]}"
    if [[ "$matched" =~ ^(layouts|outputs|layers)$ ]]; then
      CONDITION="utility"
    else
      CONDITION="$matched"
    fi
  else
    CONDITION="default"
  fi
fi

echo "========================================================================"
echo "Lanzando Perf Job: ${RUN_NAME} [Git SHA: ${GIT_SHA}]"
echo "Aplicación ID   : ${EMR_APPLICATION_ID}"
echo "Perfil          : ${PROFILE} (Executors: ${SPARK_EXECUTOR_INSTANCES}, Disk: ${SPARK_EXECUTOR_DISK}, S3A Buffer: ${S3A_FAST_UPLOAD_BUFFER}, PartSize: ${S3A_MULTIPART_SIZE})"
echo "Script          : ${ENTRY_POINT}"
echo "Args            : ${JOB_ARGS}"
echo "Condición       : ${CONDITION}"
echo "========================================================================"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[submit_perf_job] Modo Dry-Run activo. Parámetros construidos con éxito."
  echo "SPARK_PARAMS: ${SPARK_PARAMS}"
  echo "ARGS_ARRAY  : ${ARGS_ARRAY}"
  exit 0
fi

JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --region "${AWS_REGION}" \
  --application-id "${EMR_APPLICATION_ID}" \
  --execution-role-arn "${EMR_EXECUTION_ROLE_ARN}" \
  --name "${RUN_NAME}" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"${ENTRY_POINT}\",
      \"entryPointArguments\": ${ARGS_ARRAY},
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
  --tags "{
    \"Project\": \"sunat-risk-scoring\",
    \"Experiment\": \"${EXP_TAG}\",
    \"Profile\": \"${PROFILE}\",
    \"GitSHA\": \"${GIT_SHA}\",
    \"Condition\": \"${CONDITION}\"
  }" \
  --query "jobRunId" \
  --output text)

echo "✓ Job Run iniciado: ${JOB_RUN_ID}"

# Monitoreo
MAX_POLL_FAILURES=5
poll_failures=0
while true; do
  if STATE=$(aws emr-serverless get-job-run \
    --region "${AWS_REGION}" \
    --application-id "${EMR_APPLICATION_ID}" \
    --job-run-id "${JOB_RUN_ID}" \
    --query "jobRun.state" \
    --output text); then
    poll_failures=0
  else
    STATE="UNKNOWN"
    poll_failures=$((poll_failures + 1))
    echo "WARN: get-job-run falló (${poll_failures}/${MAX_POLL_FAILURES})" >&2
    if [[ "$poll_failures" -ge "$MAX_POLL_FAILURES" ]]; then
      echo "ERROR: get-job-run falló ${MAX_POLL_FAILURES} veces seguidas; abortando monitoreo (job run ${JOB_RUN_ID} puede seguir activo)" >&2
      break
    fi
  fi

  echo "[$(date +'%H:%M:%S')] Estado: ${STATE}"

  if [[ "$STATE" == "SUCCESS" ]]; then
    break
  elif [[ "$STATE" == "FAILED" || "$STATE" == "CANCELLED" ]]; then
    echo "ERROR: El job run finalizó en estado ${STATE}" >&2
    break
  fi
  sleep 15
done

# Guardar JSON completo del run
EXP_OUT_DIR="${RESULTS_DIR}/${EXP_TAG}"
mkdir -p "${EXP_OUT_DIR}"
RUN_JSON_FILE="${EXP_OUT_DIR}/run_${JOB_RUN_ID}.json"

aws emr-serverless get-job-run \
  --region "${AWS_REGION}" \
  --application-id "${EMR_APPLICATION_ID}" \
  --job-run-id "${JOB_RUN_ID}" \
  > "${RUN_JSON_FILE}"

echo "✓ Detalle JSON guardado en: ${RUN_JSON_FILE}"

if [[ "$STATE" != "SUCCESS" ]]; then
  echo "❌ ERROR: El job run ${RUN_NAME} (${JOB_RUN_ID}) no finalizó exitosamente (Estado: ${STATE})." >&2
  exit 1
fi

