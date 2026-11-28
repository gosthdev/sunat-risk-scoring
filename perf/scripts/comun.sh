#!/usr/bin/env bash
# Valores y funciones comunes de los scripts de la prueba (se carga con `source`).

export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_PAGER=""

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RAW_PROD="sunat-risk-scoring-raw"
SILVER_PROD="sunat-risk-scoring-silver"
# Jobs, common.zip y logs de EMR de todas las corridas viven en el bucket del 1x.
ARTEFACTOS="sunat-risk-scoring-perf-x1"
PREFIJO_JOBS="jobs-perf"
APP_NOMBRE="sunat-ssco-perf-spark"
APP_NOMBRE_C1="sunat-ssco-perf-spark-c1"  # perfil C1: spark.executor.instances=1 por defecto
ROL_NOMBRE="sunat-ssco-perf-emr-role"
DATASETS_PEQUENOS=(pricos epen ingresos_tributarios ssco see)

PYTHON="$RAIZ/venv/Scripts/python.exe"
[[ -x "$PYTHON" ]] || PYTHON="$RAIZ/venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="python3"

# boto3 no lee las sesiones de `aws login`: se le pasan credenciales temporales de la CLI,
# solo dentro de este subproceso.
py_aws() {
  (eval "$(aws configure export-credentials --format env)" && "$PYTHON" "$@")
}

bucket_de() {
  local bucket="sunat-risk-scoring-perf-x$1"
  if [[ "$bucket" != *"-perf-"* ]] || ! aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    echo "ERROR: el bucket $bucket no existe (correr perf/scripts/infra_perf.sh crear)." >&2
    exit 1
  fi
  echo "$bucket"
}

app_id() {
  local nombre="${1:-$APP_NOMBRE}" id
  id="$(aws emr-serverless list-applications \
    --query "applications[?name=='${nombre}' && state!='TERMINATED'].id | [0]" --output text)"
  [[ -n "$id" && "$id" != "None" ]] || { echo "ERROR: no existe la app $nombre." >&2; exit 1; }
  echo "$id"
}

rol_arn() {
  echo "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/${ROL_NOMBRE}"
}

# Sube los jobs del pipeline, common.zip y los scripts de perf a s3://perf-x1/jobs-perf/.
subir_artefactos() {
  bash "$RAIZ/src/spark/jobs/package_jobs.sh" >/dev/null
  aws s3 cp "$RAIZ/src/spark/jobs/common.zip" "s3://${ARTEFACTOS}/${PREFIJO_JOBS}/common.zip" --only-show-errors
  aws s3 sync "$RAIZ/src/spark/jobs/" "s3://${ARTEFACTOS}/${PREFIJO_JOBS}/" \
    --exclude "*" --include "0*.py" --only-show-errors
  aws s3 sync "$RAIZ/perf/spark/" "s3://${ARTEFACTOS}/${PREFIJO_JOBS}/" \
    --exclude "*" --include "*.py" --only-show-errors
}
