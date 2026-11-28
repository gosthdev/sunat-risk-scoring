#!/usr/bin/env bash
# Infraestructura aislada de la prueba de escalabilidad 3.7.
#
# Uso:
#   perf/scripts/infra_perf.sh crear            # idempotente: crea solo lo que falta
#   perf/scripts/infra_perf.sh estado
#   perf/scripts/infra_perf.sh destruir --yes   # borra datos, buckets, app y rol perf
#
# Crea:
#   - Buckets sunat-risk-scoring-perf-x1/x5/x10/x100: acceso público bloqueado y
#     vencimiento automático (respaldo por si se olvida borrar una réplica).
#   - App EMR Serverless sunat-ssco-perf-spark: mismo release y capacidad máxima
#     que la app productiva, para que la medición sea comparable (perfil Base).
#   - App sunat-ssco-perf-spark-c1 (perfil C1, "una sola máquina"): igual, pero con
#     spark.executor.instances=1. EMR Serverless arranca 3 executors por defecto y
#     rechaza un máximo de 1; el script de envío del pipeline no permite pasar esa
#     opción, así que va como valor por defecto de esta app.
#   - Rol sunat-ssco-perf-emr-role: solo LEE el RAW productivo y solo ESCRIBE en
#     buckets *-perf-*. No tiene acceso a bronze/silver/gold/artifacts productivos,
#     así que un error de configuración no puede dañar producción.
#
# Los artefactos (jobs, common.zip) y los logs de EMR van a perf-x1
# (ARTIFACTS_BUCKET=sunat-risk-scoring-perf-x1 al correr el pipeline).

set -euo pipefail

export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_PAGER=""

PREFIJO="sunat-risk-scoring-perf"
RAW_PROD="sunat-risk-scoring-raw"
APP_NOMBRE="sunat-ssco-perf-spark"
APP_NOMBRE_C1="sunat-ssco-perf-spark-c1"
ROL_NOMBRE="sunat-ssco-perf-emr-role"
RELEASE="emr-7.13.0"
TAGS_APP="Project=sunat-risk-scoring,Uso=perf-3.7"
# Igual a emr_max_cpu/memory/disk de producción (infra/services): 16 executors + driver.
CAPACIDAD='{"cpu":"68 vCPU","memory":"272 GB","disk":"500 GB"}'

# bucket:días de vencimiento. x1 se conserva hasta el final de la prueba.
BUCKETS=("${PREFIJO}-x1:30" "${PREFIJO}-x5:14" "${PREFIJO}-x10:14" "${PREFIJO}-x100:14")

exigir_perf() {
  if [[ "$1" != *"-perf-"* ]]; then
    echo "SEGURIDAD: '$1' no contiene '-perf-'. Abortando." >&2
    exit 1
  fi
}

app_id() {
  aws emr-serverless list-applications \
    --query "applications[?name=='${1:-$APP_NOMBRE}' && state!='TERMINATED'].id | [0]" \
    --output text
}

crear_buckets() {
  local entrada bucket dias
  for entrada in "${BUCKETS[@]}"; do
    bucket="${entrada%%:*}"
    dias="${entrada##*:}"
    exigir_perf "$bucket"
    if aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      echo "[=] bucket $bucket ya existe"
    else
      aws s3api create-bucket --bucket "$bucket" >/dev/null
      echo "[+] bucket $bucket creado"
    fi
    aws s3api put-public-access-block --bucket "$bucket" \
      --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" \
      --lifecycle-configuration "{\"Rules\":[{\"ID\":\"vence-perf\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"\"},\"Expiration\":{\"Days\":${dias}},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}" >/dev/null
    aws s3api put-bucket-tagging --bucket "$bucket" \
      --tagging "TagSet=[{Key=Project,Value=sunat-risk-scoring},{Key=Uso,Value=perf-3.7}]"
  done
}

crear_rol() {
  local confianza politica
  confianza='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"emr-serverless.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  politica=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListarBuckets",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
      "Resource": ["arn:aws:s3:::${RAW_PROD}", "arn:aws:s3:::${PREFIJO}-*"]
    },
    {
      "Sid": "LeerRawProductivo",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::${RAW_PROD}/*"]
    },
    {
      "Sid": "LeerEscribirBucketsPerf",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": ["arn:aws:s3:::${PREFIJO}-*/*"]
    }
  ]
}
EOF
)
  if aws iam get-role --role-name "$ROL_NOMBRE" >/dev/null 2>&1; then
    echo "[=] rol $ROL_NOMBRE ya existe"
  else
    aws iam create-role --role-name "$ROL_NOMBRE" \
      --assume-role-policy-document "$confianza" \
      --tags Key=Project,Value=sunat-risk-scoring Key=Uso,Value=perf-3.7 >/dev/null
    echo "[+] rol $ROL_NOMBRE creado"
  fi
  # Se reescribe siempre: así un cambio de permisos en este script se aplica al volver a correrlo.
  aws iam put-role-policy --role-name "$ROL_NOMBRE" --policy-name perf-s3-access --policy-document "$politica"
}

crear_app() {
  local nombre="$1" id
  shift
  id="$(app_id "$nombre")"
  if [[ -n "$id" && "$id" != "None" ]]; then
    # Mantiene la capacidad al día; EMR solo la cambia con la app detenida.
    if [[ "$(aws emr-serverless get-application --application-id "$id" --query application.state --output text)" == "STOPPED" ]]; then
      aws emr-serverless update-application --application-id "$id" --maximum-capacity "$CAPACIDAD" >/dev/null
      echo "[=] app $nombre ya existe ($id); capacidad máxima $CAPACIDAD"
    else
      echo "[!] app $nombre ($id) encendida: no se actualizó su capacidad; repetir cuando se detenga (15 min sin uso)." >&2
    fi
    return
  fi
  id=$(aws emr-serverless create-application \
    --name "$nombre" --type SPARK --release-label "$RELEASE" --architecture X86_64 \
    --maximum-capacity "$CAPACIDAD" \
    --auto-start-configuration '{"enabled":true}' \
    --auto-stop-configuration '{"enabled":true,"idleTimeoutMinutes":15}' \
    --monitoring-configuration '{"managedPersistenceMonitoringConfiguration":{"enabled":true}}' \
    --tags "$TAGS_APP" "$@" \
    --query applicationId --output text)
  echo "[+] app $nombre creada ($id)"
}

estado() {
  local entrada bucket id cuenta
  cuenta="$(aws sts get-caller-identity --query Account --output text)"
  for entrada in "${BUCKETS[@]}"; do
    bucket="${entrada%%:*}"
    if aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      echo "bucket  $bucket  OK  vence: $(aws s3api get-bucket-lifecycle-configuration --bucket "$bucket" --query 'Rules[0].Expiration.Days' --output text) días"
    else
      echo "bucket  $bucket  NO EXISTE"
    fi
  done
  if aws iam get-role --role-name "$ROL_NOMBRE" >/dev/null 2>&1; then
    echo "rol     arn:aws:iam::${cuenta}:role/${ROL_NOMBRE}  OK"
  else
    echo "rol     $ROL_NOMBRE  NO EXISTE"
  fi
  local nombre
  for nombre in "$APP_NOMBRE" "$APP_NOMBRE_C1"; do
    id="$(app_id "$nombre")"
    if [[ -n "$id" && "$id" != "None" ]]; then
      echo "app     $nombre  $id  $(aws emr-serverless get-application --application-id "$id" --query 'application.[state,releaseLabel]' --output text)"
    else
      echo "app     $nombre  NO EXISTE"
    fi
  done
  echo "rol del pipeline: arn:aws:iam::${cuenta}:role/${ROL_NOMBRE}; artefactos y logs en ${PREFIJO}-x1"
}

destruir() {
  local entrada bucket id estado_app nombre
  if [[ "${1:-}" != "--yes" ]]; then
    echo "Esto BORRA los datos y buckets perf, las apps y el rol perf. Repetir con --yes." >&2
    exit 1
  fi
  for nombre in "$APP_NOMBRE" "$APP_NOMBRE_C1"; do
  id="$(app_id "$nombre")"
  if [[ -n "$id" && "$id" != "None" ]]; then
    estado_app="$(aws emr-serverless get-application --application-id "$id" --query application.state --output text)"
    if [[ "$estado_app" == "STARTED" || "$estado_app" == "STARTING" ]]; then
      aws emr-serverless stop-application --application-id "$id"
      until [[ "$(aws emr-serverless get-application --application-id "$id" --query application.state --output text)" == "STOPPED" ]]; do sleep 10; done
    fi
    aws emr-serverless delete-application --application-id "$id"
    echo "[-] app $nombre borrada ($id)"
  fi
  done
  if aws iam get-role --role-name "$ROL_NOMBRE" >/dev/null 2>&1; then
    aws iam delete-role-policy --role-name "$ROL_NOMBRE" --policy-name perf-s3-access 2>/dev/null || true
    aws iam delete-role --role-name "$ROL_NOMBRE"
    echo "[-] rol $ROL_NOMBRE borrado"
  fi
  for entrada in "${BUCKETS[@]}"; do
    bucket="${entrada%%:*}"
    exigir_perf "$bucket"
    if aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      aws s3 rm "s3://${bucket}/" --recursive --only-show-errors
      aws s3api delete-bucket --bucket "$bucket"
      echo "[-] bucket $bucket borrado"
    fi
  done
}

case "${1:-}" in
  crear)
    crear_buckets
    crear_rol
    crear_app "$APP_NOMBRE"
    crear_app "$APP_NOMBRE_C1" --runtime-configuration '[{"classification":"spark-defaults","properties":{"spark.executor.instances":"1"}}]'
    echo
    estado
    ;;
  estado) estado ;;
  destruir) destruir "${2:-}" ;;
  *) echo "Uso: $0 crear | estado | destruir --yes" >&2; exit 1 ;;
esac
