#!/usr/bin/env bash
# Fase 3 - Crea el Athena Workgroup (se corre una sola vez; es idempotente).
#
# Uso:
#   BUCKET=mi-bucket ./07_create_athena_workgroup.sh
set -euo pipefail

RESULTS_BUCKET="${BUCKET:-${ARTIFACTS_BUCKET:-}}"
: "${RESULTS_BUCKET:?Define BUCKET o ARTIFACTS_BUCKET (bucket para almacenar resultados de Athena)}"
if [[ -z "${ATHENA_WORKGROUP:-}" ]]; then
  if aws athena get-work-group --work-group "sunat-ssco" >/dev/null 2>&1; then
    ATHENA_WORKGROUP="sunat-ssco"
  else
    ATHENA_WORKGROUP="ssco-workgroup"
  fi
fi
# Los resultados van fuera de las zonas raw/bronze/silver/gold.
RESULTS_LOCATION="s3://${RESULTS_BUCKET}/athena-results/"
# Tope de seguridad por consulta: 10 GB escaneados (evita sustos de costo).
BYTES_CUTOFF=10737418240

if aws athena get-work-group --work-group "$ATHENA_WORKGROUP" >/dev/null 2>&1; then
  echo "El workgroup $ATHENA_WORKGROUP ya existe, no se hace nada."
  exit 0
fi

aws athena create-work-group \
  --name "$ATHENA_WORKGROUP" \
  --description "Consultas del proyecto SSCO sobre el data lake (silver/gold)" \
  --configuration "ResultConfiguration={OutputLocation=${RESULTS_LOCATION}},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=${BYTES_CUTOFF},EngineVersion={SelectedEngineVersion='Athena engine version 3'}"

echo "Workgroup creado: $ATHENA_WORKGROUP (resultados en $RESULTS_LOCATION)"
