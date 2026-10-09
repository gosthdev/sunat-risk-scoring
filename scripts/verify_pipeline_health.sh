#!/usr/bin/env bash
# ==============================================================================
# scripts/verify_pipeline_health.sh
#
# Health check / Smoke test del pipeline de datos de SUNAT Risk Scoring.
# Audita el estado físico del Data Lake en AWS S3 y el Catálogo en AWS Glue:
#   1. Capa Silver: Existencia de las 6 tablas (padron_ruc, ordenes_compra,
#      epen, ingresos_tributarios, pricos, ssco).
#   2. Capa Gold: Existencia de las 3 tablas (ruc_features, regional_summary,
#      scoring_dataset).
#   3. Catálogo Glue: Base de datos y registro de al menos 5 tablas.
#   4. Athena: Existencia del workgroup configurado.
#
# Retorna:
#   0 si todas las verificaciones pasan.
#   1 si alguna tabla o dataset está ausente, con diagnóstico detallado.
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# 1. Configuración y resolución de variables
# ------------------------------------------------------------------------------
AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
DATALAKE_BUCKET="${DATALAKE_BUCKET:-sunat-risk-scoring}"
RAW_BUCKET="${RAW_BUCKET:-sunat-risk-scoring-raw}"
BRONZE_BUCKET="${BRONZE_BUCKET:-sunat-risk-scoring-bronze}"
SILVER_BUCKET="${SILVER_BUCKET:-sunat-risk-scoring-silver}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
GLUE_DATABASE="${GLUE_DATABASE:-sunat_ssco}"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-sunat-ssco}"

IS_SINGLE_BUCKET=0
if [[ "$RAW_BUCKET" == "$BRONZE_BUCKET" && "$BRONZE_BUCKET" == "$SILVER_BUCKET" && "$SILVER_BUCKET" == "$GOLD_BUCKET" ]]; then
  IS_SINGLE_BUCKET=1
fi

echo "================================================================================"
echo "AUDITORÍA DE SALUD DEL DATA LAKE Y PIPELINE (SMOKE TEST)"
echo "================================================================================"
echo "Región AWS        : ${AWS_REGION}"
echo "Modo Almacenamiento: $([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "Bucket Único (${DATALAKE_BUCKET})" || echo "Multicanasta")"
echo "Silver Bucket     : ${SILVER_BUCKET}"
echo "Gold Bucket       : ${GOLD_BUCKET}"
echo "Glue Database     : ${GLUE_DATABASE}"
echo "Athena Workgroup  : ${ATHENA_WORKGROUP}"
echo "================================================================================"

ERRORS_FOUND=0

check_s3_prefix() {
  local bucket="$1"
  local prefix="$2"
  local description="$3"

  local first_key
  first_key=$(aws s3api list-objects-v2 \
    --region "${AWS_REGION}" \
    --bucket "${bucket}" \
    --prefix "${prefix}" \
    --max-items 1 \
    --query "Contents[0].Key" \
    --output text 2>/dev/null || true)

  if [[ -n "$first_key" && "$first_key" != "None" ]]; then
    echo "  [OK] ${description}: s3://${bucket}/${prefix} (Encontrado: ${first_key})"
    return 0
  else
    echo "  [FAIL] ${description}: s3://${bucket}/${prefix} ESTÁ VACÍO O NO EXISTE" >&2
    ERRORS_FOUND=$((ERRORS_FOUND + 1))
    return 1
  fi
}

# ------------------------------------------------------------------------------
# 2. Auditoría Capa Silver (6 datasets requeridos)
# ------------------------------------------------------------------------------
echo ""
echo ">> [1/4] Auditando Capa Silver..."

SILVER_TABLES=(
  "padron_ruc"
  "ordenes_compra"
  "epen"
  "ingresos_tributarios"
  "pricos"
  "ssco"
)

for table in "${SILVER_TABLES[@]}"; do
  if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
    target_prefix="silver/${table}/"
    check_s3_prefix "${DATALAKE_BUCKET}" "${target_prefix}" "Silver [${table}]" || true
  else
    target_prefix="${table}/"
    check_s3_prefix "${SILVER_BUCKET}" "${target_prefix}" "Silver [${table}]" || true
  fi
done

# ------------------------------------------------------------------------------
# 3. Auditoría Capa Gold (3 datasets requeridos)
# ------------------------------------------------------------------------------
echo ""
echo ">> [2/4] Auditando Capa Gold..."

GOLD_TABLES=(
  "ruc_features"
  "regional_summary"
  "scoring_dataset"
)

for table in "${GOLD_TABLES[@]}"; do
  if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
    target_prefix="gold/${table}/"
    check_s3_prefix "${DATALAKE_BUCKET}" "${target_prefix}" "Gold [${table}]" || true
  else
    target_prefix="${table}/"
    check_s3_prefix "${GOLD_BUCKET}" "${target_prefix}" "Gold [${table}]" || true
  fi
done

# ------------------------------------------------------------------------------
# 4. Auditoría Catálogo AWS Glue
# ------------------------------------------------------------------------------
echo ""
echo ">> [3/4] Auditando Catálogo AWS Glue (${GLUE_DATABASE})..."

if ! aws glue get-database --region "${AWS_REGION}" --name "${GLUE_DATABASE}" >/dev/null 2>&1; then
  # Fallback a ssco_catalog si sunat_ssco no existe
  if aws glue get-database --region "${AWS_REGION}" --name "ssco_catalog" >/dev/null 2>&1; then
    GLUE_DATABASE="ssco_catalog"
    echo "  [INFO] Usando base de datos alternativa detectada: ssco_catalog"
  else
    echo "  [FAIL] La base de datos Glue '${GLUE_DATABASE}' no existe en AWS Glue!" >&2
    ERRORS_FOUND=$((ERRORS_FOUND + 1))
  fi
fi

GLUE_TABLES=$(aws glue get-tables \
  --region "${AWS_REGION}" \
  --database-name "${GLUE_DATABASE}" \
  --query 'TableList[].Name' \
  --output text 2>/dev/null || true)

TABLE_COUNT=0
if [[ -n "$GLUE_TABLES" && "$GLUE_TABLES" != "None" ]]; then
  TABLE_COUNT=$(echo "$GLUE_TABLES" | wc -w)
fi

echo "  Tablas registradas en ${GLUE_DATABASE}: ${TABLE_COUNT}"
if [[ "$TABLE_COUNT" -lt 5 ]]; then
  echo "  [FAIL] Se esperaban al menos 5 tablas registradas en Glue, pero se encontraron ${TABLE_COUNT}." >&2
  if [[ "$TABLE_COUNT" -gt 0 ]]; then
    echo "  Tablas encontradas: ${GLUE_TABLES}" >&2
  fi
  ERRORS_FOUND=$((ERRORS_FOUND + 1))
else
  echo "  [OK] Tablas catalogadas exitosamente: ${GLUE_TABLES}"
fi

# ------------------------------------------------------------------------------
# 5. Auditoría Athena Workgroup
# ------------------------------------------------------------------------------
echo ""
echo ">> [4/4] Auditando Athena Workgroup (${ATHENA_WORKGROUP})..."

if aws athena get-work-group --region "${AWS_REGION}" --work-group "${ATHENA_WORKGROUP}" >/dev/null 2>&1; then
  echo "  [OK] Workgroup '${ATHENA_WORKGROUP}' activo."
elif aws athena get-work-group --region "${AWS_REGION}" --work-group "ssco-workgroup" >/dev/null 2>&1; then
  echo "  [OK] Workgroup alternativo 'ssco-workgroup' activo."
else
  echo "  [FAIL] No se encontró el workgroup Athena '${ATHENA_WORKGROUP}' ni 'ssco-workgroup'." >&2
  ERRORS_FOUND=$((ERRORS_FOUND + 1))
fi

# ------------------------------------------------------------------------------
# 6. Diagnóstico y Resultado Final
# ------------------------------------------------------------------------------
echo ""
echo "================================================================================"
if [[ "$ERRORS_FOUND" -gt 0 ]]; then
  echo "RESUMEN: LA AUDITORÍA FALLÓ CON ${ERRORS_FOUND} INCONSISTENCIAS DETECTADAS." >&2
  echo "Por favor verifique los logs anteriores para corregir los recursos faltantes." >&2
  echo "================================================================================"
  exit 1
else
  echo "RESUMEN: TODAS LAS COMPROBACIONES DE SALUD PASARON EXITOSAMENTE."
  echo "Data Lake Silver, Gold y Catálogo Glue íntegros y listos para consumo analítico."
  echo "================================================================================"
  exit 0
fi
