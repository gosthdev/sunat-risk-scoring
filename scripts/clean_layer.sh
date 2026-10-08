#!/usr/bin/env bash
# ==============================================================================
# scripts/clean_layer.sh
#
# Elimina los prefijos de S3 de una capa antes de re-procesar (idempotencia).
#
# Uso:
#   bash scripts/clean_layer.sh <capa>
#
# Capas soportadas:
#   - bronze          (elimina padron_ruc/ y ordenes_compra/; preserva quarantine/)
#   - silver          (elimina padron_ruc/ y ordenes_compra/; preserva small datasets)
#   - gold_ruc        (elimina ruc_features/)
#   - gold_regional   (elimina regional_summary/)
#   - gold_scoring    (elimina scoring_dataset/)
# ==============================================================================

set -euo pipefail

LAYER="${1:-}"
if [[ "$LAYER" =~ ^--layer=?(.*)$ ]]; then
  if [[ "$LAYER" == "--layer" ]]; then
    LAYER="${2:-}"
  else
    LAYER="${LAYER#--layer=}"
  fi
fi

if [[ -z "${LAYER}" ]]; then
  echo "Uso: $0 <bronze|silver|gold_ruc|gold_regional|gold_scoring>" >&2
  exit 1
fi

DATALAKE_BUCKET="${DATALAKE_BUCKET:-sunat-risk-scoring}"
RAW_BUCKET="${RAW_BUCKET:-sunat-risk-scoring-raw}"
BRONZE_BUCKET="${BRONZE_BUCKET:-sunat-risk-scoring-bronze}"
SILVER_BUCKET="${SILVER_BUCKET:-sunat-risk-scoring-silver}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"

IS_SINGLE_BUCKET=0
if [[ "$RAW_BUCKET" == "$BRONZE_BUCKET" && "$BRONZE_BUCKET" == "$SILVER_BUCKET" && "$SILVER_BUCKET" == "$GOLD_BUCKET" ]]; then
  IS_SINGLE_BUCKET=1
fi

TARGETS=()

case "${LAYER}" in
  bronze|job01)
    echo "INFO: Preparando limpieza de capa Bronze..."
    if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
      TARGETS+=(
        "s3://${DATALAKE_BUCKET}/bronze/padron_ruc/"
        "s3://${DATALAKE_BUCKET}/bronze/ordenes_compra/"
      )
    else
      TARGETS+=(
        "s3://${BRONZE_BUCKET}/padron_ruc/"
        "s3://${BRONZE_BUCKET}/ordenes_compra/"
      )
    fi
    ;;

  silver|job02)
    echo "INFO: Preparando limpieza de datasets de Job 02 en Silver..."
    if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
      TARGETS+=(
        "s3://${DATALAKE_BUCKET}/silver/padron_ruc/"
        "s3://${DATALAKE_BUCKET}/silver/ordenes_compra/"
        "s3://${DATALAKE_BUCKET}/silver/compras/"
      )
    else
      TARGETS+=(
        "s3://${SILVER_BUCKET}/padron_ruc/"
        "s3://${SILVER_BUCKET}/ordenes_compra/"
        "s3://${SILVER_BUCKET}/compras/"
      )
    fi
    ;;

  gold_ruc|gold-ruc|job03)
    echo "INFO: Preparando limpieza de Gold RUC Features..."
    if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
      TARGETS+=(
        "s3://${DATALAKE_BUCKET}/gold/ruc_features/"
      )
    else
      TARGETS+=(
        "s3://${GOLD_BUCKET}/ruc_features/"
      )
    fi
    ;;

  gold_regional|gold-regional|job04)
    echo "INFO: Preparando limpieza de Gold Regional Summary..."
    if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
      TARGETS+=(
        "s3://${DATALAKE_BUCKET}/gold/regional_summary/"
      )
    else
      TARGETS+=(
        "s3://${GOLD_BUCKET}/regional_summary/"
      )
    fi
    ;;

  gold_scoring|gold-scoring|scoring|job05)
    echo "INFO: Preparando limpieza de Gold Scoring Dataset..."
    if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
      TARGETS+=(
        "s3://${DATALAKE_BUCKET}/gold/scoring_dataset/"
      )
    else
      TARGETS+=(
        "s3://${GOLD_BUCKET}/scoring_dataset/"
      )
    fi
    ;;

  *)
    echo "ERROR: Capa desconocida '${LAYER}'." >&2
    echo "Capas válidas: bronze, silver, gold_ruc, gold_regional, gold_scoring" >&2
    exit 1
    ;;
esac

for path in "${TARGETS[@]}"; do
  echo "INFO: Eliminando prefijo ${path}..."
  aws s3 rm "${path}" --recursive 2>/dev/null || true
  echo "✓ Prefijo ${path} limpio."
done

echo "✓ Limpieza para '${LAYER}' completada exitosamente."
