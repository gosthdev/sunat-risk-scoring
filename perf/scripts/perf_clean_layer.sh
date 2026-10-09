#!/usr/bin/env bash
# ==============================================================================
# perf_clean_layer.sh
#
# Limpieza segura de capas o prefijos en buckets de pruebas (perf-*).
# Incluye guarda obligatoria que aborta si el bucket es de producción.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/guard_protected_paths.sh"

BUCKET=""
LAYER=""
DRY_RUN=false
FORCE=false

usage() {
  echo "Uso: $0 --bucket <perf-bucket> --layer <capa_o_prefijo> [--dry-run] [--force]"
  echo "Ejemplo: $0 --bucket sunat-risk-scoring-perf-e2 --layer bronze"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket|-b)
      BUCKET="$2"
      shift 2
      ;;
    --layer|-l)
      LAYER="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --force|-f)
      FORCE=true
      shift
      ;;
    *)
      if [[ -z "$BUCKET" ]]; then
        BUCKET="$1"
      elif [[ -z "$LAYER" ]]; then
        LAYER="$1"
      else
        usage
      fi
      shift
      ;;
  esac
done

if [[ -z "$BUCKET" || -z "$LAYER" ]]; then
  usage
fi

TARGET_URI="s3://${BUCKET}/${LAYER}"

# 1. Validar contra guarda de protección
assert_not_protected "$TARGET_URI" "limpieza de capa"

echo "[perf_clean_layer] Objetivo validado: ${TARGET_URI}"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY-RUN] Simulación de limpieza sobre: ${TARGET_URI}"
  aws s3 ls "${TARGET_URI}/" || echo "(Prefijo actualmente vacío o no existe)"
  exit 0
fi

echo "[perf_clean_layer] Eliminando contenido de ${TARGET_URI}/..."
aws s3 rm "${TARGET_URI}/" --recursive
echo "✓ Capa limpiada exitosamente: ${TARGET_URI}"
