#!/usr/bin/env bash
# ==============================================================================
# stage_from_real.sh
#
# Copia datos desde capas reales hacia buckets de pruebas.
# Soporta modo simulacro (--dry-run) y valida que el destino nunca sea
# un bucket real protegido.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/guard_protected_paths.sh"

REAL_SILVER_BUCKET="${REAL_SILVER_BUCKET:-sunat-risk-scoring-silver}"
DRY_RUN=false
SOURCE_PATH=""
TARGET_PATH=""
MODE=""
PERF_BUCKET=""

usage() {
  echo "Uso: $0 [opciones]"
  echo "Modos predefinidos:"
  echo "  --padron-to-staging <perf_bucket>    Copia silver/padron_ruc real a s3://<perf_bucket>/staging/padron_ruc/"
  echo "  --small-to-silver   <perf_bucket>    Copia silver/{pricos,epen,ingresos_tributarios} reales a s3://<perf_bucket>/silver/"
  echo "Modo genérico:"
  echo "  --source <s3://origen> --target <s3://destino>"
  echo "Opciones:"
  echo "  --dry-run                            Ejecuta en modo simulacro (sin transferir datos)"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --padron-to-staging)
      MODE="padron"
      PERF_BUCKET="$2"
      shift 2
      ;;
    --small-to-silver)
      MODE="small"
      PERF_BUCKET="$2"
      shift 2
      ;;
    --source)
      SOURCE_PATH="$2"
      shift 2
      ;;
    --target)
      TARGET_PATH="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

execute_copy() {
  local src="$1"
  local dst="$2"

  echo "------------------------------------------------------------------------"
  echo "Origen : $src"
  echo "Destino: $dst"

  # Validar que el destino esté estrictamente desprotegido
  assert_not_protected "$dst" "copia desde datos reales"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] Simulando transferencia con aws s3 sync..."
    aws s3 sync "$src" "$dst" --dryrun
  else
    echo "Copiando datos reales hacia bucket de pruebas..."
    aws s3 sync "$src" "$dst"
    echo "✓ Transferencia completada a: $dst"
  fi
}

if [[ "$MODE" == "padron" ]]; then
  if [[ -z "$PERF_BUCKET" ]]; then usage; fi
  src="s3://${REAL_SILVER_BUCKET}/padron_ruc/"
  dst="s3://${PERF_BUCKET}/staging/padron_ruc/"
  execute_copy "$src" "$dst"

elif [[ "$MODE" == "small" ]]; then
  if [[ -z "$PERF_BUCKET" ]]; then usage; fi
  for table in pricos epen ingresos_tributarios; do
    src="s3://${REAL_SILVER_BUCKET}/${table}/"
    dst="s3://${PERF_BUCKET}/silver/${table}/"
    execute_copy "$src" "$dst"
  done

elif [[ -n "$SOURCE_PATH" && -n "$TARGET_PATH" ]]; then
  execute_copy "$SOURCE_PATH" "$TARGET_PATH"

else
  usage
fi
