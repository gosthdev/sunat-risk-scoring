#!/usr/bin/env bash
# ==============================================================================
# guard_protected_paths.sh
#
# Guarda de seguridad que impide operaciones de escritura, limpieza o
# sobreescritura sobre los buckets y rutas reales de producción.
# ==============================================================================

set -euo pipefail

# Lista canónica de buckets reales protegidos
PROTECTED_BUCKETS=(
  "sunat-risk-scoring-raw"
  "sunat-risk-scoring-bronze"
  "sunat-risk-scoring-silver"
  "sunat-risk-scoring-gold"
)

is_protected_target() {
  local target="$1"
  local clean_target
  # Remover prefijo s3:// y trailing slash
  clean_target="${target#s3://}"
  clean_target="${clean_target%%/*}"

  for protected in "${PROTECTED_BUCKETS[@]}"; do
    if [[ "$clean_target" == "$protected" || "$target" =~ (^|/)"$protected"(/|$) ]]; then
      return 0 # Es protegido
    fi
  done
  return 1 # No es protegido
}

assert_not_protected() {
  local target="$1"
  local operation="${2:-operación}"

  if is_protected_target "$target"; then
    echo "======================================================================" >&2
    echo "🚨 ERROR DE SEGURIDAD: Operación bloqueada por guarda de protección 🚨" >&2
    echo "Ruta protegida detectada: $target" >&2
    echo "Operación intentada     : $operation" >&2
    echo "Los buckets de producción NUNCA deben modificarse desde perf scripts." >&2
    echo "======================================================================" >&2
    return 1
  fi
  return 0
}

# Ejecución standalone si se invoca directamente
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  if [[ $# -eq 0 ]]; then
    echo "Uso: $0 [--check <ruta_o_bucket>]"
    exit 1
  fi

  case "$1" in
    --check)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: Debe especificar la ruta a validar." >&2
        exit 1
      fi
      if assert_not_protected "$2" "verificación manual"; then
        echo "✓ Ruta permitida (no protegida): $2"
        exit 0
      else
        exit 1
      fi
      ;;
    *)
      if assert_not_protected "$1" "verificación manual"; then
        echo "✓ Ruta permitida (no protegida): $1"
        exit 0
      else
        exit 1
      fi
      ;;
  esac
fi
