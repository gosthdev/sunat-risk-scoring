#!/usr/bin/env bash
# ==============================================================================
# scripts/check_stage.sh
#
# Determina si una etapa del pipeline de CD debe ejecutarse (idempotencia).
#
# Uso:
#   bash scripts/check_stage.sh \
#     --stage <nombre_etapa> \
#     --marker-uri <s3://bucket/_markers/...json> \
#     --files <archivo1> [archivo2 ...] \
#     [--upstream-marker <s3://bucket/_markers/...json>] ... \
#     [--force <true|false>] \
#     [--check-raw-data] \
#     [--raw-bucket <nombre_bucket>]
#
# Salida:
#   Imprime "true" si la etapa debe ejecutarse, "false" si se puede omitir.
#   Si $GITHUB_OUTPUT está definido, exporta 'needs_run=true|false'.
#   Todos los mensajes informativos se envían a stderr.
# ==============================================================================

set -euo pipefail

STAGE=""
MARKER_URI=""
FORCE="false"
CHECK_RAW_DATA="false"
RAW_BUCKET="${RAW_BUCKET:-sunat-risk-scoring-raw}"
FILES=()
UPSTREAM_MARKERS=()

# ------------------------------------------------------------------------------
# 1. Parseo de argumentos
# ------------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage)
      STAGE="$2"
      shift 2
      ;;
    --marker-uri)
      MARKER_URI="$2"
      shift 2
      ;;
    --force)
      FORCE="$2"
      shift 2
      ;;
    --check-raw-data)
      CHECK_RAW_DATA="true"
      shift 1
      ;;
    --raw-bucket)
      RAW_BUCKET="$2"
      shift 2
      ;;
    --upstream-marker)
      UPSTREAM_MARKERS+=("$2")
      shift 2
      ;;
    --files)
      shift
      while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
        FILES+=("$1")
        shift
      done
      ;;
    *)
      echo "WARN: Parámetro desconocido: $1" >&2
      shift
      ;;
  esac
done

emit_result() {
  local result="$1"
  local reason="$2"
  echo "INFO [check_stage:${STAGE:-unknown}]: Decisión=${result} (Razón: ${reason})" >&2
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "needs_run=${result}" >> "$GITHUB_OUTPUT"
  fi
  echo "${result}"
  exit 0
}

# ------------------------------------------------------------------------------
# 2. Verificación de banderas manuales
# ------------------------------------------------------------------------------
if [[ "${FORCE}" == "true" ]]; then
  emit_result "true" "Ejecución forzada manualmente (--force=true)"
fi

if [[ -z "${MARKER_URI}" ]]; then
  emit_result "true" "No se proveyó MARKER_URI; asumiendo ejecución requerida"
fi

# ------------------------------------------------------------------------------
# 3. Función auxiliar para calcular hash SHA256 de archivos y carpetas
# ------------------------------------------------------------------------------
compute_files_hash() {
  local target_files=("$@")
  python3 -c '
import sys, hashlib, os, glob

paths = sys.argv[1:]
resolved_files = set()
for p in paths:
    expanded = glob.glob(p) if any(c in p for c in ["*", "?", "["]) else [p]
    for item in expanded:
        if os.path.isfile(item):
            resolved_files.add(os.path.abspath(item))
        elif os.path.isdir(item):
            for root, _, filenames in os.walk(item):
                for f in filenames:
                    resolved_files.add(os.path.abspath(os.path.join(root, f)))

if not resolved_files:
    print("NO_FILES")
    sys.exit(0)

hasher = hashlib.sha256()
for fpath in sorted(resolved_files):
    try:
        with open(fpath, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
    except Exception as e:
        sys.stderr.write(f"Warning: could not read {fpath}: {e}\n")

print(hasher.hexdigest())
' "${target_files[@]}"
}

# ------------------------------------------------------------------------------
# 4. Caso específico: Stage Raw (Verificación de presencia física de crudos en S3)
# ------------------------------------------------------------------------------
if [[ "${CHECK_RAW_DATA}" == "true" || "${STAGE}" == "raw" ]]; then
  echo "INFO [check_stage:raw]: Verificando presencia de datasets crudos en s3://${RAW_BUCKET}/..." >&2

  # Verificar si existen archivos en padron_ruc/
  PADRON_EXISTS=$(aws s3 ls "s3://${RAW_BUCKET}/padron_ruc/" --recursive 2>/dev/null | grep -E '\.(csv|zip|gz)$' | head -n 1 || true)
  if [[ -z "$PADRON_EXISTS" ]]; then
    emit_result "true" "No se detectaron archivos crudos en s3://${RAW_BUCKET}/padron_ruc/"
  fi

  # Verificar si existen archivos en ordenes_compra/ o compras/
  ORDENES_EXISTS=$(aws s3 ls "s3://${RAW_BUCKET}/ordenes_compra/" --recursive 2>/dev/null | grep -E '\.(csv|zip|gz)$' | head -n 1 || true)
  if [[ -z "$ORDENES_EXISTS" ]]; then
    ORDENES_EXISTS=$(aws s3 ls "s3://${RAW_BUCKET}/compras/" --recursive 2>/dev/null | grep -E '\.(csv|zip|gz)$' | head -n 1 || true)
  fi
  if [[ -z "$ORDENES_EXISTS" ]]; then
    emit_result "true" "No se detectaron archivos crudos en s3://${RAW_BUCKET}/ordenes_compra/"
  fi
fi

# ------------------------------------------------------------------------------
# 5. Verificar existencia del marcador en S3
# ------------------------------------------------------------------------------
echo "INFO [check_stage:${STAGE}]: Leyendo marcador en ${MARKER_URI}..." >&2
if ! aws s3 ls "${MARKER_URI}" >/dev/null 2>&1; then
  emit_result "true" "Marcador ${MARKER_URI} no existe en S3 (primer run o re-inicialización)"
fi

MARKER_CONTENT=$(aws s3 cp "${MARKER_URI}" - 2>/dev/null || true)
if [[ -z "${MARKER_CONTENT}" ]]; then
  emit_result "true" "Marcador ${MARKER_URI} está vacío o no se pudo leer"
fi

# ------------------------------------------------------------------------------
# 6. Calcular hash actual del código local y comparar con el marcador
# ------------------------------------------------------------------------------
CURRENT_JOB_HASH=""
if [[ ${#FILES[@]} -gt 0 ]]; then
  CURRENT_JOB_HASH=$(compute_files_hash "${FILES[@]}")
  echo "INFO [check_stage:${STAGE}]: Hash local calculado=${CURRENT_JOB_HASH}" >&2
fi

CODE_CHECK_RESULT=$(python3 -c '
import sys, json

marker_str = sys.argv[1]
current_hash = sys.argv[2]
stage = sys.argv[3]

try:
    data = json.loads(marker_str)
except Exception as e:
    sys.stderr.write(f"Error parseando JSON del marcador: {e}\n")
    print("INVALID_JSON")
    sys.exit(0)

# El marcador puede tener job_hash, o urls_json_hash (para raw)
stored_hash = data.get("job_hash", "")
if not stored_hash and stage == "raw":
    stored_hash = data.get("urls_json_hash", "")

# Quitar prefijo sha256: si existe
if stored_hash.startswith("sha256:"):
    stored_hash = stored_hash[7:]

if current_hash and current_hash != "NO_FILES":
    if current_hash != stored_hash:
        sys.stderr.write(f"Hash mismatch: actual={current_hash} vs guardado={stored_hash}\n")
        print("CODE_CHANGED")
        sys.exit(0)

print("CODE_UNCHANGED")
' "${MARKER_CONTENT}" "${CURRENT_JOB_HASH}" "${STAGE}")

if [[ "${CODE_CHECK_RESULT}" == "INVALID_JSON" ]]; then
  emit_result "true" "El contenido del marcador S3 no es un JSON válido"
elif [[ "${CODE_CHECK_RESULT}" == "CODE_CHANGED" ]]; then
  emit_result "true" "El código fuente de la etapa cambió respecto al último run registrado"
fi

# ------------------------------------------------------------------------------
# 7. Verificar marcadores upstream (detectar si datos upstream cambiaron)
# ------------------------------------------------------------------------------
if [[ ${#UPSTREAM_MARKERS[@]} -gt 0 ]]; then
  for up_uri in "${UPSTREAM_MARKERS[@]}"; do
    echo "INFO [check_stage:${STAGE}]: Verificando marcador upstream ${up_uri}..." >&2

    if ! aws s3 ls "${up_uri}" >/dev/null 2>&1; then
      emit_result "true" "Marcador upstream ${up_uri} no existe"
    fi

    UPSTREAM_CONTENT=$(aws s3 cp "${up_uri}" - 2>/dev/null || true)
    if [[ -z "${UPSTREAM_CONTENT}" ]]; then
      emit_result "true" "Marcador upstream ${up_uri} está vacío"
    fi

    # Calcular hash actual del contenido del marcador upstream
    CURRENT_UPSTREAM_HASH=$(echo -n "${UPSTREAM_CONTENT}" | python3 -c '
import sys, hashlib
content = sys.stdin.read().encode("utf-8")
print(hashlib.sha256(content).hexdigest())
')

    UPSTREAM_CHECK_RESULT=$(python3 -c '
import sys, json

marker_str = sys.argv[1]
up_uri = sys.argv[2]
current_up_hash = sys.argv[3]

try:
    data = json.loads(marker_str)
except Exception:
    print("MISMATCH")
    sys.exit(0)

upstream_markers = data.get("upstream_markers", {})
stored_up_hash = upstream_markers.get(up_uri, "")

# Compatibilidad con claves legacy (ej. bronze_marker_hash, raw_marker_hash)
if not stored_up_hash:
    for k, v in data.items():
        if k.endswith("_hash") and k != "job_hash" and k != "urls_json_hash":
            # Si sólo hay un upstream legacy
            stored_up_hash = v
            break

if stored_up_hash.startswith("sha256:"):
    stored_up_hash = stored_up_hash[7:]

if current_up_hash != stored_up_hash:
    sys.stderr.write(f"Upstream hash mismatch para {up_uri}: actual={current_up_hash} vs guardado={stored_up_hash}\n")
    print("MISMATCH")
else:
    print("MATCH")
' "${MARKER_CONTENT}" "${up_uri}" "${CURRENT_UPSTREAM_HASH}")

    if [[ "${UPSTREAM_CHECK_RESULT}" == "MISMATCH" ]]; then
      emit_result "true" "El marcador upstream ${up_uri} cambió (los datos fuente se actualizaron)"
    fi
  done
fi

# ------------------------------------------------------------------------------
# 8. Todo coincide: omitir etapa
# ------------------------------------------------------------------------------
emit_result "false" "Código y dependencias upstream idénticos al último run exitoso"
