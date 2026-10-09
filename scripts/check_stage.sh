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
DATALAKE_BUCKET="${DATALAKE_BUCKET:-sunat-risk-scoring}"
RAW_BUCKET="${RAW_BUCKET:-sunat-risk-scoring-raw}"
BRONZE_BUCKET="${BRONZE_BUCKET:-sunat-risk-scoring-bronze}"
SILVER_BUCKET="${SILVER_BUCKET:-sunat-risk-scoring-silver}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
GLUE_DATABASE="${GLUE_DATABASE:-sunat_ssco}"
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
    --bronze-bucket)
      BRONZE_BUCKET="$2"
      shift 2
      ;;
    --silver-bucket)
      SILVER_BUCKET="$2"
      shift 2
      ;;
    --gold-bucket)
      GOLD_BUCKET="$2"
      shift 2
      ;;
    --artifacts-bucket)
      ARTIFACTS_BUCKET="$2"
      shift 2
      ;;
    --glue-database)
      GLUE_DATABASE="$2"
      shift 2
      ;;
    --datalake-bucket)
      DATALAKE_BUCKET="$2"
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

check_s3_prefix_has_data() {
  local bucket="$1"
  local prefix="$2"
  bucket="${bucket#s3://}"
  bucket="${bucket%%/*}"
  prefix="${prefix#/}"

  local first_key
  first_key=$(aws s3api list-objects-v2 \
    --bucket "$bucket" \
    --prefix "$prefix" \
    --max-items 1 \
    --query "Contents[0].Key" \
    --output text 2>/dev/null || true)

  if [[ -n "$first_key" && "$first_key" != "None" ]]; then
    return 0
  else
    return 1
  fi
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

  # Si los datos crudos existen físicamente pero no había marcador registrado,
  # registrarlo automáticamente y omitir la EC2 (los datos ya están en Raw).
  if ! aws s3 ls "${MARKER_URI}" >/dev/null 2>&1; then
    echo "INFO [check_stage:raw]: Datasets crudos presentes en S3 pero marcador ausente. Auto-sincronizando marcador..." >&2
    SCRIPT_BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -f "${SCRIPT_BASE_DIR}/write_marker.sh" ]]; then
      bash "${SCRIPT_BASE_DIR}/write_marker.sh" \
        --stage raw \
        --marker-uri "${MARKER_URI}" \
        --files "${FILES[@]}" 2>&1 >&2 || true
    fi
    emit_result "false" "Datasets crudos ya presentes físicamente en S3 (marcador auto-registrado)"
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
# 8. Verificación de presencia física de datos en S3 / Glue
# ------------------------------------------------------------------------------
IS_SINGLE_BUCKET=0
if [[ "$RAW_BUCKET" == "$BRONZE_BUCKET" && "$BRONZE_BUCKET" == "$SILVER_BUCKET" && "$SILVER_BUCKET" == "$GOLD_BUCKET" ]]; then
  IS_SINGLE_BUCKET=1
fi

get_layer_prefix() {
  local layer="$1"
  local subpath="$2"
  if [[ "$IS_SINGLE_BUCKET" -eq 1 ]]; then
    echo "${layer}/${subpath}/"
  else
    echo "${subpath}/"
  fi
}

case "${STAGE}" in
  bronze|job01)
    echo "INFO [check_stage:bronze]: Verificando presencia física de datos en Bronze..." >&2
    p_padron=$(get_layer_prefix "bronze" "padron_ruc")
    p_ordenes=$(get_layer_prefix "bronze" "ordenes_compra")
    b_target=$([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "$DATALAKE_BUCKET" || echo "$BRONZE_BUCKET")

    if ! check_s3_prefix_has_data "$b_target" "$p_padron"; then
      emit_result "true" "Datos físicos ausentes en Bronze (s3://${b_target}/${p_padron})"
    fi
    if ! check_s3_prefix_has_data "$b_target" "$p_ordenes"; then
      emit_result "true" "Datos físicos ausentes en Bronze (s3://${b_target}/${p_ordenes})"
    fi
    ;;

  small|job00)
    echo "INFO [check_stage:small]: Verificando presencia física de small datasets en Silver..." >&2
    s_target=$([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "$DATALAKE_BUCKET" || echo "$SILVER_BUCKET")
    for tbl in epen ingresos_tributarios pricos ssco; do
      p_tbl=$(get_layer_prefix "silver" "$tbl")
      if ! check_s3_prefix_has_data "$s_target" "$p_tbl"; then
        emit_result "true" "Datos físicos ausentes en Silver (s3://${s_target}/${p_tbl})"
      fi
    done
    ;;

  silver|job02)
    echo "INFO [check_stage:silver]: Verificando presencia física de datasets de Job 02 en Silver..." >&2
    s_target=$([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "$DATALAKE_BUCKET" || echo "$SILVER_BUCKET")
    p_padron=$(get_layer_prefix "silver" "padron_ruc")
    p_ordenes=$(get_layer_prefix "silver" "ordenes_compra")
    if ! check_s3_prefix_has_data "$s_target" "$p_padron"; then
      emit_result "true" "Datos físicos ausentes en Silver (s3://${s_target}/${p_padron})"
    fi
    if ! check_s3_prefix_has_data "$s_target" "$p_ordenes"; then
      emit_result "true" "Datos físicos ausentes en Silver (s3://${s_target}/${p_ordenes})"
    fi
    ;;

  gold_ruc|job03)
    echo "INFO [check_stage:gold_ruc]: Verificando presencia física en Gold (ruc_features)..." >&2
    g_target=$([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "$DATALAKE_BUCKET" || echo "$GOLD_BUCKET")
    p_ruc=$(get_layer_prefix "gold" "ruc_features")
    if ! check_s3_prefix_has_data "$g_target" "$p_ruc"; then
      emit_result "true" "Datos físicos ausentes en Gold (s3://${g_target}/${p_ruc})"
    fi
    ;;

  gold_regional|job04)
    echo "INFO [check_stage:gold_regional]: Verificando presencia física en Gold (regional_summary)..." >&2
    g_target=$([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "$DATALAKE_BUCKET" || echo "$GOLD_BUCKET")
    p_reg=$(get_layer_prefix "gold" "regional_summary")
    if ! check_s3_prefix_has_data "$g_target" "$p_reg"; then
      emit_result "true" "Datos físicos ausentes en Gold (s3://${g_target}/${p_reg})"
    fi
    ;;

  scoring|gold_scoring|job05)
    echo "INFO [check_stage:scoring]: Verificando presencia física en Gold (scoring_dataset)..." >&2
    g_target=$([[ "$IS_SINGLE_BUCKET" -eq 1 ]] && echo "$DATALAKE_BUCKET" || echo "$GOLD_BUCKET")
    p_score=$(get_layer_prefix "gold" "scoring_dataset")
    if ! check_s3_prefix_has_data "$g_target" "$p_score"; then
      emit_result "true" "Datos físicos ausentes en Gold (s3://${g_target}/${p_score})"
    fi
    ;;

  analytics)
    echo "INFO [check_stage:analytics]: Verificando tablas en catálogo AWS Glue (${GLUE_DATABASE})..." >&2
    table_count=$(aws glue get-tables --database-name "${GLUE_DATABASE}" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
    if [[ -z "$table_count" || "$table_count" == "None" || "$table_count" -lt 1 ]]; then
      table_count_alt=$(aws glue get-tables --database-name "ssco_catalog" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
      if [[ -z "$table_count_alt" || "$table_count_alt" == "None" || "$table_count_alt" -lt 1 ]]; then
        emit_result "true" "Catálogo Glue vacío (0 tablas registradas en ${GLUE_DATABASE})"
      fi
    fi
    ;;
esac

# ------------------------------------------------------------------------------
# 9. Todo coincide y datos físicos presentes: omitir etapa
# ------------------------------------------------------------------------------
emit_result "false" "Código, dependencias y datos físicos íntegros respecto al último run"

