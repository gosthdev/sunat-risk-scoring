#!/usr/bin/env bash
# ==============================================================================
# scripts/write_marker.sh
#
# Escribe o actualiza el marcador de finalización exitosa de una etapa en S3.
#
# Uso:
#   bash scripts/write_marker.sh \
#     --stage <nombre_etapa> \
#     --marker-uri <s3://bucket/_markers/...json> \
#     --files <archivo1> [archivo2 ...] \
#     [--upstream-marker <s3://bucket/_markers/...json>] ...
# ==============================================================================

set -euo pipefail

STAGE=""
MARKER_URI=""
FILES=()
UPSTREAM_MARKERS=()

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

if [[ -z "${MARKER_URI}" ]]; then
  echo "ERROR: MARKER_URI es obligatorio para escribir el marcador." >&2
  exit 1
fi

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
                    resolved_files.add(os.path.abspath(item))

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

JOB_HASH=""
if [[ ${#FILES[@]} -gt 0 ]]; then
  JOB_HASH=$(compute_files_hash "${FILES[@]}")
fi

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Construir mapa de hashes de marcadores upstream
UPSTREAM_JSON="{}"
if [[ ${#UPSTREAM_MARKERS[@]} -gt 0 ]]; then
  TMP_UPSTREAMS=$(mktemp)
  echo "{}" > "$TMP_UPSTREAMS"
  for up_uri in "${UPSTREAM_MARKERS[@]}"; do
    if aws s3 ls "${up_uri}" >/dev/null 2>&1; then
      UP_CONTENT=$(aws s3 cp "${up_uri}" - 2>/dev/null || true)
      UP_HASH=$(echo -n "${UP_CONTENT}" | python3 -c '
import sys, hashlib
print(hashlib.sha256(sys.stdin.read().encode("utf-8")).hexdigest())
')
      python3 -c '
import sys, json
fpath, uri, h = sys.argv[1], sys.argv[2], sys.argv[3]
with open(fpath, "r") as f:
    d = json.load(f)
d[uri] = f"sha256:{h}"
with open(fpath, "w") as f:
    json.dump(d, f)
' "$TMP_UPSTREAMS" "$up_uri" "$UP_HASH"
    else
      echo "WARN: Marcador upstream ${up_uri} no encontrado al registrar marcador." >&2
    fi
  done
  UPSTREAM_JSON=$(cat "$TMP_UPSTREAMS")
  rm -f "$TMP_UPSTREAMS"
fi

# Generar archivo JSON del marcador
TMP_MARKER=$(mktemp)
python3 -c '
import sys, json

stage, job_hash, timestamp, marker_uri, up_json_str = sys.argv[1:6]
up_markers = json.loads(up_json_str)

payload = {
    "stage": stage,
    "timestamp": timestamp,
    "marker_uri": marker_uri,
    "job_hash": f"sha256:{job_hash}" if job_hash and job_hash != "NO_FILES" else "NO_FILES",
    "upstream_markers": up_markers
}

# Claves legacy útiles para scripts de compatibilidad
if stage == "raw":
    payload["urls_json_hash"] = payload["job_hash"]

with open(sys.argv[6], "w") as f:
    json.dump(payload, f, indent=2)
' "${STAGE}" "${JOB_HASH}" "${TIMESTAMP}" "${MARKER_URI}" "${UPSTREAM_JSON}" "${TMP_MARKER}"

echo "INFO: Subiendo marcador a ${MARKER_URI}..." >&2
aws s3 cp "${TMP_MARKER}" "${MARKER_URI}"
rm -f "${TMP_MARKER}"
echo "✓ Marcador registrado exitosamente en ${MARKER_URI}" >&2
