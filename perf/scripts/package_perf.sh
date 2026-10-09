#!/usr/bin/env bash
# ==============================================================================
# package_perf.sh
#
# Empaqueta los módulos comunes y scripts de benchmark para EMR Serverless.
# ==============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PERF_DIR="${REPO_ROOT}/perf"
BUILD_DIR="${PERF_DIR}/dist"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
UPLOAD=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upload)
      UPLOAD=true
      shift
      ;;
    *)
      shift
      ;;
  esac
done

mkdir -p "${BUILD_DIR}"

echo "[package_perf] Empaquetando common.zip..."
(
  cd "${REPO_ROOT}/src/spark/common"
  zip -q -r "${BUILD_DIR}/common.zip" . -x "*.pyc" "__pycache__/*"
)

echo "[package_perf] Copiando scripts de jobs y perf a dist/..."
cp "${PERF_DIR}/spark/"*.py "${BUILD_DIR}/"
cp "${REPO_ROOT}/src/spark/jobs/"*.py "${BUILD_DIR}/"

echo "✓ Empaquetado completado en: ${BUILD_DIR}"
ls -lh "${BUILD_DIR}"

if [[ "$UPLOAD" == "true" ]]; then
  echo "[package_perf] Subiendo paquetes a s3://${ARTIFACTS_BUCKET}/jobs/perf/..."
  aws s3 sync "${BUILD_DIR}/" "s3://${ARTIFACTS_BUCKET}/jobs/perf/"
  echo "✓ Artefactos subidos a S3"
fi
