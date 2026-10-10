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
COMMON_DIR="${REPO_ROOT}/src/spark/common"
OUTPUT_ZIP="${BUILD_DIR}/common.zip"
rm -f "${OUTPUT_ZIP}"

python3 -c "
import zipfile
from pathlib import Path

output_zip = Path('${OUTPUT_ZIP}')
with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
    # 1. Módulos comunes (src/spark/common)
    for file in Path('${COMMON_DIR}').rglob('*.py'):
        if '__pycache__' not in file.parts and not file.name.endswith('.pyc'):
            zf.write(file, arcname=file.name)
    # 2. Jobs de producción (src/spark/jobs)
    for file in Path('${REPO_ROOT}/src/spark/jobs').glob('*.py'):
        if '__pycache__' not in file.parts and not file.name.endswith('.pyc'):
            zf.write(file, arcname=file.name)
    # 3. Scripts de rendimiento (perf/spark)
    for file in Path('${PERF_DIR}/spark').glob('*.py'):
        if '__pycache__' not in file.parts and not file.name.endswith('.pyc'):
            zf.write(file, arcname=file.name)
    # 4. Scripts de análisis (perf/analysis)
    for file in Path('${PERF_DIR}/analysis').glob('*.py'):
        if '__pycache__' not in file.parts and not file.name.endswith('.pyc'):
            zf.write(file, arcname=file.name)
"

echo "[package_perf] Copiando scripts de jobs, perf y análisis a dist/..."
cp "${PERF_DIR}/spark/"*.py "${BUILD_DIR}/"
cp "${REPO_ROOT}/src/spark/jobs/"*.py "${BUILD_DIR}/"
cp "${PERF_DIR}/analysis/"*.py "${BUILD_DIR}/"

echo "✓ Empaquetado completado en: ${BUILD_DIR}"
ls -lh "${BUILD_DIR}"

if [[ "$UPLOAD" == "true" ]]; then
  echo "[package_perf] Subiendo paquetes a s3://${ARTIFACTS_BUCKET}/jobs/perf/..."
  aws s3 sync "${BUILD_DIR}/" "s3://${ARTIFACTS_BUCKET}/jobs/perf/"
  echo "✓ Artefactos subidos a S3"
fi
