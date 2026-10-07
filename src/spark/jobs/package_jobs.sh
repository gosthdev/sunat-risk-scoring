#!/usr/bin/env bash
# Empaqueta src/spark/common/ en common.zip antes de cada spark-submit.
# Los módulos quedan en la RAIZ del zip (no dentro de una carpeta común/)
# para poder importarlos directo en los jobs: `from s3_paths import ...`

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$SCRIPT_DIR/../common"
OUTPUT_ZIP="$SCRIPT_DIR/common.zip"

rm -f "$OUTPUT_ZIP"
if command -v zip >/dev/null 2>&1; then
  (cd "$COMMON_DIR" && zip -r "$OUTPUT_ZIP" . -x "__pycache__/*" "*.pyc")
else
  python3 -c "
import zipfile
from pathlib import Path
common_dir = Path('${COMMON_DIR}')
output_zip = Path('${OUTPUT_ZIP}')
with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
    for file in common_dir.rglob('*'):
        if file.is_file() and '__pycache__' not in file.parts and not file.name.endswith('.pyc'):
            zf.write(file, arcname=file.relative_to(common_dir))
"
fi

echo "common.zip generado en: $OUTPUT_ZIP"
