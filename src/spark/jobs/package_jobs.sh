#!/usr/bin/env bash
# Empaqueta src/spark/common/ en common.zip antes de cada spark-submit.
# Los módulos quedan en la RAIZ del zip (no dentro de una carpeta común/)
# para poder importarlos directo en los jobs: `from s3_paths import ...`
# config/fuentes.json entra como fuentes/fuentes.json, legible desde el zip
# con importlib.resources.files("fuentes").

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$SCRIPT_DIR/../common"
REGISTRO="$SCRIPT_DIR/../../../config/fuentes.json"
OUTPUT_ZIP="$SCRIPT_DIR/common.zip"

rm -f "$OUTPUT_ZIP"
# Las rutas entran como argumentos, no interpoladas en el código Python.
python3 -c "
import sys
import zipfile
from pathlib import Path
common_dir = Path(sys.argv[1])
registro = Path(sys.argv[2])
output_zip = Path(sys.argv[3])
with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
    for file in sorted(common_dir.rglob('*')):
        if file.is_file() and '__pycache__' not in file.parts and not file.name.endswith('.pyc'):
            zf.write(file, arcname=file.relative_to(common_dir).as_posix())
    zf.write(registro, arcname='fuentes/fuentes.json')
" "$COMMON_DIR" "$REGISTRO" "$OUTPUT_ZIP"

echo "common.zip generado en: $OUTPUT_ZIP"
