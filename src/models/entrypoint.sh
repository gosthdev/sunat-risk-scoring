#!/bin/bash
set -euo pipefail

echo "=========================================================="
echo "[INFO] Iniciando entrypoint de entrenamiento SSCO Base Model"
echo "=========================================================="

# Soporte para ejecución en contenedor de SageMaker o en local
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [ -f "/opt/ml/code/train.py" ]; then
    TRAIN_SCRIPT="/opt/ml/code/train.py"
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    TRAIN_SCRIPT="${SCRIPT_DIR}/train.py"
fi

echo "[INFO] Ejecutando: ${PYTHON_BIN} ${TRAIN_SCRIPT} $@"
exec "${PYTHON_BIN}" "${TRAIN_SCRIPT}" "$@"
