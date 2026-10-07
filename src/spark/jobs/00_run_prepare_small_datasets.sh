#!/usr/bin/env bash
# Corre 00_prepare_small_datasets.py. NO se somete a EMR (ver docstring del
# script y README.md): necesita Python local con las dependencias de
# requirements.txt y credenciales AWS ya configuradas (aws configure / env
# vars / rol asumido), apuntando al mismo bucket que DATALAKE_BUCKET.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$SCRIPT_DIR/../common"

export PYTHONPATH="$COMMON_DIR:${PYTHONPATH:-}"

python3 "$SCRIPT_DIR/00_prepare_small_datasets.py"
