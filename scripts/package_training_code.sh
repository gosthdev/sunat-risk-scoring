#!/usr/bin/env bash
# Empaqueta el código de entrenamiento como sourcedir.tar.gz para SageMaker (script mode)
# y lo sube a s3://<gold-bucket>/code/sourcedir.tar.gz.
#
# Uso: bash scripts/package_training_code.sh <gold-bucket>
set -euo pipefail

GOLD_BUCKET="${1:?Uso: $0 <gold-bucket>}"
STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

cp src/models/train.py src/models/explainability.py src/models/export_outputs.py \
   src/models/orchestrator.py src/models/launch_manual.py "${STAGE}/"
cp config/experiments.yml "${STAGE}/experiments.yml"
cat > "${STAGE}/requirements.txt" <<'REQ'
pyarrow>=14.0.0
pyyaml>=6.0
s3fs>=2023.12.0
smart-open>=6.4.0
REQ

tar -czf "${STAGE}/sourcedir.tar.gz" -C "${STAGE}" train.py explainability.py export_outputs.py orchestrator.py launch_manual.py experiments.yml requirements.txt
aws s3 cp "${STAGE}/sourcedir.tar.gz" "s3://${GOLD_BUCKET}/code/sourcedir.tar.gz"
echo "Código de entrenamiento subido a s3://${GOLD_BUCKET}/code/sourcedir.tar.gz"
