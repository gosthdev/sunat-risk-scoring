#!/usr/bin/env bash
# Lanza UNA instancia EC2 efímera con mucha RAM que entrena toda la matriz de experimentos
# en paralelo, publica resultados y gráficos en S3 y se termina sola al acabar.
#
# Uso:
#   AWS_PROFILE=bigdata bash scripts/train_ec2.sh --tag ec2run1
#   bash scripts/train_ec2.sh --tag ec2run1 --experiments E1,E2 --instance-type r6i.4xlarge
#
# Seguimiento (sin SSH, el log se sube a S3 cada minuto):
#   aws s3 cp s3://<gold>/model_logs/tag=<tag>/train.log - | tail -50
set -euo pipefail

TAG=""
EXPERIMENTS="ALL"
INSTANCE_TYPE="r6i.4xlarge"   # 16 vCPU / 128 GiB
PARALLEL=3                    # experimentos a la vez (cada uno pica ~22 GiB; medido con muestras)
N_JOBS=1                      # procesos del GridSearchCV por experimento (cada uno copia X_train)
MAX_HOURS=12                  # tope de seguridad: la instancia se apaga sola
DATASET_VERSION="v1"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
ROLE_NAME="sunat-ssco-train-ec2-role"

while [ $# -gt 0 ]; do
  case "$1" in
    --tag) TAG="$2"; shift 2 ;;
    --experiments) EXPERIMENTS="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --parallel) PARALLEL="$2"; shift 2 ;;
    --n-jobs) N_JOBS="$2"; shift 2 ;;
    --max-hours) MAX_HOURS="$2"; shift 2 ;;
    --dataset-version) DATASET_VERSION="$2"; shift 2 ;;
    *) echo "Argumento desconocido: $1" >&2; exit 2 ;;
  esac
done
[ -n "${TAG}" ] || { echo "Falta --tag <nombre-de-la-corrida>" >&2; exit 2; }
export AWS_DEFAULT_REGION="${REGION}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

echo "[INFO] Cuenta: $(aws sts get-caller-identity --query Arn --output text)"

# 1. Código: se entrena exactamente lo commiteado en HEAD.
if [ -n "$(git status --porcelain src config)" ]; then
  echo "[ERROR] Hay cambios sin commitear en src/ o config/; haz commit antes de lanzar." >&2
  exit 1
fi
CODE_KEY="train_ec2/${TAG}/code.tgz"
git archive --format=tar.gz HEAD src config | aws s3 cp - "s3://${ARTIFACTS_BUCKET}/${CODE_KEY}" --only-show-errors
echo "[INFO] Código subido: s3://${ARTIFACTS_BUCKET}/${CODE_KEY} ($(git rev-parse --short HEAD))"

# 2. Rol de instancia (se crea una sola vez): S3 + SSM, nada más.
if ! aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "[INFO] Creando rol ${ROLE_NAME}"
  aws iam create-role --role-name "${ROLE_NAME}" --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name "${ROLE_NAME}" \
    --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name s3-gold-and-artifacts --policy-document "{
    \"Version\":\"2012-10-17\",
    \"Statement\":[
      {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\"],
       \"Resource\":[\"arn:aws:s3:::${GOLD_BUCKET}\",\"arn:aws:s3:::${GOLD_BUCKET}/*\"]},
      {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],
       \"Resource\":[\"arn:aws:s3:::${ARTIFACTS_BUCKET}/train_ec2/*\"]}]}"
fi
if ! aws iam get-instance-profile --instance-profile-name "${ROLE_NAME}" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "${ROLE_NAME}" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "${ROLE_NAME}" --role-name "${ROLE_NAME}"
  echo "[INFO] Esperando a que IAM propague el perfil de instancia..."
  sleep 15
fi

# 3. User data: instala, descarga el dataset (misma región: sin costo de salida),
#    entrena en paralelo, consolida y se apaga.
USER_DATA="$(mktemp)"
cat > "${USER_DATA}" <<EOF
#!/bin/bash
set -uxo pipefail
exec > >(tee /var/log/train.log) 2>&1
export AWS_DEFAULT_REGION=${REGION} PYTHONUNBUFFERED=1
TAG=${TAG}; GOLD=${GOLD_BUCKET}; LOGS="s3://\${GOLD}/model_logs/tag=\${TAG}"

# Sube el log cada minuto para poder seguir el avance sin SSH.
( while true; do aws s3 cp /var/log/train.log "\${LOGS}/train.log" --only-show-errors; sleep 60; done ) &

# Tope de seguridad: se apaga (y se termina) aunque algo se cuelgue.
shutdown -P +$((MAX_HOURS * 60))

dnf install -y python3.11 python3.11-pip
python3.11 -m venv /opt/venv
mkdir -p /opt/app /data/input /data/out
aws s3 cp "s3://${ARTIFACTS_BUCKET}/${CODE_KEY}" - | tar xz -C /opt/app
/opt/venv/bin/pip install -q -r /opt/app/src/models/requirements.txt
/opt/venv/bin/python -c "import pandas, pyarrow, numpy, sklearn; print('pandas', pandas.__version__, 'pyarrow', pyarrow.__version__, 'sklearn', sklearn.__version__)"

aws s3 cp "s3://\${GOLD}/model_inputs/dataset_version=${DATASET_VERSION}/" /data/input/ --recursive --exclude "*" --include "*.parquet"
du -sh /data/input; free -g

cd /opt/app
if [ "${EXPERIMENTS}" = "ALL" ]; then
  IDS=\$(grep -oP '^\s+- id: "\K[^"]+' config/experiments.yml)
else
  IDS=\$(echo "${EXPERIMENTS}" | tr ',' ' ')
fi

run_one() {
  /opt/venv/bin/python -m src.models.orchestrator --config config/experiments.yml \\
    --experiments "\$1" --target runner --train_dir /data/input --output_dir "/data/out/\$1" \\
    --dataset_version ${DATASET_VERSION} --n_jobs ${N_JOBS} --run_tag "\${TAG}" \\
    --gold_bucket "\${GOLD}" --publicar_s3 si --publicar_leaderboard no --graficos si --reanudar si \\
    > "/var/log/exp_\$1.log" 2>&1
  echo "[DONE] \$1 exit=\$?"
  aws s3 cp "/var/log/exp_\$1.log" "\${LOGS}/\$1.log" --only-show-errors
}
export -f run_one
export TAG GOLD LOGS
echo \$IDS | tr ' ' '\n' | xargs -P ${PARALLEL} -I{} bash -c 'run_one {}'

# Leaderboard final con lo publicado en S3.
/opt/venv/bin/python -m src.models.orchestrator --config config/experiments.yml \\
  --experiments "\$(echo \$IDS | tr ' ' ',')" --target consolidar --dataset_version ${DATASET_VERSION} \\
  --run_tag "\${TAG}" --gold_bucket "\${GOLD}" --output_dir /data/out/leaderboard || true

aws s3 cp /var/log/train.log "\${LOGS}/train.log" --only-show-errors
shutdown -h now
EOF

# 4. Lanzar (al apagarse, la instancia se termina sola).
AMI="$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)"

IID="$(aws ec2 run-instances \
  --image-id "${AMI}" --instance-type "${INSTANCE_TYPE}" \
  --iam-instance-profile "Name=${ROLE_NAME}" \
  --instance-initiated-shutdown-behavior terminate \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true}' \
  --metadata-options 'HttpTokens=required' \
  --user-data "file://${USER_DATA}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ssco-train-${TAG}}]" \
  --query 'Instances[0].InstanceId' --output text)"
rm -f "${USER_DATA}"

echo "[OK] Instancia ${IID} (${INSTANCE_TYPE}) lanzada. Termina sola al acabar."
echo "     Log:  aws s3 cp s3://${GOLD_BUCKET}/model_logs/tag=${TAG}/train.log - | tail -50"
echo "     Final: s3://${GOLD_BUCKET}/model_leaderboards/tag=${TAG}/"
echo "     Cortar: aws ec2 terminate-instances --instance-ids ${IID}"
