#!/usr/bin/env bash
# Lanza UNA instancia EC2 efímera con mucha RAM que entrena toda la matriz de experimentos
# en paralelo, publica resultados y gráficos en S3 y se termina sola al acabar.
#
# Este script NO gestiona permisos: el rol y el perfil de instancia (sunat-ssco-train-ec2-role)
# y los permisos del rol de GitHub se definen en Terraform (infra/services) y se aplican con
# `terraform apply`. Aquí solo se aprovisionan los recursos de EC2 que falten (security group).
#
# Lo que corre dentro de la instancia está en scripts/train_ec2_bootstrap.sh.
#
# Uso:
#   AWS_PROFILE=bigdata bash scripts/train_ec2.sh --tag ec2run1
#   bash scripts/train_ec2.sh --tag ec2run1 --experiments E1,E2 --instance-type r6i.4xlarge
#
# Seguimiento (sin SSH, los logs se suben a S3 cada minuto):
#   aws s3 cp s3://<gold>/model_logs/tag=<tag>/train.log - | tail -50
#   aws s3 cp s3://<gold>/model_logs/tag=<tag>/E1.log - | tail -20
set -euo pipefail

TAG=""
EXPERIMENTS="ALL"
INSTANCE_TYPE="r6i.8xlarge"   # 32 vCPU / 256 GiB (la cuota de la cuenta es de 32 vCPU)
PARALLEL=5                    # experimentos a la vez (cada uno pica ~22 GiB; medido con muestras)
N_JOBS=3                      # procesos del GridSearchCV por experimento (cada uno copia X_train)
MAX_HOURS=12                  # tope de seguridad: la instancia se apaga sola
DATASET_VERSION="v1"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
GOLD_BUCKET="${GOLD_BUCKET:-sunat-risk-scoring-gold}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-sunat-risk-scoring-artifacts}"
INSTANCE_PROFILE="sunat-ssco-train-ec2-role"   # creado por Terraform
SG_NAME="sunat-ssco-train-ec2-sg"

fail() { echo "[ERROR] $*" >&2; exit 1; }

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
[[ "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]] || fail "--tag solo admite letras, números, punto, guion y guion bajo."
[[ "${PARALLEL}" =~ ^[0-9]+$ && "${N_JOBS}" =~ ^[0-9]+$ && "${MAX_HOURS}" =~ ^[0-9]+$ ]] || fail "--parallel, --n-jobs y --max-hours deben ser enteros."
[ "${PARALLEL}" -ge 1 ] && [ "${N_JOBS}" -ge 1 ] && [ "${MAX_HOURS}" -ge 1 ] || fail "--parallel, --n-jobs y --max-hours deben ser >= 1."
export AWS_DEFAULT_REGION="${REGION}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
BOOTSTRAP="${ROOT}/scripts/train_ec2_bootstrap.sh"

echo "[INFO] Cuenta: $(aws sts get-caller-identity --query Arn --output text)"

# 0. Verificaciones previas: es mejor fallar aquí que con una instancia cara encendida.
if [ "${EXPERIMENTS}" != "ALL" ]; then
  for id in ${EXPERIMENTS//,/ }; do
    grep -q "^[[:space:]]*- id: \"${id}\"" config/experiments.yml || fail "El experimento ${id} no existe en config/experiments.yml."
  done
fi
DATASET_URI="s3://${GOLD_BUCKET}/model_inputs/dataset_version=${DATASET_VERSION}/"
N_PARQUET="$(aws s3 ls "${DATASET_URI}" --recursive | grep -c '\.parquet$' || true)"
[ "${N_PARQUET}" -ge 1 ] || fail "No hay archivos .parquet en ${DATASET_URI} (corre el Job 06 antes de entrenar)."
echo "[INFO] Dataset: ${N_PARQUET} archivos .parquet en ${DATASET_URI}"

# 1. Código: se entrena exactamente lo commiteado en HEAD.
if [ -n "$(git status --porcelain src config scripts/train_ec2_bootstrap.sh)" ]; then
  fail "Hay cambios sin commitear en src/, config/ o el bootstrap; haz commit antes de lanzar."
fi
CODE_KEY="train_ec2/${TAG}/code.tgz"
git archive --format=tar.gz HEAD src config | aws s3 cp - "s3://${ARTIFACTS_BUCKET}/${CODE_KEY}" --only-show-errors
echo "[INFO] Código subido: s3://${ARTIFACTS_BUCKET}/${CODE_KEY} ($(git rev-parse --short HEAD))"

# 2. Security group: sin puertos de entrada, salida libre (la instancia solo habla con S3 y pip).
#    Es el único recurso de EC2 que se aprovisiona aquí si falta.
VPC_ID=""
SG_ID=""
ensure_security_group() {
  VPC_ID="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
  [ "${VPC_ID}" != "None" ] || fail "No hay VPC por defecto en ${REGION}."
  SG_ID="$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
    --query 'SecurityGroups[0].GroupId' --output text)"
  if [ "${SG_ID}" = "None" ]; then
    echo "[INFO] Creando security group ${SG_NAME} en ${VPC_ID}"
    SG_ID="$(aws ec2 create-security-group --group-name "${SG_NAME}" --vpc-id "${VPC_ID}" \
      --description "Entrenamiento SSCO: sin entrada, salida libre" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=${SG_NAME}}]" \
      --query GroupId --output text)"
  fi
}
ensure_security_group

# 3. User data = variables de configuración + bootstrap. Se valida antes de lanzar.
USER_DATA="$(mktemp)"
ERR_FILE="$(mktemp)"
trap 'rm -f "${USER_DATA}" "${ERR_FILE}"' EXIT
{
  echo '#!/bin/bash'
  for v in TAG GOLD_BUCKET ARTIFACTS_BUCKET CODE_KEY DATASET_VERSION EXPERIMENTS PARALLEL N_JOBS MAX_HOURS REGION; do
    printf 'export %s=%q\n' "${v}" "${!v}"
  done
  tail -n +2 "${BOOTSTRAP}"
} > "${USER_DATA}"
bash -n "${USER_DATA}" || fail "El user-data generado tiene errores de sintaxis."
[ "$(wc -c < "${USER_DATA}")" -lt 16000 ] || fail "El user-data supera el límite de 16 KB de EC2."

# 4. Lanzar (al apagarse, la instancia se termina sola). Si una zona no tiene capacidad se
#    prueba la siguiente; si el perfil de instancia acaba de crearse, se espera a que propague.
AMI="$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)"
SUBNETS="$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${VPC_ID}" "Name=default-for-az,Values=true" \
  --query 'Subnets[].SubnetId' --output text)"
[ -n "${SUBNETS}" ] || fail "La VPC ${VPC_ID} no tiene subnets por defecto."

IID=""
for SUBNET in ${SUBNETS}; do
  for attempt in 1 2 3 4 5 6; do
    if IID="$(aws ec2 run-instances \
      --image-id "${AMI}" --instance-type "${INSTANCE_TYPE}" \
      --iam-instance-profile "Name=${INSTANCE_PROFILE}" \
      --subnet-id "${SUBNET}" --security-group-ids "${SG_ID}" \
      --instance-initiated-shutdown-behavior terminate \
      --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=100,VolumeType=gp3,DeleteOnTermination=true}' \
      --metadata-options 'HttpTokens=required' \
      --user-data "file://${USER_DATA}" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ssco-train-${TAG}}]" \
      --query 'Instances[0].InstanceId' --output text 2>"${ERR_FILE}")"; then
      break 2
    fi
    IID=""
    if grep -qi "instance profile" "${ERR_FILE}"; then
      echo "[WARN] Perfil de instancia aún no disponible (intento ${attempt}/6); espero 10 s..."
      sleep 10
      continue
    fi
    if grep -qiE "InsufficientInstanceCapacity|Unsupported" "${ERR_FILE}"; then
      echo "[WARN] Sin capacidad de ${INSTANCE_TYPE} en ${SUBNET}; pruebo otra zona."
      break
    fi
    cat "${ERR_FILE}" >&2
    fail "run-instances falló (revisa permisos y cuota de vCPU)."
  done
done
if [ -z "${IID}" ]; then
  cat "${ERR_FILE}" >&2
  if grep -qi "instance profile" "${ERR_FILE}"; then
    fail "Falta el perfil de instancia ${INSTANCE_PROFILE}: aplica Terraform en infra/services (módulo iam_train_ec2_role)."
  fi
  fail "No se pudo lanzar ${INSTANCE_TYPE} en ninguna zona."
fi

echo "[OK] Instancia ${IID} (${INSTANCE_TYPE}) lanzada. Termina sola al acabar."
echo "     Log:   aws s3 cp s3://${GOLD_BUCKET}/model_logs/tag=${TAG}/train.log - | tail -50"
echo "     Final: s3://${GOLD_BUCKET}/model_leaderboards/tag=${TAG}/"
echo "     Cortar: aws ec2 terminate-instances --instance-ids ${IID}"
