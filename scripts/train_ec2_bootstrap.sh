#!/bin/bash
# Se ejecuta DENTRO de la instancia EC2 de entrenamiento (como user-data, lanzado por
# scripts/train_ec2.sh, que antepone las variables de configuración).
#
# Variables obligatorias: TAG GOLD_BUCKET ARTIFACTS_BUCKET CODE_KEY
# Opcionales: REGION DATASET_VERSION EXPERIMENTS PARALLEL N_JOBS MAX_HOURS
# Solo para pruebas locales: SKIP_CODE SKIP_PROVISION NO_SHUTDOWN PYBIN APP_DIR WORKDIR LOG LOGDIR
#
# Garantías: ante cualquier fallo crítico sube el log a S3 y apaga la instancia (que se
# termina sola), para no dejar una máquina cara encendida sin hacer nada.
set -uo pipefail

# cloud-init ejecuta el user-data sin HOME; pip, matplotlib y joblib lo usan para cachés.
export HOME="${HOME:-/root}"

: "${TAG:?}" "${GOLD_BUCKET:?}" "${ARTIFACTS_BUCKET:?}" "${CODE_KEY:?}"
REGION="${REGION:-us-east-1}"
DATASET_VERSION="${DATASET_VERSION:-v1}"
EXPERIMENTS="${EXPERIMENTS:-ALL}"
PARALLEL="${PARALLEL:-5}"
N_JOBS="${N_JOBS:-3}"
MAX_HOURS="${MAX_HOURS:-12}"
APP_DIR="${APP_DIR:-/opt/app}"
VENV="${VENV:-/opt/venv}"
WORKDIR="${WORKDIR:-/data}"
LOG="${LOG:-/var/log/train.log}"
LOGDIR="${LOGDIR:-/var/log}"
LOGS_URI="s3://${GOLD_BUCKET}/model_logs/tag=${TAG}"

export AWS_DEFAULT_REGION="${REGION}" PYTHONUNBUFFERED=1
# 5 experimentos x (1 + n_jobs) procesos: sin límite, BLAS abriría un hilo por vCPU en cada uno.
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-2}" OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-2}" MKL_NUM_THREADS="${MKL_NUM_THREADS:-2}"

mkdir -p "$(dirname "${LOG}")" "${LOGDIR}"
exec > >(tee "${LOG}") 2>&1

upload_logs() {
  aws s3 cp "${LOG}" "${LOGS_URI}/train.log" --only-show-errors || true
  local f
  for f in "${LOGDIR}"/exp_*.log; do
    [ -f "${f}" ] || continue
    aws s3 cp "${f}" "${LOGS_URI}/$(basename "${f}" | sed 's/^exp_//')" --only-show-errors || true
  done
}

MON_PID=""
FINISHED=0
finish() {
  [ "${FINISHED}" = 1 ] && return
  FINISHED=1
  [ -n "${MON_PID}" ] && kill "${MON_PID}" 2>/dev/null
  echo "[BOOT] $(date +%T) fin del bootstrap"
  sleep 3   # deja que tee vacíe el log antes de subirlo
  upload_logs
  if [ -z "${NO_SHUTDOWN:-}" ]; then
    shutdown -h now
  fi
}
trap finish EXIT

die() {
  echo "[ERROR] $*"
  exit 1
}

retry() {
  local n="$1" i
  shift
  for i in $(seq 1 "${n}"); do
    "$@" && return 0
    echo "[WARN] intento ${i}/${n} fallido: $*"
    sleep 10
  done
  return 1
}

# Cada minuto: línea de uso de recursos y subida de logs (para seguir el avance sin SSH).
monitor() {
  while true; do
    echo "[MON] $(date +%T) $(free -g | awk '/^Mem/{printf "mem=%s/%sG", $3, $2}') load=$(cut -d' ' -f1 /proc/loadavg)/$(nproc)vcpu"
    upload_logs
    sleep 60
  done
}
monitor &
MON_PID=$!

echo "[BOOT] $(date +%T) tag=${TAG} experimentos=${EXPERIMENTS} paralelo=${PARALLEL} n_jobs=${N_JOBS}"

# Tope de seguridad: se apaga (y se termina) aunque algo se cuelgue.
if [ -z "${NO_SHUTDOWN:-}" ]; then
  shutdown -P +$((MAX_HOURS * 60))
fi

# 1. Código
if [ -z "${SKIP_CODE:-}" ]; then
  mkdir -p "${APP_DIR}"
  retry 3 bash -c "aws s3 cp 's3://${ARTIFACTS_BUCKET}/${CODE_KEY}' - | tar xz -C '${APP_DIR}'" || die "no se pudo descargar el código"
fi
[ -f "${APP_DIR}/config/experiments.yml" ] || die "falta ${APP_DIR}/config/experiments.yml"

# 2. Python y dependencias
if [ -z "${SKIP_PROVISION:-}" ]; then
  retry 3 dnf install -y python3.11 python3.11-pip || die "dnf install python3.11"
  if ! python3.11 -m venv "${VENV}"; then
    echo "[WARN] venv no disponible; se usa virtualenv"
    python3.11 -m pip install -q virtualenv && python3.11 -m virtualenv "${VENV}" || die "no se pudo crear el entorno virtual"
  fi
  retry 2 "${VENV}/bin/pip" install -q -r "${APP_DIR}/src/models/requirements.txt" || die "pip install"
  PYBIN="${VENV}/bin/python"
fi
PYBIN="${PYBIN:-python3}"
"${PYBIN}" -c "import pandas, pyarrow, numpy, sklearn, matplotlib, boto3, yaml; print('[BOOT] pandas', pandas.__version__, '| pyarrow', pyarrow.__version__, '| numpy', numpy.__version__, '| sklearn', sklearn.__version__)" || die "las dependencias no se importan"

# 3. Dataset (misma región que S3: sin costo de salida)
mkdir -p "${WORKDIR}/input" "${WORKDIR}/out"
retry 3 aws s3 cp "s3://${GOLD_BUCKET}/model_inputs/dataset_version=${DATASET_VERSION}/" "${WORKDIR}/input/" \
  --recursive --exclude "*" --include "*.parquet" --only-show-errors || die "no se pudo descargar el dataset"
N_PARQUET="$(find "${WORKDIR}/input" -name '*.parquet' | wc -l)"
[ "${N_PARQUET}" -ge 1 ] || die "no hay archivos .parquet en s3://${GOLD_BUCKET}/model_inputs/dataset_version=${DATASET_VERSION}/"
echo "[BOOT] dataset: ${N_PARQUET} archivos, $(du -sh "${WORKDIR}/input" | cut -f1)"
free -g

# 4. Experimentos
cd "${APP_DIR}" || die "no existe ${APP_DIR}"
if [ "${EXPERIMENTS}" = "ALL" ]; then
  IDS="$(grep -oP '^\s+- id: "\K[^"]+' config/experiments.yml | tr '\n' ' ')"
else
  IDS="$(echo "${EXPERIMENTS}" | tr ',' ' ')"
fi
[ -n "${IDS// /}" ] || die "no se encontraron experimentos"
echo "[BOOT] experimentos: ${IDS}"

run_one() {
  local id="$1" rc
  "${PYBIN}" -m src.models.orchestrator --config config/experiments.yml \
    --experiments "${id}" --target runner --train_dir "${WORKDIR}/input" \
    --output_dir "${WORKDIR}/out/${id}" --dataset_version "${DATASET_VERSION}" \
    --n_jobs "${N_JOBS}" --run_tag "${TAG}" --gold_bucket "${GOLD_BUCKET}" \
    --publicar_s3 si --publicar_leaderboard no --graficos si --reanudar si \
    > "${LOGDIR}/exp_${id}.log" 2>&1
  rc=$?
  echo "[DONE] ${id} exit=${rc} $(date +%T)"
  return "${rc}"
}
export -f run_one
export PYBIN WORKDIR LOGDIR TAG GOLD_BUCKET DATASET_VERSION N_JOBS

# shellcheck disable=SC2086
printf '%s\n' ${IDS} | xargs -P "${PARALLEL}" -I{} bash -c 'run_one {}'
echo "[BOOT] xargs terminó con código $? (123 = algún experimento falló)"

# 5. Leaderboard final con lo publicado en S3
"${PYBIN}" -m src.models.orchestrator --config config/experiments.yml \
  --experiments "$(echo "${IDS}" | xargs | tr ' ' ',')" --target consolidar \
  --dataset_version "${DATASET_VERSION}" --run_tag "${TAG}" --gold_bucket "${GOLD_BUCKET}" \
  --output_dir "${WORKDIR}/out/leaderboard"
echo "[BOOT] consolidación terminó con código $? (1 = falta algún resultado)"
echo "[BOOT] resultados: s3://${GOLD_BUCKET}/model_leaderboards/tag=${TAG}/"
