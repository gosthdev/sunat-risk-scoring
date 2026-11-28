#!/usr/bin/env bash
# Prepara el bucket de un factor para correr el pipeline sobre él.
#
#   1. Copia 0: el RAW real tal cual (copia S3 -> S3 dentro de AWS; producción solo se lee).
#   2. Los 5 datasets pequeños de Silver que leen los jobs 03-05 (se usan en 1x).
#   3. Si factor > 1: job de EMR (s_replicate_dataset.py) que agrega las copias 1..factor-1.
#
# Uso: AWS_PROFILE=bigdata perf/scripts/replicar.sh <factor>
# Resultado: perf/results/runs/replicacion-x<factor>.json

set -euo pipefail
source "$(dirname "$0")/comun.sh"

FACTOR="${1:?Uso: $0 <factor>}"
BUCKET="$(bucket_de "$FACTOR")"
CORRIDA="replicacion-x${FACTOR}"
DIR="$RAIZ/scratch/perf/$CORRIDA"
mkdir -p "$DIR"
: >"$DIR/pasos.tsv"

echo "== Copia 0 (RAW real) -> s3://$BUCKET/raw/"
# --delete deja el destino igual al RAW real: borra copias de una ejecución anterior.
for dataset in padron_ruc ordenes_compra; do
  aws s3 sync "s3://${RAW_PROD}/${dataset}/" "s3://${BUCKET}/raw/${dataset}/" --delete --only-show-errors
done

echo "== Datasets pequeños de Silver -> s3://$BUCKET/silver/"
for dataset in "${DATASETS_PEQUENOS[@]}"; do
  aws s3 sync "s3://${SILVER_PROD}/${dataset}/" "s3://${BUCKET}/silver/${dataset}/" --delete --only-show-errors
done

if (( FACTOR > 1 )); then
  echo "== Copias 1..$((FACTOR - 1)) con EMR"
  subir_artefactos
  APP="$(app_id)"
  JOBS="s3://${ARTEFACTOS}/${PREFIJO_JOBS}"
  PARAMS="--py-files ${JOBS}/common.zip,${JOBS}/perf_keys.py \
--conf spark.driver.cores=4 --conf spark.driver.memory=14g \
--conf spark.executor.cores=4 --conf spark.executor.memory=14g \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=2 --conf spark.dynamicAllocation.maxExecutors=6 \
--conf spark.driver.maxResultSize=4g"
  INICIO=$(date +%s)
  RUN=$(aws emr-serverless start-job-run --application-id "$APP" --execution-role-arn "$(rol_arn)" \
    --name "perf-replicar-x${FACTOR}" --execution-timeout-minutes 720 \
    --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"${JOBS}/s_replicate_dataset.py\",\"entryPointArguments\":[\"--factor\",\"${FACTOR}\",\"--destino\",\"${BUCKET}\"],\"sparkSubmitParameters\":\"${PARAMS}\"}}" \
    --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://${ARTEFACTOS}/emr-logs/\"}}}" \
    --query jobRunId --output text)
  echo "Job run: $RUN"
  while :; do
    ESTADO=$(aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$RUN" --query jobRun.state --output text)
    echo "$(date +%T) $ESTADO"
    [[ "$ESTADO" == SUCCESS || "$ESTADO" == FAILED || "$ESTADO" == CANCELLED ]] && break
    sleep 60
  done
  RC=0
  [[ "$ESTADO" == SUCCESS ]] || RC=1
  printf "replicacion\t%s\t%s\t%s\t%s\n" "$RUN" "$INICIO" "$(date +%s)" "$RC" >>"$DIR/pasos.tsv"
fi

# La CLI pagina de a 1000 objetos y devuelve una suma por página: se suman todas.
BYTES=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix raw/ --query "sum(Contents[].Size)" --output text \
  | tr -d '\r' | awk '{s += $1} END {printf "%.0f", s}')
echo "RAW en el bucket: $(awk -v b="$BYTES" 'BEGIN{printf "%.2f GB", b/1e9}')"
py_aws "$RAIZ/perf/analysis/collect_runs.py" --tipo replicacion --corrida "$CORRIDA" \
  --factor "$FACTOR" --perfil Base --app-id "$(app_id)" --pasos "$DIR/pasos.tsv" --raw-bytes "$BYTES" \
  --salida "$RAIZ/perf/results/runs/${CORRIDA}.json"
[[ "${ESTADO:-SUCCESS}" == SUCCESS ]]
