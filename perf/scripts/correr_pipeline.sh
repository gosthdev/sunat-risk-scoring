#!/usr/bin/env bash
# Corre el pipeline completo (jobs 01, 02, 04, 03, 05) sobre el bucket de un factor,
# con el script de producción sin modificar (src/spark/jobs/05_submit_spark_steps.sh).
#
# Uso: AWS_PROFILE=bigdata perf/scripts/correr_pipeline.sh <factor> <Base|C1> <corrida>
#   Base: capacidad por defecto del pipeline (2 a 16 executors de 4 vCPU / 14 GB, según
#         el volumen del RAW). Hasta S-05-r1 el pipeline usaba un techo de 6.
#   C1:   un solo executor ("una sola máquina").
# Resultado: perf/results/runs/<corrida>.json. Los logs completos quedan en scratch/perf/<corrida>/.

set -euo pipefail
source "$(dirname "$0")/comun.sh"

FACTOR="${1:?Uso: $0 <factor> <Base|C1> <corrida>}"
PERFIL="${2:?}"
CORRIDA="${3:?}"
BUCKET="$(bucket_de "$FACTOR")"
case "$PERFIL" in
  Base) MIN=2; TECHO=16; APP="$APP_NOMBRE" ;;
  C1) MIN=1; TECHO=1; APP="$APP_NOMBRE_C1" ;;
  *) echo "Perfil desconocido: $PERFIL (Base o C1)" >&2; exit 1 ;;
esac

# Todo apunta al bucket perf: si faltara alguna, 05_submit usaría producción por defecto.
export EMR_APPLICATION_ID="$(app_id "$APP")"
export EMR_EXECUTION_ROLE_ARN="$(rol_arn)"
export ARTIFACTS_BUCKET="$ARTEFACTOS" JOBS_PREFIX="$PREFIJO_JOBS"
export RAW_BUCKET="$BUCKET" BRONZE_BUCKET="$BUCKET" SILVER_BUCKET="$BUCKET" GOLD_BUCKET="$BUCKET" DATALAKE_BUCKET="$BUCKET"
export MIN_EXECUTORS="$MIN" MAX_EXECUTORS_CAP="$TECHO"
# Un fallo es un resultado: no se reintenta (duplicaría el costo). Tope de 6 h por paso.
export STEP_MAX_ATTEMPTS=1 STEP_TIMEOUT_MINUTES=360

echo "== Limpieza de las capas de la corrida anterior en s3://$BUCKET (el RAW y los datasets pequeños se conservan)"
for prefijo in bronze gold _staging silver/padron_ruc silver/ordenes_compra \
  silver/quarantine_padron_ruc silver/quarantine_ordenes_compra silver/quality_report; do
  aws s3 rm "s3://${BUCKET}/${prefijo}/" --recursive --only-show-errors
done

subir_artefactos

DIR="$RAIZ/scratch/perf/$CORRIDA"
mkdir -p "$DIR"
: >"$DIR/pasos.tsv"
for paso in 01_ingest_bronze 02_clean_silver 04_regional_gold 03_feature_gold 05_scoring_dataset_gold; do
  INICIO=$(date +%s)
  RC=0
  bash "$RAIZ/src/spark/jobs/05_submit_spark_steps.sh" --only-steps "$paso" >"$DIR/$paso.log" 2>&1 || RC=$?
  FIN=$(date +%s)
  RUN=$(grep -o 'Job Run ID asignado: [a-z0-9]*' "$DIR/$paso.log" | tail -1 | awk '{print $NF}' || true)
  printf "%s\t%s\t%s\t%s\t%s\n" "$paso" "$RUN" "$INICIO" "$FIN" "$RC" >>"$DIR/pasos.tsv"
  echo "$(date +%T) $paso  job=$RUN  codigo=$RC  $((FIN - INICIO)) s"
  (( RC == 0 )) || break
done

py_aws "$RAIZ/perf/analysis/collect_runs.py" --tipo pipeline --corrida "$CORRIDA" \
  --factor "$FACTOR" --perfil "$PERFIL" --app-id "$EMR_APPLICATION_ID" --pasos "$DIR/pasos.tsv" \
  --salida "$RAIZ/perf/results/runs/${CORRIDA}.json"
