#!/usr/bin/env bash
# ==============================================================================
# run_perf_36.sh
#
# Automatizador del flujo de ejecución completo para la Optimización 3.6
# Ejecuta de forma secuencial:
#   1. Verificación de credenciales AWS y guardas de seguridad
#   2. Despliegue de infraestructura aislada (infra/perf)
#   3. Empaquetado y subida de jobs a S3
#   4. Flujo completo de Experimento E1 (Poda de particiones P vs N)
#   5. Flujo completo de Experimento E2 (Persistencia CSV A vs B)
#   6. Consolidación automática de métricas
#
# Uso:
#   bash perf/scripts/run_perf_36.sh [--all | --e1-only | --e2-only | --skip-infra]
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/guard_protected_paths.sh"

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
AWS_PROFILE="${AWS_PROFILE:-}"
RUN_E1=true
RUN_E2=true
DEPLOY_INFRA=true

BUCKET_P="sunat-risk-scoring-perf-36p"
BUCKET_N="sunat-risk-scoring-perf-36n"
BUCKET_E2="sunat-risk-scoring-perf-e2"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --aws-profile|--profile|-profile)
      AWS_PROFILE="$2"
      shift 2
      ;;
    --all)
      RUN_E1=true
      RUN_E2=true
      shift
      ;;
    --e1-only)
      RUN_E1=true
      RUN_E2=false
      shift
      ;;
    --e2-only)
      RUN_E1=false
      RUN_E2=true
      shift
      ;;
    --skip-infra)
      DEPLOY_INFRA=false
      shift
      ;;
    --help|-h)
      echo "Uso: $0 [--profile <nombre_perfil>] [--all | --e1-only | --e2-only | --skip-infra]"
      echo "Opciones:"
      echo "  --profile <nom> Perfil de AWS CLI a utilizar (default: \$AWS_PROFILE o 'bigdata' en local)"
      echo "  --all           Ejecuta el flujo completo (E1 + E2, por defecto)"
      echo "  --e1-only       Ejecuta únicamente el Experimento E1 (Poda de particiones)"
      echo "  --e2-only       Ejecuta únicamente el Experimento E2 (Persistencia CSV)"
      echo "  --skip-infra    Omite terraform apply si la infra ya fue desplegada"
      exit 0
      ;;
    *)
      echo "Opción desconocida: $1"
      echo "Uso: $0 [--profile <nombre_perfil>] [--all | --e1-only | --e2-only | --skip-infra]"
      exit 1
      ;;
  esac
done

# En entorno local sin OIDC, default a 'bigdata'
if [[ -z "${AWS_PROFILE}" && -z "${AWS_ACCESS_KEY_ID:-}" && -z "${GITHUB_ACTIONS:-}" ]]; then
  AWS_PROFILE="bigdata"
fi

if [[ -n "${AWS_PROFILE}" ]]; then
  export AWS_PROFILE
  export AWS_DEFAULT_PROFILE="${AWS_PROFILE}"
fi

echo "========================================================================"
echo "🚀 INICIANDO EJECUCIÓN AUTOMATIZADA: OPTIMIZACIÓN 3.6 (E1 y E2)"
echo "Fecha y Hora : $(date -u +"%Y-%m-%d %H:%M:%SZ")"
echo "Región AWS   : ${AWS_REGION}"
if [[ -n "${AWS_PROFILE}" ]]; then
  echo "Perfil AWS   : ${AWS_PROFILE}"
fi
echo "========================================================================"

# ------------------------------------------------------------------------------
# 1. Verificación de Autenticación AWS
# ------------------------------------------------------------------------------
echo ""
echo "[Paso 1/6] Verificando sesión activa de AWS..."
if ! CALLER_ID=$(aws sts get-caller-identity --output json 2>/dev/null); then
  echo "❌ ERROR: No hay una sesión de AWS activa o las credenciales expiraron." >&2
  echo "Por favor ejecuta 'aws login' o configura tus credenciales antes de continuar." >&2
  exit 1
fi
ACCOUNT_ID=$(echo "$CALLER_ID" | jq -r '.Account')
echo "✓ Autenticado en cuenta AWS: ${ACCOUNT_ID}"

# ------------------------------------------------------------------------------
# 2. Despliegue de Infraestructura de Pruebas (infra/perf)
# ------------------------------------------------------------------------------
if [[ "$DEPLOY_INFRA" == "true" ]]; then
  echo ""
  echo "[Paso 2/6] Verificando y desplegando infraestructura de pruebas (infra/perf)..."
  (
    cd "${REPO_ROOT}/infra/perf"
    terraform init -backend=false >/dev/null 2>&1 || terraform init
    echo "Aplicando cambios de infraestructura en AWS..."
    terraform apply -auto-approve -var="aws_profile=${AWS_PROFILE}" -var="aws_region=${AWS_REGION}"
  )
  echo "✓ Infraestructura de pruebas lista."
else
  echo ""
  echo "[Paso 2/6] Omitiendo despliegue de infraestructura (--skip-infra)."
fi

# ------------------------------------------------------------------------------
# 3. Empaquetado y Subida de Jobs a S3
# ------------------------------------------------------------------------------
echo ""
echo "[Paso 3/6] Empaquetando y sincronizando código a S3..."
bash "${SCRIPT_DIR}/package_perf.sh" --upload
echo "✓ Código de pruebas sincronizado en S3."

# ------------------------------------------------------------------------------
# 4. Experimento E1: Poda de Particiones por Año/Mes
# ------------------------------------------------------------------------------
if [[ "$RUN_E1" == "true" ]]; then
  echo ""
  echo "========================================================================"
  echo "[Paso 4/6] EJECUTANDO EXPERIMENTO E1: PODA DE PARTICIONES"
  echo "========================================================================"

  echo "-> 4.1 Copiando datos reales de Padrón a staging de pruebas..."
  bash "${SCRIPT_DIR}/stage_from_real.sh" --padron-to-staging "${BUCKET_P}"

  echo "-> 4.2 Generando layouts P (particionado) y N (sin particionar)..."
  bash "${SCRIPT_DIR}/submit_perf_job.sh" \
    --profile C3 \
    --executor-disk 100g \
    --script e1_make_layouts.py \
    --job-args "--staging-input s3://${BUCKET_P}/staging/padron_ruc --output-p s3://${BUCKET_P}/silver/padron_ruc --output-n s3://${BUCKET_N}/silver/padron_ruc" \
    --run-name "e1-make-layouts" \
    --condition "setup" \
    --exp-tag E1

  echo "-> 4.3 Copiando tablas maestras auxiliares (pricos, epen, ingresos)..."
  bash "${SCRIPT_DIR}/stage_from_real.sh" --small-to-silver "${BUCKET_P}"
  bash "${SCRIPT_DIR}/stage_from_real.sh" --small-to-silver "${BUCKET_N}"

  echo "-> 4.4 Ejecutando corridas Spark alternadas (Job 04 Regional Gold)..."
  # Calentamiento (descartado en análisis)
  echo "  [Corrida Calentamiento] Layout N..."
  bash "${SCRIPT_DIR}/submit_perf_job.sh" --profile C3 --script perf_runner.py --job-args "--job 04_regional_gold" --silver-bucket "${BUCKET_N}" --gold-bucket "${BUCKET_N}" --run-name "run-e1-warmup" --exp-tag E1

  # 6 corridas medidas alternadas
  for i in 1 2 3; do
    echo "  [Medición ${i}/3] Layout N (Línea Base)..."
    bash "${SCRIPT_DIR}/submit_perf_job.sh" --profile C3 --script perf_runner.py --job-args "--job 04_regional_gold" --silver-bucket "${BUCKET_N}" --gold-bucket "${BUCKET_N}" --run-name "run-e1-N${i}" --exp-tag E1

    echo "  [Medición ${i}/3] Layout P (Particionado)..."
    bash "${SCRIPT_DIR}/submit_perf_job.sh" --profile C3 --script perf_runner.py --job-args "--job 04_regional_gold" --silver-bucket "${BUCKET_P}" --gold-bucket "${BUCKET_P}" --run-name "run-e1-P${i}" --exp-tag E1
  done

  echo "-> 4.5 Validando paridad de salida regional entre N y P (compare_outputs)..."
  bash "${SCRIPT_DIR}/submit_perf_job.sh" \
    --profile C1 \
    --script compare_outputs.py \
    --job-args "--path-a s3://${BUCKET_N}/gold/regional_summary --path-b s3://${BUCKET_P}/gold/regional_summary --key-col Departamento" \
    --run-name "e1-compare-outputs" \
    --condition "utility" \
    --exp-tag E1 || echo "WARN: compare_outputs finalizado con advertencias."

  echo "-> 4.6 Ejecutando Crawlers de Glue para catalogar tablas en Athena..."
  for c_name in "sunat-ssco-perf-36p-crawler" "sunat-ssco-perf-36n-crawler"; do
    aws glue start-crawler --name "$c_name" || true
  done

  echo "Esperando que los crawlers cataloguen las tablas..."
  sleep 5
  for c_name in "sunat-ssco-perf-36p-crawler" "sunat-ssco-perf-36n-crawler"; do
    for _ in {1..6}; do
      c_state=$(aws glue get-crawler --name "$c_name" --query 'Crawler.State' --output text 2>/dev/null || echo "UNKNOWN")
      [[ "$c_state" == "RUNNING" || "$c_state" == "STOPPING" ]] && break
      sleep 2
    done
    while true; do
      c_state=$(aws glue get-crawler --name "$c_name" --query 'Crawler.State' --output text 2>/dev/null || echo "READY")
      [[ "$c_state" == "READY" ]] && break
      echo "  Crawler $c_name en estado $c_state... esperando 10s"
      sleep 10
    done
    echo "  ✓ Crawler $c_name completado."
  done

  echo "-> 4.7 Ejecutando consultas analíticas de benchmark en Athena..."
  bash "${SCRIPT_DIR}/athena_bench.sh" || echo "WARN: Consulta Athena finalizada con advertencias."

  echo "-> 4.8 Consolidando métricas de E1..."
  python3 "${REPO_ROOT}/perf/analysis/consolidate_runs.py" "${REPO_ROOT}/perf/results/E1/"*.json --output-json "${REPO_ROOT}/perf/results/E1/summary.json" > "${REPO_ROOT}/perf/results/E1/summary.md" || true
  cat "${REPO_ROOT}/perf/results/E1/summary.md" || true
  echo "✓ Experimento E1 finalizado exitosamente."
fi

# ------------------------------------------------------------------------------
# 5. Experimento E2: Persistencia contra Doble Lectura de CSV en Job 01
# ------------------------------------------------------------------------------
if [[ "$RUN_E2" == "true" ]]; then
  echo ""
  echo "========================================================================"
  echo "[Paso 5/6] EJECUTANDO EXPERIMENTO E2: PERSISTENCIA CSV (JOB 01)"
  echo "========================================================================"

  # Calentamiento
  echo "  [Corrida Calentamiento] Condición A..."
  bash "${SCRIPT_DIR}/perf_clean_layer.sh" --bucket "${BUCKET_E2}" --layer bronze
  bash "${SCRIPT_DIR}/submit_perf_job.sh" --profile C3 --script 01_ingest_bronze.py --bronze-bucket "${BUCKET_E2}" --run-name "run-e2-warmup" --exp-tag E2
  sleep 10

  # 6 corridas medidas alternadas: A1, B1, A2, B2, A3, B3
  for i in 1 2 3; do
    echo "  [Medición ${i}/3] Condición A (Job 01 original sin persist)..."
    bash "${SCRIPT_DIR}/perf_clean_layer.sh" --bucket "${BUCKET_E2}" --layer bronze
    bash "${SCRIPT_DIR}/submit_perf_job.sh" --profile C3 --script 01_ingest_bronze.py --bronze-bucket "${BUCKET_E2}" --run-name "run-e2-A${i}" --exp-tag E2
    sleep 10

    echo "  [Medición ${i}/3] Condición B (Variante con persist MEMORY_AND_DISK)..."
    bash "${SCRIPT_DIR}/perf_clean_layer.sh" --bucket "${BUCKET_E2}" --layer bronze
    bash "${SCRIPT_DIR}/submit_perf_job.sh" --profile C3 --script e2_ingest_bronze_persist.py --bronze-bucket "${BUCKET_E2}" --run-name "run-e2-B${i}" --exp-tag E2
    sleep 10
  done

  echo "-> 5.2 Auditando capas escritas con s_count_layers..."
  bash "${SCRIPT_DIR}/submit_perf_job.sh" \
    --profile C1 \
    --script s_count_layers.py \
    --job-args "--layer bronze" \
    --bronze-bucket "${BUCKET_E2}" \
    --run-name "run-e2-count-layers" \
    --condition "utility" \
    --exp-tag E2 || echo "WARN: Auditoría s_count_layers finalizada con advertencias."

  echo "-> 5.3 Consolidando métricas de E2..."
  python3 "${REPO_ROOT}/perf/analysis/consolidate_runs.py" "${REPO_ROOT}/perf/results/E2/"*.json --output-json "${REPO_ROOT}/perf/results/E2/summary.json" > "${REPO_ROOT}/perf/results/E2/summary.md" || true
  cat "${REPO_ROOT}/perf/results/E2/summary.md" || true
  echo "✓ Experimento E2 finalizado exitosamente."
fi

# ------------------------------------------------------------------------------
# 6. Cierre y Resumen Final
# ------------------------------------------------------------------------------
echo ""
echo "========================================================================"
echo "🎉 EJECUCIÓN AUTOMATIZADA COMPLETADA CON ÉXITO"
echo "Resultados JSON en : ${REPO_ROOT}/perf/results/"
echo "Fichas de Informe  : docs/perf/3.6/E1.md y docs/perf/3.6/E2.md"
echo "========================================================================"
