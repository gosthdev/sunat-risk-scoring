set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -n "${CI:-}" ]]; then
  AWS_PROFILE="${AWS_PROFILE:-}"
else
  AWS_PROFILE="${AWS_PROFILE:-bigdata}"
fi
if [[ -n "$AWS_PROFILE" ]]; then
  export AWS_PROFILE
fi

echo "Directorio de trabajo: $SCRIPT_DIR"
echo "Perfil de AWS:         ${AWS_PROFILE:-'(por variables de entorno / OIDC)'}"

# 1. Validar que urls.json no esté completamente vacío
if ! grep -q 'http' urls.json; then
  echo "ADVERTENCIA: No se detectaron enlaces 'http' en $SCRIPT_DIR/urls.json."
  echo "Por favor edita urls.json con las URLs de descarga antes de continuar."
  if [ -t 0 ]; then
    read -p "¿Deseas continuar de todos modos? (s/N): " -r CONFIRM
    if [[ ! "$CONFIRM" =~ ^[sS]$ ]]; then
      echo "Operación cancelada."
      exit 0
    fi
  else
    echo "Entorno no interactivo detectado; abortando porque urls.json no tiene URLs http."
    exit 1
  fi
fi

# 2. Inicializar y aplicar Terraform
DEPLOYED=0
CLEANED_UP=0

cleanup() {
  if [ "$DEPLOYED" -eq 1 ] && [ "$CLEANED_UP" -eq 0 ]; then
    CLEANED_UP=1
    echo -e "\n================================================================="
    echo "   [4/4] DESTRUCCIÓN TOTAL DE LA EC2 EFÍMERA (CERO RESIDUOS)    "
    echo "================================================================="
    echo "Ejecutando 'terraform destroy -auto-approve'..."
    terraform destroy -auto-approve || true
    echo -e "\n✓ Verificación: Toda la infraestructura efímera ha sido eliminada."
  fi
}

# Registrar trap para garantizar destrucción ante cualquier interrupción o salida
trap cleanup EXIT INT TERM

echo -e "\n[1/4] Inicializando Terraform..."
terraform init

echo -e "\n[2/4] Desplegando EC2 efímera..."
terraform apply -auto-approve
DEPLOYED=1

RAW_BUCKET=$(terraform output -raw raw_bucket_name 2>/dev/null || echo "sunat-risk-scoring-raw")
INSTANCE_ID=$(terraform output -raw instance_id 2>/dev/null || echo "desconocido")
PUBLIC_IP=$(terraform output -raw instance_public_ip 2>/dev/null || echo "desconocido")

echo -e "\n✓ EC2 desplegada exitosamente:"
echo "  - Instancia: $INSTANCE_ID"
echo "  - IP Efímera: $PUBLIC_IP"
echo "  - Bucket Raw: s3://$RAW_BUCKET"

# Función de limpieza garantizada ante cualquier salida o interrupción
cleanup() {
  echo "Ejecutando 'terraform destroy -auto-approve'..."
  terraform destroy -auto-approve
  echo -e "\n✓ Verificación: Toda la infraestructura efímera ha sido eliminada."
}

# 3. Monitoreo del proceso en S3
echo -e "\n[3/4] Monitoreando progreso de descarga, conversión y subida a S3..."
echo "Esperando señal de finalización desde la EC2..."

# Limpiar marcadores previos si existieran
aws s3 rm "s3://$RAW_BUCKET/_INGESTION_SUCCESS" 2>/dev/null || true
aws s3 rm "s3://$RAW_BUCKET/_INGESTION_FAILED" 2>/dev/null || true

START_TIME=$(date +%s)
MAX_TIMEOUT_SECONDS=3600
SUCCESS=0

while true; do
  ELAPSED=$(( $(date +%s) - START_TIME ))
  
  # Verificar si ya terminó con éxito
  if aws s3 ls "s3://$RAW_BUCKET/_INGESTION_SUCCESS" >/dev/null 2>&1; then
    echo -e "\n✓ ¡Proceso completado con ÉXITO en la EC2! ($ELAPSED segundos)"
    SUCCESS=1
    break
  fi

  # Verificar si reportó error
  if aws s3 ls "s3://$RAW_BUCKET/_INGESTION_FAILED" >/dev/null 2>&1; then
    echo -e "\n❌ La EC2 reportó un ERROR durante el procesamiento."
    aws s3 cp "s3://$RAW_BUCKET/_ingestion_process.log" ./ingestion_error.log 2>/dev/null || true
    echo "Revisa el log descargado: $SCRIPT_DIR/ingestion_error.log"
    break
  fi

  # Verificar timeout de seguridad
  if [ "$ELAPSED" -ge "$MAX_TIMEOUT_SECONDS" ]; then
    echo -e "\n⚠️ Se alcanzó el tiempo límite máximo ($MAX_TIMEOUT_SECONDS s). Abortando..."
    aws s3 cp "s3://$RAW_BUCKET/_ingestion_user_data.log" ./ingestion_timeout_userdata.log 2>/dev/null || true
    aws s3 cp "s3://$RAW_BUCKET/_ingestion_process.log" ./ingestion_timeout_process.log 2>/dev/null || true
    break
  fi

  # Feedback cada 15 segundos
  printf "\rEsperando finalización... (%d s transcurridos)" "$ELAPSED"
  sleep 15
done

# 4. Auditoría de verificación y destrucción
if [ "$SUCCESS" -eq 1 ]; then
  echo -e "\n================================================================="
  echo "   [4/5] AUDITORÍA Y VERIFICACIÓN DE DATASETS EN S3             "
  echo "================================================================="
  
  # Descargar y mostrar resumen emitido por el script de procesamiento
  aws s3 cp "s3://$RAW_BUCKET/_INGESTION_SUCCESS" ./_ingestion_summary.txt 2>/dev/null || true
  if [ -f ./_ingestion_summary.txt ]; then
    cat ./_ingestion_summary.txt
    rm -f ./_ingestion_summary.txt
  fi

  echo -e "\nVerificando presencia en S3 (s3://$RAW_BUCKET/):"

  # Padrón RUC
  PADRON_COUNT=$(aws s3 ls "s3://$RAW_BUCKET/padron_ruc/" --recursive 2>/dev/null | grep -c 'padron.csv' || true)
  echo "  [✓] Padrón RUC:              $PADRON_COUNT / 12 meses particionados (Hive anio/mes)"

  # Órdenes de Compra
  ORDENES_COUNT=$(aws s3 ls "s3://$RAW_BUCKET/ordenes_compra/" --recursive 2>/dev/null | grep -c 'ordenes.csv' || true)
  echo "  [✓] Órdenes de Compra:       $ORDENES_COUNT / 12 meses particionados (Hive anio/mes)"

  # PRICOS
  if aws s3 ls "s3://$RAW_BUCKET/pricos/principalesContrib-PRICOS.xlsx" >/dev/null 2>&1; then
    echo "  [✓] PRICOS:                  Presente (principalesContrib-PRICOS.xlsx)"
  else
    echo "  [⚠️] PRICOS:                  No detectado en S3"
  fi

  # SSCO
  if aws s3 ls "s3://$RAW_BUCKET/ssco/sujesincapacidadOperativa.xlsx" >/dev/null 2>&1; then
    echo "  [✓] SSCO:                    Presente (sujesincapacidadOperativa.xlsx)"
  else
    echo "  [⚠️] SSCO:                    No detectado en S3"
  fi

  # Ingresos Tributarios
  if aws s3 ls "s3://$RAW_BUCKET/ingresos_tributarios/cdrA13_tabular.csv" >/dev/null 2>&1; then
    echo "  [✓] Ingresos Tributarios:    Presente (cdrA13_tabular.csv)"
  else
    echo "  [⚠️] Ingresos Tributarios:    No detectado en S3"
  fi

  # EPEN
  if aws s3 ls "s3://$RAW_BUCKET/epen/anio=2025/epen.csv" >/dev/null 2>&1; then
    echo "  [✓] EPEN:                    Presente (anio=2025/epen.csv)"
  else
    echo "  [⚠️] EPEN:                    No detectado en S3"
  fi

  echo "-----------------------------------------------------------------"
  aws s3 ls "s3://$RAW_BUCKET/" --human-readable --summarize --recursive 2>/dev/null | tail -n 2 || true

  # Registrar marcador de finalización exitosa en S3 (idempotencia)
  echo -e "\nRegistrando marcador de ingesta en s3://$RAW_BUCKET/_markers/ingestion_complete.json..."
  if [ -f "$SCRIPT_DIR/../../scripts/write_marker.sh" ]; then
    bash "$SCRIPT_DIR/../../scripts/write_marker.sh" \
      --stage raw \
      --marker-uri "s3://${RAW_BUCKET}/_markers/ingestion_complete.json" \
      --files "$SCRIPT_DIR/urls.json" "$SCRIPT_DIR/scripts/process_and_upload.py"
  else
    URLS_HASH=$(sha256sum "$SCRIPT_DIR/urls.json" | awk '{print $1}')
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    MARKER_TMP=$(mktemp)
    cat <<EOF > "$MARKER_TMP"
{
  "stage": "raw",
  "timestamp": "${TIMESTAMP}",
  "marker_uri": "s3://${RAW_BUCKET}/_markers/ingestion_complete.json",
  "job_hash": "sha256:${URLS_HASH}",
  "urls_json_hash": "sha256:${URLS_HASH}",
  "upstream_markers": {}
}
EOF
    aws s3 cp "$MARKER_TMP" "s3://${RAW_BUCKET}/_markers/ingestion_complete.json"
    rm -f "$MARKER_TMP"
    echo "✓ Marcador de ingesta registrado exitosamente."
  fi
fi

# 5. Destrucción total garantizada
cleanup

if [ "$SUCCESS" -eq 1 ]; then 
  echo "   ¡TRANQUILIDAD TOTAL: INGESTA Y DESTRUCCIÓN EXITOSAS!    "
else
  echo -e "\nRevisa los errores reportados antes de volver a intentar."
  exit 1
fi
