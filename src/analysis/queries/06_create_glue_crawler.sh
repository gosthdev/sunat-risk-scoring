#!/usr/bin/env bash
# Fase 3 - Crea (o actualiza) los crawlers de Glue sobre silver/ y gold/ y los ejecuta.
# Puebla el Glue Data Catalog con una tabla por carpeta: silver_<tabla> y gold_<tabla>.
#
# Uso:
#   BUCKET=mi-bucket GLUE_ROLE_ARN=arn:aws:iam::123456789012:role/glue-crawler ./06_create_glue_crawler.sh
#
# Requisito: silver/ y gold/ ya deben tener datos (corre antes los jobs 02 a 05).
set -euo pipefail

# Soporte multicanasta (infra/storage) o bucket único del datalake (plan.md)
if [[ -z "${GLUE_DATABASE:-}" ]]; then
  if aws glue get-database --name "sunat_ssco" >/dev/null 2>&1; then
    GLUE_DATABASE="sunat_ssco"
  else
    GLUE_DATABASE="ssco_catalog"
  fi
fi
ZONES=(silver gold)

# Auto-descubrir rol IAM si no fue provisto explícitamente
if [[ -z "${GLUE_ROLE_ARN:-}" ]]; then
  echo "INFO: 'GLUE_ROLE_ARN' no provista. Infiriendo rol desde caller-identity..."
  ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null || true)
  if [[ -n "$ACCOUNT_ID" && "$ACCOUNT_ID" != "None" ]]; then
    GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/sunat-ssco-glue-crawler-role"
    echo "✓ Rol Glue Crawler inferido: $GLUE_ROLE_ARN"
  else
    GLUE_ROLE_ARN=$(aws iam get-role --role-name sunat-ssco-glue-crawler-role --query 'Role.Arn' --output text 2>/dev/null || true)
  fi
fi
: "${GLUE_ROLE_ARN:?Define GLUE_ROLE_ARN (o asegúrate de que exista sunat-ssco-glue-crawler-role)}"

ensure_database() {
  if ! aws glue get-database --name "$GLUE_DATABASE" >/dev/null 2>&1; then
    echo "Creando base de datos Glue: $GLUE_DATABASE"
    aws glue create-database --database-input "Name=${GLUE_DATABASE}"
  fi
}

get_crawler_name() {
  local zone="$1"
  if aws glue get-crawler --name "sunat-ssco-${zone}-crawler" >/dev/null 2>&1; then
    echo "sunat-ssco-${zone}-crawler"
  else
    echo "ssco-${zone}-crawler"
  fi
}

upsert_crawler() {
  local zone="$1"
  local name
  name=$(get_crawler_name "$zone")
  local target_path
  local level_cfg

  if [[ "$zone" == "silver" && -n "${SILVER_BUCKET:-}" ]]; then
    target_path="s3://${SILVER_BUCKET}/"
    level_cfg=1
  elif [[ "$zone" == "gold" && -n "${GOLD_BUCKET:-}" ]]; then
    target_path="s3://${GOLD_BUCKET}/"
    level_cfg=1
  else
    : "${BUCKET:?Define BUCKET o (SILVER_BUCKET y GOLD_BUCKET)}"
    target_path="s3://${BUCKET}/${zone}/"
    level_cfg=2
  fi

  local config="{\"Version\":1.0,\"Grouping\":{\"TableLevelConfiguration\":${level_cfg}}}"
  local targets="{\"S3Targets\":[{\"Path\":\"${target_path}\",\"Exclusions\":[\"_markers/**\",\"**/_markers/**\"]}]}"
  local args=(
    --name "$name"
    --role "$GLUE_ROLE_ARN"
    --database-name "$GLUE_DATABASE"
    --table-prefix "${zone}_"
    --targets "$targets"
    --schema-change-policy "UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=LOG"
    --recrawl-policy "{\"RecrawlBehavior\":\"CRAWL_EVERYTHING\"}"
    --configuration "$config"
  )

  if aws glue get-crawler --name "$name" >/dev/null 2>&1; then
    echo "Actualizando crawler: $name ($target_path, TableLevelConfiguration=${level_cfg})"
    aws glue update-crawler "${args[@]}"
  else
    echo "Creando crawler: $name ($target_path, TableLevelConfiguration=${level_cfg})"
    aws glue create-crawler "${args[@]}"
  fi
}

run_crawler() {
  local name="$1"
  echo "Iniciando crawler: $name"
  aws glue start-crawler --name "$name"

  while true; do
    state=$(aws glue get-crawler --name "$name" --query 'Crawler.State' --output text)
    [[ "$state" == "READY" ]] && break
    echo "  $name -> $state"
    sleep 15
  done

  status=$(aws glue get-crawler --name "$name" --query 'Crawler.LastCrawl.Status' --output text)
  echo "Crawler $name terminó con estado: $status"
  if [[ "$status" != "SUCCEEDED" ]]; then
    error_msg=$(aws glue get-crawler --name "$name" --query 'Crawler.LastCrawl.ErrorMessage' --output text 2>/dev/null || true)
    if [[ -n "$error_msg" && "$error_msg" != "None" ]]; then
      echo "ERROR en Crawler $name: $error_msg" >&2
    else
      echo "Revisa los logs del crawler en CloudWatch" >&2
    fi
    exit 1
  fi
}

ensure_database
for zone in "${ZONES[@]}"; do upsert_crawler "$zone"; done
for zone in "${ZONES[@]}"; do
  crawler_name=$(get_crawler_name "$zone")
  run_crawler "$crawler_name"
done

echo
echo "Tablas en el catálogo ($GLUE_DATABASE):"
aws glue get-tables --database-name "$GLUE_DATABASE" --query 'TableList[].Name' --output table

table_count=$(aws glue get-tables --database-name "$GLUE_DATABASE" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
echo "Total de tablas registradas en $GLUE_DATABASE: $table_count"

if [[ -z "$table_count" || "$table_count" == "None" || "$table_count" -lt 1 ]]; then
  echo "ERROR: Glue crawlers terminaron pero no crearon tablas en la base de datos '$GLUE_DATABASE'!" >&2
  echo "Verifique que los datos en S3 contengan archivos Parquet válidos." >&2
  exit 1
fi
