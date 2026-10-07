"""
Constantes de rutas S3 por capa (raw/bronze/silver/gold).

Centralizar esto evita hardcodear strings de rutas en cada job. Si cambia
el nombre del bucket o la convención de carpetas, se edita solo este archivo.
"""

import os

# El nombre del bucket se toma de una variable de entorno para no hardcodear
# el nombre real de la cuenta en el código. Ajustar DATALAKE_BUCKET al
# lanzar el cluster EMR (variable de entorno o --conf en spark-submit).
BUCKET = os.environ.get("DATALAKE_BUCKET", "CAMBIAR-nombre-bucket")

RAW_PREFIX = "raw"
BRONZE_PREFIX = "bronze"
BRONZE_QUARANTINE_PREFIX = "bronze/quarantine"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"


def _s3(path: str) -> str:
    return f"s3://{BUCKET}/{path}"


# ---------------------------------------------------------------- RAW -----
RAW_PADRON_RUC = _s3(f"{RAW_PREFIX}/padron_ruc")
RAW_ORDENES_COMPRA = _s3(f"{RAW_PREFIX}/ordenes_compra")
RAW_PRICOS = _s3(f"{RAW_PREFIX}/pricos")
RAW_INGRESOS_TRIBUTARIOS = _s3(f"{RAW_PREFIX}/ingresos_tributarios")
RAW_EPEN = _s3(f"{RAW_PREFIX}/epen")
RAW_SSCO = _s3(f"{RAW_PREFIX}/ssco")

# -------------------------------------------------------------- BRONZE ----
BRONZE_PADRON_RUC = _s3(f"{BRONZE_PREFIX}/padron_ruc")
BRONZE_ORDENES_COMPRA = _s3(f"{BRONZE_PREFIX}/ordenes_compra")
BRONZE_PADRON_RUC_QUARANTINE = _s3(f"{BRONZE_QUARANTINE_PREFIX}/padron_ruc")
BRONZE_ORDENES_COMPRA_QUARANTINE = _s3(f"{BRONZE_QUARANTINE_PREFIX}/ordenes_compra")

# -------------------------------------------------------------- SILVER ----
SILVER_PADRON_RUC = _s3(f"{SILVER_PREFIX}/padron_ruc")
SILVER_ORDENES_COMPRA = _s3(f"{SILVER_PREFIX}/ordenes_compra")
SILVER_PRICOS = _s3(f"{SILVER_PREFIX}/pricos")
SILVER_INGRESOS_TRIBUTARIOS = _s3(f"{SILVER_PREFIX}/ingresos_tributarios")
SILVER_EPEN = _s3(f"{SILVER_PREFIX}/epen")
SILVER_SSCO = _s3(f"{SILVER_PREFIX}/ssco")

# ---------------------------------------------------------------- GOLD ----
GOLD_RUC_FEATURES = _s3(f"{GOLD_PREFIX}/ruc_features")
GOLD_REGIONAL_SUMMARY = _s3(f"{GOLD_PREFIX}/regional_summary")
GOLD_SCORING_DATASET = _s3(f"{GOLD_PREFIX}/scoring_dataset")
