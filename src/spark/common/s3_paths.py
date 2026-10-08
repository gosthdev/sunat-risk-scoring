"""Constantes de rutas S3 por capa (raw/bronze/silver/gold).

Centraliza los paths S3 para evitar hardcodear strings en cada job.
Compatible tanto con la arquitectura multicanasta (1 bucket por capa,
definida en infra/storage) como con un bucket compartido (DATALAKE_BUCKET).
"""

import os

# Resolver nombres de bucket desde variables de entorno
_base_datalake = os.environ.get("DATALAKE_BUCKET", "")
_prefix_base = _base_datalake.removesuffix("-raw")

RAW_BUCKET = os.environ.get(
    "RAW_BUCKET",
    f"{_prefix_base}-raw" if _prefix_base else "sunat-risk-scoring-raw",
)
BRONZE_BUCKET = os.environ.get(
    "BRONZE_BUCKET",
    f"{_prefix_base}-bronze" if _prefix_base else "sunat-risk-scoring-bronze",
)
SILVER_BUCKET = os.environ.get(
    "SILVER_BUCKET",
    f"{_prefix_base}-silver" if _prefix_base else "sunat-risk-scoring-silver",
)
GOLD_BUCKET = os.environ.get(
    "GOLD_BUCKET",
    f"{_prefix_base}-gold" if _prefix_base else "sunat-risk-scoring-gold",
)

# Soporte para bucket único si todos coinciden
_is_single_bucket = RAW_BUCKET == BRONZE_BUCKET == SILVER_BUCKET == GOLD_BUCKET


def _build_path(bucket: str, layer_prefix: str, subpath: str) -> str:
    if _is_single_bucket:
        return f"s3://{bucket}/{layer_prefix}/{subpath}"
    return f"s3://{bucket}/{subpath}"


# ---------------------------------------------------------------- RAW -----
RAW_PADRON_RUC = _build_path(RAW_BUCKET, "raw", "padron_ruc")
RAW_ORDENES_COMPRA = _build_path(RAW_BUCKET, "raw", "ordenes_compra")
RAW_PRICOS = _build_path(RAW_BUCKET, "raw", "pricos")
RAW_INGRESOS_TRIBUTARIOS = _build_path(RAW_BUCKET, "raw", "ingresos_tributarios")
RAW_EPEN = _build_path(RAW_BUCKET, "raw", "epen")
RAW_SSCO = _build_path(RAW_BUCKET, "raw", "ssco")

# -------------------------------------------------------------- BRONZE ----
BRONZE_PADRON_RUC = _build_path(BRONZE_BUCKET, "bronze", "padron_ruc")
BRONZE_ORDENES_COMPRA = _build_path(BRONZE_BUCKET, "bronze", "ordenes_compra")
BRONZE_PADRON_RUC_QUARANTINE = _build_path(
    BRONZE_BUCKET, "bronze", "quarantine/padron_ruc"
)
BRONZE_ORDENES_COMPRA_QUARANTINE = _build_path(
    BRONZE_BUCKET, "bronze", "quarantine/ordenes_compra"
)

# -------------------------------------------------------------- SILVER ----
SILVER_PADRON_RUC = _build_path(SILVER_BUCKET, "silver", "padron_ruc")
SILVER_ORDENES_COMPRA = _build_path(SILVER_BUCKET, "silver", "ordenes_compra")
SILVER_PRICOS = _build_path(SILVER_BUCKET, "silver", "pricos")
SILVER_INGRESOS_TRIBUTARIOS = _build_path(
    SILVER_BUCKET, "silver", "ingresos_tributarios"
)
SILVER_EPEN = _build_path(SILVER_BUCKET, "silver", "epen")
SILVER_SSCO = _build_path(SILVER_BUCKET, "silver", "ssco")

# ---------------------------------------------------------------- GOLD ----
GOLD_RUC_FEATURES = _build_path(GOLD_BUCKET, "gold", "ruc_features")
GOLD_REGIONAL_SUMMARY = _build_path(GOLD_BUCKET, "gold", "regional_summary")
GOLD_SCORING_DATASET = _build_path(GOLD_BUCKET, "gold", "scoring_dataset")
