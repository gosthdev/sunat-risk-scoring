"""
EMR Step 2 — Limpieza y normalización a capa Silver.

Lee bronze/padron_ruc y bronze/ordenes_compra (ya tipados, sin limpieza de
contenido) y aplica, con las funciones puras de silver_transforms.py:

  - Deduplicación de filas repetidas entre cargas de bronze.
  - Validación del RUC: debe tener exactamente 11 dígitos. Las filas con un
    RUC inválido van a silver/quarantine_<dataset>/ con su motivo, en vez de
    descartarse en silencio, y quedan disponibles para auditoría.
  - Normalización de nombres de departamento (region_normalizer), para
    poder cruzar correctamente contra EPEN/Ingresos Tributarios/PRICOS
    en 04_regional_gold.py.
  - Regla de negocio de órdenes: montos negativos se descartan.

Escribe a silver/, particionado por anio/mes.

Uso: spark-submit --py-files common.zip 02_clean_silver.py
"""

import sys

from s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_PADRON_RUC,
    SILVER_ORDENES_COMPRA,
    SILVER_ORDENES_COMPRA_QUARANTINE,
    SILVER_PADRON_RUC,
    SILVER_PADRON_RUC_QUARANTINE,
)
from silver_transforms import (
    preparar_ordenes_compra,
    preparar_padron_ruc,
    separar_ordenes_compra,
    separar_padron_ruc,
)
from spark_session_factory import create_spark_session


def _write_if_not_empty(df, path):
    """Evita escribir un parquet vacío (y el costo de un job de escritura) si no hay filas."""
    if df.take(1):
        df.write.mode("overwrite").partitionBy("anio", "mes").parquet(path)


def clean_padron_ruc(spark):
    entrada = preparar_padron_ruc(spark.read.parquet(BRONZE_PADRON_RUC))
    silver, cuarentena = separar_padron_ruc(entrada)

    silver.write.mode("overwrite").partitionBy("anio", "mes").parquet(SILVER_PADRON_RUC)
    _write_if_not_empty(cuarentena, SILVER_PADRON_RUC_QUARANTINE)


def clean_ordenes_compra(spark):
    entrada = preparar_ordenes_compra(spark.read.parquet(BRONZE_ORDENES_COMPRA))
    silver, cuarentena = separar_ordenes_compra(entrada)

    silver.write.mode("overwrite").partitionBy("anio", "mes").parquet(
        SILVER_ORDENES_COMPRA
    )
    _write_if_not_empty(cuarentena, SILVER_ORDENES_COMPRA_QUARANTINE)


def main():
    spark = create_spark_session("02_clean_silver")
    try:
        clean_padron_ruc(spark)
        clean_ordenes_compra(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
