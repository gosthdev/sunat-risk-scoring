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

Antes de procesar valida el esquema de bronze contra el contrato del dataset
(schema_contracts.py): un renombre o cambio de tipo falla sin tocar Silver.

Además publica silver/quality_report/ con la tasa de cuarentena por fuente,
año y mes. Si la tasa por RUC supera MAX_TASA_CUARENTENA_RUC (por defecto
0.5) el job aborta antes de escribir Silver: indica un cambio de formato en
la fuente, no ruido de datos.

Escribe a silver/, particionado por anio/mes.

Uso: spark-submit --py-files common.zip 02_clean_silver.py
"""

import os
import sys

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel
from s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_ORDENES_COMPRA_QUARANTINE,
    BRONZE_PADRON_RUC,
    BRONZE_PADRON_RUC_QUARANTINE,
    SILVER_ORDENES_COMPRA,
    SILVER_ORDENES_COMPRA_QUARANTINE,
    SILVER_PADRON_RUC,
    SILVER_PADRON_RUC_QUARANTINE,
    SILVER_QUALITY_REPORT,
)
from schema_contracts import validar_esquema
from silver_transforms import (
    construir_reporte_calidad,
    preparar_ordenes_compra,
    preparar_padron_ruc,
    separar_ordenes_compra,
    separar_padron_ruc,
    verificar_tasa_cuarentena,
)
from spark_session_factory import create_spark_session

MAX_TASA_CUARENTENA_RUC = float(os.environ.get("MAX_TASA_CUARENTENA_RUC", "0.5"))


def _write_if_not_empty(df, path):
    """Evita escribir un parquet vacío (y el costo de un job de escritura) si no hay filas."""
    if df.take(1):
        df.write.mode("overwrite").partitionBy("anio", "mes").parquet(path)


def _read_optional(spark, path) -> DataFrame | None:
    """Lee un parquet que puede no existir (bronze no escribe cuarentena vacía)."""
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        return None


def _log_reporte(reporte: DataFrame) -> None:
    # Línea fácil de grepear en los logs del driver (stdout).
    for r in reporte.orderBy("fuente", "anio", "mes").collect():
        print(
            f"CALIDAD fuente={r['fuente']} anio={r['anio']} mes={r['mes']} "
            f"bronze={r['filas_bronze']} cuarentena_bronze={r['filas_cuarentena_bronze']} "
            f"tasa_bronze={r['tasa_cuarentena_bronze']:.4f} "
            f"cuarentena_ruc={r['filas_cuarentena_ruc']} "
            f"tasa_ruc={r['tasa_cuarentena_ruc']:.4f} silver={r['filas_silver']}"
        )


def _procesar(
    spark,
    fuente,
    bronze_path,
    bronze_quarantine_path,
    preparar,
    separar,
    silver_path,
    silver_quarantine_path,
):
    bronze = spark.read.parquet(bronze_path)
    # Contrato de esquema: un cambio en la fuente falla acá, antes de escribir Silver.
    validar_esquema(bronze.schema, fuente)
    entrada = preparar(bronze).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        silver, cuarentena = separar(entrada)
        reporte = construir_reporte_calidad(
            fuente,
            bronze,
            entrada,
            silver,
            cuarentena,
            _read_optional(spark, bronze_quarantine_path),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            _log_reporte(reporte)
            # Antes de escribir nada: si la fuente cambió de formato, Silver no se toca.
            verificar_tasa_cuarentena(reporte, MAX_TASA_CUARENTENA_RUC)

            silver.write.mode("overwrite").partitionBy("anio", "mes").parquet(
                silver_path
            )
            _write_if_not_empty(cuarentena, silver_quarantine_path)
            reporte.write.mode("overwrite").partitionBy("fuente").parquet(
                SILVER_QUALITY_REPORT
            )
        finally:
            reporte.unpersist()
    finally:
        entrada.unpersist()


def clean_padron_ruc(spark):
    _procesar(
        spark,
        "padron_ruc",
        BRONZE_PADRON_RUC,
        BRONZE_PADRON_RUC_QUARANTINE,
        preparar_padron_ruc,
        separar_padron_ruc,
        SILVER_PADRON_RUC,
        SILVER_PADRON_RUC_QUARANTINE,
    )


def clean_ordenes_compra(spark):
    _procesar(
        spark,
        "ordenes_compra",
        BRONZE_ORDENES_COMPRA,
        BRONZE_ORDENES_COMPRA_QUARANTINE,
        preparar_ordenes_compra,
        separar_ordenes_compra,
        SILVER_ORDENES_COMPRA,
        SILVER_ORDENES_COMPRA_QUARANTINE,
    )


def main():
    spark = create_spark_session("02_clean_silver")
    try:
        clean_padron_ruc(spark)
        clean_ordenes_compra(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
