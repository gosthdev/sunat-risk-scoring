"""
EMR Step 2 — Limpieza y normalización a capa Silver.

Lee bronze/padron_ruc y bronze/ordenes_compra (ya tipados, sin limpieza de
contenido) y aplica:

  - Deduplicación de filas repetidas entre cargas de bronze.
  - Normalización de nombres de departamento (region_normalizer), para
    poder cruzar correctamente contra EPEN/Ingresos Tributarios/PRICOS
    en 04_regional_gold.py.
  - Filtrado de filas con claves de negocio inválidas (RUC nulo, montos
    negativos). A diferencia de la cuarentena de bronze, esto NO es error
    de formato/esquema sino de contenido, así que simplemente se descarta
    en vez de aislarse (ya pasó la validación de esquema en bronze).

Escribe a silver/, particionado por anio/mes.

Uso: spark-submit --py-files common.zip 02_clean_silver.py
"""

import sys

from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import StringType

from spark.common.region_normalizer import normalize_department
from spark.common.s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_PADRON_RUC,
    SILVER_ORDENES_COMPRA,
    SILVER_PADRON_RUC,
)
from spark.common.spark_session_factory import create_spark_session

normalize_department_udf = udf(normalize_department, StringType())


def clean_padron_ruc(spark):
    df = spark.read.parquet(BRONZE_PADRON_RUC)

    df = df.dropDuplicates(["RUC", "PERIODO_PUBLICACION"])
    df = df.filter(col("RUC").isNotNull())
    df = df.withColumn(
        "Departamento", normalize_department_udf(trim(col("Departamento")))
    )

    (df.write.mode("overwrite").partitionBy("anio", "mes").parquet(SILVER_PADRON_RUC))


def clean_ordenes_compra(spark):
    df = spark.read.parquet(BRONZE_ORDENES_COMPRA)

    df = df.dropDuplicates(["orden"])
    df = df.filter(col("ruc_entidad").isNotNull())
    df = df.filter(col("monto_total_orden_original") >= 0)
    df = df.withColumn(
        "departamento_entidad",
        normalize_department_udf(trim(col("departamento_entidad"))),
    )

    (
        df.write.mode("overwrite")
        .partitionBy("anio", "mes")
        .parquet(SILVER_ORDENES_COMPRA)
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
