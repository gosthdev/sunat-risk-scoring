"""
EMR Step 4 — Agregación regional a capa Gold (gold/regional_summary), para
el Objetivo 3 del proyecto (brecha formalidad/informalidad por región).

Combina, por departamento normalizado:

  - ruc_activos: conteo de RUC en estado ACTIVO (Padrón RUC).
  - pct_informalidad: % de informalidad laboral ponderado por el factor de
    expansión FAC300_ANUAL (EPEN), solo entre personas OCUPADAS.
  - recaudacion_soles: recaudación total en soles (Ingresos Tributarios).
  - concentracion_pricos: cantidad de RUC PRICOS en el departamento. PRICOS
    no trae Departamento, así que se obtiene cruzando PRICOS con Padrón RUC.

DEPENDENCIA: este job necesita que silver/epen, silver/ingresos_tributarios
y silver/pricos ya existan. Esos tres los produce 00_prepare_small_datasets.py
(pandas, fuera de EMR — ver README.md), no 01/02_*.py.

No se particiona (tabla chica, ~25 filas).

Uso: spark-submit --py-files common.zip 04_regional_gold.py
"""

import sys

from pyspark.sql.functions import col, count, udf, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import StringType

from spark.common.region_normalizer import department_from_ccdd
from spark.common.s3_paths import (
    GOLD_REGIONAL_SUMMARY,
    SILVER_EPEN,
    SILVER_INGRESOS_TRIBUTARIOS,
    SILVER_PADRON_RUC,
    SILVER_PRICOS,
)
from spark.common.spark_session_factory import create_spark_session

department_from_ccdd_udf = udf(department_from_ccdd, StringType())


def ruc_activos_por_departamento(spark):
    padron = spark.read.parquet(SILVER_PADRON_RUC)
    return (
        padron.filter(col("Estado") == "ACTIVO")
        .groupBy(col("Departamento"))
        .agg(count("*").alias("ruc_activos"))
    )


def informalidad_por_departamento(spark):
    epen = spark.read.parquet(SILVER_EPEN)
    epen = epen.withColumn("Departamento", department_from_ccdd_udf(col("CCDD")))

    # OCUP300 == 1 -> Ocupado. La informalidad solo tiene sentido sobre la
    # población ocupada (Informal_P viene vacío para el resto).
    ocupados = epen.filter(col("OCUP300") == 1)

    agregados = ocupados.groupBy("Departamento").agg(
        spark_sum(
            when(col("Informal_P") == 1, col("FAC300_ANUAL")).otherwise(0.0)
        ).alias("expansion_informal"),
        spark_sum("FAC300_ANUAL").alias("expansion_total"),
    )

    return agregados.withColumn(
        "pct_informalidad",
        (col("expansion_informal") / col("expansion_total")) * 100,
    ).select("Departamento", "pct_informalidad")


def recaudacion_por_departamento(spark):
    ingresos = spark.read.parquet(SILVER_INGRESOS_TRIBUTARIOS)
    return (
        ingresos.filter(col("Departamento") != "TOTAL")
        .groupBy(col("Departamento"))
        .agg(spark_sum("Monto_Recaudado").alias("recaudacion_soles"))
    )


def concentracion_pricos_por_departamento(spark):
    pricos = spark.read.parquet(SILVER_PRICOS)
    padron = (
        spark.read.parquet(SILVER_PADRON_RUC).select("RUC", "Departamento").distinct()
    )

    return (
        pricos.join(padron, on="RUC", how="inner")
        .groupBy(col("Departamento"))
        .agg(count("*").alias("concentracion_pricos"))
    )


def main():
    spark = create_spark_session("04_regional_gold")
    try:
        ruc_activos = ruc_activos_por_departamento(spark)
        informalidad = informalidad_por_departamento(spark)
        recaudacion = recaudacion_por_departamento(spark)
        pricos = concentracion_pricos_por_departamento(spark)

        resumen = (
            ruc_activos.join(informalidad, on="Departamento", how="outer")
            .join(recaudacion, on="Departamento", how="outer")
            .join(pricos, on="Departamento", how="outer")
            .na.fill(
                0, subset=["ruc_activos", "recaudacion_soles", "concentracion_pricos"]
            )
        )

        resumen.coalesce(1).write.mode("overwrite").parquet(GOLD_REGIONAL_SUMMARY)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
