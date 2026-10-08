"""
EMR Step 4 — Agregación regional a capa Gold (gold/regional_summary), para
el Objetivo 3 del proyecto (brecha formalidad/informalidad por región).

Combina, por departamento normalizado:

  - ruc_activos: conteo de RUC en estado ACTIVO en el mes de referencia
    (por defecto la última foto mensual del Padrón RUC disponible en el
    dataset, evitando sumar las 12 fotos mensuales completas).
  - pct_informalidad: % de informalidad laboral ponderado por el factor de
    expansión FAC300_ANUAL (EPEN), solo entre personas OCUPADAS.
  - recaudacion_soles: recaudación total en soles (Ingresos Tributarios).
  - concentracion_pricos: cantidad de RUC PRICOS en el departamento. PRICOS
    no trae Departamento, así que se obtiene cruzando PRICOS con el Padrón
    RUC del mes de referencia.

DEPENDENCIA: este job necesita que silver/epen, silver/ingresos_tributarios
y silver/pricos ya existan. Esos tres los produce 00_prepare_small_datasets.py
(pandas, fuera de EMR — ver README.md), no 01/02_*.py.

No se particiona (tabla chica, ~25 filas).

Uso: spark-submit --py-files common.zip 04_regional_gold.py
"""

import os
import sys

from pyspark.sql.functions import col, count, udf, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import StringType
from region_normalizer import department_from_ccdd
from s3_paths import (
    GOLD_REGIONAL_SUMMARY,
    SILVER_EPEN,
    SILVER_INGRESOS_TRIBUTARIOS,
    SILVER_PADRON_RUC,
    SILVER_PRICOS,
)
from spark_session_factory import create_spark_session

department_from_ccdd_udf = udf(department_from_ccdd, StringType())


def obtener_padron_mes_referencia(spark, padron=None, anio_ref=None, mes_ref=None):
    """Filtra el Padrón RUC a un único mes de referencia.

    Como silver/padron_ruc almacena 12 fotos mensuales completas (no
    incrementales), es crítico fijar una foto mensual de referencia (por
    defecto la última disponible en el dataset) para no multiplicar el
    conteo de contribuyentes activos por ~12.
    """
    if padron is None:
        padron = spark.read.parquet(SILVER_PADRON_RUC)

    if anio_ref is None:
        env_anio = os.environ.get("ANIO_REFERENCIA_REGIONAL")
        if env_anio:
            anio_ref = int(env_anio)

    if mes_ref is None:
        env_mes = os.environ.get("MES_REFERENCIA_REGIONAL")
        if env_mes:
            mes_ref = int(env_mes)

    if anio_ref is None or mes_ref is None:
        ultimo = (
            padron.select(col("anio").cast("int"), col("mes").cast("int"))
            .distinct()
            .orderBy(col("anio").desc(), col("mes").desc())
            .first()
        )
        if ultimo:
            anio_ref = anio_ref if anio_ref is not None else int(ultimo["anio"])
            mes_ref = mes_ref if mes_ref is not None else int(ultimo["mes"])

    if anio_ref is not None and mes_ref is not None:
        padron_filtrado = padron.filter(
            (col("anio").cast("int") == int(anio_ref))
            & (col("mes").cast("int") == int(mes_ref))
        )
    else:
        padron_filtrado = padron

    return padron_filtrado, anio_ref, mes_ref


def ruc_activos_por_departamento(spark, padron_snapshot=None):
    if padron_snapshot is None:
        padron_snapshot, _, _ = obtener_padron_mes_referencia(spark)
    return (
        padron_snapshot.filter(col("Estado") == "ACTIVO")
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


def recaudacion_por_departamento(spark, anio_ref=None):
    ingresos = spark.read.parquet(SILVER_INGRESOS_TRIBUTARIOS)

    # Filtrar por anio_ref si la columna Anio existe
    if "Anio" in ingresos.columns and anio_ref is not None:
        ingresos = ingresos.filter(col("Anio") == int(anio_ref))

    # Excluir Total y sub-jurisdicciones de Lima para evitar duplicados con la fila Lima
    excluir = ["TOTAL", "Total", "Lima Metropolitana", "Lima Provincias"]
    ingresos_filtrados = ingresos.filter(~col("Departamento").isin(excluir))

    # Normalizar Departamento al nombre canónico
    from region_normalizer import normalize_department

    normalize_dept_udf = udf(normalize_department, StringType())
    ingresos_normalizados = ingresos_filtrados.withColumn(
        "Departamento", normalize_dept_udf(col("Departamento"))
    )

    return ingresos_normalizados.groupBy(col("Departamento")).agg(
        spark_sum("Monto_Recaudado").alias("recaudacion_soles")
    )


def concentracion_pricos_por_departamento(spark, padron_snapshot=None):
    pricos = spark.read.parquet(SILVER_PRICOS)
    if padron_snapshot is None:
        padron_snapshot, _, _ = obtener_padron_mes_referencia(spark)
    padron = padron_snapshot.select("RUC", "Departamento").distinct()

    return (
        pricos.join(padron, on="RUC", how="inner")
        .groupBy(col("Departamento"))
        .agg(count("*").alias("concentracion_pricos"))
    )


def main():
    spark = create_spark_session("04_regional_gold")
    try:
        padron_ref, anio_ref, mes_ref = obtener_padron_mes_referencia(spark)
        print(
            f"[04_regional_gold] Padrón RUC filtrado a mes de referencia: anio={anio_ref}, mes={mes_ref}"
        )

        ruc_activos = ruc_activos_por_departamento(spark, padron_ref)
        informalidad = informalidad_por_departamento(spark)
        recaudacion = recaudacion_por_departamento(spark, anio_ref)
        pricos = concentracion_pricos_por_departamento(spark, padron_ref)

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
