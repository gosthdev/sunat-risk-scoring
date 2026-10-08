"""
EMR Step 3 — Feature engineering a nivel RUC (capa Gold: gold/ruc_features).

Construye, por RUC, las features de entrada para el modelo de scoring SSCO:

  - antiguedad_contratacion_estado_dias: días desde la primera orden de
    compra registrada para ese RUC hasta el fin del mes_referencia (corte
    mensual determinista).
    LIMITACIÓN CONOCIDA (ver README.md): el Padrón RUC no trae fecha de
    inscripción, así que esto NO es la antigüedad real del RUC — es un
    proxy de "hace cuánto contrata con el Estado", y queda nulo para los
    RUC que nunca contrataron con el Estado o cuya primera contratación
    sea posterior al mes_referencia.
  - actividad_economica_principal: pasada directamente del Padrón RUC.
  - monto_total_contratado_estado / cantidad_contratos_estado: agregados
    desde Órdenes de Compra.
  - es_prico: flag de pertenencia al padrón de Principales Contribuyentes.

El join de montos es por ruc_contratista (ordenes_compra) == RUC
(padron_ruc). ruc_contratista a veces es un DNI (persona natural, no RUC);
esas filas no calzan con ningún RUC del padrón y quedan fuera del join
a propósito, porque el scoring es sobre RUC/empresas, no personas.

Uso: spark-submit --py-files common.zip 03_feature_gold.py
"""

import sys

from pyspark.sql.functions import (
    col,
    concat,
    count,
    datediff,
    format_string,
    last_day,
    lit,
    to_date,
    when,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from s3_paths import (
    GOLD_RUC_FEATURES,
    SILVER_ORDENES_COMPRA,
    SILVER_PADRON_RUC,
    SILVER_PRICOS,
)
from spark_session_factory import create_spark_session


def build_contratacion_estado_features(spark):
    ordenes = spark.read.parquet(SILVER_ORDENES_COMPRA)

    ordenes = ordenes.withColumn("RUC", col("ruc_contratista").cast("long"))
    # Descarta contratistas identificados por DNI (personas naturales), no
    # son RUC y no deben entrar al join de la capa de features.
    ordenes = ordenes.filter(col("RUC").isNotNull())

    return ordenes.groupBy("RUC").agg(
        spark_sum("monto_total_orden_original").alias("monto_total_contratado_estado"),
        count("*").alias("cantidad_contratos_estado"),
        spark_min("fecha_de_emision").alias("fecha_primera_orden"),
    )


def build_prico_flag(spark):
    pricos = spark.read.parquet(SILVER_PRICOS)
    return (
        pricos.select(col("RUC"))
        .distinct()
        .withColumn("es_prico", col("RUC").isNotNull())
    )


def main():
    spark = create_spark_session("03_feature_gold")
    try:
        padron = spark.read.parquet(SILVER_PADRON_RUC)
        contratacion = build_contratacion_estado_features(spark)
        pricos = build_prico_flag(spark)

        # Fecha de referencia determinista por mes (último día del mes_referencia).
        # Evita usar current_date() para que el pipeline sea reproducible y para
        # que cada partición mensual refleje la antigüedad a su fecha de corte.
        fecha_ref_mes = last_day(
            to_date(concat(col("mes_referencia"), lit("01")), "yyyyMMdd")
        )

        features = (
            padron.select(
                col("RUC"),
                col("Actividad_Economica_CIIU_revision4_Principal").alias(
                    "actividad_economica_principal"
                ),
                col("Estado"),
                col("Condicion"),
                format_string("%d%02d", col("anio"), col("mes")).alias(
                    "mes_referencia"
                ),
            )
            .join(contratacion, on="RUC", how="left")
            .join(pricos, on="RUC", how="left")
            .withColumn(
                "antiguedad_contratacion_estado_dias",
                datediff(fecha_ref_mes, col("fecha_primera_orden")),
            )
            .withColumn(
                "antiguedad_contratacion_estado_dias",
                when(col("antiguedad_contratacion_estado_dias") < 0, None).otherwise(
                    col("antiguedad_contratacion_estado_dias")
                ),
            )
            .drop("fecha_primera_orden")
            .withColumn(
                "es_prico",
                when(col("es_prico").isNull(), False).otherwise(col("es_prico")),
            )
            .withColumn(
                "monto_total_contratado_estado",
                when(col("monto_total_contratado_estado").isNull(), 0.0).otherwise(
                    col("monto_total_contratado_estado")
                ),
            )
            .withColumn(
                "cantidad_contratos_estado",
                when(col("cantidad_contratos_estado").isNull(), 0).otherwise(
                    col("cantidad_contratos_estado")
                ),
            )
        )

        (
            features.write.mode("overwrite")
            .partitionBy("mes_referencia")
            .parquet(GOLD_RUC_FEATURES)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
