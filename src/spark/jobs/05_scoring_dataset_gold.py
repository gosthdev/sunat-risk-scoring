"""
EMR Step 5 — Dataset de entrenamiento (gold/scoring_dataset).

Une gold/ruc_features con el label SSCO (silver/ssco) y filtra a la
población objetivo (RUC que contratan con el Estado, ya reflejado en
cantidad_contratos_estado > 0).

También corre el cruce de cobertura pendiente: cuántos RUC de la lista
SSCO aparecen en ruc_features (y por lo tanto son etiquetables). Lo
imprime en el log del step en vez de solo escribirlo, porque es la
validación que faltaba antes de cerrar el diseño del modelo.

Uso: spark-submit --py-files common.zip 05_scoring_dataset_gold.py
"""

import sys

from pyspark.sql.functions import col, when
from s3_paths import GOLD_RUC_FEATURES, GOLD_SCORING_DATASET, SILVER_SSCO

# Población objetivo: TODO el Padrón, no solo quienes contratan con el
# Estado (cruce de cobertura dio 45/766 SSCO con contratos vs 764/766 en
# el Padrón). Contratación con el Estado queda como FEATURE
# (cantidad_contratos_estado, monto_total_contratado_estado), no como
# filtro de población.
from spark_session_factory import create_spark_session


def main():
    spark = create_spark_session("05_scoring_dataset_gold")
    try:
        features = spark.read.parquet(GOLD_RUC_FEATURES)
        ssco = spark.read.parquet(SILVER_SSCO)

        total_ssco = ssco.count()
        ssco_en_features = (
            ssco.join(features, on="RUC", how="inner").select("RUC").distinct().count()
        )
        print(
            f"[cobertura SSCO] {ssco_en_features}/{total_ssco} RUC de la lista SSCO aparecen en gold/ruc_features"
        )

        dataset = features.join(ssco, on="RUC", how="left").withColumn(
            "es_ssco", when(col("es_ssco").isNull(), False).otherwise(col("es_ssco"))
        )

        (
            dataset.write.mode("overwrite")
            .partitionBy("mes_referencia")
            .parquet(GOLD_SCORING_DATASET)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
