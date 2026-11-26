"""Silver del Padrón RUC: dedupe por (RUC, PERIODO_PUBLICACION), RUC válido y departamento normalizado."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim
from s3_paths import (
    BRONZE_PADRON_RUC,
    BRONZE_PADRON_RUC_QUARANTINE,
    SILVER_PADRON_RUC,
    SILVER_PADRON_RUC_QUARANTINE,
)
from silver_transforms import (
    MOTIVO_COL,
    MOTIVO_RUC_INVALIDO,
    normalize_department_udf,
    procesar,
    ruc_valido,
)


def preparar_padron_ruc(bronze: DataFrame) -> DataFrame:
    """Deduplica las fotos del Padrón por (RUC, PERIODO_PUBLICACION)."""
    clave = ["RUC", "PERIODO_PUBLICACION"]
    return bronze.repartition(*clave).dropDuplicates(clave)


def separar_padron_ruc(entrada: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devuelve (silver, cuarentena) según la validez del RUC."""
    valido = ruc_valido(col("RUC"))
    silver = entrada.filter(valido).withColumn(
        "Departamento", normalize_department_udf(trim(col("Departamento")))
    )
    cuarentena = entrada.filter(~valido).withColumn(
        MOTIVO_COL, lit(MOTIVO_RUC_INVALIDO)
    )
    return silver, cuarentena


def silver(spark, periodos):
    procesar(
        spark,
        "padron_ruc",
        BRONZE_PADRON_RUC,
        BRONZE_PADRON_RUC_QUARANTINE,
        preparar_padron_ruc,
        separar_padron_ruc,
        SILVER_PADRON_RUC,
        SILVER_PADRON_RUC_QUARANTINE,
        periodos,
    )
