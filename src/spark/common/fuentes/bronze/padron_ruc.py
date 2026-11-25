"""Bronze del Padrón RUC: CSV crudo -> Parquet tipado por (anio, mes)."""

from bronze_io import escribir_bronze, leer_raw, separar_esquema
from s3_paths import BRONZE_PADRON_RUC, BRONZE_PADRON_RUC_QUARANTINE, RAW_PADRON_RUC
from schema_definitions import PADRON_RUC_SCHEMA


def bronze(spark, periodos):
    clean, cuarentena = separar_esquema(
        leer_raw(spark, RAW_PADRON_RUC, PADRON_RUC_SCHEMA, periodos)
    )
    escribir_bronze(
        spark,
        "padron_ruc",
        clean,
        cuarentena,
        BRONZE_PADRON_RUC,
        BRONZE_PADRON_RUC_QUARANTINE,
        periodos,
    )
