"""Bronze de Órdenes de Compra: CSV crudo -> Parquet tipado por (anio, mes)."""

from bronze_io import CORRUPT_COLUMN, escribir_bronze, leer_raw, separar_esquema
from pyspark.sql.functions import coalesce, col, lit, year
from s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_ORDENES_COMPRA_QUARANTINE,
    RAW_ORDENES_COMPRA,
)
from schema_definitions import (
    ORDENES_COMPRA_DATE_COLUMNS,
    ORDENES_COMPRA_SCHEMA,
    VALID_YEAR_RANGE,
)


def separar_rango_fechas(df, date_columns):
    """Separa filas con año fuera de VALID_YEAR_RANGE en cualquiera de date_columns.

    Una fecha nula no es un año fuera de rango: la fila se queda en clean. Sin el
    coalesce, `NULL` hacía que ni filter(cond) ni filter(~cond) la conservaran.
    """
    min_year, max_year = VALID_YEAR_RANGE
    bad_year_condition = lit(False)
    for date_col in date_columns:
        bad_year_condition = bad_year_condition | coalesce(
            (year(col(date_col)) < min_year) | (year(col(date_col)) > max_year),
            lit(False),
        )
    return df.filter(~bad_year_condition), df.filter(bad_year_condition)


def bronze(spark, periodos):
    raw_df = leer_raw(spark, RAW_ORDENES_COMPRA, ORDENES_COMPRA_SCHEMA, periodos)
    schema_clean_df, schema_quarantined_df = separar_esquema(raw_df)
    clean_df, date_quarantined_df = separar_rango_fechas(
        schema_clean_df, ORDENES_COMPRA_DATE_COLUMNS
    )
    # Error de esquema y año fuera de rango se unifican en una sola cuarentena.
    cuarentena = schema_quarantined_df.unionByName(
        date_quarantined_df.withColumn(CORRUPT_COLUMN, lit(None).cast("string")),
        allowMissingColumns=True,
    )
    escribir_bronze(
        spark,
        "ordenes_compra",
        clean_df,
        cuarentena,
        BRONZE_ORDENES_COMPRA,
        BRONZE_ORDENES_COMPRA_QUARANTINE,
        periodos,
    )
