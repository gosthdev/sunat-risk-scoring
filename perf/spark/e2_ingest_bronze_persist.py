"""Experimento E2 — Ingesta Bronze con persistencia MEMORY_AND_DISK.

Variante del Job 01 que persiste el DataFrame crudo en memoria/disco
tras la primera lectura, evitando que la evaluación de cuarentena (_write_if_not_empty / take(1))
dispare una segunda lectura completa del archivo CSV crudo.

Mantiene las mismas firmas ingest_padron_ruc, ingest_ordenes_compra y main.
"""

import sys

from pyspark.sql.functions import col, lit, year
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.storagelevel import StorageLevel
from s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_ORDENES_COMPRA_QUARANTINE,
    BRONZE_PADRON_RUC,
    BRONZE_PADRON_RUC_QUARANTINE,
    RAW_ORDENES_COMPRA,
    RAW_PADRON_RUC,
)
from schema_definitions import (
    ORDENES_COMPRA_DATE_COLUMNS,
    ORDENES_COMPRA_SCHEMA,
    PADRON_RUC_SCHEMA,
    VALID_YEAR_RANGE,
)
from spark_session_factory import create_spark_session

CORRUPT_COLUMN = "_corrupt_record"


def _schema_with_corrupt(schema: StructType) -> StructType:
    """Retorna una copia del esquema incluyendo la columna corrupta sin mutar el original."""
    if CORRUPT_COLUMN in schema.fieldNames():
        return schema
    return StructType(
        list(schema.fields) + [StructField(CORRUPT_COLUMN, StringType(), True)]
    )


def _read_raw(spark, path, schema):
    """Lee CSV crudo en modo PERMISSIVE, agregando columna de registro corrupto."""
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(_schema_with_corrupt(schema))
        .csv(path)
    )


def _split_schema_quarantine(df):
    """Separa filas que Spark no pudo mapear al esquema esperado."""
    clean = df.filter(col(CORRUPT_COLUMN).isNull()).drop(CORRUPT_COLUMN)
    quarantined = df.filter(col(CORRUPT_COLUMN).isNotNull())
    return clean, quarantined


def _split_date_range_quarantine(df, date_columns):
    """Separa filas con año fuera de VALID_YEAR_RANGE en cualquiera de date_columns."""
    min_year, max_year = VALID_YEAR_RANGE
    bad_year_condition = lit(False)
    for date_col in date_columns:
        c = col(date_col)
        bad_year_condition = bad_year_condition | (
            c.isNotNull() & ((year(c) < min_year) | (year(c) > max_year))
        )
    quarantined = df.filter(bad_year_condition)
    clean = df.filter(~bad_year_condition)
    return clean, quarantined


def _write_if_not_empty(df, path):
    """Evita escribir un parquet vacío si no hay filas."""
    if df.take(1):
        df.write.mode("overwrite").parquet(path)


def ingest_padron_ruc(spark):
    raw_df = _read_raw(spark, RAW_PADRON_RUC, PADRON_RUC_SCHEMA)
    raw_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        clean_df, quarantined_df = _split_schema_quarantine(raw_df)

        (
            clean_df.write.mode("overwrite")
            .partitionBy("anio", "mes")
            .parquet(BRONZE_PADRON_RUC)
        )
        _write_if_not_empty(quarantined_df, BRONZE_PADRON_RUC_QUARANTINE)
    finally:
        raw_df.unpersist()


def ingest_ordenes_compra(spark):
    raw_df = _read_raw(spark, RAW_ORDENES_COMPRA, ORDENES_COMPRA_SCHEMA)
    raw_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        schema_clean_df, schema_quarantined_df = _split_schema_quarantine(raw_df)

        clean_df, date_quarantined_df = _split_date_range_quarantine(
            schema_clean_df, ORDENES_COMPRA_DATE_COLUMNS
        )

        (
            clean_df.write.mode("overwrite")
            .partitionBy("anio", "mes")
            .parquet(BRONZE_ORDENES_COMPRA)
        )

        all_quarantined_df = schema_quarantined_df.unionByName(
            date_quarantined_df.withColumn(CORRUPT_COLUMN, lit(None).cast("string")),
            allowMissingColumns=True,
        )
        _write_if_not_empty(all_quarantined_df, BRONZE_ORDENES_COMPRA_QUARANTINE)
    finally:
        raw_df.unpersist()


def main():
    spark = create_spark_session("e2_ingest_bronze_persist")
    try:
        ingest_padron_ruc(spark)
        ingest_ordenes_compra(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
