"""
EMR Step 1 — Ingesta a capa Bronze.

Lee los crudos de raw/padron_ruc y raw/ordenes_compra, aplica el esquema
explícito (schema_definitions.py) y escribe a Parquet en bronze/,
particionado por anio/mes. No se limpia contenido (bronze = espejo tipado
del crudo); sin embargo, las filas que:

  (a) no calzan con el esquema esperado (error de formato/tipo), o
  (b) tienen fechas con años fuera de rango válido (ver VALID_YEAR_RANGE;
      hay filas reales con años 2202/8202 en Órdenes de Compra)

se separan a bronze/quarantine/ en vez de descartarse silenciosamente o
de contaminar bronze con datos inválidos.

Asume que raw/ ya está particionado en carpetas Hive-style
(anio=2025/mes=01/...), por lo que Spark agrega "anio" y "mes" como
columnas automáticamente al leer el directorio base.

Uso: spark-submit --py-files common.zip 01_ingest_bronze.py
"""

import sys

from pyspark.sql.functions import col, lit, year
from pyspark.sql.types import StringType, StructField, StructType
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
    """Lee CSV crudo en modo PERMISSIVE, agregando columna de registro corrupto.

    PERMISSIVE + columnNameOfCorruptRecord hace que las filas que no calzan
    con el esquema no se pierdan: quedan con _corrupt_record poblado en vez
    de ser descartadas por Spark sin dejar rastro.
    """
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
    """Separa filas con año fuera de VALID_YEAR_RANGE en cualquiera de date_columns.

    Usa c.isNotNull() para asegurar que la condición sea estrictamente booleana
    y no trivaluada (SQL NULL), evitando que filas con fechas nulas sean descartadas
    tanto de clean como de quarantined.
    """
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
    """Evita escribir un parquet vacío (y el costo de un job de escritura) si no hay filas."""
    if df.take(1):
        df.write.mode("overwrite").parquet(path)


def ingest_padron_ruc(spark):
    raw_df = _read_raw(spark, RAW_PADRON_RUC, PADRON_RUC_SCHEMA)
    clean_df, quarantined_df = _split_schema_quarantine(raw_df)

    (
        clean_df.write.mode("overwrite")
        .partitionBy("anio", "mes")
        .parquet(BRONZE_PADRON_RUC)
    )
    _write_if_not_empty(quarantined_df, BRONZE_PADRON_RUC_QUARANTINE)


def ingest_ordenes_compra(spark):
    raw_df = _read_raw(spark, RAW_ORDENES_COMPRA, ORDENES_COMPRA_SCHEMA)
    schema_clean_df, schema_quarantined_df = _split_schema_quarantine(raw_df)

    clean_df, date_quarantined_df = _split_date_range_quarantine(
        schema_clean_df, ORDENES_COMPRA_DATE_COLUMNS
    )

    (
        clean_df.write.mode("overwrite")
        .partitionBy("anio", "mes")
        .parquet(BRONZE_ORDENES_COMPRA)
    )

    # Las dos fuentes de cuarentena (error de esquema y año fuera de rango)
    # se unifican en un solo parquet de cuarentena para esta tabla.
    all_quarantined_df = schema_quarantined_df.unionByName(
        date_quarantined_df.withColumn(CORRUPT_COLUMN, lit(None).cast("string")),
        allowMissingColumns=True,
    )
    _write_if_not_empty(all_quarantined_df, BRONZE_ORDENES_COMPRA_QUARANTINE)


def main():
    spark = create_spark_session("01_ingest_bronze")
    try:
        ingest_padron_ruc(spark)
        ingest_ordenes_compra(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
