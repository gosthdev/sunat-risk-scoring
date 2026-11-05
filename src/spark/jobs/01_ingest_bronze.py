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

from pyspark.errors import AnalysisException
from pyspark.sql.functions import coalesce, col, lit, year
from pyspark.sql.types import StructType
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


def _read_raw(spark, path, schema):
    """Lee CSV crudo en modo PERMISSIVE, agregando columna de registro corrupto.

    PERMISSIVE + columnNameOfCorruptRecord hace que las filas que no calzan
    con el esquema no se pierdan: quedan con _corrupt_record poblado en vez
    de ser descartadas por Spark sin dejar rastro.

    enforceSchema=false hace que Spark compare la cabecera del CSV con los
    nombres del esquema: si la fuente renombra, quita o reordena una columna
    el job falla aquí, en vez de mapear las columnas por posición y llenar
    bronze con datos en la columna equivocada.
    """
    # Copia: StructType.add muta el esquema y contaminaría el contrato.
    schema_con_corrupto = StructType(list(schema.fields)).add(CORRUPT_COLUMN, "string")
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .option("enforceSchema", "false")
        .schema(schema_con_corrupto)
        .csv(path)
    )


def _split_schema_quarantine(df):
    """Separa filas que Spark no pudo mapear al esquema esperado."""
    clean = df.filter(col(CORRUPT_COLUMN).isNull()).drop(CORRUPT_COLUMN)
    quarantined = df.filter(col(CORRUPT_COLUMN).isNotNull())
    return clean, quarantined


def _split_date_range_quarantine(df, date_columns):
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
    quarantined = df.filter(bad_year_condition)
    clean = df.filter(~bad_year_condition)
    return clean, quarantined


def _write_clean(df, path):
    """Escribe el DataFrame limpio particionado por anio y mes."""
    df.write.mode("overwrite").partitionBy("anio", "mes").parquet(path)


def _write_quarantine(df, path):
    """Escribe la cuarentena de esta corrida, aunque esté vacía.

    Si no se escribe cuando no hay filas, queda la cuarentena de una corrida anterior
    y 02 la cuenta como rechazos actuales (tasa_cuarentena_bronze inflada).
    """
    df.write.mode("overwrite").parquet(path)


def _log_salida(spark, nombre, path):
    """Imprime filas escritas en `path` (0 si no existe), fácil de grepear en el stdout del driver."""
    try:
        filas = spark.read.parquet(path).count()
    except AnalysisException:
        filas = 0
    print(f"[01_ingest_bronze] SALIDA {nombre}: filas={filas} ruta={path}")


def ingest_padron_ruc(spark):
    raw_df = _read_raw(spark, RAW_PADRON_RUC, PADRON_RUC_SCHEMA)
    clean_df, quarantined_df = _split_schema_quarantine(raw_df)

    _write_clean(clean_df, BRONZE_PADRON_RUC)
    _write_quarantine(quarantined_df, BRONZE_PADRON_RUC_QUARANTINE)
    _log_salida(spark, "padron_ruc", BRONZE_PADRON_RUC)
    _log_salida(spark, "padron_ruc_cuarentena", BRONZE_PADRON_RUC_QUARANTINE)


def ingest_ordenes_compra(spark):
    raw_df = _read_raw(spark, RAW_ORDENES_COMPRA, ORDENES_COMPRA_SCHEMA)
    schema_clean_df, schema_quarantined_df = _split_schema_quarantine(raw_df)

    clean_df, date_quarantined_df = _split_date_range_quarantine(
        schema_clean_df, ORDENES_COMPRA_DATE_COLUMNS
    )

    _write_clean(clean_df, BRONZE_ORDENES_COMPRA)

    # Las dos fuentes de cuarentena (error de esquema y año fuera de rango)
    # se unifican en un solo parquet de cuarentena para esta tabla.
    all_quarantined_df = schema_quarantined_df.unionByName(
        date_quarantined_df.withColumn(CORRUPT_COLUMN, lit(None).cast("string")),
        allowMissingColumns=True,
    )
    _write_quarantine(all_quarantined_df, BRONZE_ORDENES_COMPRA_QUARANTINE)
    _log_salida(spark, "ordenes_compra", BRONZE_ORDENES_COMPRA)
    _log_salida(spark, "ordenes_compra_cuarentena", BRONZE_ORDENES_COMPRA_QUARANTINE)


def main():
    spark = create_spark_session("01_ingest_bronze")
    try:
        ingest_padron_ruc(spark)
        ingest_ordenes_compra(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
