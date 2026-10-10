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

Argumentos opcionales (usados por infra/scripts/07_run_benchmark.sh para
comparar escritura particionada vs. sin particionar; sin argumentos el
comportamiento es el de producción: particionado y salida en bronze/):

  --partition-mode {partitioned,unpartitioned}   (default: partitioned)
  --output-root s3://bucket/prefix               (default: rutas BRONZE_* de s3_paths)
"""

import argparse
import sys
import time

from pyspark.sql.functions import col, lit, year
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
    """
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(schema.add(CORRUPT_COLUMN, "string"))
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
        bad_year_condition = bad_year_condition | (
            (year(col(date_col)) < min_year) | (year(col(date_col)) > max_year)
        )
    quarantined = df.filter(bad_year_condition)
    clean = df.filter(~bad_year_condition)
    return clean, quarantined


def _write_clean(df, path, partitioned):
    """Escribe el DataFrame limpio, con o sin partitionBy("anio", "mes").

    Sin particionar, "anio" y "mes" se conservan igual como columnas
    normales dentro de los archivos Parquet.
    """
    writer = df.write.mode("overwrite")
    if partitioned:
        writer = writer.partitionBy("anio", "mes")
    writer.parquet(path)


def _write_if_not_empty(df, path):
    """Evita escribir un parquet vacío (y el costo de un job de escritura) si no hay filas."""
    if df.take(1):
        df.write.mode("overwrite").parquet(path)


def _resolve_paths(output_root):
    """Rutas de salida: las de producción, o un árbol aislado si hay --output-root."""
    if output_root is None:
        return {
            "padron": BRONZE_PADRON_RUC,
            "padron_q": BRONZE_PADRON_RUC_QUARANTINE,
            "oc": BRONZE_ORDENES_COMPRA,
            "oc_q": BRONZE_ORDENES_COMPRA_QUARANTINE,
        }
    root = output_root.rstrip("/")
    return {
        "padron": f"{root}/padron_ruc",
        "padron_q": f"{root}/quarantine/padron_ruc",
        "oc": f"{root}/ordenes_compra",
        "oc_q": f"{root}/quarantine/ordenes_compra",
    }


def ingest_padron_ruc(spark, paths, partitioned=True):
    raw_df = _read_raw(spark, RAW_PADRON_RUC, PADRON_RUC_SCHEMA)
    clean_df, quarantined_df = _split_schema_quarantine(raw_df)

    _write_clean(clean_df, paths["padron"], partitioned)
    _write_if_not_empty(quarantined_df, paths["padron_q"])


def ingest_ordenes_compra(spark, paths, partitioned=True):
    raw_df = _read_raw(spark, RAW_ORDENES_COMPRA, ORDENES_COMPRA_SCHEMA)
    schema_clean_df, schema_quarantined_df = _split_schema_quarantine(raw_df)

    clean_df, date_quarantined_df = _split_date_range_quarantine(
        schema_clean_df, ORDENES_COMPRA_DATE_COLUMNS
    )

    _write_clean(clean_df, paths["oc"], partitioned)

    # Las dos fuentes de cuarentena (error de esquema y año fuera de rango)
    # se unifican en un solo parquet de cuarentena para esta tabla.
    all_quarantined_df = schema_quarantined_df.unionByName(
        date_quarantined_df.withColumn(CORRUPT_COLUMN, lit(None).cast("string")),
        allowMissingColumns=True,
    )
    _write_if_not_empty(all_quarantined_df, paths["oc_q"])


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Ingesta raw -> bronze")
    parser.add_argument(
        "--partition-mode",
        choices=["partitioned", "unpartitioned"],
        default="partitioned",
    )
    parser.add_argument("--output-root", default=None)
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    partitioned = args.partition_mode == "partitioned"
    paths = _resolve_paths(args.output_root)

    spark = create_spark_session("01_ingest_bronze")
    start = time.perf_counter()
    try:
        ingest_padron_ruc(spark, paths, partitioned)
        ingest_ordenes_compra(spark, paths, partitioned)
    finally:
        elapsed = time.perf_counter() - start
        # Línea fácil de grepear en los logs del driver (stdout).
        print(
            f"BENCHMARK_RESULT mode={args.partition_mode} elapsed_seconds={elapsed:.2f}"
        )
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
