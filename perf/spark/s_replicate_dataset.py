"""Genera las copias 1..factor-1 del RAW real en un bucket de pruebas (-perf-).

La copia 0 (el RAW tal cual) la copia antes perf/scripts/replicar.sh; este job
solo agrega las demás en las mismas carpetas anio=AAAA/mes=MM.

Lee el RAW con el mismo esquema que el Job 01 y conserva solo las filas que ese
esquema acepta. Las filas que el Job 01 manda a cuarentena por esquema (1.525 del
Padrón) quedan solo en la copia 0, para no multiplicar un defecto de la fuente.
Las filas con fecha fuera de rango sí se copian: son válidas por esquema.

Uso en EMR (con --py-files common.zip,perf_keys.py):
    s_replicate_dataset.py --factor 10 --destino sunat-risk-scoring-perf-x10
"""

import argparse
import json
import sys

from perf_keys import (
    NRO_ORDEN_DESPLAZAMIENTO,
    RUC_INICIO_COPIAS,
    tamano_bloque,
    validar_factor,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, concat, lit, trim, when
from pyspark.sql.types import StructType
from schema_definitions import ORDENES_COMPRA_SCHEMA, PADRON_RUC_SCHEMA

RAW_PRODUCCION = "s3://sunat-risk-scoring-raw"
CORRUPTO = "_corrupt_record"
# ~256 MB por archivo: una fila del Padrón pesa ~260 B y una de Órdenes ~490 B.
FILAS_POR_ARCHIVO = {"padron_ruc": 1_000_000, "ordenes_compra": 500_000}
RUC_11 = r"^[0-9]{11}$"


def _leer(spark: SparkSession, ruta: str, esquema: StructType) -> DataFrame:
    """Igual que _read_raw del Job 01, quedándose con las filas sin error de esquema."""
    con_corrupto = StructType(list(esquema.fields)).add(CORRUPTO, "string")
    df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPTO)
        .option("enforceSchema", "false")
        .schema(con_corrupto)
        .csv(ruta)
    )
    return df.filter(col(CORRUPTO).isNull()).drop(CORRUPTO)


def tabla_rank(padron: DataFrame, ordenes: DataFrame) -> DataFrame:
    """Numera 0..N-1 los RUC distintos de 11 dígitos (Padrón ∪ contratistas)."""
    contratistas = ordenes.select(trim(col("ruc_contratista")).alias("r")).where(
        col("r").rlike(RUC_11)
    )
    rucs = (
        padron.select(col("RUC").alias("r"))
        .union(contratistas.select(col("r").cast("long")))
        .where(col("r").between(10**10, 10**11 - 1))
        .distinct()
    )
    return (
        rucs.rdd.map(lambda fila: fila[0])
        .sortBy(lambda r: r)
        .zipWithIndex()
        .toDF(["_ruc_base", "_rank"])
    )


def _con_copias(df: DataFrame, factor: int) -> DataFrame:
    copias = df.sparkSession.range(1, factor).withColumnRenamed("id", "_copia")
    return df.crossJoin(broadcast(copias))


def _ruc_nuevo(bloque: int):
    return lit(RUC_INICIO_COPIAS) + (col("_copia") - 1) * lit(bloque) + col("_rank")


def copias_padron(padron: DataFrame, rank: DataFrame, factor: int, bloque: int):
    df = padron.join(broadcast(rank), padron["RUC"] == rank["_ruc_base"], "left")
    df = _con_copias(df, factor).withColumn(
        "RUC", when(col("_rank").isNull(), col("RUC")).otherwise(_ruc_nuevo(bloque))
    )
    return df.select(*PADRON_RUC_SCHEMA.fieldNames(), "anio", "mes")


def copias_ordenes(ordenes: DataFrame, rank: DataFrame, factor: int, bloque: int):
    clave = when(
        trim(col("ruc_contratista")).rlike(RUC_11),
        trim(col("ruc_contratista")).cast("long"),
    )
    df = ordenes.join(broadcast(rank), clave == rank["_ruc_base"], "left")
    df = (
        _con_copias(df, factor)
        .withColumn(
            "ruc_contratista",
            when(col("_rank").isNull(), col("ruc_contratista")).otherwise(
                _ruc_nuevo(bloque).cast("string")
            ),
        )
        .withColumn(
            "nro_de_orden",
            col("nro_de_orden") + col("_copia") * lit(NRO_ORDEN_DESPLAZAMIENTO),
        )
        .withColumn(
            "orden", concat(col("orden"), lit("-R"), col("_copia").cast("string"))
        )
    )
    return df.select(*ORDENES_COMPRA_SCHEMA.fieldNames(), "anio", "mes")


def _escribir(df: DataFrame, ruta: str, filas_por_archivo: int) -> None:
    # Al escribir, Spark recorta espacios por defecto; aquí no, para no alterar el dato.
    (
        df.write.mode("append")
        .partitionBy("anio", "mes")
        .option("header", "true")
        .option("maxRecordsPerFile", filas_por_archivo)
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .csv(ruta)
    )


def replicar(spark: SparkSession, origen: str, destino: str, factor: int) -> dict:
    """Agrega las copias 1..factor-1 de origen/{padron_ruc,ordenes_compra} en destino."""
    if "-perf-" not in destino and not destino.startswith("file:"):
        raise ValueError(f"SEGURIDAD: el destino '{destino}' no es un bucket -perf-.")
    # Conserva "mes=01" tal cual al reescribir las carpetas (sin inferir enteros).
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    padron = _leer(spark, f"{origen}/padron_ruc", PADRON_RUC_SCHEMA)
    ordenes = _leer(spark, f"{origen}/ordenes_compra", ORDENES_COMPRA_SCHEMA)
    rank = tabla_rank(padron, ordenes).cache()
    n_rucs = rank.count()
    bloque = tamano_bloque(n_rucs)
    validar_factor(factor, bloque)

    if factor > 1:
        _escribir(
            copias_padron(padron, rank, factor, bloque),
            f"{destino}/padron_ruc",
            FILAS_POR_ARCHIVO["padron_ruc"],
        )
        _escribir(
            copias_ordenes(ordenes, rank, factor, bloque),
            f"{destino}/ordenes_compra",
            FILAS_POR_ARCHIVO["ordenes_compra"],
        )
    return {"factor": factor, "rucs_distintos": n_rucs, "bloque": bloque}


def main(argv=None) -> int:
    from spark_session_factory import create_spark_session

    parser = argparse.ArgumentParser()
    parser.add_argument("--factor", type=int, required=True)
    parser.add_argument("--destino", required=True, help="bucket -perf- de destino")
    args = parser.parse_args(argv)

    spark = create_spark_session(f"perf_replicar_x{args.factor}")
    try:
        reporte = replicar(
            spark, RAW_PRODUCCION, f"s3://{args.destino}/raw", args.factor
        )
        print("PERF_REPLICACION " + json.dumps(reporte))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
