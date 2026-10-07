"""
Factory para crear la SparkSession con configuración estándar del proyecto.

Centralizar esto evita repetir la misma configuración (nombre de app,
shuffle, modo de escritura de particiones) en cada uno de los 4 jobs.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "64")
        # "dynamic" permite reescribir solo las particiones tocadas por el
        # job (ej. un solo mes) en vez de sobreescribir toda la tabla.
        # Importante para que los jobs sean re-ejecutables de forma segura.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "America/Lima")
        .getOrCreate()
    )
