"""
Factory para crear la SparkSession con configuración estándar del proyecto.

Centralizar esto evita repetir la misma configuración (nombre de app,
shuffle, modo de escritura de particiones) en cada uno de los 4 jobs.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # shuffle.partitions no se fija aquí: lo calcula 05_submit_spark_steps.sh según
        # el volumen del RAW (fijarlo en el código anula el --conf del envío). AQE junta
        # las particiones que queden chicas, hasta ~128 MB, salvo en tablas cacheadas.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
        # Sin esto AQE prioriza el paralelismo e ignora el tamaño objetivo.
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
        # "dynamic" permite reescribir solo las particiones tocadas por el
        # job (ej. un solo mes) en vez de sobreescribir toda la tabla.
        # Importante para que los jobs sean re-ejecutables de forma segura.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "America/Lima")
        # --- Solución a fechas antiguas / inconsistencia de calendario en Spark 3+ ---
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.avro.datetimeRebaseModeInRead", "CORRECTED")
        # --- Robustez de lectura Parquet: coerción de tipos (INT32 -> bigint) entre particiones ---
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.mergeSchema", "true")
        .getOrCreate()
    )
