"""
EMR Step 1 — Ingesta a capa Bronze.

Despacha a fuentes/bronze/<fuente>.py, una por fuente registrada con capa
bronze (config/fuentes.json): lee el crudo, aplica el esquema explícito
(schema_definitions.py) y escribe Parquet particionado por anio/mes. No se
limpia contenido (bronze = espejo tipado del crudo); las filas que no calzan
con el esquema o tienen años fuera de rango (VALID_YEAR_RANGE) van a
bronze/quarantine/ en vez de descartarse o contaminar bronze.

Qué se procesa lo dicta PENDING_PERIODS (incremental.periodos): FULL, solo
ciertos (anio, mes) o la fuente se omite. Sin la variable, todas en FULL.

Asume raw/ en carpetas Hive-style (anio=2025/mes=01/...).

Uso: spark-submit --py-files common.zip 01_ingest_bronze.py
"""

import importlib
import sys

from incremental import fuentes_de, periodos
from spark_session_factory import create_spark_session


def main():
    spark = create_spark_session("01_ingest_bronze")
    try:
        for fuente in fuentes_de("bronze"):
            pendientes = periodos(fuente)
            if pendientes == []:
                print(f"[01_ingest_bronze] {fuente}: sin periodos pendientes, se omite")
                continue
            modulo = importlib.import_module(f"fuentes.bronze.{fuente}")
            modulo.bronze(spark, pendientes)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
