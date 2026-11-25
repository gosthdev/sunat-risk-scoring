"""
Script auxiliar de preparación de datasets pequeños: PRICOS, Ingresos
Tributarios, EPEN, SSCO y Padrón SEE.

IMPORTANTE — esto NO es un EMR Step y NO se envía con spark-submit:
EMR Serverless solo acepta job runs de tipo Spark/Hive (no tiene un nodo
donde correr un script suelto), y estos tres datasets son demasiado chicos
para justificar el overhead de un cluster Spark. Según la decisión de
herramientas del proyecto, se procesan con pandas + awswrangler.

Dónde correrlo: cualquier máquina con Python 3.10+, las dependencias de
requirements.txt, y credenciales AWS con permisos de lectura/escritura
sobre el bucket (tu laptop, un teammate, un runner de GitHub Actions con
workflow_dispatch). Ver README.md de esta carpeta para el detalle de las
dos formas soportadas de ejecutarlo.

Cada dataset pasa por las mismas capas que padron_ruc y ordenes_compra:

  raw    -> archivo original (xlsx/csv); este script solo lo lee.
  bronze -> tabla tipada: aplanado, nombres estables y tipos explícitos.
            Falla si faltan columnas requeridas; no filtra ni deduplica.
  silver -> se lee desde bronze: RUC válido, deduplicación y reglas de negocio.

Despacha a fuentes/small/<fuente>.py, una por fuente registrada con capa small
(config/fuentes.json); PENDING_PERIODS (incremental.periodos) decide cuáles corren
(sin la variable, todas). Se corre cada vez que se actualice alguna de estas fuentes. 03/04/05/06 dependen de que estos silver/ ya existan.

Uso: python 00_prepare_small_datasets.py
"""

import importlib
import sys

import awswrangler as wr
from incremental import fuentes_de, periodos


def _a_parquet(df, ruta):
    wr.s3.to_parquet(df, path=ruta, dataset=True, mode="overwrite")


def _publicar(nombre, df_raw, bronze, silver, ruta_bronze, ruta_silver):
    """raw -> bronze (escrito) -> silver leído desde bronze, para poder reprocesar solo desde bronze."""
    df_bronze = bronze(df_raw)
    _a_parquet(df_bronze, ruta_bronze)
    df_silver = silver(wr.s3.read_parquet(ruta_bronze, dataset=True))
    _a_parquet(df_silver, ruta_silver)
    print(
        f"[prepare_{nombre}] bronze={len(df_bronze):,} filas en {ruta_bronze}; "
        f"silver={len(df_silver):,} filas en {ruta_silver}."
    )


def main():
    for fuente in fuentes_de("small"):
        # Periodo "-": sin parciales; cualquier valor distinto de [] es FULL.
        if periodos(fuente) == []:
            print(f"[00_prepare_small_datasets] {fuente}: omitida")
            continue
        importlib.import_module(f"fuentes.small.{fuente}").preparar(_publicar)


if __name__ == "__main__":
    sys.exit(main())
