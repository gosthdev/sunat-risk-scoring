"""
EMR Step 2 — Limpieza y normalización a capa Silver.

Despacha a fuentes/silver/<fuente>.py, una por fuente registrada con capa
silver (config/fuentes.json). Cada una lee su bronze (ya tipado, sin limpieza
de contenido) y aplica, con silver_transforms.procesar:

  - Deduplicación de filas repetidas entre cargas de bronze.
  - Validación del RUC: debe tener exactamente 11 dígitos. Las filas con un
    RUC inválido van a silver/quarantine_<dataset>/ con su motivo.
  - Normalización de departamentos (region_normalizer) para cruzar con
    EPEN/Ingresos Tributarios/PRICOS en 04_regional_gold.py.
  - Regla de negocio de órdenes: monto nulo o negativo a cuarentena.

Antes de procesar valida el esquema de bronze contra el contrato del dataset
(schema_contracts.py). Publica además silver/quality_report/ con la tasa de
cuarentena por fuente, año y mes; si la tasa por RUC supera
MAX_TASA_CUARENTENA_RUC (0.5) aborta antes de escribir Silver.

Qué se procesa lo dicta PENDING_PERIODS (incremental.periodos): FULL, ciertos
(anio, mes) o la fuente se omite. Sin la variable, todas en FULL. Escribe en
_staging/; el orquestador publica a la ruta viva (05_submit_spark_steps.sh).

Uso: spark-submit --py-files common.zip 02_clean_silver.py
"""

import importlib
import sys

from incremental import fuentes_de, periodos
from spark_session_factory import create_spark_session


def main():
    spark = create_spark_session("02_clean_silver")
    try:
        for fuente in fuentes_de("silver"):
            pendientes = periodos(fuente)
            if pendientes == []:
                print(f"[02_clean_silver] {fuente}: sin periodos pendientes, se omite")
                continue
            modulo = importlib.import_module(f"fuentes.silver.{fuente}")
            modulo.silver(spark, pendientes)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
