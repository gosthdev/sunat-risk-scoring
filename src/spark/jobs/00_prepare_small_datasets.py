"""
Script auxiliar de preparación de datasets pequeños: PRICOS, Ingresos
Tributarios y EPEN.

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

Se corre una sola vez, o cada vez que se actualice alguna de estas tres
fuentes. 04_regional_gold.py depende de que estos silver/ ya existan.

Uso: python 00_prepare_small_datasets.py
"""

import sys

import awswrangler as wr
import pandas as pd
from region_normalizer import normalize_department
from s3_paths import (
    RAW_EPEN,
    RAW_INGRESOS_TRIBUTARIOS,
    RAW_PRICOS,
    RAW_SSCO,
    SILVER_EPEN,
    SILVER_INGRESOS_TRIBUTARIOS,
    SILVER_PRICOS,
    SILVER_SSCO,
)

# Nombre real del archivo con los montos en soles (no el de variación %).
# AJUSTAR si el nombre real en raw/ingresos_tributarios/ difiere.
INGRESOS_TRIBUTARIOS_ARCHIVO = "cdrA13_tabular.csv"

# Dtypes explícitos para las columnas de EPEN que sí se usan en
# 04_regional_gold.py (mismo principio de "no dejar inferir tipos" que en
# schema_definitions.py para los jobs de Spark).
EPEN_DTYPES = {
    "CCDD": "int64",
    "OCUP300": "float64",
    "Informal_P": "float64",
    "FAC300_ANUAL": "float64",
}


def prepare_pricos():
    df = wr.s3.read_excel(f"{RAW_PRICOS}/principalesContrib-PRICOS.xlsx")
    df = df.rename(columns={"Nombre o Razón Social": "nombre_razon_social"})
    df["RUC"] = df["RUC"].astype("int64")
    df = df.drop_duplicates(subset=["RUC"])
    wr.s3.to_parquet(df, path=SILVER_PRICOS, dataset=True, mode="overwrite")


def prepare_ingresos_tributarios():
    df = wr.s3.read_csv(f"{RAW_INGRESOS_TRIBUTARIOS}/{INGRESOS_TRIBUTARIOS_ARCHIVO}")
    df["Departamento"] = df["Departamento"].map(normalize_department)
    df["Monto_Recaudado"] = pd.to_numeric(df["Monto_Recaudado"], errors="coerce")
    df = df.dropna(subset=["Monto_Recaudado"])
    wr.s3.to_parquet(
        df, path=SILVER_INGRESOS_TRIBUTARIOS, dataset=True, mode="overwrite"
    )


def prepare_epen():
    df = wr.s3.read_csv(f"{RAW_EPEN}/anio=2025/", dataset=True, dtype=EPEN_DTYPES)
    df = df.dropna(subset=["FAC300_ANUAL"])
    wr.s3.to_parquet(df, path=SILVER_EPEN, dataset=True, mode="overwrite")


def prepare_ssco():
    # Foto única (corte 30-sep-2026), no se vuelve a descargar.
    df = wr.s3.read_excel(f"{RAW_SSCO}/sujesincapacidadOperativa.xlsx")
    # AJUSTAR el nombre real de la columna de RUC si difiere en el archivo.
    df = df.rename(columns={df.columns[0]: "RUC"})
    df["RUC"] = pd.to_numeric(df["RUC"], errors="coerce")
    df = df.dropna(subset=["RUC"])
    df["RUC"] = df["RUC"].astype("int64")
    df = df[["RUC"]].drop_duplicates()
    df["es_ssco"] = True
    wr.s3.to_parquet(df, path=SILVER_SSCO, dataset=True, mode="overwrite")


def main():
    prepare_pricos()
    prepare_ingresos_tributarios()
    prepare_epen()
    prepare_ssco()


if __name__ == "__main__":
    sys.exit(main())
