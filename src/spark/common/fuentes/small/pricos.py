"""PRICOS: bronze tipado y silver con RUC válido y sin repetidos."""

import awswrangler as wr
from ruc_pandas import exigir_columnas, filtrar_ruc_validos
from s3_paths import BRONZE_PRICOS, RAW_PRICOS, SILVER_PRICOS

# Encabezado del Excel de SUNAT -> columna de bronze (todas obligatorias).
PRICOS_COLUMNAS = {"RUC": "RUC", "Nombre o Razón Social": "nombre_razon_social"}


def bronze_pricos(df):
    df = df.rename(columns=PRICOS_COLUMNAS)
    exigir_columnas(df, PRICOS_COLUMNAS.values(), "PRICOS")
    # Un object con tipos mezclados (p. ej. una nota al pie) no se puede escribir a Parquet.
    texto = df.select_dtypes("object").columns
    df[texto] = df[texto].astype("string")
    df["RUC"] = df["RUC"].astype("string")
    return df


def silver_pricos(df):
    df = filtrar_ruc_validos(df, "RUC", "prepare_pricos")
    return df.drop_duplicates(subset=["RUC"])


def preparar(publicar):
    publicar(
        "pricos",
        wr.s3.read_excel(f"{RAW_PRICOS}/principalesContrib-PRICOS.xlsx"),
        bronze_pricos,
        silver_pricos,
        BRONZE_PRICOS,
        SILVER_PRICOS,
    )
