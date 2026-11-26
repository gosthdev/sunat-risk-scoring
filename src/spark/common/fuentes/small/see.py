"""Padrón SEE: bronze con columnas usadas y silver con RUC válido y sin repetidos."""

import awswrangler as wr
import pandas as pd
from ruc_pandas import exigir_columnas, filtrar_ruc_validos
from s3_paths import BRONZE_SEE, RAW_SEE, SILVER_SEE

SEE_COLUMNAS = {"RUC": "RUC", "tipo_SEE": "tipo_see", "fecha_inicio": "fecha_inicio"}


def bronze_see(df):
    """Padrón SEE: columnas usadas con nombres estables y fecha de inicio tipada."""
    exigir_columnas(df, SEE_COLUMNAS, "Padrón SEE")
    df = df[list(SEE_COLUMNAS)].rename(columns=SEE_COLUMNAS)
    df["fecha_inicio"] = pd.to_datetime(
        df["fecha_inicio"], format="%Y-%m-%d", errors="coerce"
    ).dt.date
    return df


def silver_see(df):
    """RUC válido, tipo de comprobante en minúsculas (para el cruce en 03), sin repetidos."""
    df = filtrar_ruc_validos(df, "RUC", "prepare_see")
    df["tipo_see"] = df["tipo_see"].astype("string").str.strip().str.lower()
    return df.drop_duplicates()


def preparar(publicar):
    df = wr.s3.read_csv(
        f"{RAW_SEE}/padron_see.csv", dtype=str, usecols=list(SEE_COLUMNAS)
    )
    publicar("see", df, bronze_see, silver_see, BRONZE_SEE, SILVER_SEE)
