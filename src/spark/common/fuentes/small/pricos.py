"""PRICOS: bronze tipado y silver con RUC válido y sin repetidos."""

import awswrangler as wr
from ruc_pandas import exigir_columnas, filtrar_ruc_validos
from s3_paths import BRONZE_PRICOS, RAW_PRICOS, SILVER_PRICOS


def bronze_pricos(df):
    df = df.rename(columns={"Nombre o Razón Social": "nombre_razon_social"})
    exigir_columnas(df, {"RUC", "nombre_razon_social"}, "PRICOS")
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
