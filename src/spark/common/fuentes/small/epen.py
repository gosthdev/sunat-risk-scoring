"""EPEN: bronze con las columnas usadas y silver sin CCDD/FAC300_ANUAL nulos."""

import awswrangler as wr
from s3_paths import BRONZE_EPEN, RAW_EPEN, SILVER_EPEN

# Dtypes explícitos para las columnas de EPEN que sí se usan en
# 04_regional_gold.py (mismo principio de "no dejar inferir tipos" que en
# schema_definitions.py para los jobs de Spark).
EPEN_DTYPES = {
    "CCDD": "Int64",
    "OCUP300": "float64",
    "Informal_P": "float64",
    "FAC300_ANUAL": "float64",
}


def bronze_epen(df):
    # dataset=True agrega la partición (anio) como columna: solo se guardan las usadas.
    return df[list(EPEN_DTYPES)].astype(EPEN_DTYPES)


def silver_epen(df):
    df = df.dropna(subset=["CCDD", "FAC300_ANUAL"]).copy()
    df["CCDD"] = df["CCDD"].astype("int64")
    return df


def preparar(publicar):
    df = wr.s3.read_csv(
        f"{RAW_EPEN}/anio=2025/",
        dataset=True,
        usecols=list(EPEN_DTYPES),
        dtype=EPEN_DTYPES,
    )
    publicar("epen", df, bronze_epen, silver_epen, BRONZE_EPEN, SILVER_EPEN)
