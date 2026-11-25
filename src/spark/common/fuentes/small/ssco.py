"""SSCO: bronze con fechas tipadas y silver con una fila por RUC."""

import awswrangler as wr
import pandas as pd
from ruc_pandas import filtrar_ruc_validos
from s3_paths import BRONZE_SSCO, RAW_SSCO, SILVER_SSCO

# Texto del encabezado del Excel de SUNAT (en minúsculas) -> columna silver.
# Las fechas permiten excluir en 06 a quien ya era SSCO en el mes de corte de
# las features y separar train/test en el tiempo.
SSCO_COLUMNAS = {
    "fecha de emisión": "fecha_emision",
    "quedó firme": "fecha_firme",
    "fecha de publicación": "fecha_publicacion",
    "documento de identidad del representante": "doc_representante",
}
SSCO_FECHAS = ["fecha_emision", "fecha_firme", "fecha_publicacion"]


def _a_fecha(serie):
    # SUNAT guarda las fechas como serial de Excel; read_excel ya las devuelve como
    # datetime cuando la celda tiene formato de fecha. to_numeric sobre un datetime da
    # nanosegundos, que leídos como días desbordan.
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie.dt.date
    numeros = pd.to_numeric(serie, errors="coerce")
    # Solo celdas con valor: bronze aún tiene filas sin fecha (p. ej. notas al pie).
    if numeros[serie.notna()].notna().all():
        return pd.to_datetime(numeros, unit="D", origin="1899-12-30").dt.date
    return pd.to_datetime(serie, dayfirst=True, errors="coerce").dt.date


def bronze_ssco(df):
    """Encabezados del Excel a nombres estables y fechas tipadas; no filtra filas."""
    # Columna "RUC" si existe; si no, la primera. filtrar_ruc_validos falla si la
    # elegida no parece contener RUC (p. ej. un índice).
    columna = next(
        (c for c in df.columns if str(c).strip().upper() == "RUC"), df.columns[0]
    )
    renombres = {columna: "RUC"}
    for c in df.columns:
        for texto, destino in SSCO_COLUMNAS.items():
            if texto in str(c).lower():
                renombres[c] = destino
    df = df.rename(columns=renombres)
    if "fecha_emision" not in df.columns:
        raise ValueError(
            "Excel SSCO sin 'Fecha de emisión de la resolución': sin ella no se "
            f"puede evitar la fuga temporal. Columnas: {list(df.columns)}"
        )

    columnas = ["RUC"] + [c for c in SSCO_COLUMNAS.values() if c in df.columns]
    df = df[columnas].copy()
    df["RUC"] = df["RUC"].astype("string")
    for c in SSCO_FECHAS:
        if c in df.columns:
            df[c] = _a_fecha(df[c])
    if "doc_representante" in df.columns:
        df["doc_representante"] = df["doc_representante"].astype("string").str.strip()
    return df


def silver_ssco(df):
    """RUC válido, fecha de emisión obligatoria; una fila por RUC."""
    df = filtrar_ruc_validos(df, "RUC", "prepare_ssco")
    if df["fecha_emision"].isna().any():
        raise ValueError("Excel SSCO con fecha_emision vacía o ilegible")

    # Un RUC repetido conserva su resolución más antigua.
    df = df.sort_values("fecha_emision").drop_duplicates(subset=["RUC"])
    df["es_ssco"] = True
    return df


def preparar(publicar):
    # Lista oficial de SUNAT; se reemplaza en raw/ssco/ cuando SUNAT la actualiza.
    publicar(
        "ssco",
        wr.s3.read_excel(f"{RAW_SSCO}/sujesincapacidadOperativa.xlsx"),
        bronze_ssco,
        silver_ssco,
        BRONZE_SSCO,
        SILVER_SSCO,
    )
