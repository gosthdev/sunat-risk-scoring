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

Se corre una sola vez, o cada vez que se actualice alguna de estas tres
fuentes. 04_regional_gold.py depende de que estos silver/ ya existan.

Uso: python 00_prepare_small_datasets.py
"""

import sys

import awswrangler as wr
import pandas as pd
from botocore.exceptions import ClientError
from ruc_pandas import filtrar_ruc_validos
from s3_paths import (
    RAW_EPEN,
    RAW_INGRESOS_TRIBUTARIOS,
    RAW_PRICOS,
    RAW_SEE,
    RAW_SSCO,
    SILVER_EPEN,
    SILVER_INGRESOS_TRIBUTARIOS,
    SILVER_PRICOS,
    SILVER_SEE,
    SILVER_SSCO,
)

# Nombre real del archivo con los montos en soles (no el de variación %).
# AJUSTAR si el nombre real en raw/ingresos_tributarios/ difiere.
INGRESOS_TRIBUTARIOS_ARCHIVO = "cdrA13_tabular.csv"

# Dtypes explícitos para las columnas de EPEN que sí se usan en
# 04_regional_gold.py (mismo principio de "no dejar inferir tipos" que en
# schema_definitions.py para los jobs de Spark).
EPEN_DTYPES = {
    "CCDD": "Int64",
    "OCUP300": "float64",
    "Informal_P": "float64",
    "FAC300_ANUAL": "float64",
}


def prepare_pricos():
    df = wr.s3.read_excel(f"{RAW_PRICOS}/principalesContrib-PRICOS.xlsx")
    df = df.rename(columns={"Nombre o Razón Social": "nombre_razon_social"})
    df = filtrar_ruc_validos(df, "RUC", "prepare_pricos")
    df = df.drop_duplicates(subset=["RUC"])
    wr.s3.to_parquet(df, path=SILVER_PRICOS, dataset=True, mode="overwrite")


REQUIRED_INGRESOS_COLUMNS = {
    "Departamento",
    "Periodo",
    "Anio",
    "Mes",
    "Monto_Recaudado",
}
SUNAT_CDRO_A13_URL = (
    "https://www.sunat.gob.pe/estadisticasestudios/nota_tributaria/cdro_A13.xlsx"
)


def _no_existe(exc: ClientError) -> bool:
    """True si el error de S3 es "el objeto no existe" (otros, como permisos, se propagan)."""
    return exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey")


def prepare_ingresos_tributarios():
    raw_path = f"{RAW_INGRESOS_TRIBUTARIOS}/{INGRESOS_TRIBUTARIOS_ARCHIVO}"
    df = None
    try:
        cand_df = wr.s3.read_csv(raw_path)
        if REQUIRED_INGRESOS_COLUMNS.issubset(cand_df.columns):
            df = cand_df
        else:
            print(
                "[prepare_ingresos_tributarios] CSV en S3 no contiene las columnas requeridas. "
                "Activando transformador desde el origen..."
            )
    except wr.exceptions.NoFilesFound as exc:
        # Solo la ausencia del archivo activa el origen; permisos o red fallan.
        print(
            f"[prepare_ingresos_tributarios] Archivo CSV no disponible en S3 ({exc}). "
            "Activando transformador desde el origen..."
        )

    if df is None:
        from ingresos_transformer import process_both_ingresos

        tmp_excel = "/tmp/cdro_A13.xlsx"
        downloaded = False

        # 1. Intentar descargar cdro_A13.xlsx de S3
        try:
            wr.s3.download(f"{RAW_INGRESOS_TRIBUTARIOS}/cdro_A13.xlsx", tmp_excel)
            downloaded = True
        except ClientError as exc:
            if not _no_existe(exc):
                raise

        # 2. Si no estaba como .xlsx, intentar si cdrA13_tabular.csv en S3 era en realidad un binario Excel
        if not downloaded:
            try:
                wr.s3.download(raw_path, tmp_excel)
            except ClientError as exc:
                if not _no_existe(exc):
                    raise
            else:
                try:
                    process_both_ingresos(tmp_excel)
                    downloaded = True
                except Exception as exc:  # noqa: BLE001
                    # El objeto existe pero no es un Excel válido: se baja el oficial.
                    print(
                        f"[prepare_ingresos_tributarios] {raw_path} no es Excel ({exc})."
                    )

        # 3. Si aún no está disponible o el archivo en S3 no es Excel válido,
        # descargar el Excel oficial directamente desde SUNAT
        if not downloaded:
            print(
                "[prepare_ingresos_tributarios] Descargando cdro_A13.xlsx oficial desde SUNAT..."
            )
            import urllib.request

            req = urllib.request.Request(
                SUNAT_CDRO_A13_URL,
                headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"},
            )
            with (
                urllib.request.urlopen(req, timeout=60) as resp,
                open(tmp_excel, "wb") as f_out,
            ):
                f_out.write(resp.read())

        df_monto, df_var = process_both_ingresos(tmp_excel)
        df = df_monto

        # Reparar/actualizar la capa RAW en S3 con los CSVs estandarizados
        try:
            wr.s3.to_csv(df_monto, raw_path, index=False)
            wr.s3.to_csv(
                df_var,
                f"{RAW_INGRESOS_TRIBUTARIOS}/cdrA13_Var_tabular.csv",
                index=False,
            )
            print(
                "[prepare_ingresos_tributarios] Capa RAW sincronizada con los nuevos CSVs estandarizados."
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[prepare_ingresos_tributarios] Advertencia al sincronizar CSVs en RAW ({exc})."
            )

    # Validar tipado y columnas estandarizadas
    df["Departamento"] = df["Departamento"].astype("string")
    df["Periodo"] = df["Periodo"].astype("string")
    df["Anio"] = df["Anio"].astype("int64")
    df["Mes"] = df["Mes"].astype("string")
    df["Monto_Recaudado"] = pd.to_numeric(df["Monto_Recaudado"], errors="coerce")
    df = df.dropna(subset=["Monto_Recaudado"])

    wr.s3.to_parquet(
        df, path=SILVER_INGRESOS_TRIBUTARIOS, dataset=True, mode="overwrite"
    )


def prepare_epen():
    cols = list(EPEN_DTYPES.keys())
    df = wr.s3.read_csv(
        f"{RAW_EPEN}/anio=2025/",
        dataset=True,
        usecols=cols,
        dtype=EPEN_DTYPES,
    )
    df = df[cols].copy()
    df = df.dropna(subset=["CCDD", "FAC300_ANUAL"])
    df["CCDD"] = df["CCDD"].astype("int64")
    wr.s3.to_parquet(df, path=SILVER_EPEN, dataset=True, mode="overwrite")
    print(
        f"[prepare_epen] EPEN procesado y guardado en {SILVER_EPEN} ({len(df):,} filas)."
    )


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
    # SUNAT guarda las fechas como serial de Excel; pandas a veces ya las parsea.
    numeros = pd.to_numeric(serie, errors="coerce")
    if numeros.notna().all():
        return pd.to_datetime(numeros, unit="D", origin="1899-12-30").dt.date
    return pd.to_datetime(serie, dayfirst=True, errors="coerce").dt.date


def normalizar_ssco(df):
    """RUC, fechas de la resolución y representante; una fila por RUC."""
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

    df = filtrar_ruc_validos(df, "RUC", "prepare_ssco")
    columnas = ["RUC"] + [c for c in SSCO_COLUMNAS.values() if c in df.columns]
    df = df[columnas].copy()
    for c in SSCO_FECHAS:
        if c in df.columns:
            df[c] = _a_fecha(df[c])
    if df["fecha_emision"].isna().any():
        raise ValueError("Excel SSCO con fecha_emision vacía o ilegible")
    if "doc_representante" in df.columns:
        df["doc_representante"] = df["doc_representante"].astype("string").str.strip()

    # Un RUC repetido conserva su resolución más antigua.
    df = df.sort_values("fecha_emision").drop_duplicates(subset=["RUC"])
    df["es_ssco"] = True
    return df


def prepare_ssco():
    # Lista oficial de SUNAT; se reemplaza en raw/ssco/ cuando SUNAT la actualiza.
    df = normalizar_ssco(wr.s3.read_excel(f"{RAW_SSCO}/sujesincapacidadOperativa.xlsx"))
    wr.s3.to_parquet(df, path=SILVER_SSCO, dataset=True, mode="overwrite")


SEE_COLUMNAS = {"RUC": "RUC", "tipo_SEE": "tipo_see", "fecha_inicio": "fecha_inicio"}


def normalizar_see(df):
    """Padrón SEE: RUC, tipo de comprobante (minúsculas) y fecha de inicio."""
    faltan = set(SEE_COLUMNAS) - set(df.columns)
    if faltan:
        raise ValueError(f"Padrón SEE sin columnas {sorted(faltan)}: {list(df.columns)}")
    df = filtrar_ruc_validos(df[list(SEE_COLUMNAS)], "RUC", "prepare_see")
    df = df.rename(columns=SEE_COLUMNAS)
    df["tipo_see"] = df["tipo_see"].astype("string").str.strip().str.lower()
    df["fecha_inicio"] = pd.to_datetime(
        df["fecha_inicio"], format="%Y-%m-%d", errors="coerce"
    ).dt.date
    return df.drop_duplicates()


def prepare_see():
    df = wr.s3.read_csv(
        f"{RAW_SEE}/padron_see.csv", dtype=str, usecols=list(SEE_COLUMNAS)
    )
    df = normalizar_see(df)
    wr.s3.to_parquet(df, path=SILVER_SEE, dataset=True, mode="overwrite")
    print(f"[prepare_see] SEE guardado en {SILVER_SEE} ({len(df):,} filas).")


def main():
    prepare_pricos()
    prepare_ingresos_tributarios()
    prepare_epen()
    prepare_ssco()
    prepare_see()


if __name__ == "__main__":
    sys.exit(main())
