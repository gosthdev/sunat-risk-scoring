"""Ingresos Tributarios: bronze tipado y silver sin montos no numéricos."""

import awswrangler as wr
import pandas as pd
from botocore.exceptions import ClientError
from ruc_pandas import exigir_columnas
from s3_paths import (
    BRONZE_INGRESOS_TRIBUTARIOS,
    RAW_INGRESOS_TRIBUTARIOS,
    SILVER_INGRESOS_TRIBUTARIOS,
)

# Nombre real del archivo con los montos en soles (no el de variación %).
# AJUSTAR si el nombre real en raw/ingresos_tributarios/ difiere.
INGRESOS_TRIBUTARIOS_ARCHIVO = "cdrA13_tabular.csv"


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


def _leer_ingresos_raw():
    """Tabla de montos desde raw (CSV tabular o Excel cdro_A13); último recurso, el Excel oficial de SUNAT."""
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

        df, _ = process_both_ingresos(tmp_excel)

    return df


def bronze_ingresos(df):
    exigir_columnas(df, REQUIRED_INGRESOS_COLUMNS, "Ingresos Tributarios")
    df = df.copy()
    df["Departamento"] = df["Departamento"].astype("string")
    df["Periodo"] = df["Periodo"].astype("string")
    df["Anio"] = df["Anio"].astype("int64")
    df["Mes"] = df["Mes"].astype("string")
    df["Monto_Recaudado"] = pd.to_numeric(df["Monto_Recaudado"], errors="coerce")
    return df


def silver_ingresos(df):
    salida = df.dropna(subset=["Monto_Recaudado"])
    if len(salida) < len(df):
        print(
            f"[prepare_ingresos_tributarios] {len(df) - len(salida)} filas sin "
            "Monto_Recaudado numérico descartadas."
        )
    return salida


def preparar(publicar):
    publicar(
        "ingresos_tributarios",
        _leer_ingresos_raw(),
        bronze_ingresos,
        silver_ingresos,
        BRONZE_INGRESOS_TRIBUTARIOS,
        SILVER_INGRESOS_TRIBUTARIOS,
    )
