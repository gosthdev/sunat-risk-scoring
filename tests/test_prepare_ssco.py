"""Normalización de la lista SSCO y del Padrón SEE de SUNAT."""

import importlib
from datetime import date

import pandas as pd
import pytest

job00 = importlib.import_module("00_prepare_small_datasets")

ENCABEZADOS = [
    "RUC",
    "Razón social",
    "Domicilio fiscal",
    "Resolución de atribución como SSCO",
    "Fecha de emisión de la resolución de atribución",
    "Fecha en la que la resolución de atribución quedó firme",
    "RUC o documento de identidad del representante legal (1)",
    "Apellidos y nombres del representante legal",
    "Fecha de publicación (2)",
]


def _excel(filas):
    return pd.DataFrame(filas, columns=ENCABEZADOS)


def test_conserva_fechas_y_representante():
    # 46260 es el serial de Excel del 2026-08-26.
    df = _excel(
        [["20100000001", "X SAC", "LIMA", "R-1", 46260, 46275, " 10460556584 ", "Y", 46295]]
        * 20
    )
    salida = job00.normalizar_ssco(df)

    assert list(salida.columns) == [
        "RUC",
        "fecha_emision",
        "fecha_firme",
        "fecha_publicacion",
        "doc_representante",
        "es_ssco",
    ]
    fila = salida.iloc[0]
    assert len(salida) == 1
    assert fila["RUC"] == 20100000001
    assert fila["fecha_emision"] == date(2026, 8, 26)
    assert fila["fecha_firme"] == date(2026, 9, 10)
    assert fila["doc_representante"] == "10460556584"


def test_ruc_repetido_conserva_la_resolucion_mas_antigua():
    df = _excel(
        [["20100000001", "X", "L", "R-2", 46260, 46275, "1", "Y", 46295]] * 10
        + [["20100000001", "X", "L", "R-1", 45595, 45611, "1", "Y", 45657]] * 10
    )
    salida = job00.normalizar_ssco(df)
    assert len(salida) == 1
    assert salida.iloc[0]["fecha_emision"] == date(2024, 10, 30)


def test_falla_sin_fecha_de_emision():
    df = _excel([["20100000001", "X", "L", "R", 46260, 46275, "1", "Y", 46295]] * 10)
    df = df.drop(columns=["Fecha de emisión de la resolución de atribución"])
    with pytest.raises(ValueError, match="Fecha de emisión"):
        job00.normalizar_ssco(df)


def test_normalizar_see():
    df = pd.DataFrame(
        {
            "fecha_corte": ["2026-06-30"] * 20,
            "RUC": ["20100000001"] * 10 + ["10460556584"] * 10,
            "tipo_SEE": [" Factura Portal "] * 10 + ["recibos por honorarios"] * 10,
            "tipo_incorporacion": ["desde"] * 20,
            "fecha_inicio": ["2025-08-19"] * 10 + ["no-es-fecha"] * 10,
        }
    )
    salida = job00.normalizar_see(df).set_index("RUC")

    assert list(salida.columns) == ["tipo_see", "fecha_inicio"]
    assert len(salida) == 2  # filas repetidas se colapsan
    assert salida.loc[20100000001, "tipo_see"] == "factura portal"
    assert salida.loc[20100000001, "fecha_inicio"] == date(2025, 8, 19)
    assert pd.isna(salida.loc[10460556584, "fecha_inicio"])


def test_normalizar_see_falla_sin_columnas():
    with pytest.raises(ValueError, match="tipo_SEE"):
        job00.normalizar_see(pd.DataFrame({"RUC": ["20100000001"]}))
