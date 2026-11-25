"""Capas bronze y silver de los datasets pequeños (SSCO, SEE, PRICOS, Ingresos, EPEN)."""

import importlib
from datetime import date

import pandas as pd
import pytest
from fuentes.small import epen, ingresos_tributarios, pricos, see, ssco

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
        [
            [
                "20100000001",
                "X SAC",
                "LIMA",
                "R-1",
                46260,
                46275,
                " 10460556584 ",
                "Y",
                46295,
            ]
        ]
        * 20
    )
    salida = ssco.silver_ssco(ssco.bronze_ssco(df))

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
    salida = ssco.silver_ssco(ssco.bronze_ssco(df))
    assert len(salida) == 1
    assert salida.iloc[0]["fecha_emision"] == date(2024, 10, 30)


def test_fechas_ya_parseadas_por_read_excel():
    # read_excel entrega datetime64 cuando la celda del Excel tiene formato de fecha.
    df = _excel([["20100000001", "X", "L", "R", 46260, 46275, "1", "Y", 46295]] * 10)
    for c in ENCABEZADOS[4:6] + ENCABEZADOS[8:]:
        df[c] = pd.to_datetime("2026-09-28")
    salida = ssco.silver_ssco(ssco.bronze_ssco(df))
    assert salida.iloc[0]["fecha_emision"] == date(2026, 9, 28)
    assert salida.iloc[0]["fecha_publicacion"] == date(2026, 9, 28)


def test_falla_sin_fecha_de_emision():
    df = _excel([["20100000001", "X", "L", "R", 46260, 46275, "1", "Y", 46295]] * 10)
    df = df.drop(columns=["Fecha de emisión de la resolución de atribución"])
    with pytest.raises(ValueError, match="Fecha de emisión"):
        ssco.silver_ssco(ssco.bronze_ssco(df))


def test_see():
    df = pd.DataFrame(
        {
            "fecha_corte": ["2026-06-30"] * 20,
            "RUC": ["20100000001"] * 10 + ["10460556584"] * 10,
            "tipo_SEE": [" Factura Portal "] * 10 + ["recibos por honorarios"] * 10,
            "tipo_incorporacion": ["desde"] * 20,
            "fecha_inicio": ["2025-08-19"] * 10 + ["no-es-fecha"] * 10,
        }
    )
    salida = see.silver_see(see.bronze_see(df)).set_index("RUC")

    assert list(salida.columns) == ["tipo_see", "fecha_inicio"]
    assert len(salida) == 2  # filas repetidas se colapsan
    assert salida.loc[20100000001, "tipo_see"] == "factura portal"
    assert salida.loc[20100000001, "fecha_inicio"] == date(2025, 8, 19)
    assert pd.isna(salida.loc[10460556584, "fecha_inicio"])


def test_see_falla_sin_columnas():
    with pytest.raises(ValueError, match="tipo_SEE"):
        see.bronze_see(pd.DataFrame({"RUC": ["20100000001"]}))


def test_ssco_bronze_conserva_notas_al_pie_y_silver_las_quita():
    df = _excel(
        [["20100000001", "X", "L", "R", 46260, 46275, "1", "Y", 46295]] * 20
        + [["(1) Nota al pie de SUNAT", None, None, None, None, None, None, None, None]]
    )
    bronze = ssco.bronze_ssco(df)
    assert len(bronze) == 21
    # La nota al pie no impide leer las demás fechas como serial de Excel.
    assert bronze.iloc[0]["fecha_emision"] == date(2026, 8, 26)
    assert len(ssco.silver_ssco(bronze)) == 1


def test_pricos_bronze_tipa_y_silver_filtra_y_deduplica():
    df = pd.DataFrame(
        {
            "RUC": [20100000001] * 2 + [20100000002] * 18 + ["Fuente: SUNAT"],
            "Nombre o Razón Social": ["A"] * 20 + [None],
        }
    )
    bronze = pricos.bronze_pricos(df)
    assert len(bronze) == 21
    assert bronze["RUC"].dtype == "string"
    assert "nombre_razon_social" in bronze.columns
    assert sorted(pricos.silver_pricos(bronze)["RUC"]) == [20100000001, 20100000002]


def test_pricos_bronze_falla_sin_ruc():
    with pytest.raises(ValueError, match="RUC"):
        pricos.bronze_pricos(pd.DataFrame({"Nombre o Razón Social": ["A"]}))


def test_ingresos_bronze_tipa_y_silver_descarta_montos_ilegibles():
    df = pd.DataFrame(
        {
            "Departamento": ["LIMA", "CUSCO"],
            "Periodo": ["2025-01", "2025-01"],
            "Anio": ["2025", "2025"],
            "Mes": ["01", "01"],
            "Monto_Recaudado": ["1500.5", "n.d."],
        }
    )
    bronze = ingresos_tributarios.bronze_ingresos(df)
    assert len(bronze) == 2
    assert bronze["Anio"].dtype == "int64"
    assert ingresos_tributarios.silver_ingresos(bronze)["Departamento"].tolist() == [
        "LIMA"
    ]


def test_ingresos_bronze_falla_sin_columnas():
    with pytest.raises(ValueError, match="Monto_Recaudado"):
        ingresos_tributarios.bronze_ingresos(pd.DataFrame({"Departamento": ["LIMA"]}))


def test_epen_bronze_quita_particion_y_silver_descarta_nulos():
    df = pd.DataFrame(
        {
            "CCDD": pd.array([15, None], dtype="Int64"),
            "OCUP300": [1.0, 2.0],
            "Informal_P": [0.5, 0.6],
            "FAC300_ANUAL": [10.0, 20.0],
            "anio": ["2025", "2025"],
        }
    )
    bronze = epen.bronze_epen(df)
    assert list(bronze.columns) == list(epen.EPEN_DTYPES)
    assert len(bronze) == 2
    silver = epen.silver_epen(bronze)
    assert silver["CCDD"].tolist() == [15]
    assert silver["CCDD"].dtype == "int64"


def test_publicar_lee_silver_desde_bronze(monkeypatch):
    escritos = {}
    monkeypatch.setattr(
        job00.wr.s3, "to_parquet", lambda df, path, **_: escritos.__setitem__(path, df)
    )
    monkeypatch.setattr(job00.wr.s3, "read_parquet", lambda path, **_: escritos[path])
    job00._publicar(
        "x",
        pd.DataFrame({"v": [1, 2]}),
        lambda df: df.assign(v=df["v"] * 10),
        lambda df: df[df["v"] > 10],
        "s3://b/x",
        "s3://s/x",
    )
    assert escritos["s3://b/x"]["v"].tolist() == [10, 20]
    assert escritos["s3://s/x"]["v"].tolist() == [20]
