"""Validación de RUC para los scripts con pandas."""

import pandas as pd
import pytest
from ruc_pandas import filtrar_ruc_validos


def test_descarta_ruc_no_numericos_y_de_otra_longitud():
    df = pd.DataFrame({"RUC": ["20100000001"] * 30 + ["ABC", "12345678", None]})
    salida = filtrar_ruc_validos(df, "RUC", "test")
    assert len(salida) == 30
    assert salida["RUC"].dtype == "int64"
    assert (salida["RUC"] == 20100000001).all()


def test_falla_si_la_columna_no_parece_un_ruc():
    # Un índice 1..n (la primera columna del Excel era "N°") no son RUC.
    df = pd.DataFrame({"RUC": list(range(1, 101))})
    with pytest.raises(ValueError, match="columna correcta"):
        filtrar_ruc_validos(df, "RUC", "test")


def test_tabla_vacia_falla():
    with pytest.raises(ValueError):
        filtrar_ruc_validos(pd.DataFrame({"RUC": []}), "RUC", "test")
