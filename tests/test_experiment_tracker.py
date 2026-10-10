"""
Pruebas unitarias para el seguimiento y consolidación de experimentos (Tarea B5).
"""

import json
import os
import tempfile

import pandas as pd
import pytest

from src.models.evaluation.tracker import (
    cargar_run_records,
    consolidar_experimentos,
    formatear_tabla_markdown,
    validar_run_record,
    verificar_completitud_matriz,
)


@pytest.fixture
def mock_run_record_valido():
    return {
        "run_id": "run_E1_LR_V2",
        "fecha": "2026-10-10T12:00:00Z",
        "git_commit": "abc1234",
        "dataset_version": "v1",
        "variant": "V2",
        "model_name": "LR",
        "usa_pesos": True,
        "hiperparametros_ganadores": {"C": 1.0, "penalty": "l2"},
        "metricas_cv": {
            "cv_pr_auc_mean": 0.4520,
            "cv_pr_auc_fold": [0.44, 0.46, 0.45, 0.47, 0.44],
            "train_pr_auc": 0.4900,
        },
        "n_train": 100000,
        "n_test": 25000,
        "n_positivos_train": 500,
        "n_positivos_test": 125,
        "segundos_entrenamiento": 18.5,
    }


def test_validar_run_record_exitoso(mock_run_record_valido):
    es_valido, errores = validar_run_record(mock_run_record_valido)
    assert es_valido is True
    assert len(errores) == 0


def test_validar_run_record_incompleto():
    registro_malo = {
        "run_id": "run_bad",
        "model_name": "DT",
        # Faltan variant, dataset_version, metricas_cv
    }
    es_valido, errores = validar_run_record(registro_malo)
    assert es_valido is False
    assert any("variant" in e for e in errores)
    assert any("metricas_cv" in e for e in errores)


def test_cargar_run_records_desde_directorio(mock_run_record_valido):
    with tempfile.TemporaryDirectory() as tmpdir:
        # Archivo válido
        f_valido = os.path.join(tmpdir, "run_record_E1.json")
        with open(f_valido, "w", encoding="utf-8") as f:
            f.write(json.dumps(mock_run_record_valido) + "\n")

        # Archivo con JSON corrupto
        f_corrupto = os.path.join(tmpdir, "run_record_bad.json")
        with open(f_corrupto, "w", encoding="utf-8") as f:
            f.write("ESTO NO ES UN JSON\n")

        validos, con_error = cargar_run_records(tmpdir)
        assert len(validos) == 1
        assert validos[0]["run_id"] == "run_E1_LR_V2"
        assert len(con_error) == 1


def test_verificar_completitud_matriz():
    records_incompletos = [
        {"run_id": "run_E1_LR_V2"},
        {"run_id": "run_E2_DT_V2"},
        {"run_id": "run_E3_LR_V1"},
    ]
    check = verificar_completitud_matriz(records_incompletos)
    assert check["completas"] is False
    assert set(check["faltantes"]) == {"E4", "E5", "E6"}

    records_completos = [
        {"run_id": "E1_lr_v2"},
        {"run_id": "E2_dt_v2"},
        {"run_id": "E3_lr_v1"},
        {"run_id": "E4_dt_v1"},
        {"run_id": "E5_lr_v2_unweighted"},
        {"run_id": "E6_dt_v2_unweighted"},
    ]
    check_ok = verificar_completitud_matriz(records_completos)
    assert check_ok["completas"] is True
    assert len(check_ok["faltantes"]) == 0


def test_consolidar_experimentos_sin_predicciones(mock_run_record_valido):
    records = [
        mock_run_record_valido,
        {
            "run_id": "run_E2_DT_V2",
            "model_name": "DT",
            "variant": "V2",
            "dataset_version": "v1",
            "usa_pesos": True,
            "metricas_cv": {"cv_pr_auc_mean": 0.4100},
        },
    ]

    df = consolidar_experimentos(records)
    assert len(df) == 2
    # Ordenado por cv_pr_auc desc
    assert df.iloc[0]["run_id"] == "run_E1_LR_V2"
    assert df.iloc[0]["cv_pr_auc"] == 0.4520
    assert df.iloc[1]["run_id"] == "run_E2_DT_V2"


def test_consolidar_experimentos_con_predicciones(mock_run_record_valido):
    # Generar predicciones sintéticas para E1
    n = 100
    df_preds = pd.DataFrame(
        {
            "ruc": [f"20{i:09d}" for i in range(n)],
            "label": [1] * 5 + [0] * (n - 5),
            "score": [0.9] * 5 + [0.1] * (n - 5),
            "split": ["test"] * n,
        }
    )

    records = [mock_run_record_valido]
    predicciones_map = {mock_run_record_valido["run_id"]: df_preds}

    df_resumen = consolidar_experimentos(
        records,
        predicciones_por_run=predicciones_map,
        n_bootstrap=20,
    )

    assert df_resumen.iloc[0]["test_pr_auc"] == pytest.approx(1.0, rel=1e-2)
    assert df_resumen.iloc[0]["test_recall_5pct"] == pytest.approx(1.0, rel=1e-2)


def test_formatear_tabla_markdown(mock_run_record_valido):
    records = [mock_run_record_valido]
    df = consolidar_experimentos(records)
    tabla_md = formatear_tabla_markdown(df)

    assert "| Run ID | Modelo | Variante |" in tabla_md
    assert "run_E1_LR_V2" in tabla_md
    assert "0.4520" in tabla_md
