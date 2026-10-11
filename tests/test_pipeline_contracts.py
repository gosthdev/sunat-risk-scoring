"""Pruebas de contratos del pipeline: C2, C3, C5, preprocesamiento y métricas."""

import json
import re

import numpy as np
import pandas as pd

from src.models.evaluation.metrics import (
    _bootstrap_pr_auc_ci,
    _serializar_ndjson,
)
from src.models.export_outputs import _resolver_git_commit
from src.models.launch_manual import SAGEMAKER_METRIC_DEFINITIONS
from src.models.train import construir_pipeline_preprocesamiento


def test_imputacion_mediana_con_bandera_de_nulo():
    df = pd.DataFrame(
        {
            "monto_total_soles": [10.0, np.nan, 30.0, 50.0],
            "pct_ordenes_anuladas": [0.1, 0.2, np.nan, 0.4],
        }
    )
    prep = construir_pipeline_preprocesamiento(list(df.columns), [], es_lr=False).fit(
        df
    )
    out = prep.transform(df)
    assert not np.isnan(out).any()
    # 2 columnas + 2 banderas de nulo
    assert out.shape[1] == 4
    names = list(prep.get_feature_names_out())
    assert any("missingindicator" in n for n in names)


def test_tope_de_categorias_por_variable():
    n = 200
    df = pd.DataFrame(
        {
            "departamento": [f"D{i % 40}" for i in range(n)],
            "ciiu_principal": [f"C{i % 60}" for i in range(n)],
        }
    )
    prep = construir_pipeline_preprocesamiento([], list(df.columns)).fit(df)
    names = list(prep.get_feature_names_out())
    assert sum(n.startswith("cat_departamento") for n in names) <= 25
    assert sum(n.startswith("cat_ciiu_principal") for n in names) <= 30


def test_regex_c5_fold_unico():
    regex = next(
        m["Regex"]
        for m in SAGEMAKER_METRIC_DEFINITIONS
        if m["Name"] == "cv_pr_auc_fold"
    )
    log = "[METRIC] cv_pr_auc_fold=0.3410\n[METRIC] cv_pr_auc_fold=0.3490"
    assert re.findall(regex, log) == ["0.3410", "0.3490"]
    assert len(SAGEMAKER_METRIC_DEFINITIONS) == 7


def test_git_commit_real_o_unknown(monkeypatch):
    monkeypatch.setenv("GIT_COMMIT", "abc123")
    assert _resolver_git_commit() == "abc123"
    monkeypatch.delenv("GIT_COMMIT")
    assert _resolver_git_commit() != "HEAD"


def test_ndjson_metricas_sin_nan_y_una_linea():
    line = _serializar_ndjson({"a": float("nan"), "b": 1.5, "c": float("inf")})
    assert line.count("\n") == 1 and line.endswith("\n")
    assert json.loads(line) == {"a": None, "b": 1.5, "c": None}


def test_bootstrap_descarta_remuestreos_sin_positivos():
    y = np.array([1] + [0] * 99)
    s = np.linspace(1, 0, 100)
    low, high = _bootstrap_pr_auc_ci(y, s, n_bootstrap=200, seed=1)
    # Con un solo positivo, contar los remuestreos vacíos como 0.0 arrastraba el IC a 0.
    assert low > 0.0 and high >= low
