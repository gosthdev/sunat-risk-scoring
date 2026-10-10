"""
Pruebas unitarias para la librería de métricas (B2) y baselines (B3).
"""

import numpy as np
import pandas as pd
import pytest

from src.models.evaluation.metrics import compute_metrics
from src.models.evaluation.baselines import (
    baseline_trivial,
    baseline_analyst_rule,
    baseline_random,
)


def test_perfect_scores():
    """Scores perfectos: todos los positivos tienen mayor score que los negativos."""
    # 100 filas, 10 positivos con score 1.0, 90 negativos con score 0.0
    df = pd.DataFrame({
        "ruc": [f"20{i:09d}" for i in range(100)],
        "label": [1] * 10 + [0] * 90,
        "score": [1.0] * 10 + [0.0] * 90,
        "split": ["test"] * 100,
        "departamento": ["LIMA"] * 50 + ["AREQUIPA"] * 50,
    })

    metrics = compute_metrics(df, ks=[10, 20], n_bootstrap=50, seed=42)

    assert metrics["pr_auc"]["valor"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["roc_auc"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["precision_at_k"]["10"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["recall_at_k"]["10"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["lift_at_k"]["10"] == pytest.approx(10.0, rel=1e-3)
    assert metrics["metodo_pr_auc"] == "average_precision_sklearn"


def test_inverted_scores():
    """Scores completamente invertidos: positivos tienen los peores scores."""
    df = pd.DataFrame({
        "ruc": [f"20{i:09d}" for i in range(100)],
        "label": [1] * 10 + [0] * 90,
        "score": [0.0] * 10 + [1.0] * 90,
        "split": ["test"] * 100,
    })

    metrics = compute_metrics(df, ks=[10], n_bootstrap=10)
    assert metrics["recall_at_k"]["10"] == 0.0
    assert metrics["precision_at_k"]["10"] == 0.0
    assert metrics["pr_auc"]["valor"] < 0.15


def test_random_scores():
    """Scores aleatorios convergen a PR-AUC ≈ prevalencia y Lift@K ≈ 1."""
    np.random.seed(42)
    n = 2000
    p = 0.05
    labels = (np.random.rand(n) < p).astype(int)
    scores = np.random.rand(n)

    df = pd.DataFrame({
        "ruc": [f"20{i:09d}" for i in range(n)],
        "label": labels,
        "score": scores,
        "split": ["test"] * n,
    })

    metrics = compute_metrics(df, ks=[100], n_bootstrap=50, seed=42)
    prev = metrics["prevalencia"]
    assert metrics["pr_auc"]["valor"] == pytest.approx(prev, abs=0.04)
    assert metrics["lift_at_k"]["100"] == pytest.approx(1.0, abs=0.5)


def test_manual_10_rows():
    """10 filas calculadas a mano para verificar precisión exacta."""
    df = pd.DataFrame({
        "ruc": [f"2000000000{i}" for i in range(10)],
        "label": [1, 0, 1, 0, 0, 0, 0, 0, 0, 0],  # 2 positivos de 10 -> prev = 0.2
        "score": [0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0],
        "split": ["test"] * 10,
    })

    metrics = compute_metrics(df, ks=[2], n_bootstrap=20, seed=42)
    # En top 2: RUC 0 (label 1), RUC 1 (label 0) -> 1 positivo en top 2
    assert metrics["precision_at_k"]["2"] == 0.5
    assert metrics["recall_at_k"]["2"] == 0.5
    assert metrics["lift_at_k"]["2"] == pytest.approx(2.5)  # 0.5 / 0.2 = 2.5


def test_bootstrap_reproducible():
    """Verifica que con la misma semilla se obtiene exactamente el mismo IC."""
    df = pd.DataFrame({
        "label": [1] * 20 + [0] * 80,
        "score": np.linspace(1, 0, 100),
        "split": ["test"] * 100,
    })

    m1 = compute_metrics(df, ks=[10], n_bootstrap=100, seed=999)
    m2 = compute_metrics(df, ks=[10], n_bootstrap=100, seed=999)

    assert m1["pr_auc"]["ic_bajo"] == m2["pr_auc"]["ic_bajo"]
    assert m1["pr_auc"]["ic_alto"] == m2["pr_auc"]["ic_alto"]


def test_k_exceeds_test_size():
    """K mayor al tamaño de test lanza ValueError claro."""
    df = pd.DataFrame({
        "label": [1, 0, 1],
        "score": [0.9, 0.5, 0.1],
        "split": ["test"] * 3,
    })

    with pytest.raises(ValueError, match="excede el tamaño"):
        compute_metrics(df, ks=[10])


def test_segment_zero_positives():
    """Un segmento con 0 positivos devuelve None (null) sin romper."""
    df = pd.DataFrame({
        "ruc": [f"20{i:09d}" for i in range(20)],
        "label": [1] * 5 + [0] * 15,
        "score": np.linspace(1, 0, 20),
        "split": ["test"] * 20,
        # Todos los positivos en LIMA, TACNA tiene 0 positivos
        "departamento": ["LIMA"] * 10 + ["TACNA"] * 10,
    })

    metrics = compute_metrics(df, ks=[5], n_bootstrap=20)
    assert metrics["por_segmento"]["TACNA"] is None
    assert metrics["por_segmento"]["LIMA"] is not None
    assert metrics["por_segmento"]["LIMA"]["n_positivos"] == 5


def test_all_scores_equal():
    """Todos los scores iguales no lanzan error y desempatan de forma determinística."""
    df = pd.DataFrame({
        "ruc": ["20000000002", "20000000001", "20000000003"],
        "label": [0, 1, 0],
        "score": [0.5, 0.5, 0.5],
        "split": ["test"] * 3,
    })

    metrics = compute_metrics(df, ks=[1], n_bootstrap=10)
    # Por orden estable por RUC asc, '20000000001' queda primero en top 1 (label=1)
    assert metrics["precision_at_k"]["1"] == 1.0


def test_rejects_train_data():
    """Rechaza datos con split == 'train' a menos que modo_diagnostico=True."""
    df = pd.DataFrame({
        "label": [1, 0, 1, 0],
        "score": [0.8, 0.7, 0.3, 0.1],
        "split": ["train", "train", "test", "test"],
    })

    with pytest.raises(ValueError, match="Datos de train detectados"):
        compute_metrics(df)

    # Con modo_diagnostico=True no debe fallar
    res = compute_metrics(df, ks=[1], n_bootstrap=10, modo_diagnostico=True)
    assert res is not None


def test_b0_recall_is_zero():
    """B0 Trivial asigna score 0.0 a todo."""
    df = pd.DataFrame({
        "ruc": ["201", "202", "203"],
        "label": [1, 0, 0],
        "split": ["test"] * 3,
    })
    b0_df = baseline_trivial(df)
    assert (b0_df["score"] == 0.0).all()
    assert b0_df["run_id"].iloc[0] == "baseline_b0_trivial"


def test_b1_three_levels():
    """B1 genera niveles distintos de score según señales heurísticas."""
    df = pd.DataFrame({
        "ruc": ["201", "202", "203", "204"],
        "condicion": ["HABIDO", "NO HABIDO", "NO HALLADO", "HABIDO"],
        "nro_trab": [10, 0, 0, 5],
        "contrata_con_estado": [False, False, True, False],
        "label": [0, 1, 1, 0],
        "split": ["test"] * 4,
    })
    b1_df = baseline_analyst_rule(df)
    unique_scores = b1_df["score"].nunique()
    assert unique_scores >= 3
    assert b1_df["score"].max() <= 1.0
    assert b1_df["score"].min() >= 0.0


def test_b2_random_reproducible():
    """B2 es reproducible con semilla fija y respeta contrato C2."""
    df = pd.DataFrame({
        "ruc": [f"20{i}" for i in range(50)],
        "label": [1] * 5 + [0] * 45,
        "split": ["test"] * 50,
    })
    b2_1 = baseline_random(df, seed=123)
    b2_2 = baseline_random(df, seed=123)
    pd.testing.assert_series_equal(b2_1["score"], b2_2["score"])
