"""
Pruebas unitarias para la librería de métricas (B2) y baselines (B3).
"""

import numpy as np
import pandas as pd
import pytest

import json
import os
import tempfile

from src.models.evaluation.baselines import (
    baseline_analyst_rule,
    baseline_random,
    baseline_trivial,
    evaluate_and_export_baseline,
)
from src.models.evaluation.metrics import compute_metrics, flatten_metrics


def test_perfect_scores():
    """Scores perfectos: todos los positivos tienen mayor score que los negativos."""
    # 100 filas, 10 positivos con score 1.0, 90 negativos con score 0.0
    df = pd.DataFrame(
        {
            "ruc": [f"20{i:09d}" for i in range(100)],
            "label": [1] * 10 + [0] * 90,
            "score": [1.0] * 10 + [0.0] * 90,
            "split": ["test"] * 100,
            "departamento": ["LIMA"] * 50 + ["AREQUIPA"] * 50,
        }
    )

    metrics = compute_metrics(df, ks=[10, 20], n_bootstrap=50, seed=42)

    assert metrics["pr_auc"]["valor"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["roc_auc"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["precision_at_k"]["10"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["recall_at_k"]["10"] == pytest.approx(1.0, rel=1e-3)
    assert metrics["lift_at_k"]["10"] == pytest.approx(10.0, rel=1e-3)
    assert metrics["metodo_pr_auc"] == "average_precision_sklearn"


def test_inverted_scores():
    """Scores completamente invertidos: positivos tienen los peores scores."""
    df = pd.DataFrame(
        {
            "ruc": [f"20{i:09d}" for i in range(100)],
            "label": [1] * 10 + [0] * 90,
            "score": [0.0] * 10 + [1.0] * 90,
            "split": ["test"] * 100,
        }
    )

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

    df = pd.DataFrame(
        {
            "ruc": [f"20{i:09d}" for i in range(n)],
            "label": labels,
            "score": scores,
            "split": ["test"] * n,
        }
    )

    metrics = compute_metrics(df, ks=[100], n_bootstrap=50, seed=42)
    prev = metrics["prevalencia"]
    assert metrics["pr_auc"]["valor"] == pytest.approx(prev, abs=0.04)
    assert metrics["lift_at_k"]["100"] == pytest.approx(1.0, abs=0.5)


def test_manual_10_rows():
    """10 filas calculadas a mano para verificar precisión exacta."""
    df = pd.DataFrame(
        {
            "ruc": [f"2000000000{i}" for i in range(10)],
            "label": [1, 0, 1, 0, 0, 0, 0, 0, 0, 0],  # 2 positivos de 10 -> prev = 0.2
            "score": [0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0],
            "split": ["test"] * 10,
        }
    )

    metrics = compute_metrics(df, ks=[2], n_bootstrap=20, seed=42)
    # En top 2: RUC 0 (label 1), RUC 1 (label 0) -> 1 positivo en top 2
    assert metrics["precision_at_k"]["2"] == 0.5
    assert metrics["recall_at_k"]["2"] == 0.5
    assert metrics["lift_at_k"]["2"] == pytest.approx(2.5)  # 0.5 / 0.2 = 2.5


def test_bootstrap_reproducible():
    """Verifica que con la misma semilla se obtiene exactamente el mismo IC."""
    df = pd.DataFrame(
        {
            "label": [1] * 20 + [0] * 80,
            "score": np.linspace(1, 0, 100),
            "split": ["test"] * 100,
        }
    )

    m1 = compute_metrics(df, ks=[10], n_bootstrap=100, seed=999)
    m2 = compute_metrics(df, ks=[10], n_bootstrap=100, seed=999)

    assert m1["pr_auc"]["ic_bajo"] == m2["pr_auc"]["ic_bajo"]
    assert m1["pr_auc"]["ic_alto"] == m2["pr_auc"]["ic_alto"]


def test_k_exceeds_test_size():
    """K mayor al tamaño de test lanza ValueError claro."""
    df = pd.DataFrame(
        {
            "label": [1, 0, 1],
            "score": [0.9, 0.5, 0.1],
            "split": ["test"] * 3,
        }
    )

    with pytest.raises(ValueError, match="excede el tamaño"):
        compute_metrics(df, ks=[10])


def test_segment_zero_positives():
    """Un segmento con 0 positivos devuelve None (null) sin romper."""
    df = pd.DataFrame(
        {
            "ruc": [f"20{i:09d}" for i in range(20)],
            "label": [1] * 5 + [0] * 15,
            "score": np.linspace(1, 0, 20),
            "split": ["test"] * 20,
            # Todos los positivos en LIMA, TACNA tiene 0 positivos
            "departamento": ["LIMA"] * 10 + ["TACNA"] * 10,
        }
    )

    metrics = compute_metrics(df, ks=[5], n_bootstrap=20)
    assert metrics["por_segmento"]["TACNA"] is None
    assert metrics["por_segmento"]["LIMA"] is not None
    assert metrics["por_segmento"]["LIMA"]["n_positivos"] == 5


def test_all_scores_equal():
    """Todos los scores iguales no lanzan error y desempatan de forma determinística."""
    df = pd.DataFrame(
        {
            "ruc": ["20000000002", "20000000001", "20000000003"],
            "label": [0, 1, 0],
            "score": [0.5, 0.5, 0.5],
            "split": ["test"] * 3,
        }
    )

    metrics = compute_metrics(df, ks=[1], n_bootstrap=10)
    # Por orden estable por RUC asc, '20000000001' queda primero en top 1 (label=1)
    assert metrics["precision_at_k"]["1"] == 1.0


def test_rejects_train_data():
    """Rechaza datos con split == 'train' a menos que modo_diagnostico=True."""
    df = pd.DataFrame(
        {
            "label": [1, 0, 1, 0],
            "score": [0.8, 0.7, 0.3, 0.1],
            "split": ["train", "train", "test", "test"],
        }
    )

    with pytest.raises(ValueError, match="Datos de train detectados"):
        compute_metrics(df)

    # Con modo_diagnostico=True no debe fallar
    res = compute_metrics(df, ks=[1], n_bootstrap=10, modo_diagnostico=True)
    assert res is not None


def test_b0_recall_is_zero():
    """B0 Trivial asigna score 0.0 a todo."""
    df = pd.DataFrame(
        {
            "ruc": ["201", "202", "203"],
            "label": [1, 0, 0],
            "split": ["test"] * 3,
        }
    )
    b0_df = baseline_trivial(df)
    assert (b0_df["score"] == 0.0).all()
    assert b0_df["run_id"].iloc[0] == "baseline_b0_trivial"


def test_b1_three_levels():
    """B1 genera niveles distintos de score según señales heurísticas."""
    df = pd.DataFrame(
        {
            "ruc": ["201", "202", "203", "204"],
            "condicion": ["HABIDO", "NO HABIDO", "NO HALLADO", "HABIDO"],
            "nro_trab": [10, 0, 0, 5],
            "contrata_con_estado": [False, False, True, False],
            "label": [0, 1, 1, 0],
            "split": ["test"] * 4,
        }
    )
    b1_df = baseline_analyst_rule(df)
    unique_scores = b1_df["score"].nunique()
    assert unique_scores >= 3
    assert b1_df["score"].max() <= 1.0
    assert b1_df["score"].min() >= 0.0


def test_b2_random_reproducible():
    """B2 es reproducible con semilla fija y respeta contrato C2."""
    df = pd.DataFrame(
        {
            "ruc": [f"20{i}" for i in range(50)],
            "label": [1] * 5 + [0] * 45,
            "split": ["test"] * 50,
        }
    )
    b2_1 = baseline_random(df, seed=123)
    b2_2 = baseline_random(df, seed=123)
    pd.testing.assert_series_equal(b2_1["score"], b2_2["score"])


# ---------------------------------------------------------------------------
# Tests de flatten_metrics (Contrato C4)
# ---------------------------------------------------------------------------

C4_SCALAR_FIELDS = [
    "run_id", "split", "n", "n_positivos", "prevalencia",
    "pr_auc", "pr_auc_ic_bajo", "pr_auc_ic_alto",
    "roc_auc", "brier",
    "recall_at_100", "recall_at_500", "recall_at_1pct", "recall_at_5pct",
    "precision_at_100", "precision_at_500", "precision_at_1pct", "precision_at_5pct",
    "lift_at_100", "lift_at_500", "lift_at_1pct", "lift_at_5pct",
    "tp_1pct", "fp_1pct", "fn_1pct", "tn_1pct",
    "tp_5pct", "fp_5pct", "fn_5pct", "tn_5pct",
    "recall_lima", "recall_resto",
    "sagemaker_job_name", "instancia", "segundos_facturables", "costo_estimado_usd",
]


def _make_df_for_flatten(n: int = 1000, prevalence: float = 0.05) -> pd.DataFrame:
    np.random.seed(0)
    n_pos = max(1, int(n * prevalence))
    labels = [1] * n_pos + [0] * (n - n_pos)
    scores = np.linspace(1, 0, n).tolist()
    return pd.DataFrame({
        "ruc": [f"20{i:09d}" for i in range(n)],
        "label": labels,
        "score": scores,
        "split": ["test"] * n,
        "departamento": ["LIMA"] * (n // 2) + ["AREQUIPA"] * (n - n // 2),
        "run_id": ["run_test"] * n,
    })


def test_flatten_all_c4_fields_present():
    """flatten_metrics devuelve exactamente los campos escalares del contrato C4."""
    df = _make_df_for_flatten()
    nested = compute_metrics(df, ks=[100, 500], n_bootstrap=20, seed=42)
    flat = flatten_metrics(nested)

    for field in C4_SCALAR_FIELDS:
        assert field in flat, f"Campo C4 faltante: '{field}'"


def test_flatten_no_nested_dicts():
    """Ningún valor del dict plano es un dict o lista (Athena exige escalares)."""
    df = _make_df_for_flatten()
    nested = compute_metrics(df, ks=[100, 500], n_bootstrap=20, seed=42)
    flat = flatten_metrics(nested)

    for key, value in flat.items():
        assert not isinstance(value, (dict, list)), (
            f"Campo '{key}' contiene un sub-objeto ({type(value).__name__}), "
            "violando el esquema plano de C4."
        )


def test_flatten_sagemaker_fields_none_without_metadata():
    """Los campos de infraestructura son None cuando no se pasa sagemaker_metadata."""
    df = _make_df_for_flatten()
    nested = compute_metrics(df, ks=[100, 500], n_bootstrap=20, seed=42)
    flat = flatten_metrics(nested)

    assert flat["sagemaker_job_name"] is None
    assert flat["instancia"] is None
    assert flat["segundos_facturables"] is None
    assert flat["costo_estimado_usd"] is None


def test_flatten_sagemaker_fields_populated():
    """Los campos de infraestructura se propagan correctamente cuando se pasan."""
    df = _make_df_for_flatten()
    nested = compute_metrics(df, ks=[100, 500], n_bootstrap=20, seed=42)
    sm = {
        "sagemaker_job_name": "sunat-ssco-E1-LR-V2",
        "instancia": "ml.m5.xlarge",
        "segundos_facturables": 137.0,
        "costo_estimado_usd": 0.0078,
    }
    flat = flatten_metrics(nested, sagemaker_metadata=sm)

    assert flat["sagemaker_job_name"] == "sunat-ssco-E1-LR-V2"
    assert flat["instancia"] == "ml.m5.xlarge"
    assert flat["segundos_facturables"] == pytest.approx(137.0)
    assert flat["costo_estimado_usd"] == pytest.approx(0.0078)


# ---------------------------------------------------------------------------
# Tests de evaluate_and_export_baseline (B3 persistencia)
# ---------------------------------------------------------------------------

def _make_test_df_for_baselines(n: int = 1000) -> pd.DataFrame:
    np.random.seed(7)
    n_pos = 10
    labels = [1] * n_pos + [0] * (n - n_pos)
    scores_placeholder = np.zeros(n)
    return pd.DataFrame({
        "ruc": [f"20{i:09d}" for i in range(n)],
        "label": labels,
        "score": scores_placeholder,
        "split": ["test"] * n,
        "departamento": ["LIMA"] * (n // 2) + ["AREQUIPA"] * (n - n // 2),
        "run_id": ["placeholder"] * n,
    })


def test_evaluate_and_export_baseline_b0_produces_ndjson():
    """B0 produce un metrics.json de exactamente una línea con run_id correcto."""
    test_df = _make_test_df_for_baselines()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "metrics.json")
        flat = evaluate_and_export_baseline(
            baseline_trivial, test_df, output_path, ks=[100, 500], n_bootstrap=20
        )

        # El archivo existe y tiene exactamente una línea
        assert os.path.exists(output_path)
        with open(output_path, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1, f"Se esperaba 1 línea, se encontraron {len(lines)}"

        # El JSON es válido y tiene el run_id del baseline
        parsed = json.loads(lines[0])
        assert parsed["run_id"] == "baseline_b0_trivial"

        # La función también devuelve el mismo dict
        assert flat["run_id"] == "baseline_b0_trivial"


def test_missing_score_raises_valueerror():
    """Contrato C2 roto: falta columna 'score' → ValueError con mensaje descriptivo."""
    df_sin_score = pd.DataFrame(
        {
            "ruc": ["20000000001", "20000000002"],
            "label": [1, 0],
            "split": ["test", "test"],
        }
    )
    with pytest.raises(ValueError, match="score"):
        compute_metrics(df_sin_score)


def test_evaluate_and_export_baseline_sagemaker_fields_null():
    """Los baselines no tienen metadatos SageMaker — sus campos deben ser null."""
    test_df = _make_test_df_for_baselines()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "metrics.json")
        evaluate_and_export_baseline(
            baseline_random, test_df, output_path, ks=[100, 500], n_bootstrap=20
        )

        with open(output_path, encoding="utf-8") as f:
            parsed = json.loads(f.read())

        assert parsed["sagemaker_job_name"] is None
        assert parsed["instancia"] is None
        assert parsed["segundos_facturables"] is None
        assert parsed["costo_estimado_usd"] is None
