"""
Baselines de referencia para el modelo de scoring SSCO (Tarea B3).

Modelos baselines implementados:
- B0 Trivial: Siempre predice score = 0.0 (demuestra la paradoja de accuracy con desbalance).
- B1 Regla del Analista: Sistema de reglas basado en 3 señales heurísticas de riesgo operativo.
- B2 Azar: Asigna probabilidades aleatorias uniformes U(0, 1) con semilla determinística.

Todos los métodos retornan un DataFrame respetando el contrato C2 de predicciones.
"""

from typing import Any, Callable

import numpy as np
import pandas as pd

from src.models.evaluation.metrics import export_metrics_flat


def baseline_trivial(df: pd.DataFrame) -> pd.DataFrame:
    """
    Baseline B0 (Trivial): Asigna un score de 0.0 a todos los contribuyentes.
    Demuestra que un modelo con 99% accuracy puede tener Recall@K = 0.
    """
    res = df.copy()
    res["score"] = 0.0
    res["run_id"] = "baseline_b0_trivial"
    return res


def baseline_analyst_rule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Baseline B1 (Regla del Analista):
    Calcula una puntuación heurística sumando hasta 3 factores de riesgo:
    1. Condición tributaria irregular: 'NO HABIDO' o 'NO HALLADO'.
    2. Cero trabajadores declarados (nro_trabajadores == 0).
    3. Contrata con el Estado teniendo 1 o 0 trabajadores.

    Score normalizado en [0.0, 1.0] (puntos / 3.0).
    """
    res = df.copy()
    puntos = np.zeros(len(res), dtype=float)

    # 1. Condición tributaria
    if "condicion" in res.columns:
        cond_upper = res["condicion"].astype(str).str.upper().str.strip()
        puntos += cond_upper.isin(["NO HABIDO", "NO HALLADO"]).astype(float)
    elif "condicion_domicilio" in res.columns:
        cond_upper = res["condicion_domicilio"].astype(str).str.upper().str.strip()
        puntos += cond_upper.isin(["NO HABIDO", "NO HALLADO"]).astype(float)

    # Identificar columna de trabajadores
    col_trab = None
    for cand in ["nro_trab", "nro_trabajadores", "cant_trabajadores", "trabajadores"]:
        if cand in res.columns:
            col_trab = cand
            break

    # 2. Cero trabajadores
    if col_trab is not None:
        trab_vals = pd.to_numeric(res[col_trab], errors="coerce").fillna(0)
        puntos += (trab_vals == 0).astype(float)
    else:
        trab_vals = pd.Series(0, index=res.index)

    # 3. Contratación con Estado con <= 1 trabajador
    col_contrata = None
    for cand in [
        "contrata_con_estado",
        "proveedor_estado",
        "es_proveedor_estado",
        "tiene_adjudicaciones",
    ]:
        if cand in res.columns:
            col_contrata = cand
            break

    if col_contrata is not None:
        contrata_bool = res[col_contrata].astype(bool) | (res[col_contrata] == 1)
        puntos += (contrata_bool & (trab_vals <= 1)).astype(float)

    res["score"] = puntos / 3.0
    res["run_id"] = "baseline_b1_analyst_rule"
    return res


def baseline_random(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """
    Baseline B2 (Azar):
    Genera scores uniformes U(0, 1) determinísticos.
    Su PR-AUC converge a la prevalencia de la clase positiva y Lift@K ≈ 1.
    """
    res = df.copy()
    rng = np.random.RandomState(seed)
    res["score"] = rng.uniform(0.0, 1.0, size=len(res))
    res["run_id"] = "baseline_b2_random"
    return res


def evaluate_and_export_baseline(
    baseline_fn: Callable[[pd.DataFrame], pd.DataFrame],
    test_df: pd.DataFrame,
    output_path: str,
    ks: list[int] | None = None,
    n_bootstrap: int = 1000,
    seed: int = 42,
) -> dict[str, Any]:
    """
    Evalúa un baseline y persiste las métricas planas C4 en Single-line NDJSON.

    Permite que B0, B1 y B2 aparezcan como filas en el Leaderboard Athena
    junto a los modelos reales (E1–E6), con sagemaker_metadata=None porque
    los baselines no corren en SageMaker.

    Parámetros:
    -----------
    baseline_fn: callable
        Una de: baseline_trivial, baseline_analyst_rule, baseline_random.
    test_df: pd.DataFrame
        Conjunto de test con columnas 'label' y 'split' (contrato C2 parcial).
    output_path: str
        Destino del metrics.json. Soporta ruta local y s3://bucket/key.
        Convención: gold/model_metrics/run_id=<run_id>/metrics.json
    ks: list[int] | None
        Cortes K absolutos (por defecto [100, 500]).
    n_bootstrap: int
        Remuestreos para IC 95% de PR-AUC.
    seed: int
        Semilla del bootstrap.

    Retorna el dict plano C4 (mismo que se escribe en output_path).
    """
    if ks is None:
        ks = [100, 500]

    predictions = baseline_fn(test_df)

    return export_metrics_flat(
        predictions,
        output_path=output_path,
        sagemaker_metadata=None,
        ks=ks,
        n_bootstrap=n_bootstrap,
        seed=seed,
    )
