"""
Librería de métricas de evaluación para el modelo de scoring SSCO.

Métricas calculadas:
- PR-AUC (scikit-learn average_precision_score) con IC 95% vía Bootstrap.
- ROC-AUC (scikit-learn roc_auc_score).
- Precision@K, Recall@K, Lift@K en cortes absolutos (ej. 100, 500) y relativos (1%, 5%).
- Brier Score (calibración).
- Matrices de confusión (TP, FP, FN, TN) en cortes operativos (1%, 5%).
- Desglose por segmento geográfico (Lima vs. Resto y departamentos principales).
"""

from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score


def _resolve_ordering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ordena determinísticamente por score descendente.
    Para desempatar scores idénticos, se usa 'ruc' ascendente si existe;
    de lo contrario, preserva el índice original con mergesort estable.
    """
    if "ruc" in df.columns:
        return df.sort_values(
            by=["score", "ruc"], ascending=[False, True], kind="mergesort"
        )
    return df.sort_values(by=["score"], ascending=[False], kind="mergesort")


def _calculate_confusion_matrix(
    y_true: np.ndarray, top_k_mask: np.ndarray
) -> dict[str, int]:
    """
    Calcula TP, FP, FN, TN dado un vector booleano indicando selección en top K.
    """
    tp = int(np.sum((top_k_mask == 1) & (y_true == 1)))
    fp = int(np.sum((top_k_mask == 1) & (y_true == 0)))
    fn = int(np.sum((top_k_mask == 0) & (y_true == 1)))
    tn = int(np.sum((top_k_mask == 0) & (y_true == 0)))
    return {"TP": tp, "FP": fp, "FN": fn, "TN": tn}


def _bootstrap_pr_auc_ci(
    y_true: np.ndarray,
    y_score: np.ndarray,
    n_bootstrap: int = 1000,
    seed: int = 42,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """
    Calcula el intervalo de confianza de PR-AUC usando Bootstrap no paramétrico.
    Garantiza reproducibilidad estricta mediante semilla fija.
    """
    n = len(y_true)
    if n == 0 or np.sum(y_true) == 0:
        return (0.0, 0.0)

    # Si todos los labels son positivos o no hay variación de positivos
    if np.sum(y_true) == n:
        return (1.0, 1.0)

    rng = np.random.RandomState(seed)
    boot_scores: list[float] = []

    for _ in range(n_bootstrap):
        indices = rng.randint(0, n, size=n)
        sample_y = y_true[indices]
        sample_score = y_score[indices]

        # Si el remuestreo no tiene positivos o solo tiene positivos
        if np.sum(sample_y) == 0:
            boot_scores.append(0.0)
        elif np.sum(sample_y) == n:
            boot_scores.append(1.0)
        else:
            score_val = average_precision_score(sample_y, sample_score)
            boot_scores.append(float(score_val))

    low_p = (alpha / 2.0) * 100
    high_p = (1.0 - alpha / 2.0) * 100

    ic_low = float(np.percentile(boot_scores, low_p))
    ic_high = float(np.percentile(boot_scores, high_p))

    return (ic_low, ic_high)


def compute_metrics(
    df: pd.DataFrame,
    ks: list[int] | None = None,
    n_bootstrap: int = 1000,
    seed: int = 42,
    modo_diagnostico: bool = False,
) -> dict[str, Any]:
    """
    Calcula el reporte completo de métricas oficiales (Contrato C4).

    Parámetros:
    -----------
    df: pd.DataFrame
        DataFrame con al menos 'score', 'label' (o 'target').
        Puede contener 'split', 'departamento', 'ruc', 'run_id'.
    ks: list of int, opcional
        Lista de K absolutos a evaluar (por defecto: [100, 500]).
    n_bootstrap: int
        Número de remuestreos para el IC al 95% (por defecto 1000).
    seed: int
        Semilla para el generador pseudoaleatorio del bootstrap.
    modo_diagnostico: bool
        Si es False, rechaza datasets que contengan split == 'train'.
        Solo evalúa sobre split == 'test' o datasets de prueba dedicados.
    """
    if df.empty:
        raise ValueError("El DataFrame de predicciones está vacío.")

    # Estandarizar nombre de label si viene como target
    work_df = df.copy()
    if "label" not in work_df.columns:
        if "target" in work_df.columns:
            work_df = work_df.rename(columns={"target": "label"})
        else:
            raise ValueError(
                "El DataFrame debe contener la columna 'label' o 'target'."
            )

    if "score" not in work_df.columns:
        raise ValueError("El DataFrame debe contener la columna 'score'.")

    # Validación de división de datos (split)
    if "split" in work_df.columns:
        splits_present = set(work_df["split"].astype(str).str.lower().unique())
        if not modo_diagnostico and "train" in splits_present:
            raise ValueError(
                "Datos de train detectados en el DataFrame. Por diseño, las métricas oficiales "
                "solo deben calcularse sobre el conjunto de test. Active 'modo_diagnostico=True' "
                "si requiere evaluar deliberadamente sobre train."
            )
        # Si contiene split, filtramos test a menos que modo_diagnostico esté activo y no haya test
        if "test" in splits_present:
            work_df = work_df[work_df["split"].astype(str).str.lower() == "test"].copy()
        elif not modo_diagnostico:
            raise ValueError("No se encontraron registros con split == 'test'.")

    n_total = len(work_df)
    if n_total == 0:
        raise ValueError("No hay registros en el conjunto de test.")

    # Ordenamiento determinístico resolviendo empates
    work_df = _resolve_ordering(work_df)

    y_true = work_df["label"].to_numpy().astype(int)
    y_score = work_df["score"].to_numpy().astype(float)

    n_positives = int(np.sum(y_true))
    prevalence = float(n_positives / n_total) if n_total > 0 else 0.0

    # Determinar los cortes K
    if ks is None:
        ks = [100, 500]

    # Validar que ningún K absoluto exceda el total
    for k in ks:
        if k > n_total:
            raise ValueError(
                f"El valor de K={k} excede el tamaño del conjunto de datos (N={n_total})."
            )

    k_percent_1 = max(1, int(np.ceil(0.01 * n_total)))
    k_percent_5 = max(1, int(np.ceil(0.05 * n_total)))

    cutoffs: dict[str, int] = {}
    for k in ks:
        cutoffs[str(k)] = k
    cutoffs["1pct"] = k_percent_1
    cutoffs["5pct"] = k_percent_5

    # Métricas @ K
    precision_at_k: dict[str, float] = {}
    recall_at_k: dict[str, float] = {}
    lift_at_k: dict[str, float] = {}

    for label_k, k_val in cutoffs.items():
        k_val = min(k_val, n_total)
        top_positives = int(np.sum(y_true[:k_val]))

        prec = float(top_positives / k_val) if k_val > 0 else 0.0
        rec = float(top_positives / n_positives) if n_positives > 0 else 0.0
        lift = float(prec / prevalence) if prevalence > 0 else 1.0

        precision_at_k[label_k] = prec
        recall_at_k[label_k] = rec
        lift_at_k[label_k] = lift

    # Matrices de confusión en cortes 1% y 5%
    mask_1pct = np.zeros(n_total, dtype=int)
    mask_1pct[:k_percent_1] = 1
    cm_1pct = _calculate_confusion_matrix(y_true, mask_1pct)

    mask_5pct = np.zeros(n_total, dtype=int)
    mask_5pct[:k_percent_5] = 1
    cm_5pct = _calculate_confusion_matrix(y_true, mask_5pct)

    # PR-AUC
    if n_positives == 0:
        pr_auc_val = 0.0
        ic_bajo, ic_alto = 0.0, 0.0
    elif n_positives == n_total:
        pr_auc_val = 1.0
        ic_bajo, ic_alto = 1.0, 1.0
    else:
        pr_auc_val = float(average_precision_score(y_true, y_score))
        if n_bootstrap > 0:
            ic_bajo, ic_alto = _bootstrap_pr_auc_ci(
                y_true=y_true,
                y_score=y_score,
                n_bootstrap=n_bootstrap,
                seed=seed,
            )
        else:
            ic_bajo, ic_alto = pr_auc_val, pr_auc_val

    # ROC-AUC
    if n_positives in (0, n_total):
        roc_auc_val = None
    else:
        roc_auc_val = float(roc_auc_score(y_true, y_score))

    # Brier Score
    brier_val = float(brier_score_loss(y_true, y_score))

    # Métricas por Segmento Geográfico (si columna 'departamento' existe)
    por_segmento: dict[str, Any] = {}
    if "departamento" in work_df.columns:
        # Segmentación: LIMA vs RESTO
        work_df["es_lima"] = (
            work_df["departamento"]
            .astype(str)
            .str.upper()
            .str.strip()
            .isin(
                [
                    "LIMA",
                    "LIMA METROPOLITANA",
                    "PROVINCIA CONSTITUCIONAL DEL CALLAO",
                    "CALLAO",
                ]
            )
        )

        segments_to_eval = {
            "LIMA": work_df[work_df["es_lima"]],
            "RESTO": work_df[~work_df["es_lima"]],
        }

        # También incluir los departamentos con positivos más frecuentes
        depts = work_df["departamento"].dropna().unique()
        for dept in depts:
            dept_key = str(dept).upper().strip()
            if dept_key not in segments_to_eval:
                segments_to_eval[dept_key] = work_df[work_df["departamento"] == dept]

        for seg_name, seg_df in segments_to_eval.items():
            seg_n = len(seg_df)
            if seg_n == 0:
                continue

            seg_y_true = seg_df["label"].to_numpy().astype(int)
            seg_positives = int(np.sum(seg_y_true))

            if seg_positives == 0:
                # Regla de diseño: si el segmento tiene 0 positivos -> devuelve None (null)
                por_segmento[seg_name] = None
            else:
                seg_y_score = seg_df["score"].to_numpy().astype(float)
                seg_prev = float(seg_positives / seg_n)
                seg_pr_auc = (
                    float(average_precision_score(seg_y_true, seg_y_score))
                    if seg_positives < seg_n
                    else 1.0
                )

                # Recall@5% en segmento
                k_seg_5 = max(1, int(np.ceil(0.05 * seg_n)))
                seg_top_pos = int(np.sum(seg_y_true[:k_seg_5]))
                seg_rec_5 = float(seg_top_pos / seg_positives)

                por_segmento[seg_name] = {
                    "n": seg_n,
                    "n_positivos": seg_positives,
                    "prevalencia": seg_prev,
                    "pr_auc": seg_pr_auc,
                    "recall_at_5pct": seg_rec_5,
                }

    run_id = (
        str(work_df["run_id"].iloc[0]) if "run_id" in work_df.columns else "default_run"
    )

    return {
        "run_id": run_id,
        "split": "test",
        "n": n_total,
        "n_positivos": n_positives,
        "prevalencia": prevalence,
        "pr_auc": {
            "valor": pr_auc_val,
            "ic_bajo": ic_bajo,
            "ic_alto": ic_alto,
        },
        "roc_auc": roc_auc_val,
        "recall_at_k": recall_at_k,
        "precision_at_k": precision_at_k,
        "lift_at_k": lift_at_k,
        "brier": brier_val,
        "matriz_confusion": {
            "top_1pct": cm_1pct,
            "top_5pct": cm_5pct,
        },
        "por_segmento": por_segmento,
        "metodo_pr_auc": "average_precision_sklearn",
    }
