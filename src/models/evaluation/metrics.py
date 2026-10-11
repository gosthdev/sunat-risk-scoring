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

import json
import os
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

        # PR-AUC no está definida sin positivos: se descarta el remuestreo en lugar
        # de contarlo como 0.0 (que sesgaría el IC hacia abajo).
        if np.sum(sample_y) == 0:
            continue
        if np.sum(sample_y) == n:
            boot_scores.append(1.0)
        else:
            score_val = average_precision_score(sample_y, sample_score)
            boot_scores.append(float(score_val))

    if not boot_scores:
        return (0.0, 0.0)

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


def flatten_metrics(
    nested: dict[str, Any],
    sagemaker_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Convierte el dict anidado de compute_metrics() al esquema plano del Contrato C4.

    El resultado es serializable en Single-line NDJSON y consultable directamente
    en Amazon Athena sin necesidad de deserializar sub-objetos.

    Parámetros:
    -----------
    nested: dict
        Salida directa de compute_metrics().
    sagemaker_metadata: dict | None
        Metadatos opcionales del Training Job de SageMaker. Si se omite, los
        campos de infraestructura quedan como None (correcto para baselines).
        Claves esperadas: 'sagemaker_job_name', 'instancia',
                          'segundos_facturables', 'costo_estimado_usd'.
    """
    cm1 = nested.get("matriz_confusion", {}).get("top_1pct", {})
    cm5 = nested.get("matriz_confusion", {}).get("top_5pct", {})
    por_seg = nested.get("por_segmento", {}) or {}
    pr_auc_block = nested.get("pr_auc", {})

    recall_k = nested.get("recall_at_k", {})
    precision_k = nested.get("precision_at_k", {})
    lift_k = nested.get("lift_at_k", {})

    lima = por_seg.get("LIMA") or {}
    resto = por_seg.get("RESTO") or {}

    sm = sagemaker_metadata or {}

    return {
        "run_id": nested.get("run_id"),
        "split": nested.get("split", "test"),
        "n": nested.get("n"),
        "n_positivos": nested.get("n_positivos"),
        "prevalencia": nested.get("prevalencia"),
        "pr_auc": pr_auc_block.get("valor")
        if isinstance(pr_auc_block, dict)
        else pr_auc_block,
        "pr_auc_ic_bajo": pr_auc_block.get("ic_bajo")
        if isinstance(pr_auc_block, dict)
        else None,
        "pr_auc_ic_alto": pr_auc_block.get("ic_alto")
        if isinstance(pr_auc_block, dict)
        else None,
        "roc_auc": nested.get("roc_auc"),
        "brier": nested.get("brier"),
        "recall_at_100": recall_k.get("100"),
        "recall_at_500": recall_k.get("500"),
        "recall_at_1pct": recall_k.get("1pct"),
        "recall_at_5pct": recall_k.get("5pct"),
        "precision_at_100": precision_k.get("100"),
        "precision_at_500": precision_k.get("500"),
        "precision_at_1pct": precision_k.get("1pct"),
        "precision_at_5pct": precision_k.get("5pct"),
        "lift_at_100": lift_k.get("100"),
        "lift_at_500": lift_k.get("500"),
        "lift_at_1pct": lift_k.get("1pct"),
        "lift_at_5pct": lift_k.get("5pct"),
        "tp_1pct": cm1.get("TP"),
        "fp_1pct": cm1.get("FP"),
        "fn_1pct": cm1.get("FN"),
        "tn_1pct": cm1.get("TN"),
        "tp_5pct": cm5.get("TP"),
        "fp_5pct": cm5.get("FP"),
        "fn_5pct": cm5.get("FN"),
        "tn_5pct": cm5.get("TN"),
        "recall_lima": lima.get("recall_at_5pct"),
        "recall_resto": resto.get("recall_at_5pct"),
        "sagemaker_job_name": sm.get("sagemaker_job_name"),
        "instancia": sm.get("instancia"),
        "segundos_facturables": sm.get("segundos_facturables"),
        "costo_estimado_usd": sm.get("costo_estimado_usd"),
    }


def _sanear_nan(value: Any) -> Any:
    """NaN/inf no son JSON válido ni los lee Athena: se reemplazan por null."""
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def _serializar_ndjson(flat: dict[str, Any]) -> str:
    """Serializa el registro C4 en exactamente una línea, sin NaN."""
    limpio = {k: _sanear_nan(v) for k, v in flat.items()}
    return json.dumps(limpio, ensure_ascii=False, allow_nan=False) + "\n"


def export_metrics_flat(
    df: pd.DataFrame,
    output_path: str,
    sagemaker_metadata: dict[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Calcula métricas, aplana al esquema C4 y serializa en Single-line NDJSON.

    El archivo resultante tiene exactamente una línea de texto (requisito de
    AWS Glue SerDe y Amazon Athena para NDJSON).

    Parámetros:
    -----------
    df: pd.DataFrame
        DataFrame de predicciones (mismo contrato que compute_metrics()).
    output_path: str
        Ruta de destino. Soporta rutas locales y s3://bucket/key.
    sagemaker_metadata: dict | None
        Metadatos opcionales del Training Job (ver flatten_metrics).
    **kwargs:
        Argumentos adicionales pasados directamente a compute_metrics()
        (p. ej. ks, n_bootstrap, seed, modo_diagnostico).
    """
    nested = compute_metrics(df, **kwargs)
    flat = flatten_metrics(nested, sagemaker_metadata=sagemaker_metadata)
    line = _serializar_ndjson(flat)

    if output_path.startswith("s3://"):
        import boto3

        parts = output_path[5:].split("/", 1)
        bucket, key = parts[0], parts[1] if len(parts) > 1 else ""
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=line.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True) if os.path.dirname(
            output_path
        ) else None
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(line)

    return flat


def main(argv: list[str] | None = None) -> int:
    """CLI: evalúa un Parquet de predicciones C2 y escribe metrics.json (C4).

    Con --sagemaker_job consulta el Training Job y completa los campos de costo.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Evaluación oficial de test (C4)")
    parser.add_argument(
        "--predictions", required=True, help="Parquet C2 (local o s3://)"
    )
    parser.add_argument("--output", required=True, help="Destino de metrics.json")
    parser.add_argument(
        "--sagemaker_job", default="", help="TrainingJobName (= run_id)"
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    args = parser.parse_args(argv)

    df = pd.read_parquet(args.predictions)
    metadata = None
    if args.sagemaker_job:
        from src.models.fetch_sagemaker_metrics import get_job_cost_info

        info = get_job_cost_info(args.sagemaker_job, region=args.region)
        metadata = {
            k: info[k]
            for k in (
                "sagemaker_job_name",
                "instancia",
                "segundos_facturables",
                "costo_estimado_usd",
            )
        }
    export_metrics_flat(df, args.output, sagemaker_metadata=metadata)
    print(f"[OK] metrics.json escrito en {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
