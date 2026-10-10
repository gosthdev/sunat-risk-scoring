"""Módulo de exportación de salidas para contratos C2 (predictions) y C3 (run_record).

Garantiza el cumplimiento estricto de:
- C2: predictions.parquet con rank_global descendente por score.
- C3: run_record.json como JSON de una sola línea (Single-line NDJSON).
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd


def _clean_float(val: Any, decimals: int = 4) -> float:
    """Convierte métricas a float sanitizado, evitando NaN en NDJSON estricto."""
    try:
        f = float(val)
        return 0.0 if np.isnan(f) else round(f, decimals)
    except (ValueError, TypeError):
        return 0.0


def exportar_predicciones_y_run_record(
    df_all: pd.DataFrame,
    X_all: pd.DataFrame,
    best_pipeline: Any,
    args: Any,
    cv_mean: float,
    cv_folds_scores: list[float],
    train_pr_auc: float,
    n_features_trans: int,
    elapsed_time: float,
    best_params: dict[str, Any],
    grilla_probada: dict[str, Any] | None = None,
    git_commit: str = "HEAD",
    version_sklearn: str = "1.3.2",
) -> dict[str, Any]:
    """Genera y exporta el Parquet de predicciones (C2) y el archivo run_record.json (C3)."""
    # 1. Calcular predicciones de probabilidad sobre toda la población
    probs = best_pipeline.predict_proba(X_all)[:, 1]

    # Resolver columna RUC de forma robusta
    ruc_col = (
        "ruc"
        if "ruc" in df_all.columns
        else ("RUC" if "RUC" in df_all.columns else None)
    )
    if ruc_col is None:
        raise ValueError(
            "No se encontró columna 'ruc' o 'RUC' en el DataFrame de entrada."
        )

    df_preds = pd.DataFrame(
        {
            "ruc": df_all[ruc_col].astype(str).values,
            "score": probs.astype(float),
            "label": df_all["label"].astype(int).values,
            "split": df_all["split"].astype(str).values,
        }
    )

    # Calcular rank_global descendente por score (1 = mayor probabilidad de riesgo)
    # En caso de empates, method="min" asigna el rango mínimo y desempatamos por ruc
    df_preds = df_preds.sort_values(
        by=["score", "ruc"], ascending=[False, True]
    ).reset_index(drop=True)
    df_preds["rank_global"] = np.arange(1, len(df_preds) + 1, dtype=np.int64)

    # Ruta de salida para C2
    out_preds_path = getattr(args, "ruta_salida_predicciones", "")
    if not out_preds_path:
        out_preds_path = os.path.join(
            args.output_dir, f"predictions_{args.run_id}.parquet"
        )

    _guardar_parquet(df_preds, out_preds_path)
    print(f"[INFO] Predicciones (Contrato C2) guardadas en: {out_preds_path}")

    # 2. Construir Run Record (Contrato C3)
    n_train = int((df_all["split"] == "train").sum())
    n_test = int((df_all["split"] == "test").sum())
    n_pos_train = int(((df_all["split"] == "train") & (df_all["label"] == 1)).sum())
    n_pos_test = int(((df_all["split"] == "test") & (df_all["label"] == 1)).sum())

    params_limpios = {
        str(k).replace("model__", ""): (
            v if isinstance(v, (int, float, str, bool, list)) else str(v)
        )
        for k, v in best_params.items()
    }

    grilla_limpia = {}
    if grilla_probada:
        grilla_limpia = {
            str(k).replace("model__", ""): (
                v if isinstance(v, (int, float, str, bool, list)) else str(v)
            )
            for k, v in grilla_probada.items()
        }

    run_record = {
        "run_id": args.run_id,
        "fecha": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_commit,
        "dataset_version": args.dataset_version,
        "variant": args.variant,
        "model_name": args.model_name,
        "usa_pesos": (str(args.usa_pesos).lower() in ["si", "true", "1"]),
        "hiperparametros_ganadores": params_limpios,
        "grilla_probada": grilla_limpia,
        "n_train": n_train,
        "n_test": n_test,
        "n_positivos_train": n_pos_train,
        "n_positivos_test": n_pos_test,
        "ruta_modelo": os.path.join(args.model_dir, "model.tar.gz"),
        "ruta_predicciones": out_preds_path,
        "semilla": int(args.seed),
        "version_sklearn": version_sklearn,
        "n_features_transformadas": int(n_features_trans),
        "segundos_entrenamiento": _clean_float(elapsed_time, 2),
        "metricas_cv": {
            "cv_pr_auc_mean": _clean_float(cv_mean, 4),
            "cv_pr_auc_fold": [_clean_float(x, 4) for x in cv_folds_scores],
            "train_pr_auc": _clean_float(train_pr_auc, 4),
        },
    }

    # Ruta de salida para C3
    out_record_path = getattr(args, "ruta_salida_run_record", "")
    if not out_record_path:
        out_record_path = os.path.join(
            args.output_dir, f"run_record_{args.run_id}.json"
        )

    _guardar_single_line_json(run_record, out_record_path)
    print(
        f"[INFO] Run Record (Contrato C3 - Single-line NDJSON) guardado en: {out_record_path}"
    )

    return run_record


def _guardar_parquet(df: pd.DataFrame, path: str) -> None:
    """Guarda un DataFrame como Parquet en S3 o almacenamiento local."""
    if path.startswith("s3://"):
        df.to_parquet(path, index=False, engine="pyarrow")
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        df.to_parquet(path, index=False, engine="pyarrow")


def _guardar_single_line_json(record: dict[str, Any], path: str) -> None:
    """Guarda un diccionario como JSON de una sola línea estricta."""
    json_str = json.dumps(
        record, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    if path.startswith("s3://"):
        try:
            import smart_open  # type: ignore[import-untyped]

            with smart_open.open(path, "w", encoding="utf-8") as f:
                f.write(json_str + "\n")
        except ImportError:
            import boto3

            bucket_and_key = path.replace("s3://", "").split("/", 1)
            bucket = bucket_and_key[0]
            key = bucket_and_key[1] if len(bucket_and_key) > 1 else ""
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=bucket, Key=key, Body=(json_str + "\n").encode("utf-8")
            )
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(json_str + "\n")
