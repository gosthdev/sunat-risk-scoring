"""Script principal de entrenamiento para el Modelo Base SSCO en scikit-learn (Tarea A4).

Compatible con:
- Ejecución local y pruebas unitarias.
- SageMaker Training Job en modo script.
- Publicación de métricas CloudWatch (Contrato C5).
- Exportación de predicciones (Contrato C2) y run_record NDJSON (Contrato C3).
"""

import argparse
import glob
import os
import sys
import tarfile
import time
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.model_selection import GridSearchCV, PredefinedSplit, StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.tree import DecisionTreeClassifier

# Imports de módulos internos de models
try:
    from src.models.explainability import (
        extraer_explicabilidad_lr,
        extraer_reglas_dt,
        obtener_nombres_features,
    )
    from src.models.export_outputs import exportar_predicciones_y_run_record
except ImportError:
    from explainability import (  # type: ignore[no-redef]
        extraer_explicabilidad_lr,
        extraer_reglas_dt,
        obtener_nombres_features,
    )
    from export_outputs import (  # type: ignore[no-redef]
        exportar_predicciones_y_run_record,
    )


class Log1pTransformer(BaseEstimator, TransformerMixin):
    """Transformador para compresión de sesgo en variables monetarias y conteos (np.log1p)."""

    def fit(self, X: Any, y: Any = None) -> "Log1pTransformer":
        return self

    def transform(self, X: Any) -> np.ndarray:
        arr = np.nan_to_num(np.asarray(X, dtype=np.float64), nan=0.0)
        return np.log1p(np.maximum(0.0, arr))


def parse_args(args_list: list[str] | None = None) -> argparse.Namespace:
    """Parsea argumentos de línea de comandos para el entrenamiento."""
    parser = argparse.ArgumentParser(
        description="Entrenamiento Modelo Base SSCO - scikit-learn"
    )

    parser.add_argument(
        "--train_dir", type=str, default=os.getenv("SM_CHANNEL_TRAIN", "./data/input")
    )
    parser.add_argument(
        "--model_dir", type=str, default=os.getenv("SM_MODEL_DIR", "./data/model")
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=os.getenv("SM_OUTPUT_DATA_DIR", "./data/output"),
    )
    parser.add_argument(
        "--run_id", type=str, required=True, help="Identificador único de la corrida"
    )
    parser.add_argument("--model_name", type=str, choices=["LR", "DT"], required=True)
    parser.add_argument("--variant", type=str, choices=["V1", "V2"], default="V2")
    parser.add_argument("--usa_pesos", type=str, choices=["si", "no"], default="si")
    parser.add_argument("--dataset_version", type=str, default="v01")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--ruta_salida_predicciones", type=str, default="")
    parser.add_argument("--ruta_salida_run_record", type=str, default="")

    if args_list is not None:
        return parser.parse_args(args_list)
    return parser.parse_args()


def cargar_datos(train_dir: str) -> pd.DataFrame:
    """Carga el dataset de entrada en formato Parquet desde un archivo o directorio."""
    if os.path.isfile(train_dir):
        return pd.read_parquet(train_dir)

    parquet_files = sorted(
        glob.glob(os.path.join(train_dir, "**/*.parquet"), recursive=True)
    )
    if parquet_files:
        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)

    if os.path.exists(train_dir):
        # Intentar leer directamente con pandas / pyarrow si es directorio parquet
        try:
            return pd.read_parquet(train_dir)
        except Exception:  # noqa: BLE001, S110
            pass

    raise FileNotFoundError(
        f"No se encontraron archivos Parquet válidos en: {train_dir}"
    )


def seleccionar_columnas(df: pd.DataFrame, variant: str) -> tuple[list[str], list[str]]:
    """Determina las columnas numéricas y categóricas según el esquema y variante."""
    candidatos_num = [
        "monto_total_soles",
        "monto_mediano_soles",
        "monto_maximo_soles",
        "n_ordenes",
        "total_ordenes_validas",
        "n_entidades_distintas",
        "pct_monto_en_entidad_principal",
        "pct_ordenes_anuladas",
        "antiguedad_contratacion_estado_dias",
        "antiguedad_meses",
        "nro_trabajadores",
        "sin_trabajadores",
        "monto_por_trabajador",
        "informalidad_epen_departamento",
    ]
    num_cols = [c for c in candidatos_num if c in df.columns]

    candidatos_cat = [
        "ciiu_principal",
        "actividad_economica_principal",
        "departamento",
        "tipo_contribuyente",
    ]
    if variant == "V1":
        candidatos_cat.extend(["Estado", "Condicion"])

    cat_cols = [c for c in candidatos_cat if c in df.columns]

    return num_cols, cat_cols


def construir_pipeline_preprocesamiento(
    num_cols: list[str],
    cat_cols: list[str],
    es_lr: bool = True,
) -> ColumnTransformer:
    """Construye el ColumnTransformer para preprocesamiento sin fuga de datos."""
    num_steps: list[tuple[str, Any]] = [
        ("log1p", Log1pTransformer()),
        ("imputer", SimpleImputer(strategy="median")),
    ]
    if es_lr:
        num_steps.append(("scaler", StandardScaler()))

    num_pipeline = Pipeline(num_steps)

    cat_pipeline = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="constant", fill_value="DESCONOCIDO")),
            (
                "ohe",
                OneHotEncoder(
                    handle_unknown="infrequent_if_exist",
                    max_categories=30,
                    sparse_output=False,
                ),
            ),
        ]
    )

    transformers = []
    if num_cols:
        transformers.append(("num", num_pipeline, num_cols))
    if cat_cols:
        transformers.append(("cat", cat_pipeline, cat_cols))

    return ColumnTransformer(transformers=transformers, remainder="drop")


def empaquetar_modelo_sagemaker(model_dir: str) -> str:
    """Empaqueta los artefactos del modelo en model.tar.gz para SageMaker."""
    tar_path = os.path.join(model_dir, "model.tar.gz")
    joblib_path = os.path.join(model_dir, "model.joblib")
    with tarfile.open(tar_path, "w:gz") as tar:
        if os.path.exists(joblib_path):
            tar.add(joblib_path, arcname="model.joblib")
    return tar_path


def main(args_list: list[str] | None = None) -> int:
    """Función de entrada para el flujo de entrenamiento."""
    start_time = time.time()
    args = parse_args(args_list)

    print(f"[INFO] Iniciando entrenamiento SSCO Run ID: {args.run_id}")
    print(
        f"[INFO] Parametros: Modelo={args.model_name}, Variante={args.variant}, Pesos={args.usa_pesos}"
    )

    # 1. Cargar y particionar datos
    df = cargar_datos(args.train_dir)
    num_cols, cat_cols = seleccionar_columnas(df, args.variant)
    features = num_cols + cat_cols

    df_train = df[df["split"] == "train"].copy()

    X_train = df_train[features]
    y_train: np.ndarray = np.asarray(df_train["label"].values, dtype=int)
    folds_train: np.ndarray = np.asarray(df_train["fold"].values)

    n_train = len(df_train)
    n_pos_train = int(y_train.sum())

    print(f"[METRIC] n_train={n_train}")
    print(f"[METRIC] n_pos_train={n_pos_train}")

    # 2. Configurar Modelo y Pipeline
    es_lr = args.model_name == "LR"
    preprocessor = construir_pipeline_preprocesamiento(num_cols, cat_cols, es_lr=es_lr)
    class_weight = "balanced" if args.usa_pesos == "si" else None

    if es_lr:
        model = LogisticRegression(
            class_weight=class_weight, random_state=args.seed, max_iter=1000
        )
        param_grid = {"model__C": [0.01, 0.1, 1.0, 10.0]}
    else:
        model = DecisionTreeClassifier(
            class_weight=class_weight, random_state=args.seed
        )
        param_grid = {
            "model__max_depth": [3, 5, 6],
            "model__min_samples_leaf": [50, 200] if len(df_train) > 1000 else [2, 5],
        }

    full_pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    # 3. Validación Cruzada con Folds Predefinidos
    # Si los folds predefinidos son válidos (5 folds 0..4), usar PredefinedSplit; caso contrario StratifiedKFold
    unique_folds: np.ndarray = (
        np.unique(folds_train[folds_train >= 0])
        if (folds_train is not None and len(folds_train) > 0)
        else np.array([])
    )
    if len(unique_folds) >= 2:
        cv_splitter = PredefinedSplit(test_fold=folds_train)
    else:
        cv_splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=args.seed)

    grid_search = GridSearchCV(
        estimator=full_pipeline,
        param_grid=param_grid,
        scoring="average_precision",
        cv=cv_splitter,
        refit=True,
        n_jobs=-1,
    )

    # 4. Ajuste del modelo sobre Train únicamente
    grid_search.fit(X_train, y_train)
    best_pipeline = grid_search.best_estimator_

    # Extraer cantidad de variables transformadas
    X_train_trans = best_pipeline.named_steps["preprocessor"].transform(X_train)
    n_features_trans = X_train_trans.shape[1]
    print(f"[METRIC] n_features_transformadas={n_features_trans}")

    # 5. Métricas de CV y Entrenamiento
    cv_mean = float(grid_search.best_score_)
    best_index = grid_search.best_index_
    cv_folds_scores = []

    # Extraer score individual de cada fold evaluado
    fold_keys = [
        k
        for k in grid_search.cv_results_
        if k.startswith("split") and k.endswith("_test_score")
    ]
    fold_keys = sorted(fold_keys)
    for idx, f_key in enumerate(fold_keys):
        f_score = float(grid_search.cv_results_[f_key][best_index])
        cv_folds_scores.append(f_score)
        print(f"[METRIC] cv_pr_auc_fold_{idx}={f_score:.4f}")

    print(f"[METRIC] cv_pr_auc_mean={cv_mean:.4f}")

    y_train_pred_prob = best_pipeline.predict_proba(X_train)[:, 1]
    train_pr_auc = float(average_precision_score(y_train, y_train_pred_prob))
    print(f"[METRIC] train_pr_auc={train_pr_auc:.4f}")

    elapsed_time = round(time.time() - start_time, 2)
    print(f"[METRIC] train_seconds={elapsed_time:.2f}")
    print(f"[INFO] Hiperparametros ganadores: {grid_search.best_params_}")

    # 6. Serialización de Artefactos de Modelo
    os.makedirs(args.model_dir, exist_ok=True)
    joblib_model_path = os.path.join(args.model_dir, "model.joblib")
    joblib.dump(best_pipeline, joblib_model_path)
    empaquetar_modelo_sagemaker(args.model_dir)

    # 7. Exportación de Predicciones (C2) y Run Record (C3)
    exportar_predicciones_y_run_record(
        df_all=df,
        X_all=df[features],
        best_pipeline=best_pipeline,
        args=args,
        cv_mean=cv_mean,
        cv_folds_scores=cv_folds_scores,
        train_pr_auc=train_pr_auc,
        n_features_trans=n_features_trans,
        elapsed_time=elapsed_time,
        best_params=grid_search.best_params_,
        grilla_probada=param_grid,
    )

    # 8. Extraer Explicabilidad preliminar (Tarea A6)
    feature_names = obtener_nombres_features(
        best_pipeline.named_steps["preprocessor"], num_cols, cat_cols
    )
    if es_lr:
        df_coefs = extraer_explicabilidad_lr(best_pipeline, feature_names)
        print("[INFO] Top 5 Features por Coeficiente Absoluto (LR):")
        print(df_coefs.head(5)[["feature", "coeficiente", "impacto"]].to_string())
    else:
        rules_text = extraer_reglas_dt(best_pipeline, feature_names, max_depth=3)
        print("[INFO] Reglas principales de Decision Tree (profundidad <= 3):")
        print(rules_text[:500] + "...")

    print("[INFO] Entrenamiento y exportacion finalizada exitosamente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
