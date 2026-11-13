"""Script principal de entrenamiento para el Modelo Base SSCO en scikit-learn (Tarea A4).

Compatible con:
- Ejecución local y pruebas unitarias.
- SageMaker Training Job en modo script.
- Publicación de métricas CloudWatch (Contrato C5).
- Exportación de predicciones (Contrato C2) y run_record NDJSON (Contrato C3).
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import tarfile
import time
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin, clone
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
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
        extraer_importancias_dt,
        extraer_importancias_permutacion,
        extraer_reglas_dt,
        obtener_nombres_features,
    )
    from src.models.export_outputs import exportar_predicciones_y_run_record
except ImportError:
    from explainability import (  # type: ignore[no-redef]
        extraer_explicabilidad_lr,
        extraer_importancias_dt,
        extraer_importancias_permutacion,
        extraer_reglas_dt,
        obtener_nombres_features,
    )
    from export_outputs import (  # type: ignore[no-redef]
        exportar_predicciones_y_run_record,
    )


# Variables monetarias y de conteo asimétricas (log1p). El resto de numéricas
# (porcentajes, binarias, índices) solo se imputa y estandariza.
COLS_LOG1P = {
    "monto_total_soles",
    "monto_mediano_soles",
    "monto_maximo_soles",
    "n_ordenes",
    "total_ordenes_validas",
    "n_entidades_distintas",
    "antiguedad_contratacion_estado_dias",
    "nro_trabajadores",
    "monto_por_trabajador",
}

# Tope de categorías por variable (D-A4): ~25 departamentos, ~30 el resto (CIIU, etc.).
MAX_CATEGORIAS = {"departamento": 25}
MAX_CATEGORIAS_DEFAULT = 30


class Log1pTransformer(BaseEstimator, TransformerMixin):
    """Transformador para compresión de sesgo en variables monetarias y conteos (np.log1p)."""

    def fit(self, X: Any, y: Any = None) -> Log1pTransformer:
        return self

    def transform(self, X: Any) -> np.ndarray:
        # Preserva NaN: la imputación con mediana (y su bandera de nulo) ocurre después.
        arr = np.asarray(X, dtype=np.float64)
        return np.log1p(np.maximum(0.0, arr))

    def get_feature_names_out(self, input_features: Any = None) -> np.ndarray:
        return np.asarray(input_features, dtype=object)


class WinsorizerSuperior(BaseEstimator, TransformerMixin):
    """Recorta la cola extrema por arriba con un percentil ajustado solo en train.

    Con montos casi siempre en cero y máximos de cientos de millones, unos pocos
    registros dominan la escala de la regresión logística. Si el percentil es 0
    (columna casi toda en cero) no se recorta, para no aplastar la señal.
    """

    def __init__(self, cuantil: float = 0.995) -> None:
        self.cuantil = cuantil

    def fit(self, X: Any, y: Any = None) -> WinsorizerSuperior:
        arr = np.asarray(X, dtype=np.float64)
        with np.errstate(all="ignore"):
            topes = np.nanquantile(arr, self.cuantil, axis=0)
        topes = np.atleast_1d(topes).astype(np.float64)
        topes[~(topes > 0)] = np.inf
        self.topes_ = topes
        return self

    def transform(self, X: Any) -> np.ndarray:
        arr = np.asarray(X, dtype=np.float64)
        return np.minimum(arr, self.topes_)

    def get_feature_names_out(self, input_features: Any = None) -> np.ndarray:
        return np.asarray(input_features, dtype=object)


# Entradas del target encoding: columnas crudas y fold. Las tasas se calculan dentro del
# Pipeline para que cada ronda de la CV use solo su parte de entrenamiento.
TE_M = 100
TE_ENTRADAS = ["ciiu_principal", "departamento", "ruc_prefijo", "ruc_correlativo"]
TE_CLAVES = {
    "te_ciiu": ("ciiu_principal",),
    "te_ciiu_depto": ("ciiu_principal", "departamento"),
    "te_cohorte_ruc": ("ruc_prefijo", "cohorte"),
}


def _codigos(serie: pd.Series, universo: pd.Index) -> np.ndarray:
    """Posición de cada valor en `universo`; -1 si es nulo o no existe."""
    if isinstance(serie.dtype, pd.CategoricalDtype):
        cod = serie.cat.codes.to_numpy()
        remap = universo.get_indexer(serie.cat.categories)
        return np.where(cod >= 0, remap[np.maximum(cod, 0)], -1)
    return universo.get_indexer(serie)


class TargetEncoderFueraDeFold(BaseEstimator, TransformerMixin):
    """Tasa suavizada de SSCO por categoría (te_ciiu, te_ciiu_depto, te_cohorte_ruc).

    En `fit_transform` cada fila usa los demás folds de lo recibido; en `transform`
    usa todo lo recibido en `fit`. Así, en una ronda de CV el fold de validación no
    entra en las tasas con las que se entrena. Mismo cálculo que
    `codificar_fuera_de_fold` del job 06. Categoría nula o nueva: prevalencia.
    """

    def __init__(self, m: float = TE_M) -> None:
        self.m = m

    def _codigos_clave(self, X: pd.DataFrame) -> dict[str, np.ndarray]:
        comp = {c: X[c] for c in ("ciiu_principal", "departamento", "ruc_prefijo")}
        comp["cohorte"] = np.floor(pd.to_numeric(X["ruc_correlativo"]) / 1e5)
        if not hasattr(self, "universos_"):
            self.universos_ = {
                c: pd.Index(np.asarray(pd.unique(s.dropna()))) for c, s in comp.items()
            }
        out = {}
        for nombre, partes in TE_CLAVES.items():
            cod = np.zeros(len(X), dtype=np.int64)
            valido = np.ones(len(X), dtype=bool)
            for c in partes:
                u = self.universos_[c]
                ci = _codigos(comp[c], u)
                valido &= ci >= 0
                cod = cod * len(u) + np.maximum(ci, 0)
            out[nombre] = np.where(valido, cod, -1)
        return out

    def _ajustar(self, X: pd.DataFrame, y: Any) -> dict[str, np.ndarray]:
        if hasattr(self, "universos_"):
            del self.universos_
        y = np.asarray(y, dtype=np.float64)
        self.prev_ = float(y.mean())
        self.folds_ = np.unique(X["fold"].to_numpy())
        cods = self._codigos_clave(X)
        self.totales_ = {}
        for nombre, cod in cods.items():
            ok = cod >= 0
            tam = int(np.prod([len(self.universos_[c]) for c in TE_CLAVES[nombre]]))
            self.totales_[nombre] = (
                np.bincount(cod[ok], minlength=tam).astype(np.float64),
                np.bincount(cod[ok], weights=y[ok], minlength=tam),
            )
        return cods

    def fit(self, X: pd.DataFrame, y: Any = None) -> TargetEncoderFueraDeFold:
        self._ajustar(X, y)
        return self

    def _tasas(self, cods, n_propio=None, p_propio=None) -> np.ndarray:
        cols = []
        for nombre, cod in cods.items():
            n_tot, p_tot = self.totales_[nombre]
            ok = cod >= 0
            idx = np.maximum(cod, 0)
            n = np.where(ok, n_tot[idx], 0.0)
            p = np.where(ok, p_tot[idx], 0.0)
            if n_propio is not None:
                n = n - np.where(ok, n_propio[nombre], 0.0)
                p = p - np.where(ok, p_propio[nombre], 0.0)
            cols.append((p + self.m * self.prev_) / (n + self.m))
        return np.column_stack(cols)

    def fit_transform(self, X: pd.DataFrame, y: Any = None, **kw: Any) -> np.ndarray:
        cods = self._ajustar(X, y)
        y = np.asarray(y, dtype=np.float64)
        f = np.searchsorted(self.folds_, X["fold"].to_numpy())
        n_f: dict[str, np.ndarray] = {}
        p_f: dict[str, np.ndarray] = {}
        # Total por (categoría, fold) de la fila: se resta de su propio fold.
        for nombre, cod in cods.items():
            ok = cod >= 0
            tam = len(self.totales_[nombre][0]) * len(self.folds_)
            k = cod[ok] * len(self.folds_) + f[ok]
            n_cf = np.bincount(k, minlength=tam).astype(np.float64)
            p_cf = np.bincount(k, weights=y[ok], minlength=tam)
            idx = np.maximum(cod, 0) * len(self.folds_) + f
            n_f[nombre], p_f[nombre] = n_cf[idx], p_cf[idx]
        return self._tasas(cods, n_f, p_f)

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        return self._tasas(self._codigos_clave(X))

    def get_feature_names_out(self, input_features: Any = None) -> np.ndarray:
        return np.asarray(list(TE_CLAVES), dtype=object)


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
    parser.add_argument(
        "--model_name", type=str, choices=["LR", "DT", "RF", "HGB"], required=True
    )
    parser.add_argument("--variant", type=str, choices=["V1", "V2"], default="V2")
    parser.add_argument("--usa_pesos", type=str, choices=["si", "no"], default="si")
    parser.add_argument("--dataset_version", type=str, default="v1")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=3,
        help="Procesos paralelos de GridSearchCV (cada uno copia X_train: cuidar la RAM)",
    )
    parser.add_argument(
        "--neg_ratio",
        type=int,
        default=0,
        help="Negativos por positivo que se muestrean en train (0 = todos)",
    )
    parser.add_argument("--ruta_salida_predicciones", type=str, default="")
    parser.add_argument("--ruta_salida_run_record", type=str, default="")
    parser.add_argument(
        "--param_grid_json",
        type=str,
        default="",
        help="JSON string con grilla de hiperparámetros personalizada para GridSearchCV",
    )

    if args_list is not None:
        return parser.parse_args(args_list)
    return parser.parse_args()


# Columnas de texto con pocos valores distintos: se cargan como categóricas (códigos
# enteros) porque un str de Python por celda ocupa varios GB con ~12 millones de filas.
COLS_TEXTO_CATEGORICAS = [
    "actividad_economica_principal",
    "Estado",
    "Condicion",
    "departamento",
    "ciiu_principal",
    "tipo_contribuyente",
    "ruc_prefijo",
    "tipo_facturacion",
    "tipo_contabilidad",
    "split",
]


def _leer_parquet_compacto(rutas: list[str]) -> pd.DataFrame:
    """Lee los parquet con las columnas de texto repetitivo como categóricas."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    tablas = []
    for ruta in rutas:
        columnas = set(pq.read_schema(ruta).names)
        categoricas = [c for c in COLS_TEXTO_CATEGORICAS if c in columnas]
        tablas.append(pq.read_table(ruta, read_dictionary=categoricas))
    tabla = pa.concat_tables(tablas).unify_dictionaries()
    return tabla.to_pandas(split_blocks=True, self_destruct=True)


def cargar_datos(train_dir: str) -> pd.DataFrame:
    """Carga el dataset de entrada en formato Parquet desde un archivo o directorio."""
    if os.path.isfile(train_dir):
        rutas = [train_dir]
    else:
        rutas = sorted(
            glob.glob(os.path.join(train_dir, "**/*.parquet"), recursive=True)
        )

    if rutas:
        try:
            return _leer_parquet_compacto(rutas)
        except Exception as err:  # noqa: BLE001
            print(f"[WARN] Lectura compacta fallida ({err}); se usa pandas.")
            dfs = [pd.read_parquet(f) for f in rutas]
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
        "nro_trabajadores",
        "nrotrab_disponible",
        "sin_trabajadores",
        "monto_por_trabajador",
        "informalidad_epen_departamento",
        "ruc_correlativo",
        "meses_en_padron",
        "nuevo_en_periodo",
        "meses_hueco",
        "cambio_tipo_facturacion",
        "cambio_estado",
        "see_afiliado",
        "see_antiguedad_dias",
        "see_n_tipos",
        "see_factura_portal",
        "see_boleta_portal",
        "see_sistema_propio",
        "see_solo_honorarios",
    ]
    num_cols = [c for c in candidatos_num if c in df.columns]

    candidatos_cat = [
        "ciiu_principal",
        "actividad_economica_principal",
        "departamento",
        "tipo_contribuyente",
        "ruc_prefijo",
        "tipo_facturacion",
        "tipo_contabilidad",
    ]
    if variant == "V1":
        candidatos_cat.extend(["Estado", "Condicion"])

    cat_cols = [c for c in candidatos_cat if c in df.columns]

    # Una columna 100% nula no aporta nada: se excluye en vez de entrar en silencio.
    vacias = [c for c in num_cols + cat_cols if not df[c].notna().any()]
    for c in vacias:
        print(f"[WARN] Feature {c} llega 100% nula: se excluye del modelo")
    num_cols = [c for c in num_cols if c not in vacias]
    cat_cols = [c for c in cat_cols if c not in vacias]

    return num_cols, cat_cols


def construir_pipeline_preprocesamiento(
    num_cols: list[str],
    cat_cols: list[str],
    es_lr: bool = True,
    denso: bool = False,
    con_te: bool = False,
) -> ColumnTransformer:
    """Construye el ColumnTransformer para preprocesamiento sin fuga de datos."""

    def _num_pipeline(con_log: bool) -> Pipeline:
        steps: list[tuple[str, Any]] = []
        if con_log:
            # Solo la regresión logística es sensible a la escala de la cola extrema.
            if es_lr:
                steps.append(("winsor", WinsorizerSuperior()))
            steps.append(("log1p", Log1pTransformer()))
        steps.append(("imputer", SimpleImputer(strategy="median", add_indicator=True)))
        if es_lr:
            steps.append(("scaler", StandardScaler()))
        return Pipeline(steps)

    def _cat_pipeline(max_categories: int) -> Pipeline:
        return Pipeline(
            [
                (
                    "imputer",
                    SimpleImputer(strategy="constant", fill_value="DESCONOCIDO"),
                ),
                (
                    "ohe",
                    OneHotEncoder(
                        handle_unknown="infrequent_if_exist",
                        max_categories=max_categories,
                        sparse_output=True,
                        dtype=np.float32,
                    ),
                ),
            ]
        )

    transformers: list[tuple[str, Any, list[str]]] = []
    num_log = [c for c in num_cols if c in COLS_LOG1P]
    num_lin = [c for c in num_cols if c not in COLS_LOG1P]
    if num_log:
        transformers.append(("num_log", _num_pipeline(True), num_log))
    if num_lin:
        transformers.append(("num_lin", _num_pipeline(False), num_lin))
    for col in cat_cols:
        tope = MAX_CATEGORIAS.get(col, MAX_CATEGORIAS_DEFAULT)
        transformers.append((f"cat_{col}", _cat_pipeline(tope), [col]))

    if con_te:
        pasos: list[tuple[str, Any]] = [("te", TargetEncoderFueraDeFold())]
        if es_lr:
            pasos.append(("scaler", StandardScaler()))
        transformers.append(("te", Pipeline(pasos), TE_ENTRADAS + ["fold"]))

    # HistGradientBoosting no acepta matrices dispersas.
    return ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        **({"sparse_threshold": 0.0} if denso else {}),
    )


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
    # Las tasas te_* se calculan dentro del Pipeline con las columnas crudas y el fold.
    con_te = all(c in df.columns for c in TE_ENTRADAS + ["fold"])
    extra = [c for c in TE_ENTRADAS + ["fold"] if con_te and c not in num_cols + cat_cols]
    features = num_cols + cat_cols + extra

    # Sin copias intermedias del DataFrame: con ~10 millones de filas cada una pesa GB.
    es_train = (df["split"] == "train").to_numpy()
    X_train = df.loc[es_train, features]
    y_train: np.ndarray = np.asarray(df["label"].to_numpy()[es_train], dtype=int)
    folds_train: np.ndarray = np.asarray(df["fold"].to_numpy()[es_train])

    n_train = int(es_train.sum())
    n_pos_train = int(y_train.sum())

    print(f"[METRIC] n_train={n_train}")
    print(f"[METRIC] n_pos_train={n_pos_train}")

    # 2. Configurar Modelo y Pipeline
    es_lr = args.model_name == "LR"
    es_hgb = args.model_name == "HGB"
    preprocessor = construir_pipeline_preprocesamiento(
        num_cols, cat_cols, es_lr=es_lr, denso=es_hgb, con_te=con_te
    )
    class_weight = "balanced" if args.usa_pesos == "si" else None

    if es_lr:
        model = LogisticRegression(
            class_weight=class_weight, random_state=args.seed, max_iter=1000
        )
        param_grid = {"model__C": [0.01, 0.1, 1.0, 10.0]}
    elif es_hgb:
        model = HistGradientBoostingClassifier(
            class_weight=class_weight,
            random_state=args.seed,
            early_stopping=False,
        )
        param_grid = {
            "model__learning_rate": [0.05, 0.1],
            "model__max_iter": [200],
            "model__max_leaf_nodes": [15, 31],
        }
    elif args.model_name == "RF":
        # Solo hiperparámetros con equivalente en MLlib (numTrees, maxDepth,
        # minInstancesPerNode), para migrar sin cambiar la grilla.
        model = RandomForestClassifier(
            class_weight="balanced_subsample" if class_weight else None,
            max_features="sqrt",
            random_state=args.seed,
        )
        param_grid = {
            "model__n_estimators": [300],
            "model__max_depth": [8, 16],
            "model__min_samples_leaf": [50, 200] if n_train > 1000 else [2, 5],
        }
    else:
        model = DecisionTreeClassifier(
            class_weight=class_weight, random_state=args.seed
        )
        param_grid = {
            "model__max_depth": [3, 5, 6],
            "model__min_samples_leaf": [50, 200] if n_train > 1000 else [2, 5],
        }

    if args.param_grid_json:
        try:
            custom_grid = json.loads(args.param_grid_json)
            if isinstance(custom_grid, str):  # SageMaker puede codificar dos veces
                custom_grid = json.loads(custom_grid)
            if isinstance(custom_grid, dict) and custom_grid:
                param_grid = custom_grid
                print(
                    f"[INFO] Grilla de hiperparámetros personalizada cargada: {param_grid}"
                )
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            print(
                f"[WARN] Error parseando param_grid_json: {e}. Se mantiene grilla por defecto."
            )

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

    rng = np.random.default_rng(args.seed)

    def _muestra(idx: np.ndarray) -> np.ndarray:
        """Todos los positivos de idx y neg_ratio negativos por cada uno."""
        pos = idx[y_train[idx] == 1]
        neg = idx[y_train[idx] == 0]
        n_neg = min(len(neg), args.neg_ratio * len(pos))
        return np.sort(np.concatenate([pos, rng.choice(neg, n_neg, replace=False)]))

    cv_splits: Any = cv_splitter
    idx_ajuste = None
    if args.neg_ratio > 0:
        # Solo se submuestrea lo que se ajusta: cada fold se valida completo, así el
        # PR-AUC de CV mantiene la prevalencia real y es comparable con test.
        cv_splits = [
            (_muestra(tr), va) for tr, va in cv_splitter.split(X_train, y_train)
        ]
        idx_ajuste = _muestra(np.arange(n_train))
        print(f"[METRIC] n_train_muestra={len(idx_ajuste)}")

    grid_search = GridSearchCV(
        estimator=full_pipeline,
        param_grid=param_grid,
        scoring="average_precision",
        cv=cv_splits,
        refit=False,  # el modelo final se ajusta abajo, sobre la misma muestra que la CV
        n_jobs=args.n_jobs,
        verbose=2,
    )

    # 4. Ajuste del modelo sobre Train únicamente
    grid_search.fit(X_train, y_train)
    best_pipeline = clone(full_pipeline).set_params(**grid_search.best_params_)
    if idx_ajuste is None:
        best_pipeline.fit(X_train, y_train)
    else:
        best_pipeline.fit(X_train.iloc[idx_ajuste], y_train[idx_ajuste])

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
        print(f"[METRIC] cv_pr_auc_fold={f_score:.4f}")

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
        tabla_explicabilidad = df_coefs
    elif args.model_name == "RF":
        # Un bosque no tiene reglas legibles: solo importancias promedio de sus árboles.
        tabla_explicabilidad = extraer_importancias_dt(best_pipeline, feature_names)
        print("[INFO] Top 5 Features por importancia (RF):")
        print(tabla_explicabilidad.head(5).to_string())
    elif es_hgb:
        # Sin coeficientes ni importancias nativas: permutación sobre la muestra de ajuste.
        X_exp = X_train if idx_ajuste is None else X_train.iloc[idx_ajuste]
        y_exp = y_train if idx_ajuste is None else y_train[idx_ajuste]
        tabla_explicabilidad = extraer_importancias_permutacion(
            best_pipeline, X_exp, y_exp, seed=args.seed
        )
        print("[INFO] Top 5 Features por importancia de permutación (HGB):")
        print(tabla_explicabilidad.head(5).to_string())
    else:
        rules_text = extraer_reglas_dt(best_pipeline, feature_names, max_depth=3)
        print("[INFO] Reglas principales de Decision Tree (profundidad <= 3):")
        print(rules_text[:500] + "...")
        tabla_explicabilidad = extraer_importancias_dt(best_pipeline, feature_names)
    # Tabla completa (el log solo muestra 5 filas): el orquestador la publica en S3.
    tabla_explicabilidad.to_csv(
        os.path.join(args.model_dir, "explicabilidad.csv"), index=False
    )

    print("[INFO] Entrenamiento y exportacion finalizada exitosamente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
