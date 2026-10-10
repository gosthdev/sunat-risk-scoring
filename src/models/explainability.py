"""Módulo de explicabilidad analítica para modelos de scoring SSCO (Tarea A6).

Provee extracción e interpretación de:
- Coeficientes de Regresión Logística (impacto positivo/negativo en escala comparable).
- Importancias y reglas de decisión para Árbol de Decisión.
"""

from typing import Any

import pandas as pd
from sklearn.tree import export_text


def obtener_nombres_features(
    preprocessor: Any, num_cols: list[str], cat_cols: list[str]
) -> list[str]:
    """Extrae los nombres finales de las variables transformadas tras el ColumnTransformer."""
    try:
        names = preprocessor.get_feature_names_out()
        return [str(n) for n in names]
    except Exception:  # noqa: BLE001
        # Fallback defensivo si get_feature_names_out() no está disponible
        feature_names = [f"num__{c}" for c in num_cols]
        try:
            ohe = preprocessor.named_transformers_["cat"].named_steps["ohe"]
            cat_feature_names = ohe.get_feature_names_out(cat_cols)
            feature_names.extend([f"cat__{c}" for c in cat_feature_names])
        except Exception:  # noqa: BLE001
            feature_names.extend([f"cat__{c}" for c in cat_cols])
        return feature_names


def extraer_explicabilidad_lr(
    best_pipeline: Any, feature_names: list[str]
) -> pd.DataFrame:
    """Extrae coeficientes de Regresión Logística ordenados por su magnitud absoluta."""
    model = best_pipeline.named_steps["model"]
    if not hasattr(model, "coef_"):
        raise ValueError("El modelo proporcionado no contiene el atributo coef_.")

    coefs = model.coef_[0]
    df_coefs = pd.DataFrame(
        {
            "feature": feature_names[: len(coefs)],
            "coeficiente": coefs,
        }
    )
    df_coefs["impacto"] = df_coefs["coeficiente"].apply(
        lambda x: "Aumenta Riesgo SSCO" if x > 0 else "Disminuye Riesgo SSCO"
    )
    df_coefs["abs_coef"] = df_coefs["coeficiente"].abs()
    df_coefs = df_coefs.sort_values(by="abs_coef", ascending=False).reset_index(
        drop=True
    )
    return df_coefs


def extraer_reglas_dt(
    best_pipeline: Any, feature_names: list[str], max_depth: int = 5
) -> str:
    """Extrae las reglas legibles por humanos del Árbol de Decisión."""
    model = best_pipeline.named_steps["model"]
    if not hasattr(model, "tree_"):
        raise ValueError("El modelo proporcionado no es un árbol de decisión.")

    names = feature_names[: model.n_features_in_]
    rules = export_text(model, feature_names=list(names), max_depth=max_depth)
    return rules


def extraer_importancias_dt(
    best_pipeline: Any, feature_names: list[str]
) -> pd.DataFrame:
    """Extrae las importancias relativas de variables (Gini importance) del Árbol de Decisión."""
    model = best_pipeline.named_steps["model"]
    if not hasattr(model, "feature_importances_"):
        raise ValueError("El modelo proporcionado no contiene feature_importances_.")

    importancias = model.feature_importances_
    df_imp = pd.DataFrame(
        {
            "feature": feature_names[: len(importancias)],
            "importancia": importancias,
        }
    )
    df_imp = df_imp.sort_values(by="importancia", ascending=False).reset_index(
        drop=True
    )
    return df_imp
