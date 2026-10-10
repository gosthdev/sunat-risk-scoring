"""
Módulo de seguimiento de experimentos y consolidación de resultados (Tarea B5).

Funcionalidades:
- Validación rigurosa de esquema contra el Contrato C3 (run_record.json).
- Consolidación de metadatos de entrenamiento, hiperparámetros y CV PR-AUC.
- Cruce con métricas de test (Contrato C4) mediante compute_metrics si hay predicciones disponibles.
- Detección de corridas faltantes de la matriz oficial (E1 a E6).
- Formateo de tabla comparativa en formato Markdown.
- Registro opcional en MLflow para visualización y capturas.
"""

import glob
import json
import os
from typing import Any

import pandas as pd

from .metrics import compute_metrics

CAMPOS_OBLIGATORIOS_C3 = [
    "run_id",
    "model_name",
    "variant",
    "dataset_version",
    "metricas_cv",
]

CORRIDAS_OFICIALES_ESPERADAS = ["E1", "E2", "E3", "E4", "E5", "E6"]


def validar_run_record(record: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Valida que un diccionario cumpla con el esquema mínimo del Contrato C3.
    Retorna (es_valido, lista_errores).
    """
    errores = []
    if not isinstance(record, dict):
        return False, ["El registro no es un objeto JSON válido."]

    for campo in CAMPOS_OBLIGATORIOS_C3:
        if campo not in record:
            errores.append(f"Falta el campo obligatorio: '{campo}'")

    if "metricas_cv" in record and isinstance(record["metricas_cv"], dict):
        if "cv_pr_auc_mean" not in record["metricas_cv"]:
            errores.append("Falta 'cv_pr_auc_mean' dentro de 'metricas_cv'.")
    elif "metricas_cv" in record:
        errores.append("'metricas_cv' debe ser un diccionario.")

    return len(errores) == 0, errores


def cargar_run_records(
    ruta_o_patron: str | list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Carga todos los run_records desde una ruta de directorio, lista de rutas o patrón glob.
    Retorna (records_validos, registros_con_error).
    """
    archivos = []
    if isinstance(ruta_o_patron, list):
        archivos = ruta_o_patron
    elif os.path.isdir(ruta_o_patron):
        archivos = sorted(
            glob.glob(
                os.path.join(ruta_o_patron, "**/run_record*.json"), recursive=True
            )
        )
    elif os.path.isfile(ruta_o_patron):
        archivos = [ruta_o_patron]
    else:
        archivos = sorted(glob.glob(ruta_o_patron, recursive=True))

    records_validos = []
    registros_error = []

    for archivo in archivos:
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                line = f.readline().strip()
                if not line:
                    registros_error.append(
                        {"archivo": archivo, "error": "Archivo vacío"}
                    )
                    continue
                data = json.loads(line)
        except Exception as e:  # noqa: BLE001
            registros_error.append(
                {"archivo": archivo, "error": f"Error al leer JSON: {e!s}"}
            )
            continue

        es_valido, lista_errores = validar_run_record(data)
        if es_valido:
            data["_origen_archivo"] = archivo
            records_validos.append(data)
        else:
            registros_error.append({"archivo": archivo, "errores": lista_errores})

    return records_validos, registros_error


def verificar_completitud_matriz(
    records: list[dict[str, Any]],
    corridas_esperadas: list[str] | None = None,
) -> dict[str, Any]:
    """
    Verifica si todas las corridas oficiales (E1 a E6) están presentes.
    """
    if corridas_esperadas is None:
        corridas_esperadas = CORRIDAS_OFICIALES_ESPERADAS

    encontradas = set()
    for r in records:
        run_id = str(r.get("run_id", "")).strip()
        for esperada in corridas_esperadas:
            if esperada in run_id.upper():
                encontradas.add(esperada)
                break

    faltantes = [c for c in corridas_esperadas if c not in encontradas]
    return {
        "completas": len(faltantes) == 0,
        "encontradas": sorted(encontradas),
        "faltantes": faltantes,
        "total_encontradas": len(encontradas),
        "total_esperadas": len(corridas_esperadas),
    }


def consolidar_experimentos(
    records: list[dict[str, Any]],
    predicciones_por_run: dict[str, pd.DataFrame] | None = None,
    n_bootstrap: int = 500,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Consolida una lista de run_records (C3) en un DataFrame estructurado.
    Si se suministra un diccionario con DataFrames de predicciones (C2) por run_id,
    calcula y adjunta métricas oficiales de test (C4) con compute_metrics.
    """
    filas = []
    for r in records:
        run_id = r.get("run_id", "DESCONOCIDO")
        model_name = r.get("model_name", "N/A")
        variant = r.get("variant", "N/A")
        usa_pesos = r.get("usa_pesos", True)
        dataset_version = r.get("dataset_version", "v1")
        metricas_cv = r.get("metricas_cv", {})
        cv_pr_auc = metricas_cv.get("cv_pr_auc_mean", None)
        train_pr_auc = metricas_cv.get("train_pr_auc", None)
        segundos = r.get("segundos_entrenamiento", None)

        fila: dict[str, Any] = {
            "run_id": run_id,
            "modelo": model_name,
            "variante": variant,
            "pesos_clase": "Sí" if usa_pesos else "No",
            "dataset": dataset_version,
            "cv_pr_auc": cv_pr_auc,
            "train_pr_auc": train_pr_auc,
            "segundos": segundos,
            "test_pr_auc": None,
            "test_ic_95": None,
            "test_recall_1pct": None,
            "test_recall_5pct": None,
            "test_lift_1pct": None,
            "test_brier": None,
        }

        # Si hay predicciones para este run_id, calcular C4
        if predicciones_por_run and run_id in predicciones_por_run:
            df_pred = predicciones_por_run[run_id]
            try:
                # Filtrar conjunto de test para evaluar según Contrato C4
                if "split" in df_pred.columns:
                    df_test = df_pred[
                        df_pred["split"].astype(str).str.lower() == "test"
                    ].copy()
                else:
                    df_test = df_pred

                n_total = len(df_test)
                if n_total > 0:
                    ks_validos = [k for k in [100, 500] if k <= n_total]
                    m = compute_metrics(
                        df_test,
                        ks=ks_validos if ks_validos else None,
                        n_bootstrap=n_bootstrap,
                        seed=seed,
                    )
                    fila["test_pr_auc"] = m["pr_auc"]["valor"]
                    fila["test_ic_95"] = (
                        f"[{m['pr_auc']['ic_bajo']:.3f}, {m['pr_auc']['ic_alto']:.3f}]"
                    )
                    fila["test_recall_1pct"] = m["recall_at_k"].get("1pct")
                    fila["test_recall_5pct"] = m["recall_at_k"].get("5pct")
                    fila["test_lift_1pct"] = m["lift_at_k"].get("1pct")
                    fila["test_brier"] = m.get("brier")
            except Exception as e:  # noqa: BLE001
                fila["error_test"] = str(e)

        filas.append(fila)

    df_resumen = pd.DataFrame(filas)
    if not df_resumen.empty and "cv_pr_auc" in df_resumen.columns:
        # Ordenar por CV PR-AUC descendente
        df_resumen = df_resumen.sort_values(
            by="cv_pr_auc", ascending=False
        ).reset_index(drop=True)

    return df_resumen


def formatear_tabla_markdown(df_experimentos: pd.DataFrame) -> str:
    """
    Convierte el DataFrame consolidado de experimentos en una tabla Markdown formal.
    """
    if df_experimentos.empty:
        return "_No se registraron experimentos._\n"

    columnas_ordenadas = [
        ("run_id", "Run ID"),
        ("modelo", "Modelo"),
        ("variante", "Variante"),
        ("pesos_clase", "Pesos"),
        ("dataset", "Dataset"),
        ("cv_pr_auc", "PR-AUC CV"),
        ("test_pr_auc", "PR-AUC Test"),
        ("test_ic_95", "IC 95% Test"),
        ("test_recall_1pct", "Recall@1%"),
        ("test_recall_5pct", "Recall@5%"),
        ("test_lift_1pct", "Lift@1%"),
        ("test_brier", "Brier"),
    ]

    # Filtrar solo columnas presentes
    cols_existentes = [
        (orig, titulo)
        for orig, titulo in columnas_ordenadas
        if orig in df_experimentos.columns
    ]

    lineas = []
    cabecera = "| " + " | ".join([titulo for _, titulo in cols_existentes]) + " |"
    separador = (
        "| "
        + " | ".join(
            [":---:" if orig != "run_id" else ":---" for orig, _ in cols_existentes]
        )
        + " |"
    )
    lineas.append(cabecera)
    lineas.append(separador)

    for _, row in df_experimentos.iterrows():
        valores = []
        for orig, _ in cols_existentes:
            val = row.get(orig)
            if val is None or pd.isna(val):
                valores.append("-")
            elif isinstance(val, float):
                if "lift" in orig:
                    valores.append(f"{val:.1f}x")
                elif "recall" in orig or "auc" in orig or "brier" in orig:
                    valores.append(f"{val:.4f}")
                else:
                    valores.append(f"{val:.2f}")
            else:
                valores.append(str(val))
        lineas.append("| " + " | ".join(valores) + " |")

    return "\n".join(lineas) + "\n"


def registrar_mlflow(
    records: list[dict[str, Any]],
    df_experimentos: pd.DataFrame | None = None,
    experiment_name: str = "sunat-risk-ssco",
    tracking_uri: str | None = None,
) -> bool:
    """
    Registra los run_records y métricas oficiales de test en MLflow local.
    """
    try:
        import mlflow
    except ImportError:
        print("[AVISO] MLflow no está instalado. Omitiendo registro en MLflow.")
        return False

    try:
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        # Mapear métricas de test por run_id si df_experimentos está presente
        test_metrics_by_run = {}
        if df_experimentos is not None and not df_experimentos.empty:
            for _, row in df_experimentos.iterrows():
                test_metrics_by_run[row.get("run_id")] = row

        for r in records:
            run_id = r.get("run_id", "unnamed_run")
            with mlflow.start_run(run_name=run_id):
                # Tags
                mlflow.set_tag("model_name", r.get("model_name", "N/A"))
                mlflow.set_tag("variant", r.get("variant", "N/A"))
                mlflow.set_tag("dataset_version", r.get("dataset_version", "v1"))

                # Hiperparámetros
                params = r.get("hiperparametros_ganadores", {})
                params["usa_pesos"] = str(r.get("usa_pesos", True))
                for p_key, p_val in params.items():
                    mlflow.log_param(p_key, p_val)

                # Métricas CV
                cv_metrics = r.get("metricas_cv", {})
                for m_key, m_val in cv_metrics.items():
                    if isinstance(m_val, (int, float)):
                        mlflow.log_metric(m_key, float(m_val))

                if r.get("segundos_entrenamiento"):
                    mlflow.log_metric(
                        "segundos_entrenamiento", float(r["segundos_entrenamiento"])
                    )

                # Métricas Oficiales de Test (C4)
                if run_id in test_metrics_by_run:
                    t_row = test_metrics_by_run[run_id]
                    metricas_test_a_loguear = [
                        ("test_pr_auc", t_row.get("test_pr_auc")),
                        ("test_recall_1pct", t_row.get("test_recall_1pct")),
                        ("test_recall_5pct", t_row.get("test_recall_5pct")),
                        ("test_lift_1pct", t_row.get("test_lift_1pct")),
                        ("test_brier", t_row.get("test_brier")),
                    ]
                    for m_name, m_val in metricas_test_a_loguear:
                        if m_val is not None and not pd.isna(m_val):
                            mlflow.log_metric(m_name, float(m_val))

                    if t_row.get("test_ic_95"):
                        mlflow.set_tag("test_ic_95", str(t_row.get("test_ic_95")))

        print(
            f"[INFO] {len(records)} corridas registradas exitosamente en MLflow ({experiment_name})."
        )
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[ERROR] Error al registrar en MLflow: {e!s}")
        return False
