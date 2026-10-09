"""
EMR Step 7 — Preprocesamiento, Entrenamiento, CV, Predicciones, Run Record y Explicabilidad.

Implementa:
  - Carga de model_inputs (Contrato C1).
  - Preprocesamiento integrado con Pipeline MLlib:
      * Banderas indicadoras de nulos con significado.
      * Imputación de nulos numéricos con mediana (aprendida solo en train).
      * Indexación y codificación categórica (OneHot para LR, StringIndexer para DT).
      * Ensamblado de vector de features sin doble estandarización para LR.
  - Ponderación de desbalance mediante pesos de clase (USA_PESOS=true).
  - Validación cruzada de 5 folds usando la columna estratificada 'fold' (foldCol).
  - Optimización sobre métrica PR-AUC (areaUnderPR) sin pesos en el evaluador.
  - Inferencia sobre toda la población y cálculo de rank_global (Contrato C2).
  - Registro de auditoría y metadatos de experimento run_record.json (Contrato C3).
  - Explicabilidad del modelo oficial (coeficientes escalados LR / reglas DT).

Uso: spark-submit --py-files common.zip 07_train_model.py
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (
    Imputer,
    ImputerModel,
    OneHotEncoder,
    SQLTransformer,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.stat import Summarizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import (
    col,
    current_date,
    lit,
    when,
)
from pyspark.sql.functions import (
    rank as spark_rank,
)
from pyspark.sql.window import Window
from s3_paths import (
    GOLD_MODEL_ARTIFACTS,
    GOLD_MODEL_INPUTS,
    GOLD_MODEL_OUTPUTS,
)
from spark_session_factory import create_spark_session

FEATURES_COMUNES = [
    "nro_trabajadores",
    "sin_trabajadores",
    "tipo_contribuyente",
    "departamento",
    "ciiu_principal",
    "n_actividades",
    "n_ordenes",
    "monto_total_soles",
    "monto_mediano_soles",
    "monto_maximo_soles",
    "n_entidades_distintas",
    "pct_monto_en_entidad_principal",
    "pct_ordenes_anuladas",
    "monto_por_trabajador",
    "contrata_con_estado",
    "antiguedad_contratacion_estado_dias",
    "informalidad_epen_departamento",
]

FEATURES_V1_EXTRA = ["Estado", "Condicion"]

CATEGORICAS_CONOCIDAS = {
    "tipo_contribuyente",
    "departamento",
    "ciiu_principal",
    "Estado",
    "Condicion",
}

NUM_COLS_CON_NULOS_DEFAULT = [
    "nro_trabajadores",
    "n_ordenes",
    "monto_total_soles",
    "monto_mediano_soles",
    "monto_maximo_soles",
    "n_entidades_distintas",
    "pct_monto_en_entidad_principal",
    "antiguedad_contratacion_estado_dias",
    "monto_por_trabajador",
    "informalidad_epen_departamento",
]

COLS_MONETARIAS_LOG1P = [
    "monto_total_soles",
    "monto_mediano_soles",
    "monto_maximo_soles",
    "monto_por_trabajador",
]


def construir_pipeline(
    df,
    model_name="LR",
    variant="V2",
    weight_col="class_weight",
    semilla=42,
    max_bins=64,
):
    """Construye el Pipeline de PySpark MLlib para preprocesamiento y modelo."""
    feature_candidates = FEATURES_COMUNES + (
        FEATURES_V1_EXTRA if variant == "V1" else []
    )
    feature_cols = [c for c in feature_candidates if c in df.columns]

    cat_cols = [c for c in feature_cols if c in CATEGORICAS_CONOCIDAS]
    num_cols = [c for c in feature_cols if c not in CATEGORICAS_CONOCIDAS]

    num_cols_con_nulos = [c for c in num_cols if c in NUM_COLS_CON_NULOS_DEFAULT]

    stages = []

    # 1. Banderas de nulo para numéricas con valor ausente interpretable
    for c in num_cols_con_nulos:
        sql_expr = f"SELECT *, CASE WHEN {c} IS NULL THEN 1 ELSE 0 END AS {c}_es_nulo FROM __THIS__"
        stages.append(SQLTransformer(statement=sql_expr))

    # 2. Imputación con mediana (aprende solo de train)
    if num_cols:
        imputer = Imputer(
            inputCols=num_cols,
            outputCols=[f"{c}_imp" for c in num_cols],
            strategy="median",
        )
        stages.append(imputer)

    # 2.5. Transformación log1p para montos monetarios de colas pesadas (Regla D10)
    cols_monetarias = [c for c in num_cols if c in COLS_MONETARIAS_LOG1P]
    if cols_monetarias:
        log_exprs = [
            f"ln(1.0 + CASE WHEN {c}_imp < 0.0 THEN 0.0 ELSE {c}_imp END) AS {c}_log"
            for c in cols_monetarias
        ]
        sql_log_expr = f"SELECT *, {', '.join(log_exprs)} FROM __THIS__"
        stages.append(SQLTransformer(statement=sql_log_expr))

    num_assembler_cols = [
        f"{c}_log" if c in cols_monetarias else f"{c}_imp" for c in num_cols
    ]

    # 3. Tratamiento de variables categóricas
    indexed_cat_cols = []
    for c in cat_cols:
        idx_col = f"{c}_idx"
        indexer = StringIndexer(
            inputCol=c,
            outputCol=idx_col,
            handleInvalid="keep",
        )
        stages.append(indexer)
        indexed_cat_cols.append(idx_col)

    if model_name == "LR":
        encoded_cat_cols = []
        if cat_cols:
            encoder = OneHotEncoder(
                inputCols=indexed_cat_cols,
                outputCols=[f"{c}_ohe" for c in cat_cols],
                dropLast=True,
            )
            stages.append(encoder)
            encoded_cat_cols = [f"{c}_ohe" for c in cat_cols]

        assembler_inputs = (
            num_assembler_cols
            + [f"{c}_es_nulo" for c in num_cols_con_nulos]
            + encoded_cat_cols
        )
    else:  # DT: Solo StringIndexer
        assembler_inputs = (
            num_assembler_cols
            + [f"{c}_es_nulo" for c in num_cols_con_nulos]
            + indexed_cat_cols
        )

    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="features",
        handleInvalid="keep",
    )
    stages.append(assembler)

    # 4. Clasificador (construcción dinámica de kwargs para evitar weightCol=None en Py4J)
    if model_name == "LR":
        lr_kwargs = {
            "labelCol": "label",
            "featuresCol": "features",
            "maxIter": 100,
            "probabilityCol": "probability",
        }
        if weight_col:
            lr_kwargs["weightCol"] = weight_col
        estimator = LogisticRegression(**lr_kwargs)

    elif model_name == "DT":
        dt_kwargs = {
            "labelCol": "label",
            "featuresCol": "features",
            "seed": semilla,
            "maxBins": max_bins,
            "probabilityCol": "probability",
        }
        if weight_col:
            dt_kwargs["weightCol"] = weight_col
        estimator = DecisionTreeClassifier(**dt_kwargs)
    else:
        raise ValueError(f"Modelo desconocido: {model_name}")

    stages.append(estimator)
    pipeline = Pipeline(stages=stages)
    return pipeline, estimator, assembler_inputs


def entrenar_modelo_cv(
    train_df,
    pipeline,
    estimator,
    model_name="LR",
    semilla=42,
    num_folds=5,
    fast_dev_run=False,
    parallelism=2,
):
    """Ejecuta CrossValidator con foldCol estratificado y optimización en PR-AUC."""
    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )

    grid_builder = ParamGridBuilder()
    if model_name == "LR":
        if fast_dev_run:
            grid_builder.addGrid(estimator.regParam, [0.01]).addGrid(
                estimator.elasticNetParam, [0.0]
            )
        else:
            grid_builder.addGrid(estimator.regParam, [0.001, 0.01, 0.1]).addGrid(
                estimator.elasticNetParam, [0.0, 0.5]
            )
    elif model_name == "DT":
        if fast_dev_run:
            grid_builder.addGrid(estimator.maxDepth, [3]).addGrid(
                estimator.minInstancesPerNode, [50]
            )
        else:
            grid_builder.addGrid(estimator.maxDepth, [3, 5, 6]).addGrid(
                estimator.minInstancesPerNode, [50, 200]
            )

    param_grid = grid_builder.build()

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=num_folds,
        foldCol="fold",
        seed=semilla,
        parallelism=parallelism,
    )

    cv_model = cv.fit(train_df)
    return cv_model


def generar_predicciones_c2(
    best_model,
    all_data_df,
    model_name,
    variant,
    dataset_version,
):
    """Aplica el PipelineModel sobre la población completa y produce Contrato C2."""
    raw_preds = best_model.transform(all_data_df)
    preds = raw_preds.withColumn("score", vector_to_array(col("probability"))[1])

    w = Window.orderBy(col("score").desc())
    preds = preds.withColumn("rank_global", spark_rank().over(w))

    dept_col = (
        col("departamento") if "departamento" in preds.columns else lit("DESCONOCIDO")
    )

    c2_df = preds.select(
        col("ruc").cast("string"),
        col("score").cast("double"),
        col("rank_global").cast("int"),
        col("label").cast("int"),
        col("split").cast("string"),
        dept_col.alias("departamento"),
        lit(model_name).alias("model_name"),
        lit(variant).alias("variant"),
        lit(dataset_version).alias("dataset_version"),
        current_date().alias("scored_at"),
    )
    return c2_df


def extraer_explicabilidad(
    best_model,
    train_transformed_df,
    assembler_inputs,
    model_name="LR",
):
    """Extrae coeficientes estandarizados para LR o importancias y reglas para DT."""
    last_stage = best_model.stages[-1]
    explicabilidad = {"model_name": model_name}

    # Obtener nombres de atributos expandidos si existen en metadata
    feature_names = assembler_inputs
    try:
        meta = train_transformed_df.schema["features"].metadata
        if "ml_attr" in meta and "attrs" in meta["ml_attr"]:
            ordered = []
            for group in meta["ml_attr"]["attrs"].values():
                for item in group:
                    ordered.append((item["idx"], item["name"]))
            ordered.sort(key=lambda x: x[0])
            feature_names = [x[1] for x in ordered]
    except Exception:  # noqa: BLE001, S110
        pass

    if model_name == "LR":
        coefs = last_stage.coefficients.toArray().tolist()
        n_features = len(coefs)
        if len(feature_names) != n_features:
            feature_names = [
                feature_names[i] if i < len(feature_names) else f"feature_{i}"
                for i in range(n_features)
            ]

        # Calcular desviaciones estándar de train para ponderar escala
        try:
            summary = train_transformed_df.select(
                Summarizer.std(col("features")).alias("stds")
            ).first()
            stds = (
                summary["stds"].toArray().tolist()
                if summary and summary["stds"]
                else [1.0] * n_features
            )
        except Exception:  # noqa: BLE001
            stds = [1.0] * n_features

        importancias = []
        for name, coef, std in zip(feature_names, coefs, stds):
            importancias.append(
                {
                    "feature": str(name),
                    "coef_raw": float(coef),
                    "std_train": float(std),
                    "coef_scaled": float(coef * std),
                }
            )

        top_riesgo = sorted(importancias, key=lambda x: x["coef_scaled"], reverse=True)[
            :10
        ]
        top_proteccion = sorted(importancias, key=lambda x: x["coef_scaled"])[:10]

        explicabilidad.update(
            {
                "intercepto": float(last_stage.intercept),
                "top_factores_riesgo": top_riesgo,
                "top_factores_proteccion": top_proteccion,
                "todas_las_importancias": importancias,
            }
        )
    elif model_name == "DT":
        importances = last_stage.featureImportances.toArray().tolist()
        n_features = len(importances)
        if len(feature_names) != n_features:
            feature_names = [
                feature_names[i] if i < len(feature_names) else f"feature_{i}"
                for i in range(n_features)
            ]
        imp_list = [
            {"feature": str(name), "importancia": float(imp)}
            for name, imp in zip(feature_names, importances)
        ]
        imp_list.sort(key=lambda x: x["importancia"], reverse=True)

        explicabilidad.update(
            {
                "reglas_arbol_debug": last_stage.toDebugString,
                "importancia_features": imp_list,
                "top_features": imp_list[:10],
            }
        )

    return explicabilidad


def generar_narrativa_negocio(explicabilidad):
    """Genera narrativa en lenguaje tributario para explicar las decisiones del modelo."""
    lineas = [
        "# Informe de Explicabilidad y Criterio de Riesgo Tributario (SSCO)",
        "",
        f"**Modelo Evaluado:** {explicabilidad.get('model_name')}",
        "",
        "### Hallazgos Principales de Riesgo:",
    ]

    if explicabilidad.get("model_name") == "LR":
        top = explicabilidad.get("top_factores_riesgo", [])
        for item in top[:5]:
            feat = item["feature"]
            val = item["coef_scaled"]
            lineas.append(
                f"- **{feat}** (Impacto relativo: {val:+.4f}): "
                "Incrementa significativamente la probabilidad de clasificación como sujeto sin capacidad operativa. "
                "Coherente con entidades que registran facturación pública desproporcionada respecto a su estructura formal."
            )
    elif explicabilidad.get("model_name") == "DT":
        top = explicabilidad.get("top_features", [])
        for item in top[:5]:
            feat = item["feature"]
            imp = item["importancia"]
            lineas.append(
                f"- **{feat}** (Importancia relativa Gini: {imp:.4f}): "
                "Nodo crítico de bifurcación decisional en la partición del árbol de riesgo."
            )

    return "\n".join(lineas)


def guardar_artefactos_entrenamiento(
    run_id,
    run_record,
    preprocessing_stats,
    explicabilidad,
    narrativa,
    model_name="LR",
):
    """Guarda run_record, preprocessing_stats y explicabilidad tanto en S3 como en disco local."""
    # 1. Fallback / almacenamiento local siempre disponible
    local_art = f"/tmp/sunat_model_artifacts/{run_id}"
    os.makedirs(f"{local_art}/explicabilidad", exist_ok=True)
    with open(f"{local_art}/run_record.json", "w") as f:
        json.dump(run_record, f, indent=2)
    with open(f"{local_art}/preprocessing_stats.json", "w") as f:
        json.dump(preprocessing_stats, f, indent=2)
    with open(f"{local_art}/explicabilidad/narrativa_negocio.md", "w") as f:
        f.write(narrativa)

    if model_name == "LR":
        with open(f"{local_art}/explicabilidad/importancias_lr.json", "w") as f:
            json.dump(explicabilidad, f, indent=2)
    elif model_name == "DT":
        with open(f"{local_art}/explicabilidad/reglas_dt.txt", "w") as f:
            f.write(explicabilidad.get("reglas_arbol_debug", ""))
        with open(f"{local_art}/explicabilidad/importancias_dt.json", "w") as f:
            json.dump(explicabilidad.get("importancia_features", []), f, indent=2)

    # 2. Subida a AWS S3 si el cliente boto3 está autenticado
    try:
        import boto3  # type: ignore[import-not-found,import-untyped]
        from s3_paths import GOLD_BUCKET

        s3 = boto3.client("s3")
        prefix = GOLD_MODEL_ARTIFACTS.replace(f"s3://{GOLD_BUCKET}/", "").strip("/")

        s3.put_object(
            Bucket=GOLD_BUCKET,
            Key=f"{prefix}/{run_id}/run_record.json",
            Body=json.dumps(run_record, indent=2),
        )
        s3.put_object(
            Bucket=GOLD_BUCKET,
            Key=f"{prefix}/{run_id}/preprocessing_stats.json",
            Body=json.dumps(preprocessing_stats, indent=2),
        )
        s3.put_object(
            Bucket=GOLD_BUCKET,
            Key=f"{prefix}/{run_id}/explicabilidad/narrativa_negocio.md",
            Body=narrativa,
        )
        if model_name == "LR":
            s3.put_object(
                Bucket=GOLD_BUCKET,
                Key=f"{prefix}/{run_id}/explicabilidad/importancias_lr.json",
                Body=json.dumps(explicabilidad, indent=2),
            )
        elif model_name == "DT":
            s3.put_object(
                Bucket=GOLD_BUCKET,
                Key=f"{prefix}/{run_id}/explicabilidad/reglas_dt.txt",
                Body=explicabilidad.get("reglas_arbol_debug", ""),
            )
            s3.put_object(
                Bucket=GOLD_BUCKET,
                Key=f"{prefix}/{run_id}/explicabilidad/importancias_dt.json",
                Body=json.dumps(
                    explicabilidad.get("importancia_features", []), indent=2
                ),
            )
    except Exception:  # noqa: BLE001, S110
        pass


def get_git_commit():
    try:
        return subprocess.getoutput("git rev-parse HEAD").strip()
    except Exception:  # noqa: BLE001
        return "UNKNOWN_COMMIT"


def main():
    t_start = time.time()

    dataset_version = os.environ.get("DATASET_VERSION", "v1")
    model_name = os.environ.get("MODEL_NAME", "LR").upper()
    variant = os.environ.get("VARIANT", "V2").upper()
    usa_pesos = os.environ.get("USA_PESOS", "true").lower() == "true"
    semilla = int(os.environ.get("SEMILLA", "42"))
    fast_dev_run = os.environ.get("FAST_DEV_RUN", "false").lower() == "true"
    parallelism = int(os.environ.get("PARALLELISM", "2"))

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_id = os.environ.get(
        "RUN_ID", f"{model_name}_{variant}_{dataset_version}_{timestamp_str}"
    )

    spark = create_spark_session(f"07_train_{run_id}")
    try:
        input_path = f"{GOLD_MODEL_INPUTS}/dataset_version={dataset_version}/"
        try:
            all_data = spark.read.parquet(GOLD_MODEL_INPUTS).filter(
                col("dataset_version") == dataset_version
            )
        except Exception:  # noqa: BLE001
            all_data = spark.read.parquet(input_path)
            if "dataset_version" not in all_data.columns:
                all_data = all_data.withColumn("dataset_version", lit(dataset_version))

        train_df = all_data.filter(col("split") == "train")
        test_df = all_data.filter(col("split") == "test")

        n_train = train_df.count()
        n_test = test_df.count()
        n_pos_train = train_df.filter(col("label") == 1).count()
        n_pos_test = test_df.filter(col("label") == 1).count()

        # Pesos de clase
        if usa_pesos:
            n_neg_train = n_train - n_pos_train
            peso_pos = (n_neg_train / n_pos_train) if n_pos_train > 0 else 1.0
            train_df = train_df.withColumn(
                "class_weight",
                when(col("label") == 1, lit(peso_pos)).otherwise(lit(1.0)),
            )
            weight_col = "class_weight"
        else:
            weight_col = None

        # Construir Pipeline
        pipeline, estimator, assembler_inputs = construir_pipeline(
            df=train_df,
            model_name=model_name,
            variant=variant,
            weight_col=weight_col,
            semilla=semilla,
        )

        # Cross Validation (CV en train)
        cv_model = entrenar_modelo_cv(
            train_df=train_df,
            pipeline=pipeline,
            estimator=estimator,
            model_name=model_name,
            semilla=semilla,
            num_folds=5,
            fast_dev_run=fast_dev_run,
            parallelism=parallelism,
        )

        best_pipeline_model = cv_model.bestModel

        # Generar predicciones C2
        c2_df = generar_predicciones_c2(
            best_model=best_pipeline_model,
            all_data_df=all_data,
            model_name=model_name,
            variant=variant,
            dataset_version=dataset_version,
        )

        # Rutas de almacenamiento
        pred_path = f"{GOLD_MODEL_OUTPUTS}/predictions/model_version={run_id}/"
        model_path = f"{GOLD_MODEL_ARTIFACTS}/{run_id}/model/"

        c2_df.write.mode("overwrite").parquet(pred_path)
        best_pipeline_model.write().overwrite().save(model_path)

        # Explicabilidad
        train_transformed = best_pipeline_model.transform(train_df)
        explicabilidad = extraer_explicabilidad(
            best_model=best_pipeline_model,
            train_transformed_df=train_transformed,
            assembler_inputs=assembler_inputs,
            model_name=model_name,
        )
        narrativa = generar_narrativa_negocio(explicabilidad)

        tiempo_total = time.time() - t_start

        # Hiperparámetros ganadores y grilla probada
        best_estimator = best_pipeline_model.stages[-1]
        hiperparametros = {}
        if model_name == "LR":
            hiperparametros = {
                "regParam": float(best_estimator.getRegParam()),
                "elasticNetParam": float(best_estimator.getElasticNetParam()),
            }
            grilla_probada = (
                [{"regParam": 0.01, "elasticNetParam": 0.0}]
                if fast_dev_run
                else [
                    {"regParam": 0.001, "elasticNetParam": 0.0},
                    {"regParam": 0.001, "elasticNetParam": 0.5},
                    {"regParam": 0.01, "elasticNetParam": 0.0},
                    {"regParam": 0.01, "elasticNetParam": 0.5},
                    {"regParam": 0.1, "elasticNetParam": 0.0},
                    {"regParam": 0.1, "elasticNetParam": 0.5},
                ]
            )
        elif model_name == "DT":
            hiperparametros = {
                "maxDepth": int(best_estimator.getMaxDepth()),
                "minInstancesPerNode": int(best_estimator.getMinInstancesPerNode()),
            }
            grilla_probada = (
                [{"maxDepth": 3, "minInstancesPerNode": 50}]
                if fast_dev_run
                else [
                    {"maxDepth": 3, "minInstancesPerNode": 50},
                    {"maxDepth": 3, "minInstancesPerNode": 200},
                    {"maxDepth": 5, "minInstancesPerNode": 50},
                    {"maxDepth": 5, "minInstancesPerNode": 200},
                    {"maxDepth": 6, "minInstancesPerNode": 50},
                    {"maxDepth": 6, "minInstancesPerNode": 200},
                ]
            )
        else:
            grilla_probada = []

        vcpu_horas = round((tiempo_total / 3600.0) * 6.0, 4)
        gb_horas = round((tiempo_total / 3600.0) * 12.0, 4)

        run_record = {
            "run_id": run_id,
            "fecha": datetime.now(timezone.utc).isoformat(),
            "git_commit": get_git_commit(),
            "dataset_version": dataset_version,
            "variant": variant,
            "model_name": model_name,
            "usa_pesos": usa_pesos,
            "semilla": semilla,
            "hiperparametros_ganadores": hiperparametros,
            "grilla_probada": grilla_probada,
            "metricas_cv": {
                "pr_auc_por_param_grid": [float(x) for x in cv_model.avgMetrics],
                "pr_auc_promedio": float(max(cv_model.avgMetrics)),
            },
            "n_train": n_train,
            "n_test": n_test,
            "n_positivos_train": n_pos_train,
            "n_positivos_test": n_pos_test,
            "duracion_segundos": round(tiempo_total, 2),
            "vcpu_horas": vcpu_horas,
            "gb_horas": gb_horas,
            "ruta_modelo": model_path,
            "ruta_predicciones": pred_path,
        }

        # Extraer estadísticas de preprocesamiento (mediana de imputación y categorías)
        imputer_models = [
            s for s in best_pipeline_model.stages if isinstance(s, ImputerModel)
        ]
        medianas_imp = {}
        if imputer_models:
            surrogate_row = imputer_models[0].surrogateDF.first()
            if surrogate_row:
                medianas_imp = {
                    k: float(v)
                    for k, v in surrogate_row.asDict().items()
                    if v is not None
                }

        top_ciiu = []
        if "ciiu_principal" in train_df.columns:
            top_ciiu = [
                r["ciiu_principal"]
                for r in train_df.filter(
                    col("ciiu_principal").isNotNull()
                    & (col("ciiu_principal") != "OTROS")
                )
                .groupBy("ciiu_principal")
                .count()
                .orderBy(col("count").desc())
                .limit(30)
                .collect()
            ]

        top_dept = []
        if "departamento" in train_df.columns:
            top_dept = [
                r["departamento"]
                for r in train_df.filter(
                    col("departamento").isNotNull()
                    & (col("departamento") != "DESCONOCIDO")
                )
                .groupBy("departamento")
                .count()
                .orderBy(col("count").desc())
                .limit(25)
                .collect()
            ]

        preprocessing_stats = {
            "top_ciiu": top_ciiu,
            "top_departamentos": top_dept,
            "medianas_imputacion": medianas_imp,
            "dataset_version": dataset_version,
            "fecha": datetime.now(timezone.utc).isoformat(),
        }

        # Guardar todos los artefactos
        guardar_artefactos_entrenamiento(
            run_id=run_id,
            run_record=run_record,
            preprocessing_stats=preprocessing_stats,
            explicabilidad=explicabilidad,
            narrativa=narrativa,
            model_name=model_name,
        )

        print("[07_train_model] Entrenado con éxito. Run Record:")
        print(json.dumps(run_record, indent=2))

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
