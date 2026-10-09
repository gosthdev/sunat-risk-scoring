import importlib
import unittest

from pyspark.ml.feature import OneHotEncoder, SQLTransformer, StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

job07 = importlib.import_module("07_train_model")


class TestJob07Train(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-job07-train")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def _crear_datos_sinteticos(self, n_filas=80):
        assert self.spark is not None
        rows = []
        for i in range(1, n_filas + 1):
            ruc = f"{20100000000 + i:011d}"
            split = "train" if i <= int(n_filas * 0.75) else "test"
            fold = (i % 3) if split == "train" else None
            label = 1 if (i % 5 == 0) else 0
            dept = "LIMA" if i % 2 == 0 else "AREQUIPA"
            tipo = "SOCIEDAD ANONIMA" if i % 2 == 0 else "PERSONA NATURAL"
            ciiu = "4659" if i % 3 == 0 else "4100"
            trabajadores = float(i % 8) if (i % 4 != 0) else None
            monto = float(10000 * (i % 10))
            rows.append(
                (
                    ruc,
                    label,
                    split,
                    fold,
                    dept,
                    tipo,
                    ciiu,
                    trabajadores,
                    0 if trabajadores and trabajadores > 0 else 1,
                    1 if monto > 0 else 0,
                    monto,
                    i % 4,
                    0.0,
                    monto / trabajadores if trabajadores and trabajadores > 0 else None,
                    "ACTIVO",
                    "HABIDO",
                )
            )

        schema = StructType(
            [
                StructField("ruc", StringType(), False),
                StructField("label", IntegerType(), False),
                StructField("split", StringType(), False),
                StructField("fold", IntegerType(), True),
                StructField("departamento", StringType(), True),
                StructField("tipo_contribuyente", StringType(), True),
                StructField("ciiu_principal", StringType(), True),
                StructField("nro_trabajadores", DoubleType(), True),
                StructField("sin_trabajadores", IntegerType(), True),
                StructField("contrata_con_estado", IntegerType(), True),
                StructField("monto_total_soles", DoubleType(), True),
                StructField("n_ordenes", IntegerType(), True),
                StructField("pct_ordenes_anuladas", DoubleType(), True),
                StructField("monto_por_trabajador", DoubleType(), True),
                StructField("Estado", StringType(), True),
                StructField("Condicion", StringType(), True),
            ]
        )
        return self.spark.createDataFrame(rows, schema)

    def test_estructura_pipeline_lr_sin_doble_escalado(self):
        """Para LR, debe incluir OneHotEncoder y NO debe tener StandardScaler."""
        df = self._crear_datos_sinteticos()
        pipeline, _estimator, _inputs = job07.construir_pipeline(
            df=df,
            model_name="LR",
            variant="V2",
            weight_col="class_weight",
        )

        stages = pipeline.getStages()
        has_ohe = any(isinstance(s, OneHotEncoder) for s in stages)
        has_scaler = any(isinstance(s, StandardScaler) for s in stages)

        self.assertTrue(has_ohe, "El pipeline de LR debe incluir OneHotEncoder")
        self.assertFalse(
            has_scaler, "El pipeline de LR no debe incluir StandardScaler redundante"
        )

    def test_estructura_pipeline_dt_sin_one_hot(self):
        """Para DT, NO debe incluir OneHotEncoder (los árboles usan StringIndexer directo)."""
        df = self._crear_datos_sinteticos()
        pipeline, _estimator, _inputs = job07.construir_pipeline(
            df=df,
            model_name="DT",
            variant="V2",
            weight_col="class_weight",
        )

        stages = pipeline.getStages()
        has_ohe = any(isinstance(s, OneHotEncoder) for s in stages)
        self.assertFalse(has_ohe, "El pipeline de DT no debe incluir OneHotEncoder")

    def test_etapa_log1p_montos_monetarios(self):
        """Verifica que el pipeline incluya SQLTransformer con log1p para montos de colas pesadas."""
        df = self._crear_datos_sinteticos(n_filas=20)
        pipeline, _, assembler_inputs = job07.construir_pipeline(
            df=df,
            model_name="LR",
            variant="V2",
            weight_col=None,
        )
        stages = pipeline.getStages()
        sql_transformers = [s for s in stages if isinstance(s, SQLTransformer)]
        log_transformers = [
            s for s in sql_transformers if "ln(1.0 +" in s.getStatement()
        ]
        self.assertTrue(len(log_transformers) > 0, "Debe existir transformación log1p")
        # Verificar que assembler_inputs use las columnas transformadas con _log
        self.assertIn("monto_total_soles_log", assembler_inputs)
        self.assertNotIn("monto_total_soles_imp", assembler_inputs)

    def test_pipeline_sin_pesos_no_lanza_npe(self):
        """Verifica que usa_pesos=False (weight_col=None) no lance NullPointerException en Spark MLlib."""
        df = self._crear_datos_sinteticos(n_filas=30)
        train_df = df.filter(col("split") == "train")

        # Probar LR con weight_col=None
        pipeline_lr, estimator_lr, _ = job07.construir_pipeline(
            df=train_df,
            model_name="LR",
            variant="V2",
            weight_col=None,
        )
        self.assertFalse(estimator_lr.isDefined(estimator_lr.weightCol))
        model_lr = pipeline_lr.fit(train_df)
        self.assertIsNotNone(model_lr)

        # Probar DT con weight_col=None
        pipeline_dt, estimator_dt, _ = job07.construir_pipeline(
            df=train_df,
            model_name="DT",
            variant="V2",
            weight_col=None,
        )
        self.assertFalse(estimator_dt.isDefined(estimator_dt.weightCol))
        model_dt = pipeline_dt.fit(train_df)
        self.assertIsNotNone(model_dt)

    def test_smoke_fit_cv_dt_y_contrato_c2(self):
        """Smoke test de punta a punta para Decision Tree con CrossValidator y generación C2."""
        df = self._crear_datos_sinteticos(n_filas=40)
        train_df = df.filter(col("split") == "train").withColumn(
            "class_weight", lit(1.0)
        )

        pipeline, estimator, _inputs = job07.construir_pipeline(
            df=train_df,
            model_name="DT",
            variant="V2",
            weight_col="class_weight",
            semilla=42,
        )

        cv_model = job07.entrenar_modelo_cv(
            train_df=train_df,
            pipeline=pipeline,
            estimator=estimator,
            model_name="DT",
            semilla=42,
            num_folds=3,
            fast_dev_run=True,
            parallelism=1,
        )
        best_model = cv_model.bestModel
        self.assertIsNotNone(best_model)

        c2_df = job07.generar_predicciones_c2(
            best_model=best_model,
            all_data_df=df,
            model_name="DT",
            variant="V2",
            dataset_version="v1",
        )
        self.assertEqual(c2_df.count(), df.count())
        self.assertEqual(c2_df.filter(col("score").isNull()).count(), 0)

    def test_smoke_fit_cv_y_contrato_c2(self):
        """Smoke test de punta a punta: entrenamiento rápido con CV y producción de C2."""
        df = self._crear_datos_sinteticos(n_filas=40)
        train_df = df.filter(col("split") == "train").withColumn(
            "class_weight", lit(1.0)
        )

        pipeline, estimator, _inputs = job07.construir_pipeline(
            df=train_df,
            model_name="LR",
            variant="V2",
            weight_col="class_weight",
            semilla=42,
        )

        cv_model = job07.entrenar_modelo_cv(
            train_df=train_df,
            pipeline=pipeline,
            estimator=estimator,
            model_name="LR",
            semilla=42,
            num_folds=3,
            fast_dev_run=True,
            parallelism=1,
        )
        best_model = cv_model.bestModel
        self.assertIsNotNone(best_model)

        # Generar Contrato C2
        c2_df = job07.generar_predicciones_c2(
            best_model=best_model,
            all_data_df=df,
            model_name="LR",
            variant="V2",
            dataset_version="v1",
        )

        # 1. Total filas igual
        self.assertEqual(c2_df.count(), df.count())

        # 2. Score acotado en [0, 1] y sin nulos
        self.assertEqual(c2_df.filter(col("score").isNull()).count(), 0)
        out_of_range = c2_df.filter((col("score") < 0.0) | (col("score") > 1.0)).count()
        self.assertEqual(out_of_range, 0)

        # 3. Columnas obligatorias de C2 presentes
        required_cols = {
            "ruc",
            "score",
            "rank_global",
            "label",
            "split",
            "departamento",
            "model_name",
            "variant",
            "dataset_version",
            "scored_at",
        }
        self.assertTrue(required_cols.issubset(set(c2_df.columns)))

        # 4. Ranks únicos ordenados
        min_rank = c2_df.selectExpr("min(rank_global)").first()[0]
        self.assertEqual(min_rank, 1)

    def test_explicabilidad_lr_y_dt(self):
        """Verifica la extracción de coeficientes explicables y generación de narrativa."""
        df = self._crear_datos_sinteticos(n_filas=30)
        train_df = df.filter(col("split") == "train").withColumn(
            "class_weight", lit(1.0)
        )

        # Test explicabilidad con LR
        pipeline_lr, _, inputs_lr = job07.construir_pipeline(
            df=train_df, model_name="LR", variant="V2", weight_col="class_weight"
        )
        model_lr = pipeline_lr.fit(train_df)
        transformed_lr = model_lr.transform(train_df)
        exp_lr = job07.extraer_explicabilidad(
            model_lr, transformed_lr, inputs_lr, model_name="LR"
        )

        self.assertIn("top_factores_riesgo", exp_lr)
        self.assertIn("intercepto", exp_lr)

        narrativa_lr = job07.generar_narrativa_negocio(exp_lr)
        self.assertIn("Informe de Explicabilidad", narrativa_lr)

        # Test explicabilidad con DT
        pipeline_dt, _, inputs_dt = job07.construir_pipeline(
            df=train_df, model_name="DT", variant="V2", weight_col="class_weight"
        )
        model_dt = pipeline_dt.fit(train_df)
        transformed_dt = model_dt.transform(train_df)
        exp_dt = job07.extraer_explicabilidad(
            model_dt, transformed_dt, inputs_dt, model_name="DT"
        )

        self.assertIn("reglas_arbol_debug", exp_dt)
        self.assertIn("importancia_features", exp_dt)

    def test_guardar_artefactos_schema_c3_completo(self):
        """Verifica que guardar_artefactos_entrenamiento genere todos los archivos y campos de C3."""
        import json
        import os

        run_id = "test_run_c3_001"
        run_record = {
            "run_id": run_id,
            "fecha": "2026-10-09T00:00:00Z",
            "git_commit": "abcdef123456",
            "dataset_version": "v1",
            "variant": "V2",
            "model_name": "LR",
            "usa_pesos": True,
            "semilla": 42,
            "hiperparametros_ganadores": {"regParam": 0.01, "elasticNetParam": 0.0},
            "grilla_probada": [{"regParam": 0.01, "elasticNetParam": 0.0}],
            "metricas_cv": {
                "pr_auc_por_param_grid": [0.85],
                "pr_auc_promedio": 0.85,
            },
            "n_train": 100,
            "n_test": 25,
            "n_positivos_train": 5,
            "n_positivos_test": 1,
            "duracion_segundos": 12.5,
            "vcpu_horas": 0.02,
            "gb_horas": 0.04,
            "ruta_modelo": f"/tmp/{run_id}/model/",
            "ruta_predicciones": f"/tmp/{run_id}/predictions/",
        }
        prep_stats = {
            "top_ciiu": ["4659", "4100"],
            "top_departamentos": ["LIMA", "AREQUIPA"],
            "medianas_imputacion": {"monto_total_soles": 15000.0},
            "dataset_version": "v1",
            "fecha": "2026-10-09T00:00:00Z",
        }
        exp = {
            "model_name": "LR",
            "top_factores_riesgo": [
                {"feature": "monto_total_soles", "coef_scaled": 1.2}
            ],
        }
        narrativa = "# Narrativa de prueba"

        job07.guardar_artefactos_entrenamiento(
            run_id=run_id,
            run_record=run_record,
            preprocessing_stats=prep_stats,
            explicabilidad=exp,
            narrativa=narrativa,
            model_name="LR",
        )

        art_dir = f"/tmp/sunat_model_artifacts/{run_id}"
        self.assertTrue(os.path.exists(f"{art_dir}/run_record.json"))
        self.assertTrue(os.path.exists(f"{art_dir}/preprocessing_stats.json"))
        self.assertTrue(
            os.path.exists(f"{art_dir}/explicabilidad/narrativa_negocio.md")
        )
        self.assertTrue(
            os.path.exists(f"{art_dir}/explicabilidad/importancias_lr.json")
        )

        # Validar campos obligatorios de C3
        with open(f"{art_dir}/run_record.json") as f:
            saved_record = json.load(f)

        c3_required = [
            "run_id",
            "fecha",
            "git_commit",
            "dataset_version",
            "variant",
            "model_name",
            "usa_pesos",
            "semilla",
            "hiperparametros_ganadores",
            "grilla_probada",
            "metricas_cv",
            "n_train",
            "n_test",
            "n_positivos_train",
            "n_positivos_test",
            "duracion_segundos",
            "vcpu_horas",
            "gb_horas",
            "ruta_modelo",
            "ruta_predicciones",
        ]
        for field in c3_required:
            self.assertIn(
                field, saved_record, f"Campo {field} ausente en run_record.json"
            )

    def test_determinismo_semilla_cv(self):
        """Dos corridas de CV con la misma semilla deben producir idéntica PR-AUC."""
        df = self._crear_datos_sinteticos(n_filas=40)
        train_df = df.filter(col("split") == "train").withColumn(
            "class_weight", lit(1.0)
        )

        p1, e1, _ = job07.construir_pipeline(
            df=train_df,
            model_name="LR",
            variant="V2",
            weight_col="class_weight",
            semilla=42,
        )
        cv1 = job07.entrenar_modelo_cv(
            train_df=train_df,
            pipeline=p1,
            estimator=e1,
            model_name="LR",
            semilla=42,
            num_folds=3,
            fast_dev_run=True,
            parallelism=1,
        )

        p2, e2, _ = job07.construir_pipeline(
            df=train_df,
            model_name="LR",
            variant="V2",
            weight_col="class_weight",
            semilla=42,
        )
        cv2 = job07.entrenar_modelo_cv(
            train_df=train_df,
            pipeline=p2,
            estimator=e2,
            model_name="LR",
            semilla=42,
            num_folds=3,
            fast_dev_run=True,
            parallelism=1,
        )

        m1 = cv1.avgMetrics
        m2 = cv2.avgMetrics
        for v1, v2 in zip(m1, m2):
            self.assertAlmostEqual(v1, v2, places=4)
