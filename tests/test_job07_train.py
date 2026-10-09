import importlib
import unittest

from pyspark.ml.feature import OneHotEncoder, StandardScaler
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
