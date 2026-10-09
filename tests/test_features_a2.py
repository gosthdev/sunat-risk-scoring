import importlib
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

job03 = importlib.import_module("03_feature_gold")


class TestFeaturesA2(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-features-a2")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def test_pct_ordenes_anuladas_y_exclusion_monto(self):
        """Verifica que órdenes anuladas no sumen al monto pero sí se calculen en pct_anuladas."""
        assert self.spark is not None
        schema = StructType(
            [
                StructField("ruc_contratista", StringType(), True),
                StructField("monto_total_orden_original", DoubleType(), True),
                StructField("estadocontratacion", StringType(), True),
                StructField("ruc_entidad", LongType(), True),
                StructField("entidad", StringType(), True),
                StructField("fecha_de_emision", StringType(), True),
            ]
        )

        data = [
            ("20100000001", 1000.0, "Vigente", 20500000001, "MINEDU", "2025-02-01"),
            ("20100000001", 2000.0, "Adjudicada", 20500000001, "MINEDU", "2025-03-01"),
            ("20100000001", 5000.0, "Anulada", 20500000002, "MINSA", "2025-04-01"),
        ]
        df = self.spark.createDataFrame(data, schema)
        res = job03.build_contratacion_estado_features(self.spark, ordenes_df=df)
        row = res.filter(col("RUC") == 20100000001).first()

        assert row is not None
        # Monto total debe ser 1000 + 2000 = 3000 (excluyendo los 5000 de la orden anulada)
        self.assertAlmostEqual(row["monto_total_soles"], 3000.0)
        self.assertEqual(row["n_ordenes"], 3)
        # pct_ordenes_anuladas = 1/3 = 0.3333333
        self.assertAlmostEqual(row["pct_ordenes_anuladas"], 1.0 / 3.0, places=4)
        # Concentración con MINEDU: 3000 / 3000 = 1.0 (porque la orden de MINSA fue anulada)
        self.assertAlmostEqual(row["pct_monto_en_entidad_principal"], 1.0)

    def test_monto_por_trabajador_cero_o_nulo(self):
        """RUC con 0 o nulo trabajadores debe tener monto_por_trabajador = None, sin dividir por 0."""
        assert self.spark is not None
        padron_schema = StructType(
            [
                StructField("RUC", LongType(), True),
                StructField("mes_referencia", StringType(), True),
                StructField("departamento", StringType(), True),
                StructField("nro_trabajadores", DoubleType(), True),
                StructField("sin_trabajadores", IntegerType(), True),
            ]
        )
        contrat_schema = StructType(
            [
                StructField("RUC", LongType(), True),
                StructField("monto_total_soles", DoubleType(), True),
                StructField("n_ordenes", LongType(), True),
                StructField("pct_ordenes_anuladas", DoubleType(), True),
                StructField("fecha_primera_orden", StringType(), True),
            ]
        )
        pricos_schema = StructType(
            [
                StructField("RUC", LongType(), True),
                StructField("es_prico", StringType(), True),
            ]
        )

        padron = self.spark.createDataFrame(
            [
                (20100000001, "202506", "LIMA", 0.0, 1),
                (20100000002, "202506", "CUSCO", None, 1),
                (20100000003, "202506", "AREQUIPA", 5.0, 0),
            ],
            padron_schema,
        )

        contrat = self.spark.createDataFrame(
            [
                (20100000001, 50000.0, 2, 0.0, "2025-01-10"),
                (20100000002, 100000.0, 4, 0.0, "2025-02-15"),
                (20100000003, 50000.0, 1, 0.0, "2025-03-20"),
            ],
            contrat_schema,
        )

        pricos = self.spark.createDataFrame([], pricos_schema)

        features = job03.compute_all_ruc_features(padron, contrat, pricos)
        rows = {r["RUC"]: r["monto_por_trabajador"] for r in features.collect()}

        self.assertIsNone(rows[20100000001])
        self.assertIsNone(rows[20100000002])
        self.assertAlmostEqual(rows[20100000003], 10000.0)

    def test_contrata_con_estado_sin_ordenes(self):
        """RUC sin contratos debe tener contrata_con_estado = 0 y montos en 0."""
        assert self.spark is not None
        padron_schema = StructType(
            [
                StructField("RUC", LongType(), True),
                StructField("mes_referencia", StringType(), True),
                StructField("departamento", StringType(), True),
                StructField("nro_trabajadores", DoubleType(), True),
            ]
        )
        contrat_schema = StructType(
            [
                StructField("RUC", LongType(), True),
                StructField("monto_total_soles", DoubleType(), True),
                StructField("n_ordenes", LongType(), True),
                StructField("pct_ordenes_anuladas", DoubleType(), True),
                StructField("fecha_primera_orden", StringType(), True),
            ]
        )
        pricos_schema = StructType(
            [
                StructField("RUC", LongType(), True),
                StructField("es_prico", StringType(), True),
            ]
        )

        padron = self.spark.createDataFrame(
            [
                (20999999999, "202506", "LIMA", 2.0),
            ],
            padron_schema,
        )
        contrat = self.spark.createDataFrame([], contrat_schema)
        pricos = self.spark.createDataFrame([], pricos_schema)

        features = job03.compute_all_ruc_features(padron, contrat, pricos)
        row = features.first()
        assert row is not None

        self.assertEqual(row["contrata_con_estado"], 0)
        self.assertEqual(row["monto_total_soles"], 0.0)
        self.assertEqual(row["cantidad_contratos_estado"], 0)
        self.assertIsNone(row["antiguedad_contratacion_estado_dias"])

    def test_filtro_temporal_corte_ordenes(self):
        """Órdenes posteriores a la fecha de corte no deben incluirse en las métricas."""
        assert self.spark is not None
        schema = StructType(
            [
                StructField("ruc_contratista", StringType(), True),
                StructField("monto_total_orden_original", DoubleType(), True),
                StructField("estadocontratacion", StringType(), True),
                StructField("ruc_entidad", LongType(), True),
                StructField("entidad", StringType(), True),
                StructField("fecha_de_emision", StringType(), True),
            ]
        )

        data = [
            ("20100000001", 1000.0, "Vigente", 20500000001, "MINEDU", "2025-05-15"),
            (
                "20100000001",
                9999.0,
                "Vigente",
                20500000001,
                "MINEDU",
                "2025-08-01",
            ),  # Posterior a jun-2025
        ]
        df = self.spark.createDataFrame(data, schema)
        res = job03.build_contratacion_estado_features(
            self.spark, ordenes_df=df, fecha_corte="2025-06-30"
        )
        row = res.filter(col("RUC") == 20100000001).first()

        assert row is not None
        self.assertEqual(row["n_ordenes"], 1)
        self.assertAlmostEqual(row["monto_total_soles"], 1000.0)
