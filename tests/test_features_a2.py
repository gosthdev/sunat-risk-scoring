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
        # monto_mediano_soles en órdenes válidas
        self.assertGreaterEqual(row["monto_mediano_soles"], 1000.0)
        self.assertLessEqual(row["monto_mediano_soles"], 2000.0)

    def test_monto_mediano_soles_excluye_anuladas_estricto(self):
        """Verifica que órdenes anuladas se traten como NULL y no desplacen la mediana hacia abajo."""
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
        # 3 órdenes vigentes (2000, 4000, 6000) -> mediana = 4000.
        # 1 orden anulada (10000). Si fuera 0.0, los valores serían (0, 2000, 4000, 6000) y la mediana caería a 2000.
        data = [
            ("20100000002", 2000.0, "Vigente", 20500000001, "MINEDU", "2025-02-01"),
            ("20100000002", 4000.0, "Vigente", 20500000001, "MINEDU", "2025-03-01"),
            ("20100000002", 6000.0, "Vigente", 20500000001, "MINEDU", "2025-04-01"),
            ("20100000002", 10000.0, "Anulada", 20500000002, "MINSA", "2025-05-01"),
        ]
        df = self.spark.createDataFrame(data, schema)
        res = job03.build_contratacion_estado_features(self.spark, ordenes_df=df)
        row = res.filter(col("RUC") == 20100000002).first()
        assert row is not None

        # La mediana debe ser 4000.0 (de los vigentes 2000, 4000, 6000), no 2000.0 (distorsionada por 0.0)
        self.assertEqual(row["monto_mediano_soles"], 4000.0)
        self.assertAlmostEqual(row["monto_total_soles"], 12000.0)

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
        self.assertEqual(row["n_ordenes"], 0)
        self.assertEqual(row["monto_total_soles"], 0.0)
        self.assertEqual(row["monto_por_trabajador"], 0.0)
        self.assertEqual(row["monto_total_contratado_estado"], 0.0)
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

    def test_ordenes_con_estado_nulo_no_se_anulan(self):
        """Órdenes con estado NULL no deben ser tratadas como anuladas ni perder su monto."""
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
            ("20100000003", 5000.0, None, 20500000001, "MINEDU", "2025-03-01"),
            ("20100000003", 3000.0, "Vigente", 20500000001, "MINEDU", "2025-04-01"),
        ]
        df = self.spark.createDataFrame(data, schema)
        res = job03.build_contratacion_estado_features(self.spark, ordenes_df=df)
        row = res.filter(col("RUC") == 20100000003).first()

        assert row is not None
        self.assertEqual(row["n_ordenes"], 2)
        # La orden con estado NULL debe sumar y contar como válida
        self.assertAlmostEqual(row["monto_total_soles"], 8000.0)
        self.assertEqual(row["pct_ordenes_anuladas"], 0.0)
        self.assertAlmostEqual(row["monto_maximo_soles"], 5000.0)

    def test_attach_regional_features_no_duplicate_column(self):
        """Verifica que attach_regional_features no genere error de columna duplicada."""
        import tempfile

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
            [(20100000001, "202506", "LIMA", 10.0)], padron_schema
        )
        contrat = self.spark.createDataFrame([], contrat_schema)
        pricos = self.spark.createDataFrame([], pricos_schema)

        features = job03.compute_all_ruc_features(padron, contrat, pricos)
        self.assertIn("informalidad_epen_departamento", features.columns)

        with tempfile.TemporaryDirectory() as tmp_dir:
            reg_schema = StructType(
                [
                    StructField("Departamento", StringType(), True),
                    StructField("pct_informalidad", DoubleType(), True),
                ]
            )
            reg_df = self.spark.createDataFrame([("LIMA", 55.5)], reg_schema)
            reg_path = f"{tmp_dir}/regional_summary"
            reg_df.write.parquet(reg_path)

            attached = job03.attach_regional_features(
                self.spark, features, regional_path=reg_path
            )
            cols = [
                c for c in attached.columns if c == "informalidad_epen_departamento"
            ]
            self.assertEqual(len(cols), 1)

            row = attached.first()
            assert row is not None
            self.assertAlmostEqual(row["informalidad_epen_departamento"], 55.5)

            out_path = f"{tmp_dir}/output_features"
            attached.write.parquet(out_path)

    def test_attach_regional_features_sin_regional_falla(self):
        """Sin gold/regional_summary el job falla; no deja la feature en NULL en silencio."""
        from pyspark.errors import AnalysisException

        assert self.spark is not None
        features = self.spark.createDataFrame(
            [(20100000001, "LIMA")], "RUC long, departamento string"
        )
        with self.assertRaises(AnalysisException):
            job03.attach_regional_features(
                self.spark, features, regional_path="/tmp/ruta_inexistente_no_file"
            )

    def test_solo_se_suman_montos_en_soles(self):
        assert self.spark is not None
        df = self.spark.createDataFrame(
            [
                (
                    "20100000001",
                    1000.0,
                    "Vigente",
                    20500000001,
                    "A",
                    "2025-02-01",
                    "Nuevos Soles",
                ),
                (
                    "20100000001",
                    500.0,
                    "Vigente",
                    20500000001,
                    "A",
                    "2025-03-01",
                    "Dólar Norteamericano",
                ),
                ("20100000001", 200.0, "Vigente", 20500000001, "A", "2025-04-01", None),
            ],
            "ruc_contratista string, monto_total_orden_original double, "
            "estadocontratacion string, ruc_entidad long, entidad string, "
            "fecha_de_emision string, moneda string",
        )
        row = (
            job03.build_contratacion_estado_features(self.spark, ordenes_df=df)
            .filter(col("RUC") == 20100000001)
            .first()
        )

        assert row is not None
        self.assertEqual(row["n_ordenes"], 3)  # la orden en USD sí cuenta
        self.assertAlmostEqual(row["monto_total_soles"], 1200.0)  # sin los 500 USD

    def test_nrotrab_no_disponible_no_es_cero_trabajadores(self):
        assert self.spark is not None
        padron = self.spark.createDataFrame(
            [
                (20100000001, "NO DISPONIBLE", 2025, 6),
                (20100000002, "0", 2025, 6),
                (20100000003, "12", 2025, 6),
            ],
            "RUC long, NroTrab string, anio int, mes int",
        )
        filas = {
            r["RUC"]: r
            for r in job03.build_padron_features(
                self.spark, padron_df=padron, mes_padron="202506"
            ).collect()
        }

        self.assertEqual(
            (
                filas[20100000001]["nrotrab_disponible"],
                filas[20100000001]["sin_trabajadores"],
            ),
            (0, 0),
        )
        self.assertEqual(
            (
                filas[20100000002]["nrotrab_disponible"],
                filas[20100000002]["sin_trabajadores"],
            ),
            (1, 1),
        )
        self.assertEqual(
            (
                filas[20100000003]["nrotrab_disponible"],
                filas[20100000003]["sin_trabajadores"],
            ),
            (1, 0),
        )

    def test_verificar_salida_exige_cada_corte_y_ruc_unico(self):
        assert self.spark is not None
        cortes = [("202506", "2025-06-30"), ("202512", "2025-12-31")]
        esquema = "RUC long, mes_referencia string"

        # El mismo RUC en dos meses es válido (un corte por mes).
        ok = self.spark.createDataFrame(
            [(1, "202506"), (1, "202512"), (2, "202512")], esquema
        )
        job03.verificar_salida(ok, cortes)

        sin_corte = self.spark.createDataFrame([(1, "202506")], esquema)
        with self.assertRaises(ValueError):
            job03.verificar_salida(sin_corte, cortes)

        repetido = self.spark.createDataFrame(
            [(1, "202506"), (1, "202506"), (1, "202512")], esquema
        )
        with self.assertRaises(ValueError):
            job03.verificar_salida(repetido, cortes)
