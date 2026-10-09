import importlib
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

job06 = importlib.import_module("06_dataset_curado")


class TestJob06Dataset(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-job06-dataset")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def _crear_datos_sinteticos(self, n_filas=300, n_positivos=30, n_pricos=20):
        assert self.spark is not None
        padron_rows = []
        for i in range(1, n_filas + 1):
            ruc = 20100000000 + i
            dept = (
                "LIMA"
                if i % 2 == 0
                else ("AREQUIPA" if i % 3 == 0 else f"DEP_{i % 50}")
            )
            ciiu = (
                "4659" if i % 2 == 0 else ("4100" if i % 3 == 0 else f"CIIU_{i % 60}")
            )
            padron_rows.append(
                (
                    ruc,
                    ciiu,
                    dept,
                    float(i % 10),
                    0 if (i % 10) > 0 else 1,
                    1000.0 * (i % 5),
                    i % 5,
                    "ACTIVO",
                    "HABIDO",
                )
            )

        padron_schema = StructType(
            [
                StructField("RUC", LongType(), False),
                StructField("ciiu_principal", StringType(), True),
                StructField("departamento", StringType(), True),
                StructField("nro_trabajadores", DoubleType(), True),
                StructField("sin_trabajadores", IntegerType(), True),
                StructField("monto_total_soles", DoubleType(), True),
                StructField("n_ordenes", IntegerType(), True),
                StructField("Estado", StringType(), True),
                StructField("Condicion", StringType(), True),
            ]
        )
        padron_df = self.spark.createDataFrame(padron_rows, padron_schema)

        # Positivos SSCO
        ssco_rows = [(20100000000 + i, True) for i in range(1, n_positivos + 1)]
        ssco_schema = StructType(
            [
                StructField("RUC", LongType(), False),
                StructField("es_ssco", StringType(), True),
            ]
        )
        ssco_df = self.spark.createDataFrame(ssco_rows, ssco_schema)

        # PRICOS (últimos n_pricos contribuyentes)
        pricos_rows = [(20100000000 + n_filas - i,) for i in range(n_pricos)]
        pricos_schema = StructType(
            [
                StructField("RUC", LongType(), False),
            ]
        )
        pricos_df = self.spark.createDataFrame(pricos_rows, pricos_schema)

        return padron_df, ssco_df, pricos_df

    def test_ruc_unicidad_y_exclusion_pricos(self):
        """Verifica que no existan duplicados y que ningún PRICO ingrese al dataset."""
        padron, ssco, pricos = self._crear_datos_sinteticos()
        curado = job06.curar_dataset(padron, ssco, pricos, semilla=42)

        # 1. Unicidad
        duplicados = (
            curado.groupBy("ruc")
            .agg(count("*").alias("cnt"))
            .filter(col("cnt") > 1)
            .count()
        )
        self.assertEqual(duplicados, 0)

        # 2. Exclusión estricta de PRICOS
        pricos_rucs = {f"{r['RUC']:011d}" for r in pricos.collect()}
        curado_rucs = {r["ruc"] for r in curado.collect()}
        interseccion = pricos_rucs.intersection(curado_rucs)
        self.assertEqual(len(interseccion), 0)

    def test_split_disjunto_y_folds_validos(self):
        """Verifica que train y test sean disjuntos y que fold solo exista en train."""
        padron, ssco, pricos = self._crear_datos_sinteticos()
        curado = job06.curar_dataset(padron, ssco, pricos, semilla=42)

        train_df = curado.filter(col("split") == "train")
        test_df = curado.filter(col("split") == "test")

        train_rucs = {r["ruc"] for r in train_df.collect()}
        test_rucs = {r["ruc"] for r in test_df.collect()}

        # No hay RUC en ambos conjuntos
        self.assertEqual(len(train_rucs.intersection(test_rucs)), 0)

        # En test todos los folds deben ser nulos
        self.assertEqual(test_df.filter(col("fold").isNotNull()).count(), 0)

        # En train ningún fold debe ser nulo y debe estar entre 0 y 4
        self.assertEqual(train_df.filter(col("fold").isNull()).count(), 0)
        invalid_folds = train_df.filter((col("fold") < 0) | (col("fold") > 4)).count()
        self.assertEqual(invalid_folds, 0)

    def test_determinismo_reproducibilidad(self):
        """Dos corridas con la misma semilla deben producir exactamente el mismo DataFrame."""
        padron, ssco, pricos = self._crear_datos_sinteticos()
        run1 = job06.curar_dataset(padron, ssco, pricos, semilla=42)
        run2 = job06.curar_dataset(padron, ssco, pricos, semilla=42)

        list1 = [
            (r["ruc"], r["split"], r["fold"], r["label"])
            for r in run1.orderBy("ruc").collect()
        ]
        list2 = [
            (r["ruc"], r["split"], r["fold"], r["label"])
            for r in run2.orderBy("ruc").collect()
        ]
        self.assertEqual(list1, list2)

    def test_reporte_invariantes(self):
        """Verifica que la generación de reporte contenga todas las claves requeridas y prevalencia controlada."""
        padron, ssco, pricos = self._crear_datos_sinteticos()
        curado = job06.curar_dataset(padron, ssco, pricos, semilla=42)
        rep = job06.generar_reporte_curado(curado, dataset_version="v1")

        self.assertIn("total_filas", rep)
        self.assertIn("prevalencia_train", rep)
        self.assertIn("prevalencia_test", rep)
        self.assertIn("positivos_por_fold", rep)
        self.assertEqual(len(rep["positivos_por_fold"]), 5)

        # Invariante contractual: diferencia relativa de prevalencia < 10%
        self.assertLess(
            rep["diferencia_relativa_prevalencia"],
            0.10,
            f"Prevalencia difiere más de 10%: {rep['diferencia_relativa_prevalencia']}",
        )

        # Cada fold debe recibir positivos de forma homogénea (cíclica)
        conteos = list(rep["positivos_por_fold"].values())
        self.assertTrue(
            all(c > 0 for c in conteos), "Cada fold debe tener al menos un positivo"
        )
        self.assertLessEqual(
            max(conteos) - min(conteos),
            1,
            "La distribución cíclica debe diferir a lo sumo en 1",
        )

    def test_return_stats_preprocessing(self):
        """Verifica que curar_dataset retorne el diccionario de categorías raras cuando return_stats=True."""
        padron, ssco, pricos = self._crear_datos_sinteticos()
        _curado, stats = job06.curar_dataset(
            padron, ssco, pricos, semilla=42, return_stats=True
        )
        self.assertIn("top_ciiu", stats)
        self.assertIn("top_departamentos", stats)
        self.assertIsInstance(stats["top_ciiu"], list)
        self.assertIsInstance(stats["top_departamentos"], list)

    def test_top_k_desempate_deterministico(self):
        """Verifica que categorías con conteos empatados se desempaten de forma determinista y alfabética."""
        assert self.spark is not None
        rows = [
            (20100000001, "4659", "ZULIA", 1.0, 0, 100.0, 1, "ACTIVO", "HABIDO"),
            (20100000002, "4659", "AMAZONAS", 1.0, 0, 100.0, 1, "ACTIVO", "HABIDO"),
            (20100000003, "4659", "ANCASH", 1.0, 0, 100.0, 1, "ACTIVO", "HABIDO"),
            (20100000004, "4659", "CUSCO", 1.0, 0, 100.0, 1, "ACTIVO", "HABIDO"),
        ]
        schema = StructType(
            [
                StructField("RUC", LongType(), False),
                StructField("ciiu_principal", StringType(), True),
                StructField("departamento", StringType(), True),
                StructField("nro_trabajadores", DoubleType(), True),
                StructField("sin_trabajadores", IntegerType(), True),
                StructField("monto_total_soles", DoubleType(), True),
                StructField("n_ordenes", IntegerType(), True),
                StructField("Estado", StringType(), True),
                StructField("Condicion", StringType(), True),
            ]
        )
        padron = self.spark.createDataFrame(rows, schema)
        ssco = self.spark.createDataFrame(
            [(20100000001, True)],
            StructType(
                [
                    StructField("RUC", LongType(), False),
                    StructField("es_ssco", StringType(), True),
                ]
            ),
        )
        pricos = self.spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("RUC", LongType(), False),
                ]
            ),
        )

        _, stats = job06.curar_dataset(
            padron, ssco, pricos, semilla=42, return_stats=True, top_dept_limit=2
        )
        # AMAZONAS y ANCASH deben ser los elegidos porque tienen count=1 pero van primero lexicográficamente
        self.assertEqual(stats["top_departamentos"][:2], ["AMAZONAS", "ANCASH"])

    def test_tipo_contribuyente_imputacion_desconocido(self):
        """Verifica que valores nulos o vacíos en tipo_contribuyente sean imputados a DESCONOCIDO (Contrato C1)."""
        assert self.spark is not None
        rows = [
            (20100000001, "4659", "LIMA", None, 1.0, 0, 100.0, 1, "ACTIVO", "HABIDO"),
            (20100000002, "4659", "LIMA", "  ", 1.0, 0, 100.0, 1, "ACTIVO", "HABIDO"),
            (
                20100000003,
                "4659",
                "LIMA",
                "SOCIEDAD ANONIMA",
                1.0,
                0,
                100.0,
                1,
                "ACTIVO",
                "HABIDO",
            ),
        ]
        schema = StructType(
            [
                StructField("RUC", LongType(), False),
                StructField("ciiu_principal", StringType(), True),
                StructField("departamento", StringType(), True),
                StructField("tipo_contribuyente", StringType(), True),
                StructField("nro_trabajadores", DoubleType(), True),
                StructField("sin_trabajadores", IntegerType(), True),
                StructField("monto_total_soles", DoubleType(), True),
                StructField("n_ordenes", IntegerType(), True),
                StructField("Estado", StringType(), True),
                StructField("Condicion", StringType(), True),
            ]
        )
        padron = self.spark.createDataFrame(rows, schema)
        ssco = self.spark.createDataFrame(
            [], StructType([StructField("RUC", LongType(), False)])
        )
        pricos = self.spark.createDataFrame(
            [], StructType([StructField("RUC", LongType(), False)])
        )

        curado = job06.curar_dataset(padron, ssco, pricos, semilla=42)
        res = {r["ruc"]: r["tipo_contribuyente"] for r in curado.collect()}

        self.assertEqual(res["20100000001"], "DESCONOCIDO")
        self.assertEqual(res["20100000002"], "DESCONOCIDO")
        self.assertEqual(res["20100000003"], "SOCIEDAD ANONIMA")
