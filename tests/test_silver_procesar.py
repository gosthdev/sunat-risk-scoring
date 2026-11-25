"""silver_transforms.procesar: periodos parciales y reporte de calidad completo en staging."""

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.skipif(shutil.which("java") is None, reason="requiere Java")


class TestProcesar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]").appName("t-procesar").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_incremental_escribe_solo_el_periodo_y_conserva_el_reporte_previo(self):
        import silver_transforms as st
        from fuentes.silver import padron_ruc as pr
        from pyspark.sql.types import StructType
        from schema_definitions import PADRON_RUC_SCHEMA

        with tempfile.TemporaryDirectory() as tmp:
            bronze, silver, rep = (f"{tmp}/{n}" for n in ("bronze", "silver", "rep"))

            def fila(a, m):
                valores = {"LongType": 20100000001, "StringType": "x"}
                base = [
                    valores.get(type(f.dataType).__name__) for f in PADRON_RUC_SCHEMA
                ]
                return (*base, a, m)

            esquema = StructType(list(PADRON_RUC_SCHEMA.fields))
            esquema.add("anio", "integer").add("mes", "integer")
            filas = [fila(2025, 1), fila(2025, 2)]
            self.spark.createDataFrame(filas, esquema).write.partitionBy(
                "anio", "mes"
            ).parquet(bronze)
            # Reporte vivo previo: enero ya procesado, y otra fuente.
            previo = self.spark.createDataFrame(
                [("ordenes_compra", 2025, 1, 3, 0, 3, 0, 3, 0.0, 0.0)],
                "fuente string, anio int, mes int, filas_bronze long, "
                "filas_cuarentena_bronze long, filas_entrada long, "
                "filas_cuarentena_ruc long, filas_silver long, "
                "tasa_cuarentena_bronze double, tasa_cuarentena_ruc double",
            )
            previo.write.partitionBy("fuente").parquet(rep)

            self.enterContext(patch.object(st, "staging_de", lambda p: p + "_stg"))
            self.enterContext(patch.object(st, "SILVER_QUALITY_REPORT", rep))
            pr.procesar(
                self.spark,
                "padron_ruc",
                bronze,
                bronze + "_q",
                pr.preparar_padron_ruc,
                pr.separar_padron_ruc,
                silver,
                silver + "_q",
                [(2025, 2)],
            )
            salida = self.spark.read.parquet(silver + "_stg")
            self.assertEqual(
                [(r["anio"], r["mes"]) for r in salida.collect()], [(2025, 2)]
            )
            reporte = self.spark.read.parquet(rep + "_stg")
            self.assertEqual(
                sorted((r["fuente"], r["anio"], r["mes"]) for r in reporte.collect()),
                [("ordenes_compra", 2025, 1), ("padron_ruc", 2025, 2)],
            )
            self.assertTrue(Path(rep + "_stg").exists())
