import os
import shutil
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from region_normalizer import normalize_department

HAS_JAVA = shutil.which("java") is not None


class TestGoldJobs(unittest.TestCase):
    def test_normalize_department(self):
        self.assertEqual(normalize_department("LIMA"), "LIMA")
        self.assertEqual(normalize_department("  arequipa "), "AREQUIPA")

    def test_antiguedad_reference_date_logic(self):
        """Verifica que el cálculo de antigüedad mensual sea determinista."""
        first_order = date(2025, 1, 15)

        # Enero 2025: corte 2025-01-31
        cutoff_jan = date(2025, 1, 31)
        antiguedad_jan = (cutoff_jan - first_order).days
        self.assertEqual(antiguedad_jan, 16)

        # Febrero 2025: corte 2025-02-28
        cutoff_feb = date(2025, 2, 28)
        antiguedad_feb = (cutoff_feb - first_order).days
        self.assertEqual(antiguedad_feb, 44)

        # Diciembre 2025: corte 2025-12-31
        cutoff_dec = date(2025, 12, 31)
        antiguedad_dec = (cutoff_dec - first_order).days
        self.assertEqual(antiguedad_dec, 350)

        # Orden posterior al mes de referencia -> debe ser None
        first_order_future = date(2025, 6, 1)
        diff = (cutoff_jan - first_order_future).days
        antiguedad = None if diff < 0 else diff
        self.assertIsNone(antiguedad)

    @unittest.skipIf(not HAS_JAVA, "Requiere Java para PySpark functions")
    def test_obtener_padron_mes_referencia_with_env_vars(self):
        """Verifica que obtener_padron_mes_referencia respete las variables de entorno."""
        from importlib import import_module

        regional_mod = import_module("04_regional_gold")

        mock_spark = MagicMock()
        mock_padron = MagicMock()

        with patch.dict(
            os.environ,
            {
                "ANIO_REFERENCIA_REGIONAL": "2025",
                "MES_REFERENCIA_REGIONAL": "12",
            },
        ):
            _padron_filtrado, anio_ref, mes_ref = (
                regional_mod.obtener_padron_mes_referencia(
                    spark=mock_spark, padron=mock_padron
                )
            )
            self.assertEqual(anio_ref, 2025)
            self.assertEqual(mes_ref, 12)
            self.assertTrue(mock_padron.filter.called)

    @unittest.skipIf(not HAS_JAVA, "Requiere Java para PySpark functions")
    def test_obtener_padron_mes_referencia_auto_detect_latest(self):
        """Verifica que se seleccione el último snapshot cuando no hay variables de entorno."""
        from importlib import import_module

        regional_mod = import_module("04_regional_gold")

        mock_spark = MagicMock()
        mock_padron = MagicMock()
        mock_select = MagicMock()
        mock_padron.select.return_value = mock_select
        mock_select.distinct.return_value = mock_select
        mock_select.orderBy.return_value = mock_select
        mock_select.first.return_value = {"anio": 2025, "mes": 12}

        with patch.dict(os.environ, {}, clear=True):
            _padron_filtrado, anio_ref, mes_ref = (
                regional_mod.obtener_padron_mes_referencia(
                    spark=mock_spark, padron=mock_padron
                )
            )
            self.assertEqual(anio_ref, 2025)
            self.assertEqual(mes_ref, 12)
            self.assertTrue(mock_padron.filter.called)

    @unittest.skipIf(not HAS_JAVA, "Requiere Java para Spark local")
    def test_pyspark_integration(self):
        """Test de integración cuando Java está disponible (e.g. en GitHub Actions)."""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import (
            col,
            concat,
            datediff,
            format_string,
            last_day,
            lit,
            to_date,
        )

        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([(2025, 1), (2025, 2)], ["anio", "mes"])
            df = df.withColumn(
                "mes_referencia",
                format_string("%d%02d", col("anio"), col("mes")),
            )
            df = df.withColumn(
                "fecha_ref",
                last_day(to_date(concat(col("mes_referencia"), lit("01")), "yyyyMMdd")),
            )
            df = df.withColumn(
                "primera_orden", to_date(lit("2025-01-15"), "yyyy-MM-dd")
            )
            df = df.withColumn(
                "antiguedad", datediff(col("fecha_ref"), col("primera_orden"))
            )

            rows = {r["mes_referencia"]: r["antiguedad"] for r in df.collect()}
            self.assertEqual(rows["202501"], 16)
            self.assertEqual(rows["202502"], 44)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
