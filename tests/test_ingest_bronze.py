import shutil
import sys
import unittest
from datetime import date
from pathlib import Path

# Asegurar módulos en sys.path
_ROOT = Path(__file__).resolve().parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
_JOBS_PATH = str(_ROOT / "src" / "spark" / "jobs")
if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)
if _JOBS_PATH not in sys.path:
    sys.path.insert(0, _JOBS_PATH)

import importlib

from schema_definitions import (
    ORDENES_COMPRA_DATE_COLUMNS,
    ORDENES_COMPRA_SCHEMA,
    PADRON_RUC_SCHEMA,
)

ingest_bronze = importlib.import_module("01_ingest_bronze")

HAS_JAVA = shutil.which("java") is not None


class TestIngestBronze(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        if HAS_JAVA:
            from pyspark.sql import SparkSession

            cls.spark = (
                SparkSession.builder.master("local[1]")
                .appName("test-ingest-bronze")
                .getOrCreate()
            )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def test_schema_immutability(self):
        """Verifica que _schema_with_corrupt no mute los esquemas globales."""
        padron_fields_before = len(PADRON_RUC_SCHEMA.fields)
        ordenes_fields_before = len(ORDENES_COMPRA_SCHEMA.fields)

        self.assertNotIn(ingest_bronze.CORRUPT_COLUMN, PADRON_RUC_SCHEMA.fieldNames())
        self.assertNotIn(
            ingest_bronze.CORRUPT_COLUMN, ORDENES_COMPRA_SCHEMA.fieldNames()
        )

        # Generar copias con columna corrupta
        s1 = ingest_bronze._schema_with_corrupt(PADRON_RUC_SCHEMA)
        s2 = ingest_bronze._schema_with_corrupt(ORDENES_COMPRA_SCHEMA)

        # Los esquemas globales deben mantener su conteo original
        self.assertEqual(len(PADRON_RUC_SCHEMA.fields), padron_fields_before)
        self.assertEqual(len(ORDENES_COMPRA_SCHEMA.fields), ordenes_fields_before)
        self.assertNotIn(ingest_bronze.CORRUPT_COLUMN, PADRON_RUC_SCHEMA.fieldNames())
        self.assertNotIn(
            ingest_bronze.CORRUPT_COLUMN, ORDENES_COMPRA_SCHEMA.fieldNames()
        )

        # Los nuevos esquemas sí deben tener la columna adicional
        self.assertIn(ingest_bronze.CORRUPT_COLUMN, s1.fieldNames())
        self.assertIn(ingest_bronze.CORRUPT_COLUMN, s2.fieldNames())
        self.assertEqual(len(s1.fields), padron_fields_before + 1)
        self.assertEqual(len(s2.fields), ordenes_fields_before + 1)

        # Llamar idempotentemente sobre un esquema que ya la tiene
        s3 = ingest_bronze._schema_with_corrupt(s1)
        self.assertEqual(len(s3.fields), len(s1.fields))

    @unittest.skipUnless(HAS_JAVA, "Requiere Java para ejecutar SparkSession")
    def test_null_date_preservation(self):
        """Verifica que las filas con fechas nulas no se pierdan en _split_date_range_quarantine."""
        from pyspark.sql.types import DateType, IntegerType, StructField, StructType

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("fecha_registro", DateType(), True),
                StructField("fecha_de_emision", DateType(), True),
                StructField("fecha_compromiso_presupuestal", DateType(), True),
                StructField("fecha_de_notificacion", DateType(), True),
            ]
        )

        # 5 filas de prueba:
        # 1: Fechas válidas en todas
        # 2: Alguna fecha nula, pero el resto válida (NO debe perderse ni ir a cuarentena)
        # 3: Todas las fechas nulas (NO debe perderse ni ir a cuarentena)
        # 4: Fecha futura inválida (año 2202) -> cuarentena
        # 5: Fecha pasada inválida (año 1990) -> cuarentena
        data = [
            (
                1,
                date(2024, 1, 15),
                date(2024, 1, 16),
                date(2024, 1, 17),
                date(2024, 1, 18),
            ),
            (2, date(2024, 5, 10), None, date(2024, 5, 12), None),
            (3, None, None, None, None),
            (4, date(2202, 3, 1), date(2024, 1, 1), None, None),
            (5, date(1990, 1, 1), None, None, None),
        ]

        df = self.spark.createDataFrame(data, schema)
        clean, quarantined = ingest_bronze._split_date_range_quarantine(
            df, ORDENES_COMPRA_DATE_COLUMNS
        )

        clean_ids = [row["id"] for row in clean.collect()]
        quarantined_ids = [row["id"] for row in quarantined.collect()]

        self.assertCountEqual(clean_ids, [1, 2, 3])
        self.assertCountEqual(quarantined_ids, [4, 5])
        self.assertEqual(clean.count() + quarantined.count(), df.count())
