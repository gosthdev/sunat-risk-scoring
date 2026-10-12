"""01_ingest_bronze falla si la cabecera del CSV no calza con el esquema."""

import importlib
import shutil
import tempfile
import unittest
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql.types import LongType, StringType, StructField, StructType

HAS_JAVA = shutil.which("java") is not None


@unittest.skipUnless(HAS_JAVA, "requiere Java para PySpark")
class TestCabeceraCsvDeBronze(unittest.TestCase):
    """01_ingest_bronze falla si la cabecera del CSV no calza con el esquema."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]").appName("test-hdr").getOrCreate()
        )
        cls.bronze = importlib.import_module("01_ingest_bronze")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _csv(self, tmp_path: Path, cabecera: str) -> str:
        carpeta = tmp_path / cabecera.replace(",", "_")
        carpeta.mkdir()
        (carpeta / "datos.csv").write_text(f"{cabecera}\n20100000001,ACTIVO\n")
        return str(carpeta)

    def test_cabecera_renombrada_hace_fallar_la_lectura(self):

        esquema = StructType(
            [StructField("RUC", LongType()), StructField("Estado", StringType())]
        )
        with tempfile.TemporaryDirectory() as tmp:
            ok = self._csv(Path(tmp), "RUC,Estado")
            malo = self._csv(Path(tmp), "RUC,Estado_nuevo")

            filas = self.bronze._read_raw(self.spark, ok, esquema).collect()
            self.assertEqual(filas[0]["RUC"], 20100000001)
            with self.assertRaises(Py4JJavaError):
                self.bronze._read_raw(self.spark, malo, esquema).collect()

        # _read_raw no debe mutar el esquema que recibe (StructType.add muta).
        self.assertEqual(esquema.fieldNames(), ["RUC", "Estado"])
