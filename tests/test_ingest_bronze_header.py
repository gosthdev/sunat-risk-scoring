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

    def test_cuarentena_vacia_reemplaza_la_de_la_corrida_anterior(self):
        con_filas = self.spark.createDataFrame([(1, "malo")], "id int, motivo string")
        with tempfile.TemporaryDirectory() as tmp:
            ruta = str(Path(tmp) / "cuarentena")
            self.bronze._write_quarantine(con_filas, ruta)
            self.assertEqual(self.spark.read.parquet(ruta).count(), 1)

            self.bronze._write_quarantine(con_filas.filter("id < 0"), ruta)
            self.assertEqual(self.spark.read.parquet(ruta).count(), 0)

    def test_fecha_nula_no_se_pierde_y_anio_fuera_de_rango_va_a_cuarentena(self):
        df = self.spark.createDataFrame(
            [
                ("ok", "2025-01-10", "2025-01-11"),
                ("fecha_nula", "2025-01-10", None),
                ("anio_malo", "8202-01-10", "2025-01-11"),
            ],
            "id string, f1 string, f2 string",
        ).selectExpr("id", "to_date(f1) f1", "to_date(f2) f2")

        clean, cuarentena = self.bronze._split_date_range_quarantine(df, ["f1", "f2"])

        self.assertEqual(sorted(r["id"] for r in clean.collect()), ["fecha_nula", "ok"])
        self.assertEqual([r["id"] for r in cuarentena.collect()], ["anio_malo"])
