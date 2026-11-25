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
        cls.inc = importlib.import_module("bronze_io")
        cls.ordenes = importlib.import_module("fuentes.bronze.ordenes_compra")

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

            filas = self.inc.leer_raw(self.spark, ok, esquema, None).collect()
            self.assertEqual(filas[0]["RUC"], 20100000001)
            with self.assertRaises(Py4JJavaError):
                self.inc.leer_raw(self.spark, malo, esquema, None).collect()

        # leer_raw no debe mutar el esquema que recibe (StructType.add muta).
        self.assertEqual(esquema.fieldNames(), ["RUC", "Estado"])

    def _particion(self, base, anio, mes, filas):
        carpeta = Path(base) / f"anio={anio}" / f"mes={mes:02d}"
        carpeta.mkdir(parents=True)
        cuerpo = "".join(f"{r},{e}\n" for r, e in filas)
        (carpeta / "datos.csv").write_text(f"RUC,Estado\n{cuerpo}")

    def test_reingerir_un_periodo_limpia_su_cuarentena_y_conserva_la_otra(self):
        esquema = StructType(
            [StructField("RUC", LongType()), StructField("Estado", StringType())]
        )
        with tempfile.TemporaryDirectory() as tmp:
            raw, clean, cuar = (f"{tmp}/{n}" for n in ("raw", "clean", "cuar"))
            self._particion(raw, 2025, 1, [("20100000001", "ok"), ("xx", "malo")])
            self._particion(raw, 2025, 2, [("20100000002", "ok"), ("yy", "malo")])

            def ingerir(periodos):
                df = self.inc.leer_raw(self.spark, raw, esquema, periodos)
                c, q = self.inc.separar_esquema(df)
                self.inc.escribir_bronze(self.spark, "x", c, q, clean, cuar, periodos)

            ingerir(None)
            self.assertTrue((Path(cuar) / "anio=2025" / "mes=1").exists())
            self.assertTrue((Path(cuar) / "anio=2025" / "mes=2").exists())
            mtime_feb = (Path(cuar) / "anio=2025" / "mes=2").stat().st_mtime_ns

            # Enero se corrige en raw: ya no hay filas malas.
            self._particion(raw + "_nuevo", 2025, 1, [("20100000001", "ok")])
            shutil.rmtree(f"{raw}/anio=2025/mes=01")
            shutil.copytree(f"{raw}_nuevo/anio=2025/mes=01", f"{raw}/anio=2025/mes=01")
            ingerir([(2025, 1)])

            self.assertFalse((Path(cuar) / "anio=2025" / "mes=1").exists())
            self.assertEqual(
                (Path(cuar) / "anio=2025" / "mes=2").stat().st_mtime_ns, mtime_feb
            )
            q = self.spark.read.parquet(cuar)
            self.assertEqual([(r["anio"], r["mes"]) for r in q.collect()], [(2025, 2)])
            c = self.spark.read.parquet(clean)
            self.assertEqual(
                sorted((r["anio"], r["mes"], r["RUC"]) for r in c.collect()),
                [(2025, 1, 20100000001), (2025, 2, 20100000002)],
            )

    def test_full_quita_el_mes_que_ya_no_tiene_filas_limpias(self):
        esquema = StructType(
            [StructField("RUC", LongType()), StructField("Estado", StringType())]
        )
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        with tempfile.TemporaryDirectory() as tmp:
            raw, clean, cuar = (f"{tmp}/{n}" for n in ("raw", "clean", "cuar"))
            self._particion(raw, 2025, 1, [("20100000001", "ok")])
            self._particion(raw, 2025, 2, [("20100000002", "ok")])

            def full():
                df = self.inc.leer_raw(self.spark, raw, esquema, None)
                c, q = self.inc.separar_esquema(df)
                self.inc.escribir_bronze(self.spark, "x", c, q, clean, cuar, None)

            full()
            # Febrero queda 100% en cuarentena.
            shutil.rmtree(f"{raw}/anio=2025/mes=02")
            self._particion(raw, 2025, 2, [("zz", "malo")])
            full()
            meses = sorted(r["mes"] for r in self.spark.read.parquet(clean).collect())
            self.assertEqual(meses, [1])

    def test_cuarentena_vieja_sin_particionar_se_migra_en_full(self):
        esquema = StructType(
            [StructField("RUC", LongType()), StructField("Estado", StringType())]
        )
        with tempfile.TemporaryDirectory() as tmp:
            raw, clean, cuar = (f"{tmp}/{n}" for n in ("raw", "clean", "cuar"))
            self._particion(raw, 2025, 1, [("20100000001", "ok")])
            Path(cuar).mkdir()
            (Path(cuar) / "part-0000.parquet").write_text("layout viejo")
            df = self.inc.leer_raw(self.spark, raw, esquema, None)
            c, q = self.inc.separar_esquema(df)
            self.inc.escribir_bronze(self.spark, "x", c, q, clean, cuar, None)
            self.assertFalse((Path(cuar) / "part-0000.parquet").exists())

    def test_fecha_nula_no_se_pierde_y_anio_fuera_de_rango_va_a_cuarentena(self):
        df = self.spark.createDataFrame(
            [
                ("ok", "2025-01-10", "2025-01-11"),
                ("fecha_nula", "2025-01-10", None),
                ("anio_malo", "8202-01-10", "2025-01-11"),
            ],
            "id string, f1 string, f2 string",
        ).selectExpr("id", "to_date(f1) f1", "to_date(f2) f2")

        clean, cuarentena = self.ordenes.separar_rango_fechas(df, ["f1", "f2"])

        self.assertEqual(sorted(r["id"] for r in clean.collect()), ["fecha_nula", "ok"])
        self.assertEqual([r["id"] for r in cuarentena.collect()], ["anio_malo"])
