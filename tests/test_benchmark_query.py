import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT / "src" / "spark" / "common", _ROOT / "src" / "spark" / "jobs"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

HAS_JAVA = shutil.which("java") is not None


@unittest.skipUnless(HAS_JAVA, "Java no disponible: se omiten tests de Spark")
class TestBenchmarkQuery(unittest.TestCase):
    spark = None
    root = ""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-benchmark-query")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.root = tempfile.mkdtemp(prefix="bench_")

        data = [
            ("LIMA", 100.0, 2025, 1),
            ("LIMA", 50.0, 2025, 1),
            ("CUSCO", 30.0, 2025, 1),
            ("LIMA", 70.0, 2025, 2),
            ("PIURA", 10.0, 2025, 3),
        ]
        df = cls.spark.createDataFrame(
            data,
            "departamento_entidad string, monto_total_orden_original double, "
            "anio int, mes int",
        )
        table = "ordenes_compra"
        df.write.mode("overwrite").parquet(f"{cls.root}/unpartitioned/{table}")
        df.write.mode("overwrite").partitionBy("anio", "mes").parquet(
            f"{cls.root}/partitioned/{table}"
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()
        shutil.rmtree(cls.root, ignore_errors=True)

    def test_build_path(self):
        from benchmark_query import build_path

        self.assertEqual(
            build_path("s3://b/benchmark/r1/", "partitioned", "ordenes_compra"),
            "s3://b/benchmark/r1/partitioned/ordenes_compra",
        )

    def test_find_latest_period(self):
        from benchmark_query import find_latest_period

        path = f"{self.root}/partitioned/ordenes_compra"
        self.assertEqual(find_latest_period(self.spark, path), (2025, 3))

    def test_both_layouts_return_same_rows(self):
        from benchmark_query import run_benchmark

        result = run_benchmark(
            self.spark,
            self.root,
            "ordenes_compra",
            anio=2025,
            mes=1,
            repetitions=2,
            warmup_runs=0,
        )
        self.assertEqual(result["rows"], 3)
        self.assertEqual(len(result["times"]["partitioned"]), 2)
        self.assertEqual(len(result["times"]["unpartitioned"]), 2)
        self.assertGreater(result["speedup"], 0)


if __name__ == "__main__":
    unittest.main()