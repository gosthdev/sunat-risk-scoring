import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_PERF_SPARK = str(_ROOT / "perf" / "spark")
_COMMON = str(_ROOT / "src" / "spark" / "common")

for p in [_PERF_SPARK, _COMMON]:
    if p not in sys.path:
        sys.path.insert(0, p)

import e1_make_layouts

HAS_JAVA = shutil.which("java") is not None


class TestPerfE1Layouts(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        if HAS_JAVA:
            from pyspark.sql import SparkSession

            cls.spark = (
                SparkSession.builder.master("local[1]")
                .appName("test-perf-e1-layouts")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate()
            )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def test_e1_module_attributes(self):
        """Verifica que el módulo e1_make_layouts esté estructurado correctamente."""
        self.assertTrue(hasattr(e1_make_layouts, "make_layouts"))
        self.assertTrue(callable(e1_make_layouts.make_layouts))
        self.assertTrue(hasattr(e1_make_layouts, "main"))

    @unittest.skipUnless(HAS_JAVA, "Requiere Java para SparkSession")
    def test_make_layouts_partitioning_and_equality(self):
        """Verifica que P esté particionado, N sea plano y ambos tengan conteos idénticos."""
        from pyspark.sql.types import (
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("RUC", LongType(), False),
                StructField("Estado", StringType(), True),
                StructField("Departamento", StringType(), True),
                StructField("anio", IntegerType(), False),
                StructField("mes", IntegerType(), False),
            ]
        )

        # Generar datos para 12 meses
        data = []
        for mes in range(1, 13):
            for i in range(10):
                ruc = 20100000000 + i * 100 + mes
                data.append((ruc, "ACTIVO", "LIMA", 2024, mes))

        temp_dir = tempfile.mkdtemp()
        try:
            staging_path = f"{temp_dir}/staging"
            output_p = f"{temp_dir}/layout_p"
            output_n = f"{temp_dir}/layout_n"

            df = self.spark.createDataFrame(data, schema)
            df.write.parquet(staging_path)

            e1_make_layouts.make_layouts(
                spark=self.spark,
                staging_path=staging_path,
                output_p=output_p,
                output_n=output_n,
                num_files_n=4,
            )

            df_p = self.spark.read.parquet(output_p)
            df_n = self.spark.read.parquet(output_n)

            # Conteos idénticos
            self.assertEqual(df_p.count(), len(data))
            self.assertEqual(df_n.count(), len(data))

            # P debe tener directorios Hive
            p_dirs = list(Path(output_p).glob("anio=*/mes=*"))
            self.assertTrue(
                len(p_dirs) > 0, "Layout P no generó directorios Hive anio=/mes="
            )

            # N no debe tener directorios anio=/mes=
            n_dirs = list(Path(output_n).glob("anio=*"))
            self.assertEqual(
                len(n_dirs), 0, "Layout N contiene directorios Hive particionados"
            )

            # Cada archivo Parquet en N contiene múltiples meses
            for n_parquet in Path(output_n).glob("*.parquet"):
                single_df = self.spark.read.parquet(str(n_parquet))
                months_in_file = single_df.select("mes").distinct().count()
                self.assertGreater(
                    months_in_file, 1, "Archivo en Layout N solo contiene 1 mes"
                )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
