import importlib
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_PERF_SPARK = str(_ROOT / "perf" / "spark")
_COMMON = str(_ROOT / "src" / "spark" / "common")
_JOBS = str(_ROOT / "src" / "spark" / "jobs")

for p in [_PERF_SPARK, _COMMON, _JOBS]:
    if p not in sys.path:
        sys.path.insert(0, p)

import e2_ingest_bronze_persist

job01_orig = importlib.import_module("01_ingest_bronze")

HAS_JAVA = shutil.which("java") is not None


class TestPerfE2Equivalence(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        if HAS_JAVA:
            from pyspark.sql import SparkSession

            cls.spark = (
                SparkSession.builder.master("local[1]")
                .appName("test-perf-e2-equivalence")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate()
            )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()

    def test_e2_module_signatures(self):
        """Verifica que e2_ingest_bronze_persist contenga las mismas funciones que job 01."""
        for fn in ["ingest_padron_ruc", "ingest_ordenes_compra", "main"]:
            self.assertTrue(
                hasattr(e2_ingest_bronze_persist, fn),
                f"Falta función {fn} en e2_ingest_bronze_persist",
            )
            self.assertTrue(callable(getattr(e2_ingest_bronze_persist, fn)))

    @unittest.skipUnless(HAS_JAVA, "Requiere Java para SparkSession")
    def test_e2_equivalence_on_csv(self):
        """Verifica que la variante persist y el job original produzcan idéntico output."""
        from schema_definitions import PADRON_RUC_SCHEMA

        temp_dir = tempfile.mkdtemp()
        try:
            csv_path = f"{temp_dir}/raw_padron/anio=2024/mes=12/data.csv"
            Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

            # Escribir CSV mínimo con encabezado
            csv_content = (
                "RUC,Estado,Condicion,Tipo,Actividad_Economica_CIIU_revision3_Principal,"
                "Actividad_Economica_CIIU_revision3_Secundaria,Actividad_Economica_CIIU_revision4_Principal,"
                "NroTrab,TipoFacturacion,TipoContabilidad,ComercioExterior,UBIGEO,Departamento,Provincia,Distrito,PERIODO_PUBLICACION\n"
                "20100000001,ACTIVO,HABIDO,SOCIEDAD ANONIMA,6201,6202,6201,10,MANUAL,MANUAL,EXPORTADOR,150101,LIMA,LIMA,LIMA,202412\n"
                "20100000002,ACTIVO,HABIDO,EMPRESA INDIVIDUAL,6201,6202,6201,5,MANUAL,MANUAL,NINGUNO,150101,LIMA,LIMA,LIMA,202412\n"
            )
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write(csv_content)

            raw_base = f"{temp_dir}/raw_padron"

            # 1. Ejecución con Job 01 original
            df_orig = job01_orig._read_raw(self.spark, raw_base, PADRON_RUC_SCHEMA)
            clean_orig, quar_orig = job01_orig._split_schema_quarantine(df_orig)

            # 2. Ejecución con E2 variante persist
            df_e2 = e2_ingest_bronze_persist._read_raw(
                self.spark, raw_base, PADRON_RUC_SCHEMA
            )
            df_e2.persist()
            clean_e2, quar_e2 = e2_ingest_bronze_persist._split_schema_quarantine(df_e2)
            df_e2.unpersist()

            # Verificación de equivalencia
            self.assertEqual(clean_orig.count(), clean_e2.count())
            self.assertEqual(quar_orig.count(), quar_e2.count())

            orig_rucs = [r["RUC"] for r in clean_orig.collect()]
            e2_rucs = [r["RUC"] for r in clean_e2.collect()]
            self.assertCountEqual(orig_rucs, e2_rucs)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
