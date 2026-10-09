import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent.parent
_PERF_SPARK = str(_ROOT / "perf" / "spark")
_COMMON = str(_ROOT / "src" / "spark" / "common")
_JOBS = str(_ROOT / "src" / "spark" / "jobs")

for p in [_PERF_SPARK, _COMMON, _JOBS]:
    if p not in sys.path:
        sys.path.insert(0, p)

import perf_runner


class TestPerfRunner(unittest.TestCase):
    def test_import_all_five_jobs(self):
        """Verifica que los 5 jobs de producción se importen sin error bajo el wrapper."""
        for job_name in perf_runner.VALID_JOBS:
            with self.subTest(job=job_name):
                mod = perf_runner.import_job(job_name)
                self.assertIsNotNone(mod)
                self.assertTrue(
                    hasattr(mod, "main"), f"Job {job_name} no tiene función main"
                )
                self.assertTrue(
                    callable(mod.main),
                    f"Job {job_name}.main no es callable",
                )

    def test_dry_run_all_five_jobs(self):
        """Verifica que la ejecución dry-run sea exitosa para los 5 jobs."""
        for job_name in perf_runner.VALID_JOBS:
            with self.subTest(job=job_name):
                ret = perf_runner.run_job(job_name, dry_run=True)
                self.assertEqual(ret, 0)

    def test_patch_spark_factory(self):
        """Verifica que el parche de spark_session_factory configure el shuffle adecuadamente."""
        import spark_session_factory

        # Mockear SparkSession en pyspark.sql
        with patch("pyspark.sql.SparkSession") as mock_spark:
            builder = MagicMock()
            mock_spark.builder.appName.return_value = builder
            builder.config.return_value = builder
            builder.getOrCreate.return_value = MagicMock()

            perf_runner.patch_spark_factory(shuffle_partitions=128)
            session = spark_session_factory.create_spark_session("test_app")

            self.assertIsNotNone(session)
            applied_configs = dict(call.args for call in builder.config.call_args_list)
            self.assertEqual(applied_configs.get("spark.sql.shuffle.partitions"), "128")

    def test_guard_protected_paths_bash(self):
        """Verifica que guard_protected_paths.sh bloquee rutas reales y permita rutas perf."""
        guard_script = _ROOT / "perf" / "scripts" / "guard_protected_paths.sh"
        self.assertTrue(guard_script.exists())

        # Casos protegidos (deben retornar código de salida 1)
        protected_cases = [
            "s3://sunat-risk-scoring-raw/padron_ruc/",
            "s3://sunat-risk-scoring-bronze/",
            "s3://sunat-risk-scoring-silver/padron_ruc",
            "s3://sunat-risk-scoring-gold/scoring_dataset",
            "sunat-risk-scoring-bronze",
        ]
        for path in protected_cases:
            with self.subTest(path=path):
                proc = subprocess.run(
                    ["bash", str(guard_script), "--check", path],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    proc.returncode,
                    1,
                    f"Ruta protegida no fue bloqueada: {path}",
                )

        # Casos permitidos (deben retornar código de salida 0)
        allowed_cases = [
            "s3://sunat-risk-scoring-perf-36p/silver/",
            "s3://sunat-risk-scoring-perf-36n/staging/",
            "s3://sunat-risk-scoring-perf-e2/bronze/",
            "s3://sunat-risk-scoring-perf-x100/data/",
        ]
        for path in allowed_cases:
            with self.subTest(path=path):
                proc = subprocess.run(
                    ["bash", str(guard_script), "--check", path],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    proc.returncode,
                    0,
                    f"Ruta permitida fue bloqueada erróneamente: {path}",
                )
