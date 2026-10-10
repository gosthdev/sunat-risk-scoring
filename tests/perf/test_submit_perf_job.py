import subprocess
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_SUBMIT_SCRIPT = _ROOT / "perf" / "scripts" / "submit_perf_job.sh"


class TestSubmitPerfJob(unittest.TestCase):
    def setUp(self):
        self.assertTrue(
            _SUBMIT_SCRIPT.exists(),
            f"Script no existe en {_SUBMIT_SCRIPT}",
        )

    def run_submit_dry_run(self, extra_args: list[str]) -> subprocess.CompletedProcess:
        cmd = ["bash", str(_SUBMIT_SCRIPT), "--dry-run"] + extra_args
        return subprocess.run(
            cmd,
            cwd=str(_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

    def test_default_c3_profile_includes_100g_disk_and_s3a_bytebuffer(self):
        """Verifica que el perfil C3 por defecto configure 100g de disco y S3A bytebuffer."""
        res = self.run_submit_dry_run(["--profile", "C3"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=100g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload=true", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.multipart.size=64M", res.stdout)

    def test_executor_disk_cli_override(self):
        """Verifica que --executor-disk sobrescriba el valor del perfil."""
        res = self.run_submit_dry_run(["--profile", "C3", "--executor-disk", "150g"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=150g", res.stdout)
        self.assertNotIn("spark.emr-serverless.executor.disk=100g", res.stdout)

    def test_s3a_buffer_and_multipart_override(self):
        """Verifica que --s3a-buffer y --s3a-multipart-size sobrescriban los valores S3A."""
        res = self.run_submit_dry_run(
            [
                "--profile",
                "C3",
                "--s3a-buffer",
                "array",
                "--s3a-multipart-size",
                "128M",
            ]
        )
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=array", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.multipart.size=128M", res.stdout)

    def test_extra_conf_flags(self):
        """Verifica que --conf inyecte configuraciones Spark adicionales."""
        res = self.run_submit_dry_run(
            [
                "--profile",
                "C3",
                "--conf",
                "spark.sql.shuffle.partitions=128",
                "--conf",
                "spark.executor.extraJavaOptions=-XX:+UseG1GC",
            ]
        )
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.sql.shuffle.partitions=128", res.stdout)
        self.assertIn("spark.executor.extraJavaOptions=-XX:+UseG1GC", res.stdout)

    def test_e1_make_layouts_args_formatting(self):
        """Verifica la invocación típica de e1-make-layouts con --executor-disk 100g y job-args."""
        res = self.run_submit_dry_run(
            [
                "--profile",
                "C3",
                "--executor-disk",
                "100g",
                "--script",
                "e1_make_layouts.py",
                "--job-args",
                "--staging-input s3://sunat-risk-scoring-perf-36p/staging/padron_ruc --output-p s3://sunat-risk-scoring-perf-36p/silver/padron_ruc --output-n s3://sunat-risk-scoring-perf-36n/silver/padron_ruc",
                "--run-name",
                "e1-make-layouts",
                "--condition",
                "setup",
                "--exp-tag",
                "E1",
            ]
        )
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=100g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)
        self.assertIn("e1_make_layouts.py", res.stdout)
        self.assertIn("Condición       : setup", res.stdout)
        self.assertIn("s3://sunat-risk-scoring-perf-36p/staging/padron_ruc", res.stdout)
        self.assertIn("s3://sunat-risk-scoring-perf-36p/silver/padron_ruc", res.stdout)
        self.assertIn("s3://sunat-risk-scoring-perf-36n/silver/padron_ruc", res.stdout)

    def test_profile_c1_loads_200g_disk_and_bytebuffer(self):
        """Verifica que el perfil C1 cargue 200g de disco y S3A bytebuffer."""
        res = self.run_submit_dry_run(["--profile", "C1"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=200g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)
        self.assertIn("spark.executor.instances=1", res.stdout)

    def test_profile_c7_loads_200g_disk_and_bytebuffer(self):
        """Verifica que el perfil C7 cargue 200g de disco y S3A bytebuffer."""
        res = self.run_submit_dry_run(["--profile", "C7"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=200g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)
        self.assertIn("spark.executor.instances=7", res.stdout)

    def test_profile_e2_resolves_referenced_profile_and_defaults(self):
        """Verifica que un perfil de experimento (E2) resuelva el perfil C3 referenciado."""
        res = self.run_submit_dry_run(["--profile", "e2"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=100g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)
        self.assertIn("spark.executor.instances=3", res.stdout)

    def test_profile_e1_n_uses_hardware_defaults_without_unbound_variable(self):
        """Verifica que un perfil sin definición de hardware (e1_n) no falle y use defaults."""
        res = self.run_submit_dry_run(["--profile", "e1_n"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("spark.emr-serverless.executor.disk=100g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)

    def test_nonexistent_profile_falls_back_to_defaults_safely(self):
        """Verifica que un perfil no existente emita advertencia y aplique defaults C3."""
        res = self.run_submit_dry_run(["--profile", "perfil_inexistente"])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")
        self.assertIn("WARN: No se encontró archivo de perfil", res.stdout)
        self.assertIn("spark.emr-serverless.executor.disk=100g", res.stdout)
        self.assertIn("spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer", res.stdout)


if __name__ == "__main__":
    unittest.main()
