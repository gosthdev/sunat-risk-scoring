import importlib
import os
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)


class TestS3Paths(unittest.TestCase):
    def setUp(self):
        # Limpiar variables de entorno relevantes
        for var in [
            "DATALAKE_BUCKET",
            "RAW_BUCKET",
            "BRONZE_BUCKET",
            "SILVER_BUCKET",
            "GOLD_BUCKET",
            "S3_USE_LAYER_PREFIX",
        ]:
            if var in os.environ:
                del os.environ[var]

    def reload_s3_paths(self):
        import s3_paths

        return importlib.reload(s3_paths)

    def test_production_multi_bucket_paths(self):
        """En producción multicanasta, los buckets dedicados (*-silver, *-gold) no repiten el prefijo."""
        os.environ["RAW_BUCKET"] = "sunat-risk-scoring-raw"
        os.environ["BRONZE_BUCKET"] = "sunat-risk-scoring-bronze"
        os.environ["SILVER_BUCKET"] = "sunat-risk-scoring-silver"
        os.environ["GOLD_BUCKET"] = "sunat-risk-scoring-gold"

        s3 = self.reload_s3_paths()
        self.assertEqual(s3.RAW_PADRON_RUC, "s3://sunat-risk-scoring-raw/padron_ruc")
        self.assertEqual(
            s3.BRONZE_PADRON_RUC, "s3://sunat-risk-scoring-bronze/padron_ruc"
        )
        self.assertEqual(
            s3.SILVER_PADRON_RUC, "s3://sunat-risk-scoring-silver/padron_ruc"
        )
        self.assertEqual(s3.SILVER_EPEN, "s3://sunat-risk-scoring-silver/epen")
        self.assertEqual(
            s3.GOLD_REGIONAL_SUMMARY, "s3://sunat-risk-scoring-gold/regional_summary"
        )

    def test_single_datalake_bucket_paths(self):
        """En datalake compartido (mismo bucket para todo), se incluye el prefijo de capa."""
        os.environ["DATALAKE_BUCKET"] = "sunat-risk-scoring"
        os.environ["RAW_BUCKET"] = "sunat-risk-scoring"
        os.environ["BRONZE_BUCKET"] = "sunat-risk-scoring"
        os.environ["SILVER_BUCKET"] = "sunat-risk-scoring"
        os.environ["GOLD_BUCKET"] = "sunat-risk-scoring"

        s3 = self.reload_s3_paths()
        self.assertEqual(
            s3.RAW_PADRON_RUC, "s3://sunat-risk-scoring/raw/padron_ruc"
        )
        self.assertEqual(
            s3.BRONZE_PADRON_RUC, "s3://sunat-risk-scoring/bronze/padron_ruc"
        )
        self.assertEqual(
            s3.SILVER_PADRON_RUC, "s3://sunat-risk-scoring/silver/padron_ruc"
        )
        self.assertEqual(
            s3.GOLD_REGIONAL_SUMMARY, "s3://sunat-risk-scoring/gold/regional_summary"
        )

    def test_perf_36n_layout_paths(self):
        """Para bucket de benchmark perf-36n, silver/ se incluye en la ruta mientras raw se lee directo."""
        os.environ["RAW_BUCKET"] = "sunat-risk-scoring-raw"
        os.environ["SILVER_BUCKET"] = "sunat-risk-scoring-perf-36n"
        os.environ["GOLD_BUCKET"] = "sunat-risk-scoring-perf-36n"

        s3 = self.reload_s3_paths()
        self.assertEqual(s3.RAW_PADRON_RUC, "s3://sunat-risk-scoring-raw/padron_ruc")
        self.assertEqual(
            s3.SILVER_PADRON_RUC, "s3://sunat-risk-scoring-perf-36n/silver/padron_ruc"
        )
        self.assertEqual(
            s3.SILVER_EPEN, "s3://sunat-risk-scoring-perf-36n/silver/epen"
        )
        self.assertEqual(
            s3.GOLD_REGIONAL_SUMMARY,
            "s3://sunat-risk-scoring-perf-36n/gold/regional_summary",
        )

    def test_perf_36p_layout_paths(self):
        """Para bucket de benchmark perf-36p, silver/ se incluye en la ruta."""
        os.environ["RAW_BUCKET"] = "sunat-risk-scoring-raw"
        os.environ["SILVER_BUCKET"] = "sunat-risk-scoring-perf-36p"
        os.environ["GOLD_BUCKET"] = "sunat-risk-scoring-perf-36p"

        s3 = self.reload_s3_paths()
        self.assertEqual(
            s3.SILVER_PADRON_RUC, "s3://sunat-risk-scoring-perf-36p/silver/padron_ruc"
        )
        self.assertEqual(
            s3.GOLD_REGIONAL_SUMMARY,
            "s3://sunat-risk-scoring-perf-36p/gold/regional_summary",
        )

    def test_perf_e2_bronze_paths(self):
        """Para benchmark E2, bronze/ y bronze/quarantine se construyen con prefijo bronze."""
        os.environ["RAW_BUCKET"] = "sunat-risk-scoring-raw"
        os.environ["BRONZE_BUCKET"] = "sunat-risk-scoring-perf-e2"

        s3 = self.reload_s3_paths()
        self.assertEqual(
            s3.BRONZE_PADRON_RUC, "s3://sunat-risk-scoring-perf-e2/bronze/padron_ruc"
        )
        self.assertEqual(
            s3.BRONZE_PADRON_RUC_QUARANTINE,
            "s3://sunat-risk-scoring-perf-e2/bronze/quarantine/padron_ruc",
        )

    def test_explicit_override_force_prefix(self):
        """Verifica que S3_USE_LAYER_PREFIX fuerce el comportamiento deseado."""
        os.environ["RAW_BUCKET"] = "sunat-risk-scoring-raw"
        os.environ["SILVER_BUCKET"] = "sunat-risk-scoring-silver"
        os.environ["S3_USE_LAYER_PREFIX"] = "true"

        s3 = self.reload_s3_paths()
        self.assertEqual(
            s3.SILVER_PADRON_RUC, "s3://sunat-risk-scoring-silver/silver/padron_ruc"
        )

        os.environ["S3_USE_LAYER_PREFIX"] = "false"
        s3 = self.reload_s3_paths()
        self.assertEqual(
            s3.SILVER_PADRON_RUC, "s3://sunat-risk-scoring-silver/padron_ruc"
        )


if __name__ == "__main__":
    unittest.main()
