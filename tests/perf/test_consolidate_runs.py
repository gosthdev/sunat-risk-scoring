import json
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
_ANALYSIS = str(_ROOT / "perf" / "analysis")
if _ANALYSIS not in sys.path:
    sys.path.insert(0, _ANALYSIS)

import consolidate_runs


class TestConsolidateRuns(unittest.TestCase):
    def test_parse_real_emr_json(self):
        """Verifica parseo correcto con los nombres de campos devueltos por AWS EMR Serverless."""
        payload = {
            "jobRun": {
                "applicationId": "00g9dmeddmpavm09",
                "jobRunId": "run-test-123",
                "name": "run-e1-N1",
                "state": "SUCCESS",
                "createdAt": "2026-10-10T02:00:00.000Z",
                "totalExecutionDurationSeconds": 148,
                "totalResourceUtilization": {
                    "vCPUHour": 0.651,
                    "memoryGBHour": 2.604,
                    "storageGBHour": 3.255,
                },
                "tags": {
                    "Project": "sunat-risk-scoring",
                    "Experiment": "E1",
                    "Profile": "C3",
                    "GitSHA": "abcdef",
                },
            }
        }

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fp:
            json.dump(payload, fp)
            temp_path = Path(fp.name)

        try:
            run = consolidate_runs.parse_job_run_json(temp_path)
            self.assertEqual(run["job_run_id"], "run-test-123")
            self.assertEqual(run["name"], "run-e1-N1")
            self.assertEqual(run["state"], "SUCCESS")
            self.assertEqual(run["total_execution_seconds"], 148)
            self.assertEqual(run["vcpu_hours"], 0.651)
            self.assertEqual(run["memory_gb_hours"], 2.604)
            self.assertEqual(run["storage_gb_hours"], 3.255)
            self.assertEqual(run["condition"], "N")
        finally:
            temp_path.unlink()

    def test_analyze_runs_e1_conclusive(self):
        """Verifica que determine victoria concluyente cuando los rangos no se solapan."""
        runs = [
            # Warmup
            {
                "name": "run-e1-warmup",
                "condition": "warmup",
                "total_execution_seconds": 180,
                "vcpu_hours": 0.8,
            },
            # Condición N: [145, 148, 150]
            {
                "name": "run-e1-N1",
                "condition": "N",
                "total_execution_seconds": 145,
                "vcpu_hours": 0.65,
            },
            {
                "name": "run-e1-N2",
                "condition": "N",
                "total_execution_seconds": 148,
                "vcpu_hours": 0.65,
            },
            {
                "name": "run-e1-N3",
                "condition": "N",
                "total_execution_seconds": 150,
                "vcpu_hours": 0.66,
            },
            # Condición P: [62, 64, 65]
            {
                "name": "run-e1-P1",
                "condition": "P",
                "total_execution_seconds": 62,
                "vcpu_hours": 0.28,
            },
            {
                "name": "run-e1-P2",
                "condition": "P",
                "total_execution_seconds": 64,
                "vcpu_hours": 0.28,
            },
            {
                "name": "run-e1-P3",
                "condition": "P",
                "total_execution_seconds": 65,
                "vcpu_hours": 0.29,
            },
        ]

        analysis = consolidate_runs.analyze_runs(runs, discard_warmup=True)
        self.assertIn("N", analysis["conditions"])
        self.assertIn("P", analysis["conditions"])
        self.assertNotIn("warmup", analysis["conditions"])

        comp = analysis["comparison"]
        self.assertIsNotNone(comp)
        self.assertEqual(comp["winner_candidate"], "P")
        self.assertTrue(comp["conclusive"])
        self.assertEqual(comp["verdict"], "GANA P")
        self.assertAlmostEqual(comp["speedup_median"], 148 / 64, places=2)

    def test_analyze_runs_overlapping_inconclusive(self):
        """Verifica veredicto NO CONCLUYENTE ante solapamiento de rangos."""
        runs = [
            {
                "name": "run-e2-A1",
                "condition": "A",
                "total_execution_seconds": 100,
                "vcpu_hours": 0.5,
            },
            {
                "name": "run-e2-A2",
                "condition": "A",
                "total_execution_seconds": 110,
                "vcpu_hours": 0.5,
            },
            {
                "name": "run-e2-B1",
                "condition": "B",
                "total_execution_seconds": 105,
                "vcpu_hours": 0.4,
            },
            {
                "name": "run-e2-B2",
                "condition": "B",
                "total_execution_seconds": 95,
                "vcpu_hours": 0.4,
            },
        ]

        analysis = consolidate_runs.analyze_runs(runs, discard_warmup=False)
        comp = analysis["comparison"]
        self.assertIsNotNone(comp)
        self.assertFalse(comp["conclusive"])
        self.assertIn("NO CONCLUYENTE", comp["verdict"])


if __name__ == "__main__":
    unittest.main()
