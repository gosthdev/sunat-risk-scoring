import importlib.util
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

_SCRIPT = (
    Path(__file__).resolve().parent.parent
    / "infra"
    / "scripts"
    / "fetch_emr_metrics.py"
)
_spec = importlib.util.spec_from_file_location("fetch_emr_metrics", _SCRIPT)
assert _spec is not None and _spec.loader is not None
fem = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fem)

T0 = datetime(2026, 10, 8, 12, 0, 0, tzinfo=UTC)


def _job_run(**overrides):
    base = {
        "jobRunId": "00abc",
        "name": "sunat-ssco-bench-partitioned-20261008-120000",
        "state": "SUCCESS",
        "createdAt": T0,
        "startedAt": T0 + timedelta(seconds=40),
        "endedAt": T0 + timedelta(seconds=340),
        "updatedAt": T0 + timedelta(seconds=341),
        "queuedDurationMilliseconds": 40000,
        "totalExecutionDurationSeconds": 295,
        "totalResourceUtilization": {
            "vCPUHour": 0.5,
            "memoryGBHour": 2.0,
            "storageGBHour": 1.0,
        },
    }
    base.update(overrides)
    return base


class FakeClient:
    def __init__(self):
        self.job_runs = {
            "a": _job_run(jobRunId="a", name="sunat-ssco-01_ingest_bronze"),
            "b": _job_run(
                jobRunId="b",
                name="sunat-ssco-bench-query",
                createdAt=T0 + timedelta(minutes=10),
                startedAt=T0 + timedelta(minutes=10, seconds=5),
                endedAt=T0 + timedelta(minutes=11),
            ),
            "c": _job_run(jobRunId="c", name="otro-proyecto-job"),
        }

    def list_applications(self, **kwargs):
        return {
            "applications": [
                {"id": "app-old", "name": "sunat-ssco-spark", "state": "TERMINATED"},
                {"id": "app-1", "name": "sunat-ssco-spark", "state": "STARTED"},
            ]
        }

    def list_job_runs(self, **kwargs):
        if "nextToken" not in kwargs:
            return {
                "jobRuns": [{"id": "a", "name": "sunat-ssco-01_ingest_bronze"}],
                "nextToken": "t1",
            }
        return {
            "jobRuns": [
                {"id": "b", "name": "sunat-ssco-bench-query"},
                {"id": "c", "name": "otro-proyecto-job"},
            ]
        }

    def get_job_run(self, applicationId, jobRunId):
        return {"jobRun": self.job_runs[jobRunId]}


class TestFetchEmrMetrics(unittest.TestCase):
    def test_summarize_uses_api_timestamps(self):
        row = fem.summarize_job_run(_job_run())
        self.assertEqual(row["started_at"], "2026-10-08T12:00:40Z")
        self.assertEqual(row["ended_at"], "2026-10-08T12:05:40Z")
        self.assertEqual(row["queued_s"], 40.0)
        self.assertEqual(row["run_s"], 300.0)
        self.assertEqual(row["total_s"], 340.0)
        self.assertEqual(row["exec_s"], 295)
        self.assertEqual(row["vcpu_hours"], 0.5)

    def test_summarize_fallbacks_when_fields_missing(self):
        run = _job_run()
        del run["endedAt"]
        del run["queuedDurationMilliseconds"]
        row = fem.summarize_job_run(run)
        self.assertEqual(row["ended_at"], "2026-10-08T12:05:41Z")  # updatedAt
        self.assertEqual(row["queued_s"], 40.0)  # started - created

    def test_summarize_running_job_has_no_end(self):
        run = _job_run(state="RUNNING")
        del run["endedAt"]
        row = fem.summarize_job_run(run)
        self.assertEqual(row["ended_at"], "")
        self.assertIsNone(row["run_s"])

    def test_resolve_application_skips_terminated(self):
        self.assertEqual(
            fem.resolve_application_id(FakeClient(), "sunat-ssco-spark"), "app-1"
        )

    def test_list_job_runs_paginates_and_filters_by_prefix(self):
        ids = fem.list_job_run_ids(FakeClient(), "app-1", "sunat-ssco-", T0)
        self.assertEqual(ids, ["a", "b"])
        ids = fem.list_job_run_ids(FakeClient(), "app-1", "sunat-ssco-bench-", T0)
        self.assertEqual(ids, ["b"])

    def test_fetch_and_render_formats(self):
        rows = fem.fetch_metrics(FakeClient(), "app-1", ["b", "a"])
        self.assertEqual([r["job_run_id"] for r in rows], ["a", "b"])  # por fecha

        csv_out = fem.render(rows, "csv")
        self.assertTrue(csv_out.startswith("name,job_run_id,state"))
        self.assertEqual(len(csv_out.splitlines()), 3)

        self.assertIn('"job_run_id": "a"', fem.render(rows, "json"))
        self.assertIn("sunat-ssco-bench-query", fem.render(rows, "table"))
        self.assertTrue(fem.render([], "table").startswith("name"))


if __name__ == "__main__":
    unittest.main()
