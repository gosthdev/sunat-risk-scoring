#!/usr/bin/env python3
"""Recolector de métricas de rendimiento (EMR Serverless, Spark y Athena).

Genera CSV en --out-dir:
  emr_runs.csv        Una fila por job run (duración, vCPU/memoria/disco, costo estimado, tags).
  spark_runs.csv      Métricas agregadas del event log de Spark por job run.
  spark_stages.csv    Una fila por etapa (tareas, duración).
  spark_scans.csv     Una fila por nodo Scan parquet (filtros de partición, archivos, bytes, filas).
  athena_queries.csv  Historial del workgroup (bytes escaneados, tiempos) etiquetado por Qa/Qb/Qc y layout N/P.
  athena_explain/     Plan EXPLAIN de cada consulta y layout.

Usa la AWS CLI (respeta AWS_PROFILE / AWS_REGION). Los event logs se cachean en
<out-dir>/_cache para no volver a descargarlos.

Ejemplo:
  AWS_PROFILE=bigdata python3 perf/analysis/collect_metrics.py \
      --created-after 2026-10-10T21:00:00Z
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "perf" / "analysis" / "sql"

# Tarifas EMR Serverless us-east-1 (x86). Son estimadas: verifica la página de precios vigente.
RATE_VCPU_H = 0.052624
RATE_MEM_GB_H = 0.0057785
RATE_STORAGE_GB_H = 0.000111


def aws(*args: str, region: str | None = None):
    cmd = ["aws", *args, "--output", "json"]
    if region:
        cmd += ["--region", region]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} falló: {res.stderr.strip()[:300]}")
    return json.loads(res.stdout) if res.stdout.strip() else None


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path} ({len(rows)} filas)")


# ------------------------------------------------------------------ EMR

def find_app_id(name: str, region: str) -> str:
    apps = aws("emr-serverless", "list-applications", region=region)["applications"]
    for a in apps:
        if a["name"] == name and a["state"] != "TERMINATED":
            return a["id"]
    raise SystemExit(f"No se encontró la aplicación EMR '{name}'")


def collect_emr(app_id: str, region: str, created_after: str, name_re: re.Pattern):
    listing = aws(
        "emr-serverless", "list-job-runs", "--application-id", app_id,
        "--created-at-after", created_after, "--states", "SUCCESS", region=region,
    )["jobRuns"]
    ids = [j["id"] for j in listing if name_re.search(j.get("name", ""))]
    print(f"  {len(ids)} job runs SUCCESS desde {created_after}")

    def one(job_id):
        return aws("emr-serverless", "get-job-run", "--application-id", app_id,
                   "--job-run-id", job_id, region=region)["jobRun"]

    with ThreadPoolExecutor(8) as ex:
        runs = list(ex.map(one, ids))

    rows = []
    for jr in sorted(runs, key=lambda r: r["createdAt"]):
        tot = jr.get("totalResourceUtilization", {})
        bil = jr.get("billedResourceUtilization", {})
        vcpu, mem, sto = (bil.get("vCPUHour") or tot.get("vCPUHour") or 0,
                          bil.get("memoryGBHour") or tot.get("memoryGBHour") or 0,
                          bil.get("storageGBHour") or tot.get("storageGBHour") or 0)
        tags = jr.get("tags", {})
        rows.append({
            "job_run_id": jr["jobRunId"],
            "name": jr["name"],
            "state": jr["state"],
            "condition": tags.get("Condition", ""),
            "experiment": tags.get("Experiment", ""),
            "profile": tags.get("Profile", ""),
            "git_sha": tags.get("GitSHA", ""),
            "created_at": jr["createdAt"],
            "ended_at": jr.get("endedAt", ""),
            "execution_seconds": jr.get("totalExecutionDurationSeconds", 0),
            "vcpu_hour": tot.get("vCPUHour", 0),
            "memory_gb_hour": tot.get("memoryGBHour", 0),
            "storage_gb_hour": tot.get("storageGBHour", 0),
            "billed_vcpu_hour": bil.get("vCPUHour", 0),
            "billed_memory_gb_hour": bil.get("memoryGBHour", 0),
            "billed_storage_gb_hour": bil.get("storageGBHour", 0),
            "cost_usd_est": round(vcpu * RATE_VCPU_H + mem * RATE_MEM_GB_H + sto * RATE_STORAGE_GB_H, 5),
            "release": jr.get("releaseLabel", ""),
        })
    return rows


# ------------------------------------------------------------------ Spark event logs

def download_eventlog(bucket: str, prefix: str, app_id: str, job_id: str, cache: Path, region: str) -> list[Path]:
    dest = cache / job_id
    if not dest.exists() or not any(dest.rglob("events_*")):
        dest.mkdir(parents=True, exist_ok=True)
        uri = f"s3://{bucket}/{prefix}applications/{app_id}/jobs/{job_id}/sparklogs/"
        subprocess.run(["aws", "s3", "cp", "--recursive", "--quiet", uri, str(dest), "--region", region],
                       capture_output=True, text=True)
    return sorted(dest.rglob("events_*"))


def walk_plan(node: dict, out: list):
    out.append(node)
    for c in node.get("children", []) or []:
        walk_plan(c, out)


def norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def parse_scan_details(node: dict) -> dict:
    meta = node.get("metadata") or {}
    simple = node.get("simpleString", "") or ""

    def pick(key, regex):
        if key in meta:
            return meta[key]
        m = re.search(regex, simple)
        return m.group(1).strip() if m else ""

    return {
        "location": pick("Location", r"Location: [A-Za-z]+\(\d+ paths?\)\[([^\]]*)\]"),
        "partition_filters": pick("PartitionFilters", r"PartitionFilters: (\[.*?\])(?:, [A-Z]|$)"),
        "pushed_filters": pick("PushedFilters", r"PushedFilters: (\[.*?\])(?:, [A-Z]|$)"),
        "data_filters": pick("DataFilters", r"DataFilters: (\[.*?\])(?:, [A-Z]|$)"),
        "read_schema": pick("ReadSchema", r"ReadSchema: (\S+)"),
    }


def parse_eventlog(files: list[Path], run: dict):
    acc_names: dict[int, tuple[str, str]] = {}
    scan_nodes: dict[tuple, dict] = {}
    acc_sum: dict[int, float] = defaultdict(float)
    sql_exec: dict[int, dict] = {}
    stages: list[dict] = []
    s = defaultdict(float)
    s["tasks"] = s["tasks_failed"] = 0
    max_peak_mem = 0
    app_start = app_end = None
    executors = 0

    def register_plan(info: dict, exec_id: int):
        nodes: list = []
        walk_plan(info, nodes)
        for n in nodes:
            for m in n.get("metrics", []) or []:
                acc_names[m["accumulatorId"]] = (n.get("nodeName", ""), m["name"])
            name = n.get("nodeName", "")
            if "Scan" in name and "parquet" in (n.get("simpleString", "") + name).lower():
                ids = tuple(sorted(m["accumulatorId"] for m in n.get("metrics", []) or []))
                if ids and ids not in scan_nodes:
                    scan_nodes[ids] = {"execution_id": exec_id, "node": n}

    for fp in files:
        with open(fp, encoding="utf-8") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                e = ev.get("Event", "")
                if e.endswith("SparkListenerSQLExecutionStart"):
                    eid = ev["executionId"]
                    sql_exec[eid] = {"start": ev["time"], "description": (ev.get("description") or "")[:120]}
                    register_plan(ev["sparkPlanInfo"], eid)
                elif e.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
                    register_plan(ev["sparkPlanInfo"], ev["executionId"])
                elif e.endswith("SparkListenerSQLExecutionEnd"):
                    if ev["executionId"] in sql_exec:
                        sql_exec[ev["executionId"]]["end"] = ev["time"]
                elif e.endswith("SparkListenerDriverAccumUpdates"):
                    for acc_id, val in ev.get("accumUpdates", []):
                        acc_sum[acc_id] += val
                elif e == "SparkListenerTaskEnd":
                    s["tasks"] += 1
                    if ev.get("Task End Reason", {}).get("Reason") != "Success":
                        s["tasks_failed"] += 1
                    tm = ev.get("Task Metrics") or {}
                    s["executor_run_ms"] += tm.get("Executor Run Time", 0)
                    s["executor_cpu_ms"] += tm.get("Executor CPU Time", 0) / 1e6
                    s["deserialize_ms"] += tm.get("Executor Deserialize Time", 0)
                    s["gc_ms"] += tm.get("JVM GC Time", 0)
                    s["spill_memory_bytes"] += tm.get("Memory Bytes Spilled", 0)
                    s["spill_disk_bytes"] += tm.get("Disk Bytes Spilled", 0)
                    s["input_bytes"] += tm.get("Input Metrics", {}).get("Bytes Read", 0)
                    s["input_records"] += tm.get("Input Metrics", {}).get("Records Read", 0)
                    s["output_bytes"] += tm.get("Output Metrics", {}).get("Bytes Written", 0)
                    s["output_records"] += tm.get("Output Metrics", {}).get("Records Written", 0)
                    sr = tm.get("Shuffle Read Metrics", {})
                    s["shuffle_read_bytes"] += sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
                    s["shuffle_write_bytes"] += tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                    max_peak_mem = max(max_peak_mem, tm.get("Peak Execution Memory", 0))
                    for a in ev.get("Task Info", {}).get("Accumulables", []) or []:
                        u = a.get("Update")
                        if isinstance(u, (int, float)):
                            acc_sum[a["ID"]] += u
                elif e == "SparkListenerStageCompleted":
                    si = ev["Stage Info"]
                    sub, comp = si.get("Submission Time"), si.get("Completion Time")
                    stages.append({
                        "job_run_id": run["job_run_id"], "run_name": run["name"],
                        "stage_id": si["Stage ID"], "attempt": si.get("Stage Attempt ID", 0),
                        "name": (si.get("Stage Name") or "")[:80], "tasks": si.get("Number of Tasks", 0),
                        "duration_ms": (comp - sub) if sub and comp else "",
                    })
                elif e == "SparkListenerExecutorAdded":
                    executors += 1
                elif e == "SparkListenerApplicationStart":
                    app_start = ev.get("Timestamp")
                elif e == "SparkListenerApplicationEnd":
                    app_end = ev.get("Timestamp")

    scans = []
    for ids, info in scan_nodes.items():
        row = {"job_run_id": run["job_run_id"], "run_name": run["name"],
               "execution_id": info["execution_id"], **parse_scan_details(info["node"])}
        for acc_id in ids:
            _, mname = acc_names[acc_id]
            row[norm(mname)] = round(acc_sum.get(acc_id, 0), 2)
        scans.append(row)

    sql_durations = [v["end"] - v["start"] for v in sql_exec.values() if "end" in v]
    summary = {
        "job_run_id": run["job_run_id"], "run_name": run["name"], "condition": run["condition"],
        "app_duration_ms": (app_end - app_start) if app_start and app_end else "",
        "sql_executions": len(sql_exec),
        "sql_total_ms": sum(sql_durations),
        "stages": len(stages), "executors": executors,
        **{k: round(v, 1) for k, v in s.items()},
        "peak_execution_memory_bytes": max_peak_mem,
        "scan_nodes": len(scans),
        "scan_files_read_total": sum(x.get("number_of_files_read", 0) for x in scans),
        "scan_bytes_read_total": sum(x.get("size_of_files_read", 0) for x in scans),
    }
    return summary, stages, scans


def collect_spark(runs: list[dict], app_id: str, bucket: str, prefix: str, cache: Path, region: str):
    cache.mkdir(parents=True, exist_ok=True)
    print(f"  Descargando/leyendo event logs de {len(runs)} runs ...")

    def one(run):
        files = download_eventlog(bucket, prefix, app_id, run["job_run_id"], cache, region)
        if not files:
            return None
        return parse_eventlog(files, run)

    with ThreadPoolExecutor(6) as ex:
        results = list(ex.map(one, runs))
    summaries, stages, scans = [], [], []
    for r in results:
        if r:
            summaries.append(r[0]); stages += r[1]; scans += r[2]
    return summaries, stages, scans


# ------------------------------------------------------------------ Athena

def athena_wait(qid: str, region: str) -> dict:
    while True:
        q = aws("athena", "get-query-execution", "--query-execution-id", qid, region=region)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return q
        time.sleep(1.5)


def load_queries(tables: dict[str, str]) -> dict[str, str]:
    """Mapa 'Qa|N' -> SQL normalizado con la tabla sustituida."""
    out = {}
    for f in sorted(SQL_DIR.glob("e1_q*.sql")):
        label = "Q" + f.stem[-1]
        raw = f.read_text()
        for layout, table in tables.items():
            out[f"{label}|{layout}"] = raw.replace("${TABLE}", table)
    return out


def squash(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", "", sql)
    return re.sub(r"\s+", " ", sql).strip().rstrip(";").lower()


def collect_athena(workgroup: str, database: str, tables: dict[str, str], created_after: str, region: str, out_dir: Path):
    since = datetime.fromisoformat(created_after)
    known = {squash(sql): key for key, sql in load_queries(tables).items()}
    ids, token = [], None
    while True:
        args = ["athena", "list-query-executions", "--work-group", workgroup, "--max-items", "500"]
        if token:
            args += ["--starting-token", token]
        page = aws(*args, region=region)
        ids += page.get("QueryExecutionIds", [])
        token = page.get("NextToken")
        if not token:
            break

    rows = []
    for i in range(0, len(ids), 50):
        batch = aws("athena", "batch-get-query-execution", "--query-execution-ids", *ids[i:i + 50], region=region)
        for q in batch["QueryExecutions"]:
            sub = q["Status"]["SubmissionDateTime"]
            if datetime.fromisoformat(sub) < since:
                continue
            st = q.get("Statistics", {})
            key = known.get(squash(q["Query"]))
            if not key:
                continue
            label, layout = key.split("|")
            rows.append({
                "query_id": q["QueryExecutionId"], "query": label, "layout": layout,
                "state": q["Status"]["State"], "submitted_at": sub,
                "data_scanned_bytes": st.get("DataScannedInBytes", 0),
                "engine_ms": st.get("EngineExecutionTimeInMillis", 0),
                "total_ms": st.get("TotalExecutionTimeInMillis", 0),
                "planning_ms": st.get("QueryPlanningTimeInMillis", ""),
                "queue_ms": st.get("QueryQueueTimeInMillis", ""),
                "service_processing_ms": st.get("ServiceProcessingTimeInMillis", ""),
                "result_reused": st.get("ResultReuseInformation", {}).get("ReusedPreviousResult", ""),
            })
    rows.sort(key=lambda r: r["submitted_at"])

    explain_dir = out_dir / "athena_explain"
    explain_dir.mkdir(parents=True, exist_ok=True)
    for key, sql in load_queries(tables).items():
        label, layout = key.split("|")
        qid = aws("athena", "start-query-execution", "--query-string", "EXPLAIN " + sql,
                  "--work-group", workgroup, "--query-execution-context", f"Database={database}",
                  region=region)["QueryExecutionId"]
        q = athena_wait(qid, region)
        if q["Status"]["State"] != "SUCCEEDED":
            print(f"  WARN: EXPLAIN {key} {q['Status']['State']}: {q['Status'].get('StateChangeReason', '')}")
            continue
        res = aws("athena", "get-query-results", "--query-execution-id", qid, region=region)
        text = "\n".join(r["Data"][0].get("VarCharValue", "") for r in res["ResultSet"]["Rows"])
        (explain_dir / f"{label}_{layout}.txt").write_text(text)
    print(f"  ✓ EXPLAIN guardados en {explain_dir}")
    return rows


# ------------------------------------------------------------------ main

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    p.add_argument("--app-name", default="sunat-ssco-perf-spark")
    p.add_argument("--app-id", default=os.environ.get("EMR_APPLICATION_ID", ""))
    p.add_argument("--created-after", required=True, help="ISO 8601 UTC, ej. 2026-10-10T21:00:00Z")
    p.add_argument("--name-regex", default=".", help="Filtra job runs por nombre")
    p.add_argument("--logs-bucket", default=os.environ.get("ARTIFACTS_BUCKET", "sunat-risk-scoring-artifacts"))
    p.add_argument("--logs-prefix", default="emr-logs/")
    p.add_argument("--out-dir", default=str(REPO_ROOT / "perf" / "results" / "metrics"))
    p.add_argument("--workgroup", default="sunat-ssco")
    p.add_argument("--database", default="sunat_ssco_perf")
    p.add_argument("--table-p", default="sunat_ssco_perf.p_silver_padron_ruc")
    p.add_argument("--table-n", default="sunat_ssco_perf.n_silver_padron_ruc")
    p.add_argument("--skip-spark", action="store_true")
    p.add_argument("--skip-athena", action="store_true")
    a = p.parse_args()

    out = Path(a.out_dir)
    app_id = a.app_id or find_app_id(a.app_name, a.region)
    print(f"Aplicación EMR: {app_id}")

    print("[1/3] EMR job runs")
    runs = collect_emr(app_id, a.region, a.created_after, re.compile(a.name_regex))
    write_csv(out / "emr_runs.csv", runs)

    if not a.skip_spark:
        print("[2/3] Spark event logs")
        summaries, stages, scans = collect_spark(runs, app_id, a.logs_bucket, a.logs_prefix, out / "_cache", a.region)
        write_csv(out / "spark_runs.csv", summaries)
        write_csv(out / "spark_stages.csv", stages)
        write_csv(out / "spark_scans.csv", scans)

    if not a.skip_athena:
        print("[3/3] Athena")
        rows = collect_athena(a.workgroup, a.database, {"P": a.table_p, "N": a.table_n},
                              a.created_after, a.region, out)
        write_csv(out / "athena_queries.csv", rows)

    print("Listo.")


if __name__ == "__main__":
    sys.exit(main())
