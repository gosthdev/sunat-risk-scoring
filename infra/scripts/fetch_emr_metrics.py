"""
Extrae tiempos de ejecución de los job runs de EMR Serverless directo desde
la API (boto3), sin leer logs a mano.

NOTA sobre "steps": el proyecto corre en EMR Serverless, que NO tiene Steps ni
`describe_step` (eso es de EMR clásico sobre EC2, que exige ClusterId/StepId).
El equivalente en EMR Serverless es el "job run": cada uno de los 5 steps del
pipeline (01_ingest_bronze ... 05_scoring_dataset_gold) y cada corrida del
benchmark es un job run, y la API que entrega sus timestamps es `get_job_run`
(con `list_job_runs` para descubrirlos).

Por job run se reportan:
  created_at / started_at / ended_at : timestamps (UTC) de la API
  queued_s    : espera en cola/arranque (created -> started)
  run_s       : tiempo corriendo (started -> ended)
  total_s     : tiempo total (created -> ended)
  exec_s      : totalExecutionDurationSeconds reportado por EMR
  vcpu_hours / memory_gb_hours / storage_gb_hours : recursos consumidos

Ejemplos:
  # Todos los job runs del proyecto en las últimas 24 h
  python infra/scripts/fetch_emr_metrics.py

  # Solo las corridas del benchmark, últimas 6 h, a CSV
  python infra/scripts/fetch_emr_metrics.py --name-prefix sunat-ssco-bench- \\
      --since-hours 6 --format csv --output-file benchmark_results/emr_metrics.csv

  # Job runs concretos
  python infra/scripts/fetch_emr_metrics.py --job-run-id 00abc123 --job-run-id 00def456

Credenciales: las estándar de AWS (env vars, perfil o rol asumido). Permisos
necesarios: emr-serverless:ListApplications, ListJobRuns y GetJobRun.
"""

import argparse
import csv
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone

DEFAULT_APPLICATION_NAME = "sunat-ssco-spark"
DEFAULT_NAME_PREFIX = "sunat-ssco-"
TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}

FIELDS = [
    "name",
    "job_run_id",
    "state",
    "created_at",
    "started_at",
    "ended_at",
    "queued_s",
    "run_s",
    "total_s",
    "exec_s",
    "vcpu_hours",
    "memory_gb_hours",
    "storage_gb_hours",
]


def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""


def _seconds(start, end):
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 1)


def summarize_job_run(job_run: dict) -> dict:
    """Convierte la respuesta de get_job_run en una fila de métricas."""
    created = job_run.get("createdAt")
    started = job_run.get("startedAt")
    ended = job_run.get("endedAt")
    state = job_run.get("state", "")

    # Algunos job runs antiguos no traen endedAt: en estado terminal,
    # updatedAt es la mejor aproximación.
    if ended is None and state in TERMINAL_STATES:
        ended = job_run.get("updatedAt")

    queued_ms = job_run.get("queuedDurationMilliseconds")
    queued_s = round(queued_ms / 1000, 1) if queued_ms is not None else None
    if queued_s is None:
        queued_s = _seconds(created, started)

    usage = job_run.get("totalResourceUtilization") or {}

    return {
        "name": job_run.get("name", ""),
        "job_run_id": job_run.get("jobRunId", ""),
        "state": state,
        "created_at": _iso(created),
        "started_at": _iso(started),
        "ended_at": _iso(ended),
        "queued_s": queued_s,
        "run_s": _seconds(started, ended),
        "total_s": _seconds(created, ended),
        "exec_s": job_run.get("totalExecutionDurationSeconds"),
        "vcpu_hours": usage.get("vCPUHour"),
        "memory_gb_hours": usage.get("memoryGBHour"),
        "storage_gb_hours": usage.get("storageGBHour"),
    }


def resolve_application_id(client, application_name: str) -> str:
    """Busca el ID de la aplicación por nombre (ignora las TERMINATED)."""
    token = None
    while True:
        kwargs = {"nextToken": token} if token else {}
        resp = client.list_applications(**kwargs)
        for app in resp.get("applications", []):
            if app["name"] == application_name and app["state"] != "TERMINATED":
                return app["id"]
        token = resp.get("nextToken")
        if not token:
            break
    raise RuntimeError(
        f"No se encontró la aplicación EMR Serverless '{application_name}'"
    )


def list_job_run_ids(client, application_id, name_prefix, since):
    """IDs de job runs creados desde `since` cuyo nombre empieza con name_prefix."""
    ids = []
    token = None
    while True:
        kwargs = {"applicationId": application_id, "createdAtAfter": since}
        if token:
            kwargs["nextToken"] = token
        resp = client.list_job_runs(**kwargs)
        for run in resp.get("jobRuns", []):
            if run.get("name", "").startswith(name_prefix):
                ids.append(run["id"])
        token = resp.get("nextToken")
        if not token:
            break
    return ids


def fetch_metrics(client, application_id, job_run_ids):
    """get_job_run por cada ID -> filas ordenadas por fecha de creación."""
    rows = []
    for job_run_id in job_run_ids:
        resp = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)
        rows.append(summarize_job_run(resp["jobRun"]))
    return sorted(rows, key=lambda r: r["created_at"])


def render(rows, fmt):
    if fmt == "json":
        return json.dumps(rows, indent=2, ensure_ascii=False)

    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if row[k] is None else row[k] for k in FIELDS})
        return buf.getvalue().rstrip("\n")

    # tabla de texto
    def cell(value):
        return "-" if value in (None, "") else str(value)

    widths = {f: max([len(f)] + [len(cell(r[f])) for r in rows]) for f in FIELDS}
    header = "  ".join(f.ljust(widths[f]) for f in FIELDS)
    lines = [header, "  ".join("-" * widths[f] for f in FIELDS)]
    for r in rows:
        lines.append("  ".join(cell(r[f]).ljust(widths[f]) for f in FIELDS))
    return "\n".join(lines)


def _make_client(region):
    import boto3  # import diferido: los tests no necesitan boto3 ni credenciales

    return boto3.client("emr-serverless", region_name=region)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Tiempos de job runs de EMR Serverless desde la API"
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1",
    )
    parser.add_argument(
        "--application-id", default=os.environ.get("EMR_APPLICATION_ID")
    )
    parser.add_argument("--application-name", default=DEFAULT_APPLICATION_NAME)
    parser.add_argument(
        "--job-run-id",
        action="append",
        default=[],
        help="ID de job run (repetible). Si se indica, se ignoran --name-prefix y --since-hours.",
    )
    parser.add_argument("--name-prefix", default=DEFAULT_NAME_PREFIX)
    parser.add_argument("--since-hours", type=float, default=24.0)
    parser.add_argument("--format", choices=["table", "csv", "json"], default="table")
    parser.add_argument("--output-file", default=None)
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    client = _make_client(args.region)

    application_id = args.application_id or resolve_application_id(
        client, args.application_name
    )

    if args.job_run_id:
        job_run_ids = args.job_run_id
    else:
        since = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        job_run_ids = list_job_run_ids(client, application_id, args.name_prefix, since)

    if not job_run_ids:
        print("No se encontraron job runs con esos filtros.", file=sys.stderr)
        return 1

    rows = fetch_metrics(client, application_id, job_run_ids)
    output = render(rows, args.format)

    if args.output_file:
        with open(args.output_file, "w", encoding="utf-8") as fh:
            fh.write(output + "\n")
        print(f"{len(rows)} job runs escritos en {args.output_file}")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())