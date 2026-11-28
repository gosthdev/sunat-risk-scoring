"""Junta en un JSON lo medido en una corrida de la prueba de escalabilidad.

Por cada job de EMR Serverless lee:
  - get-job-run: estado, duración, cola y recursos consumidos y facturados;
  - stdout del driver: las líneas SALIDA y CALIDAD que imprimen los jobs (conteos de filas);
  - event log de Spark (emr-logs/.../sparklogs/): shuffle, spill, GC y duración de tareas.

Uso:
  python perf/analysis/collect_runs.py --tipo pipeline --corrida S-01-r1 --factor 1 \\
      --perfil Base --pasos scratch/perf/S-01-r1/pasos.tsv --salida perf/results/runs/S-01-r1.json

pasos.tsv (lo escriben correr_pipeline.sh y replicar.sh), separado por tabuladores:
  paso  job_run_id  inicio_epoch  fin_epoch  codigo_salida
"""

import argparse
import gzip
import json
import re
import statistics
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ARTEFACTOS = "sunat-risk-scoring-perf-x1"
APP_NOMBRE = "sunat-ssco-perf-spark"
REGION = "us-east-1"
# Tarifas públicas de EMR Serverless en us-east-1, x86, USD por hora
# (https://aws.amazon.com/emr/pricing/). El disco solo se cobra sobre 20 GB por worker,
# y eso ya viene descontado en billedResourceUtilization.
TARIFA = {"vCPUHour": 0.052624, "memoryGBHour": 0.0057785, "storageGBHour": 0.000111}

_SALIDA = re.compile(r"SALIDA (\S+): filas=(\d+)")
_CALIDAD = re.compile(
    r"CALIDAD fuente=(\S+) .* bronze=(\d+) cuarentena_bronze=(\d+) .*"
    r" cuarentena_ruc=(\d+) .* silver=(\d+)"
)
_JSON_PERF = re.compile(r"(PERF_\w+) (\{.*\})")
_GB = 1e9


def leer_filas(stdout: str) -> dict:
    """Conteos que imprimen los jobs en el stdout del driver."""
    salidas = {m.group(1): int(m.group(2)) for m in _SALIDA.finditer(stdout)}
    calidad: dict = {}
    for m in _CALIDAD.finditer(stdout):
        fuente = calidad.setdefault(
            m.group(1),
            {"bronze": 0, "cuarentena_bronze": 0, "cuarentena_ruc": 0, "silver": 0},
        )
        for clave, valor in zip(fuente, m.groups()[1:]):
            fuente[clave] += int(valor)
    extra = {m.group(1): json.loads(m.group(2)) for m in _JSON_PERF.finditer(stdout)}
    return {"salidas": salidas, "calidad_silver": calidad, **extra}


def resumir_eventos(lineas) -> dict:
    """Métricas de Spark a partir del event log (una línea JSON por evento)."""
    duraciones = []
    suma = {
        "ejecucion_ms": 0,
        "gc_ms": 0,
        "shuffle_escrito": 0,
        "shuffle_leido": 0,
        "spill_memoria": 0,
        "spill_disco": 0,
        "entrada": 0,
        "salida": 0,
    }
    heap_pico = 0
    executors = 0
    for linea in lineas:
        evento = json.loads(linea)
        tipo = evento.get("Event")
        if tipo == "SparkListenerExecutorAdded":
            executors += 1
        if tipo != "SparkListenerTaskEnd" or "Task Metrics" not in evento:
            continue
        info, m = evento["Task Info"], evento["Task Metrics"]
        duraciones.append(info["Finish Time"] - info["Launch Time"])
        lectura = m.get("Shuffle Read Metrics", {})
        suma["ejecucion_ms"] += m.get("Executor Run Time", 0)
        suma["gc_ms"] += m.get("JVM GC Time", 0)
        suma["shuffle_escrito"] += m.get("Shuffle Write Metrics", {}).get(
            "Shuffle Bytes Written", 0
        )
        suma["shuffle_leido"] += lectura.get("Remote Bytes Read", 0) + lectura.get(
            "Local Bytes Read", 0
        )
        suma["spill_memoria"] += m.get("Memory Bytes Spilled", 0)
        suma["spill_disco"] += m.get("Disk Bytes Spilled", 0)
        suma["entrada"] += m.get("Input Metrics", {}).get("Bytes Read", 0)
        suma["salida"] += m.get("Output Metrics", {}).get("Bytes Written", 0)
        heap_pico = max(
            heap_pico, evento.get("Task Executor Metrics", {}).get("JVMHeapMemory", 0)
        )
    return {
        "tareas": len(duraciones),
        "executors_lanzados": executors,
        "tiempo_tareas_s": round(suma["ejecucion_ms"] / 1000, 1),
        "gc_pct": round(100 * suma["gc_ms"] / suma["ejecucion_ms"], 2)
        if suma["ejecucion_ms"]
        else 0.0,
        "shuffle_escrito_gb": round(suma["shuffle_escrito"] / _GB, 3),
        "shuffle_leido_gb": round(suma["shuffle_leido"] / _GB, 3),
        "spill_memoria_gb": round(suma["spill_memoria"] / _GB, 3),
        "spill_disco_gb": round(suma["spill_disco"] / _GB, 3),
        "entrada_gb": round(suma["entrada"] / _GB, 3),
        "salida_gb": round(suma["salida"] / _GB, 3),
        "tarea_max_s": round(max(duraciones) / 1000, 1) if duraciones else 0.0,
        "tarea_mediana_s": round(statistics.median(duraciones) / 1000, 1)
        if duraciones
        else 0.0,
        "heap_pico_executor_gb": round(heap_pico / _GB, 2),
    }


def costo_usd(facturado: dict) -> float:
    return round(sum(facturado.get(k, 0.0) * tarifa for k, tarifa in TARIFA.items()), 4)


def leer_pasos(ruta: Path) -> list[dict]:
    pasos = []
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        if linea.strip():
            paso, run, inicio, fin, codigo = linea.split("\t")
            pasos.append(
                {
                    "paso": paso,
                    "job_run_id": run,
                    "inicio": int(inicio),
                    "fin": int(fin),
                    "codigo": int(codigo),
                }
            )
    return pasos


def _iso(valor) -> str | None:
    return valor.astimezone(UTC).isoformat() if valor else None


def _texto_s3(s3, clave: str) -> str:
    try:
        cuerpo = s3.get_object(Bucket=ARTEFACTOS, Key=clave)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return ""
    return (
        gzip.decompress(cuerpo).decode("utf-8", "replace")
        if clave.endswith(".gz")
        else cuerpo.decode()
    )


def _eventos_s3(s3, prefijo: str):
    paginas = s3.get_paginator("list_objects_v2").paginate(
        Bucket=ARTEFACTOS, Prefix=prefijo
    )
    claves = sorted(
        o["Key"]
        for p in paginas
        for o in p.get("Contents", [])
        if "/events_" in o["Key"]
    )
    for clave in claves:
        yield from s3.get_object(Bucket=ARTEFACTOS, Key=clave)["Body"].iter_lines()


def recolectar_paso(emr, s3, app: str, paso: dict) -> dict:
    run = paso["job_run_id"]
    resultado = {
        "paso": paso["paso"],
        "job_run_id": run or None,
        "codigo_salida": paso["codigo"],
    }
    resultado["pared_s"] = paso["fin"] - paso["inicio"]
    if not run:
        resultado["estado"] = "NO_LANZADO"
        return resultado
    job = emr.get_job_run(applicationId=app, jobRunId=run)["jobRun"]
    facturado = job.get("billedResourceUtilization", {})
    fin_emr = job.get("endedAt") or job.get("updatedAt")
    resultado.update(
        {
            "estado": job["state"],
            "detalle_estado": job.get("stateDetails", ""),
            "creado": _iso(job.get("createdAt")),
            "terminado": _iso(fin_emr),
            "ejecucion_s": job.get("totalExecutionDurationSeconds"),
            "cola_s": round(job.get("queuedDurationMilliseconds", 0) / 1000, 1),
            # Tiempo que corre en tu PC tras el job: publicar _staging -> ruta viva (aws s3 sync).
            "publicacion_s": round(paso["fin"] - fin_emr.timestamp(), 1)
            if fin_emr
            else None,
            "recursos": job.get("totalResourceUtilization", {}),
            "facturado": facturado,
            "costo_usd": costo_usd(facturado),
        }
    )
    base = f"emr-logs/applications/{app}/jobs/{run}"
    resultado["filas"] = leer_filas(_texto_s3(s3, f"{base}/SPARK_DRIVER/stdout.gz"))
    resultado["spark"] = resumir_eventos(_eventos_s3(s3, f"{base}/sparklogs/"))
    return resultado


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=False
    ).stdout.strip()


def main(argv=None) -> int:
    import boto3

    parser = argparse.ArgumentParser()
    parser.add_argument("--tipo", choices=["pipeline", "replicacion"], required=True)
    parser.add_argument("--corrida", required=True)
    parser.add_argument("--factor", type=int, required=True)
    parser.add_argument("--perfil", required=True)
    parser.add_argument("--pasos", type=Path, required=True)
    parser.add_argument("--salida", type=Path, required=True)
    parser.add_argument("--raw-bytes", type=int)
    parser.add_argument(
        "--app-id", help="app EMR de la corrida (por defecto, la del perfil Base)"
    )
    args = parser.parse_args(argv)

    emr = boto3.client("emr-serverless", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    app = args.app_id or next(
        a["id"]
        for a in emr.list_applications()["applications"]
        if a["name"] == APP_NOMBRE and a["state"] != "TERMINATED"
    )

    crudos = leer_pasos(args.pasos)
    pasos = [recolectar_paso(emr, s3, app, p) for p in crudos]
    lanzados = [p for p in pasos if p.get("estado") not in (None, "NO_LANZADO")]
    reporte = {
        "tipo": args.tipo,
        "corrida": args.corrida,
        "factor": args.factor,
        "perfil": args.perfil,
        "fecha": datetime.now(UTC).isoformat(timespec="seconds"),
        "commit": _git("rev-parse", "HEAD"),
        "codigo_pipeline_modificado": bool(_git("status", "--porcelain", "src")),
        "emr_application_id": app,
        "completa": all(p.get("estado") == "SUCCESS" for p in pasos),
        "total": {
            "ejecucion_s": sum(p.get("ejecucion_s") or 0 for p in lanzados),
            "pared_s": max(p["fin"] for p in crudos) - min(p["inicio"] for p in crudos)
            if crudos
            else 0,
            "vcpu_h": round(
                sum(p["facturado"].get("vCPUHour", 0) for p in lanzados), 3
            ),
            "memoria_gb_h": round(
                sum(p["facturado"].get("memoryGBHour", 0) for p in lanzados), 3
            ),
            "disco_gb_h": round(
                sum(p["facturado"].get("storageGBHour", 0) for p in lanzados), 3
            ),
            "costo_usd": round(sum(p["costo_usd"] for p in lanzados), 4),
        },
        "pasos": pasos,
    }
    if args.raw_bytes is not None:
        reporte["raw_gb_en_bucket"] = round(args.raw_bytes / _GB, 2)
    args.salida.parent.mkdir(parents=True, exist_ok=True)
    args.salida.write_text(
        json.dumps(reporte, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(
        f"{args.corrida}: completa={reporte['completa']} costo=US$ {reporte['total']['costo_usd']} -> {args.salida}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
