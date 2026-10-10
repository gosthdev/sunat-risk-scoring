#!/usr/bin/env python3
"""Consolidador de resultados de ejecuciones de rendimiento Spark.

Procesa los archivos JSON obtenidos de aws emr-serverless get-job-run,
calcula métricas de ejecución, consumo de recursos (vCPU-hora) y aplica
la regla de decisión:
"Gana una condición solo si el peor tiempo del ganador es menor que el
mejor del perdedor; si no, es 'no concluyente'".
"""

import argparse
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Any


def parse_job_run_json(file_path: Path) -> dict[str, Any]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    job_run = data.get("jobRun", data)
    tags = job_run.get("tags", {})
    utilization = job_run.get("totalResourceUtilization", {})

    # Duración total de ejecución reportada por EMR (segundos)
    total_exec_time = (
        job_run.get("totalExecutionDurationSeconds")
        or job_run.get("totalExecutionTime")
        or 0
    )

    vcpu = utilization.get("vCPUHour") or utilization.get("vCPUHours") or 0.0
    mem = utilization.get("memoryGBHour") or utilization.get("memoryGBHours") or 0.0
    storage = (
        utilization.get("storageGBHour") or utilization.get("storageGBHours") or 0.0
    )

    name = job_run.get("name", "unknown")
    condition = tags.get("Condition")
    if not condition or condition.lower() == "unknown":
        if "warmup" in name.lower():
            condition = "warmup"
        elif "make-layouts" in name.lower() or "make_layouts" in name.lower():
            condition = "setup"
        elif (
            "compare-outputs" in name.lower()
            or "compare_outputs" in name.lower()
            or "count-layers" in name.lower()
            or "audit-layers" in name.lower()
        ):
            condition = "utility"
        else:
            match = re.search(r"[-_]([A-Za-z]+)\d*$", name)
            if match:
                token = match.group(1).upper()
                if token in ("LAYOUTS", "OUTPUTS", "LAYERS"):
                    condition = "utility"
                else:
                    condition = token
            else:
                condition = "unknown"

    return {
        "job_run_id": job_run.get("jobRunId", file_path.stem),
        "name": name,
        "state": job_run.get("state", "UNKNOWN"),
        "created_at": str(job_run.get("createdAt", "")),
        "total_execution_seconds": total_exec_time,
        "vcpu_hours": round(float(vcpu), 4),
        "memory_gb_hours": round(float(mem), 4),
        "storage_gb_hours": round(float(storage), 4),
        "git_sha": tags.get("GitSHA", "unknown"),
        "profile": tags.get("Profile", "unknown"),
        "condition": condition,
        "file": str(file_path),
    }


def analyze_runs(
    runs: list[dict[str, Any]], discard_warmup: bool = True
) -> dict[str, Any]:
    if not runs:
        return {"error": "No se encontraron ejecuciones para analizar."}

    # Descartar calentamiento y jobs utilitarios/setup de la comparación
    ignored_conditions = {"setup", "utility", "unknown"}
    if discard_warmup:
        ignored_conditions.add("warmup")

    active_runs = [
        r
        for r in runs
        if r["condition"].lower() not in ignored_conditions
        and (not discard_warmup or "warmup" not in r["name"].lower())
    ]

    if not active_runs:
        return {"error": "Solo se encontraron corridas de calentamiento o utilitarias."}

    # Separar por condición (ej. A vs B, o N vs P)
    by_condition: dict[str, list[dict[str, Any]]] = {}
    for r in active_runs:
        by_condition.setdefault(r["condition"], []).append(r)

    results: dict[str, Any] = {"conditions": {}, "comparison": None}

    stats_by_cond: dict[str, dict[str, Any]] = {}

    for cond, cond_runs in by_condition.items():
        successful_runs = [
            r for r in cond_runs if r.get("state", "SUCCESS") == "SUCCESS"
        ]
        failed_runs = [
            r for r in cond_runs if r.get("state", "SUCCESS") != "SUCCESS"
        ]
        times = [
            r["total_execution_seconds"]
            for r in successful_runs
            if r["total_execution_seconds"] > 0
        ]
        vcpus = [r["vcpu_hours"] for r in successful_runs]

        if times:
            stats_by_cond[cond] = {
                "count": len(times),
                "total": len(cond_runs),
                "failed": len(failed_runs),
                "min": min(times),
                "max": max(times),
                "mean": round(statistics.mean(times), 2),
                "median": round(statistics.median(times), 2),
                "avg_vcpu_h": round(statistics.mean(vcpus), 4) if vcpus else 0.0,
            }
        else:
            stats_by_cond[cond] = {
                "count": 0,
                "total": len(cond_runs),
                "failed": len(failed_runs),
                "min": 0,
                "max": 0,
                "mean": 0.0,
                "median": 0.0,
                "avg_vcpu_h": 0.0,
            }

        results["conditions"][cond] = {
            "all_runs": cond_runs,
            "measured_runs": successful_runs,
            "failed_runs": failed_runs,
            "stats": stats_by_cond.get(cond, {}),
        }

    # Comparación si hay exactamente 2 condiciones
    if len(stats_by_cond) == 2:
        cond_names = list(stats_by_cond.keys())
        c1, c2 = cond_names[0], cond_names[1]
        s1, s2 = stats_by_cond[c1], stats_by_cond[c2]

        if s1["count"] == 0 or s2["count"] == 0:
            failed_conds = [c for c in (c1, c2) if stats_by_cond[c]["count"] == 0]
            results["comparison"] = {
                "winner_candidate": None,
                "loser_candidate": None,
                "winner_max": None,
                "loser_min": None,
                "conclusive": False,
                "verdict": f"NO CONCLUYENTE (sin corridas exitosas en: {', '.join(failed_conds)})",
                "speedup_median": 1.0,
            }
        else:
            # Determinar candidato ganador por mediana más baja
            if s1["median"] < s2["median"]:
                winner, loser = c1, c2
                s_winner, s_loser = s1, s2
            else:
                winner, loser = c2, c1
                s_winner, s_loser = s2, s1

            # Criterio estricto: peor tiempo del ganador < mejor tiempo del perdedor
            conclusive = s_winner["max"] < s_loser["min"]
            verdict = (
                f"GANA {winner}"
                if conclusive
                else "NO CONCLUYENTE (solapamiento de rangos)"
            )

            speedup = (
                round(s_loser["median"] / s_winner["median"], 2)
                if s_winner["median"] > 0
                else 1.0
            )

            results["comparison"] = {
                "winner_candidate": winner,
                "loser_candidate": loser,
                "winner_max": s_winner["max"],
                "loser_min": s_loser["min"],
                "conclusive": conclusive,
                "verdict": verdict,
                "speedup_median": speedup,
            }

    return results


def format_markdown_table(analysis: dict[str, Any]) -> str:
    lines = []
    lines.append("### Resumen Consolidado de Rendimiento\n")
    lines.append(
        "| Condición | Exitosas / Total | Mín (s) | Máx (s) | Mediana (s) | Promedio (s) | vCPU-h Prom |"
    )
    lines.append("|:---|:---:|:---:|:---:|:---:|:---:|:---:|")

    for cond, data in analysis.get("conditions", {}).items():
        s = data.get("stats", {})
        if s:
            count_str = f"{s.get('count', 0)} / {s.get('total', s.get('count', 0))}"
            if s.get("count", 0) > 0:
                lines.append(
                    f"| **{cond}** | {count_str} | {s.get('min', 0)} | {s.get('max', 0)} | "
                    f"{s.get('median', 0)} | {s.get('mean', 0)} | {s.get('avg_vcpu_h', 0)} |"
                )
            else:
                lines.append(
                    f"| **{cond}** | {count_str} | - | - | - | - | - |"
                )

    comp = analysis.get("comparison")
    if comp:
        lines.append("\n#### Evaluación de Criterio de Decisión:")
        lines.append(f"- **Candidato Ganador**: {comp['winner_candidate']}")
        lines.append(f"- **Peor tiempo del ganador**: {comp['winner_max']} s")
        lines.append(f"- **Mejor tiempo del perdedor**: {comp['loser_min']} s")
        lines.append(f"- **Aceleración (mediana)**: {comp['speedup_median']}x")
        lines.append(f"- **Veredicto Final**: **{comp['verdict']}**\n")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Consolida y compara corridas EMR Serverless."
    )
    parser.add_argument(
        "json_files", nargs="+", help="Rutas o glob a archivos get-job-run JSON"
    )
    parser.add_argument(
        "--include-warmup", action="store_true", help="No descartar la primera corrida"
    )
    parser.add_argument(
        "--output-json", help="Ruta de guardado para el análisis en formato JSON"
    )
    args = parser.parse_args()

    files = [Path(p) for p in args.json_files if Path(p).is_file()]
    if not files:
        print("ERROR: No se especificaron archivos válidos.", file=sys.stderr)
        sys.exit(1)

    runs = [parse_job_run_json(f) for f in files]
    analysis = analyze_runs(runs, discard_warmup=not args.include_warmup)

    md_report = format_markdown_table(analysis)
    print(md_report)

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as out:
            json.dump(analysis, out, indent=2)
        print(f"✓ Análisis JSON exportado a: {args.output_json}")


if __name__ == "__main__":
    main()
