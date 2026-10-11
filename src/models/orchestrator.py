"""Orquestador automatizado de experimentos para modelos SSCO (Contrato C5).

Permite:
- Cargar y ejecutar la matriz oficial de experimentos (E1..E6) definida en YAML.
- Ejecutar en modo runner local (para CI/CD ultrarrápido sin costo) o despachar
  a AWS SageMaker (para corridas oficiales en producción).
- Consolidar métricas de validación cruzada (5 folds) y PR-AUC.
- Generar un Leaderboard en Markdown y JSON con selección automática del modelo
  campeón siguiendo la regla de decisión metodológica del proyecto.
"""

import argparse
import json
import os
import sys
import time
from typing import Any

import numpy as np
import yaml

# Imports de modelos
try:
    from src.models.launch_manual import lanzar_training_job_manual
    from src.models.train import main as train_main
except ImportError:
    from launch_manual import (  # type: ignore[import-not-found,no-redef]
        lanzar_training_job_manual,
    )
    from train import main as train_main  # type: ignore[import-not-found,no-redef]


def cargar_configuracion(config_path: str) -> dict[str, Any]:
    """Carga y valida el archivo de configuración YAML de experimentos."""
    if not os.path.isfile(config_path):
        raise FileNotFoundError(
            f"Archivo de configuración no encontrado: {config_path}"
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict) or "experiments" not in config:
        raise ValueError("El archivo YAML debe contener la clave raíz 'experiments'.")

    return config


def filtrar_experimentos(
    experimentos: list[dict[str, Any]], filtro: str
) -> list[dict[str, Any]]:
    """Filtra los experimentos según la lista especificada (e.g. 'ALL' o 'E1,E2')."""
    if not filtro or filtro.strip().upper() == "ALL":
        return experimentos

    ids_permitidos = {x.strip().upper() for x in filtro.split(",") if x.strip()}
    seleccionados = [
        e for e in experimentos if str(e.get("id", "")).upper() in ids_permitidos
    ]

    if not seleccionados:
        raise ValueError(
            f"Ningún experimento coincide con el filtro '{filtro}'. "
            f"IDs disponibles: {[e.get('id') for e in experimentos]}"
        )
    return seleccionados


def validar_dataset_entrada(dataset_path: str) -> str | None:
    """Devuelve un mensaje de error si no hay un dataset real en la ruta; None si es válido."""
    if not dataset_path:
        return "Modo runner requiere --train_dir con el dataset real (no se generan datos)."
    if not os.path.isdir(dataset_path):
        return f"El directorio de entrada no existe: {dataset_path}"
    hay_parquet = any(
        f.endswith(".parquet") for _, _, files in os.walk(dataset_path) for f in files
    )
    if not hay_parquet:
        return f"No se encontraron archivos .parquet en: {dataset_path}"
    return None


def ejecutar_experimento_runner(
    exp: dict[str, Any],
    dataset_path: str,
    output_base_dir: str,
    seed: int,
    dataset_version: str,
) -> dict[str, Any]:
    """Ejecuta un experimento directamente en el entorno de cómputo local/runner."""
    exp_id = exp["id"]
    model_name = exp["model_name"]
    variant = exp.get("variant", "V2")
    usa_pesos = exp.get("usa_pesos", "si")

    pesos_suffix = "w1" if usa_pesos == "si" else "w0"
    run_id = f"ssco-run-{exp_id}-{model_name.lower()}-{variant.lower()}-{pesos_suffix}"

    model_dir = os.path.join(output_base_dir, "models", run_id)
    preds_dir = os.path.join(output_base_dir, "predictions")
    records_dir = os.path.join(output_base_dir, "run_records")
    os.makedirs(model_dir, exist_ok=True)
    os.makedirs(preds_dir, exist_ok=True)
    os.makedirs(records_dir, exist_ok=True)

    preds_file = os.path.join(preds_dir, f"predictions_{run_id}.parquet")
    record_file = os.path.join(records_dir, f"run_record_{run_id}.json")

    cli_args = [
        "--train_dir",
        dataset_path,
        "--model_dir",
        model_dir,
        "--output_dir",
        output_base_dir,
        "--run_id",
        run_id,
        "--model_name",
        model_name,
        "--variant",
        variant,
        "--usa_pesos",
        usa_pesos,
        "--dataset_version",
        dataset_version,
        "--seed",
        str(seed),
        "--ruta_salida_predicciones",
        preds_file,
        "--ruta_salida_run_record",
        record_file,
    ]

    param_grid = exp.get("hyperparameters", {}).get("param_grid")
    if param_grid and isinstance(param_grid, dict):
        cli_args.extend(["--param_grid_json", json.dumps(param_grid)])

    t0 = time.time()
    try:
        ret = train_main(cli_args)
        elapsed = round(time.time() - t0, 2)
        if ret != 0:
            return {
                "id": exp_id,
                "run_id": run_id,
                "model_name": model_name,
                "variant": variant,
                "usa_pesos": usa_pesos,
                "status": "FAILED",
                "error": f"train_main retornó código {ret}",
                "train_seconds": elapsed,
            }

        # Leer run_record.json generado
        with open(record_file, "r", encoding="utf-8") as f:
            record_data = json.loads(f.readline().strip())

        metricas_cv = record_data.get("metricas_cv", {})
        cv_mean = float(metricas_cv.get("cv_pr_auc_mean", 0.0) or 0.0)
        train_pr_auc = float(metricas_cv.get("train_pr_auc", 0.0) or 0.0)
        raw_folds = metricas_cv.get(
            "cv_pr_auc_folds", metricas_cv.get("cv_pr_auc_fold")
        )
        if isinstance(raw_folds, list):
            fold_scores = [float(x) for x in raw_folds if x is not None]
        else:
            fold_scores = []
        if not fold_scores:
            fold_scores = [
                float(metricas_cv[f"cv_pr_auc_fold_{i}"])
                for i in range(5)
                if metricas_cv.get(f"cv_pr_auc_fold_{i}") is not None
            ]
        cv_std = float(np.std(fold_scores)) if fold_scores else 0.0

        best_params = (
            record_data.get("hiperparametros_ganadores")
            or record_data.get("hiperparametros_optimos")
            or {}
        )

        return {
            "id": exp_id,
            "run_id": run_id,
            "model_name": model_name,
            "variant": variant,
            "usa_pesos": usa_pesos,
            "priority": exp.get("priority", "MUST"),
            "description": exp.get("description", ""),
            "status": "SUCCESS",
            "cv_pr_auc_mean": cv_mean,
            "cv_pr_auc_std": cv_std,
            "cv_folds": fold_scores,
            "train_pr_auc": train_pr_auc,
            "overfit_gap": round(train_pr_auc - cv_mean, 4),
            "train_seconds": elapsed,
            "best_params": best_params,
            "record_file": record_file,
            "preds_file": preds_file,
        }
    except Exception as err:  # noqa: BLE001
        return {
            "id": exp_id,
            "run_id": run_id,
            "model_name": model_name,
            "variant": variant,
            "usa_pesos": usa_pesos,
            "status": "FAILED",
            "error": str(err),
            "train_seconds": round(time.time() - t0, 2),
        }


def ejecutar_experimento_sagemaker(
    exp: dict[str, Any],
    gold_bucket: str,
    dataset_version: str,
    role_arn: str | None = None,
    instance_type: str = "ml.m5.xlarge",
) -> dict[str, Any]:
    """Despacha un experimento como SageMaker Training Job en AWS."""
    exp_id = exp["id"]
    model_name = exp["model_name"]
    variant = exp.get("variant", "V2")
    usa_pesos = exp.get("usa_pesos", "si")

    pesos_suffix = "w1" if usa_pesos == "si" else "w0"
    run_id = f"ssco-run-{exp_id}-{model_name.lower()}-{variant.lower()}-{pesos_suffix}"

    try:
        resp = lanzar_training_job_manual(
            run_id=run_id,
            model_name=model_name,
            variant=variant,
            usa_pesos=usa_pesos,
            dataset_version=dataset_version,
            instance_type=instance_type,
            role_arn=role_arn,
            gold_bucket=gold_bucket,
        )
        job_arn = resp.get("TrainingJobArn", "N/A")
        return {
            "id": exp_id,
            "run_id": run_id,
            "model_name": model_name,
            "variant": variant,
            "usa_pesos": usa_pesos,
            "priority": exp.get("priority", "MUST"),
            "description": exp.get("description", ""),
            "status": "DISPATCHED",
            "job_arn": job_arn,
            "note": "SageMaker Training Job creado exitosamente en AWS.",
        }
    except Exception as err:  # noqa: BLE001
        return {
            "id": exp_id,
            "run_id": run_id,
            "model_name": model_name,
            "variant": variant,
            "usa_pesos": usa_pesos,
            "status": "FAILED",
            "error": str(err),
        }


def seleccionar_modelo_campeon(
    resultados: list[dict[str, Any]],
) -> dict[str, Any]:
    """Selecciona el modelo campeón conforme a la Regla de Decisión del proyecto.

    Regla Oficial (informe_modelado_ssco.md Sección 4.2.3.F):
    1. Se comparan exclusivamente los candidatos V2 con pesos balanceados: la mejor
       configuración de LR (E1 y variantes) contra la mejor de DT (E2 y variantes).
    2. Si cv_pr_auc_mean(LR) >= cv_pr_auc_mean(DT): Campeón es LR por parsimonia y estabilidad.
    3. Si cv_pr_auc_mean(DT) > cv_pr_auc_mean(LR): Campeón es DT.
    4. En empate estadístico, rige el principio de la Navaja de Ockham (LR).
    """
    candidatos_oficiales = [
        r
        for r in resultados
        if r.get("status") == "SUCCESS"
        and r.get("variant") == "V2"
        and r.get("usa_pesos") == "si"
    ]

    def _mejor_de(familia: str) -> dict[str, Any] | None:
        de_familia = [r for r in candidatos_oficiales if r.get("model_name") == familia]
        return max(de_familia, key=lambda r: r.get("cv_pr_auc_mean", 0.0), default=None)

    # Cada familia compite con su mejor configuración V2 con pesos (E1/E2 y variantes).
    r_lr = _mejor_de("LR")
    r_dt = _mejor_de("DT")

    if r_lr and r_dt:
        score_lr = r_lr.get("cv_pr_auc_mean", 0.0)
        score_dt = r_dt.get("cv_pr_auc_mean", 0.0)
        diff = round(score_lr - score_dt, 4)

        if score_lr >= score_dt:
            campeon = r_lr
            retador = r_dt
            motivo = (
                f"Regresión Logística ({r_lr.get('id')}) supera o iguala a Árbol de Decisión ({r_dt.get('id')}) en PR-AUC "
                f"({score_lr:.4f} vs {score_dt:.4f}, diff = +{diff:.4f}). "
                f"Se selecciona LR como Modelo Oficial por estabilidad asintótica, menor sobreajuste "
                f"y el principio de parsimonia (Navaja de Ockham)."
            )
        else:
            campeon = r_dt
            retador = r_lr
            motivo = (
                f"Árbol de Decisión ({r_dt.get('id')}) supera a Regresión Logística ({r_lr.get('id')}) en PR-AUC "
                f"({score_dt:.4f} vs {score_lr:.4f}, diff = +{abs(diff):.4f}). "
                f"Se selecciona DT por mayor capacidad no lineal en la captura de no-linealidades tributarias."
            )
        return {
            "campeon_id": campeon.get("id"),
            "campeon_model": campeon.get("model_name"),
            "campeon_pr_auc": campeon.get("cv_pr_auc_mean"),
            "retador_id": retador.get("id"),
            "retador_model": retador.get("model_name"),
            "retador_pr_auc": retador.get("cv_pr_auc_mean"),
            "justificacion": motivo,
        }

    exitosos = [r for r in resultados if r.get("status") == "SUCCESS"]
    if exitosos:
        mejor = max(exitosos, key=lambda x: x.get("cv_pr_auc_mean", 0.0))
        return {
            "campeon_id": mejor.get("id"),
            "campeon_model": mejor.get("model_name"),
            "campeon_pr_auc": mejor.get("cv_pr_auc_mean"),
            "retador_id": "N/A",
            "retador_model": "N/A",
            "retador_pr_auc": 0.0,
            "justificacion": f"Mejor modelo disponible por PR-AUC ({mejor.get('cv_pr_auc_mean'):.4f}).",
        }

    return {
        "campeon_id": "N/A",
        "campeon_model": "N/A",
        "campeon_pr_auc": 0.0,
        "retador_id": "N/A",
        "retador_model": "N/A",
        "retador_pr_auc": 0.0,
        "justificacion": "No se encontraron ejecuciones exitosas para determinar un campeón.",
    }


def generar_leaderboard_markdown(
    resultados: list[dict[str, Any]],
    campeon_info: dict[str, Any],
    dataset_version: str,
) -> str:
    """Genera una tabla comparativa formal en Markdown para GitHub Step Summary y reportes."""
    # Ordenar por PR-AUC descendente
    ordenados = sorted(
        resultados,
        key=lambda x: (x.get("status") == "SUCCESS", x.get("cv_pr_auc_mean", 0.0)),
        reverse=True,
    )

    lines = [
        "# Leaderboard Oficial de Experimentos SSCO — SUNAT Risk Scoring",
        "",
        f"**Versión de Dataset:** `{dataset_version}` | **Métrica Primaria:** `PR-AUC (5-Fold CV)`",
        "",
        "| Rank | ID | Algoritmo | Variante | Pesos | CV PR-AUC (Media ± Desv) | Train PR-AUC | Overfit Gap | Tiempo (s) | Rol / Propósito |",
        "| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |",
    ]

    for rank, r in enumerate(ordenados, 1):
        r_id = r.get("id", "N/A")
        model = r.get("model_name", "N/A")
        variant = r.get("variant", "N/A")
        pesos = r.get("usa_pesos", "N/A")
        status = r.get("status", "N/A")

        if status == "SUCCESS":
            cv_mean = r.get("cv_pr_auc_mean", 0.0)
            cv_std = r.get("cv_pr_auc_std", 0.0)
            train_auc = r.get("train_pr_auc", 0.0)
            gap = r.get("overfit_gap", 0.0)
            t_sec = r.get("train_seconds", 0.0)

            es_campeon = r_id == campeon_info.get("campeon_id")
            badge = (
                "🏆 **CHAMPION**"
                if es_campeon
                else (
                    "Candidato Oficial"
                    if variant == "V2" and pesos == "si"
                    else ("Benchmark Techo" if variant == "V1" else "Ablación")
                )
            )

            lines.append(
                f"| {rank} | `{r_id}` | **{model}** | `{variant}` | `{pesos}` | **{cv_mean:.4f}** (±{cv_std:.3f}) | {train_auc:.4f} | {gap:+.4f} | {t_sec:.1f}s | {badge} |"
            )
        elif status == "DISPATCHED":
            lines.append(
                f"| {rank} | `{r_id}` | **{model}** | `{variant}` | `{pesos}` | *En ejecución (SageMaker)* | - | - | - | AWS Job Despachado |"
            )
        else:
            lines.append(
                f"| {rank} | `{r_id}` | **{model}** | `{variant}` | `{pesos}` | *FALLÓ* | - | - | - | Error: {r.get('error', 'Desconocido')} |"
            )

    lines.extend(
        [
            "",
            "## Justificación de Selección de Modelo",
            "",
            "> [!IMPORTANT]",
            f"> **Modelo Campeón Seleccionado:** `{campeon_info.get('campeon_model')}` (Experimento `{campeon_info.get('campeon_id')}`)",
            "> ",
            f"> {campeon_info.get('justificacion')}",
            "",
        ]
    )

    return "\n".join(lines)


def main(args_list: list[str] | None = None) -> int:
    """Punto de entrada CLI para el orquestador de experimentos."""
    parser = argparse.ArgumentParser(
        description="Orquestador Automatizado de Experimentos SSCO"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/experiments.yml",
        help="Ruta al archivo YAML de experimentos",
    )
    parser.add_argument(
        "--target",
        type=str,
        choices=["runner", "sagemaker"],
        default="runner",
        help="Entorno de ejecución: runner (local/CI) o sagemaker (AWS)",
    )
    parser.add_argument(
        "--experiments",
        type=str,
        default="ALL",
        help="Filtro de experimentos: ALL o lista separada por comas (e.g. E1,E2)",
    )
    parser.add_argument(
        "--train_dir",
        type=str,
        default="",
        help="Ruta o URI S3 al dataset de entrada",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="./data/output/orchestrator",
        help="Directorio base para guardar salidas y artefactos",
    )
    parser.add_argument(
        "--gold_bucket",
        type=str,
        default=os.getenv("GOLD_BUCKET", "sunat-risk-scoring-gold"),
        help="Bucket S3 Gold para artefactos de SageMaker",
    )
    parser.add_argument(
        "--role_arn",
        type=str,
        default=os.getenv("SAGEMAKER_ROLE_ARN", ""),
        help="Rol IAM de ejecución para SageMaker",
    )
    parser.add_argument(
        "--output_summary_file",
        type=str,
        default="",
        help="Ruta de archivo para guardar el reporte Markdown (e.g. GITHUB_STEP_SUMMARY)",
    )

    args = parser.parse_args(args_list)

    print(f"[INFO] Cargando configuración desde: {args.config}")
    config = cargar_configuracion(args.config)

    dataset_version = str(config.get("dataset_version", "v1"))
    seed = int(config.get("seed", 42))
    todos_exp = config.get("experiments", [])

    seleccionados = filtrar_experimentos(todos_exp, args.experiments)
    print(
        f"[INFO] Experimentos a ejecutar ({len(seleccionados)}): {[e['id'] for e in seleccionados]}"
    )
    print(f"[INFO] Target de ejecución: {args.target}")

    dataset_path = args.train_dir
    if args.target == "runner":
        error_dataset = validar_dataset_entrada(dataset_path)
        if error_dataset:
            print(f"[ERROR] {error_dataset}", file=sys.stderr)
            return 2

    resultados: list[dict[str, Any]] = []

    for exp in seleccionados:
        exp_id = exp["id"]
        print("\n=======================================================")
        print(f"  Ejecutando Experimento {exp_id}: {exp.get('description', '')}")
        print("=======================================================")

        if args.target == "runner":
            res = ejecutar_experimento_runner(
                exp=exp,
                dataset_path=dataset_path,
                output_base_dir=args.output_dir,
                seed=seed,
                dataset_version=dataset_version,
            )
        else:
            res = ejecutar_experimento_sagemaker(
                exp=exp,
                gold_bucket=args.gold_bucket,
                dataset_version=dataset_version,
                role_arn=args.role_arn,
            )
        resultados.append(res)
        print(f"[STATUS] Experimento {exp_id} -> {res.get('status')}")

    # Determinar Campeón y Leaderboard
    campeon_info = seleccionar_modelo_campeon(resultados)
    md_summary = generar_leaderboard_markdown(resultados, campeon_info, dataset_version)

    print("\n" + "=" * 60)
    print("                 LEADERBOARD FINAL")
    print("=" * 60)
    print(md_summary)

    # Guardar Leaderboard en JSON y Markdown
    os.makedirs(args.output_dir, exist_ok=True)
    json_path = os.path.join(args.output_dir, "leaderboard.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "dataset_version": dataset_version,
                "campeon": campeon_info,
                "experimentos": resultados,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    leaderboard_md_path = os.path.join(args.output_dir, "leaderboard.md")
    with open(leaderboard_md_path, "w", encoding="utf-8") as f:
        f.write(md_summary)

    summary_file = args.output_summary_file or os.getenv("GITHUB_STEP_SUMMARY", "")
    if summary_file and os.path.abspath(summary_file) != os.path.abspath(
        leaderboard_md_path
    ):
        summary_dir = os.path.dirname(os.path.abspath(summary_file))
        if summary_dir:
            os.makedirs(summary_dir, exist_ok=True)
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(md_summary)
        print(
            f"\n[INFO] Leaderboard guardado en: {leaderboard_md_path}, {summary_file} y {json_path}"
        )
    else:
        print(f"\n[INFO] Leaderboard guardado en: {leaderboard_md_path} y {json_path}")

    # Si algún experimento falló, retornar 1
    if any(r.get("status") == "FAILED" for r in resultados):
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
