"""Orquestador automatizado de experimentos para modelos SSCO (Contrato C5).

Permite:
- Cargar y ejecutar la matriz oficial de experimentos (E1..E6) definida en YAML.
- Ejecutar en modo runner local (para CI/CD ultrarrápido sin costo) o despachar
  a AWS SageMaker (para corridas oficiales en producción).
- Generar datos sintéticos para pruebas deterministas y validación en entornos CI.
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
import pandas as pd
import yaml

# Imports de modelos
try:
    from src.models.launch_manual import lanzar_training_job_manual
    from src.models.train import main as train_main
except ImportError:
    from launch_manual import (
        lanzar_training_job_manual,  # type: ignore[import-not-found,no-redef]
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


def generar_dataset_sintetico(
    output_dir: str, n_rows: int = 500, seed: int = 42
) -> str:
    """Genera un archivo Parquet sintético reproducible para pruebas de runner o CI."""
    os.makedirs(output_dir, exist_ok=True)
    target_parquet = os.path.join(output_dir, "dataset_scoring_synthetic.parquet")

    np.random.seed(seed)
    n_train = int(n_rows * 0.8)

    rucs = [f"{20100000000 + i:011d}" for i in range(1, n_rows + 1)]
    splits = ["train" if i < n_train else "test" for i in range(n_rows)]
    folds = [(i % 5) if splits[i] == "train" else -1 for i in range(n_rows)]
    # Prevalencia baja pero con suficientes positivos para 5 folds
    labels = [1 if (i % 8 == 0) else 0 for i in range(n_rows)]

    montos = np.random.exponential(scale=35000, size=n_rows)
    montos[::9] = np.nan
    medianas = montos / np.random.randint(1, 8, size=n_rows)
    medianas[::7] = np.nan

    n_ordenes = np.random.poisson(lam=5, size=n_rows)
    antiguedad = np.random.randint(1, 120, size=n_rows)
    n_trabajadores = np.random.choice(
        [0, 1, 3, 10, 50], size=n_rows, p=[0.4, 0.3, 0.15, 0.1, 0.05]
    )
    sin_trabajadores = (n_trabajadores == 0).astype(int)

    ciius = np.random.choice(["4659", "4100", "4711", "9999", ""], size=n_rows)
    depts = np.random.choice(["LIMA", "AREQUIPA", "CUSCO", "LORETO", None], size=n_rows)
    tipos = np.random.choice(["SOCIEDAD ANONIMA", "PERSONA NATURAL"], size=n_rows)
    estados = np.random.choice(
        ["ACTIVO", "BAJA DE OFICIO", "SUSPENSION TEMPORAL"], size=n_rows
    )
    condiciones = np.random.choice(["HABIDO", "NO HABIDO", "NO HALLADO"], size=n_rows)

    df = pd.DataFrame(
        {
            "ruc": rucs,
            "split": splits,
            "fold": folds,
            "label": labels,
            "monto_total_soles": montos,
            "monto_mediano_soles": medianas,
            "monto_maximo_soles": montos * 1.2,
            "n_ordenes": n_ordenes,
            "total_ordenes_validas": n_ordenes,
            "n_entidades_distintas": np.maximum(1, n_ordenes // 2),
            "pct_monto_en_entidad_principal": np.random.uniform(0.1, 1.0, size=n_rows),
            "pct_ordenes_anuladas": np.random.uniform(0.0, 0.2, size=n_rows),
            "antiguedad_contratacion_estado_dias": antiguedad * 30,
            "antiguedad_meses": antiguedad,
            "nro_trabajadores": n_trabajadores,
            "sin_trabajadores": sin_trabajadores,
            "monto_por_trabajador": montos / np.maximum(1, n_trabajadores),
            "informalidad_epen_departamento": np.random.uniform(
                50.0, 85.0, size=n_rows
            ),
            "ciiu_principal": ciius,
            "actividad_economica_principal": ciius,
            "departamento": depts,
            "tipo_contribuyente": tipos,
            "Estado": estados,
            "Condicion": condiciones,
        }
    )

    df.to_parquet(target_parquet, index=False)
    return target_parquet


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
        cv_mean = float(metricas_cv.get("cv_pr_auc_mean", 0.0))
        train_pr_auc = float(metricas_cv.get("train_pr_auc", 0.0))
        fold_scores = [
            float(metricas_cv[f"cv_pr_auc_fold_{i}"])
            for i in range(5)
            if f"cv_pr_auc_fold_{i}" in metricas_cv
        ]
        cv_std = float(np.std(fold_scores)) if fold_scores else 0.0

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
            "best_params": record_data.get("hiperparametros_optimos", {}),
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
    1. Se comparan exclusivamente los candidatos V2 con pesos balanceados (E1 vs E2).
    2. Si cv_pr_auc_mean(LR) >= cv_pr_auc_mean(DT): Campeón es LR (E1) por parsimonia y estabilidad.
    3. Si cv_pr_auc_mean(DT) > cv_pr_auc_mean(LR): Campeón es DT (E2).
    4. En empate estadístico, rige el principio de la Navaja de Ockham (LR).
    """
    candidatos_oficiales = [
        r
        for r in resultados
        if r.get("status") == "SUCCESS"
        and r.get("variant") == "V2"
        and r.get("usa_pesos") == "si"
    ]

    r_lr = next((r for r in candidatos_oficiales if r.get("model_name") == "LR"), None)
    r_dt = next((r for r in candidatos_oficiales if r.get("model_name") == "DT"), None)

    if r_lr and r_dt:
        score_lr = r_lr.get("cv_pr_auc_mean", 0.0)
        score_dt = r_dt.get("cv_pr_auc_mean", 0.0)
        diff = round(score_lr - score_dt, 4)

        if score_lr >= score_dt:
            campeon = r_lr
            retador = r_dt
            motivo = (
                f"Regresión Logística (E1) supera o iguala a Árbol de Decisión (E2) en PR-AUC "
                f"({score_lr:.4f} vs {score_dt:.4f}, diff = +{diff:.4f}). "
                f"Se selecciona LR como Modelo Oficial por estabilidad asintótica, menor sobreajuste "
                f"y el principio de parsimonia (Navaja de Ockham)."
            )
        else:
            campeon = r_dt
            retador = r_lr
            motivo = (
                f"Árbol de Decisión (E2) supera a Regresión Logística (E1) en PR-AUC "
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
        "--synthetic",
        action="store_true",
        help="Generar y usar dataset sintético para pruebas rápidas y deterministas",
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

    dataset_version = str(config.get("dataset_version", "v01"))
    seed = int(config.get("seed", 42))
    todos_exp = config.get("experiments", [])

    seleccionados = filtrar_experimentos(todos_exp, args.experiments)
    print(
        f"[INFO] Experimentos a ejecutar ({len(seleccionados)}): {[e['id'] for e in seleccionados]}"
    )
    print(f"[INFO] Target de ejecución: {args.target}")

    dataset_path = args.train_dir
    if args.synthetic or (not dataset_path and args.target == "runner"):
        synthetic_dir = os.path.join(args.output_dir, "synthetic_dataset")
        print(f"[INFO] Generando dataset sintético en: {synthetic_dir}")
        dataset_path = generar_dataset_sintetico(synthetic_dir, n_rows=600, seed=seed)
        print(f"[SUCCESS] Dataset sintético generado en: {dataset_path}")

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

    summary_file = args.output_summary_file or os.getenv("GITHUB_STEP_SUMMARY", "")
    if not summary_file:
        summary_file = os.path.join(args.output_dir, "leaderboard.md")

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(md_summary)
    print(f"\n[INFO] Leaderboard guardado en: {summary_file} y {json_path}")

    # Si algún experimento falló, retornar 1
    if any(r.get("status") == "FAILED" for r in resultados):
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
