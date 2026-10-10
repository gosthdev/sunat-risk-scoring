"""Auditor de capas: cuenta filas, RUCs distintos y calcula checksums.

Usa las constantes de s3_paths para inspeccionar las capas bronze, silver
y gold de acuerdo con la configuración de entorno activa.
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

# Configurar sys.path
_ROOT = Path(__file__).resolve().parent.parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)

import s3_paths


def audit_path(spark, path_uri: str, layer_name: str) -> dict[str, Any]:
    print(f"[s_count_layers] Auditando {layer_name}: {path_uri} ...")
    try:
        df = spark.read.parquet(path_uri)
    except Exception as e:  # noqa: BLE001
        print(f"  (No se pudo leer {path_uri}: {e})")
        return {
            "layer": layer_name,
            "path": path_uri,
            "exists": False,
            "error": str(e),
        }

    total_rows = df.count()
    if total_rows == 0:
        return {
            "layer": layer_name,
            "path": path_uri,
            "exists": True,
            "total_rows": 0,
            "distinct_rucs": 0,
            "checksum": "EMPTY",
        }

    # Detectar columna RUC
    ruc_col = None
    for cand in ["RUC", "ruc", "ruc_entidad", "ruc_contratista"]:
        if cand in df.columns:
            ruc_col = cand
            break

    distinct_rucs = df.select(ruc_col).distinct().count() if ruc_col else None

    # Checksum resumido
    hasher = hashlib.md5()
    # Tomar muestra de 1000 filas o agregados de columnas numéricas para checksum representativo
    summary_data = (
        f"rows:{total_rows}|rucs:{distinct_rucs}|cols:{','.join(sorted(df.columns))}"
    )
    hasher.update(summary_data.encode("utf-8"))
    checksum = hasher.hexdigest()

    return {
        "layer": layer_name,
        "path": path_uri,
        "exists": True,
        "total_rows": total_rows,
        "distinct_rucs": distinct_rucs,
        "columns": df.columns,
        "checksum": checksum,
    }


def audit_all_layers(spark, target_layer: str = "all") -> dict[str, Any]:
    targets = {
        "bronze_padron": (s3_paths.BRONZE_PADRON_RUC, "bronze"),
        "bronze_padron_quarantine": (s3_paths.BRONZE_PADRON_RUC_QUARANTINE, "bronze"),
        "bronze_ordenes": (s3_paths.BRONZE_ORDENES_COMPRA, "bronze"),
        "bronze_ordenes_quarantine": (
            s3_paths.BRONZE_ORDENES_COMPRA_QUARANTINE,
            "bronze",
        ),
        "silver_padron": (s3_paths.SILVER_PADRON_RUC, "silver"),
        "silver_ordenes": (s3_paths.SILVER_ORDENES_COMPRA, "silver"),
        "silver_pricos": (s3_paths.SILVER_PRICOS, "silver"),
        "silver_ingresos": (s3_paths.SILVER_INGRESOS_TRIBUTARIOS, "silver"),
        "silver_epen": (s3_paths.SILVER_EPEN, "silver"),
        "silver_ssco": (s3_paths.SILVER_SSCO, "silver"),
        "gold_features": (s3_paths.GOLD_RUC_FEATURES, "gold"),
        "gold_regional": (s3_paths.GOLD_REGIONAL_SUMMARY, "gold"),
        "gold_scoring": (s3_paths.GOLD_SCORING_DATASET, "gold"),
    }

    results = {}
    for name, (uri, cat) in targets.items():
        if target_layer in ("all", cat):
            results[name] = audit_path(spark, uri, name)

    return results


def print_summary_table(results: dict[str, Any]):
    print(
        "\n========================================================================================="
    )
    print(
        f"{'Capa':<30} | {'Existe':<7} | {'Filas':<12} | {'RUCs Únicos':<12} | {'Checksum':<10}"
    )
    print(
        "-----------------------------------------------------------------------------------------"
    )
    for name, r in results.items():
        if r.get("exists"):
            rows_str = f"{r.get('total_rows', 0):,}"
            rucs = r.get("distinct_rucs")
            rucs_str = f"{rucs:,}" if rucs is not None else "N/A"
            chk = r.get("checksum", "")[:8]
            print(
                f"{name:<30} | {'SÍ':<7} | {rows_str:<12} | {rucs_str:<12} | {chk:<10}"
            )
        else:
            print(f"{name:<30} | {'NO':<7} | {'-':<12} | {'-':<12} | {'-':<10}")
    print(
        "=========================================================================================\n"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Auditoría de capas Spark con s3_paths."
    )
    parser.add_argument(
        "--layer",
        choices=["all", "bronze", "silver", "gold"],
        default="all",
        help="Capa a auditar (default: all)",
    )
    parser.add_argument("--output-json", help="Ruta para exportar métricas en JSON")
    args = parser.parse_args()

    try:
        from spark_session_factory import create_spark_session
    except ImportError as e:
        print(
            f"ERROR: No se pudo inicializar SparkSession ({e}). "
            "s_count_layers debe ejecutarse en EMR Serverless o en un entorno con PySpark configurado.",
            file=sys.stderr,
        )
        sys.exit(1)

    spark = create_spark_session("s_count_layers")
    try:
        results = audit_all_layers(spark, args.layer)
        print_summary_table(results)

        if args.output_json:
            with open(args.output_json, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
            print(f"✓ Resultados guardados en: {args.output_json}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
