"""Wrapper para la ejecución de jobs reales con personalización de shuffle.

Permite ejecutar los jobs existentes (01 a 05) modificando únicamente
la configuración de shuffle (spark.sql.shuffle.partitions) y midiendo
el tiempo de ejecución de la aplicación Spark sin alterar el código fuente
de producción.
"""

import argparse
import importlib
import logging
import os
import sys
import time
from pathlib import Path

# Asegurar módulos en sys.path
_ROOT = Path(__file__).resolve().parent.parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
_JOBS_PATH = str(_ROOT / "src" / "spark" / "jobs")
_CURRENT_DIR = str(Path(__file__).resolve().parent)

for p in [".", _CURRENT_DIR, _COMMON_PATH, _JOBS_PATH]:
    if p not in sys.path:
        sys.path.insert(0, p)

for z in [Path("common.zip"), Path(__file__).resolve().parent / "common.zip"]:
    if z.is_file() and str(z) not in sys.path:
        sys.path.insert(0, str(z))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [perf_runner] %(message)s",
)
logger = logging.getLogger("perf_runner")

VALID_JOBS = [
    "01_ingest_bronze",
    "02_clean_silver",
    "03_feature_gold",
    "04_regional_gold",
    "05_scoring_dataset_gold",
]


def normalize_job_name(raw_name: str) -> str:
    clean = raw_name.strip()
    clean = clean.removesuffix(".py")
    clean = Path(clean).name
    return clean


def import_job(job_name: str):
    norm_name = normalize_job_name(job_name)
    try:
        mod = importlib.import_module(norm_name)
        if not hasattr(mod, "main") or not callable(mod.main):
            raise AttributeError(
                f"El módulo {norm_name} no contiene una función main() invocable."
            )
        return mod
    except ImportError as e:
        raise ImportError(f"No se pudo importar el job '{norm_name}': {e}") from e


def patch_spark_factory(shuffle_partitions: int):
    """Parchea create_spark_session en spark_session_factory para inyectar shuffle_partitions."""
    try:
        import spark_session_factory
        from pyspark.sql import SparkSession

        orig_factory = spark_session_factory.create_spark_session

        def perf_create_spark_session(app_name: str) -> SparkSession:
            logger.info(
                f"Inyectando spark.sql.shuffle.partitions={shuffle_partitions} en SparkSession '{app_name}'"
            )
            return (
                SparkSession.builder.appName(f"{app_name}_perf")
                .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.session.timeZone", "America/Lima")
                .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
                .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
                .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
                .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")
                .config("spark.sql.avro.datetimeRebaseModeInRead", "CORRECTED")
                .getOrCreate()
            )

        spark_session_factory.create_spark_session = perf_create_spark_session
        return orig_factory
    except (ImportError, AttributeError, RuntimeError, Exception) as e:  # noqa: BLE001
        logger.warning(
            f"No se pudo parchear create_spark_session (¿entorno sin PySpark?): {e}"
        )
        return None


def run_job(job_name: str, shuffle_partitions: int = 64, dry_run: bool = False) -> int:
    norm_name = normalize_job_name(job_name)
    logger.info(
        f"Cargando job: {norm_name} | shuffle: {shuffle_partitions} | dry_run: {dry_run}"
    )

    mod = import_job(norm_name)

    if dry_run:
        logger.info(f"Validación dry-run exitosa para job '{norm_name}'.")
        return 0

    patch_spark_factory(shuffle_partitions)

    start_time = time.time()
    logger.info(f"Iniciando ejecución de {norm_name}.main()...")
    try:
        ret = mod.main()
        exit_code = 0 if ret is None else int(ret)
    except Exception:
        logger.exception(f"Error durante la ejecución del job {norm_name}")
        return 1

    duration = time.time() - start_time
    logger.info(
        f"Ejecución completada: {norm_name} en {duration:.2f} segundos. Exit code: {exit_code}"
    )
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Wrapper de ejecución de jobs Spark para benchmarks."
    )
    parser.add_argument(
        "--job",
        required=True,
        help="Nombre del job a ejecutar (ej. 04_regional_gold o 04_regional_gold.py)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(os.environ.get("PERF_SHUFFLE_PARTITIONS", "64")),
        help="Valor de spark.sql.shuffle.partitions (default: 64 o env PERF_SHUFFLE_PARTITIONS)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo importa y valida el job sin iniciar Spark",
    )
    args = parser.parse_args()

    return run_job(
        job_name=args.job,
        shuffle_partitions=args.shuffle_partitions,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
