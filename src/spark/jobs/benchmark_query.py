"""
Benchmark de LECTURA sobre Bronze: sin particionar vs. particionado por anio/mes.

Complementa a infra/scripts/07_run_benchmark.sh (que mide la ESCRITURA).
Lee las dos versiones que dejó ese script en
    <benchmark-root>/unpartitioned/<tabla>
    <benchmark-root>/partitioned/<tabla>
y ejecuta una consulta típica: filtrar por un mes (anio, mes) y agregar por
departamento. Mide el tiempo de cada ejecución.

Qué se cronometra (por ejecución): spark.read.parquet() + filtro + agregación
+ collect(). Se incluye la lectura/listado inicial a propósito, porque forma
parte del costo real de consultar la tabla.

Para reducir ruido:
  - Se hacen ejecuciones de calentamiento (--warmup-runs) que no cuentan.
  - Los modos se alternan en cada repetición (A,B / B,A / A,B...) para que
    ninguno quede siempre primero.
  - Se verifica que ambas versiones devuelvan el mismo número de filas.

Las líneas QUERY_BENCHMARK_RESULT y QUERY_BENCHMARK_SUMMARY quedan en el
stdout del driver (logs de EMR) para poder grepearlas.

Uso:
  spark-submit --py-files common.zip benchmark_query.py \\
      --benchmark-root s3://<bronze>/benchmark/<run_id> \\
      [--table ordenes_compra] [--anio 2025 --mes 3] \\
      [--repetitions 3] [--warmup-runs 1]
"""

import argparse
import statistics
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark_session_factory import create_spark_session

MODES = ("unpartitioned", "partitioned")
TABLES = ("ordenes_compra", "padron_ruc")


def build_path(benchmark_root: str, mode: str, table: str) -> str:
    return f"{benchmark_root.rstrip('/')}/{mode}/{table}"


def typical_query(df: DataFrame, table: str, anio: int, mes: int) -> DataFrame:
    """Consulta típica: filtrar un mes y agregar por departamento."""
    filtered = df.filter((F.col("anio") == anio) & (F.col("mes") == mes))
    if table == "ordenes_compra":
        return filtered.groupBy("departamento_entidad").agg(
            F.count("*").alias("n_rows"),
            F.sum("monto_total_orden_original").alias("monto_total"),
        )
    return filtered.groupBy("Departamento").agg(F.count("*").alias("n_rows"))


def find_latest_period(spark: SparkSession, path: str) -> tuple[int, int]:
    """Último (anio, mes) disponible. No se cronometra: solo elige el filtro."""
    row = (
        spark.read.parquet(path)
        .select("anio", "mes")
        .distinct()
        .orderBy(F.col("anio").desc(), F.col("mes").desc())
        .first()
    )
    if row is None:
        raise RuntimeError(f"No hay datos en {path}")
    return int(row["anio"]), int(row["mes"])


def time_query(
    spark: SparkSession, path: str, table: str, anio: int, mes: int
) -> tuple[float, int]:
    """Ejecuta la consulta completa y devuelve (segundos, filas del mes)."""
    start = time.perf_counter()
    df = spark.read.parquet(path)
    rows = typical_query(df, table, anio, mes).collect()
    elapsed = time.perf_counter() - start
    return elapsed, sum(int(r["n_rows"]) for r in rows)


def run_benchmark(
    spark: SparkSession,
    benchmark_root: str,
    table: str,
    anio: int | None = None,
    mes: int | None = None,
    repetitions: int = 3,
    warmup_runs: int = 1,
) -> dict:
    """Corre el benchmark y devuelve los tiempos por modo."""
    paths = {m: build_path(benchmark_root, m, table) for m in MODES}

    if anio is None or mes is None:
        anio, mes = find_latest_period(spark, paths["partitioned"])
    print(f"QUERY_BENCHMARK_CONFIG table={table} anio={anio} mes={mes}")

    # Plan físico (sin ejecutar): en "partitioned" debe verse PartitionFilters
    # con anio/mes; en "unpartitioned" el filtro solo aparece en PushedFilters.
    for mode in MODES:
        print(f"--- Plan de ejecución [{mode}] ---")
        typical_query(spark.read.parquet(paths[mode]), table, anio, mes).explain(
            mode="formatted"
        )

    for i in range(warmup_runs):
        for mode in MODES:
            elapsed, rows = time_query(spark, paths[mode], table, anio, mes)
            print(
                f"QUERY_BENCHMARK_WARMUP mode={mode} run={i + 1} "
                f"elapsed_seconds={elapsed:.2f} rows={rows}"
            )

    times: dict[str, list[float]] = {m: [] for m in MODES}
    row_counts: dict[str, int] = {}
    for rep in range(1, repetitions + 1):
        order = MODES if rep % 2 == 1 else tuple(reversed(MODES))
        for mode in order:
            elapsed, rows = time_query(spark, paths[mode], table, anio, mes)
            times[mode].append(elapsed)
            row_counts[mode] = rows
            print(
                f"QUERY_BENCHMARK_RESULT table={table} mode={mode} rep={rep} "
                f"elapsed_seconds={elapsed:.2f} rows={rows}"
            )

    if len(set(row_counts.values())) != 1:
        raise RuntimeError(
            f"Las versiones devolvieron distinto número de filas: {row_counts}"
        )

    summary = {
        mode: {
            "mean": statistics.mean(t),
            "min": min(t),
            "max": max(t),
        }
        for mode, t in times.items()
    }
    for mode, s in summary.items():
        print(
            f"QUERY_BENCHMARK_SUMMARY table={table} mode={mode} "
            f"mean_s={s['mean']:.2f} min_s={s['min']:.2f} max_s={s['max']:.2f}"
        )
    speedup = summary["unpartitioned"]["mean"] / summary["partitioned"]["mean"]
    print(
        f"QUERY_BENCHMARK_SPEEDUP table={table} "
        f"unpartitioned_over_partitioned={speedup:.2f}x "
        f"(>1 = partitioned más rápido)"
    )

    return {
        "anio": anio,
        "mes": mes,
        "rows": row_counts["partitioned"],
        "times": times,
        "summary": summary,
        "speedup": speedup,
    }


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark de lectura Bronze")
    parser.add_argument("--benchmark-root", required=True)
    parser.add_argument("--table", choices=TABLES, default="ordenes_compra")
    parser.add_argument("--anio", type=int, default=None)
    parser.add_argument("--mes", type=int, default=None)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmup-runs", type=int, default=1)
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    spark = create_spark_session("benchmark_query")
    try:
        run_benchmark(
            spark,
            benchmark_root=args.benchmark_root,
            table=args.table,
            anio=args.anio,
            mes=args.mes,
            repetitions=args.repetitions,
            warmup_runs=args.warmup_runs,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
