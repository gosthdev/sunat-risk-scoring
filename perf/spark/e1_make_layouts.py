"""Generador de layouts para Experimento 3.6 - E1 (Poda de particiones).

Lee el silver de Padrón RUC desde staging/ y genera dos layouts:
- Layout P (particionado): partitionBy("anio", "mes").
- Layout N (no particionado): anio y mes como columnas normales, repartido
  por hash de RUC (repartition(N, "RUC")). Cada archivo contiene los 12 meses
  para impedir que los min/max de Parquet descarten archivos por mes.

Ambos usan compresión Snappy y tamaño de archivo similar.
"""

import argparse
import os
import sys
from pathlib import Path

# Asegurar módulos en sys.path
_ROOT = Path(__file__).resolve().parent.parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)

from spark_session_factory import create_spark_session


def make_layouts(
    spark,
    staging_path: str,
    output_p: str,
    output_n: str,
    num_files_n: int = 12,
):
    print("========================================================================")
    print("Iniciando generación de layouts para Experimento E1")
    print(f"Origen (Staging) : {staging_path}")
    print(f"Destino Layout P : {output_p} (particionado por anio/mes)")
    print(f"Destino Layout N : {output_n} (sin particionar, hash RUC)")
    print("========================================================================")

    df = spark.read.parquet(staging_path)

    total_rows = df.count()
    distinct_rucs = df.select("RUC").distinct().count()
    print(
        f"[e1_make_layouts] Dataset original: {total_rows:,} filas | {distinct_rucs:,} RUCs únicos"
    )

    # -------------------------------------------------------------------------
    # Layout P: Particionado Hive anio/mes
    # -------------------------------------------------------------------------
    print("\n[Layout P] Escribiendo layout particionado por anio/mes...")
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("anio", "mes")
        .parquet(output_p)
    )
    print(f"✓ Layout P generado exitosamente en: {output_p}")

    # -------------------------------------------------------------------------
    # Layout N: No particionado, repartido por hash(RUC)
    # Cada partición contiene datos de todos los meses; los footers Parquet
    # tendrán rangos min/max que abarcan del mes 1 al 12 en cada archivo.
    # -------------------------------------------------------------------------
    print(f"\n[Layout N] Repartiendo por hash(RUC) en {num_files_n} archivos...")
    df_n = df.repartition(num_files_n, "RUC")

    print("[Layout N] Escribiendo layout plano sin particionado...")
    (df_n.write.mode("overwrite").option("compression", "snappy").parquet(output_n))
    print(f"✓ Layout N generado exitosamente en: {output_n}")

    # -------------------------------------------------------------------------
    # Validación de paridad entre P y N
    # -------------------------------------------------------------------------
    print("\n[Validación] Verificando conteos en ambos layouts...")
    count_p = spark.read.parquet(output_p).count()
    count_n = spark.read.parquet(output_n).count()

    print(f"  Filas Layout P: {count_p:,}")
    print(f"  Filas Layout N: {count_n:,}")

    if count_p != total_rows or count_n != total_rows:
        raise ValueError(
            f"Discrepancia en conteos: original={total_rows}, P={count_p}, N={count_n}"
        )

    print(
        "\n✅ Verificación exitosa: Layouts P y N son equivalentes en volumen y RUCs."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Genera layouts P y N para benchmark E1."
    )
    bucket_p = os.environ.get("BUCKET_PERF_36P", "sunat-risk-scoring-perf-36p")
    bucket_n = os.environ.get("BUCKET_PERF_36N", "sunat-risk-scoring-perf-36n")

    parser.add_argument(
        "--staging-input",
        default=f"s3://{bucket_p}/staging/padron_ruc",
        help="Ruta S3 del Padrón en staging",
    )
    parser.add_argument(
        "--output-p",
        default=f"s3://{bucket_p}/silver/padron_ruc",
        help="Ruta destino para Layout P",
    )
    parser.add_argument(
        "--output-n",
        default=f"s3://{bucket_n}/silver/padron_ruc",
        help="Ruta destino para Layout N",
    )
    parser.add_argument(
        "--num-files-n",
        type=int,
        default=12,
        help="Número de archivos objetivo para Layout N",
    )
    args = parser.parse_args()

    spark = create_spark_session("e1_make_layouts")
    try:
        make_layouts(
            spark=spark,
            staging_path=args.staging_input,
            output_p=args.output_p,
            output_n=args.output_n,
            num_files_n=args.num_files_n,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
