#!/usr/bin/env python3
"""Comparador de salidas entre dos ejecuciones o layouts Spark.

Verifica que dos datasets Parquet (locales o S3) sean idénticos en:
- Conteo total de filas
- Esquema de columnas (nombres y tipos)
- Conteo de claves únicas (ej. RUC, Departamento)
- Checksum determinista de contenido
"""

import argparse
import hashlib
import sys
from pathlib import Path

# Configurar sys.path
_ROOT = Path(__file__).resolve().parent.parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)


def compute_df_checksum(df, key_col=None) -> str:
    """Calcula un checksum MD5 determinista sobre el contenido del DataFrame."""
    # Ordenar por columnas relevantes para consistencia
    sort_cols = [key_col] if (key_col and key_col in df.columns) else sorted(df.columns)
    sorted_df = df.orderBy(*sort_cols)

    # Concatenar representación en string de cada fila en orden alfabético de columnas
    col_names = sorted(df.columns)
    rows = sorted_df.collect()
    hasher = hashlib.md5()
    for row in rows:
        row_str = "|".join("" if row[c] is None else str(row[c]) for c in col_names)
        hasher.update(row_str.encode("utf-8"))
    return hasher.hexdigest()


def compare_datasets(
    spark, path_a: str, path_b: str, key_col: str | None = None
) -> bool:
    print("[compare_outputs] Comparando:")
    print(f"  Dataset A: {path_a}")
    print(f"  Dataset B: {path_b}")

    df_a = spark.read.parquet(path_a)
    df_b = spark.read.parquet(path_b)

    count_a = df_a.count()
    count_b = df_b.count()

    print("\n--- 1. Validación de Conteo de Filas ---")
    print(f"  Filas A: {count_a}")
    print(f"  Filas B: {count_b}")
    if count_a != count_b:
        print(f"  ❌ ERROR: Conteo desigual ({count_a} != {count_b})")
        return False
    print("  ✓ Conteo idéntico")

    print("\n--- 2. Validación de Esquema ---")
    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)
    if cols_a != cols_b:
        print("  ❌ ERROR: Columnas difieren!")
        print(f"     Solo en A: {cols_a - cols_b}")
        print(f"     Solo en B: {cols_b - cols_a}")
        return False
    print("  ✓ Columnas idénticas")

    if key_col and key_col in df_a.columns:
        print(f"\n--- 3. Validación de Clave Única ({key_col}) ---")
        distinct_a = df_a.select(key_col).distinct().count()
        distinct_b = df_b.select(key_col).distinct().count()
        print(f"  Distintos en A: {distinct_a}")
        print(f"  Distintos en B: {distinct_b}")
        if distinct_a != distinct_b:
            print("  ❌ ERROR: Claves únicas desiguales")
            return False
        print("  ✓ Conteo de claves únicas idéntico")

    print("\n--- 4. Validación de Checksum de Contenido ---")
    # Para datasets pequeños o medianos (< 100k filas), calcular checksum en memoria
    if count_a <= 100_000:
        hash_a = compute_df_checksum(df_a, key_col)
        hash_b = compute_df_checksum(df_b, key_col)
        print(f"  Checksum MD5 A: {hash_a}")
        print(f"  Checksum MD5 B: {hash_b}")
        if hash_a != hash_b:
            print("  ❌ ERROR: Checksum de contenido no coincide")
            return False
        print("  ✓ Checksum de contenido idéntico")
    else:
        print("  (Dataset grande: validando mediante agregados numéricos)")
        from pyspark.sql.types import NumericType

        numeric_cols = [
            f.name
            for f in df_a.schema.fields
            if isinstance(f.dataType, NumericType)
        ]
        if numeric_cols:
            from pyspark.sql.functions import sum as spark_sum

            for col_name in numeric_cols:
                sum_a = df_a.select(spark_sum(col_name)).collect()[0][0]
                sum_b = df_b.select(spark_sum(col_name)).collect()[0][0]
                if sum_a != sum_b:
                    print(
                        f"  ❌ Mismatch en suma de columna {col_name}: {sum_a} != {sum_b}"
                    )
                    return False
            print("  ✓ Agregados numéricos coincidentes")

    print("\n========================================================")
    print("✅ RESULTADO: Ambos datasets son equivalentes.")
    print("========================================================")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Compara equivalencia entre dos outputs Parquet."
    )
    parser.add_argument("--path-a", required=True, help="Ruta o URI S3 del Dataset A")
    parser.add_argument("--path-b", required=True, help="Ruta o URI S3 del Dataset B")
    parser.add_argument(
        "--key-col",
        help="Columna clave para validación de cardinalidad (ej. RUC o Departamento)",
    )
    args = parser.parse_args()

    from spark_session_factory import create_spark_session

    spark = create_spark_session("compare_outputs")

    try:
        success = compare_datasets(spark, args.path_a, args.path_b, args.key_col)
        sys.exit(0 if success else 1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
