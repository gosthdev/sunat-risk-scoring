"""
Genera el Reporte de Control de Calidad leyendo metadata de Parquet en S3 usando awswrangler.
"""

import awswrangler as wr
from s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_ORDENES_COMPRA_QUARANTINE,
    BRONZE_PADRON_RUC,
    BRONZE_PADRON_RUC_QUARANTINE,
    SILVER_ORDENES_COMPRA,
    SILVER_PADRON_RUC,
    SILVER_QUALITY_REPORT,
)
from schema_definitions import (
    ORDENES_COMPRA_CAMPOS_CRITICOS,
    PADRON_RUC_CAMPOS_CRITICOS,
)


def count_s3_parquet(path):
    try:
        df = wr.s3.read_parquet(path, dataset=True, columns=[])
        return len(df)
    except wr.exceptions.NoFilesFound:
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"Error leyendo {path}: {e}")
        return 0


def calculate_completeness_s3(path, columns):
    try:
        df = wr.s3.read_parquet(path, dataset=True, columns=columns)
        total = len(df)
        if total == 0:
            return {c: 0 for c in columns}

        completeness = {}
        for c in columns:
            if c in df.columns:
                non_nulls = df[c].notna().sum()
                completeness[c] = round((non_nulls / total) * 100, 2)
            else:
                completeness[c] = "N/A"
        return completeness
    except wr.exceptions.NoFilesFound:
        return {c: 0 for c in columns}
    except Exception as e:  # noqa: BLE001
        print(f"Error calculando completitud en {path}: {e}")
        return {c: 0 for c in columns}


def analyze_padron():
    print("--- 1. PADRÓN RUC ---")

    bronze_clean_count = count_s3_parquet(BRONZE_PADRON_RUC)
    quarantine_count = count_s3_parquet(BRONZE_PADRON_RUC_QUARANTINE)
    raw_count = bronze_clean_count + quarantine_count

    print(f"Total registros crudos (Raw): {raw_count}")
    pct_quarantine = (quarantine_count / raw_count) * 100 if raw_count > 0 else 0
    print(
        f"Rechazados por formato/esquema (Quarantine): {quarantine_count} ({pct_quarantine:.2f}%)"
    )

    silver_count = count_s3_parquet(SILVER_PADRON_RUC)
    business_rejects = bronze_clean_count - silver_count

    print(
        f"Rechazados por nulos/lógica de negocio (Bronze -> Silver): {business_rejects}"
    )
    print(f"Total registros válidos en Silver: {silver_count}\n")

    print("Porcentaje de Completitud por Campo Crítico en Silver:")
    completeness = calculate_completeness_s3(
        SILVER_PADRON_RUC, PADRON_RUC_CAMPOS_CRITICOS
    )
    for c, pct in completeness.items():
        print(f" - {c}: {pct}%")
    print("\n")


def analyze_ordenes():
    print("--- 2. ÓRDENES DE COMPRA ---")

    bronze_clean_count = count_s3_parquet(BRONZE_ORDENES_COMPRA)
    quarantine_count = count_s3_parquet(BRONZE_ORDENES_COMPRA_QUARANTINE)
    raw_count = bronze_clean_count + quarantine_count

    print(f"Total registros crudos (Raw): {raw_count}")
    pct_quarantine = (quarantine_count / raw_count) * 100 if raw_count > 0 else 0
    print(
        f"Rechazados por formato o año fuera de rango (Quarantine): {quarantine_count} ({pct_quarantine:.2f}%)"
    )

    silver_count = count_s3_parquet(SILVER_ORDENES_COMPRA)
    business_rejects = bronze_clean_count - silver_count

    print(
        f"Rechazados por lógica de negocio (RUC nulo o Monto Negativo): {business_rejects}"
    )
    print(f"Total registros válidos en Silver: {silver_count}\n")

    print("Porcentaje de Completitud por Campo Crítico en Silver:")
    completeness = calculate_completeness_s3(
        SILVER_ORDENES_COMPRA, ORDENES_COMPRA_CAMPOS_CRITICOS
    )
    for c, pct in completeness.items():
        print(f" - {c}: {pct}%")
    print("\n")


def analyze_cuarentena():
    """Tasa de cuarentena por fuente, año y mes (la calcula 02_clean_silver.py)."""
    print("--- 3. TASA DE CUARENTENA POR FUENTE Y MES ---")
    try:
        df = wr.s3.read_parquet(SILVER_QUALITY_REPORT, dataset=True)
    except wr.exceptions.NoFilesFound:
        print("Sin reporte: ejecutar 02_clean_silver.py primero.\n")
        return

    df = df.sort_values(["fuente", "anio", "mes"])
    for fila in df.itertuples(index=False):
        print(
            f" - {fila.fuente} {fila.anio}-{int(fila.mes):02d}: "
            f"RUC inválido {fila.tasa_cuarentena_ruc:.2%} "
            f"({fila.filas_cuarentena_ruc}/{fila.filas_entrada}), "
            f"bronze {fila.tasa_cuarentena_bronze:.2%} "
            f"({fila.filas_cuarentena_bronze} filas)"
        )
    print("\n")


def main():
    print("Generando reporte...\n")
    analyze_padron()
    analyze_ordenes()
    analyze_cuarentena()


if __name__ == "__main__":
    main()
