"""
EMR Step 6 — Curado, split determinista y asignación de folds para model_inputs (Contrato C1).

Implementa:
  - Carga de features (gold/ruc_features) filtradas por corte temporal (D3).
  - Exclusión estricta de PRICOS (D2) mediante anti-join.
  - Unión del label oficial de SSCO (D1: 1 si está en lista, 0 si no).
  - Normalización de clave 'ruc' a string de 11 caracteres.
  - Split determinista y estratificado 80/20 (train/test) con semilla 42.
  - Asignación de 5 folds (0 a 4) estratificados exclusivamente en train.
  - Agrupación de categorías raras (top 30 CIIU, top 25 departamentos)
    calculadas ESTRICTAMENTE sobre train para evitar fuga.
  - Salida hacia gold/model_inputs/dataset_version=<v>/ respetando Contrato C1.

Uso: spark-submit --py-files common.zip 06_dataset_curado.py
"""

import json
import os
import sys

from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    col,
    concat,
    format_string,
    lit,
    trim,
    when,
)
from pyspark.sql.functions import (
    hash as spark_hash,
)
from s3_paths import (
    GOLD_MODEL_INPUTS,
    GOLD_RUC_FEATURES,
    SILVER_PRICOS,
    SILVER_SSCO,
)
from spark_session_factory import create_spark_session

SEMILLA_DEFAULT = 42


def curar_dataset(
    padron_features_df,
    ssco_df,
    pricos_df,
    semilla=SEMILLA_DEFAULT,
    top_ciiu_limit=30,
    top_dept_limit=25,
):
    """Ejecuta la lógica completa de curado, exclusión, split y folds sobre DataFrames.

    Esta función pura permite pruebas unitarias deterministas sin depender de S3.
    """
    # 1. Normalizar clave RUC en padron
    padron = padron_features_df.withColumn(
        "ruc_norm",
        when(
            col("RUC").cast("string").isNotNull(),
            format_string("%011d", col("RUC").cast("long")),
        ).otherwise(col("RUC").cast("string")),
    )

    # 2. Exclusión estricta de PRICOS (Anti-join)
    pricos_norm = (
        pricos_df.withColumn(
            "ruc_prico_norm",
            when(
                col("RUC").cast("string").isNotNull(),
                format_string("%011d", col("RUC").cast("long")),
            ).otherwise(col("RUC").cast("string")),
        )
        .select("ruc_prico_norm")
        .distinct()
    )

    dataset = padron.join(
        pricos_norm,
        padron["ruc_norm"] == pricos_norm["ruc_prico_norm"],
        how="left_anti",
    ).drop("ruc_prico_norm")

    # 3. Incorporar label oficial de SSCO
    # Evalúa columna 'ruc' o 'RUC' y cualquier flag 'es_ssco'
    ssco_col_name = "RUC" if "RUC" in ssco_df.columns else "ruc"
    ssco_norm = (
        ssco_df.withColumn(
            "ruc_ssco_norm",
            when(
                col(ssco_col_name).cast("string").isNotNull(),
                format_string("%011d", col(ssco_col_name).cast("long")),
            ).otherwise(col(ssco_col_name).cast("string")),
        )
        .select("ruc_ssco_norm")
        .distinct()
        .withColumn("_flag_ssco", lit(1))
    )

    dataset = dataset.join(
        ssco_norm,
        dataset["ruc_norm"] == ssco_norm["ruc_ssco_norm"],
        how="left",
    ).drop("ruc_ssco_norm")

    dataset = dataset.withColumn(
        "label",
        when(col("_flag_ssco") == 1, 1).otherwise(0).cast("int"),
    ).drop("_flag_ssco")

    # 4. Asignar ruc oficial como string
    dataset = dataset.withColumn("ruc", col("ruc_norm")).drop("ruc_norm")
    if "RUC" in dataset.columns:
        dataset = dataset.drop("RUC")

    # 5. Split determinista y estratificado 80% train / 20% test
    # Incluir label en el hash garantiza que la distribución se estratifica para ambas clases
    dataset = dataset.withColumn(
        "_split_hash",
        (
            spark_abs(
                spark_hash(
                    concat(col("ruc"), lit(f"_salt_{semilla}_split_"), col("label"))
                )
            )
            % 100
        ).cast("int"),
    )

    dataset = dataset.withColumn(
        "split",
        when(col("_split_hash") < 20, "test").otherwise("train"),
    ).drop("_split_hash")

    # 6. Asignar fold (0 a 4) estratificado exclusivamente para filas de train
    dataset = dataset.withColumn(
        "fold",
        when(col("split") == "test", lit(None).cast("int")).otherwise(
            (
                spark_abs(
                    spark_hash(
                        concat(
                            col("ruc"), lit(f"_salt_{semilla + 1}_fold_"), col("label")
                        )
                    )
                )
                % 5
            ).cast("int")
        ),
    )

    # 7. Categorías raras calculadas ESTRICTAMENTE sobre train
    train_df = dataset.filter(col("split") == "train")

    col_ciiu = (
        "ciiu_principal"
        if "ciiu_principal" in dataset.columns
        else (
            "actividad_economica_principal"
            if "actividad_economica_principal" in dataset.columns
            else None
        )
    )

    if col_ciiu:
        top_ciiu_rows = (
            train_df.filter(col(col_ciiu).isNotNull() & (trim(col(col_ciiu)) != ""))
            .groupBy(col_ciiu)
            .count()
            .orderBy(col("count").desc())
            .limit(top_ciiu_limit)
            .collect()
        )
        top_ciiu_list = [r[col_ciiu] for r in top_ciiu_rows]

        dataset = dataset.withColumn(
            "ciiu_principal",
            when(col(col_ciiu).isin(top_ciiu_list), col(col_ciiu)).otherwise(
                lit("OTROS")
            ),
        )
        if col_ciiu != "ciiu_principal":
            dataset = dataset.drop(col_ciiu)

    if "departamento" in dataset.columns:
        top_dept_rows = (
            train_df.filter(
                col("departamento").isNotNull() & (trim(col("departamento")) != "")
            )
            .groupBy("departamento")
            .count()
            .orderBy(col("count").desc())
            .limit(top_dept_limit)
            .collect()
        )
        top_dept_list = [r["departamento"] for r in top_dept_rows]

        dataset = dataset.withColumn(
            "departamento",
            when(
                col("departamento").isin(top_dept_list), col("departamento")
            ).otherwise(lit("DESCONOCIDO")),
        )

    return dataset


def generar_reporte_curado(dataset_df, dataset_version="v1"):
    """Genera diccionario de resumen de estadísticas de calidad del dataset curado."""
    total_filas = dataset_df.count()
    train_filas = dataset_df.filter(col("split") == "train").count()
    test_filas = dataset_df.filter(col("split") == "test").count()

    pos_total = dataset_df.filter(col("label") == 1).count()
    pos_train = dataset_df.filter(
        (col("split") == "train") & (col("label") == 1)
    ).count()
    pos_test = dataset_df.filter((col("split") == "test") & (col("label") == 1)).count()

    prev_train = (pos_train / train_filas) if train_filas > 0 else 0.0
    prev_test = (pos_test / test_filas) if test_filas > 0 else 0.0
    dif_relativa = abs(prev_train - prev_test) / prev_train if prev_train > 0 else 0.0

    folds_pos = {}
    for f in range(5):
        cnt = dataset_df.filter((col("fold") == f) & (col("label") == 1)).count()
        folds_pos[f"fold_{f}"] = cnt

    return {
        "dataset_version": dataset_version,
        "total_filas": total_filas,
        "train_filas": train_filas,
        "test_filas": test_filas,
        "total_positivos": pos_total,
        "positivos_train": pos_train,
        "positivos_test": pos_test,
        "prevalencia_train": round(prev_train, 6),
        "prevalencia_test": round(prev_test, 6),
        "diferencia_relativa_prevalencia": round(dif_relativa, 6),
        "positivos_por_fold": folds_pos,
    }


def main():
    dataset_version = os.environ.get("DATASET_VERSION", "v1")
    mes_padron = "202506" if dataset_version == "v1" else "202512"
    semilla = int(os.environ.get("SEMILLA", str(SEMILLA_DEFAULT)))

    spark = create_spark_session("06_dataset_curado")
    try:
        padron_features = spark.read.parquet(GOLD_RUC_FEATURES).filter(
            col("mes_referencia") == mes_padron
        )
        ssco = spark.read.parquet(SILVER_SSCO)
        pricos = spark.read.parquet(SILVER_PRICOS)

        dataset = curar_dataset(
            padron_features_df=padron_features,
            ssco_df=ssco,
            pricos_df=pricos,
            semilla=semilla,
        )

        dataset = dataset.withColumn("dataset_version", lit(dataset_version))

        out_path = f"{GOLD_MODEL_INPUTS}/dataset_version={dataset_version}/"
        (dataset.write.mode("overwrite").parquet(out_path))

        reporte = generar_reporte_curado(dataset, dataset_version=dataset_version)
        print("[06_dataset_curado] Reporte de Curado e Invariantes:")
        print(json.dumps(reporte, indent=2))

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
