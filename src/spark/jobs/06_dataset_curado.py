"""
EMR Step 6 — Curado, split determinista y asignación de folds para model_inputs (Contrato C1).

Implementa:
  - Carga de features (gold/ruc_features) filtradas por corte temporal (D3).
  - Exclusión estricta de PRICOS (D2) mediante anti-join.
  - Unión del label oficial de SSCO (D1: 1 si está en lista, 0 si no).
  - Normalización de clave 'ruc' a string de 11 caracteres.
  - Split determinista y estratificado 70/30 (train/test) con semilla 42; los SSCO de un
    mismo representante legal van juntos (mismo split y mismo fold).
  - Asignación de 5 folds (0 a 4) estratificados en train, y -1 en test (PredefinedSplit).
  - Preservación de categorías crudas sin agrupar, delegando OHE a scikit-learn.
  - Salida hacia <gold-bucket>/model_inputs/dataset_version=<v>/ respetando Contrato C1.

Uso: spark-submit --py-files common.zip 06_dataset_curado.py
"""

import json
import os
import sys

from dataset_versions import fecha_de_version, mes_de_version
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat,
    countDistinct,
    format_string,
    lit,
    row_number,
    substring,
    when,
)
from pyspark.sql.functions import (
    count as spark_count,
)
from pyspark.sql.functions import (
    hash as spark_hash,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.window import Window
from s3_paths import (
    GOLD_MODEL_INPUTS,
    GOLD_RUC_FEATURES,
    SILVER_PRICOS,
    SILVER_SSCO,
)
from spark_session_factory import create_spark_session

SEMILLA_DEFAULT = 42
# Peso de la prevalencia en el target encoding: una categoría necesita ~100 RUC para
# alejarse de ella (con ~700 positivos en train casi todas tienen 0 o 1).
M_SUAVIZADO = 100
# ~30 % de ~1.030 SSCO: unos 310 positivos en test, para un PR-AUC de test menos ruidoso.
FRACCION_TEST = 0.3

# Población elegible: tipos de contribuyente con algún SSCO en el registro, ACTIVO y
# afiliado al SEE. En v3 deja ~2,1 M de 13 M RUC y conserva el 100 % de los SSCO; el
# resto son negativos triviales (sobre todo personas naturales sin negocio).
TIPOS_ELEGIBLES = (
    "EMPRESA INDIVIDUAL DE RESPONSABILIDAD LIMITADA",
    "PERSONA NATURAL CON NEGOCIO",
    "SOCIEDAD ANONIMA CERRADA",
    "SOCIEDAD COMERCIAL DE RESPONSABILIDAD LIMITADA",
    "CONTRATOS COLABORACION EMPRESARIAL",
    "SOCIEDAD POR ACCIONES CERRADA SIMPLIFICADA",
    "SOCIEDAD IRREGULAR",
    "SOCIEDAD ANONIMA",
)


def filtrar_poblacion(padron_features_df):
    """Deja solo los RUC que pueden ser SSCO: tipo elegible, ACTIVO y afiliado al SEE."""
    faltan = {"tipo_contribuyente", "Estado", "see_afiliado"} - set(
        padron_features_df.columns
    )
    if faltan:
        raise ValueError(f"ruc_features sin columnas para filtrar población: {faltan}")
    return padron_features_df.filter(
        col("tipo_contribuyente").isin(*TIPOS_ELEGIBLES)
        & (col("Estado") == "ACTIVO")
        & (col("see_afiliado") == 1)
    )


def curar_dataset(
    padron_features_df,
    ssco_df,
    pricos_df,
    semilla=SEMILLA_DEFAULT,
    top_ciiu_limit=30,
    top_dept_limit=25,
    return_stats=False,
    fecha_corte=None,
):
    """Ejecuta la lógica completa de curado, exclusión, split y folds sobre DataFrames.

    Esta función pura permite pruebas unitarias deterministas sin depender de S3.
    Con `fecha_corte` (yyyy-mm-dd) se descartan los SSCO cuya resolución se emitió
    en o antes del corte de las features: en ellos las features ya reflejan la
    designación (p. ej. facturación bloqueada), no la anticipan.
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
    ).drop("ruc_prico_norm", "es_prico")  # constante tras excluir PRICOS

    # 3. Incorporar label oficial de SSCO
    # Evalúa columna 'ruc' o 'RUC' y cualquier flag 'es_ssco'
    ssco_col_name = "RUC" if "RUC" in ssco_df.columns else "ruc"
    # Sin fechas (silver antiguo) el split de positivos cae al hash, como antes.
    fechas_ssco = {
        destino: (
            col(origen).cast("date")
            if origen in ssco_df.columns
            else lit(None).cast("date")
        )
        for origen, destino in (
            ("fecha_emision", "_fecha_emision"),
            ("fecha_firme", "_fecha_firme"),
        )
    }
    ssco_norm = (
        ssco_df.withColumn(
            "ruc_ssco_norm",
            when(
                col(ssco_col_name).cast("string").isNotNull(),
                format_string("%011d", col(ssco_col_name).cast("long")),
            ).otherwise(col(ssco_col_name).cast("string")),
        )
        .select(
            "ruc_ssco_norm",
            *[c.alias(n) for n, c in fechas_ssco.items()],
            (
                col("doc_representante").cast("string")
                if "doc_representante" in ssco_df.columns
                else lit(None).cast("string")
            ).alias("_rep"),
        )
        .dropDuplicates(["ruc_ssco_norm"])
        .withColumn("_flag_ssco", lit(1))
    )

    dataset = dataset.join(
        ssco_norm,
        dataset["ruc_norm"] == ssco_norm["ruc_ssco_norm"],
        how="left",
    ).drop("ruc_ssco_norm")

    if fecha_corte is not None:
        if "fecha_emision" not in ssco_df.columns:
            raise ValueError(
                "silver/ssco sin fecha_emision: no se puede aplicar el corte"
            )
        ya_ssco = col("_fecha_emision") <= lit(fecha_corte).cast("date")
        if return_stats:
            excluidos = dataset.filter(ya_ssco).count()
        dataset = dataset.filter(col("_fecha_emision").isNull() | ~ya_ssco)

    dataset = dataset.withColumn(
        "label",
        when(col("_flag_ssco") == 1, 1).otherwise(0).cast("int"),
    ).drop("_flag_ssco")

    # 4. Asignar ruc oficial como string
    dataset = dataset.withColumn("ruc", col("ruc_norm")).drop("ruc_norm")
    if "RUC" in dataset.columns:
        dataset = dataset.drop("RUC")

    # Grupo de un positivo: su representante legal (SUNAT designa en lotes de empresas
    # del mismo representante); sin representante, o en negativos, el propio RUC.
    dataset = dataset.withColumn(
        "_grupo",
        when(col("label") == 1, coalesce(col("_rep"), col("ruc"))).otherwise(
            col("ruc")
        ),
    ).drop("_rep")
    w_grupo = Window.partitionBy("label", "_grupo")

    # 5. Split determinista y estratificado 70% train / 30% test
    # Positivos: el 30% más reciente por fecha_firme va a test (¿se detecta a los que
    # SUNAT designó después?), con todo su grupo. Negativos (sin fecha): el 30% con hash menor.
    w_split = Window.partitionBy("label").orderBy(
        col("_fecha_firme").desc_nulls_last(),
        spark_abs(spark_hash(concat(col("ruc"), lit(f"_salt_{semilla}_split")))),
        col("ruc"),
    )
    w_label_cnt = Window.partitionBy("label")

    dataset = (
        dataset.withColumn("_rn", row_number().over(w_split))
        .withColumn("_n_label", spark_count("*").over(w_label_cnt))
        .withColumn(
            "_es_test", (col("_rn") <= (col("_n_label") * FRACCION_TEST)).cast("int")
        )
        .withColumn(
            "split",
            when(spark_max("_es_test").over(w_grupo) == 1, "test").otherwise("train"),
        )
        .drop("_rn", "_n_label", "_es_test")
    )

    # 6. Folds (0 a 4) solo en train, -1 en test. Positivos: bloques por fecha de emisión
    # y grupo completo en un fold, así un lote de designaciones o un mismo representante
    # no cae en train y validación a la vez (con folds aleatorios la CV daba ~13x el
    # PR-AUC de test). Negativos: cíclico por hash.
    hash_fold = spark_abs(
        spark_hash(concat(col("ruc"), lit(f"_salt_{semilla + 1}_fold")))
    )
    w_fold = Window.partitionBy("label").orderBy(
        when(col("label") == 1, col("_emision_grupo")).asc_nulls_last(),
        when(col("label") == 1, col("_grupo")),
        hash_fold,
        col("ruc"),
    )
    train_part = (
        dataset.filter(col("split") == "train")
        .withColumn("_emision_grupo", spark_min("_fecha_emision").over(w_grupo))
        .withColumn("_rn_fold", row_number().over(w_fold))
        .withColumn("_n_fold", spark_count("*").over(w_label_cnt))
        .withColumn(
            "fold",
            when(
                col("label") == 1,
                ((col("_rn_fold") - 1) * 5 / col("_n_fold")).cast("int"),
            )
            .otherwise((col("_rn_fold") - 1) % 5)
            .cast("int"),
        )
        # El grupo queda contiguo en el orden: toma el fold de su primer miembro.
        .withColumn("fold", spark_min("fold").over(w_grupo))
        .drop("_rn_fold", "_n_fold", "_emision_grupo")
    )
    test_part = dataset.filter(col("split") == "test").withColumn(
        "fold", lit(-1).cast("int")
    )
    dataset = train_part.unionByName(test_part)

    # 6b. Target encoding fuera de fold (no depende de sklearn: lo lee igual MLlib).
    for nombre, columnas, clave in claves_target_encoding():
        if all(c in dataset.columns for c in columnas):
            dataset = codificar_fuera_de_fold(dataset, clave, nombre)

    # 7. CAMBIO v3/v4 (Contrato C1): Las columnas categóricas se dejan CRUDAS sin agrupar.
    # El agrupamiento de categorías raras se delega al Pipeline de scikit-learn (OneHotEncoder).
    stats = {
        "top_ciiu": [],
        "top_departamentos": [],
    }

    if return_stats:
        if fecha_corte is not None:
            stats["ssco_excluidos_por_corte"] = excluidos
        rango = (
            dataset.filter(col("label") == 1)
            .groupBy("split")
            .agg(
                spark_min("_fecha_firme").alias("desde"),
                spark_max("_fecha_firme").alias("hasta"),
            )
            .collect()
        )
        stats["fecha_firme_positivos"] = {
            r["split"]: [str(r["desde"]), str(r["hasta"])] for r in rango
        }

    dataset = dataset.drop("_fecha_firme", "_fecha_emision", "_grupo")
    if return_stats:
        return dataset, stats
    return dataset


def claves_target_encoding():
    """(nombre, columnas que necesita, expresión de la categoría) de cada target encoding.

    Medido en v3 (categorías marcadas con train, evaluadas en test): CIIU lift ~6,
    CIIU x departamento ~7, cohorte de RUC ~3. La cohorte separa el prefijo (10/20...)
    porque cada prefijo numera sus RUC por separado.
    """
    return [
        ("te_ciiu", ["ciiu_principal"], col("ciiu_principal")),
        (
            "te_ciiu_depto",
            ["ciiu_principal", "departamento"],
            concat(col("ciiu_principal"), lit("|"), col("departamento")),
        ),
        (
            "te_cohorte_ruc",
            ["ruc", "ruc_correlativo"],
            concat(
                substring(col("ruc"), 1, 2),
                lit("|"),
                (col("ruc_correlativo") / 100000).cast("long").cast("string"),
            ),
        ),
    ]


def codificar_fuera_de_fold(dataset_df, clave, nombre, m=M_SUAVIZADO):
    """Agrega `nombre` = tasa suavizada de SSCO de la categoría `clave`, sin fuga.

    Train en el fold k: tasa con el train de los otros folds. Test: con todo train.
    Suavizado m-estimate hacia la prevalencia de train; categoría nula o sin datos
    toma la prevalencia, así la columna nunca es nula.
    """
    train = dataset_df.filter(col("split") == "train")
    prev = train.agg(avg(col("label").cast("double"))).first()[0] or 0.0

    por_fold = train.groupBy(clave.alias("_k"), "fold").agg(
        spark_count("*").alias("_n_f"), spark_sum("label").alias("_p_f")
    )
    w_k = Window.partitionBy("_k")
    por_fold = por_fold.withColumn("_n_t", spark_sum("_n_f").over(w_k)).withColumn(
        "_p_t", spark_sum("_p_f").over(w_k)
    )
    # Train ve los otros folds; test (fold -1) ve todo train.
    tabla = por_fold.select(
        "_k",
        "fold",
        (col("_n_t") - col("_n_f")).alias("_n"),
        (col("_p_t") - col("_p_f")).alias("_p"),
    ).unionByName(
        por_fold.select(
            "_k",
            lit(-1).cast("int").alias("fold"),
            col("_n_t").alias("_n"),
            col("_p_t").alias("_p"),
        ).dropDuplicates(["_k"])
    )

    return (
        dataset_df.withColumn("_k", clave)
        .join(tabla, on=["_k", "fold"], how="left")
        .withColumn(
            nombre,
            (
                (coalesce(col("_p"), lit(0)) + lit(m * prev))
                / (coalesce(col("_n"), lit(0)) + lit(m))
            ).cast("double"),
        )
        .drop("_k", "_n", "_p")
    )


def verificar_features_nuevas(dataset_df, columnas):
    """Falla si una feature llega vacía, constante o nula en todos los positivos de train.

    Evita que una feature sin datos llegue en silencio al modelo (como pasó con
    antiguedad_meses). Imprime el % no nulo por label.
    """
    faltan = sorted(set(columnas) - set(dataset_df.columns))
    if faltan:
        raise ValueError(f"model_inputs sin las features nuevas: {faltan}")

    es_pos_train = (col("split") == "train") & (col("label") == 1)
    aggs = [
        spark_count("*").alias("_filas"),
        spark_sum(es_pos_train.cast("int")).alias("_pos"),
    ]
    for c in columnas:
        aggs += [
            spark_count(c).alias(f"{c}__nn"),
            spark_count(when(es_pos_train, col(c))).alias(f"{c}__nn_pos"),
            countDistinct(c).alias(f"{c}__distintos"),
        ]
    fila = dataset_df.agg(*aggs).first()
    filas, pos = fila["_filas"], fila["_pos"] or 0

    problemas = []
    for c in columnas:
        nn, nn_pos = fila[f"{c}__nn"], fila[f"{c}__nn_pos"]
        distintos = fila[f"{c}__distintos"]
        print(
            f"[06_dataset_curado] {c}: {100.0 * nn / max(filas, 1):.2f}% no nulo, "
            f"{100.0 * nn_pos / max(pos, 1):.2f}% no nulo en positivos de train, "
            f"{distintos} valores distintos"
        )
        if nn == 0:
            problemas.append(f"{c} 100% nula")
        elif nn_pos == 0:
            problemas.append(f"{c} nula en todos los positivos de train")
        elif distintos < 2:
            problemas.append(f"{c} constante")
    if problemas:
        raise ValueError(f"Features nuevas inválidas: {problemas}")


def verificar_dataset(dataset_df):
    """Falla si el dataset está vacío, sin positivos o con un RUC repetido.

    Un RUC repetido puede caer en train y test a la vez (fuga); un dataset vacío
    o sin positivos significa que el mes de la versión no existe en ruc_features
    o que el label no cruzó, y no debe llegar a model_inputs.
    """
    fila = dataset_df.agg(
        spark_count("*").alias("filas"),
        countDistinct("ruc").alias("rucs"),
        spark_sum("label").alias("positivos"),
    ).first()
    filas, rucs, positivos = fila["filas"], fila["rucs"], fila["positivos"] or 0
    if filas == 0:
        raise ValueError(
            "Dataset curado vacío: ¿existe el mes de la versión en ruc_features?"
        )
    if positivos == 0:
        raise ValueError(
            "Dataset curado sin positivos: el label SSCO no cruzó con ruc_features"
        )
    if rucs != filas:
        raise ValueError(f"Dataset curado con RUC repetidos: {filas} filas, {rucs} RUC")


def generar_reporte_curado(dataset_df, dataset_version="v1"):
    """Genera diccionario de resumen de estadísticas de calidad del dataset curado."""
    split_label_counts = dataset_df.groupBy("split", "label").count().collect()
    counts_map = {
        (r["split"], int(r["label"])): int(r["count"]) for r in split_label_counts
    }

    pos_train = counts_map.get(("train", 1), 0)
    neg_train = counts_map.get(("train", 0), 0)
    train_filas = pos_train + neg_train

    pos_test = counts_map.get(("test", 1), 0)
    neg_test = counts_map.get(("test", 0), 0)
    test_filas = pos_test + neg_test

    total_filas = train_filas + test_filas
    pos_total = pos_train + pos_test

    prev_train = (pos_train / train_filas) if train_filas > 0 else 0.0
    prev_test = (pos_test / test_filas) if test_filas > 0 else 0.0
    dif_relativa = abs(prev_train - prev_test) / prev_train if prev_train > 0 else 0.0

    fold_counts = (
        dataset_df.filter((col("split") == "train") & (col("label") == 1))
        .groupBy("fold")
        .count()
        .collect()
    )
    fold_map = {
        int(r["fold"]): int(r["count"]) for r in fold_counts if r["fold"] is not None
    }
    folds_pos = {f"fold_{f}": fold_map.get(f, 0) for f in range(5)}

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
    mes_padron = mes_de_version(dataset_version)
    semilla = int(os.environ.get("SEMILLA", str(SEMILLA_DEFAULT)))

    spark = create_spark_session("06_dataset_curado")
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        padron_features = filtrar_poblacion(
            spark.read.parquet(GOLD_RUC_FEATURES).filter(
                col("mes_referencia") == mes_padron
            )
        )
        ssco = spark.read.parquet(SILVER_SSCO)
        pricos = spark.read.parquet(SILVER_PRICOS)

        dataset, stats = curar_dataset(
            padron_features_df=padron_features,
            ssco_df=ssco,
            pricos_df=pricos,
            semilla=semilla,
            return_stats=True,
            fecha_corte=fecha_de_version(dataset_version),
        )

        dataset = dataset.withColumn("dataset_version", lit(dataset_version))
        dataset = dataset.persist()
        verificar_dataset(dataset)
        verificar_features_nuevas(dataset, [n for n, _, _ in claves_target_encoding()])

        (
            dataset.write.mode("overwrite")
            .partitionBy("dataset_version")
            .parquet(GOLD_MODEL_INPUTS)
        )

        reporte = generar_reporte_curado(dataset, dataset_version=dataset_version)
        reporte.update(
            {
                k: v
                for k, v in stats.items()
                if k not in ("top_ciiu", "top_departamentos")
            }
        )
        print("[06_dataset_curado] Reporte de Curado e Invariantes:")
        print(json.dumps(reporte, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
