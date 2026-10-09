"""
EMR Step 3 — Feature engineering a nivel RUC (capa Gold: gold/ruc_features).

Construye, por RUC, las features ampliadas de entrada para el modelo de scoring SSCO:
  - Capacidad operativa (Padrón RUC): nro_trabajadores, sin_trabajadores,
    tipo_contribuyente, departamento, ciiu_principal, n_actividades, Estado, Condicion.
  - Perfil contractual (Órdenes de Compra con corte temporal):
    monto_total_soles, n_ordenes, monto_mediano_soles, monto_maximo_soles,
    n_entidades_distintas, pct_monto_en_entidad_principal, pct_ordenes_anuladas,
    antiguedad_contratacion_estado_dias, contrata_con_estado.
  - Relación capacidad vs contratación: monto_por_trabajador.
  - Contextual regional: informalidad_epen_departamento (desde regional_summary).
  - Pertenencia a PRICOS: es_prico.

Uso: spark-submit --py-files common.zip 03_feature_gold.py
"""

import os
import sys

from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    count,
    countDistinct,
    datediff,
    expr,
    format_string,
    last_day,
    lit,
    lower,
    to_date,
    trim,
    when,
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
from s3_paths import (
    GOLD_REGIONAL_SUMMARY,
    GOLD_RUC_FEATURES,
    SILVER_ORDENES_COMPRA,
    SILVER_PADRON_RUC,
    SILVER_PRICOS,
)
from spark_session_factory import create_spark_session


def build_contratacion_estado_features(spark, ordenes_df=None, fecha_corte=None):
    """Agrega características contractuales históricas por contratista (RUC).

    Excluye órdenes 'Anulada' de montos y calcula métricas de concentración,
    dispersión y riesgo operativo respetando la fecha de corte.
    """
    if ordenes_df is None:
        ordenes_df = spark.read.parquet(SILVER_ORDENES_COMPRA)

    ordenes = ordenes_df.withColumn("RUC", col("ruc_contratista").cast("long"))
    ordenes = ordenes.filter(col("RUC").isNotNull())

    if fecha_corte is not None:
        ordenes = ordenes.filter(
            col("fecha_de_emision") <= to_date(lit(str(fecha_corte)))
        )

    # Identificar estado anulado (insensible a mayúsculas/minúsculas)
    estado_col = (
        col("estadocontratacion")
        if "estadocontratacion" in ordenes.columns
        else (col("estado_orden") if "estado_orden" in ordenes.columns else lit(""))
    )
    es_anulada = lower(estado_col) == "anulada"

    # Monto válido solo para órdenes vigentes (NULL para órdenes anuladas para no distorsionar la mediana ni el máximo)
    monto_valido = when(
        ~es_anulada, coalesce(col("monto_total_orden_original"), lit(0.0))
    ).otherwise(lit(None))
    ordenes = ordenes.withColumn("_monto_valido", monto_valido)
    ordenes = ordenes.withColumn("_es_anulada_flag", when(es_anulada, 1).otherwise(0))

    # Entidad compradora unificada
    entidad_col = coalesce(
        col("ruc_entidad").cast("string"),
        col("entidad"),
        lit("ENTIDAD_DESCONOCIDA"),
    )
    ordenes = ordenes.withColumn("_entidad_id", entidad_col)

    # 1. Agregación principal por RUC
    resumen_ordenes = ordenes.groupBy("RUC").agg(
        coalesce(spark_sum("_monto_valido"), lit(0.0)).alias("monto_total_soles"),
        count("*").alias("n_ordenes"),
        expr("percentile_approx(_monto_valido, 0.5)").alias("monto_mediano_soles"),
        spark_max("_monto_valido").alias("monto_maximo_soles"),
        countDistinct("_entidad_id").alias("n_entidades_distintas"),
        (spark_sum("_es_anulada_flag") / count("*")).alias("pct_ordenes_anuladas"),
        spark_min("fecha_de_emision").alias("fecha_primera_orden"),
    )

    # 2. Concentración: Monto máximo facturado con una sola entidad
    entidad_agg = ordenes.groupBy("RUC", "_entidad_id").agg(
        spark_sum("_monto_valido").alias("_monto_entidad")
    )
    max_entidad = entidad_agg.groupBy("RUC").agg(
        spark_max("_monto_entidad").alias("_monto_max_entidad")
    )

    contratacion = (
        resumen_ordenes.join(max_entidad, on="RUC", how="left")
        .withColumn(
            "pct_monto_en_entidad_principal",
            when(
                (col("monto_total_soles").isNull()) | (col("monto_total_soles") <= 0.0),
                0.0,
            ).otherwise(col("_monto_max_entidad") / col("monto_total_soles")),
        )
        .drop("_monto_max_entidad")
    )

    # Columnas de compatibilidad con versión previa
    contratacion = contratacion.withColumn(
        "monto_total_contratado_estado", col("monto_total_soles")
    ).withColumn("cantidad_contratos_estado", col("n_ordenes").cast("int"))

    return contratacion


def build_prico_flag(spark, pricos_df=None):
    """Genera flag de inclusión en el Padrón de Principales Contribuyentes."""
    if pricos_df is None:
        pricos_df = spark.read.parquet(SILVER_PRICOS)
    return pricos_df.select(col("RUC")).distinct().withColumn("es_prico", lit(True))


def build_padron_features(spark, padron_df=None, mes_padron=None):
    """Extrae y normaliza features registrales y de capacidad desde Padrón RUC."""
    if padron_df is None:
        padron_df = spark.read.parquet(SILVER_PADRON_RUC)

    ciiu_rev4_col = (
        col("Actividad_Economica_CIIU_revision4_Principal")
        if "Actividad_Economica_CIIU_revision4_Principal" in padron_df.columns
        else (
            col("ciiu_principal")
            if "ciiu_principal" in padron_df.columns
            else lit("OTROS")
        )
    )

    ciiu_rev3_sec = (
        col("Actividad_Economica_CIIU_revision3_Secundaria")
        if "Actividad_Economica_CIIU_revision3_Secundaria" in padron_df.columns
        else lit(None)
    )

    nro_trab_col = (
        col("NroTrab").cast("double")
        if "NroTrab" in padron_df.columns
        else (
            col("nro_trabajadores").cast("double")
            if "nro_trabajadores" in padron_df.columns
            else lit(None).cast("double")
        )
    )

    tipo_contrib = (
        col("Tipo")
        if "Tipo" in padron_df.columns
        else (
            col("tipo_contribuyente")
            if "tipo_contribuyente" in padron_df.columns
            else lit("DESCONOCIDO")
        )
    )

    dept_col = (
        col("Departamento")
        if "Departamento" in padron_df.columns
        else (
            col("departamento")
            if "departamento" in padron_df.columns
            else lit("DESCONOCIDO")
        )
    )

    padron = (
        padron_df.withColumn("RUC", col("RUC").cast("long"))
        .withColumn("actividad_economica_principal", ciiu_rev4_col)
        .withColumn("ciiu_principal", ciiu_rev4_col)
        .withColumn(
            "tipo_contribuyente", coalesce(trim(tipo_contrib), lit("DESCONOCIDO"))
        )
        .withColumn("departamento", coalesce(trim(dept_col), lit("DESCONOCIDO")))
        .withColumn("nro_trabajadores", nro_trab_col)
        .withColumn(
            "sin_trabajadores",
            when(
                col("nro_trabajadores").isNull() | (col("nro_trabajadores") <= 0.0), 1
            ).otherwise(0),
        )
        .withColumn(
            "n_actividades",
            when(ciiu_rev3_sec.isNotNull() & (trim(ciiu_rev3_sec) != ""), 2).otherwise(
                1
            ),
        )
    )

    if "mes_referencia" in padron.columns:
        padron_res = padron
    elif "anio" in padron.columns and "mes" in padron.columns:
        padron_res = padron.withColumn(
            "mes_referencia",
            format_string("%d%02d", col("anio").cast("int"), col("mes").cast("int")),
        )
    else:
        padron_res = padron.withColumn("mes_referencia", lit("202506"))

    if mes_padron is not None:
        padron_res = padron_res.filter(col("mes_referencia") == str(mes_padron))

    cols_select = [
        "RUC",
        "actividad_economica_principal",
        "ciiu_principal",
        "Estado",
        "Condicion",
        "tipo_contribuyente",
        "departamento",
        "nro_trabajadores",
        "sin_trabajadores",
        "n_actividades",
        "mes_referencia",
    ]
    # Filtrar solo columnas que realmente existan
    available = [c for c in cols_select if c in padron_res.columns]
    return padron_res.select(*available)


def attach_regional_features(spark, features_df, regional_path=GOLD_REGIONAL_SUMMARY):
    """Une la tasa de informalidad regional desde gold/regional_summary si existe."""
    try:
        regional = spark.read.parquet(regional_path)
        col_inf = (
            col("pct_informalidad")
            if "pct_informalidad" in regional.columns
            else (
                col("tasa_informalidad_laboral")
                if "tasa_informalidad_laboral" in regional.columns
                else lit(None).cast("double")
            )
        )
        reg_clean = regional.select(
            col("Departamento").alias("departamento"),
            col_inf.alias("informalidad_epen_departamento"),
        ).distinct()
        return features_df.join(reg_clean, on="departamento", how="left")
    except Exception:  # noqa: BLE001
        return features_df.withColumn(
            "informalidad_epen_departamento", lit(None).cast("double")
        )


def compute_all_ruc_features(padron_df, contratacion_df, pricos_df, regional_df=None):
    """Combina padrón, contratación y pricos aplicando reglas de negocio de features."""
    fecha_ref_mes = last_day(
        to_date(concat(col("mes_referencia"), lit("01")), "yyyyMMdd")
    )

    features = (
        padron_df.join(contratacion_df, on="RUC", how="left")
        .join(pricos_df, on="RUC", how="left")
        .withColumn(
            "antiguedad_contratacion_estado_dias",
            datediff(fecha_ref_mes, col("fecha_primera_orden")),
        )
        .withColumn(
            "antiguedad_contratacion_estado_dias",
            when(col("antiguedad_contratacion_estado_dias") < 0, None).otherwise(
                col("antiguedad_contratacion_estado_dias")
            ),
        )
        .drop("fecha_primera_orden")
        .withColumn(
            "es_prico",
            when(col("es_prico").isNull(), False).otherwise(col("es_prico")),
        )
        .withColumn(
            "contrata_con_estado",
            when(col("n_ordenes").isNull() | (col("n_ordenes") == 0), 0).otherwise(1),
        )
        .withColumn(
            "monto_por_trabajador",
            when(
                col("nro_trabajadores").isNull()
                | (col("nro_trabajadores") <= 0.0)
                | col("monto_total_soles").isNull(),
                lit(None).cast("double"),
            ).otherwise(col("monto_total_soles") / col("nro_trabajadores")),
        )
        .withColumn(
            "monto_total_contratado_estado",
            coalesce(col("monto_total_soles"), lit(0.0)),
        )
        .withColumn(
            "cantidad_contratos_estado",
            when(col("n_ordenes").isNull(), 0).otherwise(col("n_ordenes").cast("int")),
        )
        .withColumn(
            "pct_ordenes_anuladas",
            when(col("n_ordenes").isNull() | (col("n_ordenes") == 0), 0.0).otherwise(
                col("pct_ordenes_anuladas")
            ),
        )
    )

    if regional_df is not None:
        features = features.join(regional_df, on="departamento", how="left")
    elif "informalidad_epen_departamento" not in features.columns:
        features = features.withColumn(
            "informalidad_epen_departamento", lit(None).cast("double")
        )

    return features


def main():
    dataset_version = os.environ.get("DATASET_VERSION", "v1")
    mes_padron = os.environ.get(
        "MES_PADRON", "202506" if dataset_version == "v1" else "202512"
    )
    fecha_corte_ordenes = os.environ.get(
        "FECHA_CORTE_ORDENES",
        "2025-06-30" if dataset_version == "v1" else "2025-12-31",
    )

    spark = create_spark_session("03_feature_gold")
    try:
        padron = build_padron_features(spark, mes_padron=mes_padron)
        contratacion = build_contratacion_estado_features(
            spark, fecha_corte=fecha_corte_ordenes
        )
        pricos = build_prico_flag(spark)

        features = compute_all_ruc_features(padron, contratacion, pricos)
        features = attach_regional_features(spark, features)

        (
            features.write.mode("overwrite")
            .partitionBy("mes_referencia")
            .parquet(GOLD_RUC_FEATURES)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
