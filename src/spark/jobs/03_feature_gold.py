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
  - Número de RUC: ruc_prefijo (10 persona natural, 20 empresa...) y
    ruc_correlativo (crece con la fecha de inscripción: proxy de antigüedad).
  - Historia del Padrón hasta el corte (una foto por mes): meses_en_padron,
    nuevo_en_periodo, meses_hueco, cambio_tipo_facturacion, cambio_estado.
  - Padrón SEE (comprobantes electrónicos, solo afiliaciones hasta el corte):
    see_afiliado, see_antiguedad_dias, see_n_tipos, see_factura_portal,
    see_boleta_portal, see_sistema_propio, see_solo_honorarios.

Uso: spark-submit --py-files common.zip 03_feature_gold.py
"""

import sys
from functools import reduce

from dataset_versions import parse_cortes
from pyspark.sql import DataFrame
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
    substring,
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
    SILVER_SEE,
    staging_de,
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
    es_anulada = lower(coalesce(estado_col, lit(""))) == "anulada"

    # Solo se suman montos en soles: USD/CAD/EUR... no son comparables y sumarlos
    # como soles distorsiona monto total, mediana, máximo y concentración (~0.1% de
    # las órdenes). Una moneda nula se asume soles. Estas órdenes sí cuentan en n_ordenes.
    es_soles = (
        coalesce(lower(col("moneda")), lit("nuevos soles")) == "nuevos soles"
        if "moneda" in ordenes.columns
        else lit(True)
    )

    # Monto válido solo para órdenes vigentes en soles (NULL para anuladas o en otra
    # moneda, para no distorsionar la mediana ni el máximo)
    monto_valido = when(
        ~es_anulada & es_soles, coalesce(col("monto_total_orden_original"), lit(0.0))
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
    ).withColumn("cantidad_contratos_estado", col("n_ordenes").cast("long"))

    return contratacion


def build_prico_flag(spark, pricos_df=None):
    """Genera flag de inclusión en el Padrón de Principales Contribuyentes."""
    if pricos_df is None:
        pricos_df = spark.read.parquet(SILVER_PRICOS)
    return pricos_df.select(col("RUC")).distinct().withColumn("es_prico", lit(True))


def _con_mes_referencia(padron):
    if "mes_referencia" in padron.columns:
        return padron
    if "anio" in padron.columns and "mes" in padron.columns:
        return padron.withColumn(
            "mes_referencia",
            format_string("%d%02d", col("anio").cast("int"), col("mes").cast("int")),
        )
    return padron.withColumn("mes_referencia", lit("202506"))


def build_padron_historia(padron_df, mes_corte):
    """Resume las fotos mensuales del Padrón hasta `mes_corte` (yyyymm) en una fila por RUC.

    Solo mira meses <= corte: los posteriores serían información del futuro.
    """
    padron = _con_mes_referencia(padron_df).filter(
        col("mes_referencia") <= str(mes_corte)
    )
    padron = padron.withColumn("RUC", col("RUC").cast("long")).withColumn(
        "_idx",
        substring("mes_referencia", 1, 4).cast("int") * 12
        + substring("mes_referencia", 5, 2).cast("int"),
    )
    primer_idx = padron.agg(spark_min("_idx")).first()[0]

    distintos = {
        "TipoFacturacion": "cambio_tipo_facturacion",
        "Estado": "cambio_estado",
    }
    aggs = [
        countDistinct("_idx").alias("meses_en_padron"),
        spark_min("_idx").alias("_primero"),
        spark_max("_idx").alias("_ultimo"),
    ] + [
        (countDistinct(trim(col(origen))) > 1).cast("int").alias(destino)
        for origen, destino in distintos.items()
        if origen in padron.columns
    ]
    return (
        padron.groupBy("RUC")
        .agg(*aggs)
        .withColumn("nuevo_en_periodo", (col("_primero") > primer_idx).cast("int"))
        # Meses en que desaparece entre su primera y su última aparición.
        .withColumn(
            "meses_hueco",
            col("_ultimo") - col("_primero") + 1 - col("meses_en_padron"),
        )
        .drop("_primero", "_ultimo")
    )


# Flag -> texto que debe contener tipo_see (minúsculas, ver 00_prepare_small_datasets).
SEE_TIPOS = {
    "see_factura_portal": "factura portal",
    "see_boleta_portal": "boleta portal",
    "see_sistema_propio": "sistemas del contribuyente",
}
SEE_FLAGS = [*SEE_TIPOS, "see_solo_honorarios", "see_afiliado"]


def build_see_features(see_df, fecha_corte):
    """Una fila por RUC con sus afiliaciones al SEE vigentes en `fecha_corte` (yyyy-mm-dd).

    La foto del SEE es posterior al corte; filtrar por fecha_inicio evita usar
    afiliaciones que todavía no existían.
    """
    corte = to_date(lit(fecha_corte))
    see = see_df.withColumn("RUC", col("RUC").cast("long")).filter(
        col("fecha_inicio").isNotNull() & (col("fecha_inicio") <= corte)
    )
    es_honorario = col("tipo_see").contains("honorarios")
    return (
        see.groupBy("RUC")
        .agg(
            datediff(corte, spark_min("fecha_inicio")).alias("see_antiguedad_dias"),
            countDistinct("tipo_see").alias("see_n_tipos"),
            *[
                spark_max(col("tipo_see").contains(texto).cast("int")).alias(nombre)
                for nombre, texto in SEE_TIPOS.items()
            ],
            spark_min(es_honorario.cast("int")).alias("see_solo_honorarios"),
        )
        .withColumn("see_afiliado", lit(1))
    )


def attach_see_features(features_df, see_features_df):
    """Un RUC sin afiliación al corte queda con flags y n_tipos en 0 (antigüedad NULL)."""
    return features_df.join(see_features_df, on="RUC", how="left").fillna(
        {c: 0 for c in [*SEE_FLAGS, "see_n_tipos"]}
    )


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
        expr("try_cast(NroTrab as double)")
        if "NroTrab" in padron_df.columns
        else (
            expr("try_cast(nro_trabajadores as double)")
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
        # El Padrón trae "NO DISPONIBLE" en ~97% de los RUC; ese texto no es 0
        # trabajadores. sin_trabajadores solo marca 0 declarados (<= 0).
        .withColumn(
            "nrotrab_disponible",
            when(col("nro_trabajadores").isNotNull(), 1).otherwise(0),
        )
        .withColumn(
            "sin_trabajadores",
            when(
                col("nro_trabajadores").isNotNull() & (col("nro_trabajadores") <= 0.0),
                1,
            ).otherwise(0),
        )
        # RUC = 2 dígitos de tipo + 8 de correlativo (o DNI) + 1 verificador.
        .withColumn("ruc_prefijo", (col("RUC") / 10**9).cast("int").cast("string"))
        .withColumn("ruc_correlativo", ((col("RUC") / 10).cast("long") % 10**8))
        .withColumn(
            "n_actividades",
            when(ciiu_rev3_sec.isNotNull() & (trim(ciiu_rev3_sec) != ""), 2).otherwise(
                1
            ),
        )
    )

    for origen, destino in (
        ("TipoFacturacion", "tipo_facturacion"),
        ("TipoContabilidad", "tipo_contabilidad"),
    ):
        if origen in padron.columns:
            padron = padron.withColumn(destino, trim(col(origen)))

    padron_res = _con_mes_referencia(padron)

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
        "nrotrab_disponible",
        "sin_trabajadores",
        "n_actividades",
        "tipo_facturacion",
        "tipo_contabilidad",
        "ruc_prefijo",
        "ruc_correlativo",
        "mes_referencia",
    ]
    # Filtrar solo columnas que realmente existan
    available = [c for c in cols_select if c in padron_res.columns]
    return padron_res.select(*available)


def attach_regional_features(spark, features_df, regional_path=GOLD_REGIONAL_SUMMARY):
    """Une la tasa de informalidad regional desde gold/regional_summary.

    El Job 04 debe haberlo publicado antes (pipeline.yml: gold-ruc depende de
    gold-regional). Si falta o no se puede leer, el job falla: antes se capturaba
    cualquier error y la feature salía NULL para todo el Padrón sin avisar.
    """
    if "informalidad_epen_departamento" in features_df.columns:
        features_df = features_df.drop("informalidad_epen_departamento")
    regional = spark.read.parquet(regional_path)
    col_dept = (
        col("Departamento")
        if "Departamento" in regional.columns
        else (
            col("departamento")
            if "departamento" in regional.columns
            else lit("DESCONOCIDO")
        )
    )
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
        col_dept.alias("departamento"),
        col_inf.alias("informalidad_epen_departamento"),
    ).distinct()
    return features_df.join(reg_clean, on="departamento", how="left")


def compute_all_ruc_features(padron_df, contratacion_df, pricos_df, regional_df=None):
    """Combina padrón, contratación y pricos aplicando reglas de negocio de features."""
    fecha_ref_mes = last_day(
        to_date(concat(col("mes_referencia"), lit("01")), "yyyyMMdd")
    )

    joined = padron_df.join(contratacion_df, on="RUC", how="left").join(
        pricos_df, on="RUC", how="left"
    )
    # Un RUC sin órdenes tiene 0 órdenes, 0 soles y 0 entidades (no NULL). Mediana,
    # máximo y porcentajes siguen NULL: no hay órdenes de donde calcularlos.
    sin_ordenes_en_cero = {
        c: 0
        for c in ("n_ordenes", "monto_total_soles", "n_entidades_distintas")
        if c in joined.columns
    }
    features = (
        joined.fillna(sin_ordenes_en_cero)
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
            when(col("n_ordenes").isNull(), lit(0).cast("long")).otherwise(
                col("n_ordenes").cast("long")
            ),
        )
        .withColumn(
            "pct_ordenes_anuladas",
            when(col("n_ordenes").isNull() | (col("n_ordenes") == 0), 0.0).otherwise(
                col("pct_ordenes_anuladas")
            ),
        )
    )

    if regional_df is not None:
        if "informalidad_epen_departamento" in features.columns:
            features = features.drop("informalidad_epen_departamento")
        features = features.join(regional_df, on="departamento", how="left")
    elif "informalidad_epen_departamento" not in features.columns:
        features = features.withColumn(
            "informalidad_epen_departamento", lit(None).cast("double")
        )

    return features


def verificar_salida(features_df, cortes):
    """Falla si falta un corte configurado o si un RUC se repite dentro de un mes.

    Un RUC duplicado puede caer en train y test a la vez en 06 (fuga).
    """
    # Al releer el parquet, Spark infiere la partición mes_referencia=202506 como entero.
    filas_mes = features_df.select("mes_referencia").distinct().collect()
    meses = {str(r["mes_referencia"]) for r in filas_mes}
    faltan = sorted({mes for mes, _ in cortes} - meses)
    if faltan:
        raise ValueError(f"ruc_features sin filas para los cortes: {faltan}")

    repetidos = (
        features_df.groupBy("RUC", "mes_referencia")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if repetidos:
        raise ValueError("ruc_features tiene RUC repetidos dentro de un mes_referencia")


def main():
    cortes = parse_cortes()

    spark = create_spark_session("03_feature_gold")
    try:
        padron_silver = spark.read.parquet(SILVER_PADRON_RUC)
        ordenes = spark.read.parquet(SILVER_ORDENES_COMPRA)
        pricos = build_prico_flag(spark)
        see = spark.read.parquet(SILVER_SEE)

        # Un corte por (foto del Padrón, fecha de corte de órdenes); gold queda con
        # una partición mes_referencia por corte configurado.
        partes = [
            attach_see_features(
                compute_all_ruc_features(
                    build_padron_features(
                        spark, padron_df=padron_silver, mes_padron=mes
                    ).join(
                        build_padron_historia(padron_silver, mes), on="RUC", how="left"
                    ),
                    build_contratacion_estado_features(
                        spark, ordenes_df=ordenes, fecha_corte=fecha
                    ),
                    pricos,
                ),
                build_see_features(see, fecha),
            )
            for mes, fecha in cortes
        ]
        features = reduce(DataFrame.unionByName, partes)
        features = attach_regional_features(spark, features)

        (
            features.write.mode("overwrite")
            .partitionBy("mes_referencia")
            .parquet(staging_de(GOLD_RUC_FEATURES))
        )
        escrito = spark.read.parquet(staging_de(GOLD_RUC_FEATURES))
        verificar_salida(escrito, cortes)
        print(
            f"[03_feature_gold] SALIDA gold_ruc_features: filas={escrito.count()} "
            f"cortes={cortes}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
