"""Silver de Órdenes de Compra: dedupe global por orden, RUC y monto válidos."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, lit, row_number, trim, when
from pyspark.sql.window import Window
from s3_paths import (
    BRONZE_ORDENES_COMPRA,
    BRONZE_ORDENES_COMPRA_QUARANTINE,
    SILVER_ORDENES_COMPRA,
    SILVER_ORDENES_COMPRA_QUARANTINE,
)
from silver_transforms import (
    MOTIVO_COL,
    MOTIVO_MONTO_NEGATIVO,
    MOTIVO_MONTO_NULO,
    MOTIVO_RUC_CONTRATISTA_INVALIDO,
    MOTIVO_RUC_ENTIDAD_INVALIDO,
    normalize_department_udf,
    procesar,
    ruc_valido,
)


def preparar_ordenes_compra(bronze: DataFrame) -> DataFrame:
    """Deduplica las órdenes por número de orden, quedándose con la última foto.

    La misma orden llega en varias cargas mensuales (≈37% de las filas de bronze).
    `dropDuplicates` elegía una versión y una partición (anio, mes) distintas en
    cada evaluación, y colapsaba en una sola todas las órdenes con `orden` nulo.
    Aquí gana la carga más reciente y las filas sin `orden` pasan sin deduplicar.
    """
    orden_de_recencia = [col("anio").desc(), col("mes").desc()]
    if "fecha_de_emision" in bronze.columns:
        orden_de_recencia.append(col("fecha_de_emision").desc())
    ultima = Window.partitionBy("orden").orderBy(*orden_de_recencia)
    con_orden = (
        bronze.filter(col("orden").isNotNull())
        .withColumn("_rn", row_number().over(ultima))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    return con_orden.unionByName(bronze.filter(col("orden").isNull()))


def separar_ordenes_compra(entrada: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devuelve (silver, cuarentena).

    - ruc_entidad debe ser un RUC de 11 dígitos.
    - ruc_contratista puede venir nulo, pero si viene debe ser un RUC de 11
      dígitos (el crudo mezcla RUC y DNI de 8 dígitos; un DNI no es un RUC).
    - monto_total_orden_original debe existir y ser >= 0. Un monto nulo o negativo
      no se descarta en silencio: va a cuarentena con su motivo.
    """
    entidad_ok = ruc_valido(col("ruc_entidad"))
    contratista_ok = col("ruc_contratista").isNull() | ruc_valido(
        col("ruc_contratista")
    )
    monto = col("monto_total_orden_original")
    aceptada = entidad_ok & contratista_ok & coalesce(monto >= 0, lit(False))

    motivo = concat_ws(
        ";",
        when(~entidad_ok, lit(MOTIVO_RUC_ENTIDAD_INVALIDO)),
        when(~contratista_ok, lit(MOTIVO_RUC_CONTRATISTA_INVALIDO)),
        when(monto.isNull(), lit(MOTIVO_MONTO_NULO)),
        when(monto < 0, lit(MOTIVO_MONTO_NEGATIVO)),
    )

    silver = entrada.filter(aceptada).withColumn(
        "departamento_entidad",
        normalize_department_udf(trim(col("departamento_entidad"))),
    )
    cuarentena = entrada.filter(~aceptada).withColumn(MOTIVO_COL, motivo)
    return silver, cuarentena


def silver(spark, periodos):
    # La dedupe necesita todas las versiones de cada orden: cualquier periodo
    # pendiente recalcula la tabla completa (publicacion "completa" en el registro).
    procesar(
        spark,
        "ordenes_compra",
        BRONZE_ORDENES_COMPRA,
        BRONZE_ORDENES_COMPRA_QUARANTINE,
        preparar_ordenes_compra,
        separar_ordenes_compra,
        SILVER_ORDENES_COMPRA,
        SILVER_ORDENES_COMPRA_QUARANTINE,
        None,
    )
