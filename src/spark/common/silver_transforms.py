"""
Transformaciones puras de la capa Silver (DataFrame -> DataFrame).

No leen ni escriben S3: reciben DataFrames y devuelven DataFrames. Así la
lógica de limpieza no depende del motor de consulta ni del almacenamiento,
y se prueba con datos en memoria. 02_clean_silver.py solo une lectura,
estas funciones y escritura.

Regla de calidad principal: un RUC válido tiene exactamente 11 dígitos.
Las filas con un RUC inválido no se descartan en silencio: se separan a
cuarentena con el motivo, para poder auditarlas y reportar su tasa.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    count,
    lit,
    row_number,
    trim,
    udf,
    when,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from region_normalizer import normalize_department

RUC_REGEX = r"^[0-9]{11}$"
MOTIVO_COL = "motivo_cuarentena"

MOTIVO_RUC_INVALIDO = "ruc_invalido"
MOTIVO_RUC_ENTIDAD_INVALIDO = "ruc_entidad_invalido"
MOTIVO_RUC_CONTRATISTA_INVALIDO = "ruc_contratista_invalido"

_normalize_department_udf = udf(normalize_department, StringType())


def ruc_valido(columna: Column) -> Column:
    """True si el valor tiene exactamente 11 dígitos; nulo cuenta como inválido."""
    return coalesce(trim(columna.cast("string")).rlike(RUC_REGEX), lit(False))


def preparar_padron_ruc(bronze: DataFrame) -> DataFrame:
    """Deduplica las fotos del Padrón por (RUC, PERIODO_PUBLICACION)."""
    return bronze.dropDuplicates(["RUC", "PERIODO_PUBLICACION"])


def separar_padron_ruc(entrada: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devuelve (silver, cuarentena) según la validez del RUC."""
    valido = ruc_valido(col("RUC"))
    silver = entrada.filter(valido).withColumn(
        "Departamento", _normalize_department_udf(trim(col("Departamento")))
    )
    cuarentena = entrada.filter(~valido).withColumn(
        MOTIVO_COL, lit(MOTIVO_RUC_INVALIDO)
    )
    return silver, cuarentena


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
    - Sobre las filas válidas se mantiene la regla de negocio de monto >= 0
      (esa sí se descarta, no es un problema de identificador). Un monto nulo
      no es negativo: la fila se conserva y 03 lo trata como 0.
    """
    entidad_ok = ruc_valido(col("ruc_entidad"))
    contratista_ok = col("ruc_contratista").isNull() | ruc_valido(
        col("ruc_contratista")
    )
    valido = entidad_ok & contratista_ok

    motivo = concat_ws(
        ";",
        when(~entidad_ok, lit(MOTIVO_RUC_ENTIDAD_INVALIDO)),
        when(~contratista_ok, lit(MOTIVO_RUC_CONTRATISTA_INVALIDO)),
    )

    silver = (
        entrada.filter(valido)
        .filter(coalesce(col("monto_total_orden_original") >= 0, lit(True)))
        .withColumn(
            "departamento_entidad",
            _normalize_department_udf(trim(col("departamento_entidad"))),
        )
    )
    cuarentena = entrada.filter(~valido).withColumn(MOTIVO_COL, motivo)
    return silver, cuarentena


def _conteo_por_mes(df: DataFrame, nombre: str) -> DataFrame:
    return df.groupBy("anio", "mes").agg(count(lit(1)).alias(nombre))


def construir_reporte_calidad(
    fuente: str,
    bronze: DataFrame,
    entrada: DataFrame,
    silver: DataFrame,
    cuarentena_ruc: DataFrame,
    cuarentena_bronze: DataFrame | None = None,
) -> DataFrame:
    """Una fila por (fuente, anio, mes) con conteos y tasas de cuarentena.

    - filas_bronze: filas limpias leídas de bronze (antes de deduplicar).
    - filas_cuarentena_bronze: rechazadas en bronze por esquema o fecha.
    - filas_entrada: filas tras deduplicar, las que evalúa la validación de RUC.
    - filas_cuarentena_ruc: rechazadas en Silver por RUC inválido.
    - filas_silver: filas que quedan en Silver.
    - tasa_cuarentena_bronze = rechazadas / (limpias + rechazadas) en bronze.
    - tasa_cuarentena_ruc = filas_cuarentena_ruc / filas_entrada.
    """
    conteos = [
        (bronze, "filas_bronze"),
        (entrada, "filas_entrada"),
        (silver, "filas_silver"),
        (cuarentena_ruc, "filas_cuarentena_ruc"),
    ]
    if cuarentena_bronze is not None:
        conteos.append((cuarentena_bronze, "filas_cuarentena_bronze"))

    reporte = None
    for df, nombre in conteos:
        parcial = _conteo_por_mes(df, nombre)
        reporte = (
            parcial
            if reporte is None
            else reporte.join(parcial, ["anio", "mes"], "full_outer")
        )
    assert reporte is not None
    if cuarentena_bronze is None:
        reporte = reporte.withColumn("filas_cuarentena_bronze", lit(0).cast("long"))

    columnas = [
        "filas_bronze",
        "filas_cuarentena_bronze",
        "filas_entrada",
        "filas_cuarentena_ruc",
        "filas_silver",
    ]
    reporte = reporte.fillna(0, subset=columnas)

    total_bronze = col("filas_bronze") + col("filas_cuarentena_bronze")
    return reporte.select(
        lit(fuente).alias("fuente"),
        col("anio"),
        col("mes"),
        *columnas,
        when(total_bronze > 0, col("filas_cuarentena_bronze") / total_bronze)
        .otherwise(lit(0.0))
        .alias("tasa_cuarentena_bronze"),
        when(
            col("filas_entrada") > 0, col("filas_cuarentena_ruc") / col("filas_entrada")
        )
        .otherwise(lit(0.0))
        .alias("tasa_cuarentena_ruc"),
    )


class SilverIncompletoError(RuntimeError):
    """Lo escrito en staging no coincide con lo contado en el reporte: no se publica."""


def verificar_silver_escrito(reporte: DataFrame, escrito: DataFrame) -> None:
    """Falla si las filas por (anio, mes) escritas difieren de `filas_silver` del reporte.

    Detecta meses que faltan o escrituras parciales antes de que el orquestador
    publique a la ruta viva (un silver con 5 de 12 meses llegó a publicarse).
    """
    esperado = {
        (r["anio"], r["mes"]): r["filas_silver"]
        for r in reporte.select("anio", "mes", "filas_silver").collect()
        if r["filas_silver"] > 0
    }
    real = {
        (r["anio"], r["mes"]): r["n"]
        for r in escrito.groupBy("anio", "mes").agg(count(lit(1)).alias("n")).collect()
    }
    if esperado != real:
        claves = esperado.keys() | real.keys()
        difieren = sorted(k for k in claves if esperado.get(k) != real.get(k))
        raise SilverIncompletoError(
            "Silver escrito no coincide con el reporte en (anio, mes): "
            f"{difieren}. Se aborta sin publicar."
        )


class CuarentenaExcedidaError(RuntimeError):
    """La tasa de cuarentena supera el máximo tolerado: se aborta antes de escribir Silver."""


def verificar_tasa_cuarentena(reporte: DataFrame, maximo: float) -> None:
    """Falla si algún (fuente, anio, mes) supera la tasa máxima de cuarentena por RUC.

    Es una válvula de seguridad: un cambio sistemático en el formato de la
    fuente (p. ej. RUC con decimales) mandaría casi todo a cuarentena y dejaría
    un Silver vacío. Mejor abortar antes de escribir que publicar eso.
    """
    excedidos = reporte.filter(col("tasa_cuarentena_ruc") > maximo).collect()
    if excedidos:
        detalle = ", ".join(
            f"{r['fuente']} {r['anio']}-{r['mes']}: {r['tasa_cuarentena_ruc']:.1%}"
            for r in excedidos
        )
        raise CuarentenaExcedidaError(
            f"Tasa de cuarentena por RUC sobre el máximo {maximo:.1%} en: {detalle}. "
            "Se aborta sin escribir Silver; revisar el formato de la fuente."
        )
