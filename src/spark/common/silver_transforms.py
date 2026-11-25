"""
Lo compartido de la capa Silver: validación de RUC, reporte de calidad,
verificaciones y `procesar`, el flujo leer bronze -> limpiar -> staging.

Todo lo demás son funciones puras (DataFrame -> DataFrame) que no leen ni
escriben S3. La limpieza propia de cada fuente vive en fuentes/silver/<fuente>.py.

Regla de calidad principal: un RUC válido tiene exactamente 11 dígitos.
Las filas con un RUC inválido no se descartan en silencio: se separan a
cuarentena con el motivo, para poder auditarlas y reportar su tasa.
"""

import os
from functools import reduce

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    lit,
    trim,
    udf,
    when,
)
from pyspark.sql.types import StringType
from pyspark.storagelevel import StorageLevel
from region_normalizer import normalize_department
from s3_paths import SILVER_QUALITY_REPORT, staging_de
from schema_contracts import validar_esquema

RUC_REGEX = r"^[0-9]{11}$"
MOTIVO_COL = "motivo_cuarentena"

MOTIVO_RUC_INVALIDO = "ruc_invalido"
MOTIVO_RUC_ENTIDAD_INVALIDO = "ruc_entidad_invalido"
MOTIVO_RUC_CONTRATISTA_INVALIDO = "ruc_contratista_invalido"
MOTIVO_MONTO_NULO = "monto_nulo"
MOTIVO_MONTO_NEGATIVO = "monto_negativo"

normalize_department_udf = udf(normalize_department, StringType())


def ruc_valido(columna: Column) -> Column:
    """True si el valor tiene exactamente 11 dígitos; nulo cuenta como inválido."""
    return coalesce(trim(columna.cast("string")).rlike(RUC_REGEX), lit(False))


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
    - filas_cuarentena_ruc: rechazadas en Silver (RUC inválido, monto nulo o negativo).
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


def filtrar_periodos(
    df: DataFrame, periodos: list[tuple[int, int]] | None
) -> DataFrame:
    """Solo las filas de esos (anio, mes); None deja todo (FULL)."""
    if periodos is None:
        return df
    return df.filter(
        reduce(
            lambda a, b: a | b,
            [(col("anio") == a) & (col("mes") == m) for a, m in periodos],
        )
    )


def fusionar_reporte(nuevo: DataFrame, previo: DataFrame | None) -> DataFrame:
    """Reporte vivo menos las claves (fuente, anio, mes) recalculadas, unido al nuevo.

    Así el reporte publicado sigue completo aunque la corrida solo toque algunos periodos.
    """
    if previo is None:
        return nuevo
    conservado = previo.select(*nuevo.columns).join(
        nuevo.select("fuente", "anio", "mes"), ["fuente", "anio", "mes"], "left_anti"
    )
    return conservado.unionByName(nuevo)


def _leer_opcional(spark, path) -> DataFrame | None:
    """Lee un parquet que puede no existir (bronze no escribe cuarentena vacía)."""
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        return None


def _leer_escrito(spark, path) -> DataFrame:
    """Lo escrito en staging; vacío si no quedó ningún parquet (ningún periodo con filas limpias).

    Otros errores de lectura (permisos, rutas, etc.) se propagan.
    """
    try:
        return spark.read.parquet(path)
    except AnalysisException as exc:
        if exc.getCondition() != "UNABLE_TO_INFER_SCHEMA":
            raise
        return spark.createDataFrame([], "anio int, mes int")


def _log_reporte(reporte: DataFrame) -> None:
    # Línea fácil de grepear en los logs del driver (stdout).
    for r in reporte.orderBy("fuente", "anio", "mes").collect():
        print(
            f"CALIDAD fuente={r['fuente']} anio={r['anio']} mes={r['mes']} "
            f"bronze={r['filas_bronze']} cuarentena_bronze={r['filas_cuarentena_bronze']} "
            f"tasa_bronze={r['tasa_cuarentena_bronze']:.4f} "
            f"cuarentena_ruc={r['filas_cuarentena_ruc']} "
            f"tasa_ruc={r['tasa_cuarentena_ruc']:.4f} silver={r['filas_silver']}"
        )


def _reporte_previo(spark, fuente: str, periodos) -> DataFrame | None:
    """Reporte ya escrito en staging por esta corrida; si no, el publicado."""
    previo = _leer_opcional(spark, staging_de(SILVER_QUALITY_REPORT)) or _leer_opcional(
        spark, SILVER_QUALITY_REPORT
    )
    # FULL de la fuente: sus claves viejas (incluso huérfanas) no sobreviven.
    if previo is not None and periodos is None:
        previo = previo.filter(col("fuente") != fuente)
    elif previo is not None:
        # Un periodo sin filas en bronze no genera fila nueva: la vieja no debe sobrevivir.
        pedidos = spark.createDataFrame(periodos, "anio int, mes int")
        propias = previo.filter(col("fuente") == fuente)
        previo = previo.filter(col("fuente") != fuente).unionByName(
            propias.join(pedidos, ["anio", "mes"], "left_anti").select(*previo.columns)
        )
    if previo is None:
        return None
    # En memoria: se reescribe la misma ruta de staging de la que se leyó.
    return spark.createDataFrame(previo.collect(), previo.schema)


def procesar(
    spark,
    fuente,
    bronze_path,
    bronze_quarantine_path,
    preparar,
    separar,
    silver_path,
    silver_quarantine_path,
    periodos,
):
    """Bronze (filtrado por periodos) -> preparar -> separar -> staging, con reporte de calidad."""
    bronze = filtrar_periodos(spark.read.parquet(bronze_path), periodos)
    # Contrato de esquema: un cambio en la fuente falla acá, antes de escribir Silver.
    validar_esquema(bronze.schema, fuente)
    cuarentena_bronze = _leer_opcional(spark, bronze_quarantine_path)
    if cuarentena_bronze is not None:
        cuarentena_bronze = filtrar_periodos(cuarentena_bronze, periodos)
    entrada = preparar(bronze).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        silver, cuarentena = separar(entrada)
        reporte = construir_reporte_calidad(
            fuente, bronze, entrada, silver, cuarentena, cuarentena_bronze
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            _log_reporte(reporte)
            # Antes de escribir nada: si la fuente cambió de formato, Silver no se toca.
            verificar_tasa_cuarentena(
                reporte, float(os.environ.get("MAX_TASA_CUARENTENA_RUC", "0.5"))
            )
            previo = _reporte_previo(spark, fuente, periodos)

            # Se escribe en staging: la publicación a la ruta viva la hace el
            # orquestador solo si el job completo termina bien.
            silver.write.mode("overwrite").partitionBy("anio", "mes").parquet(
                staging_de(silver_path)
            )
            verificar_silver_escrito(
                reporte, _leer_escrito(spark, staging_de(silver_path))
            )
            if cuarentena.take(1):
                cuarentena.write.mode("overwrite").partitionBy("anio", "mes").parquet(
                    staging_de(silver_quarantine_path)
                )
            fusionar_reporte(reporte, previo).write.mode("overwrite").partitionBy(
                "fuente"
            ).parquet(staging_de(SILVER_QUALITY_REPORT))
        finally:
            reporte.unpersist()
    finally:
        entrada.unpersist()
