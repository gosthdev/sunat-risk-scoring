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
from pyspark.sql.functions import coalesce, col, concat_ws, lit, trim, udf, when
from pyspark.sql.types import StringType
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
    """Deduplica las órdenes por número de orden."""
    return bronze.dropDuplicates(["orden"])


def separar_ordenes_compra(entrada: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devuelve (silver, cuarentena).

    - ruc_entidad debe ser un RUC de 11 dígitos.
    - ruc_contratista puede venir nulo, pero si viene debe ser un RUC de 11
      dígitos (el crudo mezcla RUC y DNI de 8 dígitos; un DNI no es un RUC).
    - Sobre las filas válidas se mantiene la regla de negocio de monto >= 0
      (esa sí se descarta, no es un problema de identificador).
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
        .filter(col("monto_total_orden_original") >= 0)
        .withColumn(
            "departamento_entidad",
            _normalize_department_udf(trim(col("departamento_entidad"))),
        )
    )
    cuarentena = entrada.filter(~valido).withColumn(MOTIVO_COL, motivo)
    return silver, cuarentena
