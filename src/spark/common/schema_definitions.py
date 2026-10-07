"""
Esquemas explícitos (StructType) para los datasets procesados con Spark.

Usar esquemas explícitos evita que Spark infiera tipos (más lento, y la
inferencia falla silenciosamente con columnas mixtas como ruc_contratista,
que a veces trae RUC de 11 dígitos y a veces DNI de 8).

Tipos definidos según DiccionarioDeDatosIntegrado.md.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# RUC (11 dígitos, hasta ~2*10^10) y nro_de_orden (hasta ~4.6*10^9) exceden
# el rango de IntegerType (max ~2.1*10^9) -> deben ser LongType.

PADRON_RUC_SCHEMA = StructType(
    [
        StructField("RUC", LongType(), nullable=False),
        StructField("Estado", StringType(), nullable=True),
        StructField("Condicion", StringType(), nullable=True),
        StructField("Tipo", StringType(), nullable=True),
        StructField(
            "Actividad_Economica_CIIU_revision3_Principal", StringType(), nullable=True
        ),
        StructField(
            "Actividad_Economica_CIIU_revision3_Secundaria", StringType(), nullable=True
        ),
        StructField(
            "Actividad_Economica_CIIU_revision4_Principal", StringType(), nullable=True
        ),
        StructField("NroTrab", StringType(), nullable=True),
        StructField("TipoFacturacion", StringType(), nullable=True),
        StructField("TipoContabilidad", StringType(), nullable=True),
        StructField("ComercioExterior", StringType(), nullable=True),
        StructField("UBIGEO", IntegerType(), nullable=True),
        StructField("Departamento", StringType(), nullable=True),
        StructField("Provincia", StringType(), nullable=True),
        StructField("Distrito", StringType(), nullable=True),
        StructField("PERIODO_PUBLICACION", IntegerType(), nullable=True),
    ]
)

ORDENES_COMPRA_SCHEMA = StructType(
    [
        StructField("entidad", StringType(), nullable=True),
        StructField("ruc_entidad", LongType(), nullable=True),
        StructField("departamento_entidad", StringType(), nullable=True),
        StructField("tipoorden", StringType(), nullable=True),
        StructField("nro_de_orden", LongType(), nullable=True),
        StructField("orden", StringType(), nullable=True),
        StructField("descripcion_orden", StringType(), nullable=True),
        StructField("objetocontractual", StringType(), nullable=True),
        StructField("estadocontratacion", StringType(), nullable=True),
        StructField("tipodecontratacion", StringType(), nullable=True),
        StructField("monto_total_orden_original", DoubleType(), nullable=True),
        StructField("moneda", StringType(), nullable=True),
        # Llega a veces como RUC (11 dig) y a veces como DNI (8 dig) ->
        # se mantiene como string para no perder ceros a la izquierda ni
        # forzar un cast que descarte DNIs silenciosamente en bronze.
        StructField("ruc_contratista", StringType(), nullable=True),
        StructField("nombre_razon_contratista", StringType(), nullable=True),
        StructField("fecha_registro", DateType(), nullable=True),
        StructField("fecha_de_emision", DateType(), nullable=True),
        StructField("fecha_compromiso_presupuestal", DateType(), nullable=True),
        StructField("fecha_de_notificacion", DateType(), nullable=True),
    ]
)

# Rango de años válido para columnas de fecha. El diccionario de datos
# reporta filas reales con años 2202 y 8202 en Órdenes de Compra (error de
# digitación, probablemente 2022/2025/2026). Esas filas se separan a
# bronze/quarantine en 01_ingest_bronze.py en vez de pasar como válidas.
VALID_YEAR_RANGE = (2000, 2026)

ORDENES_COMPRA_DATE_COLUMNS = [
    "fecha_registro",
    "fecha_de_emision",
    "fecha_compromiso_presupuestal",
    "fecha_de_notificacion",
]
