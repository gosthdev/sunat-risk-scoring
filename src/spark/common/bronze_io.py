"""
E/S de bronze compartida por las fuentes: lectura de raw (completa o por periodos),
separación de filas corruptas y escritura de clean/cuarentena particionadas.

Solo stdlib + pyspark (importado al usarlo): corre en EMR, jamás pandas,
awswrangler, openpyxl ni pyarrow.
"""

from incremental import borrar, ruta_particion

CORRUPT_COLUMN = "_corrupt_record"


def leer_raw(spark, base, schema, periodos_):
    """Lee CSV crudo en modo PERMISSIVE, agregando columna de registro corrupto.

    PERMISSIVE + columnNameOfCorruptRecord hace que las filas que no calzan
    con el esquema no se pierdan: quedan con _corrupt_record poblado.

    enforceSchema=false compara la cabecera del CSV con el esquema: si la fuente
    renombra, quita o reordena una columna el job falla aquí, en vez de mapear
    por posición y llenar bronze con datos en la columna equivocada.

    periodos_ None lee todo `base`; con lista, solo `anio=Y/mes=MM/` y `basePath`
    conserva anio/mes como columnas.
    """
    from pyspark.sql.types import StructType

    # Copia: StructType.add muta el esquema y contaminaría el contrato.
    schema_con_corrupto = StructType(list(schema.fields)).add(CORRUPT_COLUMN, "string")
    lector = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .option("enforceSchema", "false")
        .schema(schema_con_corrupto)
    )
    if periodos_ is None:
        return lector.csv(base)
    rutas = [ruta_particion(base, a, m, raw=True) for a, m in periodos_]
    return lector.option("basePath", base).csv(rutas)


def separar_esquema(df):
    """Separa filas que Spark no pudo mapear al esquema esperado."""
    from pyspark.sql.functions import col

    clean = df.filter(col(CORRUPT_COLUMN).isNull()).drop(CORRUPT_COLUMN)
    return clean, df.filter(col(CORRUPT_COLUMN).isNotNull())


def escribir_bronze(
    spark, nombre, clean, cuarentena, ruta_clean, ruta_cuarentena, periodos_
) -> None:
    """Escribe clean y cuarentena particionados por (anio, mes), aunque la cuarentena esté vacía.

    FULL: se borran clean y cuarentena completas (migra el layout viejo, sin particionar,
    y quita los meses que ya no tienen filas). Incremental: se borran antes las particiones de esos
    periodos en ambas, así una cuarentena vacía no deja la de la corrida anterior.
    """
    if periodos_ is None:
        # Con partitionOverwriteMode=dynamic, overwrite no borra los meses que ya no reciben filas.
        borrar(spark, [ruta_clean, ruta_cuarentena])
        modo = "overwrite"
    else:
        borrar(
            spark,
            [
                ruta_particion(base, a, m)
                for base in (ruta_clean, ruta_cuarentena)
                for a, m in periodos_
            ],
        )
        modo = "append"
    clean.write.mode(modo).partitionBy("anio", "mes").parquet(ruta_clean)
    cuarentena.write.mode(modo).partitionBy("anio", "mes").parquet(ruta_cuarentena)
    _log_salida(spark, nombre, ruta_clean)
    _log_salida(spark, f"{nombre}_cuarentena", ruta_cuarentena)


def _log_salida(spark, nombre, path) -> None:
    """Filas escritas en `path` (0 si no existe), fácil de grepear en el stdout del driver."""
    from pyspark.errors import AnalysisException

    try:
        filas = spark.read.parquet(path).count()
    except AnalysisException:
        filas = 0
    print(f"[01_ingest_bronze] SALIDA {nombre}: filas={filas} ruta={path}")
