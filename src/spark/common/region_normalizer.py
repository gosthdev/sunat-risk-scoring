"""
Normalización de nombres de departamento/región del Perú.

Las fuentes (SUNAT, INEI) escriben el mismo departamento de formas
distintas: mayúsculas/minúsculas, con o sin tildes, y a veces con variantes
de escritura (ej. "LIMA METROPOLITANA" vs "LIMA"). La normalización tiene
dos pasos:

  1. Limpieza algorítmica: sin tildes, mayúsculas, sin espacios extra.
  2. Tabla OVERRIDES para variantes de escritura conocidas que la limpieza
     algorítmica no resuelve por sí sola.

IMPORTANTE: la tabla OVERRIDES de abajo es un punto de partida con variantes
comunes conocidas de antemano. Debe completarse con los casos reales que
aparezcan al correr el EDA (comparar el set de valores únicos de
Departamento / departamento_entidad / CCDD-mapeado entre Padrón RUC,
Órdenes de Compra, Ingresos Tributarios y EPEN) antes de dar por cerrada
la normalización para el pipeline de producción.

Este módulo NO importa pyspark a propósito, para poder reutilizarse tanto
en los jobs de Spark (envuelto en un udf en el job que lo necesite) como en
00_prepare_small_datasets.py, que corre con pandas puro.
"""

import unicodedata


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def _clean(text: str) -> str:
    return _strip_accents(text).upper().strip()


# Variantes de escritura conocidas -> nombre canónico. Las claves ya pasaron
# por _clean() antes de buscarse en este diccionario.
OVERRIDES = {
    "LIMA METROPOLITANA": "LIMA",
    "PROVINCIA DE LIMA": "LIMA",
    "PROV. CONST. DEL CALLAO": "CALLAO",
    "PROVINCIA CONSTITUCIONAL DEL CALLAO": "CALLAO",
    "CALLAO - PROV. CONST.": "CALLAO",
}

# Los 24 departamentos + Callao, en su forma canónica (mayúsculas, sin
# tildes). Útil para validar que toda fila termine mapeada a un valor
# conocido (ver TODO de validación en 02_clean_silver.py).
CANONICAL_DEPARTMENTS = {
    "AMAZONAS",
    "ANCASH",
    "APURIMAC",
    "AREQUIPA",
    "AYACUCHO",
    "CAJAMARCA",
    "CALLAO",
    "CUSCO",
    "HUANCAVELICA",
    "HUANUCO",
    "ICA",
    "JUNIN",
    "LA LIBERTAD",
    "LAMBAYEQUE",
    "LIMA",
    "LORETO",
    "MADRE DE DIOS",
    "MOQUEGUA",
    "PASCO",
    "PIURA",
    "PUNO",
    "SAN MARTIN",
    "TACNA",
    "TUMBES",
    "UCAYALI",
}

# CCDD (código de departamento INEI usado en EPEN) -> nombre canónico.
# El código 7 (Callao) no se usa en la EPEN según el diccionario de datos.
CCDD_TO_DEPARTMENT = {
    1: "AMAZONAS",
    2: "ANCASH",
    3: "APURIMAC",
    4: "AREQUIPA",
    5: "AYACUCHO",
    6: "CAJAMARCA",
    8: "CUSCO",
    9: "HUANCAVELICA",
    10: "HUANUCO",
    11: "ICA",
    12: "JUNIN",
    13: "LA LIBERTAD",
    14: "LAMBAYEQUE",
    15: "LIMA",
    16: "LORETO",
    17: "MADRE DE DIOS",
    18: "MOQUEGUA",
    19: "PASCO",
    20: "PIURA",
    21: "PUNO",
    22: "SAN MARTIN",
    23: "TACNA",
    24: "TUMBES",
    25: "UCAYALI",
}


def normalize_department(raw_name):
    """Normaliza un nombre de departamento a su forma canónica.

    Devuelve None si raw_name es None, para que un dato faltante quede
    explícito en vez de convertirse en un string "NONE" silencioso.
    """
    if raw_name is None:
        return None
    cleaned = _clean(raw_name)
    return OVERRIDES.get(cleaned, cleaned)


def department_from_ccdd(ccdd):
    """Mapea el código CCDD (EPEN/INEI) a nombre de departamento canónico."""
    if ccdd is None:
        return None
    return CCDD_TO_DEPARTMENT.get(int(ccdd))
