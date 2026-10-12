"""
Contratos de esquema entre capas.

Las capas del pipeline solo se comunican por Parquet, particiones y catálogo,
así que el esquema de cada dataset es el contrato entre ellas. Los StructType
de schema_definitions.py son la fuente de verdad; contracts/*.json es su copia
versionada y revisable en los PR (un test exige que coincidan).

validar_esquema compara un esquema leído contra el contrato y falla con un
mensaje claro ANTES de escribir: un renombre o un cambio de tipo en la fuente
detiene el job en validación y no llega a Silver.
"""

import json
from pathlib import Path

from pyspark.sql.types import StructType
from schema_definitions import ORDENES_COMPRA_SCHEMA, PADRON_RUC_SCHEMA

# Columnas que Spark agrega al leer carpetas Hive-style (anio=.../mes=...).
COLUMNAS_PARTICION = ("anio", "mes")

# nombre -> (versión del contrato, esquema). Cambiar un esquema obliga a
# subir la versión y regenerar contracts/ con scripts/export_contracts.py.
CONTRATOS: dict[str, tuple[str, StructType]] = {
    "padron_ruc": ("1.0.0", PADRON_RUC_SCHEMA),
    "ordenes_compra": ("1.0.0", ORDENES_COMPRA_SCHEMA),
}


class ContratoEsquemaError(ValueError):
    """El esquema leído no cumple el contrato del dataset."""


def contrato_a_dict(nombre: str) -> dict:
    version, esquema = CONTRATOS[nombre]
    return {
        "name": nombre,
        "version": version,
        "fields": [
            {
                "name": campo.name,
                "type": campo.dataType.simpleString(),
                "nullable": campo.nullable,
            }
            for campo in esquema.fields
        ],
    }


def exportar_contratos(directorio: Path) -> list[Path]:
    """Escribe contracts/<nombre>.v<mayor>.json para cada contrato."""
    directorio.mkdir(parents=True, exist_ok=True)
    escritos = []
    for nombre in CONTRATOS:
        contrato = contrato_a_dict(nombre)
        mayor = contrato["version"].split(".")[0]
        destino = directorio / f"{nombre}.v{mayor}.json"
        destino.write_text(
            json.dumps(contrato, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        escritos.append(destino)
    return escritos


def validar_esquema(
    leido: StructType,
    nombre: str,
    columnas_extra_permitidas: tuple[str, ...] = COLUMNAS_PARTICION,
) -> None:
    """Falla con ContratoEsquemaError si `leido` no cumple el contrato `nombre`.

    Compara nombre y tipo de cada columna (la nulabilidad no cuenta: Parquet
    la relaja al leer). Las columnas de partición son extras permitidas.
    """
    version, esperado = CONTRATOS[nombre]
    esperado_por_nombre = {c.name: c.dataType.simpleString() for c in esperado.fields}
    leido_por_nombre = {c.name: c.dataType.simpleString() for c in leido.fields}

    faltantes = sorted(set(esperado_por_nombre) - set(leido_por_nombre))
    sobrantes = sorted(
        set(leido_por_nombre)
        - set(esperado_por_nombre)
        - set(columnas_extra_permitidas)
    )
    tipos = sorted(
        f"{col}: esperado {esperado_por_nombre[col]}, leído {leido_por_nombre[col]}"
        for col in set(esperado_por_nombre) & set(leido_por_nombre)
        if esperado_por_nombre[col] != leido_por_nombre[col]
    )

    if faltantes or sobrantes or tipos:
        partes = []
        if faltantes:
            partes.append(f"columnas faltantes {faltantes}")
        if sobrantes:
            partes.append(f"columnas no previstas {sobrantes}")
        if tipos:
            partes.append(f"tipos distintos {tipos}")
        raise ContratoEsquemaError(
            f"El dataset '{nombre}' no cumple el contrato v{version}: "
            + "; ".join(partes)
            + ". No se escribe Silver. Si el cambio es legítimo, actualizar "
            "schema_definitions.py, subir la versión y regenerar contracts/."
        )
