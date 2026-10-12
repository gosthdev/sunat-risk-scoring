"""Contratos de esquema: versionados en el repo y validados antes de escribir Silver."""

import json
from pathlib import Path

import pytest
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from schema_contracts import (
    CONTRATOS,
    ContratoEsquemaError,
    contrato_a_dict,
    validar_esquema,
)
from schema_definitions import (
    ORDENES_COMPRA_CAMPOS_CRITICOS,
    ORDENES_COMPRA_SCHEMA,
    PADRON_RUC_CAMPOS_CRITICOS,
    PADRON_RUC_SCHEMA,
)

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


def test_campos_criticos_del_padron_existen_en_el_esquema():
    assert set(PADRON_RUC_CAMPOS_CRITICOS) <= set(PADRON_RUC_SCHEMA.fieldNames())


def test_campos_criticos_de_ordenes_existen_en_el_esquema():
    assert set(ORDENES_COMPRA_CAMPOS_CRITICOS) <= set(
        ORDENES_COMPRA_SCHEMA.fieldNames()
    )


@pytest.mark.parametrize("nombre", sorted(CONTRATOS))
def test_contrato_versionado_coincide_con_el_esquema(nombre):
    mayor = CONTRATOS[nombre][0].split(".")[0]
    archivo = CONTRACTS_DIR / f"{nombre}.v{mayor}.json"
    assert archivo.exists(), f"falta {archivo}; correr scripts/export_contracts.py"
    versionado = json.loads(archivo.read_text(encoding="utf-8"))
    assert versionado == contrato_a_dict(nombre), (
        f"{archivo.name} no coincide con schema_definitions.py: si el cambio es "
        "intencional, subir la versión en schema_contracts.py y correr "
        "scripts/export_contracts.py"
    )


def _con_particiones(schema: StructType) -> StructType:
    return (
        StructType(list(schema.fields))
        .add("anio", IntegerType())
        .add("mes", IntegerType())
    )


def test_esquema_conforme_pasa_la_validacion():
    validar_esquema(_con_particiones(PADRON_RUC_SCHEMA), "padron_ruc")


def test_columna_renombrada_falla_la_validacion():
    campos = [
        StructField("RUC_CONTRIBUYENTE" if f.name == "RUC" else f.name, f.dataType)
        for f in PADRON_RUC_SCHEMA.fields
    ]
    with pytest.raises(ContratoEsquemaError) as ctx:
        validar_esquema(_con_particiones(StructType(campos)), "padron_ruc")
    mensaje = str(ctx.value)
    assert "faltantes ['RUC']" in mensaje
    assert "no previstas ['RUC_CONTRIBUYENTE']" in mensaje


def test_cambio_de_tipo_falla_la_validacion():
    campos = [
        StructField("RUC", StringType()) if f.name == "RUC" else f
        for f in PADRON_RUC_SCHEMA.fields
    ]
    with pytest.raises(
        ContratoEsquemaError, match="RUC: esperado bigint, leído string"
    ):
        validar_esquema(_con_particiones(StructType(campos)), "padron_ruc")


def test_validar_no_acepta_columnas_de_otro_dataset():
    otro = StructType([StructField("RUC", LongType())])
    with pytest.raises(ContratoEsquemaError):
        validar_esquema(otro, "ordenes_compra")
