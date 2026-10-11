"""Contratos de esquema: los campos críticos del reporte existen en los esquemas."""

from schema_definitions import (
    ORDENES_COMPRA_CAMPOS_CRITICOS,
    ORDENES_COMPRA_SCHEMA,
    PADRON_RUC_CAMPOS_CRITICOS,
    PADRON_RUC_SCHEMA,
)


def test_campos_criticos_del_padron_existen_en_el_esquema():
    assert set(PADRON_RUC_CAMPOS_CRITICOS) <= set(PADRON_RUC_SCHEMA.fieldNames())


def test_campos_criticos_de_ordenes_existen_en_el_esquema():
    assert set(ORDENES_COMPRA_CAMPOS_CRITICOS) <= set(
        ORDENES_COMPRA_SCHEMA.fieldNames()
    )
