"""Las copias tienen RUC únicos de 11 dígitos que no chocan con RUC reales."""

import pytest
from perf_keys import (
    RUC_INICIO_COPIAS,
    max_copias,
    nro_orden_copia,
    orden_copia,
    ruc_copia,
    tamano_bloque,
    validar_factor,
)

# RUC distintos medidos en producción (Silver Padrón, 15 fotos; perf/results/s0_baseline.json).
RUC_REALES = 13_761_829


def test_la_copia_0_es_el_dato_real():
    assert ruc_copia(20100000001, rank=5, copia=0, bloque=10) == 20100000001
    assert orden_copia("OC-1", 0) == "OC-1"
    assert nro_orden_copia(123, 0) == 123


def test_con_el_volumen_real_caben_100_copias():
    bloque = tamano_bloque(RUC_REALES)
    assert bloque == 16_000_000
    assert max_copias(bloque) >= 100
    validar_factor(100, bloque)


def test_factor_que_no_cabe_en_11_digitos_falla():
    bloque = tamano_bloque(RUC_REALES)
    with pytest.raises(ValueError):
        validar_factor(max_copias(bloque) + 1, bloque)


def test_100_copias_dan_ruc_unicos_de_11_digitos_sin_chocar_con_reales():
    bloque = tamano_bloque(RUC_REALES)
    ranks = [0, 1, 7_654_321, RUC_REALES - 1]
    nuevos = [
        ruc_copia(10**10, rank, copia, bloque)
        for copia in range(1, 100)
        for rank in ranks
    ]
    assert len(set(nuevos)) == len(nuevos)
    assert all(len(str(r)) == 11 for r in nuevos)
    assert all(r >= RUC_INICIO_COPIAS > 20_999_999_999 for r in nuevos)


def test_todas_las_combinaciones_son_distintas_en_un_bloque_pequeno():
    bloque, copias = 10, 100
    nuevos = {
        ruc_copia(0, rank, copia, bloque)
        for copia in range(1, copias)
        for rank in range(bloque)
    }
    assert len(nuevos) == (copias - 1) * bloque


def test_rank_fuera_del_bloque_falla():
    with pytest.raises(ValueError):
        ruc_copia(20100000001, rank=10, copia=1, bloque=10)


def test_orden_y_nro_de_orden_se_distinguen_por_copia():
    assert orden_copia("OC-1", 3) == "OC-1-R3"
    assert nro_orden_copia(4_600_000_000, 2) == 24_600_000_000
