"""Claves de las copias de la prueba de escalabilidad.

La copia 0 es el dato real. Cada copia r >= 1 recibe RUC nuevos de 11 dígitos:

    RUC_r = 21·10^9 + (r − 1)·bloque + rank(RUC_0)

donde rank es la posición del RUC en la lista ordenada de RUC distintos y bloque
es el espacio reservado por copia. Todo RUC real es < 21·10^9 (prefijos 10 a 20),
así que las copias no chocan con RUC reales ni entre sí, y siguen teniendo 11
dígitos (Silver descarta cualquier RUC que no los tenga).
"""

import math

RUC_INICIO_COPIAS = 21 * 10**9
RUC_FIN = 10**11  # primer número de 12 dígitos
NRO_ORDEN_DESPLAZAMIENTO = 10**10  # nro_de_orden real llega hasta ~4,6·10^9


def tamano_bloque(n_rucs: int) -> int:
    """Espacio por copia: n_rucs con 10 % de holgura, redondeado al millón."""
    return math.ceil(1.1 * n_rucs / 10**6) * 10**6


def max_copias(bloque: int) -> int:
    """Copias totales (contando la real) que caben sin pasar de 11 dígitos."""
    return (RUC_FIN - RUC_INICIO_COPIAS) // bloque + 1


def validar_factor(factor: int, bloque: int) -> None:
    if not 1 <= factor <= max_copias(bloque):
        raise ValueError(
            f"factor={factor}: con bloque={bloque} caben hasta {max_copias(bloque)} copias"
        )


def ruc_copia(ruc: int, rank: int, copia: int, bloque: int) -> int:
    if copia == 0:
        return ruc
    if not 0 <= rank < bloque:
        raise ValueError(f"rank={rank} fuera del bloque {bloque}")
    return RUC_INICIO_COPIAS + (copia - 1) * bloque + rank


def nro_orden_copia(nro: int, copia: int) -> int:
    return nro + copia * NRO_ORDEN_DESPLAZAMIENTO


def orden_copia(orden: str, copia: int) -> str:
    return orden if copia == 0 else f"{orden}-R{copia}"
