"""
Cortes de cada versión del dataset: foto mensual del Padrón + fecha de corte de órdenes.

03_feature_gold construye gold/ruc_features con uno o más cortes (una partición
mes_referencia por corte) y 06_dataset_curado elige el corte de su versión. Tener
el mapa en un solo lugar evita que 03 y 06 deriven el mes por separado.
"""

import os
from datetime import date

# v1: techo de referencia (incluye Estado/Condicion en el entrenamiento).
# v2: versión oficial sin fuga.
# v3: igual que v2 pero con el Padrón hasta mar-2026 (más historia, ~13 SSCO menos).
VERSION_CORTES = {
    "v1": ("202506", "2025-06-30"),
    "v2": ("202512", "2025-12-31"),
    "v3": ("202603", "2026-03-31"),
}


def mes_de_version(version: str) -> str:
    try:
        return VERSION_CORTES[version][0]
    except KeyError:
        raise ValueError(
            f"DATASET_VERSION desconocida: {version!r}; válidas: {sorted(VERSION_CORTES)}"
        ) from None


def fecha_de_version(version: str) -> str:
    mes_de_version(version)  # valida la versión
    return VERSION_CORTES[version][1]


def parse_cortes(valor: str | None = None) -> list[tuple[str, str]]:
    """Lee GOLD_CORTES ("202506:2025-06-30,202512:2025-12-31") como [(mes, fecha)].

    Vacío, ausente o "todas": todas las versiones de VERSION_CORTES. El orquestador
    manda "todas" porque EMR no acepta bien una variable de entorno vacía.
    """
    if valor is None:
        valor = os.environ.get("GOLD_CORTES", "")
    if not valor.strip() or valor.strip().lower() == "todas":
        return list(VERSION_CORTES.values())

    cortes = []
    for par in valor.split(","):
        mes, _, fecha = par.strip().partition(":")
        if not (len(mes) == 6 and mes.isdigit() and 1 <= int(mes[4:]) <= 12):
            raise ValueError(f"Mes de corte inválido (esperado yyyymm): {par!r}")
        try:
            date.fromisoformat(fecha)
        except ValueError:
            raise ValueError(
                f"Fecha de corte inválida (esperado yyyy-mm-dd): {par!r}"
            ) from None
        cortes.append((mes, fecha))
    return cortes
