"""
Procesamiento incremental por periodo: lectura de PENDING_PERIODS, rutas de
partición y borrado vía Hadoop.

Solo stdlib + pyspark (importado al usarlo): lo importan los jobs EMR y job00,
que corre sin Spark. Jamás pandas, awswrangler, openpyxl ni pyarrow.
"""

import json
import os
import re
from importlib import resources
from pathlib import Path

PENDING_ENV = "PENDING_PERIODS"

_REGISTRO_REPO = Path(__file__).resolve().parents[3] / "config" / "fuentes.json"
_FUENTE = re.compile(r"^[a-z][a-z0-9_]*$")
_PERIODO = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parsear(valor: str) -> dict[str, list[tuple[int, int]] | None]:
    """`fuente=FULL fuente2=2026-04,2026-05` -> {fuente: None | [(anio, mes)]}.

    Los tokens van separados por espacio; `;` también (es la forma de viajar en
    sparkSubmitParameters, donde el espacio parte los argumentos). Mal formado: ValueError.
    """
    resultado: dict[str, list[tuple[int, int]] | None] = {}
    for token in re.split(r"[\s;]+", valor.strip()):
        if not token:
            continue
        fuente, sep, alcance = token.partition("=")
        if not sep or not _FUENTE.match(fuente) or not alcance or fuente in resultado:
            raise ValueError(f"{PENDING_ENV} mal formado, token {token!r}")
        if alcance == "FULL":
            resultado[fuente] = None
            continue
        lista = []
        for p in alcance.split(","):
            m = _PERIODO.match(p)
            if not m:
                raise ValueError(
                    f"{PENDING_ENV} mal formado, periodo {p!r} en {token!r}"
                )
            lista.append((int(m[1]), int(m[2])))
        resultado[fuente] = lista
    return resultado


def periodos(fuente: str) -> list[tuple[int, int]] | None:
    """None = FULL (o env sin definir); [(anio, mes)] = esos periodos; [] = omitir la fuente."""
    valor = os.environ.get(PENDING_ENV)
    if valor is None:
        return None
    return parsear(valor).get(fuente, [])


def _registro() -> dict:
    # En common.zip el registro es fuentes/fuentes.json; fuera, <repo>/config/fuentes.json.
    try:
        texto = resources.files("fuentes").joinpath("fuentes.json").read_text()
    except (FileNotFoundError, ModuleNotFoundError):
        texto = _REGISTRO_REPO.read_text()
    return json.loads(texto)


def fuentes_de(capa: str) -> list[str]:
    """Fuentes registradas que tienen la capa dada, en el orden del registro."""
    return [f for f, d in _registro()["fuentes"].items() if capa in d["capas"]]


def ruta_particion(base: str, anio: int, mes: int, raw: bool = False) -> str:
    """raw usa `mes=MM` con cero; bronze y silver `mes=M` sin cero."""
    return (
        f"{base}/anio={anio}/mes={mes:02d}" if raw else f"{base}/anio={anio}/mes={mes}"
    )


def borrar(spark, rutas) -> None:
    """Borra recursivamente cada ruta (si existe) con el FileSystem de Hadoop."""
    jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
    for ruta in rutas:
        path = jvm.org.apache.hadoop.fs.Path(ruta)
        fs = path.getFileSystem(conf)
        if fs.exists(path):
            fs.delete(path, True)
