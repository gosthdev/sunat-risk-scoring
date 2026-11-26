"""common.zip (package_jobs.sh) debe exponer los paquetes fuentes.* y el registro."""

import json
import subprocess
import sys
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_ZIP = _ROOT / "src" / "spark" / "jobs" / "common.zip"


def _empaquetar() -> Path:
    subprocess.run(
        ["bash", str(_ROOT / "src/spark/jobs/package_jobs.sh")],
        check=True,
        capture_output=True,
    )
    return _ZIP


def test_zip_incluye_init_y_registro():
    with zipfile.ZipFile(_empaquetar()) as zf:
        nombres = set(zf.namelist())
        registro = json.loads(zf.read("fuentes/fuentes.json"))
    for capa in ("", "small/", "bronze/", "silver/"):
        assert f"fuentes/{capa}__init__.py" in nombres
    assert "s3_paths.py" in nombres
    assert registro == json.loads((_ROOT / "config/fuentes.json").read_text())


def test_import_desde_zip_sin_el_arbol_local():
    zip_path = _empaquetar()
    # -I: sin cwd ni PYTHONPATH, así solo el zip puede resolver los módulos.
    codigo = (
        "import importlib, importlib.resources, json, sys\n"
        f"sys.path.insert(0, {str(zip_path)!r})\n"
        "for m in ('fuentes', 'fuentes.small', 'fuentes.bronze', 'fuentes.silver'):\n"
        "    mod = importlib.import_module(m)\n"
        "    assert mod.__file__.startswith(sys.path[0]), mod.__file__\n"
        "pr = importlib.import_module('fuentes.silver.padron_ruc')\n"
        "assert pr.__file__.startswith(sys.path[0]) and callable(pr.silver), pr.__file__\n"
        "import incremental\n"
        "assert incremental.fuentes_de('silver') == ['padron_ruc', 'ordenes_compra']\n"
        "reg = json.loads(importlib.resources.files('fuentes').joinpath('fuentes.json').read_text())\n"
        "assert 'compartidos' in reg and 'fuentes' in reg\n"
    )
    subprocess.run([sys.executable, "-I", "-c", codigo], check=True)
