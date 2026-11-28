import sys
from pathlib import Path

# Los scripts de la prueba viven en perf/ (fuera de src/): se exponen por nombre,
# igual que hace --py-files en EMR.
_PERF = Path(__file__).resolve().parents[2] / "perf"
for _carpeta in ("spark", "analysis"):
    _ruta = str(_PERF / _carpeta)
    if _ruta not in sys.path:
        sys.path.insert(0, _ruta)
