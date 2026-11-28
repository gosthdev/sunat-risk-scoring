"""Escribe el JSON {fuente: "mensual" | "sin_periodo"} (ordenado) del portal de carga.

Uso: python scripts/gen_fuentes_permitidas.py SALIDA.json [SALIDA2.json ...]
Fuentes = claves de config/fuentes.json["fuentes"]; "sin_periodo" si están en
urls.json["small_datasets"], "mensual" si son claves de primer nivel de urls.json.
Una fuente ausente de urls.json hace fallar el script.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def fuentes(root: Path = ROOT) -> dict[str, str]:
    cfg = json.loads((root / "config/fuentes.json").read_text(encoding="utf-8"))
    urls = json.loads((root / "src/ingestion/urls.json").read_text(encoding="utf-8"))
    small = urls.get("small_datasets", {})
    mensual = set(urls) - {"small_datasets"}
    faltan = set(cfg["fuentes"]) - mensual - set(small)
    if faltan:
        raise SystemExit(f"fuentes sin clasificar en urls.json: {sorted(faltan)}")
    return {
        f: "sin_periodo" if f in small else "mensual" for f in sorted(cfg["fuentes"])
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for out in sys.argv[1:]:
        Path(out).write_text(json.dumps(fuentes(), indent=2) + "\n", encoding="utf-8")
