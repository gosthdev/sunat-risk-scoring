"""Regenera contracts/*.json desde schema_definitions.py.

Uso: python scripts/export_contracts.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src" / "spark" / "common"))

from schema_contracts import exportar_contratos

if __name__ == "__main__":
    for ruta in exportar_contratos(ROOT / "contracts"):
        print(f"✓ {ruta.relative_to(ROOT)}")
