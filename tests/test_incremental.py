"""incremental.periodos / ruta_particion / fuentes_de y la regla EMR (sin pandas ni awswrangler)."""

import subprocess
import sys
from pathlib import Path

import incremental
import pytest

ROOT = Path(__file__).resolve().parent.parent
COMMON = ROOT / "src" / "spark" / "common"


def test_tokens_full_lista_y_fuente_ausente(monkeypatch):
    monkeypatch.setenv(
        "PENDING_PERIODS", "licitaciones=FULL padron_ruc=2026-04,2026-05"
    )
    assert incremental.periodos("licitaciones") is None
    assert incremental.periodos("padron_ruc") == [(2026, 4), (2026, 5)]
    assert incremental.periodos("ordenes_compra") == []


def test_env_sin_definir_es_full_para_todas(monkeypatch):
    monkeypatch.delenv("PENDING_PERIODS", raising=False)
    assert incremental.periodos("padron_ruc") is None
    assert incremental.periodos("ordenes_compra") is None


def test_env_vacia_omite_todo(monkeypatch):
    monkeypatch.setenv("PENDING_PERIODS", "")
    assert incremental.periodos("padron_ruc") == []


def test_acepta_punto_y_coma_como_separador(monkeypatch):
    monkeypatch.setenv("PENDING_PERIODS", "padron_ruc=2026-04;see=FULL")
    assert incremental.periodos("padron_ruc") == [(2026, 4)]
    assert incremental.periodos("see") is None


@pytest.mark.parametrize(
    "valor",
    [
        "padron_ruc",
        "padron_ruc=",
        "padron_ruc=2026-13",
        "padron_ruc=2026-4",
        "padron_ruc=full",
        "=FULL",
        "a=FULL a=2026-01",
        "padron_ruc=2026-04,",
    ],
)
def test_token_mal_formado_falla_aunque_sea_de_otra_fuente(monkeypatch, valor):
    monkeypatch.setenv("PENDING_PERIODS", f"see=FULL {valor}")
    with pytest.raises(ValueError):
        incremental.periodos("see")


def test_ruta_particion_raw_con_cero_y_bronze_silver_sin_cero():
    assert incremental.ruta_particion("s3://b/x", 2025, 1, raw=True) == (
        "s3://b/x/anio=2025/mes=01"
    )
    assert incremental.ruta_particion("s3://b/x", 2025, 1) == "s3://b/x/anio=2025/mes=1"
    assert (
        incremental.ruta_particion("s3://b/x", 2025, 12) == "s3://b/x/anio=2025/mes=12"
    )


def test_fuentes_de_lee_el_registro():
    assert incremental.fuentes_de("bronze") == ["padron_ruc", "ordenes_compra"]
    assert incremental.fuentes_de("silver") == ["padron_ruc", "ordenes_compra"]
    assert set(incremental.fuentes_de("small")) == {
        "pricos",
        "ingresos_tributarios",
        "epen",
        "ssco",
        "see",
    }


def test_jobs_emr_no_importan_pandas_ni_similares():
    """01/02, incremental y fuentes bronze/silver corren en EMR: sin pandas, awswrangler, openpyxl ni pyarrow."""
    codigo = (
        "import sys, importlib\n"
        "for m in ('pandas', 'awswrangler', 'openpyxl', 'pyarrow'):\n"
        "    sys.modules[m] = None\n"
        "import incremental, bronze_io, silver_transforms\n"
        "for m in ('01_ingest_bronze', '02_clean_silver'):\n"
        "    importlib.import_module(m)\n"
        "for capa in ('bronze', 'silver'):\n"
        "    for f in incremental.fuentes_de(capa):\n"
        "        importlib.import_module(f'fuentes.{capa}.{f}')\n"
    )
    env_path = f"{COMMON}:{ROOT / 'src/spark/jobs'}"
    subprocess.run(
        [sys.executable, "-c", codigo],
        check=True,
        env={"PYTHONPATH": env_path, "PATH": "/usr/bin"},
    )
