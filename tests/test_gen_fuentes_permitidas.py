import importlib.util
import json
from pathlib import Path

import pytest

_p = Path(__file__).resolve().parent.parent / "scripts/gen_fuentes_permitidas.py"
_s = importlib.util.spec_from_file_location("gen_fuentes_permitidas", _p)
gen = importlib.util.module_from_spec(_s)
_s.loader.exec_module(gen)


def _repo(tmp_path, fuentes, urls):
    (tmp_path / "config").mkdir()
    (tmp_path / "src/ingestion").mkdir(parents=True)
    (tmp_path / "config/fuentes.json").write_text(json.dumps({"fuentes": fuentes}))
    (tmp_path / "src/ingestion/urls.json").write_text(json.dumps(urls))


def test_clasifica_y_ordena(tmp_path):
    _repo(
        tmp_path,
        {"z": {}, "b": {}, "a": {}},
        {"padron_ruc": {}, "a": {}, "small_datasets": {"z": "u", "b": "u"}},
    )
    r = gen.fuentes(tmp_path)
    assert list(r) == ["a", "b", "z"]
    assert r == {"a": "mensual", "b": "sin_periodo", "z": "sin_periodo"}


def test_fuente_sin_clasificar_falla(tmp_path):
    _repo(tmp_path, {"a": {}, "x": {}}, {"a": {}, "small_datasets": {}})
    with pytest.raises(SystemExit):
        gen.fuentes(tmp_path)


def test_repo_real():
    f = gen.fuentes()
    assert (
        list(f) == sorted(f)
        and f["pricos"] == "sin_periodo"
        and f["padron_ruc"] == "mensual"
    )


def test_copias_commiteadas_al_dia():
    raiz = Path(__file__).resolve().parent.parent
    for rel in (
        "supabase/functions/_shared/fuentes_permitidas.json",
        "web/src/fuentes_permitidas.json",
    ):
        assert json.loads((raiz / rel).read_text()) == gen.fuentes(), rel


def _soportadas():
    raiz = Path(__file__).resolve().parent.parent
    return json.loads(
        (raiz / "supabase/functions/_shared/fuentes_permitidas.json").read_text()
    )


def test_fuentes_permitidas_tienen_extensiones_y_clave_raw():
    from src.ingestion import intake
    from src.ingestion import process_and_upload as pu

    for fuente, tipo in _soportadas().items():
        assert fuente in intake.ACEPTA, fuente
        assert pu.raw_key(fuente, "2025-01" if tipo == "mensual" else "-"), fuente


def test_fuentes_permitidas_tienen_columnas_esperadas():
    pytest.importorskip("pyspark")
    pytest.importorskip("awswrangler")
    from src.ingestion import intake

    for fuente in _soportadas():
        intake.columnas_esperadas(fuente)
