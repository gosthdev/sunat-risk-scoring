"""Planner incremental: registros y archivos temporales, sin S3 ni el registro real."""

import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import pending_periods as pp

BASE = {
    "compartidos": {
        "small": ["c/small.py"],
        "bronze": ["c/b.py"],
        "silver": ["c/s.py"],
    },
    "fuentes": {
        "alfa": {
            "capas": {
                "bronze": {
                    "codigo": ["f/b_alfa.py"],
                    "salidas": [{"ruta": "X", "publicacion": "particiones"}],
                },
                "silver": {
                    "codigo": ["f/s_alfa.py"],
                    "salidas": [{"ruta": "Y", "publicacion": "particiones"}],
                },
            }
        },
        "beta": {
            "capas": {
                "bronze": {
                    "codigo": ["f/b_beta.py"],
                    "salidas": [{"ruta": "X2", "publicacion": "particiones"}],
                },
                "silver": {
                    "codigo": ["f/s_beta.py"],
                    "salidas": [{"ruta": "Y2", "publicacion": "completa"}],
                },
            }
        },
        "peq": {
            "capas": {
                "small": {
                    "codigo": ["f/small_peq.py"],
                    "salidas": [{"ruta": "Z", "publicacion": "completa"}],
                }
            }
        },
    },
}
RAW = {
    "alfa": {
        "2025-01": {"sha256": "a1"},
        "2025-02": {"sha256": "a2"},
        "2025-03": {"sha256": "a3"},
    },
    "beta": {"2025-01": {"sha256": "b1"}, "2025-02": {"sha256": "b2"}},
    "peq": {"-": {"sha256": "p"}},
}


@pytest.fixture
def repo(tmp_path):
    for r in [
        "c/small.py",
        "c/b.py",
        "c/s.py",
        "f/b_alfa.py",
        "f/s_alfa.py",
        "f/b_beta.py",
        "f/s_beta.py",
        "f/small_peq.py",
    ]:
        (tmp_path / r).parent.mkdir(exist_ok=True)
        (tmp_path / r).write_text(r)
    return tmp_path


def run(
    repo, stage, upstream, own=None, registro=BASE, force=False, pend="", capsys=None
):
    (repo / "reg.json").write_text(json.dumps(registro))
    (repo / "up.json").write_text(json.dumps(upstream))
    argv = [
        "plan",
        "--stage",
        stage,
        "--registro",
        str(repo / "reg.json"),
        "--repo",
        str(repo),
        "--upstream",
        str(repo / "up.json"),
        "--own",
        str(repo / "own.json"),
        "--expected",
        str(repo / "exp.json"),
        "--pendientes-existentes",
        pend,
    ]
    if own is not None:
        (repo / "own.json").write_text(json.dumps(own))
    if force:
        argv.append("--force")
    pp.main(argv)
    out = capsys.readouterr()
    return out.out.strip(), json.loads((repo / "exp.json").read_text()), out.err


def commit(repo, stage, published):
    pp.main(
        [
            "commit",
            "--stage",
            stage,
            "--expected",
            str(repo / "exp.json"),
            "--own",
            str(repo / "own.json"),
            "--published",
            published,
            "--out",
            str(repo / "own.json"),
        ]
    )
    return json.loads((repo / "own.json").read_text())


def sellar(repo, stage, capsys, upstream=RAW, registro=BASE):
    out, _, _ = run(repo, stage, upstream, registro=registro, capsys=capsys)
    return commit(repo, stage, out)


def test_own_ausente_es_full_y_commit_sella(repo, capsys):
    out, exp, _ = run(repo, "bronze", RAW, capsys=capsys)
    assert out == "alfa=FULL beta=FULL"
    assert set(exp) == {"alfa", "beta"}  # peq no tiene capa bronze
    assert set(commit(repo, "bronze", out)["alfa"]) == {"2025-01", "2025-02", "2025-03"}


def test_sha_igual_skip_nuevo_y_cambiado(repo, capsys):
    own = sellar(repo, "bronze", capsys)
    assert run(repo, "bronze", RAW, own, capsys=capsys)[0] == ""
    nuevo = copy.deepcopy(RAW)
    nuevo["alfa"]["2025-04"] = {"sha256": "a4"}
    assert run(repo, "bronze", nuevo, own, capsys=capsys)[0] == "alfa=2025-04"
    cambiado = copy.deepcopy(RAW)
    cambiado["alfa"]["2025-02"] = {"sha256": "OTRO"}
    assert run(repo, "bronze", cambiado, own, capsys=capsys)[0] == "alfa=2025-02"


def test_periodo_removido_se_ignora_y_se_conserva(repo, capsys):
    own = sellar(repo, "bronze", capsys)
    menos = copy.deepcopy(RAW)
    del menos["alfa"]["2025-03"]
    assert run(repo, "bronze", menos, own, capsys=capsys)[0] == ""
    assert "2025-03" in commit(repo, "bronze", "")["alfa"]


def test_sin_job_no_entra_al_processed(repo, capsys):
    up = {**RAW, "licitaciones": {"2025-01": {"sha256": "l"}}}
    out, exp, err = run(
        repo, "bronze", up, sellar(repo, "bronze", capsys), capsys=capsys
    )
    assert (
        out == ""
        and "licitaciones" not in exp
        and "sin_job" in err
        and "licitaciones" in err
    )


def test_fuente_nueva_registrada_solo_ella_y_no_cambia_huellas(repo, capsys):
    own = sellar(repo, "bronze", capsys)
    antes = json.loads((repo / "exp.json").read_text())
    reg = copy.deepcopy(BASE)
    reg["fuentes"]["gama"] = {
        "capas": {"bronze": {"codigo": ["f/b_alfa.py"], "salidas": []}}
    }
    up = {**RAW, "gama": {"2025-01": {"sha256": "g"}, "2025-02": {"sha256": "g2"}}}
    out, exp, _ = run(repo, "bronze", up, own, registro=reg, capsys=capsys)
    assert out == "gama=FULL"
    assert exp["alfa"] == antes["alfa"] and exp["beta"] == antes["beta"]


def test_cambia_codigo_de_una_fuente_en_bronze_y_cascada_a_silver(repo, capsys):
    b_own = sellar(repo, "bronze", capsys)
    s_own = sellar(repo, "silver", capsys, upstream=b_own)
    (repo / "f/b_alfa.py").write_text("cambio")
    assert run(repo, "bronze", RAW, b_own, capsys=capsys)[0] == "alfa=FULL"
    b_own = commit(repo, "bronze", "alfa=FULL")
    assert run(repo, "silver", b_own, s_own, capsys=capsys)[0] == "alfa=FULL"


def test_cambia_silver_de_fuente_no_toca_bronze(repo, capsys):
    b_own = sellar(repo, "bronze", capsys)
    s_own = sellar(repo, "silver", capsys, upstream=b_own)
    (repo / "f/s_alfa.py").write_text("cambio")
    assert run(repo, "bronze", RAW, b_own, capsys=capsys)[0] == ""
    assert run(repo, "silver", b_own, s_own, capsys=capsys)[0] == "alfa=FULL"


def test_cambia_compartido_silver_todas_full_y_bronze_intacto(repo, capsys):
    b_own = sellar(repo, "bronze", capsys)
    s_own = sellar(repo, "silver", capsys, upstream=b_own)
    (repo / "c/s.py").write_text("cambio")
    assert run(repo, "silver", b_own, s_own, capsys=capsys)[0] == "alfa=FULL beta=FULL"
    assert run(repo, "bronze", RAW, b_own, capsys=capsys)[0] == ""


def test_commit_parcial_sella_solo_lo_publicado(repo, capsys):
    own = sellar(repo, "bronze", capsys)
    nuevo = copy.deepcopy(RAW)
    nuevo["alfa"]["2025-04"] = {"sha256": "a4"}
    nuevo["alfa"]["2025-05"] = {"sha256": "a5"}
    assert run(repo, "bronze", nuevo, own, capsys=capsys)[0] == "alfa=2025-04,2025-05"
    sellado = commit(repo, "bronze", "alfa=2025-04")
    assert "2025-04" in sellado["alfa"] and "2025-05" not in sellado["alfa"]
    assert run(repo, "bronze", nuevo, sellado, capsys=capsys)[0] == "alfa=2025-05"


def test_completa_expande_a_full_con_todos_los_periodos(repo, capsys):
    b_own = sellar(repo, "bronze", capsys)
    s_own = sellar(repo, "silver", capsys, upstream=b_own)
    b_own["beta"]["2025-03"] = "nuevo"
    out, exp, _ = run(repo, "silver", b_own, s_own, capsys=capsys)
    assert out == "beta=FULL" and set(exp["beta"]) == {"2025-01", "2025-02", "2025-03"}
    assert set(commit(repo, "silver", out)["beta"]) == {"2025-01", "2025-02", "2025-03"}


def test_force_con_huellas_iguales_todas_full(repo, capsys):
    own = sellar(repo, "bronze", capsys)
    out, exp, _ = run(repo, "bronze", RAW, own, force=True, capsys=capsys)
    assert out == "alfa=FULL beta=FULL" and len(exp["alfa"]) == 3


def test_upstream_objeto_y_string_misma_huella(repo, capsys):
    _, e1, _ = run(
        repo,
        "bronze",
        {"alfa": {"2025-01": {"sha256": "x", "etag": "e"}}},
        capsys=capsys,
    )
    _, e2, _ = run(repo, "bronze", {"alfa": {"2025-01": "x"}}, capsys=capsys)
    assert e1 == e2


def test_small_periodo_guion_es_full(repo, capsys):
    out, exp, _ = run(repo, "small", RAW, capsys=capsys)
    assert out == "peq=FULL" and set(exp["peq"]) == {"-"}


def test_archivo_de_codigo_inexistente_es_error(repo, capsys):
    (repo / "f/b_beta.py").unlink()
    with pytest.raises(SystemExit, match="f/b_beta.py"):
        run(repo, "bronze", RAW, capsys=capsys)


def test_pendientes_existentes_de_fuente_registrada_avisa(repo, capsys):
    _, _, err = run(repo, "bronze", RAW, pend="alfa__hoja1/ otra", capsys=capsys)
    assert "alfa__hoja1" in err and "otra" not in err.split("WARN")[-1]


@pytest.mark.parametrize(
    "txt", ["alfa", "alfa=", "alfa=2025-13", "alfa=full", "a=FULL a=FULL"]
)
def test_published_mal_formado_falla(repo, txt):
    (repo / "exp.json").write_text("{}")
    with pytest.raises(SystemExit):
        commit(repo, "bronze", txt)
