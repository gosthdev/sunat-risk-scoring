"""job00 despacha solo las fuentes small que PENDING_PERIODS deja pendientes."""

import importlib

job00 = importlib.import_module("00_prepare_small_datasets")


def _espiar(monkeypatch):
    llamadas = []
    real = importlib.import_module

    def falso(nombre):
        if nombre.startswith("fuentes.small."):
            modulo = type("M", (), {})()
            modulo.preparar = lambda publicar, n=nombre: llamadas.append(
                (n.rsplit(".", 1)[1], publicar)
            )
            return modulo
        return real(nombre)

    monkeypatch.setattr(job00.importlib, "import_module", falso)
    return llamadas


def test_solo_el_modulo_see_con_see_full(monkeypatch):
    monkeypatch.setenv("PENDING_PERIODS", "see=FULL")
    llamadas = _espiar(monkeypatch)
    job00.main()
    assert [f for f, _ in llamadas] == ["see"]
    assert llamadas[0][1] is job00._publicar


def test_sin_env_corre_todas_en_el_orden_del_registro(monkeypatch):
    monkeypatch.delenv("PENDING_PERIODS", raising=False)
    llamadas = _espiar(monkeypatch)
    job00.main()
    assert [f for f, _ in llamadas] == [
        "pricos",
        "ingresos_tributarios",
        "epen",
        "ssco",
        "see",
    ]


def test_env_vacia_no_corre_nada(monkeypatch):
    monkeypatch.setenv("PENDING_PERIODS", "")
    llamadas = _espiar(monkeypatch)
    job00.main()
    assert llamadas == []
