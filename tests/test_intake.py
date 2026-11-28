"""Recepción de cargas del portal (_inbox/). Sin red ni AWS reales."""

import io
import json
import subprocess
import sys
import zipfile
from types import SimpleNamespace

import openpyxl
import pytest

from src.ingestion import intake
from src.ingestion import process_and_upload as pu


class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeClient:
    """Doble del cliente boto3 de S3 (un solo bucket)."""

    def __init__(self):
        self.objs: dict[str, bytes] = {}

    def poner(self, key, body):
        self.objs[key] = body

    def put_object(self, Bucket, Key, Body, **_):
        self.poner(Key, Body)

    def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None):
        return {
            "Contents": [{"Key": k} for k in sorted(self.objs) if k.startswith(Prefix)]
        }

    def head_object(self, Bucket, Key):
        if Key not in self.objs:
            raise ClientError("404")
        return {"ContentLength": len(self.objs[Key])}

    def upload_file(self, Filename, Bucket, Key):
        with open(Filename, "rb") as f:
            self.poner(Key, f.read())

    def download_file(self, Bucket, Key, Filename):
        with open(Filename, "wb") as f:
            f.write(self.objs[Key])

    def copy(self, CopySource, Bucket, Key):
        self.poner(Key, self.objs[CopySource["Key"]])

    def delete_object(self, Bucket, Key):
        self.objs.pop(Key, None)


C1 = "11111111-1111-4111-8111-111111111111"
C2 = "22222222-2222-4222-8222-222222222222"
C3 = "33333333-3333-4333-8333-333333333333"

URLS = {"padron_ruc": {"2025-01": "http://x/p1.zip"}}
FUENTES = ["padron_ruc", "ordenes_compra", "pricos", "ingresos_tributarios", "epen"]
FUENTES += ["ssco", "see"]
# Lo que el lector de raw de cada fuente exige (en producción sale de su definición).
ESPERADAS = {
    "padron_ruc": ["RUC", "Estado"],
    "ordenes_compra": ["A", "B"],
    "epen": ["A"],
    "pricos": ["RUC", "Nombre o Razón Social"],
    "see": ["RUC", "tipo_SEE"],
    "ssco": ["RUC", "fecha de emisión"],
}
SEE_OK = b"RUC,tipo_SEE\n1,a\n"
PADRON_OK = b"RUC|Estado\n1|A\n"
PADRON_OK_CSV = b"RUC,Estado\r\n1,A\r\n"
CLAVE_SEE = "see/padron_see.csv"
CLAVE_PADRON = "padron_ruc/anio=2025/mes=01/padron.csv"


def xlsx_bytes(extra: dict[str, bytes] | None = None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", "<x/>")
        for n, c in (extra or {}).items():
            z.writestr(n, c)
    return buf.getvalue()


@pytest.fixture
def env(tmp_path, monkeypatch):
    cli = FakeClient()
    monkeypatch.setattr(intake, "_s3", lambda: cli)
    monkeypatch.setattr(pu, "BASE_DIR", tmp_path)

    def aws(
        args, check=True
    ):  # el manifest va por `aws s3 cp`; se refleja en el mismo bucket
        if args[:2] == ["s3", "cp"] and args[3] == "-":
            if pu.MANIFEST_KEY not in cli.objs:
                return subprocess.CompletedProcess(args, 1, "", "404 Not Found")
            return subprocess.CompletedProcess(
                args, 0, cli.objs[pu.MANIFEST_KEY].decode(), ""
            )
        with open(args[2], "rb") as f:
            cli.poner(pu.MANIFEST_KEY, f.read())
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(pu, "_aws", aws)
    monkeypatch.setattr(pu, "bootstrap_manifest", lambda items: {})
    urls = tmp_path / "urls.json"
    urls.write_text(json.dumps(URLS))
    fuentes = tmp_path / "fuentes.json"
    fuentes.write_text(json.dumps({"fuentes": {f: {} for f in FUENTES}}))
    monkeypatch.setattr(intake, "URLS_FILE", urls)
    monkeypatch.setattr(intake, "FUENTES_FILE", fuentes)
    monkeypatch.setattr(intake, "columnas_esperadas", lambda f: ESPERADAS.get(f, []))
    return cli, tmp_path / "cargas_lote.json"


def subir(cli, fuente, periodo, archivo, data, carga=C1):
    k = f"_inbox/{fuente}/{periodo}/{carga}/{archivo}"
    cli.poner(k, data)
    return k


def manifest(cli):
    return json.loads(cli.objs[pu.MANIFEST_KEY]) if pu.MANIFEST_KEY in cli.objs else {}


def rechazada(env, fuente, periodo, archivo, data):
    cli, out = env
    k = subir(cli, fuente, periodo, archivo, data)
    (r,) = intake.main(out)
    assert r["resultado"] == "rechazada" and r["motivo"]
    assert k not in cli.objs and not manifest(cli)
    dest = "_inbox/_rechazados/" + k.removeprefix("_inbox/")
    assert dest in cli.objs and cli.objs[dest + ".txt"].decode() == r["motivo"]
    assert json.loads(out.read_text()) == [r]
    return r


def xlsx_tabla(filas):
    buf = io.BytesIO()
    wb = openpyxl.Workbook()
    for f in filas:
        wb.active.append(f)
    wb.save(buf)
    return buf.getvalue()


def ordenes_grande():
    filas = [["A", "B"]] + [[i, i * 2] for i in range(12_000)]
    esperado = "A,B\n" + "".join(f"{i},{i * 2}\n" for i in range(12_000))
    return xlsx_tabla(filas), esperado.encode()


# --- fuente reconocida y contenido ------------------------------------------------


def test_unknown_source_rejected(env):
    r = rechazada(env, "nueva", "-", "d.csv", b"a\n1\n")
    assert r["motivo"] == "Fuente no reconocida: nueva"


def test_valid_csv_goes_where_the_ec2_puts_it(env):
    cli, out = env
    k = subir(cli, "see", "-", "mi_archivo.csv", SEE_OK)
    (r,) = intake.main(out)
    assert r == {"carga_id": C1, "fuente": "see", "periodo": "-", "resultado": "ok"}
    assert cli.objs[CLAVE_SEE] == SEE_OK and k not in cli.objs
    e = manifest(cli)["see"]["-"]
    assert e["url"] == "manual" and "raw_keys" not in e and "conversion" not in e
    assert e["sha256"] == pu.hashlib.sha256(SEE_OK).hexdigest()
    assert json.loads(out.read_text()) == [r]


@pytest.mark.parametrize(
    "fuente,periodo,archivo,data",
    [
        ("padron_ruc", "2025-01", "x.csv", b"RUC|Otra\n1|A\n"),
        (
            "ordenes_compra",
            "2025-03",
            "x.xlsx",
            xlsx_tabla([["A", "Z"]] + [[1000000, 2]] * 12_000),
        ),
        ("see", "-", "x.csv", b"RUC,otra\n1,a\n"),
        ("epen", "-", "x.csv", b"Z\n1\n"),
        ("pricos", "-", "x.xlsx", xlsx_tabla([["RUC", "otra"], [1, 2]])),
        ("ssco", "-", "x.xlsx", xlsx_tabla([["RUC", "otra"], [1, 2]])),
        ("ssco", "-", "y.xlsx", xlsx_tabla([["otra", "Fecha de emisión"], [1, 2]])),
    ],
)
def test_wrong_header_rejected_per_source(env, fuente, periodo, archivo, data):
    r = rechazada(env, fuente, periodo, archivo, data)
    assert r["motivo"].startswith(
        f"El archivo no tiene las columnas esperadas para {fuente}: "
    )


def test_missing_columns_are_listed(env):
    r = rechazada(env, "see", "-", "x.csv", b"otra\n1\n")
    assert r["motivo"].endswith("faltan RUC, tipo_SEE")


def test_valid_ssco_xlsx_copied_unchanged(env):
    cli, out = env
    data = xlsx_tabla([["RUC", "Fecha de emisión de la resolución"], [1, 2]])
    subir(cli, "ssco", "-", "x.xlsx", data)
    assert intake.main(out)[0]["resultado"] == "ok"
    assert cli.objs["ssco/sujesincapacidadOperativa.xlsx"] == data


def test_source_without_rule_raises():
    with pytest.raises(ValueError):
        intake.columnas_esperadas("nueva")


@pytest.fixture
def reales():
    pytest.importorskip("pyspark")
    pytest.importorskip("awswrangler")


def test_real_definitions_cover_every_registered_source(reales):
    fuentes = json.loads(intake.FUENTES_FILE.read_text())["fuentes"]
    for f in fuentes:
        assert bool(intake.columnas_esperadas(f)) == (f != "ingresos_tributarios")
    assert "RUC" in intake.columnas_esperadas("padron_ruc")


def _escribir(tmp_path, cols, ext):
    p = tmp_path / f"h.{ext}"
    if ext == "csv":
        p.write_text(",".join(cols) + "\n" + ",".join("1" for _ in cols) + "\n")
    else:
        p.write_bytes(xlsx_tabla([cols, [1] * len(cols)]))
    return p


@pytest.mark.parametrize("fuente", ["padron_ruc", "ordenes_compra"])
def test_real_header_exact_order_case_insensitive(reales, tmp_path, fuente):
    esp = intake.columnas_esperadas(fuente)
    ok = lambda cols: intake.validar_columnas(_escribir(tmp_path, cols, "csv"), fuente)
    ok(esp)
    ok([c.lower() for c in esp])
    for malo in (esp + ["extra"], esp[::-1], esp[:-1]):
        with pytest.raises(intake.Rechazo, match="columnas esperadas"):
            ok(malo)


@pytest.mark.parametrize(
    "fuente,ext", [("see", "csv"), ("epen", "csv"), ("pricos", "xlsx")]
)
def test_real_header_small_sources_accept_extra_and_reordered(
    reales, tmp_path, fuente, ext
):
    esp = intake.columnas_esperadas(fuente)
    ok = lambda cols: intake.validar_columnas(_escribir(tmp_path, cols, ext), fuente)
    ok(esp)
    ok(esp[::-1] + ["extra"])
    with pytest.raises(intake.Rechazo, match="faltan"):
        ok(esp[1:])


def test_real_header_ssco_needs_ruc_and_fecha_emision(reales, tmp_path):
    ok = lambda cols: intake.validar_columnas(_escribir(tmp_path, cols, "xlsx"), "ssco")
    ok(["RUC", "Fecha de emisión de la resolución"])
    for malo in (["N", "Fecha de emisión de la resolución"], ["RUC", "otra"]):
        with pytest.raises(intake.Rechazo, match="faltan"):
            ok(malo)


def test_padron_uses_handler_layout(env):
    cli, out = env
    subir(cli, "padron_ruc", "2025-01", "x.csv", PADRON_OK)
    intake.main(out)
    assert cli.objs[CLAVE_PADRON] == PADRON_OK_CSV
    assert "conversion" not in manifest(cli)["padron_ruc"]["2025-01"]


def test_padron_without_month_rejected(env):
    rechazada(env, "padron_ruc", "-", "x.csv", PADRON_OK)


def test_duplicate_is_deleted_and_leaves_raw_untouched(env):
    cli, out = env
    subir(cli, "see", "-", "d.csv", SEE_OK)
    intake.main(out)
    antes = manifest(cli)
    k = subir(cli, "see", "-", "d.csv", SEE_OK, carga=C2)
    (r,) = intake.main(out)
    assert r["resultado"] == "duplicada" and k not in cli.objs
    assert manifest(cli) == antes


def test_replacement_updates_sha_and_raw(env):
    cli, out = env
    subir(cli, "see", "-", "d.csv", SEE_OK)
    intake.main(out)
    nuevo = SEE_OK + b"2,b\n"
    subir(cli, "see", "-", "d.csv", nuevo, carga=C2)
    (r,) = intake.main(out)
    assert r["resultado"] == "ok" and cli.objs[CLAVE_SEE] == nuevo
    assert manifest(cli)["see"]["-"]["sha256"] == pu.hashlib.sha256(nuevo).hexdigest()


def test_renamed_binary_as_csv_rejected(env):
    rechazada(env, "see", "-", "malo.csv", b"\x89PNG\r\n\x00\x00binario")


def test_path_traversal_name_rejected(env):
    rechazada(env, "see", "-", "..evil.csv", SEE_OK)


def test_corrupt_xlsx_rejected(env):
    rechazada(env, "pricos", "-", "d.xlsx", b"PK\x03\x04" + b"basura" * 50)
    rechazada(env, "pricos", "-", "e.xlsx", b"esto no es excel")


def test_zip_bomb_rejected(env):
    bomba = xlsx_bytes({"big.bin": b"\0" * 20_000_000})  # ~20 MB -> ratio >> 100
    rechazada(env, "pricos", "-", "d.xlsx", bomba)


def test_other_extension_and_bad_period_rejected(env):
    rechazada(env, "see", "-", "d.exe", b"MZ")
    rechazada(env, "padron_ruc", "2026-13", "d.csv", PADRON_OK)


def test_rechazados_are_skipped_and_wrong_depth_is_moved_with_reason(env):
    cli, out = env
    cli.poner("_inbox/_rechazados/x/-/c1/d.csv", b"a\n1\n")
    cli.poner("_inbox/suelto.csv", b"a\n1\n")
    cli.poner("_inbox/see/-/no-es-uuid/d.csv", b"a\n1\n")
    assert intake.main(out) == []  # sin carga_id no hay fila de resultado
    assert "_inbox/suelto.csv" not in cli.objs and not manifest(cli)
    assert cli.objs["_inbox/_rechazados/suelto.csv.txt"].decode() == intake.MOTIVO_RUTA
    assert "_inbox/_rechazados/x/-/c1/d.csv" in cli.objs
    assert "_inbox/_rechazados/see/-/no-es-uuid/d.csv.txt" in cli.objs


def test_over_5gb_rejected_before_download(env, monkeypatch):
    cli, out = env
    k = subir(cli, "see", "-", "d.csv", SEE_OK)
    monkeypatch.setattr(
        cli, "head_object", lambda Bucket, Key: {"ContentLength": 6 * 1024**3}
    )
    monkeypatch.setattr(cli, "download_file", lambda *a: pytest.fail("no debe bajarse"))
    (r,) = intake.main(out)
    assert r["resultado"] == "rechazada" and "5 GB" in r["motivo"] and k not in cli.objs


def test_non_ascii_digits_in_period_rejected(env):
    rechazada(env, "padron_ruc", "٢٠٢٥-٠١", "d.csv", b"a")


def test_source_with_fixed_format_rejects_other_extension(env):
    rechazada(env, "pricos", "-", "d.csv", b"a\n1\n")


def test_unexpected_error_rejects_only_that_item_and_leaves_no_raw(env, monkeypatch):
    cli, out = env
    malo = subir(cli, "see", "-", "malo.csv", SEE_OK, carga=C1)
    bueno = subir(
        cli,
        "pricos",
        "-",
        "bueno.xlsx",
        xlsx_tabla([["RUC", "Nombre o Razón Social"], [1, "a"]]),
        carga=C2,
    )
    orig = cli.copy

    def copy(CopySource, Bucket, Key):
        if Key == CLAVE_SEE:
            raise RuntimeError("S3 se cayo")
        orig(CopySource, Bucket, Key)

    monkeypatch.setattr(cli, "copy", copy)
    res = {r["carga_id"]: r for r in intake.main(out)}
    assert (
        res[C1]["resultado"] == "rechazada"
        and res[C1]["motivo"] == intake.MOTIVO_INTERNO
    )
    assert res[C2]["resultado"] == "ok"
    assert malo not in cli.objs and bueno not in cli.objs
    assert CLAVE_SEE not in cli.objs and "see" not in manifest(cli)
    assert "pricos/principalesContrib-PRICOS.xlsx" in cli.objs


def test_failed_item_cleans_the_copies_it_already_made(env, monkeypatch):
    cli, out = env
    subir(cli, "ordenes_compra", "2025-03", "x.xlsx", ordenes_grande()[0])

    def upload(Filename, Bucket, Key):
        cli.poner(Key, b"x")  # la copia se alcanzó a escribir y luego falla
        raise RuntimeError("corte")

    monkeypatch.setattr(cli, "upload_file", upload)
    (r,) = intake.main(out)
    assert r["motivo"] == intake.MOTIVO_INTERNO
    assert "ordenes_compra/anio=2025/mes=03/ordenes.csv" not in cli.objs
    assert not manifest(cli)


# --- la conversión de la EC2 se aplica antes de escribir raw -------------------------


def test_ordenes_xlsx_lands_as_csv_like_the_ec2(env):
    cli, out = env
    xlsx, esperado = ordenes_grande()
    subir(cli, "ordenes_compra", "2025-03", "x.xlsx", xlsx)
    intake.main(out)
    clave = "ordenes_compra/anio=2025/mes=03/ordenes.csv"
    assert cli.objs[clave] == esperado
    e = manifest(cli)["ordenes_compra"]["2025-03"]
    assert e["sha256"] == pu.hashlib.sha256(cli.objs[clave]).hexdigest()


def test_epen_xlsx_lands_as_csv(env):
    cli, out = env
    subir(cli, "epen", "-", "x.xlsx", xlsx_tabla([["A"], [7]]))
    intake.main(out)
    assert cli.objs["epen/anio=2025/epen.csv"] == b"A\n7\n"


def test_epen_csv_is_kept(env):
    cli, out = env
    subir(cli, "epen", "-", "x.csv", b"A,B\n1,2\n")
    intake.main(out)
    assert cli.objs["epen/anio=2025/epen.csv"] == b"A,B\n1,2\n"


def test_pricos_xlsx_copied_unchanged(env):
    cli, out = env
    data = xlsx_tabla([["RUC", "Nombre o Razón Social"], [1, "a"]])
    subir(cli, "pricos", "-", "otro_nombre.xlsx", data)
    intake.main(out)
    assert cli.objs["pricos/principalesContrib-PRICOS.xlsx"] == data


def _fake_transformer(monkeypatch, fn):
    monkeypatch.setitem(
        sys.modules, "ingresos_transformer", SimpleNamespace(process_both_ingresos=fn)
    )


def test_ingresos_uploads_only_the_monto_csv(env, monkeypatch):
    import pandas as pd

    cli, out = env
    _fake_transformer(
        monkeypatch,
        lambda p: (pd.DataFrame({"a": [1]}), pd.DataFrame({"b": [2]})),
    )
    subir(cli, "ingresos_tributarios", "-", "cdro_A13.xlsx", xlsx_tabla([["A"], [1]]))
    intake.main(out)
    assert cli.objs["ingresos_tributarios/cdrA13_tabular.csv"] == b"a\n1\n"
    assert "ingresos_tributarios/cdrA13_Var_tabular.csv" not in cli.objs


def test_ingresos_with_unknown_format_is_rejected(env, monkeypatch):
    cli, out = env

    def roto(p):
        raise ValueError("no calza")

    _fake_transformer(monkeypatch, roto)
    subir(cli, "ingresos_tributarios", "-", "cdro_A13.xlsx", xlsx_tabla([["A"], [1]]))
    (r,) = intake.main(out)
    assert r["resultado"] == "rechazada"
    assert "Ingresos Tributarios" in r["motivo"]
    assert "ingresos_tributarios/cdrA13_tabular.csv" not in cli.objs


def test_ordenes_below_minimum_size_rejected(env):
    rechazada(env, "ordenes_compra", "2025-03", "x.xlsx", xlsx_tabla([["A"], [1]]))


def test_same_canonical_key_twice_in_one_run_marks_the_earlier_as_replaced(env):
    cli, out = env
    subir(cli, "padron_ruc", "2025-01", "a.csv", PADRON_OK, carga=C1)
    subir(cli, "padron_ruc", "2025-01", "b.csv", b"RUC|Estado\n2|B\n", carga=C2)
    res = {r["carga_id"]: r for r in intake.main(out)}
    assert (
        res[C1]["resultado"] == "duplicada"
        and res[C1]["motivo"] == intake.MOTIVO_REEMPLAZADA
    )
    assert res[C2]["resultado"] == "ok"
    assert cli.objs[CLAVE_PADRON] == b"RUC,Estado\r\n2,B\r\n"


def test_manifest_failure_restores_overwritten_raw_and_removes_new_copies(
    env, monkeypatch
):
    cli, out = env
    cli.poner(CLAVE_PADRON, b"original")
    k = subir(cli, "padron_ruc", "2025-01", "a.csv", PADRON_OK)
    subir(cli, "see", "-", "d.csv", SEE_OK, carga=C2)

    def roto(*a, **kw):
        raise RuntimeError("manifest caido")

    monkeypatch.setattr(pu, "save_manifest", roto)
    with pytest.raises(RuntimeError):
        intake.main(out)
    assert cli.objs[CLAVE_PADRON] == b"original"
    assert CLAVE_SEE not in cli.objs
    assert k in cli.objs and not any(x.startswith("_tmp_intake/") for x in cli.objs)


def test_cleanup_runs_even_if_rechazar_fails(env, monkeypatch):
    cli, out = env
    subir(cli, "see", "-", "d.csv", SEE_OK)
    orig = cli.copy

    def copy(CopySource, Bucket, Key):
        if Key == CLAVE_SEE:
            raise RuntimeError("falla al copiar")
        orig(CopySource, Bucket, Key)

    monkeypatch.setattr(cli, "copy", copy)
    monkeypatch.setattr(
        intake, "rechazar", lambda *a: (_ for _ in ()).throw(RuntimeError("tambien"))
    )
    with pytest.raises(RuntimeError):
        intake.main(out)
    assert CLAVE_SEE not in cli.objs


def test_conv_ingresos_real_transformer_writes_csv(tmp_path):
    import pandas as pd

    wb = openpyxl.Workbook()
    wb.active.title = "cdrA13"
    wb.create_sheet("cdrA13(Var)")
    for ws in wb.worksheets:
        ws.cell(4, 3, 2025)
        ws.cell(5, 3, "Ene.")
        ws.cell(9, 1, "Lima")
        ws.cell(9, 3, 10.5)
    src = tmp_path / "in.xlsx"
    wb.save(src)
    (csv,) = pu.conv_ingresos(src, tmp_path / "out")
    assert csv.name == "cdrA13_tabular.csv"
    df = pd.read_csv(csv)
    # REQUIRED_INGRESOS_COLUMNS de fuentes/small/ingresos_tributarios.py
    assert {"Departamento", "Periodo", "Anio", "Mes", "Monto_Recaudado"} <= set(
        df.columns
    )
    assert df.loc[0, "Monto_Recaudado"] == 10.5
