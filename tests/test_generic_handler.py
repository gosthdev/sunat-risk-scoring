"""Handler genérico (caso B): conversión streaming a _pendientes/. Sin red ni AWS reales."""

import json
import shutil
import subprocess
import zipfile
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingestion import process_and_upload as pu


class FakeAws:
    """Doble de `aws`: `cp --recursive` copia a un directorio local; registra todo."""

    def __init__(self, root: Path):
        self.root = root
        self.calls: list[list[str]] = []

    def __call__(self, args, check=True):
        self.calls.append(list(args))
        if args[:2] == ["s3", "cp"] and "--recursive" in args:
            dst = self.root / args[3].removeprefix("s3://")
            shutil.copytree(args[2], dst, dirs_exist_ok=True)
        if args[:2] == ["s3", "cp"] and args[3] == "-":
            return subprocess.CompletedProcess(args, 1, "", "404 Not Found")
        return subprocess.CompletedProcess(args, 0, "", "")

    def pend(self, tabla: str, sub: str = "") -> Path:
        return self.root / pu.BRONZE_BUCKET / "_pendientes" / tabla / sub


@pytest.fixture
def aws(tmp_path, monkeypatch):
    fake = FakeAws(tmp_path / "s3")
    monkeypatch.setattr(pu, "_aws", fake)
    monkeypatch.setattr(pu, "BASE_DIR", tmp_path / "data")
    monkeypatch.setattr(pu, "FUENTES_FILE", tmp_path / "fuentes.json")
    (tmp_path / "fuentes.json").write_text(json.dumps({"fuentes": {"padron_ruc": {}}}))
    (tmp_path / "data").mkdir()
    return fake


def serve(monkeypatch, files: dict[str, Path | bytes]):
    """download_file simulado: url -> contenido."""

    def dl(url, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        c = files[url]
        dest.write_bytes(c if isinstance(c, bytes) else c.read_bytes())

    monkeypatch.setattr(pu, "download_file", dl)
    monkeypatch.setattr(
        pu, "head_validators", lambda u: {"etag": None, "last_modified": None}
    )


def ingest(fuente, periodo, url, manifest=None, registered=frozenset()):
    m = {} if manifest is None else manifest
    conv: set[str] = set()
    pu.process_item(fuente, periodo, url, m, False, set(registered), conv)
    return m, conv


def make_xlsx(path: Path, sheets: dict[str, list[list]]):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(r)
    wb.save(path)


def read(dir_: Path) -> pa.Table:
    return pa.concat_tables(
        [pq.read_table(p) for p in sorted(dir_.glob("part-*.parquet"))]
    )


def test_csv_120k_rows_three_blocks_all_string(aws, tmp_path, monkeypatch):
    src = tmp_path / "big.csv"
    with open(src, "w") as f:
        f.write("RUC,Razón Social,Monto\n")
        f.writelines(f"{i},Emp {i},{i * 2}\n" for i in range(120_000))
    serve(monkeypatch, {"http://x/big.csv": src})
    m, conv = ingest("licitaciones", "2026-04", "http://x/big.csv")
    d = aws.pend("licitaciones", "periodo=2026-04")
    pf = pq.ParquetFile(next(d.glob("part-*.parquet")))
    assert pf.metadata.num_row_groups == 3 and pf.metadata.num_rows == 120_000
    t = read(d)
    assert set(t.schema.types) == {pa.string()}
    assert t.column_names == [
        "ruc",
        "razon_social",
        "monto",
        "_archivo_origen",
        "_periodo",
    ]
    assert t["_periodo"][0].as_py() == "2026-04" and t["ruc"][5].as_py() == "5"
    assert (
        conv == {"licitaciones"} and m["licitaciones"]["2026-04"]["conversion"] == "ok"
    )
    # el archivo también aterriza en raw con layout Hive
    assert (tmp_path / "data/licitaciones/anio=2026/mes=04/big.csv").exists()


def test_csv_latin1_and_semicolon(aws, tmp_path, monkeypatch):
    src = tmp_path / "l.csv"
    src.write_bytes("Año;Descripción\n2025;camión\n".encode("latin-1"))
    serve(monkeypatch, {"http://x/l.csv": src})
    ingest("fuente_nueva", "-", "http://x/l.csv")
    t = read(aws.pend("fuente_nueva"))  # periodo "-" -> sin carpeta periodo=
    assert t["ano"][0].as_py() == "2025" and t["descripcion"][0].as_py() == "camión"


def test_xlsx_two_sheets_two_folders_with_own_schema(aws, tmp_path, monkeypatch):
    src = tmp_path / "m.xlsx"
    make_xlsx(
        src,
        {
            "Hoja Uno": [["A", "B"], [1, 2]],
            "Datos 2": [[None], ["X", "Y", "Z"], ["p", "q", "r"]],
        },
    )
    serve(monkeypatch, {"http://x/m.xlsx": src})
    _, conv = ingest("multi", "-", "http://x/m.xlsx")
    assert conv == {"multi__hoja_uno", "multi__datos_2"}
    t1, t2 = read(aws.pend("multi__hoja_uno")), read(aws.pend("multi__datos_2"))
    assert t1.column_names[:2] == ["a", "b"] and t2.column_names[:3] == ["x", "y", "z"]
    assert "_hoja" not in t1.column_names  # ya va en el nombre de la tabla
    assert t1["a"][0].as_py() == "1"


def test_xlsx_single_sheet_uses_fuente_folder(aws, tmp_path, monkeypatch):
    src = tmp_path / "s.xlsx"
    make_xlsx(src, {"Unica": [["A"], [1]]})
    serve(monkeypatch, {"http://x/s.xlsx": src})
    _, conv = ingest("solo", "-", "http://x/s.xlsx")
    assert conv == {"solo"}
    assert read(aws.pend("solo"))["_hoja"][0].as_py() == "Unica"


def test_reconversion_removes_prefix_before_upload(aws, tmp_path, monkeypatch):
    src = tmp_path / "a.csv"
    src.write_text("a\n1\n")
    serve(monkeypatch, {"http://x/a.csv": src})
    m, _ = ingest("rec", "2026-04", "http://x/a.csv")
    src.write_text("a\n1\n2\n")
    aws.calls.clear()
    pu.process_item("rec", "2026-04", "http://x/a.csv", m, True, set(), set())
    kinds = [(c[1], c[2]) for c in aws.calls if c[1] in ("rm", "cp")]
    pref = f"s3://{pu.BRONZE_BUCKET}/_pendientes/rec/periodo=2026-04/"
    assert kinds[0] == ("rm", pref) and kinds[1][0] == "cp"
    assert "--recursive" in aws.calls[0]
    assert read(aws.pend("rec", "periodo=2026-04")).num_rows == 2


def test_unreadable_file_marks_error_without_raising(aws, tmp_path, monkeypatch):
    serve(monkeypatch, {"http://x/bad.xlsx": b"no es un xlsx"})
    m, conv = ingest("roto", "-", "http://x/bad.xlsx")
    assert m["roto"]["-"]["conversion"] == "error" and conv == set()


def test_zip_is_extracted_and_registered_source_not_converted(
    aws, tmp_path, monkeypatch
):
    z = tmp_path / "z.zip"
    with zipfile.ZipFile(z, "w") as f:
        f.writestr("d.csv", "a\n1\n")
    serve(monkeypatch, {"http://x/z.zip": z})
    m, conv = ingest("padron_nuevo", "-", "http://x/z.zip", registered={"padron_nuevo"})
    assert conv == set() and "conversion" not in m["padron_nuevo"]["-"]
    assert (tmp_path / "data/padron_nuevo/d.csv").exists()
    assert not (tmp_path / "data/padron_nuevo/z.zip").exists()


def test_last_run_pendientes_lists_only_this_run(aws, tmp_path, monkeypatch):
    src = tmp_path / "a.csv"
    src.write_text("a\n1\n")
    serve(monkeypatch, {"http://x/a.csv": src})
    urls = tmp_path / "urls.json"
    urls.write_text(json.dumps({"nueva": {"2026-04": "http://x/a.csv"}}))
    monkeypatch.setattr(pu, "URLS_FILE", urls)
    uploaded: dict[str, str] = {}
    orig = aws.__call__

    def spy(args, check=True):
        if args[:2] == ["s3", "cp"] and args[3].endswith("last_run_pendientes.json"):
            uploaded["v"] = Path(args[2]).read_text()
        if args[:2] == ["s3", "cp"] and args[3].endswith(pu.MANIFEST_KEY):
            uploaded["manifest"] = Path(args[2]).read_text()
        return orig(args, check)

    monkeypatch.setattr(pu, "_aws", spy)
    pu.main()
    assert json.loads(uploaded["v"]) == ["nueva"]
    # segunda corrida: sin validadores -> inmutable -> nada convertido -> []
    monkeypatch.setattr(pu, "load_manifest", lambda: json.loads(uploaded["manifest"]))
    pu.main()
    assert json.loads(uploaded["v"]) == []


def test_error_entry_is_retried_even_without_validators(aws, tmp_path, monkeypatch):
    serve(monkeypatch, {"http://x/b.xlsx": b"roto"})
    m, _ = ingest("rt", "-", "http://x/b.xlsx")
    assert m["rt"]["-"]["conversion"] == "error"
    src = tmp_path / "b.xlsx"
    make_xlsx(src, {"H": [["A"], [1]]})
    serve(monkeypatch, {"http://x/b.xlsx": src})
    m, conv = ingest("rt", "-", "http://x/b.xlsx", manifest=m)
    assert m["rt"]["-"]["conversion"] == "ok" and conv == {"rt"}


def test_all_empty_sheets_converts_nothing_without_error(aws, tmp_path, monkeypatch):
    src = tmp_path / "e.xlsx"
    make_xlsx(src, {"A": [], "B": []})
    serve(monkeypatch, {"http://x/e.xlsx": src})
    m, conv = ingest("vacio", "-", "http://x/e.xlsx")
    assert conv == set() and m["vacio"]["-"]["conversion"] == "ok"


def test_orphan_tables_warn_and_are_not_deleted(aws, tmp_path, monkeypatch, capsys):
    src = tmp_path / "m.xlsx"
    make_xlsx(src, {"Uno": [["A"], [1]], "Dos": [["B"], [2]]})
    serve(monkeypatch, {"http://x/m.xlsx": src})
    m, _ = ingest("orf", "-", "http://x/m.xlsx")
    make_xlsx(src, {"Uno": [["A"], [1]]})  # pasa a 1 hoja
    aws.calls.clear()
    pu.process_item("orf", "-", "http://x/m.xlsx", m, True, set(), set())
    out = capsys.readouterr().out
    assert "_pendientes/orf__uno/ quedó huérfano" in out
    assert "_pendientes/orf__dos/ quedó huérfano" in out
    assert not any("orf__dos" in c[2] for c in aws.calls if c[1] == "rm")
