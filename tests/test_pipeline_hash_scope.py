"""Ningun --files de pipeline.yml (hash global de check_stage) incluye el registro ni fuentes/."""

import glob
import os
import re
import shlex
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PIPELINE = (REPO / ".github/workflows/pipeline.yml").read_text(encoding="utf-8")


def _resueltos(token):
    # Misma expansion que compute_files_hash: glob no recursivo; un directorio se recorre entero.
    items = glob.glob(token, root_dir=REPO) if re.search(r"[*?\[]", token) else [token]
    for it in items:
        p = REPO / it
        if p.is_dir():
            for root, _, names in os.walk(p):
                yield from (Path(root) / n for n in names)
        else:
            yield p


def _tokens_files():
    for linea in PIPELINE.splitlines():
        m = re.search(r"--files\s+(.*?)(?:\s*\\)?$", linea)
        if m:
            yield from (t for t in shlex.split(m.group(1)) if not t.startswith("--"))


def test_files_no_incluyen_registro_ni_fuentes():
    tokens = list(_tokens_files())
    assert tokens
    for t in tokens:
        for f in _resueltos(t):
            rel = f.relative_to(REPO).as_posix()
            assert rel != "config/fuentes.json" and not rel.startswith(
                "src/spark/common/fuentes/"
            ), (t, rel)


def test_glob_common_no_entra_a_subcarpetas():
    assert all(
        f.parent == REPO / "src/spark/common"
        for f in _resueltos("src/spark/common/*.py")
    )


def test_job_raw_pasa_force_redownload_y_bronze_bucket():
    raw = PIPELINE.split("\n  small:")[0]
    assert "TF_VAR_force_redownload: ${{ inputs.force == 'raw' }}" in raw
    assert "TF_VAR_bronze_bucket_name: ${{ env.BRONZE_BUCKET }}" in raw
    assert "FORCE_REDOWNLOAD" not in PIPELINE.split("\n  small:")[1]
