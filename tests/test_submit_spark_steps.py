"""05_submit_spark_steps.sh: reintentos y publicación staging -> ruta viva (con `aws` simulado)."""

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "src" / "spark" / "jobs" / "05_submit_spark_steps.sh"
STUBS = ROOT / "tests" / "stubs"


def _ejecutar(tmp_path, estados, step="03_feature_gold", extra_env=None):
    log = tmp_path / "aws.log"
    log.write_text("")
    states = tmp_path / "states"
    states.write_text("\n".join(estados) + "\n")
    env = {
        **os.environ,
        "PATH": f"{STUBS}:{os.environ['PATH']}",
        "AWS_LOG": str(log),
        "AWS_STATES_FILE": str(states),
        "EMR_APPLICATION_ID": "app-1",
        "EMR_EXECUTION_ROLE_ARN": "arn:aws:iam::1:role/emr",
        "POLL_INTERVAL_SECONDS": "0",
        **(extra_env or {}),
    }
    res = subprocess.run(
        ["bash", str(SCRIPT), "--only-steps", step],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    return res, log.read_text().splitlines()


def _lanzamientos(llamadas):
    return [c for c in llamadas if c.startswith("emr-serverless start-job-run")]


def _syncs(llamadas):
    return [c for c in llamadas if c.startswith("s3 sync")]


def test_exito_publica_staging_a_la_ruta_viva(tmp_path):
    res, llamadas = _ejecutar(tmp_path, ["SUCCESS"])
    assert res.returncode == 0, res.stderr
    assert len(_lanzamientos(llamadas)) == 1
    origen = "s3://sunat-risk-scoring-gold/_staging/ruc_features/"
    destino = "s3://sunat-risk-scoring-gold/ruc_features/"
    assert _syncs(llamadas) == [
        f"s3 sync {origen} {destino} --delete --only-show-errors"
    ]


def test_fallo_persistente_no_publica_y_conserva_lo_vigente(tmp_path):
    res, llamadas = _ejecutar(tmp_path, ["FAILED"])
    assert res.returncode == 1
    assert len(_lanzamientos(llamadas)) == 1
    assert _syncs(llamadas) == []
    assert "Athena sigue sirviendo la última versión válida" in res.stderr


def test_sin_salida_obligatoria_en_staging_no_publica(tmp_path):
    res, llamadas = _ejecutar(
        tmp_path, ["SUCCESS"], extra_env={"AWS_EMPTY_STAGING": "1"}
    )
    assert res.returncode == 1
    assert _syncs(llamadas) == []


def test_silver_publica_todas_las_salidas(tmp_path):
    res, llamadas = _ejecutar(tmp_path, ["SUCCESS"], step="02_clean_silver")
    assert res.returncode == 0, res.stderr
    destinos = sorted(c.split()[3] for c in _syncs(llamadas))
    assert destinos == [
        "s3://sunat-risk-scoring-silver/ordenes_compra/",
        "s3://sunat-risk-scoring-silver/padron_ruc/",
        "s3://sunat-risk-scoring-silver/quality_report/",
        "s3://sunat-risk-scoring-silver/quarantine_ordenes_compra/",
        "s3://sunat-risk-scoring-silver/quarantine_padron_ruc/",
    ]


def test_cuarentena_vacia_limpia_la_ruta_viva_en_vez_de_publicar(tmp_path):
    res, llamadas = _ejecutar(
        tmp_path,
        ["SUCCESS"],
        step="02_clean_silver",
        extra_env={"AWS_EMPTY_MATCH": "quarantine_padron_ruc"},
    )
    assert res.returncode == 0, res.stderr
    destinos = [c.split()[3] for c in _syncs(llamadas)]
    assert "s3://sunat-risk-scoring-silver/quarantine_padron_ruc/" not in destinos
    assert any(
        c.startswith("s3 rm s3://sunat-risk-scoring-silver/quarantine_padron_ruc/ ")
        for c in llamadas
    )


def test_modo_bucket_unico_usa_prefijo_de_capa(tmp_path):
    bucket = "un-solo-bucket"
    res, llamadas = _ejecutar(
        tmp_path,
        ["SUCCESS"],
        extra_env={
            "RAW_BUCKET": bucket,
            "BRONZE_BUCKET": bucket,
            "SILVER_BUCKET": bucket,
            "GOLD_BUCKET": bucket,
        },
    )
    assert res.returncode == 0, res.stderr
    assert _syncs(llamadas)[0].split()[2:4] == [
        f"s3://{bucket}/_staging/gold/ruc_features/",
        f"s3://{bucket}/gold/ruc_features/",
    ]
