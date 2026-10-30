"""Pruebas unitarias y de integración para el orquestador de experimentos SSCO."""

import json
import os
import tempfile

import pytest

from tests._dataset_minimo import escribir_parquet_minimo

pytest.importorskip("yaml", reason="pyyaml is required for orchestrator tests")
pytest.importorskip("pandas", reason="pandas is required for orchestrator tests")
pytest.importorskip("sklearn", reason="scikit-learn is required for orchestrator tests")

from src.models.orchestrator import (
    cargar_configuracion,
    ejecutar_experimento_runner,
    filtrar_experimentos,
    generar_leaderboard_markdown,
    seleccionar_modelo_campeon,
    validar_dataset_entrada,
)
from src.models.orchestrator import (
    main as orchestrator_main,
)


def test_cargar_configuracion_valida():
    """Verifica que se pueda cargar el archivo de configuración oficial."""
    config = cargar_configuracion("config/experiments.yml")
    assert "dataset_version" in config
    assert "seed" in config
    assert "experiments" in config
    assert len(config["experiments"]) >= 6

    ids = [e["id"] for e in config["experiments"]]
    assert "E1" in ids
    assert "E2" in ids
    assert "E3" in ids
    assert "E4" in ids
    assert "E5" in ids
    assert "E6" in ids


def test_cargar_configuracion_invalida():
    """Verifica el manejo de error ante archivos inexistentes o inválidos."""
    with pytest.raises(FileNotFoundError):
        cargar_configuracion("config/archivo_inexistente.yml")

    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write("clave_cualquiera: 123\n")
        temp_name = f.name

    try:
        with pytest.raises(ValueError, match="experiments"):
            cargar_configuracion(temp_name)
    finally:
        os.remove(temp_name)


def test_filtrar_experimentos():
    """Verifica el filtrado de experimentos por ID."""
    todos = [
        {"id": "E1", "model_name": "LR"},
        {"id": "E2", "model_name": "DT"},
        {"id": "E3", "model_name": "LR"},
    ]

    assert len(filtrar_experimentos(todos, "ALL")) == 3
    assert len(filtrar_experimentos(todos, "")) == 3

    filtrados_e1 = filtrar_experimentos(todos, "E1")
    assert len(filtrados_e1) == 1
    assert filtrados_e1[0]["id"] == "E1"

    filtrados_e1_e2 = filtrar_experimentos(todos, "E1, E2")
    assert len(filtrados_e1_e2) == 2

    with pytest.raises(ValueError, match="Ningún experimento coincide"):
        filtrar_experimentos(todos, "E99")


def test_validar_dataset_entrada():
    assert validar_dataset_entrada("") is not None
    assert validar_dataset_entrada("/ruta/que/no/existe") is not None
    with tempfile.TemporaryDirectory() as tmp_dir:
        assert "parquet" in validar_dataset_entrada(tmp_dir)
        escribir_parquet_minimo(tmp_dir, n_rows=50)
        assert validar_dataset_entrada(tmp_dir) is None


def test_orchestrator_falla_sin_dataset_real():
    """Sin --train_dir el modo runner debe fallar y no inventar datos."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        exit_code = orchestrator_main(
            [
                "--config",
                "config/experiments.yml",
                "--experiments",
                "E1",
                "--output_dir",
                tmp_dir,
            ]
        )
        assert exit_code == 2
        assert not os.path.exists(os.path.join(tmp_dir, "leaderboard.json"))


def test_seleccionar_modelo_campeon_regla_oficial():
    """Verifica la regla de decisión entre LR (E1) y DT (E2)."""
    # Caso 1: LR >= DT -> Campeón LR por parsimonia
    resultados_1 = [
        {
            "id": "E1",
            "model_name": "LR",
            "variant": "V2",
            "usa_pesos": "si",
            "status": "SUCCESS",
            "cv_pr_auc_mean": 0.4500,
        },
        {
            "id": "E2",
            "model_name": "DT",
            "variant": "V2",
            "usa_pesos": "si",
            "status": "SUCCESS",
            "cv_pr_auc_mean": 0.4200,
        },
    ]
    campeon_1 = seleccionar_modelo_campeon(resultados_1)
    assert campeon_1["campeon_id"] == "E1"
    assert campeon_1["campeon_model"] == "LR"
    assert "parsimonia" in campeon_1["justificacion"].lower()

    # Caso 2: DT > LR -> Campeón DT por no-linealidad
    resultados_2 = [
        {
            "id": "E1",
            "model_name": "LR",
            "variant": "V2",
            "usa_pesos": "si",
            "status": "SUCCESS",
            "cv_pr_auc_mean": 0.4100,
        },
        {
            "id": "E2",
            "model_name": "DT",
            "variant": "V2",
            "usa_pesos": "si",
            "status": "SUCCESS",
            "cv_pr_auc_mean": 0.4800,
        },
    ]
    campeon_2 = seleccionar_modelo_campeon(resultados_2)
    assert campeon_2["campeon_id"] == "E2"
    assert campeon_2["campeon_model"] == "DT"
    assert "no lineal" in campeon_2["justificacion"].lower()


def test_generar_leaderboard_markdown():
    """Verifica el formato del Leaderboard Markdown."""
    resultados = [
        {
            "id": "E1",
            "model_name": "LR",
            "variant": "V2",
            "usa_pesos": "si",
            "status": "SUCCESS",
            "cv_pr_auc_mean": 0.3500,
            "cv_pr_auc_std": 0.020,
            "train_pr_auc": 0.3600,
            "overfit_gap": 0.0100,
            "train_seconds": 1.2,
        }
    ]
    campeon_info = {
        "campeon_id": "E1",
        "campeon_model": "LR",
        "justificacion": "Mejor modelo candidato oficial.",
    }
    md = generar_leaderboard_markdown(resultados, campeon_info, "v1")
    assert "# Leaderboard Oficial" in md
    assert "| `E1` | **LR** |" in md
    assert "🏆 **CHAMPION**" in md
    assert "Justificación de Selección de Modelo" in md


def test_orchestrator_main_end_to_end():
    """Ejecución end-to-end completa del orquestador en modo runner con un dataset mínimo de prueba."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = os.path.join(tmp_dir, "data")
        escribir_parquet_minimo(data_dir, n_rows=600)
        summary_file = os.path.join(tmp_dir, "test_summary.md")
        cli_args = [
            "--config",
            "config/experiments.yml",
            "--experiments",
            "E1",
            "--train_dir",
            data_dir,
            "--output_dir",
            tmp_dir,
            "--output_summary_file",
            summary_file,
        ]

        exit_code = orchestrator_main(cli_args)
        assert exit_code == 0

        # Verificar existencia de archivos clave
        assert os.path.exists(summary_file)
        leaderboard_md = os.path.join(tmp_dir, "leaderboard.md")
        assert os.path.exists(leaderboard_md)
        json_leaderboard = os.path.join(tmp_dir, "leaderboard.json")
        assert os.path.exists(json_leaderboard)

        with open(json_leaderboard, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert data["dataset_version"] == "v1"
            assert "campeon" in data
            assert len(data["experimentos"]) == 1
            exp0 = data["experimentos"][0]
            assert exp0["id"] == "E1"
            assert exp0["status"] == "SUCCESS"
            assert len(exp0["cv_folds"]) == 5
            assert exp0["cv_pr_auc_std"] > 0
            assert exp0["best_params"] != {}


def test_orchestrator_main_nested_summary_file():
    """Verifica que el orquestador cree directorios padres inexistentes para output_summary_file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = os.path.join(tmp_dir, "data")
        escribir_parquet_minimo(data_dir, n_rows=600)
        nested_summary = os.path.join(tmp_dir, "nested", "deep", "custom_summary.md")
        cli_args = [
            "--config",
            "config/experiments.yml",
            "--experiments",
            "E1",
            "--train_dir",
            data_dir,
            "--output_dir",
            tmp_dir,
            "--output_summary_file",
            nested_summary,
        ]

        exit_code = orchestrator_main(cli_args)
        assert exit_code == 0
        assert os.path.exists(nested_summary)
        leaderboard_md = os.path.join(tmp_dir, "leaderboard.md")
        assert os.path.exists(leaderboard_md)


def test_orchestrator_fold_scores_and_params_fallbacks(monkeypatch):
    """Verifica la robustez en la extracción de métricas de CV y mejores parámetros ante formatos diversos."""
    import numpy as np

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Mocking train_main para simular distintos run_record.json
        exp = {
            "id": "E1",
            "model_name": "LR",
            "variant": "V2",
            "usa_pesos": "si",
            "hyperparameters": {"param_grid": {"model__C": [0.1]}},
        }

        # Caso 1: Formato canónico C3 (lista y hiperparametros_ganadores)
        def mock_train_c3(args_list):
            rec_idx = args_list.index("--ruta_salida_run_record")
            rec_path = args_list[rec_idx + 1]
            os.makedirs(os.path.dirname(rec_path), exist_ok=True)
            with open(rec_path, "w", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "metricas_cv": {
                                "cv_pr_auc_mean": 0.35,
                                "cv_pr_auc_folds": [0.31, 0.33, 0.37, 0.39, 0.35],
                                "train_pr_auc": 0.40,
                            },
                            "hiperparametros_ganadores": {"model__C": 0.1},
                        }
                    )
                    + "\n"
                )
            return 0

        monkeypatch.setattr("src.models.orchestrator.train_main", mock_train_c3)
        res1 = ejecutar_experimento_runner(exp, tmp_dir, tmp_dir, 42, "v1")
        assert res1["status"] == "SUCCESS"
        assert res1["cv_folds"] == [0.31, 0.33, 0.37, 0.39, 0.35]
        assert np.isclose(
            res1["cv_pr_auc_std"], float(np.std([0.31, 0.33, 0.37, 0.39, 0.35]))
        )
        assert res1["best_params"] == {"model__C": 0.1}

        # Caso 2: Formato legado con escalares y hiperparametros_optimos
        def mock_train_legacy(args_list):
            rec_idx = args_list.index("--ruta_salida_run_record")
            rec_path = args_list[rec_idx + 1]
            with open(rec_path, "w", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "metricas_cv": {
                                "cv_pr_auc_mean": 0.28,
                                "cv_pr_auc_fold_0": 0.25,
                                "cv_pr_auc_fold_1": 0.29,
                                "cv_pr_auc_fold_2": 0.30,
                                "cv_pr_auc_fold_3": 0.26,
                                "cv_pr_auc_fold_4": 0.30,
                                "train_pr_auc": 0.32,
                            },
                            "hiperparametros_optimos": {"model__C": 1.0},
                        }
                    )
                    + "\n"
                )
            return 0

        monkeypatch.setattr("src.models.orchestrator.train_main", mock_train_legacy)
        res2 = ejecutar_experimento_runner(exp, tmp_dir, tmp_dir, 42, "v1")
        assert res2["status"] == "SUCCESS"
        assert res2["cv_folds"] == [0.25, 0.29, 0.30, 0.26, 0.30]
        assert res2["cv_pr_auc_std"] > 0
        assert res2["best_params"] == {"model__C": 1.0}

        # Caso 3: Valores nulos en JSON (cv_pr_auc_fold = null, hiperparametros_ganadores = null)
        def mock_train_nulls(args_list):
            rec_idx = args_list.index("--ruta_salida_run_record")
            rec_path = args_list[rec_idx + 1]
            with open(rec_path, "w", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "metricas_cv": {
                                "cv_pr_auc_mean": None,
                                "cv_pr_auc_fold": None,
                                "train_pr_auc": None,
                            },
                            "hiperparametros_ganadores": None,
                        }
                    )
                    + "\n"
                )
            return 0

        monkeypatch.setattr("src.models.orchestrator.train_main", mock_train_nulls)
        res3 = ejecutar_experimento_runner(exp, tmp_dir, tmp_dir, 42, "v1")
        assert res3["status"] == "SUCCESS"
        assert res3["cv_folds"] == []
        assert res3["cv_pr_auc_std"] == 0.0
        assert res3["best_params"] == {}


def test_resultado_desde_record():
    from src.models.orchestrator import resultado_desde_record

    record = {
        "metricas_cv": {
            "cv_pr_auc_mean": 0.2,
            "train_pr_auc": 0.5,
            "cv_pr_auc_folds": [0.1, 0.2, 0.3],
        },
        "hiperparametros_ganadores": {"C": 1.0},
    }
    exp = {"id": "E1", "model_name": "LR", "variant": "V2", "usa_pesos": "si"}
    out = resultado_desde_record(exp, "run", record, 10.0, "r.json", "p.parquet")
    assert out["status"] == "SUCCESS"
    assert out["overfit_gap"] == pytest.approx(0.3)
    assert out["best_params"] == {"C": 1.0}


def test_target_sagemaker_lanza_un_solo_job_y_no_espera(monkeypatch):
    import src.models.orchestrator as orq

    llamadas = []

    def falso_lanzar(**kwargs):
        llamadas.append(kwargs)
        return {"TrainingJobArn": "arn:aws:sagemaker:::training-job/ssco-all-t1"}

    monkeypatch.setattr(orq, "lanzar_orquestador_sagemaker", falso_lanzar)
    with tempfile.TemporaryDirectory() as tmp_dir:
        code = orchestrator_main(
            [
                "--config",
                "config/experiments.yml",
                "--target",
                "sagemaker",
                "--run_tag",
                "t1",
                "--output_dir",
                tmp_dir,
            ]
        )
    assert code == 0
    assert len(llamadas) == 1
    assert llamadas[0]["run_tag"] == "t1"
    assert llamadas[0]["experiments"] == "ALL"


def test_reanudar_omite_experimentos_ya_publicados(monkeypatch):
    import src.models.orchestrator as orq

    record = {
        "metricas_cv": {"cv_pr_auc_mean": 0.3, "cv_pr_auc_folds": [0.3] * 5},
        "hiperparametros_ganadores": {"C": 1.0},
    }

    def falso_buscar(bucket, exp, run_id, output_dir):
        assert run_id.endswith("-tag9")
        return orq.resultado_desde_record(exp, run_id, record, 0.0, "r", "p")

    def no_debe_entrenar(**kwargs):
        raise AssertionError("no debe reentrenar un experimento ya publicado")

    monkeypatch.setattr(orq, "buscar_resultado_s3", falso_buscar)
    monkeypatch.setattr(orq, "ejecutar_experimento_runner", no_debe_entrenar)
    monkeypatch.setattr(orq, "publicar_leaderboard_s3", lambda *a, **k: None)
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = os.path.join(tmp_dir, "data")
        escribir_parquet_minimo(data_dir, n_rows=50)
        code = orchestrator_main(
            [
                "--config",
                "config/experiments.yml",
                "--experiments",
                "E1",
                "--train_dir",
                data_dir,
                "--output_dir",
                tmp_dir,
                "--publicar_s3",
                "si",
                "--reanudar",
                "si",
                "--run_tag",
                "tag9",
            ]
        )
    assert code == 0


def test_publicar_leaderboard_no_evita_pisar_el_leaderboard_compartido(monkeypatch):
    import src.models.orchestrator as orq

    monkeypatch.setattr(orq, "buscar_resultado_s3", lambda *a, **k: None)
    monkeypatch.setattr(orq, "publicar_resultado_s3", lambda *a, **k: None)

    def no_debe_publicar(*args, **kwargs):
        raise AssertionError("un job de la matriz no debe publicar el leaderboard")

    monkeypatch.setattr(orq, "publicar_leaderboard_s3", no_debe_publicar)
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = os.path.join(tmp_dir, "data")
        escribir_parquet_minimo(data_dir, n_rows=200)
        code = orchestrator_main(
            [
                "--config",
                "config/experiments.yml",
                "--experiments",
                "E1",
                "--train_dir",
                data_dir,
                "--output_dir",
                tmp_dir,
                "--publicar_s3",
                "si",
                "--publicar_leaderboard",
                "no",
                "--run_tag",
                "t3",
            ]
        )
    assert code == 0


def test_consolidar_arma_leaderboard_y_grafico_desde_s3(monkeypatch):
    import src.models.orchestrator as orq

    record = {
        "metricas_cv": {"cv_pr_auc_mean": 0.4, "cv_pr_auc_folds": [0.4] * 5},
        "hiperparametros_ganadores": {},
    }
    publicados = []
    subidos = []

    def falso_buscar(bucket, exp, run_id, output_dir):
        assert run_id.endswith("-t4")
        if exp["id"] == "E2":
            return None
        return orq.resultado_desde_record(exp, run_id, record, 0.0, "r", "p")

    monkeypatch.setattr(orq, "buscar_resultado_s3", falso_buscar)
    monkeypatch.setattr(
        orq, "publicar_leaderboard_s3", lambda b, tag, res, ver: publicados.append(res)
    )
    monkeypatch.setattr(
        orq, "publicar_archivos_s3", lambda b, pref, archivos: subidos.append(pref)
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        code = orchestrator_main(
            [
                "--config",
                "config/experiments.yml",
                "--experiments",
                "E1,E2",
                "--target",
                "consolidar",
                "--output_dir",
                tmp_dir,
                "--run_tag",
                "t4",
            ]
        )
        assert os.path.isfile(os.path.join(tmp_dir, "leaderboard.md"))
        assert os.path.getsize(os.path.join(tmp_dir, "leaderboard.png")) > 0
    estados = {r["id"]: r["status"] for r in publicados[0]}
    assert estados == {"E1": "SUCCESS", "E2": "FAILED"}
    assert subidos == ["model_leaderboards/tag=t4"]
    assert code == 1


def test_graficos_genera_y_publica_los_png_de_cada_experimento(monkeypatch):
    import src.models.orchestrator as orq

    subidos = []
    monkeypatch.setattr(orq, "buscar_resultado_s3", lambda *a, **k: None)
    monkeypatch.setattr(orq, "publicar_resultado_s3", lambda *a, **k: None)
    monkeypatch.setattr(orq, "publicar_leaderboard_s3", lambda *a, **k: None)
    monkeypatch.setattr(
        orq,
        "publicar_archivos_s3",
        lambda b, pref, archivos: subidos.append((pref, archivos)),
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = os.path.join(tmp_dir, "data")
        escribir_parquet_minimo(data_dir, n_rows=300)
        code = orchestrator_main(
            [
                "--config",
                "config/experiments.yml",
                "--experiments",
                "E1",
                "--train_dir",
                data_dir,
                "--output_dir",
                tmp_dir,
                "--publicar_s3",
                "si",
                "--graficos",
                "si",
                "--run_tag",
                "t5",
            ]
        )
    assert code == 0
    prefijo, archivos = subidos[0]
    assert prefijo == "model_reports/run_id=ssco-run-E1-lr-v2-w1-t5"
    nombres = {os.path.basename(a) for a in archivos}
    assert {"pr_curve.png", "gain_curve.png", "score_distribution.png"} <= nombres


def test_resultado_desde_record_usa_el_tiempo_del_run_record_al_consolidar():
    from src.models.orchestrator import resultado_desde_record

    record = {
        "metricas_cv": {"cv_pr_auc_mean": 0.3, "cv_pr_auc_folds": [0.3] * 5},
        "segundos_entrenamiento": 123.4,
    }
    exp = {"id": "E1", "model_name": "LR"}
    desde_s3 = resultado_desde_record(exp, "run", record, 0.0, "r", "p")
    medido = resultado_desde_record(exp, "run", record, 9.0, "r", "p")
    assert desde_s3["train_seconds"] == 123.4
    assert medido["train_seconds"] == 9.0


def _resultado(id_, modelo, variante, pesos, auc, status="SUCCESS"):
    return {
        "id": id_,
        "model_name": modelo,
        "variant": variante,
        "usa_pesos": pesos,
        "status": status,
        "cv_pr_auc_mean": auc,
    }


def test_campeon_nunca_es_un_benchmark_con_fuga_ni_una_ablacion():
    # Las regresiones logísticas oficiales fallaron: el V1 y el V2 sin pesos puntúan más alto,
    # pero no pueden ser campeones. Gana el árbol oficial.
    resultados = [
        _resultado("E1", "LR", "V2", "si", 0.0, status="FAILED"),
        _resultado("E9", "LR", "V2", "si", 0.0, status="FAILED"),
        _resultado("E3", "LR", "V1", "si", 0.9),
        _resultado("E5", "LR", "V2", "no", 0.8),
        _resultado("E2", "DT", "V2", "si", 0.4),
        _resultado("E10", "DT", "V2", "si", 0.45),
    ]
    campeon = seleccionar_modelo_campeon(resultados)
    assert campeon["campeon_id"] == "E10"
    assert "no hubo comparación" in campeon["justificacion"].lower()


def test_sin_candidatos_oficiales_no_hay_campeon():
    resultados = [
        _resultado("E3", "LR", "V1", "si", 0.9),
        _resultado("E5", "LR", "V2", "no", 0.8),
        _resultado("E1", "LR", "V2", "si", 0.0, status="FAILED"),
    ]
    campeon = seleccionar_modelo_campeon(resultados)
    assert campeon["campeon_id"] == "N/A"


def test_publicar_resultado_escribe_metricas_de_test_al_final(monkeypatch, tmp_path):
    import pandas as pd

    import src.models.evaluation.metrics as met
    import src.models.orchestrator as orq

    eventos = []

    class FalsoS3:
        def upload_file(self, origen, bucket, clave):
            eventos.append(clave)

    def falso_export(df, destino, **kwargs):
        assert set(df["split"]) == {"test"}
        eventos.append(destino)

    monkeypatch.setattr(orq, "_s3_client", lambda: FalsoS3())
    monkeypatch.setattr(met, "export_metrics_flat", falso_export)

    record = tmp_path / "record.json"
    record.write_text("{}")
    preds = tmp_path / "preds.parquet"
    pd.DataFrame(
        {
            "score": [0.1, 0.9, 0.5],
            "label": [0, 1, 0],
            "split": ["train", "test", "test"],
            "run_id": ["r1"] * 3,
        }
    ).to_parquet(preds)

    orq.publicar_resultado_s3("b", "r1", str(record), str(preds), str(tmp_path))

    assert eventos[-1] == "s3://b/model_metrics/run_id=r1/metrics.json"
