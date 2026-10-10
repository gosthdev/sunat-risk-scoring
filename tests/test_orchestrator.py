"""Pruebas unitarias y de integración para el orquestador de experimentos SSCO."""

import json
import os
import tempfile

import pytest

pytest.importorskip("yaml", reason="pyyaml is required for orchestrator tests")
pytest.importorskip("pandas", reason="pandas is required for orchestrator tests")
pytest.importorskip("sklearn", reason="scikit-learn is required for orchestrator tests")

from src.models.orchestrator import (
    cargar_configuracion,
    ejecutar_experimento_runner,
    filtrar_experimentos,
    generar_dataset_sintetico,
    generar_leaderboard_markdown,
    seleccionar_modelo_campeon,
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


def test_generar_dataset_sintetico():
    """Verifica que el generador sintético produzca un parquet válido con todos los contratos."""
    import pandas as pd

    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = generar_dataset_sintetico(tmp_dir, n_rows=150, seed=42)
        assert os.path.exists(parquet_path)

        df = pd.read_parquet(parquet_path)
        assert len(df) == 150
        assert "ruc" in df.columns
        assert "split" in df.columns
        assert "fold" in df.columns
        assert "label" in df.columns
        assert "monto_total_soles" in df.columns
        assert "ciiu_principal" in df.columns

        # Validar partición de folds
        train_df = df[df["split"] == "train"]
        test_df = df[df["split"] == "test"]
        assert len(train_df) == 120
        assert len(test_df) == 30
        assert set(train_df["fold"].unique()) == {0, 1, 2, 3, 4}
        assert list(test_df["fold"].unique()) == [-1]


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
    md = generar_leaderboard_markdown(resultados, campeon_info, "v01")
    assert "# Leaderboard Oficial" in md
    assert "| `E1` | **LR** |" in md
    assert "🏆 **CHAMPION**" in md
    assert "Justificación de Selección de Modelo" in md


def test_orchestrator_main_end_to_end():
    """Ejecución end-to-end completa del orquestador en modo runner con synthetic data."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        summary_file = os.path.join(tmp_dir, "test_summary.md")
        cli_args = [
            "--config",
            "config/experiments.yml",
            "--experiments",
            "E1",
            "--synthetic",
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
            assert data["dataset_version"] == "v01"
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
        nested_summary = os.path.join(tmp_dir, "nested", "deep", "custom_summary.md")
        cli_args = [
            "--config",
            "config/experiments.yml",
            "--experiments",
            "E1",
            "--synthetic",
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
                                "cv_pr_auc_fold": [0.31, 0.33, 0.37, 0.39, 0.35],
                                "train_pr_auc": 0.40,
                            },
                            "hiperparametros_ganadores": {"model__C": 0.1},
                        }
                    )
                    + "\n"
                )
            return 0

        monkeypatch.setattr("src.models.orchestrator.train_main", mock_train_c3)
        res1 = ejecutar_experimento_runner(
            exp, tmp_dir, tmp_dir, 42, "v01"
        )
        assert res1["status"] == "SUCCESS"
        assert res1["cv_folds"] == [0.31, 0.33, 0.37, 0.39, 0.35]
        assert np.isclose(res1["cv_pr_auc_std"], float(np.std([0.31, 0.33, 0.37, 0.39, 0.35])))
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
        res2 = ejecutar_experimento_runner(
            exp, tmp_dir, tmp_dir, 42, "v01"
        )
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
        res3 = ejecutar_experimento_runner(
            exp, tmp_dir, tmp_dir, 42, "v01"
        )
        assert res3["status"] == "SUCCESS"
        assert res3["cv_folds"] == []
        assert res3["cv_pr_auc_std"] == 0.0
        assert res3["best_params"] == {}

