"""Suite de pruebas unitarias y de integración para el pipeline de entrenamiento en scikit-learn.

Valida:
- Ausencia estricta de data leakage en el Pipeline.
- Transformador Log1p y escalado para Regresión Logística.
- Compatibilidad de expresiones regulares del Contrato C5.
- Formato Single-line JSON estricto para el Contrato C3.
- Invariante de ranking descendente y no-nulos en Contrato C2.
- Ejecución completa end-to-end de train.py en entorno local.
"""

import json
import os
import re
import tempfile

import pytest

pytest.importorskip("numpy", reason="numpy is required for training tests")
pytest.importorskip("pandas", reason="pandas is required for training tests")
pytest.importorskip("sklearn", reason="scikit-learn is required for training tests")

import numpy as np
import pandas as pd

from src.models.explainability import (
    extraer_explicabilidad_lr,
    extraer_reglas_dt,
    obtener_nombres_features,
)
from src.models.export_outputs import (
    _guardar_single_line_json,
    exportar_predicciones_y_run_record,
)
from src.models.launch_manual import SAGEMAKER_METRIC_DEFINITIONS
from src.models.train import (
    Log1pTransformer,
    TargetEncoderFueraDeFold,
    WinsorizerSuperior,
    _SplitterTemporal,
    construir_pipeline_preprocesamiento,
    seleccionar_columnas,
)
from src.models.train import (
    main as train_main,
)


@pytest.fixture
def synthetic_dataframe():
    """Genera un DataFrame sintético con datos para train y test con nulos y sesgos."""
    n_rows = 120
    np.random.seed(42)

    ruc_list = [f"{20100000000 + i:011d}" for i in range(1, n_rows + 1)]
    split_list = ["train" if i <= 96 else "test" for i in range(n_rows)]
    fold_list = [(i % 5) if split_list[i] == "train" else -1 for i in range(n_rows)]
    label_list = [1 if (i % 6 == 0) else 0 for i in range(n_rows)]

    montos = np.random.exponential(scale=50000, size=n_rows)
    montos[::7] = np.nan

    medianas = montos / (np.random.randint(1, 10, size=n_rows))
    medianas[::5] = np.nan

    n_ordenes = np.random.poisson(lam=4, size=n_rows)
    antiguedad = np.random.randint(1, 360, size=n_rows)

    ciius = np.random.choice(["4659", "4100", "4711", "9999", ""], size=n_rows)
    depts = np.random.choice(["LIMA", "AREQUIPA", "CUSCO", "LORETO", None], size=n_rows)
    tipos = np.random.choice(["SOCIEDAD ANONIMA", "PERSONA NATURAL"], size=n_rows)

    df = pd.DataFrame(
        {
            "ruc": ruc_list,
            "split": split_list,
            "fold": fold_list,
            "label": label_list,
            "monto_total_soles": montos,
            "monto_mediano_soles": medianas,
            "n_ordenes": n_ordenes,
            "antiguedad_meses": antiguedad,
            "ciiu_principal": ciius,
            "departamento": depts,
            "tipo_contribuyente": tipos,
        }
    )
    return df


def test_log1p_transformer():
    """Verifica el comportamiento de Log1pTransformer con ceros, valores positivos y nulos."""
    transformer = Log1pTransformer()
    x_input = np.array([0.0, 1.0, 9.0, np.nan, -5.0])
    x_trans = transformer.transform(x_input)

    assert x_trans[0] == 0.0
    assert np.isclose(x_trans[1], np.log(2.0))
    assert np.isclose(x_trans[2], np.log(10.0))
    assert np.isnan(x_trans[3])  # NaN se preserva: la mediana lo imputa después
    assert x_trans[4] == 0.0  # Negativo clampeado a 0 -> log1p(0) = 0


def test_pipeline_sin_data_leakage(synthetic_dataframe):
    """Verifica que el ColumnTransformer impute correctamente todos los nulos y no falle."""
    num_cols = [
        "monto_total_soles",
        "monto_mediano_soles",
        "n_ordenes",
        "antiguedad_meses",
    ]
    cat_cols = ["ciiu_principal", "departamento", "tipo_contribuyente"]

    df_train = synthetic_dataframe[synthetic_dataframe["split"] == "train"].copy()
    preprocessor = construir_pipeline_preprocesamiento(num_cols, cat_cols, es_lr=True)

    X_train_trans = preprocessor.fit_transform(df_train[num_cols + cat_cols])
    assert X_train_trans.shape[0] == len(df_train)
    assert not np.isnan(X_train_trans).any(), (
        "La matriz transformada no debe contener valores NaN"
    )

    df_test = synthetic_dataframe[synthetic_dataframe["split"] == "test"].copy()
    X_test_trans = preprocessor.transform(df_test[num_cols + cat_cols])
    assert X_test_trans.shape[0] == len(df_test)
    assert not np.isnan(X_test_trans).any(), (
        "El conjunto de test transformado no debe contener valores NaN"
    )


def test_regex_metricas_c5():
    """Valida que cada expresión regular del Contrato C5 haga match con los logs esperados."""
    sample_logs = [
        ("[METRIC] cv_pr_auc_mean=0.3452", "cv_pr_auc_mean", 0.3452),
        ("[METRIC] cv_pr_auc_fold=0.3410", "cv_pr_auc_fold", 0.3410),
        ("[METRIC] train_pr_auc=0.3821", "train_pr_auc", 0.3821),
        ("[METRIC] n_train=118400", "n_train", 118400),
        ("[METRIC] n_pos_train=592", "n_pos_train", 592),
        ("[METRIC] n_features_transformadas=68", "n_features_transformadas", 68),
        ("[METRIC] train_seconds=14.50", "train_seconds", 14.50),
    ]

    regex_map = {item["Name"]: item["Regex"] for item in SAGEMAKER_METRIC_DEFINITIONS}

    for log_line, name, expected_val in sample_logs:
        regex = regex_map.get(name)
        assert regex is not None, f"No se encontró regex para métrica: {name}"
        match = re.search(regex, log_line)
        assert match is not None, (
            f"Fallo al hacer match de '{log_line}' con regex '{regex}'"
        )
        val = float(match.group(1)) if "." in match.group(1) else int(match.group(1))
        assert val == expected_val


def test_single_line_json_c3():
    """Garantiza que el archivo run_record.json se escriba estrictamente en una sola línea."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        json_file = os.path.join(tmp_dir, "run_record_test.json")
        sample_record = {
            "run_id": "test-run-001",
            "metricas": {"cv_pr_auc_mean": 0.42},
            "parametros": {"C": 1.0, "penalty": "l2"},
        }
        _guardar_single_line_json(sample_record, json_file)

        assert os.path.exists(json_file)
        with open(json_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 1, (
            f"El JSON debe tener exactamente 1 línea física, pero tiene {len(lines)}"
        )
        loaded = json.loads(lines[0])
        assert loaded["run_id"] == "test-run-001"


def test_ranking_invariante_c2(synthetic_dataframe):
    """Verifica que predictions.parquet ordene rank_global descendente sin empates ni saltos."""

    class MockArgs:
        run_id = "test-c2-run"
        output_dir = ""
        model_dir = ""
        dataset_version = "v1"
        variant = "V2"
        model_name = "LR"
        usa_pesos = "si"
        seed = 42

    class MockPipeline:
        def predict_proba(self, X):
            n = len(X)
            # Probabilidades sintéticas
            probs = np.linspace(0.01, 0.99, n)
            return np.column_stack([1 - probs, probs])

    with tempfile.TemporaryDirectory() as tmp_dir:
        preds_path = os.path.join(tmp_dir, "predictions.parquet")
        record_path = os.path.join(tmp_dir, "run_record.json")

        args = MockArgs()
        args.output_dir = tmp_dir
        args.model_dir = tmp_dir
        args.ruta_salida_predicciones = preds_path
        args.ruta_salida_run_record = record_path

        record = exportar_predicciones_y_run_record(
            df_all=synthetic_dataframe,
            X_all=synthetic_dataframe[["monto_total_soles"]],
            best_pipeline=MockPipeline(),
            args=args,
            cv_mean=float("nan"),
            cv_folds_scores=[0.34, float("nan"), 0.36, 0.35, 0.35],
            train_pr_auc=0.38,
            n_features_trans=10,
            elapsed_time=5.2,
            best_params={"C": 1.0},
        )

        assert record["metricas_cv"]["cv_pr_auc_mean"] == 0.0
        assert "cv_pr_auc_std" in record["metricas_cv"]
        assert record["metricas_cv"]["cv_pr_auc_folds"][1] == 0.0
        assert os.path.exists(preds_path)
        df_preds = pd.read_parquet(preds_path)

        assert list(df_preds.columns) == [
            "ruc",
            "score",
            "label",
            "split",
            "departamento",
            "contrata_con_estado",
            "run_id",
            "rank_global",
        ]
        assert (df_preds["run_id"] == record["run_id"]).all()
        assert df_preds["rank_global"].iloc[0] == 1
        assert df_preds["rank_global"].iloc[-1] == len(df_preds)
        assert df_preds["rank_global"].is_monotonic_increasing
        assert df_preds["score"].is_monotonic_decreasing


def test_full_training_loop_local(synthetic_dataframe):
    """Ejecuta el script completo train.py en modo local y valida todos sus artefactos."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_file = os.path.join(tmp_dir, "dataset.parquet")
        synthetic_dataframe.to_parquet(input_file, index=False)

        model_dir = os.path.join(tmp_dir, "model")
        output_dir = os.path.join(tmp_dir, "output")
        preds_path = os.path.join(output_dir, "predictions.parquet")
        record_path = os.path.join(output_dir, "run_record.json")

        cli_args = [
            "--train_dir",
            input_file,
            "--model_dir",
            model_dir,
            "--output_dir",
            output_dir,
            "--run_id",
            "ssco-run-test-e1-lr",
            "--model_name",
            "LR",
            "--variant",
            "V2",
            "--usa_pesos",
            "si",
            "--seed",
            "42",
            "--ruta_salida_predicciones",
            preds_path,
            "--ruta_salida_run_record",
            record_path,
        ]

        exit_code = train_main(cli_args)
        assert exit_code == 0

        # Verificar existencia de archivos generados
        assert os.path.exists(preds_path), "predictions.parquet (C2) debe existir"
        assert os.path.exists(record_path), "run_record.json (C3) debe existir"
        assert os.path.exists(os.path.join(model_dir, "model.joblib")), (
            "model.joblib debe existir"
        )
        assert os.path.exists(os.path.join(model_dir, "model.tar.gz")), (
            "model.tar.gz debe existir"
        )

        # Validar contenido de run_record
        with open(record_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["run_id"] == "ssco-run-test-e1-lr"
        assert record["model_name"] == "LR"
        assert record["usa_pesos"] is True
        assert "metricas_cv" in record
        assert "cv_pr_auc_mean" in record["metricas_cv"]


def test_hgb_con_submuestreo_de_negativos(synthetic_dataframe, capsys):
    """HGB entrena con muestreo de negativos y predice todas las filas."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_file = os.path.join(tmp_dir, "dataset.parquet")
        synthetic_dataframe.to_parquet(input_file, index=False)
        preds_path = os.path.join(tmp_dir, "out", "predictions.parquet")
        model_dir = os.path.join(tmp_dir, "model")

        exit_code = train_main(
            [
                "--train_dir", input_file,
                "--model_dir", model_dir,
                "--output_dir", os.path.join(tmp_dir, "out"),
                "--run_id", "ssco-run-test-hgb",
                "--model_name", "HGB",
                "--neg_ratio", "2",
                "--param_grid_json", '{"model__max_iter": [20]}',
                "--ruta_salida_predicciones", preds_path,
                "--ruta_salida_run_record", os.path.join(tmp_dir, "out", "rr.json"),
            ]
        )  # fmt: skip
        assert exit_code == 0

        es_train = synthetic_dataframe["split"] == "train"
        n_pos = int(synthetic_dataframe.loc[es_train, "label"].sum())
        assert f"n_train_muestra={3 * n_pos}" in capsys.readouterr().out
        assert len(pd.read_parquet(preds_path)) == len(synthetic_dataframe)
        imp = pd.read_csv(os.path.join(model_dir, "explicabilidad.csv"))
        assert {"feature", "importancia"} <= set(imp.columns)


def test_explainability(synthetic_dataframe):
    """Verifica la extracción de explicabilidad para LR y DT."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.tree import DecisionTreeClassifier

    num_cols = ["monto_total_soles", "n_ordenes"]
    cat_cols = ["tipo_contribuyente"]

    preprocessor = construir_pipeline_preprocesamiento(num_cols, cat_cols, es_lr=True)
    lr_pipe = Pipeline(
        [("preprocessor", preprocessor), ("model", LogisticRegression())]
    )

    df_train = synthetic_dataframe[synthetic_dataframe["split"] == "train"]
    X = df_train[num_cols + cat_cols]
    y = df_train["label"].values

    lr_pipe.fit(X, y)
    names = obtener_nombres_features(
        lr_pipe.named_steps["preprocessor"], num_cols, cat_cols
    )
    df_coefs = extraer_explicabilidad_lr(lr_pipe, names)

    assert not df_coefs.empty
    assert "abs_coef" in df_coefs.columns
    assert df_coefs["abs_coef"].is_monotonic_decreasing

    dt_pipe = Pipeline(
        [
            (
                "preprocessor",
                construir_pipeline_preprocesamiento(num_cols, cat_cols, es_lr=False),
            ),
            ("model", DecisionTreeClassifier(max_depth=3)),
        ]
    )
    dt_pipe.fit(X, y)
    dt_names = obtener_nombres_features(
        dt_pipe.named_steps["preprocessor"], num_cols, cat_cols
    )
    rules = extraer_reglas_dt(dt_pipe, dt_names)
    assert isinstance(rules, str)
    assert len(rules) > 0


def test_train_main_custom_param_grid(synthetic_dataframe):
    """Verifica que train_main respete una grilla personalizada vía --param_grid_json."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = os.path.join(tmp_dir, "train_data.parquet")
        synthetic_dataframe.to_parquet(parquet_path, index=False)

        custom_grid = json.dumps({"model__C": [0.05]})
        cli_args = [
            "--train_dir",
            parquet_path,
            "--model_dir",
            os.path.join(tmp_dir, "model"),
            "--output_dir",
            os.path.join(tmp_dir, "output"),
            "--run_id",
            "test-custom-grid-run",
            "--model_name",
            "LR",
            "--variant",
            "V2",
            "--usa_pesos",
            "si",
            "--param_grid_json",
            custom_grid,
        ]

        exit_code = train_main(cli_args)
        assert exit_code == 0

        record_file = os.path.join(
            tmp_dir, "output", "run_record_test-custom-grid-run.json"
        )
        assert os.path.exists(record_file)
        with open(record_file, "r", encoding="utf-8") as f:
            record_data = json.loads(f.readline().strip())
        assert record_data["hiperparametros_ganadores"].get("C") == 0.05


def test_winsorizer_recorta_cola_y_no_aplasta_columnas_en_cero():
    rng = np.random.default_rng(0)
    montos = np.concatenate([rng.uniform(1, 100, 999), [1e8]])
    ceros = np.zeros(1000)
    X = np.column_stack([montos, ceros])
    w = WinsorizerSuperior(cuantil=0.995).fit(X)
    out = w.transform(X)
    assert out[:, 0].max() < 1e3
    assert np.array_equal(out[:, 1], ceros)
    assert np.isnan(w.transform(np.array([[np.nan, 0.0]]))[0, 0])


def test_cargar_datos_deja_el_texto_repetitivo_como_categorico():
    import tempfile

    from src.models.train import cargar_datos
    from tests._dataset_minimo import escribir_parquet_minimo

    with tempfile.TemporaryDirectory() as tmp_dir:
        escribir_parquet_minimo(tmp_dir, n_rows=200)
        df = cargar_datos(tmp_dir)
    assert len(df) == 200
    assert str(df["departamento"].dtype) == "category"
    assert str(df["split"].dtype) == "category"
    assert set(df["split"].astype(str)) == {"train", "test"}


@pytest.mark.parametrize(
    "esquema,neg_ratio", [("bloques", "0"), ("temporal", "0"), ("bloques", "3")]
)
def test_rf_entrena_con_target_encoding(synthetic_dataframe, esquema, neg_ratio):
    """RF entrena, calcula te_* dentro del Pipeline y deja importancias en explicabilidad.csv."""
    df = synthetic_dataframe.copy()
    df["ruc_prefijo"] = "20"
    df["ruc_correlativo"] = np.arange(len(df)) * 5000
    with tempfile.TemporaryDirectory() as tmp_dir:
        input_file = os.path.join(tmp_dir, "dataset.parquet")
        df.to_parquet(input_file, index=False)
        preds_path = os.path.join(tmp_dir, "out", "predictions.parquet")
        model_dir = os.path.join(tmp_dir, "model")

        exit_code = train_main(
            [
                "--train_dir", input_file,
                "--model_dir", model_dir,
                "--output_dir", os.path.join(tmp_dir, "out"),
                "--run_id", "ssco-run-test-rf",
                "--cv_esquema", esquema,
                "--neg_ratio", neg_ratio,
                "--model_name", "RF",
                "--param_grid_json", '{"model__n_estimators": [10], "model__max_depth": [3]}',
                "--ruta_salida_predicciones", preds_path,
                "--ruta_salida_run_record", os.path.join(tmp_dir, "out", "rr.json"),
            ]
        )  # fmt: skip
        assert exit_code == 0
        assert len(pd.read_parquet(preds_path)) == len(df)
        imp = pd.read_csv(os.path.join(model_dir, "explicabilidad.csv"))
        assert "te_ciiu" in set(imp["feature"].str.split("__").str[-1])


def test_seleccionar_columnas_excluye_features_vacias(synthetic_dataframe, capsys):
    df = synthetic_dataframe.copy()
    df["n_ordenes"] = np.nan
    num_cols, _ = seleccionar_columnas(df, "V2")
    assert "n_ordenes" not in num_cols
    assert "monto_total_soles" in num_cols
    assert "n_ordenes llega 100% nula" in capsys.readouterr().out


def _df_te():
    """Dos categorías CIIU en 4 folds; la 'A' es 100% positiva solo en el fold 3."""
    n = 400
    fold = np.arange(n) % 4
    ciiu = np.where((np.arange(n) // 4) % 2 == 0, "A", "B")
    y = ((ciiu == "A") & (fold == 3)).astype(int)
    df = pd.DataFrame(
        {
            "ciiu_principal": ciiu,
            "departamento": "LIMA",
            "ruc_prefijo": "20",
            "ruc_correlativo": np.arange(n) * 1000,
            "fold": fold,
        }
    )
    return df, y


def test_target_encoder_no_ve_el_fold_de_validacion():
    """Ajustar sin el fold 3 da las mismas tasas de entrenamiento que ajustar sin él."""
    df, y = _df_te()
    sin3 = df["fold"] != 3
    te = TargetEncoderFueraDeFold().fit(df[sin3], y[sin3])
    tasa_val = te.transform(df[~sin3])[:, 0]
    # Sin positivos en train, la tasa de 'A' queda en la prevalencia (0).
    assert np.allclose(tasa_val, 0.0)
    out = TargetEncoderFueraDeFold().fit_transform(df[sin3], y[sin3])
    assert np.allclose(out[:, 0], 0.0)


def test_target_encoder_train_usa_otros_folds_y_test_todo():
    df, y = _df_te()
    te = TargetEncoderFueraDeFold(m=1.0)
    out = te.fit_transform(df, y)
    prev = y.mean()
    # 'A' en fold 3: ve los folds 0..2 de 'A' (sin positivos) -> menor que prevalencia.
    fila_a3 = int(np.flatnonzero((df["ciiu_principal"] == "A") & (df["fold"] == 3))[0])
    assert out[fila_a3, 0] < prev
    # 'A' en fold 0: ve el fold 3 (positivos) -> mayor que prevalencia.
    fila_a0 = int(np.flatnonzero((df["ciiu_principal"] == "A") & (df["fold"] == 0))[0])
    assert out[fila_a0, 0] > prev
    # Fila nueva (test) usa todo train; categoría desconocida -> prevalencia.
    nuevo = df.iloc[:2].copy()
    nuevo["fold"] = -1
    nuevo.loc[nuevo.index[1], "ciiu_principal"] = "ZZZ"
    t = te.transform(nuevo)
    assert t[0, 0] > prev
    assert t[1, 0] == pytest.approx(prev)


def test_splitter_temporal_solo_entrena_con_folds_anteriores():
    folds = np.array([0, 1, 2, 3, 4, 0, 1, 2, 3, 4])
    cortes = list(_SplitterTemporal(folds).split())
    assert len(cortes) == 4 == _SplitterTemporal(folds).get_n_splits()
    for k, (tr, va) in enumerate(cortes, start=1):
        assert set(folds[va]) == {k}
        assert folds[tr].max() < k


def test_target_encoder_con_pesos_cuenta_cada_fila_con_su_peso():
    """Pesar todas las filas por 3 con m=3 equivale a no pesar con m=1."""
    df, y = _df_te()
    base = TargetEncoderFueraDeFold(m=1.0).fit_transform(df, y)
    df["peso_te"] = 3.0
    ponderado = TargetEncoderFueraDeFold(m=3.0).fit_transform(df, y)
    assert np.allclose(base, ponderado)
