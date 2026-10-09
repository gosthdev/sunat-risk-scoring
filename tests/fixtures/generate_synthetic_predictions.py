"""
Genera dos tablas sintéticas de predicciones.
Sirven para desarrollar y probar la librería de métricas
antes de que Dev A entregue predicciones reales del modelo.

Las tablas NO son datos reales de RUCs peruanos.
Son datos inventados con la misma estructura que C2 de lo que serían las predicciones.

Uso:
    python tests/fixtures/generate_synthetic_predictions.py

Requisitos:
    pip install pandas numpy pyarrow
"""

import numpy as np
import pandas as pd
from pathlib import Path

# ─── Configuración ────────────────────────────────────────────────────────────

SEED = 42          # Semilla fija: garantiza que dos personas obtengan el mismo archivo
N_TOTAL = 5_000    # Filas totales
PREVALENCE = 0.01  # 1% positivos → ~50 filas con label=1

FIXTURES_DIR = Path(__file__).parent  # Mismo directorio donde vive este script

DEPARTAMENTOS = [
    "AMAZONAS", "ANCASH", "APURIMAC", "AREQUIPA", "AYACUCHO",
    "CAJAMARCA", "CUSCO", "HUANCAVELICA", "HUANUCO", "ICA",
    "JUNIN", "LA LIBERTAD", "LAMBAYEQUE", "LIMA", "LORETO",
    "MADRE DE DIOS", "MOQUEGUA", "PASCO", "PIURA", "PUNO",
    "SAN MARTIN", "TACNA", "TUMBES", "UCAYALI",
]


# ─── Construcción del esqueleto común ─────────────────────────────────────────

def _base_frame(rng: np.random.Generator) -> pd.DataFrame:
    """
    Crea las columnas que son iguales en ambas tablas:
    RUCs, labels, split train/test estratificado y departamentos.

    El split es estratificado: mantiene ~1% de positivos tanto en train como en test,
    igual que hará Dev A con el Padrón real (decisión D8).
    """
    n_pos = int(N_TOTAL * PREVALENCE)  # ~50 positivos
    n_neg = N_TOTAL - n_pos            # ~4950 negativos

    # Crear labels y mezclarlos aleatoriamente
    labels = np.array([1] * n_pos + [0] * n_neg)
    rng.shuffle(labels)

    # RUCs sintéticos de 11 dígitos (formato real: 20XXXXXXXXXX)
    rucs = [
        f"20{rng.integers(100_000_000, 999_999_999):09d}"
        for _ in range(N_TOTAL)
    ]

    # Split estratificado: 80% train / 20% test, por separado para cada clase
    split_arr = np.full(N_TOTAL, "train", dtype=object)
    pos_idx = np.where(labels == 1)[0]
    neg_idx = np.where(labels == 0)[0]

    n_test_pos = max(1, int(len(pos_idx) * 0.20))
    n_test_neg = int(len(neg_idx) * 0.20)

    split_arr[rng.choice(pos_idx, size=n_test_pos, replace=False)] = "test"
    split_arr[rng.choice(neg_idx, size=n_test_neg, replace=False)] = "test"

    return pd.DataFrame({
        "ruc":             rucs,
        "label":           labels.astype(int),
        "split":           split_arr,
        "departamento":    rng.choice(DEPARTAMENTOS, size=N_TOTAL),
        "model_name":      "LR",
        "variant":         "V2",
        "dataset_version": "v1",
        "scored_at":       pd.Timestamp("2025-10-01").date(),
    })


def _add_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula rank_global: 1 = RUC con mayor score (el más riesgoso)."""
    df = df.copy()
    df["rank_global"] = (
        df["score"].rank(ascending=False, method="first").astype(int)
    )
    return df


# ─── Tabla 1: scores informativos ─────────────────────────────────────────────

def generate_informative(rng: np.random.Generator) -> pd.DataFrame:
    """
    Los positivos tienden a tener scores altos y los negativos scores bajos,
    pero NO perfectamente separados.

    Por qué no perfectos: si todos los positivos tuvieran score=1.0,
    la librería de métricas nunca vería casos intermedios (scores de 0.6, 0.7...)
    y no detectaría bugs en el cálculo de la curva.

    Distribuciones usadas (Beta):
      - Positivos:  Beta(5, 2) → concentrada entre 0.55 y 0.95
      - Negativos:  Beta(2, 8) → concentrada entre 0.05 y 0.40
    """
    df = _base_frame(rng)

    scores = np.where(
        df["label"] == 1,
        rng.beta(a=5, b=2, size=N_TOTAL),   # positivos: scores altos
        rng.beta(a=2, b=8, size=N_TOTAL),   # negativos: scores bajos
    )
    df["score"] = np.clip(scores, 0.0, 1.0).round(4)
    return _add_rank(df)


# ─── Tabla 2: scores aleatorios ───────────────────────────────────────────────

def generate_random(rng: np.random.Generator) -> pd.DataFrame:
    """
    Los scores no tienen ninguna relación con el label.
    Un modelo así no sirve para nada: el PR-AUC debería salir ~0.01
    (igual a la prevalencia, que es lo que da el azar puro).

    Si la librería de métricas da un PR-AUC alto con esta tabla,
    hay un bug en el cálculo.
    """
    df = _base_frame(rng)
    df["score"] = rng.uniform(0.0, 1.0, size=N_TOTAL).round(4)
    return _add_rank(df)


# ─── Validación del contrato C2 ───────────────────────────────────────────────

def validate_c2(df: pd.DataFrame, name: str) -> None:
    """
    Verifica que el DataFrame cumple el contrato C2 columna por columna.
    Si algo falla, lanza un AssertionError con un mensaje claro.
    """
    required_cols = {
        "ruc", "score", "rank_global", "label", "split",
        "departamento", "model_name", "variant", "dataset_version", "scored_at",
    }
    missing = required_cols - set(df.columns)
    assert not missing, f"[{name}] Faltan columnas de C2: {missing}"

    assert df["score"].between(0.0, 1.0).all(), \
        f"[{name}] Hay scores fuera de [0, 1]"

    assert df["label"].isin([0, 1]).all(), \
        f"[{name}] El campo 'label' tiene valores distintos de 0 y 1"

    assert df["ruc"].nunique() == len(df), \
        f"[{name}] Hay RUCs duplicados (debe ser 1 fila por RUC)"

    assert df["split"].isin(["train", "test"]).all(), \
        f"[{name}] El campo 'split' tiene valores distintos de 'train'/'test'"

    # Verificar que los scores no son trivialmente uniformes
    assert df["score"].nunique() > 50, \
        f"[{name}] Scores demasiado uniformes — ¿todos iguales?"

    # Resumen para confirmar que todo está bien
    n_pos = df["label"].sum()
    prev = df["label"].mean() * 100
    n_test = (df["split"] == "test").sum()
    n_test_pos = df.loc[df["split"] == "test", "label"].sum()
    print(
        f"[{name}] ✓ C2 ok | "
        f"{len(df)} filas | "
        f"{n_pos} positivos ({prev:.1f}%) | "
        f"test: {n_test} filas, {n_test_pos} positivos"
    )


# ─── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Generando fixtures sintéticos de predicciones (contrato C2)...\n")

    # La misma semilla garantiza que ambos desarrolladores obtengan
    # exactamente los mismos archivos al correr este script.
    rng = np.random.default_rng(SEED)

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    informative = generate_informative(rng)
    random_pred = generate_random(rng)

    validate_c2(informative, "informative")
    validate_c2(random_pred, "random   ")

    out_inf = FIXTURES_DIR / "synthetic_predictions_informative.parquet"
    out_rnd = FIXTURES_DIR / "synthetic_predictions_random.parquet"

    informative.to_parquet(out_inf, index=False)
    random_pred.to_parquet(out_rnd, index=False)

    print(f"\nArchivos guardados en {FIXTURES_DIR}:")
    print(f"  {out_inf.name}")
    print(f"  {out_rnd.name}")


if __name__ == "__main__":
    main()