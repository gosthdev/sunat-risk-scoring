"""
Visualizaciones analíticas para el modelo de scoring SSCO

Cada función recibe una lista de entradas con formato:
    [{"label": "E1 LR V2", "df": <DataFrame C2>}, ...]

Todas devuelven un matplotlib.figure.Figure listo para guardar o mostrar.
El caller decide si persiste en local, S3 o registra en MLflow con log_figure().

Funciones disponibles:
- plot_pr_curve            Curva Precisión–Recall con línea de prevalencia (azar)
- plot_roc_curve           Curva ROC con diagonal de azar
- plot_gain_curve          Curva de captura acumulada (la más legible para SUNAT)
- plot_lift_curve          Lift en función del % de población revisada
- plot_confusion_matrices  Matrices de confusión al top-1% y top-5% lado a lado
- plot_calibration_curve   Fracción de positivos vs score medio por bin (Brier)
- plot_score_distribution  Histograma de scores superpuesto por clase

save_all() genera y exporta los 7 gráficos a un directorio dado.
"""

from __future__ import annotations

import os
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_curve

matplotlib.use("Agg")  # backend no-interactivo; seguro en CI y SageMaker

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

PredEntry = dict[str, Any]  # {"label": str, "df": pd.DataFrame}


def _get_test(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra solo filas de test; si no hay columna split devuelve todo."""
    if "split" in df.columns:
        return df[df["split"].str.lower() == "test"].copy()
    return df.copy()


def _add_footer(fig: plt.Figure, run_id: str) -> None:
    if run_id:
        fig.text(
            0.99, 0.01, f"run_id: {run_id}",
            ha="right", va="bottom", fontsize=7, color="gray",
        )


# ---------------------------------------------------------------------------
# 1. Curva Precisión–Recall
# ---------------------------------------------------------------------------

def plot_pr_curve(
    entries: list[PredEntry],
    run_id: str = "",
) -> plt.Figure:
    """
    Curva PR para cada entrada en `entries`.
    Línea horizontal discontinua en la prevalencia del primer entry (referencia azar).
    """
    fig, ax = plt.subplots(figsize=(7, 5))

    prevalence_ref: float | None = None

    for entry in entries:
        df = _get_test(entry["df"])
        y_true = df["label"].to_numpy().astype(int)
        y_score = df["score"].to_numpy().astype(float)

        if np.sum(y_true) == 0:
            continue

        prec, rec, _ = precision_recall_curve(y_true, y_score)
        ap = average_precision_score(y_true, y_score)

        if prevalence_ref is None:
            prevalence_ref = float(np.mean(y_true))

        ax.plot(rec, prec, lw=1.8, label=f"{entry['label']} (AP={ap:.3f})")

    if prevalence_ref is not None:
        ax.axhline(
            prevalence_ref, color="gray", lw=1.2, ls="--",
            label=f"Azar (prevalencia={prevalence_ref:.4f})",
        )

    ax.set_xlabel("Recall")
    ax.set_ylabel("Precisión")
    ax.set_title("Curva Precisión–Recall")
    ax.legend(fontsize=8)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1.05)
    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# 2. Curva ROC
# ---------------------------------------------------------------------------

def plot_roc_curve(
    entries: list[PredEntry],
    run_id: str = "",
) -> plt.Figure:
    """Curva ROC con diagonal punteada de referencia (clasificador aleatorio)."""
    fig, ax = plt.subplots(figsize=(7, 5))

    for entry in entries:
        df = _get_test(entry["df"])
        y_true = df["label"].to_numpy().astype(int)
        y_score = df["score"].to_numpy().astype(float)

        if len(np.unique(y_true)) < 2:
            continue

        fpr, tpr, _ = roc_curve(y_true, y_score)
        from sklearn.metrics import roc_auc_score
        auc = roc_auc_score(y_true, y_score)
        ax.plot(fpr, tpr, lw=1.8, label=f"{entry['label']} (AUC={auc:.3f})")

    ax.plot([0, 1], [0, 1], color="gray", lw=1.2, ls="--", label="Azar")
    ax.set_xlabel("Tasa de Falsos Positivos")
    ax.set_ylabel("Tasa de Verdaderos Positivos")
    ax.set_title("Curva ROC")
    ax.legend(fontsize=8)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1.05)
    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# 3. Curva de captura acumulada (Gain Curve)
# ---------------------------------------------------------------------------

def plot_gain_curve(
    entries: list[PredEntry],
    run_id: str = "",
) -> plt.Figure:
    """
    % de la población revisada (eje X) vs % de SSCO capturados (eje Y).
    Es la visualización más directa para SUNAT: muestra cuánto ahorra el modelo
    frente a revisar aleatoriamente.
    """
    fig, ax = plt.subplots(figsize=(7, 5))

    for entry in entries:
        df = _get_test(entry["df"])
        df_sorted = df.sort_values("score", ascending=False).reset_index(drop=True)
        y_true = df_sorted["label"].to_numpy().astype(int)
        n = len(y_true)
        n_pos = int(np.sum(y_true))

        if n_pos == 0:
            continue

        cum_pos = np.cumsum(y_true)
        x = np.arange(1, n + 1) / n * 100
        y = cum_pos / n_pos * 100

        ax.plot(x, y, lw=1.8, label=entry["label"])

    # Línea de azar
    ax.plot([0, 100], [0, 100], color="gray", lw=1.2, ls="--", label="Azar")
    # Línea perfecta
    if entries:
        df0 = _get_test(entries[0]["df"])
        prev = float(df0["label"].mean())
        if prev > 0:
            ax.plot(
                [0, prev * 100, 100], [0, 100, 100],
                color="black", lw=0.8, ls=":", label="Perfecto",
            )

    ax.set_xlabel("% de la población revisada")
    ax.set_ylabel("% de SSCO capturados")
    ax.set_title("Curva de Captura Acumulada")
    ax.legend(fontsize=8)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 105)
    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# 4. Curva de Lift
# ---------------------------------------------------------------------------

def plot_lift_curve(
    entries: list[PredEntry],
    run_id: str = "",
) -> plt.Figure:
    """Lift = Precision@K / prevalencia, en función del % de la población revisada."""
    fig, ax = plt.subplots(figsize=(7, 5))

    for entry in entries:
        df = _get_test(entry["df"])
        df_sorted = df.sort_values("score", ascending=False).reset_index(drop=True)
        y_true = df_sorted["label"].to_numpy().astype(int)
        n = len(y_true)
        n_pos = int(np.sum(y_true))

        if n_pos == 0:
            continue

        prevalence = n_pos / n
        cum_pos = np.cumsum(y_true)
        k_vals = np.arange(1, n + 1)
        precision_k = cum_pos / k_vals
        lift_k = precision_k / prevalence

        x = k_vals / n * 100
        ax.plot(x, lift_k, lw=1.8, label=entry["label"])

    ax.axhline(1.0, color="gray", lw=1.2, ls="--", label="Azar (lift=1)")
    ax.set_xlabel("% de la población revisada")
    ax.set_ylabel("Lift")
    ax.set_title("Curva de Lift")
    ax.legend(fontsize=8)
    ax.set_xlim(0, 20)  # zoom en el rango operativo relevante
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# 5. Matrices de confusión al top-1% y top-5%
# ---------------------------------------------------------------------------

def plot_confusion_matrices(
    entry: PredEntry,
    run_id: str = "",
) -> plt.Figure:
    """
    Matrices de confusión al corte operativo top-1% y top-5% lado a lado.
    Acepta un solo entry (el modelo oficial).
    """
    df = _get_test(entry["df"])
    df_sorted = df.sort_values("score", ascending=False).reset_index(drop=True)
    y_true = df_sorted["label"].to_numpy().astype(int)
    n = len(y_true)

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    for ax, pct, label in zip(axes, [0.01, 0.05], ["Top-1%", "Top-5%"]):
        k = max(1, int(np.ceil(pct * n)))
        mask = np.zeros(n, dtype=int)
        mask[:k] = 1

        tp = int(np.sum((mask == 1) & (y_true == 1)))
        fp = int(np.sum((mask == 1) & (y_true == 0)))
        fn = int(np.sum((mask == 0) & (y_true == 1)))
        tn = int(np.sum((mask == 0) & (y_true == 0)))

        cm = np.array([[tp, fp], [fn, tn]])
        im = ax.imshow(cm, cmap="Blues")

        for i, row in enumerate(cm):
            for j, val in enumerate(row):
                ax.text(j, i, str(val), ha="center", va="center",
                        fontsize=13, color="black")

        ax.set_xticks([0, 1])
        ax.set_yticks([0, 1])
        ax.set_xticklabels(["Marcado", "No marcado"])
        ax.set_yticklabels(["SSCO real", "No SSCO"])
        ax.set_title(f"Matriz de Confusión {label}\n{entry['label']}")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# 6. Curva de calibración
# ---------------------------------------------------------------------------

def plot_calibration_curve(
    entries: list[PredEntry],
    run_id: str = "",
    n_bins: int = 10,
) -> plt.Figure:
    """
    Fracción real de positivos vs score medio por bin.
    Se espera mala calibración por los pesos de clase — se anota en el gráfico.
    """
    fig, ax = plt.subplots(figsize=(7, 5))

    for entry in entries:
        df = _get_test(entry["df"])
        y_true = df["label"].to_numpy().astype(int)
        y_score = df["score"].to_numpy().astype(float)

        if len(np.unique(y_true)) < 2:
            continue

        from sklearn.metrics import brier_score_loss
        brier = brier_score_loss(y_true, y_score)

        frac_pos, mean_pred = calibration_curve(
            y_true, y_score, n_bins=n_bins, strategy="uniform"
        )
        ax.plot(
            mean_pred, frac_pos, marker="o", lw=1.5,
            label=f"{entry['label']} (Brier={brier:.4f})",
        )

    ax.plot([0, 1], [0, 1], color="gray", lw=1.2, ls="--", label="Calibración perfecta")
    ax.set_xlabel("Score predicho (medio del bin)")
    ax.set_ylabel("Fracción real de positivos")
    ax.set_title("Curva de Calibración\n(scores inflados por pesos de clase — esperado)")
    ax.legend(fontsize=8)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# 7. Distribución de scores por clase
# ---------------------------------------------------------------------------

def plot_score_distribution(
    entry: PredEntry,
    run_id: str = "",
    bins: int = 40,
) -> plt.Figure:
    """
    Histograma superpuesto de scores para label=0 (negativos) y label=1 (SSCO).
    Acepta un solo entry (el modelo oficial).
    """
    df = _get_test(entry["df"])

    scores_neg = df.loc[df["label"] == 0, "score"].to_numpy()
    scores_pos = df.loc[df["label"] == 1, "score"].to_numpy()

    fig, ax = plt.subplots(figsize=(8, 4))

    ax.hist(scores_neg, bins=bins, alpha=0.6, color="steelblue",
            label=f"No SSCO (n={len(scores_neg):,})", density=True)
    ax.hist(scores_pos, bins=bins, alpha=0.7, color="tomato",
            label=f"SSCO (n={len(scores_pos):,})", density=True)

    ax.set_xlabel("Score de riesgo")
    ax.set_ylabel("Densidad")
    ax.set_title(f"Distribución de Scores por Clase — {entry['label']}")
    ax.legend(fontsize=9)
    fig.tight_layout()
    _add_footer(fig, run_id)
    return fig


# ---------------------------------------------------------------------------
# save_all — genera y exporta los 7 gráficos
# ---------------------------------------------------------------------------

def save_all(
    entries: list[PredEntry],
    output_dir: str,
    run_id: str = "",
    champion_label: str | None = None,
    dpi: int = 150,
) -> list[str]:
    """
    Genera y guarda los 7 gráficos en `output_dir` como PNG de alta resolución.

    Parámetros:
    -----------
    entries: list[PredEntry]
        Lista de modelos/baselines a comparar. Todos los gráficos comparativos
        (PR, ROC, gain, lift, calibración) usan la lista completa.
        Los gráficos individuales (matrices de confusión, score distribution)
        usan el entry cuyo label coincide con `champion_label`, o el primero.
    output_dir: str
        Directorio de destino. Se crea si no existe.
    run_id: str
        Aparece en el pie de cada figura.
    champion_label: str | None
        Label del modelo oficial para los gráficos individuales.
    dpi: int
        Resolución de los PNG (por defecto 150).

    Retorna la lista de rutas de archivos creados.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Seleccionar entry campeón para gráficos individuales
    champion = entries[0]
    if champion_label:
        for e in entries:
            if e["label"] == champion_label:
                champion = e
                break

    plots: list[tuple[str, plt.Figure]] = [
        ("pr_curve.png",           plot_pr_curve(entries, run_id=run_id)),
        ("roc_curve.png",          plot_roc_curve(entries, run_id=run_id)),
        ("gain_curve.png",         plot_gain_curve(entries, run_id=run_id)),
        ("lift_curve.png",         plot_lift_curve(entries, run_id=run_id)),
        ("confusion_matrices.png", plot_confusion_matrices(champion, run_id=run_id)),
        ("calibration_curve.png",  plot_calibration_curve(entries, run_id=run_id)),
        ("score_distribution.png", plot_score_distribution(champion, run_id=run_id)),
    ]

    saved: list[str] = []
    for filename, fig in plots:
        path = os.path.join(output_dir, filename)
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        saved.append(path)

    return saved
