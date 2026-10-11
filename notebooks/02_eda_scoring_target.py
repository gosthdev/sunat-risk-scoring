# %% [markdown]
# # 02 — EDA: Target SSCO y Correlación
#
# Análisis de los factores determinantes para el modelado de riesgo (Fase 4.2):
# 1. Severidad del desbalance de clases de `es_ssco`.
# 2. Correlación entre condición de domicilio fiscal y tasa de evasión/SSCO.
# 3. Colinealidad de features numéricas antes del entrenamiento.

# %%
import os
import warnings

warnings.filterwarnings("ignore")

import awswrangler as wr
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams.update({"figure.figsize": (8, 5), "figure.dpi": 100})
sns.set_style("whitegrid")

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "sunat-risk-scoring-gold")


def latest_partition(base_path):
    dirs = wr.s3.list_directories(base_path)
    return max(dirs) if dirs else base_path


path = latest_partition(f"s3://{GOLD_BUCKET}/scoring_dataset/")
mes = path.rstrip("/").split("=")[-1]
print(f"Partición analizada: mes_referencia={mes}")
df = wr.s3.read_parquet(path)
total = len(df)
print(f"Total registros: {total:,}")

# %% [markdown]
# ## 1. ¿Cuál es el balance de clases del target `es_ssco`?
#
# **Pregunta:** ¿Qué tan severo es el desbalance de clases y qué métricas de evaluación
# condiciona de forma obligatoria para la Fase 4.2?

# %%
counts = df["es_ssco"].value_counts()
n_false = counts.get(False, 0)
n_true = counts.get(True, 0)

fig, ax = plt.subplots(figsize=(6, 4))
bars = ax.bar(["No SSCO (0)", "SSCO (1)"], [n_false, n_true], color=["#2ecc71", "#e74c3c"])
for bar, val in zip(bars, [n_false, n_true]):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() * 1.1,
        f"{val:,}",
        ha="center",
        va="bottom",
        fontweight="bold",
        fontsize=10,
    )
ax.set_ylabel("Cantidad de contribuyentes (escala log)")
ax.set_title("Balance de clases: es_ssco (Target)")
ax.set_yscale("log")
plt.tight_layout()
plt.show()

ratio = n_false / n_true if n_true > 0 else float("inf")
print(f"No SSCO: {n_false:,} | SSCO: {n_true:,}")
print(f"Ratio negativo/positivo: {ratio:,.0f} a 1")
print(f"Prevalencia de fraude SSCO: {n_true / total * 100:.6f}%")

# %% [markdown]
# **Interpretación del hallazgo:**
# El desbalance es extremo: ~764 positivos frente a millones de negativos (prevalencia < 0.001%).
#
# **Decisiones metodológicas para 4.2:**
# - **Accuracy queda descartada:** predecir siempre `False` daría >99.99 % de exactitud sin detectar fraude.
# - **Métricas requeridas:** se debe optimizar por **AUC-PR (Precision-Recall)**, F1-Score y Recall@K.
# - **Estrategia de modelado:** entrenamiento con `class_weight='balanced'` o submuestreo estratificado.

# %% [markdown]
# ## 2. ¿La condición de domicilio fiscal discrimina el riesgo SSCO?
#
# **Pregunta:** ¿Los contribuyentes con domicilio no habido o no hallado presentan
# una probabilidad significativamente superior de ser empresas fachada (SSCO)?

# %%
tasa_cond = df.groupby("Condicion")["es_ssco"].agg(["sum", "count"])
tasa_cond["tasa_pct"] = (tasa_cond["sum"] / tasa_cond["count"] * 100).round(4)
tasa_cond = tasa_cond.sort_values("tasa_pct", ascending=True)

fig, ax = plt.subplots(figsize=(8, 4))
ax.barh(
    tasa_cond.index,
    tasa_cond["tasa_pct"],
    color=sns.color_palette("mako", len(tasa_cond)),
)
ax.set_xlabel("Tasa de incidencia SSCO (%)")
ax.set_title("Incidencia de SSCO por Condición de Domicilio Fiscal")
for i, (idx, row) in enumerate(tasa_cond.iterrows()):
    ax.text(
        row["tasa_pct"] + 0.0001,
        i,
        f"  {row['tasa_pct']:.4f}% ({int(row['sum']):,} SSCO)",
        va="center",
        fontsize=9,
        fontweight="bold",
    )
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación del hallazgo:**
# Los contribuyentes en condición **NO HABIDO** presentan una tasa relativa de SSCO
# sustancialmente superior a los contribuyentes en condición HABIDO.
# Esto valida la hipótesis de fiscalización: los sujetos sin capacidad operativa real
# abandonan o declaran domicilios inexistentes para evadir notificaciones de SUNAT.
# `Condicion` es una variable predictiva crítica para el scoring.

# %% [markdown]
# ## 3. ¿Existe colinealidad entre features numéricas antes del modelado?
#
# **Pregunta:** ¿Existen variables redundantes que deban descartarse o regularizarse
# para evitar multicolinealidad en el modelo de scoring?

# %%
num_cols = [
    "monto_total_contratado_estado",
    "cantidad_contratos_estado",
    "antiguedad_contratacion_estado_dias",
]

df_corr = df[num_cols + ["es_prico", "es_ssco"]].copy()
df_corr["es_prico"] = df_corr["es_prico"].astype(int)
df_corr["es_ssco"] = df_corr["es_ssco"].astype(int)

corr = df_corr.corr()

fig, ax = plt.subplots(figsize=(7, 5))
sns.heatmap(
    corr,
    annot=True,
    cmap="RdBu_r",
    center=0,
    fmt=".2f",
    square=True,
    ax=ax,
    linewidths=0.5,
)
ax.set_title("Matriz de correlación de Pearson — Features y Target")
plt.tight_layout()
plt.show()

# Verificación de colinealidad crítica (>0.8)
for i in range(len(corr.columns)):
    for j in range(i + 1, len(corr.columns)):
        r = corr.iloc[i, j]
        if abs(r) >= 0.7:
            print(f"⚠ Alerta de colinealidad alta: {corr.columns[i]} ↔ {corr.columns[j]} (r = {r:.2f})")

# %% [markdown]
# **Interpretación del hallazgo:**
# `cantidad_contratos_estado` y `monto_total_contratado_estado` presentan una alta correlación
# positiva. Esto indica redundancia de información entre ambas métricas de contratación.
#
# **Decisión metodológica para 4.2:**
# En modelos lineales o logísticos se recomienda regularización (L1/L2 - Lasso/Ridge) o
# seleccionar solo el monto transformado en logaritmo para evitar inflación de varianza.
