# %% [markdown]
# # 02 — EDA: Target SSCO y Correlación
#
# Análisis del target `es_ssco` (Sujetos sin Capacidad Operativa),
# comparación entre clases SSCO vs no-SSCO y matriz de correlación
# entre features numéricas. Este es el notebook más importante del
# EDA porque las decisiones de modelado (4.2) dependen directamente
# de estos hallazgos.

# %%
import os
import warnings

warnings.filterwarnings("ignore")

import awswrangler as wr
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams.update({"figure.figsize": (10, 6), "figure.dpi": 100})
sns.set_style("whitegrid")

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "sunat-risk-scoring-gold")


def latest_partition(base_path):
    dirs = wr.s3.list_directories(base_path)
    return sorted(dirs)[-1] if dirs else base_path


path = latest_partition(f"s3://{GOLD_BUCKET}/scoring_dataset/")
mes = path.rstrip("/").split("=")[-1]
print(f"Partición: mes_referencia={mes}")
df = wr.s3.read_parquet(path)
total = len(df)
print(f"Registros: {total:,}")

# %% [markdown]
# ## 1. ¿Cuál es el balance de clases del target `es_ssco`?
#
# Esta es la pregunta más crítica: un desbalance extremo condiciona
# qué modelo usar y qué métrica es válida para evaluarlo.

# %%
counts = df["es_ssco"].value_counts()
n_false = counts.get(False, 0)
n_true = counts.get(True, 0)

fig, ax = plt.subplots(figsize=(6, 4))
bars = ax.bar(["No SSCO", "SSCO"], [n_false, n_true], color=["#2ecc71", "#e74c3c"])
for bar, val in zip(bars, [n_false, n_true]):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height(),
        f"{val:,}",
        ha="center",
        va="bottom",
        fontweight="bold",
    )
ax.set_ylabel("Cantidad de RUCs (escala log)")
ax.set_title("Balance de clases: es_ssco")
ax.set_yscale("log")
plt.tight_layout()
plt.show()

ratio = n_false / n_true if n_true > 0 else float("inf")
print(f"No SSCO: {n_false:,}  |  SSCO: {n_true:,}")
print(f"Ratio negativo/positivo: {ratio:,.0f}:1")
print(f"Prevalencia SSCO: {n_true / total * 100:.4f}%")

# %% [markdown]
# **Interpretación:** El desbalance es extremo: aproximadamente 766
# positivos (SSCO) frente a millones de negativos. Consecuencias
# directas para el modelado en 4.2:
#
# - **Accuracy es engañosa:** predecir siempre False da > 99.9 % de
#   accuracy, pero no detecta ningún SSCO.
# - **Métricas válidas:** usar AUC-PR (área bajo la curva
#   precisión-recall), F1 o recall@k.
# - **Técnicas de balance:** considerar SMOTE, undersampling
#   estratificado o `class_weight='balanced'` en el modelo.

# %% [markdown]
# ## 2. ¿Se concentra SSCO en ciertos estados del contribuyente?
#
# Hipótesis: la tasa de SSCO es mayor en estados como SUSPENSION
# TEMPORAL y BAJA DE OFICIO.

# %%
tasa_estado = df.groupby("Estado")["es_ssco"].agg(["sum", "count"])
tasa_estado["tasa_pct"] = (tasa_estado["sum"] / tasa_estado["count"] * 100).round(4)
tasa_estado = tasa_estado.sort_values("tasa_pct", ascending=True)

fig, ax = plt.subplots(figsize=(8, 4))
ax.barh(
    tasa_estado.index,
    tasa_estado["tasa_pct"],
    color=sns.color_palette("rocket", len(tasa_estado)),
)
ax.set_xlabel("Tasa de SSCO (%)")
ax.set_title("Tasa de SSCO por Estado del Contribuyente")
for i, (idx, row) in enumerate(tasa_estado.iterrows()):
    ax.text(
        row["tasa_pct"],
        i,
        f"  {row['tasa_pct']:.4f}% ({int(row['sum'])})",
        va="center",
        fontsize=9,
    )
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** Si la hipótesis se confirma, la tasa de SSCO es
# sensiblemente mayor en estados como SUSPENSION TEMPORAL que en ACTIVO.
# El Estado del contribuyente es un feature discriminante para el scoring.

# %% [markdown]
# ## 3. ¿Se concentra SSCO en ciertas condiciones de domicilio?
#
# Hipótesis: la tasa de SSCO es mayor en NO HABIDO y NO HALLADO.

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
ax.set_xlabel("Tasa de SSCO (%)")
ax.set_title("Tasa de SSCO por Condición de Domicilio")
for i, (idx, row) in enumerate(tasa_cond.iterrows()):
    ax.text(
        row["tasa_pct"],
        i,
        f"  {row['tasa_pct']:.4f}% ({int(row['sum'])})",
        va="center",
        fontsize=9,
    )
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** Contribuyentes con condición NO HABIDO (domicilio
# no verificable) presentan tasas de SSCO superiores. Esto es coherente:
# un sujeto sin capacidad operativa real difícilmente mantiene un
# domicilio fiscal localizable.

# %% [markdown]
# ## 4. ¿Ser PRICO protege contra la designación SSCO?
#
# Hipótesis: prácticamente ningún Principal Contribuyente es SSCO,
# dado que tienen supervisión reforzada por SUNAT.

# %%
tasa_prico = df.groupby("es_prico")["es_ssco"].agg(["sum", "count"])
tasa_prico["tasa_pct"] = (tasa_prico["sum"] / tasa_prico["count"] * 100).round(4)
labels = {True: "PRICO", False: "No PRICO"}
tasa_prico.index = [labels.get(i, str(i)) for i in tasa_prico.index]

fig, ax = plt.subplots(figsize=(5, 4))
ax.bar(tasa_prico.index, tasa_prico["tasa_pct"], color=["#3498db", "#e74c3c"])
for i, (idx, row) in enumerate(tasa_prico.iterrows()):
    ax.text(
        i,
        row["tasa_pct"],
        f"{row['tasa_pct']:.4f}%\n({int(row['sum'])})",
        ha="center",
        va="bottom",
        fontsize=10,
    )
ax.set_ylabel("Tasa de SSCO (%)")
ax.set_title("Tasa de SSCO: PRICO vs No PRICO")
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** Se espera que la tasa de SSCO entre PRICOS sea
# prácticamente cero. Los Principales Contribuyentes están bajo
# supervisión reforzada de SUNAT, lo que hace casi imposible que
# operen sin capacidad operativa real. `es_prico` es un feature
# altamente predictivo (pero con baja prevalencia).

# %% [markdown]
# ## 5. ¿Difiere la contratación estatal entre SSCO y no-SSCO?
#
# Comparación de la distribución de cantidad de contratos y monto
# contratado entre ambas clases, solo para RUCs con contratos > 0.

# %%
mask = df["cantidad_contratos_estado"] > 0
df_c = df.loc[mask].copy()
df_c["label"] = df_c["es_ssco"].apply(lambda x: "SSCO" if x else "No SSCO")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

sns.boxplot(
    data=df_c,
    x="label",
    y="cantidad_contratos_estado",
    ax=axes[0],
    palette={"No SSCO": "#2ecc71", "SSCO": "#e74c3c"},
    showfliers=False,
)
axes[0].set_title("Cantidad de contratos (sin outliers)")
axes[0].set_yscale("log")
axes[0].set_xlabel("")

sns.boxplot(
    data=df_c,
    x="label",
    y="monto_total_contratado_estado",
    ax=axes[1],
    palette={"No SSCO": "#2ecc71", "SSCO": "#e74c3c"},
    showfliers=False,
)
axes[1].set_title("Monto total contratado (sin outliers)")
axes[1].set_yscale("log")
axes[1].set_xlabel("")

plt.suptitle("Contratación con el Estado: SSCO vs No SSCO", fontsize=13, y=1.02)
plt.tight_layout()
plt.show()

print("Cantidad de contratos (percentiles):")
print(
    df_c.groupby("label")["cantidad_contratos_estado"].describe().round(2).to_string()
)
print()
print("Monto total contratado (percentiles):")
print(
    df_c.groupby("label")["monto_total_contratado_estado"]
    .describe()
    .round(2)
    .to_string()
)

# %% [markdown]
# **Interpretación:** Si los SSCO muestran medianas similares o menores
# que los no-SSCO, la contratación estatal por sí sola no discrimina
# bien entre clases. Esto refuerza la necesidad de combinar múltiples
# features (Estado, Condición, es_prico, monto, cantidad, antigüedad)
# en un modelo de scoring en lugar de usar reglas univariadas.

# %% [markdown]
# ## 6. ¿Existen correlaciones fuertes entre las features numéricas?
#
# Matriz de correlación para detectar colinealidad antes del
# modelo de scoring (4.2). Si dos features correlacionan > 0.9,
# considerar eliminar una.

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
ax.set_title("Matriz de correlación — Features numéricas + target")
plt.tight_layout()
plt.show()

# Detectar pares con correlación alta
for i in range(len(corr.columns)):
    for j in range(i + 1, len(corr.columns)):
        r = corr.iloc[i, j]
        if abs(r) > 0.7:
            print(
                f"⚠ Correlación alta: {corr.columns[i]} ↔ {corr.columns[j]} = {r:.2f}"
            )

# %% [markdown]
# **Interpretación:** `monto_total_contratado_estado` y
# `cantidad_contratos_estado` probablemente correlacionan alto (más
# contratos → más monto acumulado). Si la correlación supera 0.9,
# considerar eliminar una de las dos para evitar colinealidad en el
# modelo de scoring. Las correlaciones con `es_ssco` serán bajas
# individualmente (dado el desbalance extremo), pero el modelo
# combinará múltiples señales débiles.
