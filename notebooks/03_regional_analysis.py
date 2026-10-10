# %% [markdown]
# # 03 — Análisis Regional (Objetivo 3)
#
# Análisis sobre `gold/regional_summary`: relación entre formalización
# empresarial e informalidad laboral, concentración de PRICOS y
# recaudación tributaria por departamento. Contraste Cajamarca vs Lima.

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

df = wr.s3.read_parquet(f"s3://{GOLD_BUCKET}/regional_summary/", dataset=True)
df = df.dropna(subset=["Departamento"])
print(f"Departamentos: {len(df)}")
df.head()

# %% [markdown]
# ## 1. ¿Existe relación entre actividad empresarial e informalidad laboral?
#
# Scatter: RUC activos vs % de informalidad laboral (EPEN) por
# departamento. Cajamarca y Lima se resaltan como polos opuestos.

# %%
fig, ax = plt.subplots(figsize=(10, 7))
ax.scatter(
    df["pct_informalidad"],
    df["ruc_activos"],
    s=100,
    alpha=0.7,
    c="#3498db",
    edgecolors="white",
    linewidth=0.5,
    zorder=5,
)

for _, row in df.iterrows():
    dep = row["Departamento"]
    bold = dep in ("LIMA", "CAJAMARCA")
    ax.annotate(
        dep,
        (row["pct_informalidad"], row["ruc_activos"]),
        fontsize=7 if not bold else 9,
        fontweight="bold" if bold else "normal",
        alpha=0.9,
        ha="center",
        va="bottom",
        xytext=(0, 5),
        textcoords="offset points",
    )

ax.set_xlabel("% Informalidad laboral (EPEN)")
ax.set_ylabel("RUC activos")
ax.set_title("RUC activos vs. Informalidad laboral por departamento")
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** Lima concentra la mayor cantidad de RUC activos con
# informalidad laboral relativamente baja. En el extremo opuesto,
# Cajamarca muestra alta informalidad con baja densidad empresarial.
# La relación inversa entre formalización empresarial y tasas de
# informalidad laboral es clara: donde hay menos empresas formales,
# el empleo informal domina.

# %% [markdown]
# ## 2. ¿Se concentran los PRICOS en las regiones de mayor recaudación?
#
# Gráfico de barras horizontales con doble eje: recaudación en barras y
# PRICOS como puntos superpuestos.

# %%
df_sorted = df.sort_values("recaudacion_soles", ascending=True)

fig, ax1 = plt.subplots(figsize=(12, 6))
y_pos = range(len(df_sorted))
ax1.barh(
    list(y_pos),
    df_sorted["recaudacion_soles"] / 1e6,
    color="#3498db",
    alpha=0.7,
    label="Recaudación (MM S/.)",
)
ax1.set_yticks(list(y_pos))
ax1.set_yticklabels(df_sorted["Departamento"], fontsize=8)
ax1.set_xlabel("Recaudación (millones S/.)")

ax2 = ax1.twiny()
ax2.scatter(
    df_sorted["concentracion_pricos"].values,
    list(y_pos),
    color="#e74c3c",
    s=60,
    zorder=5,
    label="PRICOS",
)
ax2.set_xlabel("Cantidad de PRICOS", color="#e74c3c")

ax1.set_title(
    "Recaudación tributaria y concentración de PRICOS por departamento", pad=30
)
ax1.legend(loc="lower right")
ax2.legend(loc="center right")
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** Recaudación y concentración de PRICOS siguen un
# patrón casi idéntico, ambas fuertemente concentradas en Lima. La
# presencia de Principales Contribuyentes es un predictor directo de
# recaudación regional: donde están los PRICOS, está el grueso de la
# recaudación tributaria del país.

# %% [markdown]
# ## 3. Tabla resumen: 24 departamentos con las 4 métricas
#
# Vista consolidada de las métricas regionales ordenadas por
# cantidad de RUC activos (descendente).

# %%
tabla = df[
    [
        "Departamento",
        "ruc_activos",
        "pct_informalidad",
        "recaudacion_soles",
        "concentracion_pricos",
    ]
].copy()
tabla["recaudacion_MM"] = (tabla["recaudacion_soles"] / 1e6).round(2)
tabla["pct_informalidad"] = tabla["pct_informalidad"].round(2)
tabla = tabla.drop(columns=["recaudacion_soles"])
tabla = tabla.sort_values("ruc_activos", ascending=False).reset_index(drop=True)
tabla.index += 1
tabla

# %% [markdown]
# **Interpretación:** Lima domina en RUC activos, recaudación y PRICOS.
# Departamentos con alta informalidad y baja densidad de RUC activos
# (Cajamarca, Huancavelica, Ayacucho) representan oportunidades de
# focalización para políticas de formalización tributaria.
