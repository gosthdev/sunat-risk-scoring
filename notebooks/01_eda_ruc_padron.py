# %% [markdown]
# # 01 — EDA: Contratación con el Estado
#
# Análisis univariado sobre `gold/ruc_features`: distribución y magnitud
# económica de las compras estatales (escala log-log).
# Se descartan gráficos univariados triviales (como gráficos de torta de PRICO
# o estados del RUC) que no sostienen decisiones de modelado.

# %%
import os
import warnings

warnings.filterwarnings("ignore")

import awswrangler as wr
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams.update({"figure.figsize": (9, 5), "figure.dpi": 100})
sns.set_style("whitegrid")

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "sunat-risk-scoring-gold")


def latest_partition(base_path):
    dirs = wr.s3.list_directories(base_path)
    return max(dirs) if dirs else base_path


path = latest_partition(f"s3://{GOLD_BUCKET}/ruc_features/")
mes = path.rstrip("/").split("=")[-1]
print(f"Partición analizada: mes_referencia={mes}")
df = wr.s3.read_parquet(path)
total = len(df)
print(f"Total registros: {total:,}")

# %% [markdown]
# ## 1. ¿Cómo se distribuyen los montos contratados con el Estado?
#
# **Pregunta:** ¿Siguen los montos de contratación una distribución normal o presentan
# una asimetría extrema que condicione su preprocesamiento?
#
# Histograma con ejes log-log para evaluar la ley de potencia (solo RUCs con monto > 0).

# %%
montos = df.loc[
    df["monto_total_contratado_estado"] > 0, "monto_total_contratado_estado"
]

fig, ax = plt.subplots(figsize=(9, 5))
ax.hist(montos, bins=60, color="#2ecc71", edgecolor="white")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Monto total contratado (S/.) — escala logarítmica")
ax.set_ylabel("Frecuencia de contribuyentes — escala logarítmica")
ax.set_title("Distribución del monto total contratado con el Estado (RUCs con contratos > 0)")
plt.tight_layout()
plt.show()

print(
    f"RUCs con contrataciones vigentes: {len(montos):,} de {total:,} ({len(montos) / total * 100:.2f}%)"
)
print("Estadísticas descriptivas de montos (S/.):")
print(montos.describe().round(2).to_string())

# %% [markdown]
# **Interpretación del hallazgo:**
# La distribución es marcadamente asimétrica a la derecha (*heavy-tailed*, ley de potencia).
# El 96.23 % de los RUCs nunca ha contratado con el Estado. En el 3.77 % que sí contrata,
# la mediana es órdenes de magnitud menor a la media, con una concentración de montos
# multimillonarios en una élite muy pequeña de proveedores.
#
# **Decisión metodológica para 4.2:**
# Esta variable no puede ingresarse de manera lineal. Requiere transformación logarítmica
# `log1p(monto_total_contratado_estado)` o escalamiento robusto para no distorsionar
# los algoritmos de clasificación.
