# %% [markdown]
# # 01 — EDA: Padrón RUC y Features
#
# Análisis univariado sobre `gold/ruc_features`: distribución de
# Estado/Condición del contribuyente, montos y frecuencia de contratación
# con el Estado (escala log), antigüedad de contratación y proporción de
# Principales Contribuyentes (PRICOS).

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
    return max(dirs) if dirs else base_path


path = latest_partition(f"s3://{GOLD_BUCKET}/ruc_features/")
mes = path.rstrip("/").split("=")[-1]
print(f"Partición: mes_referencia={mes}")
df = wr.s3.read_parquet(path)
total = len(df)
print(f"Registros: {total:,}")

# %% [markdown]
# ## 1. ¿Cómo se distribuye el estado del contribuyente?
#
# Conteo y porcentaje por categoría de Estado en el Padrón RUC.

# %%
estado = df["Estado"].value_counts()

fig, ax = plt.subplots(figsize=(8, 4))
bars = ax.barh(
    estado.index, estado.values, color=sns.color_palette("viridis", len(estado))
)
for bar, val in zip(bars, estado.values):
    ax.text(
        bar.get_width(),
        bar.get_y() + bar.get_height() / 2,
        f"  {val:,} ({val / total * 100:.1f}%)",
        va="center",
        fontsize=9,
    )
ax.set_xlabel("Cantidad de RUCs")
ax.set_title("Distribución por Estado del Contribuyente")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** La mayoría de contribuyentes se encuentra en estado
# ACTIVO. Los estados BAJA DE OFICIO y SUSPENSION TEMPORAL, aunque
# minoritarios, serán relevantes para el análisis del target SSCO en el
# notebook 02, ya que se espera que concentren una tasa de SSCO
# desproporcionadamente alta.

# %% [markdown]
# ## 2. ¿Cómo se distribuye la condición del domicilio?
#
# Conteo y porcentaje por categoría de Condición.

# %%
cond = df["Condicion"].value_counts()

fig, ax = plt.subplots(figsize=(8, 4))
bars = ax.barh(cond.index, cond.values, color=sns.color_palette("magma", len(cond)))
for bar, val in zip(bars, cond.values):
    ax.text(
        bar.get_width(),
        bar.get_y() + bar.get_height() / 2,
        f"  {val:,} ({val / total * 100:.1f}%)",
        va="center",
        fontsize=9,
    )
ax.set_xlabel("Cantidad de RUCs")
ax.set_title("Distribución por Condición de Domicilio")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** HABIDO domina ampliamente. La proporción de NO HABIDO
# y NO HALLADO es baja en términos absolutos pero relevante para scoring:
# la condición de domicilio irregular suele asociarse con contribuyentes
# problemáticos.

# %% [markdown]
# ## 3. ¿Cómo se distribuyen los montos contratados con el Estado?
#
# Histograma con ejes log-log para manejar el sesgo extremo a la derecha.
# Solo se grafican RUCs con monto > 0.

# %%
montos = df.loc[
    df["monto_total_contratado_estado"] > 0, "monto_total_contratado_estado"
]

fig, ax = plt.subplots()
ax.hist(montos, bins=60, color="#2ecc71", edgecolor="white")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Monto total contratado (S/.) — escala log")
ax.set_ylabel("Frecuencia (escala log)")
ax.set_title(
    "Distribución del monto total contratado con el Estado (RUCs con monto > 0)"
)
plt.tight_layout()
plt.show()

print(
    f"RUCs con monto > 0: {len(montos):,} de {total:,} ({len(montos) / total * 100:.2f}%)"
)
print(montos.describe().round(2).to_string())

# %% [markdown]
# **Interpretación:** La distribución es extremadamente sesgada a la derecha
# (heavy-tailed): la mayoría contrata por montos bajos y un grupo reducido
# concentra valores muy superiores. El eje log-log revela esta estructura
# de ley de potencia. La mediana es significativamente menor que la media,
# confirmando el sesgo.

# %% [markdown]
# ## 4. ¿Cómo se distribuye la frecuencia de contratación?
#
# Histograma con ejes log-log. Solo RUCs con contratos > 0.

# %%
contratos = df.loc[df["cantidad_contratos_estado"] > 0, "cantidad_contratos_estado"]

fig, ax = plt.subplots()
ax.hist(contratos, bins=60, color="#3498db", edgecolor="white")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Cantidad de contratos — escala log")
ax.set_ylabel("Frecuencia (escala log)")
ax.set_title(
    "Distribución de la cantidad de contratos con el Estado (RUCs con contratos > 0)"
)
plt.tight_layout()
plt.show()

print(
    f"RUCs con contratos > 0: {len(contratos):,} de {total:,} ({len(contratos) / total * 100:.2f}%)"
)
print(contratos.describe().round(2).to_string())

# %% [markdown]
# **Interpretación:** Patrón idéntico al de montos: distribución heavy-tailed.
# La mayoría de RUCs que contratan con el Estado lo hacen con pocas órdenes;
# un grupo reducido acumula cientos o miles de contratos. Ambas variables
# (monto y cantidad) probablemente correlacionan alto, lo que se verificará
# en la matriz de correlación del notebook 02.

# %% [markdown]
# ## 5. ¿Cuál es la antigüedad de contratación con el Estado?
#
# La variable `antiguedad_contratacion_estado_dias` mide días desde la
# primera orden de compra hasta el fin del `mes_referencia` (no usa
# `current_date()` — es determinista por diseño del pipeline). Solo
# aplica a RUCs con al menos una orden de compra.

# %%
antig = df["antiguedad_contratacion_estado_dias"].dropna()

fig, ax = plt.subplots()
ax.hist(antig, bins=50, color="#9b59b6", edgecolor="white")
ax.set_xlabel("Días desde primera orden de compra hasta mes de referencia")
ax.set_ylabel("Frecuencia")
ax.set_title("Distribución de antigüedad de contratación con el Estado")
plt.tight_layout()
plt.show()

print(
    f"RUCs con antigüedad calculable: {len(antig):,} de {total:,} ({len(antig) / total * 100:.2f}%)"
)
print(antig.describe().round(0).to_string())

# %% [markdown]
# **Interpretación:** La antigüedad solo se calcula para RUCs con al menos
# una orden de compra anterior al mes de referencia. No es la antigüedad
# real del RUC (el Padrón no trae fecha de inscripción), sino un proxy de
# cuánto tiempo llevan contratando con el Estado. La distribución
# probablemente muestra una concentración en valores recientes (contratos
# de los últimos años) con una cola larga hacia atrás.

# %% [markdown]
# ## 6. ¿Qué proporción de RUCs son Principales Contribuyentes?

# %%
prico = df["es_prico"].value_counts()

fig, ax = plt.subplots(figsize=(5, 5))
ax.pie(
    prico.values,
    labels=["No PRICO", "PRICO"],
    autopct="%1.2f%%",
    colors=["#95a5a6", "#e74c3c"],
    startangle=90,
)
ax.set_title("Proporción de Principales Contribuyentes (PRICOS)")
plt.tight_layout()
plt.show()

print(
    f"PRICOS: {prico.get(True, 0):,} de {total:,} ({prico.get(True, 0) / total * 100:.4f}%)"
)

# %% [markdown]
# **Interpretación:** Los PRICOS representan una fracción mínima del total
# de contribuyentes (< 1 %), pero concentran la mayor parte de la
# recaudación tributaria. Esta variable será útil como feature en el
# modelo de scoring porque se espera que casi ningún PRICO sea SSCO
# (verificado en el notebook 02).
