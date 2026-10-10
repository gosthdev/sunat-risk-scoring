# %% [markdown]
# # 00 — Control de Calidad de Datos
#
# Verificación previa al EDA: nulos por columna, registros rechazados
# en cuarentena y validación de la normalización de departamentos.
# Esta sección se ejecuta **antes** de cualquier gráfico exploratorio
# para confirmar que los datos limpios (gold) están completos y
# consistentes.

# %%
import os
import warnings

warnings.filterwarnings("ignore")

import pandas as pd
import awswrangler as wr
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams.update({"figure.figsize": (10, 6), "figure.dpi": 100})
sns.set_style("whitegrid")

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "sunat-risk-scoring-gold")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "sunat-risk-scoring-silver")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "sunat-risk-scoring-bronze")


def latest_partition(base_path):
    """Devuelve la última partición mes_referencia disponible."""
    dirs = wr.s3.list_directories(base_path)
    return sorted(dirs)[-1] if dirs else base_path


def contar(path):
    """Conteo rápido de filas sin leer columnas."""
    try:
        return len(wr.s3.read_parquet(path, dataset=True, columns=[]))
    except Exception:
        return 0

# %% [markdown]
# ## 1. Nulos por columna — `gold/ruc_features`
#
# **Pregunta:** ¿Qué columnas presentan datos faltantes y en qué proporción?

# %%
path_feat = latest_partition(f"s3://{GOLD_BUCKET}/ruc_features/")
mes = path_feat.rstrip("/").split("=")[-1]
print(f"Partición: mes_referencia={mes}")

df_feat = wr.s3.read_parquet(path_feat)
total_feat = len(df_feat)
nulos_feat = df_feat.isnull().sum()
pct_feat = (nulos_feat / total_feat * 100).round(2)

resumen_feat = pd.DataFrame(
    {"nulos": nulos_feat, "pct_nulos": pct_feat, "no_nulos": total_feat - nulos_feat}
).sort_values("pct_nulos", ascending=False)

fig, ax = plt.subplots(figsize=(10, 5))
colors = ["#e74c3c" if v > 0 else "#2ecc71" for v in resumen_feat["pct_nulos"]]
ax.barh(resumen_feat.index, resumen_feat["pct_nulos"], color=colors)
for i, (idx, row) in enumerate(resumen_feat.iterrows()):
    ax.text(row["pct_nulos"] + 0.3, i, f"{row['pct_nulos']:.2f}%", va="center", fontsize=9)
ax.set_xlabel("% de nulos")
ax.set_title("Porcentaje de nulos por columna — gold/ruc_features")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 2. Nulos por columna — `gold/scoring_dataset`

# %%
try:
    path_sd = latest_partition(f"s3://{GOLD_BUCKET}/scoring_dataset/")
    df_sd = wr.s3.read_parquet(path_sd)
    total_sd = len(df_sd)
    nulos_sd = df_sd.isnull().sum()
    pct_sd = (nulos_sd / total_sd * 100).round(2)

    resumen_sd = pd.DataFrame(
        {"nulos": nulos_sd, "pct_nulos": pct_sd, "no_nulos": total_sd - nulos_sd}
    ).sort_values("pct_nulos", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = ["#e74c3c" if v > 0 else "#2ecc71" for v in resumen_sd["pct_nulos"]]
    ax.barh(resumen_sd.index, resumen_sd["pct_nulos"], color=colors)
    for i, (idx, row) in enumerate(resumen_sd.iterrows()):
        ax.text(row["pct_nulos"] + 0.3, i, f"{row['pct_nulos']:.2f}%", va="center", fontsize=9)
    ax.set_xlabel("% de nulos")
    ax.set_title("Porcentaje de nulos por columna — gold/scoring_dataset")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.show()
except Exception as e:
    print(f"Error leyendo scoring_dataset: {e} (probablemente no existe aún)")

# %% [markdown]
# **Interpretación:** Los campos derivados del cruce con Órdenes de Compra
# (`antiguedad_contratacion_estado_dias`) presentan nulos elevados porque
# la mayoría de RUC nunca ha contratado con el Estado. Los campos del
# Padrón RUC (`RUC`, `Estado`, `Condicion`) tienen completitud al 100 %.
# Dado que `monto_total_contratado_estado` y `cantidad_contratos_estado`
# se rellenan con 0 en la capa gold, sus nulos son 0 %.

# %% [markdown]
# ## 3. Registros en cuarentena vs Bronze total
#
# **Pregunta:** ¿Cuántos registros fueron rechazados por la validación de
# ingesta y por qué razón?

# %%
bronze_padron = contar(f"s3://{BRONZE_BUCKET}/padron_ruc/")
q_padron = contar(f"s3://{BRONZE_BUCKET}/quarantine/padron_ruc/")
bronze_oc = contar(f"s3://{BRONZE_BUCKET}/ordenes_compra/")
q_oc = contar(f"s3://{BRONZE_BUCKET}/quarantine/ordenes_compra/")

resumen_q = pd.DataFrame(
    [
        {
            "dataset": "padron_ruc",
            "bronze_limpio": bronze_padron,
            "quarantine": q_padron,
            "total_raw": bronze_padron + q_padron,
        },
        {
            "dataset": "ordenes_compra",
            "bronze_limpio": bronze_oc,
            "quarantine": q_oc,
            "total_raw": bronze_oc + q_oc,
        },
    ]
)
resumen_q["pct_rechazado"] = (
    resumen_q["quarantine"] / resumen_q["total_raw"] * 100
).round(4)

fig, ax = plt.subplots(figsize=(8, 4))
x = range(len(resumen_q))
w = 0.35
ax.bar(
    [i - w / 2 for i in x],
    resumen_q["bronze_limpio"],
    w,
    label="Bronze limpio",
    color="#2ecc71",
)
ax.bar(
    [i + w / 2 for i in x],
    resumen_q["quarantine"],
    w,
    label="Quarantine",
    color="#e74c3c",
)
ax.set_xticks(list(x))
ax.set_xticklabels(resumen_q["dataset"])
ax.set_ylabel("Cantidad de registros")
ax.set_title("Registros limpios vs rechazados (quarantine) por dataset")
ax.legend()
for i, row in resumen_q.iterrows():
    if row["quarantine"] > 0:
        ax.text(
            i + w / 2,
            row["quarantine"],
            f"{row['pct_rechazado']:.4f}%",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#e74c3c",
        )
plt.tight_layout()
plt.show()

# %% [markdown]
# **Interpretación:** El porcentaje de registros en cuarentena es bajo.
# Los rechazos en Órdenes de Compra corresponden a filas con años fuera
# de rango (2202, 8202 — errores de digitación detectados en el raw).
# El Padrón RUC tiene rechazo cero o mínimo porque su esquema es más
# estable.

# %% [markdown]
# ## 4. Validación de normalización de departamento
#
# **Pregunta:** ¿`normalize_department` dejó algún departamento sin mapear
# (NULL o valor fuera de CANONICAL_DEPARTMENTS)?

# %%
CANONICAL = {
    "AMAZONAS", "ANCASH", "APURIMAC", "AREQUIPA", "AYACUCHO", "CAJAMARCA",
    "CALLAO", "CUSCO", "HUANCAVELICA", "HUANUCO", "ICA", "JUNIN",
    "LA LIBERTAD", "LAMBAYEQUE", "LIMA", "LORETO", "MADRE DE DIOS",
    "MOQUEGUA", "PASCO", "PIURA", "PUNO", "SAN MARTIN", "TACNA",
    "TUMBES", "UCAYALI",
}

# Leer la partición más reciente de silver/padron_ruc
base = f"s3://{SILVER_BUCKET}/padron_ruc/"
dirs_anio = sorted(wr.s3.list_directories(base))
latest_anio = dirs_anio[-1]
dirs_mes = sorted(wr.s3.list_directories(latest_anio))
latest_mes = dirs_mes[-1]

deptos = wr.s3.read_parquet(latest_mes, columns=["Departamento"])
unicos = set(deptos["Departamento"].dropna().unique())
no_canonicos = unicos - CANONICAL
nulos_dept = int(deptos["Departamento"].isna().sum())

print(f"Departamentos únicos encontrados: {len(unicos)}")
print(f"Nulos en Departamento: {nulos_dept:,} ({nulos_dept / len(deptos) * 100:.2f}%)")
if no_canonicos:
    print(f"⚠ Valores NO canónicos detectados: {no_canonicos}")
else:
    print("✓ Todos los departamentos están dentro del set canónico (25 departamentos).")
