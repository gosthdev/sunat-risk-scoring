"""Dataset mínimo exclusivo de tests unitarios. Nunca se usa en el pipeline."""

import os

import numpy as np
import pandas as pd


def escribir_parquet_minimo(output_dir: str, n_rows: int = 500, seed: int = 42) -> str:
    """Escribe un parquet mínimo solo para ejercitar el código en los tests unitarios."""
    os.makedirs(output_dir, exist_ok=True)
    target_parquet = os.path.join(output_dir, "dataset_minimo_tests.parquet")

    np.random.seed(seed)
    n_train = int(n_rows * 0.8)

    rucs = [f"{20100000000 + i:011d}" for i in range(1, n_rows + 1)]
    splits = ["train" if i < n_train else "test" for i in range(n_rows)]
    folds = [(i % 5) if splits[i] == "train" else -1 for i in range(n_rows)]
    # Prevalencia baja pero con suficientes positivos para 5 folds
    labels = [1 if (i % 8 == 0) else 0 for i in range(n_rows)]

    montos = np.random.exponential(scale=35000, size=n_rows)
    montos[::9] = np.nan
    medianas = montos / np.random.randint(1, 8, size=n_rows)
    medianas[::7] = np.nan

    n_ordenes = np.random.poisson(lam=5, size=n_rows)
    antiguedad = np.random.randint(1, 120, size=n_rows)
    n_trabajadores = np.random.choice(
        [0, 1, 3, 10, 50], size=n_rows, p=[0.4, 0.3, 0.15, 0.1, 0.05]
    )
    sin_trabajadores = (n_trabajadores == 0).astype(int)

    ciius = np.random.choice(["4659", "4100", "4711", "9999", ""], size=n_rows)
    dept_choices = np.array(["LIMA", "AREQUIPA", "CUSCO", "LORETO", None], dtype=object)
    depts = np.random.choice(dept_choices, size=n_rows)
    tipos = np.random.choice(["SOCIEDAD ANONIMA", "PERSONA NATURAL"], size=n_rows)
    estados = np.random.choice(
        ["ACTIVO", "BAJA DE OFICIO", "SUSPENSION TEMPORAL"], size=n_rows
    )
    condiciones = np.random.choice(["HABIDO", "NO HABIDO", "NO HALLADO"], size=n_rows)

    df = pd.DataFrame(
        {
            "ruc": rucs,
            "split": splits,
            "fold": folds,
            "label": labels,
            "monto_total_soles": montos,
            "monto_mediano_soles": medianas,
            "monto_maximo_soles": montos * 1.2,
            "n_ordenes": n_ordenes,
            "total_ordenes_validas": n_ordenes,
            "n_entidades_distintas": np.maximum(1, n_ordenes // 2),
            "pct_monto_en_entidad_principal": np.random.uniform(0.1, 1.0, size=n_rows),
            "pct_ordenes_anuladas": np.random.uniform(0.0, 0.2, size=n_rows),
            "antiguedad_contratacion_estado_dias": antiguedad * 30,
            "antiguedad_meses": antiguedad,
            "nro_trabajadores": n_trabajadores,
            "sin_trabajadores": sin_trabajadores,
            "monto_por_trabajador": montos / np.maximum(1, n_trabajadores),
            "informalidad_epen_departamento": np.random.uniform(
                50.0, 85.0, size=n_rows
            ),
            "ciiu_principal": ciius,
            "actividad_economica_principal": ciius,
            "departamento": depts,
            "tipo_contribuyente": tipos,
            "Estado": estados,
            "Condicion": condiciones,
        }
    )

    df.to_parquet(target_parquet, index=False)
    return target_parquet
