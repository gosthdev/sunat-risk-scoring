"""
Validación de RUC para los scripts con pandas (fuera de Spark).

Mismo criterio que silver_transforms.ruc_valido: un RUC tiene exactamente 11
dígitos. Sin esto, `to_numeric(coerce)` + `dropna` descartaba en silencio los
RUC no numéricos y aceptaba como válido uno de 8 dígitos (un DNI).
"""

import pandas as pd

# Si menos de esta fracción de la columna son RUC válidos, la columna casi seguro no
# es el RUC (p. ej. un índice o "N°"): se falla en vez de publicar una etiqueta falsa.
MIN_FRACCION_VALIDA = 0.95


def filtrar_ruc_validos(df: pd.DataFrame, columna: str, nombre: str) -> pd.DataFrame:
    """Devuelve `df` solo con filas cuyo `columna` es un RUC de 11 dígitos (como int64).

    Informa cuántas filas descarta y algunos ejemplos.
    """
    numerico = pd.to_numeric(df[columna], errors="coerce")
    valido = numerico.notna() & (numerico >= 10**10) & (numerico < 10**11)
    total, invalidos = len(df), int((~valido).sum())

    if total == 0 or (total - invalidos) / total < MIN_FRACCION_VALIDA:
        raise ValueError(
            f"[{nombre}] solo {total - invalidos} de {total} valores de '{columna}' "
            "son RUC de 11 dígitos: ¿es la columna correcta?"
        )
    if invalidos:
        ejemplos = df.loc[~valido, columna].head(5).tolist()
        print(
            f"[{nombre}] {invalidos} de {total} filas sin RUC válido de 11 dígitos "
            f"descartadas (ejemplos: {ejemplos})."
        )

    salida = df.loc[valido].copy()
    salida[columna] = numerico[valido].astype("int64")
    return salida
