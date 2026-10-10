"""
Script CLI para la consolidación de experimentos y generación de la tabla comparativa (Tarea B5).

Uso:
    python -m src.models.evaluation.consolidar_experimentos \\
        --artifacts_dir data/output \\
        --output docs/model/tabla_comparativa_experimentos.md \\
        --mlflow
"""

import argparse
import os
import sys
import pandas as pd

from .tracker import (
    cargar_run_records,
    consolidar_experimentos,
    formatear_tabla_markdown,
    registrar_mlflow,
    verificar_completitud_matriz,
)


def parse_args(args_list=None):
    parser = argparse.ArgumentParser(
        description="Consolidación y seguimiento de experimentos SSCO (Tarea B5)"
    )
    parser.add_argument(
        "--artifacts_dir",
        type=str,
        default="data/output",
        help="Directorio local donde se encuentran los run_record*.json y predicciones.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="docs/model/tabla_comparativa_experimentos.md",
        help="Ruta del archivo Markdown donde se escribirá la tabla comparativa.",
    )
    parser.add_argument(
        "--mlflow",
        action="store_true",
        help="Registrar las corridas en una instancia de MLflow local.",
    )
    parser.add_argument(
        "--tracking_uri",
        type=str,
        default=None,
        help="URI del servidor de tracking de MLflow (opcional).",
    )
    parser.add_argument(
        "--n_bootstrap",
        type=int,
        default=500,
        help="Número de remuestreos de bootstrap para métricas de test (default: 500).",
    )

    if args_list is not None:
        return parser.parse_args(args_list)
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[INFO] Buscando run_records en: {args.artifacts_dir}")

    records_validos, registros_con_error = cargar_run_records(args.artifacts_dir)

    if registros_con_error:
        print(f"[ALERTA] Se encontraron {len(registros_con_error)} registros con error de esquema o lectura:")
        for reg in registros_con_error:
            print(f"  - {reg.get('archivo')}: {reg.get('error') or reg.get('errores')}")

    if not records_validos:
        print(f"[ERROR] No se encontraron run_records válidos en: {args.artifacts_dir}")
        return 1

    print(f"[INFO] Se cargaron {len(records_validos)} run_records válidos.")

    # Verificar completitud contra matriz E1..E6
    check_matriz = verificar_completitud_matriz(records_validos)
    if check_matriz["completas"]:
        print("[OK] Matriz completa: todas las corridas E1 a E6 están presentes.")
    else:
        print(f"[AVISO] Corridas faltantes de la matriz oficial E1..E6: {check_matriz['faltantes']}")

    # Cargar predicciones asociadas si existen en disco
    predicciones_por_run = {}
    for r in records_validos:
        run_id = r.get("run_id")
        ruta_pred = r.get("ruta_predicciones", "")

        # Si la ruta guardada existe localmente, o buscar en artifacts_dir
        candidatos = [
            ruta_pred,
            os.path.join(args.artifacts_dir, f"predictions_{run_id}.parquet"),
            os.path.join(args.artifacts_dir, run_id, "predictions.parquet"),
        ]
        for c in candidatos:
            if c and os.path.isfile(c):
                try:
                    predicciones_por_run[run_id] = pd.read_parquet(c)
                    print(f"[INFO] Predicciones cargadas para run_id '{run_id}' desde: {c}")
                    break
                except Exception as e:
                    print(f"[ALERTA] Error al leer predicciones de {c}: {e}")

    # Consolidar
    df_experimentos = consolidar_experimentos(
        records=records_validos,
        predicciones_por_run=predicciones_por_run,
        n_bootstrap=args.n_bootstrap,
    )

    # Formatear markdown
    contenido_tabla = formatear_tabla_markdown(df_experimentos)

    cabecera_doc = f"""# Tabla Comparativa de Experimentos — Modelo Base SSCO (Tarea B5)

> **Generado automáticamente:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}  
> **Total de corridas consolidadas:** {len(records_validos)}  
> **Corridas oficiales detectadas:** {check_matriz['total_encontradas']} de {check_matriz['total_esperadas']}  

---

## 1. Matriz Consolidada de Experimentos (E1 a E6)

{contenido_tabla}

---

## 2. Criterio de Selección del Modelo Oficial

1. Se comparan las corridas de la **Variante V2 con pesos de clase (E1 vs. E2)** sobre la media de PR-AUC obtenida en CV ($\overline{{\\text{{PR-AUC}}}}_{{\\text{{CV}}}}$).
2. Si $\overline{{\\text{{PR-AUC}}}}_{{\\text{{CV}}}}(\\text{{LR}}) \\ge \\overline{{\\text{{PR-AUC}}}}_{{\\text{{CV}}}}(\\text{{DT}})$, se selecciona **Regresión Logística (E1)** debido a su parsimonia y estabilidad.
3. El subconjunto de prueba (`test`) se evalúa con la librería de métricas C4 y permanece sin intervenir en la selección de hiperparámetros.
"""

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(cabecera_doc)

    print(f"[INFO] Tabla comparativa guardada exitosamente en: {args.output}")

    # Registro en MLflow si fue solicitado
    if args.mlflow:
        registrar_mlflow(
            records=records_validos,
            df_experimentos=df_experimentos,
            tracking_uri=args.tracking_uri,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
