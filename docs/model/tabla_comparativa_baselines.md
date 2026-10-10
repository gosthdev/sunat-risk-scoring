# Tabla Comparativa de Baselines — Modelo de Scoring SSCO

> **Fecha:** 2026-10-10  
> **Responsable:** Dev B (Evaluación y Plataforma)  
> **Contrato de Evaluación:** Contrato C4 (`compute_metrics`)  
> **Dataset de Evaluación:** Fixture Sintético `synthetic_predictions_informative.parquet` (N=1,000 en test, Prevalencia=1.0%)

---

## 1. Resumen de Modelos Evaluados

| ID | Modelo / Baseline | Tipo | Descripción |
| :--- | :--- | :--- | :--- |
| **B0** | Baseline Trivial | Heurística negativa | Asigna score = 0.0 constante a todos los RUCs. |
| **B1** | Regla del Analista | Regla experta | Suma hasta 3 banderas heurísticas de riesgo operativo (condición domicilio, trabajadores, contrataciones). *(Ver Nota 1)* |
| **B2** | Azar (Random) | Pseudoaleatorio | Distribución uniforme $U(0, 1)$ con semilla determinística (`seed=42`). |
| **REF** | Sintético Informativo | Predictivo | Modelo de referencia con señal discriminante generada sintéticamente. |

---

## 2. Métricas Comparativas Oficiales (Split Test)

| Modelo | PR-AUC (IC 95%) | Recall@1% | Recall@5% | Recall@100 | Recall@500 | Lift@1% | Brier Score |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **B0 Trivial** | 0.0100 [0.005, 0.017] | 0.00 | 0.10 | 0.10 | 0.40 | 0.0x | 0.0100 |
| **B1 Analista** | 0.0100 [0.005, 0.017] | 0.00 | 0.10 | 0.10 | 0.40 | 0.0x | 0.0100 |
| **B2 Azar** | 0.0097 [0.005, 0.022] | 0.00 | 0.00 | 0.00 | 0.50 | 0.0x | 0.3269 |
| **REF Informativo** | **0.8211 [0.531, 1.000]** | **0.70** | **0.90** | **1.00** | **1.00** | **70.0x** | **0.0561** |

---

## 3. Conclusiones y Validación de la Librería de Evaluación

1. **Paradoja de la exactitud (Accuracy):**
   - El baseline trivial B0 obtendría un 99% de Accuracy en un entorno desbalanceado (1% positivos), pero su capacidad de captura de riesgo en el 1% superior es 0.0 (Lift 0.0x). Esto valida la decisión de priorizar **PR-AUC, Recall@K y Lift@K** como métricas oficiales.
2. **Convergencia del Azar (B2):**
   - El modelo aleatorio B2 produce un PR-AUC de ~0.0097, equivalente a la prevalencia natural de la muestra (1.0%), y captura el 50% de positivos cuando se fiscaliza el 50% de la población (Recall@500 = 0.50), confirmando el comportamiento teórico esperado.
3. **Punto de Referencia (REF Informativo):**
   - El modelo con información predictiva captura al 70% de los contribuyentes SSCO en el top-1% (Lift de 70x), estableciendo el umbral superior que los modelos entrenados por Dev A (LightGBM, XGBoost, Regresión Logística) buscarán igualar o superar sobre el padrón real.

> **Nota 1 (Comportamiento de B1 sobre fixtures C2):**  
> El fixture sintético implementa el esquema estricto del Contrato C2 (columnas de predicciones y metadatos del modelo: `ruc`, `label`, `score`, `split`, `departamento`). Dado que no contiene las covariables de entrada crudas (`condicion`, `nro_trab`, `contrata_con_estado`), el baseline B1 asigna score 0.0 por diseño de seguridad, convergiendo con B0 sobre este fixture específico. Al recibir el dataset enriquecido de producción, B1 discriminará las 3 reglas heurísticas.
