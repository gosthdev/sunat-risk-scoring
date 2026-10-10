# Marco Metodológico y Técnico de Explicabilidad Analítica: Modelo SSCO

## 1. Fundamento Normativo y Operativo

### A. Marco Regulatorio (Decreto Legislativo N.º 1532 y Ley N.º 27444)
El procedimiento de atribución de la condición de **Sujeto sin Capacidad Operativa (SSCO)** según el Decreto Legislativo N.º 1532 faculta a la SUNAT a desconocer el crédito fiscal y el costo/gasto tributario derivado de comprobantes emitidos por entidades sin infraestructura económica real.

En concordancia con el principio del debido procedimiento y el **deber de motivación de los actos administrativos** (Ley del Procedimiento Administrativo General, Ley N.º 27444), la administración tributaria no puede respaldar órdenes de fiscalización o imputaciones basándose en algoritmos opacos de "caja negra". Todo modelo de scoring aplicado en este contexto debe proveer:
1. **Transparencia Intrínsica:** Capacidad de auditar y comprender la estructura funcional del modelo.
2. **Atribución Local y Global:** Identificación clara de qué variables incrementan o disminuyen el riesgo para cada contribuyente y a nivel poblacional.
3. **Defendibilidad Jurídica:** Evidencia analítica sustentable ante el Tribunal Fiscal y el Poder Judicial.

---

## 2. Implementación de Explicabilidad en `src/models/explainability.py`

El módulo `explainability.py` implementa extractores analíticos específicos para los dos estimadores evaluados en la suite experimental (Regresión Logística y Árbol de Decisión), mapeando las transformaciones intermedias generadas por el `ColumnTransformer`.

### A. Trazabilidad de Variables Transformadas (`obtener_nombres_features`)
Tras la compresión logarítmica, imputación de nulos y codificación One-Hot de categorías (top-30 más frecuentes), las variables de entrada se expanden a un espacio dimensional estructurado:
- **Prefijo `num__`:** Variables cuantitativas escaladas (`num__monto_total_soles`, `num__n_ordenes`, `num__sin_trabajadores`, etc.).
- **Prefijo `cat__`:** Variables categóricas binarizadas (`cat__departamento_LIMA`, `cat__ciiu_principal_4690`, etc.).

La función `obtener_nombres_features(preprocessor, num_cols, cat_cols)` reconstruye de forma determinista el vector de características resultante para garantizar correspondencia exacta con los parámetros internos del modelo.

---

## 3. Explicabilidad para Regresión Logística (Modelo Oficial V2)

### A. Interpretación de Coeficientes Estandarizados
En la Regresión Logística con variables numéricas estandarizadas mediante `StandardScaler()` ($\mu = 0, \sigma = 1$), el modelo calcula la probabilidad condicional de riesgo mediante:
$$\log\left(\frac{P(Y=1 \mid \mathbf{x})}{1 - P(Y=1 \mid \mathbf{x})}\right) = \beta_0 + \sum_{j=1}^{p} \beta_j x_j$$

Cada coeficiente $\beta_j$ cuantifica el cambio en el log-odds de ser SSCO ante el incremento de una desviación estándar en la variable numérica $x_j$, o la presencia ($x_j=1$) de una categoría codificada.

### B. Extracción y Clasificación de Impacto (`extraer_explicabilidad_lr`)
La función `extraer_explicabilidad_lr(best_pipeline, feature_names)` extrae el vector $\boldsymbol{\beta}$ y construye una tabla analítica estructurada:
- **Magnitud Absoluta ($|\beta_j|$):** Determina la importancia e influencia relativa de la característica en la predicción global.
- **Signo y Dirección del Impacto:**
  - $\beta_j > 0$ (**Aumenta Riesgo SSCO**): Características asociadas a mayor probabilidad de evasión o simulación de operaciones (e.g. ausencia de trabajadores declarados `sin_trabajadores=1`, tasa de informalidad regional elevada, alta proporción de órdenes anuladas, o ratios anormales de monto adjudicado por trabajador).
  - $\beta_j < 0$ (**Disminuye Riesgo SSCO**): Factores de estabilidad y consistencia económica (e.g. antigüedad comprobada en contrataciones públicas, nómina de trabajadores regular, o diversificación de entidades compradoras).

---

## 4. Explicabilidad para Árbol de Decisión (Modelo Alternativo de Reglas)

### A. Extracción de Reglas de Negocio en Lenguaje Natural (`extraer_reglas_dt`)
Para el Árbol de Decisión, la explicabilidad se obtiene convirtiendo la estructura jerárquica de nodos y hojas en un conjunto ordenado de reglas lógicas condicionales legibles por auditores tributarios (`export_text`):
```text
|--- sin_trabajadores <= 0.50
|   |--- monto_total_soles <= 4.21
|   |   |--- class: 0 (Bajo Riesgo)
|   |--- monto_total_soles > 4.21
|   |   |--- class: 0
|--- sin_trabajadores > 0.50
|   |--- pct_ordenes_anuladas > 0.15
|   |   |--- class: 1 (Alto Riesgo SSCO)
```
Estas reglas permiten a los inspectores de SUNAT justificar fiscalizaciones con condiciones objetivas y verificables en el expediente administrativo.

### B. Importancia de Variables por Impureza de Gini (`extraer_importancias_dt`)
La función `extraer_importancias_dt(best_pipeline, feature_names)` calcula la reducción total de impureza (*Mean Decrease in Impurity* o Gini Importance) aportada por cada variable a través de todas las particiones del árbol, identificando los predictores con mayor poder de discriminación inicial.

---

## 5. Integración con Contratos C2 y C3

Los resultados de explicabilidad no son efímeros; se integran con la infraestructura del Data Lake:
1. **Contrato C2 (`predictions.parquet`):** Los contribuyentes priorizados en los percentiles superiores de `rank_global` pueden cruzarse de inmediato con los coeficientes de impacto para sustentar el plan de inspección.
2. **Contrato C3 (`run_record.json`):** Almacena los hiperparámetros ganadores y metadatos de configuración que validan la estabilidad y reproducibilidad de los factores explicativos extraídos.
