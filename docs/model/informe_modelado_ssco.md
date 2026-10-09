# Documento Técnico Académico: Secciones 4.2.1 – 4.2.3
## Proyecto de Big Data: Detección de Sujetos sin Capacidad Operativa (SSCO)
### Autor: Desarrollador A (Ingeniería de Datos y Modelado Predictivo)
### Formato y Estilo: Citas en APA 7.ª edición

---

## 4.2.1 Definición del Problema de Modelado

### A. Grano y Formulación del Problema
El objetivo central del sistema predictivo es mitigar el fraude tributario asociado a empresas de fachada o instrumentales que emiten comprobantes de pago falsos sin contar con infraestructura económica real, clasificadas bajo el régimen legal peruano como **Sujetos sin Capacidad Operativa (SSCO)** (Decreto Legislativo N.º 1532, 2022).

Para modelar este fenómeno analíticamente en una arquitectura Big Data (PySpark MLlib), se define un problema de **aprendizaje supervisado de clasificación binaria y ordenamiento de riesgo (ranking)** con un grano estricto de:
$$\text{Grano} = 1 \text{ fila por cada RUC único (Registro Único de Contribuyentes)}$$

El modelo no busca dictar sentencias administrativas irrevocables, sino generar un puntaje de propensión continuo $S_i \in [0, 1]$ y un ranking de riesgo ordinal descendente $\text{rank}_i \in \{1, 2, \dots, N\}$ para priorizar las acciones de fiscalización y auditoría de campo de la administración tributaria dentro de su restricción presupuestaria y operativa (Hand, 2009).

### B. Definición de la Variable Objetivo (Label)
La variable dependiente binaria $Y_i \in \{0, 1\}$ se define de forma objetiva e incorruptible:
$$Y_i = \begin{cases} 1 & \text{si el RUC } i \text{ figura en la lista oficial de SSCO publicada por SUNAT al 30 de septiembre de 2026} \\ 0 & \text{en caso contrario} \end{cases}$$

Esta formulación evita la circularidad que introduciría etiquetar a las empresas mediante reglas heurísticas subjetivas (e.g. marcar como sospechoso a quien tenga `Condicion = 'NO HABIDO'`), garantizando que la verdad terreno (*ground truth*) proviene de resoluciones de determinación firmes emitidas por la autoridad tributaria.

### C. Población Objetivo y Criterios de Exclusión
La población bajo estudio comprende la totalidad del tejido empresarial registrado en el Padrón RUC nacional al cierre del corte temporal, con una exclusión crítica y fundamentada:
1. **Exclusión Estricta de PRICOS (Principales Contribuyentes):** Los contribuyentes incorporados al directorio de PRICOS representan grandes corporaciones sujetas a auditorías continuas y esquemas de control diferenciados. Mezclarlos con el universo general distorsionaría los patrones de riesgo de las empresas fantasma (que típicamente operan como medianas o pequeñas firmas no fiscalizadas de oficio). La exclusión se realiza mediante un anti-join estricto contra `silver/pricos`.
2. **Inclusión de No Contratistas Estatales:** A diferencia de formulaciones preliminares que restringían el estudio únicamente a proveedores del Estado, la población final abarca a **todo el padrón**. La contratación pública se modela como un conjunto de predictores continuos (`contrata_con_estado`, `monto_total_soles`, `n_ordenes`), evitando el sesgo de selección muestral (Heckman, 1979).

### D. Corte Temporal y Horizonte de Predicción (Prevención de Circularidad)
Para garantizar validez temporal prospectiva y erradicar cualquier forma de fuga de información (*temporal leakage*), se implementa un diseño de corte cerrado retrospectivo (**Decisión D3**):
- **Ventana de Observación de Features:** Padrón RUC snapshot al **30 de junio de 2025** (`mes_referencia = '202506'`) y Órdenes de Compra del SEACE adjudicadas entre el **1 de enero de 2025 y el 30 de junio de 2025** (`fecha_de_emision <= '2025-06-30'`).
- **Ventana de Etiquetado del Label:** Registro oficial de SSCO consolidado al **30 de septiembre de 2026**.
- **Horizonte Predictivo:** Existe un lapso de **15 meses** entre la última fecha de observación visible para los algoritmos y la fecha del evento de etiquetado. Por ende, los modelos simulan exactamente la capacidad de alerta temprana de SUNAT con más de un año de anticipación.

---

## 4.2.2 Datos y Features

### A. Catálogo Resumido de Variables Predictoras
Las variables se agrupan en cuatro dimensiones operativas fundamentales: capacidad operativa registral, perfil de contratación pública, ratios de inconsistencia y contexto territorial regional.

| Variable | Tipo de Dato | Fuente Primaria | Ventana Temporal | Nulos y Tratamiento | Riesgo de Fuga |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `nro_trabajadores` | Numérica | Padrón RUC | Jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `sin_trabajadores` | Binaria (0/1) | Padrón RUC | Jun-2025 | Cero (ausencia de personal) | Bajo |
| `tipo_contribuyente` | Categórica | Padrón RUC | Jun-2025 | Imputa `"DESCONOCIDO"` | Bajo |
| `departamento` | Categórica | Padrón RUC | Jun-2025 | Top-25 en train / `"DESCONOCIDO"` | Bajo |
| `ciiu_principal` | Categórica | Padrón RUC | Jun-2025 | Top-30 en train / `"OTROS"` | Bajo |
| `n_actividades` | Numérica | Padrón RUC | Jun-2025 | Valor entero $\ge 1$ | Bajo |
| `contrata_con_estado` | Binaria (0/1) | SEACE | Ene–Jun 2025 | Cero si no registra compras | Bajo |
| `n_ordenes` | Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `monto_total_soles` | Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `monto_mediano_soles`| Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `monto_maximo_soles` | Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `n_entidades_distintas`| Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `pct_monto_en_entidad_principal` | Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `pct_ordenes_anuladas`| Numérica | SEACE | Ene–Jun 2025 | Cero si no contrata | Bajo |
| `antiguedad_contratacion_estado_dias` | Numérica | SEACE | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `monto_por_trabajador`| Numérica | Calculada | Ene–Jun 2025 | Imputación mediana + bandera `_es_nulo` | Bajo |
| `informalidad_epen_departamento` | Numérica | INEI EPEN | Encuesta 2025 | Mediana regional | Bajo |
| `Estado` | Categórica | Padrón RUC | Jun-2025 | Imputa `"DESCONOCIDO"` | **ALTO (Solo V1)** |
| `Condicion` | Categórica | Padrón RUC | Jun-2025 | Imputa `"DESCONOCIDO"` | **ALTO (Solo V1)** |

### B. Auditoría de Fuga y Justificación de las Variantes V1 vs. V2
Siguiendo las mejores prácticas metodológicas en Machine Learning aplicado al sector público (Kaufman et al., 2012):
- **Variante V2 (Oficial y Defendible):** Excluye formalmente las columnas registrales `Estado` y `Condicion`. La auditoría identificó que transiciones registrales como `BAJA DE OFICIO` o `NO HABIDO` suelen ser consecuencias ex-post de las indagaciones preliminares de SUNAT. Entrenar con ellas generaría una métrica artificialmente inflada que fallaría al predecir empresas que aparentan normalidad documental al momento de operar.
- **Variante V1 (Techo Experimental):** Mantiene `Estado` y `Condicion` como benchmark técnico para medir la pérdida de rendimiento que implica aislar las variables sintomáticas y auditar la resiliencia de las señales transaccionales puras.

### C. Prevalencia y Desbalance Extremo de Clases
En el corte de junio 2025 sobre el padrón depurado sin PRICOS, la cantidad de positivos SSCO identificados es del orden de 764 casos frente a más de 140,000 entidades no catalogadas como SSCO, lo que representa una **prevalencia inferior al 0.55%** ($\pi < 0.0055$). Este desbalance extremo condiciona directamente el diseño de la validación cruzada y las métricas de evaluación.

---

## 4.2.3 Modelado

### A. Arquitectura del Pipeline en Apache Spark MLlib
Para asegurar reproducibilidad estricta y prevenir la fuga de train hacia test, todo el preprocesamiento y entrenamiento se encapsula en un único objeto `pyspark.ml.Pipeline`.

```
                    ┌─────────────────────────────────────────────────────┐
                    │               model_inputs (Split: Train)           │
                    └──────────────────────────┬──────────────────────────┘
                                               │
                                               ▼
                    ┌─────────────────────────────────────────────────────┐
                    │     SQLTransformer: Banderas de Nulo (_es_nulo)     │
                    └──────────────────────────┬──────────────────────────┘
                                               │
                                               ▼
                    ┌─────────────────────────────────────────────────────┐
                    │     Imputer: Medianas de Train (Sin fuga a Test)    │
                    └──────────────────────────┬──────────────────────────┘
                                               │
                                               ▼
                    ┌─────────────────────────────────────────────────────┐
                    │         StringIndexer (handleInvalid="keep")        │
                    └──────────────────────────┬──────────────────────────┘
                                               │
                    ┌──────────────────────────┴──────────────────────────┐
                    ▼ (Si Modelo == LR)                                   ▼ (Si Modelo == DT)
       ┌─────────────────────────┐                           ┌─────────────────────────┐
       │     OneHotEncoder       │                           │   (Pasa índices directos)│
       │    (dropLast=True)      │                           │                         │
       └────────────┬────────────┘                           └────────────┬────────────┘
                    │                                                     │
                    └──────────────────────────┬──────────────────────────┘
                                               │
                                               ▼
                    ┌─────────────────────────────────────────────────────┐
                    │        VectorAssembler (features unificadas)        │
                    └──────────────────────────┬──────────────────────────┘
                                               │
                                               ▼
                    ┌─────────────────────────────────────────────────────┐
                    │   Estimator: LogisticRegression / DecisionTree      │
                    │   (Ponderado por class_weight = N_neg / N_pos)      │
                    └─────────────────────────────────────────────────────┘
```

> **Consideración Crítica sobre Estandarización:** Se omitió conscientemente el uso de `StandardScaler` previo a `LogisticRegression`. La implementación de `LogisticRegression` en Spark MLlib ejecuta internamente estandarización sobre la matriz de covariables en espacio distribuido; agregar un transformer explícito provocaría distorsión cuadrática en los coeficientes finales.

### B. Tratamiento del Desbalance: Pesos de Clase vs. Algoritmos de Resampling
En escenarios distribuidos de Big Data con alta dimensionalidad, se optó por **pesos de clase inversos a la frecuencia** en lugar de técnicas de sobremuestreo sintético como SMOTE (Chawla et al., 2002):
$$w_{y=1} = \frac{N_{\text{neg}}}{N_{\text{pos}}} \approx 180 - 200, \quad w_{y=0} = 1.0$$
- **Justificación:** SMOTE en clústeres Spark requiere librerías externas no nativas que elevan drásticamente la latencia de red al calcular k-vecinos más cercanos distribuidos. Además, generar entidades fantasma sintéticas en espacios categóricos de alta cardinalidad (CIIU y departamentos) crea combinaciones corporativas físicamente irreales.
- Los pesos de clase se aplican **exclusivamente al optimizador de entrenamiento (`weightCol`)** y **nunca al evaluador de validación cruzada**, asegurando que el cálculo de métricas de calidad permanezca inalterado sobre la distribución real de la población.

### C. Esquema de Validación Cruzada: 5 Folds Estratificados por RUC
Se descarta cualquier partición aleatoria ingenua. La partición de datos implementa:
1. **Partición 80% Train / 20% Test:** Conducida por un hash determinista con semilla fija ($42$). El conjunto de test permanece estrictamente bajo llave (*hold-out*) y jamás interviene en la selección de hiperparámetros.
2. **Validación Cruzada de 5 Folds (`foldCol`):** Dentro del conjunto de entrenamiento, se asigna el fold ($0$ a $4$) de forma estratificada por etiqueta y particionada por RUC. Se garantiza que cada fold posea al menos 20 ejemplos positivos de SSCO, evitando la colapsabilidad del optimizador L-BFGS.

### D. Métrica de Optimización: PR-AUC frente a ROC-AUC
Siguiendo las demostraciones de Saito y Rehmsmeier (2015), en contextos de prevalencia ínfima ($\pi < 1\%$), el área bajo la curva ROC (**ROC-AUC**) es engañosamente optimista debido a la enorme masa de verdaderos negativos que comprime la tasa de falsos positivos ($\text{FPR} = \frac{\text{FP}}{\text{FP} + \text{TN}} \approx 0$).

Por tanto, el `CrossValidator` de Spark MLlib se parametriza formalmente con:
$$\text{metricName} = \text{"areaUnderPR"} \quad (\text{PR-AUC / Average Precision})$$
El área bajo la curva Precision-Recall concentra la penalización en los falsos positivos y falsos negativos directamente en relación con la clase minoritaria de interés.

### E. Matriz Completa de Experimentos y Criterio de Selección

| ID Corrida | Algoritmo | Variante Features | Pesos de Clase | Dataset | Prioridad | Propósito Metodológico |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **E1** | LR (L2/ElasticNet) | **V2** | Sí | v1 | **MUST** | Candidato a modelo oficial (Lineal, interpretable, sin fuga). |
| **E2** | DT (Árbol Prof. 3–6) | **V2** | Sí | v1 | **MUST** | Candidato a modelo oficial (No lineal, reglas de negocio explícitas). |
| **E3** | LR | **V1** | Sí | v1 | **MUST** | Benchmark techo superior (evaluación del sesgo de `Estado`/`Condicion`). |
| **E4** | DT | **V1** | Sí | v1 | **MUST** | Benchmark techo superior no lineal. |
| **E5** | LR | **V2** | No | v1 | **MUST** | Control de ablación para validar impacto del reponderamiento. |
| **E6** | DT | **V2** | No | v1 | **MUST** | Control de ablación de pesos en árboles. |
| **E7** | LR | **V2** | Sí | v2 | **SHOULD** | Análisis de sensibilidad temporal (corte diciembre 2025). |
| **E8** | DT | **V2** | Sí | v2 | **SHOULD** | Análisis de sensibilidad temporal en árboles. |

**Regla de Decisión para la Elección del Modelo Oficial:**
1. Se comparan exclusivamente las corridas de la **Variante V2 con pesos de clase (E1 vs. E2)** sobre la media de PR-AUC obtenida en la validación cruzada de 5 folds ($\overline{\text{PR-AUC}}_{\text{CV}}$).
2. Si $\overline{\text{PR-AUC}}_{\text{CV}}(\text{LR}) \ge \overline{\text{PR-AUC}}_{\text{CV}}(\text{DT})$, se selecciona **Regresión Logística (E1)** como modelo oficial debido a su estabilidad asintótica y parsimonia.
3. Si $\overline{\text{PR-AUC}}_{\text{CV}}(\text{DT}) > \overline{\text{PR-AUC}}_{\text{CV}}(\text{LR})$, se selecciona **Árbol de Decisión (E2)**.
4. En situación de empate estadístico, rige el principio de la Navaja de Ockham: se elige la Regresión Logística.

---

## Referencias Bibliográficas (APA 7.ª edición)

- Chawla, N. V., Bowyer, K. W., Hall, L. O., & Kegelmeyer, W. P. (2002). SMOTE: Synthetic minority over-sampling technique. *Journal of Artificial Intelligence Research*, 16, 321–357. https://doi.org/10.1613/jair.953
- Decreto Legislativo N.º 1532. (2022). *Decreto Legislativo que regula el procedimiento de atribución de la condición de sujeto sin capacidad operativa*. Diario Oficial El Peruano.
- Hand, D. J. (2009). Measuring classifier performance: A coherent alternative to the area under the ROC curve. *Machine Learning*, 77(1), 103–123. https://doi.org/10.1007/s10994-009-5119-5
- Heckman, J. J. (1979). Sample selection bias as a specification error. *Econometrica*, 47(1), 153–161. https://doi.org/10.2307/1912352
- Kaufman, S., Rosset, S., Perlich, C., & Stitelman, O. (2012). Leakage in data mining: Formulation, detection, and avoidance. *ACM Transactions on Knowledge Discovery from Data (TKDD)*, 6(4), 1–21. https://doi.org/10.1145/2382577.2382579
- Saito, T., & Rehmsmeier, M. (2015). The precision-recall plot is more informative than the ROC plot when evaluating binary classifiers on imbalanced datasets. *PLOS ONE*, 10(3), e0118432. https://doi.org/10.1371/journal.pone.0118432
