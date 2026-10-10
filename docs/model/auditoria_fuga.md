# Auditoría de Fuga de Información (Data Leakage Audit)
## Modelo Base de Scoring SSCO — Desarrollador A

### 1. Marco Metodológico contra la Fuga de Información

La fuga de información (*target leakage* o *data leakage*) ocurre cuando un modelo de Machine Learning utiliza variables que no estarían disponibles en el momento operativo en el que se debe emitir una alerta, o variables que son consecuencia directa del hecho que se intenta predecir, inflando artificialmente las métricas de rendimiento (e.g. PR-AUC > 0.98) pero fracasando estrepitosamente en producción.

En el contexto de la fiscalización tributaria de **Sujetos sin Capacidad Operativa (SSCO)** según el Decreto Legislativo N° 1532 y la normativa SUNAT, la auditoría clasifica los riesgos en tres dimensiones:
1. **Fuga Temporal (Horizonte de Predicción):** La lista oficial de SSCO analizada corresponde al corte oficial de **30 de septiembre de 2026**. Por la Decisión **D3**, los atributos del modelo se calculan con un corte estricto al **30 de junio de 2025** (Padrón Snapshot junio 2025 y órdenes de compra con `fecha_de_emision <= 2025-06-30`). Esto garantiza un horizonte predictivo genuino de **15 meses**, simulando que SUNAT busca identificar entidades de riesgo con más de un año de anticipación a su publicación oficial.
2. **Fuga de Consecuencia (Label Contamination):** Variables como el `Estado` (e.g. `SUSPENSION TEMPORAL`, `BAJA DE OFICIO`) o la `Condicion` (e.g. `NO HABIDO`, `NO HALLADO`) en el Padrón suelen actualizarse como **resultado o consecuencia** del proceso de fiscalización o sanción tributaria, no necesariamente antes de cometer el fraude.
3. **Fuga de Preprocesamiento (Train-to-Test Leakage):** Cálculos estadísticos globales (imputación de medianas, identificación de frecuencias top-N en categorías como CIIU o departamento, y encoders) que si se ajustan en la totalidad del dataset contaminan el conjunto de evaluación `test`. En nuestra arquitectura, todo `Estimator` de PySpark MLlib se entrena estrictamente con `split == "train"`.

---

## 2. Matriz de Auditoría por Variable

| Variable / Feature | Fuente Primaria | Ventana Temporal | ¿Existía antes del evento SSCO? | Nivel de Riesgo | Decisión Operativa | Justificación Técnica y Regulatoria |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ruc` | Padrón RUC | Estática | Sí | Nulo | Identificador | Clave de grano (11 dígitos). No se usa como predictor continuo. |
| `Estado` | Padrón RUC | Snapshot 202506 | Incierto / Consecuencia | **ALTO** | **Solo Variante V1** | La baja de oficio o suspensión suele ser consecuencia de la fiscalización de SUNAT tras detectar la falta de capacidad operativa. |
| `Condicion` | Padrón RUC | Snapshot 202506 | Incierto / Consecuencia | **ALTO** | **Solo Variante V1** | Condición `NO HABIDO` puede registrarse como resultado de visitas de verificación que detonaron la investigación. |
| `actividad_economica_principal` (`ciiu_principal`) | Padrón RUC | Snapshot 202506 | Sí | Bajo | **Variantes V1 y V2** | Código CIIU Rev. 4 declarado en la inscripción o modificación registral histórica. Previo al evento. |
| `tipo_contribuyente` | Padrón RUC | Snapshot 202506 | Sí | Bajo | **Variantes V1 y V2** | Naturaleza jurídica registrada al momento de constituir la empresa. |
| `departamento` | Padrón RUC | Snapshot 202506 | Sí | Bajo | **Variantes V1 y V2** | Domicilio fiscal geográfico registrado en el padrón. |
| `nro_trabajadores` | Padrón RUC | Snapshot 202506 | Sí | Bajo | **Variantes V1 y V2** | Capacidad declarada en planillas mensuales previas a la fecha de corte. |
| `sin_trabajadores` | Padrón RUC | Snapshot 202506 | Sí | Bajo | **Variantes V1 y V2** | Indicador derivado de nulos/ceros en trabajadores. Clave para detectar empresas de fachada sin personal. |
| `n_actividades` | Padrón RUC | Snapshot 202506 | Sí | Bajo | **Variantes V1 y V2** | Número de giros comerciales inscritos. |
| `contrata_con_estado` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Bandera de presencia en compras públicas dentro de la ventana histórica permitida. |
| `n_ordenes` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Volumen contractual ejecutado previo al corte. |
| `monto_total_soles` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Sumatoria de montos en soles (excluyendo órdenes anuladas). |
| `monto_mediano_soles` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Típico tamaño de transacción con el Estado. |
| `monto_maximo_soles` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Concentración máxima por contrato individual. |
| `n_entidades_distintas` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Dispersión o cooptación de entidades públicas compradoras. |
| `pct_monto_en_entidad_principal` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Dependencia o favoritismo contractual hacia un cliente público específico. |
| `pct_ordenes_anuladas` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Frecuencia de cancelaciones/anulaciones por incumplimiento o irregularidades. |
| `antiguedad_contratacion_estado_dias` | SEACE Órdenes | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Días desde la primera orden hasta la fecha de corte. |
| `monto_por_trabajador` | Ratio cruzado | $\le$ 30-jun-2025 | Sí | Bajo | **Variantes V1 y V2** | Desproporción entre facturación estatal y planta laboral. Señal nuclear de falta de capacidad operativa. |
| `informalidad_epen_departamento` | INEI EPEN | Encuesta 2025 | Sí | Bajo | **Variantes V1 y V2** | Contexto macroeconómico regional de informalidad laboral. |
| `es_prico` | Padrón PRICOS | Registro 2025 | Sí | N/A | **EXCLUIDO** | Los PRICOS son excluidos de la población mediante anti-join. Mantener la columna generaría un vector constante en cero. |
| `RazonSocial` / `Nombres` | Padrón / SEACE | Cualquier | Sí | **CRÍTICO** | **EXCLUIDO** | Privacidad (D15) y prevención de sesgos o memorización espuria de nombres de personas naturales. |

---

## 3. Justificación de la Estrategia de Dos Variantes (V1 vs. V2)

Siguiendo la **Decisión D11**:
- **Variante V2 (Oficial de Producción):** Excluye rigurosamente `Estado` y `Condicion`. Este modelo se fundamenta en capacidad económica real, comportamiento en compras públicas, estructura de contratación y contexto territorial. **Es el modelo oficial para auditorías y toma de decisiones operativas.**
- **Variante V1 (Techo Experimental / Control de Fuga):** Incluye `Estado` y `Condicion`. Se entrena exclusivamente como punto de referencia experimental ("benchmark de techo superior") para cuantificar exactamente qué proporción del poder predictivo se debe a marcadores registrales que podrían estar contaminados temporalmente por la fiscalización previa.

Si la diferencia de rendimiento entre V1 y V2 es moderada, valida que las variables de comportamiento contractual y capacidad operativa (V2) contienen señal suficiente y robusta para la detección temprana sin depender de etiquetas sintomáticas tardías.
