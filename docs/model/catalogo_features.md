# Catálogo Detallado de Features (Feature Store Catalog)
## Proyecto: SUNAT Risk Scoring — Detección de SSCO
### Responsable: Desarrollador A (Datos y Modelo)

Este documento detalla todas las variables y transformaciones de ingeniería de características que alimentan el pipeline de entrenamiento del modelo de riesgo de Sujetos sin Capacidad Operativa (SSCO).

---

## 1. Resumen de Familias de Features

1. **Capacidad Operativa y Dimensión Empresarial (Padrón RUC)**: Representa el tamaño formal del contribuyente, personal en planilla y tipo societario.
2. **Comportamiento Contractual con el Estado (OSCE / SEACE)**: Describe el volumen de contratación, concentración en clientes estatales, dispersión y cancelaciones.
3. **Señales de Inconsistencia Económica (Ratios Cruzados)**: Ratios de desproporción entre adjudicaciones públicas millonarias y nula capacidad instalada.
4. **Contexto Macroeconómico y Territorial (INEI EPEN)**: Grado de informalidad del entorno territorial donde la empresa opera.
5. **Estado Registral Administrativo (Solo Variante V1)**: Situación registral en SUNAT utilizada como cota de comparación.

---

## 2. Catálogo Exhaustivo de Variables

| Nombre de Feature | Tipo | Fuente | Ventana / Corte | Tratamiento de Nulos | Riesgo Fuga | Variantes | Definición Funcional y Fórmula |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ruc` | `string(11)` | Padrón RUC | Snapshot 202506 | No permitido (invariante) | Ninguno | V1, V2 | Clave única de grano a nivel de contribuyente. |
| `nro_trabajadores` | `numeric (double)` | Padrón RUC | Snapshot 202506 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Número de trabajadores declarados en planilla formal (columna `NroTrab`). |
| `sin_trabajadores` | `binary (int)` | Padrón RUC | Snapshot 202506 | Imputa a 0 | Bajo | V1, V2 | Indicador binario: `1` si `nro_trabajadores` es nulo o $\le 0$; `0` si $>0$. Señal típica de cascarón. |
| `tipo_contribuyente` | `categorical (string)` | Padrón RUC | Snapshot 202506 | Imputa `"DESCONOCIDO"` | Bajo | V1, V2 | Tipo registral de contribuyente (`Tipo` en el Padrón: Sociedad Anónima, SAC, Persona Natural con Negocio, etc.). |
| `departamento` | `categorical (string)` | Padrón RUC | Snapshot 202506 | Imputa `"DESCONOCIDO"` | Bajo | V1, V2 | Ubicación política departamental normalizada según estándar UBIGEO. Restringido a top-25 más frecuentes en train. |
| `ciiu_principal` | `categorical (string)` | Padrón RUC | Snapshot 202506 | Imputa `"OTROS"` | Bajo | V1, V2 | Clasificación Industrial Internacional Uniforme (CIIU Rev. 4 principal). Se retienen los top-30 más frecuentes en train, el resto pasa a `"OTROS"`. |
| `n_actividades` | `numeric (int)` | Padrón RUC | Snapshot 202506 | Valor por defecto `1` | Bajo | V1, V2 | Conteo de actividades económicas inscritas en el RUC (principal + secundaria). |
| `contrata_con_estado` | `binary (int)` | SEACE | $\le$ 30-jun-2025 | Imputa a `0` | Bajo | V1, V2 | Bandera binaria: `1` si el RUC registra al menos una orden de compra antes del corte; `0` en caso contrario. |
| `n_ordenes` | `numeric (long)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Total de órdenes de compra emitidas para el contratista en la ventana del corte. |
| `monto_total_soles` | `numeric (double)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Monto acumulado en nuevos soles. Excluye órdenes en estado `"Anulada"`. |
| `monto_mediano_soles` | `numeric (double)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Mediana del valor de las órdenes individuales adjudicadas. |
| `monto_maximo_soles` | `numeric (double)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Valor máximo obtenido en un único contrato u orden de compra. |
| `n_entidades_distintas` | `numeric (int)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Diversificación de compradores públicos: conteo de entidades estatales distintas que contrataron a la empresa. |
| `pct_monto_en_entidad_principal` | `numeric (double)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Concentración de facturación estatal: $\frac{\text{monto max entidad}}{\text{monto total}}$. Captura dependencia extrema o direccionamiento. |
| `pct_ordenes_anuladas` | `numeric (double)` | SEACE | $\le$ 30-jun-2025 | Imputa `0.0` si no contrata | Bajo | V1, V2 | Proporción de contratos cancelados: $\frac{\text{órdenes anuladas}}{\text{total órdenes}}$. |
| `antiguedad_contratacion_estado_dias` | `numeric (int)` | SEACE | $\le$ 30-jun-2025 | Imputación mediana + bandera `_es_nulo` | Bajo | V1, V2 | Días entre la emisión de la primera orden registrada y el 30 de junio de 2025. |
| `monto_por_trabajador` | `numeric (double)` | Cruzada | $\le$ 30-jun-2025 | Nulo si trabajadores es 0 o nulo (+ bandera) | Bajo | V1, V2 | Facturación dividida por personal registrado: $\frac{\text{monto total}}{\text{nro trabajadores}}$. Señal de alerta de desproporción operativa. |
| `informalidad_epen_departamento` | `numeric (double)` | INEI EPEN | 2025 | Imputación mediana nacional | Bajo | V1, V2 | Tasa regional de informalidad laboral departamental del entorno de la empresa. |
| `Estado` | `categorical (string)` | Padrón RUC | Snapshot 202506 | Imputa `"DESCONOCIDO"` | **ALTO** | **Solo V1** | Condición registral de vigencia (e.g., ACTIVO, BAJA, SUSPENSION). |
| `Condicion` | `categorical (string)` | Padrón RUC | Snapshot 202506 | Imputa `"DESCONOCIDO"` | **ALTO** | **Solo V1** | Condición de localización de domicilio (e.g., HABIDO, NO HABIDO, NO HALLADO). |

---

## 3. Tratamiento de Faltantes y Banderas de Nulo

Para variables numéricas donde la ausencia de valor posee significado semántico de negocio (por ejemplo, contribuyentes que nunca han contratado con el Estado, o empresas que no reportaron trabajadores):
1. **Bandera indicadora binaria:** Se genera la columna `<variable>_es_nulo` (valor 1 si era NULL, 0 en caso contrario) mediante `SQLTransformer`.
2. **Imputación de mediana en pipeline:** Se imputa con la mediana estimada calculada **exclusivamente con los datos de entrenamiento** (`train`), asegurando la ausencia total de sesgo de fuga hacia el conjunto `test`.
