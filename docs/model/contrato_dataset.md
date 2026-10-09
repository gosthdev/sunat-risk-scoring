# Contrato C1 — Especificación de `model_inputs`

## 1. Identificación y Metadatos del Contrato
- **ID del Contrato:** C1 (`model_inputs`)
- **Productor:** Desarrollador A (`src/spark/jobs/06_dataset_curado.py`)
- **Consumidores:** Desarrollador A (`src/spark/jobs/07_train_model.py`), Desarrollador B (Validación B1 con Athena)
- **Ubicación en Data Lake:** `s3://<gold-bucket>/gold/model_inputs/dataset_version=<v>/`
- **Formato:** Apache Parquet particionado por `dataset_version` (ej. `dataset_version=v1`)
- **Grano del dataset:** 1 fila = 1 RUC (exactamente una fila por contribuyente, sin duplicados ni particiones temporales dentro del dataset curado)
- **Llave primaria:** `ruc` (string de 11 caracteres numéricos, sin nulos ni espacios)

---

## 2. Definición del Esquema

| Columna | Tipo PySpark | Nulos permitidos | Descripción y Reglas de Negocio |
| :--- | :--- | :--- | :--- |
| `ruc` | `StringType` | NUNCA | Identificador tributario (RUC de 11 dígitos). Llave primaria única. |
| `label` | `IntegerType` | NUNCA | Etiqueta binaria de riesgo SSCO: `1` si el RUC pertenece al registro oficial de Sujetos sin Capacidad Operativa (SUNAT, corte 30-sep-2026); `0` si no pertenece. |
| `split` | `StringType` | NUNCA | Segmento de partición: `"train"` (80% estratificado) o `"test"` (20% estratificado bajo llave). |
| `fold` | `IntegerType` | Solo en `test` | Identificador de partición de validación cruzada: `0`, `1`, `2`, `3` o `4` para filas donde `split == "train"`. Valor `NULL` estricto para filas donde `split == "test"`. |
| `departamento` | `StringType` | NO (imputa "DESCONOCIDO") | Departamento normalizado según estándar UBIGEO / `region_normalizer`. |
| `ciiu_principal` | `StringType` | NO (imputa "OTROS") | Código CIIU revisión 4 principal, restringido al top-30 más frecuente de `train` o agrupado en `"OTROS"`. |
| `tipo_contribuyente` | `StringType` | NO (imputa "DESCONOCIDO") | Tipo de contribuyente registral (Persona Jurídica, Persona Natural, etc.). |
| `nro_trabajadores` | `DoubleType` | SÍ | Número declarado de trabajadores. Si es nulo o 0, se mantiene para tratamiento en pipeline MLlib. |
| `sin_trabajadores` | `IntegerType` | NUNCA | Bandera indicadora: `1` si `nro_trabajadores IS NULL` o `nro_trabajadores <= 0`; `0` si tiene al menos 1 trabajador. |
| `n_actividades` | `IntegerType` | NUNCA | Número de actividades económicas registradas (mínimo 1). |
| `contrata_con_estado` | `IntegerType` | NUNCA | Bandera binaria: `1` si el RUC registra al menos una orden de compra en el corte temporal; `0` en caso contrario. |
| `n_ordenes` | `LongType` | SÍ | Cantidad total de órdenes de compra emitidas en el corte temporal. |
| `monto_total_soles` | `DoubleType` | SÍ | Suma total de órdenes de compra en moneda nacional (excluye órdenes en estado `"Anulada"`). |
| `monto_mediano_soles`| `DoubleType` | SÍ | Mediana de montos adjudicados en órdenes de compra del corte temporal. |
| `monto_maximo_soles` | `DoubleType` | SÍ | Monto máximo adjudicado en una orden singular durante el corte. |
| `n_entidades_distintas`| `IntegerType` | SÍ | Cantidad de entidades compradoras del Estado distintas que adjudicaron órdenes al RUC. |
| `pct_monto_en_entidad_principal` | `DoubleType` | SÍ | Concentración: monto adjudicado por la entidad principal / `monto_total_soles`. |
| `pct_ordenes_anuladas`| `DoubleType` | SÍ | Proporción de órdenes adjudicadas con estado `"Anulada"` sobre el total de órdenes recibidas. |
| `antiguedad_contratacion_estado_dias` | `IntegerType` | SÍ | Días transcurridos desde la fecha de emisión de la primera orden registrada hasta la fecha de corte. |
| `monto_por_trabajador`| `DoubleType` | SÍ | Ratio: `monto_total_soles / nro_trabajadores`. Nulo si no tiene trabajadores declarados. |
| `informalidad_epen_departamento` | `DoubleType` | SÍ | Tasa regional de informalidad laboral obtenida de la encuesta EPEN 2025. |
| `Estado` | `StringType` | SÍ | Estado registral (ACTIVO, BAJA, etc.). **Solo presente en Variante V1 (Auditoría de Fuga).** |
| `Condicion` | `StringType` | SÍ | Condición de domicilio fiscal (HABIDO, NO HABIDO, etc.). **Solo presente en Variante V1.** |

---

## 3. Invariantes y Reglas Estrictas de Calidad

1. **Unicidad de Clave:** Todo RUC aparece una y solo una vez en la tabla:
   $$\text{count(distinct ruc)} == \text{count(*)}$$
2. **Exclusión Estricta de PRICOS:**
   Ningún RUC perteneciente a `silver/pricos` puede existir en `model_inputs`. La pertenencia a PRICOS se evalúa mediante anti-join estricto.
3. **Población Objetivo:**
   Todo el Padrón RUC correspondiente al corte temporal menos PRICOS. Contratar con el Estado **no** es filtro de exclusión de población; es una feature cuantitativa (`contrata_con_estado`, `n_ordenes`).
4. **Corte Temporal Determinado (Decisión D3):**
   - Para `dataset_version = v1` (oficial): Padrón Snapshot `202506` (junio 2025) y Órdenes de Compra con `fecha_de_emision <= '2025-06-30'`.
   - Horizonte de predicción respecto al label: ~15 meses (junio 2025 vs. septiembre 2026).
5. **Estratificación del Split:**
   - Proporción global: 80% `train`, 20% `test`.
   - La prevalencia de positivos ($\frac{\text{label}=1}{\text{total}}$) en `train` y `test` no debe diferir en más de un 10% relativo.
   - La partición es determinista mediante hash determinista del RUC con semilla $42$.
6. **Validación Cruzada (K-Folds):**
   - 5 folds numerados del `0` al `4` exclusivamente en `split == "train"`.
   - Cada fold debe contener al menos 20 ejemplos positivos de SSCO.
   - En `split == "test"`, la columna `fold` es obligatoriamente `NULL`.
7. **Privacidad de Datos (Decisión D15):**
   - Prohibida la inclusión de Razones Sociales, Nombres de Representantes, Direcciones o DNI de personas naturales. Únicamente se almacena el RUC de la persona jurídica o entidad evaluada.
