# Dashboard de QuickSight: SSCO

Guía para crear y mantener el dashboard de riesgo SSCO en Amazon QuickSight. El
dashboard lee vistas de Athena sobre el catálogo `sunat_ssco` y muestra los
resultados de los modelos ya entrenados. No entrena ni recalcula nada.

## 1. Resumen

El dashboard tiene dos hojas:

| Hoja | Fuente | Para qué sirve |
|---|---|---|
| **Panorama regional** | `dash_regional` | Vista por departamento: informalidad, recaudación, concentración PRICOS y score del modelo. |
| **Ranking de fiscalización** | `dash_ranking`, `dash_score_distribucion` | Top 1000 RUC por score en el split elegido, filtros de actividad y distribución de scores por clase. |

## 2. Arquitectura

```
Gold (S3 sunat-risk-scoring-gold)
  -> Glue crawlers (catálogo sunat_ssco)
  -> Athena: vistas dash_regional, dash_ranking, dash_score_distribucion
  -> QuickSight: 3 datasets SPICE (refresco diario)
  -> Hojas "Panorama regional" y "Ranking de fiscalización"
```

Las vistas se crean una sola vez. Los datasets SPICE copian los datos de las
vistas; QuickSight no consulta Athena en cada visita.

## 3. Requisitos previos

### 3.1 Suscripción a QuickSight (manual)

- Edición **Enterprise**, región **us-east-1**.
- La suscripción se hace desde la consola de QuickSight. No está en Terraform.
- Antes de suscribir, revisar en la consola de AWS el costo estimado frente al
  presupuesto `sunat-ssco-monthly`. Referencia: unos 40 USD al mes con 1 autor y
  5 lectores.
- Confirmar que la edición soporta el mapa por departamentos de Perú. Si no lo
  soporta, usar barras o tabla en la hoja regional.

### 3.2 Rol IAM `sunat-ssco-quicksight-role` (Terraform)

El rol es nuevo y lo define Terraform en `infra/services`:

- Trust: `quicksight.amazonaws.com`.
- Política inline: `quicksight-athena-glue-s3-read`.
- Output: `quicksight_role_arn`.

Lo aplica el usuario, con el perfil `bigdata`, **antes** de configurar QuickSight:

```bash
cd infra/services
AWS_PROFILE=bigdata terraform apply
AWS_PROFILE=bigdata terraform output quicksight_role_arn
```

El rol se usa solo si el data source de Athena se crea con `RoleArn`. Si no se
indica, QuickSight usa el rol de servicio por defecto `aws-quicksight-service-role-v0`.
Para que el dashboard use el rol del proyecto, elegirlo al crear el data source.

Permisos del rol:

- Lectura S3 solo en los prefijos `regional_summary`, `ruc_features`,
  `model_outputs/predictions`, `model_runs` y `model_metrics` del bucket
  `sunat-risk-scoring-gold`. Nunca `scoring_dataset`.
- Lectura y escritura solo en `s3://sunat-risk-scoring-artifacts/athena-results/`.
- Uso del workgroup Athena `sunat-ssco` y lectura del catálogo Glue `sunat_ssco`.

## 4. Crear las vistas (una sola vez)

Las vistas están en `src/analysis/queries/`:

- `dash_regional.sql`
- `dash_ranking.sql`
- `dash_score_distribucion.sql`

Antes de crearlas, validar las columnas y tipos de las tablas base:

```sql
DESCRIBE sunat_ssco.model_metrics;
DESCRIBE sunat_ssco.gold_ruc_features;
```

Después ejecutar los tres archivos con el script de Athena. El script no fija
el perfil AWS, así que hay que indicarlo:

```bash
AWS_PROFILE=bigdata ATHENA_WORKGROUP=sunat-ssco GLUE_DATABASE=sunat_ssco \
  ./src/analysis/queries/08_run_athena_query.sh \
  src/analysis/queries/dash_regional.sql \
  src/analysis/queries/dash_ranking.sql \
  src/analysis/queries/dash_score_distribucion.sql
```

Aviso: el script descarga el resultado como CSV local en `data/results/`. Por
eso solo se usa para `CREATE VIEW` o consultas de verificación como `COUNT(*)` y
`SHOW VIEWS`. No ejecutarlo con `SELECT` sobre `dash_ranking` ni sobre tablas
con RUC, porque el CSV quedaría en la máquina local. En un `CREATE VIEW` ese CSV
no tiene datos y la descarga puede fallar aunque la vista ya exista. Si falla,
comprobar la creación directamente:

```sql
SHOW VIEWS IN sunat_ssco;
SELECT COUNT(*) FROM sunat_ssco.dash_ranking;
```

Las vistas se recrean con `CREATE OR REPLACE VIEW`, así que volver a ejecutar
el script es seguro.

## 5. Data source y datasets SPICE

### 5.1 Data source

- Tipo: Amazon Athena.
- Workgroup: `sunat-ssco`.
- Base: `sunat_ssco`.
- Rol: `sunat-ssco-quicksight-role` (ver 3.2).

### 5.2 Datasets (3, SPICE, refresco diario)

Los datasets SPICE se crean **únicamente** sobre las vistas `dash_regional`,
`dash_ranking` y `dash_score_distribucion`. Nunca sobre tablas base `gold_*`:
el rol IAM podría leerlas y `gold_ruc_features` contiene columnas que las vistas
excluyen a propósito (por ejemplo `ruc_correlativo`).

| Dataset | Vista | Columnas (lista exacta de la vista) |
|---|---|---|
| `ds_regional` | `dash_regional` | departamento, ruc_activos, pct_informalidad, recaudacion_soles, concentracion_pricos, pct_pricos_sobre_activos, run_id, model_name, variant, dataset_version, experimento, es_ultima_corrida, split, n_ruc_scoreados, n_ssco_reales, score_promedio, n_en_top_1pct, pct_top_1pct_del_depto |
| `ds_ranking` | `dash_ranking` | run_id, model_name, variant, dataset_version, experimento, es_ultima_corrida, split, ruc, score, label, rank_en_split, percentil_rank, departamento, contrata_con_estado, tipo_contribuyente, ciiu_principal, nro_trabajadores, n_ordenes, monto_total_soles, n_entidades_distintas, es_prico, see_afiliado, antiguedad_contratacion_estado_dias, see_antiguedad_dias, see_n_tipos, cambio_estado, cambio_tipo_facturacion, meses_hueco, nuevo_en_periodo, meses_en_padron |
| `ds_score_distribucion` | `dash_score_distribucion` | run_id, model_name, variant, dataset_version, experimento, es_ultima_corrida, split, label, bin_idx, bin_inicio, bin_fin, n_ruc, score_promedio, pct_en_bin, pr_auc, roc_auc |

Reglas de agregación y nulos:

- `dash_regional` es un snapshot que se repite por `run_id` y `split`. Agregar
  con `MAX` o `AVG`, nunca con `SUM`, o las cifras se multiplican.
- En `dash_regional`, `experimento` y `es_ultima_corrida` son `NULL` para los
  departamentos sin predicciones.
- `pr_auc` y `roc_auc` solo vienen informadas en `split = 'test'`.

Refresco: programar el refresco diario de los tres datasets en SPICE.

## 6. Hojas

### 6.1 Panorama regional

Fuente: `ds_regional`. Mapa o barras por departamento con score promedio,
informalidad y concentración PRICOS. Tabla con el detalle por departamento.

Los departamentos sin predicciones tienen `experimento`, `es_ultima_corrida` y
`dataset_version` en `NULL`. Por eso los filtros de `experimento`,
`dataset_version`, `es_ultima_corrida` y `split` deben aplicarse solo a los
visuales de modelo (`n_ruc_scoreados`, `score_promedio`, `n_en_top_1pct`), no a
los de contexto regional (`ruc_activos`, `pct_informalidad`,
`recaudacion_soles`, `concentracion_pricos`). Así se ven todos los departamentos.

### 6.2 Ranking de fiscalización

Fuentes: `ds_ranking` y `ds_score_distribucion`.

- Tabla de los top 1000 RUC por `rank_en_split`, con `ruc` completo y las
  columnas de contexto.
- Histograma de `pct_en_bin` por `label` a partir de `ds_score_distribucion`.
- Métricas `pr_auc` y `roc_auc` del run seleccionado.
- Filtros y orden por señales de actividad (ver 7.4). Cambian la vista de la
  tabla, no el score del modelo.

## 7. Controles

### 7.1 Modelo por defecto: E5 (`experimento` y `es_ultima_corrida`)

- El modelo por defecto es **E5** (LR V2 sin reponderación), el modelo base de
  trabajo actual.
- Filtros sobre `ds_regional`, `ds_ranking` y `ds_score_distribucion`:
  - `experimento = 'E5'`: **fijo** en el análisis compartido. Los lectores no
    pueden quitarlo ni cambiarlo: en QuickSight es un filtro con valor fijo y sin
    control visible. Motivo: quitarlo permitiría ver otros modelos, en particular
    las corridas V1 (que incluyen `Estado`/`Condicion` y son solo un techo de
    referencia, ver 7.6), que no son resultados válidos. Comparar otros
    experimentos o variantes se hace solo en un análisis aparte de su autor, sin
    compartir.
  - `es_ultima_corrida = 1`: control con valor por defecto 1.
- Con esto el dashboard sigue automáticamente la última corrida de E5 del
  `dataset_version` elegido, sin depender de un `run_id` ni de un tag.
- Filtrar por la columna `experimento`, no por texto del `run_id`.
- `es_ultima_corrida` se define en la vista: vale 1 para la corrida con la
  `fecha` más reciente dentro de cada par (`experimento`, `dataset_version`).
- Para comparar corridas concretas se quita el filtro `es_ultima_corrida` (o se
  deja en todas las corridas) y se usa el selector `run_id` (ver 7.3).

Por qué existe `es_ultima_corrida`: el `run_id` tiene el formato
`ssco-run-<ID>-<modelo>-<variante>-w1|w0[-<tag>]`, donde `<ID>` es el id del
experimento (E5). Cuando no se pasa `run_tag`, el workflow usa `github.run_id`
como tag (el sufijo del `run_id`), así que cada ejecución crea un `run_id`
nuevo. Repetir el mismo tag sobrescribe `predictions`, `model_runs` y
`model_metrics`, salvo que se use `--reanudar si`, que omite lo ya hecho. Por
eso no conviene fijar un `run_id` en el dashboard.

### 7.2 `dataset_version` (filtro obligatorio)

- `dataset_version` es el corte temporal. E5 tiene corridas en `v2` y en `v3`,
  que son corridas distintas.
- Es un filtro obligatorio de **un solo valor** (sin opción "todos"), porque
  `es_ultima_corrida` se calcula por (`experimento`, `dataset_version`) y mezclar
  cortes confundiría el ranking.
- Valores: `v1` = snapshot 202506, `v2` = 202512, `v3` = 202603.
- No hay un valor por defecto fijado en esta guía: el corte se elige según la
  comparación que se quiera hacer. `dataset_version` debe quedar fijado en un
  único valor.

### 7.3 `run_id` (selector)

- Control de lista desplegable sobre los `run_id` de `model_runs` filtrados por
  `experimento` y `dataset_version`.
- Sirve para comparar corridas concretas de E5: quitar el filtro
  `es_ultima_corrida` (o dejarlo en todas las corridas) y elegir el `run_id`. El
  filtro `experimento = 'E5'` se mantiene fijo (ver 7.1).

### 7.4 Filtros de actividad (hoja de ranking)

Todos los RUC puntuados ya son afiliados SEE y activos, porque así lo filtra el
job 06. Por eso `see_afiliado` es casi siempre 1 y **no sirve para priorizar**.
La actividad se mide con las siguientes columnas:

- `n_ordenes`, `monto_total_soles`, `contrata_con_estado`: actividad con el
  Estado (órdenes de compra).
- `see_antiguedad_dias`, `see_n_tipos`: antigüedad y variedad de comprobantes
  en SEE.
- `cambio_estado`, `cambio_tipo_facturacion`, `meses_hueco`,
  `nuevo_en_periodo`, `meses_en_padron`: movimientos del padrón mensual
  (ene-2025 a mar-2026) ya resumidos. Son flags y contadores acumulados al
  corte, no series mensuales.

Controles propuestos para ordenar y filtrar la tabla de ranking, sin cambiar el
score:

- Orden secundario por `monto_total_soles` o `n_ordenes`.
- Filtro `contrata_con_estado >= 1` y filtro de `monto_total_soles` mínimo.
- Filtro `cambio_estado = 1` o `meses_hueco > 0` para ver RUC con movimientos
  en el padrón.
- Filtro `nuevo_en_periodo = 1`.
- Filtro sobre `see_antiguedad_dias` (menor antigüedad primero).

### 7.5 `split` (filtro, crítico)

- Valor por defecto: `split = 'test'` en todas las hojas y datasets.
- `train` tiene scores inflados porque el modelo se ajustó sobre esos datos.
  Mostrarlo en rankings o métricas da una imagen falsa del riesgo.
- No mezclar `train` en rankings ni métricas. Si se muestra, rotularlo como
  "ajuste (inflado)".

### 7.6 `variant`

- `variant` es el conjunto de features: `V1` o `V2`. `V1` incluye `Estado` y
  `Condicion` solo como referencia de techo; `V2` no tiene fuga y es el oficial.
- No confundir `variant` con `dataset_version`: uno cambia las features, el otro
  cambia la fecha de corte de los datos.

## 8. Notas de interpretación

- `antiguedad_contratacion_estado_dias` es un proxy: mide desde la primera orden
  de compra, no la antigüedad real del RUC. El Padrón no tiene fecha de inscripción.
- El score es una propensión para priorizar fiscalización, no una sentencia sobre
  un contribuyente.

## 9. Compartición y gobernanza

- Compartir el dashboard con el rol **Reader** (solo lectura).
- Deshabilitar la exportación de datos (CSV y Excel) desde QuickSight.
- No compartir el dashboard fuera del equipo.
- **Decisión tomada por el usuario:** la hoja de ranking muestra el `ruc`
  completo. Riesgo aceptado: el `ruc` con prefijo 10 corresponde al DNI de
  personas naturales, y se expone a los lectores del dashboard.
- `doc_representante` no se expone en ninguna vista.

## 10. Actualización después de cada corrida

El dashboard no ve corridas nuevas hasta completar estos pasos:

1. Ejecutar el workflow manual `collect-training-results.yml` con el `run_tag` de
   la corrida.
2. Refrescar los crawlers `sunat-ssco-predictions-crawler`,
   `sunat-ssco-model-runs-crawler` y `sunat-ssco-model-metrics-crawler`. Si
   cambió la capa Gold, refrescar también `sunat-ssco-gold-crawler`
   (`regional_summary` y `ruc_features`).
3. Refrescar los datasets SPICE en QuickSight.

Si una hoja aparece vacía, revisar primero que los crawlers hayan terminado y
que exista una corrida de E5 para el `dataset_version` elegido en `model_runs`.

## 11. Trabajo futuro

- Features nuevas de movimientos (conteos y fechas de cambios por mes, de
  ene-2025 a mar-2026) requieren cambios en el job 03 y reentrenamiento.
- Quedan fuera del alcance de este dashboard, que no cambia el score del modelo.

## 12. Lista de verificación

- [ ] Suscripción Enterprise en us-east-1 y costo revisado frente a `sunat-ssco-monthly`.
- [ ] `terraform apply` en `infra/services` con `AWS_PROFILE=bigdata` y rol creado.
- [ ] Vistas creadas (`SHOW VIEWS`) y `DESCRIBE` validado.
- [ ] Data source Athena con workgroup `sunat-ssco` y rol `sunat-ssco-quicksight-role`.
- [ ] Tres datasets SPICE con refresco diario y solo sobre las vistas `dash_*`.
- [ ] Filtro `experimento = 'E5'` fijo (sin control para lectores), control `es_ultima_corrida = 1`, filtro `dataset_version` fijado en un único valor y filtro `split = 'test'`.
- [ ] Compartición en modo Reader y exportación deshabilitada.
