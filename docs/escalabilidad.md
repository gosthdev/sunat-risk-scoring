# Prueba de escalabilidad (3.7)

Se replicó el dataset real a 5× y 10× y se corrió el pipeline (jobs 01, 02, 04, 03 y 05)
en EMR Serverless sobre cada volumen. Se midieron tiempos y recursos. La prueba responde
dos preguntas:

- **A.** ¿El tiempo crece en proporción al volumen o se degrada?
- **B.** ¿En qué punto deja de alcanzar una sola máquina?

Todas las cifras de este documento vienen de corridas reales. Cada una tiene su Job Run ID
de EMR y su JSON en [`perf/results/runs/`](../perf/results/runs/).

## 1. Cómo se hizo

### 1.1 Datos

El **1×** es una copia byte a byte del RAW real de producción, hecha dentro de AWS. Producción
solo se leyó. El RAW tiene dos fuentes grandes:

| Fuente | Contenido | Tamaño |
| :--- | :--- | ---: |
| Padrón RUC | 15 fotos mensuales (2025-01 a 2026-03), de 12,4 a 13,2 M de RUC cada una; 191.408.828 filas y 13.761.829 RUC distintos | 50,36 GB |
| Órdenes de compra | 4.952.097 órdenes | 2,49 GB |
| **Total** | | **52,85 GB** |

Antes de replicar se verificó que el 1× reproduce producción
([`g1x.json`](../perf/results/g1x.json)). Los conteos de Bronze, Silver y Gold son iguales.
El cruce con el dataset del modelo también cuadra: Gold 202506 tiene 12.613.407 filas, que
menos 13.409 PRICOS dan los 12.599.998 RUC del modelo. La línea base completa está en
[`s0_baseline.json`](../perf/results/s0_baseline.json).

### 1.2 Replicación

El **k×** se arma con la copia real (copia 0) más k−1 copias con claves nuevas. El job
[`s_replicate_dataset.py`](../perf/spark/s_replicate_dataset.py) las genera en EMR.

- **Claves de las copias.** Cada copia r ≥ 1 recibe RUC nuevos de 11 dígitos a partir de
  21·10⁹, en un bloque reservado por copia ([`perf_keys.py`](../perf/spark/perf_keys.py)).
  Los RUC reales son menores que 21·10⁹, así que las copias no chocan con los reales ni entre
  sí. Además siguen siendo válidos para Silver.
- **Órdenes.** Se desplaza el número de orden y se remapean los RUC de entidad y contratista.
- **Lo que no se replica.** Las 1.525 filas que el Job 01 manda a cuarentena por esquema
  quedan solo en la copia 0, para no multiplicar un defecto de la fuente.

Resultado: los conteos de cada capa son exactamente k veces los del 1×.

| Copia | RAW en el bucket | Job Run ID | Tiempo | Costo |
| :--- | ---: | :--- | ---: | ---: |
| 1× | 52,84 GB | — (copia S3 → S3) | — | — |
| 5× | 263,04 GB | `00gak1s94ucme80b` | 587 s | US$ 0,32 |
| 10× | 525,78 GB | `00gak3pq34f3ro0b` | 1.065 s | US$ 0,59 |

| Conteo | 1× | 5× | 10× |
| :--- | ---: | ---: | ---: |
| Bronze Padrón | 191.408.828 | 957.044.140 | 1.914.088.280 |
| Bronze Órdenes | 4.952.097 | 24.760.485 | 49.520.970 |
| Silver Órdenes | 3.124.332 | 15.621.660 | 31.243.320 |
| Gold `ruc_features` y `scoring_dataset` | 38.737.472 | 193.687.360 | — (el 10× no llegó) |

### 1.3 Entorno y perfiles

Todo corrió en una infraestructura de prueba separada, creada con
[`infra_perf.sh`](../perf/scripts/infra_perf.sh):

- buckets `sunat-risk-scoring-perf-x1`, `-perf-x5` y `-perf-x10`;
- un rol de IAM que solo lee el RAW de producción y solo escribe en los buckets `-perf-`;
- dos aplicaciones EMR Serverless (emr-7.13.0, Spark 3.5.6).

Se usó el script de envío del pipeline real ([`05_submit_spark_steps.sh`](../src/spark/jobs/05_submit_spark_steps.sh))
y solo se cambiaron los buckets de destino.

| Perfil | Qué es |
| :--- | :--- |
| **Base** | Pipeline v1 tal como estaba desplegado: de 2 a 6 executors de 4 vCPU / 14 GB (+2 GB de overhead) / 20 GB de disco cada uno, y 64 particiones de shuffle |
| **Base-v2** | Pipeline v2 (sección 3): de 2 a 16 executors, según el volumen del RAW; particiones de shuffle según el volumen |
| **C1** | "Una sola máquina": el driver más **1 executor** de 4 vCPU / 16 GB / 20 GB |

Se midió el tiempo de ejecución que reporta EMR. Recursos y costo salen de lo que EMR factura
(vCPU·h y GB·h), con las tarifas de us-east-1. Shuffle, spill, memoria y GC salen del event
log de Spark ([`collect_runs.py`](../perf/analysis/collect_runs.py)).

## 2. Resultados con el pipeline original (v1)

| Corrida | Factor | Perfil | Resultado | Ejecución EMR | Costo | Shuffle | Spill a disco |
| :--- | ---: | :--- | :--- | ---: | ---: | ---: | ---: |
| S-01-r1 | 1× | Base | completa | 30,0 min | US$ 0,90 | 55,6 GB | 6,6 GB |
| S-01-r2 | 1× | Base | completa | 29,9 min | US$ 0,90 | 55,6 GB | 7,0 GB |
| S-02-r1 | 1× | C1 | **falla** en 02 | 32,8 min | US$ 0,33 | 15,6 GB | 7,6 GB |
| S-05-r1 | 5× | Base | **falla** en 02 | 28,2 min | US$ 0,93 | 85,3 GB | 40,7 GB |
| S-03-r1 | 10× | Base | **falla** en 02 | 43,4 min | US$ 1,46 | 83,4 GB | 65,1 GB |

- **El 1× es estable.** Las dos repeticiones difieren en menos de 0,5 %.
- **Las tres fallas tienen la misma causa: el disco local de los executors se llena en el paso
  02** (`No space left on device`). El paso 02 deduplica el Padrón y escribe Silver.
  - C1: 15,6 GB de shuffle más 7,6 GB de spill, contra los 20 GB de disco de su único executor.
  - 5× y 10×: con 6 executors hay 120 GB de disco, y el shuffle más el spill los superan.

## 3. Ajustes del pipeline (v2)

El objetivo fue que el pipeline procese el 5× completo **escalando horizontalmente**, es decir
con más executors y no con más disco por executor. Hubo tres cambios:

1. **Executors según el volumen.** `maxExecutors = ceil(GB del RAW / 9)`, entre 2 y 16
   (`05_submit_spark_steps.sh`). Los GB del RAW son el Padrón más Órdenes, medidos en S3 al
   lanzar. El 9 conserva la proporción del 1× (53 GB con 6 executors). Resultados: 1× → 6,
   5× → 16 (techo) y 10× → 16 (techo). La capacidad máxima de la aplicación EMR sube de 32 a
   **68 vCPU** (16 executors más el driver, de 4 vCPU cada uno) y a 272 GB.
2. **Particiones de shuffle según el volumen.**
   - Fórmula: `spark.sql.shuffle.partitions = max(64, GB del RAW × 4)`. Se calcula en el envío,
     y la sesión ya no la fija. Resultados: 1× → 212, 5× → 1.056 y 10× → 2.104.
   - El factor 4 sale del shuffle medido por GB de RAW: 0,41 GB en el paso 02 y 0,63 GB en el
     paso 03. Con 4 particiones por GB, cada partición queda en 100–160 MB.
   - AQE (`advisoryPartitionSizeInBytes=128m`, `parallelismFirst=false`) junta las particiones.
   - En el paso 02 el número de particiones es el final. Ese paso cachea su entrada (`persist`),
     y AQE no junta particiones de una tabla cacheada.
3. **`repartition` antes del deduplicado del Padrón.** En `preparar_padron_ruc` se agrega
   `repartition(RUC, PERIODO_PUBLICACION)` antes del `dropDuplicates`. Así se evita que el
   combiner haga un deduplicado parcial antes del shuffle: en el Padrón no sirve, porque casi
   no trae duplicados, y solo ocupa memoria.

## 4. Resultados con el pipeline ajustado (v2)

| Corrida | Factor | Resultado | Ejecución EMR | Costo | Executors | Shuffle | Spill a disco |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| S-06-r2 | 1× | completa | 34,5 min | US$ 1,08 | 6 | 56,1 GB | 15,4 GB |
| S-07-r1 | 5× | **completa** | 63,7 min | US$ 4,87 | 16 | 282,7 GB | 90,7 GB |
| S-08-r1 | 10× | **falla** en 03 (disco lleno) | 261,4 min | US$ 21,89 | 16 | 376,3 GB | 178,6 GB |

Tiempo de ejecución por paso:

| Paso | 1× | 5× | 10× | 5× / 1× | 10× / 5× |
| :--- | ---: | ---: | ---: | ---: | ---: |
| 01 Bronze | 313 s | 492 s | 800 s | 1,57 | 1,63 |
| 02 Silver | 787 s | 1.638 s | 3.068 s | 2,08 | 1,87 |
| 04 Gold regional | 122 s | 200 s | 274 s | 1,64 | 1,37 |
| 03 Gold features | 684 s | 1.265 s | falla (disco lleno) | 1,85 | — |
| 05 Scoring dataset | 165 s | 227 s | no se lanzó | 1,38 | — |

- **El 5× pasa.** Los 5 pasos terminan, y los conteos de todas las capas son exactamente 5
  veces los del 1×.
- **En el 10× el paso 02 pasa y falla el paso 03.** El paso 03 escribió 181 GB de shuffle en
  el 5×; en el 10× llenó el disco (`No space left on device`) tras escribir 171 GB.

## 5. Respuestas

Indicadores (calculados con [`analyze_scaling.py`](../perf/analysis/analyze_scaling.py)):

- **Eficiencia, E(V) = t(1×)·V / t(V).** Vale 1 si el tiempo crece igual que el volumen; si
  es mayor que 1, crece menos.
- **Exponente, b = log(t_b / t_a) / log(V_b / V_a).** Vale 1 si el crecimiento es proporcional;
  si es mayor que 1, el pipeline se degrada.

### A. ¿El tiempo crece en proporción o se degrada?

**Pipeline original:** no escala más allá del 1×.

- El paso 01 crece menos que el volumen: b = 0,76 entre 1× y 10×.
- El paso 02 falla por disco en el 5× y en el 10×. Con 64 particiones fijas y un máximo de 6
  executors, cada partición crece con los datos hasta que el spill llena el disco local.
- No hay un tiempo que medir: el pipeline deja de funcionar entre 1× y 5×.

**Pipeline ajustado:**

| Tramo | Executors | Tiempo total | Comportamiento |
| :--- | :--- | :--- | :--- |
| 1× → 5× | 6 → 16 | 34,5 → 63,7 min; E = 2,71, b = 0,38 | Crece menos que el volumen, porque se suman executors |
| 5× → 10× | 16 → 16 (techo) | paso 01: b = 0,70; paso 02: b = 0,90 | Casi proporcional: sin más executors, el doble de datos toma casi el doble de tiempo |
| 10× | 16 | — | El paso 03 necesita más disco local del que suman 16 executors (320 GB) y falla |

El tiempo no se degrada (b no supera 1) mientras el pipeline completa. El límite no es de
tiempo sino de **disco local**: con el techo de 16 executors está entre 5× y 10×, en el
paso 03.

### B. ¿En qué punto deja de alcanzar una sola máquina?

**Antes del volumen real.** Con un solo executor (perfil C1, corrida S-02-r1,
Job Run ID `00gagn72bag4a00b`):

- el paso 01 completa el 1×, pero tarda 1.256 s, contra 316 s en Base;
- el paso 02 falla a los 713 s por disco lleno: 15,6 GB de shuffle más 7,6 GB de spill no
  caben en los 20 GB de disco local.

Una máquina con la configuración por defecto de EMR Serverless no procesa ni el 1×, así que el
pipeline necesita varias desde el volumen actual.

## 6. Lo que no se ejecutó y por qué

- **100×.** No se corrió. El pipeline ajustado, con su techo de
  16 executors, falla en el 10× por disco en el paso 03. El 100× fallaría en el mismo punto,
  pero más temprano y a mayor costo, así que no agregaría información.
- **Job 06 (dataset curado del modelo).** Queda fuera de la prueba:
  - Depende de las etiquetas SSCO: hay 668 positivos, y en un k× solo estarían en la copia 0.
  - El job exige al menos 20 positivos por fold.
  - En las copias fallaría por diseño de los datos, no por volumen.

## 7. Gasto

| Concepto | Costo |
| :--- | ---: |
| Corridas v1 (S-01-r1, S-01-r2, S-02-r1, S-03-r1, S-05-r1) | US$ 4,52 |
| Pruebas de los ajustes (S-06-r1 y A/B del `repartition`) | US$ 4,15 |
| Corridas v2 (S-06-r2, S-07-r1, S-08-r1) | US$ 27,84 |
| Replicaciones 5× y 10× (cada factor se replicó dos veces, una para v1 y otra para v2) | ≈ US$ 1,8 |
| **Total medido en EMR** | **≈ US$ 38** |

El paso 03 del 10× costó US$ 16,33, de los cuales casi todo se fue en las 3,2 horas de
reintentos con el disco lleno.

## 8. Cómo reproducirla

```bash
export AWS_PROFILE=<perfil>
perf/scripts/infra_perf.sh crear                    # buckets -perf-, rol y apps EMR
perf/scripts/replicar.sh 1                          # copia 0 (RAW real) en perf-x1
perf/scripts/correr_pipeline.sh 1 Base S-06-r2      # pipeline sobre el 1×
perf/scripts/replicar.sh 5                          # 5× en perf-x5
perf/scripts/correr_pipeline.sh 5 Base S-07-r1
python perf/analysis/analyze_scaling.py             # tablas a partir de los JSON
perf/scripts/infra_perf.sh destruir --yes           # borra todo lo de prueba
```

## Anexo: detalle por paso y Job Run IDs

| Corrida | Factor | Perfil | Paso | Estado | Ejecución (s) | Costo (US$) | Executors | Shuffle (GB) | Spill disco (GB) | Heap pico (GB) | Job Run ID |
| :--- | ---: | :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| S-01-r1 | 1× | Base | 01_ingest_bronze | SUCCESS | 316 | 0.16 | 6 | 0.0 | 0.0 | 6.4 | `00gagk5qguqim80b` |
| S-01-r1 | 1× | Base | 02_clean_silver | SUCCESS | 733 | 0.40 | 6 | 21.9 | 6.6 | 14.6 | `00gagka35ctt280b` |
| S-01-r1 | 1× | Base | 04_regional_gold | SUCCESS | 93 | 0.03 | 3 | 0.1 | 0.0 | 3.0 | `00gagkiiolgq5o0b` |
| S-01-r1 | 1× | Base | 03_feature_gold | SUCCESS | 511 | 0.27 | 6 | 33.6 | 0.0 | 13.1 | `00gagkjudkn8g80b` |
| S-01-r1 | 1× | Base | 05_scoring_dataset_gold | SUCCESS | 145 | 0.04 | 3 | 0.0 | 0.0 | 10.5 | `00gagkpcp9m0a00b` |
| S-01-r2 | 1× | Base | 01_ingest_bronze | SUCCESS | 315 | 0.16 | 6 | 0.0 | 0.0 | 6.5 | `00gagm0g2i0sd00b` |
| S-01-r2 | 1× | Base | 02_clean_silver | SUCCESS | 732 | 0.40 | 6 | 21.9 | 7.0 | 14.2 | `00gagm4qa19oog0b` |
| S-01-r2 | 1× | Base | 04_regional_gold | SUCCESS | 94 | 0.03 | 3 | 0.1 | 0.0 | 3.4 | `00gagmd9dsh5go0b` |
| S-01-r2 | 1× | Base | 03_feature_gold | SUCCESS | 506 | 0.27 | 6 | 33.6 | 0.0 | 13.2 | `00gagmepr170qo0b` |
| S-01-r2 | 1× | Base | 05_scoring_dataset_gold | SUCCESS | 146 | 0.04 | 3 | 0.0 | 0.0 | 10.8 | `00gagmk7naotb00b` |
| S-06-r2 | 1× | Base-v2 | 01_ingest_bronze | SUCCESS | 313 | 0.16 | 6 | 0.0 | 0.0 | 6.7 | `00gak10u307ljo0b` |
| S-06-r2 | 1× | Base-v2 | 02_clean_silver | SUCCESS | 787 | 0.43 | 6 | 20.0 | 15.4 | 9.8 | `00gak14e7dkbco0b` |
| S-06-r2 | 1× | Base-v2 | 04_regional_gold | SUCCESS | 122 | 0.05 | 4 | 0.1 | 0.0 | 4.5 | `00gak1em6c8lho0b` |
| S-06-r2 | 1× | Base-v2 | 03_feature_gold | SUCCESS | 684 | 0.37 | 6 | 36.0 | 0.0 | 11.3 | `00gak1ghb4u1uo0b` |
| S-06-r2 | 1× | Base-v2 | 05_scoring_dataset_gold | SUCCESS | 165 | 0.07 | 6 | 0.0 | 0.0 | 7.4 | `00gak1nre55cq00b` |
| S-02-r1 | 1× | C1 | 01_ingest_bronze | SUCCESS | 1256 | 0.21 | 1 | 0.0 | 0.0 | 8.6 | `00gagmpqs1il980b` |
| S-02-r1 | 1× | C1 | 02_clean_silver | FAILED | 713 | 0.12 | 1 | 15.6 | 7.6 | 12.9 | `00gagn72bag4a00b` |
| S-05-r1 | 5× | Base | 01_ingest_bronze | SUCCESS | 996 | 0.55 | 6 | 0.0 | 0.0 | 8.1 | `00gahmdghoh17o0b` |
| S-05-r1 | 5× | Base | 02_clean_silver | FAILED | 694 | 0.38 | 6 | 85.3 | 40.7 | 14.3 | `00gahmnm2volr80b` |
| S-07-r1 | 5× | Base-v2 | 01_ingest_bronze | SUCCESS | 492 | 0.60 | 16 | 0.0 | 0.0 | 7.3 | `00gak22v1ce0e00b` |
| S-07-r1 | 5× | Base-v2 | 02_clean_silver | SUCCESS | 1638 | 2.24 | 16 | 100.8 | 90.7 | 13.9 | `00gak286eo9c780b` |
| S-07-r1 | 5× | Base-v2 | 04_regional_gold | SUCCESS | 200 | 0.16 | 16 | 0.7 | 0.0 | 4.7 | `00gak33klfct600b` |
| S-07-r1 | 5× | Base-v2 | 03_feature_gold | SUCCESS | 1265 | 1.67 | 16 | 181.2 | 0.0 | 13.6 | `00gak36o3e87ro0b` |
| S-07-r1 | 5× | Base-v2 | 05_scoring_dataset_gold | SUCCESS | 227 | 0.20 | 16 | 0.0 | 0.0 | 9.4 | `00gak3k5389fh00b` |
| S-03-r1 | 10× | Base | 01_ingest_bronze | SUCCESS | 1822 | 1.03 | 12 | 0.0 | 0.0 | 7.8 | `00gagrj89i0ppo0b` |
| S-03-r1 | 10× | Base | 02_clean_silver | FAILED | 784 | 0.43 | 6 | 83.4 | 65.1 | 14.6 | `00gags55jo2rno0b` |
| S-08-r1 | 10× | Base-v2 | 01_ingest_bronze | SUCCESS | 800 | 1.04 | 16 | 0.0 | 0.0 | 7.7 | `00gak44lb0h5v00b` |
| S-08-r1 | 10× | Base-v2 | 02_clean_silver | SUCCESS | 3068 | 4.28 | 16 | 204.1 | 178.6 | 14.8 | `00gak4csc6vkoo0b` |
| S-08-r1 | 10× | Base-v2 | 04_regional_gold | SUCCESS | 274 | 0.24 | 16 | 1.5 | 0.0 | 4.8 | `00gak61a8845e00b` |
| S-08-r1 | 10× | Base-v2 | 03_feature_gold | CANCELLED | 11541 | 16.33 | 16 | 170.8 | 0.0 | 12.5 | `00gak659vquhi00b` |

En la columna Executors, "Executors lanzados" cuenta los distintos que se usaron en el paso.
Con asignación dinámica puede superar el máximo simultáneo: en el paso 01 de S-03-r1 se
lanzaron 12 en total, pero nunca hubo más de 6 a la vez.
