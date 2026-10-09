# Fase 2 — Pipeline Bronze → Silver → Gold

## Estructura

```
src/spark/
├── common/                     # módulos compartidos, se empaquetan en common.zip
│   ├── s3_paths.py              # constantes de rutas S3 por capa
│   ├── schema_definitions.py    # StructType explícitos (Padrón RUC, Órdenes de Compra)
│   ├── region_normalizer.py     # normalización de nombres de departamento
│   └── spark_session_factory.py # factory de SparkSession con config estándar
└── jobs/
    ├── 00_prepare_small_datasets.py   # PRICOS/Ingresos/EPEN/SSCO → silver (pandas, NO es EMR Step)
    ├── 00_run_prepare_small_datasets.sh # Atajo de desarrollo local para 00_prepare_small_datasets.py
    ├── requirements.txt               # dependencias de 00_prepare_small_datasets.py
    ├── 01_ingest_bronze.py            # EMR Step 1: raw → bronze (+ quarantine)
    ├── 02_clean_silver.py             # EMR Step 2: bronze → silver (padron_ruc, ordenes_compra)
    ├── 03_feature_gold.py             # EMR Step 3: silver → gold/ruc_features
    ├── 04_regional_gold.py            # EMR Step 4: silver → gold/regional_summary
    ├── 05_scoring_dataset_gold.py     # EMR Step 5: gold/ruc_features + silver/ssco → gold/scoring_dataset
    ├── 05_submit_spark_steps.sh       # Orquestador secuencial de los 5 steps en EMR Serverless
    └── package_jobs.sh                # empaqueta common/ en common.zip
```

## Requisitos Previos: Infraestructura en AWS (Terraform)

La infraestructura se despliega en dos capas modulares independientes dentro de `infra/`:
1. **Capa Persistente (`infra/storage/`)**:
   Se ejecuta una sola vez para aprovisionar los 5 buckets S3 (`raw`, `bronze`, `silver`, `gold`, `artifacts`):
   ```bash
   cd infra/storage
   terraform init
   terraform apply
   ```
2. **Capa de Servicios y Cómputo (`infra/services/`)**:
   Despliega EMR Serverless, Glue (DB y Crawlers), Roles IAM y Athena:
   ```bash
   cd infra/services
   terraform init
   terraform apply
   ```
   *(Nota: Puedes ejecutar `terraform destroy` en `infra/services/` para apagar la infraestructura sin afectar los buckets S3 creados en `infra/storage/`).*

---

## Cómo correr todo, en orden

### Opción 1: Automatizado vía GitHub Actions (Método Canónico / Producción)

Los workflows ejecutan las tareas en runners seguros asumiendo el rol IAM vía OIDC:
1. **Ejecutar `.github/workflows/prepare-small-datasets.yml` (workflow_dispatch):**
   Procesa PRICOS, Ingresos Tributarios, EPEN y SSCO hacia la capa `silver/`. Se corre cuando se actualizan dichas fuentes.
2. **Ejecutar `.github/workflows/run-pipeline.yml` (workflow_dispatch):**
   Empaqueta `common.zip`, sincroniza artefactos hacia `s3://${ARTIFACTS_BUCKET}/jobs/` y ejecuta los 5 steps de EMR Serverless secuencialmente mediante `05_submit_spark_steps.sh`.

### Opción 2: Manual / Desarrollo Local

1. **Datasets pequeños (fuera de EMR):**
   ```bash
   pip install -r src/spark/jobs/requirements.txt
   export RAW_BUCKET=sunat-risk-scoring-raw
   export SILVER_BUCKET=sunat-risk-scoring-silver
   bash src/spark/jobs/00_run_prepare_small_datasets.sh
   ```
2. **Empaquetar el código compartido:**
   ```bash
   bash src/spark/jobs/package_jobs.sh
   ```
3. **Subir artefactos y correr los 5 EMR Steps en secuencia:**
   ```bash
   # Obtener valores desde: cd infra/services && terraform output
   export EMR_APPLICATION_ID=<id-de-aplicacion-emr>
   export EMR_EXECUTION_ROLE_ARN=<arn-del-rol-emr>

   # Buckets de S3 (desde cd infra/storage && terraform output)
   export ARTIFACTS_BUCKET=sunat-risk-scoring-artifacts
   export RAW_BUCKET=sunat-risk-scoring-raw
   export BRONZE_BUCKET=sunat-risk-scoring-bronze
   export SILVER_BUCKET=sunat-risk-scoring-silver
   export GOLD_BUCKET=sunat-risk-scoring-gold

   # Subir artefactos a S3
   aws s3 cp src/spark/jobs/common.zip s3://${ARTIFACTS_BUCKET}/jobs/common.zip
   aws s3 sync src/spark/jobs/ s3://${ARTIFACTS_BUCKET}/jobs/ --exclude "*" --include "0*.py"

   # Lanzar orquestación de steps
   bash src/spark/jobs/05_submit_spark_steps.sh
   ```
   *Alternativamente en un cluster Spark interactivo local:*
   ```bash
   spark-submit --py-files common.zip 01_ingest_bronze.py
   spark-submit --py-files common.zip 02_clean_silver.py
   spark-submit --py-files common.zip 03_feature_gold.py
   spark-submit --py-files common.zip 04_regional_gold.py
   spark-submit --py-files common.zip 05_scoring_dataset_gold.py
   ```

## Decisiones y limitaciones conocidas

- **`antiguedad_contratacion_estado_dias` (03_feature_gold.py) no es la
  antigüedad real del RUC.** El Padrón RUC no trae fecha de inscripción
  (confirmado en el diccionario de datos), así que no hay forma de calcular
  la antigüedad real con las fuentes actuales del proyecto. Se usa como
  proxy la fecha de su primera orden de compra con el Estado calculada
  respecto al fin del `mes_referencia` (`last_day`), garantizando total
  reproducibilidad e independencia de la fecha de corrida del job — queda nulo
  para los RUC que nunca contrataron con el Estado o cuya primera orden sea
  posterior al mes evaluado. **Pendiente de decisión de equipo:** aceptar este
  proxy tal cual, buscar otra fuente externa (ej. RENIEC), o quitar el feature
  de esta primera versión del modelo.

- **`04_regional_gold.py` y los snapshots del Padrón RUC:** `silver/padron_ruc`
  contiene 12 fotos mensuales completas del Padrón. Por ello, el cálculo de
  `ruc_activos_por_departamento` y el cruce regional de PRICOS filtran por
  un mes de referencia fijado (por defecto el último mes disponible en el
  dataset, configurable con `ANIO_REFERENCIA_REGIONAL` y `MES_REFERENCIA_REGIONAL`),
  evitando multiplicar los conteos por ~12x.

- **`00_prepare_small_datasets.py` no es un EMR Step.** EMR Serverless solo
  admite job runs de tipo Spark/Hive (no hay un nodo donde correr un script
  suelto), y PRICOS/Ingresos Tributarios/EPEN/SSCO son demasiado chicos para
  justificar un cluster Spark. Se procesan con pandas + awswrangler, desde
  cualquier máquina con credenciales AWS (ver sección anterior). Si se
  decide automatizar todo con Airflow/Step Functions más adelante, esta
  pieza encaja ahí como una tarea normal de Python, no como un job Spark.

- **La carpeta raw se llama `ordenes_compra`** (no `compras_estado`), por
  consistencia con el layout de zonas ya definido en `infra/`.

- **`region_normalizer.OVERRIDES`** cubre variantes de escritura conocidas
  de antemano. Debe completarse con los casos reales que aparezcan al
  comparar los valores únicos de departamento entre las 4 fuentes (EDA
  pendiente antes de cerrar el diseño de la normalización).

- **Cuarentena en Bronze:** `01_ingest_bronze.py` separa a
  `bronze/quarantine/` tanto las filas que no calzan con el esquema
  esperado como las filas de Órdenes de Compra con años de fecha fuera de
  rango (años 2202/8202 detectados en los datos).
