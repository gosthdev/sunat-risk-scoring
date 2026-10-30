# SUNAT Risk Scoring

Pipeline de datos y modelado para priorizar contribuyentes con riesgo de ser
**Sujetos sin Capacidad Operativa (SSCO)** a partir de fuentes públicas de SUNAT
y OSCE. Corre sobre AWS (S3, EMR Serverless, Glue, Athena, EC2) y se despliega
con Terraform y GitHub Actions.

> **Estado: avance.** El pipeline de datos (Raw → Gold) y el entrenamiento de la
> matriz de experimentos funcionan de punta a punta. La evaluación, el modelo
> definitivo y la explicabilidad siguen en curso (ver [Estado del proyecto](#estado-del-proyecto)).

## Qué problema resuelve

Dado el Padrón RUC y las órdenes de compra del Estado, estimar qué RUC se parecen
a los que SUNAT ya declaró como SSCO, para ordenar la fiscalización. La etiqueta
sale del registro oficial de SSCO; el resto de fuentes aportan las variables.

## Fuentes de datos

| Fuente | Uso |
|---|---|
| Padrón RUC (mensual, 12 snapshots de 2025) | Atributos registrales del contribuyente |
| Órdenes de Compra (OSCE, mensual) | Actividad de contratación con el Estado |
| SSCO | Etiqueta objetivo |
| PRICOS | Marca de principales contribuyentes |
| Ingresos tributarios y EPEN | Contexto regional |

Las URLs de descarga están en `infra/ingestion_ec2/urls.json`.

## Arquitectura

```
Fuentes públicas
      │  (EC2 efímera, Terraform)
      ▼
   RAW ──► Job 00 (pandas, runner) ──► SILVER (datasets pequeños)
      │
      ▼  EMR Serverless
 BRONZE (01) ──► SILVER (02) ──┬──► GOLD ruc_features (03)    ──┐
 + cuarentena    + cuarentena  └──► GOLD regional_summary (04) ──┴──► GOLD scoring_dataset (05)
                                                                          │
                                       Glue crawlers + Athena ◄───────────┤
                                                                          ▼
                                    Job 06 model_inputs ──► entrenamiento (EC2)
```

| Etapa | Qué hace | Cómputo |
|---|---|---|
| 0a Ingesta Raw | Descarga y sube las fuentes a S3 | EC2 efímera, se destruye al terminar |
| 0b Job 00 | PRICOS, EPEN, Ingresos y SSCO → Silver | Runner de GitHub (pandas) |
| 1 Bronze (Job 01) | Aplica esquema explícito; lo inválido va a cuarentena | EMR Serverless |
| 2 Silver (Job 02) | Limpieza, validación de RUC (11 dígitos), reporte de calidad | EMR Serverless |
| 3 Gold RUC (Job 03) | Features por RUC | EMR Serverless |
| 4 Gold Regional (Job 04) | Resumen por departamento | EMR Serverless |
| 5 Scoring (Job 05) | Dataset de scoring (features + SSCO) | EMR Serverless |
| 6 Analytics | Crawlers de Glue y queries de Athena | Glue / Athena |
| 7 Verify | Smoke test de integridad del lake | Runner de GitHub |

Los buckets son `raw`, `bronze`, `silver`, `gold` y `artifacts`.

### Decisiones de diseño

- **Idempotencia.** Cada etapa tiene un marcador en S3 y se salta si ni su código
  ni sus entradas cambiaron. Se puede forzar con `force_all` o por etapa.
- **Publicación en dos pasos.** Los steps 02–05 escriben en `_staging/` y se
  publican a la ruta viva solo si el job termina bien (`sync --delete`). Un job
  fallido se reintenta y, si persiste, no toca lo ya publicado.
- **Cuarentena, no descarte.** Filas con esquema inválido, años fuera de rango o
  RUC inválido se separan con su motivo; si la tasa por RUC supera
  `MAX_TASA_CUARENTENA_RUC` (0.5) el job aborta antes de escribir Silver.
- **Executors acotados.** `maxExecutors = ceil(snapshots/2) + 1`, entre 2 y 6
  (`MAX_EXECUTORS_CAP`), por el tope de 32 vCPU de la aplicación EMR.
- **Contratos.** Los esquemas y reglas están en `contracts/` y `docs/model/`.

Más detalle y limitaciones conocidas en [`src/README.md`](src/README.md).

## Modelado

La matriz de experimentos está en `config/experiments.yml` (semilla 42, métrica de
selección `average_precision`):

- **E1–E6:** regresión logística (LR) y árbol de decisión (DT), con features V2
  (sin fuga) y V1 (incluye Estado/Condición, solo como techo de referencia), con y
  sin pesos de clase.
- **E9–E12:** exploraciones de hiperparámetros sobre LR y DT con V2.

El entrenamiento corre en una EC2 puntual con `scripts/train_ec2.sh`
(workflow `train-models.yml`; resultados con `collect-training-results.yml`).
Existe también un flujo alternativo en SageMaker (`train-models-sagemaker.yml`).
El código está en `src/models/` (entrenamiento, métricas con bootstrap, curvas,
explicabilidad y consolidación de experimentos).

Modelo base declarado hasta ahora: **E9** (LR V2 con pesos), a mejorar.

## Estructura del repositorio

```
.github/workflows/   CI, CD y entrenamiento
config/              matriz de experimentos
contracts/           contratos de esquema (Padrón, Órdenes de Compra)
docs/model/          contratos del dataset, features, auditoría de fuga, informe
infra/               Terraform: storage, services, ingestion_ec2, módulos
notebooks/           EDA (calidad, Padrón, target, regional)
scripts/             orquestación (marcadores, limpieza, entrenamiento EC2)
src/spark/           jobs 00–06 y módulos comunes
src/models/          entrenamiento, evaluación, explicabilidad
src/analysis/        queries de Athena y scripts de Glue/Athena
tests/               pruebas
```

## Cómo ejecutarlo

### 1. Infraestructura (una vez)

```bash
cd infra/state    && terraform init && terraform apply   # bucket de estado (estado local, una sola vez)
cd infra/storage  && terraform init && terraform apply   # buckets S3
cd infra/services && terraform init && terraform apply   # EMR, Glue, Athena, IAM
```

`storage` y `services` guardan su estado en el bucket de `infra/state` (una `key`
por stack, bloqueo con `use_lockfile`, requiere Terraform >= 1.10). Si ya tenían
estado local, migrarlo antes de aplicar: `terraform init -migrate-state`.

Los permisos y roles viven en Terraform. Los scripts no tocan IAM.

### 2. Pipeline de datos

Lo normal es el workflow **Continuous Deployment** (`cd.yml`): corre en cada push
a `main` o `develop`, o a mano con *Run workflow*. Para rehacer todo, marcar
`force_all`. Cada paso de Spark muestra en el log el `stdout`/`stderr` del driver
(plegado si termina bien, abierto si falla) y un resumen en el *Job Summary*.

Ejecución manual, desde la raíz:

```bash
bash src/spark/jobs/package_jobs.sh
export ARTIFACTS_BUCKET=... RAW_BUCKET=... BRONZE_BUCKET=... SILVER_BUCKET=... GOLD_BUCKET=...
export EMR_APPLICATION_ID=... EMR_EXECUTION_ROLE_ARN=...
bash src/spark/jobs/05_submit_spark_steps.sh --only-steps 01_ingest_bronze
```

### 3. Entrenamiento

Ejecutar el workflow **Train & Benchmark SSCO Models (EC2)** y luego
**Collect Training Results**. La instancia (`r6i.8xlarge`, 32 vCPU) corre 5
experimentos en paralelo; el dataset ronda los 12.6 M de filas y cada experimento
pica cerca de 22 GB de RAM, por eso no entra en un runner de GitHub.

## Desarrollo

```bash
pip install -r requirements-dev.txt
ruff check . && ruff format --check .
mypy src
pytest -q
```

El CI (`ci.yml`) corre fmt, validate y tflint de Terraform, y ruff, mypy y pytest
de Python. Convención de commits: *conventional commits*.

## Estado del proyecto

| Área | Estado |
|---|---|
| Ingesta Raw y datasets pequeños | Hecho |
| Bronze → Silver → Gold, con cuarentena y reporte de calidad | Hecho |
| CD idempotente con logs por etapa | Hecho (logs de la ingesta EC2 y de Analytics, pendientes) |
| Analytics (Glue + Athena) | Hecho |
| Entrenamiento de la matriz E1–E12 (E7 y E8 sin ejecutar) | Hecho en EC2 |
| Evaluación y modelo base (E9) | En curso |
| Modelo definitivo y costo de falsos positivos/negativos | Pendiente |
| Explicabilidad | En curso |
| Antigüedad real del RUC | Pendiente: hoy es un proxy por la primera orden de compra |
