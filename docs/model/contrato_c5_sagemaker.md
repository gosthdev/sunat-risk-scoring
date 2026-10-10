# Contrato C5 — Especificación de Runtime SageMaker y Métricas CloudWatch

## 1. Identificación y Metadatos del Contrato
- **ID del Contrato:** C5 (`sagemaker_metrics_and_runtime`)
- **Productor:** Desarrollador A (`src/models/train.py`, `src/models/launch_manual.py`, `src/models/entrypoint.sh`)
- **Consumidores:** Amazon SageMaker Engine, Amazon CloudWatch Metrics, Desarrollador B (Orquestador Step Functions / Airflow / Lambda)
- **Tipo de Cómputo:** Amazon SageMaker Training Job en modo script (Scikit-Learn Framework)
- **Imagen Base ECR (us-east-1):** `683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.3-1-cpu-py3`
- **Tipo de Instancia Recomendada:** `ml.m5.xlarge` (4 vCPU, 16 GiB RAM)
- **Rol IAM de Ejecución:** `iam_sagemaker_execution_role` (política con mínimo privilegio sobre S3 Gold y CloudWatch Logs)

---

## 2. Invariante de Idempotencia y Nomenclatura

Para prevenir ejecuciones duplicadas accidentales y asegurar trazabilidad biunívoca en auditorías tributarias:
- **Regla Estricta:** `TrainingJobName == run_id`
- La API de Amazon SageMaker rechaza la creación de cualquier job con un nombre idéntico a uno existente (`ResourceInUse` / `Cannot create training job with duplicate name`), garantizando idempotencia a nivel de plataforma AWS.
- El `run_id` codifica los metadatos de la corrida (e.g. `ssco-lr-v2-pesos-v01-20261009180000`).

---

## 3. Canales de Entrada y Rutas de Salida en S3

### A. Canales de Entrada (Input Channels)
- **Canal `train`:**
  - URI S3: `s3://<gold-bucket>/gold/model_inputs/dataset_version=<v>/`
  - Formato: Apache Parquet (Contrato C1).
  - En SageMaker, el servicio monta automáticamente los archivos en la ruta `/opt/ml/input/data/train/`.

### B. Canales de Salida (Output Paths)
- **Predicciones Globales (Contrato C2):**
  - URI S3: `s3://<gold-bucket>/gold/model_outputs/predictions_<run_id>.parquet`
  - Contiene: `ruc`, `score`, `label`, `split`, `rank_global` ordenado por `[score DESC, ruc ASC]`.
- **Run Record NDJSON (Contrato C3):**
  - URI S3: `s3://<gold-bucket>/gold/model_outputs/run_record_<run_id>.json`
  - Contiene: metadatos de la corrida, hiperparámetros y métricas en formato estricto de una sola línea (Single-line NDJSON).
- **Artefactos del Modelo Entrenado:**
  - URI S3: `s3://<gold-bucket>/gold/model_artifacts/<run_id>/output/model.tar.gz`
  - Contiene: `model.joblib` con el pipeline completo ajustado (`ColumnTransformer` + Estimador).

---

## 4. Especificación de Expresiones Regulares para CloudWatch Metrics

SageMaker captura métricas de entrenamiento parseando las líneas emitidas por `stdout` mediante expresiones regulares configuradas en `MetricDefinitions`. Todo mensaje de métrica sigue el prefijo obligatorio `[METRIC] <nombre>=<valor>`.

| Nombre de Métrica (`Name`) | Expresión Regular (`Regex`) | Tipo | Descripción |
| :--- | :--- | :--- | :--- |
| `cv_pr_auc_mean` | `\[METRIC\] cv_pr_auc_mean=([0-9\.]+)` | Float | Media de PR-AUC (Average Precision) en validación cruzada de 5 folds. |
| `cv_pr_auc_fold_0` | `\[METRIC\] cv_pr_auc_fold_0=([0-9\.]+)` | Float | Métrica PR-AUC en Fold 0 de CV. |
| `cv_pr_auc_fold_1` | `\[METRIC\] cv_pr_auc_fold_1=([0-9\.]+)` | Float | Métrica PR-AUC en Fold 1 de CV. |
| `cv_pr_auc_fold_2` | `\[METRIC\] cv_pr_auc_fold_2=([0-9\.]+)` | Float | Métrica PR-AUC en Fold 2 de CV. |
| `cv_pr_auc_fold_3` | `\[METRIC\] cv_pr_auc_fold_3=([0-9\.]+)` | Float | Métrica PR-AUC en Fold 3 de CV. |
| `cv_pr_auc_fold_4` | `\[METRIC\] cv_pr_auc_fold_4=([0-9\.]+)` | Float | Métrica PR-AUC en Fold 4 de CV. |
| `train_pr_auc` | `\[METRIC\] train_pr_auc=([0-9\.]+)` | Float | Métrica PR-AUC evaluada sobre la totalidad de `train`. |
| `n_train` | `\[METRIC\] n_train=([0-9]+)` | Int | Total de filas de entrenamiento procesadas. |
| `n_pos_train` | `\[METRIC\] n_pos_train=([0-9]+)` | Int | Total de casos positivos (SSCO=1) en el conjunto de entrenamiento. |
| `n_features_transformadas` | `\[METRIC\] n_features_transformadas=([0-9]+)` | Int | Dimensión del vector de entrada tras el preprocesamiento (`ColumnTransformer`). |
| `train_seconds` | `\[METRIC\] train_seconds=([0-9\.]+)` | Float | Tiempo neto de entrenamiento y búsqueda de hiperparámetros en segundos. |

---

## 5. Parámetros de Entrada de Línea de Comandos (`train.py`)

| Argumento | Tipo | Valores Válidos | Default | Descripción |
| :--- | :--- | :--- | :--- | :--- |
| `--model_name` | String | `LR`, `DT` | `LR` | Algoritmo: Regresión Logística o Árbol de Decisión. |
| `--variant` | String | `V1`, `V2` | `V2` | Variante de features: V2 oficial (sin fuga) o V1 (control de techo). |
| `--usa_pesos` | String | `si`, `no` | `si` | Balanceo de clases (`class_weight="balanced"`). |
| `--run_id` | String | Texto alfanumérico | Auto-generado | Identificador único de la corrida. |
| `--dataset_version` | String | Texto | `v01` | Versión del dataset en S3 (`dataset_version=v1`). |
| `--seed` | Int | Entero positivo | `42` | Semilla del generador pseudoaleatorio. |
| `--train_dir` | String | Ruta / URI S3 | Env / `/opt/ml/input/data/train` | Directorio o archivo de entrada Parquet. |
| `--model_dir` | String | Ruta local | Env / `/opt/ml/model` | Directorio para artefactos del modelo. |
| `--output_dir` | String | Ruta local | Env / `/opt/ml/output/data` | Directorio para archivos de salida adicionales. |
| `--ruta_salida_predicciones`| String | URI S3 o local | Opcional | Sobrescritura de destino para Contrato C2. |
| `--ruta_salida_run_record` | String | URI S3 o local | Opcional | Sobrescritura de destino para Contrato C3. |
