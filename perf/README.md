# Suite de Pruebas de Rendimiento y Optimización (perf/)

Este módulo proporciona el entorno aislado, las herramientas de empaquetado, submit y benchmarking, y las guardas de seguridad para la ejecución de experimentos de optimización (3.6) y escalamiento (3.7) sobre **AWS EMR Serverless**, garantizando cero modificaciones o impacto sobre los datos reales de producción.

---

## 1. Estructura de Directorios

```text
perf/
├── spark/
│   ├── perf_runner.py               # Wrapper de jobs Spark con inyección de shuffle
│   ├── e1_make_layouts.py           # Generador de layouts P (particionado) y N (no particionado)
│   ├── e2_ingest_bronze_persist.py  # Variante de Job 01 con persist(MEMORY_AND_DISK)
│   └── s_count_layers.py            # Auditor de conteos y checksums de capas
├── scripts/
│   ├── guard_protected_paths.sh     # Guarda de seguridad contra buckets de producción
│   ├── perf_clean_layer.sh          # Limpieza controlada en buckets de pruebas
│   ├── stage_from_real.sh           # Copia de datos reales hacia pruebas con --dry-run
│   ├── package_perf.sh              # Empaquetador de common.zip y scripts para EMR
│   ├── submit_perf_job.sh           # Lanzador de jobs en sunat-ssco-perf-spark
│   └── athena_bench.sh              # Automatizador de consultas benchmark en Athena
├── analysis/
│   ├── consolidate_runs.py          # Consolidador de get-job-run JSON y regla de decisión
│   ├── compare_outputs.py           # Comparador estricto de datasets Parquet
│   └── sql/
│       ├── e1_qa.sql                # Conteo por estado en un mes
│       ├── e1_qb.sql                # Activos por departamento en un mes
│       └── e1_qc.sql                # Control anual completo
├── env/
│   ├── c1.env.example               # Perfil C1 (1 executor, 200 GB disco)
│   ├── c3.env.example               # Perfil C3 (3 executors, pipeline actual fijado)
│   ├── c7.env.example               # Perfil C7 (7 executors, 32 vCPU / 128 GB total)
│   ├── e1_p.env.example             # Configuración para layout P
│   ├── e1_n.env.example             # Configuración para layout N
│   └── e2.env.example               # Configuración para experimento E2
└── results/                         # Directorio donde se guardan los get-job-run JSON
```

---

## 2. Perfiles de Hardware EMR Serverless

| Perfil | Executors | vCPU Executor | RAM Executor | Disco Executor | Driver | Total Asignado |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **C3** | 3 | 4 | 14 GB | 20 GB | 4 vCPU / 14 GB | 16 vCPU / 56 GB |
| **C1** | 1 | 4 | 14 GB | 200 GB | 4 vCPU / 14 GB | 8 vCPU / 28 GB |
| **C7** | 7 | 4 | 14 GB | 200 GB | 4 vCPU / 14 GB | 32 vCPU / 112-128 GB |

*Los 3 perfiles fijan `spark.dynamicAllocation.enabled=false` para evitar variabilidad espuria debida al auto-scaling.*

---

## 3. Política de Seguridad y Guardas

Todos los scripts de operaciones (`perf_clean_layer.sh`, `stage_from_real.sh`) integran `guard_protected_paths.sh`:
1. **Buckets Protegidos:** `sunat-risk-scoring-raw`, `sunat-risk-scoring-bronze`, `sunat-risk-scoring-silver`, `sunat-risk-scoring-gold`.
2. **Rol IAM:** `sunat-ssco-perf-emr-role` tiene un `Deny` explícito para `s3:PutObject` y `s3:DeleteObject` sobre los buckets reales, permitiendo escritura exclusivamente sobre los 6 buckets efímeros de prueba (`perf-36p`, `perf-36n`, `perf-e2`, `perf-x1`, `perf-x10`, `perf-x100`).
3. **Simulacro:** `stage_from_real.sh --dry-run` permite auditar la transferencia antes de mover bytes.

---

## 4. Metodología de Medición

1. **Calentamiento:** La primera corrida de cada condición siempre se descarta para evitar sesgos de inicialización o caching de disco en AWS.
2. **Alternancia estricta:** Orden de ejecución: `Calentamiento, A, B, A, B, A, B` (6 corridas medidas efectivas).
3. **Métrica Primaria:** Duración de la aplicación Spark (`totalExecutionTime` en EMR Serverless y Spark UI).
4. **Regla de Decisión:** Una condición gana **únicamente si el peor tiempo del ganador es estrictamente menor que el mejor tiempo del perdedor** (`winner_max < loser_min`). En caso de solapamiento de rangos, el resultado se clasifica formalmente como **"NO CONCLUYENTE"**.

---

## 5. Comandos de Uso Frecuente

### Empaquetar y sincronizar artefactos
```bash
bash perf/scripts/package_perf.sh --upload
```

### Ejecutar Staging desde datos reales en modo simulacro
```bash
bash perf/scripts/stage_from_real.sh --padron-to-staging sunat-risk-scoring-perf-36p --dry-run
```

### Lanzar Job con Wrapper y Perfil C3
```bash
bash perf/scripts/submit_perf_job.sh \
  --profile C3 \
  --script perf_runner.py \
  --job-args "--job 04_regional_gold --shuffle-partitions 64" \
  --run-name "run-e1-p-01" \
  --exp-tag E1
```

### Consolidar métricas y evaluar criterio
```bash
python3 perf/analysis/consolidate_runs.py perf/results/E1/*.json
```
