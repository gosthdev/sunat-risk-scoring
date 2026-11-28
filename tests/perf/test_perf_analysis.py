"""Recolección y análisis de corridas, sin AWS (datos sintéticos)."""

import json
import math

from analyze_scaling import eficiencia, exponente, tabla_escalabilidad
from collect_runs import costo_usd, leer_filas, resumir_eventos

STDOUT = """[01_ingest_bronze] SALIDA padron_ruc: filas=191408828 ruta=s3://b/bronze/padron_ruc
[01_ingest_bronze] SALIDA padron_ruc_cuarentena: filas=1525 ruta=s3://b/bronze/quarantine/padron_ruc
CALIDAD fuente=padron_ruc anio=2025 mes=1 bronze=100 cuarentena_bronze=1 tasa_bronze=0.0099 cuarentena_ruc=0 tasa_ruc=0.0000 silver=100
CALIDAD fuente=padron_ruc anio=2025 mes=2 bronze=50 cuarentena_bronze=0 tasa_bronze=0.0000 cuarentena_ruc=2 tasa_ruc=0.0400 silver=48
PERF_REPLICACION {"factor": 10, "rucs_distintos": 13761829, "bloque": 16000000}
"""


def _tarea(inicio, fin, **metricas):
    base = {
        "Executor Run Time": 1000,
        "JVM GC Time": 100,
        "Memory Bytes Spilled": 0,
        "Disk Bytes Spilled": 0,
        "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
        "Shuffle Read Metrics": {"Remote Bytes Read": 0, "Local Bytes Read": 0},
    }
    base.update(metricas)
    return json.dumps(
        {
            "Event": "SparkListenerTaskEnd",
            "Task Info": {"Launch Time": inicio, "Finish Time": fin},
            "Task Metrics": base,
            "Task Executor Metrics": {"JVMHeapMemory": 2 * 10**9},
        }
    )


def test_lee_los_conteos_que_imprimen_los_jobs():
    filas = leer_filas(STDOUT)
    assert filas["salidas"]["padron_ruc"] == 191408828
    assert filas["salidas"]["padron_ruc_cuarentena"] == 1525
    assert filas["calidad_silver"]["padron_ruc"] == {
        "bronze": 150,
        "cuarentena_bronze": 1,
        "cuarentena_ruc": 2,
        "silver": 148,
    }
    assert filas["PERF_REPLICACION"]["bloque"] == 16000000


def test_resume_shuffle_spill_gc_y_tareas_del_event_log():
    lineas = [
        json.dumps({"Event": "SparkListenerExecutorAdded"}),
        _tarea(
            0,
            2000,
            **{
                "Disk Bytes Spilled": 3 * 10**9,
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 10**9},
            },
        ),
        _tarea(0, 6000),
        json.dumps({"Event": "SparkListenerJobEnd"}),
    ]
    r = resumir_eventos(lineas)
    assert r["tareas"] == 2
    assert r["executors_lanzados"] == 1
    assert r["gc_pct"] == 10.0
    assert r["spill_disco_gb"] == 3.0
    assert r["shuffle_escrito_gb"] == 1.0
    assert r["tarea_max_s"] == 6.0
    assert r["heap_pico_executor_gb"] == 2.0


def test_costo_con_tarifas_publicas():
    assert costo_usd(
        {"vCPUHour": 10, "memoryGBHour": 100, "storageGBHour": 0}
    ) == round(10 * 0.052624 + 100 * 0.0057785, 4)


def test_eficiencia_y_exponente():
    assert eficiencia(10, 100, 10) == 1.0
    assert eficiencia(10, 150, 10) < 0.8
    assert math.isclose(exponente(10, 100, 1, 10), 1.0)
    assert math.isclose(exponente(10, 150, 1, 10), math.log10(15))


def _corrida(factor, total, completa=True):
    pasos = [
        {"paso": p, "estado": "SUCCESS", "ejecucion_s": total / 5}
        for p in [
            "01_ingest_bronze",
            "02_clean_silver",
            "04_regional_gold",
            "03_feature_gold",
            "05_scoring_dataset_gold",
        ]
    ]
    return {
        "perfil": "Base",
        "factor": factor,
        "completa": completa,
        "total": {"ejecucion_s": total},
        "pasos": pasos,
    }


def test_tabla_solo_usa_corridas_completas():
    tabla = tabla_escalabilidad(
        [
            _corrida(1, 600),
            _corrida(1, 660),
            _corrida(10, 6300),
            _corrida(10, 99999, completa=False),
        ]
    )
    fila_10x = next(linea for linea in tabla.splitlines() if linea.startswith("| 10×"))
    assert "105.0" in fila_10x  # 6300 s = 105 min; la corrida incompleta no entra
    assert "1.00" in fila_10x  # E(10x) = 630·10/6300
