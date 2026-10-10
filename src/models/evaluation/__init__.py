"""
Módulo de evaluación del modelo de scoring SSCO.

Exporta:
    compute_metrics  — calcula todas las métricas oficiales (contrato C4)
    compute_baselines — evalúa los tres baselines (B0, B1, B2)
"""

from .baselines import baseline_analyst_rule, baseline_random, baseline_trivial
from .metrics import compute_metrics
from .tracker import (
    cargar_run_records,
    consolidar_experimentos,
    formatear_tabla_markdown,
    validar_run_record,
    verificar_completitud_matriz,
)

__all__ = [
    "baseline_analyst_rule",
    "baseline_random",
    "baseline_trivial",
    "cargar_run_records",
    "compute_metrics",
    "consolidar_experimentos",
    "formatear_tabla_markdown",
    "validar_run_record",
    "verificar_completitud_matriz",
]
