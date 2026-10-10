"""
Módulo de evaluación del modelo de scoring SSCO.

Exporta:
    compute_metrics  — calcula todas las métricas oficiales (contrato C4)
    compute_baselines — evalúa los tres baselines (B0, B1, B2)
"""

from .metrics import compute_metrics
from .baselines import baseline_trivial, baseline_analyst_rule, baseline_random
from .tracker import (
    cargar_run_records,
    consolidar_experimentos,
    formatear_tabla_markdown,
    validar_run_record,
    verificar_completitud_matriz,
)

__all__ = [
    "compute_metrics",
    "baseline_trivial",
    "baseline_analyst_rule",
    "baseline_random",
    "cargar_run_records",
    "consolidar_experimentos",
    "formatear_tabla_markdown",
    "validar_run_record",
    "verificar_completitud_matriz",
]

