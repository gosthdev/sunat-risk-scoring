"""Arma las tablas de resultados (markdown) a partir de perf/results/runs/*.json.

Solo usa lo medido: promedia las repeticiones de cada (perfil, factor) y calcula la
eficiencia y el exponente entre factores que sí se corrieron.

  E(V)     = t(1x)·V / t(V)                  1 = el tiempo crece igual que el volumen
  b(a->b)  = log(t_b / t_a) / log(V_b / V_a)  1 = proporcional; > 1 = se degrada

Uso: python perf/analysis/analyze_scaling.py [perf/results/runs]
"""

import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

PASOS = [
    "01_ingest_bronze",
    "02_clean_silver",
    "04_regional_gold",
    "03_feature_gold",
    "05_scoring_dataset_gold",
]


def eficiencia(t_1x: float, t_v: float, factor: float) -> float:
    return t_1x * factor / t_v


def exponente(t_a: float, t_b: float, v_a: float, v_b: float) -> float:
    return math.log(t_b / t_a) / math.log(v_b / v_a)


def cargar(carpeta: Path) -> list[dict]:
    return [
        json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(carpeta.glob("*.json"))
    ]


def _min(segundos) -> str:
    return "—" if segundos is None else f"{segundos / 60:.1f}"


def _spark(corrida: dict, clave: str) -> float:
    return sum(p.get("spark", {}).get(clave, 0) for p in corrida["pasos"])


def _gc_pct(corrida: dict) -> float:
    tareas = _spark(corrida, "tiempo_tareas_s")
    gc = sum(
        p.get("spark", {}).get("gc_pct", 0)
        / 100
        * p.get("spark", {}).get("tiempo_tareas_s", 0)
        for p in corrida["pasos"]
    )
    return 100 * gc / tareas if tareas else 0.0


def _estado(corrida: dict) -> str:
    if corrida["completa"]:
        return "completa"
    fallo = next((p for p in corrida["pasos"] if p.get("estado") != "SUCCESS"), None)
    if fallo is None:
        return "incompleta (interrumpida)"
    return f"falla en {fallo['paso']} ({fallo.get('estado')}: {fallo.get('detalle_estado', '')[:80]})"


def tabla_corridas(corridas: list[dict]) -> str:
    filas = [
        "| Corrida | Factor | Perfil | Estado | Ejecución EMR (min) | Pared (min) | vCPU-h | Memoria GB-h | Costo (US$) | Shuffle escrito (GB) | Spill a disco (GB) | GC (%) |",
        "| :--- | ---: | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for c in sorted(corridas, key=lambda c: (c["factor"], c["perfil"], c["corrida"])):
        t = c["total"]
        filas.append(
            f"| {c['corrida']} | {c['factor']}× | {c['perfil']} | {_estado(c)} | {_min(t['ejecucion_s'])} | "
            f"{_min(t['pared_s'])} | {t['vcpu_h']:.2f} | {t['memoria_gb_h']:.2f} | {t['costo_usd']:.2f} | "
            f"{_spark(c, 'shuffle_escrito_gb'):.1f} | {_spark(c, 'spill_disco_gb'):.1f} | {_gc_pct(c):.1f} |"
        )
    return "\n".join(filas)


def _promedios(corridas: list[dict]) -> dict:
    """(perfil, factor) -> {'total': s, paso: s} con el promedio de las corridas completas."""
    grupos = defaultdict(list)
    for c in corridas:
        if c["completa"]:
            grupos[(c["perfil"], c["factor"])].append(c)
    promedios = {}
    for clave, grupo in grupos.items():
        promedio = {
            "total": statistics.mean(c["total"]["ejecucion_s"] for c in grupo),
            "n": len(grupo),
        }
        for paso in PASOS:
            tiempos = [
                p["ejecucion_s"] for c in grupo for p in c["pasos"] if p["paso"] == paso
            ]
            promedio[paso] = statistics.mean(tiempos) if tiempos else None
        promedios[clave] = promedio
    return promedios


def tabla_escalabilidad(corridas: list[dict], perfil: str = "Base") -> str:
    prom = _promedios(corridas)
    factores = sorted(f for (p, f) in prom if p == perfil)
    if 1 not in factores:
        return f"(sin corridas completas de {perfil} en 1×)"
    cabecera = [
        "Factor",
        "Corridas",
        "Total (min)",
        "E(V) total",
        "b desde el factor anterior",
    ]
    cabecera += [f"E(V) {paso[:2]}" for paso in PASOS]
    filas = [
        "| " + " | ".join(cabecera) + " |",
        "| " + " | ".join(["---:"] * len(cabecera)) + " |",
    ]
    anterior = None
    for f in factores:
        actual, base = prom[(perfil, f)], prom[(perfil, 1)]
        b = (
            "—"
            if anterior is None
            else f"{exponente(prom[(perfil, anterior)]['total'], actual['total'], anterior, f):.2f}"
        )
        celdas = [
            f"{f}×",
            str(actual["n"]),
            _min(actual["total"]),
            f"{eficiencia(base['total'], actual['total'], f):.2f}",
            b,
        ]
        for paso in PASOS:
            ok = actual[paso] and base[paso]
            celdas.append(
                f"{eficiencia(base[paso], actual[paso], f):.2f}" if ok else "—"
            )
        filas.append("| " + " | ".join(celdas) + " |")
        anterior = f
    return "\n".join(filas)


def tabla_una_maquina(corridas: list[dict]) -> str:
    prom = _promedios(corridas)
    filas = [
        "| Factor | C1: resultado | C1 (min) | Base (min) | C1 / Base | Spill a disco C1 (GB) | GC C1 (%) |",
        "| ---: | :--- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for c in sorted(
        (c for c in corridas if c["perfil"] == "C1"), key=lambda c: c["factor"]
    ):
        base = prom.get(("Base", c["factor"]), {}).get("total")
        t = c["total"]["ejecucion_s"]
        cociente = f"{t / base:.1f}" if c["completa"] and base else "—"
        filas.append(
            f"| {c['factor']}× | {_estado(c)} | {_min(t)} | {_min(base)} | {cociente} | "
            f"{_spark(c, 'spill_disco_gb'):.1f} | {_gc_pct(c):.1f} |"
        )
    return "\n".join(filas)


def tabla_pasos(corridas: list[dict]) -> str:
    """Un renglón por paso: tiempo, costo, executors, shuffle, spill, heap y Job Run ID."""
    filas = [
        "| Corrida | Factor | Perfil | Paso | Estado | Ejecución (s) | Costo (US$) | Executors | Shuffle (GB) | Spill disco (GB) | Heap pico (GB) | Job Run ID |",
        "| :--- | ---: | :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |",
    ]
    for c in sorted(corridas, key=lambda c: (c["factor"], c["perfil"], c["corrida"])):
        for p in c["pasos"]:
            s = p.get("spark", {})
            filas.append(
                f"| {c['corrida']} | {c['factor']}× | {c['perfil']} | {p['paso']} | {p.get('estado')} | "
                f"{p.get('ejecucion_s') or 0:.0f} | {p.get('costo_usd', 0):.2f} | "
                f"{s.get('executors_lanzados', '—')} | {s.get('shuffle_escrito_gb', 0):.1f} | "
                f"{s.get('spill_disco_gb', 0):.1f} | {s.get('heap_pico_executor_gb', 0):.1f} | "
                f"`{p.get('job_run_id', '')}` |"
            )
    return "\n".join(filas)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    todas = cargar(Path(argv[0] if argv else "perf/results/runs"))
    pipeline = [c for c in todas if c["tipo"] == "pipeline"]
    print("## Corridas medidas\n")
    print(tabla_corridas(pipeline))
    for perfil in ("Base", "Base-v2"):
        print(f"\n## Escalabilidad del perfil {perfil}\n")
        print(tabla_escalabilidad(pipeline, perfil))
    print("\n## Una sola máquina (C1) frente a Base\n")
    print(tabla_una_maquina(pipeline))
    print("\n## Detalle por paso\n")
    print(tabla_pasos(pipeline))
    print("\n## Prueba A/B del repartition (solo paso 02, 1×)\n")
    print(tabla_pasos([c for c in todas if c["tipo"] == "prueba_paso02"]))
    gasto = sum(c["total"]["costo_usd"] for c in todas)
    print(f"\nGasto medido en EMR (corridas y replicaciones): US$ {gasto:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
