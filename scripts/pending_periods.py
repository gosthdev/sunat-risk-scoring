#!/usr/bin/env python3
"""Planner de procesamiento incremental por (fuente, periodo). Solo stdlib, sin red.

  plan   --stage S --upstream U.json --own P.json --expected E.json [--force]
         [--pendientes-existentes "<t1> <t2>"]
         Imprime en stdout el valor de PENDING_PERIODS (vacio = skip); todo lo demas va a stderr.
  commit --stage S --expected E.json --own P.json --published "<PENDING_PERIODS>" --out O.json
         Sella en el processed solo las huellas de lo realmente publicado.

Manifests: {fuente: {periodo: huella}}; la huella upstream puede ser un objeto con "sha256" (raw).
Huella de etapa = sha256(code_hash(etapa, fuente) + huella_entrada). Un archivo de `codigo` o
`compartidos` inexistente es un error explicito: hashear "nada" ocultaria un registro roto.
Un manifest (upstream/own) ausente se trata como vacio.
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PERIODO = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
FUENTE = re.compile(r"^[A-Za-z0-9_]+$")


def die(msg):
    sys.exit(f"ERROR [pending_periods]: {msg}")


def warn(msg):
    print(f"WARN [pending_periods]: {msg}", file=sys.stderr)


def load(path):
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8")) if p.is_file() else {}


def huella_de(entry):
    return entry["sha256"] if isinstance(entry, dict) else entry


def _hash_archivos(h, rutas, repo):
    for r in rutas:
        f = repo / r
        if not f.is_file():
            die(f"archivo de codigo inexistente: {r}")
        h.update(r.encode() + b"\0" + f.read_bytes() + b"\0")


def code_hash(etapa, fuente, registro, repo):
    capa = registro["fuentes"][fuente]["capas"][etapa]
    h = hashlib.sha256()
    _hash_archivos(h, registro["compartidos"][etapa], repo)
    _hash_archivos(h, capa.get("codigo", []), repo)
    h.update(json.dumps(capa, sort_keys=True).encode())
    return h.hexdigest()


def parse_tokens(texto):
    """'a=FULL b=2026-04,2026-05' -> {'a': None, 'b': ['2026-04', '2026-05']}."""
    out = {}
    for tok in texto.split():
        fuente, sep, val = tok.partition("=")
        if not sep or not FUENTE.match(fuente) or fuente in out:
            die(f"token mal formado: {tok!r}")
        if val == "FULL":
            out[fuente] = None
        elif val and all(PERIODO.match(p) for p in val.split(",")):
            out[fuente] = val.split(",")
        else:
            die(f"token mal formado: {tok!r}")
    return out


def plan(registro, repo, etapa, upstream, own, force):
    """Devuelve (tokens {fuente: None|[periodos]}, expected, sin_job)."""
    fuentes = registro["fuentes"]
    sin_job = sorted(f for f in upstream if f not in fuentes)
    tokens, expected = {}, {}
    for f in sorted(fuentes):
        capa = fuentes[f]["capas"].get(etapa)
        if capa is None:
            continue
        ch = code_hash(etapa, f, registro, repo)
        expected[f] = {
            p: hashlib.sha256((ch + huella_de(e)).encode()).hexdigest()
            for p, e in upstream.get(f, {}).items()
        }
        pend = sorted(p for p, h in expected[f].items() if own.get(f, {}).get(p) != h)
        completa = any(
            s.get("publicacion") == "completa" for s in capa.get("salidas", [])
        )
        if force or (
            pend
            and (
                completa
                or len(pend) == len(expected[f])
                or not all(PERIODO.match(p) for p in pend)
            )
        ):
            tokens[f] = None
        elif pend:
            tokens[f] = pend
    return tokens, expected, sin_job


def fmt(tokens):
    return " ".join(
        f"{f}=FULL" if p is None else f"{f}={','.join(p)}"
        for f, p in sorted(tokens.items())
    )


def cmd_plan(a):
    registro, repo = load(a.registro), Path(a.repo)
    if a.stage not in registro["compartidos"]:
        die(f"etapa desconocida: {a.stage}")
    tokens, expected, sin_job = plan(
        registro, repo, a.stage, load(a.upstream), load(a.own), a.force
    )
    for f in sin_job:
        warn(
            f"sin_job: la fuente '{f}' esta en el upstream pero no en config/fuentes.json; no entra al processed"
        )
    for t in a.pendientes_existentes.split():
        for f in registro["fuentes"]:
            if t.rstrip("/") == f or t.startswith(f + "__"):
                warn(
                    f"_pendientes/{t} corresponde a la fuente registrada '{f}': borra s3://BRONZE/_pendientes/{t}/ "
                    f"y la tabla dataset_pendiente_{t} del catalogo"
                )
    Path(a.expected).write_text(
        json.dumps(expected, indent=1, sort_keys=True), encoding="utf-8"
    )
    print(fmt(tokens))


def cmd_commit(a):
    expected, own = load(a.expected), load(a.own)
    for f, periodos in parse_tokens(a.published).items():
        if f not in expected:
            warn(f"'{f}' publicada pero sin huellas esperadas; no se sella")
        elif (
            periodos is None
        ):  # FULL: la tabla se publico con --delete, el processed la refleja entera
            own[f] = dict(expected[f])
        else:
            for p in periodos:
                if p in expected[f]:
                    own.setdefault(f, {})[p] = expected[f][p]
                else:
                    warn(f"{f}/{p} publicado pero sin huella esperada; no se sella")
    Path(a.out).write_text(json.dumps(own, indent=1, sort_keys=True), encoding="utf-8")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "commit"):
        p = sub.add_parser(name)
        p.add_argument("--stage", required=True)
        p.add_argument("--own", required=True)
        p.add_argument("--registro", default=str(REPO / "config" / "fuentes.json"))
        p.add_argument("--repo", default=str(REPO))
    pp = sub.choices["plan"]
    pp.add_argument("--upstream", required=True)
    pp.add_argument("--expected", required=True)
    pp.add_argument("--force", action="store_true")
    pp.add_argument("--pendientes-existentes", default="")
    pc = sub.choices["commit"]
    pc.add_argument("--expected", required=True)
    pc.add_argument("--published", required=True)
    pc.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    {"plan": cmd_plan, "commit": cmd_commit}[a.cmd](a)


if __name__ == "__main__":
    main()
