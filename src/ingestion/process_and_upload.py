#!/usr/bin/env python3
"""
Ingesta incremental ejecutada en la EC2 efímera.

1. Lee /opt/ingestion/urls.json (identidad: fuente -> periodo -> url) y
   /opt/ingestion/fuentes.json (fuentes con código productivo).
2. Decide por (fuente, periodo) con s3://RAW/_manifests/raw_manifest.json:
   nuevo -> descarga; registrado con validadores -> HEAD condicional;
   registrado sin validadores -> inmutable. Error de HEAD -> WARN y se omite.
   FORCE_REDOWNLOAD=1 ignora el manifest para decidir.
3. Descarga, hashea (sha256 del contenido que aterriza en raw) y deja
   padron_ruc/ordenes_compra/small en su layout Hive; las claves sin handler
   propio usan el handler genérico y, si no están en fuentes.json, se convierten
   en streaming a Parquet (todo string) en s3://BRONZE/_pendientes/.
4. Sincroniza a s3://RAW/, escribe el manifest y last_run_errors.json y,
   por último, emite _INGESTION_SUCCESS.
"""

import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

BASE_DIR = Path("/data")
URLS_FILE = Path("/opt/ingestion/urls.json")
FUENTES_FILE = Path("/opt/ingestion/fuentes.json")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "sunat-risk-scoring-raw")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "sunat-risk-scoring-bronze")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
CHUNK_ROWS = 50_000
MANIFEST_KEY = "_manifests/raw_manifest.json"

Entry = dict[str, Any]
Manifest = dict[str, dict[str, Entry]]


def log(msg: str):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def _aws(args: list[str], check: bool = True) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(["aws", *args], capture_output=True, text=True, check=check)


def http_head(
    url: str, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, str]]:
    """HEAD (sigue redirecciones). Devuelve (status, cabeceras en minúscula)."""
    h = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) IngestionWorker/1.0"}
    h.update(headers or {})
    req = urllib.request.Request(url, headers=h, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}


def download_file(url: str, dest_path: Path):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    log(f"Descargando: {url} -> {dest_path}")

    # Intentar curl si está disponible (soporta SSL moderno y redirecciones 30x)
    cmd = [
        "curl",
        "-fSL",
        "--retry",
        "3",
        "--retry-delay",
        "2",
        "-o",
        str(dest_path),
        url,
    ]
    res = subprocess.run(cmd, capture_output=True, check=False)
    if res.returncode != 0:
        log(f"curl falló ({res.stderr.decode()}). Reintentando con urllib...")
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) IngestionWorker/1.0"
            },
        )
        with (
            urllib.request.urlopen(req, timeout=300) as resp,
            open(dest_path, "wb") as out,
        ):
            shutil.copyfileobj(resp, out)

    log(
        f"✓ Descargado ({dest_path.stat().st_size / (1024 * 1024):.2f} MB): {dest_path.name}"
    )


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_files(paths: list[Path]) -> str:
    """Un archivo -> su sha; varios -> sha del conjunto ordenado por nombre."""
    paths = sorted(paths, key=lambda p: p.name)
    if len(paths) == 1:
        return sha256_file(paths[0])
    h = hashlib.sha256()
    for p in paths:
        h.update(p.name.encode() + b"\0" + sha256_file(p).encode())
    return h.hexdigest()


def s3_sha256(s3_uri: str) -> str | None:
    """sha256 en streaming de un objeto S3, o None si no existe."""
    ls = _aws(["s3", "ls", s3_uri], check=False)
    if ls.returncode != 0 or not ls.stdout.strip():
        return None
    proc = subprocess.Popen(["aws", "s3", "cp", s3_uri, "-"], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    h = hashlib.sha256()
    for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b""):  # type: ignore[union-attr]
        h.update(chunk)
    if proc.wait() != 0:
        raise RuntimeError(f"No se pudo leer {s3_uri}")
    return h.hexdigest()


def load_manifest() -> Manifest | None:
    """None si no existe; cualquier otro error se propaga (no se re-bootstrapea)."""
    uri = f"s3://{RAW_BUCKET}/{MANIFEST_KEY}"
    res = _aws(["s3", "cp", uri, "-"], check=False)
    if res.returncode == 0:
        return json.loads(res.stdout)
    err = res.stderr
    if "404" in err or "Not Found" in err or "NoSuchKey" in err:
        return None
    raise RuntimeError(f"No se pudo leer {uri}: {err.strip()}")


def save_manifest(manifest: Manifest):
    path = BASE_DIR / "tmp" / "raw_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    _aws(
        [
            "s3",
            "cp",
            str(path),
            f"s3://{RAW_BUCKET}/{MANIFEST_KEY}",
            "--only-show-errors",
        ]
    )
    log("✓ raw_manifest.json actualizado")


def _split_periodo(key: str) -> tuple[str, str]:
    partes = key.split("-")
    return partes[0], (partes[1].zfill(2) if len(partes) > 1 else "01")


SMALL_RAW_KEYS = {
    "pricos": "pricos/principalesContrib-PRICOS.xlsx",
    "ssco": "ssco/sujesincapacidadOperativa.xlsx",
    "ingresos_tributarios": "ingresos_tributarios/cdrA13_tabular.csv",
    "epen": "epen/anio=2025/epen.csv",
    "see": "see/padron_see.csv",
}


def raw_key(fuente: str, periodo: str) -> str | None:
    """Objeto raw que representa (fuente, periodo); None si no hay layout conocido."""
    if fuente == "padron_ruc":
        a, m = _split_periodo(periodo)
        return f"padron_ruc/anio={a}/mes={m}/padron.csv"
    if fuente == "ordenes_compra":
        a, m = _split_periodo(periodo)
        return f"ordenes_compra/anio={a}/mes={m}/ordenes.csv"
    return SMALL_RAW_KEYS.get(fuente)


def bootstrap_manifest(items: list[tuple[str, str, str]]) -> Manifest:
    """Primera corrida con raw existente: hashea lo que ya está en S3, sin validadores."""
    log(
        "Sin manifest: bootstrap desde objetos existentes en S3 (sin tráfico al origen)"
    )
    manifest: Manifest = {}
    for fuente, periodo, url in items:
        key = raw_key(fuente, periodo)
        if not key:
            continue
        sha = s3_sha256(f"s3://{RAW_BUCKET}/{key}")
        if sha:
            manifest.setdefault(fuente, {})[periodo] = {
                "sha256": sha,
                "etag": None,
                "last_modified": None,
                "url": url,
                "ingested_at": _now(),
            }
    return manifest


def needs_download(entry: Entry | None, url: str, force: bool) -> bool:
    if force or entry is None or entry.get("conversion") == "error":
        return True  # error de conversión previo: se reintenta
    etag, lm = entry.get("etag"), entry.get("last_modified")
    if not etag and not lm:
        return False  # sin validadores -> inmutable, cero red
    cond = {}
    if etag:
        cond["If-None-Match"] = etag
    if lm:
        cond["If-Modified-Since"] = lm
    target = entry.get("url") or url
    try:
        status, hdrs = http_head(target, cond)
    except Exception as e:  # noqa: BLE001
        log(f"WARN: HEAD falló para {target} ({e}). Se omite, no se re-descarga.")
        return False
    if status == 304:
        return False
    if status == 200:
        return (hdrs.get("etag") or etag) != etag or (
            hdrs.get("last-modified") or lm
        ) != lm
    log(f"WARN: HEAD {target} devolvió {status}. Se omite.")
    return False


def head_validators(url: str) -> dict[str, str | None]:
    try:
        status, hdrs = http_head(url)
        if status == 200:
            return {
                "etag": hdrs.get("etag"),
                "last_modified": hdrs.get("last-modified"),
            }
    except Exception as e:  # noqa: BLE001
        log(f"WARN: no se pudieron leer validadores de {url} ({e})")
    return {"etag": None, "last_modified": None}


# ---------------------------------------------------------------------------
# Handlers por fuente: devuelven (archivos aterrizados en BASE_DIR, url usada)
# ---------------------------------------------------------------------------


def process_padron_ruc(key: str, url: str) -> tuple[list[Path], str]:
    # key esperada: "2025-01" o similar
    anio, mes = _split_periodo(key)

    out_dir = BASE_DIR / "padron_ruc" / f"anio={anio}" / f"mes={mes}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "padron.csv"

    temp_zip = BASE_DIR / "tmp" / f"padron_{anio}_{mes}.zip"
    download_file(url, temp_zip)

    log(f"Extrayendo {temp_zip.name}...")
    extract_dir = BASE_DIR / "tmp" / f"padron_extract_{anio}_{mes}"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(temp_zip, "r") as z:
        z.extractall(extract_dir)

    # Buscar el archivo extraído (.txt o .csv)
    extracted_files = [
        f for f in extract_dir.rglob("*") if f.is_file() and not f.name.startswith(".")
    ]
    if not extracted_files:
        raise FileNotFoundError(f"No se encontraron archivos dentro de {temp_zip}")

    src_file = extracted_files[0]
    log(
        f"Archivo extraído: {src_file.name} ({src_file.stat().st_size / (1024 * 1024):.2f} MB)"
    )

    # Inspeccionar codificación y delimitador de la cabecera
    src_encoding = "utf-8"
    try:
        with open(src_file, "r", encoding="utf-8") as f:
            first_line = f.readline()
    except UnicodeDecodeError:
        src_encoding = "latin-1"
        with open(src_file, "r", encoding="latin-1") as f:
            first_line = f.readline()

    if "|" in first_line:
        log(
            f"Detectado delimitador pipe '|' (encoding: {src_encoding}). Convirtiendo a CSV estándar (coma UTF-8)..."
        )
        with (
            open(src_file, "r", encoding=src_encoding, errors="replace") as fin,
            open(out_csv, "w", encoding="utf-8", newline="") as fout,
        ):
            reader = csv.reader(fin, delimiter="|")
            writer = csv.writer(fout)
            for row in reader:
                writer.writerow(row)
    else:
        # Ya es CSV con comas
        shutil.move(str(src_file), str(out_csv))

    log(f"✓ Guardado en partición: {out_csv}")

    # Limpiar temporales para ahorrar espacio
    temp_zip.unlink(missing_ok=True)
    shutil.rmtree(extract_dir, ignore_errors=True)
    return [out_csv], url


MONTH_VARIANTS = {
    "01": ["ENERO"],
    "02": ["FEBRERO"],
    "03": ["MARZO"],
    "04": ["ABRIL"],
    "05": ["MAYO"],
    "06": ["JUNIO"],
    "07": ["JULIO"],
    "08": ["AGOSTO"],
    "09": ["SEPTIEMBRE", "SETIEMBRE"],
    "10": ["OCTUBRE"],
    "11": ["NOVIEMBRE"],
    "12": ["DICIEMBRE"],
}


def _download_osce_xlsx(url_or_key: str, anio: str, mes: str, dest_path: Path) -> str:
    """Descarga el .xlsx de OSCE validando 'PK'. Devuelve la URL que funcionó."""
    import base64

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Referer": "https://conosce.osce.gob.pe/",
    }

    candidate_urls = []
    # Si viene una URL de Pentaho redirect, extraer la URL en base64
    if "redirect.html" in url_or_key and "url=" in url_or_key:
        parsed = urllib.parse.urlparse(url_or_key)
        qs = urllib.parse.parse_qs(parsed.query)
        b64_val = qs.get("url", [""])[0]
        if b64_val:
            candidate_urls.append(base64.b64decode(b64_val).decode("utf-8"))
    elif url_or_key.startswith("http"):
        candidate_urls.append(url_or_key)

    # Agregar candidatos según variantes de nombre del mes
    base_osce = (
        f"https://conosce.osce.gob.pe/buscador/assets/67ae6c4a/reportes/ordenes/{anio}/"
    )
    variantes = MONTH_VARIANTS.get(mes, [f"MES{mes}"])
    # OSCE publicó con y sin el prefijo "CONOSCE_" según la época.
    for var in variantes:
        for prefijo in ("", "CONOSCE_"):
            candidate_urls.append(
                f"{base_osce}{prefijo}ORDENESCOMPRA{var}{anio}_0.xlsx"
            )

    # Intentar descargar de cada candidato hasta encontrar un XLSX válido (ZIP: magic bytes 'PK')
    for candidate in candidate_urls:
        log(f"Probando descarga OSCE: {candidate}")
        try:
            req = urllib.request.Request(candidate, headers=headers)
            with urllib.request.urlopen(req, timeout=180) as resp:
                if resp.status != 200:
                    continue
                # Leer primeros bytes para validar que sea zip/xlsx (PK)
                magic = resp.read(2)
                if magic != b"PK":
                    log(
                        f"Respuesta no es Excel válido (magic bytes: {magic!r}). Saltando {candidate}..."
                    )
                    continue

                # Escribir contenido completo
                with open(dest_path, "wb") as out_f:
                    out_f.write(magic)
                    shutil.copyfileobj(resp, out_f)

                size_mb = dest_path.stat().st_size / (1024 * 1024)
                log(f"✓ Excel descargado exitosamente ({size_mb:.2f} MB): {candidate}")
                return candidate
        except Exception as e:  # noqa: BLE001
            log(f"Fallo al intentar {candidate}: {e}")

    raise RuntimeError(
        f"No se pudo descargar un archivo Excel válido (.xlsx) para {anio}-{mes} tras probar variantes."
    )


def process_ordenes_compra(key: str, url: str) -> tuple[list[Path], str]:
    anio, mes = _split_periodo(key)

    out_dir = BASE_DIR / "ordenes_compra" / f"anio={anio}" / f"mes={mes}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "ordenes.csv"

    temp_xlsx = BASE_DIR / "tmp" / f"ordenes_{anio}_{mes}.xlsx"
    used_url = _download_osce_xlsx(url, anio, mes, temp_xlsx)

    # Convertir Excel a CSV garantizando contenido real
    import pandas as pd

    log(f"Leyendo Excel {temp_xlsx.name} con pandas...")
    df = pd.read_excel(temp_xlsx)
    log(f"Convertiendo a CSV (filas: {len(df)})...")
    df.to_csv(out_csv, index=False, encoding="utf-8")

    # Validar que no sea un archivo vacío o corrupto
    if out_csv.stat().st_size < 1024 * 50:
        raise ValueError(
            f"El CSV generado ({out_csv.stat().st_size} bytes) es anormalmente pequeño para {key}."
        )

    # Limpiar archivo temporal .xlsx para liberar espacio en disco
    temp_xlsx.unlink(missing_ok=True)
    log(
        f"✓ Guardado en partición ({out_csv.stat().st_size / (1024 * 1024):.2f} MB): {out_csv}"
    )
    return [out_csv], used_url


def process_pricos(_periodo: str, url: str) -> tuple[list[Path], str]:
    dest = BASE_DIR / "pricos" / "principalesContrib-PRICOS.xlsx"
    download_file(url, dest)
    return [dest], url


def process_ssco(_periodo: str, url: str) -> tuple[list[Path], str]:
    dest = BASE_DIR / "ssco" / "sujesincapacidadOperativa.xlsx"
    download_file(url, dest)
    return [dest], url


def process_ingresos_tributarios(_periodo: str, url: str) -> tuple[list[Path], str]:
    out_dir = BASE_DIR / "ingresos_tributarios"
    out_dir.mkdir(parents=True, exist_ok=True)
    dest_csv = out_dir / "cdrA13_tabular.csv"
    dest_var_csv = out_dir / "cdrA13_Var_tabular.csv"
    temp_file = BASE_DIR / "tmp" / "ingresos_tributarios.raw"
    download_file(url, temp_file)

    try:
        # Intentar des-pivoteado estandarizado con ingresos_transformer
        common_dir = str(
            Path(__file__).resolve().parent.parent.parent.parent
            / "src"
            / "spark"
            / "common"
        )
        if common_dir not in sys.path:
            sys.path.insert(0, common_dir)
        from ingresos_transformer import process_both_ingresos

        df_monto, df_var = process_both_ingresos(str(temp_file))
        df_monto.to_csv(dest_csv, index=False, encoding="utf-8")
        df_var.to_csv(dest_var_csv, index=False, encoding="utf-8")
        log(
            "✓ Ingresos Tributarios des-pivoteados exitosamente en cdrA13_tabular.csv y cdrA13_Var_tabular.csv"
        )
        temp_file.unlink(missing_ok=True)
    except Exception as err:  # noqa: BLE001
        log(
            f"Advertencia: Falló des-pivoteo de Ingresos Tributarios ({err}). Guardando respaldo..."
        )
        shutil.move(str(temp_file), str(dest_csv))
    log(f"✓ Ingresos Tributarios guardados en: {dest_csv}")
    return [dest_csv], url


def process_epen(_periodo: str, url: str) -> tuple[list[Path], str]:
    out_dir = BASE_DIR / "epen" / "anio=2025"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "epen.csv"
    temp_file = BASE_DIR / "tmp" / "epen_download.raw"
    download_file(url, temp_file)

    # Chequear si es un archivo ZIP
    if zipfile.is_zipfile(temp_file):
        log("EPEN detectado como archivo ZIP. Descomprimiendo...")
        extract_dir = BASE_DIR / "tmp" / "epen_extract"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(temp_file, "r") as z:
            z.extractall(extract_dir)

        # Buscar archivo de datos principal (el más pesado o de datos)
        candidates = [
            f
            for f in extract_dir.rglob("*")
            if f.is_file() and not f.name.startswith(".")
        ]
        if not candidates:
            raise FileNotFoundError("No se encontraron archivos dentro del ZIP de EPEN")
        candidates.sort(key=lambda f: f.stat().st_size, reverse=True)
        chosen = candidates[0]
        log(
            f"Archivo de datos detectado en EPEN: {chosen.name} ({chosen.stat().st_size / (1024 * 1024):.2f} MB)"
        )

        if chosen.suffix.lower() in [".xlsx", ".xls"]:
            log("Convirtiendo Excel de EPEN a CSV...")
            import pandas as pd

            df = pd.read_excel(chosen)
            df.to_csv(out_csv, index=False, encoding="utf-8")
        else:
            shutil.move(str(chosen), str(out_csv))
        shutil.rmtree(extract_dir, ignore_errors=True)
        temp_file.unlink(missing_ok=True)
    else:
        # Si se descargó directo (no ZIP)
        try:
            import pandas as pd

            df = pd.read_excel(temp_file)
            log("EPEN detectado como Excel (.xlsx). Convirtiendo a CSV...")
            df.to_csv(out_csv, index=False, encoding="utf-8")
            temp_file.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            shutil.move(str(temp_file), str(out_csv))

    log(f"✓ EPEN guardado en partición: {out_csv}")
    return [out_csv], url


def process_see(_periodo: str, url: str) -> tuple[list[Path], str]:
    # Padrón SEE (SUNAT): una fila por RUC y tipo de comprobante electrónico con su
    # fecha de inicio. Basta una foto: gold filtra por fecha_inicio <= corte.
    temp_zip = BASE_DIR / "tmp" / "padron_see.zip"
    download_file(url, temp_zip)
    with zipfile.ZipFile(temp_zip) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise FileNotFoundError(f"ZIP del SEE sin un único CSV: {z.namelist()}")
        out_csv = BASE_DIR / "see" / "padron_see.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        with z.open(csvs[0]) as src, open(out_csv, "wb") as dst:
            shutil.copyfileobj(src, dst)
    temp_zip.unlink(missing_ok=True)
    log(f"✓ SEE guardado ({out_csv.stat().st_size / 1024**2:.2f} MB): {out_csv}")
    return [out_csv], url


HANDLERS: dict[str, Callable[[str, str], tuple[list[Path], str]]] = {
    "padron_ruc": process_padron_ruc,
    "ordenes_compra": process_ordenes_compra,
    "pricos": process_pricos,
    "ssco": process_ssco,
    "ingresos_tributarios": process_ingresos_tributarios,
    "epen": process_epen,
    "see": process_see,
}


# ---------------------------------------------------------------------------
# Handler genérico (caso B): descarga a raw/<fuente>/[anio=Y/mes=M/]<archivo>
# ---------------------------------------------------------------------------


def process_generic(fuente: str, periodo: str, url: str) -> tuple[list[Path], str]:
    out_dir = BASE_DIR / fuente
    m = re.fullmatch(r"(\d{4})-(\d{2})", periodo)
    if m:
        out_dir = out_dir / f"anio={m.group(1)}" / f"mes={m.group(2)}"
    out_dir.mkdir(parents=True, exist_ok=True)
    name = Path(urllib.parse.unquote(urllib.parse.urlparse(url).path)).name or fuente
    dest = out_dir / name
    download_file(url, dest)
    if dest.suffix.lower() != ".zip":
        return [dest], url
    with zipfile.ZipFile(dest) as z:
        z.extractall(out_dir)
        names = [n for n in z.namelist() if not n.endswith("/")]
    dest.unlink(missing_ok=True)
    files = [out_dir / n for n in names if not Path(n).name.startswith(".")]
    if not files:
        raise FileNotFoundError(f"ZIP de {fuente} sin archivos")
    return files, url


def snake(name: Any) -> str:
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _snake_cols(cols: list[Any]) -> list[str]:
    out: list[str] = []
    for i, c in enumerate(cols):
        base = snake(c) if c is not None else ""
        base = base or f"col_{i + 1}"
        name, n = base, 2
        while name in out:
            name, n = f"{base}_{n}", n + 1
        out.append(name)
    return out


def _table(cols: list[str], data: list[list[Any]], extras: dict[str, str]):
    import pyarrow as pa

    arrays = {c: pa.array(vals, type=pa.string()) for c, vals in zip(cols, data)}
    n = len(data[0]) if data else 0
    for k, v in extras.items():
        arrays[k] = pa.array([v] * n, type=pa.string())
    return pa.table(arrays)


def _write_blocks(blocks: Iterator[Any], path: Path) -> int:
    """Escribe bloques en un Parquet snappy (un row group por bloque). Devuelve filas."""
    import pyarrow.parquet as pq

    writer, rows = None, 0
    for t in blocks:
        if writer is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(path, t.schema, compression="snappy")
        writer.write_table(t)
        rows += t.num_rows
    if writer:
        writer.close()
    return rows


def _csv_blocks(path: Path, encoding: str, extras: dict[str, str]) -> Iterator[Any]:
    import pandas as pd

    with open(path, "r", encoding=encoding, errors="replace") as f:
        sample = f.read(64 * 1024)
    try:
        sep = csv.Sniffer().sniff(sample, delimiters=",;|\t").delimiter
    except csv.Error:
        sep = ","
    cols: list[str] | None = None
    for df in pd.read_csv(
        path, dtype=str, chunksize=CHUNK_ROWS, encoding=encoding, sep=sep
    ):
        cols = cols or _snake_cols(list(df.columns))
        data = [
            df[c].astype(object).where(df[c].notna(), None).tolist() for c in df.columns
        ]
        yield _table(cols, data, extras)


def _xlsx_blocks(ws: Any, extras: dict[str, str]) -> Iterator[Any]:
    rows = ws.iter_rows(values_only=True)
    header = next((r for r in rows if any(v is not None for v in r)), None)
    if header is None:
        return
    cols = _snake_cols(list(header))
    buf: list[tuple[Any, ...]] = []

    def flush() -> Any:
        data = [
            [None if i >= len(r) or r[i] is None else str(r[i]) for r in buf]
            for i in range(len(cols))
        ]
        return _table(cols, data, extras)

    for r in rows:
        buf.append(r)
        if len(buf) >= CHUNK_ROWS:
            yield flush()
            buf = []
    if buf:
        yield flush()


def convert_file(
    path: Path, fuente: str, periodo: str, out_root: Path, parts: dict[str, int]
) -> None:
    """Convierte un archivo a out_root/<tabla>/part-N.parquet (todo string)."""
    extras = {"_archivo_origen": path.name, "_periodo": periodo}
    suffix = path.suffix.lower()

    def dest(tabla: str) -> Path:
        i = parts.get(tabla, 0)
        parts[tabla] = i + 1
        return out_root / tabla / f"part-{i:05d}.parquet"

    if suffix in (".csv", ".txt", ".tsv"):
        target = dest(fuente)
        try:
            _write_blocks(_csv_blocks(path, "utf-8", extras), target)
        except UnicodeDecodeError:
            target.unlink(missing_ok=True)
            log(f"{path.name} no es utf-8; reintentando con latin-1")
            _write_blocks(_csv_blocks(path, "latin-1", extras), target)
    elif suffix in (".xlsx", ".xlsm"):
        import openpyxl

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            multi = len(wb.sheetnames) > 1
            for hoja in wb.sheetnames:
                if multi:
                    tabla = f"{fuente}__{snake(hoja) or 'hoja'}"
                    ex = extras
                else:
                    tabla, ex = fuente, {**extras, "_hoja": hoja}
                _write_blocks(_xlsx_blocks(wb[hoja], ex), dest(tabla))
        finally:
            wb.close()
    else:
        raise ValueError(f"Formato no soportado para conversión: {path.name}")


def convert_to_pendientes(fuente: str, periodo: str, files: list[Path]) -> list[str]:
    """Convierte y sube a s3://BRONZE/_pendientes/. Devuelve las <tabla> subidas."""
    out_root = BASE_DIR / "tmp" / "pendientes" / fuente
    shutil.rmtree(out_root, ignore_errors=True)
    parts: dict[str, int] = {}
    try:
        for f in sorted(files):
            convert_file(f, fuente, periodo, out_root, parts)
        if not out_root.exists():
            log(f"WARN: {fuente}/{periodo} no produjo filas; nada que convertir")
            return []
        tablas = sorted(p.name for p in out_root.iterdir() if p.is_dir())
        for tabla in tablas:
            pref = f"s3://{BRONZE_BUCKET}/_pendientes/{tabla}/"
            if periodo != "-":
                pref += f"periodo={periodo}/"
            # re-conversión: no duplicar filas en Athena
            _aws(["s3", "rm", pref, "--recursive", "--only-show-errors"])
            _aws(
                [
                    "s3",
                    "cp",
                    str(out_root / tabla),
                    pref,
                    "--recursive",
                    "--only-show-errors",
                ]
            )
            log(f"✓ {fuente}/{periodo} convertido a {pref}")
        return tablas
    finally:
        shutil.rmtree(out_root, ignore_errors=True)


def registered_fuentes() -> set[str]:
    if not FUENTES_FILE.exists():
        log(f"WARN: no existe {FUENTES_FILE}; ninguna fuente se considera registrada")
        return set()
    return set(json.loads(FUENTES_FILE.read_text(encoding="utf-8")).get("fuentes", {}))


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------


def collect_items(config: dict[str, Any]) -> list[tuple[str, str, str]]:
    """(fuente, periodo, url). small_datasets se aplana: subclave = fuente, periodo '-'."""
    items: list[tuple[str, str, str]] = []
    for fuente, val in config.items():
        if fuente == "small_datasets" and isinstance(val, dict):
            pares = [(sub, "-", u) for sub, u in val.items()]
        elif isinstance(val, dict):
            pares = [(fuente, p, u) for p, u in val.items()]
        elif isinstance(val, str):
            pares = [(fuente, "-", val)]
        else:
            continue
        items += [
            (f, p, u.strip()) for f, p, u in pares if isinstance(u, str) and u.strip()
        ]
    return items


def process_item(
    fuente: str,
    periodo: str,
    url: str,
    manifest: Manifest,
    force: bool,
    registered: set[str],
) -> None:
    entry = manifest.get(fuente, {}).get(periodo)
    if not needs_download(entry, url, force):
        return
    log(f"\n--- {fuente} {periodo} ---")
    handler = HANDLERS.get(fuente)
    if handler:
        files, used = handler(periodo, url)
    else:
        files, used = process_generic(fuente, periodo, url)
    sha = sha256_files(files)
    new = entry is None
    changed = entry is not None and entry.get("sha256") != sha
    val = head_validators(used)

    if new or changed:
        for of, ops in manifest.items():
            for op, oe in ops.items():
                if (of, op) != (fuente, periodo) and oe.get("sha256") == sha:
                    log(f"WARN: {fuente}/{periodo} es el mismo archivo que {of}/{op}")
        entry = {**(entry or {}), "sha256": sha, "ingested_at": _now()}
    else:
        assert entry is not None
        log(f"{fuente}/{periodo}: mismo sha, solo se actualizan validadores")
    entry.update(
        {"etag": val["etag"], "last_modified": val["last_modified"], "url": used}
    )
    manifest.setdefault(fuente, {})[periodo] = entry

    convertir = (
        handler is None
        and fuente not in registered
        and (new or changed or entry.get("conversion") == "error")
    )
    if convertir:
        try:
            tablas = convert_to_pendientes(fuente, periodo, files)
            for t in sorted(set(entry.get("tablas", [])) - set(tablas)):
                log(
                    f"WARN: s3://{BRONZE_BUCKET}/_pendientes/{t}/ quedó huérfano "
                    f"({fuente}/{periodo} ya no lo produce). Bórralo a mano y su "
                    f"tabla dataset_pendiente_{t} del catálogo."
                )
            entry["tablas"] = tablas
            entry["conversion"] = "ok"
        except Exception as e:  # noqa: BLE001
            log(
                f"ERROR: conversión de {fuente}/{periodo} falló ({e}); la ingesta sigue"
            )
            entry["conversion"] = "error"
    if not (new or changed):
        for f in files:  # contenido idéntico: nada que subir a raw
            f.unlink(missing_ok=True)


def upload_to_s3() -> list[str]:
    log(f"Iniciando sincronización con S3: s3://{RAW_BUCKET}/...")
    uploaded_summary = []
    subdirs = sorted(
        p.name
        for p in BASE_DIR.iterdir()
        if p.is_dir() and p.name != "tmp" and not p.name.startswith("_")
    )
    for sub in subdirs:
        local_path = BASE_DIR / sub
        s3_path = f"s3://{RAW_BUCKET}/{sub}/"
        log(f"Sincronizando {sub} hacia {s3_path}...")
        _aws(["s3", "sync", str(local_path), s3_path, "--only-show-errors"])
        data_files = [f for f in local_path.rglob("*") if f.is_file()]
        total_mb = sum(f.stat().st_size for f in data_files) / (1024 * 1024)
        uploaded_summary.append(
            f"{sub}: {len(data_files)} archivos ({total_mb:.2f} MB)"
        )
        log(
            f"✓ {sub} sincronizado exitosamente ({len(data_files)} archivos, {total_mb:.2f} MB)."
        )
    return uploaded_summary


def write_last_run_errors(manifest: Manifest) -> None:
    """Fuentes con conversión fallida (incluye las no reintentadas); el workflow avisa."""
    errores = sorted(
        f"{f}/{p}"
        for f, ps in manifest.items()
        for p, e in ps.items()
        if e.get("conversion") == "error"
    )
    path = BASE_DIR / "tmp" / "last_run_errors.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(errores))
    _aws(
        [
            "s3",
            "cp",
            str(path),
            f"s3://{RAW_BUCKET}/_runs/last_run_errors.json",
            "--only-show-errors",
        ]
    )


def write_success(uploaded_summary: list[str]) -> None:
    success_marker = BASE_DIR / "_INGESTION_SUCCESS"
    summary_text = (
        f"STATUS=SUCCESS\n"
        f"COMPLETED_AT={time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"RAW_BUCKET={RAW_BUCKET}\n"
        f"SUMMARY=\n" + "\n".join(f"  - {s}" for s in uploaded_summary) + "\n"
    )
    success_marker.write_text(summary_text)
    _aws(["s3", "cp", str(success_marker), f"s3://{RAW_BUCKET}/_INGESTION_SUCCESS"])
    log(
        "✓ Señal de éxito y resumen subida a s3://" + RAW_BUCKET + "/_INGESTION_SUCCESS"
    )


def main():
    log("=== INICIANDO PROCESO DE INGESTA EFÍMERA ===")
    if not URLS_FILE.exists():
        raise FileNotFoundError(f"No se encontró el archivo de URLs: {URLS_FILE}")

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    items = collect_items(config)
    manifest = load_manifest()
    if manifest is None:
        manifest = bootstrap_manifest(items)
    force = os.environ.get("FORCE_REDOWNLOAD", "").lower() in ("1", "true")
    if force:
        log("FORCE_REDOWNLOAD=1: se ignora el manifest para decidir")
    registered = registered_fuentes()

    vigentes = {(f, p) for f, p, _ in items}
    for fuente, periodos in manifest.items():
        for p in periodos:
            if (fuente, p) not in vigentes:
                log(
                    f"WARN: {fuente}/{p} está en el manifest pero ya no en urls.json (se conserva)"
                )

    for fuente, periodo, url in items:
        process_item(fuente, periodo, url, manifest, force, registered)

    summary = upload_to_s3()
    save_manifest(manifest)
    write_last_run_errors(manifest)
    write_success(summary)
    log("\n=== INGESTA COMPLETADA EXITOSAMENTE ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        log(f"ERROR CRÍTICO: {e}")
        import traceback

        traceback.print_exc()
        # Reportar fallo a S3
        fail_marker = BASE_DIR / "_INGESTION_FAILED"
        fail_marker.write_text(f"ERROR={e!s}\n")
        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                str(fail_marker),
                f"s3://{RAW_BUCKET}/_INGESTION_FAILED",
            ],
            check=False,
        )
        sys.exit(1)
