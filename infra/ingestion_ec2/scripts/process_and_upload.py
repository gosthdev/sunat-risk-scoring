#!/usr/bin/env python3
"""
Script de procesamiento, conversión y carga a S3 ejecutado en la EC2 efímera.

1. Lee /opt/ingestion/urls.json.
2. Descarga archivos con streaming y resume si es posible.
3. Descomprime zips de Padrón RUC -> anio=YYYY/mes=MM/padron.csv.
4. Convierte Excel de Órdenes de Compra (.xlsx) -> anio=YYYY/mes=MM/ordenes.csv.
5. Descarga fuentes pequeñas (PRICOS, SSCO, EPEN, Ingresos Tributarios).
6. Sincroniza hacia s3://${RAW_BUCKET}/ respetando la estructura Hive.
7. Emite marcador de finalización exitosa s3://${RAW_BUCKET}/_INGESTION_SUCCESS.
"""

import csv
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

BASE_DIR = Path("/data")
URLS_FILE = Path("/opt/ingestion/urls.json")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "sunat-risk-scoring-raw")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def log(msg: str):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


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


def s3_object_size(s3_uri: str) -> int:
    """Retorna el tamaño en bytes del objeto en S3 si existe, o 0 si no existe."""
    cmd = ["aws", "s3", "ls", s3_uri]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode == 0 and res.stdout.strip():
        parts = res.stdout.strip().split()
        if len(parts) >= 3:
            try:
                return int(parts[2])
            except ValueError:
                pass
    return 0


def process_padron_ruc(key: str, url: str):
    if not url:
        return

    # key esperada: "2025-01" o similar
    partes = key.split("-")
    anio = partes[0]
    mes = partes[1].zfill(2) if len(partes) > 1 else "01"

    # Verificar si ya existe en S3 con datos reales (> 1 MB)
    s3_target = f"s3://{RAW_BUCKET}/padron_ruc/anio={anio}/mes={mes}/padron.csv"
    existing_size = s3_object_size(s3_target)
    if existing_size > 1024 * 1024:
        log(
            f"✓ Padrón RUC {key} ya existe en S3 ({existing_size / (1024 * 1024):.2f} MB). Omitiendo descarga."
        )
        return

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


def _download_osce_xlsx(url_or_key: str, anio: str, mes: str, dest_path: Path):
    """Descarga el .xlsx de OSCE validando que el archivo comience con b'PK'."""
    import base64
    import urllib.parse

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
    for var in variantes:
        candidate_urls.append(f"{base_osce}CONOSCE_ORDENESCOMPRA{var}{anio}_0.xlsx")

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
                        f"Respuesta no es Excel válido (magic bytes: {magic}). Saltando {candidate}..."
                    )
                    continue

                # Escribir contenido completo
                with open(dest_path, "wb") as out_f:
                    out_f.write(magic)
                    shutil.copyfileobj(resp, out_f)

                size_mb = dest_path.stat().st_size / (1024 * 1024)
                log(f"✓ Excel descargado exitosamente ({size_mb:.2f} MB): {candidate}")
                return
        except Exception as e:
            log(f"Fallo al intentar {candidate}: {e}")

    raise RuntimeError(
        f"No se pudo descargar un archivo Excel válido (.xlsx) para {anio}-{mes} tras probar variantes."
    )


def process_ordenes_compra(key: str, url: str):
    if not url:
        return

    partes = key.split("-")
    anio = partes[0]
    mes = partes[1].zfill(2) if len(partes) > 1 else "01"

    # Verificar si ya existe en S3 con datos reales (> 5 MB)
    s3_target = f"s3://{RAW_BUCKET}/ordenes_compra/anio={anio}/mes={mes}/ordenes.csv"
    existing_size = s3_object_size(s3_target)
    if existing_size > 5 * 1024 * 1024:
        log(
            f"✓ Órdenes de Compra {key} ya existe en S3 ({existing_size / (1024 * 1024):.2f} MB). Omitiendo descarga."
        )
        return

    out_dir = BASE_DIR / "ordenes_compra" / f"anio={anio}" / f"mes={mes}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "ordenes.csv"

    temp_xlsx = BASE_DIR / "tmp" / f"ordenes_{anio}_{mes}.xlsx"
    _download_osce_xlsx(url, anio, mes, temp_xlsx)

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


def process_small_datasets(small_dict: dict):
    # PRICOS
    if small_dict.get("pricos"):
        s3_pricos = f"s3://{RAW_BUCKET}/pricos/principalesContrib-PRICOS.xlsx"
        size_p = s3_object_size(s3_pricos)
        if size_p > 1024 * 10:
            log(
                f"✓ PRICOS ya existe en S3 ({size_p / 1024:.2f} KB). Omitiendo descarga."
            )
        else:
            dest = BASE_DIR / "pricos" / "principalesContrib-PRICOS.xlsx"
            download_file(small_dict["pricos"], dest)

    # SSCO
    if small_dict.get("ssco"):
        s3_ssco = f"s3://{RAW_BUCKET}/ssco/sujesincapacidadOperativa.xlsx"
        size_s = s3_object_size(s3_ssco)
        if size_s > 1024 * 10:
            log(f"✓ SSCO ya existe en S3 ({size_s / 1024:.2f} KB). Omitiendo descarga.")
        else:
            dest = BASE_DIR / "ssco" / "sujesincapacidadOperativa.xlsx"
            download_file(small_dict["ssco"], dest)

    # Ingresos Tributarios
    if small_dict.get("ingresos_tributarios"):
        s3_ing = f"s3://{RAW_BUCKET}/ingresos_tributarios/cdrA13_tabular.csv"
        size_i = s3_object_size(s3_ing)
        if size_i > 1024 * 10:
            log(
                f"✓ Ingresos Tributarios ya existe en S3 ({size_i / 1024:.2f} KB). Omitiendo descarga."
            )
        else:
            url = small_dict["ingresos_tributarios"].strip()
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

    # EPEN
    if small_dict.get("epen"):
        s3_epen = f"s3://{RAW_BUCKET}/epen/anio=2025/epen.csv"
        size_e = s3_object_size(s3_epen)
        if size_e > 1024 * 100:
            log(f"✓ EPEN ya existe en S3 ({size_e / 1024:.2f} KB). Omitiendo descarga.")
        else:
            url = small_dict["epen"].strip()
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
                    raise FileNotFoundError(
                        "No se encontraron archivos dentro del ZIP de EPEN"
                    )
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


def upload_to_s3():
    log(f"Iniciando sincronización con S3: s3://{RAW_BUCKET}/...")

    subdirs = [
        "padron_ruc",
        "ordenes_compra",
        "pricos",
        "ssco",
        "ingresos_tributarios",
        "epen",
    ]
    uploaded_summary = []

    for sub in subdirs:
        local_path = BASE_DIR / sub
        if local_path.exists():
            s3_path = f"s3://{RAW_BUCKET}/{sub}/"
            log(f"Sincronizando {sub} hacia {s3_path}...")
            cmd = ["aws", "s3", "sync", str(local_path), s3_path, "--only-show-errors"]
            subprocess.run(cmd, check=True)
            files = list(local_path.rglob("*"))
            data_files = [f for f in files if f.is_file()]
            total_mb = sum(f.stat().st_size for f in data_files) / (1024 * 1024)
            uploaded_summary.append(
                f"{sub}: {len(data_files)} archivos ({total_mb:.2f} MB)"
            )
            log(
                f"✓ {sub} sincronizado exitosamente ({len(data_files)} archivos, {total_mb:.2f} MB)."
            )

    # Marcar éxito y bitácora estructurada en S3
    success_marker = BASE_DIR / "_INGESTION_SUCCESS"
    summary_text = (
        f"STATUS=SUCCESS\n"
        f"COMPLETED_AT={time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"RAW_BUCKET={RAW_BUCKET}\n"
        f"SUMMARY=\n" + "\n".join(f"  - {s}" for s in uploaded_summary) + "\n"
    )
    success_marker.write_text(summary_text)
    subprocess.run(
        [
            "aws",
            "s3",
            "cp",
            str(success_marker),
            f"s3://{RAW_BUCKET}/_INGESTION_SUCCESS",
        ],
        check=True,
    )
    log(
        "✓ Señal de éxito y resumen subida a s3://" + RAW_BUCKET + "/_INGESTION_SUCCESS"
    )


def main():
    log("=== INICIANDO PROCESO DE INGESTA EFÍMERA ===")
    if not URLS_FILE.exists():
        raise FileNotFoundError(f"No se encontró el archivo de URLs: {URLS_FILE}")

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Padrón RUC
    padron_items = config.get("padron_ruc", {})
    for key, url in padron_items.items():
        if url.strip():
            log(f"\n--- Procesando Padrón RUC: {key} ---")
            process_padron_ruc(key, url.strip())

    # Órdenes de Compra
    ordenes_items = config.get("ordenes_compra", {})
    for key, url in ordenes_items.items():
        if url.strip():
            log(f"\n--- Procesando Órdenes de Compra: {key} ---")
            process_ordenes_compra(key, url.strip())

    # Small Datasets
    small_items = config.get("small_datasets", {})
    if any(bool(v.strip()) for v in small_items.values() if isinstance(v, str)):
        log("\n--- Procesando Datasets Pequeños ---")
        process_small_datasets(small_items)

    # Subida a S3
    upload_to_s3()
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
