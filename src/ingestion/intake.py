"""
Recepción de cargas subidas por el portal web (corre en el runner).

Lee s3://RAW/_inbox/{fuente}/{periodo}/{carga_id}/{archivo}, valida nombre y contenido,
y deja el archivo donde la EC2 lo habría dejado (process_and_upload.raw_key). Solo acepta
fuentes con job en fuentes.json y con las columnas que su lector de raw exige. Registra el
manifest (url "manual") y escribe cargas_lote.json:
[{carga_id, fuente, periodo, resultado: ok|duplicada|rechazada, motivo?}].
Mismo sha en el manifest -> duplicada; sha distinto -> reemplaza y reprocesa.
Inválidos -> _inbox/_rechazados/{misma ruta} + {archivo}.txt con el motivo.

Uso: python -m src.ingestion.intake [salida.json]
Env: RAW_BUCKET, AWS_REGION, URLS_FILE (def. src/ingestion/urls.json),
     FUENTES_FILE (def. config/fuentes.json), CARGAS_LOTE (def. cargas_lote.json).
"""

import contextlib
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from src.ingestion import process_and_upload as pu

RAIZ = Path(__file__).resolve().parents[2]
URLS_FILE = Path(os.environ.get("URLS_FILE", RAIZ / "src" / "ingestion" / "urls.json"))
FUENTES_FILE = Path(os.environ.get("FUENTES_FILE", RAIZ / "config" / "fuentes.json"))
INBOX = "_inbox/"
RECHAZADOS = "_inbox/_rechazados/"
NOMBRE_OK = re.compile(r"[A-Za-z0-9._-]+")
UUID_OK = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
PERIODO_OK = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])|-")
MAX_DESCOMPRIMIDO = 2 * 1024**3
MAX_RATIO = 100
MAX_SUBIDA = 5 * 1024**3
# Extensiones que la EC2 sabe procesar por fuente; el resto solo exige .csv o .xlsx
ACEPTA = {
    "padron_ruc": {"csv"},
    "ordenes_compra": {"csv", "xlsx"},
    "pricos": {"xlsx"},
    "ssco": {"xlsx"},
    "ingresos_tributarios": {"csv", "xlsx"},
    "epen": {"csv", "xlsx"},
    "see": {"csv"},
}
RESPALDO = "_tmp_intake/"  # copias de raw previas a pisarlas; se borran al terminar
MOTIVO_REEMPLAZADA = "Reemplazado por otro archivo del mismo envío."
MOTIVO_INTERNO = "Error interno al procesar; vuelve a intentarlo."
MOTIVO_LECTURA = (
    "No se pudo leer el archivo. Revise que tenga el formato correcto de esta fuente "
    "y súbalo de nuevo."
)
MOTIVO_INGRESOS = "El Excel no tiene el formato de Ingresos Tributarios de SUNAT."
MOTIVO_RUTA = (
    "La carga no tiene la estructura esperada (fuente/periodo/carga/archivo). "
    "Súbala de nuevo desde el portal."
)


class Rechazo(Exception):
    """El mensaje es el motivo, en español llano para quien subió el archivo."""


def validar_ruta(
    fuente: str, periodo: str, carga_id: str, archivo: str, registradas: set[str]
) -> None:
    for n in (fuente, carga_id, archivo):
        if not NOMBRE_OK.fullmatch(n) or ".." in n:
            raise Rechazo(
                "El nombre del archivo o de la carpeta tiene caracteres no permitidos. "
                "Use solo letras, números, punto, guion y guion bajo, y súbalo de nuevo."
            )
    if fuente not in registradas:
        raise Rechazo(f"Fuente no reconocida: {fuente}")
    if not PERIODO_OK.fullmatch(periodo) or (
        fuente in ("padron_ruc", "ordenes_compra") and periodo == "-"
    ):
        raise Rechazo(
            "El periodo indicado no es válido. Debe ser año-mes, por ejemplo 2025-03."
        )
    if fuente in pu.SMALL_RAW_KEYS and periodo != "-":
        raise Rechazo("Esta fuente no se carga por periodo. Súbala sin indicar mes.")
    ok = ACEPTA.get(fuente)
    if ok and archivo.lower().rpartition(".")[2] not in ok:
        raise Rechazo(
            "Para esta fuente se espera un archivo "
            + " o ".join(f".{e}" for e in sorted(ok))
            + "."
        )


def validar_contenido(path: Path, archivo: str) -> None:
    size = path.stat().st_size
    if size == 0:
        raise Rechazo("El archivo está vacío.")
    ext = archivo.lower().rpartition(".")[2]
    if ext == "xlsx":
        with open(path, "rb") as f:
            magic = f.read(4)
        if magic != b"PK\x03\x04":
            raise Rechazo(
                "El archivo no es un Excel (.xlsx) válido. Ábralo en Excel, guárdelo "
                "como .xlsx y súbalo de nuevo."
            )
        try:
            with zipfile.ZipFile(path) as z:
                nombres = z.namelist()
                total = sum(i.file_size for i in z.infolist())
        except zipfile.BadZipFile:
            raise Rechazo(
                "El archivo Excel está dañado y no se puede abrir. Vuelva a guardarlo y súbalo de nuevo."
            ) from None
        if "[Content_Types].xml" not in nombres:
            raise Rechazo(
                "El archivo no tiene la estructura de un Excel (.xlsx) válido."
            )
        if total > MAX_DESCOMPRIMIDO or total > MAX_RATIO * size:
            raise Rechazo(
                "El archivo Excel es demasiado grande al abrirlo y no se puede procesar. "
                "Divídalo en archivos más pequeños."
            )
    elif ext == "csv":
        with open(path, "rb") as f:
            if b"\0" in f.read(64 * 1024):
                raise Rechazo(
                    "El archivo no parece un CSV de texto (contiene datos binarios). "
                    "Expórtelo de nuevo como CSV y súbalo."
                )
        # utf-8 o latin-1: latin-1 decodifica cualquier byte, no hay nada más que validar
    else:
        raise Rechazo("Solo se aceptan archivos .xlsx o .csv.")


def columnas_esperadas(fuente: str) -> list[str]:
    """Columnas que el lector de raw de la fuente exige, tomadas de su definición."""
    comun = str(RAIZ / "src" / "spark" / "common")
    if comun not in sys.path:
        sys.path.insert(0, comun)
    if fuente in ("padron_ruc", "ordenes_compra"):  # bronze: enforceSchema=false
        import schema_definitions as sd

        esquema = (
            sd.PADRON_RUC_SCHEMA if fuente == "padron_ruc" else sd.ORDENES_COMPRA_SCHEMA
        )
        return list(esquema.names)
    if fuente == "see":
        from fuentes.small.see import SEE_COLUMNAS

        return list(SEE_COLUMNAS)
    if fuente == "epen":
        from fuentes.small.epen import EPEN_DTYPES

        return list(EPEN_DTYPES)
    if fuente == "pricos":
        from fuentes.small.pricos import PRICOS_COLUMNAS

        return list(PRICOS_COLUMNAS)
    if (
        fuente == "ssco"
    ):  # bronze_ssco: fecha de emisión (subcadena) y silver: columna RUC
        from fuentes.small.ssco import SSCO_COLUMNAS

        return ["RUC"] + [t for t, d in SSCO_COLUMNAS.items() if d == "fecha_emision"]
    if fuente == "ingresos_tributarios":
        # El formato lo valida la conversión (conv_ingresos); no hay columnas que exigir.
        # shortcut: el CSV de variación no se refresca con cargas del portal, nada lo lee; mejorar si aparece un consumidor.
        return []
    raise ValueError(f"Fuente sin regla de columnas: {fuente}")


def validar_columnas(path: Path, fuente: str) -> None:
    esperadas = columnas_esperadas(fuente)
    if not esperadas:
        return
    import pandas as pd

    try:
        if path.suffix == ".xlsx":
            cab = pd.read_excel(path, nrows=0).columns
        else:
            try:
                cab = pd.read_csv(path, nrows=0).columns
            except UnicodeDecodeError:
                cab = pd.read_csv(path, nrows=0, encoding="latin-1").columns
    except Exception as e:  # noqa: BLE001
        pu.log(f"WARN: no se pudo leer la cabecera de {fuente} ({e})")
        raise Rechazo(MOTIVO_LECTURA) from None
    cols = [str(c) for c in cab]
    if fuente in ("padron_ruc", "ordenes_compra"):
        # Spark (enforceSchema=false) compara la cabecera por posición, sin distinguir mayúsculas
        if [c.lower() for c in cols] == [e.lower() for e in esperadas]:
            return
        raise Rechazo(
            f"El archivo no tiene las columnas esperadas para {fuente}: deben ser "
            + ", ".join(esperadas)
            + ", en ese orden y sin columnas adicionales"
        )
    if fuente == "ssco":
        faltan = [
            e
            for e in esperadas
            if not (
                e in (c.strip().upper() for c in cols)
                if e == "RUC"
                else any(e in c.lower() for c in cols)
            )
        ]
    else:
        faltan = [e for e in esperadas if e not in cols]  # pandas: nombres exactos
    if faltan:
        raise Rechazo(
            f"El archivo no tiene las columnas esperadas para {fuente}: faltan "
            + ", ".join(faltan)
        )


def _existe(s3: Any, key: str) -> bool:
    try:
        s3.head_object(Bucket=pu.RAW_BUCKET, Key=key)
        return True
    except Exception as e:
        if _codigo(e) in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def rechazar(s3: Any, key: str, motivo: str) -> None:
    dest = RECHAZADOS + key.removeprefix(INBOX)
    s3.copy({"Bucket": pu.RAW_BUCKET, "Key": key}, pu.RAW_BUCKET, dest)
    s3.put_object(Bucket=pu.RAW_BUCKET, Key=dest + ".txt", Body=motivo.encode())
    s3.delete_object(Bucket=pu.RAW_BUCKET, Key=key)


def listar(s3: Any) -> list[str]:
    claves: list[str] = []
    token: dict[str, str] = {}
    while True:
        r = s3.list_objects_v2(Bucket=pu.RAW_BUCKET, Prefix=INBOX, **token)
        claves += [
            o["Key"]
            for o in r.get("Contents", [])
            if not o["Key"].startswith(RECHAZADOS) and not o["Key"].endswith("/")
        ]
        if not r.get("IsTruncated"):
            return claves
        token = {"ContinuationToken": r["NextContinuationToken"]}


def preparar(
    s3: Any,
    key: str,
    local: Path,
    out_root: Path,
    fuente: str,
    periodo: str,
    archivo: str,
    registradas: set[str],
) -> tuple[str, Path | None, str]:
    """Valida y deja listo localmente lo que irá a raw: (clave, archivo|None, sha).
    archivo None = se copia tal cual desde _inbox. Aquí no se escribe nada en S3."""
    validar_ruta(
        fuente,
        periodo,
        archivo=archivo,
        carga_id=key.split("/")[3],
        registradas=registradas,
    )
    if s3.head_object(Bucket=pu.RAW_BUCKET, Key=key)["ContentLength"] > MAX_SUBIDA:
        raise Rechazo("El archivo es demasiado grande (máximo 5 GB).")
    s3.download_file(pu.RAW_BUCKET, key, str(local))
    validar_contenido(local, archivo)
    clave = pu.raw_key(fuente, periodo)
    assert clave  # toda fuente registrada tiene layout raw
    conv = _conv_epen if fuente == "epen" else pu.CONVERSORES.get(fuente)
    if conv is None:
        validar_columnas(local, fuente)
        return clave, None, pu.sha256_file(local)
    # misma conversión que la EC2 (process_and_upload.CONVERSORES) antes de escribir raw
    shutil.rmtree(out_root, ignore_errors=True)
    try:
        (convertido,) = conv(local, out_root / clave.rpartition("/")[0])
    except Exception as e:  # noqa: BLE001
        pu.log(f"WARN: conversión de {key} falló ({e})")
        raise Rechazo(
            MOTIVO_INGRESOS if fuente == "ingresos_tributarios" else MOTIVO_LECTURA
        ) from None
    if fuente == "ordenes_compra" and convertido.stat().st_size < pu.MIN_ORDENES:
        raise Rechazo(
            "El archivo de órdenes de compra está vacío o incompleto. Revíselo y súbalo de nuevo."
        )
    validar_columnas(convertido, fuente)
    return clave, convertido, pu.sha256_file(convertido)


def _s3() -> Any:
    import boto3

    return boto3.client("s3", region_name=pu.AWS_REGION)


def _codigo(e: Exception) -> str:
    return str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", ""))


def _zip_de_datos(src: Path) -> bool:
    """ZIP con datos dentro; un .xlsx también es ZIP pero se lee como Excel."""
    if not zipfile.is_zipfile(src):
        return False
    with zipfile.ZipFile(src) as z:
        return "[Content_Types].xml" not in z.namelist()


def _conv_epen(src: Path, out_dir: Path) -> list[Path]:
    return pu.conv_epen(src, out_dir, _zip_de_datos)


def main(salida: Path) -> list[dict[str, str]]:
    pu.BASE_DIR = Path(tempfile.mkdtemp())  # en el runner /data no existe
    s3 = _s3()
    items = pu.collect_items(json.loads(URLS_FILE.read_text(encoding="utf-8")))
    manifest = pu.load_manifest()
    if manifest is None:  # igual que la EC2: sin esto su bootstrap quedaría anulado
        manifest = pu.bootstrap_manifest(items)
    base = json.loads(json.dumps(manifest))
    registradas = set(json.loads(FUENTES_FILE.read_text(encoding="utf-8"))["fuentes"])
    resultados: list[dict[str, str]] = []
    borrar: list[str] = []  # originales de _inbox, tras guardar el manifest
    creadas: list[str] = []  # claves raw nuevas de esta corrida
    respaldos: dict[
        str, str
    ] = {}  # clave raw preexistente -> copia temporal previa a pisarla
    # shortcut: agrupa por corrida de intake, no por lote_id; cargas del mismo periodo en
    # corridas distintas se reemplazan. Cambiar si el portal envía un lote_id.
    ultimas: dict[
        tuple[str, str], tuple[str, dict[str, str]]
    ] = {}  # (fuente, periodo) -> (sha, fila) de la última que escribió raw

    def restaurar(claves: list[str]) -> None:
        for c in claves:
            if c in respaldos:
                s3.copy(
                    {"Bucket": pu.RAW_BUCKET, "Key": respaldos[c]}, pu.RAW_BUCKET, c
                )
            else:
                s3.delete_object(Bucket=pu.RAW_BUCKET, Key=c)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            for key in listar(s3):
                partes = key.split("/")
                if len(partes) != 5 or not UUID_OK.fullmatch(partes[3]):
                    rechazar(s3, key, MOTIVO_RUTA)
                    continue
                _, fuente, periodo, carga_id, archivo = partes
                r = {"carga_id": carga_id, "fuente": fuente, "periodo": periodo}
                # nombre local fijo: el del usuario nunca toca el disco
                ext = archivo.lower().rpartition(".")[2]
                local = Path(tmp) / f"carga.{ext if ext in ('csv', 'xlsx') else 'bin'}"
                tocadas: list[str] = []  # claves que este item creó o pisó
                try:
                    c, archivo_conv, sha = preparar(
                        s3,
                        key,
                        local,
                        Path(tmp) / "out",
                        fuente,
                        periodo,
                        archivo,
                        registradas,
                    )
                    previa = (base.get(fuente, {}).get(periodo) or {}).get("sha256")
                    ultima = ultimas.get((fuente, periodo))
                    if sha == (ultima[0] if ultima else previa):
                        r["resultado"] = "duplicada"
                    else:
                        if c not in creadas and c not in respaldos:
                            if _existe(s3, c):  # se podrá deshacer si el manifest falla
                                bk = f"{RESPALDO}{c}"
                                s3.copy(
                                    {"Bucket": pu.RAW_BUCKET, "Key": c},
                                    pu.RAW_BUCKET,
                                    bk,
                                )
                                respaldos[c] = bk
                            else:
                                creadas.append(c)
                            tocadas.append(c)
                        if archivo_conv is None:
                            s3.copy(
                                {"Bucket": pu.RAW_BUCKET, "Key": key}, pu.RAW_BUCKET, c
                            )
                        else:
                            s3.upload_file(str(archivo_conv), pu.RAW_BUCKET, c)
                        if ultima:  # misma clave canónica: raw se queda con la última
                            ultima[1].update(
                                resultado="duplicada", motivo=MOTIVO_REEMPLAZADA
                            )
                        ultimas[(fuente, periodo)] = (sha, r)
                        r["resultado"] = "ok"
                    borrar.append(key)  # tras guardar el manifest
                except Rechazo as e:
                    rechazar(s3, key, str(e))
                    r.update(resultado="rechazada", motivo=str(e))
                except Exception as e:  # noqa: BLE001
                    pu.log(f"ERROR: {key} falló ({e!r}); se rechaza y se sigue")
                    try:
                        rechazar(s3, key, MOTIVO_INTERNO)
                    finally:  # nunca raw sin manifest, aunque rechazar también falle
                        restaurar(tocadas)
                        creadas[:] = [c for c in creadas if c not in tocadas]
                    r.update(resultado="rechazada", motivo=MOTIVO_INTERNO)
                resultados.append(r)

        for (fuente, periodo), (sha, _) in ultimas.items():
            manifest.setdefault(fuente, {})[periodo] = {
                **(base.get(fuente, {}).get(periodo) or {}),
                "sha256": sha,
                "etag": None,
                "last_modified": None,
                "url": "manual",
                "ingested_at": pu._now(),
            }
        if manifest != base:
            pu.save_manifest(manifest)
    except BaseException:
        for c in [
            *creadas,
            *respaldos,
        ]:  # raw vuelve a como estaba: sin manifest no vale
            with contextlib.suppress(Exception):
                restaurar([c])
        raise
    finally:
        for bk in respaldos.values():
            with contextlib.suppress(Exception):
                s3.delete_object(Bucket=pu.RAW_BUCKET, Key=bk)
    for key in borrar:
        s3.delete_object(Bucket=pu.RAW_BUCKET, Key=key)
    salida.write_text(
        json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return resultados


if __name__ == "__main__":
    main(
        Path(
            sys.argv[1]
            if len(sys.argv) > 1
            else os.environ.get("CARGAS_LOTE", "cargas_lote.json")
        )
    )
