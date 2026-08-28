"""Aterrizaje del audio crudo (MP3) en la zona Bronce del lago MinIO.

Observación del tutor (2026-08-22): guardar en Bronce las grabaciones **crudas** como
evidencia de auditoría, no solo el índice. Este módulo copia los MP3 del alcance
(200–299/OUT) desde el servidor Asterisk **por SFTP en ESTRICTO SOLO LECTURA, por ruta
EXACTA** (sin escanear carpetas, sin abrir el CDR) hacia `s3a://bronce/audio/date=.../`.

Reglas de seguridad (call center en producción):
- NUNCA se escribe ni se borra en el servidor Asterisk ni en su base de datos.
- Acceso por ruta exacta tomada del índice ya construido (Fase 1) → impacto mínimo.
- Idempotente: si el objeto ya está en MinIO con el mismo tamaño, se omite.
- Con estrangulamiento (`sleep`) para no saturar el disco de grabaciones en hora pico.

Lo usan el activo batch `bronze_audio` y el flujo de streaming (Fase 5).
"""
import io
import os
import time

import pandas as pd

from .config import (
    BRONCE_BUCKET,
    s3_storage_options,
)


def _s3fs():
    import s3fs

    opt = s3_storage_options()
    return s3fs.S3FileSystem(
        key=opt["key"], secret=opt["secret"], client_kwargs=opt["client_kwargs"]
    )


def audio_key(day: str, basename: str) -> str:
    """Ruta del objeto MP3 en la zona Bronce."""
    return f"{BRONCE_BUCKET}/audio/date={day}/{basename}"


def paths_del_dia(day: str) -> list[str]:
    """Rutas exactas (en Asterisk) de las grabaciones del alcance para `day`,
    leídas del índice ya materializado en Bronce (`bronze_audio_index`). NO toca
    el servidor: el índice se construyó una sola vez en la Fase 1."""
    uri = f"s3://{BRONCE_BUCKET}/audio_index/fecha={day}/"
    try:
        df = pd.read_parquet(uri, storage_options=s3_storage_options())
    except Exception:
        return []
    col = "path" if "path" in df.columns else df.columns[0]
    return [p for p in df[col].astype(str).tolist() if p]


def land_paths(paths: list[str], day: str, *, sleep_s: float = 0.05,
               limit: int = 0, sftp=None, log=print) -> dict:
    """Copia por SFTP (SOLO LECTURA, ruta exacta) las `paths` dadas → Bronce/MinIO.

    Args:
        paths: rutas remotas exactas en el servidor Asterisk.
        day: partición 'YYYY-MM-DD' (define el prefijo del objeto en MinIO).
        sleep_s: pausa entre archivos (estrangulamiento anti-saturación).
        limit: 0 = todas; N = solo las primeras N (para pruebas E2E rápidas).
        sftp: cliente SFTP ya abierto (para reusar conexión en streaming); si es
              None, se abre y cierra uno propio con las credenciales del `.env`.
        log: función de log.

    Returns:
        dict con métricas (subidos, omitidos, fallidos, bytes).
    """
    if limit:
        paths = paths[:limit]
    fs = _s3fs()

    cerrar = False
    cli = None
    if sftp is None:
        import paramiko

        host = os.environ["AST_SSH_HOST"]
        user = os.environ["AST_SSH_USER"]
        pw = os.environ["AST_SSH_PASSWORD"]
        port = int(os.getenv("AST_SSH_PORT", "22"))
        cli = paramiko.SSHClient()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cli.connect(host, port=port, username=user, password=pw, timeout=20)
        sftp = cli.open_sftp()
        cerrar = True

    up = skip = fail = nbytes = 0
    try:
        for i, r in enumerate(paths, 1):
            base = os.path.basename(r)
            key = audio_key(day, base)
            # idempotencia: si ya está con el mismo tamaño, omitir
            try:
                remote_sz = sftp.stat(r).st_size  # lectura de metadatos (solo lectura)
            except Exception as e:  # noqa: BLE001
                fail += 1
                log(f"  [FALLO stat] {r} -> {e}")
                continue
            if fs.exists(key) and fs.info(key).get("size") == remote_sz:
                skip += 1
                continue
            try:
                buf = io.BytesIO()
                sftp.getfo(r, buf)          # lectura remota → memoria (NO escribe en Asterisk)
                data = buf.getvalue()
                fs.pipe(key, data)          # subida a MinIO (nuestro lago)
                up += 1
                nbytes += len(data)
            except Exception as e:  # noqa: BLE001
                fail += 1
                log(f"  [FALLO get/put] {r} -> {e}")
            if sleep_s:
                time.sleep(sleep_s)          # estrangulamiento
            if i % 100 == 0:
                log(f"  {i}/{len(paths)} subidos={up} omitidos={skip} fallidos={fail} "
                    f"{nbytes/1e6:.1f} MB")
    finally:
        if cerrar and cli is not None:
            sftp.close()
            cli.close()

    return {"total": len(paths), "subidos": up, "omitidos": skip,
            "fallidos": fail, "mb": round(nbytes / 1e6, 1)}


def objetos_del_dia(day: str) -> dict:
    """Mapa {basename: clave_s3} de los MP3 ya aterrizados en Bronce para `day`."""
    fs = _s3fs()
    pref = f"{BRONCE_BUCKET}/audio/date={day}/"
    try:
        keys = fs.ls(pref, detail=False)
    except Exception:
        return {}
    out = {}
    for k in keys:
        base = os.path.basename(k.rstrip("/"))
        if base.lower().endswith((".mp3", ".wav")):
            out[base] = f"{BRONCE_BUCKET}/audio/date={day}/{base}"
    return out


def land_day(day: str, *, sleep_s: float = 0.05, limit: int = 0, log=print) -> dict:
    """Aterriza TODO el audio del alcance para `day` (índice → SFTP → Bronce)."""
    paths = paths_del_dia(day)
    if not paths:
        log(f"[bronze_audio] {day}: el índice no tiene rutas (¿materializar bronze_audio_index?).")
        return {"total": 0, "subidos": 0, "omitidos": 0, "fallidos": 0, "mb": 0.0}
    log(f"[bronze_audio] {day}: {len(paths)} grabaciones en el índice; copiando (SOLO LECTURA)...")
    return land_paths(paths, day, sleep_s=sleep_s, limit=limit, log=log)
