"""Resolución del origen del audio para el worker (Fase 3/5).

El worker corre NATIVO en el host y se desacopla por Kafka; los trabajos traen un
`audio_path` que puede ser:
  - una clave del lago MinIO  (`bronce/audio/date=YYYY-MM-DD/xxx.mp3`  o  `s3://bronce/...`)
  - una ruta local ya existente (`data/muestra/audios/xxx.mp3`) — compatibilidad hacia atrás.

Si es del lago, se descarga UNA vez a un caché local (`data/cache_audio/`) y se
reutiliza. Así el worker consume el MP3 crudo que aterrizó `bronze_audio`, sin copia
manual de muestras. Solo LECTURA del objeto en MinIO (nuestro lago, no Asterisk).
"""
import os

from dotenv import load_dotenv

load_dotenv(".env")

# El worker está en el host → MinIO se alcanza por el puerto publicado (localhost:9000).
MINIO_HOST = os.getenv("MINIO_ENDPOINT_HOST", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "0") == "1"
CACHE_DIR = os.path.join("data", "cache_audio")

_client = None


def _minio():
    global _client
    if _client is None:
        from minio import Minio

        _client = Minio(MINIO_HOST, access_key=MINIO_ACCESS,
                        secret_key=MINIO_SECRET, secure=MINIO_SECURE)
    return _client


def es_clave_s3(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://") or path.startswith("bronce/")


def resolver(audio_path: str) -> str:
    """Devuelve una ruta LOCAL al audio. Descarga de MinIO si hace falta.

    Compatibilidad: si `audio_path` ya existe como archivo local, se usa tal cual.
    """
    if os.path.exists(audio_path):
        return audio_path
    if not es_clave_s3(audio_path):
        # ruta local relativa que no existe → se deja que el llamador falle claro
        return audio_path

    # normalizar a (bucket, key)
    p = audio_path.replace("s3a://", "").replace("s3://", "")
    bucket, _, key = p.partition("/")
    base = os.path.basename(key)
    os.makedirs(CACHE_DIR, exist_ok=True)
    dst = os.path.join(CACHE_DIR, base)
    if os.path.exists(dst) and os.path.getsize(dst) > 0:
        return dst
    _minio().fget_object(bucket, key, dst)   # descarga (solo lectura del lago)
    return dst
