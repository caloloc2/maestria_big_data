"""Configuración compartida de la fase batch (Fase 2).

Lee credenciales/rutas del `.env` (montado en el contenedor). Expone motores
SQLAlchemy para el CDR (SOLO LECTURA) y para la capa servida (PostgreSQL).
"""
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Cargar .env (contenedor) y, si se ejecuta en host, el .env local.
for _p in ("/opt/dagster/app/.env", ".env"):
    load_dotenv(_p)

DATA_DIR = os.getenv("DATA_DIR", "/opt/dagster/app/data")
# Ruta del índice SIEMPRE relativa a DATA_DIR (no usar la var AUDIO_INDEX del .env,
# que apunta a /work/... del contenedor de diagnóstico de la Fase 1).
AUDIO_INDEX = os.path.join(DATA_DIR, "diag", "audio_index.tsv.gz")
AUDIO_ROOT = os.getenv("AUDIO_ROOT", "/home/grabacion/monitor/111111111111").rstrip("/") + "/"
CDR_TABLE = os.getenv("CDR_TABLE", "cdr")

# ── MinIO / lago S3 (zonas Bronce/Plata en Parquet) ─────────────────────────
# La capa servida sigue en PostgreSQL; MinIO solo reemplaza el filesystem de Parquet.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")  # con esquema (s3fs)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONCE_BUCKET = os.getenv("BRONCE_BUCKET", "bronce")
PLATA_BUCKET = os.getenv("PLATA_BUCKET", "plata")
# Índice de audio (Bronce) en el lago — lo escribe el activo `bronze_audio_index`.
AUDIO_INDEX_S3A = f"s3a://{BRONCE_BUCKET}/audio_index"


def s3a_spark_configs() -> dict:
    """Configuración S3A para que Spark lea/escriba en MinIO vía `s3a://`."""
    endpoint = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")  # host:puerto
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",       # requerido por MinIO
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",  # MinIO en HTTP (LAN)
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }


def s3_storage_options() -> dict:
    """`storage_options` para pandas/s3fs (escritura de Parquet a MinIO)."""
    return {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    }


def cdr_engine():
    """Motor de SOLO LECTURA al CDR de Asterisk (latin1 → UTF-8)."""
    u = os.environ["CDR_USER"]
    pw = os.environ["CDR_PASSWORD"]
    h = os.environ["CDR_HOST"]
    pt = os.getenv("CDR_PORT", "3306")
    db = os.environ["CDR_DB"]
    ch = os.getenv("CDR_CHARSET", "latin1")
    return create_engine(f"mysql+pymysql://{u}:{pw}@{h}:{pt}/{db}?charset={ch}", pool_pre_ping=True)


def cdr_jdbc() -> tuple[str, dict]:
    """URL y propiedades JDBC (MariaDB) para la lectura particionada del CDR con Spark.

    ESTRICTAMENTE SOLO LECTURA: se usa el usuario `lectura` (privilegios de solo lectura
    en la fuente) y Spark solo emite consultas SELECT. Nunca se escribe en Asterisk.
    """
    u = os.environ["CDR_USER"]
    pw = os.environ["CDR_PASSWORD"]
    h = os.environ["CDR_HOST"]
    pt = os.getenv("CDR_PORT", "3306")
    db = os.environ["CDR_DB"]
    # Esquema `jdbc:mysql://` a propósito (no `jdbc:mariadb://`): así Spark selecciona el
    # dialecto MySQL y entrecomilla los identificadores con backticks (`calldate`), no con
    # comillas dobles (que MariaDB tomaría como literal de texto y rompería el particionado).
    # El driver sigue siendo el de MariaDB 2.7.x (compatible con MariaDB 5.5.60), que acepta
    # el esquema mysql con `permitMysqlScheme`.
    url = f"jdbc:mysql://{h}:{pt}/{db}?permitMysqlScheme"
    props = {
        "user": u,
        "password": pw,
        "driver": "org.mariadb.jdbc.Driver",
    }
    return url, props


def pg_engine():
    """Motor a la capa servida (PostgreSQL del stack)."""
    h = os.getenv("PG_HOST", "postgres")
    pt = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "dagster")
    u = os.getenv("PG_USER", "dagster")
    pw = os.getenv("PG_PASSWORD", "dagster")
    return create_engine(f"postgresql+psycopg2://{u}:{pw}@{h}:{pt}/{db}")
