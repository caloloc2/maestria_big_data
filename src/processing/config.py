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


def cdr_engine():
    """Motor de SOLO LECTURA al CDR de Asterisk (latin1 → UTF-8)."""
    u = os.environ["CDR_USER"]
    pw = os.environ["CDR_PASSWORD"]
    h = os.environ["CDR_HOST"]
    pt = os.getenv("CDR_PORT", "3306")
    db = os.environ["CDR_DB"]
    ch = os.getenv("CDR_CHARSET", "latin1")
    return create_engine(f"mysql+pymysql://{u}:{pw}@{h}:{pt}/{db}?charset={ch}", pool_pre_ping=True)


def pg_engine():
    """Motor a la capa servida (PostgreSQL del stack)."""
    h = os.getenv("PG_HOST", "postgres")
    pt = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "dagster")
    u = os.getenv("PG_USER", "dagster")
    pw = os.getenv("PG_PASSWORD", "dagster")
    return create_engine(f"postgresql+psycopg2://{u}:{pw}@{h}:{pt}/{db}")
