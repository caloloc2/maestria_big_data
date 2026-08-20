"""Capa servida (Fase 2): esquema y escritura idempotente a PostgreSQL.

Tabla base `servido.llamadas` (una fila = una grabación emparejada con su CDR).
La escritura por día es idempotente (borra el día y reinserta) para permitir
reprocesos sin duplicar.
"""
import pandas as pd
from sqlalchemy import text

from .config import pg_engine

DDL = [
    "CREATE SCHEMA IF NOT EXISTS servido",
    """
    CREATE TABLE IF NOT EXISTS servido.llamadas (
        call_id       text,
        audio_path    text,
        agente        text,
        telefono      text,
        calldate      timestamp,
        ts_grabacion  timestamp,
        diff_seg      integer,
        billsec       integer,
        duration      integer,
        disposition   text,
        en_muestra    boolean,
        fecha         date
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_llamadas_fecha ON servido.llamadas (fecha)",
    "CREATE INDEX IF NOT EXISTS ix_llamadas_agente ON servido.llamadas (agente)",
]


def ensure_schema() -> None:
    eng = pg_engine()
    with eng.begin() as con:
        for stmt in DDL:
            con.execute(text(stmt))
    eng.dispose()


def replace_day(pdf: pd.DataFrame, fecha: str) -> None:
    """Reemplaza (borra + inserta) las llamadas de `fecha` en servido.llamadas."""
    ensure_schema()
    eng = pg_engine()
    with eng.begin() as con:
        con.execute(text("DELETE FROM servido.llamadas WHERE fecha = :f"), {"f": fecha})
    if len(pdf):
        pdf.to_sql("llamadas", eng, schema="servido", if_exists="append", index=False)
    eng.dispose()
