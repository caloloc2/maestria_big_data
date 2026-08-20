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

DDL_TR = [
    "CREATE SCHEMA IF NOT EXISTS servido",
    """
    CREATE TABLE IF NOT EXISTS servido.transcripciones (
        call_id            text,
        fecha              date,
        agente             text,
        dur_audio          real,
        proc_seg           real,
        device             text,
        chunks_descartados integer,
        chars              integer,
        transcript_anon    text,
        run_ts             timestamp DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_tr_fecha ON servido.transcripciones (fecha)",
]

DDL_EV = [
    "CREATE SCHEMA IF NOT EXISTS servido",
    """
    CREATE TABLE IF NOT EXISTS servido.evaluaciones (
        call_id             text,
        fecha               date,
        agente              text,
        rubrica             text,
        es_venta            integer,
        venta_valida        integer,
        infraccion_critica  boolean,
        calidad_score       integer,
        grupo_a             text,
        grupo_b             text,
        grupo_c             text,
        riesgo_reclamo      text,
        sentimiento_asesor  text,
        sentimiento_cliente text,
        confianza_llm       real,
        modelo              text,
        run_ts              timestamp DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_ev_fecha ON servido.evaluaciones (fecha)",
    "CREATE INDEX IF NOT EXISTS ix_ev_agente ON servido.evaluaciones (agente)",
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


def ensure_schema_tr() -> None:
    eng = pg_engine()
    with eng.begin() as con:
        for stmt in DDL_TR:
            con.execute(text(stmt))
    eng.dispose()


def replace_transcripciones(pdf: pd.DataFrame, fecha: str) -> None:
    """Reemplaza (borra + inserta) las transcripciones anonimizadas de `fecha`."""
    ensure_schema_tr()
    eng = pg_engine()
    with eng.begin() as con:
        con.execute(text("DELETE FROM servido.transcripciones WHERE fecha = :f"), {"f": fecha})
    if len(pdf):
        pdf.to_sql("transcripciones", eng, schema="servido", if_exists="append", index=False)
    eng.dispose()


def ensure_schema_ev() -> None:
    eng = pg_engine()
    with eng.begin() as con:
        for stmt in DDL_EV:
            con.execute(text(stmt))
    eng.dispose()


def replace_evaluaciones(pdf: pd.DataFrame, fecha: str) -> None:
    """Reemplaza (borra + inserta) las evaluaciones de calidad de `fecha`."""
    ensure_schema_ev()
    eng = pg_engine()
    with eng.begin() as con:
        con.execute(text("DELETE FROM servido.evaluaciones WHERE fecha = :f"), {"f": fecha})
    if len(pdf):
        pdf.to_sql("evaluaciones", eng, schema="servido", if_exists="append", index=False)
    eng.dispose()
