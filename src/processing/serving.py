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
    # Diarización diferida (Fase 5): las llamadas largas se transcriben ya, pero se marcan
    # para diarizarlas después con GPU (pyannote en CPU es el cuello de botella).
    "ALTER TABLE servido.transcripciones ADD COLUMN IF NOT EXISTS requiere_diarizacion boolean DEFAULT false",
    "ALTER TABLE servido.transcripciones ADD COLUMN IF NOT EXISTS diarizado boolean DEFAULT false",
    # Nº de hablantes detectados por pyannote cuando la diarización diferida se ejecuta.
    "ALTER TABLE servido.transcripciones ADD COLUMN IF NOT EXISTS n_hablantes integer",
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
    # Gemini con contexto: señal fiable de B17 (hacerse pasar por el banco).
    "ALTER TABLE servido.evaluaciones ADD COLUMN IF NOT EXISTS impersona_banco integer DEFAULT 0",
]


# Streaming (Fase 5): columna de origen (batch/streaming) + cursor de avance del poll.
# Todo vive en NUESTRO PostgreSQL; jamás se escribe en Asterisk.
DDL_STREAM = [
    "ALTER TABLE servido.llamadas ADD COLUMN IF NOT EXISTS origen text DEFAULT 'batch'",
    """
    CREATE TABLE IF NOT EXISTS servido.stream_cursor (
        nombre text PRIMARY KEY,
        valor  text,
        run_ts timestamp DEFAULT now()
    )
    """,
]


def ensure_schema() -> None:
    eng = pg_engine()
    with eng.begin() as con:
        for stmt in DDL:
            con.execute(text(stmt))
        for stmt in DDL_STREAM:
            con.execute(text(stmt))
    eng.dispose()


def get_cursor(nombre: str) -> str | None:
    ensure_schema()
    eng = pg_engine()
    with eng.connect() as con:
        r = con.execute(
            text("SELECT valor FROM servido.stream_cursor WHERE nombre = :n"), {"n": nombre}
        ).fetchone()
    eng.dispose()
    return r[0] if r else None


def set_cursor(nombre: str, valor: str) -> None:
    ensure_schema()
    eng = pg_engine()
    with eng.begin() as con:
        con.execute(text(
            "INSERT INTO servido.stream_cursor (nombre, valor, run_ts) "
            "VALUES (:n, :v, now()) "
            "ON CONFLICT (nombre) DO UPDATE SET valor = EXCLUDED.valor, run_ts = now()"
        ), {"n": nombre, "v": valor})
    eng.dispose()


def _upsert(tabla: str, row: dict, cols_tr: dict | None = None) -> None:
    """Upsert idempotente de una fila por `call_id` (borra + inserta)."""
    ensure_schema_tr() if tabla == "transcripciones" else None
    ensure_schema_ev() if tabla == "evaluaciones" else None
    if tabla == "llamadas":
        ensure_schema()
    eng = pg_engine()
    with eng.begin() as con:
        con.execute(text(f"DELETE FROM servido.{tabla} WHERE call_id = :c"),
                    {"c": row["call_id"]})
    pd.DataFrame([row]).to_sql(tabla, eng, schema="servido", if_exists="append", index=False)
    eng.dispose()


def upsert_llamada(row: dict) -> None:
    _upsert("llamadas", row)


def upsert_transcripcion(row: dict) -> None:
    _upsert("transcripciones", row)


def upsert_evaluacion(row: dict) -> None:
    _upsert("evaluaciones", row)


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


def pendientes_diarizacion(dia: str | None = None) -> list[dict]:
    """Llamadas transcritas marcadas para diarización DIFERIDA (Fase 5).

    Une el marcador de `servido.transcripciones` con `servido.llamadas` para obtener
    la clave del MP3 crudo en Bronce/MinIO (necesaria para re-procesar). Si `dia` es
    None, devuelve TODO el backlog pendiente; si se indica, solo ese día.
    """
    ensure_schema_tr()
    q = (
        "SELECT DISTINCT t.call_id, t.fecha, t.agente, l.audio_path "
        "FROM servido.transcripciones t "
        "JOIN servido.llamadas l ON l.call_id = t.call_id "
        "WHERE t.requiere_diarizacion AND NOT t.diarizado"
    )
    params: dict = {}
    if dia is not None:
        q += " AND t.fecha = :f"
        params["f"] = dia
    q += " ORDER BY t.fecha, t.call_id"
    eng = pg_engine()
    with eng.connect() as con:
        rows = [dict(r._mapping) for r in con.execute(text(q), params)]
    eng.dispose()
    return rows


def set_diarizado(call_id: str, transcript_anon: str, n_hablantes: int | None) -> None:
    """Marca una transcripción como diarizada y reemplaza su texto por la versión con
    turnos ASESOR/CLIENTE. UPDATE puntual (conserva el resto de la fila)."""
    ensure_schema_tr()
    eng = pg_engine()
    with eng.begin() as con:
        con.execute(text(
            "UPDATE servido.transcripciones "
            "SET transcript_anon = :t, n_hablantes = :n, "
            "    diarizado = true, requiere_diarizacion = false "
            "WHERE call_id = :c"
        ), {"t": transcript_anon, "n": n_hablantes, "c": call_id})
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


# ───────────────────────── KPIs (Fase 6) ─────────────────────────
DDL_KPI = [
    "CREATE SCHEMA IF NOT EXISTS servido",
    """
    CREATE TABLE IF NOT EXISTS servido.kpis (
        fecha            date PRIMARY KEY,
        n_llamadas       integer,
        n_contestadas    integer,
        contactabilidad  real,
        dur_media        real,
        n_largas         integer,
        n_evaluadas      integer,
        n_ventas         integer,
        n_ventas_validas integer,
        n_ventas_riesgo  integer,
        calidad_media    real,
        tasa_conversion  real
    )
    """,
]

_KPI_SQL = """
INSERT INTO servido.kpis (fecha,n_llamadas,n_contestadas,contactabilidad,dur_media,n_largas,
  n_evaluadas,n_ventas,n_ventas_validas,n_ventas_riesgo,calidad_media,tasa_conversion)
WITH ll AS (
  SELECT fecha,
    count(*) n_llamadas,
    count(*) FILTER (WHERE disposition='ANSWERED') n_contestadas,
    round(avg(billsec)::numeric,1) dur_media,
    count(*) FILTER (WHERE en_muestra AND billsec>=600) n_largas
  FROM servido.llamadas GROUP BY fecha
),
ev AS (
  SELECT fecha,
    count(*) n_evaluadas,
    count(*) FILTER (WHERE es_venta=1) n_ventas,
    count(*) FILTER (WHERE venta_valida_v2=1) n_ventas_validas,
    count(*) FILTER (WHERE venta_con_riesgo=1) n_ventas_riesgo,
    round(avg(calidad_score_v2)::numeric,1) calidad_media
  FROM servido.evaluaciones GROUP BY fecha
)
SELECT ll.fecha, ll.n_llamadas, ll.n_contestadas,
  CASE WHEN ll.n_llamadas>0 THEN round(100.0*ll.n_contestadas/ll.n_llamadas,1) ELSE 0 END,
  ll.dur_media, ll.n_largas,
  COALESCE(ev.n_evaluadas,0), COALESCE(ev.n_ventas,0), COALESCE(ev.n_ventas_validas,0),
  COALESCE(ev.n_ventas_riesgo,0), ev.calidad_media,
  CASE WHEN ll.n_contestadas>0 THEN round(100.0*COALESCE(ev.n_ventas,0)/ll.n_contestadas,2) ELSE 0 END
FROM ll LEFT JOIN ev ON ev.fecha=ll.fecha
"""


def build_kpis() -> dict:
    """Reconstruye servido.kpis (serie diaria: operativos + ventas/calidad v2)."""
    eng = pg_engine()
    with eng.begin() as con:
        for stmt in DDL_KPI:
            con.execute(text(stmt))
        con.execute(text("DELETE FROM servido.kpis"))
        con.execute(text(_KPI_SQL))
        r = list(con.execute(text(
            "SELECT count(*), COALESCE(sum(n_ventas),0), COALESCE(sum(n_ventas_validas),0), "
            "COALESCE(sum(n_ventas_riesgo),0) FROM servido.kpis")))[0]
    eng.dispose()
    return {"dias": int(r[0]), "ventas_total": int(r[1]),
            "ventas_validas_total": int(r[2]), "ventas_riesgo_total": int(r[3])}
