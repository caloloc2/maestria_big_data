"""Lectura del CDR de Asterisk (Fase 2) — ESTRICTAMENTE SOLO LECTURA.

Solo consultas SELECT. Nunca se escribe/modifica la tabla ni la fuente.
"""
import pandas as pd
from sqlalchemy import text

from .config import CDR_TABLE, cdr_engine

# Columnas necesarias para el enlace y los KPIs (no se traen las de PII innecesarias).
CDR_COLS = [
    "calldate", "src", "dst", "channel", "dstchannel",
    "duration", "billsec", "disposition", "uniqueid", "linkedid",
]


def read_cdr(desde: str, hasta: str) -> pd.DataFrame:
    """Lee el CDR en el rango [desde, hasta). Descarta corruptos (calldate 0000-00-00).

    Args:
        desde, hasta: 'YYYY-MM-DD HH:MM:SS'.
    """
    q = text(
        f"""
        SELECT {", ".join(CDR_COLS)}
        FROM {CDR_TABLE}
        WHERE calldate >= :d AND calldate < :h
          AND calldate <> '0000-00-00 00:00:00'
        """
    )
    eng = cdr_engine()
    df = pd.read_sql(q, eng, params={"d": desde, "h": hasta})
    eng.dispose()
    # normalización mínima de tipos de texto
    for col in ("src", "dst", "channel", "dstchannel", "disposition", "uniqueid", "linkedid"):
        df[col] = df[col].astype("string")
    return df
