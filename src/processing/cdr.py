"""Lectura del CDR de Asterisk (Fase 2) — ESTRICTAMENTE SOLO LECTURA.

Solo consultas SELECT. Nunca se escribe/modifica la tabla ni la fuente.

Dos caminos de lectura:
- `read_cdr` (pandas): para conjuntos chicos/analíticos (muestreo, validación mensual).
- `read_cdr_spark` (Spark JDBC particionado): para el volumen del activo `bronze_cdr`
  (observación del tutor: usar Spark donde hay volumen). Reparte el rango del día en
  N subconsultas SELECT paralelas por `calldate` → lecturas concurrentes escalables.
"""
import pandas as pd
from sqlalchemy import text

from .config import CDR_TABLE, cdr_engine, cdr_jdbc

# Columnas necesarias para el enlace y los KPIs (no se traen las de PII innecesarias).
CDR_COLS = [
    "calldate", "src", "dst", "channel", "dstchannel",
    "duration", "billsec", "disposition", "uniqueid", "linkedid",
]


def read_cdr_spark(spark, desde: str, hasta: str, num_partitions: int = 4):
    """Lee el CDR del rango [desde, hasta) con Spark JDBC en `num_partitions` lecturas
    paralelas (SOLO LECTURA vía subconsulta SELECT). Devuelve un Spark DataFrame.

    Args:
        spark: SparkSession activa (con el driver mariadb-java-client en el classpath).
        desde, hasta: 'YYYY-MM-DD HH:MM:SS'.
        num_partitions: nº de lecturas paralelas (≈ nº de núcleos).
    """
    url, props = cdr_jdbc()
    cols = ", ".join(CDR_COLS)
    # Subconsulta de SOLO LECTURA: acota el día y descarta corruptos en la fuente.
    subq = (
        f"(SELECT {cols} FROM {CDR_TABLE} "
        f"WHERE calldate >= '{desde}' AND calldate < '{hasta}' "
        f"AND calldate <> '0000-00-00 00:00:00') t"
    )
    reader = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", subq)
        .option("driver", props["driver"])
        .option("user", props["user"])
        .option("password", props["password"])
        # Lecturas particionadas por marca temporal → N SELECT concurrentes.
        .option("partitionColumn", "calldate")
        .option("lowerBound", desde)
        .option("upperBound", hasta)
        .option("numPartitions", str(num_partitions))
        .option("fetchsize", "2000")
    )
    return reader.load()


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
