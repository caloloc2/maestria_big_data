"""Enlace CDR ↔ grabación a escala (Fase 2), con Spark.

Reutiliza el método validado en la Fase 1 (record linkage, ref. Christen 2012):
llave compuesta con tolerancia temporal —
  audio(ext, teléfono, ts) ↔ CDR(ext∈channel/dstchannel, dst=teléfono, |Δt|≤TOL),
seleccionando el vecino más cercano en tiempo por grabación.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Extensión 2xx del agente, aislada (no precedida/seguida por otro dígito).
PAT_EXT = r"(?<![0-9])(2[0-9][0-9])(?![0-9])"
TOL_SEG = 180


def link_calls(cdr_sdf: DataFrame, audio_sdf: DataFrame, tol: int = TOL_SEG) -> DataFrame:
    # Extensión del agente desde channel / dstchannel (el src es el troncal, no el agente).
    c = (
        cdr_sdf.withColumn("ext_ch", F.regexp_extract("channel", PAT_EXT, 1))
        .withColumn("ext_dch", F.regexp_extract("dstchannel", PAT_EXT, 1))
    )
    c = c.withColumn(
        "agente",
        F.when(F.col("ext_ch") != "", F.col("ext_ch")).otherwise(F.col("ext_dch")),
    ).where(F.col("agente") != "")
    c = c.select(
        F.col("uniqueid").alias("call_id"),
        F.col("agente"),
        F.col("dst").alias("cdr_tel"),
        F.to_timestamp("calldate").alias("calldate"),
        F.col("billsec").cast("int").alias("billsec"),
        F.col("duration").cast("int").alias("duration"),
        F.col("disposition").alias("disposition"),
    )
    a = audio_sdf.select(
        F.col("path").alias("audio_path"),
        F.col("ext").alias("agente_a"),
        F.col("phone").alias("telefono"),
        F.col("ts_ts").alias("ts_grabacion"),
    )
    j = a.join(c, (a["agente_a"] == c["agente"]) & (a["telefono"] == c["cdr_tel"]), "inner")
    j = j.withColumn(
        "diff_seg",
        F.abs(F.col("ts_grabacion").cast("long") - F.col("calldate").cast("long")),
    ).where(F.col("diff_seg") <= tol)
    # vecino más cercano en tiempo por grabación (1:1)
    w = Window.partitionBy("audio_path").orderBy(F.col("diff_seg").asc())
    j = j.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1)
    return j.select(
        "call_id", "audio_path", "agente", "telefono",
        "calldate", "ts_grabacion", "diff_seg", "billsec", "duration", "disposition",
    )
