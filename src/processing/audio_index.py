"""Índice de grabaciones → alcance del proyecto (Fase 2), con Spark.

Lee el índice `audio_index.tsv.gz` (ya descargado en la Fase 1; NO se accede al
servidor), filtra el alcance **extensiones 200–299 / OUT** y parsea
(extensión_agente, teléfono, marca temporal) del nombre `{ts}-{ext}-{teléfono}`.
"""
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import AUDIO_INDEX, AUDIO_ROOT


def build_audio_scope(spark: SparkSession) -> DataFrame:
    raw = (
        spark.read.option("sep", "\t").csv(AUDIO_INDEX).toDF("path", "bytes", "mtime")
    )
    root_re = "^" + re.escape(AUDIO_ROOT)
    df = raw.withColumn("rel", F.regexp_replace("path", root_re, ""))
    df = df.withColumn("segs", F.split("rel", "/"))
    df = (
        df.withColumn("L1", F.col("segs").getItem(0))
        .withColumn("seg2", F.col("segs").getItem(1))
        .withColumn("fname", F.element_at("segs", -1))
    )
    # Alcance: carpeta L1 = extensión 200–299 y subcarpeta OUT (salientes)
    df = df.where(F.col("L1").rlike("^2[0-9][0-9]$") & (F.col("seg2") == "OUT"))
    df = df.withColumn("base", F.regexp_replace("fname", r"\.(?i)(mp3|wav)$", ""))
    df = df.where(F.col("base").rlike("^[0-9]{14}"))
    df = (
        df.withColumn("ts", F.substring("base", 1, 14))
        .withColumn("ext", F.col("L1"))
        .withColumn("flds", F.split("base", "-"))
    )
    # teléfono = todo lo que sigue al 2º guion (por si el número trae guiones)
    df = df.withColumn(
        "phone", F.expr("array_join(slice(flds, 3, size(flds) - 2), '-')")
    )
    df = (
        df.withColumn("ts_ts", F.to_timestamp("ts", "yyyyMMddHHmmss"))
        .withColumn("fecha", F.to_date(F.col("ts_ts")))
        .withColumn("bytes", F.col("bytes").cast("long"))
    )
    return df.select("path", "ext", "phone", "ts", "ts_ts", "fecha", "bytes").where(
        F.col("phone") != ""
    )
