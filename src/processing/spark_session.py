"""Fábrica de SparkSession local (Fase 2).

Modo local[N] para desarrollo en la laptop. El nº de núcleos y las particiones de
shuffle son parametrizables para medir el speedup (evidencia de escalabilidad).
"""
from pyspark.sql import SparkSession

from .config import s3a_spark_configs


def get_spark(app_name: str = "uisrael", cores: str = "*", shuffle_partitions: int | None = None) -> SparkSession:
    b = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    # Conectividad con el lago MinIO (s3a://) — zonas Bronce/Plata.
    for k, v in s3a_spark_configs().items():
        b = b.config(k, v)
    if shuffle_partitions:
        b = b.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
