"""Validación del enlace sobre un mes completo + medición de speedup (Fase 2).

Uso (dentro del contenedor Dagster):
  python -m src.processing.validate_month 2025-05           # un núcleo por defecto
  python -m src.processing.validate_month 2025-05 4         # 4 núcleos

Lee el CDR del mes (SOLO LECTURA), lee el índice de audio del alcance desde la zona
Bronce (Parquet), corre el enlace en Spark con N núcleos y reporta cobertura + tiempo.
"""
import os
import sys
import time

from pyspark.sql import functions as F

from .audio_index import build_audio_scope  # noqa: F401 (disponible si no hay bronze)
from .cdr import read_cdr
from .config import AUDIO_INDEX_S3A, DATA_DIR  # noqa: F401 (DATA_DIR usado si se reactiva lo local)
from .linkage import link_calls
from .spark_session import get_spark


def month_range(ym: str):
    y, m = map(int, ym.split("-"))
    d0 = f"{y:04d}-{m:02d}-01 00:00:00"
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    d1 = f"{ny:04d}-{nm:02d}-01 00:00:00"
    return d0, d1


def run(ym: str, cores):
    d0, d1 = month_range(ym)
    print(f"\n=== validate_month mes={ym} cores={cores} ===")
    t = time.time()
    cdr_pdf = read_cdr(d0, d1)
    print(f"[CDR] {len(cdr_pdf):,} filas leídas (solo lectura) en {time.time() - t:.1f}s")

    spark = get_spark("validate_month", cores=str(cores))
    try:
        cdr = spark.createDataFrame(cdr_pdf)
        audio = spark.read.parquet(AUDIO_INDEX_S3A)
        audio = audio.where(F.substring("ts", 1, 6) == ym.replace("-", ""))
        n_audio = audio.count()

        t = time.time()
        matched = link_calls(cdr, audio)
        n_match = matched.count()
        link_time = time.time() - t

        muestra = matched.where(
            (F.col("disposition") == "ANSWERED") & F.col("billsec").between(10, 3600)
        ).count()
        cob = 100 * n_match / n_audio if n_audio else 0
        print(f"[LINK] audio_mes={n_audio:,} emparejadas={n_match:,} "
              f"cobertura={cob:.2f}% en_muestra={muestra:,} | tiempo_link={link_time:.1f}s")
        return {"cores": cores, "audio": n_audio, "match": n_match, "cob": cob, "time": link_time}
    finally:
        spark.stop()


if __name__ == "__main__":
    ym = sys.argv[1] if len(sys.argv) > 1 else "2025-05"
    cores = sys.argv[2] if len(sys.argv) > 2 else "1"
    run(ym, cores)
