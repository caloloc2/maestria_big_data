"""Selección de la muestra estratificada para ASR (Fase 3).

Recorre días representativos repartidos en 2020→2025, cruza CDR↔grabación
(reusa el método validado en Fase 1/2), filtra `en_muestra` (ANSWERED,
billsec ∈ [10, 3600]) y toma una muestra estratificada por año.

NO accede al servidor de audio: usa el índice ya materializado
(`bronze_audio_index`) y el CDR (SOLO LECTURA). El tamaño de cada archivo sale
del propio índice (columna `bytes`), así que se estima el peso de la copia sin
tocar el servidor.

Salidas (data/muestra/):
  manifest.tsv  -> metadatos de cada llamada de la muestra
  paths.txt     -> rutas exactas para la copia (una por línea)

Uso (en el contenedor Dagster):
  python -m src.processing.sample_select
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import functions as F

from .cdr import read_cdr
from .config import AUDIO_INDEX_S3A, DATA_DIR
from .linkage import link_calls
from .spark_session import get_spark

# Días representativos (se evita 2018–2019 por el CDR incompleto). 2 por año.
DIAS = [
    "2020-06-10", "2020-10-14",
    "2021-03-10", "2021-09-15",
    "2022-04-13", "2022-11-09",
    "2023-05-10", "2023-10-11",
    "2024-03-13", "2024-09-11",
    "2025-02-12", "2025-05-14",
]
OBJETIVO = 500
SEED = 42


def _next(day: str) -> str:
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def run():
    spark = get_spark("sample_select", cores="4")
    audio_all = spark.read.parquet(AUDIO_INDEX_S3A)
    frames = []
    try:
        for day in DIAS:
            cdr_pdf = read_cdr(f"{day} 00:00:00", f"{_next(day)} 00:00:00")
            if cdr_pdf.empty:
                print(f"[{day}] CDR vacío — se omite")
                continue
            cdr = spark.createDataFrame(cdr_pdf)
            audio = audio_all.where(F.col("fecha") == F.to_date(F.lit(day)))
            linked = link_calls(cdr, audio).where(
                (F.col("disposition") == "ANSWERED") & F.col("billsec").between(10, 3600)
            )
            linked = linked.join(
                audio.select(F.col("path").alias("audio_path"), "bytes"),
                on="audio_path", how="left",
            )
            pdf = linked.toPandas()
            pdf["year"] = day[:4]
            print(f"[{day}] en_muestra={len(pdf):,}")
            frames.append(pdf)
    finally:
        spark.stop()

    full = pd.concat(frames, ignore_index=True)
    n_years = full["year"].nunique()
    por_anio = max(1, OBJETIVO // n_years)
    muestra = full.groupby("year", group_keys=False).apply(
        lambda g: g.sample(min(len(g), por_anio), random_state=SEED)
    )
    if len(muestra) < OBJETIVO:
        resto = full.drop(muestra.index).sample(
            min(OBJETIVO - len(muestra), len(full) - len(muestra)), random_state=SEED
        )
        muestra = pd.concat([muestra, resto])
    muestra = muestra.head(OBJETIVO).reset_index(drop=True)

    out = os.path.join(DATA_DIR, "muestra")
    os.makedirs(out, exist_ok=True)
    cols = ["call_id", "audio_path", "agente", "telefono", "calldate",
            "billsec", "duration", "disposition", "year", "bytes"]
    muestra[cols].to_csv(os.path.join(out, "manifest.tsv"), sep="\t", index=False)
    muestra["audio_path"].to_csv(os.path.join(out, "paths.txt"), index=False, header=False)

    mb = muestra["bytes"].fillna(0).sum() / 1e6
    print("\n=== MUESTRA SELECCIONADA ===")
    print(muestra.groupby("year").size().to_string())
    print(f"\ntotal archivos = {len(muestra)}   tamaño estimado ≈ {mb:.1f} MB")
    print(f"manifest -> {out}/manifest.tsv")
    print(f"paths    -> {out}/paths.txt")


if __name__ == "__main__":
    run()
