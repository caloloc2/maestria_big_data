"""Métricas de carga (streaming) y estimación de reproceso total (Fase 3).

Con datos que YA tenemos (CDR + índice de audio + RTF medido en el benchmark),
estima sin tocar el servidor de audio:
  (A) volumen y tasa de llamadas del alcance, y cuántas son LARGAS → carga de streaming;
  (B) tiempo estimado para transcribir TODO el corpus del alcance (reproceso completo).

Calibración bytes→segundos: se mide en la muestra local (duración real vs bytes).
Distribución/tasas: mes representativo mayo 2025 (enlace validado al 100 %).
Totales del corpus: bronze_audio_index (conteo + suma de bytes del alcance).

Uso (contenedor Dagster):
  python -m src.processing.carga_streaming
"""
import os

import pandas as pd
from pyspark.sql import functions as F

from .cdr import read_cdr
from .config import DATA_DIR
from .linkage import link_calls
from .spark_session import get_spark

RTF_GPU, RTF_CPU = 0.055, 0.096            # factores tiempo-real medidos (llamadas largas)
MES = ("2025-05-01 00:00:00", "2025-06-01 00:00:00")
L_LARGA = 600                               # "larga" = >= 10 min de conversación
L_MEDIA = 300                               # >= 5 min


def calibrar_bps(max_files: int = 150):
    """Bytes por segundo reales. Si BPS viene por entorno (host lo calibró con
    librosa), se usa ese; si no, se intenta medir con librosa localmente."""
    if os.getenv("BPS"):
        return float(os.environ["BPS"]), float(os.getenv("DUR_MEDIA", "0"))
    import librosa  # solo si hace falta (no está en el contenedor)

    man = pd.read_csv(os.path.join(DATA_DIR, "muestra", "manifest.tsv"), sep="\t")
    seg = byt = 0.0
    n = 0
    for _, r in man.iterrows():
        fn = os.path.join(DATA_DIR, "muestra", "audios", os.path.basename(r["audio_path"]))
        if os.path.exists(fn):
            try:
                seg += librosa.get_duration(path=fn)
                byt += float(r["bytes"])
                n += 1
            except Exception:  # noqa: BLE001
                pass
        if n >= max_files:
            break
    return byt / seg, seg / n


def hms(seg: float) -> str:
    h = int(seg // 3600)
    m = int((seg % 3600) // 60)
    return f"{h} h {m} min"


def main():
    bps, dur_media_muestra = calibrar_bps()
    spark = get_spark("carga_streaming", cores="4")
    idx = spark.read.parquet(os.path.join(DATA_DIR, "bronze", "audio_index"))
    tot_files = idx.count()
    tot_bytes = int(idx.agg(F.sum("bytes")).first()[0])

    cdr = spark.createDataFrame(read_cdr(*MES))
    audio_m = idx.where(F.substring("ts", 1, 6) == "202505")
    m = link_calls(cdr, audio_m).join(
        audio_m.select(F.col("path").alias("audio_path"), "bytes"), "audio_path", "left"
    )
    m = m.withColumn("hora", F.hour("ts_grabacion")).withColumn("dia", F.to_date("ts_grabacion"))
    pdf = m.select("dia", "hora", "billsec", "duration", "bytes").toPandas()
    spark.stop()

    # ---------- (A) Volumen y carga de streaming (mayo 2025) ----------
    n_mes = len(pdf)
    dias = pdf["dia"].nunique()
    por_dia = n_mes / dias
    # tasa por hora: contar por (día, hora) y ver media/pico de horas activas
    por_hora = pdf.groupby(["dia", "hora"]).size()
    larga = pdf[pdf["billsec"] >= L_LARGA]
    media = pdf[pdf["billsec"] >= L_MEDIA]
    larga_por_hora = larga.groupby(["dia", "hora"]).size()

    dur_larga_media = larga["billsec"].mean() if len(larga) else 0
    cap_gpu_h = 3600 / (dur_larga_media * RTF_GPU) if dur_larga_media else 0
    cap_cpu_h = 3600 / (dur_larga_media * RTF_CPU) if dur_larga_media else 0

    print("\n===== (A) CARGA DE STREAMING — mayo 2025 =====")
    print(f"calibración: {bps:.0f} B/s  (dur media muestra {dur_media_muestra:.0f} s)")
    print(f"grabaciones del mes (emparejadas) : {n_mes:,} en {dias} días")
    print(f"promedio por día                  : {por_dia:,.0f}")
    print(f"por hora activa  media / pico     : {por_hora.mean():.0f} / {por_hora.max()}")
    print(f"cortas <5min : {n_mes-len(media):,} ({100*(n_mes-len(media))/n_mes:.1f}%)")
    print(f"medias 5-10min: {len(media)-len(larga):,} ({100*(len(media)-len(larga))/n_mes:.1f}%)")
    print(f"LARGAS >=10min: {len(larga):,} ({100*len(larga)/n_mes:.1f}%)  dur media {dur_larga_media/60:.1f} min")
    print(f"LARGAS por día  media            : {len(larga)/dias:.0f}")
    print(f"LARGAS por hora activa  media/pico: {larga_por_hora.mean():.1f} / {larga_por_hora.max()}")
    print(f"capacidad 1 nodo (largas/hora)   : GPU ~{cap_gpu_h:.0f}  |  CPU ~{cap_cpu_h:.0f}")
    print(f"  -> holgura vs pico de largas    : GPU x{cap_gpu_h/max(1,larga_por_hora.max()):.0f}  CPU x{cap_cpu_h/max(1,larga_por_hora.max()):.0f}")

    # ---------- (B) Reproceso total del corpus ----------
    corpus_seg = tot_bytes / bps
    mes_seg = float(pdf["bytes"].fillna(0).sum()) / bps
    print("\n===== (B) REPROCESO TOTAL DEL CORPUS (alcance 200-299/OUT) =====")
    print(f"grabaciones totales : {tot_files:,}")
    print(f"tamaño total        : {tot_bytes/1e9:.1f} GB")
    print(f"audio total estimado: {corpus_seg/3600:,.0f} horas ({corpus_seg/3600/24:,.0f} días de audio)")
    print(f"proceso 1 nodo GPU  : {hms(corpus_seg*RTF_GPU)}  ({corpus_seg*RTF_GPU/3600/24:.0f} días continuos)")
    print(f"proceso 1 nodo CPU  : {hms(corpus_seg*RTF_CPU)}  ({corpus_seg*RTF_CPU/3600/24:.0f} días continuos)")
    print(f"[ref] un mes (mayo) : audio {mes_seg/3600:.0f} h  -> GPU {hms(mes_seg*RTF_GPU)} | CPU {hms(mes_seg*RTF_CPU)}")


if __name__ == "__main__":
    main()
