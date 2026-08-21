"""Definiciones de Dagster — activos Medallion.

Fase 2 (batch, real): bronze_cdr, bronze_audio_index, silver_calls.
Fases 3/4/6 (esqueleto): silver_transcriptions, gold_evaluations, gold_kpis.

Los activos batch son **particionados por día** (`DailyPartitionsDefinition`) para
habilitar backfill del histórico y linaje nativo. bronze_cdr lee el CDR en SOLO
LECTURA; silver_calls cruza CDR↔grabación (llave validada en Fase 1) y escribe la
zona Plata (Parquet) + la capa servida `servido.llamadas` (PostgreSQL).
"""
import os
from datetime import datetime, timedelta

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pyspark.sql import functions as F

from src.processing.audio_index import build_audio_scope
from src.processing.cdr import read_cdr
from src.processing.config import (
    BRONCE_BUCKET,
    DATA_DIR,
    PLATA_BUCKET,
    s3_storage_options,
)
from src.processing.linkage import link_calls
from src.processing.serving import replace_day
from src.processing.spark_session import get_spark

BRONCE, PLATA, ORO = "bronce", "plata", "oro"

# Alcance de desarrollo/validación: mayo 2025 (mes validado al 100 % en Fase 1).
daily = DailyPartitionsDefinition(start_date="2025-05-01", end_date="2025-06-01")


def _paths(day: str):
    # Zonas Bronce/Plata en el lago MinIO. pandas/s3fs escribe con esquema `s3://`;
    # Spark lee/escribe el mismo objeto vía `s3a://`.
    return {
        "cdr_write": f"s3://{BRONCE_BUCKET}/cdr/date={day}/part.parquet",
        "cdr": f"s3a://{BRONCE_BUCKET}/cdr/date={day}/part.parquet",
        "audio": f"s3a://{BRONCE_BUCKET}/audio_index",
        "silver": f"s3a://{PLATA_BUCKET}/calls/date={day}",
    }


# ───────────────────────────── Zona Bronce ─────────────────────────────
@asset(partitions_def=daily, group_name=BRONCE,
       description="CDR crudo del día desde MySQL (SOLO LECTURA) → Parquet.")
def bronze_cdr(context: AssetExecutionContext) -> MaterializeResult:
    day = context.partition_key
    d1 = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    df = read_cdr(f"{day} 00:00:00", f"{d1} 00:00:00")
    p = _paths(day)
    # Spark 3.5 no lee timestamps en nanosegundos → escribir en microsegundos.
    # Escritura directa al lago MinIO vía s3fs (storage_options).
    df.to_parquet(
        p["cdr_write"], index=False, coerce_timestamps="us",
        allow_truncated_timestamps=True, storage_options=s3_storage_options(),
    )
    context.log.info(f"bronze_cdr {day}: {len(df):,} filas → {p['cdr']}")
    return MaterializeResult(metadata={"filas_cdr": len(df), "ruta": MetadataValue.path(p["cdr"])})


@asset(group_name=BRONCE,
       description="Índice de grabaciones (alcance 200–299/OUT) → Parquet particionado por fecha.")
def bronze_audio_index(context: AssetExecutionContext) -> MaterializeResult:
    spark = get_spark("bronze_audio_index")
    try:
        scope = build_audio_scope(spark)
        out = _paths("")["audio"]
        scope.write.mode("overwrite").partitionBy("fecha").parquet(out)
        n = spark.read.parquet(out).count()
    finally:
        spark.stop()
    context.log.info(f"bronze_audio_index: {n:,} grabaciones en alcance → {out}")
    return MaterializeResult(metadata={"grabaciones_scope": n, "ruta": MetadataValue.path(out)})


# ───────────────────────────── Zona Plata ─────────────────────────────
@asset(partitions_def=daily, group_name=PLATA, deps=[bronze_cdr, bronze_audio_index],
       description="Cruce CDR↔grabación (ext+teléfono+ventana) + muestra → Parquet + servido.llamadas.")
def silver_calls(context: AssetExecutionContext) -> MaterializeResult:
    day = context.partition_key
    p = _paths(day)
    spark = get_spark("silver_calls")
    try:
        cdr = spark.read.parquet(p["cdr"])
        audio = spark.read.parquet(p["audio"]).where(F.col("fecha") == F.to_date(F.lit(day)))
        n_audio = audio.count()

        res = link_calls(cdr, audio)
        res = res.withColumn(
            "en_muestra",
            (F.col("disposition") == "ANSWERED") & F.col("billsec").between(10, 3600),
        ).withColumn("fecha", F.lit(day)).cache()

        res.write.mode("overwrite").parquet(p["silver"])
        n_match = res.count()
        pdf = res.toPandas()
    finally:
        spark.stop()

    replace_day(pdf, day)
    cobertura = round(100 * n_match / n_audio, 2) if n_audio else 0.0
    en_muestra = int(pdf["en_muestra"].sum()) if len(pdf) else 0
    context.log.info(
        f"silver_calls {day}: audio={n_audio:,} emparejadas={n_match:,} "
        f"cobertura={cobertura}% en_muestra={en_muestra:,}"
    )
    return MaterializeResult(metadata={
        "grabaciones_dia": n_audio,
        "emparejadas": n_match,
        "cobertura_pct": cobertura,
        "en_muestra": en_muestra,
        "huerfanas": n_audio - n_match,
    })


# ─────────────── Zonas Plata/Oro — esqueleto (Fases 3, 4, 6) ───────────────
@asset(partitions_def=daily, group_name=PLATA, deps=[silver_calls],
       description="Transcripciones anonimizadas: encola asr.jobs → worker Whisper → asr.results.")
def silver_transcriptions(context: AssetExecutionContext) -> MaterializeResult:
    import json
    import time

    import pandas as pd
    from confluent_kafka import Consumer, Producer
    from sqlalchemy import text as sqltext

    from src.processing.config import pg_engine
    from src.processing.serving import replace_transcripciones

    day = context.partition_key
    limit = int(os.getenv("ASR_LIMIT", "60"))
    timeout = int(os.getenv("ASR_TIMEOUT", "1800"))
    boot = os.getenv("KAFKA_BOOTSTRAP_ASSET", "kafka:9092")
    audios_dir = os.path.join(DATA_DIR, "muestra", "audios")

    # 1) muestra del día con audio disponible localmente
    eng = pg_engine()
    df = pd.read_sql(
        sqltext("SELECT call_id, audio_path, agente FROM servido.llamadas "
                "WHERE fecha = :f AND en_muestra"),
        eng, params={"f": day},
    )
    eng.dispose()
    if not len(df):
        context.log.warning(f"silver_transcriptions {day}: sin llamadas en muestra.")
        return MaterializeResult(metadata={"transcritas": 0, "nota": "sin muestra"})
    df["base"] = df["audio_path"].map(os.path.basename)
    df["ok"] = df["base"].map(lambda b: os.path.exists(os.path.join(audios_dir, b)))
    df = df[df["ok"]].head(limit)
    if not len(df):
        context.log.warning(f"silver_transcriptions {day}: sin audios locales (copiar muestra).")
        return MaterializeResult(metadata={"transcritas": 0, "nota": "sin audios locales"})

    # 2) encolar trabajos (ruta host-relativa que lee el worker nativo)
    agente_de = dict(zip(df["call_id"], df["agente"]))
    prod = Producer({"bootstrap.servers": boot})
    ids = set()
    for _, r in df.iterrows():
        prod.produce("asr.jobs", json.dumps(
            {"call_id": r["call_id"], "audio_path": f"data/muestra/audios/{r['base']}"}
        ).encode("utf-8"))
        ids.add(r["call_id"])
    prod.flush(15)
    context.log.info(f"silver_transcriptions {day}: {len(ids)} jobs encolados; esperando worker...")

    # 3) recoger resultados (dedupe por call_id; grupo único → re-lee desde el inicio)
    cons = Consumer({
        "bootstrap.servers": boot,
        "group.id": f"asset-tr-{day}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    cons.subscribe(["asr.results"])
    got = {}
    t0 = time.time()
    while len(got) < len(ids) and time.time() - t0 < timeout:
        m = cons.poll(2.0)
        if m is None or m.error():
            continue
        d = json.loads(m.value())
        if d.get("call_id") in ids:
            got[d["call_id"]] = d
    cons.close()

    # 4) guardar transcripciones anonimizadas (solo estado ok)
    rows = []
    for cid in ids:
        d = got.get(cid)
        if d and d.get("estado") == "ok":
            me = d.get("meta", {})
            rows.append({
                "call_id": cid, "fecha": day, "agente": agente_de.get(cid),
                "dur_audio": me.get("dur_audio"), "proc_seg": me.get("proc_seg"),
                "device": me.get("device"), "chunks_descartados": me.get("chunks_descartados"),
                "chars": me.get("chars"), "transcript_anon": d.get("transcript_anon"),
            })
    replace_transcripciones(pd.DataFrame(rows), day)
    context.log.info(f"silver_transcriptions {day}: transcritas={len(rows)} de {len(ids)}")
    return MaterializeResult(metadata={
        "encoladas": len(ids),
        "transcritas": len(rows),
        "faltantes": len(ids) - len(rows),
    })


@asset(partitions_def=daily, group_name=ORO, deps=[silver_transcriptions],
       description="Evaluaciones de calidad/cumplimiento (determinista + Gemini) sobre texto anonimizado.")
def gold_evaluations(context: AssetExecutionContext) -> MaterializeResult:
    import json
    import time

    import pandas as pd
    from sqlalchemy import text as sqltext

    from src.analysis import gemini_eval
    from src.processing.config import pg_engine
    from src.processing.serving import replace_evaluaciones

    day = context.partition_key
    limit = int(os.getenv("EVAL_LIMIT", "0"))       # 0 = todas
    delay = float(os.getenv("EVAL_DELAY", "1.5"))   # respeta rate limit de Gemini

    eng = pg_engine()
    df = pd.read_sql(
        sqltext("SELECT call_id, agente, transcript_anon FROM servido.transcripciones "
                "WHERE fecha = :f"),
        eng, params={"f": day},
    )
    eng.dispose()
    if limit:
        df = df.head(limit)
    if not len(df):
        context.log.warning(f"gold_evaluations {day}: sin transcripciones.")
        return MaterializeResult(metadata={"evaluadas": 0})

    rows, fallidas = [], 0
    for _, r in df.iterrows():
        try:
            ev = gemini_eval.evaluar(r["call_id"], r["transcript_anon"] or "")
            rows.append({
                "call_id": ev.call_id, "fecha": day, "agente": r["agente"], "rubrica": ev.rubrica,
                "es_venta": ev.es_venta, "venta_valida": ev.venta_valida,
                "infraccion_critica": ev.infraccion_critica, "calidad_score": ev.calidad_score,
                "grupo_a": json.dumps(ev.grupo_A), "grupo_b": ",".join(ev.grupo_B_infracciones),
                "grupo_c": ",".join(ev.grupo_C_omisiones), "riesgo_reclamo": ev.riesgo_reclamo,
                "sentimiento_asesor": ev.sentimiento_asesor,
                "sentimiento_cliente": ",".join(ev.sentimiento_cliente_trayectoria),
                "confianza_llm": ev.confianza_llm, "modelo": ev.modelo,
            })
        except Exception as e:  # noqa: BLE001
            fallidas += 1
            context.log.warning(f"eval falló call={r['call_id']}: {e}")
        time.sleep(delay)

    replace_evaluaciones(pd.DataFrame(rows), day)
    validas = int(sum(x["venta_valida"] for x in rows))
    context.log.info(f"gold_evaluations {day}: evaluadas={len(rows)} fallidas={fallidas} "
                     f"venta_valida={validas}")
    return MaterializeResult(metadata={
        "evaluadas": len(rows), "fallidas": fallidas,
        "ventas_validas": validas, "criticas": len(rows) - validas,
    })


@asset(partitions_def=daily, group_name=ORO, deps=[gold_evaluations],
       description="KPIs y anomalías por agente/periodo + intentos por contacto. Fase 6.")
def gold_kpis(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("gold_kpis: esqueleto — agregados, anomalías y KPI de intentos (Fase 6).")
    return MaterializeResult(metadata={"estado": "esqueleto", "fase": 6})


defs = Definitions(
    assets=[
        bronze_cdr,
        bronze_audio_index,
        silver_calls,
        silver_transcriptions,
        gold_evaluations,
        gold_kpis,
    ]
)
