"""Runner de streaming near-real-time (Fase 5) — ESTRICTO SOLO LECTURA sobre Asterisk.

Bucle:
  1) POLL del CDR (SELECT) por llamadas finalizadas desde el cursor (guardado en NUESTRO
     PostgreSQL, jamás en Asterisk). Solo answered, ext 200–299, billsec ≥ mínimo.
  2) LOCALIZA la grabación por ruta construida (sftp.stat, solo metadatos).
  3) ATERRIZA el MP3 crudo en Bronce/MinIO (SFTP getfo → put; nunca escribe en Asterisk).
  4) UPSERT de la llamada en servido.llamadas (origen='streaming').
  5) ENCOLA asr.jobs (clave S3 + diarizar solo si la llamada es larga/relevante).
  6) DRENA asr.results del worker → servido.transcripciones (+ evaluación Gemini → evaluaciones).

Seguridad: el usuario de BD es `lectura` (solo SELECT) y todo el acceso a grabaciones es
por SFTP de lectura por ruta exacta. Nada se modifica en el call center en producción.

Ejecución (dentro del contenedor Dagster, que ve kafka:9092/minio:9000/postgres:5432):
  docker exec uisrael_dagster_webserver bash -lc \
    "cd /opt/dagster/app && python -m src.streaming.runner"
Variables: STREAM_POLL_SECS, STREAM_GRACE_SECS, STREAM_LOOKBACK_SECS, STREAM_MIN_BILLSEC,
           STREAM_DIARIZE_MIN, STREAM_MAX_CYCLES (0=infinito), STREAM_EVAL (0/1).
"""
import json
import os
import time
from datetime import datetime

from sqlalchemy import text

from src.processing.audio_landing import audio_key, land_paths
from src.processing.config import AUDIO_ROOT, cdr_engine
from src.processing.serving import (
    get_cursor,
    set_cursor,
    upsert_evaluacion,
    upsert_llamada,
    upsert_transcripcion,
)
from src.streaming.locate import ext_agente, localizar

CURSOR = "cdr_stream"
BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POLL = int(os.getenv("STREAM_POLL_SECS", "30"))
GRACE = int(os.getenv("STREAM_GRACE_SECS", "45"))
LOOKBACK = int(os.getenv("STREAM_LOOKBACK_SECS", "1800"))
MIN_BILL = int(os.getenv("STREAM_MIN_BILLSEC", "1"))
DIAR_MIN = int(os.getenv("STREAM_DIARIZE_MIN", "600"))
# Modo de diarización: 'defer' = NO diariza en línea (se marca la llamada larga para
# diarizarla después con GPU); 'inline' = diariza las largas al vuelo (lento en CPU).
DIARIZE_MODE = os.getenv("STREAM_DIARIZE_MODE", "defer")
MAX_CYCLES = int(os.getenv("STREAM_MAX_CYCLES", "0"))
MAX_PER_CYCLE = int(os.getenv("STREAM_MAX_PER_CYCLE", "0"))  # 0 = sin tope
DO_EVAL = os.getenv("STREAM_EVAL", "1") == "1"
# Umbral de duración para el envío a Gemini: SOLO se evalúan (gastan tokens) las
# llamadas cuya grabación supera este valor. Las más cortas se transcriben, diarizan
# y anonimizan igual, pero NO se mandan al LLM (ahorro de tokens; decisión del tesista).
EVAL_MIN_SECS = int(os.getenv("STREAM_EVAL_MIN_SECS", "600"))  # 10 min
LAND_SLEEP = float(os.getenv("AUDIO_LAND_SLEEP", "0.02"))
TOL = int(os.getenv("STREAM_LOCATE_TOL", "8"))


def _sftp():
    import paramiko

    host = os.environ["AST_SSH_HOST"]
    user = os.environ["AST_SSH_USER"]
    pw = os.environ["AST_SSH_PASSWORD"]
    port = int(os.getenv("AST_SSH_PORT", "22"))
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(host, port=port, username=user, password=pw, timeout=20)
    return cli, cli.open_sftp()


def _poll_cdr(eng, desde: str, hasta: str):
    """SELECT de solo lectura: llamadas finalizadas en (desde, hasta]."""
    q = text(
        "SELECT calldate, dst, channel, dstchannel, billsec, duration, disposition, uniqueid "
        "FROM cdr "
        "WHERE calldate > :d AND calldate <= :h "
        "AND disposition = 'ANSWERED' AND billsec >= :b "
        "AND (channel REGEXP 'SIP/2[0-9][0-9]' OR dstchannel REGEXP 'SIP/2[0-9][0-9]') "
        "ORDER BY calldate ASC"
    )
    with eng.connect() as c:
        return [dict(r._mapping) for r in c.execute(q, {"d": desde, "h": hasta, "b": MIN_BILL})]


def _server_now(eng) -> datetime:
    with eng.connect() as c:
        return list(c.execute(text("SELECT NOW()")))[0][0]


def main() -> int:
    from confluent_kafka import Consumer, Producer

    eng = cdr_engine()
    cli, sftp = _sftp()
    prod = Producer({"bootstrap.servers": BOOT})
    cons = Consumer({
        "bootstrap.servers": BOOT,
        "group.id": "stream-results",
        "auto.offset.reset": "latest",   # solo resultados nuevos de este runner
        "enable.auto.commit": True,
    })
    cons.subscribe(["asr.results"])

    pendientes: dict[str, dict] = {}   # call_id -> metadatos de la llamada, esperando ASR
    print(f"[stream] runner iniciado boot={BOOT} poll={POLL}s grace={GRACE}s "
          f"lookback={LOOKBACK}s eval={DO_EVAL} eval_min={EVAL_MIN_SECS}s "
          f"diarize={DIARIZE_MODE}")

    cycle = 0
    try:
        while True:
            cycle += 1
            now = _server_now(eng)
            hasta = (now.timestamp() - GRACE)
            hasta_s = datetime.fromtimestamp(hasta).strftime("%Y-%m-%d %H:%M:%S")
            desde_s = get_cursor(CURSOR) or datetime.fromtimestamp(
                now.timestamp() - LOOKBACK).strftime("%Y-%m-%d %H:%M:%S")

            filas = _poll_cdr(eng, desde_s, hasta_s)
            loc = land = 0
            capado = False
            last_examined = None
            for f in filas:
                if MAX_PER_CYCLE and land >= MAX_PER_CYCLE:
                    capado = True
                    break
                last_examined = f["calldate"]
                ext = ext_agente(f["channel"], f["dstchannel"])
                tel = f["dst"]
                cid = f["uniqueid"]
                if not ext or not tel or not cid:
                    continue
                hit = localizar(sftp, AUDIO_ROOT, ext, tel, f["calldate"], tol=TOL)
                if not hit:
                    continue
                loc += 1
                path, dz, fmt, sz = hit
                day = f["calldate"].strftime("%Y-%m-%d")
                # aterrizar el crudo en Bronce (reutiliza el SFTP abierto; solo lectura)
                land_paths([path], day, sleep_s=LAND_SLEEP, sftp=sftp, log=lambda *_: None)
                land += 1
                base = os.path.basename(path)
                key = audio_key(day, base)
                upsert_llamada({
                    "call_id": cid, "audio_path": key, "agente": ext, "telefono": tel,
                    "calldate": f["calldate"], "ts_grabacion": f["calldate"],
                    "diff_seg": abs(dz), "billsec": int(f["billsec"] or 0),
                    "duration": int(f["duration"] or 0), "disposition": f["disposition"],
                    "en_muestra": True, "fecha": day, "origen": "streaming",
                })
                es_larga = int(f["billsec"] or 0) >= DIAR_MIN
                diarize_now = (DIARIZE_MODE == "inline") and es_larga
                prod.produce("asr.jobs", json.dumps({
                    "call_id": cid, "audio_path": key, "diarize": diarize_now,
                }).encode("utf-8"))
                pendientes[cid] = {"agente": ext, "fecha": day,
                                   "billsec": int(f["billsec"] or 0), "es_larga": es_larga}
            prod.flush(10)
            # Avance seguro del cursor: si se capó el ciclo, solo hasta la última fila
            # EXAMINADA (para no saltarse las que no se procesaron); si no, hasta `hasta`.
            if capado and last_examined is not None:
                nuevo_cursor = last_examined.strftime("%Y-%m-%d %H:%M:%S")
            else:
                nuevo_cursor = hasta_s
            if filas:
                set_cursor(CURSOR, nuevo_cursor)
            print(f"[stream] ciclo {cycle}: cdr={len(filas)} localizadas={loc} "
                  f"aterrizadas={land} pendientes_asr={len(pendientes)} cursor={nuevo_cursor}")

            # drenar resultados del worker (no bloqueante, presupuesto corto)
            _drenar(cons, pendientes)

            if MAX_CYCLES and cycle >= MAX_CYCLES:
                break
            time.sleep(POLL)
    finally:
        cons.close()
        sftp.close()
        cli.close()
        eng.dispose()
    print("[stream] runner detenido.")
    return 0


def _drenar(cons, pendientes: dict, budget: float = 8.0):
    """Consume asr.results disponibles y persiste transcripción. Envía a Gemini
    SOLO las llamadas cuya grabación supera EVAL_MIN_SECS (ahorro de tokens)."""
    t0 = time.time()
    persistidos = evaluados = omitidas_cortas = 0
    while time.time() - t0 < budget:
        m = cons.poll(1.0)
        if m is None:
            if not pendientes:
                break
            continue
        if m.error():
            continue
        try:
            d = json.loads(m.value())
        except Exception:  # noqa: BLE001
            continue
        cid = d.get("call_id")
        if cid not in pendientes:
            continue
        info = pendientes.pop(cid)
        if d.get("estado") != "ok":
            continue
        me = d.get("meta", {})
        # Diarización diferida: si el worker no la hizo, la larga queda marcada para GPU.
        diarizado = me.get("n_hablantes") is not None
        requiere_diar = bool(info.get("es_larga")) and not diarizado
        # SIEMPRE se persiste la transcripción anonimizada (proceso completo).
        upsert_transcripcion({
            "call_id": cid, "fecha": info["fecha"], "agente": info["agente"],
            "dur_audio": me.get("dur_audio"), "proc_seg": me.get("proc_seg"),
            "device": me.get("device"), "chunks_descartados": me.get("chunks_descartados"),
            "chars": me.get("chars"), "transcript_anon": d.get("transcript_anon"),
            "requiere_diarizacion": requiere_diar, "diarizado": diarizado,
        })
        persistidos += 1
        # Duración de la grabación (audio real medido por Whisper; respaldo: billsec del CDR).
        dur = me.get("dur_audio")
        if dur is None:
            dur = info.get("billsec", 0)
        # Envío a Gemini SOLO si supera el umbral → evita gastar tokens en llamadas cortas.
        if DO_EVAL and (dur or 0) > EVAL_MIN_SECS:
            _evaluar(cid, info, d.get("transcript_anon") or "")
            evaluados += 1
        else:
            omitidas_cortas += 1
    if persistidos:
        print(f"[stream]   persistidas {persistidos} transcripciones · "
              f"evaluadas Gemini (>{EVAL_MIN_SECS}s)={evaluados} · "
              f"cortas sin evaluar={omitidas_cortas} (pendientes={len(pendientes)})")


def _evaluar(cid: str, info: dict, transcript: str) -> None:
    try:
        from src.analysis import gemini_eval

        ev = gemini_eval.evaluar(cid, transcript)
        upsert_evaluacion({
            "call_id": ev.call_id, "fecha": info["fecha"], "agente": info["agente"],
            "rubrica": ev.rubrica, "es_venta": ev.es_venta, "venta_valida": ev.venta_valida,
            "infraccion_critica": ev.infraccion_critica, "calidad_score": ev.calidad_score,
            "grupo_a": json.dumps(ev.grupo_A), "grupo_b": ",".join(ev.grupo_B_infracciones),
            "grupo_c": ",".join(ev.grupo_C_omisiones), "riesgo_reclamo": ev.riesgo_reclamo,
            "sentimiento_asesor": ev.sentimiento_asesor,
            "sentimiento_cliente": ",".join(ev.sentimiento_cliente_trayectoria),
            "confianza_llm": ev.confianza_llm, "modelo": ev.modelo,
        })
    except Exception as e:  # noqa: BLE001
        print(f"[stream]   eval falló call={cid}: {e}")


if __name__ == "__main__":
    raise SystemExit(main())
