"""Whisper worker (Fase 3) — transcripción + anonimización, desacoplado por Kafka.

Bucle del transcriptor nativo (host + OpenVINO/GPU Arc). Consume `asr.jobs`
({call_id, audio_path}), transcribe con anti-alucinación (asr.py), anonimiza
(anonimizar.py) y publica en `asr.results` la transcripción ANONIMIZADA (única
que puede salir hacia la nube) + metadatos. La diarización se añadirá cuando
haya token de Hugging Face (pyannote).

Arranque (dentro del venv):
  whisper_worker/.venv/Scripts/python whisper_worker/worker.py
Variables: ASR_DEVICE (GPU/CPU), KAFKA_BOOTSTRAP, MAX_MSGS (0=infinito, N=procesa N y sale).
"""
import json
import os
import signal
import sys
import time

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_IN = os.getenv("ASR_JOBS_TOPIC", "asr.jobs")
TOPIC_OUT = os.getenv("ASR_RESULTS_TOPIC", "asr.results")
GROUP_ID = os.getenv("ASR_GROUP", "whisper-worker")
MAX_MSGS = int(os.getenv("MAX_MSGS", "0"))
DIARIZE = os.getenv("DIARIZE", "1") == "1"

_running = True


def _stop(*_):
    global _running
    _running = False


def main() -> int:
    from confluent_kafka import Consumer, Producer

    import anonimizar
    import asr
    import audio_source

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # La transcripción + diarización de una llamada larga puede tardar varios minutos.
        # Sin esto, Kafka expulsa al consumidor del grupo (MAXPOLL) y reprocesa en bucle.
        # Se amplía el intervalo máximo de poll para procesamiento largo por mensaje.
        "max.poll.interval.ms": 1800000,   # 30 min
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_IN])
    dev = os.getenv("ASR_DEVICE", "GPU")
    print(f"[worker] escuchando '{TOPIC_IN}' en {BOOTSTRAP} (device={dev}, max={MAX_MSGS or 'inf'})")

    processed = 0
    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            if MAX_MSGS and processed >= MAX_MSGS:
                break
            continue
        if msg.error():
            print("[worker] error:", msg.error())
            continue
        try:
            job = json.loads(msg.value())
        except Exception:  # noqa: BLE001
            job = {}
        call_id, path = job.get("call_id"), job.get("audio_path")
        # Política por trabajo (streaming): diarizar solo llamadas relevantes/largas.
        # Si el trabajo no trae 'diarize', se usa el default global DIARIZE.
        do_diar = job.get("diarize", DIARIZE)
        print(f"[worker] job call_id={call_id} audio={path} diarize={do_diar}")

        t0 = time.time()
        try:
            local_path = audio_source.resolver(path)   # descarga de MinIO si es clave S3
            tr = asr.transcribe(local_path, device=dev)
            n_spk = None
            if do_diar and tr["segments"]:
                try:
                    import diarizar
                    segs, n_spk = diarizar.diarizar_segmentos(local_path, tr["segments"])
                    turnos = []
                    for s in segs:  # fusionar turnos consecutivos del mismo rol
                        if turnos and turnos[-1]["rol"] == s["rol"]:
                            turnos[-1]["text"] += " " + s["text"]
                        else:
                            turnos.append({"rol": s["rol"], "text": s["text"]})
                    anon = "\n".join(
                        f"{t['rol']}: {anonimizar.anonimizar(t['text'])}" for t in turnos
                    )
                except Exception as de:  # noqa: BLE001
                    print(f"[worker]   diarización falló ({de}); sigo sin diarizar")
                    anon = anonimizar.anonimizar(tr["text"])
            else:
                anon = anonimizar.anonimizar(tr["text"])
            result = {
                "call_id": call_id,
                "transcript_anon": anon,
                "estado": "ok",
                "meta": {
                    "dur_audio": tr["dur_audio"],
                    "proc_seg": round(time.time() - t0, 1),
                    "device": dev,
                    "chunks_descartados": tr["chunks_descartados"],
                    "n_hablantes": n_spk,
                    "chars": len(anon),
                },
            }
            print(f"[worker]   -> ok  dur={tr['dur_audio']}s proc={result['meta']['proc_seg']}s "
                  f"hablantes={n_spk} chars={len(anon)} descartados={tr['chunks_descartados']}")
        except Exception as e:  # noqa: BLE001
            result = {"call_id": call_id, "transcript_anon": None, "estado": "error", "error": str(e)}
            print(f"[worker]   -> ERROR: {e}")

        producer.produce(TOPIC_OUT, json.dumps(result, ensure_ascii=False).encode("utf-8"))
        producer.flush(10)
        consumer.commit(msg)
        processed += 1
        if MAX_MSGS and processed >= MAX_MSGS:
            break

    consumer.close()
    print(f"[worker] detenido (procesados={processed}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
