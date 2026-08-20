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

_running = True


def _stop(*_):
    global _running
    _running = False


def main() -> int:
    from confluent_kafka import Consumer, Producer

    import anonimizar
    import asr

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
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
        print(f"[worker] job call_id={call_id} audio={path}")

        t0 = time.time()
        try:
            tr = asr.transcribe(path, device=dev)
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
                    "chars": len(anon),
                },
            }
            print(f"[worker]   -> ok  dur={tr['dur_audio']}s proc={result['meta']['proc_seg']}s "
                  f"chars={len(anon)} descartados={tr['chunks_descartados']}")
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
