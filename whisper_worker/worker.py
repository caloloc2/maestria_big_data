"""Whisper worker — esqueleto (Fase 0.D).

Bucle de trabajo del transcriptor nativo (host + OpenVINO). Por ahora es un
ESQUELETO: consume `asr.jobs`, registra el mensaje y publica un `asr.results`
de marcador. La transcripción real (faster-whisper/OpenVINO + diarización +
anti-alucinación) se implementa en la Fase 3.

Arranque (dentro del venv):
  whisper_worker/.venv/Scripts/python whisper_worker/worker.py
"""
import json
import os
import signal
import sys

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_IN = os.getenv("ASR_JOBS_TOPIC", "asr.jobs")
TOPIC_OUT = os.getenv("ASR_RESULTS_TOPIC", "asr.results")
GROUP_ID = os.getenv("ASR_GROUP", "whisper-worker")

_running = True


def _stop(*_):
    global _running
    _running = False


def main() -> int:
    from confluent_kafka import Consumer, Producer

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
    print(f"[whisper_worker] escuchando '{TOPIC_IN}' en {BOOTSTRAP} (esqueleto Fase 0)")

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("[whisper_worker] error:", msg.error())
            continue
        try:
            job = json.loads(msg.value())
        except Exception:
            job = {"raw": msg.value().decode("utf-8", "replace")}
        print("[whisper_worker] job recibido:", job)

        # TODO(Fase 3): normalizar audio (ffmpeg) → faster-whisper/OpenVINO (GPU Arc)
        #               → diarización → anti-alucinación → anonimización.
        result = {
            "call_id": job.get("call_id"),
            "transcript": None,
            "estado": "esqueleto_fase0",
        }
        producer.produce(TOPIC_OUT, json.dumps(result).encode("utf-8"))
        producer.flush(5)
        consumer.commit(msg)

    consumer.close()
    print("[whisper_worker] detenido.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
