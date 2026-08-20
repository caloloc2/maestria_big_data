"""Validación del entorno del Whisper worker (Fase 0.D).

Comprueba dos cosas, sin transcribir todavía:
  1) OpenVINO detecta dispositivos de cómputo — idealmente incluye la GPU Arc.
  2) Kafka es alcanzable desde el host (listener HOST en localhost:29092).

Uso (dentro del venv):
  whisper_worker/.venv/Scripts/python whisper_worker/validate_env.py
"""
import os

OK = "OK "
FAIL = "FALLO"


def check_openvino():
    print("\n[1] OpenVINO — dispositivos de cómputo")
    try:
        from openvino import Core
        core = Core()
        devs = core.available_devices
        print(f"  {OK} OpenVINO importado. Dispositivos: {devs}")
        for d in devs:
            try:
                name = core.get_property(d, "FULL_DEVICE_NAME")
                print(f"       - {d}: {name}")
            except Exception:
                print(f"       - {d}")
        if any(x.startswith("GPU") for x in devs):
            print(f"  {OK} GPU detectada → Whisper podrá acelerarse en la Arc 140V.")
        else:
            print(f"  {FAIL} No se ve GPU (solo CPU). Revisar drivers/OpenVINO GPU plugin.")
        return any(x.startswith("GPU") for x in devs)
    except Exception as e:
        print(f"  {FAIL} {type(e).__name__}: {e}")
        return False


def check_kafka():
    print("\n[2] Kafka — alcanzable desde el host (localhost:29092)")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": bootstrap})
        md = admin.list_topics(timeout=10)
        topics = sorted(md.topics.keys())
        print(f"  {OK} Conectado a {bootstrap}. Topics visibles: {topics}")
        return True
    except Exception as e:
        print(f"  {FAIL} {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("VALIDACIÓN DEL ENTORNO — Whisper worker (Fase 0.D)")
    print("=" * 60)
    gpu = check_openvino()
    kafka = check_kafka()
    print("\n" + "=" * 60)
    print(f"RESULTADO: OpenVINO GPU={'SÍ' if gpu else 'no'}   Kafka={'OK' if kafka else 'FALLO'}")
    print("=" * 60)
