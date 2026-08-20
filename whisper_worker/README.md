# `whisper_worker/` — Transcripción nativa (fuera de Docker)

Proceso Python **nativo en el host** (no contenedor) que transcribe audio con
**faster-whisper + OpenVINO**, aprovechando la GPU **Intel Arc 140V** en desarrollo
(o CPU en producción). Se comunica con el pipeline por topics Kafka:

- consume `asr.jobs`  → `{call_id, audio_path}`
- publica `asr.results` → `{call_id, transcript, metadata}`

**Por qué fuera de Docker:** validado el 2026-08-15 que el compute-runtime de Intel
en contenedores solo expone CPU; para usar la GPU Arc, Whisper corre en el host.

Se implementa/valida en las **Fases 0.D** (esqueleto + verificación de OpenVINO) y
**3** (transcripción real + diarización + anti-alucinación). Ver `fases.md`.
