"""Transcripción (ASR): coordina el paso audio→texto con el `whisper_worker`.

Publica trabajos en el topic Kafka `asr.jobs` y consume `asr.results`. El motor
real (faster-whisper + OpenVINO) corre en `whisper_worker/`. Fase 3.
"""
