"""ASR con OpenVINO + anti-alucinación (Fase 3).

Transcribe un audio con Whisper (OpenVINO GenAI) en GPU Arc (o CPU) y aplica
un filtro anti-alucinación sobre los segmentos:
  - descarta segmentos repetidos consecutivos (idénticos),
  - descarta/colapsa segmentos con repetición anómala de un token (bucle típico
    de Whisper en silencios/tonos) o con muy baja diversidad léxica.

Reutilizable por worker.py (pipeline) y por los scripts de prueba/benchmark.
"""
import os
import re

import librosa
import numpy as np
import openvino_genai

MODEL_DIR = os.getenv("ASR_MODEL_DIR", os.path.join("data", "models", "whisper-small-int8-ov"))
DEVICE = os.getenv("ASR_DEVICE", "GPU")

_pipe = None
_pipe_dev = None


def get_pipe(device: str | None = None):
    """Carga (una vez) el WhisperPipeline. Cae a CPU si el dispositivo falla."""
    global _pipe, _pipe_dev
    dev = device or DEVICE
    if _pipe is None or _pipe_dev != dev:
        try:
            _pipe = openvino_genai.WhisperPipeline(MODEL_DIR, dev)
            _pipe_dev = dev
        except Exception:  # noqa: BLE001
            _pipe = openvino_genai.WhisperPipeline(MODEL_DIR, "CPU")
            _pipe_dev = "CPU"
    return _pipe


def _norm(t: str) -> str:
    return re.sub(r"\s+", " ", t.strip().lower())


def es_repetitivo(t: str, max_run: int = 4) -> bool:
    """True si un token se repite >=max_run veces seguidas, o si el texto largo
    tiene diversidad léxica muy baja (típico de alucinación)."""
    w = _norm(t).split()
    if not w:
        return False
    run = 1
    for i in range(1, len(w)):
        run = run + 1 if w[i] == w[i - 1] else 1
        if run >= max_run:
            return True
    return len(w) >= 12 and len(set(w)) / len(w) < 0.35


def _colapsar(t: str) -> str:
    """Colapsa repeticiones simples de una palabra (4+ veces) a una sola."""
    return re.sub(r"\b(\w+)( \1\b){3,}", r"\1", t, flags=re.IGNORECASE)


def _gen(pipe, audio):
    try:
        return pipe.generate(audio, language="<|es|>", task="transcribe", return_timestamps=True)
    except TypeError:
        cfg = pipe.get_generation_config()
        cfg.language, cfg.task, cfg.return_timestamps = "<|es|>", "transcribe", True
        return pipe.generate(audio, cfg)


def transcribe(path: str, device: str | None = None) -> dict:
    pipe = get_pipe(device)
    audio, _ = librosa.load(path, sr=16000, mono=True)
    audio = audio.astype(np.float32)
    dur = len(audio) / 16000
    res = _gen(pipe, audio)

    chunks = getattr(res, "chunks", None)
    segs, drop = [], 0
    if chunks:
        prev = None
        for c in chunks:
            txt = c.text.strip()
            if not txt:
                continue
            if (prev is not None and _norm(txt) == _norm(prev)) or es_repetitivo(txt):
                drop += 1
                continue
            segs.append({"start": round(c.start_ts, 2), "end": round(c.end_ts, 2), "text": txt})
            prev = txt
        limpio = " ".join(s["text"] for s in segs)
        n_chunks = len(chunks)
    else:
        limpio = _colapsar(str(res).strip())
        n_chunks = None

    return {
        "text": limpio,
        "segments": segs,
        "dur_audio": round(dur, 1),
        "chunks_total": n_chunks,
        "chunks_descartados": drop,
    }
