"""Benchmark de modelos ASR: small vs medium (Fase 3, pulido).

Transcribe el MISMO conjunto de llamadas con Whisper `small` y `medium` (OpenVINO,
GPU), compara tiempo/RTF y guarda las transcripciones para comparar EXACTITUD
(p. ej. "Marketing VIP" vs "Marketing BIP"). Resultados para el documento.

Uso:
  PYTHONIOENCODING=utf-8 whisper_worker/.venv/Scripts/python whisper_worker/bench_modelos.py
"""
import os
import time

import librosa
import numpy as np
import openvino_genai
from huggingface_hub import snapshot_download

AUD = os.path.join("data", "muestra", "audios")
OUT = os.path.join("data", "muestra", "bench_modelos")

MODELS = {
    "small": ["OpenVINO/whisper-small-int8-ov"],
    "medium": ["OpenVINO/whisper-medium-int8-ov", "OpenVINO/whisper-medium-fp16-ov"],
}
# 1 larga (con "Marketing VIP" → muestra el error del small) + 1 media (velocidad).
CALLS = ["20210915143330-204-0987169147.mp3", "20201014121157-250-0991253778.mp3"]


def get_model(key):
    d = os.path.join("data", "models", f"whisper-{key}")
    if os.path.isdir(d) and os.listdir(d):
        return d
    for rid in MODELS[key]:
        try:
            print(f"  descargando {rid} ...", flush=True)
            snapshot_download(rid, local_dir=d)
            return d
        except Exception as e:  # noqa: BLE001
            print(f"  no disponible {rid}: {e}", flush=True)
    raise RuntimeError(f"sin modelo para {key}")


def _gen(pipe, audio):
    try:
        return pipe.generate(audio, language="<|es|>", task="transcribe", return_timestamps=True)
    except TypeError:
        cfg = pipe.get_generation_config()
        cfg.language, cfg.task, cfg.return_timestamps = "<|es|>", "transcribe", True
        return pipe.generate(audio, cfg)


def main():
    os.makedirs(OUT, exist_ok=True)
    audios = []
    for c in CALLS:
        a, _ = librosa.load(os.path.join(AUD, c), sr=16000, mono=True)
        audios.append((c, a.astype(np.float32), len(a) / 16000))

    for key in ["small", "medium"]:
        mdir = get_model(key)
        print(f"\n=== modelo {key} ({mdir}) ===", flush=True)
        pipe = openvino_genai.WhisperPipeline(mdir, "GPU")
        for c, a, dur in audios:
            t0 = time.time()
            res = _gen(pipe, a)
            dt = time.time() - t0
            txt = str(res).strip()
            with open(os.path.join(OUT, f"{key}__{c}.txt"), "w", encoding="utf-8") as f:
                f.write(txt)
            print(f"  {c}  dur={dur:.0f}s  proc={dt:.1f}s  RTF={dt/dur:.3f}x  chars={len(txt)}",
                  flush=True)
        del pipe
    print("\n[bench] transcripciones en", OUT, flush=True)


if __name__ == "__main__":
    main()
