"""Sonda ASR (Fase 3) — valida Whisper sobre OpenVINO en CPU y GPU Arc.

Descarga un modelo Whisper ya convertido a OpenVINO IR (int8), transcribe UN
audio real de la muestra en cada dispositivo y reporta tiempo + factor
tiempo-real (RTF = tiempo_transcripción / duración_audio). Primer punto del
benchmark GPU vs CPU.

Uso:
  whisper_worker/.venv/Scripts/python whisper_worker/asr_probe.py [ruta.mp3]
"""
import glob
import os
import sys
import time

import librosa
import numpy as np
import openvino_genai
from huggingface_hub import snapshot_download

MODEL_ID = "OpenVINO/whisper-small-int8-ov"
MODEL_DIR = os.path.join("data", "models", "whisper-small-int8-ov")


def get_model() -> str:
    if not os.path.isdir(MODEL_DIR) or not os.listdir(MODEL_DIR):
        print(f"[modelo] descargando {MODEL_ID} ...")
        snapshot_download(MODEL_ID, local_dir=MODEL_DIR)
    print(f"[modelo] {MODEL_DIR}")
    return MODEL_DIR


def load_audio(path: str) -> np.ndarray:
    a, _ = librosa.load(path, sr=16000, mono=True)
    return a.astype(np.float32)


def transcribe(device: str, model_dir: str, audio: np.ndarray):
    t0 = time.time()
    pipe = openvino_genai.WhisperPipeline(model_dir, device)
    load_t = time.time() - t0
    t0 = time.time()
    try:
        res = pipe.generate(audio, language="<|es|>", task="transcribe")
    except TypeError:
        cfg = pipe.get_generation_config()
        cfg.language, cfg.task = "<|es|>", "transcribe"
        res = pipe.generate(audio, cfg)
    return str(res), load_t, time.time() - t0


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else sorted(glob.glob("data/muestra/audios/*.mp3"))[0]
    md = get_model()
    audio = load_audio(path)
    dur = len(audio) / 16000
    print(f"[audio] {os.path.basename(path)}  duración={dur:.1f}s\n")
    for dev in ["CPU", "GPU"]:
        try:
            txt, lt, gt = transcribe(dev, md, audio)
            print(f"=== {dev} ===  carga={lt:.1f}s  transcripción={gt:.1f}s  RTF={gt/dur:.2f}x")
            print(txt.strip()[:500], "\n")
        except Exception as e:  # noqa: BLE001
            print(f"=== {dev} === ERROR: {e}\n")
