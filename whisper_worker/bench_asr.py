"""Benchmark ASR de llamadas LARGAS (Fase 3) — GPU Arc vs CPU.

Toma las K llamadas más largas de la muestra local (data/muestra/manifest.tsv),
las transcribe en CPU y en GPU Arc con OpenVINO GenAI (long-form, con
return_timestamps) y reporta tiempo, factor tiempo-real (RTF) y promedios.
Da el "tiempo promedio de proceso" para llamadas largas — la medición
representativa del trabajo real de auditoría.

Uso:
  whisper_worker/.venv/Scripts/python whisper_worker/bench_asr.py [K]
"""
import csv
import os
import sys
import time

import librosa
import numpy as np
import openvino_genai

MODEL_DIR = os.path.join("data", "models", "whisper-small-int8-ov")
AUD = os.path.join("data", "muestra", "audios")
MAN = os.path.join("data", "muestra", "manifest.tsv")


def longest(k: int):
    with open(MAN, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    rows.sort(key=lambda r: int(r["billsec"] or 0), reverse=True)
    out = []
    for r in rows:
        p = os.path.join(AUD, os.path.basename(r["audio_path"]))
        if os.path.exists(p):
            out.append((p, int(r["billsec"])))
        if len(out) >= k:
            break
    return out


def _gen(pipe, audio):
    try:
        return pipe.generate(audio, language="<|es|>", task="transcribe", return_timestamps=True)
    except TypeError:
        cfg = pipe.get_generation_config()
        cfg.language, cfg.task, cfg.return_timestamps = "<|es|>", "transcribe", True
        return pipe.generate(audio, cfg)


def main() -> int:
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    files = longest(k)
    print(f"[bench] {len(files)} llamadas largas:")
    audios = []
    for p, b in files:
        a, _ = librosa.load(p, sr=16000, mono=True)
        audios.append((os.path.basename(p), a.astype(np.float32), len(a) / 16000))
        print(f"  {os.path.basename(p)}  billsec={b}s  audio={len(a) / 16000:.0f}s")

    for dev in ["CPU", "GPU"]:
        print(f"\n=== {dev} ===", flush=True)
        pipe = openvino_genai.WhisperPipeline(MODEL_DIR, dev)
        tot_t = tot_d = 0.0
        for name, a, dur in audios:
            t0 = time.time()
            _gen(pipe, a)
            gt = time.time() - t0
            tot_t += gt
            tot_d += dur
            print(f"  {name}  dur={dur:.0f}s  proc={gt:.1f}s  RTF={gt / dur:.3f}x", flush=True)
        print(f"  -> PROMEDIO {dev}: RTF={tot_t / tot_d:.3f}x  "
              f"({tot_t:.0f}s de proceso para {tot_d:.0f}s de audio)", flush=True)
        del pipe
    return 0


if __name__ == "__main__":
    sys.exit(main())
