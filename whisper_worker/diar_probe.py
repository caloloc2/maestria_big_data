"""Sonda de diarización (Fase 3) — valida pyannote sobre un audio real.

Carga el pipeline pyannote/speaker-diarization-3.1 (con HF_TOKEN del .env),
lo corre sobre un audio y reporta cuántos hablantes detectó y los primeros turnos.
Confirma que los términos del modelo fueron aceptados y que corre en la máquina.

Uso:
  whisper_worker/.venv/Scripts/python whisper_worker/diar_probe.py [ruta.mp3]
"""
import glob
import os
import sys
import time

import librosa
import torch
from dotenv import load_dotenv

load_dotenv(".env")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else sorted(glob.glob("data/muestra/audios/*.mp3"))[0]
    token = os.environ["HF_TOKEN"]

    from pyannote.audio import Pipeline

    print(f"[diar] cargando pipeline (1ª vez descarga modelos)...")
    t0 = time.time()
    pipe = Pipeline.from_pretrained("pyannote/speaker-diarization-3.1", use_auth_token=token)
    print(f"[diar] pipeline cargado en {time.time()-t0:.1f}s")

    a, _ = librosa.load(path, sr=16000, mono=True)
    dur = len(a) / 16000
    wav = torch.from_numpy(a).unsqueeze(0)
    print(f"[diar] audio {os.path.basename(path)}  dur={dur:.1f}s  diarizando (CPU)...")
    t0 = time.time()
    diar = pipe({"waveform": wav, "sample_rate": 16000})
    dt = time.time() - t0

    turns = [(t.start, t.end, spk) for t, _, spk in diar.itertracks(yield_label=True)]
    speakers = sorted({spk for _, _, spk in turns})
    print(f"[diar] LISTO en {dt:.1f}s (RTF {dt/dur:.2f}x) | hablantes={len(speakers)} {speakers}")
    print("[diar] primeros turnos:")
    for s, e, spk in turns[:12]:
        print(f"   {s:6.1f}-{e:6.1f}s  {spk}")


if __name__ == "__main__":
    main()
