"""Demo diarizada (Fase 3): 2 llamadas largas → turnos ASESOR/CLIENTE, cruda vs anonimizada.

Para cada una de las N llamadas más largas de la muestra local: transcribe (GPU,
anti-alucinación), diariza (pyannote, CPU), fusiona turnos consecutivos del mismo
rol, y guarda dos versiones (cruda y anonimizada) con formato por turnos.

Uso:
  PYTHONIOENCODING=utf-8 whisper_worker/.venv/Scripts/python whisper_worker/demo_diarizado.py [N]
"""
import csv
import os
import sys
import time

import anonimizar as A
import asr
import diarizar as D

AUD = os.path.join("data", "muestra", "audios")
MAN = os.path.join("data", "muestra", "manifest.tsv")
OUT = os.path.join("data", "muestra", "ejemplos")


def longest(k):
    with open(MAN, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    rows.sort(key=lambda r: int(r["billsec"] or 0), reverse=True)
    out = []
    for r in rows:
        p = os.path.join(AUD, os.path.basename(r["audio_path"]))
        if os.path.exists(p):
            out.append((p, r))
        if len(out) >= k:
            break
    return out


def merge(segs):
    turnos = []
    for s in segs:
        if turnos and turnos[-1]["rol"] == s["rol"]:
            turnos[-1]["text"] += " " + s["text"]
        else:
            turnos.append({"rol": s["rol"], "text": s["text"]})
    return turnos


def main():
    os.makedirs(OUT, exist_ok=True)
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    for i, (p, r) in enumerate(longest(k), 1):
        t0 = time.time()
        tr = asr.transcribe(p, device="GPU")
        segs, n_spk = D.diarizar_segmentos(p, tr["segments"])
        turnos = merge(segs)
        cruda = "\n".join(f"{t['rol']}: {t['text']}" for t in turnos)
        anon = "\n".join(f"{t['rol']}: {A.anonimizar(t['text'])}" for t in turnos)
        base = os.path.basename(p)
        with open(os.path.join(OUT, f"{i}_{base}.DIAR_CRUDA.txt"), "w", encoding="utf-8") as f:
            f.write(cruda)
        with open(os.path.join(OUT, f"{i}_{base}.DIAR_ANON.txt"), "w", encoding="utf-8") as f:
            f.write(anon)
        print(f"[{i}] {base} agente={r['agente']} {int(r['billsec'])//60}min "
              f"hablantes={n_spk} turnos={len(turnos)} proc={time.time()-t0:.0f}s", flush=True)
    print(f"[demo] textos en {OUT}/  (*.DIAR_CRUDA.txt / *.DIAR_ANON.txt)")


if __name__ == "__main__":
    main()
