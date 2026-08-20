"""Demo (Fase 3): transcripción cruda → anonimizada de N llamadas largas.

Toma las N llamadas más largas de la muestra local, las transcribe (con
anti-alucinación), lista las entidades PII redactadas y guarda el texto crudo y
el anonimizado en data/muestra/ejemplos/ para inspección del dueño de los datos.

Uso:
  whisper_worker/.venv/Scripts/python whisper_worker/demo_ejemplos.py [N]
"""
import csv
import os
import sys

import anonimizar as A
import asr

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


def main():
    os.makedirs(OUT, exist_ok=True)
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    for i, (p, r) in enumerate(longest(k), 1):
        tr = asr.transcribe(p, device="GPU")
        raw = tr["text"]
        anon = A.anonimizar(raw)
        pre = A.redactar_numeros_hablados(raw)
        res = A._analyzer.analyze(text=pre, language="es", entities=A._ENTS)
        ents = [(x.entity_type, pre[x.start:x.end]) for x in res]

        base = os.path.basename(p)
        with open(os.path.join(OUT, f"{i}_{base}.CRUDA.txt"), "w", encoding="utf-8") as f:
            f.write(raw)
        with open(os.path.join(OUT, f"{i}_{base}.ANON.txt"), "w", encoding="utf-8") as f:
            f.write(anon)

        print(f"\n{'='*70}\nEJEMPLO {i}: {base}")
        print(f"agente={r['agente']}  año={r['year']}  billsec={r['billsec']}s "
              f"(~{int(r['billsec'])//60} min)  proceso={tr['dur_audio']}s audio, "
              f"chunks_descartados={tr['chunks_descartados']}")
        tags = {t: anon.count(f"<{t}>") for t in ("NOMBRE", "TELEFONO", "CEDULA",
                                                  "TARJETA", "DATO_NUMERICO")}
        print("Redacciones:", ", ".join(f"{k2}={v}" for k2, v in tags.items() if v))
        if ents:
            print("Entidades detectadas (tipo → valor crudo):")
            for et, val in ents[:12]:
                print(f"   - {et}: {val}")
        print(f"\n--- CRUDA (primeros 700) ---\n{raw[:700]}")
        print(f"\n--- ANONIMIZADA (primeros 700) ---\n{anon[:700]}")
    print(f"\n[demo] textos completos en {OUT}/")


if __name__ == "__main__":
    main()
