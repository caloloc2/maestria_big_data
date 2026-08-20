"""Diarización agente/cliente (Fase 3) con pyannote.

Sobre audio MONO mezclado, separa hablantes por características de voz
(pyannote/speaker-diarization-3.1), asigna cada segmento del ASR al hablante con
mayor solape temporal, y decide el ROL (ASESOR/CLIENTE) por heurística:
frases-ancla del guion + quién habla más (el asesor domina el pitch).

Corre en CPU (torch); RTF ~0,85× → es el paso más lento del pipeline.
"""
import os
from collections import defaultdict

import librosa
import torch
from dotenv import load_dotenv

load_dotenv(".env")

ANCLAS = (
    "mi nombre es", "le habla", "asesor", "asesora", "corporación marketing",
    "marketing vip", "marketing bip", "me comunico", "le llamo", "compañía internacional",
    "invitación", "beneficio",
)

_pipe = None


def get_pipe():
    global _pipe
    if _pipe is None:
        from pyannote.audio import Pipeline
        _pipe = Pipeline.from_pretrained(
            "pyannote/speaker-diarization-3.1", use_auth_token=os.environ["HF_TOKEN"]
        )
    return _pipe


def diarizar(path: str):
    a, _ = librosa.load(path, sr=16000, mono=True)
    wav = torch.from_numpy(a).unsqueeze(0)
    diar = get_pipe()({"waveform": wav, "sample_rate": 16000})
    return [(t.start, t.end, spk) for t, _, spk in diar.itertracks(yield_label=True)]


def _spk_de_segmento(seg, turns):
    best, best_ov = None, 0.0
    for s, e, spk in turns:
        ov = min(seg["end"], e) - max(seg["start"], s)
        if ov > best_ov:
            best_ov, best = ov, spk
    return best


def _roles(turns, segs_por_spk):
    dur = defaultdict(float)
    for s, e, spk in turns:
        dur[spk] += e - s
    if not dur:
        return {}
    # puntaje de anclas por hablante
    anclas = {spk: sum(txt.lower().count(a) for a in ANCLAS)
              for spk, txt in segs_por_spk.items()}
    if anclas and max(anclas.values()) > 0:
        asesor = max(anclas, key=anclas.get)
    else:
        asesor = max(dur, key=dur.get)  # respaldo: el que más habla
    return {spk: ("ASESOR" if spk == asesor else "CLIENTE") for spk in dur}


def diarizar_segmentos(path: str, segments: list):
    """Devuelve (segmentos_con_rol, n_hablantes)."""
    turns = diarizar(path)
    # texto por hablante para la heurística de rol
    por_spk = defaultdict(str)
    tmp = []
    for seg in segments:
        spk = _spk_de_segmento(seg, turns)
        por_spk[spk] += " " + seg["text"]
        tmp.append((seg, spk))
    roles = _roles(turns, por_spk)
    out = [{"rol": roles.get(spk, "?"), "speaker": spk,
            "start": seg["start"], "end": seg["end"], "text": seg["text"]}
           for seg, spk in tmp]
    return out, len(set(roles.values()))
