"""Weak supervision (Fase 4) — etiquetas APROXIMADas por reglas, sin LLM.

Genera una "verdad de referencia barata" para cada llamada transcrita, combinando
varias reglas simples (labeling functions) que codifican el conocimiento de la
rúbrica de auditoría (`proyecto/parametros_calidad_empresa.md`, rubrica_v1). Cada
regla es imperfecta ("débil"); su combinación PONDERADA da una etiqueta razonable
sin etiquetar a mano.

Etiquetas producidas por llamada:
  - wl_es_venta            (0/1)  ¿la llamada llegó al cierre de venta?
  - wl_infraccion_critica  (0/1)  ¿hay una infracción que anula la venta?
  - wl_venta_valida        (0/1)  venta cerrada Y sin infracción crítica.
Cada una con una CONFIANZA (0-1) y la lista de reglas que la sustentan.

Uso: `python -m src.analysis.weak_labels`  (dentro del contenedor Dagster).
Lee `servido.transcripciones`, escribe `servido.weak_labels` y compara con Gemini
(`servido.evaluaciones`) para reportar acuerdo/desacuerdo. Es la base para que
Auditoría valide a oído y luego se calibren los pesos.
"""
import re

from .rubrica import detectar, normaliza

# ─────────────────────────── Señales léxicas ───────────────────────────
# Frases de CIERRE (el Director Comercial confirma datos para diferir en la tarjeta).
CIERRE = (
    "director comercial", "confirmo sus datos", "confirmamos sus datos",
    "numero de su tarjeta", "numero de tarjeta", "fecha de vencimiento",
    "fecha de caducidad", "codigo de seguridad", "codigo de verificacion",
    "queda registrada", "bienvenido al", "felicitaciones", "su compra",
)
# Señales de RECHAZO del cliente (no cierra).
RECHAZO = (
    "no me interesa", "no gracias", "no deseo", "no quiero", "ya tengo",
    "no tengo tiempo", "llame despues", "llame luego", "no puedo ahora",
    "estoy ocupad", "no estoy interesad",
)
# Mención del permiso de grabación (parte de A03, criterio legal CRÍTICO).
PERMISO_GRAB = ("es grabada", "sera grabada", "siendo grabada", "autoriza que",
                "fines de calidad", "grabacion de esta llamada", "grabada de inicio")


def _tiene(txt_norm: str, frases) -> str | None:
    for f in frases:
        if f in txt_norm:
            return f
    return None


# ─────────────────────── Labeling functions ───────────────────────
# Cada una devuelve (objetivo, voto 0/1, peso, motivo) o None si se abstiene.

def lf_muy_corta(t: str, n: str, dur: float):
    if dur and dur < 90:
        return ("es_venta", 0, 2, f"grabación muy corta ({int(dur)}s) para cerrar")
    return None


def lf_larga(t: str, n: str, dur: float):
    if dur and dur >= 900:
        return ("es_venta", 1, 1, f"grabación larga ({int(dur)}s): suele llegar al cierre")
    return None


def lf_frases_cierre(t: str, n: str, dur: float):
    f = _tiene(n, CIERRE)
    if f:
        return ("es_venta", 1, 3, f"frase de cierre: «{f}»")
    return None


def lf_rechazo(t: str, n: str, dur: float):
    f = _tiene(n, RECHAZO)
    if f:
        return ("es_venta", 0, 2, f"rechazo del cliente: «{f}»")
    return None


def lf_dicta_tarjeta(t: str, n: str, dur: float):
    # El anonimizador reemplaza el número de tarjeta dictado por <TARJETA>: dictar la
    # tarjeta es señal fuerte de cierre de venta.
    if re.search(r"<\s*tarjeta\s*>", t, re.I):
        return ("es_venta", 1, 3, "cliente dictó número de tarjeta (<TARJETA>)")
    return None


def lf_prohibidas_criticas(t: str, n: str, dur: float):
    r = detectar(t)
    crit = [h["id"] for h in r["detalle"] if h["severidad"] == "CRITICA"]
    if crit:
        return ("infraccion_critica", 1, 3, f"palabra(s) prohibida(s) crítica(s): {','.join(crit)}")
    return None


def lf_sin_prohibidas(t: str, n: str, dur: float):
    r = detectar(t)
    if not r["b_infracciones"]:
        return ("infraccion_critica", 0, 1, "sin palabras prohibidas detectadas")
    return None


def lf_sin_permiso_grabacion(t: str, n: str, dur: float):
    # A03 (permiso de grabación) es CRÍTICO. Si no se menciona nada de grabación,
    # es señal DÉBIL de posible incumplimiento (peso bajo; el LLM/oído confirma).
    if _tiene(n, PERMISO_GRAB) is None:
        return ("infraccion_critica", 1, 1, "no se menciona el permiso de grabación (posible A03)")
    return None


LABELING_FUNCTIONS = [
    lf_muy_corta, lf_larga, lf_frases_cierre, lf_rechazo, lf_dicta_tarjeta,
    lf_prohibidas_criticas, lf_sin_prohibidas, lf_sin_permiso_grabacion,
]


def _agrega(votos: list[tuple[int, int, str]]):
    """Combina votos (voto, peso, motivo) → (etiqueta 0/1, confianza 0-1, motivos)."""
    if not votos:
        return None, 0.0, []
    w1 = sum(p for v, p, _ in votos if v == 1)
    w0 = sum(p for v, p, _ in votos if v == 0)
    total = w1 + w0
    etiqueta = 1 if w1 >= w0 else 0
    conf = round(abs(w1 - w0) / total, 2) if total else 0.0
    motivos = [m for v, _, m in votos if v == etiqueta]
    return etiqueta, conf, motivos


def etiquetar_llamada(transcript: str, dur_audio: float | None) -> dict:
    """Aplica todas las labeling functions y devuelve las etiquetas débiles."""
    t = transcript or ""
    n = normaliza(t)
    dur = float(dur_audio or 0)

    por_obj: dict[str, list] = {"es_venta": [], "infraccion_critica": []}
    disparadas = []
    for lf in LABELING_FUNCTIONS:
        r = lf(t, n, dur)
        if r is None:
            continue
        obj, voto, peso, motivo = r
        por_obj[obj].append((voto, peso, motivo))
        disparadas.append(f"{lf.__name__}: {motivo}")

    es_venta, conf_v, mot_v = _agrega(por_obj["es_venta"])
    crit, conf_c, mot_c = _agrega(por_obj["infraccion_critica"])

    # venta_valida = venta cerrada Y sin infracción crítica (regla dura de la rúbrica).
    if es_venta == 1 and crit == 0:
        venta_valida = 1
    elif crit == 1:
        venta_valida = 0
    else:
        venta_valida = 0  # sin venta → no hay venta válida
    conf_vv = round((conf_v + conf_c) / 2, 2)

    return {
        "wl_es_venta": es_venta, "conf_es_venta": conf_v,
        "wl_infraccion_critica": crit, "conf_critica": conf_c,
        "wl_venta_valida": venta_valida, "conf_venta_valida": conf_vv,
        "reglas": " | ".join(disparadas),
    }


# ─────────────────────────── Runner / reporte ───────────────────────────
DDL_WL = """
CREATE TABLE IF NOT EXISTS servido.weak_labels (
    call_id               text,
    fecha                 date,
    agente                text,
    dur_audio             real,
    wl_es_venta           integer,
    wl_infraccion_critica integer,
    wl_venta_valida       integer,
    confianza             real,
    reglas                text,
    rubrica               text DEFAULT 'rubrica_v1',
    run_ts                timestamp DEFAULT now()
)
"""


def main() -> int:
    import pandas as pd
    from sqlalchemy import text

    from src.processing.config import pg_engine

    eng = pg_engine()
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA IF NOT EXISTS servido"))
        c.execute(text(DDL_WL))

    # Une transcripción (texto) con la evaluación de Gemini (para comparar acuerdo).
    df = pd.read_sql(text(
        "SELECT t.call_id, t.fecha, t.agente, t.dur_audio, t.transcript_anon, "
        "e.es_venta AS g_es_venta, e.venta_valida AS g_venta_valida, "
        "e.infraccion_critica AS g_critica "
        "FROM servido.transcripciones t "
        "LEFT JOIN servido.evaluaciones e ON e.call_id = t.call_id"), eng)
    if not len(df):
        print("[weak] no hay transcripciones aún.")
        return 0

    filas = []
    for _, r in df.iterrows():
        wl = etiquetar_llamada(r["transcript_anon"], r["dur_audio"])
        filas.append({
            "call_id": r["call_id"], "fecha": r["fecha"], "agente": r["agente"],
            "dur_audio": r["dur_audio"], "wl_es_venta": wl["wl_es_venta"],
            "wl_infraccion_critica": wl["wl_infraccion_critica"],
            "wl_venta_valida": wl["wl_venta_valida"], "confianza": wl["conf_venta_valida"],
            "reglas": wl["reglas"],
        })
    out = pd.DataFrame(filas)

    with eng.begin() as c:
        c.execute(text("TRUNCATE servido.weak_labels"))
    out.to_sql("weak_labels", eng, schema="servido", if_exists="append", index=False)

    # ── Resumen ──
    print(f"[weak] etiquetadas {len(out)} llamadas (rubrica_v1)")
    print("  wl_es_venta:        ", out["wl_es_venta"].value_counts().to_dict())
    print("  wl_infraccion_crit: ", out["wl_infraccion_critica"].value_counts().to_dict())
    print("  wl_venta_valida:    ", out["wl_venta_valida"].value_counts().to_dict())
    print(f"  confianza media:     {out['confianza'].mean():.2f}")

    # Acuerdo con Gemini (donde exista evaluación)
    ok = df.dropna(subset=["g_critica"]).copy()
    if len(ok):
        w = out.set_index("call_id")
        ok = ok.set_index("call_id")
        ac_c = (w.loc[ok.index, "wl_infraccion_critica"] == ok["g_critica"].astype(int)).mean()
        ac_v = (w.loc[ok.index, "wl_venta_valida"] == ok["g_venta_valida"].astype(int)).mean()
        print(f"[weak] acuerdo con Gemini (n={len(ok)}): "
              f"infracción_crítica={ac_c:.0%} · venta_válida={ac_v:.0%}")
    eng.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
