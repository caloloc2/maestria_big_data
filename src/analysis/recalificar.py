"""Re-calificación (rubrica_v2) SIN volver a llamar a Gemini.

Recalcula los veredictos (calidad_score, infraccion_critica, venta_valida,
venta_con_riesgo) a partir de los juicios CRUDOS ya guardados por Gemini
(grupo_A, grupo_B, es_venta, riesgo_reclamo). Como es una función pura de datos
ya almacenados, cambiar los pesos en pesos_v2.py y re-ejecutar es instantáneo y
gratis (no re-transcribe ni re-consulta al LLM).
"""
import json

from .pesos_v2 import (
    ANULA_VENTA,
    A_SOLO_VENTA,
    PESOS_A,
    PESOS_B,
    RIESGO_ALTO,
    RUBRICA,
)


def _as_dict(grupo_a) -> dict:
    if isinstance(grupo_a, dict):
        return grupo_a
    try:
        return json.loads(grupo_a) if grupo_a else {}
    except Exception:  # noqa: BLE001
        return {}


def _as_list(grupo_b) -> list:
    if isinstance(grupo_b, (list, tuple)):
        return [str(x).strip() for x in grupo_b if str(x).strip()]
    if not grupo_b:
        return []
    return [x.strip() for x in str(grupo_b).split(",") if x.strip()]


def recalificar(grupo_a, grupo_b, es_venta, riesgo_reclamo, impersona_banco=0) -> dict:
    a = _as_dict(grupo_a)
    b = set(_as_list(grupo_b))
    es_venta = int(es_venta or 0)
    impersona = int(impersona_banco or 0)

    pen = 0
    for aid, w in PESOS_A.items():
        if aid in A_SOLO_VENTA and not es_venta:
            continue  # descargo/cierre solo aplican en venta
        if int(a.get(aid, 0)) == 0:
            pen += w
    for bid in b:
        pen += PESOS_B.get(bid, 0)
    # Impersonación del banco detectada por contexto (Gemini) = B17 real → penaliza como B17.
    if impersona and "B17" not in b:
        pen += PESOS_B.get("B17", 0)

    score = max(0, 100 - pen)
    # Anula la venta: B17 por regex O impersonación detectada por contexto.
    critica = bool(b & ANULA_VENTA) or bool(impersona)
    venta_valida = 1 if (es_venta == 1 and not critica) else 0
    # "Venta con riesgo de reclamo" (rubrica_v2): se ancla en la impersonación del banco
    # (impersona_banco / B17), que Auditoría identificó como el principal generador de
    # reclamos. NO se usa riesgo_reclamo de Gemini aquí (marca medio/alto demasiado amplio).
    venta_con_riesgo = 1 if (es_venta == 1 and (impersona or "B17" in b)) else 0

    return {
        "calidad_score": score,
        "infraccion_critica": critica,
        "venta_valida": venta_valida,
        "venta_con_riesgo": venta_con_riesgo,
        "rubrica": RUBRICA,
    }
