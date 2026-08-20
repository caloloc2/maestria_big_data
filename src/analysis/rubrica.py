"""Rúbrica de calidad — capa DETERMINISTA (Fase 4, rubrica_v1).

Detecta lo verificable sin LLM: **palabras/frases prohibidas** (Grupo B) y la
presencia del **descargo legal** (A07/C05, por frase ancla). Es la primera capa
del análisis híbrido; Gemini (capa LLM) refina los criterios de contexto (A01–A09,
sentimiento, condicionalidad de B05/B06) sobre el MISMO texto anonimizado.

No requiere red ni clave: solo texto. Ver `proyecto/parametros_calidad_empresa.md`.
"""
import re
import unicodedata

# id → (patrón regex sobre texto normalizado, severidad, condicional)
PROHIBIDAS = {
    "B01": (r"\bgarantiz\w*", "CRITICA", False),
    "B02": (r"\b(le )?aseguro\b", "CRITICA", False),
    "B03": (r"\b100\s*%|\bcien por ciento\b", "CRITICA", False),
    "B04": (r"\bsin riesgo\b", "CRITICA", False),
    "B05": (r"\baprobad[oa]s?\b", "CRITICA", True),     # solo si falta "sujeto a aprobación de Diners"
    "B06": (r"\bsin interes(es)?\b", "CRITICA", True),  # solo si falta "del banco"
    "B07": (r"\bcuotas? fijas?\b", "CRITICA", False),
    "B08": (r"\bcredito inmediato\b|\binmediat[oa]\b", "CRITICA", False),
    "B09": (r"\bno paga nada\b", "CRITICA", False),
    "B10": (r"\bprestamo\b", "CRITICA", False),
    "B11": (r"\bgratis\b|\bsin costo\b", "MAYOR", False),
    "B12": (r"\bdescuento\b", "MAYOR", False),
    "B13": (r"\bilimitad[oa]s?\b|\bpara siempre\b", "MAYOR", False),
    "B14": (r"\bsolo por hoy\b|\bultimos? cupos?\b", "MAYOR", False),
    "B15": (r"\bsi no compra hoy\b|\btodos lo estan comprando\b", "MAYOR", False),
    "B16": (r"\bsorteo\b|\bregalo\b|\bbono\b", "MAYOR", False),
    "B17": (r"\bde parte del banco\b|\bmiembro del club\b|\bllamo del banco\b", "CRITICA", False),
    "B18": (r"\bimbatible\b|\bmejor precio\b|\bprecio de costo\b|\bexclusivo para usted\b", "MAYOR", False),
}
# Contextos que "salvan" a las condicionales (si aparecen, NO es infracción):
SALVA = {
    "B05": r"sujeto a (la )?aprobacion|aprobacion (de|por) diners",
    "B06": r"sin interes(es)? del banco|del banco",
}
# Ancla del descargo legal obligatorio (A07/C05):
ANCLA_DESGRAVAMEN = r"seguro de desgravamen"


def normaliza(t: str) -> str:
    t = unicodedata.normalize("NFKD", t.lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", t)


def detectar(transcript: str) -> dict:
    n = normaliza(transcript)
    hits = []
    for bid, (pat, sev, cond) in PROHIBIDAS.items():
        m = re.search(pat, n)
        if not m:
            continue
        if cond and bid in SALVA and re.search(SALVA[bid], n):
            continue  # el contexto lo salva
        hits.append({"id": bid, "severidad": sev, "termino": m.group(0), "condicional": cond})
    a07 = bool(re.search(ANCLA_DESGRAVAMEN, n))
    criticas = [h for h in hits if h["severidad"] == "CRITICA"]
    return {
        "b_infracciones": [h["id"] for h in hits],
        "detalle": hits,
        "a07_descargo_presente": a07,
        "c05_omision_descargo": not a07,   # CRÍTICA si falta
        "infraccion_critica_determinista": bool(criticas) or (not a07),
    }
