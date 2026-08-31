"""Pesos de la rúbrica de calidad — rubrica_v2 (calibrada con Auditoría, 2026-08-30).

ARCHIVO EDITABLE A MANO. Cada peso = puntos que se RESTAN de 100 si el ítem se
incumple (Grupo A: no se cumplió; Grupo B: la palabra/frase apareció). La nota de
calidad parte de 100 y baja con las penalizaciones (piso en 0).

Solo las infracciones en ANULA_VENTA invalidan la venta (venta_valida=0). Con la
calibración v2 la ÚNICA que anula es B17 (hacerse pasar por el banco), que es el
principal generador de reclamos según Auditoría.

Notas de calibración (para el documento):
- A03 (permiso de grabación) y A07 (descargo legal) NO anulan: Auditoría confirmó
  que NUNCA se dicen (o se dicen mal) → son mejoras del spitch, no fraude → pesan pero
  no invalidan la venta.
- Grupo B por regex es ambiguo (contexto) → pendiente moverlo a validación con Gemini.
- Grupo C = 0: C01-C05 ya se penalizan en el Grupo A (evita doble conteo); C06 (tipo
  de crédito) queda pendiente de definición de Auditoría.
"""
RUBRICA = "rubrica_v2"

# Grupo A — penaliza si el ítem NO se cumplió (valor 0 en grupo_A).
PESOS_A = {
    "A01": 2, "A02": 10, "A03": 20, "A04": 12, "A05": 10,
    "A06": 12, "A07": 12, "A08": 10, "A09": 8,
}
# Ítems del Grupo A que SOLO aplican (penalizan) si hubo venta.
A_SOLO_VENTA = {"A07", "A08"}

# Grupo B — penaliza si la palabra/frase apareció (id en grupo_B_infracciones).
PESOS_B = {
    "B01": 20, "B02": 15, "B03": 15, "B04": 10, "B05": 10, "B06": 10,
    "B07": 10, "B08": 8, "B09": 10, "B10": 10, "B11": 6, "B12": 5,
    "B13": 6, "B14": 6, "B15": 10, "B16": 6, "B17": 25, "B18": 5,
}

# Grupo C — 0 en v2 (ver nota arriba).
PESOS_C = {"C01": 0, "C02": 0, "C03": 0, "C04": 0, "C05": 0, "C06": 0}

# Única(s) infracción(es) que ANULAN la venta en v2.
ANULA_VENTA = {"B17"}

# Niveles de riesgo_reclamo (de Gemini) que cuentan como riesgo.
RIESGO_ALTO = {"medio", "alto"}
