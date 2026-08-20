"""Esquema de la evaluación de calidad por llamada (Fase 4, rubrica_v1).

Validación pydantic de la etiqueta que produce el análisis híbrido
(determinista + Gemini). Garantiza JSON conforme antes de persistir en la capa
servida. Ver `proyecto/guia_etiquetado_calidad.md`.
"""
from typing import Literal

from pydantic import BaseModel, Field


class Evaluacion(BaseModel):
    call_id: str
    rubrica: str = "rubrica_v1"
    es_venta: int = Field(ge=0, le=1, description="¿La llamada llegó al cierre de venta?")
    grupo_A: dict[str, int] = Field(default_factory=dict, description="A01..A09 → 0/1")
    grupo_B_infracciones: list[str] = Field(default_factory=list)
    grupo_C_omisiones: list[str] = Field(default_factory=list)
    infraccion_critica: bool = False
    calidad_score: int = Field(ge=0, le=100, default=0)
    venta_valida: int = Field(ge=0, le=1, default=1)
    riesgo_reclamo: Literal["bajo", "medio", "alto"] = "bajo"
    sentimiento_asesor: str = "neutral"
    sentimiento_cliente_trayectoria: list[str] = Field(default_factory=list)
    fuente_etiqueta: Literal["llm_propuesta", "humano_confirmada", "humano_corregida"] = "llm_propuesta"
    confianza_llm: float = Field(ge=0.0, le=1.0, default=0.0)
    modelo: str = ""


# Criterios CRÍTICOS del Grupo A (fallarlos ⇒ infracción crítica).
A_CRITICOS = {"A03", "A07"}
# Palabras prohibidas de severidad CRÍTICA (Grupo B).
B_CRITICAS = {"B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B17"}


def aplicar_reglas_duras(ev: Evaluacion) -> Evaluacion:
    """Regla dura: venta_valida=0 ante cualquier infracción crítica.
    Solo se exige el descargo/cierre (A03/A07/C05) si la llamada fue venta."""
    a_fallos = {k for k, v in ev.grupo_A.items() if v == 0 and k in A_CRITICOS}
    if not ev.es_venta:
        a_fallos.discard("A07")  # sin venta no se exige el descargo de cierre
    b_crit = set(ev.grupo_B_infracciones) & B_CRITICAS
    c05 = "C05" in ev.grupo_C_omisiones and ev.es_venta == 1
    ev.infraccion_critica = bool(a_fallos or b_crit or c05)
    ev.venta_valida = 0 if ev.infraccion_critica else 1
    return ev
