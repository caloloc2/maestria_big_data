"""Capa LLM de la Fase 4 — evaluación de calidad con Google Gemini.

Sobre el texto ANONIMIZADO (única entrada que sale a la nube), Gemini evalúa los
criterios de contexto del Grupo A, el sentimiento y `es_venta`, y refina las
infracciones condicionales. Se combina con la capa determinista (rubrica.py) y se
aplican las reglas duras (schema.py) para obtener la evaluación final validada.

Requiere GEMINI_API_KEY en el .env. Modelo configurable (GEMINI_MODEL).
"""
import json
import os

from dotenv import load_dotenv

from .rubrica import detectar
from .schema import Evaluacion, aplicar_reglas_duras

load_dotenv(".env")
MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")

CRITERIOS_A = """
A01 Saludo (buenos días/tardes/noches).
A02 Presentación: nombre del asesor + "Corporación Marketing Vip S.A." + aliada Diners Club/Interdin.
A03 [CRÍTICA] Permiso de grabación explícito Y consentimiento del cliente.
A04 Datos obligatorios: vigencia 24 meses, reserva 45 días, valor del paquete.
A05 Aclara que la visa la aprueba el consulado (MKV solo informa).
A06 Declara que MKV NO es institución financiera.
A07 [CRÍTICA] Descargo legal (diferido Diners + seguro de desgravamen) — solo exigible si hubo venta.
A08 Cierre del Director Comercial: confirma cédula, valor, plazo/cuota, dirección, correo.
A09 Declaración final: "grabada de inicio a fin" + "no somos institución financiera".
""".strip()

PROMPT = """Eres auditor de calidad de un call center de ventas (paquetes turísticos a EE.UU. con \
diferido de tarjeta Diners Club). Analiza la TRANSCRIPCIÓN ANONIMIZADA (las etiquetas <NOMBRE>, \
<TELEFONO>, <CEDULA>, <TARJETA>, <DATO_NUMERICO> ocultan datos personales; NO las penalices).

Devuelve SOLO un JSON con estos campos:
- es_venta (0/1): ¿la llamada llegó al cierre/confirmación de la compra?
- grupo_A: objeto con A01..A09 en 0/1 según se cumplan estos criterios:
{criterios}
- grupo_C_omisiones: lista de ids omitidos entre C01(vigencia),C02(reserva 45d),C03(valor),\
C04(nombre empresa),C05(descargo legal),C06(dijo "tipo de crédito").
- sentimiento_asesor: bajo/neutral/positivo.
- sentimiento_cliente_trayectoria: lista corta (apertura→cierre), p.ej. ["neutral","positivo"].
- impersona_banco (0/1): 1 SOLO si el asesor da a entender que llama DE PARTE del banco/Diners \
o que ES el banco / una institución financiera, aunque no lo diga textual (ej. "le llamo por su \
tarjeta/cupo Diners", "del club de beneficios del banco", se presenta como el banco). 0 si deja \
claro que es Corporación Marketing Vip (empresa aliada, NO el banco) o si no menciona al banco así.
- riesgo_reclamo (bajo/medio/alto): **alto** SOLO si hay impersonación del banco, promesa \
engañosa clara (garantía falsa, "aprobado" sin condición, "sin riesgo") o cierre de venta sin \
informar condiciones clave (costo real, seguro de desgravamen). **medio** si hay ambigüedad \
relevante que podría confundir al cliente. **bajo** si la llamada fue transparente y sin promesas \
indebidas (la mayoría de las llamadas deberían ser bajo).
- confianza_llm: 0..1.

Pistas de la capa determinista (palabras prohibidas ya detectadas): {pistas}

TRANSCRIPCIÓN:
{transcript}
""".strip()


def _modelo():
    import google.generativeai as genai
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    return genai.GenerativeModel(MODEL)


def evaluar(call_id: str, transcript_anon: str) -> Evaluacion:
    det = detectar(transcript_anon)
    prompt = PROMPT.format(
        criterios=CRITERIOS_A,
        pistas=", ".join(det["b_infracciones"]) or "(ninguna)",
        transcript=transcript_anon[:15000],
    )
    modelo = _modelo()
    resp = modelo.generate_content(
        prompt, generation_config={"response_mime_type": "application/json", "temperature": 0}
    )
    # Telemetría de costo: tokens reales de entrada/salida por llamada (para estimar $).
    um = getattr(resp, "usage_metadata", None)
    if um is not None:
        print(f"[gemini-tokens] call={call_id} in={getattr(um, 'prompt_token_count', '?')} "
              f"out={getattr(um, 'candidates_token_count', '?')} "
              f"total={getattr(um, 'total_token_count', '?')} modelo={MODEL}", flush=True)
    data = json.loads(resp.text)

    ev = Evaluacion(
        call_id=call_id,
        es_venta=int(data.get("es_venta", 0)),
        grupo_A={k: int(v) for k, v in data.get("grupo_A", {}).items()},
        grupo_B_infracciones=det["b_infracciones"],   # determinista manda en B
        grupo_C_omisiones=list(data.get("grupo_C_omisiones", [])),
        sentimiento_asesor=str(data.get("sentimiento_asesor", "neutral")),
        sentimiento_cliente_trayectoria=list(data.get("sentimiento_cliente_trayectoria", [])),
        riesgo_reclamo=str(data.get("riesgo_reclamo", "bajo")),
        confianza_llm=float(data.get("confianza_llm", 0.0)),
        modelo=MODEL,
        impersona_banco=int(data.get("impersona_banco", 0)),
    )
    if det["c05_omision_descargo"] and "C05" not in ev.grupo_C_omisiones:
        ev.grupo_C_omisiones.append("C05")
    # calidad_score simple: % de A cumplidos (los pesos finos se calibran con gold set)
    a = ev.grupo_A
    ev.calidad_score = round(100 * sum(a.values()) / len(a)) if a else 0
    return aplicar_reglas_duras(ev)
